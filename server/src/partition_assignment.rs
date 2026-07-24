//! Deterministic, minimally-moving desired placement for executor partitions.

use crate::cluster_membership::{BootIdentity, ClusterNamespace, TaskMembership};
use crate::redis_utils::RedisConnection;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub const ASSIGNMENT_SCHEMA_VERSION: u16 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentDocument {
    pub schema_version: u16,
    pub version: u64,
    pub region: String,
    pub computed_at_ms: i64,
    pub eligible_members: Vec<BootIdentity>,
    /// JSON object keys are decimal partition IDs.
    pub owners: BTreeMap<u32, BootIdentity>,
}

impl AssignmentDocument {
    pub fn desired_owner(&self, partition: u32) -> Option<&BootIdentity> {
        self.owners.get(&partition)
    }

    pub fn validate(&self, partition_count: u32) -> Result<()> {
        if self.schema_version != ASSIGNMENT_SCHEMA_VERSION {
            bail!("unsupported assignment schema version");
        }
        let eligible: BTreeSet<_> = self.eligible_members.iter().collect();
        if eligible.len() != self.eligible_members.len() {
            bail!("assignment contains duplicate eligible members");
        }
        if eligible.is_empty() {
            if !self.owners.is_empty() {
                bail!("assignment without eligible members must not have owners");
            }
            return Ok(());
        }
        for partition in 0..partition_count {
            let owner = self
                .owners
                .get(&partition)
                .ok_or_else(|| anyhow::anyhow!("partition {partition} has no desired owner"))?;
            if !eligible.contains(owner) {
                bail!("partition {partition} is assigned to an ineligible task");
            }
        }
        if self
            .owners
            .keys()
            .any(|partition| *partition >= partition_count)
        {
            bail!("assignment contains an out-of-range partition");
        }
        let counts = owner_counts(&self.owners, &self.eligible_members);
        if let (Some(min), Some(max)) = (counts.values().min(), counts.values().max())
            && max - min > 1
        {
            bail!("assignment owner counts differ by more than one");
        }
        Ok(())
    }
}

fn owner_counts(
    owners: &BTreeMap<u32, BootIdentity>,
    eligible: &[BootIdentity],
) -> BTreeMap<BootIdentity, usize> {
    let mut counts: BTreeMap<BootIdentity, usize> =
        eligible.iter().cloned().map(|member| (member, 0)).collect();
    for owner in owners.values() {
        if let Some(count) = counts.get_mut(owner) {
            *count += 1;
        }
    }
    counts
}

/// Reconciles desired placement while minimizing the assignment map's Hamming
/// distance from `previous`, after removing ineligible owners and satisfying
/// the balance rule.  Tie-breaking is stable by boot ID and partition ID.
pub fn balanced_minimal_movement(
    partition_count: u32,
    previous: Option<&AssignmentDocument>,
    eligible: impl IntoIterator<Item = BootIdentity>,
    region: impl Into<String>,
    version: u64,
    now_ms: i64,
) -> AssignmentDocument {
    let eligible: Vec<BootIdentity> = eligible
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let eligible_set: BTreeSet<_> = eligible.iter().cloned().collect();

    let mut owners: BTreeMap<u32, BootIdentity> = previous
        .map(|document| {
            document
                .owners
                .iter()
                .filter(|(partition, owner)| {
                    **partition < partition_count && eligible_set.contains(*owner)
                })
                .map(|(partition, owner)| (*partition, owner.clone()))
                .collect()
        })
        .unwrap_or_default();

    if !eligible.is_empty() {
        // Orphaned/new partitions must move regardless.  Assign them to the
        // least-loaded owner first; this can only reduce later balancing moves.
        for partition in 0..partition_count {
            if owners.contains_key(&partition) {
                continue;
            }
            let counts = owner_counts(&owners, &eligible);
            let target = counts
                .iter()
                .min_by_key(|(member, count)| (**count, (*member).clone()))
                .map(|(member, _)| member.clone())
                .expect("eligible is non-empty");
            owners.insert(partition, target);
        }

        // Each move reduces max-min imbalance.  Moving from the lexicographically
        // greatest overloaded member's greatest partition gives deterministic
        // output without moving any already-valid assignment unnecessarily.
        loop {
            let counts = owner_counts(&owners, &eligible);
            let min_count = *counts.values().min().expect("eligible is non-empty");
            let max_count = *counts.values().max().expect("eligible is non-empty");
            if max_count - min_count <= 1 {
                break;
            }
            let donor = counts
                .iter()
                .filter(|(_, count)| **count == max_count)
                .map(|(member, _)| member)
                .next_back()
                .expect("max owner exists")
                .clone();
            let recipient = counts
                .iter()
                .find(|(_, count)| **count == min_count)
                .map(|(member, _)| member.clone())
                .expect("min owner exists");
            let partition = owners
                .iter()
                .filter(|(_, owner)| **owner == donor)
                .map(|(partition, _)| *partition)
                .next_back()
                .expect("donor owns a partition");
            owners.insert(partition, recipient);
        }
    }

    AssignmentDocument {
        schema_version: ASSIGNMENT_SCHEMA_VERSION,
        version,
        region: region.into(),
        computed_at_ms: now_ms,
        eligible_members: eligible,
        owners,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssignmentWrite {
    Written,
    VersionConflict,
    CoordinatorLeaseLost,
}

#[derive(Clone)]
pub struct AssignmentStore {
    redis: RedisConnection,
    namespace: ClusterNamespace,
    last_synced_view_version: Arc<AtomicU64>,
}

impl AssignmentStore {
    pub fn new(redis: impl Into<RedisConnection>, namespace: ClusterNamespace) -> Self {
        Self {
            redis: redis.into(),
            namespace,
            last_synced_view_version: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn key(&self) -> String {
        self.namespace.assignment()
    }

    pub async fn load(&self) -> Result<Option<AssignmentDocument>> {
        use redis::AsyncCommands;
        let mut redis = self.redis.clone();
        let payload: Option<Vec<u8>> = redis
            .get(self.namespace.assignment())
            .await
            .context("failed to load partition assignment")?;
        payload
            .map(|payload| {
                serde_json::from_slice(&payload).context("malformed partition assignment")
            })
            .transpose()
    }

    /// Atomically checks the exact coordinator token and prior assignment
    /// version before replacing the complete document.
    pub async fn compare_and_set(
        &self,
        coordinator_token: &str,
        expected_version: Option<u64>,
        next: &AssignmentDocument,
    ) -> Result<AssignmentWrite> {
        if next.region != self.namespace.region() {
            bail!("assignment region does not match its key namespace");
        }
        if next.version != expected_version.map_or(1, |version| version + 1) {
            bail!("assignment version must increment exactly once");
        }
        let payload = serde_json::to_vec(next)?;
        let mut redis = self.redis.clone();
        let result: i32 = redis::Script::new(
            r#"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then
                return -1
            end
            local current = redis.call('GET', KEYS[2])
            if ARGV[2] == '-1' then
                if current then return 0 end
            else
                if not current then return 0 end
                local ok, decoded = pcall(cjson.decode, current)
                if not ok or tostring(decoded.version) ~= ARGV[2] then return 0 end
            end
            redis.call('SET', KEYS[2], ARGV[3])
            return 1
            "#,
        )
        .key(self.namespace.assignment_lease())
        .key(self.namespace.assignment())
        .arg(coordinator_token)
        .arg(
            expected_version
                .map(|version| version.to_string())
                .unwrap_or_else(|| "-1".to_string()),
        )
        .arg(payload)
        .invoke_async(&mut redis)
        .await
        .context("failed to atomically write partition assignment")?;
        Ok(match result {
            1 => AssignmentWrite::Written,
            0 => AssignmentWrite::VersionConflict,
            -1 => AssignmentWrite::CoordinatorLeaseLost,
            _ => bail!("unknown assignment write result {result}"),
        })
    }

    pub async fn reconcile(
        &self,
        coordinator_token: &str,
        partition_count: u32,
        members: &[TaskMembership],
        now_ms: i64,
    ) -> Result<(AssignmentWrite, AssignmentDocument)> {
        let previous = self.load().await?;
        let eligible = members
            .iter()
            .filter(|member| member.is_assignment_eligible(now_ms))
            .map(|member| member.boot_id.clone());
        let next = balanced_minimal_movement(
            partition_count,
            previous.as_ref(),
            eligible,
            self.namespace.region(),
            previous
                .as_ref()
                .map_or(1, |assignment| assignment.version + 1),
            now_ms,
        );
        next.validate(partition_count)?;

        // Do not churn the version/timestamp when the inputs and owner map are
        // unchanged.  This makes reconciliation idempotent.
        if let Some(previous) = &previous
            && previous.eligible_members == next.eligible_members
            && previous.owners == next.owners
        {
            self.sync_partition_views(previous, partition_count).await?;
            return Ok((AssignmentWrite::Written, previous.clone()));
        }
        let outcome = self
            .compare_and_set(
                coordinator_token,
                previous.as_ref().map(|assignment| assignment.version),
                &next,
            )
            .await?;
        if outcome == AssignmentWrite::Written {
            self.sync_partition_views(&next, partition_count).await?;
        }
        Ok((outcome, next))
    }

    /// Project the canonical control-plane document into each executor slot.
    /// The projection is monotonic, so a delayed old coordinator cannot roll
    /// a partition back. A new coordinator always repairs all views once;
    /// afterward unchanged reconciliations stay local and write-free.
    async fn sync_partition_views(
        &self,
        assignment: &AssignmentDocument,
        partition_count: u32,
    ) -> Result<()> {
        if self.last_synced_view_version.load(Ordering::Acquire) == assignment.version {
            return Ok(());
        }
        let payload = serde_json::to_vec(assignment)?;
        let expected_version = assignment.version.to_string();
        for partition in 0..partition_count {
            let mut redis = self.redis.clone();
            let result: i32 = redis::Script::new(
                r#"
                local current = redis.call('GET', KEYS[1])
                if current then
                    local ok, decoded = pcall(cjson.decode, current)
                    if ok and tonumber(decoded.version) > tonumber(ARGV[1]) then return 0 end
                    if ok and tostring(decoded.version) == ARGV[1] then
                        if current == ARGV[2] then return 0 end
                        return -1
                    end
                end
                redis.call('SET', KEYS[1], ARGV[2])
                return 1
                "#,
            )
            .key(self.namespace.partition_assignment(partition))
            .arg(&expected_version)
            .arg(&payload)
            .invoke_async(&mut redis)
            .await
            .with_context(|| format!("failed to sync assignment view for partition {partition}"))?;
            if result < 0 {
                bail!(
                    "partition {partition} has a conflicting assignment at version {}",
                    assignment.version
                );
            }
        }
        self.last_synced_view_version
            .store(assignment.version, Ordering::Release);
        Ok(())
    }

    /// Repairs the partition-local lease views without changing canonical
    /// placement. The coordinator uses this while a safe membership change is
    /// being stabilized, so a prior crash during projection cannot leave lease
    /// acquisition blocked for the duration of that window.
    pub(crate) async fn repair_partition_views(
        &self,
        assignment: &AssignmentDocument,
        partition_count: u32,
    ) -> Result<()> {
        assignment.validate(partition_count)?;
        self.sync_partition_views(assignment, partition_count).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_membership::TaskLifecycle;
    use redis::AsyncCommands;
    use std::time::Duration;

    fn id(n: u128) -> BootIdentity {
        BootIdentity::parse(format!("{n:032x}")).unwrap()
    }

    fn changed(a: &AssignmentDocument, b: &AssignmentDocument) -> usize {
        (0..10)
            .filter(|partition| a.owners.get(partition) != b.owners.get(partition))
            .count()
    }

    #[test]
    fn scale_out_is_balanced_deterministic_and_minimal() {
        let a = id(1);
        let b = id(2);
        let c = id(3);
        let one = balanced_minimal_movement(10, None, [a.clone()], "test", 1, 0);
        let two = balanced_minimal_movement(10, Some(&one), [b.clone(), a.clone()], "test", 2, 1);
        let three = balanced_minimal_movement(10, Some(&two), [c.clone(), b, a], "test", 3, 2);
        one.validate(10).unwrap();
        two.validate(10).unwrap();
        three.validate(10).unwrap();
        assert_eq!(changed(&one, &two), 5);
        // 5/5 -> 4/3/3 needs exactly three moves to the new member.
        assert_eq!(changed(&two, &three), 3);
    }

    #[test]
    fn scale_in_moves_only_departing_owners_partitions() {
        let members = [id(1), id(2), id(3), id(4)];
        let four = balanced_minimal_movement(10, None, members.clone(), "test", 1, 0);
        let removed = members[1].clone();
        let remaining = [members[0].clone(), members[2].clone(), members[3].clone()];
        let three = balanced_minimal_movement(10, Some(&four), remaining, "test", 2, 1);
        for partition in 0..10 {
            if four.owners[&partition] != removed {
                assert_eq!(four.owners[&partition], three.owners[&partition]);
            }
        }
        three.validate(10).unwrap();
    }

    #[test]
    fn unchanged_membership_has_zero_movement() {
        let members = [id(1), id(2), id(3)];
        let first = balanced_minimal_movement(10, None, members.clone(), "test", 1, 0);
        let second = balanced_minimal_movement(10, Some(&first), members, "test", 2, 1);
        assert_eq!(first.owners, second.owners);
    }

    #[test]
    fn exhaustive_task_counts_stay_balanced() {
        let all: Vec<_> = (1..=10).map(id).collect();
        let mut previous = None;
        for count in 1..=10 {
            let next = balanced_minimal_movement(
                10,
                previous.as_ref(),
                all[..count].iter().cloned(),
                "test",
                count as u64,
                count as i64,
            );
            next.validate(10).unwrap();
            previous = Some(next);
        }
        for count in (1..10).rev() {
            let next = balanced_minimal_movement(
                10,
                previous.as_ref(),
                all[..count].iter().cloned(),
                "test",
                (20 - count) as u64,
                count as i64,
            );
            next.validate(10).unwrap();
            previous = Some(next);
        }
    }

    async fn redis_store(prefix: &str) -> Result<(redis::aio::ConnectionManager, AssignmentStore)> {
        let client = redis::Client::open("redis://127.0.0.1:6379/1?protocol=resp3")?;
        let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(8);
        let manager = crate::redis_utils::create_connection_manager(client, pubsub_tx).await?;
        let namespace = ClusterNamespace::new(format!("{prefix}-{}", uuid::Uuid::new_v4()))?;
        Ok((manager.clone(), AssignmentStore::new(manager, namespace)))
    }

    #[tokio::test]
    async fn concurrent_coordinators_cannot_publish_competing_assignment_versions() -> Result<()> {
        let (mut redis, store) = redis_store("assignment-cas").await?;
        let coordinator_token = uuid::Uuid::new_v4().to_string();
        let _: () = redis
            .set(store.namespace.assignment_lease(), &coordinator_token)
            .await?;

        let first = balanced_minimal_movement(
            10,
            None,
            [BootIdentity::new()],
            store.namespace.region(),
            1,
            1,
        );
        assert_eq!(
            store
                .compare_and_set(&coordinator_token, None, &first)
                .await?,
            AssignmentWrite::Written
        );

        let candidate_a = balanced_minimal_movement(
            10,
            Some(&first),
            [first.eligible_members[0].clone(), BootIdentity::new()],
            store.namespace.region(),
            2,
            2,
        );
        let candidate_b = balanced_minimal_movement(
            10,
            Some(&first),
            [first.eligible_members[0].clone(), BootIdentity::new()],
            store.namespace.region(),
            2,
            3,
        );
        let (write_a, write_b) = tokio::join!(
            store.compare_and_set(&coordinator_token, Some(1), &candidate_a),
            store.compare_and_set(&coordinator_token, Some(1), &candidate_b),
        );
        let outcomes = [write_a?, write_b?];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == AssignmentWrite::Written)
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == AssignmentWrite::VersionConflict)
                .count(),
            1
        );

        let stored = store.load().await?.context("assignment disappeared")?;
        assert!(stored == candidate_a || stored == candidate_b);
        stored.validate(10)?;
        assert_eq!(stored.version, 2);
        assert_eq!(
            store
                .compare_and_set(&coordinator_token, Some(1), &candidate_a)
                .await?,
            AssignmentWrite::VersionConflict,
            "a delayed coordinator must not overwrite the published generation"
        );

        let _: () = redis
            .del(&[
                store.namespace.assignment(),
                store.namespace.assignment_lease(),
            ])
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn rapid_membership_churn_converges_with_monotonic_balanced_assignments() -> Result<()> {
        let (mut redis, store) = redis_store("assignment-churn").await?;
        let coordinator_token = uuid::Uuid::new_v4().to_string();
        let _: () = redis
            .set(store.namespace.assignment_lease(), &coordinator_token)
            .await?;
        let members: Vec<_> = (0..10)
            .map(|server_id| {
                TaskMembership::new(
                    BootIdentity::new(),
                    server_id,
                    None,
                    Some("test:2".into()),
                    TaskLifecycle::Active,
                    1_000,
                    Duration::from_secs(60),
                )
            })
            .collect();

        let mut versions = Vec::new();
        for (step, member_count) in [1_usize, 4, 2, 10, 1].into_iter().enumerate() {
            let (write, assignment) = store
                .reconcile(
                    &coordinator_token,
                    10,
                    &members[..member_count],
                    1_000 + step as i64,
                )
                .await?;
            assert_eq!(write, AssignmentWrite::Written);
            assignment.validate(10)?;
            assert_eq!(assignment.eligible_members.len(), member_count);
            versions.push(assignment.version);
        }
        assert_eq!(versions, vec![1, 2, 3, 4, 5]);
        let final_assignment = store.load().await?.context("assignment disappeared")?;
        assert!(
            final_assignment
                .owners
                .values()
                .all(|owner| owner == &members[0].boot_id)
        );

        let _: () = redis
            .del(&[
                store.namespace.assignment(),
                store.namespace.assignment_lease(),
            ])
            .await?;
        Ok(())
    }
}
