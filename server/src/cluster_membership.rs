//! Short-lived, region-scoped ECS task membership used by executor placement.
//!
//! DynamoDB server registration is intentionally not used for executor
//! eligibility: its heartbeat is much slower than the partition takeover
//! budget.  Membership here is ephemeral control-plane state in Valkey.

use crate::redis_keys::RedisKeys;
use anyhow::{Context, Result, bail};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use uuid::Uuid;

pub const MEMBERSHIP_SCHEMA_VERSION: u16 = 2;
pub const EXECUTOR_PROTOCOL_VERSION: u16 = 2;
pub const DEFAULT_MEMBERSHIP_TTL: Duration = Duration::from_secs(4);
pub const DEFAULT_MEMBERSHIP_HEARTBEAT: Duration = Duration::from_secs(1);
const RETIRED_MEMBERSHIP_TOMBSTONE: &str = "__snaketron_retired_membership_v2__";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BootIdentity(String);

impl BootIdentity {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn parse(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        Uuid::parse_str(&value).context("boot identity must be a UUID")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for BootIdentity {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for BootIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskLifecycle {
    Warming,
    Active,
    Draining,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskMembership {
    pub schema_version: u16,
    pub boot_id: BootIdentity,
    pub server_id: u64,
    pub ecs_task_id: Option<String>,
    pub task_definition: Option<String>,
    pub executor_protocol_version: u16,
    pub lifecycle: TaskLifecycle,
    pub heartbeat_at_ms: i64,
    pub expires_at_ms: i64,
}

impl TaskMembership {
    pub fn new(
        boot_id: BootIdentity,
        server_id: u64,
        ecs_task_id: Option<String>,
        task_definition: Option<String>,
        lifecycle: TaskLifecycle,
        now_ms: i64,
        ttl: Duration,
    ) -> Self {
        Self {
            schema_version: MEMBERSHIP_SCHEMA_VERSION,
            boot_id,
            server_id,
            ecs_task_id,
            task_definition,
            executor_protocol_version: EXECUTOR_PROTOCOL_VERSION,
            lifecycle,
            heartbeat_at_ms: now_ms,
            expires_at_ms: now_ms.saturating_add(ttl.as_millis() as i64),
        }
    }

    pub fn is_assignment_eligible(&self, now_ms: i64) -> bool {
        self.lifecycle == TaskLifecycle::Active
            && self.expires_at_ms > now_ms
            && self.executor_protocol_version == EXECUTOR_PROTOCOL_VERSION
    }
}

/// Validated regional namespace whose key construction delegates to the
/// single `RedisKeys` catalog. Callers never format protocol keys themselves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterNamespace {
    region: String,
}

impl ClusterNamespace {
    pub fn new(region: impl Into<String>) -> Result<Self> {
        let region = region.into();
        if region.is_empty()
            || !region
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
        {
            bail!("region must contain only ASCII letters, digits, '-' or '_'");
        }
        Ok(Self { region })
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    pub fn members(&self) -> String {
        RedisKeys::cluster_members(&self.region)
    }

    pub fn member(&self, boot_id: &BootIdentity) -> String {
        RedisKeys::cluster_member(&self.region, boot_id.as_str())
    }

    pub fn assignment(&self) -> String {
        RedisKeys::cluster_assignment(&self.region)
    }

    pub fn assignment_lease(&self) -> String {
        RedisKeys::cluster_assignment_lease(&self.region)
    }

    pub fn partition_lease(&self, partition: u32) -> String {
        RedisKeys::cluster_partition_lease(&self.region, partition)
    }

    pub fn active_games(&self, partition: u32) -> String {
        RedisKeys::cluster_active_games(&self.region, partition)
    }

    pub fn recovery(&self, game_id: u32) -> String {
        RedisKeys::cluster_recovery(&self.region, game_id)
    }

    pub fn recovery_failure(&self, game_id: u32) -> String {
        RedisKeys::cluster_recovery_failure(&self.region, game_id)
    }

    pub fn command_group(&self, partition: u32) -> String {
        RedisKeys::executor_command_group(&self.region, partition)
    }

    pub fn command_quarantine(&self, partition: u32) -> String {
        RedisKeys::cluster_command_quarantine(&self.region, partition)
    }

    pub fn command_decisions(&self, partition: u32) -> String {
        RedisKeys::cluster_command_decisions(&self.region, partition)
    }

    pub fn completion(&self, game_id: u32) -> String {
        RedisKeys::cluster_completion(&self.region, game_id)
    }

    pub fn pending_completions(&self, partition: u32) -> String {
        RedisKeys::cluster_pending_completions(&self.region, partition)
    }

    pub fn completion_effects_done(&self, game_id: u32) -> String {
        RedisKeys::cluster_completion_effects_done(&self.region, game_id)
    }

    pub fn completion_terminal_notified(&self, game_id: u32) -> String {
        RedisKeys::cluster_completion_terminal_notified(&self.region, game_id)
    }
}

#[derive(Clone)]
pub struct MembershipStore {
    redis: ConnectionManager,
    namespace: ClusterNamespace,
    ttl: Duration,
}

impl MembershipStore {
    pub fn new(
        redis: ConnectionManager,
        namespace: ClusterNamespace,
        ttl: Duration,
    ) -> Result<Self> {
        if ttl < Duration::from_millis(500) {
            bail!("membership TTL must be at least 500ms");
        }
        Ok(Self {
            redis,
            namespace,
            ttl,
        })
    }

    pub fn namespace(&self) -> &ClusterNamespace {
        &self.namespace
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Atomically refresh the member document and its expiry index entry.
    pub async fn heartbeat(&self, membership: &TaskMembership) -> Result<()> {
        if membership.schema_version != MEMBERSHIP_SCHEMA_VERSION {
            bail!("unsupported membership schema version");
        }
        let payload = serde_json::to_vec(membership)?;
        let mut redis = self.redis.clone();
        let script = redis::Script::new(
            r#"
            local current = redis.call('GET', KEYS[1])
            if current == ARGV[5] then return 0 end
            if current then
                local current_ok, current_member = pcall(cjson.decode, current)
                local next_ok, next_member = pcall(cjson.decode, ARGV[1])
                if current_ok and next_ok
                    and current_member.lifecycle == 'DRAINING'
                    and next_member.lifecycle ~= 'DRAINING' then
                    return 0
                end
            end
            redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
            redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
            return 1
            "#,
        );
        let _: i32 = script
            .key(self.namespace.member(&membership.boot_id))
            .key(self.namespace.members())
            .arg(payload)
            .arg(self.ttl.as_millis() as u64)
            .arg(membership.expires_at_ms)
            .arg(membership.boot_id.as_str())
            .arg(RETIRED_MEMBERSHIP_TOMBSTONE)
            .invoke_async(&mut redis)
            .await
            .context("failed to refresh task membership")?;
        Ok(())
    }

    pub async fn remove(&self, boot_id: &BootIdentity) -> Result<()> {
        let mut redis = self.redis.clone();
        let script = redis::Script::new(
            r#"
            -- Keep a short-lived tombstone so an already-sent heartbeat cannot
            -- recreate an ACTIVE member after graceful retirement.
            redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
            redis.call('ZREM', KEYS[2], ARGV[1])
            return 1
            "#,
        );
        let _: i32 = script
            .key(self.namespace.member(boot_id))
            .key(self.namespace.members())
            .arg(boot_id.as_str())
            .arg(RETIRED_MEMBERSHIP_TOMBSTONE)
            .arg(self.ttl.as_millis() as u64)
            .invoke_async(&mut redis)
            .await
            .context("failed to remove task membership")?;
        Ok(())
    }

    /// Returns live documents only.  A sorted-set entry without a live member
    /// key is stale and is ignored; the next cleanup removes it.
    pub async fn list_live(&self, now_ms: i64) -> Result<Vec<TaskMembership>> {
        let mut redis = self.redis.clone();
        let members_key = self.namespace.members();
        let _: usize = redis
            .zrembyscore(&members_key, "-inf", now_ms)
            .await
            .context("failed to prune expired task memberships")?;
        let ids: Vec<String> = redis
            .zrangebyscore(&members_key, now_ms.saturating_add(1), "+inf")
            .await
            .context("failed to list live task memberships")?;

        let mut result = Vec::with_capacity(ids.len());
        for id in ids {
            let Ok(boot_id) = BootIdentity::parse(id) else {
                continue;
            };
            let payload: Option<Vec<u8>> = redis
                .get(self.namespace.member(&boot_id))
                .await
                .context("failed to read task membership")?;
            if let Some(payload) = payload {
                match serde_json::from_slice::<TaskMembership>(&payload) {
                    Ok(member) if member.expires_at_ms > now_ms => result.push(member),
                    Ok(_) => {}
                    Err(error) => warn!(%error, %boot_id, "ignoring malformed task membership"),
                }
            }
        }
        result.sort_by(|a, b| a.boot_id.cmp(&b.boot_id));
        Ok(result)
    }

    /// Runs a bounded heartbeat loop.  The supplied membership builder is
    /// called every iteration so lifecycle state can change without replacing
    /// the task identity.
    pub async fn run_heartbeat(
        &self,
        interval: Duration,
        mut current: impl FnMut(i64, Duration) -> TaskMembership,
        cancellation: CancellationToken,
    ) -> Result<()> {
        if interval >= self.ttl {
            bail!("membership heartbeat interval must be shorter than its TTL");
        }
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(()),
                _ = ticker.tick() => {
                    let now_ms = chrono::Utc::now().timestamp_millis();
                    self.heartbeat(&current(now_ms, self.ttl)).await?;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_is_region_scoped() {
        let a = ClusterNamespace::new("us-east-1").unwrap();
        let b = ClusterNamespace::new("eu-west-1").unwrap();
        assert_ne!(a.assignment(), b.assignment());
        assert_ne!(a.partition_lease(3), b.partition_lease(3));
        assert!(a.recovery(42).contains("us-east-1"));
    }

    #[test]
    fn only_current_active_v2_members_are_eligible() {
        let boot = BootIdentity::new();
        let mut member = TaskMembership::new(
            boot,
            1,
            None,
            None,
            TaskLifecycle::Active,
            1_000,
            Duration::from_secs(4),
        );
        assert!(member.is_assignment_eligible(4_999));
        assert!(!member.is_assignment_eligible(5_000));
        member.lifecycle = TaskLifecycle::Draining;
        assert!(!member.is_assignment_eligible(2_000));
    }

    #[tokio::test]
    async fn draining_and_retirement_cannot_be_undone_by_a_late_active_heartbeat() -> Result<()> {
        use redis::AsyncCommands;

        let client = redis::Client::open("redis://127.0.0.1:6379/1?protocol=resp3")?;
        let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(8);
        let manager = crate::redis_utils::create_connection_manager(client, pubsub_tx).await?;
        let namespace = ClusterNamespace::new(format!("membership-drain-{}", Uuid::new_v4()))?;
        let store =
            MembershipStore::new(manager.clone(), namespace.clone(), DEFAULT_MEMBERSHIP_TTL)?;
        let boot_id = BootIdentity::new();
        let now_ms = chrono::Utc::now().timestamp_millis();
        let active = TaskMembership::new(
            boot_id.clone(),
            1,
            None,
            None,
            TaskLifecycle::Active,
            now_ms,
            store.ttl(),
        );
        let draining = TaskMembership::new(
            boot_id.clone(),
            1,
            None,
            None,
            TaskLifecycle::Draining,
            now_ms + 1,
            store.ttl(),
        );

        store.heartbeat(&active).await?;
        store.heartbeat(&draining).await?;
        store.heartbeat(&active).await?;
        let visible = store.list_live(now_ms + 2).await?;
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].lifecycle, TaskLifecycle::Draining);

        store.remove(&boot_id).await?;
        store.heartbeat(&active).await?;
        assert!(store.list_live(now_ms + 2).await?.is_empty());

        let mut cleanup = manager;
        let _: () = cleanup
            .del(&[namespace.members(), namespace.member(&boot_id)])
            .await?;
        Ok(())
    }
}
