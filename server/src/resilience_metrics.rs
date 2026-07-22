//! Bounded-cardinality resilience telemetry emitted in CloudWatch EMF format.
//!
//! Correctness must not depend on metrics. Collection therefore uses its own
//! best-effort loop and never changes liveness or lease state when CloudWatch,
//! Valkey, or stdout is unavailable.

use crate::cluster_membership::{ClusterNamespace, MembershipStore, TaskLifecycle};
use crate::game_executor::PARTITION_COUNT;
use crate::lifecycle::TaskLifecycle as LocalTaskLifecycle;
use crate::partition_assignment::AssignmentStore;
use crate::recovery::RecoveryEnvelopeV2;
use anyhow::{Context, Result};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use redis::streams::StreamPendingReply;
use serde_json::{Map, Value, json};
use std::array;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

const EMF_NAMESPACE: &str = "Snaketron/Resilience";
const DEFAULT_EMIT_INTERVAL_SECS: u64 = 15;
const OWNERSHIP_SAMPLE_INTERVAL_MS: i64 = 500;

#[derive(Default)]
struct Counters {
    fenced_write_rejections: AtomicU64,
    recovery_fingerprint_divergences: AtomicU64,
    planned_drain_failures: AtomicU64,
    command_claims: AtomicU64,
    command_acks: AtomicU64,
    command_resends: AtomicU64,
    command_deduplications: AtomicU64,
    command_rejections: AtomicU64,
    checkpoint_writes: AtomicU64,
    checkpoint_failures: AtomicU64,
    recovered_games: AtomicU64,
    recovery_replays: AtomicU64,
    match_claim_conflicts: AtomicU64,
    duplicate_completion_effects_prevented: AtomicU64,
}

static COUNTERS: OnceLock<Counters> = OnceLock::new();

fn counters() -> &'static Counters {
    COUNTERS.get_or_init(Counters::default)
}

macro_rules! counter_fn {
    ($name:ident, $field:ident) => {
        pub fn $name(count: u64) {
            counters().$field.fetch_add(count, Ordering::Relaxed);
        }
    };
}

counter_fn!(record_fenced_write_rejection, fenced_write_rejections);
counter_fn!(
    record_recovery_fingerprint_divergence,
    recovery_fingerprint_divergences
);
counter_fn!(record_planned_drain_failure, planned_drain_failures);
counter_fn!(record_command_claims, command_claims);
counter_fn!(record_command_acks, command_acks);
counter_fn!(record_command_resends, command_resends);
counter_fn!(record_command_deduplications, command_deduplications);
counter_fn!(record_command_rejections, command_rejections);
counter_fn!(record_checkpoint_writes, checkpoint_writes);
counter_fn!(record_checkpoint_failures, checkpoint_failures);
counter_fn!(record_recovered_games, recovered_games);
counter_fn!(record_recovery_replays, recovery_replays);
counter_fn!(record_match_claim_conflicts, match_claim_conflicts);
counter_fn!(
    record_duplicate_completion_effect_prevented,
    duplicate_completion_effects_prevented
);

struct CounterSnapshot {
    fenced_write_rejections: u64,
    recovery_fingerprint_divergences: u64,
    planned_drain_failures: u64,
    command_claims: u64,
    command_acks: u64,
    command_resends: u64,
    command_deduplications: u64,
    command_rejections: u64,
    checkpoint_writes: u64,
    checkpoint_failures: u64,
    recovered_games: u64,
    recovery_replays: u64,
    match_claim_conflicts: u64,
    duplicate_completion_effects_prevented: u64,
}

fn take_counter_snapshot() -> CounterSnapshot {
    let counters = counters();
    CounterSnapshot {
        fenced_write_rejections: counters.fenced_write_rejections.swap(0, Ordering::Relaxed),
        recovery_fingerprint_divergences: counters
            .recovery_fingerprint_divergences
            .swap(0, Ordering::Relaxed),
        planned_drain_failures: counters.planned_drain_failures.swap(0, Ordering::Relaxed),
        command_claims: counters.command_claims.swap(0, Ordering::Relaxed),
        command_acks: counters.command_acks.swap(0, Ordering::Relaxed),
        command_resends: counters.command_resends.swap(0, Ordering::Relaxed),
        command_deduplications: counters.command_deduplications.swap(0, Ordering::Relaxed),
        command_rejections: counters.command_rejections.swap(0, Ordering::Relaxed),
        checkpoint_writes: counters.checkpoint_writes.swap(0, Ordering::Relaxed),
        checkpoint_failures: counters.checkpoint_failures.swap(0, Ordering::Relaxed),
        recovered_games: counters.recovered_games.swap(0, Ordering::Relaxed),
        recovery_replays: counters.recovery_replays.swap(0, Ordering::Relaxed),
        match_claim_conflicts: counters.match_claim_conflicts.swap(0, Ordering::Relaxed),
        duplicate_completion_effects_prevented: counters
            .duplicate_completion_effects_prevented
            .swap(0, Ordering::Relaxed),
    }
}

#[derive(Default)]
struct RegionalGauges {
    ready_tasks: u64,
    live_tasks: u64,
    draining_tasks: u64,
    membership_age_ms: u64,
    assignment_version: u64,
    assignment_age_ms: u64,
    assignment_imbalance: u64,
    active_partition_leases: u64,
    partition_lease_deficit: u64,
    partition_owner_mismatches: u64,
    partition_unowned_ms: u64,
    oldest_pending_command_ms: u64,
    pending_commands: u64,
    pending_completions: u64,
    quarantined_commands: u64,
    checkpoint_age_ms: u64,
    checkpoint_bytes: u64,
    active_games: u64,
    active_game_index_mismatches: u64,
}

/// Tracks lease-absence windows at control-loop resolution while the expensive
/// regional metrics scan remains on its normal 15-second cadence. Without this
/// rolling maximum, an outage that starts and ends between EMF samples is
/// invisible. The first observation is conservatively backdated by one sample
/// interval so a near-five-second outage cannot be reported as safely shorter.
struct PartitionOutageTracker {
    missing_since_ms: [Option<i64>; PARTITION_COUNT as usize],
    window_max_ms: u64,
}

impl Default for PartitionOutageTracker {
    fn default() -> Self {
        Self {
            missing_since_ms: array::from_fn(|_| None),
            window_max_ms: 0,
        }
    }
}

impl PartitionOutageTracker {
    fn observe(
        &mut self,
        now_ms: i64,
        assignment: Option<&crate::partition_assignment::AssignmentDocument>,
        leases: &[Option<Vec<u8>>],
    ) {
        for partition in 0..PARTITION_COUNT as usize {
            let desired = assignment
                .is_some_and(|document| document.owners.contains_key(&(partition as u32)));
            let missing = desired && leases.get(partition).is_none_or(Option::is_none);
            if missing {
                let since = self.missing_since_ms[partition]
                    .get_or_insert_with(|| now_ms.saturating_sub(OWNERSHIP_SAMPLE_INTERVAL_MS));
                self.window_max_ms = self
                    .window_max_ms
                    .max(now_ms.saturating_sub(*since).max(0) as u64);
            } else if let Some(since) = self.missing_since_ms[partition].take() {
                self.window_max_ms = self
                    .window_max_ms
                    .max(now_ms.saturating_sub(since).max(0) as u64);
            }
        }
    }

    fn take_window_max(&mut self, now_ms: i64) -> u64 {
        for since in self.missing_since_ms.iter().flatten() {
            self.window_max_ms = self
                .window_max_ms
                .max(now_ms.saturating_sub(*since).max(0) as u64);
        }
        std::mem::take(&mut self.window_max_ms)
    }
}

/// Starts a best-effort collector. One deterministic live task reports the
/// regional gauges while every task reports local health and counters, so
/// CloudWatch uses `Maximum` for regional gauges and `Sum` for counters.
/// Dimensions deliberately exclude partitions, users, and games.
pub fn spawn_resilience_metrics(
    redis: ConnectionManager,
    namespace: ClusterNamespace,
    lifecycle: LocalTaskLifecycle,
    server_id: u64,
    cancellation: CancellationToken,
) -> JoinHandle<()> {
    let environment =
        std::env::var("SNAKETRON_ENVIRONMENT").unwrap_or_else(|_| "development".to_string());
    let task_boot_id = lifecycle.task_boot_id().to_string();
    tokio::spawn(async move {
        let membership = match MembershipStore::new(
            redis.clone(),
            namespace.clone(),
            crate::cluster_membership::DEFAULT_MEMBERSHIP_TTL,
        ) {
            Ok(store) => store,
            Err(error) => {
                warn!(%error, "resilience metrics collector could not initialize");
                return;
            }
        };
        let assignment = AssignmentStore::new(redis.clone(), namespace.clone());
        let interval_secs = std::env::var("SNAKETRON_METRICS_INTERVAL_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_EMIT_INTERVAL_SECS)
            .max(1);
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut ownership_interval = tokio::time::interval(std::time::Duration::from_millis(
            OWNERSHIP_SAMPLE_INTERVAL_MS as u64,
        ));
        ownership_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut partition_outages = PartitionOutageTracker::default();

        loop {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    // Drain failures are often recorded immediately before the
                    // process-wide cancellation. Emit one bounded final sample
                    // so those counters are not silently lost on task exit.
                    let now_ms = chrono::Utc::now().timestamp_millis();
                    let mut gauges = match tokio::time::timeout(
                        std::time::Duration::from_secs(1),
                        collect_regional_gauges(
                            redis.clone(),
                            &namespace,
                            &membership,
                            &assignment,
                            server_id,
                            now_ms,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(gauges)) => gauges,
                        Ok(Err(error)) => {
                            warn!(%error, "final resilience metrics collection failed");
                            RegionalGauges::default()
                        }
                        Err(_) => {
                            warn!("final resilience metrics collection timed out");
                            RegionalGauges::default()
                        }
                    };
                    gauges.partition_unowned_ms = gauges
                        .partition_unowned_ms
                        .max(partition_outages.take_window_max(now_ms));
                    emit_emf(
                        &environment,
                        namespace.region(),
                        &task_boot_id,
                        lifecycle.is_ready(),
                        lifecycle.active_websockets() as u64,
                        gauges,
                        take_counter_snapshot(),
                        now_ms,
                    );
                    break;
                },
                _ = interval.tick() => {
                    let now_ms = chrono::Utc::now().timestamp_millis();
                    let result = collect_regional_gauges(
                        redis.clone(),
                        &namespace,
                        &membership,
                        &assignment,
                        server_id,
                        now_ms,
                    ).await;
                    let sampled_unowned_ms = partition_outages.take_window_max(now_ms);
                    match result {
                        Ok(mut gauges) => {
                            gauges.partition_unowned_ms = gauges
                                .partition_unowned_ms
                                .max(sampled_unowned_ms);
                            emit_emf(
                            &environment,
                            namespace.region(),
                            &task_boot_id,
                            lifecycle.is_ready(),
                            lifecycle.active_websockets() as u64,
                            gauges,
                            take_counter_snapshot(),
                            now_ms,
                            )
                        },
                        Err(error) => {
                            warn!(%error, "regional resilience metrics collection failed");
                            // Local health must remain observable even when
                            // the regional Valkey-backed gauges cannot be
                            // collected. In particular, emit LocalReady=0
                            // during a cache outage instead of relying only on
                            // CloudWatch missing-data behavior.
                            let gauges = RegionalGauges {
                                partition_unowned_ms: sampled_unowned_ms,
                                ..RegionalGauges::default()
                            };
                            emit_emf(
                                &environment,
                                namespace.region(),
                                &task_boot_id,
                                lifecycle.is_ready(),
                                lifecycle.active_websockets() as u64,
                                gauges,
                                take_counter_snapshot(),
                                now_ms,
                            );
                        },
                    }
                },
                _ = ownership_interval.tick() => {
                    let now_ms = chrono::Utc::now().timestamp_millis();
                    // The regular collector logs failures and emits local
                    // readiness. This fast observation is deliberately silent
                    // during Valkey outages so it cannot create a warning storm.
                    let _ = observe_partition_outages(
                        redis.clone(),
                        &namespace,
                        &assignment,
                        now_ms,
                        &mut partition_outages,
                    ).await;
                }
            }
        }
    })
}

async fn observe_partition_outages(
    mut redis: ConnectionManager,
    namespace: &ClusterNamespace,
    assignment_store: &AssignmentStore,
    now_ms: i64,
    tracker: &mut PartitionOutageTracker,
) -> Result<()> {
    let assignment = assignment_store.load().await?;
    let lease_keys: Vec<String> = (0..PARTITION_COUNT)
        .map(|partition| namespace.partition_lease(partition))
        .collect();
    let leases: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
        .arg(&lease_keys)
        .query_async(&mut redis)
        .await
        .context("failed to sample partition leases for outage timing")?;
    tracker.observe(now_ms, assignment.as_ref(), &leases);
    Ok(())
}

async fn collect_regional_gauges(
    mut redis: ConnectionManager,
    namespace: &ClusterNamespace,
    membership: &MembershipStore,
    assignment_store: &AssignmentStore,
    local_server_id: u64,
    now_ms: i64,
) -> Result<RegionalGauges> {
    let mut gauges = RegionalGauges::default();
    let members = membership.list_live(now_ms).await?;
    gauges.live_tasks = members.len() as u64;
    gauges.ready_tasks = members
        .iter()
        .filter(|member| member.is_assignment_eligible(now_ms))
        .count() as u64;
    gauges.draining_tasks = members
        .iter()
        .filter(|member| member.lifecycle == TaskLifecycle::Draining)
        .count() as u64;
    gauges.membership_age_ms = members
        .iter()
        .map(|member| now_ms.saturating_sub(member.heartbeat_at_ms).max(0) as u64)
        .max()
        .unwrap_or(0);

    // All tasks must emit their local counters and socket gauge, but only one
    // live task needs to download every recovery envelope for identical
    // regional gauges. Selecting the smallest membership identity is
    // deterministic and automatically hands collection to a survivor.
    let is_regional_reporter = members
        .iter()
        .min_by_key(|member| (member.server_id, member.boot_id.as_str()))
        .is_some_and(|member| member.server_id == local_server_id);
    if !is_regional_reporter {
        return Ok(gauges);
    }

    let assignment = assignment_store.load().await?;
    if let Some(assignment) = &assignment {
        gauges.assignment_version = assignment.version;
        gauges.assignment_age_ms = now_ms.saturating_sub(assignment.computed_at_ms).max(0) as u64;
        let mut owner_counts = std::collections::BTreeMap::<_, u64>::new();
        for owner in &assignment.eligible_members {
            owner_counts.insert(owner, 0);
        }
        for owner in assignment.owners.values() {
            *owner_counts.entry(owner).or_default() += 1;
        }
        if let (Some(min), Some(max)) = (owner_counts.values().min(), owner_counts.values().max()) {
            gauges.assignment_imbalance = max.saturating_sub(*min);
        }
    }

    let lease_keys: Vec<String> = (0..PARTITION_COUNT)
        .map(|partition| namespace.partition_lease(partition))
        .collect();
    let leases: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
        .arg(&lease_keys)
        .query_async(&mut redis)
        .await
        .context("failed to inspect partition leases for metrics")?;
    let (active_leases, lease_deficit, owner_mismatches) =
        summarize_partition_leases(assignment.as_ref(), &leases);
    gauges.active_partition_leases = active_leases;
    gauges.partition_lease_deficit = lease_deficit;
    gauges.partition_owner_mismatches = owner_mismatches;

    for partition in 0..PARTITION_COUNT {
        let stream_key = crate::redis_keys::RedisKeys::stream_commands(partition);
        let group = namespace.command_group(partition);
        match redis
            .xpending::<_, _, StreamPendingReply>(&stream_key, &group)
            .await
        {
            Ok(StreamPendingReply::Data(pending)) => {
                gauges.pending_commands =
                    gauges.pending_commands.saturating_add(pending.count as u64);
                if let Some((timestamp, _)) = pending.start_id.split_once('-')
                    && let Ok(timestamp) = timestamp.parse::<i64>()
                {
                    gauges.oldest_pending_command_ms = gauges
                        .oldest_pending_command_ms
                        .max(now_ms.saturating_sub(timestamp).max(0) as u64);
                }
            }
            Ok(StreamPendingReply::Empty) => {}
            Err(error) if error.to_string().contains("NOGROUP") => {}
            Err(error) => return Err(error).context("failed to inspect executor pending entries"),
        }
    }

    // These are bounded regional aggregates: one SCARD and one XLEN for each
    // of the fixed executor partitions, issued in a single pipeline. The
    // pending-completion set is the durable retry queue for external effects;
    // the quarantine stream is the durable terminal disposition for poison
    // commands. Neither requires scanning game- or user-labelled keys.
    let mut durability_pipeline = redis::pipe();
    for partition in 0..PARTITION_COUNT {
        durability_pipeline
            .cmd("SCARD")
            .arg(namespace.pending_completions(partition));
    }
    for partition in 0..PARTITION_COUNT {
        durability_pipeline
            .cmd("XLEN")
            .arg(namespace.command_quarantine(partition));
    }
    let durability_counts: Vec<u64> = durability_pipeline
        .query_async(&mut redis)
        .await
        .context("failed to inspect completion and quarantine durability queues")?;
    let partition_count = PARTITION_COUNT as usize;
    if durability_counts.len() != partition_count * 2 {
        anyhow::bail!("unexpected durability metrics pipeline response length");
    }
    gauges.pending_completions = durability_counts[..partition_count]
        .iter()
        .copied()
        .fold(0, u64::saturating_add);
    gauges.quarantined_commands = durability_counts[partition_count..]
        .iter()
        .copied()
        .fold(0, u64::saturating_add);

    let mut indexed_games = Vec::new();
    for partition in 0..PARTITION_COUNT {
        let game_ids: Vec<u32> = redis
            .smembers(namespace.active_games(partition))
            .await
            .context("failed to inspect active-game index")?;
        indexed_games.extend(game_ids.into_iter().map(|game_id| (partition, game_id)));
    }
    gauges.active_games = indexed_games.len() as u64;
    if !indexed_games.is_empty() {
        let keys: Vec<String> = indexed_games
            .iter()
            .map(|(_, game_id)| namespace.recovery(*game_id))
            .collect();
        let envelopes: Vec<Option<Vec<u8>>> = redis::cmd("MGET")
            .arg(&keys)
            .query_async(&mut redis)
            .await
            .context("failed to inspect active recovery checkpoints")?;
        for ((partition, game_id), payload) in indexed_games.into_iter().zip(envelopes) {
            let Some(payload) = payload else {
                gauges.active_game_index_mismatches += 1;
                continue;
            };
            gauges.checkpoint_bytes = gauges.checkpoint_bytes.max(payload.len() as u64);
            match serde_json::from_slice::<RecoveryEnvelopeV2>(&payload) {
                Ok(envelope)
                    if envelope.partition_id == partition && envelope.game_id == game_id =>
                {
                    gauges.checkpoint_age_ms = gauges
                        .checkpoint_age_ms
                        .max(now_ms.saturating_sub(envelope.checkpointed_at_ms).max(0) as u64);
                }
                _ => gauges.active_game_index_mismatches += 1,
            }
        }
    }
    Ok(gauges)
}

/// Summarizes the fixed partition lease set without emitting a partition or
/// owner label. A missing desired lease is a deficit; a present lease owned by
/// another boot (or a malformed/orphaned lease) is an ownership mismatch.
fn summarize_partition_leases(
    assignment: Option<&crate::partition_assignment::AssignmentDocument>,
    leases: &[Option<Vec<u8>>],
) -> (u64, u64, u64) {
    let active = leases.iter().filter(|lease| lease.is_some()).count() as u64;
    let Some(assignment) = assignment else {
        return (active, 0, 0);
    };

    let mut deficit = 0u64;
    let mut mismatches = 0u64;
    for partition in 0..PARTITION_COUNT as usize {
        let desired = assignment.owners.get(&(partition as u32));
        let lease = leases.get(partition).and_then(Option::as_deref);
        match (desired, lease) {
            (Some(_), None) => deficit += 1,
            (Some(desired), Some(lease)) if lease_owner(lease) != Some(desired.as_str()) => {
                mismatches += 1;
            }
            (None, Some(_)) => mismatches += 1,
            _ => {}
        }
    }
    (active, deficit, mismatches)
}

fn lease_owner(encoded: &[u8]) -> Option<&str> {
    let encoded = std::str::from_utf8(encoded).ok()?;
    let (boot_id, acquisition_id) = encoded.split_once(':')?;
    uuid::Uuid::parse_str(boot_id).ok()?;
    uuid::Uuid::parse_str(acquisition_id).ok()?;
    Some(boot_id)
}

fn metric(name: &str, unit: &str) -> Value {
    json!({ "Name": name, "Unit": unit })
}

#[allow(clippy::too_many_arguments)]
fn emit_emf(
    environment: &str,
    region: &str,
    task_boot_id: &str,
    local_ready: bool,
    active_websockets: u64,
    gauges: RegionalGauges,
    counters: CounterSnapshot,
    now_ms: i64,
) {
    let values = [
        ("ReadyTasks", gauges.ready_tasks, "Count"),
        ("LiveTasks", gauges.live_tasks, "Count"),
        ("DrainingTasks", gauges.draining_tasks, "Count"),
        ("MembershipAgeMs", gauges.membership_age_ms, "Milliseconds"),
        ("AssignmentVersion", gauges.assignment_version, "None"),
        ("AssignmentAgeMs", gauges.assignment_age_ms, "Milliseconds"),
        ("AssignmentImbalance", gauges.assignment_imbalance, "Count"),
        (
            "ActivePartitionLeases",
            gauges.active_partition_leases,
            "Count",
        ),
        (
            "PartitionLeaseDeficit",
            gauges.partition_lease_deficit,
            "Count",
        ),
        (
            "PartitionOwnerMismatches",
            gauges.partition_owner_mismatches,
            "Count",
        ),
        (
            "PartitionUnownedMs",
            gauges.partition_unowned_ms,
            "Milliseconds",
        ),
        ("PendingCommands", gauges.pending_commands, "Count"),
        ("PendingCompletions", gauges.pending_completions, "Count"),
        ("QuarantinedCommands", gauges.quarantined_commands, "Count"),
        (
            "OldestPendingCommandMs",
            gauges.oldest_pending_command_ms,
            "Milliseconds",
        ),
        ("CheckpointAgeMs", gauges.checkpoint_age_ms, "Milliseconds"),
        ("CheckpointBytes", gauges.checkpoint_bytes, "Bytes"),
        ("ActiveGames", gauges.active_games, "Count"),
        (
            "ActiveGameIndexMismatches",
            gauges.active_game_index_mismatches,
            "Count",
        ),
        (
            "FencedWriteRejections",
            counters.fenced_write_rejections,
            "Count",
        ),
        (
            "RecoveryFingerprintDivergences",
            counters.recovery_fingerprint_divergences,
            "Count",
        ),
        (
            "PlannedDrainFailures",
            counters.planned_drain_failures,
            "Count",
        ),
        ("CommandClaims", counters.command_claims, "Count"),
        ("CommandAcks", counters.command_acks, "Count"),
        ("CommandResends", counters.command_resends, "Count"),
        (
            "CommandDeduplications",
            counters.command_deduplications,
            "Count",
        ),
        ("CommandRejections", counters.command_rejections, "Count"),
        ("CheckpointWrites", counters.checkpoint_writes, "Count"),
        ("CheckpointFailures", counters.checkpoint_failures, "Count"),
        ("RecoveredGames", counters.recovered_games, "Count"),
        ("RecoveryReplays", counters.recovery_replays, "Count"),
        (
            "MatchClaimConflicts",
            counters.match_claim_conflicts,
            "Count",
        ),
        (
            "DuplicateCompletionEffectsPrevented",
            counters.duplicate_completion_effects_prevented,
            "Count",
        ),
        ("LocalReady", u64::from(local_ready), "Count"),
        ("ActiveWebSockets", active_websockets, "Count"),
    ];
    let definitions: Vec<Value> = values
        .iter()
        .map(|(name, _, unit)| metric(name, unit))
        .collect();
    let mut document = Map::new();
    document.insert("Environment".into(), json!(environment));
    document.insert("Region".into(), json!(region));
    document.insert("TaskBootId".into(), json!(task_boot_id));
    for (name, value, _) in values {
        document.insert(name.into(), json!(value));
    }
    document.insert(
        "_aws".into(),
        json!({
            "Timestamp": now_ms,
            "CloudWatchMetrics": [{
                "Namespace": EMF_NAMESPACE,
                "Dimensions": [
                    ["Environment"],
                    ["Environment", "Region", "TaskBootId"]
                ],
                "Metrics": definitions
            }]
        }),
    );
    println!("{}", Value::Object(document));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_membership::BootIdentity;
    use crate::partition_assignment::{ASSIGNMENT_SCHEMA_VERSION, AssignmentDocument};
    use std::collections::BTreeMap;

    #[test]
    fn public_counter_recorders_are_non_blocking() {
        // Process-global counters may have been exercised by another unit test.
        // Establish a clean baseline before checking this recorder contract.
        let _ = take_counter_snapshot();
        record_fenced_write_rejection(1);
        record_checkpoint_writes(2);
        let snapshot = take_counter_snapshot();
        assert_eq!(snapshot.fenced_write_rejections, 1);
        assert_eq!(snapshot.checkpoint_writes, 2);
        assert_eq!(take_counter_snapshot().checkpoint_writes, 0);
    }

    #[test]
    fn partition_lease_summary_separates_deficits_from_wrong_owners() -> Result<()> {
        let owner = BootIdentity::parse("11111111-1111-4111-8111-111111111111")?;
        let other = BootIdentity::parse("22222222-2222-4222-8222-222222222222")?;
        let acquisition = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
        let owners = (0..PARTITION_COUNT)
            .map(|partition| (partition, owner.clone()))
            .collect::<BTreeMap<_, _>>();
        let assignment = AssignmentDocument {
            schema_version: ASSIGNMENT_SCHEMA_VERSION,
            version: 7,
            region: "test".into(),
            computed_at_ms: 1,
            eligible_members: vec![owner.clone()],
            owners,
        };
        let mut leases = (0..PARTITION_COUNT)
            .map(|_| Some(format!("{owner}:{acquisition}").into_bytes()))
            .collect::<Vec<_>>();
        leases[1] = None;
        leases[2] = Some(format!("{other}:{acquisition}").into_bytes());
        leases[3] = Some(format!("{owner}:malformed").into_bytes());

        assert_eq!(
            summarize_partition_leases(Some(&assignment), &leases),
            (9, 1, 2),
        );
        assert_eq!(summarize_partition_leases(None, &leases), (9, 0, 0));
        Ok(())
    }

    #[test]
    fn partition_outage_max_survives_restoration_between_emf_samples() -> Result<()> {
        let owner = BootIdentity::parse("11111111-1111-4111-8111-111111111111")?;
        let acquisition = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
        let assignment = AssignmentDocument {
            schema_version: ASSIGNMENT_SCHEMA_VERSION,
            version: 1,
            region: "test".into(),
            computed_at_ms: 1,
            eligible_members: vec![owner.clone()],
            owners: (0..PARTITION_COUNT)
                .map(|partition| (partition, owner.clone()))
                .collect(),
        };
        let mut leases = (0..PARTITION_COUNT)
            .map(|_| Some(format!("{owner}:{acquisition}").into_bytes()))
            .collect::<Vec<_>>();
        let mut tracker = PartitionOutageTracker::default();

        tracker.observe(1_000, Some(&assignment), &leases);
        assert_eq!(tracker.take_window_max(1_000), 0);

        leases[0] = None;
        tracker.observe(1_500, Some(&assignment), &leases);
        tracker.observe(5_500, Some(&assignment), &leases);
        assert_eq!(tracker.take_window_max(5_500), 4_500);

        // Restoration occurs before the next 15-second EMF emission. The
        // completed duration is retained rather than reset to zero.
        leases[0] = Some(format!("{owner}:{acquisition}").into_bytes());
        tracker.observe(6_000, Some(&assignment), &leases);
        assert_eq!(tracker.take_window_max(15_000), 5_000);
        assert_eq!(tracker.take_window_max(15_000), 0);
        Ok(())
    }
}
