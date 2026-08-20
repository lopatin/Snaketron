//! Bounded-cardinality resilience telemetry exported through OpenTelemetry and
//! mirrored to CloudWatch EMF as an independent AWS-native alarm backstop.
//!
//! Correctness must not depend on metrics. Collection therefore uses its own
//! best-effort loop and never changes liveness or lease state when CloudWatch,
//! Valkey, or stdout is unavailable.
//!
//! Count/sum/max components use independent atomics. A request concurrent with
//! the interval snapshot can split those components across adjacent windows,
//! so dashboard averages derived from them are approximate over short windows.

use crate::cluster_membership::{
    ClusterNamespace, EXECUTOR_PROTOCOL_VERSION, MembershipStore, TaskLifecycle,
};
use crate::game_executor::PARTITION_COUNT;
use crate::lifecycle::TaskLifecycle as LocalTaskLifecycle;
use crate::partition_assignment::AssignmentStore;
use crate::recovery::RECOVERY_SCHEMA_VERSION;
use crate::redis_utils::RedisConnection;
use anyhow::{Context, Result};
use common::{BoostLifecycleTransition, GameState, GameType, QueueMode};
use redis::AsyncCommands;
use redis::streams::StreamPendingReply;
use serde_json::{Map, Value, json};
use std::array;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

const PRODUCTION_EMF_NAMESPACE: &str = "Snaketron/Operational";
const NON_PRODUCTION_EMF_NAMESPACE: &str = "Snaketron/OperationalDev";
const DEFAULT_EMIT_INTERVAL_SECS: u64 = 5;
const OWNERSHIP_SAMPLE_INTERVAL_MS: i64 = 500;
// The exact regional scan performs roughly sixty independently routed cluster
// operations at the certification cardinality. Keep a finite deadline, but
// allow ordinary Serverless Valkey network and task-scheduling tails.
const REGIONAL_COLLECTION_TIMEOUT_MS: u64 = 2_000;
const RECOVERY_METADATA_BATCH_SIZE: usize = 32;
const RECOVERY_TAIL_SAMPLE_BYTES: i64 = 512;
const CHECKPOINTED_AT_MARKER: &[u8] = b"\"checkpointed_at_ms\":";
const SOURCE_LEASE_TOKEN_MARKER: &[u8] = b"\"source_lease_token\":\"";
const MATCHMAKING_QUEUE_COUNT: usize = crate::matchmaking_manager::MATCHMAKING_GAME_TYPES.len()
    * crate::matchmaking_manager::MATCHMAKING_QUEUE_MODES.len()
    * crate::matchmaking_pool::MatchmakingPool::ALL.len();

#[derive(Default)]
struct Counters {
    fenced_write_rejections: AtomicU64,
    planned_drain_failures: AtomicU64,
    command_claims: AtomicU64,
    command_acks: AtomicU64,
    command_resends: AtomicU64,
    command_deduplications: AtomicU64,
    command_rejections: AtomicU64,
    boost_packet_collections: AtomicU64,
    boost_pad_respawns: AtomicU64,
    boost_activation_attempts: AtomicU64,
    boost_activation_commands_scheduled: AtomicU64,
    boost_activation_command_rejections: AtomicU64,
    boost_activations: AtomicU64,
    boost_manual_stops: AtomicU64,
    boost_depletions: AtomicU64,
    combo_food_collections: AtomicU64,
    combo_points_awarded: AtomicU64,
    game_actor_advances: AtomicU64,
    game_actor_batch_quanta_sum: AtomicU64,
    game_actor_batch_quanta_max: AtomicU64,
    game_actor_lag_ms_sum: AtomicU64,
    game_actor_lag_ms_max: AtomicU64,
    game_actor_advance_duration_us_sum: AtomicU64,
    game_actor_advance_duration_us_max: AtomicU64,
    checkpoint_writes: AtomicU64,
    checkpoint_failures: AtomicU64,
    recovered_games: AtomicU64,
    recovery_replays: AtomicU64,
    match_claim_conflicts: AtomicU64,
    duplicate_completion_effects_prevented: AtomicU64,
    http_requests: AtomicU64,
    http_responses_4xx: AtomicU64,
    http_responses_5xx: AtomicU64,
    http_request_latency_ms_sum: AtomicU64,
    http_request_latency_ms_max: AtomicU64,
    websocket_opens: AtomicU64,
    websocket_closes: AtomicU64,
    websocket_rejected_upgrades: AtomicU64,
    websocket_inbound_messages: AtomicU64,
    websocket_inbound_bytes: AtomicU64,
    websocket_outbound_messages: AtomicU64,
    websocket_outbound_bytes: AtomicU64,
    websocket_malformed_messages: AtomicU64,
    websocket_process_errors: AtomicU64,
    websocket_send_errors: AtomicU64,
    websocket_transport_errors: AtomicU64,
    websocket_session_duration_ms_sum: AtomicU64,
    websocket_session_duration_ms_max: AtomicU64,
    websocket_resync_requests: AtomicU64,
    websocket_resync_accepted: AtomicU64,
    websocket_resync_rejected: AtomicU64,
    matchmaking_admissions: AtomicU64,
    matchmaking_admission_deduplications: AtomicU64,
    matchmaking_admission_rejections: AtomicU64,
    matchmaking_commits: AtomicU64,
    matchmaking_wait_ms_sum: AtomicU64,
    matchmaking_wait_ms_max: AtomicU64,
    matchmaking_matched_players: AtomicU64,
    matchmaking_matched_lobbies: AtomicU64,
    matchmaking_errors: AtomicU64,
    matchmaking_integrity_errors: AtomicU64,
    game_created_outbox_delivery_errors: AtomicU64,
    games_completed: AtomicU64,
    game_duration_ms_sum: AtomicU64,
    game_duration_ms_max: AtomicU64,
    completed_game_players: AtomicU64,
    potg_ring_truncated: AtomicU64,
    ring_evicted_seconds_sum: AtomicU64,
    ring_evicted_seconds_max: AtomicU64,
    redis_requests: AtomicU64,
    redis_errors: AtomicU64,
    redis_request_latency_ms_sum: AtomicU64,
    redis_request_latency_ms_max: AtomicU64,
}

static COUNTERS: OnceLock<Counters> = OnceLock::new();

fn counters() -> &'static Counters {
    COUNTERS.get_or_init(Counters::default)
}

macro_rules! counter_fn {
    ($name:ident, $field:ident) => {
        pub fn $name(count: u64) {
            counters().$field.fetch_add(count, Ordering::Relaxed);
            crate::otel_metrics::$name(count);
        }
    };
}

counter_fn!(record_fenced_write_rejection, fenced_write_rejections);
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BoostMetricDimensions {
    game_type: &'static str,
    queue_mode: &'static str,
    team_side: &'static str,
    speed_band: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ComboMetricDimensions {
    game_type: &'static str,
    queue_mode: &'static str,
    team_side: &'static str,
}

fn combo_metric_dimensions(state: &GameState, snake_id: u32) -> ComboMetricDimensions {
    let game_type = match &state.game_type {
        GameType::TeamMatch { per_team: 1 } => "duel",
        GameType::TeamMatch { per_team: 2 } => "2v2",
        GameType::TeamMatch { .. } => "other-team",
        GameType::Solo => "solo",
        GameType::FreeForAll { .. } => "free-for-all",
        GameType::Custom { .. } => "custom",
    };
    let queue_mode = match &state.queue_mode {
        QueueMode::Quickmatch => "quickmatch",
        QueueMode::Competitive => "competitive",
    };
    let team_side = match state
        .arena
        .snakes
        .get(snake_id as usize)
        .and_then(|snake| snake.team_id)
        .map(|team| team.0)
    {
        Some(0) => "team-0",
        Some(1) => "team-1",
        _ => "unknown",
    };

    ComboMetricDimensions {
        game_type,
        queue_mode,
        team_side,
    }
}

fn boost_metric_dimensions(state: &GameState, snake_id: u32) -> BoostMetricDimensions {
    let game_type = match &state.game_type {
        GameType::TeamMatch { per_team: 1 } => "duel",
        GameType::TeamMatch { per_team: 2 } => "2v2",
        GameType::TeamMatch { .. } => "other-team",
        GameType::Solo => "solo",
        GameType::FreeForAll { .. } => "free-for-all",
        GameType::Custom { .. } => "custom",
    };
    let queue_mode = match &state.queue_mode {
        QueueMode::Quickmatch => "quickmatch",
        QueueMode::Competitive => "competitive",
    };
    let team_side = match state
        .arena
        .snakes
        .get(snake_id as usize)
        .and_then(|snake| snake.team_id)
        .map(|team| team.0)
    {
        Some(0) => "team-0",
        Some(1) => "team-1",
        _ => "unknown",
    };
    let speed_band = match state
        .properties
        .boost
        .as_ref()
        .map(|config| config.speed_milli)
    {
        Some(1_000) => "1.00x",
        Some(1_001..=1_250) => "1.01-1.25x",
        Some(1_251..=1_500) => "1.26-1.50x",
        Some(1_501..=1_750) => "1.51-1.75x",
        Some(1_751..=2_000) => "1.76-2.00x",
        _ => "unsupported",
    };

    BoostMetricDimensions {
        game_type,
        queue_mode,
        team_side,
        speed_band,
    }
}

/// Record one authoritative packet event after it has been durably published.
/// Dimensions are deliberately selected from finite enums/bands; game, user,
/// command, and snake identifiers are never exported as labels.
pub fn record_boost_packet_collected(state: &GameState, snake_id: u32, pad_id: u8) {
    counters()
        .boost_packet_collections
        .fetch_add(1, Ordering::Relaxed);
    let dimensions = boost_metric_dimensions(state, snake_id);
    crate::otel_metrics::record_boost_packet_collected(
        dimensions.game_type,
        dimensions.queue_mode,
        dimensions.team_side,
        dimensions.speed_band,
        pad_id,
    );
}

/// Record an absolute pad-cooldown transition observed while advancing an
/// authoritative actor. A fixed layout bounds `pad_id` cardinality.
pub fn record_boost_pad_respawned(state: &GameState, pad_id: u8) {
    counters()
        .boost_pad_respawns
        .fetch_add(1, Ordering::Relaxed);
    let dimensions = boost_metric_dimensions(state, u32::MAX);
    crate::otel_metrics::record_boost_pad_respawned(
        dimensions.game_type,
        dimensions.queue_mode,
        dimensions.speed_band,
        pad_id,
    );
}

/// Count a new (non-retry) activation command at the executor decision point.
pub fn record_boost_activation_attempt(state: &GameState, snake_id: u32) {
    counters()
        .boost_activation_attempts
        .fetch_add(1, Ordering::Relaxed);
    let dimensions = boost_metric_dimensions(state, snake_id);
    crate::otel_metrics::record_boost_activation_attempt(
        dimensions.game_type,
        dimensions.queue_mode,
        dimensions.team_side,
        dimensions.speed_band,
    );
}

/// Count the server decision for a new activation command. "Scheduled" is
/// intentionally distinct from "activated": gameplay-invalid activation is
/// a deterministic scheduled no-op. Lifecycle metrics are recorded separately
/// only when the authoritative simulation changes snake Boost state.
pub fn record_boost_activation_decision(state: &GameState, snake_id: u32, scheduled: bool) {
    let dimensions = boost_metric_dimensions(state, snake_id);
    if scheduled {
        counters()
            .boost_activation_commands_scheduled
            .fetch_add(1, Ordering::Relaxed);
    } else {
        counters()
            .boost_activation_command_rejections
            .fetch_add(1, Ordering::Relaxed);
    }
    crate::otel_metrics::record_boost_activation_decision(
        dimensions.game_type,
        dimensions.queue_mode,
        dimensions.team_side,
        dimensions.speed_band,
        scheduled,
    );
}

/// Record an exact lifecycle transition emitted by an authoritative simulation
/// quantum. Unlike command scheduling telemetry, this cannot count a no-op
/// Space command as an activation.
pub fn record_boost_lifecycle_transition(state: &GameState, transition: BoostLifecycleTransition) {
    let snake_id = match transition {
        BoostLifecycleTransition::Activated { snake_id }
        | BoostLifecycleTransition::ManuallyStopped { snake_id }
        | BoostLifecycleTransition::Depleted { snake_id } => snake_id,
    };
    let dimensions = boost_metric_dimensions(state, snake_id);
    match transition {
        BoostLifecycleTransition::Activated { .. } => {
            counters().boost_activations.fetch_add(1, Ordering::Relaxed);
            crate::otel_metrics::record_boost_lifecycle_transition(
                dimensions.game_type,
                dimensions.queue_mode,
                dimensions.team_side,
                dimensions.speed_band,
                true,
            );
        }
        BoostLifecycleTransition::ManuallyStopped { .. } => {
            counters()
                .boost_manual_stops
                .fetch_add(1, Ordering::Relaxed);
            crate::otel_metrics::record_boost_manual_stop(
                dimensions.game_type,
                dimensions.queue_mode,
                dimensions.team_side,
                dimensions.speed_band,
            );
        }
        BoostLifecycleTransition::Depleted { .. } => {
            counters().boost_depletions.fetch_add(1, Ordering::Relaxed);
            crate::otel_metrics::record_boost_lifecycle_transition(
                dimensions.game_type,
                dimensions.queue_mode,
                dimensions.team_side,
                dimensions.speed_band,
                false,
            );
        }
    }
}

/// Record one authoritative food collection after its event, catch-up
/// checkpoint, or terminal snapshot has crossed a fenced durability boundary.
/// All labels come from finite enums or finite value buckets; no game, user,
/// player, command, or snake identifier is exported.
pub fn record_combo_food_collected(
    state: &GameState,
    snake_id: u32,
    points: u32,
    combo_chain: u32,
    combo_remaining_ms_before: u32,
    boost_active: bool,
) {
    counters()
        .combo_food_collections
        .fetch_add(1, Ordering::Relaxed);
    counters()
        .combo_points_awarded
        .fetch_add(u64::from(points), Ordering::Relaxed);
    let dimensions = combo_metric_dimensions(state, snake_id);
    crate::otel_metrics::record_combo_food_collected(
        dimensions.game_type,
        dimensions.queue_mode,
        dimensions.team_side,
        points,
        combo_chain,
        combo_remaining_ms_before,
        boost_active,
    );
}

counter_fn!(record_websocket_opened, websocket_opens);
counter_fn!(record_websocket_closed, websocket_closes);
counter_fn!(
    record_websocket_rejected_upgrade,
    websocket_rejected_upgrades
);
counter_fn!(
    record_websocket_malformed_message,
    websocket_malformed_messages
);
counter_fn!(record_websocket_process_error, websocket_process_errors);
counter_fn!(record_websocket_send_error, websocket_send_errors);
counter_fn!(record_websocket_transport_error, websocket_transport_errors);
counter_fn!(record_websocket_resync_requested, websocket_resync_requests);
counter_fn!(record_websocket_resync_accepted, websocket_resync_accepted);
counter_fn!(record_websocket_resync_rejected, websocket_resync_rejected);
counter_fn!(record_matchmaking_admission, matchmaking_admissions);
counter_fn!(
    record_matchmaking_admission_deduplication,
    matchmaking_admission_deduplications
);
counter_fn!(
    record_matchmaking_admission_rejection,
    matchmaking_admission_rejections
);
counter_fn!(record_matchmaking_error, matchmaking_errors);
counter_fn!(
    record_matchmaking_integrity_error,
    matchmaking_integrity_errors
);
counter_fn!(
    record_game_created_outbox_delivery_error,
    game_created_outbox_delivery_errors
);

fn saturating_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn duration_us(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn record_sum_and_max(sum: &AtomicU64, max: &AtomicU64, value: u64) {
    sum.fetch_add(value, Ordering::Relaxed);
    max.fetch_max(value, Ordering::Relaxed);
}

/// Record one authoritative actor simulation pass. All values are
/// attribute-free and bounded-cardinality so they remain safe at game scale.
pub fn record_game_actor_advance(batch_quanta: u32, lag_ms: u64, duration: Duration) {
    let counters = counters();
    let batch_quanta = u64::from(batch_quanta);
    let advance_duration_us = duration_us(duration);
    counters.game_actor_advances.fetch_add(1, Ordering::Relaxed);
    record_sum_and_max(
        &counters.game_actor_batch_quanta_sum,
        &counters.game_actor_batch_quanta_max,
        batch_quanta,
    );
    record_sum_and_max(
        &counters.game_actor_lag_ms_sum,
        &counters.game_actor_lag_ms_max,
        lag_ms,
    );
    record_sum_and_max(
        &counters.game_actor_advance_duration_us_sum,
        &counters.game_actor_advance_duration_us_max,
        advance_duration_us,
    );
    crate::otel_metrics::record_game_actor_advance(advance_duration_us, batch_quanta, lag_ms);
}

pub fn record_http_request(status_code: u16, latency: Duration) {
    let counters = counters();
    let latency_ms = duration_ms(latency);
    counters.http_requests.fetch_add(1, Ordering::Relaxed);
    if (400..500).contains(&status_code) {
        counters.http_responses_4xx.fetch_add(1, Ordering::Relaxed);
    } else if status_code >= 500 {
        counters.http_responses_5xx.fetch_add(1, Ordering::Relaxed);
    }
    record_sum_and_max(
        &counters.http_request_latency_ms_sum,
        &counters.http_request_latency_ms_max,
        latency_ms,
    );
    crate::otel_metrics::record_http_request(status_code, latency_ms);
}

pub fn record_websocket_inbound_message(bytes: usize) {
    let counters = counters();
    let bytes = saturating_u64(bytes);
    counters
        .websocket_inbound_messages
        .fetch_add(1, Ordering::Relaxed);
    counters
        .websocket_inbound_bytes
        .fetch_add(bytes, Ordering::Relaxed);
    crate::otel_metrics::record_websocket_inbound_message(bytes);
}

pub fn record_websocket_outbound_message(bytes: usize) {
    let counters = counters();
    let bytes = saturating_u64(bytes);
    counters
        .websocket_outbound_messages
        .fetch_add(1, Ordering::Relaxed);
    counters
        .websocket_outbound_bytes
        .fetch_add(bytes, Ordering::Relaxed);
    crate::otel_metrics::record_websocket_outbound_message(bytes);
}

pub fn record_websocket_session(duration: Duration) {
    let counters = counters();
    let duration_ms = duration_ms(duration);
    record_sum_and_max(
        &counters.websocket_session_duration_ms_sum,
        &counters.websocket_session_duration_ms_max,
        duration_ms,
    );
    crate::otel_metrics::record_websocket_session(duration_ms);
}

pub fn record_matchmaking_commit(wait_ms: u64, players: usize, lobbies: usize) {
    // Only a newly committed response observed by this process reaches here.
    // An ambiguous success recovered as AlreadyCommitted is omitted rather
    // than risking double-counting the match and its players.
    let counters = counters();
    counters.matchmaking_commits.fetch_add(1, Ordering::Relaxed);
    let players = saturating_u64(players);
    let lobbies = saturating_u64(lobbies);
    counters
        .matchmaking_matched_players
        .fetch_add(players, Ordering::Relaxed);
    counters
        .matchmaking_matched_lobbies
        .fetch_add(lobbies, Ordering::Relaxed);
    record_sum_and_max(
        &counters.matchmaking_wait_ms_sum,
        &counters.matchmaking_wait_ms_max,
        wait_ms,
    );
    crate::otel_metrics::record_matchmaking_commit(wait_ms, players, lobbies);
}

pub fn record_game_completed(duration_ms: u64, players: usize) {
    // Only a newly committed response observed by this process reaches here.
    // Recovery of an ambiguous success remains uncounted because completion
    // does not carry a durable telemetry marker.
    let counters = counters();
    counters.games_completed.fetch_add(1, Ordering::Relaxed);
    let players = saturating_u64(players);
    counters
        .completed_game_players
        .fetch_add(players, Ordering::Relaxed);
    record_sum_and_max(
        &counters.game_duration_ms_sum,
        &counters.game_duration_ms_max,
        duration_ms,
    );
    crate::otel_metrics::record_game_completed(duration_ms, players);
}

/// Completion-level telemetry for the bounded PotG scorer view. Emitting once
/// per affected game makes this directly usable as a truncation incidence
/// rate, independent of how many individual entries were evicted.
pub fn record_potg_ring_truncated(evicted_seconds: u64) {
    let counters = counters();
    counters.potg_ring_truncated.fetch_add(1, Ordering::Relaxed);
    record_sum_and_max(
        &counters.ring_evicted_seconds_sum,
        &counters.ring_evicted_seconds_max,
        evicted_seconds,
    );
    crate::otel_metrics::record_potg_ring_truncated(evicted_seconds);
}

/// Announces that this task is contending for a hosted service's exclusion
/// lease, which makes its leadership gauge report zero instead of nothing.
///
/// Only one task in the fleet is elected, so a fleet-average p99 dilutes that
/// task's contribution by the fleet size. Splitting the gameplay-latency series
/// by leadership needs both halves of the split to exist, and the losing tasks
/// are the half that would otherwise be missing.
///
/// This pair is OpenTelemetry-only, unlike the rest of this module. The
/// comparison it serves needs a percentile, and the EMF mirror carries the
/// actor-advance duration as `GameActorAdvanceDurationUsSum`/`Max` on fleet-wide
/// dimensions — so an EMF copy of this signal would have nothing to split.
pub fn record_hosted_service_contention(name: &str) {
    crate::otel_metrics::register_hosted_service_lease(name);
}

/// Marks this task as an elected holder of `name`'s exclusion lease for exactly
/// as long as the returned guard lives.
///
/// Leadership ends through several paths that are easy to miss individually — a
/// failed build, a lost renewal, a disabled service, an unwind — so this is a
/// guard rather than a paired clear call. A missed clear would pin the gauge at
/// one on a task that is no longer elected, which silently reverses the
/// comparison it exists to support.
#[must_use = "leadership lasts exactly as long as this guard"]
pub fn record_hosted_service_election(name: &str) -> HostedServiceElection {
    crate::otel_metrics::hosted_service_lease_acquired(name);
    // Owned, because the guard outlives the borrow of the factory it came from
    // and the release has to name the same series the acquire did.
    HostedServiceElection {
        name: name.to_owned(),
    }
}

/// Clears one hosted service's elected-task signal when dropped.
pub struct HostedServiceElection {
    name: String,
}

impl Drop for HostedServiceElection {
    fn drop(&mut self) {
        crate::otel_metrics::hosted_service_lease_released(&self.name);
    }
}

pub fn record_redis_request(latency: Duration, failed: bool) {
    let counters = counters();
    let latency_ms = duration_ms(latency);
    counters.redis_requests.fetch_add(1, Ordering::Relaxed);
    if failed {
        counters.redis_errors.fetch_add(1, Ordering::Relaxed);
    }
    record_sum_and_max(
        &counters.redis_request_latency_ms_sum,
        &counters.redis_request_latency_ms_max,
        latency_ms,
    );
    crate::otel_metrics::record_redis_request(latency_ms, failed);
}

#[derive(Default)]
struct CounterSnapshot {
    fenced_write_rejections: u64,
    planned_drain_failures: u64,
    command_claims: u64,
    command_acks: u64,
    command_resends: u64,
    command_deduplications: u64,
    command_rejections: u64,
    boost_packet_collections: u64,
    boost_pad_respawns: u64,
    boost_activation_attempts: u64,
    boost_activation_commands_scheduled: u64,
    boost_activation_command_rejections: u64,
    boost_activations: u64,
    boost_manual_stops: u64,
    boost_depletions: u64,
    combo_food_collections: u64,
    combo_points_awarded: u64,
    game_actor_advances: u64,
    game_actor_batch_quanta_sum: u64,
    game_actor_batch_quanta_max: u64,
    game_actor_lag_ms_sum: u64,
    game_actor_lag_ms_max: u64,
    game_actor_advance_duration_us_sum: u64,
    game_actor_advance_duration_us_max: u64,
    checkpoint_writes: u64,
    checkpoint_failures: u64,
    recovered_games: u64,
    recovery_replays: u64,
    match_claim_conflicts: u64,
    duplicate_completion_effects_prevented: u64,
    http_requests: u64,
    http_responses_4xx: u64,
    http_responses_5xx: u64,
    http_request_latency_ms_sum: u64,
    http_request_latency_ms_max: u64,
    websocket_opens: u64,
    websocket_closes: u64,
    websocket_rejected_upgrades: u64,
    websocket_inbound_messages: u64,
    websocket_inbound_bytes: u64,
    websocket_outbound_messages: u64,
    websocket_outbound_bytes: u64,
    websocket_malformed_messages: u64,
    websocket_process_errors: u64,
    websocket_send_errors: u64,
    websocket_transport_errors: u64,
    websocket_session_duration_ms_sum: u64,
    websocket_session_duration_ms_max: u64,
    websocket_resync_requests: u64,
    websocket_resync_accepted: u64,
    websocket_resync_rejected: u64,
    matchmaking_admissions: u64,
    matchmaking_admission_deduplications: u64,
    matchmaking_admission_rejections: u64,
    matchmaking_commits: u64,
    matchmaking_wait_ms_sum: u64,
    matchmaking_wait_ms_max: u64,
    matchmaking_matched_players: u64,
    matchmaking_matched_lobbies: u64,
    matchmaking_errors: u64,
    matchmaking_integrity_errors: u64,
    game_created_outbox_delivery_errors: u64,
    games_completed: u64,
    game_duration_ms_sum: u64,
    game_duration_ms_max: u64,
    completed_game_players: u64,
    potg_ring_truncated: u64,
    ring_evicted_seconds_sum: u64,
    ring_evicted_seconds_max: u64,
    redis_requests: u64,
    redis_errors: u64,
    redis_request_latency_ms_sum: u64,
    redis_request_latency_ms_max: u64,
}

fn take_counter_snapshot() -> CounterSnapshot {
    let counters = counters();
    CounterSnapshot {
        fenced_write_rejections: counters.fenced_write_rejections.swap(0, Ordering::Relaxed),
        planned_drain_failures: counters.planned_drain_failures.swap(0, Ordering::Relaxed),
        command_claims: counters.command_claims.swap(0, Ordering::Relaxed),
        command_acks: counters.command_acks.swap(0, Ordering::Relaxed),
        command_resends: counters.command_resends.swap(0, Ordering::Relaxed),
        command_deduplications: counters.command_deduplications.swap(0, Ordering::Relaxed),
        command_rejections: counters.command_rejections.swap(0, Ordering::Relaxed),
        boost_packet_collections: counters.boost_packet_collections.swap(0, Ordering::Relaxed),
        boost_pad_respawns: counters.boost_pad_respawns.swap(0, Ordering::Relaxed),
        boost_activation_attempts: counters
            .boost_activation_attempts
            .swap(0, Ordering::Relaxed),
        boost_activation_commands_scheduled: counters
            .boost_activation_commands_scheduled
            .swap(0, Ordering::Relaxed),
        boost_activation_command_rejections: counters
            .boost_activation_command_rejections
            .swap(0, Ordering::Relaxed),
        boost_activations: counters.boost_activations.swap(0, Ordering::Relaxed),
        boost_manual_stops: counters.boost_manual_stops.swap(0, Ordering::Relaxed),
        boost_depletions: counters.boost_depletions.swap(0, Ordering::Relaxed),
        combo_food_collections: counters.combo_food_collections.swap(0, Ordering::Relaxed),
        combo_points_awarded: counters.combo_points_awarded.swap(0, Ordering::Relaxed),
        game_actor_advances: counters.game_actor_advances.swap(0, Ordering::Relaxed),
        game_actor_batch_quanta_sum: counters
            .game_actor_batch_quanta_sum
            .swap(0, Ordering::Relaxed),
        game_actor_batch_quanta_max: counters
            .game_actor_batch_quanta_max
            .swap(0, Ordering::Relaxed),
        game_actor_lag_ms_sum: counters.game_actor_lag_ms_sum.swap(0, Ordering::Relaxed),
        game_actor_lag_ms_max: counters.game_actor_lag_ms_max.swap(0, Ordering::Relaxed),
        game_actor_advance_duration_us_sum: counters
            .game_actor_advance_duration_us_sum
            .swap(0, Ordering::Relaxed),
        game_actor_advance_duration_us_max: counters
            .game_actor_advance_duration_us_max
            .swap(0, Ordering::Relaxed),
        checkpoint_writes: counters.checkpoint_writes.swap(0, Ordering::Relaxed),
        checkpoint_failures: counters.checkpoint_failures.swap(0, Ordering::Relaxed),
        recovered_games: counters.recovered_games.swap(0, Ordering::Relaxed),
        recovery_replays: counters.recovery_replays.swap(0, Ordering::Relaxed),
        match_claim_conflicts: counters.match_claim_conflicts.swap(0, Ordering::Relaxed),
        duplicate_completion_effects_prevented: counters
            .duplicate_completion_effects_prevented
            .swap(0, Ordering::Relaxed),
        http_requests: counters.http_requests.swap(0, Ordering::Relaxed),
        http_responses_4xx: counters.http_responses_4xx.swap(0, Ordering::Relaxed),
        http_responses_5xx: counters.http_responses_5xx.swap(0, Ordering::Relaxed),
        http_request_latency_ms_sum: counters
            .http_request_latency_ms_sum
            .swap(0, Ordering::Relaxed),
        http_request_latency_ms_max: counters
            .http_request_latency_ms_max
            .swap(0, Ordering::Relaxed),
        websocket_opens: counters.websocket_opens.swap(0, Ordering::Relaxed),
        websocket_closes: counters.websocket_closes.swap(0, Ordering::Relaxed),
        websocket_rejected_upgrades: counters
            .websocket_rejected_upgrades
            .swap(0, Ordering::Relaxed),
        websocket_inbound_messages: counters
            .websocket_inbound_messages
            .swap(0, Ordering::Relaxed),
        websocket_inbound_bytes: counters.websocket_inbound_bytes.swap(0, Ordering::Relaxed),
        websocket_outbound_messages: counters
            .websocket_outbound_messages
            .swap(0, Ordering::Relaxed),
        websocket_outbound_bytes: counters.websocket_outbound_bytes.swap(0, Ordering::Relaxed),
        websocket_malformed_messages: counters
            .websocket_malformed_messages
            .swap(0, Ordering::Relaxed),
        websocket_process_errors: counters.websocket_process_errors.swap(0, Ordering::Relaxed),
        websocket_send_errors: counters.websocket_send_errors.swap(0, Ordering::Relaxed),
        websocket_transport_errors: counters
            .websocket_transport_errors
            .swap(0, Ordering::Relaxed),
        websocket_session_duration_ms_sum: counters
            .websocket_session_duration_ms_sum
            .swap(0, Ordering::Relaxed),
        websocket_session_duration_ms_max: counters
            .websocket_session_duration_ms_max
            .swap(0, Ordering::Relaxed),
        websocket_resync_requests: counters
            .websocket_resync_requests
            .swap(0, Ordering::Relaxed),
        websocket_resync_accepted: counters
            .websocket_resync_accepted
            .swap(0, Ordering::Relaxed),
        websocket_resync_rejected: counters
            .websocket_resync_rejected
            .swap(0, Ordering::Relaxed),
        matchmaking_admissions: counters.matchmaking_admissions.swap(0, Ordering::Relaxed),
        matchmaking_admission_deduplications: counters
            .matchmaking_admission_deduplications
            .swap(0, Ordering::Relaxed),
        matchmaking_admission_rejections: counters
            .matchmaking_admission_rejections
            .swap(0, Ordering::Relaxed),
        matchmaking_commits: counters.matchmaking_commits.swap(0, Ordering::Relaxed),
        matchmaking_wait_ms_sum: counters.matchmaking_wait_ms_sum.swap(0, Ordering::Relaxed),
        matchmaking_wait_ms_max: counters.matchmaking_wait_ms_max.swap(0, Ordering::Relaxed),
        matchmaking_matched_players: counters
            .matchmaking_matched_players
            .swap(0, Ordering::Relaxed),
        matchmaking_matched_lobbies: counters
            .matchmaking_matched_lobbies
            .swap(0, Ordering::Relaxed),
        matchmaking_errors: counters.matchmaking_errors.swap(0, Ordering::Relaxed),
        matchmaking_integrity_errors: counters
            .matchmaking_integrity_errors
            .swap(0, Ordering::Relaxed),
        game_created_outbox_delivery_errors: counters
            .game_created_outbox_delivery_errors
            .swap(0, Ordering::Relaxed),
        games_completed: counters.games_completed.swap(0, Ordering::Relaxed),
        game_duration_ms_sum: counters.game_duration_ms_sum.swap(0, Ordering::Relaxed),
        game_duration_ms_max: counters.game_duration_ms_max.swap(0, Ordering::Relaxed),
        completed_game_players: counters.completed_game_players.swap(0, Ordering::Relaxed),
        potg_ring_truncated: counters.potg_ring_truncated.swap(0, Ordering::Relaxed),
        ring_evicted_seconds_sum: counters.ring_evicted_seconds_sum.swap(0, Ordering::Relaxed),
        ring_evicted_seconds_max: counters.ring_evicted_seconds_max.swap(0, Ordering::Relaxed),
        redis_requests: counters.redis_requests.swap(0, Ordering::Relaxed),
        redis_errors: counters.redis_errors.swap(0, Ordering::Relaxed),
        redis_request_latency_ms_sum: counters
            .redis_request_latency_ms_sum
            .swap(0, Ordering::Relaxed),
        redis_request_latency_ms_max: counters
            .redis_request_latency_ms_max
            .swap(0, Ordering::Relaxed),
    }
}

#[derive(Default)]
struct RegionalGauges {
    regional_collection_failures: u64,
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
    matchmaking_queue_entries: u64,
    matchmaking_oldest_queued_lobby_ms: u64,
    game_created_outbox_backlog: u64,
    game_created_outbox_oldest_age_ms: u64,
    game_created_outbox_age_index_cardinality_delta: u64,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct MatchmakingBacklogGauges {
    queue_entries: u64,
    oldest_queued_lobby_ms: u64,
    outbox_backlog: u64,
    outbox_oldest_age_ms: u64,
    outbox_age_index_cardinality_delta: u64,
}

fn summarize_matchmaking_backlogs(values: &[i64], now_ms: i64) -> Result<MatchmakingBacklogGauges> {
    let expected_values = MATCHMAKING_QUEUE_COUNT * 2 + 3;
    if values.len() != expected_values {
        anyhow::bail!(
            "matchmaking backlog summary returned {} values, expected {expected_values}",
            values.len()
        );
    }
    let mut summary = MatchmakingBacklogGauges::default();
    for queue in values[..MATCHMAKING_QUEUE_COUNT * 2].chunks_exact(2) {
        let entries = u64::try_from(queue[0]).context("negative matchmaking queue depth")?;
        summary.queue_entries = summary.queue_entries.saturating_add(entries);
        if queue[1] > 0 {
            summary.oldest_queued_lobby_ms = summary
                .oldest_queued_lobby_ms
                .max(now_ms.saturating_sub(queue[1]).max(0) as u64);
        }
    }
    let tail = &values[MATCHMAKING_QUEUE_COUNT * 2..];
    let outbox_backlog = u64::try_from(tail[0]).context("negative GameCreated outbox depth")?;
    let indexed_outbox = u64::try_from(tail[1]).context("negative outbox age-index depth")?;
    summary.outbox_backlog = outbox_backlog;
    summary.outbox_age_index_cardinality_delta = outbox_backlog.abs_diff(indexed_outbox);
    if tail[2] > 0 {
        summary.outbox_oldest_age_ms = now_ms.saturating_sub(tail[2]).max(0) as u64;
    }
    Ok(summary)
}

async fn collect_matchmaking_backlog_gauges(
    redis: &mut RedisConnection,
    now_ms: i64,
) -> Result<MatchmakingBacklogGauges> {
    // Every key shares the fixed matchmaking hash tag, so this bounded script
    // is one routed operation regardless of queue depth. Queue counts are
    // entry counts: one lobby intentionally appears in each selected game-mode
    // queue and may therefore contribute more than once.
    let script = redis::Script::new(
        r#"
        local function key_type(key)
            local response = redis.call('TYPE', key)
            if type(response) == 'table' then return response['ok'] end
            return response
        end
        local result = {}
        for index = 1, #KEYS - 2 do
            local queue_type = key_type(KEYS[index])
            if queue_type ~= 'none' and queue_type ~= 'zset' then
                return redis.error_reply('matchmaking queue has wrong type')
            end
            table.insert(result, redis.call('ZCARD', KEYS[index]))
            local oldest = redis.call('ZRANGE', KEYS[index], 0, 0, 'WITHSCORES')
            table.insert(result, #oldest == 0 and 0 or tonumber(oldest[2]))
        end
        local outbox_key = KEYS[#KEYS - 1]
        local age_key = KEYS[#KEYS]
        local outbox_type = key_type(outbox_key)
        if outbox_type ~= 'none' and outbox_type ~= 'hash' then
            return redis.error_reply('GameCreated outbox has wrong type')
        end
        local age_type = key_type(age_key)
        if age_type ~= 'none' and age_type ~= 'zset' then
            return redis.error_reply('GameCreated outbox age index has wrong type')
        end
        table.insert(result, redis.call('HLEN', outbox_key))
        table.insert(result, redis.call('ZCARD', age_key))
        local oldest_outbox = redis.call('ZRANGE', age_key, 0, 0, 'WITHSCORES')
        table.insert(result, #oldest_outbox == 0 and 0 or tonumber(oldest_outbox[2]))
        return result
        "#,
    );
    let mut invocation = script.prepare_invoke();
    for matchmaking_pool in crate::matchmaking_pool::MatchmakingPool::ALL {
        for game_type in &crate::matchmaking_manager::MATCHMAKING_GAME_TYPES {
            for queue_mode in &crate::matchmaking_manager::MATCHMAKING_QUEUE_MODES {
                invocation.key(
                    crate::redis_keys::RedisKeys::matchmaking_lobby_queue_for_pool(
                        game_type,
                        queue_mode,
                        matchmaking_pool,
                    ),
                );
            }
        }
    }
    invocation
        .key(crate::redis_keys::RedisKeys::matchmaking_game_created_outbox())
        .key(crate::redis_keys::RedisKeys::matchmaking_game_created_outbox_age());
    let values: Vec<i64> = invocation
        .invoke_async(redis)
        .await
        .context("failed to collect matchmaking backlog gauges")?;
    summarize_matchmaking_backlogs(&values, now_ms)
}

fn expected_recovery_prefix(partition: u32, game_id: u32) -> String {
    format!(
        "{{\"schema_version\":{RECOVERY_SCHEMA_VERSION},\"executor_protocol_version\":{EXECUTOR_PROTOCOL_VERSION},\"game_id\":{game_id},\"partition_id\":{partition},"
    )
}

async fn read_recovery_metadata_batch(
    redis: &mut RedisConnection,
    namespace: &ClusterNamespace,
    indexed_games: &[(u32, String, i64)],
) -> Result<Vec<(u64, Vec<u8>, Vec<u8>)>> {
    if indexed_games.len() > RECOVERY_METADATA_BATCH_SIZE {
        anyhow::bail!("recovery metadata batch exceeds fixed limit {RECOVERY_METADATA_BATCH_SIZE}");
    }
    let script = redis::Script::new(
        r#"
        local result = {}
        local tail_bytes = tonumber(ARGV[#KEYS + 1])
        for index, key in ipairs(KEYS) do
            local checkpoint_bytes = redis.call('STRLEN', key)
            result[index] = {
                checkpoint_bytes,
                redis.call('GETRANGE', key, 0, tonumber(ARGV[index])),
                redis.call('GETRANGE', key, -tail_bytes, -1)
            }
        end
        return result
        "#,
    );
    let mut invocation = script.prepare_invoke();
    for (game_id, _, _) in indexed_games {
        invocation.key(namespace.recovery(*game_id));
    }
    for (_, _, prefix_end) in indexed_games {
        invocation.arg(*prefix_end);
    }
    invocation.arg(RECOVERY_TAIL_SAMPLE_BYTES);
    let metadata = invocation
        .invoke_async(redis)
        .await
        .context("failed to inspect active recovery checkpoint metadata")?;
    Ok(metadata)
}

fn checkpointed_at_ms_from_tail(tail: &[u8]) -> Option<i64> {
    let marker_start = tail
        .windows(CHECKPOINTED_AT_MARKER.len())
        .rposition(|window| window == CHECKPOINTED_AT_MARKER)?;
    let value = &tail[marker_start + CHECKPOINTED_AT_MARKER.len()..];
    let value_end = value
        .iter()
        .position(|byte| !byte.is_ascii_digit() && *byte != b'-')?;
    if value.get(value_end) != Some(&b',') {
        return None;
    }
    let trailing_fields = &value[value_end + 1..];
    let source_token = trailing_fields
        .strip_prefix(SOURCE_LEASE_TOKEN_MARKER)?
        .strip_suffix(b"\"}")?;
    if source_token.is_empty() {
        return None;
    }
    std::str::from_utf8(&value[..value_end]).ok()?.parse().ok()
}

/// Tracks lease-absence windows at control-loop resolution while the regional
/// metrics scan remains on its normal regional cadence. Without this
/// rolling maximum, an outage that starts and ends between EMF samples is
/// invisible. The first missing observation is conservatively backdated to the
/// prior completed observation so a delayed sample cannot make a
/// near-five-second outage look safely shorter.
struct PartitionOutageTracker {
    missing_since_ms: [Option<i64>; PARTITION_COUNT as usize],
    window_max_ms: u64,
    last_observed_at_ms: Option<i64>,
}

impl Default for PartitionOutageTracker {
    fn default() -> Self {
        Self {
            missing_since_ms: array::from_fn(|_| None),
            window_max_ms: 0,
            last_observed_at_ms: None,
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
        let conservative_missing_since_ms = self
            .last_observed_at_ms
            .replace(now_ms)
            .unwrap_or_else(|| now_ms.saturating_sub(OWNERSHIP_SAMPLE_INTERVAL_MS));
        for partition in 0..PARTITION_COUNT as usize {
            let desired = assignment
                .is_some_and(|document| document.owners.contains_key(&(partition as u32)));
            let missing = desired && leases.get(partition).is_none_or(Option::is_none);
            if missing {
                let since =
                    self.missing_since_ms[partition].get_or_insert(conservative_missing_since_ms);
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
/// CloudWatch uses `Maximum` for regional gauges and `Sum` for counters. The
/// active-WebSocket gauge is emitted separately per task so dashboards can
/// sum per-task `Average` for minute-average fleet concurrency. Summed
/// per-task `Maximum` is only a conservative upper bound because task peaks
/// need not occur simultaneously.
/// Dimensions deliberately exclude partitions, users, and games.
pub fn spawn_resilience_metrics(
    redis: RedisConnection,
    namespace: ClusterNamespace,
    lifecycle: LocalTaskLifecycle,
    server_id: u64,
    cancellation: CancellationToken,
) -> JoinHandle<()> {
    // Every clone derived below is collector-only. Excluding these reads keeps
    // RedisRequests/RedisErrors/latency representative of application and
    // gameplay cache work instead of recursively measuring the sampler.
    let redis = redis.without_application_metrics();
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
                        std::time::Duration::from_millis(REGIONAL_COLLECTION_TIMEOUT_MS),
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
                            RegionalGauges {
                                regional_collection_failures: 1,
                                ..RegionalGauges::default()
                            }
                        }
                        Err(_) => {
                            warn!("final resilience metrics collection timed out");
                            RegionalGauges {
                                regional_collection_failures: 1,
                                ..RegionalGauges::default()
                            }
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
                    let result = tokio::time::timeout(
                        std::time::Duration::from_millis(REGIONAL_COLLECTION_TIMEOUT_MS),
                        collect_regional_gauges(
                            redis.clone(),
                            &namespace,
                            &membership,
                            &assignment,
                            server_id,
                            now_ms,
                        ),
                    ).await;
                    let sampled_unowned_ms = partition_outages.take_window_max(now_ms);
                    match result {
                        Ok(Ok(mut gauges)) => {
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
                        Ok(Err(error)) => {
                            warn!(%error, "regional resilience metrics collection failed");
                            // Local health must remain observable even when
                            // the regional Valkey-backed gauges cannot be
                            // collected. In particular, emit LocalReady=0
                            // during a cache outage instead of relying only on
                            // CloudWatch missing-data behavior.
                            let gauges = RegionalGauges {
                                regional_collection_failures: 1,
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
                        Err(_) => {
                            warn!("regional resilience metrics collection timed out");
                            let gauges = RegionalGauges {
                                regional_collection_failures: 1,
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
                    // The regular collector logs failures and emits local
                    // readiness. This fast observation is deliberately silent
                    // during Valkey outages so it cannot create a warning storm.
                    let _ = observe_partition_outages(
                        redis.clone(),
                        &namespace,
                        &assignment,
                        &mut partition_outages,
                    ).await;
                }
            }
        }
    })
}

async fn observe_partition_outages(
    mut redis: RedisConnection,
    namespace: &ClusterNamespace,
    assignment_store: &AssignmentStore,
    tracker: &mut PartitionOutageTracker,
) -> Result<()> {
    let assignment = assignment_store.load().await?;
    let mut leases = Vec::with_capacity(PARTITION_COUNT as usize);
    for partition in 0..PARTITION_COUNT {
        leases.push(
            redis
                .get(namespace.partition_lease(partition))
                .await
                .context("failed to sample partition lease for outage timing")?,
        );
    }
    tracker.observe(
        chrono::Utc::now().timestamp_millis(),
        assignment.as_ref(),
        &leases,
    );
    Ok(())
}

async fn collect_regional_gauges(
    mut redis: RedisConnection,
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
    // live task needs to inspect the regional recovery index. Selecting the
    // smallest membership identity is deterministic and automatically hands
    // collection to a survivor.
    let is_regional_reporter = members
        .iter()
        .min_by_key(|member| (member.server_id, member.boot_id.as_str()))
        .is_some_and(|member| member.server_id == local_server_id);
    if !is_regional_reporter {
        return Ok(gauges);
    }

    let matchmaking = collect_matchmaking_backlog_gauges(&mut redis, now_ms).await?;
    gauges.matchmaking_queue_entries = matchmaking.queue_entries;
    gauges.matchmaking_oldest_queued_lobby_ms = matchmaking.oldest_queued_lobby_ms;
    gauges.game_created_outbox_backlog = matchmaking.outbox_backlog;
    gauges.game_created_outbox_oldest_age_ms = matchmaking.outbox_oldest_age_ms;
    gauges.game_created_outbox_age_index_cardinality_delta =
        matchmaking.outbox_age_index_cardinality_delta;

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

    let mut leases = Vec::with_capacity(PARTITION_COUNT as usize);
    for partition in 0..PARTITION_COUNT {
        leases.push(
            redis
                .get(namespace.partition_lease(partition))
                .await
                .context("failed to inspect partition lease for metrics")?,
        );
    }
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
    // fixed executor partition. Partitions intentionally occupy distinct
    // cluster slots, so issue independently routed commands. The
    // pending-completion set is the durable retry queue for external effects;
    // the quarantine stream is the durable terminal disposition for poison
    // commands. Neither requires scanning game- or user-labelled keys.
    let mut pending_completions = 0_u64;
    let mut quarantined_commands = 0_u64;
    for partition in 0..PARTITION_COUNT {
        let pending: u64 = redis
            .scard(namespace.pending_completions(partition))
            .await
            .context("failed to inspect pending completion queue")?;
        let quarantined: u64 = redis
            .xlen(namespace.command_quarantine(partition))
            .await
            .context("failed to inspect command quarantine")?;
        pending_completions = pending_completions.saturating_add(pending);
        quarantined_commands = quarantined_commands.saturating_add(quarantined);
    }
    gauges.pending_completions = pending_completions;
    gauges.quarantined_commands = quarantined_commands;

    for partition in 0..PARTITION_COUNT {
        let game_ids: Vec<u32> = redis
            .smembers(namespace.active_games(partition))
            .await
            .context("failed to inspect active-game index")?;
        gauges.active_games = gauges.active_games.saturating_add(game_ids.len() as u64);

        // The active index and recovery keys for a valid game occupy the same
        // partition hash slot. Reject a corrupt cross-partition index entry
        // before batching so it is counted as a mismatch instead of making
        // the entire cluster-mode request fail with CROSSSLOT.
        let mut indexed_games = Vec::with_capacity(game_ids.len());
        for game_id in game_ids {
            if game_id % PARTITION_COUNT != partition {
                gauges.active_game_index_mismatches =
                    gauges.active_game_index_mismatches.saturating_add(1);
                continue;
            }
            let expected_prefix = expected_recovery_prefix(partition, game_id);
            let prefix_end = i64::try_from(expected_prefix.len().saturating_sub(1))
                .context("recovery checkpoint prefix exceeds Redis range")?;
            indexed_games.push((game_id, expected_prefix, prefix_end));
        }
        if indexed_games.is_empty() {
            continue;
        }

        // A checkpoint can be hundreds of KiB, while its identity lives at the
        // start and its timestamp is deliberately one of the final fields.
        // Inspect every indexed game but fetch only those bounded slices.
        // Fixed-size partition-local scripts replace a serial round trip per
        // game while bounding each atomic shard operation and preserving exact
        // fleet-wide mismatch and maximum-age semantics.
        for batch in indexed_games.chunks(RECOVERY_METADATA_BATCH_SIZE) {
            let metadata = read_recovery_metadata_batch(&mut redis, namespace, batch).await?;
            if metadata.len() != batch.len() {
                anyhow::bail!("active recovery metadata response length mismatch");
            }

            for ((_, expected_prefix, _), (checkpoint_bytes, prefix, tail)) in
                batch.iter().zip(metadata)
            {
                gauges.checkpoint_bytes = gauges.checkpoint_bytes.max(checkpoint_bytes);
                let Some(checkpointed_at_ms) = checkpointed_at_ms_from_tail(&tail) else {
                    gauges.active_game_index_mismatches += 1;
                    continue;
                };
                if checkpoint_bytes == 0 || prefix != expected_prefix.as_bytes() {
                    gauges.active_game_index_mismatches += 1;
                    continue;
                }
                gauges.checkpoint_age_ms = gauges
                    .checkpoint_age_ms
                    .max(now_ms.saturating_sub(checkpointed_at_ms).max(0) as u64);
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

fn emf_namespace(environment: &str) -> &'static str {
    if environment == "prod" {
        PRODUCTION_EMF_NAMESPACE
    } else {
        NON_PRODUCTION_EMF_NAMESPACE
    }
}

#[allow(clippy::too_many_arguments)]
fn emf_document(
    environment: &str,
    region: &str,
    task_boot_id: &str,
    local_ready: bool,
    gauges: RegionalGauges,
    counters: CounterSnapshot,
    now_ms: i64,
) -> Value {
    let namespace = emf_namespace(environment);
    let values = [
        (
            "RegionalCollectionFailures",
            gauges.regional_collection_failures,
            "Count",
        ),
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
            "MatchmakingQueueEntries",
            gauges.matchmaking_queue_entries,
            "Count",
        ),
        (
            "MatchmakingOldestQueuedLobbyMs",
            gauges.matchmaking_oldest_queued_lobby_ms,
            "Milliseconds",
        ),
        (
            "GameCreatedOutboxBacklog",
            gauges.game_created_outbox_backlog,
            "Count",
        ),
        (
            "GameCreatedOutboxOldestAgeMs",
            gauges.game_created_outbox_oldest_age_ms,
            "Milliseconds",
        ),
        (
            "GameCreatedOutboxAgeIndexCardinalityDelta",
            gauges.game_created_outbox_age_index_cardinality_delta,
            "Count",
        ),
        (
            "FencedWriteRejections",
            counters.fenced_write_rejections,
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
        (
            "BoostPacketCollections",
            counters.boost_packet_collections,
            "Count",
        ),
        ("BoostPadRespawns", counters.boost_pad_respawns, "Count"),
        (
            "BoostActivationAttempts",
            counters.boost_activation_attempts,
            "Count",
        ),
        (
            "BoostActivationCommandsScheduled",
            counters.boost_activation_commands_scheduled,
            "Count",
        ),
        (
            "BoostActivationCommandRejections",
            counters.boost_activation_command_rejections,
            "Count",
        ),
        ("BoostActivations", counters.boost_activations, "Count"),
        ("BoostManualStops", counters.boost_manual_stops, "Count"),
        ("BoostDepletions", counters.boost_depletions, "Count"),
        (
            "ComboFoodCollections",
            counters.combo_food_collections,
            "Count",
        ),
        ("ComboPointsAwarded", counters.combo_points_awarded, "Count"),
        ("GameActorAdvances", counters.game_actor_advances, "Count"),
        (
            "GameActorBatchQuantaSum",
            counters.game_actor_batch_quanta_sum,
            "Count",
        ),
        (
            "GameActorBatchQuantaMax",
            counters.game_actor_batch_quanta_max,
            "Count",
        ),
        (
            "GameActorLagMsSum",
            counters.game_actor_lag_ms_sum,
            "Milliseconds",
        ),
        (
            "GameActorLagMsMax",
            counters.game_actor_lag_ms_max,
            "Milliseconds",
        ),
        (
            "GameActorAdvanceDurationUsSum",
            counters.game_actor_advance_duration_us_sum,
            "Microseconds",
        ),
        (
            "GameActorAdvanceDurationUsMax",
            counters.game_actor_advance_duration_us_max,
            "Microseconds",
        ),
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
        ("HttpRequests", counters.http_requests, "Count"),
        ("Http4xxResponses", counters.http_responses_4xx, "Count"),
        ("Http5xxResponses", counters.http_responses_5xx, "Count"),
        (
            "HttpRequestLatencyMsSum",
            counters.http_request_latency_ms_sum,
            "Milliseconds",
        ),
        (
            "HttpRequestLatencyMsMax",
            counters.http_request_latency_ms_max,
            "Milliseconds",
        ),
        ("WebSocketOpens", counters.websocket_opens, "Count"),
        ("WebSocketCloses", counters.websocket_closes, "Count"),
        (
            "WebSocketRejectedUpgrades",
            counters.websocket_rejected_upgrades,
            "Count",
        ),
        (
            "WebSocketInboundMessages",
            counters.websocket_inbound_messages,
            "Count",
        ),
        (
            "WebSocketInboundBytes",
            counters.websocket_inbound_bytes,
            "Bytes",
        ),
        (
            "WebSocketOutboundMessages",
            counters.websocket_outbound_messages,
            "Count",
        ),
        (
            "WebSocketOutboundBytes",
            counters.websocket_outbound_bytes,
            "Bytes",
        ),
        (
            "WebSocketMalformedMessages",
            counters.websocket_malformed_messages,
            "Count",
        ),
        (
            "WebSocketProcessErrors",
            counters.websocket_process_errors,
            "Count",
        ),
        (
            "WebSocketSendErrors",
            counters.websocket_send_errors,
            "Count",
        ),
        (
            "WebSocketTransportErrors",
            counters.websocket_transport_errors,
            "Count",
        ),
        (
            "WebSocketSessionDurationMsSum",
            counters.websocket_session_duration_ms_sum,
            "Milliseconds",
        ),
        (
            "WebSocketSessionDurationMsMax",
            counters.websocket_session_duration_ms_max,
            "Milliseconds",
        ),
        (
            "WebSocketResyncRequests",
            counters.websocket_resync_requests,
            "Count",
        ),
        (
            "WebSocketResyncAccepted",
            counters.websocket_resync_accepted,
            "Count",
        ),
        (
            "WebSocketResyncRejected",
            counters.websocket_resync_rejected,
            "Count",
        ),
        (
            "MatchmakingAdmissions",
            counters.matchmaking_admissions,
            "Count",
        ),
        (
            "MatchmakingAdmissionDeduplications",
            counters.matchmaking_admission_deduplications,
            "Count",
        ),
        (
            "MatchmakingAdmissionRejections",
            counters.matchmaking_admission_rejections,
            "Count",
        ),
        ("MatchmakingCommits", counters.matchmaking_commits, "Count"),
        (
            "MatchmakingWaitMsSum",
            counters.matchmaking_wait_ms_sum,
            "Milliseconds",
        ),
        (
            "MatchmakingWaitMsMax",
            counters.matchmaking_wait_ms_max,
            "Milliseconds",
        ),
        (
            "MatchmakingMatchedPlayers",
            counters.matchmaking_matched_players,
            "Count",
        ),
        (
            "MatchmakingMatchedLobbies",
            counters.matchmaking_matched_lobbies,
            "Count",
        ),
        ("MatchmakingErrors", counters.matchmaking_errors, "Count"),
        (
            "MatchmakingIntegrityErrors",
            counters.matchmaking_integrity_errors,
            "Count",
        ),
        (
            "GameCreatedOutboxDeliveryErrors",
            counters.game_created_outbox_delivery_errors,
            "Count",
        ),
        ("GamesCompleted", counters.games_completed, "Count"),
        (
            "GameDurationMsSum",
            counters.game_duration_ms_sum,
            "Milliseconds",
        ),
        (
            "GameDurationMsMax",
            counters.game_duration_ms_max,
            "Milliseconds",
        ),
        (
            "CompletedGamePlayers",
            counters.completed_game_players,
            "Count",
        ),
        ("PotgRingTruncated", counters.potg_ring_truncated, "Count"),
        (
            "RingEvictedSecondsSum",
            counters.ring_evicted_seconds_sum,
            "Seconds",
        ),
        (
            "RingEvictedSecondsMax",
            counters.ring_evicted_seconds_max,
            "Seconds",
        ),
        ("RedisRequests", counters.redis_requests, "Count"),
        ("RedisErrors", counters.redis_errors, "Count"),
        (
            "RedisRequestLatencyMsSum",
            counters.redis_request_latency_ms_sum,
            "Milliseconds",
        ),
        (
            "RedisRequestLatencyMsMax",
            counters.redis_request_latency_ms_max,
            "Milliseconds",
        ),
        ("LocalReady", u64::from(local_ready), "Count"),
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
                "Namespace": namespace,
                "Dimensions": [
                    ["Environment"],
                    ["Environment", "Region"]
                ],
                "Metrics": definitions
            }]
        }),
    );
    Value::Object(document)
}

fn active_websockets_emf_document(
    environment: &str,
    region: &str,
    task_boot_id: &str,
    active_websockets: u64,
    now_ms: i64,
) -> Value {
    let namespace = emf_namespace(environment);
    json!({
        "Environment": environment,
        "Region": region,
        "TaskBootId": task_boot_id,
        "ActiveWebSockets": active_websockets,
        "_aws": {
            "Timestamp": now_ms,
            "CloudWatchMetrics": [{
                "Namespace": namespace,
                "Dimensions": [["Environment", "Region", "TaskBootId"]],
                "Metrics": [metric("ActiveWebSockets", "Count")]
            }]
        }
    })
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
    crate::otel_metrics::update_gauges(&crate::otel_metrics::GaugeSnapshot {
        regional_collection_failures: gauges.regional_collection_failures,
        ready_tasks: gauges.ready_tasks,
        live_tasks: gauges.live_tasks,
        draining_tasks: gauges.draining_tasks,
        membership_age_ms: gauges.membership_age_ms,
        assignment_version: gauges.assignment_version,
        assignment_age_ms: gauges.assignment_age_ms,
        assignment_imbalance: gauges.assignment_imbalance,
        active_partition_leases: gauges.active_partition_leases,
        partition_lease_deficit: gauges.partition_lease_deficit,
        partition_owner_mismatches: gauges.partition_owner_mismatches,
        partition_unowned_ms: gauges.partition_unowned_ms,
        oldest_pending_command_ms: gauges.oldest_pending_command_ms,
        pending_commands: gauges.pending_commands,
        pending_completions: gauges.pending_completions,
        quarantined_commands: gauges.quarantined_commands,
        checkpoint_age_ms: gauges.checkpoint_age_ms,
        checkpoint_bytes: gauges.checkpoint_bytes,
        active_games: gauges.active_games,
        active_game_index_mismatches: gauges.active_game_index_mismatches,
        matchmaking_queue_entries: gauges.matchmaking_queue_entries,
        matchmaking_oldest_queued_lobby_ms: gauges.matchmaking_oldest_queued_lobby_ms,
        game_created_outbox_backlog: gauges.game_created_outbox_backlog,
        game_created_outbox_oldest_age_ms: gauges.game_created_outbox_oldest_age_ms,
        game_created_outbox_age_index_cardinality_delta: gauges
            .game_created_outbox_age_index_cardinality_delta,
        local_ready: u64::from(local_ready),
        active_websockets,
    });
    println!(
        "{}",
        emf_document(
            environment,
            region,
            task_boot_id,
            local_ready,
            gauges,
            counters,
            now_ms,
        )
    );
    println!(
        "{}",
        active_websockets_emf_document(
            environment,
            region,
            task_boot_id,
            active_websockets,
            now_ms,
        )
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_membership::BootIdentity;
    use crate::partition_assignment::{ASSIGNMENT_SCHEMA_VERSION, AssignmentDocument};
    use crate::recovery::{RecoveryEnvelopeV2, ResolvedCommandState};
    use crate::redis_utils::create_connection_manager;
    use common::{GameState, GameType, QueueMode};
    use std::collections::BTreeMap;

    #[test]
    fn public_counter_recorders_are_non_blocking() {
        // Process-global counters may have been exercised by another unit test.
        // Establish a clean baseline before checking this recorder contract.
        let _ = take_counter_snapshot();
        record_fenced_write_rejection(1);
        record_checkpoint_writes(2);
        record_http_request(204, Duration::from_millis(7));
        record_http_request(404, Duration::from_millis(11));
        record_http_request(503, Duration::from_millis(17));
        record_websocket_opened(1);
        record_websocket_closed(1);
        record_websocket_rejected_upgrade(1);
        record_websocket_inbound_message(12);
        record_websocket_outbound_message(34);
        record_websocket_malformed_message(1);
        record_websocket_process_error(1);
        record_websocket_send_error(1);
        record_websocket_transport_error(1);
        record_websocket_session(Duration::from_millis(19));
        record_game_actor_advance(3, 50, Duration::from_micros(730));
        let mut boost_state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        let boost_snake_id = boost_state.add_player(99, None).unwrap().snake_id;
        record_boost_lifecycle_transition(
            &boost_state,
            BoostLifecycleTransition::Activated {
                snake_id: boost_snake_id,
            },
        );
        record_boost_lifecycle_transition(
            &boost_state,
            BoostLifecycleTransition::ManuallyStopped {
                snake_id: boost_snake_id,
            },
        );
        record_boost_lifecycle_transition(
            &boost_state,
            BoostLifecycleTransition::Depleted {
                snake_id: boost_snake_id,
            },
        );
        record_combo_food_collected(&boost_state, boost_snake_id, 3, 7, 425, true);
        record_websocket_resync_requested(2);
        record_websocket_resync_accepted(1);
        record_websocket_resync_rejected(1);
        record_matchmaking_admission(1);
        record_matchmaking_admission_deduplication(1);
        record_matchmaking_admission_rejection(1);
        record_matchmaking_commit(250, 4, 2);
        record_matchmaking_error(1);
        record_matchmaking_integrity_error(1);
        record_game_created_outbox_delivery_error(1);
        record_game_completed(3_000, 4);
        record_potg_ring_truncated(17);
        record_redis_request(Duration::from_millis(9), true);
        crate::redis_utils::drop_redis_request_measurement_for_test(true);
        crate::redis_utils::drop_redis_request_measurement_for_test(false);
        let snapshot = take_counter_snapshot();
        assert_eq!(snapshot.fenced_write_rejections, 1);
        assert_eq!(snapshot.checkpoint_writes, 2);
        assert!(snapshot.http_requests >= 3);
        assert!(snapshot.http_responses_4xx >= 1);
        assert!(snapshot.http_responses_5xx >= 1);
        assert!(snapshot.http_request_latency_ms_sum >= 35);
        assert!(snapshot.http_request_latency_ms_max >= 17);
        assert!(snapshot.websocket_opens >= 1);
        assert!(snapshot.websocket_closes >= 1);
        assert!(snapshot.websocket_rejected_upgrades >= 1);
        assert!(snapshot.websocket_inbound_messages >= 1);
        assert!(snapshot.websocket_inbound_bytes >= 12);
        assert!(snapshot.websocket_outbound_messages >= 1);
        assert!(snapshot.websocket_outbound_bytes >= 34);
        assert!(snapshot.websocket_malformed_messages >= 1);
        assert!(snapshot.websocket_process_errors >= 1);
        assert!(snapshot.websocket_send_errors >= 1);
        assert!(snapshot.websocket_transport_errors >= 1);
        assert!(snapshot.websocket_session_duration_ms_sum >= 19);
        assert!(snapshot.websocket_session_duration_ms_max >= 19);
        assert!(snapshot.game_actor_advances >= 1);
        assert!(snapshot.game_actor_batch_quanta_sum >= 3);
        assert!(snapshot.game_actor_batch_quanta_max >= 3);
        assert!(snapshot.game_actor_lag_ms_sum >= 50);
        assert!(snapshot.game_actor_lag_ms_max >= 50);
        assert!(snapshot.game_actor_advance_duration_us_sum >= 730);
        assert!(snapshot.game_actor_advance_duration_us_max >= 730);
        assert!(snapshot.boost_activations >= 1);
        assert!(snapshot.boost_manual_stops >= 1);
        assert!(snapshot.boost_depletions >= 1);
        assert!(snapshot.combo_food_collections >= 1);
        assert!(snapshot.combo_points_awarded >= 3);
        assert!(snapshot.websocket_resync_requests >= 2);
        assert!(snapshot.websocket_resync_accepted >= 1);
        assert!(snapshot.websocket_resync_rejected >= 1);
        assert!(snapshot.matchmaking_admissions >= 1);
        assert!(snapshot.matchmaking_admission_deduplications >= 1);
        assert!(snapshot.matchmaking_admission_rejections >= 1);
        assert!(snapshot.matchmaking_commits >= 1);
        assert!(snapshot.matchmaking_wait_ms_sum >= 250);
        assert!(snapshot.matchmaking_wait_ms_max >= 250);
        assert!(snapshot.matchmaking_matched_players >= 4);
        assert!(snapshot.matchmaking_matched_lobbies >= 2);
        assert!(snapshot.matchmaking_errors >= 1);
        assert!(snapshot.matchmaking_integrity_errors >= 1);
        assert!(snapshot.game_created_outbox_delivery_errors >= 1);
        assert!(snapshot.games_completed >= 1);
        assert!(snapshot.game_duration_ms_sum >= 3_000);
        assert!(snapshot.game_duration_ms_max >= 3_000);
        assert!(snapshot.completed_game_players >= 4);
        assert_eq!(snapshot.potg_ring_truncated, 1);
        assert_eq!(snapshot.ring_evicted_seconds_sum, 17);
        assert_eq!(snapshot.ring_evicted_seconds_max, 17);
        assert!(snapshot.redis_requests >= 2);
        assert!(snapshot.redis_errors >= 2);
        assert!(snapshot.redis_request_latency_ms_sum >= 9);
        assert!(snapshot.redis_request_latency_ms_max >= 9);
        assert_eq!(take_counter_snapshot().checkpoint_writes, 0);
    }

    #[test]
    fn matchmaking_backlog_summary_aggregates_fixed_queues_and_exact_ages() -> Result<()> {
        let now_ms = 10_000_i64;
        let mut values = Vec::with_capacity(MATCHMAKING_QUEUE_COUNT * 2 + 3);
        for index in 0..MATCHMAKING_QUEUE_COUNT {
            values.push(i64::try_from(index + 1)?);
            values.push(now_ms - i64::try_from((index + 1) * 100)?);
        }
        values.extend([3, 2, now_ms - 2_000]);

        assert_eq!(
            summarize_matchmaking_backlogs(&values, now_ms)?,
            MatchmakingBacklogGauges {
                queue_entries: u64::try_from(
                    MATCHMAKING_QUEUE_COUNT * (MATCHMAKING_QUEUE_COUNT + 1) / 2
                )?,
                oldest_queued_lobby_ms: u64::try_from(MATCHMAKING_QUEUE_COUNT * 100)?,
                outbox_backlog: 3,
                outbox_oldest_age_ms: 2_000,
                outbox_age_index_cardinality_delta: 1,
            }
        );
        assert!(summarize_matchmaking_backlogs(&values[..values.len() - 1], now_ms).is_err());
        Ok(())
    }

    #[test]
    fn emf_dimensions_keep_only_websocket_gauge_per_task() {
        let counters = CounterSnapshot {
            combo_food_collections: 4,
            combo_points_awarded: 9,
            ..CounterSnapshot::default()
        };
        let main = emf_document(
            "test",
            "us-test-1",
            "boot-id",
            true,
            RegionalGauges::default(),
            counters,
            123,
        );
        assert_eq!(
            main.pointer("/_aws/CloudWatchMetrics/0/Dimensions"),
            Some(&json!([["Environment"], ["Environment", "Region"]])),
        );
        assert_eq!(
            main.pointer("/_aws/CloudWatchMetrics/0/Namespace"),
            Some(&json!("Snaketron/OperationalDev")),
        );
        let main_metrics = main
            .pointer("/_aws/CloudWatchMetrics/0/Metrics")
            .and_then(Value::as_array)
            .expect("main EMF metric definitions");
        assert!(main_metrics.len() <= 100);
        assert!(
            main_metrics
                .iter()
                .all(|definition| definition["Name"] != "ActiveWebSockets")
        );
        assert_eq!(main["ComboFoodCollections"], 4);
        assert_eq!(main["ComboPointsAwarded"], 9);
        for name in ["ComboFoodCollections", "ComboPointsAwarded"] {
            assert!(
                main_metrics.iter().any(|definition| {
                    definition["Name"] == name && definition["Unit"] == "Count"
                })
            );
        }

        let sockets = active_websockets_emf_document("test", "us-test-1", "boot-id", 7, 123);
        assert_eq!(
            sockets.pointer("/_aws/CloudWatchMetrics/0/Dimensions"),
            Some(&json!([["Environment", "Region", "TaskBootId"]])),
        );
        assert_eq!(
            sockets.pointer("/_aws/CloudWatchMetrics/0/Namespace"),
            Some(&json!("Snaketron/OperationalDev")),
        );
        assert_eq!(sockets["ActiveWebSockets"], 7);
        assert_eq!(
            sockets.pointer("/_aws/CloudWatchMetrics/0/Metrics/0/Name"),
            Some(&json!("ActiveWebSockets")),
        );

        let production = active_websockets_emf_document("prod", "use1", "boot-id", 1, 123);
        assert_eq!(
            production.pointer("/_aws/CloudWatchMetrics/0/Namespace"),
            Some(&json!("Snaketron/Operational")),
        );
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
            (u64::from(PARTITION_COUNT - 1), 1, 2),
        );
        assert_eq!(
            summarize_partition_leases(None, &leases),
            (u64::from(PARTITION_COUNT - 1), 0, 0),
        );
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

        // Restoration occurs before the next regional EMF emission. The
        // completed duration is retained rather than reset to zero.
        leases[0] = Some(format!("{owner}:{acquisition}").into_bytes());
        tracker.observe(6_000, Some(&assignment), &leases);
        assert_eq!(tracker.take_window_max(15_000), 5_000);
        assert_eq!(tracker.take_window_max(15_000), 0);
        Ok(())
    }

    #[test]
    fn partition_outage_uses_actual_previous_observation_after_delayed_sample() -> Result<()> {
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
        leases[0] = None;

        // A regional collection can delay this nominally 500-millisecond
        // sampler. Bound the possible outage from the prior real observation,
        // rather than pretending the delayed samples remained 500 ms apart.
        tracker.observe(3_500, Some(&assignment), &leases);
        assert_eq!(tracker.take_window_max(3_500), 2_500);

        tracker.observe(6_500, Some(&assignment), &leases);
        assert_eq!(tracker.take_window_max(6_500), 5_500);
        Ok(())
    }

    #[tokio::test]
    async fn partition_metadata_batch_inspects_every_same_slot_recovery() -> Result<()> {
        let client = redis::Client::open("redis://127.0.0.1:6379/15?protocol=resp3")?;
        let (push_tx, _push_rx) = tokio::sync::broadcast::channel(8);
        let manager = create_connection_manager(client, push_tx).await?;
        let mut redis: RedisConnection = manager.into();
        let salt = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let namespace = ClusterNamespace::new(format!("metrics-batch-{salt}"))?;
        let partition = 7;
        let game_ids = [7, 17, 27];
        let mut indexed_games = Vec::new();

        for (offset, game_id) in game_ids.into_iter().enumerate() {
            let prefix = expected_recovery_prefix(partition, game_id);
            let checkpointed_at_ms = 1_000 + offset as i64;
            let payload = format!(
                "{prefix}\"checkpointed_at_ms\":{checkpointed_at_ms},\"source_lease_token\":\"test\"}}"
            );
            let _: () = redis
                .set(namespace.recovery(game_id), payload.as_bytes())
                .await?;
            indexed_games.push((
                game_id,
                prefix.clone(),
                i64::try_from(prefix.len().saturating_sub(1))?,
            ));
        }

        let metadata = read_recovery_metadata_batch(&mut redis, &namespace, &indexed_games).await?;
        assert_eq!(metadata.len(), indexed_games.len());
        for (index, ((_, prefix, _), (bytes, observed_prefix, tail))) in
            indexed_games.iter().zip(&metadata).enumerate()
        {
            assert!(*bytes > prefix.len() as u64);
            assert_eq!(observed_prefix, prefix.as_bytes());
            assert_eq!(
                checkpointed_at_ms_from_tail(tail),
                Some(1_000 + index as i64)
            );
        }
        let oversized = (0..=RECOVERY_METADATA_BATCH_SIZE)
            .map(|index| {
                let game_id = partition + PARTITION_COUNT * index as u32;
                let prefix = expected_recovery_prefix(partition, game_id);
                (
                    game_id,
                    prefix.clone(),
                    i64::try_from(prefix.len().saturating_sub(1)).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert!(
            read_recovery_metadata_batch(&mut redis, &namespace, &oversized)
                .await
                .is_err(),
            "one atomic metadata script must never exceed its fixed batch"
        );

        let keys = game_ids
            .into_iter()
            .map(|game_id| namespace.recovery(game_id))
            .collect::<Vec<_>>();
        let _: () = redis.del(keys).await?;
        Ok(())
    }

    #[test]
    fn bounded_checkpoint_metadata_preserves_serialized_identity_and_age() {
        let prefix = expected_recovery_prefix(7, 127);
        // Built from the live constants rather than a literal: this test pins
        // the serialized field *order* that the bounded metadata script scans
        // for, not the protocol version, which is expected to move.
        assert_eq!(
            prefix,
            format!(
                "{{\"schema_version\":{RECOVERY_SCHEMA_VERSION},\"executor_protocol_version\":{EXECUTOR_PROTOCOL_VERSION},\"game_id\":127,\"partition_id\":7,"
            )
        );

        let envelope = RecoveryEnvelopeV2::new(
            127,
            7,
            GameState::new(
                40,
                40,
                GameType::FreeForAll { max_players: 4 },
                QueueMode::Quickmatch,
                Some(7),
                123_456_789,
            ),
            "123-4".into(),
            ResolvedCommandState::default(),
            17,
            91,
            123_456_789,
            "11111111-1111-4111-8111-111111111111:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".into(),
        );
        let payload = serde_json::to_vec(&envelope).unwrap();
        assert!(payload.starts_with(prefix.as_bytes()));
        let tail_start = payload
            .len()
            .saturating_sub(RECOVERY_TAIL_SAMPLE_BYTES as usize);
        assert_eq!(
            checkpointed_at_ms_from_tail(&payload[tail_start..]),
            Some(123_456_789),
        );

        assert_eq!(checkpointed_at_ms_from_tail(b"{}"), None);
        assert_eq!(
            checkpointed_at_ms_from_tail(br#""checkpointed_at_ms":123}"#),
            None,
            "the timestamp must retain its expected following field",
        );
        assert_eq!(
            checkpointed_at_ms_from_tail(br#""checkpointed_at_ms":123,"#),
            None,
            "a truncated trailing field must not look like a checkpoint",
        );
        assert_eq!(
            checkpointed_at_ms_from_tail(br#""checkpointed_at_ms":123,"source_lease_token":"}"#,),
            None,
            "an unterminated source token must not look like a checkpoint",
        );
    }

    #[test]
    fn boost_metric_dimensions_are_finite_labels_without_identifiers() {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 2 },
            QueueMode::Competitive,
            None,
            0,
        );
        let player = state.add_player(9_876_543, None).unwrap();

        assert_eq!(
            boost_metric_dimensions(&state, player.snake_id),
            BoostMetricDimensions {
                game_type: "2v2",
                queue_mode: "competitive",
                team_side: "team-0",
                speed_band: "1.26-1.50x",
            }
        );
        assert_eq!(
            boost_metric_dimensions(&state, u32::MAX).team_side,
            "unknown"
        );
    }

    #[test]
    fn combo_metric_dimensions_are_finite_labels_without_identifiers() {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 2 },
            QueueMode::Competitive,
            None,
            0,
        );
        let player = state.add_player(9_876_543, None).unwrap();

        assert_eq!(
            combo_metric_dimensions(&state, player.snake_id),
            ComboMetricDimensions {
                game_type: "2v2",
                queue_mode: "competitive",
                team_side: "team-0",
            }
        );
        assert_eq!(
            combo_metric_dimensions(&state, u32::MAX).team_side,
            "unknown"
        );
    }
}
