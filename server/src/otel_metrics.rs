//! OpenTelemetry instruments backing the public resilience-metrics facade.
//!
//! Instrument names deliberately preserve the established Snaketron metric
//! vocabulary while using native monotonic counters, histograms, and
//! observable gauges. No instrument attribute contains a player, game,
//! partition, request, or session identifier.

use opentelemetry::metrics::{Counter, Histogram, Meter, ObservableGauge};
use opentelemetry::{KeyValue, global};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

const LATENCY_BUCKETS_MS: &[f64] = &[
    1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 10_000.0,
];
const SESSION_BUCKETS_MS: &[f64] = &[
    100.0,
    500.0,
    1_000.0,
    5_000.0,
    10_000.0,
    30_000.0,
    60_000.0,
    300_000.0,
    600_000.0,
    1_800_000.0,
    3_600_000.0,
];
const ACTOR_ADVANCE_BUCKETS_US: &[f64] = &[
    10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0, 10_000.0, 25_000.0, 50_000.0,
];
const ACTOR_BATCH_BUCKETS: &[f64] = &[0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0];
const COMBO_CHAIN_DEPTH_BUCKETS: &[f64] =
    &[1.0, 2.0, 3.0, 4.0, 5.0, 8.0, 13.0, 21.0, 34.0, 55.0, 89.0];
const COMBO_REMAINING_WINDOW_BUCKETS_MS: &[f64] = &[
    0.0, 50.0, 100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0, 900.0, 1_000.0, 1_200.0,
    1_400.0, 1_600.0, 1_800.0, 2_000.0,
];
const POTG_RING_EVICTED_SECONDS_BUCKETS: &[f64] = &[
    1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1_200.0, 1_800.0, 3_600.0,
];

struct OtelMetrics {
    fenced_write_rejections: Counter<u64>,
    planned_drain_failures: Counter<u64>,
    command_claims: Counter<u64>,
    command_acks: Counter<u64>,
    command_resends: Counter<u64>,
    command_deduplications: Counter<u64>,
    command_rejections: Counter<u64>,
    boost_packet_collections: Counter<u64>,
    boost_pad_respawns: Counter<u64>,
    boost_activation_attempts: Counter<u64>,
    boost_activation_commands_scheduled: Counter<u64>,
    boost_activation_command_rejections: Counter<u64>,
    boost_activations: Counter<u64>,
    boost_manual_stops: Counter<u64>,
    boost_depletions: Counter<u64>,
    combo_food_collections: Counter<u64>,
    combo_points_awarded: Counter<u64>,
    combo_chain_depth: Histogram<u64>,
    combo_remaining_window_at_collection: Histogram<u64>,
    game_actor_advance_duration: Histogram<u64>,
    game_actor_batch_quanta: Histogram<u64>,
    game_actor_lag: Histogram<u64>,
    checkpoint_writes: Counter<u64>,
    checkpoint_failures: Counter<u64>,
    recovered_games: Counter<u64>,
    recovery_replays: Counter<u64>,
    match_claim_conflicts: Counter<u64>,
    duplicate_completion_effects_prevented: Counter<u64>,
    http_requests: Counter<u64>,
    http4xx_responses: Counter<u64>,
    http5xx_responses: Counter<u64>,
    http_request_latency: Histogram<u64>,
    websocket_opens: Counter<u64>,
    websocket_closes: Counter<u64>,
    websocket_rejected_upgrades: Counter<u64>,
    websocket_inbound_messages: Counter<u64>,
    websocket_inbound_bytes: Counter<u64>,
    websocket_outbound_messages: Counter<u64>,
    websocket_outbound_bytes: Counter<u64>,
    websocket_malformed_messages: Counter<u64>,
    websocket_process_errors: Counter<u64>,
    websocket_send_errors: Counter<u64>,
    websocket_transport_errors: Counter<u64>,
    websocket_session_duration: Histogram<u64>,
    websocket_resync_requests: Counter<u64>,
    websocket_resync_accepted: Counter<u64>,
    websocket_resync_rejected: Counter<u64>,
    matchmaking_admissions: Counter<u64>,
    matchmaking_admission_deduplications: Counter<u64>,
    matchmaking_admission_rejections: Counter<u64>,
    matchmaking_commits: Counter<u64>,
    matchmaking_wait: Histogram<u64>,
    matchmaking_matched_players: Counter<u64>,
    matchmaking_matched_lobbies: Counter<u64>,
    matchmaking_errors: Counter<u64>,
    matchmaking_integrity_errors: Counter<u64>,
    game_created_outbox_delivery_errors: Counter<u64>,
    games_completed: Counter<u64>,
    game_duration: Histogram<u64>,
    completed_game_players: Counter<u64>,
    potg_ring_truncated: Counter<u64>,
    potg_ring_evicted_seconds: Histogram<u64>,
    redis_requests: Counter<u64>,
    redis_errors: Counter<u64>,
    redis_request_latency: Histogram<u64>,
    // Retaining the handles retains their callbacks for the provider lifetime.
    _observable_gauges: Vec<ObservableGauge<u64>>,
}

#[derive(Default)]
struct GaugeState {
    regional_collection_failures: AtomicU64,
    ready_tasks: AtomicU64,
    live_tasks: AtomicU64,
    draining_tasks: AtomicU64,
    membership_age_ms: AtomicU64,
    assignment_version: AtomicU64,
    assignment_age_ms: AtomicU64,
    assignment_imbalance: AtomicU64,
    active_partition_leases: AtomicU64,
    partition_lease_deficit: AtomicU64,
    partition_owner_mismatches: AtomicU64,
    partition_unowned_ms: AtomicU64,
    oldest_pending_command_ms: AtomicU64,
    pending_commands: AtomicU64,
    pending_completions: AtomicU64,
    quarantined_commands: AtomicU64,
    checkpoint_age_ms: AtomicU64,
    checkpoint_bytes: AtomicU64,
    active_games: AtomicU64,
    active_game_index_mismatches: AtomicU64,
    matchmaking_queue_entries: AtomicU64,
    matchmaking_oldest_queued_lobby_ms: AtomicU64,
    game_created_outbox_backlog: AtomicU64,
    game_created_outbox_oldest_age_ms: AtomicU64,
    game_created_outbox_age_index_cardinality_delta: AtomicU64,
    local_ready: AtomicU64,
    active_websockets: AtomicU64,
}

#[derive(Debug, Default)]
pub(crate) struct GaugeSnapshot {
    pub regional_collection_failures: u64,
    pub ready_tasks: u64,
    pub live_tasks: u64,
    pub draining_tasks: u64,
    pub membership_age_ms: u64,
    pub assignment_version: u64,
    pub assignment_age_ms: u64,
    pub assignment_imbalance: u64,
    pub active_partition_leases: u64,
    pub partition_lease_deficit: u64,
    pub partition_owner_mismatches: u64,
    pub partition_unowned_ms: u64,
    pub oldest_pending_command_ms: u64,
    pub pending_commands: u64,
    pub pending_completions: u64,
    pub quarantined_commands: u64,
    pub checkpoint_age_ms: u64,
    pub checkpoint_bytes: u64,
    pub active_games: u64,
    pub active_game_index_mismatches: u64,
    pub matchmaking_queue_entries: u64,
    pub matchmaking_oldest_queued_lobby_ms: u64,
    pub game_created_outbox_backlog: u64,
    pub game_created_outbox_oldest_age_ms: u64,
    pub game_created_outbox_age_index_cardinality_delta: u64,
    pub local_ready: u64,
    pub active_websockets: u64,
}

static METRICS: OnceLock<OtelMetrics> = OnceLock::new();
static GAUGE_STATE: OnceLock<GaugeState> = OnceLock::new();

pub(crate) fn init() {
    let _ = metrics();
}

fn metrics() -> &'static OtelMetrics {
    METRICS.get_or_init(OtelMetrics::new)
}

fn gauge_state() -> &'static GaugeState {
    GAUGE_STATE.get_or_init(GaugeState::default)
}

fn counter(meter: &Meter, name: &'static str, description: &'static str) -> Counter<u64> {
    meter
        .u64_counter(name)
        .with_description(description)
        .with_unit("1")
        .build()
}

fn histogram(
    meter: &Meter,
    name: &'static str,
    description: &'static str,
    boundaries: &[f64],
) -> Histogram<u64> {
    meter
        .u64_histogram(name)
        .with_description(description)
        .with_unit("ms")
        .with_boundaries(boundaries.to_vec())
        .build()
}

fn histogram_with_unit(
    meter: &Meter,
    name: &'static str,
    description: &'static str,
    unit: &'static str,
    boundaries: &[f64],
) -> Histogram<u64> {
    meter
        .u64_histogram(name)
        .with_description(description)
        .with_unit(unit)
        .with_boundaries(boundaries.to_vec())
        .build()
}

impl OtelMetrics {
    fn new() -> Self {
        Self::from_meter(global::meter("snaketron-server"))
    }

    fn from_meter(meter: Meter) -> Self {
        let mut observable_gauges = Vec::new();
        let state = gauge_state();

        macro_rules! gauge {
            ($field:ident, $name:literal, $description:literal) => {
                observable_gauges.push(
                    meter
                        .u64_observable_gauge($name)
                        .with_description($description)
                        .with_unit("1")
                        .with_callback(move |observer| {
                            observer.observe(state.$field.load(Ordering::Relaxed), &[]);
                        })
                        .build(),
                );
            };
        }

        gauge!(
            regional_collection_failures,
            "snaketron.regional_collection_failures",
            "Whether the latest regional telemetry collection failed"
        );
        gauge!(ready_tasks, "snaketron.ready_tasks", "Ready regional tasks");
        gauge!(live_tasks, "snaketron.live_tasks", "Live regional tasks");
        gauge!(
            draining_tasks,
            "snaketron.draining_tasks",
            "Draining regional tasks"
        );
        gauge!(
            membership_age_ms,
            "snaketron.membership_age_ms",
            "Age in milliseconds of the stalest live membership heartbeat"
        );
        gauge!(
            assignment_version,
            "snaketron.assignment_version",
            "Current regional assignment version"
        );
        gauge!(
            assignment_age_ms,
            "snaketron.assignment_age_ms",
            "Age in milliseconds of the current regional assignment"
        );
        gauge!(
            assignment_imbalance,
            "snaketron.assignment_imbalance",
            "Difference between the largest and smallest task partition allocation"
        );
        gauge!(
            active_partition_leases,
            "snaketron.active_partition_leases",
            "Active executor partition leases"
        );
        gauge!(
            partition_lease_deficit,
            "snaketron.partition_lease_deficit",
            "Desired executor partitions without an active lease"
        );
        gauge!(
            partition_owner_mismatches,
            "snaketron.partition_owner_mismatches",
            "Executor leases held by an unexpected owner"
        );
        gauge!(
            partition_unowned_ms,
            "snaketron.partition_unowned_ms",
            "Longest partition ownership outage in milliseconds"
        );
        gauge!(
            oldest_pending_command_ms,
            "snaketron.oldest_pending_command_ms",
            "Age in milliseconds of the oldest pending executor command"
        );
        gauge!(
            pending_commands,
            "snaketron.pending_commands",
            "Pending executor commands"
        );
        gauge!(
            pending_completions,
            "snaketron.pending_completions",
            "Pending durable completion effects"
        );
        gauge!(
            quarantined_commands,
            "snaketron.quarantined_commands",
            "Quarantined executor commands"
        );
        gauge!(
            checkpoint_age_ms,
            "snaketron.checkpoint_age_ms",
            "Age in milliseconds of the oldest active-game checkpoint"
        );
        gauge!(
            checkpoint_bytes,
            "snaketron.checkpoint_bytes",
            "Largest active-game checkpoint size in bytes"
        );
        gauge!(
            active_games,
            "snaketron.active_games",
            "Active regional games"
        );
        gauge!(
            active_game_index_mismatches,
            "snaketron.active_game_index_mismatches",
            "Active-game index and checkpoint mismatches"
        );
        gauge!(
            matchmaking_queue_entries,
            "snaketron.matchmaking_queue_entries",
            "Entries across fixed matchmaking queues"
        );
        gauge!(
            matchmaking_oldest_queued_lobby_ms,
            "snaketron.matchmaking_oldest_queued_lobby_ms",
            "Age in milliseconds of the oldest queued lobby"
        );
        gauge!(
            game_created_outbox_backlog,
            "snaketron.game_created_outbox_backlog",
            "Undelivered game-created outbox records"
        );
        gauge!(
            game_created_outbox_oldest_age_ms,
            "snaketron.game_created_outbox_oldest_age_ms",
            "Age in milliseconds of the oldest game-created outbox record"
        );
        gauge!(
            game_created_outbox_age_index_cardinality_delta,
            "snaketron.game_created_outbox_age_index_cardinality_delta",
            "Difference between outbox and outbox-age index cardinality"
        );
        gauge!(
            local_ready,
            "snaketron.local_ready",
            "Whether this server task is admitting new traffic"
        );
        gauge!(
            active_websockets,
            "snaketron.active_web_sockets",
            "Active WebSocket sessions on this server task"
        );

        Self {
            fenced_write_rejections: counter(
                &meter,
                "snaketron.fenced_write_rejections",
                "Rejected writes from stale executor owners",
            ),
            planned_drain_failures: counter(
                &meter,
                "snaketron.planned_drain_failures",
                "Cooperative task drains that failed",
            ),
            command_claims: counter(
                &meter,
                "snaketron.command_claims",
                "Executor commands claimed for processing",
            ),
            command_acks: counter(
                &meter,
                "snaketron.command_acks",
                "Executor commands acknowledged",
            ),
            command_resends: counter(
                &meter,
                "snaketron.command_resends",
                "Executor commands resent after recovery",
            ),
            command_deduplications: counter(
                &meter,
                "snaketron.command_deduplications",
                "Duplicate executor commands suppressed",
            ),
            command_rejections: counter(
                &meter,
                "snaketron.command_rejections",
                "Executor commands rejected",
            ),
            boost_packet_collections: counter(
                &meter,
                "snaketron.boost.packet_collections",
                "Authoritative Boost packets collected",
            ),
            boost_pad_respawns: counter(
                &meter,
                "snaketron.boost.pad_respawns",
                "Authoritative Boost pad cooldowns completed",
            ),
            boost_activation_attempts: counter(
                &meter,
                "snaketron.boost.activation_attempts",
                "Unique Boost activation commands evaluated by an executor",
            ),
            boost_activation_commands_scheduled: counter(
                &meter,
                "snaketron.boost.activation_commands_scheduled",
                "Boost activation commands accepted into authoritative simulation",
            ),
            boost_activation_command_rejections: counter(
                &meter,
                "snaketron.boost.activation_command_rejections",
                "Boost activation commands rejected before authoritative simulation",
            ),
            boost_activations: counter(
                &meter,
                "snaketron.boost.activations",
                "Successful authoritative Boost state activations",
            ),
            boost_manual_stops: counter(
                &meter,
                "snaketron.boost.manual_stops",
                "Successful authoritative player-requested Boost stops",
            ),
            boost_depletions: counter(
                &meter,
                "snaketron.boost.depletions",
                "Authoritative Boost state depletions after the final funded quantum",
            ),
            combo_food_collections: counter(
                &meter,
                "snaketron.combo.food_collections",
                "Authoritative food collections after a fenced durability boundary",
            ),
            combo_points_awarded: counter(
                &meter,
                "snaketron.combo.points_awarded",
                "Authoritative food points awarded after a fenced durability boundary",
            ),
            combo_chain_depth: histogram_with_unit(
                &meter,
                "snaketron.combo.chain_depth",
                "Combo chain depth at an authoritative food collection",
                "1",
                COMBO_CHAIN_DEPTH_BUCKETS,
            ),
            combo_remaining_window_at_collection: histogram(
                &meter,
                "snaketron.combo.remaining_window_at_collection",
                "Combo window remaining immediately before an authoritative food collection",
                COMBO_REMAINING_WINDOW_BUCKETS_MS,
            ),
            game_actor_advance_duration: histogram_with_unit(
                &meter,
                "snaketron.game_actor.advance_duration",
                "Authoritative game actor simulation advance duration in microseconds",
                "us",
                ACTOR_ADVANCE_BUCKETS_US,
            ),
            game_actor_batch_quanta: histogram_with_unit(
                &meter,
                "snaketron.game_actor.batch_quanta",
                "Simulation quanta processed by one authoritative actor advance",
                "1",
                ACTOR_BATCH_BUCKETS,
            ),
            game_actor_lag: histogram(
                &meter,
                "snaketron.game_actor.lag",
                "Authoritative actor lag behind its intended committed target in milliseconds",
                LATENCY_BUCKETS_MS,
            ),
            checkpoint_writes: counter(
                &meter,
                "snaketron.checkpoint_writes",
                "Active-game checkpoint writes",
            ),
            checkpoint_failures: counter(
                &meter,
                "snaketron.checkpoint_failures",
                "Failed active-game checkpoint writes",
            ),
            recovered_games: counter(
                &meter,
                "snaketron.recovered_games",
                "Games restored from recovery state",
            ),
            recovery_replays: counter(
                &meter,
                "snaketron.recovery_replays",
                "Commands replayed during game recovery",
            ),
            match_claim_conflicts: counter(
                &meter,
                "snaketron.match_claim_conflicts",
                "Atomic matchmaking claim conflicts",
            ),
            duplicate_completion_effects_prevented: counter(
                &meter,
                "snaketron.duplicate_completion_effects_prevented",
                "Duplicate durable completion effects suppressed",
            ),
            http_requests: counter(
                &meter,
                "snaketron.http_requests",
                "Completed application HTTP requests",
            ),
            http4xx_responses: counter(
                &meter,
                "snaketron.http4xx_responses",
                "Application HTTP responses with a 4xx status",
            ),
            http5xx_responses: counter(
                &meter,
                "snaketron.http5xx_responses",
                "Application HTTP responses with a 5xx status",
            ),
            http_request_latency: histogram(
                &meter,
                "snaketron.http_request_latency",
                "Application HTTP request latency in milliseconds",
                LATENCY_BUCKETS_MS,
            ),
            websocket_opens: counter(
                &meter,
                "snaketron.web_socket_opens",
                "Accepted WebSocket sessions",
            ),
            websocket_closes: counter(
                &meter,
                "snaketron.web_socket_closes",
                "Closed WebSocket sessions",
            ),
            websocket_rejected_upgrades: counter(
                &meter,
                "snaketron.web_socket_rejected_upgrades",
                "Rejected WebSocket upgrade requests",
            ),
            websocket_inbound_messages: counter(
                &meter,
                "snaketron.web_socket_inbound_messages",
                "Inbound WebSocket application messages",
            ),
            websocket_inbound_bytes: counter(
                &meter,
                "snaketron.web_socket_inbound_bytes",
                "Inbound WebSocket application payload bytes",
            ),
            websocket_outbound_messages: counter(
                &meter,
                "snaketron.web_socket_outbound_messages",
                "Outbound WebSocket application messages",
            ),
            websocket_outbound_bytes: counter(
                &meter,
                "snaketron.web_socket_outbound_bytes",
                "Outbound WebSocket application payload bytes",
            ),
            websocket_malformed_messages: counter(
                &meter,
                "snaketron.web_socket_malformed_messages",
                "Malformed inbound WebSocket messages",
            ),
            websocket_process_errors: counter(
                &meter,
                "snaketron.web_socket_process_errors",
                "WebSocket message processing failures",
            ),
            websocket_send_errors: counter(
                &meter,
                "snaketron.web_socket_send_errors",
                "WebSocket send failures",
            ),
            websocket_transport_errors: counter(
                &meter,
                "snaketron.web_socket_transport_errors",
                "WebSocket transport failures",
            ),
            websocket_session_duration: histogram(
                &meter,
                "snaketron.websocket_session_duration",
                "WebSocket session duration in milliseconds",
                SESSION_BUCKETS_MS,
            ),
            websocket_resync_requests: counter(
                &meter,
                "snaketron.web_socket_resync_requests",
                "WebSocket state resynchronization requests",
            ),
            websocket_resync_accepted: counter(
                &meter,
                "snaketron.web_socket_resync_accepted",
                "Accepted WebSocket state resynchronizations",
            ),
            websocket_resync_rejected: counter(
                &meter,
                "snaketron.web_socket_resync_rejected",
                "Rejected WebSocket state resynchronizations",
            ),
            matchmaking_admissions: counter(
                &meter,
                "snaketron.matchmaking_admissions",
                "Lobby admissions to matchmaking",
            ),
            matchmaking_admission_deduplications: counter(
                &meter,
                "snaketron.matchmaking_admission_deduplications",
                "Duplicate matchmaking admissions suppressed",
            ),
            matchmaking_admission_rejections: counter(
                &meter,
                "snaketron.matchmaking_admission_rejections",
                "Rejected matchmaking admissions",
            ),
            matchmaking_commits: counter(
                &meter,
                "snaketron.matchmaking_commits",
                "Newly committed matches observed by this process",
            ),
            matchmaking_wait: histogram(
                &meter,
                "snaketron.matchmaking_wait",
                "Committed-match wait time in milliseconds",
                SESSION_BUCKETS_MS,
            ),
            matchmaking_matched_players: counter(
                &meter,
                "snaketron.matchmaking_matched_players",
                "Players included in committed matches",
            ),
            matchmaking_matched_lobbies: counter(
                &meter,
                "snaketron.matchmaking_matched_lobbies",
                "Lobbies included in committed matches",
            ),
            matchmaking_errors: counter(
                &meter,
                "snaketron.matchmaking_errors",
                "Matchmaking operation failures",
            ),
            matchmaking_integrity_errors: counter(
                &meter,
                "snaketron.matchmaking_integrity_errors",
                "Matchmaking state integrity failures",
            ),
            game_created_outbox_delivery_errors: counter(
                &meter,
                "snaketron.game_created_outbox_delivery_errors",
                "Game-created outbox delivery failures",
            ),
            games_completed: counter(
                &meter,
                "snaketron.games_completed",
                "Newly committed completed games observed by this process",
            ),
            game_duration: histogram(
                &meter,
                "snaketron.game_duration",
                "Completed game duration in milliseconds",
                SESSION_BUCKETS_MS,
            ),
            completed_game_players: counter(
                &meter,
                "snaketron.completed_game_players",
                "Players included in completed games",
            ),
            potg_ring_truncated: counter(
                &meter,
                "snaketron.potg_ring_truncated",
                "Completed games whose bounded Play-of-the-Game selection ring evicted history",
            ),
            potg_ring_evicted_seconds: histogram_with_unit(
                &meter,
                "snaketron.ring_evicted_seconds",
                "Whole-match seconds unavailable to Play-of-the-Game selection after oldest-first eviction",
                "s",
                POTG_RING_EVICTED_SECONDS_BUCKETS,
            ),
            redis_requests: counter(
                &meter,
                "snaketron.redis_requests",
                "Application Valkey requests",
            ),
            redis_errors: counter(
                &meter,
                "snaketron.redis_errors",
                "Failed application Valkey requests",
            ),
            redis_request_latency: histogram(
                &meter,
                "snaketron.redis_request_latency",
                "Application Valkey request latency in milliseconds",
                LATENCY_BUCKETS_MS,
            ),
            _observable_gauges: observable_gauges,
        }
    }
}

macro_rules! counter_recorder {
    ($name:ident, $field:ident) => {
        pub(crate) fn $name(value: u64) {
            metrics().$field.add(value, &[]);
        }
    };
}

counter_recorder!(record_fenced_write_rejection, fenced_write_rejections);
counter_recorder!(record_planned_drain_failure, planned_drain_failures);
counter_recorder!(record_command_claims, command_claims);
counter_recorder!(record_command_acks, command_acks);
counter_recorder!(record_command_resends, command_resends);
counter_recorder!(record_command_deduplications, command_deduplications);
counter_recorder!(record_command_rejections, command_rejections);
counter_recorder!(record_checkpoint_writes, checkpoint_writes);
counter_recorder!(record_checkpoint_failures, checkpoint_failures);
counter_recorder!(record_recovered_games, recovered_games);
counter_recorder!(record_recovery_replays, recovery_replays);
counter_recorder!(record_match_claim_conflicts, match_claim_conflicts);
counter_recorder!(
    record_duplicate_completion_effect_prevented,
    duplicate_completion_effects_prevented
);
counter_recorder!(record_websocket_opened, websocket_opens);
counter_recorder!(record_websocket_closed, websocket_closes);
counter_recorder!(
    record_websocket_rejected_upgrade,
    websocket_rejected_upgrades
);
counter_recorder!(
    record_websocket_malformed_message,
    websocket_malformed_messages
);
counter_recorder!(record_websocket_process_error, websocket_process_errors);
counter_recorder!(record_websocket_send_error, websocket_send_errors);
counter_recorder!(record_websocket_transport_error, websocket_transport_errors);
counter_recorder!(record_websocket_resync_requested, websocket_resync_requests);
counter_recorder!(record_websocket_resync_accepted, websocket_resync_accepted);
counter_recorder!(record_websocket_resync_rejected, websocket_resync_rejected);
counter_recorder!(record_matchmaking_admission, matchmaking_admissions);
counter_recorder!(
    record_matchmaking_admission_deduplication,
    matchmaking_admission_deduplications
);
counter_recorder!(
    record_matchmaking_admission_rejection,
    matchmaking_admission_rejections
);
counter_recorder!(record_matchmaking_error, matchmaking_errors);
counter_recorder!(
    record_matchmaking_integrity_error,
    matchmaking_integrity_errors
);
counter_recorder!(
    record_game_created_outbox_delivery_error,
    game_created_outbox_delivery_errors
);

fn boost_attributes(
    game_type: &'static str,
    queue_mode: &'static str,
    team_side: Option<&'static str>,
    speed_band: &'static str,
    pad_id: Option<u8>,
) -> Vec<KeyValue> {
    let mut attributes = vec![
        KeyValue::new("game.type", game_type),
        KeyValue::new("game.queue_mode", queue_mode),
        KeyValue::new("boost.speed_band", speed_band),
    ];
    if let Some(team_side) = team_side {
        attributes.push(KeyValue::new("game.team_side", team_side));
    }
    if let Some(pad_id) = pad_id {
        attributes.push(KeyValue::new("boost.pad_id", i64::from(pad_id)));
    }
    attributes
}

pub(crate) fn record_boost_packet_collected(
    game_type: &'static str,
    queue_mode: &'static str,
    team_side: &'static str,
    speed_band: &'static str,
    pad_id: u8,
) {
    metrics().boost_packet_collections.add(
        1,
        &boost_attributes(
            game_type,
            queue_mode,
            Some(team_side),
            speed_band,
            Some(pad_id),
        ),
    );
}

pub(crate) fn record_boost_pad_respawned(
    game_type: &'static str,
    queue_mode: &'static str,
    speed_band: &'static str,
    pad_id: u8,
) {
    metrics().boost_pad_respawns.add(
        1,
        &boost_attributes(game_type, queue_mode, None, speed_band, Some(pad_id)),
    );
}

pub(crate) fn record_boost_activation_attempt(
    game_type: &'static str,
    queue_mode: &'static str,
    team_side: &'static str,
    speed_band: &'static str,
) {
    metrics().boost_activation_attempts.add(
        1,
        &boost_attributes(game_type, queue_mode, Some(team_side), speed_band, None),
    );
}

pub(crate) fn record_boost_activation_decision(
    game_type: &'static str,
    queue_mode: &'static str,
    team_side: &'static str,
    speed_band: &'static str,
    scheduled: bool,
) {
    let attributes = boost_attributes(game_type, queue_mode, Some(team_side), speed_band, None);
    if scheduled {
        metrics()
            .boost_activation_commands_scheduled
            .add(1, &attributes);
    } else {
        metrics()
            .boost_activation_command_rejections
            .add(1, &attributes);
    }
}

pub(crate) fn record_boost_lifecycle_transition(
    game_type: &'static str,
    queue_mode: &'static str,
    team_side: &'static str,
    speed_band: &'static str,
    activated: bool,
) {
    let attributes = boost_attributes(game_type, queue_mode, Some(team_side), speed_band, None);
    if activated {
        metrics().boost_activations.add(1, &attributes);
    } else {
        metrics().boost_depletions.add(1, &attributes);
    }
}

pub(crate) fn record_boost_manual_stop(
    game_type: &'static str,
    queue_mode: &'static str,
    team_side: &'static str,
    speed_band: &'static str,
) {
    metrics().boost_manual_stops.add(
        1,
        &boost_attributes(game_type, queue_mode, Some(team_side), speed_band, None),
    );
}

fn combo_food_value(points: u32) -> &'static str {
    match points {
        1 => "1",
        2 => "2",
        3 => "3",
        _ => "other",
    }
}

fn combo_attributes(
    game_type: &'static str,
    queue_mode: &'static str,
    team_side: &'static str,
    points: u32,
    boost_active: bool,
) -> [KeyValue; 5] {
    [
        KeyValue::new("game.type", game_type),
        KeyValue::new("game.queue_mode", queue_mode),
        KeyValue::new("game.team_side", team_side),
        // Map unexpected values into one finite fallback instead of allowing a
        // malformed/replayed event to create an unbounded label vocabulary.
        KeyValue::new("food.value", combo_food_value(points)),
        KeyValue::new("boost.active", boost_active),
    ]
}

pub(crate) fn record_combo_food_collected(
    game_type: &'static str,
    queue_mode: &'static str,
    team_side: &'static str,
    points: u32,
    combo_chain: u32,
    combo_remaining_ms_before: u32,
    boost_active: bool,
) {
    let attributes = combo_attributes(game_type, queue_mode, team_side, points, boost_active);
    let metrics = metrics();
    metrics.combo_food_collections.add(1, &attributes);
    metrics
        .combo_points_awarded
        .add(u64::from(points), &attributes);
    metrics
        .combo_chain_depth
        .record(u64::from(combo_chain), &attributes);
    metrics
        .combo_remaining_window_at_collection
        .record(u64::from(combo_remaining_ms_before), &attributes);
}

pub(crate) fn record_http_request(status_code: u16, latency_ms: u64) {
    let metrics = metrics();
    metrics.http_requests.add(1, &[]);
    if (400..500).contains(&status_code) {
        metrics.http4xx_responses.add(1, &[]);
    } else if status_code >= 500 {
        metrics.http5xx_responses.add(1, &[]);
    }
    metrics.http_request_latency.record(latency_ms, &[]);
}

pub(crate) fn record_game_actor_advance(advance_duration_us: u64, batch_quanta: u64, lag_ms: u64) {
    let metrics = metrics();
    metrics
        .game_actor_advance_duration
        .record(advance_duration_us, &[]);
    metrics.game_actor_batch_quanta.record(batch_quanta, &[]);
    metrics.game_actor_lag.record(lag_ms, &[]);
}

pub(crate) fn record_websocket_inbound_message(bytes: u64) {
    let metrics = metrics();
    metrics.websocket_inbound_messages.add(1, &[]);
    metrics.websocket_inbound_bytes.add(bytes, &[]);
}

pub(crate) fn record_websocket_outbound_message(bytes: u64) {
    let metrics = metrics();
    metrics.websocket_outbound_messages.add(1, &[]);
    metrics.websocket_outbound_bytes.add(bytes, &[]);
}

pub(crate) fn record_websocket_session(duration_ms: u64) {
    metrics()
        .websocket_session_duration
        .record(duration_ms, &[]);
}

pub(crate) fn record_matchmaking_commit(wait_ms: u64, players: u64, lobbies: u64) {
    let metrics = metrics();
    metrics.matchmaking_commits.add(1, &[]);
    metrics.matchmaking_matched_players.add(players, &[]);
    metrics.matchmaking_matched_lobbies.add(lobbies, &[]);
    metrics.matchmaking_wait.record(wait_ms, &[]);
}

pub(crate) fn record_game_completed(duration_ms: u64, players: u64) {
    let metrics = metrics();
    metrics.games_completed.add(1, &[]);
    metrics.completed_game_players.add(players, &[]);
    metrics.game_duration.record(duration_ms, &[]);
}

pub(crate) fn record_potg_ring_truncated(evicted_seconds: u64) {
    let metrics = metrics();
    metrics.potg_ring_truncated.add(1, &[]);
    metrics
        .potg_ring_evicted_seconds
        .record(evicted_seconds, &[]);
}

pub(crate) fn record_redis_request(latency_ms: u64, failed: bool) {
    let metrics = metrics();
    metrics.redis_requests.add(1, &[]);
    if failed {
        metrics.redis_errors.add(1, &[]);
    }
    metrics.redis_request_latency.record(latency_ms, &[]);
}

pub(crate) fn update_gauges(snapshot: &GaugeSnapshot) {
    let state = gauge_state();
    macro_rules! store {
        ($field:ident) => {
            state.$field.store(snapshot.$field, Ordering::Relaxed)
        };
    }
    store!(regional_collection_failures);
    store!(ready_tasks);
    store!(live_tasks);
    store!(draining_tasks);
    store!(membership_age_ms);
    store!(assignment_version);
    store!(assignment_age_ms);
    store!(assignment_imbalance);
    store!(active_partition_leases);
    store!(partition_lease_deficit);
    store!(partition_owner_mismatches);
    store!(partition_unowned_ms);
    store!(oldest_pending_command_ms);
    store!(pending_commands);
    store!(pending_completions);
    store!(quarantined_commands);
    store!(checkpoint_age_ms);
    store!(checkpoint_bytes);
    store!(active_games);
    store!(active_game_index_mismatches);
    store!(matchmaking_queue_entries);
    store!(matchmaking_oldest_queued_lobby_ms);
    store!(game_created_outbox_backlog);
    store!(game_created_outbox_oldest_age_ms);
    store!(game_created_outbox_age_index_cardinality_delta);
    store!(local_ready);
    store!(active_websockets);
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
    use opentelemetry_sdk::metrics::{
        InMemoryMetricExporterBuilder, PeriodicReader, SdkMeterProvider, Temporality,
    };

    #[test]
    fn exports_monotonic_counters_histograms_and_attribute_free_gauges() {
        let exporter = InMemoryMetricExporterBuilder::new()
            .with_temporality(Temporality::Cumulative)
            .build();
        let provider = SdkMeterProvider::builder()
            .with_reader(PeriodicReader::builder(exporter.clone()).build())
            .build();
        let instruments = OtelMetrics::from_meter(provider.meter("snaketron-test"));

        instruments.http_requests.add(1, &[]);
        instruments.http4xx_responses.add(1, &[]);
        instruments.http_request_latency.record(37, &[]);
        instruments.websocket_session_duration.record(1_500, &[]);
        instruments.redis_requests.add(1, &[]);
        instruments.redis_errors.add(1, &[]);
        instruments.redis_request_latency.record(12, &[]);
        instruments.boost_packet_collections.add(
            1,
            &boost_attributes("2v2", "competitive", Some("team-1"), "1.26-1.50x", Some(3)),
        );
        instruments.boost_activations.add(
            1,
            &boost_attributes("2v2", "competitive", Some("team-1"), "1.76-2.00x", None),
        );
        instruments.boost_manual_stops.add(
            1,
            &boost_attributes("2v2", "competitive", Some("team-1"), "1.76-2.00x", None),
        );
        let combo_metric_attributes = combo_attributes("2v2", "competitive", "team-1", 3, true);
        instruments
            .combo_food_collections
            .add(1, &combo_metric_attributes);
        instruments
            .combo_points_awarded
            .add(3, &combo_metric_attributes);
        instruments
            .combo_chain_depth
            .record(7, &combo_metric_attributes);
        instruments
            .combo_remaining_window_at_collection
            .record(425, &combo_metric_attributes);
        instruments.game_actor_advance_duration.record(730, &[]);
        instruments.game_actor_batch_quanta.record(3, &[]);
        instruments.game_actor_lag.record(50, &[]);
        instruments.potg_ring_truncated.add(1, &[]);
        instruments.potg_ring_evicted_seconds.record(17, &[]);
        update_gauges(&GaugeSnapshot {
            live_tasks: 3,
            active_websockets: 7,
            ..GaugeSnapshot::default()
        });

        provider
            .force_flush()
            .expect("metric collection should flush");
        let exported = exporter
            .get_finished_metrics()
            .expect("in-memory metrics should be readable");
        let all_metrics = exported
            .iter()
            .flat_map(|resource| resource.scope_metrics())
            .flat_map(|scope| scope.metrics())
            .collect::<Vec<_>>();

        let metric = |name: &str| {
            all_metrics
                .iter()
                .copied()
                .find(|candidate| candidate.name() == name)
                .unwrap_or_else(|| panic!("missing exported metric {name}"))
        };

        match metric("snaketron.http_requests").data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                assert!(sum.is_monotonic());
                assert_eq!(sum.temporality(), Temporality::Cumulative);
                let points = sum.data_points().collect::<Vec<_>>();
                assert_eq!(points.len(), 1);
                assert_eq!(points[0].value(), 1);
                assert_eq!(points[0].attributes().count(), 0);
            }
            data => panic!("HTTP requests used the wrong aggregation: {data:?}"),
        }

        for name in [
            "snaketron.http_request_latency",
            "snaketron.websocket_session_duration",
            "snaketron.redis_request_latency",
            "snaketron.game_actor.lag",
        ] {
            let exported_histogram = metric(name);
            assert_eq!(exported_histogram.unit(), "ms");
            match exported_histogram.data() {
                AggregatedMetrics::U64(MetricData::Histogram(histogram)) => {
                    let points = histogram.data_points().collect::<Vec<_>>();
                    assert_eq!(points.len(), 1);
                    assert_eq!(points[0].attributes().count(), 0);
                }
                data => panic!("{name} used the wrong aggregation: {data:?}"),
            }
        }

        for (name, unit) in [
            ("snaketron.game_actor.advance_duration", "us"),
            ("snaketron.game_actor.batch_quanta", "1"),
            ("snaketron.ring_evicted_seconds", "s"),
        ] {
            let exported_histogram = metric(name);
            assert_eq!(exported_histogram.unit(), unit);
            match exported_histogram.data() {
                AggregatedMetrics::U64(MetricData::Histogram(histogram)) => {
                    let points = histogram.data_points().collect::<Vec<_>>();
                    assert_eq!(points.len(), 1);
                    assert_eq!(points[0].attributes().count(), 0);
                }
                data => panic!("{name} used the wrong aggregation: {data:?}"),
            }
        }

        match metric("snaketron.boost.packet_collections").data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                let points = sum.data_points().collect::<Vec<_>>();
                assert_eq!(points.len(), 1);
                assert_eq!(points[0].value(), 1);
                assert_eq!(points[0].attributes().count(), 5);
            }
            data => panic!("Boost packet collections used the wrong aggregation: {data:?}"),
        }

        match metric("snaketron.boost.activations").data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                let points = sum.data_points().collect::<Vec<_>>();
                assert_eq!(points.len(), 1);
                assert_eq!(points[0].value(), 1);
                assert_eq!(points[0].attributes().count(), 4);
            }
            data => panic!("Boost activations used the wrong aggregation: {data:?}"),
        }

        match metric("snaketron.boost.manual_stops").data() {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                let points = sum.data_points().collect::<Vec<_>>();
                assert_eq!(points.len(), 1);
                assert_eq!(points[0].value(), 1);
                assert_eq!(points[0].attributes().count(), 4);
            }
            data => panic!("Boost manual stops used the wrong aggregation: {data:?}"),
        }

        for (name, expected_value) in [
            ("snaketron.combo.food_collections", 1),
            ("snaketron.combo.points_awarded", 3),
        ] {
            match metric(name).data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                    assert!(sum.is_monotonic());
                    let points = sum.data_points().collect::<Vec<_>>();
                    assert_eq!(points.len(), 1);
                    assert_eq!(points[0].value(), expected_value);
                    let attributes = points[0].attributes().cloned().collect::<Vec<_>>();
                    assert_eq!(attributes.len(), 5);
                    for expected in combo_attributes("2v2", "competitive", "team-1", 3, true) {
                        assert!(
                            attributes.contains(&expected),
                            "{name} missing {expected:?}"
                        );
                    }
                    assert!(attributes.iter().all(|attribute| {
                        !matches!(
                            attribute.key.as_str(),
                            "game.id" | "user.id" | "player.id" | "snake.id"
                        )
                    }));
                }
                data => panic!("{name} used the wrong aggregation: {data:?}"),
            }
        }

        for (name, unit, expected_sum) in [
            ("snaketron.combo.chain_depth", "1", 7),
            ("snaketron.combo.remaining_window_at_collection", "ms", 425),
        ] {
            let exported_histogram = metric(name);
            assert_eq!(exported_histogram.unit(), unit);
            match exported_histogram.data() {
                AggregatedMetrics::U64(MetricData::Histogram(histogram)) => {
                    let points = histogram.data_points().collect::<Vec<_>>();
                    assert_eq!(points.len(), 1);
                    assert_eq!(points[0].count(), 1);
                    assert_eq!(points[0].sum(), expected_sum);
                    let attributes = points[0].attributes().cloned().collect::<Vec<_>>();
                    assert_eq!(attributes.len(), 5);
                    for expected in combo_attributes("2v2", "competitive", "team-1", 3, true) {
                        assert!(
                            attributes.contains(&expected),
                            "{name} missing {expected:?}"
                        );
                    }
                }
                data => panic!("{name} used the wrong aggregation: {data:?}"),
            }
        }

        for (name, expected) in [
            ("snaketron.live_tasks", 3),
            ("snaketron.active_web_sockets", 7),
        ] {
            match metric(name).data() {
                AggregatedMetrics::U64(MetricData::Gauge(gauge)) => {
                    let points = gauge.data_points().collect::<Vec<_>>();
                    assert_eq!(points.len(), 1);
                    assert_eq!(points[0].value(), expected);
                    assert_eq!(points[0].attributes().count(), 0);
                }
                data => panic!("{name} used the wrong aggregation: {data:?}"),
            }
        }

        provider
            .shutdown()
            .expect("metric provider should shut down");
    }

    #[test]
    fn combo_food_value_attribute_has_a_finite_vocabulary() {
        for (points, expected) in [(1, "1"), (2, "2"), (3, "3"), (0, "other"), (999, "other")] {
            assert_eq!(combo_food_value(points), expected);
        }
    }

    #[test]
    fn combo_remaining_window_histogram_covers_the_production_default() {
        assert_eq!(
            COMBO_REMAINING_WINDOW_BUCKETS_MS.last().copied(),
            Some(f64::from(common::DEFAULT_COMBO_WINDOW_MS))
        );
        assert!(
            COMBO_REMAINING_WINDOW_BUCKETS_MS
                .windows(2)
                .all(|pair| pair[0] < pair[1])
        );
    }
}
