//! Chaos/equivalence tests for client<->server state synchronization.
//!
//! These tests run a fully in-process simulation of the production topology:
//! an authoritative server-side `GameEngine` advanced on a virtual clock the
//! way `game_executor::run_game` does, a deterministic transport with
//! configurable latency/jitter/loss standing in for the server bus + WebSocket,
//! and a client-side `GameEngine` that sees only what the transport delivers.
//! No network, no Redis, no sleeps — a 60-virtual-second game runs in
//! milliseconds and is bit-for-bit reproducible from its seeds.

use anyhow::Result;
use common::{
    Direction, GameCommand, GameCommandMessage, GameEngine, GameEvent, GameEventMessage, GameState,
    GameStatus, GameType, MAX_PREDICTION_AHEAD_MS, PseudoRandom, QueueMode,
};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::Duration;

const GAME_ID: u32 = 777;
/// Matches `EXECUTOR_POLL_INTERVAL_MS`: the executor advances the engine on a
/// 50ms interval.
const POLL_MS: i64 = 50;
/// Client animation-frame cadence.
const FRAME_MS: i64 = 16;
/// The server emits a `TickHash` heartbeat every this many committed ticks.
const PROBE_EVERY_TICKS: u32 = 10;
/// `GameEngine`'s internal `committed_state_lag_ms` (not exported).
const COMMITTED_LAG_MS: u32 = 500;

// ---------------------------------------------------------------------------
// Transport simulator
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct TransportConfig {
    base_latency_ms: i64,
    jitter_ms: i64,
    /// Applied to server->client event messages only (commands ride the
    /// client's own WebSocket; snapshots are exempt as the reliable join /
    /// recovery path).
    drop_probability: f32,
    /// When false (the default topology), per-direction FIFO ordering is
    /// enforced even under jitter, like a single TCP/WebSocket stream.
    allow_reorder: bool,
    seed: u64,
}

impl TransportConfig {
    fn lossless() -> Self {
        TransportConfig {
            base_latency_ms: 120,
            jitter_ms: 40,
            drop_probability: 0.0,
            allow_reorder: false,
            seed: 7,
        }
    }
}

enum Wire {
    ToClient(Box<GameEventMessage>),
    ToServer(Box<GameCommandMessage>),
    /// Client asking the executor for a fresh snapshot (resync protocol).
    SnapshotRequest,
}

struct InFlight {
    arrival_ms: i64,
    order: u64,
    payload: Wire,
}

impl PartialEq for InFlight {
    fn eq(&self, other: &Self) -> bool {
        (self.arrival_ms, self.order) == (other.arrival_ms, other.order)
    }
}
impl Eq for InFlight {}
impl PartialOrd for InFlight {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for InFlight {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.arrival_ms, self.order).cmp(&(other.arrival_ms, other.order))
    }
}

struct Transport {
    cfg: TransportConfig,
    rng: PseudoRandom,
    queue: BinaryHeap<Reverse<InFlight>>,
    next_order: u64,
    last_to_client_arrival: i64,
    last_to_server_arrival: i64,
    /// Hard kill switch: when false every send is discarded (server silent).
    delivery_enabled: bool,
    dropped_to_client: u64,
}

impl Transport {
    fn new(cfg: TransportConfig) -> Self {
        Transport {
            cfg,
            rng: PseudoRandom::new(cfg.seed),
            queue: BinaryHeap::new(),
            next_order: 0,
            last_to_client_arrival: 0,
            last_to_server_arrival: 0,
            delivery_enabled: true,
            dropped_to_client: 0,
        }
    }

    fn sample_latency(&mut self) -> i64 {
        if self.cfg.jitter_ms == 0 {
            return self.cfg.base_latency_ms;
        }
        let span = (self.cfg.jitter_ms * 2 + 1) as u32;
        self.cfg.base_latency_ms - self.cfg.jitter_ms + (self.rng.next_u32() % span) as i64
    }

    fn enqueue(&mut self, now_ms: i64, latency_ms: i64, payload: Wire) {
        if !self.delivery_enabled {
            return;
        }
        let mut arrival = now_ms + latency_ms.max(1);
        if !self.cfg.allow_reorder {
            let last = match payload {
                Wire::ToClient(_) => &mut self.last_to_client_arrival,
                Wire::ToServer(_) | Wire::SnapshotRequest => &mut self.last_to_server_arrival,
            };
            arrival = arrival.max(*last);
            *last = arrival;
        }
        let order = self.next_order;
        self.next_order += 1;
        self.queue.push(Reverse(InFlight {
            arrival_ms: arrival,
            order,
            payload,
        }));
    }

    fn send_to_client(&mut self, now_ms: i64, msg: GameEventMessage) {
        // Snapshots are the reliable join/recovery path; everything else
        // rides hops that can drop (broadcast lag, the WebSocket leg), so
        // the simulation lets it be dropped.
        let droppable = !matches!(msg.event, GameEvent::Snapshot { .. });
        if droppable
            && self.cfg.drop_probability > 0.0
            && self.delivery_enabled
            && self.rng.next_f32() < self.cfg.drop_probability
        {
            self.dropped_to_client += 1;
            return;
        }
        let latency = self.sample_latency();
        self.enqueue(now_ms, latency, Wire::ToClient(Box::new(msg)));
    }

    fn send_to_server(&mut self, now_ms: i64, cmd: GameCommandMessage) {
        let latency = self.sample_latency();
        self.enqueue(now_ms, latency, Wire::ToServer(Box::new(cmd)));
    }

    fn send_to_server_with_latency(
        &mut self,
        now_ms: i64,
        cmd: GameCommandMessage,
        latency_ms: i64,
    ) {
        self.enqueue(now_ms, latency_ms, Wire::ToServer(Box::new(cmd)));
    }

    fn send_snapshot_request(&mut self, now_ms: i64) {
        let latency = self.sample_latency();
        self.enqueue(now_ms, latency, Wire::SnapshotRequest);
    }

    fn pop_due(&mut self, now_ms: i64) -> Option<Wire> {
        if let Some(Reverse(head)) = self.queue.peek()
            && head.arrival_ms <= now_ms
        {
            return self.queue.pop().map(|Reverse(f)| f.payload);
        }
        None
    }

    fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn cut(&mut self) {
        self.delivery_enabled = false;
        self.queue.clear();
    }
}

// ---------------------------------------------------------------------------
// Simulation world: server engine + transport + client engine
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct ProbeResult {
    #[allow(dead_code)] // kept for debug output completeness
    tick: u32,
    matched: bool,
}

struct SimWorld {
    now_ms: i64,
    server: GameEngine,
    client: Option<GameEngine>,
    transport: Transport,
    /// Monotonic transport sequence, assigned at publish time like the
    /// executor does. Starts at 1 for the initial join snapshot.
    stream_seq: u64,
    next_probe_tick: u32,
    clock_drift_ms: i64,
    /// Full published stream for determinism comparisons:
    /// (tick, stream_seq, engine sequence, event as JSON value).
    published: Vec<(u32, u64, u64, serde_json::Value)>,
    record_published: bool,
    /// Every server-confirmed command, in processing order.
    confirmations: Vec<GameCommandMessage>,
    probe_log: Vec<ProbeResult>,
    ever_needed_resync: bool,
    auto_resync: bool,
    resync_in_flight: bool,
    /// When set, the first `SnakeRespawned` for snake 1 (the enemy from the
    /// client's perspective) is dropped at publish time while still consuming
    /// its stream_seq — modeling a failed executor publish or a client-side
    /// delivery drop of exactly that message. The paired `SnakeDied` is
    /// delivered normally.
    drop_first_enemy_respawn: bool,
    /// (tick, stream_seq) of the dropped respawn, once it happened.
    dropped_respawn: Option<(u32, u64)>,
}

impl SimWorld {
    fn new(game_seed: u64, cfg: TransportConfig) -> Self {
        // Mirror the production game-creation path (matchmaking + executor):
        // players join a Stopped tick-0 state, initial food is spawned from
        // the seeded RNG, the executor flips it to Started and builds the
        // engine from that state.
        let mut state = GameState::new(
            40,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            Some(game_seed),
            0,
        );
        state
            .add_player(1, Some("alice".to_string()))
            .expect("add player 1");
        state
            .add_player(2, Some("bob".to_string()))
            .expect("add player 2");
        state.status = GameStatus::Started { server_id: 7 };
        state.spawn_initial_food();

        let server = GameEngine::new_from_state(GAME_ID, state);
        let mut world = SimWorld {
            now_ms: 0,
            server,
            client: None,
            transport: Transport::new(cfg),
            stream_seq: 0,
            next_probe_tick: PROBE_EVERY_TICKS,
            clock_drift_ms: 0,
            published: Vec::new(),
            record_published: false,
            confirmations: Vec::new(),
            probe_log: Vec::new(),
            ever_needed_resync: false,
            auto_resync: false,
            resync_in_flight: false,
            drop_first_enemy_respawn: false,
            dropped_respawn: None,
        };

        // Initial snapshot at t0, the way a client join does (stream_seq 1).
        world.publish_snapshot();
        world
    }

    fn tick_duration_ms(&self) -> i64 {
        self.server.committed_state().properties.tick_duration_ms as i64
    }

    fn publish(&mut self, tick: u32, sequence: u64, event: GameEvent) {
        self.stream_seq += 1;
        if self.drop_first_enemy_respawn
            && self.dropped_respawn.is_none()
            && matches!(event, GameEvent::SnakeRespawned { snake_id: 1, .. })
        {
            self.dropped_respawn = Some((tick, self.stream_seq));
            return;
        }
        if self.record_published {
            self.published.push((
                tick,
                self.stream_seq,
                sequence,
                serde_json::to_value(&event).expect("event serializes"),
            ));
        }
        let msg = GameEventMessage {
            game_id: GAME_ID,
            tick,
            sequence,
            stream_seq: self.stream_seq,
            user_id: None,
            event,
        };
        self.transport.send_to_client(self.now_ms, msg);
    }

    fn publish_snapshot(&mut self) {
        let state = self.server.committed_state().clone();
        let tick = state.tick;
        let sequence = state.event_sequence;
        self.publish(tick, sequence, GameEvent::Snapshot { game_state: state });
    }

    fn publish_probe(&mut self) {
        let tick = self.server.current_tick();
        let sequence = self.server.committed_state().event_sequence;
        let event = GameEvent::TickHash {
            hash: self.server.committed_sync_hash(),
            server_ts_ms: self.now_ms,
        };
        self.publish(tick, sequence, event);
    }

    /// One executor poll: advance the authoritative engine by wall clock and
    /// publish every emitted event, plus a TickHash heartbeat every
    /// `PROBE_EVERY_TICKS` committed ticks.
    fn server_poll(&mut self) {
        let events = self
            .server
            .run_until(self.now_ms)
            .expect("server run_until");
        for (tick, sequence, event) in events {
            self.publish(tick, sequence, event);
        }
        if self.server.current_tick() >= self.next_probe_tick {
            self.publish_probe();
            self.next_probe_tick = self.server.current_tick() + PROBE_EVERY_TICKS;
        }
    }

    fn server_receive_command(&mut self, cmd: GameCommandMessage) {
        let confirmed = self
            .server
            .process_command(cmd)
            .expect("server process_command");
        self.confirmations.push(confirmed.clone());
        let tick = self.server.current_tick();
        let sequence = self.server.committed_state().event_sequence + 1;
        self.publish(
            tick,
            sequence,
            GameEvent::CommandScheduled {
                command_message: confirmed,
            },
        );
    }

    fn client_receive(&mut self, msg: GameEventMessage) {
        if self.client.is_none() {
            let GameEvent::Snapshot { game_state } = &msg.event else {
                return; // not joined yet
            };
            let mut engine = GameEngine::new_from_state(GAME_ID, game_state.clone());
            engine.set_local_player_id(1);
            self.client = Some(engine);
        }

        let is_probe = matches!(msg.event, GameEvent::TickHash { .. });
        let is_snapshot = matches!(msg.event, GameEvent::Snapshot { .. });

        self.client
            .as_mut()
            .expect("client exists")
            .process_server_event(&msg)
            .expect("client process_server_event");

        if is_snapshot {
            self.resync_in_flight = false;
        }

        let (needs_resync, probe) = {
            let status = self.client.as_ref().expect("client exists").sync_status();
            let probe = if is_probe && status.last_probe_tick == Some(msg.tick) {
                Some(ProbeResult {
                    tick: msg.tick,
                    matched: status.last_probe_matched.unwrap_or(false),
                })
            } else {
                None
            };
            (status.needs_resync, probe)
        };
        if let Some(probe) = probe {
            self.probe_log.push(probe);
        }
        if needs_resync {
            self.ever_needed_resync = true;
            if self.auto_resync && !self.resync_in_flight {
                self.request_resync();
            }
        }
    }

    /// The resync protocol: clear the flag (request issued), ask the server
    /// for a fresh snapshot over the transport.
    fn request_resync(&mut self) {
        if let Some(client) = self.client.as_mut() {
            client.clear_needs_resync();
        }
        self.resync_in_flight = true;
        self.transport.send_snapshot_request(self.now_ms);
    }

    fn deliver_due(&mut self) {
        while let Some(wire) = self.transport.pop_due(self.now_ms) {
            match wire {
                Wire::ToClient(msg) => self.client_receive(*msg),
                Wire::ToServer(cmd) => self.server_receive_command(*cmd),
                Wire::SnapshotRequest => self.publish_snapshot(),
            }
        }
    }

    fn step_ms(&mut self) {
        self.now_ms += 1;
        if self.now_ms % POLL_MS == 0 {
            self.server_poll();
        }
        self.deliver_due();
        if self.now_ms % FRAME_MS == 0 {
            let drift = self.clock_drift_ms;
            let now = self.now_ms;
            if let Some(client) = self.client.as_mut() {
                client
                    .rebuild_predicted_state(now - drift)
                    .expect("rebuild_predicted_state");
            }
            if self.auto_resync && !self.resync_in_flight {
                let needs = self
                    .client
                    .as_ref()
                    .map(|c| c.sync_status().needs_resync)
                    .unwrap_or(false);
                if needs {
                    self.ever_needed_resync = true;
                    self.request_resync();
                }
            }
        }
    }

    fn run_for(&mut self, duration_ms: i64) {
        let end = self.now_ms + duration_ms;
        while self.now_ms < end {
            self.step_ms();
        }
    }

    fn client_send_turn(&mut self, direction: Direction) -> GameCommandMessage {
        let cmd = self
            .client
            .as_mut()
            .expect("client joined")
            .process_local_command(GameCommand::Turn {
                snake_id: 0,
                direction,
            })
            .expect("process_local_command");
        self.transport.send_to_server(self.now_ms, cmd.clone());
        cmd
    }

    fn client_send_turn_with_latency(
        &mut self,
        direction: Direction,
        latency_ms: i64,
    ) -> GameCommandMessage {
        let cmd = self
            .client
            .as_mut()
            .expect("client joined")
            .process_local_command(GameCommand::Turn {
                snake_id: 0,
                direction,
            })
            .expect("process_local_command");
        self.transport
            .send_to_server_with_latency(self.now_ms, cmd.clone(), latency_ms);
        cmd
    }

    /// Deliver everything still in flight (without advancing the game), then
    /// emit one final fingerprint probe so both committed states end at the
    /// same tick and can be compared directly.
    fn drain_and_probe(&mut self) {
        let mut guard = 0;
        while !self.transport.is_empty() {
            self.now_ms += 1;
            self.deliver_due();
            guard += 1;
            assert!(guard < 1_000_000, "transport failed to drain");
        }
        self.publish_probe();
        while !self.transport.is_empty() {
            self.now_ms += 1;
            self.deliver_due();
        }
    }

    fn client(&self) -> &GameEngine {
        self.client.as_ref().expect("client joined")
    }
}

async fn with_timeout<F>(fut: F) -> Result<()>
where
    F: std::future::Future<Output = Result<()>>,
{
    tokio::time::timeout(Duration::from_secs(10), fut)
        .await
        .map_err(|_| anyhow::anyhow!("test timed out after 10s"))?
}

/// The shared lossless scenario: 60 virtual seconds, 120ms±40ms latency, no
/// loss, several player commands routed client -> server -> broadcast.
fn run_lossless_scenario(game_seed: u64, transport_seed: u64, record: bool) -> SimWorld {
    let cfg = TransportConfig {
        seed: transport_seed,
        ..TransportConfig::lossless()
    };
    let mut world = SimWorld::new(game_seed, cfg);
    world.record_published = record;

    world.run_for(5_000);
    for direction in [
        Direction::Up,
        Direction::Right,
        Direction::Down,
        Direction::Right,
        Direction::Up,
        Direction::Right,
    ] {
        world.client_send_turn(direction);
        world.run_for(5_000);
    }
    world.run_for(25_000); // total: 60 virtual seconds
    world.drain_and_probe();
    world
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lossless_transport_stays_in_sync() -> Result<()> {
    with_timeout(async {
        let world = run_lossless_scenario(0xC0FFEE, 7, false);

        let status = world.client().sync_status();
        assert!(
            status.total_probes > 0,
            "expected fingerprint probes to run, got none"
        );
        assert_eq!(
            status.total_mismatches, 0,
            "lossless transport must never diverge; probe log: {:?}",
            world.probe_log
        );
        assert_eq!(status.stream_gap_count, 0, "no gaps on lossless transport");
        assert!(
            !status.needs_resync,
            "no resync needed on lossless transport"
        );
        assert_eq!(
            world.client().current_tick(),
            world.server.current_tick(),
            "final probe fast-forwards the client to the server tick"
        );
        assert_eq!(
            world.client().committed_sync_hash(),
            world.server.committed_sync_hash(),
            "final committed states must be identical"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn lossy_transport_detects_gaps_and_resyncs() -> Result<()> {
    with_timeout(async {
        let cfg = TransportConfig {
            drop_probability: 0.05,
            seed: 21,
            ..TransportConfig::lossless()
        };
        let mut world = SimWorld::new(0xBADF00D, cfg);

        // Lossy phase: no commands needed, the game generates plenty of
        // traffic (movement, deaths, respawns, food) on its own.
        world.run_for(30_000);

        let status = world.client().sync_status();
        assert!(
            world.transport.dropped_to_client > 0,
            "transport should have dropped messages at 5%"
        );
        assert!(
            status.stream_gap_count > 0,
            "dropped messages must surface as stream gaps"
        );
        assert!(
            status.needs_resync,
            "a gap means the committed state can no longer be trusted"
        );

        // Recovery: stop the loss, run the resync protocol.
        world.transport.cfg.drop_probability = 0.0;
        let probes_before_resync = world.probe_log.len();
        world.request_resync();
        world.run_for(10_000);
        world.drain_and_probe();

        let status = world.client().sync_status();
        assert!(
            !status.needs_resync,
            "snapshot must clear needs_resync: {status:?}"
        );
        let post_resync: Vec<ProbeResult> = world.probe_log[probes_before_resync..].to_vec();
        assert!(
            !post_resync.is_empty(),
            "expected probes after the resync snapshot"
        );
        assert!(
            post_resync.iter().all(|p| p.matched),
            "probes after resync must match again: {post_resync:?}"
        );
        assert_eq!(
            world.client().committed_sync_hash(),
            world.server.committed_sync_hash(),
            "resynced client must converge to the server state"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn targeted_event_loss_detected_by_tickhash() -> Result<()> {
    with_timeout(async {
        let mut world = SimWorld::new(0xFEED, TransportConfig::lossless());

        // Establish a healthy baseline first.
        world.run_for(15_000);
        let baseline = world.client().sync_status().clone();
        assert!(baseline.total_probes > 0, "baseline probes should have run");
        assert_eq!(
            baseline.total_mismatches, 0,
            "baseline must be in sync before corrupting: {:?}",
            world.probe_log
        );

        // With stream_seq, dropping a single FoodSpawned always also tells
        // the client via the gap counter — so to exercise the hash path in
        // isolation, corrupt the client's committed food set directly by one
        // cell. stream_seq 0 bypasses the watermark (unassigned/legacy), and
        // (0, 0) is outside the food spawn area so the server can never
        // accidentally repair it.
        let corruption = GameEventMessage {
            game_id: GAME_ID,
            tick: world.client().current_tick(),
            sequence: 0,
            stream_seq: 0,
            user_id: None,
            event: GameEvent::FoodSpawned {
                position: common::Position { x: 0, y: 0 },
            },
        };
        world
            .client
            .as_mut()
            .expect("client joined")
            .process_server_event(&corruption)
            .expect("corruption event applies");

        // Probes run every PROBE_EVERY_TICKS ticks (1 virtual second); give
        // the heartbeat time for at least two of them.
        world.run_for(4_000);

        let status = world.client().sync_status();
        assert_eq!(
            status.last_probe_matched,
            Some(false),
            "TickHash must detect the corrupted food set: {status:?}"
        );
        assert!(
            status.consecutive_hash_mismatches >= 2,
            "mismatch must persist across probes: {status:?}"
        );
        assert!(
            status.total_mismatches >= 2,
            "expected repeated mismatches: {status:?}"
        );
        assert!(
            status.first_mismatch_tick.is_some(),
            "first mismatch tick recorded for RCA"
        );
        assert!(
            status.needs_resync,
            "2 consecutive mismatches must request a resync"
        );
        Ok(())
    })
    .await
}

/// The enemy-respawn desync shape: at a TeamMatch crash tick the engine emits
/// `SnakeDied` and `SnakeRespawned` as two adjacent messages. If exactly the
/// respawn is lost (failed executor publish, or a client-side delivery drop),
/// the delivered `SnakeDied` re-kills the snake the client's committed
/// catch-up had already respawned locally — and nothing short of a snapshot
/// ever revives it: the enemy stays dead on that client while it keeps
/// playing on the server. The client must (a) detect the loss via the
/// stream_seq gap and (b) fully converge after the resync snapshot.
#[tokio::test]
async fn lost_enemy_respawn_is_detected_and_healed_by_resync() -> Result<()> {
    with_timeout(async {
        let mut world = SimWorld::new(0xE5CA9E, TransportConfig::lossless());
        world.drop_first_enemy_respawn = true;

        // Passive snakes crash and respawn organically; run until the enemy's
        // first respawn has been dropped at publish time.
        let mut waited_ms = 0;
        while world.dropped_respawn.is_none() && waited_ms < 60_000 {
            world.run_for(1_000);
            waited_ms += 1_000;
        }
        let (drop_tick, drop_seq) = world
            .dropped_respawn
            .expect("enemy snake should crash and respawn within 60 virtual seconds");

        // Let the following messages arrive: the gap after the consumed
        // stream_seq must be detected, and the divergence window — enemy
        // alive on the server, dead on the client — must be observable.
        world.run_for(1_000);
        assert!(
            world.server.committed_state().arena.snakes[1].is_alive,
            "server-side enemy must be alive after its respawn at tick {drop_tick}"
        );
        assert!(
            !world.client().committed_state().arena.snakes[1].is_alive,
            "client-side enemy must be stranded dead after the lost respawn \
             (seq {drop_seq}): the delivered SnakeDied re-kills the locally \
             respawned snake and nothing else revives it"
        );
        let status = world.client().sync_status();
        assert!(
            status.stream_gap_count >= 1,
            "the consumed-but-undelivered stream_seq must surface as a gap: {status:?}"
        );
        assert!(
            status.needs_resync,
            "a gap means the committed state can no longer be trusted: {status:?}"
        );

        // The resync protocol (debounced in the real UI) heals it.
        let probes_before_resync = world.probe_log.len();
        world.request_resync();
        world.run_for(5_000);
        world.drain_and_probe();

        let status = world.client().sync_status();
        assert!(
            !status.needs_resync,
            "snapshot must clear needs_resync: {status:?}"
        );
        let post_resync: Vec<ProbeResult> = world.probe_log[probes_before_resync..].to_vec();
        assert!(
            !post_resync.is_empty(),
            "expected probes after the resync snapshot"
        );
        assert!(
            post_resync.iter().all(|p| p.matched),
            "probes after resync must match again: {post_resync:?}"
        );
        assert_eq!(
            world.client().committed_state().arena.snakes[1].is_alive,
            world.server.committed_state().arena.snakes[1].is_alive,
            "enemy liveness must agree after the resync"
        );
        assert_eq!(
            world.client().committed_sync_hash(),
            world.server.committed_sync_hash(),
            "resynced client must converge to the server state"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn prediction_freezes_when_server_goes_silent() -> Result<()> {
    with_timeout(async {
        let mut world = SimWorld::new(0xD1CE, TransportConfig::lossless());

        // Healthy phase.
        world.run_for(10_000);
        let committed_at_cut = world.client().current_tick();
        assert!(committed_at_cut > 0, "client should have advanced");

        // Server goes silent: everything in flight is lost, nothing new
        // arrives. The client keeps animating.
        world.transport.cut();

        let tick_ms = world.tick_duration_ms();
        let cap_ticks = ((COMMITTED_LAG_MS + MAX_PREDICTION_AHEAD_MS) as i64 / tick_ms) as u32 + 1;

        world.run_for(5_000);
        let predicted_after_5s = world.client().get_predicted_tick();
        let committed_after_5s = world.client().current_tick();
        assert_eq!(
            committed_after_5s, committed_at_cut,
            "committed state must freeze without server messages"
        );
        assert!(
            predicted_after_5s - committed_after_5s <= cap_ticks,
            "prediction must stay within the cap: predicted {predicted_after_5s}, \
             committed {committed_after_5s}, cap {cap_ticks}"
        );

        // 25 more virtual seconds of animation frames: prediction must have
        // stopped growing entirely.
        world.run_for(25_000);
        let predicted_after_30s = world.client().get_predicted_tick();
        assert_eq!(
            predicted_after_30s, predicted_after_5s,
            "prediction must freeze, not creep forward, while the server is silent"
        );
        assert!(
            predicted_after_30s - world.client().current_tick() <= cap_ticks,
            "cap still holds after 30 silent seconds"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn engine_determinism_across_reruns() -> Result<()> {
    with_timeout(async {
        let a = run_lossless_scenario(0xC0FFEE, 7, true);
        let b = run_lossless_scenario(0xC0FFEE, 7, true);

        assert_eq!(
            a.published.len(),
            b.published.len(),
            "reruns must publish the same number of messages"
        );
        for (i, (ea, eb)) in a.published.iter().zip(b.published.iter()).enumerate() {
            assert_eq!(
                ea, eb,
                "published stream diverged between identical reruns at index {i}"
            );
        }
        assert_eq!(
            a.server.current_tick(),
            b.server.current_tick(),
            "server ticks must match across reruns"
        );
        assert_eq!(
            a.server.committed_sync_hash(),
            b.server.committed_sync_hash(),
            "final committed hashes must match across reruns"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn late_command_reschedules() -> Result<()> {
    with_timeout(async {
        let cfg = TransportConfig {
            base_latency_ms: 100,
            jitter_ms: 0,
            ..TransportConfig::lossless()
        };
        let mut world = SimWorld::new(0xABCDEF, cfg);
        world.auto_resync = true;

        world.run_for(5_000);

        // One command stuck in the network for 800ms — well past the 500ms
        // committed-lag window the client's optimistic schedule relies on.
        let late_cmd = world.client_send_turn_with_latency(Direction::Up, 800);
        world.run_for(2_000);

        let confirmation = world
            .confirmations
            .iter()
            .find(|c| c.command_id_client == late_cmd.command_id_client)
            .expect("server confirmed the late command");
        let server_tick = confirmation
            .command_id_server
            .as_ref()
            .expect("server assigned an id")
            .tick;
        // The phantom-death mechanism: the client already played the turn at
        // its requested tick, but the server rescheduled it later — the two
        // simulations executed the same command at different ticks.
        assert!(
            server_tick > late_cmd.command_id_client.tick,
            "late command must be rescheduled: client tick {}, server tick {}",
            late_cmd.command_id_client.tick,
            server_tick
        );

        // The client must still converge: hash probes flag the divergence,
        // the resync protocol heals it, and subsequent probes match.
        world.run_for(20_000);
        world.drain_and_probe();

        let status = world.client().sync_status();
        assert_eq!(
            status.consecutive_hash_mismatches, 0,
            "divergence must have cleared: {status:?}"
        );
        assert_eq!(
            status.last_probe_matched,
            Some(true),
            "final probe must match after recovery: {status:?}"
        );
        assert!(!status.needs_resync, "no pending resync at the end");
        assert_eq!(
            world.client().committed_sync_hash(),
            world.server.committed_sync_hash(),
            "client must converge to the server state"
        );
        Ok(())
    })
    .await
}

/// A timely double-tap: without the tick ratchet, both turns are stamped on
/// the SAME client tick, distinguished only by their client sequence
/// numbers. Arriving within the committed-lag window, the server schedules
/// both at that client tick and the shared deferral rule spreads them —
/// prediction and the authoritative schedule agree, so no divergence at all.
#[tokio::test]
async fn same_tick_stamped_double_turn_stays_in_sync() -> Result<()> {
    fn clockwise(direction: Direction) -> Direction {
        match direction {
            Direction::Up => Direction::Right,
            Direction::Right => Direction::Down,
            Direction::Down => Direction::Left,
            Direction::Left => Direction::Up,
        }
    }

    with_timeout(async {
        let cfg = TransportConfig {
            base_latency_ms: 100,
            jitter_ms: 0,
            ..TransportConfig::lossless()
        };
        let mut world = SimWorld::new(0xD0B1E, cfg);
        world.auto_resync = true;

        world.run_for(2_000);

        let travel = world.server.committed_state().arena.snakes[0].direction;
        let first_turn = clockwise(travel);
        let second_turn = clockwise(first_turn); // opposite of `travel`
        let cmd1 = world.client_send_turn(first_turn);
        let cmd2 = world.client_send_turn(second_turn);

        // Ratchet-free stamping: same tick, ordered by client sequence.
        assert_eq!(
            cmd1.command_id_client.tick, cmd2.command_id_client.tick,
            "a double-tap within one predicted tick shares the tick stamp"
        );
        assert!(
            cmd2.command_id_client.sequence_number > cmd1.command_id_client.sequence_number,
            "client sequence numbers must order the pair"
        );

        world.run_for(1_500);

        // Timely arrival: the server honors the client tick for both — no
        // rebase — and the deferral rule spreads them at execution time.
        for cmd in [&cmd1, &cmd2] {
            let confirmed = world
                .confirmations
                .iter()
                .find(|c| c.command_id_client == cmd.command_id_client)
                .expect("server confirmed the command");
            assert_eq!(
                confirmed
                    .command_id_server
                    .as_ref()
                    .expect("server id")
                    .tick,
                cmd.command_id_client.tick,
                "timely commands must keep their client tick"
            );
        }

        let snake = &world.server.committed_state().arena.snakes[0];
        assert!(snake.is_alive, "the snake must not reverse into itself");
        assert_eq!(
            snake.direction, second_turn,
            "the deferred second turn must have applied"
        );

        world.run_for(10_000);
        world.drain_and_probe();

        let status = world.client().sync_status();
        assert_eq!(
            status.total_mismatches, 0,
            "timely double-tap must produce no divergence at all: {status:?}"
        );
        assert_eq!(
            world.client().committed_sync_hash(),
            world.server.committed_sync_hash(),
            "client and server must agree"
        );
        Ok(())
    })
    .await
}

/// The 180-degree-reversal shape, end to end: a player double-taps a turn
/// (two 90-degree turns in the same instant) and both commands arrive past
/// the 500ms committed-lag window, so the server rebases them onto the SAME
/// committed tick. The engine must apply the first turn on that tick and
/// defer the second to the next — never summing them into a reversal — and
/// the client's committed replica must derive the identical deferral.
#[tokio::test]
async fn rebased_double_turn_defers_and_stays_in_sync() -> Result<()> {
    fn clockwise(direction: Direction) -> Direction {
        match direction {
            Direction::Up => Direction::Right,
            Direction::Right => Direction::Down,
            Direction::Down => Direction::Left,
            Direction::Left => Direction::Up,
        }
    }

    with_timeout(async {
        let cfg = TransportConfig {
            base_latency_ms: 100,
            jitter_ms: 0,
            ..TransportConfig::lossless()
        };
        let mut world = SimWorld::new(0x5EED5, cfg);
        world.auto_resync = true;

        world.run_for(2_000);

        // Double-tap: both turns leave in the same instant, both stuck in
        // the network for 800ms.
        let travel = world.server.committed_state().arena.snakes[0].direction;
        let first_turn = clockwise(travel);
        let second_turn = clockwise(first_turn); // opposite of `travel`
        let cmd1 = world.client_send_turn_with_latency(first_turn, 800);
        let cmd2 = world.client_send_turn_with_latency(second_turn, 800);
        world.run_for(1_500);

        let server_tick_of = |world: &SimWorld, cmd: &GameCommandMessage| {
            world
                .confirmations
                .iter()
                .find(|c| c.command_id_client == cmd.command_id_client)
                .expect("server confirmed the command")
                .command_id_server
                .as_ref()
                .expect("server assigned an id")
                .tick
        };
        let tick1 = server_tick_of(&world, &cmd1);
        let tick2 = server_tick_of(&world, &cmd2);
        assert!(
            tick1 > cmd1.command_id_client.tick,
            "premise: the late pair must be rebased (client tick {}, server tick {tick1})",
            cmd1.command_id_client.tick
        );
        assert_eq!(
            tick1, tick2,
            "premise: both turns must collapse onto one committed tick"
        );

        // Intent preserved on the authoritative state: the first turn applied
        // on the collapsed tick, the deferred second one tick later.
        let snake = &world.server.committed_state().arena.snakes[0];
        assert!(snake.is_alive, "the snake must not reverse into itself");
        assert_eq!(
            snake.direction, second_turn,
            "the deferred second turn must have applied"
        );

        // The client's committed replica derives the same deferral from the
        // same CommandScheduled events: both sides stay in sync.
        world.run_for(10_000);
        world.drain_and_probe();

        let status = world.client().sync_status();
        assert_eq!(
            status.last_probe_matched,
            Some(true),
            "final probe must match: {status:?}"
        );
        assert!(!status.needs_resync, "no pending resync at the end");
        assert_eq!(
            world.client().committed_sync_hash(),
            world.server.committed_sync_hash(),
            "client must converge to the server state"
        );
        Ok(())
    })
    .await
}
