use crate::game_bus::{GameBus, PartitionEventSubscription};
use crate::game_executor::PARTITION_COUNT;
use anyhow::{Context, Result};
use common::{GameEvent, GameEventMessage, GameState, GameStatus};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::sync::{RwLock, broadcast};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// A replicated state and the transport sequence already reflected in it.
/// Keeping both values in one lock-protected entry makes it impossible for a
/// subscriber to receive state S+1 stamped with watermark S.
#[derive(Clone)]
struct ReplicatedGame {
    game_state: GameState,
    stream_seq: u64,
}

type ReplicaStore = Arc<RwLock<HashMap<u32, ReplicatedGame>>>;

fn is_stateful_unknown_event(event: &GameEventMessage) -> bool {
    !(event.stream_seq == 0 && matches!(&event.event, GameEvent::CommandRejected { .. }))
}

async fn replica_snapshot(store: &ReplicaStore, game_id: u32) -> Option<(GameState, u64)> {
    let replicas = store.read().await;
    replicas
        .get(&game_id)
        .map(|replica| (replica.game_state.clone(), replica.stream_seq))
}

/// Game event broadcast channels
pub type GameEventBroadcasters = Arc<RwLock<HashMap<u32, broadcast::Sender<GameEventMessage>>>>;

/// Tracks the replication status
#[derive(Debug, Clone)]
pub struct ReplicationStatus {
    pub partition_id: u32,
    pub is_ready: bool,
}

/// A wrapper around broadcast::Receiver that filters out events already
/// contained in the snapshot handed out alongside it.
///
/// State-bearing events use the transport-level `stream_seq`. A zero sequence
/// is reserved for out-of-band terminal command rejections, which do not mutate
/// replica state and are safe to forward by stable command identity.
pub struct FilteredEventReceiver {
    inner: broadcast::Receiver<GameEventMessage>,
    min_stream_seq: u64,
    game_id: u32,
}

impl FilteredEventReceiver {
    /// Create a new FilteredEventReceiver
    pub fn new(
        inner: broadcast::Receiver<GameEventMessage>,
        min_stream_seq: u64,
        game_id: u32,
    ) -> Self {
        Self {
            inner,
            min_stream_seq,
            game_id,
        }
    }

    /// Receive the next event that passes the filter.
    ///
    /// `Err(Lagged(n))` is surfaced to the caller instead of being silently
    /// swallowed: a lagged receiver has lost events, and the caller must
    /// resync its downstream consumer (e.g. send a fresh snapshot). The
    /// receiver stays usable afterwards and continues from the oldest
    /// retained message.
    pub async fn recv(&mut self) -> Result<GameEventMessage, broadcast::error::RecvError> {
        loop {
            let event = self.inner.recv().await?;

            // Snapshots re-anchor the stream unconditionally. A restarted
            // executor (failover/resume) begins a NEW stream_seq sequence
            // starting near 1; filtering its snapshot as "stale" against the
            // old stream's high watermark would wedge this subscriber — and
            // its client — forever. Forward the snapshot and adopt its
            // stream as the new baseline (mirrors the client engine, which
            // also resets its watermark on every snapshot).
            if matches!(&event.event, GameEvent::Snapshot { .. }) {
                self.min_stream_seq = event.stream_seq;
                debug!(
                    "Forwarding snapshot for game {} and re-anchoring stream (stream_seq {})",
                    self.game_id, event.stream_seq
                );
                return Ok(event);
            }

            let is_fresh = if event.stream_seq > 0 {
                event.stream_seq > self.min_stream_seq
            } else {
                matches!(&event.event, GameEvent::CommandRejected { .. })
            };

            if is_fresh {
                debug!(
                    "Forwarding event for game {} (sequence {}, stream_seq {})",
                    self.game_id, event.sequence, event.stream_seq
                );
                return Ok(event);
            }
            debug!(
                "Filtering out stale or unsequenced state event for game {} (sequence {}, stream_seq {} <= {})",
                self.game_id, event.sequence, event.stream_seq, self.min_stream_seq
            );
        }
    }
}

/// PartitionReplica subscribes to partition events on the game bus and maintains game states
pub struct PartitionReplica {
    partition_id: u32,
    bus: Arc<GameBus>,
    replica_store: ReplicaStore,
    game_event_broadcasters: GameEventBroadcasters,
    status: Arc<RwLock<ReplicationStatus>>,
    cancellation_token: CancellationToken,
    /// Wall-clock ms of the last gap-triggered snapshot request, for
    /// rate-limiting self-heal requests under sustained loss.
    last_gap_request_ms: std::sync::atomic::AtomicI64,
    /// A delta after a stream gap cannot be applied to or broadcast from stale
    /// state. Keep the game cold until an authoritative snapshot reanchors it.
    cold_games: RwLock<HashSet<u32>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContinuityAction {
    Apply,
    SuppressGap { expected: u64 },
    SuppressWhileCold,
}

fn continuity_action(
    cold_games: &mut HashSet<u32>,
    game_id: u32,
    last_stream_seq: u64,
    incoming_stream_seq: u64,
    is_snapshot: bool,
) -> ContinuityAction {
    if is_snapshot {
        cold_games.remove(&game_id);
        return ContinuityAction::Apply;
    }
    if cold_games.contains(&game_id) {
        return ContinuityAction::SuppressWhileCold;
    }
    if last_stream_seq > 0 && incoming_stream_seq > last_stream_seq.saturating_add(1) {
        cold_games.insert(game_id);
        return ContinuityAction::SuppressGap {
            expected: last_stream_seq.saturating_add(1),
        };
    }
    ContinuityAction::Apply
}

impl PartitionReplica {
    fn new(
        partition_id: u32,
        bus: Arc<GameBus>,
        replica_store: ReplicaStore,
        game_event_broadcasters: GameEventBroadcasters,
        cancellation_token: CancellationToken,
    ) -> Self {
        let status = Arc::new(RwLock::new(ReplicationStatus {
            partition_id,
            // Readiness becomes true only after the durable stream readers are
            // subscribed and the initial snapshot request was accepted.
            is_ready: false,
        }));

        Self {
            partition_id,
            bus,
            replica_store,
            game_event_broadcasters,
            status,
            cancellation_token,
            last_gap_request_ms: std::sync::atomic::AtomicI64::new(0),
            cold_games: RwLock::new(HashSet::new()),
        }
    }

    /// Get the current replication status
    pub fn status(&self) -> Arc<RwLock<ReplicationStatus>> {
        self.status.clone()
    }

    async fn evict_game(&self, game_id: u32) {
        {
            let mut replicas = self.replica_store.write().await;
            replicas.remove(&game_id);
        }
        {
            let mut broadcasters = self.game_event_broadcasters.write().await;
            broadcasters.remove(&game_id);
        }
        self.cold_games.write().await.remove(&game_id);
        info!(
            "Game {} durably completed; evicted from replication cache and dropped broadcasters",
            game_id
        );
    }

    /// Process a game event and update the game state
    async fn process_event(&self, event_msg: GameEventMessage) -> Result<()> {
        let game_id = event_msg.game_id;
        debug!(
            "Processing game event for game {} in partition {}",
            game_id, self.partition_id
        );

        // Transport-integrity check, kept as defense-in-depth. The Streams
        // bus itself doesn't drop, but a gap can still appear past it (trim
        // horizon after a long outage, broadcast-lag downstream): a gap in
        // stream_seq means this replica lost messages and its state can no
        // longer be trusted, so ask the executor for fresh snapshots. Stale
        // or duplicate messages are dropped instead of double-applied. On a
        // healthy system this path sits idle (see DEBUGGING.md).
        let is_snapshot = matches!(&event_msg.event, GameEvent::Snapshot { .. });
        let last_stream_seq = if event_msg.stream_seq > 0 {
            let replicas = self.replica_store.read().await;
            replicas
                .get(&game_id)
                .map(|replica| replica.stream_seq)
                .unwrap_or(0)
        } else {
            0
        };
        let continuity = {
            let mut cold_games = self.cold_games.write().await;
            continuity_action(
                &mut cold_games,
                game_id,
                last_stream_seq,
                event_msg.stream_seq,
                is_snapshot,
            )
        };
        match continuity {
            ContinuityAction::Apply => {}
            ContinuityAction::SuppressGap { expected } => {
                warn!(
                    "Replica for partition {} detected stream gap for game {}: expected {}, got {} ({} messages lost); suppressing deltas until a snapshot",
                    self.partition_id,
                    game_id,
                    expected,
                    event_msg.stream_seq,
                    event_msg.stream_seq.saturating_sub(expected)
                );
                self.request_snapshots_rate_limited().await;
                return Ok(());
            }
            ContinuityAction::SuppressWhileCold => {
                debug!(
                    "Replica for partition {} suppressing game {} delta at stream_seq {} while waiting for a recovery snapshot",
                    self.partition_id, game_id, event_msg.stream_seq
                );
                self.request_snapshots_rate_limited().await;
                return Ok(());
            }
        }

        if event_msg.stream_seq > 0 {
            let last = last_stream_seq;
            if !is_snapshot && last > 0 && event_msg.stream_seq <= last {
                debug!(
                    "Replica for partition {} dropping stale message for game {} (stream_seq {} <= {})",
                    self.partition_id, game_id, event_msg.stream_seq, last
                );
                return Ok(());
            }
        }

        let mut fingerprint_divergence = None;
        {
            // State mutation and its watermark update share this one write
            // guard. A subscriber therefore observes either the complete old
            // pair or the complete new pair, never a mixture of the two.
            let mut replicas = self.replica_store.write().await;
            match &event_msg.event {
                GameEvent::Snapshot { game_state } => {
                    debug!(
                        "Received snapshot for game {} at tick {} (stream_seq {})",
                        game_id, event_msg.tick, event_msg.stream_seq
                    );
                    let stream_seq = if event_msg.stream_seq > 0 {
                        event_msg.stream_seq
                    } else {
                        replicas
                            .get(&game_id)
                            .map(|replica| replica.stream_seq)
                            .unwrap_or(0)
                    };
                    replicas.insert(
                        game_id,
                        ReplicatedGame {
                            game_state: game_state.clone(),
                            stream_seq,
                        },
                    );
                }
                _ => {
                    let expected_fingerprint = match &event_msg.event {
                        GameEvent::TickHash { hash, .. } => Some(*hash),
                        _ => None,
                    };
                    if let Some(replica) = replicas.get_mut(&game_id) {
                        {
                            let game_state = &mut replica.game_state;
                            // Tick forward until we reach the event's tick. This must
                            // loop: events can arrive more than one tick apart (quiet
                            // stretches emit nothing), and catching up a single tick
                            // per event leaves the replica permanently behind —
                            // corrupting every join snapshot served from it.
                            while game_state.tick < event_msg.tick {
                                if let Err(e) = game_state.tick_forward(true) {
                                    error!("Error during tick_forward: {:?}", e);
                                    break;
                                }
                            }

                            if let Some(expected) = expected_fingerprint {
                                let actual = game_state.sync_hash();
                                if actual != expected {
                                    fingerprint_divergence = Some((expected, actual));
                                }
                            }

                            if fingerprint_divergence.is_none() {
                                // TickHash is a no-op; every other event mutates the
                                // local replica after it has advanced to the event tick.
                                game_state.apply_event(event_msg.event.clone(), None);
                                debug!(
                                    "Applied event to game {} state: {:?}",
                                    game_id, event_msg.event
                                );
                            }
                        }

                        if fingerprint_divergence.is_none() && event_msg.stream_seq > 0 {
                            replica.stream_seq = event_msg.stream_seq;
                        }
                    } else {
                        if is_stateful_unknown_event(&event_msg) {
                            // A newly started replica subscribes before its
                            // requested snapshots arrive, so state deltas for
                            // not-yet-anchored games are expected during warmup.
                            // The explicit gap path above remains WARN-level.
                            debug!("Received state event for unknown game {}", game_id);
                        } else {
                            // A rejection is an out-of-band command outcome and
                            // does not mutate replica state. Missing stateful
                            // events still warn, so this expected race can stay
                            // at debug without masking a cold replica.
                            debug!(
                                "Received command rejection for unknown local replica {}",
                                game_id
                            );
                        }
                    }
                }
            }
        }

        if let Some((expected, actual)) = fingerprint_divergence {
            crate::resilience_metrics::record_recovery_fingerprint_divergence(1);
            self.cold_games.write().await.insert(game_id);
            warn!(
                game_id,
                partition = self.partition_id,
                tick = event_msg.tick,
                expected,
                actual,
                "Replica fingerprint diverged; suppressing deltas until a fresh snapshot"
            );
            self.request_snapshots_rate_limited().await;
            return Ok(());
        }

        // Broadcast the event to any local subscribers
        {
            let broadcasters = self.game_event_broadcasters.read().await;
            if let Some(sender) = broadcasters.get(&game_id) {
                match sender.send(event_msg.clone()) {
                    Ok(receiver_count) => {
                        if receiver_count == 0 {
                            debug!("No receivers for game {} broadcast", game_id);
                        }
                    }
                    Err(_) => {
                        // Tokio broadcast returns SendError whenever the last
                        // socket receiver has gone away. That is normal as
                        // clients finish or switch generations.
                        debug!("No receivers for game {} broadcast", game_id);
                    }
                }
            }
        }

        Ok(())
    }

    async fn process_received_event(&self, event: GameEventMessage) -> Result<()> {
        let game_id = event.game_id;
        let is_terminal_snapshot = matches!(
            &event.event,
            GameEvent::Snapshot { game_state }
                if matches!(game_state.status, GameStatus::Complete { .. })
        );
        self.process_event(event).await?;
        if is_terminal_snapshot {
            // The fenced completion transaction writes the immutable
            // completion record, recovery envelope, stored snapshot, and
            // pending-effect index before appending this event.
            // `process_event` broadcasts before returning.
            self.evict_game(game_id).await;
        }
        Ok(())
    }

    /// Ask the executor to republish snapshots for this partition, at most
    /// once per second, so sustained loss doesn't turn into a request storm.
    async fn request_snapshots_rate_limited(&self) {
        use std::sync::atomic::Ordering;
        let now_ms = chrono::Utc::now().timestamp_millis();
        let last = self.last_gap_request_ms.load(Ordering::Relaxed);
        if now_ms - last < 1000 {
            return;
        }
        if self
            .last_gap_request_ms
            .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return; // Another task just requested.
        }
        if let Err(e) = self
            .bus
            .request_partition_snapshots(self.partition_id)
            .await
        {
            error!(
                "Failed to request snapshots for partition {} after stream gap: {}",
                self.partition_id, e
            );
        }
    }

    /// Run the replication worker
    pub async fn run(self) -> Result<()> {
        info!(
            "Starting replication worker for partition {}",
            self.partition_id
        );

        // Initial stream anchoring is a readiness dependency, not a process
        // liveness dependency. A replacement task launched while Valkey is
        // unavailable keeps this worker alive and retries from a clean anchor
        // until the dependency returns.
        let subscription = loop {
            let result = tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => return Ok(()),
                result = self.bus.subscribe_to_partition_events(self.partition_id) => result,
            };
            match result {
                Ok(subscription) => break subscription,
                Err(error) => warn!(
                    partition = self.partition_id,
                    %error,
                    "Replication stream anchor unavailable; retrying locally"
                ),
            }
            tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => return Ok(()),
                _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {}
            }
        };

        // Keep the successfully anchored readers while retrying the initial
        // snapshot request; this preserves the subscribe-before-request race
        // guarantee without spawning duplicate reader tasks.
        loop {
            let result = tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => return Ok(()),
                result = self.bus.request_partition_snapshots(self.partition_id) => result,
            };
            match result {
                Ok(()) => break,
                Err(error) => warn!(
                    partition = self.partition_id,
                    %error,
                    "Initial replication snapshot request unavailable; retrying locally"
                ),
            }
            tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => return Ok(()),
                _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {}
            }
        }

        let PartitionEventSubscription {
            partition_id: _,
            mut event_receiver,
        } = subscription;

        // Mark as ready immediately (initial state arrives via the snapshot
        // request above; there is no historical catch-up phase)
        self.status.write().await.is_ready = true;

        // Main event processing loop
        loop {
            tokio::select! {
                biased;

                _ = self.cancellation_token.cancelled() => {
                    info!("Replication worker for partition {} shutting down", self.partition_id);
                    break;
                }

                // Process events from partition subscription
                event = event_receiver.recv() => {
                    match event {
                        Some(event) => {
                            if let Err(e) = self.process_received_event(event).await {
                                error!("Failed to process event in partition {}: {}", self.partition_id, e);
                            }
                        }
                        None => {
                            error!("Partition {} subscription closed unexpectedly, replication worker exiting",
                                self.partition_id);
                            break;
                        }
                    }
                }

            }
        }

        self.status.write().await.is_ready = false;

        Ok(())
    }
}

/// Manager for running multiple replicas
pub struct ReplicationManager {
    workers: Vec<tokio::task::JoinHandle<Result<()>>>,
    replica_store: ReplicaStore,
    game_event_broadcasters: GameEventBroadcasters,
    statuses: Arc<RwLock<HashMap<u32, Arc<RwLock<ReplicationStatus>>>>>,
    bus: Arc<GameBus>,
    /// Coalesce partition-wide cold-join snapshot requests across all local
    /// WebSocket connections. A request republishes every active game in the
    /// partition, so issuing one per reconnecting player would amplify a
    /// scale event unnecessarily.
    last_on_demand_request_ms: Vec<AtomicI64>,
}

/// API for querying replicated game states
// Internal trait: callers never need extra auto trait bounds on the futures.
#[allow(async_fn_in_trait)]
pub trait GameStateReader: Send + Sync {
    /// Get a game state by ID
    async fn get_game_state(&self, game_id: u32) -> Option<GameState>;

    /// Get all game states for a partition
    async fn get_partition_games(&self, partition_id: u32) -> Vec<(u32, GameState)>;

    /// Check if replication is ready
    async fn is_ready(&self) -> bool;
}

impl GameStateReader for ReplicationManager {
    async fn get_game_state(&self, game_id: u32) -> Option<GameState> {
        replica_snapshot(&self.replica_store, game_id)
            .await
            .map(|(game_state, _)| game_state)
    }

    async fn get_partition_games(&self, partition_id: u32) -> Vec<(u32, GameState)> {
        let replicas = self.replica_store.read().await;
        replicas
            .iter()
            .filter(|(game_id, _)| *game_id % PARTITION_COUNT == partition_id)
            .map(|(id, replica)| (*id, replica.game_state.clone()))
            .collect()
    }

    async fn is_ready(&self) -> bool {
        ReplicationManager::is_ready(self).await
    }
}

impl ReplicationManager {
    /// Request fresh snapshots for a partition, coalesced to at most one
    /// request every 500ms on this gateway. Returns `true` when this call
    /// published a request and `false` when a concurrent/recent caller already
    /// did so.
    pub async fn request_partition_snapshots(&self, partition_id: u32) -> Result<bool> {
        const REQUEST_INTERVAL_MS: i64 = 500;

        let Some(last_request) = self.last_on_demand_request_ms.get(partition_id as usize) else {
            anyhow::bail!("invalid partition {partition_id} for snapshot request");
        };
        let now_ms = chrono::Utc::now().timestamp_millis();
        let previous_ms = last_request.load(Ordering::Relaxed);
        if now_ms >= previous_ms && now_ms - previous_ms < REQUEST_INTERVAL_MS {
            return Ok(false);
        }
        if last_request
            .compare_exchange(previous_ms, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return Ok(false);
        }

        self.bus.request_partition_snapshots(partition_id).await?;
        Ok(true)
    }

    /// Load the most recently published game snapshot from Redis.
    ///
    /// Durably completed games are deliberately evicted from the in-memory replication cache,
    /// while their final snapshot remains in Redis for a short grace period. Callers can use
    /// this method before falling back to durable storage.
    pub async fn get_stored_snapshot(&self, game_id: u32) -> Result<Option<GameState>> {
        self.bus.get_stored_snapshot(game_id).await
    }

    /// Subscribe to game events for a specific game.
    /// Returns the current game state, its transport watermark (the
    /// stream_seq already reflected in that state — stamp it onto snapshots
    /// derived from the state), and a receiver for subsequent events.
    ///
    /// Ordering matters: the broadcast subscription is created BEFORE the
    /// state is read. A broadcast receiver only sees messages sent after
    /// `subscribe()`, so subscribing first guarantees no event between
    /// snapshot and subscription can be missed; events already contained in
    /// the snapshot are dropped by the stream_seq filter instead.
    pub async fn subscribe_to_game(
        &self,
        game_id: u32,
    ) -> Result<(GameState, u64, FilteredEventReceiver)> {
        let receiver = {
            let mut broadcasters = self.game_event_broadcasters.write().await;
            broadcasters
                .entry(game_id)
                .or_insert_with(|| {
                    let (tx, _) = broadcast::channel(1028);
                    tx
                })
                .subscribe()
        };

        let (game_state, watermark) = replica_snapshot(&self.replica_store, game_id)
            .await
            .context("Game not available in replication manager")?;

        let filtered_receiver = FilteredEventReceiver {
            inner: receiver,
            min_stream_seq: watermark,
            game_id,
        };

        Ok((game_state, watermark, filtered_receiver))
    }

    /// Last transport stream_seq applied to the replica state of a game.
    pub async fn get_stream_seq(&self, game_id: u32) -> u64 {
        let replicas = self.replica_store.read().await;
        replicas
            .get(&game_id)
            .map(|replica| replica.stream_seq)
            .unwrap_or(0)
    }

    /// Get a game state (replicas are always ready; no readiness gate)
    pub async fn get_game_state_when_ready(&self, game_id: u32) -> Option<GameState> {
        self.get_game_state(game_id).await
    }

    /// Wait for a game to become available in the replication manager
    /// Returns the game state once available, or an error if timeout is reached
    pub async fn wait_for_game(&self, game_id: u32, timeout_secs: u64) -> Result<GameState> {
        use tokio::time::{Duration, timeout};

        let deadline = timeout(Duration::from_secs(timeout_secs), async {
            let mut backoff_ms = 10;
            const MAX_BACKOFF_MS: u64 = 500;

            loop {
                // Check if game is available
                if let Some(game_state) = self.get_game_state(game_id).await {
                    debug!("Game {} found in replication manager", game_id);
                    return Ok(game_state);
                }

                // Wait with exponential backoff
                debug!(
                    "Game {} not yet available, waiting {}ms",
                    game_id, backoff_ms
                );
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;

                // Increase backoff for next iteration
                backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
            }
        });

        match deadline.await {
            Ok(result) => result,
            Err(_) => {
                error!(
                    "Timeout waiting for game {} to become available after {} seconds",
                    game_id, timeout_secs
                );
                Err(anyhow::anyhow!(
                    "Game {} did not become available within {} seconds",
                    game_id,
                    timeout_secs
                ))
            }
        }
    }

    /// Create and start replication workers for specified partitions
    pub async fn new(
        partitions: Vec<u32>,
        cancellation_token: CancellationToken,
        bus: Arc<GameBus>,
    ) -> Result<Self> {
        let replica_store = Arc::new(RwLock::new(HashMap::new()));
        let game_event_broadcasters = Arc::new(RwLock::new(HashMap::new()));
        let statuses = Arc::new(RwLock::new(HashMap::new()));
        let mut workers = Vec::new();

        for partition_id in partitions {
            // Create worker
            let worker = PartitionReplica::new(
                partition_id,
                bus.clone(),
                replica_store.clone(),
                game_event_broadcasters.clone(),
                cancellation_token.clone(),
            );

            // Store status reference
            {
                let mut status_map = statuses.write().await;
                status_map.insert(partition_id, worker.status());
            }

            // Spawn worker task
            let handle = tokio::spawn(worker.run());
            workers.push(handle);
        }

        Ok(Self {
            workers,
            replica_store,
            game_event_broadcasters,
            statuses,
            bus,
            last_on_demand_request_ms: (0..PARTITION_COUNT).map(|_| AtomicI64::new(0)).collect(),
        })
    }

    /// Check that every configured stream reader reached its subscription
    /// anchor and that none of the worker tasks has exited unexpectedly.
    pub async fn is_ready(&self) -> bool {
        if self.workers.is_empty() || self.workers.iter().any(|worker| worker.is_finished()) {
            return false;
        }

        let statuses = self.statuses.read().await;
        if statuses.len() != self.workers.len() {
            return false;
        }
        for status in statuses.values() {
            if !status.read().await.is_ready {
                return false;
            }
        }
        true
    }

    /// A worker that has terminated cannot recover locally and is therefore a
    /// critical failure. Workers that are merely still anchoring remain live
    /// and keep readiness false.
    pub fn has_failed_worker(&self) -> bool {
        self.workers.iter().any(|worker| worker.is_finished())
    }

    /// Get status of all workers
    pub async fn get_status(&self) -> HashMap<u32, ReplicationStatus> {
        let mut result = HashMap::new();
        let statuses = self.statuses.read().await;
        for (partition_id, status) in statuses.iter() {
            let s = status.read().await;
            result.insert(*partition_id, s.clone());
        }
        result
    }

    /// Wait for all workers to complete
    pub async fn wait(self) -> Result<()> {
        for worker in self.workers {
            worker.await??;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ContinuityAction, FilteredEventReceiver, GameEventBroadcasters, PartitionReplica,
        ReplicaStore, ReplicatedGame, continuity_action, is_stateful_unknown_event,
        replica_snapshot,
    };
    use crate::game_bus::GameBus;
    use crate::game_executor::PARTITION_COUNT;
    use common::{GameEvent, GameEventMessage, GameState, GameStatus, GameType, QueueMode};
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use tokio::sync::{Barrier, RwLock, broadcast};
    use tokio_util::sync::CancellationToken;

    fn event(sequence: u64, stream_seq: u64) -> GameEventMessage {
        GameEventMessage {
            game_id: 1,
            tick: 1,
            sequence,
            stream_seq,
            user_id: None,
            event: GameEvent::TickHash {
                hash: 0,
                server_ts_ms: 0,
            },
        }
    }

    fn snapshot(sequence: u64, stream_seq: u64) -> GameEventMessage {
        let state = GameState::new(
            10,
            10,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        GameEventMessage {
            game_id: 1,
            tick: 1,
            sequence,
            stream_seq,
            user_id: None,
            event: GameEvent::Snapshot { game_state: state },
        }
    }

    #[tokio::test]
    async fn filter_drops_stale_and_passes_fresh_by_stream_seq() {
        let (tx, rx) = broadcast::channel(16);
        let mut filtered = FilteredEventReceiver::new(rx, 500, 1);

        tx.send(event(0, 400)).unwrap(); // stale: absorbed by the snapshot
        tx.send(event(999, 0)).unwrap(); // invalid: state events must be sequenced
        tx.send(event(0, 501)).unwrap(); // fresh
        let got = filtered.recv().await.unwrap();
        assert_eq!(got.stream_seq, 501);
    }

    #[tokio::test]
    async fn snapshot_re_anchors_stream_after_executor_restart() {
        // Subscriber anchored to the OLD executor's stream at watermark 500.
        let (tx, rx) = broadcast::channel(16);
        let mut filtered = FilteredEventReceiver::new(rx, 500, 1);

        // A restarted executor begins a new stream: snapshot at stream_seq 2,
        // then ordinary events 3, 4. Without re-anchoring, ALL of these would
        // be filtered as stale (< 500) and the client would be wedged.
        tx.send(snapshot(7, 2)).unwrap();
        tx.send(event(8, 3)).unwrap();
        tx.send(event(8, 2)).unwrap(); // duplicate/stale vs the new anchor
        tx.send(event(9, 4)).unwrap();

        let got = filtered.recv().await.unwrap();
        assert!(matches!(got.event, GameEvent::Snapshot { .. }));
        assert_eq!(got.stream_seq, 2);

        let got = filtered.recv().await.unwrap();
        assert_eq!(got.stream_seq, 3);

        // The stale seq-2 event is skipped; next delivered is 4.
        let got = filtered.recv().await.unwrap();
        assert_eq!(got.stream_seq, 4);
    }

    #[tokio::test]
    async fn unsequenced_terminal_rejections_are_forwarded_out_of_band() {
        use common::ClientCommandIdentityV2;

        let (tx, rx) = broadcast::channel(16);
        let mut filtered = FilteredEventReceiver::new(rx, 10, 1);

        let mut rejection = event(0, 0);
        rejection.user_id = Some(7);
        rejection.event = GameEvent::CommandRejected {
            command_id: ClientCommandIdentityV2 {
                game_id: 1,
                user_id: 7,
                client_game_session_id: "session".to_owned(),
                sequence: 1,
            },
            reason: "invalid command".to_owned(),
        };
        tx.send(rejection).unwrap();
        let got = filtered.recv().await.unwrap();
        assert!(matches!(got.event, GameEvent::CommandRejected { .. }));
        assert!(!is_stateful_unknown_event(&got));
    }

    #[test]
    fn unknown_state_events_are_distinguished_from_out_of_band_rejections() {
        assert!(is_stateful_unknown_event(&event(1, 1)));
        assert!(is_stateful_unknown_event(&snapshot(1, 1)));
    }

    #[tokio::test]
    async fn replica_snapshot_never_observes_state_without_its_watermark() {
        let old_state = GameState::new(
            10,
            10,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            10,
        );
        let mut new_state = old_state.clone();
        new_state.start_ms = 11;

        let store: ReplicaStore = Arc::new(RwLock::new(HashMap::from([(
            1,
            ReplicatedGame {
                game_state: old_state,
                stream_seq: 10,
            },
        )])));
        let state_is_updated = Arc::new(Barrier::new(2));
        let finish_update = Arc::new(Barrier::new(2));

        // Stop a writer at the exact old-race point: state S+1 has been
        // written, but its watermark has not. The reader must remain blocked
        // on the shared entry lock until the pair is complete.
        let writer = {
            let store = store.clone();
            let state_is_updated = state_is_updated.clone();
            let finish_update = finish_update.clone();
            tokio::spawn(async move {
                let mut replicas = store.write().await;
                let replica = replicas.get_mut(&1).expect("replica");
                replica.game_state = new_state;
                state_is_updated.wait().await;
                finish_update.wait().await;
                replica.stream_seq = 11;
            })
        };

        state_is_updated.wait().await;
        let reader = {
            let store = store.clone();
            tokio::spawn(async move { replica_snapshot(&store, 1).await.expect("snapshot") })
        };
        tokio::task::yield_now().await;
        assert!(
            !reader.is_finished(),
            "snapshot reader must not observe a half-updated replica entry"
        );

        finish_update.wait().await;
        writer.await.expect("writer task");
        let (observed_state, observed_stream_seq) = reader.await.expect("reader task");
        assert_eq!(observed_state.start_ms, 11);
        assert_eq!(observed_stream_seq, 11);
    }

    #[tokio::test]
    async fn terminal_snapshot_is_broadcast_before_replica_eviction() {
        let client = redis::Client::open("redis://127.0.0.1:6379/1?protocol=resp3")
            .expect("valid local Redis URL");
        let (push_tx, _push_rx) = broadcast::channel(8);
        let manager = crate::redis_utils::create_connection_manager(client.clone(), push_tx)
            .await
            .expect("local Redis is required for replication tests");
        let bus = Arc::new(
            GameBus::new(
                manager.clone(),
                (0..PARTITION_COUNT)
                    .map(|_| manager.clone().into())
                    .collect(),
                (0..PARTITION_COUNT)
                    .map(|_| manager.clone().into())
                    .collect(),
                manager,
                client,
                CancellationToken::new(),
            )
            .expect("test GameBus"),
        );

        let replica_store: ReplicaStore = Arc::new(RwLock::new(HashMap::new()));
        let (event_tx, mut event_rx) = broadcast::channel(8);
        let broadcasters: GameEventBroadcasters =
            Arc::new(RwLock::new(HashMap::from([(1, event_tx)])));
        let replica = PartitionReplica::new(
            1,
            bus,
            replica_store.clone(),
            broadcasters.clone(),
            CancellationToken::new(),
        );
        let mut state = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, None, 0);
        state.status = GameStatus::Complete {
            winning_snake_id: None,
        };
        let terminal = GameEventMessage {
            game_id: 1,
            tick: state.tick,
            sequence: state.event_sequence,
            stream_seq: 1,
            user_id: None,
            event: GameEvent::Snapshot { game_state: state },
        };

        replica
            .process_received_event(terminal.clone())
            .await
            .expect("terminal event processes");

        let received = event_rx
            .recv()
            .await
            .expect("existing receiver gets terminal snapshot");
        assert_eq!(received.game_id, terminal.game_id);
        assert_eq!(received.stream_seq, terminal.stream_seq);
        assert!(matches!(
            received.event,
            GameEvent::Snapshot { game_state }
                if matches!(game_state.status, GameStatus::Complete { .. })
        ));
        assert!(matches!(
            event_rx.recv().await,
            Err(broadcast::error::RecvError::Closed)
        ));
        assert!(replica_store.read().await.is_empty());
        assert!(broadcasters.read().await.is_empty());
    }

    #[test]
    fn stream_gap_suppresses_deltas_until_snapshot_reanchors() {
        let mut cold_games = HashSet::new();

        assert_eq!(
            continuity_action(&mut cold_games, 1, 100, 105, false),
            ContinuityAction::SuppressGap { expected: 101 }
        );
        assert!(cold_games.contains(&1));
        assert_eq!(
            continuity_action(&mut cold_games, 1, 100, 106, false),
            ContinuityAction::SuppressWhileCold
        );

        assert_eq!(
            continuity_action(&mut cold_games, 1, 100, 107, true),
            ContinuityAction::Apply
        );
        assert!(!cold_games.contains(&1));
        assert_eq!(
            continuity_action(&mut cold_games, 1, 107, 108, false),
            ContinuityAction::Apply
        );
    }
}
