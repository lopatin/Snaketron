use crate::game_bus::{GameBus, PartitionSubscription};
use crate::game_executor::{PARTITION_COUNT, StreamEvent};
use anyhow::{Context, Result};
use common::{GameEvent, GameEventMessage, GameState, GameStatus};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// In-memory game state storage
pub type GameStateStore = Arc<RwLock<HashMap<u32, GameState>>>;

/// Per-game last-seen transport stream_seq (the replica watermark).
/// Updated together with `GameStateStore` while holding the state write lock,
/// so a state clone and its watermark are always mutually consistent.
pub type GameStreamSeqs = Arc<RwLock<HashMap<u32, u64>>>;

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
/// Filtering uses the transport-level stream_seq when both the message and the
/// snapshot watermark carry one (> 0); otherwise it falls back to the legacy
/// engine event sequence, which is not reliably monotonic across snapshots.
pub struct FilteredEventReceiver {
    inner: broadcast::Receiver<GameEventMessage>,
    min_sequence: u64,
    min_stream_seq: u64,
    game_id: u32,
}

impl FilteredEventReceiver {
    /// Create a new FilteredEventReceiver
    pub fn new(
        inner: broadcast::Receiver<GameEventMessage>,
        min_sequence: u64,
        min_stream_seq: u64,
        game_id: u32,
    ) -> Self {
        Self {
            inner,
            min_sequence,
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
            if matches!(event.event, GameEvent::Snapshot { .. }) {
                self.min_stream_seq = event.stream_seq;
                self.min_sequence = event.sequence;
                debug!(
                    "Forwarding snapshot for game {} and re-anchoring stream (stream_seq {})",
                    self.game_id, event.stream_seq
                );
                return Ok(event);
            }

            let is_fresh = if event.stream_seq > 0 && self.min_stream_seq > 0 {
                event.stream_seq > self.min_stream_seq
            } else {
                event.sequence > self.min_sequence
            };

            if is_fresh {
                debug!(
                    "Forwarding event for game {} (sequence {}, stream_seq {})",
                    self.game_id, event.sequence, event.stream_seq
                );
                return Ok(event);
            }
            debug!(
                "Filtering out stale event for game {} (sequence {} <= {}, stream_seq {} <= {})",
                self.game_id,
                event.sequence,
                self.min_sequence,
                event.stream_seq,
                self.min_stream_seq
            );
        }
    }
}

/// PartitionReplica subscribes to partition events on the game bus and maintains game states
pub struct PartitionReplica {
    partition_id: u32,
    bus: Arc<GameBus>,
    game_states: GameStateStore,
    game_event_broadcasters: GameEventBroadcasters,
    game_stream_seqs: GameStreamSeqs,
    status: Arc<RwLock<ReplicationStatus>>,
    cancellation_token: CancellationToken,
    /// Wall-clock ms of the last gap-triggered snapshot request, for
    /// rate-limiting self-heal requests under sustained loss.
    last_gap_request_ms: std::sync::atomic::AtomicI64,
}

impl PartitionReplica {
    pub fn new(
        partition_id: u32,
        bus: Arc<GameBus>,
        game_states: GameStateStore,
        game_event_broadcasters: GameEventBroadcasters,
        game_stream_seqs: GameStreamSeqs,
        cancellation_token: CancellationToken,
    ) -> Self {
        let status = Arc::new(RwLock::new(ReplicationStatus {
            partition_id,
            // Immediately ready: the subscription anchors at the stream tail
            // and initial state arrives via the snapshot request below.
            is_ready: true,
        }));

        Self {
            partition_id,
            bus,
            game_states,
            game_event_broadcasters,
            game_stream_seqs,
            status,
            cancellation_token,
            last_gap_request_ms: std::sync::atomic::AtomicI64::new(0),
        }
    }

    /// Get the current replication status
    pub fn status(&self) -> Arc<RwLock<ReplicationStatus>> {
        self.status.clone()
    }

    async fn evict_game(&self, game_id: u32) {
        {
            let mut states = self.game_states.write().await;
            states.remove(&game_id);
        }
        {
            let mut broadcasters = self.game_event_broadcasters.write().await;
            broadcasters.remove(&game_id);
        }
        {
            let mut seqs = self.game_stream_seqs.write().await;
            seqs.remove(&game_id);
        }
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
        if event_msg.stream_seq > 0 {
            let last = {
                let seqs = self.game_stream_seqs.read().await;
                seqs.get(&game_id).copied().unwrap_or(0)
            };
            if !is_snapshot && last > 0 {
                if event_msg.stream_seq <= last {
                    debug!(
                        "Replica for partition {} dropping stale message for game {} (stream_seq {} <= {})",
                        self.partition_id, game_id, event_msg.stream_seq, last
                    );
                    return Ok(());
                }
                if event_msg.stream_seq > last + 1 {
                    warn!(
                        "Replica for partition {} detected stream gap for game {}: expected {}, got {} ({} messages lost); requesting snapshots",
                        self.partition_id,
                        game_id,
                        last + 1,
                        event_msg.stream_seq,
                        event_msg.stream_seq - last - 1
                    );
                    self.request_snapshots_rate_limited().await;
                }
            }
        }

        match &event_msg.event {
            GameEvent::Snapshot { game_state } => {
                info!(
                    "Received snapshot for game {} at tick {} (stream_seq {})",
                    game_id, event_msg.tick, event_msg.stream_seq
                );
                // Always update with the latest snapshot
                let mut states = self.game_states.write().await;
                states.insert(game_id, game_state.clone());
            }
            _ => {
                let mut states = self.game_states.write().await;
                if let Some(game_state) = states.get_mut(&game_id) {
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

                    // Apply event to game state
                    game_state.apply_event(event_msg.event.clone(), None);
                    debug!(
                        "Applied event to game {} state: {:?}",
                        game_id, event_msg.event
                    );
                } else {
                    warn!("Received event for unknown game {}", game_id);
                }
            }
        }

        // Record the watermark AFTER the state update so a concurrent
        // subscribe_to_game (which reads watermark first, then state) can
        // only over-deliver, never under-deliver.
        if event_msg.stream_seq > 0 {
            let mut seqs = self.game_stream_seqs.write().await;
            seqs.insert(game_id, event_msg.stream_seq);
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
                        // This shouldn't happen with broadcast channels, but log if it does
                        warn!(
                            "Failed to broadcast event for game {} - channel may be closed",
                            game_id
                        );
                    }
                }
            }
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

        // Subscribe to the partition
        let subscription = self.bus.subscribe_to_partition(self.partition_id).await?;

        // Request initial snapshots for this partition
        self.bus
            .request_partition_snapshots(self.partition_id)
            .await?;

        // Destructure subscription so each receiver can be borrowed independently in select!
        let PartitionSubscription {
            partition_id: _,
            mut event_receiver,
            mut command_receiver,
            mut snapshot_request_receiver,
        } = subscription;

        // Mark as ready immediately (initial state arrives via the snapshot
        // request above; there is no historical catch-up phase)
        self.status.write().await.is_ready = true;

        // Events and commands use separate Redis streams, so the durable completion marker can
        // arrive before the final snapshot even though the executor publishes the snapshot first.
        // Track both sides and evict only after this replica has processed (and broadcast) that
        // terminal snapshot.
        let mut terminal_snapshots_seen = HashSet::new();
        let mut durable_completion_markers = HashSet::new();

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
                            let game_id = event.game_id;
                            let is_terminal_snapshot = matches!(
                                &event.event,
                                GameEvent::Snapshot { game_state }
                                    if matches!(game_state.status, GameStatus::Complete { .. })
                            );
                            if let Err(e) = self.process_event(event).await {
                                error!("Failed to process event in partition {}: {}", self.partition_id, e);
                            } else if is_terminal_snapshot {
                                terminal_snapshots_seen.insert(game_id);
                                if durable_completion_markers.remove(&game_id) {
                                    terminal_snapshots_seen.remove(&game_id);
                                    self.evict_game(game_id).await;
                                }
                            }
                        }
                        None => {
                            error!("Partition {} subscription closed unexpectedly, replication worker exiting",
                                self.partition_id);
                            break;
                        }
                    }
                }

                // Completion commands are published only after the final state is durable.
                // Until then, keep the completed in-memory state available for refreshes.
                Some(command) = command_receiver.recv() => {
                    if let StreamEvent::StatusUpdated {
                        game_id,
                        status: GameStatus::Complete { .. },
                    } = command
                    {
                        if terminal_snapshots_seen.remove(&game_id) {
                            self.evict_game(game_id).await;
                        } else {
                            durable_completion_markers.insert(game_id);
                        }
                    }
                }

                // Drain snapshot requests (processed by game executor, not used here)
                Some(_) = snapshot_request_receiver.recv() => {
                    // Snapshot requests are handled by the game executor, we just drain them
                    // to prevent the channel from filling up and stalling the stream reader
                }
            }
        }

        Ok(())
    }
}

/// Manager for running multiple replicas
pub struct ReplicationManager {
    workers: Vec<tokio::task::JoinHandle<Result<()>>>,
    game_states: GameStateStore,
    game_event_broadcasters: GameEventBroadcasters,
    game_stream_seqs: GameStreamSeqs,
    statuses: Arc<RwLock<HashMap<u32, Arc<RwLock<ReplicationStatus>>>>>,
    bus: Arc<GameBus>,
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
        let states = self.game_states.read().await;
        states.get(&game_id).cloned()
    }

    async fn get_partition_games(&self, partition_id: u32) -> Vec<(u32, GameState)> {
        let states = self.game_states.read().await;
        states
            .iter()
            .filter(|(game_id, _)| *game_id % PARTITION_COUNT == partition_id)
            .map(|(id, state)| (*id, state.clone()))
            .collect()
    }

    async fn is_ready(&self) -> bool {
        // Replicas are ready from startup; see PartitionReplica::new
        true
    }
}

impl ReplicationManager {
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

        // Watermark is read BEFORE the state (separate locks): if an event
        // lands in between, the filter under-estimates and re-delivers an
        // event already in the state — harmless, receivers skip stale
        // stream_seqs. The reverse order could silently drop an event.
        let watermark = self.get_stream_seq(game_id).await;
        let game_state = self
            .get_game_state(game_id)
            .await
            .context("Game not available in replication manager")?;

        let filtered_receiver = FilteredEventReceiver {
            inner: receiver,
            min_sequence: game_state.event_sequence,
            min_stream_seq: watermark,
            game_id,
        };

        Ok((game_state, watermark, filtered_receiver))
    }

    /// Last transport stream_seq applied to the replica state of a game.
    pub async fn get_stream_seq(&self, game_id: u32) -> u64 {
        let seqs = self.game_stream_seqs.read().await;
        seqs.get(&game_id).copied().unwrap_or(0)
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
        redis_url: &str,
    ) -> Result<Self> {
        let game_states = Arc::new(RwLock::new(HashMap::new()));
        let game_event_broadcasters = Arc::new(RwLock::new(HashMap::new()));
        let game_stream_seqs: GameStreamSeqs = Arc::new(RwLock::new(HashMap::new()));
        let statuses = Arc::new(RwLock::new(HashMap::new()));
        let mut workers = Vec::new();

        // The replication workers get their own Redis connection, isolated
        // from the main server's connection. The push channel only satisfies
        // the shared connection-manager config; nothing here subscribes to
        // Pub/Sub pushes.
        let redis_client = redis::Client::open(redis_url)?;
        let (pubsub_tx, _pubsub_rx) = tokio::sync::broadcast::channel(5000);
        let redis =
            crate::redis_utils::create_connection_manager(redis_client.clone(), pubsub_tx.clone())
                .await?;

        let bus = Arc::new(GameBus::new(
            redis,
            redis_client,
            cancellation_token.clone(),
        ));

        for partition_id in partitions {
            // Create worker
            let worker = PartitionReplica::new(
                partition_id,
                bus.clone(),
                game_states.clone(),
                game_event_broadcasters.clone(),
                game_stream_seqs.clone(),
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
            game_states,
            game_event_broadcasters,
            game_stream_seqs,
            statuses,
            bus,
        })
    }

    /// Get the shared game state store
    pub fn game_states(&self) -> GameStateStore {
        self.game_states.clone()
    }

    /// Check if all workers are ready (always true; see PartitionReplica::new)
    pub async fn is_ready(&self) -> bool {
        true
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
    use super::FilteredEventReceiver;
    use common::{GameEvent, GameEventMessage, GameState, GameType, QueueMode};
    use tokio::sync::broadcast;

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
        let mut filtered = FilteredEventReceiver::new(rx, 0, 500, 1);

        tx.send(event(0, 400)).unwrap(); // stale: absorbed by the snapshot
        tx.send(event(0, 501)).unwrap(); // fresh
        let got = filtered.recv().await.unwrap();
        assert_eq!(got.stream_seq, 501);
    }

    #[tokio::test]
    async fn snapshot_re_anchors_stream_after_executor_restart() {
        // Subscriber anchored to the OLD executor's stream at watermark 500.
        let (tx, rx) = broadcast::channel(16);
        let mut filtered = FilteredEventReceiver::new(rx, 0, 500, 1);

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
    async fn legacy_messages_without_stream_seq_filter_by_engine_sequence() {
        let (tx, rx) = broadcast::channel(16);
        let mut filtered = FilteredEventReceiver::new(rx, 10, 0, 1);

        tx.send(event(9, 0)).unwrap(); // stale by engine sequence
        tx.send(event(11, 0)).unwrap(); // fresh
        let got = filtered.recv().await.unwrap();
        assert_eq!(got.sequence, 11);
    }
}
