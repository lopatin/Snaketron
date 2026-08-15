//! Stateless gateway event routing.
//!
//! Every task tails all partition event streams and forwards per-game events
//! to locally subscribed sockets. Nothing here retains a reconstructed
//! `GameState`: the only live authoritative state in the region is the lease
//! holder's actor state, and the only durable copy is its fenced Redis
//! recovery envelope. Joins first-frame from that envelope (the "recovery
//! bridge") and re-anchor on the next authoritative `Snapshot`; everything in
//! between is governed by the per-subscription continuity rules in
//! [`GameEventSubscription`].

use crate::game_bus::{GameBus, PartitionEvent, PartitionEventSubscription};
use anyhow::Result;
use common::{GameEvent, GameEventMessage, GameState, GameStatus};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, broadcast};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Per-game broadcast channels, created only when a local socket subscribes.
/// A game with no local subscriber has no entry and costs nothing.
pub type GameEventBroadcasters = Arc<RwLock<HashMap<u32, broadcast::Sender<GameEventMessage>>>>;

/// How often a cold subscription (or a fresh join) may publish a targeted
/// snapshot request, per game, per gateway — coalesced across every local
/// socket waiting on that game.
const TARGETED_REQUEST_INTERVAL_MS: i64 = 500;

/// Tracks one partition reader's readiness.
#[derive(Debug, Clone)]
pub struct ReplicationStatus {
    pub partition_id: u32,
    pub is_ready: bool,
}

/// Pure pacing state for targeted snapshot requests: at most one publish per
/// game per [`TARGETED_REQUEST_INTERVAL_MS`] on this gateway, regardless of
/// how many sockets are waiting on the game.
#[derive(Default)]
struct RequestPacer {
    last_request_ms: HashMap<u32, i64>,
}

impl RequestPacer {
    /// Entries are pruned opportunistically so the map stays bounded by the
    /// number of games requested in the last interval, not ever requested.
    const PRUNE_THRESHOLD: usize = 256;

    fn should_publish(&mut self, game_id: u32, now_ms: i64) -> bool {
        if let Some(&last) = self.last_request_ms.get(&game_id)
            && now_ms >= last
            && now_ms - last < TARGETED_REQUEST_INTERVAL_MS
        {
            return false;
        }
        if self.last_request_ms.len() >= Self::PRUNE_THRESHOLD {
            self.last_request_ms
                .retain(|_, last| now_ms - *last < TARGETED_REQUEST_INTERVAL_MS);
        }
        self.last_request_ms.insert(game_id, now_ms);
        true
    }
}

/// Shared targeted-snapshot request path. The pacer is claimed before the
/// publish so concurrent callers coalesce even while an XADD is in flight.
///
/// `bus` is `None` only in unit tests that exercise pure subscription
/// continuity; production construction always supplies one.
pub struct SnapshotRequester {
    bus: Option<Arc<GameBus>>,
    pacer: Mutex<RequestPacer>,
}

impl SnapshotRequester {
    fn new(bus: Arc<GameBus>) -> Self {
        Self {
            bus: Some(bus),
            pacer: Mutex::new(RequestPacer::default()),
        }
    }

    #[cfg(test)]
    pub(crate) fn detached() -> Self {
        Self {
            bus: None,
            pacer: Mutex::new(RequestPacer::default()),
        }
    }

    /// Ask the executor to republish one game's snapshot. Returns whether this
    /// call published a request; `false` means a recent caller already did.
    pub async fn request_game_snapshot(&self, game_id: u32) -> Result<bool> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        if !self.pacer.lock().await.should_publish(game_id, now_ms) {
            return Ok(false);
        }
        let Some(bus) = &self.bus else {
            return Ok(true);
        };
        bus.request_game_snapshot(game_id).await?;
        Ok(true)
    }
}

/// Continuity state of one socket's subscription to one game.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Continuity {
    /// No trusted anchor: sequenced deltas are suppressed until a `Snapshot`
    /// (or a contiguous continuation of an explicit [`anchor`]) arrives.
    Cold,
    Live {
        last_stream_seq: u64,
    },
}

/// What [`GameEventSubscription::next`] hands the forwarding loop.
///
/// This value is consumed immediately by the caller's `match` and never
/// stored. Boxing the event would add a heap allocation to every live game
/// event only to reduce stack padding on the cold `WentCold` arm.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum SubscriptionUpdate {
    /// Forward this event to the socket.
    Event(GameEventMessage),
    /// The subscription lost continuity (gap or broadcast lag) and suppressed
    /// delivery until the next snapshot; a targeted request is already paced.
    /// Any in-flight work tied to the previous snapshot is now stale.
    WentCold,
}

/// One socket's ordered, continuity-checked view of a game's event stream.
///
/// The router holds no game state, so continuity is a per-subscription
/// property with these rules (they are load-bearing — see the PRD's gateway
/// event-router requirements):
/// - a subscription is cold until it forwards an authoritative `Snapshot`;
///   while cold, sequenced deltas are suppressed and the targeted snapshot
///   request is re-paced on the shared 500 ms cadence;
/// - every `Snapshot` re-anchors the watermark unconditionally, because a
///   restarted or failed-over executor begins a new `stream_seq` sequence;
/// - `stream_seq == 0` events are out-of-band terminal `CommandRejected`
///   messages, forwarded by stable command identity even while cold;
/// - a sequence gap or local broadcast lag returns the subscription to cold
///   rather than forwarding unverified continuations.
pub struct GameEventSubscription {
    inner: broadcast::Receiver<GameEventMessage>,
    game_id: u32,
    requester: Arc<SnapshotRequester>,
    continuity: Continuity,
}

impl GameEventSubscription {
    /// Build a subscription over a bare channel with a detached requester,
    /// for unit tests of continuity behavior.
    #[cfg(test)]
    pub(crate) fn for_test(inner: broadcast::Receiver<GameEventMessage>, game_id: u32) -> Self {
        Self {
            inner,
            game_id,
            requester: Arc::new(SnapshotRequester::detached()),
            continuity: Continuity::Cold,
        }
    }

    /// Adopt a known-good watermark — the recovery bridge snapshot's — so
    /// deltas contiguous with the checkpointed envelope forward immediately
    /// without waiting for a live snapshot. Any lost event in between
    /// surfaces as an ordinary gap and re-anchors through a snapshot.
    pub fn anchor(&mut self, stream_seq: u64) {
        self.continuity = Continuity::Live {
            last_stream_seq: stream_seq,
        };
    }

    /// Receive the next forwardable event, or `None` when the game's channel
    /// is gone (terminal teardown, or the reader worker failed — which is
    /// task-fatal anyway).
    pub async fn next(&mut self) -> Option<SubscriptionUpdate> {
        loop {
            let received = if self.continuity == Continuity::Cold {
                tokio::select! {
                    biased;
                    received = self.inner.recv() => received,
                    _ = tokio::time::sleep(std::time::Duration::from_millis(
                        TARGETED_REQUEST_INTERVAL_MS as u64,
                    )) => {
                        self.request_snapshot().await;
                        continue;
                    }
                }
            } else {
                self.inner.recv().await
            };

            let event_msg = match received {
                Ok(event_msg) => event_msg,
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(
                        game_id = self.game_id,
                        skipped, "Game subscription lagged; going cold until a fresh snapshot"
                    );
                    if self.go_cold().await {
                        return Some(SubscriptionUpdate::WentCold);
                    }
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            };

            // Snapshots re-anchor the stream unconditionally. A restarted
            // executor (failover/resume) begins a NEW stream_seq sequence
            // starting near 1; filtering its snapshot as "stale" against the
            // old stream's high watermark would wedge this subscriber — and
            // its client — forever.
            if matches!(event_msg.event, GameEvent::Snapshot { .. }) {
                self.continuity = Continuity::Live {
                    last_stream_seq: event_msg.stream_seq,
                };
                return Some(SubscriptionUpdate::Event(event_msg));
            }

            if event_msg.stream_seq == 0 {
                // A zero sequence is reserved for out-of-band terminal command
                // rejections, which carry their own stable command identity
                // and must reach the player even during warm-up. Any other
                // unsequenced event cannot be ordered and is dropped.
                if matches!(event_msg.event, GameEvent::CommandRejected { .. }) {
                    return Some(SubscriptionUpdate::Event(event_msg));
                }
                continue;
            }

            match self.continuity {
                Continuity::Cold => {
                    // A delta cannot be applied to unknown client state; the
                    // paced targeted request will re-anchor with a snapshot.
                    debug!(
                        game_id = self.game_id,
                        stream_seq = event_msg.stream_seq,
                        "Suppressing delta while subscription is cold"
                    );
                    continue;
                }
                Continuity::Live { last_stream_seq } => {
                    if event_msg.stream_seq <= last_stream_seq {
                        // Duplicate or stale relative to the anchor.
                        continue;
                    }
                    if event_msg.stream_seq == last_stream_seq + 1 {
                        self.continuity = Continuity::Live {
                            last_stream_seq: event_msg.stream_seq,
                        };
                        return Some(SubscriptionUpdate::Event(event_msg));
                    }
                    warn!(
                        game_id = self.game_id,
                        expected = last_stream_seq + 1,
                        got = event_msg.stream_seq,
                        "Game subscription detected stream gap; going cold until a fresh snapshot"
                    );
                    if self.go_cold().await {
                        return Some(SubscriptionUpdate::WentCold);
                    }
                    continue;
                }
            }
        }
    }

    /// Returns whether this was a live→cold transition (worth reporting).
    async fn go_cold(&mut self) -> bool {
        let was_live = self.continuity != Continuity::Cold;
        self.continuity = Continuity::Cold;
        self.request_snapshot().await;
        was_live
    }

    async fn request_snapshot(&self) {
        if let Err(error) = self.requester.request_game_snapshot(self.game_id).await {
            warn!(
                game_id = self.game_id,
                %error,
                "Failed to publish targeted snapshot request"
            );
        }
    }
}

/// Forward an event to the game's local subscribers, if any, and tear the
/// channel down once nothing further can be broadcast for the game.
async fn forward_event(channels: &GameEventBroadcasters, event_msg: GameEventMessage) {
    let game_id = event_msg.game_id;
    let is_terminal_snapshot = matches!(
        &event_msg.event,
        GameEvent::Snapshot { game_state }
            if matches!(game_state.status, GameStatus::Complete { .. })
    );

    let delivery_failed = {
        let channels = channels.read().await;
        match channels.get(&game_id) {
            // SendError means the last receiver is gone — normal as
            // clients finish or switch generations.
            Some(sender) => sender.send(event_msg).is_err(),
            None => false,
        }
    };

    if is_terminal_snapshot {
        // The fenced completion transaction made the terminal state durable
        // before this event was published, and nothing further will be
        // broadcast for the game. Receivers drain the terminal snapshot
        // first, then observe Closed.
        if channels.write().await.remove(&game_id).is_some() {
            info!(
                game_id,
                "Forwarded terminal snapshot; dropped game broadcaster"
            );
        }
        return;
    }

    if delivery_failed {
        let mut channels = channels.write().await;
        // Re-check under the write lock: a new subscriber may have attached
        // to this exact sender since the failed send.
        if let Some(sender) = channels.get(&game_id)
            && sender.receiver_count() == 0
        {
            channels.remove(&game_id);
            debug!(game_id, "Dropped game broadcaster with no subscribers");
        }
    }
}

/// One partition's resumable event reader: anchors on the ordered event
/// stream, proves readiness through the boot-unique barrier marker, then
/// forwards each game's events to its local broadcast channel.
pub struct PartitionEventRouter {
    partition_id: u32,
    bus: Arc<GameBus>,
    channels: GameEventBroadcasters,
    status: Arc<RwLock<ReplicationStatus>>,
    cancellation_token: CancellationToken,
}

impl PartitionEventRouter {
    fn new(
        partition_id: u32,
        bus: Arc<GameBus>,
        channels: GameEventBroadcasters,
        cancellation_token: CancellationToken,
    ) -> Self {
        let status = Arc::new(RwLock::new(ReplicationStatus {
            partition_id,
            // Readiness becomes true only after the reader is anchored and
            // its boot-unique barrier marker was consumed in order.
            is_ready: false,
        }));

        Self {
            partition_id,
            bus,
            channels,
            status,
            cancellation_token,
        }
    }

    pub fn status(&self) -> Arc<RwLock<ReplicationStatus>> {
        self.status.clone()
    }

    /// Run the partition reader.
    pub async fn run(self) -> Result<()> {
        info!(
            "Starting event router worker for partition {}",
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
                    "Event stream anchor unavailable; retrying locally"
                ),
            }
            tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => return Ok(()),
                _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {}
            }
        };

        let PartitionEventSubscription {
            partition_id: _,
            mut event_receiver,
        } = subscription;

        // A gateway must not enter routing merely because its request was
        // appended. Retry the boot-unique request until the executor publishes
        // its completion marker after every active actor's requested snapshot.
        // The marker shares this exact ordered stream, so observing it proves
        // an anchored reader that saw every preceding snapshot in order (the
        // reader forwards them without retaining state); empty partitions work
        // too. The request/marker wire shape is version-stable: old executors
        // answering it with the full fan-out remain a correct, strictly
        // stronger response, so no protocol bump accompanies this contract.
        let completion_id = uuid::Uuid::new_v4().to_string();
        let mut request_retry = tokio::time::interval(std::time::Duration::from_secs(2));
        request_retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => return Ok(()),
                _ = request_retry.tick() => {
                    let request = self.bus.request_partition_snapshots_with_barrier(
                            self.partition_id,
                            &completion_id,
                        );
                    let result = tokio::select! {
                        biased;
                        _ = self.cancellation_token.cancelled() => return Ok(()),
                        result = request => result,
                    };
                    if let Err(error) = result {
                        warn!(
                            partition = self.partition_id,
                            %error,
                            "Initial reader barrier request unavailable; retrying locally"
                        );
                    }
                }
                event = event_receiver.recv() => {
                    match event {
                        Some(PartitionEvent::Game(event)) => {
                            forward_event(&self.channels, event).await;
                        }
                        Some(PartitionEvent::SnapshotBarrier {
                            completion_id: observed,
                        }) if observed == completion_id => break,
                        Some(PartitionEvent::SnapshotBarrier { .. }) => {}
                        Some(PartitionEvent::Discontinuity {
                            resume_id,
                            oldest_id,
                        }) => anyhow::bail!(
                            "partition {} event stream crossed its trim horizon before readiness (resume {}, oldest {})",
                            self.partition_id,
                            resume_id,
                            oldest_id
                        ),
                        None => anyhow::bail!(
                            "partition {} subscription closed before initial reader barrier",
                            self.partition_id
                        ),
                    }
                }
            }
        }

        self.status.write().await.is_ready = true;
        info!(
            partition = self.partition_id,
            %completion_id,
            "Initial reader barrier consumed"
        );

        // Main forwarding loop.
        loop {
            tokio::select! {
                biased;

                _ = self.cancellation_token.cancelled() => {
                    info!("Event router worker for partition {} shutting down", self.partition_id);
                    break;
                }

                event = event_receiver.recv() => {
                    match event {
                        Some(PartitionEvent::Game(event)) => {
                            forward_event(&self.channels, event).await;
                        }
                        Some(PartitionEvent::SnapshotBarrier { .. }) => {}
                        Some(PartitionEvent::Discontinuity {
                            resume_id,
                            oldest_id,
                        }) => {
                            self.status.write().await.is_ready = false;
                            anyhow::bail!(
                                "partition {} event stream crossed its trim horizon after readiness (resume {}, oldest {})",
                                self.partition_id,
                                resume_id,
                                oldest_id
                            );
                        }
                        None => {
                            self.status.write().await.is_ready = false;
                            anyhow::bail!(
                                "partition {} subscription closed unexpectedly after readiness",
                                self.partition_id
                            );
                        }
                    }
                }

            }
        }

        self.status.write().await.is_ready = false;

        Ok(())
    }
}

/// Stateless per-task router over all partition event streams.
pub struct GameEventRouter {
    workers: Vec<tokio::task::JoinHandle<Result<()>>>,
    channels: GameEventBroadcasters,
    statuses: Arc<RwLock<HashMap<u32, Arc<RwLock<ReplicationStatus>>>>>,
    requester: Arc<SnapshotRequester>,
}

impl GameEventRouter {
    /// Create and start reader workers for the specified partitions.
    pub async fn new(
        partitions: Vec<u32>,
        cancellation_token: CancellationToken,
        bus: Arc<GameBus>,
    ) -> Result<Self> {
        let channels: GameEventBroadcasters = Arc::new(RwLock::new(HashMap::new()));
        let statuses = Arc::new(RwLock::new(HashMap::new()));
        let requester = Arc::new(SnapshotRequester::new(bus.clone()));
        let mut workers = Vec::new();

        for partition_id in partitions {
            let worker = PartitionEventRouter::new(
                partition_id,
                bus.clone(),
                channels.clone(),
                cancellation_token.clone(),
            );

            {
                let mut status_map = statuses.write().await;
                status_map.insert(partition_id, worker.status());
            }

            let handle = tokio::spawn(worker.run());
            workers.push(handle);
        }

        Ok(Self {
            workers,
            channels,
            statuses,
            requester,
        })
    }

    /// Subscribe to a game's events. Infallible: the per-game channel is
    /// created on demand, and the subscription starts cold — the caller
    /// provides the first frame (recovery bridge or live snapshot) and may
    /// [`GameEventSubscription::anchor`] on its watermark.
    ///
    /// Registering the receiver here, BEFORE requesting or sending any
    /// snapshot, guarantees no event between snapshot and subscription can be
    /// missed; the per-subscription continuity rules drop the overlap instead.
    pub async fn subscribe_to_game(&self, game_id: u32) -> GameEventSubscription {
        let receiver = {
            let mut channels = self.channels.write().await;
            // Opportunistically drop channels whose last subscriber left, so
            // abandoned games do not accumulate entries.
            channels.retain(|id, sender| *id == game_id || sender.receiver_count() > 0);
            channels
                .entry(game_id)
                .or_insert_with(|| broadcast::channel(1028).0)
                .subscribe()
        };

        GameEventSubscription {
            inner: receiver,
            game_id,
            requester: self.requester.clone(),
            continuity: Continuity::Cold,
        }
    }

    /// Ask the executor to republish one game's snapshot, coalesced to at
    /// most one request per game per 500 ms on this gateway. Actor-side
    /// checkpoint coalescing bounds the executor's output independently.
    pub async fn request_game_snapshot(&self, game_id: u32) -> Result<bool> {
        self.requester.request_game_snapshot(game_id).await
    }

    /// Load the most recently checkpointed game snapshot from Redis. Live
    /// games update it on every checkpoint; durably completed games leave
    /// their terminal snapshot in it for a short grace period.
    pub async fn get_stored_snapshot(&self, game_id: u32) -> Result<Option<GameState>> {
        let bus = self
            .requester
            .bus
            .as_ref()
            .expect("production router always has a bus");
        bus.get_stored_snapshot(game_id).await
    }

    /// Check that every configured stream reader consumed its boot-unique
    /// barrier and that none of the worker tasks has exited unexpectedly.
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

    /// Get status of all workers.
    pub async fn get_status(&self) -> HashMap<u32, ReplicationStatus> {
        let mut result = HashMap::new();
        let statuses = self.statuses.read().await;
        for (partition_id, status) in statuses.iter() {
            let s = status.read().await;
            result.insert(*partition_id, s.clone());
        }
        result
    }

    /// Wait for all workers to complete.
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
        GameEventBroadcasters, GameEventRouter, PartitionEventRouter, RequestPacer,
        SnapshotRequester, SubscriptionUpdate, TARGETED_REQUEST_INTERVAL_MS,
    };
    use crate::game_bus::{GameBus, SnapshotRequest};
    use crate::game_executor::PARTITION_COUNT;
    use crate::redis_keys::RedisKeys;
    use common::{GameEvent, GameEventMessage, GameState, GameStatus, GameType, QueueMode};
    use redis::AsyncCommands;
    use redis::streams::StreamRangeReply;
    use std::sync::Arc;
    use tokio::sync::broadcast;
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
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        GameEventMessage {
            game_id: 1,
            tick: state.tick,
            sequence,
            stream_seq,
            user_id: None,
            event: GameEvent::Snapshot { game_state: state },
        }
    }

    fn terminal_snapshot(game_id: u32, stream_seq: u64) -> GameEventMessage {
        let mut state = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, None, 0);
        state.status = GameStatus::Complete {
            winning_snake_id: None,
        };
        GameEventMessage {
            game_id,
            tick: state.tick,
            sequence: state.event_sequence,
            stream_seq,
            user_id: None,
            event: GameEvent::Snapshot { game_state: state },
        }
    }

    async fn test_bus(db: u32) -> (Arc<GameBus>, redis::Client, CancellationToken) {
        let client = redis::Client::open(format!("redis://127.0.0.1:6379/{db}?protocol=resp3"))
            .expect("valid local Redis URL");
        let (push_tx, _push_rx) = broadcast::channel(8);
        let manager = crate::redis_utils::create_connection_manager(client.clone(), push_tx)
            .await
            .expect("local Redis is required for routing tests");
        let token = CancellationToken::new();
        let bus = Arc::new(
            GameBus::new(
                manager.clone(),
                (0..PARTITION_COUNT)
                    .map(|_| manager.clone().into())
                    .collect(),
                (0..PARTITION_COUNT)
                    .map(|_| manager.clone().into())
                    .collect(),
                manager.clone(),
                manager,
                client.clone(),
                token.clone(),
            )
            .expect("test GameBus"),
        );
        (bus, client, token)
    }

    fn test_subscription() -> (
        broadcast::Sender<GameEventMessage>,
        super::GameEventSubscription,
    ) {
        let (tx, rx) = broadcast::channel(16);
        (tx, super::GameEventSubscription::for_test(rx, 1))
    }

    fn expect_event(update: Option<SubscriptionUpdate>) -> GameEventMessage {
        match update {
            Some(SubscriptionUpdate::Event(event)) => event,
            other => panic!("expected a forwarded event, got {other:?}"),
        }
    }

    #[test]
    fn targeted_requests_coalesce_per_game_and_stay_bounded() {
        let mut pacer = RequestPacer::default();

        assert!(pacer.should_publish(1, 1_000));
        assert!(!pacer.should_publish(1, 1_000 + TARGETED_REQUEST_INTERVAL_MS - 1));
        assert!(pacer.should_publish(2, 1_200), "games pace independently");
        assert!(pacer.should_publish(1, 1_000 + TARGETED_REQUEST_INTERVAL_MS));

        // A clock that moved backwards must not wedge the pacer forever.
        assert!(pacer.should_publish(3, 5_000));
        assert!(pacer.should_publish(3, 4_000));

        // The map prunes expired entries rather than growing with every game
        // ever requested.
        let now = 100_000;
        for game_id in 100..(100 + RequestPacer::PRUNE_THRESHOLD as u32) {
            assert!(pacer.should_publish(game_id, now));
        }
        assert!(pacer.should_publish(9_999, now + TARGETED_REQUEST_INTERVAL_MS));
        assert!(pacer.last_request_ms.len() <= 2);
    }

    #[tokio::test]
    async fn cold_subscription_suppresses_deltas_until_snapshot_reanchors() {
        let (tx, mut subscription) = test_subscription();

        tx.send(event(1, 41)).unwrap(); // suppressed: no anchor yet
        tx.send(snapshot(2, 42)).unwrap();
        tx.send(event(3, 43)).unwrap();

        let got = expect_event(subscription.next().await);
        assert!(matches!(got.event, GameEvent::Snapshot { .. }));
        assert_eq!(got.stream_seq, 42);

        let got = expect_event(subscription.next().await);
        assert_eq!(got.stream_seq, 43);
    }

    #[tokio::test]
    async fn snapshot_reanchors_unconditionally_across_stream_epochs() {
        let (tx, mut subscription) = test_subscription();
        subscription.anchor(500);

        // A restarted executor begins a new stream: snapshot at stream_seq 2,
        // then ordinary events 3, 4. Without re-anchoring, ALL of these would
        // be filtered as stale (< 500) and the client would be wedged.
        tx.send(snapshot(7, 2)).unwrap();
        tx.send(event(8, 3)).unwrap();
        tx.send(event(8, 2)).unwrap(); // duplicate/stale vs the new anchor
        tx.send(event(9, 4)).unwrap();

        let got = expect_event(subscription.next().await);
        assert!(matches!(got.event, GameEvent::Snapshot { .. }));
        assert_eq!(got.stream_seq, 2);
        assert_eq!(expect_event(subscription.next().await).stream_seq, 3);
        assert_eq!(expect_event(subscription.next().await).stream_seq, 4);
    }

    #[tokio::test]
    async fn bridge_anchor_forwards_contiguous_deltas_without_a_live_snapshot() {
        let (tx, mut subscription) = test_subscription();
        subscription.anchor(100);

        tx.send(event(1, 100)).unwrap(); // already folded into the bridge
        tx.send(event(2, 101)).unwrap();
        tx.send(event(3, 102)).unwrap();

        assert_eq!(expect_event(subscription.next().await).stream_seq, 101);
        assert_eq!(expect_event(subscription.next().await).stream_seq, 102);
    }

    #[tokio::test]
    async fn gap_goes_cold_and_recovers_on_the_requested_snapshot() {
        let (tx, mut subscription) = test_subscription();
        subscription.anchor(100);

        tx.send(event(1, 105)).unwrap(); // gap: 101..=104 lost
        tx.send(event(2, 106)).unwrap(); // suppressed while cold
        tx.send(snapshot(3, 107)).unwrap();
        tx.send(event(4, 108)).unwrap();

        assert!(matches!(
            subscription.next().await,
            Some(SubscriptionUpdate::WentCold)
        ));
        let got = expect_event(subscription.next().await);
        assert!(matches!(got.event, GameEvent::Snapshot { .. }));
        assert_eq!(got.stream_seq, 107);
        assert_eq!(expect_event(subscription.next().await).stream_seq, 108);
    }

    #[tokio::test]
    async fn zero_seq_terminal_rejections_pass_even_while_cold() {
        use common::ClientCommandIdentityV2;

        let (tx, mut subscription) = test_subscription();

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
            command_id_client: None,
            session_rejected_from: None,
        };
        tx.send(event(1, 0)).unwrap(); // unsequenced state event: dropped
        tx.send(rejection).unwrap();

        let got = expect_event(subscription.next().await);
        assert!(matches!(got.event, GameEvent::CommandRejected { .. }));
    }

    #[tokio::test]
    async fn lagged_subscription_goes_cold_once_and_reanchors() {
        let (tx, mut subscription) = test_subscription();
        subscription.anchor(0);

        // Overflow the 16-slot channel so the receiver observes Lagged.
        for stream_seq in 1..=40u64 {
            tx.send(event(stream_seq, stream_seq)).unwrap();
        }
        assert!(matches!(
            subscription.next().await,
            Some(SubscriptionUpdate::WentCold)
        ));

        tx.send(snapshot(41, 41)).unwrap();
        let got = expect_event(subscription.next().await);
        assert!(matches!(got.event, GameEvent::Snapshot { .. }));
        assert_eq!(got.stream_seq, 41);
    }

    #[tokio::test]
    async fn subscription_ends_when_the_channel_closes() {
        let (tx, mut subscription) = test_subscription();
        subscription.anchor(0);
        tx.send(event(1, 1)).unwrap();
        drop(tx);

        assert_eq!(expect_event(subscription.next().await).stream_seq, 1);
        assert!(subscription.next().await.is_none());
    }

    #[tokio::test]
    async fn terminal_snapshot_is_forwarded_before_channel_teardown() {
        let channels: GameEventBroadcasters = Default::default();
        let manager = GameEventRouter {
            workers: Vec::new(),
            channels: channels.clone(),
            statuses: Default::default(),
            requester: Arc::new(SnapshotRequester::detached()),
        };
        let mut subscription = manager.subscribe_to_game(1).await;

        super::forward_event(&channels, terminal_snapshot(1, 7)).await;

        let got = expect_event(subscription.next().await);
        assert_eq!(got.stream_seq, 7);
        assert!(matches!(
            got.event,
            GameEvent::Snapshot { game_state }
                if matches!(game_state.status, GameStatus::Complete { .. })
        ));
        assert!(subscription.next().await.is_none());
        assert!(channels.read().await.is_empty());
    }

    #[tokio::test]
    async fn abandoned_channels_are_pruned_without_disturbing_live_ones() {
        let manager = GameEventRouter {
            workers: Vec::new(),
            channels: Default::default(),
            statuses: Default::default(),
            requester: Arc::new(SnapshotRequester::detached()),
        };

        let abandoned = manager.subscribe_to_game(7).await;
        let _live = manager.subscribe_to_game(8).await;
        drop(abandoned);

        let _fresh = manager.subscribe_to_game(9).await;
        let channels = manager.channels.read().await;
        assert!(!channels.contains_key(&7), "abandoned channel pruned");
        assert!(channels.contains_key(&8), "live channel retained");
        assert!(channels.contains_key(&9));
    }

    #[tokio::test]
    async fn readiness_waits_for_matching_barrier_and_forwards_preceding_snapshots()
    -> anyhow::Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let (bus, client, token) = test_bus(9).await;
            let partition = 1;
            let event_stream = RedisKeys::stream_events(partition);
            let request_stream = RedisKeys::stream_snapshot_requests(partition);
            let mut redis = client.get_multiplexed_async_connection().await?;
            let _: i64 = redis.del(&[&event_stream, &request_stream]).await?;

            let channels: GameEventBroadcasters = Default::default();
            let manager = GameEventRouter {
                workers: Vec::new(),
                channels: channels.clone(),
                statuses: Default::default(),
                requester: Arc::new(SnapshotRequester::new(bus.clone())),
            };
            let mut subscription = manager.subscribe_to_game(1).await;

            let worker = PartitionEventRouter::new(partition, bus, channels, token.clone());
            let status = worker.status();
            let worker = tokio::spawn(worker.run());

            let request = loop {
                let entries: StreamRangeReply =
                    redis.xrange_count(&request_stream, "-", "+", 1).await?;
                if let Some(entry) = entries.ids.first()
                    && let Some(payload) = entry.map.get("data")
                {
                    let bytes = redis::from_redis_value::<Vec<u8>>(payload)?;
                    break serde_json::from_slice::<SnapshotRequest>(&bytes)?;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            };
            assert_eq!(
                request.game_id, None,
                "partition readiness must request every active game"
            );
            let completion_id = request
                .completion_id
                .expect("readiness request includes a completion ID");
            assert!(!status.read().await.is_ready);

            let _: String = redis::cmd("XADD")
                .arg(&event_stream)
                .arg("*")
                .arg("snapshot_barrier")
                .arg(uuid::Uuid::new_v4().to_string())
                .query_async(&mut redis)
                .await?;
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            assert!(
                !status.read().await.is_ready,
                "a foreign barrier must not satisfy this boot's readiness proof"
            );

            let pre_barrier_snapshot = snapshot(1, 1);
            let _: String = redis::cmd("XADD")
                .arg(&event_stream)
                .arg("*")
                .arg("data")
                .arg(serde_json::to_vec(&pre_barrier_snapshot)?)
                .query_async(&mut redis)
                .await?;
            let _: String = redis::cmd("XADD")
                .arg(&event_stream)
                .arg("*")
                .arg("snapshot_barrier")
                .arg(&completion_id)
                .query_async(&mut redis)
                .await?;

            loop {
                if status.read().await.is_ready {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }

            // The reader forwarded (not retained) the pre-barrier snapshot.
            let got = expect_event(subscription.next().await);
            assert!(matches!(got.event, GameEvent::Snapshot { .. }));
            assert_eq!(got.stream_seq, pre_barrier_snapshot.stream_seq);

            token.cancel();
            worker.await??;
            let _: i64 = redis.del(&[&event_stream, &request_stream]).await?;
            Ok::<(), anyhow::Error>(())
        })
        .await?
    }
}
