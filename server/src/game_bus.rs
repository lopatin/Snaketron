//! The game-critical message bus: partition-scoped events, commands, and
//! snapshot requests between game executors, replicas, and WebSocket servers.
//!
//! Backed by Redis/Valkey Streams — an ordered, trimmed, replayable log per
//! partition. Readers resume from their last-delivered entry, so subscriber
//! blips lose nothing and slow consumers get real backpressure instead of
//! drops. The `stream_seq`/resync machinery remains as defense-in-depth for
//! the hops beyond this bus (broadcast fan-out, the WebSocket leg) and for
//! trim-horizon loss after a long outage; its counters should sit at ~0
//! (see DEBUGGING.md).
//!
//! Loss-tolerant fan-out (chat, lobby updates, user counts) is NOT routed
//! through this bus — it stays on plain Pub/Sub
//! (see `PubSubManager::subscribe_to_channel`).
//!
//! ## Streams latency rules (learned the hard way — see STREAMS_MIGRATION.md)
//!
//! The 2025 Streams implementation was abandoned because blocking `XREAD`s
//! were multiplexed onto shared connections, adding 100–900 ms per event.
//! The rules encoded here:
//! 1. Every blocking reader owns a DEDICATED connection; publishers use the
//!    shared non-blocking `ConnectionManager` and never queue behind a
//!    parked read.
//! 2. `XREAD BLOCK` is a push-like wait (Redis wakes the reader on write) —
//!    the BLOCK value is a liveness checkpoint, not a poll interval.
//! 3. One `XREAD` watches all three partition streams at once.
//! 4. Streams are trimmed with `MAXLEN ~` at publish time; no consumer
//!    groups, no XACK — consumers are fan-out readers tracking their own
//!    position.

use crate::game_executor::StreamEvent;
use crate::redis_keys::RedisKeys;
use anyhow::{Context, Result};
use common::{GameEvent, GameEventMessage, GameState};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use redis::streams::{StreamMaxlen, StreamReadOptions, StreamReadReply};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Snapshot request message for a partition
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SnapshotRequest {
    pub partition_id: u32,
    pub requester_id: Option<u64>, // Optional server ID of requester
}

/// A subscription to a partition's events, commands and requests
pub struct PartitionSubscription {
    pub partition_id: u32,
    pub event_receiver: mpsc::Receiver<GameEventMessage>,
    pub command_receiver: mpsc::Receiver<StreamEvent>,
    pub snapshot_request_receiver: mpsc::Receiver<SnapshotRequest>,
}

impl PartitionSubscription {
    pub async fn recv_event(&mut self) -> Option<GameEventMessage> {
        self.event_receiver.recv().await
    }

    pub async fn recv_command(&mut self) -> Option<StreamEvent> {
        self.command_receiver.recv().await
    }

    pub async fn recv_snapshot_request(&mut self) -> Option<SnapshotRequest> {
        self.snapshot_request_receiver.recv().await
    }
}

/// Approximate trim bounds (`MAXLEN ~`). At game rates (a handful of events
/// per 100 ms tick, 1/10 of games per partition) the events bound keeps
/// several minutes of backlog — ample for any reconnect window — while
/// keeping memory strictly bounded (the old implementation never trimmed).
const EVENTS_MAXLEN: usize = 8192;
// Commands get the same window as events, not a smaller one: they are the one
// message class with NO end-to-end sequence numbers, so loss past the trim
// horizon is detectable only by the reader's horizon check — give it the
// longest practical window.
const COMMANDS_MAXLEN: usize = 8192;
const SNAPREQ_MAXLEN: usize = 64;

/// How long one XREAD parks before returning empty. Purely a liveness /
/// cancellation checkpoint — delivery latency does not depend on it.
const XREAD_BLOCK_MS: usize = 5_000;
/// Max entries drained per XREAD round trip.
const XREAD_COUNT: usize = 512;
/// Capacity of the mpsc channels handed to subscribers. When full, the
/// reader awaits — backpressure, not drops.
const SUBSCRIBER_CHANNEL_CAPACITY: usize = 2000;
/// Backoff before rebuilding a failed reader connection.
const READER_RECONNECT_BACKOFF_MS: u64 = 100;

/// The game-critical transport. All methods take `&self`; internal
/// connections are cheaply cloneable handles.
pub struct GameBus {
    /// Shared non-blocking connection for XADD/KV. Never used for blocking
    /// reads (rule #1).
    redis: ConnectionManager,
    /// Used to open a dedicated connection per reader task.
    redis_client: redis::Client,
    cancellation_token: CancellationToken,
}

impl GameBus {
    pub fn new(
        redis: ConnectionManager,
        redis_client: redis::Client,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            redis,
            redis_client,
            cancellation_token,
        }
    }

    async fn xadd(&self, key: &str, maxlen: usize, payload: Vec<u8>) -> Result<()> {
        let mut redis = self.redis.clone();
        let _: String = redis
            .xadd_maxlen(key, StreamMaxlen::Approx(maxlen), "*", &[("data", payload)])
            .await
            .with_context(|| format!("Failed to XADD to {}", key))?;
        Ok(())
    }

    pub async fn publish_event(&self, partition_id: u32, event: &GameEventMessage) -> Result<()> {
        let payload = serde_json::to_vec(event).context("Failed to serialize event")?;
        self.xadd(
            &RedisKeys::stream_events(partition_id),
            EVENTS_MAXLEN,
            payload,
        )
        .await?;
        debug!(
            "XADD event to partition {} for game {} (stream_seq {})",
            partition_id, event.game_id, event.stream_seq
        );
        Ok(())
    }

    /// Publish a snapshot AND persist it under the game's snapshot key.
    /// Returns the constructed message so callers can trace exactly what was
    /// published.
    pub async fn publish_snapshot(
        &self,
        partition_id: u32,
        game_id: u32,
        snapshot: &GameState,
        stream_seq: u64,
    ) -> Result<GameEventMessage> {
        let event = GameEventMessage {
            game_id,
            tick: snapshot.tick,
            sequence: snapshot.event_sequence,
            stream_seq,
            user_id: None,
            event: GameEvent::Snapshot {
                game_state: snapshot.clone(),
            },
        };

        let payload = serde_json::to_vec(&event).context("Failed to serialize snapshot event")?;
        self.xadd(
            &RedisKeys::stream_events(partition_id),
            EVENTS_MAXLEN,
            payload,
        )
        .await?;
        self.store_snapshot(game_id, snapshot).await?;

        info!(
            "XADD snapshot for game {} at tick {} to partition {} (stream_seq {})",
            game_id, snapshot.tick, partition_id, stream_seq
        );
        Ok(event)
    }

    pub async fn publish_command(&self, partition_id: u32, command: &StreamEvent) -> Result<()> {
        let payload = serde_json::to_vec(command).context("Failed to serialize command")?;
        self.xadd(
            &RedisKeys::stream_commands(partition_id),
            COMMANDS_MAXLEN,
            payload,
        )
        .await
    }

    pub async fn request_partition_snapshots(&self, partition_id: u32) -> Result<()> {
        let request = SnapshotRequest {
            partition_id,
            requester_id: None,
        };
        let payload =
            serde_json::to_vec(&request).context("Failed to serialize snapshot request")?;
        self.xadd(
            &RedisKeys::stream_snapshot_requests(partition_id),
            SNAPREQ_MAXLEN,
            payload,
        )
        .await
    }

    /// Store a game snapshot in Redis WITHOUT publishing it to subscribers.
    ///
    /// Two callers rely on this: the game loop refreshes it periodically so a
    /// takeover executor can resume in-flight games from near-current state
    /// (see game_executor::resume_partition_games), and the completion path
    /// stores the terminal state as the reload fallback before publishing
    /// completion events and releasing the in-memory copy. 5-minute TTL: a
    /// game whose executor stays dead longer than that is not resumable.
    pub async fn store_snapshot(&self, game_id: u32, snapshot: &GameState) -> Result<()> {
        let key = RedisKeys::game_snapshot(game_id);
        let data =
            serde_json::to_vec(snapshot).context("Failed to serialize snapshot for storage")?;
        let mut redis = self.redis.clone();
        let _: () = redis
            .set_ex(&key, data, 300)
            .await
            .context("Failed to store snapshot")?;
        Ok(())
    }

    pub async fn get_stored_snapshot(&self, game_id: u32) -> Result<Option<GameState>> {
        let mut redis = self.redis.clone();
        let data: Option<Vec<u8>> = redis
            .get(RedisKeys::game_snapshot(game_id))
            .await
            .context("Failed to get snapshot from Redis")?;
        match data {
            Some(bytes) => Ok(Some(
                serde_json::from_slice(&bytes).context("Failed to deserialize snapshot")?,
            )),
            None => Ok(None),
        }
    }

    pub async fn subscribe_to_partition(&self, partition_id: u32) -> Result<PartitionSubscription> {
        let (event_tx, event_rx) = mpsc::channel(SUBSCRIBER_CHANNEL_CAPACITY);
        let (command_tx, command_rx) = mpsc::channel(SUBSCRIBER_CHANNEL_CAPACITY);
        let (request_tx, request_rx) = mpsc::channel(SUBSCRIBER_CHANNEL_CAPACITY);

        // Anchor all three streams SYNCHRONOUSLY, before this method returns,
        // on the warm shared connection. Anchoring inside the reader task
        // would leave a window (spawn -> new connection -> XINFO) during
        // which a published entry raises the tail past itself and is then
        // never delivered — e.g. a snapshot response beating a fresh
        // replica's reader startup. Anchored here, every XADD that happens
        // after subscribe() returns is strictly newer than the anchor.
        let mut redis = self.redis.clone();
        let last_ids = [
            resolve_stream_anchor(&mut redis, &RedisKeys::stream_events(partition_id)).await?,
            resolve_stream_anchor(&mut redis, &RedisKeys::stream_commands(partition_id)).await?,
            resolve_stream_anchor(
                &mut redis,
                &RedisKeys::stream_snapshot_requests(partition_id),
            )
            .await?,
        ];

        let client = self.redis_client.clone();
        let token = self.cancellation_token.clone();
        tokio::spawn(async move {
            partition_reader(
                client,
                partition_id,
                last_ids,
                event_tx,
                command_tx,
                request_tx,
                token,
            )
            .await;
        });

        Ok(PartitionSubscription {
            partition_id,
            event_receiver: event_rx,
            command_receiver: command_rx,
            snapshot_request_receiver: request_rx,
        })
    }
}

/// Resolve a stream's current tail ID as the subscription anchor.
///
/// Only a confirmed "no such key" maps to `0-0` (an empty stream has no
/// history, so "from creation" equals "from now"). Every OTHER error —
/// -LOADING after a Redis restart, connection resets, cluster redirects —
/// propagates instead of defaulting: defaulting to 0-0 on a live stream
/// would replay the entire retained backlog as fresh messages (re-running
/// stale commands and resurrecting completed games).
async fn resolve_stream_anchor(redis: &mut ConnectionManager, key: &str) -> Result<String> {
    match redis
        .xinfo_stream::<_, redis::streams::StreamInfoStreamReply>(key)
        .await
    {
        Ok(info) => Ok(info.last_generated_id),
        Err(e) if stream_does_not_exist(&e) => Ok("0-0".to_string()),
        Err(e) => Err(e).with_context(|| format!("Failed to anchor stream {}", key)),
    }
}

fn stream_does_not_exist(e: &redis::RedisError) -> bool {
    e.kind() == redis::ErrorKind::ResponseError
        && e.detail().is_some_and(|d| d.contains("no such key"))
}

/// The per-subscription reader task: one dedicated connection, one blocking
/// XREAD covering all three partition streams, entries forwarded with
/// backpressure. Survives connection failures by reconnecting and resuming
/// from the last-delivered IDs (zero loss within the trim window; falling
/// behind the trim horizon is detected and logged loudly).
///
/// `last_ids` are concrete tail anchors resolved by `subscribe_to_partition`
/// BEFORE the subscription was handed out. NEVER pass "$" to XREAD here:
/// "$" re-evaluates to "now" on every call, so an entry written to stream B
/// while we were processing stream A's reply would be skipped forever.
async fn partition_reader(
    client: redis::Client,
    partition_id: u32,
    mut last_ids: [String; 3],
    event_tx: mpsc::Sender<GameEventMessage>,
    command_tx: mpsc::Sender<StreamEvent>,
    request_tx: mpsc::Sender<SnapshotRequest>,
    cancellation_token: CancellationToken,
) {
    let events_key = RedisKeys::stream_events(partition_id);
    let commands_key = RedisKeys::stream_commands(partition_id);
    let requests_key = RedisKeys::stream_snapshot_requests(partition_id);

    let mut first_connect = true;
    'reconnect: loop {
        if cancellation_token.is_cancelled() {
            break;
        }

        // Dedicated connection: blocking XREADs park it, and nothing else
        // shares it (rule #1 — this is what made the old implementation slow).
        let mut conn = match client.get_multiplexed_async_connection().await {
            Ok(conn) => conn,
            Err(e) => {
                warn!(
                    "Stream reader for partition {} failed to connect: {}; retrying",
                    partition_id, e
                );
                tokio::time::sleep(std::time::Duration::from_millis(
                    READER_RECONNECT_BACKOFF_MS,
                ))
                .await;
                continue;
            }
        };
        debug!("Stream reader for partition {} connected", partition_id);

        // After an outage, verify we haven't fallen behind the trim horizon.
        // XREAD from a pre-horizon ID silently resumes at the oldest retained
        // entry — fine for the events stream (stream_seq gap detection heals
        // it downstream) but the commands stream has no sequence numbers, so
        // this loud log is the ONLY record that commands were lost.
        if !first_connect {
            for (slot, key) in [&events_key, &commands_key, &requests_key]
                .iter()
                .enumerate()
            {
                match conn
                    .xrange_count::<_, _, _, _, redis::streams::StreamRangeReply>(key, "-", "+", 1)
                    .await
                {
                    Ok(reply) => {
                        if let Some(oldest) = reply
                            .ids
                            .first()
                            .filter(|oldest| stream_id_less_than(&last_ids[slot], &oldest.id))
                        {
                            error!(
                                "Stream reader for partition {} fell behind the trim horizon on {} (resume {} < oldest {}): messages were lost during the outage; resuming from oldest",
                                partition_id, key, last_ids[slot], oldest.id
                            );
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Stream reader for partition {} could not check trim horizon on {}: {}",
                            partition_id, key, e
                        );
                    }
                }
            }
        }
        first_connect = false;

        loop {
            let options = StreamReadOptions::default()
                .count(XREAD_COUNT)
                .block(XREAD_BLOCK_MS);
            let keys = [&events_key, &commands_key, &requests_key];
            let ids: [&String; 3] = [&last_ids[0], &last_ids[1], &last_ids[2]];

            let reply: StreamReadReply = tokio::select! {
                biased;
                _ = cancellation_token.cancelled() => {
                    info!("Stream reader for partition {} shutting down", partition_id);
                    break 'reconnect;
                }
                result = conn.xread_options::<_, _, StreamReadReply>(&keys, &ids, &options) => {
                    match result {
                        Ok(reply) => reply,
                        Err(e) => {
                            warn!(
                                "Stream reader for partition {} XREAD failed: {}; reconnecting from last IDs",
                                partition_id, e
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(
                                READER_RECONNECT_BACKOFF_MS,
                            ))
                            .await;
                            continue 'reconnect;
                        }
                    }
                }
            };

            for stream in reply.keys {
                for entry in stream.ids {
                    let Some(payload) = entry.map.get("data") else {
                        warn!(
                            "Stream entry {} on {} has no data field; skipping",
                            entry.id, stream.key
                        );
                        continue;
                    };
                    let bytes: Vec<u8> = match redis::from_redis_value(payload) {
                        Ok(bytes) => bytes,
                        Err(e) => {
                            warn!(
                                "Stream entry {} on {} has non-bytes payload: {}; skipping",
                                entry.id, stream.key, e
                            );
                            continue;
                        }
                    };

                    // Forward with backpressure: if the subscriber is slow we
                    // wait (the log holds the backlog) instead of dropping.
                    // A closed receiver means the subscriber is gone, and
                    // cancellation must win even while parked on a full
                    // channel — otherwise shutdown hangs behind a stalled
                    // subscriber.
                    let outcome = if stream.key == events_key {
                        match serde_json::from_slice::<GameEventMessage>(&bytes) {
                            Ok(msg) => forward(&event_tx, msg, &cancellation_token).await,
                            Err(e) => {
                                warn!("Malformed event on {}: {}; skipping", stream.key, e);
                                Forward::Delivered
                            }
                        }
                    } else if stream.key == commands_key {
                        match serde_json::from_slice::<StreamEvent>(&bytes) {
                            Ok(msg) => forward(&command_tx, msg, &cancellation_token).await,
                            Err(e) => {
                                warn!("Malformed command on {}: {}; skipping", stream.key, e);
                                Forward::Delivered
                            }
                        }
                    } else {
                        match serde_json::from_slice::<SnapshotRequest>(&bytes) {
                            Ok(msg) => forward(&request_tx, msg, &cancellation_token).await,
                            Err(e) => {
                                warn!(
                                    "Malformed snapshot request on {}: {}; skipping",
                                    stream.key, e
                                );
                                Forward::Delivered
                            }
                        }
                    };

                    match outcome {
                        Forward::Delivered => {}
                        Forward::SubscriberGone => {
                            info!(
                                "Stream reader for partition {} subscriber dropped; exiting",
                                partition_id
                            );
                            break 'reconnect;
                        }
                        Forward::Cancelled => {
                            info!(
                                "Stream reader for partition {} cancelled during delivery; exiting",
                                partition_id
                            );
                            break 'reconnect;
                        }
                    }

                    let slot = if stream.key == events_key {
                        0
                    } else if stream.key == commands_key {
                        1
                    } else {
                        2
                    };
                    last_ids[slot] = entry.id.clone();
                }
            }
        }
    }
}

enum Forward {
    Delivered,
    SubscriberGone,
    Cancelled,
}

/// Backpressure-aware forward that still honors shutdown.
async fn forward<T>(
    tx: &mpsc::Sender<T>,
    msg: T,
    cancellation_token: &CancellationToken,
) -> Forward {
    tokio::select! {
        biased;
        _ = cancellation_token.cancelled() => Forward::Cancelled,
        result = tx.send(msg) => match result {
            Ok(()) => Forward::Delivered,
            Err(_) => Forward::SubscriberGone,
        },
    }
}

/// Compare two stream entry IDs ("<ms>-<seq>") numerically.
fn stream_id_less_than(a: &str, b: &str) -> bool {
    fn parse(id: &str) -> (u64, u64) {
        let mut parts = id.splitn(2, '-');
        let ms = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        let seq = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        (ms, seq)
    }
    parse(a) < parse(b)
}

#[cfg(test)]
mod tests {
    use super::stream_id_less_than;

    #[test]
    fn stream_id_ordering() {
        assert!(stream_id_less_than("5-1", "5-2"));
        assert!(stream_id_less_than("5-9", "6-0"));
        assert!(stream_id_less_than("0-0", "1-0"));
        // Numeric, not lexicographic: 9-x < 10-x.
        assert!(stream_id_less_than("9-0", "10-0"));
        assert!(!stream_id_less_than("10-0", "9-0"));
        assert!(!stream_id_less_than("5-2", "5-2"));
    }
}
