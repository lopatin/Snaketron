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
//! 4. Fan-out event/request streams remain bounded. Executor commands are
//!    untrimmed at publish time and retire only through the executor consumer
//!    group.

use crate::cluster_membership::ClusterNamespace;
use crate::game_executor::StreamEvent;
use crate::partition_lease::PartitionLeaseGuard;
use crate::recovery::{
    CommandDecisionV1, RECOVERY_FAILURE_SCHEMA_VERSION, RecoveryEnvelopeV2, RecoveryFailureV1,
};
use crate::redis_keys::RedisKeys;
use anyhow::{Context, Result};
use common::{GameEvent, GameEventMessage, GameState};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use redis::streams::{
    StreamAutoClaimReply, StreamId, StreamMaxlen, StreamReadOptions, StreamReadReply,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
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
    pub command_anchor: String,
    pub event_receiver: mpsc::Receiver<GameEventMessage>,
    pub command_receiver: mpsc::Receiver<CommandDelivery>,
    pub snapshot_request_receiver: mpsc::Receiver<SnapshotRequest>,
}

pub struct SnapshotRequestSubscription {
    pub partition_id: u32,
    pub receiver: mpsc::Receiver<SnapshotRequest>,
}

impl PartitionSubscription {
    pub async fn recv_event(&mut self) -> Option<GameEventMessage> {
        self.event_receiver.recv().await
    }

    pub async fn recv_command(&mut self) -> Option<CommandDelivery> {
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
const SNAPREQ_MAXLEN: usize = 64;
/// Poison/rejection entries are operational evidence, not an unbounded
/// correctness log. The terminal outcome is independently durable before ACK.
const COMMAND_QUARANTINE_MAXLEN: usize = 8192;

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
const EXECUTOR_GROUP_BATCH: usize = 512;
const EXECUTOR_GROUP_IDLE: Duration = Duration::from_millis(50);
const FENCED_OPERATION_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(750);

#[derive(Debug, Clone)]
// Stream commands are hot-path values; avoid a heap allocation solely to equalize poison size.
#[allow(clippy::large_enum_variant)]
pub enum CommandDeliveryPayload {
    Command(StreamEvent),
    Poison { raw: Vec<u8>, reason: String },
}

/// A group delivery retains its Redis ID until a fenced checkpoint/terminal
/// disposition ACKs it.
#[derive(Debug, Clone)]
pub struct CommandDelivery {
    pub stream_id: String,
    pub payload: CommandDeliveryPayload,
    pub decision: Option<CommandDecisionV1>,
}

#[derive(Debug, Clone)]
pub struct ReclaimedCommandBatch {
    pub deliveries: Vec<CommandDelivery>,
    pub deleted_pending_ids: Vec<String>,
    pub complete: bool,
}

/// The game-critical transport. All methods take `&self`; internal
/// connections are cheaply cloneable handles.
pub struct GameBus {
    /// Shared non-blocking connection for XADD/KV. Never used for blocking
    /// reads (rule #1).
    redis: ConnectionManager,
    /// Used to open a dedicated connection per reader task.
    redis_client: redis::Client,
    cancellation_token: CancellationToken,
    #[cfg(test)]
    checkpoint_failures_remaining: AtomicUsize,
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
            #[cfg(test)]
            checkpoint_failures_remaining: AtomicUsize::new(0),
        }
    }

    #[cfg(test)]
    pub(crate) fn fail_next_checkpoints(&self, count: usize) {
        self.checkpoint_failures_remaining
            .store(count, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn consume_checkpoint_failure(&self) -> bool {
        self.checkpoint_failures_remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
    }

    async fn xadd(&self, key: &str, maxlen: usize, payload: Vec<u8>) -> Result<()> {
        let mut redis = self.redis.clone();
        let _: String = redis
            .xadd_maxlen(key, StreamMaxlen::Approx(maxlen), "*", &[("data", payload)])
            .await
            .with_context(|| format!("Failed to XADD to {}", key))?;
        Ok(())
    }

    async fn xadd_untrimmed(&self, key: &str, payload: Vec<u8>) -> Result<String> {
        let mut redis = self.redis.clone();
        redis
            .xadd(key, "*", &[("data", payload)])
            .await
            .with_context(|| format!("Failed to XADD to {key}"))
    }

    pub async fn publish_command(&self, partition_id: u32, command: &StreamEvent) -> Result<()> {
        let payload = serde_json::to_vec(command).context("Failed to serialize command")?;
        // Commands are correctness-bearing. Publish-time MAXLEN could trim a
        // pending entry before its checkpoint; cleanup is group-aware instead.
        self.xadd_untrimmed(&RedisKeys::stream_commands(partition_id), payload)
            .await
            .map(|_| ())
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

    /// Idempotently creates the durable executor consumer group at the start
    /// of the stream. Concurrent tasks may race here; `BUSYGROUP` means the
    /// required group already exists and is therefore success.
    pub async fn ensure_executor_command_group(
        &self,
        namespace: &ClusterNamespace,
        partition_id: u32,
    ) -> Result<()> {
        let stream = RedisKeys::stream_commands(partition_id);
        let group = namespace.command_group(partition_id);
        let mut redis = self.redis.clone();
        let mut command = redis::cmd("XGROUP");
        command
            .arg("CREATE")
            .arg(&stream)
            .arg(&group)
            .arg("0-0")
            .arg("MKSTREAM");
        let operation = command.query_async::<()>(&mut redis);
        match tokio::time::timeout(FENCED_OPERATION_TIMEOUT, operation).await {
            Err(error) => Err(error).context("executor command-group creation timed out"),
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) if error.code() == Some("BUSYGROUP") => Ok(()),
            Ok(Err(error)) => Err(error).context("failed to create executor command group"),
        }
    }

    /// Opens the executor-only group reader on its own connection.
    pub async fn subscribe_executor_commands(
        &self,
        guard: PartitionLeaseGuard,
    ) -> Result<ExecutorCommandConsumer> {
        let connection = self
            .redis_client
            .get_multiplexed_async_connection()
            .await
            .context("failed to open dedicated executor command connection")?;
        let group = guard.namespace().command_group(guard.partition());
        Ok(ExecutorCommandConsumer {
            connection,
            stream_key: RedisKeys::stream_commands(guard.partition()),
            group,
            consumer: guard.encoded_token(),
            guard,
            claim_cursor: "0-0".to_string(),
            cancellation: self.cancellation_token.clone(),
        })
    }

    /// Snapshot requests remain fan-out traffic. This reader watches only the
    /// request stream: an executor must never
    /// accidentally tail-read the authoritative command stream.
    pub async fn subscribe_executor_snapshot_requests(
        &self,
        partition_id: u32,
    ) -> Result<SnapshotRequestSubscription> {
        let key = RedisKeys::stream_snapshot_requests(partition_id);
        let mut redis = self.redis.clone();
        let anchor = resolve_stream_anchor(&mut redis, &key).await?;
        let (sender, receiver) = mpsc::channel(SUBSCRIBER_CHANNEL_CAPACITY);
        let client = self.redis_client.clone();
        let cancellation = self.cancellation_token.clone();
        tokio::spawn(async move {
            snapshot_request_reader(client, key, anchor, sender, cancellation).await;
        });
        Ok(SnapshotRequestSubscription {
            partition_id,
            receiver,
        })
    }

    pub async fn publish_event_fenced(
        &self,
        guard: &PartitionLeaseGuard,
        event: &GameEventMessage,
    ) -> Result<String> {
        if event.game_id % crate::game_executor::PARTITION_COUNT != guard.partition() {
            anyhow::bail!("event game does not belong to fenced partition");
        }
        let payload = serde_json::to_vec(event).context("failed to serialize fenced event")?;
        let mut redis = self.redis.clone();
        let script = redis::Script::new(
            r#"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return {0, ''} end
            local id = redis.call(
                'XADD', KEYS[2], 'MAXLEN', '~', ARGV[2], '*', 'data', ARGV[3]
            )
            return {1, id}
            "#,
        );
        let mut invocation = script.prepare_invoke();
        invocation
            .key(guard.lease_key())
            .key(RedisKeys::stream_events(guard.partition()))
            .arg(guard.encoded_token())
            .arg(EVENTS_MAXLEN)
            .arg(payload);
        let operation = invocation.invoke_async(&mut redis);
        let (code, stream_id): (i32, String) =
            tokio::time::timeout(FENCED_OPERATION_TIMEOUT, operation)
                .await
                .context("fenced event publication timed out")?
                .context("failed to publish fenced game event")?;
        if code != 1 {
            crate::resilience_metrics::record_fenced_write_rejection(1);
            anyhow::bail!("stale partition lease rejected event publication");
        }
        Ok(stream_id)
    }

    /// Durably records the exact command decision before publishing its
    /// client-visible event. Retrying an ambiguously completed call with the
    /// same bytes is a no-op; a conflicting decision for one stream entry is a
    /// hard invariant failure.
    pub async fn publish_command_decision_fenced(
        &self,
        guard: &PartitionLeaseGuard,
        decision: &CommandDecisionV1,
    ) -> Result<()> {
        decision.validate()?;
        if decision.event.game_id % crate::game_executor::PARTITION_COUNT != guard.partition() {
            anyhow::bail!("command decision game does not belong to fenced partition");
        }
        let decision_payload = serde_json::to_vec(decision)?;
        let event_payload = serde_json::to_vec(&decision.event)?;
        let mut redis = self.redis.clone();
        let script = redis::Script::new(
            r#"
            local journal_type = redis.call('TYPE', KEYS[2])
            if type(journal_type) == 'table' then journal_type = journal_type.ok end
            if journal_type ~= 'none' and journal_type ~= 'hash' then return -3 end
            local event_type = redis.call('TYPE', KEYS[3])
            if type(event_type) == 'table' then event_type = event_type.ok end
            if event_type ~= 'none' and event_type ~= 'stream' then return -3 end
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
            local current = redis.call('HGET', KEYS[2], ARGV[2])
            if current then
                if current ~= ARGV[3] then return -2 end
                return 0
            end
            redis.call('HSET', KEYS[2], ARGV[2], ARGV[3])
            redis.call(
                'XADD', KEYS[3], 'MAXLEN', '~', ARGV[4], '*', 'data', ARGV[5]
            )
            return 1
            "#,
        );
        let mut invocation = script.prepare_invoke();
        invocation
            .key(guard.lease_key())
            .key(guard.namespace().command_decisions(guard.partition()))
            .key(RedisKeys::stream_events(guard.partition()))
            .arg(guard.encoded_token())
            .arg(&decision.source_stream_id)
            .arg(decision_payload)
            .arg(EVENTS_MAXLEN)
            .arg(event_payload);
        let result: i32 = tokio::time::timeout(
            FENCED_OPERATION_TIMEOUT,
            invocation.invoke_async(&mut redis),
        )
        .await
        .context("fenced command decision publication timed out")??;
        match result {
            0 | 1 => Ok(()),
            -1 => {
                crate::resilience_metrics::record_fenced_write_rejection(1);
                anyhow::bail!("stale partition lease rejected command decision")
            }
            -2 => anyhow::bail!("command stream entry has a conflicting durable decision"),
            -3 => anyhow::bail!("command decision publication found a Redis key with wrong type"),
            other => anyhow::bail!("unknown command decision publication result {other}"),
        }
    }

    /// Batch-loads the partition's write-ahead decisions once during takeover
    /// under the live token. Missing fields are normal: those commands had no
    /// client-visible decision before the prior executor stopped.
    pub async fn load_command_decisions_fenced(
        &self,
        guard: &PartitionLeaseGuard,
    ) -> Result<HashMap<String, CommandDecisionV1>> {
        let mut redis = self.redis.clone();
        let script = redis::Script::new(
            r#"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return {0, {}} end
            local journal_type = redis.call('TYPE', KEYS[2])
            if type(journal_type) == 'table' then journal_type = journal_type.ok end
            if journal_type ~= 'none' and journal_type ~= 'hash' then return {-1, {}} end
            return {1, redis.call('HGETALL', KEYS[2])}
            "#,
        );
        let mut invocation = script.prepare_invoke();
        invocation
            .key(guard.lease_key())
            .key(guard.namespace().command_decisions(guard.partition()))
            .arg(guard.encoded_token());
        let (authority, payloads): (i32, HashMap<String, Vec<u8>>) = tokio::time::timeout(
            FENCED_OPERATION_TIMEOUT,
            invocation.invoke_async(&mut redis),
        )
        .await
        .context("fenced command decision batch load timed out")??;
        match authority {
            1 => {}
            0 => anyhow::bail!("partition lease authority was lost before decision batch load"),
            -1 => anyhow::bail!("command decision journal has the wrong Redis type"),
            other => anyhow::bail!("unknown command decision batch-load result {other}"),
        }
        let mut decisions = HashMap::with_capacity(payloads.len());
        for (stream_id, payload) in payloads {
            crate::recovery::validate_stream_id(&stream_id)?;
            let decision: CommandDecisionV1 =
                serde_json::from_slice(&payload).context("malformed durable command decision")?;
            decision.validate()?;
            if decision.source_stream_id != stream_id
                || decision.event.game_id % crate::game_executor::PARTITION_COUNT
                    != guard.partition()
            {
                anyhow::bail!("durable command decision identity mismatch");
            }
            decisions.insert(stream_id, decision);
        }
        Ok(decisions)
    }

    pub async fn publish_command_fenced(
        &self,
        guard: &PartitionLeaseGuard,
        command: &StreamEvent,
    ) -> Result<String> {
        let payload = serde_json::to_vec(command)?;
        let mut redis = self.redis.clone();
        let script = redis::Script::new(
            r#"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return {0, ''} end
            local id = redis.call('XADD', KEYS[2], '*', 'data', ARGV[2])
            return {1, id}
            "#,
        );
        let mut invocation = script.prepare_invoke();
        invocation
            .key(guard.lease_key())
            .key(RedisKeys::stream_commands(guard.partition()))
            .arg(guard.encoded_token())
            .arg(payload);
        let operation = invocation.invoke_async(&mut redis);
        let (code, stream_id): (i32, String) =
            tokio::time::timeout(FENCED_OPERATION_TIMEOUT, operation)
                .await
                .context("fenced command publication timed out")??;
        if code != 1 {
            crate::resilience_metrics::record_fenced_write_rejection(1);
            anyhow::bail!("stale partition lease rejected executor command publication");
        }
        Ok(stream_id)
    }

    /// Atomically persists the recovery envelope, refreshes the reload
    /// snapshot, indexes the game, and ACKs exactly the command entries covered
    /// by that envelope.
    pub async fn checkpoint_and_ack_fenced(
        &self,
        guard: &PartitionLeaseGuard,
        envelope: &RecoveryEnvelopeV2,
        covered_stream_ids: &[String],
        retention: std::time::Duration,
    ) -> Result<usize> {
        envelope.validate()?;
        if envelope.partition_id != guard.partition()
            || envelope.game_id % crate::game_executor::PARTITION_COUNT != guard.partition()
        {
            anyhow::bail!("recovery envelope does not belong to fenced partition");
        }
        if envelope.source_lease_token != guard.encoded_token() {
            anyhow::bail!("recovery envelope source token is not the live guard");
        }
        for id in covered_stream_ids {
            crate::recovery::validate_stream_id(id)?;
        }

        #[cfg(test)]
        if self.consume_checkpoint_failure() {
            return Err(redis::RedisError::from((
                redis::ErrorKind::IoError,
                "injected checkpoint write failure",
            ))
            .into());
        }

        let recovery_payload = serde_json::to_vec(envelope)?;
        let snapshot_payload = serde_json::to_vec(&envelope.game_state)?;
        let script = redis::Script::new(
            r#"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
            local journal_type = redis.call('TYPE', KEYS[6])
            if type(journal_type) == 'table' then journal_type = journal_type.ok end
            if journal_type ~= 'none' and journal_type ~= 'hash' then return -2 end
            redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[3])
            redis.call('SET', KEYS[3], ARGV[4], 'PX', ARGV[3])
            redis.call('SADD', KEYS[4], ARGV[5])
            if #ARGV == 6 then return 0 end
            local ids = {}
            for i = 7, #ARGV do ids[#ids + 1] = ARGV[i] end
            local acked = redis.call('XACK', KEYS[5], ARGV[6], unpack(ids))
            redis.call('HDEL', KEYS[6], unpack(ids))
            return acked
            "#,
        );
        let mut invocation = script.prepare_invoke();
        invocation
            .key(guard.lease_key())
            .key(guard.namespace().recovery(envelope.game_id))
            .key(RedisKeys::game_snapshot(envelope.game_id))
            .key(guard.namespace().active_games(guard.partition()))
            .key(RedisKeys::stream_commands(guard.partition()))
            .key(guard.namespace().command_decisions(guard.partition()))
            .arg(guard.encoded_token())
            .arg(recovery_payload)
            .arg(retention.as_millis() as u64)
            .arg(snapshot_payload)
            .arg(envelope.game_id)
            .arg(guard.namespace().command_group(guard.partition()));
        for id in covered_stream_ids {
            invocation.arg(id);
        }
        let mut redis = self.redis.clone();
        let acked: i64 = tokio::time::timeout(
            FENCED_OPERATION_TIMEOUT,
            invocation.invoke_async(&mut redis),
        )
        .await
        .context("fenced recovery checkpoint timed out")?
        .context("failed to persist fenced recovery checkpoint")?;
        match acked {
            -1 => {
                crate::resilience_metrics::record_fenced_write_rejection(1);
                anyhow::bail!("stale partition lease rejected checkpoint/ACK");
            }
            -2 => anyhow::bail!("checkpoint/ACK found a command journal with wrong type"),
            _ => {}
        }
        crate::resilience_metrics::record_command_acks(acked as u64);
        Ok(acked as usize)
    }

    /// Extends the lifetime of an existing recovery checkpoint without
    /// changing its state/cursor or acknowledging any command work. Terminal-
    /// pending actors use this while the completion record is waiting on a
    /// transient database read. Missing keys are an error: this operation must
    /// never manufacture an incomplete recovery source.
    pub async fn refresh_recovery_ttl_fenced(
        &self,
        guard: &PartitionLeaseGuard,
        game_id: u32,
        retention: std::time::Duration,
    ) -> Result<()> {
        if game_id % crate::game_executor::PARTITION_COUNT != guard.partition() {
            anyhow::bail!("recovery TTL refresh game does not belong to fenced partition");
        }
        let retention_ms = u64::try_from(retention.as_millis())
            .context("recovery TTL refresh retention exceeds Redis range")?;
        if retention_ms == 0 {
            anyhow::bail!("recovery TTL refresh retention must be non-zero");
        }

        let mut redis = self.redis.clone();
        let script = redis::Script::new(
            r#"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
            if redis.call('EXISTS', KEYS[2]) ~= 1 or
               redis.call('EXISTS', KEYS[3]) ~= 1 then
                return -2
            end
            redis.call('PEXPIRE', KEYS[2], ARGV[2])
            redis.call('PEXPIRE', KEYS[3], ARGV[2])
            return 1
            "#,
        );
        let mut invocation = script.prepare_invoke();
        invocation
            .key(guard.lease_key())
            .key(guard.namespace().recovery(game_id))
            .key(RedisKeys::game_snapshot(game_id))
            .arg(guard.encoded_token())
            .arg(retention_ms);
        let operation = invocation.invoke_async(&mut redis);
        let result: i32 = tokio::time::timeout(FENCED_OPERATION_TIMEOUT, operation)
            .await
            .context("fenced recovery TTL refresh timed out")??;
        match result {
            1 => Ok(()),
            -1 => {
                crate::resilience_metrics::record_fenced_write_rejection(1);
                anyhow::bail!("stale partition lease rejected recovery TTL refresh")
            }
            -2 => anyhow::bail!("recovery TTL refresh found a missing durable checkpoint key"),
            other => anyhow::bail!("unknown recovery TTL refresh result {other}"),
        }
    }

    pub async fn xack_fenced(
        &self,
        guard: &PartitionLeaseGuard,
        stream_ids: &[String],
    ) -> Result<usize> {
        if stream_ids.is_empty() {
            return Ok(0);
        }
        let script = redis::Script::new(
            r#"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
            local journal_type = redis.call('TYPE', KEYS[3])
            if type(journal_type) == 'table' then journal_type = journal_type.ok end
            if journal_type ~= 'none' and journal_type ~= 'hash' then return -2 end
            local ids = {}
            for i = 3, #ARGV do ids[#ids + 1] = ARGV[i] end
            local acked = redis.call('XACK', KEYS[2], ARGV[2], unpack(ids))
            redis.call('HDEL', KEYS[3], unpack(ids))
            return acked
            "#,
        );
        let mut invocation = script.prepare_invoke();
        invocation
            .key(guard.lease_key())
            .key(RedisKeys::stream_commands(guard.partition()))
            .key(guard.namespace().command_decisions(guard.partition()))
            .arg(guard.encoded_token())
            .arg(guard.namespace().command_group(guard.partition()));
        for id in stream_ids {
            crate::recovery::validate_stream_id(id)?;
            invocation.arg(id);
        }
        let mut redis = self.redis.clone();
        let acked: i64 = tokio::time::timeout(
            FENCED_OPERATION_TIMEOUT,
            invocation.invoke_async(&mut redis),
        )
        .await
        .context("fenced command ACK timed out")??;
        match acked {
            -1 => {
                crate::resilience_metrics::record_fenced_write_rejection(1);
                anyhow::bail!("stale partition lease rejected command ACK");
            }
            -2 => anyhow::bail!("command ACK found a decision journal with wrong type"),
            _ => {}
        }
        crate::resilience_metrics::record_command_acks(acked as u64);
        Ok(acked as usize)
    }

    /// Retires only entries strictly older than the group's oldest pending
    /// item (or its last-delivered ID when the PEL is empty). This keeps every
    /// unacknowledged takeover candidate and every not-yet-delivered command;
    /// publishers never apply an independent MAXLEN policy.
    pub async fn trim_executor_commands_fenced(
        &self,
        guard: &PartitionLeaseGuard,
    ) -> Result<usize> {
        let mut redis = self.redis.clone();
        let operation = async {
            let script = redis::Script::new(
                r#"
                if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
                local pending = redis.call('XPENDING', KEYS[2], ARGV[2], '-', '+', 1)
                local threshold = nil
                if #pending > 0 then
                    threshold = pending[1][1]
                else
                    local groups = redis.call('XINFO', 'GROUPS', KEYS[2])
                    for _, group in ipairs(groups) do
                        local name = nil
                        local delivered = nil
                        for i = 1, #group, 2 do
                            if group[i] == 'name' then name = group[i + 1] end
                            if group[i] == 'last-delivered-id' then delivered = group[i + 1] end
                        end
                        if name == ARGV[2] then
                            threshold = delivered
                            break
                        end
                    end
                end
                if not threshold or threshold == '0-0' then return 0 end
                return redis.call('XTRIM', KEYS[2], 'MINID', threshold)
                "#,
            );
            script
                .key(guard.lease_key())
                .key(RedisKeys::stream_commands(guard.partition()))
                .arg(guard.encoded_token())
                .arg(guard.namespace().command_group(guard.partition()))
                .invoke_async(&mut redis)
                .await
        };
        let trimmed: i64 = tokio::time::timeout(FENCED_OPERATION_TIMEOUT, operation)
            .await
            .context("group-aware command trim timed out")??;
        if trimmed < 0 {
            crate::resilience_metrics::record_fenced_write_rejection(1);
            anyhow::bail!("stale partition lease rejected command trim");
        }
        Ok(trimmed as usize)
    }

    /// A malformed entry is never silently skipped: quarantine and ACK happen
    /// together under the live token.
    pub async fn quarantine_and_ack_fenced(
        &self,
        guard: &PartitionLeaseGuard,
        stream_id: &str,
        raw: &[u8],
        reason: &str,
    ) -> Result<()> {
        crate::recovery::validate_stream_id(stream_id)?;
        let mut redis = self.redis.clone();
        let operation = async {
            let script = redis::Script::new(
                r#"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
            local journal_type = redis.call('TYPE', KEYS[4])
            if type(journal_type) == 'table' then journal_type = journal_type.ok end
            if journal_type ~= 'none' and journal_type ~= 'hash' then return -2 end
            redis.call(
                'XADD', KEYS[2], 'MAXLEN', '~', ARGV[6], '*',
                'source_id', ARGV[2], 'raw', ARGV[3], 'reason', ARGV[4]
            )
            redis.call('XACK', KEYS[3], ARGV[5], ARGV[2])
            redis.call('HDEL', KEYS[4], ARGV[2])
            return 1
                "#,
            );
            script
                .key(guard.lease_key())
                .key(guard.namespace().command_quarantine(guard.partition()))
                .key(RedisKeys::stream_commands(guard.partition()))
                .key(guard.namespace().command_decisions(guard.partition()))
                .arg(guard.encoded_token())
                .arg(stream_id)
                .arg(raw)
                .arg(reason)
                .arg(guard.namespace().command_group(guard.partition()))
                .arg(COMMAND_QUARANTINE_MAXLEN)
                .invoke_async(&mut redis)
                .await
        };
        let result: i32 = tokio::time::timeout(FENCED_OPERATION_TIMEOUT, operation)
            .await
            .context("fenced poison disposition timed out")??;
        match result {
            1 => {}
            -1 => {
                crate::resilience_metrics::record_fenced_write_rejection(1);
                anyhow::bail!("stale partition lease rejected poison disposition");
            }
            -2 => anyhow::bail!("poison disposition found a decision journal with wrong type"),
            other => anyhow::bail!("unknown poison disposition result {other}"),
        }
        Ok(())
    }

    /// Durably resolves a syntactically valid v2 command that cannot be
    /// incorporated by an actor. The client-visible rejection, diagnostic
    /// quarantine entry, and consumer-group ACK share one fenced transaction,
    /// so an executor crash can never retire the command without first making
    /// its terminal outcome replayable from the event stream.
    pub async fn reject_and_ack_fenced(
        &self,
        guard: &PartitionLeaseGuard,
        stream_id: &str,
        event: &GameEventMessage,
        reason: &str,
    ) -> Result<String> {
        crate::recovery::validate_stream_id(stream_id)?;
        let GameEvent::CommandRejected {
            command_id,
            reason: event_reason,
        } = &event.event
        else {
            anyhow::bail!("replyable command disposition requires CommandRejected");
        };
        crate::recovery::validate_client_command_identity(command_id)?;
        if event.game_id % crate::game_executor::PARTITION_COUNT != guard.partition()
            || command_id.game_id != event.game_id
            || event.user_id != Some(command_id.user_id)
            || event_reason != reason
        {
            anyhow::bail!("replyable command rejection does not match fenced delivery");
        }

        let payload = serde_json::to_vec(event).context("failed to serialize command rejection")?;
        let mut redis = self.redis.clone();
        let operation = async {
            let script = redis::Script::new(
                r#"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return {-1, ''} end
            local journal_type = redis.call('TYPE', KEYS[5])
            if type(journal_type) == 'table' then journal_type = journal_type.ok end
            if journal_type ~= 'none' and journal_type ~= 'hash' then return {-2, ''} end
            redis.call(
                'XADD', KEYS[2], 'MAXLEN', '~', ARGV[7], '*',
                'source_id', ARGV[2], 'raw', '', 'reason', ARGV[3]
            )
            local event_id = redis.call(
                'XADD', KEYS[4], 'MAXLEN', '~', ARGV[5], '*', 'data', ARGV[6]
            )
            redis.call('XACK', KEYS[3], ARGV[4], ARGV[2])
            redis.call('HDEL', KEYS[5], ARGV[2])
            return {1, event_id}
                "#,
            );
            script
                .key(guard.lease_key())
                .key(guard.namespace().command_quarantine(guard.partition()))
                .key(RedisKeys::stream_commands(guard.partition()))
                .key(RedisKeys::stream_events(guard.partition()))
                .key(guard.namespace().command_decisions(guard.partition()))
                .arg(guard.encoded_token())
                .arg(stream_id)
                .arg(reason)
                .arg(guard.namespace().command_group(guard.partition()))
                .arg(EVENTS_MAXLEN)
                .arg(payload)
                .arg(COMMAND_QUARANTINE_MAXLEN)
                .invoke_async(&mut redis)
                .await
        };
        let (result, event_id): (i32, String) =
            tokio::time::timeout(FENCED_OPERATION_TIMEOUT, operation)
                .await
                .context("fenced command rejection timed out")??;
        match result {
            1 => {}
            -1 => {
                crate::resilience_metrics::record_fenced_write_rejection(1);
                anyhow::bail!("stale partition lease rejected command rejection");
            }
            -2 => anyhow::bail!("command rejection found a decision journal with wrong type"),
            other => anyhow::bail!("unknown command rejection result {other}"),
        }
        crate::resilience_metrics::record_command_acks(1);
        Ok(event_id)
    }

    pub async fn load_partition_recovery_fenced(
        &self,
        guard: &PartitionLeaseGuard,
        retention: std::time::Duration,
    ) -> Result<Vec<RecoveryEnvelopeV2>> {
        let retention_ms = u64::try_from(retention.as_millis())
            .context("recovery-failure retention exceeds Redis range")?;
        if retention_ms == 0 {
            anyhow::bail!("recovery-failure retention must be non-zero");
        }
        let namespace = guard.namespace();
        let partition = guard.partition();
        let mut redis = self.redis.clone();
        let game_ids: Vec<u32> = redis
            .smembers(namespace.active_games(partition))
            .await
            .context("failed to load partition active-game index")?;
        if game_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut pipe = redis::pipe();
        for game_id in &game_ids {
            pipe.get(namespace.recovery(*game_id));
        }
        let payloads: Vec<Option<Vec<u8>>> = pipe
            .query_async(&mut redis)
            .await
            .context("failed to batch-load partition recovery envelopes")?;
        let mut envelopes = Vec::with_capacity(game_ids.len());
        for (game_id, payload) in game_ids.into_iter().zip(payloads) {
            let parsed = (|| -> Result<RecoveryEnvelopeV2> {
                let payload = payload.ok_or_else(|| {
                    anyhow::anyhow!(
                        "active game {game_id} in partition {partition} has no recovery envelope"
                    )
                })?;
                let envelope: RecoveryEnvelopeV2 = serde_json::from_slice(&payload)
                    .with_context(|| format!("malformed recovery envelope for game {game_id}"))?;
                envelope.validate()?;
                if envelope.game_id != game_id || envelope.partition_id != partition {
                    anyhow::bail!("recovery envelope/index identity mismatch for game {game_id}");
                }
                Ok(envelope)
            })();
            match parsed {
                Ok(envelope) => envelopes.push(envelope),
                Err(error) => {
                    self.mark_recovery_unrecoverable_fenced(
                        guard,
                        game_id,
                        &error.to_string(),
                        retention_ms,
                    )
                    .await?;
                    warn!(
                        partition,
                        game_id,
                        %error,
                        "Removed one unrecoverable game from the active index"
                    );
                }
            }
        }
        envelopes.sort_by_key(|envelope| envelope.game_id);
        Ok(envelopes)
    }

    async fn mark_recovery_unrecoverable_fenced(
        &self,
        guard: &PartitionLeaseGuard,
        game_id: u32,
        diagnostic: &str,
        retention_ms: u64,
    ) -> Result<()> {
        let failure = RecoveryFailureV1 {
            schema_version: RECOVERY_FAILURE_SCHEMA_VERSION,
            game_id,
            partition_id: guard.partition(),
            detected_at_ms: chrono::Utc::now().timestamp_millis(),
            diagnostic: diagnostic.to_owned(),
        };
        failure.validate()?;
        let failure_payload = serde_json::to_vec(&failure)?;
        let mut redis = self.redis.clone();
        let operation = async {
            redis::Script::new(
                r#"
                if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
                if not redis.call('GET', KEYS[3]) then
                    redis.call('SET', KEYS[3], ARGV[3], 'PX', ARGV[4])
                end
                redis.call('SREM', KEYS[2], ARGV[2])
                return 1
                "#,
            )
            .key(guard.lease_key())
            .key(guard.namespace().active_games(guard.partition()))
            .key(guard.namespace().recovery_failure(game_id))
            .arg(guard.encoded_token())
            .arg(game_id)
            .arg(&failure_payload)
            .arg(retention_ms)
            .invoke_async(&mut redis)
            .await
        };
        let result: i32 = tokio::time::timeout(FENCED_OPERATION_TIMEOUT, operation)
            .await
            .context("fenced unrecoverable-game disposition timed out")??;
        if result != 1 {
            crate::resilience_metrics::record_fenced_write_rejection(1);
            anyhow::bail!("stale partition lease rejected unrecoverable-game disposition");
        }
        Ok(())
    }

    pub async fn get_recovery_failure(
        &self,
        namespace: &ClusterNamespace,
        game_id: u32,
    ) -> Result<Option<RecoveryFailureV1>> {
        let mut redis = self.redis.clone();
        let payload: Option<Vec<u8>> = redis
            .get(namespace.recovery_failure(game_id))
            .await
            .context("failed to load game recovery-failure marker")?;
        payload
            .map(|payload| {
                let marker: RecoveryFailureV1 = serde_json::from_slice(&payload)
                    .context("malformed game recovery-failure marker")?;
                marker.validate()?;
                if marker.game_id != game_id {
                    anyhow::bail!("recovery-failure marker game identity mismatch");
                }
                Ok(marker)
            })
            .transpose()
    }

    pub async fn get_recovery(
        &self,
        namespace: &ClusterNamespace,
        game_id: u32,
    ) -> Result<Option<RecoveryEnvelopeV2>> {
        let mut redis = self.redis.clone();
        let payload: Option<Vec<u8>> = redis
            .get(namespace.recovery(game_id))
            .await
            .context("failed to load game recovery envelope")?;
        payload
            .map(|payload| {
                let envelope: RecoveryEnvelopeV2 =
                    serde_json::from_slice(&payload).context("malformed game recovery envelope")?;
                envelope.validate()?;
                if envelope.game_id != game_id {
                    anyhow::bail!("recovery envelope game identity mismatch");
                }
                Ok(envelope)
            })
            .transpose()
    }

    pub async fn remove_active_game_fenced(
        &self,
        guard: &PartitionLeaseGuard,
        game_id: u32,
    ) -> Result<bool> {
        let mut redis = self.redis.clone();
        let operation = async {
            let script = redis::Script::new(
                r#"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
            return redis.call('SREM', KEYS[2], ARGV[2])
                "#,
            );
            script
                .key(guard.lease_key())
                .key(guard.namespace().active_games(guard.partition()))
                .arg(guard.encoded_token())
                .arg(game_id)
                .invoke_async(&mut redis)
                .await
        };
        let result: i32 = tokio::time::timeout(FENCED_OPERATION_TIMEOUT, operation)
            .await
            .context("fenced active-game removal timed out")??;
        if result < 0 {
            crate::resilience_metrics::record_fenced_write_rejection(1);
            anyhow::bail!("stale partition lease rejected active-game removal");
        }
        Ok(result == 1)
    }

    /// Generic immutable fenced record plus pending-index mutation used by
    /// completion/future executor effects. `SET NX` makes ambiguous retries
    /// converge without permitting a different second record.
    pub async fn put_immutable_pending_fenced(
        &self,
        guard: &PartitionLeaseGuard,
        record_key: &str,
        pending_index_key: &str,
        pending_member: &str,
        payload: &[u8],
    ) -> Result<bool> {
        let mut redis = self.redis.clone();
        let operation = async {
            let script = redis::Script::new(
                r#"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
            local current = redis.call('GET', KEYS[2])
            if current then
                if current ~= ARGV[3] then return -2 end
                redis.call('SADD', KEYS[3], ARGV[2])
                return 0
            end
            redis.call('SET', KEYS[2], ARGV[3])
            redis.call('SADD', KEYS[3], ARGV[2])
            return 1
                "#,
            );
            script
                .key(guard.lease_key())
                .key(record_key)
                .key(pending_index_key)
                .arg(guard.encoded_token())
                .arg(pending_member)
                .arg(payload)
                .invoke_async(&mut redis)
                .await
        };
        let result: i32 = tokio::time::timeout(FENCED_OPERATION_TIMEOUT, operation)
            .await
            .context("fenced immutable record timed out")??;
        match result {
            1 => Ok(true),
            0 => Ok(false),
            -1 => {
                crate::resilience_metrics::record_fenced_write_rejection(1);
                anyhow::bail!("stale partition lease rejected immutable record")
            }
            -2 => anyhow::bail!("immutable executor record conflicts with existing payload"),
            other => anyhow::bail!("unknown immutable record result {other}"),
        }
    }

    /// Establishes the immutable completion record, final checkpoint and
    /// pending-effect index in one fenced transaction before DynamoDB effects.
    pub async fn commit_completion_record_fenced(
        &self,
        guard: &PartitionLeaseGuard,
        envelope: &RecoveryEnvelopeV2,
        covered_stream_ids: &[String],
        record: &crate::completion::CompletionRecordV1,
        retention: std::time::Duration,
    ) -> Result<bool> {
        envelope.validate()?;
        record.validate()?;
        if record.game_id != envelope.game_id
            || record.partition_id != guard.partition()
            || envelope.partition_id != guard.partition()
            || envelope.source_lease_token != guard.encoded_token()
        {
            anyhow::bail!("completion/checkpoint identity does not match fenced partition");
        }
        let retention_ms = u64::try_from(retention.as_millis())
            .context("completion checkpoint retention exceeds Redis range")?;
        if retention_ms == 0 {
            anyhow::bail!("completion checkpoint retention must be non-zero");
        }
        let record_payload = crate::completion::canonical_json_bytes(record)?;
        let recovery_payload = serde_json::to_vec(envelope)?;
        let snapshot_payload = serde_json::to_vec(&envelope.game_state)?;
        let terminal_status_payload = serde_json::to_vec(&StreamEvent::StatusUpdated {
            game_id: record.game_id,
            status: record.final_state.status.clone(),
        })?;
        let terminal_stream_seq = envelope
            .next_event_stream_sequence
            .checked_add(1)
            .context("terminal snapshot stream sequence overflow")?;
        let terminal_snapshot_payload = serde_json::to_vec(&GameEventMessage {
            game_id: record.game_id,
            tick: record.final_state.tick,
            sequence: record.final_state.event_sequence,
            stream_seq: terminal_stream_seq,
            user_id: None,
            event: GameEvent::Snapshot {
                game_state: record.final_state.clone(),
            },
        })?;
        let script = redis::Script::new(
            r#"
            local function key_type(key)
                local value = redis.call('TYPE', key)
                if type(value) == 'table' then return value.ok end
                return value
            end
            local function require_type(key, expected)
                local actual = key_type(key)
                return actual == 'none' or actual == expected
            end

            if not require_type(KEYS[1], 'string') then return -3 end
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
            if not require_type(KEYS[2], 'string') then return -3 end
            local current = redis.call('GET', KEYS[2])
            local created = 0
            if current then
                if current ~= ARGV[2] then return -2 end
            else
                created = 1
            end

            local record_ok, decoded_record = pcall(cjson.decode, ARGV[2])
            if not record_ok then return -4 end
            if not require_type(KEYS[5], 'set') or
               not require_type(KEYS[6], 'set') or
               not require_type(KEYS[7], 'stream') or
               not require_type(KEYS[8], 'hash') or
               not require_type(KEYS[9], 'string') or
               not require_type(KEYS[10], 'stream') or
               not require_type(KEYS[11], 'hash') then
                return -3
            end

            local active_json = redis.call('HGET', KEYS[8], ARGV[6])
            local active_match = nil
            if active_json then
                local active_ok
                active_ok, active_match = pcall(cjson.decode, active_json)
                if not active_ok then return -4 end
            end

            local cleanup_keys = {}
            local seen = {}
            local function add_cleanup_key(key)
                if not seen[key] then
                    seen[key] = true
                    cleanup_keys[#cleanup_keys + 1] = key
                end
            end
            local function add_user(user_id)
                if user_id ~= nil then
                    add_cleanup_key(ARGV[10] .. tostring(user_id) .. ARGV[12])
                end
            end
            local final_state = decoded_record.final_state or {}
            for user_id, _ in pairs(final_state.players or {}) do add_user(user_id) end
            for _, user_id in ipairs(final_state.spectators or {}) do add_user(user_id) end
            if active_match then
                for _, player in ipairs(active_match.players or {}) do add_user(player.user_id) end
                for _, player in ipairs(active_match.spectators or {}) do add_user(player.user_id) end
                for _, lobby_code in ipairs(active_match.lobby_codes or {}) do
                    if type(lobby_code) ~= 'string' then return -4 end
                    add_cleanup_key(ARGV[11] .. lobby_code .. ARGV[12])
                end
            end

            local delete_keys = {}
            for _, key in ipairs(cleanup_keys) do
                if not require_type(key, 'string') then return -3 end
                if redis.call('GET', key) == ARGV[6] then
                    delete_keys[#delete_keys + 1] = key
                end
            end

            local notified = redis.call('GET', KEYS[9])
            if notified and notified ~= ARGV[14] then return -2 end

            if created == 1 then redis.call('SET', KEYS[2], ARGV[2]) end
            redis.call('SET', KEYS[3], ARGV[3], 'PX', ARGV[4])
            redis.call('SET', KEYS[4], ARGV[5], 'PX', ARGV[4])
            redis.call('SREM', KEYS[5], ARGV[6])
            redis.call('SADD', KEYS[6], ARGV[6])
            for _, key in ipairs(delete_keys) do redis.call('DEL', key) end
            if active_json then redis.call('HDEL', KEYS[8], ARGV[6]) end

            if not notified then
                redis.call('XADD', KEYS[10], 'MAXLEN', '~', ARGV[13], '*', 'data', ARGV[9])
                redis.call('XADD', KEYS[7], '*', 'data', ARGV[8])
                redis.call('SET', KEYS[9], ARGV[14])
            end
            -- Retire covered work only after every authoritative record and
            -- terminal publication above has succeeded. Valkey rejects a
            -- noeviction OOM before a script's first write, but scripts still
            -- do not roll back earlier commands after other runtime errors;
            -- acknowledging last preserves the recoverable completion trigger.
            if #ARGV >= 15 then
                local ids = {}
                for i = 15, #ARGV do ids[#ids + 1] = ARGV[i] end
                redis.call('XACK', KEYS[7], ARGV[7], unpack(ids))
                redis.call('HDEL', KEYS[11], unpack(ids))
            end
            return created
            "#,
        );
        let mut invocation = script.prepare_invoke();
        invocation
            .key(guard.lease_key())
            .key(guard.namespace().completion(record.game_id))
            .key(guard.namespace().recovery(record.game_id))
            .key(RedisKeys::game_snapshot(record.game_id))
            .key(guard.namespace().active_games(guard.partition()))
            .key(guard.namespace().pending_completions(guard.partition()))
            .key(RedisKeys::stream_commands(guard.partition()))
            .key(RedisKeys::matchmaking_active_matches())
            .key(
                guard
                    .namespace()
                    .completion_terminal_notified(record.game_id),
            )
            .key(RedisKeys::stream_events(guard.partition()))
            .key(guard.namespace().command_decisions(guard.partition()))
            .arg(guard.encoded_token())
            .arg(record_payload)
            .arg(recovery_payload)
            .arg(retention_ms)
            .arg(snapshot_payload)
            .arg(record.game_id)
            .arg(guard.namespace().command_group(guard.partition()))
            .arg(terminal_status_payload)
            .arg(terminal_snapshot_payload)
            .arg(RedisKeys::MATCHMAKING_USER_ACTIVE_GAME_PREFIX)
            .arg(RedisKeys::MATCHMAKING_LOBBY_ACTIVE_GAME_PREFIX)
            .arg(RedisKeys::MATCHMAKING_ACTIVE_GAME_SUFFIX)
            .arg(EVENTS_MAXLEN)
            .arg(record.revision.to_string());
        for id in covered_stream_ids {
            crate::recovery::validate_stream_id(id)?;
            invocation.arg(id);
        }
        let mut redis = self.redis.clone();
        let result: i32 = tokio::time::timeout(
            FENCED_OPERATION_TIMEOUT,
            invocation.invoke_async(&mut redis),
        )
        .await
        .context("fenced completion commit timed out")??;
        match result {
            1 => Ok(true),
            0 => Ok(false),
            -1 => {
                crate::resilience_metrics::record_fenced_write_rejection(1);
                anyhow::bail!("stale partition lease rejected completion commit")
            }
            -2 => anyhow::bail!("immutable completion conflicts with existing record"),
            -3 => anyhow::bail!("completion commit found a Redis key with the wrong type"),
            -4 => anyhow::bail!("completion commit found malformed durable JSON"),
            other => anyhow::bail!("unknown completion commit result {other}"),
        }
    }

    /// Marks one idempotent DynamoDB effect complete. Once all effects are
    /// marked, the record leaves the partition pending index but remains
    /// immutable for its cleanup grace period.
    pub async fn mark_completion_effect_done_fenced(
        &self,
        guard: &PartitionLeaseGuard,
        record: &crate::completion::CompletionRecordV1,
        effect_id: &str,
        cleanup_grace: std::time::Duration,
    ) -> Result<bool> {
        record.validate()?;
        if record.partition_id != guard.partition() || record.effect(effect_id).is_none() {
            anyhow::bail!("completion effect does not belong to fenced partition record");
        }
        let cleanup_grace_ms = u64::try_from(cleanup_grace.as_millis())
            .context("completion cleanup grace exceeds Redis range")?;
        if cleanup_grace_ms == 0 {
            anyhow::bail!("completion cleanup grace must be non-zero");
        }
        let mut redis = self.redis.clone();
        let operation = async {
            let script = redis::Script::new(
                r#"
            local function key_type(key)
                local value = redis.call('TYPE', key)
                if type(value) == 'table' then return value.ok end
                return value
            end
            local function require_type(key, expected)
                local actual = key_type(key)
                return actual == 'none' or actual == expected
            end
            if not require_type(KEYS[1], 'string') or
               not require_type(KEYS[2], 'set') or
               not require_type(KEYS[3], 'set') or
               not require_type(KEYS[4], 'string') or
               not require_type(KEYS[5], 'string') then return -3 end
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
            if not redis.call('GET', KEYS[4]) then return -2 end
            if redis.call('GET', KEYS[5]) ~= ARGV[5] then return -2 end
            local added = redis.call('SADD', KEYS[2], ARGV[2])
            if redis.call('SCARD', KEYS[2]) == tonumber(ARGV[3]) then
                redis.call('SREM', KEYS[3], ARGV[4])
                redis.call('PEXPIRE', KEYS[2], ARGV[6])
                redis.call('PEXPIRE', KEYS[4], ARGV[6])
                redis.call('PEXPIRE', KEYS[5], ARGV[6])
            end
            return added
                "#,
            );
            script
                .key(guard.lease_key())
                .key(guard.namespace().completion_effects_done(record.game_id))
                .key(guard.namespace().pending_completions(guard.partition()))
                .key(guard.namespace().completion(record.game_id))
                .key(
                    guard
                        .namespace()
                        .completion_terminal_notified(record.game_id),
                )
                .arg(guard.encoded_token())
                .arg(effect_id)
                .arg(record.effects.len())
                .arg(record.game_id)
                .arg(record.revision.to_string())
                .arg(cleanup_grace_ms)
                .invoke_async(&mut redis)
                .await
        };
        let result: i32 = tokio::time::timeout(FENCED_OPERATION_TIMEOUT, operation)
            .await
            .context("fenced completion effect mark timed out")??;
        match result {
            -1 => {
                crate::resilience_metrics::record_fenced_write_rejection(1);
                anyhow::bail!("stale partition lease rejected completion effect mark")
            }
            -2 => anyhow::bail!("completion terminal cleanup was not durably confirmed"),
            -3 => anyhow::bail!("completion effect mark found a Redis key with the wrong type"),
            _ => {}
        }
        Ok(result == 1)
    }

    pub async fn list_pending_completion_ids(
        &self,
        namespace: &ClusterNamespace,
        partition: u32,
    ) -> Result<Vec<u32>> {
        let mut redis = self.redis.clone();
        let mut game_ids: Vec<u32> = redis
            .smembers(namespace.pending_completions(partition))
            .await
            .context("failed to list pending completion records")?;
        game_ids.sort_unstable();
        Ok(game_ids)
    }

    pub async fn load_pending_completion(
        &self,
        namespace: &ClusterNamespace,
        partition: u32,
        game_id: u32,
    ) -> Result<crate::completion::CompletionRecordV1> {
        let mut redis = self.redis.clone();
        let payload: Option<Vec<u8>> = redis
            .get(namespace.completion(game_id))
            .await
            .context("failed to load pending completion record")?;
        let payload = payload.ok_or_else(|| {
            anyhow::anyhow!("pending completion {game_id} has no immutable record")
        })?;
        let record: crate::completion::CompletionRecordV1 = serde_json::from_slice(&payload)
            .with_context(|| format!("pending completion {game_id} has a malformed record"))?;
        record.validate()?;
        if record.game_id != game_id || record.partition_id != partition {
            anyhow::bail!("pending completion index/record identity mismatch");
        }
        Ok(record)
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

        let command_anchor = last_ids[1].clone();
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
            command_anchor,
            event_receiver: event_rx,
            command_receiver: command_rx,
            snapshot_request_receiver: request_rx,
        })
    }
}

pub struct ExecutorCommandConsumer {
    connection: redis::aio::MultiplexedConnection,
    stream_key: String,
    group: String,
    consumer: String,
    guard: PartitionLeaseGuard,
    claim_cursor: String,
    cancellation: CancellationToken,
}

impl ExecutorCommandConsumer {
    pub fn guard(&self) -> &PartitionLeaseGuard {
        &self.guard
    }

    /// Claims one bounded slice of every prior consumer's PEL. With exclusive
    /// lease authority, a zero idle threshold is safe and avoids takeover lag.
    /// The token check and XAUTOCLAIM are one Redis operation: a request paused
    /// between those steps must not steal work after a successor takes over.
    pub async fn reclaim_next(&mut self) -> Result<ReclaimedCommandBatch> {
        let operation = async {
            let script = redis::Script::new(
                r#"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return {0, false} end
            local claimed = redis.call(
                'XAUTOCLAIM', KEYS[2], ARGV[2], ARGV[3], 0, ARGV[4],
                'COUNT', ARGV[5]
            )
            return {1, claimed}
                "#,
            );
            script
                .key(self.guard.lease_key())
                .key(&self.stream_key)
                .arg(self.guard.encoded_token())
                .arg(&self.group)
                .arg(&self.consumer)
                .arg(&self.claim_cursor)
                .arg(EXECUTOR_GROUP_BATCH)
                .invoke_async(&mut self.connection)
                .await
        };
        let (authority, reply): (i32, Option<StreamAutoClaimReply>) =
            tokio::time::timeout(FENCED_OPERATION_TIMEOUT, operation)
                .await
                .context("fenced pending executor command reclaim timed out")?
                .context("failed to reclaim pending executor commands under lease")?;
        if authority != 1 {
            anyhow::bail!("partition lease authority was lost before pending-command reclaim");
        }
        let reply = reply.context("fenced XAUTOCLAIM returned no reply")?;
        self.claim_cursor = reply.next_stream_id.clone();
        let complete = reply.next_stream_id == "0-0";
        let mut deliveries = reply
            .claimed
            .into_iter()
            .map(decode_command_delivery)
            .collect::<Vec<_>>();
        if !deliveries.is_empty() {
            crate::resilience_metrics::record_command_claims(deliveries.len() as u64);
        }
        deliveries.sort_by(|left, right| {
            crate::recovery::validate_stream_id(&left.stream_id)
                .unwrap_or_default()
                .cmp(&crate::recovery::validate_stream_id(&right.stream_id).unwrap_or_default())
        });
        Ok(ReclaimedCommandBatch {
            deliveries,
            deleted_pending_ids: reply.deleted_ids,
            complete,
        })
    }

    pub async fn read_new_now(&mut self) -> Result<Vec<CommandDelivery>> {
        self.read_new_fenced().await
    }

    pub async fn read_new_blocking(&mut self) -> Result<Vec<CommandDelivery>> {
        loop {
            let deliveries = self.read_new_fenced().await?;
            if !deliveries.is_empty() {
                return Ok(deliveries);
            }
            tokio::select! {
                biased;
                _ = self.cancellation.cancelled() => return Ok(Vec::new()),
                _ = tokio::time::sleep(EXECUTOR_GROUP_IDLE) => {}
            }
        }
    }

    /// Atomically validates the exact acquisition token and assigns new group
    /// entries. XREADGROUP cannot block inside Lua, so the public blocking-style
    /// reader uses a short local idle between empty fenced reads.
    async fn read_new_fenced(&mut self) -> Result<Vec<CommandDelivery>> {
        let operation = async {
            let script = redis::Script::new(
                r#"
            if redis.call('GET', KEYS[1]) ~= ARGV[1] then return {0, false} end
            local entries = redis.call(
                'XREADGROUP', 'GROUP', ARGV[2], ARGV[3],
                'COUNT', ARGV[4], 'STREAMS', KEYS[2], '>'
            )
            return {1, entries}
                "#,
            );
            script
                .key(self.guard.lease_key())
                .key(&self.stream_key)
                .arg(self.guard.encoded_token())
                .arg(&self.group)
                .arg(&self.consumer)
                .arg(EXECUTOR_GROUP_BATCH)
                .invoke_async(&mut self.connection)
                .await
        };
        let (authority, reply): (i32, Option<StreamReadReply>) =
            tokio::time::timeout(FENCED_OPERATION_TIMEOUT, operation)
                .await
                .context("fenced executor group read timed out")?
                .context("executor fenced XREADGROUP failed")?;
        if authority != 1 {
            anyhow::bail!("partition lease authority was lost before executor group read");
        }
        let Some(reply) = reply else {
            return Ok(Vec::new());
        };
        let mut deliveries = Vec::new();
        for stream in reply.keys {
            for entry in stream.ids {
                deliveries.push(decode_command_delivery(entry));
            }
        }
        Ok(deliveries)
    }
}

fn decode_command_delivery(entry: StreamId) -> CommandDelivery {
    let stream_id = entry.id;
    let payload = match entry.map.get("data") {
        None => CommandDeliveryPayload::Poison {
            raw: Vec::new(),
            reason: "stream entry has no data field".to_string(),
        },
        Some(value) => match redis::from_redis_value::<Vec<u8>>(value) {
            Err(error) => CommandDeliveryPayload::Poison {
                raw: Vec::new(),
                reason: format!("stream entry data is not bytes: {error}"),
            },
            Ok(raw) => match serde_json::from_slice::<StreamEvent>(&raw) {
                Ok(command) => CommandDeliveryPayload::Command(command),
                Err(error) => CommandDeliveryPayload::Poison {
                    raw,
                    reason: format!("malformed executor command: {error}"),
                },
            },
        },
    };
    CommandDelivery {
        stream_id,
        payload,
        decision: None,
    }
}

async fn snapshot_request_reader(
    client: redis::Client,
    key: String,
    mut last_id: String,
    sender: mpsc::Sender<SnapshotRequest>,
    cancellation: CancellationToken,
) {
    'reconnect: loop {
        if cancellation.is_cancelled() || sender.is_closed() {
            return;
        }
        let mut connection = match client.get_multiplexed_async_connection().await {
            Ok(connection) => connection,
            Err(error) => {
                warn!(%error, %key, "snapshot-request reader failed to connect");
                tokio::time::sleep(Duration::from_millis(READER_RECONNECT_BACKOFF_MS)).await;
                continue;
            }
        };
        loop {
            let options = StreamReadOptions::default()
                .count(XREAD_COUNT)
                .block(XREAD_BLOCK_MS);
            let keys = [&key];
            let ids = [&last_id];
            let read = connection.xread_options::<_, _, StreamReadReply>(&keys, &ids, &options);
            let reply = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return,
                reply = read => match reply {
                    Ok(reply) => reply,
                    Err(error) => {
                        warn!(%error, %key, "snapshot-request reader reconnecting after XREAD failure");
                        continue 'reconnect;
                    }
                }
            };
            for stream in reply.keys {
                for entry in stream.ids {
                    let id = entry.id.clone();
                    let Some(payload) = entry.map.get("data") else {
                        warn!(%id, %key, "snapshot request has no data field");
                        last_id = id;
                        continue;
                    };
                    let request = redis::from_redis_value::<Vec<u8>>(payload)
                        .map_err(anyhow::Error::from)
                        .and_then(|bytes| {
                            serde_json::from_slice::<SnapshotRequest>(&bytes)
                                .map_err(anyhow::Error::from)
                        });
                    match request {
                        Ok(request) => {
                            if matches!(
                                forward(&sender, request, &cancellation).await,
                                Forward::SubscriberGone | Forward::Cancelled
                            ) {
                                return;
                            }
                        }
                        Err(error) => {
                            warn!(%error, %id, %key, "malformed snapshot request");
                        }
                    }
                    last_id = id;
                }
            }
        }
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
    command_tx: mpsc::Sender<CommandDelivery>,
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
                    if stream.key == commands_key {
                        let entry_id = entry.id.clone();
                        let outcome = forward(
                            &command_tx,
                            decode_command_delivery(entry),
                            &cancellation_token,
                        )
                        .await;
                        match outcome {
                            Forward::Delivered => {
                                last_ids[1] = entry_id;
                                continue;
                            }
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
                    }
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

                    let slot = if stream.key == events_key { 0 } else { 2 };
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
