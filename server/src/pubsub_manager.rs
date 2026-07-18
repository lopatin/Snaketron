use crate::game_executor::StreamEvent;
use crate::redis_keys::RedisKeys;
use anyhow::{Context, Result, anyhow};
use common::{GameEvent, GameEventMessage, GameState};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, PushInfo, PushKind, Value};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const GAME_CREATION_ACK_TIMEOUT: Duration = Duration::from_secs(10);
const GAME_CREATION_RETRY_INTERVAL: Duration = Duration::from_millis(100);

pub struct ChannelReceiver {
    inner: tokio::sync::broadcast::Receiver<PushInfo>,
    channel: String,
}

impl ChannelReceiver {
    /// Receive the next message for this channel.
    ///
    /// Only a closed broadcast (shutdown) returns an error. Lagging behind
    /// the shared Redis push firehose and malformed payloads are logged and
    /// skipped — before this, either one propagated as a fatal error and the
    /// subscription task above us broke its loop, permanently and silently
    /// severing a partition's entire event/command feed on this server. Lost
    /// messages surface downstream as stream_seq gaps, which trigger a
    /// snapshot resync.
    pub async fn recv<T>(&mut self) -> Result<T>
    where
        T: DeserializeOwned + Send + 'static,
    {
        loop {
            let PushInfo { kind, data } = match self.inner.recv().await {
                Ok(info) => info,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(
                        "Redis push receiver for channel {} lagged, {} pushes skipped; continuing (downstream gap detection will resync)",
                        self.channel, skipped
                    );
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Err(anyhow!("Redis push broadcast closed"));
                }
            };

            if !matches!(kind, PushKind::Message) {
                continue;
            }

            let [Value::BulkString(ch), Value::BulkString(payload)] = &data[..] else {
                continue;
            };

            let channel = match String::from_utf8(ch.clone()) {
                Ok(channel) => channel,
                Err(e) => {
                    warn!("Skipping Redis push with non-UTF-8 channel name: {}", e);
                    continue;
                }
            };

            if channel != self.channel {
                continue;
            }

            match serde_json::from_slice::<T>(payload) {
                Ok(msg) => return Ok(msg),
                Err(e) => {
                    warn!("Skipping malformed message on channel {}: {}", channel, e);
                    continue;
                }
            }
        }
    }
}

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

/// Manager for PubSub operations
#[derive(Clone)]
pub struct PubSubManager {
    redis: ConnectionManager,
    pubsub_tx: tokio::sync::broadcast::Sender<PushInfo>,
    cancellation_token: CancellationToken,
}

impl PubSubManager {
    /// Create a new PubSub manager
    pub fn new(
        redis: ConnectionManager,
        pubsub_tx: tokio::sync::broadcast::Sender<PushInfo>,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            redis,
            pubsub_tx,
            cancellation_token,
        }
    }

    pub async fn subscribe_to_channel(&mut self, channel: &str) -> Result<ChannelReceiver> {
        // Subscribe to the redis channel
        self.redis.subscribe(channel).await?;

        // Subscribe to the broadcast channel
        Ok(ChannelReceiver {
            inner: self.pubsub_tx.subscribe(),
            channel: channel.to_string(),
        })
    }

    /// Publish an event to a partition channel
    pub async fn publish_event(
        &mut self,
        partition_id: u32,
        event: &GameEventMessage,
    ) -> Result<()> {
        let channel = RedisKeys::partition_events(partition_id);
        let data = serde_json::to_vec(event).context("Failed to serialize event")?;

        let _: () = self
            .redis
            .publish(&channel, data)
            .await
            .context("Failed to publish event")?;

        debug!(
            "Published event to channel {} for game {}: {:?}",
            channel, event.game_id, event.event
        );

        Ok(())
    }

    /// Publish a snapshot to a partition channel and store in Redis.
    ///
    /// `stream_seq` is the executor-assigned transport sequence for this
    /// message (0 for pre-executor publishes, e.g. match creation). Returns
    /// the constructed snapshot event so callers can trace exactly what was
    /// published without rebuilding it.
    pub async fn publish_snapshot(
        &self,
        partition_id: u32,
        game_id: u32,
        snapshot: &GameState,
        stream_seq: u64,
    ) -> Result<GameEventMessage> {
        // Create a snapshot event
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

        // Publish to partition events channel
        let channel = RedisKeys::partition_events(partition_id);
        let data = serde_json::to_vec(&event).context("Failed to serialize snapshot event")?;

        let mut redis = self.redis.clone();
        let _: () = redis
            .publish(&channel, data)
            .await
            .context("Failed to publish snapshot")?;

        self.store_snapshot(game_id, snapshot).await?;

        info!(
            "Published snapshot for game {} at tick {} to partition {} (stream_seq {})",
            game_id, snapshot.tick, partition_id, stream_seq
        );

        Ok(event)
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

    /// Request snapshots for all games in a partition
    pub async fn request_partition_snapshots(&mut self, partition_id: u32) -> Result<()> {
        let channel = RedisKeys::snapshot_requests(partition_id);
        let request = SnapshotRequest {
            partition_id,
            requester_id: None,
        };

        let data = serde_json::to_vec(&request).context("Failed to serialize snapshot request")?;

        let _: () = self
            .redis
            .publish(&channel, data)
            .await
            .context("Failed to publish snapshot request")?;

        debug!("Requested snapshots for partition {}", partition_id);
        Ok(())
    }

    /// Get stored snapshot from Redis
    pub async fn get_stored_snapshot(&mut self, game_id: u32) -> Result<Option<GameState>> {
        let data: Option<Vec<u8>> = self
            .redis
            .get(RedisKeys::game_snapshot(game_id))
            .await
            .context("Failed to get snapshot from Redis")?;

        match data {
            Some(bytes) => {
                let snapshot =
                    serde_json::from_slice(&bytes).context("Failed to deserialize snapshot")?;
                Ok(Some(snapshot))
            }
            None => Ok(None),
        }
    }

    /// Subscribe to a partition's events, commands and snapshot requests
    pub async fn subscribe_to_partition(
        &mut self,
        partition_id: u32,
    ) -> Result<PartitionSubscription> {
        // Create channels for receiving messages
        let (event_tx, event_rx) = mpsc::channel(2000);
        let (command_tx, command_rx) = mpsc::channel(2000);
        let (request_tx, request_rx) = mpsc::channel(2000);

        // Establish all Redis subscriptions before reporting readiness. Redis Pub/Sub is
        // ephemeral, so returning while these SUBSCRIBE commands are still in flight can drop
        // the first GameCreated command during server startup.
        let events_handle = self
            .spawn_channel_handler(
                RedisKeys::partition_events(partition_id),
                event_tx,
                "Events",
            )
            .await?;
        let commands_handle = self
            .spawn_channel_handler(
                RedisKeys::partition_commands(partition_id),
                command_tx,
                "Commands",
            )
            .await?;
        let requests_handle = self
            .spawn_channel_handler(
                RedisKeys::snapshot_requests(partition_id),
                request_tx,
                "Requests",
            )
            .await?;

        tokio::spawn(async move {
            let (events_result, commands_result, requests_result) =
                tokio::join!(events_handle, commands_handle, requests_handle);

            if let Err(e) = events_result {
                error!("Events task panicked: {}", e);
            }
            if let Err(e) = commands_result {
                error!("Commands task panicked: {}", e);
            }
            if let Err(e) = requests_result {
                error!("Requests task panicked: {}", e);
            }
        });

        Ok(PartitionSubscription {
            partition_id,
            event_receiver: event_rx,
            command_receiver: command_rx,
            snapshot_request_receiver: request_rx,
        })
    }

    /// Publish a command to a partition
    pub async fn publish_command(&self, partition_id: u32, command: &StreamEvent) -> Result<()> {
        let json = serde_json::to_string(&command).context("Failed to serialize command")?;
        let channel = RedisKeys::partition_commands(partition_id);
        let mut redis = self.redis.clone();

        let StreamEvent::GameCreated { game_id, .. } = command else {
            let _: () = redis
                .publish(&channel, json)
                .await
                .context("Failed to publish command")?;
            return Ok(());
        };

        // GameCreated cannot be fire-and-forget: Redis Pub/Sub drops messages published before
        // the partition executor subscribes. Retry until the responsible executor confirms that
        // it accepted this game, making startup and failover races visible to the caller.
        let ack_key = RedisKeys::game_creation_ack(*game_id);
        let _: usize = redis.del(&ack_key).await?;
        let deadline = Instant::now() + GAME_CREATION_ACK_TIMEOUT;

        loop {
            let _: usize = redis
                .publish(&channel, &json)
                .await
                .context("Failed to publish GameCreated command")?;

            let acknowledged: bool = redis.exists(&ack_key).await?;
            if acknowledged {
                let _: usize = redis.del(&ack_key).await?;
                return Ok(());
            }

            if Instant::now() >= deadline {
                return Err(anyhow!(
                    "Partition executor did not acknowledge game {} within {:?}",
                    game_id,
                    GAME_CREATION_ACK_TIMEOUT
                ));
            }

            sleep(GAME_CREATION_RETRY_INTERVAL).await;
        }
    }

    /// Spawn a task to handle messages from a single channel
    async fn spawn_channel_handler<T>(
        &mut self,
        channel_name: String,
        sender: mpsc::Sender<T>,
        task_name: &str,
    ) -> Result<tokio::task::JoinHandle<()>>
    where
        T: DeserializeOwned + Send + 'static + Clone,
    {
        let mut channel_receiver = self.subscribe_to_channel(&channel_name).await?;
        let cancellation_token = self.cancellation_token.clone();
        let task_name = task_name.to_string();

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;

                    _ = cancellation_token.cancelled() => {
                        info!("{} subscription task for channel {} received cancellation signal", task_name, channel_name);
                        break;
                    }

                    message = channel_receiver.recv::<T>() => {
                        match message {
                            Ok(msg) => {
                                if let Err(e) = sender.try_send(msg.clone()) {
                                    match e {
                                        mpsc::error::TrySendError::Full(_) => {
                                            error!("{} channel full, message dropped", task_name);
                                        }
                                        mpsc::error::TrySendError::Closed(_) => {
                                            warn!("{} receiver dropped, stopping subscription", task_name);
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                error!("Failed to receive {} from channel {}: {}", task_name, channel_name, e);
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(handle)
    }
}
