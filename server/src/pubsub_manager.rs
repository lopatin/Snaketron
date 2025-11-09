use crate::redis_keys::RedisKeys;
use crate::redis_utils;
use anyhow::{Context, Result};
use common::{GameEvent, GameEventMessage, GameState};
use futures_util::StreamExt;
use redis::aio::{ConnectionManager, PubSub};
use redis::{AsyncCommands, Client, PushInfo, PushKind, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use serde::de::DeserializeOwned;
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub struct ChannelReceiver {
    inner: tokio::sync::broadcast::Receiver<PushInfo>,
    channel: String,
}

impl ChannelReceiver
{
    pub async fn recv<T>(&mut self) -> Result<T>
    where
        T: DeserializeOwned + Send + 'static,
    {
        loop {
            let PushInfo { kind, data } = self.inner.recv().await?;

            if !matches!(kind, PushKind::Message) {
                continue;
            }

            let [Value::BulkString(ch), Value::BulkString(payload)] = &data[..] else {
                continue;
            };

            let channel = String::from_utf8(ch.clone())
                .context("Failed to parse channel name as UTF-8")?;

            if channel != self.channel {
                continue;
            }

            let msg = serde_json::from_slice::<T>(payload)
                .with_context(|| format!("Failed to deserialize message from channel {}", channel))?;

            return Ok(msg);
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
    pub command_receiver: mpsc::Receiver<Vec<u8>>,
    pub snapshot_request_receiver: mpsc::Receiver<SnapshotRequest>,
}

impl PartitionSubscription {
    pub async fn recv_event(&mut self) -> Option<GameEventMessage> {
        self.event_receiver.recv().await
    }

    pub async fn recv_command(&mut self) -> Option<Vec<u8>> {
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
        Self { redis, pubsub_tx, cancellation_token }
    }

    pub async fn subscribe_to_channel(
        &mut self,
        channel: &str,
    ) -> Result<ChannelReceiver> {
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

        let _: () = self.redis
            .publish(&channel, data)
            .await
            .context("Failed to publish event")?;

        debug!(
            "Published event to channel {} for game {}: {:?}",
            channel, event.game_id, event.event
        );

        Ok(())
    }

    /// Publish a snapshot to a partition channel and store in Redis
    pub async fn publish_snapshot(
        &self,
        partition_id: u32,
        game_id: u32,
        snapshot: &GameState,
    ) -> Result<()> {
        // Create a snapshot event
        let event = GameEventMessage {
            game_id,
            tick: snapshot.tick,
            sequence: snapshot.event_sequence,
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

        // Also store in Redis with TTL (5 minutes)
        let key = RedisKeys::game_snapshot(game_id);
        let snapshot_data = serde_json::to_vec(snapshot)
            .context("Failed to serialize snapshot for storage")?;
        let _: () = redis
            .set_ex(&key, snapshot_data, 300)
            .await
            .context("Failed to store snapshot")?;

        info!(
            "Published snapshot for game {} at tick {} to partition {}",
            game_id, snapshot.tick, partition_id
        );

        Ok(())
    }

    /// Request snapshots for all games in a partition
    pub async fn request_partition_snapshots(&mut self, partition_id: u32) -> Result<()> {
        let channel = RedisKeys::snapshot_requests(partition_id);
        let request = SnapshotRequest {
            partition_id,
            requester_id: None,
        };

        let data = serde_json::to_vec(&request)
            .context("Failed to serialize snapshot request")?;

        let _: () = self.redis
            .publish(&channel, data)
            .await
            .context("Failed to publish snapshot request")?;

        debug!("Requested snapshots for partition {}", partition_id);
        Ok(())
    }

    /// Get stored snapshot from Redis
    pub async fn get_stored_snapshot(&mut self, game_id: u32) -> Result<Option<GameState>> {
        let data: Option<Vec<u8>> = self.redis
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

        // Spawn task to handle PubSub connection
        let mut self_for_subscription = self.clone();
        tokio::spawn(async move {
            if let Err(e) = self_for_subscription.handle_partition_subscription(
                RedisKeys::partition_events(partition_id),
                RedisKeys::partition_commands(partition_id),
                RedisKeys::snapshot_requests(partition_id),
                event_tx,
                command_tx,
                request_tx,
            ).await {
                error!("Partition subscription handler failed: {}", e);
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
    pub async fn publish_command(&self, partition_id: u32, command: &[u8]) -> Result<()> {
        let channel = RedisKeys::partition_commands(partition_id);
        let mut redis = self.redis.clone();
        let _: () = redis
            .publish(&channel, command)
            .await
            .context("Failed to publish command")?;
        Ok(())
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

    /// Handle partition subscription in a separate task
    async fn handle_partition_subscription(
        &mut self,
        event_channel: String,
        command_channel: String,
        request_channel: String,
        event_tx: mpsc::Sender<GameEventMessage>,
        command_tx: mpsc::Sender<Vec<u8>>,
        request_tx: mpsc::Sender<SnapshotRequest>,
    ) -> Result<()> {
        // Spawn all three channel handlers
        let events_handle = self.spawn_channel_handler(
            event_channel, event_tx, "Events").await?;
        let commands_handle = self.spawn_channel_handler(
            command_channel, command_tx, "Commands").await?;
        let requests_handle = self.spawn_channel_handler(
            request_channel, request_tx, "Requests").await?;

        // Wait for all tasks to complete
        let (events_result, commands_result, requests_result) = tokio::join!(
            events_handle,
            commands_handle,
            requests_handle
        );

        // Log task panics
        if let Err(e) = events_result {
            error!("Events task panicked: {}", e);
        }
        if let Err(e) = commands_result {
            error!("Commands task panicked: {}", e);
        }
        if let Err(e) = requests_result {
            error!("Requests task panicked: {}", e);
        }

        Ok(())
    }
}
