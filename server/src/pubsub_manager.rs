use anyhow::{Result, Context};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc, oneshot};
use tracing::{info, error, warn, debug};
use serde::{Serialize, Deserialize};
use redis::aio::{ConnectionManager, PubSub};
use redis::{AsyncCommands, Client};
use common::{GameState, GameEventMessage, GameEvent};
use futures_util::StreamExt;

/// Channel patterns for PubSub
pub mod channels {
    pub fn partition_events(partition_id: u32) -> String {
        format!("snaketron:events:partition:{}", partition_id)
    }
    
    pub fn partition_commands(partition_id: u32) -> String {
        format!("snaketron:commands:partition:{}", partition_id)
    }
    
    pub fn snapshot_requests(partition_id: u32) -> String {
        format!("snaketron:snapshot-requests:partition:{}", partition_id)
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
    pub(crate) redis_conn: ConnectionManager,
    pub(crate) redis_url: String,
    pub(crate) subscriptions: Arc<RwLock<HashMap<String, mpsc::Sender<Vec<u8>>>>>,
}

impl PubSubManager {
    /// Create a new PubSub manager
    pub async fn new(redis_url: &str) -> Result<Self> {
        let client = Client::open(redis_url)
            .context("Failed to create Redis client")?;
        let redis_conn = ConnectionManager::new(client).await
            .context("Failed to create Redis connection manager")?;
        
        Ok(Self {
            redis_conn,
            redis_url: redis_url.to_string(),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
        })
    }
    
    /// Clone the connection manager (for passing to other components)
    pub fn connection(&self) -> ConnectionManager {
        self.redis_conn.clone()
    }
    
    /// Publish an event to a partition channel
    pub async fn publish_event(&mut self, partition_id: u32, event: &GameEventMessage) -> Result<()> {
        let channel = channels::partition_events(partition_id);
        let data = serde_json::to_vec(event)
            .context("Failed to serialize event")?;
        
        let _: () = self.redis_conn.publish(&channel, data).await
            .context("Failed to publish event")?;
        
        debug!("Published event to channel {} for game {}: {:?}", channel, event.game_id, event.event);
        Ok(())
    }
    
    /// Publish a snapshot to a partition channel and store in Redis
    pub async fn publish_snapshot(&mut self, partition_id: u32, game_id: u32, snapshot: &GameState) -> Result<()> {
        // Create a snapshot event
        let event = GameEventMessage {
            game_id,
            tick: snapshot.tick,
            sequence: snapshot.event_sequence,
            user_id: None,
            event: GameEvent::Snapshot { game_state: snapshot.clone() },
        };
        
        // Publish to partition events channel
        let channel = channels::partition_events(partition_id);
        let data = serde_json::to_vec(&event)
            .context("Failed to serialize snapshot event")?;
        
        let _: () = self.redis_conn.publish(&channel, data).await
            .context("Failed to publish snapshot")?;
        
        // Also store in Redis with TTL (5 minutes)
        let key = format!("snaketron:snapshot:game:{}", game_id);
        let snapshot_data = serde_json::to_vec(snapshot)
            .context("Failed to serialize snapshot for storage")?;
        let _: () = self.redis_conn.set_ex(&key, snapshot_data, 300).await
            .context("Failed to store snapshot")?;
        
        info!("Published snapshot for game {} at tick {} to partition {}", game_id, snapshot.tick, partition_id);
        Ok(())
    }
    
    /// Request snapshots for all games in a partition
    pub async fn request_partition_snapshots(&mut self, partition_id: u32) -> Result<()> {
        let channel = channels::snapshot_requests(partition_id);
        let request = SnapshotRequest {
            partition_id,
            requester_id: None,
        };
        let data = serde_json::to_vec(&request)
            .context("Failed to serialize snapshot request")?;
        
        let _: () = self.redis_conn.publish(&channel, data).await
            .context("Failed to publish snapshot request")?;
        
        debug!("Requested snapshots for partition {}", partition_id);
        Ok(())
    }
    
    /// Get stored snapshot from Redis
    pub async fn get_stored_snapshot(&mut self, game_id: u32) -> Result<Option<GameState>> {
        let key = format!("snaketron:snapshot:game:{}", game_id);
        let data: Option<Vec<u8>> = self.redis_conn.get(&key).await
            .context("Failed to get snapshot from Redis")?;
        
        match data {
            Some(bytes) => {
                let snapshot = serde_json::from_slice(&bytes)
                    .context("Failed to deserialize snapshot")?;
                Ok(Some(snapshot))
            }
            None => Ok(None)
        }
    }
    
    
    /// Subscribe to a partition's events, commands and snapshot requests
    pub async fn subscribe_to_partition(&mut self, partition_id: u32) -> Result<PartitionSubscription> {
        let event_channel = channels::partition_events(partition_id);
        let command_channel = channels::partition_commands(partition_id);
        let request_channel = channels::snapshot_requests(partition_id);
        
        // Create channels for receiving messages
        let (event_tx, event_rx) = mpsc::channel(1000);
        let (command_tx, command_rx) = mpsc::channel(100);
        let (request_tx, request_rx) = mpsc::channel(100);
        
        // Spawn task to handle PubSub connection
        let redis_url = self.redis_url.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_partition_subscription(
                redis_url,
                event_channel,
                command_channel,
                request_channel,
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
    pub async fn publish_command(&mut self, partition_id: u32, command: &[u8]) -> Result<()> {
        let channel = channels::partition_commands(partition_id);
        let _: () = self.redis_conn.publish(&channel, command).await
            .context("Failed to publish command")?;
        Ok(())
    }
}


/// Handle partition subscription in a separate task
async fn handle_partition_subscription(
    redis_url: String,
    event_channel: String,
    command_channel: String,
    request_channel: String,
    event_tx: mpsc::Sender<GameEventMessage>,
    command_tx: mpsc::Sender<Vec<u8>>,
    request_tx: mpsc::Sender<SnapshotRequest>,
) -> Result<()> {
    let client = Client::open(redis_url.as_str())
        .context("Failed to create Redis client for subscription")?;
    let mut pubsub = client.get_async_pubsub().await
        .context("Failed to create PubSub connection")?;
    
    // Subscribe to all three channels
    pubsub.subscribe(&event_channel).await
        .context("Failed to subscribe to event channel")?;
    pubsub.subscribe(&command_channel).await
        .context("Failed to subscribe to command channel")?;
    pubsub.subscribe(&request_channel).await
        .context("Failed to subscribe to request channel")?;
    
    info!("Subscribed to partition channels: {}, {} and {}", event_channel, command_channel, request_channel);
    
    // Process messages
    loop {
        let msg = pubsub.on_message().next().await.unwrap();
        match msg.get_channel_name() {
            name if name == event_channel => {
                let payload: Vec<u8> = msg.get_payload()
                    .context("Failed to get event payload")?;
                match serde_json::from_slice::<GameEventMessage>(&payload) {
                    Ok(event) => {
                        if event_tx.send(event).await.is_err() {
                            warn!("Event receiver dropped, stopping subscription");
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Failed to deserialize game event: {}", e);
                    }
                }
            }
            name if name == command_channel => {
                let payload: Vec<u8> = msg.get_payload()
                    .context("Failed to get command payload")?;
                if command_tx.send(payload).await.is_err() {
                    warn!("Command receiver dropped, stopping subscription");
                    break;
                }
            }
            name if name == request_channel => {
                let payload: Vec<u8> = msg.get_payload()
                    .context("Failed to get request payload")?;
                match serde_json::from_slice::<SnapshotRequest>(&payload) {
                    Ok(request) => {
                        if request_tx.send(request).await.is_err() {
                            warn!("Request receiver dropped, stopping subscription");
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Failed to deserialize snapshot request: {}", e);
                    }
                }
            }
            _ => {
                warn!("Received message on unexpected channel");
            }
        }
    }
    
    Ok(())
}