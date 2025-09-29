use anyhow::{Result, Context};
use tokio::sync::mpsc;
use tracing::{info, warn, error};
use redis::aio::{ConnectionManager, PubSub};
use redis::{AsyncCommands, Client};
use futures_util::StreamExt;

use crate::matchmaking_manager::{MatchmakingManager, MatchNotification};
use crate::redis_keys::RedisKeys;
use crate::redis_utils;
use crate::ws_server::WSMessage;
use common::GameType;

/// Add a user to the matchmaking queue
pub async fn add_to_matchmaking_queue(
    matchmaking_manager: &mut MatchmakingManager,
    user_id: u32,
    username: String,
    mmr: i32,
    game_type: GameType,
    queue_mode: common::QueueMode,
) -> Result<()> {
    // Add to queue
    matchmaking_manager.add_to_queue(user_id, username, mmr, game_type.clone(), queue_mode.clone()).await?;
    
    info!("User {} added to matchmaking queue for game type {:?}", user_id, game_type);
    Ok(())
}

/// Remove a user from the matchmaking queue
pub async fn remove_from_matchmaking_queue(
    matchmaking_manager: &mut MatchmakingManager,
    user_id: u32,
) -> Result<()> {
    let removed_type = matchmaking_manager.remove_from_queue(user_id).await?;
    
    if let Some(game_type) = removed_type {
        info!("User {} removed from matchmaking queue for {:?}", user_id, game_type);
    } else {
        info!("User {} was not in matchmaking queue", user_id);
    }
    
    Ok(())
}

/// Check if a user has been matched to a game
pub async fn check_match_status(
    matchmaking_manager: &mut MatchmakingManager,
    user_id: u32,
) -> Result<Option<u32>> {
    let status = matchmaking_manager.get_queue_status(user_id).await?;
    
    if let Some(status) = status {
        Ok(status.matched_game_id)
    } else {
        Ok(None)
    }
}

/// Subscribe to match notifications for a user
pub async fn subscribe_to_match_notifications(
    redis_url: &str,
    user_id: u32,
    notification_tx: mpsc::Sender<MatchNotification>,
) -> Result<()> {
    let client = Client::open(redis_url)
        .context("Failed to create Redis client for notifications")?;
    
    let mut pubsub = client.get_async_pubsub().await
        .context("Failed to create PubSub connection")?;
    
    let redis_keys = RedisKeys::new();
    let channel = redis_keys.matchmaking_notification_channel(user_id);
    pubsub.subscribe(&channel).await
        .context("Failed to subscribe to notification channel")?;
    
    // Spawn a task to listen for notifications
    tokio::spawn(async move {
        let mut pubsub_stream = pubsub.on_message();
        
        while let Some(msg) = pubsub_stream.next().await {
            match msg.get_payload::<String>() {
                Ok(payload) => {
                    match serde_json::from_str::<MatchNotification>(&payload) {
                        Ok(notification) => {
                            if notification_tx.send(notification).await.is_err() {
                                warn!("Failed to send match notification - receiver dropped");
                                break;
                            }
                        }
                        Err(e) => {
                            error!("Failed to deserialize match notification: {}", e);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to get notification payload: {}", e);
                }
            }
        }
    });
    
    Ok(())
}

/// Publish a match notification
pub async fn publish_match_notification(
    redis_conn: &mut ConnectionManager,
    user_id: u32,
    notification: MatchNotification,
) -> Result<()> {
    let redis_keys = RedisKeys::new();
    let channel = redis_keys.matchmaking_notification_channel(user_id);
    let payload = serde_json::to_string(&notification)?;
    
    let _: () = redis_conn.publish(&channel, payload).await?;
    
    Ok(())
}

/// Notify all matched players about their game
pub async fn notify_matched_players(
    redis_conn: &mut ConnectionManager,
    game_id: u32,
    partition_id: u32,
    user_ids: &[u32],
) -> Result<()> {
    for user_id in user_ids {
        let notification = MatchNotification::MatchFound {
            game_id,
            partition_id,
            players: vec![], // Could include player details if needed
        };
        
        publish_match_notification(redis_conn, *user_id, notification).await?;
    }
    
    Ok(())
}

/// Send queue position update to a user
pub async fn send_queue_position_update(
    matchmaking_manager: &mut MatchmakingManager,
    redis_conn: &mut ConnectionManager,
    user_id: u32,
    game_type: &GameType,
    queue_mode: &common::QueueMode,
) -> Result<()> {
    if let Some(position) = matchmaking_manager.get_queue_position(user_id, game_type, queue_mode).await? {
        // Estimate wait time based on position (simplified)
        let estimated_wait_seconds = (position as u32) * 5; // 5 seconds per position
        
        let notification = MatchNotification::QueueJoined {
            position,
            estimated_wait_seconds,
        };
        
        publish_match_notification(redis_conn, user_id, notification).await?;
    }
    
    Ok(())
}

/// Handle matchmaking for WebSocket connections
pub struct MatchmakingHandler {
    matchmaking_manager: MatchmakingManager,
    redis_conn: ConnectionManager,
    notification_rx: mpsc::Receiver<MatchNotification>,
    ws_tx: mpsc::Sender<WSMessage>,
}

impl MatchmakingHandler {
    pub async fn new(
        redis_url: &str,
        ws_tx: mpsc::Sender<WSMessage>,
    ) -> Result<(Self, mpsc::Sender<MatchNotification>)> {
        let matchmaking_manager = MatchmakingManager::new(redis_url).await?;

        let client = Client::open(redis_url)?;
        let redis_conn = redis_utils::create_connection_manager(client.clone()).await?;
        
        let (notification_tx, notification_rx) = mpsc::channel(32);
        
        let handler = Self {
            matchmaking_manager,
            redis_conn,
            notification_rx,
            ws_tx,
        };
        
        Ok((handler, notification_tx))
    }
    
    /// Process match notifications and forward to WebSocket
    pub async fn process_notifications(mut self) {
        while let Some(notification) = self.notification_rx.recv().await {
            let ws_message = match notification {
                MatchNotification::MatchFound { game_id, .. } => {
                    WSMessage::MatchFound { game_id }
                }
                MatchNotification::QueueJoined { position, estimated_wait_seconds } => {
                    WSMessage::QueueUpdate { 
                        position: position as u32, 
                        estimated_wait_seconds 
                    }
                }
                MatchNotification::QueueLeft => {
                    WSMessage::QueueLeft
                }
            };
            
            if self.ws_tx.send(ws_message).await.is_err() {
                warn!("WebSocket connection closed, stopping notification processor");
                break;
            }
        }
    }
}