use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio_tungstenite::tungstenite::Message;
use crate::ws_server::WSMessage;
use crate::game_manager::GameManager;
use tracing::info;

/// Manages WebSocket connections for players
pub struct PlayerConnectionManager {
    /// Maps user_id to a channel that can send messages to their WebSocket connection
    connections: Arc<RwLock<HashMap<i32, mpsc::Sender<Message>>>>,
}

impl PlayerConnectionManager {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Register a player's connection
    pub async fn register(&self, user_id: i32, sender: mpsc::Sender<Message>) {
        let mut connections = self.connections.write().await;
        connections.insert(user_id, sender);
    }
    
    /// Unregister a player's connection
    pub async fn unregister(&self, user_id: i32) {
        let mut connections = self.connections.write().await;
        connections.remove(&user_id);
    }
    
    /// Send a message to a specific player
    pub async fn send_to_player(&self, user_id: i32, message: WSMessage) -> Result<(), &'static str> {
        let connections = self.connections.read().await;
        if let Some(sender) = connections.get(&user_id) {
            let json = serde_json::to_string(&message)
                .map_err(|_| "Failed to serialize message")?;
            sender.send(Message::Text(json.into())).await
                .map_err(|_| "Failed to send message to player")?;
            Ok(())
        } else {
            Err("Player not connected")
        }
    }
    
    /// Notify players that they've been matched
    pub async fn notify_match_found_and_join(
        &self, 
        player_ids: &[i32], 
        game_id: u32, 
        _games_manager: Arc<RwLock<GameManager>>
    ) {
        let connections = self.connections.read().await;
        
        // Send each player the match found notification
        // The game will send its own snapshot when they join
        for &user_id in player_ids {
            if let Some(sender) = connections.get(&user_id) {
                // Send MatchFound notification
                let match_msg = WSMessage::MatchFound { game_id };
                if let Ok(json) = serde_json::to_string(&match_msg) {
                    let _ = sender.send(Message::Text(json.into())).await;
                    tracing::info!(user_id, game_id, "Sent MatchFound notification");
                }
            }
        }
    }
    
    /// Legacy method - kept for compatibility but should be phased out
    pub async fn notify_match_found(&self, player_ids: &[i32], game_id: u32) {
        let connections = self.connections.read().await;
        for &user_id in player_ids {
            if let Some(sender) = connections.get(&user_id) {
                let match_msg = WSMessage::MatchFound { game_id };
                if let Ok(json) = serde_json::to_string(&match_msg) {
                    let _ = sender.send(Message::Text(json.into())).await;
                }
            }
        }
    }
    
    /// Get which players are connected locally
    pub async fn get_connected_players(&self, player_ids: &[i32]) -> Vec<i32> {
        let connections = self.connections.read().await;
        player_ids.iter()
            .copied()
            .filter(|id| connections.contains_key(id))
            .collect()
    }
    
    /// Notify specific players that they've been matched (used for cross-server notifications)
    pub async fn notify_remote_match_found(
        &self, 
        player_ids: &[i32], 
        game_id: u32,
        game_host_server_id: &str,
    ) -> Vec<i32> {
        let connections = self.connections.read().await;
        let mut notified = Vec::new();
        
        for &user_id in player_ids {
            if let Some(sender) = connections.get(&user_id) {
                // Send MatchFound notification
                let match_msg = WSMessage::MatchFound { game_id };
                if let Ok(json) = serde_json::to_string(&match_msg) {
                    if sender.send(Message::Text(json.into())).await.is_ok() {
                        notified.push(user_id);
                        info!(user_id, game_id, game_host_server_id, "Sent cross-server MatchFound notification");
                    }
                }
            }
        }
        
        notified
    }
    
    /// Broadcast shutdown notice to all connected players
    pub async fn broadcast_shutdown_notice(&self, grace_period_seconds: u32) {
        let connections = self.connections.read().await;
        
        // Create shutdown message
        let shutdown_msg = WSMessage::ServerShutdown {
            reason: "Server is shutting down for maintenance".to_string(),
            grace_period_seconds,
        };
        
        if let Ok(json) = serde_json::to_string(&shutdown_msg) {
            let text_msg = Message::Text(json.into());
            
            for (user_id, sender) in connections.iter() {
                if let Err(e) = sender.send(text_msg.clone()).await {
                    info!(user_id, "Failed to send shutdown notice: {}", e);
                } else {
                    info!(user_id, "Sent shutdown notice");
                }
            }
        }
    }
}