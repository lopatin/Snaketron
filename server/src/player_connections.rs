use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use crate::ws_server::WSMessage;

/// Manages WebSocket connections for players
pub struct PlayerConnectionManager {
    /// Maps user_id to a channel that can send messages to their WebSocket connection
    connections: Arc<RwLock<HashMap<i32, mpsc::Sender<WSMessage>>>>,
}

impl PlayerConnectionManager {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Register a player's connection
    pub async fn register(&self, user_id: i32, sender: mpsc::Sender<WSMessage>) {
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
            sender.send(message).await
                .map_err(|_| "Failed to send message to player")?;
            Ok(())
        } else {
            Err("Player not connected")
        }
    }
    
    /// Send a MatchFound message to multiple players
    pub async fn notify_match_found(&self, player_ids: &[i32], game_id: u32) {
        let connections = self.connections.read().await;
        for &user_id in player_ids {
            if let Some(sender) = connections.get(&user_id) {
                let _ = sender.send(WSMessage::MatchFound { game_id }).await;
            }
        }
    }
}