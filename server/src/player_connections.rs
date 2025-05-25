use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock, Mutex};
use tokio_tungstenite::tungstenite::Message;
use crate::ws_server::WSMessage;
use crate::games_manager::GamesManager;
use common::{GameEventMessage, GameEvent};

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
    
    /// Notify players that they've been matched and automatically join them to the game
    pub async fn notify_match_found_and_join(
        &self, 
        player_ids: &[i32], 
        game_id: u32, 
        games_manager: Arc<Mutex<GamesManager>>
    ) {
        let connections = self.connections.read().await;
        
        // Get the game snapshot first
        let games_mgr = games_manager.lock().await;
        let game_snapshot = match games_mgr.get_game_snapshot(game_id).await {
            Ok(snapshot) => snapshot,
            Err(e) => {
                tracing::error!(game_id, error = %e, "Failed to get game snapshot for matched players");
                return;
            }
        };
        drop(games_mgr);
        
        // Send each player the game snapshot directly
        for &user_id in player_ids {
            if let Some(sender) = connections.get(&user_id) {
                // Send the snapshot event
                let snapshot_event = GameEventMessage {
                    game_id,
                    tick: game_snapshot.tick,
                    user_id: Some(user_id as u32),
                    event: GameEvent::Snapshot { 
                        game_state: game_snapshot.clone() 
                    },
                };
                
                // Send snapshot wrapped in GameEvent message
                let game_event_msg = WSMessage::GameEvent(snapshot_event);
                if let Ok(json) = serde_json::to_string(&game_event_msg) {
                    let _ = sender.send(Message::Text(json.into())).await;
                    tracing::info!(user_id, game_id, "Sent initial game snapshot to matched player");
                } else {
                    tracing::error!(user_id, game_id, "Failed to serialize game event message");
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
}