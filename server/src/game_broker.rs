use std::collections::HashMap;
use std::sync::Arc;
use anyhow::Result;
use tokio::sync::{broadcast, Mutex, mpsc};
use common::{GameCommandMessage, GameEventMessage};
use sqlx::PgPool;
use tonic::transport::Channel;
use tokio_stream::StreamExt;

// Re-export the generated proto types
pub mod game_relay {
    tonic::include_proto!("game_relay");
}

use game_relay::game_relay_client::GameRelayClient;

/// Trait for abstracting message distribution between game servers
#[async_trait::async_trait]
pub trait GameMessageBroker: Send + Sync {
    /// Subscribe to commands for a specific game
    async fn subscribe_commands(&self, game_id: u32) -> Result<broadcast::Receiver<GameCommandMessage>>;
    
    /// Publish a command to a specific game
    async fn publish_command(&self, game_id: u32, command: GameCommandMessage) -> Result<()>;
    
    /// Subscribe to events from a specific game
    async fn subscribe_events(&self, game_id: u32) -> Result<broadcast::Receiver<GameEventMessage>>;
    
    /// Publish an event from a specific game
    async fn publish_event(&self, game_id: u32, event: GameEventMessage) -> Result<()>;
    
    /// Get the server ID that hosts a specific game
    async fn get_game_location(&self, game_id: u32) -> Result<Option<String>>;
    
    /// Check if a game is hosted locally
    async fn is_game_local(&self, game_id: u32) -> Result<bool>;
    
    /// Get self as Any for downcasting
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Local channels for in-memory message passing within a server
#[derive(Clone)]
struct LocalChannels {
    command_txs: Arc<Mutex<HashMap<u32, broadcast::Sender<GameCommandMessage>>>>,
    event_txs: Arc<Mutex<HashMap<u32, broadcast::Sender<GameEventMessage>>>>,
}

impl LocalChannels {
    fn new() -> Self {
        Self {
            command_txs: Arc::new(Mutex::new(HashMap::new())),
            event_txs: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    
    /// Create channels for a new game (only if they don't exist)
    async fn create_game_channels(&self, game_id: u32) -> Result<()> {
        let mut cmd_txs = self.command_txs.lock().await;
        let mut evt_txs = self.event_txs.lock().await;
        
        // Only create channels if they don't already exist
        if !cmd_txs.contains_key(&game_id) {
            let (command_tx, _) = broadcast::channel(32);
            let (event_tx, _) = broadcast::channel(32);
            
            cmd_txs.insert(game_id, command_tx);
            evt_txs.insert(game_id, event_tx);
        }
        
        Ok(())
    }
    
    /// Remove channels for a game
    async fn remove_game_channels(&self, game_id: u32) -> Result<()> {
        let mut cmd_txs = self.command_txs.lock().await;
        let mut evt_txs = self.event_txs.lock().await;
        
        cmd_txs.remove(&game_id);
        evt_txs.remove(&game_id);
        
        Ok(())
    }
    
    async fn subscribe_commands(&self, game_id: u32) -> Result<broadcast::Receiver<GameCommandMessage>> {
        let txs = self.command_txs.lock().await;
        let tx = txs.get(&game_id)
            .ok_or_else(|| anyhow::anyhow!("Game {} not found", game_id))?;
        Ok(tx.subscribe())
    }
    
    async fn publish_command(&self, game_id: u32, command: GameCommandMessage) -> Result<()> {
        let txs = self.command_txs.lock().await;
        let tx = txs.get(&game_id)
            .ok_or_else(|| anyhow::anyhow!("Game {} not found", game_id))?;
        let _ = tx.send(command)?;
        Ok(())
    }
    
    async fn subscribe_events(&self, game_id: u32) -> Result<broadcast::Receiver<GameEventMessage>> {
        let txs = self.event_txs.lock().await;
        let tx = txs.get(&game_id)
            .ok_or_else(|| anyhow::anyhow!("Game {} not found", game_id))?;
        Ok(tx.subscribe())
    }
    
    async fn publish_event(&self, game_id: u32, event: GameEventMessage) -> Result<()> {
        let txs = self.event_txs.lock().await;
        let tx = txs.get(&game_id)
            .ok_or_else(|| anyhow::anyhow!("Game {} not found", game_id))?;
        let _ = tx.send(event)?;
        Ok(())
    }
    
    async fn contains_game(&self, game_id: u32) -> bool {
        let txs = self.command_txs.lock().await;
        txs.contains_key(&game_id)
    }
}

/// Game message broker that handles both local and distributed messaging
#[derive(Clone)]
pub struct GameBroker {
    local_channels: LocalChannels,
    grpc_clients: Arc<Mutex<HashMap<String, GameRelayClient<Channel>>>>,
    db_pool: PgPool,
    server_id: String,
    /// Cache of game locations (game_id -> server_id)
    game_locations: Arc<Mutex<HashMap<u32, String>>>,
    /// Active gRPC streams to remote servers (server_id -> stream sender)
    remote_streams: Arc<Mutex<HashMap<String, mpsc::Sender<game_relay::GameMessage>>>>,
    /// Remote event receivers kept alive to prevent channel closure (game_id -> event receiver)
    remote_event_rxs: Arc<Mutex<HashMap<u32, broadcast::Receiver<GameEventMessage>>>>,
}

impl GameBroker {
    pub fn new(db_pool: PgPool, server_id: String) -> Self {
        Self {
            local_channels: LocalChannels::new(),
            grpc_clients: Arc::new(Mutex::new(HashMap::new())),
            db_pool,
            server_id,
            game_locations: Arc::new(Mutex::new(HashMap::new())),
            remote_streams: Arc::new(Mutex::new(HashMap::new())),
            remote_event_rxs: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    
    /// Create channels for a new local game and register in database
    pub async fn create_game_channels(&self, game_id: u32) -> Result<()> {
        // Create local channels
        self.local_channels.create_game_channels(game_id).await?;
        
        // Register game location in database
        let server_uuid = uuid::Uuid::parse_str(&self.server_id)?;
        sqlx::query("UPDATE games SET server_id = $1 WHERE id = $2")
            .bind(server_uuid)
            .bind(game_id as i32)
            .execute(&self.db_pool)
            .await?;
        
        // Update cache
        let mut locations = self.game_locations.lock().await;
        locations.insert(game_id, self.server_id.clone());
        
        Ok(())
    }
    
    /// Remove channels for a game
    pub async fn remove_game_channels(&self, game_id: u32) -> Result<()> {
        // Remove local channels
        self.local_channels.remove_game_channels(game_id).await?;
        
        // Remove from cache
        let mut locations = self.game_locations.lock().await;
        locations.remove(&game_id);
        
        Ok(())
    }
    
    /// Look up which server hosts a game
    async fn lookup_game_server(&self, game_id: u32) -> Result<Option<String>> {
        // Check cache first
        {
            let locations = self.game_locations.lock().await;
            if let Some(server_id) = locations.get(&game_id) {
                return Ok(Some(server_id.clone()));
            }
        }
        
        // Query database
        let row = sqlx::query_as::<_, (uuid::Uuid,)>(
            "SELECT s.id FROM games g JOIN servers s ON g.server_id = s.id WHERE g.id = $1"
        )
        .bind(game_id as i32)
        .fetch_optional(&self.db_pool)
        .await?;
        
        if let Some((server_uuid,)) = row {
            let server_id = server_uuid.to_string();
            // Update cache
            let mut locations = self.game_locations.lock().await;
            locations.insert(game_id, server_id.clone());
            Ok(Some(server_id))
        } else {
            Ok(None)
        }
    }
    
    /// Get or create gRPC client for a server
    async fn get_grpc_client(&self, server_id: &str) -> Result<GameRelayClient<Channel>> {
        let mut clients = self.grpc_clients.lock().await;
        
        if let Some(client) = clients.get(server_id) {
            return Ok(client.clone());
        }
        
        // Look up server's gRPC address
        let server_uuid = uuid::Uuid::parse_str(server_id)?;
        let row = sqlx::query_as::<_, (String, Option<i32>)>(
            "SELECT host, grpc_port FROM servers WHERE id = $1"
        )
        .bind(server_uuid)
        .fetch_one(&self.db_pool)
        .await?;
        
        let grpc_port = row.1
            .ok_or_else(|| anyhow::anyhow!("Server {} has no gRPC port configured", server_id))?;
        
        let addr = format!("http://{}:{}", row.0, grpc_port);
        let client = GameRelayClient::connect(addr).await?;
        
        clients.insert(server_id.to_string(), client.clone());
        Ok(client)
    }
    
    /// Establish a bidirectional stream to a remote server
    async fn ensure_stream_to_server(&self, server_id: &str) -> Result<()> {
        let mut streams = self.remote_streams.lock().await;
        
        // Check if we already have a stream
        if streams.contains_key(server_id) {
            return Ok(());
        }
        
        // Get gRPC client
        let mut client = self.get_grpc_client(server_id).await?;
        
        // Create channel for sending messages
        let (tx, rx) = mpsc::channel(32);
        
        // Start the bidirectional stream
        let request_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        let response = client.stream_game_messages(request_stream).await?;
        let mut response_stream = response.into_inner();
        
        // Store the sender
        streams.insert(server_id.to_string(), tx);
        
        // Spawn task to handle incoming messages from remote server
        let server_id_clone = server_id.to_string();
        let broker = self.clone(); // We need Clone trait
        tokio::spawn(async move {
            while let Some(result) = response_stream.next().await {
                match result {
                    Ok(game_message) => {
                        // Handle incoming message from remote server
                        broker.handle_remote_message(game_message).await;
                    }
                    Err(e) => {
                        eprintln!("Error receiving from server {}: {:?}", server_id_clone, e);
                        break;
                    }
                }
            }
            // Clean up stream on error
            let mut streams = broker.remote_streams.lock().await;
            streams.remove(&server_id_clone);
        });
        
        Ok(())
    }
    
    /// Handle incoming message from remote server
    async fn handle_remote_message(&self, message: game_relay::GameMessage) {
        use game_relay::game_message::Message;
        
        match message.message {
            Some(Message::Event(event)) => {
                // Deserialize just the event, then reconstruct the message
                if let Ok((game_event, _)) = bincode::serde::decode_from_slice::<common::GameEvent, bincode::config::Configuration>(&event.event_data, bincode::config::standard()) {
                    let event_msg = GameEventMessage {
                        game_id: event.game_id,
                        tick: event.tick,
                        user_id: event.user_id.map(|id| id as u32),
                        event: game_event,
                    };
                    // Forward to local subscribers
                    if let Err(e) = self.local_channels.publish_event(event.game_id, event_msg.clone()).await {
                    } else {
                    }
                } else {
                    if let Err(e) = bincode::serde::decode_from_slice::<common::GameEvent, bincode::config::Configuration>(&event.event_data, bincode::config::standard()) {
                    }
                }
            }
            Some(Message::Command(cmd)) => {
                // This shouldn't happen in normal flow (commands go TO remote)
                eprintln!("Received unexpected command from remote: game_id={}", cmd.game_id);
            }
            _ => {
                // Subscribe/unsubscribe handled elsewhere
            }
        }
    }
}

#[async_trait::async_trait]
impl GameMessageBroker for GameBroker {
    async fn subscribe_commands(&self, game_id: u32) -> Result<broadcast::Receiver<GameCommandMessage>> {
        // For now, we only support subscribing to local games
        // Remote subscriptions would require establishing a gRPC stream
        self.local_channels.subscribe_commands(game_id).await
    }
    
    async fn publish_command(&self, game_id: u32, command: GameCommandMessage) -> Result<()> {
        // Check if game is local
        if self.is_game_local(game_id).await? {
            return self.local_channels.publish_command(game_id, command).await;
        }
        
        // Game is remote - forward via gRPC
        if let Some(server_id) = self.lookup_game_server(game_id).await? {
            if server_id == self.server_id {
                // This shouldn't happen, but handle gracefully
                return self.local_channels.publish_command(game_id, command).await;
            }
            
            // Ensure we have a stream to the remote server
            self.ensure_stream_to_server(&server_id).await?;
            
            // Serialize command
            let command_data = bincode::serde::encode_to_vec(&command.command, bincode::config::standard())?;
            
            // Create gRPC message
            let grpc_command = game_relay::GameCommand {
                game_id,
                tick: command.tick,
                user_id: command.user_id as i32,
                command_data,
            };
            
            let message = game_relay::GameMessage {
                message: Some(game_relay::game_message::Message::Command(grpc_command)),
            };
            
            // Send via stream
            let streams = self.remote_streams.lock().await;
            if let Some(tx) = streams.get(&server_id) {
                tx.send(message).await?;
                Ok(())
            } else {
                Err(anyhow::anyhow!("Failed to get stream to server {}", server_id))
            }
        } else {
            Err(anyhow::anyhow!("Game {} not found on any server", game_id))
        }
    }
    
    async fn subscribe_events(&self, game_id: u32) -> Result<broadcast::Receiver<GameEventMessage>> {
        // Check if game is local
        if self.is_game_local(game_id).await? {
                return self.local_channels.subscribe_events(game_id).await;
        }
        
        // For remote games, we need to:
        // 1. Ensure stream to remote server
        // 2. Send subscribe message
        // 3. Create local event channel that will receive forwarded events
        
        if let Some(server_id) = self.lookup_game_server(game_id).await? {
            // Ensure stream exists
            self.ensure_stream_to_server(&server_id).await?;
            
            // Send subscribe message
            let subscribe = game_relay::Subscribe {
                game_id,
                commands: false,
                events: true,
            };
            
            let message = game_relay::GameMessage {
                message: Some(game_relay::game_message::Message::Subscribe(subscribe)),
            };
            
            let streams = self.remote_streams.lock().await;
            if let Some(tx) = streams.get(&server_id) {
                tx.send(message).await?;
            }
            drop(streams); // Release lock before sleeping
            
            // Give the remote server time to process the subscribe message
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            
            // Create a local channel for this remote game if it doesn't exist
            self.local_channels.create_game_channels(game_id).await?;
            
            // Subscribe to keep the channel alive
            let keeper_rx = self.local_channels.subscribe_events(game_id).await?;
            
            // Store the keeper receiver to prevent channel closure
            let mut remote_rxs = self.remote_event_rxs.lock().await;
            remote_rxs.insert(game_id, keeper_rx);
            
            // Now we can subscribe to the local channel which will receive forwarded events
            self.local_channels.subscribe_events(game_id).await
        } else {
            Err(anyhow::anyhow!("Game {} not found", game_id))
        }
    }
    
    async fn publish_event(&self, game_id: u32, event: GameEventMessage) -> Result<()> {
        // Events are always published locally (by the game loop)
        // They will be forwarded to remote subscribers via gRPC
        self.local_channels.publish_event(game_id, event).await
    }
    
    async fn get_game_location(&self, game_id: u32) -> Result<Option<String>> {
        self.lookup_game_server(game_id).await
    }
    
    async fn is_game_local(&self, game_id: u32) -> Result<bool> {
        if let Some(server_id) = self.lookup_game_server(game_id).await? {
            Ok(server_id == self.server_id)
        } else {
            // Check if it's a local game that hasn't been registered in DB yet
            Ok(self.local_channels.contains_game(game_id).await)
        }
    }
    
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{timeout, Duration};
    
    // Note: Tests for GameBroker require a database connection and are covered
    // in integration tests. The LocalChannels functionality is tested indirectly
    // through the GameBroker interface.
}