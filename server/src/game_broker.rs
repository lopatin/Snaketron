use std::collections::HashMap;
use std::sync::Arc;
use anyhow::Result;
use tokio::sync::{broadcast, Mutex};
use common::{GameCommandMessage, GameEventMessage};
use sqlx::PgPool;
use tonic::transport::Channel;

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
}

/// Local in-memory broker for single-server deployment
pub struct LocalBroker {
    command_txs: Arc<Mutex<HashMap<u32, broadcast::Sender<GameCommandMessage>>>>,
    event_txs: Arc<Mutex<HashMap<u32, broadcast::Sender<GameEventMessage>>>>,
}

impl LocalBroker {
    pub fn new() -> Self {
        Self {
            command_txs: Arc::new(Mutex::new(HashMap::new())),
            event_txs: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    
    /// Create channels for a new game
    pub async fn create_game_channels(&self, game_id: u32) -> Result<()> {
        let (command_tx, _) = broadcast::channel(32);
        let (event_tx, _) = broadcast::channel(32);
        
        let mut cmd_txs = self.command_txs.lock().await;
        let mut evt_txs = self.event_txs.lock().await;
        
        cmd_txs.insert(game_id, command_tx);
        evt_txs.insert(game_id, event_tx);
        
        Ok(())
    }
    
    /// Remove channels for a game
    pub async fn remove_game_channels(&self, game_id: u32) -> Result<()> {
        let mut cmd_txs = self.command_txs.lock().await;
        let mut evt_txs = self.event_txs.lock().await;
        
        cmd_txs.remove(&game_id);
        evt_txs.remove(&game_id);
        
        Ok(())
    }
}

#[async_trait::async_trait]
impl GameMessageBroker for LocalBroker {
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
    
    async fn get_game_location(&self, game_id: u32) -> Result<Option<String>> {
        let txs = self.command_txs.lock().await;
        if txs.contains_key(&game_id) {
            Ok(Some("local".to_string()))
        } else {
            Ok(None)
        }
    }
    
    async fn is_game_local(&self, game_id: u32) -> Result<bool> {
        let txs = self.command_txs.lock().await;
        Ok(txs.contains_key(&game_id))
    }
}

/// Distributed broker for multi-server deployment
pub struct DistributedBroker {
    local_broker: LocalBroker,
    grpc_clients: Arc<Mutex<HashMap<String, GameRelayClient<Channel>>>>,
    db_pool: PgPool,
    server_id: String,
    /// Cache of game locations (game_id -> server_id)
    game_locations: Arc<Mutex<HashMap<u32, String>>>,
}

impl DistributedBroker {
    pub fn new(db_pool: PgPool, server_id: String) -> Self {
        Self {
            local_broker: LocalBroker::new(),
            grpc_clients: Arc::new(Mutex::new(HashMap::new())),
            db_pool,
            server_id,
            game_locations: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    
    /// Create channels for a new local game and register in database
    pub async fn create_game_channels(&self, game_id: u32) -> Result<()> {
        // Create local channels
        self.local_broker.create_game_channels(game_id).await?;
        
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
}

#[async_trait::async_trait]
impl GameMessageBroker for DistributedBroker {
    async fn subscribe_commands(&self, game_id: u32) -> Result<broadcast::Receiver<GameCommandMessage>> {
        // For now, we only support subscribing to local games
        // Remote subscriptions would require establishing a gRPC stream
        self.local_broker.subscribe_commands(game_id).await
    }
    
    async fn publish_command(&self, game_id: u32, command: GameCommandMessage) -> Result<()> {
        // Check if game is local
        if self.local_broker.is_game_local(game_id).await? {
            return self.local_broker.publish_command(game_id, command).await;
        }
        
        // Game is remote - forward via gRPC
        if let Some(server_id) = self.lookup_game_server(game_id).await? {
            if server_id == self.server_id {
                // This shouldn't happen, but handle gracefully
                return self.local_broker.publish_command(game_id, command).await;
            }
            
            // TODO: Implement gRPC forwarding
            // For now, return error
            return Err(anyhow::anyhow!("Remote command forwarding not yet implemented"));
        }
        
        Err(anyhow::anyhow!("Game {} not found on any server", game_id))
    }
    
    async fn subscribe_events(&self, game_id: u32) -> Result<broadcast::Receiver<GameEventMessage>> {
        // For now, we only support subscribing to local games
        self.local_broker.subscribe_events(game_id).await
    }
    
    async fn publish_event(&self, game_id: u32, event: GameEventMessage) -> Result<()> {
        // Events are always published locally (by the game loop)
        // They will be forwarded to remote subscribers via gRPC
        self.local_broker.publish_event(game_id, event).await
    }
    
    async fn get_game_location(&self, game_id: u32) -> Result<Option<String>> {
        self.lookup_game_server(game_id).await
    }
    
    async fn is_game_local(&self, game_id: u32) -> Result<bool> {
        if let Some(server_id) = self.lookup_game_server(game_id).await? {
            Ok(server_id == self.server_id)
        } else {
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{timeout, Duration};
    
    #[tokio::test]
    async fn test_local_broker_create_and_subscribe() {
        let broker = LocalBroker::new();
        let game_id = 123;
        
        // Should fail before game is created
        assert!(broker.subscribe_commands(game_id).await.is_err());
        assert!(broker.subscribe_events(game_id).await.is_err());
        
        // Create game channels
        broker.create_game_channels(game_id).await.unwrap();
        
        // Now subscriptions should work
        let _cmd_rx = broker.subscribe_commands(game_id).await.unwrap();
        let _evt_rx = broker.subscribe_events(game_id).await.unwrap();
        
        // Game should be local
        assert!(broker.is_game_local(game_id).await.unwrap());
        assert_eq!(broker.get_game_location(game_id).await.unwrap(), Some("local".to_string()));
    }
    
    #[tokio::test]
    async fn test_local_broker_publish_and_receive() {
        let broker = LocalBroker::new();
        let game_id = 456;
        
        broker.create_game_channels(game_id).await.unwrap();
        
        // Subscribe before publishing
        let mut cmd_rx = broker.subscribe_commands(game_id).await.unwrap();
        let mut evt_rx = broker.subscribe_events(game_id).await.unwrap();
        
        // Publish command
        let test_cmd = GameCommandMessage {
            tick: 100,
            received_order: 1,
            user_id: 1,
            command: common::GameCommand::Tick,
        };
        broker.publish_command(game_id, test_cmd.clone()).await.unwrap();
        
        // Receive command
        let received_cmd = timeout(Duration::from_secs(1), cmd_rx.recv()).await
            .expect("Timeout waiting for command")
            .expect("Failed to receive command");
        assert_eq!(received_cmd, test_cmd);
        
        // Publish event
        let test_evt = GameEventMessage {
            game_id,
            tick: 101,
            user_id: Some(1),
            event: common::GameEvent::FoodSpawned { position: common::Position { x: 10, y: 20 } },
        };
        broker.publish_event(game_id, test_evt.clone()).await.unwrap();
        
        // Receive event
        let received_evt = timeout(Duration::from_secs(1), evt_rx.recv()).await
            .expect("Timeout waiting for event")
            .expect("Failed to receive event");
        assert_eq!(received_evt, test_evt);
    }
    
    #[tokio::test]
    async fn test_local_broker_multiple_subscribers() {
        let broker = LocalBroker::new();
        let game_id = 789;
        
        broker.create_game_channels(game_id).await.unwrap();
        
        // Create multiple subscribers
        let mut cmd_rx1 = broker.subscribe_commands(game_id).await.unwrap();
        let mut cmd_rx2 = broker.subscribe_commands(game_id).await.unwrap();
        
        // Publish command
        let test_cmd = GameCommandMessage {
            tick: 200,
            received_order: 1,
            user_id: 2,
            command: common::GameCommand::Tick,
        };
        broker.publish_command(game_id, test_cmd.clone()).await.unwrap();
        
        // Both subscribers should receive the command
        let received1 = timeout(Duration::from_secs(1), cmd_rx1.recv()).await
            .expect("Timeout on subscriber 1")
            .expect("Failed to receive on subscriber 1");
        let received2 = timeout(Duration::from_secs(1), cmd_rx2.recv()).await
            .expect("Timeout on subscriber 2")
            .expect("Failed to receive on subscriber 2");
        
        assert_eq!(received1, test_cmd);
        assert_eq!(received2, test_cmd);
    }
    
    #[tokio::test]
    async fn test_local_broker_remove_game() {
        let broker = LocalBroker::new();
        let game_id = 999;
        
        broker.create_game_channels(game_id).await.unwrap();
        assert!(broker.is_game_local(game_id).await.unwrap());
        
        broker.remove_game_channels(game_id).await.unwrap();
        assert!(!broker.is_game_local(game_id).await.unwrap());
        
        // Subscriptions should fail after removal
        assert!(broker.subscribe_commands(game_id).await.is_err());
        assert!(broker.subscribe_events(game_id).await.is_err());
    }
}