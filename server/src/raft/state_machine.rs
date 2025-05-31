use anyhow::Result;
use common::GameState;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};
use tokio::sync::broadcast;

use crate::game_manager::GameManager;
use tokio::sync::RwLock as TokioRwLock;
use crate::replica_manager::{GameReplica, ReplicaManager, ReplicationCommand};
use super::types::{ClientRequest, ClientResponse, RaftNodeId, StateChangeEvent};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StateMachineSnapshot {
    pub last_applied_index: Option<u64>,
    pub game_replicas: HashMap<u32, GameReplica>,
    pub server_registry: HashMap<String, ServerRegistration>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerRegistration {
    pub server_id: String,
    pub host: String,
    pub port: u16,
    pub grpc_port: u16,
    pub max_capacity: u32,
    #[serde(skip, default = "std::time::Instant::now")]
    pub last_heartbeat: std::time::Instant,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StateMachineRequest {
    Apply(ClientRequest),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StateMachineResponse {
    Applied(ClientResponse),
}

pub struct GameStateMachine {
    node_id: RaftNodeId,
    game_manager: Arc<TokioRwLock<GameManager>>,
    replica_manager: Arc<ReplicaManager>,
    
    // Application state
    last_applied_index: Option<u64>,
    server_registry: HashMap<String, ServerRegistration>,
    
    // Event emitter
    event_tx: Option<broadcast::Sender<StateChangeEvent>>,
}

impl GameStateMachine {
    pub fn new(
        node_id: RaftNodeId,
        game_manager: Arc<TokioRwLock<GameManager>>,
        replica_manager: Arc<ReplicaManager>,
    ) -> Self {
        Self {
            node_id,
            game_manager,
            replica_manager,
            last_applied_index: None,
            server_registry: HashMap::new(),
            event_tx: None,
        }
    }
    
    /// Set the event sender for state change notifications
    pub fn set_event_sender(&mut self, tx: broadcast::Sender<StateChangeEvent>) {
        self.event_tx = Some(tx);
    }
    
    /// Emit a state change event
    fn emit_event(&self, event: StateChangeEvent) {
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(event);
        }
    }
    
    pub fn last_applied_log(&self) -> Option<u64> {
        self.last_applied_index
    }
    
    pub async fn apply(&mut self, index: &u64, request: &ClientRequest) -> Result<ClientResponse> {
        debug!("Applying request at index {}: {:?}", index, request);
        
        let response = match request {
            ClientRequest::UpdateGameState { game_id, state, tick } => {
                self.apply_game_state_update(*game_id, state.clone(), *tick).await?
            }
            
            ClientRequest::TransferAuthority { game_id, from_server, to_server, reason } => {
                self.apply_authority_transfer(*game_id, from_server.clone(), to_server.clone(), reason.clone()).await?
            }
            
            ClientRequest::CreateGame { game_id, initial_state, authority_server, players, discovery_source, discovered_at } => {
                self.apply_create_game(*game_id, initial_state.clone(), authority_server.clone(), players.clone(), discovery_source.clone(), *discovered_at).await?
            }
            
            ClientRequest::DeleteGame { game_id, reason } => {
                self.apply_delete_game(*game_id, reason.clone()).await?
            }
            
            ClientRequest::RegisterServer { server_id, host, port, grpc_port, max_capacity } => {
                self.apply_register_server(
                    server_id.clone(),
                    host.clone(),
                    *port,
                    *grpc_port,
                    *max_capacity
                ).await?
            }
            
            ClientRequest::UpdateServerHeartbeat { server_id } => {
                self.apply_heartbeat(server_id.clone()).await?
            }
            
            ClientRequest::RemoveServer { server_id } => {
                self.apply_remove_server(server_id.clone()).await?
            }
        };
        
        // Update last applied index
        self.last_applied_index = Some(*index);
        
        Ok(response)
    }
    
    async fn apply_game_state_update(
        &self,
        game_id: u32,
        state: GameState,
        tick: u32,
    ) -> Result<ClientResponse> {
        // Send to replica manager
        let command = ReplicationCommand::UpdateGameState {
            game_id,
            state: state.clone(),
            version: tick as u64,
            tick,
            source_server: self.node_id.0.clone(),
        };
        
        self.replica_manager
            .handle_replication_command(command)
            .await?;
        
        // If we're the authority, update game manager
        if self.game_manager.read().await.is_authority_for(game_id).await {
            // Game manager handles its own state updates during tick
            debug!("Game state update applied for game {}", game_id);
        }
        
        Ok(ClientResponse::GameStateUpdated { version: tick as u64 })
    }
    
    async fn apply_authority_transfer(
        &self,
        game_id: u32,
        from_server: String,
        to_server: String,
        reason: String,
    ) -> Result<ClientResponse> {
        info!(
            "Applying authority transfer for game {} from {} to {} (reason: {})",
            game_id, from_server, to_server, reason
        );
        
        let command = ReplicationCommand::UpdateAuthority {
            game_id,
            new_authority: to_server.clone(),
            version: 0, // Version will be set by replica manager
            reason: reason.clone(),
        };
        
        self.replica_manager
            .handle_replication_command(command)
            .await?;
        
        // Emit state change event
        self.emit_event(StateChangeEvent::AuthorityTransferred {
            game_id,
            from: from_server.clone(),
            to: to_server.clone(),
        });
        
        // Note: The GameExecutorService will handle authority transfers
        // when it receives the AuthorityTransferred event
        
        Ok(ClientResponse::AuthorityTransferred { new_authority: to_server })
    }
    
    async fn apply_create_game(
        &self,
        game_id: u32,
        initial_state: GameState,
        authority_server: String,
        players: Vec<u32>,
        discovery_source: String,
        discovered_at: i64,
    ) -> Result<ClientResponse> {
        info!("Creating game {} with authority on {} (discovered by {} at {})", 
              game_id, authority_server, discovery_source, discovered_at);
        
        // Create replica
        let replica = GameReplica {
            game_id,
            state: initial_state.clone(),
            version: 0,
            authority_server: authority_server.clone(),
            last_update: std::time::Instant::now(),
            tick: 0,
        };
        
        self.replica_manager.add_replica(replica).await?;
        
        // Emit state change event
        self.emit_event(StateChangeEvent::GameAssigned {
            game_id,
            authority: authority_server.clone(),
            players: players.clone(),
        });
        
        // Note: The GameExecutorService will handle starting the game
        // when it receives the GameAssigned event
        
        Ok(ClientResponse::GameCreated { game_id })
    }
    
    async fn apply_delete_game(
        &self,
        game_id: u32,
        reason: String,
    ) -> Result<ClientResponse> {
        info!("Deleting game {} (reason: {})", game_id, reason);
        
        // Remove replica
        let command = ReplicationCommand::DeleteGame {
            game_id,
            version: u64::MAX,
            reason,
        };
        
        self.replica_manager
            .handle_replication_command(command)
            .await?;
        
        // Emit state change event
        self.emit_event(StateChangeEvent::GameDeleted { game_id });
        
        // Note: The GameExecutorService will handle stopping the game
        // when it receives the GameDeleted event
        
        Ok(ClientResponse::GameDeleted)
    }
    
    async fn apply_register_server(
        &mut self,
        server_id: String,
        host: String,
        port: u16,
        grpc_port: u16,
        max_capacity: u32,
    ) -> Result<ClientResponse> {
        info!("Registering server {}: {}:{}", server_id, host, port);
        
        self.server_registry.insert(server_id.clone(), ServerRegistration {
            server_id: server_id.clone(),
            host,
            port,
            grpc_port,
            max_capacity,
            last_heartbeat: std::time::Instant::now(),
        });
        
        // Emit state change event
        self.emit_event(StateChangeEvent::ServerRegistered {
            server_id,
        });
        
        Ok(ClientResponse::ServerRegistered)
    }
    
    async fn apply_heartbeat(&mut self, server_id: String) -> Result<ClientResponse> {
        if let Some(server) = self.server_registry.get_mut(&server_id) {
            server.last_heartbeat = std::time::Instant::now();
            debug!("Updated heartbeat for server {}", server_id);
            Ok(ClientResponse::HeartbeatRecorded)
        } else {
            warn!("Heartbeat for unknown server {}", server_id);
            Ok(ClientResponse::Error("Unknown server".to_string()))
        }
    }
    
    async fn apply_remove_server(&mut self, server_id: String) -> Result<ClientResponse> {
        info!("Removing server {}", server_id);
        self.server_registry.remove(&server_id);
        
        // Emit state change event
        self.emit_event(StateChangeEvent::ServerRemoved {
            server_id,
        });
        
        Ok(ClientResponse::ServerRemoved)
    }
    
    /// Take a direct snapshot without serialization
    pub async fn take_direct_snapshot(&self) -> Result<StateMachineSnapshot> {
        let replicas = self.replica_manager.get_all_replicas().await;
        
        Ok(StateMachineSnapshot {
            last_applied_index: self.last_applied_index,
            game_replicas: replicas.into_iter().map(|r| (r.game_id, r)).collect(),
            server_registry: self.server_registry.clone(),
        })
    }
    
    /// Restore from a direct snapshot
    pub async fn restore_from_snapshot(&mut self, snapshot: &StateMachineSnapshot) -> Result<()> {
        info!("Restoring from snapshot with {} games and {} servers",
            snapshot.game_replicas.len(),
            snapshot.server_registry.len()
        );
        
        self.last_applied_index = snapshot.last_applied_index;
        self.server_registry = snapshot.server_registry.clone();
        
        // Restore game replicas
        for (game_id, replica) in &snapshot.game_replicas {
            self.replica_manager.add_replica(replica.clone()).await?;
            
            // Start game if we're the authority
            if replica.authority_server == self.node_id.0 {
                let mut gm = self.game_manager.write().await;
                gm.start_game(*game_id).await?;
            }
        }
        
        Ok(())
    }
    
    // Keep old methods for compatibility during migration
    pub async fn take_snapshot(&self) -> Result<(Option<u64>, Vec<u8>)> {
        let snapshot = self.take_direct_snapshot().await?;
        let data = bincode::serde::encode_to_vec(&snapshot, bincode::config::standard())?;
        Ok((self.last_applied_index, data))
    }
    
    pub async fn restore_snapshot(&mut self, data: &[u8]) -> Result<()> {
        let (snapshot, _): (StateMachineSnapshot, _) = bincode::serde::decode_from_slice(data, bincode::config::standard())?;
        self.restore_from_snapshot(&snapshot).await
    }
}