use anyhow::Result;
use common::{GameEventMessage, GameState, GameStatus};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use async_raft::NodeId;
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
    pub server_id: u64,
    pub hostname: String,
    pub grpc_port: u16,
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
    node_id: NodeId,
    game_states: HashMap<u32, GameState>,
    servers: HashMap<u64, ServerRegistration>,
    event_tx: broadcast::Sender<StateChangeEvent>,
}

impl GameStateMachine {
    pub fn new(node_id: NodeId, event_tx: broadcast::Sender<StateChangeEvent>) -> Self {
        Self {
            node_id,
            game_states: HashMap::new(),
            servers: HashMap::new(),
            event_tx,
        }
    }

    pub async fn apply(&mut self, index: &u64, request: &ClientRequest) -> Result<ClientResponse> {
        debug!("Applying request at index {}: {:?}", index, request);
        
        let response = match request {
            ClientRequest::CreateGame { game_id, game_state } => {
                // Check if the game already exists
                if self.game_states.contains_key(game_id) {
                    warn!("Game {} already exists, ignoring create request", game_id);
                    return Ok(ClientResponse::Error(format!("Game {} already exists", game_id)));
                }
                
                // Insert the new game state
                self.game_states.insert(*game_id, game_state.clone());
                
                // Emit event
                self.emit_event(StateChangeEvent::GameCreated { game_id: *game_id });
                
                ClientResponse::Success
            }
            
            ClientRequest::StartGame { game_id, server_id } => {
                if let Some(game_state) = self.game_states.get_mut(game_id) {
                    match *game_state.status {
                        GameStatus::Stopped => {
                            game_state.status = GameStatus::Started { server_id };
                            ClientResponse::Success
                        }
                        other => {
                            warn!("Attempted to start game {} which is not stopped (current status: {:?})", game_id, other);
                            ClientResponse::Error(format!("Game {} is not stopped", game_id));
                        }
                    }
                } else {
                    warn!("Attempted to start unknown game {}", game_id);
                    ClientResponse::Error(format!("Unknown game ID: {}", game_id))
                }
            }
            
            ClientRequest::ProcessGameEvent(event) => {
                if let Some(game_state) = self.game_states.get_mut(*event.game_id) {
                    // Process the game event
                    game_state.apply_event(*event.event, None);
                    
                    // Emit event
                    self.emit_event(StateChangeEvent::GameEvent { event: event.clone() });
                    
                    ClientResponse::Success
                } else {
                    warn!("Received game event for unknown game {}", event.game_id);
                    ClientResponse::Error(format!("Unknown game ID: {}", event.game_id))
                }
            }
            
            ClientRequest::RegisterServer { server_id, hostname, grpc_port} => {
                info!("Registering server {}: {}:{}", server_id, hostname, grpc_port);
                
                // Check if the server already exists
                if self.servers.contains_key(server_id) {
                    warn!("Server {} is already registered", server_id);
                    return Ok(ClientResponse::Error(format!("Server {} already registered", server_id)));
                }
                
                // Register the server
                let registration = ServerRegistration {
                    server_id: *server_id,
                    hostname: hostname.clone(),
                    grpc_port: *grpc_port,
                };
                
                self.servers.insert(*server_id, registration);
                
                // Emit state change event
                self.emit_event(StateChangeEvent::ServerRegistered {
                    server_id: *server_id,
                });
                
                ClientResponse::ServerRegistered
            }
            
            ClientRequest::RemoveServer { server_id } => {
                if self.servers.remove(server_id).is_some() {
                    // Emit state change event
                    match self.event_tx.send(StateChangeEvent::ServerRemoved { server_id: server_id.clone() }) {
                        Ok(num_receivers) => {
                            info!("Server {} removed, notified {} receivers", server_id, num_receivers);
                            ClientResponse::ServerRemoved
                        }
                        Err(e) => {
                            warn!("Failed to notify receivers about server {} removal: {}", server_id, e);
                            ClientResponse::Error(format!("Failed to notify receivers: {}", e))
                        }
                    }
                } else {
                    warn!("Attempted to remove unknown server {}", server_id);
                    ClientResponse::Error(format!("Unknown server ID: {}", server_id))
                }
            }
        };
        
        Ok(response)
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
    
    // async fn apply_register_server(
    //     &mut self,
    //     server_id: u64,
    //     hostname: String,
    //     grpc_port: u16,
    // ) -> Result<ClientResponse> {
    //     info!("Registering server {}: {}:{}", server_id, hostname, grpc_port);
    //     
    //     self.servers.insert(server_id.clone(), ServerRegistration {
    //         server_id: server_id.clone(),
    //         host,
    //         port,
    //         grpc_port,
    //         max_capacity,
    //         last_heartbeat: std::time::Instant::now(),
    //     });
    //     
    //     // Emit state change event
    //     self.emit_event(StateChangeEvent::ServerRegistered {
    //         server_id,
    //     });
    //     
    //     Ok(ClientResponse::ServerRegistered)
    // }
    // 
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