use anyhow::Result;
use common::{GameEventMessage, GameState, GameStatus};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use async_raft::NodeId;
use async_raft::raft::MembershipConfig;
use tracing::{debug, error, info, warn};
use tokio::sync::broadcast;

use crate::game_manager::GameManager;
use tokio::sync::RwLock as TokioRwLock;
use crate::replica_manager::{GameReplica, ReplicaManager, ReplicationCommand};
use super::types::{ClientRequest, ClientResponse, RaftNodeId, StateChangeEvent};

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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GameStateMachine {
    pub node_id: NodeId,
    pub game_states: HashMap<u32, GameState>,
    pub servers: HashMap<u64, ServerRegistration>,
    pub last_applied_log: u64,
}

impl GameStateMachine {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            game_states: HashMap::new(),
            servers: HashMap::new(),
            last_applied_log: 0,
        }
    }

    pub async fn apply(
        &mut self, 
        index: &u64, 
        request: &ClientRequest, 
        out: Option<&mut Vec<StateChangeEvent>>
    ) -> Result<(ClientResponse, Vec<StateChangeEvent>)> {
        debug!("Applying request at index {}: {:?}", index, request);

        self.last_applied_log = *index;

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
                if let Some(events) = out {
                    events.push(StateChangeEvent::GameCreated { game_id: *game_id });
                }

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
                    if let Some(events) = out {
                        events.push(StateChangeEvent::GameEvent { event: event.clone() });
                    }

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
                if let Some(events) = out {
                    events.push(StateChangeEvent::ServerRegistered {
                        server_id: *server_id,
                    });
                }

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

    pub(crate) fn last_applied_log(&self) -> u64 {
        self.last_applied_index.unwrap_or(0)
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
    // pub async fn take_direct_snapshot(&self) -> Result<StateMachineSnapshot> {
    //     Ok(StateMachineSnapshot {
    //         last_applied_index: self.last_applied_index,
    //         game_replicas: replicas.into_iter().map(|r| (r.game_id, r)).collect(),
    //         server_registry: self.server_registry.clone(),
    //     })
    // }
    
    /// Restore from a direct snapshot
    // pub async fn restore_from_snapshot(&mut self, snapshot: &StateMachineSnapshot) -> Result<()> {
    //     info!("Restoring from snapshot with {} games and {} servers",
    //         snapshot.game_replicas.len(),
    //         snapshot.server_registry.len()
    //     );
    //
    //     self.last_applied_index = snapshot.last_applied_index;
    //     self.server_registry = snapshot.server_registry.clone();
    //
    //     // Restore game replicas
    //     for (game_id, replica) in &snapshot.game_replicas {
    //         self.replica_manager.add_replica(replica.clone()).await?;
    //
    //         // Start game if we're the authority
    //         if replica.authority_server == self.node_id.0 {
    //             let mut gm = self.game_manager.write().await;
    //             gm.start_game(*game_id).await?;
    //         }
    //     }
    //
    //     Ok(())
    // }

}