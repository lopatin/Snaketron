use anyhow::Result;
use common::{GameEventMessage, GameState, GameStatus};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use async_raft::NodeId;
use async_raft::raft::MembershipConfig;
use tracing::{debug, error, info, warn};
use tokio::sync::broadcast;

use tokio::sync::RwLock as TokioRwLock;
use super::types::{ClientRequest, ClientResponse, StateChangeEvent};

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
        mut out: Option<&mut Vec<StateChangeEvent>>
    ) -> Result<(ClientResponse, Vec<StateChangeEvent>)> {
        debug!("Applying request at index {}: {:?}", index, request);

        self.last_applied_log = *index;

        let response = match request {
            ClientRequest::CreateGame { game_id, game_state } => {
                // Check if the game already exists
                if self.game_states.contains_key(game_id) {
                    warn!("Game {} already exists, ignoring create request", game_id);
                    return Ok((ClientResponse::Error(format!("Game {} already exists", game_id)), vec![]));
                }

                // Insert the new game state
                self.game_states.insert(*game_id, game_state.clone());

                // Emit event
                if let Some(ref mut events) = out {
                    events.push(StateChangeEvent::GameCreated { game_id: *game_id });
                }

                ClientResponse::Success
            }

            ClientRequest::StartGame { game_id, server_id } => {
                if let Some(game_state) = self.game_states.get_mut(game_id) {
                    match &game_state.status {
                        GameStatus::Stopped => {
                            game_state.status = GameStatus::Started { server_id: *server_id };
                            ClientResponse::Success
                        }
                        other => {
                            warn!("Attempted to start game {} which is not stopped (current status: {:?})", game_id, other);
                            ClientResponse::Error(format!("Game {} is not stopped", game_id))
                        }
                    }
                } else {
                    warn!("Attempted to start unknown game {}", game_id);
                    ClientResponse::Error(format!("Unknown game ID: {}", game_id))
                }
            }

            ClientRequest::ProcessGameEvent(event) => {
                if let Some(game_state) = self.game_states.get_mut(&event.game_id) {
                    // Process the game event
                    game_state.apply_event(event.event.clone(), None);

                    // Emit event
                    if let Some(ref mut events) = out {
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
                    return Ok((ClientResponse::Error(format!("Server {} already registered", server_id)), vec![]));
                }

                // Register the server
                let registration = ServerRegistration {
                    server_id: *server_id,
                    hostname: hostname.clone(),
                    grpc_port: *grpc_port,
                };

                self.servers.insert(*server_id, registration);

                // Emit state change event
                if let Some(ref mut events) = out {
                    events.push(StateChangeEvent::ServerRegistered {
                        server_id: *server_id,
                    });
                }

                ClientResponse::ServerRegistered
            }

            ClientRequest::RemoveServer { server_id } => {
                if self.servers.remove(server_id).is_some() {
                    // Emit state change event
                    if let Some(ref mut events) = out {
                        events.push(StateChangeEvent::ServerRemoved { server_id: *server_id });
                    }
                    info!("Server {} removed", server_id);
                    ClientResponse::ServerRemoved
                } else {
                    warn!("Attempted to remove unknown server {}", server_id);
                    ClientResponse::Error(format!("Unknown server ID: {}", server_id))
                }
            }
        };

        let events = out.map(|v| v.clone()).unwrap_or_default();
        Ok((response, events))
    }

}