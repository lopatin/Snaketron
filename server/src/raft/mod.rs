pub mod storage;
pub mod network;
pub mod state_machine;
pub mod types;

use async_raft::{Config, Raft, RaftMetrics, raft::ClientWriteRequest, raft::ClientWriteResponse, raft::MembershipConfig, NodeId, ClientWriteError};
use anyhow::Result;
use std::sync::Arc;
use std::collections::{HashSet, HashMap};
use std::time::Instant;
use tokio::sync::{RwLock, broadcast, mpsc};
use tracing::{info, debug};

use tokio::sync::RwLock as TokioRwLock;
use common::GameState;
pub use storage::GameRaftStorage;
pub use network::GameRaftNetwork;
pub use state_machine::{GameStateMachine, StateMachineRequest, StateMachineResponse};
pub use types::{ClientRequest, ClientResponse, StateChangeEvent};
use crate::game_relay;

pub type GameRaft = Raft<ClientRequest, ClientResponse, GameRaftNetwork, GameRaftStorage>;

const CLUSTER_NAME: &str = "snaketron";

/// Track learner progress
#[derive(Clone, Debug)]
pub struct LearnerProgress {
    pub node_id: NodeId,
    pub matched_index: u64,
    pub started_at: Instant,
    pub last_updated: Instant,
}

pub struct RaftNode {
    pub id: NodeId,
    pub raft: Arc<GameRaft>,
    pub storage: Arc<GameRaftStorage>,
    pub network: Arc<GameRaftNetwork>,
    pub metrics: Arc<RwLock<RaftMetrics>>,
    /// Track learner nodes and their progress
    learners: Arc<RwLock<HashMap<NodeId, LearnerProgress>>>,
    state_change_tx: broadcast::Sender<StateChangeEvent>,
}

impl RaftNode {
    pub async fn new(node_id: NodeId, initial_members: Vec<NodeId>) -> Result<Self> {
        // Event sender
        let (state_change_tx, _) = broadcast::channel(1024);
        
        // Create the storage layer
        let storage = Arc::new(GameRaftStorage::new(node_id, state_change_tx.clone()));
        
        // Create the network layer
        let network = Arc::new(GameRaftNetwork::new(node_id));
        
        // Configure Raft
        let config = Arc::new(Config::build(CLUSTER_NAME.to_string())
            .election_timeout_min(150)
            .election_timeout_max(300)
            .heartbeat_interval(50)
            .max_payload_entries(500)
            .validate()?);
        
        // Create the Raft instance
        let raft = Arc::new(Raft::new(
            node_id,
            config,
            network.clone(),
            storage.clone(),
        ));
        
        // Initialize if this is the first node
        if initial_members.len() == 1 && initial_members[0] == node_id {
            let members: HashSet<u64> = initial_members.into_iter().collect();
            raft.initialize(members).await?;
        }
        
        let metrics = Arc::new(RwLock::new(raft.metrics().borrow().clone()));
        
        Ok(Self {
            id: node_id,
            raft,
            storage,
            network,
            metrics,
            learners: Arc::new(RwLock::new(HashMap::new())),
            state_change_tx,
        })
    }
    
    pub async fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics();
        metrics.borrow().state == async_raft::State::Leader
    }
    
    pub async fn get_leader(&self) -> Option<u64> {
        let metrics = self.raft.metrics();
        metrics.borrow().current_leader
    }

    pub fn subscribe_state_events(&self) -> broadcast::Receiver<StateChangeEvent> {
        self.state_change_tx.subscribe()
    }
    
    pub async fn get_game_state(&self, game_id: u32) -> Option<GameState> {
        let storage = self.storage.clone();
        let state_machine = storage.get_state_machine().await;
        state_machine.game_states.get(&game_id).cloned()
    }

    pub async fn propose(&self, request: ClientRequest) -> Result<ClientResponse> {
        let client_request = ClientWriteRequest::new(request);
        match self.raft.client_write(client_request).await {
            Ok(response) => Ok(response.data),
            Err(ClientWriteError::ForwardToLeader(data, leader)) => {
                if let Some(leader_id) = leader {
                    info!("Forwarding request to leader {}: {:?}", leader_id, data);
                    let mut leader_client = self.network.get_client(leader_id).await
                        .ok_or_else(|| anyhow::anyhow!("No connection to leader {}", leader_id))?;
                    
                    // Serialize the client request
                    let serialized_request = bincode::serde::encode_to_vec(&data, bincode::config::standard())
                        .map_err(|e| anyhow::anyhow!("Failed to serialize request: {}", e))?;
                    
                    let request = crate::game_relay::RaftProposeRequest {
                        client_request: serialized_request,
                    };
                    
                    let response = leader_client.raft_propose(tonic::Request::new(request)).await
                        .map_err(|e| anyhow::anyhow!("Failed to forward to leader: {}", e))?;
                    let response = response.into_inner();
                    
                    if response.success {
                        if let Some(client_response_bytes) = response.client_response {
                            let (client_response, _): (ClientResponse, _) = bincode::serde::decode_from_slice(
                                &client_response_bytes,
                                bincode::config::standard()
                            ).map_err(|e| anyhow::anyhow!("Failed to deserialize response: {}", e))?;
                            Ok(client_response)
                        } else {
                            Err(anyhow::anyhow!("No response from leader"))
                        }
                    } else {
                        Err(anyhow::anyhow!("Leader returned error: {:?}", response.error))
                    }
                } else {
                    Err(anyhow::anyhow!("No leader available to forward request"))
                }
            },
            Err(e) => Err(anyhow::anyhow!("Failed to propose to Raft: {}", e)),
        }
    }
   
    /// Add a new node as a learner (non-voting member)
    pub async fn add_learner(&self, node_id: String, addr: String) -> Result<()> {
        let raft_id = node_id.parse::<u64>().unwrap_or_else(|_| {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            node_id.hash(&mut hasher);
            hasher.finish()
        });
        
        info!("Adding node {} as learner at {}", node_id, addr);
        
        // Add to network peers
        self.network.add_peer(raft_id, addr).await;
        
        // Add as non-voter (learner)
        self.raft.add_non_voter(raft_id).await?;
        
        // Track the learner
        let mut learners = self.learners.write().await;
        learners.insert(raft_id, LearnerProgress {
            node_id: raft_id,
            matched_index: 0,
            started_at: Instant::now(),
            last_updated: Instant::now(),
        });
        
        Ok(())
    }
    
    /// Check if a learner is caught up and ready for promotion
    pub async fn is_learner_caught_up(&self, node_id: &str) -> bool {
        let raft_id = node_id.parse::<u64>().unwrap_or(0);
        let learners = self.learners.read().await;
        let metrics = self.raft.metrics();
        let current_metrics = metrics.borrow();
        
        if let Some(learner) = learners.get(&raft_id) {
            // Check if learner is within 10 entries of the leader's log
            let leader_last_log = current_metrics.last_log_index;
            let is_caught_up = leader_last_log.saturating_sub(learner.matched_index) <= 10;
            
            // Also check that learner has been stable for at least 5 seconds
            let is_stable = learner.last_updated.elapsed().as_secs() >= 5;
            
            is_caught_up && is_stable
        } else {
            false
        }
    }
    
    /// Promote a learner to a voting member
    pub async fn promote_learner(&self, node_id: String) -> Result<()> {
        let raft_id = node_id.parse::<u64>().unwrap_or(0);
        
        if !self.is_learner_caught_up(&node_id).await {
            return Err(anyhow::anyhow!("Learner {} is not caught up yet", node_id));
        }
        
        info!("Promoting learner {} to voting member", node_id);
        
        // Get current membership and add as voting member
        let metrics = self.raft.metrics();
        let mut members = metrics.borrow().membership_config.members.clone();
        members.insert(raft_id);
        
        // Change membership to include the learner as a voter
        self.raft.change_membership(members).await?;
        
        // Remove from learner tracking
        self.learners.write().await.remove(&raft_id);
        
        Ok(())
    }
    
    /// Update learner progress (called by the leader)
    pub async fn update_learner_progress(&self, node_id: NodeId, matched_index: u64) {
        let mut learners = self.learners.write().await;
        if let Some(learner) = learners.get_mut(&node_id) {
            learner.matched_index = matched_index;
            learner.last_updated = Instant::now();
            debug!("Updated learner {} progress to index {}", node_id, matched_index);
        }
    }
    
    /// Emit a state change event (called by the state machine)
    pub fn emit_state_change(&self, event: StateChangeEvent) {
        // Ignore send errors (no receivers)
        let _ = self.state_change_tx.send(event);
    }
    
    /// Get a reference to the state change sender for the state machine
    pub fn get_state_change_sender(&self) -> broadcast::Sender<StateChangeEvent> {
        self.state_change_tx.clone()
    }

    
    pub async fn remove_node(&self, node_id: NodeId) -> Result<()> {
        
        // Get current membership and remove node
        let metrics = self.raft.metrics();
        let mut members = metrics.borrow().membership_config.members.clone();
        members.remove(&node_id);
        
        self.raft.change_membership(members).await?;
        self.network.remove_peer(node_id).await;
        Ok(())
    }
}
