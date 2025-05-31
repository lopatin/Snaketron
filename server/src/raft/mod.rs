pub mod storage;
pub mod network;
pub mod state_machine;
pub mod types;

use async_raft::{Config, Raft, RaftMetrics, raft::ClientWriteRequest, raft::ClientWriteResponse};
use anyhow::Result;
use std::sync::Arc;
use std::collections::HashSet;
use tokio::sync::RwLock;

use crate::game_manager::GameManager;
use crate::replica_manager::ReplicaManager;
use tokio::sync::RwLock as TokioRwLock;

pub use storage::GameRaftStorage;
pub use network::GameRaftNetwork;
pub use state_machine::{GameStateMachine, StateMachineRequest, StateMachineResponse};
pub use types::{RaftNodeId, ClientRequest, ClientResponse};

pub type GameRaft = Raft<ClientRequest, ClientResponse, GameRaftNetwork, GameRaftStorage>;

pub struct RaftNode {
    pub id: RaftNodeId,
    pub raft: Arc<GameRaft>,
    pub storage: Arc<GameRaftStorage>,
    pub network: Arc<GameRaftNetwork>,
    pub metrics: Arc<RwLock<RaftMetrics>>,
}

impl RaftNode {
    pub async fn new(
        id: String,
        game_manager: Arc<TokioRwLock<GameManager>>,
        replica_manager: Arc<ReplicaManager>,
        initial_members: Vec<RaftNodeId>,
    ) -> Result<Self> {
        let node_id = RaftNodeId(id.clone());
        
        // Parse string ID to u64 for Raft
        let raft_id = id.parse::<u64>().unwrap_or_else(|_| {
            // Use hash of string as fallback
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            id.hash(&mut hasher);
            hasher.finish()
        });
        
        // Create storage
        let storage = Arc::new(GameRaftStorage::new(
            node_id.clone(),
            game_manager.clone(),
            replica_manager.clone(),
        ));
        
        // Create network layer
        let network = Arc::new(GameRaftNetwork::new(node_id.clone()));
        
        // Configure Raft
        let config = Arc::new(Config::build(id.clone())
            .election_timeout_min(150)
            .election_timeout_max(300)
            .heartbeat_interval(50)
            .max_payload_entries(500)
            .validate()?);
        
        // Create Raft instance
        let raft = Arc::new(Raft::new(
            raft_id,
            config,
            network.clone(),
            storage.clone(),
        ));
        
        // Initialize if this is the first node
        if initial_members.len() == 1 && initial_members[0] == node_id {
            let members: HashSet<u64> = initial_members.iter()
                .map(|n| n.0.parse::<u64>().unwrap_or(0))
                .collect();
            raft.initialize(members).await?;
        }
        
        let metrics = Arc::new(RwLock::new(raft.metrics().borrow().clone()));
        
        Ok(Self {
            id: node_id,
            raft,
            storage,
            network,
            metrics,
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
    
    pub async fn propose(&self, request: ClientRequest) -> Result<ClientResponse> {
        let client_request = ClientWriteRequest::new(request);
        match self.raft.client_write(client_request).await {
            Ok(response) => Ok(response.data),
            Err(e) => Err(anyhow::anyhow!("Failed to propose to Raft: {}", e)),
        }
    }
    
    pub async fn add_node(&self, node_id: RaftNodeId, addr: String) -> Result<()> {
        let raft_id = node_id.0.parse::<u64>().unwrap_or(0);
        self.network.add_peer(raft_id, addr).await;
        self.raft.add_non_voter(raft_id).await?;
        
        // Get current membership and add new node
        let metrics = self.raft.metrics();
        let mut members = metrics.borrow().membership_config.members.clone();
        members.insert(raft_id);
        
        self.raft.change_membership(members).await?;
        Ok(())
    }
    
    pub async fn remove_node(&self, node_id: RaftNodeId) -> Result<()> {
        let raft_id = node_id.0.parse::<u64>().unwrap_or(0);
        
        // Get current membership and remove node
        let metrics = self.raft.metrics();
        let mut members = metrics.borrow().membership_config.members.clone();
        members.remove(&raft_id);
        
        self.raft.change_membership(members).await?;
        self.network.remove_peer(raft_id).await;
        Ok(())
    }
}