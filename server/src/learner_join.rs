use anyhow::{anyhow, Context, Result};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tonic::{Request, Status};
use tracing::{debug, error, info, warn};
use crate::game_relay::{Empty, JoinAsLearnerRequest, PromotionRequest};
use crate::game_relay::game_relay_client::GameRelayClient;
use crate::raft::RaftNode;

/// Represents the leader status of a node
#[derive(Debug, Clone)]
pub enum LeaderStatus {
    IsLeader,
    Follower(String), // Contains leader address
    Unknown,
}

/// Service for discovering the current Raft leader
pub struct LeaderDiscovery {
    peers: Vec<String>,
    grpc_timeout: Duration,
}

impl LeaderDiscovery {
    pub fn new(peers: Vec<String>) -> Self {
        Self {
            peers,
            grpc_timeout: Duration::from_secs(5),
        }
    }
    
    /// Find the current leader in the cluster
    pub async fn find_leader(&self) -> Result<(String, String)> {
        info!("Starting leader discovery among {} peers", self.peers.len());
        
        for peer in &self.peers {
            debug!("Checking peer: {}", peer);
            match self.check_peer_status(peer).await {
                Ok(LeaderStatus::IsLeader) => {
                    info!("Found leader at {}", peer);
                    return Ok((peer.clone(), peer.clone()));
                }
                Ok(LeaderStatus::Follower(leader_addr)) => {
                    info!("Peer {} reports leader at {}", peer, leader_addr);
                    return Ok((leader_addr, peer.clone()));
                }
                Ok(LeaderStatus::Unknown) => {
                    warn!("Peer {} status unknown, continuing", peer);
                }
                Err(e) => {
                    warn!("Failed to contact peer {}: {}", peer, e);
                }
            }
        }
        
        Err(anyhow!("No leader found in cluster"))
    }
    
    async fn check_peer_status(&self, peer: &str) -> Result<LeaderStatus> {
        let mut client = tokio::time::timeout(
            self.grpc_timeout,
            GameRelayClient::connect(format!("http://{}", peer))
        )
        .await
        .context("Connection timeout")?
        .context("Failed to connect")?;
        
        let response = tokio::time::timeout(
            self.grpc_timeout,
            client.get_raft_status(Request::new(Empty {}))
        )
        .await
        .context("Request timeout")?
        .context("Failed to get Raft status")?;
        
        let status = response.into_inner();
        
        if status.is_leader {
            Ok(LeaderStatus::IsLeader)
        } else if !status.leader_addr.is_empty() {
            Ok(LeaderStatus::Follower(status.leader_addr))
        } else {
            Ok(LeaderStatus::Unknown)
        }
    }
}

/// Protocol for joining a Raft cluster as a learner
pub struct LearnerJoinProtocol {
    node_id: String,
    grpc_addr: String,
    raft_node: Arc<RaftNode>,
}

impl LearnerJoinProtocol {
    pub fn new(node_id: String, grpc_addr: String, raft_node: Arc<RaftNode>) -> Self {
        Self {
            node_id,
            grpc_addr,
            raft_node,
        }
    }
    
    /// Execute the full join protocol
    pub async fn execute_join(&self, peers: Vec<String>) -> Result<()> {
        info!("Executing learner join protocol for node {}", self.node_id);

        // Phase 1: Discover leader
        let discovery = LeaderDiscovery::new(peers);
        let (leader_addr, _) = discovery.find_leader().await
            .context("Failed to discover leader")?;
        
        // Phase 2: Request to join as learner
        info!("Requesting to join cluster as learner from leader at {}", leader_addr);
        let mut client = GameRelayClient::connect(format!("http://{}", leader_addr)).await
            .context("Failed to connect to leader")?;
        
        let request = JoinAsLearnerRequest {
            node_id: self.node_id.clone(),
            grpc_addr: self.grpc_addr.clone(),
        };
        
        let response = client.request_join_as_learner(Request::new(request)).await
            .context("Failed to send join request")?;
        
        let join_response = response.into_inner();
        if !join_response.accepted {
            return Err(anyhow!("Join request rejected: {}", join_response.reason));
        }
        
        info!("Join request accepted, waiting for synchronization");
        
        // Phase 3: Wait for synchronization
        self.wait_for_sync().await
            .context("Failed to synchronize with cluster")?;
        
        // Phase 4: Request promotion
        info!("Requesting promotion to voting member");
        self.request_promotion(&leader_addr).await
            .context("Failed to get promoted")?;
        
        info!("Successfully joined cluster as voting member");
        Ok(())
    }
    
    async fn wait_for_sync(&self) -> Result<()> {
        let timeout = Duration::from_secs(60);
        let start = Instant::now();
        let check_interval = Duration::from_secs(1);
        
        loop {
            let metrics = self.raft_node.metrics.read().await;
            let last_log = metrics.last_log_index;
            let last_applied = metrics.last_applied;
            let lag = last_log.saturating_sub(last_applied);
            
            debug!("Sync progress: last_log={}, last_applied={}, lag={}", 
                   last_log, last_applied, lag);
            
            if lag < 10 {
                info!("Node synchronized with cluster (lag: {})", lag);
                return Ok(());
            }
            
            if start.elapsed() > timeout {
                return Err(anyhow!("Synchronization timeout after {:?}", timeout));
            }
            
            sleep(check_interval).await;
        }
    }
    
    async fn request_promotion(&self, leader_addr: &str) -> Result<()> {
        let mut client = GameRelayClient::connect(format!("http://{}", leader_addr)).await
            .context("Failed to connect to leader")?;
        
        let request = PromotionRequest {
            node_id: self.node_id.clone(),
        };
        
        let response = client.request_promotion(Request::new(request)).await
            .context("Failed to send promotion request")?;
        
        let promo_response = response.into_inner();
        if !promo_response.promoted {
            return Err(anyhow!("Promotion rejected: {}", promo_response.reason));
        }
        
        Ok(())
    }
}