use async_raft::{
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, VoteRequest, VoteResponse,
    },
    RaftNetwork,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tracing::{debug, error, warn};

use crate::game_broker::game_relay::{
    game_relay_client::GameRelayClient,
    RaftMessage as ProtoRaftMessage,
    RaftAppendEntries, RaftAppendEntriesResponse,
    RaftVoteRequest, RaftVoteResponse,
    RaftInstallSnapshot, RaftInstallSnapshotResponse,
};
use super::types::{ClientRequest, RaftNodeId};

pub struct GameRaftNetwork {
    node_id: RaftNodeId,
    connections: Arc<RwLock<HashMap<u64, GameRelayClient<Channel>>>>,
}

impl GameRaftNetwork {
    pub fn new(node_id: RaftNodeId) -> Self {
        Self {
            node_id,
            connections: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    pub async fn add_peer(&self, node_id: u64, addr: String) {
        match GameRelayClient::connect(addr.clone()).await {
            Ok(client) => {
                let mut connections = self.connections.write().await;
                connections.insert(node_id, client);
                debug!("Added Raft peer {}: {}", node_id, addr);
            }
            Err(e) => {
                error!("Failed to connect to Raft peer {} at {}: {}", node_id, addr, e);
            }
        }
    }
    
    pub async fn remove_peer(&self, node_id: u64) {
        let mut connections = self.connections.write().await;
        connections.remove(&node_id);
        debug!("Removed Raft peer {}", node_id);
    }
    
    async fn get_client(&self, target: u64) -> Option<GameRelayClient<Channel>> {
        let connections = self.connections.read().await;
        connections.get(&target).cloned()
    }
}

#[async_trait]
impl RaftNetwork<ClientRequest> for GameRaftNetwork {
    async fn append_entries(
        &self,
        target: u64,
        rpc: AppendEntriesRequest<ClientRequest>,
    ) -> anyhow::Result<AppendEntriesResponse> {
        let client = self.get_client(target).await
            .ok_or_else(|| anyhow::anyhow!("No connection to node {}", target))?;
        
        let request = ProtoRaftMessage {
            message: Some(crate::game_broker::game_relay::raft_message::Message::AppendEntries(
                RaftAppendEntries {
                    term: rpc.term,
                    leader_id: rpc.leader_id,
                    prev_log_index: rpc.prev_log_index,
                    prev_log_term: rpc.prev_log_term,
                    entries: bincode::serde::encode_to_vec(&rpc.entries, bincode::config::standard())?,
                    leader_commit: rpc.leader_commit,
                }
            )),
        };
        
        let mut client = client.clone();
        let response = client.raft_rpc(tonic::Request::new(request)).await?;
        
        if let Some(crate::game_broker::game_relay::raft_message::Message::AppendResponse(resp)) = 
            response.into_inner().message {
            Ok(AppendEntriesResponse {
                term: resp.term,
                success: resp.success,
                conflict_opt: if resp.has_conflict {
                    Some(async_raft::raft::ConflictOpt {
                        term: resp.conflict_term,
                        index: resp.conflict_index,
                    })
                } else {
                    None
                },
            })
        } else {
            Err(anyhow::anyhow!("Invalid response type"))
        }
    }

    async fn install_snapshot(
        &self,
        target: u64,
        rpc: InstallSnapshotRequest,
    ) -> anyhow::Result<InstallSnapshotResponse> {
        let client = self.get_client(target).await
            .ok_or_else(|| anyhow::anyhow!("No connection to node {}", target))?;
        
        let request = ProtoRaftMessage {
            message: Some(crate::game_broker::game_relay::raft_message::Message::InstallSnapshot(
                RaftInstallSnapshot {
                    term: rpc.term,
                    leader_id: rpc.leader_id,
                    last_included_index: rpc.last_included_index,
                    last_included_term: rpc.last_included_term,
                    offset: rpc.offset,
                    data: rpc.data,
                    done: rpc.done,
                }
            )),
        };
        
        let mut client = client.clone();
        let response = client.raft_rpc(tonic::Request::new(request)).await?;
        
        if let Some(crate::game_broker::game_relay::raft_message::Message::SnapshotResponse(resp)) = 
            response.into_inner().message {
            Ok(InstallSnapshotResponse { term: resp.term })
        } else {
            Err(anyhow::anyhow!("Invalid response type"))
        }
    }

    async fn vote(&self, target: u64, rpc: VoteRequest) -> anyhow::Result<VoteResponse> {
        let client = self.get_client(target).await
            .ok_or_else(|| anyhow::anyhow!("No connection to node {}", target))?;
        
        let request = ProtoRaftMessage {
            message: Some(crate::game_broker::game_relay::raft_message::Message::VoteRequest(
                RaftVoteRequest {
                    term: rpc.term,
                    candidate_id: rpc.candidate_id,
                    last_log_index: rpc.last_log_index,
                    last_log_term: rpc.last_log_term,
                }
            )),
        };
        
        let mut client = client.clone();
        let response = client.raft_rpc(tonic::Request::new(request)).await?;
        
        if let Some(crate::game_broker::game_relay::raft_message::Message::VoteResponse(resp)) = 
            response.into_inner().message {
            Ok(VoteResponse {
                term: resp.term,
                vote_granted: resp.vote_granted,
            })
        } else {
            Err(anyhow::anyhow!("Invalid response type"))
        }
    }
}