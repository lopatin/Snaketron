use async_raft::{
    storage::{CurrentSnapshotData, HardState, InitialState},
    raft::{Entry, EntryPayload, MembershipConfig},
    RaftStorage,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};
use anyhow::{Context, Result};

use crate::game_manager::GameManager;
use tokio::sync::RwLock as TokioRwLock;
use crate::replica_manager::{ReplicaManager, ReplicationCommand};
use super::types::{ClientRequest, ClientResponse, RaftNodeId};
use super::state_machine::GameStateMachine;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GameSnapshot {
    pub index: u64,
    pub term: u64,
    pub membership: MembershipConfig,
    pub data: Vec<u8>,
}

#[derive(Clone)]
pub struct GameRaftStorage {
    node_id: RaftNodeId,
    
    // Raft persistent state
    current_term: Arc<RwLock<u64>>,
    voted_for: Arc<RwLock<Option<u64>>>,
    log: Arc<RwLock<BTreeMap<u64, Entry<ClientRequest>>>>,
    
    // Raft snapshot state
    snapshot: Arc<RwLock<Option<GameSnapshot>>>,
    
    // Application state machine
    state_machine: Arc<RwLock<GameStateMachine>>,
    
    // Membership
    membership: Arc<RwLock<MembershipConfig>>,
}

impl GameRaftStorage {
    pub fn new(
        node_id: RaftNodeId,
        game_manager: Arc<TokioRwLock<GameManager>>,
        replica_manager: Arc<ReplicaManager>,
    ) -> Self {
        let state_machine = Arc::new(RwLock::new(GameStateMachine::new(
            node_id.clone(),
            game_manager,
            replica_manager,
        )));
        
        // Parse node_id to u64 for Raft
        let raft_node_id = node_id.0.parse::<u64>().unwrap_or(0);
        
        Self {
            node_id,
            current_term: Arc::new(RwLock::new(0)),
            voted_for: Arc::new(RwLock::new(None)),
            log: Arc::new(RwLock::new(BTreeMap::new())),
            snapshot: Arc::new(RwLock::new(None)),
            state_machine,
            membership: Arc::new(RwLock::new(MembershipConfig::new_initial(raft_node_id))),
        }
    }
}

#[async_trait]
impl RaftStorage<ClientRequest, ClientResponse> for GameRaftStorage {
    type Snapshot = std::io::Cursor<Vec<u8>>;
    type ShutdownError = std::io::Error;

    async fn get_membership_config(&self) -> Result<MembershipConfig> {
        Ok(self.membership.read().await.clone())
    }

    async fn get_initial_state(&self) -> Result<InitialState> {
        let term = *self.current_term.read().await;
        let voted_for = self.voted_for.read().await.clone();
        let log = self.log.read().await;
        
        let (last_log_index, last_log_term) = log
            .values()
            .last()
            .map(|e| (e.index, e.term))
            .unwrap_or((0, 0));
        
        let last_applied_log = self.state_machine.read().await.last_applied_log()
            .unwrap_or(0);
        
        let membership = self.membership.read().await.clone();
        
        debug!(
            "Getting initial state: term={}, last_log=({}, {}), last_applied={}",
            term, last_log_index, last_log_term, last_applied_log
        );
        
        Ok(InitialState {
            last_log_index,
            last_log_term,
            last_applied_log,
            hard_state: HardState {
                current_term: term,
                voted_for,
            },
            membership,
        })
    }

    async fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        *self.current_term.write().await = hs.current_term;
        *self.voted_for.write().await = hs.voted_for.clone();
        debug!("Saved hard state: term={}, voted_for={:?}", hs.current_term, hs.voted_for);
        Ok(())
    }

    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<ClientRequest>>> {
        let log = self.log.read().await;
        let entries: Vec<_> = log
            .range(start..stop)
            .map(|(_, entry)| entry.clone())
            .collect();
        
        debug!("Getting log entries from {} to {}: {} entries", start, stop, entries.len());
        Ok(entries)
    }

    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> Result<()> {
        let mut log = self.log.write().await;
        
        let stop = stop.unwrap_or(u64::MAX);
        let keys_to_remove: Vec<_> = log
            .range(start..stop)
            .map(|(k, _)| *k)
            .collect();
        
        let num_removed = keys_to_remove.len();
        for key in keys_to_remove {
            log.remove(&key);
        }
        
        debug!("Deleted {} log entries from index {}", num_removed, start);
        Ok(())
    }

    async fn append_entry_to_log(&self, entry: &Entry<ClientRequest>) -> Result<()> {
        let mut log = self.log.write().await;
        log.insert(entry.index, entry.clone());
        debug!("Appended entry at index {}: {:?}", entry.index, entry.payload);
        Ok(())
    }

    async fn replicate_to_log(&self, entries: &[Entry<ClientRequest>]) -> Result<()> {
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.index, entry.clone());
        }
        debug!("Replicated {} entries to log", entries.len());
        Ok(())
    }

    async fn apply_entry_to_state_machine(
        &self,
        index: &u64,
        data: &ClientRequest,
    ) -> Result<ClientResponse> {
        let mut sm = self.state_machine.write().await;
        let response = sm.apply(index, data).await
            .context("Failed to apply entry")?;
        
        debug!("Applied entry at index {} to state machine", index);
        Ok(response)
    }

    async fn replicate_to_state_machine(&self, entries: &[(&u64, &ClientRequest)]) -> Result<()> {
        let mut sm = self.state_machine.write().await;
        for (index, data) in entries {
            sm.apply(index, data).await
                .context("Failed to apply entry")?;
        }
        debug!("Replicated {} entries to state machine", entries.len());
        Ok(())
    }

    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<Self::Snapshot>> {
        let sm = self.state_machine.read().await;
        let (last_applied_log, snapshot_data) = sm.take_snapshot().await
            .context("Failed to create snapshot")?;
        
        let membership = self.membership.read().await.clone();
        
        let last_log = self.log.read().await
            .values()
            .last()
            .map(|e| (e.index, e.term))
            .unwrap_or((0, 0));
        
        let snapshot = GameSnapshot {
            index: last_log.0,
            term: last_log.1,
            membership: membership.clone(),
            data: snapshot_data,
        };
        
        *self.snapshot.write().await = Some(snapshot.clone());
        
        info!("Created snapshot at index {}", last_log.0);
        
        Ok(CurrentSnapshotData {
            term: snapshot.term,
            index: snapshot.index,
            membership: snapshot.membership,
            snapshot: Box::new(std::io::Cursor::new(snapshot.data)),
        })
    }

    async fn create_snapshot(&self) -> Result<(String, Box<Self::Snapshot>)> {
        let snapshot_data = self.do_log_compaction().await?;
        let id = format!("{}-{}-{}", self.node_id.0, snapshot_data.term, snapshot_data.index);
        Ok((id, snapshot_data.snapshot))
    }

    async fn finalize_snapshot_installation(
        &self,
        index: u64,
        term: u64,
        delete_through: Option<u64>,
        id: String,
        snapshot: Box<Self::Snapshot>,
    ) -> Result<()> {
        let mut data = Vec::new();
        let mut cursor = snapshot;
        std::io::Read::read_to_end(&mut *cursor, &mut data)
            .context("Failed to read snapshot")?;
        
        // Get current membership to preserve it
        let membership = self.membership.read().await.clone();
        
        let snapshot = GameSnapshot {
            index,
            term,
            membership,
            data,
        };
        
        // Restore state machine from snapshot
        let mut sm = self.state_machine.write().await;
        sm.restore_snapshot(&snapshot.data).await
            .context("Failed to restore snapshot")?;
        
        *self.snapshot.write().await = Some(snapshot);
        
        // Delete old log entries
        if let Some(through) = delete_through {
            self.delete_logs_from(0, Some(through + 1)).await?;
        }
        
        info!("Finalized snapshot installation at index {}", index);
        Ok(())
    }

    async fn get_current_snapshot(&self) -> Result<Option<CurrentSnapshotData<Self::Snapshot>>> {
        let snapshot = self.snapshot.read().await;
        
        Ok(snapshot.as_ref().map(|s| CurrentSnapshotData {
            term: s.term,
            index: s.index,
            membership: s.membership.clone(),
            snapshot: Box::new(std::io::Cursor::new(s.data.clone())),
        }))
    }
}