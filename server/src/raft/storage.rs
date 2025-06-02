use async_raft::{storage::{CurrentSnapshotData, HardState, InitialState}, raft::{Entry, EntryPayload, MembershipConfig}, RaftStorage, NodeId};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::io;
use std::io::Cursor;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use tokio::sync::{RwLock, broadcast};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};
use tracing::{debug, error, info};
use anyhow::{Context, Result};

use tokio::sync::RwLock as TokioRwLock;
use super::types::{ClientRequest, ClientResponse, StateChangeEvent};
use super::state_machine::{GameStateMachine};

const ERR_INCONSISTENT_LOG: &str = "a query was received which was expecting data to be in place which does not exist in the log";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StateMachineSnapshot {
    /// The last index covered by this snapshot.
    pub index: u64,
    /// The term of the last index covered by this snapshot.
    pub term: u64,
    /// The last membership config included in this snapshot.
    pub membership: MembershipConfig,
    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

pub struct GameRaftStorage {
    /// The node ID of this Raft node.
    node_id: NodeId,
    
    /// Raft persistent state
    log: RwLock<BTreeMap<u64, Entry<ClientRequest>>>,

    /// The current hard state.
    hard_state: RwLock<Option<HardState>>,
    
    /// Application state machine
    state_machine: RwLock<GameStateMachine>,

    /// The current snapshot.
    current_snapshot: RwLock<Option<StateMachineSnapshot>>,

    /// Channel for broadcasting state change events
    event_tx: broadcast::Sender<StateChangeEvent>,
}

impl GameRaftStorage {
    pub fn new(node_id: NodeId, event_tx: broadcast::Sender<StateChangeEvent>) -> Self {
        Self {
            node_id,
            log: RwLock::new(BTreeMap::new()),
            hard_state: RwLock::new(None),
            state_machine: RwLock::new(GameStateMachine::new(node_id.clone())),
            current_snapshot: RwLock::new(None),
            event_tx,
        }
    }
}

#[async_trait]
impl RaftStorage<ClientRequest, ClientResponse> for GameRaftStorage {
    type Snapshot = Cursor<Vec<u8>>;
    type ShutdownError = std::io::Error;

    async fn get_membership_config(&self) -> Result<MembershipConfig> {
        let log = self.log.read().await;
        let cfg_opt = log.values().rev().find_map(|entry| match &entry.payload {
            EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
            EntryPayload::SnapshotPointer(snap) => Some(snap.membership.clone()),
            _ => None,
        });
        Ok(match cfg_opt {
            Some(cfg) => cfg,
            None => MembershipConfig::new_initial(self.node_id),
        })
    }

    async fn get_initial_state(&self) -> Result<InitialState> {
        let membership = self.get_membership_config().await?;
        let mut hs = self.hard_state.write().await;
        let log = self.log.read().await;
        let sm = self.state_machine.read().await;
        match &mut *hs {
            Some(inner) => {
                let (last_log_index, last_log_term) = match log.values().rev().next() {
                    Some(log) => (log.index, log.term),
                    None => (0, 0),
                };
                let last_applied_log = sm.last_applied_log();
                Ok(InitialState {
                    last_log_index,
                    last_log_term,
                    last_applied_log,
                    hard_state: inner.clone(),
                    membership,
                })
            }
            None => {
                let new = InitialState::new_initial(self.node_id);
                *hs = Some(new.hard_state.clone());
                Ok(new)
            }
        }
    }

    async fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        *self.hard_state.write().await = Some(hs.clone());
        Ok(())
    }

    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<ClientRequest>>> {
        // Invalid request, return empty vec.
        if start > stop {
            error!("invalid request, start > stop");
            return Ok(vec![]);
        }
        let log = self.log.read().await;
        Ok(log.range(start..stop).map(|(_, val)| val.clone()).collect())
    }

    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> Result<()> {
        if stop.as_ref().map(|stop| &start > stop).unwrap_or(false) {
            tracing::error!("invalid request, start > stop");
            return Ok(());
        }
        let mut log = self.log.write().await;

        // If a stop point was specified, delete from start until the given stop point.
        if let Some(stop) = stop.as_ref() {
            for key in start..*stop {
                log.remove(&key);
            }
            return Ok(());
        }
        // Else, just split off the remainder.
        log.split_off(&start);
        Ok(())
    }

    async fn append_entry_to_log(&self, entry: &Entry<ClientRequest>) -> Result<()> {
        let mut log = self.log.write().await;
        log.insert(entry.index, entry.clone());
        Ok(())
    }

    async fn replicate_to_log(&self, entries: &[Entry<ClientRequest>]) -> Result<()> {
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.index, entry.clone());
        }
        Ok(())
    }

    async fn apply_entry_to_state_machine(
        &self,
        index: &u64,
        data: &ClientRequest,
    ) -> Result<ClientResponse> {
        let mut sm = self.state_machine.write().await;
        let mut events_out: Vec<StateChangeEvent> = Vec::new();
        let (rsp, events) = sm
            .apply(index, data, Some(&mut events_out)).await
            .context("Failed to apply entry to state machine")?;
        
        // Emit state change events
        for event in events {
            self.event_tx.send(event)
                .map_err(|_| anyhow::anyhow!("Failed to send state change event"))?;
        }
        
        Ok(rsp)
    }

    async fn replicate_to_state_machine(&self, entries: &[(&u64, &ClientRequest)]) -> Result<()> {
        let mut sm = self.state_machine.write().await;
        let mut events_out: Vec<StateChangeEvent> = Vec::new();
        
        for (index, data) in entries {
            sm.apply(index, data, Some(&mut events_out)).await
                .context("Failed to replicate entry to state machine")?;
        }
        
        // Emit state change events
        for event in events_out {
            self.event_tx.send(event)
                .map_err(|_| anyhow::anyhow!("Failed to send state change event"))?;
        }
        
        Ok(())
    }

    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<Self::Snapshot>> {

        let (data, last_applied_log);
        {
            // Serialize the data of the state machine with bincode
            let sm = self.state_machine.read().await;
            let sm_bytes: Vec<u8> = bincode::serde::encode_to_vec(&sm, bincode::config::standard())
                .context("Failed to serialize state machine")?;
            data = bincode::serde::encode_to_vec(&sm, bincode::config::standard())
                .context("Failed to serialize state machine snapshot")?;
            last_applied_log = sm.last_applied_log;
        } // Release state machine read lock.

        let membership_config;
        {
            // Go backwards through the log to find the most recent membership config <= the `through` index.
            let log = self.log.read().await;
            membership_config = log
                .values()
                .rev()
                .skip_while(|entry| entry.index > last_applied_log)
                .find_map(|entry| match &entry.payload {
                    EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| MembershipConfig::new_initial(self.node_id));
        } // Release log read lock.

        let snapshot_bytes: Vec<u8>;
        let term;
        {
            let mut log = self.log.write().await;
            let mut current_snapshot = self.current_snapshot.write().await;
            term = log
                .get(&last_applied_log)
                .map(|entry| entry.term)
                .ok_or_else(|| anyhow::anyhow!(ERR_INCONSISTENT_LOG))?;
            *log = log.split_off(&last_applied_log);
            log.insert(
                last_applied_log,
                Entry::new_snapshot_pointer(last_applied_log, term, "".into(), membership_config.clone()),
            );

            let snapshot = StateMachineSnapshot {
                index: last_applied_log,
                term,
                membership: membership_config.clone(),
                data,
            };
            snapshot_bytes = bincode::serde::encode_to_vec(
                &snapshot,
                bincode::config::standard()
            ).context("Failed to serialize snapshot")?;

            *current_snapshot = Some(snapshot);
        } // Release log & snapshot write locks.

        Ok(CurrentSnapshotData {
            term,
            index: last_applied_log,
            membership: membership_config.clone(),
            snapshot: Box::new(Cursor::new(snapshot_bytes))
        })
    }

    async fn create_snapshot(&self) -> Result<(String, Box<Self::Snapshot>)> {
        let snapshot_data = self.do_log_compaction().await?;
        let id = format!("{}-{}-{}", self.node_id, snapshot_data.term, snapshot_data.index);
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
        // Read the serialized snapshot data
        let mut data = Vec::new();
        let mut snapshot = snapshot;
        tokio::io::AsyncReadExt::read_to_end(&mut *snapshot, &mut data).await
            .context("Failed to read snapshot")?;

        // Deserialize the state machine snapshot
        let (new_snapshot, _): (StateMachineSnapshot, _) =
            bincode::serde::decode_from_slice(&data, bincode::config::standard())
            .context("Failed to deserialize snapshot")?;

        // Update log.
        {
            // Go backwards through the log to find the most recent membership config <= the `through` index.
            let mut log = self.log.write().await;
            let membership_config = log
                .values()
                .rev()
                .skip_while(|entry| entry.index > index)
                .find_map(|entry| match &entry.payload {
                    EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| MembershipConfig::new_initial(self.node_id));

            match &delete_through {
                Some(through) => {
                    *log = log.split_off(&(through + 1));
                }
                None => log.clear(),
            }
            log.insert(index, Entry::new_snapshot_pointer(index, term, id, membership_config));
        }

        // Update the state machine.
        {
            let new_sm: GameStateMachine = bincode::serde::decode_from_slice(
                &new_snapshot.data, 
                bincode::config::standard()
            ).context("Failed to deserialize state machine from snapshot")?.0;
            let mut sm = self.state_machine.write().await;
            *sm = new_sm;
        }

        // Update the current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    async fn get_current_snapshot(&self) -> Result<Option<CurrentSnapshotData<Self::Snapshot>>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let bytes = bincode::serde::encode_to_vec(snapshot, bincode::config::standard())?;
                Ok(Some(CurrentSnapshotData {
                    index: snapshot.index,
                    term: snapshot.term,
                    membership: snapshot.membership.clone(),
                    snapshot: Box::new(Cursor::new(bytes)),
                }))
            }
            None => Ok(None),
        }
    }
}