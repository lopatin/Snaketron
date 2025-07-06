use anyhow::{Result, Context};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{info, error, warn, debug};
use common::{GameState, GameEvent, GameEventMessage, GameStatus};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, streams::{StreamReadOptions, StreamReadReply}};
use crate::game_executor::StreamEvent;

/// In-memory game state storage
pub type GameStateStore = Arc<RwLock<HashMap<u32, GameState>>>;

/// Tracks the replication status
#[derive(Debug, Clone)]
pub struct ReplicationStatus {
    pub partition_id: u32,
    pub is_ready: bool,
    pub last_processed_id: String,
    pub games_replicated: usize,
}

/// PartitionReplica subscribes to Redis stream partitions and maintains game states
pub struct PartitionReplica {
    partition_id: u32,
    redis_conn: ConnectionManager,
    game_states: GameStateStore,
    status: Arc<RwLock<ReplicationStatus>>,
    stream_key: String,
    cancellation_token: CancellationToken,
}

impl PartitionReplica {
    pub fn new(
        partition_id: u32,
        redis_conn: ConnectionManager,
        game_states: GameStateStore,
        cancellation_token: CancellationToken,
    ) -> Self {
        let stream_key = format!("snaketron:game-events:partition-{}", partition_id);
        let status = Arc::new(RwLock::new(ReplicationStatus {
            partition_id,
            is_ready: false,
            last_processed_id: "0-0".to_string(),
            games_replicated: 0,
        }));

        Self {
            partition_id,
            redis_conn,
            game_states,
            status,
            stream_key,
            cancellation_token,
        }
    }

    /// Get the current replication status
    pub fn status(&self) -> Arc<RwLock<ReplicationStatus>> {
        self.status.clone()
    }
    
    /// Get the current tail ID of the stream to use as a catch-up target
    async fn get_stream_tail_id(&mut self) -> Option<String> {
        // Use XINFO STREAM to get stream info including last-generated-id
        let cmd = redis::cmd("XINFO")
            .arg("STREAM")
            .arg(&self.stream_key)
            .clone();
        
        match self.redis_conn.send_packed_command(&cmd).await {
            Ok(redis::Value::Array(info)) => {
                // Parse the XINFO STREAM response to find last-generated-id
                let mut last_id = None;
                let mut i = 0;
                while i < info.len() - 1 {
                    if let redis::Value::BulkString(key) = &info[i] {
                        if key == b"last-generated-id" {
                            if let redis::Value::BulkString(id) = &info[i + 1] {
                                if let Ok(id_str) = String::from_utf8(id.clone()) {
                                    last_id = Some(id_str);
                                    break;
                                }
                            }
                        }
                    }
                    i += 2;
                }
                
                if let Some(id) = last_id {
                    info!("Partition {} will catch up to stream position: {}", 
                        self.partition_id, id);
                    Some(id)
                } else {
                    info!("Partition {} stream has no last-generated-id", self.partition_id);
                    None
                }
            }
            Ok(_) => {
                warn!("Unexpected response format from XINFO STREAM for partition {}", self.partition_id);
                None
            }
            Err(e) => {
                // Stream might not exist yet, which is fine
                debug!("Failed to get stream info for partition {}: {}. Stream might be empty.", 
                    self.partition_id, e);
                None
            }
        }
    }

    /// Process a single stream event and update game state
    async fn process_event(&self, event: StreamEvent) -> Result<()> {
        match event {
            StreamEvent::GameEvent(event_msg) => {
                let game_id = event_msg.game_id;
                let mut states = self.game_states.write().await;
                if let Some(game_state) = states.get_mut(&game_id) {
                    // Apply event to game state
                    game_state.apply_event(event_msg.event.clone(), None);
                    debug!("Applied event to game {} state: {:?}", game_id, event_msg.event);
                } else {
                    warn!("Received event for unknown game {}", game_id);
                }
            }
            StreamEvent::StatusUpdated { game_id, status } => {
                // Only process games that belong to this partition
                if game_id % 10 == self.partition_id - 1 {
                    let mut states = self.game_states.write().await;
                    if let Some(game_state) = states.get_mut(&game_id) {
                        game_state.status = status.clone();
                        debug!("Updated status for game {} to {:?}", game_id, status);
                        
                        // Remove completed games from memory after some time
                        if matches!(status, GameStatus::Complete { .. }) {
                            // In production, you might want to keep completed games for a while
                            // or move them to a different storage
                            info!("Game {} completed, removing from replication", game_id);
                            states.remove(&game_id);
                        }
                    }
                }
            }
            StreamEvent::GameCreated { .. } => {
                // Only process games that belong to this partition
                // if game_id % 10 == self.partition_id - 1 {
                //     info!("Replicating new game {} in partition {}", game_id, self.partition_id);
                //     let mut states = self.game_states.write().await;
                //     states.insert(game_id, game_state);
                // }
            }
            StreamEvent::GameCommandSubmitted { .. } => {
                // Commands are not relevant for state replication
                debug!("Ignoring GameCommandSubmitted event for replication");
            }
        }
        Ok(())
    }

    /// Catch up by reading all events from the stream
    async fn catch_up(&mut self) -> Result<()> {
        info!("Starting catch-up for partition {}", self.partition_id);
        
        // First, get the current tail of the stream to have a fixed target
        let target_id = self.get_stream_tail_id().await;
        
        let mut last_id = "0-0".to_string();
        let mut events_processed = 0;
        
        loop {
            // Read in batches
            let options = StreamReadOptions::default()
                .count(100); // Read up to 100 messages at a time
                
            let reply: StreamReadReply = self.redis_conn
                .xread_options(&[&self.stream_key], &[&last_id], &options)
                .await
                .context("Failed to read from stream during catch-up")?;
            
            let mut batch_empty = true;
            let mut reached_target = false;
            
            for stream_data in reply.keys {
                for stream_id in stream_data.ids {
                    batch_empty = false;
                    let current_id = stream_id.id.clone();
                    
                    // Parse and process the event
                    if let Some(data) = stream_id.map.get("data") {
                        if let redis::Value::BulkString(bytes) = data {
                            match serde_json::from_slice::<StreamEvent>(bytes) {
                                Ok(event) => {
                                    if let Err(e) = self.process_event(event).await {
                                        error!("Failed to process event during catch-up: {}", e);
                                    } else {
                                        // Only process games that belong to this partition
                                        events_processed += 1;
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to parse stream event during catch-up: {}", e);
                                }
                            }
                        }
                    }
                    
                    // Check if we've reached our target
                    if let Some(ref target) = target_id {
                        if &current_id == target {
                            reached_target = true;
                            info!("Partition {} reached target position during catch-up", self.partition_id);
                        }
                    }
                    
                    last_id = current_id;
                }
            }
            
            // Exit if we've reached our target or if there are no more messages
            if reached_target || batch_empty {
                break;
            }
            
            // Update status
            {
                let mut status = self.status.write().await;
                status.last_processed_id = last_id.clone();
                status.games_replicated = self.game_states.read().await.len();
            }
            
            if events_processed % 1000 == 0 {
                info!("Partition {} catch-up progress: {} events processed", 
                    self.partition_id, events_processed);
            }
        }

        Ok(())
    }

    /// Run the replication worker
    pub async fn run(mut self) -> Result<()> {
        info!("Starting replication worker for partition {}", self.partition_id);
        
        // First, catch up with existing events
        self.catch_up().await?;
        
        // Catch up again
        self.catch_up().await?;

        // Mark as ready
        self.status.write().await.is_ready = true;

        info!("Partition {} catch-up complete to last ID: {}", 
            self.partition_id, self.status.read().await.last_processed_id);

        // Then, continuously process new events
        let mut last_id = self.status.read().await.last_processed_id.clone();
        loop {
            tokio::select! {
                biased;
                
                _ = self.cancellation_token.cancelled() => {
                    info!("Replication worker for partition {} shutting down", self.partition_id);
                    break;
                }
                
                // Read new events with blocking
                stream_read = async {
                    let options = StreamReadOptions::default()
                        .count(10)
                        .block(100);
                    
                    self.redis_conn.xread_options(&[&self.stream_key], &[&last_id], &options).await
                } => {
                    match stream_read {
                        Ok(reply) => {
                            let reply: StreamReadReply = reply;
                            for stream_data in reply.keys {
                                for stream_id in stream_data.ids {
                                    last_id = stream_id.id.clone();
                                    
                                    // Parse and process the event
                                    if let Some(data) = stream_id.map.get("data") {
                                        if let redis::Value::BulkString(bytes) = data {
                                            match serde_json::from_slice::<StreamEvent>(bytes) {
                                                Ok(event) => {
                                                    if let Err(e) = self.process_event(event).await {
                                                        error!("Failed to process event: {}", e);
                                                    }
                                                }
                                                Err(e) => {
                                                    error!("Failed to parse stream event: {}", e);
                                                }
                                            }
                                        }
                                    }
                                    
                                    // Update status
                                    {
                                        let mut status = self.status.write().await;
                                        status.last_processed_id = last_id.clone();
                                        status.games_replicated = self.game_states.read().await.len();
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to read from Redis stream: {}", e);
                            // Sleep briefly before retrying
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
}

/// Manager for running multiple replicas
pub struct ReplicationManager {
    workers: Vec<tokio::task::JoinHandle<Result<()>>>,
    game_states: GameStateStore,
    statuses: Arc<RwLock<HashMap<u32, Arc<RwLock<ReplicationStatus>>>>>,
}

/// API for querying replicated game states
pub trait GameStateReader: Send + Sync {
    /// Get a game state by ID
    async fn get_game_state(&self, game_id: u32) -> Option<GameState>;
    
    /// Get all game states for a partition
    async fn get_partition_games(&self, partition_id: u32) -> Vec<(u32, GameState)>;
    
    /// Check if replication is ready
    async fn is_ready(&self) -> bool;
}

impl GameStateReader for ReplicationManager {
    async fn get_game_state(&self, game_id: u32) -> Option<GameState> {
        let states = self.game_states.read().await;
        states.get(&game_id).cloned()
    }
    
    async fn get_partition_games(&self, partition_id: u32) -> Vec<(u32, GameState)> {
        let states = self.game_states.read().await;
        states.iter()
            .filter(|(game_id, _)| *game_id % 10 == partition_id - 1)
            .map(|(id, state)| (*id, state.clone()))
            .collect()
    }
    
    async fn is_ready(&self) -> bool {
        let statuses = self.statuses.read().await;
        for (_, status) in statuses.iter() {
            let s = status.read().await;
            if !s.is_ready {
                return false;
            }
        }
        true
    }
}

impl ReplicationManager {
    /// Get a game state, waiting for replication to be ready first
    /// This is the main method that game executors should use
    pub async fn get_game_state_when_ready(&self, game_id: u32) -> Option<GameState> {
        // Wait for replication to be ready (with timeout)
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(30);
        
        while !self.is_ready().await {
            if start.elapsed() > timeout {
                warn!("Timeout waiting for replication to be ready when fetching game {}", game_id);
                return None;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        
        // Now get the game state
        self.get_game_state(game_id).await
    }
    
    /// Create and start replication workers for specified partitions
    pub async fn new(
        partitions: Vec<u32>,
        cancellation_token: CancellationToken,
        redis_conn: ConnectionManager,
    ) -> Result<Self> {
        let game_states = Arc::new(RwLock::new(HashMap::new()));
        let statuses = Arc::new(RwLock::new(HashMap::new()));
        let mut workers = Vec::new();
        
        for partition_id in partitions {
            // Create worker
            let worker = PartitionReplica::new(
                partition_id,
                redis_conn.clone(),
                game_states.clone(),
                cancellation_token.clone(),
            );
            
            // Store status reference
            {
                let mut status_map = statuses.write().await;
                status_map.insert(partition_id, worker.status());
            }
            
            // Spawn worker task
            let handle = tokio::spawn(worker.run());
            workers.push(handle);
        }
        
        Ok(Self {
            workers,
            game_states,
            statuses,
        })
    }
    
    /// Get the shared game state store
    pub fn game_states(&self) -> GameStateStore {
        self.game_states.clone()
    }
    
    /// Check if all workers are ready
    pub async fn is_ready(&self) -> bool {
        let statuses = self.statuses.read().await;
        for (_, status) in statuses.iter() {
            let s = status.read().await;
            if !s.is_ready {
                return false;
            }
        }
        true
    }
    
    /// Get status of all workers
    pub async fn get_status(&self) -> HashMap<u32, ReplicationStatus> {
        let mut result = HashMap::new();
        let statuses = self.statuses.read().await;
        for (partition_id, status) in statuses.iter() {
            let s = status.read().await;
            result.insert(*partition_id, s.clone());
        }
        result
    }
    
    /// Wait for all workers to complete
    pub async fn wait(self) -> Result<()> {
        for worker in self.workers {
            worker.await??;
        }
        Ok(())
    }
}
