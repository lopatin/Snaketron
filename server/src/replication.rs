use anyhow::{Result, Context};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{info, error, warn, debug};
use common::{GameState, GameEvent, GameEventMessage};
use crate::pubsub_manager::PubSubManager;
use crate::game_executor::PARTITION_COUNT;

/// In-memory game state storage
pub type GameStateStore = Arc<RwLock<HashMap<u32, GameState>>>;

/// Game event broadcast channels
pub type GameEventBroadcasters = Arc<RwLock<HashMap<u32, broadcast::Sender<GameEventMessage>>>>;

/// Tracks the replication status
#[derive(Debug, Clone)]
pub struct ReplicationStatus {
    pub partition_id: u32,
    pub is_ready: bool,
}

/// A wrapper around broadcast::Receiver that filters out events before a certain sequence number
pub struct FilteredEventReceiver {
    inner: broadcast::Receiver<GameEventMessage>,
    min_sequence: u64,
    game_id: u32,
}

impl FilteredEventReceiver {
    /// Create a new FilteredEventReceiver
    pub fn new(inner: broadcast::Receiver<GameEventMessage>, min_sequence: u64, game_id: u32) -> Self {
        Self {
            inner,
            min_sequence,
            game_id,
        }
    }

    /// Receive the next event that passes the filter
    pub async fn recv(&mut self) -> Result<GameEventMessage, broadcast::error::RecvError> {
        loop {
            let event = match self.inner.recv().await {
                Ok(event) => event,
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!("Broadcast receiver for game {} lagged and skipped {} messages - receiver too slow!",
                        self.game_id, skipped);
                    continue;
                }
                Err(e) => return Err(e),
            };

            // Only forward events after our snapshot sequence
            if event.sequence > self.min_sequence {
                debug!("Forwarding event for game {} with sequence {} (min_sequence: {})",
                    self.game_id, event.sequence, self.min_sequence);
                return Ok(event);
            } else {
                debug!("Filtering out stale event for game {} with sequence {} (min_sequence: {})",
                    self.game_id, event.sequence, self.min_sequence);
                // Continue to next event
            }
        }
    }
}

/// PartitionReplica subscribes to partition events via PubSub and maintains game states
pub struct PartitionReplica {
    partition_id: u32,
    pubsub: Arc<Mutex<PubSubManager>>,
    game_states: GameStateStore,
    game_event_broadcasters: GameEventBroadcasters,
    status: Arc<RwLock<ReplicationStatus>>,
    cancellation_token: CancellationToken,
}

impl PartitionReplica {
    pub fn new(
        partition_id: u32,
        pubsub: Arc<Mutex<PubSubManager>>,
        game_states: GameStateStore,
        game_event_broadcasters: GameEventBroadcasters,
        cancellation_token: CancellationToken,
    ) -> Self {
        let status = Arc::new(RwLock::new(ReplicationStatus {
            partition_id,
            is_ready: true, // With PubSub, we're immediately ready
        }));

        Self {
            partition_id,
            pubsub,
            game_states,
            game_event_broadcasters,
            status,
            cancellation_token,
        }
    }

    /// Get the current replication status
    pub fn status(&self) -> Arc<RwLock<ReplicationStatus>> {
        self.status.clone()
    }
    
    /// Process a game event and update the game state
    async fn process_event(&self, event_msg: GameEventMessage) -> Result<()> {
        let game_id = event_msg.game_id;
        debug!("Processing game event for game {} in partition {}", game_id, self.partition_id);
        
        match &event_msg.event {
            GameEvent::Snapshot { game_state } => {
                info!("Received snapshot for game {} at tick {}", game_id, event_msg.tick);
                // Always update with the latest snapshot
                let mut states = self.game_states.write().await;
                states.insert(game_id, game_state.clone());
            }
            _ => {
                let mut states = self.game_states.write().await;
                if let Some(game_state) = states.get_mut(&game_id) {
                    // Tick forward until we reach the event's tick
                    if event_msg.tick > game_state.tick {
                        if let Err(e) = game_state.tick_forward(true) {
                            error!("Error during tick_forward: {:?}", e);
                        }
                    }

                    // Apply event to game state
                    game_state.apply_event(event_msg.event.clone(), None);
                    debug!("Applied event to game {} state: {:?}", game_id, event_msg.event);
                } else {
                    warn!("Received event for unknown game {}", game_id);
                }                       
            }
        }
        
        // Broadcast the event to any local subscribers
        {
            let broadcasters = self.game_event_broadcasters.read().await;
            if let Some(sender) = broadcasters.get(&game_id) {
                match sender.send(event_msg.clone()) {
                    Ok(receiver_count) => {
                        if receiver_count == 0 {
                            debug!("No receivers for game {} broadcast", game_id);
                        }
                    }
                    Err(_) => {
                        // This shouldn't happen with broadcast channels, but log if it does
                        warn!("Failed to broadcast event for game {} - channel may be closed", game_id);
                    }
                }
            }
        }
        
        Ok(())
    }


    /// Run the replication worker
    pub async fn run(self) -> Result<()> {
        info!("Starting replication worker for partition {}", self.partition_id);
        
        // Subscribe to the partition
        let mut pubsub = self.pubsub.lock().await;
        let subscription = pubsub.subscribe_to_partition(self.partition_id).await?;

        // Request initial snapshots for this partition
        pubsub.request_partition_snapshots(self.partition_id).await?;
        drop(pubsub); // Release lock

        // Destructure subscription so each receiver can be borrowed independently in select!
        let crate::pubsub_manager::PartitionSubscription {
            partition_id: _,
            mut event_receiver,
            mut command_receiver,
            mut snapshot_request_receiver,
        } = subscription;

        // Mark as ready immediately (no catch-up needed with PubSub)
        self.status.write().await.is_ready = true;

        // Main event processing loop
        loop {
            tokio::select! {
                biased;

                _ = self.cancellation_token.cancelled() => {
                    info!("Replication worker for partition {} shutting down", self.partition_id);
                    break;
                }

                // Process events from partition subscription
                event = event_receiver.recv() => {
                    match event {
                        Some(event) => {
                            if let Err(e) = self.process_event(event).await {
                                error!("Failed to process event in partition {}: {}", self.partition_id, e);
                            }
                        }
                        None => {
                            error!("Partition {} subscription closed unexpectedly, replication worker exiting",
                                self.partition_id);
                            break;
                        }
                    }
                }

                // Drain commands (processed by game executor, not used here)
                Some(_) = command_receiver.recv() => {
                    // Commands are handled by the game executor, we just drain them
                    // to prevent the channel from filling up and blocking the PubSub handler
                }

                // Drain snapshot requests (processed by game executor, not used here)
                Some(_) = snapshot_request_receiver.recv() => {
                    // Snapshot requests are handled by the game executor, we just drain them
                    // to prevent the channel from filling up and blocking the PubSub handler
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
    game_event_broadcasters: GameEventBroadcasters,
    statuses: Arc<RwLock<HashMap<u32, Arc<RwLock<ReplicationStatus>>>>>,
    pubsub: Arc<Mutex<PubSubManager>>,
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
            .filter(|(game_id, _)| *game_id % PARTITION_COUNT == partition_id)
            .map(|(id, state)| (*id, state.clone()))
            .collect()
    }
    
    async fn is_ready(&self) -> bool {
        // With PubSub, we're always ready
        true
    }
}

impl ReplicationManager {
    /// Subscribe to game events for a specific game
    /// Returns the current game state as a snapshot and a receiver for subsequent events
    pub async fn subscribe_to_game(&self, game_id: u32) -> Result<(GameState, FilteredEventReceiver)> {
        // Get state from memory or fail
        let game_state = self.get_game_state(game_id).await
            .context("Game not available in replication manager")?;
        
        // Get or create broadcast channel for this game
        let receiver = {
            let mut broadcasters = self.game_event_broadcasters.write().await;
            let sender = broadcasters.entry(game_id)
                .or_insert_with(|| {
                    let (tx, _) = broadcast::channel(1028);
                    tx
                });
            
            sender.subscribe()
        };
        
        // Create filtered receiver
        let snapshot_sequence = game_state.event_sequence;
        let filtered_receiver = FilteredEventReceiver {
            inner: receiver,
            min_sequence: snapshot_sequence,
            game_id,
        };
        
        Ok((game_state, filtered_receiver))
    }
    
    /// Get a game state, always ready with PubSub
    pub async fn get_game_state_when_ready(&self, game_id: u32) -> Option<GameState> {
        self.get_game_state(game_id).await
    }
    
    /// Wait for a game to become available in the replication manager
    /// Returns the game state once available, or an error if timeout is reached
    pub async fn wait_for_game(&self, game_id: u32, timeout_secs: u64) -> Result<GameState> {
        use tokio::time::{timeout, Duration};
        
        let deadline = timeout(Duration::from_secs(timeout_secs), async {
            let mut backoff_ms = 10;
            const MAX_BACKOFF_MS: u64 = 500;
            
            loop {
                // Check if game is available
                if let Some(game_state) = self.get_game_state(game_id).await {
                    debug!("Game {} found in replication manager", game_id);
                    return Ok(game_state);
                }
                
                // Wait with exponential backoff
                debug!("Game {} not yet available, waiting {}ms", game_id, backoff_ms);
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                
                // Increase backoff for next iteration
                backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
            }
        });
        
        match deadline.await {
            Ok(result) => result,
            Err(_) => {
                error!("Timeout waiting for game {} to become available after {} seconds", game_id, timeout_secs);
                Err(anyhow::anyhow!("Game {} did not become available within {} seconds", game_id, timeout_secs))
            }
        }
    }
    
    /// Create and start replication workers for specified partitions
    pub async fn new(
        partitions: Vec<u32>,
        cancellation_token: CancellationToken,
        redis_url: &str,
    ) -> Result<Self> {
        let game_states = Arc::new(RwLock::new(HashMap::new()));
        let game_event_broadcasters = Arc::new(RwLock::new(HashMap::new()));
        let statuses = Arc::new(RwLock::new(HashMap::new()));
        let mut workers = Vec::new();
        
        // Create PubSub manager
        let pubsub = Arc::new(Mutex::new(PubSubManager::new(redis_url).await?));
        
        for partition_id in partitions {
            // Create worker
            let worker = PartitionReplica::new(
                partition_id,
                pubsub.clone(),
                game_states.clone(),
                game_event_broadcasters.clone(),
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
            game_event_broadcasters,
            statuses,
            pubsub,
        })
    }
    
    /// Get the shared game state store
    pub fn game_states(&self) -> GameStateStore {
        self.game_states.clone()
    }
    
    /// Check if all workers are ready (always true with PubSub)
    pub async fn is_ready(&self) -> bool {
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