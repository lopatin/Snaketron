use std::time::Duration;
use common::{GameCommandMessage, GameEventMessage, GameEngine, GameState, GameCommand, GameEvent};
use anyhow::Result;
use tokio::sync::{broadcast, mpsc, oneshot};
use std::sync::Arc;
use crate::game_broker::GameMessageBroker;
use crate::replica_manager::{ReplicaManager, ReplicationCommand};
use tracing::{debug, info, warn, error};
use crate::raft::{ClientRequest, RaftNode};

pub struct GameManager {
    broker: Arc<dyn GameMessageBroker>,
    replica_manager: Option<Arc<ReplicaManager>>,
    server_id: String,
}

impl GameManager {
    pub fn new(broker: Arc<dyn GameMessageBroker>, server_id: String) -> Self {
        GameManager {
            broker,
            replica_manager: None,
            server_id,
        }
    }
    
    pub fn with_replica_manager(mut self, replica_manager: Arc<ReplicaManager>) -> Self {
        self.replica_manager = Some(replica_manager);
        self
    }

    pub async fn start_game(&mut self, id: u32) -> Result<()> {
        info!("GameManager: start_game called for game {}", id);
        
        // // Check if game channels already exist (game is actually running)
        // // We need to try subscribing to see if channels exist
        // match self.broker.subscribe_events(id).await {
        //     Ok(_) => {
        //         info!("GameManager: Game {} already has channels, skipping start", id);
        //         return Ok(()); // Game already running
        //     }
        //     Err(_) => {
        //         info!("GameManager: Game {} channels don't exist, starting game", id);
        //         // Continue with starting the game
        //     }
        // }
        
        // Create the game engine
        let start_ms = chrono::Utc::now().timestamp_millis();
        let rng_seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let game = GameEngine::new_with_seed(id, start_ms, rng_seed);
        info!("GameManager: Created game engine for game {}", id);
        
        // For GameBroker, create game channels (registers in DB) and get snapshot receiver
        let snapshot_rx = if let Some(game_broker) = self.broker.as_any().downcast_ref::<crate::game_broker::GameBroker>() {
            let (_, snapshot_rx) = game_broker.create_game_channels(id).await?;
            snapshot_rx
        } else {
            // For other broker implementations, create a dummy channel
            let (_, snapshot_rx) = mpsc::channel(32);
            snapshot_rx
        };
        
        // Subscribe to commands through broker
        let command_rx = self.broker.subscribe_commands(id).await?;
        let event_broker = self.broker.clone();
        let replica_manager = self.replica_manager.clone();
        let server_id = self.server_id.clone();
        
        // Spawn the game loop
        info!("GameManager: Spawning game loop for game {}", id);
        tokio::spawn(async move {
            info!("GameManager: Game loop started for game {}", id);
            Self::run_game_loop(id, game, command_rx, event_broker, snapshot_rx, replica_manager, server_id).await;
        });
        
        Ok(())
    }
    
    async fn run_game_loop(
        game_id: u32,
        raft: &RaftNode,
        mut engine: GameEngine,
        mut cmd_rx: broadcast::Receiver<GameCommandMessage>,
        event_broker: Arc<dyn GameMessageBroker>,
        mut snapshot_rx: mpsc::Receiver<oneshot::Sender<GameState>>,
        replica_manager: Option<Arc<ReplicaManager>>,
        server_id: String,
    ) {
        info!("GameManager: run_game_loop called for game {}", game_id);
        
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Run game ticks
                    let now_ms = chrono::Utc::now().timestamp_millis();
                    match engine.run_until(now_ms) {
                        Ok(events) => {
                            for event in events {
                                let event_msg = GameEventMessage {
                                    game_id,
                                    tick: engine.current_tick(),
                                    user_id: None,
                                    event,
                                };
                                
                                if let Err(e) = raft.propose(ClientRequest::ProcessGameEvent(event_msg.clone())).await
                                        .expect("Failed to propose game event") {
                                    warn!(game_id, error = %e, "Failed to publish game event");
                                } else {
                                    debug!(game_id, "Published game event: {:?}", event_msg);
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Error running game tick: {:?}", e);
                        }
                    }
                }
            }
        }
    }

    // pub async fn join_game(
    //     &self,
    //     game_id: u32,
    // ) -> Result<(mpsc::Sender<GameCommandMessage>, broadcast::Receiver<GameEventMessage>)> {
    //     let handle = self.broker.join_game(game_id).await?;
    //     Ok((handle.command_tx, handle.event_rx))
    // }
    
    pub async fn get_game_snapshot(&self, game_id: u32) -> Result<GameState> {
        // Use the unified game handle for snapshot requests
        let handle = self.broker.join_game(game_id).await?;
        
        let (response_tx, response_rx) = oneshot::channel();
        handle.snapshot_tx.send(response_tx).await
            .map_err(|_| anyhow::anyhow!("Failed to request snapshot"))?;
        
        response_rx.await
            .map_err(|_| anyhow::anyhow!("Failed to receive snapshot"))
    }
    
    // Methods for Raft integration
    
    pub async fn stop_game(&self, game_id: u32) -> Result<()> {
        // TODO: Implement graceful game shutdown
        info!("Stopping game {}", game_id);
        Ok(())
    }
    
    pub async fn is_authority_for(&self, game_id: u32) -> bool {
        // Check if we have active game channels for this game
        self.broker.subscribe_events(game_id).await.is_ok()
    }
    
    pub async fn accept_authority_transfer(&self, game_id: u32, _state: GameState) -> Result<()> {
        info!("Accepting authority for game {}", game_id);
        // For now, just start the game normally
        // TODO: Implement starting game with given state
        Ok(())
    }
    
    pub async fn release_authority(&self, game_id: u32) -> Result<()> {
        info!("Releasing authority for game {}", game_id);
        self.stop_game(game_id).await
    }
}
