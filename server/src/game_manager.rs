use std::time::Duration;
use common::{GameCommandMessage, GameEventMessage, GameEngine, GameState, server_process_incoming_command, GameCommand, GameEvent};
use anyhow::Result;
use tokio::sync::{broadcast, mpsc, oneshot};
use std::sync::Arc;
use crate::game_broker::GameMessageBroker;
use crate::replica_manager::{ReplicaManager, ReplicationCommand};
use tracing::{debug, info, warn, error};


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
        
        // Check if game channels already exist (game is actually running)
        // We need to try subscribing to see if channels exist
        match self.broker.subscribe_events(id).await {
            Ok(_) => {
                info!("GameManager: Game {} already has channels, skipping start", id);
                return Ok(()); // Game already running
            }
            Err(_) => {
                info!("GameManager: Game {} channels don't exist, starting game", id);
                // Continue with starting the game
            }
        }
        
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
        mut engine: GameEngine,
        mut cmd_rx: broadcast::Receiver<GameCommandMessage>,
        event_broker: Arc<dyn GameMessageBroker>,
        mut snapshot_rx: mpsc::Receiver<oneshot::Sender<GameState>>,
        replica_manager: Option<Arc<ReplicaManager>>,
        server_id: String,
    ) {
        let mut game_version: u64 = 0;
        let mut interval = tokio::time::interval(Duration::from_millis(16)); // ~60 FPS
        let mut snapshot_broadcast_interval = tokio::time::interval(Duration::from_secs(5)); // Broadcast snapshot every 5 seconds
        snapshot_broadcast_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        
        info!("GameManager: run_game_loop called for game {}", game_id);
        
        // Send initial snapshot immediately
        let initial_snapshot = engine.get_committed_state().clone();
        let initial_event = GameEventMessage {
            game_id,
            tick: initial_snapshot.tick,
            user_id: None,
            event: GameEvent::Snapshot { game_state: initial_snapshot },
        };
        info!("GameManager: Publishing initial snapshot for game {}", game_id);
        if let Err(e) = event_broker.publish_event(game_id, initial_event).await {
            warn!(game_id, error = %e, "Failed to send initial game snapshot");
        } else {
            info!(game_id, "Successfully sent initial game snapshot");
        }
        
        loop {
            tokio::select! {
                // Handle snapshot requests
                Some(response_tx) = snapshot_rx.recv() => {
                    // Get current committed state
                    let snapshot = engine.get_committed_state().clone();
                    let _ = response_tx.send(snapshot);
                }
                
                // Periodic snapshot broadcast for distributed servers
                _ = snapshot_broadcast_interval.tick() => {
                    // Only broadcast if we have the GameBroker (for distributed support)
                    if let Some(game_broker) = event_broker.as_any().downcast_ref::<crate::game_broker::GameBroker>() {
                        let snapshot = engine.get_committed_state().clone();
                        let _ = game_broker.broadcast_snapshot(game_id, snapshot).await;
                    }
                }
                
                // Handle game tick
                _ = interval.tick() => {
                    let mut state_changed = false;
                    
                    // Process all pending commands
                    while let Ok(cmd) = cmd_rx.try_recv() {
                        // Check if this is a snapshot request
                        if matches!(cmd.command, GameCommand::RequestSnapshot) {
                            info!(game_id, "Processing RequestSnapshot command");
                            // Send snapshot event
                            let snapshot = engine.get_committed_state().clone();
                            let snapshot_event = GameEventMessage {
                                game_id,
                                tick: snapshot.tick,
                                user_id: None,
                                event: GameEvent::Snapshot { game_state: snapshot },
                            };
                            if let Err(e) = event_broker.publish_event(game_id, snapshot_event).await {
                                error!(game_id, error = %e, "Failed to publish snapshot");
                                break;
                            } else {
                                info!(game_id, "Published snapshot in response to RequestSnapshot");
                            }
                        } else {
                            let events = server_process_incoming_command(&mut engine, cmd);
                            if !events.is_empty() {
                                state_changed = true;
                            }
                            for event in events {
                                // Broadcast event through broker
                                if event_broker.publish_event(game_id, event).await.is_err() {
                                    // Error publishing event
                                    break;
                                }
                            }
                        }
                    }
                    
                    // Run game tick
                    let now_ms = chrono::Utc::now().timestamp_millis();
                    match engine.run_until(now_ms) {
                        Ok(events) => {
                            if !events.is_empty() {
                                state_changed = true;
                            }
                            for event in events {
                                let event_msg = GameEventMessage {
                                    game_id,
                                    tick: engine.current_tick(),
                                    user_id: None,
                                    event,
                                };
                                if event_broker.publish_event(game_id, event_msg).await.is_err() {
                                    // Error publishing event
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Error running game tick: {:?}", e);
                        }
                    }
                    
                    // Send replication event if state changed
                    if state_changed {
                        if let Some(ref replica_manager) = replica_manager {
                            game_version += 1;
                            let state = engine.get_committed_state().clone();
                            let tick = engine.current_tick();
                            
                            let replication_cmd = ReplicationCommand::UpdateGameState {
                                game_id,
                                state,
                                version: game_version,
                                tick,
                                source_server: server_id.clone(),
                            };
                            
                            if let Err(e) = replica_manager.get_replication_sender().send(replication_cmd).await {
                                warn!("Failed to send replication command: {}", e);
                            }
                        }
                    }
                }
            }
        }
    }

    pub async fn join_game(
        &self,
        game_id: u32,
    ) -> Result<(mpsc::Sender<GameCommandMessage>, broadcast::Receiver<GameEventMessage>)> {
        let handle = self.broker.join_game(game_id).await?;
        Ok((handle.command_tx, handle.event_rx))
    }
    
    pub async fn get_game_snapshot(&self, game_id: u32) -> Result<GameState> {
        // Use the unified game handle for snapshot requests
        let handle = self.broker.join_game(game_id).await?;
        
        let (response_tx, response_rx) = oneshot::channel();
        handle.snapshot_tx.send(response_tx).await
            .map_err(|_| anyhow::anyhow!("Failed to request snapshot"))?;
        
        response_rx.await
            .map_err(|_| anyhow::anyhow!("Failed to receive snapshot"))
    }
}
