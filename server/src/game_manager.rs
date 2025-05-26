use std::time::Duration;
use common::{GameCommandMessage, GameEventMessage, GameEngine, GameState, server_process_incoming_command, GameCommand, GameEvent};
use anyhow::Result;
use tokio::sync::{broadcast, mpsc, oneshot};
use std::sync::Arc;
use crate::game_broker::GameMessageBroker;


pub struct GameManager {
    broker: Arc<dyn GameMessageBroker>,
}

impl GameManager {
    pub fn new(broker: Arc<dyn GameMessageBroker>) -> Self {
        GameManager {
            broker,
        }
    }

    pub async fn start_game(&mut self, id: u32) -> Result<()> {
        // Check if game is already running by trying to check if it's local
        if let Ok(true) = self.broker.is_game_local(id).await {
            return Ok(()); // Game already running locally
        }
        
        // Create the game engine
        let start_ms = chrono::Utc::now().timestamp_millis();
        let rng_seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let game = GameEngine::new_with_seed(id, start_ms, rng_seed);
        
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
        
        // Spawn the game loop
        tokio::spawn(async move {
            Self::run_game_loop(id, game, command_rx, event_broker, snapshot_rx).await;
        });
        
        Ok(())
    }
    
    async fn run_game_loop(
        game_id: u32,
        mut engine: GameEngine,
        mut cmd_rx: broadcast::Receiver<GameCommandMessage>,
        event_broker: Arc<dyn GameMessageBroker>,
        mut snapshot_rx: mpsc::Receiver<oneshot::Sender<GameState>>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_millis(16)); // ~60 FPS
        let mut snapshot_broadcast_interval = tokio::time::interval(Duration::from_secs(5)); // Broadcast snapshot every 5 seconds
        snapshot_broadcast_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        
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
                    // Process all pending commands
                    while let Ok(cmd) = cmd_rx.try_recv() {
                        // Check if this is a snapshot request
                        if matches!(cmd.command, GameCommand::RequestSnapshot) {
                            // Send snapshot event
                            let snapshot = engine.get_committed_state().clone();
                            let snapshot_event = GameEventMessage {
                                game_id,
                                tick: snapshot.tick,
                                user_id: None,
                                event: GameEvent::Snapshot { game_state: snapshot },
                            };
                            if event_broker.publish_event(game_id, snapshot_event).await.is_err() {
                                break;
                            }
                        } else {
                            let events = server_process_incoming_command(&mut engine, cmd);
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
