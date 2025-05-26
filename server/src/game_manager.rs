use std::collections::HashMap;
use std::time::Duration;
use common::{GameCommandMessage, GameEventMessage, GameEngine, GameState, server_process_incoming_command, GameCommand, GameEvent};
use anyhow::Result;
use tokio::sync::{broadcast, mpsc, oneshot};
use std::sync::Arc;
use crate::game_broker::GameMessageBroker;


pub struct GameManager {
    command_txs: HashMap<u32, broadcast::Sender<GameCommandMessage>>,
    event_txs: HashMap<u32, broadcast::Sender<GameEventMessage>>,
    snapshot_txs: HashMap<u32, mpsc::Sender<oneshot::Sender<GameState>>>,
    broker: Option<Arc<dyn GameMessageBroker>>,
}

impl GameManager {
    pub fn new() -> Self {
        GameManager {
            command_txs: HashMap::new(),
            event_txs: HashMap::new(),
            snapshot_txs: HashMap::new(),
            broker: None,
        }
    }
    
    pub fn new_with_broker(broker: Arc<dyn GameMessageBroker>) -> Self {
        GameManager {
            command_txs: HashMap::new(),
            event_txs: HashMap::new(),
            snapshot_txs: HashMap::new(),
            broker: Some(broker),
        }
    }

    pub async fn start_game(&mut self, id: u32) -> Result<()> {
        // Check if game is already running
        if self.snapshot_txs.contains_key(&id) {
            return Ok(()); // Game already running, nothing to do
        }
        
        // Check if we're using a broker
        if let Some(broker) = &self.broker {
            // For GameBroker, create game channels (registers in DB)
            if let Some(game_broker) = broker.as_any().downcast_ref::<crate::game_broker::GameBroker>() {
                game_broker.create_game_channels(id).await?;
            }
            
            let start_ms = chrono::Utc::now().timestamp_millis();
            let rng_seed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let game = GameEngine::new_with_seed(id, start_ms, rng_seed);
            
            // Subscribe to commands through broker
            let command_rx = broker.subscribe_commands(id).await?;
            let event_broker = broker.clone();
            let (snapshot_tx, snapshot_rx) = mpsc::channel(32);
            self.snapshot_txs.insert(id, snapshot_tx);
            
            // Spawn the game loop with broker
            tokio::spawn(async move {
                Self::run_game_loop_with_broker(id, game, command_rx, event_broker, snapshot_rx).await;
            });
        } else {
            // Original implementation for backward compatibility
            if self.command_txs.contains_key(&id) {
                return Err(anyhow::anyhow!("Game already exists"));
            }
            
            let start_ms = chrono::Utc::now().timestamp_millis();
            let rng_seed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            let game = GameEngine::new_with_seed(id, start_ms, rng_seed);
            
            let (command_tx, command_rx) = broadcast::channel(32);
            let (event_tx, _) = broadcast::channel(32);
            let (snapshot_tx, snapshot_rx) = mpsc::channel(32);
            
            self.command_txs.insert(id, command_tx);
            self.event_txs.insert(id, event_tx.clone());
            self.snapshot_txs.insert(id, snapshot_tx);
            
            // Spawn the game loop
            tokio::spawn(Self::run_game_loop(id, game, command_rx, event_tx, snapshot_rx));
        }
        
        Ok(())
    }
    
    async fn run_game_loop_with_broker(
        game_id: u32,
        mut engine: GameEngine,
        mut cmd_rx: broadcast::Receiver<GameCommandMessage>,
        event_broker: Arc<dyn GameMessageBroker>,
        mut snapshot_rx: mpsc::Receiver<oneshot::Sender<GameState>>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_millis(16)); // ~60 FPS
        
        loop {
            tokio::select! {
                // Handle snapshot requests
                Some(response_tx) = snapshot_rx.recv() => {
                    // Get current committed state
                    let snapshot = engine.get_committed_state().clone();
                    let _ = response_tx.send(snapshot);
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
    ) -> Result<(broadcast::Sender<GameCommandMessage>, broadcast::Receiver<GameEventMessage>)> {
        if let Some(broker) = &self.broker {
            // Create wrapper channels that forward through the broker
            let (cmd_tx, mut cmd_rx) = broadcast::channel(32);
            let event_rx = broker.subscribe_events(game_id).await.map_err(|e| {
                e
            })?;
            
            // Give the subscription a moment to establish
            tokio::time::sleep(Duration::from_millis(100)).await;
            
            // Send a special command to request game snapshot
            // This will trigger the game to send a snapshot event
            let snapshot_request = GameCommandMessage {
                tick: 0,
                received_order: 0,
                user_id: 0, // System command
                command: GameCommand::RequestSnapshot,
            };
            broker.publish_command(game_id, snapshot_request).await.map_err(|e| {
                e
            })?;
            
            // Spawn a task to forward commands through the broker
            let broker_clone = broker.clone();
            tokio::spawn(async move {
                while let Ok(cmd) = cmd_rx.recv().await {
                    let _ = broker_clone.publish_command(game_id, cmd).await;
                }
            });
            
            Ok((cmd_tx, event_rx))
        } else {
            // Original implementation
            let tx = self.command_txs.get(&game_id)
                .ok_or_else(|| anyhow::anyhow!("Game not found"))?
                .clone();

            let rx = self.event_txs.get(&game_id)
                .ok_or_else(|| anyhow::anyhow!("Game not found"))?
                .subscribe();

            Ok((tx, rx))
        }
    }
    
    pub async fn get_game_snapshot(&self, game_id: u32) -> Result<GameState> {
        // Check if we have a broker and if the game is remote
        if let Some(broker) = &self.broker {
            if !broker.is_game_local(game_id).await? {
                // For remote games, we need to fetch via gRPC
                // For now, return a placeholder error
                return Err(anyhow::anyhow!("Remote game snapshot not yet implemented"));
            }
        }
        
        // Local game - use snapshot channel
        let snapshot_tx = self.snapshot_txs.get(&game_id)
            .ok_or_else(|| anyhow::anyhow!("Game not found"))?;
        
        let (response_tx, response_rx) = oneshot::channel();
        snapshot_tx.send(response_tx).await
            .map_err(|_| anyhow::anyhow!("Failed to request snapshot"))?;
        
        response_rx.await
            .map_err(|_| anyhow::anyhow!("Failed to receive snapshot"))
    }
    
    async fn run_game_loop(
        game_id: u32,
        mut engine: GameEngine,
        mut cmd_rx: broadcast::Receiver<GameCommandMessage>,
        event_tx: broadcast::Sender<GameEventMessage>,
        mut snapshot_rx: mpsc::Receiver<oneshot::Sender<GameState>>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_millis(16)); // ~60 FPS
        
        loop {
            tokio::select! {
                // Handle snapshot requests
                Some(response_tx) = snapshot_rx.recv() => {
                    // Get current committed state
                    let snapshot = engine.get_committed_state().clone();
                    let _ = response_tx.send(snapshot);
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
                            if event_tx.send(snapshot_event).is_err() {
                                break;
                            }
                        } else {
                            let events = server_process_incoming_command(&mut engine, cmd);
                            for event in events {
                                // Broadcast event to all connected clients
                                if event_tx.send(event).is_err() {
                                    // No receivers, game might be abandoned
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
                                if event_tx.send(event_msg).is_err() {
                                    // No receivers, game might be abandoned
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
}
