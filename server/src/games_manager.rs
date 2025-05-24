use std::collections::HashMap;
use std::time::{Duration, Instant};
use common::{GameCommand, GameCommandMessage, GameEvent, GameEventMessage, GameEngine, GameState, server_process_incoming_command};
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use anyhow::Result;
use tokio::sync::{broadcast, mpsc, oneshot};
use uuid::Uuid;
use std::sync::Arc;
use crate::game_broker::GameMessageBroker;

pub struct GamesManager {
    command_txs: HashMap<u32, broadcast::Sender<GameCommandMessage>>,
    event_txs: HashMap<u32, broadcast::Sender<GameEventMessage>>,
    snapshot_txs: HashMap<u32, mpsc::Sender<oneshot::Sender<GameState>>>,
    broker: Option<Arc<dyn GameMessageBroker>>,
}

impl GamesManager {
    pub fn new() -> Self {
        GamesManager {
            command_txs: HashMap::new(),
            event_txs: HashMap::new(),
            snapshot_txs: HashMap::new(),
            broker: None,
        }
    }
    
    pub fn new_with_broker(broker: Arc<dyn GameMessageBroker>) -> Self {
        GamesManager {
            command_txs: HashMap::new(),
            event_txs: HashMap::new(),
            snapshot_txs: HashMap::new(),
            broker: Some(broker),
        }
    }

    pub async fn start_game(&mut self, id: u32) -> Result<()> {
        if self.command_txs.contains_key(&id) {
            return Err(anyhow::anyhow!("Game already exists"));
        }

        let start_ms = chrono::Utc::now().timestamp_millis();
        // Generate a random seed for this game instance
        let rng_seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let mut game = GameEngine::new_with_seed(id, start_ms, rng_seed);
        
        let (command_tx, command_rx) = broadcast::channel(32);
        let (event_tx, _) = broadcast::channel(32);
        let (snapshot_tx, snapshot_rx) = mpsc::channel(32);
        
        self.command_txs.insert(id, command_tx);
        self.event_txs.insert(id, event_tx.clone());
        self.snapshot_txs.insert(id, snapshot_tx);
        
        // Spawn the game loop
        tokio::spawn(Self::run_game_loop(id, game, command_rx, event_tx, snapshot_rx));
        
        Ok(())
    }

    pub async fn join_game(
        &self,
        game_id: u32,
    ) -> Result<(broadcast::Sender<GameCommandMessage>, broadcast::Receiver<GameEventMessage>)> {
        let tx = self.command_txs.get(&game_id)
            .ok_or_else(|| anyhow::anyhow!("Game not found"))?
            .clone();

        let rx = self.event_txs.get(&game_id)
            .ok_or_else(|| anyhow::anyhow!("Game not found"))?
            .subscribe();

        Ok((tx, rx))
    }
    
    pub async fn get_game_snapshot(&self, game_id: u32) -> Result<GameState> {
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
                        let events = server_process_incoming_command(&mut engine, cmd);
                        for event in events {
                            // Broadcast event to all connected clients
                            if event_tx.send(event).is_err() {
                                // No receivers, game might be abandoned
                                break;
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
