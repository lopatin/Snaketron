use std::collections::HashMap;
use std::time::{Duration, Instant};
use common::{GameCommand, GameCommandMessage, GameEvent, GameEventMessage, GameEngine, server_process_incoming_command};
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use anyhow::Result;
use tokio::sync::{broadcast, mpsc};
use uuid::Uuid;

pub struct GamesManager {
    command_txs: HashMap<u32, broadcast::Sender<GameCommandMessage>>,
    event_txs: HashMap<u32, broadcast::Sender<GameEventMessage>>,
}

impl GamesManager {
    pub fn new() -> Self {
        GamesManager {
            command_txs: HashMap::new(),
            event_txs: HashMap::new(),
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
        
        self.command_txs.insert(id, command_tx);
        self.event_txs.insert(id, event_tx.clone());
        
        // Spawn the game loop
        tokio::spawn(Self::run_game_loop(id, game, command_rx, event_tx));
        
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
    
    async fn run_game_loop(
        game_id: u32,
        mut engine: GameEngine,
        mut cmd_rx: broadcast::Receiver<GameCommandMessage>,
        event_tx: broadcast::Sender<GameEventMessage>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_millis(16)); // ~60 FPS
        
        loop {
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
            
            interval.tick().await;
        }
    }
}
