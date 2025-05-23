use std::collections::HashMap;
use common::{GameCommand, GameEventMessage, GameEngine};
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use anyhow::Result;
use tokio::sync::{broadcast, mpsc};

pub struct GamesManager {
    games: HashMap<u32, Box<GameEngine>>,
    command_txs: HashMap<u32, broadcast::Sender<GameCommand>>,
    command_rxs: HashMap<u32, broadcast::Receiver<GameCommand>>,
    event_txs: HashMap<u32, broadcast::Sender<GameEventMessage>>,
    event_rxs: HashMap<u32, broadcast::Receiver<GameEventMessage>>,
}

impl GamesManager {
    fn new() -> Self {
        GamesManager {
            games: HashMap::new(),
            command_txs: HashMap::new(),
            command_rxs: HashMap::new(),
            event_txs: HashMap::new(),
            event_rxs: HashMap::new(),
        }
    }

    pub async fn start_game(&mut self, id: u32) -> Result<&GameEngine> {
        if self.games.contains_key(&id) {
            return Err(anyhow::anyhow!("Game already exists"));
        }

        let game = GameEngine::new(id, 10, 10);
        self.games.insert(id, Box::new(game));
        let (mut command_tx, command_rx) = broadcast::channel(32);
        let (mut event_tx, event_rx) = broadcast::channel(32);
        self.command_txs.insert(id, command_tx);
        self.command_rxs.insert(id, command_rx);
        self.event_txs.insert(id, event_tx);
        self.event_rxs.insert(id, event_rx);
        Ok(self.games[id])
    }

    pub async fn join_game(
        &self,
        game_id: u32,
    ) -> Result<(broadcast::Sender<GameCommand>, broadcast::Receiver<GameEventMessage>)> {

        let tx = self.command_txs.get(&game_id)
            .ok_or_else(|| anyhow::anyhow!("Game not found"))?
            .clone();

        let rx = self.event_txs.get(&game_id)
            .ok_or_else(|| anyhow::anyhow!("Game not found"))?
            .subscribe();

        Ok((tx, rx))
    }
}
