use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;

use common::{GameEventMessage, GameState, GameStatus};

pub mod directory;
mod listener;
mod recorder;

pub use listener::ReplayListener;
pub use recorder::GameReplayRecorder;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampedEvent {
    pub tick: u32,
    pub timestamp: SystemTime,
    pub event: GameEventMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GameReplay {
    pub version: u32,
    pub game_id: u32,
    pub initial_state: GameState,
    pub events: Vec<TimestampedEvent>,
    pub metadata: ReplayMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayMetadata {
    pub players: Vec<PlayerInfo>,
    pub start_time: SystemTime,
    pub end_time: SystemTime,
    pub final_status: GameStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerInfo {
    pub user_id: u32,
    pub snake_id: u32,
    pub username: String,
}

impl GameReplay {
    pub fn new(game_id: u32, initial_state: GameState) -> Self {
        Self {
            version: 1,
            game_id,
            initial_state,
            events: Vec::new(),
            metadata: ReplayMetadata {
                players: Vec::new(),
                start_time: SystemTime::now(),
                end_time: SystemTime::now(),
                final_status: GameStatus::Stopped,
            },
        }
    }
}
