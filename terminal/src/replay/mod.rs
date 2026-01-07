pub mod player;
pub mod reader;

use common::{GameEventMessage, GameState, GameStatus};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlayerInfo {
    pub user_id: u32,
    pub snake_id: u32,
    pub username: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayMetadata {
    pub players: Vec<PlayerInfo>,
    pub start_time: SystemTime,
    pub end_time: SystemTime,
    pub final_status: GameStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampedEvent {
    pub tick: u32,
    pub timestamp: SystemTime,
    pub event: GameEventMessage,
}

pub struct ReplayData {
    pub metadata: ReplayMetadata,
    pub initial_state: GameState,
    pub events: Vec<TimestampedEvent>,
}
