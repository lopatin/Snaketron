use serde::{Deserialize, Serialize};
use async_raft::{AppData, AppDataResponse, NodeId};
use common::{GameCommandMessage, GameEventMessage, GameState};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientRequest {
    CreateGame {
        game_id: u32,
        game_state: GameState,
    },
    
    StartGame {
        game_id: u32,
        server_id: u64,
    },
    
    ProcessGameEvent(GameEventMessage),

    RegisterServer {
        server_id: u64,
        hostname: String,
        grpc_port: u16,
    },
    
    RemoveServer {
        server_id: u64,
    },
    
    /// Submit a game command from a user
    SubmitGameCommand {
        game_id: u32,
        user_id: u32,
        command: GameCommandMessage,
        current_tick: u64, // Server's view of current game tick
    },
}

// IntoRequest implementation removed - not needed

// Implement the AppData trait for ClientRequest
impl AppData for ClientRequest {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientResponse {
    Success,
    GameJoined { game_id: u32, user_id: u32, snake_id: u32 },
    GameCreated { game_id: u32 },
    GameDeleted,
    ServerRegistered,
    HeartbeatRecorded,
    ServerRemoved,
    Error(String),
}

impl From<Vec<u8>> for ClientResponse {
    fn from(bytes: Vec<u8>) -> Self {
        bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap().0
    }
}

// Implement the AppDataResponse trait for ClientResponse
impl AppDataResponse for ClientResponse {}

// State change events emitted by the state machine
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StateChangeEvent {
    GameCreated {
        game_id: u32,
    },
    GameEvent {
        event: GameEventMessage,
    },
    ServerRegistered {
        server_id: u64,
    },
    ServerRemoved {
        server_id: u64,
    },
    GameCommandSubmitted {
        game_id: u32,
        user_id: u32,
        command: GameCommandMessage,
        tick_submitted: u64,
    },
}