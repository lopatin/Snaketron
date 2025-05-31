use serde::{Deserialize, Serialize};
use async_raft::{AppData, AppDataResponse};
use common::GameState;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RaftNodeId(pub String);

impl From<String> for RaftNodeId {
    fn from(s: String) -> Self {
        RaftNodeId(s)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientRequest {
    // Game state updates
    UpdateGameState {
        game_id: u32,
        state: GameState,
        tick: u32,
    },
    
    // Authority transfers
    TransferAuthority {
        game_id: u32,
        from_server: String,
        to_server: String,
        reason: String,
    },
    
    // Game lifecycle
    CreateGame {
        game_id: u32,
        initial_state: GameState,
        authority_server: String,
    },
    
    DeleteGame {
        game_id: u32,
        reason: String,
    },
    
    // Service registry
    RegisterServer {
        server_id: String,
        host: String,
        port: u16,
        grpc_port: u16,
        max_capacity: u32,
    },
    
    UpdateServerHeartbeat {
        server_id: String,
    },
    
    RemoveServer {
        server_id: String,
    },
}

// Implement the AppData trait for ClientRequest
impl AppData for ClientRequest {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientResponse {
    Success,
    GameStateUpdated { version: u64 },
    AuthorityTransferred { new_authority: String },
    GameCreated { game_id: u32 },
    GameDeleted,
    ServerRegistered,
    HeartbeatRecorded,
    ServerRemoved,
    Error(String),
}

// Implement the AppDataResponse trait for ClientResponse
impl AppDataResponse for ClientResponse {}