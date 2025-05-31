use serde::{Deserialize, Serialize};
use async_raft::{AppData, AppDataResponse};
use common::GameState;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum GameDiscoveryStatus {
    Discovered,      // Found in DB, pending Raft submission
    Submitted,       // Submitted to Raft
    Assigned,        // Raft assigned to a server
    Started,         // Game actually started
}

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
        players: Vec<u32>,
        discovery_source: String,
        discovered_at: i64,
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

// State change events emitted by the state machine
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StateChangeEvent {
    GameAssigned {
        game_id: u32,
        authority: String,
        players: Vec<u32>,
    },
    GameStateUpdated {
        game_id: u32,
        version: u64,
    },
    AuthorityTransferred {
        game_id: u32,
        from: String,
        to: String,
    },
    GameDeleted {
        game_id: u32,
    },
    ServerRegistered {
        server_id: String,
    },
    ServerRemoved {
        server_id: String,
    },
}