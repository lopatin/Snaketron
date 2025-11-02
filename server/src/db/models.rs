use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Server {
    pub id: i32,
    pub grpc_address: String,
    pub last_heartbeat: Option<DateTime<Utc>>,
    pub region: String,
    pub origin: String, // HTTP origin e.g., "http://localhost:8080"
    pub ws_url: String, // WebSocket URL e.g., "ws://localhost:8080/ws"
    pub created_at: DateTime<Utc>,
    pub status: String,
    pub current_game_count: i32,
    pub max_game_capacity: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: i32,
    pub username: String,
    pub password_hash: String,
    pub mmr: i32,
    pub xp: i32,
    pub created_at: DateTime<Utc>,
    pub is_guest: bool,
    pub guest_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Game {
    pub id: i32,
    pub server_id: Option<i32>,
    pub game_type: JsonValue,
    pub game_state: Option<JsonValue>,
    pub status: String,
    pub ended_at: Option<DateTime<Utc>>,
    pub last_activity: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub game_mode: String,
    pub is_private: bool,
    pub game_code: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GamePlayer {
    pub id: i32,
    pub game_id: i32,
    pub user_id: i32,
    pub team_id: i32,
    pub joined_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomGameLobby {
    pub id: i32,
    pub game_code: String,
    pub host_user_id: i32,
    pub settings: JsonValue,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub game_id: Option<i32>,
    pub state: String,
}

// Type alias for consistency
pub type CustomLobby = CustomGameLobby;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LobbyMetadata {
    pub lobby_code: String, // Primary identifier, format: {REGION_CODE}-{HASH} (e.g., USE1-A3B2C4D5)
    pub host_user_id: i32,
    pub region: String,
    pub created_at: DateTime<Utc>,
    pub state: String, // waiting | queued | matched
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GameSpectator {
    pub game_id: i32,
    pub user_id: i32,
    pub joined_at: DateTime<Utc>,
}

// DynamoDB specific models for single table design
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamoItem {
    pub pk: String,
    pub sk: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gsi1pk: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gsi1sk: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gsi2pk: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gsi2sk: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<i64>,
    #[serde(flatten)]
    pub data: JsonValue,
}
