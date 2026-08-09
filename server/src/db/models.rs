use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

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
    pub mmr: i32, // Legacy field, kept for backwards compatibility
    pub ranked_mmr: i32,
    pub casual_mmr: i32,
    pub xp: i32,
    pub created_at: DateTime<Utc>,
    pub is_guest: bool,
    pub guest_token: Option<String>,
    #[serde(default)]
    pub is_stress_test: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crazygames_user_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_picture_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_iat: Option<i64>,
}

/// The verified, server-side view of a CrazyGames identity.  None of these
/// fields may be populated from `getUser()` or other client-controlled data;
/// they come exclusively from a successfully verified CrazyGames JWT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrazyGamesProfile {
    pub provider_user_id: String,
    pub username: String,
    pub avatar_url: String,
    pub profile_iat: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CrazyGamesAccountResolution {
    Created,
    GuestClaimed,
    Returning,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CrazyGamesGuestPromotion {
    Check,
    Allow,
    #[default]
    Decline,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CrazyGamesPreferences {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tutorial_seen: Option<HashMap<String, bool>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lobby_preferences: Option<CrazyGamesLobbyPreferences>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boost_input_mode: Option<CrazyGamesBoostInputMode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CrazyGamesLobbyPreferences {
    pub selected_modes: Vec<String>,
    pub competitive: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CrazyGamesBoostInputMode {
    Hold,
    Toggle,
}

impl CrazyGamesPreferences {
    /// Merge an incoming full/partial preference snapshot. Tutorial completion
    /// is monotonic; device races can never make a completed tutorial unseen.
    /// Other present fields use last-write-wins, while omitted fields retain
    /// their current value.
    pub fn merge(&self, incoming: &Self) -> Self {
        let tutorial_seen = match (&self.tutorial_seen, &incoming.tutorial_seen) {
            (None, None) => None,
            (current, next) => {
                let mut merged = current.clone().unwrap_or_default();
                for (tutorial, seen) in next.iter().flat_map(|items| items.iter()) {
                    if *seen || !merged.contains_key(tutorial) {
                        merged.insert(tutorial.clone(), *seen);
                    }
                }
                Some(merged)
            }
        };

        Self {
            tutorial_seen,
            lobby_preferences: incoming
                .lobby_preferences
                .clone()
                .or_else(|| self.lobby_preferences.clone()),
            boost_input_mode: incoming.boost_input_mode.or(self.boost_input_mode),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CrazyGamesAccount {
    pub user: User,
    pub profile: CrazyGamesProfile,
    pub resolution: CrazyGamesAccountResolution,
    pub preferences: CrazyGamesPreferences,
}

#[derive(Debug, Clone)]
pub enum CrazyGamesAccountOutcome {
    Resolved(Box<CrazyGamesAccount>),
    GuestLinkConsentRequired,
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
    #[serde(default)]
    pub matchmaking_pool: crate::matchmaking_pool::MatchmakingPool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GameSpectator {
    pub game_id: i32,
    pub user_id: i32,
    pub joined_at: DateTime<Utc>,
}

// Ranking entry for leaderboards
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankingEntry {
    pub user_id: i32,
    pub username: String,
    pub mmr: i32,
    pub games_played: i32,
    pub wins: i32,
    pub losses: i32,
    pub region: String,
    pub queue_mode: String, // "ranked" or "casual"
    pub game_type: String,  // "solo", "duel", "2v2", "ffa"
    pub season: u32,
    pub updated_at: DateTime<Utc>,
}

// High score entry for solo game leaderboards
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HighScoreEntry {
    pub game_id: String,
    pub user_id: i32,
    pub username: String,
    pub score: i32,
    pub region: String,
    pub game_type: String, // "solo"
    pub season: u32,
    pub timestamp: DateTime<Utc>,
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
