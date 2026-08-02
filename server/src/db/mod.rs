pub mod dynamodb;
pub mod models;
pub mod queries;

use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

use crate::completion::{CompletionEffect, CompletionRecordV1, EffectApplyResult};
use crate::season::Season;
use common::GameState;
use models::*;

/// How old a server heartbeat may be before that server no longer counts as
/// alive. Shared by every consumer (region cache, load balancing, ws-url
/// lookup) so a region is never advertised while its servers are considered
/// ineligible, or vice versa.
pub const SERVER_HEARTBEAT_FRESHNESS_SECONDS: i64 = 60;

/// The metadata a server registers under; heartbeats re-assert it so a
/// registration deleted out from under a live server (TTL reaper, manual
/// cleanup) is transparently recreated on the next heartbeat.
#[derive(Debug, Clone)]
pub struct ServerRegistration {
    pub grpc_address: String,
    pub region: String,
    pub origin: String,
    pub ws_url: String,
}

// Several DB methods take a full column set as separate parameters by design.
#[allow(clippy::too_many_arguments)]
#[async_trait]
pub trait Database: Send + Sync {
    // Server operations
    async fn register_server(
        &self,
        grpc_address: &str,
        region: &str,
        origin: &str,
        ws_url: &str,
    ) -> Result<i32>;
    async fn update_server_heartbeat(
        &self,
        server_id: i32,
        registration: &ServerRegistration,
    ) -> Result<()>;
    async fn update_server_status(&self, server_id: i32, status: &str) -> Result<()>;
    async fn get_server_for_load_balancing(&self, region: &str) -> Result<i32>;
    async fn get_active_servers(&self, region: &str) -> Result<Vec<(i32, String)>>;
    async fn get_region_ws_url(&self, region: &str) -> Result<Option<String>>;

    // User operations
    async fn create_user(&self, username: &str, password_hash: &str, mmr: i32) -> Result<User>;
    async fn create_guest_user(&self, nickname: &str, guest_token: &str, mmr: i32) -> Result<User>;
    async fn get_user_by_id(&self, user_id: i32) -> Result<Option<User>>;
    async fn get_user_by_username(&self, username: &str) -> Result<Option<User>>;
    async fn update_user_mmr(&self, user_id: i32, mmr: i32) -> Result<()>;
    async fn update_guest_username(&self, user_id: i32, username: &str) -> Result<()>;
    async fn add_user_xp(&self, user_id: i32, xp_to_add: i32) -> Result<i32>; // Returns new total XP

    // MMR operations for ranked/casual queues
    async fn update_user_mmr_by_mode(
        &self,
        user_id: i32,
        mmr_delta: i32,
        queue_mode: &common::QueueMode,
    ) -> Result<i32>; // Returns new MMR
    async fn get_user_mmrs(&self, user_ids: &[i32]) -> Result<HashMap<i32, (i32, i32)>>; // Returns (ranked_mmr, casual_mmr) for each user

    // Ranking/leaderboard operations
    async fn upsert_ranking(
        &self,
        user_id: i32,
        username: &str,
        mmr: i32,
        queue_mode: &common::QueueMode,
        game_type: &common::GameType,
        region: &str,
        season: Season,
        won: bool,
    ) -> Result<()>;
    async fn get_leaderboard(
        &self,
        queue_mode: &common::QueueMode,
        game_type: Option<&common::GameType>,
        region: Option<&str>,
        season: Season,
        limit: usize,
    ) -> Result<Vec<RankingEntry>>;
    async fn get_user_ranking(
        &self,
        user_id: i32,
        queue_mode: &common::QueueMode,
        game_type: &common::GameType,
        region: &str,
        season: Season,
    ) -> Result<Option<RankingEntry>>;

    // High score operations for solo games
    async fn insert_high_score(
        &self,
        game_id: &str,
        user_id: i32,
        username: &str,
        score: i32,
        game_type: &common::GameType,
        region: &str,
        season: Season,
    ) -> Result<()>;
    async fn get_high_scores(
        &self,
        game_type: &common::GameType,
        region: Option<&str>,
        season: Season,
        limit: usize,
    ) -> Result<Vec<HighScoreEntry>>;

    // Game operations
    /// Allocate a globally unique game ID from durable storage.
    ///
    /// Runtime and database-created games share this authoritative allocator.
    async fn allocate_game_id(&self) -> Result<i32>;
    async fn create_game(
        &self,
        server_id: i32,
        game_type: &JsonValue,
        game_mode: &str,
        is_private: bool,
        game_code: Option<&str>,
    ) -> Result<i32>;
    async fn get_game_by_id(&self, game_id: i32) -> Result<Option<Game>>;
    async fn get_game_by_code(&self, game_code: &str) -> Result<Option<Game>>;
    async fn update_game_status(&self, game_id: i32, status: &str) -> Result<()>;
    /// Persist the final authoritative state for a completed runtime game.
    ///
    /// Unlike `create_game`, this uses the ID already coordinated by the runtime's
    /// durable allocator, so it can upsert a record that was not previously created
    /// in DynamoDB.
    async fn upsert_completed_game(
        &self,
        game_id: i32,
        server_id: i32,
        game_state: &GameState,
    ) -> Result<()>;
    /// Apply one immutable completion effect with its idempotency marker in
    /// the same database transaction as the mutation.
    async fn apply_completion_effect(
        &self,
        completion: &CompletionRecordV1,
        effect: &CompletionEffect,
    ) -> Result<EffectApplyResult>;
    async fn add_player_to_game(&self, game_id: i32, user_id: i32, team_id: i32) -> Result<()>;
    async fn get_game_players(&self, game_id: i32) -> Result<Vec<GamePlayer>>;
    async fn get_player_count(&self, game_id: i32) -> Result<i64>;

    // Custom lobby operations
    async fn create_custom_lobby(
        &self,
        game_code: &str,
        host_user_id: i32,
        settings: &JsonValue,
    ) -> Result<i32>;
    async fn update_custom_lobby_game_id(&self, lobby_id: i32, game_id: i32) -> Result<()>;
    async fn get_custom_lobby_host(&self, game_id: i32) -> Result<Option<i32>>;
    async fn get_custom_lobby_by_code(&self, game_code: &str) -> Result<Option<CustomLobby>>;

    // Spectator operations
    async fn add_spectator_to_game(&self, game_id: i32, user_id: i32) -> Result<()>;
}
