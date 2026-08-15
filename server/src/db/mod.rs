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
    async fn create_guest_user(
        &self,
        nickname: &str,
        guest_token: &str,
        mmr: i32,
        is_stress_test: bool,
    ) -> Result<User>;
    /// Convert an existing public guest into a password-backed account while
    /// preserving the guest's durable user ID and all ID-owned progress.
    async fn upgrade_guest_to_account(
        &self,
        user_id: i32,
        username: &str,
        password_hash: &str,
    ) -> Result<User>;
    async fn get_user_by_id(&self, user_id: i32) -> Result<Option<User>>;
    async fn get_user_by_username(&self, username: &str) -> Result<Option<User>>;
    async fn update_user_mmr(&self, user_id: i32, mmr: i32) -> Result<()>;
    async fn update_guest_username(&self, user_id: i32, username: &str) -> Result<()>;
    async fn add_user_xp(&self, user_id: i32, xp_to_add: i32) -> Result<i32>; // Returns new total XP

    /// Resolve one verified CrazyGames identity into a durable Snaketron
    /// account. Implementations own the uniqueness/claim transaction; callers
    /// must never construct an account from unverified portal profile data.
    /// Initial browser preferences may be imported only when an eligible
    /// authenticated guest is actually claimed with explicit permission; a
    /// newly created provider identity must not inherit unscoped state from a
    /// shared browser. A consent check is read-only and returns a typed outcome
    /// when an eligible guest could be claimed.
    async fn resolve_crazygames_account(
        &self,
        profile: &CrazyGamesProfile,
        guest_candidate_user_id: Option<i32>,
        guest_promotion: CrazyGamesGuestPromotion,
        initial_preferences: Option<&CrazyGamesPreferences>,
    ) -> Result<CrazyGamesAccountOutcome>;
    /// Save the CrazyGames-linked user's preference snapshot. Implementations
    /// must reject users which are not linked to CrazyGames.
    async fn save_crazygames_preferences(
        &self,
        user_id: i32,
        preferences: &CrazyGamesPreferences,
    ) -> Result<CrazyGamesPreferences>;

    // MMR operations for ranked/casual queues
    async fn update_user_mmr_by_mode(
        &self,
        user_id: i32,
        mmr_delta: i32,
        queue_mode: &common::QueueMode,
    ) -> Result<i32>; // Returns new MMR
    async fn get_user_mmrs(&self, user_ids: &[i32]) -> Result<HashMap<i32, (i32, i32)>>; // Returns (ranked_mmr, casual_mmr) for each user

    /// Guest status for a batch of accounts, for callers rendering rosters of
    /// public names. Display names are not unique across guests, so a name
    /// alone cannot identify an account; only the ID can. IDs with no account
    /// are absent from the map rather than defaulted.
    async fn get_users_are_guests(&self, user_ids: &[i32]) -> Result<HashMap<i32, bool>>;

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
    /// Return a global Solo snapshot for news. Backends must opt into leader
    /// claims by proving their read is globally ordered and by inspecting the
    /// actual top row before applying public-attribution filters.
    async fn get_news_high_score_snapshot(
        &self,
        _game_type: &common::GameType,
        _season: Season,
    ) -> Result<NewsHighScoreSnapshot> {
        Ok(NewsHighScoreSnapshot {
            leader: None,
            coverage: NewsLeaderboardCoverage::BoundedSample,
        })
    }

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
    /// Return the newest durable completed games first.
    ///
    /// Backends that do not provide a completion index safely expose no recent
    /// games instead of blocking unrelated server features on this optional
    /// read model.
    async fn get_recent_completed_games(&self, _limit: usize) -> Result<Vec<Game>> {
        Ok(Vec::new())
    }
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
    /// Read one user's compact match history. Implementations must use the
    /// supplied authenticated user ID as the partition scope and treat cursors
    /// as opaque, scope-bound continuation tokens.
    async fn get_match_history(
        &self,
        _user_id: i32,
        _limit: usize,
        _cursor: Option<&str>,
    ) -> Result<MatchHistoryPage> {
        Err(anyhow::anyhow!(
            "match history is not supported by this database"
        ))
    }
    /// Read the global administrative history projection without loading full
    /// game snapshots.
    async fn get_admin_match_history(
        &self,
        _limit: usize,
        _cursor: Option<&str>,
    ) -> Result<MatchHistoryPage> {
        Err(anyhow::anyhow!(
            "administrative match history is not supported by this database"
        ))
    }
    /// Missing configuration intentionally resolves to safe defaults so test
    /// and alternate database implementations fail closed for ads.
    async fn get_runtime_config(&self) -> Result<RuntimeConfigRecord> {
        Ok(RuntimeConfigRecord::default())
    }
    async fn update_runtime_config(
        &self,
        _expected_version: u64,
        _config: &RuntimeConfig,
        _actor: &RuntimeConfigActor,
    ) -> Result<RuntimeConfigRecord> {
        Err(anyhow::anyhow!(
            "runtime configuration updates are not supported by this database"
        ))
    }
    async fn get_runtime_config_audit(
        &self,
        _limit: usize,
        _cursor: Option<&str>,
    ) -> Result<RuntimeConfigAuditPage> {
        Err(anyhow::anyhow!(
            "runtime configuration audit is not supported by this database"
        ))
    }
    /// Atomically reserve one pre-match ad-break opportunity for every
    /// targeted user. Alternate database implementations fail closed for ads.
    async fn try_claim_pre_match_ad_break(
        &self,
        _break_id: &str,
        _user_ids: &[u32],
        _now_ms: i64,
        _minimum_interval_ms: i64,
        _policy_version: u64,
    ) -> Result<bool> {
        Ok(false)
    }
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
