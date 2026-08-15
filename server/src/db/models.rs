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
    /// Lifetime completed matches across modes, regions, and seasons.
    #[serde(default)]
    pub games_played: i32,
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
    /// Season captured when the completion became durable. Legacy rows do not
    /// have this field and are deliberately excluded from seasonal news.
    #[serde(default)]
    pub season: Option<crate::season::Season>,
    pub game_type: JsonValue,
    pub game_state: Option<JsonValue>,
    pub status: String,
    pub ended_at: Option<DateTime<Utc>>,
    pub last_activity: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub game_mode: String,
    pub is_private: bool,
    pub game_code: Option<String>,
    /// Durable object-store reference for the complete replay recording.
    /// Legacy games and completions produced before replay capture omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_object: Option<crate::replay_store::ReplayObjectMetadata>,
    /// Canonical server-selected Play-of-the-Game clip. A completed game may
    /// legitimately omit it when no candidate clears the scoring threshold.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub play_of_the_game: Option<common::HighlightClip>,
    /// Internal proof that completion persistence verified this result as a
    /// public source. Legacy rows without proof fail closed for news.
    #[serde(default, skip_serializing)]
    pub news_eligible: bool,
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
    #[serde(default)]
    pub ad_break: Option<crate::ads::LobbyAdBreak>,
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
    /// Whether the source game was verified public when this row was written.
    /// Legacy rows lack that proof and intentionally fail closed.
    #[serde(default, skip_serializing)]
    pub news_eligible: bool,
}

/// Whether a news read came from the globally ordered leaderboard index or
/// from a bounded, unordered compatibility scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NewsLeaderboardCoverage {
    OrderedGlobalIndex,
    BoundedSample,
}

/// An optional, proven Solo leader. The winning row's immutable timestamp is
/// when that score was posted.
#[derive(Debug, Clone)]
pub struct NewsHighScoreSnapshot {
    pub leader: Option<HighScoreEntry>,
    pub coverage: NewsLeaderboardCoverage,
}

/// A player-shaped projection of an immutable completed match. History rows
/// intentionally contain only result data; the substantially larger final
/// `GameState` remains on the completed-game item for bounded snapshot reloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct MatchHistoryPlayer {
    pub user_id: u32,
    pub username: String,
    pub team_id: Option<u8>,
    pub score: u32,
    pub team_score: Option<u32>,
    pub xp_gained: u32,
    pub mmr_delta: Option<i32>,
    pub outcome: String,
}

/// Compact, versioned match result shared by the player History modal and the
/// administrative history view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct MatchHistorySummary {
    pub schema_version: u16,
    pub game_id: u32,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub started_at_ms: i64,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub ended_at_ms: i64,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub duration_ms: u64,
    pub mode: String,
    pub mode_label: String,
    pub queue_mode: String,
    pub is_private: bool,
    pub is_stress_test: bool,
    pub completed_by_inactivity: bool,
    pub players: Vec<MatchHistoryPlayer>,
    pub winner_user_ids: Vec<u32>,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub snapshot_available_until_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct MatchHistoryPage {
    pub entries: Vec<MatchHistorySummary>,
    pub next_cursor: Option<String>,
}

impl MatchHistoryPage {
    pub fn empty() -> Self {
        Self {
            entries: Vec::new(),
            next_cursor: None,
        }
    }
}

pub const RUNTIME_CONFIG_SCHEMA_VERSION: u16 = 2;

const fn runtime_config_schema_version() -> u16 {
    RUNTIME_CONFIG_SCHEMA_VERSION
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct RuntimeAnnouncementConfig {
    pub enabled: bool,
    pub message: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct RuntimeDistributionAdsConfig {
    pub enabled: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct RuntimeAdsDistributionsConfig {
    pub web: RuntimeDistributionAdsConfig,
    pub crazygames: RuntimeDistributionAdsConfig,
    pub itch: RuntimeDistributionAdsConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct RuntimeAdsConfig {
    pub enabled: bool,
    pub minimum_games_played: u32,
    pub minimum_interval_minutes: u16,
    pub distributions: RuntimeAdsDistributionsConfig,
}

impl Default for RuntimeAdsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            minimum_games_played: 1,
            minimum_interval_minutes: 10,
            distributions: RuntimeAdsDistributionsConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct RuntimeHistoryConfig {
    pub snapshot_retention_days: u16,
    pub summary_retention_days: u16,
}

impl Default for RuntimeHistoryConfig {
    fn default() -> Self {
        Self {
            snapshot_retention_days: 30,
            summary_retention_days: 365,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct RuntimeConfig {
    pub announcement: RuntimeAnnouncementConfig,
    pub ads: RuntimeAdsConfig,
    pub history: RuntimeHistoryConfig,
}

impl RuntimeConfig {
    pub const MAX_ANNOUNCEMENT_CHARACTERS: usize = 280;
    pub const MAX_AD_MINIMUM_GAMES_PLAYED: u32 = 10_000;
    pub const MAX_AD_INTERVAL_MINUTES: u16 = 24 * 60;
    pub const MAX_HISTORY_RETENTION_DAYS: u16 = 3650;

    pub fn validate(&self) -> Result<(), String> {
        let message = self.announcement.message.trim();
        if self.announcement.enabled && message.is_empty() {
            return Err("announcement message is required when the announcement is enabled".into());
        }
        if self.announcement.message.chars().count() > Self::MAX_ANNOUNCEMENT_CHARACTERS {
            return Err(format!(
                "announcement message must be at most {} characters",
                Self::MAX_ANNOUNCEMENT_CHARACTERS
            ));
        }
        if self.announcement.message.chars().any(char::is_control) {
            return Err("announcement message must not contain control characters".into());
        }
        if self.ads.minimum_games_played > Self::MAX_AD_MINIMUM_GAMES_PLAYED {
            return Err(format!(
                "minimum games played must be at most {}",
                Self::MAX_AD_MINIMUM_GAMES_PLAYED
            ));
        }
        if !(1..=Self::MAX_AD_INTERVAL_MINUTES).contains(&self.ads.minimum_interval_minutes) {
            return Err(format!(
                "minimum ad interval must be between 1 and {} minutes",
                Self::MAX_AD_INTERVAL_MINUTES
            ));
        }
        if !(1..=Self::MAX_HISTORY_RETENTION_DAYS).contains(&self.history.snapshot_retention_days) {
            return Err(format!(
                "snapshot retention must be between 1 and {} days",
                Self::MAX_HISTORY_RETENTION_DAYS
            ));
        }
        if !(1..=Self::MAX_HISTORY_RETENTION_DAYS).contains(&self.history.summary_retention_days) {
            return Err(format!(
                "summary retention must be between 1 and {} days",
                Self::MAX_HISTORY_RETENTION_DAYS
            ));
        }
        if self.history.summary_retention_days < self.history.snapshot_retention_days {
            return Err("summary retention must be at least snapshot retention".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct RuntimeConfigActor {
    pub user_id: i32,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct RuntimeConfigRecord {
    #[serde(default = "runtime_config_schema_version")]
    pub schema_version: u16,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub version: u64,
    pub config: RuntimeConfig,
    pub updated_by: Option<RuntimeConfigActor>,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub updated_at_ms: i64,
}

/// Public, read-only projection of runtime settings. Administrative metadata
/// and internal retention policy are deliberately omitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct PublicRuntimeConfig {
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub version: u64,
    pub announcement: RuntimeAnnouncementConfig,
}

impl From<&RuntimeConfigRecord> for PublicRuntimeConfig {
    fn from(record: &RuntimeConfigRecord) -> Self {
        Self {
            version: record.version,
            announcement: record.config.announcement.clone(),
        }
    }
}

impl Default for RuntimeConfigRecord {
    fn default() -> Self {
        Self {
            schema_version: RUNTIME_CONFIG_SCHEMA_VERSION,
            version: 0,
            config: RuntimeConfig::default(),
            updated_by: None,
            updated_at_ms: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct RuntimeConfigAuditPage {
    pub entries: Vec<RuntimeConfigRecord>,
    pub next_cursor: Option<String>,
}

impl RuntimeConfigAuditPage {
    pub fn empty() -> Self {
        Self {
            entries: Vec::new(),
            next_cursor: None,
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_config_defaults_are_safe_and_valid() {
        let config = RuntimeConfig::default();
        assert_eq!(RUNTIME_CONFIG_SCHEMA_VERSION, 2);
        assert!(!config.announcement.enabled);
        assert!(config.announcement.message.is_empty());
        assert!(!config.ads.enabled);
        assert_eq!(config.ads.minimum_games_played, 1);
        assert_eq!(config.ads.minimum_interval_minutes, 10);
        assert!(!config.ads.distributions.web.enabled);
        assert!(!config.ads.distributions.crazygames.enabled);
        assert!(!config.ads.distributions.itch.enabled);
        assert_eq!(config.history.snapshot_retention_days, 30);
        assert_eq!(config.history.summary_retention_days, 365);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn runtime_config_rejects_invalid_retention_and_announcement() {
        let mut config = RuntimeConfig::default();
        config.announcement.enabled = true;
        assert!(config.validate().is_err());

        config.announcement.message = "Maintenance soon".to_string();
        config.history.snapshot_retention_days = 366;
        config.history.summary_retention_days = 365;
        assert!(config.validate().is_err());

        config.history.snapshot_retention_days = 30;
        config.ads.minimum_games_played = RuntimeConfig::MAX_AD_MINIMUM_GAMES_PLAYED + 1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn public_runtime_config_omits_admin_metadata_and_retention() {
        let record = RuntimeConfigRecord {
            schema_version: RUNTIME_CONFIG_SCHEMA_VERSION,
            version: 3,
            config: RuntimeConfig::default(),
            updated_by: Some(RuntimeConfigActor {
                user_id: 7,
                username: "operator".to_string(),
            }),
            updated_at_ms: 123,
        };
        let value = serde_json::to_value(PublicRuntimeConfig::from(&record)).unwrap();
        assert_eq!(value["version"], 3);
        assert!(value.get("announcement").is_some());
        assert!(value.get("ads").is_none());
        assert!(value.get("history").is_none());
        assert!(value.get("updatedBy").is_none());
        assert!(value.get("updatedAtMs").is_none());
    }

    #[test]
    fn persisted_runtime_config_tolerates_missing_and_unknown_fields() {
        let config: RuntimeConfig = serde_json::from_value(serde_json::json!({
            "ads": {
                "enabled": true,
                "minimumGamesPlayed": 4,
                "distributions": {
                    "web": { "enabled": true },
                    "crazygames": { "enabled": false },
                    "itch": { "enabled": false }
                },
                "futureAdSetting": "ignored"
            },
            "futureSection": {
                "enabled": true
            }
        }))
        .unwrap();

        assert_eq!(config.announcement, RuntimeAnnouncementConfig::default());
        assert!(config.ads.enabled);
        assert_eq!(config.ads.minimum_games_played, 4);
        assert_eq!(config.ads.minimum_interval_minutes, 10);
        assert!(config.ads.distributions.web.enabled);
        assert!(!config.ads.distributions.crazygames.enabled);
        assert!(!config.ads.distributions.itch.enabled);
        assert_eq!(config.history, RuntimeHistoryConfig::default());
    }

    #[test]
    fn legacy_runtime_config_record_defaults_schema_version() {
        let record: RuntimeConfigRecord = serde_json::from_value(serde_json::json!({
            "version": 4,
            "config": {},
            "updatedBy": null,
            "updatedAtMs": 123
        }))
        .unwrap();

        assert_eq!(record.schema_version, RUNTIME_CONFIG_SCHEMA_VERSION);
        assert_eq!(record.config, RuntimeConfig::default());
    }
}
