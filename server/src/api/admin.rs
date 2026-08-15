use anyhow::Error as AnyError;
use axum::{
    Extension, Json,
    extract::{Query, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Deserializer};
use tracing::error;

use crate::db::models::{
    MatchHistoryPage, PublicRuntimeConfig, RuntimeAdsConfig, RuntimeAdsDistributionsConfig,
    RuntimeAnnouncementConfig, RuntimeConfig, RuntimeConfigActor, RuntimeConfigAuditPage,
    RuntimeConfigRecord, RuntimeDistributionAdsConfig, RuntimeHistoryConfig,
};

use super::auth::AuthState;
use super::middleware::AuthUser;

const DEFAULT_PAGE_SIZE: usize = 25;
const MAX_PAGE_SIZE: usize = 50;
const MAX_CURSOR_BYTES: usize = 4 * 1024;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PageQuery {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StrictRuntimeAnnouncementConfig {
    enabled: bool,
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StrictRuntimeAdsConfig {
    enabled: bool,
    minimum_games_played: u32,
    minimum_interval_minutes: u16,
    distributions: StrictRuntimeAdsDistributionsConfig,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StrictRuntimeAdsDistributionsConfig {
    web: StrictRuntimeDistributionAdsConfig,
    crazygames: StrictRuntimeDistributionAdsConfig,
    itch: StrictRuntimeDistributionAdsConfig,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StrictRuntimeDistributionAdsConfig {
    enabled: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StrictRuntimeHistoryConfig {
    snapshot_retention_days: u16,
    summary_retention_days: u16,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StrictRuntimeConfig {
    announcement: StrictRuntimeAnnouncementConfig,
    ads: StrictRuntimeAdsConfig,
    history: StrictRuntimeHistoryConfig,
}

impl From<StrictRuntimeConfig> for RuntimeConfig {
    fn from(config: StrictRuntimeConfig) -> Self {
        Self {
            announcement: RuntimeAnnouncementConfig {
                enabled: config.announcement.enabled,
                message: config.announcement.message,
            },
            ads: RuntimeAdsConfig {
                enabled: config.ads.enabled,
                minimum_games_played: config.ads.minimum_games_played,
                minimum_interval_minutes: config.ads.minimum_interval_minutes,
                distributions: RuntimeAdsDistributionsConfig {
                    web: RuntimeDistributionAdsConfig {
                        enabled: config.ads.distributions.web.enabled,
                    },
                    crazygames: RuntimeDistributionAdsConfig {
                        enabled: config.ads.distributions.crazygames.enabled,
                    },
                    itch: RuntimeDistributionAdsConfig {
                        enabled: config.ads.distributions.itch.enabled,
                    },
                },
            },
            history: RuntimeHistoryConfig {
                snapshot_retention_days: config.history.snapshot_retention_days,
                summary_retention_days: config.history.summary_retention_days,
            },
        }
    }
}

fn deserialize_runtime_config_strict<'de, D>(deserializer: D) -> Result<RuntimeConfig, D::Error>
where
    D: Deserializer<'de>,
{
    StrictRuntimeConfig::deserialize(deserializer).map(Into::into)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct UpdateRuntimeConfigRequest {
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub expected_version: u64,
    #[serde(deserialize_with = "deserialize_runtime_config_strict")]
    pub config: RuntimeConfig,
}

#[derive(Debug)]
pub enum AdminApiError {
    BadRequest(String),
    Conflict,
    Internal(AnyError),
}

impl AdminApiError {
    fn database(error: AnyError) -> Self {
        let message = error.to_string().to_ascii_lowercase();
        if message.starts_with("invalid history cursor:")
            || message.starts_with("invalid config audit cursor:")
        {
            Self::BadRequest("Invalid pagination cursor".to_string())
        } else if (message.contains("runtime config") && message.contains("conflict"))
            || message.contains("version conflict")
        {
            Self::Conflict
        } else {
            Self::Internal(error)
        }
    }
}

impl IntoResponse for AdminApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            Self::Conflict => (
                StatusCode::CONFLICT,
                "Runtime configuration changed; reload it and retry".to_string(),
            ),
            Self::Internal(error) => {
                error!(?error, "history/configuration API error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error".to_string(),
                )
            }
        };
        let mut response = (status, Json(serde_json::json!({ "error": message }))).into_response();
        apply_no_store(&mut response);
        response
    }
}

fn page_options(query: &PageQuery) -> Result<(usize, Option<&str>), AdminApiError> {
    let limit = query.limit.unwrap_or(DEFAULT_PAGE_SIZE);
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return Err(AdminApiError::BadRequest(format!(
            "limit must be between 1 and {MAX_PAGE_SIZE}"
        )));
    }
    let cursor = query.cursor.as_deref();
    if cursor.is_some_and(|cursor| cursor.is_empty() || cursor.len() > MAX_CURSOR_BYTES) {
        return Err(AdminApiError::BadRequest(
            "Invalid pagination cursor".to_string(),
        ));
    }
    Ok((limit, cursor))
}

fn apply_no_store(response: &mut Response) {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, must-revalidate"),
    );
    response
        .headers_mut()
        .insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
}

fn apply_public_config_cache(response: &mut Response) {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=30"),
    );
}

/// Return only the current authenticated user's compact history projection.
pub async fn get_user_history(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    Query(query): Query<PageQuery>,
) -> Result<Json<MatchHistoryPage>, AdminApiError> {
    let (limit, cursor) = page_options(&query)?;
    let mut page = state
        .db
        .get_match_history(auth_user.user_id, limit, cursor)
        .await
        .map_err(AdminApiError::database)?;
    redact_history_for_user(&mut page, auth_user.user_id);
    Ok(Json(page))
}

/// The durable projection is shared with administrators so it is written only
/// once, but a player endpoint must expose only that player's result fields.
fn redact_history_for_user(page: &mut MatchHistoryPage, user_id: i32) {
    page.entries.iter_mut().for_each(|entry| {
        entry
            .players
            .retain(|player| i32::try_from(player.user_id) == Ok(user_id));
        entry
            .winner_user_ids
            .retain(|winner| i32::try_from(*winner) == Ok(user_id));
    });
}

/// Public, deliberately redacted configuration used by the game client.
pub async fn get_public_config(State(state): State<AuthState>) -> Result<Response, AdminApiError> {
    let record = state
        .db
        .get_runtime_config()
        .await
        .map_err(AdminApiError::database)?;
    let mut response = Json(PublicRuntimeConfig::from(&record)).into_response();
    apply_public_config_cache(&mut response);
    Ok(response)
}

pub async fn get_admin_history(
    State(state): State<AuthState>,
    Query(query): Query<PageQuery>,
) -> Result<Json<MatchHistoryPage>, AdminApiError> {
    let (limit, cursor) = page_options(&query)?;
    state
        .db
        .get_admin_match_history(limit, cursor)
        .await
        .map(Json)
        .map_err(AdminApiError::database)
}

pub async fn get_admin_config(
    State(state): State<AuthState>,
) -> Result<Json<RuntimeConfigRecord>, AdminApiError> {
    state
        .db
        .get_runtime_config()
        .await
        .map(Json)
        .map_err(AdminApiError::database)
}

pub async fn update_admin_config(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    Json(request): Json<UpdateRuntimeConfigRequest>,
) -> Result<Json<RuntimeConfigRecord>, AdminApiError> {
    request
        .config
        .validate()
        .map_err(AdminApiError::BadRequest)?;
    let actor = RuntimeConfigActor {
        user_id: auth_user.user_id,
        username: auth_user.username,
    };
    state
        .db
        .update_runtime_config(request.expected_version, &request.config, &actor)
        .await
        .map(Json)
        .map_err(AdminApiError::database)
}

pub async fn get_config_audit(
    State(state): State<AuthState>,
    Query(query): Query<PageQuery>,
) -> Result<Json<RuntimeConfigAuditPage>, AdminApiError> {
    let (limit, cursor) = page_options(&query)?;
    state
        .db
        .get_runtime_config_audit(limit, cursor)
        .await
        .map(Json)
        .map_err(AdminApiError::database)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pagination_is_strict_and_bounded() {
        assert_eq!(
            page_options(&PageQuery {
                limit: None,
                cursor: None
            })
            .unwrap()
            .0,
            25
        );
        assert!(
            page_options(&PageQuery {
                limit: Some(0),
                cursor: None
            })
            .is_err()
        );
        assert!(
            page_options(&PageQuery {
                limit: Some(51),
                cursor: None
            })
            .is_err()
        );
        assert!(
            page_options(&PageQuery {
                limit: Some(10),
                cursor: Some(String::new()),
            })
            .is_err()
        );
    }

    #[test]
    fn config_update_rejects_unknown_fields() {
        let top_level = serde_json::from_value::<UpdateRuntimeConfigRequest>(serde_json::json!({
            "expectedVersion": 0,
            "config": RuntimeConfig::default(),
            "unexpected": true
        }));
        assert!(top_level.is_err());

        let nested = serde_json::from_value::<UpdateRuntimeConfigRequest>(serde_json::json!({
            "expectedVersion": 0,
            "config": {
                "announcement": {
                    "enabled": false,
                    "message": ""
                },
                "ads": {
                    "enabled": false,
                    "minimumGamesPlayed": 1,
                    "minimumIntervalMinutes": 10,
                    "distributions": {
                        "web": { "enabled": false },
                        "crazygames": { "enabled": false },
                        "itch": { "enabled": false }
                    },
                    "unexpected": true
                },
                "history": {
                    "snapshotRetentionDays": 30,
                    "summaryRetentionDays": 365
                }
            }
        }));
        assert!(nested.is_err());
    }

    #[test]
    fn config_update_accepts_explicit_distribution_policy() {
        let request = serde_json::from_value::<UpdateRuntimeConfigRequest>(serde_json::json!({
            "expectedVersion": 3,
            "config": {
                "announcement": {
                    "enabled": false,
                    "message": ""
                },
                "ads": {
                    "enabled": true,
                    "minimumGamesPlayed": 2,
                    "minimumIntervalMinutes": 15,
                    "distributions": {
                        "web": { "enabled": false },
                        "crazygames": { "enabled": true },
                        "itch": { "enabled": false }
                    }
                },
                "history": {
                    "snapshotRetentionDays": 30,
                    "summaryRetentionDays": 365
                }
            }
        }))
        .unwrap();

        assert!(request.config.ads.enabled);
        assert_eq!(request.config.ads.minimum_games_played, 2);
        assert_eq!(request.config.ads.minimum_interval_minutes, 15);
        assert!(!request.config.ads.distributions.web.enabled);
        assert!(request.config.ads.distributions.crazygames.enabled);
        assert!(!request.config.ads.distributions.itch.enabled);
    }

    #[test]
    fn cursor_database_errors_are_bad_requests() {
        let response = AdminApiError::database(anyhow::anyhow!(
            "invalid history cursor: malformed encoding"
        ))
        .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn player_history_redacts_every_other_players_progression() {
        use crate::db::models::{MatchHistoryPlayer, MatchHistorySummary};

        let player = |user_id, outcome: &str| MatchHistoryPlayer {
            user_id,
            username: format!("player-{user_id}"),
            team_id: None,
            score: 10,
            team_score: None,
            xp_gained: 5,
            mmr_delta: Some(2),
            outcome: outcome.to_string(),
        };
        let mut page = MatchHistoryPage {
            entries: vec![MatchHistorySummary {
                schema_version: 1,
                game_id: 99,
                started_at_ms: 1,
                ended_at_ms: 2,
                duration_ms: 1,
                mode: "duel".to_string(),
                mode_label: "Duel".to_string(),
                queue_mode: "competitive".to_string(),
                is_private: false,
                is_stress_test: false,
                completed_by_inactivity: false,
                players: vec![player(7, "win"), player(8, "loss")],
                winner_user_ids: vec![7],
                snapshot_available_until_ms: 3,
            }],
            next_cursor: None,
        };

        redact_history_for_user(&mut page, 8);

        assert_eq!(page.entries[0].players.len(), 1);
        assert_eq!(page.entries[0].players[0].user_id, 8);
        assert!(page.entries[0].winner_user_ids.is_empty());
    }
}
