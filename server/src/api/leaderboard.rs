use axum::{Json, extract::{Query, State}, http::StatusCode, Extension};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};
use std::sync::Arc;

use crate::db::Database;
use crate::season::{get_current_season, Season};
use crate::api::middleware::AuthUser;
use common::{QueueMode, GameType};

/// Query parameters for leaderboard endpoint
#[derive(Debug, Deserialize)]
pub struct LeaderboardQuery {
    /// Queue mode: "quickmatch" or "competitive"
    pub queue_mode: String,
    /// Game type: "solo", "duel", "2v2", "ffa"
    pub game_type: String,
    /// Season (optional, defaults to current season)
    pub season: Option<Season>,
    /// Number of entries to return (default: 25, max: 100)
    pub limit: Option<usize>,
    /// Offset for pagination (default: 0)
    pub offset: Option<usize>,
    /// Region filter (optional, omit for global rankings)
    pub region: Option<String>,
}

/// Leaderboard entry response format for frontend (for ranked/competitive modes)
#[derive(Debug, Serialize)]
pub struct LeaderboardEntryResponse {
    pub rank: usize,
    pub username: String,
    pub mmr: i32,
    pub wins: i32,
    pub losses: i32,
    #[serde(rename = "winRate")]
    pub win_rate: f64,
}

/// High score entry response format for frontend (for solo mode)
#[derive(Debug, Serialize)]
pub struct HighScoreEntryResponse {
    pub rank: usize,
    pub username: String,
    pub score: i32,
    pub timestamp: String,
    #[serde(rename = "gameId")]
    pub game_id: String,
}

/// Leaderboard response (supports both ranking and high score entries)
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum LeaderboardEntry {
    Ranking(LeaderboardEntryResponse),
    HighScore(HighScoreEntryResponse),
}

/// Leaderboard response
#[derive(Debug, Serialize)]
pub struct LeaderboardResponse {
    pub entries: Vec<LeaderboardEntry>,
    pub season: Season,
    #[serde(rename = "queueMode")]
    pub queue_mode: String,
    #[serde(rename = "gameType")]
    pub game_type: String,
    #[serde(rename = "hasMore")]
    pub has_more: bool,
}

/// Seasons list response
#[derive(Debug, Serialize)]
pub struct SeasonsResponse {
    pub seasons: Vec<Season>,
    pub current: Season,
}

/// State for leaderboard endpoints (contains database)
#[derive(Clone)]
pub struct LeaderboardState {
    pub db: Arc<dyn Database>,
}

/// Get leaderboard rankings
/// Query parameters:
/// - queue_mode: "quickmatch" or "competitive"
/// - game_type: "solo", "duel", "2v2", "ffa"
/// - season: optional, defaults to current season
/// - limit: optional, defaults to 25, max 100
/// - offset: optional, defaults to 0
pub async fn get_leaderboard(
    State(state): State<LeaderboardState>,
    Query(query): Query<LeaderboardQuery>,
) -> Json<LeaderboardResponse> {
    // Parse queue mode
    let queue_mode = match query.queue_mode.to_lowercase().as_str() {
        "quickmatch" | "casual" => QueueMode::Quickmatch,
        "competitive" | "ranked" => QueueMode::Competitive,
        _ => {
            warn!("Invalid queue_mode: {}, defaulting to Quickmatch", query.queue_mode);
            QueueMode::Quickmatch
        }
    };

    // Parse game type
    let game_type = match query.game_type.to_lowercase().as_str() {
        "solo" => GameType::Solo,
        "duel" | "1v1" => GameType::TeamMatch { per_team: 1 },
        "2v2" => GameType::TeamMatch { per_team: 2 },
        "ffa" | "free-for-all" => GameType::FreeForAll { max_players: 8 },
        _ => {
            warn!("Invalid game_type: {}, defaulting to Solo", query.game_type);
            GameType::Solo
        }
    };

    // Get season (default to current)
    let season = query.season.unwrap_or_else(get_current_season);

    // Parse limit and offset with constraints
    let limit = query.limit.unwrap_or(25).min(100).max(1);
    let offset = query.offset.unwrap_or(0);

    // Fetch one extra entry to determine if there are more results
    let fetch_limit = limit + 1;

    // For Solo mode, fetch high scores instead of rankings
    if matches!(game_type, GameType::Solo) {
        info!(
            "Fetching Solo high scores - region: {:?}, season: {}, limit: {}, offset: {}",
            query.region.as_deref(),
            season,
            limit,
            offset
        );

        let high_scores = match state.db.get_high_scores(
            &game_type,
            query.region.as_deref(),
            season,
            offset + fetch_limit,
        ).await {
            Ok(mut scores) => {
                info!("Fetched {} high scores from database", scores.len());
                // Skip entries up to offset
                scores.drain(..offset.min(scores.len()));
                info!("After offset, {} high scores remain", scores.len());
                scores
            }
            Err(e) => {
                error!("Failed to fetch high scores: {:?}", e);
                return Json(LeaderboardResponse {
                    entries: vec![],
                    season,
                    queue_mode: query.queue_mode,
                    game_type: query.game_type,
                    has_more: false,
                });
            }
        };

        // Check if there are more results
        let has_more = high_scores.len() > limit;

        // Transform high scores to response format
        let response_entries: Vec<LeaderboardEntry> = high_scores
            .into_iter()
            .take(limit)
            .enumerate()
            .map(|(idx, entry)| {
                LeaderboardEntry::HighScore(HighScoreEntryResponse {
                    rank: offset + idx + 1,
                    username: entry.username,
                    score: entry.score,
                    timestamp: entry.timestamp.to_rfc3339(),
                    game_id: entry.game_id,
                })
            })
            .collect();

        info!(
            "Returning {} high score entries (has_more: {})",
            response_entries.len(),
            has_more
        );

        return Json(LeaderboardResponse {
            entries: response_entries,
            season,
            queue_mode: query.queue_mode,
            game_type: query.game_type,
            has_more,
        });
    }

    // For non-Solo modes, query rankings (existing logic)
    let entries = match state.db.get_leaderboard(
        &queue_mode,
        Some(&game_type),
        query.region.as_deref(), // Pass region if specified, None for global
        season,
        offset + fetch_limit, // Fetch up to offset + limit + 1
    ).await {
        Ok(mut entries) => {
            // Skip entries up to offset
            entries.drain(..offset.min(entries.len()));
            entries
        }
        Err(e) => {
            error!("Failed to fetch leaderboard: {:?}", e);
            return Json(LeaderboardResponse {
                entries: vec![],
                season,
                queue_mode: query.queue_mode,
                game_type: query.game_type,
                has_more: false,
            });
        }
    };

    // Check if there are more results
    let has_more = entries.len() > limit;

    // Transform entries to response format
    let response_entries: Vec<LeaderboardEntry> = entries
        .into_iter()
        .take(limit) // Take only the requested limit
        .enumerate()
        .map(|(idx, entry)| {
            let total_games = entry.wins + entry.losses;
            let win_rate = if total_games > 0 {
                (entry.wins as f64 / total_games as f64) * 100.0
            } else {
                0.0
            };

            LeaderboardEntry::Ranking(LeaderboardEntryResponse {
                rank: offset + idx + 1,
                username: entry.username,
                mmr: entry.mmr,
                wins: entry.wins,
                losses: entry.losses,
                win_rate,
            })
        })
        .collect();

    Json(LeaderboardResponse {
        entries: response_entries,
        season,
        queue_mode: query.queue_mode,
        game_type: query.game_type,
        has_more,
    })
}

/// List available seasons
/// Returns a list of all seasons that have ranking data
pub async fn list_seasons(
    State(_state): State<LeaderboardState>,
) -> Json<SeasonsResponse> {
    // Placeholder: return only the current season until season schedule/roller exists
    let current_season = get_current_season();
    let seasons = vec![current_season];

    Json(SeasonsResponse {
        seasons,
        current: current_season,
    })
}

/// User ranking response
#[derive(Debug, Serialize)]
pub struct UserRankingResponse {
    pub rank: Option<usize>,
    pub mmr: Option<i32>,
    pub wins: Option<i32>,
    pub losses: Option<i32>,
    #[serde(rename = "winRate")]
    pub win_rate: Option<f64>,
}

/// Get current user's ranking
/// Query parameters: queue_mode, game_type, season (optional), region (optional)
pub async fn get_my_ranking(
    Extension(auth_user): Extension<AuthUser>,
    State(state): State<LeaderboardState>,
    Query(query): Query<LeaderboardQuery>,
) -> Result<Json<UserRankingResponse>, StatusCode> {
    // Parse queue mode
    let queue_mode = match query.queue_mode.to_lowercase().as_str() {
        "quickmatch" | "casual" => QueueMode::Quickmatch,
        "competitive" | "ranked" => QueueMode::Competitive,
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    // Parse game type
    let game_type = match query.game_type.to_lowercase().as_str() {
        "solo" => GameType::Solo,
        "duel" | "1v1" => GameType::TeamMatch { per_team: 1 },
        "2v2" => GameType::TeamMatch { per_team: 2 },
        "ffa" | "free-for-all" => GameType::FreeForAll { max_players: 8 },
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    // Get season (default to current)
    let season = query.season.unwrap_or_else(get_current_season);

    // Get region (default to us-east-1 if not specified)
    let region = query.region.as_deref().unwrap_or("us-east-1");

    // Get user's ranking from database
    let ranking = match state.db.get_user_ranking(
        auth_user.user_id,
        &queue_mode,
        &game_type,
        region,
        season,
    ).await {
        Ok(Some(entry)) => {
            // Calculate rank by querying all entries with higher MMR
            let all_entries = state.db.get_leaderboard(
                &queue_mode,
                Some(&game_type),
                Some(region),
                season,
                10000, // Large limit to get all entries
            ).await.unwrap_or_default();

            let rank = all_entries.iter()
                .position(|e| e.user_id == auth_user.user_id)
                .map(|pos| pos + 1);

            let total_games = entry.wins + entry.losses;
            let win_rate = if total_games > 0 {
                Some((entry.wins as f64 / total_games as f64) * 100.0)
            } else {
                None
            };

            UserRankingResponse {
                rank,
                mmr: Some(entry.mmr),
                wins: Some(entry.wins),
                losses: Some(entry.losses),
                win_rate,
            }
        }
        Ok(None) | Err(_) => {
            // User has no ranking yet
            UserRankingResponse {
                rank: None,
                mmr: None,
                wins: None,
                losses: None,
                win_rate: None,
            }
        }
    };

    Ok(Json(ranking))
}
