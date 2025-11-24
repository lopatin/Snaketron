use axum::{Json, extract::{Query, State}};
use serde::{Deserialize, Serialize};
use tracing::{error, warn};
use std::sync::Arc;

use crate::db::Database;
use crate::season::get_current_season;
use common::{QueueMode, GameType};

/// Query parameters for leaderboard endpoint
#[derive(Debug, Deserialize)]
pub struct LeaderboardQuery {
    /// Queue mode: "quickmatch" or "competitive"
    pub queue_mode: String,
    /// Game type: "solo", "duel", "2v2", "ffa"
    pub game_type: String,
    /// Season (optional, defaults to current season)
    pub season: Option<String>,
    /// Number of entries to return (default: 25, max: 100)
    pub limit: Option<usize>,
    /// Offset for pagination (default: 0)
    pub offset: Option<usize>,
}

/// Leaderboard entry response format for frontend
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

/// Leaderboard response
#[derive(Debug, Serialize)]
pub struct LeaderboardResponse {
    pub entries: Vec<LeaderboardEntryResponse>,
    pub season: String,
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
    pub seasons: Vec<String>,
    pub current: String,
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

    // Query leaderboard from database (global rankings, no region filter)
    let entries = match state.db.get_leaderboard(
        &queue_mode,
        Some(&game_type),
        None, // region = None for global rankings
        &season,
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
                season: season.clone(),
                queue_mode: query.queue_mode,
                game_type: query.game_type,
                has_more: false,
            });
        }
    };

    // Check if there are more results
    let has_more = entries.len() > limit;

    // Transform entries to response format
    let response_entries: Vec<LeaderboardEntryResponse> = entries
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

            LeaderboardEntryResponse {
                rank: offset + idx + 1,
                username: entry.username,
                mmr: entry.mmr,
                wins: entry.wins,
                losses: entry.losses,
                win_rate,
            }
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
    State(state): State<LeaderboardState>,
) -> Json<SeasonsResponse> {
    // For now, we'll return the current season and a few past seasons
    // In the future, this could query DynamoDB for all available ranking tables
    let current_season = get_current_season();

    // Generate past seasons (current season and last 3 seasons)
    // Season format: YYYY-SN where N is 1-4 (quarterly)
    let mut seasons = vec![current_season.clone()];

    // Parse current season to generate past seasons
    if let Some((year_str, season_num_str)) = current_season.split_once("-S") {
        if let (Ok(mut year), Ok(mut season_num)) = (year_str.parse::<i32>(), season_num_str.parse::<i32>()) {
            // Add previous 3 seasons
            for _ in 0..3 {
                season_num -= 1;
                if season_num < 1 {
                    season_num = 4;
                    year -= 1;
                }
                seasons.push(format!("{}-S{}", year, season_num));
            }
        }
    }

    Json(SeasonsResponse {
        seasons,
        current: current_season,
    })
}
