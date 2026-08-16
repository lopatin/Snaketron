use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tracing::{error, info, warn};

use crate::api::middleware::AuthUser;
use crate::db::Database;
use crate::season::{Season, get_current_season, get_ranking_region, get_season_at, seasons_at};
use common::{GameType, QueueMode};

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
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct LeaderboardEntryResponse {
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub rank: usize,
    /// The account behind the row. Display names are not unique — guests are
    /// exempt from the username index — so this is the only way a client can
    /// tell which row is the signed-in player's. The board and
    /// [`get_my_ranking`] answer questions about different regions, so their
    /// rank numbers must never be compared to find that row. Account IDs are
    /// already visible to every client through lobby and gameplay messages.
    #[serde(rename = "userId")]
    pub user_id: i32,
    pub username: String,
    pub mmr: i32,
    pub wins: i32,
    pub losses: i32,
    #[serde(rename = "winRate")]
    pub win_rate: f64,
    /// Whether the account is a guest, so a name-alike can be told apart from
    /// a registered account of the same name.
    #[serde(rename = "isGuest")]
    pub is_guest: bool,
}

/// High score entry response format for frontend (for solo mode)
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct HighScoreEntryResponse {
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub rank: usize,
    /// See [`LeaderboardEntryResponse::user_id`].
    #[serde(rename = "userId")]
    pub user_id: i32,
    pub username: String,
    pub score: i32,
    pub timestamp: String,
    #[serde(rename = "gameId")]
    pub game_id: String,
    /// See [`LeaderboardEntryResponse::is_guest`]. High-score names are
    /// historical snapshots, so this reflects the account today.
    #[serde(rename = "isGuest")]
    pub is_guest: bool,
}

/// Leaderboard response (supports both ranking and high score entries)
#[derive(Debug, Serialize)]
#[serde(untagged)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum LeaderboardEntry {
    Ranking(LeaderboardEntryResponse),
    HighScore(HighScoreEntryResponse),
}

/// Leaderboard response
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
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
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct SeasonsResponse {
    pub seasons: Vec<Season>,
    pub current: Season,
}

/// State for leaderboard endpoints (contains database)
#[derive(Clone)]
pub struct LeaderboardState {
    pub db: Arc<dyn Database>,
}

/// Get leaderboard rankings.
///
/// Query parameters:
/// - queue_mode: "quickmatch" or "competitive"
/// - game_type: "solo", "duel", "2v2", "ffa"
/// - season: optional, defaults to current season
/// - limit: optional, defaults to 25, max 100
/// - offset: optional, defaults to 0
/// - region: optional; **omitting it returns the true global ladder across
///   every region**, which is what the Global selection asks for.
///
/// The absent-region case deliberately means something different here than in
/// [`get_my_ranking`], which is always about one region. Do not "fix" the two
/// into agreement: a player's own badge is regional by design, while this
/// board spans regions.
pub async fn get_leaderboard(
    State(state): State<LeaderboardState>,
    Query(query): Query<LeaderboardQuery>,
) -> Json<LeaderboardResponse> {
    // Parse queue mode
    let queue_mode = match query.queue_mode.to_lowercase().as_str() {
        "quickmatch" | "casual" => QueueMode::Quickmatch,
        "competitive" | "ranked" => QueueMode::Competitive,
        _ => {
            warn!(
                "Invalid queue_mode: {}, defaulting to Quickmatch",
                query.queue_mode
            );
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
    let ranking_region = query
        .region
        .as_deref()
        .map(|region| get_ranking_region(Some(region)));

    // Parse limit and offset with constraints
    let limit = query.limit.unwrap_or(25).clamp(1, 100);
    let offset = query.offset.unwrap_or(0);

    // Fetch one extra entry to determine if there are more results
    let fetch_limit = limit + 1;

    // For Solo mode, fetch high scores instead of rankings
    if matches!(game_type, GameType::Solo) {
        info!(
            "Fetching Solo high scores - region: {:?}, season: {}, limit: {}, offset: {}",
            ranking_region.as_deref(),
            season,
            limit,
            offset
        );

        let high_scores = match state
            .db
            .get_high_scores(
                &game_type,
                ranking_region.as_deref(),
                season,
                offset + fetch_limit,
            )
            .await
        {
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

        // Preserve the established constant-read path. Names in score rows are
        // historical snapshots; current verified profile data is used for
        // active account/lobby identity without amplifying public reads. The
        // one addition is a single batched guest-status read for the page.
        let high_scores: Vec<_> = high_scores.into_iter().take(limit).collect();
        let guest_flags = guest_flags_for(
            &state,
            &high_scores
                .iter()
                .map(|entry| entry.user_id)
                .collect::<Vec<_>>(),
        )
        .await;

        let response_entries: Vec<LeaderboardEntry> = high_scores
            .into_iter()
            .enumerate()
            .map(|(idx, entry)| {
                LeaderboardEntry::HighScore(HighScoreEntryResponse {
                    rank: offset + idx + 1,
                    user_id: entry.user_id,
                    is_guest: guest_flags.get(&entry.user_id).copied().unwrap_or(false),
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
    let entries = match state
        .db
        .get_leaderboard(
            &queue_mode,
            Some(&game_type),
            ranking_region.as_deref(), // Pass region if specified, None for global
            season,
            offset + fetch_limit, // Fetch up to offset + limit + 1
        )
        .await
    {
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
    let entries: Vec<_> = entries.into_iter().take(limit).collect();
    let guest_flags = guest_flags_for(
        &state,
        &entries
            .iter()
            .map(|entry| entry.user_id)
            .collect::<Vec<_>>(),
    )
    .await;

    let response_entries: Vec<LeaderboardEntry> = entries
        .into_iter()
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
                user_id: entry.user_id,
                is_guest: guest_flags.get(&entry.user_id).copied().unwrap_or(false),
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

/// Guest status for the accounts on one rendered page.
///
/// A failed lookup must not take the board down with it, so an unreadable
/// batch degrades to an unlabelled page: the marker is a disambiguation aid,
/// not part of the standings.
async fn guest_flags_for(state: &LeaderboardState, user_ids: &[i32]) -> HashMap<i32, bool> {
    if user_ids.is_empty() {
        return HashMap::new();
    }

    match state.db.get_users_are_guests(user_ids).await {
        Ok(flags) => flags,
        Err(error) => {
            warn!("Failed to read guest status for leaderboard page: {error:?}");
            HashMap::new()
        }
    }
}

/// List every season that has begun, newest first.
pub async fn list_seasons(State(_state): State<LeaderboardState>) -> Json<SeasonsResponse> {
    let now = Utc::now();
    let current_season = get_season_at(now);
    let seasons = seasons_at(now);

    Json(SeasonsResponse {
        seasons,
        current: current_season,
    })
}

/// User ranking response
///
/// Deliberately carries no ladder position. Deriving one meant reading the
/// whole ranking partition per request, and this endpoint is polled after
/// every rated match — see [`get_my_ranking`].
#[derive(Debug, Serialize)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct UserRankingResponse {
    pub mmr: Option<i32>,
    pub wins: Option<i32>,
    pub losses: Option<i32>,
    #[serde(rename = "winRate")]
    pub win_rate: Option<f64>,
}

/// Get the current user's ranking in one region.
///
/// Query parameters: queue_mode, game_type, season (optional), region (optional)
///
/// This is a hot endpoint: the post-match rating reveal polls it up to eight
/// times per player per rated match, on top of leaderboard page loads. It must
/// stay a single keyed read of one ranking partition.
///
/// **It is also deliberately regional, including when the client is showing
/// the Global board.** Rankings are stored per region — a player owns one row
/// per region they have played a ranked game in — and a player's own standing
/// is reported for a single one of them:
///
/// 1. the region the client passes, which the web client sets to the region
///    its websocket is connected to while Global is selected, so the badge
///    reflects where the player is actually playing; failing that,
/// 2. the region of whichever server answers this request.
///
/// [`get_leaderboard`] answers a different question and is *not* symmetric
/// with this one: with no region it returns the true global ladder across all
/// regions. A player can therefore see a Global board where their own row
/// carries a rating earned in another region while this badge shows their
/// rating in the region they are connected to. That is intended.
pub async fn get_my_ranking(
    Extension(auth_user): Extension<AuthUser>,
    State(state): State<LeaderboardState>,
    Query(query): Query<LeaderboardQuery>,
) -> Result<Json<UserRankingResponse>, StatusCode> {
    let user = state
        .db
        .get_user_by_id(auth_user.user_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if user.is_guest != auth_user.is_guest {
        return Err(StatusCode::UNAUTHORIZED);
    }

    read_user_ranking(&state, auth_user.user_id, query).await
}

/// Read any player's ranking in one region.
///
/// Standing is already public — `/api/leaderboard` publishes the same MMR
/// alongside the username, and the post-match card shows an opponent's badge
/// next to their name. This endpoint exists so a surface that knows a user id
/// but is not that user (the Play of the Game caption naming whoever earned
/// it) can render a real badge instead of guessing or omitting one.
///
/// It is deliberately anonymous and carries no identity beyond the requested
/// id: no username, no session, no counters the leaderboard does not already
/// expose.
pub async fn get_user_ranking_by_id(
    State(state): State<LeaderboardState>,
    Path(user_id): Path<i32>,
    Query(query): Query<LeaderboardQuery>,
) -> Result<Json<UserRankingResponse>, StatusCode> {
    read_user_ranking(&state, user_id, query).await
}

async fn read_user_ranking(
    state: &LeaderboardState,
    user_id: i32,
    query: LeaderboardQuery,
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

    // Matchmaking exposes logical IDs such as `use1`, while the established
    // ranking keyspace uses physical IDs such as `us-east-1`. An absent region
    // is not "global" here: it resolves to this server's own region, which is
    // the documented fallback for a client with no live websocket. Unlike
    // `get_leaderboard`, this endpoint never spans regions.
    let region = get_ranking_region(query.region.as_deref());

    // One keyed read of this region's partition, and nothing else.
    let ranking = match state
        .db
        .get_user_ranking(user_id, &queue_mode, &game_type, &region, season)
        .await
    {
        Ok(Some(entry)) => {
            let total_games = entry.wins + entry.losses;
            let win_rate = if total_games > 0 {
                Some((entry.wins as f64 / total_games as f64) * 100.0)
            } else {
                None
            };

            UserRankingResponse {
                mmr: Some(entry.mmr),
                wins: Some(entry.wins),
                losses: Some(entry.losses),
                win_rate,
            }
        }
        // The player has no row in this region. This is the only empty answer:
        // a read that *failed* is not evidence of an absent ranking.
        Ok(None) => UserRankingResponse {
            mmr: None,
            wins: None,
            losses: None,
            win_rate: None,
        },
        Err(error) => {
            // Reporting a throttled or timed-out read as an empty ranking is
            // what made an established player's badge flip to Unranked and
            // back between page loads. A failure status lets the client keep
            // showing the rank it already had.
            error!(user_id, "Failed to read user ranking: {error:?}");
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    Ok(Json(ranking))
}
