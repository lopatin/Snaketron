use std::time::Duration;
use chrono::Utc;
use sqlx::{Executor, PgPool, Postgres, Transaction};
use tracing::{error, info, trace, warn};
use tokio_util::sync::CancellationToken;
use std::sync::Arc;
use anyhow::Context;
use common::{GameState, GameType};
use crate::raft::{ClientRequest, ClientResponse, RaftNode};

// --- Configuration Constants ---
const MIN_PLAYERS: usize = 2;
const MAX_PLAYERS: usize = 10;
// Initial game state will be null

// MMR matching ranges that expand over time
const MMR_RANGES: [(i32, i32); 4] = [
    (0, 100),    // 0-5 seconds: Very close skill
    (0, 250),    // 5-10 seconds: Close skill
    (0, 500),    // 10-20 seconds: Moderate difference
    (0, 1000),   // 20+ seconds: Any skill level
];

// Wait time thresholds (in seconds)
const WAIT_THRESHOLDS: [i64; 4] = [5, 10, 20, 30];

// Minimum players based on wait time
const MIN_PLAYERS_BY_WAIT: [usize; 4] = [
    10,  // 0-5s: Try for full matches
    6,   // 5-10s: Accept partial matches
    4,   // 10-20s: Accept smaller matches
    2,   // 20s+: Accept any match
];

#[derive(sqlx::FromRow, Debug)]
struct MatchmakingPlayer {
    game_request_id: i32,
    user_id: i32,
    mmr: i32,
    wait_seconds: i64,
}

#[derive(sqlx::FromRow, Debug)]
struct ServerLoad {
    id: i32,
    current_game_count: i32,
    max_game_capacity: i32,
}

/// Main matchmaking loop that runs on each server
pub async fn run_matchmaking_loop(
    pool: PgPool,
    raft: Arc<RaftNode>,
    server_id: u64,
    cancellation_token: CancellationToken,
) {
    info!(?server_id, "Starting adaptive matchmaking loop");

    let mut interval = tokio::time::interval(Duration::from_secs(2));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                info!("Matchmaking loop received shutdown signal");
                break;
            }
            _ = interval.tick() => {
                // Continue with matchmaking logic
            }
        }
        
        // Update server heartbeat
        if let Err(e) = update_heartbeat(&pool, server_id).await {
            error!(?server_id, error = %e, "Failed to update heartbeat");
        }

        // Get distinct game types that have waiting players
        let game_types: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT DISTINCT game_type
            FROM game_requests
            WHERE game_id IS NULL
            ORDER BY game_type
            "#
        )
        .fetch_all(&pool)
        .await
        .unwrap_or_else(|e| {
            error!(error = %e, "Failed to fetch waiting game types");
            vec![]
        });
        
        for game_type in &game_types {
            match create_adaptive_match(&pool, raft.clone(), game_type.clone(), server_id).await {
                Ok(Some((game_id, players))) => {
                    info!(
                        game_id,
                        game_type = ?game_type,
                        player_count = players.len(),
                        "Created match successfully - game will be started by assigned server"
                    );
                }
                Ok(None) => {
                    trace!(game_type = ?game_type, "No suitable match found");
                }
                Err(e) if is_serialization_error(&e) => {
                    trace!(game_type = ?game_type, "Serialization conflict, will retry next tick");
                }
                Err(e) => {
                    error!(game_type = ?game_type, error = %e, "Matchmaking error");
                }
            }
        }
    }
}

/// Update server heartbeat
async fn update_heartbeat(pool: &PgPool, server_id: u64) -> Result<(), sqlx::Error> {
    let now = Utc::now();
    sqlx::query(
        r#"
        UPDATE servers
        SET last_heartbeat = $1
        WHERE id = $2
        "#
    )
    .bind(now)
    .bind(server_id as i32)
    .execute(pool)
    .await?;
    Ok(())
}

/// Create an adaptive match using expanding skill ranges
async fn create_adaptive_match(
    pool: &PgPool,
    raft: Arc<RaftNode>,
    game_type: serde_json::Value,
    server_id: u64,
) -> anyhow::Result<Option<(i32, Vec<i32>)>> {
    let mut tx = pool.begin().await?;
    tx.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").await?;


    // Get the oldest waiting player to determine match urgency
    let oldest_wait: Option<i64> = sqlx::query_scalar(
        r#"
        SELECT CASE 
            WHEN COUNT(*) = 0 THEN NULL
            ELSE EXTRACT(EPOCH FROM (NOW() - MIN(request_time)))::BIGINT
        END
        FROM game_requests
        WHERE game_type = $1 AND game_id IS NULL
        "#
    )
    .bind(&game_type)
    .fetch_optional(&mut *tx)
    .await?;

    let wait_seconds = oldest_wait.unwrap_or(0);

    // Determine which tier of matching to use based on wait time
    let tier = WAIT_THRESHOLDS
        .iter()
        .position(|&threshold| wait_seconds < threshold)
        .unwrap_or(WAIT_THRESHOLDS.len() - 1);

    let (_min_mmr_diff, max_mmr_diff) = MMR_RANGES.get(tier).copied().unwrap_or((0, 1000));
    let base_min_players = MIN_PLAYERS_BY_WAIT.get(tier).copied().unwrap_or(MIN_PLAYERS);
    
    // For game types with max_players, never require more than max_players
    let (min_players, max_players_for_game) = match &game_type {
        serde_json::Value::Object(obj) => {
            if let Some(serde_json::Value::Object(ffa_obj)) = obj.get("FreeForAll") {
                if let Some(serde_json::Value::Number(max)) = ffa_obj.get("max_players") {
                    if let Some(max_val) = max.as_u64() {
                        (base_min_players.min(max_val as usize), max_val as usize)
                    } else {
                        (base_min_players, MAX_PLAYERS)
                    }
                } else {
                    (base_min_players, MAX_PLAYERS)
                }
            } else {
                (base_min_players, MAX_PLAYERS)
            }
        }
        _ => (base_min_players, MAX_PLAYERS)
    };

    // Find matching players - use INNER JOIN to ensure we only get valid users
    let players_result = sqlx::query_as::<_, MatchmakingPlayer>(
        r#"
        SELECT 
            gr.id as game_request_id,
            gr.user_id,
            u.mmr,
            EXTRACT(EPOCH FROM (NOW() - gr.request_time))::BIGINT as wait_seconds
        FROM game_requests gr
        INNER JOIN users u ON gr.user_id = u.id
        WHERE gr.game_type = $1 
            AND gr.game_id IS NULL
        ORDER BY gr.request_time ASC
        LIMIT $2
        FOR UPDATE OF gr SKIP LOCKED
        "#
    )
    .bind(&game_type)
    .bind(MAX_PLAYERS as i32)
    .fetch_all(&mut *tx)
    .await;
    
    let players = match players_result {
        Ok(p) => p,
        Err(e) => {
            // Log the error but don't fail - just skip this game type
            trace!(?game_type, error = %e, "No matching players found");
            tx.rollback().await?;
            return Ok(None);
        }
    };
    
    // If no players found, return early
    if players.is_empty() {
        tx.rollback().await?;
        return Ok(None);
    }
    
    // Filter by MMR difference if we have multiple players
    let filtered_players = if players.len() > 1 {
        let avg_mmr: i32 = players.iter()
            .map(|p| p.mmr)
            .sum::<i32>() / players.len() as i32;
        
        players.into_iter()
            .filter(|p| (p.mmr - avg_mmr).abs() <= max_mmr_diff)
            .collect()
    } else {
        players
    };

    // Check if we have enough players
    if filtered_players.len() < min_players {
        tx.rollback().await?;
        return Ok(None);
    }

    // Take up to max_players_for_game players
    let matched_players: Vec<_> = filtered_players.into_iter()
        .take(max_players_for_game)
        .collect();
    
    let user_ids: Vec<i32> = matched_players.iter()
        .map(|p| p.user_id)
        .collect();
    let request_ids: Vec<i32> = matched_players.iter()
        .map(|p| p.game_request_id)
        .collect();

    // Use the current server (no need to select least loaded in this version)

    // Use the game_type that was passed in
    
    let game_id: i32 = sqlx::query_scalar(
        r#"
        INSERT INTO games (server_id, game_type, game_state, status, created_at, last_activity)
        VALUES ($1, $2, NULL, 'waiting', NOW(), NOW())
        RETURNING id
        "#
    )
    .bind(server_id as i32)
    .bind(&game_type)
    .fetch_one(&mut *tx)
    .await?;

    // Insert players into game_players table
    for (idx, user_id) in user_ids.iter().enumerate() {
        sqlx::query(
            r#"
            INSERT INTO game_players (game_id, user_id, team_id, joined_at)
            VALUES ($1, $2, $3, NOW())
            "#
        )
        .bind(game_id)
        .bind(user_id)
        .bind(idx as i32 % 2)  // Alternate teams for now
        .execute(&mut *tx)
        .await?;
    }

    // Update game requests to mark them as matched
    let updated = sqlx::query(
        r#"
        UPDATE game_requests
        SET game_id = $1
        WHERE id = ANY($2)
        "#
    )
    .bind(game_id)
    .bind(&request_ids)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if updated != request_ids.len() as u64 {
        tx.rollback().await?;
        return Err(sqlx::Error::RowNotFound.into());
    }

    // Increment server game count
    sqlx::query(
        r#"
        UPDATE servers
        SET current_game_count = current_game_count + 1
        WHERE id = $1
        "#
    )
    .bind(server_id as i32)
    .execute(&mut *tx)
    .await?;

    // Commit to db
    tx.commit().await?;
    
    // Create game state with players
    let game_type_enum: GameType = serde_json::from_value(game_type.clone())
        .map_err(|e| anyhow::anyhow!("Failed to deserialize game type: {}", e))?;
    let mut game_state = GameState::new(40, 40, game_type_enum, None);
    
    // Add players to the game state
    for user_id in user_ids.iter() {
        game_state.add_player(*user_id as u32)?;
    }
    
    // Save the game to Raft
    match raft.propose(ClientRequest::CreateGame {
        game_id: game_id as u32,
        game_state,
    }).await.context("Failed to save game to raft")? {
        ClientResponse::Success => info!(game_id, "Game created and saved to Raft"),
        ClientResponse::Error(msg) => {
            warn!(game_id, error = %msg, "Game creation failed in Raft");
            return Err(anyhow::anyhow!("Raft error: {}", msg));
        }
        _ => {
            warn!(game_id, "Unexpected response from Raft when creating game");
            return Err(anyhow::anyhow!("Unexpected Raft response"));
        }
    }

    // Log match details
    let avg_mmr = matched_players.iter()
        .map(|p| p.mmr)
        .sum::<i32>() / matched_players.len() as i32;
    let max_wait = matched_players.iter()
        .map(|p| p.wait_seconds)
        .max()
        .unwrap_or(0);
    
    info!(
        game_id,
        ?server_id,
        player_count = matched_players.len(),
        avg_mmr,
        max_wait_seconds = max_wait,
        mmr_range = max_mmr_diff,
        "Match created"
    );

    Ok(Some((game_id, user_ids)))
}

/// Select the least loaded healthy server
async fn select_least_loaded_server(
    tx: &mut Transaction<'_, Postgres>,
) -> Result<i32, sqlx::Error> {
    let server: ServerLoad = sqlx::query_as(
        r#"
        SELECT id, current_game_count, max_game_capacity
        FROM servers
        WHERE last_heartbeat > NOW() - INTERVAL '30 seconds'
          AND current_game_count < max_game_capacity
        ORDER BY 
            CAST(current_game_count AS FLOAT) / NULLIF(max_game_capacity, 0) ASC,
            RANDOM()
        LIMIT 1
        FOR UPDATE SKIP LOCKED
        "#
    )
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| sqlx::Error::RowNotFound)?;

    Ok(server.id)
}

/// Check if an error is a serialization failure
fn is_serialization_error(error: &anyhow::Error) -> bool {
    if let Some(sqlx_err) = error.downcast_ref::<sqlx::Error>() {
        match sqlx_err {
            sqlx::Error::Database(db_err) => {
                // PostgreSQL serialization failure error code is 40001
                db_err.code().map(|c| c == "40001").unwrap_or(false)
            }
            _ => false,
        }
    } else {
        false
    }
}
