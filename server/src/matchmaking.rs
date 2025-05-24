use std::time::Duration;
use chrono::{DateTime, Utc};
use sqlx::{Executor, PgPool, Postgres, Transaction};
use tracing::{error, info, trace, warn};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::games_manager::GamesManager;
use crate::player_connections::PlayerConnectionManager;

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
    id: uuid::Uuid,
    current_game_count: i32,
    max_game_capacity: i32,
}

/// Main matchmaking loop that runs on each server
pub async fn run_matchmaking_loop(
    pool: PgPool, 
    server_id: uuid::Uuid,
    games_manager: Arc<Mutex<GamesManager>>,
    player_connections: Arc<PlayerConnectionManager>,
) {
    info!(?server_id, "Starting adaptive matchmaking loop");

    let mut interval = tokio::time::interval(Duration::from_secs(2));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        
        // Update server heartbeat
        if let Err(e) = update_heartbeat(&pool, server_id).await {
            error!(?server_id, error = %e, "Failed to update heartbeat");
        }

        // Try to create matches for different game types
        let game_types = vec![
            serde_json::json!({"FreeForAll": {"max_players": 2}}),
            serde_json::json!({"FreeForAll": {"max_players": 10}}),
            serde_json::json!({"TeamMatch": {"per_team": 2}}),
        ];
        
        for game_type in &game_types {
            match create_adaptive_match(&pool, game_type.clone(), server_id).await {
                Ok(Some((game_id, players))) => {
                    info!(
                        game_id,
                        game_type = ?game_type,
                        player_count = players.len(),
                        "Created match successfully"
                    );
                    
                    info!("About to start game {} and notify players {:?}", game_id, players);
                    
                    // Start the game on this server
                    if let Err(e) = games_manager.lock().await.start_game(game_id as u32).await {
                        error!(game_id, error = %e, "Failed to start game");
                        continue;
                    }
                    
                    info!("Game {} started successfully", game_id);
                    
                    // Notify players that match was found
                    player_connections.notify_match_found(&players, game_id as u32).await;
                    info!("Players notified of match {}", game_id);
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
async fn update_heartbeat(pool: &PgPool, server_id: uuid::Uuid) -> Result<(), sqlx::Error> {
    let now = Utc::now();
    sqlx::query(
        r#"
        UPDATE servers
        SET last_heartbeat = $1
        WHERE id = $2
        "#
    )
    .bind(now)
    .bind(server_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Create an adaptive match using expanding skill ranges
async fn create_adaptive_match(
    pool: &PgPool,
    game_type: serde_json::Value,
    server_id: uuid::Uuid,
) -> Result<Option<(i32, Vec<i32>)>, sqlx::Error> {
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

    let (min_mmr_diff, max_mmr_diff) = MMR_RANGES.get(tier).copied().unwrap_or((0, 1000));
    let base_min_players = MIN_PLAYERS_BY_WAIT.get(tier).copied().unwrap_or(MIN_PLAYERS);
    
    // For game types with max_players, never require more than max_players
    let min_players = match &game_type {
        serde_json::Value::Object(obj) => {
            if let Some(serde_json::Value::Object(ffa_obj)) = obj.get("FreeForAll") {
                if let Some(serde_json::Value::Number(max)) = ffa_obj.get("max_players") {
                    if let Some(max_val) = max.as_u64() {
                        base_min_players.min(max_val as usize)
                    } else {
                        base_min_players
                    }
                } else {
                    base_min_players
                }
            } else {
                base_min_players
            }
        }
        _ => base_min_players
    };

    // Find matching players
    let players: Vec<MatchmakingPlayer> = sqlx::query_as(
        r#"
        WITH available_players AS (
            SELECT 
                gr.id as game_request_id,
                gr.user_id,
                u.mmr,
                EXTRACT(EPOCH FROM (NOW() - gr.request_time))::BIGINT as wait_seconds
            FROM game_requests gr
            JOIN users u ON gr.user_id = u.id
            WHERE gr.game_type = $1 AND gr.game_id IS NULL
            ORDER BY gr.request_time ASC
            LIMIT $2
            FOR UPDATE OF gr SKIP LOCKED
        ),
        player_stats AS (
            SELECT AVG(mmr) as avg_mmr
            FROM available_players
        )
        SELECT 
            ap.game_request_id,
            ap.user_id,
            ap.mmr,
            ap.wait_seconds
        FROM available_players ap
        CROSS JOIN player_stats ps
        WHERE ABS(ap.mmr - ps.avg_mmr) <= $3
        ORDER BY ap.wait_seconds DESC
        LIMIT $4
        "#
    )
    .bind(&game_type)
    .bind(MAX_PLAYERS as i32 * 2) // Fetch more to filter by MMR
    .bind(max_mmr_diff)
    .bind(MAX_PLAYERS as i32)
    .fetch_all(&mut *tx)
    .await?;

    // Check if we have enough players
    if players.len() < min_players {
        tx.rollback().await?;
        return Ok(None);
    }

    // Take up to MAX_PLAYERS players
    let matched_players: Vec<_> = players.into_iter().take(MAX_PLAYERS).collect();
    let user_ids: Vec<i32> = matched_players.iter().map(|p| p.user_id).collect();
    let request_ids: Vec<i32> = matched_players.iter().map(|p| p.game_request_id).collect();

    // Use the current server (no need to select least loaded in this version)

    // Use the game_type that was passed in
    
    let game_id: i32 = sqlx::query_scalar(
        r#"
        INSERT INTO games (server_id, game_type, game_state, status, created_at, last_activity)
        VALUES ($1, $2, NULL, 'waiting', NOW(), NOW())
        RETURNING id
        "#
    )
    .bind(server_id)
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
        return Err(sqlx::Error::RowNotFound);
    }

    // Increment server game count
    sqlx::query(
        r#"
        UPDATE servers
        SET current_game_count = current_game_count + 1
        WHERE id = $1
        "#
    )
    .bind(server_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    // Log match details
    let avg_mmr = matched_players.iter().map(|p| p.mmr).sum::<i32>() / matched_players.len() as i32;
    let max_wait = matched_players.iter().map(|p| p.wait_seconds).max().unwrap_or(0);
    
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
) -> Result<uuid::Uuid, sqlx::Error> {
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
fn is_serialization_error(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Database(db_err) => {
            // PostgreSQL serialization failure error code is 40001
            db_err.code().map(|c| c == "40001").unwrap_or(false)
        }
        _ => false,
    }
}

/// Optional: Clean up stale game requests
pub async fn cleanup_stale_requests(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        r#"
        DELETE FROM game_requests
        WHERE request_time < NOW() - INTERVAL '1 hour'
          AND game_id IS NULL
        "#
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

/// Optional: Rebalance games across servers
pub async fn rebalance_server_loads(pool: &PgPool) -> Result<(), sqlx::Error> {
    // This could be implemented to move games from overloaded servers
    // to less loaded ones, but would require careful state synchronization
    Ok(())
}