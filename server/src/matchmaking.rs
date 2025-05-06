use std::time::Duration;
use chrono::{DateTime, Utc};
use sqlx::{types::chrono::NaiveDateTime, Executor, PgPool, Postgres, Row, Transaction};
use tracing::{error, info, trace, warn};

pub async fn run_matchmaking_loop(pool: PgPool, server_id: i32) {
    info!(server_id, "Starting matchmaking loop");

    let mut interval = tokio::time::interval(Duration::from_secs(4));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        let now = Utc::now();

        // match sqlx::query(
        //     r#"
        //     UPDATE servers
        //     SET last_heartbeat = $1
        //     WHERE id = $2
        //     "#
        // )
        //     .bind::<DateTime<Utc>>(now)
        //     .bind::<i32>(server_id)
        //     .execute(&pool)
        //     .await
        // {
        //     Ok(result) => {
        //         if result.rows_affected() == 1 {
        //             trace!(server_id, timestamp = %now, "Heartbeat sent successfully.");
        //         } else {
        //             warn!(server_id, "Heartbeat update affected {} rows (expected 1). Server record might be missing.", result.rows_affected());
        //         }
        //     }
        //     Err(e) => {
        //         error!(server_id, error = %e, "Failed to send heartbeat");
        //     }
        // }
    }
}



// --- Configuration Constants ---
const MATCH_SIZE: usize = 10;
const BRONZE_MIN_MMR: i32 = 0;
const BRONZE_MAX_MMR: i32 = 999;
const INITIAL_GAME_STATE: i32 = 0; // Example state: 0 = Pending Start

// Structure to hold selected player info temporarily
#[derive(sqlx::FromRow, Debug)]
struct PotentialPlayer {
    game_request_id: i32,
    user_id: i32,
    mmr: i32,
    request_time: NaiveDateTime,
}

/// Attempts to find and create a match for a given game type and target mmr.
/// Prioritizes players who have been waiting longer.
pub async fn match_players_for_rank(
    pool: &PgPool,
    game_type: i32,
    rank: i32,
) -> Result<Option<(i32, Vec<i32>)>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    tx.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;").await?;

    // Select potential players
    let players: Vec<PotentialPlayer> = sqlx::query_as(
        r#"
        SELECT
            gr.id as game_request_id,
            gr.user_id,
            u.mmr,
            gr.request_time
        FROM game_requests gr
        JOIN users u ON gr.user_id = u.id
        WHERE
            u.mmr >= $1 AND gr.game_type = $2
        ORDER BY gr.request_time ASC
        LIMIT $3
        FOR UPDATE OF gr SKIP LOCKED
        "#
    )
        .bind::<i32>(rank)
        .bind::<i32>(game_type)
        .bind::<i64>(MATCH_SIZE as i64) // LIMIT expects i64
        .fetch_all(&mut *tx) // Use &mut *tx to borrow the transaction mutably
        .await?;

    // Check if enough players were found
    if players.len() < MATCH_SIZE {
        // Not enough players found, rollback the transaction and return None
        tx.rollback().await?;
        info!(
            "Not enough players for Bronze match (found {}), rolling back.",
            players.len()
        );
        return Ok(None);
    }

    info!("Found {} suitable players for a Bronze match.", players.len());

    // Extract user IDs and game request IDs
    let matched_user_ids: Vec<i32> = players.iter().map(|p| p.user_id).collect();
    let game_request_ids: Vec<i32> = players.iter().map(|p| p.game_request_id).collect();

    // Create the new game instance
    let new_game_id: i32 = sqlx::query_scalar(
        r#"
        INSERT INTO games (server_id, game_type, game_state)
        VALUES ($1, $2, $3)
        RETURNING id
        "#
    )
        // .bind::<i32>(server_id_for_new_game)
        .bind::<i32>(game_type)
        .bind::<i32>(INITIAL_GAME_STATE)
        .fetch_one(&mut *tx)
        .await?;

    info!("Created new game with ID: {}", new_game_id);

    // Remove the matched game requests
    let deleted_rows = sqlx::query(
        r#"
        DELETE FROM game_requests
        WHERE id = ANY($1)
        "#
    )
        .bind::<Vec<i32>>(game_request_ids) // Bind the game_request_ids vector
        .execute(&mut *tx) // Use &mut *tx
        .await?
        .rows_affected();

    if deleted_rows != MATCH_SIZE as u64 {
        // This shouldn't happen if the transaction isolation works correctly,
        // but as a safeguard, rollback if the number of deleted rows doesn't match.
        tx.rollback().await?;
        error!(
            "Error: Deleted {} game requests, but expected {}. Rolling back transaction.",
            deleted_rows, MATCH_SIZE
        );

        // Consider returning a specific error type here
        return Err(sqlx::Error::RowNotFound);
    }

    info!("Removed {} game requests.", deleted_rows);

    tx.commit().await?;

    info!(
        "Successfully created game {} for users: {:?}",
        new_game_id, matched_user_ids
    );

    Ok(Some((new_game_id, matched_user_ids)))
}
