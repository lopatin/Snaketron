use std::time::Duration;
use anyhow::Result;
use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Run the game cleanup service
pub async fn run_cleanup_service(
    pool: PgPool,
    cancellation_token: CancellationToken,
) -> Result<()> {
    info!("Starting game cleanup service");
    
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                info!("Game cleanup service shutting down");
                break;
            }
            _ = interval.tick() => {
                if let Err(e) = cleanup_games(&pool).await {
                    error!("Failed to run game cleanup: {}", e);
                }
            }
        }
    }
    
    Ok(())
}

/// Perform game cleanup
pub async fn cleanup_games(pool: &PgPool) -> Result<()> {
    // Start a transaction for atomic cleanup
    let mut tx = pool.begin().await?;
    
    // 1. Delete finished games older than 5 minutes
    let finished_deleted = sqlx::query(
        r#"
        DELETE FROM games
        WHERE status = 'finished' 
        AND ended_at < NOW() - INTERVAL '5 minutes'
        "#
    )
    .execute(&mut *tx)
    .await?;
    
    if finished_deleted.rows_affected() > 0 {
        info!("Deleted {} finished games", finished_deleted.rows_affected());
    }
    
    // 2. Delete waiting games with no players older than 2 minutes
    let waiting_deleted = sqlx::query(
        r#"
        DELETE FROM games g
        WHERE g.status = 'waiting'
        AND g.last_activity < NOW() - INTERVAL '2 minutes'
        AND NOT EXISTS (
            SELECT 1 FROM game_players gp WHERE gp.game_id = g.id
        )
        "#
    )
    .execute(&mut *tx)
    .await?;
    
    if waiting_deleted.rows_affected() > 0 {
        info!("Deleted {} waiting games with no players", waiting_deleted.rows_affected());
    }
    
    // 3. Mark abandoned games (active with no activity for 10 minutes)
    let abandoned = sqlx::query(
        r#"
        UPDATE games
        SET status = 'abandoned', ended_at = NOW()
        WHERE status = 'active'
        AND last_activity < NOW() - INTERVAL '10 minutes'
        "#
    )
    .execute(&mut *tx)
    .await?;
    
    if abandoned.rows_affected() > 0 {
        info!("Marked {} games as abandoned", abandoned.rows_affected());
    }
    
    // 4. Delete abandoned games older than 5 minutes
    let abandoned_deleted = sqlx::query(
        r#"
        DELETE FROM games
        WHERE status = 'abandoned'
        AND ended_at < NOW() - INTERVAL '5 minutes'
        "#
    )
    .execute(&mut *tx)
    .await?;
    
    if abandoned_deleted.rows_affected() > 0 {
        info!("Deleted {} abandoned games", abandoned_deleted.rows_affected());
    }
    
    // 5. Update server game counts
    // First, get the current counts per server
    let server_counts: Vec<(uuid::Uuid, i64)> = sqlx::query_as(
        r#"
        SELECT server_id, COUNT(*) as game_count
        FROM games
        WHERE status IN ('waiting', 'active')
        GROUP BY server_id
        "#
    )
    .fetch_all(&mut *tx)
    .await?;
    
    // Update each server's game count
    for (server_id, count) in server_counts {
        sqlx::query(
            r#"
            UPDATE servers
            SET current_game_count = $1
            WHERE id = $2
            "#
        )
        .bind(count as i32)
        .bind(server_id)
        .execute(&mut *tx)
        .await?;
    }
    
    // Also set count to 0 for servers with no games
    sqlx::query(
        r#"
        UPDATE servers
        SET current_game_count = 0
        WHERE id NOT IN (
            SELECT DISTINCT server_id 
            FROM games 
            WHERE status IN ('waiting', 'active')
        )
        "#
    )
    .execute(&mut *tx)
    .await?;
    
    // 6. Clean up old matchmaking requests (older than 30 minutes)
    let requests_deleted = sqlx::query(
        r#"
        DELETE FROM game_requests
        WHERE request_time < NOW() - INTERVAL '30 minutes'
        "#
    )
    .execute(&mut *tx)
    .await?;
    
    if requests_deleted.rows_affected() > 0 {
        info!("Deleted {} old matchmaking requests", requests_deleted.rows_affected());
    }
    
    // Commit the transaction
    tx.commit().await?;
    
    Ok(())
}