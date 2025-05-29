use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use sqlx::PgPool;
use tracing::{error, info, debug};
use anyhow::Result;

use crate::game_manager::GameManager;
use crate::player_connections::PlayerConnectionManager;

/// Main game discovery loop that polls for games assigned to this server
pub async fn run_game_discovery_loop(
    pool: PgPool,
    server_id: uuid::Uuid,
    games_manager: Arc<RwLock<GameManager>>,
    player_connections: Arc<PlayerConnectionManager>,
    cancellation_token: CancellationToken,
) {
    info!(?server_id, "Starting game discovery loop");

    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                info!("Game discovery loop received shutdown signal");
                break;
            }
            _ = interval.tick() => {
                // Check for games assigned to this server
                if let Err(e) = check_and_start_assigned_games(
                    &pool,
                    server_id,
                    &games_manager,
                    &player_connections,
                ).await {
                    error!("Game discovery error: {}", e);
                }
                
                // Check for remote games with local participants
                if let Err(e) = check_remote_games_with_local_players(
                    &pool,
                    server_id,
                    &games_manager,
                    &player_connections,
                ).await {
                    error!("Remote game discovery error: {}", e);
                }
            }
        }
    }
}

/// Check for newly assigned games and start them
async fn check_and_start_assigned_games(
    pool: &PgPool,
    server_id: uuid::Uuid,
    games_manager: &Arc<RwLock<GameManager>>,
    player_connections: &Arc<PlayerConnectionManager>,
) -> Result<()> {
    // Find games assigned to this server that aren't running yet
    // We check for games in 'waiting' status as they haven't been started
    let game_ids: Vec<i32> = sqlx::query_scalar(
        r#"
        SELECT g.id 
        FROM games g
        WHERE g.server_id = $1 
        AND g.status = 'waiting'
        "#
    )
    .bind(server_id)
    .fetch_all(pool)
    .await?;

    for game_id in game_ids {
        info!(game_id, "Starting newly assigned game");
        
        // First update game status to active to prevent duplicate processing
        let updated = sqlx::query(
            r#"
            UPDATE games 
            SET status = 'active', last_activity = NOW()
            WHERE id = $1 AND status = 'waiting'
            RETURNING id
            "#
        )
        .bind(game_id)
        .fetch_optional(pool)
        .await?;
        
        if updated.is_none() {
            // Game was already processed by another poll
            continue;
        }
        
        // Start the game
        if let Err(e) = games_manager.write().await.start_game(game_id as u32).await {
            error!(game_id, error = %e, "Failed to start assigned game");
            // Revert status on failure
            sqlx::query(
                r#"
                UPDATE games 
                SET status = 'waiting'
                WHERE id = $1
                "#
            )
            .bind(game_id)
            .execute(pool)
            .await?;
            continue;
        }
        
        // Give the game loop a moment to start and be ready for snapshot requests
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Get players for this game from game_requests
        let player_ids: Vec<i32> = sqlx::query_scalar(
            r#"
            SELECT user_id 
            FROM game_requests 
            WHERE game_id = $1
            "#
        )
        .bind(game_id)
        .fetch_all(pool)
        .await?;

        info!(game_id, ?player_ids, ?server_id, "Notifying players and joining them to game on this server");
        
        // Notify players and automatically join them to the game
        player_connections.notify_match_found_and_join(
            &player_ids, 
            game_id as u32, 
            games_manager.clone()
        ).await;
    }

    Ok(())
}

/// Check for games where we have local participants but the game is hosted elsewhere
async fn check_remote_games_with_local_players(
    pool: &PgPool,
    server_id: uuid::Uuid,
    games_manager: &Arc<RwLock<GameManager>>,
    player_connections: &Arc<PlayerConnectionManager>,
) -> Result<()> {
    // Find games where we have local players but the game is on another server
    // This query looks for games where:
    // 1. The game is NOT hosted on this server
    // 2. We have players connected to this server who are assigned to the game
    // 3. The game is active
    let remote_games_with_local_players: Vec<(i32, uuid::Uuid)> = sqlx::query_as(
        r#"
        SELECT DISTINCT g.id, g.server_id
        FROM games g
        INNER JOIN game_requests gr ON gr.game_id = g.id
        WHERE g.server_id != $1
        AND g.status = 'active'
        AND EXISTS (
            -- Check if any of the game's players might be on this server
            -- In a real implementation, you'd have a sessions table
            SELECT 1 FROM game_requests gr2 
            WHERE gr2.game_id = g.id
            -- For testing, we'll assume players are distributed across servers
        )
        "#
    )
    .bind(server_id)
    .fetch_all(pool)
    .await?;
    
    for (game_id, game_server_id) in remote_games_with_local_players {
        debug!(game_id, ?game_server_id, "Found remote game with potential local players");
        
        // For each remote game, we need to ensure our local players can join
        // The GameBroker will handle the cross-server communication
        // We just need to make sure the game channels are available locally
        
        // Note: The actual player notification happens in the matchmaking service
        // This is just for games that might have been missed or need reconnection
    }
    
    Ok(())
}