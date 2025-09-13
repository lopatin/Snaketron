use anyhow::{Result, Context};
use std::time::Duration;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, trace};
use chrono::Utc;
use common::{GameType, GameState};

use crate::game_executor::{StreamEvent, PARTITION_COUNT};
use crate::pubsub_manager::PubSubManager;
use crate::matchmaking_manager::{MatchmakingManager, QueuedPlayer, ActiveMatch, MatchStatus};

// --- Configuration Constants ---
const MIN_PLAYERS: usize = 2;
const MAX_PLAYERS: usize = 10;
const GAME_START_DELAY_MS: i64 = 3000; // 3 second countdown before game starts

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
    2,   // 0-5s: Accept matches quickly
    2,   // 5-10s: Still accept quick matches
    2,   // 10-20s: Accept any match
    2,   // 20s+: Accept any match
];

/// Main matchmaking loop
pub async fn run_matchmaking_loop(
    mut matchmaking_manager: MatchmakingManager,
    mut pubsub: PubSubManager,
    cancellation_token: CancellationToken,
) -> Result<()> {
    info!("Starting adaptive matchmaking loop");

    let mut tick_interval = interval(Duration::from_secs(2));
    tick_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                info!("Redis matchmaking loop received shutdown signal");
                break;
            }
            _ = tick_interval.tick() => {
                // Continue with matchmaking logic
            }
        }

        // Get distinct game types from Redis
        // For now, we'll check a few common game types
        // In production, we'd maintain a set of active game types
        let game_types = vec![
            GameType::FreeForAll { max_players: 4 },
            GameType::FreeForAll { max_players: 6 },
            GameType::FreeForAll { max_players: 10 },
            GameType::TeamMatch { per_team: 2 },
            GameType::TeamMatch { per_team: 3 },
        ];

        for game_type in &game_types {
            match create_match(&mut matchmaking_manager, &mut pubsub, game_type.clone()).await {
                Ok(Some((game_id, players))) => {
                    info!(
                        game_id,
                        game_type = ?game_type,
                        player_count = players.len(),
                        "Created match successfully via Redis"
                    );
                }
                Ok(None) => {
                    trace!(game_type = ?game_type, "No suitable match found");
                }
                Err(e) => {
                    error!(game_type = ?game_type, error = %e, "Redis matchmaking error");
                }
            }
        }
    }

    Ok(())
}

/// Create an adaptive match
async fn create_match(
    matchmaking_manager: &mut MatchmakingManager,
    pubsub: &mut PubSubManager,
    game_type: GameType,
) -> Result<Option<(u32, Vec<u32>)>> {
    // Get all queued players for this game type
    let queued_players = matchmaking_manager.get_queued_players(&game_type).await?;
    
    if queued_players.is_empty() {
        return Ok(None);
    }

    // Calculate wait time based on oldest player
    let now = Utc::now().timestamp_millis();
    let oldest_wait = queued_players.iter()
        .map(|p| {
            // We'd need to get this from the queue timestamp
            // For now, assume they're ordered by queue time
            0i64 // Placeholder
        })
        .max()
        .unwrap_or(0);

    let wait_seconds = oldest_wait / 1000;

    // Determine which tier of matching to use based on wait time
    let tier = WAIT_THRESHOLDS
        .iter()
        .position(|&threshold| wait_seconds < threshold)
        .unwrap_or(WAIT_THRESHOLDS.len() - 1);

    let (_min_mmr_diff, max_mmr_diff) = MMR_RANGES.get(tier).copied().unwrap_or((0, 1000));
    let base_min_players = MIN_PLAYERS_BY_WAIT.get(tier).copied().unwrap_or(MIN_PLAYERS);
    
    // For game types with max_players, never require more than max_players
    let (min_players, max_players_for_game) = match &game_type {
        GameType::FreeForAll { max_players } => {
            (base_min_players.min(*max_players as usize), *max_players as usize)
        }
        GameType::TeamMatch { per_team } => {
            let total_max = per_team * 2; // Two teams
            (base_min_players.min(total_max as usize), total_max as usize)
        }
        GameType::Solo => {
            (1, 1) // Solo games are single player
        }
        GameType::Custom { .. } => {
            (base_min_players, MAX_PLAYERS) // Custom games use default limits
        }
    };

    // Filter by MMR if we have multiple players
    let filtered_players = if queued_players.len() > 1 {
        let avg_mmr: i32 = queued_players.iter()
            .map(|p| p.mmr)
            .sum::<i32>() / queued_players.len() as i32;
        
        queued_players.into_iter()
            .filter(|p| (p.mmr - avg_mmr).abs() <= max_mmr_diff)
            .collect()
    } else {
        queued_players
    };

    // Check if we have enough players
    if filtered_players.len() < min_players {
        return Ok(None);
    }

    // Take up to max_players_for_game players
    let matched_players: Vec<_> = filtered_players.into_iter()
        .take(max_players_for_game)
        .collect();
    
    let user_ids: Vec<u32> = matched_players.iter()
        .map(|p| p.user_id)
        .collect();

    // Generate game ID
    let game_id = matchmaking_manager.generate_game_id().await?;
    let partition_id = game_id % PARTITION_COUNT;

    // Create game state
    let start_ms = Utc::now().timestamp_millis() + GAME_START_DELAY_MS;
    
    // For TeamMatch games, add extra width for end zones
    let (width, height) = match &game_type {
        GameType::TeamMatch { .. } => (60, 40),  // 40 + 10 + 10 for end zones
        _ => (40, 40),
    };
    
    // Generate a random seed for the game
    let rng_seed = Some(Utc::now().timestamp_millis() as u64 ^ (game_id as u64));
    let mut game_state = GameState::new(width, height, game_type.clone(), rng_seed, start_ms);
    
    // Add players to the game state
    for player in &matched_players {
        game_state.add_player(player.user_id, Some(player.username.clone()))?;
    }
    
    // Spawn initial food items
    game_state.spawn_initial_food();

    // Store active match information in Redis
    let match_info = ActiveMatch {
        players: matched_players.clone(),
        game_type: game_type.clone(),
        status: MatchStatus::Waiting,
        partition_id,
        created_at: now,
    };
    matchmaking_manager.store_active_match(game_id, match_info).await?;

    // Remove players from queue
    matchmaking_manager.remove_players_from_queue(&game_type, &user_ids).await?;

    // Publish GameCreated event to Redis stream
    let event = StreamEvent::GameCreated {
        game_id,
        game_state: game_state.clone(),
    };
    
    // Publish initial snapshot
    pubsub.publish_snapshot(partition_id, game_id, &game_state).await
        .context("Failed to publish initial game snapshot")?;
    
    // Send GameCreated event via partition command channel
    let serialized = serde_json::to_vec(&event)
        .context("Failed to serialize GameCreated event")?;
    pubsub.publish_command(partition_id, &serialized).await
        .context("Failed to publish GameCreated event")?;
    
    info!(game_id, partition_id, "Game created and published to Redis stream");

    // Log match details
    let avg_mmr = matched_players.iter()
        .map(|p| p.mmr)
        .sum::<i32>() / matched_players.len() as i32;
    
    info!(
        game_id,
        player_count = matched_players.len(),
        avg_mmr,
        mmr_range = max_mmr_diff,
        "Redis match created"
    );
    
    Ok(Some((game_id, user_ids)))
}

/// Create a match from a specific set of players (for custom games)
pub async fn create_custom_match(
    matchmaking_manager: &mut MatchmakingManager,
    pubsub: &mut PubSubManager,
    players: Vec<QueuedPlayer>,
    game_type: GameType,
) -> Result<u32> {
    let user_ids: Vec<u32> = players.iter().map(|p| p.user_id).collect();
    
    // Generate game ID
    let game_id = matchmaking_manager.generate_game_id().await?;
    let partition_id = game_id % PARTITION_COUNT;

    // Create game state
    let start_ms = Utc::now().timestamp_millis() + GAME_START_DELAY_MS;
    let (width, height) = match &game_type {
        GameType::TeamMatch { .. } => (60, 40),
        _ => (40, 40),
    };
    
    let rng_seed = Some(Utc::now().timestamp_millis() as u64 ^ (game_id as u64));
    let mut game_state = GameState::new(width, height, game_type.clone(), rng_seed, start_ms);
    
    // Add players
    for player in &players {
        game_state.add_player(player.user_id, Some(player.username.clone()))?;
    }
    
    game_state.spawn_initial_food();

    // Store active match
    let match_info = ActiveMatch {
        players: players.clone(),
        game_type: game_type.clone(),
        status: MatchStatus::Waiting,
        partition_id,
        created_at: Utc::now().timestamp_millis(),
    };
    matchmaking_manager.store_active_match(game_id, match_info).await?;

    // Publish events
    let event = StreamEvent::GameCreated {
        game_id,
        game_state: game_state.clone(),
    };
    
    pubsub.publish_snapshot(partition_id, game_id, &game_state).await?;
    
    let serialized = serde_json::to_vec(&event)?;
    pubsub.publish_command(partition_id, &serialized).await?;
    
    info!(game_id, partition_id, player_count = players.len(), "Custom match created");
    
    Ok(game_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_match_creation_logic() {
        // Test the match creation logic
        // This would require mocking Redis and PubSub
    }
}