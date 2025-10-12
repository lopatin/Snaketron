use anyhow::{Result, Context};
use std::time::Duration;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, trace, warn};
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
            GameType::FreeForAll { max_players: 2 },
            GameType::FreeForAll { max_players: 3 },
            GameType::FreeForAll { max_players: 4 },
            GameType::FreeForAll { max_players: 6 },
            GameType::FreeForAll { max_players: 10 },
            GameType::TeamMatch { per_team: 1 },
            GameType::TeamMatch { per_team: 2 },
            GameType::TeamMatch { per_team: 3 },
        ];

        let mut total_games_created = 0;

        for game_type in &game_types {
            // Clean up expired entries for both queue modes before attempting to create matches
            // Expire entries older than 5 minutes (300 seconds)
            const MAX_QUEUE_AGE_SECONDS: i64 = 300;

            // Clean up quickmatch queue
            if let Err(e) = matchmaking_manager.cleanup_expired_entries(game_type, &common::QueueMode::Quickmatch, MAX_QUEUE_AGE_SECONDS).await {
                error!(game_type = ?game_type, error = %e, "Failed to cleanup expired quickmatch queue entries");
            }

            // Clean up competitive queue
            if let Err(e) = matchmaking_manager.cleanup_expired_entries(game_type, &common::QueueMode::Competitive, MAX_QUEUE_AGE_SECONDS).await {
                error!(game_type = ?game_type, error = %e, "Failed to cleanup expired competitive queue entries");
            }

            // Try to create matches for both queue modes using batch algorithm
            // First try quickmatch
            match create_matches_batch(&mut matchmaking_manager, &mut pubsub, game_type.clone(), common::QueueMode::Quickmatch).await {
                Ok(games_count) if games_count > 0 => {
                    total_games_created += games_count;
                    info!(
                        game_type = ?game_type,
                        queue_mode = "quickmatch",
                        games_count,
                        "Created quickmatch games via batch matchmaking"
                    );
                }
                Ok(_) => {
                    trace!(game_type = ?game_type, queue_mode = "quickmatch", "No suitable matches found");
                }
                Err(e) => {
                    error!(game_type = ?game_type, queue_mode = "quickmatch", error = %e, "Batch matchmaking error");
                }
            }

            // Then try competitive
            match create_matches_batch(&mut matchmaking_manager, &mut pubsub, game_type.clone(), common::QueueMode::Competitive).await {
                Ok(games_count) if games_count > 0 => {
                    total_games_created += games_count;
                    info!(
                        game_type = ?game_type,
                        queue_mode = "competitive",
                        games_count,
                        "Created competitive games via batch matchmaking"
                    );
                }
                Ok(_) => {
                    trace!(game_type = ?game_type, queue_mode = "competitive", "No suitable matches found");
                }
                Err(e) => {
                    error!(game_type = ?game_type, queue_mode = "competitive", error = %e, "Batch matchmaking error");
                }
            }

            // Try lobby-based matchmaking for quickmatch
            match create_lobby_matches(&mut matchmaking_manager, &mut pubsub, game_type.clone(), common::QueueMode::Quickmatch).await {
                Ok(games_count) if games_count > 0 => {
                    total_games_created += games_count;
                    info!(
                        game_type = ?game_type,
                        queue_mode = "quickmatch",
                        games_count,
                        "Created quickmatch games via lobby matchmaking"
                    );
                }
                Ok(_) => {
                    trace!(game_type = ?game_type, queue_mode = "quickmatch", "No suitable lobby matches found");
                }
                Err(e) => {
                    error!(game_type = ?game_type, queue_mode = "quickmatch", error = %e, "Lobby matchmaking error");
                }
            }

            // Try lobby-based matchmaking for competitive
            match create_lobby_matches(&mut matchmaking_manager, &mut pubsub, game_type.clone(), common::QueueMode::Competitive).await {
                Ok(games_count) if games_count > 0 => {
                    total_games_created += games_count;
                    info!(
                        game_type = ?game_type,
                        queue_mode = "competitive",
                        games_count,
                        "Created competitive games via lobby matchmaking"
                    );
                }
                Ok(_) => {
                    trace!(game_type = ?game_type, queue_mode = "competitive", "No suitable lobby matches found");
                }
                Err(e) => {
                    error!(game_type = ?game_type, queue_mode = "competitive", error = %e, "Lobby matchmaking error");
                }
            }
        }

        // If no games were created this round, add a small delay to avoid tight looping
        if total_games_created == 0 {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    Ok(())
}

/// Helper struct to hold player data with time-weighted rank
#[derive(Debug, Clone)]
struct RankedPlayer {
    player: QueuedPlayer,
    wait_time_ms: i64,
    time_weighted_rank: f64,
}

/// Create multiple matches from a batch of players using time-weighted ranking
async fn create_matches_batch(
    matchmaking_manager: &mut MatchmakingManager,
    pubsub: &mut PubSubManager,
    game_type: GameType,
    queue_mode: common::QueueMode,
) -> Result<usize> {
    let now = Utc::now().timestamp_millis();

    // Step 1: Load 3 batches from Redis
    let longest_waiting = matchmaking_manager.get_longest_waiting_users(&game_type, &queue_mode).await?;
    let lowest_mmr_ids = matchmaking_manager.get_lowest_mmr_users(&game_type, &queue_mode).await?;
    let highest_mmr_ids = matchmaking_manager.get_highest_mmr_users(&game_type, &queue_mode).await?;

    // Step 2: Build combined user set and player map
    use std::collections::{HashMap, HashSet};

    let mut user_ids_set: HashSet<u32> = HashSet::new();
    let mut players_map: HashMap<u32, (QueuedPlayer, i64)> = HashMap::new();

    // Add longest waiting users (we already have full data)
    for (player, timestamp) in longest_waiting {
        user_ids_set.insert(player.user_id);
        players_map.insert(player.user_id, (player, timestamp));
    }

    // Collect user IDs from MMR batches that we don't already have
    let mut user_ids_to_fetch: Vec<u32> = Vec::new();
    for user_id in lowest_mmr_ids.into_iter().chain(highest_mmr_ids.into_iter()) {
        if user_ids_set.insert(user_id) {
            user_ids_to_fetch.push(user_id);
        }
    }

    // Step 3: Batch fetch full data for users from MMR batches
    if !user_ids_to_fetch.is_empty() {
        let user_statuses = matchmaking_manager.batch_get_user_status(&user_ids_to_fetch).await?;
        for (user_id, status) in user_statuses {
            let player = QueuedPlayer {
                user_id,
                mmr: status.mmr,
                username: status.username,
            };
            players_map.insert(user_id, (player, status.request_time));
        }
    }

    if players_map.is_empty() {
        return Ok(0);
    }

    // Step 4: Calculate time-weighted ranks
    let avg_mmr: f64 = players_map.values()
        .map(|(p, _)| p.mmr as f64)
        .sum::<f64>() / players_map.len() as f64;

    let mut ranked_players: Vec<RankedPlayer> = players_map.into_iter()
        .map(|(_, (player, timestamp))| {
            let wait_time_ms = now - timestamp;
            let wait_seconds = (wait_time_ms as f64) / 1000.0;

            // Time weighting: 0s = 0%, 30s = 50%, 60s+ = 100%
            let weight = (wait_seconds / 60.0).min(1.0);
            let time_weighted_rank = (player.mmr as f64) + (avg_mmr - (player.mmr as f64)) * weight;

            RankedPlayer {
                player,
                wait_time_ms,
                time_weighted_rank,
            }
        })
        .collect();

    // Step 5: Sort by time-weighted rank
    ranked_players.sort_by(|a, b| {
        a.time_weighted_rank.partial_cmp(&b.time_weighted_rank)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    // Step 6: Create as many games as possible
    let mut games_created = 0;

    // Determine player requirements for this game type
    let (min_players, max_players_for_game) = match &game_type {
        GameType::FreeForAll { max_players } => {
            (MIN_PLAYERS.min(*max_players as usize), *max_players as usize)
        }
        GameType::TeamMatch { per_team } => {
            let total_max = per_team * 2;
            (MIN_PLAYERS.min(total_max as usize), total_max as usize)
        }
        GameType::Solo => {
            (1, 1)
        }
        GameType::Custom { .. } => {
            (MIN_PLAYERS, MAX_PLAYERS)
        }
    };

    // Create games from the sorted list
    let mut available_players = ranked_players;

    while available_players.len() >= min_players {
        // Take next batch of players for a game
        let players_for_game: Vec<QueuedPlayer> = available_players.iter()
            .take(max_players_for_game)
            .map(|rp| rp.player.clone())
            .collect();

        if players_for_game.len() < min_players {
            break;
        }

        // Create the game
        match create_single_game(
            matchmaking_manager,
            pubsub,
            &game_type,
            &queue_mode,
            players_for_game,
            now,
        ).await {
            Ok(game_id) => {
                games_created += 1;
                info!(game_id, "Created game {} from batch", games_created);

                // Remove matched players from available pool
                available_players.drain(0..max_players_for_game.min(available_players.len()));
            }
            Err(e) => {
                error!(error = %e, "Failed to create game from batch");
                break;
            }
        }
    }

    Ok(games_created)
}

/// Create a single game from a specific set of players (helper function)
async fn create_single_game(
    matchmaking_manager: &mut MatchmakingManager,
    pubsub: &mut PubSubManager,
    game_type: &GameType,
    queue_mode: &common::QueueMode,
    matched_players: Vec<QueuedPlayer>,
    created_at: i64,
) -> Result<u32> {
    let user_ids: Vec<u32> = matched_players.iter()
        .map(|p| p.user_id)
        .collect();

    // Generate game ID
    let game_id = matchmaking_manager.generate_game_id().await?;
    let partition_id = game_id % PARTITION_COUNT;

    // Create game state
    let start_ms = Utc::now().timestamp_millis() + GAME_START_DELAY_MS;

    let (width, height) = match game_type {
        GameType::TeamMatch { .. } => (60, 40),
        _ => (40, 40),
    };

    let rng_seed = Some(Utc::now().timestamp_millis() as u64 ^ (game_id as u64));
    let mut game_state = GameState::new(width, height, game_type.clone(), rng_seed, start_ms);

    // Add players to the game state
    for player in &matched_players {
        game_state.add_player(player.user_id, Some(player.username.clone()))?;
    }

    game_state.spawn_initial_food();

    // Store active match information
    let match_info = ActiveMatch {
        players: matched_players.clone(),
        game_type: game_type.clone(),
        status: MatchStatus::Waiting,
        partition_id,
        created_at,
    };
    matchmaking_manager.store_active_match(game_id, match_info).await?;

    // Send match notifications
    let redis_keys = crate::redis_keys::RedisKeys::new();
    let redis_url = std::env::var("SNAKETRON_REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    if let Ok(client) = redis::Client::open(redis_url.as_str()) {
        if let Ok(mut conn) = client.get_multiplexed_tokio_connection().await {
            for user_id in &user_ids {
                let channel = redis_keys.matchmaking_notification_channel(*user_id);
                let notification = serde_json::json!({
                    "type": "MatchFound",
                    "game_id": game_id,
                    "partition_id": partition_id
                });

                let _: Result<i32, _> = redis::cmd("PUBLISH")
                    .arg(&channel)
                    .arg(notification.to_string())
                    .query_async(&mut conn).await;
            }
        }
    }

    // Remove players from queue
    matchmaking_manager.remove_players_from_queue(game_type, queue_mode, &user_ids).await?;

    // Publish game events
    let event = StreamEvent::GameCreated {
        game_id,
        game_state: game_state.clone(),
    };

    pubsub.publish_snapshot(partition_id, game_id, &game_state).await
        .context("Failed to publish initial game snapshot")?;

    let serialized = serde_json::to_vec(&event)
        .context("Failed to serialize GameCreated event")?;
    pubsub.publish_command(partition_id, &serialized).await
        .context("Failed to publish GameCreated event")?;

    let avg_mmr = matched_players.iter()
        .map(|p| p.mmr)
        .sum::<i32>() / matched_players.len() as i32;

    info!(
        game_id,
        player_count = matched_players.len(),
        avg_mmr,
        "Batch match created"
    );

    Ok(game_id)
}

/// Create matches from lobbies in the queue
async fn create_lobby_matches(
    matchmaking_manager: &mut MatchmakingManager,
    pubsub: &mut PubSubManager,
    game_type: GameType,
    queue_mode: common::QueueMode,
) -> Result<usize> {
    // Get all queued lobbies for this game type and queue mode
    let lobbies = matchmaking_manager.get_queued_lobbies(&game_type, &queue_mode).await?;

    if lobbies.is_empty() {
        return Ok(0);
    }

    let mut games_created = 0;

    // For now, each lobby creates its own game
    // In the future, we could match lobbies together
    for lobby in lobbies {
        // Convert lobby members to QueuedPlayers
        let players: Vec<QueuedPlayer> = lobby.members.iter().map(|m| QueuedPlayer {
            user_id: m.user_id as u32,
            mmr: 1000, // We'll use the lobby's avg_mmr instead
            username: m.username.clone(),
        }).collect();

        if players.is_empty() {
            warn!("Empty lobby {} in queue, skipping", lobby.lobby_id);
            continue;
        }

        let now = Utc::now().timestamp_millis();

        // Create the game for this lobby
        match create_single_game(
            matchmaking_manager,
            pubsub,
            &game_type,
            &queue_mode,
            players.clone(),
            now,
        ).await {
            Ok(game_id) => {
                games_created += 1;
                info!("Created game {} for lobby {} with {} members",
                    game_id, lobby.lobby_id, players.len());

                // Remove lobby from queue
                if let Err(e) = matchmaking_manager.remove_lobby_from_queue(
                    &game_type,
                    &queue_mode,
                    lobby.lobby_id,
                ).await {
                    error!("Failed to remove lobby {} from queue: {}", lobby.lobby_id, e);
                }

                // Publish match notification to lobby channel
                let redis_keys = crate::redis_keys::RedisKeys::new();
                let redis_url = std::env::var("SNAKETRON_REDIS_URL")
                    .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

                if let Ok(client) = redis::Client::open(redis_url.as_str()) {
                    if let Ok(mut conn) = client.get_multiplexed_tokio_connection().await {
                        let channel = redis_keys.matchmaking_lobby_notification_channel(lobby.lobby_id);
                        let notification = serde_json::json!({
                            "type": "MatchFound",
                            "game_id": game_id,
                            "partition_id": game_id % PARTITION_COUNT
                        });

                        let _: Result<i32, _> = redis::cmd("PUBLISH")
                            .arg(&channel)
                            .arg(notification.to_string())
                            .query_async(&mut conn).await;

                        info!("Published match notification to lobby {}", lobby.lobby_id);
                    }
                }
            }
            Err(e) => {
                error!("Failed to create game for lobby {}: {}", lobby.lobby_id, e);
            }
        }
    }

    Ok(games_created)
}

/// Create an adaptive match
async fn create_match(
    matchmaking_manager: &mut MatchmakingManager,
    pubsub: &mut PubSubManager,
    game_type: GameType,
    queue_mode: common::QueueMode,
) -> Result<Option<(u32, Vec<u32>)>> {
    // Get all queued players for this game type and queue mode
    let queued_players = matchmaking_manager.get_queued_players(&game_type, &queue_mode).await?;
    
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

    // Update each player's status with the matched game ID
    for user_id in &user_ids {
        // Update the user's queue status to include the matched game ID
        // This allows the WebSocket handler to detect the match
        let redis_keys = crate::redis_keys::RedisKeys::new();
        let channel = redis_keys.matchmaking_notification_channel(*user_id);
        let notification = serde_json::json!({
            "type": "MatchFound",
            "game_id": game_id,
            "partition_id": partition_id
        });
        
        info!("Publishing match notification to channel: {} for user {}", channel, user_id);
        
        // Publish notification to user's channel using Redis connection
        let redis_url = std::env::var("SNAKETRON_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        if let Ok(client) = redis::Client::open(redis_url.as_str()) {
            if let Ok(mut conn) = client.get_multiplexed_tokio_connection().await {
                match redis::cmd("PUBLISH")
                    .arg(&channel)
                    .arg(notification.to_string())
                    .query_async::<i32>(&mut conn).await {
                    Ok(subscribers) => {
                        info!("Published match notification to {} subscribers", subscribers);
                    }
                    Err(e) => {
                        error!("Failed to publish match notification: {}", e);
                    }
                }
            } else {
                error!("Failed to get Redis connection for notifications");
            }
        } else {
            error!("Failed to create Redis client for notifications");
        }
    }
    
    // Remove players from queue
    matchmaking_manager.remove_players_from_queue(&game_type, &queue_mode, &user_ids).await?;

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