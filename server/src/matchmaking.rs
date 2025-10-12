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

/// Represents a valid combination of lobbies that can form a game
#[derive(Debug, Clone)]
struct MatchmakingCombination {
    lobbies: Vec<crate::matchmaking_manager::QueuedLobby>,
    /// Team assignments: (lobby_id, team_id) pairs for lobby-level assignment
    /// For player-level assignment (e.g., splitting a 2-player lobby in 1v1),
    /// this will assign teams in order of members
    team_assignments: Option<Vec<(i32, common::TeamId)>>,
    total_players: usize,
    avg_mmr: i32,
}

impl MatchmakingCombination {
    /// Check if this combination is valid for the given game type
    fn is_valid(&self, game_type: &GameType) -> bool {
        match game_type {
            GameType::Solo => self.total_players == 1,
            GameType::TeamMatch { per_team: 1 } => {
                // 1v1: need exactly 2 players
                self.total_players == 2 && self.team_assignments.is_some()
            }
            GameType::TeamMatch { per_team: 2 } => {
                // 2v2: need exactly 4 players
                self.total_players == 4 && self.team_assignments.is_some()
            }
            GameType::FreeForAll { max_players } => {
                self.total_players >= 2 && self.total_players <= *max_players as usize
            }
            _ => false,
        }
    }
}

/// Find the best combination of lobbies that can form a valid game
fn find_best_lobby_combination(
    lobbies: &[crate::matchmaking_manager::QueuedLobby],
    game_type: &GameType,
) -> Option<MatchmakingCombination> {
    if lobbies.is_empty() {
        return None;
    }

    match game_type {
        GameType::Solo => find_solo_combination(lobbies),
        GameType::TeamMatch { per_team: 1 } => find_1v1_combination(lobbies),
        GameType::TeamMatch { per_team: 2 } => find_2v2_combination(lobbies),
        GameType::FreeForAll { max_players } => find_ffa_combination(lobbies, *max_players as usize),
        _ => None,
    }
}

/// Find a solo game combination (1 player)
fn find_solo_combination(lobbies: &[crate::matchmaking_manager::QueuedLobby]) -> Option<MatchmakingCombination> {
    // Solo: any single-player lobby
    lobbies.iter()
        .find(|l| l.members.len() == 1)
        .map(|l| MatchmakingCombination {
            lobbies: vec![l.clone()],
            team_assignments: None,  // Solo has no teams
            total_players: 1,
            avg_mmr: l.avg_mmr,
        })
}

/// Find a 1v1 combination (2 players, 1 per team)
fn find_1v1_combination(lobbies: &[crate::matchmaking_manager::QueuedLobby]) -> Option<MatchmakingCombination> {
    // Case 1: One lobby with exactly 2 players (split into teams)
    if let Some(lobby) = lobbies.iter().find(|l| l.members.len() == 2) {
        return Some(MatchmakingCombination {
            lobbies: vec![lobby.clone()],
            // Special marker: negative lobby_id means "split this lobby's players"
            team_assignments: Some(vec![
                (lobby.lobby_id, common::TeamId(0)),
                (-lobby.lobby_id, common::TeamId(1)), // Negative indicates second player
            ]),
            total_players: 2,
            avg_mmr: lobby.avg_mmr,
        });
    }

    // Case 2: Two 1-player lobbies (each on separate team)
    let single_lobbies: Vec<_> = lobbies.iter()
        .filter(|l| l.members.len() == 1)
        .take(2)
        .collect();

    if single_lobbies.len() == 2 {
        let avg_mmr = (single_lobbies[0].avg_mmr + single_lobbies[1].avg_mmr) / 2;
        return Some(MatchmakingCombination {
            lobbies: vec![single_lobbies[0].clone(), single_lobbies[1].clone()],
            team_assignments: Some(vec![
                (single_lobbies[0].lobby_id, common::TeamId(0)),
                (single_lobbies[1].lobby_id, common::TeamId(1)),
            ]),
            total_players: 2,
            avg_mmr,
        });
    }

    None
}

/// Find a 2v2 combination (4 players, 2 per team)
fn find_2v2_combination(lobbies: &[crate::matchmaking_manager::QueuedLobby]) -> Option<MatchmakingCombination> {
    // Case 1: Two lobbies with 2 players each (each lobby = 1 team) - PREFERRED
    let two_player_lobbies: Vec<_> = lobbies.iter()
        .filter(|l| l.members.len() == 2)
        .take(2)
        .collect();

    if two_player_lobbies.len() == 2 {
        let avg_mmr = (two_player_lobbies[0].avg_mmr + two_player_lobbies[1].avg_mmr) / 2;
        return Some(MatchmakingCombination {
            lobbies: vec![two_player_lobbies[0].clone(), two_player_lobbies[1].clone()],
            team_assignments: Some(vec![
                (two_player_lobbies[0].lobby_id, common::TeamId(0)),
                (two_player_lobbies[1].lobby_id, common::TeamId(1)),
            ]),
            total_players: 4,
            avg_mmr,
        });
    }

    // Case 2: One lobby with exactly 4 players (split into 2 teams of 2)
    if let Some(lobby) = lobbies.iter().find(|l| l.members.len() == 4) {
        return Some(MatchmakingCombination {
            lobbies: vec![lobby.clone()],
            // Use negative IDs to indicate player indices within the lobby
            // Format: first 2 players on Team 0, last 2 on Team 1
            team_assignments: Some(vec![
                (lobby.lobby_id, common::TeamId(0)),      // First 2 players
                (-lobby.lobby_id, common::TeamId(1)),     // Last 2 players
            ]),
            total_players: 4,
            avg_mmr: lobby.avg_mmr,
        });
    }

    // Case 3: One 3-player lobby + one 1-player lobby (3+1 on different teams)
    let three_player = lobbies.iter().find(|l| l.members.len() == 3);
    let one_player = lobbies.iter().find(|l| l.members.len() == 1);

    if let (Some(l3), Some(l1)) = (three_player, one_player) {
        let avg_mmr = ((l3.avg_mmr * 3) + l1.avg_mmr) / 4;
        return Some(MatchmakingCombination {
            lobbies: vec![l3.clone(), l1.clone()],
            team_assignments: Some(vec![
                (l3.lobby_id, common::TeamId(0)),   // 3-player lobby on Team 0
                (l1.lobby_id, common::TeamId(1)),   // 1-player lobby on Team 1
            ]),
            total_players: 4,
            avg_mmr,
        });
    }

    None
}

/// Find an FFA combination (2+ players up to max_players)
fn find_ffa_combination(lobbies: &[crate::matchmaking_manager::QueuedLobby], max_players: usize) -> Option<MatchmakingCombination> {
    // Greedy approach: take lobbies until we reach max_players
    let mut selected = Vec::new();
    let mut total = 0;
    let mut total_mmr_weighted = 0;
    let mut has_incompatible_lobbies = false;

    for lobby in lobbies {
        if total + lobby.members.len() <= max_players {
            total += lobby.members.len();
            total_mmr_weighted += lobby.avg_mmr * lobby.members.len() as i32;
            selected.push(lobby.clone());
        } else {
            // Found a lobby that doesn't fit - remember this
            has_incompatible_lobbies = true;
        }
    }

    // Determine if we should create a match:
    // 1. Multiple lobbies combined: always match (better FFA game)
    // 2. Single lobby with incompatible lobbies remaining: match it (can't wait for them)
    // 3. Single lobby alone: don't match (wait for more lobbies to potentially combine)
    let should_match = if selected.len() > 1 {
        // Multiple lobbies: combine them
        true
    } else if selected.len() == 1 && has_incompatible_lobbies {
        // Single lobby but there are other lobbies that can't be combined with it
        // Match this lobby since waiting won't help
        true
    } else {
        // Single lobby with no other lobbies in queue: wait for more to arrive
        false
    };

    if total >= 2 && should_match {
        let avg_mmr = total_mmr_weighted / total as i32;
        Some(MatchmakingCombination {
            lobbies: selected,
            team_assignments: None,  // FFA has no teams
            total_players: total,
            avg_mmr,
        })
    } else {
        None
    }
}

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

/// Create matches from lobbies in the queue using advanced combination matching
async fn create_lobby_matches(
    matchmaking_manager: &mut MatchmakingManager,
    pubsub: &mut PubSubManager,
    game_type: GameType,
    queue_mode: common::QueueMode,
) -> Result<usize> {
    // Get all queued lobbies for this game type and queue mode
    let mut available_lobbies = matchmaking_manager.get_queued_lobbies(&game_type, &queue_mode).await?;

    if available_lobbies.is_empty() {
        return Ok(0);
    }

    let mut games_created = 0;

    // Try to create as many games as possible from available lobbies
    while !available_lobbies.is_empty() {
        // Find the best combination of lobbies for this game type
        let combination = match find_best_lobby_combination(&available_lobbies, &game_type) {
            Some(comb) => comb,
            None => {
                // No valid combinations found, stop trying
                trace!("No valid lobby combinations found for {:?}", game_type);
                break;
            }
        };

        // Validate the combination
        if !combination.is_valid(&game_type) {
            warn!("Invalid combination found for {:?}, skipping", game_type);
            break;
        }

        // Create game from this combination
        match create_game_from_lobbies(
            matchmaking_manager,
            pubsub,
            &game_type,
            &queue_mode,
            &combination,
        ).await {
            Ok(game_id) => {
                games_created += 1;
                info!(
                    "Created game {} from {} lobbies with {} total players (avg MMR: {})",
                    game_id,
                    combination.lobbies.len(),
                    combination.total_players,
                    combination.avg_mmr
                );

                // Remove matched lobbies from available pool and queue
                for lobby in &combination.lobbies {
                    available_lobbies.retain(|l| l.lobby_id != lobby.lobby_id);

                    if let Err(e) = matchmaking_manager.remove_lobby_from_queue(
                        &game_type,
                        &queue_mode,
                        lobby.lobby_id,
                    ).await {
                        error!("Failed to remove lobby {} from queue: {}", lobby.lobby_id, e);
                    }
                }

                // Publish match notifications to all lobby members
                if let Err(e) = publish_lobby_match_notifications(
                    &combination.lobbies,
                    game_id,
                ).await {
                    error!("Failed to publish match notifications: {}", e);
                }
            }
            Err(e) => {
                error!("Failed to create game from lobby combination: {}", e);
                break; // Stop trying to create more games on error
            }
        }
    }

    Ok(games_created)
}

/// Create a game from a combination of lobbies with proper team assignments
async fn create_game_from_lobbies(
    matchmaking_manager: &mut MatchmakingManager,
    pubsub: &mut PubSubManager,
    game_type: &GameType,
    queue_mode: &common::QueueMode,
    combination: &MatchmakingCombination,
) -> Result<u32> {
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

    // Add players to game state with team assignments
    let mut all_players = Vec::new();

    if let Some(team_assignments) = &combination.team_assignments {
        // Team-based game (1v1, 2v2)
        let mut lobby_team_map = std::collections::HashMap::new();
        for (lobby_id, team_id) in team_assignments {
            lobby_team_map.insert(*lobby_id, *team_id);
        }

        for lobby in &combination.lobbies {
            // Get the team for this lobby (or its members)
            let team_id = lobby_team_map.get(&lobby.lobby_id).copied();

            // Check if this lobby needs to be split (negative lobby_id marker)
            let needs_split = lobby_team_map.contains_key(&(-lobby.lobby_id));

            if needs_split && lobby.members.len() > 1 {
                // Split lobby members across teams (for 1v1 with 2-player lobby, or 2v2 with 4-player lobby)
                let split_point = lobby.members.len() / 2;
                for (idx, member) in lobby.members.iter().enumerate() {
                    let player_team = if idx < split_point {
                        lobby_team_map.get(&lobby.lobby_id).copied()
                    } else {
                        lobby_team_map.get(&(-lobby.lobby_id)).copied()
                    };

                    // Temporarily add player without team, we'll fix this in GameState
                    game_state.add_player(member.user_id as u32, Some(member.username.clone()))?;

                    // Update the snake's team_id directly
                    if let Some(player) = game_state.players.get(&(member.user_id as u32)) {
                        if let Some(snake) = game_state.arena.snakes.get_mut(player.snake_id as usize) {
                            snake.team_id = player_team;
                        }
                    }

                    all_players.push(QueuedPlayer {
                        user_id: member.user_id as u32,
                        mmr: combination.avg_mmr,
                        username: member.username.clone(),
                    });
                }
            } else {
                // All members of this lobby go on the same team
                for member in &lobby.members {
                    game_state.add_player(member.user_id as u32, Some(member.username.clone()))?;

                    // Update the snake's team_id
                    if let Some(player) = game_state.players.get(&(member.user_id as u32)) {
                        if let Some(snake) = game_state.arena.snakes.get_mut(player.snake_id as usize) {
                            snake.team_id = team_id;
                        }
                    }

                    all_players.push(QueuedPlayer {
                        user_id: member.user_id as u32,
                        mmr: combination.avg_mmr,
                        username: member.username.clone(),
                    });
                }
            }
        }
    } else {
        // Non-team game (Solo, FFA)
        for lobby in &combination.lobbies {
            for member in &lobby.members {
                game_state.add_player(member.user_id as u32, Some(member.username.clone()))?;

                all_players.push(QueuedPlayer {
                    user_id: member.user_id as u32,
                    mmr: combination.avg_mmr,
                    username: member.username.clone(),
                });
            }
        }
    }

    game_state.spawn_initial_food();

    // Store active match information
    let match_info = ActiveMatch {
        players: all_players,
        game_type: game_type.clone(),
        status: MatchStatus::Waiting,
        partition_id,
        created_at: Utc::now().timestamp_millis(),
    };
    matchmaking_manager.store_active_match(game_id, match_info).await?;

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

    Ok(game_id)
}

/// Publish match found notifications to all members of matched lobbies
async fn publish_lobby_match_notifications(
    lobbies: &[crate::matchmaking_manager::QueuedLobby],
    game_id: u32,
) -> Result<()> {
    let redis_keys = crate::redis_keys::RedisKeys::new();
    let redis_url = std::env::var("SNAKETRON_REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    let client = redis::Client::open(redis_url.as_str())
        .context("Failed to open Redis client")?;
    let mut conn = client.get_multiplexed_tokio_connection().await
        .context("Failed to get Redis connection")?;

    let partition_id = game_id % PARTITION_COUNT;

    for lobby in lobbies {
        let channel = redis_keys.matchmaking_lobby_notification_channel(lobby.lobby_id);
        let notification = serde_json::json!({
            "type": "MatchFound",
            "game_id": game_id,
            "partition_id": partition_id
        });

        let _: Result<i32, _> = redis::cmd("PUBLISH")
            .arg(&channel)
            .arg(notification.to_string())
            .query_async(&mut conn).await;

        info!("Published match notification to lobby {} (code: {})", lobby.lobby_id, lobby.lobby_code);
    }

    Ok(())
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