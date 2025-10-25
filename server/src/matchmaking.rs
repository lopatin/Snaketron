use anyhow::{Context, Result};
use chrono::Utc;
use common::{GameState, GameType};
use std::time::Duration;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, trace, warn};

use crate::game_executor::{PARTITION_COUNT, StreamEvent};
use crate::matchmaking_manager::{ActiveMatch, MatchStatus, MatchmakingManager, QueuedPlayer};
use crate::pubsub_manager::PubSubManager;

// --- Configuration Constants ---
const GAME_START_DELAY_MS: i64 = 3000; // 3 second countdown before game starts

/// Explicit player-level team assignment
#[derive(Debug, Clone)]
struct TeamAssignment {
    lobby_id: i32,
    member_indices: Vec<usize>, // Which members of this lobby
    team_id: common::TeamId,
}

/// Represents a valid combination of lobbies that can form a game
#[derive(Debug, Clone)]
struct MatchmakingCombination {
    lobbies: Vec<crate::matchmaking_manager::QueuedLobby>,
    /// Player-level team assignments (explicit about which lobby members go on which team)
    team_assignments: Vec<TeamAssignment>,
    /// Spectators: (lobby_id, member_indices) for players who will spectate
    spectators: Vec<(i32, Vec<usize>)>,
    total_players: usize,
    avg_mmr: i32,
}

impl MatchmakingCombination {
    /// Check if this combination is valid for the given game type
    fn is_valid(&self, game_type: &GameType) -> bool {
        match game_type {
            GameType::Solo => self.total_players == 1 && self.team_assignments.is_empty(),
            GameType::TeamMatch { per_team } => {
                let total_needed = (per_team * 2) as usize;
                // Check we have the right number of players
                if self.total_players != total_needed {
                    return false;
                }
                // Check we have team assignments
                if self.team_assignments.is_empty() {
                    return false;
                }
                // Verify team assignments cover all players
                let mut assigned_count = 0;
                for assignment in &self.team_assignments {
                    assigned_count += assignment.member_indices.len();
                }
                assigned_count == total_needed
            }
            GameType::FreeForAll { max_players } => {
                self.total_players >= 2
                    && self.total_players <= *max_players as usize
                    && self.team_assignments.is_empty()
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
        GameType::TeamMatch { per_team } => find_team_combination(lobbies, *per_team as usize),
        GameType::FreeForAll { max_players } => {
            find_ffa_combination(lobbies, *max_players as usize)
        }
        _ => None,
    }
}

/// Find a solo game combination (1 player)
fn find_solo_combination(
    lobbies: &[crate::matchmaking_manager::QueuedLobby],
) -> Option<MatchmakingCombination> {
    // Solo: any single-player lobby
    lobbies
        .iter()
        .find(|l| l.members.len() == 1)
        .map(|l| MatchmakingCombination {
            lobbies: vec![l.clone()],
            team_assignments: Vec::new(), // Solo has no teams
            spectators: Vec::new(),
            total_players: 1,
            avg_mmr: l.avg_mmr,
        })
}

/// Find a team combination for per_team players on each of 2 teams (generic for any x vs. x)
fn find_team_combination(
    lobbies: &[crate::matchmaking_manager::QueuedLobby],
    per_team: usize,
) -> Option<MatchmakingCombination> {
    let total_needed = per_team * 2;

    // Priority 1: Exact matches (no spectators)
    if let Some(combo) = find_exact_team_match(lobbies, per_team, total_needed) {
        return Some(combo);
    }

    // Priority 2: Matches with spectators (lobby has too many players)
    find_team_match_with_spectators(lobbies, per_team, total_needed)
}

/// Find exact team match using recursive backtracking
fn find_exact_team_match(
    lobbies: &[crate::matchmaking_manager::QueuedLobby],
    per_team: usize,
    total_needed: usize,
) -> Option<MatchmakingCombination> {
    let mut team_a: Vec<(usize, Vec<usize>)> = Vec::new(); // (lobby_idx, member_indices)
    let mut team_b: Vec<(usize, Vec<usize>)> = Vec::new();

    if backtrack_assign(
        lobbies,
        0, // Current lobby index
        &mut team_a,
        &mut team_b,
        per_team,
        per_team,
    ) {
        // Convert to MatchmakingCombination
        build_combination(lobbies, team_a, team_b, Vec::new())
    } else {
        None
    }
}

/// Recursive backtracking to assign lobbies/players to teams
fn backtrack_assign(
    lobbies: &[crate::matchmaking_manager::QueuedLobby],
    lobby_idx: usize,
    team_a: &mut Vec<(usize, Vec<usize>)>, // (lobby_idx, member_indices)
    team_b: &mut Vec<(usize, Vec<usize>)>,
    remaining_a: usize,
    remaining_b: usize,
) -> bool {
    // Base case: both teams filled
    if remaining_a == 0 && remaining_b == 0 {
        return true;
    }

    // No more lobbies and teams not filled
    if lobby_idx >= lobbies.len() {
        return false;
    }

    let lobby = &lobbies[lobby_idx];
    let lobby_size = lobby.members.len();

    // Option 1: Skip this lobby (try matching with other lobbies)
    if backtrack_assign(
        lobbies,
        lobby_idx + 1,
        team_a,
        team_b,
        remaining_a,
        remaining_b,
    ) {
        return true;
    }

    // Option 2: Assign entire lobby to Team A
    if lobby_size <= remaining_a {
        team_a.push((lobby_idx, (0..lobby_size).collect()));
        if backtrack_assign(
            lobbies,
            lobby_idx + 1,
            team_a,
            team_b,
            remaining_a - lobby_size,
            remaining_b,
        ) {
            return true;
        }
        team_a.pop();
    }

    // Option 3: Assign entire lobby to Team B
    if lobby_size <= remaining_b {
        team_b.push((lobby_idx, (0..lobby_size).collect()));
        if backtrack_assign(
            lobbies,
            lobby_idx + 1,
            team_a,
            team_b,
            remaining_a,
            remaining_b - lobby_size,
        ) {
            return true;
        }
        team_b.pop();
    }

    // Option 4: Split lobby between teams (only if beneficial)
    // This is expensive, so only try if we can't fill teams otherwise
    if lobby_size > 1 {
        for split_point in 1..lobby_size {
            let team_a_portion = split_point;
            let team_b_portion = lobby_size - split_point;

            if team_a_portion <= remaining_a && team_b_portion <= remaining_b {
                team_a.push((lobby_idx, (0..split_point).collect()));
                team_b.push((lobby_idx, (split_point..lobby_size).collect()));

                if backtrack_assign(
                    lobbies,
                    lobby_idx + 1,
                    team_a,
                    team_b,
                    remaining_a - team_a_portion,
                    remaining_b - team_b_portion,
                ) {
                    return true;
                }

                team_a.pop();
                team_b.pop();
            }
        }
    }

    false
}

/// Build a MatchmakingCombination from team assignments
fn build_combination(
    lobbies: &[crate::matchmaking_manager::QueuedLobby],
    team_a: Vec<(usize, Vec<usize>)>,
    team_b: Vec<(usize, Vec<usize>)>,
    spectators: Vec<(i32, Vec<usize>)>,
) -> Option<MatchmakingCombination> {
    // Collect unique lobbies that are used
    let mut used_lobby_indices = std::collections::HashSet::new();
    for (lobby_idx, _) in &team_a {
        used_lobby_indices.insert(*lobby_idx);
    }
    for (lobby_idx, _) in &team_b {
        used_lobby_indices.insert(*lobby_idx);
    }

    let mut used_lobbies = Vec::new();
    for idx in used_lobby_indices {
        used_lobbies.push(lobbies[idx].clone());
    }

    // Build team assignments
    let mut team_assignments = Vec::new();
    for (lobby_idx, member_indices) in team_a {
        team_assignments.push(TeamAssignment {
            lobby_id: lobbies[lobby_idx].lobby_id,
            member_indices,
            team_id: common::TeamId(0),
        });
    }
    for (lobby_idx, member_indices) in team_b {
        team_assignments.push(TeamAssignment {
            lobby_id: lobbies[lobby_idx].lobby_id,
            member_indices,
            team_id: common::TeamId(1),
        });
    }

    // Calculate total players and average MMR
    let mut total_players = 0;
    let mut total_mmr_weighted = 0;

    for assignment in &team_assignments {
        let lobby = lobbies.iter().find(|l| l.lobby_id == assignment.lobby_id)?;
        total_players += assignment.member_indices.len();
        total_mmr_weighted += lobby.avg_mmr * assignment.member_indices.len() as i32;
    }

    let avg_mmr = if total_players > 0 {
        total_mmr_weighted / total_players as i32
    } else {
        0
    };

    Some(MatchmakingCombination {
        lobbies: used_lobbies,
        team_assignments,
        spectators,
        total_players,
        avg_mmr,
    })
}

/// Find match allowing spectators (lower priority)
fn find_team_match_with_spectators(
    lobbies: &[crate::matchmaking_manager::QueuedLobby],
    per_team: usize,
    total_needed: usize,
) -> Option<MatchmakingCombination> {
    // Look for a single large lobby where some players can spectate
    for lobby in lobbies {
        if lobby.members.len() >= total_needed {
            // Find requesting user's index
            let requesting_user_idx = lobby
                .members
                .iter()
                .position(|m| m.user_id as u32 == lobby.requesting_user_id);

            // Build player list: requesting user first, then others
            let mut player_indices = Vec::new();

            // Add requesting user first (if found in lobby)
            if let Some(idx) = requesting_user_idx {
                player_indices.push(idx);
            }

            // Add other members until we have total_needed
            for idx in 0..lobby.members.len() {
                if player_indices.len() >= total_needed {
                    break;
                }
                if Some(idx) != requesting_user_idx {
                    player_indices.push(idx);
                }
            }

            // Remaining members become spectators
            let spectator_indices: Vec<usize> = (0..lobby.members.len())
                .filter(|idx| !player_indices.contains(idx))
                .collect();

            // Split players evenly between teams
            let team_a_members: Vec<usize> =
                player_indices.iter().take(per_team).copied().collect();
            let team_b_members: Vec<usize> =
                player_indices.iter().skip(per_team).copied().collect();

            let team_assignments = vec![
                TeamAssignment {
                    lobby_id: lobby.lobby_id,
                    member_indices: team_a_members,
                    team_id: common::TeamId(0),
                },
                TeamAssignment {
                    lobby_id: lobby.lobby_id,
                    member_indices: team_b_members,
                    team_id: common::TeamId(1),
                },
            ];

            return Some(MatchmakingCombination {
                lobbies: vec![lobby.clone()],
                team_assignments,
                spectators: vec![(lobby.lobby_id, spectator_indices)],
                total_players: total_needed,
                avg_mmr: lobby.avg_mmr,
            });
        }
    }

    None
}

/// Find an FFA combination (2+ players up to max_players)
fn find_ffa_combination(
    lobbies: &[crate::matchmaking_manager::QueuedLobby],
    max_players: usize,
) -> Option<MatchmakingCombination> {
    trace!(
        max_players = max_players,
        lobby_count = lobbies.len(),
        "find_ffa_combination called"
    );

    // Log details of all input lobbies
    for (idx, lobby) in lobbies.iter().enumerate() {
        trace!(
            idx = idx,
            lobby_id = lobby.lobby_id,
            members = lobby.members.len(),
            avg_mmr = lobby.avg_mmr,
            lobby_code = %lobby.lobby_code,
            "Input lobby details"
        );
    }

    // Greedy approach: take lobbies until we reach max_players
    let mut selected = Vec::new();
    let mut total = 0;
    let mut total_mmr_weighted = 0;
    let mut has_incompatible_lobbies = false;

    for (idx, lobby) in lobbies.iter().enumerate() {
        let would_exceed = total + lobby.members.len() > max_players;

        if !would_exceed {
            total += lobby.members.len();
            total_mmr_weighted += lobby.avg_mmr * lobby.members.len() as i32;
            selected.push(lobby.clone());

            trace!(
                idx = idx,
                lobby_id = lobby.lobby_id,
                members = lobby.members.len(),
                total_players_now = total,
                "✓ SELECTED lobby (fits within max_players)"
            );
        } else {
            // Found a lobby that doesn't fit - remember this
            has_incompatible_lobbies = true;

            trace!(
                idx = idx,
                lobby_id = lobby.lobby_id,
                members = lobby.members.len(),
                total_players = total,
                max_players = max_players,
                would_total = total + lobby.members.len(),
                "✗ SKIPPED lobby (would exceed max_players)"
            );
        }
    }

    trace!(
        selected_count = selected.len(),
        total_players = total,
        has_incompatible_lobbies = has_incompatible_lobbies,
        "After lobby selection"
    );

    // Determine if we should create a match:
    // 1. Multiple lobbies combined: always match (better FFA game)
    // 2. Single lobby with incompatible lobbies remaining: match it (can't wait for them)
    // 3. Single lobby alone: don't match (wait for more lobbies to potentially combine)
    let (should_match, reason) = if selected.len() > 1 {
        // Multiple lobbies: combine them
        (true, "Multiple lobbies - combine them")
    } else if selected.len() == 1 && has_incompatible_lobbies {
        // Single lobby but there are other lobbies that can't be combined with it
        // Match this lobby since waiting won't help
        (true, "Single lobby with incompatible lobbies - match now")
    } else {
        // Single lobby with no other lobbies in queue: wait for more to arrive
        (false, "Single lobby alone - wait for more")
    };

    trace!(
        should_match = should_match,
        reason = reason,
        total_players = total,
        min_players = 2,
        "Matching decision"
    );

    if total >= 2 && should_match {
        let avg_mmr = total_mmr_weighted / total as i32;

        info!(
            total_players = total,
            lobby_count = selected.len(),
            avg_mmr = avg_mmr,
            "✓ FFA MATCH CREATED"
        );

        Some(MatchmakingCombination {
            lobbies: selected,
            team_assignments: Vec::new(), // FFA has no teams
            spectators: Vec::new(),
            total_players: total,
            avg_mmr,
        })
    } else {
        warn!(
            total_players = total,
            should_match = should_match,
            reason = reason,
            "✗ NO FFA MATCH - not enough players or shouldn't match yet"
        );

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
            GameType::Solo,
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
            if let Err(e) = matchmaking_manager
                .cleanup_expired_entries(
                    game_type,
                    &common::QueueMode::Quickmatch,
                    MAX_QUEUE_AGE_SECONDS,
                )
                .await
            {
                error!(game_type = ?game_type, error = %e, "Failed to cleanup expired quickmatch queue entries");
            }

            // Clean up competitive queue
            if let Err(e) = matchmaking_manager
                .cleanup_expired_entries(
                    game_type,
                    &common::QueueMode::Competitive,
                    MAX_QUEUE_AGE_SECONDS,
                )
                .await
            {
                error!(game_type = ?game_type, error = %e, "Failed to cleanup expired competitive queue entries");
            }

            // Try lobby-based matchmaking for quickmatch
            match create_lobby_matches(
                &mut matchmaking_manager,
                &mut pubsub,
                game_type.clone(),
                common::QueueMode::Quickmatch,
            )
            .await
            {
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
            match create_lobby_matches(
                &mut matchmaking_manager,
                &mut pubsub,
                game_type.clone(),
                common::QueueMode::Competitive,
            )
            .await
            {
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

/// Calculate maximum acceptable MMR difference based on wait time
/// Returns the maximum MMR difference that this lobby will accept for matching
fn calculate_max_mmr_diff(wait_seconds: f64) -> f64 {
    if wait_seconds < 10.0 {
        // Linear interpolation from 100 to 300 over 0-10 seconds
        100.0 + (wait_seconds / 10.0) * 200.0
    } else if wait_seconds < 30.0 {
        // Linear interpolation from 300 to 900 over 10-30 seconds
        300.0 + ((wait_seconds - 10.0) / 20.0) * 600.0
    } else {
        // After 30 seconds, match with anyone
        9999.0
    }
}

/// Check if two lobbies are compatible for matching based on MMR and wait time
fn are_lobbies_compatible(
    lobby1: &crate::matchmaking_manager::QueuedLobby,
    lobby2: &crate::matchmaking_manager::QueuedLobby,
    now_ms: i64,
) -> bool {
    let wait1_s = ((now_ms - lobby1.queued_at) as f64) / 1000.0;
    let wait2_s = ((now_ms - lobby2.queued_at) as f64) / 1000.0;

    let max_diff1 = calculate_max_mmr_diff(wait1_s);
    let max_diff2 = calculate_max_mmr_diff(wait2_s);

    let mmr_diff = (lobby1.avg_mmr - lobby2.avg_mmr).abs() as f64;

    // Both lobbies must accept the MMR difference
    let compatible = mmr_diff <= max_diff1 && mmr_diff <= max_diff2;

    if !compatible {
        trace!(
            lobby1_id = lobby1.lobby_id,
            lobby2_id = lobby2.lobby_id,
            mmr1 = lobby1.avg_mmr,
            mmr2 = lobby2.avg_mmr,
            mmr_diff = mmr_diff,
            max_diff1 = max_diff1,
            max_diff2 = max_diff2,
            "Lobbies not compatible for matching yet"
        );
    }

    compatible
}

/// Filter a list of lobbies to only include those compatible with a reference lobby
fn filter_compatible_lobbies(
    reference_lobby: &crate::matchmaking_manager::QueuedLobby,
    lobbies: &[crate::matchmaking_manager::QueuedLobby],
    now_ms: i64,
) -> Vec<crate::matchmaking_manager::QueuedLobby> {
    lobbies
        .iter()
        .filter(|lobby| {
            lobby.lobby_id == reference_lobby.lobby_id
                || are_lobbies_compatible(reference_lobby, lobby, now_ms)
        })
        .cloned()
        .collect()
}

/// Create matches from lobbies in the queue using advanced combination matching
async fn create_lobby_matches(
    matchmaking_manager: &mut MatchmakingManager,
    pubsub: &mut PubSubManager,
    game_type: GameType,
    queue_mode: common::QueueMode,
) -> Result<usize> {
    // Get all queued lobbies for this game type and queue mode
    let mut available_lobbies = matchmaking_manager
        .get_queued_lobbies(&game_type, &queue_mode)
        .await?;

    if available_lobbies.is_empty() {
        return Ok(0);
    }

    // Calculate acceptable MMR range for each lobby based on wait time
    let now = Utc::now().timestamp_millis();

    // Log wait times and acceptable MMR ranges for debugging
    // Requirements:
    // - 0-100 MMR difference: match immediately (0s)
    // - ~300 MMR difference: wait 10s
    // - 900+ MMR difference: wait 30s (max)
    for lobby in &mut available_lobbies {
        let wait_time_ms = now - lobby.queued_at;
        let wait_seconds = (wait_time_ms as f64) / 1000.0;

        // Calculate maximum acceptable MMR difference based on wait time
        // 0s: 100 MMR
        // 10s: 300 MMR
        // 30s+: unlimited (9999)
        let max_mmr_diff = if wait_seconds < 10.0 {
            // Linear interpolation from 100 to 300 over 0-10 seconds
            100.0 + (wait_seconds / 10.0) * 200.0
        } else if wait_seconds < 30.0 {
            // Linear interpolation from 300 to 900 over 10-30 seconds
            300.0 + ((wait_seconds - 10.0) / 20.0) * 600.0
        } else {
            // After 30 seconds, match with anyone
            9999.0
        };

        // Store the max acceptable MMR difference in the lobby (we'll use this for filtering)
        // For now, we don't modify the lobby's MMR, we'll filter during matching
        // Store as a "virtual" adjustment by keeping original MMR
        trace!(
            lobby_id = lobby.lobby_id,
            wait_seconds = wait_seconds,
            original_mmr = lobby.avg_mmr,
            max_mmr_diff = max_mmr_diff,
            "Calculated acceptable MMR range for lobby"
        );
    }

    // Sort lobbies by wait time (longest waiting first) for priority matching
    available_lobbies.sort_by(|a, b| a.queued_at.cmp(&b.queued_at));

    let mut games_created = 0;

    // Try to create as many games as possible from available lobbies
    while !available_lobbies.is_empty() {
        // Get the longest-waiting lobby (first in sorted list)
        let priority_lobby = &available_lobbies[0];

        let wait_time_s = ((now - priority_lobby.queued_at) as f64) / 1000.0;
        let max_acceptable_mmr_diff = calculate_max_mmr_diff(wait_time_s);

        info!(
            priority_lobby_id = priority_lobby.lobby_id,
            priority_mmr = priority_lobby.avg_mmr,
            wait_time_s = wait_time_s,
            max_acceptable_mmr_diff = max_acceptable_mmr_diff,
            available_lobbies = available_lobbies.len(),
            game_type = ?game_type,
            "Starting match attempt for priority lobby"
        );

        // Filter lobbies to only those compatible with the priority lobby
        let compatible_lobbies = filter_compatible_lobbies(priority_lobby, &available_lobbies, now);

        info!(
            priority_lobby_id = priority_lobby.lobby_id,
            compatible_count = compatible_lobbies.len(),
            total_available = available_lobbies.len(),
            "Compatibility filtering complete"
        );

        if compatible_lobbies.is_empty() {
            // No compatible lobbies found, wait for more time to pass
            warn!(
                lobby_id = priority_lobby.lobby_id,
                mmr = priority_lobby.avg_mmr,
                wait_time_ms = now - priority_lobby.queued_at,
                "No compatible lobbies found for priority lobby - waiting for more time or lobbies"
            );
            break;
        }

        // Find the best combination of compatible lobbies for this game type
        info!(
            game_type = ?game_type,
            compatible_lobbies = compatible_lobbies.len(),
            "Calling find_best_lobby_combination"
        );

        let combination = match find_best_lobby_combination(&compatible_lobbies, &game_type) {
            Some(comb) => {
                info!(
                    lobbies_in_combo = comb.lobbies.len(),
                    total_players = comb.total_players,
                    avg_mmr = comb.avg_mmr,
                    "find_best_lobby_combination returned a combination"
                );
                comb
            }
            None => {
                // No valid combinations found from compatible lobbies
                // This means we need to wait longer or the game type requirements can't be met
                warn!(
                    game_type = ?game_type,
                    compatible_lobbies = compatible_lobbies.len(),
                    "No valid lobby combinations found from compatible lobbies"
                );
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
        )
        .await
        {
            Ok(game_id) => {
                games_created += 1;
                info!(
                    "Created game {} from {} lobbies with {} total players (avg MMR: {})",
                    game_id,
                    combination.lobbies.len(),
                    combination.total_players,
                    combination.avg_mmr
                );

                // Remove matched lobbies from available pool and ALL queues they were in
                for lobby in &combination.lobbies {
                    available_lobbies.retain(|l| l.lobby_id != lobby.lobby_id);

                    // Use remove_lobby_from_all_queues to ensure lobby is removed from
                    // all game type queues it was registered for (prevents double-matching)
                    if let Err(e) = matchmaking_manager
                        .remove_lobby_from_all_queues(lobby)
                        .await
                    {
                        error!(
                            "Failed to remove lobby {} from all queues: {}",
                            lobby.lobby_id, e
                        );
                    }
                }

                // Publish match notifications to all lobby members
                if let Err(e) =
                    publish_lobby_match_notifications(&combination.lobbies, game_id).await
                {
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

    if !combination.team_assignments.is_empty() {
        // Team-based game (1v1, 2v2, 3v3, etc.)
        // Build a map of user_id -> team_id for quick lookup
        use std::collections::HashMap;
        let mut user_team_map: HashMap<u32, common::TeamId> = HashMap::new();

        for assignment in &combination.team_assignments {
            // Find the lobby that contains these members
            let lobby = combination
                .lobbies
                .iter()
                .find(|l| l.lobby_id == assignment.lobby_id)
                .ok_or_else(|| {
                    anyhow::anyhow!("Lobby {} not found in combination", assignment.lobby_id)
                })?;

            // Map each member index to their team
            for &member_idx in &assignment.member_indices {
                if let Some(member) = lobby.members.get(member_idx) {
                    user_team_map.insert(member.user_id as u32, assignment.team_id);
                }
            }
        }

        // Add all assigned players to the game
        for assignment in &combination.team_assignments {
            let lobby = combination
                .lobbies
                .iter()
                .find(|l| l.lobby_id == assignment.lobby_id)
                .ok_or_else(|| {
                    anyhow::anyhow!("Lobby {} not found in combination", assignment.lobby_id)
                })?;

            for &member_idx in &assignment.member_indices {
                if let Some(member) = lobby.members.get(member_idx) {
                    // Add player to game state
                    game_state.add_player(member.user_id as u32, Some(member.username.clone()))?;

                    // Update the snake's team_id
                    if let Some(player) = game_state.players.get(&(member.user_id as u32)) {
                        if let Some(snake) =
                            game_state.arena.snakes.get_mut(player.snake_id as usize)
                        {
                            snake.team_id = Some(assignment.team_id);
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
    matchmaking_manager
        .store_active_match(game_id, match_info)
        .await?;

    // Publish game events
    let event = StreamEvent::GameCreated {
        game_id,
        game_state: game_state.clone(),
    };

    pubsub
        .publish_snapshot(partition_id, game_id, &game_state)
        .await
        .context("Failed to publish initial game snapshot")?;

    let serialized = serde_json::to_vec(&event).context("Failed to serialize GameCreated event")?;
    pubsub
        .publish_command(partition_id, &serialized)
        .await
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

    let client = redis::Client::open(redis_url.as_str()).context("Failed to open Redis client")?;
    let mut conn = client
        .get_multiplexed_tokio_connection()
        .await
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
            .query_async(&mut conn)
            .await;

        info!(
            "Published match notification to lobby {} (code: {})",
            lobby.lobby_id, lobby.lobby_code
        );
    }

    Ok(())
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
    matchmaking_manager
        .store_active_match(game_id, match_info)
        .await?;

    // Publish events
    let event = StreamEvent::GameCreated {
        game_id,
        game_state: game_state.clone(),
    };

    pubsub
        .publish_snapshot(partition_id, game_id, &game_state)
        .await?;

    let serialized = serde_json::to_vec(&event)?;
    pubsub.publish_command(partition_id, &serialized).await?;

    info!(
        game_id,
        partition_id,
        player_count = players.len(),
        "Custom match created"
    );

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
