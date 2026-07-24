use anyhow::{Context, Result};
use chrono::Utc;
use common::{
    DEFAULT_QUICKMATCH_TEAM_TIME_LIMIT_MS, DEFAULT_TEAM_TIME_LIMIT_MS, GameState, GameType,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, trace, warn};

use crate::db::Database;
use crate::game_bus::GameBus;
use crate::game_executor::PARTITION_COUNT;
use crate::game_executor::StreamEvent;
use crate::lobby_manager::LobbyManager;
use crate::matchmaking_manager::{
    ActiveMatch, GameCreatedOutboxRecord, MatchCommitOutcome, MatchStatus, MatchmakingManager,
    QueuedPlayer,
};

// --- Configuration Constants ---
const GAME_START_DELAY_MS: i64 = 3000; // 3 second countdown before game starts
const FFA_MAX_RECURSION_DEPTH: usize = 8;
const GAME_CREATED_OUTBOX_LANE_CAPACITY: usize = 1;

struct GameCreatedOutboxDelivery {
    record: GameCreatedOutboxRecord,
    expected_payload: String,
}

async fn run_game_created_outbox_worker(
    partition_id: u32,
    mut matchmaking: MatchmakingManager,
    game_bus: Arc<GameBus>,
    mut batches: mpsc::Receiver<Vec<GameCreatedOutboxDelivery>>,
    cancellation: CancellationToken,
) -> Result<()> {
    loop {
        let batch = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Ok(()),
            batch = batches.recv() => batch.context(
                "game-created outbox worker channel closed unexpectedly"
            )?,
        };
        for delivery in batch {
            if delivery.record.partition_id != partition_id {
                anyhow::bail!(
                    "game-created outbox record for partition {} reached worker {partition_id}",
                    delivery.record.partition_id
                );
            }
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(()),
                _ = deliver_game_created_outbox_record(
                    &mut matchmaking,
                    game_bus.as_ref(),
                    delivery,
                ) => {}
            }
        }
    }
}

async fn deliver_game_created_outbox_record(
    matchmaking: &mut MatchmakingManager,
    game_bus: &GameBus,
    delivery: GameCreatedOutboxDelivery,
) {
    let game_id = delivery.record.game_id;
    if let Err(error) = game_bus.publish_game_created_once(&delivery.record).await {
        warn!(game_id, %error, "game-created outbox delivery failed; retrying");
        return;
    }
    match matchmaking
        .acknowledge_game_created_outbox(game_id, &delivery.expected_payload)
        .await
    {
        Ok(_) => {
            if let Err(error) = game_bus.expire_game_created_delivery_marker(game_id).await {
                warn!(game_id, %error, "failed to expire acknowledged game-created marker");
            }
        }
        Err(error) => {
            warn!(game_id, %error, "game-created outbox acknowledgement failed; retrying");
        }
    }
}

fn unexpected_game_created_outbox_worker_exit(
    joined: Option<std::result::Result<Result<()>, tokio::task::JoinError>>,
) -> anyhow::Error {
    match joined {
        Some(Ok(Ok(()))) => anyhow::anyhow!("game-created outbox worker exited unexpectedly"),
        Some(Ok(Err(error))) => error,
        Some(Err(error)) => anyhow::anyhow!("game-created outbox worker task failed: {error}"),
        None => anyhow::anyhow!("all game-created outbox workers exited unexpectedly"),
    }
}

/// Retry the two single-slot halves of match creation until both are durable.
/// Every task may help; duplicate workers are harmless because destination
/// publication and source acknowledgement are compare-and-set operations.
pub async fn run_game_created_outbox_loop(
    mut matchmaking: MatchmakingManager,
    game_bus: Arc<GameBus>,
    cancellation: CancellationToken,
) -> Result<()> {
    let worker_cancellation = cancellation.child_token();
    let mut workers = JoinSet::new();
    let mut lanes = Vec::with_capacity(PARTITION_COUNT as usize);
    for partition_id in 0..PARTITION_COUNT {
        let (sender, receiver) = mpsc::channel(GAME_CREATED_OUTBOX_LANE_CAPACITY);
        lanes.push(sender);
        let worker_matchmaking = matchmaking.clone();
        let worker_bus = game_bus.clone();
        let worker_token = worker_cancellation.clone();
        workers.spawn(async move {
            run_game_created_outbox_worker(
                partition_id,
                worker_matchmaking,
                worker_bus,
                receiver,
                worker_token,
            )
            .await
            .with_context(|| {
                format!("game-created outbox worker for partition {partition_id} failed")
            })
        });
    }

    let mut ticker = interval(Duration::from_millis(100));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut cursor = 0_u64;
    let loop_result = async {
        loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(()),
                joined = workers.join_next() => {
                    return Err(unexpected_game_created_outbox_worker_exit(joined));
                }
                _ = ticker.tick() => {}
            }

            let scan_result = tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(()),
                joined = workers.join_next() => {
                    return Err(unexpected_game_created_outbox_worker_exit(joined));
                }
                result = matchmaking.scan_game_created_outbox(cursor) => result,
            };
            let (next_cursor, records) = match scan_result {
                Ok(batch) => batch,
                Err(error) => {
                    warn!(%error, "game-created outbox scan failed; retrying");
                    cursor = 0;
                    continue;
                }
            };
            cursor = next_cursor;
            let mut partition_batches: Vec<Vec<GameCreatedOutboxDelivery>> =
                (0..PARTITION_COUNT).map(|_| Vec::new()).collect();
            for (field, payload) in records {
                let record: GameCreatedOutboxRecord = serde_json::from_str(&payload)
                    .context("game-created outbox contains malformed JSON")?;
                record.validate()?;
                if field != record.game_id.to_string() {
                    anyhow::bail!("game-created outbox field/payload identity mismatch");
                }
                let partition_id = record.partition_id;
                partition_batches[partition_id as usize].push(GameCreatedOutboxDelivery {
                    record,
                    expected_payload: payload,
                });
            }
            for (partition_id, batch) in partition_batches.into_iter().enumerate() {
                if batch.is_empty() {
                    continue;
                }
                match lanes[partition_id].try_send(batch) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(batch)) => {
                        trace!(
                            record_count = batch.len(),
                            partition_id,
                            "game-created outbox lane is full; leaving records in Redis for retry"
                        );
                    }
                    Err(mpsc::error::TrySendError::Closed(batch)) => {
                        anyhow::bail!(
                            "game-created outbox worker channel for partition {} closed while routing {} records",
                            partition_id,
                            batch.len()
                        );
                    }
                }
            }
        }
    }
    .await;

    worker_cancellation.cancel();
    drop(lanes);
    let mut shutdown_error = None;
    while let Some(joined) = workers.join_next().await {
        let error = match joined {
            Ok(Ok(())) => None,
            Ok(Err(error)) => {
                Some(error.context("game-created outbox worker failed during shutdown"))
            }
            Err(error) => Some(anyhow::anyhow!(
                "game-created outbox worker task failed during shutdown: {error}"
            )),
        };
        if shutdown_error.is_none() {
            shutdown_error = error;
        }
    }
    loop_result.and(shutdown_error.map_or(Ok(()), Err))
}

#[derive(Debug)]
enum MatchCreationOutcome {
    Committed(u32),
    Conflict { game_id: u32, reason: String },
}

struct PreparedMatch {
    game_id: u32,
    partition_id: u32,
    game_state: GameState,
    match_info: ActiveMatch,
}

/// Explicit player-level team assignment
#[derive(Debug, Clone)]
struct TeamAssignment {
    lobby_code: String,
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
    spectators: Vec<(String, Vec<usize>)>,
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
    // Prefer exact solo lobbies first
    if let Some(lobby) = lobbies.iter().find(|l| l.members.len() == 1) {
        return Some(MatchmakingCombination {
            lobbies: vec![lobby.clone()],
            team_assignments: Vec::new(), // Solo has no teams
            spectators: Vec::new(),
            total_players: 1,
            avg_mmr: lobby.avg_mmr,
        });
    }

    // Allow larger lobbies by selecting the requesting user as the lone player and marking
    // everyone else as spectators.
    if let Some(lobby) = lobbies.iter().find(|l| !l.members.is_empty()) {
        let requesting_idx = lobby
            .members
            .iter()
            .position(|m| m.user_id == lobby.requesting_user_id)
            .unwrap_or(0);

        let mut spectator_indices: Vec<usize> = (0..lobby.members.len()).collect();
        spectator_indices.retain(|idx| *idx != requesting_idx);

        return Some(MatchmakingCombination {
            lobbies: vec![lobby.clone()],
            team_assignments: Vec::new(), // Solo has no teams
            spectators: if spectator_indices.is_empty() {
                Vec::new()
            } else {
                vec![(lobby.lobby_code.clone(), spectator_indices)]
            },
            total_players: 1,
            avg_mmr: lobby.avg_mmr,
        });
    }

    None
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

    // Priority 2: Single lobby can be split exactly across both teams
    if let Some(combo) = find_single_lobby_split(lobbies, per_team, total_needed) {
        return Some(combo);
    }

    // Priority 2: Matches with spectators (lobby has too many players)
    find_team_match_with_spectators(lobbies, per_team, total_needed)
}

/// Find exact team match using recursive backtracking
fn find_exact_team_match(
    lobbies: &[crate::matchmaking_manager::QueuedLobby],
    per_team: usize,
    _total_needed: usize,
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
    spectators: Vec<(String, Vec<usize>)>,
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
            lobby_code: lobbies[lobby_idx].lobby_code.clone(),
            member_indices,
            team_id: common::TeamId(0),
        });
    }
    for (lobby_idx, member_indices) in team_b {
        team_assignments.push(TeamAssignment {
            lobby_code: lobbies[lobby_idx].lobby_code.clone(),
            member_indices,
            team_id: common::TeamId(1),
        });
    }

    // Calculate total players and average MMR
    let mut total_players = 0;
    let mut total_mmr_weighted = 0;

    for assignment in &team_assignments {
        let lobby = lobbies
            .iter()
            .find(|l| l.lobby_code == assignment.lobby_code)?;
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

/// Try to build a combination by splitting a single lobby evenly across both teams
fn find_single_lobby_split(
    lobbies: &[crate::matchmaking_manager::QueuedLobby],
    per_team: usize,
    total_needed: usize,
) -> Option<MatchmakingCombination> {
    for lobby in lobbies {
        if lobby.members.len() != total_needed {
            continue;
        }

        let host_index = lobby
            .members
            .iter()
            .position(|m| m.user_id == lobby.requesting_user_id);

        let mut player_indices = Vec::with_capacity(total_needed);

        if let Some(idx) = host_index {
            player_indices.push(idx);
        }

        for idx in 0..lobby.members.len() {
            if player_indices.len() >= total_needed {
                break;
            }
            if Some(idx) != host_index {
                player_indices.push(idx);
            }
        }

        if player_indices.len() < total_needed {
            continue;
        }

        let team_a_members: Vec<usize> = player_indices.iter().take(per_team).copied().collect();
        let team_b_members: Vec<usize> = player_indices
            .iter()
            .skip(per_team)
            .take(per_team)
            .copied()
            .collect();

        if team_a_members.len() < per_team || team_b_members.len() < per_team {
            continue;
        }

        return Some(MatchmakingCombination {
            lobbies: vec![lobby.clone()],
            team_assignments: vec![
                TeamAssignment {
                    lobby_code: lobby.lobby_code.clone(),
                    member_indices: team_a_members,
                    team_id: common::TeamId(0),
                },
                TeamAssignment {
                    lobby_code: lobby.lobby_code.clone(),
                    member_indices: team_b_members,
                    team_id: common::TeamId(1),
                },
            ],
            spectators: Vec::new(),
            total_players: total_needed,
            avg_mmr: lobby.avg_mmr,
        });
    }

    None
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
                .position(|m| m.user_id == lobby.requesting_user_id);

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
                    lobby_code: lobby.lobby_code.clone(),
                    member_indices: team_a_members,
                    team_id: common::TeamId(0),
                },
                TeamAssignment {
                    lobby_code: lobby.lobby_code.clone(),
                    member_indices: team_b_members,
                    team_id: common::TeamId(1),
                },
            ];

            return Some(MatchmakingCombination {
                lobbies: vec![lobby.clone()],
                team_assignments,
                spectators: vec![(lobby.lobby_code.clone(), spectator_indices)],
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
    let now_ms = Utc::now().timestamp_millis();
    let max_depth = lobbies.len().min(FFA_MAX_RECURSION_DEPTH);

    fn passes_wait_time_rules(
        player_count: usize,
        longest_wait_ms: i64,
        max_players: usize,
    ) -> bool {
        if player_count < 2 {
            return false;
        }

        if player_count >= max_players {
            return true;
        }

        let wait_seconds = (longest_wait_ms as f64) / 1000.0;
        match player_count {
            2 => wait_seconds > 30.0,
            3 => wait_seconds > 15.0,
            _ => true,
        }
    }

    fn build_ffa_combination_from_selection(
        lobbies: &[crate::matchmaking_manager::QueuedLobby],
        selection: &[usize],
        max_players: usize,
    ) -> MatchmakingCombination {
        let mut used_lobbies = Vec::new();
        let mut spectators: Vec<(String, Vec<usize>)> = Vec::new();
        let mut remaining_slots = max_players;
        let mut total_players = 0;
        let mut total_mmr_weighted = 0;

        for &lobby_idx in selection {
            let lobby = &lobbies[lobby_idx];
            used_lobbies.push(lobby.clone());

            let requesting_idx = lobby
                .members
                .iter()
                .position(|m| m.user_id == lobby.requesting_user_id);

            let mut playing_indices = Vec::new();

            if remaining_slots > 0
                && let Some(idx) = requesting_idx
            {
                playing_indices.push(idx);
                remaining_slots -= 1;
                total_players += 1;
                total_mmr_weighted += lobby.avg_mmr;
            }

            for idx in 0..lobby.members.len() {
                if remaining_slots == 0 {
                    break;
                }
                if Some(idx) == requesting_idx {
                    continue;
                }
                playing_indices.push(idx);
                remaining_slots -= 1;
                total_players += 1;
                total_mmr_weighted += lobby.avg_mmr;
            }

            let spectator_indices: Vec<usize> = (0..lobby.members.len())
                .filter(|idx| !playing_indices.contains(idx))
                .collect();

            if !spectator_indices.is_empty() {
                spectators.push((lobby.lobby_code.clone(), spectator_indices));
            }
        }

        let avg_mmr = if total_players > 0 {
            total_mmr_weighted / total_players as i32
        } else {
            0
        };

        MatchmakingCombination {
            lobbies: used_lobbies,
            team_assignments: Vec::new(),
            spectators,
            total_players,
            avg_mmr,
        }
    }

    // Keep a complete party intact even when older partial lobbies are also
    // waiting. Without this fast path, the include-first backtracking below can
    // fill the first slots from a partial lobby and turn members of the complete
    // lobby into spectators.
    if let Some((lobby_idx, _)) = lobbies
        .iter()
        .enumerate()
        .find(|(_, lobby)| lobby.members.len() == max_players)
    {
        return Some(build_ffa_combination_from_selection(
            lobbies,
            &[lobby_idx],
            max_players,
        ));
    }

    // Fast path: single-lobby decision based on wait thresholds
    if lobbies.len() == 1 {
        let lobby = &lobbies[0];
        let player_count = lobby.members.len().min(max_players);
        let wait_ms = now_ms - lobby.queued_at;
        if passes_wait_time_rules(player_count, wait_ms, max_players) {
            return Some(build_ffa_combination_from_selection(
                lobbies,
                &[0],
                max_players,
            ));
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn backtrack_ffa(
        lobbies: &[crate::matchmaking_manager::QueuedLobby],
        max_players: usize,
        now_ms: i64,
        idx: usize,
        depth: usize,
        max_depth: usize,
        selection: &mut Vec<usize>,
        total_members: usize,
        longest_wait_ms: i64,
    ) -> Option<MatchmakingCombination> {
        // Evaluate current selection before diving deeper
        if !selection.is_empty() {
            let player_count = total_members.min(max_players);
            if passes_wait_time_rules(player_count, longest_wait_ms, max_players) {
                return Some(build_ffa_combination_from_selection(
                    lobbies,
                    selection,
                    max_players,
                ));
            }
        }

        if depth >= max_depth || idx >= lobbies.len() {
            return None;
        }

        // Option 1: include current lobby
        let lobby = &lobbies[idx];
        selection.push(idx);
        let new_total = total_members + lobby.members.len();
        let new_longest = longest_wait_ms.max(now_ms - lobby.queued_at);
        if let Some(combo) = backtrack_ffa(
            lobbies,
            max_players,
            now_ms,
            idx + 1,
            depth + 1,
            max_depth,
            selection,
            new_total,
            new_longest,
        ) {
            return Some(combo);
        }
        selection.pop();

        // Option 2: skip current lobby
        backtrack_ffa(
            lobbies,
            max_players,
            now_ms,
            idx + 1,
            depth + 1,
            max_depth,
            selection,
            total_members,
            longest_wait_ms,
        )
    }

    backtrack_ffa(
        lobbies,
        max_players,
        now_ms,
        0,
        0,
        max_depth,
        &mut Vec::new(),
        0,
        0,
    )
}

/// Main matchmaking loop
pub async fn run_matchmaking_loop(
    mut matchmaking_manager: MatchmakingManager,
    cancellation_token: CancellationToken,
    lobby_manager: Arc<LobbyManager>,
    db: Arc<dyn Database>,
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
            GameType::FreeForAll { max_players: 4 },
            GameType::TeamMatch { per_team: 1 },
            GameType::TeamMatch { per_team: 2 },
        ];

        let mut total_games_created = 0;

        for game_type in &game_types {
            // Try lobby-based matchmaking for quickmatch
            match create_lobby_matches(
                &mut matchmaking_manager,
                game_type.clone(),
                common::QueueMode::Quickmatch,
                lobby_manager.clone(),
                db.clone(),
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
                game_type.clone(),
                common::QueueMode::Competitive,
                lobby_manager.clone(),
                db.clone(),
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
            lobby1_id = lobby1.lobby_code,
            lobby2_id = lobby2.lobby_code,
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
            lobby.lobby_code == reference_lobby.lobby_code
                || are_lobbies_compatible(reference_lobby, lobby, now_ms)
        })
        .cloned()
        .collect()
}

/// Create matches from lobbies in the queue using advanced combination matching
async fn create_lobby_matches(
    matchmaking_manager: &mut MatchmakingManager,
    game_type: GameType,
    queue_mode: common::QueueMode,
    lobby_manager: Arc<LobbyManager>,
    db: Arc<dyn Database>,
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
            lobby_id = lobby.lobby_code,
            wait_seconds = wait_seconds,
            original_mmr = lobby.avg_mmr,
            max_mmr_diff = max_mmr_diff,
            "Calculated acceptable MMR range for lobby"
        );
    }

    // Sort lobbies by wait time (longest waiting first) for priority matching
    available_lobbies.sort_by_key(|a| a.queued_at);

    let mut games_created = 0;

    // Try to create as many games as possible from available lobbies
    while !available_lobbies.is_empty() {
        // Randomly choose between:
        // 1. Get the longest-waiting lobby (first in sorted list)
        // 2. Random lobby
        let priority_lobby = if rand::random::<f32>() < 0.5 {
            &available_lobbies[0]
        } else {
            &available_lobbies[rand::random::<usize>() % available_lobbies.len()]
        };

        let wait_time_s = ((now - priority_lobby.queued_at) as f64) / 1000.0;
        let max_acceptable_mmr_diff = calculate_max_mmr_diff(wait_time_s);

        info!(
            priority_lobby_id = priority_lobby.lobby_code,
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
            priority_lobby_id = priority_lobby.lobby_code,
            compatible_count = compatible_lobbies.len(),
            total_available = available_lobbies.len(),
            "Compatibility filtering complete"
        );

        if compatible_lobbies.is_empty() {
            // No compatible lobbies found, wait for more time to pass
            warn!(
                lobby_id = priority_lobby.lobby_code,
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

        let creation = create_game_from_lobbies(
            matchmaking_manager,
            &game_type,
            &queue_mode,
            &combination,
            db.as_ref(),
        )
        .await;

        match creation {
            Ok(MatchCreationOutcome::Committed(game_id)) => {
                games_created += 1;
                info!(
                    "Created game {} from {} lobbies with {} total players (avg MMR: {})",
                    game_id,
                    combination.lobbies.len(),
                    combination.total_players,
                    combination.avg_mmr
                );

                // The atomic commit removed every exact queue identity and
                // moved durable lobby metadata to `matched`. Pub/Sub remains
                // a best-effort presentation refresh.
                for lobby in &combination.lobbies {
                    available_lobbies.retain(|l| l.lobby_code != lobby.lobby_code);

                    if let Err(e) = lobby_manager.publish_lobby_update(&lobby.lobby_code).await {
                        error!(
                            lobby_code = lobby.lobby_code,
                            error = %e,
                            "Failed to publish matched lobby state"
                        );
                    }
                }
            }
            Ok(MatchCreationOutcome::Conflict { game_id, reason }) => {
                // The selected identities are stale. Do not repeatedly allocate IDs for
                // them in this local pass; the next Redis read will observe the winner.
                for lobby in &combination.lobbies {
                    available_lobbies.retain(|candidate| {
                        candidate.lobby_code != lobby.lobby_code
                            || candidate.queue_token != lobby.queue_token
                    });
                }
                info!(
                    game_id,
                    reason, "Atomic matchmaking claim lost; discarded the unused durable game ID"
                );
            }
            Err(e) => {
                error!("Failed to create game from lobby combination: {}", e);
                break; // Stop trying to create more games on error
            }
        }
    }

    Ok(games_created)
}

/// Allocate an ID and construct all match data without mutating Valkey.
async fn prepare_game_from_lobbies(
    matchmaking_manager: &mut MatchmakingManager,
    game_type: &GameType,
    queue_mode: &common::QueueMode,
    combination: &MatchmakingCombination,
    db: &dyn Database,
) -> Result<PreparedMatch> {
    let game_id = matchmaking_manager.generate_game_id(db).await?;
    let partition_id = game_id % PARTITION_COUNT;

    // Create game state
    let start_ms = Utc::now().timestamp_millis() + GAME_START_DELAY_MS;

    let (width, height) = match game_type {
        GameType::TeamMatch { .. } => (60, 40),
        _ => (40, 40),
    };

    let rng_seed = Some(Utc::now().timestamp_millis() as u64 ^ (game_id as u64));
    let mut game_state = GameState::new(
        width,
        height,
        game_type.clone(),
        queue_mode.clone(),
        rng_seed,
        start_ms,
    );

    // Apply queue-mode-specific time limits for team games
    if matches!(game_type, GameType::TeamMatch { .. }) {
        game_state.properties.time_limit_ms = Some(match queue_mode {
            common::QueueMode::Quickmatch => DEFAULT_QUICKMATCH_TEAM_TIME_LIMIT_MS,
            common::QueueMode::Competitive => DEFAULT_TEAM_TIME_LIMIT_MS,
        });
    }

    // Add players to game state with team assignments
    let mut all_players = Vec::new();
    let mut spectators: Vec<QueuedPlayer> = Vec::new();
    use std::collections::{HashMap, HashSet};

    // Build quick lookup for spectators keyed by lobby_code
    let spectator_map: HashMap<&str, HashSet<usize>> = combination
        .spectators
        .iter()
        .map(|(code, indices)| (code.as_str(), indices.iter().copied().collect()))
        .collect();

    if !combination.team_assignments.is_empty() {
        // Add all assigned players to the game
        for assignment in &combination.team_assignments {
            let lobby = combination
                .lobbies
                .iter()
                .find(|l| l.lobby_code == assignment.lobby_code)
                .ok_or_else(|| {
                    anyhow::anyhow!("Lobby {} not found in combination", assignment.lobby_code)
                })?;

            for &member_idx in &assignment.member_indices {
                if let Some(member) = lobby.members.get(member_idx) {
                    // Add player to game state with explicit team assignment
                    game_state.add_player_with_team(
                        member.user_id,
                        Some(member.username.clone()),
                        Some(assignment.team_id),
                    )?;

                    all_players.push(QueuedPlayer {
                        user_id: member.user_id,
                        mmr: combination.avg_mmr,
                        username: member.username.clone(),
                    });
                }
            }
        }

        // Register any extras marked as spectators (not part of team assignments)
        for lobby in &combination.lobbies {
            if let Some(indices) = spectator_map.get(lobby.lobby_code.as_str()) {
                for idx in indices {
                    if let Some(member) = lobby.members.get(*idx) {
                        game_state.add_spectator(member.user_id, Some(member.username.clone()));
                        spectators.push(QueuedPlayer {
                            user_id: member.user_id,
                            mmr: combination.avg_mmr,
                            username: member.username.clone(),
                        });
                    }
                }
            }
        }
    } else {
        // Non-team game (Solo, FFA)
        for lobby in &combination.lobbies {
            let spectators_for_lobby = spectator_map
                .get(lobby.lobby_code.as_str())
                .cloned()
                .unwrap_or_default();

            for (idx, member) in lobby.members.iter().enumerate() {
                // Skip spectators for solo queues that came from multi-member lobbies
                if spectators_for_lobby.contains(&idx) {
                    game_state.add_spectator(member.user_id, Some(member.username.clone()));
                    spectators.push(QueuedPlayer {
                        user_id: member.user_id,
                        mmr: combination.avg_mmr,
                        username: member.username.clone(),
                    });
                    continue;
                }

                game_state.add_player(member.user_id, Some(member.username.clone()))?;

                all_players.push(QueuedPlayer {
                    user_id: member.user_id,
                    mmr: combination.avg_mmr,
                    username: member.username.clone(),
                });
            }
        }
    }

    game_state.spawn_initial_food();

    let mut lobby_codes: Vec<String> = combination
        .lobbies
        .iter()
        .map(|lobby| lobby.lobby_code.clone())
        .collect();
    lobby_codes.sort();
    lobby_codes.dedup();
    let match_info = ActiveMatch {
        players: all_players,
        game_type: game_type.clone(),
        status: MatchStatus::Waiting,
        partition_id,
        created_at: Utc::now().timestamp_millis(),
        spectators,
        lobby_codes,
    };

    Ok(PreparedMatch {
        game_id,
        partition_id,
        game_state,
        match_info,
    })
}

async fn create_game_from_lobbies(
    matchmaking_manager: &mut MatchmakingManager,
    game_type: &GameType,
    queue_mode: &common::QueueMode,
    combination: &MatchmakingCombination,
    db: &dyn Database,
) -> Result<MatchCreationOutcome> {
    let prepared =
        prepare_game_from_lobbies(matchmaking_manager, game_type, queue_mode, combination, db)
            .await?;

    let event = StreamEvent::GameCreated {
        game_id: prepared.game_id,
        game_state: prepared.game_state,
    };
    let payload = serde_json::to_string(&event).context("Failed to serialize GameCreated")?;

    match matchmaking_manager
        .commit_match(
            prepared.game_id,
            prepared.partition_id,
            game_type,
            queue_mode,
            &prepared.match_info,
            &payload,
            &combination.lobbies,
        )
        .await?
    {
        MatchCommitOutcome::Committed { outbox_id } => {
            info!(
                game_id = prepared.game_id,
                partition_id = prepared.partition_id,
                outbox_id,
                "Atomically committed match and durable GameCreated outbox record"
            );
            Ok(MatchCreationOutcome::Committed(prepared.game_id))
        }
        MatchCommitOutcome::AlreadyCommitted => {
            warn!(
                game_id = prepared.game_id,
                "Atomic match commit was retried after an ambiguous response"
            );
            Ok(MatchCreationOutcome::Committed(prepared.game_id))
        }
        MatchCommitOutcome::Conflict { reason } => Ok(MatchCreationOutcome::Conflict {
            game_id: prepared.game_id,
            reason,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lobby_manager::LobbyMember;
    use crate::matchmaking_manager::QueuedLobby;
    use crate::redis_keys::RedisKeys;
    use crate::redis_utils::{RedisClient, RedisConnection};
    use common::{QueueMode, TeamId};
    use redis::AsyncCommands;

    static OUTBOX_DB10_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn ffa_lobby(code: &str, user_ids: &[u32], queued_at: i64) -> QueuedLobby {
        QueuedLobby {
            lobby_code: code.to_string(),
            queue_token: format!("token-{code}"),
            members: user_ids
                .iter()
                .map(|user_id| LobbyMember {
                    user_id: *user_id,
                    username: format!("player_{user_id}"),
                    ts: queued_at as f64,
                })
                .collect(),
            avg_mmr: 1000,
            game_types: vec![GameType::FreeForAll { max_players: 4 }],
            queue_mode: QueueMode::Quickmatch,
            queued_at,
            requesting_user_id: user_ids[0],
        }
    }

    fn random_game_id_base() -> u32 {
        let candidate = rand::random::<u32>() % (u32::MAX - 2 * PARTITION_COUNT);
        candidate - candidate % PARTITION_COUNT
    }

    fn game_created_outbox_delivery(game_id: u32) -> Result<GameCreatedOutboxDelivery> {
        let game_created_payload = serde_json::to_string(&StreamEvent::GameCreated {
            game_id,
            game_state: GameState::new(
                10,
                10,
                GameType::Solo,
                QueueMode::Quickmatch,
                Some(u64::from(game_id)),
                Utc::now().timestamp_millis(),
            ),
        })?;
        let record = GameCreatedOutboxRecord {
            schema_version: 1,
            game_id,
            partition_id: game_id % PARTITION_COUNT,
            game_created_payload,
        };
        record.validate()?;
        Ok(GameCreatedOutboxDelivery {
            expected_payload: serde_json::to_string(&record)?,
            record,
        })
    }

    #[tokio::test]
    async fn test_match_creation_logic() {
        // Test the match creation logic
        // This would require mocking Redis and PubSub
    }

    #[tokio::test]
    async fn stalled_game_created_lane_does_not_block_another_partition() -> Result<()> {
        let _test_lock = OUTBOX_DB10_TEST_LOCK.lock().await;
        // Dedicated logical DB for this test. Nothing is flushed: every key
        // touched below is explicitly removed before and after the exercise.
        let url = "redis://127.0.0.1:6379/10?protocol=resp3";
        let client = redis::Client::open(url)?;
        let app_client = RedisClient::open(url, None)?;
        let global = app_client.get_managed_connection().await?;
        let partition_zero = app_client.get_managed_connection().await?;
        let partition_one = app_client.get_managed_connection().await?;
        let matchmaking = MatchmakingManager::new(global.clone())?;
        let mut partition_connections: Vec<RedisConnection> =
            (0..PARTITION_COUNT).map(|_| global.clone()).collect();
        partition_connections[0] = partition_zero.clone();
        partition_connections[1] = partition_one;
        let bus = Arc::new(GameBus::new(
            global.clone(),
            partition_connections,
            (0..PARTITION_COUNT).map(|_| global.clone()).collect(),
            global,
            app_client,
            CancellationToken::new(),
        )?);

        let salt = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let base_game_id = random_game_id_base();
        let partition_zero_game_id = base_game_id;
        let partition_one_game_id = base_game_id + 1;
        let partition_zero_field = partition_zero_game_id.to_string();
        let partition_one_field = partition_one_game_id.to_string();
        let partition_zero_payload =
            game_created_outbox_delivery(partition_zero_game_id)?.expected_payload;
        let partition_one_payload =
            game_created_outbox_delivery(partition_one_game_id)?.expected_payload;

        let outbox_key = RedisKeys::matchmaking_game_created_outbox();
        let partition_zero_stream = RedisKeys::stream_commands(0);
        let partition_one_stream = RedisKeys::stream_commands(1);
        let partition_zero_marker =
            RedisKeys::matchmaking_game_created_delivery(partition_zero_game_id);
        let partition_one_marker =
            RedisKeys::matchmaking_game_created_delivery(partition_one_game_id);
        let stall_key = format!("snaketron:test:game-created-outbox-stall:{salt}");
        let lane_name = format!("snaketron-outbox-lane-stall-{salt}");
        let mut inspector = client.get_multiplexed_async_connection().await?;
        let _: usize = inspector
            .hdel(
                &outbox_key,
                &[partition_zero_field.as_str(), partition_one_field.as_str()],
            )
            .await?;
        let _: usize = inspector
            .del(&[
                stall_key.as_str(),
                partition_zero_stream.as_str(),
                partition_one_stream.as_str(),
                partition_zero_marker.as_str(),
                partition_one_marker.as_str(),
            ])
            .await?;
        let _: usize = inspector
            .hset(&outbox_key, &partition_zero_field, &partition_zero_payload)
            .await?;
        let _: usize = inspector
            .hset(&outbox_key, &partition_one_field, &partition_one_payload)
            .await?;

        let mut stalled_connection = partition_zero.clone();
        let _: () = redis::cmd("CLIENT")
            .arg("SETNAME")
            .arg(&lane_name)
            .query_async(&mut stalled_connection)
            .await?;
        let blocked_key = stall_key.clone();
        let blocked_read = tokio::spawn(async move {
            redis::cmd("BLPOP")
                .arg(blocked_key)
                .arg(0)
                .query_async::<Option<(String, String)>>(&mut stalled_connection)
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let clients: String = redis::cmd("CLIENT")
                    .arg("LIST")
                    .query_async(&mut inspector)
                    .await?;
                if clients.lines().any(|line| {
                    line.contains(&format!("name={lane_name}")) && line.contains("cmd=blpop")
                }) {
                    return Ok::<(), anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .context("partition-zero outbox lane never entered its blocking operation")??;

        let cancellation = CancellationToken::new();
        let outbox_loop = tokio::spawn(run_game_created_outbox_loop(
            matchmaking,
            bus,
            cancellation.clone(),
        ));

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let source: Option<String> =
                    inspector.hget(&outbox_key, &partition_one_field).await?;
                let stream_len: u64 = inspector.xlen(&partition_one_stream).await?;
                let marker_ttl_ms: i64 = inspector.pttl(&partition_one_marker).await?;
                if source.is_none() && stream_len == 1 && marker_ttl_ms > 0 {
                    return Ok::<(), anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .context("partition-one GameCreated was delayed behind stalled partition zero")??;
        let partition_zero_source: Option<String> =
            inspector.hget(&outbox_key, &partition_zero_field).await?;
        anyhow::ensure!(
            partition_zero_source.as_deref() == Some(partition_zero_payload.as_str()),
            "partition-zero source record changed while its publish was blocked"
        );
        anyhow::ensure!(
            inspector.xlen::<_, u64>(&partition_zero_stream).await? == 0,
            "partition-zero command was delivered before its lane was released"
        );
        anyhow::ensure!(
            !blocked_read.is_finished() && !outbox_loop.is_finished(),
            "blocked lane or critical outbox loop exited before release"
        );

        let _: usize = inspector.rpush(&stall_key, "release").await?;
        tokio::time::timeout(Duration::from_secs(1), blocked_read)
            .await
            .context("partition-zero blocking operation did not release")???;
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let source: Option<String> =
                    inspector.hget(&outbox_key, &partition_zero_field).await?;
                let stream_len: u64 = inspector.xlen(&partition_zero_stream).await?;
                let marker_ttl_ms: i64 = inspector.pttl(&partition_zero_marker).await?;
                if source.is_none() && stream_len == 1 && marker_ttl_ms > 0 {
                    return Ok::<(), anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .context("partition-zero GameCreated did not complete after release")??;

        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(1), outbox_loop)
            .await
            .context("game-created outbox loop did not stop after cancellation")???;
        let _: usize = inspector
            .hdel(
                &outbox_key,
                &[partition_zero_field.as_str(), partition_one_field.as_str()],
            )
            .await?;
        let _: usize = inspector
            .del(&[
                stall_key.as_str(),
                partition_zero_stream.as_str(),
                partition_one_stream.as_str(),
                partition_zero_marker.as_str(),
                partition_one_marker.as_str(),
            ])
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn game_created_lane_continues_after_one_record_fails() -> Result<()> {
        let _test_lock = OUTBOX_DB10_TEST_LOCK.lock().await;
        let url = "redis://127.0.0.1:6379/10?protocol=resp3";
        let client = redis::Client::open(url)?;
        let app_client = RedisClient::open(url, None)?;
        let global = app_client.get_managed_connection().await?;
        let partition_redis = app_client.get_managed_connection().await?;
        let matchmaking = MatchmakingManager::new(global.clone())?;
        let partition_id = 2;
        let mut partition_connections = (0..PARTITION_COUNT)
            .map(|_| global.clone())
            .collect::<Vec<_>>();
        partition_connections[partition_id as usize] = partition_redis;
        let bus = Arc::new(GameBus::new(
            global.clone(),
            partition_connections,
            (0..PARTITION_COUNT).map(|_| global.clone()).collect(),
            global,
            app_client,
            CancellationToken::new(),
        )?);

        let base_game_id = random_game_id_base();
        let failing_game_id = base_game_id + partition_id;
        let valid_game_id = failing_game_id + PARTITION_COUNT;
        let failing_field = failing_game_id.to_string();
        let valid_field = valid_game_id.to_string();
        let failing_delivery = game_created_outbox_delivery(failing_game_id)?;
        let valid_delivery = game_created_outbox_delivery(valid_game_id)?;
        let failing_payload = failing_delivery.expected_payload.clone();
        let valid_payload = valid_delivery.expected_payload.clone();
        let outbox_key = RedisKeys::matchmaking_game_created_outbox();
        let command_stream = RedisKeys::stream_commands(partition_id);
        let failing_marker = RedisKeys::matchmaking_game_created_delivery(failing_game_id);
        let valid_marker = RedisKeys::matchmaking_game_created_delivery(valid_game_id);
        let mut inspector = client.get_multiplexed_async_connection().await?;
        let _: usize = inspector
            .hdel(&outbox_key, &[failing_field.as_str(), valid_field.as_str()])
            .await?;
        let _: usize = inspector
            .del(&[
                command_stream.as_str(),
                failing_marker.as_str(),
                valid_marker.as_str(),
            ])
            .await?;
        let _: usize = inspector
            .hset(&outbox_key, &failing_field, &failing_payload)
            .await?;
        let _: usize = inspector
            .hset(&outbox_key, &valid_field, &valid_payload)
            .await?;
        // GET inside the idempotent publish script fails only for this record.
        let _: usize = inspector.rpush(&failing_marker, "wrong-type").await?;

        let cancellation = CancellationToken::new();
        let (sender, receiver) = mpsc::channel(1);
        let worker = tokio::spawn(run_game_created_outbox_worker(
            partition_id,
            matchmaking,
            bus,
            receiver,
            cancellation.clone(),
        ));
        sender
            .send(vec![failing_delivery, valid_delivery])
            .await
            .context("same-partition outbox worker closed before receiving its batch")?;

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let valid_source: Option<String> =
                    inspector.hget(&outbox_key, &valid_field).await?;
                let stream_len: u64 = inspector.xlen(&command_stream).await?;
                let marker_ttl_ms: i64 = inspector.pttl(&valid_marker).await?;
                if valid_source.is_none() && stream_len == 1 && marker_ttl_ms > 0 {
                    return Ok::<(), anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .context("valid record remained starved behind a failed same-partition record")??;
        let failing_source: Option<String> = inspector.hget(&outbox_key, &failing_field).await?;
        anyhow::ensure!(
            failing_source.as_deref() == Some(failing_payload.as_str()),
            "failed source record was unexpectedly acknowledged"
        );

        cancellation.cancel();
        drop(sender);
        tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .context("same-partition outbox worker did not stop after cancellation")???;
        let _: usize = inspector
            .hdel(&outbox_key, &[failing_field.as_str(), valid_field.as_str()])
            .await?;
        let _: usize = inspector
            .del(&[
                command_stream.as_str(),
                failing_marker.as_str(),
                valid_marker.as_str(),
            ])
            .await?;
        Ok(())
    }

    #[test]
    fn one_player_lobby_forms_a_solo_match() {
        let lobby = QueuedLobby {
            lobby_code: "SOLO1".to_string(),
            queue_token: "token-solo1".to_string(),
            members: vec![LobbyMember {
                user_id: 5,
                username: "solo_player".to_string(),
                ts: 100.0,
            }],
            avg_mmr: 1000,
            game_types: vec![GameType::Solo],
            queue_mode: QueueMode::Quickmatch,
            queued_at: 0,
            requesting_user_id: 5,
        };

        let combo = find_best_lobby_combination(&[lobby], &GameType::Solo)
            .expect("a one-player lobby should form one solo match");

        assert_eq!(combo.total_players, 1);
        assert_eq!(combo.lobbies.len(), 1);
        assert!(combo.team_assignments.is_empty());
        assert!(combo.spectators.is_empty());
    }

    #[test]
    fn duo_lobby_splits_into_duel() {
        let lobby = QueuedLobby {
            lobby_code: "ABC123".to_string(),
            queue_token: "token-abc123".to_string(),
            members: vec![
                LobbyMember {
                    user_id: 10,
                    username: "player_one".to_string(),
                    ts: 123.0,
                },
                LobbyMember {
                    user_id: 11,
                    username: "player_two".to_string(),
                    ts: 124.0,
                },
            ],
            avg_mmr: 1200,
            game_types: vec![GameType::TeamMatch { per_team: 1 }],
            queue_mode: QueueMode::Quickmatch,
            queued_at: 0,
            requesting_user_id: 10,
        };

        let combo = find_best_lobby_combination(&[lobby], &GameType::TeamMatch { per_team: 1 })
            .expect("expected to find a duel combination for a two-player lobby");

        assert_eq!(combo.total_players, 2);
        assert_eq!(combo.lobbies.len(), 1);
        assert_eq!(combo.team_assignments.len(), 2);

        let mut team_a_indices = Vec::new();
        let mut team_b_indices = Vec::new();

        for assignment in &combo.team_assignments {
            if assignment.team_id == TeamId(0) {
                team_a_indices = assignment.member_indices.clone();
            } else if assignment.team_id == TeamId(1) {
                team_b_indices = assignment.member_indices.clone();
            }
        }

        assert_eq!(team_a_indices, vec![0]);
        assert_eq!(team_b_indices, vec![1]);
    }

    #[test]
    fn four_player_lobby_splits_into_two_v_two_without_spectators() {
        let lobby = QueuedLobby {
            lobby_code: "TEAM4".to_string(),
            queue_token: "token-team4".to_string(),
            members: (20..24)
                .map(|user_id| LobbyMember {
                    user_id,
                    username: format!("player_{user_id}"),
                    ts: f64::from(user_id),
                })
                .collect(),
            avg_mmr: 1200,
            game_types: vec![GameType::TeamMatch { per_team: 2 }],
            queue_mode: QueueMode::Quickmatch,
            queued_at: 0,
            requesting_user_id: 20,
        };

        let combo = find_best_lobby_combination(&[lobby], &GameType::TeamMatch { per_team: 2 })
            .expect("a four-player lobby should form one 2v2 match");

        assert_eq!(combo.total_players, 4);
        assert_eq!(combo.lobbies.len(), 1);
        assert!(combo.spectators.is_empty());
        assert_eq!(combo.team_assignments.len(), 2);
        assert!(
            combo
                .team_assignments
                .iter()
                .all(|assignment| assignment.member_indices.len() == 2)
        );
    }

    #[test]
    fn ffa_prefers_full_lobby_over_older_partial_lobby() {
        let now = Utc::now().timestamp_millis();
        let partial = ffa_lobby("PARTIAL", &[1], now - 1_000);
        let full = ffa_lobby("FULL", &[10, 11, 12, 13], now);

        let combo = find_ffa_combination(&[partial, full], 4)
            .expect("a complete four-player FFA lobby should match");

        assert_eq!(combo.total_players, 4);
        assert_eq!(combo.lobbies.len(), 1);
        assert_eq!(combo.lobbies[0].lobby_code, "FULL");
        assert!(combo.spectators.is_empty());
    }

    #[test]
    fn ffa_still_combines_partial_lobbies_when_no_full_lobby_exists() {
        let now = Utc::now().timestamp_millis();
        let first = ffa_lobby("FIRST", &[1, 2], now);
        let second = ffa_lobby("SECOND", &[3, 4], now);

        let combo = find_ffa_combination(&[first, second], 4)
            .expect("two fresh two-player lobbies should form a four-player FFA");

        assert_eq!(combo.total_players, 4);
        assert_eq!(combo.lobbies.len(), 2);
        assert!(combo.spectators.is_empty());
    }
}
