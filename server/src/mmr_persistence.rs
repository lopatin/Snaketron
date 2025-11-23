use crate::db::Database;
use anyhow::{anyhow, Result};
use common::{GameState, GameType, QueueMode, TeamId};
use skillratings::weng_lin::{weng_lin, weng_lin_multi_team, weng_lin_two_teams, WengLinConfig, WengLinRating};
use skillratings::MultiTeamOutcome;
use skillratings::Outcomes;
use std::collections::HashMap;
use tracing::{error, info, warn};

/// Persist MMR changes for all players in a completed game to the database.
/// Uses the Weng-Lin algorithm to calculate new ratings and atomic ADD operations for updates.
///
/// # Arguments
/// * `db` - Database interface
/// * `game_id` - The ID of the completed game
/// * `game_state` - The final game state containing players, scores, and game type
pub async fn persist_player_mmr(
    db: &dyn Database,
    game_id: u32,
    game_state: &GameState,
) -> Result<()> {
    // Only update MMR for non-solo games
    if matches!(game_state.game_type, GameType::Solo) {
        info!("Skipping MMR update for solo game {}", game_id);
        return Ok(());
    }

    let player_count = game_state.players.len();
    if player_count == 0 {
        info!("No players to update MMR for in game {}", game_id);
        return Ok(());
    }

    info!(
        "Calculating MMR changes for game {} ({:?}, {:?}) with {} players",
        game_id, game_state.game_type, game_state.queue_mode, player_count
    );

    // Calculate MMR deltas based on game type
    let mmr_deltas = match &game_state.game_type {
        GameType::TeamMatch { per_team } => {
            calculate_team_match_mmr_deltas(db, game_state, *per_team).await?
        }
        GameType::FreeForAll { .. } => {
            calculate_ffa_mmr_deltas(db, game_state).await?
        }
        GameType::Custom { .. } => {
            // For custom games, determine if it's team-based or FFA
            if game_state.team_scores.is_some() {
                // Custom team game
                calculate_team_match_mmr_deltas(db, game_state, 1).await?
            } else {
                // Custom FFA game
                calculate_ffa_mmr_deltas(db, game_state).await?
            }
        }
        GameType::Solo => return Ok(()), // Already handled above
    };

    // Apply MMR deltas to database
    apply_mmr_deltas(db, game_id, &game_state.queue_mode, mmr_deltas).await?;

    info!("Finished persisting MMR for game {}", game_id);
    Ok(())
}

/// Calculate MMR deltas for team-based matches (1v1, 2v2, etc.)
async fn calculate_team_match_mmr_deltas(
    db: &dyn Database,
    game_state: &GameState,
    per_team: u8,
) -> Result<HashMap<u32, i32>> {
    let team_scores = game_state
        .team_scores
        .as_ref()
        .ok_or_else(|| anyhow!("Team scores missing for team match"))?;

    // Determine winning team
    let winning_team = team_scores
        .iter()
        .max_by_key(|(_, score)| *score)
        .map(|(team_id, _)| *team_id);

    // Build team rosters
    let mut team_0_users = Vec::new();
    let mut team_1_users = Vec::new();

    for (user_id, player) in &game_state.players {
        let snake = &game_state.arena.snakes[player.snake_id as usize];
        match snake.team_id {
            Some(TeamId(0)) => team_0_users.push(*user_id),
            Some(TeamId(1)) => team_1_users.push(*user_id),
            _ => warn!("Player {} has invalid team ID in game", user_id),
        }
    }

    if team_0_users.is_empty() || team_1_users.is_empty() {
        return Err(anyhow!("One or both teams are empty"));
    }

    // Get current MMRs
    let all_users: Vec<i32> = team_0_users
        .iter()
        .chain(team_1_users.iter())
        .map(|&id| id as i32)
        .collect();
    let mmr_map = db.get_user_mmrs(&all_users).await?;

    // Extract MMRs based on queue mode
    let get_mmr = |user_id: u32| -> i32 {
        mmr_map.get(&(user_id as i32)).map(|(ranked, casual)| {
            match game_state.queue_mode {
                QueueMode::Competitive => *ranked,
                QueueMode::Quickmatch => *casual,
            }
        }).unwrap_or(1000)
    };

    // Create Weng-Lin ratings
    let team_0_ratings: Vec<WengLinRating> = team_0_users
        .iter()
        .map(|&user_id| {
            let mmr = get_mmr(user_id);
            WengLinRating { rating: mmr as f64, uncertainty: 350.0 }
        })
        .collect();

    let team_1_ratings: Vec<WengLinRating> = team_1_users
        .iter()
        .map(|&user_id| {
            let mmr = get_mmr(user_id);
            WengLinRating { rating: mmr as f64, uncertainty: 350.0 }
        })
        .collect();

    // Determine outcome
    let outcome = match winning_team {
        Some(TeamId(0)) => Outcomes::WIN,
        Some(TeamId(1)) => Outcomes::LOSS,
        _ => Outcomes::DRAW, // Tie or no winner
    };

    // Calculate new ratings
    let config = WengLinConfig::new();
    let (new_team_0, new_team_1) = weng_lin_two_teams(&team_0_ratings, &team_1_ratings, &outcome, &config);

    // Calculate deltas
    let mut deltas = HashMap::new();
    for (i, &user_id) in team_0_users.iter().enumerate() {
        let old_mmr = get_mmr(user_id);
        let new_mmr = new_team_0[i].rating as i32;
        deltas.insert(user_id, new_mmr - old_mmr);
    }
    for (i, &user_id) in team_1_users.iter().enumerate() {
        let old_mmr = get_mmr(user_id);
        let new_mmr = new_team_1[i].rating as i32;
        deltas.insert(user_id, new_mmr - old_mmr);
    }

    info!(
        "Team match: Team 0 ({}), Team 1 ({}) - Winner: {:?}",
        team_0_users.len(),
        team_1_users.len(),
        winning_team
    );

    Ok(deltas)
}

/// Calculate MMR deltas for free-for-all matches
async fn calculate_ffa_mmr_deltas(
    db: &dyn Database,
    game_state: &GameState,
) -> Result<HashMap<u32, i32>> {
    // Get final placements based on scores
    let mut player_scores: Vec<(u32, u32)> = game_state
        .players
        .iter()
        .map(|(user_id, player)| {
            let score = game_state.scores.get(&player.snake_id).copied().unwrap_or(0);
            (*user_id, score)
        })
        .collect();

    // Sort by score descending (higher score = better placement)
    player_scores.sort_by(|a, b| b.1.cmp(&a.1));

    // Get current MMRs
    let all_users: Vec<i32> = player_scores.iter().map(|(id, _)| *id as i32).collect();
    let mmr_map = db.get_user_mmrs(&all_users).await?;

    // Extract MMRs based on queue mode
    let get_mmr = |user_id: u32| -> i32 {
        mmr_map.get(&(user_id as i32)).map(|(ranked, casual)| {
            match game_state.queue_mode {
                QueueMode::Competitive => *ranked,
                QueueMode::Quickmatch => *casual,
            }
        }).unwrap_or(1000)
    };

    // If only 2 players, use 1v1 algorithm
    if player_scores.len() == 2 {
        let user_0 = player_scores[0].0;
        let user_1 = player_scores[1].0;

        let rating_0 = WengLinRating { rating: get_mmr(user_0) as f64, uncertainty: 350.0 };
        let rating_1 = WengLinRating { rating: get_mmr(user_1) as f64, uncertainty: 350.0 };

        let config = WengLinConfig::new();
        let (new_rating_0, new_rating_1) = weng_lin(&rating_0, &rating_1, &Outcomes::WIN, &config);

        let mut deltas = HashMap::new();
        deltas.insert(user_0, new_rating_0.rating as i32 - get_mmr(user_0));
        deltas.insert(user_1, new_rating_1.rating as i32 - get_mmr(user_1));

        return Ok(deltas);
    }

    // For 3+ players, use multi-team algorithm (each player is their own team)
    let teams_with_ratings: Vec<Vec<WengLinRating>> = player_scores
        .iter()
        .map(|(user_id, _)| {
            vec![WengLinRating { rating: get_mmr(*user_id) as f64, uncertainty: 350.0 }]
        })
        .collect();

    // Convert ranks to MultiTeamOutcome (lower rank = better placement)
    let teams_with_outcomes: Vec<(&[WengLinRating], MultiTeamOutcome)> = teams_with_ratings
        .iter()
        .enumerate()
        .map(|(rank, team)| {
            (
                team.as_slice(),
                MultiTeamOutcome::new(rank + 1), // 1-indexed rank (1st place, 2nd place, etc.)
            )
        })
        .collect();

    let config = WengLinConfig::new();
    let new_ratings = weng_lin_multi_team(&teams_with_outcomes, &config);

    // Calculate deltas
    let mut deltas = HashMap::new();
    for (i, (user_id, _)) in player_scores.iter().enumerate() {
        let old_mmr = get_mmr(*user_id);
        let new_mmr = new_ratings[i][0].rating as i32;
        deltas.insert(*user_id, new_mmr - old_mmr);
    }

    info!(
        "FFA match: {} players, placements: {:?}",
        player_scores.len(),
        player_scores.iter().map(|(id, score)| (*id, *score)).collect::<Vec<_>>()
    );

    Ok(deltas)
}

/// Apply calculated MMR deltas to the database using atomic operations
async fn apply_mmr_deltas(
    db: &dyn Database,
    game_id: u32,
    queue_mode: &QueueMode,
    deltas: HashMap<u32, i32>,
) -> Result<()> {
    for (user_id, delta) in deltas {
        if delta == 0 {
            info!("User {} MMR unchanged in game {}", user_id, game_id);
            continue;
        }

        match db
            .update_user_mmr_by_mode(user_id as i32, delta, queue_mode)
            .await
        {
            Ok(new_total) => {
                let sign = if delta > 0 { "+" } else { "" };
                info!(
                    "User {} {:?} MMR: {}{} (new total: {}) from game {}",
                    user_id,
                    queue_mode,
                    sign,
                    delta,
                    new_total,
                    game_id
                );
            }
            Err(e) => {
                error!(
                    "Failed to update MMR for user {} in game {}: {:?}",
                    user_id, game_id, e
                );
                // Don't fail the whole operation if one user fails
                // This ensures other players still get their MMR updates
            }
        }
    }

    Ok(())
}
