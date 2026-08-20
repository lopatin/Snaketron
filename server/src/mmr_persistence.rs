use crate::db::Database;
use crate::season::{Season, get_region};
use anyhow::{Result, anyhow};
use common::{GameState, GameStatus, GameType, QueueMode, TeamId};
use skillratings::MultiTeamOutcome;
use skillratings::Outcomes;
use skillratings::weng_lin::{
    WengLinConfig, WengLinRating, weng_lin, weng_lin_multi_team, weng_lin_two_teams,
};
use std::collections::{HashMap, HashSet};
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MmrEffectSpec {
    pub user_id: u32,
    pub delta: i32,
    pub won: bool,
}

/// Inactivity completions carry their outcome in the terminal status rather
/// than in the score tables. Keeping that distinction explicit prevents a
/// 0-0 forfeit from being materialized as a draw while leaving every ordinary
/// completion's score-based ranking unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MmrOutcomeOverride {
    ScoreBased,
    NoContest,
    WinningSnake(u32),
}

fn mmr_outcome_override(game_state: &GameState) -> Result<MmrOutcomeOverride> {
    if !game_state.completed_by_inactivity {
        return Ok(MmrOutcomeOverride::ScoreBased);
    }

    match game_state.status {
        GameStatus::Complete {
            winning_snake_id: Some(snake_id),
        } => Ok(MmrOutcomeOverride::WinningSnake(snake_id)),
        GameStatus::Complete {
            winning_snake_id: None,
        } => Ok(MmrOutcomeOverride::NoContest),
        _ => Err(anyhow!(
            "game marked completed by inactivity is not terminal"
        )),
    }
}

fn user_id_for_snake(game_state: &GameState, snake_id: u32) -> Result<u32> {
    let mut matching_users = game_state
        .players
        .iter()
        .filter_map(|(user_id, player)| (player.snake_id == snake_id).then_some(*user_id));
    let user_id = matching_users
        .next()
        .ok_or_else(|| anyhow!("winning snake {snake_id} has no player"))?;
    if matching_users.next().is_some() {
        return Err(anyhow!("winning snake {snake_id} has multiple players"));
    }
    Ok(user_id)
}

fn winning_team_for_mmr(game_state: &GameState) -> Result<Option<TeamId>> {
    match mmr_outcome_override(game_state)? {
        MmrOutcomeOverride::ScoreBased => {
            let team_scores = game_state
                .team_scores
                .as_ref()
                .ok_or_else(|| anyhow!("Team scores missing for team match"))?;
            Ok(unique_winning_team(team_scores))
        }
        MmrOutcomeOverride::NoContest => Ok(None),
        MmrOutcomeOverride::WinningSnake(snake_id) => {
            // Also prove the winner belongs to the persisted player roster;
            // indexing the arena alone would accept an orphaned snake.
            user_id_for_snake(game_state, snake_id)?;
            game_state
                .arena
                .snakes
                .get(snake_id as usize)
                .ok_or_else(|| anyhow!("winning snake {snake_id} is missing"))?
                .team_id
                .map(Some)
                .ok_or_else(|| anyhow!("winning snake {snake_id} has no team"))
        }
    }
}

/// Calculate immutable MMR effects without applying them. Completion recovery
/// calls this before committing its authoritative record, ensuring a retry
/// cannot derive different deltas from later ratings.
pub async fn calculate_mmr_effect_specs(
    db: &dyn Database,
    game_state: &GameState,
) -> Result<Vec<MmrEffectSpec>> {
    if matches!(game_state.game_type, GameType::Solo) {
        return Ok(Vec::new());
    }

    // When every player expires in the same authoritative tick there is no
    // winner and therefore no rating event. Return before reading mutable MMR
    // state so completion retries preserve the no-contest decision exactly.
    if mmr_outcome_override(game_state)? == MmrOutcomeOverride::NoContest {
        return Ok(Vec::new());
    }

    let (deltas, winners) = match &game_state.game_type {
        GameType::TeamMatch { per_team } => (
            calculate_team_match_mmr_deltas(db, game_state, *per_team).await?,
            get_team_match_winners(game_state)?,
        ),
        GameType::FreeForAll { .. } => (
            calculate_ffa_mmr_deltas(db, game_state).await?,
            get_ffa_winners(game_state)?,
        ),
        GameType::Custom { .. } if game_state.team_scores.is_some() => (
            calculate_team_match_mmr_deltas(db, game_state, 1).await?,
            get_team_match_winners(game_state)?,
        ),
        GameType::Custom { .. } => (
            calculate_ffa_mmr_deltas(db, game_state).await?,
            get_ffa_winners(game_state)?,
        ),
        GameType::Solo => unreachable!("solo returned above"),
    };

    Ok(deltas
        .into_iter()
        .map(|(user_id, delta)| MmrEffectSpec {
            user_id,
            delta,
            won: winners.contains(&user_id),
        })
        .collect())
}

/// Persist MMR changes for all players in a completed game to the database.
/// Uses the Weng-Lin algorithm to calculate new ratings and atomic ADD operations for updates.
/// For Solo games, persists high scores instead of MMR.
///
/// # Arguments
/// * `db` - Database interface
/// * `game_id` - The ID of the completed game
/// * `game_state` - The final game state containing players, scores, and game type
pub async fn persist_player_mmr(
    db: &dyn Database,
    game_id: u32,
    game_state: &GameState,
    season: Season,
) -> Result<()> {
    // Handle Solo games differently - persist high scores instead of MMR
    if matches!(game_state.game_type, GameType::Solo) {
        info!("Persisting high scores for solo game {}", game_id);
        persist_solo_high_scores(db, game_id, game_state, season).await?;
        return Ok(());
    }

    if mmr_outcome_override(game_state)? == MmrOutcomeOverride::NoContest {
        info!("Skipping MMR for all-idle no-contest game {}", game_id);
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

    // Calculate MMR deltas and determine winners based on game type
    let (mmr_deltas, winners) = match &game_state.game_type {
        GameType::TeamMatch { per_team } => {
            let deltas = calculate_team_match_mmr_deltas(db, game_state, *per_team).await?;
            let winners = get_team_match_winners(game_state)?;
            (deltas, winners)
        }
        GameType::FreeForAll { .. } => {
            let deltas = calculate_ffa_mmr_deltas(db, game_state).await?;
            let winners = get_ffa_winners(game_state)?;
            (deltas, winners)
        }
        GameType::Custom { .. } => {
            // For custom games, determine if it's team-based or FFA
            if game_state.team_scores.is_some() {
                // Custom team game
                let deltas = calculate_team_match_mmr_deltas(db, game_state, 1).await?;
                let winners = get_team_match_winners(game_state)?;
                (deltas, winners)
            } else {
                // Custom FFA game
                let deltas = calculate_ffa_mmr_deltas(db, game_state).await?;
                let winners = get_ffa_winners(game_state)?;
                (deltas, winners)
            }
        }
        GameType::Solo => return Ok(()), // Already handled above
    };

    // Apply MMR deltas to database and update rankings
    apply_mmr_deltas(
        db,
        game_id,
        &game_state.queue_mode,
        game_state,
        mmr_deltas,
        winners,
        season,
    )
    .await?;

    info!("Finished persisting MMR for game {}", game_id);
    Ok(())
}

/// Calculate MMR deltas for team-based matches (1v1, 2v2, etc.)
async fn calculate_team_match_mmr_deltas(
    db: &dyn Database,
    game_state: &GameState,
    _per_team: u8,
) -> Result<HashMap<u32, i32>> {
    // Inactivity forfeits use the authoritative terminal winner; ordinary
    // matches continue to use the score table.
    let winning_team = winning_team_for_mmr(game_state)?;

    // Build team rosters
    let mut team_0_users = Vec::new();
    let mut team_1_users = Vec::new();

    for (user_id, player) in &game_state.players {
        let snake = game_state
            .arena
            .snakes
            .get(player.snake_id as usize)
            .ok_or_else(|| anyhow!("Player {user_id} references a missing snake"))?;
        match snake.team_id {
            Some(TeamId(0)) => team_0_users.push(*user_id),
            Some(TeamId(1)) => team_1_users.push(*user_id),
            _ => warn!("Player {} has invalid team ID in game", user_id),
        }
    }
    team_0_users.sort_unstable();
    team_1_users.sort_unstable();

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
    ensure_all_mmr_users_exist(&mmr_map, &all_users)?;

    // Extract MMRs based on queue mode
    let get_mmr = |user_id: u32| -> i32 {
        mmr_map
            .get(&(user_id as i32))
            .map(|(ranked, casual)| match game_state.queue_mode {
                QueueMode::Competitive => *ranked,
                QueueMode::Quickmatch => *casual,
            })
            .expect("all participant MMRs checked above")
    };

    // Create Weng-Lin ratings
    let team_0_ratings: Vec<WengLinRating> = team_0_users
        .iter()
        .map(|&user_id| {
            let mmr = get_mmr(user_id);
            WengLinRating {
                rating: mmr as f64,
                uncertainty: 350.0,
            }
        })
        .collect();

    let team_1_ratings: Vec<WengLinRating> = team_1_users
        .iter()
        .map(|&user_id| {
            let mmr = get_mmr(user_id);
            WengLinRating {
                rating: mmr as f64,
                uncertainty: 350.0,
            }
        })
        .collect();

    // Determine outcome
    let outcome = match winning_team {
        Some(TeamId(0)) => Outcomes::WIN,
        Some(TeamId(1)) => Outcomes::LOSS,
        None => Outcomes::DRAW, // Tie or no winner
        Some(team) => {
            return Err(anyhow!(
                "Cannot calculate two-team MMR for winning team {}",
                team.0
            ));
        }
    };

    // Calculate new ratings
    let config = WengLinConfig::new();
    let (new_team_0, new_team_1) =
        weng_lin_two_teams(&team_0_ratings, &team_1_ratings, &outcome, &config);

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
    suppress_idle_rating_gains(game_state, &mut deltas);

    info!(
        "Team match: Team 0 ({}), Team 1 ({}) - Winner: {:?}",
        team_0_users.len(),
        team_1_users.len(),
        winning_team
    );

    Ok(deltas)
}

fn ensure_all_mmr_users_exist(mmr_map: &HashMap<i32, (i32, i32)>, user_ids: &[i32]) -> Result<()> {
    if let Some(missing) = user_ids
        .iter()
        .find(|user_id| !mmr_map.contains_key(user_id))
    {
        Err(anyhow!("Cannot materialize MMR for missing user {missing}"))
    } else {
        Ok(())
    }
}

fn unique_winning_team(team_scores: &HashMap<TeamId, u32>) -> Option<TeamId> {
    let max_score = team_scores.values().copied().max()?;
    let mut leaders = team_scores
        .iter()
        .filter_map(|(team, score)| (*score == max_score).then_some(*team));
    let winner = leaders.next()?;
    leaders.next().is_none().then_some(winner)
}

/// Removal for inactivity can never become a rating reward, even if the
/// remaining teammates later win. Losses still apply, preserving the cost of
/// abandoning a match without creating a positive AFK outcome.
fn suppress_idle_rating_gains(game_state: &GameState, deltas: &mut HashMap<u32, i32>) {
    for user_id in &game_state.idle_kicked_user_ids {
        if let Some(delta) = deltas.get_mut(user_id) {
            *delta = (*delta).min(0);
        }
    }
}

/// Return FFA participants in rating order. An inactivity survivor is forced
/// into first place and removed players sort behind every active finisher;
/// players within each group retain their ordinary score order.
/// The boolean records whether first place came from the terminal status so a
/// tied score cannot turn the forced win back into a draw.
fn ordered_ffa_player_scores(game_state: &GameState) -> Result<(Vec<(u32, u32)>, bool)> {
    let forced_winner = match mmr_outcome_override(game_state)? {
        MmrOutcomeOverride::ScoreBased => None,
        MmrOutcomeOverride::NoContest => return Ok((Vec::new(), false)),
        MmrOutcomeOverride::WinningSnake(snake_id) => {
            Some(user_id_for_snake(game_state, snake_id)?)
        }
    };

    let mut player_scores: Vec<(u32, u32)> = game_state
        .players
        .iter()
        .map(|(user_id, player)| {
            let score = game_state
                .scores
                .get(&player.snake_id)
                .copied()
                .unwrap_or(0);
            (*user_id, score)
        })
        .collect();

    player_scores.sort_by(|left, right| {
        let left_is_winner = forced_winner == Some(left.0);
        let right_is_winner = forced_winner == Some(right.0);
        let left_was_idle_kicked = game_state.is_player_idle_kicked(left.0);
        let right_was_idle_kicked = game_state.is_player_idle_kicked(right.0);
        right_is_winner
            .cmp(&left_is_winner)
            .then_with(|| left_was_idle_kicked.cmp(&right_was_idle_kicked))
            .then_with(|| right.1.cmp(&left.1))
            .then_with(|| left.0.cmp(&right.0))
    });

    Ok((player_scores, forced_winner.is_some()))
}

/// Calculate MMR deltas for free-for-all matches
async fn calculate_ffa_mmr_deltas(
    db: &dyn Database,
    game_state: &GameState,
) -> Result<HashMap<u32, i32>> {
    let (player_scores, forced_winner) = ordered_ffa_player_scores(game_state)?;

    // Get current MMRs
    let all_users: Vec<i32> = player_scores.iter().map(|(id, _)| *id as i32).collect();
    let mmr_map = db.get_user_mmrs(&all_users).await?;
    ensure_all_mmr_users_exist(&mmr_map, &all_users)?;

    // Extract MMRs based on queue mode
    let get_mmr = |user_id: u32| -> i32 {
        mmr_map
            .get(&(user_id as i32))
            .map(|(ranked, casual)| match game_state.queue_mode {
                QueueMode::Competitive => *ranked,
                QueueMode::Quickmatch => *casual,
            })
            .expect("all participant MMRs checked above")
    };

    // If only 2 players, use 1v1 algorithm
    if player_scores.len() == 2 {
        let user_0 = player_scores[0].0;
        let user_1 = player_scores[1].0;

        let rating_0 = WengLinRating {
            rating: get_mmr(user_0) as f64,
            uncertainty: 350.0,
        };
        let rating_1 = WengLinRating {
            rating: get_mmr(user_1) as f64,
            uncertainty: 350.0,
        };

        let outcome = if !forced_winner && player_scores[0].1 == player_scores[1].1 {
            Outcomes::DRAW
        } else {
            Outcomes::WIN
        };
        let config = WengLinConfig::new();
        let (new_rating_0, new_rating_1) = weng_lin(&rating_0, &rating_1, &outcome, &config);

        let mut deltas = HashMap::new();
        deltas.insert(user_0, new_rating_0.rating as i32 - get_mmr(user_0));
        deltas.insert(user_1, new_rating_1.rating as i32 - get_mmr(user_1));
        suppress_idle_rating_gains(game_state, &mut deltas);

        return Ok(deltas);
    }

    // For 3+ players, use multi-team algorithm (each player is their own team)
    let teams_with_ratings: Vec<Vec<WengLinRating>> = player_scores
        .iter()
        .map(|(user_id, _)| {
            vec![WengLinRating {
                rating: get_mmr(*user_id) as f64,
                uncertainty: 350.0,
            }]
        })
        .collect();

    // Convert ranks to MultiTeamOutcome (lower rank = better placement)
    let mut rank = 1;
    let teams_with_outcomes: Vec<(&[WengLinRating], MultiTeamOutcome)> = teams_with_ratings
        .iter()
        .enumerate()
        .map(|(index, team)| {
            if index > 0 {
                if forced_winner && index == 1 {
                    // The terminal winner is always alone in first even when
                    // a kicked player has the same or a higher score.
                    rank = 2;
                } else {
                    // Two independent reasons to open a new placement tier:
                    // crossing the boundary between removed and remaining
                    // players, or a genuine score drop. Players who are on the
                    // same side of that boundary and scored the same tie.
                    let crosses_removal_boundary = game_state
                        .is_player_idle_kicked(player_scores[index].0)
                        != game_state.is_player_idle_kicked(player_scores[index - 1].0);
                    let scored_less = player_scores[index].1 < player_scores[index - 1].1;
                    if crosses_removal_boundary || scored_less {
                        rank = index + 1;
                    }
                }
            }
            (team.as_slice(), MultiTeamOutcome::new(rank))
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
        player_scores
            .iter()
            .map(|(id, score)| (*id, *score))
            .collect::<Vec<_>>()
    );

    suppress_idle_rating_gains(game_state, &mut deltas);
    Ok(deltas)
}

/// Apply calculated MMR deltas to the database using atomic operations
/// Also updates the rankings table for leaderboards
async fn apply_mmr_deltas(
    db: &dyn Database,
    game_id: u32,
    queue_mode: &QueueMode,
    game_state: &GameState,
    deltas: HashMap<u32, i32>,
    winners: HashSet<u32>,
    season: Season,
) -> Result<()> {
    let region = get_region();

    for (user_id, delta) in deltas {
        if delta == 0 {
            info!("User {} MMR unchanged in game {}", user_id, game_id);
            continue;
        }

        // Update user MMR
        let new_mmr = match db
            .update_user_mmr_by_mode(user_id as i32, delta, queue_mode)
            .await
        {
            Ok(new_total) => {
                let sign = if delta > 0 { "+" } else { "" };
                info!(
                    "User {} {:?} MMR: {}{} (new total: {}) from game {}",
                    user_id, queue_mode, sign, delta, new_total, game_id
                );
                new_total
            }
            Err(e) => {
                error!(
                    "Failed to update MMR for user {} in game {}: {:?}",
                    user_id, game_id, e
                );
                continue; // Skip ranking update if MMR update failed
            }
        };

        // Update ranking
        let username = game_state
            .usernames
            .get(&user_id)
            .cloned()
            .unwrap_or_else(|| format!("User{}", user_id));

        let won = winners.contains(&user_id);

        match db
            .upsert_ranking(
                user_id as i32,
                &username,
                new_mmr,
                queue_mode,
                &game_state.game_type,
                &region,
                season,
                won,
            )
            .await
        {
            Ok(_) => {
                info!(
                    "Updated ranking for user {} in {} {} (season: {})",
                    user_id,
                    match queue_mode {
                        QueueMode::Competitive => "ranked",
                        QueueMode::Quickmatch => "casual",
                    },
                    region,
                    season
                );
            }
            Err(e) => {
                error!(
                    "Failed to update ranking for user {} in game {}: {:?}",
                    user_id, game_id, e
                );
                // Don't fail the whole operation if ranking update fails
            }
        }
    }

    Ok(())
}

/// Get winners for team-based matches
fn get_team_match_winners(game_state: &GameState) -> Result<HashSet<u32>> {
    let mut winners = HashSet::new();

    if let Some(winning_team) = winning_team_for_mmr(game_state)? {
        // Add all players from the winning team
        for (user_id, player) in &game_state.players {
            let Some(snake) = game_state.arena.snakes.get(player.snake_id as usize) else {
                continue;
            };
            if snake.team_id == Some(winning_team) && !game_state.is_player_idle_kicked(*user_id) {
                winners.insert(*user_id);
            }
        }
    }

    Ok(winners)
}

/// Get winners for FFA matches (top player or tied for first)
fn get_ffa_winners(game_state: &GameState) -> Result<HashSet<u32>> {
    let mut winners = HashSet::new();

    match mmr_outcome_override(game_state)? {
        MmrOutcomeOverride::NoContest => return Ok(winners),
        MmrOutcomeOverride::WinningSnake(snake_id) => {
            winners.insert(user_id_for_snake(game_state, snake_id)?);
            return Ok(winners);
        }
        MmrOutcomeOverride::ScoreBased => {}
    }

    // Get all player scores
    let player_scores: Vec<(u32, u32)> = game_state
        .players
        .iter()
        .filter(|(user_id, _)| !game_state.is_player_idle_kicked(**user_id))
        .map(|(user_id, player)| {
            let score = game_state
                .scores
                .get(&player.snake_id)
                .copied()
                .unwrap_or(0);
            (*user_id, score)
        })
        .collect();

    if player_scores.is_empty() {
        return Ok(winners);
    }

    // Find max score
    let max_score = player_scores
        .iter()
        .map(|(_, score)| *score)
        .max()
        .unwrap_or(0);

    // Add all players with max score (handles ties)
    for (user_id, score) in player_scores {
        if score == max_score {
            winners.insert(user_id);
        }
    }

    Ok(winners)
}

/// Persist high scores for solo games
async fn persist_solo_high_scores(
    db: &dyn Database,
    game_id: u32,
    game_state: &GameState,
    season: Season,
) -> Result<()> {
    let region = get_region();

    info!(
        "Persisting high scores for solo game {} with {} players (season: {}, region: {})",
        game_id,
        game_state.players.len(),
        season,
        region
    );

    // For each player, insert their high score
    for (user_id, player) in &game_state.players {
        let score = game_state
            .scores
            .get(&player.snake_id)
            .copied()
            .unwrap_or(0);
        let username = game_state
            .usernames
            .get(user_id)
            .cloned()
            .unwrap_or_else(|| format!("User{}", user_id));

        debug!(
            "Processing high score for user {} ({}): score={}, snake_id={}",
            user_id, username, score, player.snake_id
        );

        match db
            .insert_high_score(
                &game_id.to_string(),
                *user_id as i32,
                &username,
                score as i32,
                &game_state.game_type,
                &region,
                season,
            )
            .await
        {
            Ok(_) => {
                info!(
                    "Inserted high score for user {} (score: {}) in solo game {} (season: {})",
                    user_id, score, game_id, season
                );
            }
            Err(e) => {
                error!(
                    "Failed to insert high score for user {} in game {}: {:?}",
                    user_id, game_id, e
                );
                // Don't fail the whole operation if one high score insert fails
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        MmrOutcomeOverride, get_ffa_winners, get_team_match_winners, mmr_outcome_override,
        ordered_ffa_player_scores, suppress_idle_rating_gains, winning_team_for_mmr,
    };
    use common::{GameState, GameStatus, GameType, QueueMode, TeamId};
    use std::collections::{HashMap, HashSet};

    fn team_state() -> GameState {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            Some(7),
            0,
        );
        state
            .add_player(11, Some("eleven".into()))
            .expect("team zero player should be added");
        state
            .add_player(22, Some("twenty-two".into()))
            .expect("team one player should be added");
        state
    }

    fn ffa_state() -> GameState {
        let mut state = GameState::new(
            60,
            40,
            GameType::FreeForAll { max_players: 3 },
            QueueMode::Competitive,
            Some(9),
            0,
        );
        for user_id in [11, 22, 33] {
            state
                .add_player(user_id, Some(user_id.to_string()))
                .expect("FFA player should be added");
        }
        state
    }

    #[test]
    fn team_inactivity_forfeit_prefers_terminal_winner_over_scores() {
        let mut state = team_state();
        let winner_snake_id = state.players[&11].snake_id;
        let team_scores = state
            .team_scores
            .as_mut()
            .expect("team match should have team scores");
        team_scores.insert(TeamId(0), 0);
        team_scores.insert(TeamId(1), 50);
        state.completed_by_inactivity = true;
        state.idle_kicked_user_ids = vec![22];
        state.status = GameStatus::Complete {
            winning_snake_id: Some(winner_snake_id),
        };

        assert_eq!(winning_team_for_mmr(&state).unwrap(), Some(TeamId(0)));
        assert_eq!(get_team_match_winners(&state).unwrap(), HashSet::from([11]));
    }

    #[test]
    fn kicked_teammate_is_not_marked_as_a_winner() {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 2 },
            QueueMode::Quickmatch,
            Some(8),
            0,
        );
        for user_id in [11, 22, 33, 44] {
            state
                .add_player(user_id, Some(user_id.to_string()))
                .expect("team player should be added");
        }
        state.completed_by_inactivity = true;
        state.idle_kicked_user_ids = vec![22, 33, 44];
        state.status = GameStatus::Complete {
            winning_snake_id: Some(state.players[&11].snake_id),
        };

        assert_eq!(get_team_match_winners(&state).unwrap(), HashSet::from([11]));
    }

    #[test]
    fn ffa_inactivity_forfeit_forces_terminal_winner_ahead_of_scores() {
        let mut state = ffa_state();
        let winner_snake_id = state.players[&22].snake_id;
        state.scores.insert(state.players[&11].snake_id, 100);
        state.scores.insert(winner_snake_id, 0);
        state.scores.insert(state.players[&33].snake_id, 50);
        state.completed_by_inactivity = true;
        state.idle_kicked_user_ids = vec![11, 33];
        state.status = GameStatus::Complete {
            winning_snake_id: Some(winner_snake_id),
        };

        let (ordered, forced_winner) = ordered_ffa_player_scores(&state).unwrap();
        assert!(forced_winner);
        assert_eq!(
            ordered
                .into_iter()
                .map(|(user_id, _)| user_id)
                .collect::<Vec<_>>(),
            vec![22, 11, 33]
        );
        assert_eq!(get_ffa_winners(&state).unwrap(), HashSet::from([22]));
    }

    #[test]
    fn simultaneous_all_idle_completion_is_no_contest() {
        let mut state = team_state();
        state.completed_by_inactivity = true;
        state.idle_kicked_user_ids = vec![11, 22];
        state.status = GameStatus::Complete {
            winning_snake_id: None,
        };

        assert_eq!(
            mmr_outcome_override(&state).unwrap(),
            MmrOutcomeOverride::NoContest
        );
        assert!(get_team_match_winners(&state).unwrap().is_empty());
    }

    #[test]
    fn ordinary_completion_remains_score_based() {
        let mut state = team_state();
        let terminal_snake_id = state.players[&11].snake_id;
        let team_scores = state
            .team_scores
            .as_mut()
            .expect("team match should have team scores");
        team_scores.insert(TeamId(0), 1);
        team_scores.insert(TeamId(1), 2);
        state.status = GameStatus::Complete {
            winning_snake_id: Some(terminal_snake_id),
        };

        assert_eq!(
            mmr_outcome_override(&state).unwrap(),
            MmrOutcomeOverride::ScoreBased
        );
        assert_eq!(winning_team_for_mmr(&state).unwrap(), Some(TeamId(1)));
        assert_eq!(get_team_match_winners(&state).unwrap(), HashSet::from([22]));
    }

    #[test]
    fn ordinary_ffa_completion_places_removed_players_last() {
        let mut state = ffa_state();
        state.scores.insert(state.players[&11].snake_id, 100);
        state.scores.insert(state.players[&22].snake_id, 25);
        state.scores.insert(state.players[&33].snake_id, 50);
        state.idle_kicked_user_ids = vec![11];

        let (ordered, forced_winner) = ordered_ffa_player_scores(&state).unwrap();
        assert!(!forced_winner);
        assert_eq!(
            ordered
                .into_iter()
                .map(|(user_id, _)| user_id)
                .collect::<Vec<_>>(),
            vec![33, 22, 11]
        );
        assert_eq!(get_ffa_winners(&state).unwrap(), HashSet::from([33]));
    }

    #[test]
    fn removed_players_can_lose_rating_but_never_gain_it() {
        let mut state = team_state();
        state.idle_kicked_user_ids = vec![11];
        let mut positive = HashMap::from([(11, 18), (22, 18)]);
        suppress_idle_rating_gains(&state, &mut positive);
        assert_eq!(positive, HashMap::from([(11, 0), (22, 18)]));

        let mut negative = HashMap::from([(11, -18), (22, 18)]);
        suppress_idle_rating_gains(&state, &mut negative);
        assert_eq!(negative, HashMap::from([(11, -18), (22, 18)]));
    }
}
