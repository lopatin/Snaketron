use crate::{Direction, GameCommand, GameState, GameType, Position, Snake, TeamId};
use std::collections::{HashSet, VecDeque};

/// Determines if a user_id represents an AI player
pub fn is_ai_player(user_id: u32) -> bool {
    // AI players have user_ids starting from u32::MAX and counting down
    user_id >= u32::MAX - 10
}

/// Basic AI that tries to reach food while avoiding obstacles
/// In team games, also decides when to return to base to score points
pub fn calculate_ai_move(
    game_state: &GameState,
    snake_id: u32,
    current_direction: Direction,
) -> Option<Direction> {
    let snake = game_state.arena.snakes.get(snake_id as usize)?;
    if !snake.is_alive {
        return None;
    }

    let head = snake.head().ok()?;
    let arena_width = game_state.arena.width as i16;
    let arena_height = game_state.arena.height as i16;

    // Get all possible directions (always excluding opposite of current direction)
    let mut possible_directions = vec![
        Direction::Up,
        Direction::Down,
        Direction::Left,
        Direction::Right,
    ];

    // Always remove opposite direction to prevent 180-degree turns
    possible_directions.retain(|&d| !current_direction.is_opposite(&d));

    // Determine target based on game mode and strategy
    let target = if is_team_game(game_state)
        && let Some(team_id) = snake.team_id
    {
        // Decide whether to return to base or collect more food
        if should_return_to_base(game_state, snake, team_id) {
            // RETURN_TO_BASE mode: Navigate to goal
            find_path_to_base(game_state, head, team_id, arena_width, arena_height)?
        } else {
            // COLLECT_FOOD mode: Find nearest food
            find_nearest_food(game_state, head, arena_width, arena_height)?
        }
    } else {
        // Non-team game: always collect food
        find_nearest_food(game_state, head, arena_width, arena_height)?
    };

    // Use the determined target (replaces nearest_food in original code)
    let nearest_food = target;

    // Score each direction
    let mut best_direction = current_direction;
    let mut best_score = i32::MIN;

    for direction in possible_directions {
        let new_pos = get_new_position(head, direction);

        // Skip if out of bounds
        if !is_within_bounds(&new_pos, arena_width, arena_height) {
            continue;
        }

        // Skip if collision with any snake
        if would_collide_with_snake(game_state, &new_pos) {
            continue;
        }

        // Calculate score for this direction
        let mut score = 0;

        // Prefer moving toward food
        let food_distance = manhattan_distance(&new_pos, &nearest_food);
        score -= food_distance as i32 * 10;

        // Prefer staying in center of arena
        let center_x = arena_width / 2;
        let center_y = arena_height / 2;
        let center_distance = manhattan_distance(
            &new_pos,
            &Position {
                x: center_x,
                y: center_y,
            },
        );
        score -= center_distance as i32;

        // Check if this move would trap us (simple lookahead)
        let escape_routes = count_escape_routes(game_state, &new_pos, arena_width, arena_height);
        if escape_routes == 0 {
            continue; // Skip moves that would trap us
        }
        score += escape_routes as i32 * 5;

        if score > best_score {
            best_score = score;
            best_direction = direction;
        }
    }

    // If no safe move found, keep current direction
    if best_score == i32::MIN {
        Some(current_direction)
    } else {
        Some(best_direction)
    }
}

/// Choose the command issued by the production bot for one decision cycle.
///
/// Keeping this policy beside [`calculate_ai_move`] gives offline simulations
/// the exact same Boost-first behavior as the networked bot binary. The
/// caller owns cadence and transport identity, just as the live bot does.
pub fn calculate_ai_command(game_state: &GameState, snake_id: u32) -> Option<GameCommand> {
    let snake = game_state.arena.snakes.get(snake_id as usize)?;
    if !snake.is_alive {
        return None;
    }
    if game_state.properties.boost.is_some() && snake.boost().charge_ms > 0 && !snake.boost().active
    {
        return Some(GameCommand::ActivateBoost { snake_id });
    }

    let direction =
        calculate_ai_move(game_state, snake_id, snake.direction).unwrap_or(snake.direction);
    Some(GameCommand::Turn {
        snake_id,
        direction,
    })
}

fn get_new_position(pos: &Position, direction: Direction) -> Position {
    match direction {
        Direction::Up => Position {
            x: pos.x,
            y: pos.y - 1,
        },
        Direction::Down => Position {
            x: pos.x,
            y: pos.y + 1,
        },
        Direction::Left => Position {
            x: pos.x - 1,
            y: pos.y,
        },
        Direction::Right => Position {
            x: pos.x + 1,
            y: pos.y,
        },
    }
}

fn is_within_bounds(pos: &Position, width: i16, height: i16) -> bool {
    pos.x >= 0 && pos.x < width && pos.y >= 0 && pos.y < height
}

fn would_collide_with_snake(game_state: &GameState, pos: &Position) -> bool {
    for snake in &game_state.arena.snakes {
        if snake.is_alive && snake.contains_point(pos, false) {
            return true;
        }
    }
    false
}

fn manhattan_distance(p1: &Position, p2: &Position) -> u16 {
    ((p1.x - p2.x).abs() + (p1.y - p2.y).abs()) as u16
}

fn find_nearest_food(
    game_state: &GameState,
    start: &Position,
    width: i16,
    height: i16,
) -> Option<Position> {
    if game_state.arena.food.is_empty() {
        // If no food, return center of arena as target
        return Some(Position {
            x: width / 2,
            y: height / 2,
        });
    }

    // Find nearest food using BFS
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    queue.push_back(*start);
    visited.insert(*start);

    while let Some(pos) = queue.pop_front() {
        // Check if this position has food
        if game_state.arena.food.contains(&pos) {
            return Some(pos);
        }

        // Add neighbors to queue
        for direction in &[
            Direction::Up,
            Direction::Down,
            Direction::Left,
            Direction::Right,
        ] {
            let new_pos = get_new_position(&pos, *direction);

            if is_within_bounds(&new_pos, width, height) && !visited.contains(&new_pos) {
                visited.insert(new_pos);
                queue.push_back(new_pos);
            }
        }
    }

    // Fallback to first food item
    game_state.arena.food.first().copied()
}

fn count_escape_routes(game_state: &GameState, pos: &Position, width: i16, height: i16) -> u8 {
    let mut count = 0;

    for direction in &[
        Direction::Up,
        Direction::Down,
        Direction::Left,
        Direction::Right,
    ] {
        let new_pos = get_new_position(pos, *direction);

        if is_within_bounds(&new_pos, width, height)
            && !would_collide_with_snake(game_state, &new_pos)
        {
            count += 1;
        }
    }

    count
}

// ============================================================================
// TEAM GAME AI HELPERS
// ============================================================================

/// Check if the current game is a team match
fn is_team_game(game_state: &GameState) -> bool {
    matches!(game_state.game_type, GameType::TeamMatch { .. })
}

/// A team is this close to the target before its next bank is treated as
/// match-winning by opponents deciding whether to cash in what they carry.
const MATCH_POINT_MARGIN: u32 = 3;

/// Urgency from the score race.
///
/// Team matches have no clock, so endgame pressure comes from the standings
/// rather than from time remaining: banking is worth everything when it wins
/// the match outright, and nearly as much when the other side is one bank from
/// winning and carried points are about to become worthless.
fn score_race_pressure(game_state: &GameState, team_id: TeamId, carried_points: u32) -> i32 {
    let Some(score_limit) = game_state.properties.score_limit else {
        return 0;
    };
    let Some(team_scores) = game_state.team_scores.as_ref() else {
        return 0;
    };

    let our_score = team_scores.get(&team_id).copied().unwrap_or(0);
    if our_score + carried_points >= score_limit {
        return 100; // Banking ends the match in our favour right now.
    }

    let best_opponent = team_scores
        .iter()
        .filter(|(other, _)| **other != team_id)
        .map(|(_, score)| *score)
        .max()
        .unwrap_or(0);
    if best_opponent + MATCH_POINT_MARGIN >= score_limit {
        return 100; // Cash in before the match can end without these points.
    }

    0
}

/// Get the score differential for this team (positive = winning, negative = losing)
fn get_score_differential(game_state: &GameState, team_id: TeamId) -> i32 {
    if let Some(ref team_scores) = game_state.team_scores {
        let our_score = *team_scores.get(&team_id).unwrap_or(&0) as i32;

        // Find opponent's score (there are only 2 teams: 0 and 1)
        let opponent_team_id = if team_id.0 == 0 { TeamId(1) } else { TeamId(0) };
        let opponent_score = *team_scores.get(&opponent_team_id).unwrap_or(&0) as i32;

        our_score - opponent_score
    } else {
        0 // No team scores available, treat as neutral
    }
}

/// Find the path to the team's base goal using BFS
/// Returns the next position to move toward to reach the goal
fn find_path_to_base(
    game_state: &GameState,
    start: &Position,
    team_id: TeamId,
    width: i16,
    height: i16,
) -> Option<Position> {
    // Get the goal bounds for our team
    let (goal_x, goal_y_start, goal_y_end) = game_state.arena.goal_bounds(team_id)?;

    // Find the nearest point in the goal opening using BFS
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    queue.push_back(*start);
    visited.insert(*start);

    while let Some(pos) = queue.pop_front() {
        // Check if this position is in the goal opening
        if pos.x == goal_x && pos.y >= goal_y_start && pos.y <= goal_y_end {
            return Some(pos);
        }

        // Add neighbors to queue
        for direction in &[
            Direction::Up,
            Direction::Down,
            Direction::Left,
            Direction::Right,
        ] {
            let new_pos = get_new_position(&pos, *direction);

            if is_within_bounds(&new_pos, width, height) && !visited.contains(&new_pos) {
                visited.insert(new_pos);
                queue.push_back(new_pos);
            }
        }
    }

    // Fallback: return center of goal opening
    Some(Position {
        x: goal_x,
        y: (goal_y_start + goal_y_end) / 2,
    })
}

/// Determine if the snake should return to base to score points
fn should_return_to_base(game_state: &GameState, snake: &Snake, team_id: TeamId) -> bool {
    let carried_points = game_state.carried_food(snake);

    // Don't return if not carrying any points
    if carried_points == 0 {
        return false;
    }

    // Check if already in own base - if so, exit to collect more food
    if let Ok(head) = snake.head()
        && game_state.arena.is_in_team_base(head, team_id)
    {
        return false; // Already home, go back out
    }

    // Calculate return score based on multiple factors
    let base_food_score = carried_points * 10;

    // Race pressure: strong incentive when this bank wins, or when the other
    // side is about to win and unbanked points are about to be lost.
    let race_pressure_score = score_race_pressure(game_state, team_id, carried_points);

    // Score differential: take more risk if losing badly
    let score_diff = get_score_differential(game_state, team_id);
    let score_differential_bonus = if score_diff < -10 {
        -20 // Lower threshold when losing (stay greedy)
    } else {
        0
    };

    let total_return_score =
        base_food_score as i32 + race_pressure_score + score_differential_bonus;

    // Decision threshold: >= 40 means return to base
    total_return_score >= 40
}
