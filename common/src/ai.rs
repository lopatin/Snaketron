use crate::{Direction, GameState, Position, Snake};
use std::collections::{HashSet, VecDeque};

/// Determines if a user_id represents an AI player
pub fn is_ai_player(user_id: u32) -> bool {
    // AI players have user_ids starting from u32::MAX and counting down
    user_id >= u32::MAX - 10
}

/// Basic AI that tries to reach food while avoiding obstacles
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
    
    // Find the nearest food using BFS
    let nearest_food = find_nearest_food(game_state, head, arena_width, arena_height)?;
    
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
        let center_distance = manhattan_distance(&new_pos, &Position { x: center_x, y: center_y });
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

fn get_new_position(pos: &Position, direction: Direction) -> Position {
    match direction {
        Direction::Up => Position { x: pos.x, y: pos.y - 1 },
        Direction::Down => Position { x: pos.x, y: pos.y + 1 },
        Direction::Left => Position { x: pos.x - 1, y: pos.y },
        Direction::Right => Position { x: pos.x + 1, y: pos.y },
    }
}

fn is_within_bounds(pos: &Position, width: i16, height: i16) -> bool {
    pos.x >= 0 && pos.x < width && pos.y >= 0 && pos.y < height
}

fn would_collide_with_snake(game_state: &GameState, pos: &Position) -> bool {
    for snake in &game_state.arena.snakes {
        if snake.is_alive && snake.contains_point(pos) {
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
        return Some(Position { x: width / 2, y: height / 2 });
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
        for direction in &[Direction::Up, Direction::Down, Direction::Left, Direction::Right] {
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

fn count_escape_routes(
    game_state: &GameState,
    pos: &Position,
    width: i16,
    height: i16,
) -> u8 {
    let mut count = 0;
    
    for direction in &[Direction::Up, Direction::Down, Direction::Left, Direction::Right] {
        let new_pos = get_new_position(pos, *direction);
        
        if is_within_bounds(&new_pos, width, height) && 
           !would_collide_with_snake(game_state, &new_pos) {
            count += 1;
        }
    }
    
    count
}