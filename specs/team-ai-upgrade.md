# Team Game AI Upgrade - Implementation Plan

## Overview
Upgrade the bot AI to make intelligent decisions about when to return to base in team games, implementing a dynamic strategy that adapts to game state.

## User Requirements
- **Strategy**: Dynamic - adapt based on game state (time remaining, score differential, food carried)
- **Time Management**: Rush home when time is running low (<20 seconds)
- **Danger Response**: Stay greedy - only avoid immediate death, don't preemptively return due to nearby enemies

## Current State Analysis

### Existing AI Implementation (`common/src/ai.rs`)
- **Location**: `calculate_ai_move()` function (lines 11-94)
- **Current Behavior**:
  - Uses BFS to find nearest food
  - Greedy algorithm - always pursues food
  - No awareness of team bases or scoring mechanics
  - Safety checks: collision avoidance, escape route counting

### Team Game Mechanics
- **Scoring**: Points locked in when snake returns to own base with food
- **Food Value**: Each food = 2 segments, 2 segments = 1 point when scored
- **Respawn**: Snake resets to initial length after scoring
- **Time Limit**: 90 seconds per game
- **Death Penalty**: Lose all carried food on death

## Implementation Plan

### 1. Architecture: Dual-Mode Decision System

Add a high-level decision layer that chooses between two modes:

```
1. Mode Selection (new)
   ├─> COLLECT_FOOD mode → Use existing food-seeking logic
   └─> RETURN_TO_BASE mode → Navigate to home base

2. Direction Selection (existing + enhanced)
   └─> Score directions based on current mode's objective
```

### 2. Decision Logic: When to Return to Base

Implement dynamic threshold calculation based on multiple factors:

#### Formula: Return Score
```rust
return_score = base_food_score + time_pressure_score + score_differential_bonus

base_food_score = carried_food_points * 10
  // More food carried = more incentive to return
  // carried_food_points = (snake.food + extra_segments) / 2

time_pressure_score = if time_remaining < 20s { 100 } else { 0 }
  // Strong incentive to return when <20s left

score_differential_bonus = if losing_badly { -20 } else { 0 }
  // If losing by >10 points, take more risks (lower threshold)

Decision: if return_score >= 40 → RETURN_TO_BASE mode
```

#### Specific Thresholds
- **Early game (>40s remaining)**: Return after 4-6 food (8-12 segments, 4-6 points)
- **Mid game (20-40s remaining)**: Return after 3-4 food (6-8 segments, 3-4 points)
- **Late game (<20s remaining)**: Return with ANY food (prioritize securing points)
- **Losing badly**: Increase thresholds slightly (take more risk)

### 3. Pathfinding to Base

Implement BFS pathfinding to navigate to goal opening:

```rust
fn find_path_to_base(game_state: &GameState, snake_id: u32) -> Option<Position>
```

**Target**: Goal opening of own team's base
- Use `arena.goal_bounds(team_id)` to get goal coordinates
- Navigate to nearest point in goal opening
- Reuse existing BFS implementation pattern from `find_nearest_food()`

### 4. Implementation Steps

#### Step 1: Extend `calculate_ai_move()` Signature
```rust
// OLD
pub fn calculate_ai_move(
    game_state: &GameState,
    snake_id: u32,
    current_direction: Direction
) -> Option<Direction>

// NEW - add start_ms for time calculations
pub fn calculate_ai_move(
    game_state: &GameState,
    snake_id: u32,
    current_direction: Direction,
) -> Option<Direction>
```

Note: `game_state` already contains `start_ms` field, so no signature change needed.

#### Step 2: Add Helper Functions

```rust
// Determine if team game
fn is_team_game(game_state: &GameState) -> bool

// Calculate current time remaining
fn calculate_time_remaining_ms(game_state: &GameState) -> i64

// Calculate points snake is carrying
fn calculate_carried_points(snake: &Snake, starting_length: usize) -> u32

// Determine if should return to base
fn should_return_to_base(
    game_state: &GameState,
    snake: &Snake,
    snake_id: u32,
    starting_length: usize
) -> bool

// Find path to base goal
fn find_path_to_base(
    game_state: &GameState,
    snake_id: u32,
    team_id: TeamId
) -> Option<Position>

// Get team score differential
fn get_score_differential(
    game_state: &GameState,
    team_id: TeamId
) -> i32  // positive = winning, negative = losing
```

#### Step 3: Modify Main AI Logic

```rust
pub fn calculate_ai_move(...) -> Option<Direction> {
    let snake = &game_state.arena.snakes[snake_id as usize];

    // Early exit for non-team games - use existing logic
    if !is_team_game(game_state) || snake.team_id.is_none() {
        return existing_food_seeking_logic(...);
    }

    let team_id = snake.team_id.unwrap();
    let starting_length = game_state.properties.starting_snake_length as usize;

    // Determine target based on mode
    let target = if should_return_to_base(game_state, snake, snake_id, starting_length) {
        // RETURN_TO_BASE mode
        find_path_to_base(game_state, snake_id, team_id)?
    } else {
        // COLLECT_FOOD mode (existing logic)
        find_nearest_food(game_state, head, &game_state.arena.food)
            .unwrap_or(center_position)
    };

    // Score directions toward target (existing logic applies)
    // ... rest of existing direction scoring code ...
}
```

#### Step 4: Update Bot Usage

The bot (`bot/src/main.rs`) already passes all required information:
```rust
calculate_ai_move(predicted_state, snake_id, snake.direction)
```

No changes needed to bot integration.

### 5. Edge Cases to Handle

1. **Already in base with food**: Should exit base to collect more food
   - Check if already in own base before deciding to return

2. **No path to base**: Blocked by walls/snakes
   - Fallback to food collection if can't reach base

3. **Time runs out while returning**:
   - Not preventable, but algorithm already handles this (rush home <20s)

4. **Snake dies on way to base**:
   - Points lost, respawn handled by game engine, AI continues normally

5. **Non-team game types**:
   - Early exit to use existing food-only logic

6. **Negative or zero food carried**:
   - Don't return to base, continue collecting

### 6. Files to Modify

#### Primary File
- **`common/src/ai.rs`** - Main AI logic (all changes here)
  - Add helper functions (7 new functions)
  - Modify `calculate_ai_move()` to support dual-mode logic
  - Reuse existing BFS pattern for base pathfinding

#### No Changes Required
- `bot/src/main.rs` - Bot already passes necessary data
- `common/src/game_state.rs` - All needed helper methods already exist
- `common/src/snake.rs` - No modifications needed

### 7. Testing Strategy

#### Manual Testing
1. Run bot in team match: `cargo run -p bot -- --url https://snaketron.io --mode 2v2`
2. Observe behavior:
   - Collects food initially
   - Returns to base when carrying ~4-6 food
   - Returns immediately when <20s remaining
   - Respawns and continues playing

#### Unit Tests
Add tests in `common/src/ai.rs`:
```rust
#[test]
fn test_should_return_early_game() {
    // Snake with 3 food, 60s remaining → should NOT return
}

#[test]
fn test_should_return_late_game() {
    // Snake with 1 food, 15s remaining → SHOULD return
}

#[test]
fn test_should_return_lots_of_food() {
    // Snake with 6 food, 50s remaining → SHOULD return
}

#[test]
fn test_find_path_to_base() {
    // Verify pathfinding to goal opening works
}
```

### 8. Performance Considerations

- BFS pathfinding runs once per tick (100ms intervals)
- Minimal overhead: single BFS scan (~O(width × height))
- No impact on game server performance (client-side AI only)

### 9. Future Enhancements (Out of Scope)

- Predict enemy snake movements
- Coordinate with teammate
- Steal food near enemy base
- Block enemy snakes from scoring

## Summary

This implementation adds team-aware decision-making to the bot AI while preserving the existing food-seeking logic. The dual-mode system is simple, efficient, and adapts dynamically to game state. All changes are isolated to `common/src/ai.rs`, making it a clean, low-risk upgrade.
