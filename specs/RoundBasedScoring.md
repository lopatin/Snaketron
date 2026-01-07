# Round-Based Team Scoring Implementation

## Overview
Transform the current single-game system into a round-based system where:
- **Quick Match**: Single round (first team to win 1 round wins the match)
- **Competitive Mode**: Best of 3 rounds (first team to win 2 rounds wins the match)

## Implementation Status

### ✅ Completed
- [x] Created implementation plan document
- [x] Add round tracking fields to GameState
- [x] Add GameEvent types for round transitions
- [x] Update apply_event() for new events
- [x] Implement round transition logic in tick_forward
- [x] Update TypeScript types for round tracking
- [x] Update Scoreboard UI to show round wins
- [x] Handle round transitions in GameArena
- [x] Test round-based scoring (compilation and existing tests pass)
- [x] Fix endzone scoring to trigger round completion
- [x] Fix draw handling when both teams lose all snakes simultaneously

---

## Implementation Details

### 1. Add Round Tracking Fields to GameState (Rust)
**File: `common/src/game_state.rs`**

Add new fields to `GameState`:
```rust
pub current_round: u32,                    // Current round number (1, 2, 3...)
pub round_wins: HashMap<TeamId, u32>,      // Rounds won by each team
pub rounds_to_win: u32,                    // 1 for quick, 2 for competitive
pub round_start_times: Vec<i64>,          // Start time of each round (ms)
pub is_transitioning: bool,                // True during round transitions
```

### 2. Add GameEvent Types for Round Transitions
**File: `common/src/game_state.rs`**

Add to `GameEvent` enum:
```rust
// Round lifecycle events
RoundCompleted { winning_team_id: TeamId, round_number: u32 },
RoundStarting { round_number: u32, start_time: i64 },
MatchCompleted { winning_team_id: TeamId, final_scores: HashMap<TeamId, u32> },

// Arena reset events (for new round)
ArenaReset,
SnakeRespawned { snake_id: u32, position: Position, direction: Direction },
AllFoodCleared,
FoodRespawned { positions: Vec<Position> },
RoundWinRecorded { team_id: TeamId, total_wins: u32 },
```

### 3. Update apply_event() Method
**File: `common/src/game_state.rs`**

Add handlers for new events:
```rust
GameEvent::RoundCompleted { winning_team_id, round_number } => {
    // Log the round completion
}
GameEvent::RoundWinRecorded { team_id, total_wins } => {
    self.round_wins.insert(team_id, total_wins);
}
GameEvent::RoundStarting { round_number, start_time } => {
    self.current_round = round_number;
    self.is_transitioning = true;
    self.round_start_times.push(start_time);
}
GameEvent::ArenaReset => {
    // Reset tick counter for the new round
    // Clear any round-specific state
}
GameEvent::AllFoodCleared => {
    self.arena.food.clear();
}
GameEvent::SnakeRespawned { snake_id, position, direction } => {
    if let Ok(snake) = self.get_snake_mut(snake_id) {
        snake.body = vec![position, Position {
            x: position.x - 1,
            y: position.y
        }];
        snake.direction = direction;
        snake.is_alive = true;
        snake.food = 0;
    }
}
GameEvent::FoodRespawned { positions } => {
    self.arena.food = positions;
}
GameEvent::MatchCompleted { winning_team_id, final_scores } => {
    // Match is over, no more rounds
}
```

### 4. Implement Round Transition Logic
**File: `common/src/game_state.rs`**

In `tick_forward()`:
- When a round ends (all snakes on one team are dead):
  1. Emit `RoundCompleted` event
  2. Emit `RoundWinRecorded` event to update round_wins
  3. Check if any team has reached `rounds_to_win`
  4. If match complete: Emit `MatchCompleted` and `StatusUpdated` with Complete
  5. If match continues: Start new round sequence:
     - Emit `RoundStarting` event (increments round, sets transition flag)
     - Emit `ArenaReset` event
     - Emit `AllFoodCleared` event
     - Emit `SnakeRespawned` events for each snake
     - Emit `FoodRespawned` event with initial food positions

### 5. Add Round Reset Helper
**File: `common/src/game_state.rs`**

Add helper to generate reset events:
```rust
fn generate_round_reset_events(&self, rng: &mut PseudoRandom) -> Vec<GameEvent> {
    let mut events = vec![];

    // Generate snake respawn events
    let spawn_positions = self.calculate_spawn_positions(...);
    for (snake_id, (pos, dir)) in spawn_positions {
        events.push(GameEvent::SnakeRespawned {
            snake_id,
            position: pos,
            direction: dir
        });
    }

    // Generate food respawn events
    let food_positions = self.generate_initial_food(rng);
    events.push(GameEvent::FoodRespawned { positions: food_positions });

    events
}
```

### 6. Initialize Round Settings
**File: `common/src/game_state.rs`**

In `GameState::new()`:
```rust
// Determine rounds_to_win based on game type
let rounds_to_win = match &game_type {
    GameType::TeamMatch { .. } => 1,  // Default to quick match
    // Later: Add a field to TeamMatch to specify competitive mode
    _ => 1,
};

// Initialize round tracking
let round_wins = if matches!(&game_type, GameType::TeamMatch { .. }) {
    let mut wins = HashMap::new();
    wins.insert(TeamId(0), 0);
    wins.insert(TeamId(1), 0);
    wins
} else {
    HashMap::new()
};
```

### 7. Update TypeScript Types
**File: `client/web/types/index.ts`**

Add to `GameState` interface:
```typescript
current_round: number;
round_wins: Record<number, number>;  // TeamId to round wins
rounds_to_win: number;
round_start_times: number[];
is_transitioning: boolean;
```

Add corresponding event types for TypeScript.

### 8. Update Scoreboard UI
**File: `client/web/components/Scoreboard.tsx`**

- Display round wins instead of snake scores for team games
- Show current round number (e.g., "Round 2 of 3")
- During round transitions, show "Round X Starting..." message
- Format: "Team 1: 2 - Team 2: 1" for round wins

### 9. Handle Round Transitions in GameArena
**File: `client/web/components/GameArena.tsx`**

- Listen for round transition events via processServerEvent
- Show round transition overlay (e.g., "Round 2 Starting in 3...")
- Handle countdown for new rounds (use `round_start_times`)
- The arena will automatically update as events are applied

## Key Implementation Details
1. **Event Sourcing**: All state changes happen through events
2. **Atomic Transitions**: Round reset is a series of events applied atomically
3. **Deterministic**: Given same events, all clients reach same state
4. **Round wins** are tracked separately from snake scores
5. **Replayable**: Event log can recreate entire match history including all rounds

## Testing Considerations
- Test event application for all new event types
- Verify round transition event sequence
- Test single-round games (quick match)
- Test multi-round games (competitive)
- Ensure deterministic behavior across clients
- Verify event log can replay multi-round matches