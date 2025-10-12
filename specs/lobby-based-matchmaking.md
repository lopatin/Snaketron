# Lobby-Based Matchmaking Design

**Status**: In Progress
**Created**: 2025-10-11
**Last Updated**: 2025-10-11

## Overview

This document describes the design and implementation of comprehensive lobby-based matchmaking that supports all game modes (Solo, 1v1, 2v2, FFA) for both Quickmatch and Competitive queues.

## Requirements

1. ✅ Lobby-based matchmaking works for both casual (Quickmatch) and competitive modes
2. ✅ Support for all 4 game modes:
   - Solo (1 player)
   - 1v1 (TeamMatch with per_team=1, 2 players total)
   - 2v2 (TeamMatch with per_team=2, 4 players total)
   - FFA (FreeForAll with variable max_players)
3. ✅ Lobby max size: 8 players
4. ✅ Matchmaking tries to create games for each game mode on every iteration
5. ⏳ Comprehensive unit tests verify all matchmaking combinations

## Current Implementation Status

### ✅ Completed (Phase 0-4 from guest-users-and-lobbies.md)
- [x] Guest user backend (authentication, JWT, database)
- [x] Guest user frontend (UI, API integration)
- [x] LobbyManager backend (Redis heartbeats, join/leave)
- [x] Lobby UI (sidebar, invite modal, member list)
- [x] Basic matchmaking integration (single lobby → single game)

### ⏳ In Progress
- [ ] Enhanced lobby matchmaking algorithm
- [ ] Multi-lobby combination matching
- [ ] Team assignment for lobbies in team games
- [ ] Comprehensive test suite

## Architecture

### Data Structures

#### Enhanced QueuedLobby (IN PROGRESS)
**File**: `server/src/matchmaking_manager.rs:22-27`

```rust
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueuedLobby {
    pub lobby_id: i32,
    pub lobby_code: String,
    pub members: Vec<LobbyMember>,
    pub avg_mmr: i32,

    // NEW FIELDS NEEDED:
    pub game_type: GameType,      // What game mode they queued for
    pub queue_mode: QueueMode,     // Quickmatch or Competitive
    pub queued_at: i64,            // Timestamp for wait time calculation
}
```

**Status**: ❌ Not implemented yet
**Changes needed**:
- Add `game_type`, `queue_mode`, `queued_at` fields
- Update `add_lobby_to_queue()` in matchmaking_manager.rs
- Update serialization/deserialization

#### New MatchmakingCombination Type (TODO)
**File**: `server/src/matchmaking.rs` (new addition)

```rust
/// Represents a valid combination of lobbies that can form a game
struct MatchmakingCombination {
    lobbies: Vec<QueuedLobby>,
    team_assignments: Option<Vec<(i32, TeamId)>>,  // (lobby_id, team_id) pairs
    total_players: usize,
    avg_mmr: i32,
}

impl MatchmakingCombination {
    fn is_valid(&self, game_type: &GameType) -> bool {
        match game_type {
            GameType::Solo => self.total_players == 1,
            GameType::TeamMatch { per_team: 1 } => self.total_players == 2,
            GameType::TeamMatch { per_team: 2 } => self.total_players == 4,
            GameType::FreeForAll { max_players } => {
                self.total_players >= 2 && self.total_players <= *max_players as usize
            }
            _ => false,
        }
    }
}
```

**Status**: ❌ Not implemented yet

### Matchmaking Algorithm

#### Current Implementation (INCOMPLETE)
**File**: `server/src/matchmaking.rs:423-509`

**Current behavior**:
- Gets all lobbies for a game type and queue mode
- Creates ONE game per lobby (line 441: "For now, each lobby creates its own game")
- No multi-lobby matching
- No team assignment logic

**Problems**:
1. Cannot match multiple lobbies together (e.g., 2 solo players for 1v1)
2. No validation that lobby size matches game mode requirements
3. Team assignments are random, not lobby-aware

#### Proposed Algorithm

##### High-Level Flow
```
create_lobby_matches(game_type, queue_mode):
    1. Get all queued lobbies for this game_type and queue_mode
    2. While lobbies remain available:
        a. Find best combination of lobbies that can form a valid game
        b. If valid combination found:
            - Create game with proper team assignments
            - Remove matched lobbies from queue
            - Publish match notifications
        c. Else: break (no more valid combinations)
    3. Return number of games created
```

##### Combination Finding Logic

**For Solo (1 player)**:
- Take any single-member lobby
- No team assignment needed

**For 1v1 (TeamMatch per_team=1, 2 players)**:
- **Case A**: One 2-player lobby → Split into Team 0 and Team 1
- **Case B**: Two 1-player lobbies → Each on separate team (Team 0, Team 1)

**For 2v2 (TeamMatch per_team=2, 4 players)**:
- **Case A**: One 4-player lobby → First 2 on Team 0, last 2 on Team 1
- **Case B**: Two 2-player lobbies → Each lobby = one team
- **Case C**: One 3-player lobby + one 1-player lobby → 3 on one team, 1 on other (needs MMR balancing)

**For FFA (variable max_players)**:
- Greedy combination: Take lobbies until reaching max_players
- Minimum 2 players required
- No team assignments

##### MMR Considerations
- Calculate weighted average MMR across all lobbies in combination
- Prefer combinations with similar MMR (within 500 MMR range for Quickmatch)
- Tighter MMR requirements for Competitive mode

### Team Assignment Strategy

#### Lobby-Level Assignment (Preferred)
For team games, assign teams at the **lobby level** rather than individual player level:

```rust
// For 2v2 with two 2-player lobbies
team_assignments = vec![
    (lobby_1.lobby_id, TeamId(0)),  // All members of lobby 1 → Team 0
    (lobby_2.lobby_id, TeamId(1)),  // All members of lobby 2 → Team 1
]
```

**Benefits**:
- Keeps lobby members together on same team (what players expect)
- Simpler logic and easier to test
- Matches typical party-based gameplay

#### Player-Level Assignment (For Split Cases)
For cases like 1v1 with a 2-player lobby, split at player level:

```rust
// For 1v1 with one 2-player lobby
// Assign first player to Team 0, second to Team 1
team_assignments = vec![
    (lobby_1.members[0].user_id, TeamId(0)),
    (lobby_1.members[1].user_id, TeamId(1)),
]
```

### Integration Points

#### WebSocket Handler Changes
**File**: `server/src/ws_server.rs`

**Current behavior** (line ~2800 in QueueForMatch handler):
```rust
// Host-only permission to queue lobby
if current_lobby_id == lobby.id {
    // Calculate average MMR
    let avg_mmr = calculate_avg_mmr(&members);

    // Add lobby to queue
    mm.add_lobby_to_queue(
        lobby.id,
        &lobby.lobby_code,
        members.clone(),
        avg_mmr,
        game_type.clone(),  // ALREADY PASSING THIS
        queue_mode.clone(),  // ALREADY PASSING THIS
    ).await?;
}
```

**Status**: ✅ Already passes game_type and queue_mode!

**Action needed**: Just update `add_lobby_to_queue()` signature to accept and store these values

#### Game Creation Changes
**File**: `server/src/matchmaking.rs:325-421` (`create_single_game()`)

**Current behavior**:
- Takes a flat list of `QueuedPlayer`
- Assigns teams alternately (A, B, A, B) for TeamMatch
- No awareness of which players came from same lobby

**Needed changes**:
- Accept `MatchmakingCombination` instead of `Vec<QueuedPlayer>`
- Use `team_assignments` from combination
- Respect lobby groupings when adding players to GameState

## Test Plan

### Test Categories

#### 1. Solo Mode Tests
```rust
#[tokio::test]
async fn test_solo_lobby_creates_solo_game() {
    // 1-player lobby queues for Solo
    // Expected: Creates Solo game with 1 player
}

#[tokio::test]
async fn test_multi_player_lobby_cannot_queue_solo() {
    // 2-player lobby tries to queue for Solo
    // Expected: Validation error or ignored by matchmaker
}
```

#### 2. 1v1 Tests
```rust
#[tokio::test]
async fn test_two_player_lobby_creates_1v1_with_split_teams() {
    // 1 lobby with 2 players queues for 1v1
    // Expected: Creates 1v1 game with players on separate teams
    // Verify: Player 1 on Team 0, Player 2 on Team 1
}

#[tokio::test]
async fn test_two_single_lobbies_create_1v1() {
    // 2 lobbies, each with 1 player, queue for 1v1
    // Expected: Creates 1v1 game with each player on separate team
    // Verify: Both players matched, correct team assignments
}

#[tokio::test]
async fn test_single_lobby_waits_for_match() {
    // 1 lobby with 1 player queues for 1v1
    // Expected: Remains in queue (needs another player)
}
```

#### 3. 2v2 Tests
```rust
#[tokio::test]
async fn test_two_player_lobby_joins_2v2_same_team() {
    // 2 lobbies, each with 2 players, queue for 2v2
    // Expected: Creates 2v2 game
    // Verify: Lobby 1 members on Team 0, Lobby 2 members on Team 1
}

#[tokio::test]
async fn test_three_player_lobby_joins_one_player_for_2v2() {
    // 1 lobby with 3 players + 1 lobby with 1 player queue for 2v2
    // Expected: Creates 2v2 game (3 vs 1 unbalanced but valid)
    // Verify: 3-player lobby on one team, 1-player on other
}

#[tokio::test]
async fn test_four_player_lobby_creates_2v2() {
    // 1 lobby with 4 players queues for 2v2
    // Expected: Creates 2v2 game
    // Verify: First 2 players on Team 0, last 2 on Team 1
}

#[tokio::test]
async fn test_two_double_lobbies_prioritized_over_split() {
    // Setup: Two 2-player lobbies and one 4-player lobby queue
    // Expected: Match the two 2-player lobbies first (better team balance)
}
```

#### 4. FFA Tests
```rust
#[tokio::test]
async fn test_ffa_multiple_lobbies_combine() {
    // 3 lobbies with 2, 1, 2 players queue for FFA (max 6)
    // Expected: All combine into one 5-player FFA game
}

#[tokio::test]
async fn test_ffa_respects_max_players() {
    // 2 lobbies with 3 players each queue for FFA (max 4)
    // Expected: Only one lobby gets matched (first come first served)
}

#[tokio::test]
async fn test_ffa_minimum_players() {
    // 1 lobby with 1 player queues for FFA (max 4)
    // Expected: Waits in queue (FFA needs at least 2 players)
}
```

#### 5. Edge Cases & Mixed Modes
```rust
#[tokio::test]
async fn test_lobby_too_large_for_game_mode() {
    // 5-player lobby queues for 2v2 (max 4)
    // Expected: Validation error or partial matching (take first 4)
}

#[tokio::test]
async fn test_quickmatch_and_competitive_dont_mix() {
    // Lobby A queues Quickmatch 1v1, Lobby B queues Competitive 1v1
    // Expected: Do not match together
}

#[tokio::test]
async fn test_mmr_filtering_for_lobbies() {
    // Lobby A (avg MMR 1000), Lobby B (avg MMR 2000) queue for 1v1
    // Expected: Do not match (MMR difference > threshold)
}
```

### Test File Structure
**Location**: `server/tests/lobby_matchmaking_tests.rs` (new file)

```rust
mod common;
use common::{TestEnvironment, TestClient};

// Helper function to create lobby and queue
async fn create_lobby_and_queue(
    env: &TestEnvironment,
    server_idx: usize,
    user_ids: &[u32],
    game_type: GameType,
    queue_mode: QueueMode,
) -> Result<(Vec<TestClient>, i32)> {
    // 1. Connect clients
    // 2. First client creates lobby
    // 3. Other clients join lobby
    // 4. Host queues for match
    // Return (clients, lobby_id)
}

// Test implementations...
```

## Implementation Roadmap

### Phase 1: Data Structure Updates ⏳
**Estimated time**: 2-3 hours

- [ ] Update `QueuedLobby` struct in `server/src/matchmaking_manager.rs`
  - [ ] Add `game_type: GameType` field
  - [ ] Add `queue_mode: QueueMode` field
  - [ ] Add `queued_at: i64` field
  - [ ] Update serialization derives

- [ ] Update `add_lobby_to_queue()` method signature
  - [ ] Accept `game_type` and `queue_mode` parameters
  - [ ] Store in QueuedLobby struct
  - [ ] Update Redis storage

- [ ] Add `MatchmakingCombination` struct in `server/src/matchmaking.rs`
  - [ ] Define struct with lobbies, team_assignments, total_players, avg_mmr
  - [ ] Implement `is_valid()` method for game type validation

- [ ] Update WebSocket handler (if needed)
  - [ ] Verify `QueueForMatch` handler passes game_type to queue

**Files to modify**:
- `server/src/matchmaking_manager.rs`
- `server/src/matchmaking.rs`

### Phase 2: Matchmaking Algorithm Implementation ⏳
**Estimated time**: 4-5 hours

- [ ] Implement `find_best_lobby_combination()` in `server/src/matchmaking.rs`
  - [ ] Dispatch to mode-specific finders based on GameType

- [ ] Implement `find_solo_combination()`
  - [ ] Find single 1-player lobby

- [ ] Implement `find_1v1_combination()`
  - [ ] Case A: One 2-player lobby (split teams)
  - [ ] Case B: Two 1-player lobbies (separate teams)
  - [ ] Prefer Case B for better competitive balance

- [ ] Implement `find_2v2_combination()`
  - [ ] Case A: One 4-player lobby (split into 2 teams)
  - [ ] Case B: Two 2-player lobbies (each lobby = one team)
  - [ ] Case C: One 3-player + one 1-player lobby
  - [ ] Prefer Case B for balanced teams

- [ ] Implement `find_ffa_combination()`
  - [ ] Greedy selection up to max_players
  - [ ] Ensure minimum 2 players
  - [ ] Consider MMR balance

- [ ] Add MMR filtering to combination finders
  - [ ] Calculate weighted average MMR
  - [ ] Apply thresholds (500 for Quickmatch, 250 for Competitive)

**Files to modify**:
- `server/src/matchmaking.rs`

### Phase 3: Game Creation with Team Assignment ⏳
**Estimated time**: 2-3 hours

- [ ] Create `create_game_from_lobbies()` function
  - [ ] Accept `MatchmakingCombination` instead of flat player list
  - [ ] Apply team assignments from combination
  - [ ] Handle both lobby-level and player-level assignments

- [ ] Update `create_single_game()` or create new variant
  - [ ] Extract players from lobbies with team info
  - [ ] Create GameState with proper team assignments
  - [ ] Ensure initial positions respect teams

- [ ] Update `create_lobby_matches()` main loop
  - [ ] Use new combination-finding logic
  - [ ] Call new game creation function
  - [ ] Remove matched lobbies from available pool

- [ ] Implement notification system
  - [ ] Publish to all lobby members via lobby notification channel
  - [ ] Include team assignment info in notification

**Files to modify**:
- `server/src/matchmaking.rs`

### Phase 4: Comprehensive Testing ⏳
**Estimated time**: 6-8 hours

- [ ] Create `server/tests/lobby_matchmaking_tests.rs`
- [ ] Implement helper functions
  - [ ] `create_lobby_and_queue()`
  - [ ] `wait_for_lobby_match()`
  - [ ] `verify_team_assignment()`

#### Solo Tests
- [ ] `test_solo_lobby_creates_solo_game()`
- [ ] `test_multi_player_lobby_cannot_queue_solo()`

#### 1v1 Tests
- [ ] `test_two_player_lobby_creates_1v1_with_split_teams()`
- [ ] `test_two_single_lobbies_create_1v1()`
- [ ] `test_single_lobby_waits_for_match()`

#### 2v2 Tests
- [ ] `test_two_player_lobby_joins_2v2_same_team()`
- [ ] `test_three_player_lobby_joins_one_player_for_2v2()`
- [ ] `test_four_player_lobby_creates_2v2()`
- [ ] `test_two_double_lobbies_prioritized_over_split()`

#### FFA Tests
- [ ] `test_ffa_multiple_lobbies_combine()`
- [ ] `test_ffa_respects_max_players()`
- [ ] `test_ffa_minimum_players()`

#### Edge Cases
- [ ] `test_lobby_too_large_for_game_mode()`
- [ ] `test_quickmatch_and_competitive_dont_mix()`
- [ ] `test_mmr_filtering_for_lobbies()`

**Files to create**:
- `server/tests/lobby_matchmaking_tests.rs`

### Phase 5: Integration & Polish ⏳
**Estimated time**: 2-3 hours

- [ ] Validation in WebSocket handler
  - [ ] Check lobby size against game mode requirements
  - [ ] Return helpful error messages

- [ ] Client-side improvements
  - [ ] Show matchmaking status in lobby UI
  - [ ] Display estimated wait time
  - [ ] Show which game mode is queued

- [ ] Documentation updates
  - [ ] Update API documentation
  - [ ] Add matchmaking algorithm explanation to docs
  - [ ] Update user guide

- [ ] Performance testing
  - [ ] Test with 50+ concurrent lobbies
  - [ ] Verify matchmaking loop performance
  - [ ] Check Redis performance under load

**Files to modify**:
- `server/src/ws_server.rs`
- `client/web/components/Sidebar.tsx`
- Various documentation files

## Design Decisions & Trade-offs

### 1. Greedy vs Optimal Matching
**Decision**: Greedy (first-fit)
**Rationale**: Simpler to implement, predictable behavior, lower latency
**Trade-off**: May not always find the "best" MMR-balanced match globally

### 2. Lobby-Level vs Player-Level Team Assignment
**Decision**: Lobby-level by default, player-level only when necessary
**Rationale**: Keeps friends together, matches player expectations
**Trade-off**: Some team games may have uneven team sizes (3v1 in 2v2)

### 3. Strict vs Flexible Lobby Size Validation
**Decision**: Flexible with warnings
**Rationale**: Don't block larger lobbies, just use subset of players
**Trade-off**: May confuse players if only some lobby members get into game

### 4. MMR Filtering Strictness
**Decision**: Apply same MMR thresholds as individual matchmaking
**Rationale**: Maintain competitive integrity
**Trade-off**: Longer wait times for mixed-skill lobbies

## Open Questions

### 1. Lobby Size Validation
**Question**: Should we enforce max lobby size based on queued game mode?
**Options**:
- A) Block queueing if lobby too large (5 players can't queue for 2v2)
- B) Allow but only use first N players that fit
- C) Allow and suggest splitting into multiple lobbies

**Recommendation**: Option A (block with helpful message)

### 2. Team Assignment in 1v1 with 2-Player Lobby
**Question**: Should we always split a 2-player lobby for 1v1?
**Options**:
- A) Always split (current plan)
- B) Show warning and ask for confirmation
- C) Don't allow 2-player lobbies to queue for 1v1

**Recommendation**: Option A (always split) - simplest and most predictable

### 3. Priority for Single-Lobby vs Multi-Lobby Matches
**Question**: Should single-lobby games get priority?
**Example**: 4-player lobby vs waiting for 2x 2-player lobbies for 2v2
**Options**:
- A) First-come-first-served (no priority)
- B) Prefer balanced matches (2x 2-player lobbies)
- C) Prefer single-lobby matches (faster, simpler)

**Recommendation**: Option A (FIFO) - fairest to all players

### 4. Unbalanced Team Sizes
**Question**: Should we allow unbalanced teams (3v1 for 2v2)?
**Options**:
- A) Allow any combination that totals correct player count
- B) Require balanced teams (2v2 must be exactly 2v2)
- C) Allow but deprioritize unbalanced combinations

**Recommendation**: Option A (allow) initially, can revisit based on player feedback

## Metrics & Monitoring

### Key Metrics to Track
- Lobby matchmaking success rate (% of lobbies that find match within 30s)
- Average wait time by game mode and lobby size
- Team balance in matched games
- MMR spread in matched games
- Queue depth over time

### Logging
- Log every lobby combination attempt (success/failure)
- Log team assignments for manual review
- Log lobby sizes and game modes queued
- Log MMR calculations

## References

- Original lobby spec: `specs/guest-users-and-lobbies.md`
- Redis matchmaking: `specs/redis-matchmaking.md`
- Team match design: `specs/TeamMatch.md`
- Current matchmaking: `server/src/matchmaking.rs`
- Current matchmaking manager: `server/src/matchmaking_manager.rs`
- Current lobby manager: `server/src/lobby_manager.rs`
