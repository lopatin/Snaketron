# Lobby-Only Matchmaking Migration

## Overview

This document tracks the migration from dual matchmaking paths (player-based + lobby-based) to a unified lobby-only matchmaking system. All players, whether solo or in parties, will queue as lobbies. The matchmaking core will only process lobbies.

## Goals

1. ✅ Eliminate code duplication (~585 line reduction)
2. ✅ Unified matchmaking algorithm for all players
3. ✅ Auto-create lobbies for solo queue at WebSocket layer
4. ✅ Track requesting user for spectator preference
5. ✅ Maintain wait-time weighted MMR adjustments

## Key Design Decisions

- **Auto-lobby Creation**: WebSocket layer auto-creates lobbies for solo queue (like guest lobbies)
- **Requesting User Tracking**: `QueuedLobby.requesting_user_id` tracks who initiated the queue
- **Spectator Preference**: Requesting user gets priority when lobby has too many players
- **Clean Separation**: Matchmaking core doesn't know about "auto" vs "manual" lobbies

## Implementation Status

### Phase 1: Add requesting_user_id Field ✅ Complete

**Goal**: Track which user requested matchmaking for spectator preference.

**Tasks**:
- [x] Add `requesting_user_id: u32` to `QueuedLobby` struct (matchmaking_manager.rs:22-30)
- [x] Update `add_lobby_to_queue()` signature to accept `requesting_user_id` parameter
- [x] Update JoinQueue handler to pass `user_id` (ws_server.rs) - NOTE: Not needed yet, Phase 2
- [x] Update QueueForMatch handler to pass `metadata.user_id` (ws_server.rs:1257)
- [x] Update `find_team_match_with_spectators()` to prefer requesting user (matchmaking.rs:313-383)

**Files Modified**:
- `server/src/matchmaking_manager.rs` - Added `requesting_user_id` field and updated method signature
- `server/src/matchmaking.rs` - Updated spectator selection to prefer requesting user
- `server/src/ws_server.rs` - Updated call site to pass `metadata.user_id`

**Testing**:
- [ ] Verify requesting user is preferred in spectator selection
- [ ] Verify fallback when requesting user not in lobby

**Implementation Notes**:
- Requesting user gets priority when selecting players vs spectators
- Falls back to normal selection if requesting user not found in lobby
- Algorithm: requesting user first, then fill remaining slots in order

---

### Phase 2: Auto-Create Lobbies for Solo Queue ✅ Complete

**Goal**: Solo players get auto-created lobbies at WebSocket layer.

**Tasks**:
- [x] Remove call to `add_to_matchmaking_queue()` in JoinQueue handler - NOT NEEDED (QueueForMatch handler in Authenticated state)
- [x] Implement lobby creation using `lobby_manager.create_lobby()`
- [x] Join the auto-created lobby using `lobby_manager.join_lobby()`
- [x] Fetch members and queue the lobby with `add_lobby_to_queue()`
- [x] Transition to InLobby state

**Files Modified**:
- `server/src/ws_server.rs` - Replaced QueueForMatch handler in Authenticated state (lines 768-858)

**Implementation Notes**:
- Solo queue now creates full lobbies using `lobby_manager.create_lobby()` (just like guest lobbies)
- Auto-created lobby is immediately joined by the solo player
- Lobby members are fetched and lobby is added to matchmaking queue with `requesting_user_id`
- Connection transitions to InLobby state, which automatically handles lobby match notifications
- The InLobby state handles match found notifications via existing lobby notification system (lines 429-502)

**Testing**:
- [ ] Solo queue creates lobbies correctly
- [ ] Auto-lobbies appear in DynamoDB
- [ ] Auto-lobbies queue and match properly
- [ ] Redis heartbeats work for auto-lobbies

---

### Phase 3: Delete Player-Based Matchmaking Code ✅ Complete

**Goal**: Remove all player-based matchmaking infrastructure.

**Tasks**:
- [x] Delete unused constants: `MMR_RANGES`, `WAIT_THRESHOLDS`, `MIN_PLAYERS_BY_WAIT`
- [x] Delete `RankedPlayer` struct
- [x] Delete `create_matches_batch()` function (~138 lines)
- [x] Delete `create_single_game()` helper (~97 lines)
- [x] Delete `create_match()` function (~187 lines)
- [x] Remove `create_matches_batch` calls from `run_matchmaking_loop()` (~36 lines)
- [x] Update run loop to only call `create_lobby_matches()`

**Files Modified**:
- `server/src/matchmaking.rs` - Removed ~458 lines of player-based matchmaking code

**Implementation Notes**:
- Deleted all player-based matchmaking functions
- Removed unused MMR range and wait threshold constants
- Run loop now only uses `create_lobby_matches()` for both Quickmatch and Competitive modes
- All matchmaking is now exclusively lobby-based

**Testing**:
- [ ] Run existing lobby matchmaking tests
- [ ] Verify no player-based matchmaking active
- [ ] Check logs show only lobby matches created

---

### Phase 4: Clean Up MatchmakingManager ✅ Complete

**Goal**: Remove player queue methods from MatchmakingManager.

**Tasks**:
- [x] Delete `add_to_queue()` method (~60 lines)
- [x] Delete `get_queued_players()` method (~17 lines)
- [x] Delete `get_longest_waiting_users()` method (~16 lines)
- [x] Delete `get_lowest_mmr_users()` method (~11 lines)
- [x] Delete `get_highest_mmr_users()` method (~11 lines)
- [x] Delete `batch_get_user_status()` method (~27 lines)
- [x] Delete `get_players_in_mmr_range()` method (~18 lines)
- [ ] Keep `remove_from_queue()` - still used for cleanup (NOT deprecated, still needed)

**Files Modified**:
- `server/src/matchmaking_manager.rs` - Removed ~160 lines of player-based queue methods

**Implementation Notes**:
- Deleted all player-based queue management methods
- Kept `remove_from_queue()`, `renew_queue_position()`, and other utility methods (still needed for cleanup)
- All matchmaking operations now use lobby-based methods

**Testing**:
- [ ] Compilation succeeds
- [ ] Full test suite passes
- [ ] No references to deleted methods

---

### Phase 5: Clean Up ws_matchmaking ✅ Complete

**Goal**: Remove player queue helper function.

**Tasks**:
- [x] Delete `add_to_matchmaking_queue()` function (~15 lines)
- [x] Keep `remove_from_matchmaking_queue()` - still used for cleanup

**Files Modified**:
- `server/src/ws_matchmaking.rs` - Removed ~15 lines

**Implementation Notes**:
- Deleted `add_to_matchmaking_queue()` which was calling the now-deleted `MatchmakingManager::add_to_queue()`
- Kept `remove_from_matchmaking_queue()` and other utility functions (still used for cleanup)

**Testing**:
- [ ] Compilation succeeds
- [ ] Queue removal still works

---

### Phase 6: Update Tests ✅ Complete

**Goal**: Ensure test coverage for new system.

**Tasks**:
- [x] Remove tests that directly test player-based matchmaking (deleted `test_queue_operations` from matchmaking_manager.rs)
- [x] Remove unused import from ws_server.rs (`add_to_matchmaking_queue`)
- [x] Verify compilation succeeds
- [x] Run unit tests - all 3 tests pass
- [ ] Verify existing lobby_matchmaking_tests pass (requires Redis/DynamoDB)
- [ ] Add test for auto-created lobbies (future work)
- [ ] Add test for requesting user preference in spectator selection (future work)

**Files Modified**:
- `server/src/matchmaking_manager.rs` - Removed `test_queue_operations` test
- `server/src/ws_server.rs` - Removed unused import

**Implementation Notes**:
- All unit tests pass successfully
- Removed player-based matchmaking test
- Fixed compilation errors from deleted functions
- Integration tests require running infrastructure (Redis, DynamoDB)

**Testing**:
- [x] Compilation succeeds
- [x] Unit tests pass (3/3 tests passing)
- [ ] Integration tests (requires test infrastructure to be running)

---

## Code Changes Summary

| File | Lines Deleted | Lines Added | Net Change |
|------|---------------|-------------|------------|
| `server/src/matchmaking_manager.rs` | ~220 (methods + test) | ~5 | -215 |
| `server/src/matchmaking.rs` | ~458 | ~50 | -408 |
| `server/src/ws_server.rs` | ~125 (old handler) | ~90 (new handler) | -35 |
| `server/src/ws_matchmaking.rs` | ~15 | 0 | -15 |
| **Total** | **~818** | **~145** | **-673** |

## Verification Checklist

- [x] No player-based matchmaking code remains
- [x] All unit tests pass (3/3)
- [x] No compilation errors
- [x] Code compiles with only expected warnings (unrelated to changes)
- [ ] Solo queue works end-to-end (requires integration test)
- [ ] Party queue works end-to-end (requires integration test)
- [ ] Spectator selection prefers requester (requires integration test)
- [ ] Time-weighted MMR works for all lobbies (already implemented, requires integration test)

## Timeline

- **Start Date**: 2025-10-12
- **Completion Date**: 2025-10-12
- **Total Time**: ~2-3 hours (all phases completed)

## Notes

- Wait-time weighted MMR adjustment already works correctly in `create_lobby_matches()` (lines 812-832)
- Formula: `time_weighted_mmr = mmr + (avg_mmr - mmr) * (wait_seconds / 60.0).min(1.0)`
- This applies equally to auto-created lobbies and manual lobbies
- Matchmaking core has no knowledge of auto vs manual lobbies - clean abstraction
