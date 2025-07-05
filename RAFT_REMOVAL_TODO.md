# TODO: Complete Raft Removal from SnakeTron

This document tracks what needs to be done to completely remove Raft from the SnakeTron architecture and rely on Redis streams for game event distribution.

## Current State
- ✅ Game executor now uses Redis streams instead of Raft
- ✅ Game events are published to partition-specific Redis streams
- ✅ Game executor reads commands from Redis streams
- ❌ Integration tests are broken (e.g., test_simple_game) because game creation still goes through Raft

## Remaining Work

### 1. Game Creation Flow
**Current**: Games are created via Raft (CreateGame request)
**Needed**: 
- Create games directly in database/Redis
- Publish GameCreated event to appropriate Redis stream partition
- Update matchmaking service to bypass Raft

### 2. Game Command Submission
**Current**: Commands go through WebSocket → Raft → Game Executor
**Needed**:
- WebSocket server should publish commands directly to Redis streams
- Remove `SubmitGameCommand` from Raft

### 3. Game State Storage
**Current**: Game states are stored in Raft's replicated state machine
**Needed**:
- Store game states in Redis or PostgreSQL
- Game executor should persist state after each tick
- Implement state recovery on executor restart

### 4. Game Event Distribution
**Current**: Game events flow through Raft to WebSocket clients
**Needed**:
- WebSocket servers should subscribe to Redis streams
- Implement proper event filtering per client

### 5. Server Registration/Discovery
**Current**: Servers register via Raft
**Needed**:
- Use database or Redis for server registration
- Implement heartbeat mechanism directly

### 6. StartGame Request
**Current**: StartGame goes through Raft to update game status
**Needed**:
- Update game status directly in storage
- Publish StatusUpdated event to Redis stream

### 7. Replay System
**Current**: Replay listener subscribes to Raft state events
**Needed**:
- Subscribe to Redis streams instead
- Update replay format if needed

### 8. Testing
- Update integration tests to work without Raft
- Create new tests for Redis stream-based architecture

## Benefits of Removing Raft
- Simpler architecture
- Better horizontal scalability
- Easier debugging
- Lower latency for game events
- No consensus overhead for game operations

## Migration Strategy
1. Implement parallel Redis-based paths alongside Raft
2. Gradually migrate each component
3. Run both systems in parallel for validation
4. Remove Raft dependencies once stable