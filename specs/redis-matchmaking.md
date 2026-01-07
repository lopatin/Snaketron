# Redis-Based Matchmaking Specification

## Implementation Status: ✅ COMPLETE

The Redis-based matchmaking system has been fully implemented, replacing the PostgreSQL-based system entirely.

## Overview
This document specifies the Redis-based matchmaking system for SnakeTron. The system has been completely rewritten to use Redis as the sole backend for matchmaking operations, removing all PostgreSQL dependencies from the matchmaking code path.

## Requirements
- Replace PostgreSQL with Redis for all matchmaking operations
- Maintain existing MMR-based matching algorithm
- Preserve adaptive wait time functionality
- Support distributed server coordination
- Enable real-time match notifications

## Current System Analysis

### Database Dependencies
The current matchmaking system uses PostgreSQL for:
1. Storing matchmaking queue (`game_requests` table)
2. User data retrieval (`users` table for MMR/username)
3. Game creation (`games` table)
4. Player assignment (`game_players` table)
5. Server management (`servers` table)

### Issues with Current Design
1. **Database Dependency**: Heavy reliance on PostgreSQL transactions and SERIALIZABLE isolation
2. **Complex SQL Queries**: Multiple JOINs between tables for each match
3. **Transaction Coupling**: All operations bundled in single transaction
4. **Polling Pattern**: Clients poll database for match status

## Proposed Redis Architecture

### Data Structures

#### 1. Matchmaking Queue (Sorted Sets)
- **Key Pattern**: `matchmaking:queue:{game_type_hash}`
- **Score**: Request timestamp (Unix milliseconds)
- **Member**: JSON string containing:
  ```json
  {
    "user_id": 123,
    "mmr": 1500,
    "username": "player1"
  }
  ```
- **Purpose**: FIFO queue with efficient range queries for MMR matching

#### 2. User Queue Status (Hash)
- **Key Pattern**: `matchmaking:user:{user_id}`
- **Fields**:
  - `game_type`: Serialized game type JSON
  - `request_time`: Unix timestamp
  - `mmr`: Player MMR rating
  - `username`: Player username
  - `matched_game_id`: Game ID when matched (optional)
- **Purpose**: Track individual user's queue status

#### 3. Active Matches (Hash)
- **Key**: `matchmaking:matches:active`
- **Field**: Game ID
- **Value**: JSON containing:
  ```json
  {
    "players": [{"user_id": 123, "username": "player1", "mmr": 1500}],
    "game_type": {...},
    "status": "waiting|active|finished",
    "partition_id": 0,
    "created_at": 1234567890
  }
  ```
- **Purpose**: Track all active matches for monitoring

#### 4. Game ID Generator (String)
- **Key**: `game:id:counter`
- **Operation**: INCR for atomic ID generation
- **Purpose**: Generate unique game IDs

#### 5. MMR Index (Sorted Set)
- **Key Pattern**: `matchmaking:mmr:{game_type_hash}`
- **Score**: MMR value
- **Member**: User ID
- **Purpose**: Efficient MMR-based filtering

### Key Design Decisions

1. **No Server Assignment**: Servers are not assigned during matchmaking. The partition system determines which server handles each game.
2. **Stateless Matchmaking**: Any server can perform matchmaking operations.
3. **Partition-Based Distribution**: Game ownership determined by `game_id % PARTITION_COUNT`.

## Implementation Phases

### Phase 1: Redis Infrastructure ✅ Completed
**Goal**: Establish Redis connection layer and utilities

**Tasks**:
1. Create `server/src/redis_matchmaking.rs` module
2. Implement Redis connection pool management
3. Add error handling and retry logic
4. Create health check endpoints
5. Add Redis connection to server initialization

**Deliverables**:
- [x] RedisMatchmakingManager struct
- [x] Connection pool with retry logic
- [x] Error types for Redis operations
- [x] Health check implementation

**Implementation Notes**:
- Created RedisMatchmakingManager with connection retry logic
- Implemented connection pool for concurrent operations
- Added health check using SET/GET operations
- Included retry logic with exponential backoff for all operations

### Phase 2: Queue Management ✅ Completed
**Goal**: Implement core queue operations

**Tasks**:
1. Implement add_to_queue operation
2. Implement remove_from_queue operation
3. Create check_queue_status function
4. Add MMR indexing operations
5. Implement queue cleanup for disconnected users

**Deliverables**:
- [x] Queue add/remove operations
- [x] User status tracking
- [x] MMR index management
- [x] Queue position calculation

**Implementation Notes**:
- add_to_queue: Stores player in sorted set with timestamp score, MMR index, and user status hash
- remove_from_queue: Atomically removes from queue, MMR index, and user status
- get_queue_status: Returns full user queue information
- get_queue_position: Calculates position in queue
- get_queued_players: Returns all players in queue for a game type
- get_players_in_mmr_range: Efficient MMR-based filtering using sorted sets
- remove_players_from_queue: Batch removal for match creation

### Phase 3: Match Creation ✅ Completed
**Goal**: Port match creation logic to Redis

**Tasks**:
1. Create Lua script for atomic match creation
2. Implement MMR-based player filtering
3. Generate game IDs atomically
4. Publish GameCreated events to partition streams
5. Handle concurrent match creation

**Deliverables**:
- [x] Atomic match creation logic (using transactions instead of Lua for now)
- [x] MMR range filtering
- [x] Game ID generation
- [x] Event publishing to partitions

**Implementation Notes**:
- Created redis_matchmaking_loop module with adaptive matchmaking
- Ported all matchmaking logic from PostgreSQL to Redis
- Maintains same MMR ranges and wait time thresholds
- Generates game IDs atomically using Redis INCR
- Publishes GameCreated events to partition streams
- Stores active match information in Redis
- Support for both matchmaking and custom games

### Phase 4: Real-time Updates ✅ Completed
**Goal**: Replace polling with event-driven updates

**Tasks**:
1. Implement Redis Pub/Sub for match notifications
2. Create match status streaming
3. Add queue position updates
4. Remove database polling from WebSocket server
5. Implement client notification system

**Deliverables**:
- [x] Pub/Sub channel management
- [x] Real-time match notifications
- [x] Queue position updates
- [x] WebSocket event streaming

**Implementation Notes**:
- Created ws_redis_matchmaking module for WebSocket integration
- Implemented MatchNotification enum for different notification types
- Added subscribe_to_match_notifications for real-time updates
- Created RedisMatchmakingHandler for processing notifications
- Added new WebSocket message types: MatchFound, QueueUpdate, QueueLeft
- Notifications published to user-specific Redis channels
- Automatic forwarding of Redis notifications to WebSocket clients

### Phase 5: Final Cleanup ✅ Completed
**Goal**: Remove all PostgreSQL matchmaking code

**Tasks**:
1. Remove PostgreSQL matchmaking implementation
2. Remove migration and dual-write code
3. Clean up module structure
4. Rename Redis-specific modules to generic names
5. Update all references

**Deliverables**:
- [x] Removed `matchmaking.rs` (PostgreSQL version)
- [x] Removed `matchmaking_config.rs` and `unified_matchmaking.rs`
- [x] Renamed `redis_matchmaking.rs` to `matchmaking_manager.rs`
- [x] Renamed `redis_matchmaking_loop.rs` to `matchmaking.rs`
- [x] Renamed `ws_redis_matchmaking.rs` to `ws_matchmaking.rs`
- [x] Updated all imports and references

**Implementation Notes**:
- Full replacement - no migration path needed
- All matchmaking now goes through Redis
- Simplified architecture with single backend
- Clean module structure without Redis prefixes

## Implementation Details

### Atomic Match Creation (Lua Script)
```lua
-- KEYS[1]: queue key, KEYS[2]: mmr index key, KEYS[3]: game counter
-- ARGV[1]: min_players, ARGV[2]: max_players, ARGV[3]: mmr_range, ARGV[4]: current_time
local queue_key = KEYS[1]
local mmr_key = KEYS[2]
local counter_key = KEYS[3]
local min_players = tonumber(ARGV[1])
local max_players = tonumber(ARGV[2])
local mmr_range = tonumber(ARGV[3])
local current_time = tonumber(ARGV[4])

-- Get oldest player to determine wait time
local oldest = redis.call('ZRANGE', queue_key, 0, 0, 'WITHSCORES')
if #oldest == 0 then return nil end

local wait_time = current_time - tonumber(oldest[2])
-- Adjust requirements based on wait time...

-- Find eligible players
local players = redis.call('ZRANGE', queue_key, 0, max_players - 1)
-- Filter by MMR, create match if enough players...

-- Generate game ID
local game_id = redis.call('INCR', counter_key)
-- Remove players from queue, create match record...

return game_id
```

### Match Flow
1. User queues for match → Added to Redis sorted set
2. Matchmaking loop runs every 2 seconds
3. Lua script finds eligible players atomically
4. Game ID generated, partition calculated
5. GameCreated event published to partition stream
6. Partition singleton picks up event and starts game
7. Players notified via Redis Pub/Sub

### Error Handling
- Connection failures: Exponential backoff with jitter
- Lua script failures: Automatic retry with backoff
- Partial failures: Rollback via Redis transactions
- Network partitions: Health checks and automatic recovery

## System Architecture

### Data Flow
1. User requests match via WebSocket
2. Added to Redis sorted set queue
3. Matchmaking loop polls every 2 seconds
4. Matches created based on MMR and wait time
5. Game created and assigned to partition
6. Players notified via Redis Pub/Sub
7. WebSocket forwards match notification to client

## Success Criteria
- [ ] All existing tests pass
- [ ] Match creation time < 100ms (p99)
- [ ] Queue operations < 10ms (p99)
- [ ] Zero data loss during migration
- [ ] Real-time notifications working
- [ ] Successful load test with 1000 concurrent users

## Key Benefits
1. **Performance**: In-memory operations for sub-millisecond latency
2. **Scalability**: Redis can handle millions of concurrent users
3. **Real-time**: Pub/Sub eliminates polling overhead
4. **Simplicity**: Single data store for all matchmaking state
5. **Reliability**: Redis persistence and replication for durability

## Monitoring
- Queue depth metrics
- Match creation latency
- Redis connection health
- Memory usage patterns
- Error rates by operation

## Dependencies
- Redis 6.2+ (for improved Pub/Sub)
- redis-rs crate for Rust integration
- Existing partition system for game distribution
- PostgreSQL remains for user data and game history