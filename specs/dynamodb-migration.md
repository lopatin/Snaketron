# DynamoDB Migration Specification

## Overview
Complete migration from PostgreSQL to DynamoDB for the SnakeTron game server, including LocalStack integration for local development.

## Important Notes
- **Rust Version**: This migration requires Rust 1.86.0 or later due to AWS SDK requirements
- **Current Status**: Core database abstraction layer is complete, but compilation requires Rust upgrade

## Migration Status Tracker

### Phase 1: Infrastructure Setup
- [x] Add LocalStack to docker-compose.yml
- [x] Create DynamoDB table initialization scripts
- [x] Update docker-compose.dev.yml for development
- [x] Configure AWS SDK environment variables

### Phase 2: Dependencies & Core Module
- [x] Update server/Cargo.toml dependencies
- [x] Create server/src/db module structure
- [x] Implement DynamoDB client wrapper
- [x] Create data models with serde support

### Phase 3: Core Operations Migration
- [x] Server registration & heartbeats (game_server.rs updated)
- [ ] User authentication & creation (ws_server.rs needs update)
- [ ] Game lifecycle management (ws_server.rs needs update)
- [ ] Custom lobby operations (ws_server.rs needs update)
- [ ] Game player management (ws_server.rs needs update)
- [ ] Spectator management (ws_server.rs needs update)

### Phase 4: Test Infrastructure
- [ ] Update test utilities for LocalStack
- [ ] Migrate integration tests
- [ ] Add DynamoDB-specific tests
- [ ] Remove PostgreSQL test infrastructure

### Phase 5: Cleanup
- [ ] Remove PostgreSQL dependencies
- [ ] Remove migration files
- [ ] Update documentation
- [ ] Update production docker-compose

## DynamoDB Table Design

### Main Table: `snaketron-main`

**Keys:**
- Partition Key: `pk` (String)
- Sort Key: `sk` (String)

**Global Secondary Indexes:**

**GSI1 - Entity Type Index:**
- Partition Key: `gsi1pk` (String) - Entity type
- Sort Key: `gsi1sk` (String) - Timestamp/status
- Use cases: List all servers, all games by status

**GSI2 - Region/Heartbeat Index:**
- Partition Key: `gsi2pk` (String) - Region
- Sort Key: `gsi2sk` (String) - heartbeat#serverId
- Use cases: Find active servers by region for load balancing

**Attributes:**
- `ttl` (Number) - Unix timestamp for TTL
- `data` (Map) - Entity-specific data
- Additional type-specific attributes

### Username Index Table: `snaketron-usernames`

**Keys:**
- Partition Key: `username` (String)

**Attributes:**
- `userId` (Number)
- `passwordHash` (String)
- `mmr` (Number)

### Game Code Index Table: `snaketron-game-codes`

**Keys:**
- Partition Key: `gameCode` (String)

**Attributes:**
- `gameId` (String)
- `isPrivate` (Boolean)
- `status` (String)

## Entity Access Patterns

### Servers
- **Create:** Put item with pk=`SERVER#<id>`, sk=`META`
- **Heartbeat:** Update item, set heartbeat timestamp
- **List by region:** Query GSI2 with gsi2pk=`<region>`
- **Get load balanced:** Query GSI2, sort by game count

### Users
- **Create:** Put item with pk=`USER#<id>`, sk=`META`
- **Get by ID:** Get item with pk=`USER#<id>`, sk=`META`
- **Get by username:** Query username table, then get by ID
- **Update MMR:** Update item attribute

### Games
- **Create:** Put item with pk=`GAME#<id>`, sk=`META`
- **Add player:** Put item with pk=`GAME#<id>`, sk=`PLAYER#<user_id>`
- **Get with players:** Query with pk=`GAME#<id>`
- **Update status:** Update item attribute

### Custom Lobbies
- **Create:** Put item with pk=`LOBBY#<code>`, sk=`META`
- **Set TTL:** Set ttl attribute to expire time
- **Get by code:** Get item or use game code table

## Implementation Notes

### LocalStack Configuration
- Use version 3.0+ for better DynamoDB support
- Enable PERSISTENCE=1 for data persistence
- Set up health checks for readiness

### AWS SDK Configuration
- Use `aws-sdk-dynamodb` with `aws-config`
- Configure endpoint override for LocalStack
- Use same client code for local and production

### Error Handling
- Implement retry logic with exponential backoff
- Handle ProvisionedThroughputExceededException
- Convert DynamoDB errors to application errors

### Migration Strategy
- Implement database trait for abstraction
- Support both backends temporarily if needed
- Use feature flags for gradual rollout