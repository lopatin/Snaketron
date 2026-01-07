# Database Migration Patches

This document tracks the changes needed to migrate from PostgreSQL to DynamoDB.

## Files to Update:

### 1. server/src/game_server.rs
- Replace `PgPool` with `Arc<dyn Database>`
- Update all database queries to use Database trait methods
- Remove direct SQL queries

### 2. server/src/ws_server.rs  
- Replace `PgPool` with `Arc<dyn Database>`
- Update TestJwtVerifier to use Database trait
- Update all database helper functions

### 3. server/src/api/server.rs
- Replace `PgPool` with `Arc<dyn Database>`
- Update API endpoints to use Database trait

### 4. server/src/api/auth.rs
- Replace `PgPool` with `Arc<dyn Database>`
- Update authentication to use Database trait methods

### 5. Test files
- Update test infrastructure to use LocalStack
- Replace TestDatabase with DynamoDB test setup

## Key Changes Required:

1. Replace all `sqlx::query` calls with appropriate Database trait methods
2. Update function signatures to accept `Arc<dyn Database>` instead of `PgPool`
3. Remove SQL migration code
4. Update error handling for DynamoDB errors instead of SQL errors