# SnakeTron Server Overview

The SnakeTron server is a Rust application that has the following responsibilities:

- Runs the authoritative game loop
- Runs the WebSocket and gRPC servers
- Clusters with other servers for high availability using Raft consensus
- Runs matchmaking and other periodic system tasks

# Architecture
Auto-scalability and resiliency is a key design goal of the SnakeTron server, which is achieved using clustering and auto fail-over of the game state. Infrastructure simplicity is also a goal. It should be a simple binary with no external dependencies other than the PostgreSQL database.

The server is deployed as a Rust binary inside a Docker container, designed for AWS Fargate deployment. Every server will have the following components:

## Game Manager
The GameManager holds actively running GameState instances which are assigned to the local server.

## WebSocket Server
Clients will connect to this server to send commands and receive game update events. It will interact with the game instance in the GameManager on behalf of the user.

## Service Manager
Manages background services like matchmaking, Raft consensus, and database heartbeats.

# Docker Deployment

The server is containerized for easy deployment. See [docker-readme.md](docker-readme.md) for detailed instructions.

## Quick Start with Docker

```bash
# Start server with database
docker-compose up --build

# Server will be available at:
# - WebSocket: ws://localhost:8080
# - gRPC: localhost:50051
```

## Environment Variables

Required environment variables:
- `SNAKETRON_DB_HOST`: Database host
- `SNAKETRON_DB_PORT`: Database port (default: 5432)
- `SNAKETRON_DB_USER`: Database username
- `SNAKETRON_DB_PASS`: Database password
- `SNAKETRON_DB_NAME`: Database name
- `SNAKETRON_HTTP_PORT`: HTTP/WebSocket port (default: 8080)
- `SNAKETRON_GRPC_PORT`: gRPC port (default: 50051)
- `SNAKETRON_REGION`: Server region identifier

Optional gameplay balance:

- `SNAKETRON_BOOST_SPEED_MULTIPLIER`: Boosted snake speed for newly created duel and 2v2 matches. Accepts `1.000` through `2.000` with up to three decimal places; defaults to `1.500`. The server validates it at startup and snapshots the resolved value into each match.
- `SNAKETRON_PLAYER_IDLE_GRACE_MS`: Time without gameplay input before the kick countdown begins. Defaults to `10000`.
- `SNAKETRON_PLAYER_IDLE_COUNTDOWN_MS`: Length of the visible kick countdown after the idle grace period. Defaults to `10000`.

The server validates both inactivity phases at startup and snapshots their sum
as the authoritative kick deadline alongside the countdown length. Existing
matches retain the policy they started with; after a server restart, changed
values apply to new matches without a client deployment.

Season schedule:

- Seasons roll automatically on UTC calendar quarters; there is no current-season environment variable or deployment step.
- Existing and pre-launch numeric data remains in Season 0 through `2026-09-30T23:59:59.999Z`. Season 1 begins at `2026-10-01T00:00:00Z`, Season 2 at `2027-01-01T00:00:00Z`, and subsequent seasons begin every January, April, July, and October.
- Completed games derive their immutable season from the authoritative completion timestamp, so a delayed retry after a boundary cannot move a result into another season.
- Skill rating carries across season boundaries; seasonal ranking wins/losses and Solo high-score partitions restart in the new season.

Administration:

- Administrative access to `/api/admin/*` comes from either of two independent grants, and is recalculated from the current database user on every authenticated request. Guests and stress-test users are never administrators, whichever grant claims otherwise.
  - **The durable `isAdmin` flag on the account.** False by default and absent from accounts nobody has granted it. This is how a deployed environment gets an administrator: run `scripts/set-user-admin.sh --user-id <id> --apply` from the deployment repository. It writes the account row directly, so it takes effect on that user's next request with no deploy and no restart.
  - **`SNAKETRON_ADMIN_USER_IDS`**: comma-separated durable numeric user IDs. Kept for local development and the Skin Factory tooling, which bootstrap an administrator from the environment against a throwaway database where there is no account to have flagged first. Changing it needs a process restart, so it is the wrong tool for a deployed environment.
- Runtime announcements, provider-neutral pre-match ad policy, and history-retention settings are stored in DynamoDB and managed through `/api/admin/config`. The safe defaults disable every ad distribution with a one-game threshold and 10-minute durable interval, retain snapshots for 30 days, and retain compact summaries for 365 days.
- Match-history projections are created by the immutable completion pipeline. Existing completed-game rows are not retroactively projected, so a deployment begins recording browseable history with the first completion processed after rollout; backfill requires an explicit migration from retained snapshots.

Completed game retention:

- `SNAKETRON_COMPLETED_GAME_RETENTION_DAYS`: Compatibility setting for the legacy direct completed-game upsert path (default: `30`). Immutable completion processing uses the runtime history configuration instead.
- Completed snapshots use the `ttl` attribute on the main DynamoDB table. At startup, the server waits boundedly for that table to become `ACTIVE`, verifies TTL with `dynamodb:DescribeTimeToLive`, and enables it with `dynamodb:UpdateTimeToLive` when necessary.
- The server fails startup if it cannot verify that TTL is `ENABLING` or `ENABLED` on exactly the `ttl` attribute. The runtime IAM role therefore always needs `dynamodb:DescribeTimeToLive`; it also needs `dynamodb:UpdateTimeToLive` unless deployment automation guarantees TTL is already configured.
- Prefer configuring the same TTL setting in deployment infrastructure. The startup check remains fail-fast so expired snapshots do not silently accumulate when infrastructure or IAM drifts.

Runtime game IDs are allocated durably in DynamoDB. Redis is not an authority
for game ID allocation.

Replay object storage:

- `SNAKETRON_REPLAY_S3_BUCKET`: Private S3 bucket for durable completed-game recordings. Omitting it disables the replay store; production Compose requires it.
- `SNAKETRON_REPLAY_S3_PREFIX`: Object-key prefix (default: `recordings`). Replays up to 1 MiB use the legacy deterministic `<prefix>/v1/games/<game-id>.replay.json.gz` object. Larger replays use content-addressed v2 manifest roots and 1 MiB chunks, so a stale upload for the same game cannot overwrite the committed root.
- `SNAKETRON_REPLAY_S3_STORAGE_CLASS`: `STANDARD`, `STANDARD_IA`, `ONEZONE_IA`, or `INTELLIGENT_TIERING` (default: `INTELLIGENT_TIERING`).
- `SNAKETRON_REPLAY_S3_KMS_KEY_ID`: Optional KMS key ID. Objects use SSE-KMS when set and SSE-S3 (`AES256`) otherwise.
- `SNAKETRON_REPLAY_S3_FORCE_PATH_STYLE`: Set to `true` for LocalStack; leave unset for AWS.
- `SNAKETRON_REPLAY_MAX_COMPRESSED_BYTES` and `SNAKETRON_REPLAY_MAX_UNCOMPRESSED_BYTES`: Positive per-object limits applied before or during decompression (defaults: 16 MiB and 64 MiB). They cap each manifest/chunk, not the aggregate recording; full in-process reconstruction has a separate 512 MiB safety limit.
- `SNAKETRON_REPLAY_CACHE_PREFIX`: Versioned Valkey key prefix (default: `snaketron:replay-cache`).
- `SNAKETRON_REPLAY_CACHE_TTL_SECONDS`: Recent-recording cache TTL (default: 3600 seconds).
- `SNAKETRON_REPLAY_CACHE_MAX_BYTES`: Maximum encoded manifest or chunk value admitted to the cache (default: 8 MiB; hard maximum: 64 MiB). Large recordings remain cacheable one object at a time.
- `SNAKETRON_REPLAY_CACHE_TIMEOUT_MS`: Per-operation cache deadline before falling back to S3 (default: 250 ms).
- `SNAKETRON_POTG_ENABLED`: Server-side Play-of-the-Game kill switch. It defaults to `true`; set it to `false`, `off`, `no`, or `0` to keep recording public games while returning the banner fallback instead of selecting new highlights.

The runtime role needs only `s3:GetObject` and `s3:PutObject` on the configured
prefix, plus KMS encrypt/decrypt permissions when a KMS key is configured. S3
objects are gzip-compressed and verified against version, length, and SHA-256
metadata before replay bytes are returned. Large roots additionally bind the
ordered chunk list and the aggregate canonical-recording digest. The
Valkey/ElastiCache layer caches recent manifests/chunks and is cache-aside
only: misses, timeouts, corrupt entries, and write failures do not make an
otherwise valid S3 recording unavailable.

Public replay APIs:

- `GET /api/games/:game_id/highlight` is anonymous and returns one bounded JSON state: `{"status":"pending"}`, `{"status":"ready","play_of_the_game":...}`, or `{"status":"unavailable"}`. Pending responses include `Retry-After: 1`.
- `GET /api/games/:game_id/replay` is anonymous. Recordings up to 8 MiB return the verified, uncompressed `GameRecordingV1` JSON. Larger recordings require one standard `Range: bytes=start-end` header and return `206 application/octet-stream`; each response is limited to 8 MiB and fetches only overlapping chunks. Reads use Valkey first and fall back to S3 on a miss, corrupt entry, timeout, or cache outage.
- At launch every game is readable through these endpoints, including custom games. Future private-custom-game access control must be added explicitly when that product mode ships.
- IDs must be positive decimal DynamoDB IDs. Replay ranges are capped at 8 MiB and highlight JSON at 256 KiB; malformed, incompatible, corrupt, and invalid-range payloads are never returned. PersistGame verifies replay semantics and the end hash once before publishing object metadata, not synchronously on every GET.

Production actors append the complete replay to a lease-fenced Redis journal;
recovery checkpoints serialize only its cursor. Every new production
completion carries only a bounded cursor/sequence/final-sync reference, so the
partition actor never assembles, verifies, or serializes the full archive.
XP/MMR/ranking effects run first and remain eligible during an S3 or replay
failure. `CompletionEffect::PersistGame` refreshes and hydrates the journal,
performs the one full semantic verification, serializes/uploads canonical
bytes on a two-slot blocking materializer pool, then stores the verified object
reference and optional Play-of-the-Game JSON on the `GAME#<id>/META` row. The
journal is deleted atomically only after the durable PersistGame effect marker
is written.
A completion containing a recording fails loudly when
`SNAKETRON_REPLAY_S3_BUCKET` is not configured; legacy completions without
recordings remain valid.

Replay recording is enabled for ordinary production matches. Server-attested
stress/bot matches (`is_stress_test`) and every process running with
`SNAKETRON_TEST_MODE=1|true|yes|on` are excluded, preventing automated test
traffic from entering production replay storage.

# Testing

The server includes a comprehensive testing framework for WebSocket functionality.

## Running Tests

```bash
# Run all server tests
cargo test -p server

# Run with output for debugging
RUST_LOG=info cargo test -p server -- --nocapture

# Run specific test
cargo test -p server test_ping_pong

# Exercise DynamoDB -> S3 -> Valkey replay wiring after starting local services
docker compose up -d localstack redis
cargo test -p server --test replay_persistence_integration_tests -- --ignored

# Run tests with shorter timeout (recommended for CI)
cargo test -p server -- --test-threads=1
```

## Test Framework Features

- **In-memory server creation**: Tests can spawn real WebSocket servers on random ports
- **Mock JWT verification**: Configurable authentication for testing different scenarios
- **Async test utilities**: Full async/await support with Tokio runtime
- **Timeout protection**: All tests have built-in timeouts to prevent hanging
- **Test client wrapper**: Simplified WebSocket client for sending/receiving messages

## Example Test

```rust
#[tokio::test]
async fn test_ping_pong() -> Result<()> {
    // Create test server with mock auth
    let server = TestServerBuilder::new()
        .with_port(0)  // Random port
        .with_mock_auth()
        .build()
        .await?;
    
    // Connect client and test
    let mut client = server.connect_client().await?;
    client.send_ping().await?;
    client.expect_pong().await?;
    
    // Cleanup
    client.disconnect().await?;
    server.shutdown().await?;
    
    Ok(())
}
```
