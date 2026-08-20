# Docker Setup for SnakeTron Server

## Local Development

### Quick Start

```bash
# Build and start both database and server
docker-compose up --build

# Or run in detached mode
docker-compose up -d --build

# View logs
docker-compose logs -f server

# Stop services
docker-compose down
```

### Accessing the Server

- WebSocket: `ws://localhost:8080`
- gRPC: `localhost:50051`
- Database: `localhost:5432`

### Volumes

- Replays are saved to `./replays` directory (mounted as volume)

## Production Deployment (AWS Fargate)

### Build and Push to ECR

```bash
# Build production image
docker build -f server/Dockerfile -t snaketron-server .

# Tag for ECR
docker tag snaketron-server:latest ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/snaketron-server:latest

# Push to ECR
docker push ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/snaketron-server:latest
```

### Environment Variables for Fargate

Required environment variables:
- `SNAKETRON_DB_HOST`: RDS endpoint
- `SNAKETRON_DB_PORT`: Database port (5432)
- `SNAKETRON_DB_USER`: Database username
- `SNAKETRON_DB_PASS`: Database password
- `SNAKETRON_DB_NAME`: Database name
- `SNAKETRON_REGION`: AWS region
- `SNAKETRON_HTTP_PORT`: HTTP/WebSocket port (8080)
- `SNAKETRON_GRPC_PORT`: gRPC port (50051)

Advertisement variables are optional and server-owned. Omitting them keeps all
ads disabled:

| Variable | Default | Notes |
| --- | --- | --- |
| `SNAKETRON_ADS_ENABLED` | `false` | Deployment capability kill switch; false overrides every placement and runtime switch. |
| `SNAKETRON_ADS_<DISTRIBUTION>_PROVIDER` | `none` | Adapter key for `WEB`, `CRAZYGAMES`, or `ITCH`; `none` keeps only that distribution ad-free. |
| `SNAKETRON_ADS_<DISTRIBUTION>_BOTTOM_BANNER_ENABLED` | distribution enabled | Bottom banner on mobile and desktop for that distribution. |
| `SNAKETRON_ADS_<DISTRIBUTION>_SIDE_BANNERS_ENABLED` | distribution enabled | Side banners on desktop for that distribution. |
| `SNAKETRON_ADS_<DISTRIBUTION>_PRE_MATCH_VIDEO_ENABLED` | distribution enabled | Video participation before lobby matchmaking for that distribution. |
| `SNAKETRON_AD_BREAK_TIMEOUT_SECONDS` | `120` | Barrier safety deadline; valid range 5–300 seconds. Clients skip SDK submission when the remaining provider lifecycle budget is too short. |
| `SNAKETRON_MATCHMAKING_QUEUE_LEASE_ENFORCEMENT` | `true` | Enforce the five-minute queued-lobby heartbeat lease. Use `false` only for phase 1 of a pre-v8 rolling upgrade. |

Each distribution's placement switches inherit whether that distribution is
enabled (the global switch is on and its provider is not `none`). A single
shared task can therefore route different policies to each client build:

```text
SNAKETRON_ADS_ENABLED=true
SNAKETRON_ADS_WEB_PROVIDER=none
SNAKETRON_ADS_CRAZYGAMES_PROVIDER=crazygames
SNAKETRON_ADS_ITCH_PROVIDER=none
```

Do not put SDK secrets or placement IDs in these generic variables. Provider
credentials and SDK-specific settings belong to that provider's deployment
integration. The old scalar `SNAKETRON_ADS_PROVIDER` is unsupported because it
cannot describe a shared website/portal server.

The versioned DynamoDB runtime config is the live authority for pre-match video
ads. Its safe default disables ads for every distribution. Administrators can
enable the global policy and individual `web`, `crazygames`, and `itch`
targets, set the all-members minimum-games threshold (0–10,000), and set the
durable per-target interval (1–1,440 minutes). A runtime toggle cannot exceed
the provider and placement capabilities above. Banners remain deployment-owned
placements and are never authorized by browser code.

Protocol v9 clients report their distribution during WebSocket authentication.
Legacy payloads are still admitted but receive a disabled ad configuration;
keep the global switch off until the intended v9 browser builds are deployed.

Advertisement enablement and queue-lease enforcement are second-phase rollout
settings. During an upgrade from pre-v8, deploy the protocol v9 binary to all gateway,
matcher, and completion-executor tasks with the global ad switch off and
`SNAKETRON_MATCHMAKING_QUEUE_LEASE_ENFORCEMENT=false`. The new fleet dual-writes
and refreshes leases without rejecting legacy queue records. Pause new
matchmaking admissions, wait for the queue to drain to zero, and drain all
older tasks and connections before enabling queue-lease enforcement and ads as
desired. This maintenance boundary prevents a pre-v8 admission retry from
bypassing a v8 cancellation fence.
Fresh deployments retain the `true` lease-enforcement default. A mixed fleet
contains old Lua admission code that cannot honor the v8 lobby barrier; keeping
ads disabled during that interval also makes any completion-counter undercount
fail safely by delaying eligibility.

The counter starts from an explicit v8 baseline. Pre-v8 users with no
`gamesPlayed` attribute are zero, and a completion already claimed by a legacy
executor is not replayed merely to populate the new projection. Once the fleet
is fully on the new binary, new completions increment it exactly once. Consequently,
historical users must finish the configured number of post-rollout games before
ads become eligible; operators should not infer or backfill unknown totals.

Redis/Valkey cluster-node clocks must remain synchronized; ad admission
subtracts a 2-second cross-slot lease allowance. Alert before absolute node
clock skew approaches that bound, because membership leases and matchmaking
metadata intentionally retain their historical rolling-compatible hash slots.

### Task Definition Configuration

- Memory: 512 MB (minimum)
- CPU: 256 units (0.25 vCPU)
- Network mode: awsvpc
- Exposed ports: 8080 (WebSocket), 50051 (gRPC)

### Health Check

The server exposes WebSocket on port 8080. For ALB health checks, you may need to implement an HTTP health endpoint.

## Troubleshooting

### Database Connection Issues

If the server can't connect to the database:
1. Check that the database container is healthy: `docker-compose ps`
2. Verify environment variables are correct
3. Ensure the database has been initialized with migrations

### Build Issues

If the build fails:
1. Ensure you have sufficient disk space
2. Try cleaning Docker cache: `docker system prune`
3. Check that all Rust dependencies are available

### Performance Tuning

For production:
- Adjust `RUST_LOG` level (default: info)
- Configure appropriate CPU/memory limits
- Use AWS RDS for database with proper instance size
