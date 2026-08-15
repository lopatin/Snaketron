<p align="center">
  <a href="https://snaketron.io">
    <img src="client/web/SnaketronLogo.png" alt="SnakeTron" width="500">
  </a>
</p>

<p align="center">
  <a href="https://snaketron.io"><b>▶ Play now at snaketron.io</b></a>
</p>

SnakeTron is a competitive online multiplayer Snake game — real-time matches, ranked seasons, and no mercy! The game engine and the auto-scaling server architecture behind [snaketron.io](https://snaketron.io) are written in Rust and open-sourced in this repository.

## Features

- **Game modes**: Solo practice, Duel (1v1), 2v2 team matches, Free-for-All, and private Custom games with configurable arena size, tick rate, food spawn rate, and player limits
- **Matchmaking**: casual Quickmatch and ranked Competitive queues, plus lobbies with server-moderated chat and invite links
- **Combos**: every food pickup restarts a two-second timer; keep the chain alive and successive pickups are worth +1, +1, +2, and then +3 each, in both points and snake length
- **Boost**: hold-to-boost speed bursts fueled by Boost pads scattered around the arena (Solo gives you an unlimited tank to practice with)
- **Accounts**: register/login with JWT auth, or play instantly as a guest
- **Progression**: seasonal MMR with leaderboards (`/api/leaderboard`, `/api/seasons`), plus lifetime XP
- **Server-controlled ads**: provider-neutral banner placements and a lobby-wide pre-match video barrier, disabled by default
- **Netcode**: the game engine is shared between server and client, enabling client-side prediction with server authority

## Architecture

- **Backend**: Rust server (Tokio + axum) serving the REST API and WebSocket connections on a single HTTP port
- **Cluster coordination**: Redis (Valkey) — server membership and heartbeats, partition assignment with fenced leases, and Redis Streams as the game event/command bus. The server running a game's loop is not necessarily the WebSocket server its players are connected to.
- **Persistence**: AWS DynamoDB — a single-table-style main table with GSIs, plus small auxiliary tables for username uniqueness and game codes; LocalStack stands in for it in local development
- **Frontend**: React + TypeScript consuming a Rust game engine compiled to WebAssembly (wasm-pack), bundled with webpack
- **Shared game logic**: the `common/` crate compiles to both native (server) and WASM (client)
- **Infrastructure**: Docker containers, designed for AWS Fargate deployment

## Quick Start

Prerequisites: Rust (stable), [wasm-pack](https://rustwasm.github.io/wasm-pack/), Node.js, and Docker.

### Using Docker (Recommended)

#### Development (hot reloading)
```bash
# Start LocalStack (DynamoDB), Redis (Valkey), and the server with auto-reload on code changes
./dev.sh

# In another terminal, build and start the client
cd client
wasm-pack build --target web --out-dir pkg
cd web
npm install
npm start
```

#### Production-like
```bash
# Start LocalStack (DynamoDB), Redis (Valkey), and the server (full rebuild each time)
docker-compose up --build
```

This starts only the backend stack — build and start the client in another terminal with the same commands as in development.

The game will be available at:
- Frontend: http://localhost:3000 (webpack dev server)
- HTTP API + WebSocket: localhost:8080 (WebSocket endpoint at ws://localhost:8080/ws)
- DynamoDB (LocalStack): http://localhost:4566
- Redis (Valkey): localhost:6379

### Manual Setup

1. Start the data services (LocalStack + Redis) and create the DynamoDB tables:
   ```bash
   ./test-deps.sh
   ```

2. Run the server (`.cargo/config.toml` supplies the LocalStack/Redis defaults; only the region must be set explicitly):
   ```bash
   SNAKETRON_REGION=us cargo run --bin server
   ```

3. Build and run the client:
   ```bash
   cd client
   wasm-pack build --target web --out-dir pkg
   cd web
   npm install
   npm start
   ```

## Development

### Running Tests

Server integration tests need Redis and LocalStack DynamoDB running — start them with `./test-deps.sh` first (it also creates the DynamoDB tables).

```bash
# Run all Rust tests
cargo test

# Run server tests with logging
RUST_LOG=info cargo test -p server -- --nocapture

# Curated serial suites (set up their own env, run single-threaded)
./run_matchmaking_tests.sh
./run_quickmatch_tests.sh
```

Client tests and checks:

```bash
cd client/web
npm test            # Playwright end-to-end tests
npm run test:unit   # Node unit tests
npm run type-check  # TypeScript type check
```

### Code Quality

PRs are welcome. CI requires clean formatting and a warning-free clippy pass on every PR (mirrored as a deploy gate):

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

### Generated TypeScript Types

TypeScript types for everything crossing the WebSocket are generated from the Rust source of truth with ts-rs. After changing any wire type, regenerate them and commit the diff:

```bash
./scripts/gen-types.sh
```

### Advertisement Configuration

See [the advertising design](docs/advertising-design.md) for the authority and
lobby-state diagrams plus desktop, mobile, fallback, and admin screenshots.

Advertisement capability is resolved by the server at startup and sent to each
browser session. The browser reports which distribution build it is running
(`web`, `crazygames`, or `itch`); the server maps that distribution to its
configured provider and placements. Client build flags only make an SDK adapter
available. Live pre-match authorization, distribution targeting, game-count
eligibility, and frequency are stored in the versioned DynamoDB runtime config
managed at `/admin`. Invalid deployment values fail server startup.

| Variable | Default | Meaning |
| --- | --- | --- |
| `SNAKETRON_ADS_ENABLED` | `false` | Deployment capability kill switch. When false, every placement is disabled regardless of runtime policy. |
| `SNAKETRON_ADS_<DISTRIBUTION>_PROVIDER` | `none` | Adapter key for `WEB`, `CRAZYGAMES`, or `ITCH`. A `none` distribution stays ad-free even when the global switch is on. |
| `SNAKETRON_ADS_<DISTRIBUTION>_BOTTOM_BANNER_ENABLED` | distribution enabled | Show the horizontal bottom placement for that distribution. |
| `SNAKETRON_ADS_<DISTRIBUTION>_SIDE_BANNERS_ENABLED` | distribution enabled | Show desktop side placements for that distribution; mobile clients omit them. |
| `SNAKETRON_ADS_<DISTRIBUTION>_PRE_MATCH_VIDEO_ENABLED` | distribution enabled | Let that distribution receive video during a lobby-wide break. |
| `SNAKETRON_AD_BREAK_TIMEOUT_SECONDS` | `120` | Server safety deadline for the lobby barrier; valid range is 5–300 seconds. Providers skip submission unless enough lifecycle budget remains. |
| `SNAKETRON_MATCHMAKING_QUEUE_LEASE_ENFORCEMENT` | `true` | Reject and reap queued generations after five minutes without a member heartbeat. Set this to `false` only during the first phase of an upgrade from a pre-v8 fleet. |

For one server that serves the website, CrazyGames, and itch.io simultaneously:

```bash
SNAKETRON_ADS_ENABLED=true \
SNAKETRON_ADS_WEB_PROVIDER=none \
SNAKETRON_ADS_CRAZYGAMES_PROVIDER=crazygames \
SNAKETRON_ADS_ITCH_PROVIDER=none \
cargo run --bin server
```

Once a website H5 adapter is registered, set
`SNAKETRON_ADS_WEB_PROVIDER` to its adapter key. itch.io can remain `none`
without disabling ads for the other distributions. Placement switches may be
set independently for each distribution after the global switch is on. The
old scalar `SNAKETRON_ADS_PROVIDER` is intentionally unsupported because a
shared server cannot route one provider correctly to every build.

Pre-match video remains disabled until an administrator enables the runtime
advertising master switch and the intended `web`, `crazygames`, or `itch`
distribution toggles. That same record owns the 0–10,000 minimum-games
threshold and the 1–1,440 minute per-user interval. Every lobby member must
meet the game threshold; every targeted member must clear the durable interval
or the whole lobby skips the break. Provider IDs and the break timeout remain
deployment capabilities and cannot be enabled from the admin page.

Distribution reporting is part of gameplay protocol v9. Older authentication
payloads remain accepted, but receive a disabled ad configuration because the
server cannot safely infer a build from an account or token. Keep the global
switch off during a v9 client rollout; enable it only after the intended web
and portal builds are reporting their distribution.

Roll this protocol out in two phases. For an upgrade from a pre-v8 fleet, first
deploy the protocol v9 binary to every gateway, matcher, and completion executor with
`SNAKETRON_ADS_ENABLED=false` and
`SNAKETRON_MATCHMAKING_QUEUE_LEASE_ENFORCEMENT=false`. New tasks still write
and refresh queue leases, while mixed-fleet matchers continue accepting legacy
queue records. Pause new matchmaking admissions for the cutover, let the queue
drain to zero, and drain every old task and connection; a pre-v8 admission
retry cannot observe the v8 cancellation fence. Then set queue-lease
enforcement to `true` and optionally enable ads in the second configuration
rollout. Fresh deployments should retain the `true` default.
Older binaries cannot participate in the v8 lobby fence or completion counter;
the disabled-by-default first phase makes any rollout-window undercount
conservative (players skip ads longer) rather than exposing a newcomer.
`gamesPlayed` has an explicit v8 baseline: existing rows without the attribute
are treated as zero, and completions whose legacy idempotency effect already
won during the mixed-fleet phase are not replayed. After every completion
executor is on the new binary, each new completion advances the durable counter exactly
once. This intentionally requires historical players to complete the configured
number of post-rollout games before becoming eligible; no historical totals are
guessed or backfilled.

### Game Replays

The repo includes replay-recording infrastructure (`server/src/replay/`) and sample `.replay` captures in `replays/`, though recording is not currently wired into the running server. Play the samples back in the terminal viewer:

```bash
cargo run --bin snaketron -- replays/
```

### itch.io Build

`ITCH_BUILD=true npm run build:prod` (in `client/web`) produces a relative-path HTML5 bundle suitable for uploading to itch.io.

### CrazyGames Build

`CRAZYGAMES_BUILD=true npm run build:prod` (in `client/web`) produces a relative-path HTML5 bundle that loads the CrazyGames v3 SDK before the game bundle and omits third-party analytics. The build makes the CrazyGames ad adapter available; the server-side advertisement configuration above decides at runtime whether it is used. CrazyGames cloud-data storage stays off unless `CRAZYGAMES_DATA_ENABLED=true` is explicitly set at build time. See [CRAZYGAMES.md](CRAZYGAMES.md) for portal settings and the QA checklist.

### Autoscaling Load Tests

Point a coordinated fleet of AI players at a cluster and make it sweat:

```bash
cargo run --release -p loadtest -- \
  --target https://snaketron.io \
  --confirm-production \
  --require-scale-out \
  --mode duel \
  --queue-mode competitive
```

The load runner supports Solo, Duel, 2v2, and FFA; creates deterministic full-party multiplayer lobbies; plays real games using the shared Rust game engine and AI; ramps to 256 maintained sessions by default; and writes an HTML/JSON report with per-failure details. See [loadtest/README.md](loadtest/README.md) for profiles, safety controls, and report semantics.

### Project Structure

- `common/` - Shared game logic (compiled to both native and WASM)
- `server/` - Game server: WebSocket sessions, matchmaking, game executors, persistence
- `client/` - WebAssembly client module and the React/TypeScript web app (`client/web/`)
- `bot/` - CLI that runs one or more AI bots against a live server over WebSocket
- `macros/` - Proc-macro crate defining a `serde_wasm_bindgen` attribute (not currently used by other crates)
- `terminal/` - Terminal-based replay player for `.replay` captures
- `loadtest/` - Coordinated AI load generator and aggregate reporting
- `replays/` - Sample `.replay` game captures for the terminal viewer
- `scripts/` - Development helpers (type generation, DynamoDB init, test dependencies)
- `specs/` - Design documents and PRDs (matchmaking, Boost, autoscaling resilience, ...)
- `tla_specs/` - TLA+ specifications (model-check them with `tla2tools.jar` at the repo root)
- `docs/` - Screenshots, pull-request assets, and assorted design notes

### Further Documentation

- [DEBUGGING.md](DEBUGGING.md) - Runbook for diagnosing state-synchronization bugs (traces, TickHash, replaying to a local repro)
- [CRAZYGAMES.md](CRAZYGAMES.md) - CrazyGames portal integration: build flags, pilot scope, and QA checklist
- [loadtest/README.md](loadtest/README.md) - Load test profiles, safety controls, and report semantics

## Production Deployment

See [server/docker-readme.md](server/docker-readme.md) for detailed Docker and AWS Fargate deployment instructions.

## License

MIT — see [LICENSE](LICENSE).
