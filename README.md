<p align="center">
  <a href="https://snaketron.io">
    <img src="client/web/SnaketronLogo.png" alt="SnakeTron" width="500">
  </a>
</p>

<p align="center">
  <a href="https://snaketron.io"><b>▶ Play now at snaketron.io</b></a>
</p>

SnakeTron is a competitive online multiplayer Snake game — real-time matches, ranked seasons, and no mercy for a mistimed turn. The game engine and the auto-scaling server architecture behind [snaketron.io](https://snaketron.io) are written in Rust and open-sourced in this repository.

## Features

- **Game modes**: Solo practice, Duel (1v1), 2v2 team matches, Free-for-All, and private Custom games with configurable arena size, tick rate, food spawn rate, and player limits
- **Matchmaking**: casual Quickmatch and ranked Competitive queues, plus lobbies with server-moderated chat and invite links
- **Combos**: every food pickup restarts a two-second timer; keep the chain alive and successive pickups are worth +1, +1, +2, and then +3 each, in both points and snake length
- **Boost**: hold-to-boost speed bursts fueled by Boost pads scattered around the arena (Solo gives you an unlimited tank to practice with)
- **Accounts**: register/login with JWT auth, or play instantly as a guest
- **Progression**: seasonal MMR with leaderboards (`/api/leaderboard`, `/api/seasons`), plus lifetime XP
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

### Game Replays

The repo includes replay-recording infrastructure (`server/src/replay/`) and sample `.replay` captures in `replays/`, though recording is not currently wired into the running server. Play the samples back in the terminal viewer:

```bash
cargo run --bin snaketron -- replays/
```

### itch.io Build

`ITCH_BUILD=true npm run build:prod` (in `client/web`) produces a relative-path HTML5 bundle suitable for uploading to itch.io.

### CrazyGames Build

`CRAZYGAMES_BUILD=true npm run build:prod` (in `client/web`) produces a relative-path HTML5 bundle that loads the CrazyGames v3 SDK before the game bundle and omits third-party analytics. Ads and CrazyGames cloud-data storage stay off unless `CRAZYGAMES_ADS_ENABLED=true` / `CRAZYGAMES_DATA_ENABLED=true` are also set at build time. See [CRAZYGAMES.md](CRAZYGAMES.md) for portal settings and the QA checklist.

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
