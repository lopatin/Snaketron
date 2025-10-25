# CODEX.md

This file guides ChatGPT/Codex when working on the Snaketron repository. The intent is to mirror the context provided to Claude while highlighting the conventions and tooling expectations for this environment.

## Architecture Snapshot
- **common/**: Shared Rust crate containing the game engine (`GameEngine`, `GameState`, `Snake`, `Arena`). Compiles to both native code and WASM for client-side prediction with server authority.
- **server/**: Tokio-based authoritative server exposing WebSocket endpoints for players and Redis/Valkey for intra-cluster communication. Relies on Valkey for coordination and DynamoDB for persistence. Designed for horizontal scaling where the WebSocket host and game loop host can differ.
- **client/**: Rust-to-WASM core plus a React/TypeScript frontend. Uses canvas rendering and consumes WASM bindings.
- **_old/**: Legacy code; ignore for all tasks.

Key architectural decisions:
- Service discovery and health: servers self-register in the DB and emit heartbeats.
- Real-time links: WebSocket for clients; Valkey pub/sub for server-to-server.
- Cluster coordination: Valkey-backed singleton management for matchmaking and load distribution.
- Game logic parity: the `common` crate ensures server and client stay in sync.
- Containerization: Docker workflows for both development and production (AWS Fargate target).

## Working Agreements
- Respect the compressed snake representation in `common/src/snake.rs::step_forward`. Straight snakes of length *n* store only head, turns, tail.
- Never touch `_old/`.

## Recommended Commands
### Docker
```bash
./dev.sh                                  # bring up dev stack with hot reload
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up
docker-compose up --build                 # production-style rebuild
docker-compose up -d --build              # detached
docker-compose logs -f server             # tail server logs
docker-compose down                       # stop all services
docker-compose up -d db                   # database only
```

### Server (Rust)
```bash
cargo run --bin server
cargo build --bin server --release
cargo test -p server
RUST_LOG=info cargo test -p server -- --nocapture
cargo test -p server test_ping_pong       # specific test
cargo watch -x "run --bin server"
```

### Client (WASM + Web)
```bash
cd client && wasm-pack build --target web --out-dir pkg
cd client/web && npm install
cd client/web && npm start
cd client/web && npm run build
cd client/web && npx tsc --noEmit
```

### Whole Project
```bash
cargo build --all
cargo test --all
cargo clippy --all-targets --all-features
cargo fmt --all
cargo fmt --all -- --check
```

## Testing Notes
- Server integration tests live in `server/src/ws_server.rs::test_utils`. They offer mock JWT verification, in-memory server builders, simplified WebSocket clients, and 10-second timeouts to avoid hangs.
- Tests that depend on Redis/Dynamo or other services should rely on `test-deps.sh`.
- Integration tests should stand up the server similarly to production rather than bypassing initialization logic.

## Workflow Tips for ChatGPT/Codex
- Prefer `rg` for source searches and `cargo`/`npm`/`docker` commands from the repo root. Always set the working directory in tool calls.
- Observe existing formatting and run `cargo fmt`, `npm run lint`, or related formatters only when necessary or explicitly requested.
- Be mindful of the workspace's instructions around migrations, snake representation, and avoiding `_old/`.
- Summaries in responses should reference concrete files and line numbers (e.g., `server/src/foo.rs:42`) when describing changes.

## Deployment Context
- Development: `docker-compose` orchestrates Postgres, Redis, and service containers.
- Production: Containers aim at AWS Fargate; keep infrastructure changes compatible with Fargate expectations.

Keep this document handy when triaging new tasks—align changes with the architectural notes and workflows above, and lean on the common crate for shared logic.
