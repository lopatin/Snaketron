# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture Overview

SnakeTron is a multiplayer Snake game built with a Rust backend and WebAssembly frontend. The architecture prioritizes auto-scalability, resilience, and infrastructure simplicity.

### Core Components

1. **Common Library** (`common/`)
   - Shared game logic between server and client
   - Contains `GameEngine`, `GameState`, `Snake`, and `Arena` modules
   - Enables client-side prediction and server authority

2. **Server** (`server/`)
   - Authoritative game server built on Tokio async runtime
   - WebSocket connections for real-time gameplay
   - gRPC for inter-server communication in cluster
   - Raft for game state replication
   - PostgreSQL database for persistence
   - Designed for horizontal scaling with automatic failover

3. **Client** (`client/`)
   - WebAssembly module compiled from Rust
   - React frontend consuming WASM functions
   - Canvas-based rendering system

### Key Architectural Decisions

- **Service Discovery**: Servers register themselves in the database and send heartbeats. This is used for discovering Raft peers.
- **Real-time Communication**: WebSocket for client-server, gRPC streaming for server-server.
- **Raft**: Game and server state is replicated using Raft. This enables automatic failover and leader election.
- **Shared Game Logic**: Common crate compiled to both native (server) and WASM (client) enables consistent game behavior and client-side prediction.
- **Decoupling**: The server which is running the game loop is not necessarily the same server that is running the WebSocket server that the game client connects to.
- **Database Schema**: Well-structured tables for servers, users, games, and matchmaking with proper indexes for performance.

### Development Workflow

1. Start the PostgreSQL database first
2. Run database migrations (automatic on server start via Refinery)
3. Start the server which will register itself and begin accepting connections
4. Build the client WASM package and start the webpack dev server
5. The client connects via WebSocket to the server for gameplay

## Project Structure
- The _old directory can be fully ignored for all purposes

## Development Notes
- When developing migrations, just update the V1 migration, instead of adding a new one. And then, when necessary to run tests, clear / restart the db docker container before running them if necessary.
- When modifying Snake position vectors, always remember to use the compressed Snake representation where the snake vector contains the head, turns, and tail, rather than every point of the logical snake. The logic is described in the code at @common/src/snake.rs in the `step_forward` function. For example, a straight snake of length 5 should have only 2 positions in the body vector, not 5.

## Commands

### Database
```bash
# Start PostgreSQL database
docker-compose up -d

# Stop database
docker-compose down
```

### Server Development
```bash
# Run the server (from root directory)
cargo run --bin server

# Build server
cargo build --bin server --release

# Run server tests
cargo test -p server

# Run tests with debug output
RUST_LOG=info cargo test -p server -- --nocapture

# Run specific test
cargo test -p server test_ping_pong

# Watch for changes and rebuild
cargo watch -x "run --bin server"
```

### Testing

The server includes a comprehensive WebSocket testing framework:

- **Test Utilities**: Located in `server/src/ws_server.rs::test_utils`
- **Mock JWT Verifier**: Allows testing without real authentication
- **Test Server Builder**: Creates in-memory servers on random ports
- **Test Client**: Simplified WebSocket client for testing
- **Timeout Protection**: All tests have 10-second timeouts to prevent hanging

Example test pattern:
```rust
#[tokio::test]
async fn test_websocket_functionality() -> Result<()> {
    let server = TestServerBuilder::new()
        .with_mock_auth()
        .build()
        .await?;
    
    let mut client = server.connect_client().await?;
    // Test your WebSocket functionality here
    
    client.disconnect().await?;
    server.shutdown().await?;
    Ok(())
}
```

### Client Development
```bash
# Build WASM package (from client directory)
cd client && wasm-pack build --target web --out-dir pkg

# Install web dependencies (from client/web directory)
cd client/web && npm install

# Start development server with hot reload
cd client/web && npm start

# Build production bundle
cd client/web && npm run build
```

### Full Project Build
```bash
# Build all Rust components
cargo build --all

# Run all tests
cargo test --all
```

### Development Best Practices
- Make sure that `cargo test` passes before committing any changes

### Testing Guidelines
- Server integration tests should not access the DB directly, they should emulate what the main method would do on servers, which do their own maintenance rather than needing it to be in a test harness.
```