# SnakeTron

A competitive online multiplayer Snake game built with Rust (backend + WebAssembly frontend).

## Architecture

- **Backend**: Rust server with WebSocket connections, gRPC for inter-server communication, and Raft consensus
- **Frontend**: React + WebAssembly (compiled from Rust)
- **Database**: PostgreSQL with automatic migrations
- **Cache/Leader Election**: Redis for optional leader election and distributed coordination
- **Infrastructure**: Docker containers, designed for AWS Fargate deployment

## Quick Start

### Using Docker (Recommended)

#### For Development (with hot reloading):
```bash
# Start database and server with auto-reload on code changes
./dev.sh

# The web client is automatically built and served by the Rust server
# Access the game at http://localhost:3001
```

#### For Production-like environment:
```bash
# Start database and server (includes web client build)
docker-compose up --build
```

The game will be available at:
- Full Application: http://localhost:3001 (API + static files)
- WebSocket Server: ws://localhost:8080
- gRPC Server: localhost:50051 (Docker container)
- Database: localhost:5432 (Docker container)
- Redis: localhost:6379 (Docker container)

### Manual Setup

1. Start PostgreSQL and Redis:
   ```bash
   docker-compose up -d db redis
   ```

2. Build the web client:
   ```bash
   cd client
   wasm-pack build --target web --out-dir pkg
   cd web
   npm install
   npm run build
   ```

3. Run the server (which serves the web client):
   ```bash
   # Set the web directory to serve the built files
   export SNAKETRON_WEB_DIR=client/web/dist
   cargo run --bin server
   ```

The application will be available at http://localhost:3001

## Development

### Running Tests

```bash
# Run all tests
cargo test

# Run server tests with logging
RUST_LOG=info cargo test -p server -- --nocapture
```

### Project Structure

- `common/` - Shared game logic (compiled to both native and WASM)
- `server/` - Game server with WebSocket and gRPC support
- `client/` - WebAssembly client module
- `terminal/` - Terminal-based game viewer and replay player
- `specs/` - Design documents and specifications

## Production Deployment

See [server/docker-readme.md](server/docker-readme.md) for detailed Docker and AWS Fargate deployment instructions.

## License

MIT
