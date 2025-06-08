# SnakeTron

A competitive online multiplayer Snake game built with Rust (backend + WebAssembly frontend).

## Architecture

- **Backend**: Rust server with WebSocket connections, gRPC for inter-server communication, and Raft consensus
- **Frontend**: React + WebAssembly (compiled from Rust)
- **Database**: PostgreSQL with automatic migrations
- **Infrastructure**: Docker containers, designed for AWS Fargate deployment

## Quick Start

### Using Docker (Recommended)

```bash
# Start database and server
docker-compose up --build

# In another terminal, build and start the client
cd client
wasm-pack build --target web --out-dir pkg
cd web
npm install
npm start
```

The game will be available at:
- Frontend: http://localhost:3000 (webpack dev server)
- WebSocket Server: ws://localhost:8080 (Docker container)
- gRPC Server: localhost:50051 (Docker container)
- Database: localhost:5432 (Docker container)

### Manual Setup

1. Start PostgreSQL:
   ```bash
   docker-compose up -d db
   ```

2. Run the server:
   ```bash
   cargo run --bin server
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
- `specs/` - TLA+ specifications for distributed systems design

## Production Deployment

See [server/docker-readme.md](server/docker-readme.md) for detailed Docker and AWS Fargate deployment instructions.

## License

MIT
