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
- `SNAKETRON_WS_PORT`: WebSocket port (default: 8080)
- `SNAKETRON_GRPC_PORT`: gRPC port (default: 50051)
- `SNAKETRON_REGION`: Server region identifier

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