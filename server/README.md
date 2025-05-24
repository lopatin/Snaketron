# Snaketron Server Overview

The Snaketron server is a Rust application that has the following responsibilities:

- Runs the authoritative game loop
- Runs the WebSocket and REST API servers
- Clusters with other servers for high availability
- Runs matchmaking and other periodic system tasks

# Architecture
Auto-scalability and resiliency is a key design goal of the Snaketron server, which is achieved using clustering and auto fail-over of the game state. Infrastructure simplicity is also a goal. It should be a simple binary with no external dependencies other than the RDS master database.

The server will be deployed as a Rust binary inside a Docker container in an AWS auto-scaling group via Elastic Beanstalk. Every server will have the following components:

## Game Manager
The GameManager holds actively running GameState instances which are assigned to the local server.

## WebSocket Server
Clients will connect to this server to send commands and receive game update events. It will interact with the game instance in the GameManager on behalf of the user.

## Service Manager

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