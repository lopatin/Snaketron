mod common;

use anyhow::Result;
use tokio::time::Duration;
use tracing::info;
use crate::common::{TestServerBuilder, MockJwtVerifier};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_ping_pong() -> Result<()> {
    // Wrap the entire test in a timeout
    tokio::time::timeout(Duration::from_secs(10), async {
        // Initialize tracing for tests
        let _ = tracing_subscriber::fmt::try_init();
        
        info!("Starting ping/pong test");
        
        // Create and start test server
        let server = TestServerBuilder::new()
            .with_port(0)  // Random available port
            .with_mock_auth()  // Accept any token
            .build()
            .await?;
        
        info!("Test server built, connecting client to {}", server.addr);
        
        // Connect a client
        let mut client = server.connect_client().await?;
        
        info!("Client connected, sending ping");
        
        // Send ping
        client.send_ping().await?;
        
        info!("Ping sent, expecting pong");
        
        // Expect pong response
        client.expect_pong().await?;
        
        info!("Pong received, test successful");
        
        // Cleanup
        client.disconnect().await?;
        server.shutdown().await?;
        
        Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("Test timed out after 10 seconds"))?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_join_game_receives_snapshot() -> Result<()> {
    // Wrap the entire test in a timeout
    tokio::time::timeout(Duration::from_secs(10), async {
        // Initialize tracing for tests
        let _ = tracing_subscriber::fmt::try_init();
        
        info!("Starting join game snapshot test");
        
        // Create and start test server
        let server = TestServerBuilder::new()
            .with_port(0)  // Random available port
            .with_mock_auth()  // Accept any token
            .build()
            .await?;
        
        info!("Test server built at {}", server.addr);
        
        // Create a game with unique ID to avoid conflicts when tests run in parallel
        let test_game_id = (rand::random::<u16>() as u32) + 400000;
        server.create_game(test_game_id).await?;
        info!("Game {} created", test_game_id);
        
        // Give the game loop time to start
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Connect a client
        let mut client = server.connect_client().await?;
        info!("Client connected");
        
        // Authenticate the client
        client.authenticate(1).await?;
        info!("Client authenticated");
        
        // Join the game
        client.join_game(test_game_id).await?;
        info!("Join game message sent");
        
        // Receive the snapshot event
        let event = client.receive_game_event().await?;
        let event = event.ok_or_else(|| anyhow::anyhow!("Expected game event, got None"))?;
        info!("Received event: {:?}", event);
        
        // Verify it's a snapshot event
        match event.event {
            ::common::GameEvent::Snapshot { game_state } => {
                // Verify the game state has expected properties
                assert_eq!(game_state.arena.width, 10);
                assert_eq!(game_state.arena.height, 10);
                assert_eq!(event.game_id, test_game_id);
                info!("Snapshot verified successfully");
            }
            _ => {
                return Err(anyhow::anyhow!("Expected Snapshot event, got {:?}", event.event));
            }
        }
        
        // Cleanup
        client.disconnect().await?;
        server.shutdown().await?;
        
        Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("Test timed out after 10 seconds"))?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_authenticated_connection() -> Result<()> {
    // Wrap the entire test in a timeout
    tokio::time::timeout(Duration::from_secs(10), async {
        // Initialize tracing for tests
        let _ = tracing_subscriber::fmt::try_init();
        
        info!("Starting authenticated connection test");
        
        // Create a mock JWT verifier that expects a specific token
        let jwt_verifier = std::sync::Arc::new(
            MockJwtVerifier::new().with_token("valid_token", 42)
        );
        
        // Create and start test server with custom JWT verifier
        let server = TestServerBuilder::new()
            .with_port(0)
            .with_jwt_verifier(jwt_verifier)
            .build()
            .await?;
        
        info!("Test server built at {}", server.addr);
        
        // Connect a client
        let mut client = server.connect_client().await?;
        info!("Client connected");
        
        // Authenticate with the token that the mock verifier expects
        // The mock verifier is configured with token "valid_token" -> user_id 42
        client.authenticate_with_token("valid_token").await?;
        info!("Client authenticated with valid token");
        
        // Test that authenticated client can send ping
        client.send_ping().await?;
        client.expect_pong().await?;
        info!("Authenticated ping/pong successful");
        
        // Cleanup
        client.disconnect().await?;
        server.shutdown().await?;
        
        Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("Test timed out after 10 seconds"))?
}