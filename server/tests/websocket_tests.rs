mod common;

use crate::common::{TestClient, TestEnvironment};
use anyhow::Result;
use server::ws_server::WSMessage;
use tokio::time::{Duration, timeout};
use tracing::info;

// #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(dead_code)]
async fn test_ping_pong() -> Result<()> {
    // Wrap the entire test in a timeout
    timeout(Duration::from_secs(10), async {
        // Initialize tracing for tests
        let _ = tracing_subscriber::fmt::try_init();

        info!("Starting ping/pong test");

        // Create test environment
        let mut env = TestEnvironment::new("test_ping_pong").await?;
        env.add_server().await?;

        let server_addr = env.ws_addr(0).expect("Server should exist");
        info!("Test server built, connecting client to {}", server_addr);

        // Connect a client
        let mut client = TestClient::connect(&server_addr).await?;

        info!("Client connected, sending ping");

        // Send ping
        client.send_message(WSMessage::Ping).await?;

        info!("Ping sent, expecting pong");

        // Expect pong response
        timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(msg) = client.receive_text().await {
                    if let Ok(ws_msg) = serde_json::from_str::<WSMessage>(&msg) {
                        if matches!(ws_msg, WSMessage::Pong) {
                            info!("Pong received");
                            return Ok::<(), anyhow::Error>(());
                        }
                    }
                }
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("Timeout waiting for pong"))??;

        info!("Pong received, test successful");

        // Cleanup
        client.disconnect().await?;
        env.shutdown().await?;

        Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("Test timed out after 10 seconds"))?
}

// #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(dead_code)]
async fn test_join_game_receives_snapshot() -> Result<()> {
    // Wrap the entire test in a timeout
    timeout(Duration::from_secs(10), async {
        // Initialize tracing for tests
        let _ = tracing_subscriber::fmt::try_init();

        info!("Starting join game snapshot test");

        // Create test environment with users
        let mut env = TestEnvironment::new("test_join_game_snapshot").await?;
        env.add_server().await?;
        env.create_user().await?;
        env.create_user().await?;

        let server_addr = env.ws_addr(0).expect("Server should exist");
        info!("Test server built at {}", server_addr);

        // Connect two clients to trigger matchmaking
        let mut client1 = TestClient::connect(&server_addr).await?;
        let mut client2 = TestClient::connect(&server_addr).await?;
        info!("Clients connected");

        // Authenticate the clients
        client1.authenticate(env.user_ids()[0]).await?;
        client2.authenticate(env.user_ids()[1]).await?;
        info!("Clients authenticated");

        // Queue for match
        let game_type = ::common::GameType::FreeForAll { max_players: 2 };
        client1
            .send_message(WSMessage::QueueForMatch {
                game_type: game_type.clone(),
                queue_mode: ::common::QueueMode::Quickmatch,
            })
            .await?;
        client2
            .send_message(WSMessage::QueueForMatch {
                game_type,
                queue_mode: ::common::QueueMode::Quickmatch,
            })
            .await?;
        info!("Clients queued for match");

        // Wait for automatic match and join
        let event = timeout(Duration::from_secs(30), async {
            loop {
                if let Some(event) = client1.receive_game_event().await? {
                    return Ok::<_, anyhow::Error>(event);
                }
            }
        })
        .await??;

        info!("Received event: {:?}", event);

        // Verify it's a snapshot event
        match event.event {
            ::common::GameEvent::Snapshot { game_state } => {
                // Verify the game state has expected properties
                assert_eq!(game_state.arena.width, 10);
                assert_eq!(game_state.arena.height, 10);
                assert!(!game_state.arena.snakes.is_empty());
                info!("Snapshot verified successfully");
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Expected Snapshot event, got {:?}",
                    event.event
                ));
            }
        }

        // Cleanup
        client1.disconnect().await?;
        client2.disconnect().await?;
        env.shutdown().await?;

        Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("Test timed out after 10 seconds"))?
}

// #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(dead_code)]
async fn test_authenticated_connection() -> Result<()> {
    timeout(Duration::from_secs(10), async {
        let _ = tracing_subscriber::fmt::try_init();

        info!("Starting authenticated connection test");

        // Create test environment with a user
        let mut env = TestEnvironment::new("test_authenticated_connection").await?;
        env.add_server().await?;
        env.create_user().await?;

        let server_addr = env.ws_addr(0).expect("Server should exist");

        // Connect and authenticate
        let mut client = TestClient::connect(&server_addr).await?;
        client.authenticate(env.user_ids()[0]).await?;
        info!("Client authenticated successfully");

        // Send a ping to verify connection is working
        client.send_message(WSMessage::Ping).await?;

        // Wait for pong
        timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(msg) = client.receive_text().await {
                    if let Ok(ws_msg) = serde_json::from_str::<WSMessage>(&msg) {
                        if matches!(ws_msg, WSMessage::Pong) {
                            return Ok::<(), anyhow::Error>(());
                        }
                    }
                }
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("Timeout waiting for pong"))??;

        // Cleanup
        client.disconnect().await?;
        env.shutdown().await?;

        Ok(())
    })
    .await
    .map_err(|_| anyhow::anyhow!("Test timed out after 10 seconds"))?
}
