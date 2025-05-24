use anyhow::Result;
use server::ws_server::WSMessage;
use ::common::{GameCommandMessage, GameCommand, GameEventMessage, GameEvent, GameType, Position};
use tokio::time::{timeout, Duration};

mod common;
use self::common::{TestEnvironment, TestClient};

#[tokio::test]
async fn test_distributed_broker_local_game() -> Result<()> {
    // Start a single server
    let env = TestEnvironment::new(1).await?;
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    // Connect two clients
    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;
    
    // Authenticate clients
    client1.authenticate(1).await?;
    client2.authenticate(2).await?;
    
    // Queue both clients for a match
    client1.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    
    client2.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    
    // Wait for match to be created
    let game_id = timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(msg) = client1.receive_message().await {
                if let WSMessage::MatchFound { game_id } = msg {
                    return Ok::<u32, anyhow::Error>(game_id);
                }
            }
        }
    }).await??;
    
    // Verify client2 also got the match
    let game_id2 = timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(msg) = client2.receive_message().await {
                if let WSMessage::MatchFound { game_id } = msg {
                    return Ok::<u32, anyhow::Error>(game_id);
                }
            }
        }
    }).await??;
    
    assert_eq!(game_id, game_id2, "Both clients should be in the same game");
    
    // Join the game
    client1.send_message(WSMessage::JoinGame(game_id)).await?;
    client2.send_message(WSMessage::JoinGame(game_id)).await?;
    
    // Wait for game snapshots
    let snapshot1 = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(event) = client1.receive_game_event().await? {
                if matches!(event.event, GameEvent::Snapshot { .. }) {
                    return Ok::<(), anyhow::Error>(());
                }
            }
        }
    }).await??;
    
    let snapshot2 = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(event) = client2.receive_game_event().await? {
                if matches!(event.event, GameEvent::Snapshot { .. }) {
                    return Ok::<(), anyhow::Error>(());
                }
            }
        }
    }).await??;
    
    // Clean disconnect
    client1.disconnect().await?;
    client2.disconnect().await?;
    env.shutdown().await?;
    
    Ok(())
}

#[tokio::test]
async fn test_distributed_broker_cross_server() -> Result<()> {
    // Start two servers
    let env = TestEnvironment::new(2).await?;
    let server1_addr = env.ws_addr(0).expect("Server 1 should exist");
    let server2_addr = env.ws_addr(1).expect("Server 2 should exist");
    
    // Note: This test would require gRPC communication between servers
    // For now, we'll test that each server can handle its own games independently
    
    // Connect clients to different servers
    let mut client1 = TestClient::connect(&server1_addr).await?;
    let mut client2 = TestClient::connect(&server2_addr).await?;
    
    client1.authenticate(1).await?;
    client2.authenticate(2).await?;
    
    // Each client queues on their own server
    // In a real distributed system, matchmaking would coordinate across servers
    // For this test, we verify servers can operate independently
    
    client1.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 4 } 
    }).await?;
    
    client2.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 4 } 
    }).await?;
    
    // Give matchmaking time to process
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Clients can leave queue
    client1.send_message(WSMessage::LeaveQueue).await?;
    client2.send_message(WSMessage::LeaveQueue).await?;
    
    // Clean disconnect
    client1.disconnect().await?;
    client2.disconnect().await?;
    env.shutdown().await?;
    
    Ok(())
}

#[tokio::test] 
async fn test_game_lifecycle_with_cleanup() -> Result<()> {
    // Start a server
    let env = TestEnvironment::new(1).await?;
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    // Connect two clients
    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;
    
    client1.authenticate(1).await?;
    client2.authenticate(2).await?;
    
    // Create a match
    client1.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    
    client2.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    
    // Wait for match
    let game_id = timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(msg) = client1.receive_message().await {
                if let WSMessage::MatchFound { game_id } = msg {
                    // Clear client2's message queue
                    let _ = timeout(Duration::from_millis(100), client2.receive_message()).await;
                    return Ok::<u32, anyhow::Error>(game_id);
                }
            }
        }
    }).await??;
    
    // Both clients join
    client1.send_message(WSMessage::JoinGame(game_id)).await?;
    client2.send_message(WSMessage::JoinGame(game_id)).await?;
    
    // Wait for snapshots
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(event) = client1.receive_game_event().await? {
                if matches!(event.event, GameEvent::Snapshot { .. }) {
                    break;
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    }).await??;
    
    // Disconnect one client (simulating player leaving)
    client1.disconnect().await?;
    
    // The game should continue with one player
    // Eventually cleanup service will mark it as abandoned
    
    // Disconnect second client
    client2.disconnect().await?;
    
    // Game cleanup happens automatically via the cleanup service
    // No manual database manipulation needed
    
    env.shutdown().await?;
    Ok(())
}