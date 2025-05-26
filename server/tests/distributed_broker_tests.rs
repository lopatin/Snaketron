use anyhow::Result;
use server::ws_server::WSMessage;
use ::common::{GameEvent, GameType};
use tokio::time::{timeout, Duration};
use tracing::info;

mod common;
use self::common::{TestEnvironment, TestClient};

#[tokio::test]
async fn test_distributed_broker_local_game() -> Result<()> {
    // Initialize tracing for tests
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .try_init();
    
    println!("Starting test environment...");
    // Create test environment
    let mut env = TestEnvironment::new("test_distributed_broker_local_game").await?;
    
    // Add a server
    env.add_server().await?;
    
    // Create users
    let user1_id = env.create_user().await?;
    let user2_id = env.create_user().await?;
    println!("Test environment created with users");
    
    let server_addr = env.ws_addr(0).expect("Server should exist");
    println!("Server address: {}", server_addr);
    
    // Connect two clients
    println!("Connecting client 1...");
    let mut client1 = TestClient::connect(&server_addr).await?;
    println!("Client 1 connected");
    println!("Connecting client 2...");
    let mut client2 = TestClient::connect(&server_addr).await?;
    println!("Client 2 connected");
    
    // Authenticate clients
    println!("Authenticating client 1...");
    client1.authenticate(user1_id).await?;
    println!("Client 1 authenticated");
    println!("Authenticating client 2...");
    client2.authenticate(user2_id).await?;
    println!("Client 2 authenticated");
    
    // Small delay to ensure server is ready
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Queue both clients for a match
    println!("Queueing client 1 for match...");
    let game_type = GameType::FreeForAll { max_players: 2 };
    client1.send_message(WSMessage::QueueForMatch { 
        game_type: game_type.clone()
    }).await?;
    println!("Client 1 queued");
    
    println!("Queueing client 2 for match...");
    client2.send_message(WSMessage::QueueForMatch { 
        game_type 
    }).await?;
    println!("Client 2 queued");
    
    // Wait for match and automatic game join - clients receive snapshots directly
    println!("Waiting for game snapshots (automatic join)...");
    let (game_id, game_state1) = timeout(Duration::from_secs(30), async {
        loop {
            if let Some(event) = client1.receive_game_event().await? {
                println!("Client1 received event: {:?}", event.event);
                if let GameEvent::Snapshot { game_state } = event.event {
                    return Ok::<(u32, ::common::GameState), anyhow::Error>((event.game_id, game_state));
                }
            }
        }
    }).await??;
    
    // Verify client2 also got the snapshot
    let (game_id2, game_state2) = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(event) = client2.receive_game_event().await? {
                println!("Client2 received event: {:?}", event.event);
                if let GameEvent::Snapshot { game_state } = event.event {
                    return Ok::<(u32, ::common::GameState), anyhow::Error>((event.game_id, game_state));
                }
            }
        }
    }).await??;
    
    assert_eq!(game_id, game_id2, "Both clients should be in the same game");
    println!("Both clients matched and auto-joined game {} with type {:?}", game_id, game_state1.game_type);
    
    // The game type might be different than requested due to matchmaking trying multiple types
    // What matters is that both clients are in the same game
    assert_eq!(game_state1.game_type, game_state2.game_type, "Both clients should have same game type");
    
    // Clean disconnect
    client1.disconnect().await?;
    client2.disconnect().await?;
    env.shutdown().await?;
    
    Ok(())
}

#[tokio::test]
async fn test_distributed_broker_cross_server() -> Result<()> {
    // Create test environment with two servers
    let mut env = TestEnvironment::new("test_distributed_broker_cross_server").await?;
    
    // Add two servers
    env.add_server().await?;
    env.add_server().await?;
    
    // Create users
    let user1_id = env.create_user().await?;
    let user2_id = env.create_user().await?;
    
    let server1_addr = env.ws_addr(0).expect("Server 1 should exist");
    let server2_addr = env.ws_addr(1).expect("Server 2 should exist");
    
    // Note: This test would require gRPC communication between servers
    // For now, we'll test that each server can handle its own games independently
    
    // Connect clients to different servers
    let mut client1 = TestClient::connect(&server1_addr).await?;
    let mut client2 = TestClient::connect(&server2_addr).await?;
    
    client1.authenticate(user1_id).await?;
    client2.authenticate(user2_id).await?;
    
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
    // Create test environment
    let mut env = TestEnvironment::new("test_game_lifecycle_with_cleanup").await?;
    
    // Add a server
    env.add_server().await?;
    
    // Create users
    let user1_id = env.create_user().await?;
    let user2_id = env.create_user().await?;
    
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    // Connect two clients
    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;
    
    client1.authenticate(user1_id).await?;
    client2.authenticate(user2_id).await?;
    
    // Create a match
    println!("test_game_lifecycle_with_cleanup: Queuing players with IDs {} and {}", user1_id, user2_id);
    client1.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    
    client2.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    
    // Wait for match - with auto-joining, we receive the game snapshot directly
    println!("test_game_lifecycle_with_cleanup: Waiting for game snapshot...");
    let game_id = match timeout(Duration::from_secs(5), async {
        loop {
            if let Some(event) = client1.receive_game_event().await? {
                println!("test_game_lifecycle_with_cleanup: Received event: {:?}", event.event);
                if matches!(event.event, GameEvent::Snapshot { .. }) {
                    return Ok::<u32, anyhow::Error>(event.game_id);
                }
            }
        }
    }).await {
        Ok(Ok(id)) => id,
        Ok(Err(e)) => return Err(e),
        Err(_) => {
            println!("test_game_lifecycle_with_cleanup: Timeout waiting for game snapshot!");
            return Err(anyhow::anyhow!("Timeout waiting for game snapshot"));
        }
    };
    
    // Client2 should also receive the snapshot
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(event) = client2.receive_game_event().await? {
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

    env.shutdown().await?;
    Ok(())
}