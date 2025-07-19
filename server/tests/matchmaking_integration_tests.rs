use anyhow::Result;
use server::ws_server::WSMessage;
use ::common::{GameType, GameEvent};
use tokio::time::{timeout, Duration};
use futures_util::future::join_all;

mod common;
use self::common::{TestEnvironment, TestClient};

// #[tokio::test]
#[allow(dead_code)]
async fn test_minimal() -> Result<()> {
    // Minimal test to check timing
    println!("Test started");
    tokio::time::sleep(Duration::from_secs(1)).await;
    println!("Test completed");
    Ok(())
}

// #[tokio::test]
#[allow(dead_code)]
async fn test_simple_two_player_match() -> Result<()> {
    // Simple test with just 2 players to debug matchmaking
    let mut env = TestEnvironment::new("test_simple_two_player_match").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;
    
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    // Connect and authenticate both clients
    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;
    
    client1.authenticate(env.user_ids()[0]).await?;
    client2.authenticate(env.user_ids()[1]).await?;
    
    println!("Clients authenticated. User IDs: {:?}", env.user_ids());
    
    // Queue for match with just 2 players max
    client1.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    println!("Client 1 queued for match");
    
    client2.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    println!("Client 2 queued for match");
    
    // Wait for game snapshots with longer timeout and debug output
    println!("Waiting for game snapshots...");
    let start = std::time::Instant::now();
    
    let game_id1 = timeout(Duration::from_secs(30), async {
        loop {
            if let Some(event) = client1.receive_game_event().await? {
                println!("Client 1 received event: {:?}", event.event);
                if matches!(event.event, GameEvent::Snapshot { .. }) {
                    println!("Client 1 got snapshot for game {} after {:?}", event.game_id, start.elapsed());
                    return Ok::<u32, anyhow::Error>(event.game_id);
                }
            }
        }
    }).await??;
    
    let game_id2 = timeout(Duration::from_secs(5), async {
        loop {
            if let Some(event) = client2.receive_game_event().await? {
                println!("Client 2 received event: {:?}", event.event);
                if matches!(event.event, GameEvent::Snapshot { .. }) {
                    println!("Client 2 got snapshot for game {} after {:?}", event.game_id, start.elapsed());
                    return Ok::<u32, anyhow::Error>(event.game_id);
                }
            }
        }
    }).await??;
    
    assert_eq!(game_id1, game_id2, "Both players should be in same game");
    println!("Test passed! Both clients matched to game {}", game_id1);
    
    client1.disconnect().await?;
    client2.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

// #[tokio::test]
#[allow(dead_code)]
async fn test_basic_matchmaking() -> Result<()> {
    let mut env = TestEnvironment::new("test_basic_matchmaking").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;
    
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;
    
    client1.authenticate(env.user_ids()[0]).await?;
    client2.authenticate(env.user_ids()[1]).await?;
    
    // Queue for match
    client1.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    
    client2.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    
    // Both should receive MatchFound
    let game_id1 = wait_for_match(&mut client1).await?;
    let game_id2 = wait_for_match(&mut client2).await?;
    
    assert_eq!(game_id1, game_id2, "Both players should be matched to same game");
    
    client1.disconnect().await?;
    client2.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

// #[tokio::test]
#[allow(dead_code)]
async fn test_leave_queue() -> Result<()> {
    let mut env = TestEnvironment::new("test_leave_queue").await?;
    env.add_server().await?;
    env.create_user().await?;
    
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    let mut client = TestClient::connect(&server_addr).await?;
    client.authenticate(env.user_ids()[0]).await?;
    
    // Queue and immediately leave
    client.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    client.send_message(WSMessage::LeaveQueue).await?;
    
    // Should not receive a match
    let result = timeout(Duration::from_secs(2), wait_for_match(&mut client)).await;
    assert!(result.is_err(), "Should timeout waiting for match after leaving queue");
    
    client.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

// #[tokio::test]
#[allow(dead_code)]
async fn test_team_matchmaking() -> Result<()> {
    let mut env = TestEnvironment::new("test_team_matchmaking").await?;
    env.add_server().await?;
    for _ in 0..4 {
        env.create_user().await?;
    }
    
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    // Connect 4 clients
    let mut clients = Vec::new();
    for i in 0..4 {
        let mut client = TestClient::connect(&server_addr).await?;
        client.authenticate(env.user_ids()[i]).await?;
        clients.push(client);
    }
    
    // All queue for match
    for client in &mut clients {
        client.send_message(WSMessage::QueueForMatch { 
            game_type: GameType::FreeForAll { max_players: 4 } 
        }).await?;
    }
    
    // All should get matched
    let mut game_ids = Vec::new();
    for client in &mut clients {
        game_ids.push(wait_for_match(client).await?);
    }
    
    // All should be in same game
    let first_game_id = game_ids[0];
    assert!(game_ids.iter().all(|&id| id == first_game_id), 
            "All 4 players should be matched to the same game");
    
    for client in clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

// #[tokio::test]
#[allow(dead_code)]
async fn test_concurrent_matchmaking() -> Result<()> {
    let mut env = TestEnvironment::new("test_concurrent_matchmaking").await?;
    env.add_server().await?;
    
    // Create only 6 players to reduce complexity and timing issues
    for _ in 0..6 {
        env.create_user().await?;
    }
    
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    // Give the server time to fully initialize all background services
    // The matchmaking loop runs every 2 seconds, game discovery runs every 1 second
    // When running multiple tests in parallel, initialization can be slower
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    println!("Starting concurrent matchmaking test with 6 clients");
    
    // Connect all clients first
    let mut clients = Vec::new();
    for (i, user_id) in env.user_ids().iter().copied().enumerate() {
        println!("Client {} (user_id={}) starting", i, user_id);
        let mut client = TestClient::connect(&server_addr).await?;
        println!("Client {} connected", i);
        
        client.authenticate(user_id).await?;
        println!("Client {} authenticated", i);
        
        clients.push(client);
    }
    
    // Queue all clients for match
    for (i, client) in clients.iter_mut().enumerate() {
        client.send_message(WSMessage::QueueForMatch { 
            game_type: GameType::FreeForAll { max_players: 2 } 
        }).await?;
        println!("Client {} queued for match", i);
    }
    
    // Wait for all clients to get matched
    let mut game_ids = Vec::new();
    for (i, client) in clients.iter_mut().enumerate() {
        match timeout(Duration::from_secs(30), wait_for_match(client)).await {
            Ok(Ok(game_id)) => {
                println!("Client {} matched to game {}", i, game_id);
                game_ids.push(game_id);
            }
            Ok(Err(e)) => {
                println!("Client {} error: {}", i, e);
                return Err(e);
            }
            Err(_) => {
                println!("Client {} timed out waiting for match", i);
                return Err(anyhow::anyhow!("Client {} timed out", i));
            }
        }
    }
    
    // Disconnect all clients
    for (i, client) in clients.into_iter().enumerate() {
        client.disconnect().await?;
        println!("Client {} disconnected", i);
    }
    
    // Should have created some games for all players
    game_ids.sort();
    game_ids.dedup();
    println!("Created {} unique games for 6 players", game_ids.len());
    assert_eq!(game_ids.len(), 3, "Should create exactly 3 games for 6 players with max_players=2");
    
    env.shutdown().await?;
    Ok(())
}

// #[tokio::test]
#[allow(dead_code)]
async fn test_disconnect_during_queue() -> Result<()> {
    let mut env = TestEnvironment::new("test_disconnect_during_queue").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;
    
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;
    
    client1.authenticate(env.user_ids()[0]).await?;
    client2.authenticate(env.user_ids()[1]).await?;
    
    // Both queue
    client1.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 3 } 
    }).await?;
    
    client2.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 3 } 
    }).await?;
    
    // Client1 disconnects while in queue
    tokio::time::sleep(Duration::from_millis(100)).await;
    client1.disconnect().await?;
    
    // Client2 should not get matched (needs 3 players)
    let result = timeout(Duration::from_secs(2), wait_for_match(&mut client2)).await;
    assert!(result.is_err(), "Should not match with insufficient players");
    
    client2.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

// #[tokio::test]
#[allow(dead_code)]
async fn test_rejoin_active_game() -> Result<()> {
    let mut env = TestEnvironment::new("test_rejoin_active_game").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;
    
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;
    
    client1.authenticate(env.user_ids()[0]).await?;
    client2.authenticate(env.user_ids()[1]).await?;
    
    // Get matched
    client1.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    
    client2.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    
    // Both get matched and auto-joined (wait_for_match now waits for the snapshot)
    let game_id = wait_for_match(&mut client1).await?;
    let game_id2 = wait_for_match(&mut client2).await?;
    assert_eq!(game_id, game_id2, "Both players should be in same game");
    
    // Client1 disconnects
    client1.disconnect().await?;
    
    // Client1 reconnects and rejoins
    let mut client1_new = TestClient::connect(&server_addr).await?;
    client1_new.authenticate(env.user_ids()[0]).await?;
    client1_new.send_message(WSMessage::JoinGame(game_id)).await?;
    
    // Should receive game snapshot
    wait_for_snapshot(&mut client1_new).await?;
    
    client1_new.disconnect().await?;
    client2.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

// Helper functions
async fn wait_for_match(client: &mut TestClient) -> Result<u32> {
    timeout(Duration::from_secs(30), async {
        loop {
            match client.receive_message().await? {
                WSMessage::GameEvent(event) => {
                    if matches!(event.event, GameEvent::Snapshot { .. }) {
                        return Ok(event.game_id);
                    }
                }
                _ => {
                    // Ignore other messages
                }
            }
        }
    }).await?
}

async fn wait_for_snapshot(client: &mut TestClient) -> Result<()> {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(event) = client.receive_game_event().await? {
                if matches!(event.event, GameEvent::Snapshot { .. }) {
                    return Ok(());
                }
            }
        }
    }).await?
}