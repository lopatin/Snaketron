use anyhow::Result;
use server::ws_server::WSMessage;
use ::common::{GameType, GameEvent};
use tokio::time::{timeout, Duration};
use futures_util::future::join_all;

mod common;
use self::common::{TestEnvironment, TestClient};

#[tokio::test]
async fn test_basic_matchmaking() -> Result<()> {
    let env = TestEnvironment::new(1).await?;
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;
    
    client1.authenticate(1).await?;
    client2.authenticate(2).await?;
    
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

#[tokio::test]
async fn test_leave_queue() -> Result<()> {
    let env = TestEnvironment::new(1).await?;
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    let mut client = TestClient::connect(&server_addr).await?;
    client.authenticate(1).await?;
    
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

#[tokio::test]
async fn test_team_matchmaking() -> Result<()> {
    let env = TestEnvironment::new(1).await?;
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    // Connect 4 clients for 2v2
    let mut clients = Vec::new();
    for i in 1..=4 {
        let mut client = TestClient::connect(&server_addr).await?;
        client.authenticate(i).await?;
        clients.push(client);
    }
    
    // All queue for team match
    for client in &mut clients {
        client.send_message(WSMessage::QueueForMatch { 
            game_type: GameType::TeamMatch { per_team: 2 } 
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
            "All players should be in same team match");
    
    for client in clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_concurrent_matchmaking() -> Result<()> {
    let env = TestEnvironment::new(1).await?;
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    // Create many clients concurrently
    let client_futures = (1..=10).map(|i| {
        let addr = server_addr.clone();
        async move {
            let mut client = TestClient::connect(&addr).await?;
            client.authenticate(i).await?;
            
            client.send_message(WSMessage::QueueForMatch { 
                game_type: GameType::FreeForAll { max_players: 2 } 
            }).await?;
            
            let game_id = wait_for_match(&mut client).await?;
            client.disconnect().await?;
            
            Ok::<u32, anyhow::Error>(game_id)
        }
    });
    
    let results = join_all(client_futures).await;
    
    // All clients should have been matched
    let mut game_ids = Vec::new();
    for result in results {
        game_ids.push(result?);
    }
    
    // Should have created 5 games (10 players / 2 per game)
    game_ids.sort();
    game_ids.dedup();
    assert_eq!(game_ids.len(), 5, "Should create 5 games for 10 players");
    
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_disconnect_during_queue() -> Result<()> {
    let env = TestEnvironment::new(1).await?;
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;
    
    client1.authenticate(1).await?;
    client2.authenticate(2).await?;
    
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

#[tokio::test]
async fn test_rejoin_active_game() -> Result<()> {
    let env = TestEnvironment::new(1).await?;
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;
    
    client1.authenticate(1).await?;
    client2.authenticate(2).await?;
    
    // Get matched
    client1.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    
    client2.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    
    let game_id = wait_for_match(&mut client1).await?;
    let _ = wait_for_match(&mut client2).await?;
    
    // Both join
    client1.send_message(WSMessage::JoinGame(game_id)).await?;
    client2.send_message(WSMessage::JoinGame(game_id)).await?;
    
    // Wait for snapshots
    wait_for_snapshot(&mut client1).await?;
    wait_for_snapshot(&mut client2).await?;
    
    // Client1 disconnects
    client1.disconnect().await?;
    
    // Client1 reconnects and rejoins
    let mut client1_new = TestClient::connect(&server_addr).await?;
    client1_new.authenticate(1).await?;
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
    timeout(Duration::from_secs(10), async {
        loop {
            match client.receive_message().await? {
                WSMessage::MatchFound { game_id } => return Ok(game_id),
                _ => continue,
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