use anyhow::Result;
use server::ws_server::WSMessage;
use ::common::{GameType, GameEvent};
use tokio::time::{timeout, Duration};

mod common;
use self::common::{TestEnvironment, TestClient};

#[tokio::test]
async fn test_multiple_servers_independent_games() -> Result<()> {
    // Start 3 servers
    let env = TestEnvironment::new(3).await?;
    
    // Create games on each server independently
    for server_idx in 0..3 {
        let server_addr = env.ws_addr(server_idx).expect("Server should exist");
        
        let mut client1 = TestClient::connect(&server_addr).await?;
        let mut client2 = TestClient::connect(&server_addr).await?;
        
        client1.authenticate(server_idx as i32 * 10 + 1).await?;
        client2.authenticate(server_idx as i32 * 10 + 2).await?;
        
        // Queue on this server
        client1.send_message(WSMessage::QueueForMatch { 
            game_type: GameType::FreeForAll { max_players: 2 } 
        }).await?;
        
        client2.send_message(WSMessage::QueueForMatch { 
            game_type: GameType::FreeForAll { max_players: 2 } 
        }).await?;
        
        // Get matched
        let game_id = wait_for_match(&mut client1).await?;
        let _ = wait_for_match(&mut client2).await?;
        
        // Join game
        client1.send_message(WSMessage::JoinGame(game_id)).await?;
        client2.send_message(WSMessage::JoinGame(game_id)).await?;
        
        // Verify game starts
        wait_for_snapshot(&mut client1).await?;
        wait_for_snapshot(&mut client2).await?;
        
        // Disconnect
        client1.disconnect().await?;
        client2.disconnect().await?;
    }
    
    // All servers operated independently
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_server_load_distribution() -> Result<()> {
    // Start 2 servers
    let env = TestEnvironment::new(2).await?;
    let server1_addr = env.ws_addr(0).expect("Server 1 should exist");
    let server2_addr = env.ws_addr(1).expect("Server 2 should exist");
    
    // Connect multiple clients distributed across servers
    let mut clients_server1 = Vec::new();
    let mut clients_server2 = Vec::new();
    
    // 4 clients on server 1
    for i in 1..=4 {
        let mut client = TestClient::connect(&server1_addr).await?;
        client.authenticate(i).await?;
        clients_server1.push(client);
    }
    
    // 4 clients on server 2
    for i in 5..=8 {
        let mut client = TestClient::connect(&server2_addr).await?;
        client.authenticate(i).await?;
        clients_server2.push(client);
    }
    
    // Queue all clients for matches
    for client in &mut clients_server1 {
        client.send_message(WSMessage::QueueForMatch { 
            game_type: GameType::FreeForAll { max_players: 2 } 
        }).await?;
    }
    
    for client in &mut clients_server2 {
        client.send_message(WSMessage::QueueForMatch { 
            game_type: GameType::FreeForAll { max_players: 2 } 
        }).await?;
    }
    
    // Each server should create 2 games (4 players / 2 per game)
    let mut game_ids_server1 = Vec::new();
    for client in &mut clients_server1 {
        game_ids_server1.push(wait_for_match(client).await?);
    }
    
    let mut game_ids_server2 = Vec::new();
    for client in &mut clients_server2 {
        game_ids_server2.push(wait_for_match(client).await?);
    }
    
    // Verify games were created on each server
    game_ids_server1.sort();
    game_ids_server1.dedup();
    assert_eq!(game_ids_server1.len(), 2, "Server 1 should have 2 games");
    
    game_ids_server2.sort();
    game_ids_server2.dedup();
    assert_eq!(game_ids_server2.len(), 2, "Server 2 should have 2 games");
    
    // Disconnect all clients
    for client in clients_server1 {
        client.disconnect().await?;
    }
    for client in clients_server2 {
        client.disconnect().await?;
    }
    
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_server_isolation() -> Result<()> {
    // Start 2 servers
    let env = TestEnvironment::new(2).await?;
    let server1_addr = env.ws_addr(0).expect("Server 1 should exist");
    let server2_addr = env.ws_addr(1).expect("Server 2 should exist");
    
    // Connect client to server 1
    let mut client1 = TestClient::connect(&server1_addr).await?;
    client1.authenticate(1).await?;
    
    // Queue on server 1
    client1.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    
    // Connect client to server 2
    let mut client2 = TestClient::connect(&server2_addr).await?;
    client2.authenticate(2).await?;
    
    // Queue on server 2 with same game type
    client2.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    
    // Without cross-server matchmaking, they shouldn't be matched together
    let match_result = timeout(Duration::from_secs(3), wait_for_match(&mut client1)).await;
    assert!(match_result.is_err(), "Client 1 shouldn't get matched without another player on same server");
    
    let match_result = timeout(Duration::from_secs(1), wait_for_match(&mut client2)).await;
    assert!(match_result.is_err(), "Client 2 shouldn't get matched without another player on same server");
    
    // Clean up
    client1.send_message(WSMessage::LeaveQueue).await?;
    client2.send_message(WSMessage::LeaveQueue).await?;
    
    client1.disconnect().await?;
    client2.disconnect().await?;
    
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_concurrent_operations_multiple_servers() -> Result<()> {
    // Start 2 servers
    let env = TestEnvironment::new(2).await?;
    
    // Run concurrent operations on both servers
    let mut handles = Vec::new();
    
    for server_idx in 0..2 {
        let server_addr = env.ws_addr(server_idx).expect("Server should exist").clone();
        
        let handle = tokio::spawn(async move {
            // Each server handles 3 games concurrently
            let mut game_handles = Vec::new();
            
            for game_idx in 0..3 {
                let addr = server_addr.clone();
                let game_handle = tokio::spawn(async move {
                    let base_id = (server_idx * 100 + game_idx * 10) as i32;
                    
                    let mut client1 = TestClient::connect(&addr).await?;
                    let mut client2 = TestClient::connect(&addr).await?;
                    
                    client1.authenticate(base_id + 1).await?;
                    client2.authenticate(base_id + 2).await?;
                    
                    client1.send_message(WSMessage::QueueForMatch { 
                        game_type: GameType::FreeForAll { max_players: 2 } 
                    }).await?;
                    
                    client2.send_message(WSMessage::QueueForMatch { 
                        game_type: GameType::FreeForAll { max_players: 2 } 
                    }).await?;
                    
                    let game_id = wait_for_match(&mut client1).await?;
                    let _ = wait_for_match(&mut client2).await?;
                    
                    client1.send_message(WSMessage::JoinGame(game_id)).await?;
                    client2.send_message(WSMessage::JoinGame(game_id)).await?;
                    
                    wait_for_snapshot(&mut client1).await?;
                    wait_for_snapshot(&mut client2).await?;
                    
                    client1.disconnect().await?;
                    client2.disconnect().await?;
                    
                    Ok::<(), anyhow::Error>(())
                });
                game_handles.push(game_handle);
            }
            
            // Wait for all games on this server
            for handle in game_handles {
                handle.await??;
            }
            
            Ok::<(), anyhow::Error>(())
        });
        
        handles.push(handle);
    }
    
    // Wait for all servers to complete
    for handle in handles {
        handle.await??;
    }
    
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