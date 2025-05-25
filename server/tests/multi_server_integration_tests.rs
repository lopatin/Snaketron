use anyhow::Result;
use server::ws_server::WSMessage;
use ::common::{GameType, GameEvent};
use tokio::time::{timeout, Duration};

mod common;
use self::common::{TestEnvironment, TestClient};

#[tokio::test]
async fn test_multiple_servers_independent_games() -> Result<()> {
    // Start 3 servers with 6 users (2 per server)
    let mut env = TestEnvironment::new("test_multiple_servers_independent_games").await?;
    for _ in 0..3 {
        env.add_server(false).await?;
    }
    for _ in 0..6 {
        env.create_user().await?;
    }
    
    // Create games on each server independently
    for server_idx in 0..3 {
        let server_addr = env.ws_addr(server_idx).expect("Server should exist");
        
        let mut client1 = TestClient::connect(&server_addr).await?;
        let mut client2 = TestClient::connect(&server_addr).await?;
        
        let user_ids = env.user_ids();
        client1.authenticate(user_ids[server_idx * 2]).await?;
        client2.authenticate(user_ids[server_idx * 2 + 1]).await?;
        
        // Queue on this server
        client1.send_message(WSMessage::QueueForMatch { 
            game_type: GameType::FreeForAll { max_players: 2 } 
        }).await?;
        
        client2.send_message(WSMessage::QueueForMatch { 
            game_type: GameType::FreeForAll { max_players: 2 } 
        }).await?;
        
        // Wait for game to start (auto-join after match)
        let game_id1 = wait_for_game_start(&mut client1).await?;
        let game_id2 = wait_for_game_start(&mut client2).await?;
        
        assert_eq!(game_id1, game_id2, "Both clients should be in the same game");
        
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
    // Start 2 servers with 8 users
    let mut env = TestEnvironment::new("test_server_load_distribution").await?;
    env.add_server(false).await?;
    env.add_server(false).await?;
    for _ in 0..8 {
        env.create_user().await?;
    }
    let server1_addr = env.ws_addr(0).expect("Server 1 should exist");
    let server2_addr = env.ws_addr(1).expect("Server 2 should exist");
    
    // Connect multiple clients distributed across servers
    let mut clients_server1 = Vec::new();
    let mut clients_server2 = Vec::new();
    
    let user_ids = env.user_ids();
    
    // 4 clients on server 1
    for i in 0..4 {
        let mut client = TestClient::connect(&server1_addr).await?;
        client.authenticate(user_ids[i]).await?;
        clients_server1.push(client);
    }
    
    // 4 clients on server 2
    for i in 4..8 {
        let mut client = TestClient::connect(&server2_addr).await?;
        client.authenticate(user_ids[i]).await?;
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
        game_ids_server1.push(wait_for_game_start(client).await?);
    }
    
    let mut game_ids_server2 = Vec::new();
    for client in &mut clients_server2 {
        game_ids_server2.push(wait_for_game_start(client).await?);
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
    // Start 2 servers with 1 user
    let mut env = TestEnvironment::new("test_server_isolation").await?;
    env.add_server(false).await?;
    env.add_server(false).await?;
    env.create_user().await?;
    let server1_addr = env.ws_addr(0).expect("Server 1 should exist");
    let server2_addr = env.ws_addr(1).expect("Server 2 should exist");
    
    // Connect client to server 1
    let mut client1 = TestClient::connect(&server1_addr).await?;
    client1.authenticate(env.user_ids()[0]).await?;
    
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
    let mut env = TestEnvironment::new("test_concurrent_games_on_multiple_servers").await?;
    env.add_server(false).await?;
    env.add_server(false).await?;
    for _ in 0..8 {
        env.create_user().await?;
    }
    
    // Run concurrent operations on both servers
    let mut handles = Vec::new();
    let user_ids = env.user_ids().to_vec(); // Clone for moving into tasks
    
    for server_idx in 0..2 {
        let server_addr = env.ws_addr(server_idx).expect("Server should exist").clone();
        let user_ids_clone = user_ids.clone();
        
        let handle = tokio::spawn(async move {
            // Each server handles 3 games concurrently, but we only have 8 users total
            // So we'll use 2 games per server (4 users per server)
            let mut game_handles = Vec::new();
            
            for game_idx in 0..2 {
                let addr = server_addr.clone();
                let user_idx_base = server_idx * 4 + game_idx * 2;
                let user1_id = user_ids_clone[user_idx_base];
                let user2_id = user_ids_clone[user_idx_base + 1];
                
                let game_handle = tokio::spawn(async move {
                    let mut client1 = TestClient::connect(&addr).await?;
                    let mut client2 = TestClient::connect(&addr).await?;
                    
                    client1.authenticate(user1_id).await?;
                    client2.authenticate(user2_id).await?;
                    
                    client1.send_message(WSMessage::QueueForMatch { 
                        game_type: GameType::FreeForAll { max_players: 2 } 
                    }).await?;
                    
                    client2.send_message(WSMessage::QueueForMatch { 
                        game_type: GameType::FreeForAll { max_players: 2 } 
                    }).await?;
                    
                    let game_id1 = wait_for_game_start(&mut client1).await?;
                    let game_id2 = wait_for_game_start(&mut client2).await?;
                    
                    assert_eq!(game_id1, game_id2, "Both clients should be in the same game");
                    
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
async fn wait_for_game_start(client: &mut TestClient) -> Result<u32> {
    timeout(Duration::from_secs(10), async {
        loop {
            if let Some(event) = client.receive_game_event().await? {
                if matches!(event.event, GameEvent::Snapshot { .. }) {
                    return Ok(event.game_id);
                }
            }
        }
    }).await?
}