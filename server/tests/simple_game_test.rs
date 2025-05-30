mod common;

use anyhow::Result;
use server::ws_server::WSMessage;
use ::common::{GameEvent, GameType};
use tokio::time::{timeout, Duration};
use crate::common::{TestEnvironment, TestClient};

#[tokio::test]
async fn test_simple_game_creation() -> Result<()> {
    // Initialize tracing
    let _ = tracing_subscriber::fmt::try_init();
    
    // Create environment
    let mut env = TestEnvironment::new("test_simple_game_creation").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;
    
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    // Connect clients
    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;
    
    client1.authenticate(env.user_ids()[0]).await?;
    client2.authenticate(env.user_ids()[1]).await?;
    
    println!("Clients authenticated");
    
    // Queue for match
    client1.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    client2.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    
    println!("Clients queued");
    
    // Wait for any message - just see what happens
    let msg1 = timeout(Duration::from_secs(10), async {
        client1.receive_message().await
    }).await??;
    
    println!("Client1 received: {:?}", msg1);
    
    let msg2 = timeout(Duration::from_secs(10), async {
        client2.receive_message().await
    }).await??;
    
    println!("Client2 received: {:?}", msg2);
    
    // If we got MatchFound, try joining after a longer delay
    if let (WSMessage::MatchFound { game_id: id1 }, WSMessage::MatchFound { game_id: id2 }) = (&msg1, &msg2) {
        assert_eq!(id1, id2);
        let game_id = *id1;
        
        println!("Both clients got MatchFound for game {}", game_id);
        
        // Wait longer for game to be created
        println!("Waiting 5 seconds for game creation...");
        tokio::time::sleep(Duration::from_secs(5)).await;
        
        println!("Attempting to join game {}", game_id);
        client1.send_message(WSMessage::JoinGame(game_id)).await?;
        
        // See what happens
        let response = timeout(Duration::from_secs(5), async {
            client1.receive_message().await
        }).await??;
        
        println!("After join attempt, received: {:?}", response);
    }
    
    env.shutdown().await?;
    Ok(())
}