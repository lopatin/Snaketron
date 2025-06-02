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
    
    // Wait for game snapshots - clients should receive these when matched
    let msg1 = timeout(Duration::from_secs(10), async {
        client1.receive_message().await
    }).await??;
    
    println!("Client1 received: {:?}", msg1);
    
    let msg2 = timeout(Duration::from_secs(10), async {
        client2.receive_message().await
    }).await??;
    
    println!("Client2 received: {:?}", msg2);
    
    // Verify both clients received game snapshots
    match (&msg1, &msg2) {
        (WSMessage::GameEvent(event1), WSMessage::GameEvent(event2)) => {
            // Check that both events are snapshots for the same game
            assert_eq!(event1.game_id, event2.game_id);
            let game_id = event1.game_id;
            
            println!("Both clients received game snapshots for game {}", game_id);
            
            // Verify the events are snapshots
            match (&event1.event, &event2.event) {
                (GameEvent::Snapshot { game_state: state1 }, GameEvent::Snapshot { game_state: state2 }) => {
                    println!("Game snapshot verified - game has {} players", state1.players.len());
                    
                    // Verify both users are in the game
                    assert!(state1.players.contains_key(&(env.user_ids()[0] as u32)));
                    assert!(state1.players.contains_key(&(env.user_ids()[1] as u32)));
                    
                    // States should be identical
                    assert_eq!(state1.tick, state2.tick);
                    assert_eq!(state1.players.len(), 2);
                }
                _ => panic!("Expected Snapshot events, got {:?} and {:?}", event1.event, event2.event),
            }
        }
        _ => panic!("Expected GameEvent messages, got {:?} and {:?}", msg1, msg2),
    }
    
    env.shutdown().await?;
    Ok(())
}