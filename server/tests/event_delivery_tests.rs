mod common;

use anyhow::Result;
use server::ws_server::WSMessage;
use ::common::{GameEvent, GameEventMessage, GameType, GameCommand, GameCommandMessage, Direction, CommandId};
use tokio::time::{timeout, Duration};
use crate::common::{TestEnvironment, TestClient};

// #[tokio::test]
#[allow(dead_code)]
async fn test_game_events_delivered() -> Result<()> {
    // Initialize tracing for tests
    let _ = tracing_subscriber::fmt::try_init();
    
    // Create test environment
    let mut env = TestEnvironment::new("test_game_events_delivered").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;
    
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    // Connect two clients
    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;
    
    // Authenticate clients
    client1.authenticate(env.user_ids()[0]).await?;
    client2.authenticate(env.user_ids()[1]).await?;
    
    // Create a game through matchmaking
    // Queue both clients for a match
    client1.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    client2.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    
    // Wait for match found and join the game
    let game_id = timeout(Duration::from_secs(5), async {
        loop {
            match client1.receive_message().await? {
                WSMessage::GameEvent(event) => {
                    if let GameEvent::Snapshot { .. } = event.event {
                        return Ok::<_, anyhow::Error>(event.game_id);
                    }
                }
                _ => continue,
            }
        }
    }).await??;
    
    // Wait for client2 to also get game snapshot
    timeout(Duration::from_secs(5), async {
        loop {
            match client2.receive_message().await? {
                WSMessage::GameEvent(event) => {
                    if let GameEvent::Snapshot { .. } = event.event {
                        assert_eq!(event.game_id, game_id);
                        return Ok::<_, anyhow::Error>(());
                    }
                }
                _ => continue,
            }
        }
    }).await??;
    
    // Give the server a moment to create the game after sending MatchFound
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Now both clients join the game
    client1.send_message(WSMessage::JoinGame(game_id)).await?;
    client2.send_message(WSMessage::JoinGame(game_id)).await?;
    
    // Wait for initial snapshots
    let snapshot1 = timeout(Duration::from_secs(5), async {
        loop {
            if let WSMessage::GameEvent(event) = client1.receive_message().await? {
                if matches!(event.event, GameEvent::Snapshot { .. }) {
                    return Ok::<_, anyhow::Error>(event);
                }
            }
        }
    }).await??;
    
    let snapshot2 = timeout(Duration::from_secs(5), async {
        loop {
            if let WSMessage::GameEvent(event) = client2.receive_message().await? {
                if matches!(event.event, GameEvent::Snapshot { .. }) {
                    return Ok::<_, anyhow::Error>(event);
                }
            }
        }
    }).await??;
    
    assert_eq!(snapshot1.game_id, snapshot2.game_id);
    assert_eq!(snapshot1.game_id, game_id);
    
    // Send a command from client1
    client1.send_message(WSMessage::GameCommand(GameCommandMessage {
        command_id_client: CommandId {
            tick: 0,
            user_id: env.user_ids()[0] as u32,
            sequence_number: 0,
        },
        command_id_server: None,
        command: GameCommand::Turn { 
            snake_id: env.user_ids()[0] as u32, 
            direction: Direction::Up 
        },
    })).await?;
    
    // Both clients should receive game events
    let event1 = timeout(Duration::from_secs(5), async {
        loop {
            if let WSMessage::GameEvent(event) = client1.receive_message().await? {
                // Skip if it's another snapshot
                if !matches!(event.event, GameEvent::Snapshot { .. }) {
                    return Ok::<_, anyhow::Error>(event);
                }
            }
        }
    }).await??;
    
    let event2 = timeout(Duration::from_secs(5), async {
        loop {
            if let WSMessage::GameEvent(event) = client2.receive_message().await? {
                // Skip if it's another snapshot
                if !matches!(event.event, GameEvent::Snapshot { .. }) {
                    return Ok::<_, anyhow::Error>(event);
                }
            }
        }
    }).await??;
    
    // Both should receive events for the same game
    assert_eq!(event1.game_id, game_id);
    assert_eq!(event2.game_id, game_id);
    
    // Clean up
    client1.disconnect().await?;
    client2.disconnect().await?;
    env.shutdown().await?;
    
    Ok(())
}

// #[tokio::test]
#[allow(dead_code)]
async fn test_game_events_continue_after_reconnect() -> Result<()> {
    // Initialize tracing for tests
    let _ = tracing_subscriber::fmt::try_init();
    
    // Create test environment
    let mut env = TestEnvironment::new("test_game_events_continue_after_reconnect").await?;
    env.add_server().await?;
    env.create_user().await?;
    
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    // Connect client
    let mut client = TestClient::connect(&server_addr).await?;
    client.authenticate(env.user_ids()[0]).await?;
    
    // Create a game through matchmaking
    client.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 2 } 
    }).await?;
    
    // Wait for match found
    let game_id = timeout(Duration::from_secs(5), async {
        loop {
            if let WSMessage::GameEvent(event) = client.receive_message().await? {
                if let GameEvent::Snapshot { .. } = event.event {
                    let game_id = event.game_id;
                    return Ok::<_, anyhow::Error>(game_id);
                }
            }
        }
    }).await??;
    
    // Join the game
    client.send_message(WSMessage::JoinGame(game_id)).await?;
    
    // Wait for initial snapshot
    let _snapshot = timeout(Duration::from_secs(5), async {
        loop {
            if let WSMessage::GameEvent(event) = client.receive_message().await? {
                if matches!(event.event, GameEvent::Snapshot { .. }) {
                    return Ok::<_, anyhow::Error>(event);
                }
            }
        }
    }).await??;
    
    // Send a command
    client.send_message(WSMessage::GameCommand(GameCommandMessage {
        command_id_client: CommandId {
            tick: 0,
            user_id: env.user_ids()[0] as u32,
            sequence_number: 0,
        },
        command_id_server: None,
        command: GameCommand::Turn { 
            snake_id: env.user_ids()[0] as u32, 
            direction: Direction::Up 
        },
    })).await?;
    
    // Should receive event
    let _event = timeout(Duration::from_secs(5), async {
        loop {
            if let WSMessage::GameEvent(event) = client.receive_message().await? {
                if !matches!(event.event, GameEvent::Snapshot { .. }) {
                    return Ok::<_, anyhow::Error>(event);
                }
            }
        }
    }).await??;
    
    // Disconnect and reconnect
    client.disconnect().await?;
    let mut client = TestClient::connect(&server_addr).await?;
    client.authenticate(env.user_ids()[0]).await?;
    
    // Rejoin the same game
    client.send_message(WSMessage::JoinGame(game_id)).await?;
    
    // Should receive snapshot
    let _snapshot = timeout(Duration::from_secs(5), async {
        loop {
            if let WSMessage::GameEvent(event) = client.receive_message().await? {
                if matches!(event.event, GameEvent::Snapshot { .. }) {
                    return Ok::<_, anyhow::Error>(event);
                }
            }
        }
    }).await??;
    
    // Send another command
    client.send_message(WSMessage::GameCommand(GameCommandMessage {
        command_id_client: CommandId {
            tick: 0,
            user_id: env.user_ids()[0] as u32,
            sequence_number: 1,
        },
        command_id_server: None,
        command: GameCommand::Turn { 
            snake_id: env.user_ids()[0] as u32, 
            direction: Direction::Down 
        },
    })).await?;
    
    // Should still receive events
    let event = timeout(Duration::from_secs(5), async {
        loop {
            if let WSMessage::GameEvent(event) = client.receive_message().await? {
                if !matches!(event.event, GameEvent::Snapshot { .. }) {
                    return Ok::<_, anyhow::Error>(event);
                }
            }
        }
    }).await??;
    
    assert_eq!(event.game_id, game_id);
    
    // Clean up
    client.disconnect().await?;
    env.shutdown().await?;
    
    Ok(())
}