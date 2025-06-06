mod common;

use anyhow::Result;
use server::ws_server::WSMessage;
use ::common::{GameEvent, GameType, GameCommand, GameCommandMessage, Direction, GameStatus, CommandId};
use tokio::time::{timeout, Duration};
use crate::common::{TestEnvironment, TestClient};

#[tokio::test]
async fn test_simple_game() -> Result<()> {
    // Initialize tracing
    let _ = tracing_subscriber::fmt::try_init();
    
    // Create environment
    let mut env = TestEnvironment::new("test_simple_game").await?;
    let (_, server_id) = env.add_server().await?;
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
    
    // Verify both clients received game snapshots and extract game_id and snake_ids
    let (_game_id, snake1_id, snake2_id) = match (&msg1, &msg2) {
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
                    
                    assert_eq!(state1.status, GameStatus::Started { server_id });
                    assert_eq!(state2.status, GameStatus::Started { server_id });
                    
                    // Get snake IDs for each player
                    let snake1_id = state1.players.get(&(env.user_ids()[0] as u32))
                        .expect("Player 1 should have a snake").snake_id;
                    let snake2_id = state1.players.get(&(env.user_ids()[1] as u32))
                        .expect("Player 2 should have a snake").snake_id;
                    
                    println!("Initial game state - Status: {:?}, Snakes count: {}", state1.status, state1.arena.snakes.len());
                    println!("Snake 1 ID: {}, Snake 2 ID: {}", snake1_id, snake2_id);
                    
                    (game_id, snake1_id, snake2_id)
                }
                _ => panic!("Expected Snapshot events, got {:?} and {:?}", event1.event, event2.event),
            }
        }
        _ => panic!("Expected GameEvent messages, got {:?} and {:?}", msg1, msg2),
    };
    
    println!("Game started with ID: {}, Snake 1 ID: {}, Snake 2 ID: {}", 
             _game_id, snake1_id, snake2_id);
    
    // Now simulate the game:
    // Snake 1 (user 1) has snake at x=3 going RIGHT - will hit right wall
    // Snake 2 (user 2) has snake at x=36 going LEFT - will hit left wall
    // We'll make snake 2 turn to avoid crashing
    
    // Snake 2 turns up before the collision point to avoid head-on crash
    // Need to turn before tick 15 to avoid collision at the center
    // tokio::time::sleep(Duration::from_millis(3000)).await; // Wait 10 ticks
    
    client2.send_message(WSMessage::GameCommand(
        GameCommandMessage {
            command_id_client: CommandId {
                tick: 0,
                user_id: env.user_ids()[1] as u32,
                sequence_number: 0,
            },
            command_id_server: None,
            command: GameCommand::Turn { 
                snake_id: snake2_id, 
                direction: Direction::Up 
            },
        }
    )).await?;
    println!("Snake 2 sent turn up command");
    
    // Continue the game and collect events
    let mut snake1_died = false;
    let mut snake2_died = false;
    let start_time = tokio::time::Instant::now();
    
    // Run for up to 10 seconds to see the outcome
    while start_time.elapsed() < Duration::from_secs(10) && (!snake1_died || !snake2_died) {
        // Try to receive events from both clients
        tokio::select! {
            msg = timeout(Duration::from_millis(100), client1.receive_message()) => {
                if let Ok(Ok(WSMessage::GameEvent(event))) = msg {
                    match &event.event {
                        GameEvent::SnakeDied { snake_id } => {
                            if *snake_id == snake1_id {
                                println!("Snake 1 died! event= {:?}", event);
                                snake1_died = true;
                            } else if *snake_id == snake2_id {
                                println!("Snake 2 died! event= {:?}", event);
                                snake2_died = true;
                            }
                        }
                        GameEvent::StatusUpdated { status } => {
                            println!("Client1: Game status updated to {:?}", status);
                            if let GameStatus::Complete { winning_snake_id } = status {
                                println!("Game complete! Winner: {:?}", winning_snake_id);
                                if *winning_snake_id == None {
                                    println!("Game ended in a draw - both snakes died");
                                    return Ok::<(), anyhow::Error>(());
                                }
                                assert_eq!(*winning_snake_id, Some(snake2_id), "Snake 2 should win");
                                return Ok::<(), anyhow::Error>(());
                            }
                        }
                        _ => {
                            println!("Client1 received event: {:?}", event.event);
                        }
                    }
                }
            }
            msg = timeout(Duration::from_millis(100), client2.receive_message()) => {
                if let Ok(Ok(WSMessage::GameEvent(event))) = msg {
                    match &event.event {
                        GameEvent::SnakeDied { snake_id } => {
                            if *snake_id == snake1_id {
                                println!("Snake 1 died! event= {:?}", event);
                                snake1_died = true;
                            } else if *snake_id == snake2_id {
                                println!("Snake 2 died! event= {:?}", event);
                                snake2_died = true;
                            }
                        }
                        GameEvent::StatusUpdated { status } => {
                            println!("Client2: Game status updated to {:?}", status);
                            if let GameStatus::Complete { winning_snake_id } = status {
                                println!("Game complete! Winner: {:?}", winning_snake_id);
                                if *winning_snake_id == None {
                                    println!("Game ended in a draw - both snakes died");
                                    return Ok::<(), anyhow::Error>(());
                                }
                                assert_eq!(*winning_snake_id, Some(snake2_id), "Snake 2 should win");
                                return Ok::<(), anyhow::Error>(());
                            }
                        }
                        _ => {
                            println!("Client2 received event: {:?}", event.event);
                        }
                    }
                }
            }
            else => {
                // No messages available, continue
            }
        }
    }
    
    // Wait for game completion event
    let game_ended = timeout(Duration::from_secs(5), async {
        loop {
            tokio::select! {
                msg = client1.receive_message() => {
                    if let Ok(WSMessage::GameEvent(event)) = msg {
                        if let GameEvent::StatusUpdated { status } = &event.event {
                            if matches!(status, GameStatus::Complete { .. }) {
                                println!("Game completed with status: {:?}", status);
                                return Ok::<(), anyhow::Error>(());
                            }
                        }
                    }
                }
                msg = client2.receive_message() => {
                    if let Ok(WSMessage::GameEvent(event)) = msg {
                        if let GameEvent::StatusUpdated { status } = &event.event {
                            if matches!(status, GameStatus::Complete { .. }) {
                                println!("Game completed with status: {:?}", status);
                                return Ok::<(), anyhow::Error>(());
                            }
                        }
                    }
                }
            }
        }
    }).await;
    
    assert!(game_ended.is_ok(), "Game should have ended with a completion status");
    
    // In this test both snakes actually die at the same time (head-on collision)
    // So we expect both to be dead with no winner
    assert!(snake1_died, "Snake 1 should have died");
    assert!(snake2_died, "Snake 2 should have died");
    
    // Output the replay file location
    if let Some(server) = env.server(0) {
        if let Some(replay_listener) = server.replay_listener() {
            // Wait a bit for the replay to be saved
            tokio::time::sleep(Duration::from_millis(500)).await;
            
            match replay_listener.get_replay_path(_game_id).await {
                Ok(replay_path) => {
                    println!("\n=== REPLAY FILE SAVED ===");
                    println!("Game replay saved to: {}", replay_path.display());
                    println!("========================\n");
                }
                Err(e) => {
                    println!("Warning: Could not get replay path: {}", e);
                }
            }
        }
    }
    
    env.shutdown().await?;
    Ok(())
}