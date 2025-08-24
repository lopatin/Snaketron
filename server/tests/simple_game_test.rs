mod common;

use anyhow::Result;
use server::ws_server::WSMessage;
use ::common::{GameEvent, GameType, GameCommand, GameCommandMessage, Direction, GameStatus, CommandId};
use tokio::time::{timeout, Duration};
use crate::common::{TestEnvironment, TestClient};
use redis::AsyncCommands;

#[tokio::test]
async fn test_simple_game() -> Result<()> {
    // Initialize tracing
    let _ = tracing_subscriber::fmt::try_init();
    
    // Clean up Redis before starting the test
    let redis_client = redis::Client::open("redis://localhost:6379")?;
    let mut redis_conn = redis_client.get_async_connection().await?;
    let _: () = redis::cmd("FLUSHDB").query_async(&mut redis_conn).await?;
    
    // Small delay to ensure Redis is ready
    tokio::time::sleep(Duration::from_millis(100)).await;
    
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
    
    // Wait for JoinGame messages first
    let join_msg1 = timeout(Duration::from_secs(10), async {
        client1.receive_message().await
    }).await??;
    
    println!("Client1 received: {:?}", join_msg1);
    
    let join_msg2 = timeout(Duration::from_secs(10), async {
        client2.receive_message().await
    }).await??;
    
    println!("Client2 received: {:?}", join_msg2);
    
    // Verify both clients received JoinGame messages for the same game
    let game_id = match (&join_msg1, &join_msg2) {
        (WSMessage::JoinGame(id1), WSMessage::JoinGame(id2)) => {
            assert_eq!(id1, id2, "Both clients should join the same game");
            *id1
        }
        _ => panic!("Expected JoinGame messages, got {:?} and {:?}", join_msg1, join_msg2),
    };
    
    println!("Both clients joined game {}", game_id);
    
    // Clients need to acknowledge the join by sending JoinGame back
    client1.join_game(game_id).await?;
    client2.join_game(game_id).await?;
    
    println!("Clients acknowledged join");
    
    // Now wait for game snapshots after joining
    let msg1 = timeout(Duration::from_secs(10), async {
        client1.receive_message().await
    }).await??;
    
    println!("Client1 received after join: {:?}", msg1);
    
    let msg2 = timeout(Duration::from_secs(10), async {
        client2.receive_message().await
    }).await??;
    
    println!("Client2 received after join: {:?}", msg2);
    
    // Verify both clients received game snapshots and extract game_id and snake_ids
    let (_game_id, snake1_id, snake2_id, snake1_dir, snake2_dir) = match (&msg1, &msg2) {
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
                    
                    // Check that both states show the game as started (server_id may vary)
                    assert!(matches!(state1.status, GameStatus::Started { .. }));
                    assert!(matches!(state2.status, GameStatus::Started { .. }));
                    
                    // Get snake IDs for each player
                    let snake1_id = state1.players.get(&(env.user_ids()[0] as u32))
                        .expect("Player 1 should have a snake").snake_id;
                    let snake2_id = state1.players.get(&(env.user_ids()[1] as u32))
                        .expect("Player 2 should have a snake").snake_id;
                    
                    println!("Initial game state - Status: {:?}, Snakes count: {}", state1.status, state1.arena.snakes.len());
                    println!("Player 1 (user_id {}): snake_id {}", env.user_ids()[0], snake1_id);
                    println!("Player 2 (user_id {}): snake_id {}", env.user_ids()[1], snake2_id);
                    
                    // Debug: Print snake positions and directions
                    for (idx, snake) in state1.arena.snakes.iter().enumerate() {
                        println!("Snake {} - alive: {}, direction: {:?}, body: {:?}, length: {}", 
                            idx, snake.is_alive, snake.direction, snake.body, snake.length());
                    }
                    println!("Arena dimensions: {}x{}", state1.arena.width, state1.arena.height);
                    
                    // Get initial directions
                    let snake1_dir = state1.arena.snakes[snake1_id as usize].direction;
                    let snake2_dir = state1.arena.snakes[snake2_id as usize].direction;
                    
                    (game_id, snake1_id, snake2_id, snake1_dir, snake2_dir)
                }
                _ => panic!("Expected Snapshot events, got {:?} and {:?}", event1.event, event2.event),
            }
        }
        _ => panic!("Expected GameEvent messages, got {:?} and {:?}", msg1, msg2),
    };
    
    println!("Game started with ID: {}, Snake 1 ID: {}, Snake 2 ID: {}", 
             _game_id, snake1_id, snake2_id);
    println!("Snake 1 initial direction: {:?}, Snake 2 initial direction: {:?}", snake1_dir, snake2_dir);
    
    // Now simulate the game:
    // We need to determine which snake each player controls and their positions
    // Player assignments to snake positions can vary due to HashMap iteration order
    
    // Determine which player has which snake based on initial direction
    let (turning_player_client, turning_snake_id, turning_user_id) = {
        // The snake going LEFT (from right side) needs to turn to avoid wall
        if matches!(snake1_dir, Direction::Left) {
            println!("Player 1's snake is going LEFT, needs to turn");
            (&mut client1, snake1_id, env.user_ids()[0] as u32)
        } else {
            println!("Player 2's snake is going LEFT, needs to turn");
            (&mut client2, snake2_id, env.user_ids()[1] as u32)
        }
    };
    
    // Store which snake should win (the one that turns)
    let expected_winner = turning_snake_id;
    
    // Send turn command early to ensure the snake turns before hitting the wall
    // Both snakes start 35 cells from opposite walls, so they'd hit at tick 35
    // But they're dying at tick 15, which suggests a different issue
    // Let's turn very early - within first few ticks
    tokio::time::sleep(Duration::from_millis(300)).await;
    
    turning_player_client.send_message(WSMessage::GameCommand(
        GameCommandMessage {
            command_id_client: CommandId {
                tick: 0,
                user_id: turning_user_id,
                sequence_number: 0,
            },
            command_id_server: None,
            command: GameCommand::Turn { 
                snake_id: turning_snake_id, 
                direction: Direction::Up 
            },
        }
    )).await?;
    
    // Wait then turn left to continue avoiding walls
    tokio::time::sleep(Duration::from_millis(300)).await;
     
    turning_player_client.send_message(WSMessage::GameCommand(
        GameCommandMessage {
            command_id_client: CommandId {
                tick: 0,
                user_id: turning_user_id,
                sequence_number: 0,
            },
            command_id_server: None,
            command: GameCommand::Turn { 
                snake_id: turning_snake_id, 
                direction: Direction::Left 
            },
        }
    )).await?;
    
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
                                println!("Snake 1 (id={}) died at tick {}! Initial direction was {:?}", 
                                    snake1_id, event.tick, snake1_dir);
                                snake1_died = true;
                            } else if *snake_id == snake2_id {
                                println!("Snake 2 (id={}) died at tick {}! Initial direction was {:?}", 
                                    snake2_id, event.tick, snake2_dir);
                                snake2_died = true;
                            }
                        }
                        GameEvent::StatusUpdated { status } => {
                            println!("Client1: Game status updated to {:?}", status);
                            if let GameStatus::Complete { winning_snake_id } = status {
                                println!("Game complete! Winner: {:?}", winning_snake_id);
                                assert_ne!(*winning_snake_id, None, "Game should not end in a draw");
                                assert_eq!(*winning_snake_id, Some(expected_winner), "The snake that turned should win");
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
                                println!("Snake 1 (id={}) died at tick {}! Initial direction was {:?}", 
                                    snake1_id, event.tick, snake1_dir);
                                snake1_died = true;
                            } else if *snake_id == snake2_id {
                                println!("Snake 2 (id={}) died at tick {}! Initial direction was {:?}", 
                                    snake2_id, event.tick, snake2_dir);
                                snake2_died = true;
                            }
                        }
                        GameEvent::StatusUpdated { status } => {
                            println!("Client2: Game status updated to {:?}", status);
                            if let GameStatus::Complete { winning_snake_id } = status {
                                println!("Game complete! Winner: {:?}", winning_snake_id);
                                assert_ne!(*winning_snake_id, None, "Game should not end in a draw");
                                assert_eq!(*winning_snake_id, Some(expected_winner), "The snake that turned should win");
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
    
    // Debug final state
    println!("Final state - Snake 1 (id {}) died: {}, Snake 2 (id {}) died: {}", 
        snake1_id, snake1_died, snake2_id, snake2_died);
    
    // The test should not end in a draw
    assert!(!(snake1_died && snake2_died), "Game should not end in a draw - only one snake should die");
    
    // The snake that went straight (RIGHT direction) should die hitting the wall
    // The snake that turned (originally LEFT direction) should survive
    let right_going_snake_died = if matches!(snake1_dir, Direction::Right) {
        snake1_died
    } else {
        snake2_died
    };
    
    let left_going_snake_died = if matches!(snake1_dir, Direction::Left) {
        snake1_died
    } else {
        snake2_died
    };
    
    assert!(right_going_snake_died, "The snake going RIGHT should have died hitting the wall");
    assert!(!left_going_snake_died, "The snake that turned (originally going LEFT) should survive");
    
    // Output the replay file location
    if let Some(server) = env.server(0) {
        // if let Some(replay_listener) = server.replay_listener() {
        //     // Wait a bit for the replay to be saved
        //     tokio::time::sleep(Duration::from_millis(500)).await;
        //     
        //     match replay_listener.get_replay_path(_game_id).await {
        //         Ok(replay_path) => {
        //             println!("\n=== REPLAY FILE SAVED ===");
        //             println!("Game replay saved to: {}", replay_path.display());
        //             println!("========================\n");
        //         }
        //         Err(e) => {
        //             println!("Warning: Could not get replay path: {}", e);
        //         }
        //     }
        // }
    }
    
    env.shutdown().await?;
    Ok(())
}