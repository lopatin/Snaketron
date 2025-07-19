mod common;

use anyhow::Result;
use server::ws_server::WSMessage;
use ::common::{GameEvent, GameType, GameCommand, GameCommandMessage, Direction, GameStatus, CommandId};
use tokio::time::{timeout, Duration};
use crate::common::{TestEnvironment, TestClient};
use redis::AsyncCommands;

#[tokio::test]
async fn test_solo_game() -> Result<()> {
    // Initialize tracing
    let _ = tracing_subscriber::fmt::try_init();
    
    // Clean up Redis before starting the test
    let redis_client = redis::Client::open("redis://localhost:6379")?;
    let mut redis_conn = redis_client.get_async_connection().await?;
    let _: () = redis::cmd("FLUSHDB").query_async(&mut redis_conn).await?;
    
    // Small delay to ensure Redis is ready
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Create environment
    let mut env = TestEnvironment::new("test_solo_game").await?;
    let (_, server_id) = env.add_server().await?;
    env.create_user().await?;
    
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    // Connect client
    let mut client = TestClient::connect(&server_addr).await?;
    client.authenticate(env.user_ids()[0]).await?;
    
    println!("Client authenticated");
    
    // Queue for a solo match (max_players: 1)
    client.send_message(WSMessage::QueueForMatch { 
        game_type: GameType::FreeForAll { max_players: 1 } 
    }).await?;
    
    println!("Client queued for solo game");
    
    // Wait for JoinGame message
    let join_msg = timeout(Duration::from_secs(10), async {
        client.receive_message().await
    }).await??;
    
    println!("Client received: {:?}", join_msg);
    
    // Verify client received JoinGame message
    let game_id = match &join_msg {
        WSMessage::JoinGame(id) => *id,
        _ => panic!("Expected JoinGame message, got {:?}", join_msg),
    };
    
    println!("Client joined game {}", game_id);
    
    // Client needs to acknowledge the join by sending JoinGame back
    client.join_game(game_id).await?;
    
    println!("Client acknowledged join");
    
    // Now wait for game snapshot after joining
    let msg = timeout(Duration::from_secs(10), async {
        client.receive_message().await
    }).await??;
    
    println!("Client received after join: {:?}", msg);
    
    // Verify client received game snapshot and extract snake_id
    let (_game_id, snake_id, initial_direction) = match &msg {
        WSMessage::GameEvent(event) => {
            // Check that this is a snapshot for the game
            assert_eq!(event.game_id, game_id);
            
            println!("Client received game snapshot for game {}", game_id);
            
            // Verify the event is a snapshot
            match &event.event {
                GameEvent::Snapshot { game_state } => {
                    println!("Game snapshot verified - game has {} players", game_state.players.len());
                    
                    // Verify only one user is in the game (solo)
                    assert_eq!(game_state.players.len(), 1);
                    assert!(game_state.players.contains_key(&(env.user_ids()[0] as u32)));
                    
                    assert_eq!(game_state.status, GameStatus::Started { server_id });
                    
                    // Get snake ID for the player
                    let snake_id = game_state.players.get(&(env.user_ids()[0] as u32))
                        .expect("Player should have a snake").snake_id;
                    
                    println!("Initial game state - Status: {:?}, Snakes count: {}", game_state.status, game_state.arena.snakes.len());
                    println!("Player (user_id {}): snake_id {}", env.user_ids()[0], snake_id);
                    
                    // Debug: Print snake position and direction
                    let snake = &game_state.arena.snakes[snake_id as usize];
                    println!("Snake - alive: {}, direction: {:?}, body: {:?}, length: {}", 
                        snake.is_alive, snake.direction, snake.body, snake.length());
                    println!("Arena dimensions: {}x{}", game_state.arena.width, game_state.arena.height);
                    
                    // Get initial direction
                    let initial_dir = snake.direction;
                    
                    (game_id, snake_id, initial_dir)
                }
                _ => panic!("Expected Snapshot event, got {:?}", event.event),
            }
        }
        _ => panic!("Expected GameEvent message, got {:?}", msg),
    };
    
    println!("Solo game started with ID: {}, Snake ID: {}, Initial direction: {:?}", 
             _game_id, snake_id, initial_direction);
    
    // In a solo game, we need to survive as long as possible
    // Let's implement a simple strategy: turn when approaching walls
    let user_id = env.user_ids()[0] as u32;
    let mut sequence_number = 0;
    
    // If snake is going towards a wall, turn to avoid it
    // Based on the test_simple_game, snakes typically start in the middle going left or right
    if matches!(initial_direction, Direction::Left | Direction::Right) {
        // Turn up initially to avoid horizontal walls
        tokio::time::sleep(Duration::from_millis(300)).await;
        
        client.send_message(WSMessage::GameCommand(
            GameCommandMessage {
                command_id_client: CommandId {
                    tick: 0,
                    user_id,
                    sequence_number,
                },
                command_id_server: None,
                command: GameCommand::Turn { 
                    snake_id, 
                    direction: Direction::Up 
                },
            }
        )).await?;
        sequence_number += 1;
        
        // After going up for a bit, turn to create a pattern
        tokio::time::sleep(Duration::from_millis(500)).await;
        
        client.send_message(WSMessage::GameCommand(
            GameCommandMessage {
                command_id_client: CommandId {
                    tick: 0,
                    user_id,
                    sequence_number,
                },
                command_id_server: None,
                command: GameCommand::Turn { 
                    snake_id, 
                    direction: Direction::Right 
                },
            }
        )).await?;
        sequence_number += 1;
        
        // Continue with more turns to create a survival pattern
        tokio::time::sleep(Duration::from_millis(500)).await;
        
        client.send_message(WSMessage::GameCommand(
            GameCommandMessage {
                command_id_client: CommandId {
                    tick: 0,
                    user_id,
                    sequence_number,
                },
                command_id_server: None,
                command: GameCommand::Turn { 
                    snake_id, 
                    direction: Direction::Down 
                },
            }
        )).await?;
    }
    
    // Track game events
    let mut snake_died = false;
    let mut death_tick = 0;
    let start_time = tokio::time::Instant::now();
    
    // Run for up to 10 seconds to see the outcome
    while start_time.elapsed() < Duration::from_secs(10) && !snake_died {
        // Try to receive events
        match timeout(Duration::from_millis(100), client.receive_message()).await {
            Ok(Ok(WSMessage::GameEvent(event))) => {
                match &event.event {
                    GameEvent::SnakeDied { snake_id: died_id } => {
                        if *died_id == snake_id {
                            println!("Snake (id={}) died at tick {}!", snake_id, event.tick);
                            snake_died = true;
                            death_tick = event.tick;
                        }
                    }
                    GameEvent::StatusUpdated { status } => {
                        println!("Game status updated to {:?}", status);
                        if let GameStatus::Complete { winning_snake_id } = status {
                            println!("Solo game complete! Final result: {:?}", winning_snake_id);
                            // In a solo game:
                            // - If the snake died, there should be no winner (None)
                            // - If the snake is still alive when game ends, it's the winner
                            if snake_died {
                                assert_eq!(*winning_snake_id, None, "Solo game with dead snake should end with no winner");
                                // Verify the snake survived for a reasonable amount of time
                                assert!(death_tick > 10, "Snake should survive for more than 10 ticks before dying");
                                println!("Snake survived for {} ticks before dying", death_tick);
                            } else {
                                // Game ended but snake didn't die (maybe time limit or other reason)
                                assert_eq!(*winning_snake_id, Some(snake_id), "Solo game with alive snake should have that snake as winner");
                                println!("Game ended with snake still alive");
                            }
                            
                            env.shutdown().await?;
                            return Ok(());
                        }
                    }
                    GameEvent::FoodEaten { snake_id: eating_snake_id, position } => {
                        if *eating_snake_id == snake_id {
                            println!("Snake ate food at position {:?} at tick {}", position, event.tick);
                        }
                    }
                    _ => {
                        println!("Client received event at tick {}: {:?}", event.tick, event.event);
                    }
                }
            }
            Ok(Err(e)) => {
                println!("Error receiving message: {}", e);
            }
            Err(_) => {
                // Timeout - no message available, continue
            }
            _ => {
                // Other message types - ignore
            }
        }
    }
    
    // If we reach here without the game ending, fail the test
    panic!("Solo game should have ended within 10 seconds");
}