mod common;

use anyhow::Result;
use server::ws_server::WSMessage;
use ::common::{GameEvent, GameType, TeamId, GameStatus};
use tokio::time::{timeout, Duration};
use crate::common::{TestEnvironment, TestClient};
use redis::AsyncCommands;

#[tokio::test]
async fn test_duel_game() -> Result<()> {
    // Initialize tracing
    let _ = tracing_subscriber::fmt::try_init();
    
    // Clean up Redis test database before starting the test
    let redis_client = redis::Client::open("redis://localhost:6379/1")?;
    let mut redis_conn = redis_client.get_async_connection().await?;
    let _: () = redis::cmd("FLUSHDB").query_async(&mut redis_conn).await?;
    
    // Small delay to ensure Redis is ready
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Create environment
    let mut env = TestEnvironment::new("test_duel_game").await?;
    let (_, _server_id) = env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;
    
    let server_addr = env.ws_addr(0).expect("Server should exist");
    
    // Connect clients
    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;
    
    client1.authenticate(env.user_ids()[0]).await?;
    client2.authenticate(env.user_ids()[1]).await?;
    
    println!("Clients authenticated");
    
    // Queue for match - TeamMatch with 1 per team (duel mode)
    client1.send_message(WSMessage::QueueForMatch {
        game_type: GameType::TeamMatch { per_team: 1 },
        queue_mode: ::common::QueueMode::Quickmatch,
    }).await?;
    client2.send_message(WSMessage::QueueForMatch {
        game_type: GameType::TeamMatch { per_team: 1 },
        queue_mode: ::common::QueueMode::Quickmatch,
    }).await?;
    
    println!("Clients queued for duel mode");
    
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
                    
                    // Check that both states show the game as started
                    assert!(matches!(state1.status, GameStatus::Started { .. }));
                    assert!(matches!(state2.status, GameStatus::Started { .. }));
                    
                    // Verify game type is TeamMatch
                    assert!(matches!(state1.game_type, GameType::TeamMatch { per_team: 1 }));
                    
                    // Check team zone configuration exists
                    assert!(state1.arena.team_zone_config.is_some(), "Team zone config should exist for duel mode");
                    let team_config = state1.arena.team_zone_config.as_ref().unwrap();
                    assert_eq!(team_config.end_zone_depth, 10, "End zone depth should be 10");
                    
                    // Get snake IDs for each player
                    let snake1_id = state1.players.get(&(env.user_ids()[0] as u32))
                        .expect("Player 1 should have a snake").snake_id;
                    let snake2_id = state1.players.get(&(env.user_ids()[1] as u32))
                        .expect("Player 2 should have a snake").snake_id;
                    
                    println!("Initial game state - Status: {:?}, Snakes count: {}", state1.status, state1.arena.snakes.len());
                    println!("Player 1 (user_id {}): snake_id {}", env.user_ids()[0], snake1_id);
                    println!("Player 2 (user_id {}): snake_id {}", env.user_ids()[1], snake2_id);
                    
                    // Debug: Print snake positions, teams and directions
                    for (idx, snake) in state1.arena.snakes.iter().enumerate() {
                        println!("Snake {} - alive: {}, direction: {:?}, team: {:?}, body: {:?}, length: {}", 
                            idx, snake.is_alive, snake.direction, snake.team_id, snake.body, snake.length());
                        
                        // Verify team assignments
                        assert!(snake.team_id.is_some(), "Snake {} should have a team assignment", idx);
                    }
                    
                    // Check snake starting positions
                    let snake1 = &state1.arena.snakes[snake1_id as usize];
                    let snake2 = &state1.arena.snakes[snake2_id as usize];
                    
                    // Verify teams are different
                    assert_ne!(snake1.team_id, snake2.team_id, "Snakes should be on different teams");
                    
                    // Get head positions
                    let snake1_head = snake1.head().expect("Snake 1 should have a head");
                    let snake2_head = snake2.head().expect("Snake 2 should have a head");
                    
                    println!("Snake 1 head position: {:?}, team: {:?}", snake1_head, snake1.team_id);
                    println!("Snake 2 head position: {:?}, team: {:?}", snake2_head, snake2.team_id);
                    
                    // Verify starting positions in endzones
                    // One snake should be in left endzone (x=5), one in right endzone (x=35 for 40-width arena)
                    let expected_left_x = 5;
                    let expected_right_x = state1.arena.width as i16 - 5;
                    
                    // Check that one snake is in left endzone and one in right
                    let has_left_snake = snake1_head.x == expected_left_x || snake2_head.x == expected_left_x;
                    let has_right_snake = snake1_head.x == expected_right_x || snake2_head.x == expected_right_x;
                    
                    assert!(has_left_snake, "One snake should start in left endzone at x={}", expected_left_x);
                    assert!(has_right_snake, "One snake should start in right endzone at x={}", expected_right_x);
                    
                    // Verify directions match positions
                    // Note: Team assignments alternate - first player gets TeamA, second gets TeamB
                    // TeamA starts in left endzone (x=5), TeamB starts in right endzone (x=55)
                    if snake1_head.x == expected_left_x {
                        assert_eq!(snake1.direction, ::common::Direction::Right, 
                            "Snake in left endzone should face right");
                        // Snake in left endzone should be facing toward TeamB's goal
                    } else {
                        assert_eq!(snake1.direction, ::common::Direction::Left, 
                            "Snake in right endzone should face left");
                        // Snake in right endzone should be facing toward TeamA's goal
                    }
                    
                    if snake2_head.x == expected_left_x {
                        assert_eq!(snake2.direction, ::common::Direction::Right, 
                            "Snake in left endzone should face right");
                    } else {
                        assert_eq!(snake2.direction, ::common::Direction::Left, 
                            "Snake in right endzone should face left");
                    }
                    
                    // Verify that each team has one snake
                    let team_0_count = [snake1, snake2].iter()
                        .filter(|s| s.team_id == Some(TeamId(0))).count();
                    let team_1_count = [snake1, snake2].iter()
                        .filter(|s| s.team_id == Some(TeamId(1))).count();
                    assert_eq!(team_0_count, 1, "There should be exactly one Team 0 snake");
                    assert_eq!(team_1_count, 1, "There should be exactly one Team 1 snake");
                    
                    println!("Arena dimensions: {}x{}", state1.arena.width, state1.arena.height);
                    println!("End zone depth: {}", team_config.end_zone_depth);
                    println!("Goal width: {}", team_config.goal_width);
                    
                    println!("\n✅ Duel mode test passed!");
                    println!("  - Snakes correctly positioned in their endzones");
                    println!("  - Team assignments are correct");
                    println!("  - Snakes facing the correct directions");
                }
                _ => panic!("Expected Snapshot events, got {:?} and {:?}", event1.event, event2.event),
            }
        }
        _ => panic!("Expected GameEvent messages, got {:?} and {:?}", msg1, msg2),
    };
    
    env.shutdown().await?;
    Ok(())
}