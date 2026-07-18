mod common;

use crate::common::{TestClient, TestEnvironment};
use ::common::{
    CommandId, Direction, GameCommand, GameCommandMessage, GameEvent, GameStatus, GameType,
};
use anyhow::Result;
use server::ws_server::WSMessage;
use tokio::time::{Duration, timeout};

/// Wait for a JoinGame message, skipping unrelated messages (e.g. UserCountUpdate).
async fn wait_for_join_game(client: &mut TestClient, label: &str) -> Result<u32> {
    timeout(Duration::from_secs(10), async {
        loop {
            match client.receive_message().await? {
                WSMessage::JoinGame(id) => break Ok::<u32, anyhow::Error>(id),
                other => println!(
                    "{}: ignoring message while waiting for JoinGame: {:?}",
                    label, other
                ),
            }
        }
    })
    .await?
}

/// Wait for a GameEvent carrying a Snapshot, skipping unrelated messages.
async fn wait_for_snapshot(client: &mut TestClient, label: &str) -> Result<WSMessage> {
    timeout(Duration::from_secs(10), async {
        loop {
            let msg = client.receive_message().await?;
            match &msg {
                WSMessage::GameEvent(event)
                    if matches!(event.event, GameEvent::Snapshot { .. }) =>
                {
                    break Ok::<WSMessage, anyhow::Error>(msg);
                }
                other => println!(
                    "{}: ignoring message while waiting for snapshot: {:?}",
                    label, other
                ),
            }
        }
    })
    .await?
}

#[tokio::test]
async fn test_simple_game() -> Result<()> {
    // Initialize tracing
    let _ = tracing_subscriber::fmt::try_init();

    // Clean up the Redis test database (db 1, used by TestEnvironment) before starting
    let redis_client = redis::Client::open("redis://127.0.0.1:6379/1")?;
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await?;
    let _: () = redis::cmd("FLUSHDB").query_async(&mut redis_conn).await?;

    // Small delay to ensure Redis is ready
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create environment
    let mut env = TestEnvironment::new("test_simple_game").await?;
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

    // Queue for match
    client1
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 2 },
            queue_mode: ::common::QueueMode::Quickmatch,
        })
        .await?;
    client2
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 2 },
            queue_mode: ::common::QueueMode::Quickmatch,
        })
        .await?;

    println!("Clients queued");

    // Wait for JoinGame messages first, skipping unrelated messages
    let join_id1 = wait_for_join_game(&mut client1, "Client1").await?;
    println!("Client1 received JoinGame({})", join_id1);

    let join_id2 = wait_for_join_game(&mut client2, "Client2").await?;
    println!("Client2 received JoinGame({})", join_id2);

    // Verify both clients received JoinGame messages for the same game
    assert_eq!(join_id1, join_id2, "Both clients should join the same game");
    let game_id = join_id1;

    println!("Both clients joined game {}", game_id);

    // Clients need to acknowledge the join by sending JoinGame back
    client1.join_game(game_id).await?;
    client2.join_game(game_id).await?;

    println!("Clients acknowledged join");

    // Now wait for game snapshots after joining, skipping unrelated messages
    let msg1 = wait_for_snapshot(&mut client1, "Client1").await?;
    println!("Client1 received after join: {:?}", msg1);

    let msg2 = wait_for_snapshot(&mut client2, "Client2").await?;
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
                (
                    GameEvent::Snapshot { game_state: state1 },
                    GameEvent::Snapshot { game_state: state2 },
                ) => {
                    println!(
                        "Game snapshot verified - game has {} players",
                        state1.players.len()
                    );

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
                    let snake1_id = state1
                        .players
                        .get(&(env.user_ids()[0] as u32))
                        .expect("Player 1 should have a snake")
                        .snake_id;
                    let snake2_id = state1
                        .players
                        .get(&(env.user_ids()[1] as u32))
                        .expect("Player 2 should have a snake")
                        .snake_id;

                    println!(
                        "Initial game state - Status: {:?}, Snakes count: {}",
                        state1.status,
                        state1.arena.snakes.len()
                    );
                    println!(
                        "Player 1 (user_id {}): snake_id {}",
                        env.user_ids()[0],
                        snake1_id
                    );
                    println!(
                        "Player 2 (user_id {}): snake_id {}",
                        env.user_ids()[1],
                        snake2_id
                    );

                    // Debug: Print snake positions and directions
                    for (idx, snake) in state1.arena.snakes.iter().enumerate() {
                        println!(
                            "Snake {} - alive: {}, direction: {:?}, body: {:?}, length: {}",
                            idx,
                            snake.is_alive,
                            snake.direction,
                            snake.body,
                            snake.length()
                        );
                    }
                    println!(
                        "Arena dimensions: {}x{}",
                        state1.arena.width, state1.arena.height
                    );

                    // Get initial directions
                    let snake1_dir = state1.arena.snakes[snake1_id as usize].direction;
                    let snake2_dir = state1.arena.snakes[snake2_id as usize].direction;

                    (game_id, snake1_id, snake2_id, snake1_dir, snake2_dir)
                }
                _ => panic!(
                    "Expected Snapshot events, got {:?} and {:?}",
                    event1.event, event2.event
                ),
            }
        }
        _ => panic!("Expected GameEvent messages, got {:?} and {:?}", msg1, msg2),
    };

    println!(
        "Game started with ID: {}, Snake 1 ID: {}, Snake 2 ID: {}",
        _game_id, snake1_id, snake2_id
    );
    println!(
        "Snake 1 initial direction: {:?}, Snake 2 initial direction: {:?}",
        snake1_dir, snake2_dir
    );

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

    // The two snakes spawn facing each other on the same row, so if neither
    // turns they collide head-on at ~tick 15. The game also begins with a ~3s
    // countdown (GAME_START_DELAY_MS) during which it stays at tick 0;
    // commands sent during the countdown would all be scheduled for the same
    // tick (and the two turns would cancel out), so wait until ticking has
    // started before turning.
    timeout(Duration::from_secs(10), async {
        loop {
            if let WSMessage::GameEvent(event) = turning_player_client.receive_message().await?
                && event.tick >= 1
            {
                break Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;

    turning_player_client
        .send_message(WSMessage::GameCommand(GameCommandMessage {
            command_id_client: CommandId {
                tick: 0,
                user_id: turning_user_id,
                sequence_number: 0,
            },
            command_id_server: None,
            command: GameCommand::Turn {
                snake_id: turning_snake_id,
                direction: Direction::Up,
            },
        }))
        .await?;

    // Wait until the Up turn has actually executed before scheduling the next
    // turn, so the two turns land on different ticks.
    timeout(Duration::from_secs(10), async {
        loop {
            if let WSMessage::GameEvent(event) = turning_player_client.receive_message().await?
                && let GameEvent::SnakeTurned {
                    snake_id,
                    direction: Direction::Up,
                } = event.event
                && snake_id == turning_snake_id
            {
                break Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;

    // Turn left to continue avoiding walls
    turning_player_client
        .send_message(WSMessage::GameCommand(GameCommandMessage {
            command_id_client: CommandId {
                tick: 0,
                user_id: turning_user_id,
                sequence_number: 1,
            },
            command_id_server: None,
            command: GameCommand::Turn {
                snake_id: turning_snake_id,
                direction: Direction::Left,
            },
        }))
        .await?;

    // Track deaths and wait for the game to complete.
    // Current FFA semantics: the game completes only when ALL snakes are dead,
    // and the final status carries no winner (winning_snake_id is None; XP is
    // awarded to players instead). The snake that turned should simply outlive
    // the snake that kept going straight into the head-on collision course.
    let mut snake1_death_tick: Option<u32> = None;
    let mut snake2_death_tick: Option<u32> = None;
    let mut final_winning_snake_id: Option<Option<u32>> = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);

    while final_winning_snake_id.is_none() && tokio::time::Instant::now() < deadline {
        let msg = tokio::select! {
            msg = timeout(Duration::from_millis(500), client1.receive_message()) => msg,
            msg = timeout(Duration::from_millis(500), client2.receive_message()) => msg,
        };
        let Ok(Ok(WSMessage::GameEvent(event))) = msg else {
            continue;
        };
        match &event.event {
            GameEvent::SnakeDied { snake_id } => {
                if *snake_id == snake1_id && snake1_death_tick.is_none() {
                    println!(
                        "Snake 1 (id={}) died at tick {}! Initial direction was {:?}",
                        snake1_id, event.tick, snake1_dir
                    );
                    snake1_death_tick = Some(event.tick);
                } else if *snake_id == snake2_id && snake2_death_tick.is_none() {
                    println!(
                        "Snake 2 (id={}) died at tick {}! Initial direction was {:?}",
                        snake2_id, event.tick, snake2_dir
                    );
                    snake2_death_tick = Some(event.tick);
                }
            }
            GameEvent::StatusUpdated { status } => {
                println!("Game status updated to {:?}", status);
                if let GameStatus::Complete { winning_snake_id } = status {
                    final_winning_snake_id = Some(*winning_snake_id);
                }
            }
            _ => {}
        }
    }

    let winning_snake_id =
        final_winning_snake_id.expect("Game should complete within the deadline");

    // Current FFA semantics: no winner is recorded in the final status.
    assert_eq!(
        winning_snake_id, None,
        "FFA games complete with no winner recorded"
    );

    let snake1_death =
        snake1_death_tick.expect("Snake 1 should have died before the game completed");
    let snake2_death =
        snake2_death_tick.expect("Snake 2 should have died before the game completed");

    // The snake that kept going straight dies first (right wall at ~tick 36);
    // the snake that turned avoids the head-on collision and outlives it.
    let (turned_death, straight_death) = if turning_snake_id == snake1_id {
        (snake1_death, snake2_death)
    } else {
        (snake2_death, snake1_death)
    };
    assert!(
        turned_death > straight_death,
        "The snake that turned (died at tick {}) should outlive the snake that went straight (died at tick {})",
        turned_death,
        straight_death
    );

    // Output the replay file location
    if let Some(_server) = env.server(0) {
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
