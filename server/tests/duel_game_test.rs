mod common;

use crate::common::{TestClient, TestEnvironment};
use ::common::{
    CommandId, Direction, GameCommand, GameCommandMessage, GameEvent, GameStatus, GameType, TeamId,
};
use anyhow::Result;
use server::ws_server::WSMessage;
use tokio::time::{Duration, timeout};

// TestEnvironment sets process-global env vars, so tests in this binary must
// not run concurrently.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
async fn test_duel_game() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;

    // Initialize tracing
    let _ = tracing_subscriber::fmt::try_init();

    // Clean up Redis test database before starting the test
    let redis_client = redis::Client::open("redis://localhost:6379/1")?;
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await?;
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
    client1
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::TeamMatch { per_team: 1 },
            queue_mode: ::common::QueueMode::Quickmatch,
        })
        .await?;
    client2
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::TeamMatch { per_team: 1 },
            queue_mode: ::common::QueueMode::Quickmatch,
        })
        .await?;

    println!("Clients queued for duel mode");

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
    match (&msg1, &msg2) {
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

                    // Check that both states show the game as started
                    assert!(matches!(state1.status, GameStatus::Started { .. }));
                    assert!(matches!(state2.status, GameStatus::Started { .. }));

                    // Verify game type is TeamMatch
                    assert!(matches!(
                        state1.game_type,
                        GameType::TeamMatch { per_team: 1 }
                    ));

                    // Check team zone configuration exists
                    assert!(
                        state1.arena.team_zone_config.is_some(),
                        "Team zone config should exist for duel mode"
                    );
                    let team_config = state1.arena.team_zone_config.as_ref().unwrap();
                    assert_eq!(
                        team_config.end_zone_depth, 10,
                        "End zone depth should be 10"
                    );

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

                    // Debug: Print snake positions, teams and directions
                    for (idx, snake) in state1.arena.snakes.iter().enumerate() {
                        println!(
                            "Snake {} - alive: {}, direction: {:?}, team: {:?}, body: {:?}, length: {}",
                            idx,
                            snake.is_alive,
                            snake.direction,
                            snake.team_id,
                            snake.body,
                            snake.length()
                        );

                        // Verify team assignments
                        assert!(
                            snake.team_id.is_some(),
                            "Snake {} should have a team assignment",
                            idx
                        );
                    }

                    // Check snake starting positions
                    let snake1 = &state1.arena.snakes[snake1_id as usize];
                    let snake2 = &state1.arena.snakes[snake2_id as usize];

                    // Verify teams are different
                    assert_ne!(
                        snake1.team_id, snake2.team_id,
                        "Snakes should be on different teams"
                    );

                    // Get head positions
                    let snake1_head = snake1.head().expect("Snake 1 should have a head");
                    let snake2_head = snake2.head().expect("Snake 2 should have a head");

                    println!(
                        "Snake 1 head position: {:?}, team: {:?}",
                        snake1_head, snake1.team_id
                    );
                    println!(
                        "Snake 2 head position: {:?}, team: {:?}",
                        snake2_head, snake2.team_id
                    );

                    // Verify starting positions in endzones.
                    // Snakes spawn one cell inside their endzone boundary, facing the
                    // goal gate: left head at end_zone_depth - 2, right head at
                    // width - end_zone_depth + 1 (see calculate_team_starting_positions).
                    let end_zone_depth = team_config.end_zone_depth as i16;
                    let expected_left_x = end_zone_depth - 2;
                    let expected_right_x = state1.arena.width as i16 - end_zone_depth + 1;

                    // Check that one snake is in left endzone and one in right
                    let has_left_snake =
                        snake1_head.x == expected_left_x || snake2_head.x == expected_left_x;
                    let has_right_snake =
                        snake1_head.x == expected_right_x || snake2_head.x == expected_right_x;

                    assert!(
                        has_left_snake,
                        "One snake should start in left endzone at x={}",
                        expected_left_x
                    );
                    assert!(
                        has_right_snake,
                        "One snake should start in right endzone at x={}",
                        expected_right_x
                    );

                    // Verify directions match positions
                    // Note: Team assignments alternate - first player gets TeamA, second gets TeamB
                    // TeamA (TeamId(0)) starts in the left endzone, TeamB (TeamId(1)) in the right
                    if snake1_head.x == expected_left_x {
                        assert_eq!(
                            snake1.direction,
                            ::common::Direction::Right,
                            "Snake in left endzone should face right"
                        );
                        // Snake in left endzone should be facing toward TeamB's goal
                    } else {
                        assert_eq!(
                            snake1.direction,
                            ::common::Direction::Left,
                            "Snake in right endzone should face left"
                        );
                        // Snake in right endzone should be facing toward TeamA's goal
                    }

                    if snake2_head.x == expected_left_x {
                        assert_eq!(
                            snake2.direction,
                            ::common::Direction::Right,
                            "Snake in left endzone should face right"
                        );
                    } else {
                        assert_eq!(
                            snake2.direction,
                            ::common::Direction::Left,
                            "Snake in right endzone should face left"
                        );
                    }

                    // Verify that each team has one snake
                    let team_0_count = [snake1, snake2]
                        .iter()
                        .filter(|s| s.team_id == Some(TeamId(0)))
                        .count();
                    let team_1_count = [snake1, snake2]
                        .iter()
                        .filter(|s| s.team_id == Some(TeamId(1)))
                        .count();
                    assert_eq!(team_0_count, 1, "There should be exactly one Team 0 snake");
                    assert_eq!(team_1_count, 1, "There should be exactly one Team 1 snake");

                    println!(
                        "Arena dimensions: {}x{}",
                        state1.arena.width, state1.arena.height
                    );
                    println!("End zone depth: {}", team_config.end_zone_depth);
                    println!("Goal width: {}", team_config.goal_width);

                    println!("\n✅ Duel mode test passed!");
                    println!("  - Snakes correctly positioned in their endzones");
                    println!("  - Team assignments are correct");
                    println!("  - Snakes facing the correct directions");
                }
                _ => panic!(
                    "Expected Snapshot events, got {:?} and {:?}",
                    event1.event, event2.event
                ),
            }
        }
        _ => panic!("Expected GameEvent messages, got {:?} and {:?}", msg1, msg2),
    };

    env.shutdown().await?;
    Ok(())
}

/// A Turn command targeting another player's snake must be ignored by the
/// executor, even when the attacker also spoofs the victim's user_id in the
/// client command id. The victim's own command (sent second, in the opposite
/// direction) is the positive control proving the command pipeline is live.
#[tokio::test]
async fn test_turn_for_unowned_snake_is_ignored() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;

    let _ = tracing_subscriber::fmt::try_init();

    let redis_client = redis::Client::open("redis://localhost:6379/1")?;
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await?;
    let _: () = redis::cmd("FLUSHDB").query_async(&mut redis_conn).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut env = TestEnvironment::new("test_turn_for_unowned_snake_is_ignored").await?;
    let (_, _server_id) = env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;

    let server_addr = env.ws_addr(0).expect("Server should exist");

    let mut attacker = TestClient::connect(&server_addr).await?;
    let mut victim = TestClient::connect(&server_addr).await?;

    let attacker_user_id = env.user_ids()[0] as u32;
    let victim_user_id = env.user_ids()[1] as u32;

    attacker.authenticate(env.user_ids()[0]).await?;
    victim.authenticate(env.user_ids()[1]).await?;

    attacker
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::TeamMatch { per_team: 1 },
            queue_mode: ::common::QueueMode::Quickmatch,
        })
        .await?;
    victim
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::TeamMatch { per_team: 1 },
            queue_mode: ::common::QueueMode::Quickmatch,
        })
        .await?;

    let join_id1 = wait_for_join_game(&mut attacker, "Attacker").await?;
    let join_id2 = wait_for_join_game(&mut victim, "Victim").await?;
    assert_eq!(join_id1, join_id2, "Both clients should join the same game");
    let game_id = join_id1;

    attacker.join_game(game_id).await?;
    victim.join_game(game_id).await?;

    let snapshot_msg = wait_for_snapshot(&mut attacker, "Attacker").await?;
    wait_for_snapshot(&mut victim, "Victim").await?;

    let victim_snake_id = match &snapshot_msg {
        WSMessage::GameEvent(event) => match &event.event {
            GameEvent::Snapshot { game_state } => {
                game_state
                    .players
                    .get(&victim_user_id)
                    .expect("Victim should have a snake")
                    .snake_id
            }
            other => panic!("Expected Snapshot event, got {:?}", other),
        },
        other => panic!("Expected GameEvent message, got {:?}", other),
    };

    // Wait until the countdown is over and the game is actually ticking, so
    // both commands below execute promptly (and long before either snake can
    // reach a wall — duel snakes spawn facing the length of the arena).
    timeout(Duration::from_secs(10), async {
        loop {
            if let WSMessage::GameEvent(event) = attacker.receive_message().await?
                && event.tick >= 1
            {
                break Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;

    // The attack: sent from the attacker's connection, targeting the victim's
    // snake AND claiming the victim's user_id. Duel snakes face Left/Right,
    // so Up and Down are both legal turns.
    attacker
        .send_game_command(
            game_id,
            GameCommandMessage {
                command_id_client: CommandId {
                    tick: 0,
                    user_id: victim_user_id,
                    sequence_number: 0,
                },
                command_id_server: None,
                command: GameCommand::Turn {
                    snake_id: victim_snake_id,
                    direction: Direction::Up,
                },
            },
        )
        .await?;

    // Positive control, sent after the attack: the victim legitimately turns
    // its own snake the other way.
    victim
        .send_game_command(
            game_id,
            GameCommandMessage {
                command_id_client: CommandId {
                    tick: 0,
                    user_id: victim_user_id,
                    sequence_number: 0,
                },
                command_id_server: None,
                command: GameCommand::Turn {
                    snake_id: victim_snake_id,
                    direction: Direction::Down,
                },
            },
        )
        .await?;

    // The forged command was submitted first, so if the executor accepted it,
    // its CommandScheduled { ..Up } would be published before the legitimate
    // command's. Scan until the legitimate turn is executed, then keep
    // scanning briefly for stragglers.
    let mut legit_turn_executed = false;
    let forbidden = |event: &GameEvent| match event {
        GameEvent::SnakeTurned {
            snake_id,
            direction,
        } => *snake_id == victim_snake_id && *direction == Direction::Up,
        GameEvent::CommandScheduled { command_message } => {
            command_message.command
                == GameCommand::Turn {
                    snake_id: victim_snake_id,
                    direction: Direction::Up,
                }
        }
        _ => false,
    };

    timeout(Duration::from_secs(10), async {
        while !legit_turn_executed {
            if let WSMessage::GameEvent(event) = attacker.receive_message().await? {
                assert!(
                    !forbidden(&event.event),
                    "forged turn for another player's snake reached the engine: {:?}",
                    event.event
                );
                if let GameEvent::SnakeTurned {
                    snake_id,
                    direction,
                } = &event.event
                    && *snake_id == victim_snake_id
                    && *direction == Direction::Down
                {
                    legit_turn_executed = true;
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    })
    .await??;

    // Tail scan: nothing stemming from the forged command may show up late.
    let tail_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    while tokio::time::Instant::now() < tail_deadline {
        match timeout(Duration::from_millis(100), attacker.receive_message()).await {
            Ok(Ok(WSMessage::GameEvent(event))) => {
                assert!(
                    !forbidden(&event.event),
                    "forged turn surfaced after the legitimate one: {:?}",
                    event.event
                );
            }
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => {} // no message within 100ms; keep scanning until the deadline
        }
    }

    println!(
        "✅ Forged turn (attacker user {} -> snake {}) was ignored; legitimate turn executed",
        attacker_user_id, victim_snake_id
    );

    env.shutdown().await?;
    Ok(())
}
