use anyhow::Result;
use common::{
    CommandId, Direction, GameCommand, GameCommandMessage, GameEvent, GameEventMessage, GameState,
    GameStatus, QueueMode,
};
use std::time::SystemTime;
use terminal::replay::player::ReplayPlayer;
use terminal::replay::{PlayerInfo, ReplayData, ReplayMetadata, TimestampedEvent};

#[test]
fn test_replay_player_with_tick_forward() -> Result<()> {
    // Create initial game state
    let start_ms = 1700000000000; // Fixed timestamp for testing
    let mut initial_state = GameState::new(
        20,
        20,
        common::GameType::FreeForAll { max_players: 2 },
        QueueMode::Quickmatch,
        Some(12345),
        start_ms,
    );
    initial_state.add_player(1)?;
    initial_state.add_player(2)?;
    initial_state.status = GameStatus::Started { server_id: 1 };

    // Create replay data
    let metadata = ReplayMetadata {
        players: vec![
            PlayerInfo {
                user_id: 1,
                snake_id: 0,
                username: "Player1".to_string(),
            },
            PlayerInfo {
                user_id: 2,
                snake_id: 1,
                username: "Player2".to_string(),
            },
        ],
        start_time: SystemTime::now(),
        end_time: SystemTime::now(),
        final_status: GameStatus::Complete {
            winning_snake_id: Some(0),
        },
    };

    let mut events = vec![];

    // Add a CommandScheduled event at tick 1
    let command_msg = GameCommandMessage {
        command_id_client: CommandId {
            tick: 1,
            user_id: 1,
            sequence_number: 1,
        },
        command_id_server: Some(CommandId {
            tick: 3,
            user_id: 1,
            sequence_number: 1,
        }),
        command: GameCommand::Turn {
            snake_id: 0,
            direction: Direction::Up,
        },
    };

    events.push(TimestampedEvent {
        tick: 1,
        timestamp: SystemTime::now(),
        event: GameEventMessage {
            game_id: 1,
            tick: 1,
            sequence: 1,
            user_id: Some(1),
            event: GameEvent::CommandScheduled {
                command_message: command_msg,
            },
        },
    });

    // Add SnakeTurned event at tick 3 (when command executes)
    events.push(TimestampedEvent {
        tick: 3,
        timestamp: SystemTime::now(),
        event: GameEventMessage {
            game_id: 1,
            tick: 3,
            sequence: 2,
            user_id: None,
            event: GameEvent::SnakeTurned {
                snake_id: 0,
                direction: Direction::Up,
            },
        },
    });

    let replay_data = ReplayData {
        metadata,
        initial_state: initial_state.clone(),
        events,
    };

    // Create replay player
    let mut player = ReplayPlayer::new(replay_data);

    // Initial state check
    assert_eq!(player.current_tick(), 0);
    assert_eq!(player.current_state().tick, 0);

    // Check initial snake directions
    let initial_direction = player.current_state().arena.snakes[0].direction;

    // Step forward to tick 1 - command should be scheduled
    player.step_forward(1);
    assert_eq!(player.current_tick(), 1);

    // Step forward to tick 3 - command is scheduled to execute at tick 3
    player.step_forward(2);
    assert_eq!(player.current_tick(), 3);

    // At tick 3, the command should be in the queue but not yet executed
    let snake = &player.current_state().arena.snakes[0];
    assert_eq!(snake.direction, initial_direction); // Still facing original direction

    // The command is scheduled for tick 3, so it executes when tick_forward processes tick 3
    // We see the result at tick 4
    player.step_forward(1);
    assert_eq!(player.current_tick(), 4);

    // The snake should have turned up
    let snake = &player.current_state().arena.snakes[0];
    assert_eq!(snake.direction, Direction::Up);

    // Test backward stepping (should rebuild from start)
    player.step_backward(3);
    assert_eq!(player.current_tick(), 1);

    // Snake should still be facing its original direction at tick 1
    let snake = &player.current_state().arena.snakes[0];
    assert_eq!(snake.direction, initial_direction);

    // Step forward again to verify consistency
    player.step_forward(3);
    assert_eq!(player.current_tick(), 4);
    let snake = &player.current_state().arena.snakes[0];
    assert_eq!(snake.direction, Direction::Up);

    // Test seeking
    player.seek_to_tick(0);
    assert_eq!(player.current_tick(), 0);

    player.seek_to_tick(5);
    assert_eq!(player.current_tick(), 5);

    println!("Replay player test passed!");

    Ok(())
}
