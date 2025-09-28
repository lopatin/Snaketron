use common::{GameState, GameType, GameEvent, TeamId};

fn main() {
    // Create a team match game
    let game_type = GameType::TeamMatch {
        rounds_to_win: 1,
        round_time_limit_ms: Some(60000),
    };

    let mut game_state = GameState::new(40, 40, game_type, Some(123), 1000);

    // Set up the game to be in transition
    game_state.is_transitioning = true;
    game_state.current_round = 1;

    println!("Initial state: is_transitioning = {}", game_state.is_transitioning);

    // Apply MatchCompleted event
    game_state.apply_event(
        GameEvent::MatchCompleted {
            winning_team_id: TeamId(0),
        },
        None
    );

    println!("After MatchCompleted: is_transitioning = {}", game_state.is_transitioning);

    // Verify the flag was cleared
    assert!(!game_state.is_transitioning, "is_transitioning should be false after match completes");

    println!("✓ Test passed: is_transitioning is correctly cleared when match completes");
}