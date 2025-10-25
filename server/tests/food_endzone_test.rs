use anyhow::Result;
use common::{GameState, GameType, TeamId};

#[test]
fn test_initial_food_spawning() -> Result<()> {
    // Create a FreeForAll game
    let mut game_state = GameState::new(
        40,
        40,
        GameType::FreeForAll { max_players: 4 },
        Some(12345),
        0,
    );

    // Add players
    game_state.add_player(1, Some("Player1".to_string()))?;
    game_state.add_player(2, Some("Player2".to_string()))?;

    // Verify no food initially
    assert_eq!(game_state.arena.food.len(), 0, "Should start with no food");

    // Spawn initial food
    game_state.spawn_initial_food();

    // Verify food was spawned to target amount (should be 10 by default)
    assert_eq!(
        game_state.arena.food.len(),
        10,
        "Should spawn exactly 10 food items initially"
    );

    println!(
        "Test passed: Initial food spawning created {} food items",
        game_state.arena.food.len()
    );

    Ok(())
}

#[test]
fn test_food_never_spawns_in_endzones() -> Result<()> {
    // Create a team match game with endzones
    let game_type = GameType::TeamMatch { per_team: 1 };
    let mut game_state = GameState::new(60, 40, game_type, Some(12345), 0);

    // Add two players to different teams
    game_state.add_player(1, Some("Player1".to_string()))?;
    game_state.add_player(2, Some("Player2".to_string()))?;

    // Assign teams
    game_state.arena.snakes[0].team_id = Some(TeamId(0));
    game_state.arena.snakes[1].team_id = Some(TeamId(1));

    // Start the game
    game_state.status = common::GameStatus::Started { server_id: 1 };

    // Run many ticks to spawn lots of food
    for _ in 0..1000 {
        game_state.tick_forward(false);
    }

    // Check that all food is within main field bounds (x: 10 to 49)
    let (left_bound, right_bound) = game_state
        .arena
        .main_field_bounds()
        .expect("Team game should have main field bounds");

    for food_pos in &game_state.arena.food {
        assert!(
            food_pos.x >= left_bound && food_pos.x <= right_bound,
            "Food at x={} is outside main field bounds [{}, {}]",
            food_pos.x,
            left_bound,
            right_bound
        );
    }

    // Verify we actually spawned some food
    assert!(!game_state.arena.food.is_empty(), "No food was spawned");

    println!(
        "Test passed: {} food items spawned, all within main field",
        game_state.arena.food.len()
    );

    Ok(())
}
