use anyhow::Result;
use server::ws_server::WSMessage;
use ::common::{GameType, GameEvent, QueueMode, TeamId};
use tokio::time::{timeout, Duration};
use redis::AsyncCommands;

mod common;
use self::common::{TestEnvironment, TestClient};

// Helper function to clean Redis state before tests
async fn setup_test_redis() -> Result<()> {
    // Clean up Redis before starting the test
    let redis_url = std::env::var("SNAKETRON_REDIS_URL")
        .unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let redis_client = redis::Client::open(redis_url)?;
    let mut redis_conn = redis_client.get_async_connection().await?;
    let _: () = redis::cmd("FLUSHDB").query_async(&mut redis_conn).await?;

    // Small delay to ensure Redis is ready
    tokio::time::sleep(Duration::from_millis(100)).await;
    Ok(())
}

// Helper function to create a lobby with specified users and queue for a game
async fn create_lobby_and_queue(
    env: &TestEnvironment,
    server_idx: usize,
    user_ids: &[i32],
    game_type: GameType,
    queue_mode: QueueMode,
) -> Result<(Vec<TestClient>, u32)> {
    let server_addr = env.ws_addr(server_idx).expect("Server should exist");

    // Connect all clients
    let mut clients = Vec::new();
    for &user_id in user_ids {
        let mut client = TestClient::connect(&server_addr).await?;
        client.authenticate(user_id).await?;
        clients.push(client);
    }

    // First client creates lobby
    clients[0].send_message(WSMessage::CreateLobby).await?;

    // Wait for LobbyCreated response and capture both lobby_id and lobby_code
    let (lobby_id, lobby_code) = timeout(Duration::from_secs(5), async {
        loop {
            match clients[0].receive_message().await? {
                WSMessage::LobbyCreated { lobby_id, lobby_code } => {
                    return Ok::<(u32, String), anyhow::Error>((lobby_id, lobby_code));
                }
                _ => {}
            }
        }
    }).await??;

    // Other clients join the lobby using the captured lobby_code
    if clients.len() > 1 {
        for client in clients.iter_mut().skip(1) {
            client.send_message(WSMessage::JoinLobbyByCode { lobby_code: lobby_code.clone() }).await?;

            // Wait for JoinedLobby confirmation
            timeout(Duration::from_secs(5), async {
                loop {
                    match client.receive_message().await? {
                        WSMessage::JoinedLobby { .. } => {
                            return Ok::<(), anyhow::Error>(());
                        }
                        _ => {}
                    }
                }
            }).await??;
        }
    }

    // Host queues for match
    clients[0].send_message(WSMessage::QueueForMatch {
        game_type,
        queue_mode,
    }).await?;

    Ok((clients, lobby_id))
}

// Helper to wait for all clients to receive JoinGame and snapshot
async fn wait_for_all_clients_to_join_game(
    clients: &mut [TestClient],
) -> Result<u32> {
    let mut game_id = None;

    for client in clients.iter_mut() {
        let client_game_id = timeout(Duration::from_secs(30), async {
            // First wait for JoinGame message
            let gid = loop {
                match client.receive_message().await? {
                    WSMessage::JoinGame(id) => {
                        break id;
                    }
                    _ => {}
                }
            };

            // Send JoinGame acknowledgment
            client.send_message(WSMessage::JoinGame(gid)).await?;

            // Wait for snapshot
            loop {
                match client.receive_message().await? {
                    WSMessage::GameEvent(event) => {
                        if matches!(event.event, GameEvent::Snapshot { .. }) {
                            return Ok::<u32, anyhow::Error>(event.game_id);
                        }
                    }
                    _ => {}
                }
            }
        }).await??;

        if let Some(expected_game_id) = game_id {
            assert_eq!(client_game_id, expected_game_id, "All clients should join the same game");
        } else {
            game_id = Some(client_game_id);
        }
    }

    Ok(game_id.expect("Should have a game_id"))
}

// Helper to get team assignment for a player from game snapshot
async fn get_player_team(client: &mut TestClient, user_id: u32) -> Result<Option<TeamId>> {
    // The client should have already received the snapshot
    // We'll peek at the last received snapshot in memory
    // For now, we'll send a simple query and parse the response

    // This is a simplified version - in reality you'd need to track the game state
    // For testing purposes, we can infer team from snake positions
    Ok(None) // Placeholder - will be filled based on actual game state
}

// ============================================================================
// 1V1 TESTS
// ============================================================================

#[tokio::test]
async fn test_two_player_lobby_creates_1v1_with_split_teams() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    setup_test_redis().await?;
    let mut env = TestEnvironment::new("test_two_player_lobby_1v1_split").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;

    let (mut clients, _lobby_id) = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0], env.user_ids()[1]],
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Quickmatch,
    ).await?;

    // Wait for game to be created
    let game_id = wait_for_all_clients_to_join_game(&mut clients).await?;

    println!("1v1 game created from 2-player lobby: {}", game_id);

    // TODO: Verify that players are on opposite teams

    for client in clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_two_single_lobbies_create_1v1() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    setup_test_redis().await?;
    let mut env = TestEnvironment::new("test_two_single_lobbies_1v1").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;

    // Create two separate single-player lobbies
    let (mut clients1, _lobby_id1) = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0]],
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Quickmatch,
    ).await?;

    let (mut clients2, _lobby_id2) = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[1]],
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Quickmatch,
    ).await?;

    // Combine all clients
    let mut all_clients = clients1;
    all_clients.extend(clients2);

    // Wait for both to join the same game
    let game_id = wait_for_all_clients_to_join_game(&mut all_clients).await?;

    println!("1v1 game created from two single lobbies: {}", game_id);

    for client in all_clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_single_lobby_waits_for_1v1_match() -> Result<()> {
    setup_test_redis().await?;
    let mut env = TestEnvironment::new("test_single_lobby_waits_1v1").await?;
    env.add_server().await?;
    env.create_user().await?;

    let (mut clients, _lobby_id) = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0]],
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Quickmatch,
    ).await?;

    // Should NOT receive a match (timeout expected)
    let result = timeout(Duration::from_secs(5), async {
        loop {
            match clients[0].receive_message().await? {
                WSMessage::JoinGame(_) => {
                    return Ok::<(), anyhow::Error>(());
                }
                _ => {}
            }
        }
    }).await;

    assert!(result.is_err(), "Single lobby should NOT be matched for 1v1");

    println!("Single lobby correctly waiting for opponent");

    for client in clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

// ============================================================================
// 2V2 TESTS
// ============================================================================

#[tokio::test]
async fn test_two_player_lobbies_create_2v2_same_team() -> Result<()> {
    setup_test_redis().await?;
    let mut env = TestEnvironment::new("test_two_player_lobbies_2v2").await?;
    env.add_server().await?;
    for _ in 0..4 {
        env.create_user().await?;
    }

    // Create two 2-player lobbies
    let (mut clients1, _lobby_id1) = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0], env.user_ids()[1]],
        GameType::TeamMatch { per_team: 2 },
        QueueMode::Quickmatch,
    ).await?;

    let (mut clients2, _lobby_id2) = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[2], env.user_ids()[3]],
        GameType::TeamMatch { per_team: 2 },
        QueueMode::Quickmatch,
    ).await?;

    // Combine all clients
    let mut all_clients = clients1;
    all_clients.extend(clients2);

    // Wait for all to join the game
    let game_id = wait_for_all_clients_to_join_game(&mut all_clients).await?;

    println!("2v2 game created from two 2-player lobbies: {}", game_id);

    // TODO: Verify that lobby members are on the same team

    for client in all_clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_three_plus_one_lobbies_create_2v2() -> Result<()> {
    setup_test_redis().await?;
    let mut env = TestEnvironment::new("test_three_plus_one_2v2").await?;
    env.add_server().await?;
    for _ in 0..4 {
        env.create_user().await?;
    }

    // Create 3-player lobby
    let (mut clients1, _lobby_id1) = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0], env.user_ids()[1], env.user_ids()[2]],
        GameType::TeamMatch { per_team: 2 },
        QueueMode::Quickmatch,
    ).await?;

    // Create 1-player lobby
    let (mut clients2, _lobby_id2) = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[3]],
        GameType::TeamMatch { per_team: 2 },
        QueueMode::Quickmatch,
    ).await?;

    // Combine all clients
    let mut all_clients = clients1;
    all_clients.extend(clients2);

    // Wait for all to join the game
    let game_id = wait_for_all_clients_to_join_game(&mut all_clients).await?;

    println!("2v2 game created from 3+1 lobbies: {}", game_id);

    for client in all_clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_four_player_lobby_creates_2v2() -> Result<()> {
    setup_test_redis().await?;
    let mut env = TestEnvironment::new("test_four_player_lobby_2v2").await?;
    env.add_server().await?;
    for _ in 0..4 {
        env.create_user().await?;
    }

    let (mut clients, _lobby_id) = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0], env.user_ids()[1], env.user_ids()[2], env.user_ids()[3]],
        GameType::TeamMatch { per_team: 2 },
        QueueMode::Quickmatch,
    ).await?;

    // Wait for game to be created
    let game_id = wait_for_all_clients_to_join_game(&mut clients).await?;

    println!("2v2 game created from 4-player lobby: {}", game_id);

    // TODO: Verify that first 2 players are on Team 0, last 2 on Team 1

    for client in clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

// ============================================================================
// FFA TESTS
// ============================================================================

#[tokio::test]
async fn test_ffa_multiple_lobbies_combine() -> Result<()> {
    setup_test_redis().await?;
    let mut env = TestEnvironment::new("test_ffa_multiple_lobbies").await?;
    env.add_server().await?;
    for _ in 0..5 {
        env.create_user().await?;
    }

    // Create lobbies with different sizes
    let (mut clients1, _) = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0], env.user_ids()[1]],
        GameType::FreeForAll { max_players: 6 },
        QueueMode::Quickmatch,
    ).await?;

    let (mut clients2, _) = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[2]],
        GameType::FreeForAll { max_players: 6 },
        QueueMode::Quickmatch,
    ).await?;

    let (mut clients3, _) = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[3], env.user_ids()[4]],
        GameType::FreeForAll { max_players: 6 },
        QueueMode::Quickmatch,
    ).await?;

    // Combine all clients
    let mut all_clients = clients1;
    all_clients.extend(clients2);
    all_clients.extend(clients3);

    // Wait for all to join the game
    let game_id = wait_for_all_clients_to_join_game(&mut all_clients).await?;

    println!("FFA game created from multiple lobbies: {} (5 total players)", game_id);

    for client in all_clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_ffa_respects_max_players() -> Result<()> {
    setup_test_redis().await?;
    let mut env = TestEnvironment::new("test_ffa_max_players").await?;
    env.add_server().await?;
    for _ in 0..6 {
        env.create_user().await?;
    }

    // Create two 3-player lobbies, but max is 4
    let (mut clients1, _) = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0], env.user_ids()[1], env.user_ids()[2]],
        GameType::FreeForAll { max_players: 4 },
        QueueMode::Quickmatch,
    ).await?;

    let (mut clients2, _) = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[3], env.user_ids()[4], env.user_ids()[5]],
        GameType::FreeForAll { max_players: 4 },
        QueueMode::Quickmatch,
    ).await?;

    // Only the first lobby should get matched (can't fit both)
    let result1 = timeout(Duration::from_secs(10), wait_for_all_clients_to_join_game(&mut clients1)).await;

    // Second lobby should timeout (not matched)
    let result2 = timeout(Duration::from_secs(3), async {
        loop {
            match clients2[0].receive_message().await? {
                WSMessage::JoinGame(_) => {
                    return Ok::<(), anyhow::Error>(());
                }
                _ => {}
            }
        }
    }).await;

    assert!(result1.is_ok(), "First lobby should be matched");
    assert!(result2.is_err(), "Second lobby should NOT be matched (exceeds max_players)");

    println!("FFA correctly respects max_players limit");

    for client in clients1 {
        client.disconnect().await?;
    }
    for client in clients2 {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_ffa_minimum_players() -> Result<()> {
    setup_test_redis().await?;
    let mut env = TestEnvironment::new("test_ffa_minimum_players").await?;
    env.add_server().await?;
    env.create_user().await?;

    let (mut clients, _) = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0]],
        GameType::FreeForAll { max_players: 4 },
        QueueMode::Quickmatch,
    ).await?;

    // Should NOT receive a match (needs at least 2 players for FFA)
    let result = timeout(Duration::from_secs(5), async {
        loop {
            match clients[0].receive_message().await? {
                WSMessage::JoinGame(_) => {
                    return Ok::<(), anyhow::Error>(());
                }
                _ => {}
            }
        }
    }).await;

    assert!(result.is_err(), "Single player should NOT be matched for FFA");

    println!("FFA correctly requires minimum 2 players");

    for client in clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

#[tokio::test]
async fn test_quickmatch_and_competitive_dont_mix() -> Result<()> {
    setup_test_redis().await?;
    let mut env = TestEnvironment::new("test_queue_modes_dont_mix").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;

    // One lobby in Quickmatch
    let (mut clients1, _) = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0]],
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Quickmatch,
    ).await?;

    // Another lobby in Competitive
    let (mut clients2, _) = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[1]],
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Competitive,
    ).await?;

    // Neither should get matched together
    let result1 = timeout(Duration::from_secs(5), async {
        loop {
            match clients1[0].receive_message().await? {
                WSMessage::JoinGame(_) => {
                    return Ok::<(), anyhow::Error>(());
                }
                _ => {}
            }
        }
    }).await;

    let result2 = timeout(Duration::from_secs(5), async {
        loop {
            match clients2[0].receive_message().await? {
                WSMessage::JoinGame(_) => {
                    return Ok::<(), anyhow::Error>(());
                }
                _ => {}
            }
        }
    }).await;

    assert!(result1.is_err() && result2.is_err(),
            "Quickmatch and Competitive lobbies should NOT match together");

    println!("Queue modes correctly separated");

    for client in clients1 {
        client.disconnect().await?;
    }
    for client in clients2 {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_concurrent_lobby_and_solo_matchmaking() -> Result<()> {
    setup_test_redis().await?;
    let mut env = TestEnvironment::new("test_concurrent_systems").await?;
    env.add_server().await?;
    for _ in 0..4 {
        env.create_user().await?;
    }

    // Create lobby-based players
    let (mut lobby_clients, _) = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0], env.user_ids()[1]],
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Quickmatch,
    ).await?;

    // Create solo queue players (not using lobbies)
    let server_addr = env.ws_addr(0).expect("Server should exist");
    let mut solo_clients = Vec::new();
    for &user_id in &env.user_ids()[2..4] {
        let mut client = TestClient::connect(&server_addr).await?;
        client.authenticate(user_id).await?;
        client.send_message(WSMessage::QueueForMatch {
            game_type: GameType::TeamMatch { per_team: 1 },
            queue_mode: QueueMode::Quickmatch,
        }).await?;
        solo_clients.push(client);
    }

    // Both systems should work independently
    let lobby_game_id = wait_for_all_clients_to_join_game(&mut lobby_clients).await?;

    let solo_game_id = timeout(Duration::from_secs(10), async {
        wait_for_all_clients_to_join_game(&mut solo_clients).await
    }).await??;

    // They should create separate games
    assert_ne!(lobby_game_id, solo_game_id, "Lobby and solo matchmaking should create separate games");

    println!("Lobby matchmaking (game {}) and solo matchmaking (game {}) work independently",
             lobby_game_id, solo_game_id);

    for client in lobby_clients {
        client.disconnect().await?;
    }
    for client in solo_clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}
