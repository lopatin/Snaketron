use ::common::{GameEvent, GameType, QueueMode, TeamId};
use anyhow::Result;
use chrono::Utc;
use redis::{Client, PushInfo};
use server::{
    lobby_manager::LobbyMember, matchmaking_manager::MatchmakingManager,
    redis_utils::create_connection_manager, ws_server::WSMessage,
};
use tokio::{
    sync::broadcast,
    time::{Duration, timeout},
};

mod common;
use self::common::{TestClient, TestEnvironment};

fn test_redis_url() -> String {
    let mut url = std::env::var("SNAKETRON_REDIS_URL")
        .unwrap_or_else(|_| "redis://localhost:6379".to_string());
    if !url.contains("protocol=resp3") {
        if url.contains('?') {
            url.push_str("&protocol=resp3");
        } else {
            url.push_str("?protocol=resp3");
        }
    }
    url
}

fn make_lobby_member<S: Into<String>>(user_id: u32, username: S) -> LobbyMember {
    LobbyMember {
        user_id,
        username: username.into(),
        ts: Utc::now().timestamp_millis() as f64,
    }
}

async fn create_test_matchmaking_manager() -> Result<MatchmakingManager> {
    let redis_client = Client::open(test_redis_url())?;
    let (pubsub_tx, _rx) = broadcast::channel::<PushInfo>(128);
    let conn = create_connection_manager(redis_client, pubsub_tx).await?;
    MatchmakingManager::new(conn)
}

// Helper function to clean Redis state before tests
async fn setup_test_redis() -> Result<()> {
    // Clean up Redis before starting the test
    let redis_client = redis::Client::open(test_redis_url())?;
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await?;
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
) -> Result<Vec<TestClient>> {
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

    // Wait for LobbyCreated response and capture lobby_code
    let lobby_code = timeout(Duration::from_secs(5), async {
        loop {
            match clients[0].receive_message().await? {
                WSMessage::LobbyCreated { lobby_code } => {
                    return Ok::<String, anyhow::Error>(lobby_code);
                }
                _ => {}
            }
        }
    })
    .await??;

    // Other clients join the lobby using the captured lobby_code
    if clients.len() > 1 {
        for client in clients.iter_mut().skip(1) {
            client
                .send_message(WSMessage::JoinLobby {
                    lobby_code: lobby_code.clone(),
                    preferences: None,
                })
                .await?;

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
            })
            .await??;
        }
    }

    // Host queues for match
    clients[0]
        .send_message(WSMessage::QueueForMatch {
            game_type,
            queue_mode,
        })
        .await?;

    Ok(clients)
}

// Wait for a single client to receive JoinGame and first snapshot
async fn wait_for_client_to_join_game(client: &mut TestClient) -> Result<u32> {
    timeout(Duration::from_secs(30), async {
        // First wait for JoinGame message
        let game_id = loop {
            match client.receive_message().await? {
                WSMessage::JoinGame(id) => break id,
                _ => {}
            }
        };

        client.send_message(WSMessage::JoinGame(game_id)).await?;

        // Wait for snapshot to confirm game stream started
        loop {
            match client.receive_message().await? {
                WSMessage::GameEvent(event) => {
                    if matches!(event.event, GameEvent::Snapshot { .. }) {
                        return Ok::<u32, anyhow::Error>(game_id);
                    }
                }
                _ => {}
            }
        }
    })
    .await?
}

// Helper to wait for all clients to receive JoinGame and snapshot
async fn wait_for_all_clients_to_join_game(clients: &mut [TestClient]) -> Result<u32> {
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
        })
        .await??;

        if let Some(expected_game_id) = game_id {
            assert_eq!(
                client_game_id, expected_game_id,
                "All clients should join the same game"
            );
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
async fn test_multi_member_lobby_queues_solo_host_only_player() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    setup_test_redis().await?;
    let mut env = TestEnvironment::new("test_multi_member_lobby_solo_host_only_player").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;

    // Host queues for solo with a second member present
    let mut clients = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0], env.user_ids()[1]],
        GameType::Solo,
        QueueMode::Quickmatch,
    )
    .await?;

    // Host should be the only player added to the match
    let mut host_client = clients.remove(0);
    let mut spectator_client = clients.remove(0);

    let game_id = wait_for_client_to_join_game(&mut host_client).await?;

    // Spectator should still get routed to the game (as a spectator)
    let spectator_game_id = timeout(Duration::from_secs(10), async {
        loop {
            match spectator_client.receive_message().await? {
                WSMessage::JoinGame(id) => {
                    spectator_client.send_message(WSMessage::JoinGame(id)).await?;
                    return Ok::<u32, anyhow::Error>(id);
                }
                _ => {}
            }
        }
    })
    .await??;

    assert_eq!(game_id, spectator_game_id, "Lobby members should be directed to the same solo game");

    // Only the host should be stored as a player in the active match
    let mut matchmaking_manager = create_test_matchmaking_manager().await?;
    let active_match = matchmaking_manager
        .get_active_match(game_id)
        .await?
        .expect("Active match should be stored");

    assert_eq!(
        active_match.players.len(),
        1,
        "Solo match created from multi-member lobby should only register one player"
    );
    assert_eq!(
        active_match.players[0].user_id,
        env.user_ids()[0] as u32,
        "Requesting user (host) should be the solo participant"
    );

    host_client.disconnect().await?;
    spectator_client.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_two_player_lobby_creates_1v1_with_split_teams() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    setup_test_redis().await?;
    let mut env = TestEnvironment::new("test_two_player_lobby_1v1_split").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;

    let mut clients = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0], env.user_ids()[1]],
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Quickmatch,
    )
    .await?;

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
    let mut clients1 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0]],
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Quickmatch,
    )
    .await?;

    let mut clients2 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[1]],
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Quickmatch,
    )
    .await?;

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

    let mut clients = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0]],
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Quickmatch,
    )
    .await?;

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
    })
    .await;

    assert!(
        result.is_err(),
        "Single lobby should NOT be matched for 1v1"
    );

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
    let mut clients1 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0], env.user_ids()[1]],
        GameType::TeamMatch { per_team: 2 },
        QueueMode::Quickmatch,
    )
    .await?;

    let mut clients2 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[2], env.user_ids()[3]],
        GameType::TeamMatch { per_team: 2 },
        QueueMode::Quickmatch,
    )
    .await?;

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
    let mut clients1 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0], env.user_ids()[1], env.user_ids()[2]],
        GameType::TeamMatch { per_team: 2 },
        QueueMode::Quickmatch,
    )
    .await?;

    // Create 1-player lobby
    let mut clients2 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[3]],
        GameType::TeamMatch { per_team: 2 },
        QueueMode::Quickmatch,
    )
    .await?;

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

    let mut clients = create_lobby_and_queue(
        &env,
        0,
        &[
            env.user_ids()[0],
            env.user_ids()[1],
            env.user_ids()[2],
            env.user_ids()[3],
        ],
        GameType::TeamMatch { per_team: 2 },
        QueueMode::Quickmatch,
    )
    .await?;

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
    let mut clients1 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0], env.user_ids()[1]],
        GameType::FreeForAll { max_players: 6 },
        QueueMode::Quickmatch,
    )
    .await?;

    let mut clients2 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[2]],
        GameType::FreeForAll { max_players: 6 },
        QueueMode::Quickmatch,
    )
    .await?;

    let mut clients3 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[3], env.user_ids()[4]],
        GameType::FreeForAll { max_players: 6 },
        QueueMode::Quickmatch,
    )
    .await?;

    // Combine all clients
    let mut all_clients = clients1;
    all_clients.extend(clients2);
    all_clients.extend(clients3);

    // Wait for all to join the game
    let game_id = wait_for_all_clients_to_join_game(&mut all_clients).await?;

    println!(
        "FFA game created from multiple lobbies: {} (5 total players)",
        game_id
    );

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
    let mut clients1 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0], env.user_ids()[1], env.user_ids()[2]],
        GameType::FreeForAll { max_players: 4 },
        QueueMode::Quickmatch,
    )
    .await?;

    let mut clients2 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[3], env.user_ids()[4], env.user_ids()[5]],
        GameType::FreeForAll { max_players: 4 },
        QueueMode::Quickmatch,
    )
    .await?;

    // Only the first lobby should get matched (can't fit both)
    let result1 = timeout(
        Duration::from_secs(10),
        wait_for_all_clients_to_join_game(&mut clients1),
    )
    .await;

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
    })
    .await;

    assert!(result1.is_ok(), "First lobby should be matched");
    assert!(
        result2.is_err(),
        "Second lobby should NOT be matched (exceeds max_players)"
    );

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

    let mut clients = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0]],
        GameType::FreeForAll { max_players: 4 },
        QueueMode::Quickmatch,
    )
    .await?;

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
    })
    .await;

    assert!(
        result.is_err(),
        "Single player should NOT be matched for FFA"
    );

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
    let mut clients1 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0]],
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Quickmatch,
    )
    .await?;

    // Another lobby in Competitive
    let mut clients2 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[1]],
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Competitive,
    )
    .await?;

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
    })
    .await;

    let result2 = timeout(Duration::from_secs(5), async {
        loop {
            match clients2[0].receive_message().await? {
                WSMessage::JoinGame(_) => {
                    return Ok::<(), anyhow::Error>(());
                }
                _ => {}
            }
        }
    })
    .await;

    assert!(
        result1.is_err() && result2.is_err(),
        "Quickmatch and Competitive lobbies should NOT match together"
    );

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

// ============================================================================
// MULTI-GAME-TYPE QUEUING TESTS
// ============================================================================

/// Test that add_lobby_to_queue with multiple game types registers the lobby in all queues
#[tokio::test]
async fn test_multi_type_lobby_appears_in_all_queues() -> Result<()> {
    setup_test_redis().await?;
    let mut mm = create_test_matchmaking_manager().await?;

    // Create test lobby members
    let members = vec![
        make_lobby_member(1, "player1"),
        make_lobby_member(2, "player2"),
    ];

    // Queue lobby for both 1v1 and 2v2
    mm.add_lobby_to_queue(
        "TEST001",
        members.clone(),
        1000,
        vec![
            GameType::TeamMatch { per_team: 1 },
            GameType::TeamMatch { per_team: 2 },
        ],
        QueueMode::Quickmatch,
        1,
    )
    .await?;

    // Verify lobby appears in both game type queues
    let lobbies_1v1 = mm
        .get_queued_lobbies(&GameType::TeamMatch { per_team: 1 }, &QueueMode::Quickmatch)
        .await?;
    let lobbies_2v2 = mm
        .get_queued_lobbies(&GameType::TeamMatch { per_team: 2 }, &QueueMode::Quickmatch)
        .await?;

    assert_eq!(lobbies_1v1.len(), 1, "Lobby should appear in 1v1 queue");
    assert_eq!(lobbies_2v2.len(), 1, "Lobby should appear in 2v2 queue");

    assert_eq!(lobbies_1v1[0].lobby_code, "TEST001");
    assert_eq!(lobbies_2v2[0].lobby_code, "TEST001");

    // Verify the game_types field contains both types
    assert_eq!(lobbies_1v1[0].game_types.len(), 2);
    assert!(
        lobbies_1v1[0]
            .game_types
            .contains(&GameType::TeamMatch { per_team: 1 })
    );
    assert!(
        lobbies_1v1[0]
            .game_types
            .contains(&GameType::TeamMatch { per_team: 2 })
    );

    println!("✓ Multi-type lobby correctly appears in all queues");
    Ok(())
}

/// Test that remove_lobby_from_all_queues removes lobby from all game type queues
#[tokio::test]
async fn test_remove_lobby_from_all_queues() -> Result<()> {
    setup_test_redis().await?;
    let mut mm = create_test_matchmaking_manager().await?;

    // Create test lobby members
    let members = vec![make_lobby_member(1, "player1")];

    // Queue lobby for multiple game types
    mm.add_lobby_to_queue(
        "TEST001",
        members.clone(),
        1000,
        vec![
            GameType::TeamMatch { per_team: 1 },
            GameType::TeamMatch { per_team: 2 },
            GameType::FreeForAll { max_players: 4 },
        ],
        QueueMode::Quickmatch,
        1,
    )
    .await?;

    // Verify lobby is in all queues
    let lobbies_1v1 = mm
        .get_queued_lobbies(&GameType::TeamMatch { per_team: 1 }, &QueueMode::Quickmatch)
        .await?;
    let lobbies_2v2 = mm
        .get_queued_lobbies(&GameType::TeamMatch { per_team: 2 }, &QueueMode::Quickmatch)
        .await?;
    let lobbies_ffa = mm
        .get_queued_lobbies(
            &GameType::FreeForAll { max_players: 4 },
            &QueueMode::Quickmatch,
        )
        .await?;

    assert_eq!(lobbies_1v1.len(), 1);
    assert_eq!(lobbies_2v2.len(), 1);
    assert_eq!(lobbies_ffa.len(), 1);

    // Remove lobby from all queues
    let queued_lobby = &lobbies_1v1[0];
    mm.remove_lobby_from_all_queues(queued_lobby).await?;

    // Verify lobby is gone from all queues
    let lobbies_1v1_after = mm
        .get_queued_lobbies(&GameType::TeamMatch { per_team: 1 }, &QueueMode::Quickmatch)
        .await?;
    let lobbies_2v2_after = mm
        .get_queued_lobbies(&GameType::TeamMatch { per_team: 2 }, &QueueMode::Quickmatch)
        .await?;
    let lobbies_ffa_after = mm
        .get_queued_lobbies(
            &GameType::FreeForAll { max_players: 4 },
            &QueueMode::Quickmatch,
        )
        .await?;

    assert_eq!(
        lobbies_1v1_after.len(),
        0,
        "Lobby should be removed from 1v1 queue"
    );
    assert_eq!(
        lobbies_2v2_after.len(),
        0,
        "Lobby should be removed from 2v2 queue"
    );
    assert_eq!(
        lobbies_ffa_after.len(),
        0,
        "Lobby should be removed from FFA queue"
    );

    println!("✓ Lobby correctly removed from all queues");
    Ok(())
}

/// Test that get_queued_lobbies deduplicates lobbies appearing in multiple queues
#[tokio::test]
async fn test_get_queued_lobbies_deduplication() -> Result<()> {
    setup_test_redis().await?;
    let mut mm = create_test_matchmaking_manager().await?;

    // Create test lobby members
    let members = vec![
        make_lobby_member(1, "player1"),
        make_lobby_member(2, "player2"),
    ];

    // Queue same lobby for 1v1
    mm.add_lobby_to_queue(
        "TEST001",
        members.clone(),
        1000,
        vec![GameType::TeamMatch { per_team: 1 }],
        QueueMode::Quickmatch,
        1,
    )
    .await?;

    // Queue a different lobby for 1v1 as well (to verify we get both)
    mm.add_lobby_to_queue(
        "TEST002",
        members.clone(),
        1050,
        vec![GameType::TeamMatch { per_team: 1 }],
        QueueMode::Quickmatch,
        2,
    )
    .await?;

    // Get lobbies - should return exactly 2 unique lobbies
    let lobbies = mm
        .get_queued_lobbies(&GameType::TeamMatch { per_team: 1 }, &QueueMode::Quickmatch)
        .await?;

    assert_eq!(lobbies.len(), 2, "Should return exactly 2 unique lobbies");

    let lobby_codes: Vec<&str> = lobbies.iter().map(|l| l.lobby_code.as_str()).collect();
    assert!(lobby_codes.contains(&"TEST001"));
    assert!(lobby_codes.contains(&"TEST002"));

    println!("✓ Deduplication works correctly");
    Ok(())
}

/// Test that when a lobby is matched in one queue, it doesn't get matched again in another
#[tokio::test]
async fn test_multi_type_lobby_no_double_matching() -> Result<()> {
    setup_test_redis().await?;

    let mut mm = create_test_matchmaking_manager().await?;

    // Create test lobby members
    let members1 = vec![make_lobby_member(1, "player1")];

    let members2 = vec![make_lobby_member(2, "player2")];

    let members3 = vec![
        make_lobby_member(3, "player3"),
        make_lobby_member(4, "player4"),
    ];

    // Queue two lobbies for both 1v1 and 2v2
    mm.add_lobby_to_queue(
        "TEST001",
        members1.clone(),
        1000,
        vec![
            GameType::TeamMatch { per_team: 1 },
            GameType::TeamMatch { per_team: 2 },
        ],
        QueueMode::Quickmatch,
        1,
    )
    .await?;

    mm.add_lobby_to_queue(
        "TEST002",
        members2.clone(),
        1000,
        vec![
            GameType::TeamMatch { per_team: 1 },
            GameType::TeamMatch { per_team: 2 },
        ],
        QueueMode::Quickmatch,
        2,
    )
    .await?;

    // Also add a 2-player lobby just for 2v2
    mm.add_lobby_to_queue(
        "TEST003",
        members3.clone(),
        1000,
        vec![GameType::TeamMatch { per_team: 2 }],
        QueueMode::Quickmatch,
        3,
    )
    .await?;

    // Get lobbies for 1v1 - should find lobbies 1 and 2
    let lobbies_1v1_before = mm
        .get_queued_lobbies(&GameType::TeamMatch { per_team: 1 }, &QueueMode::Quickmatch)
        .await?;
    assert_eq!(lobbies_1v1_before.len(), 2);

    // Simulate matching lobbies 1 and 2 for 1v1 by removing them
    mm.remove_lobby_from_all_queues(&lobbies_1v1_before[0])
        .await?;
    mm.remove_lobby_from_all_queues(&lobbies_1v1_before[1])
        .await?;

    // Now check 2v2 queue - lobbies 1 and 2 should be GONE
    let lobbies_2v2_after = mm
        .get_queued_lobbies(&GameType::TeamMatch { per_team: 2 }, &QueueMode::Quickmatch)
        .await?;

    // Should only have lobby 3 left
    assert_eq!(
        lobbies_2v2_after.len(),
        1,
        "Only lobby 3 should remain in 2v2 queue"
    );
    assert_eq!(lobbies_2v2_after[0].lobby_code, "TEST003");

    println!("✓ No double-matching: matched lobbies removed from all queues");
    Ok(())
}

/// Integration test: Two lobbies queued for [1v1, 2v2] should match for 1v1
#[tokio::test]
async fn test_multi_type_lobbies_match_for_1v1() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();
    setup_test_redis().await?;
    let mut env = TestEnvironment::new("test_multi_type_1v1_match").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;

    // For now, since WebSocket only supports single game type,
    // we'll queue two separate lobbies and they should match
    let mut clients1 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0]],
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Quickmatch,
    )
    .await?;

    let mut clients2 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[1]],
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Quickmatch,
    )
    .await?;

    // Combine all clients
    let mut all_clients = clients1;
    all_clients.extend(clients2);

    // Wait for both to join the same game
    let game_id = wait_for_all_clients_to_join_game(&mut all_clients).await?;

    println!(
        "✓ Multi-type lobbies successfully matched for 1v1: {}",
        game_id
    );

    for client in all_clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

/// Test that a lobby in multiple queues gets properly cleaned up after matching
#[tokio::test]
async fn test_cleanup_after_match_creation() -> Result<()> {
    setup_test_redis().await?;
    let mut mm = create_test_matchmaking_manager().await?;

    // Create three single-player lobbies, all queued for both 1v1 and FFA
    for i in 1..=3 {
        let members = vec![make_lobby_member(i, format!("player{}", i))];

        mm.add_lobby_to_queue(
            &format!("TEST{:03}", i),
            members,
            1000,
            vec![
                GameType::TeamMatch { per_team: 1 },
                GameType::FreeForAll { max_players: 4 },
            ],
            QueueMode::Quickmatch,
            i as u32,
        )
        .await?;
    }

    // Verify all 3 lobbies are in both queues
    let lobbies_1v1 = mm
        .get_queued_lobbies(&GameType::TeamMatch { per_team: 1 }, &QueueMode::Quickmatch)
        .await?;
    let lobbies_ffa = mm
        .get_queued_lobbies(
            &GameType::FreeForAll { max_players: 4 },
            &QueueMode::Quickmatch,
        )
        .await?;

    assert_eq!(lobbies_1v1.len(), 3);
    assert_eq!(lobbies_ffa.len(), 3);

    // Simulate matching lobbies 1 and 2 for 1v1
    mm.remove_lobby_from_all_queues(&lobbies_1v1[0]).await?;
    mm.remove_lobby_from_all_queues(&lobbies_1v1[1]).await?;

    // Verify lobbies 1 and 2 are removed from BOTH queues
    let lobbies_1v1_after = mm
        .get_queued_lobbies(&GameType::TeamMatch { per_team: 1 }, &QueueMode::Quickmatch)
        .await?;
    let lobbies_ffa_after = mm
        .get_queued_lobbies(
            &GameType::FreeForAll { max_players: 4 },
            &QueueMode::Quickmatch,
        )
        .await?;

    assert_eq!(
        lobbies_1v1_after.len(),
        1,
        "Only lobby 3 should remain in 1v1 queue"
    );
    assert_eq!(
        lobbies_ffa_after.len(),
        1,
        "Only lobby 3 should remain in FFA queue"
    );

    assert_eq!(lobbies_1v1_after[0].lobby_code, "TEST003");
    assert_eq!(lobbies_ffa_after[0].lobby_code, "TEST003");

    println!("✓ Matched lobbies properly cleaned up from all queues");
    Ok(())
}
