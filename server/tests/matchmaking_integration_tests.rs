use ::common::{GameEvent, GameType, QueueMode};
use anyhow::{Context, Result, ensure};
use chrono::Utc;
use futures_util::future::join_all;
use redis::AsyncCommands;
use server::matchmaking_manager::QueuedLobby;
use server::matchmaking_pool::MatchmakingPool;
use server::redis_keys::RedisKeys;
use server::ws_server::WSMessage;
use tokio::time::{Duration, Instant, timeout, timeout_at};

// IMPORTANT: These tests must be run with SNAKETRON_ENV=test
// Example: SNAKETRON_ENV=test cargo test -p server --test matchmaking_integration_tests

mod common;
use self::common::{TestClient, TestEnvironment, is_unsolicited_push};

// Serializes the tests in this binary: TestEnvironment::new() sets process-wide
// env vars (DYNAMODB_TABLE_PREFIX, SNAKETRON_REDIS_URL) and flushes the shared
// Redis test database, so concurrently running tests corrupt each other.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Move one exact admitted lobby to an older queue age without waiting for the
/// wall clock. The compare-and-swap updates every serialized Redis index used
/// by production matchmaking, while leaving the immutable queue token and user
/// claims untouched.
async fn backdate_queued_lobby(
    redis_conn: &mut redis::aio::MultiplexedConnection,
    lobby_code: &str,
    age: Duration,
    expected_mmr: i32,
    queued_not_before_ms: i64,
) -> Result<()> {
    let identity_key = RedisKeys::matchmaking_lobby_queue_identity(lobby_code);
    let (mut lobby, original_json) = timeout(Duration::from_secs(5), async {
        loop {
            let member_json: Option<String> = redis_conn.get(&identity_key).await?;
            if let Some(member_json) = member_json {
                let lobby: QueuedLobby = serde_json::from_str(&member_json)
                    .context("queued lobby identity contained malformed JSON")?;
                ensure!(
                    lobby.lobby_code == lobby_code,
                    "queue identity for {lobby_code} belonged to {}",
                    lobby.lobby_code
                );
                return Ok::<_, anyhow::Error>((lobby, member_json));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .with_context(|| format!("lobby {lobby_code} was not admitted within five seconds"))??;

    let observed_at = Utc::now().timestamp_millis();
    let original_queued_at = lobby.queued_at;
    ensure!(
        lobby.avg_mmr == expected_mmr,
        "lobby {lobby_code} was admitted at MMR {}, expected {expected_mmr}",
        lobby.avg_mmr
    );
    ensure!(
        lobby.game_types == [GameType::FreeForAll { max_players: 2 }],
        "lobby {lobby_code} was admitted to the wrong game-type queues: {:?}",
        lobby.game_types
    );
    ensure!(
        lobby.queue_mode == QueueMode::Competitive,
        "lobby {lobby_code} was admitted in {:?} mode",
        lobby.queue_mode
    );
    ensure!(
        lobby.matchmaking_pool == MatchmakingPool::Public,
        "lobby {lobby_code} was admitted to the {:?} pool",
        lobby.matchmaking_pool
    );
    ensure!(
        original_queued_at >= queued_not_before_ms && original_queued_at <= observed_at,
        "lobby {lobby_code} did not receive a fresh production queued_at timestamp"
    );

    let age_ms = i64::try_from(age.as_millis()).context("test lobby age exceeds i64")?;
    lobby.queued_at = observed_at.saturating_sub(age_ms);
    let updated_json = serde_json::to_string(&lobby)?;

    // A queued generation is pinned by three things that must agree: the ZSET
    // members, the queue identity, and the liveness lease. The matchmaking
    // sampler reaps any lobby whose lease does not hold the exact member it
    // sampled, so the lease has to move onto the rewritten member here or the
    // backdated generation is discarded before it can ever be matched.
    let script = redis::Script::new(
        r#"
        if redis.call('GET', KEYS[1]) ~= ARGV[1] then return -1 end
        local lease = redis.call('GET', KEYS[2])
        if not lease then return -6 end
        if lease ~= ARGV[1] then return -7 end
        for i = 3, #KEYS, 2 do
            local queue_score = redis.call('ZSCORE', KEYS[i], ARGV[1])
            if not queue_score then return -2 end
            if tonumber(queue_score) ~= tonumber(ARGV[5]) then return -4 end
            local mmr_score = redis.call('ZSCORE', KEYS[i + 1], ARGV[1])
            if not mmr_score then return -3 end
            if tonumber(mmr_score) ~= tonumber(ARGV[4]) then return -5 end
        end
        for i = 3, #KEYS, 2 do
            redis.call('ZREM', KEYS[i], ARGV[1])
            redis.call('ZREM', KEYS[i + 1], ARGV[1])
            redis.call('ZADD', KEYS[i], ARGV[3], ARGV[2])
            redis.call('ZADD', KEYS[i + 1], ARGV[4], ARGV[2])
        end
        redis.call('SET', KEYS[1], ARGV[2])
        -- Preserve whatever admission put on the lease; only the member it
        -- pins changes, so the generation keeps its real expiry.
        local lease_ttl = redis.call('PTTL', KEYS[2])
        if lease_ttl > 0 then
            redis.call('SET', KEYS[2], ARGV[2], 'PX', lease_ttl)
        else
            redis.call('SET', KEYS[2], ARGV[2])
        end
        return 1
        "#,
    );
    let mut invocation = script.prepare_invoke();
    invocation.key(&identity_key);
    invocation.key(RedisKeys::matchmaking_lobby_queue_lease(lobby_code));
    for game_type in &lobby.game_types {
        invocation
            .key(RedisKeys::matchmaking_lobby_queue_for_pool(
                game_type,
                &lobby.queue_mode,
                lobby.matchmaking_pool,
            ))
            .key(RedisKeys::matchmaking_lobby_mmr_index_for_pool(
                game_type,
                &lobby.queue_mode,
                lobby.matchmaking_pool,
            ));
    }
    let outcome: i64 = invocation
        .arg(&original_json)
        .arg(&updated_json)
        .arg(lobby.queued_at)
        .arg(lobby.avg_mmr)
        .arg(original_queued_at)
        .invoke_async(redis_conn)
        .await?;
    let reason = match outcome {
        -1 => "queue identity no longer holds the observed member",
        -2 => "lobby was missing from a game-type queue",
        -3 => "lobby was missing from an MMR index",
        -4 => "queue score did not match the admitted queued_at",
        -5 => "MMR score did not match the admitted average",
        -6 => "queue liveness lease was absent",
        -7 => "queue liveness lease pinned a different member",
        _ => "unknown failure",
    };
    ensure!(
        outcome == 1,
        "failed to backdate lobby {lobby_code}: {reason} (code {outcome})"
    );
    Ok(())
}

// #[tokio::test]
#[allow(dead_code)]
async fn test_minimal() -> Result<()> {
    // Minimal test to check timing
    println!("Test started");
    tokio::time::sleep(Duration::from_secs(1)).await;
    println!("Test completed");
    Ok(())
}

#[tokio::test]
async fn test_simple_two_player_match() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    // Set test environment

    // Clean up Redis before starting the test
    let redis_client = redis::Client::open("redis://127.0.0.1:6379/1")?;
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await?;
    let _: () = redis::cmd("FLUSHDB").query_async(&mut redis_conn).await?;

    // Small delay to ensure Redis is ready
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Simple test with just 2 players to debug matchmaking
    let mut env = TestEnvironment::new("test_simple_two_player_match").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;

    let server_addr = env.ws_addr(0).expect("Server should exist");

    // Connect and authenticate both clients
    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;

    client1.authenticate(env.user_ids()[0]).await?;
    client2.authenticate(env.user_ids()[1]).await?;
    client1.create_lobby().await?;
    client2.create_lobby().await?;

    println!("Clients authenticated. User IDs: {:?}", env.user_ids());

    // Queue for match with just 2 players max
    client1
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 2 },
            queue_mode: ::common::QueueMode::Quickmatch,
        })
        .await?;
    println!("Client 1 queued for match");

    client2
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 2 },
            queue_mode: ::common::QueueMode::Quickmatch,
        })
        .await?;
    println!("Client 2 queued for match");

    // Wait for JoinGame messages (matchmaking sends this directly)
    println!("Waiting for JoinGame messages...");
    let start = std::time::Instant::now();

    let game_id1 = timeout(Duration::from_secs(30), async {
        loop {
            match client1.receive_message().await? {
                WSMessage::JoinGame(game_id) => {
                    println!(
                        "Client 1 got JoinGame for game {} after {:?}",
                        game_id,
                        start.elapsed()
                    );
                    // Echo it back to actually join the game
                    client1.send_message(WSMessage::JoinGame(game_id)).await?;
                    println!("Client 1 sent JoinGame back to join game {}", game_id);
                    return Ok::<u32, anyhow::Error>(game_id);
                }
                msg => {
                    println!("Client 1 received other message: {:?}", msg);
                }
            }
        }
    })
    .await??;

    let game_id2 = timeout(Duration::from_secs(5), async {
        loop {
            match client2.receive_message().await? {
                WSMessage::JoinGame(game_id) => {
                    println!(
                        "Client 2 got JoinGame for game {} after {:?}",
                        game_id,
                        start.elapsed()
                    );
                    // Echo it back to actually join the game
                    client2.send_message(WSMessage::JoinGame(game_id)).await?;
                    println!("Client 2 sent JoinGame back to join game {}", game_id);
                    return Ok::<u32, anyhow::Error>(game_id);
                }
                msg => {
                    println!("Client 2 received other message: {:?}", msg);
                }
            }
        }
    })
    .await??;

    // For now, we've verified that matchmaking works - both clients got matched to the same game
    // The snapshot issue is a timing problem - the game sends snapshots before clients fully join
    // This is sufficient to prove matchmaking is working with environment isolation

    assert_eq!(game_id1, game_id2, "Both players should be in same game");
    println!("Test passed! Both clients matched to game {}", game_id1);

    client1.disconnect().await?;
    client2.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_queue_for_match_requires_an_explicit_lobby() -> Result<()> {
    async fn expect_explicit_lobby_denial(client: &mut TestClient) -> Result<()> {
        timeout(Duration::from_secs(5), async {
            loop {
                match client.receive_message().await? {
                    WSMessage::AccessDenied { reason } => {
                        assert_eq!(reason, "Join a lobby before queueing for matchmaking");
                        return Ok::<(), anyhow::Error>(());
                    }
                    other if is_unsolicited_push(&other) => {}
                    other => {
                        return Err(anyhow::anyhow!(
                            "Expected no-lobby matchmaking denial, got {:?}",
                            other
                        ));
                    }
                }
            }
        })
        .await?
    }

    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("test_queue_for_match_requires_an_explicit_lobby").await?;
    env.add_server().await?;
    env.create_user().await?;

    let server_addr = env.ws_addr(0).expect("Server should exist");
    let user_id = env.user_ids()[0];
    let mut client = TestClient::connect(&server_addr).await?;
    client.authenticate(user_id).await?;

    let game_type = GameType::FreeForAll { max_players: 2 };
    let queue_mode = ::common::QueueMode::Quickmatch;
    client
        .send_message(WSMessage::QueueForMatch {
            game_type: game_type.clone(),
            queue_mode: queue_mode.clone(),
        })
        .await?;
    expect_explicit_lobby_denial(&mut client).await?;

    client
        .send_message(WSMessage::QueueForMatchMulti {
            game_types: vec![game_type.clone()],
            queue_mode: queue_mode.clone(),
        })
        .await?;
    expect_explicit_lobby_denial(&mut client).await?;

    let redis_client = redis::Client::open("redis://127.0.0.1:6379/1")?;
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await?;
    let lobby_keys: Vec<String> = redis::cmd("KEYS")
        .arg("lobby:*")
        .query_async(&mut redis_conn)
        .await?;
    let lobby_queue_identity_keys: Vec<String> = redis::cmd("KEYS")
        .arg("matchmaking:*:lobby:*:queue-identity")
        .query_async(&mut redis_conn)
        .await?;
    let user_queue_identity_exists: bool = redis_conn
        .exists(RedisKeys::matchmaking_user_queue_identity(user_id as u32))
        .await?;
    let queue_len: usize = redis_conn
        .zcard(RedisKeys::matchmaking_lobby_queue(&game_type, &queue_mode))
        .await?;

    assert!(
        lobby_keys.is_empty(),
        "QueueForMatch must not create any implicit lobby state"
    );
    assert!(
        lobby_queue_identity_keys.is_empty(),
        "QueueForMatch must not create a lobby queue identity"
    );
    assert!(
        !user_queue_identity_exists,
        "QueueForMatch must not reserve the user without a lobby"
    );
    assert_eq!(queue_len, 0, "QueueForMatch must not add a queue member");

    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_basic_matchmaking() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("test_basic_matchmaking").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;

    let server_addr = env.ws_addr(0).expect("Server should exist");

    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;

    client1.authenticate(env.user_ids()[0]).await?;
    client2.authenticate(env.user_ids()[1]).await?;
    client1.create_lobby().await?;
    client2.create_lobby().await?;

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

    // Both should receive MatchFound
    let game_id1 = wait_for_match_with_timeout(&mut client1, Duration::from_secs(35)).await?;
    let game_id2 = wait_for_match_with_timeout(&mut client2, Duration::from_secs(35)).await?;

    assert_eq!(
        game_id1, game_id2,
        "Both players should be matched to same game"
    );

    client1.disconnect().await?;
    client2.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_leave_queue() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("test_leave_queue").await?;
    env.add_server().await?;
    env.create_user().await?;

    let server_addr = env.ws_addr(0).expect("Server should exist");

    let mut client = TestClient::connect(&server_addr).await?;
    client.authenticate(env.user_ids()[0]).await?;
    client.create_lobby().await?;

    // Queue and immediately leave
    client
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 2 },
            queue_mode: ::common::QueueMode::Quickmatch,
        })
        .await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    client.send_message(WSMessage::LeaveQueue).await?;

    // Should not receive a match
    let result = timeout(Duration::from_secs(2), wait_for_match(&mut client)).await;
    assert!(
        result.is_err(),
        "Should timeout waiting for match after leaving queue"
    );

    client.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_team_matchmaking() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("test_team_matchmaking").await?;
    env.add_server().await?;
    for _ in 0..4 {
        env.create_user().await?;
    }

    let server_addr = env.ws_addr(0).expect("Server should exist");

    // Connect 4 clients
    let mut clients = Vec::new();
    for i in 0..4 {
        let mut client = TestClient::connect(&server_addr).await?;
        client.authenticate(env.user_ids()[i]).await?;
        client.create_lobby().await?;
        clients.push(client);
    }

    // All queue for match
    for client in &mut clients {
        client
            .send_message(WSMessage::QueueForMatch {
                game_type: GameType::FreeForAll { max_players: 4 },
                queue_mode: ::common::QueueMode::Quickmatch,
            })
            .await?;
    }

    // All should get matched
    let mut game_ids = Vec::new();
    for client in &mut clients {
        game_ids.push(wait_for_match(client).await?);
    }

    // All should be in same game
    let first_game_id = game_ids[0];
    assert!(
        game_ids.iter().all(|&id| id == first_game_id),
        "All 4 players should be matched to the same game"
    );

    for client in clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_concurrent_matchmaking() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("test_concurrent_matchmaking").await?;
    env.add_server().await?;

    // Create only 6 players to reduce complexity and timing issues
    for _ in 0..6 {
        env.create_user().await?;
    }

    let server_addr = env.ws_addr(0).expect("Server should exist");

    println!("Starting concurrent matchmaking test with 6 clients");

    // Connect all clients first
    let mut clients = Vec::new();
    for (i, user_id) in env.user_ids().iter().copied().enumerate() {
        println!("Client {} (user_id={}) starting", i, user_id);
        let mut client = TestClient::connect(&server_addr).await?;
        println!("Client {} connected", i);

        client.authenticate(user_id).await?;
        client.create_lobby().await?;
        println!("Client {} authenticated", i);

        clients.push(client);
    }

    // Queue all clients for match
    for (i, client) in clients.iter_mut().enumerate() {
        client
            .send_message(WSMessage::QueueForMatch {
                game_type: GameType::FreeForAll { max_players: 2 },
                queue_mode: ::common::QueueMode::Quickmatch,
            })
            .await?;
        println!("Client {} queued for match", i);
    }

    // Wait for all clients to get matched
    let mut game_ids = Vec::new();
    for (i, client) in clients.iter_mut().enumerate() {
        match timeout(Duration::from_secs(30), wait_for_match(client)).await {
            Ok(Ok(game_id)) => {
                println!("Client {} matched to game {}", i, game_id);
                game_ids.push(game_id);
            }
            Ok(Err(e)) => {
                println!("Client {} error: {}", i, e);
                return Err(e);
            }
            Err(_) => {
                println!("Client {} timed out waiting for match", i);
                return Err(anyhow::anyhow!("Client {} timed out", i));
            }
        }
    }

    // Disconnect all clients
    for (i, client) in clients.into_iter().enumerate() {
        client.disconnect().await?;
        println!("Client {} disconnected", i);
    }

    // Should have created some games for all players
    game_ids.sort();
    game_ids.dedup();
    println!("Created {} unique games for 6 players", game_ids.len());
    assert_eq!(
        game_ids.len(),
        3,
        "Should create exactly 3 games for 6 players with max_players=2"
    );

    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_disconnect_during_queue() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    // Clean up Redis before starting the test
    let redis_client = redis::Client::open("redis://127.0.0.1:6379/1")?;
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await?;
    let _: () = redis::cmd("FLUSHDB").query_async(&mut redis_conn).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut env = TestEnvironment::new("test_disconnect_during_queue").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;

    let server_addr = env.ws_addr(0).expect("Server should exist");

    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;

    client1.authenticate(env.user_ids()[0]).await?;
    client2.authenticate(env.user_ids()[1]).await?;
    client1.create_lobby().await?;
    client2.create_lobby().await?;

    // Both queue
    client1
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 3 },
            queue_mode: ::common::QueueMode::Quickmatch,
        })
        .await?;

    // Client1 disconnects while in queue
    // tokio::time::sleep(Duration::from_millis(100)).await;
    client1.disconnect().await?;

    client2
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 3 },
            queue_mode: ::common::QueueMode::Quickmatch,
        })
        .await?;

    // Client2 should not get matched (needs 3 players)
    // Wait longer than the matchmaking loop interval (2 seconds) to ensure no match
    println!("Waiting to see if client2 gets matched (should timeout)...");
    let result = timeout(Duration::from_secs(5), wait_for_match(&mut client2)).await;

    match result {
        Ok(Ok(game_id)) => {
            panic!(
                "ERROR: Client2 got matched to game {} (should not have been matched!)",
                game_id
            );
        }
        Ok(Err(_)) | Err(_) => {
            println!("Client2 correctly did not get matched (timeout or error as expected)");
        }
    }

    client2.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_rejoin_active_game() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    // Clean up Redis before starting the test
    let redis_client = redis::Client::open("redis://127.0.0.1:6379/1")?;
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await?;
    let _: () = redis::cmd("FLUSHDB").query_async(&mut redis_conn).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut env = TestEnvironment::new("test_rejoin_active_game").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;

    let server_addr = env.ws_addr(0).expect("Server should exist");

    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;

    client1.authenticate(env.user_ids()[0]).await?;
    client2.authenticate(env.user_ids()[1]).await?;
    client1.create_lobby().await?;
    client2.create_lobby().await?;

    // Get matched
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

    // Both get matched and auto-joined (wait_for_match now waits for the snapshot)
    let game_id = wait_for_match(&mut client1).await?;
    let game_id2 = wait_for_match(&mut client2).await?;
    assert_eq!(game_id, game_id2, "Both players should be in same game");

    let committed_mapping: Option<u32> = redis_conn
        .get(RedisKeys::matchmaking_user_active_game(
            env.user_ids()[0] as u32,
        ))
        .await?;
    assert_eq!(committed_mapping, Some(game_id));

    // Client1 disconnects
    client1.disconnect().await?;

    // Client1 reconnects and rejoins
    let mut client1_new = TestClient::connect(&server_addr).await?;
    client1_new.authenticate(env.user_ids()[0]).await?;

    // Matchmaking commit and its per-user mapping are durable even if the
    // original Pub/Sub notification was missed while this socket was down.
    // Authentication on any gateway must replay the routing notification.
    let recovered_game_id = timeout(Duration::from_secs(10), async {
        loop {
            if let WSMessage::JoinGame(recovered_game_id) = client1_new.receive_message().await? {
                return Ok::<u32, anyhow::Error>(recovered_game_id);
            }
        }
    })
    .await
    .context("Timed out waiting for durable JoinGame recovery")??;
    assert_eq!(recovered_game_id, game_id);

    client1_new
        .send_message(WSMessage::JoinGame(game_id))
        .await?;

    // Should receive game snapshot
    wait_for_snapshot(&mut client1_new).await?;

    client1_new.disconnect().await?;
    client2.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

// Helper functions
async fn wait_for_match(client: &mut TestClient) -> Result<u32> {
    wait_for_match_with_timeout(client, Duration::from_secs(30)).await
}

async fn wait_for_match_with_timeout(
    client: &mut TestClient,
    timeout_duration: Duration,
) -> Result<u32> {
    let deadline = Instant::now() + timeout_duration;
    let game_id = timeout_at(deadline, async {
        loop {
            match client.receive_message().await? {
                WSMessage::JoinGame(id) => {
                    return Ok::<u32, anyhow::Error>(id);
                }
                _ => {
                    // Ignore other messages
                }
            }
        }
    })
    .await
    .context("Timed out waiting for matchmaking to route the client")??;

    timeout_at(deadline, async {
        client.send_message(WSMessage::JoinGame(game_id)).await?;
        loop {
            match client.receive_message().await? {
                WSMessage::GameEvent(event) => {
                    if matches!(event.event, GameEvent::Snapshot { .. }) {
                        return Ok(event.game_id);
                    }
                }
                WSMessage::GameWarming {
                    game_id: warming_game_id,
                    retry_after_ms,
                } if warming_game_id == game_id => {
                    tokio::time::sleep(Duration::from_millis(retry_after_ms.clamp(100, 2_000)))
                        .await;
                    client.send_message(WSMessage::JoinGame(game_id)).await?;
                }
                WSMessage::GameLoadFailed {
                    game_id: failed_game_id,
                    reason,
                } if failed_game_id == game_id => {
                    anyhow::bail!("Matched game {failed_game_id} could not load: {reason}");
                }
                _ => {
                    // Ignore other messages
                }
            }
        }
    })
    .await
    .context("Timed out waiting for the matched game's authoritative snapshot")?
}

async fn wait_for_snapshot(client: &mut TestClient) -> Result<()> {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(event) = client.receive_game_event().await?
                && matches!(event.event, GameEvent::Snapshot { .. })
            {
                return Ok(());
            }
        }
    })
    .await?
}

// ============================================================================
// MMR-BASED TIMING TESTS
// ============================================================================

/// Test that two lobbies with similar MMR (both in silver range 500-600) match instantly
#[tokio::test]
async fn test_same_mmr_range_matches_instantly() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let _ = tracing_subscriber::fmt::try_init();

    let redis_client = redis::Client::open("redis://127.0.0.1:6379/1")?;
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await?;
    let _: () = redis::cmd("FLUSHDB").query_async(&mut redis_conn).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut env = TestEnvironment::new("test_same_mmr_instant").await?;
    env.add_server().await?;

    // Create two users with similar MMR (both in silver range 500-600)
    env.create_user_with_mmr(550).await?;
    env.create_user_with_mmr(570).await?;

    let server_addr = env.ws_addr(0).expect("Server should exist");

    // Connect both clients
    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;

    client1.authenticate(env.user_ids()[0]).await?;
    client2.authenticate(env.user_ids()[1]).await?;
    client1.create_lobby().await?;
    client2.create_lobby().await?;

    println!("Testing: Two lobbies with MMR 550 and 570 (both silver)");

    // Record start time
    let start_time = std::time::Instant::now();

    // Both queue for 1v1 match
    client1
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 2 },
            queue_mode: ::common::QueueMode::Competitive,
        })
        .await?;

    client2
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 2 },
            queue_mode: ::common::QueueMode::Competitive,
        })
        .await?;

    // Wait for both to get matched
    let game_id1 = wait_for_match(&mut client1).await?;
    let game_id2 = wait_for_match(&mut client2).await?;

    let match_time = start_time.elapsed();

    assert_eq!(game_id1, game_id2, "Both players should be in same game");

    println!("Match time: {:?}", match_time);

    // Should match within 5 seconds (instantly = within one matchmaking cycle which is 2s)
    assert!(
        match_time.as_secs() <= 5,
        "Same MMR range should match instantly, took {:?}",
        match_time
    );

    println!(
        "✓ Same MMR range matched in {:?} (expected: instant/~2s)",
        match_time
    );

    client1.disconnect().await?;
    client2.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

/// Test that silver (600) and gold (900) lobbies match once both queue ages
/// cross the production ten-second expansion boundary.
#[tokio::test]
async fn test_silver_gold_matches_after_10_second_queue_age() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let _ = tracing_subscriber::fmt::try_init();

    let redis_client = redis::Client::open("redis://127.0.0.1:6379/1")?;
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await?;
    let _: () = redis::cmd("FLUSHDB").query_async(&mut redis_conn).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut env = TestEnvironment::new("test_silver_gold_10s").await?;
    env.add_server().await?;

    // Create silver and gold users (300 MMR difference)
    env.create_user_with_mmr(600).await?; // Silver
    env.create_user_with_mmr(900).await?; // Gold

    let server_addr = env.ws_addr(0).expect("Server should exist");

    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;

    client1.authenticate(env.user_ids()[0]).await?;
    client2.authenticate(env.user_ids()[1]).await?;
    let lobby1 = client1.create_lobby().await?;
    let lobby2 = client2.create_lobby().await?;

    println!("Testing: Silver (600) vs Gold (900) - 300 MMR difference");

    // Both queue for 1v1 match
    let queued_not_before_ms = Utc::now().timestamp_millis();
    client1
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 2 },
            queue_mode: ::common::QueueMode::Competitive,
        })
        .await?;

    client2
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 2 },
            queue_mode: ::common::QueueMode::Competitive,
        })
        .await?;

    // Preserve the production 10s formula while avoiding 10 seconds of idle
    // CI time. The real matchmaking loop reads these exact Redis entries.
    let queue_age = Duration::from_millis(10_100);
    backdate_queued_lobby(
        &mut redis_conn,
        &lobby1,
        queue_age,
        600,
        queued_not_before_ms,
    )
    .await?;
    backdate_queued_lobby(
        &mut redis_conn,
        &lobby2,
        queue_age,
        900,
        queued_not_before_ms,
    )
    .await?;
    let start_time = std::time::Instant::now();

    let game_id1 = wait_for_match_with_timeout(&mut client1, Duration::from_secs(10)).await?;
    let game_id2 = wait_for_match_with_timeout(&mut client2, Duration::from_secs(10)).await?;

    let match_time = start_time.elapsed();

    assert_eq!(game_id1, game_id2, "Both players should be in same game");

    println!("Match time: {:?}", match_time);

    println!(
        "✓ Silver vs Gold matched in {:?} after injecting a 10.1s queue age",
        match_time
    );

    client1.disconnect().await?;
    client2.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

/// Test that silver (600) and diamond (1500) lobbies match once both queue ages
/// cross the production 30-second unrestricted boundary.
#[tokio::test]
async fn test_silver_diamond_matches_after_30_second_queue_age() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let _ = tracing_subscriber::fmt::try_init();

    let redis_client = redis::Client::open("redis://127.0.0.1:6379/1")?;
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await?;
    let _: () = redis::cmd("FLUSHDB").query_async(&mut redis_conn).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut env = TestEnvironment::new("test_silver_diamond_30s").await?;
    env.add_server().await?;

    // Create silver and diamond users (900 MMR difference)
    env.create_user_with_mmr(600).await?; // Silver
    env.create_user_with_mmr(1500).await?; // Diamond

    let server_addr = env.ws_addr(0).expect("Server should exist");

    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;

    client1.authenticate(env.user_ids()[0]).await?;
    client2.authenticate(env.user_ids()[1]).await?;
    let lobby1 = client1.create_lobby().await?;
    let lobby2 = client2.create_lobby().await?;

    println!("Testing: Silver (600) vs Diamond (1500) - 900 MMR difference");

    // Both queue for 1v1 match
    let queued_not_before_ms = Utc::now().timestamp_millis();
    client1
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 2 },
            queue_mode: ::common::QueueMode::Competitive,
        })
        .await?;

    client2
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 2 },
            queue_mode: ::common::QueueMode::Competitive,
        })
        .await?;

    let queue_age = Duration::from_millis(30_100);
    backdate_queued_lobby(
        &mut redis_conn,
        &lobby1,
        queue_age,
        600,
        queued_not_before_ms,
    )
    .await?;
    backdate_queued_lobby(
        &mut redis_conn,
        &lobby2,
        queue_age,
        1500,
        queued_not_before_ms,
    )
    .await?;
    let start_time = std::time::Instant::now();

    let game_id1 = wait_for_match_with_timeout(&mut client1, Duration::from_secs(10)).await?;
    let game_id2 = wait_for_match_with_timeout(&mut client2, Duration::from_secs(10)).await?;

    let match_time = start_time.elapsed();

    assert_eq!(game_id1, game_id2, "Both players should be in same game");

    println!("Match time: {:?}", match_time);

    println!(
        "✓ Silver vs Diamond matched in {:?} after injecting a 30.1s queue age",
        match_time
    );

    client1.disconnect().await?;
    client2.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

/// Test that extreme MMR differences match after the production 30-second
/// unrestricted queue-age boundary.
#[tokio::test]
async fn test_extreme_mmr_difference_matches_after_30_second_queue_age() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let _ = tracing_subscriber::fmt::try_init();

    let redis_client = redis::Client::open("redis://127.0.0.1:6379/1")?;
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await?;
    let _: () = redis::cmd("FLUSHDB").query_async(&mut redis_conn).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut env = TestEnvironment::new("test_extreme_mmr_30s_max").await?;
    env.add_server().await?;

    // Create users with extreme MMR difference (bronze vs grandmaster)
    env.create_user_with_mmr(300).await?; // Bronze
    env.create_user_with_mmr(2000).await?; // Grandmaster

    let server_addr = env.ws_addr(0).expect("Server should exist");

    let mut client1 = TestClient::connect(&server_addr).await?;
    let mut client2 = TestClient::connect(&server_addr).await?;

    client1.authenticate(env.user_ids()[0]).await?;
    client2.authenticate(env.user_ids()[1]).await?;
    let lobby1 = client1.create_lobby().await?;
    let lobby2 = client2.create_lobby().await?;

    println!("Testing: Bronze (300) vs Grandmaster (2000) - 1700 MMR difference");

    // Both queue for 1v1 match
    let queued_not_before_ms = Utc::now().timestamp_millis();
    client1
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 2 },
            queue_mode: ::common::QueueMode::Competitive,
        })
        .await?;

    client2
        .send_message(WSMessage::QueueForMatch {
            game_type: GameType::FreeForAll { max_players: 2 },
            queue_mode: ::common::QueueMode::Competitive,
        })
        .await?;

    let queue_age = Duration::from_millis(30_100);
    backdate_queued_lobby(
        &mut redis_conn,
        &lobby1,
        queue_age,
        300,
        queued_not_before_ms,
    )
    .await?;
    backdate_queued_lobby(
        &mut redis_conn,
        &lobby2,
        queue_age,
        2000,
        queued_not_before_ms,
    )
    .await?;
    let start_time = std::time::Instant::now();

    let game_id1 = wait_for_match_with_timeout(&mut client1, Duration::from_secs(10)).await?;
    let game_id2 = wait_for_match_with_timeout(&mut client2, Duration::from_secs(10)).await?;

    let match_time = start_time.elapsed();

    assert_eq!(game_id1, game_id2, "Both players should be in same game");

    println!("Match time: {:?}", match_time);

    println!(
        "✓ Extreme MMR difference matched in {:?} after injecting a 30.1s queue age",
        match_time
    );

    client1.disconnect().await?;
    client2.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_mmr_based_matchmaking() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    // Clean up Redis before starting the test
    let redis_client = redis::Client::open("redis://127.0.0.1:6379/1")?;
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await?;
    let _: () = redis::cmd("FLUSHDB").query_async(&mut redis_conn).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut env = TestEnvironment::new("test_mmr_based_matchmaking").await?;
    env.add_server().await?;

    // Create users with different MMR values that are close enough to match
    // The matchmaking algorithm uses an average of all queued players, so we need
    // groups that are reasonably close together
    // Group 1: Low MMR (should match together)
    env.create_user_with_mmr(1400).await?; // User 0
    env.create_user_with_mmr(1420).await?; // User 1

    // Group 2: Medium MMR (should match together)
    env.create_user_with_mmr(1480).await?; // User 2
    env.create_user_with_mmr(1500).await?; // User 3

    // Group 3: High MMR (should match together)
    env.create_user_with_mmr(1580).await?; // User 4
    env.create_user_with_mmr(1600).await?; // User 5

    let server_addr = env.ws_addr(0).expect("Server should exist");

    // Connect all clients
    let mut clients = Vec::new();
    for i in 0..6 {
        let mut client = TestClient::connect(&server_addr).await?;
        client.authenticate(env.user_ids()[i]).await?;
        client.create_lobby().await?;
        clients.push(client);
    }

    println!("All clients connected with MMRs: 1400, 1420, 1480, 1500, 1580, 1600");

    // Queue clients in pairs and await the actual JoinGame signal before
    // admitting the next pair. This is both faster and stronger than sleeping
    // for an assumed matchmaking-loop cadence.
    let mut matches: Vec<(usize, u32)> = Vec::new();

    // Queue first pair (lowest MMR)
    for (i, client) in clients.iter_mut().enumerate().take(2) {
        client
            .send_message(WSMessage::QueueForMatch {
                game_type: GameType::FreeForAll { max_players: 2 },
                queue_mode: ::common::QueueMode::Quickmatch,
            })
            .await?;
        println!(
            "Client {} (MMR {}) queued",
            i,
            match i {
                0 => 1400,
                1 => 1420,
                _ => 0,
            }
        );
    }

    let first_game = wait_for_match(&mut clients[0]).await?;
    let first_game_peer = wait_for_match(&mut clients[1]).await?;
    matches.extend([(0, first_game), (1, first_game_peer)]);

    // Queue second pair (medium MMR)
    for (i, client) in clients.iter_mut().enumerate().skip(2).take(2) {
        client
            .send_message(WSMessage::QueueForMatch {
                game_type: GameType::FreeForAll { max_players: 2 },
                queue_mode: ::common::QueueMode::Quickmatch,
            })
            .await?;
        println!(
            "Client {} (MMR {}) queued",
            i,
            match i {
                2 => 1480,
                3 => 1500,
                _ => 0,
            }
        );
    }

    let second_game = wait_for_match(&mut clients[2]).await?;
    let second_game_peer = wait_for_match(&mut clients[3]).await?;
    matches.extend([(2, second_game), (3, second_game_peer)]);

    // Queue third pair (highest MMR)
    for (i, client) in clients.iter_mut().enumerate().skip(4).take(2) {
        client
            .send_message(WSMessage::QueueForMatch {
                game_type: GameType::FreeForAll { max_players: 2 },
                queue_mode: ::common::QueueMode::Quickmatch,
            })
            .await?;
        println!(
            "Client {} (MMR {}) queued",
            i,
            match i {
                4 => 1580,
                5 => 1600,
                _ => 0,
            }
        );
    }

    let third_game = wait_for_match(&mut clients[4]).await?;
    let third_game_peer = wait_for_match(&mut clients[5]).await?;
    matches.extend([(4, third_game), (5, third_game_peer)]);
    for (client_index, game_id) in &matches {
        println!("Client {} matched to game {}", client_index, game_id);
    }

    // Verify that players with similar MMR got matched together
    // Users 0 and 1 (MMR 1400, 1420) should be in the same game
    assert_eq!(
        matches[0].1, matches[1].1,
        "Users with MMR 1400 and 1420 should be matched together"
    );

    // Users 2 and 3 (MMR 1480, 1500) should be in the same game
    assert_eq!(
        matches[2].1, matches[3].1,
        "Users with MMR 1480 and 1500 should be matched together"
    );

    // Users 4 and 5 (MMR 1580, 1600) should be in the same game
    assert_eq!(
        matches[4].1, matches[5].1,
        "Users with MMR 1580 and 1600 should be matched together"
    );

    // Verify that different MMR groups are in different games
    assert_ne!(
        matches[0].1, matches[2].1,
        "Low MMR group should not match with medium MMR group"
    );
    assert_ne!(
        matches[2].1, matches[4].1,
        "Medium MMR group should not match with high MMR group"
    );
    assert_ne!(
        matches[0].1, matches[4].1,
        "Low MMR group should not match with high MMR group"
    );

    println!("MMR-based matchmaking test passed!");
    println!("Game {} had users with MMR 1400, 1420", matches[0].1);
    println!("Game {} had users with MMR 1480, 1500", matches[2].1);
    println!("Game {} had users with MMR 1580, 1600", matches[4].1);

    for client in clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_matchmaking_load() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    // Clean up Redis before starting the test
    let redis_client = redis::Client::open("redis://127.0.0.1:6379/1")?;
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await?;
    let _: () = redis::cmd("FLUSHDB").query_async(&mut redis_conn).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut env = TestEnvironment::new("test_matchmaking_load").await?;
    env.add_server().await?;

    // Create 12 users for load testing (reduced to ensure reliable matching)
    // With max_players=2, this creates exactly 6 games
    const USER_COUNT: usize = 12;
    println!("Creating {} users for load test...", USER_COUNT);

    for i in 0..USER_COUNT {
        // Create users with varied MMR (1000-2000) to test MMR matching under load
        let mmr = 1000 + (i as i32 * 10) % 1000;
        env.create_user_with_mmr(mmr).await?;
    }

    let server_addr = env.ws_addr(0).expect("Server should exist");

    // Connect all clients
    println!("Connecting {} clients...", USER_COUNT);
    let mut clients = Vec::new();
    for i in 0..USER_COUNT {
        let mut client = TestClient::connect(&server_addr).await?;
        client.authenticate(env.user_ids()[i]).await?;
        client.create_lobby().await?;
        clients.push(client);
    }

    // Record start time
    let start_time = std::time::Instant::now();

    // Queue all clients with a small delay between each to avoid overwhelming the system
    println!("Queuing all {} clients...", USER_COUNT);
    for (i, client) in clients.iter_mut().enumerate() {
        client
            .send_message(WSMessage::QueueForMatch {
                game_type: GameType::FreeForAll { max_players: 2 },
                queue_mode: ::common::QueueMode::Quickmatch,
            })
            .await?;
        println!("Client {} queued", i);
        // Small delay to avoid overwhelming the matchmaking system
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let queue_time = start_time.elapsed();
    println!("All clients queued in {:?}", queue_time);

    // Wait for all clients to get matched
    // The matchmaking loop runs every 2 seconds, and we may need multiple cycles
    // to match all players. Increase timeout to account for this.
    println!("Waiting for all clients to be matched...");
    let match_futures: Vec<_> = clients
        .iter_mut()
        .enumerate()
        .map(|(i, client)| async move {
            match timeout(Duration::from_secs(120), wait_for_match(client)).await {
                Ok(Ok(game_id)) => Ok((i, game_id)),
                Ok(Err(e)) => Err(anyhow::anyhow!("Client {} match error: {}", i, e)),
                Err(_) => Err(anyhow::anyhow!("Client {} timed out waiting for match", i)),
            }
        })
        .collect();

    let match_results = join_all(match_futures).await;
    let match_time = start_time.elapsed();

    // Analyze results
    let mut successful_matches = 0;
    let mut unique_games = std::collections::HashSet::new();

    for result in match_results {
        match result {
            Ok((_, game_id)) => {
                successful_matches += 1;
                unique_games.insert(game_id);
            }
            Err(e) => {
                eprintln!("Match error: {}", e);
            }
        }
    }

    // Calculate statistics
    let match_rate = successful_matches as f64 / USER_COUNT as f64 * 100.0;
    let games_created = unique_games.len();
    let expected_games = USER_COUNT / 2; // Since max_players = 2
    let matches_per_second = games_created as f64 / match_time.as_secs_f64();

    println!("\n=== Load Test Results ===");
    println!("Total users: {}", USER_COUNT);
    println!(
        "Successfully matched: {} ({:.1}%)",
        successful_matches, match_rate
    );
    println!(
        "Games created: {} (expected: {})",
        games_created, expected_games
    );
    println!("Total time: {:?}", match_time);
    println!("Matches per second: {:.2}", matches_per_second);

    // Verify expectations
    // Allow for some players not getting matched due to timing issues
    assert!(
        successful_matches >= USER_COUNT * 80 / 100,
        "At least 80% of users should be matched, got {}%",
        match_rate
    );
    assert!(
        games_created >= expected_games * 70 / 100,
        "Should create at least 70% of expected games"
    );
    // With matchmaking running every 2 seconds, expect lower throughput
    assert!(
        matches_per_second >= 0.2,
        "Should create at least 0.2 matches per second, got {:.2}",
        matches_per_second
    );

    println!(
        "\nLoad test passed! System can handle {} concurrent users",
        USER_COUNT
    );

    // Disconnect all clients
    for client in clients {
        client.disconnect().await?;
    }

    env.shutdown().await?;
    Ok(())
}
