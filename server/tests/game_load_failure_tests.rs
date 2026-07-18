use ::common::{GameEvent, GameState, GameStatus, GameType, QueueMode};
use anyhow::{Context, Result};
use redis::AsyncCommands;
use server::db::DURABLE_GAME_ID_FLOOR;
use server::game_bus::{BusKind, GameBus};
use server::game_executor::{PARTITION_COUNT, StreamEvent, run_game_executor};
use server::matchmaking_manager::MatchmakingManager;
use server::pubsub_manager::PubSubManager;
use server::redis_keys::RedisKeys;
use server::redis_utils::create_connection_manager;
use server::replication::ReplicationManager;
use server::ws_server::WSMessage;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

mod common;
use self::common::{TestClient, TestEnvironment};

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn joining_an_unknown_game_returns_an_explicit_load_failure_and_can_retry() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("unknown_game_load_failure").await?;
    let user_id = env.create_user().await?;
    let (_, server_id) = env.add_server().await?;

    let server_addr = env.ws_addr(0).expect("test server should be running");
    let mut client = TestClient::connect(&server_addr).await?;
    client.authenticate(user_id).await?;

    let missing_game_id = unique_runtime_game_id();
    client.join_game(missing_game_id).await?;

    let failure = timeout(Duration::from_secs(10), async {
        loop {
            match client.receive_message().await? {
                WSMessage::GameLoadFailed { game_id, reason } => {
                    return Ok::<_, anyhow::Error>((game_id, reason));
                }
                _ => continue,
            }
        }
    })
    .await??;

    assert_eq!(failure.0, missing_game_id);
    assert!(
        failure.1.to_lowercase().contains("not found")
            || failure.1.to_lowercase().contains("expired"),
        "unexpected failure reason: {}",
        failure.1
    );

    // A retry uses the same socket. Persisting the game between attempts proves
    // that a second JoinGame request restarts the subscription after a failure.
    let mut final_state = completed_game_state(user_id as u32)?;
    final_state.tick = 74;
    env.db()
        .upsert_completed_game(missing_game_id as i32, server_id as i32, &final_state)
        .await?;
    client.join_game(missing_game_id).await?;

    let retried_state = receive_snapshot(&mut client).await?;
    assert_eq!(retried_state.status, final_state.status);
    assert_eq!(retried_state.tick, final_state.tick);

    client.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn joining_a_durably_saved_completed_game_returns_its_final_snapshot() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("durable_completed_game_reload").await?;
    let user_id = env.create_user().await?;
    let (_, server_id) = env.add_server().await?;

    let game_id = unique_runtime_game_id();
    let mut final_state = completed_game_state(user_id as u32)?;
    final_state.tick = 73;
    final_state.event_sequence = 101;
    final_state.scores.insert(0, 900);

    env.db()
        .upsert_completed_game(game_id as i32, server_id as i32, &final_state)
        .await?;

    // Reproduce the completion race: Dynamo already has the final state while Redis still
    // contains the preceding active snapshot. Loading must prefer the durable terminal state.
    let redis_url = std::env::var("SNAKETRON_REDIS_URL")?;
    let redis_client = redis::Client::open(redis_url)?;
    let mut redis = redis_client.get_multiplexed_async_connection().await?;
    let mut stale_state = completed_game_state(user_id as u32)?;
    stale_state.tick = final_state.tick - 1;
    stale_state.event_sequence = final_state.event_sequence - 1;
    stale_state.status = GameStatus::Started {
        server_id: server_id as u64,
    };
    let _: () = redis
        .set_ex(
            RedisKeys::game_snapshot(game_id),
            serde_json::to_vec(&stale_state)?,
            300,
        )
        .await?;

    let server_addr = env.ws_addr(0).expect("test server should be running");
    let mut client = TestClient::connect(&server_addr).await?;
    client.authenticate(user_id).await?;
    client.join_game(game_id).await?;

    let loaded_state = receive_snapshot(&mut client).await?;

    assert_eq!(loaded_state.status, final_state.status);
    assert_eq!(loaded_state.tick, final_state.tick);
    assert_eq!(loaded_state.event_sequence, final_state.event_sequence);
    assert_eq!(loaded_state.scores, final_state.scores);

    client.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn completed_game_snapshots_are_denied_to_non_players_in_redis_and_dynamo() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("completed_game_reload_access_denied").await?;
    let player_user_id = env.create_user().await?;
    let spectator_user_id = env.create_user().await?;
    let other_user_id = env.create_user().await?;
    let (_, server_id) = env.add_server().await?;

    let game_id = unique_runtime_game_id();
    let mut final_state = completed_game_state(player_user_id as u32)?;
    final_state.add_spectator(
        spectator_user_id as u32,
        Some("Recorded spectator".to_string()),
    );
    env.db()
        .upsert_completed_game(game_id as i32, server_id as i32, &final_state)
        .await?;

    let redis_url = std::env::var("SNAKETRON_REDIS_URL")?;
    let redis_client = redis::Client::open(redis_url)?;
    let mut redis = redis_client.get_multiplexed_async_connection().await?;
    let snapshot_key = RedisKeys::game_snapshot(game_id);
    let _: () = redis
        .set_ex(&snapshot_key, serde_json::to_vec(&final_state)?, 300)
        .await?;
    let chat_history_key = RedisKeys::game_chat_history_key(game_id);
    let seeded_chat = serde_json::json!({
        "game_id": game_id,
        "message_id": "private-completed-game-chat",
        "user_id": player_user_id,
        "username": "Reloading player",
        "message": "participants only",
        "timestamp_ms": chrono::Utc::now().timestamp_millis(),
    })
    .to_string();
    let _: i64 = redis.rpush(&chat_history_key, seeded_chat).await?;
    let initial_chat_history_len: usize = redis.llen(&chat_history_key).await?;

    let server_addr = env.ws_addr(0).expect("test server should be running");
    let mut spectator_client = TestClient::connect(&server_addr).await?;
    spectator_client.authenticate(spectator_user_id).await?;
    spectator_client.join_game(game_id).await?;
    let spectator_state = receive_snapshot(&mut spectator_client).await?;
    assert!(
        spectator_state
            .spectators
            .contains(&(spectator_user_id as u32))
    );
    spectator_client.disconnect().await?;

    let mut client = TestClient::connect(&server_addr).await?;
    client.authenticate(other_user_id).await?;
    client.join_game(game_id).await?;

    let redis_failure = receive_load_failure_without_game_chat(&mut client, game_id).await?;
    assert_eq!(redis_failure.0, game_id);
    assert_eq!(redis_failure.1, "This game is unavailable");

    // A denied JoinGame must leave the connection outside the game. It therefore cannot publish
    // to the game's chat history (or receive its own message through a leaked subscription).
    client
        .send_message(WSMessage::Chat(
            "unauthorized completed-game write".to_string(),
        ))
        .await?;
    let chat_denial = receive_chat_denial_without_game_chat(&mut client, game_id).await?;
    assert_eq!(chat_denial, "Chat is only available in a lobby or game");
    let chat_history_len: usize = redis.llen(&chat_history_key).await?;
    assert_eq!(
        chat_history_len, initial_chat_history_len,
        "denied connection appended to completed-game chat history"
    );

    // Remove the grace-period cache and retry on the same socket to exercise the durable
    // fallback independently. The requesting user is still absent from GameState.players.
    let _: () = redis.del(&snapshot_key).await?;
    client.join_game(game_id).await?;

    let dynamo_failure = receive_load_failure_without_game_chat(&mut client, game_id).await?;
    assert_eq!(dynamo_failure.0, game_id);
    assert_eq!(dynamo_failure.1, "This game is unavailable");

    client
        .send_message(WSMessage::Chat(
            "unauthorized durable-game write".to_string(),
        ))
        .await?;
    let durable_chat_denial = receive_chat_denial_without_game_chat(&mut client, game_id).await?;
    assert_eq!(
        durable_chat_denial,
        "Chat is only available in a lobby or game"
    );
    let durable_chat_history_len: usize = redis.llen(&chat_history_key).await?;
    assert_eq!(
        durable_chat_history_len, initial_chat_history_len,
        "Dynamo-denied connection appended to completed-game chat history"
    );

    client.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn live_game_join_is_authorized_before_enabling_game_chat() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("live_game_join_access_denied").await?;
    let player_user_id = env.create_user().await?;
    let other_user_id = env.create_user().await?;
    let (_, server_id) = env.add_server().await?;
    let game_id = unique_runtime_game_id();

    let mut live_state = completed_game_state(player_user_id as u32)?;
    live_state.status = GameStatus::Started { server_id };

    let redis_url = std::env::var("SNAKETRON_REDIS_URL")?;
    let redis_client = redis::Client::open(redis_url)?;
    let (push_tx, _) = broadcast::channel(16);
    let publisher_connection =
        create_connection_manager(redis_client.clone(), push_tx.clone()).await?;
    // Publish on the same transport the test server's replicas subscribe to.
    let publisher = GameBus::new(
        BusKind::from_env(),
        PubSubManager::new(
            publisher_connection.clone(),
            push_tx,
            CancellationToken::new(),
        ),
        publisher_connection,
        redis_client.clone(),
        CancellationToken::new(),
    );
    let partition_id = game_id % PARTITION_COUNT;

    // Pub/Sub startup is asynchronous. Republish until this server's replica proves the live
    // state is available to the same authorization path used by JoinGame.
    timeout(Duration::from_secs(10), async {
        loop {
            publisher
                .publish_snapshot(partition_id, game_id, &live_state, 0)
                .await?;
            if env
                .server(0)
                .expect("test server should be running")
                .replication_manager()
                .get_game_state_when_ready(game_id)
                .await
                .is_some()
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .context("live game did not reach the replication cache")??;

    let mut redis = redis_client.get_multiplexed_async_connection().await?;
    let chat_history_key = RedisKeys::game_chat_history_key(game_id);
    let seeded_chat = serde_json::json!({
        "game_id": game_id,
        "message_id": "private-live-game-chat",
        "user_id": player_user_id,
        "username": "Reloading player",
        "message": "live participants only",
        "timestamp_ms": chrono::Utc::now().timestamp_millis(),
    })
    .to_string();
    let _: i64 = redis.rpush(&chat_history_key, seeded_chat).await?;
    let initial_chat_history_len: usize = redis.llen(&chat_history_key).await?;

    let server_addr = env.ws_addr(0).expect("test server should be running");
    let mut client = TestClient::connect(&server_addr).await?;
    client.authenticate(other_user_id).await?;
    client.join_game(game_id).await?;

    let failure = receive_load_failure_without_game_chat(&mut client, game_id).await?;
    assert_eq!(failure.0, game_id);
    assert_eq!(failure.1, "This game is unavailable");

    client
        .send_message(WSMessage::Chat("unauthorized live-game write".to_string()))
        .await?;
    let chat_denial = receive_chat_denial_without_game_chat(&mut client, game_id).await?;
    assert_eq!(chat_denial, "Chat is only available in a lobby or game");
    let chat_history_len: usize = redis.llen(&chat_history_key).await?;
    assert_eq!(chat_history_len, initial_chat_history_len);

    client.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn initial_join_waits_for_the_live_replica_after_executor_snapshot_storage() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("initial_join_replica_readiness").await?;
    let player_user_id = env.create_user().await?;
    let (_, server_id) = env.add_server().await?;
    let game_id = unique_runtime_game_id();

    let mut live_state = completed_game_state(player_user_id as u32)?;
    live_state.status = GameStatus::Started { server_id };
    live_state.event_sequence = 17;

    let redis_url = std::env::var("SNAKETRON_REDIS_URL")?;
    let redis_client = redis::Client::open(redis_url)?;
    let mut redis = redis_client.get_multiplexed_async_connection().await?;
    let snapshot_key = RedisKeys::game_snapshot(game_id);
    let _: () = redis
        .set_ex(&snapshot_key, serde_json::to_vec(&live_state)?, 300)
        .await?;

    let (push_tx, _) = broadcast::channel(16);
    let publisher_connection =
        create_connection_manager(redis_client.clone(), push_tx.clone()).await?;
    // Publish on the same transport the test server's replicas subscribe to.
    let publisher = GameBus::new(
        BusKind::from_env(),
        PubSubManager::new(
            publisher_connection.clone(),
            push_tx,
            CancellationToken::new(),
        ),
        publisher_connection,
        redis_client.clone(),
        CancellationToken::new(),
    );

    let server_addr = env.ws_addr(0).expect("test server should be running");
    let mut client = TestClient::connect(&server_addr).await?;
    client.authenticate(player_user_id).await?;
    client.join_game(game_id).await?;

    // The server sees the executor-stored Redis snapshot first and enters its bounded
    // readiness wait. Publishing shortly afterward models the replica consuming the initial
    // snapshot after GameCreated was already acknowledged.
    tokio::time::sleep(Duration::from_millis(200)).await;
    publisher
        .publish_snapshot(game_id % PARTITION_COUNT, game_id, &live_state, 0)
        .await?;

    let loaded_state = receive_snapshot(&mut client).await?;
    assert_eq!(loaded_state.start_ms, live_state.start_ms);
    assert_eq!(loaded_state.event_sequence, live_state.event_sequence);
    assert_eq!(loaded_state.status, live_state.status);

    let _: usize = redis.del(snapshot_key).await?;
    client.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn durable_allocator_skips_retained_ids_and_completion_upsert_rejects_collisions()
-> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("durable_game_id_collision_safety").await?;
    let user_id = env.create_user().await?;
    let (_, server_id) = env.add_server().await?;

    let first_allocated_id = env.db().allocate_game_id().await?;
    let retained_id = first_allocated_id + 1;
    let retained_state = completed_game_state(user_id as u32)?;
    env.db()
        .upsert_completed_game(retained_id, server_id as i32, &retained_state)
        .await?;

    let next_allocated_id = env.db().allocate_game_id().await?;
    assert!(
        next_allocated_id > retained_id,
        "durable allocation must skip retained game IDs"
    );

    let mut colliding_state = completed_game_state(user_id as u32)?;
    colliding_state.start_ms = retained_state.start_ms - 5_000;
    let collision_error = env
        .db()
        .upsert_completed_game(retained_id, server_id as i32, &colliding_state)
        .await
        .expect_err("a different runtime identity must not overwrite retained history");
    assert!(
        collision_error
            .to_string()
            .contains("persist completed game state"),
        "unexpected collision error: {collision_error:#}"
    );

    let loaded = env
        .db()
        .get_game_by_id(retained_id)
        .await?
        .expect("retained game should still exist")
        .game_state
        .expect("retained game should have a final state");
    let loaded_state: GameState = serde_json::from_value(loaded)?;
    assert_eq!(loaded_state.start_ms, retained_state.start_ms);

    // Metadata-first games created through the database API are an intentional adoption
    // case, not an ID collision.
    let metadata_game_id = env
        .db()
        .create_game(
            server_id as i32,
            &serde_json::to_value(GameType::Solo)?,
            "matchmaking",
            false,
            None,
        )
        .await?;
    let metadata_game_state = completed_game_state(user_id as u32)?;
    env.db()
        .upsert_completed_game(metadata_game_id, server_id as i32, &metadata_game_state)
        .await?;

    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn durable_allocator_stays_disjoint_from_legacy_ids_across_redis_loss() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let env = TestEnvironment::new("mixed_allocator_rollout_safety").await?;
    let redis_url = std::env::var("SNAKETRON_REDIS_URL")?;
    let redis_client = redis::Client::open(redis_url)?;
    let mut legacy_connection = redis_client.get_multiplexed_async_connection().await?;
    let (push_tx, _) = broadcast::channel(16);
    let manager_connection = create_connection_manager(redis_client, push_tx).await?;
    let mut matchmaking_manager = MatchmakingManager::new(manager_connection)?;
    let counter_key = RedisKeys::game_id_counter();

    let _: usize = legacy_connection.del(&counter_key).await?;
    let legacy_before: i32 = legacy_connection.incr(&counter_key, 1).await?;
    let durable_before = matchmaking_manager
        .generate_game_id(env.db().as_ref())
        .await?;
    let legacy_after: i32 = legacy_connection.incr(&counter_key, 1).await?;

    assert!(legacy_before < DURABLE_GAME_ID_FLOOR);
    assert_eq!(legacy_after, legacy_before + 1);
    assert!(durable_before >= DURABLE_GAME_ID_FLOOR as u32);

    // Losing Redis can rewind a legacy allocator, but it cannot rewind or overlap the
    // disjoint DynamoDB epoch used by new nodes.
    let _: usize = legacy_connection.del(&counter_key).await?;
    let legacy_after_reset: i32 = legacy_connection.incr(&counter_key, 1).await?;
    let durable_after_reset = matchmaking_manager
        .generate_game_id(env.db().as_ref())
        .await?;

    assert!(legacy_after_reset < DURABLE_GAME_ID_FLOOR);
    assert!(durable_after_reset > durable_before);
    assert_ne!(durable_after_reset, legacy_after_reset as u32);
    let legacy_counter_after_new_allocation: i32 = legacy_connection.get(&counter_key).await?;
    assert_eq!(legacy_counter_after_new_allocation, legacy_after_reset);

    // If the rollout precondition is violated, fail rather than silently sharing a namespace.
    let _: () = legacy_connection
        .set(&counter_key, DURABLE_GAME_ID_FLOOR)
        .await?;
    let error = matchmaking_manager
        .generate_game_id(env.db().as_ref())
        .await
        .expect_err("legacy counter entering the durable epoch must fail fast");
    assert!(error.to_string().contains("reserved durable namespace"));
    let _: usize = legacy_connection.del(&counter_key).await?;

    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn executor_persists_a_completed_game_for_reload_after_cache_loss() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("executor_completed_game_persistence").await?;
    let user_id = env.create_user().await?;
    let game_id = env.db().allocate_game_id().await? as u32;
    let final_state = completed_game_state(user_id as u32)?;
    let partition_id = game_id % PARTITION_COUNT;

    let redis_url = std::env::var("SNAKETRON_REDIS_URL")?;
    let redis_client = redis::Client::open(redis_url)?;
    let cancellation_token = CancellationToken::new();
    let (push_tx, _) = broadcast::channel(32);
    let executor_connection =
        create_connection_manager(redis_client.clone(), push_tx.clone()).await?;
    let executor_pubsub = PubSubManager::new(
        executor_connection.clone(),
        push_tx.clone(),
        cancellation_token.clone(),
    );
    // This test drives hand-built Pub/Sub publishers, so the executor's bus
    // is pinned to pubsub independent of the SNAKETRON_BUS test matrix.
    let executor_bus = Arc::new(GameBus::new(
        BusKind::PubSub,
        executor_pubsub,
        executor_connection.clone(),
        redis_client.clone(),
        cancellation_token.clone(),
    ));
    let publisher_pubsub = PubSubManager::new(
        executor_connection.clone(),
        push_tx,
        cancellation_token.clone(),
    );
    let replication_manager = Arc::new(
        ReplicationManager::new(
            vec![partition_id],
            cancellation_token.clone(),
            &std::env::var("SNAKETRON_REDIS_URL")?,
            BusKind::PubSub,
        )
        .await?,
    );
    let executor_db = env.db();
    let executor_token = cancellation_token.clone();
    let executor_task = tokio::spawn(async move {
        run_game_executor(
            4242,
            partition_id,
            executor_connection,
            executor_bus,
            executor_db,
            replication_manager,
            executor_token,
        )
        .await
    });

    // The acknowledged publisher retries across the executor's subscription startup window.
    publisher_pubsub
        .publish_command(
            partition_id,
            &StreamEvent::GameCreated {
                game_id,
                game_state: final_state.clone(),
            },
        )
        .await
        .context("executor did not acknowledge GameCreated")?;

    timeout(Duration::from_secs(20), async {
        loop {
            if let Some(game) = env.db().get_game_by_id(game_id as i32).await? {
                if let Some(game_state) = game.game_state {
                    let persisted_state: GameState = serde_json::from_value(game_state)?;
                    if persisted_state.start_ms == final_state.start_ms
                        && matches!(persisted_state.status, GameStatus::Complete { .. })
                    {
                        return Ok::<_, anyhow::Error>(());
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .context("executor did not persist the completed game in time")??;

    cancellation_token.cancel();
    timeout(Duration::from_secs(5), executor_task)
        .await
        .context("direct game executor did not stop in time")?
        .context("direct game executor task panicked")??;

    // Remove the grace-period cache so the reload proves the executor wrote DynamoDB.
    let mut redis = redis_client.get_multiplexed_async_connection().await?;
    let _: usize = redis.del(RedisKeys::game_snapshot(game_id)).await?;

    // A newly started server has neither the old in-memory replica nor the deleted Redis
    // snapshot, so its successful reload necessarily comes from DynamoDB.
    let (reload_server_index, _) = env.add_server().await?;
    let server_addr = env
        .ws_addr(reload_server_index)
        .expect("reload test server should be running");
    let mut client = TestClient::connect(&server_addr).await?;
    client.authenticate(user_id).await?;
    client.join_game(game_id).await?;
    let loaded_state = receive_snapshot(&mut client).await?;
    assert_eq!(loaded_state.status, final_state.status);

    client.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

async fn receive_snapshot(client: &mut TestClient) -> Result<GameState> {
    timeout(Duration::from_secs(10), async {
        loop {
            match client.receive_message().await? {
                WSMessage::GameEvent(event) => {
                    if let GameEvent::Snapshot { game_state } = event.event {
                        return Ok::<_, anyhow::Error>(game_state);
                    }
                }
                WSMessage::GameLoadFailed { reason, .. } => {
                    return Err(anyhow::anyhow!("saved game failed to load: {reason}"));
                }
                _ => continue,
            }
        }
    })
    .await
    .context("timed out waiting for a game snapshot")?
}

async fn receive_load_failure_without_game_chat(
    client: &mut TestClient,
    denied_game_id: u32,
) -> Result<(u32, String)> {
    timeout(Duration::from_secs(10), async {
        loop {
            match client.receive_message().await? {
                WSMessage::GameLoadFailed { game_id, reason } => {
                    return Ok::<_, anyhow::Error>((game_id, reason));
                }
                WSMessage::GameChatHistory { game_id, .. }
                | WSMessage::GameChatMessage { game_id, .. }
                    if game_id == denied_game_id =>
                {
                    return Err(anyhow::anyhow!(
                        "denied user received chat data for game {denied_game_id}"
                    ));
                }
                _ => continue,
            }
        }
    })
    .await?
}

async fn receive_chat_denial_without_game_chat(
    client: &mut TestClient,
    denied_game_id: u32,
) -> Result<String> {
    timeout(Duration::from_secs(3), async {
        loop {
            match client.receive_message().await? {
                WSMessage::AccessDenied { reason } => return Ok::<_, anyhow::Error>(reason),
                WSMessage::GameChatHistory { game_id, .. }
                | WSMessage::GameChatMessage { game_id, .. }
                    if game_id == denied_game_id =>
                {
                    return Err(anyhow::anyhow!(
                        "denied user received chat data for game {denied_game_id}"
                    ));
                }
                _ => continue,
            }
        }
    })
    .await?
}

fn completed_game_state(player_user_id: u32) -> Result<GameState> {
    let mut state = GameState::new(
        20,
        20,
        GameType::Solo,
        QueueMode::Quickmatch,
        Some(8675309),
        chrono::Utc::now().timestamp_millis() - 10_000,
    );
    state.add_player(player_user_id, Some("Reloading player".to_string()))?;
    state.status = GameStatus::Complete {
        winning_snake_id: Some(0),
    };
    Ok(state)
}

fn unique_runtime_game_id() -> u32 {
    let bytes = uuid::Uuid::new_v4().into_bytes();
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) & (i32::MAX as u32)
}
