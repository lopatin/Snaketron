use ::common::{GameEvent, GameState, GameType, QueueMode, TeamId};
use anyhow::Result;
use chrono::Utc;
use futures_util::StreamExt;
use redis::{AsyncCommands, Client, PushInfo};
use server::{
    game_bus::GameBus,
    game_executor::StreamEvent,
    lobby_manager::{Lobby, LobbyMember, LobbyPreferences},
    matchmaking_manager::{
        ActiveMatch, GameCreatedOutboxRecord, MatchCommitOutcome, MatchStatus, MatchmakingManager,
        QueuedPlayer,
    },
    redis_keys::RedisKeys,
    redis_utils::create_connection_manager,
    ws_server::WSMessage,
};
use std::collections::BTreeMap;
use tokio::{
    sync::broadcast,
    time::{Duration, timeout},
};
use tokio_util::sync::CancellationToken;

mod common;
use self::common::{TestClient, TestEnvironment};

use tracing_subscriber::{EnvFilter, fmt};

// Serializes the tests in this binary: TestEnvironment::new() sets process-wide
// env vars (DYNAMODB_TABLE_PREFIX, SNAKETRON_REDIS_URL) and flushes the shared
// Redis test database, so concurrently running tests corrupt each other.
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub fn init_tracing() {
    // try_init: another test may have already installed the global subscriber.
    let _ = fmt()
        // allow configuring via RUST_LOG, e.g. RUST_LOG=trace
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();
}

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

async fn seed_lobby_metadata(lobby_codes: &[&str]) -> Result<()> {
    let mut redis = Client::open(test_redis_url())?
        .get_multiplexed_async_connection()
        .await?;
    let mut pipe = redis::pipe();
    pipe.atomic();
    for lobby_code in lobby_codes {
        pipe.hset(RedisKeys::lobby_metadata(lobby_code), "state", "waiting");
    }
    let _: () = pipe.query_async(&mut redis).await?;
    Ok(())
}

async fn lobby_state(lobby_code: &str) -> Result<Option<String>> {
    let mut redis = Client::open(test_redis_url())?
        .get_multiplexed_async_connection()
        .await?;
    Ok(redis
        .hget(RedisKeys::lobby_metadata(lobby_code), "state")
        .await?)
}

#[tokio::test]
async fn repeated_and_concurrent_lobby_admission_keeps_one_queue_identity() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    setup_test_redis().await?;
    seed_lobby_metadata(&["REPEAT1"]).await?;

    let mut left = create_test_matchmaking_manager().await?;
    let mut right = create_test_matchmaking_manager().await?;
    let game_type = GameType::FreeForAll { max_players: 2 };
    let queue_mode = QueueMode::Quickmatch;
    let members = vec![make_lobby_member(91, "repeat-player")];

    let (left_result, right_result) = tokio::join!(
        left.add_lobby_to_queue(
            "REPEAT1",
            members.clone(),
            1_000,
            vec![game_type.clone()],
            queue_mode.clone(),
            91,
        ),
        right.add_lobby_to_queue(
            "REPEAT1",
            members.clone(),
            1_000,
            vec![game_type.clone()],
            queue_mode.clone(),
            91,
        )
    );
    left_result?;
    right_result?;

    left.add_lobby_to_queue(
        "REPEAT1",
        members,
        1_000,
        vec![game_type.clone()],
        queue_mode.clone(),
        91,
    )
    .await?;

    let mut redis = Client::open(test_redis_url())?
        .get_multiplexed_async_connection()
        .await?;
    let queue_key = RedisKeys::matchmaking_lobby_queue(&game_type, &queue_mode);
    let mmr_key = RedisKeys::matchmaking_lobby_mmr_index(&game_type, &queue_mode);
    let identity_key = RedisKeys::matchmaking_lobby_queue_identity("REPEAT1");
    let queue_members: Vec<String> = redis.zrange(&queue_key, 0, -1).await?;
    let mmr_members: Vec<String> = redis.zrange(&mmr_key, 0, -1).await?;
    let identity: String = redis.get(&identity_key).await?;

    assert_eq!(queue_members, vec![identity.clone()]);
    assert_eq!(mmr_members, vec![identity.clone()]);
    let queued_lobby: server::matchmaking_manager::QueuedLobby = serde_json::from_str(&identity)?;
    assert_eq!(queued_lobby.lobby_code, "REPEAT1");
    assert!(!queued_lobby.queue_token.is_empty());
    let user_queue_identity: String = redis
        .get(RedisKeys::matchmaking_user_queue_identity(91))
        .await?;
    assert_eq!(
        user_queue_identity,
        format!("REPEAT1:{}", queued_lobby.queue_token)
    );
    assert_eq!(lobby_state("REPEAT1").await?.as_deref(), Some("queued"));

    Ok(())
}

#[tokio::test]
async fn one_user_cannot_be_admitted_through_two_lobbies() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    setup_test_redis().await?;
    seed_lobby_metadata(&["USERA1", "USERB1"]).await?;

    let mut left = create_test_matchmaking_manager().await?;
    let mut right = create_test_matchmaking_manager().await?;
    let game_type = GameType::FreeForAll { max_players: 2 };
    let queue_mode = QueueMode::Quickmatch;

    let (left_result, right_result) = tokio::join!(
        left.add_lobby_to_queue(
            "USERA1",
            vec![make_lobby_member(93, "shared-player")],
            1_000,
            vec![game_type.clone()],
            queue_mode.clone(),
            93,
        ),
        right.add_lobby_to_queue(
            "USERB1",
            vec![make_lobby_member(93, "shared-player")],
            1_000,
            vec![game_type.clone()],
            queue_mode.clone(),
            93,
        )
    );
    assert_ne!(
        left_result.is_ok(),
        right_result.is_ok(),
        "exactly one lobby may reserve a user"
    );

    let (winner, loser) = if left_result.is_ok() {
        ("USERA1", "USERB1")
    } else {
        ("USERB1", "USERA1")
    };
    assert!(left.get_queued_lobby_by_code(winner).await?.is_some());
    assert!(left.get_queued_lobby_by_code(loser).await?.is_none());
    assert_eq!(lobby_state(winner).await?.as_deref(), Some("queued"));
    assert_eq!(lobby_state(loser).await?.as_deref(), Some("waiting"));
    assert_eq!(
        left.get_queued_lobbies(&game_type, &queue_mode)
            .await?
            .len(),
        1
    );

    assert!(
        left.remove_lobby_from_all_queues_by_code(winner).await?,
        "winner cancellation should release its user reservation"
    );
    assert_eq!(lobby_state(winner).await?.as_deref(), Some("waiting"));
    right
        .add_lobby_to_queue(
            loser,
            vec![make_lobby_member(93, "shared-player")],
            1_000,
            vec![game_type],
            queue_mode,
            93,
        )
        .await?;
    assert!(right.get_queued_lobby_by_code(loser).await?.is_some());
    assert_eq!(lobby_state(loser).await?.as_deref(), Some("queued"));

    Ok(())
}

#[tokio::test]
async fn cancellation_compare_deletes_only_the_observed_queue_identity() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    setup_test_redis().await?;
    seed_lobby_metadata(&["CANCEL1"]).await?;

    let mut manager = create_test_matchmaking_manager().await?;
    let game_types = vec![
        GameType::TeamMatch { per_team: 1 },
        GameType::FreeForAll { max_players: 2 },
    ];
    let queue_mode = QueueMode::Quickmatch;
    manager
        .add_lobby_to_queue(
            "CANCEL1",
            vec![make_lobby_member(92, "cancel-player")],
            1_100,
            game_types.clone(),
            queue_mode.clone(),
            92,
        )
        .await?;

    let admitted = manager
        .get_queued_lobby_by_code("CANCEL1")
        .await?
        .expect("lobby should have one queue identity");
    let mut stale = admitted.clone();
    stale.queue_token = uuid::Uuid::new_v4().to_string();
    manager.remove_lobby_from_all_queues(&stale).await?;

    let still_admitted = manager
        .get_queued_lobby_by_code("CANCEL1")
        .await?
        .expect("stale cancellation must not remove the current identity");
    assert_eq!(still_admitted.queue_token, admitted.queue_token);
    assert_eq!(lobby_state("CANCEL1").await?.as_deref(), Some("queued"));

    let mut redis = Client::open(test_redis_url())?
        .get_multiplexed_async_connection()
        .await?;
    let user_queue_identity_exists: bool = redis
        .exists(RedisKeys::matchmaking_user_queue_identity(92))
        .await?;
    assert!(user_queue_identity_exists);

    assert!(
        manager
            .remove_lobby_from_all_queues_by_code("CANCEL1")
            .await?
    );
    assert!(
        !manager
            .remove_lobby_from_all_queues_by_code("CANCEL1")
            .await?
    );
    assert_eq!(lobby_state("CANCEL1").await?.as_deref(), Some("waiting"));

    let identity_exists: bool = redis
        .exists(RedisKeys::matchmaking_lobby_queue_identity("CANCEL1"))
        .await?;
    assert!(!identity_exists);
    let user_queue_identity_exists: bool = redis
        .exists(RedisKeys::matchmaking_user_queue_identity(92))
        .await?;
    assert!(!user_queue_identity_exists);

    let _: () = redis
        .hset(RedisKeys::lobby_metadata("CANCEL1"), "state", "matched")
        .await?;
    let _: () = redis
        .set(RedisKeys::matchmaking_lobby_active_game("CANCEL1"), "1001")
        .await?;
    assert!(
        !manager
            .remove_lobby_from_all_queues_by_code("CANCEL1")
            .await?
    );
    assert_eq!(lobby_state("CANCEL1").await?.as_deref(), Some("matched"));
    for game_type in &game_types {
        let queue_len: usize = redis
            .zcard(RedisKeys::matchmaking_lobby_queue(game_type, &queue_mode))
            .await?;
        let mmr_len: usize = redis
            .zcard(RedisKeys::matchmaking_lobby_mmr_index(
                game_type,
                &queue_mode,
            ))
            .await?;
        assert_eq!((queue_len, mmr_len), (0, 0));
    }

    Ok(())
}

#[tokio::test]
async fn ambiguous_admission_and_cancellation_retries_converge_from_fresh_callers() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    setup_test_redis().await?;
    seed_lobby_metadata(&["RESPONSE1"]).await?;

    let game_type = GameType::FreeForAll { max_players: 2 };
    let queue_mode = QueueMode::Quickmatch;
    let members = vec![make_lobby_member(95, "response-player")];

    let mut first_caller = create_test_matchmaking_manager().await?;
    first_caller
        .add_lobby_to_queue(
            "RESPONSE1",
            members.clone(),
            1_000,
            vec![game_type.clone()],
            queue_mode.clone(),
            95,
        )
        .await?;
    let first_identity = first_caller
        .get_queued_lobby_by_code("RESPONSE1")
        .await?
        .expect("first admission must commit one identity");
    drop(first_caller); // Model loss of the successful script response/caller.

    let mut retrying_caller = create_test_matchmaking_manager().await?;
    retrying_caller
        .add_lobby_to_queue(
            "RESPONSE1",
            members,
            1_000,
            vec![game_type.clone()],
            queue_mode.clone(),
            95,
        )
        .await?;
    let retried_identity = retrying_caller
        .get_queued_lobby_by_code("RESPONSE1")
        .await?
        .expect("admission retry must recover the committed identity");
    assert_eq!(retried_identity.queue_token, first_identity.queue_token);
    assert_eq!(lobby_state("RESPONSE1").await?.as_deref(), Some("queued"));

    let mut redis = Client::open(test_redis_url())?
        .get_multiplexed_async_connection()
        .await?;
    let queue_key = RedisKeys::matchmaking_lobby_queue(&game_type, &queue_mode);
    let mmr_key = RedisKeys::matchmaking_lobby_mmr_index(&game_type, &queue_mode);
    assert_eq!(redis.zcard::<_, usize>(&queue_key).await?, 1);
    assert_eq!(redis.zcard::<_, usize>(&mmr_key).await?, 1);

    assert!(
        retrying_caller
            .remove_lobby_from_all_queues_by_code("RESPONSE1")
            .await?
    );
    drop(retrying_caller); // Model loss of the successful cancellation response.

    let mut cancellation_retry = create_test_matchmaking_manager().await?;
    assert!(
        !cancellation_retry
            .remove_lobby_from_all_queues_by_code("RESPONSE1")
            .await?
    );
    assert_eq!(lobby_state("RESPONSE1").await?.as_deref(), Some("waiting"));
    assert_eq!(redis.zcard::<_, usize>(&queue_key).await?, 0);
    assert_eq!(redis.zcard::<_, usize>(&mmr_key).await?, 0);
    assert!(
        !redis
            .exists::<_, bool>(RedisKeys::matchmaking_lobby_queue_identity("RESPONSE1"))
            .await?
    );
    assert!(
        !redis
            .exists::<_, bool>(RedisKeys::matchmaking_user_queue_identity(95))
            .await?
    );

    Ok(())
}

#[tokio::test]
async fn admission_requires_live_metadata_and_cancel_does_not_resurrect_it() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    setup_test_redis().await?;

    let mut manager = create_test_matchmaking_manager().await?;
    let game_type = GameType::FreeForAll { max_players: 2 };
    let queue_mode = QueueMode::Quickmatch;
    let admission = manager
        .add_lobby_to_queue(
            "EXPIRE1",
            vec![make_lobby_member(94, "expired-player")],
            1_000,
            vec![game_type.clone()],
            queue_mode.clone(),
            94,
        )
        .await;
    assert!(admission.is_err());
    assert!(manager.get_queued_lobby_by_code("EXPIRE1").await?.is_none());

    seed_lobby_metadata(&["EXPIRE1"]).await?;
    manager
        .add_lobby_to_queue(
            "EXPIRE1",
            vec![make_lobby_member(94, "expired-player")],
            1_000,
            vec![game_type.clone()],
            queue_mode.clone(),
            94,
        )
        .await?;

    let mut redis = Client::open(test_redis_url())?
        .get_multiplexed_async_connection()
        .await?;
    let _: () = redis.del(RedisKeys::lobby_metadata("EXPIRE1")).await?;
    assert!(
        manager
            .remove_lobby_from_all_queues_by_code("EXPIRE1")
            .await?
    );
    let metadata_exists: bool = redis.exists(RedisKeys::lobby_metadata("EXPIRE1")).await?;
    let queue_len: usize = redis
        .zcard(RedisKeys::matchmaking_lobby_queue(&game_type, &queue_mode))
        .await?;
    let user_claim_exists: bool = redis
        .exists(RedisKeys::matchmaking_user_queue_identity(94))
        .await?;
    assert!(!metadata_exists);
    assert_eq!(queue_len, 0);
    assert!(!user_claim_exists);

    Ok(())
}

fn committed_match_fixture(game_id: u32, user_ids: &[u32]) -> Result<(ActiveMatch, String)> {
    let game_type = GameType::FreeForAll { max_players: 2 };
    let mut game_state = GameState::new(
        40,
        40,
        game_type.clone(),
        QueueMode::Quickmatch,
        Some(u64::from(game_id)),
        Utc::now().timestamp_millis() + 3_000,
    );
    let mut players = Vec::new();
    for user_id in user_ids {
        let username = format!("atomic-player-{user_id}");
        game_state.add_player(*user_id, Some(username.clone()))?;
        players.push(QueuedPlayer {
            user_id: *user_id,
            mmr: 1_000,
            username,
        });
    }
    game_state.spawn_initial_food();

    let active_match = ActiveMatch {
        players,
        spectators: Vec::new(),
        lobby_codes: Vec::new(),
        game_type,
        status: MatchStatus::Waiting,
        partition_id: game_id % server::game_executor::PARTITION_COUNT,
        created_at: Utc::now().timestamp_millis(),
    };
    let payload = serde_json::to_string(&StreamEvent::GameCreated {
        game_id,
        game_state,
    })?;

    Ok((active_match, payload))
}

#[tokio::test]
async fn concurrent_atomic_claims_commit_exactly_one_match() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    setup_test_redis().await?;
    seed_lobby_metadata(&["ATOMIC1", "ATOMIC2"]).await?;

    let mut left = create_test_matchmaking_manager().await?;
    let mut right = create_test_matchmaking_manager().await?;
    let game_type = GameType::FreeForAll { max_players: 2 };
    let queue_mode = QueueMode::Quickmatch;

    for (code, user_id) in [("ATOMIC1", 101_u32), ("ATOMIC2", 102_u32)] {
        left.add_lobby_to_queue(
            code,
            vec![make_lobby_member(user_id, format!("player-{user_id}"))],
            1_000,
            vec![game_type.clone()],
            queue_mode.clone(),
            user_id,
        )
        .await?;
    }
    let lobbies = left.get_queued_lobbies(&game_type, &queue_mode).await?;
    assert_eq!(lobbies.len(), 2);
    assert!(lobbies.iter().all(|lobby| !lobby.queue_token.is_empty()));

    let pubsub_client = Client::open(test_redis_url())?;
    let mut pubsub = pubsub_client.get_async_pubsub().await?;
    for lobby in &lobbies {
        pubsub
            .subscribe(RedisKeys::matchmaking_lobby_notification_channel(
                &lobby.lobby_code,
            ))
            .await?;
    }
    let mut notification_stream = pubsub.on_message();

    let mut redis = Client::open(test_redis_url())?
        .get_multiplexed_async_connection()
        .await?;
    for user_id in [101_u32, 102_u32] {
        let _: usize = redis
            .hset(
                RedisKeys::matchmaking_user_status(user_id),
                "status",
                "queued",
            )
            .await?;
    }

    let left_game_id = 1_000_000_001;
    let right_game_id = 1_000_000_002;
    let (left_match, left_payload) = committed_match_fixture(left_game_id, &[101, 102])?;
    let (right_match, right_payload) = committed_match_fixture(right_game_id, &[101, 102])?;
    let left_lobbies = lobbies.clone();
    let right_lobbies = lobbies.clone();

    let (left_result, right_result) = tokio::join!(
        left.commit_match(
            left_game_id,
            left_game_id % server::game_executor::PARTITION_COUNT,
            &game_type,
            &queue_mode,
            &left_match,
            &left_payload,
            &left_lobbies,
        ),
        right.commit_match(
            right_game_id,
            right_game_id % server::game_executor::PARTITION_COUNT,
            &game_type,
            &queue_mode,
            &right_match,
            &right_payload,
            &right_lobbies,
        )
    );

    let left_result = left_result?;
    let right_result = right_result?;
    let (winner_id, winner_payload, loser_id) = match (&left_result, &right_result) {
        (MatchCommitOutcome::Committed { .. }, MatchCommitOutcome::Conflict { .. }) => {
            (left_game_id, left_payload.clone(), right_game_id)
        }
        (MatchCommitOutcome::Conflict { .. }, MatchCommitOutcome::Committed { .. }) => {
            (right_game_id, right_payload.clone(), left_game_id)
        }
        outcomes => panic!("expected one commit and one conflict, got {outcomes:?}"),
    };
    // A caller can disappear after the script committed but before it
    // observed the response. A fresh task retries the identical claim.
    let mut retrying_caller = create_test_matchmaking_manager().await?;
    let repeated = if winner_id == left_game_id {
        retrying_caller
            .commit_match(
                left_game_id,
                left_game_id % server::game_executor::PARTITION_COUNT,
                &game_type,
                &queue_mode,
                &left_match,
                &left_payload,
                &left_lobbies,
            )
            .await?
    } else {
        retrying_caller
            .commit_match(
                right_game_id,
                right_game_id % server::game_executor::PARTITION_COUNT,
                &game_type,
                &queue_mode,
                &right_match,
                &right_payload,
                &right_lobbies,
            )
            .await?
    };
    assert_eq!(repeated, MatchCommitOutcome::AlreadyCommitted);

    let mut notified_channels = Vec::new();
    for _ in 0..lobbies.len() {
        let notification = timeout(Duration::from_secs(1), notification_stream.next())
            .await?
            .expect("commit should publish one notification per lobby");
        notified_channels.push(notification.get_channel_name().to_string());
        let payload_json: String = notification.get_payload()?;
        let payload: serde_json::Value = serde_json::from_str(&payload_json)?;
        assert_eq!(payload["type"], "MatchFound");
        assert_eq!(payload["game_id"], winner_id);
        assert_eq!(
            payload["partition_id"],
            winner_id % server::game_executor::PARTITION_COUNT
        );
    }
    notified_channels.sort();
    let mut expected_channels: Vec<String> = lobbies
        .iter()
        .map(|lobby| RedisKeys::matchmaking_lobby_notification_channel(&lobby.lobby_code))
        .collect();
    expected_channels.sort();
    assert_eq!(notified_channels, expected_channels);
    assert!(
        timeout(Duration::from_millis(100), notification_stream.next())
            .await
            .is_err(),
        "idempotent commit retry must not duplicate MatchFound"
    );
    assert_eq!(left.get_user_active_game(101).await?, Some(winner_id));
    assert_eq!(left.get_user_active_game(102).await?, Some(winner_id));
    assert_eq!(
        left.get_lobby_active_game("ATOMIC1").await?,
        Some(winner_id)
    );
    assert_eq!(
        left.get_lobby_active_game("ATOMIC2").await?,
        Some(winner_id)
    );

    let active_count: usize = redis.hlen(RedisKeys::matchmaking_active_matches()).await?;
    assert_eq!(active_count, 1);
    let winner: Option<String> = redis
        .hget(
            RedisKeys::matchmaking_active_matches(),
            winner_id.to_string(),
        )
        .await?;
    let loser: Option<String> = redis
        .hget(
            RedisKeys::matchmaking_active_matches(),
            loser_id.to_string(),
        )
        .await?;
    assert!(winner.is_some());
    assert!(loser.is_none());

    for user_id in [101_u32, 102_u32] {
        let mapped_game: String = redis
            .get(RedisKeys::matchmaking_user_active_game(user_id))
            .await?;
        assert_eq!(mapped_game, winner_id.to_string());
        let queue_status_exists: bool = redis
            .exists(RedisKeys::matchmaking_user_status(user_id))
            .await?;
        assert!(!queue_status_exists);
        let queue_identity_exists: bool = redis
            .exists(RedisKeys::matchmaking_user_queue_identity(user_id))
            .await?;
        assert!(!queue_identity_exists);
    }
    for lobby in &lobbies {
        let mapped_game: String = redis
            .get(RedisKeys::matchmaking_lobby_active_game(&lobby.lobby_code))
            .await?;
        assert_eq!(mapped_game, winner_id.to_string());
        let queue_identity_exists: bool = redis
            .exists(RedisKeys::matchmaking_lobby_queue_identity(
                &lobby.lobby_code,
            ))
            .await?;
        assert!(!queue_identity_exists);
        assert_eq!(
            lobby_state(&lobby.lobby_code).await?.as_deref(),
            Some("matched")
        );
    }

    let queue_len: usize = redis
        .zcard(RedisKeys::matchmaking_lobby_queue(&game_type, &queue_mode))
        .await?;
    let mmr_len: usize = redis
        .zcard(RedisKeys::matchmaking_lobby_mmr_index(
            &game_type,
            &queue_mode,
        ))
        .await?;
    assert_eq!((queue_len, mmr_len), (0, 0));

    let (_cursor, mut outbox) = left.scan_game_created_outbox(0).await?;
    assert_eq!(outbox.len(), 1);
    let (outbox_game_id, outbox_payload) = outbox.pop().expect("one committed outbox record");
    assert_eq!(outbox_game_id, winner_id.to_string());
    let outbox_record: GameCreatedOutboxRecord = serde_json::from_str(&outbox_payload)?;
    let redis_client = Client::open(test_redis_url())?;
    let (pubsub_tx, _rx) = broadcast::channel::<PushInfo>(128);
    let conn = create_connection_manager(redis_client.clone(), pubsub_tx).await?;
    let game_bus = GameBus::new(
        conn.clone(),
        (0..server::game_executor::PARTITION_COUNT)
            .map(|_| conn.clone().into())
            .collect(),
        conn.clone(),
        conn,
        redis_client,
        CancellationToken::new(),
    )?;
    let first_delivery = game_bus.publish_game_created_once(&outbox_record).await?;
    let retry_delivery = game_bus.publish_game_created_once(&outbox_record).await?;
    assert_eq!(first_delivery, retry_delivery);
    assert!(
        left.acknowledge_game_created_outbox(winner_id, "different-payload")
            .await
            .expect_err("a changed outbox payload must not expire the delivery fence")
            .to_string()
            .contains("payload changed")
    );
    assert!(
        left.acknowledge_game_created_outbox(winner_id, &outbox_payload)
            .await?
    );
    game_bus
        .expire_game_created_delivery_marker(winner_id)
        .await?;

    let winner_partition = winner_id % server::game_executor::PARTITION_COUNT;
    let loser_partition = loser_id % server::game_executor::PARTITION_COUNT;
    let winner_entries: redis::streams::StreamRangeReply = redis
        .xrange_all(RedisKeys::stream_commands(winner_partition))
        .await?;
    assert_eq!(winner_entries.ids.len(), 1);
    let stored_payload: String = redis::from_redis_value(
        winner_entries.ids[0]
            .map
            .get("data")
            .expect("GameCreated stream entry has data"),
    )?;
    assert_eq!(stored_payload, winner_payload);
    let StreamEvent::GameCreated { game_id, .. } = serde_json::from_str(&stored_payload)? else {
        panic!("atomic commit did not append GameCreated");
    };
    assert_eq!(game_id, winner_id);
    let loser_stream_len: usize = redis
        .xlen(RedisKeys::stream_commands(loser_partition))
        .await?;
    assert_eq!(loser_stream_len, 0);

    Ok(())
}

#[tokio::test]
async fn atomic_claim_conflict_writes_nothing() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    setup_test_redis().await?;
    seed_lobby_metadata(&["BLOCK1", "BLOCK2"]).await?;

    let mut manager = create_test_matchmaking_manager().await?;
    let game_type = GameType::FreeForAll { max_players: 2 };
    let queue_mode = QueueMode::Quickmatch;
    for (code, user_id) in [("BLOCK1", 201_u32), ("BLOCK2", 202_u32)] {
        manager
            .add_lobby_to_queue(
                code,
                vec![make_lobby_member(user_id, format!("player-{user_id}"))],
                1_000,
                vec![game_type.clone()],
                queue_mode.clone(),
                user_id,
            )
            .await?;
    }
    let lobbies = manager.get_queued_lobbies(&game_type, &queue_mode).await?;

    let mut redis = Client::open(test_redis_url())?
        .get_multiplexed_async_connection()
        .await?;
    for user_id in [201_u32, 202_u32] {
        let _: usize = redis
            .hset(
                RedisKeys::matchmaking_user_status(user_id),
                "status",
                "queued",
            )
            .await?;
    }
    let _: () = redis
        .set(
            RedisKeys::matchmaking_user_active_game(202),
            "existing-game",
        )
        .await?;

    let game_id = 1_000_000_011;
    let partition = game_id % server::game_executor::PARTITION_COUNT;
    let (active_match, payload) = committed_match_fixture(game_id, &[201, 202])?;
    let outcome = manager
        .commit_match(
            game_id,
            partition,
            &game_type,
            &queue_mode,
            &active_match,
            &payload,
            &lobbies,
        )
        .await?;
    assert!(matches!(outcome, MatchCommitOutcome::Conflict { .. }));
    assert_eq!(manager.get_user_active_game(201).await?, None);
    assert!(manager.get_user_active_game(202).await.is_err());
    assert_eq!(manager.get_lobby_active_game("BLOCK1").await?, None);

    let queue_len: usize = redis
        .zcard(RedisKeys::matchmaking_lobby_queue(&game_type, &queue_mode))
        .await?;
    let mmr_len: usize = redis
        .zcard(RedisKeys::matchmaking_lobby_mmr_index(
            &game_type,
            &queue_mode,
        ))
        .await?;
    assert_eq!((queue_len, mmr_len), (2, 2));
    let active_count: usize = redis.hlen(RedisKeys::matchmaking_active_matches()).await?;
    assert_eq!(active_count, 0);
    let stream_len: usize = redis.xlen(RedisKeys::stream_commands(partition)).await?;
    assert_eq!(stream_len, 0);
    for lobby in &lobbies {
        let mapping: Option<String> = redis
            .get(RedisKeys::matchmaking_lobby_active_game(&lobby.lobby_code))
            .await?;
        assert!(mapping.is_none());
        let queue_identity_exists: bool = redis
            .exists(RedisKeys::matchmaking_lobby_queue_identity(
                &lobby.lobby_code,
            ))
            .await?;
        assert!(queue_identity_exists);
        assert_eq!(
            lobby_state(&lobby.lobby_code).await?.as_deref(),
            Some("queued")
        );
    }
    let first_user_mapping: Option<String> = redis
        .get(RedisKeys::matchmaking_user_active_game(201))
        .await?;
    assert!(first_user_mapping.is_none());
    let existing_mapping: String = redis
        .get(RedisKeys::matchmaking_user_active_game(202))
        .await?;
    assert_eq!(existing_mapping, "existing-game");
    for user_id in [201_u32, 202_u32] {
        let status_exists: bool = redis
            .exists(RedisKeys::matchmaking_user_status(user_id))
            .await?;
        assert!(status_exists);
        let queue_identity_exists: bool = redis
            .exists(RedisKeys::matchmaking_user_queue_identity(user_id))
            .await?;
        assert!(queue_identity_exists);
    }

    Ok(())
}

#[tokio::test]
async fn retired_socket_cleanup_preserves_replacement_lobby_and_active_mapping() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("retired_socket_cleanup").await?;
    env.add_server().await?;
    let user_id = env.create_user().await?;
    let server_addr = env.ws_addr(0).expect("server should exist");

    let mut old_socket = TestClient::connect(&server_addr).await?;
    old_socket.authenticate(user_id).await?;
    old_socket.send_message(WSMessage::CreateLobby).await?;
    let lobby_code = timeout(Duration::from_secs(5), async {
        loop {
            if let WSMessage::LobbyCreated { lobby_code } = old_socket.receive_message().await? {
                return Ok::<String, anyhow::Error>(lobby_code);
            }
        }
    })
    .await??;

    let mut redis = Client::open(test_redis_url())?
        .get_multiplexed_async_connection()
        .await?;
    let members_key = RedisKeys::lobby_members_set(&lobby_code);
    let old_members: Vec<String> = redis.zrange(&members_key, 0, -1).await?;
    assert_eq!(old_members.len(), 1);
    let old_transport = old_members[0].clone();

    // Restore the same lobby through a replacement transport before retiring
    // the old one, as the planned make-before-break client does.
    let mut replacement = TestClient::connect(&server_addr).await?;
    replacement.authenticate(user_id).await?;
    replacement
        .send_message(WSMessage::JoinLobby {
            lobby_code: lobby_code.clone(),
            preferences: None,
        })
        .await?;
    timeout(Duration::from_secs(5), async {
        loop {
            if let WSMessage::JoinedLobby {
                lobby_code: joined_code,
            } = replacement.receive_message().await?
            {
                assert_eq!(joined_code, lobby_code);
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;

    let overlapping_members: Vec<String> = redis.zrange(&members_key, 0, -1).await?;
    assert_eq!(overlapping_members.len(), 2);
    let replacement_transport = overlapping_members
        .iter()
        .find(|member| **member != old_transport)
        .expect("replacement websocket must have its own presence generation")
        .clone();

    // Durable active context is independent of either transport generation.
    let game_id = 42_101_u32;
    redis
        .set::<_, _, ()>(
            RedisKeys::matchmaking_user_active_game(user_id as u32),
            game_id,
        )
        .await?;
    redis
        .set::<_, _, ()>(
            RedisKeys::matchmaking_lobby_active_game(&lobby_code),
            game_id,
        )
        .await?;

    old_socket.disconnect().await?;
    // Give the server's close branch enough time to execute. The retired
    // transport lease may remain briefly, but it must not compare-delete the
    // replacement generation or durable context.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let members_after_close: Vec<String> = redis.zrange(&members_key, 0, -1).await?;
    assert!(members_after_close.contains(&replacement_transport));
    assert!(
        redis
            .exists::<_, bool>(RedisKeys::lobby_metadata(&lobby_code))
            .await?
    );
    assert_eq!(
        redis
            .get::<_, Option<u32>>(RedisKeys::matchmaking_user_active_game(user_id as u32))
            .await?,
        Some(game_id)
    );
    assert_eq!(
        redis
            .get::<_, Option<u32>>(RedisKeys::matchmaking_lobby_active_game(&lobby_code))
            .await?,
        Some(game_id)
    );

    replacement.send_ping().await?;
    timeout(Duration::from_secs(2), async {
        loop {
            if matches!(replacement.receive_message().await?, WSMessage::Pong { .. }) {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;

    replacement.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

async fn await_lobby_roster_without_forbidden_member(
    client: &mut TestClient,
    lobby_code: &str,
    expected_user_ids: &[u32],
    forbidden_user_id: u32,
) -> Result<()> {
    loop {
        let WSMessage::LobbyUpdate {
            lobby_code: update_lobby_code,
            members,
            ..
        } = client.receive_message().await?
        else {
            continue;
        };
        if update_lobby_code != lobby_code {
            continue;
        }

        let mut user_ids: Vec<_> = members.into_iter().map(|member| member.user_id).collect();
        user_ids.sort_unstable();
        if user_ids.contains(&forbidden_user_id) {
            anyhow::bail!(
                "stale Pub/Sub member {forbidden_user_id} was forwarded to lobby {lobby_code}"
            );
        }
        if user_ids == expected_user_ids {
            return Ok(());
        }
    }
}

async fn fail_on_lobby_update(client: &mut TestClient, lobby_code: &str) -> Result<()> {
    loop {
        if let WSMessage::LobbyUpdate {
            lobby_code: update_lobby_code,
            ..
        } = client.receive_message().await?
            && update_lobby_code == lobby_code
        {
            anyhow::bail!("unexpected lobby update for {lobby_code}");
        }
    }
}

async fn assert_no_lobby_updates(
    first: &mut TestClient,
    second: &mut TestClient,
    lobby_code: &str,
    duration: Duration,
) -> Result<()> {
    match timeout(duration, async {
        tokio::try_join!(
            fail_on_lobby_update(first, lobby_code),
            fail_on_lobby_update(second, lobby_code),
        )
    })
    .await
    {
        Err(_) => Ok(()),
        Ok(Err(error)) => Err(error),
        Ok(Ok(_)) => unreachable!("lobby update sentinels never return successfully"),
    }
}

async fn await_lobby_switch_denial(client: &mut TestClient, lobby_code: &str) -> Result<()> {
    loop {
        match client.receive_message().await? {
            WSMessage::AccessDenied { reason } if reason.contains("Leave") => return Ok(()),
            WSMessage::LobbyCreated { .. } | WSMessage::JoinedLobby { .. } => {
                anyhow::bail!("direct lobby switch unexpectedly succeeded");
            }
            WSMessage::LobbyUpdate {
                lobby_code: update_lobby_code,
                ..
            }
            | WSMessage::LobbyChatHistory {
                lobby_code: update_lobby_code,
                ..
            } if update_lobby_code == lobby_code => {
                anyhow::bail!("lobby scope {lobby_code} restarted before switch denial");
            }
            _ => {}
        }
    }
}

async fn fail_on_lobby_scope_replay(client: &mut TestClient, lobby_code: &str) -> Result<()> {
    loop {
        match client.receive_message().await? {
            WSMessage::LobbyUpdate {
                lobby_code: update_lobby_code,
                ..
            }
            | WSMessage::LobbyChatHistory {
                lobby_code: update_lobby_code,
                ..
            } if update_lobby_code == lobby_code => {
                anyhow::bail!("lobby scope {lobby_code} was restarted after a denied switch");
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn lobby_sockets_reconcile_durable_roster_and_ignore_stale_pubsub_payload() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("lobby_roster_read_repair").await?;
    env.add_server().await?;
    let host_user_id = env.create_user().await?;
    let follower_user_id = env.create_user().await?;
    let durable_only_user_id = env.create_user().await?;
    let stale_only_user_id = env.create_user().await?;
    let server_addr = env.ws_addr(0).expect("server should exist");

    let mut host = TestClient::connect(&server_addr).await?;
    host.authenticate(host_user_id).await?;
    host.send_message(WSMessage::CreateLobby).await?;
    let lobby_code = timeout(Duration::from_secs(5), async {
        loop {
            if let WSMessage::LobbyCreated { lobby_code } = host.receive_message().await? {
                return Ok::<String, anyhow::Error>(lobby_code);
            }
        }
    })
    .await??;

    let mut follower = TestClient::connect(&server_addr).await?;
    follower.authenticate(follower_user_id).await?;
    follower
        .send_message(WSMessage::JoinLobby {
            lobby_code: lobby_code.clone(),
            preferences: None,
        })
        .await?;
    timeout(Duration::from_secs(5), async {
        loop {
            if let WSMessage::JoinedLobby {
                lobby_code: joined_code,
            } = follower.receive_message().await?
                && joined_code == lobby_code
            {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;

    let mut baseline = [host_user_id as u32, follower_user_id as u32];
    baseline.sort_unstable();
    timeout(Duration::from_secs(3), async {
        tokio::try_join!(
            await_lobby_roster_without_forbidden_member(
                &mut host,
                &lobby_code,
                &baseline,
                stale_only_user_id as u32,
            ),
            await_lobby_roster_without_forbidden_member(
                &mut follower,
                &lobby_code,
                &baseline,
                stale_only_user_id as u32,
            ),
        )
    })
    .await??;

    // Phase 1: mutate the authoritative roster without publishing anything.
    // Both sockets must converge from the periodic durable read alone.
    let mut redis = Client::open(test_redis_url())?
        .get_multiplexed_async_connection()
        .await?;
    let expires_at = Utc::now().timestamp_millis() + 60_000;
    redis
        .zadd::<_, _, _, ()>(
            RedisKeys::lobby_members_set(&lobby_code),
            format!("{durable_only_user_id}:synthetic-transport"),
            expires_at,
        )
        .await?;

    let mut expected = [
        host_user_id as u32,
        follower_user_id as u32,
        durable_only_user_id as u32,
    ];
    expected.sort_unstable();
    timeout(Duration::from_secs(3), async {
        tokio::try_join!(
            await_lobby_roster_without_forbidden_member(
                &mut host,
                &lobby_code,
                &expected,
                stale_only_user_id as u32,
            ),
            await_lobby_roster_without_forbidden_member(
                &mut follower,
                &lobby_code,
                &expected,
                stale_only_user_id as u32,
            ),
        )
    })
    .await??;

    // Phase 2: inject a stale roster through the real Redis Pub/Sub path. Its
    // removed sentinel member must never reach either socket; the durable read
    // resolves to the already-sent logical roster and emits nothing.
    let stale_lobby = Lobby {
        lobby_code: lobby_code.clone(),
        members: BTreeMap::from([
            (
                host_user_id as u32,
                make_lobby_member(host_user_id as u32, "stale-host"),
            ),
            (
                follower_user_id as u32,
                make_lobby_member(follower_user_id as u32, "stale-follower"),
            ),
            (
                stale_only_user_id as u32,
                make_lobby_member(stale_only_user_id as u32, "removed-member"),
            ),
        ]),
        host_user_id,
        state: "waiting".to_owned(),
        preferences: LobbyPreferences::default(),
    };
    let stale_hint = serde_json::json!({
        "LobbyUpdate": {
            "lobby": stale_lobby,
        }
    })
    .to_string();
    let subscribers: i64 = redis
        .publish(RedisKeys::lobby_updates_channel(), &stale_hint)
        .await?;
    assert!(
        subscribers > 0,
        "stale hint must traverse the server's real Pub/Sub subscription"
    );
    assert_no_lobby_updates(
        &mut host,
        &mut follower,
        &lobby_code,
        Duration::from_secs(2),
    )
    .await?;

    // Phase 3: wait longer than the ten-second presence-heartbeat cadence.
    // Scores change, but the client-visible roster does not, so no update is
    // resent by either periodic reconciliation loop.
    assert_no_lobby_updates(
        &mut host,
        &mut follower,
        &lobby_code,
        Duration::from_secs(11),
    )
    .await?;

    follower.disconnect().await?;
    host.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn direct_lobby_switch_is_denied_without_restarting_current_scope() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    let mut env = TestEnvironment::new("lobby_switch_denial").await?;
    env.add_server().await?;
    let user_id = env.create_user().await?;
    let server_addr = env.ws_addr(0).expect("server should exist");

    let mut client = TestClient::connect(&server_addr).await?;
    client.authenticate(user_id).await?;
    client.send_message(WSMessage::CreateLobby).await?;
    let lobby_code = timeout(Duration::from_secs(5), async {
        loop {
            if let WSMessage::LobbyCreated { lobby_code } = client.receive_message().await? {
                return Ok::<String, anyhow::Error>(lobby_code);
            }
        }
    })
    .await??;
    timeout(
        Duration::from_secs(3),
        await_lobby_roster_without_forbidden_member(
            &mut client,
            &lobby_code,
            &[user_id as u32],
            u32::MAX,
        ),
    )
    .await??;

    // Seed chat history and consume its live delivery. If denial accidentally
    // rebuilds the lobby scope, history loading would replay this message.
    client
        .send_message(WSMessage::Chat("before-denial".to_owned()))
        .await?;
    timeout(Duration::from_secs(3), async {
        loop {
            if let WSMessage::LobbyChatMessage {
                lobby_code: message_lobby_code,
                message,
                ..
            } = client.receive_message().await?
                && message_lobby_code == lobby_code
                && message == "before-denial"
            {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;

    client.send_message(WSMessage::CreateLobby).await?;
    timeout(
        Duration::from_secs(3),
        await_lobby_switch_denial(&mut client, &lobby_code),
    )
    .await??;
    client
        .send_message(WSMessage::JoinLobby {
            lobby_code: "USE1-DENIED".to_owned(),
            preferences: None,
        })
        .await?;
    timeout(
        Duration::from_secs(3),
        await_lobby_switch_denial(&mut client, &lobby_code),
    )
    .await??;

    match timeout(
        Duration::from_millis(1500),
        fail_on_lobby_scope_replay(&mut client, &lobby_code),
    )
    .await
    {
        Err(_) => {}
        Ok(Err(error)) => return Err(error),
        Ok(Ok(_)) => unreachable!("lobby scope replay sentinel never returns successfully"),
    }

    client
        .send_message(WSMessage::Chat("after-denial".to_owned()))
        .await?;
    timeout(Duration::from_secs(3), async {
        loop {
            if let WSMessage::LobbyChatMessage {
                lobby_code: message_lobby_code,
                message,
                ..
            } = client.receive_message().await?
                && message_lobby_code == lobby_code
                && message == "after-denial"
            {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;

    client.disconnect().await?;
    env.shutdown().await?;
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
            if let WSMessage::LobbyCreated { lobby_code } = clients[0].receive_message().await? {
                return Ok::<String, anyhow::Error>(lobby_code);
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
                    if let WSMessage::JoinedLobby { .. } = client.receive_message().await? {
                        return Ok::<(), anyhow::Error>(());
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
            if let WSMessage::JoinGame(id) = client.receive_message().await? {
                break id;
            }
        };

        client.send_message(WSMessage::JoinGame(game_id)).await?;

        // Wait for snapshot to confirm game stream started
        loop {
            if let WSMessage::GameEvent(event) = client.receive_message().await?
                && matches!(event.event, GameEvent::Snapshot { .. })
            {
                return Ok::<u32, anyhow::Error>(game_id);
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
                if let WSMessage::JoinGame(id) = client.receive_message().await? {
                    break id;
                }
            };

            // Send JoinGame acknowledgment
            client.send_message(WSMessage::JoinGame(gid)).await?;

            // Wait for snapshot
            loop {
                if let WSMessage::GameEvent(event) = client.receive_message().await?
                    && matches!(event.event, GameEvent::Snapshot { .. })
                {
                    return Ok::<u32, anyhow::Error>(event.game_id);
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
#[allow(dead_code)] // test helper retained for future team assertions
async fn get_player_team(_client: &mut TestClient, _user_id: u32) -> Result<Option<TeamId>> {
    // The client should have already received the snapshot
    // We'll peek at the last received snapshot in memory
    // For now, we'll send a simple query and parse the response

    // This is a simplified version - in reality you'd need to track the game state
    // For testing purposes, we can infer team from snake positions
    Ok(None) // Placeholder - will be filled based on actual game state
}

// Helper to age a single queued lobby in Redis
async fn age_single_queued_lobby(
    game_type: &GameType,
    queue_mode: &QueueMode,
    new_queued_at: i64,
) -> Result<()> {
    let mut mm = create_test_matchmaking_manager().await?;
    let mut redis_conn = redis::Client::open(test_redis_url())?
        .get_multiplexed_async_connection()
        .await?;

    let mut lobbies = Vec::new();
    for _ in 0..10 {
        lobbies = mm
            .get_queued_lobbies(game_type, queue_mode)
            .await
            .expect("Should fetch queued lobbies");
        if lobbies.len() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(
        lobbies.len(),
        1,
        "Expected a single queued lobby after waiting for queue processing"
    );

    let lobby = lobbies[0].clone();
    let original_json = serde_json::to_string(&lobby)?;

    let mut aged_lobby = lobby.clone();
    aged_lobby.queued_at = new_queued_at;
    let updated_json = serde_json::to_string(&aged_lobby)?;

    let queue_key = RedisKeys::matchmaking_lobby_queue(game_type, queue_mode);
    let mmr_key = RedisKeys::matchmaking_lobby_mmr_index(game_type, queue_mode);
    let identity_key = RedisKeys::matchmaking_lobby_queue_identity(&lobby.lobby_code);

    let mut pipe = redis::pipe();
    pipe.atomic()
        .zrem(&queue_key, &original_json)
        .zrem(&mmr_key, &original_json)
        .zadd(&queue_key, &updated_json, new_queued_at)
        .zadd(&mmr_key, &updated_json, aged_lobby.avg_mmr)
        .set(&identity_key, &updated_json);
    let _: () = pipe.query_async(&mut redis_conn).await?;

    Ok(())
}

// ============================================================================
// 1V1 TESTS
// ============================================================================

#[tokio::test]
async fn test_multi_member_lobby_queues_solo_host_only_player() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
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
            if let WSMessage::JoinGame(id) = spectator_client.receive_message().await? {
                spectator_client
                    .send_message(WSMessage::JoinGame(id))
                    .await?;
                return Ok::<u32, anyhow::Error>(id);
            }
        }
    })
    .await??;

    assert_eq!(
        game_id, spectator_game_id,
        "Lobby members should be directed to the same solo game"
    );

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
    assert_eq!(
        active_match.spectators.len(),
        1,
        "Non-host lobby members should be recorded as spectators"
    );
    assert_eq!(
        active_match.spectators[0].user_id,
        env.user_ids()[1] as u32,
        "Non-host should be recorded as spectator"
    );

    host_client.disconnect().await?;
    spectator_client.disconnect().await?;
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_two_player_lobby_creates_1v1_with_split_teams() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
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
    let _guard = TEST_LOCK.lock().await;
    let _ = tracing_subscriber::fmt::try_init();
    setup_test_redis().await?;
    let mut env = TestEnvironment::new("test_two_single_lobbies_1v1").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;

    // Create two separate single-player lobbies
    let clients1 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0]],
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Quickmatch,
    )
    .await?;

    let clients2 = create_lobby_and_queue(
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
    let _guard = TEST_LOCK.lock().await;
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
            if let WSMessage::JoinGame(_) = clients[0].receive_message().await? {
                return Ok::<(), anyhow::Error>(());
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
    let _guard = TEST_LOCK.lock().await;
    setup_test_redis().await?;
    let mut env = TestEnvironment::new("test_two_player_lobbies_2v2").await?;
    env.add_server().await?;
    for _ in 0..4 {
        env.create_user().await?;
    }

    // Create two 2-player lobbies
    let clients1 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0], env.user_ids()[1]],
        GameType::TeamMatch { per_team: 2 },
        QueueMode::Quickmatch,
    )
    .await?;

    let clients2 = create_lobby_and_queue(
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
    let _guard = TEST_LOCK.lock().await;
    setup_test_redis().await?;
    let mut env = TestEnvironment::new("test_three_plus_one_2v2").await?;
    env.add_server().await?;
    for _ in 0..4 {
        env.create_user().await?;
    }

    // Create 3-player lobby
    let clients1 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0], env.user_ids()[1], env.user_ids()[2]],
        GameType::TeamMatch { per_team: 2 },
        QueueMode::Quickmatch,
    )
    .await?;

    // Create 1-player lobby
    let clients2 = create_lobby_and_queue(
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
    let _guard = TEST_LOCK.lock().await;
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
    let _guard = TEST_LOCK.lock().await;
    setup_test_redis().await?;
    let mut env = TestEnvironment::new("test_ffa_multiple_lobbies").await?;
    env.add_server().await?;
    for _ in 0..4 {
        env.create_user().await?;
    }

    // Create lobbies with different sizes
    let clients1 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0], env.user_ids()[1]],
        GameType::FreeForAll { max_players: 4 },
        QueueMode::Quickmatch,
    )
    .await?;

    let clients2 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[2]],
        GameType::FreeForAll { max_players: 4 },
        QueueMode::Quickmatch,
    )
    .await?;

    let clients3 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[3]],
        GameType::FreeForAll { max_players: 4 },
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
        "FFA game created from multiple lobbies: {} (4 total players)",
        game_id
    );

    for client in all_clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_ffa_single_lobby() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    init_tracing();

    setup_test_redis().await?;
    let mut env = TestEnvironment::new("test_ffa_single_lobby").await?;
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
        GameType::FreeForAll { max_players: 4 },
        QueueMode::Quickmatch,
    )
    .await?;

    // Wait for game to be created
    let game_id = wait_for_all_clients_to_join_game(&mut clients).await?;

    println!("FFA game created from single lobby: {}", game_id);

    for client in clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_ffa_minimum_players() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
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
            if let WSMessage::JoinGame(_) = clients[0].receive_message().await? {
                return Ok::<(), anyhow::Error>(());
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

#[tokio::test]
async fn test_ffa_two_player_lobby_matches_after_30_seconds() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    setup_test_redis().await?;
    let mut env =
        TestEnvironment::new("test_ffa_two_player_lobby_matches_after_30_seconds").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;

    let mut clients = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0], env.user_ids()[1]],
        GameType::FreeForAll { max_players: 4 },
        QueueMode::Quickmatch,
    )
    .await?;

    // Age the queued lobby so the longest wait time passes the 30s threshold for 2 players
    let game_type = GameType::FreeForAll { max_players: 4 };
    let queue_mode = QueueMode::Quickmatch;
    let aged_ts = Utc::now().timestamp_millis() - 31_000; // ~31s ago
    age_single_queued_lobby(&game_type, &queue_mode, aged_ts).await?;

    // Allow a short tick for the matchmaking loop to pick up the aged lobby
    tokio::time::sleep(Duration::from_millis(500)).await;

    let game_id = timeout(Duration::from_secs(10), async {
        wait_for_all_clients_to_join_game(&mut clients).await
    })
    .await??;

    println!(
        "FFA game created for two-player lobby after ~30s wait threshold: {}",
        game_id
    );

    for client in clients {
        client.disconnect().await?;
    }
    env.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_ffa_three_player_lobby_matches_after_15_seconds() -> Result<()> {
    let _guard = TEST_LOCK.lock().await;
    setup_test_redis().await?;
    let mut env =
        TestEnvironment::new("test_ffa_three_player_lobby_matches_after_15_seconds").await?;
    env.add_server().await?;
    for _ in 0..3 {
        env.create_user().await?;
    }

    let mut clients = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0], env.user_ids()[1], env.user_ids()[2]],
        GameType::FreeForAll { max_players: 4 },
        QueueMode::Quickmatch,
    )
    .await?;

    // Age the queued lobby so the longest wait time passes the 15s threshold for 3 players
    let game_type = GameType::FreeForAll { max_players: 4 };
    let queue_mode = QueueMode::Quickmatch;
    let aged_ts = Utc::now().timestamp_millis() - 16_000; // ~16s ago
    age_single_queued_lobby(&game_type, &queue_mode, aged_ts).await?;

    // Allow a short tick for the matchmaking loop to pick up the aged lobby
    tokio::time::sleep(Duration::from_millis(500)).await;

    let game_id = timeout(Duration::from_secs(10), async {
        wait_for_all_clients_to_join_game(&mut clients).await
    })
    .await??;

    println!(
        "FFA game created for three-player lobby after ~15s wait threshold: {}",
        game_id
    );

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
    let _guard = TEST_LOCK.lock().await;
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
            if let WSMessage::JoinGame(_) = clients1[0].receive_message().await? {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await;

    let result2 = timeout(Duration::from_secs(5), async {
        loop {
            if let WSMessage::JoinGame(_) = clients2[0].receive_message().await? {
                return Ok::<(), anyhow::Error>(());
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
    let _guard = TEST_LOCK.lock().await;
    setup_test_redis().await?;
    seed_lobby_metadata(&["TEST001"]).await?;
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
    let _guard = TEST_LOCK.lock().await;
    setup_test_redis().await?;
    seed_lobby_metadata(&["TEST001"]).await?;
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
    let _guard = TEST_LOCK.lock().await;
    setup_test_redis().await?;
    seed_lobby_metadata(&["TEST001", "TEST002"]).await?;
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
        vec![
            make_lobby_member(3, "player3"),
            make_lobby_member(4, "player4"),
        ],
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
    let _guard = TEST_LOCK.lock().await;
    setup_test_redis().await?;
    seed_lobby_metadata(&["TEST001", "TEST002", "TEST003"]).await?;

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
    let _guard = TEST_LOCK.lock().await;
    let _ = tracing_subscriber::fmt::try_init();
    setup_test_redis().await?;
    let mut env = TestEnvironment::new("test_multi_type_1v1_match").await?;
    env.add_server().await?;
    env.create_user().await?;
    env.create_user().await?;

    // For now, since WebSocket only supports single game type,
    // we'll queue two separate lobbies and they should match
    let clients1 = create_lobby_and_queue(
        &env,
        0,
        &[env.user_ids()[0]],
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Quickmatch,
    )
    .await?;

    let clients2 = create_lobby_and_queue(
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
    let _guard = TEST_LOCK.lock().await;
    setup_test_redis().await?;
    seed_lobby_metadata(&["TEST001", "TEST002", "TEST003"]).await?;
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
            i,
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
