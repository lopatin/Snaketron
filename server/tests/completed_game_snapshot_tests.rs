use anyhow::{Context, Result};
use common::{GameState, GameStatus, GameType, QueueMode};
use redis::AsyncCommands;
use server::{
    game_executor::PARTITION_COUNT, pubsub_manager::PubSubManager, redis_keys::RedisKeys,
    redis_utils::create_connection_manager,
};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

fn redis_test_url() -> String {
    let url = std::env::var("SNAKETRON_REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    if url.contains("protocol=") {
        url
    } else {
        let separator = if url.contains('?') { '&' } else { '?' };
        format!("{url}{separator}protocol=resp3")
    }
}

async fn test_pubsub_manager() -> Result<(PubSubManager, redis::aio::ConnectionManager)> {
    let client = redis::Client::open(redis_test_url())
        .context("Redis is required for this integration test; start it with ./test-deps.sh")?;
    let (push_tx, _) = broadcast::channel(32);
    let redis = create_connection_manager(client, push_tx.clone())
        .await
        .context("Redis is required for this integration test; start it with ./test-deps.sh")?;
    let inspection_connection = redis.clone();
    let manager = PubSubManager::new(redis, push_tx, CancellationToken::new());

    Ok((manager, inspection_connection))
}

fn unique_game_id() -> u32 {
    let id = uuid::Uuid::new_v4();
    let bytes = id.as_bytes();
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

fn completed_game_state() -> GameState {
    let mut state = GameState::new(
        20,
        20,
        GameType::Solo,
        QueueMode::Quickmatch,
        Some(12345),
        chrono::Utc::now().timestamp_millis(),
    );
    state.tick = 42;
    state.event_sequence = 99;
    state.status = GameStatus::Complete {
        winning_snake_id: Some(7),
    };
    state.scores.insert(7, 1200);
    state
}

#[tokio::test]
async fn completed_game_snapshot_round_trips_through_redis() -> Result<()> {
    let (mut manager, mut redis) = test_pubsub_manager().await?;
    let game_id = unique_game_id();
    let key = RedisKeys::game_snapshot(game_id);
    let expected = completed_game_state();

    manager
        .publish_snapshot(game_id % PARTITION_COUNT, game_id, &expected)
        .await?;

    let actual = manager
        .get_stored_snapshot(game_id)
        .await?
        .context("completed snapshot should be available for reload")?;
    let ttl_seconds: i64 = redis.ttl(&key).await?;

    assert_eq!(actual.status, expected.status);
    assert_eq!(actual.tick, expected.tick);
    assert_eq!(actual.event_sequence, expected.event_sequence);
    assert_eq!(actual.scores, expected.scores);
    assert!(
        (1..=300).contains(&ttl_seconds),
        "completed snapshot should have the configured five-minute TTL; got {ttl_seconds} seconds"
    );

    let _: usize = redis.del(&key).await?;
    Ok(())
}

#[tokio::test]
async fn terminal_snapshot_can_be_cached_before_completion_is_broadcast() -> Result<()> {
    let (manager, mut redis) = test_pubsub_manager().await?;
    let game_id = unique_game_id();
    let key = RedisKeys::game_snapshot(game_id);
    let expected = completed_game_state();

    manager.store_snapshot(game_id, &expected).await?;

    let mut reader = manager.clone();
    let actual = reader
        .get_stored_snapshot(game_id)
        .await?
        .context("terminal snapshot should be cached before eviction")?;
    assert_eq!(actual.status, expected.status);
    assert_eq!(actual.event_sequence, expected.event_sequence);

    let _: usize = redis.del(&key).await?;
    Ok(())
}

#[tokio::test]
async fn missing_game_snapshot_returns_none() -> Result<()> {
    let (mut manager, mut redis) = test_pubsub_manager().await?;
    let game_id = unique_game_id();
    let key = RedisKeys::game_snapshot(game_id);
    let _: usize = redis.del(&key).await?;

    assert!(manager.get_stored_snapshot(game_id).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn malformed_game_snapshot_returns_deserialization_error() -> Result<()> {
    let (mut manager, mut redis) = test_pubsub_manager().await?;
    let game_id = unique_game_id();
    let key = RedisKeys::game_snapshot(game_id);
    let _: () = redis.set_ex(&key, b"not-json".as_slice(), 30).await?;

    let error = manager
        .get_stored_snapshot(game_id)
        .await
        .expect_err("corrupt snapshots must not be treated as valid game state");
    assert!(
        error.to_string().contains("Failed to deserialize snapshot"),
        "unexpected error: {error:#}"
    );

    let _: usize = redis.del(&key).await?;
    Ok(())
}
