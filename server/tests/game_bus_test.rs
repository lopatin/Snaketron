//! Integration tests for the Streams game bus.
//!
//! Run against the test-deps Redis (test-deps.sh). Partition-scoped production
//! paths reject IDs outside the fixed executor range, so these tests serialize
//! their mutations of the ten regional stream keys.
//!
//! The headline test is `paused_consumer_loses_nothing`: the exact scenario —
//! a subscriber that stops draining for a while — where the old Pub/Sub
//! transport dropped messages (broadcast lag / at-most-once) and Streams
//! must not.

use anyhow::Result;
use common::{
    ClientCommandIdentityV2, CommandId, Direction, GameCommand, GameCommandMessage, GameEvent,
    GameEventMessage, GameState, GameStatus, GameType, QueueMode,
};
use server::cluster_membership::{BootIdentity, ClusterNamespace};
use server::completion::{COMPLETION_SCHEMA_VERSION, CompletionEffect, CompletionRecordV1};
use server::game_bus::GameBus;
use server::game_executor::{PARTITION_COUNT, StreamEvent};
use server::matchmaking_manager::{ActiveMatch, MatchStatus, QueuedPlayer};
use server::partition_lease::PartitionLeaseStore;
use server::recovery::{RecoveryEnvelopeV2, ResolvedCommandState};
use server::redis_keys::RedisKeys;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

// Dedicated logical DB for this binary: startup deletes the ten regional
// stream triplets so stale prior runs cannot affect exact-length assertions.
const REDIS_URL: &str = "redis://127.0.0.1:6379/11?protocol=resp3";
const TEST_EVENTS_MAXLEN: usize = 8192;
static STREAMS_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static TEST_NAMESPACE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Select one real executor partition; the suite-level lock isolates its
/// regional stream keys from the other tests in this file.
fn test_partition(salt: u32) -> u32 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    nanos.wrapping_add(salt) % PARTITION_COUNT
}

fn unique_namespace(label: &str) -> Result<ClusterNamespace> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let sequence = TEST_NAMESPACE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    ClusterNamespace::new(format!("{label}-{}-{nanos}-{sequence}", std::process::id()))
}

/// Integration tests intentionally seed fan-out events without owning an
/// executor lease. Keep that capability in test code: the production
/// `GameBus` exposes only fenced authoritative event/snapshot writes.
struct StreamsTestBus {
    bus: GameBus,
    publisher: redis::aio::ConnectionManager,
}

impl Deref for StreamsTestBus {
    type Target = GameBus;

    fn deref(&self) -> &Self::Target {
        &self.bus
    }
}

impl StreamsTestBus {
    async fn publish_event(&self, partition_id: u32, event: &GameEventMessage) -> Result<()> {
        let payload = serde_json::to_vec(event)?;
        let mut redis = self.publisher.clone();
        let _: String = redis::cmd("XADD")
            .arg(RedisKeys::stream_events(partition_id))
            .arg("MAXLEN")
            .arg("~")
            .arg(TEST_EVENTS_MAXLEN)
            .arg("*")
            .arg("data")
            .arg(payload)
            .query_async(&mut redis)
            .await?;
        Ok(())
    }
}

async fn streams_bus(token: CancellationToken) -> Result<StreamsTestBus> {
    let client = redis::Client::open(REDIS_URL)?;
    let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(64);
    let redis =
        server::redis_utils::create_connection_manager(client.clone(), pubsub_tx.clone()).await?;
    let mut cleanup_connection = redis.clone();
    let mut regional_streams = Vec::with_capacity(PARTITION_COUNT as usize * 3);
    for partition in 0..PARTITION_COUNT {
        regional_streams.push(RedisKeys::stream_events(partition));
        regional_streams.push(RedisKeys::stream_commands(partition));
        regional_streams.push(RedisKeys::stream_snapshot_requests(partition));
    }
    let _: () = redis::cmd("DEL")
        .arg(&regional_streams)
        .query_async(&mut cleanup_connection)
        .await?;
    Ok(StreamsTestBus {
        bus: GameBus::new(
            redis.clone(),
            (0..PARTITION_COUNT).map(|_| redis.clone().into()).collect(),
            redis.clone(),
            redis.clone(),
            client,
            token,
        )?,
        publisher: redis,
    })
}

#[tokio::test]
async fn one_expired_recovery_is_isolated_from_valid_games() -> Result<()> {
    let _test_lock = STREAMS_TEST_LOCK.lock().await;
    timeout(Duration::from_secs(10), async {
        use redis::AsyncCommands;

        let token = CancellationToken::new();
        let bus = streams_bus(token.clone()).await?;
        let partition = test_partition(30);
        let valid_game_id = partition;
        let expired_game_id = partition + PARTITION_COUNT;
        let namespace = unique_namespace("recovery-isolation")?;
        let boot_id = BootIdentity::new();
        let client = redis::Client::open(REDIS_URL)?;
        let mut redis = client.get_multiplexed_async_connection().await?;
        let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(8);
        let manager =
            server::redis_utils::create_connection_manager(client.clone(), pubsub_tx).await?;
        let mut owners = serde_json::Map::new();
        owners.insert(
            partition.to_string(),
            serde_json::Value::String(boot_id.to_string()),
        );
        let assignment = serde_json::json!({ "owners": owners });
        let _: () = redis
            .set(
                namespace.partition_assignment(partition),
                serde_json::to_vec(&assignment)?,
            )
            .await?;
        let lease_store = PartitionLeaseStore::new(
            manager,
            namespace.clone(),
            Duration::from_secs(10),
            Duration::from_millis(750),
        )?;
        let guard = lease_store
            .try_acquire(partition, &boot_id)
            .await?
            .expect("test partition lease acquired");

        let state = GameState::new(
            10,
            10,
            GameType::Solo,
            QueueMode::Quickmatch,
            Some(7),
            chrono::Utc::now().timestamp_millis(),
        );
        let envelope = RecoveryEnvelopeV2::new(
            valid_game_id,
            partition,
            state,
            "0-0".into(),
            ResolvedCommandState::default(),
            0,
            0,
            chrono::Utc::now().timestamp_millis(),
            guard.encoded_token(),
        );
        let _: () = redis
            .set(
                namespace.recovery(valid_game_id),
                serde_json::to_vec(&envelope)?,
            )
            .await?;
        let _: () = redis
            .sadd(
                namespace.active_games(partition),
                &[valid_game_id, expired_game_id],
            )
            .await?;

        let recovery_retention = Duration::from_secs(60);
        let recovered = bus
            .load_partition_recovery_fenced(&guard, recovery_retention)
            .await?;
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].game_id, valid_game_id);
        assert!(
            redis
                .sismember::<_, _, bool>(namespace.active_games(partition), valid_game_id)
                .await?
        );
        assert!(
            !redis
                .sismember::<_, _, bool>(namespace.active_games(partition), expired_game_id)
                .await?
        );
        let failure = bus
            .get_recovery_failure(&namespace, expired_game_id)
            .await?
            .expect("expired game has a durable terminal marker");
        assert_eq!(failure.partition_id, partition);
        let failure_ttl_ms: i64 = redis
            .pttl(namespace.recovery_failure(expired_game_id))
            .await?;
        assert!(
            failure_ttl_ms > 0 && failure_ttl_ms <= recovery_retention.as_millis() as i64,
            "recovery-failure marker must expire with recovery history, got {failure_ttl_ms}ms"
        );

        let _: () = redis
            .del(&[
                namespace.partition_assignment(partition),
                guard.lease_key(),
                namespace.recovery(valid_game_id),
                namespace.recovery_failure(expired_game_id),
                namespace.active_games(partition),
            ])
            .await?;
        token.cancel();
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn indexed_recovery_ignores_ten_thousand_unrelated_snapshots() -> Result<()> {
    let _test_lock = STREAMS_TEST_LOCK.lock().await;
    timeout(Duration::from_secs(30), async {
        use redis::AsyncCommands;

        const UNRELATED_COUNT: usize = 10_000;
        let token = CancellationToken::new();
        let bus = streams_bus(token.clone()).await?;
        let partition = test_partition(35);
        let game_ids = [partition, partition + PARTITION_COUNT];
        let namespace = unique_namespace("indexed-recovery")?;
        let boot_id = BootIdentity::new();
        let client = redis::Client::open(REDIS_URL)?;
        let mut redis = client.get_multiplexed_async_connection().await?;
        let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(8);
        let manager =
            server::redis_utils::create_connection_manager(client.clone(), pubsub_tx).await?;
        let mut owners = serde_json::Map::new();
        owners.insert(
            partition.to_string(),
            serde_json::Value::String(boot_id.to_string()),
        );
        let _: () = redis
            .set(
                namespace.partition_assignment(partition),
                serde_json::to_vec(&serde_json::json!({ "owners": owners }))?,
            )
            .await?;
        let lease_store = PartitionLeaseStore::new(
            manager,
            namespace.clone(),
            Duration::from_secs(30),
            Duration::from_millis(750),
        )?;
        let guard = lease_store
            .try_acquire(partition, &boot_id)
            .await?
            .expect("test partition lease acquired");

        for game_id in game_ids {
            let state = GameState::new(
                10,
                10,
                GameType::Solo,
                QueueMode::Quickmatch,
                Some(u64::from(game_id)),
                chrono::Utc::now().timestamp_millis(),
            );
            let envelope = RecoveryEnvelopeV2::new(
                game_id,
                partition,
                state,
                "0-0".into(),
                ResolvedCommandState::default(),
                0,
                0,
                chrono::Utc::now().timestamp_millis(),
                guard.encoded_token(),
            );
            let _: () = redis
                .set(namespace.recovery(game_id), serde_json::to_vec(&envelope)?)
                .await?;
        }
        let _: () = redis
            .sadd(namespace.active_games(partition), &game_ids)
            .await?;
        let baseline: Vec<u32> = bus
            .load_partition_recovery_fenced(&guard, Duration::from_secs(60))
            .await?
            .into_iter()
            .map(|envelope| envelope.game_id)
            .collect();
        assert_eq!(baseline, game_ids);

        let unrelated_keys: Vec<String> = (0..UNRELATED_COUNT)
            .map(|offset| RedisKeys::game_snapshot(1_800_000_000 + offset as u32))
            .collect();
        let mut writes = redis::pipe();
        for key in &unrelated_keys {
            writes.cmd("SET").arg(key).arg("unrelated").ignore();
        }
        writes.query_async::<()>(&mut redis).await?;

        let recovered: Vec<u32> = bus
            .load_partition_recovery_fenced(&guard, Duration::from_secs(60))
            .await?
            .into_iter()
            .map(|envelope| envelope.game_id)
            .collect();
        assert_eq!(recovered, baseline);
        assert_eq!(recovered.len(), game_ids.len());

        for keys in unrelated_keys.chunks(1_000) {
            let _: usize = redis::cmd("UNLINK")
                .arg(keys)
                .query_async(&mut redis)
                .await?;
        }
        let _: () = redis
            .del(&[
                namespace.partition_assignment(partition),
                guard.lease_key(),
                namespace.recovery(game_ids[0]),
                namespace.recovery(game_ids[1]),
                namespace.active_games(partition),
            ])
            .await?;
        token.cancel();
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn reacquisition_fences_old_token_events_checkpoints_and_acks() -> Result<()> {
    let _test_lock = STREAMS_TEST_LOCK.lock().await;
    timeout(Duration::from_secs(10), async {
        use redis::AsyncCommands;

        let token = CancellationToken::new();
        let bus = streams_bus(token.clone()).await?;
        let partition = 9;
        let game_id = 19;
        let namespace = unique_namespace("stale-token")?;
        let boot_id = BootIdentity::new();
        let client = redis::Client::open(REDIS_URL)?;
        let mut redis = client.get_multiplexed_async_connection().await?;
        let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(8);
        let manager =
            server::redis_utils::create_connection_manager(client.clone(), pubsub_tx).await?;
        let mut owners = serde_json::Map::new();
        owners.insert(
            partition.to_string(),
            serde_json::Value::String(boot_id.to_string()),
        );
        let _: () = redis
            .set(
                namespace.partition_assignment(partition),
                serde_json::to_vec(&serde_json::json!({ "owners": owners }))?,
            )
            .await?;
        let lease_store = PartitionLeaseStore::new(
            manager,
            namespace.clone(),
            Duration::from_secs(10),
            Duration::from_millis(750),
        )?;
        let old_guard = lease_store
            .try_acquire(partition, &boot_id)
            .await?
            .expect("old lease acquired");
        assert!(lease_store.release(&old_guard).await?);
        let new_guard = lease_store
            .try_acquire(partition, &boot_id)
            .await?
            .expect("new lease acquired");
        assert_ne!(old_guard.encoded_token(), new_guard.encoded_token());

        let state = GameState::new(
            10,
            10,
            GameType::Solo,
            QueueMode::Quickmatch,
            Some(7),
            chrono::Utc::now().timestamp_millis(),
        );
        let message = GameEventMessage {
            game_id,
            tick: 0,
            sequence: 0,
            stream_seq: 1,
            user_id: None,
            event: GameEvent::Snapshot {
                game_state: state.clone(),
            },
        };
        assert!(
            bus.publish_event_fenced(&old_guard, &message)
                .await
                .is_err()
        );

        let envelope = RecoveryEnvelopeV2::new(
            game_id,
            partition,
            state.clone(),
            "1-0".into(),
            ResolvedCommandState::default(),
            0,
            1,
            chrono::Utc::now().timestamp_millis(),
            old_guard.encoded_token(),
        );
        assert!(
            bus.checkpoint_and_ack_fenced(
                &old_guard,
                &envelope,
                &["1-0".into()],
                Duration::from_secs(60),
            )
            .await
            .is_err()
        );
        assert!(bus.xack_fenced(&old_guard, &["1-0".into()]).await.is_err());

        let _: () = redis
            .sadd(namespace.active_games(partition), game_id)
            .await?;
        assert!(
            bus.remove_active_game_fenced(&old_guard, game_id)
                .await
                .is_err()
        );
        assert!(
            redis
                .sismember::<_, _, bool>(namespace.active_games(partition), game_id)
                .await?
        );

        let now = chrono::Utc::now().timestamp_millis();
        let mut final_state = state;
        final_state.status = GameStatus::Complete {
            winning_snake_id: None,
        };
        let record = CompletionRecordV1 {
            schema_version: COMPLETION_SCHEMA_VERSION,
            game_id,
            partition_id: partition,
            revision: uuid::Uuid::new_v4(),
            ended_at_ms: now,
            server_id: 1,
            final_state: final_state.clone(),
            effects: vec![CompletionEffect::PersistGame { id: "game".into() }],
        };
        let final_envelope = RecoveryEnvelopeV2::new(
            game_id,
            partition,
            final_state,
            "1-0".into(),
            ResolvedCommandState::default(),
            0,
            2,
            now,
            old_guard.encoded_token(),
        );
        assert!(
            bus.commit_completion_record_fenced(
                &old_guard,
                &final_envelope,
                &[],
                &record,
                Duration::from_secs(60),
            )
            .await
            .is_err()
        );
        assert_eq!(
            redis
                .get::<_, Option<Vec<u8>>>(namespace.completion(game_id))
                .await?,
            None
        );
        assert!(
            !redis
                .sismember::<_, _, bool>(namespace.pending_completions(partition), game_id)
                .await?
        );
        assert!(lease_store.validate(&new_guard).await?);

        let _: () = redis
            .del(&[
                namespace.partition_assignment(partition),
                new_guard.lease_key(),
                namespace.recovery(game_id),
                namespace.active_games(partition),
                namespace.completion(game_id),
                namespace.pending_completions(partition),
            ])
            .await?;
        token.cancel();
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn group_aware_trim_never_deletes_large_pending_backlog() -> Result<()> {
    let _test_lock = STREAMS_TEST_LOCK.lock().await;
    timeout(Duration::from_secs(30), async {
        use redis::AsyncCommands;

        const TOTAL: usize = 8_300;
        let token = CancellationToken::new();
        let bus = streams_bus(token.clone()).await?;
        let partition = test_partition(32);
        let namespace = unique_namespace("trim-safety")?;
        let boot_id = BootIdentity::new();
        let successor_boot_id = BootIdentity::new();
        let client = redis::Client::open(REDIS_URL)?;
        let mut redis = client.get_multiplexed_async_connection().await?;
        let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(8);
        let manager =
            server::redis_utils::create_connection_manager(client.clone(), pubsub_tx).await?;
        let mut owners = serde_json::Map::new();
        owners.insert(
            partition.to_string(),
            serde_json::Value::String(boot_id.to_string()),
        );
        let _: () = redis
            .set(
                namespace.partition_assignment(partition),
                serde_json::to_vec(&serde_json::json!({ "owners": owners }))?,
            )
            .await?;
        let stream = RedisKeys::stream_commands(partition);
        bus.ensure_executor_command_group(&namespace, partition)
            .await?;
        bus.ensure_executor_command_group(&namespace, partition)
            .await?;
        for _ in 0..TOTAL {
            redis::cmd("XADD")
                .arg(&stream)
                .arg("*")
                .arg("data")
                .arg(b"not-json".as_slice())
                .query_async::<String>(&mut redis)
                .await?;
        }
        let lease_store = PartitionLeaseStore::new(
            manager,
            namespace.clone(),
            Duration::from_secs(30),
            Duration::from_millis(750),
        )?;
        let guard = lease_store
            .try_acquire(partition, &boot_id)
            .await?
            .expect("test partition lease acquired");
        let mut consumer = bus.subscribe_executor_commands(guard.clone()).await?;
        let mut pending_ids = Vec::with_capacity(TOTAL);
        loop {
            let deliveries = consumer.read_new_now().await?;
            if deliveries.is_empty() {
                break;
            }
            pending_ids.extend(deliveries.into_iter().map(|delivery| delivery.stream_id));
        }
        assert_eq!(pending_ids.len(), TOTAL);
        assert_eq!(bus.trim_executor_commands_fenced(&guard).await?, 0);
        assert_eq!(redis.xlen::<_, usize>(&stream).await?, TOTAL);

        // Take authority away from the original consumer after the pending
        // entry has survived more than the historical 8,192-entry trim bound.
        // The replacement must reclaim the oldest exact ID, not merely prove
        // that Redis still reports the same stream length.
        let oldest_pending_id = pending_ids
            .first()
            .cloned()
            .expect("the large pending backlog has an oldest entry");
        let mut successor_owners = serde_json::Map::new();
        successor_owners.insert(
            partition.to_string(),
            serde_json::Value::String(successor_boot_id.to_string()),
        );
        let _: () = redis
            .set(
                namespace.partition_assignment(partition),
                serde_json::to_vec(&serde_json::json!({ "owners": successor_owners }))?,
            )
            .await?;
        assert!(lease_store.release(&guard).await?);
        let successor_guard = lease_store
            .try_acquire(partition, &successor_boot_id)
            .await?
            .expect("successor acquired the reassigned lease");
        let mut successor = bus
            .subscribe_executor_commands(successor_guard.clone())
            .await?;
        let reclaimed = successor.reclaim_next().await?;
        assert!(reclaimed.deleted_pending_ids.is_empty());
        assert_eq!(
            reclaimed
                .deliveries
                .first()
                .map(|delivery| delivery.stream_id.as_str()),
            Some(oldest_pending_id.as_str()),
            "successor did not reclaim the oldest retained pending entry"
        );

        for batch in pending_ids.chunks(128) {
            assert_eq!(bus.xack_fenced(&successor_guard, batch).await?, batch.len());
        }
        let trimmed = bus.trim_executor_commands_fenced(&successor_guard).await?;
        assert!(trimmed >= TOTAL - 1);
        assert!(redis.xlen::<_, usize>(&stream).await? <= 1);

        let _: () = redis
            .del(&[
                namespace.partition_assignment(partition),
                successor_guard.lease_key(),
                stream,
            ])
            .await?;
        token.cancel();
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn group_created_after_publish_starts_at_zero_and_delivers_history() -> Result<()> {
    let _test_lock = STREAMS_TEST_LOCK.lock().await;
    timeout(Duration::from_secs(10), async {
        use redis::AsyncCommands;

        let token = CancellationToken::new();
        let bus = streams_bus(token.clone()).await?;
        let partition = test_partition(34);
        let namespace = unique_namespace("group-zero")?;
        let owner = BootIdentity::new();
        let client = redis::Client::open(REDIS_URL)?;
        let mut redis = client.get_multiplexed_async_connection().await?;
        let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(8);
        let manager =
            server::redis_utils::create_connection_manager(client.clone(), pubsub_tx).await?;
        let stream = RedisKeys::stream_commands(partition);
        let state = GameState::new(
            10,
            10,
            GameType::Solo,
            QueueMode::Quickmatch,
            Some(1),
            chrono::Utc::now().timestamp_millis(),
        );
        let payload = serde_json::to_vec(&StreamEvent::GameCreated {
            game_id: partition,
            game_state: state,
        })?;
        let historical_id: String = redis.xadd(&stream, "*", &[("data", payload)]).await?;

        // Matchmaking can commit GameCreated before the first executor task has
        // finished warming. Creating the stable group at '$' would skip it.
        bus.ensure_executor_command_group(&namespace, partition)
            .await?;
        let mut owners = serde_json::Map::new();
        owners.insert(
            partition.to_string(),
            serde_json::Value::String(owner.to_string()),
        );
        let _: () = redis
            .set(
                namespace.partition_assignment(partition),
                serde_json::to_vec(&serde_json::json!({ "owners": owners }))?,
            )
            .await?;
        let lease_store = PartitionLeaseStore::new(
            manager,
            namespace.clone(),
            Duration::from_secs(10),
            Duration::from_millis(750),
        )?;
        let guard = lease_store
            .try_acquire(partition, &owner)
            .await?
            .expect("assigned owner acquired its lease");
        let mut consumer = bus.subscribe_executor_commands(guard.clone()).await?;
        let deliveries = consumer.read_new_now().await?;
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].stream_id, historical_id);

        let _: () = redis
            .del(&[
                namespace.partition_assignment(partition),
                guard.lease_key(),
                stream,
            ])
            .await?;
        token.cancel();
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn game_created_checkpoint_and_index_precede_ack() -> Result<()> {
    let _test_lock = STREAMS_TEST_LOCK.lock().await;
    timeout(Duration::from_secs(15), async {
        use redis::AsyncCommands;

        let token = CancellationToken::new();
        let bus = streams_bus(token.clone()).await?;
        // These correctness paths require a real executor partition (0..10).
        // Give each such test a fixed, distinct partition so parallel tests
        // cannot delete one another's shared partition stream.
        let partition = 7;
        let base_game_id = 1_400_000_000u32;
        let game_id = base_game_id
            + (partition + server::game_executor::PARTITION_COUNT
                - base_game_id % server::game_executor::PARTITION_COUNT)
                % server::game_executor::PARTITION_COUNT;
        let namespace = unique_namespace("game-created-checkpoint")?;
        let owner = BootIdentity::new();
        let client = redis::Client::open(REDIS_URL)?;
        let mut redis = client.get_multiplexed_async_connection().await?;
        let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(8);
        let manager =
            server::redis_utils::create_connection_manager(client.clone(), pubsub_tx).await?;
        let stream = RedisKeys::stream_commands(partition);
        let _: () = redis.del(&stream).await?;
        let mut owners = serde_json::Map::new();
        owners.insert(
            partition.to_string(),
            serde_json::Value::String(owner.to_string()),
        );
        let _: () = redis
            .set(
                namespace.partition_assignment(partition),
                serde_json::to_vec(&serde_json::json!({ "owners": owners }))?,
            )
            .await?;
        let lease_store = PartitionLeaseStore::new(
            manager,
            namespace.clone(),
            Duration::from_secs(30),
            Duration::from_millis(750),
        )?;
        let guard = lease_store
            .try_acquire(partition, &owner)
            .await?
            .expect("test partition lease acquired");
        bus.ensure_executor_command_group(&namespace, partition)
            .await?;

        let state = GameState::new(
            30,
            30,
            GameType::Solo,
            QueueMode::Quickmatch,
            Some(9),
            chrono::Utc::now().timestamp_millis(),
        );
        let payload = serde_json::to_vec(&StreamEvent::GameCreated {
            game_id,
            game_state: state.clone(),
        })?;
        let stream_id: String = redis.xadd(&stream, "*", &[("data", payload)]).await?;
        let mut consumer = bus.subscribe_executor_commands(guard.clone()).await?;
        let deliveries = consumer.read_new_now().await?;
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].stream_id, stream_id);

        let envelope = RecoveryEnvelopeV2::new(
            game_id,
            partition,
            state,
            stream_id.clone(),
            ResolvedCommandState::default(),
            0,
            0,
            chrono::Utc::now().timestamp_millis(),
            guard.encoded_token(),
        );

        // Force the active-index write (which precedes XACK in the Lua
        // operation) to fail. Redis scripts do not roll back earlier SETs,
        // but the correctness-bearing entry must remain pending until a retry
        // can establish the index and ACK together.
        let _: () = redis
            .set(namespace.active_games(partition), "wrong-type")
            .await?;
        assert!(
            bus.checkpoint_and_ack_fenced(
                &guard,
                &envelope,
                std::slice::from_ref(&stream_id),
                Duration::from_secs(60),
            )
            .await
            .is_err()
        );
        let pending: redis::streams::StreamPendingCountReply = redis
            .xpending_count(&stream, namespace.command_group(partition), "-", "+", 10)
            .await?;
        assert_eq!(pending.ids.len(), 1);
        assert_eq!(pending.ids[0].id, stream_id);
        assert!(
            bus.get_recovery(&namespace, game_id).await?.is_some(),
            "a partial Redis script failure may leave the safe checkpoint prefix"
        );

        let _: () = redis.del(namespace.active_games(partition)).await?;
        assert_eq!(
            bus.checkpoint_and_ack_fenced(
                &guard,
                &envelope,
                std::slice::from_ref(&stream_id),
                Duration::from_secs(60),
            )
            .await?,
            1
        );
        assert!(
            redis
                .sismember::<_, _, bool>(namespace.active_games(partition), game_id)
                .await?
        );
        assert_eq!(
            bus.get_recovery(&namespace, game_id)
                .await?
                .expect("initial GameCreated checkpoint exists")
                .command_cursor,
            stream_id
        );
        assert_eq!(
            bus.xack_fenced(&guard, std::slice::from_ref(&stream_id))
                .await?,
            0,
            "GameCreated was ACKed only after checkpoint and index succeeded"
        );

        let _: () = redis
            .del(&[
                namespace.partition_assignment(partition),
                guard.lease_key(),
                namespace.recovery(game_id),
                namespace.active_games(partition),
                RedisKeys::game_snapshot(game_id),
                stream,
            ])
            .await?;
        token.cancel();
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn stale_group_reader_cannot_capture_a_post_takeover_command() -> Result<()> {
    let _test_lock = STREAMS_TEST_LOCK.lock().await;
    timeout(Duration::from_secs(10), async {
        use redis::AsyncCommands;

        let token = CancellationToken::new();
        let bus = streams_bus(token.clone()).await?;
        let partition = test_partition(33);
        let namespace = unique_namespace("stale-group-reader")?;
        let owner_a = BootIdentity::new();
        let owner_b = BootIdentity::new();
        let client = redis::Client::open(REDIS_URL)?;
        let mut redis = client.get_multiplexed_async_connection().await?;
        let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(8);
        let manager =
            server::redis_utils::create_connection_manager(client.clone(), pubsub_tx).await?;
        let stream = RedisKeys::stream_commands(partition);

        let assignment = |owner: &BootIdentity| {
            let mut owners = serde_json::Map::new();
            owners.insert(
                partition.to_string(),
                serde_json::Value::String(owner.to_string()),
            );
            serde_json::json!({ "owners": owners })
        };
        let _: () = redis
            .set(
                namespace.partition_assignment(partition),
                serde_json::to_vec(&assignment(&owner_a))?,
            )
            .await?;
        bus.ensure_executor_command_group(&namespace, partition)
            .await?;
        let groups_before: redis::streams::StreamInfoGroupsReply =
            redis.xinfo_groups(&stream).await?;
        let group_before = groups_before
            .groups
            .iter()
            .find(|group| group.name == namespace.command_group(partition))
            .expect("executor group exists before the stale read");
        assert_eq!(group_before.last_delivered_id, "0-0");
        assert_eq!(group_before.pending, 0);
        let lease_store = PartitionLeaseStore::new(
            manager,
            namespace.clone(),
            Duration::from_secs(10),
            Duration::from_millis(750),
        )?;
        let guard_a = lease_store
            .try_acquire(partition, &owner_a)
            .await?
            .expect("initial owner acquired its assigned lease");
        let mut stale_consumer = bus.subscribe_executor_commands(guard_a.clone()).await?;
        let stale_read = tokio::spawn(async move { stale_consumer.read_new_blocking().await });

        // Let A enter an empty read while its token is still valid, then replace
        // that exact token before publishing. A split GET/XREADGROUP sequence
        // assigns this entry to A after takeover and strands it in A's PEL.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _: () = redis
            .set(
                namespace.partition_assignment(partition),
                serde_json::to_vec(&assignment(&owner_b))?,
            )
            .await?;
        assert!(lease_store.release(&guard_a).await?);
        let guard_b = lease_store
            .try_acquire(partition, &owner_b)
            .await?
            .expect("successor acquired the released lease");

        let state = GameState::new(
            10,
            10,
            GameType::Solo,
            QueueMode::Quickmatch,
            Some(1),
            chrono::Utc::now().timestamp_millis(),
        );
        let payload = serde_json::to_vec(&StreamEvent::GameCreated {
            game_id: partition,
            game_state: state,
        })?;
        let stream_id: String = redis.xadd(&stream, "*", &[("data", payload)]).await?;

        let stale_error = timeout(Duration::from_secs(2), stale_read)
            .await??
            .expect_err("stale consumer assigned a command after lease replacement");
        assert!(stale_error.to_string().contains("lease authority was lost"));

        let groups_after_stale_read: redis::streams::StreamInfoGroupsReply =
            redis.xinfo_groups(&stream).await?;
        let group_after_stale_read = groups_after_stale_read
            .groups
            .iter()
            .find(|group| group.name == namespace.command_group(partition))
            .expect("executor group still exists after the stale read");
        assert_eq!(
            group_after_stale_read.last_delivered_id, "0-0",
            "a stale fenced read must not advance the group cursor"
        );
        assert_eq!(
            group_after_stale_read.pending, 0,
            "a stale fenced read must not mutate the PEL"
        );

        let stale_event = GameEventMessage {
            game_id: partition,
            tick: 0,
            sequence: 0,
            stream_seq: 1,
            user_id: None,
            event: GameEvent::Snapshot {
                game_state: GameState::new(
                    10,
                    10,
                    GameType::Solo,
                    QueueMode::Quickmatch,
                    Some(1),
                    chrono::Utc::now().timestamp_millis(),
                ),
            },
        };
        assert!(
            bus.publish_event_fenced(&guard_a, &stale_event)
                .await
                .is_err(),
            "the stale reader must not dispatch an authoritative mutation"
        );
        assert_eq!(
            redis
                .xlen::<_, usize>(RedisKeys::stream_events(partition))
                .await?,
            0
        );

        let mut successor = bus.subscribe_executor_commands(guard_b.clone()).await?;
        let deliveries = successor.read_new_now().await?;
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].stream_id, stream_id);
        let reclaimed = successor.reclaim_next().await?;
        assert_eq!(reclaimed.deliveries.len(), 1);
        assert_eq!(reclaimed.deliveries[0].stream_id, stream_id);

        let mut stale_reclaimer = bus.subscribe_executor_commands(guard_a).await?;
        let reclaim_error = stale_reclaimer
            .reclaim_next()
            .await
            .expect_err("stale consumer reclaimed the successor's pending entry");
        assert!(
            reclaim_error
                .to_string()
                .contains("lease authority was lost")
        );
        let pending: redis::streams::StreamPendingCountReply = redis
            .xpending_count(&stream, namespace.command_group(partition), "-", "+", 10)
            .await?;
        assert_eq!(pending.ids.len(), 1);
        assert_eq!(pending.ids[0].id, stream_id);
        assert_eq!(pending.ids[0].consumer, guard_b.encoded_token());

        let _: () = redis
            .del(&[
                namespace.partition_assignment(partition),
                guard_b.lease_key(),
                namespace.command_quarantine(partition),
                RedisKeys::stream_events(partition),
                stream,
            ])
            .await?;
        token.cancel();
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn replyable_rejection_is_durable_before_command_ack() -> Result<()> {
    let _test_lock = STREAMS_TEST_LOCK.lock().await;
    timeout(Duration::from_secs(15), async {
        use redis::AsyncCommands;

        let token = CancellationToken::new();
        let bus = streams_bus(token.clone()).await?;
        let partition = 8;
        let base_game_id = 1_500_000_000u32;
        let game_id = base_game_id
            + (partition + server::game_executor::PARTITION_COUNT
                - base_game_id % server::game_executor::PARTITION_COUNT)
                % server::game_executor::PARTITION_COUNT;
        let namespace = unique_namespace("replyable-rejection")?;
        let boot_id = BootIdentity::new();
        let client = redis::Client::open(REDIS_URL)?;
        let mut redis = client.get_multiplexed_async_connection().await?;
        let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(8);
        let manager =
            server::redis_utils::create_connection_manager(client.clone(), pubsub_tx).await?;
        let command_stream = RedisKeys::stream_commands(partition);
        let event_stream = RedisKeys::stream_events(partition);
        let _: () = redis.del(&[&command_stream, &event_stream]).await?;
        let mut owners = serde_json::Map::new();
        owners.insert(
            partition.to_string(),
            serde_json::Value::String(boot_id.to_string()),
        );
        let _: () = redis
            .set(
                namespace.partition_assignment(partition),
                serde_json::to_vec(&serde_json::json!({ "owners": owners }))?,
            )
            .await?;
        bus.ensure_executor_command_group(&namespace, partition)
            .await?;
        let lease_store = PartitionLeaseStore::new(
            manager,
            namespace.clone(),
            Duration::from_secs(10),
            Duration::from_millis(750),
        )?;
        let guard = lease_store
            .try_acquire(partition, &boot_id)
            .await?
            .expect("test partition lease acquired");
        let mut consumer = bus.subscribe_executor_commands(guard.clone()).await?;
        let mut events = bus.subscribe_to_partition(partition).await?;

        let command_id = ClientCommandIdentityV2 {
            game_id,
            user_id: 77,
            client_game_session_id: format!("session-{}", namespace.region()),
            sequence: 1,
        };
        let command = StreamEvent::GameCommandSubmittedV2 {
            game_id,
            user_id: command_id.user_id,
            command_id: command_id.clone(),
            command: GameCommandMessage {
                command_id_client: CommandId {
                    tick: 1,
                    user_id: command_id.user_id,
                    sequence_number: 1,
                },
                command_id_server: None,
                command: GameCommand::Turn {
                    snake_id: 1,
                    direction: Direction::Up,
                },
            },
        };
        let stream_id = bus.publish_command_fenced(&guard, &command).await?;
        let deliveries = consumer.read_new_blocking().await?;
        assert!(
            deliveries
                .iter()
                .any(|delivery| delivery.stream_id == stream_id)
        );

        let reason = "authoritative actor is no longer active";
        let rejection = GameEventMessage {
            game_id,
            tick: 0,
            sequence: 0,
            stream_seq: 0,
            user_id: Some(command_id.user_id),
            event: GameEvent::CommandRejected {
                command_id: command_id.clone(),
                reason: reason.into(),
            },
        };
        let rejection_stream_id = bus
            .reject_and_ack_fenced(&guard, &stream_id, &rejection, reason)
            .await?;
        let received = timeout(Duration::from_secs(2), events.recv_event())
            .await?
            .expect("event reader ended before durable rejection");
        assert!(matches!(
            received.event,
            GameEvent::CommandRejected {
                command_id: received_id,
                ..
            } if received_id == command_id
        ));
        assert_eq!(
            bus.xack_fenced(&guard, std::slice::from_ref(&stream_id))
                .await?,
            0,
            "the rejection transaction must already have ACKed the command"
        );
        assert_eq!(
            redis
                .xlen::<_, usize>(namespace.command_quarantine(partition))
                .await?,
            0,
            "a replyable rejection is a normal terminal outcome, not poison"
        );
        let poison_id = bus.publish_command_fenced(&guard, &command).await?;
        let poison_deliveries = consumer.read_new_blocking().await?;
        assert!(
            poison_deliveries
                .iter()
                .any(|delivery| delivery.stream_id == poison_id)
        );
        bus.quarantine_and_ack_fenced(
            &guard,
            &poison_id,
            b"malformed-fixture",
            "fixture cannot receive a terminal outcome",
        )
        .await?;
        assert_eq!(
            bus.xack_fenced(&guard, std::slice::from_ref(&poison_id))
                .await?,
            0,
            "the quarantine transaction must already have ACKed the poison entry"
        );
        assert_eq!(
            redis
                .xlen::<_, usize>(namespace.command_quarantine(partition))
                .await?,
            1,
            "unreplyable poison must remain quarantined for diagnosis"
        );

        let _: usize = redis
            .xdel(&command_stream, &[&stream_id, &poison_id])
            .await?;
        let _: usize = redis.xdel(&event_stream, &[&rejection_stream_id]).await?;
        let _: i32 = redis::cmd("XGROUP")
            .arg("DESTROY")
            .arg(&command_stream)
            .arg(namespace.command_group(partition))
            .query_async(&mut redis)
            .await?;
        let _: () = redis
            .del(&[
                namespace.partition_assignment(partition),
                guard.lease_key(),
                namespace.command_quarantine(partition),
            ])
            .await?;
        token.cancel();
        Ok(())
    })
    .await?
}

fn event(game_id: u32, stream_seq: u64) -> GameEventMessage {
    GameEventMessage {
        game_id,
        tick: stream_seq as u32,
        sequence: stream_seq,
        stream_seq,
        user_id: None,
        event: GameEvent::TickHash {
            hash: stream_seq,
            server_ts_ms: 0,
        },
    }
}

async fn cleanup(partition: u32) {
    if let Ok(client) = redis::Client::open(REDIS_URL)
        && let Ok(mut conn) = client.get_multiplexed_async_connection().await
    {
        use redis::AsyncCommands;
        let _: std::result::Result<(), _> = conn
            .del::<_, ()>(&[
                RedisKeys::stream_events(partition),
                RedisKeys::stream_commands(partition),
                RedisKeys::stream_snapshot_requests(partition),
            ])
            .await;
    }
}

#[tokio::test]
async fn fenced_completion_cleans_matchmaking_and_notifies_exactly_once() -> Result<()> {
    let _test_lock = STREAMS_TEST_LOCK.lock().await;
    timeout(Duration::from_secs(15), async {
        use redis::AsyncCommands;

        let token = CancellationToken::new();
        let bus = streams_bus(token.clone()).await?;
        let partition = test_partition(20);
        cleanup(partition).await;
        let game_id = partition;
        let newer_game_id = game_id + 1;
        let namespace = unique_namespace("completion-test")?;
        let boot_id = BootIdentity::new();

        let client = redis::Client::open(REDIS_URL)?;
        let mut redis = client.get_multiplexed_async_connection().await?;
        let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(8);
        let manager =
            server::redis_utils::create_connection_manager(client.clone(), pubsub_tx).await?;
        let mut owners = serde_json::Map::new();
        owners.insert(
            partition.to_string(),
            serde_json::Value::String(boot_id.to_string()),
        );
        let assignment = serde_json::json!({ "owners": owners });
        let _: () = redis
            .set(
                namespace.partition_assignment(partition),
                serde_json::to_vec(&assignment)?,
            )
            .await?;
        let lease_store = PartitionLeaseStore::new(
            manager,
            namespace.clone(),
            Duration::from_secs(10),
            Duration::from_millis(750),
        )?;
        let guard = lease_store
            .try_acquire(partition, &boot_id)
            .await?
            .expect("test partition lease acquired");

        let now = chrono::Utc::now().timestamp_millis();
        let mut final_state = GameState::new(
            20,
            20,
            GameType::Solo,
            QueueMode::Quickmatch,
            Some(1),
            now - 1_000,
        );
        let player = final_state.add_player(101, Some("player-101".into()))?;
        final_state.add_spectator(202, Some("spectator-202".into()));
        final_state.scores.insert(player.snake_id, 55);
        final_state.status = GameStatus::Complete {
            winning_snake_id: Some(player.snake_id),
        };
        let record = CompletionRecordV1 {
            schema_version: COMPLETION_SCHEMA_VERSION,
            game_id,
            partition_id: partition,
            revision: uuid::Uuid::new_v4(),
            ended_at_ms: now,
            server_id: 42,
            final_state: final_state.clone(),
            effects: vec![
                CompletionEffect::PersistGame { id: "game".into() },
                CompletionEffect::InsertHighScore {
                    id: "high_score:101".into(),
                    user_id: 101,
                    username: "player-101".into(),
                    score: 55,
                    game_type: GameType::Solo,
                    region: "test".into(),
                    season: 1,
                },
            ],
        };
        record.validate()?;
        let envelope = RecoveryEnvelopeV2::new(
            game_id,
            partition,
            final_state,
            "0-0".into(),
            ResolvedCommandState::default(),
            0,
            7,
            now,
            guard.encoded_token(),
        );
        let active_match = ActiveMatch {
            players: vec![QueuedPlayer {
                user_id: 101,
                mmr: 1_000,
                username: "player-101".into(),
            }],
            spectators: vec![QueuedPlayer {
                user_id: 202,
                mmr: 1_000,
                username: "spectator-202".into(),
            }],
            lobby_codes: vec!["LOBBY-A".into(), "LOBBY-B".into()],
            game_type: GameType::Solo,
            status: MatchStatus::Active,
            partition_id: partition,
            created_at: now - 2_000,
        };
        let active_matches_key = RedisKeys::matchmaking_active_matches();
        let user_player_key = RedisKeys::matchmaking_user_active_game(101);
        let user_spectator_key = RedisKeys::matchmaking_user_active_game(202);
        let lobby_a_key = RedisKeys::matchmaking_lobby_active_game("LOBBY-A");
        let lobby_b_key = RedisKeys::matchmaking_lobby_active_game("LOBBY-B");
        let game_value = game_id.to_string();
        let _: () = redis
            .hset(
                &active_matches_key,
                game_id.to_string(),
                serde_json::to_string(&active_match)?,
            )
            .await?;
        for key in [
            &user_player_key,
            &user_spectator_key,
            &lobby_a_key,
            &lobby_b_key,
        ] {
            let _: () = redis.set(key, &game_value).await?;
        }
        let _: () = redis
            .sadd(namespace.active_games(partition), game_id)
            .await?;

        assert!(
            bus.commit_completion_record_fenced(
                &guard,
                &envelope,
                &[],
                &record,
                Duration::from_secs(60),
            )
            .await?
        );
        bus.cleanup_matchmaking_for_completion(&record).await?;
        for key in [
            &user_player_key,
            &user_spectator_key,
            &lobby_a_key,
            &lobby_b_key,
        ] {
            assert_eq!(redis.get::<_, Option<String>>(key).await?, None);
        }
        assert_eq!(
            redis
                .hget::<_, _, Option<String>>(&active_matches_key, game_id.to_string())
                .await?,
            None
        );
        assert_eq!(
            redis
                .xlen::<_, u64>(RedisKeys::stream_events(partition))
                .await?,
            1
        );
        assert_eq!(
            redis
                .xlen::<_, u64>(RedisKeys::stream_commands(partition))
                .await?,
            1
        );

        // Shape the retry like a crash: reload the record into fresh HashMaps
        // before serializing it again. Canonical immutable bytes must accept
        // the same semantic record regardless of map iteration order.
        let stored_record: Vec<u8> = redis.get(namespace.completion(game_id)).await?;
        let reloaded_record: CompletionRecordV1 = serde_json::from_slice(&stored_record)?;

        // An ambiguous retry neither republishes terminal messages nor
        // changes already-clean state.
        assert!(
            !bus.commit_completion_record_fenced(
                &guard,
                &envelope,
                &[],
                &reloaded_record,
                Duration::from_secs(60),
            )
            .await?
        );
        assert_eq!(
            redis
                .xlen::<_, u64>(RedisKeys::stream_events(partition))
                .await?,
            1
        );
        assert_eq!(
            redis
                .xlen::<_, u64>(RedisKeys::stream_commands(partition))
                .await?,
            1
        );

        // A delayed completion may still find its old ActiveMatch, but every
        // mapping is compare-deleted against the completed game ID.
        let _: () = redis
            .hset(
                &active_matches_key,
                game_id.to_string(),
                serde_json::to_string(&active_match)?,
            )
            .await?;
        for key in [
            &user_player_key,
            &user_spectator_key,
            &lobby_a_key,
            &lobby_b_key,
        ] {
            let _: () = redis.set(key, newer_game_id).await?;
        }
        assert!(
            !bus.commit_completion_record_fenced(
                &guard,
                &envelope,
                &[],
                &record,
                Duration::from_secs(60),
            )
            .await?
        );
        bus.cleanup_matchmaking_for_completion(&record).await?;
        for key in [
            &user_player_key,
            &user_spectator_key,
            &lobby_a_key,
            &lobby_b_key,
        ] {
            assert_eq!(redis.get::<_, u32>(key).await?, newer_game_id);
        }
        assert_eq!(
            redis
                .hget::<_, _, Option<String>>(&active_matches_key, game_id.to_string())
                .await?,
            None
        );

        // Pending retries are driven solely by the immutable completion. The
        // shorter-lived recovery/snapshot cache may disappear during a long
        // database outage without making the durable effects unrecoverable.
        let _: () = redis
            .del(&[
                namespace.recovery(game_id),
                RedisKeys::game_snapshot(game_id),
            ])
            .await?;
        let loaded = bus
            .load_pending_completion(&namespace, partition, game_id)
            .await?;
        assert_eq!(loaded.game_id, record.game_id);
        assert_eq!(loaded.revision, record.revision);
        assert_eq!(loaded.effects.len(), record.effects.len());

        // A corrupt index member is isolated by the per-ID load API. A drain
        // can report these two records and still reach the valid later game.
        let missing_game_id = game_id + 10;
        let malformed_game_id = game_id + 20;
        let _: () = redis
            .sadd(
                namespace.pending_completions(partition),
                &[missing_game_id, malformed_game_id],
            )
            .await?;
        let _: () = redis
            .set(
                namespace.completion(malformed_game_id),
                b"not-json".as_slice(),
            )
            .await?;
        let pending_ids = bus
            .list_pending_completion_ids(&namespace, partition)
            .await?;
        assert!(pending_ids.contains(&missing_game_id));
        assert!(pending_ids.contains(&malformed_game_id));
        assert!(pending_ids.contains(&game_id));
        assert!(
            bus.load_pending_completion(&namespace, partition, missing_game_id)
                .await
                .is_err()
        );
        assert!(
            bus.load_pending_completion(&namespace, partition, malformed_game_id)
                .await
                .is_err()
        );
        let loaded = bus
            .load_pending_completion(&namespace, partition, game_id)
            .await?;
        assert_eq!(loaded.game_id, record.game_id);
        assert_eq!(loaded.revision, record.revision);
        assert_eq!(loaded.effects.len(), record.effects.len());
        let _: () = redis
            .srem(
                namespace.pending_completions(partition),
                &[missing_game_id, malformed_game_id],
            )
            .await?;
        let _: () = redis.del(namespace.completion(malformed_game_id)).await?;

        // Pending records have no TTL. Only the final effect confirmation
        // removes the pending index and starts the configured cleanup grace.
        let grace = Duration::from_secs(30);
        assert!(
            bus.mark_completion_effect_done_fenced(&guard, &record, record.effects[0].id(), grace,)
                .await?
        );
        assert_eq!(
            redis.pttl::<_, i64>(namespace.completion(game_id)).await?,
            -1
        );
        assert!(
            redis
                .sismember::<_, _, bool>(namespace.pending_completions(partition), game_id)
                .await?
        );
        assert!(
            !bus.mark_completion_effect_done_fenced(
                &guard,
                &record,
                record.effects[0].id(),
                grace,
            )
            .await?
        );
        assert!(
            bus.mark_completion_effect_done_fenced(&guard, &record, record.effects[1].id(), grace,)
                .await?
        );
        assert!(
            !redis
                .sismember::<_, _, bool>(namespace.pending_completions(partition), game_id)
                .await?
        );
        for key in [
            namespace.completion(game_id),
            namespace.completion_effects_done(game_id),
            namespace.completion_terminal_notified(game_id),
        ] {
            let ttl: i64 = redis.pttl(key).await?;
            assert!(ttl > 0 && ttl <= grace.as_millis() as i64);
        }

        let _: () = redis
            .del(&[
                namespace.partition_assignment(partition),
                guard.lease_key(),
                namespace.recovery(game_id),
                namespace.active_games(partition),
                namespace.pending_completions(partition),
                namespace.completion(game_id),
                namespace.completion_effects_done(game_id),
                namespace.completion_terminal_notified(game_id),
                RedisKeys::game_snapshot(game_id),
                RedisKeys::stream_events(partition),
                RedisKeys::stream_commands(partition),
                user_player_key,
                user_spectator_key,
                lobby_a_key,
                lobby_b_key,
            ])
            .await?;
        let _: usize = redis.hdel(active_matches_key, game_id.to_string()).await?;
        token.cancel();
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn paused_consumer_loses_nothing() -> Result<()> {
    let _test_lock = STREAMS_TEST_LOCK.lock().await;
    timeout(Duration::from_secs(10), async {
        let token = CancellationToken::new();
        let bus = streams_bus(token.clone()).await?;
        let partition = test_partition(1);
        let game_id = partition;

        let mut sub = bus.subscribe_to_partition(partition).await?;
        // Give the reader task a moment to issue its first XREAD.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Publish MORE messages than the subscriber channel holds (2000)
        // while the consumer is completely paused. Under Pub/Sub this
        // overflows and drops; under Streams the reader backpressure-blocks
        // and the log holds the rest (3000 < the 8192 trim bound).
        const TOTAL: u64 = 3000;
        for seq in 1..=TOTAL {
            bus.publish_event(partition, &event(game_id, seq)).await?;
        }

        // Resume consuming: every message must arrive, in order.
        for expected in 1..=TOTAL {
            let msg = sub
                .recv_event()
                .await
                .expect("stream ended before all messages were delivered");
            assert_eq!(
                msg.stream_seq, expected,
                "expected contiguous stream_seq {} but got {} — a streams transport must never lose or reorder",
                expected, msg.stream_seq
            );
        }

        token.cancel();
        cleanup(partition).await;
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn three_streams_route_to_their_channels_in_order() -> Result<()> {
    let _test_lock = STREAMS_TEST_LOCK.lock().await;
    timeout(Duration::from_secs(10), async {
        let token = CancellationToken::new();
        let bus = streams_bus(token.clone()).await?;
        let partition = test_partition(2);

        let mut sub = bus.subscribe_to_partition(partition).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        for seq in 1..=5u64 {
            bus.publish_event(partition, &event(partition, seq)).await?;
            bus.publish_command(
                partition,
                &StreamEvent::StatusUpdated {
                    game_id: seq as u32,
                    status: common::GameStatus::Stopped,
                },
            )
            .await?;
        }
        bus.request_partition_snapshots(partition).await?;

        for expected in 1..=5u64 {
            let msg = sub.recv_event().await.expect("event stream ended");
            assert_eq!(msg.stream_seq, expected);
        }
        for expected in 1..=5u32 {
            let cmd = sub.recv_command().await.expect("command stream ended");
            match cmd.payload {
                server::game_bus::CommandDeliveryPayload::Command(StreamEvent::StatusUpdated {
                    game_id,
                    ..
                }) => assert_eq!(game_id, expected),
                other => panic!("unexpected command: {:?}", other),
            }
        }
        let req = sub
            .recv_snapshot_request()
            .await
            .expect("request stream ended");
        assert_eq!(req.partition_id, partition);

        token.cancel();
        cleanup(partition).await;
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn subscription_starts_at_now_not_history() -> Result<()> {
    let _test_lock = STREAMS_TEST_LOCK.lock().await;
    timeout(Duration::from_secs(10), async {
        let token = CancellationToken::new();
        let bus = streams_bus(token.clone()).await?;
        let partition = test_partition(3);

        // History that must NOT be delivered: subscriptions start at the
        // stream tail and rely on snapshots for initial state, not on
        // replaying stream history.
        for seq in 1..=50u64 {
            bus.publish_event(partition, &event(partition, seq)).await?;
        }

        let mut sub = bus.subscribe_to_partition(partition).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;

        bus.publish_event(partition, &event(partition, 100)).await?;
        let msg = sub.recv_event().await.expect("event stream ended");
        assert_eq!(
            msg.stream_seq, 100,
            "a new subscription must start at the tail, got historical seq {}",
            msg.stream_seq
        );

        token.cancel();
        cleanup(partition).await;
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn streams_are_trimmed_at_publish_time() -> Result<()> {
    let _test_lock = STREAMS_TEST_LOCK.lock().await;
    timeout(Duration::from_secs(30), async {
        let token = CancellationToken::new();
        let bus = streams_bus(token.clone()).await?;
        let partition = test_partition(4);

        // Publish past the events MAXLEN (8192). `MAXLEN ~` trims at node
        // granularity, so allow slack above the bound — the point is that
        // length is bounded, not unbounded like the 2025 implementation.
        for seq in 1..=9500u64 {
            bus.publish_event(partition, &event(partition, seq)).await?;
        }

        let client = redis::Client::open(REDIS_URL)?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        use redis::AsyncCommands;
        let len: u64 = conn.xlen(RedisKeys::stream_events(partition)).await?;
        assert!(
            len < 9200,
            "stream length {} suggests MAXLEN trimming is not applied",
            len
        );
        assert!(len >= 8000, "stream over-trimmed: {}", len);

        token.cancel();
        cleanup(partition).await;
        Ok(())
    })
    .await?
}

/// The performance gate: end-to-end XADD -> subscriber delivery latency at
/// game-like rates. The 2025 Streams implementation measured 100-900 ms here
/// because blocking reads were multiplexed onto shared connections; the
/// current design must be push-like. Thresholds are generous for CI noise
/// while still catching any reintroduction of poll-quantized delivery.
#[tokio::test]
async fn delivery_latency_is_push_like() -> Result<()> {
    let _test_lock = STREAMS_TEST_LOCK.lock().await;
    timeout(Duration::from_secs(60), async {
        let token = CancellationToken::new();
        let bus = streams_bus(token.clone()).await?;
        let partition = test_partition(5);

        let mut sub = bus.subscribe_to_partition(partition).await?;
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Game-like cadence: bursts of 4 events every 50 ms (executor poll
        // interval), 100 rounds = 400 samples over ~5 s.
        let mut latencies_us: Vec<u128> = Vec::with_capacity(400);
        for round in 0..100u64 {
            let sent = Instant::now();
            for i in 0..4u64 {
                bus.publish_event(partition, &event(partition, round * 4 + i + 1))
                    .await?;
            }
            for _ in 0..4 {
                sub.recv_event().await.expect("event stream ended");
            }
            latencies_us.push(sent.elapsed().as_micros() / 4);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        latencies_us.sort_unstable();
        let p50 = latencies_us[latencies_us.len() / 2];
        let p99 = latencies_us[latencies_us.len() * 99 / 100];
        println!(
            "streams delivery latency: p50={}us p99={}us (n={})",
            p50,
            p99,
            latencies_us.len()
        );

        assert!(
            p50 < 10_000,
            "p50 delivery latency {}us — streams should be push-like (<10ms)",
            p50
        );
        assert!(
            p99 < 50_000,
            "p99 delivery latency {}us — smells like poll-quantized or blocked-connection delivery",
            p99
        );

        token.cancel();
        cleanup(partition).await;
        Ok(())
    })
    .await?
}

/// Kill the reader's dedicated connection mid-stream and verify it resumes
/// from its last-delivered ID with no loss and no duplicates — the property
/// Pub/Sub fundamentally cannot provide across a reconnect.
#[tokio::test]
async fn reader_reconnect_resumes_without_loss_or_duplicates() -> Result<()> {
    let _test_lock = STREAMS_TEST_LOCK.lock().await;
    timeout(Duration::from_secs(15), async {
        let token = CancellationToken::new();
        let bus = streams_bus(token.clone()).await?;
        let partition = test_partition(7);

        let mut sub = bus.subscribe_to_partition(partition).await?;

        for seq in 1..=100u64 {
            bus.publish_event(partition, &event(partition, seq)).await?;
        }
        for expected in 1..=50u64 {
            let msg = sub.recv_event().await.expect("stream ended");
            assert_eq!(msg.stream_seq, expected);
        }

        // Sever every parked XREAD connection on this Redis. Readers are
        // designed to reconnect and resume from their last-delivered ID, so
        // even collateral kills of other tests' readers are harmless.
        let client = redis::Client::open(REDIS_URL)?;
        let mut admin = client.get_multiplexed_async_connection().await?;
        let list: String = redis::cmd("CLIENT")
            .arg("LIST")
            .query_async(&mut admin)
            .await?;
        let mut killed = 0;
        for line in list.lines() {
            if line.contains("cmd=xread")
                && let Some(id) = line.split_whitespace().find_map(|f| f.strip_prefix("id="))
            {
                let _: i64 = redis::cmd("CLIENT")
                    .arg("KILL")
                    .arg("ID")
                    .arg(id)
                    .query_async(&mut admin)
                    .await?;
                killed += 1;
            }
        }
        assert!(
            killed > 0,
            "expected to kill at least the reader's parked XREAD"
        );

        for seq in 101..=200u64 {
            bus.publish_event(partition, &event(partition, seq)).await?;
        }

        // Everything from 51 onward must arrive exactly once, in order,
        // across the reconnect boundary.
        for expected in 51..=200u64 {
            let msg = sub.recv_event().await.expect("stream ended after kill");
            assert_eq!(
                msg.stream_seq, expected,
                "reconnect must resume from the last-delivered ID: expected {}, got {}",
                expected, msg.stream_seq
            );
        }

        token.cancel();
        cleanup(partition).await;
        Ok(())
    })
    .await?
}

/// Manual latency benchmark; ignored in normal runs.
/// cargo test -p server --test game_bus_test bench_streams -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn bench_streams() -> Result<()> {
    let _test_lock = STREAMS_TEST_LOCK.lock().await;
    let token = CancellationToken::new();
    let bus = streams_bus(token.clone()).await?;
    let partition = test_partition(6);

    let mut sub = bus.subscribe_to_partition(partition).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut latencies_us: Vec<u128> = Vec::new();
    for round in 0..200u64 {
        let sent = Instant::now();
        bus.publish_event(partition, &event(partition, round + 1))
            .await?;
        sub.recv_event().await.expect("stream ended");
        latencies_us.push(sent.elapsed().as_micros());
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    latencies_us.sort_unstable();
    println!(
        "streams: p50={}us p90={}us p99={}us",
        latencies_us[latencies_us.len() / 2],
        latencies_us[latencies_us.len() * 90 / 100],
        latencies_us[latencies_us.len() * 99 / 100],
    );
    token.cancel();
    cleanup(partition).await;
    Ok(())
}
