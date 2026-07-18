//! Executor failover: resuming in-flight games from stored Redis snapshots.
//!
//! The pure selection logic (partition/status filtering, replica-over-snapshot
//! precedence) is unit-tested in game_executor.rs. This test covers the
//! Redis-facing half against a real Redis (started via test-deps.sh): a game
//! whose executor died leaves only its periodically refreshed stored snapshot
//! behind, and a takeover executor must discover it through the same
//! store/scan path the production code uses.
//!
//! Isolation note: this test only writes/reads uniquely numbered
//! game:snapshot:* keys — it publishes nothing on the shared pub/sub
//! channels, so it cannot interfere with (or be affected by) a dev server
//! running against the same Redis.

use anyhow::Result;
use common::{GameState, GameStatus, GameType, QueueMode};
use server::game_executor::PARTITION_COUNT;
use server::pubsub_manager::PubSubManager;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

const REDIS_URL: &str = "redis://127.0.0.1:6379/1?protocol=resp3";

fn unique_game_id(partition: u32) -> u32 {
    // Unique per run, aligned to the requested partition.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    // Keep well away from small hand-assigned ids; preserve partition residue.
    let base = 1_000_000 + (nanos % 500_000);
    base - (base % PARTITION_COUNT) + partition
}

fn started_state(tick: u32) -> GameState {
    let mut state = GameState::new(
        20,
        20,
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Quickmatch,
        Some(7),
        0,
    );
    state.status = GameStatus::Started { server_id: 42 };
    state.tick = tick;
    state
}

#[tokio::test]
async fn stored_snapshot_is_discoverable_for_resume() -> Result<()> {
    timeout(Duration::from_secs(10), async {
        let redis_client = redis::Client::open(REDIS_URL)?;
        let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(64);
        let mut redis =
            server::redis_utils::create_connection_manager(redis_client, pubsub_tx.clone()).await?;

        let token = CancellationToken::new();
        let pubsub = PubSubManager::new(redis.clone(), pubsub_tx, token.clone());

        let partition = 4;
        let game_id = unique_game_id(partition);
        let dead_executor_state = started_state(137);

        // The dying executor's last act: the TickHash-cadence snapshot store.
        pubsub.store_snapshot(game_id, &dead_executor_state).await?;

        // A takeover executor discovers it through the same scan path
        // run_game_executor uses, with an empty (cold) replica.
        let discovered = server::game_executor::load_stored_snapshots(&mut redis).await;
        let resumable =
            server::game_executor::select_resumable_games(partition, Vec::new(), discovered);

        let resumed = resumable
            .iter()
            .find(|(id, _)| *id == game_id)
            .unwrap_or_else(|| panic!("game {} not discovered for resume", game_id));
        assert_eq!(resumed.1.tick, 137, "resume must use the stored state");
        assert!(matches!(
            resumed.1.status,
            GameStatus::Started { server_id: 42 }
        ));

        // A game whose stored snapshot says Complete must not be resurrected.
        let complete_id = unique_game_id(partition) + PARTITION_COUNT;
        let mut complete_state = started_state(200);
        complete_state.status = GameStatus::Complete {
            winning_snake_id: None,
        };
        pubsub.store_snapshot(complete_id, &complete_state).await?;

        let discovered = server::game_executor::load_stored_snapshots(&mut redis).await;
        let resumable =
            server::game_executor::select_resumable_games(partition, Vec::new(), discovered);
        assert!(
            !resumable.iter().any(|(id, _)| *id == complete_id),
            "completed game must not be selected for resume"
        );

        // Clean up our keys.
        use redis::AsyncCommands;
        let _: () = redis
            .del(server::redis_keys::RedisKeys::game_snapshot(game_id))
            .await?;
        let _: () = redis
            .del(server::redis_keys::RedisKeys::game_snapshot(complete_id))
            .await?;

        Ok(())
    })
    .await?
}
