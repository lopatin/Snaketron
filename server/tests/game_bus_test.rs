//! Integration tests for the Streams game-bus transport (SNAKETRON_BUS=streams).
//!
//! Run against the test-deps Redis (test-deps.sh). Isolation: these tests use
//! partition ids far outside the live range (game partitions are 0..10) so
//! their stream keys cannot collide with a dev server or other tests sharing
//! the same Redis.
//!
//! The headline test is `paused_consumer_loses_nothing`: the exact scenario —
//! a subscriber that stops draining for a while — where Pub/Sub drops
//! messages (broadcast lag / at-most-once) and Streams must not.

use anyhow::Result;
use common::{GameEvent, GameEventMessage};
use server::game_bus::{BusKind, GameBus};
use server::game_executor::StreamEvent;
use server::pubsub_manager::PubSubManager;
use server::redis_keys::RedisKeys;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

const REDIS_URL: &str = "redis://127.0.0.1:6379/1?protocol=resp3";

/// Unique high partition id per test run, far above the live range 0..10.
fn test_partition(salt: u32) -> u32 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    1_000_000 + (nanos % 100_000) + salt * 1_000_000
}

async fn streams_bus(token: CancellationToken) -> Result<GameBus> {
    let client = redis::Client::open(REDIS_URL)?;
    let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(64);
    let redis =
        server::redis_utils::create_connection_manager(client.clone(), pubsub_tx.clone()).await?;
    let pubsub = PubSubManager::new(redis.clone(), pubsub_tx, token.clone());
    Ok(GameBus::new(BusKind::Streams, pubsub, redis, client, token))
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
async fn paused_consumer_loses_nothing() -> Result<()> {
    timeout(Duration::from_secs(10), async {
        let token = CancellationToken::new();
        let bus = streams_bus(token.clone()).await?;
        let partition = test_partition(1);
        let game_id = partition;

        let mut sub = bus.subscribe_to_partition(partition).await?;
        // Give the reader task a moment to issue its first XREAD ("$").
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
            match cmd {
                StreamEvent::StatusUpdated { game_id, .. } => assert_eq!(game_id, expected),
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
    timeout(Duration::from_secs(10), async {
        let token = CancellationToken::new();
        let bus = streams_bus(token.clone()).await?;
        let partition = test_partition(3);

        // History that must NOT be delivered: subscriptions anchor on
        // snapshots (like the pubsub transport), not on stream history.
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

/// Manual A/B benchmark across both transports; ignored in normal runs.
/// cargo test -p server --test game_bus_test bench_both -- --ignored --nocapture
#[tokio::test]
#[ignore]
async fn bench_both_transports() -> Result<()> {
    for kind in [BusKind::PubSub, BusKind::Streams] {
        let token = CancellationToken::new();
        let client = redis::Client::open(REDIS_URL)?;
        let (pubsub_tx, _rx) = tokio::sync::broadcast::channel(5000);
        let redis =
            server::redis_utils::create_connection_manager(client.clone(), pubsub_tx.clone())
                .await?;
        let pubsub = PubSubManager::new(redis.clone(), pubsub_tx, token.clone());
        let bus = GameBus::new(kind, pubsub, redis, client, token.clone());
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
            "{:?}: p50={}us p90={}us p99={}us",
            kind,
            latencies_us[latencies_us.len() / 2],
            latencies_us[latencies_us.len() * 90 / 100],
            latencies_us[latencies_us.len() * 99 / 100],
        );
        token.cancel();
        cleanup(partition).await;
    }
    Ok(())
}
