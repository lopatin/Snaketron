//! Drains the emitter channel into the Valkey analytics stream.
//!
//! One task per game server. It is the only writer to the stream, and it is
//! registered in the supervised handles so its **final flush runs before the
//! task exits** — a detached version would silently lose whatever was buffered
//! at SIGTERM, which is precisely the failure the supervision contract exists
//! to prevent.

use std::sync::Arc;
use std::time::Duration;

use redis::AsyncCommands;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::redis_utils::RedisConnection;

use super::emitter::{DropReason, EmitterMetrics};
use super::event::to_json_line;
use super::proto;

/// How many events one XADD carries. Batching keeps Valkey command volume
/// proportional to batches rather than to events.
const DEFAULT_BATCH: usize = 256;
const DEFAULT_INTERVAL: Duration = Duration::from_millis(2_000);

pub struct FlusherConfig {
    pub stream_key: String,
    pub maxlen: usize,
    pub batch: usize,
    pub interval: Duration,
}

impl FlusherConfig {
    pub fn from_env(stream_key: String) -> Self {
        let parse = |name: &str, fallback: usize| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(fallback)
        };
        Self {
            stream_key,
            // Bounded so a stalled exporter cannot grow Valkey without limit.
            // Loss beyond the horizon is acceptable for analytics but must be
            // counted, which the exporter does on reconnect.
            maxlen: parse("SNAKETRON_ANALYTICS_MAXLEN", 1_000_000),
            batch: parse("SNAKETRON_ANALYTICS_BATCH", DEFAULT_BATCH),
            interval: Duration::from_millis(
                parse("SNAKETRON_ANALYTICS_FLUSH_INTERVAL_MS", 2_000) as u64
            ),
        }
    }
}

impl Default for FlusherConfig {
    fn default() -> Self {
        Self {
            stream_key: String::new(),
            maxlen: 1_000_000,
            batch: DEFAULT_BATCH,
            interval: DEFAULT_INTERVAL,
        }
    }
}

/// Spawns the flusher. The handle must be registered in the supervised
/// `handles` vec so shutdown joins it.
pub fn spawn(
    redis: RedisConnection,
    mut receiver: mpsc::Receiver<proto::Event>,
    metrics: Arc<EmitterMetrics>,
    config: FlusherConfig,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut pending: Vec<proto::Event> = Vec::with_capacity(config.batch);
        let mut ticker = tokio::time::interval(config.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                received = receiver.recv() => match received {
                    Some(event) => {
                        pending.push(event);
                        if pending.len() >= config.batch {
                            flush(&redis, &config, &mut pending, &metrics).await;
                        }
                    }
                    None => break,
                },
                _ = ticker.tick() => {
                    if !pending.is_empty() {
                        flush(&redis, &config, &mut pending, &metrics).await;
                    }
                }
                _ = cancel.cancelled() => break,
            }
        }

        // The reason this handle is joined: everything still buffered is lost
        // unless it is written here.
        receiver.close();
        while let Ok(event) = receiver.try_recv() {
            pending.push(event);
        }
        if !pending.is_empty() {
            let remaining = pending.len() as u64;
            match tokio::time::timeout(
                Duration::from_secs(5),
                flush(&redis, &config, &mut pending, &metrics),
            )
            .await
            {
                Ok(()) => {}
                Err(_) => {
                    warn!("analytics final flush timed out; {remaining} events dropped");
                    metrics.record_drops(DropReason::FlushTimeout, remaining);
                }
            }
        }
    })
}

async fn flush(
    redis: &RedisConnection,
    config: &FlusherConfig,
    pending: &mut Vec<proto::Event>,
    metrics: &EmitterMetrics,
) {
    if pending.is_empty() {
        return;
    }
    let batch = std::mem::take(pending);
    let count = batch.len() as u64;

    let mut lines = Vec::with_capacity(batch.len());
    for event in &batch {
        match to_json_line(event) {
            Ok(line) => lines.push(line),
            Err(error) => {
                // A single unserializable event must not poison the batch.
                warn!("dropping unserializable analytics event: {error}");
                metrics.record_drop(DropReason::Rejected);
            }
        }
    }
    if lines.is_empty() {
        return;
    }

    let payload = lines.join("\n");
    let mut connection = redis.clone();
    // MAXLEN ~ rather than untrimmed: bounded loss is acceptable for
    // analytics, unbounded Valkey growth is not. This deliberately differs
    // from the executor command stream, where trimming could discard a pending
    // entry that still needs delivery.
    let result: redis::RedisResult<String> = connection
        .xadd_maxlen(
            &config.stream_key,
            redis::streams::StreamMaxlen::Approx(config.maxlen),
            "*",
            &[("data", payload.as_str())],
        )
        .await;

    if let Err(error) = result {
        warn!("analytics XADD failed; {count} events dropped: {error}");
        metrics.record_drops(DropReason::BufferFull, count);
    }
}

/// Splits a stream entry's payload back into events.
///
/// One entry carries a whole batch, so the exporter needs the inverse of the
/// join above.
pub fn split_entry(payload: &str) -> impl Iterator<Item = &str> {
    payload.split('\n').filter(|line| !line.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_batch_round_trips_through_one_entry() {
        let lines = vec!["{\"a\":1}", "{\"a\":2}", "{\"a\":3}"];
        let payload = lines.join("\n");
        let restored: Vec<&str> = split_entry(&payload).collect();
        assert_eq!(restored, lines);
    }

    #[test]
    fn splitting_ignores_empty_lines() {
        assert_eq!(split_entry("").count(), 0);
        assert_eq!(split_entry("\n\n").count(), 0);
        assert_eq!(split_entry("{\"a\":1}\n").count(), 1);
    }

    #[test]
    fn config_reads_bounded_defaults() {
        let config = FlusherConfig::from_env("k".to_owned());
        assert_eq!(config.stream_key, "k");
        assert!(config.maxlen > 0, "the stream must always be bounded");
        assert!(config.batch > 0);
    }
}
