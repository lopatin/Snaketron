//! The durable export path: Valkey stream -> S3.
//!
//! The rule that makes this at-least-once rather than lossy: entries are
//! acknowledged ONLY after every object for the batch is durably written. A
//! crash between the write and the ack replays the same claimed entries, which
//! reproduce the same object key, which the conditional write refuses — so the
//! replay is a no-op rather than a duplicate.
//!
//! That chain only holds because keys are derived from stream ids, which
//! `XAUTOCLAIM` reproduces exactly. A wall-clock component anywhere in the key
//! would break it silently.

use std::sync::Arc;

use anyhow::Result;
use tracing::{info, warn};

use super::batch::{BatchLimits, BufferedEvent, EventBatcher, PendingFile};
use super::emitter::{DropReason, EmitterMetrics};
use super::object_store::{ObjectStore, PutOutcome, compress, content_hash, object_key};

/// How many consecutive non-retryable failures a batch may cause before it is
/// discarded. Analytics must never wedge itself: a poison batch that blocked
/// the exporter forever would silently stop all export.
const POISON_THRESHOLD: u32 = 5;

pub struct ExportTarget {
    pub dataset: String,
    pub host: String,
}

/// Writes a drained batch, returning the stream cursor that is now safe to
/// acknowledge.
///
/// Returns `Ok(None)` when nothing could be written, so the caller does NOT
/// ack and the entries stay pending for redelivery.
pub async fn write_batch(
    store: &Arc<dyn ObjectStore>,
    target: &ExportTarget,
    files: &[PendingFile],
    metrics: &EmitterMetrics,
) -> Result<Option<String>> {
    let mut highest: Option<String> = None;

    for file in files {
        let body = file.body();
        let hash = content_hash(&body);
        let key = object_key(
            &target.dataset,
            &file.date,
            &target.host,
            &file.first_cursor,
            &file.last_cursor,
            &hash,
        );
        let compressed = compress(&body)?;

        match store.put_if_absent(&key, compressed).await? {
            PutOutcome::Written => {}
            PutOutcome::AlreadyPresent => {
                // The idempotent-replay path. Because the key carries a
                // content hash, an existing key means identical bytes, so this
                // is success rather than a conflict.
                info!("analytics object {key} already present; treating replay as success");
            }
        }

        highest = Some(match highest {
            Some(current) if current >= file.last_cursor => current,
            _ => file.last_cursor.clone(),
        });
    }

    let _ = metrics;
    Ok(highest)
}

/// Tracks repeated failures for one batch so a poison batch is eventually
/// dropped rather than blocking export forever.
#[derive(Debug, Default)]
pub struct PoisonTracker {
    signature: Option<String>,
    failures: u32,
}

impl PoisonTracker {
    /// Records a failure. Returns true when the batch should be discarded.
    pub fn record_failure(&mut self, signature: &str) -> bool {
        if self.signature.as_deref() == Some(signature) {
            self.failures += 1;
        } else {
            self.signature = Some(signature.to_owned());
            self.failures = 1;
        }
        self.failures >= POISON_THRESHOLD
    }

    pub fn record_success(&mut self) {
        self.signature = None;
        self.failures = 0;
    }

    pub fn failures(&self) -> u32 {
        self.failures
    }
}

/// Reports events lost to stream trimming.
///
/// Trim-horizon loss is acceptable for analytics but must never be silent
/// (invariant I9): an operator has to be able to tell "no traffic" from
/// "we dropped it".
pub fn report_discontinuity(metrics: &EmitterMetrics, estimated_gap: u64) {
    if estimated_gap == 0 {
        return;
    }
    warn!("analytics stream discontinuity; approximately {estimated_gap} events were trimmed");
    metrics.record_drops(DropReason::Trimmed, estimated_gap);
}

/// Converts raw stream entries into buffered events, bucketed by event date.
pub fn buffer_entries(
    batcher: &mut EventBatcher,
    entries: impl IntoIterator<Item = (String, String, i64)>,
) {
    for (cursor, line, occurred_at_ms) in entries {
        batcher.push(BufferedEvent {
            date: event_date(occurred_at_ms),
            line,
            cursor,
        });
    }
}

/// `YYYY-MM-DD` in UTC from an epoch-millisecond timestamp.
pub fn event_date(occurred_at_ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(occurred_at_ms)
        .unwrap_or_else(chrono::Utc::now)
        .format("%Y-%m-%d")
        .to_string()
}

pub fn default_limits() -> BatchLimits {
    let parse = |name: &str, fallback: usize| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(fallback)
    };
    BatchLimits {
        max_batch_age: std::time::Duration::from_millis(parse(
            "SNAKETRON_ANALYTICS_MAX_BATCH_AGE_MS",
            300_000,
        ) as u64),
        max_buffer_bytes: parse("SNAKETRON_ANALYTICS_MAX_BUFFER_BYTES", 64 * 1024 * 1024),
        max_buffer_events: parse("SNAKETRON_ANALYTICS_MAX_BUFFER_EVENTS", 100_000),
        max_events_per_file: parse("SNAKETRON_ANALYTICS_MAX_EVENTS_PER_FILE", 50_000),
        max_bytes_per_file: parse("SNAKETRON_ANALYTICS_MAX_BYTES_PER_FILE", 32 * 1024 * 1024),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeStore {
        written: Mutex<Vec<String>>,
        existing: Mutex<Vec<String>>,
        fail_next: Mutex<bool>,
    }

    #[async_trait]
    impl ObjectStore for FakeStore {
        async fn put_if_absent(&self, key: &str, _body: Vec<u8>) -> Result<PutOutcome> {
            if std::mem::replace(&mut *self.fail_next.lock().unwrap(), false) {
                anyhow::bail!("injected failure");
            }
            if self.existing.lock().unwrap().iter().any(|k| k == key) {
                return Ok(PutOutcome::AlreadyPresent);
            }
            self.existing.lock().unwrap().push(key.to_owned());
            self.written.lock().unwrap().push(key.to_owned());
            Ok(PutOutcome::Written)
        }
    }

    fn target() -> ExportTarget {
        ExportTarget {
            dataset: "game-events".to_owned(),
            host: "use1-1".to_owned(),
        }
    }

    fn batch(cursors: &[&str]) -> Vec<PendingFile> {
        let mut batcher = EventBatcher::new(default_limits());
        for cursor in cursors {
            batcher.push(BufferedEvent {
                line: format!("{{\"event_id\":\"{cursor}\"}}"),
                date: "2026-08-19".to_owned(),
                cursor: (*cursor).to_owned(),
            });
        }
        batcher.drain()
    }

    #[tokio::test]
    async fn a_written_batch_reports_its_highest_cursor() {
        let store: Arc<dyn ObjectStore> = Arc::new(FakeStore::default());
        let metrics = EmitterMetrics::default();
        let acked = write_batch(&store, &target(), &batch(&["1-0", "2-0"]), &metrics)
            .await
            .unwrap();
        assert_eq!(acked.as_deref(), Some("2-0"));
    }

    /// The central correctness path: a crash between the write and the ack
    /// replays the same entries, which must produce the same key and be
    /// refused — no duplicate object, and still safe to ack.
    #[tokio::test]
    async fn replaying_a_batch_is_an_idempotent_no_op() {
        let fake = Arc::new(FakeStore::default());
        let store: Arc<dyn ObjectStore> = fake.clone();
        let metrics = EmitterMetrics::default();
        let files = batch(&["1-0", "2-0"]);

        let first = write_batch(&store, &target(), &files, &metrics)
            .await
            .unwrap();
        let second = write_batch(&store, &target(), &files, &metrics)
            .await
            .unwrap();

        assert_eq!(first, second, "a replay must be safe to ack identically");
        assert_eq!(
            fake.written.lock().unwrap().len(),
            1,
            "the replay must not create a second object"
        );
    }

    /// A failed write must NOT yield an ack cursor, so the entries stay
    /// pending and are redelivered.
    #[tokio::test]
    async fn a_failed_write_does_not_produce_an_ack() {
        let fake = Arc::new(FakeStore::default());
        *fake.fail_next.lock().unwrap() = true;
        let store: Arc<dyn ObjectStore> = fake;
        let metrics = EmitterMetrics::default();
        let result = write_batch(&store, &target(), &batch(&["1-0"]), &metrics).await;
        assert!(result.is_err(), "the caller must not ack after a failure");
    }

    #[tokio::test]
    async fn a_batch_spanning_dates_writes_every_file_before_acking() {
        let fake = Arc::new(FakeStore::default());
        let store: Arc<dyn ObjectStore> = fake.clone();
        let metrics = EmitterMetrics::default();

        let mut batcher = EventBatcher::new(default_limits());
        batcher.push(BufferedEvent {
            line: "{\"a\":1}".to_owned(),
            date: "2026-08-19".to_owned(),
            cursor: "1-0".to_owned(),
        });
        batcher.push(BufferedEvent {
            line: "{\"a\":2}".to_owned(),
            date: "2026-08-20".to_owned(),
            cursor: "2-0".to_owned(),
        });
        let files = batcher.drain();

        let acked = write_batch(&store, &target(), &files, &metrics)
            .await
            .unwrap();
        assert_eq!(fake.written.lock().unwrap().len(), 2);
        assert_eq!(acked.as_deref(), Some("2-0"), "ack only the highest cursor");
    }

    #[test]
    fn a_poison_batch_is_discarded_rather_than_wedging_export() {
        let mut tracker = PoisonTracker::default();
        for attempt in 1..POISON_THRESHOLD {
            assert!(
                !tracker.record_failure("sig"),
                "attempt {attempt} must retry"
            );
        }
        assert!(
            tracker.record_failure("sig"),
            "must give up at the threshold"
        );
    }

    #[test]
    fn a_different_batch_resets_the_poison_counter() {
        let mut tracker = PoisonTracker::default();
        tracker.record_failure("a");
        tracker.record_failure("a");
        assert_eq!(tracker.failures(), 2);
        tracker.record_failure("b");
        assert_eq!(tracker.failures(), 1);
        tracker.record_success();
        assert_eq!(tracker.failures(), 0);
    }

    /// Trim loss is acceptable but must be counted; silence would make "no
    /// traffic" and "we lost it" indistinguishable.
    #[test]
    fn a_discontinuity_is_counted_not_silent() {
        let metrics = EmitterMetrics::default();
        report_discontinuity(&metrics, 0);
        assert_eq!(metrics.dropped(DropReason::Trimmed), 0);
        report_discontinuity(&metrics, 250);
        assert_eq!(metrics.dropped(DropReason::Trimmed), 250);
    }

    /// The partition is UTC, and must stay UTC: a fleet spanning regions would
    /// otherwise write the same instant into two different `dt=` buckets
    /// depending on each host's local zone, and Athena would read a day that
    /// silently means something different per file.
    ///
    /// The two constants straddle UTC midnight while sharing a local calendar
    /// day in any zone west of UTC, so a switch to local time turns the
    /// `assert_ne` below into a failure rather than a silent behaviour change.
    #[test]
    fn events_are_bucketed_by_utc_event_date() {
        // 2026-08-20T23:59:59.999Z and one millisecond later.
        let before = event_date(1_787_270_399_999);
        let after = event_date(1_787_270_400_000);

        // Pin the values, not just the fact that they differ: "they changed"
        // also holds for a local-time bucket that changed at the wrong instant.
        assert_eq!(before, "2026-08-20");
        assert_eq!(after, "2026-08-21");
        assert_ne!(before, after, "midnight must change the bucket");
    }

    #[test]
    fn buffering_assigns_dates_from_event_time() {
        let mut batcher = EventBatcher::new(default_limits());
        buffer_entries(
            &mut batcher,
            vec![
                ("1-0".to_owned(), "{\"a\":1}".to_owned(), 1_787_270_399_999),
                ("2-0".to_owned(), "{\"a\":2}".to_owned(), 1_787_270_400_000),
            ],
        );
        let files = batcher.drain();
        assert_eq!(
            files.len(),
            2,
            "different event dates must not share a file"
        );
    }
}

#[cfg(test)]
mod key_tests {
    use crate::redis_keys::RedisKeys;

    /// One hash tag per region keeps every analytics key in a single slot,
    /// matching the convention the other cluster families use.
    #[test]
    fn the_stream_key_is_region_scoped_and_hash_tagged() {
        let key = RedisKeys::analytics_events("use1");
        assert_eq!(key, "snaketron:{snaketron:analytics:use1}:events:v1");
        assert_ne!(key, RedisKeys::analytics_events("euw1"));
        let tag_open = key.find('{').unwrap();
        let tag_close = key.find('}').unwrap();
        assert!(tag_close > tag_open, "the hash tag must be well formed");
    }

    /// It must not collide with the game bus: that stream is a different
    /// thing with different retention and no consumer group.
    #[test]
    fn the_analytics_stream_is_not_a_game_bus_stream() {
        let analytics = RedisKeys::analytics_events("use1");
        for partition in 0..10 {
            assert_ne!(analytics, RedisKeys::stream_events(partition));
        }
    }

    #[test]
    fn the_consumer_group_is_region_scoped() {
        assert_eq!(
            RedisKeys::analytics_exporter_group("use1"),
            "snaketron-analytics-exporter:use1"
        );
        assert_ne!(
            RedisKeys::analytics_exporter_group("use1"),
            RedisKeys::analytics_exporter_group("euw1")
        );
    }
}
