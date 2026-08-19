//! The non-durable websocket export path.
//!
//! Deliberately different from the durable path, and the asymmetry is the
//! point: at roughly 95x the volume of game events, durably buffering every
//! websocket message would dominate Valkey cost for data that does not justify
//! it. So this buffers in memory and accepts loss on a crash.
//!
//! What it must NOT do is lose data on a *graceful* stop. A task that is
//! cancelled but never joined would silently drop its final batch, which is
//! exactly why this is registered in the supervised handles rather than
//! spawned and forgotten.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::batch::{BatchLimits, BufferedEvent, EventBatcher};
use super::emitter::{DropReason, EmitterMetrics};
use super::exporter::{ExportTarget, write_batch};
use super::object_store::ObjectStore;

/// Bounded intake. `try_send` and drop-on-full, never a blocking send: a
/// websocket handler must not wait on analytics.
#[derive(Clone)]
pub struct WsEventSink {
    sender: mpsc::Sender<BufferedEvent>,
    metrics: Arc<EmitterMetrics>,
}

impl WsEventSink {
    pub fn record(&self, event: BufferedEvent) -> bool {
        match self.sender.try_send(event) {
            Ok(()) => true,
            Err(_) => {
                self.metrics.record_drop(DropReason::BufferFull);
                false
            }
        }
    }

    pub fn metrics(&self) -> Arc<EmitterMetrics> {
        self.metrics.clone()
    }
}

pub struct WsExporterConfig {
    pub limits: BatchLimits,
    pub target: ExportTarget,
    pub channel_capacity: usize,
    /// Bound on the final flush, so a stuck upload cannot delay task exit past
    /// the host's shutdown budget.
    pub flush_timeout: Duration,
    /// Fraction of sessions to record, 0.0-1.0.
    ///
    /// Sampling is deterministic per session rather than per event: a sampled
    /// session is recorded COMPLETELY, so a funnel through it stays intact.
    /// Random per-event sampling would silently break every per-session
    /// analysis while looking like it worked.
    pub sample_rate: f64,
}

/// Whether a session is in the sample.
///
/// A stable hash of the session id, so the decision is identical on every task
/// and across restarts — the same session is never half-recorded.
pub fn is_sampled(session_id: &str, sample_rate: f64) -> bool {
    if sample_rate >= 1.0 {
        return true;
    }
    if sample_rate <= 0.0 {
        return false;
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::hash::Hash::hash(&session_id, &mut hasher);
    let hash = std::hash::Hasher::finish(&hasher);
    // Map into [0,1) and compare. u64::MAX + 1 as f64 avoids the edge case
    // where the maximum hash would round to exactly 1.0.
    (hash as f64 / (u64::MAX as f64 + 1.0)) < sample_rate
}

/// Creates the sink and the loop that drains it.
pub fn create(
    store: Arc<dyn ObjectStore>,
    config: WsExporterConfig,
    cancel: CancellationToken,
) -> (WsEventSink, impl std::future::Future<Output = ()> + Send) {
    let (sender, mut receiver) = mpsc::channel(config.channel_capacity.max(1));
    let metrics = Arc::new(EmitterMetrics::default());
    let sink = WsEventSink {
        sender,
        metrics: metrics.clone(),
    };

    let task = async move {
        let mut batcher = EventBatcher::new(config.limits.clone());
        let mut ticker = tokio::time::interval(Duration::from_millis(500));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                received = receiver.recv() => match received {
                    Some(event) => {
                        batcher.push(event);
                        if batcher.should_flush() {
                            flush(&store, &config.target, &mut batcher, &metrics).await;
                        }
                    }
                    None => break,
                },
                _ = ticker.tick() => {
                    if batcher.should_flush() {
                        flush(&store, &config.target, &mut batcher, &metrics).await;
                    }
                }
                _ = cancel.cancelled() => break,
            }
        }

        // The reason this task is joined rather than detached: everything
        // buffered right now is lost unless it is written here.
        receiver.close();
        while let Ok(event) = receiver.try_recv() {
            batcher.push(event);
        }
        if !batcher.is_empty() {
            let pending = batcher.buffered_events() as u64;
            match tokio::time::timeout(
                config.flush_timeout,
                flush(&store, &config.target, &mut batcher, &metrics),
            )
            .await
            {
                Ok(()) => {}
                Err(_) => {
                    warn!("websocket analytics final flush timed out; {pending} events dropped");
                    metrics.record_drops(DropReason::FlushTimeout, pending);
                }
            }
        }
    };

    (sink, task)
}

async fn flush(
    store: &Arc<dyn ObjectStore>,
    target: &ExportTarget,
    batcher: &mut EventBatcher,
    metrics: &EmitterMetrics,
) {
    let files = batcher.drain();
    if files.is_empty() {
        return;
    }
    let dropped: u64 = files.iter().map(|f| f.events.len() as u64).sum();
    if let Err(error) = write_batch(store, target, &files, metrics).await {
        // Non-durable by design: there is no pending list to fall back on, so
        // a failed write is loss, and loss must be counted.
        warn!("websocket analytics batch failed: {error}");
        metrics.record_drops(DropReason::Rejected, dropped);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytics::object_store::PutOutcome;
    use anyhow::Result;
    use async_trait::async_trait;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeStore {
        keys: Mutex<Vec<String>>,
        stall: Mutex<bool>,
    }

    #[async_trait]
    impl ObjectStore for FakeStore {
        async fn put_if_absent(&self, key: &str, _body: Vec<u8>) -> Result<PutOutcome> {
            if *self.stall.lock().unwrap() {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            self.keys.lock().unwrap().push(key.to_owned());
            Ok(PutOutcome::Written)
        }
    }

    fn config(limits: BatchLimits) -> WsExporterConfig {
        WsExporterConfig {
            limits,
            target: ExportTarget {
                dataset: "websocket-events".to_owned(),
                host: "use1-1".to_owned(),
            },
            channel_capacity: 8,
            flush_timeout: Duration::from_millis(200),
            sample_rate: 1.0,
        }
    }

    fn event(id: u32) -> BufferedEvent {
        BufferedEvent {
            line: format!("{{\"n\":{id}}}"),
            date: "2026-08-19".to_owned(),
            cursor: format!("{id:05}"),
        }
    }

    fn eager() -> BatchLimits {
        BatchLimits {
            max_batch_age: Duration::from_secs(3600),
            max_buffer_bytes: 1_000_000,
            max_buffer_events: 1_000_000,
            max_events_per_file: 1_000,
            max_bytes_per_file: 1_000_000,
        }
    }

    /// The property that makes this safe to run on every task: a graceful stop
    /// writes what is buffered.
    #[tokio::test]
    async fn a_graceful_shutdown_flushes_the_buffer() {
        let fake = Arc::new(FakeStore::default());
        let store: Arc<dyn ObjectStore> = fake.clone();
        let cancel = CancellationToken::new();
        let (sink, task) = create(store, config(eager()), cancel.clone());
        let handle = tokio::spawn(task);

        for id in 0..5 {
            assert!(sink.record(event(id)));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(fake.keys.lock().unwrap().is_empty(), "not flushed yet");

        cancel.cancel();
        handle.await.unwrap();
        assert_eq!(
            fake.keys.lock().unwrap().len(),
            1,
            "the final flush must write"
        );
    }

    /// A full channel drops and counts without blocking the caller.
    #[tokio::test]
    async fn a_full_channel_drops_rather_than_blocking_the_websocket() {
        let store: Arc<dyn ObjectStore> = Arc::new(FakeStore::default());
        let cancel = CancellationToken::new();
        let (sink, _task) = create(store, config(eager()), cancel);

        let mut accepted = 0;
        let started = std::time::Instant::now();
        for id in 0..200 {
            if sink.record(event(id)) {
                accepted += 1;
            }
        }
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "record must never block"
        );
        assert!(accepted <= 8, "the bound must hold");
        assert!(sink.metrics().dropped(DropReason::BufferFull) > 0);
    }

    /// A stuck upload must not hold the task past its budget; the remainder is
    /// dropped and counted instead.
    #[tokio::test]
    async fn a_stalled_final_flush_is_bounded_and_counted() {
        let fake = Arc::new(FakeStore::default());
        *fake.stall.lock().unwrap() = true;
        let store: Arc<dyn ObjectStore> = fake.clone();
        let cancel = CancellationToken::new();
        let (sink, task) = create(store, config(eager()), cancel.clone());
        let metrics = sink.metrics();
        let handle = tokio::spawn(task);

        sink.record(event(1));
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("shutdown must not hang on a stalled upload")
            .unwrap();
        assert_eq!(metrics.dropped(DropReason::FlushTimeout), 1);
    }

    #[tokio::test]
    async fn reaching_a_cap_flushes_without_waiting_for_cancellation() {
        let fake = Arc::new(FakeStore::default());
        let store: Arc<dyn ObjectStore> = fake.clone();
        let cancel = CancellationToken::new();
        let (sink, task) = create(
            store,
            config(BatchLimits {
                max_buffer_events: 3,
                ..eager()
            }),
            cancel.clone(),
        );
        let handle = tokio::spawn(task);

        for id in 0..3 {
            sink.record(event(id));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            fake.keys.lock().unwrap().len(),
            1,
            "the cap must trigger a flush"
        );

        cancel.cancel();
        handle.await.unwrap();
    }
}

#[cfg(test)]
mod sampling_tests {
    use super::is_sampled;

    /// A sampled session must be recorded completely, so the decision has to
    /// be stable for a given session id.
    #[test]
    fn sampling_is_deterministic_per_session() {
        for rate in [0.1, 0.5, 0.9] {
            let first = is_sampled("session-abc", rate);
            for _ in 0..20 {
                assert_eq!(is_sampled("session-abc", rate), first, "must not flap");
            }
        }
    }

    #[test]
    fn the_extremes_are_all_or_nothing() {
        assert!(is_sampled("anything", 1.0));
        assert!(is_sampled("anything", 2.0), "above 1 clamps to always");
        assert!(!is_sampled("anything", 0.0));
        assert!(!is_sampled("anything", -1.0), "below 0 clamps to never");
    }

    /// The rate should be roughly honoured across many sessions — this is what
    /// makes it a useful cost knob rather than a coin flip.
    #[test]
    fn the_sample_rate_is_approximately_honoured() {
        let total = 4_000;
        let sampled = (0..total)
            .filter(|index| is_sampled(&format!("session-{index}"), 0.25))
            .count();
        let ratio = sampled as f64 / total as f64;
        assert!(
            (0.20..=0.30).contains(&ratio),
            "expected roughly 25%, got {ratio}"
        );
    }
}
