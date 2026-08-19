//! The in-process analytics emitter.
//!
//! Invariant I1 of the PRD: no analytics path may block, await on, or apply
//! backpressure to a gameplay or websocket task. Every method here is
//! non-blocking and sheds load rather than growing, because the alternatives
//! are both worse — an unbounded channel OOMs the container that also holds
//! game state, and a blocking send turns an S3 outage into gameplay latency.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;

use super::proto;

/// Why an event was dropped. Every drop is counted, because silent loss is a
/// defect (invariant I9) — a dashboard that quietly undercounts is worse than
/// one that shows a gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// The bounded channel was full: the server shed load to protect gameplay.
    BufferFull,
    /// The Valkey stream aged out before the exporter drained it.
    Trimmed,
    /// A batch was rejected repeatedly and discarded rather than wedging.
    Rejected,
    /// A shutdown flush did not finish inside its budget.
    FlushTimeout,
}

impl DropReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BufferFull => "buffer_full",
            Self::Trimmed => "trimmed",
            Self::Rejected => "rejected",
            Self::FlushTimeout => "flush_timeout",
        }
    }
}

#[derive(Debug, Default)]
pub struct EmitterMetrics {
    emitted: AtomicU64,
    dropped_buffer_full: AtomicU64,
    dropped_trimmed: AtomicU64,
    dropped_rejected: AtomicU64,
    dropped_flush_timeout: AtomicU64,
}

impl EmitterMetrics {
    pub fn emitted(&self) -> u64 {
        self.emitted.load(Ordering::Relaxed)
    }

    pub fn dropped(&self, reason: DropReason) -> u64 {
        self.counter(reason).load(Ordering::Relaxed)
    }

    pub fn total_dropped(&self) -> u64 {
        self.dropped(DropReason::BufferFull)
            + self.dropped(DropReason::Trimmed)
            + self.dropped(DropReason::Rejected)
            + self.dropped(DropReason::FlushTimeout)
    }

    pub fn record_drop(&self, reason: DropReason) {
        self.counter(reason).fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_drops(&self, reason: DropReason, count: u64) {
        self.counter(reason).fetch_add(count, Ordering::Relaxed);
    }

    fn counter(&self, reason: DropReason) -> &AtomicU64 {
        match reason {
            DropReason::BufferFull => &self.dropped_buffer_full,
            DropReason::Trimmed => &self.dropped_trimmed,
            DropReason::Rejected => &self.dropped_rejected,
            DropReason::FlushTimeout => &self.dropped_flush_timeout,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EmitterConfig {
    /// Bounded channel capacity. Sized so the worst case is a bounded memory
    /// cost rather than an unbounded one.
    pub buffer: usize,
}

impl Default for EmitterConfig {
    fn default() -> Self {
        Self {
            buffer: std::env::var("SNAKETRON_ANALYTICS_BUFFER")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(16_384),
        }
    }
}

/// Cheap to clone; hand one to every call site that emits.
#[derive(Clone)]
pub struct AnalyticsEmitter {
    sender: mpsc::Sender<proto::Event>,
    metrics: Arc<EmitterMetrics>,
}

impl AnalyticsEmitter {
    /// Creates the emitter and returns the receiver the flusher drains.
    pub fn new(config: EmitterConfig) -> (Self, mpsc::Receiver<proto::Event>) {
        let (sender, receiver) = mpsc::channel(config.buffer.max(1));
        (
            Self {
                sender,
                metrics: Arc::new(EmitterMetrics::default()),
            },
            receiver,
        )
    }

    pub fn metrics(&self) -> Arc<EmitterMetrics> {
        self.metrics.clone()
    }

    /// Records an event, or drops it.
    ///
    /// Never blocks and never awaits: `try_send` is the whole point. Returns
    /// whether the event was accepted, so a caller may count locally, but no
    /// caller should ever branch on it in a way that affects gameplay.
    pub fn emit(&self, event: proto::Event) -> bool {
        match self.sender.try_send(event) {
            Ok(()) => {
                self.metrics.emitted.fetch_add(1, Ordering::Relaxed);
                true
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics.record_drop(DropReason::BufferFull);
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // Shutdown already drained the flusher. Not an error.
                self.metrics.record_drop(DropReason::BufferFull);
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(id: &str) -> proto::Event {
        proto::Event {
            event_id: id.to_owned(),
            event_name: "guest_created".to_owned(),
            event_version: 1,
            occurred_at_ms: 1,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn emitting_delivers_to_the_flusher() {
        let (emitter, mut rx) = AnalyticsEmitter::new(EmitterConfig { buffer: 4 });
        assert!(emitter.emit(event("a")));
        assert_eq!(rx.recv().await.unwrap().event_id, "a");
        assert_eq!(emitter.metrics().emitted(), 1);
    }

    /// The load-bearing property: a full buffer drops and counts, and the call
    /// still returns immediately. Anything else would let analytics reach into
    /// gameplay latency.
    #[tokio::test]
    async fn a_full_buffer_drops_and_counts_without_blocking() {
        let (emitter, _rx) = AnalyticsEmitter::new(EmitterConfig { buffer: 2 });
        assert!(emitter.emit(event("a")));
        assert!(emitter.emit(event("b")));

        // The next 100 must all be refused, promptly.
        let started = std::time::Instant::now();
        for index in 0..100 {
            assert!(!emitter.emit(event(&format!("overflow-{index}"))));
        }
        assert!(
            started.elapsed() < std::time::Duration::from_millis(100),
            "emit must never block"
        );

        let metrics = emitter.metrics();
        assert_eq!(metrics.emitted(), 2);
        assert_eq!(metrics.dropped(DropReason::BufferFull), 100);
        assert_eq!(metrics.total_dropped(), 100);
    }

    /// A closed receiver is shutdown, not an error: the process is going away
    /// and a late emit must not panic a gameplay task.
    #[tokio::test]
    async fn emitting_after_shutdown_is_counted_not_fatal() {
        let (emitter, rx) = AnalyticsEmitter::new(EmitterConfig { buffer: 4 });
        drop(rx);
        assert!(!emitter.emit(event("a")));
        assert_eq!(emitter.metrics().total_dropped(), 1);
    }

    #[test]
    fn drop_reasons_render_as_stable_metric_labels() {
        assert_eq!(DropReason::BufferFull.as_str(), "buffer_full");
        assert_eq!(DropReason::Trimmed.as_str(), "trimmed");
        assert_eq!(DropReason::Rejected.as_str(), "rejected");
        assert_eq!(DropReason::FlushTimeout.as_str(), "flush_timeout");
    }
}
