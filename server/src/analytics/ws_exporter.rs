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
use super::event::{EventIdentity, EventOrigin, envelope_at, to_json_line};
use super::exporter::{ExportTarget, event_date, write_batch};
use super::object_store::ObjectStore;
use super::proto;
use super::ws_sink::Account;

/// Which way a frame was travelling.
///
/// The column values live here rather than at the two hooks so the inbound and
/// outbound halves cannot spell them differently.
///
/// Crate-private, together with [`WsFrameRecord`] and [`WsEventSink::record`],
/// so `ws_sink`'s two hooks stay the only way to put a frame on this channel:
/// a caller that could name a direction itself could label an inbound frame
/// outbound, and nothing downstream would notice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Direction {
    Inbound,
    Outbound,
}

impl Direction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Inbound => "in",
            Self::Outbound => "out",
        }
    }
}

/// One frame, as much of it as the websocket task is willing to pay to
/// describe.
///
/// This — not a finished event — is what crosses the channel, because
/// everything needed to turn it into one (the envelope, the protobuf round
/// trip, the JSON) is work the gameplay task must not do. The two fields that
/// genuinely cannot be recovered later are carried by value: `occurred_at_ms`
/// because a clock read on the drain would report the drain's time as the
/// frame's, and `origin` because it is per-task state the drain would
/// otherwise have to look up globally.
///
/// `message_type` is `&'static str` rather than an owned name so a frame costs
/// no allocation at all: the inbound hook has the variant's own name and the
/// outbound hook interns the wire tag against that same set.
pub(crate) struct WsFrameRecord {
    pub(crate) origin: Arc<EventOrigin>,
    pub(crate) session_id: Option<Arc<str>>,
    /// The account the connection had authenticated as when the frame passed,
    /// or `None` for a frame that arrived before authentication. Copied by
    /// value because it is two machine words, so carrying it costs no
    /// allocation and no refcount.
    pub(crate) account: Option<Account>,
    pub(crate) direction: Direction,
    pub(crate) message_type: &'static str,
    pub(crate) byte_len: usize,
    pub(crate) game_id: Option<i64>,
    pub(crate) occurred_at_ms: i64,
}

impl WsFrameRecord {
    /// The projection. Pure: origin plus hook data in, one event out.
    fn into_event(self) -> proto::Event {
        envelope_at(
            &self.origin,
            EventIdentity {
                session_id: self.session_id.map(|id| id.to_string()),
                // Absent, not zero, when there is no account: a placeholder id
                // would join to a real row belonging to someone else.
                user_id: self.account.map(|account| i64::from(account.user_id)),
                // QUERY AUTHORS: `is_guest` is a bare proto3 bool, so it has no
                // presence and cannot say "unknown". A frame with no account is
                // therefore stamped `true`, read as "not known to be a
                // registered account". That keeps `is_guest = false` a positive
                // claim backed by a verified account, so a pre-authentication
                // frame can never be counted as registered-user activity. The
                // cost is the other direction: rows where `user_id` is null are
                // guest-flagged but have no account at all, so any guest-vs-
                // registered split must filter on `user_id IS NOT NULL` first.
                is_guest: self.account.is_none_or(|account| account.is_guest),
                // Unlike the guest flag this defaults to FALSE when unknown,
                // and deliberately: a row with no account cannot be attributed
                // to a load test either, and marking unknown traffic synthetic
                // would delete real traffic from every filtered view.
                is_stress_test: self.account.is_some_and(|account| account.is_stress_test),
                ..Default::default()
            },
            // Type and size only: message bodies carry chat and tokens and are
            // never recorded.
            proto::event::Payload::WebsocketMessage(proto::WebsocketMessage {
                direction: self.direction.as_str().to_owned(),
                message_type: self.message_type.to_owned(),
                byte_len: i64::try_from(self.byte_len).unwrap_or(i64::MAX),
                game_id: self.game_id,
            }),
            self.occurred_at_ms,
        )
    }

    /// Projects the frame and serializes it — the whole cost the hooks were
    /// relieved of, run here on the exporter's task.
    fn project(self) -> anyhow::Result<BufferedEvent> {
        let event = self.into_event();
        Ok(BufferedEvent {
            line: to_json_line(&event)?,
            date: event_date(event.occurred_at_ms),
            // No crash stability is needed here (R5.6) — the buffer is lost on
            // a crash anyway — but the cursor still has to sort in write order
            // within one host's day, which the event's UUIDv7 already does.
            cursor: event.event_id,
        })
    }
}

/// Bounded intake. `try_send` and drop-on-full, never a blocking send: a
/// websocket handler must not wait on analytics.
#[derive(Clone)]
pub struct WsEventSink {
    sender: mpsc::Sender<WsFrameRecord>,
    metrics: Arc<EmitterMetrics>,
}

impl WsEventSink {
    pub(crate) fn record(&self, frame: WsFrameRecord) -> bool {
        match self.sender.try_send(frame) {
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

impl WsExporterConfig {
    /// The R5 knobs, read with the same parse idiom as
    /// `exporter::default_limits` so an unparseable value falls back to the
    /// default rather than refusing to start a game server over analytics.
    pub fn from_env(target: ExportTarget) -> Self {
        Self::from_lookup(target, |name| std::env::var(name).ok())
    }

    /// The parsing, with the environment injected.
    ///
    /// Split out so the defaults and the overrides can both be asserted
    /// without mutating process environment that every other test shares.
    fn from_lookup(target: ExportTarget, lookup: impl Fn(&str) -> Option<String>) -> Self {
        let parse = |name: &str, fallback: usize| {
            lookup(name)
                .and_then(|value| value.parse().ok())
                .unwrap_or(fallback)
        };
        Self {
            limits: BatchLimits {
                max_batch_age: Duration::from_millis(parse(
                    "SNAKETRON_WS_EXPORT_MAX_BATCH_AGE_MS",
                    300_000,
                ) as u64),
                // Half the durable path's byte budget (R5.2): this buffer is
                // pure process memory in the container that also holds game
                // state, with no Valkey behind it to absorb a burst.
                max_buffer_bytes: parse("SNAKETRON_WS_EXPORT_MAX_BUFFER_BYTES", 32 * 1024 * 1024),
                max_buffer_events: parse("SNAKETRON_WS_EXPORT_MAX_BUFFER_EVENTS", 100_000),
                // How a flush is split into objects, which R5.2 does not make
                // configurable — it bounds the buffer, not the file. Matching
                // the durable path keeps one object shape in the raw tier for
                // both datasets rather than inventing a second.
                max_events_per_file: 50_000,
                max_bytes_per_file: 32 * 1024 * 1024,
            },
            target,
            channel_capacity: parse("SNAKETRON_WS_EXPORT_BUFFER", 65_536),
            flush_timeout: Duration::from_millis(parse(
                "SNAKETRON_WS_EXPORT_FLUSH_TIMEOUT_MS",
                5_000,
            ) as u64),
            // A non-finite rate would make `is_sampled` answer no for every
            // session, which is indistinguishable from a dead pipeline. Fall
            // back to recording everything so a typo is visible in the data
            // volume rather than in its absence.
            sample_rate: lookup("SNAKETRON_WS_EXPORT_SAMPLE")
                .and_then(|value| value.parse::<f64>().ok())
                .filter(|rate| rate.is_finite())
                .unwrap_or(1.0),
        }
    }
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
///
/// The loop owns the projection as well as the batching: a frame arrives as
/// the little that the websocket task could afford to say about it, and every
/// cost of turning that into a row is paid here.
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
                    Some(frame) => {
                        push_projected(&mut batcher, frame, &metrics);
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
        while let Ok(frame) = receiver.try_recv() {
            push_projected(&mut batcher, frame, &metrics);
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

/// Projects one frame and buffers it, or counts the loss.
///
/// A frame that cannot be serialized must not stop the ones behind it, and the
/// loss must be counted rather than silent (invariant I9).
fn push_projected(batcher: &mut EventBatcher, frame: WsFrameRecord, metrics: &EmitterMetrics) {
    match frame.project() {
        Ok(event) => batcher.push(event),
        Err(error) => {
            warn!("dropping unserializable websocket analytics event: {error}");
            metrics.record_drop(DropReason::Rejected);
        }
    }
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

    fn test_origin() -> Arc<EventOrigin> {
        Arc::new(EventOrigin {
            environment: "test".to_owned(),
            region: "use1".to_owned(),
            aws_region: "us-east-1".to_owned(),
            instance_id: "1:boot".to_owned(),
        })
    }

    /// One frame in the shape the hooks hand over: nothing projected yet.
    fn frame(id: u32) -> WsFrameRecord {
        WsFrameRecord {
            origin: test_origin(),
            session_id: Some(Arc::from(format!("s-{id}").as_str())),
            account: Some(Account {
                user_id: 1_000 + id as i32,
                is_guest: false,
                is_stress_test: false,
            }),
            direction: Direction::Outbound,
            message_type: "GameEvent",
            byte_len: 64,
            game_id: Some(i64::from(id)),
            occurred_at_ms: 1_755_000_000_000,
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
            assert!(sink.record(frame(id)));
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
            if sink.record(frame(id)) {
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

        sink.record(frame(1));
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
            sink.record(frame(id));
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
mod config_tests {
    use super::*;

    fn target() -> ExportTarget {
        ExportTarget {
            dataset: "websocket-events".to_owned(),
            host: "use1-1".to_owned(),
        }
    }

    fn nothing_set(_: &str) -> Option<String> {
        None
    }

    /// The PRD's defaults (R5.1, R5.2, R5.5, R5.7), pinned by value: an
    /// unconfigured deployment is the one every deployment starts as, and a
    /// silently changed default would move the loss window or the cost.
    #[test]
    fn an_unconfigured_exporter_uses_the_documented_defaults() {
        let config = WsExporterConfig::from_lookup(target(), nothing_set);
        assert_eq!(config.channel_capacity, 65_536);
        assert_eq!(config.limits.max_batch_age, Duration::from_millis(300_000));
        assert_eq!(config.limits.max_buffer_bytes, 33_554_432);
        assert_eq!(config.limits.max_buffer_events, 100_000);
        assert_eq!(config.flush_timeout, Duration::from_millis(5_000));
        assert_eq!(config.sample_rate, 1.0);
    }

    #[test]
    fn every_knob_is_read_from_the_environment() {
        let config = WsExporterConfig::from_lookup(target(), |name| {
            Some(
                match name {
                    "SNAKETRON_WS_EXPORT_BUFFER" => "128",
                    "SNAKETRON_WS_EXPORT_MAX_BATCH_AGE_MS" => "1000",
                    "SNAKETRON_WS_EXPORT_MAX_BUFFER_BYTES" => "2048",
                    "SNAKETRON_WS_EXPORT_MAX_BUFFER_EVENTS" => "7",
                    "SNAKETRON_WS_EXPORT_FLUSH_TIMEOUT_MS" => "250",
                    "SNAKETRON_WS_EXPORT_SAMPLE" => "0.25",
                    other => panic!("unexpected variable {other}"),
                }
                .to_owned(),
            )
        });
        assert_eq!(config.channel_capacity, 128);
        assert_eq!(config.limits.max_batch_age, Duration::from_millis(1_000));
        assert_eq!(config.limits.max_buffer_bytes, 2_048);
        assert_eq!(config.limits.max_buffer_events, 7);
        assert_eq!(config.flush_timeout, Duration::from_millis(250));
        assert_eq!(config.sample_rate, 0.25);
    }

    /// Analytics must never keep a game server from starting, so garbage falls
    /// back rather than failing.
    #[test]
    fn unparseable_values_fall_back_to_the_defaults() {
        let config = WsExporterConfig::from_lookup(target(), |_| Some("banana".to_owned()));
        assert_eq!(config.channel_capacity, 65_536);
        assert_eq!(config.limits.max_buffer_bytes, 33_554_432);
        assert_eq!(config.sample_rate, 1.0);
    }

    /// A NaN rate compares false against every threshold, so `is_sampled`
    /// would answer no for every session — a silently dead pipeline that looks
    /// exactly like no traffic.
    #[test]
    fn a_non_finite_sample_rate_records_everything_rather_than_nothing() {
        let config = WsExporterConfig::from_lookup(target(), |name| {
            (name == "SNAKETRON_WS_EXPORT_SAMPLE").then(|| "NaN".to_owned())
        });
        assert_eq!(config.sample_rate, 1.0);
        assert!(is_sampled("any-session", config.sample_rate));
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

#[cfg(test)]
mod projection_tests {
    use super::*;

    fn origin() -> Arc<EventOrigin> {
        Arc::new(EventOrigin {
            environment: "test".to_owned(),
            region: "use1".to_owned(),
            aws_region: "us-east-1".to_owned(),
            instance_id: "42:boot".to_owned(),
        })
    }

    fn record(
        session_id: Option<&str>,
        direction: Direction,
        message_type: &'static str,
        byte_len: usize,
        game_id: Option<i64>,
    ) -> WsFrameRecord {
        with_account(None, session_id, direction, message_type, byte_len, game_id)
    }

    fn with_account(
        account: Option<Account>,
        session_id: Option<&str>,
        direction: Direction,
        message_type: &'static str,
        byte_len: usize,
        game_id: Option<i64>,
    ) -> WsFrameRecord {
        WsFrameRecord {
            origin: origin(),
            session_id: session_id.map(Arc::from),
            account,
            direction,
            message_type,
            byte_len,
            game_id,
            occurred_at_ms: 1_755_000_000_000,
        }
    }

    fn payload(event: &proto::Event) -> &proto::WebsocketMessage {
        match event.payload.as_ref().expect("a payload") {
            proto::event::Payload::WebsocketMessage(message) => message,
            other => panic!("expected a websocket message, got {other:?}"),
        }
    }

    #[test]
    fn a_projected_event_carries_direction_type_size_and_game() {
        let event = record(
            Some("s_abc"),
            Direction::Outbound,
            "GameEvent",
            4096,
            Some(77),
        )
        .into_event();

        assert_eq!(event.event_name, "websocket_message");
        assert_eq!(
            event.identity.as_ref().unwrap().session_id.as_deref(),
            Some("s_abc")
        );
        let message = payload(&event);
        assert_eq!(message.direction, "out");
        assert_eq!(message.message_type, "GameEvent");
        assert_eq!(message.byte_len, 4096);
        assert_eq!(message.game_id, Some(77));
    }

    /// The two directions must be distinguishable in the table, and the labels
    /// must come from one place.
    #[test]
    fn the_two_directions_project_distinct_labels() {
        let inbound = record(None, Direction::Inbound, "PlayerReady", 12, None).into_event();
        let outbound = record(None, Direction::Outbound, "PlayerReady", 12, None).into_event();
        assert_eq!(payload(&inbound).direction, "in");
        assert_eq!(payload(&outbound).direction, "out");
        assert_ne!(
            payload(&inbound).direction,
            payload(&outbound).direction,
            "a frame's direction must be readable from the row"
        );
    }

    /// A pre-authentication frame has no session to attribute, and must say so
    /// rather than inventing one that joins to nothing.
    #[test]
    fn an_unauthenticated_frame_carries_no_session() {
        let event = record(None, Direction::Inbound, "Authenticate", 200, None).into_event();
        assert!(event.identity.as_ref().unwrap().session_id.is_none());
        assert!(
            payload(&event).game_id.is_none(),
            "an absent game must be absent, not zero"
        );
    }

    /// The join this column exists for: a frame recorded after authentication
    /// has to name the account, or the row is unattributable however it is
    /// queried.
    #[test]
    fn an_authenticated_frame_names_the_account_behind_it() {
        let event = with_account(
            Some(Account {
                user_id: 4242,
                is_guest: false,
                is_stress_test: false,
            }),
            Some("s_auth"),
            Direction::Inbound,
            "PlayerReady",
            12,
            Some(9),
        )
        .into_event();

        let identity = event.identity.as_ref().expect("an identity");
        assert_eq!(identity.user_id, Some(4242));
        assert!(
            !identity.is_guest,
            "a verified registered account is the one case that may claim this"
        );
    }

    /// A guest is a real account with a real id — the flag is the only thing
    /// separating it from a registered one, so it has to survive projection
    /// rather than falling back to the proto default.
    #[test]
    fn a_guest_account_is_named_and_flagged_as_a_guest() {
        let event = with_account(
            Some(Account {
                user_id: 1001,
                is_guest: true,
                is_stress_test: false,
            }),
            Some("s_guest"),
            Direction::Outbound,
            "GameEvent",
            64,
            None,
        )
        .into_event();

        let identity = event.identity.as_ref().expect("an identity");
        assert_eq!(identity.user_id, Some(1001));
        assert!(identity.is_guest);
    }

    /// Honesty about the handshake: those frames genuinely predate any
    /// account, so `user_id` must be ABSENT rather than zero — a zero would
    /// join to whatever row happens to hold that id.
    ///
    /// `is_guest` has no presence and so cannot say "unknown"; `true` is the
    /// deliberate choice, because `false` would read as a verified registered
    /// account on a row that has no account at all.
    #[test]
    fn a_pre_authentication_frame_carries_no_account_and_cannot_read_as_registered() {
        let event = record(None, Direction::Inbound, "Authenticate", 200, None).into_event();

        let identity = event.identity.as_ref().expect("an identity");
        assert_eq!(
            identity.user_id, None,
            "an absent account must be absent, not zero"
        );
        assert!(
            identity.is_guest,
            "a row with no account must not present itself as a registered user"
        );
    }

    /// The invariant a query author can lean on, over every account state a
    /// frame can be in: `is_guest = false` is a positive claim, and it is only
    /// ever made where an account is actually named.
    #[test]
    fn is_guest_is_false_only_on_rows_that_name_an_account() {
        for account in [
            None,
            Some(Account {
                user_id: 5,
                is_guest: true,
                is_stress_test: false,
            }),
            Some(Account {
                user_id: 6,
                is_guest: false,
                is_stress_test: false,
            }),
        ] {
            let event =
                with_account(account, None, Direction::Outbound, "GameEvent", 1, None).into_event();
            let identity = event.identity.as_ref().expect("an identity");
            assert!(
                identity.is_guest || identity.user_id.is_some(),
                "is_guest=false with no user_id claims a registered account that does not exist"
            );
        }
    }

    /// The proto field is a signed 64-bit integer and `byte_len` is a `usize`.
    /// Saturating keeps an absurd length from wrapping into a negative size,
    /// which would read as a corrupt row rather than an implausible one.
    #[test]
    fn an_implausible_length_saturates_rather_than_wrapping() {
        let event = record(None, Direction::Outbound, "GameEvent", usize::MAX, None).into_event();
        assert_eq!(payload(&event).byte_len, i64::MAX);
    }

    /// The reason the hooks pay for a clock read: the frame's own time has to
    /// survive the hand-off, or every row would report when the exporter got
    /// round to it — and the partition it lands in would follow.
    #[test]
    fn the_frames_own_timestamp_survives_projection() {
        // 2026-04-19T12:00:00Z: midday, so no timezone slip could move the
        // derived date and let this pass by accident.
        let captured = 1_776_600_000_000;
        let mut frame = record(None, Direction::Inbound, "Ping", 8, None);
        frame.occurred_at_ms = captured;

        let buffered = frame.project().expect("a frame must serialize");
        assert_eq!(buffered.date, event_date(captured));
        assert!(
            buffered.line.contains(&captured.to_string()),
            "the serialized row must carry the captured time: {}",
            buffered.line
        );

        let mut later = record(None, Direction::Inbound, "Ping", 8, None);
        later.occurred_at_ms = captured;
        assert_eq!(
            later.into_event().occurred_at_ms,
            captured,
            "projection must not re-read the clock"
        );
    }
}
