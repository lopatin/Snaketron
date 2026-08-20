//! Process-global websocket-message sink.
//!
//! Shaped like `sink.rs` and for the same reason: the alternative is threading
//! an exporter handle through `handle_websocket_connection` and into the
//! detached forwarder task, which would put analytics plumbing inside the
//! gateway's connection lifecycle for no behavioural gain.
//!
//! Safe as a global on the same terms: recording is fire-and-forget, drops
//! under pressure, returns nothing, and no call site may branch on it.
//!
//! The split from `sink.rs` is the tier, not the style — these events bypass
//! Valkey entirely and go straight to S3 (`ws_exporter`), because at the
//! volume of one event per frame the durable path's cost is not justified.
//!
//! Everything here runs on a gameplay task, so everything here is deliberately
//! cheap: a frame is described, not projected. Building the event, encoding it,
//! and serializing it all happen on the exporter's own task, off the frame
//! path entirely — see [`super::ws_exporter::WsFrameRecord`].

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use super::event::{EventOrigin, now_ms};
use super::ws_exporter::{Direction, WsEventSink, WsFrameRecord, is_sampled};

/// Sentinel for "this connection is not seated in a game". A game id is a
/// `u32` on the wire, so no real value collides with it.
const NO_GAME: i64 = -1;

struct Sink {
    events: WsEventSink,
    /// Shared rather than cloned per frame: the four strings in it are the same
    /// for every event this task will ever emit, and copying them on the frame
    /// path would be four allocations to say something already known.
    origin: Arc<EventOrigin>,
    sample_rate: f64,
}

static SINK: OnceLock<Sink> = OnceLock::new();

/// Installs the sink. Called once during server start.
///
/// A second call is ignored rather than panicking, matching `sink::install`:
/// tests start several servers in one process, and a panic there would be a
/// test-only failure mode with no production meaning.
pub fn install(sink: WsEventSink, origin: EventOrigin, sample_rate: f64) {
    let _ = SINK.set(Sink {
        events: sink,
        origin: Arc::new(origin),
        sample_rate,
    });
}

pub fn is_installed() -> bool {
    SINK.get().is_some()
}

/// One connection's analytics context.
///
/// Shared by the inbound loop and the outbound forwarder because the forwarder
/// is a separate task handed already-serialized frames: it can see neither the
/// connection state machine nor the authenticated identity, and both hooks
/// must agree about them or the two directions would not join.
pub struct WsConnection {
    /// Decided once, from a key that exists before the first frame and never
    /// changes, so a connection is wholly recorded or wholly absent.
    ///
    /// Keyed on the connection rather than on the session id — which R5.7
    /// names — because the session id is minted at authentication, so keying
    /// on it would leave the handshake frames with no decision to make. A
    /// session belongs to exactly one connection, so the property R5.7 is
    /// protecting still holds: a sampled session is complete.
    sampled: bool,
    /// The session this connection is carrying, once authentication has minted
    /// one. Absent before then because there is genuinely no session yet, and
    /// a placeholder would join to nothing while looking like it joined.
    ///
    /// `Arc<str>` rather than `String` because this is written once, at
    /// authentication, and then read under the lock by two tasks on every
    /// frame: sharing it makes that read a refcount bump instead of a copy
    /// inside the critical section.
    session_id: Mutex<Option<Arc<str>>>,
    /// The game this connection is seated in. Kept here rather than read from
    /// `ConnectionState` because the outbound forwarder cannot reach it.
    game_id: AtomicI64,
}

impl WsConnection {
    /// `connection_key` must be stable for the connection's whole life.
    ///
    /// A connection created before `install` is never sampled, which — together
    /// with the callers gating on [`WsConnection::records`] — is what makes a
    /// deployment without analytics cost nothing per frame.
    pub fn new(connection_key: &str) -> Self {
        match SINK.get() {
            Some(sink) => Self::at_sample_rate(connection_key, sink.sample_rate),
            None => Self::at_sample_rate(connection_key, 0.0),
        }
    }

    /// The decision, with the rate injected, so recording can be exercised
    /// without installing the process-global sink.
    fn at_sample_rate(connection_key: &str, sample_rate: f64) -> Self {
        Self {
            sampled: is_sampled(connection_key, sample_rate),
            session_id: Mutex::new(None),
            game_id: AtomicI64::new(NO_GAME),
        }
    }

    /// Whether this connection contributes frames at all.
    ///
    /// The gate belongs at the CALL SITE, not inside `record_*`: an argument is
    /// evaluated before the call, so a hook that names its own frame — the
    /// forwarder has to read the type back off the wire — would do that work
    /// for an unsampled connection and for a deployment with no sink installed.
    /// Reading one `bool` is the whole cost of the analytics path for those.
    pub fn records(&self) -> bool {
        self.sampled
    }

    /// Attaches the session id minted at authentication, so every later frame
    /// joins to `session_started`.
    pub fn bind_session(&self, session_id: &str) {
        if let Ok(mut held) = self.session_id.lock() {
            *held = Some(Arc::from(session_id));
        }
    }

    pub fn set_game_id(&self, game_id: Option<u32>) {
        self.game_id
            .store(game_id.map_or(NO_GAME, i64::from), Ordering::Relaxed);
    }

    fn session_id(&self) -> Option<Arc<str>> {
        self.session_id.lock().ok().and_then(|held| held.clone())
    }

    /// The seat this connection's frames are currently stamped with.
    pub fn game_id(&self) -> Option<i64> {
        match self.game_id.load(Ordering::Relaxed) {
            NO_GAME => None,
            game_id => Some(game_id),
        }
    }
}

/// Records a frame received from the client.
///
/// `byte_len` is the serialized length of the frame as it arrived. Fire and
/// forget: no call site may branch on the outcome.
///
/// Callers gate on [`WsConnection::records`] first; this rechecks so the
/// function is safe on its own, not because the gate is optional.
pub fn record_inbound(connection: &WsConnection, message_type: &'static str, byte_len: usize) {
    record(connection, Direction::Inbound, message_type, byte_len);
}

/// Records a frame on its way to the client.
pub fn record_outbound(connection: &WsConnection, message_type: &'static str, byte_len: usize) {
    record(connection, Direction::Outbound, message_type, byte_len);
}

fn record(
    connection: &WsConnection,
    direction: Direction,
    message_type: &'static str,
    byte_len: usize,
) {
    let Some(sink) = SINK.get() else { return };
    record_into(sink, connection, direction, message_type, byte_len);
}

/// The hand-off itself, with the sink passed in rather than looked up, so the
/// frame path is reachable from a test without installing a process-global.
///
/// What is left on the frame path: an uncontended mutex lock that yields a
/// refcount bump, a relaxed atomic load, a clock read, and a `try_send` of a
/// struct of machine words. Nothing allocates, nothing waits, and nothing can
/// fail back to the caller.
///
/// The clock read is the one piece that cannot move to the drain: reading it
/// there would report the exporter's time as the frame's, and the event-time
/// partition follows that value.
fn record_into(
    sink: &Sink,
    connection: &WsConnection,
    direction: Direction,
    message_type: &'static str,
    byte_len: usize,
) {
    if !connection.sampled {
        return;
    }
    // The bool is the drop signal, and dropping is the designed behaviour under
    // pressure: a websocket must not care. `record` never blocks and never
    // fails back to the caller.
    let _ = sink.events.record(WsFrameRecord {
        origin: sink.origin.clone(),
        session_id: connection.session_id(),
        direction,
        message_type,
        byte_len,
        game_id: connection.game_id(),
        occurred_at_ms: now_ms(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The whole point of the shared per-connection context: both hooks read
    /// the same identity, so the two directions of one session join.
    #[test]
    fn a_connection_shares_one_identity_across_both_directions() {
        let connection = WsConnection::new("ws-1");
        connection.bind_session("s_shared");
        connection.set_game_id(Some(9));

        // The inbound loop and the outbound forwarder each read the context
        // independently; they must see the same answer.
        let from_inbound = connection.session_id().expect("a bound session");
        let from_outbound = connection.session_id().expect("a bound session");
        assert_eq!(&*from_inbound, "s_shared");
        assert_eq!(from_inbound, from_outbound);
        assert_eq!(connection.game_id(), Some(9));
    }

    /// The session id is written once and read on every frame from two tasks,
    /// so the value taken under the lock must be a refcount bump rather than a
    /// copy of the string.
    #[test]
    fn reading_the_session_shares_the_string_rather_than_copying_it() {
        let connection = WsConnection::new("ws-share");
        connection.bind_session("s_shared");
        let first = connection.session_id().expect("a bound session");
        let second = connection.session_id().expect("a bound session");
        assert!(
            Arc::ptr_eq(&first, &second),
            "each read allocated its own copy"
        );
    }

    #[test]
    fn leaving_a_game_clears_the_recorded_game_id() {
        let connection = WsConnection::new("ws-2");
        connection.set_game_id(Some(3));
        assert_eq!(connection.game_id(), Some(3));
        connection.set_game_id(None);
        assert_eq!(
            connection.game_id(),
            None,
            "a frame after leaving must not still name the game"
        );
    }

    /// Sampling is decided once, at construction, so a session that
    /// authenticates part-way through cannot flip from out to in and leave a
    /// half-recorded funnel.
    #[test]
    fn the_sampling_decision_does_not_move_when_the_session_binds() {
        // Both sides of the decision, and both AFTER binding — the property
        // that can actually regress is that binding a session does not change
        // whether the connection records, in either direction.
        //
        // Asserting `records() == records()` around the bind would be vacuous:
        // `sampled` is a plain immutable field, so that comparison cannot fail
        // however the code is broken.
        let inside = WsConnection::at_sample_rate("ws-in", 1.0);
        let outside = WsConnection::at_sample_rate("ws-out", 0.0);
        assert!(inside.records());
        assert!(!outside.records());

        inside.bind_session("s_late");
        inside.set_game_id(Some(1));
        outside.bind_session("s_late_too");
        outside.set_game_id(Some(2));

        assert!(
            inside.records(),
            "binding a session must not drop a sampled connection out"
        );
        assert!(
            !outside.records(),
            "binding a session must not pull an excluded connection in"
        );
        // And the bind is what later frames join on, so it has to have landed.
        assert_eq!(inside.session_id().as_deref(), Some("s_late"));
    }

    /// Recording with no sink installed must be a no-op rather than a panic:
    /// a deployment without analytics runs unchanged.
    ///
    /// This test is only meaningful while nothing else in the binary installs
    /// the global, which nothing in the test profile does — `install` is called
    /// from `GameServer::new` behind `SNAKETRON_ANALYTICS_BUCKET`.
    #[test]
    fn recording_without_a_sink_is_a_no_op() {
        assert!(!is_installed());
        let connection = WsConnection::new("ws-4");
        assert!(
            !connection.records(),
            "no sink means nothing to sample into"
        );
        record_inbound(&connection, "Ping", 32);
        record_outbound(&connection, "Pong", 48);
    }

    /// The gate the two hooks branch on. An unsampled connection must answer
    /// no, because the caller skips naming its own frame on the strength of it.
    #[test]
    fn an_unsampled_connection_reports_that_it_records_nothing() {
        assert!(WsConnection::at_sample_rate("ws-gate-in", 1.0).records());
        assert!(!WsConnection::at_sample_rate("ws-gate-out", 0.0).records());
    }
}

#[cfg(test)]
mod export_path_tests {
    use super::*;
    use crate::analytics::emitter::DropReason;
    use crate::analytics::exporter::ExportTarget;
    use crate::analytics::object_store::{ObjectStore, PutOutcome};
    use crate::analytics::ws_exporter::{WsExporterConfig, create};
    use anyhow::Result;
    use async_trait::async_trait;
    use std::io::Read;
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;

    #[derive(Default)]
    struct FakeStore {
        objects: StdMutex<Vec<(String, String)>>,
    }

    #[async_trait]
    impl ObjectStore for FakeStore {
        async fn put_if_absent(&self, key: &str, body: Vec<u8>) -> Result<PutOutcome> {
            let mut decoded = String::new();
            flate2::read::GzDecoder::new(body.as_slice()).read_to_string(&mut decoded)?;
            self.objects.lock().unwrap().push((key.to_owned(), decoded));
            Ok(PutOutcome::Written)
        }
    }

    fn sink_over(store: Arc<dyn ObjectStore>, capacity: usize) -> (Sink, CancellationToken) {
        let config = WsExporterConfig {
            // Wide enough that only the shutdown flush writes, so a test that
            // asserts on the written object is asserting on that flush.
            limits: crate::analytics::BatchLimits {
                max_batch_age: Duration::from_secs(3_600),
                max_buffer_bytes: 1 << 20,
                max_buffer_events: 100_000,
                max_events_per_file: 1_000,
                max_bytes_per_file: 1 << 20,
            },
            target: ExportTarget {
                dataset: "websocket-events".to_owned(),
                host: "use1-7".to_owned(),
            },
            channel_capacity: capacity,
            flush_timeout: Duration::from_secs(5),
            sample_rate: 1.0,
        };
        let cancel = CancellationToken::new();
        let (events, task) = create(store, config, cancel.clone());
        tokio::spawn(task);
        (
            Sink {
                events,
                origin: Arc::new(EventOrigin {
                    environment: "test".to_owned(),
                    region: "use1".to_owned(),
                    aws_region: "us-east-1".to_owned(),
                    instance_id: "7:boot".to_owned(),
                }),
                sample_rate: 1.0,
            },
            cancel,
        )
    }

    /// End to end through the sink: a recorded frame has to survive the
    /// hand-off, the exporter's projection, serialization, batching, and the
    /// write, and land under the websocket dataset rather than the game-events
    /// one.
    #[tokio::test]
    async fn a_recorded_frame_reaches_the_websocket_dataset_as_ndjson() {
        let fake = Arc::new(FakeStore::default());
        let store: Arc<dyn ObjectStore> = fake.clone();
        let (sink, cancel) = sink_over(store, 64);

        let connection = WsConnection::at_sample_rate("ws-e2e", 1.0);
        connection.bind_session("s_e2e");
        connection.set_game_id(Some(4242));
        record_into(&sink, &connection, Direction::Inbound, "PlayerReady", 31);
        record_into(&sink, &connection, Direction::Outbound, "GameEvent", 900);

        cancel.cancel();
        // The exporter's final flush runs on its own task; poll for the write
        // rather than sleeping a fixed amount.
        let mut objects = Vec::new();
        for _ in 0..100 {
            objects = fake.objects.lock().unwrap().clone();
            if !objects.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let (key, body) = objects.first().expect("the shutdown flush must write");
        assert!(
            key.starts_with("raw/websocket-events/dt="),
            "wrong dataset prefix: {key}"
        );
        assert!(key.contains("host=use1-7"), "wrong host partition: {key}");

        let lines: Vec<serde_json::Value> = body
            .lines()
            .map(|line| serde_json::from_str(line).expect("every line must be JSON"))
            .collect();
        assert_eq!(lines.len(), 2, "both directions must be written");
        assert_eq!(lines[0]["event_name"], "websocket_message");
        assert_eq!(lines[0]["identity"]["session_id"], "s_e2e");
        assert_eq!(lines[0]["websocket_message"]["direction"], "in");
        assert_eq!(lines[0]["websocket_message"]["message_type"], "PlayerReady");
        // Quoted per the proto3 JSON mapping, matching the Athena DDL.
        assert_eq!(lines[0]["websocket_message"]["byte_len"], "31");
        assert_eq!(lines[0]["websocket_message"]["game_id"], "4242");
        assert_eq!(lines[1]["websocket_message"]["direction"], "out");
        assert_eq!(lines[0]["region"], "use1", "the origin must reach the row");
    }

    /// Invariant I1 at the hook, not just at the channel: an overwhelmed
    /// exporter must cost the websocket nothing but a counted drop.
    #[tokio::test]
    async fn an_overwhelmed_sink_drops_and_counts_without_blocking_the_hook() {
        let store: Arc<dyn ObjectStore> = Arc::new(FakeStore::default());
        let (sink, _cancel) = sink_over(store, 1);
        let metrics = sink.events.metrics();
        let connection = WsConnection::at_sample_rate("ws-flood", 1.0);

        let started = std::time::Instant::now();
        for _ in 0..500 {
            record_into(&sink, &connection, Direction::Outbound, "GameEvent", 64);
        }
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "recording must never wait on the exporter"
        );
        assert!(
            metrics.dropped(DropReason::BufferFull) > 0,
            "shed load must be counted, never silent"
        );
    }

    /// The sample rate as a cost knob: an excluded connection contributes
    /// nothing at all, while an included one over the same sink still does —
    /// so this cannot pass by recording being broken outright.
    #[tokio::test]
    async fn an_unsampled_connection_emits_nothing_while_a_sampled_one_does() {
        let fake = Arc::new(FakeStore::default());
        let store: Arc<dyn ObjectStore> = fake.clone();
        let (sink, cancel) = sink_over(store, 64);

        let excluded = WsConnection::at_sample_rate("ws-out", 0.0);
        assert!(!excluded.records());
        for _ in 0..50 {
            record_into(&sink, &excluded, Direction::Inbound, "Ping", 8);
        }

        let included = WsConnection::at_sample_rate("ws-in", 1.0);
        record_into(&sink, &included, Direction::Inbound, "PlayerReady", 8);

        cancel.cancel();
        let mut objects = Vec::new();
        for _ in 0..100 {
            objects = fake.objects.lock().unwrap().clone();
            if !objects.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let body: String = objects.iter().map(|(_, body)| body.as_str()).collect();
        assert!(
            body.contains("PlayerReady"),
            "the sampled connection must still be written"
        );
        assert!(
            !body.contains("\"Ping\""),
            "an excluded connection must contribute nothing: {body}"
        );
    }
}
