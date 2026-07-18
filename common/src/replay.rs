//! Offline root-cause analysis over sync traces (`common::trace`).
//!
//! Three tools, all pure and portable (file I/O is native-only):
//! - [`ServerReplay`]: re-drives a `GameEngine` from a server trace's anchor
//!   state through the recorded command/event timeline and verifies that the
//!   engine re-emits exactly the recorded event stream — proving (or refuting)
//!   that the server simulation is deterministic.
//! - [`ClientReplay`]: re-drives a `GameEngine` the way the web client does
//!   (process_server_event per received message, recorded local commands
//!   scheduled as-is) and verifies the client's recorded committed-state
//!   fingerprints are reproduced.
//! - [`diff_traces`]: joins a server trace and a client trace of the same game
//!   into a [`DivergenceReport`]: lost stream sequences, first fingerprint
//!   mismatch, command wire latency and rescheduling, clock drift, and a
//!   root-cause [`DivergenceReport::verdict`].

use crate::trace::{TraceRecord, TraceSide};
use crate::{
    CommandId, DEFAULT_TICK_INTERVAL_MS, GameCommandMessage, GameEngine, GameEvent,
    GameEventMessage, GameState,
};
use anyhow::{Context, Result, bail};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt::Write as _;

/// First point where a replay stopped matching its recording.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct Divergence {
    pub tick: u32,
    pub kind: String,
    /// What the recorded trace says happened.
    pub expected: String,
    /// What the replayed engine produced.
    pub actual: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplayOutcome {
    pub deterministic: bool,
    pub ticks_replayed: u32,
    pub events_compared: usize,
    pub first_divergence: Option<Divergence>,
}

impl ReplayOutcome {
    pub fn render(&self) -> String {
        let mut s = String::new();
        let _ = writeln!(s, "== server replay ==");
        let _ = writeln!(s, "deterministic:   {}", self.deterministic);
        let _ = writeln!(s, "ticks replayed:  {}", self.ticks_replayed);
        let _ = writeln!(s, "events compared: {}", self.events_compared);
        render_divergence(&mut s, &self.first_divergence);
        s
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ClientReplayOutcome {
    pub reproduces: bool,
    pub ticks_replayed: u32,
    pub fingerprints_compared: usize,
    pub first_divergence: Option<Divergence>,
}

impl ClientReplayOutcome {
    pub fn render(&self) -> String {
        let mut s = String::new();
        let _ = writeln!(s, "== client replay ==");
        let _ = writeln!(s, "reproduces:            {}", self.reproduces);
        let _ = writeln!(s, "ticks replayed:        {}", self.ticks_replayed);
        let _ = writeln!(s, "fingerprints compared: {}", self.fingerprints_compared);
        render_divergence(&mut s, &self.first_divergence);
        s
    }
}

fn render_divergence(s: &mut String, divergence: &Option<Divergence>) {
    match divergence {
        None => {
            let _ = writeln!(s, "first divergence: none");
        }
        Some(d) => {
            let _ = writeln!(s, "first divergence: tick {} [{}]", d.tick, d.kind);
            let _ = writeln!(s, "  recorded: {}", d.expected);
            let _ = writeln!(s, "  replayed: {}", d.actual);
        }
    }
}

/// Side declared by the trace's Meta record, if any. Used by the CLI to pick
/// the right replay for a trace file.
pub fn trace_side(records: &[TraceRecord]) -> Option<TraceSide> {
    records.iter().find_map(|r| match r {
        TraceRecord::Meta { side, .. } => Some(*side),
        _ => None,
    })
}

fn event_value(event: &GameEvent) -> serde_json::Value {
    serde_json::to_value(event).unwrap_or(serde_json::Value::Null)
}

fn fmt_event(tick: u32, sequence: u64, event: &GameEvent) -> String {
    format!("tick={} seq={} {}", tick, sequence, event_value(event))
}

fn fmt_cmd(cmd: &GameCommandMessage) -> String {
    serde_json::to_value(cmd)
        .map(|v| v.to_string())
        .unwrap_or_else(|_| format!("{:?}", cmd))
}

fn fmt_hash(hash: u64) -> String {
    format!("{:#018x}", hash)
}

// ---------------------------------------------------------------------------
// Server replay
// ---------------------------------------------------------------------------

enum ServerItem {
    Cmd {
        ts_ms: i64,
        cmd: GameCommandMessage,
    },
    Event {
        ts_ms: i64,
        msg: Box<GameEventMessage>,
    },
    Probe {
        ts_ms: i64,
        tick: u32,
        hash: u64,
    },
}

/// Deterministic replay of a server-side trace.
pub struct ServerReplay {
    game_id: u32,
    anchor: Box<GameState>,
    timeline: Vec<ServerItem>,
}

impl ServerReplay {
    pub fn from_records(records: Vec<TraceRecord>) -> Result<ServerReplay> {
        let side = trace_side(&records).context("trace has no Meta record")?;
        if side != TraceSide::Server {
            bail!("trace Meta.side is {:?}, expected Server", side);
        }
        let game_id = records
            .iter()
            .find_map(|r| match r {
                TraceRecord::Meta { game_id, .. } => Some(*game_id),
                _ => None,
            })
            .context("trace has no Meta record")?;

        let mut anchor: Option<Box<GameState>> = None;
        let mut timeline = Vec::new();
        for record in records {
            // Only records at or after the anchor participate in the replay.
            match record {
                TraceRecord::State { state, .. } => {
                    if anchor.is_none() {
                        anchor = Some(state);
                    }
                }
                TraceRecord::CmdIn { ts_ms, cmd } if anchor.is_some() => {
                    timeline.push(ServerItem::Cmd { ts_ms, cmd });
                }
                TraceRecord::EventOut { ts_ms, msg } if anchor.is_some() => {
                    timeline.push(ServerItem::Event { ts_ms, msg });
                }
                TraceRecord::Fingerprint { ts_ms, tick, hash } if anchor.is_some() => {
                    timeline.push(ServerItem::Probe { ts_ms, tick, hash });
                }
                _ => {}
            }
        }
        let anchor = anchor.context("server trace has no State record to anchor the replay")?;

        Ok(ServerReplay {
            game_id,
            anchor,
            timeline,
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_file(path: impl AsRef<std::path::Path>) -> Result<ServerReplay> {
        Self::from_records(crate::trace::read_trace(path)?)
    }

    /// Rebuild the engine from the anchor state and walk a virtual clock
    /// through the recorded timeline, comparing everything the engine emits
    /// against everything the trace says was published.
    pub fn replay(&self) -> Result<ReplayOutcome> {
        let mut engine = GameEngine::new_from_state(self.game_id, (*self.anchor).clone());
        let anchor_tick = self.anchor.tick;

        // Engine-emitted gameplay events not yet matched to a recorded
        // EventOut. Snapshots are excluded: they are re-anchoring payloads,
        // not gameplay events, and are byte-for-byte huge.
        let mut emitted: VecDeque<(u32, u64, GameEvent)> = VecDeque::new();
        // Commands scheduled by our process_command calls, awaiting their
        // recorded CommandScheduled EventOut counterpart.
        let mut scheduled: VecDeque<GameCommandMessage> = VecDeque::new();
        let mut events_compared = 0usize;
        let mut first_divergence: Option<Divergence> = None;

        fn advance(
            engine: &mut GameEngine,
            ts_ms: i64,
            emitted: &mut VecDeque<(u32, u64, GameEvent)>,
        ) -> Result<()> {
            for (tick, sequence, event) in engine.run_until(ts_ms)? {
                if !matches!(event, GameEvent::Snapshot { .. }) {
                    emitted.push_back((tick, sequence, event));
                }
            }
            Ok(())
        }

        for item in &self.timeline {
            if first_divergence.is_some() {
                break;
            }
            match item {
                ServerItem::Cmd { ts_ms, cmd } => {
                    advance(&mut engine, *ts_ms, &mut emitted)?;
                    scheduled.push_back(engine.process_command(cmd.clone())?);
                }
                ServerItem::Probe { ts_ms, tick, hash } => {
                    advance(&mut engine, *ts_ms, &mut emitted)?;
                    // Fingerprints are sampled between executor loop turns, so
                    // the replayed tick can legitimately be ahead by the time
                    // we process the record; only compare exact-tick samples.
                    if engine.current_tick() == *tick && engine.committed_sync_hash() != *hash {
                        first_divergence = Some(Divergence {
                            tick: *tick,
                            kind: "fingerprint_mismatch".into(),
                            expected: fmt_hash(*hash),
                            actual: fmt_hash(engine.committed_sync_hash()),
                        });
                    }
                }
                ServerItem::Event { ts_ms, msg } => {
                    advance(&mut engine, *ts_ms, &mut emitted)?;
                    match &msg.event {
                        GameEvent::Snapshot { .. } => {}
                        GameEvent::TickHash { hash, .. } => {
                            events_compared += 1;
                            if engine.current_tick() != msg.tick {
                                first_divergence = Some(Divergence {
                                    tick: msg.tick,
                                    kind: "tick_hash_tick_mismatch".into(),
                                    expected: format!("committed tick {}", msg.tick),
                                    actual: format!("committed tick {}", engine.current_tick()),
                                });
                            } else if engine.committed_sync_hash() != *hash {
                                first_divergence = Some(Divergence {
                                    tick: msg.tick,
                                    kind: "tick_hash_mismatch".into(),
                                    expected: fmt_hash(*hash),
                                    actual: fmt_hash(engine.committed_sync_hash()),
                                });
                            }
                        }
                        GameEvent::CommandScheduled { command_message } => {
                            events_compared += 1;
                            match scheduled.pop_front() {
                                None => {
                                    first_divergence = Some(Divergence {
                                        tick: msg.tick,
                                        kind: "unexpected_command_scheduled".into(),
                                        expected: fmt_cmd(command_message),
                                        actual: "<replay scheduled no command>".into(),
                                    });
                                }
                                Some(ours) => {
                                    if serde_json::to_value(&ours).ok()
                                        != serde_json::to_value(command_message).ok()
                                    {
                                        first_divergence = Some(Divergence {
                                            tick: msg.tick,
                                            kind: "command_schedule_mismatch".into(),
                                            expected: fmt_cmd(command_message),
                                            actual: fmt_cmd(&ours),
                                        });
                                    }
                                }
                            }
                        }
                        _ => {
                            events_compared += 1;
                            match emitted.pop_front() {
                                None => {
                                    first_divergence = Some(Divergence {
                                        tick: msg.tick,
                                        kind: "missing_event".into(),
                                        expected: fmt_event(msg.tick, msg.sequence, &msg.event),
                                        actual: "<engine emitted no event>".into(),
                                    });
                                }
                                Some((tick, sequence, event)) => {
                                    if tick != msg.tick
                                        || sequence != msg.sequence
                                        || event_value(&event) != event_value(&msg.event)
                                    {
                                        first_divergence = Some(Divergence {
                                            tick: msg.tick,
                                            kind: "event_mismatch".into(),
                                            expected: fmt_event(msg.tick, msg.sequence, &msg.event),
                                            actual: fmt_event(tick, sequence, &event),
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if first_divergence.is_none()
            && let Some((tick, sequence, event)) = emitted.pop_front()
        {
            first_divergence = Some(Divergence {
                tick,
                kind: "extra_event".into(),
                expected: "<no further recorded events>".into(),
                actual: fmt_event(tick, sequence, &event),
            });
        }

        Ok(ReplayOutcome {
            deterministic: first_divergence.is_none(),
            ticks_replayed: engine.current_tick().saturating_sub(anchor_tick),
            events_compared,
            first_divergence,
        })
    }
}

// ---------------------------------------------------------------------------
// Client replay
// ---------------------------------------------------------------------------

enum ClientItem {
    Event { msg: Box<GameEventMessage> },
    Cmd { cmd: GameCommandMessage },
    Probe { tick: u32, hash: u64 },
}

/// Replay of a client-side trace: reproduces the committed state exactly as
/// the live client built it and checks the recorded fingerprints.
pub struct ClientReplay {
    game_id: u32,
    anchor: Box<GameState>,
    timeline: Vec<ClientItem>,
}

impl ClientReplay {
    pub fn from_records(records: Vec<TraceRecord>) -> Result<ClientReplay> {
        let side = trace_side(&records).context("trace has no Meta record")?;
        if side != TraceSide::Client {
            bail!("trace Meta.side is {:?}, expected Client", side);
        }
        let game_id = records
            .iter()
            .find_map(|r| match r {
                TraceRecord::Meta { game_id, .. } => Some(*game_id),
                _ => None,
            })
            .context("trace has no Meta record")?;

        // Anchor: the first recorded State, or the state inside the first
        // received Snapshot. Records before the anchor predate any
        // authoritative state and are skipped, exactly as the live client's
        // pre-snapshot state was thrown away.
        let mut anchor: Option<Box<GameState>> = None;
        let mut timeline = Vec::new();
        for record in records {
            match record {
                TraceRecord::State { state, .. } => {
                    if anchor.is_none() {
                        anchor = Some(state);
                    }
                }
                TraceRecord::EventIn { msg, .. } => {
                    if anchor.is_some() {
                        timeline.push(ClientItem::Event { msg });
                    } else if let GameEvent::Snapshot { game_state } = &msg.event {
                        anchor = Some(Box::new(game_state.clone()));
                        // Keep the snapshot in the timeline: re-applying it is
                        // idempotent and sets the stream watermark like the
                        // live client's processing did.
                        timeline.push(ClientItem::Event { msg });
                    }
                }
                TraceRecord::CmdOut { cmd, .. } if anchor.is_some() => {
                    timeline.push(ClientItem::Cmd { cmd });
                }
                TraceRecord::Fingerprint { tick, hash, .. } if anchor.is_some() => {
                    timeline.push(ClientItem::Probe { tick, hash });
                }
                _ => {}
            }
        }
        let anchor =
            anchor.context("client trace has no State record and no Snapshot EventIn to anchor")?;

        Ok(ClientReplay {
            game_id,
            anchor,
            timeline,
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_file(path: impl AsRef<std::path::Path>) -> Result<ClientReplay> {
        Self::from_records(crate::trace::read_trace(path)?)
    }

    pub fn replay(&self) -> Result<ClientReplayOutcome> {
        let mut engine = GameEngine::new_from_state(self.game_id, (*self.anchor).clone());
        let anchor_tick = self.anchor.tick;
        let mut fingerprints_compared = 0usize;
        let mut first_divergence: Option<Divergence> = None;

        for item in &self.timeline {
            if first_divergence.is_some() {
                break;
            }
            match item {
                ClientItem::Event { msg } => {
                    engine.process_server_event(msg)?;
                }
                ClientItem::Cmd { cmd } => {
                    schedule_recorded_client_command(&mut engine, self.game_id, cmd)?;
                }
                ClientItem::Probe { tick, hash } => {
                    fingerprints_compared += 1;
                    if engine.current_tick() != *tick {
                        first_divergence = Some(Divergence {
                            tick: *tick,
                            kind: "fingerprint_tick_mismatch".into(),
                            expected: format!("committed tick {}", tick),
                            actual: format!("committed tick {}", engine.current_tick()),
                        });
                    } else if engine.committed_sync_hash() != *hash {
                        first_divergence = Some(Divergence {
                            tick: *tick,
                            kind: "fingerprint_mismatch".into(),
                            expected: fmt_hash(*hash),
                            actual: fmt_hash(engine.committed_sync_hash()),
                        });
                    }
                }
            }
        }

        Ok(ClientReplayOutcome {
            reproduces: first_divergence.is_none(),
            ticks_replayed: engine.current_tick().saturating_sub(anchor_tick),
            fingerprints_compared,
            first_divergence,
        })
    }
}

/// Schedule a recorded client command exactly as `process_local_command` did
/// at capture time: the recorded message already carries the tick the client
/// chose, so it is applied as-is (never re-derived) through a synthetic
/// CommandScheduled message. `tick: 0` / `stream_seq: 0` guarantee this can
/// neither fast-forward the committed state nor disturb gap accounting.
fn schedule_recorded_client_command(
    engine: &mut GameEngine,
    game_id: u32,
    cmd: &GameCommandMessage,
) -> Result<()> {
    let msg = GameEventMessage {
        game_id,
        tick: 0,
        sequence: 0,
        stream_seq: 0,
        user_id: Some(cmd.command_id_client.user_id),
        event: GameEvent::CommandScheduled {
            command_message: cmd.clone(),
        },
    };
    engine.process_server_event(&msg)
}

// ---------------------------------------------------------------------------
// Trace diffing
// ---------------------------------------------------------------------------

/// Inclusive range of missing transport sequences.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SeqRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FingerprintMismatch {
    pub tick: u32,
    pub server_hash: u64,
    pub client_hash: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct CommandLatencyEntry {
    pub user_id: u32,
    /// Tick the client scheduled the command at (CmdOut.predicted_tick).
    pub client_tick: u32,
    /// CmdIn receive time minus CmdOut send time; None if the command never
    /// reached the server (itself a transport-loss signal).
    pub wire_latency_ms: Option<i64>,
    /// Tick the server actually scheduled the command at.
    pub server_tick: Option<u32>,
    /// server_tick - client_tick; non-zero means the command was rescheduled.
    pub tick_delta: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClockDriftSummary {
    pub samples: usize,
    pub min_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub max_abs_ms: f64,
    pub mean_rtt_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TaggedNote {
    pub side: TraceSide,
    pub ts_ms: i64,
    pub note: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DivergenceReport {
    /// EventOut stream_seqs the server published (up to the last seq the
    /// client saw) that never appear in the client's EventIn records.
    pub missing_stream_seqs: Vec<SeqRange>,
    /// Server tick of the earliest missing message, for ordering against the
    /// first fingerprint mismatch.
    pub first_missing_seq_tick: Option<u32>,
    /// Earliest tick where both sides recorded a Fingerprint and disagree.
    pub first_fingerprint_mismatch: Option<FingerprintMismatch>,
    pub command_latency: Vec<CommandLatencyEntry>,
    pub clock_drift_summary: Option<ClockDriftSummary>,
    pub notes: Vec<TaggedNote>,
    pub tick_duration_ms: u32,
}

impl DivergenceReport {
    /// Root-cause heuristic, most-specific first.
    pub fn verdict(&self) -> String {
        let first_mismatch_tick = self.first_fingerprint_mismatch.as_ref().map(|m| m.tick);
        if !self.missing_stream_seqs.is_empty() {
            // Loss that precedes (or exists without) any hash divergence is
            // the root cause; divergence is just its downstream symptom.
            let loss_precedes_mismatch = match (self.first_missing_seq_tick, first_mismatch_tick) {
                (_, None) => true,
                (Some(loss_tick), Some(mismatch_tick)) => loss_tick <= mismatch_tick,
                (None, Some(_)) => false,
            };
            if loss_precedes_mismatch {
                return "TRANSPORT_LOSS".into();
            }
        }
        if first_mismatch_tick.is_some() {
            return "ENGINE_NONDETERMINISM".into();
        }
        let rescheduled = self
            .command_latency
            .iter()
            .any(|c| c.tick_delta.is_some_and(|d| d != 0));
        let drift_exceeds_half_tick = self
            .clock_drift_summary
            .as_ref()
            .is_some_and(|s| s.max_abs_ms > self.tick_duration_ms as f64 / 2.0);
        if drift_exceeds_half_tick && rescheduled {
            return "CLOCK_DRIFT".into();
        }
        "IN_SYNC".into()
    }

    pub fn render(&self) -> String {
        let mut s = String::new();
        let _ = writeln!(s, "== trace divergence report ==");
        let _ = writeln!(s, "verdict: {}", self.verdict());
        let _ = writeln!(s, "tick duration: {} ms", self.tick_duration_ms);

        if self.missing_stream_seqs.is_empty() {
            let _ = writeln!(s, "missing stream seqs: none");
        } else {
            let total: u64 = self
                .missing_stream_seqs
                .iter()
                .map(|r| r.end - r.start + 1)
                .sum();
            let _ = writeln!(
                s,
                "missing stream seqs: {} message(s) in {} range(s){}",
                total,
                self.missing_stream_seqs.len(),
                self.first_missing_seq_tick
                    .map(|t| format!(", first at server tick {}", t))
                    .unwrap_or_default()
            );
            for r in &self.missing_stream_seqs {
                let _ = writeln!(s, "  seq {}..={}", r.start, r.end);
            }
        }

        match &self.first_fingerprint_mismatch {
            None => {
                let _ = writeln!(s, "first fingerprint mismatch: none");
            }
            Some(m) => {
                let _ = writeln!(
                    s,
                    "first fingerprint mismatch: tick {} server={} client={}",
                    m.tick,
                    fmt_hash(m.server_hash),
                    fmt_hash(m.client_hash)
                );
            }
        }

        let _ = writeln!(s, "commands: {}", self.command_latency.len());
        for c in &self.command_latency {
            let _ = writeln!(
                s,
                "  user {} client_tick={} wire={} server_tick={} delta={}",
                c.user_id,
                c.client_tick,
                c.wire_latency_ms
                    .map(|v| format!("{}ms", v))
                    .unwrap_or_else(|| "LOST".into()),
                c.server_tick
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".into()),
                c.tick_delta
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "-".into()),
            );
        }

        match &self.clock_drift_summary {
            None => {
                let _ = writeln!(s, "clock drift: no samples");
            }
            Some(d) => {
                let _ = writeln!(
                    s,
                    "clock drift: samples={} min={:.1}ms max={:.1}ms mean={:.1}ms mean_rtt={:.1}ms",
                    d.samples, d.min_ms, d.max_ms, d.mean_ms, d.mean_rtt_ms
                );
            }
        }

        if self.notes.is_empty() {
            let _ = writeln!(s, "notes: none");
        } else {
            let _ = writeln!(s, "notes:");
            for n in &self.notes {
                let _ = writeln!(s, "  [{:?} @ {}] {}", n.side, n.ts_ms, n.note);
            }
        }
        s
    }
}

fn seqs_to_ranges(seqs: &[u64]) -> Vec<SeqRange> {
    let mut ranges: Vec<SeqRange> = Vec::new();
    for &seq in seqs {
        match ranges.last_mut() {
            Some(last) if last.end + 1 == seq => last.end = seq,
            _ => ranges.push(SeqRange {
                start: seq,
                end: seq,
            }),
        }
    }
    ranges
}

/// Join a server trace and a client trace of the same game.
pub fn diff_traces(server: &[TraceRecord], client: &[TraceRecord]) -> DivergenceReport {
    let tick_duration_ms = [server, client]
        .iter()
        .find_map(|records| {
            records.iter().find_map(|r| match r {
                TraceRecord::Meta {
                    tick_duration_ms, ..
                } => Some(*tick_duration_ms),
                _ => None,
            })
        })
        .unwrap_or(DEFAULT_TICK_INTERVAL_MS);

    // --- server side ---
    let mut server_seq_ticks: BTreeMap<u64, u32> = BTreeMap::new();
    let mut server_fingerprints: BTreeMap<u32, u64> = BTreeMap::new();
    let mut cmd_in_ts: HashMap<CommandId, i64> = HashMap::new();
    let mut server_assigned: HashMap<CommandId, u32> = HashMap::new();
    let mut notes: Vec<TaggedNote> = Vec::new();

    for record in server {
        match record {
            TraceRecord::EventOut { msg, .. } => {
                if msg.stream_seq > 0 {
                    server_seq_ticks.insert(msg.stream_seq, msg.tick);
                }
                if let GameEvent::CommandScheduled { command_message } = &msg.event
                    && let Some(server_id) = &command_message.command_id_server
                {
                    server_assigned
                        .insert(command_message.command_id_client.clone(), server_id.tick);
                }
            }
            TraceRecord::CmdIn { ts_ms, cmd } => {
                cmd_in_ts.insert(cmd.command_id_client.clone(), *ts_ms);
                if let Some(server_id) = &cmd.command_id_server {
                    server_assigned.insert(cmd.command_id_client.clone(), server_id.tick);
                }
            }
            TraceRecord::Fingerprint { tick, hash, .. } => {
                server_fingerprints.entry(*tick).or_insert(*hash);
            }
            TraceRecord::Note { ts_ms, note } => notes.push(TaggedNote {
                side: TraceSide::Server,
                ts_ms: *ts_ms,
                note: note.clone(),
            }),
            _ => {}
        }
    }

    // --- client side ---
    let mut client_seqs: HashSet<u64> = HashSet::new();
    let mut client_first_seq: Option<u64> = None;
    let mut client_fingerprints: BTreeMap<u32, u64> = BTreeMap::new();
    let mut cmd_outs: Vec<(i64, u32, GameCommandMessage)> = Vec::new();
    let mut drift_samples: Vec<(f64, f64)> = Vec::new();

    for record in client {
        match record {
            TraceRecord::EventIn { msg, .. } => {
                if msg.stream_seq > 0 {
                    client_first_seq.get_or_insert(msg.stream_seq);
                    client_seqs.insert(msg.stream_seq);
                }
            }
            TraceRecord::CmdOut {
                ts_ms,
                predicted_tick,
                cmd,
            } => cmd_outs.push((*ts_ms, *predicted_tick, cmd.clone())),
            TraceRecord::Fingerprint { tick, hash, .. } => {
                client_fingerprints.entry(*tick).or_insert(*hash);
            }
            TraceRecord::Clock {
                drift_ms, rtt_ms, ..
            } => drift_samples.push((*drift_ms, *rtt_ms)),
            TraceRecord::Note { ts_ms, note } => notes.push(TaggedNote {
                side: TraceSide::Client,
                ts_ms: *ts_ms,
                note: note.clone(),
            }),
            _ => {}
        }
    }
    notes.sort_by_key(|n| n.ts_ms);

    // Missing seqs: only within the window the client was actually attached.
    // Seqs below the first received message were absorbed into the join
    // snapshot (not lost); seqs above the highest received may simply be
    // still in flight when the trace ended.
    let client_min_seq = client_first_seq.unwrap_or(0);
    let client_max_seq = client_seqs.iter().max().copied().unwrap_or(0);
    let missing: Vec<u64> = server_seq_ticks
        .iter()
        .filter(|(seq, _)| {
            **seq > client_min_seq && **seq <= client_max_seq && !client_seqs.contains(seq)
        })
        .map(|(seq, _)| *seq)
        .collect();
    let first_missing_seq_tick = missing
        .first()
        .and_then(|seq| server_seq_ticks.get(seq).copied());
    let missing_stream_seqs = seqs_to_ranges(&missing);

    let first_fingerprint_mismatch = server_fingerprints.iter().find_map(|(tick, server_hash)| {
        client_fingerprints
            .get(tick)
            .filter(|client_hash| *client_hash != server_hash)
            .map(|client_hash| FingerprintMismatch {
                tick: *tick,
                server_hash: *server_hash,
                client_hash: *client_hash,
            })
    });

    let command_latency: Vec<CommandLatencyEntry> = cmd_outs
        .iter()
        .map(|(ts_ms, predicted_tick, cmd)| {
            let client_id = &cmd.command_id_client;
            let server_tick = server_assigned.get(client_id).copied();
            CommandLatencyEntry {
                user_id: client_id.user_id,
                client_tick: *predicted_tick,
                wire_latency_ms: cmd_in_ts.get(client_id).map(|received| received - ts_ms),
                server_tick,
                tick_delta: server_tick.map(|t| t as i64 - *predicted_tick as i64),
            }
        })
        .collect();

    let clock_drift_summary = if drift_samples.is_empty() {
        None
    } else {
        let n = drift_samples.len() as f64;
        Some(ClockDriftSummary {
            samples: drift_samples.len(),
            min_ms: drift_samples
                .iter()
                .map(|(d, _)| *d)
                .fold(f64::MAX, f64::min),
            max_ms: drift_samples
                .iter()
                .map(|(d, _)| *d)
                .fold(f64::MIN, f64::max),
            mean_ms: drift_samples.iter().map(|(d, _)| *d).sum::<f64>() / n,
            max_abs_ms: drift_samples
                .iter()
                .map(|(d, _)| d.abs())
                .fold(0.0, f64::max),
            mean_rtt_ms: drift_samples.iter().map(|(_, r)| *r).sum::<f64>() / n,
        })
    };

    DivergenceReport {
        missing_stream_seqs,
        first_missing_seq_tick,
        first_fingerprint_mismatch,
        command_latency,
        clock_drift_summary,
        notes,
        tick_duration_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace::TRACE_FORMAT_VERSION;
    use crate::{Direction, GameCommand, GameStatus, GameType, QueueMode};

    const GAME_ID: u32 = 7;
    const START_MS: i64 = 1_000;
    const TICK_MS: i64 = 100;

    /// Drive a real seeded engine through a synthetic game the way the game
    /// executor does (run_until on a 50ms virtual clock, commands injected
    /// mid-stream, TickHash heartbeats and fingerprints every 500ms) while
    /// recording a server trace with the real TraceRecord shapes.
    fn build_synthetic_server_trace() -> Vec<TraceRecord> {
        let mut state = GameState::new(
            40,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            Some(42),
            START_MS,
        );
        state.add_player(1, Some("alice".into())).unwrap();
        state.add_player(2, Some("bob".into())).unwrap();
        state.status = GameStatus::Started { server_id: 99 };
        state.spawn_initial_food();

        let mut engine = GameEngine::new_from_state(GAME_ID, state.clone());
        let mut records = vec![
            TraceRecord::Meta {
                version: TRACE_FORMAT_VERSION,
                side: TraceSide::Server,
                game_id: GAME_ID,
                session: "synthetic".into(),
                ts_ms: START_MS,
                build: "test".into(),
                tick_duration_ms: TICK_MS as u32,
            },
            TraceRecord::State {
                ts_ms: START_MS,
                tick: 0,
                state: Box::new(state),
            },
        ];

        let mut stream_seq = 0u64;
        // (virtual receive time, user_id, snake_id, direction)
        let commands = [
            (START_MS + 300, 1u32, 0u32, Direction::Up),
            (START_MS + 2_200, 2u32, 1u32, Direction::Up),
        ];

        let mut ts = START_MS;
        while ts <= START_MS + 3_500 {
            for (tick, sequence, event) in engine.run_until(ts).unwrap() {
                stream_seq += 1;
                records.push(TraceRecord::EventOut {
                    ts_ms: ts,
                    msg: Box::new(GameEventMessage {
                        game_id: GAME_ID,
                        tick,
                        sequence,
                        stream_seq,
                        user_id: None,
                        event,
                    }),
                });
            }

            for (cmd_ts, user_id, snake_id, direction) in commands {
                if cmd_ts == ts {
                    let cmd = GameCommandMessage {
                        command_id_client: CommandId {
                            tick: engine.current_tick() + 2,
                            user_id,
                            sequence_number: 0,
                        },
                        command_id_server: None,
                        command: GameCommand::Turn {
                            snake_id,
                            direction,
                        },
                    };
                    records.push(TraceRecord::CmdIn {
                        ts_ms: ts,
                        cmd: cmd.clone(),
                    });
                    let scheduled = engine.process_command(cmd).unwrap();
                    stream_seq += 1;
                    records.push(TraceRecord::EventOut {
                        ts_ms: ts,
                        msg: Box::new(GameEventMessage {
                            game_id: GAME_ID,
                            tick: engine.current_tick(),
                            sequence: engine.get_committed_state().event_sequence + 1,
                            stream_seq,
                            user_id: None,
                            event: GameEvent::CommandScheduled {
                                command_message: scheduled,
                            },
                        }),
                    });
                }
            }

            if (ts - START_MS) % 500 == 0 && ts > START_MS {
                let hash = engine.committed_sync_hash();
                let tick = engine.current_tick();
                stream_seq += 1;
                records.push(TraceRecord::EventOut {
                    ts_ms: ts,
                    msg: Box::new(GameEventMessage {
                        game_id: GAME_ID,
                        tick,
                        sequence: engine.get_committed_state().event_sequence,
                        stream_seq,
                        user_id: None,
                        event: GameEvent::TickHash {
                            hash,
                            server_ts_ms: ts,
                        },
                    }),
                });
                records.push(TraceRecord::Fingerprint {
                    ts_ms: ts,
                    tick,
                    hash,
                });
            }

            ts += 50;
        }

        records
    }

    /// Derive the client-side trace a lossless client would have recorded for
    /// the given server trace: every EventOut received as an EventIn, every
    /// CmdIn preceded by its CmdOut 40ms earlier, fingerprints sampled at
    /// every TickHash by actually re-driving a client engine.
    fn build_matching_client_trace(server_records: &[TraceRecord]) -> Vec<TraceRecord> {
        let mut records = vec![TraceRecord::Meta {
            version: TRACE_FORMAT_VERSION,
            side: TraceSide::Client,
            game_id: GAME_ID,
            session: "synthetic-client".into(),
            ts_ms: START_MS,
            build: "test".into(),
            tick_duration_ms: TICK_MS as u32,
        }];

        let mut engine: Option<GameEngine> = None;
        for record in server_records {
            match record {
                TraceRecord::CmdIn { ts_ms, cmd } => {
                    let cmd_out_ts = ts_ms - 40;
                    records.push(TraceRecord::CmdOut {
                        ts_ms: cmd_out_ts,
                        predicted_tick: cmd.command_id_client.tick,
                        cmd: cmd.clone(),
                    });
                    if let Some(engine) = engine.as_mut() {
                        schedule_recorded_client_command(engine, GAME_ID, cmd).unwrap();
                    }
                }
                TraceRecord::EventOut { ts_ms, msg } => {
                    if engine.is_none() {
                        if let GameEvent::Snapshot { game_state } = &msg.event {
                            engine = Some(GameEngine::new_from_state(GAME_ID, game_state.clone()));
                        } else {
                            continue; // client discards pre-snapshot messages
                        }
                    }
                    let engine = engine.as_mut().unwrap();
                    records.push(TraceRecord::EventIn {
                        ts_ms: ts_ms + 30,
                        committed_tick: engine.current_tick(),
                        msg: msg.clone(),
                    });
                    engine.process_server_event(msg).unwrap();
                    if matches!(msg.event, GameEvent::TickHash { .. }) {
                        records.push(TraceRecord::Fingerprint {
                            ts_ms: ts_ms + 30,
                            tick: engine.current_tick(),
                            hash: engine.committed_sync_hash(),
                        });
                    }
                }
                _ => {}
            }
        }

        records
    }

    fn gameplay_event_count(records: &[TraceRecord]) -> usize {
        records
            .iter()
            .filter(|r| {
                matches!(
                    r,
                    TraceRecord::EventOut { msg, .. }
                        if !matches!(msg.event, GameEvent::Snapshot { .. } | GameEvent::TickHash { .. })
                )
            })
            .count()
    }

    #[test]
    fn server_replay_is_deterministic() {
        let records = build_synthetic_server_trace();
        assert!(
            gameplay_event_count(&records) > 4,
            "synthetic trace should contain real gameplay events"
        );

        let outcome = ServerReplay::from_records(records)
            .unwrap()
            .replay()
            .unwrap();
        assert!(
            outcome.deterministic,
            "expected deterministic replay, got {:?}",
            outcome.first_divergence
        );
        assert!(outcome.ticks_replayed >= 25);
        assert!(outcome.events_compared > 4);
    }

    #[test]
    fn tampered_event_diverges_at_the_right_tick() {
        let mut records = build_synthetic_server_trace();

        // Corrupt the first recorded SnakeTurned event: flip its direction.
        let mut tampered_tick = None;
        for record in records.iter_mut() {
            if let TraceRecord::EventOut { msg, .. } = record
                && let GameEvent::SnakeTurned { direction, .. } = &mut msg.event
            {
                *direction = Direction::Down;
                tampered_tick = Some(msg.tick);
                break;
            }
        }
        let tampered_tick = tampered_tick.expect("synthetic trace has a SnakeTurned event");

        let outcome = ServerReplay::from_records(records)
            .unwrap()
            .replay()
            .unwrap();
        assert!(!outcome.deterministic);
        let divergence = outcome.first_divergence.expect("divergence expected");
        assert_eq!(divergence.tick, tampered_tick);
        assert_eq!(divergence.kind, "event_mismatch");
    }

    #[test]
    fn client_replay_reproduces_recorded_fingerprints() {
        let server_records = build_synthetic_server_trace();
        let client_records = build_matching_client_trace(&server_records);

        let outcome = ClientReplay::from_records(client_records)
            .unwrap()
            .replay()
            .unwrap();
        assert!(
            outcome.reproduces,
            "expected reproduction, got {:?}",
            outcome.first_divergence
        );
        assert!(outcome.fingerprints_compared >= 5);
    }

    #[test]
    fn dropped_message_reports_exact_missing_seq_and_transport_loss() {
        let server_records = build_synthetic_server_trace();
        let mut client_records = build_matching_client_trace(&server_records);

        // Drop one mid-stream gameplay EventIn from the client trace.
        let victim_idx = {
            let event_in_indices: Vec<usize> = client_records
                .iter()
                .enumerate()
                .filter(|(_, r)| {
                    matches!(
                        r,
                        TraceRecord::EventIn { msg, .. }
                            if !matches!(msg.event, GameEvent::Snapshot { .. })
                    )
                })
                .map(|(i, _)| i)
                .collect();
            event_in_indices[event_in_indices.len() / 2]
        };
        let dropped_seq = match &client_records[victim_idx] {
            TraceRecord::EventIn { msg, .. } => msg.stream_seq,
            _ => unreachable!(),
        };
        assert!(dropped_seq > 0);
        client_records.remove(victim_idx);

        let report = diff_traces(&server_records, &client_records);
        assert_eq!(
            report.missing_stream_seqs,
            vec![SeqRange {
                start: dropped_seq,
                end: dropped_seq
            }]
        );
        assert_eq!(report.verdict(), "TRANSPORT_LOSS");

        // Both commands matched with the synthetic 40ms wire latency.
        assert_eq!(report.command_latency.len(), 2);
        for entry in &report.command_latency {
            assert_eq!(entry.wire_latency_ms, Some(40));
            assert!(entry.server_tick.is_some());
        }
    }

    #[test]
    fn lossless_traces_are_in_sync() {
        let server_records = build_synthetic_server_trace();
        let client_records = build_matching_client_trace(&server_records);
        let report = diff_traces(&server_records, &client_records);
        assert!(report.missing_stream_seqs.is_empty());
        assert!(report.first_fingerprint_mismatch.is_none());
        assert_eq!(report.verdict(), "IN_SYNC");
    }

    /// Not a test of behavior: writes the synthetic demo traces to disk for
    /// exercising the `trace_rca` CLI end-to-end. Opt-in via env var so
    /// normal test runs stay side-effect free.
    #[test]
    fn write_demo_traces_for_cli() {
        let Ok(dir) = std::env::var("SNAKETRON_DEMO_TRACE_DIR") else {
            return;
        };
        let server_records = build_synthetic_server_trace();
        let client_records = build_matching_client_trace(&server_records);
        let dir = std::path::PathBuf::from(dir);
        for (name, records) in [
            ("server_trace.jsonl", &server_records),
            ("client_trace.jsonl", &client_records),
        ] {
            let path = dir.join(name);
            let _ = std::fs::remove_file(&path);
            let mut writer = crate::trace::TraceWriter::create(&path).unwrap();
            for r in records {
                writer.record(r).unwrap();
            }
            writer.flush().unwrap();
        }
    }
}
