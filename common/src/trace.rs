//! Sync trace ("flight recorder") format shared by the server, the web client,
//! and the offline root-cause-analysis tooling.
//!
//! A trace is a JSONL stream of `TraceRecord`s describing everything a game
//! engine instance observed: the initial state, every command and event that
//! crossed its boundary (with timestamps), periodic state fingerprints, clock
//! sync samples, and anomaly notes. A server trace and a client trace for the
//! same game can be joined offline to find the first divergent tick, missing
//! events, and command-scheduling latency — and to re-drive the engine
//! deterministically as a local reproduction of a production bug.
//!
//! Records are plain serde data so the same shapes work in native and WASM
//! builds; the TypeScript client writes literally the same JSON. File I/O
//! lives behind `#[cfg(not(target_arch = "wasm32"))]`.

use crate::{GameCommandMessage, GameEventMessage, GameState};
use serde::{Deserialize, Serialize};

pub const TRACE_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraceSide {
    Server,
    Client,
}

// NOTE: externally tagged (serde default) on purpose — internally-tagged enums
// (`#[serde(tag = ...)]`) buffer content through serde's private Content type,
// which cannot deserialize integer-keyed maps like GameState's HashMap<u32, _>.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TraceRecord {
    /// First record of every trace.
    Meta {
        version: u32,
        side: TraceSide,
        game_id: u32,
        /// Distinguishes multiple traces of the same game (e.g. per user).
        session: String,
        ts_ms: i64,
        build: String,
        tick_duration_ms: u32,
    },
    /// Full engine state, including RNG on the server side. The first `State`
    /// record is the replay starting point; later ones (e.g. applied
    /// snapshots) re-anchor it.
    State {
        ts_ms: i64,
        tick: u32,
        state: Box<GameState>,
    },
    /// Server perspective: a message published toward clients.
    EventOut {
        ts_ms: i64,
        msg: Box<GameEventMessage>,
    },
    /// Client perspective: a message received from the server.
    /// `committed_tick` is the local committed tick at the moment of receipt,
    /// which exposes how far ahead/behind the client was.
    EventIn {
        ts_ms: i64,
        committed_tick: u32,
        msg: Box<GameEventMessage>,
    },
    /// Server perspective: a player command received for scheduling.
    CmdIn { ts_ms: i64, cmd: GameCommandMessage },
    /// Client perspective: a locally predicted command sent to the server.
    /// `predicted_tick` is the tick the client scheduled it at.
    CmdOut {
        ts_ms: i64,
        predicted_tick: u32,
        cmd: GameCommandMessage,
    },
    /// Periodic state fingerprint of this side's committed state.
    Fingerprint { ts_ms: i64, tick: u32, hash: u64 },
    /// Client clock-sync sample.
    Clock {
        ts_ms: i64,
        drift_ms: f64,
        rtt_ms: f64,
    },
    /// Anomalies observed by the runtime: detected sequence gaps, hash
    /// mismatches, channel overflows, lag warnings, reconnects...
    Note { ts_ms: i64, note: String },
}

impl TraceRecord {
    pub fn ts_ms(&self) -> i64 {
        match self {
            TraceRecord::Meta { ts_ms, .. }
            | TraceRecord::State { ts_ms, .. }
            | TraceRecord::EventOut { ts_ms, .. }
            | TraceRecord::EventIn { ts_ms, .. }
            | TraceRecord::CmdIn { ts_ms, .. }
            | TraceRecord::CmdOut { ts_ms, .. }
            | TraceRecord::Fingerprint { ts_ms, .. }
            | TraceRecord::Clock { ts_ms, .. }
            | TraceRecord::Note { ts_ms, .. } => *ts_ms,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub use io::{TraceWriter, read_trace};

#[cfg(not(target_arch = "wasm32"))]
mod io {
    use super::TraceRecord;
    use anyhow::{Context, Result};
    use std::fs::{File, OpenOptions};
    use std::io::{BufRead, BufReader, BufWriter, Write};
    use std::path::{Path, PathBuf};

    /// Append-only JSONL trace writer. Flushes on every batch boundary so a
    /// crashed or killed game still leaves a readable trace — the moments a
    /// game dies unexpectedly are exactly the ones worth debugging.
    pub struct TraceWriter {
        path: PathBuf,
        writer: BufWriter<File>,
    }

    impl TraceWriter {
        pub fn create(path: impl AsRef<Path>) -> Result<Self> {
            let path = path.as_ref().to_path_buf();
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .with_context(|| format!("Failed to create trace dir {:?}", parent))?;
            }
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .with_context(|| format!("Failed to open trace file {:?}", path))?;
            Ok(TraceWriter {
                path,
                writer: BufWriter::new(file),
            })
        }

        pub fn path(&self) -> &Path {
            &self.path
        }

        pub fn record(&mut self, record: &TraceRecord) -> Result<()> {
            let line = serde_json::to_string(record).context("Failed to serialize trace record")?;
            writeln!(self.writer, "{}", line).context("Failed to write trace record")?;
            Ok(())
        }

        pub fn flush(&mut self) -> Result<()> {
            self.writer.flush().context("Failed to flush trace file")
        }
    }

    /// Read a JSONL trace, tolerating a truncated final line (crash mid-write).
    pub fn read_trace(path: impl AsRef<Path>) -> Result<Vec<TraceRecord>> {
        let path = path.as_ref();
        let file =
            File::open(path).with_context(|| format!("Failed to open trace file {:?}", path))?;
        let lines: Vec<String> = BufReader::new(file)
            .lines()
            .collect::<std::io::Result<_>>()
            .with_context(|| format!("Failed to read trace file {:?}", path))?;

        let mut records = Vec::new();
        for (idx, line) in lines.iter().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<TraceRecord>(line) {
                Ok(record) => records.push(record),
                // A torn final line is expected after a crash; anything
                // else mid-file is corruption worth failing loudly on.
                Err(_) if idx + 1 == lines.len() => break,
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Corrupt trace record at {:?}:{}: {}",
                        path,
                        idx + 1,
                        e
                    ));
                }
            }
        }
        Ok(records)
    }
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests {
    use super::*;
    use crate::{GameType, QueueMode};

    #[test]
    fn trace_roundtrip() {
        let dir = std::env::temp_dir().join("snaketron_trace_test");
        let path = dir.join("roundtrip.jsonl");
        let _ = std::fs::remove_file(&path);

        let state = GameState::new(
            10,
            10,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            Some(7),
            0,
        );

        let records = vec![
            TraceRecord::Meta {
                version: TRACE_FORMAT_VERSION,
                side: TraceSide::Server,
                game_id: 1,
                session: "test".into(),
                ts_ms: 100,
                build: "test-build".into(),
                tick_duration_ms: 100,
            },
            TraceRecord::State {
                ts_ms: 100,
                tick: 0,
                state: Box::new(state),
            },
            TraceRecord::Fingerprint {
                ts_ms: 200,
                tick: 1,
                hash: 0xdeadbeef,
            },
            TraceRecord::Note {
                ts_ms: 300,
                note: "gap detected: expected stream_seq 5, got 9".into(),
            },
        ];

        let mut writer = TraceWriter::create(&path).unwrap();
        for r in &records {
            writer.record(r).unwrap();
        }
        writer.flush().unwrap();

        let read = read_trace(&path).unwrap();
        assert_eq!(read.len(), records.len());
        assert!(matches!(read[0], TraceRecord::Meta { game_id: 1, .. }));
        assert!(matches!(read[2], TraceRecord::Fingerprint { hash, .. } if hash == 0xdeadbeef));

        let _ = std::fs::remove_file(&path);
    }
}
