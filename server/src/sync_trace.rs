//! Server-side flight recorder for game sync traces.
//!
//! Wraps `common::trace::TraceWriter` with environment-driven configuration,
//! file rotation (oldest traces pruned beyond a cap), and hard fault
//! isolation: recorder failures log, disable the recorder, and never
//! propagate into the game loop.

use anyhow::{Context, Result, bail};
use common::trace::{TraceRecord, TraceWriter};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tracing::warn;

pub const DEFAULT_TRACE_DIR: &str = "./traces";
pub const DEFAULT_TRACE_MAX_FILES: usize = 200;

#[derive(Debug, Clone)]
pub struct TraceConfig {
    pub enabled: bool,
    pub dir: PathBuf,
    pub max_files: usize,
}

impl TraceConfig {
    /// Tracing is enabled by default; `SNAKETRON_TRACE_DISABLE=1` disables it,
    /// `SNAKETRON_TRACE_DIR` overrides the directory, and
    /// `SNAKETRON_TRACE_MAX_FILES` caps the number of retained trace files.
    pub fn from_env() -> Self {
        let enabled = std::env::var("SNAKETRON_TRACE_DISABLE")
            .map(|v| v != "1")
            .unwrap_or(true);
        let dir =
            std::env::var("SNAKETRON_TRACE_DIR").unwrap_or_else(|_| DEFAULT_TRACE_DIR.to_string());
        let max_files = std::env::var("SNAKETRON_TRACE_MAX_FILES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_TRACE_MAX_FILES);
        Self {
            enabled,
            dir: dir.into(),
            max_files,
        }
    }
}

fn unix_ts() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Delete the oldest `.jsonl` files in `dir` until at most `keep` remain.
/// Sorted by modification time (name as tiebreaker) so freshly written traces
/// survive.
fn prune_trace_files(dir: &Path, keep: usize) -> Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        // Directory not created yet: nothing to prune.
        Err(_) => return Ok(()),
    };

    let mut files: Vec<(SystemTime, PathBuf)> = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
            continue;
        }
        let modified = entry
            .metadata()
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        files.push((modified, path));
    }

    if files.len() <= keep {
        return Ok(());
    }

    files.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    let excess = files.len() - keep;
    for (_, path) in files.into_iter().take(excess) {
        std::fs::remove_file(&path)
            .with_context(|| format!("Failed to prune trace file {:?}", path))?;
    }
    Ok(())
}

/// Flight recorder for a single game. All methods are infallible from the
/// caller's perspective: any I/O error logs a warning and disables the
/// recorder for the rest of the game.
pub struct GameTraceRecorder {
    writer: Option<TraceWriter>,
    game_id: u32,
}

impl GameTraceRecorder {
    pub fn for_server_game(game_id: u32) -> Self {
        Self::new(&TraceConfig::from_env(), game_id)
    }

    pub fn new(config: &TraceConfig, game_id: u32) -> Self {
        if !config.enabled {
            return Self {
                writer: None,
                game_id,
            };
        }

        // Leave room for the file about to be created.
        if let Err(e) = prune_trace_files(&config.dir, config.max_files.saturating_sub(1)) {
            warn!("Failed to prune trace files in {:?}: {}", config.dir, e);
        }

        let path = config
            .dir
            .join(format!("game_{}_server_{}.jsonl", game_id, unix_ts()));
        let writer = match TraceWriter::create(&path) {
            Ok(writer) => Some(writer),
            Err(e) => {
                warn!(
                    "Failed to create trace file for game {}: {}; tracing disabled for this game",
                    game_id, e
                );
                None
            }
        };
        Self { writer, game_id }
    }

    pub fn is_enabled(&self) -> bool {
        self.writer.is_some()
    }

    pub fn path(&self) -> Option<&Path> {
        self.writer.as_ref().map(|w| w.path())
    }

    pub fn record(&mut self, record: &TraceRecord) {
        if let Some(writer) = self.writer.as_mut()
            && let Err(e) = writer.record(record)
        {
            warn!(
                "Trace write failed for game {}: {}; tracing disabled for this game",
                self.game_id, e
            );
            self.writer = None;
        }
    }

    pub fn note(&mut self, note: impl Into<String>) {
        self.record(&TraceRecord::Note {
            ts_ms: chrono::Utc::now().timestamp_millis(),
            note: note.into(),
        });
    }

    pub fn flush(&mut self) {
        if let Some(writer) = self.writer.as_mut()
            && let Err(e) = writer.flush()
        {
            warn!(
                "Trace flush failed for game {}: {}; tracing disabled for this game",
                self.game_id, e
            );
            self.writer = None;
        }
    }
}

/// Write a client-uploaded trace into the shared trace directory as
/// `game_<id>_client_<user>_<unix_ts>.jsonl`. Unlike `GameTraceRecorder`,
/// failures here are surfaced to the caller (the HTTP handler reports them).
pub fn write_client_trace(
    config: &TraceConfig,
    game_id: u32,
    user_id: u32,
    records: &[TraceRecord],
) -> Result<PathBuf> {
    if !config.enabled {
        bail!("tracing is disabled (SNAKETRON_TRACE_DISABLE=1)");
    }

    if let Err(e) = prune_trace_files(&config.dir, config.max_files.saturating_sub(1)) {
        warn!("Failed to prune trace files in {:?}: {}", config.dir, e);
    }

    let path = config.dir.join(format!(
        "game_{}_client_{}_{}.jsonl",
        game_id,
        user_id,
        unix_ts()
    ));
    let mut writer = TraceWriter::create(&path)?;
    for record in records {
        writer.record(record)?;
    }
    writer.flush()?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::trace::{TRACE_FORMAT_VERSION, TraceSide, read_trace};

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "snaketron_sync_trace_{}_{}",
            name,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn config(dir: &Path, max_files: usize) -> TraceConfig {
        TraceConfig {
            enabled: true,
            dir: dir.to_path_buf(),
            max_files,
        }
    }

    #[test]
    fn recorder_writes_readable_trace() {
        let dir = test_dir("write");
        let mut recorder = GameTraceRecorder::new(&config(&dir, 10), 42);
        assert!(recorder.is_enabled());

        recorder.record(&TraceRecord::Meta {
            version: TRACE_FORMAT_VERSION,
            side: TraceSide::Server,
            game_id: 42,
            session: "1".into(),
            ts_ms: 100,
            build: "test".into(),
            tick_duration_ms: 100,
        });
        recorder.note("hello");
        recorder.flush();

        let path = recorder.path().unwrap().to_path_buf();
        let records = read_trace(&path).unwrap();
        assert_eq!(records.len(), 2);
        assert!(matches!(records[0], TraceRecord::Meta { game_id: 42, .. }));
        assert!(matches!(&records[1], TraceRecord::Note { note, .. } if note == "hello"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn disabled_recorder_writes_nothing() {
        let dir = test_dir("disabled");
        let mut recorder = GameTraceRecorder::new(
            &TraceConfig {
                enabled: false,
                dir: dir.clone(),
                max_files: 10,
            },
            1,
        );
        assert!(!recorder.is_enabled());
        recorder.note("ignored");
        recorder.flush();

        let count = std::fs::read_dir(&dir).unwrap().count();
        assert_eq!(count, 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn recorder_prunes_oldest_files() {
        let dir = test_dir("prune");
        // Pre-create 5 trace files; creation order gives increasing mtimes
        // (name is the tiebreaker when the filesystem truncates timestamps).
        for i in 1..=5 {
            std::fs::write(dir.join(format!("game_1_server_{}.jsonl", i)), "{}\n").unwrap();
        }

        let recorder = GameTraceRecorder::new(&config(&dir, 3), 99);
        assert!(recorder.is_enabled());

        let mut names: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(names.len(), 3, "expected 3 files, got {:?}", names);
        // The three oldest pre-existing files are gone; the two newest remain
        // along with the newly created recorder file.
        assert!(names.contains(&"game_1_server_4.jsonl".to_string()));
        assert!(names.contains(&"game_1_server_5.jsonl".to_string()));
        assert!(names.iter().any(|n| n.starts_with("game_99_server_")));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn client_trace_roundtrip() {
        let dir = test_dir("client");
        let records = vec![TraceRecord::Fingerprint {
            ts_ms: 1,
            tick: 2,
            hash: 3,
        }];
        let path = write_client_trace(&config(&dir, 10), 7, 8, &records).unwrap();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        assert!(name.starts_with("game_7_client_8_"));

        let read = read_trace(&path).unwrap();
        assert_eq!(read.len(), 1);
        assert!(matches!(read[0], TraceRecord::Fingerprint { hash: 3, .. }));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn client_trace_rejected_when_disabled() {
        let dir = test_dir("client_disabled");
        let result = write_client_trace(
            &TraceConfig {
                enabled: false,
                dir: dir.clone(),
                max_files: 10,
            },
            1,
            2,
            &[],
        );
        assert!(result.is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
