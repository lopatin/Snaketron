use super::*;
use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::Path;

pub struct ReplayReader;

impl ReplayReader {
    pub fn load_replay(path: &Path) -> Result<ReplayData> {
        let file =
            File::open(path).with_context(|| format!("Failed to open replay file: {:?}", path))?;
        let decoder = GzDecoder::new(file);
        let reader = BufReader::new(decoder);
        let mut lines = reader.lines();

        // Parse metadata (first line)
        let metadata_line = lines
            .next()
            .context("Replay file is empty")?
            .context("Failed to read metadata line")?;
        let metadata: ReplayMetadata =
            serde_json::from_str(&metadata_line).context("Failed to parse replay metadata")?;

        // Parse initial state (second line)
        let initial_state_line = lines
            .next()
            .context("Replay file missing initial state")?
            .context("Failed to read initial state line")?;
        let initial_state: GameState = serde_json::from_str(&initial_state_line)
            .context("Failed to parse initial game state")?;

        // Parse events
        let mut events = Vec::new();
        for (i, line_result) in lines.enumerate() {
            let line =
                line_result.with_context(|| format!("Failed to read event line {}", i + 3))?;
            let event: TimestampedEvent = serde_json::from_str(&line)
                .with_context(|| format!("Failed to parse event on line {}", i + 3))?;
            events.push(event);
        }

        Ok(ReplayData {
            metadata,
            initial_state,
            events,
        })
    }

    pub fn list_replays(dir: &Path) -> Result<Vec<PathBuf>> {
        if !dir.exists() {
            return Ok(Vec::new());
        }

        let mut replays = Vec::new();

        // Helper function to recursively find replay files
        fn find_replay_files(dir: &Path, replays: &mut Vec<PathBuf>) -> Result<()> {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();

                if path.is_dir() {
                    // Recursively search subdirectories (e.g., test directories)
                    find_replay_files(&path, replays).ok();
                } else if path.extension() == Some(OsStr::new("replay")) {
                    replays.push(path);
                }
            }
            Ok(())
        }

        // Find all replay files recursively
        find_replay_files(dir, &mut replays)?;

        // Sort by modification time (newest first)
        replays.sort_by(|a, b| {
            let a_time = a
                .metadata()
                .and_then(|m| m.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            let b_time = b
                .metadata()
                .and_then(|m| m.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            b_time.cmp(&a_time)
        });

        Ok(replays)
    }
}
