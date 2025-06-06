use std::path::{Path, PathBuf};
use std::fs;
use anyhow::{Result, Context};

/// Get the standard replay directory path
pub fn get_replay_directory() -> PathBuf {
    // Use a consistent directory in /tmp/snaketron_replays
    PathBuf::from("/tmp/snaketron_replays")
}

/// Ensure the replay directory exists
pub fn ensure_replay_directory() -> Result<PathBuf> {
    let dir = get_replay_directory();
    fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create replay directory: {:?}", dir))?;
    Ok(dir)
}

/// Get replay directory for tests (isolated per test)
pub fn get_test_replay_directory(test_name: &str) -> PathBuf {
    let base = get_replay_directory();
    base.join(format!("test_{}", test_name))
}

/// Clean up old replay files (optional utility)
pub fn cleanup_old_replays(days: u64) -> Result<usize> {
    let dir = get_replay_directory();
    if !dir.exists() {
        return Ok(0);
    }
    
    let mut removed = 0;
    let cutoff = std::time::SystemTime::now()
        .checked_sub(std::time::Duration::from_secs(days * 24 * 60 * 60))
        .context("Invalid duration")?;
    
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.extension().and_then(|s| s.to_str()) == Some("replay") {
            if let Ok(metadata) = entry.metadata() {
                if let Ok(modified) = metadata.modified() {
                    if modified < cutoff {
                        fs::remove_file(&path)?;
                        removed += 1;
                    }
                }
            }
        }
    }
    
    Ok(removed)
}