//! The checked-in Play-of-the-Game calibration corpus must stay replayable.
//!
//! Every clip carries a `gameplay_version`, and both the browser player and
//! `HighlightClip::replay_and_verify` reject one that differs from
//! `GAMEPLAY_REPLAY_VERSION`. A protocol bump therefore silently strands the
//! whole corpus: the artifacts still parse, the summary tests still pass, and
//! the only symptom is that clips stop playing.
//!
//! Replaying each clip and checking its recorded end hash makes the corpus
//! self-proving. When a bump changes no gameplay, restamping the clips is
//! correct and this test says so; when a bump *does* change gameplay, the hash
//! fails and the corpus genuinely has to be re-recorded.

use common::HighlightClip;
use std::path::{Path, PathBuf};

fn corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("common/ has a parent")
        .join("docs/qa/play-of-the-game-calibration/clips")
}

#[test]
fn every_calibration_clip_replays_to_its_recorded_hash() {
    let mut clips: Vec<PathBuf> = std::fs::read_dir(corpus_dir())
        .expect("calibration corpus is checked in")
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect();
    clips.sort();

    assert!(
        !clips.is_empty(),
        "calibration corpus is empty; the directory moved or was deleted"
    );

    for path in clips {
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{name}: {e}"));
        let clip: HighlightClip =
            serde_json::from_str(&raw).unwrap_or_else(|e| panic!("{name} does not parse: {e}"));

        let final_state = clip
            .replay_and_verify()
            .unwrap_or_else(|e| panic!("{name} does not replay: {e}"));
        assert_eq!(
            final_state.sync_hash(),
            clip.end_sync_hash,
            "{name} replays to a different end state than the one recorded"
        );
    }
}
