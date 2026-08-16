use common::HighlightClip;

const FIXTURE_JSON: &str = include_str!("../../client/web/fixtures/potg-goal-run.json");

#[test]
fn checked_in_browser_highlight_replays_and_keeps_lossless_hash_json() {
    let wire: serde_json::Value = serde_json::from_str(FIXTURE_JSON).unwrap();
    assert!(wire["end_sync_hash"].is_string());

    let clip: HighlightClip = serde_json::from_value(wire).unwrap();
    let final_state = clip.replay_and_verify().unwrap();
    assert_eq!(final_state.sync_hash(), clip.end_sync_hash);
    assert_eq!(
        (clip.window.end_tick - clip.window.start_tick) * clip.anchor.properties.tick_duration_ms,
        9_000
    );
    assert_eq!(clip.viewer_duration_ms(), 12_500);
    assert_eq!(clip.window.focus_tick, clip.breakdown.focus_tick);
}
