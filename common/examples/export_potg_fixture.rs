//! Regenerates the browser QA's production-shaped Play-of-the-Game fixture.
//!
//! The clip is written to stdout, so the redirect is not optional — without it
//! the fixture on disk keeps whatever scoring rules and gameplay version it was
//! last written under, and the QA lab silently exercises a stale clip. Run from
//! the repository root:
//!
//! `cargo run -q -p common --example export_potg_fixture > client/web/fixtures/potg-goal-run.json`
//!
//! Regenerate after any change to `HighlightConfig`'s defaults (including its
//! `rules_version`), to `GAMEPLAY_REPLAY_VERSION`, or to the serialized shape
//! of `GameState`: the fixture embeds all three and nothing else validates it.

use anyhow::{Context, Result};
use common::{
    Direction, GAME_RECORDING_FORMAT_VERSION, GAMEPLAY_REPLAY_VERSION, GameEvent, GameRecordingV1,
    HighlightConfig, Position, RecordedGameMessage, ReplayAnchor, ReplayVisibility,
    ScenarioCommand, ScenarioCommandKind, ScenarioScript, select_highlight,
};

fn main() -> Result<()> {
    let mut script =
        ScenarioScript::from_json(include_str!("../../client/web/scenarios/team-bank.json"))?;
    script.id = "potg-goal-run-fixture".into();
    script.run_ticks = 230;
    script.expect.clear();

    // Give the loaded runner a short, authored setup route before turning for
    // home. The genuine banking event then lands after the scorer's six-second
    // lead-in and leaves room for its full three-second post-roll.
    script.pose.snakes[0].body = vec![Position { x: 43, y: 20 }, Position { x: 43, y: 24 }];
    script.pose.snakes[0].direction = Direction::Up;
    script.pose.snakes[1].body = vec![Position { x: 20, y: 30 }, Position { x: 16, y: 30 }];
    script.pose.snakes[1].direction = Direction::Right;
    script.pose.snakes[1].is_alive = Some(false);
    script.commands = vec![
        ScenarioCommand {
            at_tick: 28,
            user_id: 1,
            command: ScenarioCommandKind::Turn(Direction::Right),
        },
        ScenarioCommand {
            at_tick: 38,
            user_id: 1,
            command: ScenarioCommandKind::Turn(Direction::Down),
        },
        ScenarioCommand {
            at_tick: 66,
            user_id: 1,
            command: ScenarioCommandKind::Turn(Direction::Left),
        },
    ];

    let loaded = script.load()?;
    let anchor = loaded.initial_state.clone();
    let run = loaded.run()?;
    let mut events: Vec<(u32, GameEvent)> = (10..=run.final_state.tick)
        .step_by(10)
        .map(|tick| {
            (
                tick,
                GameEvent::TickHash {
                    hash: 0,
                    server_ts_ms: i64::from(tick) * 100,
                },
            )
        })
        .chain(run.events.iter().filter_map(|(tick, _, event)| {
            (!matches!(event, GameEvent::Snapshot { .. })).then_some((*tick, event.clone()))
        }))
        .collect();
    events.sort_by_key(|(tick, event)| (*tick, !matches!(event, GameEvent::TickHash { .. })));
    let messages = events
        .into_iter()
        .enumerate()
        .map(|(index, (tick, event))| RecordedGameMessage {
            tick,
            sequence: index as u64 + 1,
            event,
        })
        .collect();
    let recording = GameRecordingV1 {
        format_version: GAME_RECORDING_FORMAT_VERSION,
        gameplay_version: GAMEPLAY_REPLAY_VERSION,
        game_id: 4242,
        visibility: ReplayVisibility::Public,
        anchors: vec![ReplayAnchor {
            tick: anchor.tick,
            sequence: 0,
            state: anchor,
        }],
        messages,
        end_tick: run.final_state.tick,
        end_sync_hash: run.final_state.sync_hash(),
    };
    recording.verify_end_hash()?;
    let clip = select_highlight(&recording, &HighlightConfig::default())?
        .context("the QA recording did not clear the PotG threshold")?;
    clip.replay_and_verify()?;

    println!("{}", serde_json::to_string_pretty(&clip)?);
    Ok(())
}
