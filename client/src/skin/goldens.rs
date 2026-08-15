//! The parity gate.
//!
//! These traces were recorded from the renderer *before* any skin extraction
//! and are the definition of "the default skin still looks exactly like it used
//! to". Every later refactor — the corpse rewrite, the trait, the mask hoist,
//! the document interpreter — has to reproduce them byte for byte.
//!
//! Regenerate deliberately and never casually:
//!
//! ```text
//! SKIN_GOLDEN_BLESS=1 cargo test -p client skin::goldens
//! ```
//!
//! A blessed change means pixels moved. The diff is the review.

use crate::render::{RosterFacing, roster_label_ink, roster_snake_layout, snake_palette};
use crate::skin::corpse::paint_dead_arena;
use crate::skin::fixtures::{ARENA_BODIES, CELL_SIZES, POSES, ROTATIONS};
use crate::skin::paint::{OpRecorder, PaintCtx};
use crate::skin::{
    ClassicSkin, SkinIdentity, SnakePose, SnakeRole, SnakeSkin, paint_alive_with_occlusion,
};

/// The identity that resolves to the colours the original traces were recorded
/// with (`BLUE[1]`), so the trace still describes the same picture.
const ARENA_IDENTITY: SkinIdentity = SkinIdentity {
    role: SnakeRole::Teammate,
    shade_slot: 1,
};

/// `RED[0]` — what the roster traces were recorded with.
const ROSTER_IDENTITY: SkinIdentity = SkinIdentity {
    role: SnakeRole::Enemy,
    shade_slot: 0,
};

/// Path of the committed trace, relative to the crate root.
const GOLDEN_PATH: &str = "src/skin/goldens/classic.trace";

fn record(paint: impl FnOnce(&mut PaintCtx)) -> String {
    let mut recorder = OpRecorder::new();
    paint(&mut PaintCtx::recording(&mut recorder));
    recorder.to_golden()
}

/// Every input combination `snake_palette` distinguishes.
///
/// This section is the reason the trait refactor is safe: `SnakeRole` has to
/// collapse these seven inputs into a role without changing a single output,
/// and an exhaustive table is the only way to know it didn't.
fn palette_section(out: &mut String) {
    out.push_str("## palette\n");
    for is_team_game in [false, true] {
        for local_snake_id in [None, Some(0usize), Some(1)] {
            for local_team in [None, Some(0u8), Some(1)] {
                for snake_team in [None, Some(0u8), Some(1)] {
                    for team_member_slot in 0..3usize {
                        for snake_count in 1..=4usize {
                            for snake_index in 0..4usize {
                                let (fill, outline) = snake_palette(
                                    snake_index,
                                    snake_team,
                                    team_member_slot,
                                    snake_count,
                                    is_team_game,
                                    local_snake_id,
                                    local_team,
                                );
                                out.push_str(&format!(
                                    "team={is_team_game} local_snake={local_snake_id:?} \
                                     local_team={local_team:?} snake_team={snake_team:?} \
                                     slot={team_member_slot} count={snake_count} \
                                     index={snake_index} -> {fill} {outline}\n"
                                ));
                            }
                        }
                    }
                }
            }
        }
    }
}

/// The living-snake painter across every pose, boost state, mask mode, and
/// cell size the arena and roster actually produce.
fn alive_section(out: &mut String) {
    out.push_str("\n## alive\n");
    for pose in POSES {
        for &cell_size in CELL_SIZES {
            for boost_active in [false, true] {
                for mask in [None, Some("#ffffff")] {
                    let mask_label = mask.unwrap_or("none");
                    out.push_str(&format!(
                        "\n# alive pose={} cell={cell_size:?} boost={boost_active} mask={mask_label}\n",
                        pose.name
                    ));
                    let trace = record(|ctx| {
                        let posed = SnakePose::still(pose.cells, cell_size, boost_active);
                        paint_alive_with_occlusion(
                            ctx,
                            &ClassicSkin,
                            &posed,
                            &ARENA_IDENTITY,
                            mask,
                        )
                        .expect("recording painter never fails");
                    });
                    out.push_str(&trace);
                }
            }
        }
    }
}

/// The corpse painter across bodies, rotations, and cell sizes.
fn dead_section(out: &mut String) {
    out.push_str("\n## dead\n");
    for body in ARENA_BODIES {
        for &rotation in ROTATIONS {
            for &cell_size in CELL_SIZES {
                out.push_str(&format!(
                    "\n# dead body={} rotation={rotation} cell={cell_size:?}\n",
                    body.name
                ));
                let trace = record(|ctx| {
                    paint_dead_arena(ctx, body.body, cell_size, 40.0, 24.0, rotation)
                        .expect("recording painter never fails");
                });
                out.push_str(&trace);
            }
        }
    }
}

/// Roster geometry and ink, which generic chrome derives from what will become
/// the skin's reported metrics. Pinning the numbers here is what proves the
/// `overhang_px` change is unit-correct rather than off by a factor of two.
fn roster_section(out: &mut String) {
    out.push_str("\n## roster\n");
    for (width, height) in [(124.0, 19.0), (96.0, 14.0), (200.0, 32.0)] {
        for boost_active in [false, true] {
            let layout = roster_snake_layout(width, height, boost_active);
            for facing in [RosterFacing::Right, RosterFacing::Left] {
                let cells = layout.body_cells(facing);
                out.push_str(&format!(
                    "roster w={width:?} h={height:?} boost={boost_active} facing={facing:?} \
                     cells={cells:?}\n"
                ));
            }
            out.push_str(&format!(
                "\n# roster glyph w={width:?} h={height:?} boost={boost_active}\n"
            ));
            let trace = record(|ctx| {
                let cells = layout.body_cells(RosterFacing::Right);
                let posed = SnakePose::still(&cells, layout.cell_size(), boost_active);
                paint_alive_with_occlusion(ctx, &ClassicSkin, &posed, &ROSTER_IDENTITY, None)
                    .expect("recording painter never fails");
            });
            out.push_str(&trace);
        }
    }

    out.push_str("\n## base_theme\n");
    // The arena's dressing, arranged for the screen. Pinning it here is what
    // proves routing zones, walls, and endzone text through the viewer's skin
    // did not move a single colour.
    for local_team in [None, Some(0u8), Some(1u8)] {
        let sides = ClassicSkin
            .base_theme()
            .expect("the classic skin dresses the arena")
            .sides(local_team);
        out.push_str(&format!(
            "local_team={local_team:?} zones={}/{} walls={}/{} text={}/{}\n",
            sides.left_zone,
            sides.right_zone,
            sides.left_wall,
            sides.right_wall,
            sides.left_text,
            sides.right_text
        ));
    }

    out.push_str("\n## celebration_theme\n");
    let celebration = ClassicSkin
        .celebration_theme()
        .expect("the classic skin dresses its celebration");
    out.push_str(&format!(
        "effect={} accents={}/{} readouts={}/{}\n",
        celebration.effect,
        celebration.friendly_accent,
        celebration.enemy_accent,
        celebration.readout_friendly,
        celebration.readout_enemy
    ));

    out.push_str("\n## label_ink\n");
    for fill in [
        "#70bfe3", "#3c8dde", "#ff6b6b", "#e34e5b", "#556270", "#f7b731",
    ] {
        out.push_str(&format!("{fill} -> {}\n", roster_label_ink(fill)));
    }
}

/// Build the complete trace document.
pub fn render_all() -> String {
    let mut out = String::new();
    out.push_str("# Snaketron classic skin — recorded canvas operations.\n");
    out.push_str("# Regenerate with SKIN_GOLDEN_BLESS=1 cargo test -p client skin::goldens\n");
    palette_section(&mut out);
    alive_section(&mut out);
    dead_section(&mut out);
    roster_section(&mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn golden_file() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(GOLDEN_PATH)
    }

    #[test]
    fn classic_skin_matches_its_committed_trace() {
        let rendered = render_all();
        let path = golden_file();

        if std::env::var("SKIN_GOLDEN_BLESS").is_ok() {
            std::fs::create_dir_all(path.parent().expect("golden has a parent"))
                .expect("golden directory is writable");
            std::fs::write(&path, &rendered).expect("golden is writable");
            return;
        }

        let committed = std::fs::read_to_string(&path).unwrap_or_else(|error| {
            panic!(
                "missing golden at {}: {error}\n\
                 Record it with SKIN_GOLDEN_BLESS=1 cargo test -p client skin::goldens",
                path.display()
            )
        });

        if committed == rendered {
            return;
        }

        // Report the first divergence rather than dumping tens of thousands of
        // identical lines at whoever broke it.
        let (line_number, expected, actual) = committed
            .lines()
            .zip(rendered.lines())
            .enumerate()
            .find(|(_, (a, b))| a != b)
            .map(|(index, (a, b))| (index + 1, a.to_string(), b.to_string()))
            .unwrap_or_else(|| {
                (
                    committed.lines().count().min(rendered.lines().count()) + 1,
                    format!("<{} lines>", committed.lines().count()),
                    format!("<{} lines>", rendered.lines().count()),
                )
            });

        panic!(
            "the default skin no longer paints what it used to.\n\
             First divergence at line {line_number} of {}:\n  \
             committed: {expected}\n  \
             rendered:  {actual}\n\
             If this change is intended, re-record with \
             SKIN_GOLDEN_BLESS=1 cargo test -p client skin::goldens and review the diff.",
            path.display()
        );
    }
}
