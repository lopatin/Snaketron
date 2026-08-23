//! The canonical poses every skin is rendered against.
//!
//! One corpus, shared by the golden traces, the conformance suite, and the
//! `/qa/skins` contact sheet, so "it looked right in the QA route" and "the
//! goldens pass" are statements about the same pictures.

use crate::skin::{SkinIdentity, SnakeRole};
#[cfg(test)]
use common::Position;

/// A named body, head first, in whole screen cells with rotation already
/// applied — the same contract the skin painter has always had.
pub struct Pose {
    pub name: &'static str,
    pub cells: &'static [(f64, f64)],
}

/// Poses chosen to cover every structural branch in the painter: the
/// single-cell early return, a body with no corners, corners in both
/// orientations, several corners in a row, and a body long enough to run the
/// head gradient off its end.
pub const POSES: &[Pose] = &[
    Pose {
        name: "single_cell",
        cells: &[(3.0, 3.0)],
    },
    Pose {
        name: "two_cell",
        cells: &[(4.0, 4.0), (3.0, 4.0)],
    },
    Pose {
        name: "straight_horizontal",
        cells: &[(8.0, 3.0), (3.0, 3.0)],
    },
    Pose {
        name: "straight_vertical",
        cells: &[(3.0, 8.0), (3.0, 3.0)],
    },
    Pose {
        name: "single_corner",
        cells: &[(8.0, 3.0), (3.0, 3.0), (3.0, 8.0)],
    },
    Pose {
        name: "zigzag",
        cells: &[(10.0, 2.0), (6.0, 2.0), (6.0, 6.0), (2.0, 6.0), (2.0, 9.0)],
    },
    Pose {
        name: "longer_than_head_gradient",
        cells: &[(20.0, 4.0), (0.0, 4.0)],
    },
    Pose {
        name: "reversed_travel",
        cells: &[(2.0, 6.0), (9.0, 6.0), (9.0, 1.0)],
    },
    // Everything below this line was added for the shading engine
    // (`specs/skin-shading-prd.md` S1) and recorded from the *unchanged*
    // painter. New poses go on the end so the golden diff stays additive:
    // `alive_section` iterates `POSES` in order, so appending appends blocks
    // and moves none.
    Pose {
        // Consecutive turns, which produce runs exactly one cell long. Legal —
        // `Snake::step_forward` turns on any tick — and absent from the corpus
        // above, so nothing tested a body space whose runs are shorter than a
        // single tile until now.
        name: "one_cell_runs",
        cells: &[
            (4.0, 2.0),
            (4.0, 3.0),
            (5.0, 3.0),
            (5.0, 4.0),
            (6.0, 4.0),
            (6.0, 6.0),
        ],
    },
    Pose {
        // Long enough that a tiled source wraps many times and the head
        // gradient has run out well before the tail. 33 cells against the
        // 10-cell ramp and a 3-cell tile: eleven wraps, two thirds of the body
        // unlit.
        name: "tile_wrapping_length",
        cells: &[(32.0, 8.0), (0.0, 8.0)],
    },
    Pose {
        // The body passes through (4, 2) twice. The engine kills a snake whose
        // head enters its own body, so no live snake looks like this — but arc
        // length and paint dedup give different answers here and nowhere else,
        // so it is the only fixture that can tell the two apart.
        name: "self_crossing",
        cells: &[(2.0, 2.0), (6.0, 2.0), (6.0, 5.0), (4.0, 5.0), (4.0, 1.0)],
    },
    Pose {
        // A wide U-turn: out along one arm, four cells across, and back. It
        // spans the same 21 cells as `longer_than_head_gradient` so the two
        // sit at one width in the Builder's preview row, and it is the pose
        // that shows what `single_corner` cannot — a pattern crossing *two*
        // turns, and the head running back alongside the tail where any
        // mismatch between the two arms is side by side and obvious.
        name: "wide_u_turn",
        cells: &[(0.0, 2.0), (20.0, 2.0), (20.0, 6.0), (0.0, 6.0)],
    },
    Pose {
        // Four cells: the length every snake is dealt at spawn
        // (`DEFAULT_SNAKE_LENGTH` in `common::game_state`). Shorter than the
        // head glow and shorter than most patterns' repeat, so it is the pose
        // that answers "what does this look like in the second before anyone
        // has eaten anything" — which is every player's first sight of it.
        name: "starting_length",
        cells: &[(5.0, 3.0), (2.0, 3.0)],
    },
];

/// Bodies the Builder's preview draws, kept out of the corpus above.
///
/// [`POSES`] is a correctness instrument: every entry is there because the
/// painter has a branch only it reaches, and every entry costs a block of
/// recorded trace plus a pass of the conformance suite at three cell sizes and
/// four clock samples. Four straight bodies at four lengths reach exactly the
/// branches one straight body reaches, so putting them there would buy no
/// coverage and charge for it — and would mean re-recording the goldens every
/// time someone nudged a preview.
///
/// They all start at x = 0, so a column of them lines up on its left edge and
/// the difference between them reads as length rather than as position.
pub const PREVIEW_ONLY_POSES: &[Pose] = &[
    Pose {
        name: "straight_16",
        cells: &[(15.0, 3.0), (0.0, 3.0)],
    },
    Pose {
        name: "straight_18",
        cells: &[(17.0, 3.0), (0.0, 3.0)],
    },
    Pose {
        name: "straight_19",
        cells: &[(18.0, 3.0), (0.0, 3.0)],
    },
    Pose {
        // The image-model geometry guide is rendered through the real painter
        // at the live arena maximum (15 px/cell), then enlarged for review.
        // One empty cell on every side keeps the round caps and any legal
        // in-body antialiasing away from the canvas edge. This is deliberately
        // preview-only: it is a framing fixture, not another conformance case,
        // and adding it to POSES would rewrite the append-only golden corpus.
        name: PROTOTYPE_GEOMETRY_POSE_NAME,
        cells: &[(16.0, 1.0), (1.0, 1.0)],
    },
];

/// Repository-owned image-model guide pose and its exact framing in cells.
///
/// The body occupies x=1..17 and y=1..2 inside an 18x3 canvas. Coordinates
/// are cell origins, head first; therefore `(16, 1)` is the right-facing head
/// and `(1, 1)` is the tail. The Manhattan arc is 15 cells, hence 16 occupied
/// cells after including the head cell.
pub const PROTOTYPE_GEOMETRY_POSE_NAME: &str = "prototype_straight_16";
#[cfg(test)]
pub const PROTOTYPE_GEOMETRY_CANVAS_CELLS: (f64, f64) = (18.0, 3.0);

/// Resolve a pose by name, preview-only bodies included.
///
/// The corpus is what the goldens and the conformance suite iterate; this is
/// what a *renderer caller* asks, and it may have either kind.
pub fn pose_by_name(name: &str) -> Option<&'static Pose> {
    POSES
        .iter()
        .chain(PREVIEW_ONLY_POSES)
        .find(|pose| pose.name == name)
}

/// Arena bodies for the corpse painter, which still consumes untransformed
/// grid positions plus a rotation.
#[cfg(test)]
pub struct ArenaBody {
    pub name: &'static str,
    pub body: &'static [Position],
}

#[cfg(test)]
pub const ARENA_BODIES: &[ArenaBody] = &[
    ArenaBody {
        name: "single_cell",
        body: &[Position { x: 5, y: 5 }],
    },
    ArenaBody {
        name: "straight",
        body: &[Position { x: 12, y: 5 }, Position { x: 6, y: 5 }],
    },
    ArenaBody {
        name: "cornered",
        body: &[
            Position { x: 12, y: 5 },
            Position { x: 6, y: 5 },
            Position { x: 6, y: 11 },
        ],
    },
];

/// Cell sizes spanning the renderer's shrink-to-fit range (the arena walks
/// integer sizes from 15 down to 5).
pub const CELL_SIZES: &[f64] = &[5.0, 10.0, 15.0];

/// Every arena rotation, because the corpse painter transforms internally.
#[cfg(test)]
pub const ROTATIONS: &[i32] = &[0, 90, 180, 270];

/// Animation clock samples. Classic is time-invariant, so pinning these costs
/// nothing; an animated skin's conformance check needs several distinct values.
pub const ANIM_SAMPLES: &[f64] = &[0.0, 250.0, 1_000.0, 4_321.5];

/// The roles a fixture can be painted as, named for the QA route.
pub const ROLE_NAMES: &[&str] = &[
    "own",
    "teammate",
    "enemy",
    "spectated0",
    "spectated1",
    "ffa0",
    "ffa1",
    "ffa2",
    "ffa3",
];

/// Resolve a [`ROLE_NAMES`] entry.
///
/// Teammates use shade slot 1 so a contact sheet shows the within-team shade
/// that distinguishes a 2v2 partner, rather than repeating the own colour.
pub fn identity_by_name(name: &str) -> Option<SkinIdentity> {
    let identity = match name {
        "own" => SkinIdentity {
            role: SnakeRole::Own,
            shade_slot: 0,
        },
        "teammate" => SkinIdentity {
            role: SnakeRole::Teammate,
            shade_slot: 1,
        },
        "enemy" => SkinIdentity {
            role: SnakeRole::Enemy,
            shade_slot: 0,
        },
        "spectated0" => SkinIdentity {
            role: SnakeRole::SpectatedTeam(0),
            shade_slot: 0,
        },
        "spectated1" => SkinIdentity {
            role: SnakeRole::SpectatedTeam(1),
            shade_slot: 0,
        },
        "ffa0" | "ffa1" | "ffa2" | "ffa3" => SkinIdentity {
            role: SnakeRole::FreeForAll {
                palette_slot: name.as_bytes()[3] - b'0',
            },
            shade_slot: 0,
        },
        _ => return None,
    };
    Some(identity)
}

/// The extent of a pose, in cells, so a surface can size a tile that actually
/// contains it. `tile_wrapping_length` is 33 cells wide and would be clipped by
/// any fixed tile sized for the older corpus.
pub fn pose_extent(cells: &[(f64, f64)]) -> (f64, f64) {
    let width = cells.iter().map(|cell| cell.0).fold(0.0, f64::max) + 1.0;
    let height = cells.iter().map(|cell| cell.1).fold(0.0, f64::max) + 1.0;
    (width, height)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The golden trace is written by iterating `POSES` in order, so a
    /// reordering rewrites every block after the moved pose and reads as
    /// "classic changed" in review. Appending is the only safe edit, and this
    /// pins that: the prefix is frozen, the tail is free to grow.
    #[test]
    fn pose_order_is_append_only() {
        const FROZEN_PREFIX: &[&str] = &[
            "single_cell",
            "two_cell",
            "straight_horizontal",
            "straight_vertical",
            "single_corner",
            "zigzag",
            "longer_than_head_gradient",
            "reversed_travel",
            "one_cell_runs",
            "tile_wrapping_length",
            "self_crossing",
        ];

        let names: Vec<&str> = POSES.iter().map(|pose| pose.name).collect();
        assert!(
            names.len() >= FROZEN_PREFIX.len(),
            "poses were removed: {names:?}"
        );
        assert_eq!(
            &names[..FROZEN_PREFIX.len()],
            FROZEN_PREFIX,
            "poses were reordered or renamed. Every existing golden block would \
             move, so the diff would no longer show what actually changed. Add \
             new poses to the end of POSES and extend FROZEN_PREFIX."
        );
    }

    /// The Builder names these in its own source, across a crate boundary and
    /// a wasm one, so nothing type-checks the pair. Deleting one shows up as a
    /// blank canvas in a modal rather than as a build error — which is exactly
    /// how `mid_horizontal` went missing once already.
    #[test]
    fn the_bodies_the_builder_asks_for_all_resolve() {
        for name in [
            // The preview deck's slides.
            "straight_16",
            "straight_18",
            "straight_19",
            "longer_than_head_gradient",
            "starting_length",
            "self_crossing",
            "zigzag",
            "wide_u_turn",
        ] {
            assert!(
                pose_by_name(name).is_some(),
                "the Builder asks for `{name}` and nothing answers"
            );
        }
    }

    #[test]
    fn prototype_geometry_contract_matches_the_real_preview_pose() {
        let contract: serde_json::Value = serde_json::from_str(include_str!(
            "../../../skin-schema/prototype-geometry-v1.json"
        ))
        .expect("prototype geometry contract is valid JSON");
        let source = &contract["renderer_source"];

        assert_eq!(
            source["fixture"].as_str(),
            Some(PROTOTYPE_GEOMETRY_POSE_NAME)
        );
        let pose = pose_by_name(PROTOTYPE_GEOMETRY_POSE_NAME)
            .expect("prototype geometry pose remains resolvable by the real painter");
        let contract_cells: Vec<(f64, f64)> = source["compressed_cells_head_first"]
            .as_array()
            .expect("contract has compressed cells")
            .iter()
            .map(|cell| {
                let pair = cell.as_array().expect("each cell is an x/y pair");
                (
                    pair[0].as_f64().expect("x is numeric"),
                    pair[1].as_f64().expect("y is numeric"),
                )
            })
            .collect();
        assert_eq!(contract_cells, pose.cells);

        let occupied_cells = pose
            .cells
            .windows(2)
            .map(|pair| (pair[0].0 - pair[1].0).abs() + (pair[0].1 - pair[1].1).abs())
            .sum::<f64>()
            + 1.0;
        assert_eq!(source["body_cells"].as_f64(), Some(occupied_cells));
        assert_eq!(source["head_direction"].as_str(), Some("right"));
        assert!(pose.cells[0].0 > pose.cells[1].0, "head must be rightmost");
        assert_eq!(pose.cells[0].1, pose.cells[1].1, "guide must be straight");

        let canvas = source["canvas_cells"]
            .as_array()
            .expect("contract has canvas cell dimensions");
        assert_eq!(
            (canvas[0].as_f64().unwrap(), canvas[1].as_f64().unwrap()),
            PROTOTYPE_GEOMETRY_CANVAS_CELLS
        );
        assert_eq!(pose.cells, &[(16.0, 1.0), (1.0, 1.0)]);

        let ordinary = pose_by_name("straight_16").expect("ordinary preview remains present");
        let translated: Vec<(f64, f64)> =
            pose.cells.iter().map(|(x, y)| (x - 1.0, y + 2.0)).collect();
        assert_eq!(
            translated, ordinary.cells,
            "padding must not change body geometry"
        );

        let live_sizes: Vec<f64> = contract["live_cell_sizes_px"]
            .as_array()
            .expect("contract has live scales")
            .iter()
            .map(|value| value.as_f64().expect("live scale is numeric"))
            .collect();
        assert_eq!(live_sizes, CELL_SIZES);
        assert_eq!(source["native_cell_px"].as_f64(), Some(15.0));
        assert_eq!(
            source["native_cell_px"].as_f64(),
            CELL_SIZES.last().copied()
        );
    }

    /// The shading engine needs a body whose runs are one cell long, and one
    /// long enough for a tile to wrap. Losing either silently would take the
    /// coverage with it.
    #[test]
    fn the_shading_corpus_covers_short_runs_and_long_bodies() {
        let one_cell = POSES
            .iter()
            .find(|pose| pose.name == "one_cell_runs")
            .expect("the one-cell-run pose is part of the corpus");
        let shortest = one_cell
            .cells
            .windows(2)
            .map(|pair| {
                (pair[0].0 - pair[1].0)
                    .abs()
                    .max((pair[0].1 - pair[1].1).abs())
            })
            .fold(f64::MAX, f64::min);
        assert_eq!(shortest, 1.0, "no run in the pose is a single cell long");

        let long = POSES
            .iter()
            .find(|pose| pose.name == "tile_wrapping_length")
            .expect("the tile-wrapping pose is part of the corpus");
        let (width, _) = pose_extent(long.cells);
        assert!(
            width >= 30.0,
            "a {width}-cell body does not wrap a tile enough times to show a seam"
        );
    }

    #[test]
    fn every_named_role_resolves() {
        for name in ROLE_NAMES {
            assert!(identity_by_name(name).is_some(), "{name} did not resolve");
        }
        assert!(identity_by_name("nonesuch").is_none());
    }

    /// A template names the role its picker card should paint, and that name
    /// crosses a crate boundary as a string — skin-schema cannot see this list.
    /// A typo there would silently fall back to whatever the caller does with
    /// an unresolvable role, so the two ends are tied together here.
    #[test]
    fn every_template_names_a_role_that_resolves() {
        for template in skin_schema::v2::templates() {
            assert!(
                identity_by_name(&template.preview_role).is_some(),
                "template `{}` previews as `{}`, which is not a role",
                template.id,
                template.preview_role
            );
        }
    }
}
