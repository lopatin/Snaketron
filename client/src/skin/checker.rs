//! The checkerboard family — Gambit, Harlequin and Pitlane.
//!
//! What forced Rust: a `SkinDoc` has no vocabulary for a pattern. Its whole
//! expressive range is a palette, a head, a contour and a handful of animated
//! scalars (`skin-schema/src/lib.rs`), and `ParamSkin` compiles every document
//! into the same seven-layer classic stack. A checkerboard is not a tuning of
//! that stack; it is two more layers in it. The compositor has had the
//! machinery since the shading engine landed, and until now nothing shipped
//! used it — these three are its first real consumers.
//!
//! **How the board is built.** A snake is a ribbon one cell wide, so a
//! checkerboard on it is two lanes of squares, each lane a repeating band along
//! the body, offset from the other by half a period. That is exactly two
//! [`Source::Tiled`] layers: same period, opposite `t_center`, half a period of
//! `phase_cells` between them. Nothing else in the stack changes, which is why
//! these skins inherit every structural guarantee the shared painter has —
//! the contour, the Boost band, the occlusion contract and the head are all
//! still the shared stack's.
//!
//! **Why the squares are readable.** The carried-food number is anchored two
//! cells behind the head, on the body — so on a patterned snake it sits over
//! two different tones rather than one, and the closed-form contrast rule the
//! document validator applies stops describing the pixels
//! (`specs/skin-shading-prd.md` section 13 says so explicitly, and calls the
//! property review-enforced from there on). Rather than accept that, every
//! colourway here keeps **both** of its tones on the same side of the ink
//! divide: the base fill is light enough that the derived ink is the dark
//! slate, and the check tone is lighter still. The number therefore clears AA
//! on both squares, and `the_number_stays_readable_on_both_squares` checks it
//! at every baked frame rather than leaving it as a claim.
//!
//! **Corners.** A checkerboard is a directional texture, so the default `Own`
//! joint policy — each run painting its own half of the corner cell — shows a
//! visible orientation flip at every turn (`specs/skin-shading-prd.md` section
//! 14, which asks for exactly this to be described rather than glossed). It is
//! kept: `Bisector` is rejected for pattern sources, and on a checkerboard the
//! flip reads as the board turning the corner rather than as a defect.

use crate::skin::classic::document_layers;
use crate::skin::composite::{
    BaseThemeOwned, CelebrationThemeOwned, CompositeConfig, CompositeSkin, Frame, Swatch,
    span_layer,
};
use crate::skin::layer::{Binding, ColorSlot, Region, Source, Span};
use crate::skin::{
    BaseTheme, CelebrationTheme, PaintCtx, SkinColors, SkinIdentity, SkinMetrics, SnakePose,
    SnakeSkin,
};
use skin_schema::color::Rgb;
use std::sync::OnceLock;
use wasm_bindgen::prelude::*;

const OUTLINE_EXTRA: f64 = 2.0;
const BOOST_COLOR: &str = "#fff200";
const BOOST_EXTRA: f64 = 6.0;
const HEAD_CORE_RATIO: f64 = 0.37;
const GRADIENT_CELLS: f64 = 8.0;
/// Lighter than classic's 0.3. The head ramp brightens the body, and on a
/// patterned one it is also washing out the pattern; pulling it back keeps the
/// board legible in the cells nearest the head without losing the which-way-is
/// -it-facing cue the ramp exists to give.
const GRADIENT_MAX_OPACITY: f64 = 0.22;

/// One role's three colours.
///
/// `check` is the second square. It rides the per-role accent slot, which is
/// what lets one layer stack paint a different board for every side without
/// the layers knowing anything about roles.
#[derive(Clone, Copy)]
struct Tones {
    fill: &'static str,
    outline: &'static str,
    check: &'static str,
}

const fn tones(fill: &'static str, outline: &'static str, check: &'static str) -> Tones {
    Tones {
        fill,
        outline,
        check,
    }
}

/// The neutral free-for-all slots, shared by the family: the board is the
/// family's identity, and the two unaffiliated slots are the same steel and
/// gold everywhere so a spectator reads them as "nobody's colour" rather than
/// as a fourth and fifth team.
const STEEL: Tones = tones("#7c8899", "#4e5763", "#d5dbe3");
const GOLD: Tones = tones("#cf9a26", "#946c12", "#f8e9bc");

/// The board's geometry, in cells.
#[derive(Clone, Copy)]
struct Board {
    /// One full square-plus-gap repeat along the body. The squares are half
    /// this long and half a cell across, so a period of 1.0 gives true squares
    /// at any cell size and a larger period gives blocks.
    period_cells: f64,
}

impl Board {
    /// How many tiles one lane emits over a body of `cells` cells. Used by the
    /// cost test, which is the only reason a skin author needs to know.
    #[cfg(test)]
    fn tiles_over(self, cells: f64) -> usize {
        (cells / self.period_cells).ceil() as usize + 1
    }
}

/// How a board moves.
///
/// Every arm varies paint *arguments* only — a colour, a layer opacity, or a
/// per-tile alpha. None of them can change how many rectangles are emitted,
/// which is what keeps op-count invariance a property rather than a promise.
#[derive(Clone, Copy)]
enum Motion {
    /// The pale square breathes, so the board's contrast rises and falls. The
    /// cheapest animation there is: the colour was going to be set anyway, so
    /// this costs no ops at all.
    ///
    /// It only ever darkens, and that is a fact about the colour rather than a
    /// preference. `shift_lightness` moves a channel toward white, so on a tone
    /// that is already near white there is almost nothing left to move: at a
    /// 0.2 amplitude the pale square lightens by two or three levels and darkens
    /// by forty. A symmetric swing would therefore have spent half its cycle
    /// invisible — which is exactly how it looked on the contact sheet.
    Drift { amount: f64 },
    /// The two lanes trade emphasis, so the board appears to tilt back and
    /// forth. Two baked opacity tracks, four ops per snake.
    Tilt { floor: f64 },
    /// A gleam travels head to tail. The only arm that needs a per-tile
    /// expression, and therefore the only one that pays two extra ops per
    /// tile — which is why the skin that uses it has the largest squares.
    Gleam { cells_per_crest: f64, floor: f64 },
}

/// Everything that distinguishes one board from another.
struct Recipe {
    id: &'static str,
    name: &'static str,
    friendly: [Tones; 2],
    enemy: [Tones; 2],
    core: &'static str,
    /// The head ramp's colour. A warm board wants a warm light on it.
    ramp_rgb: (u8, u8, u8),
    board: Board,
    motion: Motion,
    period_ms: f64,
    base: BaseTheme<'static>,
    celebration: CelebrationTheme<'static>,
    engine: OnceLock<CompositeSkin>,
}

/// A handle onto one recipe. Zero-sized in effect — the compiled skin lives in
/// the recipe's `OnceLock` and is built once per process, never per frame.
pub struct CheckerSkin(&'static Recipe);

impl CheckerSkin {
    fn engine(&self) -> &'static CompositeSkin {
        self.0.engine.get_or_init(|| compile(self.0))
    }
}

/// Gambit — a chessboard. The highest-contrast board of the three, and the
/// stillest: the pattern is the idea, so the motion is only a slow breath in
/// the pale squares.
static GAMBIT: Recipe = Recipe {
    id: "gambit@1",
    name: "Gambit",
    friendly: [
        tones("#6ea5e6", "#3f6ea8", "#dbeafd"),
        tones("#4a86d8", "#27568f", "#c4d9f8"),
    ],
    enemy: [
        tones("#f57f78", "#b8514a", "#ffdedb"),
        tones("#e05450", "#9e2f2c", "#f9c8c4"),
    ],
    core: "#111c33",
    ramp_rgb: (255, 255, 255),
    board: Board { period_cells: 1.0 },
    motion: Motion::Drift { amount: 0.20 },
    period_ms: 3_400.0,
    base: BaseTheme {
        friendly_zone: "#e8effb",
        enemy_zone: "#fdecec",
        friendly_wall: "#7b98c4",
        enemy_wall: "#c98d8a",
        friendly_text: "#ccdcf0",
        enemy_text: "#f0d0cf",
    },
    celebration: CelebrationTheme {
        effect: "goal-impact-wave",
        friendly_accent: "#4a86d8",
        enemy_accent: "#e05450",
        readout_friendly: "#24548f",
        readout_enemy: "#9e2f2c",
    },
    engine: OnceLock::new(),
};

/// Harlequin — a jester's board, where the second square is a different *hue*
/// rather than a lighter version of the first. Both hues stay on their own side
/// of the wheel, which is what makes a two-colour board legal at all: violet
/// and cyan are both cool, rose and amber both warm.
static HARLEQUIN: Recipe = Recipe {
    id: "harlequin@1",
    name: "Harlequin",
    friendly: [
        tones("#a288ef", "#6a4fc4", "#72e0ea"),
        tones("#9275e6", "#52399f", "#5fd3e0"),
    ],
    enemy: [
        tones("#ea6f8b", "#ad3e5d", "#fbcd82"),
        tones("#dc5c78", "#97263f", "#f2b75f"),
    ],
    core: "#1b1130",
    ramp_rgb: (255, 250, 255),
    board: Board { period_cells: 1.0 },
    motion: Motion::Tilt { floor: 0.62 },
    period_ms: 2_800.0,
    base: BaseTheme {
        friendly_zone: "#efe9fc",
        enemy_zone: "#fdeaef",
        friendly_wall: "#9a86cc",
        enemy_wall: "#c9899b",
        friendly_text: "#ddd2f2",
        enemy_text: "#f2d3dc",
    },
    celebration: CelebrationTheme {
        effect: "goal-impact-wave",
        friendly_accent: "#9275e6",
        enemy_accent: "#dc5c78",
        readout_friendly: "#52399f",
        readout_enemy: "#97263f",
    },
    engine: OnceLock::new(),
};

/// Pitlane — a racing board. Its squares are twice as long as the other two
/// skins', which is a design choice and a budget one at once: big blocks are
/// what a race flag looks like, and halving the tile count is what pays for the
/// per-tile expression its travelling gleam needs.
static PITLANE: Recipe = Recipe {
    id: "pitlane@1",
    name: "Pitlane",
    friendly: [
        tones("#2fb5af", "#17817d", "#c6f1ee"),
        tones("#219f9d", "#106e6d", "#9adcda"),
    ],
    enemy: [
        tones("#f5834c", "#b25423", "#fdd7bd"),
        tones("#e0642c", "#9d3f13", "#f7bd97"),
    ],
    core: "#0a2224",
    ramp_rgb: (255, 248, 238),
    board: Board { period_cells: 2.0 },
    motion: Motion::Gleam {
        cells_per_crest: 7.0,
        // The trough still has to read as a board rather than as a plain
        // snake, which is what sets this floor rather than taste.
        floor: 0.55,
    },
    period_ms: 2_000.0,
    base: BaseTheme {
        friendly_zone: "#e6f6f5",
        enemy_zone: "#fdefe6",
        friendly_wall: "#74b3b0",
        enemy_wall: "#c8906d",
        friendly_text: "#c9e8e6",
        enemy_text: "#f0d6c3",
    },
    celebration: CelebrationTheme {
        effect: "goal-impact-wave",
        friendly_accent: "#219f9d",
        enemy_accent: "#e0642c",
        readout_friendly: "#106e6d",
        readout_enemy: "#9d3f13",
    },
    engine: OnceLock::new(),
};

/// Every board in the family, in catalogue order.
pub const FAMILY: [CheckerSkin; 3] = [
    CheckerSkin(&GAMBIT),
    CheckerSkin(&HARLEQUIN),
    CheckerSkin(&PITLANE),
];

/// The check tone at one point in the cycle.
///
/// Only [`Motion::Drift`] moves it, and step zero is always the authored
/// colour — which is what makes the resting pose the one the palette was
/// designed against, on the roster, in a golden, and under reduced motion.
fn check_tone(motion: Motion, hex: &str, turns: f64) -> String {
    let Motion::Drift { amount } = motion else {
        return hex.to_string();
    };
    let Some(rgb) = Rgb::parse(hex) else {
        return hex.to_string();
    };
    // A dip and a return: zero at the top of the cycle, deepest halfway
    // through. Starting at zero is what makes step zero the authored colour,
    // which is the pose the roster, the goldens and a reduced-motion viewer
    // all hold.
    let depth = (1.0 - (turns * std::f64::consts::TAU).cos()) / 2.0;
    skin_schema::shift_lightness(rgb, -amount * depth).to_hex()
}

/// The two lanes' opacities at one point in the cycle.
///
/// Both stay well clear of zero on purpose. A lane that faded out entirely
/// would leave a dashed stripe down one side of the snake rather than a board
/// with a quieter half, and that pose would be the one a reduced-motion viewer
/// was left holding.
fn lane_opacities(motion: Motion, turns: f64) -> Vec<f64> {
    let Motion::Tilt { floor } = motion else {
        return Vec::new();
    };
    let mid = (1.0 + floor) / 2.0;
    let swing = (1.0 - floor) / 2.0;
    let phase = (turns * std::f64::consts::TAU).cos();
    vec![mid + swing * phase, mid - swing * phase]
}

/// The travelling-gleam alpha, as an expression over body space and the clock.
///
/// `time` runs 0..1 turns, so subtracting `s / cells_per_crest` sends the crest
/// from head to tail. It stays grammatical for the schema's parser, which is
/// the sandbox boundary every expression goes through
/// (`specs/skin-shading-prd.md` section 9.2).
fn gleam_expr(cells_per_crest: f64, floor: f64) -> String {
    let mid = (1.0 + floor) / 2.0;
    let swing = (1.0 - floor) / 2.0;
    format!("{mid} + {swing} * sin(tau * (time - s / {cells_per_crest}))")
}

fn compile(recipe: &'static Recipe) -> CompositeSkin {
    let swatch = |tones: Tones, turns: f64| Swatch {
        fill: tones.fill.to_string(),
        outline: tones.outline.to_string(),
        label: crate::render::roster_label_ink(tones.fill).to_string(),
        swatch: tones.fill.to_string(),
        accent: check_tone(recipe.motion, tones.check, turns),
    };

    let frames: Vec<Frame> = (0..skin_schema::ANIMATION_STEPS)
        .map(|step| {
            let turns = step as f64 / skin_schema::ANIMATION_STEPS as f64;
            Frame {
                friendly: [
                    swatch(recipe.friendly[0], turns),
                    swatch(recipe.friendly[1], turns),
                ],
                enemy: [
                    swatch(recipe.enemy[0], turns),
                    swatch(recipe.enemy[1], turns),
                ],
                free_for_all: [
                    swatch(recipe.friendly[0], turns),
                    swatch(recipe.enemy[0], turns),
                    swatch(STEEL, turns),
                    swatch(GOLD, turns),
                ],
                ramp_opacity: GRADIENT_MAX_OPACITY,
                wave_phase_turns: 0.0,
                time_turns: turns,
                params: lane_opacities(recipe.motion, turns),
                literals: Vec::new(),
            }
        })
        .collect();

    let mut layers = document_layers(
        BOOST_EXTRA,
        OUTLINE_EXTRA,
        recipe.ramp_rgb,
        GRADIENT_CELLS,
        HEAD_CORE_RATIO,
    );

    // The board goes directly on top of the body and directly under the head
    // ramp. Under the ramp so the cells nearest the head are lightened along
    // with everything else — a board that stayed at full contrast through the
    // ramp would fight the direction cue the ramp exists to give.
    let insert_at = layers
        .iter()
        .position(|layer| layer.id == "body")
        .map(|body| body + 1)
        .unwrap_or(layers.len());

    let alpha = match recipe.motion {
        Motion::Gleam {
            cells_per_crest,
            floor,
        } => {
            let source = gleam_expr(cells_per_crest, floor);
            Some(std::sync::Arc::new(
                skin_schema::expr::Expr::parse(&source)
                    .expect("the gleam expression is built from literals and is grammatical"),
            ))
        }
        _ => None,
    };

    // Two lanes, half a cell wide each, meeting on the centreline; the far one
    // shifted half a period along the body. That offset is the checkerboard.
    let lanes = [("check-near", -0.25, 0.0), ("check-far", 0.25, 0.5)];
    for (index, (id, t_center, phase)) in lanes.into_iter().enumerate() {
        let mut layer = span_layer(
            id,
            Region::Body,
            Span::WHOLE,
            Source::Tiled {
                color: ColorSlot::Accent,
                period_cells: recipe.board.period_cells,
                duty: 0.5,
                half_width: Binding::Const(0.25),
                t_center: Binding::Const(t_center),
                phase_cells: phase * recipe.board.period_cells,
                alpha: alpha.clone(),
            },
        );
        // A one-cell snake is a disc with a head core on it. Half a cell of
        // board on a body that small reads as a smudge, and the head cap and
        // core cover most of it anyway.
        layer.omit_on_single_cell = true;
        layer.opacity = match recipe.motion {
            Motion::Tilt { .. } => Binding::Param(index),
            _ => Binding::ONE,
        };
        layers.insert(insert_at + index, layer);
    }

    CompositeSkin::new(
        recipe.id,
        recipe.name,
        layers,
        frames,
        recipe.period_ms,
        CompositeConfig {
            boost_color: BOOST_COLOR.to_string(),
            head_core_color: recipe.core.to_string(),
            head_core_ratio: HEAD_CORE_RATIO,
            head_core_is_dark: true,
            wave: None,
        },
        Some(BaseThemeOwned {
            friendly_zone: recipe.base.friendly_zone.to_string(),
            enemy_zone: recipe.base.enemy_zone.to_string(),
            friendly_wall: recipe.base.friendly_wall.to_string(),
            enemy_wall: recipe.base.enemy_wall.to_string(),
            friendly_text: recipe.base.friendly_text.to_string(),
            enemy_text: recipe.base.enemy_text.to_string(),
        }),
        Some(CelebrationThemeOwned {
            effect: recipe.celebration.effect.to_string(),
            friendly_accent: recipe.celebration.friendly_accent.to_string(),
            enemy_accent: recipe.celebration.enemy_accent.to_string(),
            readout_friendly: recipe.celebration.readout_friendly.to_string(),
            readout_enemy: recipe.celebration.readout_enemy.to_string(),
        }),
    )
    .unwrap_or_else(|problems| panic!("the {} layer stack is invalid: {problems:?}", recipe.id))
}

impl SnakeSkin for CheckerSkin {
    fn id(&self) -> &str {
        self.engine().id()
    }

    fn name(&self) -> &str {
        self.engine().name()
    }

    fn colors(&self, identity: &SkinIdentity) -> SkinColors<'_> {
        self.engine().colors(identity)
    }

    fn metrics(&self, boost_active: bool) -> SkinMetrics {
        self.engine().metrics(boost_active)
    }

    fn paint_alive(
        &self,
        ctx: &mut PaintCtx,
        pose: &SnakePose,
        identity: &SkinIdentity,
    ) -> Result<(), JsValue> {
        self.engine().paint_alive(ctx, pose, identity)
    }

    fn base_theme(&self) -> Option<BaseTheme<'_>> {
        self.engine().base_theme()
    }

    fn celebration_theme(&self) -> Option<CelebrationTheme<'_>> {
        self.engine().celebration_theme()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skin::layer::{Layer, LayerKind};
    use crate::skin::paint::{OpRecorder, PaintCtx};
    use crate::skin::{SnakeRole, fixtures};
    use skin_schema::color::{
        ENEMY_HUES, FRIENDLY_HUES, HueWindow, NEUTRAL_CHROMA, contrast_ratio, perceptual_distance,
    };

    const RECIPES: [&Recipe; 3] = [&GAMBIT, &HARLEQUIN, &PITLANE];

    /// Every role's tones, read from the recipe rather than from a copy — so
    /// these tests check the palette that ships, exactly as ember's hue test
    /// does.
    fn every_role(recipe: &'static Recipe) -> Vec<Tones> {
        let mut all = recipe.friendly.to_vec();
        all.extend_from_slice(&recipe.enemy);
        all.push(STEEL);
        all.push(GOLD);
        all
    }

    /// The steps a viewer can actually land on.
    fn every_step() -> impl Iterator<Item = f64> {
        (0..skin_schema::ANIMATION_STEPS)
            .map(|step| step as f64 / skin_schema::ANIMATION_STEPS as f64)
    }

    /// The property `specs/skin-shading-prd.md` section 13 gives up on for a
    /// textured body, recovered by construction.
    ///
    /// The carried-food number and the roster name sit on the body two cells
    /// behind the head, where a checkerboard puts two different tones under
    /// them. Keeping both tones on the same side of the ink divide is what
    /// makes one ink legible on both — and the check has to hold at every
    /// baked frame, because a drifting check tone is a moving target.
    #[test]
    fn the_number_stays_readable_on_both_squares() {
        for recipe in RECIPES {
            for tones in every_role(recipe) {
                let ink = Rgb::parse(crate::render::roster_label_ink(tones.fill))
                    .expect("the derived ink is a flat hex");
                for turns in every_step() {
                    let check = check_tone(recipe.motion, tones.check, turns);
                    for (what, hex) in [("fill", tones.fill), ("check", check.as_str())] {
                        let tone = Rgb::parse(hex).expect("a flat hex tone");
                        let ratio = contrast_ratio(ink, tone);
                        assert!(
                            ratio >= skin_schema::MIN_LABEL_CONTRAST,
                            "{}: the number reaches only {ratio:.2}:1 on the \
                             {what} `{hex}` at {turns:.3} turns. It sits on \
                             both squares, so both have to clear {}:1",
                            recipe.id,
                            skin_schema::MIN_LABEL_CONTRAST
                        );
                    }
                }
            }
        }
    }

    /// A board has to look like a board. Two tones a viewer cannot tell apart
    /// is a plain snake that pays for a pattern it does not have.
    /// Composite `source-over`, which is what an alpha on a lane actually does
    /// to the colour a viewer sees. Canvas blends in sRGB, so this does too.
    fn over(check: Rgb, fill: Rgb, alpha: f64) -> Rgb {
        Rgb {
            r: fill.r + alpha * (check.r - fill.r),
            g: fill.g + alpha * (check.g - fill.g),
            b: fill.b + alpha * (check.b - fill.b),
        }
    }

    /// The colour a viewer actually sees in the pale squares, at one point in
    /// the cycle and at whichever lane opacity the motion is holding.
    ///
    /// Both mechanisms move the check *toward* the fill, which is why this has
    /// to be what the distinctness rule reads. Checking the authored colour
    /// would clear a board that spends half its cycle mush.
    fn visible_checks(recipe: &'static Recipe, tones: Tones, turns: f64) -> Vec<Rgb> {
        let fill = Rgb::parse(tones.fill).expect("a flat hex fill");
        let check =
            Rgb::parse(&check_tone(recipe.motion, tones.check, turns)).expect("a flat hex check");
        match recipe.motion {
            Motion::Tilt { .. } => lane_opacities(recipe.motion, turns)
                .into_iter()
                .map(|alpha| over(check, fill, alpha))
                .collect(),
            // The gleam's alpha is per tile and sweeps its whole range along
            // the body, so the floor is the worst any tile can be.
            Motion::Gleam { floor, .. } => vec![over(check, fill, floor), check],
            Motion::Drift { .. } => vec![check],
        }
    }

    /// A board has to look like a board — at every step, in every lane, and in
    /// the trough of a gleam, not just in the pose the palette was picked in.
    ///
    /// Measured as a perceptual distance rather than a contrast ratio, because
    /// contrast ratio only sees lightness: it would fail Harlequin, whose two
    /// squares are deliberately a violet and a cyan of similar lightness, while
    /// passing a board that had gone genuinely flat.
    #[test]
    fn the_two_squares_stay_distinguishable_through_the_whole_cycle() {
        /// Comfortably above the ~0.02 threshold of visibility, and below the
        /// ~0.19 the quietest board sits at when resting.
        const MIN_DISTANCE: f64 = 0.10;

        for recipe in RECIPES {
            for tones in every_role(recipe) {
                let fill = Rgb::parse(tones.fill).expect("a flat hex fill");
                for turns in every_step() {
                    for check in visible_checks(recipe, tones, turns) {
                        let distance = perceptual_distance(fill, check);
                        assert!(
                            distance >= MIN_DISTANCE,
                            "{}: `{}` and `{}` are only {distance:.3} apart at \
                             {turns:.3} turns; the board has gone flat",
                            recipe.id,
                            tones.fill,
                            check.to_hex(),
                        );
                        assert!(
                            check.relative_luminance() > fill.relative_luminance(),
                            "{}: the pale square `{}` has gone darker than the \
                             fill `{}` at {turns:.3} turns, inverting the board",
                            recipe.id,
                            check.to_hex(),
                            tones.fill,
                        );
                    }
                }
            }
        }
    }

    /// The board also has to *move*. Every motion arm is a claim that something
    /// changes over the cycle, and a floor set too close to one leaves a skin
    /// that passes every other check while being still.
    #[test]
    fn every_board_visibly_changes_across_its_cycle() {
        for recipe in RECIPES {
            let tones = recipe.friendly[0];
            let fill = Rgb::parse(tones.fill).expect("a flat hex fill");
            let extremes: Vec<f64> = every_step()
                .flat_map(|turns| {
                    visible_checks(recipe, tones, turns)
                        .into_iter()
                        .map(|check| perceptual_distance(fill, check))
                        .collect::<Vec<_>>()
                })
                .collect();
            let quietest = extremes.iter().cloned().fold(f64::MAX, f64::min);
            let loudest = extremes.iter().cloned().fold(0.0, f64::max);
            assert!(
                loudest - quietest >= 0.04,
                "{}: the board's contrast only travels {:.3} over its cycle, \
                 which is not a motion anyone will see",
                recipe.id,
                loudest - quietest
            );
        }
    }

    /// The rule that outranks art direction. A board is a strong visual, and
    /// half of it is the accent — so the accent is held to the same window as
    /// the fill even though nothing outside this test would check it.
    #[test]
    fn every_tone_including_the_check_stays_on_its_own_side() {
        fn assert_window(recipe: &Recipe, what: &str, hex: &str, window: HueWindow, side: &str) {
            let color = Rgb::parse(hex).expect("a flat hex colour");
            let (hue, chroma) = color.oklch_hue_chroma();
            if chroma <= NEUTRAL_CHROMA {
                return;
            }
            assert!(
                window.contains(hue),
                "{}: {what} `{hex}` reads at {hue:.0}deg, outside the {side} \
                 window ({:.0}..{:.0})",
                recipe.id,
                window.from,
                window.to
            );
        }

        for recipe in RECIPES {
            for (index, tones) in recipe.friendly.iter().enumerate() {
                for (what, hex) in [
                    ("fill", tones.fill),
                    ("outline", tones.outline),
                    ("check", tones.check),
                ] {
                    let label = format!("friendly[{index}].{what}");
                    assert_window(recipe, &label, hex, FRIENDLY_HUES, "friendly");
                }
            }
            for (index, tones) in recipe.enemy.iter().enumerate() {
                for (what, hex) in [
                    ("fill", tones.fill),
                    ("outline", tones.outline),
                    ("check", tones.check),
                ] {
                    let label = format!("enemy[{index}].{what}");
                    assert_window(recipe, &label, hex, ENEMY_HUES, "enemy");
                }
            }

            // World dressing carries the same obligation: your side of the
            // arena has to read as your side.
            assert_window(
                recipe,
                "base.friendly_wall",
                recipe.base.friendly_wall,
                FRIENDLY_HUES,
                "friendly",
            );
            assert_window(
                recipe,
                "base.enemy_wall",
                recipe.base.enemy_wall,
                ENEMY_HUES,
                "enemy",
            );
            assert_window(
                recipe,
                "base.friendly_zone",
                recipe.base.friendly_zone,
                FRIENDLY_HUES,
                "friendly",
            );
            assert_window(
                recipe,
                "base.enemy_zone",
                recipe.base.enemy_zone,
                ENEMY_HUES,
                "enemy",
            );
            assert_window(
                recipe,
                "celebration.friendly_accent",
                recipe.celebration.friendly_accent,
                FRIENDLY_HUES,
                "friendly",
            );
            assert_window(
                recipe,
                "celebration.enemy_accent",
                recipe.celebration.enemy_accent,
                ENEMY_HUES,
                "enemy",
            );
            assert!(
                skin_schema::KNOWN_EFFECTS.contains(&recipe.celebration.effect),
                "{}: names an effect the client cannot draw",
                recipe.id
            );
        }
    }

    /// The head core has to stay findable on every board, and the roster's
    /// white ready-check has to stay legible on the core.
    #[test]
    fn the_head_stays_findable_and_the_ready_check_readable() {
        let white = Rgb::parse("#ffffff").expect("literal");
        for recipe in RECIPES {
            let core = Rgb::parse(recipe.core).expect("a flat hex core");
            assert!(
                contrast_ratio(core, white) >= skin_schema::MIN_READY_CHECK_CONTRAST,
                "{}: the ready-check is illegible on this core",
                recipe.id
            );
            for tones in every_role(recipe) {
                for hex in [tones.fill, tones.check] {
                    let body = Rgb::parse(hex).expect("a flat hex tone");
                    let ratio = contrast_ratio(core, body);
                    assert!(
                        ratio >= skin_schema::MIN_HEAD_CORE_CONTRAST,
                        "{}: the head core reaches only {ratio:.2}:1 on `{hex}`",
                        recipe.id
                    );
                }
            }
            assert!(CheckerSkin(recipe).metrics(false).head_core_is_dark);
        }
    }

    /// Two lanes, offset by half a period — that offset is the whole pattern,
    /// so a regression that lost it would leave a skin that still passed every
    /// generic check while painting stripes.
    #[test]
    fn the_board_is_two_offset_lanes_and_nothing_else() {
        for recipe in RECIPES {
            let engine = CheckerSkin(recipe).engine();
            let lanes: Vec<&Layer> = engine
                .layers()
                .iter()
                .filter(|layer| layer.id.starts_with("check-"))
                .collect();
            assert_eq!(lanes.len(), 2, "{}: expected exactly two lanes", recipe.id);

            let read = |layer: &Layer| match &layer.kind {
                LayerKind::Span {
                    source:
                        Source::Tiled {
                            period_cells,
                            duty,
                            half_width,
                            t_center,
                            phase_cells,
                            ..
                        },
                    ..
                } => (
                    *period_cells,
                    *duty,
                    half_width.clone(),
                    t_center.clone(),
                    *phase_cells,
                ),
                other => panic!("{}: a lane is not a tiled span: {other:?}", recipe.id),
            };
            let near = read(lanes[0]);
            let far = read(lanes[1]);

            assert_eq!(near.0, far.0, "{}: lanes disagree on period", recipe.id);
            assert_eq!((near.1, far.1), (0.5, 0.5), "squares are half the period");
            assert_eq!(
                (near.2, far.2),
                (Binding::Const(0.25), Binding::Const(0.25)),
                "lanes are half a cell wide"
            );
            assert_eq!(
                (near.3, far.3),
                (Binding::Const(-0.25), Binding::Const(0.25)),
                "{}: the lanes are not either side of the centreline",
                recipe.id
            );
            assert!(
                (far.4 - near.4 - near.0 / 2.0).abs() < 1e-9,
                "{}: the lanes are {} apart, not half of the {} period — that \
                 is stripes, not a checkerboard",
                recipe.id,
                far.4 - near.4,
                near.0
            );

            // Both lanes are body layers, so neither adds overhang: the
            // contour is still the only thing painting past the silhouette.
            assert_eq!(
                CheckerSkin(recipe).metrics(false).overhang_px,
                OUTLINE_EXTRA / 2.0
            );
            assert_eq!(
                CheckerSkin(recipe).metrics(true).overhang_px,
                BOOST_EXTRA / 2.0
            );
        }
    }

    /// Reduced motion holds every board still, including the one whose gleam is
    /// a per-tile expression — the arm most likely to leak the clock.
    #[test]
    fn reduced_motion_holds_the_board_still() {
        for recipe in RECIPES {
            let skin = CheckerSkin(recipe);
            let golden = |anim_ms: f64, reduced_motion: bool| {
                let mut recorder = OpRecorder::new();
                skin.paint_alive(
                    &mut PaintCtx::recording(&mut recorder),
                    &SnakePose {
                        cells: fixtures::POSES
                            .iter()
                            .find(|pose| pose.name == "longer_than_head_gradient")
                            .expect("the corpus has a long body")
                            .cells,
                        cell_size: 12.0,
                        detail_scale: 1.0,
                        boost_active: false,
                        seed: 0.0,
                        anim_ms,
                        reduced_motion,
                    },
                    &SkinIdentity {
                        role: SnakeRole::Own,
                        shade_slot: 0,
                    },
                )
                .expect("a recording painter cannot fail");
                recorder.to_golden()
            };

            let resting = golden(0.0, true);
            for anim_ms in [0.0, 700.0, 5_000.0, f64::NAN] {
                assert_eq!(
                    resting,
                    golden(anim_ms, true),
                    "{} moved under reduced motion at {anim_ms}ms",
                    recipe.id
                );
            }
            assert_ne!(
                resting,
                golden(recipe.period_ms * 0.3, false),
                "{} never actually animates, so its motion is decoration in a \
                 comment",
                recipe.id
            );
        }
    }

    /// The op budget, checked where a board is most expensive: the longest
    /// body in the corpus. `skin::perf` enforces the ceiling across the whole
    /// catalogue; this one exists so a change to a board's period fails here,
    /// next to the constant that caused it, with the arithmetic spelled out.
    #[test]
    fn a_board_costs_what_its_period_says_it_costs() {
        for recipe in RECIPES {
            let skin = CheckerSkin(recipe);
            let mut recorder = OpRecorder::new();
            skin.paint_alive(
                &mut PaintCtx::recording(&mut recorder),
                &SnakePose::still(crate::skin::perf::PERF_BODY, 15.0, true),
                &SkinIdentity {
                    role: SnakeRole::Own,
                    shade_slot: 0,
                },
            )
            .expect("a recording painter cannot fail");

            let ops = recorder.shapes().len();
            let per_tile = match recipe.motion {
                // One fill_rect, plus the alpha set and cleared around it.
                Motion::Gleam { .. } => 3,
                _ => 1,
            };
            let budget = 2 * recipe.board.tiles_over(21.0) * per_tile + 100;
            assert!(
                ops <= budget,
                "{} emits {ops} ops on a 21-cell body, over the {budget} its \
                 {}-cell period and motion account for",
                recipe.id,
                recipe.board.period_cells
            );
            assert!(
                ops < 200,
                "{} emits {ops} ops, over the per-snake ceiling skin::perf \
                 enforces",
                recipe.id
            );
        }
    }

    /// A board on a one-cell snake is a smudge under the head core, so the
    /// lanes sit out — and saying so on the layer is what keeps it a rule
    /// rather than an accident of how short bodies happen to allocate.
    #[test]
    fn a_single_cell_snake_wears_no_board() {
        for recipe in RECIPES {
            for layer in CheckerSkin(recipe)
                .engine()
                .layers()
                .iter()
                .filter(|layer| layer.id.starts_with("check-"))
            {
                assert!(
                    !layer.applies(false, 1),
                    "{}: a lane paints on a single-cell body",
                    recipe.id
                );
                assert!(layer.applies(false, 2));
            }
        }
    }
}
