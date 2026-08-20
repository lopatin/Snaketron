//! The sprite-sheet family — skins whose art is a square sheet that *plays*.
//!
//! **`y` is time, `x` is the body.** One row of the sheet is a whole snake's
//! worth of skin, laid out from the head; playing the rows in order animates
//! the skin in place. Twenty rows by default, and a square sheet at twenty rows
//! is also twenty cells long, so a sheet carries its own scale and nothing has
//! to be written down twice.
//!
//! This is a different shape of art from [`crate::skin::animal`]'s coats, not a
//! better one. A coat is a still texture that clothes any length of snake; a
//! sheet is a strip of *frames*, and the cost of that is a sheet twenty times
//! taller for the same length of pattern. Reach for a coat unless the skin
//! genuinely moves.
//!
//! **Animation is free, structurally.** The row is an argument to the same
//! single `drawImage` the still case emits — `AtlasRegion::source_rect` walks
//! the source rectangle down the sheet — so an animated skin costs exactly what
//! a still one does and op-count invariance holds without a special case. That
//! is the whole reason frames live in the atlas rather than in the layer stack.
//!
//! **Sheets are worn three ways**, and all three are the same layer with
//! different numbers:
//!
//! - *Repeating*, pinned to the whole body: a living coat. Any length of snake
//!   is covered, exactly as a texture is.
//! - *Once, from the head*: a sprite. The body beneath shows past its end.
//! - *Once, from the tail*: the same, from the other end.
//!
//! **A sprite that ends needs a fade.** A bitmap's own edge is a hard vertical
//! line across the body, and it reads as the art being cut off rather than as
//! the art ending — which is what [`Fade`] exists for, and why the flag
//! declares one. A twenty-cell sprite on a forty-cell snake is the ordinary
//! case, not an edge case.
//!
//! **Transparency works and is load-bearing.** These blits are ordinary
//! `drawImage` calls, so a sheet's alpha composites over whatever is beneath
//! it — which is what makes sprites *stack*. Nothing here forces opacity the
//! way a coat does, so a translucent sheet is a legitimate thing to author, and
//! a second sheet layer over the first is just another entry in the stack.
//!
//! **The art comes from `client/design/tools/sprite_sheet.py`.** It is the only
//! thing that should ever write these PNGs. Sourced images are seamless-ish;
//! that script rolls both wrap joins to the centre where they can be measured,
//! repairs them, and rolls back. A sheet has to wrap in `y` as well as `x`,
//! because row `n-1` is followed by row `0` and a discontinuity there is a jolt
//! once per cycle rather than a static seam.

use crate::skin::atlas::{Atlas, AtlasRegion, DEFAULT_SPRITE_ROWS};
use crate::skin::classic::document_layers;
use crate::skin::composite::{
    BaseThemeOwned, CelebrationThemeOwned, CompositeConfig, CompositeSkin, Frame, Swatch,
    span_layer,
};
use crate::skin::layer::{Anchor, Fade, Fit, LayerKind, Region, Source, Span};
use crate::skin::{
    BaseTheme, CelebrationTheme, PaintCtx, SideCue, SkinColors, SkinIdentity, SkinMetrics,
    SnakePose, SnakeSkin,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::OnceLock;
use wasm_bindgen::prelude::*;

/// Matches `skin::animal`: the contour carries the friend/foe reading for every
/// skin whose body is a picture rather than a palette, so it is widened well
/// past a painted skin's and the two families teach the same cue.
const OUTLINE_EXTRA: f64 = 5.0;
const BOOST_COLOR: &str = "#fff200";
const BOOST_EXTRA: f64 = 9.0;
const HEAD_CORE_RATIO: f64 = 0.37;
const GRADIENT_CELLS: f64 = 7.0;
const GRADIENT_MAX_OPACITY: f64 = 0.22;

/// Milliseconds one row is held for.
///
/// Twenty rows at this rate is a shade under two seconds a cycle, which is a
/// coat rippling or a flag turning over rather than either flickering or
/// visibly stepping. It is per-row rather than per-cycle so a sheet with more
/// rows plays *longer*, not faster — the alternative silently speeds up any
/// sheet authored at finer resolution.
const MS_PER_ROW: f64 = 90.0;

/// The live-tunable properties of one sheet, and where they come from.
///
/// **`client/design/sprites/tuning.json` is the source of truth**, not the
/// literals in the recipes below — those are only the fallback for an id the
/// file does not mention. The sidebar on `/qa/skins` edits these, previews them
/// through [`tune`], and writes them back to that file, so what you saw in the
/// browser is what the next build compiles.
///
/// Only properties that can change *without rebuilding the art* are here.
/// Rotation and repeat length are baked into the PNG by `sprite_sheet.py`; a
/// slider for them would be a slider that quietly does nothing.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Tuning {
    pub anim_speed: f64,
    pub drift_cells: f64,
}

const TUNING_FILE: &str = include_str!("../../design/sprites/tuning.json");

fn tuning_table() -> &'static HashMap<String, Tuning> {
    static TABLE: OnceLock<HashMap<String, Tuning>> = OnceLock::new();
    TABLE.get_or_init(|| {
        let mut table = HashMap::new();
        // A malformed file must not take the skins down with it: every value
        // has a compiled fallback, so a bad edit costs the tuning and nothing
        // else. This runs once per process, at first paint.
        let Ok(parsed) = serde_json::from_str::<serde_json::Value>(TUNING_FILE) else {
            return table;
        };
        let Some(object) = parsed.as_object() else {
            return table;
        };
        for (id, entry) in object {
            let (Some(speed), Some(drift)) = (
                entry.get("anim_speed").and_then(serde_json::Value::as_f64),
                entry.get("drift_cells").and_then(serde_json::Value::as_f64),
            ) else {
                continue;
            };
            table.insert(
                id.clone(),
                Tuning {
                    anim_speed: speed,
                    drift_cells: drift,
                },
            );
        }
        table
    })
}

impl Tuning {
    /// What this recipe ships with, after the file has had its say.
    fn of(recipe: &Recipe) -> Self {
        let fallback = Self {
            anim_speed: recipe.anim_speed,
            drift_cells: recipe.wear.drift_cells,
        };
        tuning_table().get(recipe.id).copied().unwrap_or(fallback)
    }

    /// Clamped to what the engine can actually paint.
    ///
    /// These are **guards, not preferences**: a zero or negative animation
    /// speed divides the period to nothing or runs the sheet backwards through
    /// `frame_index`, and a non-finite drift puts a `NaN` in a source
    /// rectangle. The bounds are deliberately far outside anything anyone would
    /// choose, because the QA sidebar's number boxes are free-form on purpose —
    /// a value clamped here without the author noticing would be a slider that
    /// lies, which is exactly what those boxes exist to avoid.
    fn sane(self) -> Self {
        Self {
            anim_speed: if self.anim_speed.is_finite() {
                self.anim_speed.clamp(0.01, 100.0)
            } else {
                1.0
            },
            drift_cells: if self.drift_cells.is_finite() {
                self.drift_cells.clamp(-1024.0, 1024.0)
            } else {
                0.0
            },
        }
    }
}

thread_local! {
    /// Skins rebuilt by [`tune`], which win over the compiled ones.
    ///
    /// The rebuilt skin is **leaked**, because everything downstream —
    /// `id()`, `colors()`, `paint_alive` — hands out `&'static` borrows of it,
    /// and a live edit is a development affordance on one QA page rather than
    /// anything a player reaches. A few kilobytes per drag of a slider.
    static LIVE: RefCell<HashMap<&'static str, &'static CompositeSkin>> =
        RefCell::new(HashMap::new());
}

/// Rebuild one sheet skin with new tuning, for the QA sidebar's live preview.
///
/// Returns whether the id named a sheet skin. Persisting is a separate step:
/// the sidebar posts the same values to the dev server, which writes
/// `tuning.json` — so a preview that is never saved dies with the page.
pub fn tune(id: &str, tuning: Tuning) -> bool {
    let Some(recipe) = FAMILY.iter().map(|skin| skin.0).find(|r| r.id == id) else {
        return false;
    };
    let built: &'static CompositeSkin = Box::leak(Box::new(compile_with(recipe, tuning.sane())));
    LIVE.with(|live| live.borrow_mut().insert(recipe.id, built));
    true
}

/// Read the tuning of every sheet skin, as JSON, for the QA sidebar.
#[wasm_bindgen(js_name = readSkinTuning)]
pub fn read_skin_tuning() -> String {
    let entries: Vec<String> = FAMILY
        .iter()
        .map(|skin| {
            let recipe = skin.0;
            let tuning = Tuning::of(recipe).sane();
            format!(
                "{{\"id\":{},\"name\":{},\"anim_speed\":{},\"drift_cells\":{},\
                 \"rotation_degrees\":{},\"repeat_cells\":{},\"repeats\":{}}}",
                serde_json::to_string(recipe.id).unwrap_or_default(),
                serde_json::to_string(recipe.name).unwrap_or_default(),
                tuning.anim_speed,
                tuning.drift_cells,
                recipe.rotation_degrees,
                recipe.sheet.cells(),
                recipe.wear.repeat,
            )
        })
        .collect();
    format!("[{}]", entries.join(","))
}

/// Apply tuning to one sheet skin for the rest of this page's life.
#[wasm_bindgen(js_name = setSkinTuning)]
pub fn set_skin_tuning(id: &str, anim_speed: f64, drift_cells: f64) -> bool {
    tune(
        id,
        Tuning {
            anim_speed,
            drift_cells,
        },
    )
}

/// A sprite sheet, and its shape.
#[derive(Clone, Copy)]
struct Sheet {
    /// Versioned and relative, resolved against the page's `<base href>`.
    url: &'static str,
    /// The PNG's own dimensions, declared rather than discovered because the
    /// source rectangle is computed before anything has decoded. Checked
    /// against the committed file by `the_declared_sheet_matches_the_png`.
    width: f64,
    height: f64,
    rows: usize,
    /// Texels per cell of body the art was drawn for. Must match `CELL` in
    /// `client/design/tools/sprite_sheet.py`, which is what wrote the file.
    texels_per_cell: f64,
}

impl Sheet {
    /// Cells of body one row covers.
    ///
    /// A row is one cell *tall*, so its length in cells is simply its aspect —
    /// which for a square sheet is exactly the row count. Derived rather than
    /// declared so the two can never disagree.
    fn cells(&self) -> f64 {
        self.width / (self.height / self.rows as f64)
    }

    /// Cells of body one row covers **at authored scale**.
    ///
    /// The same as `cells()` for a sheet whose rows are one cell tall, which is
    /// every coat. They part company for a picture: a flag's row is 1.4 cells
    /// tall, so its aspect says 14.5 cells while it was drawn to span 20.
    fn cells_long(&self) -> f64 {
        self.width / self.texels_per_cell
    }

    /// How tall a row is, in cells, at authored scale. Above 1 means the body
    /// clips it — see [`Fit::Cutout`].
    fn cells_tall(&self) -> f64 {
        (self.height / self.rows as f64) / self.texels_per_cell
    }
}

/// How a sheet is worn. All three shapes are one layer with different numbers.
#[derive(Clone, Copy)]
struct Wear {
    from: Anchor,
    /// Cells the sprite covers. `None` means "whatever the sheet holds" — the
    /// whole body when repeating, and the sheet's own length when not. Written
    /// down nowhere, so a rebuild at a different row length cannot leave the
    /// span and the art disagreeing about how long the picture is.
    cells: Option<f64>,
    /// Whether the sheet repeats along the body once it runs out.
    repeat: bool,
    /// The ramp that stops a non-repeating sprite ending in a hard edge.
    fade: Option<Fade>,
    /// Cells the pattern slides along the body per animation cycle, away from
    /// the anchor. Only meaningful for a repeating sheet.
    drift_cells: f64,
}

struct Recipe {
    id: &'static str,
    name: &'static str,
    /// The sheet's representative colour, reported to every surface that cannot
    /// draw a sprite — the results-table pill, a CSS swatch, contrast maths.
    tone: &'static str,
    label: &'static str,
    friendly_outline: [&'static str; 2],
    enemy_outline: [&'static str; 2],
    free_outline: [&'static str; 2],
    core: &'static str,
    ramp_rgb: (u8, u8, u8),
    /// Multiplier on how fast the sheet plays its rows. 1.0 is one row every
    /// [`MS_PER_ROW`]; 0.5 is half that, so the cycle takes twice as long.
    anim_speed: f64,
    /// How far the art was turned when the sheet was built, in degrees.
    ///
    /// Recorded rather than applied: rotation is a property of the **pixels**
    /// and is baked in by `client/design/tools/sprite_sheet.py --rotate`. It
    /// cannot be a paint-time transform, because a row is sampled as a
    /// one-cell-tall strip along the body — rotating that quad would turn the
    /// snake's silhouette, not the pattern inside it. Kept here so the shipped
    /// PNG can always be traced back to the command that made it.
    ///
    /// Read only by `a_rotated_sheet_records_the_turn_that_produced_it`, and
    /// that is the point: paint time must never consult it, or the turn would
    /// be applied twice — once in the pixels and once in the transform.
    rotation_degrees: f64,
    sheet: Sheet,
    wear: Wear,
    base: BaseTheme<'static>,
    celebration: CelebrationTheme<'static>,
    engine: OnceLock<CompositeSkin>,
}

/// A handle onto one recipe. The compiled skin lives in the recipe's
/// `OnceLock` and is built once per process, never per frame.
pub struct SpriteSkin(&'static Recipe);

impl SpriteSkin {
    fn engine(&self) -> &'static CompositeSkin {
        if let Some(live) = LIVE.with(|live| live.borrow().get(self.0.id).copied()) {
            return live;
        }
        self.0.engine.get_or_init(|| compile(self.0))
    }
}

/// Shared with `skin::animal` on purpose: a player should learn the rim cue
/// once, and it should mean the same thing on every skin whose body cannot
/// carry it.
const FRIENDLY_RIMS: [&str; 2] = ["#1b6fd0", "#0d4f9e"];
const ENEMY_RIMS: [&str; 2] = ["#d92f3a", "#a5121c"];
const FREE_RIMS: [&str; 2] = ["#4a5766", "#b07d12"];

const SAVANNA_BASE: BaseTheme<'static> = BaseTheme {
    friendly_zone: "#e8f1fb",
    enemy_zone: "#fdece9",
    friendly_wall: "#7d9fc4",
    enemy_wall: "#c78b84",
    friendly_text: "#cbdcef",
    enemy_text: "#f2d3cd",
};

const SAVANNA_CELEBRATION: CelebrationTheme<'static> = CelebrationTheme {
    effect: "goal-impact-wave",
    friendly_accent: "#4b86d0",
    enemy_accent: "#d9605c",
    readout_friendly: "#1b558f",
    readout_enemy: "#9c2a26",
};

const PARADE_BASE: BaseTheme<'static> = BaseTheme {
    friendly_zone: "#eef2fb",
    enemy_zone: "#fdeeee",
    friendly_wall: "#8092c0",
    enemy_wall: "#c58a8a",
    friendly_text: "#d2daee",
    enemy_text: "#f0d4d4",
};

const PARADE_CELEBRATION: CelebrationTheme<'static> = CelebrationTheme {
    effect: "goal-impact-wave",
    friendly_accent: "#3c5aa6",
    enemy_accent: "#bf2b35",
    readout_friendly: "#1e3a72",
    readout_enemy: "#8f1c24",
};

/// A living coat: the sheet repeats along the whole body, so any length of
/// snake is clothed and the marks ripple where they lie.
const fn living_coat(drift_cells: f64) -> Wear {
    Wear {
        from: Anchor::Whole,
        cells: None,
        repeat: true,
        fade: None,
        drift_cells,
    }
}

static TIGER_LIVE: Recipe = Recipe {
    anim_speed: 1.0,
    rotation_degrees: 0.0,
    id: "tiger-live@1",
    name: "Living Tiger",
    tone: "#c2661b",
    label: "#0f172a",
    friendly_outline: FRIENDLY_RIMS,
    enemy_outline: ENEMY_RIMS,
    free_outline: FREE_RIMS,
    core: "#1a0d04",
    ramp_rgb: (255, 246, 232),
    sheet: Sheet {
        url: "images/skins/tiger-live.v1.png",
        width: 896.0,
        height: 320.0,
        rows: DEFAULT_SPRITE_ROWS,
        texels_per_cell: 16.0,
    },
    wear: living_coat(0.0),
    base: SAVANNA_BASE,
    celebration: SAVANNA_CELEBRATION,
    engine: OnceLock::new(),
};

static ZEBRA_LIVE: Recipe = Recipe {
    // Half speed: a hide ripples, it does not flicker.
    anim_speed: 0.5,
    // Turned so the stripes run *along* the snake rather than banding across
    // it every cell. Baked into the PNG by the build; see `rotation_degrees`.
    rotation_degrees: 30.0,
    id: "zebra-live@1",
    name: "Living Zebra",
    tone: "#8a8a8a",
    label: "#0f172a",
    friendly_outline: FRIENDLY_RIMS,
    enemy_outline: ENEMY_RIMS,
    free_outline: FREE_RIMS,
    core: "#12161c",
    ramp_rgb: (255, 255, 255),
    sheet: Sheet {
        url: "images/skins/zebra-live.v1.png",
        width: 576.0,
        height: 320.0,
        rows: DEFAULT_SPRITE_ROWS,
        texels_per_cell: 16.0,
    },
    // Half a cell of travel per animation cycle, backwards: the marks creep
    // *toward* the head, which reads as the snake sliding forwards through its
    // own coat rather than the coat travelling with it.
    wear: living_coat(-0.5),
    base: SAVANNA_BASE,
    celebration: SAVANNA_CELEBRATION,
    engine: OnceLock::new(),
};

/// The flag is the one sheet worn as a **sprite** rather than as a coat, and
/// every part of that is deliberate.
///
/// A flag is a thing with a top and a bottom; repeating it along a snake would
/// stamp a row of little flags, which is not what a flag looks like. So it is
/// pinned to the head, drawn once, and allowed to end — and the fade is what
/// makes ending look intentional. Without it the twentieth cell is a hard
/// vertical cut with the base colour after it.
///
/// The ramp is six cells of the twenty, which is long enough to read as the
/// flag dissolving and short enough to leave most of it solid.
static STARS_AND_STRIPES: Recipe = Recipe {
    anim_speed: 1.0,
    rotation_degrees: 0.0,
    id: "stars-and-stripes@1",
    name: "Stars and Stripes",
    // The mean of the sheet: red, white and blue in roughly equal measure is a
    // dusty mid-tone, and that is the honest one-colour answer for a pill.
    // The honest mean of a flag that is roughly two parts cream to two parts
    // red to one part navy — which comes out a dusty rose, and is what a
    // results-table pill for this skin actually is.
    tone: "#b97f8c",
    label: "#0f172a",
    friendly_outline: FRIENDLY_RIMS,
    enemy_outline: ENEMY_RIMS,
    free_outline: FREE_RIMS,
    core: "#14203f",
    ramp_rgb: (255, 250, 245),
    sheet: Sheet {
        url: "images/skins/stars-and-stripes.v1.png",
        width: 320.0,
        height: 308.0,
        // Fourteen, not twenty: a picture's frames must step by a whole period
        // of the source or they *translate* between frames, and a translation
        // across a one-cell body is indistinguishable from the snake rotating.
        // This source holds fourteen whole periods, so it gets fourteen honest
        // frames rather than twenty that slide.
        rows: 14,
        texels_per_cell: 16.0,
    },
    wear: Wear {
        from: Anchor::Head,
        cells: None,
        repeat: false,
        fade: Some(Fade::trailing(6.0, 12)),
        drift_cells: 0.0,
    },
    base: PARADE_BASE,
    celebration: PARADE_CELEBRATION,
    engine: OnceLock::new(),
};

const GARAGE_BASE: BaseTheme<'static> = BaseTheme {
    friendly_zone: "#eaeef6",
    enemy_zone: "#fbebec",
    friendly_wall: "#76839c",
    enemy_wall: "#b8797d",
    friendly_text: "#ccd5e6",
    enemy_text: "#eed2d4",
};

const GARAGE_CELEBRATION: CelebrationTheme<'static> = CelebrationTheme {
    effect: "goal-impact-wave",
    friendly_accent: "#4a6ea8",
    enemy_accent: "#cf2733",
    readout_friendly: "#22406f",
    readout_enemy: "#9a1620",
};

/// Racing livery: decal tape on black, worn as a repeating coat.
///
/// The only sheet in the family whose ink is **light**. The others are worn
/// over mid-tone animal colour where a near-black number reads best; this one
/// is mostly black, and a dark number on it disappears. Ink is chosen against
/// the coat it sits on, which is the whole reason `label` is a field rather
/// than a constant.
static RACE_LIVERY: Recipe = Recipe {
    anim_speed: 1.0,
    rotation_degrees: 0.0,
    id: "race-livery@1",
    name: "Race Livery",
    // Mostly black with red and white decals. The raw mean is nearly black,
    // and that is unusable rather than merely dark: **no** core colour is 2:1
    // against a tone that deep, because there is nothing below it to contrast
    // with, so `the_ink_and_the_core_are_legible_on_the_tone_they_sit_on`
    // rejects it outright. This is the same warm grey pulled up to where the
    // head can still be told from the body — still honestly dark, still
    // obviously not one of the animals.
    tone: "#7a5458",
    label: "#ffffff",
    friendly_outline: FRIENDLY_RIMS,
    enemy_outline: ENEMY_RIMS,
    free_outline: FREE_RIMS,
    core: "#0b0b0d",
    // A cool white light, because the decals are white and red and a warm ramp
    // would push the whole thing orange.
    ramp_rgb: (245, 248, 255),
    sheet: Sheet {
        url: "images/skins/race-livery.v1.png",
        width: 1280.0,
        height: 320.0,
        rows: DEFAULT_SPRITE_ROWS,
        texels_per_cell: 16.0,
    },
    wear: living_coat(0.0),
    base: GARAGE_BASE,
    celebration: GARAGE_CELEBRATION,
    engine: OnceLock::new(),
};

/// Every sheet skin, in catalogue order.
pub const FAMILY: [SpriteSkin; 4] = [
    SpriteSkin(&ZEBRA_LIVE),
    SpriteSkin(&TIGER_LIVE),
    SpriteSkin(&STARS_AND_STRIPES),
    SpriteSkin(&RACE_LIVERY),
];

fn compile(recipe: &'static Recipe) -> CompositeSkin {
    compile_with(recipe, Tuning::of(recipe).sane())
}

fn compile_with(recipe: &'static Recipe, tuning: Tuning) -> CompositeSkin {
    let swatch = |outline: &'static str| Swatch {
        // Every role reports the same sheet, because every role wears the same
        // sheet. The rim is where the roles differ, and it is what the
        // conformance suite is pointed at by `side_cue`.
        fill: recipe.tone.to_string(),
        outline: outline.to_string(),
        label: recipe.label.to_string(),
        swatch: recipe.tone.to_string(),
        accent: recipe.tone.to_string(),
    };

    // One baked frame per row, so `frame_index` walking the frames *is* the
    // sheet walking its rows — no resampling between two clocks that would
    // otherwise dwell unevenly on some rows and skip others.
    let frames: Vec<Frame> = (0..recipe.sheet.rows)
        .map(|row| Frame {
            friendly: [
                swatch(recipe.friendly_outline[0]),
                swatch(recipe.friendly_outline[1]),
            ],
            enemy: [
                swatch(recipe.enemy_outline[0]),
                swatch(recipe.enemy_outline[1]),
            ],
            free_for_all: [
                swatch(recipe.friendly_outline[0]),
                swatch(recipe.enemy_outline[0]),
                swatch(recipe.free_outline[0]),
                swatch(recipe.free_outline[1]),
            ],
            ramp_opacity: GRADIENT_MAX_OPACITY,
            wave_phase_turns: 0.0,
            time_turns: row as f64 / recipe.sheet.rows as f64,
            layer_opacity: Vec::new(),
            scalars: Vec::new(),
            literals: Vec::new(),
        })
        .collect();

    let mut layers = document_layers(
        BOOST_EXTRA,
        OUTLINE_EXTRA,
        recipe.ramp_rgb,
        GRADIENT_CELLS,
        HEAD_CORE_RATIO,
    );
    // Same reasoning as the coats: a flat colour re-laid over the head cell is
    // a patch stamped on the art. The ramp and the dark core stay, because they
    // modulate the art rather than replace it, and the head is the one part of
    // a snake that has to stay unambiguous.
    layers.retain(|layer| !matches!(layer.id, "head-cap" | "head-highlight"));

    // Rasterise the contour in one pass. `radius` is exactly half `line_width`,
    // so a run's round cap and a joint disc are the *same* circle — and painting
    // that boundary two or three times composites its antialiased edge two or
    // three times, which makes the elbow of every corner heavier than the rest
    // of the outline. Opaque colour, so only the edge pixels differ; visible
    // all the same, and reported as such.
    //
    // Classic keeps the old emission because its committed trace pins the op
    // sequence byte for byte, and that trace is the tripwire for accidental
    // changes to shared painting. Flipping it there is a deliberate act with a
    // re-recorded golden, not a side effect of this.
    for layer in &mut layers {
        if layer.region == Region::Contour
            && let LayerKind::Ribbon { single_pass, .. } = &mut layer.kind
        {
            *single_pass = true;
        }
    }

    let insert_at = layers
        .iter()
        .position(|layer| layer.id == "body")
        .map(|body| body + 1)
        .unwrap_or(layers.len());

    let cells = recipe.sheet.cells();
    let mut sprite = span_layer(
        "sheet",
        Region::Body,
        Span {
            from: recipe.wear.from,
            // A picture spans what it was *drawn* to span, not what its
            // aspect implies: the flag's row is 1.4 cells tall, so its aspect
            // says 14.5 cells while it was authored for 20. Using the aspect
            // here would cut the flag short and nothing would say so.
            natural: recipe
                .wear
                .cells
                .or((!recipe.wear.repeat).then(|| recipe.sheet.cells_long())),
            // A sprite squeezed below a few cells is a smear rather than a
            // picture, so it steps aside and lets the base show instead.
            min: if recipe.wear.repeat { 0.0 } else { 3.0 },
            priority: 10,
        },
        Source::Image {
            region: 0,
            fit: if recipe.wear.repeat {
                Fit::Tile {
                    cells_per_repeat: Some(cells),
                }
            } else {
                Fit::Cutout {
                    cells_tall: recipe.sheet.cells_tall(),
                }
            },
            fade: recipe.wear.fade,
            drift_cells: tuning.drift_cells,
        },
    );
    // A one-cell snake is a disc with a head core on it; a fragment of sheet on
    // a body that small is a smudge the core mostly covers anyway.
    sprite.omit_on_single_cell = true;
    layers.insert(insert_at, sprite);

    CompositeSkin::with_atlas(
        recipe.id,
        recipe.name,
        layers,
        frames,
        MS_PER_ROW * recipe.sheet.rows as f64 / tuning.anim_speed,
        CompositeConfig {
            boost_color: BOOST_COLOR.to_string(),
            head_core_color: recipe.core.to_string(),
            head_core_ratio: HEAD_CORE_RATIO,
            head_core_is_dark: true,
            wave: None,
        },
        Atlas::new(
            [recipe.sheet.url.to_string()],
            // The whole image, read as rows. A sheet cannot be padded on any
            // edge: `x` because padding would gap every repeat, `y` because
            // padding would gap every frame (`skin::atlas`).
            vec![AtlasRegion::sheet(
                0,
                recipe.sheet.width,
                recipe.sheet.height,
                recipe.sheet.rows,
            )],
        ),
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

impl SnakeSkin for SpriteSkin {
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

    /// The body is a picture, so the rim is what says whose side it is on.
    fn side_cue(&self) -> SideCue {
        SideCue::Contour
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
    use crate::skin::paint::{OpRecorder, PaintOp};
    use crate::skin::{SnakePose, SnakeRole};

    fn ops(skin: &SpriteSkin, cells: &[(f64, f64)], anim_ms: f64) -> Vec<PaintOp> {
        let mut recorder = OpRecorder::new();
        skin.paint_alive(
            &mut PaintCtx::recording(&mut recorder),
            &SnakePose {
                cells,
                cell_size: 12.0,
                boost_active: false,
                anim_ms,
                reduced_motion: false,
                detail_scale: 1.0,
            },
            &SkinIdentity {
                role: SnakeRole::Own,
                shade_slot: 0,
            },
        )
        .expect("a recording painter cannot fail");
        recorder.ops().to_vec()
    }

    fn blits(ops: &[PaintOp]) -> Vec<(f64, f64, f64, f64)> {
        let mut alpha = 1.0;
        let mut out = Vec::new();
        for op in ops {
            match op {
                PaintOp::SetGlobalAlpha(value) => alpha = *value,
                PaintOp::DrawImage {
                    source: (_, sy, _, _),
                    dest: (dx, _, dw, _),
                    ..
                } => out.push((*sy, *dx, *dw, alpha)),
                _ => {}
            }
        }
        out
    }

    /// Every skin in the family compiles. `compile` panics on an invalid layer
    /// stack, so this is the registration check the catalogue would otherwise
    /// only make the first time somebody wore one.
    #[test]
    fn every_sheet_skin_compiles_and_reports_itself() {
        for skin in &FAMILY {
            assert!(!skin.id().is_empty());
            assert!(!skin.name().is_empty());
            assert_eq!(skin.side_cue(), SideCue::Contour);
            assert!(skin.base_theme().is_some());
            assert!(skin.celebration_theme().is_some());
        }
    }

    fn by_id(id: &str) -> &'static SpriteSkin {
        FAMILY
            .iter()
            .find(|skin| skin.id() == id)
            .unwrap_or_else(|| panic!("no skin called {id}"))
    }

    /// One baked frame per row, walked in order over the period. This is the
    /// property that makes the animation actually play the sheet rather than
    /// resample it: two clocks that dwell unevenly would visibly stutter.
    #[test]
    fn a_sheet_skin_plays_every_row_exactly_once_per_cycle() {
        let skin = by_id("zebra-live@1");
        let body = [(0.0, 0.0), (24.0, 0.0)];
        // The engine's own period. Deriving it from `recipe.anim_speed` was
        // wrong in a way that only showed up once somebody used the sidebar:
        // the recipe literal is a *fallback*, and the skin actually plays at
        // whatever `tuning.json` says. A test that reads a different source
        // from the code under test is testing nothing.
        let period = skin.engine().period_ms();

        let mut rows = Vec::new();
        for step in 0..DEFAULT_SPRITE_ROWS {
            let painted = blits(&ops(skin, &body, step as f64 * period / 20.0));
            assert!(!painted.is_empty(), "row {step} painted nothing");
            rows.push(painted[0].0);
        }
        // Derived from the recipe, not written down: the sheets are rebuilt at
        // whatever `--cell` the art needs, and a literal here goes stale the
        // first time that changes.
        let row_height = skin.0.sheet.height / skin.0.sheet.rows as f64;
        let expected: Vec<f64> = (0..DEFAULT_SPRITE_ROWS)
            .map(|row| row as f64 * row_height)
            .collect();
        assert_eq!(rows, expected, "rows must play in order, one per frame");

        // And the cycle closes rather than drifting.
        let wrapped = blits(&ops(skin, &body, period));
        assert_eq!(wrapped[0].0, 0.0);
    }

    /// The clock may change what a sheet draws and never how much it draws.
    #[test]
    fn a_playing_sheet_costs_exactly_what_a_still_one_does() {
        for skin in &FAMILY {
            let body = [(0.0, 0.0), (18.0, 0.0), (18.0, 9.0)];
            let counts: Vec<usize> = (0..DEFAULT_SPRITE_ROWS)
                .map(|step| ops(skin, &body, step as f64 * MS_PER_ROW).len())
                .collect();
            assert!(
                counts.windows(2).all(|pair| pair[0] == pair[1]),
                "{} changed op count with the clock: {counts:?}",
                skin.id()
            );
        }
    }

    /// The flag's whole configuration, asserted as behaviour rather than as
    /// fields: pinned at the head, drawn once, and ending in a ramp instead of
    /// a cut.
    #[test]
    fn the_flag_is_drawn_once_from_the_head_and_fades_out() {
        let flag = by_id("stars-and-stripes@1");
        assert_eq!(flag.id(), "stars-and-stripes@1");
        // Comfortably longer than the twenty cells the sheet covers.
        let painted = blits(&ops(flag, &[(0.0, 0.0), (36.0, 0.0)], 0.0));
        assert!(!painted.is_empty());

        // It starts at the head and stops well before the tail. The head is at
        // `-0.5`, not `0`: spans cover the *paintable* range, which includes
        // the half-cell cap past the head's centre.
        let reach = painted
            .iter()
            .map(|(_, dx, _, _)| *dx)
            .fold(f64::MIN, f64::max);
        assert!(
            painted.iter().any(|(_, dx, _, _)| (dx + 0.5).abs() < 1e-6),
            "the flag must start at the head cap: {painted:?}"
        );
        assert!(
            reach < 30.0,
            "the flag must not repeat down the whole snake: reached {reach}"
        );

        // And it ends in a descending ramp rather than at full strength.
        let alphas: Vec<f64> = painted.iter().map(|(_, _, _, alpha)| *alpha).collect();
        assert!(
            alphas.iter().any(|alpha| *alpha > 0.99),
            "most of the flag must be solid: {alphas:?}"
        );
        let tail = &alphas[alphas.len().saturating_sub(12)..];
        assert!(
            tail.windows(2).all(|pair| pair[0] > pair[1]),
            "the flag must fade monotonically: {tail:?}"
        );
        assert!(
            *tail.last().unwrap() < 0.05,
            "the flag must reach nothing: {tail:?}"
        );
    }

    /// The repeating sheets are worn the other way, and cover any length.
    #[test]
    fn a_living_coat_repeats_to_cover_whatever_it_is_given() {
        for skin in FAMILY.iter().filter(|skin| skin.0.wear.repeat) {
            for length in [4.0, 20.0, 60.0] {
                let painted = blits(&ops(skin, &[(0.0, 0.0), (length, 0.0)], 0.0));
                // How far the paint actually *reaches*, not where the last blit
                // began: one repeat is now longer than most bodies, so the last
                // blit usually starts at the head and covers everything.
                let reach = painted
                    .iter()
                    .map(|(_, dx, dw, _)| dx + dw)
                    .fold(f64::MIN, f64::max);
                assert!(
                    reach >= length + 0.5 - 1e-6,
                    "{} covered only {reach} of {length} cells",
                    skin.id()
                );
                assert!(
                    painted.iter().all(|(_, _, _, alpha)| *alpha > 0.99),
                    "a coat has no fade to apply"
                );
            }
        }
    }

    /// Sprites stack. A sheet layer is an ordinary layer, so a second one over
    /// the first composites through its alpha — which is what makes "a sprite
    /// is just a layer" a claim about the engine and not a turn of phrase.
    #[test]
    fn two_sheet_layers_stack_and_both_paint() {
        let sheet = |id, region| {
            span_layer(
                id,
                region,
                Span::WHOLE,
                Source::Image {
                    region: 0,
                    fit: Fit::Tile {
                        cells_per_repeat: Some(20.0),
                    },
                    fade: None,
                    drift_cells: 0.0,
                },
            )
        };
        let skin = CompositeSkin::with_atlas(
            "stacked@test",
            "Stacked",
            vec![
                span_layer(
                    "base",
                    Region::Body,
                    Span::WHOLE,
                    Source::Solid(crate::skin::layer::ColorSlot::Fill),
                ),
                sheet("under", Region::Body),
                sheet("over", Region::Body),
            ],
            vec![Frame {
                friendly: [
                    Swatch {
                        fill: "#3c8dde".into(),
                        outline: "#286eae".into(),
                        label: "#ffffff".into(),
                        swatch: "#3c8dde".into(),
                        accent: "#3c8dde".into(),
                    },
                    Swatch {
                        fill: "#3c8dde".into(),
                        outline: "#286eae".into(),
                        label: "#ffffff".into(),
                        swatch: "#3c8dde".into(),
                        accent: "#3c8dde".into(),
                    },
                ],
                enemy: [
                    Swatch {
                        fill: "#de3c3c".into(),
                        outline: "#ae2828".into(),
                        label: "#ffffff".into(),
                        swatch: "#de3c3c".into(),
                        accent: "#de3c3c".into(),
                    },
                    Swatch {
                        fill: "#de3c3c".into(),
                        outline: "#ae2828".into(),
                        label: "#ffffff".into(),
                        swatch: "#de3c3c".into(),
                        accent: "#de3c3c".into(),
                    },
                ],
                free_for_all: std::array::from_fn(|_| Swatch {
                    fill: "#888888".into(),
                    outline: "#555555".into(),
                    label: "#ffffff".into(),
                    swatch: "#888888".into(),
                    accent: "#888888".into(),
                }),
                ramp_opacity: 0.2,
                wave_phase_turns: 0.0,
                time_turns: 0.0,
                layer_opacity: Vec::new(),
                scalars: Vec::new(),
                literals: Vec::new(),
            }],
            1_000.0,
            CompositeConfig {
                boost_color: BOOST_COLOR.to_string(),
                head_core_color: "#222222".to_string(),
                head_core_ratio: HEAD_CORE_RATIO,
                head_core_is_dark: true,
                wave: None,
            },
            Atlas::new(
                ["images/skins/sheet.v1.png".to_string()],
                vec![AtlasRegion::sheet(0, 1280.0, 1280.0, 20)],
            ),
            None,
            None,
        )
        .expect("two sheet layers is a valid stack");

        let mut recorder = OpRecorder::new();
        skin.paint_alive(
            &mut PaintCtx::recording(&mut recorder),
            &SnakePose::still(&[(0.0, 0.0), (20.0, 0.0)], 12.0, false),
            &SkinIdentity {
                role: SnakeRole::Own,
                shade_slot: 0,
            },
        )
        .expect("a recording painter cannot fail");

        let drawn = recorder
            .ops()
            .iter()
            .filter(|op| matches!(op, PaintOp::DrawImage { .. }))
            .count();
        assert!(
            drawn >= 4,
            "both sheet layers must paint their repeats, saw {drawn}"
        );
    }

    /// Ink is chosen against the art it sits on, not inherited. Three of these
    /// sheets are mid-tone and take a near-black number; the livery is dark and
    /// takes a white one, and getting that backwards makes the carried-food
    /// readout vanish on exactly one skin.
    #[test]
    fn the_ink_and_the_core_are_legible_on_the_tone_they_sit_on() {
        use skin_schema::color::{Rgb, contrast_ratio};

        let white = Rgb::parse("#ffffff").expect("literal");
        let slate = Rgb::parse("#0f172a").expect("literal");
        for skin in &FAMILY {
            let recipe = skin.0;
            let tone = Rgb::parse(recipe.tone).expect("a flat hex tone");
            let ink = Rgb::parse(recipe.label).expect("a flat hex ink");
            let ratio = contrast_ratio(ink, tone);
            assert!(
                ratio >= skin_schema::MIN_READY_CHECK_CONTRAST,
                "{}: the number reaches only {ratio:.2}:1 against `{}`",
                recipe.id,
                recipe.tone
            );
            let best = contrast_ratio(white, tone).max(contrast_ratio(slate, tone));
            assert!(
                ratio >= best - 1e-9,
                "{}: `{}` reaches {ratio:.2}:1 where the other ink would reach \
                 {best:.2}:1",
                recipe.id,
                recipe.label
            );

            let core = Rgb::parse(recipe.core).expect("a flat hex core");
            assert!(
                contrast_ratio(core, white) >= skin_schema::MIN_READY_CHECK_CONTRAST,
                "{}: the ready-check is illegible on this core",
                recipe.id
            );
            assert!(
                contrast_ratio(core, tone) >= skin_schema::MIN_HEAD_CORE_CONTRAST,
                "{}: the head core does not stand out on this sheet",
                recipe.id
            );
            assert!(skin.metrics(false).head_core_is_dark);
        }
    }

    /// Repeat length is the sheet's **width**, and the only cure for a visible
    /// repeat is more of it.
    ///
    /// A 20-cell repeat on a 33-cell snake redraws 13 cells — 40% of the body —
    /// and no amount of seam work hides that, because it is not a seam. A wider
    /// sheet at the same 16px cell is a longer repeat, so the sheet is
    /// deliberately not required to be square.
    #[test]
    fn a_wider_sheet_is_a_longer_repeat_at_the_same_cell_size() {
        let square = Sheet {
            url: "x",
            width: 320.0,
            height: 320.0,
            rows: 20,
            texels_per_cell: 16.0,
        };
        assert_eq!(square.cells(), 20.0);
        // Same 16px rows, twice the width: twice the repeat, same texel density.
        let wide = Sheet {
            width: 640.0,
            ..square
        };
        assert_eq!(wide.cells(), 40.0);
        assert_eq!(
            wide.height / wide.rows as f64,
            square.height / square.rows as f64
        );
        // And the atlas agrees, so `cells_per_repeat` and the source rectangle
        // cannot drift apart.
        let region = AtlasRegion::sheet(0, wide.width, wide.height, wide.rows);
        assert_eq!(region.frame_cells(0.0), wide.cells());
    }

    /// Provenance that would otherwise rot: the rotation recorded in the
    /// recipe is not read at paint time — it is baked into the PNG — so nothing
    /// would notice if it drifted from the command that built the art. Turning
    /// a texture changes its mark spacing, and therefore the row length the
    /// build derives, so the two are checked against each other here.
    #[test]
    fn a_rotated_sheet_records_the_turn_that_produced_it() {
        for skin in &FAMILY {
            let recipe = skin.0;
            assert!(
                recipe.rotation_degrees.is_finite()
                    && (0.0..360.0).contains(&recipe.rotation_degrees),
                "{}: {} is not an angle",
                recipe.id,
                recipe.rotation_degrees
            );
            assert!(
                recipe.anim_speed.is_finite() && recipe.anim_speed > 0.0,
                "{}: an animation speed of {} would stop or reverse the sheet",
                recipe.id,
                recipe.anim_speed
            );
            // A turned sheet is not square, because turning changes the mark
            // spacing and the build sizes the row from it.
            if recipe.rotation_degrees % 90.0 != 0.0 {
                assert_ne!(
                    recipe.sheet.width, recipe.sheet.height,
                    "{}: declares a turn but ships square art — was it rebuilt?",
                    recipe.id
                );
            }
        }
    }

    /// The tuning file is the source of truth, and a live edit really rebuilds.
    ///
    /// Both halves matter. If the file were merely advisory the sidebar would
    /// save into a void; if `tune` did not rebuild, the preview would lie.
    #[test]
    fn tuning_comes_from_the_file_and_a_live_edit_rebuilds_the_skin() {
        // Every shipped sheet is named by the file, or the sidebar has nothing
        // to write back and the fallback silently wins.
        for skin in &FAMILY {
            assert!(
                tuning_table().contains_key(skin.0.id),
                "{} is missing from client/design/sprites/tuning.json",
                skin.0.id
            );
        }
        // The mechanism, not the numbers. `tuning.json` is edited from the QA
        // sidebar and committed as data, so pinning a value here would make
        // every tuning session a failing build — which is exactly what
        // happened, and is a worse outcome than the check is worth.
        for skin in &FAMILY {
            let from_file = tuning_table()
                .get(skin.0.id)
                .copied()
                .expect("checked above");
            assert_eq!(
                Tuning::of(skin.0),
                from_file,
                "{} paints with something other than the file",
                skin.0.id
            );
        }

        // A live edit changes the period, which is the observable end of
        // `anim_speed`, and it must take effect without a rebuild.
        let zebra = by_id("zebra-live@1");
        let period = |skin: &SpriteSkin| skin.engine().period_ms();
        let before = period(zebra);
        assert!(tune(
            "zebra-live@1",
            Tuning {
                anim_speed: 2.0,
                drift_cells: 3.0
            }
        ));
        assert!(
            period(zebra) < before,
            "a faster sheet must have a shorter period: {before} -> {}",
            period(zebra)
        );
        assert!(!tune(
            "not-a-skin@1",
            Tuning {
                anim_speed: 1.0,
                drift_cells: 0.0
            }
        ));

        // Absurd values are clamped rather than allowed to divide by nothing.
        assert!(tune(
            "zebra-live@1",
            Tuning {
                anim_speed: 0.0,
                drift_cells: f64::NAN
            }
        ));
        assert!(period(zebra).is_finite() && period(zebra) > 0.0);

        // Put it back, so test order cannot matter.
        assert!(tune("zebra-live@1", Tuning::of(zebra.0)));
    }

    /// A picture is drawn at authored scale and clipped, not squashed to fit.
    ///
    /// The flag is 14.7 times wider than tall, so mapping its full height onto
    /// one cell — which is what every other fit does, correctly, for a texture —
    /// compressed it until it read as wrapped round a cylinder. `Fit::Cutout`
    /// draws it taller than the body instead and lets the silhouette clip trim
    /// the slivers.
    #[test]
    fn a_picture_sheet_is_drawn_taller_than_the_body_and_clipped() {
        let flag = by_id("stars-and-stripes@1");
        let sheet = flag.0.sheet;
        assert!(
            sheet.cells_tall() > 1.0,
            "a picture worth clipping has to overflow the body: {}",
            sheet.cells_tall()
        );
        // Authored scale in both axes, which is what makes it undistorted: the
        // row's aspect and its cells-long-by-cells-tall must agree.
        let drawn_aspect = sheet.cells_long() / sheet.cells_tall();
        let row_aspect = sheet.width / (sheet.height / sheet.rows as f64);
        assert!(
            (drawn_aspect - row_aspect).abs() < 1e-6,
            "the flag would be stretched: drawn {drawn_aspect:.3} vs source {row_aspect:.3}"
        );

        // And the engine actually emits it taller than one cell.
        let ops = ops(flag, &[(0.0, 0.0), (30.0, 0.0)], 0.0);
        let heights: Vec<f64> = ops
            .iter()
            .filter_map(|op| match op {
                PaintOp::DrawImage {
                    dest: (_, dy, _, dh),
                    ..
                } => Some((*dy, *dh)),
                _ => None,
            })
            .map(|(dy, dh)| {
                // Centred on the centreline, so the top edge is -h/2.
                assert!((dy + dh / 2.0).abs() < 1e-9, "not centred: {dy} {dh}");
                dh
            })
            .collect();
        assert!(!heights.is_empty());
        assert!(
            heights.iter().all(|h| *h > 1.0),
            "the picture must overflow the body: {heights:?}"
        );
    }

    /// The declared dimensions and the committed PNG cannot drift, because the
    /// source rectangle is computed from the declaration long before anything
    /// has decoded — so a stale number is a silently mis-sampled sheet rather
    /// than a loud failure.
    ///
    /// Reads the IHDR directly: 8 bytes of signature, then a length and type,
    /// then width and height as big-endian `u32`.
    #[test]
    fn the_declared_sheet_matches_the_png() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("web/public");
        for skin in &FAMILY {
            let sheet = skin.0.sheet;
            let path = root.join(sheet.url);
            let bytes = match std::fs::read(&path) {
                Ok(bytes) => bytes,
                // The art is produced by `client/design/tools/sprite_sheet.py`
                // and is not in the tree yet. Failing here would make the whole
                // suite red for a missing input rather than for a mistake, so
                // this reports and moves on — and `FAMILY` is not registered in
                // `skin::registry` until the sheets exist, so nothing can ship
                // in this state.
                Err(_) => {
                    println!("no sheet at {}; skipping dimension check", path.display());
                    continue;
                }
            };
            assert_eq!(&bytes[1..4], b"PNG", "{} is not a PNG", sheet.url);
            let dimension = |offset: usize| {
                u32::from_be_bytes([
                    bytes[offset],
                    bytes[offset + 1],
                    bytes[offset + 2],
                    bytes[offset + 3],
                ]) as f64
            };
            assert_eq!(dimension(16), sheet.width, "{} width", sheet.url);
            assert_eq!(dimension(20), sheet.height, "{} height", sheet.url);
            assert_eq!(
                sheet.height % sheet.rows as f64,
                0.0,
                "{} must divide into {} whole rows, or every frame samples \
                 across a row boundary",
                sheet.url,
                sheet.rows
            );
        }
    }
}
