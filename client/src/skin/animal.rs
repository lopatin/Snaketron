//! The animal family — Zebra, Tiger and Jaguar, each as a hide and as a print.
//!
//! What forced Rust: these are the first skins made of **pixels**. A `SkinDoc`
//! describes a palette, a head, a contour and a few animated scalars
//! (`skin-schema/src/lib.rs`); it has no vocabulary for an atlas, a texture, or
//! a repeat length, and section 8 of `specs/skin-shading-prd.md` puts all three
//! on the layer stack rather than in the document. The checkerboards were the
//! compositor's first pattern consumers; these are its first image ones.
//!
//! **The coat is opaque, and that is the whole design.** It replaces the body
//! fill and everything painted under it, so a tiger wears a tiger's colours
//! rather than a team colour with stripes laid over it. Tinting the coat per
//! role was tried and is wrong: a cool-shifted tiger is a striped blue snake,
//! which is neither a tiger nor a thing anyone asked for.
//!
//! **So both sides look alike, and the contour is what tells them apart.** That
//! is a deliberate trade, declared through [`SideCue::Contour`] and enforced —
//! `skin_conformance_team_colours_stay_on_their_own_side` checks the nominated
//! channel, not the fill, and checks that friendly and hostile are genuinely far
//! apart in it. The contour is also **widened to [`OUTLINE_EXTRA`]**, more than
//! twice a painted skin's, because a cue carrying the whole friend/foe reading
//! has to be legible at five pixels a cell. Everything outside the body is
//! still the skin's: the renderer only erases what the snake covers.
//!
//! **Two takes per animal.** A photographic hide and a flat print are different
//! things to wear, and both are honest ways to be a tiger. Both come out of
//! `client/design/tools/build_coat_textures.py`, which is the source for all six
//! PNGs and the file to change if they should look different — the textures
//! themselves are build output and are never hand-edited.
//!
//! **The repeat is longer than a cell, and that is the point.** Seven to ten
//! cells. A pattern keyed to the grid reads as machined; at this length a snake
//! shows a dozen marks before anything comes round, and the repeat has to be
//! looked for. It costs one `drawImage` per repeat per run, so a 21-cell snake
//! wears its coat for two or three blits.
//!
//! **They tile because a model painted the join.** A crop out of a texture does
//! not wrap, and cross-fading its ends together tiles the ghost of that fade —
//! which shipped once and was visible as a repeating blemish on the zebra. The
//! strips are now built with LaMa inpainting a gap between two copies of the
//! tile, so the join is generated rather than blended; see the build script.
//!
//! **What this costs, stated plainly.** `specs/skin-shading-prd.md` section 13
//! gives up the closed-form label-contrast rule for a textured body and calls it
//! review-enforced from there on. A coat that spans black to white cannot share
//! one ink with the number that sits on it, so the carried-food readout leans on
//! its halo over these skins in a way it does not over a painted one. The head
//! is kept clean — the ramp and the dark core paint *over* the coat — so the one
//! part of a snake that must stay unambiguous still does.
//!
//! **Corners.** A coat is a directional texture, so the default `Own` joint
//! policy shows an orientation flip at every turn (`specs/skin-shading-prd.md`
//! section 14). It is kept — `Bisector` is rejected for blits — and on fur the
//! flip reads as the marks wrapping the outside of the bend.

use crate::skin::atlas::{Atlas, AtlasRegion};
use crate::skin::classic::document_layers;
use crate::skin::composite::{
    BaseThemeOwned, CelebrationThemeOwned, CompositeConfig, CompositeSkin, Frame, Swatch,
    span_layer,
};
use crate::skin::layer::{Fit, LayerKind, Region, Source, Span};
use crate::skin::{
    BaseTheme, CelebrationTheme, PaintCtx, SideCue, SkinColors, SkinIdentity, SkinMetrics,
    SnakePose, SnakeSkin,
};
use std::sync::OnceLock;
use wasm_bindgen::prelude::*;

/// Twice a painted skin's contour, because here it is not decoration: it is the
/// only thing on the snake that says whose side it is on. At 2.5px per side it
/// still clears the `MAX_OVERHANG_PX` the schema holds document skins to.
const OUTLINE_EXTRA: f64 = 5.0;
const BOOST_COLOR: &str = "#fff200";
const BOOST_EXTRA: f64 = 9.0;
const HEAD_CORE_RATIO: f64 = 0.37;
const GRADIENT_CELLS: f64 = 7.0;
/// Lighter than classic's 0.3. The head ramp brightens the body, and on a
/// textured one it also washes the coat out; pulling it back keeps the marks
/// legible near the head without losing the which-way-is-it-facing cue.
const GRADIENT_MAX_OPACITY: f64 = 0.22;

/// A coat texture, and how it is worn.
#[derive(Clone, Copy)]
struct Texture {
    /// Versioned and relative, resolved against the page's `<base href>` so an
    /// embedded build under a non-root path finds it too.
    url: &'static str,
    /// The PNG's own dimensions. Declared rather than discovered because the
    /// source rectangle is computed before anything has decoded — and checked
    /// against the committed file by `the_declared_texture_matches_the_png`,
    /// so the two cannot drift.
    width: f64,
    height: f64,
    /// How many cells of body one repeat covers.
    repeat_cells: f64,
}

/// Everything that distinguishes one coat from another.
struct Recipe {
    id: &'static str,
    name: &'static str,
    /// The coat's own representative colour, reported to every surface that
    /// cannot draw a texture. The same for every role, which is the point.
    coat: &'static str,
    /// Ink for the carried-food number and the roster name. Chosen against the
    /// texture rather than derived from a flat fill, because there is no flat
    /// fill — see `the_label_ink_is_legible_on_the_coat_it_sits_on`.
    label: &'static str,
    /// The contour, per side. This is the friend/foe reading, so the two are
    /// held to the hue windows and to a minimum distance apart.
    friendly_outline: [&'static str; 2],
    enemy_outline: [&'static str; 2],
    /// The remaining free-for-all slots. Unaffiliated snakes still need to be
    /// told apart from each other, and from both teams.
    free_outline: [&'static str; 2],
    core: &'static str,
    /// The head ramp's colour. A warm coat wants a warm light on it.
    ramp_rgb: (u8, u8, u8),
    texture: Texture,
    base: BaseTheme<'static>,
    celebration: CelebrationTheme<'static>,
    engine: OnceLock<CompositeSkin>,
}

/// A handle onto one recipe. The compiled skin lives in the recipe's
/// `OnceLock` and is built once per process, never per frame.
pub struct AnimalSkin(&'static Recipe);

impl AnimalSkin {
    fn engine(&self) -> &'static CompositeSkin {
        self.0.engine.get_or_init(|| compile(self.0))
    }
}

/// The contour palette the whole family shares.
///
/// One set of rims for six coats, so a player learns the cue once: cool blue is
/// a friend, warm red is not, and steel and gold are nobody. They are deep and
/// saturated on purpose — a rim has two or three pixels to work with.
const FRIENDLY_RIMS: [&str; 2] = ["#1b6fd0", "#0d4f9e"];
const ENEMY_RIMS: [&str; 2] = ["#d92f3a", "#a5121c"];
const FREE_RIMS: [&str; 2] = ["#4a5766", "#b07d12"];

macro_rules! coat {
    (
        $ident:ident, $id:literal, $name:literal,
        coat: $coat:literal, label: $label:literal, core: $core:literal,
        ramp: $ramp:expr,
        texture: ($url:literal, $w:expr, $h:expr, $repeat:expr),
        base: $base:expr, celebration: $celebration:expr,
    ) => {
        static $ident: Recipe = Recipe {
            id: $id,
            name: $name,
            coat: $coat,
            label: $label,
            friendly_outline: FRIENDLY_RIMS,
            enemy_outline: ENEMY_RIMS,
            free_outline: FREE_RIMS,
            core: $core,
            ramp_rgb: $ramp,
            texture: Texture {
                url: $url,
                width: $w,
                height: $h,
                repeat_cells: $repeat,
            },
            base: $base,
            celebration: $celebration,
            engine: OnceLock::new(),
        };
    };
}

/// The savanna dressing, shared by the three hides and their prints: a cool
/// side and a warm side, so the arena keeps saying what the coats cannot.
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

coat! {
    ZEBRA, "zebra@1", "Zebra",
    // The mean of the hide, which on a black-and-white animal is a mid grey —
    // and which is what a results-table pill for this skin honestly is.
    coat: "#8a8a8a", label: "#0f172a", core: "#12161c",
    ramp: (255, 255, 255),
    texture: ("images/skins/zebra.v1.png", 768.0, 64.0, 12.0),
    base: SAVANNA_BASE, celebration: SAVANNA_CELEBRATION,
}

coat! {
    ZEBRA_PRINT, "zebra-print@1", "Zebra Print",
    coat: "#8f8f8f", label: "#0f172a", core: "#12161c",
    ramp: (255, 255, 255),
    texture: ("images/skins/zebra-print.v1.png", 768.0, 64.0, 12.0),
    base: SAVANNA_BASE, celebration: SAVANNA_CELEBRATION,
}

coat! {
    TIGER, "tiger@1", "Tiger",
    coat: "#c2661b", label: "#0f172a", core: "#1a0d04",
    ramp: (255, 246, 232),
    texture: ("images/skins/tiger.v1.png", 832.0, 64.0, 13.0),
    base: SAVANNA_BASE, celebration: SAVANNA_CELEBRATION,
}

coat! {
    TIGER_PRINT, "tiger-print@1", "Tiger Print",
    coat: "#c96f1d", label: "#0f172a", core: "#1a0d04",
    ramp: (255, 246, 232),
    texture: ("images/skins/tiger-print.v1.png", 832.0, 64.0, 13.0),
    base: SAVANNA_BASE, celebration: SAVANNA_CELEBRATION,
}

coat! {
    JAGUAR, "jaguar@1", "Jaguar",
    coat: "#c08a5c", label: "#0f172a", core: "#1c1207",
    ramp: (255, 248, 238),
    texture: ("images/skins/jaguar.v1.png", 832.0, 64.0, 13.0),
    base: SAVANNA_BASE, celebration: SAVANNA_CELEBRATION,
}

coat! {
    JAGUAR_PRINT, "jaguar-print@1", "Jaguar Print",
    coat: "#c58f61", label: "#0f172a", core: "#1c1207",
    ramp: (255, 248, 238),
    texture: ("images/skins/jaguar-print.v1.png", 832.0, 64.0, 13.0),
    base: SAVANNA_BASE, celebration: SAVANNA_CELEBRATION,
}

/// Every coat in the family, in catalogue order: each animal's hide followed by
/// its print.
pub const FAMILY: [AnimalSkin; 6] = [
    AnimalSkin(&ZEBRA),
    AnimalSkin(&ZEBRA_PRINT),
    AnimalSkin(&TIGER),
    AnimalSkin(&TIGER_PRINT),
    AnimalSkin(&JAGUAR),
    AnimalSkin(&JAGUAR_PRINT),
];

fn compile(recipe: &'static Recipe) -> CompositeSkin {
    let swatch = |outline: &'static str| Swatch {
        // Every role reports the same coat, because every role wears the same
        // coat. The pill in a results table is the honest one-colour answer to
        // "what does this snake look like".
        fill: recipe.coat.to_string(),
        outline: outline.to_string(),
        label: recipe.label.to_string(),
        // The *rim*, not the coat, for the DOM micro-surfaces that get one
        // colour and no texture. Those surfaces exist to tell players apart,
        // and the rim is the only thing here that does.
        swatch: outline.to_string(),
        accent: recipe.coat.to_string(),
    };

    // Nothing about this family varies with the clock: a coat is a coat. One
    // baked frame is the whole ring, which is also why it costs nothing.
    let frames = vec![Frame {
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
        time_turns: 0.0,
        layer_opacity: Vec::new(),
        scalars: Vec::new(),
        literals: Vec::new(),
    }];

    let mut layers = document_layers(
        BOOST_EXTRA,
        OUTLINE_EXTRA,
        recipe.ramp_rgb,
        GRADIENT_CELLS,
        HEAD_CORE_RATIO,
    );

    // The two layers that re-lay flat colour over the head cell. On a painted
    // skin they are the head; over an opaque coat they would be a coloured
    // patch stamped on the fur, so they go. What remains above the coat is the
    // ramp — which lightens the marks rather than covering them, and is still
    // the direction cue — and the dark core.
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

    let mut coat = span_layer(
        "coat",
        Region::Body,
        Span::WHOLE,
        Source::Image {
            region: 0,
            fit: Fit::Tile {
                cells_per_repeat: Some(recipe.texture.repeat_cells),
            },
            fade: None,
            drift_cells: 0.0,
        },
    );
    // A one-cell snake is a disc with a head core on it; a fragment of coat on
    // a body that small is a smudge, and the core covers most of it.
    coat.omit_on_single_cell = true;
    layers.insert(insert_at, coat);

    CompositeSkin::with_atlas(
        recipe.id,
        recipe.name,
        layers,
        frames,
        1.0,
        CompositeConfig {
            boost_color: BOOST_COLOR.to_string(),
            head_core_color: recipe.core.to_string(),
            head_core_ratio: HEAD_CORE_RATIO,
            head_core_is_dark: true,
            wave: None,
        },
        Atlas::new(
            [recipe.texture.url.to_string()],
            // One region, and it is the whole image: a tiling texture cannot be
            // padded along the axis it repeats on without putting a gap between
            // every repeat, so it wraps instead (`skin::atlas`).
            vec![AtlasRegion {
                image: 0,
                x: 0.0,
                y: 0.0,
                width: recipe.texture.width,
                height: recipe.texture.height,
                frames: None,
            }],
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

impl SnakeSkin for AnimalSkin {
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

    /// The body is an animal's, so the rim is what says whose side it is on.
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
    use crate::skin::layer::{Layer, LayerKind};
    use crate::skin::paint::{OpRecorder, PaintCtx};
    use crate::skin::{SnakeRole, fixtures};
    use skin_schema::color::{
        ENEMY_HUES, FRIENDLY_HUES, HueWindow, NEUTRAL_CHROMA, Rgb, contrast_ratio,
        perceptual_distance,
    };

    const RECIPES: [&Recipe; 6] = [
        &ZEBRA,
        &ZEBRA_PRINT,
        &TIGER,
        &TIGER_PRINT,
        &JAGUAR,
        &JAGUAR_PRINT,
    ];

    /// The committed textures, so a test can read what actually shipped rather
    /// than what a constant claims shipped.
    const PNGS: [(&str, &[u8]); 6] = [
        (
            "images/skins/zebra.v1.png",
            include_bytes!("../../web/public/images/skins/zebra.v1.png"),
        ),
        (
            "images/skins/zebra-print.v1.png",
            include_bytes!("../../web/public/images/skins/zebra-print.v1.png"),
        ),
        (
            "images/skins/tiger.v1.png",
            include_bytes!("../../web/public/images/skins/tiger.v1.png"),
        ),
        (
            "images/skins/tiger-print.v1.png",
            include_bytes!("../../web/public/images/skins/tiger-print.v1.png"),
        ),
        (
            "images/skins/jaguar.v1.png",
            include_bytes!("../../web/public/images/skins/jaguar.v1.png"),
        ),
        (
            "images/skins/jaguar-print.v1.png",
            include_bytes!("../../web/public/images/skins/jaguar-print.v1.png"),
        ),
    ];

    /// The rule that outranks art direction, applied where this family put it.
    ///
    /// The generic conformance suite checks the nominated channel already; this
    /// checks the *other* half — that the two sides are far enough apart in it
    /// to be told apart at a glance, which a hue window alone does not
    /// guarantee.
    #[test]
    fn the_two_sides_rims_are_unmistakably_different() {
        /// Three times the ~0.08 nobody would miss. These are two or three
        /// pixels of rim at the smallest cell size, so "different" has to mean
        /// obviously different — but the deepest blue and the deepest red are
        /// both dark, and a distance measured in OKLab counts that shared
        /// darkness against them. 0.25 is what the shipped pair of *deep* rims
        /// reaches; the light pair is far beyond it.
        const MIN_DISTANCE: f64 = 0.25;

        for recipe in RECIPES {
            for friendly in recipe.friendly_outline {
                for enemy in recipe.enemy_outline {
                    let distance = perceptual_distance(
                        Rgb::parse(friendly).expect("a flat hex rim"),
                        Rgb::parse(enemy).expect("a flat hex rim"),
                    );
                    assert!(
                        distance >= MIN_DISTANCE,
                        "{}: the friendly rim `{friendly}` and the hostile rim \
                         `{enemy}` are only {distance:.3} apart, and on this \
                         family the rim is the *only* thing that says which is \
                         which",
                        recipe.id
                    );
                }
            }
        }
    }

    /// The rims have to sit in their own hue windows, and the neutral slots
    /// must not impersonate either side.
    #[test]
    fn every_rim_stays_on_its_own_side() {
        /// `must_be_coloured` separates the two jobs a colour can have here.
        /// A rim *is* the side cue, so a gray one is a bug. A pale endzone
        /// tint is dressing — near-white by design, like classic's — and
        /// judging it by hue would fail every arena that has ever shipped.
        fn assert_window(
            recipe: &Recipe,
            what: &str,
            hex: &str,
            window: HueWindow,
            side: &str,
            must_be_coloured: bool,
        ) {
            let color = Rgb::parse(hex).expect("a flat hex colour");
            let (hue, chroma) = color.oklch_hue_chroma();
            if chroma <= NEUTRAL_CHROMA {
                assert!(
                    !must_be_coloured,
                    "{}: {what} `{hex}` is a gray, so it tells nobody whose \
                     side it is on",
                    recipe.id
                );
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
            for (index, hex) in recipe.friendly_outline.iter().enumerate() {
                assert_window(
                    recipe,
                    &format!("friendly rim [{index}]"),
                    hex,
                    FRIENDLY_HUES,
                    "friendly",
                    true,
                );
            }
            for (index, hex) in recipe.enemy_outline.iter().enumerate() {
                assert_window(
                    recipe,
                    &format!("enemy rim [{index}]"),
                    hex,
                    ENEMY_HUES,
                    "enemy",
                    true,
                );
            }

            // World dressing carries the same obligation.
            for (what, hex, window, side) in [
                (
                    "base.friendly_wall",
                    recipe.base.friendly_wall,
                    FRIENDLY_HUES,
                    "friendly",
                ),
                (
                    "base.friendly_zone",
                    recipe.base.friendly_zone,
                    FRIENDLY_HUES,
                    "friendly",
                ),
                (
                    "base.enemy_wall",
                    recipe.base.enemy_wall,
                    ENEMY_HUES,
                    "enemy",
                ),
                (
                    "base.enemy_zone",
                    recipe.base.enemy_zone,
                    ENEMY_HUES,
                    "enemy",
                ),
                (
                    "celebration.friendly_accent",
                    recipe.celebration.friendly_accent,
                    FRIENDLY_HUES,
                    "friendly",
                ),
                (
                    "celebration.enemy_accent",
                    recipe.celebration.enemy_accent,
                    ENEMY_HUES,
                    "enemy",
                ),
            ] {
                assert_window(recipe, what, hex, window, side, false);
            }

            assert!(
                skin_schema::KNOWN_EFFECTS.contains(&recipe.celebration.effect),
                "{}: names an effect the client cannot draw",
                recipe.id
            );
        }
    }

    /// A rim that carries the side reading has to be wide enough to see.
    ///
    /// This is the other half of moving the cue off the body: a `SideCue`
    /// declaration with a hairline contour behind it would satisfy every colour
    /// check and still leave two teams indistinguishable on the board.
    #[test]
    fn the_rim_is_wide_enough_to_carry_the_reading() {
        /// Per side, in pixels, at the arena's own scale. A painted skin's is
        /// half this.
        const MIN_RIM_PX: f64 = 2.0;

        for recipe in RECIPES {
            let skin = AnimalSkin(recipe);
            assert_eq!(skin.side_cue(), SideCue::Contour);
            assert!(
                skin.metrics(false).overhang_px >= MIN_RIM_PX,
                "{}: a {}px rim is too thin to be the only side cue",
                recipe.id,
                skin.metrics(false).overhang_px
            );
            // ...and the Boost band still outranks it, or the band it hides is
            // competitive information nobody can read.
            assert!(skin.metrics(true).overhang_px > skin.metrics(false).overhang_px);
        }
    }

    /// The number and the roster name sit on the coat, and the coat is a
    /// photograph — so the ink cannot be derived from a flat fill the way every
    /// painted skin's is.
    ///
    /// `specs/skin-shading-prd.md` section 13 hands this to review for textured
    /// bodies, and this is that review written down.
    ///
    /// The 4.5:1 bar every painted skin clears is **unreachable here, by
    /// construction**: a coat that spans black stripe to white stripe has no
    /// ink that clears AA on both, and picking one would only choose which half
    /// of the snake the number vanishes on. So the bar is the 3:1 one WCAG sets
    /// for large and bold text — which the readout is, and which it is drawn
    /// with a contrasting halo on top of — and the ink must additionally be the
    /// *better* of the two available inks for this coat, so no recipe can pick
    /// the worse one and still pass.
    #[test]
    fn the_label_ink_is_legible_on_the_coat_it_sits_on() {
        let white = Rgb::parse("#ffffff").expect("literal");
        let slate = Rgb::parse("#0f172a").expect("literal");
        for recipe in RECIPES {
            let coat = Rgb::parse(recipe.coat).expect("a flat hex coat");
            let ink = Rgb::parse(recipe.label).expect("a flat hex ink");
            let ratio = contrast_ratio(ink, coat);
            assert!(
                ratio >= skin_schema::MIN_READY_CHECK_CONTRAST,
                "{}: the number reaches only {ratio:.2}:1 against the coat's \
                 representative tone `{}`",
                recipe.id,
                recipe.coat
            );
            let best = contrast_ratio(white, coat).max(contrast_ratio(slate, coat));
            assert!(
                ratio >= best - 1e-9,
                "{}: `{}` reaches {ratio:.2}:1 on this coat where the other ink \
                 would reach {best:.2}:1",
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
                contrast_ratio(core, coat) >= skin_schema::MIN_HEAD_CORE_CONTRAST,
                "{}: the head core does not stand out on this coat",
                recipe.id
            );
            assert!(AnimalSkin(recipe).metrics(false).head_core_is_dark);
        }
    }

    /// The structural claim: one tiled texture over the body, nothing flat
    /// stamped on the head, and the head core still on top.
    ///
    /// A regression that turned the coat back into a sprite — one blit, clipped
    /// — would still paint something and still pass every generic check, which
    /// is exactly what the image path did before this family existed.
    #[test]
    fn the_coat_is_one_opaque_tiled_texture_over_the_whole_body() {
        for recipe in RECIPES {
            let engine = AnimalSkin(recipe).engine();
            let layers = engine.layers();
            let ids: Vec<&str> = layers.iter().map(|layer| layer.id).collect();

            assert_eq!(
                ids,
                vec![
                    "boost-band",
                    "outline",
                    "body",
                    "coat",
                    "head-ramp",
                    "head-core"
                ],
                "{}: unexpected layer stack",
                recipe.id
            );

            let coat: &Layer = &layers[3];
            assert_eq!(coat.region, Region::Body, "{}", recipe.id);
            assert!(coat.omit_on_single_cell, "{}", recipe.id);
            assert!(
                coat.opacity_track.is_none(),
                "{}: the coat is opaque; an opacity track would blend it with \
                 the fill it is supposed to replace",
                recipe.id
            );
            match &coat.kind {
                LayerKind::Span {
                    source:
                        Source::Image {
                            region,
                            fit: Fit::Tile { cells_per_repeat },
                            fade: None,
                            drift_cells: 0.0,
                        },
                    ..
                } => {
                    assert_eq!(*region, 0, "{}", recipe.id);
                    assert_eq!(
                        *cells_per_repeat,
                        Some(recipe.texture.repeat_cells),
                        "{}",
                        recipe.id
                    );
                }
                other => panic!("{}: the coat is not a tiled texture: {other:?}", recipe.id),
            }

            // A pattern keyed to the grid is not a coat. This is the whole
            // reason the texture subsystem had to grow a repeat length.
            assert!(
                recipe.texture.repeat_cells >= 3.0,
                "{}: a {}-cell repeat is short enough to read as a grid",
                recipe.id,
                recipe.texture.repeat_cells
            );

            // The coat is a body layer, so it adds no overhang: the contour is
            // still the only thing painting past the silhouette.
            assert_eq!(
                AnimalSkin(recipe).metrics(false).overhang_px,
                OUTLINE_EXTRA / 2.0
            );
            assert_eq!(
                AnimalSkin(recipe).metrics(true).overhang_px,
                BOOST_EXTRA / 2.0
            );
        }
    }

    /// The source rectangle is computed from declared numbers, before anything
    /// has decoded. If those numbers stop describing the committed PNG, the
    /// skin samples a fraction of its own texture and the repeat stops being
    /// seamless — silently, and only in a browser.
    #[test]
    fn the_declared_texture_matches_the_png() {
        /// Width and height out of a PNG's IHDR, which is always the first
        /// chunk and always at a fixed offset.
        fn dimensions(bytes: &[u8]) -> (f64, f64) {
            assert_eq!(&bytes[..8], b"\x89PNG\r\n\x1a\n", "not a PNG");
            let read = |at: usize| {
                u32::from_be_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]]) as f64
            };
            (read(16), read(20))
        }

        for recipe in RECIPES {
            let (_, bytes) = PNGS
                .iter()
                .find(|(url, _)| *url == recipe.texture.url)
                .unwrap_or_else(|| {
                    panic!(
                        "{}: names `{}`, which this test does not have a copy \
                         of — add it to PNGS",
                        recipe.id, recipe.texture.url
                    )
                });
            let (width, height) = dimensions(bytes);
            assert_eq!(
                (recipe.texture.width, recipe.texture.height),
                (width, height),
                "{}: declares {}x{} but `{}` is {width}x{height}. Re-run \
                 client/design/tools/build_coat_textures.py and update the \
                 recipe, or the coat samples part of itself.",
                recipe.id,
                recipe.texture.width,
                recipe.texture.height,
                recipe.texture.url
            );

            // One cell of body is the texture's height, so the repeat length
            // and the aspect have to agree or the marks come out stretched.
            let natural = width / height;
            assert!(
                (natural - recipe.texture.repeat_cells).abs() < 1e-9,
                "{}: a {natural}-cell-long texture worn over {} cells would \
                 squash the marks",
                recipe.id,
                recipe.texture.repeat_cells
            );
        }
    }

    /// Every coat is its own file. Two recipes sharing a texture would be two
    /// catalogue entries that paint the same snake.
    #[test]
    fn no_two_coats_share_a_texture() {
        let mut seen = std::collections::HashSet::new();
        for recipe in RECIPES {
            assert!(
                seen.insert(recipe.texture.url),
                "{} reuses `{}`",
                recipe.id,
                recipe.texture.url
            );
        }
    }

    /// A coat costs one blit per repeat, and the repeat length is the knob.
    /// `skin::perf` enforces the ceiling across the catalogue; this one exists
    /// so a change to a repeat length fails here, next to the number that
    /// caused it, with the arithmetic spelled out.
    #[test]
    fn a_coat_costs_one_blit_per_repeat() {
        for recipe in RECIPES {
            let mut recorder = OpRecorder::new();
            AnimalSkin(recipe)
                .paint_alive(
                    &mut PaintCtx::recording(&mut recorder),
                    &SnakePose::still(crate::skin::perf::PERF_BODY, 15.0, true),
                    &SkinIdentity {
                        role: SnakeRole::Own,
                        shade_slot: 0,
                    },
                )
                .expect("a recording painter cannot fail");

            let blits = recorder
                .ops()
                .iter()
                .filter(|op| matches!(op, crate::skin::paint::PaintOp::DrawImage { .. }))
                .count();
            // 21 cells of paint over the recipe's repeat, rounded up.
            let expected = (21.0 / recipe.texture.repeat_cells).ceil() as usize;
            assert_eq!(
                blits, expected,
                "{}: a 21-cell body took {blits} blits, not the {expected} its \
                 {}-cell repeat accounts for",
                recipe.id, recipe.texture.repeat_cells
            );

            let ops = recorder.shapes().len();
            assert!(
                ops < 200,
                "{} emits {ops} ops, over the per-snake ceiling skin::perf \
                 enforces",
                recipe.id
            );
        }
    }

    /// A coat does not move, and that has to be true of the ops as well as of
    /// the intent — an animated skin that forgot to bake a ring would paint the
    /// same frame while claiming a clock.
    #[test]
    fn a_coat_is_still_at_every_clock() {
        for recipe in RECIPES {
            let skin = AnimalSkin(recipe);
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
                        boost_active: false,
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
                for reduced_motion in [true, false] {
                    assert_eq!(
                        resting,
                        golden(anim_ms, reduced_motion),
                        "{} moved at {anim_ms}ms",
                        recipe.id
                    );
                }
            }
        }
    }

    /// A coat on a one-cell snake is a smudge under the head core, so it sits
    /// out — and saying so on the layer is what keeps it a rule rather than an
    /// accident of how short bodies happen to allocate.
    #[test]
    fn a_single_cell_snake_wears_no_coat() {
        for recipe in RECIPES {
            let coat = AnimalSkin(recipe)
                .engine()
                .layers()
                .iter()
                .find(|layer| layer.id == "coat")
                .expect("there is a coat");
            assert!(!coat.applies(false, 1), "{}", recipe.id);
            assert!(coat.applies(false, 2), "{}", recipe.id);
        }
    }
}
