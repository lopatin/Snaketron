//! The default skin — the look Snaketron has always had.
//!
//! This is a mechanical extraction of the original renderer, not a
//! reinterpretation of it, which is why the golden traces recorded before the
//! skin system existed still pass unchanged. Everything here is deliberately
//! literal; the constants are the ones players have been looking at for years.

use crate::skin::composite::{
    BaseThemeOwned, CelebrationThemeOwned, CompositeConfig, CompositeSkin, Frame, Swatch,
};
use crate::skin::layer::{Binding, ColorSlot, DiscPaint, Layer, LayerKind, LayerTransform, Region};
use crate::skin::space::ClipShape;
use crate::skin::{
    BaseTheme, CelebrationTheme, PaintCtx, SkinColors, SkinIdentity, SkinMetrics, SnakePose,
    SnakeSkin,
};
use std::borrow::Cow;
use wasm_bindgen::prelude::*;

/// Team colours, indexed by the within-team shade slot.
const BLUE: [(&str, &str); 2] = [("#70bfe3", "#5299bb"), ("#3c8dde", "#286eae")];
const RED: [(&str, &str); 2] = [("#ff6b6b", "#b84444"), ("#e34e5b", "#a92f3a")];
/// The remaining free-for-all slots, after blue and red.
const STEEL: (&str, &str) = ("#556270", "#353c47");
const GOLD: (&str, &str) = ("#f7b731", "#a87d1f");

/// The dark core inside a living snake's head, as a fraction of one cell.
const HEAD_CORE_RADIUS_RATIO: f64 = 0.38;
const HEAD_CORE_COLOR: &str = "#333";
/// How far the head's brightening reaches back along the body, in cells, and
/// how strong it is at the head itself.
const HEAD_GRADIENT_CELLS: f64 = 10.0;
const HEAD_GRADIENT_MAX_OPACITY: f64 = 0.3;

/// The Boost band. Its presence is competitive information — an opponent has to
/// be able to see you are boosting — so conformance requires every skin to
/// paint *something* distinct here, and document-authored skins are pinned to
/// these exact values.
const BOOST_OUTER_COLOR: &str = "#fff200";
const BOOST_OUTER_EXTRA: f64 = 6.0;
const ORDINARY_OUTLINE_EXTRA: f64 = 2.0;

/// The colours one role resolves to.
///
/// Exists for the golden trace's exhaustive palette table, which pins the role
/// resolution against every input combination the old `snake_palette` handled.
/// It reads the baked frame rather than a second copy of the constants, so the
/// table can never describe colours the skin does not paint.
#[cfg(test)]
pub(crate) fn classic_palette(identity: &SkinIdentity) -> (String, String) {
    let colors = ClassicSkin.colors(identity);
    (colors.fill.to_string(), colors.outline.to_string())
}

/// The arena dressing that has always been there: a cool tint and wall on the
/// friendly side, a warm one opposite, and endzone lettering a shade deeper
/// than the zone it sits on so it reads without competing with the snakes.
pub(crate) const CLASSIC_BASE_THEME: BaseTheme<'static> = BaseTheme {
    friendly_zone: "#e6f4fa",
    enemy_zone: "#ffe6e6",
    friendly_wall: "#7aa8c1",
    enemy_wall: "#c18888",
    friendly_text: "#c0d8e4",
    enemy_text: "#e4c0c0",
};

/// The goal celebration as it ships: a ripple in the scoring side's colour,
/// with a deeper readout tint for the number that rides it.
pub(crate) const CLASSIC_CELEBRATION_THEME: CelebrationTheme<'static> = CelebrationTheme {
    effect: "goal-impact-wave",
    friendly_accent: "#5299bb",
    enemy_accent: "#d45454",
    readout_friendly: "#2b6f8c",
    readout_enemy: "#a83232",
};

/// The classic look, expressed as a layer stack.
///
/// Seven layers, in the order the hand-written painter emits them, and the
/// order is the whole point. Only the *tail* disc precedes the head ramp; the
/// head is painted after it, which is what stops the ramp washing the head out.
/// Getting that wrong is the easiest way to produce something that looks nearly
/// right and fails parity.
///
/// Each layer maps onto a lowering that already existed in the painter, so this
/// stack emits the same canvas ops as [`ClassicSkin`] — byte for byte, proven
/// natively by `classic_as_layers_is_byte_identical_to_the_painter` below. That
/// is what lets the golden trace recorded before the skin system existed keep
/// proving what it always proved, instead of being spent on a pixel tolerance.
pub fn classic_layers() -> Vec<Layer> {
    document_layers(
        BOOST_OUTER_EXTRA,
        ORDINARY_OUTLINE_EXTRA,
        (255, 255, 255),
        HEAD_GRADIENT_CELLS,
        HEAD_CORE_RADIUS_RATIO,
    )
}

/// The classic stack, parameterised.
///
/// Every v1 skin document describes a point in exactly this space — two contour
/// widths, a ramp colour and length, a core size — which is why compiling a
/// document into layers reproduces what the fixed painter produced rather than
/// approximating it. The shape is shared rather than copied so the two can
/// never drift.
pub fn document_layers(
    boost_extra: f64,
    outline_extra: f64,
    ramp_rgb: (u8, u8, u8),
    ramp_cells: f64,
    core_ratio: f64,
) -> Vec<Layer> {
    // First-party stacks name their layers with literals, so their ids are
    // borrowed and cost nothing; a compiled document owns its names instead.
    let contour = |id: &'static str, color, extra, boost_only| Layer {
        id: Cow::Borrowed(id),
        region: Region::Contour,
        clip: ClipShape::Silhouette,
        kind: LayerKind::Ribbon {
            color,
            extra,
            joints: true,
            tail_cap: false,
            fill_before_strokes: false,
            refill_before_tail_cap: false,
            single_pass: false,
        },
        transform: LayerTransform::default(),
        boost_only,
        omit_on_single_cell: false,
        opacity: Binding::ONE,
    };
    let head_disc = |id: &'static str, paint, radius_ratio, omit_on_single_cell| Layer {
        id: Cow::Borrowed(id),
        region: Region::Head,
        clip: ClipShape::Silhouette,
        kind: LayerKind::HeadDisc {
            paint,
            radius: Binding::Const(radius_ratio),
        },
        transform: LayerTransform::default(),
        boost_only: false,
        omit_on_single_cell,
        opacity: Binding::ONE,
    };

    vec![
        // The Boost band is outermost, and only exists while boosting. Its
        // presence is competitive information, so nothing may bury it.
        contour("boost-band", ColorSlot::Boost, boost_extra, true),
        contour("outline", ColorSlot::Outline, outline_extra, false),
        // The body itself: a capsule ribbon with rounded joints and a tail cap.
        Layer {
            id: "body".into(),
            region: Region::Body,
            clip: ClipShape::Silhouette,
            kind: LayerKind::Ribbon {
                color: ColorSlot::Fill,
                extra: 0.0,
                joints: true,
                tail_cap: true,
                fill_before_strokes: true,
                refill_before_tail_cap: true,
                single_pass: false,
            },
            transform: LayerTransform::default(),
            boost_only: false,
            omit_on_single_cell: false,
            opacity: Binding::ONE,
        },
        // The brightening behind the head, so a glance tells you which way a
        // snake is travelling. Painted over full cell squares rather than the
        // rounded silhouette — see `ClipShape::Cells`.
        Layer {
            id: "head-ramp".into(),
            region: Region::Body,
            clip: ClipShape::Cells,
            kind: LayerKind::HeadRamp {
                rgb: ramp_rgb,
                length_cells: ramp_cells,
                // The legacy curve: a linear falloff scaled by the frame's
                // `ramp_opacity`, with the configured wave added on top. A
                // compiled v2 document supplies the whole curve instead.
                opacity: None,
            },
            transform: LayerTransform::default(),
            boost_only: false,
            omit_on_single_cell: true,
            opacity: Binding::ONE,
        },
        // The head is re-laid over the ramp, then brightened to the ramp's peak,
        // then cored. A single-cell snake skips the first two: its body disc is
        // already the head.
        head_disc("head-cap", DiscPaint::Slot(ColorSlot::Fill), 0.5, true),
        head_disc("head-highlight", DiscPaint::RampPeak, 0.5, true),
        head_disc(
            "head-core",
            DiscPaint::Slot(ColorSlot::HeadCore),
            core_ratio,
            false,
        ),
    ]
}

/// The one baked frame classic needs. Classic is time-invariant, so its ring
/// has a single step and `anim_ms` can never reach it.
pub fn classic_frame() -> Frame {
    let swatch = |(fill, outline): (&str, &str)| Swatch {
        fill: fill.to_string(),
        outline: outline.to_string(),
        label: crate::render::roster_label_ink(fill).to_string(),
        // A flat skin is its own swatch.
        swatch: fill.to_string(),
        accent: fill.to_string(),
        extra: Vec::new(),
    };
    Frame {
        friendly: [swatch(BLUE[0]), swatch(BLUE[1])],
        enemy: [swatch(RED[0]), swatch(RED[1])],
        free_for_all: [swatch(BLUE[0]), swatch(RED[0]), swatch(STEEL), swatch(GOLD)],
        ramp_opacity: HEAD_GRADIENT_MAX_OPACITY,
        wave_phase_turns: 0.0,
        time_turns: 0.0,
        params: Vec::new(),
    }
}

/// Classic, built on the compositor.
pub fn classic_composite() -> CompositeSkin {
    CompositeSkin::new(
        "classic@1",
        "Classic",
        classic_layers(),
        vec![classic_frame()],
        1.0,
        CompositeConfig {
            boost_color: BOOST_OUTER_COLOR.to_string(),
            head_core_color: HEAD_CORE_COLOR.to_string(),
            head_core_ratio: HEAD_CORE_RADIUS_RATIO,
            head_core_is_dark: true,
            wave: None,
        },
        Some(BaseThemeOwned {
            friendly_zone: CLASSIC_BASE_THEME.friendly_zone.to_string(),
            enemy_zone: CLASSIC_BASE_THEME.enemy_zone.to_string(),
            friendly_wall: CLASSIC_BASE_THEME.friendly_wall.to_string(),
            enemy_wall: CLASSIC_BASE_THEME.enemy_wall.to_string(),
            friendly_text: CLASSIC_BASE_THEME.friendly_text.to_string(),
            enemy_text: CLASSIC_BASE_THEME.enemy_text.to_string(),
        }),
        Some(CelebrationThemeOwned {
            effect: CLASSIC_CELEBRATION_THEME.effect.to_string(),
            friendly_accent: CLASSIC_CELEBRATION_THEME.friendly_accent.to_string(),
            enemy_accent: CLASSIC_CELEBRATION_THEME.enemy_accent.to_string(),
            readout_friendly: CLASSIC_CELEBRATION_THEME.readout_friendly.to_string(),
            readout_enemy: CLASSIC_CELEBRATION_THEME.readout_enemy.to_string(),
        }),
    )
    .expect("the classic layer stack satisfies the regioned frame")
}

/// The compiled classic stack, built once for the process.
///
/// Classic is time-invariant and role-resolved from a single baked frame, so
/// this is a handful of strings and seven layers — cheap enough that the
/// `OnceLock` is about identity rather than cost.
fn classic_engine() -> &'static CompositeSkin {
    static ENGINE: std::sync::OnceLock<CompositeSkin> = std::sync::OnceLock::new();
    ENGINE.get_or_init(classic_composite)
}

/// The look Snaketron ships with.
///
/// Since `specs/skin-shading-prd.md` S6 this is a handle on the compositor
/// rather than a painter of its own: every method below delegates to
/// [`classic_engine`]. The flip changed no canvas ops at all, which is why the
/// golden trace recorded before the skin system existed still passes unchanged
/// — the strongest form of the parity guarantee section 12 asks for, and it
/// did not have to be spent to get it.
#[derive(Clone, Copy, Debug, Default)]
pub struct ClassicSkin;

impl SnakeSkin for ClassicSkin {
    fn id(&self) -> &str {
        "classic@1"
    }

    fn name(&self) -> &str {
        "Classic"
    }

    fn colors(&self, identity: &SkinIdentity) -> SkinColors<'_> {
        classic_engine().colors(identity)
    }

    fn metrics(&self, boost_active: bool) -> SkinMetrics {
        classic_engine().metrics(boost_active)
    }

    fn base_theme(&self) -> Option<BaseTheme<'_>> {
        Some(CLASSIC_BASE_THEME)
    }

    fn celebration_theme(&self) -> Option<CelebrationTheme<'_>> {
        Some(CLASSIC_CELEBRATION_THEME)
    }

    fn paint_alive(
        &self,
        ctx: &mut PaintCtx,
        pose: &SnakePose,
        identity: &SkinIdentity,
    ) -> Result<(), JsValue> {
        classic_engine().paint_alive(ctx, pose, identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ribbon_of(id: &str) -> (ColorSlot, f64) {
        let layers = classic_layers();
        let layer = layers
            .iter()
            .find(|layer| layer.id == id)
            .unwrap_or_else(|| panic!("no layer named {id}"));
        match layer.kind {
            LayerKind::Ribbon { color, extra, .. } => (color, extra),
            _ => panic!("{id} is not a ribbon"),
        }
    }

    #[test]
    fn active_boost_outline_layers_yellow_outside_the_ordinary_contour() {
        let layers = classic_layers();
        let contour: Vec<&str> = layers
            .iter()
            .filter(|layer| layer.region == Region::Contour)
            .map(|layer| layer.id.as_ref())
            .collect();
        assert_eq!(
            contour,
            vec!["boost-band", "outline"],
            "the Boost band must be the outermost contour layer"
        );

        assert_eq!(BOOST_OUTER_COLOR, "#fff200");
        let (boost_color, boost_extra) = ribbon_of("boost-band");
        let (outline_color, outline_extra) = ribbon_of("outline");
        assert_eq!(boost_color, ColorSlot::Boost);
        assert_eq!(outline_color, ColorSlot::Outline);
        // The band shows 2px outside the ordinary contour on every side, which
        // is what an opponent reads.
        assert_eq!(boost_extra / 2.0 - outline_extra / 2.0, 2.0);
        assert_eq!(ClassicSkin.metrics(true).overhang_px, 3.0);
    }

    #[test]
    fn inactive_snake_keeps_the_existing_single_pixel_contour_and_mask() {
        let layers = classic_layers();
        let live: Vec<&str> = layers
            .iter()
            .filter(|layer| layer.region == Region::Contour && layer.applies(false, 5))
            .map(|layer| layer.id.as_ref())
            .collect();
        assert_eq!(live, vec!["outline"], "a calm snake has no Boost band");
        assert_eq!(ribbon_of("outline").1 / 2.0, 1.0);
        assert_eq!(ClassicSkin.metrics(false).overhang_px, 1.0);
    }
}

#[cfg(test)]
mod parity {
    use super::*;
    use crate::skin::SnakeRole;

    /// What proves classic still looks like classic, now that the stroke
    /// painter is gone.
    ///
    /// Through S5 this module compared the compositor against `paint_body` op
    /// for op. That comparison passed at every stage — which is why the flip in
    /// S6 changed nothing, and why `client/src/skin/goldens/classic.trace` is
    /// still byte-for-byte the trace recorded from the *original* renderer,
    /// before the skin system existed. With `paint_body` deleted the trace is
    /// the oracle on its own: it is a complete record of the old lowering, it
    /// runs natively in CI, and re-recording it is the one thing that would
    /// hide a regression. Left here so the next reader knows where the
    /// guarantee lives rather than concluding it was dropped.
    ///
    /// The browser baselines in `client/web/tests/skins` are the same guarantee
    /// for the five skins that never had a trace.
    #[test]
    fn the_golden_trace_is_where_classic_parity_now_lives() {
        let trace = include_str!("goldens/classic.trace");
        assert!(
            trace.contains("## alive"),
            "the golden trace no longer records the living-snake painter"
        );
        // Reported numbers, pinned against the literals players have been
        // looking at for years rather than against another copy of the code.
        assert_eq!(ClassicSkin.metrics(false).overhang_px, 1.0);
        assert_eq!(ClassicSkin.metrics(true).overhang_px, 3.0);
        assert_eq!(
            ClassicSkin.metrics(false).head_core_radius_ratio,
            HEAD_CORE_RADIUS_RATIO
        );
        assert!(ClassicSkin.metrics(false).head_core_is_dark);

        let own = ClassicSkin.colors(&SkinIdentity {
            role: SnakeRole::Own,
            shade_slot: 0,
        });
        assert_eq!((own.fill, own.outline), BLUE[0]);
        let enemy = ClassicSkin.colors(&SkinIdentity {
            role: SnakeRole::Enemy,
            shade_slot: 1,
        });
        assert_eq!((enemy.fill, enemy.outline), RED[1]);
    }
}
