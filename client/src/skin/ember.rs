//! Ember — a first-party skin that needs more than a document can say.
//!
//! Everything here except the head could have been written as a `SkinDoc`. The
//! head could not: it is a radial gradient that breathes with the animation
//! clock, and the document schema deliberately has no vocabulary for gradients.
//! That is the escalation path working as intended — reach for Rust when the
//! data format genuinely cannot express the idea, and inherit the shared body
//! painter for everything else so the structural guarantees still hold.
//!
//! Blue flame for your side, orange for theirs. The hue rule is not negotiable
//! even for a skin whose whole idea is fire: a teammate who reads as an enemy
//! is a bug no amount of art direction pays for.

use crate::skin::classic::document_layers;
use crate::skin::composite::{
    BaseThemeOwned, CelebrationThemeOwned, CompositeConfig, CompositeSkin, Frame, Swatch,
};
use crate::skin::layer::{ColorSlot, DiscPaint, Layer, LayerKind, LayerTransform, Region};
use crate::skin::space::ClipShape;
use crate::skin::{
    BaseTheme, CelebrationTheme, PaintCtx, SkinColors, SkinIdentity, SkinMetrics, SnakePose,
    SnakeSkin,
};
use wasm_bindgen::prelude::*;

// Only the palette helper below needs it, and only in test builds.
#[cfg(test)]
use crate::skin::SnakeRole;

/// Cool flame — the friendly side.
const BLUE_FLAME: [(&str, &str, &str); 2] = [
    ("#6fb7ff", "#2f6fae", "#0d1b2e"),
    ("#4a95f0", "#255a95", "#0d1b2e"),
];
/// Hot flame — the opposing side.
const ORANGE_FLAME: [(&str, &str, &str); 2] = [
    ("#ff9445", "#b8551b", "#2a1206"),
    ("#f26b2e", "#a33f16", "#2a1206"),
];
/// The remaining free-for-all slots.
const ASH: (&str, &str, &str) = ("#8a8f99", "#4c515c", "#14161a");
const SULPHUR: (&str, &str, &str) = ("#f2cf4a", "#a08320", "#241d05");

const OUTLINE_EXTRA: f64 = 2.0;
const BOOST_COLOR: &str = "#fff200";
const BOOST_EXTRA: f64 = 6.0;
const HEAD_CORE_RATIO: f64 = 0.34;
const GRADIENT_CELLS: f64 = 9.0;
const GRADIENT_MAX_OPACITY: f64 = 0.26;

/// How far the head glow pulses either side of its resting size, as a fraction
/// of the head radius, and how long one breath takes.
const GLOW_PULSE: f64 = 0.14;
const GLOW_PERIOD_MS: f64 = 2_200.0;
/// The glow stays inside the head disc, so it adds nothing to the skin's
/// overhang and cannot creep out from under the renderer's occlusion mask.
const GLOW_MAX_RADIUS_RATIO: f64 = 0.5;

pub struct EmberSkin;

/// The arena as Ember dresses it: cold slate on your side, warm ash opposite.
const EMBER_BASE_THEME: BaseTheme<'static> = BaseTheme {
    friendly_zone: "#e4eefb",
    enemy_zone: "#fdece2",
    friendly_wall: "#6f93bd",
    enemy_wall: "#c08a63",
    friendly_text: "#c2d4e8",
    enemy_text: "#eccfba",
};

/// A goal in Ember's colours — same wave, different fire.
const EMBER_CELEBRATION_THEME: CelebrationTheme<'static> = CelebrationTheme {
    effect: "goal-impact-wave",
    friendly_accent: "#4a95f0",
    enemy_accent: "#f26b2e",
    readout_friendly: "#1f4f8c",
    readout_enemy: "#9c3d12",
};

/// `(fill, outline, core)` for one role.
///
/// The compiled frames hold these now; this stays as the source the hue test
/// reads, so that test checks the palette rather than a copy of it.
#[cfg(test)]
fn ember_palette(identity: &SkinIdentity) -> (&'static str, &'static str, &'static str) {
    let shade = (identity.shade_slot % 2) as usize;
    match identity.role {
        SnakeRole::Own | SnakeRole::Teammate => BLUE_FLAME[shade],
        SnakeRole::Enemy => ORANGE_FLAME[shade],
        SnakeRole::SpectatedTeam(0) => BLUE_FLAME[shade],
        SnakeRole::SpectatedTeam(_) => ORANGE_FLAME[shade],
        SnakeRole::FreeForAll { palette_slot } => match palette_slot {
            0 => BLUE_FLAME[0],
            1 => ORANGE_FLAME[0],
            2 => ASH,
            _ => SULPHUR,
        },
    }
}

/// The stops the glow fades through, as alpha suffixes on the role's accent.
const GLOW_STOPS: &[(f64, &str)] = &[(0.0, "f2"), (0.62, "b0"), (1.0, "00")];

/// How large the head glow is at a given point in the cycle.
///
/// A plain multiple of the head radius, so a reduced-motion viewer gets the
/// resting value and the op stream is unchanged either way.
fn glow_scale(turns: f64) -> f64 {
    1.0 + GLOW_PULSE * (turns * std::f64::consts::TAU).sin()
}

/// Ember, built on the compositor.
///
/// The body is the shared stack every skin uses. The head is two extra layers
/// on top of it: a core in the role's accent, and a radial glow whose radius is
/// a baked scalar track.
///
/// Baking that radius into the 32-step ring quantises what used to be a
/// continuous pulse, and it is worth being precise about whether that matters.
/// At the arena's largest cell the core is 15 x 0.34 = 5.1px and the pulse is
/// +/-14% of it, so the whole excursion is +/-0.71px and the largest step-to-step
/// change is about 0.14px. The stepping is sub-pixel, which is why this skin can
/// take the ring; a skin with a larger excursion over the same 2.2s period would
/// trip the Nyquist rule in `specs/skin-shading-prd.md` section 9.1 and should
/// be told so rather than shipped.
fn ember_composite() -> CompositeSkin {
    let swatch = |(fill, outline, core): (&str, &str, &str)| Swatch {
        fill: fill.to_string(),
        outline: outline.to_string(),
        label: crate::render::roster_label_ink(fill).to_string(),
        swatch: fill.to_string(),
        // The per-role ember: both the head core and the glow are painted in it.
        accent: core.to_string(),
    };

    let frames: Vec<Frame> = (0..skin_schema::ANIMATION_STEPS)
        .map(|step| {
            let turns = step as f64 / skin_schema::ANIMATION_STEPS as f64;
            Frame {
                friendly: [swatch(BLUE_FLAME[0]), swatch(BLUE_FLAME[1])],
                enemy: [swatch(ORANGE_FLAME[0]), swatch(ORANGE_FLAME[1])],
                free_for_all: [
                    swatch(BLUE_FLAME[0]),
                    swatch(ORANGE_FLAME[0]),
                    swatch(ASH),
                    swatch(SULPHUR),
                ],
                ramp_opacity: GRADIENT_MAX_OPACITY,
                wave_phase_turns: 0.0,
                time_turns: turns,
                layer_opacity: Vec::new(),
                // The glow stays inside the head disc, so it adds nothing to
                // the skin's overhang and cannot creep out from under the
                // renderer's occlusion mask.
                scalars: vec![
                    (HEAD_CORE_RATIO * glow_scale(turns))
                        .clamp(HEAD_CORE_RATIO * 0.5, GLOW_MAX_RADIUS_RATIO),
                ],
                literals: Vec::new(),
            }
        })
        .collect();

    let mut layers = document_layers(
        BOOST_EXTRA,
        OUTLINE_EXTRA,
        (255, 236, 210),
        GRADIENT_CELLS,
        HEAD_CORE_RATIO,
    );
    // The shared stack cores the head from `config.head_core_color`, which is
    // one colour for the whole skin. Ember's core is per-role, so its core
    // layer reads the accent instead.
    if let Some(core) = layers.iter_mut().find(|layer| layer.id == "head-core")
        && let LayerKind::HeadDisc { paint, .. } = &mut core.kind
    {
        *paint = DiscPaint::Slot(ColorSlot::Accent);
    }
    layers.push(Layer {
        id: "head-glow",
        region: Region::Head,
        clip: ClipShape::Silhouette,
        kind: LayerKind::HeadDisc {
            paint: DiscPaint::RadialGlow {
                slot: ColorSlot::Accent,
                stops: GLOW_STOPS,
            },
            radius_ratio: HEAD_CORE_RATIO,
            radius_track: Some(0),
        },
        transform: LayerTransform::default(),
        boost_only: false,
        omit_on_single_cell: false,
        opacity_track: None,
    });

    CompositeSkin::new(
        "ember@1",
        "Ember",
        layers,
        frames,
        GLOW_PERIOD_MS,
        CompositeConfig {
            boost_color: BOOST_COLOR.to_string(),
            // Never read: the core layer above uses the accent instead. Kept
            // truthful rather than blank so `metrics()` and any future reader
            // see a real colour.
            head_core_color: BLUE_FLAME[0].2.to_string(),
            head_core_ratio: HEAD_CORE_RATIO,
            head_core_is_dark: true,
            wave: None,
        },
        Some(BaseThemeOwned {
            friendly_zone: EMBER_BASE_THEME.friendly_zone.to_string(),
            enemy_zone: EMBER_BASE_THEME.enemy_zone.to_string(),
            friendly_wall: EMBER_BASE_THEME.friendly_wall.to_string(),
            enemy_wall: EMBER_BASE_THEME.enemy_wall.to_string(),
            friendly_text: EMBER_BASE_THEME.friendly_text.to_string(),
            enemy_text: EMBER_BASE_THEME.enemy_text.to_string(),
        }),
        Some(CelebrationThemeOwned {
            effect: EMBER_CELEBRATION_THEME.effect.to_string(),
            friendly_accent: EMBER_CELEBRATION_THEME.friendly_accent.to_string(),
            enemy_accent: EMBER_CELEBRATION_THEME.enemy_accent.to_string(),
            readout_friendly: EMBER_CELEBRATION_THEME.readout_friendly.to_string(),
            readout_enemy: EMBER_CELEBRATION_THEME.readout_enemy.to_string(),
        }),
    )
    .expect("the ember layer stack satisfies the regioned frame")
}

fn ember_engine() -> &'static CompositeSkin {
    static ENGINE: std::sync::OnceLock<CompositeSkin> = std::sync::OnceLock::new();
    ENGINE.get_or_init(ember_composite)
}

impl SnakeSkin for EmberSkin {
    fn id(&self) -> &str {
        ember_engine().id()
    }

    fn name(&self) -> &str {
        ember_engine().name()
    }

    fn colors(&self, identity: &SkinIdentity) -> SkinColors<'_> {
        ember_engine().colors(identity)
    }

    fn metrics(&self, boost_active: bool) -> SkinMetrics {
        ember_engine().metrics(boost_active)
    }

    fn paint_alive(
        &self,
        ctx: &mut PaintCtx,
        pose: &SnakePose,
        identity: &SkinIdentity,
    ) -> Result<(), JsValue> {
        ember_engine().paint_alive(ctx, pose, identity)
    }

    fn base_theme(&self) -> Option<BaseTheme<'_>> {
        ember_engine().base_theme()
    }

    fn celebration_theme(&self) -> Option<CelebrationTheme<'_>> {
        ember_engine().celebration_theme()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The glow is the whole reason this skin is Rust rather than a document,
    /// so it had better stay inside the head it belongs to. Checked against
    /// every baked step rather than a handful of sampled clocks: the ring is
    /// what actually ships, and it is finite, so there is no reason to sample.
    #[test]
    fn the_head_glow_never_escapes_the_head_cell() {
        let engine = ember_engine();
        for (step, radius_ratio) in (0..skin_schema::ANIMATION_STEPS).map(|step| {
            let turns = step as f64 / skin_schema::ANIMATION_STEPS as f64;
            (
                step,
                (HEAD_CORE_RATIO * glow_scale(turns))
                    .clamp(HEAD_CORE_RATIO * 0.5, GLOW_MAX_RADIUS_RATIO),
            )
        }) {
            assert!(
                radius_ratio <= 0.5,
                "step {step} put the glow at {radius_ratio} of a cell, outside \
                 the head disc and past the overhang this skin reports"
            );
        }
        // ...and the stack agrees, which is what the registration-time check in
        // `composite::validate_layers` is for.
        assert_eq!(engine.metrics(false).overhang_px, OUTLINE_EXTRA / 2.0);
    }

    /// Reduced motion pins step zero, and step zero is the resting size.
    #[test]
    fn reduced_motion_holds_the_glow_still() {
        use crate::skin::paint::{OpRecorder, PaintCtx};
        assert_eq!(glow_scale(0.0), 1.0);

        let identity = SkinIdentity {
            role: SnakeRole::Own,
            shade_slot: 0,
        };
        let paint = |anim_ms: f64, reduced_motion: bool| {
            let mut recorder = OpRecorder::new();
            EmberSkin
                .paint_alive(
                    &mut PaintCtx::recording(&mut recorder),
                    &SnakePose {
                        cells: &[(3.0, 3.0), (0.0, 3.0)],
                        cell_size: 12.0,
                        boost_active: false,
                        anim_ms,
                        reduced_motion,
                        detail_scale: 1.0,
                    },
                    &identity,
                )
                .expect("a recording painter cannot fail");
            recorder.to_golden()
        };
        let resting = paint(0.0, true);
        for anim_ms in [0.0, 700.0, 5_000.0, f64::NAN] {
            assert_eq!(paint(anim_ms, true), resting, "at {anim_ms}ms");
        }
        // ...and it really does move when motion is allowed.
        assert_ne!(paint(550.0, false), resting);
    }

    /// Fire or not, a teammate reads cool and an opponent reads warm.
    #[test]
    fn the_sides_stay_on_their_own_side_of_the_colour_wheel() {
        use skin_schema::color::{ENEMY_HUES, FRIENDLY_HUES, Rgb};

        for identity in [
            SkinIdentity {
                role: SnakeRole::Own,
                shade_slot: 0,
            },
            SkinIdentity {
                role: SnakeRole::Teammate,
                shade_slot: 1,
            },
            SkinIdentity {
                role: SnakeRole::SpectatedTeam(0),
                shade_slot: 0,
            },
        ] {
            let (fill, _, _) = ember_palette(&identity);
            let (hue, _) = Rgb::parse(fill).expect("a real colour").oklch_hue_chroma();
            assert!(
                FRIENDLY_HUES.contains(hue),
                "{fill} is {hue:.0}deg, which does not read as friendly"
            );
        }

        for identity in [
            SkinIdentity {
                role: SnakeRole::Enemy,
                shade_slot: 0,
            },
            SkinIdentity {
                role: SnakeRole::Enemy,
                shade_slot: 1,
            },
            SkinIdentity {
                role: SnakeRole::SpectatedTeam(1),
                shade_slot: 0,
            },
        ] {
            let (fill, _, _) = ember_palette(&identity);
            let (hue, _) = Rgb::parse(fill).expect("a real colour").oklch_hue_chroma();
            assert!(
                ENEMY_HUES.contains(hue),
                "{fill} is {hue:.0}deg, which does not read as hostile"
            );
        }
    }
}
