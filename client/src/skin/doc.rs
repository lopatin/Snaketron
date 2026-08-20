//! Skins authored as data.
//!
//! A [`SkinDoc`] is compiled once, at registration, into a ring of fully
//! resolved palettes — one per animation step. Painting a frame is then an
//! index and a handful of borrows, with no colour maths, no formatting, and no
//! allocation on the hot path. It also means the validator can check every
//! frame a viewer could ever see, because they all exist as data before the
//! skin is ever painted.
//!
//! The document layer is kept honest by a single test: the classic look
//! expressed as a document has to paint exactly what the hand-written classic
//! skin paints. If the interpreter and the reference ever disagree, the
//! interpreter is wrong.

use crate::skin::classic::document_layers;
use crate::skin::composite::{
    BaseThemeOwned, CelebrationThemeOwned, CompositeConfig, CompositeSkin, Frame, Swatch,
};
use crate::skin::{
    BaseTheme, CelebrationTheme, PaintCtx, SkinColors, SkinIdentity, SkinMetrics, SnakePose,
    SnakeSkin,
};
use skin_schema::color::Rgb;
use skin_schema::{
    ANIMATION_STEPS, AnimationSpec, SkinDoc, SkinDocError, TrackTarget, animation_offset,
    derive_label_ink, shift_lightness,
};
use wasm_bindgen::prelude::*;

/// A skin built from a document.
///
/// Since `specs/skin-shading-prd.md` S9 this is a compiled [`CompositeSkin`]
/// rather than a parameter bundle handed to a fixed painter. The document
/// schema did not have to change to get there: a v1 document describes exactly
/// the layer stack classic uses, so compiling one into layers produces the same
/// canvas ops it always produced. That is the whole reason the four shipped
/// documents could move without a single pixel of review — proven natively by
/// the golden trace for classic, and in the browser by the baselines in
/// `client/web/tests/skins` for the other four.
pub struct ParamSkin {
    engine: CompositeSkin,
}

impl ParamSkin {
    /// Validate and compile a document.
    pub fn compile(doc: &SkinDoc) -> Result<Self, Vec<SkinDocError>> {
        skin_schema::validate(doc)?;

        let animation = doc.animation.as_ref();
        let steps = if animation.is_some() {
            ANIMATION_STEPS
        } else {
            1
        };

        let gradient_color = Rgb::parse(&doc.head.gradient.color).expect("validated");
        let gradient_rgb = (
            (gradient_color.r * 255.0).round() as u8,
            (gradient_color.g * 255.0).round() as u8,
            (gradient_color.b * 255.0).round() as u8,
        );

        let resolve = |pair: &skin_schema::ColorPair, step: usize| -> Swatch {
            let fill = Rgb::parse(&pair.fill).expect("validated");
            let outline = Rgb::parse(&pair.outline).expect("validated");
            let fill = shift_lightness(
                fill,
                animation_offset(animation, TrackTarget::BodyLightness, step),
            );
            let outline = shift_lightness(
                outline,
                animation_offset(animation, TrackTarget::OutlineLightness, step),
            );
            let label = doc
                .labels
                .ink
                .clone()
                .unwrap_or_else(|| derive_label_ink(fill).to_hex());
            let swatch = doc.labels.swatch.clone().unwrap_or_else(|| fill.to_hex());
            Swatch {
                fill: fill.to_hex(),
                outline: outline.to_hex(),
                label,
                // A document skin has no signature element beyond its body, so
                // its accent is its fill.
                accent: fill.to_hex(),
                swatch,
            }
        };

        let wave = animation.and_then(|spec| spec.wave.clone());
        let frames: Vec<Frame> = (0..steps)
            .map(|step| Frame {
                friendly: [
                    resolve(&doc.palette.friendly[0], step),
                    resolve(&doc.palette.friendly[1], step),
                ],
                enemy: [
                    resolve(&doc.palette.enemy[0], step),
                    resolve(&doc.palette.enemy[1], step),
                ],
                free_for_all: [
                    resolve(&doc.palette.free_for_all[0], step),
                    resolve(&doc.palette.free_for_all[1], step),
                    resolve(&doc.palette.free_for_all[2], step),
                    resolve(&doc.palette.free_for_all[3], step),
                ],
                ramp_opacity: (doc.head.gradient.max_opacity
                    + animation_offset(animation, TrackTarget::GradientOpacity, step))
                .clamp(0.0, 1.0),
                // The wave advances with the compiled step, so it moves at the
                // document's own period with no per-frame maths.
                wave_phase_turns: wave
                    .as_ref()
                    .map(|wave| wave.crests_per_cycle * step as f64 / steps.max(1) as f64)
                    .unwrap_or(0.0),
                time_turns: step as f64 / steps.max(1) as f64,
                layer_opacity: Vec::new(),
                scalars: Vec::new(),
                literals: Vec::new(),
            })
            .collect();

        let core = Rgb::parse(&doc.head.core_color).expect("validated");
        let engine = CompositeSkin::new(
            doc.id.clone(),
            doc.name.clone(),
            document_layers(
                doc.outline.boost_band.extra_px,
                doc.outline.extra_px,
                gradient_rgb,
                doc.head.gradient.length_cells,
                doc.head.core_ratio,
            ),
            frames,
            animation.map_or(1.0, |spec: &AnimationSpec| spec.period_ms),
            CompositeConfig {
                boost_color: doc.outline.boost_band.color.clone(),
                head_core_color: doc.head.core_color.clone(),
                head_core_ratio: doc.head.core_ratio,
                head_core_is_dark: core.relative_luminance() < 0.35,
                wave: wave
                    .as_ref()
                    .map(|wave| (wave.cells_per_crest, wave.amplitude)),
            },
            doc.base.clone().map(|base| BaseThemeOwned {
                friendly_zone: base.friendly_zone,
                enemy_zone: base.enemy_zone,
                friendly_wall: base.friendly_wall,
                enemy_wall: base.enemy_wall,
                friendly_text: base.friendly_text,
                enemy_text: base.enemy_text,
            }),
            doc.celebration.clone().map(|c| CelebrationThemeOwned {
                effect: c.effect,
                friendly_accent: c.friendly_accent,
                enemy_accent: c.enemy_accent,
                readout_friendly: c.readout_friendly,
                readout_enemy: c.readout_enemy,
            }),
        )
        .map_err(|problems| {
            problems
                .into_iter()
                .map(|problem| SkinDocError {
                    field: format!("layers.{}", problem.layer),
                    problem: problem.problem,
                })
                .collect::<Vec<_>>()
        })?;

        Ok(Self { engine })
    }

    /// Compile from JSON.
    pub fn from_json(json: &str) -> Result<Self, Vec<SkinDocError>> {
        let doc: SkinDoc = serde_json::from_str(json).map_err(|error| {
            vec![SkinDocError {
                field: "document".to_string(),
                problem: error.to_string(),
            }]
        })?;
        Self::compile(&doc)
    }
}

impl SnakeSkin for ParamSkin {
    fn id(&self) -> &str {
        self.engine.id()
    }

    fn name(&self) -> &str {
        self.engine.name()
    }

    fn colors(&self, identity: &SkinIdentity) -> SkinColors<'_> {
        self.engine.colors(identity)
    }

    fn metrics(&self, boost_active: bool) -> SkinMetrics {
        self.engine.metrics(boost_active)
    }

    fn paint_alive(
        &self,
        ctx: &mut PaintCtx,
        pose: &SnakePose,
        identity: &SkinIdentity,
    ) -> Result<(), JsValue> {
        self.engine.paint_alive(ctx, pose, identity)
    }

    fn base_theme(&self) -> Option<BaseTheme<'_>> {
        self.engine.base_theme()
    }

    fn celebration_theme(&self) -> Option<CelebrationTheme<'_>> {
        self.engine.celebration_theme()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classic_doc() -> ParamSkin {
        ParamSkin::from_json(include_str!("../../../skin-schema/skins/classic.skin.json"))
            .expect("the shipped classic document compiles")
    }

    /// A still document compiles to a stack whose layers exist, in the order
    /// the painter emitted them. The byte-for-byte check against the reference
    /// painter lives in `skin::classic`; this is the structural one.
    #[test]
    fn a_document_compiles_to_the_classic_layer_shape() {
        let skin = classic_doc();
        let ids: Vec<&str> = skin.engine.layers().iter().map(|layer| layer.id).collect();
        assert_eq!(
            ids,
            vec![
                "boost-band",
                "outline",
                "body",
                "head-ramp",
                "head-cap",
                "head-highlight",
                "head-core",
            ]
        );
    }

    /// Absurd clocks and reduced motion both land on the resting pose, which
    /// is what makes a roster glyph and a golden trace reproducible.
    #[test]
    fn reduced_motion_and_absurd_clocks_land_on_the_resting_pose() {
        use crate::skin::paint::{OpRecorder, PaintCtx};
        let skin = classic_doc();
        let identity = SkinIdentity {
            role: crate::skin::SnakeRole::Own,
            shade_slot: 0,
        };
        let paint = |anim_ms: f64, reduced_motion: bool| {
            let mut recorder = OpRecorder::new();
            skin.paint_alive(
                &mut PaintCtx::recording(&mut recorder),
                &SnakePose {
                    cells: &[(3.0, 3.0), (0.0, 3.0)],
                    cell_size: 10.0,
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
        assert_eq!(paint(12_345.0, true), resting);
        assert_eq!(paint(f64::NAN, false), resting);
        assert_eq!(paint(-500.0, false), resting);
    }
}
