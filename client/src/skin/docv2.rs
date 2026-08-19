//! Compiling a v2 layer document into the compositor.
//!
//! `specs/skin-layer-documents-prd.md`. Where `skin::doc` compiles a v1
//! document into *the* layer stack — classic's seven layers, parameterised —
//! this compiles a document that names its own layers. The renderer did not
//! have to change to allow it: v2's vocabulary is the compositor's vocabulary,
//! which is the whole thesis of that PRD.
//!
//! Three things happen here that are worth finding quickly:
//!
//! - **Expressions become [`Binding`]s**, placed by what they read rather than
//!   by what an author declares. Reads nothing, fold it. Reads only the clock,
//!   bake 32 values. Reads the snake, evaluate per snake-frame. An author never
//!   states a tier and so can never state it wrongly.
//! - **The system layers are inserted here**, not authored: the Boost band
//!   outermost and the head core topmost. Their *positions* are competitive
//!   information, so a document may not hold them.
//! - **Classic's parity knobs are derived**, not exposed. They exist for
//!   byte-exact classic parity and mean nothing to an author.

use crate::skin::composite::{
    BaseThemeOwned, CelebrationThemeOwned, CompositeConfig, CompositeSkin, Frame, Swatch,
};
use crate::skin::layer::{
    Binding, ColorSlot, DiscPaint, Fade, Fit, Layer, LayerKind, LayerTransform, Region, Source,
    Span, Stop,
};
use crate::skin::space::{ClipShape, CornerPolicy};
use crate::skin::{
    BaseTheme, CelebrationTheme, PaintCtx, SkinColors, SkinIdentity, SkinMetrics, SnakePose,
    SnakeSkin,
};
use skin_schema::color::Rgb;
use skin_schema::expr::{Env, Expr, Input};
use skin_schema::v2::{
    AnchorV2, ClipV2, ColorRef, ColorTarget, CornerV2, DiscPaintName, DiscPaintV2, FitV2,
    GradientAxis, LayerBodyV2, LayerV2, PropExpr, RegionV2, SkinDocV2, SlotName, SourceV2,
    validate_v2,
};
use skin_schema::{ANIMATION_STEPS, ColorPair, SkinDocError, derive_label_ink, shift_lightness};
use std::sync::Arc;
use wasm_bindgen::prelude::*;

/// A skin built from a v2 layer document.
pub struct LayerSkin {
    engine: CompositeSkin,
}

/// One layer with its groups flattened away.
struct Flat<'a> {
    name: String,
    body: &'a LayerBodyV2,
    /// The layer's own opacity multiplied by every group it sits inside.
    opacity: Expr,
    transform: &'a skin_schema::v2::TransformV2,
    boost_only: bool,
    omit_on_single_cell: bool,
}

/// Places expressions by what they read, and collects the per-step table.
///
/// The placement rule is the one thing in this file that has to be right, so
/// it is stated once, here, rather than repeated at each property:
///
/// | Reads | Where it goes | Cost |
/// | --- | --- | --- |
/// | nothing | folded at compile time | none |
/// | `len`, `boost` or `seed` | evaluated per snake-frame | one eval |
/// | only `time` | baked into 32 values | one lookup |
///
/// The middle row is the one a tier-based reading gets wrong: `boost` is
/// *constant-tier* — it cannot change within a snake-frame — and folding it
/// anyway would freeze every boost-reactive layer at "not boosting", with
/// nothing to report the mistake.
struct Baker {
    steps: usize,
    /// One expression per allocated parameter slot, evaluated per step later.
    per_step: Vec<Expr>,
}

impl Baker {
    fn bind(&mut self, expr: &Expr) -> Binding {
        let inputs = expr.inputs();
        if inputs.is_empty() {
            return Binding::Const(expr.eval(&Env::default()));
        }
        if inputs.contains(Input::Len)
            || inputs.contains(Input::Boost)
            || inputs.contains(Input::Seed)
        {
            return Binding::Snake(Arc::new(expr.clone()));
        }
        self.per_step.push(expr.clone());
        Binding::Param(self.per_step.len() - 1)
    }

    /// Values for one step of the ring.
    fn params_at(&self, step: usize) -> Vec<f64> {
        let time = step as f64 / self.steps as f64;
        self.per_step
            .iter()
            .map(|expr| {
                expr.eval(&Env {
                    time,
                    ..Env::default()
                })
            })
            .collect()
    }
}

/// Interns the colour references that need a per-role slot in [`Swatch::extra`].
///
/// A plain slot reference resolves through the swatch's own fields and needs no
/// slot; anything *derived* — a lightened fill, a named literal — does, because
/// it is a different colour per role and per step.
#[derive(Default)]
struct Palette {
    extra: Vec<ColorRef>,
}

impl Palette {
    fn slot_for(&mut self, reference: &ColorRef) -> ColorSlot {
        if reference.lighten.is_none()
            && let ColorTarget::Slot { slot } = &reference.target
        {
            return match slot {
                SlotName::Fill => ColorSlot::Fill,
                SlotName::Outline => ColorSlot::Outline,
                SlotName::Accent => ColorSlot::Accent,
                SlotName::HeadCore => ColorSlot::HeadCore,
            };
        }
        if let Some(index) = self.extra.iter().position(|held| held == reference) {
            return ColorSlot::Literal(index);
        }
        self.extra.push(reference.clone());
        ColorSlot::Literal(self.extra.len() - 1)
    }
}

fn parse_or_zero(expr: &PropExpr) -> Expr {
    // Validation has already run and reported anything unparseable, so this is
    // only reached for a document that passed. A zero keeps the compiler total
    // rather than making a cosmetic able to panic.
    expr.parse().unwrap_or(Expr::Const(0.0))
}

fn multiply(left: Expr, right: &Expr) -> Expr {
    use skin_schema::expr::BinOp;
    Expr::Bin(BinOp::Mul, Box::new(left), Box::new(right.clone()))
}

/// Flatten groups, multiplying each group's opacity into its children.
fn flatten<'a>(layers: &'a [LayerV2], inherited: Option<&Expr>, into: &mut Vec<Flat<'a>>) {
    for layer in layers {
        let own = parse_or_zero(&layer.opacity);
        let opacity = match inherited {
            Some(outer) => multiply(outer.clone(), &own),
            None => own,
        };
        match &layer.body {
            LayerBodyV2::Group { layers: children } => {
                flatten(children, Some(&opacity), into);
            }
            body => into.push(Flat {
                name: layer.name.clone(),
                body,
                opacity,
                transform: &layer.transform,
                boost_only: layer.boost_only,
                omit_on_single_cell: layer.omit_on_single_cell,
            }),
        }
    }
}

fn region_of(region: RegionV2) -> Region {
    match region {
        RegionV2::Contour => Region::Contour,
        RegionV2::Body => Region::Body,
        RegionV2::Head => Region::Head,
    }
}

fn clip_of(clip: ClipV2) -> ClipShape {
    match clip {
        ClipV2::Silhouette => ClipShape::Silhouette,
        ClipV2::Cells => ClipShape::Cells,
    }
}

fn corner_of(corner: CornerV2) -> CornerPolicy {
    match corner {
        CornerV2::Fan => CornerPolicy::Own,
        CornerV2::Bisector => CornerPolicy::Bisector,
    }
}

fn span_of(span: &skin_schema::v2::SpanV2) -> Span {
    Span {
        from: match span.from {
            AnchorV2::Whole => crate::skin::layer::Anchor::Whole,
            AnchorV2::Head => crate::skin::layer::Anchor::Head,
            AnchorV2::Tail => crate::skin::layer::Anchor::Tail,
            AnchorV2::At { at } => crate::skin::layer::Anchor::At(at),
            AnchorV2::Fraction { fraction } => crate::skin::layer::Anchor::Fraction(fraction),
        },
        natural: span.natural,
        min: span.min,
        priority: span.priority,
    }
}

fn fit_of(fit: &FitV2) -> Fit {
    match fit {
        FitV2::Clip => Fit::Clip,
        FitV2::Stretch => Fit::Stretch,
        FitV2::Tile { cells_per_repeat } => Fit::Tile {
            cells_per_repeat: *cells_per_repeat,
        },
        FitV2::Cutout { cells_tall } => Fit::Cutout {
            cells_tall: *cells_tall,
        },
    }
}

impl LayerSkin {
    /// Validate and compile a v2 document.
    pub fn compile(doc: &SkinDocV2) -> Result<Self, Vec<SkinDocError>> {
        validate_v2(doc)?;

        let mut flat = Vec::new();
        flatten(&doc.layers, None, &mut flat);

        // One cycle's worth of steps only when something actually reads the
        // clock. A still document bakes a single frame, which is what keeps a
        // converted v1 skin exactly as cheap as it was.
        let animates = flat.iter().any(|layer| {
            layer.opacity.inputs().contains(Input::Time)
                || layer
                    .transform
                    .fields()
                    .iter()
                    .any(|(_, expr)| parse_or_zero(expr).inputs().contains(Input::Time))
                || body_reads_clock(layer.body)
        }) || doc.layers.iter().any(document_color_animates);
        let steps = if animates { ANIMATION_STEPS } else { 1 };

        let mut baker = Baker {
            steps,
            per_step: Vec::new(),
        };
        let mut palette = Palette::default();
        let mut layers = Vec::with_capacity(flat.len() + 2);

        // The Boost band, outermost and painted only while boosting. Pinned
        // here rather than authored: an opponent has to be able to see that
        // you are boosting, so neither its colour, its width, nor its place in
        // the stack is a style choice.
        layers.push(Layer {
            id: "boost-band".into(),
            region: Region::Contour,
            clip: ClipShape::Silhouette,
            kind: LayerKind::Ribbon {
                color: ColorSlot::Boost,
                extra: skin_schema::REQUIRED_BOOST_EXTRA_PX,
                joints: true,
                tail_cap: false,
                fill_before_strokes: false,
                refill_before_tail_cap: false,
                single_pass: false,
            },
            transform: LayerTransform::default(),
            boost_only: true,
            omit_on_single_cell: false,
            opacity: Binding::ONE,
        });

        for layer in &flat {
            layers.push(compile_layer(layer, &mut baker, &mut palette, doc));
        }

        // The head core, topmost. Its position is not authorable either, and
        // for a reason that buys the author something: because the core is
        // composited last, art may span the head cell freely and the label
        // rules still have a flat disc of a known colour to reason about.
        layers.push(Layer {
            id: "head-core".into(),
            region: Region::Head,
            clip: ClipShape::Silhouette,
            kind: LayerKind::HeadDisc {
                paint: DiscPaint::Slot(ColorSlot::HeadCore),
                radius: Binding::Const(doc.head_core.ratio),
            },
            transform: LayerTransform::default(),
            boost_only: false,
            omit_on_single_cell: false,
            opacity: Binding::ONE,
        });

        let frames: Vec<Frame> = (0..steps)
            .map(|step| bake_frame(doc, &baker, &palette, step, steps))
            .collect();

        let core = Rgb::parse(&doc.head_core.color).expect("validated");
        let engine = CompositeSkin::new(
            doc.id.clone(),
            doc.name.clone(),
            layers,
            frames,
            doc.period_ms,
            CompositeConfig {
                boost_color: skin_schema::REQUIRED_BOOST_COLOR.to_string(),
                head_core_color: doc.head_core.color.clone(),
                head_core_ratio: doc.head_core.ratio,
                head_core_is_dark: core.relative_luminance() < 0.35,
                // A v2 document expresses its glow as one expression, so there
                // is no separate wave for the legacy path to add.
                wave: None,
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

    #[cfg(test)]
    pub(crate) fn layers(&self) -> &[Layer] {
        self.engine.layers()
    }
}

/// Whether a layer's kind-specific properties read the clock.
fn body_reads_clock(body: &LayerBodyV2) -> bool {
    let reads = |expr: &PropExpr| parse_or_zero(expr).inputs().contains(Input::Time);
    match body {
        // Flattened away before this is asked.
        LayerBodyV2::Group { .. } => false,
        LayerBodyV2::Ribbon { color, .. } => color_reads_clock(color),
        LayerBodyV2::HeadRamp { .. } => false,
        LayerBodyV2::HeadDisc {
            paint,
            radius_ratio,
        } => {
            reads(radius_ratio)
                || matches!(paint, DiscPaintV2::Ref(reference) if color_reads_clock(reference))
        }
        LayerBodyV2::Span { source, .. } => match source {
            SourceV2::Solid { color } => color_reads_clock(color),
            SourceV2::Gradient { stops, .. } => stops.iter().any(|stop| {
                reads(&stop.offset) || reads(&stop.alpha) || color_reads_clock(&stop.color)
            }),
            SourceV2::Band {
                color,
                half_width,
                t_center,
                alpha,
                ..
            } => {
                color_reads_clock(color)
                    || reads(half_width)
                    || reads(t_center)
                    || alpha.as_ref().is_some_and(reads)
            }
            SourceV2::Image { drift_cells, .. } => reads(drift_cells),
            SourceV2::Text { color, .. } => color_reads_clock(color),
        },
    }
}

fn color_reads_clock(reference: &ColorRef) -> bool {
    reference
        .lighten
        .as_ref()
        .is_some_and(|expr| parse_or_zero(expr).inputs().contains(Input::Time))
}

/// Whether any colour anywhere under this layer animates.
///
/// Separate from [`body_reads_clock`] because a group's children have to be
/// reached before flattening decides the ring's size.
fn document_color_animates(layer: &LayerV2) -> bool {
    match &layer.body {
        LayerBodyV2::Group { layers } => layers.iter().any(document_color_animates),
        body => body_reads_clock(body),
    }
}

fn compile_layer(
    flat: &Flat<'_>,
    baker: &mut Baker,
    palette: &mut Palette,
    doc: &SkinDocV2,
) -> Layer {
    let mut bind = |expr: &PropExpr| baker.bind(&parse_or_zero(expr));

    let transform = LayerTransform {
        translate: (
            bind(&flat.transform.translate_s),
            bind(&flat.transform.translate_t),
        ),
        scale: (bind(&flat.transform.scale_s), bind(&flat.transform.scale_t)),
        rotate_turns: bind(&flat.transform.rotate_turns),
    };

    let kind = match flat.body {
        // Unreachable: `flatten` never emits one.
        LayerBodyV2::Group { .. } => LayerKind::Ribbon {
            color: ColorSlot::Fill,
            extra: 0.0,
            joints: true,
            tail_cap: false,
            fill_before_strokes: false,
            refill_before_tail_cap: false,
            single_pass: false,
        },
        LayerBodyV2::Ribbon {
            region,
            color,
            extra_px,
            joints,
            tail_cap,
        } => LayerKind::Ribbon {
            color: palette.slot_for(color),
            extra: *extra_px,
            joints: *joints,
            tail_cap: *tail_cap,
            // Not authored, and not arbitrary: these three exist so classic's
            // body ribbon lays its fill down before its strokes and re-fills
            // before the tail cap, which is the emission order the golden
            // trace pins. Deriving them from the region reproduces that order
            // for any document without asking an author to know it exists.
            fill_before_strokes: *region == RegionV2::Body,
            refill_before_tail_cap: *tail_cap,
            single_pass: false,
        },
        LayerBodyV2::HeadRamp {
            color,
            length_cells,
        } => {
            let rgb = Rgb::parse(color).expect("validated");
            LayerKind::HeadRamp {
                rgb: (
                    (rgb.r * 255.0).round() as u8,
                    (rgb.g * 255.0).round() as u8,
                    (rgb.b * 255.0).round() as u8,
                ),
                length_cells: *length_cells,
                // Always an expression for a document: the curve is the
                // author's, falloff included.
                opacity: Some(Arc::new(flat.opacity.clone())),
            }
        }
        LayerBodyV2::HeadDisc {
            paint,
            radius_ratio,
        } => LayerKind::HeadDisc {
            paint: match paint {
                DiscPaintV2::Named(DiscPaintName::RampPeak) => DiscPaint::RampPeak,
                DiscPaintV2::Ref(reference) => DiscPaint::Slot(palette.slot_for(reference)),
            },
            radius: bind(radius_ratio),
        },
        LayerBodyV2::Span {
            clip: _,
            span,
            corner,
            source,
            ..
        } => LayerKind::Span {
            span: span_of(span),
            source: compile_source(source, &mut bind, palette, doc),
            corner: corner_of(*corner),
        },
    };

    Layer {
        id: flat.name.clone().into(),
        region: match flat.body {
            LayerBodyV2::Ribbon { region, .. } | LayerBodyV2::Span { region, .. } => {
                region_of(*region)
            }
            LayerBodyV2::HeadRamp { .. } => Region::Body,
            LayerBodyV2::HeadDisc { .. } | LayerBodyV2::Group { .. } => Region::Head,
        },
        clip: match flat.body {
            LayerBodyV2::Span { clip, .. } => clip_of(*clip),
            // The glow paints whole cell squares, which *is* the cells clip.
            LayerBodyV2::HeadRamp { .. } => ClipShape::Cells,
            _ => ClipShape::Silhouette,
        },
        kind,
        transform,
        boost_only: flat.boost_only,
        omit_on_single_cell: flat.omit_on_single_cell,
        // The glow's opacity is its curve, applied per cell, so applying it a
        // second time as a layer alpha would square it.
        opacity: if matches!(flat.body, LayerBodyV2::HeadRamp { .. }) {
            Binding::ONE
        } else {
            baker.bind(&flat.opacity)
        },
    }
}

fn compile_source(
    source: &SourceV2,
    bind: &mut impl FnMut(&PropExpr) -> Binding,
    palette: &mut Palette,
    doc: &SkinDocV2,
) -> Source {
    match source {
        SourceV2::Solid { color } => Source::Solid(palette.slot_for(color)),
        SourceV2::Gradient { axis, stops } => {
            let stops: Vec<Stop> = stops
                .iter()
                .map(|stop| Stop {
                    offset: bind(&stop.offset),
                    color: palette.slot_for(&stop.color),
                    alpha: bind(&stop.alpha),
                })
                .collect();
            match axis {
                GradientAxis::AlongBody => Source::LinearAlongBody(stops),
                GradientAxis::FromStart => Source::RadialFromStart(stops),
            }
        }
        SourceV2::Band {
            color,
            period_cells,
            duty,
            phase_cells,
            half_width,
            t_center,
            alpha,
        } => Source::Tiled {
            color: palette.slot_for(color),
            period_cells: *period_cells,
            duty: *duty,
            half_width: bind(half_width),
            t_center: bind(t_center),
            phase_cells: *phase_cells,
            alpha: alpha.as_ref().map(|expr| Arc::new(parse_or_zero(expr))),
        },
        SourceV2::Image {
            texture,
            fit,
            fade,
            drift_cells,
        } => Source::Image {
            // Resolved against the document's own declarations, which
            // validation has already checked exist.
            region: doc
                .textures
                .iter()
                .position(|entry| &entry.name == texture)
                .unwrap_or(0),
            fit: fit_of(fit),
            fade: fade.map(|fade| Fade {
                lead_cells: fade.lead_cells,
                trail_cells: fade.trail_cells,
                steps: fade.steps,
            }),
            // Drift is a rate rather than a paint argument, so it is folded
            // rather than bound; the renderer advances it from `time_turns`.
            drift_cells: match bind(drift_cells) {
                Binding::Const(value) => value,
                _ => 0.0,
            },
        },
        // Lowered in V4. Until then a text layer compiles to nothing visible
        // rather than to something wrong.
        SourceV2::Text { color, .. } => Source::Solid(palette.slot_for(color)),
    }
}

/// Resolve one document colour reference for one role at one step.
fn resolve_color(reference: &ColorRef, pair: &ColorPair, doc: &SkinDocV2, time: f64) -> String {
    let base = match &reference.target {
        ColorTarget::Slot { slot } => match slot {
            SlotName::Fill => pair.fill.clone(),
            SlotName::Outline => pair.outline.clone(),
            SlotName::Accent => pair.accent.clone().unwrap_or_else(|| pair.fill.clone()),
            SlotName::HeadCore => doc.head_core.color.clone(),
        },
        ColorTarget::Literal { literal } => doc
            .literals
            .get(literal)
            .cloned()
            .unwrap_or_else(|| "#ff00ff".to_string()),
    };

    let Some(rgb) = Rgb::parse(&base) else {
        return base;
    };
    match &reference.lighten {
        None => rgb.to_hex(),
        Some(expr) => {
            let amount = parse_or_zero(expr).eval(&Env {
                time,
                ..Env::default()
            });
            shift_lightness(rgb, amount.clamp(-1.0, 1.0)).to_hex()
        }
    }
}

fn bake_frame(
    doc: &SkinDocV2,
    baker: &Baker,
    palette: &Palette,
    step: usize,
    steps: usize,
) -> Frame {
    let time = step as f64 / steps as f64;
    let swatch = |pair: &ColorPair| {
        let fill = Rgb::parse(&pair.fill).expect("validated");
        Swatch {
            fill: pair.fill.clone(),
            outline: pair.outline.clone(),
            label: doc
                .labels
                .ink
                .clone()
                .unwrap_or_else(|| derive_label_ink(fill).to_hex()),
            swatch: doc
                .labels
                .swatch
                .clone()
                .unwrap_or_else(|| pair.fill.clone()),
            accent: pair.accent.clone().unwrap_or_else(|| pair.fill.clone()),
            extra: palette
                .extra
                .iter()
                .map(|reference| resolve_color(reference, pair, doc, time))
                .collect(),
        }
    };

    Frame {
        friendly: [
            swatch(&doc.palette.friendly[0]),
            swatch(&doc.palette.friendly[1]),
        ],
        enemy: [swatch(&doc.palette.enemy[0]), swatch(&doc.palette.enemy[1])],
        free_for_all: [
            swatch(&doc.palette.free_for_all[0]),
            swatch(&doc.palette.free_for_all[1]),
            swatch(&doc.palette.free_for_all[2]),
            swatch(&doc.palette.free_for_all[3]),
        ],
        // A v2 glow carries its own curve, so these two exist only for the
        // legacy path and are never read for this skin.
        ramp_opacity: 0.0,
        wave_phase_turns: 0.0,
        time_turns: time,
        params: baker.params_at(step),
    }
}

impl SnakeSkin for LayerSkin {
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

/// Documents that exercise the v2 vocabulary, for the conformance suite.
///
/// Every skin the compositor can produce has to satisfy the same rules, and
/// until v2 skins ship there is nothing in the catalogue that animates a
/// gradient stop, slides a band, or reacts to Boost. These fixtures are how
/// those paths get held to the rules — in particular op-count invariance,
/// which is the property the whole "expressions change arguments, never
/// structure" design rests on.
///
/// Each is built on the converted classic document, so its palette is already
/// conformant and what each fixture adds is the only thing under test.
#[cfg(test)]
pub(crate) fn conformance_fixtures() -> Vec<SkinDocV2> {
    use skin_schema::v2::{SpanV2, StopV2, TransformV2, upgrade};

    let base = || {
        let v1: skin_schema::SkinDoc =
            serde_json::from_str(include_str!("../../../skin-schema/skins/classic.skin.json"))
                .expect("the shipped classic document parses");
        upgrade(&v1)
    };
    let layer = |name: &str, opacity: &str, body: LayerBodyV2| LayerV2 {
        name: name.to_string(),
        boost_only: false,
        omit_on_single_cell: false,
        opacity: PropExpr(opacity.to_string()),
        transform: TransformV2::default(),
        body,
    };
    let body_span = |source: SourceV2| LayerBodyV2::Span {
        region: RegionV2::Body,
        clip: ClipV2::Silhouette,
        span: SpanV2::whole(),
        corner: CornerV2::Fan,
        source,
    };

    // The PRD's headline example: a gleam whose crest travels the body once
    // per cycle, expressed as stop offsets that read the clock.
    let mut shine = base();
    shine.id = "fixture-shine@1".to_string();
    shine.name = "Shine".to_string();
    shine
        .literals
        .insert("gleam".to_string(), "#fff7d6".to_string());
    let stop = |offset: &str, color: ColorRef, alpha: &str| StopV2 {
        offset: PropExpr(offset.to_string()),
        color,
        alpha: PropExpr(alpha.to_string()),
    };
    shine.layers.push(layer(
        "Shine",
        "1",
        body_span(SourceV2::Gradient {
            axis: GradientAxis::AlongBody,
            stops: vec![
                stop("saw(time) - 0.15", ColorRef::slot(SlotName::Fill), "0"),
                stop(
                    "saw(time)",
                    ColorRef {
                        target: ColorTarget::Literal {
                            literal: "gleam".to_string(),
                        },
                        lighten: None,
                    },
                    "0.55",
                ),
                stop("saw(time) + 0.15", ColorRef::slot(SlotName::Fill), "0"),
            ],
        }),
    ));

    // A band that both pulses and slides across the body: the geometry moves,
    // the number of rectangles does not.
    let mut pulse = base();
    pulse.id = "fixture-pulse@1".to_string();
    pulse.name = "Pulse".to_string();
    pulse.palette.friendly[0].accent = Some("#a8e6ff".to_string());
    pulse.layers.push(layer(
        "Pulse band",
        "0.5 + 0.4 * sin(tau * time)",
        body_span(SourceV2::Band {
            color: ColorRef::slot(SlotName::Accent),
            period_cells: 4.0,
            duty: 0.5,
            phase_cells: 0.0,
            half_width: PropExpr("0.15 + 0.05 * sin(tau * time)".to_string()),
            t_center: PropExpr("0.2 * sin(tau * time)".to_string()),
            alpha: Some(PropExpr("0.6 + 0.4 * noise(s, 0)".to_string())),
        }),
    ));

    // Reads the snake rather than the clock, so it exercises the third
    // binding tier — and a still viewer still sees it change with Boost.
    let mut reactive = base();
    reactive.id = "fixture-reactive@1".to_string();
    reactive.name = "Reactive".to_string();
    reactive.layers.push(layer(
        "Boost sheen",
        "mix(0.15, 0.45, boost)",
        body_span(SourceV2::Solid {
            color: ColorRef {
                target: ColorTarget::Slot {
                    slot: SlotName::Fill,
                },
                lighten: Some(PropExpr("0.25".to_string())),
            },
        }),
    ));

    // A group, and a glow whose falloff is not the linear one the renderer
    // used to impose.
    let mut grouped = base();
    grouped.id = "fixture-grouped@1".to_string();
    grouped.name = "Grouped".to_string();
    let cap = grouped.layers.remove(3);
    let highlight = grouped.layers.remove(3);
    grouped.layers.push(layer(
        "Head dressing",
        "0.6",
        LayerBodyV2::Group {
            layers: vec![cap, highlight],
        },
    ));
    if let LayerBodyV2::HeadRamp { .. } = grouped.layers[2].body {
        grouped.layers[2].opacity =
            PropExpr("0.35 * smoothstep(9, 0, s) * (0.85 + 0.15 * sin(tau * time))".to_string());
    }

    vec![shine, pulse, reactive, grouped]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skin::paint::OpRecorder;
    use skin_schema::SkinDoc;
    use skin_schema::v2::upgrade;

    fn classic_v1() -> SkinDoc {
        serde_json::from_str(include_str!("../../../skin-schema/skins/classic.skin.json"))
            .expect("the shipped classic document parses")
    }

    /// Every fixture has to be a document a player could actually have saved.
    #[test]
    fn the_conformance_fixtures_are_valid_documents() {
        for doc in conformance_fixtures() {
            if let Err(errors) = LayerSkin::compile(&doc) {
                panic!("fixture `{}` does not compile: {errors:?}", doc.id);
            }
        }
    }

    fn record(skin: &dyn SnakeSkin, cells: &[(f64, f64)], boost: bool) -> String {
        let mut recorder = OpRecorder::new();
        skin.paint_alive(
            &mut PaintCtx::recording(&mut recorder),
            &SnakePose::still(cells, 15.0, boost),
            &SkinIdentity {
                role: crate::skin::SnakeRole::Own,
                shade_slot: 0,
            },
        )
        .expect("a recording painter cannot fail");
        recorder.to_golden()
    }

    /// **The V0 gate.** The classic look, converted to v2 and compiled through
    /// the new path, has to emit exactly what the v1 path emits — byte for
    /// byte, across poses and both Boost states.
    ///
    /// This is the same oracle the compositor flip was held to, pointed at the
    /// schema flip. It is what makes "v2 is the compositor's own model" a
    /// checked claim rather than a description.
    #[test]
    fn classic_as_a_v2_document_paints_exactly_what_v1_paints() {
        let v1 = crate::skin::doc::ParamSkin::compile(&classic_v1())
            .expect("the shipped classic document compiles as v1");
        let v2 =
            LayerSkin::compile(&upgrade(&classic_v1())).expect("...and as v2 after conversion");

        // Straight, single cell, one corner, and a four-corner zigzag — the
        // shapes that exercise caps, joints and the head/tail branches.
        let poses: [&[(f64, f64)]; 5] = [
            &[(6.0, 3.0), (2.0, 3.0)],
            &[(4.0, 4.0)],
            &[(6.0, 3.0), (3.0, 3.0), (3.0, 6.0)],
            &[(6.0, 3.0), (3.0, 3.0), (3.0, 6.0), (8.0, 6.0), (8.0, 9.0)],
            &[(2.0, 2.0), (2.0, 5.0)],
        ];
        for cells in poses {
            for boost in [false, true] {
                assert_eq!(
                    record(&v2, cells, boost),
                    record(&v1, cells, boost),
                    "v2 diverged from v1 on {cells:?} (boost: {boost})"
                );
            }
        }
    }

    /// The stack the compiler builds: the author's layers with the two system
    /// layers wrapped around them, in the order the painter emits.
    #[test]
    fn the_system_layers_bracket_the_authors() {
        let skin = LayerSkin::compile(&upgrade(&classic_v1())).expect("compiles");
        let ids: Vec<&str> = skin
            .layers()
            .iter()
            .map(|layer| layer.id.as_ref())
            .collect();
        assert_eq!(
            ids,
            vec![
                "boost-band",
                "Outline",
                "Body",
                "Head glow",
                "Head cap",
                "Head highlight",
                "head-core",
            ]
        );
    }

    /// A still document bakes one frame. Converting a v1 skin must not make it
    /// pay for a 32-step ring it does not use.
    #[test]
    fn a_still_document_bakes_a_single_frame() {
        let skin = LayerSkin::compile(&upgrade(&classic_v1())).expect("compiles");
        assert_eq!(skin.engine.frame_count(), 1);

        let mut animated = upgrade(&classic_v1());
        animated.layers[1].opacity = PropExpr("0.8 + 0.2 * sin(tau * time)".to_string());
        let skin = LayerSkin::compile(&animated).expect("compiles");
        assert_eq!(skin.engine.frame_count(), ANIMATION_STEPS);
    }

    /// Expressions are placed by what they read. The middle case is the one a
    /// tier-based reading gets wrong: `boost` cannot change within a
    /// snake-frame, and folding it anyway would freeze the layer.
    #[test]
    fn expressions_are_placed_by_what_they_read() {
        let mut doc = upgrade(&classic_v1());
        doc.layers[1].opacity = PropExpr("0.5".to_string());
        doc.layers[3].opacity = PropExpr("mix(0.7, 1, boost)".to_string());
        doc.layers[4].opacity = PropExpr("0.5 + 0.5 * sin(tau * time)".to_string());

        let skin = LayerSkin::compile(&doc).expect("compiles");
        let by_id = |id: &str| {
            skin.layers()
                .iter()
                .find(|layer| layer.id == id)
                .expect("layer present")
                .opacity
                .clone()
        };
        assert_eq!(by_id("Body"), Binding::Const(0.5), "folded");
        assert!(
            matches!(by_id("Head cap"), Binding::Snake(_)),
            "a boost-reactive layer must stay live per snake"
        );
        assert!(
            matches!(by_id("Head highlight"), Binding::Param(_)),
            "a clock-only expression is baked"
        );
    }

    /// A group's opacity multiplies into its children and the group itself
    /// disappears — the compositor never sees one, so nothing about op-count
    /// invariance depends on how an author organised their panel.
    #[test]
    fn groups_flatten_into_their_children() {
        let mut doc = upgrade(&classic_v1());
        let cap = doc.layers.remove(3);
        let highlight = doc.layers.remove(3);
        doc.layers.push(LayerV2 {
            name: "Head dressing".to_string(),
            boost_only: false,
            omit_on_single_cell: false,
            opacity: PropExpr("0.5".to_string()),
            transform: skin_schema::v2::TransformV2::default(),
            body: LayerBodyV2::Group {
                layers: vec![cap, highlight],
            },
        });

        let skin = LayerSkin::compile(&doc).expect("compiles");
        let ids: Vec<&str> = skin
            .layers()
            .iter()
            .map(|layer| layer.id.as_ref())
            .collect();
        assert!(!ids.contains(&"Head dressing"), "{ids:?}");
        assert!(ids.contains(&"Head cap") && ids.contains(&"Head highlight"));

        // 0.5 (group) x 1 (own) — resolved, not merely stored.
        let cap = skin
            .layers()
            .iter()
            .find(|layer| layer.id == "Head cap")
            .expect("present");
        assert_eq!(cap.opacity, Binding::Const(0.5));
    }

    /// Derived colours are per role, which is the reason the table lives on
    /// the swatch: one lightened fill is a different colour on each side.
    #[test]
    fn a_derived_colour_resolves_differently_for_each_side() {
        let mut doc = upgrade(&classic_v1());
        let LayerBodyV2::Ribbon { color, .. } = &mut doc.layers[1].body else {
            panic!("the second layer is the body ribbon");
        };
        color.lighten = Some(PropExpr("0.2".to_string()));

        let skin = LayerSkin::compile(&doc).expect("compiles");
        let frame = skin.engine.frame_at(0);
        assert_eq!(frame.friendly[0].extra.len(), 1);
        assert_ne!(
            frame.friendly[0].extra[0], frame.enemy[0].extra[0],
            "a lightened fill must stay the side's own colour"
        );
        // ...and it is genuinely lighter than the fill it derives from.
        let base = Rgb::parse(&frame.friendly[0].fill).expect("hex");
        let lit = Rgb::parse(&frame.friendly[0].extra[0]).expect("hex");
        assert!(lit.relative_luminance() > base.relative_luminance());
    }

    /// A named literal is one colour for everyone — that is what makes it a
    /// literal rather than a palette entry.
    #[test]
    fn a_named_literal_is_the_same_colour_for_every_role() {
        let mut doc = upgrade(&classic_v1());
        doc.literals
            .insert("gleam".to_string(), "#fff7d6".to_string());
        doc.layers.push(LayerV2 {
            name: "Gleam".to_string(),
            boost_only: false,
            omit_on_single_cell: false,
            opacity: PropExpr("0.4".to_string()),
            transform: skin_schema::v2::TransformV2::default(),
            body: LayerBodyV2::Span {
                region: RegionV2::Body,
                clip: ClipV2::Silhouette,
                span: skin_schema::v2::SpanV2::whole(),
                corner: CornerV2::Fan,
                source: SourceV2::Solid {
                    color: ColorRef {
                        target: ColorTarget::Literal {
                            literal: "gleam".to_string(),
                        },
                        lighten: None,
                    },
                },
            },
        });

        let skin = LayerSkin::compile(&doc).expect("compiles");
        let frame = skin.engine.frame_at(0);
        assert_eq!(frame.friendly[0].extra[0], "#fff7d6");
        assert_eq!(frame.enemy[0].extra[0], "#fff7d6");
    }

    /// The document's accent reaches the renderer's accent slot, which is the
    /// capability v1 documents could never use.
    #[test]
    fn a_documents_accent_reaches_the_renderers_accent_slot() {
        let mut doc = upgrade(&classic_v1());
        doc.palette.friendly[0].accent = Some("#ffd166".to_string());
        let skin = LayerSkin::compile(&doc).expect("compiles");
        assert_eq!(skin.engine.frame_at(0).friendly[0].accent, "#ffd166");

        // Absent still means the fill, so nothing that already shipped moves.
        assert_eq!(
            skin.engine.frame_at(0).enemy[0].accent,
            skin.engine.frame_at(0).enemy[0].fill
        );
    }
}
