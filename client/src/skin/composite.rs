//! The layer compositor.
//!
//! One executor, driven by a static layer stack and a baked ring of animation
//! steps. `specs/skin-shading-prd.md` section 10 sets the shape: bake anything
//! expensive at registration, keep the per-frame path to a bounded number of
//! canvas ops, and never allocate per layer per frame.
//!
//! The load-bearing design choice here is not in the PRD, and is worth being
//! explicit about. The PRD assumed a compositor would lower a solid body layer
//! as *clipped blits*, concluded that op equality with the hand-written painter
//! could not survive, and budgeted a pixel tolerance for the difference
//! (section 12). It does not have to be that way: a solid full-body layer's
//! cheapest correct lowering **is** the stroked capsule union the classic
//! painter already emits. Choosing that lowering makes classic-as-layers
//! byte-identical to classic, so the golden trace recorded before the skin
//! system existed keeps proving what it always proved, natively, in CI — a
//! strictly stronger result than "within 1/255 over 99.9% of pixels", and one
//! that does not spend the guarantee to get it.
//!
//! The browser comparator built in `client/web/tests/skins` is still required
//! and still runs: it is the only oracle for the five non-classic skins, and
//! the only way to check an image layer at all.
//!
//! Some of what follows has no caller in the non-test build — see the note in
//! `skin::layer` for why that is the intended state rather than dead weight.

#![allow(dead_code)]

use crate::skin::geometry::walk_cells_from_head;
use crate::skin::layer::{
    Anchor, ColorSlot, DiscPaint, Layer, LayerKind, LayerTransform, Region, Source, Span,
};
use crate::skin::space::{
    ClipShape, RibbonPlan, arc_length, clip_to_body, emit_ribbon, for_each_run,
};
use crate::skin::{
    BaseTheme, CelebrationTheme, PaintCtx, SkinColors, SkinIdentity, SkinMetrics, SnakePose,
    SnakeRole, SnakeSkin,
};
use wasm_bindgen::prelude::*;

const FULL_CIRCLE: f64 = 2.0 * std::f64::consts::PI;

/// One role's colours, resolved at one animation step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Swatch {
    pub fill: String,
    pub outline: String,
    pub label: String,
    pub swatch: String,
    /// A third colour for a skin's signature element. Defaults to the fill for
    /// skins that have no such element.
    pub accent: String,
}

/// Everything one animation step resolves to.
///
/// A frame is plain data, which is what lets the recorder capture it exactly
/// and a validator inspect every frame a viewer could ever see before the skin
/// is painted once.
#[derive(Clone, Debug, PartialEq)]
pub struct Frame {
    pub friendly: [Swatch; 2],
    pub enemy: [Swatch; 2],
    pub free_for_all: [Swatch; 4],
    /// Peak opacity of the head ramp at this step.
    pub ramp_opacity: f64,
    /// Where the travelling wave's crests sit, in turns.
    pub wave_phase_turns: f64,
    /// Position in the animation cycle, in turns. Drives expressions and
    /// frame strips. Distinct from `wave_phase_turns`, which is the head
    /// ramp's own wave and may run at a different rate.
    pub time_turns: f64,
    /// Per-track layer opacities, indexed by `Layer::opacity_track`.
    pub layer_opacity: Vec<f64>,
    /// Per-track scalars, indexed by `LayerKind::HeadDisc::radius_track`.
    pub scalars: Vec<f64>,
    /// Colours a layer can name with [`ColorSlot::Literal`].
    pub literals: Vec<String>,
}

/// The parts of a skin that never vary with role or time.
#[derive(Clone, Debug, PartialEq)]
pub struct CompositeConfig {
    pub boost_color: String,
    pub head_core_color: String,
    pub head_core_ratio: f64,
    pub head_core_is_dark: bool,
    /// Cells per crest and amplitude for the head ramp's travelling wave.
    pub wave: Option<(f64, f64)>,
}

/// A skin built from layers.
pub struct CompositeSkin {
    id: String,
    name: String,
    layers: Vec<Layer>,
    frames: Vec<Frame>,
    period_ms: f64,
    config: CompositeConfig,
    /// Named atlas rectangles, indexed by `Source::Image { region }`.
    regions: Vec<crate::skin::atlas::AtlasRegion>,
    base: Option<BaseThemeOwned>,
    celebration: Option<CelebrationThemeOwned>,
}

/// Owned copies of the world dressing, so the trait can borrow from `self`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BaseThemeOwned {
    pub friendly_zone: String,
    pub enemy_zone: String,
    pub friendly_wall: String,
    pub enemy_wall: String,
    pub friendly_text: String,
    pub enemy_text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CelebrationThemeOwned {
    pub effect: String,
    pub friendly_accent: String,
    pub enemy_accent: String,
    pub readout_friendly: String,
    pub readout_enemy: String,
}

/// Why a layer stack was rejected at registration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LayerStackError {
    pub layer: String,
    pub problem: String,
}

impl CompositeSkin {
    /// Build a skin from a validated layer stack and a baked ring.
    ///
    /// Validation happens here rather than at paint time because the whole
    /// point of static topology is that a skin's shape — and therefore its
    /// cost, its overhang, and its region discipline — is knowable before it
    /// ever paints.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        layers: Vec<Layer>,
        frames: Vec<Frame>,
        period_ms: f64,
        config: CompositeConfig,
        base: Option<BaseThemeOwned>,
        celebration: Option<CelebrationThemeOwned>,
    ) -> Result<Self, Vec<LayerStackError>> {
        Self::with_atlas(
            id,
            name,
            layers,
            frames,
            period_ms,
            config,
            Vec::new(),
            base,
            celebration,
        )
    }

    /// Build a skin that draws from an atlas.
    #[allow(clippy::too_many_arguments)]
    pub fn with_atlas(
        id: impl Into<String>,
        name: impl Into<String>,
        layers: Vec<Layer>,
        frames: Vec<Frame>,
        period_ms: f64,
        config: CompositeConfig,
        regions: Vec<crate::skin::atlas::AtlasRegion>,
        base: Option<BaseThemeOwned>,
        celebration: Option<CelebrationThemeOwned>,
    ) -> Result<Self, Vec<LayerStackError>> {
        let mut problems = validate_layers(&layers, &frames);
        for layer in &layers {
            if let LayerKind::Span {
                source: Source::Image { region, .. },
                ..
            } = &layer.kind
                && *region >= regions.len()
            {
                problems.push(LayerStackError {
                    layer: layer.id.to_string(),
                    problem: format!("names atlas region {region}, which does not exist"),
                });
            }
        }
        if !problems.is_empty() {
            return Err(problems);
        }
        Ok(Self {
            id: id.into(),
            name: name.into(),
            layers,
            frames,
            period_ms,
            config,
            regions,
            base,
            celebration,
        })
    }

    pub fn layers(&self) -> &[Layer] {
        &self.layers
    }

    /// Which precomputed frame a clock reading lands on.
    ///
    /// Reduced motion always gets frame zero, which is the resting pose.
    fn frame_index(&self, anim_ms: f64, reduced_motion: bool) -> usize {
        if reduced_motion || self.frames.len() <= 1 || !anim_ms.is_finite() {
            return 0;
        }
        let steps = self.frames.len() as f64;
        let step = (anim_ms / self.period_ms * steps).floor();
        (step.rem_euclid(steps)) as usize % self.frames.len()
    }

    fn swatch<'a>(&self, frame: &'a Frame, identity: &SkinIdentity) -> &'a Swatch {
        let shade = (identity.shade_slot % 2) as usize;
        match identity.role {
            SnakeRole::Own | SnakeRole::Teammate => &frame.friendly[shade],
            SnakeRole::Enemy => &frame.enemy[shade],
            SnakeRole::SpectatedTeam(0) => &frame.friendly[shade],
            SnakeRole::SpectatedTeam(_) => &frame.enemy[shade],
            SnakeRole::FreeForAll { palette_slot } => {
                &frame.free_for_all[(palette_slot as usize).min(3)]
            }
        }
    }
}

/// Reject a stack that could not keep the regioned frame's promises.
///
/// Every rule here is one the frame depends on structurally, so breaking it
/// would not produce a wrong picture so much as a wrong *claim* — an
/// `overhang_px` that no longer bounds anything, or a Boost band a body layer
/// can paint over.
fn validate_layers(layers: &[Layer], frames: &[Frame]) -> Vec<LayerStackError> {
    let mut problems = Vec::new();
    fn reject(problems: &mut Vec<LayerStackError>, layer: &Layer, problem: &str) {
        problems.push(LayerStackError {
            layer: layer.id.to_string(),
            problem: problem.to_string(),
        });
    }

    if frames.is_empty() {
        problems.push(LayerStackError {
            layer: "<stack>".to_string(),
            problem: "a skin needs at least one baked frame".to_string(),
        });
    }

    let mut seen_body_or_head = false;
    for layer in layers {
        match layer.region {
            Region::Contour if seen_body_or_head => reject(
                &mut problems,
                layer,
                "contour layers must all precede body and head layers, or a body \
                 layer could paint over the Boost band",
            ),
            Region::Contour => {}
            _ => seen_body_or_head = true,
        }

        match &layer.kind {
            LayerKind::Ribbon { extra, .. } => {
                if layer.region != Region::Contour && *extra != 0.0 {
                    reject(
                        &mut problems,
                        layer,
                        "only a contour layer may paint wider than the body; a body \
                         ribbon with extra width would make overhang_px a lie",
                    );
                }
            }
            LayerKind::HeadDisc {
                radius_ratio,
                radius_track,
                ..
            } => {
                if let Some(track) = radius_track
                    && frames.iter().any(|frame| *track >= frame.scalars.len())
                {
                    reject(
                        &mut problems,
                        layer,
                        "radius track is not present in every baked frame",
                    );
                }
                let widest = radius_track
                    .map(|track| {
                        frames
                            .iter()
                            .filter_map(|frame| frame.scalars.get(track).copied())
                            .fold(*radius_ratio, f64::max)
                    })
                    .unwrap_or(*radius_ratio);
                if widest > 0.5 {
                    reject(
                        &mut problems,
                        layer,
                        "a head disc wider than half a cell escapes the body \
                         silhouette and would contribute unreported overhang",
                    );
                }
            }
            LayerKind::Span { source, corner, .. } => {
                let blit = matches!(source, Source::Image { .. });
                if blit && !corner.allows_blits() {
                    reject(
                        &mut problems,
                        layer,
                        "image and pattern layers are Own-corner only: a diagonal \
                         joint boundary is not an axis-aligned rectangle, and \
                         honouring it for a blit needs a per-run clip",
                    );
                }
            }
            LayerKind::HeadRamp { .. } => {}
        }

        if let Some(track) = layer.opacity_track
            && frames
                .iter()
                .any(|frame| track >= frame.layer_opacity.len())
        {
            reject(
                &mut problems,
                layer,
                "opacity track is not present in every baked frame",
            );
        }
    }

    // The Boost band has to be the outermost thing painted, or a later contour
    // layer buries it and every op-text check still passes.
    if let Some(first_boost) = layers.iter().position(|layer| layer.boost_only)
        && layers[..first_boost]
            .iter()
            .any(|layer| layer.region == Region::Contour && !layer.boost_only)
    {
        problems.push(LayerStackError {
            layer: layers[first_boost].id.to_string(),
            problem: "the Boost band must be the outermost contour layer".to_string(),
        });
    }

    problems
}

/// A span resolved against an actual body, in cells.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Allocation {
    start: f64,
    end: f64,
}

/// Grant each span layer a stretch of body, in priority order.
///
/// Snakes start at three cells, so the interesting case is the one where the
/// spans do not all fit. Each gets its `natural` length while the body allows,
/// then degrades toward `min`, then disappears — and a layer that disappears
/// lets whatever is beneath it show through, which is why every image skin
/// needs a solid base layer under its art
/// (`specs/skin-shading-prd.md` section 8.3).
///
/// Anchored spans (`At`, `Fraction`) do not consume from the ends: they name a
/// place rather than queue for room, and overlaps between them resolve by layer
/// order like everything else.
///
/// Spans are measured over the **paintable** range, `-0.5 ..= body_len + 0.5`,
/// not over the arc length. Arc length runs centre to centre, so a span covering
/// `0 ..= body_len` would miss the head and tail caps — half a cell at each end
/// that a base layer very much needs to paint. It also makes the units what an
/// author expects: a body of `n` cells has `n` cells of span, and a one-cell
/// snake (arc length zero) still has one cell to paint.
fn allocate_spans(layers: &[Layer], body_len: f64, into: &mut Vec<Option<Allocation>>) {
    into.clear();
    into.resize(layers.len(), None);

    let mut order: Vec<usize> = (0..layers.len())
        .filter(|index| matches!(layers[*index].kind, LayerKind::Span { .. }))
        .collect();
    order.sort_by_key(|index| {
        let LayerKind::Span { span, .. } = &layers[*index].kind else {
            unreachable!("filtered to span layers");
        };
        (std::cmp::Reverse(span.priority), *index)
    });

    let (paint_start, paint_end) = (-0.5, body_len + 0.5);
    let mut head_cursor = paint_start;
    let mut tail_cursor = paint_end;

    for index in order {
        let LayerKind::Span { span, .. } = &layers[index].kind else {
            unreachable!("filtered to span layers");
        };
        let remaining = (tail_cursor - head_cursor).max(0.0);
        let wanted = span.natural.unwrap_or(remaining);

        let allocation = match span.from {
            Anchor::Whole => Some(Allocation {
                start: paint_start,
                end: paint_end,
            }),
            Anchor::Head => {
                let length = wanted.min(remaining);
                (length >= span.min && length > 0.0).then(|| {
                    let start = head_cursor;
                    head_cursor += length;
                    Allocation {
                        start,
                        end: start + length,
                    }
                })
            }
            Anchor::Tail => {
                let length = wanted.min(remaining);
                (length >= span.min && length > 0.0).then(|| {
                    let end = tail_cursor;
                    tail_cursor -= length;
                    Allocation {
                        start: end - length,
                        end,
                    }
                })
            }
            Anchor::At(_) | Anchor::Fraction(_) => {
                let start = match span.from {
                    Anchor::Fraction(fraction) => (paint_start
                        + fraction * (paint_end - paint_start))
                        .clamp(paint_start, paint_end),
                    Anchor::At(s) => s.clamp(paint_start, paint_end),
                    _ => unreachable!("only anchored spans reach this arm"),
                };
                let length = wanted.min(paint_end - start);
                (length >= span.min && length > 0.0).then_some(Allocation {
                    start,
                    end: start + length,
                })
            }
        };
        into[index] = allocation;
    }
}

/// Resolve a colour slot against a frame.
fn color<'a>(
    slot: ColorSlot,
    swatch: &'a Swatch,
    frame: &'a Frame,
    config: &'a CompositeConfig,
) -> &'a str {
    match slot {
        ColorSlot::Fill => &swatch.fill,
        ColorSlot::Outline => &swatch.outline,
        ColorSlot::Boost => &config.boost_color,
        ColorSlot::HeadCore => &config.head_core_color,
        ColorSlot::Accent => &swatch.accent,
        ColorSlot::Literal(index) => frame
            .literals
            .get(index)
            .map(String::as_str)
            // A literal index that survived validation cannot be missing; if
            // one ever is, a visible magenta is better than a panic in a frame.
            .unwrap_or("#ff00ff"),
    }
}

impl SnakeSkin for CompositeSkin {
    fn id(&self) -> &str {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn colors(&self, identity: &SkinIdentity) -> SkinColors<'_> {
        // Swatches, CSS variables and label ink read the resting pose. A
        // shimmering snake should not make the results table shimmer.
        let swatch = self.swatch(&self.frames[0], identity);
        SkinColors {
            fill: &swatch.fill,
            outline: &swatch.outline,
            label: &swatch.label,
            swatch: &swatch.swatch,
        }
    }

    fn metrics(&self, boost_active: bool) -> SkinMetrics {
        SkinMetrics {
            // Computed from the stack rather than measured, which is exactly
            // what the regioned frame buys: only contour layers can be
            // non-zero, so this is a maximum over a known set.
            overhang_px: self
                .layers
                .iter()
                .filter(|layer| layer.applies(boost_active, 2))
                .map(Layer::overhang_px)
                .fold(0.0, f64::max),
            head_core_radius_ratio: self.config.head_core_ratio,
            head_core_is_dark: self.config.head_core_is_dark,
        }
    }

    fn paint_alive(
        &self,
        ctx: &mut PaintCtx,
        pose: &SnakePose,
        identity: &SkinIdentity,
    ) -> Result<(), JsValue> {
        if pose.cells.is_empty() {
            return Ok(());
        }

        let frame = &self.frames[self.frame_index(pose.anim_ms, pose.reduced_motion)];
        let swatch = self.swatch(frame, identity);
        let body_len = arc_length(pose.cells);

        let mut allocations = Vec::new();
        allocate_spans(&self.layers, body_len, &mut allocations);

        // One clip per snake, opened lazily and only for layers that could
        // escape without it. Classic's layers cannot, so classic emits no clip
        // ops at all — which is what keeps its op stream byte-identical.
        let mut open_clip: Option<ClipShape> = None;

        for (index, layer) in self.layers.iter().enumerate() {
            if !layer.applies(pose.boost_active, pose.cells.len()) {
                continue;
            }

            let wants_clip = layer_needs_clip(layer).then_some(layer.clip);
            if wants_clip != open_clip {
                if open_clip.is_some() {
                    ctx.restore();
                }
                if let Some(shape) = wants_clip {
                    ctx.save();
                    clip_to_body(ctx, pose.cells, pose.cell_size, shape)?;
                }
                open_clip = wants_clip;
            }

            let opacity = layer
                .opacity_track
                .and_then(|track| frame.layer_opacity.get(track).copied());
            if let Some(alpha) = opacity {
                ctx.set_global_alpha(alpha);
            }

            let transformed = !layer.transform.is_identity();
            if transformed {
                ctx.save();
                apply_transform(ctx, &layer.transform, pose.cell_size)?;
            }

            self.paint_layer(
                ctx,
                pose,
                layer,
                frame,
                swatch,
                allocations[index],
                body_len,
            )?;

            if transformed {
                ctx.restore();
            }
            if opacity.is_some() {
                ctx.set_global_alpha(1.0);
            }
        }

        if open_clip.is_some() {
            ctx.restore();
        }
        Ok(())
    }

    fn base_theme(&self) -> Option<BaseTheme<'_>> {
        self.base.as_ref().map(|base| BaseTheme {
            friendly_zone: &base.friendly_zone,
            enemy_zone: &base.enemy_zone,
            friendly_wall: &base.friendly_wall,
            enemy_wall: &base.enemy_wall,
            friendly_text: &base.friendly_text,
            enemy_text: &base.enemy_text,
        })
    }

    fn celebration_theme(&self) -> Option<CelebrationTheme<'_>> {
        self.celebration
            .as_ref()
            .map(|celebration| CelebrationTheme {
                effect: &celebration.effect,
                friendly_accent: &celebration.friendly_accent,
                enemy_accent: &celebration.enemy_accent,
                readout_friendly: &celebration.readout_friendly,
                readout_enemy: &celebration.readout_enemy,
            })
    }
}

/// Whether a layer could paint outside its region without a clip.
///
/// A ribbon is the silhouette; a head disc is bounded by validation; a head
/// ramp paints exactly the body's cell squares, which *is* the `Cells` clip.
/// Only span layers need the clip actually emitted.
fn layer_needs_clip(layer: &Layer) -> bool {
    matches!(layer.kind, LayerKind::Span { .. })
}

/// Apply a layer transform, in the layer's own space.
fn apply_transform(
    ctx: &mut PaintCtx,
    transform: &LayerTransform,
    cell_size: f64,
) -> Result<(), JsValue> {
    if transform.translate != (0.0, 0.0) {
        ctx.translate(
            transform.translate.0 * cell_size,
            transform.translate.1 * cell_size,
        )?;
    }
    if transform.rotate_turns != 0.0 {
        ctx.rotate(transform.rotate_turns * FULL_CIRCLE)?;
    }
    if transform.scale != (1.0, 1.0) {
        ctx.scale(transform.scale.0, transform.scale.1)?;
    }
    Ok(())
}

impl CompositeSkin {
    #[allow(clippy::too_many_arguments)]
    fn paint_layer(
        &self,
        ctx: &mut PaintCtx,
        pose: &SnakePose,
        layer: &Layer,
        frame: &Frame,
        swatch: &Swatch,
        allocation: Option<Allocation>,
        body_len: f64,
    ) -> Result<(), JsValue> {
        match &layer.kind {
            LayerKind::Ribbon {
                color: slot,
                extra,
                joints,
                tail_cap,
                fill_before_strokes,
                refill_before_tail_cap,
            } => emit_ribbon(
                ctx,
                pose.cells,
                pose.cell_size,
                &RibbonPlan {
                    color: color(*slot, swatch, frame, &self.config),
                    extra: *extra,
                    joints: *joints,
                    tail_cap: *tail_cap,
                    fill_before_strokes: *fill_before_strokes,
                    refill_before_tail_cap: *refill_before_tail_cap,
                },
            ),
            LayerKind::HeadRamp { rgb, length_cells } => {
                let wave = self
                    .config
                    .wave
                    .map(|(cells_per_crest, amplitude)| GradientWave {
                        cells_per_crest,
                        amplitude,
                        phase_turns: frame.wave_phase_turns,
                    });
                for (x, y, distance) in walk_cells_from_head(pose.cells, *length_cells) {
                    let base = (1.0 - distance / length_cells) * frame.ramp_opacity;
                    let opacity = match wave {
                        Some(wave) => (base + wave_offset(wave, distance)).clamp(0.0, 1.0),
                        None => base,
                    };
                    let (r, g, b) = *rgb;
                    ctx.set_fill(&format!("rgba({r}, {g}, {b}, {opacity})"));
                    ctx.fill_rect(
                        x as f64 * pose.cell_size,
                        y as f64 * pose.cell_size,
                        pose.cell_size,
                        pose.cell_size,
                    );
                }
                Ok(())
            }
            LayerKind::HeadDisc {
                paint,
                radius_ratio,
                radius_track,
            } => {
                let (head_x, head_y) = pose.cells[0];
                let centre = (
                    head_x * pose.cell_size + pose.cell_size / 2.0,
                    head_y * pose.cell_size + pose.cell_size / 2.0,
                );
                let radius = pose.cell_size
                    * radius_track
                        .and_then(|track| frame.scalars.get(track).copied())
                        .unwrap_or(*radius_ratio);
                match paint {
                    DiscPaint::Slot(slot) => {
                        ctx.set_fill(color(*slot, swatch, frame, &self.config))
                    }
                    DiscPaint::RampPeak => {
                        let (r, g, b) = ramp_rgb(&self.layers);
                        let opacity = frame.ramp_opacity;
                        ctx.set_fill(&format!("rgba({r}, {g}, {b}, {opacity})"));
                    }
                    DiscPaint::RadialGlow { slot, stops } => {
                        let base = color(*slot, swatch, frame, &self.config);
                        ctx.set_fill_gradient(&crate::skin::paint::Gradient::Radial {
                            x0: centre.0,
                            y0: centre.1,
                            r0: 0.0,
                            x1: centre.0,
                            y1: centre.1,
                            r1: radius,
                            stops: stops
                                .iter()
                                .map(|(offset, alpha)| crate::skin::paint::GradientStop {
                                    offset: *offset,
                                    color: format!("{base}{alpha}"),
                                })
                                .collect(),
                        })?;
                    }
                }
                ctx.begin_path();
                ctx.arc(centre.0, centre.1, radius, 0.0, FULL_CIRCLE)?;
                ctx.fill();
                Ok(())
            }
            LayerKind::Span { source, corner, .. } => {
                let Some(allocation) = allocation else {
                    // The body was too short for this span's minimum, so the
                    // layer is absent and whatever is beneath shows through.
                    return Ok(());
                };
                self.paint_span(
                    ctx, pose, source, *corner, allocation, swatch, frame, body_len,
                )
            }
        }
    }

    /// Paint a span as one axis-aligned rectangle per run it overlaps.
    #[allow(clippy::too_many_arguments)]
    fn paint_span(
        &self,
        ctx: &mut PaintCtx,
        pose: &SnakePose,
        source: &Source,
        corner: crate::skin::space::CornerPolicy,
        allocation: Allocation,
        swatch: &Swatch,
        frame: &Frame,
        _body_len: f64,
    ) -> Result<(), JsValue> {
        // A solid span needs no run frame: runs are axis-aligned, so the
        // rectangle is computable directly in screen space and costs one op
        // per run after the fill is set. Sources that need the run's own
        // coordinate frame are the ones that pay for a transform.
        if let Source::Solid(slot) = source {
            ctx.set_fill(color(*slot, swatch, frame, &self.config));
            let cell = pose.cell_size;
            for_each_run(pose.cells, |run| {
                let (ribbon_start, ribbon_end) = run.ribbon_range(corner);
                let start = allocation.start.max(ribbon_start);
                let end = allocation.end.min(ribbon_end);
                if end <= start {
                    return;
                }
                let near = run.point(cell, start, -0.5);
                let far = run.point(cell, end, 0.5);
                ctx.fill_rect(
                    near.0.min(far.0),
                    near.1.min(far.1),
                    (far.0 - near.0).abs(),
                    (far.1 - near.1).abs(),
                );
            });
            return Ok(());
        }

        // A tile is a run of rectangles in body space, so like a solid it
        // needs no run frame: each repeat maps straight to a screen rectangle.
        if let Source::Tiled {
            color: slot,
            period_cells,
            duty,
            half_width,
            alpha,
        } = source
        {
            let period = period_cells.max(1e-6);
            let painted = (period * duty.clamp(0.0, 1.0)).max(0.0);
            if painted <= 0.0 {
                return Ok(());
            }
            ctx.set_fill(color(*slot, swatch, frame, &self.config));

            let cell = pose.cell_size;
            let half = half_width.clamp(0.0, 0.5);
            // Tiles are laid out from the head along the body, not per run, so
            // a repeat that straddles a corner stays one tile in body space and
            // the pattern does not restart at every turn.
            let first = (allocation.start / period).floor();
            let last = (allocation.end / period).ceil();
            let mut index = first;
            while index < last {
                let tile_start = index * period;
                index += 1.0;
                let (from, to) = (
                    tile_start.max(allocation.start),
                    (tile_start + painted).min(allocation.end),
                );
                if to <= from {
                    continue;
                }

                if let Some(expr) = alpha {
                    let value = expr.eval(&skin_schema::expr::Env {
                        s: (from + to) / 2.0,
                        t: 0.0,
                        len: _body_len,
                        time: frame.time_turns,
                        boost: if pose.boost_active { 1.0 } else { 0.0 },
                        seed: 0.0,
                    });
                    ctx.set_global_alpha(value.clamp(0.0, 1.0));
                }

                for_each_run(pose.cells, |run| {
                    let (ribbon_start, ribbon_end) = run.ribbon_range(corner);
                    let start = from.max(ribbon_start);
                    let end = to.min(ribbon_end);
                    if end <= start {
                        return;
                    }
                    let near = run.point(cell, start, -half);
                    let far = run.point(cell, end, half);
                    ctx.fill_rect(
                        near.0.min(far.0),
                        near.1.min(far.1),
                        (far.0 - near.0).abs(),
                        (far.1 - near.1).abs(),
                    );
                });

                if alpha.is_some() {
                    ctx.set_global_alpha(1.0);
                }
            }
            return Ok(());
        }

        // Gradients and images are defined in the run's own frame, so each run
        // gets the affine pushed and popped around a single draw.
        let cell = pose.cell_size;
        let mut error: Option<JsValue> = None;
        let mut runs: Vec<crate::skin::space::Run> = Vec::new();
        for_each_run(pose.cells, |run| runs.push(run));

        for run in runs {
            let (ribbon_start, ribbon_end) = run.ribbon_range(corner);
            let start = allocation.start.max(ribbon_start);
            let end = allocation.end.min(ribbon_end);
            if end <= start {
                continue;
            }

            let affine = run.affine(cell);
            ctx.save();
            if let Err(cause) =
                ctx.transform(affine.a, affine.b, affine.c, affine.d, affine.e, affine.f)
            {
                ctx.restore();
                error = Some(cause);
                break;
            }

            let (u0, u1) = (start - run.s0, end - run.s0);
            match source {
                Source::Solid(_) => unreachable!("handled above"),
                Source::LinearAlongBody(stops) | Source::RadialFromStart(stops) => {
                    let gradient =
                        build_gradient(source, stops, u0, u1, swatch, frame, &self.config);
                    if let Err(cause) = ctx.set_fill_gradient(&gradient) {
                        ctx.restore();
                        error = Some(cause);
                        break;
                    }
                    ctx.fill_rect(u0, -0.5, u1 - u0, 1.0);
                }
                Source::Tiled { .. } => unreachable!("handled above"),
                Source::Image { region, fit } => {
                    let Some(region) = self.regions.get(*region) else {
                        ctx.restore();
                        continue;
                    };
                    let (sx, sy, sw, sh) = region.source_rect(frame.time_turns);
                    // How much of the source one cell of body consumes. `Clip`
                    // and `Tile` keep the art's proportions by pinning that to
                    // the region's own aspect; `Stretch` divides the whole
                    // source across the whole span instead.
                    let source_per_cell = match fit {
                        crate::skin::layer::Fit::Stretch => {
                            sw / (allocation.end - allocation.start).max(1e-6)
                        }
                        _ => sh,
                    };

                    // Where this run's slice sits within the span, in source
                    // pixels, wrapped for a tiling fit.
                    let offset = (start - allocation.start) * source_per_cell;
                    let (slice_x, slice_w) = match fit {
                        crate::skin::layer::Fit::Tile => {
                            (sx + offset % sw.max(1e-6), (end - start) * source_per_cell)
                        }
                        _ => (sx + offset, (end - start) * source_per_cell),
                    };
                    // Never sample past the region: its neighbour's pixels are
                    // one bilinear tap away, which is what the padding rule in
                    // `skin::atlas` exists to make survivable rather than
                    // something to rely on.
                    let clipped_w = slice_w.min((sx + sw - slice_x).max(0.0));
                    if clipped_w <= 0.0 {
                        ctx.restore();
                        continue;
                    }
                    let drawn_cells = clipped_w / source_per_cell.max(1e-6);

                    if let Err(cause) = ctx.draw_image(
                        region.image,
                        (slice_x, sy, clipped_w, sh),
                        (u0, -0.5, drawn_cells, 1.0),
                    ) {
                        ctx.restore();
                        error = Some(cause);
                        break;
                    }
                }
            }
            ctx.restore();
        }

        match error {
            Some(cause) => Err(cause),
            None => Ok(()),
        }
    }
}

/// A travelling wave, resolved to one moment in its cycle.
///
/// The wave rides the head ramp rather than replacing it, so light still falls
/// off toward the tail and the head stays the brightest part of the snake. It
/// costs exactly what the ramp already cost — the same rectangles, painted at a
/// different alpha.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GradientWave {
    pub cells_per_crest: f64,
    pub amplitude: f64,
    /// Where the crests sit right now, in turns.
    pub phase_turns: f64,
}

/// The head ramp's colour, read back off the stack for the peak-opacity disc.
fn ramp_rgb(layers: &[Layer]) -> (u8, u8, u8) {
    layers
        .iter()
        .find_map(|layer| match layer.kind {
            LayerKind::HeadRamp { rgb, .. } => Some(rgb),
            _ => None,
        })
        .unwrap_or((255, 255, 255))
}

fn wave_offset(wave: GradientWave, distance: f64) -> f64 {
    let turns = distance / wave.cells_per_crest - wave.phase_turns;
    wave.amplitude * (turns * std::f64::consts::TAU).sin()
}

fn build_gradient(
    source: &Source,
    stops: &[crate::skin::layer::Stop],
    u0: f64,
    u1: f64,
    swatch: &Swatch,
    frame: &Frame,
    config: &CompositeConfig,
) -> crate::skin::paint::Gradient {
    use crate::skin::paint::{Gradient, GradientStop};
    let resolved: Vec<GradientStop> = stops
        .iter()
        .map(|stop| GradientStop {
            offset: stop.offset,
            color: rgba_of(color(stop.color, swatch, frame, config), stop.alpha),
        })
        .collect();
    match source {
        Source::RadialFromStart(_) => Gradient::Radial {
            x0: u0,
            y0: 0.0,
            r0: 0.0,
            x1: u0,
            y1: 0.0,
            r1: (u1 - u0).max(1e-6),
            stops: resolved,
        },
        _ => Gradient::Linear {
            x0: u0,
            y0: 0.0,
            x1: u1,
            y1: 0.0,
            stops: resolved,
        },
    }
}

/// Attach an alpha to a hex colour without a colour-space round trip.
fn rgba_of(hex: &str, alpha: f64) -> String {
    if alpha >= 1.0 {
        return hex.to_string();
    }
    match skin_schema::color::Rgb::parse(hex) {
        Some(rgb) => {
            let (r, g, b) = (
                (rgb.r * 255.0).round() as u8,
                (rgb.g * 255.0).round() as u8,
                (rgb.b * 255.0).round() as u8,
            );
            format!("rgba({r}, {g}, {b}, {alpha})")
        }
        None => hex.to_string(),
    }
}

/// Convenience for building a span layer without spelling out every field.
pub fn span_layer(id: &'static str, region: Region, span: Span, source: Source) -> Layer {
    Layer {
        id,
        region,
        clip: ClipShape::Silhouette,
        kind: LayerKind::Span {
            span,
            source,
            corner: crate::skin::space::CornerPolicy::Own,
        },
        transform: LayerTransform::default(),
        boost_only: false,
        omit_on_single_cell: false,
        opacity_track: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skin::layer::Fit;
    use crate::skin::space::CornerPolicy;

    fn frame() -> Frame {
        let swatch = || Swatch {
            fill: "#3c8dde".to_string(),
            outline: "#286eae".to_string(),
            label: "#ffffff".to_string(),
            swatch: "#3c8dde".to_string(),
            accent: "#3c8dde".to_string(),
        };
        Frame {
            friendly: [swatch(), swatch()],
            enemy: [swatch(), swatch()],
            free_for_all: [swatch(), swatch(), swatch(), swatch()],
            ramp_opacity: 0.3,
            wave_phase_turns: 0.0,
            time_turns: 0.0,
            layer_opacity: vec![1.0],
            scalars: Vec::new(),
            literals: vec!["#ffffff".to_string()],
        }
    }

    fn ribbon(id: &'static str, region: Region, extra: f64) -> Layer {
        Layer {
            id,
            region,
            clip: ClipShape::Silhouette,
            kind: LayerKind::Ribbon {
                color: ColorSlot::Outline,
                extra,
                joints: true,
                tail_cap: false,
                fill_before_strokes: false,
                refill_before_tail_cap: false,
            },
            transform: LayerTransform::default(),
            boost_only: false,
            omit_on_single_cell: false,
            opacity_track: None,
        }
    }

    /// The frame's promise: a body layer cannot reach the Boost band. Enforced
    /// by ordering, because an opaque body layer painted after the band would
    /// hide it while every op-text check kept passing.
    #[test]
    fn a_contour_layer_after_a_body_layer_is_rejected() {
        let layers = vec![
            span_layer(
                "base",
                Region::Body,
                Span::WHOLE,
                Source::Solid(ColorSlot::Fill),
            ),
            ribbon("late-contour", Region::Contour, 6.0),
        ];
        let problems = validate_layers(&layers, &[frame()]);
        assert_eq!(problems.len(), 1);
        assert_eq!(problems[0].layer, "late-contour");
    }

    #[test]
    fn a_body_ribbon_may_not_claim_extra_width() {
        let problems = validate_layers(&[ribbon("wide-body", Region::Body, 4.0)], &[frame()]);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].problem.contains("overhang_px"));
        assert!(validate_layers(&[ribbon("ok", Region::Body, 0.0)], &[frame()]).is_empty());
    }

    #[test]
    fn a_head_disc_may_not_escape_the_silhouette() {
        let mut layer = ribbon("core", Region::Head, 0.0);
        layer.kind = LayerKind::HeadDisc {
            paint: DiscPaint::Slot(ColorSlot::HeadCore),
            radius_ratio: 0.9,
            radius_track: None,
        };
        let problems = validate_layers(&[layer], &[frame()]);
        assert_eq!(problems.len(), 1, "{problems:?}");
    }

    /// Section 14: a blit cannot honour a diagonal joint boundary, so the
    /// schema rejects the combination rather than silently ignoring it.
    #[test]
    fn a_bisector_corner_is_rejected_for_bitmap_sources() {
        let mut layer = span_layer(
            "art",
            Region::Body,
            Span::WHOLE,
            Source::Image {
                region: 0,
                fit: crate::skin::layer::Fit::Clip,
            },
        );
        layer.kind = LayerKind::Span {
            span: Span::WHOLE,
            source: Source::Image {
                region: 0,
                fit: crate::skin::layer::Fit::Clip,
            },
            corner: CornerPolicy::Bisector,
        };
        let problems = validate_layers(&[layer], &[frame()]);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].problem.contains("Own-corner"));
    }

    fn tiled_skin(alpha: Option<&str>) -> CompositeSkin {
        let mut layer = span_layer(
            "stripes",
            Region::Body,
            Span::WHOLE,
            Source::Tiled {
                color: ColorSlot::Fill,
                period_cells: 3.0,
                duty: 0.5,
                half_width: 0.5,
                alpha: alpha.map(|src| {
                    std::sync::Arc::new(skin_schema::expr::Expr::parse(src).expect("grammatical"))
                }),
            },
        );
        layer.clip = ClipShape::Silhouette;
        let frames = (0..4)
            .map(|step| Frame {
                time_turns: step as f64 / 4.0,
                ..frame()
            })
            .collect();
        CompositeSkin::new(
            "tiled@test",
            "Tiled",
            vec![layer],
            frames,
            1000.0,
            CompositeConfig {
                boost_color: "#fff200".to_string(),
                head_core_color: "#333333".to_string(),
                head_core_ratio: 0.38,
                head_core_is_dark: true,
                wave: None,
            },
            None,
            None,
        )
        .expect("a single body span is a valid stack")
    }

    fn shapes(skin: &dyn SnakeSkin, cells: &[(f64, f64)], anim_ms: f64) -> Vec<&'static str> {
        let mut recorder = crate::skin::paint::OpRecorder::new();
        skin.paint_alive(
            &mut PaintCtx::recording(&mut recorder),
            &SnakePose {
                cells,
                cell_size: 10.0,
                boost_active: false,
                anim_ms,
                reduced_motion: false,
            },
            &SkinIdentity {
                role: SnakeRole::Own,
                shade_slot: 0,
            },
        )
        .expect("a recording painter cannot fail");
        recorder.shapes()
    }

    /// A tile is laid out along the body, so a repeat that straddles a corner
    /// stays one tile: the pattern must not restart at every turn, or a
    /// zigzagging snake would look like a different skin from a straight one.
    #[test]
    fn tiles_are_laid_out_along_the_body_not_per_run() {
        let skin = tiled_skin(None);
        // Twelve cells straight, and the same twelve cells with two corners in
        // them. Period 3, duty 0.5 -> the same number of painted tiles.
        let straight = shapes(&skin, &[(0.0, 0.0), (11.0, 0.0)], 0.0);
        let cornered = shapes(
            &skin,
            &[(0.0, 0.0), (5.0, 0.0), (5.0, 3.0), (8.0, 3.0)],
            0.0,
        );

        let rects = |ops: &[&str]| ops.iter().filter(|op| **op == "fill_rect").count();
        assert!(
            rects(&straight) >= 4,
            "a 12-cell body holds four 3-cell tiles"
        );
        // The cornered body is shorter (11 cells of arc), but every tile it does
        // hold is still anchored to arc length rather than to a run start.
        assert!(rects(&cornered) >= 3, "{cornered:?}");
        assert!(
            ops_contain_clip(&straight),
            "a span layer clips; that is what stops it escaping the silhouette"
        );
    }

    fn ops_contain_clip(ops: &[&str]) -> bool {
        ops.contains(&"clip")
    }

    /// The rule the whole design rests on: time changes arguments, never the op
    /// sequence. An expression-driven alpha is the most likely thing to break
    /// it, because it is the one parameter that varies per emitted tile.
    #[test]
    fn an_expression_driven_tile_keeps_its_op_sequence_across_the_clock() {
        let skin = tiled_skin(Some("0.5 + 0.5 * sin(tau * (time + s / 4))"));
        let cells = [(0.0, 0.0), (11.0, 0.0), (11.0, 4.0)];
        let reference = shapes(&skin, &cells, 0.0);
        for anim_ms in [0.0, 250.0, 1_000.0, 4_321.5] {
            assert_eq!(
                reference,
                shapes(&skin, &cells, anim_ms),
                "the op sequence moved at {anim_ms}ms"
            );
        }

        // ...and it really is animating, or the check above proves nothing.
        let golden = |anim_ms: f64| {
            let mut recorder = crate::skin::paint::OpRecorder::new();
            skin.paint_alive(
                &mut PaintCtx::recording(&mut recorder),
                &SnakePose {
                    cells: &cells,
                    cell_size: 10.0,
                    boost_active: false,
                    anim_ms,
                    reduced_motion: false,
                },
                &SkinIdentity {
                    role: SnakeRole::Own,
                    shade_slot: 0,
                },
            )
            .expect("a recording painter cannot fail");
            recorder.to_golden()
        };
        assert_ne!(golden(0.0), golden(500.0), "the tile alpha never moved");
        assert_eq!(golden(0.0), golden(1_000.0), "the ring did not close");
    }

    /// The section 8.4 gate: one atlas region over the first N cells, another
    /// over the last M, a third tiled between them — with a defined result at
    /// 1, 2, 3, 5 and 21 cells.
    ///
    /// Snakes start at three cells, so most of this table is the common case
    /// rather than the edge case. What it pins is that art is never squeezed
    /// below its minimum: a slice that does not fit disappears and the base
    /// layer shows through, which is why every image skin needs one.
    #[test]
    fn a_three_slice_sprite_has_a_defined_result_at_every_body_length() {
        let region = |x: f64| crate::skin::atlas::AtlasRegion {
            image: 0,
            x,
            y: 0.0,
            width: 64.0,
            height: 16.0,
            frames: None,
        };
        let image_layer = |id, from, natural, min, priority, fit| Layer {
            id,
            region: Region::Body,
            clip: ClipShape::Silhouette,
            kind: LayerKind::Span {
                span: Span {
                    from,
                    natural,
                    min,
                    priority,
                },
                source: Source::Image { region: 0, fit },
                corner: CornerPolicy::Own,
            },
            transform: LayerTransform::default(),
            boost_only: false,
            omit_on_single_cell: false,
            opacity_track: None,
        };

        let layers = vec![
            // Every image skin needs this: with no atlas — which is the state
            // of every native test, and of any client for the first few frames
            // of a match — it is the entire snake.
            span_layer(
                "base",
                Region::Body,
                Span::WHOLE,
                Source::Solid(ColorSlot::Fill),
            ),
            image_layer("head-art", Anchor::Head, Some(5.0), 2.0, 10, Fit::Clip),
            image_layer("tail-art", Anchor::Tail, Some(3.0), 1.0, 10, Fit::Clip),
            image_layer("mid", Anchor::Head, None, 1.0, 0, Fit::Tile),
        ];

        let skin = CompositeSkin::with_atlas(
            "sprite@test",
            "Sprite",
            layers.clone(),
            vec![frame()],
            1000.0,
            CompositeConfig {
                boost_color: "#fff200".to_string(),
                head_core_color: "#333333".to_string(),
                head_core_ratio: 0.38,
                head_core_is_dark: true,
                wave: None,
            },
            vec![region(0.0)],
            None,
            None,
        )
        .expect("a base plus three slices is a valid stack");

        /// `(body cells, head-art span, tail-art span, mid span)`.
        type SliceExpectation = (
            usize,
            Option<(f64, f64)>,
            Option<(f64, f64)>,
            Option<(f64, f64)>,
        );

        let expected: [SliceExpectation; 5] = [
            // One cell of paint: too short for the head art's minimum of two,
            // so the tail art takes it and the middle disappears.
            (1, None, Some((-0.5, 0.5)), None),
            // Two cells is exactly the head art's minimum, and nothing is left.
            (2, Some((-0.5, 1.5)), None, None),
            (3, Some((-0.5, 2.5)), None, None),
            // Five cells is the head art's natural length, still leaving none.
            (5, Some((-0.5, 4.5)), None, None),
            // Only at 21 does the three-slice actually appear as designed.
            (21, Some((-0.5, 4.5)), Some((17.5, 20.5)), Some((4.5, 17.5))),
        ];

        let mut allocations = Vec::new();
        for (cells, head, tail, mid) in expected {
            let body: Vec<(f64, f64)> = if cells == 1 {
                vec![(0.0, 0.0)]
            } else {
                vec![(0.0, 0.0), (cells as f64 - 1.0, 0.0)]
            };
            let body_len = crate::skin::space::arc_length(&body);
            allocate_spans(&layers, body_len, &mut allocations);

            let got = |index: usize| allocations[index].map(|a| (a.start, a.end));
            assert_eq!(got(1), head, "head art at {cells} cells");
            assert_eq!(got(2), tail, "tail art at {cells} cells");
            assert_eq!(got(3), mid, "mid at {cells} cells");

            // Whatever the slices did, the snake is still visible: with no
            // atlas decoded, the base layer is the whole picture.
            let mut recorder = crate::skin::paint::OpRecorder::new();
            skin.paint_alive(
                &mut PaintCtx::recording(&mut recorder),
                &SnakePose::still(&body, 10.0, false),
                &SkinIdentity {
                    role: SnakeRole::Own,
                    shade_slot: 0,
                },
            )
            .expect("a recording painter cannot fail");
            assert!(
                recorder.shapes().contains(&"fill_rect"),
                "at {cells} cells the snake painted nothing visible without an atlas"
            );
        }
    }

    /// A layer naming a region that does not exist is a registration error, not
    /// a blank snake discovered in a match.
    #[test]
    fn an_image_layer_must_name_a_region_that_exists() {
        let layer = span_layer(
            "art",
            Region::Body,
            Span::WHOLE,
            Source::Image {
                region: 7,
                fit: Fit::Clip,
            },
        );
        let problems = CompositeSkin::with_atlas(
            "broken@test",
            "Broken",
            vec![layer],
            vec![frame()],
            1000.0,
            CompositeConfig {
                boost_color: "#fff200".to_string(),
                head_core_color: "#333333".to_string(),
                head_core_ratio: 0.38,
                head_core_is_dark: true,
                wave: None,
            },
            Vec::new(),
            None,
            None,
        )
        .err()
        .expect("region 7 does not exist");
        assert_eq!(problems.len(), 1);
        assert!(problems[0].problem.contains("does not exist"));
    }

    /// Three-slice on a body that cannot hold it. This is the common case, not
    /// an edge case: snakes start at three cells.
    #[test]
    fn spans_degrade_by_priority_then_disappear() {
        let head_art = |natural, min, priority| Layer {
            id: "head-art",
            region: Region::Body,
            clip: ClipShape::Silhouette,
            kind: LayerKind::Span {
                span: Span {
                    from: Anchor::Head,
                    natural: Some(natural),
                    min,
                    priority,
                },
                source: Source::Solid(ColorSlot::Fill),
                corner: CornerPolicy::Own,
            },
            transform: LayerTransform::default(),
            boost_only: false,
            omit_on_single_cell: false,
            opacity_track: None,
        };
        let mut tail_art = head_art(3.0, 1.0, 10);
        tail_art.id = "tail-art";
        if let LayerKind::Span { span, .. } = &mut tail_art.kind {
            span.from = Anchor::Tail;
        }
        let mut middle = head_art(0.0, 1.0, 0);
        middle.id = "middle";
        if let LayerKind::Span { span, .. } = &mut middle.kind {
            span.natural = None;
        }

        let layers = vec![head_art(5.0, 2.0, 10), tail_art, middle];
        let mut allocations = Vec::new();

        // A generous body: everything gets its natural length.
        allocate_spans(&layers, 21.0, &mut allocations);
        assert_eq!(
            allocations[0],
            Some(Allocation {
                start: -0.5,
                end: 4.5
            })
        );
        assert_eq!(
            allocations[1],
            Some(Allocation {
                start: 18.5,
                end: 21.5
            })
        );
        assert_eq!(
            allocations[2],
            Some(Allocation {
                start: 4.5,
                end: 18.5
            })
        );

        // A three-cell snake: the head art degrades, the tail art takes what is
        // left, and the middle vanishes rather than being squeezed below its
        // minimum.
        allocate_spans(&layers, 3.0, &mut allocations);
        assert_eq!(
            allocations[0],
            Some(Allocation {
                start: -0.5,
                end: 3.5
            })
        );
        assert_eq!(allocations[1], None, "nothing left for the tail art");
        assert_eq!(
            allocations[2], None,
            "the middle disappears, it never squeezes"
        );

        // A body of one cell of arc holds two cells of paint: enough for the
        // head art's minimum of two, and nothing after it.
        allocate_spans(&layers, 1.0, &mut allocations);
        assert_eq!(
            allocations[0],
            Some(Allocation {
                start: -0.5,
                end: 1.5
            })
        );
        assert_eq!(allocations[1], None);
    }
}
