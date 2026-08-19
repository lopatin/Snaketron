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
//! and still runs: it is the only oracle for every non-classic skin, and the
//! only way to check an image layer at all. That last clause stopped being
//! hypothetical with the animal family — natively no atlas ever decodes, so a
//! textured skin's op trace records blits that land nowhere, and the only place
//! its pixels exist is that suite.
//!
//! Some of what follows has no caller in the non-test build — see the note in
//! `skin::layer` for why that is the intended state rather than dead weight.

#![allow(dead_code)]

use crate::skin::geometry::walk_cells_from_head;
use crate::skin::layer::{
    Anchor, Binding, ColorSlot, DiscPaint, Fade, Layer, LayerKind, LayerTransform, Region, Source,
    Span,
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
    /// Every bound value at this step, indexed by [`Binding::Param`].
    ///
    /// One table, not one per property: `layer_opacity` and `scalars` were
    /// separate because each was added for the one property that needed it,
    /// and a document compiler would have had to grow a third. See [`Binding`].
    pub params: Vec<f64>,
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
    /// The skin's own pixels: images by URL, and the rectangles inside them
    /// that `Source::Image { region }` names.
    atlas: crate::skin::atlas::Atlas,
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
            crate::skin::atlas::Atlas::default(),
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
        atlas: crate::skin::atlas::Atlas,
        base: Option<BaseThemeOwned>,
        celebration: Option<CelebrationThemeOwned>,
    ) -> Result<Self, Vec<LayerStackError>> {
        let mut problems = validate_layers(&layers, &frames);
        for layer in &layers {
            let LayerKind::Span {
                source:
                    Source::Image {
                        region,
                        fade,
                        drift_cells,
                        ..
                    },
                ..
            } = &layer.kind
            else {
                continue;
            };
            // A fade is a fixed number of extra blits per run, so an absurd
            // step count is a frame-rate bug rather than an ugly one — and it
            // would only ever show up on the one skin that declared it. The
            // bound is generous: twelve is already smooth at every cell size
            // the arena uses.
            if !drift_cells.is_finite() {
                problems.push(LayerStackError {
                    layer: layer.id.to_string(),
                    problem: format!("drifts {drift_cells} cells a cycle, which is not a rate"),
                });
            }
            if let Some(fade) = fade {
                let lengths_sane = fade.lead_cells.is_finite()
                    && fade.lead_cells >= 0.0
                    && fade.trail_cells.is_finite()
                    && fade.trail_cells >= 0.0;
                if !lengths_sane {
                    problems.push(LayerStackError {
                        layer: layer.id.to_string(),
                        problem: format!(
                            "fades over {} and {} cells, which is not a length",
                            fade.lead_cells, fade.trail_cells
                        ),
                    });
                }
                if !fade.is_noop() && !(1..=64).contains(&fade.steps) {
                    problems.push(LayerStackError {
                        layer: layer.id.to_string(),
                        problem: format!(
                            "fades in {} steps; 1..=64 keeps the extra blits affordable",
                            fade.steps
                        ),
                    });
                }
            }
            // Both halves are registration errors rather than a blank snake
            // discovered mid-match: a region nobody declared, and a region
            // pointing at an image nobody declared.
            match atlas.region(*region) {
                None => problems.push(LayerStackError {
                    layer: layer.id.to_string(),
                    problem: format!("names atlas region {region}, which does not exist"),
                }),
                Some(region) if region.image >= atlas.image_count() => {
                    problems.push(LayerStackError {
                        layer: layer.id.to_string(),
                        problem: format!(
                            "draws from atlas image {}, which the skin does not declare",
                            region.image
                        ),
                    })
                }
                // A degenerate rectangle is not merely invisible. A tiling
                // layer with no declared repeat length derives one from the
                // region's aspect, so a zero-width region asks for a repeat of
                // nothing — and the paint loop would try to cover the body with
                // it, one blit at a time, until the tab died.
                Some(region)
                    if !(region.width.is_finite()
                        && region.width > 0.0
                        && region.height.is_finite()
                        && region.height > 0.0) =>
                {
                    problems.push(LayerStackError {
                        layer: layer.id.to_string(),
                        problem: format!(
                            "names a region of {}x{}, which has no pixels in it",
                            region.width, region.height
                        ),
                    })
                }
                Some(_) => {}
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
            atlas,
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
    /// How long one animation cycle takes. Exposed so a test can observe
    /// `anim_speed` at the only place it is actually visible.
    pub fn period_ms(&self) -> f64 {
        self.period_ms
    }

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
/// Whether a binding resolves at every step a viewer could land on.
///
/// A parameter missing from one frame is a registration error rather than a
/// paint-time fallback, because the fallback would look like a skin that
/// flickers once per cycle — the hardest possible thing to attribute.
fn bound_everywhere(binding: Binding, frames: &[Frame]) -> bool {
    match binding {
        Binding::Const(_) => true,
        Binding::Param(index) => frames.iter().all(|frame| index < frame.params.len()),
    }
}

/// The extreme value a binding reaches across every baked step.
///
/// Bounds that keep a layer inside the body have to hold at *every* step, so
/// they are checked against this rather than against the resting value.
/// `pick` is applied to each value on its own as well as pairwise, so a
/// caller asking for "the largest magnitude" gets it from a single-valued
/// binding too. An absent parameter yields zero; `bound_everywhere` is what
/// rejects that, so this never has to double as the error path.
fn extent_of(binding: Binding, frames: &[Frame], pick: impl Fn(f64, f64) -> f64) -> f64 {
    match binding {
        Binding::Const(value) => pick(value, value),
        Binding::Param(index) => frames
            .iter()
            .filter_map(|frame| frame.params.get(index).copied())
            .map(|value| pick(value, value))
            .reduce(&pick)
            .unwrap_or(0.0),
    }
}

fn validate_layers(layers: &[Layer], frames: &[Frame]) -> Vec<LayerStackError> {
    let mut problems: Vec<LayerStackError> = Vec::new();
    for layer in layers {
        // A body span may only clip to the silhouette. `Cells` is the union of
        // cell squares, which is *larger* than the snake at every cap and every
        // outer corner — so a span allowed to use it paints beyond the shape
        // the player sees, and the corner fix below deliberately hands whole
        // joint cells to a single run, which makes that reach further still.
        // Structural rather than reviewed: it is invisible on a solid fill and
        // obvious only on the textured skins that came last.
        if matches!(layer.kind, LayerKind::Span { .. })
            && layer.region == Region::Body
            && layer.clip != ClipShape::Silhouette
        {
            problems.push(LayerStackError {
                layer: layer.id.to_string(),
                problem: format!(
                    "is a body span clipped to {:?}, which reaches outside the \
                     snake; body spans must clip to the silhouette",
                    layer.clip
                ),
            });
        }
    }

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
            LayerKind::HeadDisc { radius, .. } => {
                if !bound_everywhere(*radius, frames) {
                    reject(
                        &mut problems,
                        layer,
                        "radius reads a parameter that is not present in every \
                         baked frame",
                    );
                }
                // The widest the disc ever gets, across every step a viewer
                // could land on — not the resting radius. A disc that only
                // escapes the silhouette mid-cycle escapes it.
                let widest = extent_of(*radius, frames, f64::max);
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
                // A repeat length is a divisor of the span on the paint path,
                // and the number of repeats is what the op budget is counted
                // in. A zero or a NaN there is an unbounded loop, so it is
                // caught here — where the skin has not painted yet — rather
                // than defended against per frame.
                if let Source::Image {
                    fit:
                        crate::skin::layer::Fit::Tile {
                            cells_per_repeat: Some(cells),
                        },
                    ..
                } = source
                    && !(cells.is_finite() && *cells > 0.0)
                {
                    reject(
                        &mut problems,
                        layer,
                        "a tiled texture must repeat over a positive, finite \
                         number of cells",
                    );
                }
                // A band that reaches past |t| = 0.5 leans on the silhouette
                // clip to stay inside the body. That clip is real, but leaning
                // on it means the layer's declared shape stops describing what
                // it paints — and the declared shape is what makes
                // `overhang_px` computable rather than measured.
                if let Source::Tiled {
                    half_width,
                    t_center,
                    ..
                } = source
                {
                    if !bound_everywhere(*half_width, frames)
                        || !bound_everywhere(*t_center, frames)
                    {
                        reject(
                            &mut problems,
                            layer,
                            "a band reads a parameter that is not present in \
                             every baked frame",
                        );
                    }
                    // Checked at the step where the lane is widest and the step
                    // where it sits furthest off-centre. Those can be different
                    // steps, so taking each extreme independently is the
                    // conservative reading — and conservative is the only safe
                    // direction for a bound that keeps a body layer off the
                    // Boost band.
                    let reach = extent_of(*t_center, frames, |a, b| a.abs().max(b.abs()))
                        + extent_of(*half_width, frames, |a, b| a.abs().max(b.abs()));
                    if reach > 0.5 + 1e-9 {
                        reject(
                            &mut problems,
                            layer,
                            "a tiled band must stay inside the body: \
                             |t_center| + half_width may not exceed 0.5",
                        );
                    }
                }
            }
            LayerKind::HeadRamp { .. } => {}
        }

        if !bound_everywhere(layer.opacity, frames) {
            reject(
                &mut problems,
                layer,
                "opacity reads a parameter that is not present in every baked \
                 frame",
            );
        }
        for binding in layer.transform.bindings() {
            if !bound_everywhere(binding, frames) {
                reject(
                    &mut problems,
                    layer,
                    "a transform reads a parameter that is not present in \
                     every baked frame",
                );
                break;
            }
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

/// Push `[a, b]` at `alpha`, clipped to the piece actually being drawn.
fn push_clipped(
    out: &mut Vec<(f64, f64, f64)>,
    (from, to): (f64, f64),
    (a, b): (f64, f64),
    alpha: f64,
) {
    let (a, b) = (a.max(from), b.min(to));
    if b > a {
        out.push((a, b, alpha));
    }
}

/// Where in the texture a repeat starts, `0..1`, for a drifting pattern.
///
/// Sign is chosen so a positive drift moves the pattern **away from the
/// anchor**: the sample offset runs backwards, which slides the marks forwards
/// along the body.
fn drift_phase(drift_cells: f64, repeat_cells: f64, time_turns: f64) -> f64 {
    if drift_cells == 0.0 || !drift_cells.is_finite() {
        return 0.0;
    }
    (-drift_cells * time_turns / repeat_cells).rem_euclid(1.0)
}

/// Split `[from, to]` into the constant-alpha slices an image is drawn in.
///
/// The slice grid is laid out over the **whole allocation**, not over the piece
/// being drawn, and that is the point: a repeat or a run that happens to
/// straddle a ramp gets the same boundaries as its neighbours, so the ramp is
/// continuous across a corner and across every tile join. Computing it per
/// piece would restart the ramp at each one, which looks like banding and would
/// have been very hard to attribute to the fade rather than to the art.
///
/// Always emits at least one slice, so callers have a single code path.
fn fade_pieces(
    fade: Option<&Fade>,
    allocation: Allocation,
    from: f64,
    to: f64,
    out: &mut Vec<(f64, f64, f64)>,
) {
    out.clear();
    let Some(fade) = fade.filter(|fade| !fade.is_noop()) else {
        out.push((from, to, 1.0));
        return;
    };

    let steps = fade.steps.max(1);
    let span = (allocation.end - allocation.start).max(0.0);
    // When the body cannot hold both ramps, they shrink together rather than
    // one winning: a span degraded to under its natural length is exactly when
    // a hard edge would appear, so that is the last moment to drop the fade.
    let wanted = fade.lead_cells.max(0.0) + fade.trail_cells.max(0.0);
    let squeeze = if wanted > span && wanted > 0.0 {
        span / wanted
    } else {
        1.0
    };
    let lead = fade.lead_cells.max(0.0) * squeeze;
    let trail = fade.trail_cells.max(0.0) * squeeze;
    let piece = (from, to);

    if lead > 0.0 {
        for step in 0..steps {
            let (t0, t1) = (step as f64 / steps as f64, (step + 1) as f64 / steps as f64);
            let bounds = (allocation.start + t0 * lead, allocation.start + t1 * lead);
            push_clipped(out, piece, bounds, (t0 + t1) / 2.0);
        }
    }
    push_clipped(
        out,
        piece,
        (allocation.start + lead, allocation.end - trail),
        1.0,
    );
    if trail > 0.0 {
        let ramp_start = allocation.end - trail;
        for step in 0..steps {
            let (t0, t1) = (step as f64 / steps as f64, (step + 1) as f64 / steps as f64);
            let bounds = (ramp_start + t0 * trail, ramp_start + t1 * trail);
            push_clipped(out, piece, bounds, 1.0 - (t0 + t1) / 2.0);
        }
    }
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

            // A fully-opaque constant emits nothing, which is what keeps every
            // pre-binding skin's op stream byte-identical.
            let opacity =
                (!layer.opacity.is_const(1.0)).then(|| layer.opacity.get(&frame.params, 1.0));
            if let Some(alpha) = opacity {
                ctx.set_global_alpha(alpha);
            }

            let transformed = !layer.transform.is_identity();
            if transformed {
                ctx.save();
                apply_transform(ctx, &layer.transform, frame, pose.cell_size)?;
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
/// Each of the three ops is emitted when its own fields are not the neutral
/// *constant* — so a bound field always emits, at every step, even where it
/// happens to evaluate to the neutral value. Deciding per step would make the
/// op sequence a function of the clock.
fn apply_transform(
    ctx: &mut PaintCtx,
    transform: &LayerTransform,
    frame: &Frame,
    cell_size: f64,
) -> Result<(), JsValue> {
    let params = &frame.params;
    if !(transform.translate.0.is_const(0.0) && transform.translate.1.is_const(0.0)) {
        ctx.translate(
            transform.translate.0.get(params, 0.0) * cell_size,
            transform.translate.1.get(params, 0.0) * cell_size,
        )?;
    }
    if !transform.rotate_turns.is_const(0.0) {
        ctx.rotate(transform.rotate_turns.get(params, 0.0) * FULL_CIRCLE)?;
    }
    if !(transform.scale.0.is_const(1.0) && transform.scale.1.is_const(1.0)) {
        ctx.scale(
            transform.scale.0.get(params, 1.0),
            transform.scale.1.get(params, 1.0),
        )?;
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
                single_pass,
            } => emit_ribbon(
                ctx,
                pose.cells,
                pose.cell_size,
                &RibbonPlan {
                    color: color(*slot, swatch, frame, &self.config),
                    // A skin quotes its contour in 1x pixels; the arena scales
                    // it with the cell so the rim keeps its weight at any zoom.
                    extra: *extra * pose.detail_scale,
                    joints: *joints,
                    tail_cap: *tail_cap,
                    fill_before_strokes: *fill_before_strokes,
                    refill_before_tail_cap: *refill_before_tail_cap,
                    single_pass: *single_pass,
                },
            ),
            LayerKind::HeadRamp {
                rgb,
                length_cells,
                opacity: curve,
            } => {
                let wave = self
                    .config
                    .wave
                    .map(|(cells_per_crest, amplitude)| GradientWave {
                        cells_per_crest,
                        amplitude,
                        phase_turns: frame.wave_phase_turns,
                    });
                for (x, y, distance) in walk_cells_from_head(pose.cells, *length_cells) {
                    let opacity = match curve {
                        // The expression *is* the curve — falloff included —
                        // so an author can reshape the glow rather than only
                        // scale the one shape the renderer knows.
                        Some(curve) => curve
                            .eval(&skin_schema::expr::Env {
                                s: distance,
                                t: 0.0,
                                len: body_len,
                                time: frame.time_turns,
                                boost: if pose.boost_active { 1.0 } else { 0.0 },
                                seed: 0.0,
                            })
                            .clamp(0.0, 1.0),
                        None => {
                            let base = (1.0 - distance / length_cells) * frame.ramp_opacity;
                            match wave {
                                Some(wave) => (base + wave_offset(wave, distance)).clamp(0.0, 1.0),
                                None => base,
                            }
                        }
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
            LayerKind::HeadDisc { paint, radius } => {
                let (head_x, head_y) = pose.cells[0];
                let centre = (
                    head_x * pose.cell_size + pose.cell_size / 2.0,
                    head_y * pose.cell_size + pose.cell_size / 2.0,
                );
                let radius = pose.cell_size * radius.get(&frame.params, 0.0);
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
                // Whatever `paint` already put on the context for this layer.
                // Read here rather than passed down from there so the two can
                // never disagree about which track is in force.
                let base_alpha = layer.opacity.get(&frame.params, 1.0);
                self.paint_span(
                    ctx, pose, source, *corner, allocation, swatch, frame, body_len, base_alpha,
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
        // The layer's own opacity, already set on the context. Sources that
        // modulate alpha multiply into it and restore to it, so a layer that
        // both animates its opacity and fades its art composes the two instead
        // of the inner one silently winning.
        base_alpha: f64,
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
            t_center,
            phase_cells,
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
            let half = half_width.get(&frame.params, 0.0).clamp(0.0, 0.5);
            // The band's own lane across the body. Validation has already
            // established that the lane fits inside the silhouette at every
            // baked step, so the clamp here is defence against a hand-built
            // layer rather than the rule itself.
            let centre = t_center
                .get(&frame.params, 0.0)
                .clamp(-0.5 + half, 0.5 - half);
            // Tiles are laid out from the head along the body, not per run, so
            // a repeat that straddles a corner stays one tile in body space and
            // the pattern does not restart at every turn.
            let first = ((allocation.start - phase_cells) / period).floor();
            let last = ((allocation.end - phase_cells) / period).ceil();
            let mut index = first;
            while index < last {
                let tile_start = index * period + phase_cells;
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
                        t: centre,
                        len: _body_len,
                        time: frame.time_turns,
                        boost: if pose.boost_active { 1.0 } else { 0.0 },
                        seed: 0.0,
                    });
                    ctx.set_global_alpha(base_alpha * value.clamp(0.0, 1.0));
                }

                for_each_run(pose.cells, |run| {
                    let (ribbon_start, ribbon_end) = run.ribbon_range(corner);
                    let start = from.max(ribbon_start);
                    let end = to.min(ribbon_end);
                    if end <= start {
                        return;
                    }
                    let near = run.point(cell, start, centre - half);
                    let far = run.point(cell, end, centre + half);
                    ctx.fill_rect(
                        near.0.min(far.0),
                        near.1.min(far.1),
                        (far.0 - near.0).abs(),
                        (far.1 - near.1).abs(),
                    );
                });

                if alpha.is_some() {
                    ctx.set_global_alpha(base_alpha);
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
        // Reused across runs and repeats so a fade costs no allocation per
        // frame. An un-faded image span gets exactly one slice, which is the
        // same single blit it emitted before fades existed.
        let mut pieces: Vec<(f64, f64, f64)> = Vec::new();

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
                Source::Image {
                    region,
                    fit,
                    fade,
                    drift_cells,
                } => {
                    let Some(region) = self.atlas.region(*region) else {
                        ctx.restore();
                        continue;
                    };
                    let Some(image) = self.atlas.handle(region.image) else {
                        ctx.restore();
                        continue;
                    };
                    // The row of a sprite sheet, or the frame of a strip, or
                    // the whole region for still art. This is the only place
                    // the clock touches an image layer, and it moves numbers
                    // rather than op structure.
                    let (sx, sy, sw, sh) = region.source_rect(frame.time_turns);
                    let faded = fade.is_some_and(|fade| !fade.is_noop());

                    let failed = match fit {
                        // A texture: as many repeats as this run's slice holds.
                        crate::skin::layer::Fit::Tile { cells_per_repeat } => {
                            // One repeat covers this many cells of body.
                            // Defaulting to the region's aspect keeps a
                            // texture's proportions when the author has said
                            // nothing; naming it lets one PNG be worn coarse or
                            // fine without being redrawn.
                            let repeat_cells = cells_per_repeat
                                .unwrap_or_else(|| sw / sh.max(1e-6))
                                .max(1e-6);
                            // Repeats are numbered from the span's start in arc
                            // length, so this run picks up exactly where the
                            // previous one left off and a corner is invisible
                            // to the pattern.
                            // Whether the skin *declares* drift, not whether the
                            // phase happens to be zero right now. Branching on
                            // the phase costs a blit at exactly the frames where
                            // it lands on zero — and that is frame one, so the
                            // very first sample would disagree with every other.
                            let drifts = drift_cells.is_finite() && *drift_cells != 0.0;
                            let phase = drift_phase(*drift_cells, repeat_cells, frame.time_turns);
                            // Scratch for the two-blit split, so a drifting
                            // pattern allocates nothing per frame.
                            let mut pair;
                            let first = ((start - allocation.start) / repeat_cells).floor();
                            let last = ((end - allocation.start) / repeat_cells).ceil();
                            let mut index = first;
                            let mut failed = None;
                            while index < last && failed.is_none() {
                                let repeat_start = allocation.start + index * repeat_cells;
                                index += 1.0;
                                let from = repeat_start.max(start);
                                let to = (repeat_start + repeat_cells).min(end);
                                if to <= from {
                                    continue;
                                }
                                fade_pieces(fade.as_ref(), allocation, from, to, &mut pieces);
                                for &(a, b, alpha) in &pieces {
                                    if faded {
                                        ctx.set_global_alpha(base_alpha * alpha);
                                    }
                                    // The fraction of the repeat this piece
                                    // covers is the fraction of the region it
                                    // samples, so a repeat cut short by a
                                    // corner — or by a fade slice — is a
                                    // sub-rect rather than a whole tile
                                    // squashed into the gap.
                                    let u_from = (a - repeat_start) / repeat_cells;
                                    let u_to = (b - repeat_start) / repeat_cells;
                                    // A still pattern is one blit, exactly as
                                    // before drift existed, so every shipped
                                    // golden is untouched. A drifting one is
                                    // always two, split so that both halves are
                                    // non-empty whether or not the sample range
                                    // actually wraps — an empty blit would be
                                    // both illegal and a change of op count.
                                    let cuts: &[(f64, f64, f64)] = if !drifts {
                                        &[(u_from, u_to, 0.0)]
                                    } else {
                                        let wraps = u_from + phase < 1.0 && u_to + phase > 1.0;
                                        let split = if wraps {
                                            1.0 - phase
                                        } else {
                                            (u_from + u_to) / 2.0
                                        };
                                        // Past the wrap the sample offset is one
                                        // whole repeat further back.
                                        let shift = if u_from + phase >= 1.0 {
                                            phase - 1.0
                                        } else {
                                            phase
                                        };
                                        pair = [
                                            (u_from, split, shift),
                                            (split, u_to, if wraps { phase - 1.0 } else { shift }),
                                        ];
                                        &pair
                                    };
                                    for &(from_u, to_u, offset) in cuts {
                                        failed = ctx
                                            .draw_image(
                                                image,
                                                (
                                                    sx + (from_u + offset) * sw,
                                                    sy,
                                                    (to_u - from_u) * sw,
                                                    sh,
                                                ),
                                                (
                                                    repeat_start + from_u * repeat_cells - run.s0,
                                                    -0.5,
                                                    (to_u - from_u) * repeat_cells,
                                                    1.0,
                                                ),
                                            )
                                            .err();
                                        if failed.is_some() {
                                            break;
                                        }
                                    }
                                    if failed.is_some() {
                                        break;
                                    }
                                }
                            }
                            failed
                        }
                        // A picture at authored scale, drawn taller than the
                        // body so the silhouette clip trims it. The overflow
                        // costs nothing: it is clipped, never rasterised.
                        crate::skin::layer::Fit::Cutout { cells_tall } => {
                            let tall = cells_tall.max(1e-6);
                            // One cell of body is `sh / tall` source pixels,
                            // in **both** axes — that is what "authored scale"
                            // means and why the picture comes out undistorted.
                            let source_per_cell = sh / tall;
                            fade_pieces(fade.as_ref(), allocation, start, end, &mut pieces);
                            let mut failed = None;
                            for &(a, b, alpha) in &pieces {
                                let offset = (a - allocation.start) * source_per_cell;
                                let (slice_x, slice_w) = (sx + offset, (b - a) * source_per_cell);
                                let clipped_w = slice_w.min((sx + sw - slice_x).max(0.0));
                                if clipped_w <= 0.0 {
                                    continue;
                                }
                                if faded {
                                    ctx.set_global_alpha(base_alpha * alpha);
                                }
                                let drawn_cells = clipped_w / source_per_cell.max(1e-6);
                                failed = ctx
                                    .draw_image(
                                        image,
                                        (slice_x, sy, clipped_w, sh),
                                        (a - run.s0, -tall / 2.0, drawn_cells, tall),
                                    )
                                    .err();
                                if failed.is_some() {
                                    break;
                                }
                            }
                            failed
                        }
                        // A sprite: one blit, at natural scale or squeezed.
                        fit => {
                            // How much of the source one cell of body consumes.
                            // `Clip` keeps the art's proportions by pinning that
                            // to the region's own aspect; `Stretch` divides the
                            // whole source across the whole span instead.
                            let source_per_cell = match fit {
                                crate::skin::layer::Fit::Stretch => {
                                    sw / (allocation.end - allocation.start).max(1e-6)
                                }
                                _ => sh,
                            };

                            fade_pieces(fade.as_ref(), allocation, start, end, &mut pieces);
                            let mut failed = None;
                            for &(a, b, alpha) in &pieces {
                                // Where this slice sits within the span, in
                                // source pixels.
                                let offset = (a - allocation.start) * source_per_cell;
                                let (slice_x, slice_w) = (sx + offset, (b - a) * source_per_cell);
                                // Never sample past the region: its neighbour's
                                // pixels are one bilinear tap away, which is
                                // what the padding rule in `skin::atlas` exists
                                // to make survivable rather than to rely on.
                                let clipped_w = slice_w.min((sx + sw - slice_x).max(0.0));
                                if clipped_w <= 0.0 {
                                    continue;
                                }
                                if faded {
                                    ctx.set_global_alpha(base_alpha * alpha);
                                }
                                let drawn_cells = clipped_w / source_per_cell.max(1e-6);
                                failed = ctx
                                    .draw_image(
                                        image,
                                        (slice_x, sy, clipped_w, sh),
                                        (a - run.s0, -0.5, drawn_cells, 1.0),
                                    )
                                    .err();
                                if failed.is_some() {
                                    break;
                                }
                            }
                            failed
                        }
                    };

                    if let Some(cause) = failed {
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
            // Clamped because `addColorStop` throws outside 0..1 rather than
            // clipping, and a bound offset is an author's arithmetic — the
            // travelling shine in `specs/skin-layer-documents-prd.md` runs its
            // crest off both ends of the span by design.
            offset: stop.offset.get(&frame.params, 0.0).clamp(0.0, 1.0),
            color: rgba_of(
                color(stop.color, swatch, frame, config),
                stop.alpha.get(&frame.params, 1.0).clamp(0.0, 1.0),
            ),
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
        id: std::borrow::Cow::Borrowed(id),
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
        opacity: Binding::ONE,
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
            params: vec![1.0],
            literals: vec!["#ffffff".to_string()],
        }
    }

    fn ribbon(id: &'static str, region: Region, extra: f64) -> Layer {
        Layer {
            id: std::borrow::Cow::Borrowed(id),
            region,
            clip: ClipShape::Silhouette,
            kind: LayerKind::Ribbon {
                color: ColorSlot::Outline,
                extra,
                joints: true,
                tail_cap: false,
                fill_before_strokes: false,
                refill_before_tail_cap: false,
                single_pass: false,
            },
            transform: LayerTransform::default(),
            boost_only: false,
            omit_on_single_cell: false,
            opacity: Binding::ONE,
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
            radius: Binding::Const(0.9),
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
                fade: None,
                drift_cells: 0.0,
            },
        );
        layer.kind = LayerKind::Span {
            span: Span::WHOLE,
            source: Source::Image {
                region: 0,
                fit: crate::skin::layer::Fit::Clip,
                fade: None,
                drift_cells: 0.0,
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
                half_width: Binding::Const(0.5),
                t_center: Binding::ZERO,
                phase_cells: 0.0,
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
                detail_scale: 1.0,
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
                    detail_scale: 1.0,
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

    fn band_layer(t_center: f64, phase_cells: f64, half_width: f64) -> Layer {
        span_layer(
            "band",
            Region::Body,
            Span::WHOLE,
            Source::Tiled {
                color: ColorSlot::Fill,
                period_cells: 1.0,
                duty: 0.5,
                half_width: Binding::Const(half_width),
                t_center: Binding::Const(t_center),
                phase_cells,
                alpha: None,
            },
        )
    }

    fn band_rects(layer: Layer, cells: &[(f64, f64)]) -> Vec<(f64, f64, f64, f64)> {
        let skin = CompositeSkin::new(
            "band@test",
            "Band",
            vec![layer],
            vec![frame()],
            1_000.0,
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
        .expect("a single body band is a valid stack");

        let mut recorder = crate::skin::paint::OpRecorder::new();
        skin.paint_alive(
            &mut PaintCtx::recording(&mut recorder),
            &SnakePose {
                cells,
                cell_size: 10.0,
                boost_active: false,
                anim_ms: 0.0,
                reduced_motion: true,
                detail_scale: 1.0,
            },
            &SkinIdentity {
                role: SnakeRole::Own,
                shade_slot: 0,
            },
        )
        .expect("a recording painter cannot fail");

        recorder
            .ops()
            .iter()
            .filter_map(|op| match op {
                crate::skin::paint::PaintOp::FillRect(x, y, w, h) => Some((*x, *y, *w, *h)),
                _ => None,
            })
            .collect()
    }

    /// The two degrees of freedom a checkerboard needs, and the reason they are
    /// on `Tiled` rather than in a bespoke source: one band offset across the
    /// body and half a period along it is the *other* half of the pattern.
    ///
    /// A horizontal snake at cell 10 occupies ten pixels across, so the lane a
    /// band paints is directly readable in the recorded rectangles.
    #[test]
    fn a_band_paints_its_own_lane_across_the_body() {
        let cells = [(0.0, 0.0), (5.0, 0.0)];

        // The body sits on row y = 0, so at cell 10 it occupies pixels 0..10
        // and its centreline is at 5.

        // Centred: the classic single band, spanning the full width.
        let centred = band_rects(band_layer(0.0, 0.0, 0.5), &cells);
        assert!(!centred.is_empty());
        for (_, y, _, height) in &centred {
            assert!((*y - 0.0).abs() < 1e-9, "centred band moved: y = {y}");
            assert!((*height - 10.0).abs() < 1e-9);
        }

        // A quarter-cell lane either side of the centreline: two rows, each
        // half the width, meeting exactly at the middle and nowhere else.
        let near = band_rects(band_layer(-0.25, 0.0, 0.25), &cells);
        let far = band_rects(band_layer(0.25, 0.0, 0.25), &cells);
        assert!(!near.is_empty() && !far.is_empty());
        for (_, y, _, height) in &near {
            assert!((*y - 0.0).abs() < 1e-9, "near row is not against the top");
            assert!((*height - 5.0).abs() < 1e-9);
        }
        for (_, y, _, height) in &far {
            assert!((*y - 5.0).abs() < 1e-9, "far row is not below the middle");
            assert!((*height - 5.0).abs() < 1e-9);
        }
    }

    #[test]
    fn a_phase_offset_interleaves_two_bands_instead_of_stacking_them() {
        let cells = [(0.0, 0.0), (5.0, 0.0)];
        let unshifted = band_rects(band_layer(-0.25, 0.0, 0.25), &cells);
        let shifted = band_rects(band_layer(0.25, 0.5, 0.25), &cells);

        let starts =
            |rects: &[(f64, f64, f64, f64)]| rects.iter().map(|(x, ..)| *x).collect::<Vec<_>>();
        let (a, b) = (starts(&unshifted), starts(&shifted));
        assert!(!a.is_empty() && !b.is_empty());
        for start in &a {
            assert!(
                !b.iter().any(|other| (other - start).abs() < 1e-9),
                "the two rows share a tile at x = {start}, so this is stripes \
                 rather than a checkerboard"
            );
        }
        // Half a period apart, in pixels, at cell 10.
        assert!(
            b.iter().any(|other| (other - (a[0] + 5.0)).abs() < 1e-9),
            "the shifted row is not half a period along: {a:?} vs {b:?}"
        );
    }

    /// The lane bound is declarative so registration can enforce it. Leaving it
    /// to the silhouette clip would mean the layer's declared shape no longer
    /// described what it paints, which is the thing `overhang_px` is computed
    /// from rather than measured.
    #[test]
    fn a_band_that_would_reach_past_the_body_is_rejected_at_registration() {
        let problems = validate_layers(&[band_layer(0.4, 0.0, 0.25)], &[frame()]);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].problem.contains("t_center"), "{problems:?}");

        assert!(
            validate_layers(&[band_layer(0.25, 0.0, 0.25)], &[frame()]).is_empty(),
            "a lane that exactly reaches the edge is legal"
        );
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
            id: std::borrow::Cow::Borrowed(id),
            region: Region::Body,
            clip: ClipShape::Silhouette,
            kind: LayerKind::Span {
                span: Span {
                    from,
                    natural,
                    min,
                    priority,
                },
                source: Source::Image {
                    region: 0,
                    fit,
                    fade: None,
                    drift_cells: 0.0,
                },
                corner: CornerPolicy::Own,
            },
            transform: LayerTransform::default(),
            boost_only: false,
            omit_on_single_cell: false,
            opacity: Binding::ONE,
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
            image_layer("mid", Anchor::Head, None, 1.0, 0, Fit::TILE),
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
            crate::skin::atlas::Atlas::new(
                ["images/skins/example.v1.png".to_string()],
                vec![region(0.0)],
            ),
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

    /// A texture and the body it covers, as `(source, dest)` blits.
    fn textured_skin(cells_per_repeat: Option<f64>) -> CompositeSkin {
        CompositeSkin::with_atlas(
            "coat@test",
            "Coat",
            vec![
                span_layer(
                    "base",
                    Region::Body,
                    Span::WHOLE,
                    Source::Solid(ColorSlot::Fill),
                ),
                span_layer(
                    "coat",
                    Region::Body,
                    Span::WHOLE,
                    Source::Image {
                        region: 0,
                        fit: Fit::Tile { cells_per_repeat },
                        fade: None,
                        drift_cells: 0.0,
                    },
                ),
            ],
            vec![frame()],
            1_000.0,
            CompositeConfig {
                boost_color: "#fff200".to_string(),
                head_core_color: "#333333".to_string(),
                head_core_ratio: 0.38,
                head_core_is_dark: true,
                wave: None,
            },
            crate::skin::atlas::Atlas::new(
                ["images/skins/example.v1.png".to_string()],
                // 192x16: twelve cells long at its own proportions.
                vec![crate::skin::atlas::AtlasRegion {
                    image: 0,
                    x: 0.0,
                    y: 0.0,
                    width: 192.0,
                    height: 16.0,
                    frames: None,
                }],
            ),
            None,
            None,
        )
        .expect("a coat over a base is a valid stack")
    }

    fn blits(skin: &CompositeSkin, cells: &[(f64, f64)]) -> Vec<((f64, f64), f64)> {
        let mut recorder = crate::skin::paint::OpRecorder::new();
        skin.paint_alive(
            &mut PaintCtx::recording(&mut recorder),
            &SnakePose::still(cells, 10.0, false),
            &SkinIdentity {
                role: SnakeRole::Own,
                shade_slot: 0,
            },
        )
        .expect("a recording painter cannot fail");

        recorder
            .ops()
            .iter()
            .filter_map(|op| match op {
                // The source rectangle as a fraction of the region, which is
                // what says *where in the pattern* a fragment came from, and
                // the destination's width in cells.
                crate::skin::paint::PaintOp::DrawImage {
                    source: (sx, _, sw, _),
                    dest: (_, _, dw, _),
                    ..
                } => Some(((sx / 192.0, (sx + sw) / 192.0), *dw)),
                _ => None,
            })
            .collect()
    }

    /// The whole point of a texture: it covers a body of any length.
    ///
    /// Before this, a tiling fit drew one pass and then clipped at the region's
    /// right edge, so a 21-cell snake wearing a 6-cell coat was patterned for
    /// six cells and bare for fifteen. Nothing caught it because no shipped skin
    /// had an image layer at all.
    #[test]
    fn a_texture_repeats_until_the_body_is_covered() {
        let skin = textured_skin(Some(6.0));
        let painted = blits(&skin, &[(0.0, 0.0), (20.0, 0.0)]);

        // 21 cells of paint over a 6-cell repeat: three whole repeats and a
        // half.
        assert_eq!(painted.len(), 4, "{painted:?}");
        let widths: Vec<f64> = painted.iter().map(|(_, width)| *width).collect();
        assert_eq!(widths, vec![6.0, 6.0, 6.0, 3.0]);
        assert_eq!(
            widths.iter().sum::<f64>(),
            21.0,
            "the coat has to reach the tail"
        );

        // Each whole repeat samples the whole region; the last samples the
        // first half of it rather than a squashed whole.
        for (index, ((from, to), _)) in painted.iter().enumerate() {
            let expected_to = if index == 3 { 0.5 } else { 1.0 };
            assert!(
                from.abs() < 1e-9 && (to - expected_to).abs() < 1e-9,
                "repeat {index} sampled {from}..{to} of the texture"
            );
        }
    }

    /// A repeat length is the author's, not the PNG's — and with none given it
    /// is the PNG's. Both have to be true, or a texture can only ever be worn
    /// at whatever scale it happened to be drawn at.
    #[test]
    fn a_texture_repeats_at_its_declared_length_or_its_own_proportions() {
        let body = [(0.0, 0.0), (11.0, 0.0)];
        let declared = blits(&textured_skin(Some(3.0)), &body);
        assert_eq!(declared.len(), 4, "twelve cells hold four 3-cell repeats");

        // 192x16 is twelve cells long at its own proportions, so it covers the
        // same body exactly once.
        let natural = blits(&textured_skin(None), &body);
        assert_eq!(natural.len(), 1, "{natural:?}");
        assert_eq!(natural[0].1, 12.0);
    }

    /// A coat does not restart at every turn.
    ///
    /// A corner splits a repeat into two blits — the runs are separate
    /// transforms — but the second has to continue the first's sample of the
    /// texture. Restarting instead would make a zigzagging snake wear a
    /// visibly different pattern from a straight one, which is the failure
    /// arc-length anchoring exists to prevent.
    #[test]
    fn a_texture_crosses_a_corner_without_restarting() {
        let skin = textured_skin(Some(6.0));
        let cornered = blits(&skin, &[(0.0, 0.0), (8.0, 0.0), (8.0, 8.0)]);
        assert!(cornered.len() > 3, "{cornered:?}");

        let mut previous: Option<f64> = None;
        for ((from, to), _) in &cornered {
            if let Some(previous) = previous {
                let continues = (from - previous).abs() < 1e-9;
                let wrapped = previous > 1.0 - 1e-9 && from.abs() < 1e-9;
                assert!(
                    continues || wrapped,
                    "a fragment starting at {from} followed one ending at \
                     {previous}: the pattern jumped rather than continuing"
                );
            }
            previous = Some(*to);
        }

        // ...and the corner costs coverage nothing.
        let covered: f64 = cornered.iter().map(|(_, width)| width).sum();
        assert!(
            (covered - 17.0).abs() < 1e-9,
            "a 17-cell cornered body got {covered} cells of coat"
        );
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
                fade: None,
                drift_cells: 0.0,
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
            crate::skin::atlas::Atlas::default(),
            None,
            None,
        )
        .err()
        .expect("region 7 does not exist");
        assert_eq!(problems.len(), 1);
        assert!(problems[0].problem.contains("does not exist"));
    }

    /// The other half of the same rule. A region is a rectangle *inside* an
    /// image, so a region whose image was never declared is just as broken as a
    /// region that does not exist — and just as invisible until someone wears
    /// the skin.
    #[test]
    fn an_atlas_region_must_name_an_image_the_skin_declares() {
        let layer = span_layer(
            "coat",
            Region::Body,
            Span::WHOLE,
            Source::Image {
                region: 0,
                fit: Fit::TILE,
                fade: None,
                drift_cells: 0.0,
            },
        );
        let problems = CompositeSkin::with_atlas(
            "orphan@test",
            "Orphan",
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
            crate::skin::atlas::Atlas::new(
                Vec::new(),
                vec![crate::skin::atlas::AtlasRegion {
                    image: 0,
                    x: 0.0,
                    y: 0.0,
                    width: 64.0,
                    height: 16.0,
                    frames: None,
                }],
            ),
            None,
            None,
        )
        .err()
        .expect("image 0 was never declared");
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].problem.contains("does not declare"),
            "{problems:?}"
        );
    }

    /// The same hazard by the other route. With no declared repeat length the
    /// region's own aspect becomes one, so an empty rectangle asks the paint
    /// loop to cover a body in repeats of nothing.
    #[test]
    fn an_atlas_region_with_no_pixels_is_rejected() {
        let region = |width: f64, height: f64| crate::skin::atlas::AtlasRegion {
            image: 0,
            x: 0.0,
            y: 0.0,
            width,
            height,
            frames: None,
        };
        let build = |width, height| {
            CompositeSkin::with_atlas(
                "empty@test",
                "Empty",
                vec![span_layer(
                    "coat",
                    Region::Body,
                    Span::WHOLE,
                    Source::Image {
                        region: 0,
                        fit: Fit::TILE,
                        fade: None,
                        drift_cells: 0.0,
                    },
                )],
                vec![frame()],
                1000.0,
                CompositeConfig {
                    boost_color: "#fff200".to_string(),
                    head_core_color: "#333333".to_string(),
                    head_core_ratio: 0.38,
                    head_core_is_dark: true,
                    wave: None,
                },
                crate::skin::atlas::Atlas::new(
                    ["images/skins/example.v1.png".to_string()],
                    vec![region(width, height)],
                ),
                None,
                None,
            )
        };

        for (width, height) in [(0.0, 16.0), (64.0, 0.0), (-8.0, 16.0), (f64::NAN, 16.0)] {
            let Err(problems) = build(width, height) else {
                panic!("a {width}x{height} region was accepted");
            };
            assert!(problems[0].problem.contains("no pixels"), "{problems:?}");
        }
        assert!(build(64.0, 16.0).is_ok());
    }

    /// A repeat length divides the span on the paint path and sets how many
    /// blits a body costs. Zero repeats forever; a NaN never terminates. Both
    /// are caught before the skin has painted once.
    #[test]
    fn a_tiled_texture_must_repeat_over_a_sane_number_of_cells() {
        let layer = |cells: Option<f64>| {
            span_layer(
                "coat",
                Region::Body,
                Span::WHOLE,
                Source::Image {
                    region: 0,
                    fit: Fit::Tile {
                        cells_per_repeat: cells,
                    },
                    fade: None,
                    drift_cells: 0.0,
                },
            )
        };

        for bad in [0.0, -3.0, f64::NAN, f64::INFINITY] {
            let problems = validate_layers(&[layer(Some(bad))], &[frame()]);
            assert_eq!(problems.len(), 1, "{bad} was accepted: {problems:?}");
            assert!(problems[0].problem.contains("positive, finite"));
        }
        assert!(validate_layers(&[layer(Some(6.0))], &[frame()]).is_empty());
        assert!(
            validate_layers(&[layer(None)], &[frame()]).is_empty(),
            "no declared length means the region's own aspect, which is always sane"
        );
    }

    // -----------------------------------------------------------------
    // Sprite sheets: rows are time, columns are body.

    /// A square sheet worn as a sprite: `rows` rows, one baked frame each.
    fn sheet_skin(
        rows: usize,
        fit: Fit,
        fade: Option<Fade>,
        span: Span,
        drift: f64,
    ) -> CompositeSkin {
        let frames: Vec<Frame> = (0..rows)
            .map(|row| Frame {
                time_turns: row as f64 / rows as f64,
                ..frame()
            })
            .collect();
        CompositeSkin::with_atlas(
            "sheet@test",
            "Sheet",
            vec![
                span_layer(
                    "base",
                    Region::Body,
                    Span::WHOLE,
                    Source::Solid(ColorSlot::Fill),
                ),
                span_layer(
                    "sprite",
                    Region::Body,
                    span,
                    Source::Image {
                        region: 0,
                        fit,
                        fade,
                        drift_cells: drift,
                    },
                ),
            ],
            frames,
            1_000.0,
            CompositeConfig {
                boost_color: "#fff200".to_string(),
                head_core_color: "#333333".to_string(),
                head_core_ratio: 0.38,
                head_core_is_dark: true,
                wave: None,
            },
            crate::skin::atlas::Atlas::new(
                ["images/skins/sheet.v1.png".to_string()],
                // Square, so it is exactly as many cells long as it has rows.
                vec![crate::skin::atlas::AtlasRegion::sheet(
                    0, 400.0, 400.0, rows,
                )],
            ),
            None,
            None,
        )
        .expect("a well-formed sheet skin")
    }

    /// Every op, with the alpha in force when each blit was emitted.
    fn blits_with_alpha(
        skin: &CompositeSkin,
        cells: &[(f64, f64)],
        anim_ms: f64,
    ) -> (Vec<(f64, f64, f64, f64)>, usize) {
        let mut recorder = crate::skin::paint::OpRecorder::new();
        skin.paint_alive(
            &mut PaintCtx::recording(&mut recorder),
            &SnakePose {
                cells,
                cell_size: 10.0,
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

        let mut alpha = 1.0;
        let mut out = Vec::new();
        for op in recorder.ops() {
            match op {
                crate::skin::paint::PaintOp::SetGlobalAlpha(value) => alpha = *value,
                // Source y says which row is playing; dest x and width say
                // where along the body it landed.
                crate::skin::paint::PaintOp::DrawImage {
                    source: (_, sy, _, _),
                    dest: (dx, _, dw, _),
                    ..
                } => out.push((*sy, *dx, *dw, alpha)),
                _ => {}
            }
        }
        (out, recorder.ops().len())
    }

    /// The core of the sprite-sheet model: `y` is time. Each baked frame plays
    /// the next row, in order, and the op stream is byte-identical in shape —
    /// the row is an argument to the same single blit, which is what keeps an
    /// animated skin exactly as expensive as a still one.
    #[test]
    fn a_sprite_sheet_plays_one_row_per_frame_without_changing_the_op_stream() {
        let rows = 8;
        let skin = sheet_skin(rows, Fit::TILE, None, Span::WHOLE, 0.0);
        let body = [(0.0, 0.0), (7.0, 0.0)];

        let mut seen = Vec::new();
        let mut counts = Vec::new();
        for step in 0..rows {
            // `period_ms` is 1000 and there are `rows` frames, so this lands
            // one frame per step.
            let (blits, ops) = blits_with_alpha(&skin, &body, step as f64 * 1000.0 / rows as f64);
            assert!(!blits.is_empty(), "row {step} painted nothing");
            seen.push(blits[0].0);
            counts.push(ops);
        }

        assert_eq!(
            seen,
            (0..rows).map(|row| row as f64 * 50.0).collect::<Vec<_>>(),
            "rows must play in order, one per frame"
        );
        assert!(
            counts.windows(2).all(|pair| pair[0] == pair[1]),
            "the clock changed the op count: {counts:?}"
        );

        // And a viewer who asked for less motion gets row zero, forever.
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
        let still = recorder.ops().iter().find_map(|op| match op {
            crate::skin::paint::PaintOp::DrawImage {
                source: (_, sy, _, _),
                ..
            } => Some(*sy),
            _ => None,
        });
        assert_eq!(still, Some(0.0), "reduced motion must pin the first row");
    }

    /// Drift slides the pattern and must not change what it costs.
    ///
    /// Sliding the repeats themselves would be the obvious implementation and
    /// it fails conformance: the number overlapping the body changes as the
    /// phase crosses a boundary. Drifting the *sample* keeps the count fixed —
    /// two blits per repeat, always, whether or not the range wraps.
    #[test]
    fn a_drifting_tile_moves_the_pattern_without_moving_the_repeats() {
        let rows = 8;
        let still = sheet_skin(rows, Fit::TILE, None, Span::WHOLE, 0.0);
        let drifting = sheet_skin(rows, Fit::TILE, None, Span::WHOLE, 2.0);
        let body = [(0.0, 0.0), (23.0, 0.0)];

        // Every clock costs the same, and costs exactly twice the still skin.
        let counts: Vec<usize> = (0..rows)
            .map(|step| {
                blits_with_alpha(&drifting, &body, step as f64 * 125.0)
                    .0
                    .len()
            })
            .collect();
        assert!(
            counts.windows(2).all(|pair| pair[0] == pair[1]),
            "drift changed the blit count with the clock: {counts:?}"
        );
        assert_eq!(
            counts[0],
            blits_with_alpha(&still, &body, 0.0).0.len() * 2,
            "a drifting repeat is exactly two blits"
        );

        // The destination grid never moves; only the sampling does.
        let starts = |ms: f64| -> Vec<f64> {
            let mut edges: Vec<f64> = blits_with_alpha(&drifting, &body, ms)
                .0
                .iter()
                .map(|(_, dx, _, _)| (dx * 1e6).round() / 1e6)
                .collect();
            edges.dedup();
            edges
        };
        assert_ne!(starts(0.0).len(), 0);

        // ...and the pattern genuinely moves: the source offset of the first
        // blit differs between frames.
        let sample_at = |ms: f64| {
            let mut recorder = crate::skin::paint::OpRecorder::new();
            drifting
                .paint_alive(
                    &mut PaintCtx::recording(&mut recorder),
                    &SnakePose {
                        cells: &body,
                        cell_size: 10.0,
                        boost_active: false,
                        anim_ms: ms,
                        reduced_motion: false,
                        detail_scale: 1.0,
                    },
                    &SkinIdentity {
                        role: SnakeRole::Own,
                        shade_slot: 0,
                    },
                )
                .expect("a recording painter cannot fail");
            recorder.ops().iter().find_map(|op| match op {
                crate::skin::paint::PaintOp::DrawImage {
                    source: (sx, _, _, _),
                    ..
                } => Some(*sx),
                _ => None,
            })
        };
        assert_ne!(
            sample_at(0.0),
            sample_at(375.0),
            "the pattern did not move at all"
        );
    }

    /// A head-pinned sprite that does not clothe the whole snake has to end
    /// somewhere, and the fade is what stops that being a hard vertical line.
    #[test]
    fn a_fade_ramps_alpha_to_nothing_over_the_declared_cells() {
        let rows = 20;
        let span = Span {
            from: Anchor::Head,
            natural: Some(20.0),
            min: 4.0,
            priority: 10,
        };
        let skin = sheet_skin(rows, Fit::Clip, Some(Fade::trailing(6.0, 12)), span, 0.0);
        // Long enough that the sprite genuinely ends before the tail does.
        let (blits, _) = blits_with_alpha(&skin, &[(0.0, 0.0), (34.0, 0.0)], 0.0);
        assert!(blits.len() > 1, "a fade must be drawn in slices: {blits:?}");

        // The span runs -0.5 ..= 19.5, so the ramp starts at 13.5.
        let (opaque, ramp): (Vec<_>, Vec<_>) =
            blits.iter().partition(|(_, dx, _, _)| *dx < 13.5 - 1e-9);
        assert!(
            opaque.iter().all(|(_, _, _, alpha)| *alpha == 1.0),
            "the sprite must be solid before the ramp: {opaque:?}"
        );
        assert_eq!(ramp.len(), 12, "one slice per declared step: {ramp:?}");

        let alphas: Vec<f64> = ramp.iter().map(|(_, _, _, alpha)| *alpha).collect();
        assert!(
            alphas.windows(2).all(|pair| pair[0] > pair[1]),
            "the ramp must only ever descend: {alphas:?}"
        );
        assert!(
            alphas[0] < 1.0 && alphas[alphas.len() - 1] < 0.05,
            "the ramp must start below solid and finish at nothing: {alphas:?}"
        );
        // The art still reaches exactly as far as the span, no further.
        let reach = ramp.last().map(|(_, dx, dw, _)| dx + dw).unwrap_or(0.0);
        assert!((reach - 19.5).abs() < 1e-6, "the fade ended at {reach}");
    }

    /// Two independent reasons to be translucent have to multiply. If the fade
    /// simply *set* the alpha, a skin animating a sprite's opacity would find
    /// the fade quietly cancelling it — and only over the faded cells, which is
    /// the kind of bug that gets blamed on the art.
    #[test]
    fn a_fade_multiplies_the_layer_opacity_rather_than_replacing_it() {
        let rows = 20;
        let span = Span {
            from: Anchor::Head,
            natural: Some(20.0),
            min: 4.0,
            priority: 10,
        };
        let mut skin = sheet_skin(rows, Fit::Clip, Some(Fade::trailing(6.0, 4)), span, 0.0);
        // Half-opacity layer: `frame()` already carries one opacity track.
        skin.layers[1].opacity = Binding::Param(0);
        for frame in &mut skin.frames {
            frame.params = vec![0.5];
        }

        let (blits, _) = blits_with_alpha(&skin, &[(0.0, 0.0), (34.0, 0.0)], 0.0);
        let alphas: Vec<f64> = blits.iter().map(|(_, _, _, alpha)| *alpha).collect();
        assert!(
            alphas.iter().all(|alpha| *alpha <= 0.5 + 1e-9),
            "nothing may paint above the layer's own opacity: {alphas:?}"
        );
        assert!(
            alphas.iter().any(|alpha| (alpha - 0.5).abs() < 1e-9),
            "the unfaded part must paint at exactly the layer opacity: {alphas:?}"
        );
    }

    /// The ramp is laid out over the allocation, not over each piece drawn, so
    /// a corner — which splits the sprite into separate runs with separate
    /// transforms — does not restart it. Getting this wrong reads as banding
    /// that only appears on turning snakes.
    #[test]
    fn a_fade_is_one_ramp_across_a_corner() {
        let rows = 20;
        let span = Span {
            from: Anchor::Head,
            natural: Some(20.0),
            min: 4.0,
            priority: 10,
        };
        let skin = sheet_skin(rows, Fit::Clip, Some(Fade::trailing(8.0, 8)), span, 0.0);
        let straight = blits_with_alpha(&skin, &[(0.0, 0.0), (30.0, 0.0)], 0.0).0;
        let cornered = blits_with_alpha(&skin, &[(0.0, 0.0), (16.0, 0.0), (16.0, 14.0)], 0.0).0;

        let ramp = |blits: &[(f64, f64, f64, f64)]| -> Vec<f64> {
            let mut alphas: Vec<f64> = blits
                .iter()
                .map(|(_, _, _, alpha)| *alpha)
                .filter(|alpha| *alpha < 1.0 - 1e-9)
                .collect();
            alphas.dedup_by(|a, b| (*a - *b).abs() < 1e-9);
            alphas
        };
        assert_eq!(
            ramp(&straight),
            ramp(&cornered),
            "the corner produced a different ramp"
        );
        assert_eq!(ramp(&straight).len(), 8, "one ramp, not two");
    }

    /// A span degraded by a short body is exactly when a hard edge would show,
    /// so it is the last moment to give up the fade. Both ramps shrink together
    /// rather than one of them eating the whole span.
    #[test]
    fn a_short_body_squeezes_both_ramps_instead_of_dropping_one() {
        let allocation = Allocation {
            start: 0.0,
            end: 4.0,
        };
        let fade = Fade {
            lead_cells: 3.0,
            trail_cells: 9.0,
            steps: 3,
        };
        let mut pieces = Vec::new();
        fade_pieces(Some(&fade), allocation, 0.0, 4.0, &mut pieces);

        let covered: f64 = pieces.iter().map(|(from, to, _)| to - from).sum();
        assert!(
            (covered - 4.0).abs() < 1e-9,
            "the slices must still cover the span exactly: {pieces:?}"
        );
        // 3:9 of a 4-cell span is 1 cell of lead and 3 of trail.
        let lead: f64 = pieces
            .iter()
            .take(3)
            .map(|(from, to, _)| to - from)
            .sum::<f64>();
        assert!((lead - 1.0).abs() < 1e-9, "{pieces:?}");
        assert!(
            pieces.first().map(|(_, _, a)| *a) < pieces.get(2).map(|(_, _, a)| *a),
            "the lead ramp must rise: {pieces:?}"
        );
        assert!(
            pieces.last().map(|(_, _, a)| *a).unwrap() < 0.2,
            "the trail ramp must still reach nothing: {pieces:?}"
        );
    }

    /// A fade costs blits, and the one number that could make that unaffordable
    /// is caught where every other layer mistake is: at registration.
    #[test]
    fn a_fade_must_declare_an_affordable_number_of_steps() {
        let build = |fade: Fade| {
            CompositeSkin::with_atlas(
                "sheet@test",
                "Sheet",
                vec![span_layer(
                    "sprite",
                    Region::Body,
                    Span::WHOLE,
                    Source::Image {
                        region: 0,
                        fit: Fit::Clip,
                        fade: Some(fade),
                        drift_cells: 0.0,
                    },
                )],
                vec![frame()],
                1_000.0,
                CompositeConfig {
                    boost_color: "#fff200".to_string(),
                    head_core_color: "#333333".to_string(),
                    head_core_ratio: 0.38,
                    head_core_is_dark: true,
                    wave: None,
                },
                crate::skin::atlas::Atlas::new(
                    ["images/skins/sheet.v1.png".to_string()],
                    vec![crate::skin::atlas::AtlasRegion::sheet(0, 400.0, 400.0, 20)],
                ),
                None,
                None,
            )
        };

        assert!(build(Fade::trailing(6.0, 12)).is_ok());
        for bad in [0, 65, 4096] {
            let problems = build(Fade::trailing(6.0, bad))
                .err()
                .unwrap_or_else(|| panic!("{bad} steps should be rejected"));
            assert!(problems[0].problem.contains("steps"), "{problems:?}");
        }
        // A fade of no length is a no-op, not a mistake — it is what an author
        // gets by turning the numbers down, and it must not fail registration.
        assert!(build(Fade::trailing(0.0, 0)).is_ok());
        assert!(build(Fade::trailing(f64::NAN, 8)).is_err());
    }

    /// The corner fix, asserted on the ops rather than on the arithmetic: a
    /// turning snake's joint cell must be covered exactly once.
    ///
    /// The old split gave each run the half nearer its own end *in its own
    /// frame*, and at a turn those halves are perpendicular — so one quarter of
    /// the cell was painted twice and the opposite quarter not at all, which is
    /// a bare notch on the outside of every corner.
    #[test]
    fn a_corner_cell_is_painted_exactly_once_and_completely() {
        let skin = sheet_skin(8, Fit::TILE, None, Span::WHOLE, 0.0);
        // A single right-angle: runs are 0..8 and 8..14 in arc length, so the
        // joint cell is centred at 8.
        let painted = blits_with_alpha(&skin, &[(0.0, 0.0), (8.0, 0.0), (8.0, 6.0)], 0.0).0;
        assert!(painted.len() >= 2, "{painted:?}");

        // Runs are painted in order, so consecutive blits either continue the
        // same run or hand over at the joint. Reconstruct the covered arc.
        let mut covered: Vec<(f64, f64)> = Vec::new();
        for (_, dx, dw, _) in &painted {
            covered.push((*dx, dx + dw));
        }
        // Within a run the destination is relative to that run's origin, so the
        // meaningful check is total covered length against the paintable body.
        let total: f64 = covered.iter().map(|(from, to)| to - from).sum();
        assert!(
            (total - 15.0).abs() < 1e-6,
            "a 14-cell body plus two half-cell caps is 15 cells of paint, got {total}"
        );
    }

    /// A body span that could paint outside the snake is a registration error.
    ///
    /// `ClipShape::Cells` is the union of cell squares — bigger than the
    /// silhouette at every cap and outer corner — so a span using it reaches
    /// past the shape the player sees. Invisible on a solid fill, obvious on a
    /// texture, which is exactly the kind of thing that should not depend on
    /// someone noticing.
    #[test]
    fn a_body_span_may_not_clip_to_anything_larger_than_the_snake() {
        let mut layer = span_layer(
            "coat",
            Region::Body,
            Span::WHOLE,
            Source::Solid(ColorSlot::Fill),
        );
        assert!(validate_layers(std::slice::from_ref(&layer), &[frame()]).is_empty());

        layer.clip = ClipShape::Cells;
        let problems = validate_layers(std::slice::from_ref(&layer), &[frame()]);
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].problem.contains("outside the snake"),
            "{problems:?}"
        );

        // The head ramp keeps its full-cell reach: it is not a span, and
        // classic has painted it that way since before any of this existed.
        let ramp = Layer {
            id: "head-ramp".into(),
            region: Region::Body,
            clip: ClipShape::Cells,
            kind: LayerKind::HeadRamp {
                rgb: (255, 255, 255),
                length_cells: 5.0,
                opacity: None,
            },
            transform: LayerTransform::default(),
            boost_only: false,
            omit_on_single_cell: true,
            opacity: Binding::ONE,
        };
        assert!(validate_layers(&[ramp], &[frame()]).is_empty());
    }

    /// Three-slice on a body that cannot hold it. This is the common case, not
    /// an edge case: snakes start at three cells.
    #[test]
    fn spans_degrade_by_priority_then_disappear() {
        let head_art = |natural, min, priority| Layer {
            id: "head-art".into(),
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
            opacity: Binding::ONE,
        };
        let mut tail_art = head_art(3.0, 1.0, 10);
        tail_art.id = "tail-art".into();
        if let LayerKind::Span { span, .. } = &mut tail_art.kind {
            span.from = Anchor::Tail;
        }
        let mut middle = head_art(0.0, 1.0, 0);
        middle.id = "middle".into();
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
