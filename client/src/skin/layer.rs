//! The layer model.
//!
//! `specs/skin-shading-prd.md` section 6 states the rule everything else
//! depends on: **layer count and order are fixed at registration.** A layer
//! that should appear animates its opacity from zero. Time may change numbers
//! and colours; it may not add, remove, reorder, or conditionally skip a layer.
//!
//! That rule is what buys op-count invariance — already a conformance check —
//! a cost knowable before a skin ever paints, and a bake plan computable at
//! registration. It is enforced structurally here: [`Layer`] carries no
//! predicate over time, and the two conditions that *do* remove a layer
//! ([`Layer::boost_only`] and [`Layer::omit_on_single_cell`]) are properties of
//! the pose, not of the clock, so two frames of the same pose always emit the
//! same op sequence.

// The compositor's surface still runs slightly ahead of the skins that ship on
// it. That was the intended state after `specs/skin-shading-prd.md` S7-S8 — the
// engine gained spans, tiling, images and corner policies before any first-party
// skin needed them — and it is now mostly historical: the checkerboards use
// tiled bands and the animal family uses image textures, so the two biggest
// items here have real consumers. What is left unused is genuinely unused
// (`Fit::Stretch`, `CornerPolicy::Bisector`, frame strips), implemented and
// tested. Deleting them to satisfy the lint would delete the features.
#![allow(dead_code)]

use crate::skin::space::{ClipShape, CornerPolicy};
use std::borrow::Cow;

/// A number a layer paints with: fixed at registration, or looked up per
/// baked frame.
///
/// This is the compositor's *single* mechanism for "a value that may vary with
/// the clock". It replaces three ad-hoc ones that grew up alongside each other
/// — `opacity_track`, `radius_track`, and the head ramp's own
/// `ramp_opacity`/`wave` pair — each of which animated exactly the one property
/// somebody needed at the time.
///
/// Unifying them is what makes a *document* able to animate anything
/// (`specs/skin-layer-documents-prd.md` section 11): the compiler lowers every
/// expression the same way, and does not need a renderer change per property.
/// It also keeps the cost model honest, because the two cases are exactly the
/// two costs — a `Param` is one table slot per step and a lookup per frame, a
/// `Const` is nothing at all.
#[derive(Clone, Debug, PartialEq)]
pub enum Binding {
    Const(f64),
    /// Index into the baked frame's parameter table. One slot per step,
    /// resolved with a lookup.
    Param(usize),
    /// Evaluated once per snake per frame.
    ///
    /// The third tier exists because `len`, `boost` and `seed` are constant
    /// for a whole snake-frame but *not* constant at registration, so neither
    /// of the other two can carry them: folding freezes a boost-reactive layer
    /// at "not boosting", and baking into the per-step table would give every
    /// snake in the match one body length and one seed.
    Snake(std::sync::Arc<skin_schema::expr::Expr>),
}

impl Binding {
    pub const ZERO: Self = Self::Const(0.0);
    pub const ONE: Self = Self::Const(1.0);

    /// The value at one baked frame, for one snake.
    ///
    /// A `Param` missing from the table falls back rather than panicking.
    /// Registration rejects such a stack, so this is unreachable in practice —
    /// but a cosmetic must never be able to kill a frame, and that rule is
    /// worth more here than a louder failure.
    pub fn get(&self, params: &[f64], env: &skin_schema::expr::Env, fallback: f64) -> f64 {
        match self {
            Binding::Const(value) => *value,
            Binding::Param(index) => params.get(*index).copied().unwrap_or(fallback),
            Binding::Snake(expr) => expr.eval(env),
        }
    }

    pub fn as_const(&self) -> Option<f64> {
        match self {
            Binding::Const(value) => Some(*value),
            Binding::Param(_) | Binding::Snake(_) => None,
        }
    }

    /// Whether this is exactly the given constant.
    ///
    /// Emission asks this before writing an op: a layer whose opacity is the
    /// constant 1 emits no `globalAlpha` at all, which is precisely why
    /// classic's op stream is unchanged by this mechanism existing.
    pub fn is_const(&self, value: f64) -> bool {
        self.as_const() == Some(value)
    }
}

impl From<f64> for Binding {
    fn from(value: f64) -> Self {
        Binding::Const(value)
    }
}

/// Where a layer is allowed to paint.
///
/// The regioned frame is the structural substitute for the deferred pixel
/// validator (`specs/skin-shading-prd.md` section 5.2), and each rule earns its
/// place:
///
/// - only [`Region::Contour`] layers contribute overhang, so `overhang_px`
///   stays computable without measuring pixels;
/// - [`Region::Body`] layers are clipped and so cannot paint over the Boost
///   band, which an opaque body layer would otherwise hide while every op-text
///   check still passed;
/// - the Boost band is pinned outermost within the contour, so a later contour
///   layer cannot bury it either.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Region {
    /// Outside the body clip. The only source of overhang.
    Contour,
    /// Inside one clip per snake.
    Body,
    /// A bounded slot at `s ≈ 0`, inside the body clip.
    Head,
}

/// Which palette entry a layer paints with.
///
/// Resolved against the baked frame, so a layer never holds a colour string of
/// its own and animation never allocates one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColorSlot {
    Fill,
    Outline,
    /// The Boost band. Competitive information, so every skin has one.
    Boost,
    /// The dark disc inside the head.
    HeadCore,
    /// A third per-role colour, for whatever a skin's signature element is.
    /// Ember's head glow is the reason it exists.
    Accent,
    /// An index into the skin's baked literal table.
    Literal(usize),
}

/// A transform applied inside the run affine and outside the source.
///
/// Composition is `run_affine ∘ layer_transform ∘ source`: the per-run affine
/// maps body space to the screen and belongs to the renderer; this sits inside
/// it and belongs to the skin (`specs/skin-shading-prd.md` section 6.1).
///
/// Units follow the layer's declared space — cells in body space — and
/// `rotate_turns` is in **turns**, matching the animation DSL so a rotation can
/// be driven by a track without a unit conversion.
///
/// A transform changes neither clipping nor overhang. A transformed body layer
/// still clips to its declared shape, and a contour layer's `extra` is what
/// `overhang_px` reads. Both claims are machine-checked rather than asserted:
/// the recorder replays the transform stack, so the `TransformLiar` skin in
/// `skin::conformance` fails the overhang check exactly as a real skin would.
#[derive(Clone, Debug, PartialEq)]
pub struct LayerTransform {
    pub translate: (Binding, Binding),
    pub scale: (Binding, Binding),
    pub rotate_turns: Binding,
}

impl Default for LayerTransform {
    fn default() -> Self {
        Self {
            translate: (Binding::ZERO, Binding::ZERO),
            scale: (Binding::ONE, Binding::ONE),
            rotate_turns: Binding::ZERO,
        }
    }
}

impl LayerTransform {
    /// Whether this transform can be skipped entirely.
    ///
    /// A bound field is never identity even if it happens to hold identity
    /// values at some step: the op has to be emitted at *every* step or the
    /// op sequence would change with the clock, which is the one thing
    /// `skin_conformance_animation_only_varies_paint_arguments` forbids.
    pub fn is_identity(&self) -> bool {
        self.translate.0.is_const(0.0)
            && self.translate.1.is_const(0.0)
            && self.scale.0.is_const(1.0)
            && self.scale.1.is_const(1.0)
            && self.rotate_turns.is_const(0.0)
    }

    /// Every binding in this transform, for validation and cost accounting.
    pub fn bindings(&self) -> [&Binding; 5] {
        [
            &self.translate.0,
            &self.translate.1,
            &self.scale.0,
            &self.scale.1,
            &self.rotate_turns,
        ]
    }
}

/// Where a span starts.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Anchor {
    /// The entire paintable body, consuming no room from either end.
    ///
    /// A base layer is not queueing for space — it is what the art sits *on*.
    /// Without this it would take the whole body at its priority and leave the
    /// slices above it with nothing, which is a three-slice skin that silently
    /// renders as a plain one.
    Whole,
    Head,
    Tail,
    /// A fixed arc length from the head, in cells.
    At(f64),
    /// A fraction of the body's length.
    Fraction(f64),
}

/// A stretch of body a layer covers, in cells.
///
/// Snakes start at three cells, so a five-cell head sprite and a three-cell
/// tail sprite neither fit nor avoid each other. That is the common case, not
/// an edge case (`specs/skin-shading-prd.md` section 8.3), and the allocator in
/// `skin::composite` is what resolves it: spans are granted `natural` while the
/// body allows, then degraded toward `min`, then dropped.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Span {
    pub from: Anchor,
    /// Preferred length in cells. `None` means "whatever is left".
    pub natural: Option<f64>,
    /// Below this the layer is skipped entirely rather than squeezed.
    pub min: f64,
    /// Higher wins when the body is too short for everything.
    pub priority: i32,
}

impl Span {
    /// The whole body, which is what a base layer wants.
    pub const WHOLE: Self = Self {
        from: Anchor::Whole,
        natural: None,
        min: 0.0,
        priority: i32::MAX,
    };
}

/// What happens to a source when its span is shorter than its natural length.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum Fit {
    /// Draw at natural scale and clip the far end. Art keeps its proportions.
    #[default]
    Clip,
    /// Compress into the span. Available, not default: it distorts in ways an
    /// author cannot predict from looking at the source.
    Stretch,
    /// Repeat along `s`, for as many repeats as the span holds.
    ///
    /// This is what a *texture* is, as opposed to a sprite: the art is not
    /// placed on the snake, it clothes it, and a body of any length is covered.
    /// Repeats are laid out from the span's start in **arc length**, never per
    /// run, so a repeat straddling a corner stays one repeat and the pattern
    /// does not restart at every turn.
    ///
    /// A repeat may be as long as the author likes, and the interesting case is
    /// **longer than one cell**: a coat pattern that repeated every cell would
    /// read as a machine-made stripe rather than as an animal. Each repeat costs
    /// one `drawImage`, so the length is also the cost knob — see
    /// `skin::animal` for a family that picks it deliberately.
    Tile {
        /// How many cells of body one repeat covers.
        ///
        /// `None` keeps the region's own proportions, mapping its height across
        /// the body and deriving the length from its aspect. `Some(n)` decouples
        /// the two, which is what lets one texture be authored at whatever
        /// resolution suits it and then worn at whatever scale reads best.
        cells_per_repeat: Option<f64>,
    },
    /// Draw at the art's **authored scale** and let the body clip the rest.
    ///
    /// Every other fit maps the source's height onto exactly one cell, which is
    /// the right answer for a texture — a coat is one cell wide because a snake
    /// is. It is the wrong answer for a *picture*. A flag is 14.7 times wider
    /// than it is tall; squeezing its full height into one cell squashes it,
    /// and the alternative of showing only a thin band throws the picture away.
    ///
    /// So this one keeps the art's proportions and draws it **taller than the
    /// body**, centred on the centreline. The silhouette clip every body span
    /// already carries does the rest, so the snake becomes a window onto the
    /// picture: on a 1.4-cell-tall flag it hides the outermost slivers and
    /// shows the rest undistorted, and a turn reveals a different part of it.
    ///
    /// Costs nothing extra — the overflow is clipped, not painted — and the op
    /// count is unchanged.
    Cutout {
        /// The region's height, in cells, at the scale it was authored for.
        cells_tall: f64,
    },
}

impl Fit {
    /// A tile at the source's own proportions.
    pub const TILE: Self = Self::Tile {
        cells_per_repeat: None,
    };
}

/// A gradient stop in body space.
///
/// Offset and alpha are bindings, which is what makes a travelling shine one
/// layer rather than a schema feature: the crest is a stop whose offset reads
/// the clock. The stop *count* stays static, because that is what decides how
/// many `addColorStop` calls the frame makes.
#[derive(Clone, Debug, PartialEq)]
pub struct Stop {
    pub offset: Binding,
    pub color: ColorSlot,
    pub alpha: Binding,
}

/// What fills a span.
#[derive(Clone, Debug, PartialEq)]
pub enum Source {
    /// A flat colour. The degenerate layer.
    Solid(ColorSlot),
    /// Stops along `s`.
    LinearAlongBody(Vec<Stop>),
    /// Stops outward from the span's start, across the body.
    RadialFromStart(Vec<Stop>),
    /// A tile repeated along the body.
    ///
    /// `specs/skin-shading-prd.md` section 7 describes a pattern as "a tile,
    /// itself a small baked layer stack, repeated in `s` and/or `t`". For a
    /// tile whose content is a flat colour with an alpha, that repetition *is*
    /// a sequence of rectangles in body space — so this needs no bitmap, no
    /// `CanvasPattern`, and no browser-only code path. Everything it emits is
    /// recordable, which means stripes, dashes and travelling bands are
    /// checkable natively like the rest of the system.
    ///
    /// One tile layer is a single band: a repeat along `s` at one place across
    /// the body. Patterns that alternate in **both** axes — a checkerboard is
    /// the obvious one — are two of these, offset against each other by
    /// `t_center` and `phase_cells`. Building them from bands rather than from
    /// a dedicated two-dimensional source keeps the emission loop unchanged and
    /// keeps the cost legible: a skin author can count the rectangles.
    ///
    /// Bitmap tiling is a different feature and lives on [`Source::Image`] as
    /// [`Fit::Tile`], and the split is worth keeping straight: this source is a
    /// *shape* a skin can describe in numbers, while that one is *pixels* an
    /// artist drew. A stripe with a hard edge is cheaper and sharper here; a
    /// stripe with a taper, a fork and a soft margin is only expressible there.
    Tiled {
        color: ColorSlot,
        /// Length of one repeat along the body, in cells.
        period_cells: f64,
        /// How much of each period is painted, `0..1`.
        duty: f64,
        /// Half-width across the body, `0..0.5`.
        half_width: Binding,
        /// Where the band sits across the body, `-0.5..0.5`. `0.0` is the
        /// centreline, which is what a single band wants; a checkerboard's two
        /// rows sit either side of it.
        ///
        /// `|t_center| + half_width` may not exceed `0.5`: a body layer that
        /// reached past the silhouette could paint over the Boost band, and
        /// keeping the bound declarative is what lets `validate_layers` catch
        /// it at registration instead of a pixel validator catching it never.
        ///
        /// Bound rather than fixed, so a band can slide across the body — but
        /// the bound is then checked against every baked step, not just the
        /// resting one.
        t_center: Binding,
        /// Offset of the first repeat along the body, in cells. Tiles are
        /// otherwise pinned to absolute multiples of `period_cells` from the
        /// head, so without this two bands can only ever be in phase.
        phase_cells: f64,
        /// Optional alpha, evaluated once per emitted tile at its centre in
        /// body space. `None` is fully opaque.
        alpha: Option<std::sync::Arc<skin_schema::expr::Expr>>,
    },
    /// A named atlas region.
    Image {
        region: usize,
        fit: Fit,
        /// Optional alpha ramps at the span's two ends.
        fade: Option<Fade>,
        /// Cells the pattern slides along the body per animation cycle,
        /// positive meaning *away* from the span's anchor. Only meaningful for
        /// [`Fit::Tile`]; a sprite drawn once has nowhere to slide to.
        ///
        /// Drift is applied to which part of the texture each repeat **samples**,
        /// never to where the repeats sit. Sliding the repeats themselves would
        /// change how many of them overlap the body as the phase crossed a
        /// boundary, and op-count invariance is checked across the clock — so
        /// that version of this feature fails conformance rather than looking
        /// wrong. Sampling instead costs one extra blit per repeat, always,
        /// which keeps the count a property of the skin and not of the moment.
        drift_cells: f64,
    },
}

/// Alpha ramps at the ends of an image span.
///
/// This exists because a sprite that does not clothe the whole snake has to
/// *end* somewhere, and a bitmap's own edge is a hard vertical line across the
/// body — which reads as the art being cut off rather than as the art ending.
/// A head-pinned twenty-cell sprite on a forty-cell snake is the ordinary case,
/// not an edge case.
///
/// The ramp is approximated by `steps` constant-alpha slices rather than a real
/// canvas gradient, and that is the whole design: `globalAlpha` is a number the
/// recorder captures, so a fade is checkable natively like everything else,
/// while a gradient-masked blit would need an offscreen canvas and would be
/// invisible to every test outside a browser. The slice boundaries are fixed by
/// the allocation, so the op count is a function of the pose and never of the
/// clock.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Fade {
    /// Ramp length at the span's head-ward end, in cells. Alpha runs `0 -> 1`.
    pub lead_cells: f64,
    /// Ramp length at the span's tail-ward end, in cells. Alpha runs `1 -> 0`.
    pub trail_cells: f64,
    /// Constant-alpha slices per ramp. More is smoother and costs one blit
    /// each; twelve is indistinguishable from a gradient at any cell size the
    /// arena uses.
    pub steps: usize,
}

impl Fade {
    /// A fade only at the tail-ward end — what a head-pinned sprite wants.
    pub const fn trailing(cells: f64, steps: usize) -> Self {
        Self {
            lead_cells: 0.0,
            trail_cells: cells,
            steps,
        }
    }

    /// Whether this fade asks for nothing.
    ///
    /// Deliberately about the *lengths* only. Folding `steps == 0` in here as
    /// well would turn a plainly wrong fade — six cells of ramp in zero slices
    /// — into a silent no-fade, which is the hard edge the author was trying to
    /// remove, arriving with no error to explain it.
    pub fn is_noop(&self) -> bool {
        self.lead_cells <= 0.0 && self.trail_cells <= 0.0
    }
}

/// One layer of a compiled skin.
///
/// The specialised lowerings ([`LayerKind::Ribbon`], [`LayerKind::HeadRamp`],
/// [`LayerKind::HeadDisc`]) are not legacy escape hatches — they are the three
/// shapes a snake body actually needs that a span of rectangles cannot express:
/// a capsule union with round caps, a per-cell ramp, and a disc. Classic is
/// built entirely from them, which is what lets classic-as-layers emit the same
/// op stream as the painter it replaces.
#[derive(Clone, Debug, PartialEq)]
pub struct Layer {
    /// What this layer is called. `Cow` because first-party skins name their
    /// layers with literals and documents name theirs at runtime — a borrowed
    /// id costs an author-written skin nothing, and an owned one is what lets
    /// a compiled document report errors against the name its author chose.
    pub id: Cow<'static, str>,
    pub region: Region,
    pub clip: ClipShape,
    pub kind: LayerKind,
    pub transform: LayerTransform,
    /// Paint only while the snake is boosting.
    pub boost_only: bool,
    /// Skip for a body of a single cell.
    ///
    /// A one-cell snake is structurally distinct — no occlusion mask, no head
    /// gradient, no white overlay — and saying so on the layer is the
    /// difference between a documented rule and an accident of lowering
    /// (`specs/skin-shading-prd.md` section 15).
    pub omit_on_single_cell: bool,
    /// The layer's opacity. [`Binding::ONE`] means fully opaque, and emits no
    /// `globalAlpha` op at all — which is why adding this mechanism left every
    /// existing skin's op stream untouched.
    pub opacity: Binding,
}

/// The geometry a layer covers, and how it is emitted.
#[derive(Clone, Debug, PartialEq)]
pub enum LayerKind {
    /// A stroked capsule ribbon along the whole body.
    Ribbon {
        color: ColorSlot,
        /// Total width beyond one cell, across both sides. Non-zero only in
        /// the contour region, where it is the sole source of overhang.
        extra: f64,
        joints: bool,
        tail_cap: bool,
        /// Emission-order knobs that exist for byte-exact classic parity; see
        /// [`crate::skin::space::RibbonPlan`].
        fill_before_strokes: bool,
        refill_before_tail_cap: bool,
        /// Paint the ribbon as one stroked path, so a boundary shared between
        /// runs is antialiased once instead of two or three times. See
        /// [`crate::skin::space::RibbonPlan::single_pass`].
        single_pass: bool,
    },
    /// One rectangle per body cell, ramping with distance from the head.
    HeadRamp {
        /// The brightening colour, as channels, because the per-cell string is
        /// built the way it always has been.
        rgb: (u8, u8, u8),
        length_cells: f64,
        /// The painted opacity of one cell, as a function of `s` (its distance
        /// from the head, in cells) and `time`.
        ///
        /// The ramp is the one place the compositor already walks cells, so it
        /// is the one place a *per-cell* expression is affordable — which is
        /// why a travelling wave lives here and nowhere else.
        ///
        /// Note this is the **final** opacity, not a peak the renderer then
        /// applies a falloff to. The legacy path hard-codes a linear falloff
        /// with the wave added *after* it; making the whole curve the
        /// expression's business is what turns "the head glow" from a fixed
        /// shape with two knobs into something an author can actually reshape.
        /// `None` keeps the legacy pairing of the frame's `ramp_opacity` with
        /// the skin's configured wave; `Some` supersedes both, and is what a
        /// compiled document always emits.
        opacity: Option<std::sync::Arc<skin_schema::expr::Expr>>,
    },
    /// A disc centred on the head cell.
    HeadDisc {
        paint: DiscPaint,
        /// Radius as a fraction of one cell. `0.5` is the head cap. Bound
        /// values keep the topology static — the same disc every frame, at a
        /// different size.
        radius: Binding,
    },
    /// Rectangles in body space across a span.
    Span {
        span: Span,
        source: Source,
        corner: CornerPolicy,
    },
}

/// What a head disc is filled with.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum DiscPaint {
    Slot(ColorSlot),
    /// A radial gradient from the disc's centre, in one role colour at a
    /// sequence of alphas. The alphas are two-hex-digit suffixes because that
    /// is how the colour reaches canvas either way, and building them as
    /// `rgba(...)` would allocate more per frame, not less.
    RadialGlow {
        slot: ColorSlot,
        stops: &'static [(f64, &'static str)],
    },
    /// The head-ramp colour at the ramp's current peak opacity — classic's
    /// white overlay, which is the ramp's own maximum applied as a disc.
    RampPeak,
}

impl Layer {
    /// How far past the body this layer paints, per side.
    ///
    /// Only contour layers can be non-zero, which is what keeps `overhang_px`
    /// computable as a maximum over contour layers rather than a measurement.
    pub fn overhang_px(&self) -> f64 {
        match (&self.region, &self.kind) {
            (Region::Contour, LayerKind::Ribbon { extra, .. }) => extra / 2.0,
            _ => 0.0,
        }
    }

    /// Whether this layer paints for a given pose.
    ///
    /// Deliberately a function of the pose alone. If the clock could reach it,
    /// op-count invariance would be a hope rather than a property.
    pub fn applies(&self, boost_active: bool, cell_count: usize) -> bool {
        if self.boost_only && !boost_active {
            return false;
        }
        if self.omit_on_single_cell && cell_count <= 1 {
            return false;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use skin_schema::expr::Env;

    fn ribbon(region: Region, extra: f64) -> Layer {
        Layer {
            id: "test".into(),
            region,
            clip: ClipShape::Silhouette,
            kind: LayerKind::Ribbon {
                color: ColorSlot::Fill,
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

    /// The rule that keeps `overhang_px` honest without a pixel validator: a
    /// body layer cannot contribute overhang no matter what it declares.
    #[test]
    fn only_contour_layers_contribute_overhang() {
        assert_eq!(ribbon(Region::Contour, 6.0).overhang_px(), 3.0);
        assert_eq!(ribbon(Region::Body, 6.0).overhang_px(), 0.0);
        assert_eq!(ribbon(Region::Head, 6.0).overhang_px(), 0.0);
    }

    /// Layer presence may depend on the pose and never on the clock, because
    /// op-count invariance is checked by comparing two clocks on one pose.
    #[test]
    fn layer_presence_depends_only_on_the_pose() {
        let mut layer = ribbon(Region::Contour, 6.0);
        assert!(layer.applies(false, 5));

        layer.boost_only = true;
        assert!(!layer.applies(false, 5));
        assert!(layer.applies(true, 5));

        layer.boost_only = false;
        layer.omit_on_single_cell = true;
        assert!(!layer.applies(false, 1));
        assert!(layer.applies(false, 2));
    }

    #[test]
    fn the_default_layer_transform_is_the_identity() {
        assert!(LayerTransform::default().is_identity());
        assert!(
            !LayerTransform {
                translate: (Binding::ZERO, Binding::Const(0.1)),
                ..LayerTransform::default()
            }
            .is_identity()
        );
    }

    /// A bound field is never the identity, even where it currently holds
    /// identity values. Emission depends on this: a transform skipped at one
    /// step and emitted at another would change the op sequence with the
    /// clock, which is exactly what conformance forbids.
    #[test]
    fn a_bound_transform_is_never_skipped_however_it_evaluates() {
        let bound = LayerTransform {
            translate: (Binding::Param(0), Binding::ZERO),
            ..LayerTransform::default()
        };
        assert!(!bound.is_identity());
        assert_eq!(bound.translate.0.get(&[0.0], &Env::default(), 0.0), 0.0);
    }

    /// The neutral constant is what suppresses an op; a bound value never
    /// does, and a missing parameter falls back rather than panicking,
    /// because a cosmetic may not be able to kill a frame.
    #[test]
    fn a_binding_reads_its_table_and_survives_a_missing_slot() {
        assert!(Binding::ONE.is_const(1.0));
        assert!(!Binding::Param(0).is_const(1.0));
        assert_eq!(
            Binding::Param(1).get(&[0.2, 0.7], &Env::default(), 1.0),
            0.7
        );
        assert_eq!(Binding::Param(9).get(&[0.2], &Env::default(), 1.0), 1.0);
        assert_eq!(Binding::Const(0.4).get(&[], &Env::default(), 1.0), 0.4);
    }
}
