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

// The compositor's surface runs ahead of the skins that ship on it: every item
// below is implemented and covered by the test suite, but the six catalogue
// skins are all the classic-shaped stack, so the non-test build never reaches
// them. That is the intended state after `specs/skin-shading-prd.md` S7-S8 —
// the engine gained spans, tiling, images and corner policies before any
// first-party skin needed them. Deleting them to satisfy the lint would delete
// the features; the alternative is to say so here.
#![allow(dead_code)]

use crate::skin::space::{ClipShape, CornerPolicy};

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
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LayerTransform {
    pub translate: (f64, f64),
    pub scale: (f64, f64),
    pub rotate_turns: f64,
}

impl Default for LayerTransform {
    fn default() -> Self {
        Self {
            translate: (0.0, 0.0),
            scale: (1.0, 1.0),
            rotate_turns: 0.0,
        }
    }
}

impl LayerTransform {
    pub fn is_identity(&self) -> bool {
        *self == Self::default()
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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Fit {
    /// Draw at natural scale and clip the far end. Art keeps its proportions.
    #[default]
    Clip,
    /// Compress into the span. Available, not default: it distorts in ways an
    /// author cannot predict from looking at the source.
    Stretch,
    /// Repeat along `s`. The right default for the middle of a three-slice.
    Tile,
}

/// A gradient stop in body space.
#[derive(Clone, Debug, PartialEq)]
pub struct Stop {
    pub offset: f64,
    pub color: ColorSlot,
    pub alpha: f64,
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
    /// [`Fit::Tile`].
    Tiled {
        color: ColorSlot,
        /// Length of one repeat along the body, in cells.
        period_cells: f64,
        /// How much of each period is painted, `0..1`.
        duty: f64,
        /// Half-width across the body, `0..0.5`.
        half_width: f64,
        /// Where the band sits across the body, `-0.5..0.5`. `0.0` is the
        /// centreline, which is what a single band wants; a checkerboard's two
        /// rows sit either side of it.
        ///
        /// `|t_center| + half_width` may not exceed `0.5`: a body layer that
        /// reached past the silhouette could paint over the Boost band, and
        /// keeping the bound declarative is what lets `validate_layers` catch
        /// it at registration instead of a pixel validator catching it never.
        t_center: f64,
        /// Offset of the first repeat along the body, in cells. Tiles are
        /// otherwise pinned to absolute multiples of `period_cells` from the
        /// head, so without this two bands can only ever be in phase.
        phase_cells: f64,
        /// Optional alpha, evaluated once per emitted tile at its centre in
        /// body space. `None` is fully opaque.
        alpha: Option<std::sync::Arc<skin_schema::expr::Expr>>,
    },
    /// A named atlas region.
    Image { region: usize, fit: Fit },
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
    pub id: &'static str,
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
    /// Index into the baked frame's per-layer opacity table, when the layer's
    /// opacity is animated. `None` means fully opaque and never touched.
    pub opacity_track: Option<usize>,
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
    },
    /// One rectangle per body cell, ramping with distance from the head.
    HeadRamp {
        /// The brightening colour, as channels, because the per-cell string is
        /// built the way it always has been.
        rgb: (u8, u8, u8),
        length_cells: f64,
    },
    /// A disc centred on the head cell.
    HeadDisc {
        paint: DiscPaint,
        /// Radius as a fraction of one cell. `0.5` is the head cap.
        radius_ratio: f64,
        /// Index into the baked frame's scalar table, when the radius is
        /// animated. The topology is still static — the same disc is painted
        /// every frame, at a different size.
        radius_track: Option<usize>,
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

    fn ribbon(region: Region, extra: f64) -> Layer {
        Layer {
            id: "test",
            region,
            clip: ClipShape::Silhouette,
            kind: LayerKind::Ribbon {
                color: ColorSlot::Fill,
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
                translate: (0.0, 0.1),
                ..LayerTransform::default()
            }
            .is_identity()
        );
    }
}
