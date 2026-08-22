//! The painter sink every skin draws through.
//!
//! [`PaintCtx`] exists for one reason: a skin's output has to be verifiable
//! without a browser. The `Web` arm forwards one-for-one to
//! `CanvasRenderingContext2d`, so routing the existing painters through it
//! changes no pixels; the `Rec` arm records the same calls as plain data, which
//! is what the golden-trace tests in `skin::goldens` compare. Because both arms
//! sit behind one enum, a skin cannot accidentally paint through a path the
//! tests do not observe.
//!
//! Ops are deliberately only what the painters actually need. Compositing
//! controls beyond `globalAlpha` are absent for the same reason an op nobody
//! paints with is absent: it is surface to keep working, and adding one when a
//! skin needs it is a few lines here plus a golden line in the trace.
//!
//! **The recorder tracks the transform and the clip.** It has to. The moment a
//! skin emits a transform, a recorder that measured pre-transform coordinates
//! would report the wrong extent — and the conformance suite's overhang check
//! would keep passing while measuring nothing. See [`OpRecorder::painted_extent`]
//! and the `TransformLiar` skin in `skin::conformance`.

// `Affine`'s helpers, `close_path` and `set_transform` are used by the
// recorder's transform replay and by compositor lowerings that no shipped skin
// reaches yet. They are covered by tests; the non-test build simply does not
// call them. See the same note in `skin::layer`.
#![allow(dead_code)]

use wasm_bindgen::prelude::*;

/// One stop in a gradient fill.
#[derive(Clone, Debug, PartialEq)]
pub struct GradientStop {
    pub offset: f64,
    pub color: String,
}

/// A gradient described as data so the recorder can capture it exactly.
///
/// Gradients are stored as data rather than as browser handles because a skin
/// is compiled at registration, in a process-wide `OnceLock` with no canvas in
/// scope — and because the conformance and golden tests run natively, where
/// there is no canvas at all. The `Web` arm materialises the real
/// `CanvasGradient` at paint time.
#[derive(Clone, Debug, PartialEq)]
pub enum Gradient {
    Radial {
        x0: f64,
        y0: f64,
        r0: f64,
        x1: f64,
        y1: f64,
        r1: f64,
        stops: Vec<GradientStop>,
    },
    Linear {
        x0: f64,
        y0: f64,
        x1: f64,
        y1: f64,
        stops: Vec<GradientStop>,
    },
}

/// A single recorded canvas operation.
///
/// `f64` fields are compared and rendered through Rust's shortest round-trip
/// float formatting, which is injective: two different `f64` values can never
/// print the same text, so a golden diff is exact rather than approximate.
#[derive(Clone, Debug, PartialEq)]
pub enum PaintOp {
    SetFill(String),
    SetFillGradient(Gradient),
    SetStroke(String),
    SetLineWidth(f64),
    SetLineCap(String),
    SetGlobalAlpha(f64),
    BeginPath,
    MoveTo(f64, f64),
    LineTo(f64, f64),
    Arc(f64, f64, f64, f64, f64),
    Rect(f64, f64, f64, f64),
    ClosePath,
    Clip,
    Fill,
    Stroke,
    FillRect(f64, f64, f64, f64),
    Save,
    Restore,
    Translate(f64, f64),
    Rotate(f64),
    Scale(f64, f64),
    Transform(f64, f64, f64, f64, f64, f64),
    SetTransform(f64, f64, f64, f64, f64, f64),
    /// One blit from an atlas.
    ///
    /// `image` indexes the process-wide atlas store rather than naming a URL,
    /// so the recorded op is stable text and a native test can assert on
    /// placement without any decoding having happened.
    DrawImage {
        image: usize,
        source: (f64, f64, f64, f64),
        dest: (f64, f64, f64, f64),
    },
}

/// A 2x3 affine transform, in canvas order: `[a b c d e f]`.
///
/// Canvas bakes the current transform into path coordinates as they are added,
/// so tracking it here reproduces exactly what the browser does with the same
/// op stream — which is what makes the recorded extent a statement about pixels
/// rather than about arguments.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Affine {
    pub a: f64,
    pub b: f64,
    pub c: f64,
    pub d: f64,
    pub e: f64,
    pub f: f64,
}

impl Affine {
    pub const IDENTITY: Self = Self {
        a: 1.0,
        b: 0.0,
        c: 0.0,
        d: 1.0,
        e: 0.0,
        f: 0.0,
    };

    /// `self ∘ other` — apply `other` first, in canvas's post-multiply order.
    pub fn then(self, other: Self) -> Self {
        Self {
            a: self.a * other.a + self.c * other.b,
            b: self.b * other.a + self.d * other.b,
            c: self.a * other.c + self.c * other.d,
            d: self.b * other.c + self.d * other.d,
            e: self.a * other.e + self.c * other.f + self.e,
            f: self.b * other.e + self.d * other.f + self.f,
        }
    }

    pub fn apply(self, x: f64, y: f64) -> (f64, f64) {
        (
            self.a * x + self.c * y + self.e,
            self.b * x + self.d * y + self.f,
        )
    }

    /// An upper bound on how much this transform can stretch a length.
    ///
    /// The true bound is the larger singular value; this uses the larger column
    /// norm, which is never smaller. Over-estimating is the safe direction for
    /// an honesty check: it can make a skin look like it paints wider than it
    /// does, never narrower.
    pub fn max_stretch(self) -> f64 {
        (self.a * self.a + self.b * self.b)
            .sqrt()
            .max((self.c * self.c + self.d * self.d).sqrt())
    }

    /// The axis-aligned bounds of a rectangle after transformation.
    fn map_bounds(self, (x0, y0, x1, y1): (f64, f64, f64, f64)) -> (f64, f64, f64, f64) {
        let corners = [
            self.apply(x0, y0),
            self.apply(x1, y0),
            self.apply(x0, y1),
            self.apply(x1, y1),
        ];
        corners.iter().fold(
            (f64::MAX, f64::MAX, f64::MIN, f64::MIN),
            |(ax0, ay0, ax1, ay1), (x, y)| (ax0.min(*x), ay0.min(*y), ax1.max(*x), ay1.max(*y)),
        )
    }
}

#[cfg(test)]
impl PaintOp {
    /// Render one op as a golden-file line. Stable, diffable, and exact.
    pub fn to_golden_line(&self) -> String {
        fn stops(stops: &[GradientStop]) -> String {
            stops
                .iter()
                .map(|stop| format!("{:?}:{}", stop.offset, stop.color))
                .collect::<Vec<_>>()
                .join(" ")
        }

        match self {
            PaintOp::SetFill(color) => format!("set_fill {color}"),
            PaintOp::SetFillGradient(Gradient::Radial {
                x0,
                y0,
                r0,
                x1,
                y1,
                r1,
                stops: s,
            }) => format!(
                "set_fill_radial_gradient({:?}, {:?}, {:?}, {:?}, {:?}, {:?}) [{}]",
                x0,
                y0,
                r0,
                x1,
                y1,
                r1,
                stops(s)
            ),
            PaintOp::SetFillGradient(Gradient::Linear {
                x0,
                y0,
                x1,
                y1,
                stops: s,
            }) => format!(
                "set_fill_linear_gradient({:?}, {:?}, {:?}, {:?}) [{}]",
                x0,
                y0,
                x1,
                y1,
                stops(s)
            ),
            PaintOp::SetStroke(color) => format!("set_stroke {color}"),
            PaintOp::SetLineWidth(width) => format!("set_line_width({width:?})"),
            PaintOp::SetLineCap(cap) => format!("set_line_cap {cap}"),
            PaintOp::SetGlobalAlpha(alpha) => format!("set_global_alpha({alpha:?})"),
            PaintOp::BeginPath => "begin_path".to_string(),
            PaintOp::MoveTo(x, y) => format!("move_to({x:?}, {y:?})"),
            PaintOp::LineTo(x, y) => format!("line_to({x:?}, {y:?})"),
            PaintOp::Arc(x, y, r, start, end) => {
                format!("arc({x:?}, {y:?}, {r:?}, {start:?}, {end:?})")
            }
            PaintOp::Rect(x, y, w, h) => format!("rect({x:?}, {y:?}, {w:?}, {h:?})"),
            PaintOp::ClosePath => "close_path".to_string(),
            PaintOp::Clip => "clip".to_string(),
            PaintOp::Fill => "fill".to_string(),
            PaintOp::Stroke => "stroke".to_string(),
            PaintOp::FillRect(x, y, w, h) => format!("fill_rect({x:?}, {y:?}, {w:?}, {h:?})"),
            PaintOp::Save => "save".to_string(),
            PaintOp::Restore => "restore".to_string(),
            PaintOp::Translate(x, y) => format!("translate({x:?}, {y:?})"),
            PaintOp::Rotate(radians) => format!("rotate({radians:?})"),
            PaintOp::Scale(x, y) => format!("scale({x:?}, {y:?})"),
            PaintOp::Transform(a, b, c, d, e, f) => {
                format!("transform({a:?}, {b:?}, {c:?}, {d:?}, {e:?}, {f:?})")
            }
            PaintOp::SetTransform(a, b, c, d, e, f) => {
                format!("set_transform({a:?}, {b:?}, {c:?}, {d:?}, {e:?}, {f:?})")
            }
            PaintOp::DrawImage {
                image,
                source: (sx, sy, sw, sh),
                dest: (dx, dy, dw, dh),
            } => format!(
                "draw_image({image}, [{sx:?}, {sy:?}, {sw:?}, {sh:?}] -> \
                 [{dx:?}, {dy:?}, {dw:?}, {dh:?}])"
            ),
        }
    }

    /// The op's name without its arguments.
    ///
    /// This is what conformance uses to prove animation only varies paint
    /// *arguments*: two frames of the same skin must produce the same sequence
    /// of names, no matter how far apart their animation clocks are.
    pub fn shape(&self) -> &'static str {
        match self {
            PaintOp::SetFill(_) => "set_fill",
            PaintOp::SetFillGradient(_) => "set_fill_gradient",
            PaintOp::SetStroke(_) => "set_stroke",
            PaintOp::SetLineWidth(_) => "set_line_width",
            PaintOp::SetLineCap(_) => "set_line_cap",
            PaintOp::SetGlobalAlpha(_) => "set_global_alpha",
            PaintOp::BeginPath => "begin_path",
            PaintOp::MoveTo(..) => "move_to",
            PaintOp::LineTo(..) => "line_to",
            PaintOp::Arc(..) => "arc",
            PaintOp::Rect(..) => "rect",
            PaintOp::ClosePath => "close_path",
            PaintOp::Clip => "clip",
            PaintOp::Fill => "fill",
            PaintOp::Stroke => "stroke",
            PaintOp::FillRect(..) => "fill_rect",
            PaintOp::Save => "save",
            PaintOp::Restore => "restore",
            PaintOp::Translate(..) => "translate",
            PaintOp::Rotate(..) => "rotate",
            PaintOp::Scale(..) => "scale",
            PaintOp::Transform(..) => "transform",
            PaintOp::SetTransform(..) => "set_transform",
            PaintOp::DrawImage { .. } => "draw_image",
        }
    }

    /// The rectangle this op covers **in its own coordinates**, if it covers
    /// anything geometric. The recorder maps it through the live transform.
    ///
    /// Strokes are widened by the current line width because a stroke straddles
    /// its path. An arc is bounded by its enclosing square, which after a
    /// rotation over-estimates the true ellipse — the safe direction.
    pub fn painted_bounds(&self, line_width: f64) -> Option<(f64, f64, f64, f64)> {
        match self {
            PaintOp::FillRect(x, y, w, h) | PaintOp::Rect(x, y, w, h) => {
                Some((*x, *y, x + w, y + h))
            }
            PaintOp::Arc(x, y, r, _, _) => Some((x - r, y - r, x + r, y + r)),
            PaintOp::DrawImage {
                dest: (x, y, w, h), ..
            } => Some((*x, *y, x + w, y + h)),
            PaintOp::MoveTo(x, y) | PaintOp::LineTo(x, y) => {
                let half = line_width / 2.0;
                Some((x - half, y - half, x + half, y + half))
            }
            _ => None,
        }
    }
}

/// Records every op a skin issues, for native golden and conformance tests.
#[cfg(test)]
#[derive(Clone, Debug, Default)]
pub struct OpRecorder {
    ops: Vec<PaintOp>,
}

#[cfg(test)]
impl OpRecorder {
    pub fn new() -> Self {
        Self::default()
    }

    /// The recorded stream as golden-file text: one op per line.
    pub fn to_golden(&self) -> String {
        let mut out = String::new();
        for op in &self.ops {
            out.push_str(&op.to_golden_line());
            out.push('\n');
        }
        out
    }

    /// The op-name sequence, for animation op-count-invariance checks.
    pub fn shapes(&self) -> Vec<&'static str> {
        self.ops.iter().map(PaintOp::shape).collect()
    }

    /// The recorded ops themselves, for a test that needs an argument rather
    /// than a name — where a rectangle actually landed, say. Golden text and
    /// op names both answer "did this change"; this answers "to what".
    pub fn ops(&self) -> &[PaintOp] {
        &self.ops
    }

    /// The union of every painted bound, **in device pixels**, honouring the
    /// transform stack and every clip in force.
    ///
    /// This is the measurement the overhang-honesty check reads. Two properties
    /// make it trustworthy rather than merely plausible:
    ///
    /// - **Transforms are replayed**, so a skin cannot paint inside its budget
    ///   and then translate the result outside it. Before this, the first
    ///   `translate` turned the check into a no-op that still passed.
    /// - **Clips only ever shrink it**, and are tracked as the bounding box of
    ///   the clip path. A non-rectangular clip therefore over-reports what
    ///   survives — the safe direction, because it can only accuse a skin of
    ///   painting wider than it does.
    ///
    /// `None` when the skin painted nothing geometric.
    pub fn painted_extent(&self) -> Option<(f64, f64, f64, f64)> {
        /// One entry of the canvas state stack.
        #[derive(Clone, Copy)]
        struct State {
            ctm: Affine,
            clip: Option<(f64, f64, f64, f64)>,
            line_width: f64,
        }

        fn intersect(
            left: (f64, f64, f64, f64),
            right: (f64, f64, f64, f64),
        ) -> Option<(f64, f64, f64, f64)> {
            let bounds = (
                left.0.max(right.0),
                left.1.max(right.1),
                left.2.min(right.2),
                left.3.min(right.3),
            );
            (bounds.0 < bounds.2 && bounds.1 < bounds.3).then_some(bounds)
        }

        let mut state = State {
            ctm: Affine::IDENTITY,
            clip: None,
            line_width: 1.0,
        };
        let mut stack: Vec<State> = Vec::new();
        // Path bounds accumulate in device space, exactly as canvas bakes the
        // transform into each point as it is added.
        let mut path: Option<(f64, f64, f64, f64)> = None;
        // The same path without the stroke allowance, for the consumers that
        // do not stroke it.
        let mut filled_path: Option<(f64, f64, f64, f64)> = None;
        let mut extent: Option<(f64, f64, f64, f64)> = None;

        for op in &self.ops {
            match op {
                PaintOp::Save => stack.push(state),
                PaintOp::Restore => {
                    if let Some(popped) = stack.pop() {
                        state = popped;
                    }
                }
                PaintOp::SetLineWidth(width) => state.line_width = *width,
                PaintOp::Translate(x, y) => {
                    state.ctm = state.ctm.then(Affine {
                        e: *x,
                        f: *y,
                        ..Affine::IDENTITY
                    })
                }
                PaintOp::Rotate(radians) => {
                    let (sin, cos) = radians.sin_cos();
                    state.ctm = state.ctm.then(Affine {
                        a: cos,
                        b: sin,
                        c: -sin,
                        d: cos,
                        e: 0.0,
                        f: 0.0,
                    })
                }
                PaintOp::Scale(x, y) => {
                    state.ctm = state.ctm.then(Affine {
                        a: *x,
                        d: *y,
                        ..Affine::IDENTITY
                    })
                }
                PaintOp::Transform(a, b, c, d, e, f) => {
                    state.ctm = state.ctm.then(Affine {
                        a: *a,
                        b: *b,
                        c: *c,
                        d: *d,
                        e: *e,
                        f: *f,
                    })
                }
                PaintOp::SetTransform(a, b, c, d, e, f) => {
                    state.ctm = Affine {
                        a: *a,
                        b: *b,
                        c: *c,
                        d: *d,
                        e: *e,
                        f: *f,
                    }
                }
                PaintOp::BeginPath => {
                    path = None;
                    filled_path = None;
                }
                PaintOp::Clip => {
                    // A clip with no path clips everything away; canvas treats
                    // an empty path as an empty region, and so do we.
                    // A clip is the region the path encloses, not the region a
                    // pen tracing it would cover.
                    let region = filled_path.unwrap_or((0.0, 0.0, 0.0, 0.0));
                    state.clip = Some(match state.clip {
                        None => region,
                        Some(existing) => {
                            intersect(existing, region).unwrap_or((0.0, 0.0, 0.0, 0.0))
                        }
                    });
                }
                _ => {}
            }

            let Some(local) = op.painted_bounds(state.line_width * state.ctm.max_stretch()) else {
                continue;
            };
            // A bare path point is grown by half a line width, because a
            // *stroke* of it reaches that far. A fill or a clip does not — the
            // region is the path itself — so the two are tracked apart and the
            // consumer picks. Growing both was a quiet over-estimate that only
            // became visible when a `move_to` was added to start a disc's
            // subpath: the point sits exactly on the circle the arc already
            // contributes, and the clip appeared to grow by half a pixel
            // without anything moving.
            let (device, exact) = match op {
                PaintOp::MoveTo(x, y) | PaintOp::LineTo(x, y) => {
                    let (dx, dy) = state.ctm.apply(*x, *y);
                    let half = state.line_width * state.ctm.max_stretch() / 2.0;
                    (
                        (dx - half, dy - half, dx + half, dy + half),
                        (dx, dy, dx, dy),
                    )
                }
                _ => {
                    let mapped = state.ctm.map_bounds(local);
                    (mapped, mapped)
                }
            };

            // Path-building ops contribute to the path, which only reaches the
            // canvas through a later fill, stroke, or clip. Treating them as
            // painted immediately is what the recorder has always done, and it
            // over-reports rather than under-reports.
            path = Some(match path {
                None => device,
                Some((px0, py0, px1, py1)) => (
                    px0.min(device.0),
                    py0.min(device.1),
                    px1.max(device.2),
                    py1.max(device.3),
                ),
            });
            filled_path = Some(match filled_path {
                None => exact,
                Some((px0, py0, px1, py1)) => (
                    px0.min(exact.0),
                    py0.min(exact.1),
                    px1.max(exact.2),
                    py1.max(exact.3),
                ),
            });

            // `move_to` lays down no ink under any consumer — a stroke is
            // bounded by the segments and arcs that follow it, a fill and a
            // clip by the region they enclose. It still moves the path bounds
            // above, because a later segment starts there.
            if matches!(op, PaintOp::MoveTo(_, _)) {
                continue;
            }

            let Some(visible) = (match state.clip {
                None => Some(device),
                Some(clip) => intersect(device, clip),
            }) else {
                continue;
            };

            extent = Some(match extent {
                None => visible,
                Some((ax0, ay0, ax1, ay1)) => (
                    ax0.min(visible.0),
                    ay0.min(visible.1),
                    ax1.max(visible.2),
                    ay1.max(visible.3),
                ),
            });
        }
        extent
    }
}

/// Where a [`PaintCtx`] sends its ops.
pub enum PaintSink<'a> {
    /// Production: forward one-for-one to the browser canvas.
    Web(&'a web_sys::CanvasRenderingContext2d),
    /// Tests: record the ops as data. Compiled out of release builds, so the
    /// shipped bundle carries only the forwarding arm.
    #[cfg(test)]
    Rec(&'a mut OpRecorder),
    /// Tests: discard every op.
    ///
    /// Exists for the allocation census in [`crate::skin::perf`]. Recording
    /// allocates by construction — a `Vec` of ops, a `String` per colour — so
    /// measuring a painter's own allocations through the recorder would
    /// measure the recorder. This arm is the painter running against nothing.
    #[cfg(test)]
    Null,
}

/// The painter handed to every skin.
///
/// Every method is a direct forward in the `Web` arm — no normalization, no
/// reordering, no batching — which is what lets a recorded trace stand in for
/// the real canvas in tests.
pub struct PaintCtx<'a> {
    sink: PaintSink<'a>,
}

// `PaintSink` has a single variant outside test builds, which makes every
// `if let PaintSink::Web(..)` below irrefutable there. Writing them as matches
// would need a `#[cfg]` on every arm; this keeps the two builds textually
// identical.
#[allow(irrefutable_let_patterns)]
impl<'a> PaintCtx<'a> {
    pub fn web(ctx: &'a web_sys::CanvasRenderingContext2d) -> Self {
        Self {
            sink: PaintSink::Web(ctx),
        }
    }

    #[cfg(test)]
    pub fn recording(recorder: &'a mut OpRecorder) -> Self {
        Self {
            sink: PaintSink::Rec(recorder),
        }
    }

    /// A painter that keeps nothing, for measuring what painting itself costs.
    #[cfg(test)]
    pub fn null() -> Self {
        Self {
            sink: PaintSink::Null,
        }
    }

    #[cfg(test)]
    fn record(&mut self, op: impl FnOnce() -> PaintOp) {
        if let PaintSink::Rec(recorder) = &mut self.sink {
            recorder.ops.push(op());
        }
    }

    /// In a release build the op is never constructed, so recording costs
    /// nothing — not even the string it would have allocated.
    #[cfg(not(test))]
    #[inline(always)]
    fn record(&mut self, _op: impl FnOnce() -> PaintOp) {}

    pub fn set_fill(&mut self, color: &str) {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.set_fill_style_str(color);
        }
        self.record(|| PaintOp::SetFill(color.to_string()));
    }

    pub fn set_fill_gradient(&mut self, gradient: &Gradient) -> Result<(), JsValue> {
        if let PaintSink::Web(ctx) = &mut self.sink {
            let (canvas_gradient, stops) = match gradient {
                Gradient::Radial {
                    x0,
                    y0,
                    r0,
                    x1,
                    y1,
                    r1,
                    stops,
                } => (
                    ctx.create_radial_gradient(*x0, *y0, *r0, *x1, *y1, *r1)?,
                    stops,
                ),
                Gradient::Linear {
                    x0,
                    y0,
                    x1,
                    y1,
                    stops,
                } => (ctx.create_linear_gradient(*x0, *y0, *x1, *y1), stops),
            };
            for stop in stops {
                canvas_gradient.add_color_stop(stop.offset as f32, &stop.color)?;
            }
            ctx.set_fill_style_canvas_gradient(&canvas_gradient);
        }
        self.record(|| PaintOp::SetFillGradient(gradient.clone()));
        Ok(())
    }

    pub fn set_stroke(&mut self, color: &str) {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.set_stroke_style_str(color);
        }
        self.record(|| PaintOp::SetStroke(color.to_string()));
    }

    pub fn set_line_width(&mut self, width: f64) {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.set_line_width(width);
        }
        self.record(|| PaintOp::SetLineWidth(width));
    }

    pub fn set_line_cap(&mut self, cap: &str) {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.set_line_cap(cap);
        }
        self.record(|| PaintOp::SetLineCap(cap.to_string()));
    }

    /// Layer opacity.
    ///
    /// Deliberately `globalAlpha` rather than an `rgba(...)` string: a layer's
    /// opacity is animatable, and rebuilding a colour string every frame is
    /// exactly the per-frame allocation section 11 budgets to zero.
    pub fn set_global_alpha(&mut self, alpha: f64) {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.set_global_alpha(alpha);
        }
        self.record(|| PaintOp::SetGlobalAlpha(alpha));
    }

    pub fn begin_path(&mut self) {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.begin_path();
        }
        self.record(|| PaintOp::BeginPath);
    }

    pub fn move_to(&mut self, x: f64, y: f64) {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.move_to(x, y);
        }
        self.record(|| PaintOp::MoveTo(x, y));
    }

    pub fn line_to(&mut self, x: f64, y: f64) {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.line_to(x, y);
        }
        self.record(|| PaintOp::LineTo(x, y));
    }

    pub fn arc(
        &mut self,
        x: f64,
        y: f64,
        radius: f64,
        start_angle: f64,
        end_angle: f64,
    ) -> Result<(), JsValue> {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.arc(x, y, radius, start_angle, end_angle)?;
        }
        self.record(|| PaintOp::Arc(x, y, radius, start_angle, end_angle));
        Ok(())
    }

    /// Add a rectangle subpath. Used to build the `cells` clip shape, which is
    /// a union of cell squares rather than the rounded silhouette.
    pub fn rect(&mut self, x: f64, y: f64, width: f64, height: f64) {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.rect(x, y, width, height);
        }
        self.record(|| PaintOp::Rect(x, y, width, height));
    }

    pub fn close_path(&mut self) {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.close_path();
        }
        self.record(|| PaintOp::ClosePath);
    }

    /// Clip to the current path, nonzero winding.
    ///
    /// One clip per snake, never per layer — per-layer clipping is what would
    /// make the compositor expensive (`specs/skin-shading-prd.md` section 10).
    pub fn clip(&mut self) {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.clip();
        }
        self.record(|| PaintOp::Clip);
    }

    pub fn fill(&mut self) {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.fill();
        }
        self.record(|| PaintOp::Fill);
    }

    pub fn stroke(&mut self) {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.stroke();
        }
        self.record(|| PaintOp::Stroke);
    }

    /// Blit a sub-rectangle of an atlas image.
    ///
    /// A frame strip is this op with a moving source rectangle — one
    /// `drawImage` with different arguments, which satisfies op-count
    /// invariance exactly. That is why `specs/skin-shading-prd.md` section 8.4
    /// makes frame strips the sanctioned way to animate sprite art.
    ///
    /// **The op is recorded whether or not the pixels have arrived.** That is
    /// what keeps op-count invariance a property of the pose rather than of the
    /// network: an atlas decoding mid-match would otherwise change a skin's op
    /// sequence between two frames of the same snake. Canvas refuses to draw a
    /// broken image at all — it throws — so the readiness check is here rather
    /// than left to the browser, and a texture that never arrives simply leaves
    /// the layer beneath it showing.
    pub fn draw_image(
        &mut self,
        image: usize,
        source: (f64, f64, f64, f64),
        dest: (f64, f64, f64, f64),
    ) -> Result<(), JsValue> {
        if let PaintSink::Web(ctx) = &mut self.sink
            && crate::skin::atlas::is_ready(image)
            && let Some(element) = crate::skin::atlas::image_element(image)
        {
            ctx.draw_image_with_html_image_element_and_sw_and_sh_and_dx_and_dy_and_dw_and_dh(
                &element, source.0, source.1, source.2, source.3, dest.0, dest.1, dest.2, dest.3,
            )?;
            crate::skin::atlas::record_draw(image);
        }
        self.record(|| PaintOp::DrawImage {
            image,
            source,
            dest,
        });
        Ok(())
    }

    pub fn fill_rect(&mut self, x: f64, y: f64, width: f64, height: f64) {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.fill_rect(x, y, width, height);
        }
        self.record(|| PaintOp::FillRect(x, y, width, height));
    }

    pub fn save(&mut self) {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.save();
        }
        self.record(|| PaintOp::Save);
    }

    pub fn restore(&mut self) {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.restore();
        }
        self.record(|| PaintOp::Restore);
    }

    pub fn translate(&mut self, x: f64, y: f64) -> Result<(), JsValue> {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.translate(x, y)?;
        }
        self.record(|| PaintOp::Translate(x, y));
        Ok(())
    }

    pub fn rotate(&mut self, radians: f64) -> Result<(), JsValue> {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.rotate(radians)?;
        }
        self.record(|| PaintOp::Rotate(radians));
        Ok(())
    }

    pub fn scale(&mut self, x: f64, y: f64) -> Result<(), JsValue> {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.scale(x, y)?;
        }
        self.record(|| PaintOp::Scale(x, y));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn transform(
        &mut self,
        a: f64,
        b: f64,
        c: f64,
        d: f64,
        e: f64,
        f: f64,
    ) -> Result<(), JsValue> {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.transform(a, b, c, d, e, f)?;
        }
        self.record(|| PaintOp::Transform(a, b, c, d, e, f));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn set_transform(
        &mut self,
        a: f64,
        b: f64,
        c: f64,
        d: f64,
        e: f64,
        f: f64,
    ) -> Result<(), JsValue> {
        if let PaintSink::Web(ctx) = &mut self.sink {
            ctx.set_transform(a, b, c, d, e, f)?;
        }
        self.record(|| PaintOp::SetTransform(a, b, c, d, e, f));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shortest-round-trip float formatting is injective, which is the whole
    /// basis for treating a golden text diff as an exact pixel diff.
    #[test]
    fn golden_float_formatting_distinguishes_neighbouring_values() {
        let one = PaintOp::SetLineWidth(1.0);
        let barely_more = PaintOp::SetLineWidth(1.0 + f64::EPSILON);
        assert_ne!(one.to_golden_line(), barely_more.to_golden_line());

        // ...and round-trips, so a golden line is a faithful record.
        let rendered = PaintOp::FillRect(-1.0, 0.30000000000000004, 12.5, 1e-9).to_golden_line();
        assert_eq!(rendered, "fill_rect(-1.0, 0.30000000000000004, 12.5, 1e-9)");
    }

    #[test]
    fn recorder_tracks_line_width_when_measuring_stroke_extent() {
        let mut recorder = OpRecorder::new();
        let mut ctx = PaintCtx::recording(&mut recorder);
        ctx.set_line_width(10.0);
        ctx.move_to(0.0, 0.0);
        ctx.line_to(0.0, 0.0);

        // A 10px-wide stroke reaches 5px either side of its path.
        assert_eq!(recorder.painted_extent(), Some((-5.0, -5.0, 5.0, 5.0)));
    }

    #[test]
    fn shapes_ignore_arguments_so_animation_can_be_checked_structurally() {
        let mut first = OpRecorder::new();
        let mut second = OpRecorder::new();
        PaintCtx::recording(&mut first).set_fill("#000000");
        PaintCtx::recording(&mut second).set_fill("#ffffff");
        assert_eq!(first.shapes(), second.shapes());
        assert_ne!(first.to_golden(), second.to_golden());
    }

    /// The property the whole overhang check rests on once transforms exist:
    /// a translated rectangle is measured where it lands, not where it was
    /// written. Without this the first `translate` in any skin would turn the
    /// check into a no-op that still reported success.
    #[test]
    fn painted_extent_follows_the_transform() {
        let mut recorder = OpRecorder::new();
        {
            let mut ctx = PaintCtx::recording(&mut recorder);
            ctx.translate(100.0, 50.0).unwrap();
            ctx.fill_rect(0.0, 0.0, 10.0, 10.0);
        }
        assert_eq!(
            recorder.painted_extent(),
            Some((100.0, 50.0, 110.0, 60.0)),
            "the recorder measured pre-transform coordinates"
        );
    }

    #[test]
    fn save_and_restore_unwind_the_transform() {
        let mut recorder = OpRecorder::new();
        {
            let mut ctx = PaintCtx::recording(&mut recorder);
            ctx.save();
            ctx.translate(1000.0, 1000.0).unwrap();
            ctx.restore();
            ctx.fill_rect(0.0, 0.0, 4.0, 4.0);
        }
        assert_eq!(recorder.painted_extent(), Some((0.0, 0.0, 4.0, 4.0)));
    }

    /// Scale multiplies a stroke's width as well as its position, so a skin
    /// cannot hide overhang inside a scale factor either.
    #[test]
    fn scale_widens_strokes_as_well_as_moving_them() {
        let mut recorder = OpRecorder::new();
        {
            let mut ctx = PaintCtx::recording(&mut recorder);
            ctx.scale(3.0, 3.0).unwrap();
            ctx.set_line_width(2.0);
            ctx.move_to(10.0, 0.0);
            // A segment rather than the bare `move_to` this used to probe
            // with: a move lays down no ink, so measuring one was measuring
            // the model rather than the picture. The subject is unchanged —
            // whether the transform scales the pen as well as the point.
            ctx.line_to(10.0, 0.0);
        }
        // The point lands at x=30, and the 2px stroke is 6px wide there.
        assert_eq!(recorder.painted_extent(), Some((27.0, -3.0, 33.0, 3.0)));
    }

    /// A rotation maps an axis-aligned box to a diamond; the recorder reports
    /// the enclosing box, which is never smaller than the truth.
    #[test]
    fn rotation_is_measured_conservatively() {
        let mut recorder = OpRecorder::new();
        {
            let mut ctx = PaintCtx::recording(&mut recorder);
            ctx.rotate(std::f64::consts::FRAC_PI_4).unwrap();
            ctx.fill_rect(-1.0, -1.0, 2.0, 2.0);
        }
        let (x0, y0, x1, y1) = recorder.painted_extent().expect("painted something");
        let half = 2.0_f64.sqrt();
        assert!((x0 + half).abs() < 1e-9 && (x1 - half).abs() < 1e-9);
        assert!((y0 + half).abs() < 1e-9 && (y1 - half).abs() < 1e-9);
    }

    /// A clip can only shrink what a skin is measured as having painted —
    /// which is what makes "body layers cannot escape the silhouette" a
    /// checkable claim rather than a convention.
    #[test]
    fn a_clip_bounds_what_counts_as_painted() {
        let mut recorder = OpRecorder::new();
        {
            let mut ctx = PaintCtx::recording(&mut recorder);
            ctx.begin_path();
            ctx.rect(0.0, 0.0, 10.0, 10.0);
            ctx.clip();
            // Paints far outside the clip; none of it reaches the canvas.
            ctx.fill_rect(-100.0, -100.0, 500.0, 500.0);
        }
        assert_eq!(recorder.painted_extent(), Some((0.0, 0.0, 10.0, 10.0)));
    }

    #[test]
    fn a_restored_clip_stops_constraining() {
        let mut recorder = OpRecorder::new();
        {
            let mut ctx = PaintCtx::recording(&mut recorder);
            ctx.save();
            ctx.begin_path();
            ctx.rect(0.0, 0.0, 10.0, 10.0);
            ctx.clip();
            ctx.restore();
            ctx.fill_rect(0.0, 0.0, 50.0, 50.0);
        }
        assert_eq!(recorder.painted_extent(), Some((0.0, 0.0, 50.0, 50.0)));
    }
}
