//! Body space, and the lowerings that put things into it.
//!
//! A Snaketron body is a polyline of axis-aligned runs, so the coordinate
//! system a skin author wants — `s` along the snake from the head, `t` across
//! it — maps to the screen through exactly one similarity transform per run.
//! That is the whole reason `specs/skin-shading-prd.md` picks body space
//! (section 5): tiling, relative placement and sprite spans become arithmetic
//! instead of ad-hoc pixel math, and no source ever has to know which way the
//! snake is pointing.
//!
//! | Space | Meaning |
//! | --- | --- |
//! | `s` | arc length from the head centre, in cells |
//! | `t` | across the body, `-0.5..0.5` |
//!
//! Nothing here allocates. Every walk is a callback over the compressed body,
//! because all of it runs once per snake per frame.

// `CornerPolicy::Bisector` is implemented and tested but unused by the six
// shipped skins, all of which are solid-bodied and cannot see a corner seam.
// See the note in `skin::layer`.
#![allow(dead_code)]

use crate::skin::PaintCtx;
use crate::skin::geometry::for_each_body_cell;
use crate::skin::paint::Affine;
use wasm_bindgen::prelude::*;

const FULL_CIRCLE: f64 = 2.0 * std::f64::consts::PI;

/// One straight run of the body, annotated with where it sits along the snake.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Run {
    /// Cell coordinates of the run's first point, head-most.
    pub from: (f64, f64),
    /// Cell coordinates of the run's last point.
    pub to: (f64, f64),
    /// Arc length at `from`, in cells from the head centre.
    pub s0: f64,
    /// Length of the run in cells, centre to centre. Zero only for the
    /// degenerate run a one-cell body produces.
    pub len: f64,
    /// Index of this run in the body, head first.
    pub index: usize,
    /// Whether another run follows this one.
    pub has_next: bool,
}

impl Run {
    /// Arc length at the run's far end.
    pub fn s1(self) -> f64 {
        self.s0 + self.len
    }

    /// Unit travel direction in cell coordinates.
    ///
    /// A body of one cell has no travel at all. It still needs a body frame —
    /// a span layer has to be able to paint it — so it gets the canonical one,
    /// pointing right. Which way a headless dot faces is not observable.
    pub fn direction(self) -> (f64, f64) {
        let (dx, dy) = (self.to.0 - self.from.0, self.to.1 - self.from.1);
        if dx == 0.0 && dy == 0.0 {
            return (1.0, 0.0);
        }
        let length = self.len.max(1.0);
        (dx / length, dy / length)
    }

    /// The transform from body space to screen pixels for this run.
    ///
    /// Maps `(s - s0, t)` to a pixel: travel along `+x`, across along `+y`,
    /// scaled by the cell size and rotated to the run's heading. It is a
    /// similarity — no shear, no anisotropy — which is what lets a source be
    /// baked once and reused at every orientation.
    pub fn affine(self, cell_size: f64) -> Affine {
        let (dx, dy) = self.direction();
        let origin = (
            self.from.0 * cell_size + cell_size / 2.0,
            self.from.1 * cell_size + cell_size / 2.0,
        );
        Affine {
            a: dx * cell_size,
            b: dy * cell_size,
            c: -dy * cell_size,
            d: dx * cell_size,
            e: origin.0,
            f: origin.1,
        }
    }

    /// A point in body space, in screen pixels.
    pub fn point(self, cell_size: f64, s: f64, t: f64) -> (f64, f64) {
        self.affine(cell_size).apply(s - self.s0, t)
    }

    /// The arc range this run's ribbon covers under a corner policy.
    ///
    /// The half-cell beyond each end is the run's cap. At an interior joint the
    /// two runs would otherwise both claim the whole joint cell, so the policy
    /// decides how much each keeps — see [`CornerPolicy`].
    pub fn ribbon_range(self, policy: CornerPolicy) -> (f64, f64) {
        let head_side = if self.index == 0 {
            0.5
        } else {
            policy.head_reach()
        };
        let tail_side = if self.has_next {
            policy.tail_reach()
        } else {
            0.5
        };
        (self.s0 - head_side, self.s1() + tail_side)
    }
}

/// How two runs divide the cell they share.
///
/// A cell-wide ribbon turning 90 degrees leaves an unfilled outer wedge of
/// `c² − πc²/4` — **21% of a cell's area, at every cell size**. It does not
/// improve on larger cells; it grows in absolute pixels. There is no policy
/// that makes the wedge disappear without changing the silhouette, and changing
/// the silhouette changes overhang, which moves the roster layout
/// (`specs/skin-shading-prd.md` section 14).
///
/// `Fan` — a polar sweep through the corner — is deliberately **not** here. The
/// PRD says it ships only with a skin that proves it is needed, and a schema
/// surface with no implementation behind it is worse than an absence.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CornerPolicy {
    /// The head-ward run paints the **whole** joint cell; the tail-ward run
    /// does not touch it.
    ///
    /// Splitting the cell down the middle was the first attempt and it is
    /// geometrically incoherent: each run keeps the half nearer its own end
    /// *in its own frame*, and at a turn those two halves are perpendicular. So
    /// a quarter of the cell gets painted twice and the opposite quarter never
    /// gets painted at all — a notch of bare body on the outside of every
    /// corner, plainly visible on any textured skin.
    ///
    /// Giving the cell to one run costs nothing and fixes both: coverage is
    /// exact, and the orientation flip moves from the cell's midline to its
    /// far edge, which is where a viewer already expects the body to turn.
    #[default]
    Own,
    /// Each run reaches the joint cell's far edge, so the two overlap across
    /// the whole cell.
    ///
    /// The later run wins the overlap, which turns one hard break into a
    /// softer one placed at the cell boundary instead of its centre. Only
    /// meaningful for path-fillable sources; image and pattern layers are
    /// `Own`-only, because honouring a non-rectangular boundary for a blit
    /// would need a per-run clip and section 10 forbids that on cost grounds.
    Bisector,
}

impl CornerPolicy {
    /// How far back past a joint at the run's **head** end its ribbon reaches.
    ///
    /// Negative under `Own`: the joint cell belongs to the run before this one,
    /// so this one starts half a cell late and leaves it alone.
    fn head_reach(self) -> f64 {
        match self {
            CornerPolicy::Own => -0.5,
            CornerPolicy::Bisector => 0.5,
        }
    }

    /// How far past a joint at the run's **tail** end its ribbon reaches.
    ///
    /// Half a cell under either policy, which is the whole joint cell. They
    /// differ in what the *next* run does with it: `Own` skips it, `Bisector`
    /// paints it again on top.
    fn tail_reach(self) -> f64 {
        0.5
    }

    /// Whether a source that can only fill axis-aligned rectangles may use it.
    pub fn allows_blits(self) -> bool {
        matches!(self, CornerPolicy::Own)
    }
}

/// Walk the body's runs, head first, with arc lengths attached.
///
/// Pairs that are neither horizontal nor vertical yield nothing, matching the
/// renderer's long-standing behaviour of skipping a pair it cannot draw as an
/// axis-aligned run.
pub fn for_each_run(cells: &[(f64, f64)], mut visit: impl FnMut(Run)) {
    // A one-cell body is a run of zero length rather than no run at all. It has
    // no arc length, but it does have a cell to paint, and a span layer that
    // saw nothing here would render a freshly spawned snake invisible.
    if cells.len() == 1 {
        visit(Run {
            from: cells[0],
            to: cells[0],
            s0: 0.0,
            len: 0.0,
            index: 0,
            has_next: false,
        });
        return;
    }

    let mut s0 = 0.0;
    let mut index = 0usize;
    let pairs = cells.len().saturating_sub(1);

    for pair in 0..pairs {
        let (from, to) = (cells[pair], cells[pair + 1]);
        let (dx, dy) = (to.0 - from.0, to.1 - from.1);
        let len = if dx.abs() < 0.01 {
            dy.abs()
        } else if dy.abs() < 0.01 {
            dx.abs()
        } else {
            continue;
        };
        if len == 0.0 {
            continue;
        }

        // `has_next` has to mean "another run follows", not "another pair
        // follows": a diagonal pair produces no run, and treating it as one
        // would leave the previous run's cap off the end of the snake.
        let has_next = (pair + 1..pairs).any(|later| {
            let (a, b) = (cells[later], cells[later + 1]);
            let (ldx, ldy) = (b.0 - a.0, b.1 - a.1);
            (ldx.abs() < 0.01 && ldy.abs() >= 0.01) || (ldy.abs() < 0.01 && ldx.abs() >= 0.01)
        });

        visit(Run {
            from,
            to,
            s0,
            len,
            index,
            has_next,
        });
        s0 += len;
        index += 1;
    }
}

/// Total arc length of the body in cells, head centre to tail centre.
pub fn arc_length(cells: &[(f64, f64)]) -> f64 {
    let mut total = 0.0;
    for_each_run(cells, |run| total = run.s1());
    total
}

/// The two shapes a body layer may be clipped to.
///
/// The distinction is load-bearing rather than a nicety. Classic's
/// head-proximity ramp paints unclipped full-cell rectangles, deliberately
/// tinting pixels inside the body's cells but *outside* its rounded silhouette
/// — the surround of the head cap and every joint's outer wedge. At `cell = 15`
/// that is roughly 24 px² per affected cell. Clipping that ramp to the
/// silhouette would change those pixels, and classic-as-layers would miss its
/// parity target for a reason that has nothing to do with the compositor being
/// wrong.
///
/// Neither shape exceeds the body's cells, so neither contributes overhang.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ClipShape {
    /// The capsule union: run strips with round caps, filled nonzero. What the
    /// snake actually looks like, and what an author almost always means.
    #[default]
    Silhouette,
    /// The union of the body's cell squares. Larger than the silhouette at
    /// every cap and every outer corner.
    Cells,
}

/// Build a clip path for the body and apply it.
///
/// One clip per snake, never per layer — per-layer clipping is what would make
/// the compositor expensive (`specs/skin-shading-prd.md` section 10). Callers
/// are expected to have issued `save` first and to `restore` afterwards.
pub fn clip_to_body(
    ctx: &mut PaintCtx,
    cells: &[(f64, f64)],
    cell_size: f64,
    shape: ClipShape,
) -> Result<(), JsValue> {
    ctx.begin_path();
    match shape {
        ClipShape::Silhouette => build_silhouette(ctx, cells, cell_size)?,
        ClipShape::Cells => build_cells(ctx, cells, cell_size),
    }
    ctx.clip();
    Ok(())
}

/// The capsule union, as path subpaths.
///
/// Every subpath winds the same way, so a nonzero fill of the whole thing is
/// their union — overlaps at joints cost winding number, not correctness.
fn build_silhouette(
    ctx: &mut PaintCtx,
    cells: &[(f64, f64)],
    cell_size: f64,
) -> Result<(), JsValue> {
    let radius = cell_size / 2.0;
    let centre = |cell: (f64, f64)| (cell.0 * cell_size + radius, cell.1 * cell_size + radius);

    if cells.len() == 1 {
        let (x, y) = centre(cells[0]);
        ctx.arc(x, y, radius, 0.0, FULL_CIRCLE)?;
        return Ok(());
    }

    let mut error = None;
    for_each_run(cells, |run| {
        if error.is_some() {
            return;
        }
        let (ax, ay) = centre(run.from);
        let (bx, by) = centre(run.to);
        let (x0, y0) = (ax.min(bx), ay.min(by));
        let (x1, y1) = (ax.max(bx), ay.max(by));
        // The straight part runs **centre to centre** and is half a cell wide
        // either side of the run; the discs below are what round the caps.
        //
        // Extending the rectangle by the radius along the run axis as well —
        // which is what this did until a checkerboard was the first skin to
        // clip against it — squares off both caps, so `Silhouette` quietly
        // became `Cells` for any straight body. Nothing caught it because no
        // shipped skin had a layer that clipped: classic's ribbon *is* the
        // silhouette, so it emits no clip at all. A patterned body layer is
        // the first thing that can paint into the corner the cap rounds away,
        // and it duly did, right where the contour should have been.
        let (rx, ry, rw, rh) = if x1 > x0 {
            (x0, y0 - radius, x1 - x0, cell_size)
        } else if y1 > y0 {
            (x0 - radius, y0, cell_size, y1 - y0)
        } else {
            // No extent in either axis: the discs are the whole shape.
            (x0, y0, 0.0, 0.0)
        };
        ctx.rect(rx, ry, rw, rh);
        if let Err(cause) = ctx.arc(ax, ay, radius, 0.0, FULL_CIRCLE) {
            error = Some(cause);
            return;
        }
        if let Err(cause) = ctx.arc(bx, by, radius, 0.0, FULL_CIRCLE) {
            error = Some(cause);
        }
    });

    match error {
        Some(cause) => Err(cause),
        None => Ok(()),
    }
}

/// The union of the body's cell squares.
fn build_cells(ctx: &mut PaintCtx, cells: &[(f64, f64)], cell_size: f64) {
    if cells.len() == 1 {
        ctx.rect(
            cells[0].0 * cell_size,
            cells[0].1 * cell_size,
            cell_size,
            cell_size,
        );
        return;
    }

    // Repeats are harmless: a nonzero fill of two identical rectangles wound
    // the same way is still that rectangle, so a self-crossing body needs no
    // dedup here.
    for_each_body_cell(cells, |x, y, _| {
        ctx.rect(
            x as f64 * cell_size,
            y as f64 * cell_size,
            cell_size,
            cell_size,
        );
        true
    });
}

/// How to emit one stroked capsule ribbon.
///
/// This is the lowering a solid full-body layer uses, and it is the same
/// grammar the hand-written classic painter has always emitted: stroked runs
/// with round caps, plus a disc at each joint. Keeping it means classic
/// expressed as layers produces the *same op stream* as classic, so the golden
/// trace recorded before the skin system existed keeps proving what it always
/// proved — a stronger outcome than the pixel tolerance
/// `specs/skin-shading-prd.md` section 12 budgets for.
///
/// `fill_before_strokes` and `refill_before_tail_cap` exist only for that
/// parity. Classic sets its fill style up front and then redundantly re-sets it
/// before the tail disc; both are no-ops for the picture and neither is worth
/// preserving on its own merits, but reproducing them is what keeps the
/// comparison byte-exact rather than tolerance-based.
#[derive(Clone, Copy, Debug)]
pub struct RibbonPlan<'a> {
    pub color: &'a str,
    /// Total width beyond one cell, across both sides.
    pub extra: f64,
    /// A disc at every interior joint, rounding the corners.
    pub joints: bool,
    /// A disc at the tail.
    pub tail_cap: bool,
    /// Set the fill style before the strokes rather than before the joints.
    pub fill_before_strokes: bool,
    /// Re-set the fill style immediately before the tail disc.
    pub refill_before_tail_cap: bool,
    /// Paint the whole ribbon as **one** stroked path.
    ///
    /// The default emits a stroke per run plus a disc per joint, and at a joint
    /// all three are the *same circle*: `radius` is exactly half `line_width`,
    /// so a run's round cap and the joint disc have identical boundaries. With
    /// an opaque colour the interior is unchanged — but the antialiased edge
    /// pixels composite two or three times, turning about 50% coverage into
    /// about 87%. The elbow of every corner comes out harder and heavier than
    /// the rest of the outline, which is visible and was reported as such.
    ///
    /// One path with one `stroke` rasterises the union in a single coverage
    /// pass, so a shared boundary is antialiased once. The joint and tail discs
    /// are then redundant — the round caps already cover exactly those circles
    /// — and are skipped.
    ///
    /// Off by default because classic's committed trace pins its op sequence
    /// byte for byte, and that trace is the tripwire for accidental changes to
    /// shared painting.
    pub single_pass: bool,
}

impl RibbonPlan<'_> {
    fn line_width(&self, cell_size: f64) -> f64 {
        cell_size + self.extra
    }

    fn radius(&self, cell_size: f64) -> f64 {
        cell_size / 2.0 + self.extra / 2.0
    }
}

/// Paint a ribbon along the whole body.
///
/// A one-cell body has no runs, so it lowers to a single disc — which is
/// exactly what a snake that is only a head looks like.
pub fn emit_ribbon(
    ctx: &mut PaintCtx,
    cells: &[(f64, f64)],
    cell_size: f64,
    plan: &RibbonPlan<'_>,
) -> Result<(), JsValue> {
    if cells.is_empty() {
        return Ok(());
    }

    let radius = plan.radius(cell_size);
    let centre = |cell: (f64, f64)| {
        (
            cell.0 * cell_size + cell_size / 2.0,
            cell.1 * cell_size + cell_size / 2.0,
        )
    };

    if cells.len() == 1 {
        let (x, y) = centre(cells[0]);
        ctx.set_fill(plan.color);
        ctx.begin_path();
        ctx.arc(x, y, radius, 0.0, FULL_CIRCLE)?;
        ctx.fill();
        return Ok(());
    }

    ctx.set_stroke(plan.color);
    if plan.fill_before_strokes {
        ctx.set_fill(plan.color);
    }

    if plan.single_pass {
        ctx.set_line_width(plan.line_width(cell_size));
        ctx.set_line_cap("round");
        ctx.begin_path();
        for_each_run(cells, |run| {
            let (ax, ay) = centre(run.from);
            let (bx, by) = centre(run.to);
            if run.from.0 == run.to.0 {
                ctx.move_to(ax, ay.min(by));
                ctx.line_to(ax, ay.max(by));
            } else {
                ctx.move_to(ax.min(bx), ay);
                ctx.line_to(ax.max(bx), ay);
            }
        });
        ctx.stroke();
        return Ok(());
    }

    let mut error = None;
    for_each_run(cells, |run| {
        if error.is_some() {
            return;
        }
        let (ax, ay) = centre(run.from);
        let (bx, by) = centre(run.to);
        ctx.set_line_width(plan.line_width(cell_size));
        ctx.set_line_cap("round");
        ctx.begin_path();
        if run.from.0 == run.to.0 {
            ctx.move_to(ax, ay.min(by));
            ctx.line_to(ax, ay.max(by));
        } else {
            ctx.move_to(ax.min(bx), ay);
            ctx.line_to(ax.max(bx), ay);
        }
        ctx.stroke();
    });
    if let Some(cause) = error.take() {
        return Err(cause);
    }

    if plan.joints {
        if !plan.fill_before_strokes {
            ctx.set_fill(plan.color);
        }
        for joint in &cells[1..cells.len() - 1] {
            let (x, y) = centre(*joint);
            ctx.begin_path();
            ctx.arc(x, y, radius, 0.0, FULL_CIRCLE)?;
            ctx.fill();
        }
    }

    if plan.tail_cap {
        if plan.refill_before_tail_cap {
            ctx.set_fill(plan.color);
        }
        let (x, y) = centre(cells[cells.len() - 1]);
        ctx.begin_path();
        ctx.arc(x, y, cell_size / 2.0, 0.0, FULL_CIRCLE)?;
        ctx.fill();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skin::fixtures::POSES;
    use crate::skin::paint::OpRecorder;

    fn runs(cells: &[(f64, f64)]) -> Vec<Run> {
        let mut collected = Vec::new();
        for_each_run(cells, |run| collected.push(run));
        collected
    }

    #[test]
    fn runs_carry_arc_length_along_the_body() {
        let found = runs(&[(0.0, 0.0), (4.0, 0.0), (4.0, 3.0)]);
        assert_eq!(found.len(), 2);
        assert_eq!((found[0].s0, found[0].len), (0.0, 4.0));
        assert_eq!((found[1].s0, found[1].len), (4.0, 3.0));
        assert_eq!(arc_length(&[(0.0, 0.0), (4.0, 0.0), (4.0, 3.0)]), 7.0);
    }

    /// A diagonal pair draws nothing, so the run before it is the last run and
    /// must still get its tail cap. Deriving `has_next` from the pair index
    /// instead would silently cut the end off such a body.
    #[test]
    fn a_diagonal_pair_does_not_count_as_a_following_run() {
        let found = runs(&[(0.0, 0.0), (4.0, 0.0), (7.0, 3.0)]);
        assert_eq!(found.len(), 1);
        assert!(!found[0].has_next, "the diagonal pair produced no run");
    }

    /// The affine is the whole reason body space is tractable: it has to put
    /// `s` along the snake and `t` across it, for every heading.
    #[test]
    fn the_run_affine_maps_body_space_onto_the_screen() {
        let cell = 10.0;
        for (cells, heading) in [
            (vec![(1.0, 1.0), (5.0, 1.0)], "right"),
            (vec![(5.0, 1.0), (1.0, 1.0)], "left"),
            (vec![(1.0, 1.0), (1.0, 5.0)], "down"),
            (vec![(1.0, 5.0), (1.0, 1.0)], "up"),
        ] {
            let run = runs(&cells)[0];
            let start = run.point(cell, 0.0, 0.0);
            let expected_start = (
                cells[0].0 * cell + cell / 2.0,
                cells[0].1 * cell + cell / 2.0,
            );
            assert_eq!(start, expected_start, "{heading}: s=0 is the head centre");

            // s advances toward the tail...
            let along = run.point(cell, run.len, 0.0);
            let expected_end = (
                cells[1].0 * cell + cell / 2.0,
                cells[1].1 * cell + cell / 2.0,
            );
            assert!(
                (along.0 - expected_end.0).abs() < 1e-9 && (along.1 - expected_end.1).abs() < 1e-9,
                "{heading}: s={} should reach the far end",
                run.len
            );

            // ...and t is perpendicular, half a cell to each side.
            let edge = run.point(cell, 0.0, 0.5);
            let distance = ((edge.0 - start.0).powi(2) + (edge.1 - start.1).powi(2)).sqrt();
            assert!(
                (distance - cell / 2.0).abs() < 1e-9,
                "{heading}: t=0.5 should be half a cell across"
            );
        }
    }

    /// One pass means one coverage computation, which is the whole point: the
    /// default paints a joint's circle up to three times — two round caps and a
    /// disc, all the same circle — and an antialiased boundary composited three
    /// times is a visibly heavier edge even in a fully opaque colour.
    #[test]
    fn a_single_pass_ribbon_paints_each_boundary_once() {
        let cells = [(0.0, 0.0), (4.0, 0.0), (4.0, 3.0)];
        let plan = |single_pass| RibbonPlan {
            color: "#1b6fd0",
            extra: 5.0,
            joints: true,
            tail_cap: false,
            fill_before_strokes: false,
            refill_before_tail_cap: false,
            single_pass,
        };
        let count = |single_pass| {
            let mut recorder = crate::skin::paint::OpRecorder::new();
            emit_ribbon(
                &mut PaintCtx::recording(&mut recorder),
                &cells,
                10.0,
                &plan(single_pass),
            )
            .expect("a recording painter cannot fail");
            let strokes = recorder
                .ops()
                .iter()
                .filter(|op| matches!(op, crate::skin::paint::PaintOp::Stroke))
                .count();
            let fills = recorder
                .ops()
                .iter()
                .filter(|op| matches!(op, crate::skin::paint::PaintOp::Fill))
                .count();
            (strokes, fills)
        };

        // Two runs and one interior joint: three paints over the joint circle.
        assert_eq!(count(false), (2, 1));
        // One paint, covering exactly the same union.
        assert_eq!(count(true), (1, 0));
    }

    /// Corner policy decides who owns the shared cell. Under `Own` the
    /// head-ward run takes all of it and the tail-ward run starts past it, so
    /// the two ranges **abut exactly**: no gap to leave a bare notch, no
    /// overlap to paint twice. `Bisector` instead lets both cover it.
    #[test]
    fn corner_policy_hands_the_joint_cell_to_one_run() {
        let cells = [(0.0, 0.0), (4.0, 0.0), (4.0, 3.0)];
        let found = runs(&cells);

        let (head_start, head_end) = found[0].ribbon_range(CornerPolicy::Own);
        assert_eq!(
            (head_start, head_end),
            (-0.5, 4.5),
            "the head-ward run takes the whole joint cell"
        );
        let (tail_start, tail_end) = found[1].ribbon_range(CornerPolicy::Own);
        assert_eq!(
            (tail_start, tail_end),
            (4.5, 7.5),
            "the tail-ward run starts past it"
        );
        assert_eq!(head_end, tail_start, "the two must abut exactly");

        // Total covered arc equals the paintable body, once over.
        let covered: f64 = found
            .iter()
            .map(|run| {
                let (from, to) = run.ribbon_range(CornerPolicy::Own);
                to - from
            })
            .sum();
        assert_eq!(
            covered,
            arc_length(&cells) + 1.0,
            "every cell painted exactly once, caps included"
        );

        let (_, overlap_end) = found[0].ribbon_range(CornerPolicy::Bisector);
        let (overlap_start, _) = found[1].ribbon_range(CornerPolicy::Bisector);
        assert!(
            overlap_end > overlap_start,
            "bisector has the two runs overlap across the joint cell"
        );

        assert!(CornerPolicy::Own.allows_blits());
        assert!(
            !CornerPolicy::Bisector.allows_blits(),
            "a diagonal boundary is not an axis-aligned rectangle, so drawImage \
             cannot honour it without a per-run clip"
        );
    }

    /// The two clip shapes have to actually differ, and in the direction the
    /// classic ramp depends on: `Cells` must reach past `Silhouette` at a cap.
    #[test]
    fn the_cells_clip_is_larger_than_the_silhouette() {
        let cells = [(1.0, 1.0), (4.0, 1.0)];
        let cell_size = 10.0;

        let measure = |shape: ClipShape| {
            let mut recorder = OpRecorder::new();
            {
                let mut ctx = PaintCtx::recording(&mut recorder);
                ctx.save();
                clip_to_body(&mut ctx, &cells, cell_size, shape).unwrap();
                // Paint far wider than the body; the clip is what bounds it.
                ctx.fill_rect(-500.0, -500.0, 1000.0, 1000.0);
                ctx.restore();
            }
            recorder.painted_extent().expect("something was painted")
        };

        let (sx0, sy0, sx1, sy1) = measure(ClipShape::Silhouette);
        let (cx0, cy0, cx1, cy1) = measure(ClipShape::Cells);

        // The body occupies cells x=1..4, y=1 -> pixels 10..50, 10..20.
        assert_eq!((cx0, cy0, cx1, cy1), (10.0, 10.0, 50.0, 20.0));
        // The silhouette's round caps stop at the same bounding box here, so
        // compare where they genuinely differ: the corner of that box, which
        // the capsule misses and the cell square fills.
        assert_eq!((sx0, sy0, sx1, sy1), (10.0, 10.0, 50.0, 20.0));

        // At a corner the difference is real: the cell square covers the outer
        // wedge the capsule union leaves empty.
        let cornered = [(1.0, 1.0), (4.0, 1.0), (4.0, 4.0)];
        let mut recorder = OpRecorder::new();
        {
            let mut ctx = PaintCtx::recording(&mut recorder);
            ctx.save();
            clip_to_body(&mut ctx, &cornered, cell_size, ClipShape::Cells).unwrap();
            ctx.fill_rect(-500.0, -500.0, 1000.0, 1000.0);
            ctx.restore();
        }
        assert_eq!(
            recorder.painted_extent(),
            Some((10.0, 10.0, 50.0, 50.0)),
            "the cells clip covers every body cell square"
        );
    }

    /// The silhouette's straight part runs centre to centre, and the caps are
    /// the discs.
    ///
    /// This has to be asserted on the path itself rather than on
    /// `painted_extent`, and that is the whole point of the test. The recorder
    /// tracks a clip as its **bounding box** (`skin::paint`), and a capsule and
    /// the cell rectangle around it have the *same* bounding box — so the
    /// measurement every other clip test uses is structurally blind to a
    /// squared-off cap. It was blind to one: the rectangle extended by the
    /// radius along the run axis too, which made `Silhouette` behave as `Cells`
    /// for every straight body, and the first skin with a clipped body layer
    /// painted into the corner the contour rounds away.
    #[test]
    fn the_silhouettes_straight_part_stops_at_the_end_centres() {
        let cell_size = 10.0;
        let rects = |cells: &[(f64, f64)]| {
            let mut recorder = OpRecorder::new();
            {
                let mut ctx = PaintCtx::recording(&mut recorder);
                clip_to_body(&mut ctx, cells, cell_size, ClipShape::Silhouette).unwrap();
            }
            recorder
                .ops()
                .iter()
                .filter_map(|op| match op {
                    crate::skin::paint::PaintOp::Rect(x, y, w, h) => Some((*x, *y, *w, *h)),
                    _ => None,
                })
                .collect::<Vec<_>>()
        };

        // Cells x = 1..4 on row y = 1, at cell 10: centres run x = 15..45, and
        // the ribbon is half a cell either side of y = 15.
        assert_eq!(
            rects(&[(1.0, 1.0), (4.0, 1.0)]),
            vec![(15.0, 10.0, 30.0, 10.0)],
            "a horizontal run's rectangle must stop at the end centres; \
             reaching 10.0..50.0 would square off both caps"
        );

        // The same, turned: centres run y = 15..45, ribbon either side of 15.
        assert_eq!(
            rects(&[(1.0, 1.0), (1.0, 4.0)]),
            vec![(10.0, 15.0, 10.0, 30.0)]
        );

        // A one-cell body never reaches the run walk at all: it is a single
        // disc, and a rectangle here would square that disc off too.
        assert_eq!(rects(&[(1.0, 1.0)]), Vec::new());

        // Consecutive turns make one-cell runs, which is where a rectangle
        // that reached past the end centres would overlap its neighbour's cap
        // and square off the outside of the turn.
        assert_eq!(
            rects(&[(1.0, 1.0), (1.0, 2.0), (2.0, 2.0)]),
            vec![(10.0, 15.0, 10.0, 10.0), (15.0, 20.0, 10.0, 10.0)],
            "each one-cell run spans its own centres and no further"
        );
    }

    /// Neither clip shape may extend past the body's cells, or a body layer
    /// could contribute overhang and the contour would stop being the only
    /// source of it (`specs/skin-shading-prd.md` section 5.2).
    #[test]
    fn no_clip_shape_reaches_outside_the_body_cells() {
        for pose in POSES {
            for shape in [ClipShape::Silhouette, ClipShape::Cells] {
                for &cell_size in &[5.0, 15.0] {
                    let mut recorder = OpRecorder::new();
                    {
                        let mut ctx = PaintCtx::recording(&mut recorder);
                        ctx.save();
                        clip_to_body(&mut ctx, pose.cells, cell_size, shape).unwrap();
                        ctx.fill_rect(-1000.0, -1000.0, 4000.0, 4000.0);
                        ctx.restore();
                    }
                    let (x0, y0, x1, y1) = recorder
                        .painted_extent()
                        .expect("the clip admits something");

                    let body_x0 =
                        pose.cells.iter().map(|c| c.0).fold(f64::MAX, f64::min) * cell_size;
                    let body_y0 =
                        pose.cells.iter().map(|c| c.1).fold(f64::MAX, f64::min) * cell_size;
                    let body_x1 = pose.cells.iter().map(|c| c.0).fold(f64::MIN, f64::max)
                        * cell_size
                        + cell_size;
                    let body_y1 = pose.cells.iter().map(|c| c.1).fold(f64::MIN, f64::max)
                        * cell_size
                        + cell_size;

                    let slack = 1e-9;
                    assert!(
                        x0 >= body_x0 - slack
                            && y0 >= body_y0 - slack
                            && x1 <= body_x1 + slack
                            && y1 <= body_y1 + slack,
                        "{shape:?} on {} at cell {cell_size} admits paint outside \
                         the body cells: ({x0}, {y0}, {x1}, {y1}) vs \
                         ({body_x0}, {body_y0}, {body_x1}, {body_y1})",
                        pose.name
                    );
                }
            }
        }
    }
}
