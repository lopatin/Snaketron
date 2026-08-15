//! Body geometry every skin can build on.
//!
//! A compressed snake body is a polyline of turns, not a list of cells, so
//! every painter has to classify runs, find corners, and sometimes walk the
//! body cell by cell. Doing that once here means a skin author starts from
//! "what should a segment look like" instead of re-deriving the traversal —
//! and it means the traversal has exactly one set of rounding behaviours.

/// One straight run between two consecutive body points.
///
/// Endpoints stay in cell coordinates, in body order. Painters convert to
/// pixels themselves so each can apply its own widths and insets without this
/// module having to know about them.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Segment {
    /// True when the run is vertical on screen (constant x).
    pub vertical: bool,
    pub a: (f64, f64),
    pub b: (f64, f64),
}

impl Segment {
    /// The run's fixed axis in pixels, offset to the centre of the cell.
    pub fn axis_center(self, cell_size: f64) -> f64 {
        if self.vertical {
            self.a.0 * cell_size + cell_size / 2.0
        } else {
            self.a.1 * cell_size + cell_size / 2.0
        }
    }

    /// The run's extent in pixels along its varying axis, centre to centre.
    pub fn span_centers(self, cell_size: f64) -> (f64, f64) {
        let (from, to) = if self.vertical {
            (self.a.1, self.b.1)
        } else {
            (self.a.0, self.b.0)
        };
        (
            from.min(to) * cell_size + cell_size / 2.0,
            from.max(to) * cell_size + cell_size / 2.0,
        )
    }

    /// The run's extent in pixels along its varying axis, cell edge to edge.
    pub fn span_edges(self, cell_size: f64) -> (f64, f64) {
        let (from, to) = if self.vertical {
            (self.a.1, self.b.1)
        } else {
            (self.a.0, self.b.0)
        };
        (from.min(to) * cell_size, from.max(to) * cell_size)
    }

    /// The run's fixed axis in pixels at the cell edge.
    pub fn axis_edge(self, cell_size: f64) -> f64 {
        if self.vertical {
            self.a.0 * cell_size
        } else {
            self.a.1 * cell_size
        }
    }
}

/// Classify each consecutive pair of body points into a straight run.
///
/// Points that are neither aligned horizontally nor vertically yield nothing,
/// matching the renderer's long-standing behaviour of simply skipping a pair it
/// cannot draw as an axis-aligned run.
pub fn segments(cells: &[(f64, f64)]) -> impl Iterator<Item = Segment> + '_ {
    cells.windows(2).filter_map(|window| {
        let (a, b) = (window[0], window[1]);
        if (a.0 - b.0).abs() < 0.01 {
            Some(Segment {
                vertical: true,
                a,
                b,
            })
        } else if (a.1 - b.1).abs() < 0.01 {
            Some(Segment {
                vertical: false,
                a,
                b,
            })
        } else {
            None
        }
    })
}

/// The corner cells — every body point except the head and the tail.
pub fn joints(cells: &[(f64, f64)]) -> &[(f64, f64)] {
    if cells.len() < 3 {
        &[]
    } else {
        &cells[1..cells.len() - 1]
    }
}

/// Pack two small integer cell coordinates into one key.
///
/// This replaces a `format!("{x},{y}")` string allocated per cell per snake per
/// frame. Grid coordinates are `i16` in the engine, so nothing here can
/// collide, and the traversal order is unchanged — which is why the golden
/// traces are the proof that this is a pure win rather than a behaviour change.
#[inline]
fn cell_key(x: i64, y: i64) -> i64 {
    (x << 32) | (y & 0xffff_ffff)
}

/// Walk the body cell by cell from the head, reporting each cell's **arc
/// length** — its distance from the head along the body, in cells.
///
/// Corners belong to the run that reached them, so the first cell of every
/// later run is skipped. That is the only rule here.
///
/// In particular this does *not* deduplicate. Arc length is a property of the
/// body's path, so a cell the body occupies twice has two arc lengths and is
/// reported twice; deduplicating paint is a separate job with a separate
/// answer, done by [`walk_cells_from_head`]. The two were one loop until the
/// shading engine needed clean body-space coordinates
/// (`specs/skin-shading-prd.md` section 16, item 1): entangled, a self-crossing
/// body would have reported the cells after a crossing as one cell closer to
/// the head than they are, and every span placed along the body would have
/// inherited that error.
///
/// `visit` returns `false` to stop the walk early. Nothing is allocated.
pub fn for_each_body_cell(cells: &[(f64, f64)], mut visit: impl FnMut(i64, i64, f64) -> bool) {
    let mut distance = 0.0;

    for (index, window) in cells.windows(2).enumerate() {
        let (x1, y1) = (window[0].0 as i64, window[0].1 as i64);
        let (x2, y2) = (window[1].0 as i64, window[1].1 as i64);

        if x1 == x2 {
            let step = if y2 > y1 { 1 } else { -1 };
            let mut y = y1;
            loop {
                if !(index > 0 && y == y1) {
                    if !visit(x1, y, distance) {
                        return;
                    }
                    distance += 1.0;
                }
                if y == y2 {
                    break;
                }
                y += step;
            }
        } else if y1 == y2 {
            let step = if x2 > x1 { 1 } else { -1 };
            let mut x = x1;
            loop {
                if !(index > 0 && x == x1) {
                    if !visit(x, y1, distance) {
                        return;
                    }
                    distance += 1.0;
                }
                if x == x2 {
                    break;
                }
                x += step;
            }
        }
    }
}

/// Walk the body cell by cell from the head, yielding `(x, y, distance)` for
/// the first `limit` distinct cells.
///
/// This is the *paint* walk: a cell already painted is never painted again,
/// which is why a snake that doubles back gets no brighter patch where it
/// overlaps. The distances come from [`for_each_body_cell`], so a skipped
/// repeat leaves a gap in the sequence rather than shifting everything behind
/// it — which is exactly the difference between the two jobs.
pub fn walk_cells_from_head(cells: &[(f64, f64)], limit: f64) -> Vec<(i64, i64, f64)> {
    let mut collected = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for_each_body_cell(cells, |x, y, distance| {
        // Distance only grows, so the first cell past the limit is the last
        // cell worth walking to.
        if distance >= limit {
            return false;
        }
        if seen.insert(cell_key(x, y)) {
            collected.push((x, y, distance));
        }
        true
    });

    collected
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segments_classify_runs_and_skip_diagonals() {
        let cells = [(0.0, 0.0), (0.0, 4.0), (3.0, 4.0), (5.0, 7.0)];
        let found: Vec<_> = segments(&cells).collect();
        assert_eq!(found.len(), 2, "the diagonal pair yields no run");
        assert!(found[0].vertical);
        assert!(!found[1].vertical);
    }

    #[test]
    fn joints_exclude_head_and_tail() {
        assert_eq!(joints(&[(1.0, 1.0)]), &[]);
        assert_eq!(joints(&[(1.0, 1.0), (2.0, 1.0)]), &[]);
        assert_eq!(joints(&[(1.0, 1.0), (2.0, 1.0), (2.0, 5.0)]), &[(2.0, 1.0)]);
    }

    /// Negative coordinates have to survive packing, because a rotated arena
    /// can hand the painter cells left of the origin.
    #[test]
    fn packed_keys_separate_every_nearby_cell_including_negatives() {
        let mut seen = std::collections::HashSet::new();
        for x in -3i64..3 {
            for y in -3i64..3 {
                assert!(seen.insert(cell_key(x, y)), "collision at ({x}, {y})");
            }
        }
    }

    #[test]
    fn head_walk_counts_each_cell_once_and_respects_the_limit() {
        // An L that revisits nothing: 4 cells across, then 3 down.
        let cells = [(0.0, 0.0), (3.0, 0.0), (3.0, 2.0)];
        let walked = walk_cells_from_head(&cells, 10.0);
        let coords: Vec<_> = walked.iter().map(|(x, y, _)| (*x, *y)).collect();
        assert_eq!(
            coords,
            vec![(0, 0), (1, 0), (2, 0), (3, 0), (3, 1), (3, 2)],
            "the corner is counted once, by the run that reached it"
        );
        let distances: Vec<_> = walked.iter().map(|(_, _, d)| *d).collect();
        assert_eq!(distances, vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0]);

        let clipped = walk_cells_from_head(&cells, 3.0);
        assert_eq!(clipped.len(), 3, "the limit clips by distance, not by run");
    }

    /// A body that crosses itself is where arc length and paint dedup give
    /// different answers, and the reason they are now two functions.
    ///
    /// The engine kills a snake whose head enters its own body, so this cannot
    /// happen in play — but body space has to be right for the case anyway,
    /// because a wrong arc length here silently misplaces every span on every
    /// body that ever gets near it.
    #[test]
    fn arc_length_stays_monotonic_where_paint_dedups() {
        // Across the top, down the right, back left, then up through the cell
        // the first run already covered at (4, 2).
        let cells = [(2.0, 2.0), (6.0, 2.0), (6.0, 5.0), (4.0, 5.0), (4.0, 1.0)];

        let mut visits: Vec<(i64, i64, f64)> = Vec::new();
        for_each_body_cell(&cells, |x, y, distance| {
            visits.push((x, y, distance));
            true
        });

        // The crossing cell is reported twice, at two different arc lengths.
        let crossings: Vec<f64> = visits
            .iter()
            .filter(|(x, y, _)| *x == 4 && *y == 2)
            .map(|(_, _, distance)| *distance)
            .collect();
        assert_eq!(
            crossings.len(),
            2,
            "the body passes through (4, 2) twice; arc length has to say so"
        );
        assert_ne!(crossings[0], crossings[1]);

        // Arc length counts steps and never repeats or goes backwards.
        let distances: Vec<f64> = visits.iter().map(|(_, _, d)| *d).collect();
        assert_eq!(
            distances,
            (0..visits.len()).map(|i| i as f64).collect::<Vec<_>>(),
            "arc length must be the step count, with no gaps and no rewinds"
        );

        // The paint walk sees the crossing cell once, and — this is the part
        // that used to be wrong — the cells *after* it keep the arc lengths
        // they actually have rather than being pulled one cell closer.
        let painted = walk_cells_from_head(&cells, 100.0);
        assert_eq!(
            painted
                .iter()
                .filter(|(x, y, _)| *x == 4 && *y == 2)
                .count(),
            1,
            "a cell is painted once no matter how often the body crosses it"
        );
        let last = painted.last().expect("the walk painted something");
        assert_eq!(
            last.2,
            (visits.len() - 1) as f64,
            "dropping a repeat must not renumber the cells behind it"
        );
    }
}
