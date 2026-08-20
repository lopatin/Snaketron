//! The dead-snake painter.
//!
//! Death is deliberately identity-erasing: every corpse is the same gray
//! regardless of the player's skin, because reading a crowded board during a
//! fight matters more than showing off. That is a product ruling, not an
//! oversight — see ruling 5 in `specs/skins-prd.md`. The trait exposes this as
//! a default `paint_dead` so the decision can be revisited without reshaping
//! anything, but no skin can override it today.
//!
//! Its occlusion and border extents are fixed at 1px and never read a skin's
//! reported overhang, so a wide-outlined skin cannot fatten its own corpse.

#[cfg(test)]
use crate::render::transform_coords;
use crate::skin::PaintCtx;
use crate::skin::geometry::{joints, segments};
#[cfg(test)]
use common::Position;
use wasm_bindgen::prelude::*;

/// Corpse fill and border. Fixed, not skin-derived.
const CORPSE_FILL: &str = "#f0f0f0";
const CORPSE_BORDER: &str = "#d0d0d0";
const CORPSE_CROSS: &str = "#666";
/// How far a corpse paints beyond its body cells, per side. Independent of any
/// skin's metrics by design.
pub const CORPSE_OVERHANG_PX: f64 = 1.0;
/// The X on a dead head, as a fraction of one cell.
const CORPSE_CROSS_RATIO: f64 = 0.3;
const CORPSE_CROSS_WIDTH: f64 = 2.0;

const FULL_CIRCLE: f64 = 2.0 * std::f64::consts::PI;

/// Paint one dead snake from a body already resolved to screen cells.
pub fn paint_dead(ctx: &mut PaintCtx, cells: &[(f64, f64)], cell_size: f64) -> Result<(), JsValue> {
    ctx.set_fill(CORPSE_FILL);

    if cells.is_empty() {
        return Ok(());
    }

    // A snake compressed to a single cell is drawn as one disc. It gets no
    // occlusion pass at all — the disc plus its border already covers the grid
    // dot underneath, and adding a mask here would change pixels that have
    // looked this way since the beginning.
    if cells.len() == 1 {
        let (center_x, center_y) = cell_center(cells[0], cell_size);

        ctx.set_fill(CORPSE_BORDER);
        ctx.begin_path();
        ctx.arc(
            center_x,
            center_y,
            cell_size / 2.0 + CORPSE_OVERHANG_PX,
            0.0,
            FULL_CIRCLE,
        )?;
        ctx.fill();

        ctx.set_fill(CORPSE_FILL);
        ctx.begin_path();
        ctx.arc(center_x, center_y, cell_size / 2.0, 0.0, FULL_CIRCLE)?;
        ctx.fill();

        paint_cross(ctx, center_x, center_y, cell_size)?;
        return Ok(());
    }

    // First pass: erase the grid dots under the body.
    ctx.set_fill("#ffffff");
    for segment in segments(cells) {
        let (min, max) = segment.span_edges(cell_size);
        let axis = segment.axis_edge(cell_size);
        if segment.vertical {
            ctx.fill_rect(
                axis - CORPSE_OVERHANG_PX,
                min - CORPSE_OVERHANG_PX,
                cell_size + CORPSE_OVERHANG_PX * 2.0,
                (max - min) + cell_size + CORPSE_OVERHANG_PX * 2.0,
            );
        } else {
            ctx.fill_rect(
                min - CORPSE_OVERHANG_PX,
                axis - CORPSE_OVERHANG_PX,
                (max - min) + cell_size + CORPSE_OVERHANG_PX * 2.0,
                cell_size + CORPSE_OVERHANG_PX * 2.0,
            );
        }
    }
    for cell in cells {
        ctx.fill_rect(
            cell.0 * cell_size - CORPSE_OVERHANG_PX,
            cell.1 * cell_size - CORPSE_OVERHANG_PX,
            cell_size + CORPSE_OVERHANG_PX * 2.0,
            cell_size + CORPSE_OVERHANG_PX * 2.0,
        );
    }

    // Second pass: the border, one pixel wider than the body.
    ctx.set_stroke(CORPSE_BORDER);
    paint_runs(ctx, cells, cell_size, cell_size + CORPSE_OVERHANG_PX * 2.0);
    ctx.set_fill(CORPSE_BORDER);
    paint_joints(ctx, cells, cell_size, cell_size / 2.0 + CORPSE_OVERHANG_PX)?;

    // Third pass: the body itself.
    ctx.set_stroke(CORPSE_FILL);
    ctx.set_fill(CORPSE_FILL);
    paint_runs(ctx, cells, cell_size, cell_size);
    paint_joints(ctx, cells, cell_size, cell_size / 2.0)?;

    let (head_x, head_y) = cell_center(cells[0], cell_size);
    let (tail_x, tail_y) = cell_center(cells[cells.len() - 1], cell_size);

    ctx.set_fill(CORPSE_FILL);
    ctx.begin_path();
    ctx.arc(tail_x, tail_y, cell_size / 2.0, 0.0, FULL_CIRCLE)?;
    ctx.fill();

    ctx.begin_path();
    ctx.arc(head_x, head_y, cell_size / 2.0, 0.0, FULL_CIRCLE)?;
    ctx.fill();

    paint_cross(ctx, head_x, head_y, cell_size)
}

/// Arena entry point: transform the body, then paint it.
///
/// The live arena goes through `SnakeSkin::paint_dead` instead; this stays as
/// the goldens' way of exercising the corpse across every rotation.
#[cfg(test)]
pub fn paint_dead_arena(
    ctx: &mut PaintCtx,
    snake_body: &[Position],
    cell_size: f64,
    game_width: f64,
    game_height: f64,
    rotation_int: i32,
) -> Result<(), JsValue> {
    let cells: Vec<(f64, f64)> = snake_body
        .iter()
        .map(|point| {
            transform_coords(
                point.x as f64,
                point.y as f64,
                game_width,
                game_height,
                rotation_int,
            )
        })
        .collect();
    paint_dead(ctx, &cells, cell_size)
}

fn cell_center(cell: (f64, f64), cell_size: f64) -> (f64, f64) {
    (
        cell.0 * cell_size + cell_size / 2.0,
        cell.1 * cell_size + cell_size / 2.0,
    )
}

fn paint_runs(ctx: &mut PaintCtx, cells: &[(f64, f64)], cell_size: f64, line_width: f64) {
    for segment in segments(cells) {
        let axis = segment.axis_center(cell_size);
        let (min, max) = segment.span_centers(cell_size);
        ctx.set_line_width(line_width);
        ctx.set_line_cap("round");
        ctx.begin_path();
        if segment.vertical {
            ctx.move_to(axis, min);
            ctx.line_to(axis, max);
        } else {
            ctx.move_to(min, axis);
            ctx.line_to(max, axis);
        }
        ctx.stroke();
    }
}

fn paint_joints(
    ctx: &mut PaintCtx,
    cells: &[(f64, f64)],
    cell_size: f64,
    radius: f64,
) -> Result<(), JsValue> {
    for joint in joints(cells) {
        let (center_x, center_y) = cell_center(*joint, cell_size);
        ctx.begin_path();
        ctx.arc(center_x, center_y, radius, 0.0, FULL_CIRCLE)?;
        ctx.fill();
    }
    Ok(())
}

fn paint_cross(
    ctx: &mut PaintCtx,
    center_x: f64,
    center_y: f64,
    cell_size: f64,
) -> Result<(), JsValue> {
    ctx.set_stroke(CORPSE_CROSS);
    ctx.set_line_width(CORPSE_CROSS_WIDTH);
    let reach = cell_size * CORPSE_CROSS_RATIO;
    ctx.begin_path();
    ctx.move_to(center_x - reach, center_y - reach);
    ctx.line_to(center_x + reach, center_y + reach);
    ctx.stroke();
    ctx.begin_path();
    ctx.move_to(center_x - reach, center_y + reach);
    ctx.line_to(center_x + reach, center_y - reach);
    ctx.stroke();
    Ok(())
}
