use common::{BoostPad, GameState, Position};
use std::collections::HashSet;
use wasm_bindgen::prelude::*;

/// Transform coordinates based on rotation angle
fn transform_coords(x: f64, y: f64, width: f64, height: f64, rotation: i32) -> (f64, f64) {
    match rotation {
        90 => (height - y - 1.0, x),
        180 => (width - x - 1.0, height - y - 1.0),
        270 => (y, width - x - 1.0),
        _ => (x, y), // 0 degrees or default
    }
}

/// Get effective dimensions based on rotation (swap width/height for 90/270)
fn get_effective_dimensions(width: f64, height: f64, rotation: i32) -> (f64, f64) {
    match rotation {
        90 | 270 => (height, width),
        _ => (width, height),
    }
}

/// Return the screen-space bounding box of a square Boost footprint in grid
/// cells. Transforming every occupied cell keeps the 2x2 full-tank packet
/// centered correctly through all four arena rotations.
fn transformed_pad_bounds(
    pad: &BoostPad,
    game_width: f64,
    game_height: f64,
    rotation: i32,
) -> Option<(f64, f64, f64, f64)> {
    let mut cells = pad.footprint_cells().into_iter();
    let first = cells.next()?;
    let (first_x, first_y) = transform_coords(
        first.x as f64,
        first.y as f64,
        game_width,
        game_height,
        rotation,
    );
    let (mut min_x, mut max_x) = (first_x, first_x);
    let (mut min_y, mut max_y) = (first_y, first_y);

    for cell in cells {
        let (x, y) = transform_coords(
            cell.x as f64,
            cell.y as f64,
            game_width,
            game_height,
            rotation,
        );
        min_x = min_x.min(x);
        max_x = max_x.max(x);
        min_y = min_y.min(y);
        max_y = max_y.max(y);
    }

    Some((min_x, min_y, max_x - min_x + 1.0, max_y - min_y + 1.0))
}

fn transformed_active_pad_bounds(
    pads: &[BoostPad],
    game_width: f64,
    game_height: f64,
    rotation: i32,
) -> Vec<(f64, f64, f64, f64)> {
    pads.iter()
        .filter(|pad| pad.respawn_at_tick.is_none())
        .filter_map(|pad| transformed_pad_bounds(pad, game_width, game_height, rotation))
        .collect()
}

const NOS_INK: &str = "#172033";
const NOS_BLUE: &str = "#3b82f6";
const NOS_BLUE_HIGHLIGHT: &str = "#93c5fd";
const NOS_BLUE_SHADE: &str = "#2563eb";
const NOS_LABEL: &str = "#f8fafc";
const NOS_STEEL_DARK: &str = "#475569";
const NOS_STEEL_LIGHT: &str = "#cbd5e1";
const NOS_ORANGE: &str = "#ff641e";

/// The dark core inside a living snake's head, as a fraction of one cell.
const HEAD_CORE_RADIUS_RATIO: f64 = 0.38;

const BOOST_OUTER_COLOR: &str = "#fff200";
const BOOST_OUTER_EXTRA: f64 = 6.0;
const ORDINARY_OUTLINE_EXTRA: f64 = 2.0;
const BOOST_MASK_EXTRA: f64 = 3.0;
const ORDINARY_MASK_EXTRA: f64 = 1.0;

#[derive(Clone, Copy, Debug, PartialEq)]
enum SnakeOutlinePaint {
    BoostOuter,
    Ordinary,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct SnakeOutlineLayer {
    paint: SnakeOutlinePaint,
    extra: f64,
}

impl SnakeOutlineLayer {
    fn color(self, ordinary_color: &str) -> &str {
        match self.paint {
            SnakeOutlinePaint::BoostOuter => BOOST_OUTER_COLOR,
            SnakeOutlinePaint::Ordinary => ordinary_color,
        }
    }

    fn line_width(self, cell_size: f64) -> f64 {
        cell_size + self.extra
    }

    fn radius(self, cell_size: f64) -> f64 {
        cell_size / 2.0 + self.extra / 2.0
    }
}

const ACTIVE_SNAKE_OUTLINE_LAYERS: [SnakeOutlineLayer; 2] = [
    SnakeOutlineLayer {
        paint: SnakeOutlinePaint::BoostOuter,
        extra: BOOST_OUTER_EXTRA,
    },
    SnakeOutlineLayer {
        paint: SnakeOutlinePaint::Ordinary,
        extra: ORDINARY_OUTLINE_EXTRA,
    },
];

const ORDINARY_SNAKE_OUTLINE_LAYERS: [SnakeOutlineLayer; 1] = [SnakeOutlineLayer {
    paint: SnakeOutlinePaint::Ordinary,
    extra: ORDINARY_OUTLINE_EXTRA,
}];

fn snake_outline_layers(boost_active: bool) -> &'static [SnakeOutlineLayer] {
    if boost_active {
        &ACTIVE_SNAKE_OUTLINE_LAYERS
    } else {
        &ORDINARY_SNAKE_OUTLINE_LAYERS
    }
}

fn snake_mask_extra(boost_active: bool) -> f64 {
    if boost_active {
        BOOST_MASK_EXTRA
    } else {
        ORDINARY_MASK_EXTRA
    }
}

const NOS_REGULAR_WIDTH_RATIO: f64 = 0.50;
const NOS_REGULAR_HEIGHT_RATIO: f64 = 0.88;
const NOS_FULL_WIDTH_RATIO: f64 = 0.68;
const NOS_FULL_HEIGHT_RATIO: f64 = 0.88;
const NOS_PRESSURE_PLATE_WHITE_WIDTH_RATIO: f64 = 1.0;
const NOS_PRESSURE_PLATE_WHITE_HEIGHT_RATIO: f64 = 0.60;
const NOS_STANDARD_ORANGE_WIDTH_RATIO: f64 = 0.46;
const NOS_STANDARD_ORANGE_HEIGHT_RATIO: f64 = 0.42;
const NOS_FULL_ORANGE_WIDTH_RATIO: f64 = 0.76;
const NOS_FULL_ORANGE_HEIGHT_RATIO: f64 = 0.44;
const NOS_FULL_WORDMARK_MIN_WIDTH: f64 = 10.0;
const NOS_FULL_WORDMARK_MIN_HEIGHT: f64 = 6.0;
const NOS_FULL_PLATE_CENTER_Y_RATIO: f64 = 0.075;
const NOS_REGULAR_GROWTH_PER_SIDE_PX: f64 = 1.0;

#[derive(Clone, Copy)]
enum NosBottleSkin {
    Standard,
    Full,
}

impl NosBottleSkin {
    fn orange_ratios(self) -> (f64, f64) {
        match self {
            Self::Standard => (
                NOS_STANDARD_ORANGE_WIDTH_RATIO,
                NOS_STANDARD_ORANGE_HEIGHT_RATIO,
            ),
            Self::Full => (NOS_FULL_ORANGE_WIDTH_RATIO, NOS_FULL_ORANGE_HEIGHT_RATIO),
        }
    }

    fn plate_center_y(self, height: f64) -> f64 {
        match self {
            Self::Standard => 0.0,
            Self::Full => height * NOS_FULL_PLATE_CENTER_Y_RATIO,
        }
    }
}

/// Give each pickup a slight amount of optical overscan. At the 5px arena
/// floor this is one extra pixel; at normal sizes it grows to at most two.
/// Curves and fractional coordinates can then antialias into that room without
/// making the icon feel smaller than the gameplay cell it represents.
fn nos_visual_extent(footprint_px: f64) -> f64 {
    footprint_px + (footprint_px * 0.16).clamp(1.0, 2.0)
}

fn regular_nos_dimensions(cell_size: f64) -> (f64, f64, f64) {
    let base_extent = nos_visual_extent(cell_size);
    let base_width = (base_extent * NOS_REGULAR_WIDTH_RATIO).max(3.8);
    let base_height = base_extent * NOS_REGULAR_HEIGHT_RATIO;

    // Regular pickups sit at 45 degrees. Scale the bottle uniformly until its
    // projected bounds gain one screen pixel on every side, preserving the
    // established canister proportions while allowing a small cell overlap.
    let base_projected_size = (base_width + base_height) / std::f64::consts::SQRT_2;
    let projected_growth = NOS_REGULAR_GROWTH_PER_SIDE_PX * 2.0;
    let scale = (base_projected_size + projected_growth) / base_projected_size;

    (
        base_extent + projected_growth,
        base_width * scale,
        base_height * scale,
    )
}

fn full_nos_dimensions(cell_size: f64) -> (f64, f64, f64) {
    let extent = nos_visual_extent(cell_size * 2.0);
    (
        extent,
        (extent * NOS_FULL_WIDTH_RATIO).max(7.0),
        extent * NOS_FULL_HEIGHT_RATIO,
    )
}

fn rect_path(ctx: &web_sys::CanvasRenderingContext2d, x: f64, y: f64, width: f64, height: f64) {
    ctx.begin_path();
    ctx.move_to(x, y);
    ctx.line_to(x + width, y);
    ctx.line_to(x + width, y + height);
    ctx.line_to(x, y + height);
    ctx.close_path();
}

fn nos_bottle_body_path(ctx: &web_sys::CanvasRenderingContext2d, width: f64, height: f64) {
    let half_width = width / 2.0;
    let neck = width * 0.24;
    ctx.begin_path();
    ctx.move_to(-neck, -height * 0.43);
    ctx.line_to(neck, -height * 0.43);
    ctx.line_to(neck, -height * 0.37);
    ctx.line_to(half_width * 0.72, -height * 0.31);
    ctx.line_to(half_width, -height * 0.23);
    ctx.line_to(half_width, height * 0.39);
    ctx.line_to(half_width * 0.76, height * 0.48);
    ctx.line_to(-half_width * 0.76, height * 0.48);
    ctx.line_to(-half_width, height * 0.39);
    ctx.line_to(-half_width, -height * 0.23);
    ctx.line_to(-half_width * 0.72, -height * 0.31);
    ctx.line_to(-neck, -height * 0.37);
    ctx.close_path();
}

fn nos_wordmark_size(width: f64, height: f64, skin: NosBottleSkin) -> Option<f64> {
    if matches!(skin, NosBottleSkin::Standard) {
        return None;
    }

    let (orange_width_ratio, orange_height_ratio) = skin.orange_ratios();
    let plate_width = width * orange_width_ratio;
    let plate_height = height * orange_height_ratio;
    if plate_width < NOS_FULL_WORDMARK_MIN_WIDTH || plate_height < NOS_FULL_WORDMARK_MIN_HEIGHT {
        return None;
    }

    Some((plate_height * 0.72).clamp(5.0, 9.0))
}

fn nos_pressure_plate_dimensions(
    width: f64,
    height: f64,
    skin: NosBottleSkin,
) -> (f64, f64, f64, f64) {
    let (orange_width_ratio, orange_height_ratio) = skin.orange_ratios();
    (
        width * NOS_PRESSURE_PLATE_WHITE_WIDTH_RATIO,
        height * NOS_PRESSURE_PLATE_WHITE_HEIGHT_RATIO,
        width * orange_width_ratio,
        height * orange_height_ratio,
    )
}

fn nos_wordmark_font(size: f64) -> String {
    format!("900 {size}px \"Arial Black\", Arial, sans-serif")
}

/// Paint one faceted Pressure Plate NOS bottle in a local vector coordinate
/// system. Canvas antialiasing keeps the angled silhouette clean, while every
/// authored edge remains straight and mechanical.
fn draw_nos_bottle(
    ctx: &web_sys::CanvasRenderingContext2d,
    center_x: f64,
    center_y: f64,
    width: f64,
    height: f64,
    angle: f64,
    skin: NosBottleSkin,
) -> Result<(), JsValue> {
    ctx.save();
    ctx.translate(center_x, center_y)?;
    ctx.rotate(angle)?;
    ctx.set_line_join("bevel");
    ctx.set_line_cap("butt");

    let outline = (width * 0.095).clamp(0.52, 1.1);

    // The shell uses the exact active Start Game blue. A restrained planar
    // facet supplies depth without restoring the old rounded/cartoon gloss.
    nos_bottle_body_path(ctx, width, height);
    ctx.set_fill_style_str(NOS_BLUE);
    ctx.fill();
    ctx.set_line_width(outline);
    ctx.set_stroke_style_str(NOS_INK);
    ctx.stroke();

    ctx.begin_path();
    ctx.move_to(width * 0.22, -height * 0.34);
    ctx.line_to(width * 0.43, -height * 0.23);
    ctx.line_to(width * 0.43, height * 0.38);
    ctx.line_to(width * 0.31, height * 0.45);
    ctx.line_to(width * 0.16, height * 0.45);
    ctx.line_to(width * 0.16, -height * 0.34);
    ctx.close_path();
    ctx.set_fill_style_str(NOS_BLUE_SHADE);
    ctx.fill();

    ctx.begin_path();
    ctx.move_to(-width * 0.28, -height * 0.22);
    ctx.line_to(-width * 0.28, height * 0.35);
    ctx.set_stroke_style_str(NOS_BLUE_HIGHLIGHT);
    ctx.set_line_width((width * 0.075).clamp(0.34, 0.72));
    ctx.stroke();

    // The white plate spans the entire canister width. The orange face remains
    // an inset, axis-aligned rectangle so white is a separator rather than a
    // competing body color.
    let (white_plate_width, white_plate_height, orange_plate_width, orange_plate_height) =
        nos_pressure_plate_dimensions(width, height, skin);
    let plate_center_y = skin.plate_center_y(height);
    rect_path(
        ctx,
        -white_plate_width / 2.0,
        plate_center_y - white_plate_height / 2.0,
        white_plate_width,
        white_plate_height,
    );
    ctx.set_fill_style_str(NOS_LABEL);
    ctx.fill();

    rect_path(
        ctx,
        -orange_plate_width / 2.0,
        plate_center_y - orange_plate_height / 2.0,
        orange_plate_width,
        orange_plate_height,
    );
    ctx.set_fill_style_str(NOS_ORANGE);
    ctx.fill();

    // The full pickup has enough horizontal label area for the actual NOS
    // wordmark. Micro field icons keep only the orange pressure plate rather
    // than pretending three unreadable marks are letters.
    if let Some(wordmark_size) = nos_wordmark_size(width, height, skin) {
        ctx.save();
        ctx.translate(0.0, plate_center_y)?;
        ctx.set_fill_style_str(NOS_LABEL);
        ctx.set_text_align("center");
        ctx.set_text_baseline("middle");
        ctx.set_font(&nos_wordmark_font(wordmark_size));
        ctx.fill_text_with_max_width("NOS", 0.0, 0.0, orange_plate_width * 0.88)?;
        ctx.restore();
    }

    // A squared steel collar and orange valve complete the pressure-vessel
    // silhouette without introducing soft caps or beverage-bottle curves.
    let cap_width = (width * 0.52).max(1.7);
    let cap_height = (height * 0.105).clamp(0.75, 2.2);
    rect_path(ctx, -cap_width / 2.0, -height * 0.52, cap_width, cap_height);
    ctx.set_fill_style_str(NOS_STEEL_LIGHT);
    ctx.fill();
    ctx.set_line_width((outline * 0.65).max(0.42));
    ctx.set_stroke_style_str(NOS_STEEL_DARK);
    ctx.stroke();

    let valve_width = (width * 0.20).max(0.75);
    rect_path(
        ctx,
        -valve_width / 2.0,
        -height * 0.58,
        valve_width,
        (cap_height * 0.50).max(0.45),
    );
    ctx.set_fill_style_str(NOS_ORANGE);
    ctx.fill();

    // Restore the dark outer edge after the label and highlights meet it.
    nos_bottle_body_path(ctx, width, height);
    ctx.set_line_width(outline);
    ctx.set_stroke_style_str(NOS_INK);
    ctx.stroke();
    ctx.restore();
    Ok(())
}

fn draw_regular_nos_canister(
    ctx: &web_sys::CanvasRenderingContext2d,
    left: f64,
    top: f64,
    cell_size: f64,
) -> Result<(), JsValue> {
    let (_extent, width, height) = regular_nos_dimensions(cell_size);
    draw_nos_bottle(
        ctx,
        left + cell_size / 2.0,
        top + cell_size / 2.0,
        width,
        height,
        std::f64::consts::FRAC_PI_4,
        NosBottleSkin::Standard,
    )
}

fn draw_full_nos_canister(
    ctx: &web_sys::CanvasRenderingContext2d,
    left: f64,
    top: f64,
    cell_size: f64,
) -> Result<(), JsValue> {
    let footprint = cell_size * 2.0;
    let (_extent, bottle_width, bottle_height) = full_nos_dimensions(cell_size);
    let center_x = left + footprint / 2.0;
    let center_y = top + footprint / 2.0;

    // One deliberately broad pressure vessel makes the 2x2 packet read as a
    // single high-value item. Its upright stance and oversized label are the
    // visual hierarchy; no rack or connector suggests multiple collectibles.
    draw_nos_bottle(
        ctx,
        center_x,
        center_y,
        bottle_width,
        bottle_height,
        0.0,
        NosBottleSkin::Full,
    )
}

fn grid_dot_is_covered_by_boost(
    dot_x: f64,
    dot_y: f64,
    active_pad_bounds: &[(f64, f64, f64, f64)],
) -> bool {
    active_pad_bounds.iter().any(|(x, y, width, height)| {
        dot_x >= *x && dot_x <= x + width && dot_y >= *y && dot_y <= y + height
    })
}

fn snake_palette(
    snake_index: usize,
    snake_team: Option<u8>,
    team_member_slot: usize,
    snake_count: usize,
    is_team_game: bool,
    local_snake_id: Option<usize>,
    local_team: Option<u8>,
) -> (&'static str, &'static str) {
    const BLUE: [(&str, &str); 2] = [("#70bfe3", "#5299bb"), ("#3c8dde", "#286eae")];
    const RED: [(&str, &str); 2] = [("#ff6b6b", "#b84444"), ("#e34e5b", "#a92f3a")];

    if is_team_game {
        // Players see teammates as blue and opponents as red, with restrained
        // within-team shades so the roster can map each 2v2 player to the
        // corresponding snake. Spectators retain canonical team 0/1 colors.
        let shade = team_member_slot % 2;
        return match (local_team, snake_team) {
            (Some(ours), Some(theirs)) if ours == theirs => BLUE[shade],
            (Some(_), Some(_)) => RED[shade],
            (None, Some(0)) => BLUE[shade],
            (None, Some(_)) => RED[shade],
            _ if Some(snake_index) == local_snake_id => BLUE[shade],
            _ => RED[shade],
        };
    }

    if Some(snake_index) == local_snake_id {
        BLUE[0]
    } else if snake_count == 2 {
        RED[0]
    } else {
        match snake_index % 4 {
            0 if local_snake_id.is_none() => BLUE[0],
            1 => RED[0],
            2 => ("#556270", "#353c47"),
            _ => ("#f7b731", "#a87d1f"),
        }
    }
}

/// Paint one living snake's complete skin in screen-space cell coordinates.
///
/// `cells` is the compressed body — head first — with every point already run
/// through `transform_coords`, so this routine is independent of arena rotation
/// and of which surface it is drawing onto. The arena render loop and the
/// roster glyph both call it, which is what keeps a player's roster snake
/// identical to the snake they steer.
///
/// `mask_color` paints the opaque pass that erases the arena's grid dots from
/// under the snake. Surfaces with no dot grid behind them (the roster) pass
/// `None` and get the same skin with nothing painted behind it.
///
/// Cell coordinates must be whole, non-negative numbers because the head
/// gradient walks the body one cell at a time; `cell_size` is free to be
/// fractional.
fn draw_alive_snake_skin(
    ctx: &web_sys::CanvasRenderingContext2d,
    cells: &[(f64, f64)],
    cell_size: f64,
    fill: &str,
    outline: &str,
    boost_active: bool,
    mask_color: Option<&str>,
) -> Result<(), JsValue> {
    if cells.is_empty() {
        return Ok(());
    }

    let full_circle = 2.0 * std::f64::consts::PI;
    let outline_layers = snake_outline_layers(boost_active);
    let mask_extra = snake_mask_extra(boost_active);

    // Handle single-segment snake (just a head)
    if cells.len() == 1 {
        let (tx, ty) = cells[0];
        let center_x = tx * cell_size + cell_size / 2.0;
        let center_y = ty * cell_size + cell_size / 2.0;

        // Paint the active Boost band underneath the ordinary contour.
        // The body radius remains unchanged, so this is cosmetic only.
        for layer in outline_layers {
            ctx.set_fill_style_str(layer.color(outline));
            ctx.begin_path();
            ctx.arc(
                center_x,
                center_y,
                layer.radius(cell_size),
                0.0,
                full_circle,
            )?;
            ctx.fill();
        }

        // Draw as a full circle
        ctx.set_fill_style_str(fill);
        ctx.begin_path();
        ctx.arc(center_x, center_y, cell_size / 2.0, 0.0, full_circle)?;
        ctx.fill();

        // Draw inner circle
        ctx.set_fill_style_str("#333");
        ctx.begin_path();
        ctx.arc(center_x, center_y, cell_size * 0.38, 0.0, full_circle)?;
        ctx.fill();
        return Ok(());
    }

    // First pass: fill through the complete visual outline to cover grid dots.
    // Boost adds a crisp two-pixel signal band, while ordinary snakes retain
    // their exact existing 1px mask.
    if let Some(mask) = mask_color {
        ctx.set_fill_style_str(mask);

        for window in cells.windows(2) {
            let ((tx1, ty1), (tx2, ty2)) = (window[0], window[1]);

            if (tx1 - tx2).abs() < 0.01 {
                // Vertical segment after transformation - draw rectangle
                let x = tx1 * cell_size;
                let min_y = ty1.min(ty2) * cell_size;
                let max_y = ty1.max(ty2) * cell_size;
                ctx.fill_rect(
                    x - mask_extra,
                    min_y - mask_extra,
                    cell_size + mask_extra * 2.0,
                    (max_y - min_y) + cell_size + mask_extra * 2.0,
                );
            } else if (ty1 - ty2).abs() < 0.01 {
                // Horizontal segment after transformation - draw rectangle
                let y = ty1 * cell_size;
                let min_x = tx1.min(tx2) * cell_size;
                let max_x = tx1.max(tx2) * cell_size;
                ctx.fill_rect(
                    min_x - mask_extra,
                    y - mask_extra,
                    (max_x - min_x) + cell_size + mask_extra * 2.0,
                    cell_size + mask_extra * 2.0,
                );
            }
        }

        // Fill rectangles for all body points through the same mask.
        for (tx, ty) in cells {
            ctx.fill_rect(
                tx * cell_size - mask_extra,
                ty * cell_size - mask_extra,
                cell_size + mask_extra * 2.0,
                cell_size + mask_extra * 2.0,
            );
        }
    }

    // Second pass: paint complete outline layers from outside in. An active
    // snake gets yellow first and its ordinary contour second; an inactive
    // snake keeps the original single contour pass.
    for layer in outline_layers {
        let layer_color = layer.color(outline);
        ctx.set_stroke_style_str(layer_color);

        // Draw this outline layer for every body segment before moving inward,
        // preventing outer yellow from crossing dark joints.
        for window in cells.windows(2) {
            let ((tx1, ty1), (tx2, ty2)) = (window[0], window[1]);

            if (tx1 - tx2).abs() < 0.01 {
                // Vertical segment after transformation
                let x = tx1 * cell_size + cell_size / 2.0;
                let min_y = ty1.min(ty2) * cell_size + cell_size / 2.0;
                let max_y = ty1.max(ty2) * cell_size + cell_size / 2.0;

                ctx.set_line_width(layer.line_width(cell_size));
                ctx.set_line_cap("round");
                ctx.begin_path();
                ctx.move_to(x, min_y);
                ctx.line_to(x, max_y);
                ctx.stroke();
            } else if (ty1 - ty2).abs() < 0.01 {
                // Horizontal segment after transformation
                let y = ty1 * cell_size + cell_size / 2.0;
                let min_x = tx1.min(tx2) * cell_size + cell_size / 2.0;
                let max_x = tx1.max(tx2) * cell_size + cell_size / 2.0;

                ctx.set_line_width(layer.line_width(cell_size));
                ctx.set_line_cap("round");
                ctx.begin_path();
                ctx.move_to(min_x, y);
                ctx.line_to(max_x, y);
                ctx.stroke();
            }
        }

        // Fill every corner joint for this same layer.
        ctx.set_fill_style_str(layer_color);
        for (tx, ty) in &cells[1..cells.len() - 1] {
            let center_x = tx * cell_size + cell_size / 2.0;
            let center_y = ty * cell_size + cell_size / 2.0;

            ctx.begin_path();
            ctx.arc(
                center_x,
                center_y,
                layer.radius(cell_size),
                0.0,
                full_circle,
            )?;
            ctx.fill();
        }
    }

    // Third pass: Draw the actual snake
    ctx.set_stroke_style_str(fill);
    ctx.set_fill_style_str(fill);

    // Draw main body segments
    for window in cells.windows(2) {
        let ((tx1, ty1), (tx2, ty2)) = (window[0], window[1]);

        if (tx1 - tx2).abs() < 0.01 {
            // Vertical segment after transformation
            let x = tx1 * cell_size + cell_size / 2.0;
            let min_y = ty1.min(ty2) * cell_size + cell_size / 2.0;
            let max_y = ty1.max(ty2) * cell_size + cell_size / 2.0;

            ctx.set_line_width(cell_size);
            ctx.set_line_cap("round");
            ctx.begin_path();
            ctx.move_to(x, min_y);
            ctx.line_to(x, max_y);
            ctx.stroke();
        } else if (ty1 - ty2).abs() < 0.01 {
            // Horizontal segment after transformation
            let y = ty1 * cell_size + cell_size / 2.0;
            let min_x = tx1.min(tx2) * cell_size + cell_size / 2.0;
            let max_x = tx1.max(tx2) * cell_size + cell_size / 2.0;

            ctx.set_line_width(cell_size);
            ctx.set_line_cap("round");
            ctx.begin_path();
            ctx.move_to(min_x, y);
            ctx.line_to(max_x, y);
            ctx.stroke();
        }
    }

    // Draw corner joints as circles to create smooth turns
    for (tx, ty) in &cells[1..cells.len() - 1] {
        let center_x = tx * cell_size + cell_size / 2.0;
        let center_y = ty * cell_size + cell_size / 2.0;

        ctx.begin_path();
        ctx.arc(center_x, center_y, cell_size / 2.0, 0.0, full_circle)?;
        ctx.fill();
    }

    // Get head and tail information
    let (head_tx, head_ty) = cells[0];
    let head_center_x = head_tx * cell_size + cell_size / 2.0;
    let head_center_y = head_ty * cell_size + cell_size / 2.0;

    let (tail_tx, tail_ty) = cells[cells.len() - 1];
    let tail_center_x = tail_tx * cell_size + cell_size / 2.0;
    let tail_center_y = tail_ty * cell_size + cell_size / 2.0;

    // Draw actual tail and head (no separate border circles needed)
    // The round line caps already provide the border
    ctx.set_fill_style_str(fill);

    // Draw tail as full circle
    ctx.begin_path();
    ctx.arc(
        tail_center_x,
        tail_center_y,
        cell_size / 2.0,
        0.0,
        full_circle,
    )?;
    ctx.fill();

    // Fourth pass: Add white overlay gradient for first 10 cells from head
    // Draw white overlay on segments within 10 cells of head
    // First, collect all cells with their distances
    let mut cells_with_distance = Vec::new();
    let mut current_distance = 0.0;
    let mut seen_cells = HashSet::new();

    for (seg_idx, window) in cells.windows(2).enumerate() {
        let (x1, y1) = (window[0].0 as i64, window[0].1 as i64);
        let (x2, y2) = (window[1].0 as i64, window[1].1 as i64);

        // Process each cell in the segment, respecting direction
        if x1 == x2 {
            // Vertical segment
            let x = x1;
            let step = if y2 > y1 { 1 } else { -1 };
            let mut y = y1;

            loop {
                let cell_key = format!("{},{}", x, y);

                // Skip the first cell of non-first segments (it's a corner already processed)
                if !(seg_idx > 0 && y == y1) && !seen_cells.contains(&cell_key) {
                    seen_cells.insert(cell_key.clone());
                    if current_distance < 10.0 {
                        cells_with_distance.push((x, y, current_distance));
                    }
                    current_distance += 1.0;
                }

                if y == y2 {
                    break;
                }
                y += step;
            }
        } else if y1 == y2 {
            // Horizontal segment
            let y = y1;
            let step = if x2 > x1 { 1 } else { -1 };
            let mut x = x1;

            loop {
                let cell_key = format!("{},{}", x, y);

                // Skip the first cell of non-first segments (it's a corner already processed)
                if !(seg_idx > 0 && x == x1) && !seen_cells.contains(&cell_key) {
                    seen_cells.insert(cell_key.clone());
                    if current_distance < 10.0 {
                        cells_with_distance.push((x, y, current_distance));
                    }
                    current_distance += 1.0;
                }

                if x == x2 {
                    break;
                }
                x += step;
            }
        }
    }

    // Now draw all collected cells with their proper distances
    for (x, y, distance) in cells_with_distance {
        let opacity = (1.0 - distance / 10.0) * 0.3;
        ctx.set_fill_style_str(&format!("rgba(255, 255, 255, {})", opacity));
        ctx.fill_rect(
            x as f64 * cell_size,
            y as f64 * cell_size,
            cell_size,
            cell_size,
        );
    }

    // Draw head as full circle (after overlay for proper layering)
    ctx.set_fill_style_str(fill);
    ctx.begin_path();
    ctx.arc(
        head_center_x,
        head_center_y,
        cell_size / 2.0,
        0.0,
        full_circle,
    )?;
    ctx.fill();

    // Draw white overlay on head (strongest opacity)
    ctx.set_fill_style_str("rgba(255, 255, 255, 0.3)");
    ctx.begin_path();
    ctx.arc(
        head_center_x,
        head_center_y,
        cell_size / 2.0,
        0.0,
        full_circle,
    )?;
    ctx.fill();

    // Draw smaller inner circle in head with different color
    ctx.set_fill_style_str("#333");
    ctx.begin_path();
    ctx.arc(
        head_center_x,
        head_center_y,
        cell_size * HEAD_CORE_RADIUS_RATIO,
        0.0,
        full_circle,
    )?;
    ctx.fill();

    Ok(())
}

// ---------------------------------------------------------------------------
// Roster glyph
//
// The match roster draws one snake per player. It is painted here, next to the
// arena renderer and through the same `draw_alive_snake_skin` and
// `snake_palette` calls, so a roster snake is the player's actual skin rather
// than a hand-tuned lookalike that has to be kept in sync by hand.
// ---------------------------------------------------------------------------

const ROSTER_LABEL_DARK_INK: &str = "#0f172a";
const ROSTER_LABEL_LIGHT_INK: &str = "#ffffff";
/// Clearance between the dark head core and the name, as a fraction of a cell.
const ROSTER_LABEL_HEAD_GAP_RATIO: f64 = 0.20;
/// How far past the tail's centre the name may run. The tail cap is a solid
/// half-cell, so this stays inside the body.
const ROSTER_LABEL_TAIL_OVERHANG_RATIO: f64 = 0.30;
const ROSTER_LABEL_SIZE_RATIO: f64 = 0.5;
const ROSTER_LABEL_MIN_SIZE: f64 = 6.0;
const ROSTER_LABEL_MAX_SIZE: f64 = 11.0;
/// Optical centring for mixed-case text on a `middle` baseline.
const ROSTER_LABEL_BASELINE_NUDGE: f64 = 0.25;
const ROSTER_LABEL_ELLIPSIS: &str = "…";
const ROSTER_DEFAULT_FONT_FAMILY: &str =
    "-apple-system, BlinkMacSystemFont, \"Segoe UI\", sans-serif";

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
enum RosterFacing {
    Left,
    Right,
}

impl RosterFacing {
    /// The sign that points from the head back down the body.
    fn toward_tail(self) -> f64 {
        match self {
            Self::Right => -1.0,
            Self::Left => 1.0,
        }
    }

    fn text_align(self) -> &'static str {
        match self {
            Self::Right => "right",
            Self::Left => "left",
        }
    }
}

/// Everything `snake_palette` needs to resolve one snake's colours. The web
/// client assembles this from the game state and passes it back, so hex values
/// never have to be mirrored into TypeScript.
#[derive(Clone, Copy, Debug, serde::Deserialize)]
struct SnakeSkinInputs {
    snake_index: usize,
    #[serde(default)]
    team_id: Option<u8>,
    #[serde(default)]
    team_member_slot: usize,
    snake_count: usize,
    is_team_game: bool,
    #[serde(default)]
    local_snake_id: Option<usize>,
    #[serde(default)]
    local_team_id: Option<u8>,
}

impl SnakeSkinInputs {
    fn colors(&self) -> (&'static str, &'static str) {
        snake_palette(
            self.snake_index,
            self.team_id,
            self.team_member_slot,
            self.snake_count,
            self.is_team_game,
            self.local_snake_id,
            self.local_team_id,
        )
    }

    /// `{ fill, outline, label }` — the reported form of this snake's skin, so
    /// callers can mirror onto the DOM exactly what was painted.
    fn colors_json(&self) -> Result<String, JsValue> {
        let (fill, outline) = self.colors();
        serde_json::to_string(&serde_json::json!({
            "fill": fill,
            "outline": outline,
            "label": roster_label_ink(fill),
        }))
        .map_err(|e| JsValue::from_str(&e.to_string()))
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
struct RosterSnakeRequest {
    #[serde(flatten)]
    skin: SnakeSkinInputs,
    facing: RosterFacing,
    #[serde(default)]
    name: String,
    #[serde(default)]
    font_family: Option<String>,
    #[serde(default)]
    boost_active: bool,
    #[serde(default)]
    is_ready: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct RosterSnakeLayout {
    cell_size: f64,
    cells: usize,
    offset_x: f64,
    offset_y: f64,
}

impl RosterSnakeLayout {
    /// The compressed body the skin painter expects: head first, tail last,
    /// laid out along a single row exactly as a straight arena snake would be.
    fn body_cells(self, facing: RosterFacing) -> [(f64, f64); 2] {
        let last = (self.cells - 1) as f64;
        match facing {
            RosterFacing::Right => [(last, 0.0), (0.0, 0.0)],
            RosterFacing::Left => [(0.0, 0.0), (last, 0.0)],
        }
    }

    fn head_center(self, facing: RosterFacing) -> (f64, f64) {
        let (head_x, head_y) = self.body_cells(facing)[0];
        (
            head_x * self.cell_size + self.cell_size / 2.0,
            head_y * self.cell_size + self.cell_size / 2.0,
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct RosterLabelLayout {
    x: f64,
    y: f64,
    max_width: f64,
    align: &'static str,
    font_size: f64,
}

fn finite_positive(value: f64) -> Option<f64> {
    (value.is_finite() && value > 0.0).then_some(value)
}

/// Size the glyph so the snake is as thick as the roster row allows and as long
/// as fits, then centre the leftover slack. Thickness sets the cell size, so
/// the cell count is chosen to land the body span as close to the full width as
/// possible without ever exceeding the row height.
fn roster_snake_layout(width: f64, height: f64, boost_active: bool) -> RosterSnakeLayout {
    let outline_extra = if boost_active {
        BOOST_OUTER_EXTRA
    } else {
        ORDINARY_OUTLINE_EXTRA
    };
    let max_cell = (height - outline_extra).max(1.0);
    let available = (width - outline_extra).max(max_cell);
    let cells = ((available / max_cell).round().max(2.0) as usize).max(2);
    let cell_size = max_cell.min(available / cells as f64);
    let span = cell_size * cells as f64;

    RosterSnakeLayout {
        cell_size,
        cells,
        offset_x: (width - span) / 2.0,
        offset_y: (height - cell_size) / 2.0,
    }
}

/// Anchor the name to the head. It begins (or ends) just clear of the dark head
/// core and runs back toward the tail, so every roster name lines up with the
/// head the player steers instead of floating in the middle of the body.
fn roster_label_layout(layout: RosterSnakeLayout, facing: RosterFacing) -> RosterLabelLayout {
    let cell = layout.cell_size;
    let last = (layout.cells - 1) as f64;
    let (head_cell, tail_cell) = match facing {
        RosterFacing::Right => (last, 0.0),
        RosterFacing::Left => (0.0, last),
    };
    let head_center = head_cell * cell + cell / 2.0;
    let tail_center = tail_cell * cell + cell / 2.0;
    let toward_tail = facing.toward_tail();

    let head_edge =
        head_center + toward_tail * cell * (HEAD_CORE_RADIUS_RATIO + ROSTER_LABEL_HEAD_GAP_RATIO);
    let tail_edge = tail_center - toward_tail * cell * ROSTER_LABEL_TAIL_OVERHANG_RATIO;

    RosterLabelLayout {
        x: head_edge,
        y: cell / 2.0 + ROSTER_LABEL_BASELINE_NUDGE,
        max_width: (head_edge - tail_edge).abs(),
        align: facing.text_align(),
        font_size: (cell * ROSTER_LABEL_SIZE_RATIO)
            .clamp(ROSTER_LABEL_MIN_SIZE, ROSTER_LABEL_MAX_SIZE),
    }
}

fn relative_luminance(hex: &str) -> f64 {
    let normalized = hex.strip_prefix('#').unwrap_or(hex);
    if normalized.len() != 6 || !normalized.chars().all(|c| c.is_ascii_hexdigit()) {
        return 1.0;
    }

    let channel = |offset: usize| -> f64 {
        let value = u8::from_str_radix(&normalized[offset..offset + 2], 16).unwrap_or(0);
        let value = f64::from(value) / 255.0;
        if value <= 0.04045 {
            value / 12.92
        } else {
            ((value + 0.055) / 1.055).powf(2.4)
        }
    };

    channel(0) * 0.2126 + channel(2) * 0.7152 + channel(4) * 0.0722
}

/// The deeper slate clears WCAG AA even on the darkest authored red skin at the
/// roster's small label size; white cannot clear 4.5:1 on these mid-tone team
/// colours.
fn roster_label_ink(fill: &str) -> &'static str {
    let fill_luminance = relative_luminance(fill);
    let dark_contrast =
        (fill_luminance + 0.05) / (relative_luminance(ROSTER_LABEL_DARK_INK) + 0.05);
    let light_contrast = 1.05 / (fill_luminance + 0.05);
    if dark_contrast >= light_contrast {
        ROSTER_LABEL_DARK_INK
    } else {
        ROSTER_LABEL_LIGHT_INK
    }
}

fn roster_label_shadow(ink: &str) -> &'static str {
    if ink == ROSTER_LABEL_LIGHT_INK {
        "rgb(23 32 51 / 34%)"
    } else {
        "rgb(255 255 255 / 38%)"
    }
}

fn roster_name_candidate(characters: &[char], take: usize) -> String {
    let candidate: String = characters[..take].iter().collect();
    format!("{}{ROSTER_LABEL_ELLIPSIS}", candidate.trim_end())
}

/// Trim a player name to the space between head and tail, never splitting a
/// character. Returns an empty string when not even the ellipsis fits.
fn fit_roster_name(
    ctx: &web_sys::CanvasRenderingContext2d,
    name: &str,
    max_width: f64,
) -> Result<String, JsValue> {
    if max_width <= 0.0 || name.is_empty() {
        return Ok(String::new());
    }
    if ctx.measure_text(name)?.width() <= max_width {
        return Ok(name.to_string());
    }
    if ctx.measure_text(ROSTER_LABEL_ELLIPSIS)?.width() > max_width {
        return Ok(String::new());
    }

    let characters: Vec<char> = name.chars().collect();
    let mut low = 0usize;
    let mut high = characters.len();
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if ctx
            .measure_text(&roster_name_candidate(&characters, middle))?
            .width()
            <= max_width
        {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    Ok(roster_name_candidate(&characters, low))
}

fn draw_roster_label(
    ctx: &web_sys::CanvasRenderingContext2d,
    layout: RosterSnakeLayout,
    facing: RosterFacing,
    name: &str,
    fill: &str,
    font_family: Option<&str>,
) -> Result<(), JsValue> {
    let label = roster_label_layout(layout, facing);
    let family = font_family
        .map(str::trim)
        .filter(|family| !family.is_empty())
        .unwrap_or(ROSTER_DEFAULT_FONT_FAMILY);

    ctx.save();
    ctx.set_font(&format!("900 {}px {family}", label.font_size));
    ctx.set_text_align(label.align);
    ctx.set_text_baseline("middle");

    let visible = fit_roster_name(ctx, name, label.max_width)?;
    if !visible.is_empty() {
        let ink = roster_label_ink(fill);
        ctx.set_fill_style_str(ink);
        ctx.set_shadow_color(roster_label_shadow(ink));
        ctx.set_shadow_blur(0.0);
        ctx.set_shadow_offset_x(0.0);
        ctx.set_shadow_offset_y(1.0);
        ctx.fill_text(&visible, label.x, label.y)?;
    }
    ctx.restore();
    Ok(())
}

/// Put readiness on the same visual anchor as identity: the dark core of the
/// snake's actual head. A stroked path stays crisp at both roster breakpoints
/// and mirrors automatically with the renderer's facing-aware head geometry.
fn draw_roster_ready_check(
    ctx: &web_sys::CanvasRenderingContext2d,
    layout: RosterSnakeLayout,
    facing: RosterFacing,
) -> Result<(), JsValue> {
    let (center_x, center_y) = layout.head_center(facing);
    let cell = layout.cell_size;

    ctx.save();
    ctx.set_stroke_style_str("#ffffff");
    ctx.set_line_width((cell * 0.14).clamp(1.5, 2.4));
    ctx.set_line_cap("round");
    ctx.set_line_join("round");
    ctx.begin_path();
    ctx.move_to(center_x - cell * 0.22, center_y);
    ctx.line_to(center_x - cell * 0.06, center_y + cell * 0.17);
    ctx.line_to(center_x + cell * 0.24, center_y - cell * 0.19);
    ctx.stroke();
    ctx.restore();
    Ok(())
}

/// Paint one roster snake onto its own canvas.
///
/// `request_json` carries the palette inputs (see `SnakeSkinInputs`) plus the
/// glyph's `facing`, the player `name` to set inside the body, and an optional
/// `font_family`. `css_width`/`css_height` are the canvas' laid-out CSS size;
/// the backing store is resized here for `device_pixel_ratio`.
///
/// Returns the `{ fill, outline, label }` this snake was painted with, so the
/// DOM can advertise the same skin the pixels came from rather than a
/// separately derived guess.
#[wasm_bindgen(js_name = renderRosterSnake)]
pub fn render_roster_snake(
    canvas: &web_sys::HtmlCanvasElement,
    css_width: f64,
    css_height: f64,
    device_pixel_ratio: f64,
    request_json: &str,
) -> Result<String, JsValue> {
    let request: RosterSnakeRequest =
        serde_json::from_str(request_json).map_err(|e| JsValue::from_str(&e.to_string()))?;
    let colors = request.skin.colors_json()?;

    let (Some(width), Some(height)) = (finite_positive(css_width), finite_positive(css_height))
    else {
        return Ok(colors);
    };
    let ratio = finite_positive(device_pixel_ratio).unwrap_or(1.0);
    let pixel_width = (width * ratio).round().max(1.0);
    let pixel_height = (height * ratio).round().max(1.0);

    if canvas.width() != pixel_width as u32 {
        canvas.set_width(pixel_width as u32);
    }
    if canvas.height() != pixel_height as u32 {
        canvas.set_height(pixel_height as u32);
    }

    let ctx = canvas
        .get_context("2d")
        .map_err(|_| JsValue::from_str("Failed to get 2d context"))?
        .ok_or_else(|| JsValue::from_str("2d context is null"))?
        .dyn_into::<web_sys::CanvasRenderingContext2d>()
        .map_err(|_| JsValue::from_str("Failed to cast to 2d context"))?;

    ctx.set_transform(1.0, 0.0, 0.0, 1.0, 0.0, 0.0)?;
    ctx.clear_rect(0.0, 0.0, pixel_width, pixel_height);
    ctx.set_transform(
        pixel_width / width,
        0.0,
        0.0,
        pixel_height / height,
        0.0,
        0.0,
    )?;

    let (fill, outline) = request.skin.colors();
    let layout = roster_snake_layout(width, height, request.boost_active);

    ctx.save();
    ctx.translate(layout.offset_x, layout.offset_y)?;
    draw_alive_snake_skin(
        &ctx,
        &layout.body_cells(request.facing),
        layout.cell_size,
        fill,
        outline,
        request.boost_active,
        None,
    )?;
    if request.is_ready {
        draw_roster_ready_check(&ctx, layout, request.facing)?;
    }
    draw_roster_label(
        &ctx,
        layout,
        request.facing,
        &request.name,
        fill,
        request.font_family.as_deref(),
    )?;
    ctx.restore();
    Ok(colors)
}

/// Resolve one snake's authoritative colours without drawing anything, for the
/// small non-canvas swatches (results table, legends) that also have to match
/// the arena exactly. Returns `{ fill, outline, label }`.
#[wasm_bindgen(js_name = snakeSkinColors)]
pub fn snake_skin_colors(request_json: &str) -> Result<String, JsValue> {
    let skin: SnakeSkinInputs =
        serde_json::from_str(request_json).map_err(|e| JsValue::from_str(&e.to_string()))?;
    skin.colors_json()
}

/// Map a screen-relative input direction to the game-coordinate direction for a
/// given arena rotation. This is the inverse of `transform_coords`, kept beside
/// it so input and rendering share one rotation convention and cannot drift.
/// Directions are the `Direction` serde strings ("Up"/"Down"/"Left"/"Right");
/// an unrecognized direction is returned unchanged.
#[wasm_bindgen(js_name = screenDirectionToGame)]
pub fn screen_direction_to_game(direction: &str, rotation: f64) -> String {
    let mapped = match (rotation as i32, direction) {
        (90, "Up") => "Left",
        (90, "Right") => "Up",
        (90, "Down") => "Right",
        (90, "Left") => "Down",
        (180, "Up") => "Down",
        (180, "Down") => "Up",
        (180, "Left") => "Right",
        (180, "Right") => "Left",
        (270, "Up") => "Right",
        (270, "Right") => "Down",
        (270, "Down") => "Left",
        (270, "Left") => "Up",
        // 0 degrees (or any unrecognized rotation/direction): identity
        (_, other) => other,
    };
    mapped.to_string()
}

// ---------------------------------------------------------------------------
// Carried-food readout
//
// Each living snake wears the food it would bank if it reached its own base
// right now, drawn a couple of cells behind the head and inside the body. It
// shares `roster_label_ink` with the roster glyph, so the number on a snake and
// that player's roster name are inked by the same contrast rule.
// ---------------------------------------------------------------------------

/// How far behind the head the number is anchored. The head wears a `#333`
/// core of radius `cell_size * HEAD_CORE_RADIUS_RATIO`, so a number centered on
/// the head — or on cell 1, once it reaches two digits — would sit on top of
/// it. Two cells back clears the core while still reading as "at the head".
const CARRIED_LABEL_OFFSET_CELLS: usize = 2;
/// The arena canvas is not devicePixelRatio-scaled, so these are physical
/// pixels. `GameArena.tsx` walks `cell_size` down through the integers 15..=5,
/// and the floor keeps the number readable at the small end rather than
/// letting it shrink with the cell into illegibility.
const CARRIED_LABEL_MIN_PX: f64 = 7.0;
const CARRIED_LABEL_MAX_PX: f64 = 14.0;
/// Canvas strokes straddle the glyph outline, so the halo eats half its width
/// inward. Kept well under the team-zone convention's `size * 0.35`, which
/// would swallow a 900-weight stem at the 7px floor and leave the number
/// reading as its own halo.
const CARRIED_LABEL_HALO_RATIO: f64 = 0.20;
/// Room a number gets when it runs *along* the body: many cells of snake, so
/// nothing has to give. Across the body it gets the painted band instead —
/// `cell_size` of fill plus the ordinary contour — which is the width the
/// snake actually occupies on screen.
const CARRIED_LABEL_ALONG_BODY_CELLS: f64 = 2.0;
const CARRIED_LABEL_ACROSS_BODY_BLEED_PX: f64 = ORDINARY_OUTLINE_EXTRA;
/// Advance width of one Arial Black digit, in em. Used to pick a font size the
/// number fits at, rather than squashing glyphs horizontally into the band:
/// team arenas render rotated, so most snakes read screen-vertical and the
/// across-body case is the common one, not the fallback. A uniformly smaller
/// number stays a number; a 50%-condensed one becomes vertical bars.
const CARRIED_LABEL_DIGIT_ADVANCE_EM: f64 = 0.667;

/// A cell of snake body, and the axis of the straight run it belongs to.
#[derive(Clone, Copy, Debug, PartialEq)]
struct BodyAnchor {
    cell: Position,
    /// Axis in *grid* space, before any arena rotation is applied.
    run_is_horizontal: bool,
}

/// The grid cell `cells_back` steps behind the head, walking the *compressed*
/// body (head, turns, tail — see `common/src/snake.rs`). Consecutive body
/// points are always axis-aligned, so a segment spans `|dx| + |dy|` cells and
/// shares its first cell with the previous segment, matching `Snake::length`.
/// Clamps to the tail for a snake shorter than `cells_back`; returns `None`
/// only for an empty body.
///
/// The axis of the run the cell landed on is reported alongside it. Taking it
/// from the segment rather than from the head-to-cell chord matters: for one
/// step after every turn that chord is diagonal, and guessing from it picks
/// the wrong orientation on half of all turns.
fn body_cell_behind_head(body: &[Position], cells_back: usize) -> Option<BodyAnchor> {
    let head = *body.first()?;
    let mut remaining = cells_back;

    for window in body.windows(2) {
        let (p1, p2) = (window[0], window[1]);
        let (dx, dy) = (p2.x - p1.x, p2.y - p1.y);
        let span = (dx.abs() + dy.abs()) as usize;
        if span == 0 {
            continue;
        }
        if remaining <= span {
            let step = remaining as i16;
            return Some(BodyAnchor {
                cell: Position {
                    x: p1.x + dx.signum() * step,
                    y: p1.y + dy.signum() * step,
                },
                run_is_horizontal: dx != 0,
            });
        }
        remaining -= span;
    }

    // Shorter than `cells_back` (or a body with no extent at all): clamp to
    // the tail, keeping the last real run's axis where there was one.
    let last_run_is_horizontal = body
        .windows(2)
        .rev()
        .find(|w| w[0] != w[1])
        .map(|w| w[1].x != w[0].x)
        .unwrap_or(true);
    Some(BodyAnchor {
        cell: body.last().copied().unwrap_or(head),
        run_is_horizontal: last_run_is_horizontal,
    })
}

/// Whether a grid-space run reads horizontally *on screen*. The 90 and 270
/// rotations swap the axes; 0 and 180 preserve them. Team matches default to
/// 270/90, so a snake driving down the field is screen-vertical — getting this
/// backwards would pick the wrong width allowance in the primary game mode.
fn anchor_run_is_horizontal_on_screen(run_is_horizontal: bool, rotation: i32) -> bool {
    match rotation {
        90 | 270 => !run_is_horizontal,
        _ => run_is_horizontal,
    }
}

/// The opposite pole from the chosen ink. The roster paints its names on a
/// clean panel and can use a soft translucent shadow; the arena has grid dots
/// and team-zone tints behind the snake, so the readout takes an opaque halo.
fn carried_label_halo(ink: &str) -> &'static str {
    if ink == ROSTER_LABEL_LIGHT_INK {
        ROSTER_LABEL_DARK_INK
    } else {
        ROSTER_LABEL_LIGHT_INK
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct CarriedFoodLabelLayout {
    font_px: f64,
    halo_px: f64,
}

/// Text metrics for the carried-food readout. The glyph tracks the cell size
/// between the legibility floor and the ceiling that stops the number from
/// dominating the snake at full zoom, then shrinks *uniformly* until its
/// natural width fits the room the body gives it. Nothing is condensed, so a
/// number never degrades into bars; at the floor a wide value simply overhangs
/// the band by a pixel or so, where the halo keeps it readable.
fn carried_food_label_layout(
    cell_size: f64,
    digits: usize,
    horizontal_on_screen: bool,
) -> CarriedFoodLabelLayout {
    let available = if horizontal_on_screen {
        cell_size * CARRIED_LABEL_ALONG_BODY_CELLS
    } else {
        cell_size + CARRIED_LABEL_ACROSS_BODY_BLEED_PX
    };
    let per_digit = CARRIED_LABEL_DIGIT_ADVANCE_EM * digits.max(1) as f64;
    let font_px = cell_size
        .min(available / per_digit)
        .clamp(CARRIED_LABEL_MIN_PX, CARRIED_LABEL_MAX_PX);

    CarriedFoodLabelLayout {
        font_px,
        halo_px: font_px * CARRIED_LABEL_HALO_RATIO,
    }
}

/// Deliberately not shared with `nos_wordmark_font`: the pickup wordmark and
/// the gameplay readout should be free to diverge.
fn carried_food_label_font(size: f64) -> String {
    format!("900 {size}px \"Arial Black\", Arial, sans-serif")
}

/// One queued readout: where it goes, what it says, and how it is inked.
struct CarriedFoodLabel {
    center_x: f64,
    center_y: f64,
    text: String,
    horizontal_on_screen: bool,
    ink: &'static str,
}

// ---------------------------------------------------------------------------
// Player-relative food value
//
// Food has one authoritative position but a different prospective value for
// each player. The renderer therefore resolves the local predicted snake once
// and stamps that value onto every food only while its combo is live. A
// spectator (or a player with an expired chain) sees the ordinary unlabelled
// food instead.
// ---------------------------------------------------------------------------

const FOOD_VALUE_LABEL_MIN_PX: f64 = 5.0;
const FOOD_VALUE_LABEL_MAX_PX: f64 = 10.0;
const FOOD_VALUE_LABEL_SIZE_RATIO: f64 = 0.76;
const FOOD_VALUE_LABEL_ADVANCE_EM: f64 = 0.68;
const FOOD_VALUE_LABEL_HALO_RATIO: f64 = 0.13;
const FOOD_VALUE_LABEL_HALO_MIN_PX: f64 = 0.55;
const FOOD_VALUE_LABEL_HALO_MAX_PX: f64 = 1.0;

#[derive(Clone, Copy, Debug, PartialEq)]
struct FoodValueLabelLayout {
    font_px: f64,
    halo_px: f64,
}

fn food_value_label_layout(cell_size: f64, characters: usize) -> FoodValueLabelLayout {
    // The one-pixel food outline belongs to the icon, so a tiny label may use
    // that same overscan. At the 5px arena floor, the legibility clamp wins by
    // a fraction of a pixel instead of collapsing the value mark.
    let available_width = cell_size + 2.0;
    let font_for_width = available_width / (FOOD_VALUE_LABEL_ADVANCE_EM * characters.max(1) as f64);
    let font_px = (cell_size * FOOD_VALUE_LABEL_SIZE_RATIO)
        .min(font_for_width)
        .clamp(FOOD_VALUE_LABEL_MIN_PX, FOOD_VALUE_LABEL_MAX_PX);

    FoodValueLabelLayout {
        font_px,
        halo_px: (font_px * FOOD_VALUE_LABEL_HALO_RATIO)
            .clamp(FOOD_VALUE_LABEL_HALO_MIN_PX, FOOD_VALUE_LABEL_HALO_MAX_PX),
    }
}

fn food_value_label_font(size: f64) -> String {
    format!("900 {size}px \"Arial Black\", Arial, sans-serif")
}

fn combo_food_label_text(value: u32) -> String {
    value.to_string()
}

fn combo_food_label_value(
    chain_count: u32,
    remaining_ms: u32,
    max_food_value: u32,
    is_alive: bool,
) -> Option<u32> {
    if !is_alive || remaining_ms == 0 {
        return None;
    }

    let value = chain_count.saturating_add(1).min(max_food_value.max(1));
    (value > 1).then_some(value)
}

/// Renders a typed game state to a canvas element.
///
/// This is the core renderer: it reads the engine's own `GameState` directly
/// (no JSON string, no `serde_json::Value` indexing), so every field access is
/// type-checked and no silent `unwrap_or` defaults can mask a schema change.
/// Local/opponent usernames are resolved here from `state.usernames` rather than
/// being threaded in as scalar side-channel arguments.
pub fn render_game_state(
    state: &GameState,
    canvas: &web_sys::HtmlCanvasElement,
    cell_size: f64,
    local_user_id: Option<u32>,
    rotation_int: i32,
    draw_celebration: &js_sys::Function,
) -> Result<(), JsValue> {
    let context = canvas
        .get_context("2d")
        .map_err(|_| JsValue::from_str("Failed to get 2d context"))?
        .ok_or_else(|| JsValue::from_str("2d context is null"))?;

    let ctx = context
        .dyn_into::<web_sys::CanvasRenderingContext2d>()
        .map_err(|_| JsValue::from_str("Failed to cast to 2d context"))?;

    // Extract arena dimensions
    let arena = &state.arena;
    let game_width = arena.width as f64;
    let game_height = arena.height as f64;

    // Get effective dimensions for rendering (swapped for vertical orientations)
    let (width, height) = get_effective_dimensions(game_width, game_height, rotation_int);

    // Use a fixed dot radius of 1px to match the background dots
    let dot_radius = 1.0;

    // Get actual canvas dimensions
    let canvas_width = canvas.width() as f64;
    let canvas_height = canvas.height() as f64;

    // Clear entire canvas with white background (including padding area)
    ctx.set_fill_style_str("#ffffff");
    ctx.fill_rect(0.0, 0.0, canvas_width, canvas_height);

    // Add 1px padding offset for all drawing operations
    let padding = 1.0;

    // Save the current state
    ctx.save();
    ctx.translate(padding, padding)?;

    // Fill the game area with white to ensure clean background
    ctx.set_fill_style_str("#ffffff");
    ctx.fill_rect(
        0.0,
        0.0,
        canvas_width - 2.0 * padding,
        canvas_height - 2.0 * padding,
    );

    // Determine which snake belongs to the local player and their team (needed for perspective-based rendering)
    let (local_snake_id, local_player_team): (Option<usize>, Option<u8>) =
        if let Some(user_id) = local_user_id {
            // Resolve the local player's snake index from the players map.
            let snake_id = state
                .players
                .get(&user_id)
                .map(|player| player.snake_id as usize);

            // Get the team of the local player's snake
            let team = snake_id
                .and_then(|sid| arena.snakes.get(sid))
                .and_then(|snake| snake.team_id)
                .map(|t| t.0);

            (snake_id, team)
        } else {
            (None, None)
        };
    let local_food_value = local_snake_id
        .and_then(|snake_id| arena.snakes.get(snake_id))
        .and_then(|snake| {
            combo_food_label_value(
                snake.combo.chain_count,
                snake.combo.remaining_ms,
                state.properties.combo.max_food_value,
                snake.is_alive,
            )
        });

    // Draw team zones if present
    let team_zone_config_data = arena.team_zone_config.as_ref();
    if let Some(team_zone_config) = team_zone_config_data {
        let end_zone_depth = team_zone_config.end_zone_depth as f64;

        // Determine zone background colors based on local player's team
        let (left_color, right_color) = match local_player_team {
            Some(0) => ("#e6f4fa", "#ffe6e6"), // Blue left, red right
            Some(1) => ("#ffe6e6", "#e6f4fa"), // Red left, blue right
            _ => ("#e6f4fa", "#ffe6e6"),       // Default: blue left, red right
        };

        // In the original orientation, zones are on left and right
        // We need to transform these based on rotation
        match rotation_int {
            90 => {
                // 90° CW: left zone becomes top, right zone becomes bottom
                // Top zone
                ctx.set_fill_style_str(left_color);
                ctx.fill_rect(0.0, 0.0, width * cell_size, end_zone_depth * cell_size);

                // Bottom zone
                ctx.set_fill_style_str(right_color);
                ctx.fill_rect(
                    0.0,
                    (height - end_zone_depth) * cell_size,
                    width * cell_size,
                    end_zone_depth * cell_size,
                );
            }
            180 => {
                // 180°: left zone becomes right, right zone becomes left
                // Right zone (was left)
                ctx.set_fill_style_str(left_color);
                ctx.fill_rect(
                    (width - end_zone_depth) * cell_size,
                    0.0,
                    end_zone_depth * cell_size,
                    height * cell_size,
                );

                // Left zone (was right)
                ctx.set_fill_style_str(right_color);
                ctx.fill_rect(0.0, 0.0, end_zone_depth * cell_size, height * cell_size);
            }
            270 => {
                // 270° CW: left zone becomes bottom, right zone becomes top
                // Bottom zone (was left)
                ctx.set_fill_style_str(left_color);
                ctx.fill_rect(
                    0.0,
                    (height - end_zone_depth) * cell_size,
                    width * cell_size,
                    end_zone_depth * cell_size,
                );

                // Top zone (was right)
                ctx.set_fill_style_str(right_color);
                ctx.fill_rect(0.0, 0.0, width * cell_size, end_zone_depth * cell_size);
            }
            _ => {
                // 0° or default: normal orientation
                // Left zone
                ctx.set_fill_style_str(left_color);
                ctx.fill_rect(0.0, 0.0, end_zone_depth * cell_size, height * cell_size);

                // Right zone
                ctx.set_fill_style_str(right_color);
                ctx.fill_rect(
                    (width - end_zone_depth) * cell_size,
                    0.0,
                    end_zone_depth * cell_size,
                    height * cell_size,
                );
            }
        }
    }

    // Available Boost footprints own the grid intersections on their boundary
    // and interior. Omitting those dots up front avoids a white cleanup halo
    // and matches the way solid gameplay objects visually clear the grid.
    // Cooling pads are deliberately absent from this mask as well as the art.
    let active_boost_pad_bounds =
        transformed_active_pad_bounds(&arena.boost_pads, game_width, game_height, rotation_int);

    // Draw dots at grid intersections (like the background pattern)
    ctx.set_fill_style_str("rgba(0, 0, 0, 0.3)"); // Same as background dots

    // Scale dot spacing with cell size to maintain consistent visual density
    let dot_spacing = cell_size;
    let dots_x = (width).ceil() as u32;
    let dots_y = (height).ceil() as u32;

    // Start from 1 and end at dots_x/y - 1 to skip outer edge dots
    for x in 1..dots_x {
        for y in 1..dots_y {
            let dot_x = x as f64 * dot_spacing;
            let dot_y = y as f64 * dot_spacing;

            if grid_dot_is_covered_by_boost(x as f64, y as f64, &active_boost_pad_bounds) {
                continue;
            }

            // Skip dots that are on the exact edges
            if dot_x >= width * cell_size || dot_y >= height * cell_size {
                continue;
            }

            // Draw a small circle dot
            ctx.begin_path();
            ctx.arc(dot_x, dot_y, dot_radius, 0.0, 2.0 * std::f64::consts::PI)?;
            ctx.fill();
        }
    }

    // Draw endzone text after dots but before walls and snakes
    // This ensures text is visible over dots but under snakes
    if let Some(team_zone_config) = team_zone_config_data {
        let end_zone_depth = team_zone_config.end_zone_depth as f64;

        // Build team labels from player usernames; show both teammates side by side
        let mut team_names: [Vec<String>; 2] = [Vec::new(), Vec::new()];
        for (user_id, player) in &state.players {
            if let Some(snake) = arena.snakes.get(player.snake_id as usize)
                && let Some(team_id) = snake.team_id
                && (team_id.0 as usize) < 2
            {
                let username = state
                    .usernames
                    .get(user_id)
                    .cloned()
                    .unwrap_or_else(|| user_id.to_string());
                team_names[team_id.0 as usize].push(username);
            }
        }

        for names in team_names.iter_mut() {
            names.sort();
        }

        // Background and text colors based on perspective
        let (left_bg_color, right_bg_color, left_text_color, right_text_color) =
            match local_player_team {
                Some(0) => ("#e6f4fa", "#ffe6e6", "#c0d8e4", "#e4c0c0"),
                Some(1) => ("#ffe6e6", "#e6f4fa", "#e4c0c0", "#c0d8e4"),
                _ => ("#e6f4fa", "#ffe6e6", "#c0d8e4", "#e4c0c0"),
            };

        // Local/opponent fallback labels, resolved from the state's username map
        // (previously threaded in as scalar arguments from JS).
        let local_name = local_user_id
            .and_then(|uid| state.usernames.get(&uid))
            .map(|s| s.to_uppercase())
            .unwrap_or_else(|| "USER 0".to_string());
        let opponent_name = local_user_id
            .and_then(|local| {
                state
                    .usernames
                    .iter()
                    .find(|(uid, _)| **uid != local)
                    .map(|(_, name)| name.to_uppercase())
            })
            .unwrap_or_else(|| "USER 1".to_string());

        let default_team0 = match local_player_team {
            Some(0) => local_name.clone(),
            Some(1) => opponent_name.clone(),
            _ => opponent_name.clone(),
        };
        let default_team1 = match local_player_team {
            Some(0) => opponent_name.clone(),
            Some(1) => local_name.clone(),
            _ => local_name.clone(),
        };

        let format_names = |names: &[String], fallback: &str| -> Vec<String> {
            if names.is_empty() {
                vec![fallback.to_string()]
            } else {
                names.iter().map(|s| s.to_uppercase()).collect()
            }
        };

        let team0_labels = format_names(&team_names[0], &default_team0);
        let team1_labels = format_names(&team_names[1], &default_team1);

        ctx.set_text_baseline("middle");
        ctx.set_text_align("center");

        // Compute font size that fits inside a given box
        let compute_font_size = |text: &str, max_w: f64, max_h: f64| -> f64 {
            if text.is_empty() || max_w <= 0.0 || max_h <= 0.0 {
                return 1.0;
            }
            let mut size = (max_h * 0.7).min(48.0); // start reasonable
            let min_size = 8.0;
            let estimate_width = |s: f64| text.len() as f64 * s * 0.6;
            while (estimate_width(size) > max_w * 0.9 || size > max_h * 0.8) && size > min_size {
                size -= 1.0;
            }
            size.max(min_size)
        };

        let draw_label_with_size = |ctx: &web_sys::CanvasRenderingContext2d,
                                    text: &str,
                                    center_x: f64,
                                    center_y: f64,
                                    box_w: f64,
                                    box_h: f64,
                                    text_color: &str,
                                    bg_color: &str,
                                    font_size: f64|
         -> Result<(), JsValue> {
            let size = font_size.min(compute_font_size(text, box_w, box_h));
            ctx.set_font(&format!("900 {}px Impact, 'Arial Black', sans-serif", size));
            ctx.set_line_width(size * 0.35);
            ctx.set_stroke_style_str(bg_color);
            ctx.stroke_text(text, center_x, center_y)?;
            ctx.set_fill_style_str(text_color);
            ctx.fill_text(text, center_x, center_y)?;
            Ok(())
        };

        // Helper to draw team labels inside a given rectangle, splitting it into two sub-areas
        let draw_team_zone = |rect: (f64, f64, f64, f64),
                              split_vertical: bool,
                              names: &[String],
                              bg_color: &str,
                              text_color: &str,
                              split_labels: bool|
         -> Result<(), JsValue> {
            let (x, y, w, h) = rect;

            // Decide whether to split into two sub-areas (only when we have >1 name and game mode requires it)
            let (centers, box_w, box_h): (Vec<(f64, f64)>, f64, f64) =
                if split_labels && names.len() > 1 {
                    if split_vertical {
                        let half_h = h / 2.0;
                        (
                            vec![
                                (x + w / 2.0, y + half_h / 2.0),
                                (x + w / 2.0, y + half_h + half_h / 2.0),
                            ],
                            w * 0.8,
                            half_h * 0.9,
                        )
                    } else {
                        let half_w = w / 2.0;
                        (
                            vec![
                                (x + half_w / 2.0, y + h / 2.0),
                                (x + half_w + half_w / 2.0, y + h / 2.0),
                            ],
                            half_w * 0.9,
                            h * 0.8,
                        )
                    }
                } else {
                    // Single name fills whole zone
                    (vec![(x + w / 2.0, y + h / 2.0)], w * 0.9, h * 0.8)
                };

            // Use the same font size for all labels in this zone: smallest that fits every label
            let mut needed_size = compute_font_size(
                names.first().map(|s| s.as_str()).unwrap_or(""),
                box_w,
                box_h,
            );
            if split_labels
                && names.len() > 1
                && let Some(name) = names.get(1)
            {
                needed_size = needed_size.min(compute_font_size(name, box_w, box_h));
            }

            for (i, name) in names.iter().take(centers.len()).enumerate() {
                draw_label_with_size(
                    &ctx,
                    name,
                    centers[i].0,
                    centers[i].1,
                    box_w,
                    box_h,
                    text_color,
                    bg_color,
                    needed_size,
                )?;
            }
            Ok(())
        };

        // Compute the rectangles for each team zone in the current orientation
        let (team0_rect, team1_rect, split_vertical) = match rotation_int {
            90 => (
                // team0 = top, team1 = bottom
                (0.0, 0.0, width * cell_size, end_zone_depth * cell_size),
                (
                    0.0,
                    (height - end_zone_depth) * cell_size,
                    width * cell_size,
                    end_zone_depth * cell_size,
                ),
                false,
            ),
            180 => (
                // team0 = right, team1 = left
                (
                    (width - end_zone_depth) * cell_size,
                    0.0,
                    end_zone_depth * cell_size,
                    height * cell_size,
                ),
                (0.0, 0.0, end_zone_depth * cell_size, height * cell_size),
                true,
            ),
            270 => (
                // team0 = bottom, team1 = top
                (
                    0.0,
                    (height - end_zone_depth) * cell_size,
                    width * cell_size,
                    end_zone_depth * cell_size,
                ),
                (0.0, 0.0, width * cell_size, end_zone_depth * cell_size),
                false,
            ),
            _ => (
                // team0 = left, team1 = right
                (0.0, 0.0, end_zone_depth * cell_size, height * cell_size),
                (
                    (width - end_zone_depth) * cell_size,
                    0.0,
                    end_zone_depth * cell_size,
                    height * cell_size,
                ),
                true,
            ),
        };

        // Draw labels for each team zone (supports up to two names per team)
        let team0_split_labels = team0_labels.len() > 1;
        let team1_split_labels = team1_labels.len() > 1;
        draw_team_zone(
            team0_rect,
            split_vertical,
            &team0_labels,
            left_bg_color,
            left_text_color,
            team0_split_labels,
        )?;
        draw_team_zone(
            team1_rect,
            split_vertical,
            &team1_labels,
            right_bg_color,
            right_text_color,
            team1_split_labels,
        )?;
    }

    // Note: Walls will be drawn after snakes to ensure dead snakes appear behind walls

    // Draw food
    {
        // First pass: Draw white squares to erase grid dots
        ctx.set_fill_style_str("#ffffff");
        for food in &arena.food {
            let (tx, ty) = transform_coords(
                food.x as f64,
                food.y as f64,
                game_width,
                game_height,
                rotation_int,
            );
            let cell_x = tx * cell_size;
            let cell_y = ty * cell_size;
            // Draw white rectangle 1px larger than the cell to erase dots
            ctx.fill_rect(cell_x - 1.0, cell_y - 1.0, cell_size + 2.0, cell_size + 2.0);
        }

        // Second pass: Draw the actual food
        for food in &arena.food {
            let (tx, ty) = transform_coords(
                food.x as f64,
                food.y as f64,
                game_width,
                game_height,
                rotation_int,
            );
            let cell_x = tx * cell_size;
            let cell_y = ty * cell_size;
            let center_x = cell_x + cell_size / 2.0;
            let center_y = cell_y + cell_size / 2.0;
            let radius = cell_size / 2.0;

            // Draw darker border
            ctx.set_fill_style_str("#5e8a5e");
            ctx.begin_path();
            ctx.arc(
                center_x,
                center_y,
                radius + 1.0,
                0.0,
                2.0 * std::f64::consts::PI,
            )?;
            ctx.fill();

            // Draw food base
            ctx.set_fill_style_str("#85b885");
            ctx.begin_path();
            ctx.arc(center_x, center_y, radius, 0.0, 2.0 * std::f64::consts::PI)?;
            ctx.fill();

            // Draw single light reflection in top-left
            ctx.set_fill_style_str("#a0c8a0");
            ctx.begin_path();
            ctx.arc(
                center_x - radius * 0.35,
                center_y - radius * 0.35,
                radius * 0.25,
                0.0,
                2.0 * std::f64::consts::PI,
            )?;
            ctx.fill();
        }

        // Third pass: the same food is worth the same prospective amount to
        // this local player, so every item receives one compact white mark.
        // Spectators and inactive chains resolve `None` above and retain the
        // original clean food art.
        if let Some(value) = local_food_value {
            let text = combo_food_label_text(value);
            let layout = food_value_label_layout(cell_size, text.chars().count());
            ctx.save();
            ctx.set_text_align("center");
            ctx.set_text_baseline("middle");
            ctx.set_line_join("round");
            ctx.set_font(&food_value_label_font(layout.font_px));
            ctx.set_line_width(layout.halo_px);
            ctx.set_stroke_style_str("#416c45");
            ctx.set_fill_style_str("#ffffff");

            for food in &arena.food {
                let (tx, ty) = transform_coords(
                    food.x as f64,
                    food.y as f64,
                    game_width,
                    game_height,
                    rotation_int,
                );
                let center_x = tx * cell_size + cell_size / 2.0;
                // A quarter physical pixel counters the optical low bias of a
                // bold digit on Canvas' `middle` baseline.
                let center_y = ty * cell_size + cell_size / 2.0 - 0.25;
                ctx.stroke_text(&text, center_x, center_y)?;
                ctx.fill_text(&text, center_x, center_y)?;
            }
            ctx.restore();
        }
    }

    // Draw only available Boost packets after food and before snakes. Cooling
    // pads disappear completely; there is no gray placeholder competing with
    // gameplay. The authoritative footprint selects a single diagonal NOS
    // canister for a 1x pad or one oversized upright tank for a 2x2 pad.
    for pad in &arena.boost_pads {
        if pad.respawn_at_tick.is_some() {
            continue;
        }
        let Some((x, y, width_cells, height_cells)) =
            transformed_pad_bounds(pad, game_width, game_height, rotation_int)
        else {
            continue;
        };
        let left = x * cell_size;
        let top = y * cell_size;
        if pad.size_cells > 1 && width_cells >= 2.0 && height_cells >= 2.0 {
            draw_full_nos_canister(&ctx, left, top, cell_size)?;
        } else {
            draw_regular_nos_canister(&ctx, left, top, cell_size)?;
        }
    }

    // JavaScript owns score-effect animation, but the scoring snake must stay
    // above it. Temporarily return the canvas to its public, un-translated
    // coordinate system so the callback can use the same 1px-padded positions
    // it used when effects were painted after the complete Rust frame. Restore
    // our field transform afterwards before drawing snakes and walls.
    ctx.restore();
    ctx.save();
    // Canvas and cell size are supplied for focused renderers such as the
    // tutorial crop. Existing live callbacks intentionally ignore arguments.
    let celebration_result = draw_celebration.call2(
        &JsValue::NULL,
        canvas.as_ref(),
        &JsValue::from_f64(cell_size),
    );
    ctx.restore();
    if let Err(error) = celebration_result {
        // A cosmetic renderer must never suppress gameplay. The web-side
        // renderer also isolates each swappable effect in `finally` blocks;
        // this callback-level save/restore and the explicit resets below are
        // defense in depth for any error that still crosses the WASM boundary.
        web_sys::console::error_2(
            &JsValue::from_str("Score celebration callback failed"),
            &error,
        );
    }
    ctx.set_transform(1.0, 0.0, 0.0, 1.0, 0.0, 0.0)?;
    ctx.set_global_alpha(1.0);
    ctx.save();
    ctx.translate(padding, padding)?;

    // Draw snakes (both alive and dead)
    let snakes = &arena.snakes;
    // Carried-food readouts are queued here and painted after every snake and
    // the walls, so a neighbouring snake's mask or a goal wall cannot cut into
    // a number.
    let mut carried_food_labels: Vec<CarriedFoodLabel> = Vec::new();
    for (index, snake) in snakes.iter().enumerate() {
        let is_alive = snake.is_alive;

        if is_alive {
            // Choose snake color based on perspective in team games
            let team_member_slot = snakes[..index]
                .iter()
                .filter(|candidate| candidate.team_id == snake.team_id)
                .count();
            let (color, ordinary_border_color) = snake_palette(
                index,
                snake.team_id.map(|team| team.0),
                team_member_slot,
                snakes.len(),
                team_zone_config_data.is_some(),
                local_snake_id,
                local_player_team,
            );

            // Hand the shared skin painter a rotation-resolved body. The roster
            // glyph calls the very same routine, so the two can never drift.
            let cells: Vec<(f64, f64)> = snake
                .body
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

            draw_alive_snake_skin(
                &ctx,
                &cells,
                cell_size,
                color,
                ordinary_border_color,
                snake.boost().active,
                Some("#ffffff"),
            )?;

            // Queue the carried-food readout. It rides a couple of cells behind
            // the head, clear of the dark head core and on the stretch of body
            // the head gradient lightens most. A snake carrying nothing queues
            // nothing.
            let carried_food = state.carried_food(snake);
            if carried_food > 0
                && let Some(anchor) = body_cell_behind_head(&snake.body, CARRIED_LABEL_OFFSET_CELLS)
                && Some(&anchor.cell) != snake.body.first()
            {
                let (anchor_tx, anchor_ty) = transform_coords(
                    anchor.cell.x as f64,
                    anchor.cell.y as f64,
                    game_width,
                    game_height,
                    rotation_int,
                );
                carried_food_labels.push(CarriedFoodLabel {
                    center_x: anchor_tx * cell_size + cell_size / 2.0,
                    center_y: anchor_ty * cell_size + cell_size / 2.0,
                    text: carried_food.to_string(),
                    horizontal_on_screen: anchor_run_is_horizontal_on_screen(
                        anchor.run_is_horizontal,
                        rotation_int,
                    ),
                    ink: roster_label_ink(color),
                });
            }
        } else {
            // Render dead snake with faint solid color
            let color = "#f0f0f0"; // Light gray for dead snakes
            let border_color = "#d0d0d0"; // Slightly darker border

            ctx.set_fill_style_str(color);

            // Draw snake body
            let body = &snake.body;
            if body.is_empty() {
                continue;
            }

            // Handle single-segment snake (just a head)
            if body.len() == 1 {
                let head = &body[0];
                let (tx, ty) = transform_coords(
                    head.x as f64,
                    head.y as f64,
                    game_width,
                    game_height,
                    rotation_int,
                );
                let center_x = tx * cell_size + cell_size / 2.0;
                let center_y = ty * cell_size + cell_size / 2.0;

                // Draw border
                ctx.set_fill_style_str(border_color);
                ctx.begin_path();
                ctx.arc(
                    center_x,
                    center_y,
                    cell_size / 2.0 + 1.0,
                    0.0,
                    2.0 * std::f64::consts::PI,
                )?;
                ctx.fill();

                // Draw as a full circle
                ctx.set_fill_style_str(color);
                ctx.begin_path();
                ctx.arc(
                    center_x,
                    center_y,
                    cell_size / 2.0,
                    0.0,
                    2.0 * std::f64::consts::PI,
                )?;
                ctx.fill();

                // Draw X mark on head
                ctx.set_stroke_style_str("#666");
                ctx.set_line_width(2.0);
                let x_size = cell_size * 0.3;
                ctx.begin_path();
                ctx.move_to(center_x - x_size, center_y - x_size);
                ctx.line_to(center_x + x_size, center_y + x_size);
                ctx.stroke();
                ctx.begin_path();
                ctx.move_to(center_x - x_size, center_y + x_size);
                ctx.line_to(center_x + x_size, center_y - x_size);
                ctx.stroke();
                continue;
            }

            // First pass: Fill with white rectangles to cover grid dots
            ctx.set_fill_style_str("#ffffff");

            // Fill white rectangles for body segments (expanded by 1px)
            for window in body.windows(2) {
                let (p1, p2) = (&window[0], &window[1]);
                let x1 = p1.x as f64;
                let y1 = p1.y as f64;
                let x2 = p2.x as f64;
                let y2 = p2.y as f64;

                // Transform both points
                let (tx1, ty1) = transform_coords(x1, y1, game_width, game_height, rotation_int);
                let (tx2, ty2) = transform_coords(x2, y2, game_width, game_height, rotation_int);

                if (tx1 - tx2).abs() < 0.01 {
                    // Vertical segment after transformation - draw rectangle
                    let x = tx1 * cell_size;
                    let min_y = ty1.min(ty2) * cell_size;
                    let max_y = ty1.max(ty2) * cell_size;
                    ctx.fill_rect(
                        x - 1.0,
                        min_y - 1.0,
                        cell_size + 2.0,
                        (max_y - min_y) + cell_size + 2.0,
                    );
                } else if (ty1 - ty2).abs() < 0.01 {
                    // Horizontal segment after transformation - draw rectangle
                    let y = ty1 * cell_size;
                    let min_x = tx1.min(tx2) * cell_size;
                    let max_x = tx1.max(tx2) * cell_size;
                    ctx.fill_rect(
                        min_x - 1.0,
                        y - 1.0,
                        (max_x - min_x) + cell_size + 2.0,
                        cell_size + 2.0,
                    );
                }
            }

            // Fill white rectangles for all body points (expanded by 1px)
            for point in body.iter() {
                let (tx, ty) = transform_coords(
                    point.x as f64,
                    point.y as f64,
                    game_width,
                    game_height,
                    rotation_int,
                );
                let rect_x = tx * cell_size - 1.0;
                let rect_y = ty * cell_size - 1.0;
                ctx.fill_rect(rect_x, rect_y, cell_size + 2.0, cell_size + 2.0);
            }

            // Second pass: Draw borders (1px larger)
            ctx.set_stroke_style_str(border_color);

            // Draw border for body segments
            for window in body.windows(2) {
                let (p1, p2) = (&window[0], &window[1]);
                let x1 = p1.x as f64;
                let y1 = p1.y as f64;
                let x2 = p2.x as f64;
                let y2 = p2.y as f64;

                // Transform both points
                let (tx1, ty1) = transform_coords(x1, y1, game_width, game_height, rotation_int);
                let (tx2, ty2) = transform_coords(x2, y2, game_width, game_height, rotation_int);

                if (tx1 - tx2).abs() < 0.01 {
                    // Vertical segment after transformation
                    let x = tx1 * cell_size + cell_size / 2.0;
                    let min_y = ty1.min(ty2) * cell_size + cell_size / 2.0;
                    let max_y = ty1.max(ty2) * cell_size + cell_size / 2.0;

                    ctx.set_line_width(cell_size + 2.0);
                    ctx.set_line_cap("round");
                    ctx.begin_path();
                    ctx.move_to(x, min_y);
                    ctx.line_to(x, max_y);
                    ctx.stroke();
                } else if (ty1 - ty2).abs() < 0.01 {
                    // Horizontal segment after transformation
                    let y = ty1 * cell_size + cell_size / 2.0;
                    let min_x = tx1.min(tx2) * cell_size + cell_size / 2.0;
                    let max_x = tx1.max(tx2) * cell_size + cell_size / 2.0;

                    ctx.set_line_width(cell_size + 2.0);
                    ctx.set_line_cap("round");
                    ctx.begin_path();
                    ctx.move_to(min_x, y);
                    ctx.line_to(max_x, y);
                    ctx.stroke();
                }
            }

            // Draw border for corner joints
            ctx.set_fill_style_str(border_color);
            for point in &body[1..body.len() - 1] {
                let (tx, ty) = transform_coords(
                    point.x as f64,
                    point.y as f64,
                    game_width,
                    game_height,
                    rotation_int,
                );
                let center_x = tx * cell_size + cell_size / 2.0;
                let center_y = ty * cell_size + cell_size / 2.0;

                ctx.begin_path();
                ctx.arc(
                    center_x,
                    center_y,
                    cell_size / 2.0 + 1.0,
                    0.0,
                    2.0 * std::f64::consts::PI,
                )?;
                ctx.fill();
            }

            // Third pass: Draw the actual snake
            ctx.set_stroke_style_str(color);
            ctx.set_fill_style_str(color);

            // Draw main body segments
            for window in body.windows(2) {
                let (p1, p2) = (&window[0], &window[1]);
                let x1 = p1.x as f64;
                let y1 = p1.y as f64;
                let x2 = p2.x as f64;
                let y2 = p2.y as f64;

                // Transform both points
                let (tx1, ty1) = transform_coords(x1, y1, game_width, game_height, rotation_int);
                let (tx2, ty2) = transform_coords(x2, y2, game_width, game_height, rotation_int);

                if (tx1 - tx2).abs() < 0.01 {
                    // Vertical segment after transformation
                    let x = tx1 * cell_size + cell_size / 2.0;
                    let min_y = ty1.min(ty2) * cell_size + cell_size / 2.0;
                    let max_y = ty1.max(ty2) * cell_size + cell_size / 2.0;

                    ctx.set_line_width(cell_size);
                    ctx.set_line_cap("round");
                    ctx.begin_path();
                    ctx.move_to(x, min_y);
                    ctx.line_to(x, max_y);
                    ctx.stroke();
                } else if (ty1 - ty2).abs() < 0.01 {
                    // Horizontal segment after transformation
                    let y = ty1 * cell_size + cell_size / 2.0;
                    let min_x = tx1.min(tx2) * cell_size + cell_size / 2.0;
                    let max_x = tx1.max(tx2) * cell_size + cell_size / 2.0;

                    ctx.set_line_width(cell_size);
                    ctx.set_line_cap("round");
                    ctx.begin_path();
                    ctx.move_to(min_x, y);
                    ctx.line_to(max_x, y);
                    ctx.stroke();
                }
            }

            // Draw corner joints as circles to create smooth turns
            for point in &body[1..body.len() - 1] {
                let (tx, ty) = transform_coords(
                    point.x as f64,
                    point.y as f64,
                    game_width,
                    game_height,
                    rotation_int,
                );
                let center_x = tx * cell_size + cell_size / 2.0;
                let center_y = ty * cell_size + cell_size / 2.0;

                ctx.begin_path();
                ctx.arc(
                    center_x,
                    center_y,
                    cell_size / 2.0,
                    0.0,
                    2.0 * std::f64::consts::PI,
                )?;
                ctx.fill();
            }

            // Get head and tail information
            let head = &body[0];
            let head_x = head.x as f64;
            let head_y = head.y as f64;
            let (head_tx, head_ty) =
                transform_coords(head_x, head_y, game_width, game_height, rotation_int);
            let head_center_x = head_tx * cell_size + cell_size / 2.0;
            let head_center_y = head_ty * cell_size + cell_size / 2.0;

            let tail = &body[body.len() - 1];
            let tail_x = tail.x as f64;
            let tail_y = tail.y as f64;
            let (tail_tx, tail_ty) =
                transform_coords(tail_x, tail_y, game_width, game_height, rotation_int);
            let tail_center_x = tail_tx * cell_size + cell_size / 2.0;
            let tail_center_y = tail_ty * cell_size + cell_size / 2.0;

            // Draw tail as full circle
            ctx.set_fill_style_str(color);
            ctx.begin_path();
            ctx.arc(
                tail_center_x,
                tail_center_y,
                cell_size / 2.0,
                0.0,
                2.0 * std::f64::consts::PI,
            )?;
            ctx.fill();

            // Draw head as full circle
            ctx.begin_path();
            ctx.arc(
                head_center_x,
                head_center_y,
                cell_size / 2.0,
                0.0,
                2.0 * std::f64::consts::PI,
            )?;
            ctx.fill();

            // Draw X mark on dead snake head
            ctx.set_stroke_style_str("#666");
            ctx.set_line_width(2.0);
            let x_size = cell_size * 0.3;
            ctx.begin_path();
            ctx.move_to(head_center_x - x_size, head_center_y - x_size);
            ctx.line_to(head_center_x + x_size, head_center_y + x_size);
            ctx.stroke();
            ctx.begin_path();
            ctx.move_to(head_center_x - x_size, head_center_y + x_size);
            ctx.line_to(head_center_x + x_size, head_center_y - x_size);
            ctx.stroke();
        }
    }

    // Draw walls AFTER snakes so dead snakes appear behind walls
    if let Some(team_zone_config) = team_zone_config_data {
        let end_zone_depth = team_zone_config.end_zone_depth as f64;
        let goal_width = team_zone_config.goal_width as f64;

        // Draw walls as 3px solid rectangles between field and endzone cells
        let wall_thickness = 3.0;

        // Determine wall colors based on local player's team
        let (left_wall_color, right_wall_color) = match local_player_team {
            Some(0) => ("#7aa8c1", "#c18888"), // Local is Team 0: blue left, red right
            Some(1) => ("#c18888", "#7aa8c1"), // Local is Team 1: red left, blue right
            _ => ("#7aa8c1", "#c18888"),       // Default: blue left, red right
        };

        // Draw walls based on rotation
        match rotation_int {
            90 => {
                // 90° CW: walls are horizontal at top and bottom
                let goal_center = width / 2.0;
                let goal_half_width = goal_width / 2.0;
                let goal_x_start = (goal_center - goal_half_width).floor();
                let goal_x_end = (goal_center + goal_half_width).ceil();

                // Top wall (was left wall)
                ctx.set_fill_style_str(left_wall_color);
                let wall_y = end_zone_depth * cell_size - wall_thickness / 2.0;

                if goal_x_start > 0.0 {
                    ctx.fill_rect(0.0, wall_y, goal_x_start * cell_size, wall_thickness);
                }
                if goal_x_end < width {
                    ctx.fill_rect(
                        goal_x_end * cell_size,
                        wall_y,
                        (width - goal_x_end) * cell_size,
                        wall_thickness,
                    );
                }

                // Bottom wall (was right wall)
                ctx.set_fill_style_str(right_wall_color);
                let wall_y = (height - end_zone_depth) * cell_size - wall_thickness / 2.0;

                if goal_x_start > 0.0 {
                    ctx.fill_rect(0.0, wall_y, goal_x_start * cell_size, wall_thickness);
                }
                if goal_x_end < width {
                    ctx.fill_rect(
                        goal_x_end * cell_size,
                        wall_y,
                        (width - goal_x_end) * cell_size,
                        wall_thickness,
                    );
                }
            }
            180 => {
                // 180°: walls are vertical but swapped positions
                let goal_center = height / 2.0;
                let goal_half_width = goal_width / 2.0;
                let goal_y_start = (goal_center - goal_half_width).floor();
                let goal_y_end = (goal_center + goal_half_width).ceil();

                // Right wall (was left wall)
                ctx.set_fill_style_str(left_wall_color);
                let wall_x = (width - end_zone_depth) * cell_size - wall_thickness / 2.0;

                if goal_y_start > 0.0 {
                    ctx.fill_rect(wall_x, 0.0, wall_thickness, goal_y_start * cell_size);
                }
                if goal_y_end < height {
                    ctx.fill_rect(
                        wall_x,
                        goal_y_end * cell_size,
                        wall_thickness,
                        (height - goal_y_end) * cell_size,
                    );
                }

                // Left wall (was right wall)
                ctx.set_fill_style_str(right_wall_color);
                let wall_x = end_zone_depth * cell_size - wall_thickness / 2.0;

                if goal_y_start > 0.0 {
                    ctx.fill_rect(wall_x, 0.0, wall_thickness, goal_y_start * cell_size);
                }
                if goal_y_end < height {
                    ctx.fill_rect(
                        wall_x,
                        goal_y_end * cell_size,
                        wall_thickness,
                        (height - goal_y_end) * cell_size,
                    );
                }
            }
            270 => {
                // 270° CW: walls are horizontal at bottom and top
                let goal_center = width / 2.0;
                let goal_half_width = goal_width / 2.0;
                let goal_x_start = (goal_center - goal_half_width).floor();
                let goal_x_end = (goal_center + goal_half_width).ceil();

                // Bottom wall (was left wall)
                ctx.set_fill_style_str(left_wall_color);
                let wall_y = (height - end_zone_depth) * cell_size - wall_thickness / 2.0;

                if goal_x_start > 0.0 {
                    ctx.fill_rect(0.0, wall_y, goal_x_start * cell_size, wall_thickness);
                }
                if goal_x_end < width {
                    ctx.fill_rect(
                        goal_x_end * cell_size,
                        wall_y,
                        (width - goal_x_end) * cell_size,
                        wall_thickness,
                    );
                }

                // Top wall (was right wall)
                ctx.set_fill_style_str(right_wall_color);
                let wall_y = end_zone_depth * cell_size - wall_thickness / 2.0;

                if goal_x_start > 0.0 {
                    ctx.fill_rect(0.0, wall_y, goal_x_start * cell_size, wall_thickness);
                }
                if goal_x_end < width {
                    ctx.fill_rect(
                        goal_x_end * cell_size,
                        wall_y,
                        (width - goal_x_end) * cell_size,
                        wall_thickness,
                    );
                }
            }
            _ => {
                // 0° or default: normal vertical walls
                let goal_center = height / 2.0;
                let goal_half_width = goal_width / 2.0;
                let goal_y_start = (goal_center - goal_half_width).floor();
                let goal_y_end = (goal_center + goal_half_width).ceil();

                // Left wall
                ctx.set_fill_style_str(left_wall_color);
                let wall_x = end_zone_depth * cell_size - wall_thickness / 2.0;

                if goal_y_start > 0.0 {
                    ctx.fill_rect(wall_x, 0.0, wall_thickness, goal_y_start * cell_size);
                }
                if goal_y_end < height {
                    ctx.fill_rect(
                        wall_x,
                        goal_y_end * cell_size,
                        wall_thickness,
                        (height - goal_y_end) * cell_size,
                    );
                }

                // Right wall
                ctx.set_fill_style_str(right_wall_color);
                let wall_x = (width - end_zone_depth) * cell_size - wall_thickness / 2.0;

                if goal_y_start > 0.0 {
                    ctx.fill_rect(wall_x, 0.0, wall_thickness, goal_y_start * cell_size);
                }
                if goal_y_end < height {
                    ctx.fill_rect(
                        wall_x,
                        goal_y_end * cell_size,
                        wall_thickness,
                        (height - goal_y_end) * cell_size,
                    );
                }
            }
        }
    }

    // Carried-food readouts, painted after every snake and the walls.
    // `font`, `textAlign`, `textBaseline` and `lineJoin` are the one class of
    // canvas state nothing else in this file resets, so the pass is wrapped to
    // keep it from moving unrelated text.
    if !carried_food_labels.is_empty() {
        ctx.save();
        ctx.set_text_align("center");
        ctx.set_text_baseline("middle");
        ctx.set_line_join("round");
        for label in &carried_food_labels {
            let layout =
                carried_food_label_layout(cell_size, label.text.len(), label.horizontal_on_screen);
            ctx.set_font(&carried_food_label_font(layout.font_px));
            ctx.set_line_width(layout.halo_px);
            ctx.set_stroke_style_str(carried_label_halo(label.ink));
            ctx.stroke_text(&label.text, label.center_x, label.center_y)?;
            ctx.set_fill_style_str(label.ink);
            ctx.fill_text(&label.text, label.center_x, label.center_y)?;
        }
        ctx.restore();
    }

    // Draw game info
    ctx.set_fill_style_str("#333");
    ctx.set_font("16px monospace");
    ctx.fill_text(&format!("Tick: {}", state.tick), 10.0, canvas_height + 20.0)?;

    // Restore the canvas state (remove padding translation)
    ctx.restore();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Head at (10,5), three cells left to the turn at (7,5), three cells down
    /// to the tail at (7,8). Seven cells long, stored as three points.
    fn l_shaped_body() -> Vec<Position> {
        vec![
            Position { x: 10, y: 5 },
            Position { x: 7, y: 5 },
            Position { x: 7, y: 8 },
        ]
    }

    #[test]
    fn carried_label_anchor_walks_the_compressed_body_cell_by_cell() {
        let body = l_shaped_body();
        let cell_at = |back| {
            body_cell_behind_head(&body, back)
                .expect("body is not empty")
                .cell
        };

        // Every step is one grid cell even though the body stores only three
        // points, and the walk turns the corner without skipping or repeating.
        assert_eq!(cell_at(0), Position { x: 10, y: 5 });
        assert_eq!(cell_at(1), Position { x: 9, y: 5 });
        assert_eq!(cell_at(2), Position { x: 8, y: 5 });
        assert_eq!(cell_at(3), Position { x: 7, y: 5 });
        assert_eq!(cell_at(4), Position { x: 7, y: 6 });
        assert_eq!(cell_at(6), Position { x: 7, y: 8 });

        // The walk visits exactly `Snake::length` distinct cells.
        let cells: HashSet<_> = (0..7).map(cell_at).collect();
        assert_eq!(cells.len(), 7);
    }

    #[test]
    fn carried_label_anchor_reports_the_axis_of_the_run_it_landed_on() {
        let body = l_shaped_body();
        let horizontal_at = |back| {
            body_cell_behind_head(&body, back)
                .expect("body is not empty")
                .run_is_horizontal
        };

        // Cells 0..=3 sit on the horizontal run out of the head; 4 onward sit
        // on the vertical run past the turn. The axis must come from the run
        // the cell is on, not from the head-to-cell chord.
        for back in 0..=3 {
            assert!(horizontal_at(back), "cell {back} is on the horizontal run");
        }
        for back in 4..=6 {
            assert!(!horizontal_at(back), "cell {back} is on the vertical run");
        }
    }

    /// A snake that turned one cell ago has a diagonal head-to-anchor chord,
    /// so any orientation guessed from that chord is wrong for half of all
    /// turns. The anchor's own run is the only reliable source.
    #[test]
    fn carried_label_axis_is_correct_on_the_step_right_after_a_turn() {
        // Head at (10,5): one cell left of the turn at (9,5), then straight
        // down. The anchor two cells back is (9,6) — on the VERTICAL run —
        // while the head-to-anchor chord is diagonal.
        let body = vec![
            Position { x: 10, y: 5 },
            Position { x: 9, y: 5 },
            Position { x: 9, y: 9 },
        ];
        let anchor = body_cell_behind_head(&body, 2).expect("body is not empty");

        assert_eq!(anchor.cell, Position { x: 9, y: 6 });
        assert!(
            !anchor.run_is_horizontal,
            "the cell behind a fresh turn lies on the vertical run"
        );
        // Unrotated that run is screen-vertical; the rotated team arenas
        // flip it. Both must follow the run, not the diagonal chord.
        assert!(!anchor_run_is_horizontal_on_screen(
            anchor.run_is_horizontal,
            0
        ));
        assert!(anchor_run_is_horizontal_on_screen(
            anchor.run_is_horizontal,
            270
        ));
    }

    #[test]
    fn carried_label_anchor_clamps_to_the_tail_and_rejects_an_empty_body() {
        let body = l_shaped_body();

        // A snake shorter than the offset clamps instead of wrapping or panicking.
        for back in [7, 99] {
            let anchor = body_cell_behind_head(&body, back).expect("body is not empty");
            assert_eq!(anchor.cell, Position { x: 7, y: 8 });
            assert!(!anchor.run_is_horizontal, "the tail run is vertical");
        }
        assert_eq!(body_cell_behind_head(&[], CARRIED_LABEL_OFFSET_CELLS), None);

        // A zero-extent body resolves to the head, which the draw pass skips
        // via its `anchor.cell != *head` guard rather than stamping the number
        // on top of the dark head disc.
        let degenerate = vec![Position { x: 3, y: 3 }, Position { x: 3, y: 3 }];
        assert_eq!(
            body_cell_behind_head(&degenerate, CARRIED_LABEL_OFFSET_CELLS)
                .expect("body is not empty")
                .cell,
            Position { x: 3, y: 3 }
        );
    }

    #[test]
    fn carried_label_orientation_follows_every_arena_rotation() {
        // Grid-horizontal stays horizontal only in the unrotated orientations;
        // team matches default to 270/90, where the same run is screen-vertical.
        for rotation in [0, 180] {
            assert!(anchor_run_is_horizontal_on_screen(true, rotation));
            assert!(!anchor_run_is_horizontal_on_screen(false, rotation));
        }
        for rotation in [90, 270] {
            assert!(!anchor_run_is_horizontal_on_screen(true, rotation));
            assert!(anchor_run_is_horizontal_on_screen(false, rotation));
        }
    }

    #[test]
    fn carried_label_layout_never_condenses_and_stays_within_its_clamp() {
        // GameArena.tsx walks cell size down through the integers 15..=5, and
        // the arena canvas is not devicePixelRatio-scaled, so these are
        // physical pixels.
        for cell_size in 5..=15 {
            let cell_size = f64::from(cell_size);
            for digits in 1..=4 {
                for horizontal in [true, false] {
                    let layout = carried_food_label_layout(cell_size, digits, horizontal);

                    assert!(layout.font_px >= CARRIED_LABEL_MIN_PX);
                    assert!(layout.font_px <= CARRIED_LABEL_MAX_PX);
                    assert!(layout.font_px <= cell_size.max(CARRIED_LABEL_MIN_PX));
                    // The halo must never eat a whole stem of a 900-weight glyph.
                    assert!(layout.halo_px < layout.font_px * 0.25);

                    // Either the number fits the room it was given, or the
                    // legibility floor is what stopped it shrinking further.
                    let available = if horizontal {
                        cell_size * CARRIED_LABEL_ALONG_BODY_CELLS
                    } else {
                        cell_size + CARRIED_LABEL_ACROSS_BODY_BLEED_PX
                    };
                    let natural = layout.font_px * CARRIED_LABEL_DIGIT_ADVANCE_EM * digits as f64;
                    assert!(
                        natural <= available + 1e-9 || layout.font_px == CARRIED_LABEL_MIN_PX,
                        "{digits} digits at cell {cell_size} (horizontal={horizontal}) \
                         overflow without hitting the floor"
                    );
                }
            }
        }

        // A wider number shrinks rather than condensing, and the tighter
        // across-body room shrinks it at least as much as the roomy along-body
        // case — which is the orientation most team-match snakes are in.
        let one = carried_food_label_layout(10.0, 1, false);
        let two = carried_food_label_layout(10.0, 2, false);
        let three = carried_food_label_layout(10.0, 3, false);
        assert!(two.font_px < one.font_px);
        assert!(three.font_px <= two.font_px);
        assert!(
            carried_food_label_layout(10.0, 2, false).font_px
                <= carried_food_label_layout(10.0, 2, true).font_px
        );

        // A single digit never shrinks at a comfortable cell size.
        assert_eq!(carried_food_label_layout(12.0, 1, false).font_px, 12.0);
    }

    #[test]
    fn carried_label_font_declares_the_requested_size_and_black_weight() {
        let font = carried_food_label_font(9.0);

        assert!(font.starts_with("900 9px "));
        assert!(font.contains("Arial Black"));
        assert!(!font.to_ascii_lowercase().contains("italic"));
    }

    #[test]
    fn combo_food_labels_follow_the_local_snakes_next_value_and_cap() {
        assert_eq!(combo_food_label_value(1, 1_000, 3, true), Some(2));
        assert_eq!(combo_food_label_value(2, 750, 3, true), Some(3));
        assert_eq!(combo_food_label_value(99, 50, 3, true), Some(3));

        // The first ordinary food, expiry, death, and a defensive cap of one
        // all preserve the original unlabelled food art.
        assert_eq!(combo_food_label_value(0, 0, 3, true), None);
        assert_eq!(combo_food_label_value(2, 0, 3, true), None);
        assert_eq!(combo_food_label_value(2, 500, 3, false), None);
        assert_eq!(combo_food_label_value(0, 500, 1, true), None);

        assert_eq!(combo_food_label_text(2), "2");
        assert_eq!(combo_food_label_text(3), "3");
    }

    #[test]
    fn food_value_label_stays_readable_across_every_arena_cell_size() {
        for cell_size in 5..=15 {
            let cell_size = f64::from(cell_size);
            let layout = food_value_label_layout(cell_size, 1);

            assert!(layout.font_px >= FOOD_VALUE_LABEL_MIN_PX);
            assert!(layout.font_px <= FOOD_VALUE_LABEL_MAX_PX);
            assert!(layout.halo_px >= FOOD_VALUE_LABEL_HALO_MIN_PX);
            assert!(layout.halo_px <= FOOD_VALUE_LABEL_HALO_MAX_PX);
            assert!(layout.halo_px < layout.font_px * 0.2);

            let natural_width = layout.font_px * FOOD_VALUE_LABEL_ADVANCE_EM;
            assert!(
                natural_width <= cell_size + 2.0 + 1e-9
                    || layout.font_px == FOOD_VALUE_LABEL_MIN_PX,
                "food value at cell {cell_size} overflows without hitting the floor"
            );
        }

        assert!((food_value_label_layout(10.0, 1).font_px - 7.6).abs() < 1e-9);
        assert_eq!(food_value_label_layout(15.0, 1).font_px, 10.0);

        let font = food_value_label_font(7.0);
        assert!(font.starts_with("900 7px "));
        assert!(font.contains("Arial Black"));
        assert!(!font.to_ascii_lowercase().contains("italic"));
    }

    #[test]
    fn team_palette_distinguishes_teammates_without_crossing_team_hues() {
        let local = snake_palette(0, Some(0), 0, 4, true, Some(0), Some(0));
        let teammate = snake_palette(2, Some(0), 1, 4, true, Some(0), Some(0));
        let opponent = snake_palette(1, Some(1), 0, 4, true, Some(0), Some(0));

        assert_ne!(local, teammate);
        assert_ne!(local, opponent);
        assert!(local.0.starts_with("#70"));
        assert!(teammate.0.starts_with("#3c"));
        assert!(opponent.0.starts_with("#ff"));
    }

    #[test]
    fn team_palette_is_stable_for_spectators() {
        assert_eq!(
            snake_palette(3, Some(0), 1, 4, true, None, None),
            ("#3c8dde", "#286eae")
        );
        assert_eq!(
            snake_palette(0, Some(1), 0, 4, true, None, None),
            ("#ff6b6b", "#b84444")
        );
    }

    #[test]
    fn active_boost_outline_layers_yellow_outside_the_ordinary_contour() {
        let ordinary_color = "#5299bb";
        let layers = snake_outline_layers(true);

        assert_eq!(BOOST_OUTER_COLOR, "#fff200");
        assert_eq!(layers.len(), 2);
        assert_eq!(layers[0].paint, SnakeOutlinePaint::BoostOuter);
        assert_eq!(layers[0].color(ordinary_color), BOOST_OUTER_COLOR);
        assert_eq!(layers[1].paint, SnakeOutlinePaint::Ordinary);
        assert_eq!(layers[1].color(ordinary_color), ordinary_color);

        let cell_size = 10.0;
        assert_eq!(layers[0].line_width(cell_size), 16.0);
        assert_eq!(layers[1].line_width(cell_size), 12.0);
        assert_eq!(layers[0].radius(cell_size), 8.0);
        assert_eq!(layers[1].radius(cell_size), 6.0);
        assert_eq!(
            layers[0].radius(cell_size) - layers[1].radius(cell_size),
            2.0
        );
        assert_eq!(layers[1].radius(cell_size) - cell_size / 2.0, 1.0);
        assert_eq!(snake_mask_extra(true), 3.0);
    }

    #[test]
    fn inactive_snake_keeps_the_existing_single_pixel_contour_and_mask() {
        let ordinary_color = "#b84444";
        let layers = snake_outline_layers(false);

        assert_eq!(layers.len(), 1);
        assert_eq!(layers[0].paint, SnakeOutlinePaint::Ordinary);
        assert_eq!(layers[0].color(ordinary_color), ordinary_color);

        let cell_size = 10.0;
        assert_eq!(layers[0].line_width(cell_size), 12.0);
        assert_eq!(layers[0].radius(cell_size), 6.0);
        assert_eq!(layers[0].radius(cell_size) - cell_size / 2.0, 1.0);
        assert_eq!(snake_mask_extra(false), 1.0);
    }

    /// The two roster sizes the stylesheet actually lays out. The snake must
    /// use the full row height for its thickness — that is what makes it read
    /// as the same object as the arena snake — and then fill as much of the
    /// width as whole cells allow, with the remainder split evenly.
    #[test]
    fn roster_layout_fills_the_row_and_centres_the_leftover_slack() {
        let desktop = roster_snake_layout(124.0, 19.0, false);
        assert_eq!(desktop.cells, 7);
        assert_eq!(desktop.cell_size, 17.0);
        assert_eq!(desktop.offset_x, 2.5);
        assert_eq!(desktop.offset_y, 1.0);
        // The ordinary contour is 1px per side, so it fits inside the slack.
        assert!(desktop.offset_y >= ORDINARY_OUTLINE_EXTRA / 2.0);
        assert!(desktop.offset_x >= ORDINARY_OUTLINE_EXTRA / 2.0);

        let mobile = roster_snake_layout(102.0, 17.0, false);
        assert_eq!(mobile.cells, 7);
        assert!((mobile.cell_size - 100.0 / 7.0).abs() < 1e-9);
        assert!(mobile.cell_size <= 17.0 - ORDINARY_OUTLINE_EXTRA);
        assert!((mobile.cell_size * mobile.cells as f64 - 100.0).abs() < 1e-9);
        assert!((mobile.offset_x - 1.0).abs() < 1e-9);

        // An active Boost band is three pixels per side, so it needs the room.
        let boosting = roster_snake_layout(124.0, 19.0, true);
        assert_eq!(boosting.cell_size, 13.0);
        assert_eq!(boosting.offset_y, 3.0);

        // Degenerate rows still produce a drawable snake rather than nothing.
        let cramped = roster_snake_layout(6.0, 3.0, false);
        assert!(cramped.cells >= 2);
        assert!(cramped.cell_size > 0.0);
        assert!(cramped.cell_size <= 1.0);
    }

    #[test]
    fn roster_body_is_a_straight_snake_with_its_head_on_the_facing_side() {
        let layout = roster_snake_layout(124.0, 19.0, false);

        // Head first, matching the engine's compressed body.
        assert_eq!(
            layout.body_cells(RosterFacing::Right),
            [(6.0, 0.0), (0.0, 0.0)]
        );
        assert_eq!(
            layout.body_cells(RosterFacing::Left),
            [(0.0, 0.0), (6.0, 0.0)]
        );

        // Whole cell coordinates: the shared skin painter walks the head
        // gradient one cell at a time.
        for (x, y) in layout.body_cells(RosterFacing::Right) {
            assert_eq!(x.fract(), 0.0);
            assert_eq!(y, 0.0);
        }
    }

    #[test]
    fn roster_label_anchors_to_the_head_and_clears_its_dark_core() {
        let layout = roster_snake_layout(124.0, 19.0, false);
        let cell = layout.cell_size;
        let core_radius = cell * HEAD_CORE_RADIUS_RATIO;

        let right = roster_label_layout(layout, RosterFacing::Right);
        let head_center_x = (layout.cells - 1) as f64 * cell + cell / 2.0;
        assert_eq!(right.align, "right");
        assert!(
            (head_center_x - right.x - core_radius - cell * ROSTER_LABEL_HEAD_GAP_RATIO).abs()
                < 1e-9,
            "the name must keep the authored gap beyond the head core"
        );
        assert!((right.y - (cell / 2.0 + ROSTER_LABEL_BASELINE_NUDGE)).abs() < 1e-9);
        assert_eq!(right.font_size, 8.5);

        // The name never runs past the tail's own cell.
        let tail_center_x = cell / 2.0;
        assert!(right.x - right.max_width > tail_center_x - cell / 2.0);
        assert!(right.max_width > 80.0);

        let left = roster_label_layout(layout, RosterFacing::Left);
        assert_eq!(left.align, "left");
        assert!(
            (left.x - cell / 2.0 - core_radius - cell * ROSTER_LABEL_HEAD_GAP_RATIO).abs() < 1e-9,
            "the mirrored name must keep the same head clearance"
        );

        // Both facings are exact mirrors of one another about the body span.
        let span = cell * layout.cells as f64;
        assert!((left.x - (span - right.x)).abs() < 1e-9);
        assert!((left.max_width - right.max_width).abs() < 1e-9);
        assert_eq!(left.font_size, right.font_size);
        assert_eq!(left.y, right.y);

        let left_head = layout.head_center(RosterFacing::Left);
        let right_head = layout.head_center(RosterFacing::Right);
        assert_eq!(left_head.1, right_head.1);
        assert!((left_head.0 - (span - right_head.0)).abs() < 1e-9);
    }

    #[test]
    fn roster_label_font_size_tracks_the_snake_thickness() {
        // Half the body thickness, so the name scales with the glyph instead of
        // being pinned to a hand-picked pixel size per breakpoint.
        let mobile = roster_snake_layout(86.0, 17.0, false);
        assert_eq!(
            roster_label_layout(mobile, RosterFacing::Left).font_size,
            mobile.cell_size * ROSTER_LABEL_SIZE_RATIO
        );
        // Very small rows clamp instead of vanishing.
        assert_eq!(
            roster_label_layout(roster_snake_layout(20.0, 6.0, false), RosterFacing::Left)
                .font_size,
            ROSTER_LABEL_MIN_SIZE
        );
    }

    #[test]
    fn roster_label_ink_clears_contrast_on_every_authored_snake_colour() {
        let contrast = |first: &str, second: &str| {
            let (a, b) = (relative_luminance(first), relative_luminance(second));
            (a.max(b) + 0.05) / (a.min(b) + 0.05)
        };

        for fill in ["#70bfe3", "#3c8dde", "#ff6b6b", "#e34e5b", "#f7b731"] {
            let ink = roster_label_ink(fill);
            assert_eq!(ink, ROSTER_LABEL_DARK_INK);
            assert!(contrast(ink, fill) >= 4.5, "{ink} must clear AA on {fill}");
        }

        // The dark field skin is the one that flips to light ink.
        assert_eq!(roster_label_ink("#556270"), ROSTER_LABEL_LIGHT_INK);
        assert_eq!(
            roster_label_shadow(ROSTER_LABEL_LIGHT_INK),
            "rgb(23 32 51 / 34%)"
        );
        assert_eq!(
            roster_label_shadow(ROSTER_LABEL_DARK_INK),
            "rgb(255 255 255 / 38%)"
        );
        // Malformed input must not panic.
        assert_eq!(roster_label_ink("not-a-colour"), ROSTER_LABEL_DARK_INK);
    }

    /// The roster resolves colours through `snake_palette`, so a roster snake
    /// is the same skin the arena paints for that player.
    #[test]
    fn roster_request_resolves_the_arena_palette_from_its_wire_shape() {
        let request: RosterSnakeRequest = serde_json::from_str(
            r#"{
                "snake_index": 2,
                "team_id": 0,
                "team_member_slot": 1,
                "snake_count": 4,
                "is_team_game": true,
                "local_snake_id": 0,
                "local_team_id": 0,
                "facing": "right",
                "name": "Troncat33"
            }"#,
        )
        .expect("the web client's roster request must parse");

        assert_eq!(request.facing, RosterFacing::Right);
        assert_eq!(request.name, "Troncat33");
        assert!(!request.boost_active);
        assert!(!request.is_ready);
        assert_eq!(request.font_family, None);
        assert_eq!(
            request.skin.colors(),
            snake_palette(2, Some(0), 1, 4, true, Some(0), Some(0))
        );

        let ready: RosterSnakeRequest = serde_json::from_str(
            r#"{
                "snake_index": 1,
                "team_id": 1,
                "snake_count": 2,
                "is_team_game": true,
                "facing": "left",
                "is_ready": true
            }"#,
        )
        .expect("ready roster request must parse");
        assert!(ready.is_ready);

        // Spectators (no local snake) still resolve a valid palette.
        let spectator: SnakeSkinInputs = serde_json::from_str(
            r#"{"snake_index":1,"team_id":1,"snake_count":2,"is_team_game":true}"#,
        )
        .unwrap();
        assert_eq!(
            spectator.colors(),
            snake_palette(1, Some(1), 0, 2, true, None, None)
        );
    }

    #[test]
    fn roster_names_truncate_on_character_boundaries() {
        let characters: Vec<char> = "A🐍BCDE".chars().collect();
        assert_eq!(roster_name_candidate(&characters, 4), "A🐍BC…");
        assert_eq!(roster_name_candidate(&characters, 0), "…");
        // Trailing space before the ellipsis is dropped.
        assert_eq!(
            roster_name_candidate(&"Tron cat".chars().collect::<Vec<_>>(), 5),
            "Tron…"
        );
    }

    #[test]
    fn pad_coordinates_follow_every_arena_rotation() {
        assert_eq!(transform_coords(2.0, 3.0, 10.0, 8.0, 0), (2.0, 3.0));
        assert_eq!(transform_coords(2.0, 3.0, 10.0, 8.0, 90), (4.0, 2.0));
        assert_eq!(transform_coords(2.0, 3.0, 10.0, 8.0, 180), (7.0, 4.0));
        assert_eq!(transform_coords(2.0, 3.0, 10.0, 8.0, 270), (3.0, 7.0));
    }

    #[test]
    fn full_packet_footprint_stays_two_by_two_through_rotation() {
        let pad = BoostPad {
            id: 0,
            position: common::Position { x: 2, y: 3 },
            charge_ms: 3_000,
            size_cells: 2,
            respawn_at_tick: None,
        };

        assert_eq!(
            transformed_pad_bounds(&pad, 10.0, 8.0, 0),
            Some((2.0, 3.0, 2.0, 2.0))
        );
        assert_eq!(
            transformed_pad_bounds(&pad, 10.0, 8.0, 90),
            Some((3.0, 2.0, 2.0, 2.0))
        );
        assert_eq!(
            transformed_pad_bounds(&pad, 10.0, 8.0, 180),
            Some((6.0, 3.0, 2.0, 2.0))
        );
        assert_eq!(
            transformed_pad_bounds(&pad, 10.0, 8.0, 270),
            Some((3.0, 6.0, 2.0, 2.0))
        );
    }

    #[test]
    fn pressure_plate_palette_uses_start_game_blue_and_nos_orange() {
        let colors: HashSet<_> = [
            NOS_INK,
            NOS_BLUE,
            NOS_BLUE_HIGHLIGHT,
            NOS_BLUE_SHADE,
            NOS_LABEL,
            NOS_STEEL_DARK,
            NOS_STEEL_LIGHT,
            NOS_ORANGE,
        ]
        .into_iter()
        .collect();

        assert_eq!(colors.len(), 8);
        assert_eq!(NOS_BLUE, "#3b82f6");
        assert_eq!(NOS_LABEL, "#f8fafc");
        assert_eq!(NOS_ORANGE, "#ff641e");
        assert!(!colors.contains("#f8c84a"));
    }

    #[test]
    fn pressure_plate_white_band_spans_the_body_and_orange_face_is_inset() {
        let body_width = 10.0;
        let body_height = 20.0;
        let (white_width, white_height, standard_width, standard_height) =
            nos_pressure_plate_dimensions(body_width, body_height, NosBottleSkin::Standard);
        let (_, _, full_width, full_height) =
            nos_pressure_plate_dimensions(body_width, body_height, NosBottleSkin::Full);

        assert_eq!(white_width, body_width);
        assert!(standard_width < full_width);
        assert!(standard_height < full_height);
        assert!(full_width < white_width);
        assert!(full_height < white_height);
        assert_eq!(NosBottleSkin::Standard.orange_ratios(), (0.46, 0.42));

        let full_center_y = NosBottleSkin::Full.plate_center_y(body_height);
        assert!(full_center_y - white_height / 2.0 >= body_height * -0.23);
        assert!(full_center_y + white_height / 2.0 <= body_height * 0.39);
    }

    #[test]
    fn pressure_plate_wordmark_uses_only_the_legible_full_pickup_skin() {
        for cell_size in 5..=15 {
            let (_, width, height) = regular_nos_dimensions(f64::from(cell_size));
            assert_eq!(
                nos_wordmark_size(width, height, NosBottleSkin::Standard),
                None
            );
        }

        for cell_size in 5..=8 {
            let (_, width, height) = full_nos_dimensions(f64::from(cell_size));
            assert_eq!(nos_wordmark_size(width, height, NosBottleSkin::Full), None);
        }

        let (_, width, height) = full_nos_dimensions(9.0);
        let size = nos_wordmark_size(width, height, NosBottleSkin::Full)
            .expect("the wide full-pickup label should fit its horizontal wordmark");
        let font = nos_wordmark_font(size);
        assert!(font.starts_with("900 "));
        assert!(!font.to_ascii_lowercase().contains("italic"));
    }

    #[test]
    fn regular_pressure_plate_grows_one_pixel_per_side_without_distortion() {
        for cell_size in 5..=15 {
            let cell_size = f64::from(cell_size);
            let (extent, width, height) = regular_nos_dimensions(cell_size);
            let base_extent = nos_visual_extent(cell_size);
            let base_width = (base_extent * NOS_REGULAR_WIDTH_RATIO).max(3.8);
            let base_height = base_extent * NOS_REGULAR_HEIGHT_RATIO;
            let base_rotated_bounds = (base_width + base_height) / std::f64::consts::SQRT_2;
            let rotated_bounds = (width + height) / std::f64::consts::SQRT_2;

            assert_eq!(extent, base_extent + NOS_REGULAR_GROWTH_PER_SIDE_PX * 2.0);
            assert!(
                (width / height - base_width / base_height).abs() < 1e-12,
                "bottle proportions changed at {cell_size}px"
            );
            assert!(
                (rotated_bounds - base_rotated_bounds - NOS_REGULAR_GROWTH_PER_SIDE_PX * 2.0).abs()
                    < 1e-10,
                "diagonal bottle did not grow one pixel per side at {cell_size}px"
            );
        }
    }

    #[test]
    fn full_pressure_plate_is_one_large_upright_canister_inside_its_optical_box() {
        for cell_size in 5..=15 {
            let cell_size = f64::from(cell_size);
            let footprint = cell_size * 2.0;
            let (extent, bottle_width, bottle_height) = full_nos_dimensions(cell_size);

            assert!(extent >= footprint + 1.0);
            assert!(extent <= footprint + 2.0);
            assert!(bottle_width >= 7.0);
            assert!(bottle_height >= 8.0);
            assert!(bottle_height > bottle_width);
            assert!(bottle_width / bottle_height > 0.75);
            assert!(bottle_width / bottle_height < 0.80);
            let outline = (bottle_width * 0.095).clamp(0.52, 1.1);
            assert!(
                bottle_width + outline * 2.0 < extent,
                "wide bottle overflows horizontally at {cell_size}px"
            );
            assert!(
                bottle_height * 1.08 < extent,
                "upright bottle overflows vertically at {cell_size}px"
            );
        }
    }

    #[test]
    fn active_boosts_mask_only_their_surrounding_grid_intersections() {
        let regular = (2.0, 3.0, 1.0, 1.0);
        let double = (6.0, 1.0, 2.0, 2.0);
        let bounds = [regular, double];

        for x in 2..=3 {
            for y in 3..=4 {
                assert!(grid_dot_is_covered_by_boost(
                    f64::from(x),
                    f64::from(y),
                    &bounds,
                ));
            }
        }
        for x in 6..=8 {
            for y in 1..=3 {
                assert!(grid_dot_is_covered_by_boost(
                    f64::from(x),
                    f64::from(y),
                    &bounds,
                ));
            }
        }

        assert!(!grid_dot_is_covered_by_boost(1.0, 3.0, &bounds));
        assert!(!grid_dot_is_covered_by_boost(4.0, 4.0, &bounds));
        assert!(!grid_dot_is_covered_by_boost(9.0, 2.0, &bounds));
    }

    #[test]
    fn cooling_boosts_do_not_mask_grid_dots() {
        let pads = [
            BoostPad {
                id: 0,
                position: common::Position { x: 2, y: 3 },
                charge_ms: 750,
                size_cells: 1,
                respawn_at_tick: None,
            },
            BoostPad {
                id: 1,
                position: common::Position { x: 6, y: 1 },
                charge_ms: 3_000,
                size_cells: 2,
                respawn_at_tick: Some(80),
            },
        ];
        let bounds = transformed_active_pad_bounds(&pads, 10.0, 8.0, 0);

        assert_eq!(bounds, vec![(2.0, 3.0, 1.0, 1.0)]);
        assert!(grid_dot_is_covered_by_boost(2.0, 3.0, &bounds));
        assert!(!grid_dot_is_covered_by_boost(6.0, 1.0, &bounds));
    }
}
