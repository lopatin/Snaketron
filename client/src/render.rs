use serde_json::Value;
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

/// Renders the game state to a canvas element
/// Takes a JSON string representation of the game state, the local user ID, rotation angle, and usernames
#[wasm_bindgen]
pub fn render_game(
    game_state_json: &str,
    canvas: web_sys::HtmlCanvasElement,
    cell_size: f64,
    local_user_id: Option<u32>,
    rotation: f64,
    local_username: Option<String>,
    opponent_username: Option<String>,
) -> Result<(), JsValue> {
    // Parse the JSON game state
    let game_state: Value = serde_json::from_str(game_state_json)
        .map_err(|e| JsValue::from_str(&format!("Failed to parse game state: {}", e)))?;

    let context = canvas
        .get_context("2d")
        .map_err(|_| JsValue::from_str("Failed to get 2d context"))?
        .ok_or_else(|| JsValue::from_str("2d context is null"))?;

    let ctx = context
        .dyn_into::<web_sys::CanvasRenderingContext2d>()
        .map_err(|_| JsValue::from_str("Failed to cast to 2d context"))?;

    // Extract arena dimensions
    let arena = &game_state["arena"];
    let game_width = arena["width"].as_u64().unwrap_or(10) as f64;
    let game_height = arena["height"].as_u64().unwrap_or(10) as f64;
    let rotation_int = rotation as i32;

    // Get effective dimensions for rendering (swapped for vertical orientations)
    let (width, height) = get_effective_dimensions(game_width, game_height, rotation_int);

    // Use a fixed dot radius of 1px to match the background dots
    let dot_radius = 1.0;

    // Get actual canvas dimensions
    let canvas_width = canvas.width() as f64;
    let canvas_height = canvas.height() as f64;

    // Clear entire canvas with white background (including padding area)
    ctx.set_fill_style(&JsValue::from_str("#ffffff"));
    ctx.fill_rect(0.0, 0.0, canvas_width, canvas_height);

    // Add 1px padding offset for all drawing operations
    let padding = 1.0;

    // Save the current state
    ctx.save();
    ctx.translate(padding, padding)?;

    // Fill the game area with white to ensure clean background
    ctx.set_fill_style(&JsValue::from_str("#ffffff"));
    ctx.fill_rect(
        0.0,
        0.0,
        canvas_width - 2.0 * padding,
        canvas_height - 2.0 * padding,
    );

    // Determine which snake belongs to the local player and their team (needed for perspective-based rendering)
    let (local_snake_id, local_player_team) = if let Some(user_id) = local_user_id {
        if let Some(players) = game_state["players"].as_object() {
            let snake_id = players
                .get(&user_id.to_string())
                .and_then(|player| player["snake_id"].as_u64())
                .map(|id| id as usize);

            // Get the team of the local player's snake
            let team = if let (Some(sid), Some(snakes)) = (snake_id, arena["snakes"].as_array()) {
                snakes.get(sid).and_then(|snake| snake["team_id"].as_u64())
            } else {
                None
            };

            (snake_id, team)
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };

    // Draw team zones if present
    let team_zone_config_data = arena["team_zone_config"].as_object().cloned();
    if let Some(team_zone_config) = &team_zone_config_data {
        let end_zone_depth = team_zone_config["end_zone_depth"].as_u64().unwrap_or(10) as f64;

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
                ctx.set_fill_style(&JsValue::from_str(left_color));
                ctx.fill_rect(0.0, 0.0, width * cell_size, end_zone_depth * cell_size);

                // Bottom zone
                ctx.set_fill_style(&JsValue::from_str(right_color));
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
                ctx.set_fill_style(&JsValue::from_str(left_color));
                ctx.fill_rect(
                    (width - end_zone_depth) * cell_size,
                    0.0,
                    end_zone_depth * cell_size,
                    height * cell_size,
                );

                // Left zone (was right)
                ctx.set_fill_style(&JsValue::from_str(right_color));
                ctx.fill_rect(0.0, 0.0, end_zone_depth * cell_size, height * cell_size);
            }
            270 => {
                // 270° CW: left zone becomes bottom, right zone becomes top
                // Bottom zone (was left)
                ctx.set_fill_style(&JsValue::from_str(left_color));
                ctx.fill_rect(
                    0.0,
                    (height - end_zone_depth) * cell_size,
                    width * cell_size,
                    end_zone_depth * cell_size,
                );

                // Top zone (was right)
                ctx.set_fill_style(&JsValue::from_str(right_color));
                ctx.fill_rect(0.0, 0.0, width * cell_size, end_zone_depth * cell_size);
            }
            _ => {
                // 0° or default: normal orientation
                // Left zone
                ctx.set_fill_style(&JsValue::from_str(left_color));
                ctx.fill_rect(0.0, 0.0, end_zone_depth * cell_size, height * cell_size);

                // Right zone
                ctx.set_fill_style(&JsValue::from_str(right_color));
                ctx.fill_rect(
                    (width - end_zone_depth) * cell_size,
                    0.0,
                    end_zone_depth * cell_size,
                    height * cell_size,
                );
            }
        }
    }

    // Draw dots at grid intersections (like the background pattern)
    ctx.set_fill_style(&JsValue::from_str("rgba(0, 0, 0, 0.3)")); // Same as background dots

    // Scale dot spacing with cell size to maintain consistent visual density
    let dot_spacing = cell_size;
    let dots_x = (width).ceil() as u32;
    let dots_y = (height).ceil() as u32;

    // Start from 1 and end at dots_x/y - 1 to skip outer edge dots
    for x in 1..dots_x {
        for y in 1..dots_y {
            let dot_x = x as f64 * dot_spacing;
            let dot_y = y as f64 * dot_spacing;

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
    if let Some(team_zone_config) = &team_zone_config_data {
        let end_zone_depth = team_zone_config["end_zone_depth"].as_u64().unwrap_or(10) as f64;

        // Build team labels from player usernames; show both teammates side by side
        let username_map = game_state["usernames"].as_object();
        let mut team_names: [Vec<String>; 2] = [Vec::new(), Vec::new()];
        if let (Some(players), Some(snakes)) = (
            game_state["players"].as_object(),
            arena["snakes"].as_array(),
        ) {
            for (user_id_str, player_val) in players {
                if let Some(snake_id) = player_val["snake_id"].as_u64() {
                    if let Some(snake) = snakes.get(snake_id as usize) {
                        if let Some(team_id) = snake["team_id"].as_u64() {
                            if (team_id as usize) < 2 {
                                let username = username_map
                                    .and_then(|map| map.get(user_id_str))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or_else(|| user_id_str.as_str());
                                team_names[team_id as usize].push(username.to_string());
                            }
                        }
                    }
                }
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

        let local_name = local_username
            .as_ref()
            .map(|s| s.to_uppercase())
            .unwrap_or_else(|| "USER 0".to_string());
        let opponent_name = opponent_username
            .as_ref()
            .map(|s| s.to_uppercase())
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

        let mut format_names = |names: &[String], fallback: &str| -> Vec<String> {
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
            ctx.set_stroke_style(&JsValue::from_str(bg_color));
            ctx.stroke_text(text, center_x, center_y)?;
            ctx.set_fill_style(&JsValue::from_str(text_color));
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
            let mut needed_size =
                compute_font_size(names.get(0).map(|s| s.as_str()).unwrap_or(""), box_w, box_h);
            if split_labels && names.len() > 1 {
                if let Some(name) = names.get(1) {
                    needed_size = needed_size.min(compute_font_size(name, box_w, box_h));
                }
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
    if let Some(food_array) = arena["food"].as_array() {
        // First pass: Draw white squares to erase grid dots
        ctx.set_fill_style(&JsValue::from_str("#ffffff"));
        for food in food_array {
            if let (Some(x), Some(y)) = (food["x"].as_i64(), food["y"].as_i64()) {
                let (tx, ty) =
                    transform_coords(x as f64, y as f64, game_width, game_height, rotation_int);
                let cell_x = tx * cell_size;
                let cell_y = ty * cell_size;
                // Draw white rectangle 1px larger than the cell to erase dots
                ctx.fill_rect(cell_x - 1.0, cell_y - 1.0, cell_size + 2.0, cell_size + 2.0);
            }
        }

        // Second pass: Draw the actual food
        for food in food_array {
            if let (Some(x), Some(y)) = (food["x"].as_i64(), food["y"].as_i64()) {
                let (tx, ty) =
                    transform_coords(x as f64, y as f64, game_width, game_height, rotation_int);
                let cell_x = tx * cell_size;
                let cell_y = ty * cell_size;
                let center_x = cell_x + cell_size / 2.0;
                let center_y = cell_y + cell_size / 2.0;
                let radius = cell_size / 2.0;

                // Draw darker border
                ctx.set_fill_style(&JsValue::from_str("#5e8a5e"));
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
                ctx.set_fill_style(&JsValue::from_str("#85b885"));
                ctx.begin_path();
                ctx.arc(center_x, center_y, radius, 0.0, 2.0 * std::f64::consts::PI)?;
                ctx.fill();

                // Draw single light reflection in top-left
                ctx.set_fill_style(&JsValue::from_str("#a0c8a0"));
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
        }
    }

    // Draw snakes (both alive and dead)
    if let Some(snakes) = arena["snakes"].as_array() {
        for (index, snake) in snakes.iter().enumerate() {
            let is_alive = snake["is_alive"].as_bool().unwrap_or(false);

            if is_alive {
                // Choose snake color based on perspective in team games
                let (color, border_color) = if team_zone_config_data.is_some() {
                    // Team game: use perspective-based coloring
                    if Some(index) == local_snake_id {
                        // Local player is always blue
                        ("#70bfe3", "#5299bb")
                    } else {
                        // Opponent is always red (in 2-team games)
                        ("#ff6b6b", "#b84444")
                    }
                } else {
                    // Non-team game: use existing perspective-based logic
                    if Some(index) == local_snake_id {
                        // Local player is always blue
                        ("#70bfe3", "#5299bb")
                    } else if snakes.len() == 2 {
                        // In 2-player games, opponent is always red
                        ("#ff6b6b", "#b84444")
                    } else {
                        // Multi-player: use different colors for other players
                        match index % 4 {
                            0 if local_snake_id.is_none() => ("#70bfe3", "#5299bb"), // Blue if no local player
                            1 => ("#ff6b6b", "#b84444"),                             // Red
                            2 => ("#556270", "#353c47"),                             // Gray
                            _ => ("#f7b731", "#a87d1f"),                             // Yellow
                        }
                    }
                };

                ctx.set_fill_style(&JsValue::from_str(color));

                // Draw snake body
                if let Some(body) = snake["body"].as_array() {
                    if body.is_empty() {
                        continue;
                    }

                    // Handle single-segment snake (just a head)
                    if body.len() == 1 {
                        if let Some(head) = body.first() {
                            if let (Some(x), Some(y)) = (head["x"].as_i64(), head["y"].as_i64()) {
                                let (tx, ty) = transform_coords(
                                    x as f64,
                                    y as f64,
                                    game_width,
                                    game_height,
                                    rotation_int,
                                );
                                let center_x = tx * cell_size + cell_size / 2.0;
                                let center_y = ty * cell_size + cell_size / 2.0;

                                // Draw border
                                ctx.set_fill_style(&JsValue::from_str(border_color));
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
                                ctx.set_fill_style(&JsValue::from_str(color));
                                ctx.begin_path();
                                ctx.arc(
                                    center_x,
                                    center_y,
                                    cell_size / 2.0,
                                    0.0,
                                    2.0 * std::f64::consts::PI,
                                )?;
                                ctx.fill();

                                // Draw inner circle
                                ctx.set_fill_style(&JsValue::from_str("#333"));
                                ctx.begin_path();
                                ctx.arc(
                                    center_x,
                                    center_y,
                                    cell_size * 0.38,
                                    0.0,
                                    2.0 * std::f64::consts::PI,
                                )?;
                                ctx.fill();
                                ctx.set_fill_style(&JsValue::from_str(color));
                            }
                        }
                        continue;
                    }

                    // First pass: Fill with white rectangles to cover grid dots
                    ctx.set_fill_style(&JsValue::from_str("#ffffff"));

                    // Fill white rectangles for body segments (expanded by 1px)
                    for window in body.windows(2) {
                        if let (Some(p1), Some(p2)) = (window.get(0), window.get(1)) {
                            let x1 = p1["x"].as_i64().unwrap_or(0) as f64;
                            let y1 = p1["y"].as_i64().unwrap_or(0) as f64;
                            let x2 = p2["x"].as_i64().unwrap_or(0) as f64;
                            let y2 = p2["y"].as_i64().unwrap_or(0) as f64;

                            // Transform both points
                            let (tx1, ty1) =
                                transform_coords(x1, y1, game_width, game_height, rotation_int);
                            let (tx2, ty2) =
                                transform_coords(x2, y2, game_width, game_height, rotation_int);

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
                    }

                    // Fill white rectangles for all body points (expanded by 1px)
                    for point in body.iter() {
                        if let (Some(x), Some(y)) = (point["x"].as_i64(), point["y"].as_i64()) {
                            let (tx, ty) = transform_coords(
                                x as f64,
                                y as f64,
                                game_width,
                                game_height,
                                rotation_int,
                            );
                            let rect_x = tx * cell_size - 1.0;
                            let rect_y = ty * cell_size - 1.0;
                            ctx.fill_rect(rect_x, rect_y, cell_size + 2.0, cell_size + 2.0);
                        }
                    }

                    // Second pass: Draw borders (1px larger)
                    ctx.set_stroke_style(&JsValue::from_str(border_color));

                    // Draw border for body segments
                    for window in body.windows(2) {
                        if let (Some(p1), Some(p2)) = (window.get(0), window.get(1)) {
                            let x1 = p1["x"].as_i64().unwrap_or(0) as f64;
                            let y1 = p1["y"].as_i64().unwrap_or(0) as f64;
                            let x2 = p2["x"].as_i64().unwrap_or(0) as f64;
                            let y2 = p2["y"].as_i64().unwrap_or(0) as f64;

                            // Transform both points
                            let (tx1, ty1) =
                                transform_coords(x1, y1, game_width, game_height, rotation_int);
                            let (tx2, ty2) =
                                transform_coords(x2, y2, game_width, game_height, rotation_int);

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
                    }

                    // Draw border for corner joints
                    ctx.set_fill_style(&JsValue::from_str(border_color));
                    for i in 1..body.len() - 1 {
                        if let Some(point) = body.get(i) {
                            if let (Some(x), Some(y)) = (point["x"].as_i64(), point["y"].as_i64()) {
                                let (tx, ty) = transform_coords(
                                    x as f64,
                                    y as f64,
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
                        }
                    }

                    // Third pass: Draw the actual snake
                    ctx.set_stroke_style(&JsValue::from_str(color));
                    ctx.set_fill_style(&JsValue::from_str(color));

                    // Draw main body segments
                    for window in body.windows(2) {
                        if let (Some(p1), Some(p2)) = (window.get(0), window.get(1)) {
                            let x1 = p1["x"].as_i64().unwrap_or(0) as f64;
                            let y1 = p1["y"].as_i64().unwrap_or(0) as f64;
                            let x2 = p2["x"].as_i64().unwrap_or(0) as f64;
                            let y2 = p2["y"].as_i64().unwrap_or(0) as f64;

                            // Transform both points
                            let (tx1, ty1) =
                                transform_coords(x1, y1, game_width, game_height, rotation_int);
                            let (tx2, ty2) =
                                transform_coords(x2, y2, game_width, game_height, rotation_int);

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
                    }

                    // Draw corner joints as circles to create smooth turns
                    for i in 1..body.len() - 1 {
                        if let Some(point) = body.get(i) {
                            if let (Some(x), Some(y)) = (point["x"].as_i64(), point["y"].as_i64()) {
                                let (tx, ty) = transform_coords(
                                    x as f64,
                                    y as f64,
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
                        }
                    }

                    // Get head and tail information
                    let head = body.first().unwrap();
                    let head_x = head["x"].as_i64().unwrap_or(0) as f64;
                    let head_y = head["y"].as_i64().unwrap_or(0) as f64;
                    let (head_tx, head_ty) =
                        transform_coords(head_x, head_y, game_width, game_height, rotation_int);
                    let head_center_x = head_tx * cell_size + cell_size / 2.0;
                    let head_center_y = head_ty * cell_size + cell_size / 2.0;

                    let tail = body.last().unwrap();
                    let tail_x = tail["x"].as_i64().unwrap_or(0) as f64;
                    let tail_y = tail["y"].as_i64().unwrap_or(0) as f64;
                    let (tail_tx, tail_ty) =
                        transform_coords(tail_x, tail_y, game_width, game_height, rotation_int);
                    let tail_center_x = tail_tx * cell_size + cell_size / 2.0;
                    let tail_center_y = tail_ty * cell_size + cell_size / 2.0;

                    // Draw actual tail and head (no separate border circles needed)
                    // The round line caps already provide the border
                    ctx.set_fill_style(&JsValue::from_str(color));

                    // Draw tail as full circle
                    ctx.begin_path();
                    ctx.arc(
                        tail_center_x,
                        tail_center_y,
                        cell_size / 2.0,
                        0.0,
                        2.0 * std::f64::consts::PI,
                    )?;
                    ctx.fill();

                    // Fourth pass: Add white overlay gradient for first 10 cells from head
                    // Calculate cumulative distances from head
                    let mut cumulative_distance = 0.0;
                    let mut segment_distances = Vec::new();

                    for window in body.windows(2) {
                        if let (Some(p1), Some(p2)) = (window.get(0), window.get(1)) {
                            let x1 = p1["x"].as_i64().unwrap_or(0) as f64;
                            let y1 = p1["y"].as_i64().unwrap_or(0) as f64;
                            let x2 = p2["x"].as_i64().unwrap_or(0) as f64;
                            let y2 = p2["y"].as_i64().unwrap_or(0) as f64;

                            let segment_length = ((x2 - x1).abs() + (y2 - y1).abs()) as f64;
                            segment_distances.push((cumulative_distance, segment_length));
                            cumulative_distance += segment_length;
                        }
                    }

                    // Draw white overlay on segments within 10 cells of head
                    // First, collect all cells with their distances
                    let mut cells_with_distance = Vec::new();
                    let mut current_distance = 0.0;
                    let mut seen_cells = HashSet::new();

                    for (seg_idx, window) in body.windows(2).enumerate() {
                        if let (Some(p1), Some(p2)) = (window.get(0), window.get(1)) {
                            let x1 = p1["x"].as_i64().unwrap_or(0);
                            let y1 = p1["y"].as_i64().unwrap_or(0);
                            let x2 = p2["x"].as_i64().unwrap_or(0);
                            let y2 = p2["y"].as_i64().unwrap_or(0);

                            // Process each cell in the segment, respecting direction
                            if x1 == x2 {
                                // Vertical segment
                                let x = x1;
                                let step = if y2 > y1 { 1 } else { -1 };
                                let mut y = y1;

                                loop {
                                    let cell_key = format!("{},{}", x, y);

                                    // Skip the first cell of non-first segments (it's a corner already processed)
                                    if !(seg_idx > 0 && y == y1) && !seen_cells.contains(&cell_key)
                                    {
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
                                    if !(seg_idx > 0 && x == x1) && !seen_cells.contains(&cell_key)
                                    {
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
                    }

                    // Now draw all collected cells with their proper distances
                    for (x, y, distance) in cells_with_distance {
                        let opacity = (1.0 - distance / 10.0) * 0.3;
                        ctx.set_fill_style(&JsValue::from_str(&format!(
                            "rgba(255, 255, 255, {})",
                            opacity
                        )));

                        let (tx, ty) = transform_coords(
                            x as f64,
                            y as f64,
                            game_width,
                            game_height,
                            rotation_int,
                        );
                        ctx.fill_rect(tx * cell_size, ty * cell_size, cell_size, cell_size);
                    }

                    // Draw head as full circle (after overlay for proper layering)
                    ctx.set_fill_style(&JsValue::from_str(color));
                    ctx.begin_path();
                    ctx.arc(
                        head_center_x,
                        head_center_y,
                        cell_size / 2.0,
                        0.0,
                        2.0 * std::f64::consts::PI,
                    )?;
                    ctx.fill();

                    // Draw white overlay on head (strongest opacity)
                    ctx.set_fill_style(&JsValue::from_str("rgba(255, 255, 255, 0.3)"));
                    ctx.begin_path();
                    ctx.arc(
                        head_center_x,
                        head_center_y,
                        cell_size / 2.0,
                        0.0,
                        2.0 * std::f64::consts::PI,
                    )?;
                    ctx.fill();

                    // Draw smaller inner circle in head with different color
                    ctx.set_fill_style(&JsValue::from_str("#333"));
                    ctx.begin_path();
                    ctx.arc(
                        head_center_x,
                        head_center_y,
                        cell_size * 0.38,
                        0.0,
                        2.0 * std::f64::consts::PI,
                    )?;
                    ctx.fill();
                }
            } else {
                // Render dead snake with faint solid color
                let color = "#f0f0f0"; // Light gray for dead snakes
                let border_color = "#d0d0d0"; // Slightly darker border

                ctx.set_fill_style(&JsValue::from_str(color));

                // Draw snake body
                if let Some(body) = snake["body"].as_array() {
                    if body.is_empty() {
                        continue;
                    }

                    // Handle single-segment snake (just a head)
                    if body.len() == 1 {
                        if let Some(head) = body.first() {
                            if let (Some(x), Some(y)) = (head["x"].as_i64(), head["y"].as_i64()) {
                                let (tx, ty) = transform_coords(
                                    x as f64,
                                    y as f64,
                                    game_width,
                                    game_height,
                                    rotation_int,
                                );
                                let center_x = tx * cell_size + cell_size / 2.0;
                                let center_y = ty * cell_size + cell_size / 2.0;

                                // Draw border
                                ctx.set_fill_style(&JsValue::from_str(border_color));
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
                                ctx.set_fill_style(&JsValue::from_str(color));
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
                                ctx.set_stroke_style(&JsValue::from_str("#666"));
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
                            }
                        }
                        continue;
                    }

                    // First pass: Fill with white rectangles to cover grid dots
                    ctx.set_fill_style(&JsValue::from_str("#ffffff"));

                    // Fill white rectangles for body segments (expanded by 1px)
                    for window in body.windows(2) {
                        if let (Some(p1), Some(p2)) = (window.get(0), window.get(1)) {
                            let x1 = p1["x"].as_i64().unwrap_or(0) as f64;
                            let y1 = p1["y"].as_i64().unwrap_or(0) as f64;
                            let x2 = p2["x"].as_i64().unwrap_or(0) as f64;
                            let y2 = p2["y"].as_i64().unwrap_or(0) as f64;

                            // Transform both points
                            let (tx1, ty1) =
                                transform_coords(x1, y1, game_width, game_height, rotation_int);
                            let (tx2, ty2) =
                                transform_coords(x2, y2, game_width, game_height, rotation_int);

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
                    }

                    // Fill white rectangles for all body points (expanded by 1px)
                    for point in body.iter() {
                        if let (Some(x), Some(y)) = (point["x"].as_i64(), point["y"].as_i64()) {
                            let (tx, ty) = transform_coords(
                                x as f64,
                                y as f64,
                                game_width,
                                game_height,
                                rotation_int,
                            );
                            let rect_x = tx * cell_size - 1.0;
                            let rect_y = ty * cell_size - 1.0;
                            ctx.fill_rect(rect_x, rect_y, cell_size + 2.0, cell_size + 2.0);
                        }
                    }

                    // Second pass: Draw borders (1px larger)
                    ctx.set_stroke_style(&JsValue::from_str(border_color));

                    // Draw border for body segments
                    for window in body.windows(2) {
                        if let (Some(p1), Some(p2)) = (window.get(0), window.get(1)) {
                            let x1 = p1["x"].as_i64().unwrap_or(0) as f64;
                            let y1 = p1["y"].as_i64().unwrap_or(0) as f64;
                            let x2 = p2["x"].as_i64().unwrap_or(0) as f64;
                            let y2 = p2["y"].as_i64().unwrap_or(0) as f64;

                            // Transform both points
                            let (tx1, ty1) =
                                transform_coords(x1, y1, game_width, game_height, rotation_int);
                            let (tx2, ty2) =
                                transform_coords(x2, y2, game_width, game_height, rotation_int);

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
                    }

                    // Draw border for corner joints
                    ctx.set_fill_style(&JsValue::from_str(border_color));
                    for i in 1..body.len() - 1 {
                        if let Some(point) = body.get(i) {
                            if let (Some(x), Some(y)) = (point["x"].as_i64(), point["y"].as_i64()) {
                                let (tx, ty) = transform_coords(
                                    x as f64,
                                    y as f64,
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
                        }
                    }

                    // Third pass: Draw the actual snake
                    ctx.set_stroke_style(&JsValue::from_str(color));
                    ctx.set_fill_style(&JsValue::from_str(color));

                    // Draw main body segments
                    for window in body.windows(2) {
                        if let (Some(p1), Some(p2)) = (window.get(0), window.get(1)) {
                            let x1 = p1["x"].as_i64().unwrap_or(0) as f64;
                            let y1 = p1["y"].as_i64().unwrap_or(0) as f64;
                            let x2 = p2["x"].as_i64().unwrap_or(0) as f64;
                            let y2 = p2["y"].as_i64().unwrap_or(0) as f64;

                            // Transform both points
                            let (tx1, ty1) =
                                transform_coords(x1, y1, game_width, game_height, rotation_int);
                            let (tx2, ty2) =
                                transform_coords(x2, y2, game_width, game_height, rotation_int);

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
                    }

                    // Draw corner joints as circles to create smooth turns
                    for i in 1..body.len() - 1 {
                        if let Some(point) = body.get(i) {
                            if let (Some(x), Some(y)) = (point["x"].as_i64(), point["y"].as_i64()) {
                                let (tx, ty) = transform_coords(
                                    x as f64,
                                    y as f64,
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
                        }
                    }

                    // Get head and tail information
                    let head = body.first().unwrap();
                    let head_x = head["x"].as_i64().unwrap_or(0) as f64;
                    let head_y = head["y"].as_i64().unwrap_or(0) as f64;
                    let (head_tx, head_ty) =
                        transform_coords(head_x, head_y, game_width, game_height, rotation_int);
                    let head_center_x = head_tx * cell_size + cell_size / 2.0;
                    let head_center_y = head_ty * cell_size + cell_size / 2.0;

                    let tail = body.last().unwrap();
                    let tail_x = tail["x"].as_i64().unwrap_or(0) as f64;
                    let tail_y = tail["y"].as_i64().unwrap_or(0) as f64;
                    let (tail_tx, tail_ty) =
                        transform_coords(tail_x, tail_y, game_width, game_height, rotation_int);
                    let tail_center_x = tail_tx * cell_size + cell_size / 2.0;
                    let tail_center_y = tail_ty * cell_size + cell_size / 2.0;

                    // Draw tail as full circle
                    ctx.set_fill_style(&JsValue::from_str(color));
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
                    ctx.set_stroke_style(&JsValue::from_str("#666"));
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
        }
    }

    // Draw walls AFTER snakes so dead snakes appear behind walls
    if let Some(team_zone_config) = &team_zone_config_data {
        let end_zone_depth = team_zone_config["end_zone_depth"].as_u64().unwrap_or(10) as f64;
        let goal_width = team_zone_config["goal_width"].as_u64().unwrap_or(5) as f64;

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
                ctx.set_fill_style(&JsValue::from_str(left_wall_color));
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
                ctx.set_fill_style(&JsValue::from_str(right_wall_color));
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
                ctx.set_fill_style(&JsValue::from_str(left_wall_color));
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
                ctx.set_fill_style(&JsValue::from_str(right_wall_color));
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
                ctx.set_fill_style(&JsValue::from_str(left_wall_color));
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
                ctx.set_fill_style(&JsValue::from_str(right_wall_color));
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
                ctx.set_fill_style(&JsValue::from_str(left_wall_color));
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
                ctx.set_fill_style(&JsValue::from_str(right_wall_color));
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

    // Draw game info
    ctx.set_fill_style(&JsValue::from_str("#333"));
    ctx.set_font("16px monospace");
    if let Some(tick) = game_state["tick"].as_u64() {
        ctx.fill_text(&format!("Tick: {}", tick), 10.0, canvas_height + 20.0)?;
    }

    // Restore the canvas state (remove padding translation)
    ctx.restore();

    Ok(())
}
