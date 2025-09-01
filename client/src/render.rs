use wasm_bindgen::prelude::*;
use serde_json::Value;
use std::collections::HashSet;

/// Renders the game state to a canvas element
/// Takes a JSON string representation of the game state and the local user ID
#[wasm_bindgen]
pub fn render_game(game_state_json: &str, canvas: web_sys::HtmlCanvasElement, cell_size: f64, local_user_id: Option<u32>) -> Result<(), JsValue> {
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
    let width = arena["width"].as_u64().unwrap_or(10) as u32;
    let height = arena["height"].as_u64().unwrap_or(10) as u32;

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
    
    // Save the current state and translate for padding
    ctx.save();
    ctx.translate(padding, padding)?;
    
    // Fill the game area with white again to ensure clean background
    ctx.set_fill_style(&JsValue::from_str("#ffffff"));
    ctx.fill_rect(0.0, 0.0, canvas_width - 2.0 * padding, canvas_height - 2.0 * padding);
    
    // Determine which snake belongs to the local player and their team (needed for perspective-based rendering)
    let (local_snake_id, local_player_team) = if let Some(user_id) = local_user_id {
        if let Some(players) = game_state["players"].as_object() {
            let snake_id = players.get(&user_id.to_string())
                .and_then(|player| player["snake_id"].as_u64())
                .map(|id| id as usize);
            
            // Get the team of the local player's snake
            let team = if let (Some(sid), Some(snakes)) = (snake_id, arena["snakes"].as_array()) {
                snakes.get(sid)
                    .and_then(|snake| snake["team_id"].as_u64())
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
        
        // Determine colors based on local player's team
        let (left_color, right_color, left_label, right_label) = match local_player_team {
            Some(0) => {
                // Local player is Team 0 (left) - they see blue on left, red on right
                ("#e6f4fa", "#ffe6e6", "YOUR GOAL", "ENEMY GOAL")
            },
            Some(1) => {
                // Local player is Team 1 (right) - they see red on left, blue on right
                ("#ffe6e6", "#e6f4fa", "ENEMY GOAL", "YOUR GOAL")
            },
            _ => {
                // No local player or unknown team - use default
                ("#e6f4fa", "#ffe6e6", "TEAM 0", "TEAM 1")
            }
        };
        
        // Draw left end zone
        ctx.set_fill_style(&JsValue::from_str(left_color));
        ctx.fill_rect(0.0, 0.0, end_zone_depth * cell_size, height as f64 * cell_size);
        
        // Draw right end zone
        ctx.set_fill_style(&JsValue::from_str(right_color));
        ctx.fill_rect(
            (width as f64 - end_zone_depth) * cell_size, 
            0.0, 
            end_zone_depth * cell_size, 
            height as f64 * cell_size
        );
        
        // Draw team names in end zones
        ctx.set_fill_style(&JsValue::from_str("#ffffff"));
        ctx.set_font(&format!("{}px bold italic sans-serif", cell_size * 2.0));
        ctx.set_text_align("center");
        ctx.set_text_baseline("middle");
        
        // Left zone text
        ctx.fill_text(
            left_label,
            end_zone_depth * cell_size / 2.0,
            height as f64 * cell_size / 2.0
        )?;
        
        // Right zone text
        ctx.fill_text(
            right_label,
            (width as f64 - end_zone_depth / 2.0) * cell_size,
            height as f64 * cell_size / 2.0
        )?;
    }
    
    // Draw dots at grid intersections (like the background pattern)
    ctx.set_fill_style(&JsValue::from_str("rgba(0, 0, 0, 0.3)")); // Same as background dots
    
    // Scale dot spacing with cell size to maintain consistent visual density
    let dot_spacing = cell_size;
    let dots_x = ((canvas_width - 2.0 * padding) / dot_spacing).ceil() as u32;
    let dots_y = ((canvas_height - 2.0 * padding) / dot_spacing).ceil() as u32;
    
    // Start from 1 and end at dots_x/y - 1 to skip outer edge dots
    for x in 1..dots_x {
        for y in 1..dots_y {
            let dot_x = x as f64 * dot_spacing;
            let dot_y = y as f64 * dot_spacing;
            
            // Skip dots that are on or outside the canvas edges (accounting for padding)
            if dot_x >= canvas_width - 2.0 * padding || dot_y >= canvas_height - 2.0 * padding {
                continue;
            }
            
            // Draw a small circle dot
            ctx.begin_path();
            ctx.arc(dot_x, dot_y, dot_radius, 0.0, 2.0 * std::f64::consts::PI)?;
            ctx.fill();
        }
    }
    
    // Draw walls after dots so they cover the dots
    if let Some(team_zone_config) = &team_zone_config_data {
        let end_zone_depth = team_zone_config["end_zone_depth"].as_u64().unwrap_or(10) as f64;
        let goal_width = team_zone_config["goal_width"].as_u64().unwrap_or(5) as f64;
        
        // Draw walls as 3px solid rectangles between field and endzone cells
        let wall_thickness = 3.0;
        
        let goal_center = height as f64 / 2.0;
        let goal_half_width = goal_width / 2.0;
        let goal_y_start = goal_center - goal_half_width;
        let goal_y_end = goal_center + goal_half_width;
        
        // Round goal boundaries to nearest cell edges
        let goal_y_start_aligned = goal_y_start.floor();
        let goal_y_end_aligned = goal_y_end.ceil();
        
        // Determine wall colors based on local player's team
        let (left_wall_color, right_wall_color) = match local_player_team {
            Some(0) => ("#7aa8c1", "#c18888"),  // Local is Team 0: blue left, red right
            Some(1) => ("#c18888", "#7aa8c1"),  // Local is Team 1: red left, blue right
            _ => ("#7aa8c1", "#c18888"),        // Default: blue left, red right
        };
        
        // Left boundary wall (between endzone and field)
        ctx.set_fill_style(&JsValue::from_str(left_wall_color));
        
        let team_a_wall_x = end_zone_depth * cell_size - wall_thickness / 2.0;
        
        // Top wall segment (before goal)
        if goal_y_start_aligned > 0.0 {
            ctx.fill_rect(
                team_a_wall_x,
                0.0,
                wall_thickness,
                goal_y_start_aligned * cell_size
            );
        }
        
        // Bottom wall segment (after goal)
        if goal_y_end_aligned < height as f64 {
            ctx.fill_rect(
                team_a_wall_x,
                goal_y_end_aligned * cell_size,
                wall_thickness,
                (height as f64 - goal_y_end_aligned) * cell_size
            );
        }
        
        // Right boundary wall (between field and endzone)
        ctx.set_fill_style(&JsValue::from_str(right_wall_color));
        
        let team_b_wall_x = (width as f64 - end_zone_depth) * cell_size - wall_thickness / 2.0;
        
        // Top wall segment (before goal)
        if goal_y_start_aligned > 0.0 {
            ctx.fill_rect(
                team_b_wall_x,
                0.0,
                wall_thickness,
                goal_y_start_aligned * cell_size
            );
        }
        
        // Bottom wall segment (after goal)
        if goal_y_end_aligned < height as f64 {
            ctx.fill_rect(
                team_b_wall_x,
                goal_y_end_aligned * cell_size,
                wall_thickness,
                (height as f64 - goal_y_end_aligned) * cell_size
            );
        }
    }

    // Draw food
    if let Some(food_array) = arena["food"].as_array() {
        // First pass: Draw white squares to erase grid dots
        ctx.set_fill_style(&JsValue::from_str("#ffffff"));
        for food in food_array {
            if let (Some(x), Some(y)) = (food["x"].as_i64(), food["y"].as_i64()) {
                let cell_x = x as f64 * cell_size;
                let cell_y = y as f64 * cell_size;
                // Draw white rectangle 1px larger than the cell to erase dots
                ctx.fill_rect(cell_x - 1.0, cell_y - 1.0, cell_size + 2.0, cell_size + 2.0);
            }
        }
        
        // Second pass: Draw the actual food
        for food in food_array {
            if let (Some(x), Some(y)) = (food["x"].as_i64(), food["y"].as_i64()) {
                let cell_x = x as f64 * cell_size;
                let cell_y = y as f64 * cell_size;
                let center_x = cell_x + cell_size / 2.0;
                let center_y = cell_y + cell_size / 2.0;
                let radius = cell_size / 2.0;
                
                // Draw darker border
                ctx.set_fill_style(&JsValue::from_str("#5e8a5e"));
                ctx.begin_path();
                ctx.arc(center_x, center_y, radius + 1.0, 0.0, 2.0 * std::f64::consts::PI)?;
                ctx.fill();
                
                // Draw food base
                ctx.set_fill_style(&JsValue::from_str("#85b885"));
                ctx.begin_path();
                ctx.arc(center_x, center_y, radius, 0.0, 2.0 * std::f64::consts::PI)?;
                ctx.fill();
                
                // Draw single light reflection in top-left
                ctx.set_fill_style(&JsValue::from_str("#a0c8a0"));
                ctx.begin_path();
                ctx.arc(center_x - radius * 0.35, center_y - radius * 0.35, radius * 0.25, 0.0, 2.0 * std::f64::consts::PI)?;
                ctx.fill();
            }
        }
    }

    // Draw snakes
    if let Some(snakes) = arena["snakes"].as_array() {
        for (index, snake) in snakes.iter().enumerate() {
            if snake["is_alive"].as_bool().unwrap_or(false) {
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
                            0 if local_snake_id.is_none() => ("#70bfe3", "#5299bb"),  // Blue if no local player
                            1 => ("#ff6b6b", "#b84444"),  // Red
                            2 => ("#556270", "#353c47"),  // Gray
                            _ => ("#f7b731", "#a87d1f"),  // Yellow
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
                                let center_x = x as f64 * cell_size + cell_size / 2.0;
                                let center_y = y as f64 * cell_size + cell_size / 2.0;
                                
                                // Draw border
                                ctx.set_fill_style(&JsValue::from_str(border_color));
                                ctx.begin_path();
                                ctx.arc(center_x, center_y, cell_size / 2.0 + 1.0, 0.0, 2.0 * std::f64::consts::PI)?;
                                ctx.fill();
                                
                                // Draw as a full circle
                                ctx.set_fill_style(&JsValue::from_str(color));
                                ctx.begin_path();
                                ctx.arc(center_x, center_y, cell_size / 2.0, 0.0, 2.0 * std::f64::consts::PI)?;
                                ctx.fill();
                                
                                // Draw inner circle
                                ctx.set_fill_style(&JsValue::from_str("#333"));
                                ctx.begin_path();
                                ctx.arc(center_x, center_y, cell_size * 0.38, 0.0, 2.0 * std::f64::consts::PI)?;
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

                            if x1 == x2 {
                                // Vertical segment - draw rectangle
                                let x = x1 * cell_size;
                                let min_y = y1.min(y2) * cell_size;
                                let max_y = y1.max(y2) * cell_size;
                                ctx.fill_rect(x - 1.0, min_y - 1.0, cell_size + 2.0, (max_y - min_y) + cell_size + 2.0);
                            } else if y1 == y2 {
                                // Horizontal segment - draw rectangle
                                let y = y1 * cell_size;
                                let min_x = x1.min(x2) * cell_size;
                                let max_x = x1.max(x2) * cell_size;
                                ctx.fill_rect(min_x - 1.0, y - 1.0, (max_x - min_x) + cell_size + 2.0, cell_size + 2.0);
                            }
                        }
                    }

                    // Fill white rectangles for all body points (expanded by 1px)
                    for point in body.iter() {
                        if let (Some(x), Some(y)) = (point["x"].as_i64(), point["y"].as_i64()) {
                            let rect_x = x as f64 * cell_size - 1.0;
                            let rect_y = y as f64 * cell_size - 1.0;
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

                            if x1 == x2 {
                                // Vertical segment
                                let x = x1 * cell_size + cell_size / 2.0;
                                let min_y = y1.min(y2) * cell_size + cell_size / 2.0;
                                let max_y = y1.max(y2) * cell_size + cell_size / 2.0;
                                
                                ctx.set_line_width(cell_size + 2.0);
                                ctx.set_line_cap("round");
                                ctx.begin_path();
                                ctx.move_to(x, min_y);
                                ctx.line_to(x, max_y);
                                ctx.stroke();
                            } else if y1 == y2 {
                                // Horizontal segment
                                let y = y1 * cell_size + cell_size / 2.0;
                                let min_x = x1.min(x2) * cell_size + cell_size / 2.0;
                                let max_x = x1.max(x2) * cell_size + cell_size / 2.0;
                                
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
                    for i in 1..body.len()-1 {
                        if let Some(point) = body.get(i) {
                            if let (Some(x), Some(y)) = (point["x"].as_i64(), point["y"].as_i64()) {
                                let center_x = x as f64 * cell_size + cell_size / 2.0;
                                let center_y = y as f64 * cell_size + cell_size / 2.0;
                                
                                ctx.begin_path();
                                ctx.arc(center_x, center_y, cell_size / 2.0 + 1.0, 0.0, 2.0 * std::f64::consts::PI)?;
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

                            if x1 == x2 {
                                // Vertical segment
                                let x = x1 * cell_size + cell_size / 2.0;
                                let min_y = y1.min(y2) * cell_size + cell_size / 2.0;
                                let max_y = y1.max(y2) * cell_size + cell_size / 2.0;
                                
                                ctx.set_line_width(cell_size);
                                ctx.set_line_cap("round");
                                ctx.begin_path();
                                ctx.move_to(x, min_y);
                                ctx.line_to(x, max_y);
                                ctx.stroke();
                            } else if y1 == y2 {
                                // Horizontal segment
                                let y = y1 * cell_size + cell_size / 2.0;
                                let min_x = x1.min(x2) * cell_size + cell_size / 2.0;
                                let max_x = x1.max(x2) * cell_size + cell_size / 2.0;
                                
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
                    for i in 1..body.len()-1 {
                        if let Some(point) = body.get(i) {
                            if let (Some(x), Some(y)) = (point["x"].as_i64(), point["y"].as_i64()) {
                                let center_x = x as f64 * cell_size + cell_size / 2.0;
                                let center_y = y as f64 * cell_size + cell_size / 2.0;
                                
                                ctx.begin_path();
                                ctx.arc(center_x, center_y, cell_size / 2.0, 0.0, 2.0 * std::f64::consts::PI)?;
                                ctx.fill();
                            }
                        }
                    }

                    // Get head and tail information
                    let head = body.first().unwrap();
                    let head_x = head["x"].as_i64().unwrap_or(0) as f64;
                    let head_y = head["y"].as_i64().unwrap_or(0) as f64;
                    let head_center_x = head_x * cell_size + cell_size / 2.0;
                    let head_center_y = head_y * cell_size + cell_size / 2.0;
                    
                    let tail = body.last().unwrap();
                    let tail_x = tail["x"].as_i64().unwrap_or(0) as f64;
                    let tail_y = tail["y"].as_i64().unwrap_or(0) as f64;
                    let tail_center_x = tail_x * cell_size + cell_size / 2.0;
                    let tail_center_y = tail_y * cell_size + cell_size / 2.0;

                    // Draw actual tail and head (no separate border circles needed)
                    // The round line caps already provide the border
                    ctx.set_fill_style(&JsValue::from_str(color));
                    
                    // Draw tail as full circle
                    ctx.begin_path();
                    ctx.arc(tail_center_x, tail_center_y, cell_size / 2.0, 0.0, 2.0 * std::f64::consts::PI)?;
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
                    }
                    
                    // Now draw all collected cells with their proper distances
                    for (x, y, distance) in cells_with_distance {
                        let opacity = (1.0 - distance / 10.0) * 0.3;
                        ctx.set_fill_style(&JsValue::from_str(&format!("rgba(255, 255, 255, {})", opacity)));
                        
                        ctx.fill_rect(
                            x as f64 * cell_size,
                            y as f64 * cell_size,
                            cell_size,
                            cell_size
                        );
                    }
                    
                    
                    // Draw head as full circle (after overlay for proper layering)
                    ctx.set_fill_style(&JsValue::from_str(color));
                    ctx.begin_path();
                    ctx.arc(head_center_x, head_center_y, cell_size / 2.0, 0.0, 2.0 * std::f64::consts::PI)?;
                    ctx.fill();
                    
                    // Draw white overlay on head (strongest opacity)
                    ctx.set_fill_style(&JsValue::from_str("rgba(255, 255, 255, 0.3)"));
                    ctx.begin_path();
                    ctx.arc(head_center_x, head_center_y, cell_size / 2.0, 0.0, 2.0 * std::f64::consts::PI)?;
                    ctx.fill();

                    // Draw smaller inner circle in head with different color
                    ctx.set_fill_style(&JsValue::from_str("#333"));
                    ctx.begin_path();
                    ctx.arc(head_center_x, head_center_y, cell_size * 0.38, 0.0, 2.0 * std::f64::consts::PI)?;
                    ctx.fill();
                }
            }
        }
    }

    // Draw game info
    ctx.set_fill_style(&JsValue::from_str("#333"));
    ctx.set_font("16px monospace");
    if let Some(tick) = game_state["tick"].as_u64() {
        ctx.fill_text(&format!("Tick: {}", tick), 10.0, 
            canvas_height + 20.0)?;
    }

    // Restore the canvas state (remove padding translation)
    ctx.restore();

    Ok(())
}