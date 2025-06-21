use wasm_bindgen::prelude::*;
use serde_json::Value;

/// Renders the game state to a canvas element
/// Takes a JSON string representation of the game state
#[wasm_bindgen]
pub fn render_game(game_state_json: &str, canvas: web_sys::HtmlCanvasElement, cell_size: f64) -> Result<(), JsValue> {
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

    // Calculate total canvas size
    let canvas_width = width as f64 * cell_size;
    let canvas_height = height as f64 * cell_size;

    // Clear canvas with white background
    ctx.set_fill_style(&JsValue::from_str("#ffffff"));
    ctx.fill_rect(0.0, 0.0, canvas_width, canvas_height);

    // Draw dots at grid intersections (like the background pattern)
    ctx.set_fill_style(&JsValue::from_str("rgba(0, 0, 0, 0.3)")); // Same as background dots
    
    // Only draw dots at 15px intervals to match background, regardless of cell size
    let dot_spacing = 15.0;
    let dots_x = (canvas_width / dot_spacing).ceil() as u32;
    let dots_y = (canvas_height / dot_spacing).ceil() as u32;
    
    // Start from 1 and end at dots_x/y - 1 to skip outer edge dots
    for x in 1..dots_x {
        for y in 1..dots_y {
            let dot_x = x as f64 * dot_spacing;
            let dot_y = y as f64 * dot_spacing;
            
            // Skip dots that are on or outside the canvas edges
            if dot_x >= canvas_width || dot_y >= canvas_height {
                continue;
            }
            
            // Draw a small circle dot
            ctx.begin_path();
            ctx.arc(dot_x, dot_y, dot_radius, 0.0, 2.0 * std::f64::consts::PI)?;
            ctx.fill();
        }
    }

    // Draw food
    if let Some(food_array) = arena["food"].as_array() {
        ctx.set_fill_style(&JsValue::from_str("#ff6b6b"));
        for food in food_array {
            if let (Some(x), Some(y)) = (food["x"].as_i64(), food["y"].as_i64()) {
                let cell_x = x as f64 * cell_size;
                let cell_y = y as f64 * cell_size;
                ctx.fill_rect(cell_x, cell_y, cell_size, cell_size);
            }
        }
    }

    // Draw snakes
    if let Some(snakes) = arena["snakes"].as_array() {
        for (index, snake) in snakes.iter().enumerate() {
            if snake["is_alive"].as_bool().unwrap_or(false) {
                // Choose snake color based on index
                let color = match index % 4 {
                    0 => "#4ecdc4",
                    1 => "#556270",
                    2 => "#ff6b6b",
                    _ => "#f7b731",
                };
                
                // Calculate darker shade for border (darken by ~30%)
                let border_color = match index % 4 {
                    0 => "#349a92",  // Darker teal
                    1 => "#353c47",  // Darker gray
                    2 => "#b84444",  // Darker red
                    _ => "#a87d1f",  // Darker yellow
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
                    
                    // First pass: Draw borders (1px larger)
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

                    // Second pass: Draw the actual snake
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

                    // Draw head as full circle
                    ctx.begin_path();
                    ctx.arc(head_center_x, head_center_y, cell_size / 2.0, 0.0, 2.0 * std::f64::consts::PI)?;
                    ctx.fill();

                    // Draw smaller inner circle in head with different color
                    ctx.set_fill_style(&JsValue::from_str("#333"));
                    ctx.begin_path();
                    ctx.arc(head_center_x, head_center_y, cell_size * 0.38, 0.0, 2.0 * std::f64::consts::PI)?;
                    ctx.fill();
                    
                    // Reset fill style back to snake color
                    ctx.set_fill_style(&JsValue::from_str(color));
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

    Ok(())
}