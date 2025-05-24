use wasm_bindgen::prelude::*;
use serde_json::Value;

/// Renders the game state to a canvas element
/// Takes a JSON string representation of the game state
#[wasm_bindgen]
pub fn render_game(game_state_json: &str, canvas: web_sys::HtmlCanvasElement) -> Result<(), JsValue> {
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

    let cell_size = 20.0;
    let padding = 1.0;

    // Clear canvas
    ctx.clear_rect(0.0, 0.0, 
        (width as f64 * (cell_size + padding)) + padding,
        (height as f64 * (cell_size + padding)) + padding
    );

    // Draw background grid
    ctx.set_fill_style(&JsValue::from_str("#f0f0f0"));
    for x in 0..width {
        for y in 0..height {
            ctx.fill_rect(
                (x as f64 * (cell_size + padding)) + padding,
                (y as f64 * (cell_size + padding)) + padding,
                cell_size,
                cell_size,
            );
        }
    }

    // Draw food
    if let Some(food_array) = arena["food"].as_array() {
        ctx.set_fill_style(&JsValue::from_str("#ff6b6b"));
        for food in food_array {
            if let (Some(x), Some(y)) = (food["x"].as_i64(), food["y"].as_i64()) {
                ctx.fill_rect(
                    (x as f64 * (cell_size + padding)) + padding,
                    (y as f64 * (cell_size + padding)) + padding,
                    cell_size,
                    cell_size,
                );
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
                ctx.set_fill_style(&JsValue::from_str(color));

                // Draw snake body
                if let Some(body) = snake["body"].as_array() {
                    // The body is compressed, so we need to draw lines between consecutive points
                    for window in body.windows(2) {
                        if let (Some(p1), Some(p2)) = (window.get(0), window.get(1)) {
                            let x1 = p1["x"].as_i64().unwrap_or(0);
                            let y1 = p1["y"].as_i64().unwrap_or(0);
                            let x2 = p2["x"].as_i64().unwrap_or(0);
                            let y2 = p2["y"].as_i64().unwrap_or(0);

                            // Draw line segment between p1 and p2
                            let min_x = x1.min(x2);
                            let max_x = x1.max(x2);
                            let min_y = y1.min(y2);
                            let max_y = y1.max(y2);

                            for x in min_x..=max_x {
                                for y in min_y..=max_y {
                                    ctx.fill_rect(
                                        (x as f64 * (cell_size + padding)) + padding,
                                        (y as f64 * (cell_size + padding)) + padding,
                                        cell_size,
                                        cell_size,
                                    );
                                }
                            }
                        }
                    }

                    // Draw snake head with a different shade
                    if let Some(head) = body.first() {
                        if let (Some(x), Some(y)) = (head["x"].as_i64(), head["y"].as_i64()) {
                            ctx.set_fill_style(&JsValue::from_str("#333"));
                            ctx.fill_rect(
                                (x as f64 * (cell_size + padding)) + padding + 2.0,
                                (y as f64 * (cell_size + padding)) + padding + 2.0,
                                cell_size - 4.0,
                                cell_size - 4.0,
                            );
                        }
                    }
                }
            }
        }
    }

    // Draw game info
    ctx.set_fill_style(&JsValue::from_str("#333"));
    ctx.set_font("16px monospace");
    if let Some(tick) = game_state["tick"].as_u64() {
        ctx.fill_text(&format!("Tick: {}", tick), 10.0, 
            (height as f64 * (cell_size + padding)) + padding + 20.0)?;
    }

    Ok(())
}