use common::*;
use wasm_bindgen::prelude::*;


#[wasm_bindgen]
pub fn render(game: Game, canvas: web_sys::HtmlCanvasElement) {
    let context = canvas
        .get_context("2d")
        .expect("Failed to get 2d context")
        .expect("Failed to get 2d context");

    let cell_size = 20;

    // render grid
    let ctx = context
        .dyn_into::<web_sys::CanvasRenderingContext2d>()
        .expect("Failed to cast to 2d context");

    ctx.set_fill_style(&JsValue::from_str("#eee"));

    for x in 0..game.width {
        for y in 0..game.height {
            ctx.fill_rect(
                (x * (cell_size + 1)) as f64,
                (y * (cell_size + 1)) as f64,
                cell_size as f64,
                cell_size as f64,
            );
        }
    }

}
