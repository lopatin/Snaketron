mod render;

pub use render::*;
pub use common::*;

use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn run(a: u32, b: u32) -> u32 {
    add(a, b)
}