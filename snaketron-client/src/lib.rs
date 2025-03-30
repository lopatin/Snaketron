use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn run() {
    // Call the shared function from the common crate.
    common::add(2, 3);
}