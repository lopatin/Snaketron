use wasm_bindgen::prelude::*;

#[derive(Debug)]
#[wasm_bindgen]
pub struct Game {
    pub width: u16,
    pub height: u16,
}

#[wasm_bindgen]
impl Game {
    #[wasm_bindgen]
    pub fn new(width: u16, height: u16) -> Self {
        Game { width, height }
    }

    pub fn get_area(&self) -> u32 {
        (self.width as u32) * (self.height as u32)
    }
}
