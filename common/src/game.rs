use wasm_bindgen::prelude::*;
use crate::Snake;

#[derive(Debug)]
#[wasm_bindgen]
pub struct Game {
    pub id: i64,
    pub width: u16,
    pub height: u16,
    pub snakes: Vec<Snake>,
}

#[wasm_bindgen]
impl Game {
    #[wasm_bindgen]
    pub fn new(id: i64, width: u16, height: u16) -> Self {
        let snakes = Vec::new();
        Game { id, width, height, snakes }
    }

    pub fn get_area(&self) -> u32 {
        (self.width as u32) * (self.height as u32)
    }


}
