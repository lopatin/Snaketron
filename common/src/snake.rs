use std::collections::VecDeque;

// Direction
#[derive(Debug)]
pub enum Direction {
    Up,
    Down,
    Left,
    Right,
}

// Snake
#[derive(Debug)]
pub struct Snake {
    pub body: VecDeque<(u32, u32)>,
    pub direction: Direction,
    pub food: u32,
}

impl Snake {
    pub fn head(&self) -> &(u32, u32) {
        self.body.front().expect("Snake body should not be empty")
    }

    pub fn tail(&self) -> &(u32, u32) {
        self.body.back().expect("Snake body should not be empty")
    }
}

