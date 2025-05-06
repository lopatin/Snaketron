use std::collections::VecDeque;

#[derive(Debug)]
pub enum Direction {
    Up,
    Down,
    Left,
    Right,
}

#[derive(Debug)]
pub struct Snake {
    pub body: VecDeque<(u16, u16)>,
    pub direction: Direction,
    pub food: u32,
}

impl Snake {
    pub fn head(&self) -> &(u16, u16) {
        self.body.front().expect("Snake body should not be empty")
    }

    pub fn tail(&self) -> &(u16, u16) {
        self.body.back().expect("Snake body should not be empty")
    }
}

