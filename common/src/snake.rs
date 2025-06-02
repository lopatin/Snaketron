use std::collections::VecDeque;
use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Copy)]
pub enum Direction {
    Up,
    Down,
    Left,
    Right,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Position {
    pub x: i16,
    pub y: i16,
}

impl Position {
    pub fn is_between(&self, p1: &Position, p2: &Position) -> bool {
        (self.x >= p1.x && self.x <= p2.x || self.x <= p1.x && self.x >= p2.x) &&
        (self.y >= p1.y && self.y <= p2.y || self.y <= p1.y && self.y >= p2.y)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Player {
    pub user_id: u32,
    pub snake_id: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Snake {
    /// `body` is a compressed representation of the snake.
    /// It contains the head, turns, and tail positions.
    pub body: Vec<Position>,
    pub direction: Direction,
    pub is_alive: bool,
    pub food: u32,
}

impl Snake {

    pub fn head(&self) -> Result<&Position> {
        self.body.get(0).context("Snake has no head")
    }

    pub fn tail(&self) -> Result<&Position> {
        self.body.last().context("Snake has no tail")
    }

    pub fn step_forward(&mut self) {
        if !self.is_alive || self.body.len() < 2 {
            return;
        }

        let current_head = self.body[0];
        let (new_head_x, new_head_y) = match self.direction {
            Direction::Up => (current_head.x, current_head.y - 1),
            Direction::Down => (current_head.x, current_head.y + 1),
            Direction::Left => (current_head.x - 1, current_head.y),
            Direction::Right => (current_head.x + 1, current_head.y),
        };

        // New head position
        let p0 = Position { x: new_head_x, y: new_head_y };
        let p1 = self.body[0];
        let p2 = self.body[1];

        // If new head is collinear, update the head position in place.
        // Otherwise, push the new head position to the front of the body.
        if (p0.x == p1.x && p1.x == p2.x) || (p0.y == p1.y && p1.y == p2.y) {
            self.body[0].x = p0.x;
            self.body[0].y = p0.y;
        } else {
            self.body.insert(0, p0);
        }

        if self.food > 0 {
            // Snake grows: tail doesn't move this step.
            self.food -= 1;
        } else {
            // Snake does not grow: tail moves forward.
            let tail_idx = self.body.len() - 1;
            let point_before_tail = self.body[tail_idx - 1];
            let tail_end_pos = &mut self.body[tail_idx];

            // Move tail_end_pos one step towards point_before_tail
            if tail_end_pos.x < point_before_tail.x {
                tail_end_pos.x += 1;
            } else if tail_end_pos.x > point_before_tail.x {
                tail_end_pos.x -= 1;
            } else if tail_end_pos.y < point_before_tail.y {
                tail_end_pos.y += 1;
            } else if tail_end_pos.y > point_before_tail.y {
                tail_end_pos.y -= 1;
            }

            // Remove the last element of the body if is identical to the point before tail
            if *tail_end_pos == point_before_tail {
                self.body.pop();
            }
        }
    }

    pub fn contains_point(&self, point: &Position) -> bool {
        self.iter_body().any(|(p1, p2)| point.is_between(p1, p2))
    }

    pub fn iter_body(&self) -> impl Iterator<Item=(&Position, &Position)> {
        self.body.iter().zip(self.body.iter().skip(1))
    }
}

