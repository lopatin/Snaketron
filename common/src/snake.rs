use crate::{BOOST_TICK_INTERVAL_MS, MAX_BOOST_SPEED_MILLI, NORMAL_SNAKE_SPEED_MILLI, TeamId};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Copy)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum Direction {
    Up,
    Down,
    Left,
    Right,
}

impl Direction {
    /// Returns true if the two directions are opposite (180 degrees apart)
    pub fn is_opposite(&self, other: &Direction) -> bool {
        matches!(
            (self, other),
            (Direction::Up, Direction::Down)
                | (Direction::Down, Direction::Up)
                | (Direction::Left, Direction::Right)
                | (Direction::Right, Direction::Left)
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct Position {
    pub x: i16,
    pub y: i16,
}

impl Position {
    pub fn is_between(&self, p1: &Position, p2: &Position) -> bool {
        (self.x >= p1.x && self.x <= p2.x || self.x <= p1.x && self.x >= p2.x)
            && (self.y >= p1.y && self.y <= p2.y || self.y <= p1.y && self.y >= p2.y)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct Player {
    pub user_id: u32,
    pub snake_id: u32,
}

/// Snake-owned Boost fuel and activation state.
///
/// Charge is stored as milliseconds of funded Boost time. It may only be
/// mutated through the crate-visible lifecycle methods on [`Snake`], keeping
/// speed changes coupled to Boost activation and depletion.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct SnakeBoost {
    pub charge_ms: u32,
    pub active: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct Snake {
    /// `body` is a compressed representation of the snake.
    /// It contains the head, turns, and tail positions.
    pub body: Vec<Position>,
    pub direction: Direction,
    pub is_alive: bool,
    pub food: u32,
    pub team_id: Option<TeamId>, // New field for team assignment
    /// Authoritative movement rate in milli-normal units. Internal Boost
    /// lifecycle methods are the only gameplay code allowed to change it.
    #[serde(default = "default_snake_speed_milli")]
    pub(crate) speed_milli: u16,
    /// Residual milli-milliseconds toward the snake's next cell movement.
    #[serde(default)]
    pub(crate) movement_credit: u32,
    #[serde(default)]
    pub(crate) boost: SnakeBoost,
}

fn default_snake_speed_milli() -> u16 {
    NORMAL_SNAKE_SPEED_MILLI
}

impl Snake {
    /// Construct a snake in the only valid externally-creatable movement
    /// state: normal speed, zero residual credit, and no stored/active Boost.
    /// Boost fields intentionally have no public setters; gameplay transitions
    /// inside `common` own every later mutation.
    pub fn new(
        body: Vec<Position>,
        direction: Direction,
        is_alive: bool,
        food: u32,
        team_id: Option<TeamId>,
    ) -> Self {
        Self {
            body,
            direction,
            is_alive,
            food,
            team_id,
            speed_milli: NORMAL_SNAKE_SPEED_MILLI,
            movement_credit: 0,
            boost: SnakeBoost::default(),
        }
    }

    pub fn speed_milli(&self) -> u16 {
        self.speed_milli
    }

    pub fn movement_credit(&self) -> u32 {
        self.movement_credit
    }

    pub fn boost(&self) -> &SnakeBoost {
        &self.boost
    }

    /// Add packet charge without activating Boost or changing speed.
    ///
    /// Returns the absolute post-collection charge when any fuel was stored.
    /// `None` means the packet was not consumed (dead snake, empty packet, or
    /// an already-full/invalid-capacity meter).
    pub(crate) fn collect_boost_charge(&mut self, amount_ms: u32, capacity_ms: u32) -> Option<u32> {
        if !self.is_alive
            || amount_ms == 0
            || capacity_ms == 0
            || !amount_ms.is_multiple_of(BOOST_TICK_INTERVAL_MS)
            || !capacity_ms.is_multiple_of(BOOST_TICK_INTERVAL_MS)
            || self.boost.charge_ms >= capacity_ms
        {
            return None;
        }

        let available_ms = capacity_ms - self.boost.charge_ms;
        self.boost.charge_ms += amount_ms.min(available_ms);
        Some(self.boost.charge_ms)
    }

    /// Activate stored Boost using immutable, validated match configuration.
    ///
    /// The command is a deterministic no-op unless this is a living, inactive
    /// snake with a whole-quantum charge inside the configured capacity.
    pub(crate) fn try_activate_boost(
        &mut self,
        configured_speed_milli: u16,
        capacity_ms: u32,
    ) -> bool {
        let valid_config = (NORMAL_SNAKE_SPEED_MILLI..=MAX_BOOST_SPEED_MILLI)
            .contains(&configured_speed_milli)
            && capacity_ms > 0
            && capacity_ms.is_multiple_of(BOOST_TICK_INTERVAL_MS);
        let valid_charge = self.boost.charge_ms > 0
            && self.boost.charge_ms <= capacity_ms
            && self.boost.charge_ms.is_multiple_of(BOOST_TICK_INTERVAL_MS);

        if !self.is_alive
            || self.boost.active
            || self.speed_milli != NORMAL_SNAKE_SPEED_MILLI
            || !valid_config
            || !valid_charge
        {
            return false;
        }

        self.boost.active = true;
        self.speed_milli = configured_speed_milli;
        true
    }

    /// Stop active Boost without discarding stored fuel or movement phase.
    ///
    /// This is the authoritative release edge for hold-to-Boost controls. The
    /// transition is deliberately idempotent so retries cannot invert state.
    pub(crate) fn try_deactivate_boost(&mut self) -> bool {
        if !self.is_alive || !self.boost.active {
            return false;
        }

        self.boost.active = false;
        self.speed_milli = NORMAL_SNAKE_SPEED_MILLI;
        true
    }

    /// Reserve one funded 50 ms Boost quantum while retaining boosted speed.
    ///
    /// Depletion is finalized only after movement and packet collection, so a
    /// snake that lands on a packet during its final funded quantum continues
    /// Boost seamlessly.
    pub(crate) fn reserve_boost_quantum(&mut self) -> bool {
        if !self.is_alive || !self.boost.active || self.boost.charge_ms < BOOST_TICK_INTERVAL_MS {
            return false;
        }

        self.boost.charge_ms -= BOOST_TICK_INTERVAL_MS;
        true
    }

    /// Restore normal speed if the post-collection meter is still empty.
    /// Returns whether active Boost was depleted and finalized.
    pub(crate) fn finalize_boost_depletion(&mut self) -> bool {
        if !self.boost.active || self.boost.charge_ms != 0 {
            return false;
        }

        self.boost.active = false;
        self.speed_milli = NORMAL_SNAKE_SPEED_MILLI;
        true
    }

    /// Add deterministic movement credit and consume at most one opportunity.
    ///
    /// `normal_movement_interval_ms` defines one cell at normal speed. Boost
    /// team games pass 100 ms while running at a 50 ms simulation quantum;
    /// legacy and Custom modes pass their configured tick duration for both
    /// arguments. Arithmetic is widened so multiplication cannot overflow.
    pub(crate) fn accrue_movement_credit(
        &mut self,
        tick_duration_ms: u32,
        normal_movement_interval_ms: u32,
    ) -> bool {
        if !self.is_alive {
            return false;
        }

        assert!(
            normal_movement_interval_ms > 0,
            "normal movement interval must be positive"
        );

        let threshold =
            u64::from(NORMAL_SNAKE_SPEED_MILLI) * u64::from(normal_movement_interval_ms);
        let accumulated = u64::from(self.movement_credit)
            + u64::from(self.speed_milli) * u64::from(tick_duration_ms);

        assert!(
            accumulated < threshold * 2,
            "a snake may accrue at most one movement opportunity per simulation quantum"
        );

        let (moves, residual) = if accumulated >= threshold {
            (true, accumulated - threshold)
        } else {
            (false, accumulated)
        };
        self.movement_credit =
            u32::try_from(residual).expect("movement credit residual must fit in serialized state");
        moves
    }

    /// Clear all fuel and movement phase when the snake gets a new life.
    pub(crate) fn reset_boost_and_movement(&mut self) {
        self.speed_milli = NORMAL_SNAKE_SPEED_MILLI;
        self.movement_credit = 0;
        self.boost = SnakeBoost::default();
    }

    pub fn head(&self) -> Result<&Position> {
        self.body.first().context("Snake has no head")
    }

    pub fn tail(&self) -> Result<&Position> {
        self.body.last().context("Snake has no tail")
    }

    /// The direction the snake actually traveled on its last movement step,
    /// derived from the head and the point behind it. Unlike `direction`,
    /// this cannot be flipped by a turn command that hasn't been stepped yet,
    /// so it is the reference for 180-degree-turn validation when several
    /// commands execute on one tick. Falls back to `direction` for bodies
    /// that have no extent (e.g. a snake placed but not yet positioned).
    pub fn travel_direction(&self) -> Direction {
        if self.body.len() >= 2 {
            let head = self.body[0];
            let neck = self.body[1];
            if head.x > neck.x {
                return Direction::Right;
            }
            if head.x < neck.x {
                return Direction::Left;
            }
            if head.y > neck.y {
                return Direction::Down;
            }
            if head.y < neck.y {
                return Direction::Up;
            }
        }
        self.direction
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
        let p0 = Position {
            x: new_head_x,
            y: new_head_y,
        };
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

    pub fn contains_point(&self, point: &Position, skip_head: bool) -> bool {
        self.iter_body().enumerate().any(|(idx, (p1, p2))| {
            if skip_head && idx == 0 && *p1 == *point {
                return false;
            }

            point.is_between(p1, p2)
        })
    }

    pub fn is_head(&self, point: &Position) -> bool {
        if let Some(head) = self.body.first() {
            *head == *point
        } else {
            false
        }
    }

    pub fn iter_body(&self) -> impl Iterator<Item = (&Position, &Position)> {
        self.body.iter().zip(self.body.iter().skip(1))
    }

    /// Calculate the actual length of the snake (number of grid cells it occupies)
    pub fn length(&self) -> usize {
        if self.body.len() < 2 {
            return self.body.len();
        }

        let mut length = 0;
        for (p1, p2) in self.iter_body() {
            // Calculate Manhattan distance between consecutive points
            let distance = ((p2.x - p1.x).abs() + (p2.y - p1.y).abs()) as usize;
            length += distance;
        }
        length + 1 // Add 1 for the head
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snake_with_body(body: Vec<Position>, direction: Direction) -> Snake {
        Snake {
            body,
            direction,
            is_alive: true,
            food: 0,
            team_id: None,
            speed_milli: NORMAL_SNAKE_SPEED_MILLI,
            movement_credit: 0,
            boost: SnakeBoost::default(),
        }
    }

    #[test]
    fn boost_defaults_are_normal_and_empty() {
        let snake = snake_with_body(
            vec![Position { x: 1, y: 1 }, Position { x: 0, y: 1 }],
            Direction::Right,
        );

        assert_eq!(snake.speed_milli, NORMAL_SNAKE_SPEED_MILLI);
        assert_eq!(snake.movement_credit, 0);
        assert_eq!(snake.boost, SnakeBoost::default());
    }

    #[test]
    fn collecting_charge_caps_at_capacity_without_activating() {
        let mut snake = snake_with_body(vec![], Direction::Right);

        assert_eq!(snake.collect_boost_charge(1_000, 3_000), Some(1_000));
        assert_eq!(snake.collect_boost_charge(25, 3_000), None);
        assert_eq!(snake.collect_boost_charge(1_000, 3_025), None);
        assert_eq!(snake.collect_boost_charge(5_000, 3_000), Some(3_000));
        assert_eq!(snake.collect_boost_charge(1_000, 3_000), None);
        assert_eq!(snake.boost.charge_ms, 3_000);
        assert!(!snake.boost.active);
        assert_eq!(snake.speed_milli, NORMAL_SNAKE_SPEED_MILLI);
    }

    #[test]
    fn activation_requires_valid_living_snake_and_match_configuration() {
        let mut snake = snake_with_body(vec![], Direction::Right);

        assert!(!snake.try_activate_boost(1_500, 3_000));
        snake.collect_boost_charge(1_000, 3_000);
        assert!(!snake.try_activate_boost(MAX_BOOST_SPEED_MILLI + 1, 3_000));
        assert!(!snake.try_activate_boost(1_500, 3_025));

        assert!(snake.try_activate_boost(1_500, 3_000));
        assert!(snake.boost.active);
        assert_eq!(snake.speed_milli, 1_500);
        assert!(!snake.try_activate_boost(2_000, 3_000));
    }

    #[test]
    fn deactivation_is_idempotent_and_preserves_charge_and_movement_phase() {
        let mut snake = snake_with_body(vec![], Direction::Right);
        snake.collect_boost_charge(1_000, 3_000);
        snake.movement_credit = 42_000;
        assert!(snake.try_activate_boost(1_500, 3_000));

        assert!(snake.try_deactivate_boost());
        assert!(!snake.boost.active);
        assert_eq!(snake.speed_milli, NORMAL_SNAKE_SPEED_MILLI);
        assert_eq!(snake.boost.charge_ms, 1_000);
        assert_eq!(snake.movement_credit, 42_000);

        assert!(!snake.try_deactivate_boost());
        assert_eq!(snake.boost.charge_ms, 1_000);
        assert!(snake.try_activate_boost(1_500, 3_000));
    }

    #[test]
    fn final_funded_quantum_can_refill_before_depletion_finalizes() {
        let mut snake = snake_with_body(vec![], Direction::Right);
        snake.collect_boost_charge(BOOST_TICK_INTERVAL_MS, 100);
        assert!(snake.try_activate_boost(1_500, 100));

        assert!(snake.reserve_boost_quantum());
        assert_eq!(snake.boost.charge_ms, 0);
        assert!(snake.boost.active);
        assert_eq!(snake.speed_milli, 1_500);

        assert_eq!(
            snake.collect_boost_charge(BOOST_TICK_INTERVAL_MS, 100),
            Some(BOOST_TICK_INTERVAL_MS)
        );
        assert!(!snake.finalize_boost_depletion());
        assert!(snake.boost.active);

        assert!(snake.reserve_boost_quantum());
        assert!(snake.finalize_boost_depletion());
        assert!(!snake.boost.active);
        assert_eq!(snake.speed_milli, NORMAL_SNAKE_SPEED_MILLI);
    }

    #[test]
    fn movement_credit_is_deterministic_and_preserves_residual() {
        let mut snake = snake_with_body(vec![], Direction::Right);

        assert!(!snake.accrue_movement_credit(50, 100));
        assert_eq!(snake.movement_credit, 50_000);
        assert!(snake.accrue_movement_credit(50, 100));
        assert_eq!(snake.movement_credit, 0);

        snake.collect_boost_charge(100, 100);
        assert!(snake.try_activate_boost(1_500, 100));
        assert!(!snake.accrue_movement_credit(50, 100));
        assert_eq!(snake.movement_credit, 75_000);
        assert!(snake.accrue_movement_credit(50, 100));
        assert_eq!(snake.movement_credit, 50_000);
    }

    #[test]
    fn every_supported_speed_has_exact_long_run_distance() {
        const QUANTA: u32 = 200;
        for speed_milli in [1_000, 1_250, 1_500, 1_750, 2_000] {
            let mut snake = snake_with_body(vec![], Direction::Right);
            snake.speed_milli = speed_milli;

            let moves = (0..QUANTA)
                .filter(|_| snake.accrue_movement_credit(50, 100))
                .count() as u32;
            assert_eq!(
                moves,
                u32::from(speed_milli) / 10,
                "wrong ten-second distance at {speed_milli} milli-speed"
            );
            assert_eq!(snake.movement_credit, 0);
        }
    }

    #[test]
    fn reset_clears_boost_and_movement_phase() {
        let mut snake = snake_with_body(vec![], Direction::Right);
        snake.collect_boost_charge(1_000, 3_000);
        assert!(snake.try_activate_boost(1_500, 3_000));
        snake.movement_credit = 42_000;

        snake.reset_boost_and_movement();

        assert_eq!(snake.speed_milli, NORMAL_SNAKE_SPEED_MILLI);
        assert_eq!(snake.movement_credit, 0);
        assert_eq!(snake.boost, SnakeBoost::default());
    }

    /// Pins `travel_direction` to the engine's coordinate system, where y
    /// grows downward: `step_forward` maps Up to y-1 and Down to y+1, so a
    /// head with a LARGER y than its neck got there by moving Down.
    #[test]
    fn travel_direction_matches_step_forward_convention() {
        for direction in [
            Direction::Up,
            Direction::Down,
            Direction::Left,
            Direction::Right,
        ] {
            // A straight snake facing `direction`, then stepped once: its
            // geometry must report the direction it just moved in.
            let mut snake = snake_with_body(
                vec![Position { x: 10, y: 10 }, Position { x: 7, y: 10 }],
                Direction::Right,
            );
            snake.step_forward(); // establish geometry along +x
            snake.direction = direction;
            // Turning up/down from horizontal travel is a legal 90-degree
            // turn; stepping commits the new direction into the geometry.
            if matches!(direction, Direction::Up | Direction::Down) {
                snake.step_forward();
            }
            snake.direction = Direction::Left; // decoy: geometry must win
            if matches!(direction, Direction::Up | Direction::Down) {
                assert_eq!(
                    snake.travel_direction(),
                    direction,
                    "vertical travel {direction:?} misreported"
                );
            }
        }

        // Explicit fixtures for all four axes.
        let up = snake_with_body(
            vec![Position { x: 5, y: 3 }, Position { x: 5, y: 6 }],
            Direction::Left,
        );
        assert_eq!(up.travel_direction(), Direction::Up);

        let down = snake_with_body(
            vec![Position { x: 5, y: 6 }, Position { x: 5, y: 3 }],
            Direction::Left,
        );
        assert_eq!(down.travel_direction(), Direction::Down);

        let left = snake_with_body(
            vec![Position { x: 2, y: 5 }, Position { x: 6, y: 5 }],
            Direction::Up,
        );
        assert_eq!(left.travel_direction(), Direction::Left);

        let right = snake_with_body(
            vec![Position { x: 6, y: 5 }, Position { x: 2, y: 5 }],
            Direction::Up,
        );
        assert_eq!(right.travel_direction(), Direction::Right);

        // Degenerate body (no extent): falls back to `direction`.
        let degenerate = snake_with_body(
            vec![Position { x: 0, y: 0 }, Position { x: 0, y: 0 }],
            Direction::Down,
        );
        assert_eq!(degenerate.travel_direction(), Direction::Down);
    }
}
