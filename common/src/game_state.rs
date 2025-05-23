use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::process::Command;
use wasm_bindgen::prelude::*;
use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};
use crate::{Direction, Position, Snake};
use crate::util::RandomGenerator;

const DEFAULT_SNAKE_LENGTH: usize = 3;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum GameCommand {
    Tick,
    Turn { snake_id: u32, direction: Direction },
    PositionQueueReplace { snake_id: u32, positions: VecDeque<Position> },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct GameEventMessage {
    pub game_id: u32,
    pub tick: u32,
    pub player_id: Option<u32>,
    pub event: GameEvent,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum GameEvent {
    SnakeTurned { snake_id: u32, direction: Direction },
    SnakeDied { snake_id: u32 },
    FoodSpawned { position: Position },
    FoodEaten { snake_id: u32, position: Position },
    Snapshot { game_state: GameState },
    CommandPendingOnServer { command_message: GameCommandMessage },
    PositionQueueUpdate { snake_id: u32, positions: VecDeque<Position> }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Arena {
    pub width: u16,
    pub height: u16,
    pub snakes: Vec<Snake>,
    pub food: HashSet<Position>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct GameProperties {
    pub available_food_target: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum GameType {
    TeamMatch { per_team: u8 },
    FreeForAll { max_players: u8 },
}

impl GameType {
    pub fn is_duel(&self) -> bool {
        self == &GameType::TeamMatch { per_team: 1 }
    }
}

// Serializable state for snapshots
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct GameState {
    // Data members
    pub tick: u32,
    pub arena: Arena,
    pub game_type: GameType,
    pub properties: GameProperties,
    pub position_queues: HashMap<u32, VecDeque<Position>>,

    // Ephemeral state
    #[serde(skip)]
    pub rng: Option<dyn RandomGenerator>,
}

// Wrapper for BinaryHeap to order commands by their intended execution tick.
#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct GameCommandMessage {
    pub tick: u32,
    pub received_order: u32,
    pub command: GameCommand,
}

impl Ord for GameCommandMessage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.tick.cmp(&self.tick) // Min-heap for tick
            .then_with(|| other.received_order.cmp(&self.received_order)) // Min-heap for order
    }
}

impl PartialOrd for GameCommandMessage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}

impl GameState {
    pub fn new(width: u16, height: u16, rng: Option<dyn RandomGenerator>) -> Self {
        GameState {
            tick: 0,
            arena: Arena {
                width,
                height,
                snakes: Vec::new(),
                food: HashSet::new(),
            },
            game_type: GameType::TeamMatch { per_team: 1 },
            properties: GameProperties {
                available_food_target: 5,
            },
            position_queues: HashMap::new(),
            rng
        }
    }

    pub fn current_tick(&self) -> u32 { self.tick }

    fn get_snake(&self, snake_id: u32) -> Result<&mut Snake> {
        self.arena.snakes.get(snake_id).context("Snake not found")
    }

    fn iter_snakes(&self) -> impl Iterator<Item = (u32, &Snake)> {
        self.arena.snakes.iter().enumerate().map(|(id, snake)| (id as u32, snake))
    }

    pub fn exec_command(&mut self, command: &GameCommand) -> Result<Vec<GameEvent>> {
        let out: Vec<GameEvent> = Vec::new();
        match command {
            GameCommand::Tick => {

                if self.tick == 0 {

                }

                // Take a snapshot of the existing snakes to rollback dead ones after movement
                let old_snakes = self.arena.snakes.clone();

                // Move snakes
                for (snake) in self.arena.snakes.iter_mut() {
                    if snake.is_alive {
                        snake.step_forward()
                    }
                }

                // Check for collisions
                let mut crashed_snake_ids: Vec<u32> = Vec::new();
                'main_snake_loop: for (snake_id, snake) in self.iter_snakes() {
                    let head = snake.head()?;
                    if snake.is_alive {
                        // If not within bounds
                        if !head.x >= 0 && head.x < self.arena.width as i16 &&
                                head.y >= 0 && head.y < self.arena.height as i16 {
                            crashed_snake_ids.push(snake_id);
                            continue 'main_snake_loop;
                        }

                        // If crashed with other snake
                        for (other_snake_id, other_snake) in self.iter_snakes() {
                            if snake_id != other_snake_id && other_snake.is_alive &&
                                    other_snake.contains_point(head) {
                                crashed_snake_ids.push(snake_id);
                                continue 'main_snake_loop;
                            }
                        }
                    }
                }

                // Rollback and kill snakes that crashed
                for snake_id in crashed_snake_ids {
                    self.arena.snakes[snake_id] = old_snakes[snake_id].clone();
                    self.apply_event(GameEvent::SnakeDied { snake_id }, &out);
                }

                // Eat food
                for (snake_id, snake) in self.iter_snakes() {
                    let head = snake.head()?;
                    if snake.is_alive && self.arena.food.contains(head) {
                        self.apply_event(GameEvent::FoodEaten { snake_id, position: *head }, &out);
                    }
                }

                // Spawn new food
                if self.arena.food.len() < self.properties.available_food_target {
                    // The client will not have rng so it won't be able to spawn food.
                    // This is by design as there's no reason for the client to spawn food.
                    if let Some(rng) = &self.rng {
                        let position = Position {
                            x: rng.random_u32() % self.arena.width as u32,
                            y: rng.random_u32() % self.arena.height as u32,
                        };

                        if !self.arena.food.contains(&position) &&
                                !self.arena.snakes.iter().any(|s| s.is_alive && s.contains_point(&position)) {
                            self.apply_event(GameEvent::FoodSpawned { position }, &out);
                        }
                    }
                }

                // Increment tick
                self.tick += 1;
            }

            GameCommand::Turn { snake_id, direction } => {
                let snake = self.get_snake(snake_id)?;
                if snake.is_alive && snake.direction != direction {
                    self.apply_event(GameEvent::SnakeTurned { snake_id, direction }, &out);
                }
            }

            GameCommand::PositionQueueReplace { snake_id, positions } => {
                if let Ok(snake) = self.get_snake(snake_id) {
                    self.apply_event(GameEvent::PositionQueueUpdate { snake_id, positions }, &out);
                }
            }
        }

        Ok(out)
    }

    pub fn apply_event(&mut self, event: GameEvent, out: Option<&mut Vec<GameEvent>>) {
        if let Some(out) = out {
            out.push(event.clone());
        }

        match event {
            GameEvent::Snapshot { game_state } => {
                *self = game_state;
            }

            GameEvent::SnakeTurned { snake_id, direction } => {
                if let Ok(snake) = self.get_snake(snake_id) {
                    snake.direction = direction;
                }
            }

            GameEvent::SnakeDied { snake_id } => {
                if let Ok(snake) = self.get_snake(snake_id) {
                    snake.is_alive = false;
                }
            }

            GameEvent::FoodSpawned { position } => {
                self.arena.food.insert(position);
            }

            GameEvent::FoodEaten { snake_id, position } => {
                if let Ok(snake) = self.get_snake(snake_id) {
                    if self.arena.food.remove(&position) {
                        snake.food += 1;
                    }
                }
            }

            GameEvent::PositionQueueUpdate { snake_id, positions } => {
                if let Ok(_) = self.get_snake(snake_id) {
                    self.position_queues.insert(snake_id, positions);
                }
            }

            GameEvent::CommandPendingOnServer(_) => {}
        }

    }
}
