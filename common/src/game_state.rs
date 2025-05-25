use std::collections::{HashMap, HashSet, VecDeque};
use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};
use crate::{Direction, Position, Snake};
use crate::util::PseudoRandom;

const DEFAULT_SNAKE_LENGTH: usize = 3;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum GameCommand {
    Tick,
    Turn { snake_id: u32, direction: Direction },
    PositionQueueReplace { snake_id: u32, positions: VecDeque<Position> },
    RequestSnapshot,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct GameEventMessage {
    pub game_id: u32,
    pub tick: u32,
    pub user_id: Option<u32>,
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
    PositionQueueUpdate { snake_id: u32, positions: VecDeque<Position> },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Arena {
    pub width: u16,
    pub height: u16,
    pub snakes: Vec<Snake>,
    pub food: Vec<Position>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct GameProperties {
    pub available_food_target: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum GameType {
    TeamMatch { per_team: u8 },
    FreeForAll { max_players: u8 },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum GameStatus {
    Waiting,    // Created, waiting for players to connect
    Active,     // Game in progress
    Finished,   // Game completed normally
    Abandoned,  // All players disconnected
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rng: Option<PseudoRandom>,
}

// Wrapper for BinaryHeap to order commands by their intended execution tick.
#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct GameCommandMessage {
    pub tick: u32,
    pub received_order: u32,
    pub user_id: u32,
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
    pub fn new(width: u16, height: u16, rng_seed: Option<u64>) -> Self {
        Self::new_with_type(width, height, GameType::TeamMatch { per_team: 1 }, rng_seed)
    }
    
    pub fn new_with_type(width: u16, height: u16, game_type: GameType, rng_seed: Option<u64>) -> Self {
        GameState {
            tick: 0,
            arena: Arena {
                width,
                height,
                snakes: Vec::new(),
                food: Vec::new(),
            },
            game_type,
            properties: GameProperties {
                available_food_target: 5,
            },
            position_queues: HashMap::new(),
            rng: rng_seed.map(PseudoRandom::new)
        }
    }

    pub fn current_tick(&self) -> u32 { self.tick }

    fn get_snake_mut(&mut self, snake_id: u32) -> Result<&mut Snake> {
        self.arena.snakes.get_mut(snake_id as usize).context("Snake not found")
    }

    fn iter_snakes(&self) -> impl Iterator<Item = (u32, &Snake)> {
        self.arena.snakes.iter().enumerate().map(|(id, snake)| (id as u32, snake))
    }

    fn has_food(&self, position: &Position) -> bool {
        self.arena.food.contains(position)
    }

    fn remove_food(&mut self, position: &Position) -> bool {
        if let Some(index) = self.arena.food.iter().position(|p| p == position) {
            self.arena.food.remove(index);
            true
        } else {
            false
        }
    }

    pub fn exec_command(&mut self, command: GameCommand) -> Result<Vec<GameEvent>> {
        let mut out: Vec<GameEvent> = Vec::new();
        match command {
            GameCommand::Tick => {

                if self.tick == 0 {
                    // Emit snapshot on first tick
                    out.push(GameEvent::Snapshot { game_state: self.clone() });
                }

                // Take a snapshot of the existing snakes to rollback dead ones after movement
                let old_snakes = self.arena.snakes.clone();

                // Move snakes
                for snake in self.arena.snakes.iter_mut() {
                    if snake.is_alive {
                        snake.step_forward()
                    }
                }

                // Check for collisions
                let mut crashed_snake_ids: Vec<u32> = Vec::new();
                let width = self.arena.width as i16;
                let height = self.arena.height as i16;
                'main_snake_loop: for (snake_id, snake) in self.iter_snakes() {
                    let head = snake.head()?;
                    if snake.is_alive {
                        // If not within bounds
                        if !head.x >= 0 && head.x < width && head.y >= 0 && head.y < height {
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
                    self.arena.snakes[snake_id as usize] = old_snakes[snake_id as usize].clone();
                    self.apply_event(GameEvent::SnakeDied { snake_id }, Some(&mut out));
                }

                // Eat food
                let mut food_eaten_events: Vec<GameEvent> = Vec::new();
                for (snake_id, snake) in self.iter_snakes() {
                    let head = snake.head()?;
                    if snake.is_alive && self.arena.food.contains(head) {
                        food_eaten_events.push(GameEvent::FoodEaten { snake_id, position: *head });
                    }
                }
                for event in food_eaten_events {
                    self.apply_event(event, Some(&mut out));
                }

                // Spawn new food
                if self.arena.food.len() < self.properties.available_food_target {
                    // The client will not have rng so it won't be able to spawn food.
                    // This is by design as there's no reason for the client to spawn food.
                    if let Some(rng) = &mut self.rng {
                        let position = Position {
                            x: (rng.next_u16() % self.arena.width) as i16,
                            y: (rng.next_u16() % self.arena.height) as i16,
                        };

                        if !self.arena.food.contains(&position) &&
                                !self.arena.snakes.iter().any(|s| s.is_alive && s.contains_point(&position)) {
                            self.apply_event(GameEvent::FoodSpawned { position }, Some(&mut out));
                        }
                    }
                }

                // Increment tick
                self.tick += 1;
            }

            GameCommand::Turn { snake_id, direction } => {
                let snake = self.get_snake_mut(snake_id)?;
                if snake.is_alive && snake.direction != direction {
                    self.apply_event(GameEvent::SnakeTurned { snake_id, direction }, Some(&mut out));
                }
            }

            GameCommand::PositionQueueReplace { snake_id, positions } => {
                if let Ok(snake) = self.get_snake_mut(snake_id) {
                    self.apply_event(GameEvent::PositionQueueUpdate { snake_id, positions }, Some(&mut out));
                }
            }
            
            GameCommand::RequestSnapshot => {
                // This command doesn't change state, just triggers a snapshot event
                // The snapshot will be sent by the game loop
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
                if let Ok(snake) = self.get_snake_mut(snake_id) {
                    snake.direction = direction;
                }
            }

            GameEvent::SnakeDied { snake_id } => {
                if let Ok(snake) = self.get_snake_mut(snake_id) {
                    snake.is_alive = false;
                }
            }

            GameEvent::FoodSpawned { position } => {
                if !self.has_food(&position) {
                    self.arena.food.push(position);
                }
            }

            GameEvent::FoodEaten { snake_id, position } => {
                let removed = self.remove_food(&position);
                if let Ok(snake) = self.get_snake_mut(snake_id) {
                    if removed {
                        snake.food += 1;
                    }
                }
            }

            GameEvent::PositionQueueUpdate { snake_id, positions } => {
                if let Ok(_) = self.get_snake_mut(snake_id) {
                    self.position_queues.insert(snake_id, positions);
                }
            }

            GameEvent::CommandPendingOnServer { command_message } => { }
        }

    }
}
