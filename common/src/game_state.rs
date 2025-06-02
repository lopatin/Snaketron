use std::collections::{BinaryHeap, HashMap, HashSet};
use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};
use crate::{Direction, Player, Position, Snake};
use crate::util::PseudoRandom;

const DEFAULT_SNAKE_LENGTH: usize = 3;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum GameCommand {
    // User command for movement
    Turn { snake_id: u32, direction: Direction },
    
    // System command for failover
    UpdateStatus { status: GameStatus },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GameEventMessage {
    pub game_id: u32,
    pub tick: u32,
    pub user_id: Option<u32>,
    pub event: GameEvent,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum GameEvent {
    SnakeTurned { snake_id: u32, direction: Direction },
    SnakeDied { snake_id: u32 },
    FoodSpawned { position: Position },
    FoodEaten { snake_id: u32, position: Position },
    Snapshot { game_state: GameState },
    CommandScheduled { command_message: GameCommandMessage },
    PlayerJoined { user_id: u32, snake_id: u32 },
    StatusUpdated { status: GameStatus },
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
    Stopped,
    Started { server_id: u64 },
    Complete { winning_snake_id: Option<u32> },
}

impl GameType {
    pub fn is_duel(&self) -> bool {
        self == &GameType::TeamMatch { per_team: 1 }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CommandQueue {
    queue: BinaryHeap<GameCommandMessage>,
    active_ids: HashSet<CommandId>,
    tombstone_ids: HashSet<CommandId>,
}

impl CommandQueue {
    pub fn new() -> Self {
        CommandQueue {
            queue: BinaryHeap::new(),
            active_ids: HashSet::new(),
            tombstone_ids: HashSet::new(),
        }
    }
    
    pub fn push(&mut self, command_message: GameCommandMessage) {
        if self.active_ids.insert(command_message.id().clone()) {
            self.queue.push(command_message.clone());
            self.tombstone_ids.remove(command_message.id());

            // Delete the non-server-sent command from the queue.
            if command_message.command_id_server.is_some()
                && self.active_ids.contains(&command_message.command_id_client) {
                self.tombstone_ids.insert(command_message.command_id_client);
            }
        }
    }
    
    pub fn pop(&mut self, max_tick: u32) -> Option<GameCommandMessage> {
        if let Some(command_message) = self.queue.peek() {
            if command_message.tick() > max_tick {
                return None; // No commands for this tick
            }
        }
        
        if let Some(command_message) = self.queue.pop() {
            let id = command_message.id();
            self.active_ids.remove(id);
            if self.tombstone_ids.remove(id) {
                // Ignore the command if it's a tombstone. 
                // Continue popping the next command.
                self.pop(max_tick)
            } else {
                Some(command_message)
            }
        } else {
            None
        }
    }
}


// Serializable state for snapshots
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GameState {
    pub tick: u32,
    pub status: GameStatus,
    pub arena: Arena,
    pub game_type: GameType,
    pub properties: GameProperties,
    pub command_queue: CommandQueue,
    // Players by user_id
    pub players: HashMap<u32, Player>,
    pub rng: Option<PseudoRandom>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct CommandId {
    pub tick: u32,
    pub user_id: u32,
    pub sequence_number: u32,
}

impl Ord for CommandId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.tick, self.user_id, self.sequence_number)
            .cmp(&(other.tick, other.user_id, other.sequence_number))
    }
}

impl PartialOrd for CommandId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Wrapper for BinaryHeap to order commands by their intended execution tick.
#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct GameCommandMessage {
    pub command_id_client: CommandId,
    pub command_id_server: Option<CommandId>,
    pub command: GameCommand,
}

impl GameCommandMessage {
    pub fn tick(&self) -> u32 {
        self.command_id_server.as_ref().map_or(self.command_id_client.tick, |id| id.tick)
    }
    
    pub fn id(&self) -> &CommandId {
        self.command_id_server.as_ref().unwrap_or(&self.command_id_client)
    }
}

impl Ord for GameCommandMessage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.command_id_server.as_ref().unwrap_or(&self.command_id_client)
            .cmp(other.command_id_server.as_ref().unwrap_or(&self.command_id_client))
    }
}

impl PartialOrd for GameCommandMessage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}


impl GameState {
    pub fn new(
        width: u16, 
        height: u16, 
        game_type: GameType, 
        rng_seed: Option<u64>
    ) -> Self {
        GameState {
            tick: 0,
            status: GameStatus::Stopped,
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
            command_queue: CommandQueue::new(),
            players: HashMap::new(),
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
    
    pub fn schedule_command(&mut self, command_message: &GameCommandMessage) {
        self.apply_event(GameEvent::CommandScheduled { command_message: command_message.clone() }, None);
    }
    
    pub fn join(&mut self, _user_id: u32) {
    }
    
    pub fn tick_forward(&mut self) -> Result<Vec<GameEvent>> {

        let mut out: Vec<GameEvent> = Vec::new();
        
        // Emit snapshot on first tick
        if self.tick == 0 {
            out.push(GameEvent::Snapshot { game_state: self.clone() });
        }
        
        // Exec commands in the queue until the only ones left are for after this tick
        while let Some(command_message) = self.command_queue.pop(self.tick) {
            if let Ok(events) = self.exec_command(command_message.command) {
                out.extend(events);
            }
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
        
        Ok(out)
    }

    fn exec_command(&mut self, command: GameCommand) -> Result<Vec<GameEvent>> {
        let mut out: Vec<GameEvent> = Vec::new();
        match command {
            GameCommand::Turn { snake_id, direction } => {
                let snake = self.get_snake_mut(snake_id)?;
                if snake.is_alive && snake.direction != direction {
                    self.apply_event(GameEvent::SnakeTurned { snake_id, direction }, Some(&mut out));
                }
            }
            GameCommand::UpdateStatus { .. } => {}
        }

        Ok(out)
    }

    // Must be idempotent so that Raft can reapply events if needed.
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

            GameEvent::CommandScheduled { command_message } => {
                self.command_queue.push(command_message);
            }
            
            GameEvent::PlayerJoined { .. } => {}
            
            GameEvent::StatusUpdated { status } => {
                self.status = status;
            }
        }

    }
}

