use std::collections::{BinaryHeap, HashMap, HashSet};
use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};
use crate::{Direction, Player, Position, Snake};
use crate::util::PseudoRandom;

const DEFAULT_SNAKE_LENGTH: usize = 4;

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
    // PlayerJoined { user_id: u32, snake_id: u32 },
    StatusUpdated { status: GameStatus },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Arena {
    pub width: u16,
    pub height: u16,
    pub snakes: Vec<Snake>,
    pub food: Vec<Position>,
}

impl Arena {
    pub fn add_snake(&mut self, snake: Snake) -> Result<u32> {
        if self.snakes.len() >= u32::MAX as usize {
            return Err(anyhow::anyhow!("Arena is full, cannot add more snakes"));
        }
        let id = self.snakes.len() as u32;
        self.snakes.push(snake);
        Ok(id)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct GameProperties {
    pub available_food_target: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct CustomGameSettings {
    pub arena_width: u16,
    pub arena_height: u16,
    pub tick_duration_ms: u16,
    pub food_spawn_rate: f32,  // food per minute
    pub max_players: u8,
    pub game_mode: GameMode,
    pub is_private: bool,
    pub allow_spectators: bool,
    pub snake_start_length: u8,
    pub tactical_mode: bool,  // vs classic mode
}

impl Default for CustomGameSettings {
    fn default() -> Self {
        CustomGameSettings {
            arena_width: 40,
            arena_height: 40,
            tick_duration_ms: 300,
            food_spawn_rate: 3.0,
            max_players: 4,
            game_mode: GameMode::FreeForAll { max_players: 4 },
            is_private: true,
            allow_spectators: true,
            snake_start_length: 4,
            tactical_mode: false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum GameMode {
    Solo,  // Practice mode - just one player
    Duel,  // 1v1
    FreeForAll { max_players: u8 },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum GameType {
    TeamMatch { per_team: u8 },
    FreeForAll { max_players: u8 },
    Custom { settings: CustomGameSettings },
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
    // Custom game fields
    pub game_code: Option<String>,
    pub host_user_id: Option<u32>,
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
        // Calculate food target based on custom settings or defaults
        let available_food_target = match &game_type {
            GameType::Custom { settings } => {
                // Convert food per minute to approximate target count
                // Assuming 300ms ticks (3.33 ticks/sec), we get ~200 ticks/min
                // So food_target = food_spawn_rate * arena_size / 200
                let arena_size = (settings.arena_width * settings.arena_height) as f32;
                let base_target = (settings.food_spawn_rate * arena_size / 1600.0).max(1.0);
                base_target.round() as usize
            },
            _ => 5, // Default for non-custom games
        };
        
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
                available_food_target,
            },
            command_queue: CommandQueue::new(),
            players: HashMap::new(),
            rng: rng_seed.map(PseudoRandom::new),
            game_code: None,
            host_user_id: None,
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
    
    fn calculate_starting_positions(&self, player_count: usize) -> Vec<(Position, Direction)> {
        let mut positions = Vec::new();
        let arena_width = self.arena.width as i16;
        let arena_height = self.arena.height as i16;
        
        // Get snake length from custom settings or use default
        let snake_length = match &self.game_type {
            GameType::Custom { settings } => settings.snake_start_length as i16,
            _ => DEFAULT_SNAKE_LENGTH as i16,
        };
        
        match player_count {
            0 => {},
            1 => {
                // Single snake starts on the right, facing left
                let x = arena_width - snake_length - 1;
                let y = arena_height / 2;
                positions.push((Position { x, y }, Direction::Left));
            },
            2 => {
                // Two snakes start on opposite sides, facing each other
                let y = arena_height / 2;
                
                // Right side, facing left
                let x_right = arena_width - snake_length - 1;
                positions.push((Position { x: x_right, y }, Direction::Left));
                
                // Left side, facing right
                let x_left = snake_length;
                positions.push((Position { x: x_left, y }, Direction::Right));
            },
            _ => {
                // More than 2 players: arranged in two columns facing each other
                let left_count = (player_count + 1) / 2;
                let right_count = player_count / 2;
                
                // Calculate vertical spacing
                let vertical_margin = 2;
                let usable_height = arena_height - 2 * vertical_margin;
                
                // Left column (facing right)
                let x_left = snake_length;
                for i in 0..left_count {
                    let y = if left_count == 1 {
                        arena_height / 2
                    } else {
                        vertical_margin + (i as i16 * usable_height) / (left_count - 1) as i16
                    };
                    positions.push((Position { x: x_left, y }, Direction::Right));
                }
                
                // Right column (facing left)
                let x_right = arena_width - snake_length - 1;
                for i in 0..right_count {
                    let y = if right_count == 1 {
                        arena_height / 2
                    } else {
                        vertical_margin + (i as i16 * usable_height) / (right_count - 1) as i16
                    };
                    positions.push((Position { x: x_right, y }, Direction::Left));
                }
            }
        }
        
        positions
    }

    pub fn add_player(&mut self, user_id: u32) -> Result<Player> {
        if self.players.contains_key(&user_id) {
            return Err(anyhow::anyhow!("Player with user_id {} already exists", user_id));
        }

        // Only rearrange players on tick 0
        if self.tick != 0 {
            return Err(anyhow::anyhow!("Cannot add player after the game has started"));
        }

        // Add new player first with temporary position
        let snake = Snake {
            body: vec![Position { x: 0, y: 0 }, Position { x: 0, y: 0 }],
            direction: Direction::Right,
            is_alive: true,
            food: 0,
        };

        let snake_id = self.arena.add_snake(snake)?;
        let player = Player { user_id, snake_id };
        self.players.insert(user_id, player.clone());

        // Calculate starting positions for all players
        let player_count = self.players.len();
        let starting_positions = self.calculate_starting_positions(player_count);

        // Get snake length from custom settings or use default
        let snake_length = match &self.game_type {
            GameType::Custom { settings } => settings.snake_start_length as usize,
            _ => DEFAULT_SNAKE_LENGTH,
        };
        
        // Rearrange all snakes to their starting positions
        for (idx, (player_id, player)) in self.players.iter().enumerate() {
            if idx < starting_positions.len() {
                let (head_pos, direction) = &starting_positions[idx];
                let snake = &mut self.arena.snakes[player.snake_id as usize];
                
                // Build compressed snake body: just head and tail for a straight snake
                let tail_pos = match direction {
                    Direction::Left => Position { 
                        x: head_pos.x + (snake_length - 1) as i16, 
                        y: head_pos.y 
                    },
                    Direction::Right => Position { 
                        x: head_pos.x - (snake_length - 1) as i16, 
                        y: head_pos.y 
                    },
                    Direction::Up => Position { 
                        x: head_pos.x, 
                        y: head_pos.y + (snake_length - 1) as i16 
                    },
                    Direction::Down => Position { 
                        x: head_pos.x, 
                        y: head_pos.y - (snake_length - 1) as i16 
                    },
                };
                
                snake.body = vec![*head_pos, tail_pos];
                snake.direction = *direction;
            }
        }
        
        Ok(player)
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
                if !(head.x >= 0 && head.x < width && head.y >= 0 && head.y < height) {
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

        // Check if game should end (only one or no snakes alive)
        let alive_snakes: Vec<u32> = self.arena.snakes
            .iter()
            .enumerate()
            .filter(|(_, snake)| snake.is_alive)
            .map(|(idx, _)| idx as u32)
            .collect();
        
        if alive_snakes.len() <= 1 && matches!(self.status, GameStatus::Started { .. }) {
            let winning_snake_id = alive_snakes.first().copied();
            self.apply_event(
                GameEvent::StatusUpdated { 
                    status: GameStatus::Complete { winning_snake_id } 
                },
                Some(&mut out)
            );
        }

        // Increment tick
        self.tick += 1;
        
        Ok(out)
    }

    fn exec_command(&mut self, command: GameCommand) -> Result<Vec<GameEvent>> {
        let mut out: Vec<GameEvent> = Vec::new();
        match command {
            GameCommand::Turn { snake_id, direction } => {
                // Check if tactical mode is enabled before borrowing snake mutably
                let is_tactical = match &self.game_type {
                    GameType::Custom { settings } => settings.tactical_mode,
                    _ => false,
                };
                
                // Get current snake state
                let snake = self.arena.snakes.get(snake_id as usize)
                    .context("Snake not found")?;
                
                if snake.is_alive && snake.direction != direction {
                    // In tactical mode, prevent 180-degree turns
                    if is_tactical && snake.direction.is_opposite(&direction) {
                        // Ignore the command - cannot turn 180 degrees in tactical mode
                        return Ok(out);
                    }
                    
                    self.apply_event(GameEvent::SnakeTurned { snake_id, direction }, Some(&mut out));
                }
            }
            GameCommand::UpdateStatus { .. } => {}
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

            GameEvent::CommandScheduled { command_message } => {
                self.command_queue.push(command_message);
            }
            
            GameEvent::StatusUpdated { status } => {
                self.status = status;
            }
        }

    }
}

