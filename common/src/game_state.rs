use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};
use crate::{Direction, Player, Position, Snake, DEFAULT_CUSTOM_GAME_TICK_MS, DEFAULT_FOOD_TARGET, DEFAULT_TICK_INTERVAL_MS};
use crate::util::PseudoRandom;
use log::debug;

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
    pub sequence: u64,
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
    pub tick_duration_ms: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct CustomGameSettings {
    pub arena_width: u16,
    pub arena_height: u16,
    pub tick_duration_ms: u32,
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
            tick_duration_ms: DEFAULT_CUSTOM_GAME_TICK_MS,
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum SoloMode {
    Classic,   // Classic snake movement
    Tactical,  // Enhanced movement (no 180-degree turns)
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum GameType {
    Solo,
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
    
    pub fn is_solo(&self) -> bool {
        match self {
            GameType::Custom { settings } => settings.game_mode == GameMode::Solo,
            _ => false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CommandQueue {
    queue: BinaryHeap<Reverse<GameCommandMessage>>,
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
    
    pub fn has_commands_for_tick(&self, tick: u32) -> bool {
        if let Some(command_message) = self.queue.peek() {
            command_message.0.tick() <= tick
        } else {
            false
        }
    }
    
    pub fn push(&mut self, command_message: GameCommandMessage) {
        // debug!("CommandQueue::push: Command added to queue");
        eprintln!("COMMON DEBUG: Command added to queue: {:?}", command_message);
        self.queue.push(Reverse(command_message.clone()));

        // Delete the non-server-sent command from the queue.
        if command_message.command_id_server.is_some() {
            // debug!("CommandQueue::push: Tombstoning client command {:?}", command_message.command_id_client);
            eprintln!("COMMON DEBUG: Tombstoning client command {:?}", command_message.command_id_client);
            self.tombstone_ids.insert(command_message.command_id_client);
        }
    }
    
    pub fn pop(&mut self, max_tick: u32) -> Option<GameCommandMessage> {
        // debug!("CommandQueue::pop: Called with max_tick {}", max_tick);
        eprintln!("COMMON DEBUG: CommandQueue::pop called with max_tick {}", max_tick);
        if let Some(Reverse(command_message)) = self.queue.peek() {
            // debug!("CommandQueue::pop: Peeked command tick: {}, max_tick: {}", command_message.tick(), max_tick);
            eprintln!("COMMON DEBUG: Peeked command tick: {}, max_tick: {}", command_message.tick(), max_tick);
            if command_message.tick() > max_tick {
                // debug!("CommandQueue::pop: No commands ready for this tick");
                eprintln!("COMMON DEBUG: No commands ready for this tick");
                return None; // No commands for this tick
            }
        }
        
        if let Some(Reverse(command_message)) = self.queue.pop() {
            // debug!("CommandQueue::pop: Popped command: {:?}", command_message);
            eprintln!("COMMON DEBUG: Popped command: {:?}", command_message);
            if command_message.command_id_server.is_none() && self.tombstone_ids.remove(&command_message.command_id_client) {
                eprintln!("COMMON DEBUG: Command {:?} is tombstoned, skipping and popping next", command_message.command_id_client);
                // Ignore the command if it's a tombstone. 
                // Continue popping the next command.
                self.pop(max_tick)
            } else {
                // debug!("CommandQueue::pop: Returning command: {:?}", command_message);
                eprintln!("COMMON DEBUG: Returning command: {:?}", command_message);
                Some(command_message)
            }
        } else {
            // debug!("CommandQueue::pop: Queue is empty");
            eprintln!("COMMON DEBUG: CommandQueue::pop: Queue is empty");
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
    // Game start timestamp in milliseconds
    pub start_ms: i64,
    // Event sequence number for this game
    pub event_sequence: u64,
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
            .cmp(other.command_id_server.as_ref().unwrap_or(&other.command_id_client))
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
        rng_seed: Option<u64>,
        start_ms: i64
    ) -> Self {
        let properties = match &game_type { 
            GameType::Custom { settings } => GameProperties {
                available_food_target: DEFAULT_FOOD_TARGET,
                tick_duration_ms: settings.tick_duration_ms,
            },
            _ => GameProperties {
                available_food_target: DEFAULT_FOOD_TARGET,
                tick_duration_ms: DEFAULT_TICK_INTERVAL_MS,
            },
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
            properties,
            command_queue: CommandQueue::new(),
            players: HashMap::new(),
            rng: rng_seed.map(PseudoRandom::new),
            game_code: None,
            host_user_id: None,
            start_ms,
            event_sequence: 0,
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
    
    pub fn has_scheduled_commands(&self, tick: u32) -> bool {
        self.command_queue.has_commands_for_tick(tick)
    }
    
    pub fn join(&mut self, _user_id: u32) {
    }
    
    pub fn tick_forward(&mut self) -> Result<Vec<(u64, GameEvent)>> {

        let mut out: Vec<(u64, GameEvent)> = Vec::new();

        // Emit snapshot on first tick
        if self.tick == 0 {
            self.event_sequence += 1;
            out.push((self.event_sequence, GameEvent::Snapshot { game_state: self.clone() }));
        }
       
        // Exec commands in the queue until the only ones left are for after this tick
        // debug!("tick_forward: Checking for commands at tick {}", self.tick);
        eprintln!("COMMON DEBUG: tick_forward checking commands at tick {}", self.tick);
        while let Some(command_message) = self.command_queue.pop(self.tick) {
            debug!("tick_forward: Popped command from queue: {:?}", command_message);
            eprintln!("COMMON DEBUG: Popped command: {:?}", command_message);
            match self.exec_command(command_message.command) {
                Ok(events) => {
                    debug!("tick_forward: exec_command returned {} events", events.len());
                    eprintln!("COMMON DEBUG: exec_command returned {} events", events.len());
                    out.extend(events);
                }
                Err(e) => {
                    debug!("tick_forward: exec_command failed with error: {:?}", e);
                    eprintln!("COMMON DEBUG: exec_command error: {:?}", e);
                }
            }
        }
        // debug!("tick_forward: Finished processing commands for tick {}", self.tick);

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
        
        // For solo games, only end when no snakes are alive
        // For multiplayer games, end when 1 or fewer snakes are alive
        let should_end = if self.game_type.is_solo() {
            alive_snakes.is_empty()
        } else {
            alive_snakes.len() <= 1
        };
        
        if should_end && matches!(self.status, GameStatus::Started { .. }) {
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

    fn exec_command(&mut self, command: GameCommand) -> Result<Vec<(u64, GameEvent)>> {
        debug!("exec_command: Entering with command {:?}", command);
        eprintln!("COMMON DEBUG: exec_command called with {:?}", command);
        let mut out: Vec<(u64, GameEvent)> = Vec::new();
        match command {
            GameCommand::Turn { snake_id, direction } => {
                debug!("exec_command: Processing Turn command - snake_id: {}, direction: {:?}", snake_id, direction);
                eprintln!("COMMON DEBUG: Turn command - snake_id: {}, direction: {:?}", snake_id, direction);
                
                // Check if tactical mode is enabled before borrowing snake mutably
                let is_tactical = match &self.game_type {
                    GameType::Custom { settings } => settings.tactical_mode,
                    _ => false,
                };
                debug!("exec_command: Tactical mode: {}", is_tactical);
                
                // Get current snake state
                let snake = self.arena.snakes.get(snake_id as usize)
                    .context("Snake not found")?;
                
                debug!("exec_command: Snake {} state - alive: {}, current_direction: {:?}, requested_direction: {:?}", 
                      snake_id, snake.is_alive, snake.direction, direction);
                eprintln!("COMMON DEBUG: Snake {} - alive: {}, current: {:?}, requested: {:?}", 
                         snake_id, snake.is_alive, snake.direction, direction);
                
                if snake.is_alive && snake.direction != direction {
                    debug!("exec_command: Snake is alive and direction is different");
                    
                    // In tactical mode, prevent 180-degree turns
                    if is_tactical && snake.direction.is_opposite(&direction) {
                        debug!("exec_command: Ignoring command - 180-degree turn attempted in tactical mode");
                        eprintln!("COMMON DEBUG: Ignoring 180-degree turn in tactical mode");
                        // Ignore the command - cannot turn 180 degrees in tactical mode
                        return Ok(out);
                    }
                    
                    debug!("exec_command: Generating SnakeTurned event for snake {}", snake_id);
                    eprintln!("COMMON DEBUG: Generating SnakeTurned event for snake {}", snake_id);
                    self.apply_event(GameEvent::SnakeTurned { snake_id, direction }, Some(&mut out));
                    debug!("exec_command: SnakeTurned event applied successfully");
                } else {
                    if !snake.is_alive {
                        debug!("exec_command: Ignoring command - snake {} is dead", snake_id);
                        eprintln!("COMMON DEBUG: Ignoring - snake {} is dead", snake_id);
                    } else if snake.direction == direction {
                        debug!("exec_command: Ignoring command - snake {} already facing {:?}", snake_id, direction);
                        eprintln!("COMMON DEBUG: Ignoring - snake {} already facing {:?}", snake_id, direction);
                    }
                }
            }
            GameCommand::UpdateStatus { .. } => {
                debug!("exec_command: Processing UpdateStatus command");
            }
        }

        debug!("exec_command: Returning {} events", out.len());
        eprintln!("COMMON DEBUG: exec_command returning {} events", out.len());
        Ok(out)
    }

    pub fn apply_event(&mut self, event: GameEvent, out: Option<&mut Vec<(u64, GameEvent)>>) {
        if let Some(out) = out {
            self.event_sequence += 1;
            out.push((self.event_sequence, event.clone()));
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BinaryHeap;

    fn create_command_id(tick: u32, user_id: u32, seq: u32) -> CommandId {
        CommandId {
            tick,
            user_id,
            sequence_number: seq,
        }
    }

    fn create_command_message(tick: u32, user_id: u32, seq: u32, with_server_id: bool) -> GameCommandMessage {
        let client_id = create_command_id(tick, user_id, seq);
        let server_id = if with_server_id {
            Some(create_command_id(tick, user_id, seq))
        } else {
            None
        };
        
        GameCommandMessage {
            command_id_client: client_id,
            command_id_server: server_id,
            command: GameCommand::Turn {
                snake_id: 1,
                direction: Direction::Up,
            },
        }
    }

    #[test]
    fn test_command_queue_basic_push_pop() {
        let mut queue = CommandQueue::new();
        
        // Push a command
        let cmd = create_command_message(10, 1, 1, false);
        queue.push(cmd.clone());
        
        // Pop should return the command
        let popped = queue.pop(10);
        assert!(popped.is_some());
        assert_eq!(popped.unwrap().command_id_client, cmd.command_id_client);
        
        // Queue should now be empty
        assert!(queue.pop(10).is_none());
    }

    #[test]
    fn test_command_queue_tick_ordering() {
        let mut queue = CommandQueue::new();
        
        // Push commands with different ticks
        // Note: sequence numbers should increase with tick to maintain consistent ordering
        let cmd1 = create_command_message(20, 1, 3, false);
        let cmd2 = create_command_message(10, 1, 1, false);
        let cmd3 = create_command_message(15, 1, 2, false);
        
        println!("cmd1 (tick 20): {:?}", cmd1.command_id_client);
        println!("cmd2 (tick 10): {:?}", cmd2.command_id_client);
        println!("cmd3 (tick 15): {:?}", cmd3.command_id_client);
        
        // Test comparisons
        println!("cmd2 < cmd1: {}", cmd2 < cmd1);
        println!("cmd2 < cmd3: {}", cmd2 < cmd3);
        println!("cmd3 < cmd1: {}", cmd3 < cmd1);
        
        use std::cmp::Reverse;
        println!("Reverse(cmd2) > Reverse(cmd1): {}", Reverse(cmd2.clone()) > Reverse(cmd1.clone()));
        println!("Reverse(cmd2) > Reverse(cmd3): {}", Reverse(cmd2.clone()) > Reverse(cmd3.clone()));
        println!("Reverse(cmd3) > Reverse(cmd1): {}", Reverse(cmd3.clone()) > Reverse(cmd1.clone()));
        
        queue.push(cmd1);
        queue.push(cmd2);
        queue.push(cmd3);
        
        // Should pop in tick order (10, 15, 20)
        let cmd1 = queue.pop(25).unwrap();
        assert_eq!(cmd1.tick(), 10);
        
        let cmd2 = queue.pop(25).unwrap();
        assert_eq!(cmd2.tick(), 15);
        
        let cmd3 = queue.pop(25).unwrap();
        assert_eq!(cmd3.tick(), 20);
    }

    #[test]
    fn test_command_queue_max_tick_filtering() {
        let mut queue = CommandQueue::new();
        
        // Push commands with different ticks
        queue.push(create_command_message(10, 1, 1, false));
        queue.push(create_command_message(20, 1, 2, false));
        queue.push(create_command_message(30, 1, 3, false));
        
        // Pop with max_tick = 15 should only return tick 10
        let cmd1 = queue.pop(15);
        assert!(cmd1.is_some());
        assert_eq!(cmd1.unwrap().tick(), 10);
        
        // Pop again with max_tick = 15 should return None
        assert!(queue.pop(15).is_none());
        
        // Pop with max_tick = 25 should return tick 20
        let cmd2 = queue.pop(25);
        assert!(cmd2.is_some());
        assert_eq!(cmd2.unwrap().tick(), 20);
    }

    #[test]
    fn test_command_queue_tombstoning() {
        let mut queue = CommandQueue::new();
        
        // Push client command
        let client_cmd = create_command_message(10, 1, 1, false);
        queue.push(client_cmd.clone());
        
        // Push server command with same client_id - should tombstone the client command
        let mut server_cmd = client_cmd.clone();
        server_cmd.command_id_server = Some(create_command_id(10, 1, 1));
        queue.push(server_cmd.clone());
        
        // Pop should return the server command (not the tombstoned client command)
        let popped = queue.pop(10).unwrap();
        assert!(popped.command_id_server.is_some());
        
        // Queue should now be empty (client command was tombstoned)
        assert!(queue.pop(10).is_none());
    }

    #[test]
    fn test_command_queue_multiple_tombstoning() {
        let mut queue = CommandQueue::new();
        
        // Push multiple client commands
        queue.push(create_command_message(10, 1, 1, false));
        queue.push(create_command_message(10, 1, 2, false));
        queue.push(create_command_message(10, 1, 3, false));
        
        // Push server command that tombstones the second client command
        let mut server_cmd = create_command_message(10, 1, 2, false);
        server_cmd.command_id_server = Some(create_command_id(10, 1, 2));
        queue.push(server_cmd);
        
        // Pop should return commands in order, skipping the tombstoned one
        let cmd1 = queue.pop(10).unwrap();
        assert_eq!(cmd1.command_id_client.sequence_number, 1);
        assert!(cmd1.command_id_server.is_none());
        
        let cmd2 = queue.pop(10).unwrap();
        assert_eq!(cmd2.command_id_client.sequence_number, 2);
        assert!(cmd2.command_id_server.is_some());
        
        let cmd3 = queue.pop(10).unwrap();
        assert_eq!(cmd3.command_id_client.sequence_number, 3);
        assert!(cmd3.command_id_server.is_none());
    }

    #[test]
    fn test_command_queue_deduplication() {
        let mut queue = CommandQueue::new();
        
        // Push same command twice (should be deduped using active_ids)
        let cmd = create_command_message(10, 1, 1, false);
        queue.push(cmd.clone());
        queue.push(cmd.clone());
        
        // Should be able to pop twice (no deduplication implemented)
        assert!(queue.pop(10).is_some());
        assert!(queue.pop(10).is_some());
        assert!(queue.pop(10).is_none());
    }

    #[test]
    fn test_command_queue_same_tick_ordering() {
        let mut queue = CommandQueue::new();
        
        // Push commands with same tick but different users/sequences
        queue.push(create_command_message(10, 2, 1, false));
        queue.push(create_command_message(10, 1, 2, false));
        queue.push(create_command_message(10, 1, 1, false));
        
        // Should pop in order: (tick=10, user=1, seq=1), (tick=10, user=1, seq=2), (tick=10, user=2, seq=1)
        let cmd1 = queue.pop(10).unwrap();
        assert_eq!(cmd1.command_id_client.user_id, 1);
        assert_eq!(cmd1.command_id_client.sequence_number, 1);
        
        let cmd2 = queue.pop(10).unwrap();
        assert_eq!(cmd2.command_id_client.user_id, 1);
        assert_eq!(cmd2.command_id_client.sequence_number, 2);
        
        let cmd3 = queue.pop(10).unwrap();
        assert_eq!(cmd3.command_id_client.user_id, 2);
        assert_eq!(cmd3.command_id_client.sequence_number, 1);
    }

    #[test]
    fn test_command_queue_server_tick_override() {
        let mut queue = CommandQueue::new();
        
        // Create command with client tick 10 but server tick 15
        let mut cmd = create_command_message(10, 1, 1, false);
        cmd.command_id_server = Some(create_command_id(15, 1, 1));
        queue.push(cmd.clone());
        
        // Should not be available at tick 10
        assert!(queue.pop(10).is_none());
        
        // Should be available at tick 15
        let popped = queue.pop(15).unwrap();
        assert_eq!(popped.tick(), 15);
    }

    #[test]
    fn test_command_queue_empty_operations() {
        let mut queue = CommandQueue::new();
        
        // Pop from empty queue
        assert!(queue.pop(100).is_none());
        
        // Push and pop, then try again
        queue.push(create_command_message(10, 1, 1, false));
        assert!(queue.pop(10).is_some());
        assert!(queue.pop(10).is_none());
    }

    #[test]
    fn test_command_id_ordering() {
        // Test that CommandId ordering works correctly
        let id1 = create_command_id(10, 1, 1);
        let id2 = create_command_id(10, 1, 2);
        let id3 = create_command_id(10, 2, 1);
        let id4 = create_command_id(11, 1, 1);
        
        assert!(id1 < id2);  // Same tick and user, lower sequence
        assert!(id1 < id3);  // Same tick, lower user id
        assert!(id1 < id4);  // Lower tick
    }

    #[test]
    fn test_binary_heap_with_reverse() {
        use std::collections::BinaryHeap;
        use std::cmp::Reverse;
        
        // Create a heap
        let mut heap = BinaryHeap::new();
        
        // Push some numbers wrapped in Reverse
        heap.push(Reverse(20));
        heap.push(Reverse(10));
        heap.push(Reverse(15));
        
        // Pop should give us 10, 15, 20 (min-heap behavior)
        assert_eq!(heap.pop().unwrap().0, 10);
        assert_eq!(heap.pop().unwrap().0, 15);
        assert_eq!(heap.pop().unwrap().0, 20);
    }
    
    #[test]
    fn test_game_command_message_heap() {
        use std::collections::BinaryHeap;
        use std::cmp::Reverse;
        
        // Create messages with different ticks but same user and sequence
        let msg1 = create_command_message(20, 1, 1, false);
        let msg2 = create_command_message(10, 1, 1, false);
        let msg3 = create_command_message(15, 1, 1, false);
        
        // Test direct comparison
        println!("msg1: {:?}", msg1);
        println!("msg2: {:?}", msg2);
        println!("msg1.command_id_client: {:?}", msg1.command_id_client);
        println!("msg2.command_id_client: {:?}", msg2.command_id_client);
        println!("msg2 (tick 10) < msg1 (tick 20): {}", msg2 < msg1);
        println!("msg2.cmp(&msg1): {:?}", msg2.cmp(&msg1));
        println!("msg2.command_id_client.cmp(&msg1.command_id_client): {:?}", msg2.command_id_client.cmp(&msg1.command_id_client));
        
        // Push to heap
        let mut heap = BinaryHeap::new();
        heap.push(Reverse(msg1.clone()));
        heap.push(Reverse(msg2.clone()));
        heap.push(Reverse(msg3.clone()));
        
        // Pop and check order
        let first = heap.pop().unwrap().0;
        println!("First popped: tick = {}", first.tick());
        assert_eq!(first.tick(), 10);
        
        let second = heap.pop().unwrap().0;
        println!("Second popped: tick = {}", second.tick());
        assert_eq!(second.tick(), 15);
        
        let third = heap.pop().unwrap().0;
        println!("Third popped: tick = {}", third.tick());
        assert_eq!(third.tick(), 20);
    }

    #[test]
    fn test_simple_message_comparison() {
        // Create two messages with different ticks but same user and sequence
        let msg_tick10 = create_command_message(10, 1, 1, false);
        let msg_tick20 = create_command_message(20, 1, 1, false);
        
        // Also check if they're actually different
        println!("msg_tick10 == msg_tick20: {}", msg_tick10 == msg_tick20);
        
        // Print debug info
        println!("msg_tick10.command_id_client: {:?}", msg_tick10.command_id_client);
        println!("msg_tick20.command_id_client: {:?}", msg_tick20.command_id_client);
        println!("msg_tick10.id(): {:?}", msg_tick10.id());
        println!("msg_tick20.id(): {:?}", msg_tick20.id());
        println!("msg_tick10.cmp(&msg_tick20): {:?}", msg_tick10.cmp(&msg_tick20));
        println!("msg_tick10.command_id_client.cmp(&msg_tick20.command_id_client): {:?}", 
                 msg_tick10.command_id_client.cmp(&msg_tick20.command_id_client));
        
        // Check the actual comparison being used in Ord
        let id1 = msg_tick10.command_id_server.as_ref().unwrap_or(&msg_tick10.command_id_client);
        let id2 = msg_tick20.command_id_server.as_ref().unwrap_or(&msg_tick20.command_id_client);
        println!("id1: {:?}", id1);
        println!("id2: {:?}", id2);
        println!("id1.cmp(id2): {:?}", id1.cmp(id2));
        
        // Let's manually implement what Ord::cmp should do
        let manual_cmp = msg_tick10.command_id_server.as_ref()
            .unwrap_or(&msg_tick10.command_id_client)
            .cmp(
                msg_tick20.command_id_server.as_ref()
                    .unwrap_or(&msg_tick20.command_id_client)
            );
        println!("Manual cmp result: {:?}", manual_cmp);
        
        // Check Ord trait directly
        use std::cmp::Ord;
        println!("Ord::cmp result: {:?}", Ord::cmp(&msg_tick10, &msg_tick20));
        
        // This test will show us what's actually happening
        if msg_tick10 < msg_tick20 {
            println!("tick 10 < tick 20 (expected behavior)");
        } else if msg_tick10 > msg_tick20 {
            println!("tick 10 > tick 20 (inverted behavior!)");
        } else {
            println!("tick 10 == tick 20 (they're equal?!)");
        }
    }

    #[test]
    fn test_game_command_message_ordering() {
        // Test GameCommandMessage ordering directly
        // Note: Using different sequence numbers to avoid identical commands
        let msg1 = create_command_message(10, 1, 1, false);
        let msg2 = create_command_message(20, 1, 2, false);
        let msg3 = create_command_message(15, 1, 3, false);
        
        // Debug: Let's see what's actually happening
        println!("msg1 (tick 10) < msg2 (tick 20): {}", msg1 < msg2);
        println!("msg1 (tick 10) > msg2 (tick 20): {}", msg1 > msg2);
        println!("msg1.cmp(&msg2): {:?}", msg1.cmp(&msg2));
        
        // Direct comparison - smaller ticks should be less than larger ticks
        assert!(msg1 < msg2);  // tick 10 < tick 20
        assert!(msg1 < msg3);  // tick 10 < tick 15
        assert!(msg3 < msg2);  // tick 15 < tick 20
        
        // Test with server IDs
        let mut msg_with_server = create_command_message(10, 1, 1, false);
        msg_with_server.command_id_server = Some(create_command_id(25, 1, 1));
        
        assert!(msg2 < msg_with_server);  // tick 20 < tick 25 (server tick overrides)
    }

    #[test]
    fn test_reverse_game_command_message_ordering() {
        use std::cmp::Reverse;
        
        // Test GameCommandMessage ordering when wrapped in Reverse
        // Note: Using different sequence numbers to avoid identical commands
        let msg1 = create_command_message(10, 1, 1, false);
        let msg2 = create_command_message(20, 1, 2, false);
        let msg3 = create_command_message(15, 1, 3, false);
        
        // Wrap in Reverse
        let rev1 = Reverse(msg1.clone());
        let rev2 = Reverse(msg2.clone());
        let rev3 = Reverse(msg3.clone());
        
        // Reversed comparison - larger ticks should be "less than" when wrapped in Reverse
        assert!(rev2 < rev1);  // Reverse(tick 20) < Reverse(tick 10)
        assert!(rev3 < rev1);  // Reverse(tick 15) < Reverse(tick 10)
        assert!(rev2 < rev3);  // Reverse(tick 20) < Reverse(tick 15)
        
        // Test in a BinaryHeap to see actual behavior
        let mut heap = BinaryHeap::new();
        heap.push(Reverse(msg2.clone()));
        heap.push(Reverse(msg1.clone()));
        heap.push(Reverse(msg3.clone()));
        
        // Pop should give us the smallest tick first (min-heap behavior)
        let first = heap.pop().unwrap().0;
        assert_eq!(first.tick(), 10);
        
        let second = heap.pop().unwrap().0;
        assert_eq!(second.tick(), 15);
        
        let third = heap.pop().unwrap().0;
        assert_eq!(third.tick(), 20);
    }
}