use crate::util::PseudoRandom;
use crate::{
    DEFAULT_CUSTOM_GAME_TICK_MS, DEFAULT_FOOD_TARGET, DEFAULT_TEAM_TIME_LIMIT_MS,
    DEFAULT_TICK_INTERVAL_MS, Direction, Player, Position, Snake,
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

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
    SnakeTurned {
        snake_id: u32,
        direction: Direction,
    },
    SnakeDied {
        snake_id: u32,
    },
    FoodSpawned {
        position: Position,
    },
    FoodEaten {
        snake_id: u32,
        position: Position,
    },
    Snapshot {
        game_state: GameState,
    },
    CommandScheduled {
        command_message: GameCommandMessage,
    },
    // PlayerJoined { user_id: u32, snake_id: u32 },
    StatusUpdated {
        status: GameStatus,
    },
    ScoreUpdated {
        snake_id: u32,
        score: u32,
    },
    TeamScoreUpdated {
        team_id: TeamId,
        score: u32,
    },

    SnakeRespawned {
        snake_id: u32,
        position: Position,
        direction: Direction,
    },

    // XP event
    XPAwarded {
        player_xp: HashMap<u32, u32>,
    }, // user_id -> xp_gained
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct TeamZoneConfig {
    pub end_zone_depth: u16, // Depth of each end zone (10 cells)
    pub goal_width: u16,     // Width of goal opening in cells
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Copy, Hash, PartialOrd, Ord)]
pub struct TeamId(pub u8);

impl TeamId {
    pub fn new(index: u8) -> Self {
        TeamId(index)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Arena {
    pub width: u16,
    pub height: u16,
    pub snakes: Vec<Snake>,
    pub food: Vec<Position>,
    pub team_zone_config: Option<TeamZoneConfig>, // New field - minimal state
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

    /// Calculate team end zone bounds
    pub fn team_zone_bounds(&self, team_id: TeamId) -> Option<(i16, i16, i16, i16)> {
        self.team_zone_config.as_ref().map(|config| {
            match team_id.0 {
                0 => {
                    // Team 0 zone (left side)
                    (
                        0,
                        config.end_zone_depth as i16 - 1,
                        0,
                        self.height as i16 - 1,
                    )
                }
                1 => {
                    // Team 1 zone (right side)
                    let x_start = self.width as i16 - config.end_zone_depth as i16;
                    (x_start, self.width as i16 - 1, 0, self.height as i16 - 1)
                }
                _ => {
                    // For additional teams, could extend to top/bottom or other zones
                    (
                        0,
                        config.end_zone_depth as i16 - 1,
                        0,
                        self.height as i16 - 1,
                    )
                }
            }
        })
    }

    /// Calculate main field bounds
    pub fn main_field_bounds(&self) -> Option<(i16, i16)> {
        self.team_zone_config.as_ref().map(|config| {
            (
                config.end_zone_depth as i16,
                self.width as i16 - config.end_zone_depth as i16 - 1,
            )
        })
    }

    /// Calculate goal position for a given team
    pub fn goal_bounds(&self, team: TeamId) -> Option<(i16, i16, i16)> {
        self.team_zone_config.as_ref().map(|config| {
            let goal_center = self.height as i16 / 2;
            let half_width = config.goal_width as i16 / 2;
            let y_start = goal_center - half_width;
            let y_end = goal_center + half_width;

            let x_pos = match team.0 {
                0 => config.end_zone_depth as i16 - 1, // Right edge of Team 0 zone
                1 => self.width as i16 - config.end_zone_depth as i16, // Left edge of Team 1 zone
                _ => config.end_zone_depth as i16 - 1, // Default to team 0 position for other teams
            };

            (x_pos, y_start, y_end)
        })
    }

    /// Check if a position is within a wall (not in goal opening)
    pub fn is_wall_position(&self, pos: &Position) -> bool {
        if let Some(config) = &self.team_zone_config {
            // Check if at zone boundary
            let at_team_a_boundary = pos.x == config.end_zone_depth as i16 - 1;
            let at_team_b_boundary = pos.x == self.width as i16 - config.end_zone_depth as i16;

            if at_team_a_boundary || at_team_b_boundary {
                // Check if within goal opening
                if let Some((_x, y_start, y_end)) = self.goal_bounds(if at_team_a_boundary {
                    TeamId(0)
                } else {
                    TeamId(1)
                }) {
                    return pos.y < y_start || pos.y > y_end;
                }
            }
        }
        false
    }

    pub fn is_in_team_base(&self, pos: &Position, team_id: TeamId) -> bool {
        self.team_zone_bounds(team_id)
            .map(|(x_start, x_end, y_start, y_end)| {
                pos.x >= x_start && pos.x <= x_end && pos.y >= y_start && pos.y <= y_end
            })
            .unwrap_or(false)
    }

    pub fn is_in_enemy_base(&self, pos: &Position, team_id: TeamId) -> bool {
        let enemy_team = match team_id.0 {
            0 => TeamId(1),
            1 => TeamId(0),
            _ => TeamId(1),
        };

        self.is_in_team_base(pos, enemy_team)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct GameProperties {
    pub available_food_target: usize,
    pub tick_duration_ms: u32,
    pub time_limit_ms: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct CustomGameSettings {
    pub arena_width: u16,
    pub arena_height: u16,
    pub tick_duration_ms: u32,
    pub food_spawn_rate: f32, // food per minute
    pub max_players: u8,
    pub game_mode: GameMode,
    pub is_private: bool,
    pub allow_spectators: bool,
    pub snake_start_length: u8,
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
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum GameMode {
    Solo, // Practice mode - just one player
    Duel, // 1v1
    FreeForAll { max_players: u8 },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub enum QueueMode {
    Quickmatch,  // Quick casual matches
    Competitive, // Ranked competitive matches
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
        // eprintln!("COMMON DEBUG: Command added to queue: {:?}", command_message);
        self.queue.push(Reverse(command_message.clone()));

        // Delete the non-server-sent command from the queue.
        if command_message.command_id_server.is_some() {
            // debug!("CommandQueue::push: Tombstoning client command {:?}", command_message.command_id_client);
            // eprintln!("COMMON DEBUG: Tombstoning client command {:?}", command_message.command_id_client);
            self.tombstone_ids.insert(command_message.command_id_client);
        }
    }

    pub fn pop(&mut self, max_tick: u32) -> Option<GameCommandMessage> {
        // debug!("CommandQueue::pop: Called with max_tick {}", max_tick);
        // eprintln!("COMMON DEBUG: CommandQueue::pop called with max_tick {}", max_tick);
        if let Some(Reverse(command_message)) = self.queue.peek() {
            // debug!("CommandQueue::pop: Peeked command tick: {}, max_tick: {}", command_message.tick(), max_tick);
            // eprintln!("COMMON DEBUG: Peeked command tick: {}, max_tick: {}", command_message.tick(), max_tick);
            if command_message.tick() > max_tick {
                // debug!("CommandQueue::pop: No commands ready for this tick");
                // eprintln!("COMMON DEBUG: No commands ready for this tick");
                return None; // No commands for this tick
            }
        }

        if let Some(Reverse(command_message)) = self.queue.pop() {
            // debug!("CommandQueue::pop: Popped command: {:?}", command_message);
            // eprintln!("COMMON DEBUG: Popped command: {:?}", command_message);
            if command_message.command_id_server.is_none()
                && self
                    .tombstone_ids
                    .remove(&command_message.command_id_client)
            {
                // eprintln!("COMMON DEBUG: Command {:?} is tombstoned, skipping and popping next", command_message.command_id_client);
                // Ignore the command if it's a tombstone.
                // Continue popping the next command.
                self.pop(max_tick)
            } else {
                // debug!("CommandQueue::pop: Returning command: {:?}", command_message);
                // eprintln!("COMMON DEBUG: Returning command: {:?}", command_message);
                Some(command_message)
            }
        } else {
            // debug!("CommandQueue::pop: Queue is empty");
            // eprintln!("COMMON DEBUG: CommandQueue::pop: Queue is empty");
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
    pub queue_mode: QueueMode,
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
    // Username mappings by user_id
    pub usernames: HashMap<u32, String>,
    // Spectators by user_id (do not have snakes/players)
    pub spectators: HashSet<u32>,
    // Score tracking - snake_id -> score
    pub scores: HashMap<u32, u32>,
    // Team scores for team games - team_id -> score
    pub team_scores: Option<HashMap<TeamId, u32>>,

    // XP tracking
    pub player_xp: HashMap<u32, u32>, // user_id -> xp_gained
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct CommandId {
    pub tick: u32,
    pub user_id: u32,
    pub sequence_number: u32,
}

impl Ord for CommandId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.tick, self.user_id, self.sequence_number).cmp(&(
            other.tick,
            other.user_id,
            other.sequence_number,
        ))
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
        self.command_id_server
            .as_ref()
            .map_or(self.command_id_client.tick, |id| id.tick)
    }

    pub fn id(&self) -> &CommandId {
        self.command_id_server
            .as_ref()
            .unwrap_or(&self.command_id_client)
    }
}

impl Ord for GameCommandMessage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.command_id_server
            .as_ref()
            .unwrap_or(&self.command_id_client)
            .cmp(
                other
                    .command_id_server
                    .as_ref()
                    .unwrap_or(&other.command_id_client),
            )
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
        queue_mode: QueueMode,
        rng_seed: Option<u64>,
        start_ms: i64,
    ) -> Self {
        let (tick_duration_ms, time_limit_ms) = match &game_type {
            GameType::Custom { settings } => (settings.tick_duration_ms, None),
            _ => (
                DEFAULT_TICK_INTERVAL_MS,
                if matches!(&game_type, GameType::TeamMatch { .. }) {
                    Some(DEFAULT_TEAM_TIME_LIMIT_MS)
                } else {
                    None
                },
            ),
        };

        let properties = GameProperties {
            available_food_target: DEFAULT_FOOD_TARGET,
            tick_duration_ms,
            time_limit_ms,
        };

        // Set up team zones for team-based games
        let team_zone_config = match &game_type {
            GameType::TeamMatch { .. } => {
                // Calculate goal width as 20% of arena height
                let goal_width = ((height as f32 * 0.2).round() as u16).max(3);
                // Make sure it's odd for symmetry
                let goal_width = if goal_width % 2 == 0 {
                    goal_width + 1
                } else {
                    goal_width
                };

                Some(TeamZoneConfig {
                    end_zone_depth: 10,
                    goal_width,
                })
            }
            _ => None,
        };

        let team_scores = if matches!(&game_type, GameType::TeamMatch { .. }) {
            let mut scores = HashMap::new();
            scores.insert(TeamId(0), 0);
            scores.insert(TeamId(1), 0);
            Some(scores)
        } else {
            None
        };

        GameState {
            tick: 0,
            status: GameStatus::Stopped,
            arena: Arena {
                width,
                height,
                snakes: Vec::new(),
                food: Vec::new(),
                team_zone_config,
            },
            game_type: game_type.clone(),
            queue_mode,
            properties,
            command_queue: CommandQueue::new(),
            players: HashMap::new(),
            rng: rng_seed.map(PseudoRandom::new),
            game_code: None,
            host_user_id: None,
            start_ms,
            event_sequence: 0,
            usernames: HashMap::new(),
            spectators: HashSet::new(),
            scores: HashMap::new(),
            team_scores,

            player_xp: HashMap::new(),
        }
    }

    pub fn current_tick(&self) -> u32 {
        self.tick
    }

    pub fn is_complete(&self) -> bool {
        matches!(self.status, GameStatus::Complete { .. })
    }

    fn get_snake_mut(&mut self, snake_id: u32) -> Result<&mut Snake> {
        self.arena
            .snakes
            .get_mut(snake_id as usize)
            .context("Snake not found")
    }

    fn iter_snakes(&self) -> impl Iterator<Item = (u32, &Snake)> {
        self.arena
            .snakes
            .iter()
            .enumerate()
            .map(|(id, snake)| (id as u32, snake))
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
        let snake_length = self.starting_snake_length() as i16;

        // For team games, adjust starting positions to be in the main field
        let (left_boundary, right_boundary) =
            if let Some((left, right)) = self.arena.main_field_bounds() {
                (left + 2, right - 2) // Add buffer from walls
            } else {
                (0, arena_width - 1)
            };

        match player_count {
            0 => {}
            1 => {
                // Single snake starts on the right side of main field, facing left
                let x = right_boundary - snake_length;
                let y = arena_height / 2;
                positions.push((Position { x, y }, Direction::Left));
            }
            2 => {
                // Check if this is a TeamMatch (duel) game
                if let GameType::TeamMatch { per_team: 1 } = &self.game_type {
                    // Duel mode: snakes start in their own endzones
                    let y = arena_height / 2;

                    // Team A in left endzone (centered at x=5), facing right toward Team B's goal
                    positions.push((Position { x: 5, y }, Direction::Right));

                    // Team B in right endzone (centered at x=arena_width-5), facing left toward Team A's goal
                    positions.push((
                        Position {
                            x: arena_width - 5,
                            y,
                        },
                        Direction::Left,
                    ));
                } else {
                    // FreeForAll: Two snakes start on opposite sides of main field, facing each other
                    let y = arena_height / 2;

                    // Right side of main field, facing left
                    let x_right = right_boundary - snake_length;
                    positions.push((Position { x: x_right, y }, Direction::Left));

                    // Left side of main field, facing right
                    let x_left = left_boundary + snake_length;
                    positions.push((Position { x: x_left, y }, Direction::Right));
                }
            }
            _ => {
                // More than 2 players: arranged in two columns facing each other
                let left_count = (player_count + 1) / 2;
                let right_count = player_count / 2;

                // Calculate vertical spacing
                let vertical_margin = 2;
                let usable_height = arena_height - 2 * vertical_margin;

                // Left column (facing right) - use main field boundaries
                let x_left = left_boundary + snake_length;
                for i in 0..left_count {
                    let y = if left_count == 1 {
                        arena_height / 2
                    } else {
                        vertical_margin + (i as i16 * usable_height) / (left_count - 1) as i16
                    };
                    positions.push((Position { x: x_left, y }, Direction::Right));
                }

                // Right column (facing left) - use main field boundaries
                let x_right = right_boundary - snake_length;
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

    fn calculate_team_starting_positions(&self) -> Vec<(Position, Direction)> {
        if !matches!(self.game_type, GameType::TeamMatch { .. }) {
            return self.calculate_starting_positions(self.players.len());
        }

        let Some(config) = &self.arena.team_zone_config else {
            return self.calculate_starting_positions(self.players.len());
        };

        let mut positions: Vec<Option<(Position, Direction)>> =
            vec![None; self.arena.snakes.len()];

        let mut team_snakes: [Vec<usize>; 2] = [Vec::new(), Vec::new()];
        for (idx, snake) in self.arena.snakes.iter().enumerate() {
            match snake.team_id {
                Some(TeamId(0)) => team_snakes[0].push(idx),
                Some(TeamId(1)) => team_snakes[1].push(idx),
                _ => {}
            }
        }

        let snake_length = self.starting_snake_length() as i16;
        let width = self.arena.width as i16;
        let height = self.arena.height as i16;
        let end_zone_depth = config.end_zone_depth as i16;

        // Place snakes near the goal opening so they face the gap instead of a wall
        let mut positions_for_side =
            |count: usize, team_id: TeamId, is_left: bool| -> Vec<(Position, Direction)> {
            let mut side_positions = Vec::with_capacity(count);
            if count == 0 {
                return side_positions;
            }

            let boundary_x = if is_left {
                end_zone_depth - 1
            } else {
                width - end_zone_depth
            };
            // Head sits one cell inside the boundary so first move reaches the gate column
            let head_x = if is_left {
                (boundary_x - 1).max(0)
            } else {
                (boundary_x + 1).min(width - 1)
            };

            // Use goal opening for vertical placement to align with the gate
            let (_goal_x, y_start, y_end) = self
                .arena
                .goal_bounds(team_id)
                .unwrap_or((boundary_x, height / 2, height / 2));
            let gate_top = y_start.max(0);
            let gate_bottom = y_end.min(height - 1);
            let gate_span = (gate_bottom - gate_top).max(0);

            for i in 0..count {
                let y = if count == 1 {
                    (gate_top + gate_bottom) / 2
                } else {
                    // Evenly space within gate interior, avoiding the extreme ends
                    let spacing = (gate_span as f64) / ((count as f64) + 1.0);
                    let pos = gate_top as f64 + spacing * ((i as f64) + 1.0);
                    pos.round()
                        .clamp(gate_top as f64, gate_bottom as f64) as i16
                };
                let direction = if is_left {
                    Direction::Right
                } else {
                    Direction::Left
                };
                side_positions.push((Position { x: head_x, y }, direction));
            }

            side_positions
        };

        let team0_positions = positions_for_side(team_snakes[0].len(), TeamId(0), true);
        let team1_positions = positions_for_side(team_snakes[1].len(), TeamId(1), false);

        for (idx, pos) in team_snakes[0].iter().zip(team0_positions.into_iter()) {
            if *idx < positions.len() {
                positions[*idx] = Some(pos);
            }
        }
        for (idx, pos) in team_snakes[1].iter().zip(team1_positions.into_iter()) {
            if *idx < positions.len() {
                positions[*idx] = Some(pos);
            }
        }

        let fallback = self.calculate_starting_positions(self.players.len());
        positions
            .into_iter()
            .enumerate()
            .map(|(idx, pos)| {
                pos.unwrap_or_else(|| {
                    fallback
                        .get(idx)
                        .copied()
                        .unwrap_or((
                            Position { x: 0, y: 0 },
                            Direction::Right,
                        ))
                })
            })
            .collect()
    }

    fn apply_starting_positions(&mut self, player_count: usize) {
        let starting_positions = if matches!(self.game_type, GameType::TeamMatch { .. }) {
            self.calculate_team_starting_positions()
        } else {
            self.calculate_starting_positions(player_count)
        };

        let snake_length = match &self.game_type {
            GameType::Custom { settings } => settings.snake_start_length as usize,
            _ => DEFAULT_SNAKE_LENGTH,
        };

        for (snake_id, snake) in self.arena.snakes.iter_mut().enumerate() {
            if let Some((head_pos, direction)) = starting_positions.get(snake_id) {
                let tail_pos = match direction {
                    Direction::Left => Position {
                        x: head_pos.x + (snake_length - 1) as i16,
                        y: head_pos.y,
                    },
                    Direction::Right => Position {
                        x: head_pos.x - (snake_length - 1) as i16,
                        y: head_pos.y,
                    },
                    Direction::Up => Position {
                        x: head_pos.x,
                        y: head_pos.y + (snake_length - 1) as i16,
                    },
                    Direction::Down => Position {
                        x: head_pos.x,
                        y: head_pos.y - (snake_length - 1) as i16,
                    },
                };

                snake.body = vec![*head_pos, tail_pos];
                snake.direction = *direction;
            }
        }
    }

    fn starting_snake_length(&self) -> usize {
        match &self.game_type {
            GameType::Custom { settings } => settings.snake_start_length as usize,
            _ => DEFAULT_SNAKE_LENGTH,
        }
    }

    fn respawn_event_for_snake(&self, snake_id: u32) -> Option<GameEvent> {
        let starting_positions = if matches!(self.game_type, GameType::TeamMatch { .. }) {
            self.calculate_team_starting_positions()
        } else {
            self.calculate_starting_positions(self.players.len())
        };
        let position_idx = snake_id as usize;

        let mut candidate_positions: Vec<(Position, Direction)> = Vec::new();
        if let Some(preferred) = starting_positions.get(position_idx) {
            candidate_positions.push(*preferred);
        }
        candidate_positions.extend(starting_positions.into_iter());

        for (pos, dir) in candidate_positions {
            let occupied = self
                .arena
                .snakes
                .iter()
                .any(|s| s.is_alive && s.contains_point(&pos, false));
            if !occupied {
                return Some(GameEvent::SnakeRespawned {
                    snake_id,
                    position: pos,
                    direction: dir,
                });
            }
        }

        None
    }

    pub fn add_player_with_team(
        &mut self,
        user_id: u32,
        username: Option<String>,
        team_override: Option<TeamId>,
    ) -> Result<Player> {
        if self.players.contains_key(&user_id) {
            return Err(anyhow::anyhow!(
                "Player with user_id {} already exists",
                user_id
            ));
        }

        // Only rearrange players on tick 0
        if self.tick != 0 {
            return Err(anyhow::anyhow!(
                "Cannot add player after the game has started"
            ));
        }

        // Store username if provided
        if let Some(name) = username {
            self.usernames.insert(user_id, name);
        }

        // Determine team assignment for team games
        let team_id = match (&self.game_type, team_override) {
            (GameType::TeamMatch { .. }, Some(team)) => Some(team),
            (GameType::TeamMatch { .. }, None) => {
                // Assign teams alternately: A, B, A, B...
                let existing_player_count = self.players.len();
                let team_index = (existing_player_count % 2) as u8;
                Some(TeamId(team_index))
            }
            _ => None,
        };

        // Add new player first with temporary position
        let snake = Snake {
            body: vec![Position { x: 0, y: 0 }, Position { x: 0, y: 0 }],
            direction: Direction::Right,
            is_alive: true,
            food: 0,
            team_id,
        };

        let snake_id = self.arena.add_snake(snake)?;
        let player = Player { user_id, snake_id };
        self.players.insert(user_id, player.clone());

        // Calculate starting positions for all players
        let player_count = self.players.len();
        self.apply_starting_positions(player_count);

        Ok(player)
    }

    pub fn add_player(&mut self, user_id: u32, username: Option<String>) -> Result<Player> {
        self.add_player_with_team(user_id, username, None)
    }

    pub fn add_spectator(&mut self, user_id: u32, username: Option<String>) {
        if let Some(name) = username {
            self.usernames.insert(user_id, name);
        }
        self.spectators.insert(user_id);
    }

    /// Spawns initial food items when the game starts
    pub fn spawn_initial_food(&mut self) {
        if self.rng.is_none() {
            return; // Can't spawn food without RNG
        }

        let target_food = self.properties.available_food_target;
        let mut attempts = 0;
        const MAX_ATTEMPTS: usize = 1000; // Prevent infinite loop

        while self.arena.food.len() < target_food && attempts < MAX_ATTEMPTS {
            attempts += 1;

            if let Some(rng) = &mut self.rng {
                // For team games, only spawn food in the main field (not in endzones)
                let (x_min, x_max) = if let Some((left, right)) = self.arena.main_field_bounds() {
                    (left, right)
                } else {
                    (0, self.arena.width as i16 - 1)
                };

                // Calculate center and standard deviation for normal distribution
                let x_center = (x_min + x_max) as f32 / 2.0;
                let y_center = (self.arena.height as f32) / 2.0;

                // Use std_dev = range/6 so 99.7% of values fall within bounds (3-sigma rule)
                let x_range = (x_max - x_min + 1) as f32;
                let y_range = self.arena.height as f32;
                let x_std_dev = x_range / 6.0;
                let y_std_dev = y_range / 6.0;

                // Generate normally distributed position centered in the arena
                let x_normal = rng.next_normal(x_center, x_std_dev);
                let y_normal = rng.next_normal(y_center, y_std_dev);

                // Clamp to ensure within bounds
                let x = (x_normal.round() as i16).clamp(x_min, x_max);
                let y = (y_normal.round() as i16).clamp(0, self.arena.height as i16 - 1);

                let position = Position { x, y };

                // Check if position is valid (not occupied by food or snake)
                if !self.arena.food.contains(&position)
                    && !self
                        .arena
                        .snakes
                        .iter()
                        .any(|s| s.is_alive && s.contains_point(&position, false))
                {
                    self.arena.food.push(position);
                }
            }
        }
    }

    pub fn schedule_command(&mut self, command_message: &GameCommandMessage) {
        // Only allow gameplay commands from active players; spectators should never drive snakes.
        if let GameCommand::Turn { .. } = command_message.command {
            let issuing_user_id = command_message
                .command_id_server
                .as_ref()
                .map(|id| id.user_id)
                .unwrap_or(command_message.command_id_client.user_id);

            if !self.players.contains_key(&issuing_user_id) {
                return;
            }
        }

        self.apply_event(
            GameEvent::CommandScheduled {
                command_message: command_message.clone(),
            },
            None,
        );
    }

    pub fn has_scheduled_commands(&self, tick: u32) -> bool {
        self.command_queue.has_commands_for_tick(tick)
    }

    pub fn join(&mut self, _user_id: u32) {}

    pub fn tick_forward(&mut self, movement_only: bool) -> Result<Vec<(u64, GameEvent)>> {
        let mut out: Vec<(u64, GameEvent)> = Vec::new();

        // Emit snapshot on first tick
        if self.tick == 0 {
            self.event_sequence += 1;
            out.push((
                self.event_sequence,
                GameEvent::Snapshot {
                    game_state: self.clone(),
                },
            ));
        }

        // Exec commands in the queue until the only ones left are for after this tick
        // debug!("tick_forward: Checking for commands at tick {}", self.tick);
        // eprintln!("COMMON DEBUG: tick_forward checking commands at tick {}", self.tick);
        while let Some(command_message) = self.command_queue.pop(self.tick) {
            // debug!("tick_forward: Popped command from queue: {:?}", command_message);
            // eprintln!("COMMON DEBUG: Popped command: {:?}", command_message);
            match self.exec_command(command_message.command) {
                Ok(events) => {
                    // debug!("tick_forward: exec_command returned {} events", events.len());
                    // eprintln!("COMMON DEBUG: exec_command returned {} events", events.len());
                    out.extend(events);
                }
                Err(_e) => {
                    // debug!("tick_forward: exec_command failed with error: {:?}", e);
                    // eprintln!("COMMON DEBUG: exec_command error: {:?}", e);
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
        let mut died_snake_ids = HashSet::new();
        let width = self.arena.width as i16;
        let height = self.arena.height as i16;
        'main_snake_loop: for (snake_id, snake) in self.iter_snakes() {
            let head = snake.head()?;
            if snake.is_alive {
                // Check for wall collisions in team games
                if self.arena.is_wall_position(head) {
                    died_snake_ids.insert(snake_id);
                    continue 'main_snake_loop;
                }

                // Entering the enemy base kills the snake
                if let Some(team_id) = snake.team_id {
                    if self.arena.is_in_enemy_base(head, team_id) {
                        died_snake_ids.insert(snake_id);
                        continue 'main_snake_loop;
                    }
                }

                // If not within bounds
                if !(head.x >= 0 && head.x < width && head.y >= 0 && head.y < height) {
                    died_snake_ids.insert(snake_id);
                    continue 'main_snake_loop;
                }

                // If crashed with other snake
                for (other_snake_id, other_snake) in self.iter_snakes() {
                    let is_self = snake_id == other_snake_id;
                    if other_snake.is_alive && other_snake.contains_point(head, is_self) {
                        died_snake_ids.insert(snake_id);
                        continue 'main_snake_loop;
                    }
                }
            }
        }

        // Rollback and kill snakes that crashed
        for snake_id in died_snake_ids {
            self.arena.snakes[snake_id as usize] = old_snakes[snake_id as usize].clone();
            self.apply_event(GameEvent::SnakeDied { snake_id }, Some(&mut out));

            if let GameType::TeamMatch { .. } = &self.game_type {
                if let Some(event) = self.respawn_event_for_snake(snake_id) {
                    self.apply_event(event, Some(&mut out));
                }
            }
        }

        // Eat food
        let mut food_eaten_events: Vec<GameEvent> = Vec::new();
        for (snake_id, snake) in self.iter_snakes() {
            let head = snake.head()?;
            if snake.is_alive && self.arena.food.contains(head) {
                food_eaten_events.push(GameEvent::FoodEaten {
                    snake_id,
                    position: *head,
                });
            }
        }
        for event in food_eaten_events {
            self.apply_event(event, Some(&mut out));
        }

        // Spawn new food
        if !movement_only && self.arena.food.len() < self.properties.available_food_target {
            // The client will not have rng so it won't be able to spawn food.
            // This is by design as there's no reason for the client to spawn food.
            if let Some(rng) = &mut self.rng {
                // For team games, only spawn food in the main field (not in endzones)
                let (x_min, x_max) = if let Some((left, right)) = self.arena.main_field_bounds() {
                    (left, right)
                } else {
                    (0, self.arena.width as i16 - 1)
                };

                // Calculate center and standard deviation for normal distribution
                let x_center = (x_min + x_max) as f32 / 2.0;
                let y_center = (self.arena.height as f32) / 2.0;

                // Use std_dev = range/6 so 99.7% of values fall within bounds (3-sigma rule)
                let x_range = (x_max - x_min + 1) as f32;
                let y_range = self.arena.height as f32;
                let x_std_dev = x_range / 6.0;
                let y_std_dev = y_range / 6.0;

                // Generate normally distributed position centered in the arena
                let x_normal = rng.next_normal(x_center, x_std_dev);
                let y_normal = rng.next_normal(y_center, y_std_dev);

                // Clamp to ensure within bounds
                let x = (x_normal.round() as i16).clamp(x_min, x_max);
                let y = (y_normal.round() as i16).clamp(0, self.arena.height as i16 - 1);

                let position = Position { x, y };

                if !self.arena.food.contains(&position)
                    && !self
                        .arena
                        .snakes
                        .iter()
                        .any(|s| s.is_alive && s.contains_point(&position, false))
                {
                    self.apply_event(GameEvent::FoodSpawned { position }, Some(&mut out));
                }
            }
        }

        // Calculate and update scores
        if !movement_only {
            let mut score_updates: Vec<(u32, u32)> = Vec::new();
            for (snake_id, snake) in self.iter_snakes() {
                // Calculate the actual length of the snake
                let mut length = 0;
                if snake.body.len() >= 2 {
                    for i in 0..snake.body.len() - 1 {
                        let p1 = &snake.body[i];
                        let p2 = &snake.body[i + 1];
                        let distance = ((p2.x - p1.x).abs() + (p2.y - p1.y).abs()) as usize;
                        length += distance;
                    }
                    length += 1; // Add 1 for the head
                } else {
                    length = snake.body.len();
                }

                // Score is length minus initial size (2)
                let score = if snake.is_alive {
                    length.saturating_sub(2) as u32
                } else {
                    self.scores.get(&snake_id).copied().unwrap_or(0)
                };

                // Collect score updates
                let old_score = self.scores.get(&snake_id).copied().unwrap_or(0);
                if score != old_score {
                    score_updates.push((snake_id, score));
                }
            }

            // Apply individual score updates
            for (snake_id, score) in score_updates {
                self.apply_event(GameEvent::ScoreUpdated { snake_id, score }, Some(&mut out));
            }

            // Track goal touches as simple score increments in team games
            if let GameType::TeamMatch { .. } = &self.game_type {
                if self.team_scores.is_some() {
                    let mut team_score_deltas: HashMap<TeamId, u32> = HashMap::new();
                    let mut respawns: Vec<u32> = Vec::new();
                    let starting_length = self.starting_snake_length();

                    for (snake_id, snake) in self.iter_snakes() {
                        if !snake.is_alive {
                            continue;
                        }

                        let Some(team_id) = snake.team_id else {
                            continue;
                        };

                        let Ok(head) = snake.head() else {
                            continue;
                        };

                        if !self.arena.is_in_team_base(head, team_id) {
                            continue;
                        }

                        let snake_length = snake.length();
                        let extra_segments = snake_length.saturating_sub(starting_length);
                        let carried_segments = extra_segments + snake.food as usize;
                        let carried_food = (carried_segments / 2) as u32;

                        if carried_food == 0 {
                            continue;
                        }

                        *team_score_deltas.entry(team_id).or_default() += carried_food;
                        respawns.push(snake_id);
                    }

                    for (team_id, delta) in team_score_deltas {
                        let current_score = self
                            .team_scores
                            .as_ref()
                            .and_then(|scores| scores.get(&team_id).copied())
                            .unwrap_or(0);

                        self.apply_event(
                            GameEvent::TeamScoreUpdated {
                                team_id,
                                score: current_score + delta,
                            },
                            Some(&mut out),
                        );
                    }

                    for snake_id in respawns {
                        self.apply_event(GameEvent::SnakeDied { snake_id }, Some(&mut out));
                        if let Some(event) = self.respawn_event_for_snake(snake_id) {
                            self.apply_event(event, Some(&mut out));
                        }
                    }
                }
            }

            // Check completion conditions
            let alive_snakes: Vec<u32> = self
                .arena
                .snakes
                .iter()
                .enumerate()
                .filter(|(_, snake)| snake.is_alive)
                .map(|(idx, _)| idx as u32)
                .collect();

            if matches!(self.status, GameStatus::Started { .. }) {
                match &self.game_type {
                    GameType::TeamMatch { .. } => {
                        if let Some(limit_ms) = self.properties.time_limit_ms {
                            let elapsed_ms =
                                (self.tick as i64) * (self.properties.tick_duration_ms as i64);
                            if elapsed_ms >= limit_ms as i64 {
                                let winning_team = self
                                    .team_scores
                                    .as_ref()
                                    .and_then(|scores| {
                                        scores
                                            .iter()
                                            .max_by_key(|(_, score)| *score)
                                            .map(|(team_id, _)| *team_id)
                                    });

                                let winning_snake_id = winning_team
                                    .and_then(|team_id| {
                                        self.arena
                                            .snakes
                                            .iter()
                                            .enumerate()
                                            .find(|(_, snake)| {
                                                snake.team_id == Some(team_id) && snake.is_alive
                                            })
                                            .map(|(idx, _)| idx as u32)
                                    })
                                    .or_else(|| {
                                        winning_team.and_then(|team_id| {
                                            self.arena
                                                .snakes
                                                .iter()
                                                .enumerate()
                                                .find(|(_, snake)| snake.team_id == Some(team_id))
                                                .map(|(idx, _)| idx as u32)
                                        })
                                    });

                                let mut player_xp_awards = HashMap::new();
                                for (user_id, player) in &self.players {
                                    let score = self
                                        .scores
                                        .get(&player.snake_id)
                                        .copied()
                                        .unwrap_or(0);
                                    let snake =
                                        &self.arena.snakes[player.snake_id as usize];
                                    let is_winner = winning_team
                                        .map_or(false, |team| snake.team_id == Some(team));

                                    let base_xp = score * 10; // 10 XP per food eaten
                                    let bonus_xp = if is_winner { 50 } else { 10 }; // Winner bonus or participation
                                    player_xp_awards.insert(*user_id, base_xp + bonus_xp);
                                }

                                self.apply_event(
                                    GameEvent::XPAwarded {
                                        player_xp: player_xp_awards,
                                    },
                                    Some(&mut out),
                                );

                                self.apply_event(
                                    GameEvent::StatusUpdated {
                                        status: GameStatus::Complete { winning_snake_id },
                                    },
                                    Some(&mut out),
                                );
                            }
                        }
                    }
                    _ => {
                        if alive_snakes.is_empty() {
                            let winning_snake_id = None;

                            let mut player_xp_awards = HashMap::new();
                            for (user_id, player) in &self.players {
                                let score =
                                    self.scores.get(&player.snake_id).copied().unwrap_or(0);
                                let base_xp = score * 10; // 10 XP per food eaten
                                player_xp_awards.insert(*user_id, base_xp + 10);
                            }

                            self.apply_event(
                                GameEvent::XPAwarded {
                                    player_xp: player_xp_awards,
                                },
                                Some(&mut out),
                            );

                            self.apply_event(
                                GameEvent::StatusUpdated {
                                    status: GameStatus::Complete { winning_snake_id },
                                },
                                Some(&mut out),
                            );
                        }
                    }
                }
            }
        }

        // Increment tick
        self.tick += 1;

        Ok(out)
    }

    fn exec_command(&mut self, command: GameCommand) -> Result<Vec<(u64, GameEvent)>> {
        // debug!("exec_command: Entering with command {:?}", command);
        // eprintln!("COMMON DEBUG: exec_command called with {:?}", command);
        let mut out: Vec<(u64, GameEvent)> = Vec::new();
        match command {
            GameCommand::Turn {
                snake_id,
                direction,
            } => {
                // debug!("exec_command: Processing Turn command - snake_id: {}, direction: {:?}", snake_id, direction);
                // eprintln!("COMMON DEBUG: Turn command - snake_id: {}, direction: {:?}", snake_id, direction);

                // Get current snake state
                let snake = self
                    .arena
                    .snakes
                    .get(snake_id as usize)
                    .context("Snake not found")?;

                // debug!("exec_command: Snake {} state - alive: {}, current_direction: {:?}, requested_direction: {:?}",
                //       snake_id, snake.is_alive, snake.direction, direction);
                // eprintln!("COMMON DEBUG: Snake {} - alive: {}, current: {:?}, requested: {:?}",
                //          snake_id, snake.is_alive, snake.direction, direction);

                if snake.is_alive && snake.direction != direction {
                    // debug!("exec_command: Snake is alive and direction is different");

                    // Always prevent 180-degree turns
                    if snake.direction.is_opposite(&direction) {
                        // debug!("exec_command: Ignoring command - 180-degree turn attempted");
                        // eprintln!("COMMON DEBUG: Ignoring 180-degree turn");
                        // Ignore the command - cannot turn 180 degrees
                        return Ok(out);
                    }

                    // debug!("exec_command: Generating SnakeTurned event for snake {}", snake_id);
                    // eprintln!("COMMON DEBUG: Generating SnakeTurned event for snake {}", snake_id);
                    self.apply_event(
                        GameEvent::SnakeTurned {
                            snake_id,
                            direction,
                        },
                        Some(&mut out),
                    );
                    // debug!("exec_command: SnakeTurned event applied successfully");
                } else {
                    if !snake.is_alive {
                        // debug!("exec_command: Ignoring command - snake {} is dead", snake_id);
                        // eprintln!("COMMON DEBUG: Ignoring - snake {} is dead", snake_id);
                    } else if snake.direction == direction {
                        // debug!("exec_command: Ignoring command - snake {} already facing {:?}", snake_id, direction);
                        // eprintln!("COMMON DEBUG: Ignoring - snake {} already facing {:?}", snake_id, direction);
                    }
                }
            }
            GameCommand::UpdateStatus { .. } => {
                // debug!("exec_command: Processing UpdateStatus command");
            }
        }

        // debug!("exec_command: Returning {} events", out.len());
        // eprintln!("COMMON DEBUG: exec_command returning {} events", out.len());
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

            GameEvent::SnakeTurned {
                snake_id,
                direction,
            } => {
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
                        snake.food += 2; // Each food now adds 2 segments instead of 1
                    }
                }
            }

            GameEvent::CommandScheduled { command_message } => {
                self.command_queue.push(command_message);
            }

            GameEvent::StatusUpdated { status } => {
                self.status = status;
            }

            GameEvent::ScoreUpdated { snake_id, score } => {
                self.scores.insert(snake_id, score);
            }

            GameEvent::TeamScoreUpdated { team_id, score } => {
                if let Some(ref mut team_scores) = self.team_scores {
                    team_scores.insert(team_id, score);
                }
            }

            GameEvent::SnakeRespawned {
                snake_id,
                position,
                direction,
            } => {
                let snake_length = self.starting_snake_length() as i16;

                // Build compressed snake body: just head and tail for a straight snake
                let tail_pos = match direction {
                    Direction::Left => Position {
                        x: position.x + snake_length - 1,
                        y: position.y,
                    },
                    Direction::Right => Position {
                        x: position.x - snake_length + 1,
                        y: position.y,
                    },
                    Direction::Up => Position {
                        x: position.x,
                        y: position.y + snake_length - 1,
                    },
                    Direction::Down => Position {
                        x: position.x,
                        y: position.y - snake_length + 1,
                    },
                };

                // Now update the snake
                if let Ok(snake) = self.get_snake_mut(snake_id) {
                    snake.body = vec![position, tail_pos];
                    snake.direction = direction;
                    snake.is_alive = true;
                    snake.food = 0;
                }
            }

            GameEvent::XPAwarded { player_xp } => {
                eprintln!("APPLYING XPAwarded event: {:?}", player_xp);
                self.player_xp = player_xp;
                eprintln!("GameState.player_xp after applying: {:?}", self.player_xp);
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

    fn create_command_message(
        tick: u32,
        user_id: u32,
        seq: u32,
        with_server_id: bool,
    ) -> GameCommandMessage {
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
    fn snake_collides_with_itself_after_turning() {
        let mut game = GameState::new(
            10,
            10,
            GameType::FreeForAll { max_players: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );

        game.arena.snakes.push(Snake {
            body: vec![
                Position { x: 2, y: 2 },
                Position { x: 2, y: 3 },
                Position { x: 1, y: 3 },
                Position { x: 1, y: 2 },
                Position { x: 1, y: 1 },
            ],
            direction: Direction::Left,
            is_alive: true,
            food: 1,
            team_id: None,
        });

        let events = game
            .tick_forward(true)
            .expect("tick_forward should succeed");

        assert!(
            events
                .iter()
                .any(|(_, event)| matches!(event, GameEvent::SnakeDied { snake_id: 0 })),
            "expected snake to die after colliding with itself"
        );
        assert!(!game.arena.snakes[0].is_alive);
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
        println!(
            "Reverse(cmd2) > Reverse(cmd1): {}",
            Reverse(cmd2.clone()) > Reverse(cmd1.clone())
        );
        println!(
            "Reverse(cmd2) > Reverse(cmd3): {}",
            Reverse(cmd2.clone()) > Reverse(cmd3.clone())
        );
        println!(
            "Reverse(cmd3) > Reverse(cmd1): {}",
            Reverse(cmd3.clone()) > Reverse(cmd1.clone())
        );

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
    fn team_scores_when_returning_food_to_base() {
        let mut game = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            Some(4242),
            0,
        );

        game.add_player(1, Some("Player1".to_string()))
            .expect("add player 1");
        game.add_player(2, Some("Player2".to_string()))
            .expect("add player 2");

        {
            let snake = &mut game.arena.snakes[0];
            snake.body = vec![
                Position { x: 5, y: 10 },
                Position { x: 2, y: 10 },
            ];
            snake.direction = Direction::Right;
            snake.is_alive = true;
            snake.food = 2; // carrying one food
        }

        let events = game.tick_forward(false).expect("tick_forward should work");

        let scored = events.iter().any(|(_, event)| {
            matches!(
                event,
                GameEvent::TeamScoreUpdated {
                    team_id,
                    score: 1
                } if *team_id == TeamId(0)
            )
        });
        assert!(scored, "team should score after returning food to base");

        let respawned = events.iter().any(|(_, event)| {
            matches!(event, GameEvent::SnakeRespawned { snake_id, .. } if *snake_id == 0)
        });
        assert!(respawned, "snake should respawn after scoring");

        let score = game
            .team_scores
            .as_ref()
            .and_then(|scores| scores.get(&TeamId(0)).copied())
            .unwrap_or(0);
        assert_eq!(score, 1, "team score should increment by carried food");

        let snake = &game.arena.snakes[0];
        assert!(snake.is_alive);
        assert_eq!(snake.food, 0, "snake should not keep carried food after respawn");
    }

    #[test]
    fn snake_dies_on_enemy_base_contact() {
        let mut game = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            Some(777),
            0,
        );

        game.add_player(1, Some("Player1".to_string()))
            .expect("add player 1");
        game.add_player(2, Some("Player2".to_string()))
            .expect("add player 2");

        let enemy_zone_start =
            game.arena.width as i16 - game.arena.team_zone_config.as_ref().unwrap().end_zone_depth as i16;

        {
            let snake = &mut game.arena.snakes[0];
            snake.body = vec![
                Position {
                    x: enemy_zone_start + 1,
                    y: 15,
                },
                Position {
                    x: enemy_zone_start - 2,
                    y: 15,
                },
            ];
            snake.direction = Direction::Right;
            snake.is_alive = true;
        }

        let events = game.tick_forward(false).expect("tick_forward should work");

        assert!(
            events.iter().any(|(_, event)| {
                matches!(event, GameEvent::SnakeDied { snake_id } if *snake_id == 0)
            }),
            "snake should die when entering enemy base"
        );

        assert!(
            game.arena.snakes[0].is_alive,
            "snake should respawn after dying in team games"
        );

        let score = game
            .team_scores
            .as_ref()
            .and_then(|scores| scores.get(&TeamId(0)).copied())
            .unwrap_or(0);
        assert_eq!(score, 0, "touching enemy base should not award points");
    }

    #[test]
    fn test_command_id_ordering() {
        // Test that CommandId ordering works correctly
        let id1 = create_command_id(10, 1, 1);
        let id2 = create_command_id(10, 1, 2);
        let id3 = create_command_id(10, 2, 1);
        let id4 = create_command_id(11, 1, 1);

        assert!(id1 < id2); // Same tick and user, lower sequence
        assert!(id1 < id3); // Same tick, lower user id
        assert!(id1 < id4); // Lower tick
    }

    #[test]
    fn test_binary_heap_with_reverse() {
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;

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
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;

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
        println!(
            "msg2.command_id_client.cmp(&msg1.command_id_client): {:?}",
            msg2.command_id_client.cmp(&msg1.command_id_client)
        );

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
        println!(
            "msg_tick10.command_id_client: {:?}",
            msg_tick10.command_id_client
        );
        println!(
            "msg_tick20.command_id_client: {:?}",
            msg_tick20.command_id_client
        );
        println!("msg_tick10.id(): {:?}", msg_tick10.id());
        println!("msg_tick20.id(): {:?}", msg_tick20.id());
        println!(
            "msg_tick10.cmp(&msg_tick20): {:?}",
            msg_tick10.cmp(&msg_tick20)
        );
        println!(
            "msg_tick10.command_id_client.cmp(&msg_tick20.command_id_client): {:?}",
            msg_tick10
                .command_id_client
                .cmp(&msg_tick20.command_id_client)
        );

        // Check the actual comparison being used in Ord
        let id1 = msg_tick10
            .command_id_server
            .as_ref()
            .unwrap_or(&msg_tick10.command_id_client);
        let id2 = msg_tick20
            .command_id_server
            .as_ref()
            .unwrap_or(&msg_tick20.command_id_client);
        println!("id1: {:?}", id1);
        println!("id2: {:?}", id2);
        println!("id1.cmp(id2): {:?}", id1.cmp(id2));

        // Let's manually implement what Ord::cmp should do
        let manual_cmp = msg_tick10
            .command_id_server
            .as_ref()
            .unwrap_or(&msg_tick10.command_id_client)
            .cmp(
                msg_tick20
                    .command_id_server
                    .as_ref()
                    .unwrap_or(&msg_tick20.command_id_client),
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
        assert!(msg1 < msg2); // tick 10 < tick 20
        assert!(msg1 < msg3); // tick 10 < tick 15
        assert!(msg3 < msg2); // tick 15 < tick 20

        // Test with server IDs
        let mut msg_with_server = create_command_message(10, 1, 1, false);
        msg_with_server.command_id_server = Some(create_command_id(25, 1, 1));

        assert!(msg2 < msg_with_server); // tick 20 < tick 25 (server tick overrides)
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
        assert!(rev2 < rev1); // Reverse(tick 20) < Reverse(tick 10)
        assert!(rev3 < rev1); // Reverse(tick 15) < Reverse(tick 10)
        assert!(rev2 < rev3); // Reverse(tick 20) < Reverse(tick 15)

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
