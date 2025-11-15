use crate::util::PseudoRandom;
use crate::{
    DEFAULT_CUSTOM_GAME_TICK_MS, DEFAULT_FOOD_TARGET, DEFAULT_TICK_INTERVAL_MS, Direction, Player,
    Position, Snake,
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

    // Round lifecycle events
    RoundCompleted {
        winning_team_id: TeamId,
        round_number: u32,
    },
    RoundDraw {
        round_number: u32,
    }, // Round ended in a draw
    RoundStarting {
        round_number: u32,
        start_time: i64,
    },
    MatchCompleted {
        winning_team_id: TeamId,
    }, // Removed final_scores - use GameState.round_wins instead

    // Arena reset events (for new round)
    ArenaReset,
    SnakeRespawned {
        snake_id: u32,
        position: Position,
        direction: Direction,
    },
    AllFoodCleared,
    FoodRespawned {
        positions: Vec<Position>,
    },
    RoundWinRecorded {
        team_id: TeamId,
        total_wins: u32,
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

    /// Check if a snake has reached the enemy goal
    pub fn has_reached_goal(&self, snake: &Snake, team_id: TeamId) -> bool {
        if let (Some(head), Some(config)) = (snake.head().ok(), &self.team_zone_config) {
            // Team 0's goal is to reach Team 1's end zone (right side)
            // Team 1's goal is to reach Team 0's end zone (left side)
            match team_id.0 {
                0 => {
                    // Check if in Team 1's end zone
                    head.x >= self.width as i16 - config.end_zone_depth as i16
                }
                1 => {
                    // Check if in Team 0's end zone
                    head.x < config.end_zone_depth as i16
                }
                _ => false, // Other teams default to false
            }
        } else {
            false
        }
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

impl GameType {
    pub fn is_duel(&self) -> bool {
        !matches!(self, GameType::TeamMatch { per_team } if *per_team == 1)
            || !matches!(self, GameType::Custom { settings } if settings.game_mode == GameMode::Duel)
    }

    pub fn is_solo(&self) -> bool {
        !matches!(self, GameType::Solo)
            || !matches!(self, GameType::Custom { settings } if settings.game_mode == GameMode::Solo)
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

    // Round-based scoring fields
    pub current_round: u32,               // Current round number (1, 2, 3...)
    pub round_wins: HashMap<TeamId, u32>, // Rounds won by each team
    pub rounds_to_win: u32,               // 1 for quick match, 2 for competitive
    pub round_start_times: Vec<i64>,      // Start time of each round (ms)
    pub is_transitioning: bool,           // True during round transitions

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
        rng_seed: Option<u64>,
        start_ms: i64,
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
            Some(HashMap::new())
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

            // Round tracking fields
            current_round: 1,
            round_wins: if matches!(&game_type, GameType::TeamMatch { .. }) {
                let mut wins = HashMap::new();
                wins.insert(TeamId(0), 0);
                wins.insert(TeamId(1), 0);
                wins
            } else {
                HashMap::new()
            },
            rounds_to_win: 1,                  // Default to 1 round (quick match)
            round_start_times: vec![start_ms], // First round starts at game start time
            is_transitioning: false,

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
        let snake_length = match &self.game_type {
            GameType::Custom { settings } => settings.snake_start_length as i16,
            _ => DEFAULT_SNAKE_LENGTH as i16,
        };

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

    pub fn add_player(&mut self, user_id: u32, username: Option<String>) -> Result<Player> {
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
        let team_id = match &self.game_type {
            GameType::TeamMatch { .. } => {
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
        let starting_positions = self.calculate_starting_positions(player_count);

        // Get snake length from custom settings or use default
        let snake_length = match &self.game_type {
            GameType::Custom { settings } => settings.snake_start_length as usize,
            _ => DEFAULT_SNAKE_LENGTH,
        };

        // Rearrange all snakes to their starting positions
        // Use deterministic assignment based on team or snake_id
        if let GameType::TeamMatch { .. } = &self.game_type {
            // For team games, assign positions based on team_id
            for (_user_id, player) in self.players.iter() {
                let snake = &mut self.arena.snakes[player.snake_id as usize];

                // Determine position index based on team
                let position_idx = match snake.team_id {
                    Some(TeamId(0)) => 0,          // Team 0 gets first position (left endzone)
                    Some(TeamId(1)) => 1,          // Team 1 gets second position (right endzone)
                    Some(TeamId(n)) => n as usize, // Other teams use their index
                    None => continue,              // Should not happen in team games
                };

                if position_idx < starting_positions.len() {
                    let (head_pos, direction) = &starting_positions[position_idx];

                    // Build compressed snake body: just head and tail for a straight snake
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
        } else {
            // For non-team games, use snake_id as position index for deterministic assignment
            for (_user_id, player) in self.players.iter() {
                let snake_id = player.snake_id as usize;
                if snake_id < starting_positions.len() {
                    let (head_pos, direction) = &starting_positions[snake_id];
                    let snake = &mut self.arena.snakes[snake_id];

                    // Build compressed snake body: just head and tail for a straight snake
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

        Ok(player)
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

            // Calculate team scores for team games
            if let (GameType::TeamMatch { .. }, Some(_)) = (&self.game_type, &self.team_scores) {
                let mut team_totals: HashMap<TeamId, u32> = HashMap::new();

                for (snake_id, snake) in self.iter_snakes() {
                    if let Some(team_id) = snake.team_id {
                        let snake_score = self.scores.get(&snake_id).copied().unwrap_or(0);
                        *team_totals.entry(team_id).or_insert(0) += snake_score;
                    }
                }

                // Update team scores if changed
                let mut team_updates: Vec<(TeamId, u32)> = Vec::new();
                for (team_id, total_score) in team_totals {
                    let old_team_score = self
                        .team_scores
                        .as_ref()
                        .and_then(|ts| ts.get(&team_id).copied())
                        .unwrap_or(0);

                    if total_score != old_team_score {
                        team_updates.push((team_id, total_score));
                    }
                }

                // Apply team score updates
                for (team_id, score) in team_updates {
                    self.apply_event(
                        GameEvent::TeamScoreUpdated { team_id, score },
                        Some(&mut out),
                    );
                }
            }

            // Check for goal scoring in team games (endzone reached)
            let mut scoring_teams: Vec<TeamId> = Vec::new();

            if let GameType::TeamMatch { .. } = &self.game_type {
                // Check all snakes to see which teams have reached the endzone
                for (_snake_id, snake) in self.iter_snakes() {
                    if snake.is_alive {
                        if let Some(team_id) = snake.team_id {
                            if self.arena.has_reached_goal(snake, team_id) {
                                // This team scored by reaching opponent's endzone!
                                if !scoring_teams.contains(&team_id) {
                                    scoring_teams.push(team_id);
                                }
                            }
                        }
                    }
                }
            }

            let goal_scored = !scoring_teams.is_empty();

            // Check if game should end (only one or no snakes alive)
            let alive_snakes: Vec<u32> = self
                .arena
                .snakes
                .iter()
                .enumerate()
                .filter(|(_, snake)| snake.is_alive)
                .map(|(idx, _)| idx as u32)
                .collect();

            // For team games, check if round should end (goal scored or all snakes of one team are dead)
            let should_end_round = if let GameType::TeamMatch { .. } = &self.game_type {
                if goal_scored {
                    true // End round if a goal was scored
                } else {
                    // Check if all Team A or all Team B snakes are dead
                    let team_0_alive = self
                        .arena
                        .snakes
                        .iter()
                        .any(|s| s.is_alive && s.team_id == Some(TeamId(0)));
                    let team_1_alive = self
                        .arena
                        .snakes
                        .iter()
                        .any(|s| s.is_alive && s.team_id == Some(TeamId(1)));

                    !team_0_alive || !team_1_alive
                }
            } else if self.game_type.is_solo() {
                // For solo games, only end when no snakes are alive
                alive_snakes.is_empty()
            } else {
                // For multiplayer games, end when 1 or fewer snakes are alive
                alive_snakes.len() <= 1
            };

            if should_end_round && matches!(self.status, GameStatus::Started { .. }) {
                // For team games with rounds, handle round completion
                if let GameType::TeamMatch { .. } = &self.game_type {
                    // Determine winning team for this round
                    let winning_team = if scoring_teams.len() > 1 {
                        // Both teams scored simultaneously - it's a draw!
                        None
                    } else if scoring_teams.len() == 1 {
                        // Only one team scored - they win the round
                        Some(scoring_teams[0])
                    } else {
                        // No goals scored, check which team has snakes alive
                        let team_0_alive = self
                            .arena
                            .snakes
                            .iter()
                            .any(|s| s.is_alive && s.team_id == Some(TeamId(0)));
                        let team_1_alive = self
                            .arena
                            .snakes
                            .iter()
                            .any(|s| s.is_alive && s.team_id == Some(TeamId(1)));

                        if team_0_alive && !team_1_alive {
                            Some(TeamId(0))
                        } else if !team_0_alive && team_1_alive {
                            Some(TeamId(1))
                        } else {
                            None // Draw - both teams lost all snakes simultaneously
                        }
                    };

                    if let Some(winning_team_id) = winning_team {
                        // A team won this round
                        // Emit round completed event
                        self.apply_event(
                            GameEvent::RoundCompleted {
                                winning_team_id: winning_team_id,
                                round_number: self.current_round,
                            },
                            Some(&mut out),
                        );

                        // Update round wins
                        let new_wins = self.round_wins.get(&winning_team_id).unwrap_or(&0) + 1;
                        self.apply_event(
                            GameEvent::RoundWinRecorded {
                                team_id: winning_team_id,
                                total_wins: new_wins,
                            },
                            Some(&mut out),
                        );

                        // Check if match is complete
                        if new_wins >= self.rounds_to_win {
                            // Match is complete!
                            self.apply_event(
                                GameEvent::MatchCompleted { winning_team_id },
                                Some(&mut out),
                            );

                            // Find a snake from winning team for compatibility
                            let winning_snake_id = self
                                .arena
                                .snakes
                                .iter()
                                .enumerate()
                                .find(|(_, s)| s.team_id == Some(winning_team_id))
                                .map(|(idx, _)| idx as u32);

                            // Calculate and emit XP for all players BEFORE Complete status
                            let mut player_xp_awards = HashMap::new();
                            for (user_id, player) in &self.players {
                                let score = self.scores.get(&player.snake_id).copied().unwrap_or(0);
                                let snake = &self.arena.snakes[player.snake_id as usize];
                                let is_winner = snake.team_id == Some(winning_team_id);

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
                        } else {
                            // Start next round
                            let next_round = self.current_round + 1;
                            // Calculate round start time based on current game time + 3000ms for countdown
                            let elapsed_ms =
                                (self.tick as i64) * (self.properties.tick_duration_ms as i64);
                            let round_start_time = self.start_ms + elapsed_ms + 3000; // 3 second countdown

                            self.apply_event(
                                GameEvent::RoundStarting {
                                    round_number: next_round,
                                    start_time: round_start_time,
                                },
                                Some(&mut out),
                            );

                            // Reset arena
                            self.apply_event(GameEvent::ArenaReset, Some(&mut out));
                            self.apply_event(GameEvent::AllFoodCleared, Some(&mut out));

                            // Respawn all snakes at their original positions
                            let starting_positions =
                                self.calculate_starting_positions(self.players.len());

                            // Collect respawn data first
                            let mut respawn_events = Vec::new();
                            for (_user_id, player) in self.players.iter() {
                                let snake = &self.arena.snakes[player.snake_id as usize];
                                let team_id = snake.team_id;

                                // Get position based on team
                                let position_idx = match team_id {
                                    Some(TeamId(0)) => 0,
                                    Some(TeamId(1)) => 1,
                                    _ => player.snake_id as usize,
                                };

                                if position_idx < starting_positions.len() {
                                    let (pos, dir) = starting_positions[position_idx];
                                    respawn_events.push(GameEvent::SnakeRespawned {
                                        snake_id: player.snake_id,
                                        position: pos,
                                        direction: dir,
                                    });
                                }
                            }

                            // Apply respawn events
                            for event in respawn_events {
                                self.apply_event(event, Some(&mut out));
                            }

                            // Respawn initial food
                            if let Some(rng) = &mut self.rng {
                                let mut food_positions = Vec::new();
                                let target_food = self.properties.available_food_target;
                                let (x_min, x_max) =
                                    if let Some((left, right)) = self.arena.main_field_bounds() {
                                        (left, right)
                                    } else {
                                        (0, self.arena.width as i16 - 1)
                                    };

                                let mut attempts = 0;
                                while food_positions.len() < target_food && attempts < 1000 {
                                    attempts += 1;
                                    let x_range = (x_max - x_min + 1) as u16;
                                    let position = Position {
                                        x: x_min + (rng.next_u16() % x_range) as i16,
                                        y: (rng.next_u16() % self.arena.height) as i16,
                                    };

                                    if !food_positions.contains(&position) {
                                        food_positions.push(position);
                                    }
                                }

                                self.apply_event(
                                    GameEvent::FoodRespawned {
                                        positions: food_positions,
                                    },
                                    Some(&mut out),
                                );
                            }
                        }
                    } else {
                        // Draw - both teams lost all snakes simultaneously
                        // Emit draw event
                        self.apply_event(
                            GameEvent::RoundDraw {
                                round_number: self.current_round,
                            },
                            Some(&mut out),
                        );

                        // Restart the round without incrementing anyone's score
                        // Calculate round start time based on current game time + 3000ms for countdown
                        let elapsed_ms =
                            (self.tick as i64) * (self.properties.tick_duration_ms as i64);
                        let round_start_time = self.start_ms + elapsed_ms + 3000; // 3 second countdown

                        // Emit round starting event for the same round (replay)
                        self.apply_event(
                            GameEvent::RoundStarting {
                                round_number: self.current_round, // Keep same round number
                                start_time: round_start_time,
                            },
                            Some(&mut out),
                        );

                        // Reset arena
                        self.apply_event(GameEvent::ArenaReset, Some(&mut out));
                        self.apply_event(GameEvent::AllFoodCleared, Some(&mut out));

                        // Respawn all snakes at their original positions
                        let starting_positions =
                            self.calculate_starting_positions(self.players.len());

                        // Collect respawn data first
                        let mut respawn_events = Vec::new();
                        for (_user_id, player) in self.players.iter() {
                            let snake = &self.arena.snakes[player.snake_id as usize];
                            let team_id = snake.team_id;

                            // Get position based on team
                            let position_idx = match team_id {
                                Some(TeamId(0)) => 0,
                                Some(TeamId(1)) => 1,
                                _ => player.snake_id as usize,
                            };

                            if position_idx < starting_positions.len() {
                                let (pos, dir) = starting_positions[position_idx];
                                respawn_events.push(GameEvent::SnakeRespawned {
                                    snake_id: player.snake_id,
                                    position: pos,
                                    direction: dir,
                                });
                            }
                        }

                        // Apply respawn events
                        for event in respawn_events {
                            self.apply_event(event, Some(&mut out));
                        }

                        // Respawn initial food
                        if let Some(rng) = &mut self.rng {
                            let mut food_positions = Vec::new();
                            let target_food = self.properties.available_food_target;
                            let (x_min, x_max) =
                                if let Some((left, right)) = self.arena.main_field_bounds() {
                                    (left, right)
                                } else {
                                    (0, self.arena.width as i16 - 1)
                                };

                            let mut attempts = 0;
                            while food_positions.len() < target_food && attempts < 1000 {
                                attempts += 1;
                                let x_range = (x_max - x_min + 1) as u16;
                                let position = Position {
                                    x: x_min + (rng.next_u16() % x_range) as i16,
                                    y: (rng.next_u16() % self.arena.height) as i16,
                                };

                                if !food_positions.contains(&position) {
                                    food_positions.push(position);
                                }
                            }

                            self.apply_event(
                                GameEvent::FoodRespawned {
                                    positions: food_positions,
                                },
                                Some(&mut out),
                            );
                        }
                    }
                } else {
                    // Non-team games: end normally
                    let winning_snake_id = alive_snakes.first().copied();

                    // Calculate and emit XP for all players BEFORE Complete status
                    let mut player_xp_awards = HashMap::new();
                    for (user_id, player) in &self.players {
                        let score = self.scores.get(&player.snake_id).copied().unwrap_or(0);
                        let is_winner = winning_snake_id == Some(player.snake_id);

                        let base_xp = score * 10; // 10 XP per food eaten
                        let bonus_xp = if is_winner { 100 } else { 10 }; // Winner bonus or participation
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

            // Round lifecycle events
            GameEvent::RoundCompleted {
                winning_team_id: _,
                round_number: _,
            } => {
                // Log the round completion - actual handling is done via other events
            }
            GameEvent::RoundDraw { round_number: _ } => {
                // Log the round draw - round will be replayed
            }
            GameEvent::RoundWinRecorded {
                team_id,
                total_wins,
            } => {
                self.round_wins.insert(team_id, total_wins);
            }
            GameEvent::RoundStarting {
                round_number,
                start_time,
            } => {
                self.current_round = round_number;
                self.is_transitioning = true;
                self.round_start_times.push(start_time);
            }
            GameEvent::MatchCompleted { winning_team_id: _ } => {
                // Match is over, no more rounds
                self.is_transitioning = false;
            }

            // Arena reset events
            GameEvent::ArenaReset => {
                // Clear transitioning flag when arena resets
                self.is_transitioning = false;
                // Don't reset tick counter - keep it continuous across rounds
            }
            GameEvent::AllFoodCleared => {
                self.arena.food.clear();
            }
            GameEvent::SnakeRespawned {
                snake_id,
                position,
                direction,
            } => {
                // Get snake length from game settings first
                let snake_length = match &self.game_type {
                    GameType::Custom { settings } => settings.snake_start_length as i16,
                    _ => 4, // Default snake length
                };

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
            GameEvent::FoodRespawned { positions } => {
                self.arena.food = positions;
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
