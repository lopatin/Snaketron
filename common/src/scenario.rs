//! Deterministic, data-driven gameplay scenarios.
//!
//! A scenario is loaded through the same `GameState` constructor and command
//! queue used by production play. Only a deliberately small set of balance
//! fields can be overridden; outcomes are always produced by the real engine.

use crate::{
    CommandId, Direction, GameCommand, GameCommandMessage, GameEvent, GameState, GameStatus,
    GameType, Position, QueueMode, SnakeCombo, TeamId, boost_config_for, calculate_ai_move,
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

pub const SCENARIO_FORMAT_VERSION: u32 = 1;

fn default_scenario_format_version() -> u32 {
    SCENARIO_FORMAT_VERSION
}

fn default_time_scale() -> f32 {
    1.0
}

fn default_camera_deadzone() -> f32 {
    0.2
}

fn default_camera_ease() -> f32 {
    0.16
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct ScenarioScript {
    #[serde(default = "default_scenario_format_version")]
    pub format_version: u32,
    pub id: String,
    pub world: ScenarioWorld,
    pub pose: ScenarioPose,
    #[serde(default)]
    pub commands: Vec<ScenarioCommand>,
    pub run_ticks: u32,
    #[serde(default)]
    pub presentation: ScenarioPresentation,
    #[serde(default)]
    pub expect: Vec<ScenarioExpectation>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct ScenarioWorld {
    pub game_type: GameType,
    pub queue_mode: QueueMode,
    pub rng_seed: Option<u64>,
    /// Canonical dimensions are inferred when these are omitted (60x40 for
    /// team modes, 40x40 otherwise). Custom modes use their own settings.
    #[serde(default)]
    pub arena_width: Option<u16>,
    #[serde(default)]
    pub arena_height: Option<u16>,
    #[serde(default)]
    pub overrides: ScenarioOverrides,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct ScenarioOverrides {
    #[serde(default)]
    pub combo: Option<ScenarioComboOverride>,
    #[serde(default)]
    pub boost: Option<ScenarioBoostOverride>,
    #[serde(default)]
    pub player_idle_timeout_ms: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct ScenarioComboOverride {
    pub window_ms: u32,
    pub max_food_value: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct ScenarioBoostOverride {
    pub speed_milli: u16,
    pub capacity_ms: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct ScenarioPose {
    pub snakes: Vec<ScenarioSnakePose>,
    #[serde(default)]
    pub food: Vec<Position>,
    /// Stable JSON representation: `[[team_id, score], ...]`.
    #[serde(default)]
    pub team_scores: Vec<(u8, u32)>,
    #[serde(default)]
    pub start_tick: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct ScenarioSnakePose {
    pub user_id: u32,
    pub name: String,
    pub body: Vec<Position>,
    pub direction: Direction,
    #[serde(default)]
    pub food: u32,
    #[serde(default)]
    pub team_id: Option<u8>,
    #[serde(default)]
    pub is_alive: Option<bool>,
    #[serde(default)]
    pub boost_charge_ms: u32,
    #[serde(default)]
    pub boost_active: bool,
    #[serde(default)]
    pub combo_chain: u32,
    #[serde(default)]
    pub combo_remaining_ms: u32,
    #[serde(default)]
    pub driver: ScenarioDriver,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum ScenarioDriver {
    #[default]
    Scripted,
    Ai,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct ScenarioCommand {
    pub at_tick: u32,
    pub user_id: u32,
    pub command: ScenarioCommandKind,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum ScenarioCommandKind {
    Turn(Direction),
    ActivateBoost,
    DeactivateBoost,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct ScenarioPresentation {
    #[serde(default)]
    pub camera: ScenarioCamera,
    #[serde(default = "default_time_scale")]
    pub default_time_scale: f32,
    #[serde(default)]
    pub star_snake_id: Option<u32>,
    #[serde(default)]
    pub rotation: i32,
    #[serde(default)]
    pub segments: Vec<ScenarioPlaybackSegment>,
    #[serde(default)]
    pub addons: ScenarioAddons,
}

impl Default for ScenarioPresentation {
    fn default() -> Self {
        Self {
            camera: ScenarioCamera::default(),
            default_time_scale: default_time_scale(),
            star_snake_id: None,
            rotation: 0,
            segments: Vec::new(),
            addons: ScenarioAddons::default(),
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum ScenarioCamera {
    #[default]
    FullArena,
    Fixed {
        x: f32,
        y: f32,
        width: f32,
        height: f32,
    },
    Follow {
        snake_id: u32,
        #[serde(default = "default_camera_deadzone")]
        deadzone: f32,
        #[serde(default = "default_camera_ease")]
        ease: f32,
        /// Horizontal field of view in grid cells. `None` uses the player's
        /// default. Authored per scenario because framing is a staging
        /// decision, not a module constant: a shot staged on the vertical axis
        /// needs a tighter window than one staged across the arena, and the
        /// derived height (`width / aspect`) shrinks as output gets wider.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        width_cells: Option<f32>,
    },
    Track {
        keyframes: Vec<ScenarioCameraKeyframe>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct ScenarioCameraKeyframe {
    pub at_ms: u32,
    pub x: f32,
    pub y: f32,
    pub width: f32,
    pub height: f32,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct ScenarioPlaybackSegment {
    pub until_ms: u32,
    pub time_scale: f32,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct ScenarioAddons {
    #[serde(default)]
    pub combo_callout: bool,
    #[serde(default)]
    pub boost_meter: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum ScenarioExpectation {
    SnakeDead { snake_id: u32, at_tick: u32 },
    FinalSyncHash(String),
}

#[derive(Debug, Clone)]
pub struct LoadedScenario {
    pub script: ScenarioScript,
    pub initial_state: GameState,
    ai_snakes: Vec<(u32, u32)>, // (user_id, snake_id)
}

#[derive(Debug, Clone)]
pub struct ScenarioRun {
    pub final_state: GameState,
    pub events: Vec<(u32, u64, GameEvent)>,
}

/// Incremental authoritative playback for a loaded script.
///
/// Unlike [`GameEngine`], this runtime has no committed/predicted split and no
/// wall-clock lag. It advances the posed state directly, using the production
/// command queue and AI policy, and can deterministically rewind by rebuilding
/// from the loader-owned initial state.
#[derive(Debug, Clone)]
pub struct ScenarioPlayback {
    initial_state: GameState,
    state: GameState,
    ai_snakes: Vec<(u32, u32)>,
    ai_sequences: HashMap<u32, u32>,
    end_tick: u32,
    events: Vec<(u32, u64, GameEvent)>,
}

impl ScenarioPlayback {
    pub fn state(&self) -> &GameState {
        &self.state
    }

    pub fn events(&self) -> &[(u32, u64, GameEvent)] {
        &self.events
    }

    pub fn start_tick(&self) -> u32 {
        self.initial_state.tick
    }

    pub fn end_tick(&self) -> u32 {
        self.end_tick
    }

    pub fn reset(&mut self) {
        self.state = self.initial_state.clone();
        self.ai_sequences.clear();
        self.events.clear();
    }

    /// Advance one real simulation quantum. `false` means the authoritative
    /// rules produce scoring, respawns, and food exactly as they do server-side.
    pub fn advance_one(&mut self) -> Result<bool> {
        if self.state.tick >= self.end_tick || self.state.is_complete() {
            return Ok(false);
        }

        for (user_id, snake_id) in &self.ai_snakes {
            let Some(snake) = self.state.arena.snakes.get(*snake_id as usize) else {
                continue;
            };
            let direction = snake.direction;
            let Some(next_direction) = calculate_ai_move(&self.state, *snake_id, direction) else {
                continue;
            };
            if next_direction == direction {
                continue;
            }
            let sequence = self.ai_sequences.entry(*user_id).or_default();
            *sequence = sequence.saturating_add(1);
            let id = CommandId {
                tick: self.state.tick,
                user_id: *user_id,
                sequence_number: 1_000_000_u32.saturating_add(*sequence),
            };
            self.state.schedule_command(&GameCommandMessage {
                command_id_client: id.clone(),
                command_id_server: Some(id),
                command: GameCommand::Turn {
                    snake_id: *snake_id,
                    direction: next_direction,
                },
            });
        }

        let step = self.state.tick_forward(false)?;
        let event_tick = self.state.tick;
        self.events.extend(
            step.into_iter()
                .map(|(sequence, event)| (event_tick, sequence, event)),
        );
        Ok(true)
    }

    /// Seek to a script tick. Backward seeks replay from the original pose;
    /// forward seeks preserve the current state and do only the missing work.
    pub fn seek_to_tick(&mut self, target_tick: u32) -> Result<u32> {
        let target_tick = target_tick.clamp(self.start_tick(), self.end_tick);
        if target_tick < self.state.tick {
            self.reset();
        }
        while self.state.tick < target_tick && self.advance_one()? {}
        Ok(self.state.tick)
    }

    fn into_run(self) -> ScenarioRun {
        ScenarioRun {
            final_state: self.state,
            events: self.events,
        }
    }
}

impl ScenarioScript {
    pub fn from_json(json: &str) -> Result<Self> {
        let script: Self = serde_json::from_str(json).context("invalid scenario JSON")?;
        script.validate()?;
        Ok(script)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.format_version == SCENARIO_FORMAT_VERSION,
            "unsupported scenario format {}, expected {}",
            self.format_version,
            SCENARIO_FORMAT_VERSION
        );
        ensure!(!self.id.trim().is_empty(), "scenario id must not be empty");
        ensure!(
            !self.pose.snakes.is_empty(),
            "scenario must pose at least one snake"
        );
        ensure!(self.run_ticks > 0, "scenario run_ticks must be positive");
        ensure!(
            (0.1..=4.0).contains(&self.presentation.default_time_scale),
            "default_time_scale must be in 0.1..=4"
        );
        ensure!(
            matches!(
                self.presentation.rotation.rem_euclid(360),
                0 | 90 | 180 | 270
            ),
            "scenario rotation must be a quarter turn"
        );
        let mut last_until = 0;
        for segment in &self.presentation.segments {
            ensure!(
                segment.until_ms > last_until,
                "playback segment boundaries must be strictly increasing"
            );
            ensure!(
                (0.1..=4.0).contains(&segment.time_scale),
                "segment time_scale must be in 0.1..=4"
            );
            last_until = segment.until_ms;
        }
        match &self.presentation.camera {
            ScenarioCamera::Fixed { width, height, .. } => {
                ensure!(
                    *width > 0.0 && *height > 0.0,
                    "fixed camera must have positive size"
                );
            }
            ScenarioCamera::Follow { deadzone, ease, .. } => {
                ensure!(
                    (0.0..=1.0).contains(deadzone),
                    "camera deadzone must be in 0..=1"
                );
                ensure!((0.0..=1.0).contains(ease), "camera ease must be in 0..=1");
            }
            ScenarioCamera::Track { keyframes } => {
                ensure!(
                    !keyframes.is_empty(),
                    "camera track needs at least one keyframe"
                );
                ensure!(
                    keyframes
                        .windows(2)
                        .all(|pair| pair[0].at_ms < pair[1].at_ms),
                    "camera keyframes must be strictly time ordered"
                );
                ensure!(
                    keyframes
                        .iter()
                        .all(|frame| frame.width > 0.0 && frame.height > 0.0),
                    "camera keyframes must have positive size"
                );
            }
            ScenarioCamera::FullArena => {}
        }
        Ok(())
    }

    pub fn load(self) -> Result<LoadedScenario> {
        self.validate()?;
        let (width, height) = scenario_dimensions(&self.world)?;

        let base_boost = boost_config_for(&self.world.game_type, width, height);
        let configured_boost = match (&self.world.overrides.boost, base_boost) {
            (None, existing) => existing,
            (Some(_), None) => bail!("Boost override is not valid for this mode and arena"),
            (Some(override_), Some(mut config)) => {
                config.speed_milli = override_.speed_milli;
                config.capacity_ms = override_.capacity_ms;
                config.packet_charge_ms = override_.capacity_ms / 4;
                config
                    .validate()
                    .context("invalid scenario Boost override")?;
                Some(config)
            }
        };

        let mut state = if let Some(boost) = configured_boost {
            GameState::new_with_boost_config(
                width,
                height,
                self.world.game_type.clone(),
                self.world.queue_mode.clone(),
                self.world.rng_seed,
                0,
                boost,
            )?
        } else {
            GameState::new(
                width,
                height,
                self.world.game_type.clone(),
                self.world.queue_mode.clone(),
                self.world.rng_seed,
                0,
            )
        };

        if let Some(combo) = &self.world.overrides.combo {
            state.properties.combo.window_ms = combo.window_ms;
            state.properties.combo.max_food_value = combo.max_food_value;
        }
        if let Some(timeout) = self.world.overrides.player_idle_timeout_ms {
            ensure!(timeout > 0, "player_idle_timeout_ms must be positive");
            state.properties.player_idle_timeout_ms = timeout;
            state.properties.player_idle_warning_ms = state
                .properties
                .player_idle_warning_ms
                .min(timeout.saturating_sub(1));
        }

        let mut users = HashSet::new();
        let mut ai_users = Vec::new();
        let mut snake_by_user = HashMap::new();
        for pose in &self.pose.snakes {
            ensure!(
                users.insert(pose.user_id),
                "duplicate scenario user_id {}",
                pose.user_id
            );
            validate_body(pose, width, height)?;
            let team_override = pose.team_id.map(TeamId);
            if !matches!(state.game_type, GameType::TeamMatch { .. }) && team_override.is_some() {
                bail!("team_id is only valid in a team scenario");
            }
            let player =
                state.add_player_with_team(pose.user_id, Some(pose.name.clone()), team_override)?;
            snake_by_user.insert(pose.user_id, player.snake_id);
            if pose.driver == ScenarioDriver::Ai {
                ai_users.push((pose.user_id, player.snake_id));
            }
        }

        // Adding a player recalculates every spawn, so apply authored poses
        // only after the complete roster exists.
        for pose in &self.pose.snakes {
            let player_snake_id = *snake_by_user
                .get(&pose.user_id)
                .context("scenario player has no snake mapping")?;
            let snake = state
                .arena
                .snakes
                .get_mut(player_snake_id as usize)
                .context("scenario player has no snake")?;
            snake.body = pose.body.clone();
            snake.direction = pose.direction;
            snake.food = pose.food;
            snake.is_alive = pose.is_alive.unwrap_or(true);
            snake.combo = SnakeCombo {
                chain_count: pose.combo_chain,
                remaining_ms: pose.combo_remaining_ms,
            };

            match state.properties.boost.as_ref() {
                Some(config) => {
                    ensure!(
                        pose.boost_charge_ms <= config.capacity_ms,
                        "snake {} Boost charge exceeds capacity",
                        player_snake_id
                    );
                    ensure!(
                        pose.boost_charge_ms
                            .is_multiple_of(crate::BOOST_TICK_INTERVAL_MS),
                        "snake {} Boost charge must align to the simulation quantum",
                        player_snake_id
                    );
                    if config.unlimited {
                        ensure!(
                            pose.boost_charge_ms == 0 || pose.boost_charge_ms == config.capacity_ms,
                            "unlimited Boost pose must omit charge or use full capacity"
                        );
                        snake.boost.charge_ms = config.capacity_ms;
                    } else {
                        snake.boost.charge_ms = pose.boost_charge_ms;
                    }
                    snake.boost.active = pose.boost_active;
                    snake.boost.intent = pose.boost_active;
                    snake.speed_milli = if pose.boost_active {
                        ensure!(snake.boost.charge_ms > 0, "active Boost pose needs charge");
                        config.speed_milli
                    } else {
                        crate::NORMAL_SNAKE_SPEED_MILLI
                    };
                }
                None => {
                    ensure!(
                        pose.boost_charge_ms == 0 && !pose.boost_active,
                        "Boost pose requires a Boost-enabled mode"
                    );
                }
            }
        }

        validate_food(&self.pose.food, &state)?;
        state.arena.food = self.pose.food.clone();
        if !self.pose.team_scores.is_empty() {
            let scores = state
                .team_scores
                .as_mut()
                .context("team_scores are only valid in a team scenario")?;
            for (team_id, score) in &self.pose.team_scores {
                ensure!(*team_id <= 1, "team score id must be 0 or 1");
                scores.insert(TeamId(*team_id), *score);
            }
        }

        state.tick = self.pose.start_tick;
        state.status = GameStatus::Started { server_id: 0 };
        for tick in state.player_last_activity_ticks.values_mut() {
            *tick = state.tick;
        }

        let end_tick = state.tick.saturating_add(self.run_ticks);
        let mut sequence_by_user: HashMap<u32, u32> = HashMap::new();
        let mut commands = self.commands.clone();
        commands.sort_by_key(|entry| (entry.at_tick, entry.user_id));
        for command in commands {
            ensure!(
                command.at_tick >= state.tick && command.at_tick < end_tick,
                "scenario command at tick {} is outside [{}, {})",
                command.at_tick,
                state.tick,
                end_tick
            );
            let player = state
                .players
                .get(&command.user_id)
                .with_context(|| format!("command references unknown user {}", command.user_id))?;
            let sequence = sequence_by_user.entry(command.user_id).or_default();
            *sequence = sequence.saturating_add(1);
            let id = CommandId {
                tick: command.at_tick,
                user_id: command.user_id,
                sequence_number: *sequence,
            };
            let engine_command = match command.command {
                ScenarioCommandKind::Turn(direction) => GameCommand::Turn {
                    snake_id: player.snake_id,
                    direction,
                },
                ScenarioCommandKind::ActivateBoost => GameCommand::ActivateBoost {
                    snake_id: player.snake_id,
                },
                ScenarioCommandKind::DeactivateBoost => GameCommand::DeactivateBoost {
                    snake_id: player.snake_id,
                },
            };
            state.schedule_command(&GameCommandMessage {
                command_id_client: id.clone(),
                command_id_server: Some(id),
                command: engine_command,
            });
        }

        state
            .validate_boost_invariants()
            .context("scenario pose violates engine invariants")?;
        let loaded = LoadedScenario {
            script: self,
            initial_state: state,
            ai_snakes: ai_users,
        };
        validate_presentation_targets(&loaded)?;
        Ok(loaded)
    }
}

impl LoadedScenario {
    pub fn playback(&self) -> ScenarioPlayback {
        ScenarioPlayback {
            initial_state: self.initial_state.clone(),
            state: self.initial_state.clone(),
            ai_snakes: self.ai_snakes.clone(),
            ai_sequences: HashMap::new(),
            end_tick: self
                .initial_state
                .tick
                .saturating_add(self.script.run_ticks),
            events: Vec::new(),
        }
    }

    pub fn run(&self) -> Result<ScenarioRun> {
        let mut playback = self.playback();
        playback.seek_to_tick(playback.end_tick())?;
        let run = playback.into_run();
        self.assert_expectations(&run)?;
        Ok(run)
    }

    pub fn assert_expectations(&self, run: &ScenarioRun) -> Result<()> {
        for expected in &self.script.expect {
            match expected {
                ScenarioExpectation::SnakeDead { snake_id, at_tick } => ensure!(
                    run.events.iter().any(|(tick, _, event)| {
                        *tick == *at_tick
                            && matches!(
                                event,
                                GameEvent::SnakeDied {
                                    snake_id: observed,
                                    ..
                                } if observed == snake_id
                            )
                    }),
                    "expected snake {snake_id} to die at tick {at_tick}"
                ),
                ScenarioExpectation::FinalSyncHash(expected) => {
                    let expected = parse_sync_hash(expected)?;
                    ensure!(
                        run.final_state.sync_hash() == expected,
                        "final sync hash mismatch: expected {expected:#018x}, got {:#018x}",
                        run.final_state.sync_hash()
                    );
                }
            }
        }
        Ok(())
    }
}

fn scenario_dimensions(world: &ScenarioWorld) -> Result<(u16, u16)> {
    let inferred = match &world.game_type {
        GameType::TeamMatch { .. } => (60, 40),
        GameType::Custom { settings } => (settings.arena_width, settings.arena_height),
        _ => (40, 40),
    };
    let width = world.arena_width.unwrap_or(inferred.0);
    let height = world.arena_height.unwrap_or(inferred.1);
    ensure!(
        width >= 8 && height >= 8,
        "scenario arena must be at least 8x8"
    );
    if let GameType::Custom { settings } = &world.game_type {
        ensure!(
            (width, height) == (settings.arena_width, settings.arena_height),
            "custom scenario dimensions must match CustomGameSettings"
        );
    }
    Ok((width, height))
}

fn validate_body(pose: &ScenarioSnakePose, width: u16, height: u16) -> Result<()> {
    ensure!(
        pose.body.len() >= 2,
        "snake {} body needs head and tail",
        pose.user_id
    );
    for point in &pose.body {
        ensure!(
            point.x >= 0 && point.x < width as i16 && point.y >= 0 && point.y < height as i16,
            "snake {} body point ({},{}) is outside the arena",
            pose.user_id,
            point.x,
            point.y
        );
    }
    for pair in pose.body.windows(2) {
        ensure!(
            pair[0] != pair[1] && (pair[0].x == pair[1].x || pair[0].y == pair[1].y),
            "snake {} body must use distinct axis-aligned compressed segments",
            pose.user_id
        );
    }
    Ok(())
}

fn validate_food(food: &[Position], state: &GameState) -> Result<()> {
    let mut seen = HashSet::new();
    for position in food {
        ensure!(
            position.x >= 0
                && position.x < state.arena.width as i16
                && position.y >= 0
                && position.y < state.arena.height as i16,
            "food ({},{}) is outside the arena",
            position.x,
            position.y
        );
        ensure!(seen.insert(*position), "scenario contains duplicate food");
        ensure!(
            !state.arena.is_boost_pad_position(position),
            "scenario food overlaps a Boost pad"
        );
        ensure!(
            !state
                .arena
                .snakes
                .iter()
                .any(|snake| snake.contains_point(position, false)),
            "scenario food overlaps a snake"
        );
    }
    Ok(())
}

fn validate_presentation_targets(loaded: &LoadedScenario) -> Result<()> {
    let snake_count = loaded.initial_state.arena.snakes.len() as u32;
    if let Some(star) = loaded.script.presentation.star_snake_id {
        ensure!(
            star < snake_count,
            "star_snake_id references a missing snake"
        );
    }
    if let ScenarioCamera::Follow { snake_id, .. } = loaded.script.presentation.camera {
        ensure!(
            snake_id < snake_count,
            "follow camera references a missing snake"
        );
    }
    Ok(())
}

fn parse_sync_hash(value: &str) -> Result<u64> {
    let trimmed = value.trim();
    if let Some(hex) = trimmed.strip_prefix("0x") {
        u64::from_str_radix(hex, 16).context("invalid hexadecimal FinalSyncHash")
    } else {
        trimmed.parse().context("invalid decimal FinalSyncHash")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_script() -> ScenarioScript {
        ScenarioScript {
            format_version: SCENARIO_FORMAT_VERSION,
            id: "deterministic-turn".into(),
            world: ScenarioWorld {
                game_type: GameType::FreeForAll { max_players: 2 },
                queue_mode: QueueMode::Quickmatch,
                rng_seed: None,
                arena_width: None,
                arena_height: None,
                overrides: ScenarioOverrides {
                    player_idle_timeout_ms: Some(600_000),
                    ..Default::default()
                },
            },
            pose: ScenarioPose {
                snakes: vec![
                    ScenarioSnakePose {
                        user_id: 1,
                        name: "YOU".into(),
                        body: vec![Position { x: 12, y: 12 }, Position { x: 8, y: 12 }],
                        direction: Direction::Right,
                        food: 0,
                        team_id: None,
                        is_alive: None,
                        boost_charge_ms: 3_000,
                        boost_active: false,
                        combo_chain: 0,
                        combo_remaining_ms: 0,
                        driver: ScenarioDriver::Scripted,
                    },
                    ScenarioSnakePose {
                        user_id: 2,
                        name: "RIVAL".into(),
                        body: vec![Position { x: 25, y: 25 }, Position { x: 25, y: 29 }],
                        direction: Direction::Up,
                        food: 0,
                        team_id: None,
                        is_alive: None,
                        boost_charge_ms: 0,
                        boost_active: false,
                        combo_chain: 0,
                        combo_remaining_ms: 0,
                        driver: ScenarioDriver::Scripted,
                    },
                ],
                food: vec![Position { x: 20, y: 20 }],
                team_scores: Vec::new(),
                start_tick: 0,
            },
            commands: vec![ScenarioCommand {
                at_tick: 4,
                user_id: 1,
                command: ScenarioCommandKind::Turn(Direction::Down),
            }],
            run_ticks: 12,
            presentation: ScenarioPresentation {
                camera: ScenarioCamera::Follow {
                    snake_id: 0,
                    deadzone: default_camera_deadzone(),
                    ease: default_camera_ease(),
                    width_cells: None,
                },
                star_snake_id: Some(0),
                ..Default::default()
            },
            expect: Vec::new(),
        }
    }

    #[test]
    fn identical_scenarios_produce_identical_events_and_hashes() {
        let loaded = base_script().load().unwrap();
        let first = loaded.run().unwrap();
        let second = loaded.run().unwrap();
        assert_eq!(
            first.final_state.sync_hash(),
            second.final_state.sync_hash()
        );
        assert_eq!(
            serde_json::to_string(&first.events).unwrap(),
            serde_json::to_string(&second.events).unwrap()
        );
    }

    #[test]
    fn incremental_playback_rewinds_to_the_same_authoritative_state() {
        let loaded = base_script().load().unwrap();
        let mut playback = loaded.playback();
        let start_tick = playback.start_tick();

        playback.seek_to_tick(start_tick + 10).unwrap();
        let first_hash = playback.state().sync_hash();
        let first_events = playback.events().to_vec();

        playback.seek_to_tick(start_tick + 3).unwrap();
        assert_eq!(playback.state().tick, start_tick + 3);
        playback.seek_to_tick(start_tick + 10).unwrap();

        assert_eq!(playback.state().sync_hash(), first_hash);
        assert_eq!(
            serde_json::to_string(playback.events()).unwrap(),
            serde_json::to_string(&first_events).unwrap()
        );
    }

    #[test]
    fn presentation_rotation_rejects_non_quarter_turns() {
        let mut script = base_script();
        script.presentation.rotation = 45;
        assert!(
            script
                .validate()
                .unwrap_err()
                .to_string()
                .contains("quarter turn")
        );
    }

    #[test]
    fn unsafe_and_unknown_overrides_fail_closed() {
        let json = serde_json::to_string(&base_script()).unwrap();
        let json = json.replace(
            "\"player_idle_timeout_ms\":600000",
            "\"player_idle_timeout_ms\":600000,\"tick_duration_ms\":1",
        );
        let error = ScenarioScript::from_json(&json).unwrap_err().to_string();
        assert!(error.contains("invalid scenario JSON"));
    }

    #[test]
    fn final_hash_expectation_accepts_hex_and_rejects_drift() {
        let script = base_script();
        let expected = script
            .clone()
            .load()
            .unwrap()
            .run()
            .unwrap()
            .final_state
            .sync_hash();
        let mut matching = script.clone();
        matching.expect = vec![ScenarioExpectation::FinalSyncHash(format!("{expected:#x}"))];
        matching.load().unwrap().run().unwrap();

        let mut drifting = script;
        drifting.expect = vec![ScenarioExpectation::FinalSyncHash("0x1".into())];
        assert!(drifting.load().unwrap().run().is_err());
    }

    #[test]
    fn malformed_pose_and_command_are_rejected() {
        let mut duplicate = base_script();
        duplicate.pose.snakes[1].user_id = 1;
        assert!(
            duplicate
                .load()
                .unwrap_err()
                .to_string()
                .contains("duplicate")
        );

        let mut unknown_user = base_script();
        unknown_user.commands[0].user_id = 999;
        assert!(
            unknown_user
                .load()
                .unwrap_err()
                .to_string()
                .contains("unknown user")
        );
    }

    #[test]
    fn every_checked_in_scenario_runs_deterministically() {
        let fixtures = [
            include_str!("../../client/web/scenarios/demolition-cutoff.json"),
            include_str!("../../client/web/scenarios/combo-frenzy.json"),
            include_str!("../../client/web/scenarios/team-bank.json"),
        ];
        for fixture in fixtures {
            let script = ScenarioScript::from_json(fixture).unwrap();
            let loaded = script.load().unwrap();
            let first = loaded.run().unwrap();
            let second = loaded.run().unwrap();
            assert_eq!(
                first.final_state.sync_hash(),
                second.final_state.sync_hash()
            );
            assert!(!loaded.script.expect.is_empty());
        }
    }

    #[test]
    fn trailer_scenarios_cover_their_editorial_cues_without_frozen_padding() {
        let fixtures = [
            (
                "demolition",
                include_str!("../../tools/video/scenarios/demolition-cutoff.json"),
            ),
            (
                "bank",
                include_str!("../../tools/video/scenarios/team-45pt-celebration.json"),
            ),
            (
                "combo",
                include_str!("../../tools/video/scenarios/boost-combo-clutch.json"),
            ),
        ];

        for (kind, fixture) in fixtures {
            let script = ScenarioScript::from_json(fixture).unwrap();
            let expected_end_tick = script.pose.start_tick + script.run_ticks;
            let loaded = script.load().unwrap();
            let first = loaded.run().unwrap();
            let second = loaded.run().unwrap();
            assert_eq!(first.final_state.tick, expected_end_tick, "{kind}");
            assert_eq!(
                first.final_state.sync_hash(),
                second.final_state.sync_hash(),
                "{kind}"
            );

            let has_editorial_cue = first.events.iter().any(|(_, _, event)| match kind {
                "demolition" => matches!(
                    event,
                    GameEvent::SnakeDied {
                        snake_id: 1,
                        cause: crate::DeathCause::SnakeBody { killer_snake_id: 0 }
                    }
                ),
                "bank" => matches!(
                    event,
                    GameEvent::SnakeDied {
                        snake_id: 0,
                        cause: crate::DeathCause::Banked
                    }
                ),
                "combo" => matches!(
                    event,
                    GameEvent::FoodEaten {
                        snake_id: 0,
                        combo_chain,
                        boost_active: true,
                        ..
                    } if *combo_chain >= 2
                ),
                _ => false,
            });
            assert!(has_editorial_cue, "{kind} fixture lacks its editorial cue");
        }
    }
}
