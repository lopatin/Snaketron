//! Deterministic scenario playback through the production arena renderer.
//!
//! The shared loader owns all privileged state posing. This module only drives
//! its authoritative playback, exposes compact per-frame/cue metadata, and
//! renders a full arena into a reusable scratch canvas before camera-cropping
//! it into the caller's canvas.

use crate::render;
use common::{
    DeathCause, GameEvent, GameState, HighlightClip, LoadedScenario, Position, RecordedGameMessage,
    ScenarioAddons, ScenarioCamera, ScenarioPlayback, ScenarioPlaybackSegment,
    ScenarioPresentation, ScenarioScript, SnakeCrash, TeamGoal,
    advance_and_apply_replicated_message,
};
use serde::Serialize;
use std::collections::HashMap;
use wasm_bindgen::{JsCast, closure::Closure, prelude::*};

const FOLLOW_CAMERA_WIDTH_CELLS: f64 = 26.0;

/// Largest cell size the live game ever draws, in CSS pixels.
///
/// `GameArena` starts at 15 and only ever shrinks to fit the viewport
/// (`client/web/components/GameArena.tsx:679`), so 15 is the game's true
/// visual scale on any monitor. A scenario that zooms past it renders food,
/// grid dots and snakes far larger than the DOM addons beside them — the
/// canvas scales with the camera while the boost meter and callouts stay in
/// CSS pixels — and the shot stops looking like the product. Capture reaches
/// 1080p with `deviceScaleFactor`, not by zooming the arena.
const MAX_CELL_SIZE_CSS_PX: f64 = 15.0;

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
struct CameraRect {
    x: f64,
    y: f64,
    width: f64,
    height: f64,
}

impl CameraRect {
    fn full(width: f64, height: f64) -> Self {
        Self {
            x: 0.0,
            y: 0.0,
            width,
            height,
        }
    }

    fn clamped(self, arena_width: f64, arena_height: f64) -> Self {
        let width = self.width.clamp(f64::EPSILON, arena_width);
        let height = self.height.clamp(f64::EPSILON, arena_height);
        Self {
            x: self.x.clamp(0.0, (arena_width - width).max(0.0)),
            y: self.y.clamp(0.0, (arena_height - height).max(0.0)),
            width,
            height,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct ScenarioSnakeFrame {
    snake_id: u32,
    user_id: Option<u32>,
    head: Option<Position>,
    is_alive: bool,
}

#[derive(Clone, Debug, Serialize)]
struct ScenarioHeadFrame {
    tick: u32,
    snakes: Vec<ScenarioSnakeFrame>,
}

#[derive(Clone, Debug, Serialize)]
struct ScenarioPickupCue {
    tick: u32,
    sequence: u64,
    snake_id: u32,
    position: Position,
    points: u32,
    combo_chain: u32,
    combo_remaining_ms_before: u32,
    boost_active: bool,
}

#[derive(Clone, Debug, Serialize)]
struct ScenarioDeathCue {
    tick: u32,
    sequence: u64,
    snake_id: u32,
    cause: DeathCause,
    hold_position: Option<Position>,
}

#[derive(Clone, Debug, Serialize)]
struct ScenarioCueTrack {
    tick_duration_ms: u32,
    start_tick: u32,
    end_tick: u32,
    crashes: Vec<SnakeCrash>,
    goals: Vec<TeamGoal>,
    pickups: Vec<ScenarioPickupCue>,
    deaths: Vec<ScenarioDeathCue>,
    heads: Vec<ScenarioHeadFrame>,
}

impl ScenarioCueTrack {
    fn death_hold(
        &self,
        snake_id: u32,
        at_tick: u32,
        camera_focus_tick: Option<u32>,
    ) -> Option<Position> {
        self.deaths
            .iter()
            .rev()
            .find(|death| {
                death.snake_id == snake_id
                    && death.tick <= at_tick
                    // A death in highlight pre-roll must not freeze the
                    // camera through a later respawned payoff. Once the focus
                    // is reached, the newest death becomes the final hold.
                    && camera_focus_tick.is_none_or(|focus| death.tick >= focus)
            })
            .and_then(|death| death.hold_position)
    }

    fn follow_head(
        &self,
        snake_id: u32,
        at_tick: u32,
        camera_focus_tick: Option<u32>,
    ) -> Option<Position> {
        self.death_hold(snake_id, at_tick, camera_focus_tick)
            .or_else(|| {
                self.heads
                    .iter()
                    .rev()
                    .find(|frame| frame.tick <= at_tick)
                    .and_then(|frame| {
                        frame
                            .snakes
                            .iter()
                            .find(|snake| snake.snake_id == snake_id)
                            .and_then(|snake| snake.head)
                    })
            })
    }
}

fn user_id_for_snake(state: &GameState, snake_id: u32) -> Option<u32> {
    state
        .players
        .iter()
        .filter_map(|(user_id, player)| (player.snake_id == snake_id).then_some(*user_id))
        .min()
}

fn head_frame_at(state: &GameState, tick: u32) -> ScenarioHeadFrame {
    ScenarioHeadFrame {
        tick,
        snakes: state
            .arena
            .snakes
            .iter()
            .enumerate()
            .map(|(snake_id, snake)| ScenarioSnakeFrame {
                snake_id: snake_id as u32,
                user_id: user_id_for_snake(state, snake_id as u32),
                head: snake.head().ok().copied(),
                is_alive: snake.is_alive,
            })
            .collect(),
    }
}

fn heads_by_snake(frame: &ScenarioHeadFrame) -> HashMap<u32, Position> {
    frame
        .snakes
        .iter()
        .filter_map(|snake| snake.head.map(|head| (snake.snake_id, head)))
        .collect()
}

trait ScenarioTimeline: Clone {
    fn state(&self) -> &GameState;
    fn events(&self) -> &[(u32, u64, GameEvent)];
    fn start_tick(&self) -> u32;
    fn end_tick(&self) -> u32;
    fn cursor_tick(&self) -> u32;
    fn advance_one(&mut self) -> anyhow::Result<bool>;
    fn seek_to_tick(&mut self, target_tick: u32) -> anyhow::Result<u32>;
}

impl ScenarioTimeline for ScenarioPlayback {
    fn state(&self) -> &GameState {
        self.state()
    }

    fn events(&self) -> &[(u32, u64, GameEvent)] {
        self.events()
    }

    fn start_tick(&self) -> u32 {
        self.start_tick()
    }

    fn end_tick(&self) -> u32 {
        self.end_tick()
    }

    fn cursor_tick(&self) -> u32 {
        self.state().tick
    }

    fn advance_one(&mut self) -> anyhow::Result<bool> {
        self.advance_one()
    }

    fn seek_to_tick(&mut self, target_tick: u32) -> anyhow::Result<u32> {
        self.seek_to_tick(target_tick)
    }
}

/// Movement-only playback for a server-cut highlight. A separate logical
/// cursor keeps terminal post-roll advancing even though the immutable final
/// GameState correctly stops ticking after match completion.
#[derive(Clone)]
struct HighlightPlayback {
    game_id: u32,
    initial_state: GameState,
    state: GameState,
    messages: Vec<RecordedGameMessage>,
    next_message: usize,
    start_tick: u32,
    end_tick: u32,
    cursor_tick: u32,
    events: Vec<(u32, u64, GameEvent)>,
}

impl HighlightPlayback {
    fn new(clip: &HighlightClip) -> Result<Self, String> {
        // This is both the compatibility gate and the mandatory end-hash
        // assertion. Never let a version-skewed or incomplete clip render as
        // if it were authoritative.
        clip.replay_and_verify()
            .map_err(|error| error.to_string())?;

        let mut initial_state = clip.anchor.clone();
        for message in clip
            .messages
            .iter()
            .filter(|message| message.tick <= clip.window.start_tick)
        {
            initial_state = advance_and_apply_replicated_message(
                &initial_state,
                &message.envelope(clip.game_id),
            )
            .map_err(|error| error.to_string())?;
        }
        while initial_state.tick < clip.window.start_tick && !initial_state.is_complete() {
            initial_state
                .tick_forward(true)
                .map_err(|error| error.to_string())?;
        }

        let messages = clip
            .messages
            .iter()
            .filter(|message| message.tick > clip.window.start_tick)
            .cloned()
            .collect();
        Ok(Self {
            game_id: clip.game_id,
            state: initial_state.clone(),
            initial_state,
            messages,
            next_message: 0,
            start_tick: clip.window.start_tick,
            end_tick: clip.window.end_tick,
            cursor_tick: clip.window.start_tick,
            events: Vec::new(),
        })
    }

    fn reset(&mut self) {
        self.state = self.initial_state.clone();
        self.next_message = 0;
        self.cursor_tick = self.start_tick;
        self.events.clear();
    }
}

impl ScenarioTimeline for HighlightPlayback {
    fn state(&self) -> &GameState {
        &self.state
    }

    fn events(&self) -> &[(u32, u64, GameEvent)] {
        &self.events
    }

    fn start_tick(&self) -> u32 {
        self.start_tick
    }

    fn end_tick(&self) -> u32 {
        self.end_tick
    }

    fn cursor_tick(&self) -> u32 {
        self.cursor_tick
    }

    fn advance_one(&mut self) -> anyhow::Result<bool> {
        if self.cursor_tick >= self.end_tick {
            return Ok(false);
        }
        let target_tick = self.cursor_tick.saturating_add(1).min(self.end_tick);
        while let Some(message) = self.messages.get(self.next_message) {
            if message.tick > target_tick {
                break;
            }
            self.state =
                advance_and_apply_replicated_message(&self.state, &message.envelope(self.game_id))?;
            self.events
                .push((message.tick, message.sequence, message.event.clone()));
            self.next_message += 1;
        }
        while self.state.tick < target_tick && !self.state.is_complete() {
            self.state.tick_forward(true)?;
        }
        self.cursor_tick = target_tick;
        Ok(true)
    }

    fn seek_to_tick(&mut self, target_tick: u32) -> anyhow::Result<u32> {
        let target_tick = target_tick.clamp(self.start_tick, self.end_tick);
        if target_tick < self.cursor_tick {
            self.reset();
        }
        while self.cursor_tick < target_tick && self.advance_one()? {}
        Ok(self.state.tick)
    }
}

#[derive(Clone)]
enum ScenarioFrameSource {
    Script(ScenarioPlayback),
    Highlight(HighlightPlayback),
}

impl ScenarioTimeline for ScenarioFrameSource {
    fn state(&self) -> &GameState {
        match self {
            Self::Script(playback) => playback.state(),
            Self::Highlight(playback) => playback.state(),
        }
    }

    fn events(&self) -> &[(u32, u64, GameEvent)] {
        match self {
            Self::Script(playback) => playback.events(),
            Self::Highlight(playback) => playback.events(),
        }
    }

    fn start_tick(&self) -> u32 {
        match self {
            Self::Script(playback) => playback.start_tick(),
            Self::Highlight(playback) => playback.start_tick(),
        }
    }

    fn end_tick(&self) -> u32 {
        match self {
            Self::Script(playback) => playback.end_tick(),
            Self::Highlight(playback) => playback.end_tick(),
        }
    }

    fn cursor_tick(&self) -> u32 {
        match self {
            Self::Script(playback) => playback.cursor_tick(),
            Self::Highlight(playback) => playback.cursor_tick(),
        }
    }

    fn advance_one(&mut self) -> anyhow::Result<bool> {
        match self {
            Self::Script(playback) => playback.advance_one(),
            Self::Highlight(playback) => playback.advance_one(),
        }
    }

    fn seek_to_tick(&mut self, target_tick: u32) -> anyhow::Result<u32> {
        match self {
            Self::Script(playback) => playback.seek_to_tick(target_tick),
            Self::Highlight(playback) => playback.seek_to_tick(target_tick),
        }
    }
}

fn build_cue_track<T: ScenarioTimeline>(mut playback: T) -> Result<ScenarioCueTrack, String> {
    let tick_duration_ms = playback.state().properties.tick_duration_ms.max(1);
    let start_tick = playback.start_tick();
    let end_tick = playback.end_tick();
    let mut crashes = Vec::new();
    let mut goals = Vec::new();
    let mut pickups = Vec::new();
    let mut deaths = Vec::new();
    let mut heads = vec![head_frame_at(playback.state(), playback.cursor_tick())];

    loop {
        let previous_heads = heads_by_snake(heads.last().expect("initial head frame"));
        let event_start = playback.events().len();
        if !playback.advance_one().map_err(|error| error.to_string())? {
            break;
        }

        let state = playback.state();
        let tick = playback.cursor_tick();
        let tick_crashes: Vec<_> = state
            .recent_crashes
            .iter()
            .filter(|crash| crash.tick == tick)
            .cloned()
            .collect();
        let tick_goals: Vec<_> = state
            .recent_goals
            .iter()
            .filter(|goal| goal.tick == tick)
            .cloned()
            .collect();
        let events = playback.events()[event_start..].to_vec();

        for (event_tick, sequence, event) in events {
            match event {
                GameEvent::FoodEaten {
                    snake_id,
                    position,
                    points,
                    combo_chain,
                    combo_remaining_ms_before,
                    boost_active,
                } => pickups.push(ScenarioPickupCue {
                    tick: event_tick,
                    sequence,
                    snake_id,
                    position,
                    points,
                    combo_chain,
                    combo_remaining_ms_before,
                    boost_active,
                }),
                GameEvent::SnakeDied { snake_id, cause } => {
                    // Collisions carry their attempted impact cell; banking
                    // carries the goal-entry cell. Other causes retain the
                    // last rendered head instead of jumping to a respawn.
                    let hold_position = tick_crashes
                        .iter()
                        .find(|crash| crash.snake_id == snake_id)
                        .map(|crash| crash.position)
                        .or_else(|| {
                            tick_goals
                                .iter()
                                .find(|goal| goal.snake_id == snake_id)
                                .map(|goal| goal.position)
                        })
                        .or_else(|| previous_heads.get(&snake_id).copied());
                    deaths.push(ScenarioDeathCue {
                        tick: event_tick,
                        sequence,
                        snake_id,
                        cause,
                        hold_position,
                    });
                }
                _ => {}
            }
        }

        crashes.extend(tick_crashes);
        goals.extend(tick_goals);
        heads.push(head_frame_at(state, tick));
    }

    Ok(ScenarioCueTrack {
        tick_duration_ms,
        start_tick,
        end_tick,
        crashes,
        goals,
        pickups,
        deaths,
        heads,
    })
}

struct ScenarioCore {
    playback: ScenarioFrameSource,
    presentation: ScenarioPresentation,
    cue_track: ScenarioCueTrack,
    duration_ms: u32,
    poster_ms: u32,
    camera_focus_tick: Option<u32>,
    elapsed_ms: u32,
}

impl ScenarioCore {
    fn from_json(script_json: &str) -> Result<Self, String> {
        let script = ScenarioScript::from_json(script_json).map_err(|error| error.to_string())?;
        let loaded = script.load().map_err(|error| error.to_string())?;
        Self::from_loaded(loaded)
    }

    fn from_loaded(loaded: LoadedScenario) -> Result<Self, String> {
        let tick_duration_ms = loaded.initial_state.properties.tick_duration_ms.max(1);
        let duration_ms = loaded.script.run_ticks.saturating_mul(tick_duration_ms);
        loaded.run().map_err(|error| error.to_string())?;
        let playback = ScenarioFrameSource::Script(loaded.playback());
        let cue_track = build_cue_track(playback.clone())?;
        let presentation = loaded.script.presentation.clone();
        Ok(Self {
            playback,
            presentation,
            cue_track,
            duration_ms,
            poster_ms: duration_ms,
            camera_focus_tick: None,
            elapsed_ms: 0,
        })
    }

    fn from_highlight_json(clip_json: &str) -> Result<Self, String> {
        let clip: HighlightClip =
            serde_json::from_str(clip_json).map_err(|error| error.to_string())?;
        let tick_duration_ms = clip.anchor.properties.tick_duration_ms.max(1);
        let playback = ScenarioFrameSource::Highlight(HighlightPlayback::new(&clip)?);
        let duration_ms = clip
            .window
            .end_tick
            .saturating_sub(clip.window.start_tick)
            .saturating_mul(tick_duration_ms);
        let poster_ms = clip
            .window
            .focus_tick
            .saturating_sub(clip.window.start_tick)
            .saturating_mul(tick_duration_ms)
            .min(duration_ms);
        let presentation = ScenarioPresentation {
            camera: ScenarioCamera::Follow {
                snake_id: clip.presentation.follow_snake_id,
                deadzone: 0.2,
                ease: 0.16,
                // Play of the Game keeps the player default; the band's
                // framing is tuned in §5.5, not per clip.
                width_cells: None,
            },
            default_time_scale: 1.0,
            star_snake_id: Some(clip.star_snake_id),
            rotation: clip.presentation.rotation,
            segments: clip
                .presentation
                .segments
                .iter()
                .map(|segment| ScenarioPlaybackSegment {
                    until_ms: segment
                        .until_tick
                        .saturating_sub(clip.window.start_tick)
                        .saturating_mul(tick_duration_ms)
                        .min(duration_ms),
                    time_scale: segment.time_scale,
                })
                .collect(),
            addons: ScenarioAddons {
                combo_callout: true,
                boost_meter: true,
            },
        };
        let cue_track = build_cue_track(playback.clone())?;
        Ok(Self {
            playback,
            presentation,
            cue_track,
            duration_ms,
            poster_ms,
            camera_focus_tick: Some(clip.window.focus_tick),
            elapsed_ms: 0,
        })
    }

    fn target_tick(&self, elapsed_ms: u32) -> u32 {
        let tick_ms = self.playback.state().properties.tick_duration_ms.max(1);
        self.playback
            .start_tick()
            .saturating_add(elapsed_ms.min(self.duration_ms) / tick_ms)
            .min(self.playback.end_tick())
    }

    fn seek_elapsed(&mut self, elapsed_ms: u32) -> Result<u32, String> {
        let elapsed_ms = elapsed_ms.min(self.duration_ms);
        let target_tick = self.target_tick(elapsed_ms);
        let rendered_tick = self
            .playback
            .seek_to_tick(target_tick)
            .map_err(|error| error.to_string())?;
        self.elapsed_ms = elapsed_ms;
        Ok(rendered_tick)
    }

    fn state(&self) -> &GameState {
        self.playback.state()
    }

    fn rotation(&self) -> i32 {
        self.presentation.rotation.rem_euclid(360)
    }

    fn local_user_id(&self) -> Option<u32> {
        let camera_snake = match &self.presentation.camera {
            ScenarioCamera::Follow { snake_id, .. } => Some(*snake_id),
            _ => None,
        };
        let preferred_snake = self.presentation.star_snake_id.or(camera_snake);
        preferred_snake
            .and_then(|snake_id| user_id_for_snake(self.state(), snake_id))
            .or_else(|| self.state().players.keys().copied().min())
    }

    fn frame_meta_json(&self) -> Result<String, String> {
        let state = self.state();
        let snakes: Vec<_> = state
            .arena
            .snakes
            .iter()
            .enumerate()
            .map(|(snake_id, snake)| {
                serde_json::json!({
                    "snake_id": snake_id,
                    "user_id": user_id_for_snake(state, snake_id as u32),
                    "head": snake.head().ok(),
                    "is_alive": snake.is_alive,
                    "food": snake.food,
                    "boost": snake.boost(),
                    "combo": &snake.combo,
                })
            })
            .collect();
        serde_json::to_string(&serde_json::json!({
            "elapsed_ms": self.elapsed_ms,
            "tick": state.tick,
            "predicted_tick": state.tick,
            "committed_tick": state.tick,
            "tick_duration_ms": state.properties.tick_duration_ms,
            "arena_width": state.arena.width,
            "arena_height": state.arena.height,
            "rotation": self.rotation(),
            "star_snake_id": self.presentation.star_snake_id,
            "addons": &self.presentation.addons,
            "boost_config": &state.properties.boost,
            "combo_config": &state.properties.combo,
            "cues": &state.recent_crashes,
            "goals": &state.recent_goals,
            "snakes": snakes,
        }))
        .map_err(|error| error.to_string())
    }

    fn camera_rect(&self, elapsed_ms: u32, target_width: f64, target_height: f64) -> CameraRect {
        let state = self.state();
        let rotation = self.rotation();
        let game_width = f64::from(state.arena.width);
        let game_height = f64::from(state.arena.height);
        let (arena_width, arena_height) =
            render::get_effective_dimensions(game_width, game_height, rotation);

        let camera = match &self.presentation.camera {
            ScenarioCamera::FullArena => CameraRect::full(arena_width, arena_height),
            ScenarioCamera::Fixed {
                x,
                y,
                width,
                height,
            } => CameraRect {
                x: f64::from(*x),
                y: f64::from(*y),
                width: f64::from(*width),
                height: f64::from(*height),
            },
            ScenarioCamera::Track { keyframes } => {
                let elapsed_ms = elapsed_ms.min(self.duration_ms);
                let first = &keyframes[0];
                let last = &keyframes[keyframes.len() - 1];
                if elapsed_ms <= first.at_ms {
                    CameraRect {
                        x: f64::from(first.x),
                        y: f64::from(first.y),
                        width: f64::from(first.width),
                        height: f64::from(first.height),
                    }
                } else if elapsed_ms >= last.at_ms {
                    CameraRect {
                        x: f64::from(last.x),
                        y: f64::from(last.y),
                        width: f64::from(last.width),
                        height: f64::from(last.height),
                    }
                } else {
                    let pair = keyframes
                        .windows(2)
                        .find(|pair| elapsed_ms <= pair[1].at_ms)
                        .expect("validated track brackets elapsed time");
                    let span = (pair[1].at_ms - pair[0].at_ms) as f64;
                    let progress = (elapsed_ms - pair[0].at_ms) as f64 / span;
                    let lerp = |from: f32, to: f32| {
                        f64::from(from) + (f64::from(to) - f64::from(from)) * progress
                    };
                    CameraRect {
                        x: lerp(pair[0].x, pair[1].x),
                        y: lerp(pair[0].y, pair[1].y),
                        width: lerp(pair[0].width, pair[1].width),
                        height: lerp(pair[0].height, pair[1].height),
                    }
                }
            }
            ScenarioCamera::Follow {
                snake_id,
                deadzone,
                ease,
                width_cells,
            } => {
                let aspect = (target_width / target_height).max(f64::EPSILON);
                let requested = width_cells
                    .map(f64::from)
                    .filter(|w| w.is_finite() && *w > 0.0)
                    .unwrap_or(FOLLOW_CAMERA_WIDTH_CELLS);
                let mut width = requested.min(arena_width);
                let mut height = width / aspect;
                if height > arena_height {
                    height = arena_height;
                    width = (height * aspect).min(arena_width);
                }

                let target_tick = self.target_tick(elapsed_ms);
                let mut center: Option<(f64, f64)> = None;
                let deadzone_x = width * f64::from(*deadzone) / 2.0;
                let deadzone_y = height * f64::from(*deadzone) / 2.0;
                let ease = f64::from(*ease);

                for frame in self
                    .cue_track
                    .heads
                    .iter()
                    .filter(|frame| frame.tick <= target_tick)
                {
                    let head =
                        self.cue_track
                            .follow_head(*snake_id, frame.tick, self.camera_focus_tick);
                    let Some(head) = head else {
                        continue;
                    };
                    let (x, y) = render::transform_coords(
                        f64::from(head.x),
                        f64::from(head.y),
                        game_width,
                        game_height,
                        rotation,
                    );
                    let head_center = (x + 0.5, y + 0.5);
                    let Some((center_x, center_y)) = center else {
                        center = Some(head_center);
                        continue;
                    };
                    let desired_x = if head_center.0 < center_x - deadzone_x {
                        head_center.0 + deadzone_x
                    } else if head_center.0 > center_x + deadzone_x {
                        head_center.0 - deadzone_x
                    } else {
                        center_x
                    };
                    let desired_y = if head_center.1 < center_y - deadzone_y {
                        head_center.1 + deadzone_y
                    } else if head_center.1 > center_y + deadzone_y {
                        head_center.1 - deadzone_y
                    } else {
                        center_y
                    };
                    center = Some((
                        center_x + (desired_x - center_x) * ease,
                        center_y + (desired_y - center_y) * ease,
                    ));
                }

                let (center_x, center_y) =
                    center.unwrap_or((arena_width / 2.0, arena_height / 2.0));
                CameraRect {
                    x: center_x - width / 2.0,
                    y: center_y - height / 2.0,
                    width,
                    height,
                }
            }
        };
        camera.clamped(arena_width, arena_height)
    }
}

fn create_scratch_canvas() -> Result<web_sys::HtmlCanvasElement, JsValue> {
    let document = web_sys::window()
        .and_then(|window| window.document())
        .ok_or_else(|| JsValue::from_str("scenario playback needs a DOM document"))?;
    document
        .create_element("canvas")?
        .dyn_into::<web_sys::HtmlCanvasElement>()
        .map_err(|_| JsValue::from_str("failed to create the scenario scratch canvas"))
}

fn render_scenario_frame(
    core: &ScenarioCore,
    elapsed_ms: u32,
    scratch: &web_sys::HtmlCanvasElement,
    target: &web_sys::HtmlCanvasElement,
    draw_celebration: &js_sys::Function,
    draw_post_snakes: &js_sys::Function,
) -> Result<(), JsValue> {
    let target_width = f64::from(target.width());
    let target_height = f64::from(target.height());
    if target_width <= 0.0 || target_height <= 0.0 {
        return Ok(());
    }

    let mut camera = core.camera_rect(elapsed_ms, target_width, target_height);
    let mut cell_size = (target_width / camera.width).min(target_height / camera.height);
    if !cell_size.is_finite() || cell_size <= 0.0 {
        return Ok(());
    }

    // Never draw the arena larger than the live game does. `target` is sized in
    // device pixels while the DOM addons beside it lay out in CSS pixels, so a
    // camera tight enough to exceed the game's own 15px cell blows the food,
    // grid and snakes up out of proportion with the boost meter and callouts.
    // When the requested window would do that, widen the window about its
    // centre instead of scaling up — the shot gets more arena, not a zoom.
    let client_width = f64::from(target.client_width());
    let device_pixel_ratio = if client_width > 0.0 {
        target_width / client_width
    } else {
        1.0
    };
    let max_cell_size = MAX_CELL_SIZE_CSS_PX * device_pixel_ratio;
    if max_cell_size.is_finite() && max_cell_size > 0.0 && cell_size > max_cell_size {
        let center_x = camera.x + camera.width / 2.0;
        let center_y = camera.y + camera.height / 2.0;
        cell_size = max_cell_size;
        camera.width = target_width / cell_size;
        camera.height = target_height / cell_size;
        camera.x = center_x - camera.width / 2.0;
        camera.y = center_y - camera.height / 2.0;
    }

    let state = core.state();
    let rotation = core.rotation();
    let (arena_width, arena_height) = render::get_effective_dimensions(
        f64::from(state.arena.width),
        f64::from(state.arena.height),
        rotation,
    );
    let frame_width = (arena_width * cell_size).ceil() as u32 + 2;
    let frame_height = (arena_height * cell_size).ceil() as u32 + 2;
    if scratch.width() != frame_width {
        scratch.set_width(frame_width);
    }
    if scratch.height() != frame_height {
        scratch.set_height(frame_height);
    }

    render::render_game_state(
        state,
        scratch,
        render::FrameOptions {
            cell_size,
            local_user_id: core.local_user_id(),
            rotation,
            // Capture is deterministic by construction: the harness owns the
            // clock, so an animated skin must not paint from a wall clock.
            // Reduced motion is off because a trailer is a video, not a UI.
            anim_ms: 0.0,
            reduced_motion: false,
            local_skin_ref: None,
        },
        draw_celebration,
        draw_post_snakes,
    )?;

    let context = target
        .get_context("2d")?
        .ok_or_else(|| JsValue::from_str("scenario target has no 2d context"))?
        .dyn_into::<web_sys::CanvasRenderingContext2d>()
        .map_err(|_| JsValue::from_str("failed to cast the scenario target context"))?;
    let source_width = camera.width * cell_size;
    let source_height = camera.height * cell_size;
    let destination_x = ((target_width - source_width) / 2.0).max(0.0);
    let destination_y = ((target_height - source_height) / 2.0).max(0.0);
    context.set_fill_style_str("#ffffff");
    context.fill_rect(0.0, 0.0, target_width, target_height);
    context.draw_image_with_html_canvas_element_and_sw_and_sh_and_dx_and_dy_and_dw_and_dh(
        scratch,
        camera.x * cell_size + 1.0,
        camera.y * cell_size + 1.0,
        source_width,
        source_height,
        destination_x,
        destination_y,
        source_width,
        source_height,
    )?;
    Ok(())
}

/// WASM boundary for deterministic scenario playback.
#[wasm_bindgen]
pub struct ScenarioPlayer {
    core: ScenarioCore,
    scratch: web_sys::HtmlCanvasElement,
    noop_effect: Closure<dyn FnMut()>,
}

#[wasm_bindgen]
impl ScenarioPlayer {
    #[wasm_bindgen(constructor)]
    pub fn new(script_json: &str) -> Result<ScenarioPlayer, JsValue> {
        console_error_panic_hook::set_once();
        Ok(Self {
            core: ScenarioCore::from_json(script_json)
                .map_err(|error| JsValue::from_str(&error))?,
            scratch: create_scratch_canvas()?,
            noop_effect: Closure::new(|| {}),
        })
    }

    /// Construct the same player from a server-cut, movement-only replay.
    /// The clip is compatibility-checked and replayed through its end hash
    /// before the first frame is exposed to JavaScript.
    #[wasm_bindgen(js_name = fromHighlightClip)]
    pub fn from_highlight_clip(clip_json: &str) -> Result<ScenarioPlayer, JsValue> {
        console_error_panic_hook::set_once();
        Ok(Self {
            core: ScenarioCore::from_highlight_json(clip_json)
                .map_err(|error| JsValue::from_str(&error))?,
            scratch: create_scratch_canvas()?,
            noop_effect: Closure::new(|| {}),
        })
    }

    #[wasm_bindgen(js_name = durationMs)]
    pub fn duration_ms(&self) -> u32 {
        self.core.duration_ms
    }

    #[wasm_bindgen(js_name = posterMs)]
    pub fn poster_ms(&self) -> u32 {
        self.core.poster_ms
    }

    #[wasm_bindgen(js_name = renderedTick)]
    pub fn rendered_tick(&self) -> u32 {
        self.core.state().tick
    }

    #[wasm_bindgen(js_name = seek)]
    pub fn seek(&mut self, elapsed_ms: u32) -> Result<u32, JsValue> {
        self.core
            .seek_elapsed(elapsed_ms)
            .map_err(|error| JsValue::from_str(&error))
    }

    #[wasm_bindgen(js_name = frameMetaJson)]
    pub fn frame_meta_json(&mut self, elapsed_ms: u32) -> Result<String, JsValue> {
        self.seek(elapsed_ms)?;
        self.core
            .frame_meta_json()
            .map_err(|error| JsValue::from_str(&error))
    }

    #[wasm_bindgen(js_name = cueTrackJson)]
    pub fn cue_track_json(&self) -> Result<String, JsValue> {
        serde_json::to_string(&self.core.cue_track)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    #[wasm_bindgen(js_name = currentStateJson)]
    pub fn current_state_json(&self) -> Result<String, JsValue> {
        serde_json::to_string(self.core.state())
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    #[wasm_bindgen(js_name = renderFrame)]
    pub fn render_frame(
        &mut self,
        elapsed_ms: u32,
        target: &web_sys::HtmlCanvasElement,
    ) -> Result<(), JsValue> {
        self.seek(elapsed_ms)?;
        let noop: &js_sys::Function = self.noop_effect.as_ref().unchecked_ref();
        render_scenario_frame(&self.core, elapsed_ms, &self.scratch, target, noop, noop)
    }

    #[wasm_bindgen(js_name = renderFrameWithEffects)]
    pub fn render_frame_with_effects(
        &mut self,
        elapsed_ms: u32,
        target: &web_sys::HtmlCanvasElement,
        draw_celebration: &js_sys::Function,
        draw_post_snakes: &js_sys::Function,
    ) -> Result<(), JsValue> {
        self.seek(elapsed_ms)?;
        render_scenario_frame(
            &self.core,
            elapsed_ms,
            &self.scratch,
            target,
            draw_celebration,
            draw_post_snakes,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{
        Direction, GameType, QueueMode, SCENARIO_FORMAT_VERSION, ScenarioAddons, ScenarioCommand,
        ScenarioCommandKind, ScenarioDriver, ScenarioOverrides, ScenarioPose, ScenarioPresentation,
        ScenarioSnakePose, ScenarioWorld,
    };

    fn script_json() -> String {
        serde_json::to_string(&ScenarioScript {
            format_version: SCENARIO_FORMAT_VERSION,
            id: "wasm-boundary-seek".into(),
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
                    deadzone: 0.2,
                    ease: 0.16,
                    width_cells: None,
                },
                star_snake_id: Some(0),
                addons: ScenarioAddons {
                    combo_callout: true,
                    boost_meter: true,
                },
                ..Default::default()
            },
            expect: Vec::new(),
        })
        .unwrap()
    }

    /// Every advertised payoff must be inside the camera at the frame it
    /// happens (PRD P3.6). This is the check that build 1 lacked: its
    /// "DEMOLITIONS!" hero shot ran a Follow camera on the killer's head while
    /// the victim died 3 cells below the bottom edge, so the kill appeared in
    /// no frame of the 7-second shot — and every mechanical QC gate still
    /// passed, because the capture only recorded the star's head.
    #[test]
    fn trailer_payoffs_are_inside_the_camera_at_1920x1080() {
        const CAPTURE_W: f64 = 1920.0;
        const CAPTURE_H: f64 = 1080.0;

        let fixtures = [
            (
                "demolition-cutoff",
                include_str!("../../tools/video/scenarios/demolition-cutoff.json"),
            ),
            (
                "team-45pt-celebration",
                include_str!("../../tools/video/scenarios/team-45pt-celebration.json"),
            ),
            (
                "boost-combo-clutch",
                include_str!("../../tools/video/scenarios/boost-combo-clutch.json"),
            ),
            (
                "ffa-four-snakes",
                include_str!("../../tools/video/scenarios/ffa-four-snakes.json"),
            ),
            (
                "solo-classic-run",
                include_str!("../../tools/video/scenarios/solo-classic-run.json"),
            ),
        ];

        for (name, json) in fixtures {
            let script = common::ScenarioScript::from_json(json).unwrap();
            let run = script.clone().load().unwrap().run().unwrap();
            let tick_ms = run.final_state.properties.tick_duration_ms;
            let star = script.presentation.star_snake_id.unwrap_or(0);

            // Deaths are the payoff whose participants are easiest to pin
            // down: the victim must be visible, not merely the camera subject.
            //
            // Only deaths the *star* is party to are payoffs. A populated
            // arena (narrative.md §7) has extras running their own lanes, and
            // one of them leaving the field at the far side of the map is set
            // dressing that is deliberately staged off camera — asserting on
            // it would make "put more snakes in frame" and "keep the payoff in
            // frame" mutually exclusive.
            let deaths: Vec<(u32, u32)> = run
                .events
                .iter()
                .filter_map(|(tick, _, event)| match event {
                    common::GameEvent::SnakeDied {
                        snake_id, cause, ..
                    } => {
                        let killer = match cause {
                            common::DeathCause::SnakeBody { killer_snake_id } => {
                                Some(*killer_snake_id)
                            }
                            common::DeathCause::HeadToHead { other_snake_id } => {
                                Some(*other_snake_id)
                            }
                            _ => None,
                        };
                        (*snake_id == star || killer == Some(star)).then_some((*tick, *snake_id))
                    }
                    _ => None,
                })
                .collect();

            for (tick, victim_id) in deaths {
                let mut player = ScenarioCore::from_json(json).unwrap();
                player.seek_elapsed(tick * tick_ms).unwrap();
                let rect = player.camera_rect(tick * tick_ms, CAPTURE_W, CAPTURE_H);

                let victim = player
                    .state()
                    .arena
                    .snakes
                    .get(victim_id as usize)
                    .unwrap_or_else(|| panic!("{name}: victim {victim_id} missing at tick {tick}"));
                let head = *victim.head().unwrap();
                let (x, y) = (f64::from(head.x), f64::from(head.y));

                assert!(
                    x >= rect.x
                        && x <= rect.x + rect.width
                        && y >= rect.y
                        && y <= rect.y + rect.height,
                    "{name}: the payoff is off camera — victim {victim_id} died at cell \
                     ({x}, {y}) on tick {tick}, but the camera rect is \
                     x[{:.2}, {:.2}] y[{:.2}, {:.2}]. Restage the shot or tighten \
                     `presentation.camera.Follow.width_cells`.",
                    rect.x,
                    rect.x + rect.width,
                    rect.y,
                    rect.y + rect.height,
                );
            }
        }
    }

    #[test]
    fn backward_seek_replays_to_the_same_state_as_a_fresh_player() {
        let json = script_json();
        let mut player = ScenarioCore::from_json(&json).unwrap();
        let tick_ms = player.state().properties.tick_duration_ms;
        player.seek_elapsed(tick_ms * 10).unwrap();
        let expected_hash = player.state().sync_hash();
        let expected_json = serde_json::to_string(player.state()).unwrap();

        player.seek_elapsed(tick_ms * 3).unwrap();
        assert_eq!(player.state().tick, 3);
        player.seek_elapsed(tick_ms * 10).unwrap();

        let mut fresh = ScenarioCore::from_json(&json).unwrap();
        fresh.seek_elapsed(tick_ms * 10).unwrap();
        assert_eq!(player.state().sync_hash(), expected_hash);
        assert_eq!(player.state().sync_hash(), fresh.state().sync_hash());
        assert_eq!(
            serde_json::to_string(player.state()).unwrap(),
            expected_json
        );
    }

    #[test]
    fn boundary_metadata_uses_authoritative_ticks_and_full_cue_contract() {
        let mut player = ScenarioCore::from_json(&script_json()).unwrap();
        let tick_ms = player.state().properties.tick_duration_ms;
        assert_eq!(player.duration_ms, tick_ms * 12);
        player.seek_elapsed(tick_ms * 5 + tick_ms / 2).unwrap();
        assert_eq!(player.state().tick, 5);

        let meta: serde_json::Value =
            serde_json::from_str(&player.frame_meta_json().unwrap()).unwrap();
        assert_eq!(meta["tick"], 5);
        assert_eq!(meta["predicted_tick"], meta["committed_tick"]);
        assert_eq!(meta["snakes"].as_array().unwrap().len(), 2);
        assert_eq!(meta["addons"]["combo_callout"], true);

        let cues = serde_json::to_value(&player.cue_track).unwrap();
        assert!(cues.get("heads").is_some());
        assert!(cues.get("crashes").is_some());
        assert!(cues.get("goals").is_some());
        assert!(cues.get("pickups").is_some());
    }

    #[test]
    fn highlight_clips_use_movement_only_seek_and_reject_bad_end_hashes() {
        let loaded = ScenarioScript::from_json(&script_json())
            .unwrap()
            .load()
            .unwrap();
        let anchor = loaded.initial_state.clone();
        let mut expected = anchor.clone();
        for _ in 0..4 {
            expected.tick_forward(true).unwrap();
        }
        let tick_ms = anchor.properties.tick_duration_ms;
        let clip = HighlightClip {
            clip_format_version: common::HIGHLIGHT_CLIP_FORMAT_VERSION,
            gameplay_version: common::GAMEPLAY_REPLAY_VERSION,
            game_id: 71,
            star_user_id: 1,
            star_snake_id: 0,
            star_name: "YOU".into(),
            reason: common::HighlightReason::FeedingFrenzy { pickups: 8 },
            score: 120,
            breakdown: common::HighlightScoreBreakdown::default(),
            window: common::HighlightWindow {
                start_tick: 0,
                end_tick: 4,
                focus_tick: 2,
            },
            anchor,
            messages: Vec::new(),
            end_sync_hash: expected.sync_hash(),
            presentation: common::HighlightPresentation {
                rotation: 90,
                follow_snake_id: 0,
                segments: vec![common::HighlightSpeedSegment {
                    until_tick: 4,
                    time_scale: 0.5,
                }],
            },
            config: common::HighlightConfig::default(),
        };
        let json = serde_json::to_string(&clip).unwrap();
        let mut player = ScenarioCore::from_highlight_json(&json).unwrap();
        assert_eq!(player.duration_ms, tick_ms * 4);
        assert_eq!(player.poster_ms, tick_ms * 2);
        assert_eq!(player.presentation.rotation, 90);
        assert_eq!(player.presentation.segments[0].until_ms, tick_ms * 4);
        player.seek_elapsed(player.duration_ms).unwrap();
        assert_eq!(player.state().sync_hash(), expected.sync_hash());

        let mut corrupt = clip;
        corrupt.end_sync_hash ^= 1;
        assert!(
            ScenarioCore::from_highlight_json(&serde_json::to_string(&corrupt).unwrap()).is_err()
        );
    }

    #[test]
    fn fixed_and_track_cameras_clamp_and_interpolate_in_rotated_space() {
        let mut player = ScenarioCore::from_json(&script_json()).unwrap();
        player.presentation.rotation = 90;
        player.presentation.camera = ScenarioCamera::Fixed {
            x: 35.0,
            y: 35.0,
            width: 20.0,
            height: 20.0,
        };
        assert_eq!(
            player.camera_rect(0, 160.0, 100.0),
            CameraRect {
                x: 20.0,
                y: 20.0,
                width: 20.0,
                height: 20.0,
            }
        );

        player.presentation.camera = ScenarioCamera::Track {
            keyframes: vec![
                common::ScenarioCameraKeyframe {
                    at_ms: 0,
                    x: 0.0,
                    y: 0.0,
                    width: 20.0,
                    height: 10.0,
                },
                common::ScenarioCameraKeyframe {
                    at_ms: 100,
                    x: 10.0,
                    y: 8.0,
                    width: 16.0,
                    height: 8.0,
                },
            ],
        };
        assert_eq!(
            player.camera_rect(50, 160.0, 100.0),
            CameraRect {
                x: 5.0,
                y: 4.0,
                width: 18.0,
                height: 9.0,
            }
        );
    }

    #[test]
    fn follow_camera_holds_the_banking_cell_instead_of_following_the_respawn() {
        let player = ScenarioCore::from_json(include_str!("../web/scenarios/team-bank.json"))
            .expect("checked-in team-bank scenario");
        let death = player
            .cue_track
            .deaths
            .iter()
            .find(|death| death.snake_id == 0)
            .expect("banking death cue");
        assert_eq!(death.cause, DeathCause::Banked);
        let goal = player
            .cue_track
            .goals
            .iter()
            .find(|goal| goal.snake_id == 0 && goal.tick == death.tick)
            .expect("banking goal cue");
        assert_eq!(death.hold_position, Some(goal.position));
        assert_eq!(
            player
                .cue_track
                .follow_head(0, player.cue_track.end_tick, None),
            Some(goal.position),
        );
    }

    #[test]
    fn highlight_camera_ignores_pre_roll_death_but_holds_latest_payoff_death() {
        let track = ScenarioCueTrack {
            tick_duration_ms: 50,
            start_tick: 0,
            end_tick: 30,
            crashes: Vec::new(),
            goals: Vec::new(),
            pickups: Vec::new(),
            deaths: vec![
                ScenarioDeathCue {
                    tick: 5,
                    sequence: 1,
                    snake_id: 0,
                    cause: DeathCause::Wall,
                    hold_position: Some(Position { x: 5, y: 5 }),
                },
                ScenarioDeathCue {
                    tick: 22,
                    sequence: 2,
                    snake_id: 0,
                    cause: DeathCause::Wall,
                    hold_position: Some(Position { x: 22, y: 22 }),
                },
            ],
            heads: vec![ScenarioHeadFrame {
                tick: 15,
                snakes: vec![ScenarioSnakeFrame {
                    snake_id: 0,
                    user_id: Some(1),
                    head: Some(Position { x: 15, y: 15 }),
                    is_alive: true,
                }],
            }],
        };

        assert_eq!(
            track.follow_head(0, 15, Some(20)),
            Some(Position { x: 15, y: 15 })
        );
        assert_eq!(
            track.follow_head(0, 30, Some(20)),
            Some(Position { x: 22, y: 22 })
        );
    }
}
