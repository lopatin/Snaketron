//! Animated pre-match tutorial scenes drawn by the real game renderer.
//!
//! Every frame is a genuine [`GameState`] built from the same constructors as
//! production matches and painted by [`render::render_game_state`]. The
//! timelines only pose states at the game's real 50 ms Boost quantum; they do
//! not draw substitute arrows, sprites, or CSS approximations over the arena.
//!
//! Each scene owns one fixed 16:10 camera. A [`TutorialScenePlayer`] reuses a
//! detached full-arena canvas while it renders successive states and crops the
//! result into the caller's canvas. Keeping the ordinary renderer's full frame
//! intact preserves its one-pixel padding, grid, walls, snake skins, food, and
//! NOS artwork exactly.

use crate::render;
use common::{
    BOOST_TICK_INTERVAL_MS, Direction, GameState, GameType, Player, Position, QueueMode, Snake,
    SnakeCrash, TeamId,
};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

const TEAM_ARENA_WIDTH: u16 = 60;
const TEAM_ARENA_HEIGHT: u16 = 40;
const FIELD_ARENA_SIZE: u16 = 40;
const SCENE_DURATION_MS: u32 = 2_400;
const SCENE_POSTER_MS: u32 = SCENE_DURATION_MS;
const CAMERA_WIDTH: f64 = 24.0;
const CAMERA_HEIGHT: f64 = 15.0;

/// Any fixed seed produces the same constructor state every time. Tutorial
/// frames replace food explicitly, but retaining an RNG keeps construction on
/// the ordinary production path.
const SCENE_RNG_SEED: u64 = 20_260_806;

/// The region of an arena presented by one timeline, in grid cells.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Camera {
    x: f64,
    y: f64,
    width: f64,
    height: f64,
}

impl Camera {
    const fn focused(x: f64, y: f64) -> Self {
        Self {
            x,
            y,
            width: CAMERA_WIDTH,
            height: CAMERA_HEIGHT,
        }
    }
}

struct Scene {
    state: GameState,
    camera: Camera,
}

type SceneFrameBuilder = fn(u32) -> GameState;

struct SceneDefinition {
    id: &'static str,
    duration_ms: u32,
    poster_ms: u32,
    camera: Camera,
    build_frame: SceneFrameBuilder,
}

impl SceneDefinition {
    fn frame(&self, elapsed_ms: u32) -> Scene {
        let elapsed_ms = quantize_elapsed(elapsed_ms.min(self.duration_ms));
        Scene {
            state: (self.build_frame)(elapsed_ms),
            camera: self.camera,
        }
    }
}

fn quantize_elapsed(elapsed_ms: u32) -> u32 {
    elapsed_ms - elapsed_ms % BOOST_TICK_INTERVAL_MS
}

fn set_timeline_tick(state: &mut GameState, elapsed_ms: u32) {
    state.tick = elapsed_ms / BOOST_TICK_INTERVAL_MS;
}

/// Integer interpolation deliberately advances on grid cells. The caller has
/// already quantized time to the real simulation cadence, so the renderer
/// never invents sub-cell snake positions that gameplay cannot occupy.
fn lerp_cell(elapsed_ms: u32, start_ms: u32, end_ms: u32, start: i16, end: i16) -> i16 {
    if elapsed_ms <= start_ms {
        return start;
    }
    if elapsed_ms >= end_ms || start_ms >= end_ms {
        return end;
    }

    let elapsed = i64::from(elapsed_ms - start_ms);
    let duration = i64::from(end_ms - start_ms);
    let distance = i64::from(end - start);
    (i64::from(start) + distance * elapsed / duration) as i16
}

fn position(x: i16, y: i16) -> Position {
    Position { x, y }
}

/// Pose a straight snake at a fixed length, with its tail trailing opposite
/// its direction of travel. Tutorial motion translates both ends so movement
/// never masquerades as growth.
fn straight_body(head: Position, direction: Direction, length: i16) -> Vec<Position> {
    debug_assert!(length >= 2);
    let tail_distance = length - 1;
    let tail = match direction {
        Direction::Up => position(head.x, head.y + tail_distance),
        Direction::Down => position(head.x, head.y - tail_distance),
        Direction::Left => position(head.x + tail_distance, head.y),
        Direction::Right => position(head.x - tail_distance, head.y),
    };
    vec![head, tail]
}

fn add_crash_cue(state: &mut GameState, snake_id: u32, crash_position: Position) {
    state.recent_crashes.push(SnakeCrash {
        tick: state.tick,
        snake_id,
        position: crash_position,
    });
}

/// A team match laid out exactly as matchmaking builds one: 60x40, 10-cell
/// end zones, a centered goal opening, and the canonical twelve-canister NOS
/// field.
fn team_scene_state(elapsed_ms: u32) -> GameState {
    let mut state = GameState::new(
        TEAM_ARENA_WIDTH,
        TEAM_ARENA_HEIGHT,
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Quickmatch,
        Some(SCENE_RNG_SEED),
        0,
    );
    set_timeline_tick(&mut state, elapsed_ms);

    // The renderer resolves end-zone labels from these production maps.
    state.usernames.insert(1, "YOU".to_owned());
    state.usernames.insert(2, "RIVAL".to_owned());
    state.players.insert(
        1,
        Player {
            user_id: 1,
            snake_id: 0,
        },
    );
    state.players.insert(
        2,
        Player {
            user_id: 2,
            snake_id: 1,
        },
    );
    state.arena.snakes.clear();
    state.arena.food.clear();
    state
}

/// A 40x40 open field matching Solo and FFA. A free-for-all receives the real
/// teamless NOS layout from `GameState::new`; Solo receives its real unlimited
/// Boost configuration and no pads.
fn field_scene_state(game_type: GameType, elapsed_ms: u32) -> GameState {
    let mut state = GameState::new(
        FIELD_ARENA_SIZE,
        FIELD_ARENA_SIZE,
        game_type,
        QueueMode::Quickmatch,
        Some(SCENE_RNG_SEED),
        0,
    );
    set_timeline_tick(&mut state, elapsed_ms);
    state.arena.snakes.clear();
    state.arena.food.clear();
    state
}

/// Place a Solo snake while retaining the full unlimited tank created by the
/// real player-admission path. Active illustration frames use the dedicated
/// visual constructor because gameplay deliberately exposes no public Boost
/// state mutator.
fn solo_scene_with_snake(
    elapsed_ms: u32,
    body: Vec<Position>,
    direction: Direction,
    food: u32,
    boost_active: bool,
) -> GameState {
    // Player admission is intentionally restricted to tick zero. Construct
    // and place the snake there, then stamp the tutorial timeline tick once
    // the ordinary admission path has supplied Solo's full unlimited tank.
    let mut state = field_scene_state(GameType::Solo, 0);
    state.usernames.insert(1, "YOU".to_owned());

    if boost_active {
        state.arena.snakes.push(Snake::for_illustration(
            body, direction, None, food, true, true,
        ));
        state.players.insert(
            1,
            Player {
                user_id: 1,
                snake_id: 0,
            },
        );
    } else {
        let player = state
            .add_player(1, Some("YOU".to_owned()))
            .expect("a tutorial Solo arena always has room for its one player");
        let snake = &mut state.arena.snakes[player.snake_id as usize];
        snake.body = body;
        snake.direction = direction;
        snake.food = food;
        snake.is_alive = true;
    }

    set_timeline_tick(&mut state, elapsed_ms);
    state.arena.food.clear();
    state
}

/// Team carry: a food-laden snake approaches its own gate, crosses it, and is
/// reset to starting length as the banked team score increases.
fn frame_team_carry(elapsed_ms: u32) -> GameState {
    const BANK_MS: u32 = 1_550;
    let mut state = team_scene_state(elapsed_ms);
    let banked = elapsed_ms >= BANK_MS;

    let snake = if banked {
        if let Some(scores) = state.team_scores.as_mut() {
            scores.insert(TeamId(0), 1);
        }
        Snake::for_illustration(
            vec![position(4, 20), position(7, 20)],
            Direction::Left,
            Some(TeamId(0)),
            0,
            true,
            false,
        )
    } else {
        let head_x = lerp_cell(elapsed_ms, 250, 1_500, 18, 8);
        Snake::for_illustration(
            straight_body(position(head_x, 20), Direction::Left, 6),
            Direction::Left,
            Some(TeamId(0)),
            0,
            true,
            false,
        )
    };
    state.arena.snakes.push(snake);
    state.arena.food = vec![position(17, 15), position(20, 25), position(23, 18)];
    state
}

/// Team NOS: the snake reaches a quarter-tank packet, the authoritative pad
/// enters cooldown, and the same production skin shows active Boost.
fn frame_team_boost(elapsed_ms: u32) -> GameState {
    const PICKUP_MS: u32 = 1_300;
    let mut state = team_scene_state(elapsed_ms);
    let boosted = elapsed_ms >= PICKUP_MS;

    let snake = if boosted {
        let head_x = lerp_cell(elapsed_ms, PICKUP_MS, 2_050, 26, 31);
        if let Some(packet) = state.arena.boost_pads.iter_mut().find(|pad| pad.id == 4) {
            packet.respawn_at_tick = Some(state.tick + 160);
        }
        Snake::for_illustration(
            straight_body(position(head_x, 12), Direction::Right, 7),
            Direction::Right,
            Some(TeamId(0)),
            0,
            true,
            true,
        )
    } else {
        let head_x = lerp_cell(elapsed_ms, 250, 1_250, 18, 25);
        Snake::for_illustration(
            straight_body(position(head_x, 12), Direction::Right, 7),
            Direction::Right,
            Some(TeamId(0)),
            0,
            true,
            false,
        )
    };
    state.arena.snakes.push(snake);
    state.arena.food = vec![position(20, 16)];
    state
}

/// Enemy-base danger: a live local snake crosses the rival boundary and the
/// renderer's ordinary dead-snake path replaces it with a pale body and X.
fn frame_team_danger(elapsed_ms: u32) -> GameState {
    const CRASH_MS: u32 = 1_550;
    let mut state = team_scene_state(elapsed_ms);
    let crashed = elapsed_ms >= CRASH_MS;
    let local = if crashed {
        Snake::for_illustration(
            straight_body(position(49, 20), Direction::Right, 5),
            Direction::Right,
            Some(TeamId(0)),
            0,
            false,
            false,
        )
    } else {
        let head_x = lerp_cell(elapsed_ms, 250, 1_500, 42, 49);
        Snake::for_illustration(
            straight_body(position(head_x, 20), Direction::Right, 5),
            Direction::Right,
            Some(TeamId(0)),
            0,
            true,
            false,
        )
    };
    state.arena.snakes.push(local);
    state.arena.snakes.push(Snake::for_illustration(
        vec![position(45, 22), position(45, 27)],
        Direction::Up,
        Some(TeamId(1)),
        0,
        true,
        false,
    ));
    if crashed {
        // Production rolls the body back to x=49 and records the attempted
        // enemy-base cell separately for its transient crash effect.
        add_crash_cue(&mut state, 0, position(50, 20));
    }
    state.arena.food = vec![position(40, 16), position(46, 26)];
    state
}

/// FFA food: the collision tick keeps the original length and queues two
/// growth cells; the next two occupied cells extrude that growth behind the
/// head exactly as `Snake::step_forward` does.
fn frame_ffa_food(elapsed_ms: u32) -> GameState {
    const EAT_MS: u32 = 1_450;
    const GROWN_MS: u32 = 1_900;
    let eaten = elapsed_ms >= EAT_MS;
    let (body, pending_growth) = if eaten {
        let head_x = lerp_cell(elapsed_ms, EAT_MS, GROWN_MS, 22, 24);
        let grown = u32::try_from(head_x - 22).unwrap_or(0);
        (vec![position(head_x, 20), position(18, 20)], 2 - grown)
    } else {
        let head_x = lerp_cell(elapsed_ms, 250, 1_400, 12, 21);
        (straight_body(position(head_x, 20), Direction::Right, 5), 0)
    };
    let mut state = field_scene_state(GameType::FreeForAll { max_players: 4 }, elapsed_ms);
    state.arena.snakes.push(Snake::for_illustration(
        body,
        Direction::Right,
        None,
        pending_growth,
        true,
        false,
    ));

    state.arena.food = vec![position(14, 14), position(27, 25)];
    if !eaten {
        state.arena.food.push(position(22, 20));
    }
    state
}

/// Solo food includes a real turn. It rounds the corner at (20,23), eats at
/// (20,18), then spends the next two moves extruding precisely two cells.
fn frame_solo_food(elapsed_ms: u32) -> GameState {
    const TURN_MS: u32 = 800;
    const EAT_MS: u32 = 1_450;
    const GROWN_MS: u32 = 1_750;

    let (body, direction, pending_growth) = if elapsed_ms < TURN_MS {
        let head_x = lerp_cell(elapsed_ms, 150, 750, 14, 20);
        (
            straight_body(position(head_x, 23), Direction::Right, 5),
            Direction::Right,
            0,
        )
    } else if elapsed_ms < EAT_MS {
        let head_y = lerp_cell(elapsed_ms, TURN_MS, 1_400, 23, 19);
        let vertical_distance = 23 - head_y;
        let horizontal_distance = 4 - vertical_distance;
        let mut body = vec![position(20, head_y), position(20, 23)];
        if horizontal_distance > 0 {
            body.push(position(20 - horizontal_distance, 23));
        }
        body.dedup();
        (body, Direction::Up, 0)
    } else if elapsed_ms <= GROWN_MS {
        let head_y = lerp_cell(elapsed_ms, EAT_MS, GROWN_MS, 18, 16);
        let grown = u32::try_from(18 - head_y).unwrap_or(0);
        (
            vec![position(20, head_y), position(20, 22)],
            Direction::Up,
            2 - grown,
        )
    } else {
        let head_y = lerp_cell(elapsed_ms, GROWN_MS, 2_100, 16, 14);
        (
            straight_body(position(20, head_y), Direction::Up, 7),
            Direction::Up,
            0,
        )
    };

    let mut state = solo_scene_with_snake(elapsed_ms, body, direction, pending_growth, false);
    state.arena.food = vec![position(14, 14), position(27, 25)];
    if elapsed_ms < EAT_MS {
        state.arena.food.push(position(20, 18));
    }
    state
}

/// FFA collision: the local snake advances into a rival's body, then the same
/// state is rendered through the production dead-snake/X path.
fn frame_ffa_crash(elapsed_ms: u32) -> GameState {
    const CRASH_MS: u32 = 1_500;
    let mut state = field_scene_state(GameType::FreeForAll { max_players: 4 }, elapsed_ms);
    let crashed = elapsed_ms >= CRASH_MS;
    let local = if crashed {
        Snake::for_illustration(
            straight_body(position(18, 15), Direction::Right, 5),
            Direction::Right,
            None,
            0,
            false,
            false,
        )
    } else {
        let head_x = lerp_cell(elapsed_ms, 250, 1_450, 12, 18);
        Snake::for_illustration(
            straight_body(position(head_x, 15), Direction::Right, 5),
            Direction::Right,
            None,
            0,
            true,
            false,
        )
    };
    state.arena.snakes.push(local);
    state.arena.snakes.push(Snake::for_illustration(
        vec![position(19, 12), position(19, 23)],
        Direction::Up,
        None,
        0,
        true,
        false,
    ));
    if crashed {
        // The rival owns (19,15). Gameplay restores the local body to its
        // last valid head at x=18, leaving its dead/X marker unobscured.
        add_crash_cue(&mut state, 0, position(19, 15));
    }
    state.arena.food = vec![position(13, 24)];
    state
}

/// FFA NOS: the local snake collects the real field-layout packet at (16,12),
/// which disappears into cooldown before the active Boost contour advances.
fn frame_ffa_boost(elapsed_ms: u32) -> GameState {
    const PICKUP_MS: u32 = 1_250;
    let mut state = field_scene_state(GameType::FreeForAll { max_players: 4 }, elapsed_ms);
    let boosted = elapsed_ms >= PICKUP_MS;
    let snake = if boosted {
        let head_x = lerp_cell(elapsed_ms, PICKUP_MS, 2_050, 16, 24);
        if let Some(packet) = state.arena.boost_pads.iter_mut().find(|pad| pad.id == 4) {
            packet.respawn_at_tick = Some(state.tick + 160);
        }
        Snake::for_illustration(
            straight_body(position(head_x, 12), Direction::Right, 6),
            Direction::Right,
            None,
            0,
            true,
            true,
        )
    } else {
        let head_x = lerp_cell(elapsed_ms, 250, 1_200, 10, 15);
        Snake::for_illustration(
            straight_body(position(head_x, 12), Direction::Right, 6),
            Direction::Right,
            None,
            0,
            true,
            false,
        )
    };
    state.arena.snakes.push(snake);
    state
}

/// Solo Boost: there is no pickup phase. The real unlimited configuration
/// remains on the state while the snake switches from its full idle tank to an
/// active production Boost skin and crosses the frame quickly.
fn frame_solo_boost(elapsed_ms: u32) -> GameState {
    const BOOST_MS: u32 = 700;
    let boosted = elapsed_ms >= BOOST_MS;
    let head_x = if boosted {
        lerp_cell(elapsed_ms, BOOST_MS, 1_950, 17, 29)
    } else {
        lerp_cell(elapsed_ms, 150, 650, 12, 17)
    };
    let mut state = solo_scene_with_snake(
        elapsed_ms,
        straight_body(position(head_x, 18), Direction::Right, 5),
        Direction::Right,
        0,
        boosted,
    );
    state.arena.food = vec![position(30, 18)];
    state
}

/// Solo run: the snake curls around a tight clockwise loop. Its final upward
/// move would enter the still-occupied top edge, so production-style rollback
/// leaves the body on its last valid pose and the renderer shows a dead X.
fn frame_solo_run(elapsed_ms: u32) -> GameState {
    const RIGHT_TURN_MS: u32 = 950;
    const DOWN_TURN_MS: u32 = 1_300;
    const LEFT_TURN_MS: u32 = 1_650;
    const LAST_SAFE_MS: u32 = 1_900;
    const CRASH_MS: u32 = 1_950;

    let crashed = elapsed_ms >= CRASH_MS;
    let (body, direction) = if elapsed_ms < RIGHT_TURN_MS {
        let head_x = lerp_cell(elapsed_ms, 100, 900, 15, 21);
        (
            straight_body(position(head_x, 17), Direction::Right, 13),
            Direction::Right,
        )
    } else if elapsed_ms < DOWN_TURN_MS {
        let head_y = lerp_cell(elapsed_ms, RIGHT_TURN_MS, 1_250, 17, 20);
        let travelled = head_y - 17;
        let tail_x = 21 - (12 - travelled);
        let mut body = vec![position(21, head_y), position(21, 17), position(tail_x, 17)];
        body.dedup();
        (body, Direction::Down)
    } else if elapsed_ms < LEFT_TURN_MS {
        let head_x = lerp_cell(elapsed_ms, DOWN_TURN_MS, 1_600, 21, 18);
        let travelled = 3 + (21 - head_x);
        let tail_x = 21 - (12 - travelled);
        let mut body = vec![
            position(head_x, 20),
            position(21, 20),
            position(21, 17),
            position(tail_x, 17),
        ];
        body.dedup();
        (body, Direction::Left)
    } else {
        let head_y = if crashed {
            18
        } else {
            lerp_cell(elapsed_ms, LEFT_TURN_MS, LAST_SAFE_MS, 20, 18)
        };
        let travelled = 6 + (20 - head_y);
        let tail_x = 21 - (12 - travelled);
        let mut body = vec![
            position(18, head_y),
            position(18, 20),
            position(21, 20),
            position(21, 17),
            position(tail_x, 17),
        ];
        body.dedup();
        (body, Direction::Up)
    };

    let mut state = solo_scene_with_snake(elapsed_ms, body.clone(), direction, 0, false);
    if crashed {
        state.arena.snakes[0] = Snake::for_illustration(body, direction, None, 0, false, false);
        // (18,17) is still part of the top edge when the head at (18,18)
        // attempts to move upward, making this an actual self-collision.
        add_crash_cue(&mut state, 0, position(18, 17));
    }
    state.arena.food = vec![position(29, 12), position(30, 24)];
    state
}

const SCENES: &[SceneDefinition] = &[
    SceneDefinition {
        id: "team-carry",
        duration_ms: SCENE_DURATION_MS,
        poster_ms: SCENE_POSTER_MS,
        camera: Camera::focused(0.0, 13.0),
        build_frame: frame_team_carry,
    },
    SceneDefinition {
        id: "team-boost",
        duration_ms: SCENE_DURATION_MS,
        poster_ms: SCENE_POSTER_MS,
        camera: Camera::focused(10.0, 3.0),
        build_frame: frame_team_boost,
    },
    SceneDefinition {
        id: "team-danger",
        duration_ms: SCENE_DURATION_MS,
        poster_ms: SCENE_POSTER_MS,
        camera: Camera::focused(36.0, 13.0),
        build_frame: frame_team_danger,
    },
    SceneDefinition {
        id: "ffa-food",
        duration_ms: SCENE_DURATION_MS,
        poster_ms: SCENE_POSTER_MS,
        camera: Camera::focused(7.0, 12.0),
        build_frame: frame_ffa_food,
    },
    SceneDefinition {
        id: "ffa-boost",
        duration_ms: SCENE_DURATION_MS,
        poster_ms: SCENE_POSTER_MS,
        camera: Camera::focused(2.0, 3.0),
        build_frame: frame_ffa_boost,
    },
    SceneDefinition {
        id: "ffa-crash",
        duration_ms: SCENE_DURATION_MS,
        poster_ms: SCENE_POSTER_MS,
        camera: Camera::focused(7.0, 10.0),
        build_frame: frame_ffa_crash,
    },
    SceneDefinition {
        id: "solo-food",
        duration_ms: SCENE_DURATION_MS,
        poster_ms: SCENE_POSTER_MS,
        camera: Camera::focused(7.0, 12.0),
        build_frame: frame_solo_food,
    },
    SceneDefinition {
        id: "solo-boost",
        duration_ms: SCENE_DURATION_MS,
        poster_ms: SCENE_POSTER_MS,
        camera: Camera::focused(7.0, 11.0),
        build_frame: frame_solo_boost,
    },
    SceneDefinition {
        id: "solo-run",
        duration_ms: SCENE_DURATION_MS,
        poster_ms: SCENE_POSTER_MS,
        camera: Camera::focused(8.0, 10.0),
        build_frame: frame_solo_run,
    },
];

fn scene_index(scene_id: &str) -> Option<usize> {
    SCENES.iter().position(|scene| scene.id == scene_id)
}

#[cfg(test)]
fn scene_by_id(scene_id: &str, elapsed_ms: u32) -> Option<Scene> {
    scene_index(scene_id).map(|index| SCENES[index].frame(elapsed_ms))
}

fn create_scratch_canvas() -> Result<web_sys::HtmlCanvasElement, JsValue> {
    let document = web_sys::window()
        .and_then(|window| window.document())
        .ok_or_else(|| JsValue::from_str("tutorial scenes need a DOM document"))?;
    document
        .create_element("canvas")?
        .dyn_into::<web_sys::HtmlCanvasElement>()
        .map_err(|_| JsValue::from_str("failed to create the tutorial frame canvas"))
}

fn render_scene(
    scene: &Scene,
    scratch: &web_sys::HtmlCanvasElement,
    target: &web_sys::HtmlCanvasElement,
    draw_celebration: &js_sys::Function,
) -> Result<(), JsValue> {
    let target_width = target.width() as f64;
    let target_height = target.height() as f64;
    if target_width <= 0.0 || target_height <= 0.0 {
        // A zero-sized canvas is a layout race, not an error. The web wrapper's
        // ResizeObserver will ask this same player to draw again.
        return Ok(());
    }

    // Fit the fixed 16:10 camera without stretching grid cells. Existing
    // callers with the old thumbnail ratio receive a very small letterbox;
    // the progressive UI uses the matching aspect ratio and fills edge-to-edge.
    let cell_size = (target_width / scene.camera.width).min(target_height / scene.camera.height);
    if cell_size <= 0.0 {
        return Ok(());
    }

    let arena_width = scene.state.arena.width as f64;
    let arena_height = scene.state.arena.height as f64;
    let frame_width = (arena_width * cell_size).ceil() as u32 + 2;
    let frame_height = (arena_height * cell_size).ceil() as u32 + 2;
    if scratch.width() != frame_width {
        scratch.set_width(frame_width);
    }
    if scratch.height() != frame_height {
        scratch.set_height(frame_height);
    }

    render::render_game_state(
        &scene.state,
        scratch,
        cell_size,
        Some(1),
        0,
        draw_celebration,
    )?;

    let context = target
        .get_context("2d")?
        .ok_or_else(|| JsValue::from_str("tutorial canvas has no 2d context"))?
        .dyn_into::<web_sys::CanvasRenderingContext2d>()
        .map_err(|_| JsValue::from_str("failed to cast the tutorial 2d context"))?;

    let source_width = scene.camera.width * cell_size;
    let source_height = scene.camera.height * cell_size;
    let destination_x = ((target_width - source_width) / 2.0).max(0.0);
    let destination_y = ((target_height - source_height) / 2.0).max(0.0);

    context.set_fill_style_str("#ffffff");
    context.fill_rect(0.0, 0.0, target_width, target_height);
    context.draw_image_with_html_canvas_element_and_sw_and_sh_and_dx_and_dy_and_dw_and_dh(
        scratch,
        // +1 skips the arena renderer's padding so the crop lands on cells.
        scene.camera.x * cell_size + 1.0,
        scene.camera.y * cell_size + 1.0,
        source_width,
        source_height,
        destination_x,
        destination_y,
        source_width,
        source_height,
    )?;
    Ok(())
}

/// A deterministic, one-shot tutorial renderer. The player validates its
/// scene up front and owns one reusable full-arena scratch canvas; callers can
/// therefore render every 50 ms frame without allocating a DOM canvas each
/// time. Elapsed time is clamped at the final frame, so playback naturally
/// holds on the semantic outcome after `durationMs()`.
#[wasm_bindgen]
pub struct TutorialScenePlayer {
    scene_index: usize,
    scratch: web_sys::HtmlCanvasElement,
    draw_celebration: Closure<dyn FnMut()>,
}

#[wasm_bindgen]
impl TutorialScenePlayer {
    #[wasm_bindgen(constructor)]
    pub fn new(scene_id: &str) -> Result<TutorialScenePlayer, JsValue> {
        let scene_index = scene_index(scene_id)
            .ok_or_else(|| JsValue::from_str(&format!("unknown tutorial scene {scene_id}")))?;
        Ok(Self {
            scene_index,
            scratch: create_scratch_canvas()?,
            // Tutorial timelines never emit score celebrations, but the shared
            // renderer accepts the live game's cosmetic callback. Keep one
            // inert callback with the player so rendering stays allocation-free
            // and does not rely on `Function` construction/eval under CSP.
            draw_celebration: Closure::new(|| {}),
        })
    }

    #[wasm_bindgen(js_name = durationMs)]
    pub fn duration_ms(&self) -> u32 {
        SCENES[self.scene_index].duration_ms
    }

    #[wasm_bindgen(js_name = posterMs)]
    pub fn poster_ms(&self) -> u32 {
        SCENES[self.scene_index].poster_ms
    }

    #[wasm_bindgen(js_name = renderFrame)]
    pub fn render_frame(
        &self,
        elapsed_ms: u32,
        target: &web_sys::HtmlCanvasElement,
    ) -> Result<(), JsValue> {
        let scene = SCENES[self.scene_index].frame(elapsed_ms);
        render_scene(
            &scene,
            &self.scratch,
            target,
            self.draw_celebration.as_ref().unchecked_ref(),
        )
    }
}

/// Every scene id this module can draw, for the TypeScript-side exhaustiveness
/// check and progressive-step lookup.
#[wasm_bindgen(js_name = tutorialSceneIds)]
pub fn tutorial_scene_ids() -> Vec<String> {
    SCENES.iter().map(|scene| scene.id.to_owned()).collect()
}

/// Compatibility entry point for the original static canvas wrapper. It now
/// renders the authored poster frame through the same reusable-player path.
#[wasm_bindgen(js_name = renderTutorialScene)]
pub fn render_tutorial_scene(
    scene_id: &str,
    canvas: &web_sys::HtmlCanvasElement,
) -> Result<(), JsValue> {
    let player = TutorialScenePlayer::new(scene_id)?;
    player.render_frame(player.poster_ms(), canvas)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn scene(scene_id: &str, elapsed_ms: u32) -> Scene {
        scene_by_id(scene_id, elapsed_ms).expect("registered tutorial scene")
    }

    fn snake_length(snake: &Snake) -> u32 {
        if snake.body.is_empty() {
            return 0;
        }
        1 + snake
            .body
            .windows(2)
            .map(|pair| {
                u32::from(pair[0].x.abs_diff(pair[1].x)) + u32::from(pair[0].y.abs_diff(pair[1].y))
            })
            .sum::<u32>()
    }

    fn camera_contains(camera: Camera, point: Position) -> bool {
        let x = f64::from(point.x);
        let y = f64::from(point.y);
        x >= camera.x
            && x + 1.0 <= camera.x + camera.width
            && y >= camera.y
            && y + 1.0 <= camera.y + camera.height
    }

    #[test]
    fn registry_ids_and_timeline_metadata_are_unique_and_quantized() {
        let ids: HashSet<&str> = SCENES.iter().map(|scene| scene.id).collect();
        assert_eq!(ids.len(), SCENES.len());
        assert_eq!(SCENES.len(), 9);

        for definition in SCENES {
            assert!(definition.duration_ms > 0);
            assert_eq!(definition.duration_ms % BOOST_TICK_INTERVAL_MS, 0);
            assert!(definition.poster_ms <= definition.duration_ms);
            assert_eq!(definition.poster_ms % BOOST_TICK_INTERVAL_MS, 0);

            // A time past the one-shot duration must hold the exact final
            // semantic frame instead of wrapping back to the approach.
            let final_frame = definition.frame(definition.duration_ms);
            let held_frame = definition.frame(definition.duration_ms + 10_000);
            assert_eq!(final_frame.state.tick, held_frame.state.tick);
            assert_eq!(
                final_frame.state.arena.snakes[0].body,
                held_frame.state.arena.snakes[0].body
            );
            assert_eq!(
                final_frame.state.arena.snakes[0].is_alive,
                held_frame.state.arena.snakes[0].is_alive
            );
            assert_eq!(
                final_frame.state.arena.snakes[0].boost().active,
                held_frame.state.arena.snakes[0].boost().active
            );
        }
    }

    #[test]
    fn every_camera_is_fixed_sixteen_by_ten_in_bounds_and_keeps_the_subject_visible() {
        for definition in SCENES {
            for elapsed_ms in (0..=definition.duration_ms).step_by(BOOST_TICK_INTERVAL_MS as usize)
            {
                let frame = definition.frame(elapsed_ms);
                let camera = frame.camera;
                assert!((camera.width / camera.height - 1.6).abs() < f64::EPSILON);
                assert!(camera.x >= 0.0 && camera.y >= 0.0);
                assert!(
                    camera.x + camera.width <= f64::from(frame.state.arena.width),
                    "{} camera exceeds arena width",
                    definition.id
                );
                assert!(
                    camera.y + camera.height <= f64::from(frame.state.arena.height),
                    "{} camera exceeds arena height",
                    definition.id
                );
                let head = frame.state.arena.snakes[0].body[0];
                assert!(
                    camera_contains(camera, head),
                    "{} loses its subject at {elapsed_ms}ms: {head:?}",
                    definition.id
                );
                frame
                    .state
                    .validate_boost_invariants()
                    .unwrap_or_else(|error| {
                        panic!(
                            "{} produced an invalid real-renderer frame at {elapsed_ms}ms: {error}",
                            definition.id
                        )
                    });
            }
        }
    }

    #[test]
    fn early_and_poster_frames_express_each_lesson_semantically() {
        let carry_early = scene("team-carry", 0).state;
        let carry_poster = scene("team-carry", SCENE_POSTER_MS).state;
        assert!(
            snake_length(&carry_early.arena.snakes[0])
                > snake_length(&carry_poster.arena.snakes[0])
        );
        assert_eq!(
            carry_early
                .team_scores
                .as_ref()
                .and_then(|scores| scores.get(&TeamId(0)))
                .copied(),
            Some(0)
        );
        assert_eq!(
            carry_poster
                .team_scores
                .as_ref()
                .and_then(|scores| scores.get(&TeamId(0)))
                .copied(),
            Some(1)
        );

        for scene_id in ["team-boost", "ffa-boost"] {
            let early = scene(scene_id, 0).state;
            let poster = scene(scene_id, SCENE_POSTER_MS).state;
            assert!(!early.arena.snakes[0].boost().active);
            assert!(poster.arena.snakes[0].boost().active);
            assert!(
                early.arena.boost_pads[4].respawn_at_tick.is_none(),
                "{scene_id} must begin with its packet visible"
            );
            assert!(
                poster.arena.boost_pads[4].respawn_at_tick.is_some(),
                "{scene_id} must finish after collecting its packet"
            );
        }

        for scene_id in ["team-danger", "ffa-crash", "solo-run"] {
            assert!(scene(scene_id, 0).state.arena.snakes[0].is_alive);
            assert!(
                !scene(scene_id, SCENE_POSTER_MS).state.arena.snakes[0].is_alive,
                "{scene_id} must hold on the renderer's dead/X state"
            );
        }

        for (scene_id, eaten_food) in [
            ("ffa-food", position(22, 20)),
            ("solo-food", position(20, 18)),
        ] {
            let early = scene(scene_id, 0).state;
            let poster = scene(scene_id, SCENE_POSTER_MS).state;
            assert!(early.arena.food.contains(&eaten_food));
            assert!(!poster.arena.food.contains(&eaten_food));
            assert!(snake_length(&poster.arena.snakes[0]) > snake_length(&early.arena.snakes[0]));
        }

        let solo_food_early = scene("solo-food", 0).state;
        let solo_food_after_turn = scene("solo-food", 1_000).state;
        assert_eq!(solo_food_early.arena.snakes[0].direction, Direction::Right);
        assert_eq!(
            solo_food_after_turn.arena.snakes[0].direction,
            Direction::Up
        );

        let solo_boost_early = scene("solo-boost", 0).state;
        let solo_boost_poster = scene("solo-boost", SCENE_POSTER_MS).state;
        assert!(!solo_boost_early.arena.snakes[0].boost().active);
        assert!(solo_boost_poster.arena.snakes[0].boost().active);
        assert!(solo_boost_poster.properties.boost.unwrap().unlimited);

        let run_early = scene("solo-run", 0).state;
        let run_poster = scene("solo-run", SCENE_POSTER_MS).state;
        assert_eq!(run_early.arena.snakes[0].direction, Direction::Right);
        assert_eq!(run_poster.arena.snakes[0].direction, Direction::Up);
        assert!(!run_poster.arena.snakes[0].is_alive);
        assert!(run_poster.arena.snakes[0].body.len() > run_early.arena.snakes[0].body.len());
        assert_ne!(
            run_early.arena.snakes[0].body[0],
            run_poster.arena.snakes[0].body[0]
        );
    }

    #[test]
    fn timelines_preserve_length_except_for_authored_food_growth_and_banking() {
        for (scene_id, expected_length) in [
            ("team-boost", 7),
            ("team-danger", 5),
            ("ffa-boost", 6),
            ("ffa-crash", 5),
            ("solo-boost", 5),
            ("solo-run", 13),
        ] {
            for elapsed_ms in (0..=SCENE_DURATION_MS).step_by(BOOST_TICK_INTERVAL_MS as usize) {
                let state = scene(scene_id, elapsed_ms).state;
                assert_eq!(
                    snake_length(&state.arena.snakes[0]),
                    expected_length,
                    "{scene_id} changed length at {elapsed_ms}ms"
                );
            }
        }

        for elapsed_ms in (0..=SCENE_DURATION_MS).step_by(BOOST_TICK_INTERVAL_MS as usize) {
            let state = scene("team-carry", elapsed_ms).state;
            let score = state
                .team_scores
                .as_ref()
                .and_then(|scores| scores.get(&TeamId(0)))
                .copied()
                .expect("team score");
            let expected_length = if score == 0 { 6 } else { 4 };
            assert_eq!(
                snake_length(&state.arena.snakes[0]),
                expected_length,
                "team-carry changed length outside its banking reset at {elapsed_ms}ms"
            );
        }

        for (scene_id, eaten_food) in [
            ("ffa-food", position(22, 20)),
            ("solo-food", position(20, 18)),
        ] {
            let initial_length = snake_length(&scene(scene_id, 0).state.arena.snakes[0]);
            let mut previous_length = initial_length;
            for elapsed_ms in (0..=SCENE_DURATION_MS).step_by(BOOST_TICK_INTERVAL_MS as usize) {
                let state = scene(scene_id, elapsed_ms).state;
                let snake = &state.arena.snakes[0];
                let length = snake_length(snake);
                if state.arena.food.contains(&eaten_food) {
                    assert_eq!(
                        length, initial_length,
                        "{scene_id} grew before eating at {elapsed_ms}ms"
                    );
                    assert_eq!(snake.food, 0);
                } else {
                    assert_eq!(
                        length + snake.food,
                        initial_length + 2,
                        "{scene_id} must account for exactly two growth cells at {elapsed_ms}ms"
                    );
                    assert!(length >= previous_length);
                }
                previous_length = length;
            }
            assert_eq!(previous_length, initial_length + 2);
        }
    }

    #[test]
    fn crash_frames_rollback_bodies_and_keep_dead_heads_visible() {
        let team_last_safe = scene("team-danger", 1_500).state;
        let team_crash = scene("team-danger", SCENE_POSTER_MS).state;
        assert_eq!(
            team_crash.arena.snakes[0].body,
            team_last_safe.arena.snakes[0].body
        );
        assert_eq!(team_crash.arena.snakes[0].body[0], position(49, 20));
        assert_eq!(team_crash.recent_crashes[0].position, position(50, 20));
        assert!(
            team_crash
                .arena
                .is_in_enemy_base(&team_crash.recent_crashes[0].position, TeamId(0))
        );
        assert!(
            !team_crash.arena.snakes[1].contains_point(&team_crash.arena.snakes[0].body[0], false)
        );

        let ffa_last_safe = scene("ffa-crash", 1_450).state;
        let ffa_crash = scene("ffa-crash", SCENE_POSTER_MS).state;
        assert_eq!(
            ffa_crash.arena.snakes[0].body,
            ffa_last_safe.arena.snakes[0].body
        );
        assert_eq!(ffa_crash.arena.snakes[0].body[0], position(18, 15));
        assert_eq!(ffa_crash.recent_crashes[0].position, position(19, 15));
        assert!(
            ffa_crash.arena.snakes[1].contains_point(&ffa_crash.recent_crashes[0].position, false)
        );
        assert!(
            !ffa_crash.arena.snakes[1].contains_point(&ffa_crash.arena.snakes[0].body[0], false)
        );

        let solo_last_safe = scene("solo-run", 1_900).state;
        let solo_crash = scene("solo-run", SCENE_POSTER_MS).state;
        assert_eq!(
            solo_crash.arena.snakes[0].body,
            solo_last_safe.arena.snakes[0].body
        );
        assert_eq!(solo_crash.arena.snakes[0].body[0], position(18, 18));
        assert_eq!(solo_crash.recent_crashes[0].position, position(18, 17));
        assert!(
            solo_crash.arena.snakes[0].contains_point(&solo_crash.recent_crashes[0].position, true)
        );
    }
}
