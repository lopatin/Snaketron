//! Still frames for the pre-match tutorial, drawn by the real game renderer.
//!
//! Every illustration here is a genuine [`GameState`] built by the same
//! constructors production matches use — `GameState::new` lays out the team
//! zones, the goal opening and the canonical NOS pad field — and is painted by
//! [`render::render_game_state`], the exact function that paints the arena
//! during play. Nothing is a screenshot and nothing is a hand-drawn imitation,
//! so the tutorial cannot drift away from what the game actually looks like,
//! on any platform or device pixel ratio.
//!
//! Scenes are composed at arena scale and then cropped: the frame is rendered
//! to a detached canvas the size of the whole arena and the interesting region
//! is blitted into the caller's canvas. Cropping this way rather than by
//! transforming the caller's context keeps the renderer's own coordinate
//! assumptions — its 1px padding, its full-canvas background fill, its debug
//! tick label just below the frame — entirely intact.

use crate::render;
use common::{Direction, GameState, GameType, Player, Position, QueueMode, Snake, TeamId};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

/// The region of the arena a scene shows, in grid cells.
struct Camera {
    x: f64,
    y: f64,
    width: f64,
    height: f64,
}

struct Scene {
    state: GameState,
    camera: Camera,
}

const TEAM_ARENA_WIDTH: u16 = 60;
const TEAM_ARENA_HEIGHT: u16 = 40;
const FIELD_ARENA_SIZE: u16 = 40;
/// Any fixed seed produces the same food layout every time, which is what a
/// still frame wants. Scenes overwrite `arena.food` anyway; this only keeps
/// construction on the ordinary code path.
const SCENE_RNG_SEED: u64 = 20_260_806;

/// A team match laid out exactly as matchmaking builds one: 60x40, 10-cell end
/// zones, a centered goal opening, and the canonical 12-canister NOS field.
fn team_scene_state() -> GameState {
    let mut state = GameState::new(
        TEAM_ARENA_WIDTH,
        TEAM_ARENA_HEIGHT,
        GameType::TeamMatch { per_team: 1 },
        QueueMode::Quickmatch,
        Some(SCENE_RNG_SEED),
        0,
    );
    // The renderer paints each end zone with the name of the side that owns
    // it, resolved from these maps, so a cropped base reads as *yours*.
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

/// A 40x40 open field: no end zones and no goal, matching Solo and FFA. A
/// free-for-all also gets the teamless NOS layout here, because `GameState::new`
/// places it — which is the point of building scenes through the real
/// constructor rather than posing them by hand.
fn field_scene_state(game_type: GameType) -> GameState {
    let mut state = GameState::new(
        FIELD_ARENA_SIZE,
        FIELD_ARENA_SIZE,
        game_type,
        QueueMode::Quickmatch,
        Some(SCENE_RNG_SEED),
        0,
    );
    state.arena.snakes.clear();
    state.arena.food.clear();
    state
}

fn position(x: i16, y: i16) -> Position {
    Position { x, y }
}

/// Team bullet 1 — a long snake carrying food home through its own goal.
///
/// The crop frames the blue end zone, the wall, and the goal opening, so the
/// "bring it back through the gap" shape is legible at thumbnail size.
fn scene_team_carry() -> Scene {
    let mut state = team_scene_state();
    // Head already inside the base, body trailing back out through the goal.
    // A body this much longer than the 4-cell start length is exactly what
    // "carrying food" looks like on screen.
    state.arena.snakes.push(Snake::for_illustration(
        vec![position(4, 20), position(21, 20)],
        Direction::Left,
        Some(TeamId(0)),
        0,
        true,
        false,
    ));
    state.arena.food = vec![position(16, 15), position(19, 25), position(23, 18)];

    Scene {
        state,
        camera: Camera {
            x: 0.0,
            y: 12.0,
            width: 25.0,
            height: 17.0,
        },
    }
}

/// Team bullet 2 — NOS canisters on the field and a snake running boosted.
///
/// The crop is chosen to contain both canister sizes the renderer draws: a
/// 2x2 full-tank bottle and a 1x1 quarter packet.
fn scene_team_boost() -> Scene {
    let mut state = team_scene_state();
    state.arena.snakes.push(Snake::for_illustration(
        vec![position(25, 11), position(13, 11)],
        Direction::Right,
        Some(TeamId(0)),
        0,
        true,
        true,
    ));
    state.arena.food = vec![position(18, 15)];

    // Frames the 2x2 full-tank canister at (14,4) and the 1x1 quarter packet
    // at (26,12) together, so both sizes the renderer draws are on screen.
    Scene {
        state,
        camera: Camera {
            x: 10.0,
            y: 2.0,
            width: 22.0,
            height: 15.0,
        },
    }
}

/// Team bullet 3 — the enemy end zone, and what happens if you enter it.
///
/// The whole 60x40 board was tried here first and is unreadable at thumbnail
/// size: every snake collapses to a couple of pixels. Framing the rival base
/// instead teaches the rule that catches new players out — touching it kills
/// you, it is not a scoring move — while staying legible.
fn scene_team_danger() -> Scene {
    let mut state = team_scene_state();
    // A blue snake that crossed into the red zone and died there. The renderer
    // greys a dead snake out and marks its head with an X.
    state.arena.snakes.push(Snake::for_illustration(
        vec![position(52, 20), position(45, 20)],
        Direction::Right,
        Some(TeamId(0)),
        0,
        false,
        false,
    ));
    state.arena.snakes.push(Snake::for_illustration(
        vec![position(43, 27), position(43, 22)],
        Direction::Up,
        Some(TeamId(1)),
        0,
        true,
        false,
    ));
    state.arena.food = vec![position(41, 15), position(46, 25)];

    Scene {
        state,
        camera: Camera {
            x: 36.0,
            y: 12.0,
            width: 24.0,
            height: 16.0,
        },
    }
}

/// Open-field bullet — a snake about to reach food.
fn scene_field_food(game_type: GameType) -> Scene {
    let mut state = field_scene_state(game_type);
    state.arena.snakes.push(Snake::for_illustration(
        vec![position(18, 20), position(9, 20)],
        Direction::Right,
        None,
        0,
        true,
        false,
    ));
    state.arena.food = vec![position(23, 20), position(14, 14), position(26, 27)];

    Scene {
        state,
        camera: Camera {
            x: 6.0,
            y: 11.0,
            width: 24.0,
            height: 17.0,
        },
    }
}

/// Open-field bullet — one life, and how you lose it.
///
/// A dead snake alone does not read: the renderer paints it near-white, so at
/// thumbnail size the frame looks empty. Pairing it with a live snake one cell
/// from a rival's body shows both the cause and the result.
fn scene_field_crash(game_type: GameType) -> Scene {
    let mut state = field_scene_state(game_type);
    // Live snake, head one cell short of the rival body ahead of it.
    state.arena.snakes.push(Snake::for_illustration(
        vec![position(17, 15), position(9, 15)],
        Direction::Right,
        None,
        0,
        true,
        false,
    ));
    // The rival it is about to run into.
    state.arena.snakes.push(Snake::for_illustration(
        vec![position(19, 11), position(19, 21)],
        Direction::Up,
        None,
        0,
        true,
        false,
    ));
    // And a snake that already made that mistake.
    state.arena.snakes.push(Snake::for_illustration(
        vec![position(25, 24), position(25, 19)],
        Direction::Down,
        None,
        0,
        false,
        false,
    ));
    state.arena.food = vec![position(13, 23)];

    Scene {
        state,
        camera: Camera {
            x: 6.0,
            y: 8.0,
            width: 24.0,
            height: 17.0,
        },
    }
}

/// Solo bullet 3 — one long survivor, no rivals.
fn scene_solo_run() -> Scene {
    let mut state = field_scene_state(GameType::Solo);
    state.arena.snakes.push(Snake::for_illustration(
        vec![
            position(26, 12),
            position(14, 12),
            position(14, 26),
            position(28, 26),
            position(28, 19),
        ],
        Direction::Right,
        None,
        0,
        true,
        false,
    ));
    state.arena.food = vec![position(31, 12), position(9, 32), position(33, 33)];

    Scene {
        state,
        camera: Camera {
            x: 4.0,
            y: 5.0,
            width: 34.0,
            height: 30.0,
        },
    }
}

type SceneBuilder = fn() -> Scene;

/// The single scene registry. Lookup and enumeration both read this table, so
/// the published id list cannot drift from what is actually drawable.
/// Open-field bullet — collectible Boost on the free-for-all map.
///
/// Frames the top-left 2x2 full tank at (4,4) together with two 1x1 quarter
/// packets from the inner ring at (16,12) and (23,12), so both pickup sizes
/// the renderer draws are on screen at thumbnail size. The pads themselves
/// come from the real layout: `field_scene_state` builds a 40x40 free-for-all,
/// which is exactly the map the teamless layout is drawn on.
fn scene_field_boost() -> Scene {
    let mut state = field_scene_state(GameType::FreeForAll { max_players: 4 });
    state.arena.snakes.push(Snake::for_illustration(
        vec![position(13, 12), position(4, 12)],
        Direction::Right,
        None,
        0,
        true,
        true,
    ));

    Scene {
        state,
        camera: Camera {
            x: 2.0,
            y: 2.0,
            width: 24.0,
            height: 16.0,
        },
    }
}

/// Solo bullet — Boost that never runs out.
///
/// A solo map carries no pickups at all, which is the point: there is nothing
/// to frame except the boosting snake itself. The renderer paints the
/// active-Boost contour, so a long snake mid-burst is what carries the idea.
fn scene_solo_boost() -> Scene {
    let mut state = field_scene_state(GameType::Solo);
    state.arena.snakes.push(Snake::for_illustration(
        vec![position(27, 18), position(9, 18)],
        Direction::Right,
        None,
        0,
        true,
        true,
    ));
    state.arena.food = vec![position(31, 18)];

    Scene {
        state,
        camera: Camera {
            x: 6.0,
            y: 11.0,
            width: 28.0,
            height: 15.0,
        },
    }
}

const SCENES: &[(&str, SceneBuilder)] = &[
    ("team-carry", scene_team_carry),
    ("team-boost", scene_team_boost),
    ("team-danger", scene_team_danger),
    ("ffa-food", || {
        scene_field_food(GameType::FreeForAll { max_players: 4 })
    }),
    ("ffa-crash", || {
        scene_field_crash(GameType::FreeForAll { max_players: 4 })
    }),
    ("ffa-boost", scene_field_boost),
    ("solo-food", || scene_field_food(GameType::Solo)),
    ("solo-boost", scene_solo_boost),
    ("solo-run", scene_solo_run),
];

fn scene_by_id(scene_id: &str) -> Option<Scene> {
    SCENES
        .iter()
        .find(|(id, _)| *id == scene_id)
        .map(|(_, build)| build())
}

/// Every scene id this module can draw, for the TypeScript side to check its
/// own table against.
#[wasm_bindgen(js_name = tutorialSceneIds)]
pub fn tutorial_scene_ids() -> Vec<String> {
    SCENES.iter().map(|(id, _)| (*id).to_owned()).collect()
}

/// Draw one tutorial scene into `canvas`, filling it edge to edge while
/// preserving the scene's aspect ratio.
///
/// `canvas` is expected to already be sized in device pixels (CSS size times
/// `devicePixelRatio`), exactly like the arena canvas during play, so the
/// illustration is crisp on high-density displays.
#[wasm_bindgen(js_name = renderTutorialScene)]
pub fn render_tutorial_scene(
    scene_id: &str,
    canvas: &web_sys::HtmlCanvasElement,
) -> Result<(), JsValue> {
    let Some(scene) = scene_by_id(scene_id) else {
        return Err(JsValue::from_str(&format!(
            "unknown tutorial scene {scene_id}"
        )));
    };

    let target_width = canvas.width() as f64;
    let target_height = canvas.height() as f64;
    if target_width <= 0.0 || target_height <= 0.0 {
        // A zero-sized canvas is a layout race, not an error: the caller
        // re-renders when its ResizeObserver reports real dimensions.
        return Ok(());
    }

    // Fit the requested region into the target, letterboxing rather than
    // stretching so arena cells stay square.
    let cell_size = (target_width / scene.camera.width).min(target_height / scene.camera.height);
    if cell_size <= 0.0 {
        return Ok(());
    }

    let arena_width = scene.state.arena.width as f64;
    let arena_height = scene.state.arena.height as f64;
    // Matches the renderer's own sizing convention: one pixel of padding on
    // every side of the cell grid.
    let frame_width = (arena_width * cell_size).ceil() + 2.0;
    let frame_height = (arena_height * cell_size).ceil() + 2.0;

    let document = web_sys::window()
        .and_then(|window| window.document())
        .ok_or_else(|| JsValue::from_str("tutorial scenes need a DOM document"))?;
    let frame: web_sys::HtmlCanvasElement = document
        .create_element("canvas")?
        .dyn_into::<web_sys::HtmlCanvasElement>()
        .map_err(|_| JsValue::from_str("failed to create the tutorial frame canvas"))?;
    frame.set_width(frame_width as u32);
    frame.set_height(frame_height as u32);

    // The real thing: the same function that paints the arena every frame
    // during a match.
    render::render_game_state(&scene.state, &frame, cell_size, Some(1), 0)?;

    let context = canvas
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
        &frame,
        // +1 skips the renderer's padding so the crop lands on cell edges.
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
