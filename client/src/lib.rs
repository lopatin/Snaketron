mod render;

use wasm_bindgen::prelude::*;
use common::{GameEngine, GameCommand, Direction, GameEvent, GameEventMessage, GameState};
use serde_json;

/// The main client-side game interface exposed to JavaScript.
/// This wraps the GameEngine and provides a clean WASM boundary.
#[wasm_bindgen]
pub struct GameClient {
    engine: GameEngine,
}

#[wasm_bindgen]
impl GameClient {
    /// Creates a new game client instance
    #[wasm_bindgen(constructor)]
    pub fn new(game_id: u32, start_ms: i64) -> Self {
        // Set panic hook for better error messages in browser console
        console_error_panic_hook::set_once();
        
        // Initialize logging for WASM - this will send log messages to browser console
        wasm_logger::init(wasm_logger::Config::default());
        
        GameClient {
            engine: GameEngine::new(game_id, start_ms),
        }
    }
    
    /// Creates a new game client instance from an existing game state
    #[wasm_bindgen(js_name = newFromState)]
    pub fn new_from_state(game_id: u32, start_ms: i64, state_json: &str) -> Result<GameClient, JsValue> {
        // Set panic hook for better error messages in browser console
        console_error_panic_hook::set_once();
        
        // Initialize logging for WASM - this will send log messages to browser console
        wasm_logger::init(wasm_logger::Config::default());
        
        let game_state: GameState = serde_json::from_str(state_json)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        Ok(GameClient {
            engine: GameEngine::new_from_state(game_id, start_ms, game_state),
        })
    }

    /// Set the local player ID
    #[wasm_bindgen(js_name = setLocalPlayerId)]
    pub fn set_local_player_id(&mut self, player_id: u32) {
        self.engine.set_local_player_id(player_id);
    }

    /// Run the game engine until the specified timestamp
    /// Returns a JSON array of game events that occurred
    #[wasm_bindgen(js_name = runUntil)]
    pub fn run_until(&mut self, ts_ms: i64) -> Result<String, JsValue> {
        let events = self.engine.run_until(ts_ms)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        serde_json::to_string(&events)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
    
    #[wasm_bindgen(js_name = rebuildPredictedState)]
    pub fn rebuild_predicted_state(&mut self, ts_ms: i64) -> Result<(), JsValue> {
        self.engine.rebuild_predicted_state(ts_ms)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Process a turn command for a snake with client-side prediction
    /// Returns the command message that should be sent to the server
    #[wasm_bindgen(js_name = processTurn)]
    pub fn process_turn(&mut self, snake_id: u32, direction: &str) -> Result<String, JsValue> {
        let dir = match direction {
            "Up" => Direction::Up,
            "Down" => Direction::Down,
            "Left" => Direction::Left,
            "Right" => Direction::Right,
            _ => return Err(JsValue::from_str("Invalid direction")),
        };

        let command = GameCommand::Turn { snake_id, direction: dir };
        
        // Process with client-side prediction
        let command_message = self.engine.process_local_command(command)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        // Return the command message as JSON to be sent to server
        serde_json::to_string(&command_message)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
    
    /// Process a server event for reconciliation
    #[wasm_bindgen(js_name = processServerEvent)]
    pub fn process_server_event(&mut self, event_message_json: &str, current_ts: i64) -> Result<(), JsValue> {
        let event_message: GameEventMessage = serde_json::from_str(event_message_json)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        self.engine.process_server_event(&event_message, current_ts)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }
    
    /// Initialize game state from a snapshot
    #[wasm_bindgen(js_name = initializeFromSnapshot)]
    pub fn initialize_from_snapshot(&mut self, state_json: &str, current_ts: i64) -> Result<(), JsValue> {
        let game_state: GameState = serde_json::from_str(state_json)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        
        // Create a GameEventMessage with the snapshot event
        let event_message = GameEventMessage {
            game_id: self.engine.game_id(),
            tick: game_state.current_tick(),
            user_id: None,
            event: GameEvent::Snapshot { game_state },
        };
        
        self.engine.process_server_event(&event_message, current_ts)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Get the current game state as JSON
    #[wasm_bindgen(js_name = getGameStateJson)]
    pub fn get_game_state_json(&self) -> Result<String, JsValue> {
        self.engine.get_predicted_state_json()
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Get the committed (server-authoritative) state as JSON
    #[wasm_bindgen(js_name = getCommittedStateJson)]
    pub fn get_committed_state_json(&self) -> Result<String, JsValue> {
        self.engine.get_committed_state_json()
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Get the event log as JSON
    #[wasm_bindgen(js_name = getEventLogJson)]
    pub fn get_event_log_json(&self) -> Result<String, JsValue> {
        self.engine.get_event_log_json()
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Get the current tick number
    #[wasm_bindgen(js_name = getCurrentTick)]
    pub fn get_current_tick(&self) -> u32 {
        self.engine.current_tick()
    }

    /// Get the game ID
    #[wasm_bindgen(js_name = getGameId)]
    pub fn get_game_id(&self) -> u32 {
        self.engine.game_id()
    }
}

/// Render functions exposed to JavaScript
pub use render::render_game;