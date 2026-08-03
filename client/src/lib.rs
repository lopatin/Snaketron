mod render;

use common::{Direction, GameCommand, GameEngine, GameEvent, GameEventMessage, GameState};
use wasm_bindgen::prelude::*;

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
    pub fn new_from_state(game_id: u32, state_json: &str) -> Result<GameClient, JsValue> {
        // Set panic hook for better error messages in browser console
        console_error_panic_hook::set_once();

        // Initialize logging for WASM - this will send log messages to browser console
        wasm_logger::init(wasm_logger::Config::default());

        let game_state: GameState =
            serde_json::from_str(state_json).map_err(|e| JsValue::from_str(&e.to_string()))?;

        Ok(GameClient {
            engine: GameEngine::new_from_state(game_id, game_state),
        })
    }

    /// Set the local player ID
    #[wasm_bindgen(js_name = setLocalPlayerId)]
    pub fn set_local_player_id(&mut self, player_id: u32) {
        self.engine.set_local_player_id(player_id);
    }

    /// Run the game engine until the specified timestamp
    /// Returns a JSON array of game events that occurred with their tick numbers
    #[wasm_bindgen(js_name = runUntil)]
    pub fn run_until(&mut self, ts_ms: i64) -> Result<String, JsValue> {
        let events = self
            .engine
            .run_until(ts_ms)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        // Convert to a format that's easier to work with in JavaScript
        let events_with_ticks: Vec<serde_json::Value> = events
            .into_iter()
            .map(|(tick, sequence, event)| {
                serde_json::json!({
                    "tick": tick,
                    "sequence": sequence,
                    "event": event
                })
            })
            .collect();

        serde_json::to_string(&events_with_ticks).map_err(|e| JsValue::from_str(&e.to_string()))
    }

    #[wasm_bindgen(js_name = rebuildPredictedState)]
    pub fn rebuild_predicted_state(&mut self, ts_ms: i64) -> Result<(), JsValue> {
        self.engine
            .rebuild_predicted_state(ts_ms)
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

        let command = GameCommand::Turn {
            snake_id,
            direction: dir,
        };

        // Process with client-side prediction
        let command_message = self
            .engine
            .process_local_command(command)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        // Return the command message as JSON to be sent to server
        serde_json::to_string(&command_message).map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Process a server event for reconciliation
    #[wasm_bindgen(js_name = processServerEvent)]
    pub fn process_server_event(&mut self, event_message_json: &str) -> Result<(), JsValue> {
        let event_message: GameEventMessage = serde_json::from_str(event_message_json)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        self.engine
            .process_server_event(&event_message)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Apply a raw server WSMessage frame (`{"GameEvent": <GameEventMessage>}`).
    ///
    /// The `GameEventMessage` is deserialized from the raw frame text entirely
    /// in Rust, so full-range `u64` fields survive intact. Passing the parsed
    /// JS object here instead would have already lost precision: `JSON.parse`
    /// widens every number to an f64, corrupting `TickHash.hash` (an unmasked
    /// 64-bit digest) and breaking divergence detection with false mismatches.
    #[wasm_bindgen(js_name = processServerFrame)]
    pub fn process_server_frame(&mut self, frame_json: &str) -> Result<(), JsValue> {
        let frame: serde_json::Value =
            serde_json::from_str(frame_json).map_err(|e| JsValue::from_str(&e.to_string()))?;
        let inner = frame
            .get("GameEvent")
            .ok_or_else(|| JsValue::from_str("expected a GameEvent frame"))?;
        let event_message: GameEventMessage =
            serde_json::from_value(inner.clone()).map_err(|e| JsValue::from_str(&e.to_string()))?;

        self.engine
            .process_server_event(&event_message)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Build a client from a raw server WSMessage frame carrying a Snapshot
    /// (`{"GameEvent": { ..., "event": { "Snapshot": { "game_state": ... } } }}`).
    ///
    /// Deserializing the `GameState` from the raw text in Rust keeps its u64
    /// fields (notably `rng.state`) intact, matching `processServerFrame`.
    #[wasm_bindgen(js_name = newFromSnapshotFrame)]
    pub fn new_from_snapshot_frame(game_id: u32, frame_json: &str) -> Result<GameClient, JsValue> {
        console_error_panic_hook::set_once();

        let frame: serde_json::Value =
            serde_json::from_str(frame_json).map_err(|e| JsValue::from_str(&e.to_string()))?;
        let game_state_val = frame
            .get("GameEvent")
            .and_then(|e| e.get("event"))
            .and_then(|e| e.get("Snapshot"))
            .and_then(|s| s.get("game_state"))
            .ok_or_else(|| JsValue::from_str("expected a GameEvent Snapshot frame"))?;
        let game_state: GameState = serde_json::from_value(game_state_val.clone())
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        Ok(GameClient {
            engine: GameEngine::new_from_state(game_id, game_state),
        })
    }

    /// Initialize game state from a snapshot
    #[wasm_bindgen(js_name = initializeFromSnapshot)]
    pub fn initialize_from_snapshot(
        &mut self,
        state_json: &str,
        _current_ts: i64,
    ) -> Result<(), JsValue> {
        let game_state: GameState =
            serde_json::from_str(state_json).map_err(|e| JsValue::from_str(&e.to_string()))?;

        // Create a GameEventMessage with the snapshot event
        let event_message = GameEventMessage {
            game_id: self.engine.game_id(),
            tick: game_state.current_tick(),
            sequence: game_state.event_sequence,
            stream_seq: 0, // locally constructed, not from the transport

            user_id: None,
            event: GameEvent::Snapshot { game_state },
        };

        self.engine
            .process_server_event(&event_message)
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Get the current game state as JSON
    #[wasm_bindgen(js_name = getGameStateJson)]
    pub fn get_game_state_json(&self) -> Result<String, JsValue> {
        self.engine
            .get_predicted_state_json()
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Get the committed (server-authoritative) state as JSON
    #[wasm_bindgen(js_name = getCommittedStateJson)]
    pub fn get_committed_state_json(&self) -> Result<String, JsValue> {
        self.engine
            .get_committed_state_json()
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Get the event log as JSON
    #[wasm_bindgen(js_name = getEventLogJson)]
    pub fn get_event_log_json(&self) -> Result<String, JsValue> {
        self.engine
            .get_event_log_json()
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Get the current tick number (alias for getCommittedTick)
    #[wasm_bindgen(js_name = getCurrentTick)]
    pub fn get_current_tick(&self) -> u32 {
        self.engine.current_tick()
    }

    /// Get the committed tick number (server-authoritative state tick)
    #[wasm_bindgen(js_name = getCommittedTick)]
    pub fn get_committed_tick(&self) -> u32 {
        self.engine.current_tick()
    }

    /// Get the predicted tick number (client-side predicted state tick)
    #[wasm_bindgen(js_name = getPredictedTick)]
    pub fn get_predicted_tick(&self) -> u32 {
        // Access the predicted state tick from the engine
        self.engine.get_predicted_tick()
    }

    /// Get the game ID
    #[wasm_bindgen(js_name = getGameId)]
    pub fn get_game_id(&self) -> u32 {
        self.engine.game_id()
    }

    /// Get the engine's sync status (stream gaps, hash probes, needs_resync) as JSON
    #[wasm_bindgen(js_name = getSyncStatusJson)]
    pub fn get_sync_status_json(&self) -> Result<String, JsValue> {
        self.engine
            .sync_status_json()
            .map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Get the committed-state sync hash as a decimal string
    /// (u64 exceeds JS safe-integer range, so it crosses the boundary as text)
    #[wasm_bindgen(js_name = getCommittedHash)]
    pub fn get_committed_hash(&self) -> String {
        self.engine.committed_sync_hash().to_string()
    }

    /// Clear the needs_resync flag once a resync request has been sent
    #[wasm_bindgen(js_name = clearNeedsResync)]
    pub fn clear_needs_resync(&mut self) {
        self.engine.clear_needs_resync();
    }

    /// Get the snake ID for a given user ID
    /// Returns None if the user is not in the game
    #[wasm_bindgen(js_name = getSnakeIdForUser)]
    pub fn get_snake_id_for_user(&self, user_id: u32) -> Option<u32> {
        // Read the player directly from the engine's predicted state. (This used
        // to serialize the entire predicted state to JSON and re-parse it on
        // every keypress just to read one field.)
        self.render_state()
            .players
            .get(&user_id)
            .map(|player| player.snake_id)
    }

    /// Render the engine's current predicted state directly to a canvas — no
    /// JSON round-trip. Replaces the free `render_game(json, ...)` export, which
    /// re-parsed the engine's own state from a string into `serde_json::Value`
    /// every frame.
    #[wasm_bindgen(js_name = render)]
    pub fn render(
        &self,
        canvas: &web_sys::HtmlCanvasElement,
        cell_size: f64,
        rotation: f64,
        local_user_id: Option<u32>,
    ) -> Result<(), JsValue> {
        render::render_game_state(
            self.render_state(),
            canvas,
            cell_size,
            local_user_id,
            rotation as i32,
        )
    }
}

impl GameClient {
    /// The state to render/query: the predicted state, or the committed state
    /// if prediction is not active.
    fn render_state(&self) -> &GameState {
        self.engine
            .predicted_state()
            .unwrap_or_else(|| self.engine.committed_state())
    }
}

/// Input helper exposed to JavaScript (maps a screen direction to a game
/// direction for the current arena rotation); see render.rs.
pub use render::screen_direction_to_game;

#[cfg(test)]
mod tests {
    use super::*;

    // The whole reason processServerFrame/newFromSnapshotFrame exist: parsing a
    // GameEventMessage out of the raw frame text in Rust must preserve a
    // full-range u64 hash exactly. A JS `JSON.parse` would widen it to an f64
    // and silently alter it, which is what corrupted divergence detection.
    #[test]
    fn frame_parse_preserves_full_range_u64_hash() {
        // A digest well above 2^53 (JS Number.MAX_SAFE_INTEGER).
        let hash: u64 = 0xFEDC_BA98_7654_3210;
        assert!(hash > (1u64 << 53));

        let frame = format!(
            r#"{{"GameEvent":{{"game_id":1,"tick":5,"sequence":9,"stream_seq":9,"user_id":null,"event":{{"TickHash":{{"hash":{hash},"server_ts_ms":123}}}}}}}}"#
        );

        let frame_val: serde_json::Value = serde_json::from_str(&frame).unwrap();
        let inner = frame_val.get("GameEvent").expect("GameEvent key present");
        let msg: GameEventMessage = serde_json::from_value(inner.clone()).unwrap();

        match msg.event {
            GameEvent::TickHash { hash: got, .. } => assert_eq!(got, hash),
            other => panic!("expected TickHash, got {other:?}"),
        }
    }
}
