mod render;
mod tutorial;

use common::{Direction, GameCommand, GameEngine, GameEvent, GameEventMessage, GameState};
use wasm_bindgen::prelude::*;

/// The main client-side game interface exposed to JavaScript.
/// This wraps the GameEngine and provides a clean WASM boundary.
#[wasm_bindgen]
pub struct GameClient {
    engine: GameEngine,
}

fn game_client_from_snapshot_frame(game_id: u32, frame_json: &str) -> Result<GameClient, String> {
    let frame: serde_json::Value = serde_json::from_str(frame_json).map_err(|e| e.to_string())?;
    let game_state_val = frame
        .get("GameEvent")
        .and_then(|e| e.get("event"))
        .and_then(|e| e.get("Snapshot"))
        .and_then(|s| s.get("game_state"))
        .ok_or_else(|| "expected a GameEvent Snapshot frame".to_string())?;
    let game_state: GameState =
        serde_json::from_value(game_state_val.clone()).map_err(|e| e.to_string())?;

    Ok(GameClient {
        engine: GameEngine::try_new_from_snapshot_state(game_id, game_state)
            .map_err(|e| e.to_string())?,
    })
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
            engine: GameEngine::try_new_from_snapshot_state(game_id, game_state)
                .map_err(|e| JsValue::from_str(&e.to_string()))?,
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

    /// Activate the snake's stored Boost charge with client-side prediction.
    ///
    /// The command deliberately carries only the snake ID. Charge, duration,
    /// speed, and pad state are all derived by the shared engine.
    #[wasm_bindgen(js_name = processActivateBoost)]
    pub fn process_activate_boost(&mut self, snake_id: u32) -> Result<String, JsValue> {
        let command = GameCommand::ActivateBoost { snake_id };

        let command_message = self
            .engine
            .process_local_command(command)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        serde_json::to_string(&command_message).map_err(|e| JsValue::from_str(&e.to_string()))
    }

    /// Stop consuming the snake's stored Boost charge with client-side
    /// prediction. The explicit command is idempotent, so retries cannot
    /// accidentally toggle Boost back on.
    #[wasm_bindgen(js_name = processDeactivateBoost)]
    pub fn process_deactivate_boost(&mut self, snake_id: u32) -> Result<String, JsValue> {
        let command = GameCommand::DeactivateBoost { snake_id };

        let command_message = self
            .engine
            .process_local_command(command)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

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
        game_client_from_snapshot_frame(game_id, frame_json)
            .map_err(|error| JsValue::from_str(&error))
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

    /// A compact rollback-visible view of the recent predicted cosmetic events:
    /// collisions and team goals. The web renderer reads this beside the canvas
    /// render so crash and celebration effects start (or retract) in the exact
    /// frame the predicted visual state changes, without waiting for React
    /// state propagation or serializing the full game state.
    #[wasm_bindgen(js_name = getPredictedVisualStateJson)]
    pub fn get_predicted_visual_state_json(&self) -> Result<String, JsValue> {
        let state = self.render_state();
        serde_json::to_string(&serde_json::json!({
            "predicted_tick": state.current_tick(),
            "committed_tick": self.engine.current_tick(),
            "tick_duration_ms": state.properties.tick_duration_ms,
            "cues": &state.recent_crashes,
            "goals": &state.recent_goals,
        }))
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
    /// JSON round-trip. `draw_celebration` is invoked after the field and pickups
    /// but before snakes, so JavaScript-owned cosmetic effects can share this
    /// canvas without covering gameplay actors.
    #[wasm_bindgen(js_name = render)]
    pub fn render(
        &self,
        canvas: &web_sys::HtmlCanvasElement,
        cell_size: f64,
        rotation: f64,
        local_user_id: Option<u32>,
        draw_celebration: &js_sys::Function,
    ) -> Result<(), JsValue> {
        render::render_game_state(
            self.render_state(),
            canvas,
            cell_size,
            local_user_id,
            rotation as i32,
            draw_celebration,
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
    use common::{
        DEFAULT_TICK_INTERVAL_MS, GameCommandMessage, GameStatus, GameType, Position, QueueMode,
    };

    fn charged_duel_state() -> (GameState, u32) {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        let snake_id = state
            .add_player(7, Some("boost-client".into()))
            .unwrap()
            .snake_id;
        let pad = state.arena.boost_pads[0].clone();
        let snake = &mut state.arena.snakes[snake_id as usize];
        snake.body = vec![
            pad.position,
            Position {
                x: pad.position.x - 1,
                y: pad.position.y,
            },
        ];
        snake.direction = Direction::Right;

        // A normal-speed snake has only half a movement opportunity in the
        // first 50 ms quantum, so it remains on the selected packet and
        // collects that pad's authoritative per-packet value.
        state.tick_forward(true).unwrap();
        assert_eq!(
            state.arena.snakes[snake_id as usize].boost().charge_ms,
            pad.charge_ms
        );
        state.validate_boost_invariants().unwrap();
        (state, snake_id)
    }

    fn legacy_completed_boostless_state(width: u16, height: u16, game_type: GameType) -> GameState {
        let mut state = GameState::new(
            width,
            height,
            game_type.clone(),
            QueueMode::Competitive,
            Some(91),
            0,
        );
        let player_count = match &game_type {
            GameType::Solo => 1,
            GameType::TeamMatch { per_team } => u32::from(*per_team) * 2,
            GameType::FreeForAll { max_players } => u32::from(*max_players),
            GameType::Custom { .. } => unreachable!("custom is not a legacy Boost mode"),
        };
        for user_id in 1..=player_count {
            state.add_player(user_id, None).unwrap();
        }
        state.status = GameStatus::Complete {
            winning_snake_id: Some(0),
        };

        let mut persisted = serde_json::to_value(state).unwrap();
        persisted
            .as_object_mut()
            .unwrap()
            .remove("player_action_counts");
        persisted["properties"]["available_food_target"] =
            serde_json::json!(common::DEFAULT_FOOD_TARGET);
        persisted["properties"]["tick_duration_ms"] = serde_json::json!(DEFAULT_TICK_INTERVAL_MS);
        persisted["properties"]["time_limit_ms"] =
            if matches!(game_type, GameType::TeamMatch { per_team: 1 | 2 }) {
                serde_json::json!(90_000)
            } else {
                serde_json::Value::Null
            };
        persisted["properties"]
            .as_object_mut()
            .unwrap()
            .remove("score_limit");
        persisted["properties"]
            .as_object_mut()
            .unwrap()
            .remove("boost");
        persisted["arena"]
            .as_object_mut()
            .unwrap()
            .remove("boost_pads");
        for snake in persisted["arena"]["snakes"].as_array_mut().unwrap() {
            let snake = snake.as_object_mut().unwrap();
            snake.remove("speed_milli");
            snake.remove("movement_credit");
            snake.remove("boost");
        }
        serde_json::from_value(persisted).unwrap()
    }

    fn legacy_timed_boost_duel_state() -> GameState {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Competitive,
            Some(91),
            0,
        );
        state.add_player(7, Some("legacy-blue".into())).unwrap();
        state.add_player(8, Some("legacy-red".into())).unwrap();
        state.status = GameStatus::Complete {
            winning_snake_id: Some(0),
        };
        state.properties.time_limit_ms = Some(90_000);
        state.properties.score_limit = None;
        state
    }

    fn snapshot_frame(game_id: u32, state: &GameState) -> String {
        serde_json::to_string(&serde_json::json!({
            "GameEvent": {
                "game_id": game_id,
                "tick": state.tick,
                "sequence": state.event_sequence,
                "stream_seq": 0,
                "user_id": null,
                "event": { "Snapshot": { "game_state": state } }
            }
        }))
        .unwrap()
    }

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

    #[test]
    fn wasm_snapshot_boundary_renders_only_compatible_completed_history() {
        for (width, height, game_type) in [
            (60, 40, GameType::TeamMatch { per_team: 1 }),
            (40, 40, GameType::FreeForAll { max_players: 4 }),
            (40, 40, GameType::Solo),
        ] {
            let completed = legacy_completed_boostless_state(width, height, game_type);
            let frame = snapshot_frame(42, &completed);
            let client = game_client_from_snapshot_frame(42, &frame)
                .expect("legacy completed snapshot frame must render");
            let rendered = client.engine.predicted_state().unwrap();
            assert!(matches!(rendered.status, GameStatus::Complete { .. }));
            assert_eq!(rendered.properties.boost, None);
            assert!(rendered.arena.boost_pads.is_empty());

            let mut nonterminal = completed;
            nonterminal.status = GameStatus::Started { server_id: 1 };
            assert!(
                game_client_from_snapshot_frame(42, &snapshot_frame(42, &nonterminal)).is_err()
            );
        }

        let completed = legacy_timed_boost_duel_state();
        game_client_from_snapshot_frame(42, &snapshot_frame(42, &completed))
            .expect("timed Boost-team completion must remain renderable");

        let mut malformed_current = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Competitive,
            None,
            0,
        );
        malformed_current.add_player(7, None).unwrap();
        malformed_current.add_player(8, None).unwrap();
        malformed_current.status = GameStatus::Complete {
            winning_snake_id: None,
        };
        malformed_current.arena.boost_pads.pop();
        assert!(
            game_client_from_snapshot_frame(42, &snapshot_frame(42, &malformed_current)).is_err()
        );
    }

    #[test]
    fn activate_boost_wire_command_contains_only_snake_identity() {
        let command = GameCommand::ActivateBoost { snake_id: 17 };
        assert_eq!(
            serde_json::to_value(command).unwrap(),
            serde_json::json!({ "ActivateBoost": { "snake_id": 17 } })
        );
    }

    #[test]
    fn deactivate_boost_wire_command_contains_only_snake_identity() {
        let command = GameCommand::DeactivateBoost { snake_id: 17 };
        assert_eq!(
            serde_json::to_value(command).unwrap(),
            serde_json::json!({ "DeactivateBoost": { "snake_id": 17 } })
        );
    }

    #[test]
    fn frame_parse_recognizes_absolute_boost_packet_event() {
        let frame = r#"{"GameEvent":{"game_id":1,"tick":12,"sequence":3,"stream_seq":4,"user_id":null,"event":{"BoostPacketCollected":{"pad_id":2,"snake_id":1,"charge_ms_after":2000,"respawn_at_tick":172}}}}"#;
        let frame_val: serde_json::Value = serde_json::from_str(frame).unwrap();
        let message: GameEventMessage =
            serde_json::from_value(frame_val["GameEvent"].clone()).unwrap();

        match message.event {
            GameEvent::BoostPacketCollected {
                pad_id,
                snake_id,
                charge_ms_after,
                respawn_at_tick,
            } => {
                assert_eq!((pad_id, snake_id), (2, 1));
                assert_eq!(charge_ms_after, 2_000);
                assert_eq!(respawn_at_tick, 172);
            }
            other => panic!("expected BoostPacketCollected, got {other:?}"),
        }
    }

    /// The one line that makes score celebrations predicted rather than
    /// delayed: the visual-state payload the web renderer polls every frame
    /// must expose goal cues from the PREDICTED state, which runs ahead of the
    /// committed state the client receives from the transport.
    #[test]
    fn predicted_visual_state_exposes_goal_cues_ahead_of_committed_state() {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        state.add_player(1, None).unwrap();
        state.add_player(2, None).unwrap();
        {
            // One movement step outside its own goal mouth, carrying 2 points.
            let snake = &mut state.arena.snakes[0];
            snake.body = vec![Position { x: 10, y: 18 }, Position { x: 13, y: 18 }];
            snake.direction = Direction::Left;
            snake.is_alive = true;
            snake.food = 4;
        }
        let tick_ms = i64::from(state.properties.tick_duration_ms);

        let mut client = GameClient {
            engine: GameEngine::try_new_from_state(42, state).unwrap(),
        };
        client.rebuild_predicted_state(tick_ms * 4).unwrap();

        let visual: serde_json::Value =
            serde_json::from_str(&client.get_predicted_visual_state_json().unwrap()).unwrap();

        assert_eq!(visual["committed_tick"], 0);
        assert!(visual["predicted_tick"].as_u64().unwrap() >= 2);
        assert_eq!(
            visual["goals"],
            serde_json::json!([{
                "tick": 2,
                "team_id": 0,
                "snake_id": 0,
                "position": { "x": 9, "y": 18 },
                "points": 2,
            }]),
            "the payload must carry predicted goal cues under the `goals` key"
        );
        assert!(
            client.engine.committed_state().recent_goals.is_empty(),
            "committed state has not reached the goal, so reading it would delay the celebration"
        );
    }

    #[test]
    fn client_boundary_roundtrip_matches_shared_engine_with_nonzero_boost() {
        let (state, snake_id) = charged_duel_state();
        let committed_json = serde_json::to_string(&state).unwrap();

        let restored: GameState = serde_json::from_str(&committed_json).unwrap();
        restored.validate_boost_invariants().unwrap();
        assert_eq!(restored.sync_hash(), state.sync_hash());

        // Exercise the same methods exported through wasm-bindgen, then compare
        // their predicted result with an independently driven native engine.
        let mut client = GameClient {
            engine: GameEngine::try_new_from_state(42, restored.clone()).unwrap(),
        };
        client.set_local_player_id(7);
        let boundary_command: GameCommandMessage =
            serde_json::from_str(&client.process_activate_boost(snake_id).unwrap()).unwrap();
        client.rebuild_predicted_state(100).unwrap();

        let mut native = GameEngine::try_new_from_state(42, restored).unwrap();
        native.set_local_player_id(7);
        let native_command = native
            .process_local_command(GameCommand::ActivateBoost { snake_id })
            .unwrap();
        native.rebuild_predicted_state(100).unwrap();

        assert_eq!(boundary_command, native_command);
        let boundary_state: GameState =
            serde_json::from_str(&client.get_game_state_json().unwrap()).unwrap();
        let native_state = native.predicted_state().unwrap();
        assert_eq!(boundary_state.sync_hash(), native_state.sync_hash());
        assert_eq!(
            serde_json::to_value(&boundary_state).unwrap(),
            serde_json::to_value(native_state).unwrap()
        );

        let snake = &boundary_state.arena.snakes[snake_id as usize];
        assert!(snake.boost().active);
        assert_eq!(
            snake.boost().charge_ms,
            state.arena.snakes[snake_id as usize].boost().charge_ms
                - state.properties.tick_duration_ms
        );
        assert_eq!(snake.speed_milli(), 1_500);

        let boundary_stop: GameCommandMessage =
            serde_json::from_str(&client.process_deactivate_boost(snake_id).unwrap()).unwrap();
        let native_stop = native
            .process_local_command(GameCommand::DeactivateBoost { snake_id })
            .unwrap();
        assert_eq!(boundary_stop, native_stop);

        client.rebuild_predicted_state(150).unwrap();
        native.rebuild_predicted_state(150).unwrap();
        let boundary_stopped: GameState =
            serde_json::from_str(&client.get_game_state_json().unwrap()).unwrap();
        let native_stopped = native.predicted_state().unwrap();
        assert_eq!(boundary_stopped.sync_hash(), native_stopped.sync_hash());
        assert_eq!(
            serde_json::to_value(&boundary_stopped).unwrap(),
            serde_json::to_value(native_stopped).unwrap()
        );

        let stopped_snake = &boundary_stopped.arena.snakes[snake_id as usize];
        assert!(!stopped_snake.boost().active);
        assert_eq!(
            stopped_snake.boost().charge_ms,
            state.arena.snakes[snake_id as usize].boost().charge_ms
                - state.properties.tick_duration_ms
        );
        assert_eq!(stopped_snake.speed_milli(), 1_000);
    }
}
