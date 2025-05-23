use anyhow::{Context, Result};
use std::collections::{BinaryHeap, VecDeque};
use wasm_bindgen::JsValue;
use crate::{GameCommand, GameEventMessage, GameEvent, GameState, GameCommandMessage};
use crate::util::{RandomGenerator};

#[derive(Debug)]
pub struct GameEngine {
    game_id: u32,
    committed_state: GameState,
    predicted_state: Option<GameState>,

    event_log: Vec<GameEventMessage>,

    pending_commands: BinaryHeap<GameCommandMessage>,
    tick_duration_ms: u32,
    committed_state_lag_ms: u32,

    unconfirmed_local_inputs: VecDeque<(u32, GameCommand, u32)>,
    local_player_id: Option<u32>,

    random_generator: Option<Box<dyn RandomGenerator>>,
    start_ms: i64,
    command_counter: u32,
}


impl GameEngine {
    pub fn new(game_id: u32, start_ms: i64) -> Self {
        GameEngine {
            game_id,
            committed_state: GameState::new(game_id, 10, 10),
            predicted_state: Some(GameState::new(game_id, 10, 10)),
            event_log: Vec::new(),
            pending_commands: BinaryHeap::new(),
            tick_duration_ms: 300,
            committed_state_lag_ms: 500,
            unconfirmed_local_inputs: VecDeque::new(),
            local_player_id: None,
            random_generator: None,
            start_ms,
            command_counter: 0,
        }
    }

    pub fn set_local_player_id(&mut self, player_id: u32) {
        self.local_player_id = Some(player_id);
    }

    pub fn set_random_generator(&mut self, generator: Box<dyn RandomGenerator>) {
        self.random_generator = Some(generator);
    }

    /// Run the required amount of ticks so that the game is at the given timestamp.
    /// Can be called from a very fast interval loop or requestAnimationFrame.
    pub fn run_until(&mut self, ts_ms: i64) -> Result<Vec<GameEvent>> {
        let predicted_target_tick = ((ts_ms - self.start_ms) / self.tick_duration_ms as i64) as u32;
        let lagged_target_tick = predicted_target_tick - (self.committed_state_lag_ms / self.tick_duration_ms) as u32;
        let mut out: Vec<GameEvent> = Vec::new();

        while self.committed_state.current_tick() < lagged_target_tick {
            while let Some(&cmd) = self.pending_commands.peek() {
                if cmd.tick > self.committed_state.current_tick() {
                    break;
                }

                let popped_cmd_msg = self.pending_commands.pop().context("Failed to pop command")?;

                if popped_cmd_msg.tick < self.committed_state.current_tick() {
                    // Ignore commands that are already in the past
                    continue;
                }

                for event in self.committed_state.exec_command(&popped_cmd_msg.command)? {
                    out.push(event);
                }
            }

            for event in self.committed_state.exec_command(&GameCommand::Tick)? {
                out.push(event);
            }
        }

        if let Some(predicted_state) = &mut self.predicted_state {
            *predicted_state = self.committed_state.clone();
            let mut remaining_commands = self.pending_commands.clone();
            while predicted_state.current_tick() < predicted_target_tick {
                while let Some(&cmd) = remaining_commands.peek() {
                    if cmd.tick > predicted_state.current_tick() {
                        break;
                    }

                    let popped_cmd_msg = remaining_commands.pop()
                        .context("Failed to pop remaining command")?;

                    if popped_cmd_msg.tick < predicted_state.current_tick() {
                        // Ignore commands that are already in the past
                        continue;
                    }

                    predicted_state.exec_command(&popped_cmd_msg.command)?;
                }

                predicted_state.exec_command(&GameCommand::Tick)?;
            }
        }

        Ok(out)
    }

    fn apply_command(&mut self, command: &GameCommand) -> Vec<GameEventMessage> {
        let event = GameEventMessage {
            game_id: self.game_id,
            tick,
            user_id: None,
            event: GameEvent::CommandPendingOnServer(GameCommandMessage {
                tick: tick,
                received_order: self.command_counter,
                user_id: 0,
                command: command.clone(),
            }),
        };
        self.committed_state.apply_event(&event);

        let mut emitted_events: Vec<GameEventMessage> = Vec::new();

        emitted_events
    }

    pub fn server_process_incoming_command(
        &mut self,
        command: GameCommand,
        user_id: u32,
        client_command_tick: u32,
    ) -> Result<GameCommandMessage> {

        // let server_scheduled_tick = client_command_tick
        //     .max(self.committed_state.current_tick() + 1) // At least the next committed tick
        //     .max(self.committed_state.current_tick() + self.frames_to_lag / 4); // Give some buffer, heuristic

        let received_order = self.command_counter;
        self.command_counter += 1; // Increment for next command

        let cmd = GameCommandMessage {
            tick: server_scheduled_tick,
            received_order,
            user_id,
            command: command.clone(),
        };
        self.pending_commands.push(cmd.clone());

        // Create and return the event that informs clients about this pending command.
        let pending_event_notification = GameEventMessage {
            game_id: self.game_id,
            // The 'tick' of this notification event itself could be the current server committed_tick,
            // indicating when the server became aware and scheduled it.
            tick: self.committed_state.current_tick(),
            user_id: None, // System event about a user's action
            event: GameEvent::CommandPendingOnServer(cmd),
        };
        pending_event_notification
    }

    /// SERVER: Advances the game by one tick. Processes commands, simulates, generates events.
    /// Returns events that should be broadcast to clients.
    pub fn server_tick(&mut self) -> Vec<GameEventMessage> {
        let mut emitted_events: Vec<GameEventMessage> = Vec::new();
        let current_authoritative_tick = self.committed_state.current_tick() + 1;

        // 1. Process scheduled commands for this tick
        while let Some(scheduled_cmd) = self.pending_commands.peek() {
            if scheduled_cmd.tick <= current_authoritative_tick {
                let cmd = self.pending_commands.pop().unwrap();

                // Ensure committed state is at the command's tick (or tick before if command is start of tick)
                // For simplicity, assume command applies AT cmd.target_tick.
                // If committed_state is behind, simulate to catch up.
                while self.committed_state.current_tick() < cmd.tick -1 {
                    let sim_events = self.committed_state.advance_tick_simulation();
                    for ev in sim_events {
                        self.event_log.push(ev.clone());
                        emitted_events.push(ev);
                    }
                }
                self.committed_state.snapshot.tick = cmd.tick; // Set tick for event application

                let event_opt: Option<GameEventMessage> = match cmd.command {
                    GameCommand::Turn(direction) => {
                        // Find the player_id associated with this user_id
                        self.committed_state.snapshot.arena.snakes.iter()
                            .find(|(_, snake)| snake.user_id == cmd.user_id && snake.is_alive)
                            .map(|(player_id, _)| GameEventMessage {
                                game_id: self.game_id, tick: cmd.tick, user_id: Some(cmd.user_id),
                                event: GameEvent::PlayerTurned { player_id: *player_id, direction },
                            })
                    }
                    GameCommand::SpawnPlayer => {
                        if !self.committed_state.snapshot.arena.snakes.values().any(|s| s.user_id == cmd.user_id && s.is_alive) {
                            if let Some((pos, dir)) = self.committed_state.find_spawn_location_and_direction(cmd.user_id) {
                                let player_id = self.committed_state.snapshot.next_player_id_to_assign;
                                Some(GameEventMessage {
                                    game_id: self.game_id, tick: cmd.tick, user_id: Some(cmd.user_id),
                                    event: GameEvent::PlayerJoined { player_id, user_id: cmd.user_id, initial_pos: pos, initial_direction: dir },
                                })
                            } else { None /* No space to spawn */ }
                        } else { None /* Player already in game */ }
                    }
                };

                if let Some(event) = event_opt {
                    self.committed_state.apply_event(&event);
                    self.event_log.push(event.clone());
                    emitted_events.push(event);
                }
            } else {
                break; // Next command is for a future tick
            }
        }

        // 2. Advance committed_state simulation for the current tick if no command already set it.
        // Or if it already processed commands for current_authoritative_tick, this advances it one more step.
        if self.committed_state.current_tick() < current_authoritative_tick {
            self.committed_state.snapshot.tick = current_authoritative_tick -1; // advance_tick_simulation will increment it
            let sim_events = self.committed_state.advance_tick_simulation(); // advances to current_authoritative_tick
            for ev in sim_events {
                self.event_log.push(ev.clone());
                emitted_events.push(ev);
            }
        }


        // 3. Periodically create snapshots
        if current_authoritative_tick > 0 && current_authoritative_tick % (self.frames_to_lag * 2) == 0 { // e.g., every 2*lag frames
            let snapshot_event = GameEventMessage {
                game_id: self.game_id, tick: current_authoritative_tick, user_id: None,
                event: GameEvent::Snapshot(Box::new(self.committed_state.snapshot.clone())),
            };
            self.event_log.push(snapshot_event.clone());
            // Note: Snapshot events are large. Clients might request them or get them less frequently.
            // For now, include in broadcast if generated.
            emitted_events.push(snapshot_event);
        }
        emitted_events
    }

    /// CLIENT: Queues a local command for prediction and sends to server.
    pub fn client_queue_command(&mut self, command_js: JsValue) -> Result<(), JsValue> {
        let command: GameCommand = command_js.into_serde().map_err(|e| JsValue::from_str(&format!("Cmd Deser Error: {}", e)))?;
        let local_player_id = self.local_player_id.ok_or_else(|| JsValue::from_str("Local player ID not set"))?;
        let current_predicted_tick = self.predicted_state.current_tick();

        // Create a speculative event for client-side prediction
        // The server will ultimately decide the true event and its tick.
        let speculative_event_kind_opt = match command {
            GameCommand::Turn(direction) => {
                // Client predicts its own turn
                Some(GameEvent::PlayerTurned {player_id: local_player_id, direction })
            }
            GameCommand::SpawnPlayer => {
                // Client predicts its own spawn. Server confirms/denies/adjusts.
                if self.predicted_state.snapshot.arena.snakes.values().any(|s| s.id == local_player_id && s.is_alive) {
                    None // Already spawned in prediction
                } else {
                    self.predicted_state.find_spawn_location_and_direction(local_player_id)
                        .map(|(pos, dir)| GameEvent::PlayerJoined { player_id: local_player_id, user_id: local_player_id, initial_pos: pos, initial_direction: dir})
                }
            }
        };

        if let Some(speculative_event_kind) = speculative_event_kind_opt {
            let speculative_event = GameEventMessage {
                game_id: self.game_id,
                tick: current_predicted_tick, // Predict at current tick
                user_id: Some(local_player_id), // Assuming local_player_id is also user_id for this context
                event: speculative_event_kind,
            };
            self.predicted_state.apply_event(&speculative_event);
        }

        // Store command to send to server and for reconciliation
        self.unconfirmed_local_inputs.push_back((current_predicted_tick, command.clone(), local_player_id));

        // TODO: Actually send `command` along with `current_predicted_tick` and `local_player_id` (as user_id) to server.
        // e.g., network_send_command(command, current_predicted_tick, local_player_id);

        Ok(())
    }

    /// CLIENT: Processes events received from the server.
    pub fn client_receive_server_events(&mut self, events_js: JsValue) -> Result<(), JsValue> {
        let server_events: Vec<GameEventMessage> = events_js.into_serde().map_err(|e| JsValue::from_str(&format!("Events Deser Error: {}", e)))?;

        for event in server_events {
            if event.game_id != self.game_id { continue; }

            // Advance committed state to the tick BEFORE the event, applying simulation logic
            while self.committed_state.current_tick() < event.tick -1 {
                let _sim_events = self.committed_state.advance_tick_simulation();
                // These simulation events from committed state should match server's implicit ones.
                // We could add them to event_log if server doesn't send them explicitly.
            }

            // Apply the authoritative event from the server
            self.committed_state.apply_event(&event);
            self.event_log.push(event.clone()); // Maintain a local copy of the authoritative log

            // Attempt to remove confirmed commands from unconfirmed_local_inputs.
            // This is a simplified reconciliation; real systems use sequence numbers.
            if let Some(event_user_id) = event.user_id {
                if Some(event_user_id) == self.local_player_id { // Check if it's an event for our player
                    self.unconfirmed_local_inputs.retain(|(cmd_tick, _cmd, _player_id)| {
                        // A more robust check would compare command type, sequence numbers, etc.
                        // If event is for a tick later than command, keep command.
                        // If event is PlayerTurned and command was Turn, and ticks align, it's likely confirmed.
                        // This needs careful design. For now, a simple tick check.
                        !(event.tick >= *cmd_tick &&
                            event_confirms_command_kind(&event.event, &_cmd))
                    });
                }
            }
        }

        // ROLLBACK AND REPLAY:
        // 1. Reset predicted_state to the latest committed_state.
        self.predicted_state = self.committed_state.clone();

        // 2. Replay unconfirmed local inputs.
        let inputs_to_replay = self.unconfirmed_local_inputs.clone(); // Avoid borrow issues
        for (cmd_tick, command, player_id) in inputs_to_replay {
            // Ensure predicted_state is at the correct tick for the command
            while self.predicted_state.current_tick() < cmd_tick {
                let _ = self.predicted_state.advance_tick_simulation();
            }
            // Re-create the speculative event based on the replayed command
            // This logic should mirror client_queue_command's event creation
            let speculative_event_kind_opt = match command {
                GameCommand::Turn(direction) => Some(GameEvent::PlayerTurned {player_id, direction }),
                GameCommand::SpawnPlayer => {
                    if self.predicted_state.snapshot.arena.snakes.values().any(|s| s.id == player_id && s.is_alive) { None }
                    else {
                        self.predicted_state.find_spawn_location_and_direction(player_id) // Assuming player_id is user_id
                            .map(|(pos, dir)| GameEvent::PlayerJoined { player_id, user_id: player_id, initial_pos: pos, initial_direction: dir})
                    }
                }
            };
            if let Some(kind) = speculative_event_kind_opt {
                let speculative_event = GameEventMessage {
                    game_id: self.game_id, tick: cmd_tick, user_id: Some(player_id),
                    event: kind,
                };
                self.predicted_state.apply_event(&speculative_event);
            }
        }
        Ok(())
    }

    /// CLIENT: Advances the predicted state by one tick (e.g., per frame for rendering).
    // #[wasm_bindgen]
    pub fn client_advance_predicted_tick(&mut self) {
        // Only advance if predicted is not too far ahead of committed (anti-rubberbanding measure)
        if self.predicted_state.current_tick() < self.committed_state.current_tick() + self.frames_to_lag * 2 {
            let _ = self.predicted_state.advance_tick_simulation();
        } else {
            // Predicted state is too far ahead, clone committed to prevent massive desyncs visually.
            // This is a hard correction; smoother methods might exist.
            self.predicted_state = self.committed_state.clone();
        }
    }

    /// To load a game from history (e.g., for a late-joining client or loading a save)
    pub fn load_from_event_log(&mut self, full_event_log: Vec<GameEventMessage>) {
        self.event_log.clear();
        self.pending_commands.clear();
        self.unconfirmed_local_inputs.clear();

        // Find the latest snapshot, if any.
        let mut last_snapshot_idx: Option<usize> = None;
        for (i, event) in full_event_log.iter().enumerate().rev() {
            if matches!(event.event, GameEvent::Snapshot(_)) {
                last_snapshot_idx = Some(i);
                break;
            }
        }

        // Initialize width/height from GameStarted or snapshot
        let mut initial_width = 10; // Default
        let mut initial_height = 10; // Default

        if let Some(event) = full_event_log.first() {
            if let GameEvent::GameStarted{width, height} = event.event {
                initial_width = width;
                initial_height = height;
            }
        }


        self.committed_state = ActiveGameState::new(self.game_id, initial_width, initial_height, 0);

        let events_to_apply_start_idx = if let Some(idx) = last_snapshot_idx {
            if let GameEvent::Snapshot(snap_data) = &full_event_log[idx].event {
                self.committed_state = ActiveGameState::from_snapshot(*snap_data.clone());
                initial_width = snap_data.arena.width; // update from snapshot
                initial_height = snap_data.arena.height;
                idx + 1 // Start applying events *after* the snapshot
            } else { 0 } // Should not happen if idx is Some
        } else { 0 }; // No snapshot, apply all events

        for i in 0..events_to_apply_start_idx { // Add events up to (but not including) start_idx to log
            self.event_log.push(full_event_log[i].clone());
        }

        for i in events_to_apply_start_idx..full_event_log.len() {
            let event = &full_event_log[i];
            while self.committed_state.current_tick() < event.tick -1 {
                let _ = self.committed_state.advance_tick_simulation(); // Fill gaps with simulation
            }
            self.committed_state.apply_event(event);
            self.event_log.push(event.clone());
        }

        // Predicted state should mirror committed state after loading. Client will predict from here.
        self.predicted_state = self.committed_state.clone();
    }


    // --- WASM Exposed Getters ---
    // #[wasm_bindgen(js_name = getPredictedStateSnapshotJson)]
    pub fn get_predicted_state_snapshot_json(&self) -> JsValue {
        JsValue::from_serde(&self.predicted_state.snapshot).unwrap()
    }
    // #[wasm_bindgen(js_name = getCommittedStateSnapshotJson)]
    pub fn get_committed_state_snapshot_json(&self) -> JsValue {
        JsValue::from_serde(&self.committed_state.snapshot).unwrap()
    }
    // #[wasm_bindgen(js_name = getEventLogJson)]
    pub fn get_event_log_json(&self) -> JsValue {
        JsValue::from_serde(&self.event_log).unwrap()
    }
    // #[wasm_bindgen(getter, js_name = gameId)]
    pub fn game_id(&self) -> u32 { self.game_id }

}

// Helper for client_receive_server_events reconciliation
fn event_confirms_command_kind(event_kind: &GameEvent, command: &GameCommand) -> bool {
    match (event_kind, command) {
        (GameEvent::PlayerTurned { .. }, GameCommand::Turn(_)) => true,
        (GameEvent::PlayerJoined { .. }, GameCommand::SpawnPlayer) => true,
        _ => false,
    }
}