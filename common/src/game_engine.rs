use anyhow::Result;
use crate::{GameCommand, GameEventMessage, GameEvent, GameState, GameCommandMessage, GameType, CommandId, DEFAULT_TICK_INTERVAL_MS};

pub struct GameEngine {
    game_id: u32,
    committed_state: GameState,
    predicted_state: Option<GameState>,

    event_log: Vec<GameEventMessage>,

    tick_duration_ms: u32,
    committed_state_lag_ms: u32,

    local_player_id: Option<u32>,

    start_ms: i64,
    command_counter: u32,
    /// Track the last command tick sent
    last_command_tick: Option<u32>,
}


impl GameEngine {
    pub fn new(game_id: u32, start_ms: i64) -> Self {
        GameEngine {
            game_id,
            committed_state: GameState::new(10, 10, GameType::TeamMatch { per_team: 1 }, None, start_ms),
            predicted_state: Some(GameState::new(10, 10, GameType::TeamMatch { per_team: 1 }, None, start_ms)),
            event_log: Vec::new(),
            tick_duration_ms: DEFAULT_TICK_INTERVAL_MS as u32,
            committed_state_lag_ms: 500,
            local_player_id: None,
            start_ms,
            command_counter: 0,
            last_command_tick: None,
        }
    }

    pub fn new_with_seed(game_id: u32, start_ms: i64, rng_seed: u64) -> Self {
        Self::new_with_seed_and_type(game_id, start_ms, rng_seed, GameType::TeamMatch { per_team: 1 })
    }
    
    pub fn new_with_seed_and_type(game_id: u32, start_ms: i64, rng_seed: u64, game_type: GameType) -> Self {
        // Extract dimensions and tick duration from custom settings if available
        let (width, height, tick_duration_ms) = match &game_type {
            GameType::Custom { settings } => (
                settings.arena_width,
                settings.arena_height,
                settings.tick_duration_ms as u32,
            ),
            _ => (40, 40, DEFAULT_TICK_INTERVAL_MS as u32), // Default dimensions for non-custom games
        };
        
        GameEngine {
            game_id,
            committed_state: GameState::new(width, height, game_type.clone(), Some(rng_seed), start_ms),
            predicted_state: Some(GameState::new(width, height, game_type, None, start_ms)), // Client prediction doesn't need RNG
            event_log: Vec::new(),
            tick_duration_ms,
            committed_state_lag_ms: 500,
            local_player_id: None,
            start_ms,
            command_counter: 0,
            last_command_tick: None,
        }
    }

    pub fn new_from_state(game_id: u32, start_ms: i64, game_state: GameState) -> Self {
        // Extract tick duration from custom settings if available
        let tick_duration_ms = match &game_state.game_type {
            GameType::Custom { settings } => settings.tick_duration_ms as u32,
            _ => DEFAULT_TICK_INTERVAL_MS as u32, // Default for non-custom games
        };
        
        // Use start_ms from game_state if available, otherwise use the provided start_ms
        let actual_start_ms = if game_state.start_ms != 0 {
            game_state.start_ms
        } else {
            start_ms
        };
        
        GameEngine {
            game_id,
            committed_state: game_state.clone(),
            predicted_state: Some(game_state),
            event_log: Vec::new(),
            tick_duration_ms,
            committed_state_lag_ms: 500,
            local_player_id: None,
            start_ms: actual_start_ms,
            command_counter: 0,
            last_command_tick: None,
        }
    }

    pub fn set_local_player_id(&mut self, player_id: u32) {
        self.local_player_id = Some(player_id);
    }
    
    /// Process a local command with client-side prediction
    pub fn process_local_command(&mut self, command: GameCommand) -> Result<GameCommandMessage> {
        let Some(player_id) = self.local_player_id else {
            return Err(anyhow::anyhow!("Local player ID not set"));
        };
        
        let mut predicted_tick = self.predicted_state.as_ref()
            .map(|s| s.current_tick())
            .unwrap_or(0);
        
        // Ensure the tick is higher than the last command sent
        if let Some(last_tick) = self.last_command_tick {
            if predicted_tick <= last_tick {
                predicted_tick = last_tick + 1;
            }
        }
        
        // Update the last command tick
        self.last_command_tick = Some(predicted_tick);
        
        // Create command with client ID
        let command_message = GameCommandMessage {
            command_id_client: CommandId {
                tick: predicted_tick,
                user_id: player_id,
                sequence_number: 0, // Sequence number no longer needed
            },
            command_id_server: None,
            command,
        };
        
        // Add to committed state command queue
        self.committed_state.schedule_command(&command_message);
        
        // Add to predicted state command queue
        // if let Some(predicted_state) = &mut self.predicted_state {
        //     predicted_state.schedule_command(&command_message);
        // }
        
        Ok(command_message)
    }


    /// Process a server event and reconcile with local predictions
    pub fn process_server_event(&mut self, event_message: &GameEventMessage, _current_ts: i64) -> Result<()> {
        // For CommandScheduled events, we can skip them as they're already handled locally
        // if matches!(event_message.event, GameEvent::CommandScheduled { .. }) {
        //     return Ok(());
        // }
        
        // Step the committed state forward to the event tick before applying the event
        // This ensures events are applied at the correct tick (similar to ReplayViewer)
        while self.committed_state.current_tick() < event_message.tick {
            self.committed_state.tick_forward()?;
        }
        
        match &event_message.event {
            GameEvent::Snapshot { game_state } => {
                self.committed_state = game_state.clone();
            }
            GameEvent::CommandScheduled { .. } => {
                // Apply event to committed state
                self.committed_state.apply_event(event_message.event.clone(), None);
                
                // Also schedule in predicted state if it exists
                if let Some(predicted_state) = &mut self.predicted_state {
                    predicted_state.apply_event(event_message.event.clone(), None);
                }
            }
            _ => {
                // Otherwise apply event just to the committed state
                self.committed_state.apply_event(event_message.event.clone(), None);
            }
        }
        
        Ok(())
    }
    
    /// Rebuild predicted state from committed state and advance to current time
    pub fn rebuild_predicted_state(&mut self, current_ts: i64) -> Result<()> {
        // Calculate target tick
        let predicted_target_tick = ((current_ts - self.start_ms) / self.tick_duration_ms as i64) as u32;
        
        // Check if we need to rebuild by comparing with existing predicted state
        let needs_rebuild = self.predicted_state
            .as_ref()
            .map_or(false, |state| predicted_target_tick > state.current_tick());
        
        if needs_rebuild {
            // Preserve the command queue from the old predicted state
            // let command_queue = self.predicted_state
            //     .as_ref()
            //     .map(|state| state.command_queue.clone())
            //     .unwrap_or_else(|| CommandQueue::new());
            
            // Clone committed state and restore command queue
            let mut new_predicted_state = self.committed_state.clone();
            // new_predicted_state.command_queue = command_queue;
            
            // Advance to target tick
            while new_predicted_state.current_tick() < predicted_target_tick {
                new_predicted_state.tick_forward()?;
            }
            
            self.predicted_state = Some(new_predicted_state);
        }
        
        Ok(())
    }

    /// Run the required amount of ticks so that the game is at the given timestamp.
    /// Can be called from a very fast interval loop or requestAnimationFrame.
    pub fn run_until(&mut self, ts_ms: i64) -> Result<Vec<(u32, u64, GameEvent)>> {
        let predicted_target_tick = ((ts_ms - self.start_ms) / self.tick_duration_ms as i64) as u32;
        let lag_ticks = self.committed_state_lag_ms / self.tick_duration_ms;
        let lagged_target_tick = predicted_target_tick.saturating_sub(lag_ticks);
        let mut out: Vec<(u32, u64, GameEvent)> = Vec::new();

        while self.committed_state.current_tick() < lagged_target_tick {
            let current_tick = self.committed_state.current_tick();
            for (sequence, event) in self.committed_state.tick_forward()? {
                eprintln!("game_engine: Emitting event at tick {} seq {}: {:?}", current_tick, sequence, event);
                out.push((current_tick, sequence, event));
            }
        }

        // Run predicted state to current time (not lagged)
        if let Some(predicted_state) = &mut self.predicted_state {
            while predicted_state.current_tick() < predicted_target_tick {
                predicted_state.tick_forward()?;
            }
        }

        Ok(out)
    }

    pub fn process_command(
        &mut self,
        command_message: GameCommandMessage,
    ) -> Result<GameCommandMessage> {

        let server_scheduled_tick = command_message.command_id_client.tick
            .max(self.committed_state.current_tick());

        let received_order = self.command_counter;
        self.command_counter += 1;
        
        let command_id_server = CommandId { 
            tick: server_scheduled_tick, 
            user_id: command_message.command_id_client.user_id,
            sequence_number: received_order
        };

        let cmd = GameCommandMessage {
            command_id_client: command_message.command_id_client,
            command_id_server: Some(command_id_server),
            command: command_message.command,
        };
        
        self.committed_state.schedule_command(&cmd);
        if let Some(predicted_state) = &mut self.predicted_state {
            predicted_state.schedule_command(&cmd);
        }

        Ok(cmd)
    }


    // --- JSON Getters for WASM ---
    pub fn get_predicted_state_json(&self) -> Result<String> {
        Ok(serde_json::to_string(&self.predicted_state)?)
    }
    
    pub fn get_committed_state(&self) -> &GameState {
        &self.committed_state
    }
    
    pub fn get_committed_state_json(&self) -> Result<String> {
        Ok(serde_json::to_string(&self.committed_state)?)
    }
    
    pub fn get_event_log_json(&self) -> Result<String> {
        Ok(serde_json::to_string(&self.event_log)?)
    }
    
    pub fn game_id(&self) -> u32 { self.game_id }
    
    pub fn current_tick(&self) -> u32 {
        self.committed_state.current_tick()
    }
    
    pub fn get_predicted_tick(&self) -> u32 {
        self.predicted_state
            .as_ref()
            .map(|state| state.current_tick())
            .unwrap_or_else(|| self.committed_state.current_tick())
    }

}

// Standalone function for server-side command processing
// pub fn server_process_incoming_command(
//     engine: &mut GameEngine,
//     command_msg: GameCommandMessage,
// ) -> Vec<GameEventMessage> {
//     // Add the command to the engine's pending commands
//     engine.pending_commands.push(command_msg.clone());
//     
//     // Return event indicating command is pending on server
//     vec![GameEventMessage {
//         game_id: engine.game_id,
//         tick: engine.committed_state.current_tick(),
//         user_id: None,
//         event: GameEvent::CommandPendingOnServer { 
//             command_message: command_msg 
//         },
//     }]
// }
