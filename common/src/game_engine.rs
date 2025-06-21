use anyhow::Result;
use std::collections::VecDeque;
use crate::{GameCommand, GameEventMessage, GameEvent, GameState, GameCommandMessage, GameType, CommandId};
use wasm_bindgen::prelude::*;

#[derive(Debug, Clone)]
struct UnconfirmedCommand {
    command_message: GameCommandMessage,
    local_sequence: u32,
}

pub struct GameEngine {
    game_id: u32,
    committed_state: GameState,
    predicted_state: Option<GameState>,

    event_log: Vec<GameEventMessage>,

    tick_duration_ms: u32,
    committed_state_lag_ms: u32,

    unconfirmed_local_commands: VecDeque<UnconfirmedCommand>,
    local_player_id: Option<u32>,
    local_sequence_counter: u32,

    start_ms: i64,
    command_counter: u32,
}


impl GameEngine {
    pub fn new(game_id: u32, start_ms: i64) -> Self {
        GameEngine {
            game_id,
            committed_state: GameState::new(10, 10, GameType::TeamMatch { per_team: 1 }, None),
            predicted_state: Some(GameState::new(10, 10, GameType::TeamMatch { per_team: 1 }, None)),
            event_log: Vec::new(),
            tick_duration_ms: 300,
            committed_state_lag_ms: 500,
            unconfirmed_local_commands: VecDeque::new(),
            local_player_id: None,
            local_sequence_counter: 0,
            start_ms,
            command_counter: 0,
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
            _ => (40, 40, 300), // Default dimensions for non-custom games
        };
        
        GameEngine {
            game_id,
            committed_state: GameState::new(width, height, game_type.clone(), Some(rng_seed)),
            predicted_state: Some(GameState::new(width, height, game_type, None)), // Client prediction doesn't need RNG
            event_log: Vec::new(),
            tick_duration_ms,
            committed_state_lag_ms: 500,
            unconfirmed_local_commands: VecDeque::new(),
            local_player_id: None,
            local_sequence_counter: 0,
            start_ms,
            command_counter: 0,
        }
    }

    pub fn new_from_state(game_id: u32, start_ms: i64, game_state: GameState) -> Self {
        // Extract tick duration from custom settings if available
        let tick_duration_ms = match &game_state.game_type {
            GameType::Custom { settings } => settings.tick_duration_ms as u32,
            _ => 300, // Default for non-custom games
        };
        
        GameEngine {
            game_id,
            committed_state: game_state.clone(),
            predicted_state: Some(game_state),
            event_log: Vec::new(),
            tick_duration_ms,
            committed_state_lag_ms: 500,
            unconfirmed_local_commands: VecDeque::new(),
            local_player_id: None,
            local_sequence_counter: 0,
            start_ms,
            command_counter: 0,
        }
    }

    pub fn set_local_player_id(&mut self, player_id: u32) {
        self.local_player_id = Some(player_id);
    }
    
    /// Get the last unconfirmed command that should be sent to server
    pub fn get_last_unconfirmed_command(&self) -> Option<&GameCommandMessage> {
        self.unconfirmed_local_commands.back()
            .map(|cmd| &cmd.command_message)
    }
    
    /// Process a local command with client-side prediction
    pub fn process_local_command(&mut self, command: GameCommand) -> Result<GameCommandMessage> {
        let Some(player_id) = self.local_player_id else {
            return Err(anyhow::anyhow!("Local player ID not set"));
        };
        
        let predicted_tick = self.predicted_state.as_ref()
            .map(|s| s.current_tick())
            .unwrap_or(0);
        
        // Create command with client ID
        let command_message = GameCommandMessage {
            command_id_client: CommandId {
                tick: predicted_tick,
                user_id: player_id,
                sequence_number: self.local_sequence_counter,
            },
            command_id_server: None,
            command,
        };
        
        // Add to unconfirmed commands
        self.unconfirmed_local_commands.push_back(UnconfirmedCommand {
            command_message: command_message.clone(),
            local_sequence: self.local_sequence_counter,
        });
        self.local_sequence_counter += 1;
        
        // Apply to predicted state immediately
        if let Some(predicted_state) = &mut self.predicted_state {
            predicted_state.schedule_command(&command_message);
        }
        
        Ok(command_message)
    }


    /// Process a server event and reconcile with local predictions
    pub fn process_server_event(&mut self, event: &GameEvent) -> Result<()> {
        // Apply event to committed state
        self.committed_state.apply_event(event.clone(), None);
        
        // Handle specific events that require reconciliation
        match event {
            GameEvent::CommandScheduled { command_message } => {
                // Remove matching unconfirmed command if this is our command
                if let Some(player_id) = self.local_player_id {
                    if command_message.command_id_client.user_id == player_id {
                        self.unconfirmed_local_commands.retain(|unconfirmed| {
                            unconfirmed.command_message.command_id_client.sequence_number 
                                != command_message.command_id_client.sequence_number
                        });
                    }
                }
                
                // Rebuild predicted state
                self.rebuild_predicted_state()?;
            }
            GameEvent::Snapshot { game_state } => {
                // Full state sync - clear unconfirmed commands and reset
                self.committed_state = game_state.clone();
                self.unconfirmed_local_commands.clear();
                self.predicted_state = Some(game_state.clone());
            }
            _ => {
                // Other events just need to be applied to predicted state too
                if let Some(predicted_state) = &mut self.predicted_state {
                    predicted_state.apply_event(event.clone(), None);
                }
            }
        }
        
        Ok(())
    }
    
    /// Rebuild predicted state from committed state + unconfirmed commands
    fn rebuild_predicted_state(&mut self) -> Result<()> {
        // Clone committed state as base
        self.predicted_state = Some(self.committed_state.clone());
        
        if let Some(predicted_state) = &mut self.predicted_state {
            // Re-apply all unconfirmed commands
            for unconfirmed in &self.unconfirmed_local_commands {
                predicted_state.schedule_command(&unconfirmed.command_message);
            }
        }
        
        Ok(())
    }

    /// Run the required amount of ticks so that the game is at the given timestamp.
    /// Can be called from a very fast interval loop or requestAnimationFrame.
    pub fn run_until(&mut self, ts_ms: i64) -> Result<Vec<GameEvent>> {
        let predicted_target_tick = ((ts_ms - self.start_ms) / self.tick_duration_ms as i64) as u32;
        let lag_ticks = (self.committed_state_lag_ms / self.tick_duration_ms) as u32;
        let lagged_target_tick = predicted_target_tick.saturating_sub(lag_ticks);
        let mut out: Vec<GameEvent> = Vec::new();

        while self.committed_state.current_tick() < lagged_target_tick {
            for event in self.committed_state.tick_forward()? {
                out.push(event);
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
