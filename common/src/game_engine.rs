use anyhow::{Context, Result};
use std::collections::{BinaryHeap, VecDeque};
use crate::{GameCommand, GameEventMessage, GameEvent, GameState, GameCommandMessage, GameType};

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

    start_ms: i64,
    command_counter: u32,
}


impl GameEngine {
    pub fn new(game_id: u32, start_ms: i64) -> Self {
        GameEngine {
            game_id,
            committed_state: GameState::new(10, 10, None),
            predicted_state: Some(GameState::new(10, 10, None)),
            event_log: Vec::new(),
            pending_commands: BinaryHeap::new(),
            tick_duration_ms: 300,
            committed_state_lag_ms: 500,
            unconfirmed_local_inputs: VecDeque::new(),
            local_player_id: None,
            start_ms,
            command_counter: 0,
        }
    }

    pub fn new_with_seed(game_id: u32, start_ms: i64, rng_seed: u64) -> Self {
        Self::new_with_seed_and_type(game_id, start_ms, rng_seed, GameType::TeamMatch { per_team: 1 })
    }
    
    pub fn new_with_seed_and_type(game_id: u32, start_ms: i64, rng_seed: u64, game_type: GameType) -> Self {
        GameEngine {
            game_id,
            committed_state: GameState::new_with_type(10, 10, game_type.clone(), Some(rng_seed)),
            predicted_state: Some(GameState::new_with_type(10, 10, game_type, None)), // Client prediction doesn't need RNG
            event_log: Vec::new(),
            pending_commands: BinaryHeap::new(),
            tick_duration_ms: 300,
            committed_state_lag_ms: 500,
            unconfirmed_local_inputs: VecDeque::new(),
            local_player_id: None,
            start_ms,
            command_counter: 0,
        }
    }

    pub fn set_local_player_id(&mut self, player_id: u32) {
        self.local_player_id = Some(player_id);
    }


    /// Run the required amount of ticks so that the game is at the given timestamp.
    /// Can be called from a very fast interval loop or requestAnimationFrame.
    pub fn run_until(&mut self, ts_ms: i64) -> Result<Vec<GameEvent>> {
        let predicted_target_tick = ((ts_ms - self.start_ms) / self.tick_duration_ms as i64) as u32;
        let lag_ticks = (self.committed_state_lag_ms / self.tick_duration_ms) as u32;
        let lagged_target_tick = predicted_target_tick.saturating_sub(lag_ticks);
        let mut out: Vec<GameEvent> = Vec::new();

        while self.committed_state.current_tick() < lagged_target_tick {
            while let Some(cmd) = self.pending_commands.peek() {
                if cmd.tick > self.committed_state.current_tick() {
                    break;
                }

                let popped_cmd_msg = self.pending_commands.pop().context("Failed to pop command")?;

                if popped_cmd_msg.tick < self.committed_state.current_tick() {
                    // Ignore commands that are already in the past
                    continue;
                }

                for event in self.committed_state.exec_command(popped_cmd_msg.command)? {
                    out.push(event);
                }
            }

            for event in self.committed_state.exec_command(GameCommand::Tick)? {
                out.push(event);
            }
        }

        if let Some(predicted_state) = &mut self.predicted_state {
            *predicted_state = self.committed_state.clone();
            let mut remaining_commands = self.pending_commands.clone();
            while predicted_state.current_tick() < predicted_target_tick {
                while let Some(cmd) = remaining_commands.peek() {
                    if cmd.tick > predicted_state.current_tick() {
                        break;
                    }

                    let popped_cmd_msg = remaining_commands.pop()
                        .context("Failed to pop remaining command")?;

                    if popped_cmd_msg.tick < predicted_state.current_tick() {
                        // Ignore commands that are already in the past
                        continue;
                    }

                    predicted_state.exec_command(popped_cmd_msg.command)?;
                }

                predicted_state.exec_command(GameCommand::Tick)?;
            }
        }

        Ok(out)
    }

    pub fn server_process_incoming_command(
        &mut self,
        command: GameCommand,
        user_id: u32,
        client_command_tick: u32,
    ) -> Result<GameCommandMessage> {

        let server_scheduled_tick = client_command_tick
            .max(self.committed_state.current_tick() + 1) // At least the next committed tick
            .max(self.committed_state.current_tick() + 2); // Give some buffer

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
            event: GameEvent::CommandPendingOnServer {
                command_message: cmd.clone(),
            },
        };

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
pub fn server_process_incoming_command(
    engine: &mut GameEngine,
    command_msg: GameCommandMessage,
) -> Vec<GameEventMessage> {
    // Add the command to the engine's pending commands
    engine.pending_commands.push(command_msg.clone());
    
    // Return event indicating command is pending on server
    vec![GameEventMessage {
        game_id: engine.game_id,
        tick: engine.committed_state.current_tick(),
        user_id: None,
        event: GameEvent::CommandPendingOnServer { 
            command_message: command_msg 
        },
    }]
}
