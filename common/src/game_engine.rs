use crate::{
    CommandId, DEFAULT_TICK_INTERVAL_MS, GameCommand, GameCommandMessage, GameEvent,
    GameEventMessage, GameState, GameType, QueueMode,
};
use anyhow::Result;
use serde::Serialize;

/// How far past its last authoritative anchor the predicted state may free-run,
/// on top of the committed-state lag. Once the committed state stops advancing
/// (server silent, stream dead), prediction freezes after this window instead
/// of simulating a ghost game indefinitely.
pub const MAX_PREDICTION_AHEAD_MS: u32 = 1000;

/// Client-side synchronization health, updated on every processed server
/// message. Exposed to the UI so it can detect divergence (hash mismatches),
/// message loss (stream gaps), and trigger a resync instead of drifting.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SyncStatus {
    /// Last transport sequence seen (0 = none yet).
    pub last_stream_seq: u64,
    /// Number of distinct gap incidents observed.
    pub stream_gap_count: u32,
    /// Total messages known to have been missed.
    pub missed_messages: u64,
    /// Stale/duplicate messages skipped instead of double-applied.
    pub stale_messages_skipped: u64,
    /// Tick of the last server fingerprint probe processed.
    pub last_probe_tick: Option<u32>,
    /// Whether the last probe matched our committed state.
    pub last_probe_matched: Option<bool>,
    pub consecutive_hash_mismatches: u32,
    pub total_probes: u64,
    pub total_mismatches: u64,
    /// First tick at which a hash mismatch was observed (for RCA).
    pub first_mismatch_tick: Option<u32>,
    /// Set when a gap or repeated mismatch means the client should request a
    /// fresh snapshot. Cleared automatically when a snapshot is applied.
    pub needs_resync: bool,
    /// Highest tick seen in any server message (liveness reference).
    pub last_server_tick: u32,
    /// Server wall-clock from the last TickHash heartbeat (clock reference).
    pub last_server_ts_ms: Option<i64>,
}

pub struct GameEngine {
    game_id: u32,
    committed_state: GameState,
    predicted_state: Option<GameState>,
    event_log: Vec<GameEventMessage>,
    committed_state_lag_ms: u32,
    local_player_id: Option<u32>,
    command_counter: u32,
    last_command_tick: Option<u32>,
    sync_status: SyncStatus,
}

impl GameEngine {
    pub fn new(game_id: u32, start_ms: i64) -> Self {
        GameEngine {
            game_id,
            committed_state: GameState::new(
                10,
                10,
                GameType::TeamMatch { per_team: 1 },
                QueueMode::Quickmatch,
                None,
                start_ms,
            ),
            predicted_state: Some(GameState::new(
                10,
                10,
                GameType::TeamMatch { per_team: 1 },
                QueueMode::Quickmatch,
                None,
                start_ms,
            )),
            event_log: Vec::new(),
            committed_state_lag_ms: 500,
            local_player_id: None,
            command_counter: 0,
            last_command_tick: None,
            sync_status: SyncStatus::default(),
        }
    }

    pub fn new_with_seed(game_id: u32, start_ms: i64, rng_seed: u64) -> Self {
        Self::new_with_seed_and_type(
            game_id,
            start_ms,
            rng_seed,
            GameType::TeamMatch { per_team: 1 },
        )
    }

    pub fn new_with_seed_and_type(
        game_id: u32,
        start_ms: i64,
        rng_seed: u64,
        game_type: GameType,
    ) -> Self {
        // Extract dimensions and tick duration from custom settings if available
        let (width, height, _tick_duration_ms) = match &game_type {
            GameType::Custom { settings } => (
                settings.arena_width,
                settings.arena_height,
                settings.tick_duration_ms,
            ),
            _ => (40, 40, DEFAULT_TICK_INTERVAL_MS), // Default dimensions for non-custom games
        };

        GameEngine {
            game_id,
            committed_state: GameState::new(
                width,
                height,
                game_type.clone(),
                QueueMode::Quickmatch,
                Some(rng_seed),
                start_ms,
            ),
            predicted_state: Some(GameState::new(
                width,
                height,
                game_type,
                QueueMode::Quickmatch,
                None,
                start_ms,
            )), // Client prediction doesn't need RNG
            event_log: Vec::new(),
            committed_state_lag_ms: 500,
            local_player_id: None,
            command_counter: 0,
            last_command_tick: None,
            sync_status: SyncStatus::default(),
        }
    }

    pub fn new_from_state(game_id: u32, game_state: GameState) -> Self {
        let mut predicted_state = game_state.clone();
        predicted_state.rng = None; // Remove RNG so client doesn't generate food

        GameEngine {
            game_id,
            committed_state: game_state,
            predicted_state: Some(predicted_state),
            event_log: Vec::new(),
            committed_state_lag_ms: 500,
            local_player_id: None,
            command_counter: 0,
            last_command_tick: None,
            sync_status: SyncStatus::default(),
        }
    }

    pub fn set_local_player_id(&mut self, player_id: u32) {
        self.local_player_id = Some(player_id);
    }

    pub fn committed_state(&self) -> &GameState {
        &self.committed_state
    }

    pub fn predicted_state(&self) -> Option<&GameState> {
        self.predicted_state.as_ref()
    }

    /// Process a local command with client-side prediction
    pub fn process_local_command(&mut self, command: GameCommand) -> Result<GameCommandMessage> {
        let Some(player_id) = self.local_player_id else {
            return Err(anyhow::anyhow!("Local player ID not set"));
        };

        let current_predicted_tick = self
            .predicted_state
            .as_ref()
            .map(|s| s.current_tick())
            .unwrap_or(0);
        let mut predicted_tick = current_predicted_tick;

        // Ensure the tick is higher than the last command sent, but never let
        // the ratchet run away from the present: if a clock spike once pushed
        // last_command_tick far into the future, an unbounded ratchet would
        // schedule every subsequent command at that far-future tick — the
        // snake would simply stop responding. Rapid inputs may legitimately
        // queue a few ticks ahead; beyond that margin the ratchet resets.
        const MAX_COMMAND_AHEAD_TICKS: u32 = 8;
        if let Some(last_tick) = self.last_command_tick
            && predicted_tick <= last_tick
            && last_tick < current_predicted_tick + MAX_COMMAND_AHEAD_TICKS
        {
            predicted_tick = last_tick + 1;
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

        Ok(command_message)
    }

    /// Process a server event and reconcile with local predictions
    pub fn process_server_event(&mut self, event_message: &GameEventMessage) -> Result<()> {
        let is_snapshot = matches!(&event_message.event, GameEvent::Snapshot { .. });

        // Transport-integrity accounting. A gap means messages were lost
        // somewhere between the game executor and us; our committed state can
        // no longer be trusted and a snapshot resync is required.
        if event_message.stream_seq > 0 {
            let last = self.sync_status.last_stream_seq;
            if is_snapshot {
                // A snapshot re-anchors the stream: everything before it is
                // superseded, so the watermark resets unconditionally.
                self.sync_status.last_stream_seq = event_message.stream_seq;
            } else if last == 0 {
                self.sync_status.last_stream_seq = event_message.stream_seq;
            } else if event_message.stream_seq <= last {
                // Duplicate or stale delivery: applying it again would corrupt
                // state (e.g. FoodEaten grows the snake twice). Skip it.
                self.sync_status.stale_messages_skipped += 1;
                return Ok(());
            } else {
                if event_message.stream_seq > last + 1 {
                    self.sync_status.stream_gap_count += 1;
                    self.sync_status.missed_messages += event_message.stream_seq - last - 1;
                    self.sync_status.needs_resync = true;
                }
                self.sync_status.last_stream_seq = event_message.stream_seq;
            }
        }

        if event_message.tick > self.sync_status.last_server_tick {
            self.sync_status.last_server_tick = event_message.tick;
        }

        // Step the committed state forward to the event tick before applying the event
        // This ensures events are applied at the correct tick (similar to ReplayViewer)
        while self.committed_state.current_tick() < event_message.tick {
            self.committed_state.tick_forward(true)?;
        }

        // Fingerprint probes compare instead of mutate.
        if let GameEvent::TickHash {
            hash: server_hash,
            server_ts_ms,
        } = &event_message.event
        {
            let local_hash = self.committed_state.sync_hash();
            let matched = local_hash == *server_hash;
            self.sync_status.last_probe_tick = Some(event_message.tick);
            self.sync_status.last_probe_matched = Some(matched);
            self.sync_status.total_probes += 1;
            self.sync_status.last_server_ts_ms = Some(*server_ts_ms);
            if matched {
                self.sync_status.consecutive_hash_mismatches = 0;
            } else {
                self.sync_status.total_mismatches += 1;
                self.sync_status.consecutive_hash_mismatches += 1;
                if self.sync_status.first_mismatch_tick.is_none() {
                    self.sync_status.first_mismatch_tick = Some(event_message.tick);
                }
                // A single mismatch can be a transient in-flight command; two
                // in a row means we have genuinely diverged.
                if self.sync_status.consecutive_hash_mismatches >= 2 {
                    self.sync_status.needs_resync = true;
                }
            }
            return Ok(());
        }

        self.committed_state
            .apply_event(event_message.event.clone(), None);

        if is_snapshot {
            // Fresh authoritative state: divergence bookkeeping starts over.
            self.sync_status.needs_resync = false;
            self.sync_status.consecutive_hash_mismatches = 0;
        }

        // Also schedule in predicted state if it exists
        if let GameEvent::CommandScheduled { command_message: _ } = &event_message.event
            && let Some(predicted_state) = &mut self.predicted_state
        {
            predicted_state.apply_event(event_message.event.clone(), None);
        }

        Ok(())
    }

    /// Maximum tick the predicted state may reach given the committed state's
    /// current tick: the committed lag window plus a bounded free-run margin.
    /// When server messages stop arriving, the committed state freezes and
    /// this cap freezes prediction shortly after, instead of letting the
    /// client simulate a game the backend is no longer running.
    fn max_predicted_tick(&self) -> u32 {
        let tick_duration_ms = self.committed_state.properties.tick_duration_ms.max(1);
        let ahead_ticks =
            (self.committed_state_lag_ms + MAX_PREDICTION_AHEAD_MS) / tick_duration_ms;
        self.committed_state.current_tick() + ahead_ticks.max(1)
    }

    /// Rebuild predicted state from committed state and advance to current time
    pub fn rebuild_predicted_state(&mut self, current_ts: i64) -> Result<()> {
        // Handle pre-start case: if current time is before start time, don't advance
        let elapsed_ms = current_ts - self.committed_state.start_ms;
        if elapsed_ms < 0 {
            return Ok(());
        }

        // Calculate target tick, bounded so prediction cannot run away from
        // the last authoritative state (see `max_predicted_tick`).
        let tick_duration_ms = self.committed_state.properties.tick_duration_ms as i64;
        let predicted_target_tick =
            ((elapsed_ms / tick_duration_ms) as u32).min(self.max_predicted_tick());

        // Special case: if committed state is complete, always rebuild predicted from it
        // This ensures the predicted state shows the authoritative final outcome
        if self.committed_state.is_complete() {
            let mut new_predicted_state = self.committed_state.clone();
            new_predicted_state.rng = None;
            self.predicted_state = Some(new_predicted_state);
            return Ok(());
        }

        // Check if we need to rebuild by comparing with existing predicted state
        let needs_rebuild = self
            .predicted_state
            .as_ref()
            .is_none_or(|state| predicted_target_tick > state.current_tick());

        if needs_rebuild {
            // Clone committed state
            let mut new_predicted_state = self.committed_state.clone();

            // Remove RNG from predicted state so it doesn't generate food locally
            new_predicted_state.rng = None;

            // Advance to target tick (stops if game completes)
            while !new_predicted_state.is_complete()
                && new_predicted_state.current_tick() < predicted_target_tick
            {
                new_predicted_state.tick_forward(false)?;
            }

            self.predicted_state = Some(new_predicted_state);
        }

        Ok(())
    }

    /// Run the required amount of ticks so that the game is at the given timestamp.
    /// Can be called from a very fast interval loop or requestAnimationFrame.
    pub fn run_until(&mut self, ts_ms: i64) -> Result<Vec<(u32, u64, GameEvent)>> {
        let tick_duration_ms = self.committed_state.properties.tick_duration_ms;

        // Handle pre-start case: if current time is before start time, don't advance
        let elapsed_ms = ts_ms - self.committed_state.start_ms;
        if elapsed_ms < 0 {
            return Ok(Vec::new());
        }

        let wallclock_target_tick = (elapsed_ms / tick_duration_ms as i64) as u32;
        let lag_ticks = self.committed_state_lag_ms / tick_duration_ms;
        let lagged_target_tick = wallclock_target_tick.saturating_sub(lag_ticks);
        let mut out: Vec<(u32, u64, GameEvent)> = Vec::new();

        while !self.committed_state.is_complete()
            && self.committed_state.current_tick() < lagged_target_tick
        {
            let events = self.committed_state.tick_forward(false)?;
            // Label events with the POST-step tick: an event produced during
            // the step N -> N+1 describes the state at N+1. Receivers
            // fast-forward their committed state to the event's tick before
            // applying, so a pre-step label would make them apply movement
            // effects (FoodEaten, SnakeDied, ...) one movement-step early —
            // e.g. growing the snake a tick before the server does, forking
            // the body geometry permanently.
            let post_tick = self.committed_state.current_tick();
            for (sequence, event) in events {
                out.push((post_tick, sequence, event));
            }
        }

        // Run predicted state to current time (not lagged), bounded by the
        // prediction cap relative to the just-advanced committed state.
        let predicted_target_tick = wallclock_target_tick.min(self.max_predicted_tick());
        if let Some(predicted_state) = &mut self.predicted_state {
            while !predicted_state.is_complete()
                && predicted_state.current_tick() < predicted_target_tick
            {
                predicted_state.tick_forward(true)?;
            }
        }

        Ok(out)
    }

    pub fn process_command(
        &mut self,
        command_message: GameCommandMessage,
    ) -> Result<GameCommandMessage> {
        let server_scheduled_tick = command_message
            .command_id_client
            .tick
            .max(self.committed_state.current_tick());

        let received_order = self.command_counter;
        self.command_counter += 1;

        let command_id_server = CommandId {
            tick: server_scheduled_tick,
            user_id: command_message.command_id_client.user_id,
            sequence_number: received_order,
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

    pub fn game_id(&self) -> u32 {
        self.game_id
    }

    pub fn current_tick(&self) -> u32 {
        self.committed_state.current_tick()
    }

    pub fn get_predicted_tick(&self) -> u32 {
        self.predicted_state
            .as_ref()
            .map(|state| state.current_tick())
            .unwrap_or_else(|| self.committed_state.current_tick())
    }

    // --- Sync health / debugging ---

    pub fn sync_status(&self) -> &SyncStatus {
        &self.sync_status
    }

    pub fn sync_status_json(&self) -> Result<String> {
        Ok(serde_json::to_string(&self.sync_status)?)
    }

    /// Fingerprint of the committed state at its current tick.
    pub fn committed_sync_hash(&self) -> u64 {
        self.committed_state.sync_hash()
    }

    /// Call after a resync request has been issued so it isn't re-triggered
    /// every frame while the snapshot is in flight.
    pub fn clear_needs_resync(&mut self) {
        self.sync_status.needs_resync = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Direction;

    fn clockwise(direction: Direction) -> Direction {
        match direction {
            Direction::Up => Direction::Right,
            Direction::Right => Direction::Down,
            Direction::Down => Direction::Left,
            Direction::Left => Direction::Up,
        }
    }

    /// The prod path of the same-tick double-turn bug: two quick inputs are
    /// stamped with client ticks the committed state has already passed (they
    /// arrived later than the committed-lag window), so `process_command`
    /// rebases both onto the same tick via `max(client_tick, current_tick)`.
    /// The engine must not execute both before one movement step: the second
    /// turn is deferred one tick, so the player's two-step maneuver completes
    /// without ever reversing the snake.
    #[test]
    fn rebased_turns_on_same_tick_defer_instead_of_reversing() {
        let mut state = GameState::new(30, 30, GameType::Solo, QueueMode::Quickmatch, None, 0);
        state.add_player(1, None).expect("add player");
        let snake_id = state.players[&1].snake_id;
        let tick_ms = state.properties.tick_duration_ms as i64;
        let mut engine = GameEngine::new_from_state(1, state);

        // Advance the committed state past a few ticks (run_until lags the
        // wall-clock target by the 500 ms committed-lag window).
        engine.run_until(tick_ms * 10).expect("run_until");
        let committed_tick = engine.current_tick();
        assert!(committed_tick >= 2, "committed state should have advanced");

        let snake = &engine.committed_state().arena.snakes[snake_id as usize];
        let travel = snake.direction;
        let first_turn = clockwise(travel);
        let second_turn = clockwise(first_turn); // opposite of `travel`
        let length_before = snake.length();

        // Client ticks 1 and 2 are already in the committed past: both
        // commands get rebased onto `committed_tick`.
        for (client_tick, direction) in [(1, first_turn), (2, second_turn)] {
            let scheduled = engine
                .process_command(GameCommandMessage {
                    command_id_client: CommandId {
                        tick: client_tick,
                        user_id: 1,
                        sequence_number: 0,
                    },
                    command_id_server: None,
                    command: GameCommand::Turn {
                        snake_id,
                        direction,
                    },
                })
                .expect("process_command");
            // The premise of this test: rebasing collapses both commands
            // onto the same tick. If scheduling ever changes to spread
            // them out, this test is no longer exercising the deferral.
            assert_eq!(
                scheduled.command_id_server.expect("server id").tick,
                committed_tick,
                "rebasing must collapse the command onto the current tick"
            );
        }

        // Advance a few more ticks so the rebased pair executes (first turn
        // at `committed_tick`, deferred second turn one tick later).
        engine.run_until(tick_ms * 13).expect("run_until");
        assert!(engine.current_tick() > committed_tick + 1);

        let snake = &engine.committed_state().arena.snakes[snake_id as usize];
        assert!(
            snake.is_alive,
            "snake must survive two turns rebased onto one tick"
        );
        assert_eq!(
            snake.direction, second_turn,
            "the deferred second turn must apply on the following tick"
        );
        assert_eq!(
            snake.length(),
            length_before,
            "the maneuver must not corrupt the body geometry"
        );
    }
}
