use crate::{
    BoostLifecycleTransition, CommandId, DEFAULT_TICK_INTERVAL_MS, GameCommand, GameCommandMessage,
    GameEvent, GameEventMessage, GameState, GameType, QueueMode,
};
use anyhow::Result;
use serde::Serialize;

/// How far past its last authoritative anchor the predicted state may free-run,
/// on top of the committed-state lag. Once the committed state stops advancing
/// (server silent, stream dead), prediction freezes after this window instead
/// of simulating a ghost game indefinitely.
pub const MAX_PREDICTION_AHEAD_MS: u32 = 1000;

/// Client snapshot admission is strict for every live state and every current
/// snapshot. The sole compatibility exception is immutable history from the
/// immediately preceding gameplay generations after deserialization defaults.
fn validate_client_snapshot_state(game_state: &GameState) -> Result<()> {
    match game_state.validate_boost_invariants() {
        Ok(()) => Ok(()),
        Err(_strict_error) if game_state.is_legacy_completed_snapshot() => Ok(()),
        Err(strict_error) => Err(strict_error),
    }
}

/// Client-side synchronization health, updated on every processed server
/// message. Exposed to the UI so it can detect divergence (hash mismatches),
/// message loss (stream gaps), and trigger a resync instead of drifting.
#[derive(Debug, Clone, Default, Serialize)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct SyncStatus {
    /// Last transport sequence seen (0 = none yet).
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub last_stream_seq: u64,
    /// Number of distinct gap incidents observed.
    pub stream_gap_count: u32,
    /// Total messages known to have been missed.
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub missed_messages: u64,
    /// Stale/duplicate messages skipped instead of double-applied.
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub stale_messages_skipped: u64,
    /// Tick of the last server fingerprint probe processed.
    pub last_probe_tick: Option<u32>,
    /// Whether the last probe matched our committed state.
    pub last_probe_matched: Option<bool>,
    pub consecutive_hash_mismatches: u32,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub total_probes: u64,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub total_mismatches: u64,
    /// First tick at which a hash mismatch was observed (for RCA).
    pub first_mismatch_tick: Option<u32>,
    /// Set when a gap or repeated mismatch means the client should request a
    /// fresh snapshot. Cleared automatically when a snapshot is applied.
    pub needs_resync: bool,
    /// Highest tick seen in any server message (liveness reference).
    pub last_server_tick: u32,
    /// Server wall-clock from the last TickHash heartbeat (clock reference).
    #[cfg_attr(feature = "ts-gen", ts(type = "number | null"))]
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
    /// Monotonic sequence for locally-issued commands. Makes every
    /// `command_id_client` unique (tombstoning is keyed by it) and orders
    /// same-tick local commands; the engine's one-turn-per-snake-per-tick
    /// deferral turns that order into tick spacing at execution time.
    local_command_seq: u32,
    /// Locally predicted commands that do not yet have an authoritative
    /// scheduled/rejected outcome. They live outside committed state so a
    /// rejection can always rebuild cleanly even if its server tick is later
    /// than the speculative execution tick.
    speculative_commands: Vec<GameCommandMessage>,
    sync_status: SyncStatus,
    /// Authoritative input changed since the last prediction replay. A replay
    /// must happen even when wall-clock time has not crossed another tick so
    /// visual prediction (including crash cues) retracts in the same frame.
    prediction_needs_rebuild: bool,
}

impl GameEngine {
    pub fn new(game_id: u32, start_ms: i64) -> Self {
        GameEngine {
            game_id,
            committed_state: GameState::new(
                60,
                40,
                GameType::TeamMatch { per_team: 1 },
                QueueMode::Quickmatch,
                None,
                start_ms,
            ),
            predicted_state: Some(GameState::new(
                60,
                40,
                GameType::TeamMatch { per_team: 1 },
                QueueMode::Quickmatch,
                None,
                start_ms,
            )),
            event_log: Vec::new(),
            committed_state_lag_ms: 500,
            local_player_id: None,
            command_counter: 0,
            local_command_seq: 0,
            speculative_commands: Vec::new(),
            sync_status: SyncStatus::default(),
            prediction_needs_rebuild: false,
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
            GameType::TeamMatch { .. } => (60, 40, DEFAULT_TICK_INTERVAL_MS),
            _ => (40, 40, DEFAULT_TICK_INTERVAL_MS),
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
            local_command_seq: 0,
            speculative_commands: Vec::new(),
            sync_status: SyncStatus::default(),
            prediction_needs_rebuild: false,
        }
    }

    pub fn new_from_state(game_id: u32, game_state: GameState) -> Self {
        Self::try_new_from_state(game_id, game_state)
            .expect("restored game state must satisfy gameplay invariants")
    }

    pub fn try_new_from_state(game_id: u32, game_state: GameState) -> Result<Self> {
        Self::try_new_from_state_with_command_counter(game_id, game_state, 0)
    }

    /// Restore a state received through a client snapshot boundary. Unlike
    /// authoritative recovery, this permits the narrow immutable legacy shape
    /// recognized by `validate_client_snapshot_state` so completed pre-Boost
    /// results remain viewable.
    pub fn try_new_from_snapshot_state(game_id: u32, game_state: GameState) -> Result<Self> {
        validate_client_snapshot_state(&game_state)?;
        Ok(Self::from_validated_state(game_id, game_state, 0))
    }

    /// Restore an authoritative engine without reusing server command IDs.
    /// `next_command_sequence` is part of the v2 recovery envelope.
    pub fn new_from_state_with_command_counter(
        game_id: u32,
        game_state: GameState,
        next_command_sequence: u32,
    ) -> Self {
        Self::try_new_from_state_with_command_counter(game_id, game_state, next_command_sequence)
            .expect("recovered game state must satisfy gameplay invariants")
    }

    pub fn try_new_from_state_with_command_counter(
        game_id: u32,
        game_state: GameState,
        next_command_sequence: u32,
    ) -> Result<Self> {
        game_state.validate_boost_invariants()?;
        Ok(Self::from_validated_state(
            game_id,
            game_state,
            next_command_sequence,
        ))
    }

    fn from_validated_state(
        game_id: u32,
        game_state: GameState,
        next_command_sequence: u32,
    ) -> Self {
        let mut predicted_state = game_state.clone();
        predicted_state.rng = None; // Remove RNG so client doesn't generate food

        GameEngine {
            game_id,
            committed_state: game_state,
            predicted_state: Some(predicted_state),
            event_log: Vec::new(),
            committed_state_lag_ms: 500,
            local_player_id: None,
            command_counter: next_command_sequence,
            local_command_seq: 0,
            speculative_commands: Vec::new(),
            sync_status: SyncStatus::default(),
            prediction_needs_rebuild: false,
        }
    }

    pub fn next_server_command_sequence(&self) -> u32 {
        self.command_counter
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

    /// Apply a pre-match readiness transition to the authoritative committed
    /// state. The owning executor calls this as the local half of the event it
    /// is about to publish, so its own state and every replica converge on one
    /// implementation of the transition.
    ///
    /// Deliberately narrow: readiness is the only state the executor mutates
    /// outside the simulation, and it can only do so before tick 1. Anything
    /// else must go through `run_until`.
    pub fn apply_pre_match_readiness_event(&mut self, event: GameEvent) -> Result<()> {
        if !matches!(
            event,
            GameEvent::PlayerReady { .. } | GameEvent::MatchStartScheduled { .. }
        ) {
            return Err(anyhow::anyhow!(
                "only readiness transitions may be applied outside the simulation"
            ));
        }
        if self.committed_state.current_tick() != 0 {
            return Err(anyhow::anyhow!(
                "the readiness gate cannot be touched after the simulation has started"
            ));
        }

        self.committed_state.apply_event(event, None);
        // The epoch this state starts from may have just changed, so any
        // prediction built against the old one is stale.
        self.prediction_needs_rebuild = true;
        Ok(())
    }

    /// Process a local command with client-side prediction
    pub fn process_local_command(&mut self, command: GameCommand) -> Result<GameCommandMessage> {
        let Some(player_id) = self.local_player_id else {
            return Err(anyhow::anyhow!("Local player ID not set"));
        };

        let predicted_tick = self
            .predicted_state
            .as_ref()
            .map(|s| s.current_tick())
            .unwrap_or(0);

        // Stamp the command at the current predicted tick with a monotonic
        // local sequence number. Rapid inputs may share a tick: the sequence
        // keeps their ids unique and their order well-defined, and the
        // engine's one-turn-per-snake-per-tick deferral spreads them across
        // ticks at execution time — the same rule the server applies, so
        // prediction and the confirmed schedule stay aligned. (This replaced
        // a stateful tick ratchet whose `last_command_tick` a clock spike
        // could poison.)
        self.local_command_seq += 1;
        let command_message = GameCommandMessage {
            command_id_client: CommandId {
                tick: predicted_tick,
                user_id: player_id,
                sequence_number: self.local_command_seq,
            },
            command_id_server: None,
            command,
        };

        self.speculative_commands.push(command_message.clone());
        if let Some(predicted_state) = &mut self.predicted_state {
            predicted_state.schedule_command(&command_message);
        }

        Ok(command_message)
    }

    /// Retract a command that never made it into the browser's durable
    /// delivery outbox. This is deliberately separate from an authoritative
    /// rejection: no server outcome exists yet, but prediction must still be
    /// rebuilt so a failed local enqueue cannot manufacture player activity.
    pub fn discard_local_command(&mut self, command_id_client: &CommandId) -> bool {
        let before = self.speculative_commands.len();
        self.speculative_commands
            .retain(|command| &command.command_id_client != command_id_client);
        let removed = self.speculative_commands.len() != before;
        // Even an already-absent identity should re-anchor prediction. The
        // browser calls this only after delivery admission fails, and the safe
        // fallback is to rebuild from committed state plus whatever commands
        // remain durably queued.
        self.prediction_needs_rebuild = true;
        removed
    }

    /// Process a server event and reconcile with local predictions
    pub fn process_server_event(&mut self, event_message: &GameEventMessage) -> Result<()> {
        let is_snapshot = matches!(&event_message.event, GameEvent::Snapshot { .. });
        if let GameEvent::Snapshot { game_state } = &event_message.event {
            if game_state.tick != event_message.tick {
                self.sync_status.needs_resync = true;
                return Err(anyhow::anyhow!(
                    "snapshot envelope tick {} does not match state tick {}",
                    event_message.tick,
                    game_state.tick
                ));
            }
            if let Err(error) = validate_client_snapshot_state(game_state) {
                self.sync_status.needs_resync = true;
                return Err(error);
            }
        }

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

        // Advance and apply against a candidate so malformed deltas cannot
        // leave movement/cooldown catch-up committed without their event (or
        // mutate only one half of a Boost collection). A rejected candidate is
        // discarded and the transport is told to request a fresh snapshot.
        let mut candidate = match &event_message.event {
            GameEvent::Snapshot { game_state } => game_state.clone(),
            _ => self.committed_state.clone(),
        };
        if !is_snapshot {
            while candidate.current_tick() < event_message.tick {
                if let Err(error) = candidate.tick_forward(true) {
                    self.sync_status.needs_resync = true;
                    return Err(error);
                }
            }
        }

        // Fingerprint probes compare instead of mutate.
        if let GameEvent::TickHash {
            hash: server_hash,
            server_ts_ms,
        } = &event_message.event
        {
            let local_hash = candidate.sync_hash();
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
            self.committed_state = candidate;
            return Ok(());
        }

        if !is_snapshot
            && let Err(error) = candidate.try_apply_replicated_event(event_message.event.clone())
        {
            self.sync_status.needs_resync = true;
            return Err(error);
        }
        self.committed_state = candidate;

        match &event_message.event {
            GameEvent::CommandScheduled { command_message }
            | GameEvent::CommandScheduledV2 {
                command_message, ..
            } => {
                self.speculative_commands.retain(|speculative| {
                    speculative.command_id_client != command_message.command_id_client
                });
            }
            GameEvent::CommandRejected {
                command_id_client: Some(command_id_client),
                session_rejected_from,
                ..
            } => {
                self.speculative_commands
                    .retain(|speculative| speculative.command_id_client != *command_id_client);
                // A session-wide fence can cover commands whose v2 identity
                // is known only to the transport outbox. Force snapshot repair
                // after retracting the exact decoded command.
                if session_rejected_from.is_some() {
                    self.sync_status.needs_resync = true;
                }
            }
            _ => {}
        }
        self.prediction_needs_rebuild = true;

        if is_snapshot {
            // Fresh authoritative state: divergence bookkeeping starts over.
            // A snapshot is a complete state anchor. Retaining pre-snapshot
            // speculative commands could replay an input that the snapshot
            // already reflects (or that executed before it), especially a
            // turn. The transport outbox still owns retrying any genuinely
            // unacknowledged command after the anchor.
            self.speculative_commands.clear();
            self.sync_status.needs_resync = false;
            self.sync_status.consecutive_hash_mismatches = 0;
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
        // Handle pre-start case: if current time is before start time, don't
        // advance. `simulation_start_ms` returns None while the pre-match
        // readiness gate holds the match, which parks prediction here exactly
        // as a not-yet-reached start time does.
        let elapsed_ms = match self.committed_state.simulation_start_ms() {
            Some(epoch) => current_ts - epoch,
            None => -1,
        };
        if elapsed_ms < 0 {
            if self.prediction_needs_rebuild {
                let mut new_predicted_state = self.committed_state.clone();
                new_predicted_state.rng = None;
                self.predicted_state = Some(new_predicted_state);
                self.prediction_needs_rebuild = false;
            }
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
            self.prediction_needs_rebuild = false;
            return Ok(());
        }

        // Check if we need to rebuild by comparing with existing predicted state
        let needs_rebuild = self.prediction_needs_rebuild
            || self
                .predicted_state
                .as_ref()
                .is_none_or(|state| predicted_target_tick > state.current_tick());

        if needs_rebuild {
            // Clone committed state
            let mut new_predicted_state = self.committed_state.clone();

            // Remove RNG from predicted state so it doesn't generate food locally
            new_predicted_state.rng = None;
            for command in &self.speculative_commands {
                new_predicted_state.schedule_command(command);
            }

            // Advance to target tick (stops if game completes)
            while !new_predicted_state.is_complete()
                && new_predicted_state.current_tick() < predicted_target_tick
            {
                new_predicted_state.tick_forward(false)?;
            }

            self.predicted_state = Some(new_predicted_state);
            self.prediction_needs_rebuild = false;
        }

        Ok(())
    }

    /// Run the required amount of ticks so that the game is at the given timestamp.
    /// Can be called from a very fast interval loop or requestAnimationFrame.
    pub fn run_until(&mut self, ts_ms: i64) -> Result<Vec<(u32, u64, GameEvent)>> {
        self.run_until_observing_boost(ts_ms, &mut |_| {})
    }

    /// Advance committed simulation while reporting exact Boost lifecycle
    /// transitions from each authoritative quantum. Predicted-state replay is
    /// intentionally excluded from the observer.
    pub fn run_until_observing_boost(
        &mut self,
        ts_ms: i64,
        observer: &mut impl FnMut(BoostLifecycleTransition),
    ) -> Result<Vec<(u32, u64, GameEvent)>> {
        let tick_duration_ms = self.committed_state.properties.tick_duration_ms;

        // Handle pre-start case: if current time is before start time, don't
        // advance. A match still held by the readiness gate has no simulation
        // epoch at all and is treated identically — no ticks, no events — on
        // the authoritative executor and in every client engine alike.
        let Some(simulation_epoch_ms) = self.committed_state.simulation_start_ms() else {
            return Ok(Vec::new());
        };
        let elapsed_ms = ts_ms - simulation_epoch_ms;
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
            let events = self
                .committed_state
                .tick_forward_observing_boost(false, observer)?;
            // Label events with the POST-step tick: an event produced during
            // the step N -> N+1 describes the state at N+1. Receivers
            // fast-forward their committed state to the event's tick before
            // applying, so a pre-step label would make them apply movement
            // effects (FoodEaten, SnakeDied, ...) one movement-step early —
            // e.g. growing the snake a tick before the server does, forking
            // the body geometry permanently.
            let post_tick = self.committed_state.current_tick();
            for (sequence, event) in events {
                // The engine's initial snapshot is deliberately captured
                // before the first 0 -> 1 simulation step. Its envelope must
                // carry that same internal tick; all mutation events describe
                // the post-step state and keep the post-step label.
                let event_tick = match &event {
                    GameEvent::Snapshot { game_state } => game_state.tick,
                    _ => post_tick,
                };
                out.push((event_tick, sequence, event));
            }
        }

        // Run predicted state to current time (not lagged), bounded by the
        // prediction cap relative to the just-advanced committed state.
        if self.prediction_needs_rebuild {
            self.rebuild_predicted_state(ts_ms)?;
        }
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

    /// Milliseconds the committed state trails the wall-clock target after the
    /// engine's intentional committed-state lag window is applied. A terminal
    /// or not-yet-started game has no scheduler lag.
    pub fn authoritative_scheduler_lag_ms(&self, ts_ms: i64) -> u64 {
        // A match still held by the readiness gate has no simulation epoch, so
        // it is "not yet started" and by definition cannot be running late.
        let Some(simulation_epoch_ms) = self.committed_state.simulation_start_ms() else {
            return 0;
        };
        if self.committed_state.is_complete() || ts_ms < simulation_epoch_ms {
            return 0;
        }

        let tick_duration_ms = self.committed_state.properties.tick_duration_ms.max(1);
        let elapsed_ms = ts_ms.saturating_sub(simulation_epoch_ms);
        let wallclock_target_tick =
            u32::try_from(elapsed_ms / i64::from(tick_duration_ms)).unwrap_or(u32::MAX);
        let lagged_target_tick =
            wallclock_target_tick.saturating_sub(self.committed_state_lag_ms / tick_duration_ms);
        u64::from(lagged_target_tick.saturating_sub(self.committed_state.current_tick()))
            * u64::from(tick_duration_ms)
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

    /// Replays an already-decided authoritative command after crash recovery.
    /// The server ID and scheduled tick are preserved exactly; recomputing them
    /// from the successor's older checkpoint would make the same client command
    /// resolve differently after failover.
    pub fn replay_scheduled_command(
        &mut self,
        command_message: GameCommandMessage,
        scheduled_at_tick: u32,
    ) -> Result<()> {
        let server_id = command_message
            .command_id_server
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("replayed server command has no server ID"))?;
        if server_id.sequence_number != self.command_counter {
            return Err(anyhow::anyhow!(
                "replayed server command sequence {} does not match next sequence {}",
                server_id.sequence_number,
                self.command_counter
            ));
        }
        if server_id.user_id != command_message.command_id_client.user_id {
            return Err(anyhow::anyhow!(
                "replayed server command user identity does not match"
            ));
        }
        if server_id.tick < self.committed_state.current_tick() {
            return Err(anyhow::anyhow!(
                "replayed server command tick {} predates checkpoint tick {}",
                server_id.tick,
                self.committed_state.current_tick()
            ));
        }
        if scheduled_at_tick < self.committed_state.current_tick()
            || scheduled_at_tick > server_id.tick
        {
            return Err(anyhow::anyhow!(
                "replayed command activity tick {} must be between checkpoint tick {} and scheduled tick {}",
                scheduled_at_tick,
                self.committed_state.current_tick(),
                server_id.tick
            ));
        }

        // Replay the old checkpoint up to the instant at which the original
        // actor accepted this command before recording its activity. Writing a
        // future activity tick into the checkpoint first would let a later
        // input retroactively protect the player from a deadline crossed
        // during catch-up.
        while self.committed_state.current_tick() < scheduled_at_tick {
            if self.committed_state.is_complete() {
                return Err(anyhow::anyhow!(
                    "replayed command was accepted after the recovered game completed"
                ));
            }
            self.committed_state.tick_forward(false)?;
        }

        // Recovery has no speculative local input. Re-anchor prediction to the
        // exact recovered state after each temporal catch-up so queued commands
        // and inactivity metadata cannot diverge inside the server engine.
        let mut recovered_predicted_state = self.committed_state.clone();
        recovered_predicted_state.rng = None;
        self.predicted_state = Some(recovered_predicted_state);

        self.command_counter = self
            .command_counter
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("server command sequence overflow"))?;
        self.committed_state.schedule_command(&command_message);
        if let Some(predicted_state) = &mut self.predicted_state {
            predicted_state.schedule_command(&command_message);
        }
        Ok(())
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
    use crate::{
        BOOST_TICK_INTERVAL_MS, ClientCommandIdentityV2, DEFAULT_BOOST_SPEED_MILLI, Direction,
        GameStatus, NORMAL_SNAKE_SPEED_MILLI, Position,
    };

    fn engine_with_imminent_wall_crash() -> (GameEngine, u32, i64) {
        let mut state = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, None, 0);
        state.add_player(1, None).expect("add player");
        let snake_id = state.players[&1].snake_id;
        let snake = &mut state.arena.snakes[snake_id as usize];
        snake.body = vec![Position { x: 1, y: 5 }, Position { x: 4, y: 5 }];
        snake.direction = Direction::Left;
        snake.is_alive = true;
        snake.food = 0;

        let tick_ms = state.properties.tick_duration_ms as i64;
        (GameEngine::new_from_state(1, state), snake_id, tick_ms)
    }

    /// A team snake one movement step outside its own goal mouth, carrying two
    /// points' worth of food.
    fn engine_with_imminent_goal() -> (GameEngine, i64) {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        state.add_player(1, None).expect("add player 1");
        state.add_player(2, None).expect("add player 2");
        let snake = &mut state.arena.snakes[0];
        snake.body = vec![Position { x: 10, y: 18 }, Position { x: 13, y: 18 }];
        snake.direction = Direction::Left;
        snake.is_alive = true;
        snake.food = 4;

        let tick_ms = state.properties.tick_duration_ms as i64;
        (GameEngine::new_from_state(1, state), tick_ms)
    }

    /// Score celebrations run off prediction, so the cue has to be visible in
    /// predicted state while the committed state still trails behind it.
    #[test]
    fn prediction_exposes_the_goal_cue_before_committed_state_reaches_it() {
        let (mut engine, tick_ms) = engine_with_imminent_goal();

        engine
            .rebuild_predicted_state(tick_ms * 4)
            .expect("prediction rebuild");

        let predicted = engine.predicted_state().expect("predicted state");
        assert_eq!(
            predicted.recent_goals.len(),
            1,
            "prediction must surface the goal it simulated"
        );
        let cue = &predicted.recent_goals[0];
        assert_eq!(cue.tick, 2);
        assert_eq!(cue.snake_id, 0);
        assert_eq!(cue.team_id.0, 0);
        assert_eq!(cue.points, 2);
        assert_eq!(cue.position, Position { x: 9, y: 18 });

        assert_eq!(engine.committed_state().current_tick(), 0);
        assert!(
            engine.committed_state().recent_goals.is_empty(),
            "prediction must expose the goal before committed state reaches it"
        );
    }

    /// The composite the cue's placement in the movement path exists for.
    ///
    /// A client's committed state advances under `movement_only`, which skips
    /// the authoritative scoring block entirely — it receives the score and
    /// respawn from the transport instead. Prediction is rebuilt from that
    /// committed state every frame, so if the cue were only produced under the
    /// scoring gate it would vanish from prediction the moment committed state
    /// passed the goal tick, retracting a celebration of a real goal. Emitting
    /// it during movement keeps it visible on both sides of the catch-up.
    #[test]
    fn a_real_goal_cue_survives_committed_catch_up_past_the_goal_tick() {
        let (mut engine, tick_ms) = engine_with_imminent_goal();

        engine
            .rebuild_predicted_state(tick_ms * 4)
            .expect("prediction rebuild");
        let predicted_cue = engine
            .predicted_state()
            .expect("predicted state")
            .recent_goals
            .first()
            .cloned()
            .expect("test setup must first predict the goal");

        // Drive committed state past the goal tick exactly as the transport
        // does: `process_server_event` fast-forwards with `tick_forward(true)`
        // before applying each authoritative event.
        engine
            .process_server_event(&GameEventMessage {
                game_id: 1,
                tick: 6,
                sequence: 1,
                stream_seq: 1,
                user_id: None,
                event: GameEvent::TeamScoreUpdated {
                    team_id: predicted_cue.team_id,
                    score: predicted_cue.points,
                },
            })
            .expect("authoritative score");

        let committed = engine.committed_state();
        assert!(
            committed.current_tick() > predicted_cue.tick,
            "committed state must have advanced past the goal tick"
        );
        assert_eq!(
            committed.recent_goals,
            vec![predicted_cue.clone()],
            "movement-only catch-up must retain the cue the server also recorded"
        );

        engine
            .rebuild_predicted_state(tick_ms * 8)
            .expect("post-catch-up prediction rebuild");
        assert!(
            engine
                .predicted_state()
                .expect("predicted state")
                .recent_goals
                .contains(&predicted_cue),
            "a rebuild from the caught-up committed state must not retract a real goal"
        );
    }

    /// The mirror of the crash-cue retraction: if authoritative input turns the
    /// snake away from its goal, the replayed prediction must drop the cue so
    /// the celebration can be shut off instead of finishing on a phantom goal.
    #[test]
    fn same_target_reconciliation_retracts_an_invalid_goal_cue() {
        let (mut engine, tick_ms) = engine_with_imminent_goal();
        let target_ts = tick_ms * 4;

        engine
            .rebuild_predicted_state(target_ts)
            .expect("initial prediction rebuild");
        assert_eq!(
            engine
                .predicted_state()
                .expect("predicted state")
                .recent_goals
                .len(),
            1,
            "test setup must first predict the goal"
        );

        engine
            .process_server_event(&GameEventMessage {
                game_id: 1,
                tick: 0,
                sequence: 1,
                stream_seq: 1,
                user_id: Some(1),
                event: GameEvent::SnakeTurned {
                    snake_id: 0,
                    direction: Direction::Up,
                },
            })
            .expect("authoritative turn");

        engine
            .rebuild_predicted_state(target_ts)
            .expect("same-target reconciliation rebuild");

        assert!(
            engine
                .predicted_state()
                .expect("reconciled prediction")
                .recent_goals
                .is_empty(),
            "same-target replay must retract the invalid predicted goal cue"
        );
    }

    fn clockwise(direction: Direction) -> Direction {
        match direction {
            Direction::Up => Direction::Right,
            Direction::Right => Direction::Down,
            Direction::Down => Direction::Left,
            Direction::Left => Direction::Up,
        }
    }

    /// Local commands are stamped at the current predicted tick with a
    /// monotonic sequence number: rapid inputs share a tick but never share
    /// an id (tombstoning is keyed by the client id) and stay strictly
    /// ordered. This replaced the stateful tick ratchet.
    #[test]
    fn local_commands_share_tick_with_unique_sequence_numbers() {
        let mut state = GameState::new(30, 30, GameType::Solo, QueueMode::Quickmatch, None, 0);
        state.add_player(1, None).expect("add player");
        let snake_id = state.players[&1].snake_id;
        let tick_ms = state.properties.tick_duration_ms as i64;
        let mut engine = GameEngine::new_from_state(1, state);
        engine.set_local_player_id(1);

        // Advance prediction so commands are stamped mid-game.
        engine
            .rebuild_predicted_state(tick_ms * 5)
            .expect("rebuild");
        let predicted_tick = engine.get_predicted_tick();
        assert!(predicted_tick > 0, "prediction should have advanced");

        let cmd1 = engine
            .process_local_command(GameCommand::Turn {
                snake_id,
                direction: Direction::Up,
            })
            .expect("command 1");
        let cmd2 = engine
            .process_local_command(GameCommand::Turn {
                snake_id,
                direction: Direction::Down,
            })
            .expect("command 2");

        assert_eq!(cmd1.command_id_client.tick, predicted_tick);
        assert_eq!(cmd2.command_id_client.tick, predicted_tick);
        assert!(
            cmd2.command_id_client.sequence_number > cmd1.command_id_client.sequence_number,
            "local sequence must be strictly increasing"
        );
        assert_ne!(
            cmd1.command_id_client, cmd2.command_id_client,
            "same-tick local commands must have distinct ids"
        );
    }

    #[test]
    fn recovery_replay_preserves_the_authoritative_activity_tick() {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        let snake_id = state.add_player(7, None).unwrap().snake_id;
        state.add_player(8, None).unwrap();
        state.tick = 5;

        let mut engine = GameEngine::try_new_from_state_with_command_counter(99, state, 3)
            .expect("restore checkpoint");
        let recovered_command = GameCommandMessage {
            command_id_client: CommandId {
                tick: 20,
                user_id: 7,
                sequence_number: 4,
            },
            command_id_server: Some(CommandId {
                tick: 20,
                user_id: 7,
                sequence_number: 3,
            }),
            command: GameCommand::PlayerActivity { snake_id },
        };

        engine
            .replay_scheduled_command(recovered_command, 12)
            .expect("replay authoritative decision");

        assert_eq!(
            engine.committed_state().player_last_activity_ticks.get(&7),
            Some(&12),
            "recovery must retain when the server originally received the command"
        );
        assert_eq!(
            engine
                .predicted_state()
                .unwrap()
                .player_last_activity_ticks
                .get(&7),
            Some(&12),
            "committed and predicted recovery states must share the same activity anchor"
        );
        assert_eq!(engine.next_server_command_sequence(), 4);
    }

    #[test]
    fn started_recovery_replay_cannot_write_future_activity_or_revive_an_expired_player() {
        fn checkpoint_near_deadline() -> (GameState, u32, u32) {
            let mut state = GameState::new(
                60,
                40,
                GameType::TeamMatch { per_team: 1 },
                QueueMode::Quickmatch,
                None,
                0,
            );
            let idle_snake_id = state.add_player(7, None).unwrap().snake_id;
            let active_snake_id = state.add_player(8, None).unwrap().snake_id;
            state.status = GameStatus::Started { server_id: 1 };
            state.rng = None;
            state.properties.available_food_target = 0;

            let timeout_ticks =
                state.properties.player_idle_timeout_ms / state.properties.tick_duration_ms;
            state.tick = timeout_ticks - 2;
            state.player_last_activity_ticks.insert(7, 0);
            state.player_last_activity_ticks.insert(8, state.tick);
            state.validate_boost_invariants().unwrap();
            (state, idle_snake_id, active_snake_id)
        }

        fn activity_command(
            snake_id: u32,
            server_tick: u32,
            sequence_number: u32,
        ) -> GameCommandMessage {
            GameCommandMessage {
                command_id_client: CommandId {
                    tick: server_tick,
                    user_id: 7,
                    sequence_number: 1,
                },
                command_id_server: Some(CommandId {
                    tick: server_tick,
                    user_id: 7,
                    sequence_number,
                }),
                command: GameCommand::PlayerActivity { snake_id },
            }
        }

        let (near_state, idle_snake_id, _) = checkpoint_near_deadline();
        let deadline_tick = near_state.tick + 2;
        let accepted_tick = deadline_tick - 1;
        let mut accepted = GameEngine::try_new_from_state_with_command_counter(99, near_state, 0)
            .expect("restore live checkpoint");
        accepted
            .replay_scheduled_command(
                activity_command(idle_snake_id, deadline_tick + 5, 0),
                accepted_tick,
            )
            .expect("replay activity accepted just before expiry");

        let accepted_state = accepted.committed_state();
        assert_eq!(accepted_state.tick, accepted_tick);
        assert_eq!(
            accepted_state.player_last_activity_ticks.get(&7),
            Some(&accepted_tick)
        );
        assert!(!accepted_state.is_player_idle_kicked(7));
        accepted_state.validate_boost_invariants().unwrap();

        let (crossing_state, idle_snake_id, active_snake_id) = checkpoint_near_deadline();
        let mut crossing =
            GameEngine::try_new_from_state_with_command_counter(100, crossing_state, 0)
                .expect("restore expiring checkpoint");
        crossing
            .replay_scheduled_command(
                activity_command(idle_snake_id, deadline_tick + 5, 0),
                deadline_tick,
            )
            .expect("replay must retain terminal catch-up state");

        let crossing_state = crossing.committed_state();
        assert_eq!(crossing_state.tick, deadline_tick);
        assert_eq!(
            crossing_state.player_last_activity_ticks.get(&7),
            Some(&0),
            "a decision observed after expiry must not write activity into the future"
        );
        assert!(crossing_state.is_player_idle_kicked(7));
        assert!(!crossing_state.arena.snakes[idle_snake_id as usize].is_alive);
        assert!(matches!(
            crossing_state.status,
            GameStatus::Complete {
                winning_snake_id: Some(winner)
            } if winner == active_snake_id
        ));
        crossing_state.validate_boost_invariants().unwrap();
    }

    #[test]
    fn replicated_idle_kick_is_idempotent_after_client_fast_forward() {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 2 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        for user_id in 1..=4 {
            state.add_player(user_id, None).expect("add team player");
        }
        state.status = GameStatus::Started { server_id: 1 };
        state.rng = None;
        state.properties.available_food_target = 0;
        state.properties.player_idle_timeout_ms = 1_000;
        state.properties.player_idle_warning_ms = 500;
        state.tick = 8;
        state.player_last_activity_ticks.insert(1, 0);
        for user_id in 2..=4 {
            state.player_last_activity_ticks.insert(user_id, 8);
        }
        let snake_id = state.players[&1].snake_id;
        let mut engine = GameEngine::new_from_state(91, state);

        engine
            .process_server_event(&GameEventMessage {
                game_id: 91,
                tick: 10,
                sequence: 1,
                stream_seq: 1,
                user_id: None,
                event: GameEvent::PlayerIdleKicked {
                    user_id: 1,
                    snake_id,
                },
            })
            .expect("exact already-derived kick must be accepted");

        let committed = engine.committed_state();
        assert_eq!(committed.idle_kicked_user_ids, vec![1]);
        assert!(!committed.arena.snakes[snake_id as usize].is_alive);
        assert!(matches!(committed.status, GameStatus::Started { .. }));
        assert!(!engine.sync_status().needs_resync);
        committed.validate_boost_invariants().unwrap();
    }

    #[test]
    fn rejected_predicted_boost_rebuilds_from_unmodified_committed_state() {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        let snake_id = state.add_player(7, None).unwrap().snake_id;
        state.arena.snakes[snake_id as usize].boost.charge_ms = 1_000;
        let mut engine = GameEngine::new_from_state(99, state);
        engine.set_local_player_id(7);

        let local = engine
            .process_local_command(GameCommand::ActivateBoost { snake_id })
            .unwrap();
        engine.rebuild_predicted_state(50).unwrap();
        let predicted = &engine.predicted_state().unwrap().arena.snakes[snake_id as usize];
        assert!(predicted.boost.active);
        assert_eq!(predicted.speed_milli, DEFAULT_BOOST_SPEED_MILLI);

        let committed = &engine.committed_state().arena.snakes[snake_id as usize];
        assert!(!committed.boost.active);
        assert_eq!(committed.speed_milli, NORMAL_SNAKE_SPEED_MILLI);

        engine
            .process_server_event(&GameEventMessage {
                game_id: 99,
                tick: 1,
                sequence: 0,
                stream_seq: 1,
                user_id: None,
                event: GameEvent::CommandRejected {
                    command_id: ClientCommandIdentityV2 {
                        game_id: 99,
                        user_id: 7,
                        client_game_session_id: "session".into(),
                        sequence: 1,
                    },
                    reason: "rejected for test".into(),
                    command_id_client: Some(local.command_id_client),
                    session_rejected_from: None,
                },
            })
            .unwrap();
        engine.rebuild_predicted_state(50).unwrap();

        let predicted = &engine.predicted_state().unwrap().arena.snakes[snake_id as usize];
        assert!(!predicted.boost.active);
        assert_eq!(predicted.boost.charge_ms, 1_000);
        assert_eq!(predicted.speed_milli, NORMAL_SNAKE_SPEED_MILLI);
    }

    #[test]
    fn predicted_boost_release_stops_immediately_without_spending_charge() {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        let snake_id = state.add_player(7, None).unwrap().snake_id;
        let snake = &mut state.arena.snakes[snake_id as usize];
        snake.boost.charge_ms = 1_000;
        snake.boost.active = true;
        snake.speed_milli = DEFAULT_BOOST_SPEED_MILLI;
        let mut engine = GameEngine::new_from_state(99, state);
        engine.set_local_player_id(7);

        engine
            .process_local_command(GameCommand::DeactivateBoost { snake_id })
            .unwrap();
        engine.rebuild_predicted_state(50).unwrap();

        let predicted = &engine.predicted_state().unwrap().arena.snakes[snake_id as usize];
        assert!(!predicted.boost.active);
        assert_eq!(predicted.boost.charge_ms, 1_000);
        assert_eq!(predicted.speed_milli, NORMAL_SNAKE_SPEED_MILLI);

        let committed = &engine.committed_state().arena.snakes[snake_id as usize];
        assert!(committed.boost.active);
        assert_eq!(committed.boost.charge_ms, 1_000);
        assert_eq!(committed.speed_milli, DEFAULT_BOOST_SPEED_MILLI);
    }

    #[test]
    fn snapshot_reanchor_discards_pre_snapshot_boost_prediction() {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        let snake_id = state.add_player(7, None).unwrap().snake_id;
        state.arena.snakes[snake_id as usize].boost.charge_ms = 1_000;
        let mut engine = GameEngine::new_from_state(99, state);
        engine.set_local_player_id(7);

        engine
            .process_local_command(GameCommand::ActivateBoost { snake_id })
            .unwrap();
        engine.rebuild_predicted_state(50).unwrap();
        assert!(
            engine.predicted_state().unwrap().arena.snakes[snake_id as usize]
                .boost
                .active
        );

        let authoritative_anchor = engine.committed_state().clone();
        engine
            .process_server_event(&GameEventMessage {
                game_id: 99,
                tick: authoritative_anchor.current_tick(),
                sequence: 0,
                stream_seq: 1,
                user_id: None,
                event: GameEvent::Snapshot {
                    game_state: authoritative_anchor,
                },
            })
            .unwrap();
        engine.rebuild_predicted_state(50).unwrap();

        let predicted = &engine.predicted_state().unwrap().arena.snakes[snake_id as usize];
        assert!(!predicted.boost.active);
        assert_eq!(predicted.boost.charge_ms, 1_000);
        assert_eq!(predicted.speed_milli, NORMAL_SNAKE_SPEED_MILLI);
    }

    #[test]
    fn malformed_boost_snapshot_is_rejected_before_it_can_run_forever() {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        let snake_id = state.add_player(7, None).unwrap().snake_id;
        let snake = &mut state.arena.snakes[snake_id as usize];
        snake.boost.active = true;
        snake.boost.charge_ms = 25;
        snake.speed_milli = DEFAULT_BOOST_SPEED_MILLI;

        assert!(GameEngine::try_new_from_state(99, state).is_err());
    }

    fn legacy_boostless_snapshot(
        width: u16,
        height: u16,
        game_type: GameType,
        status: GameStatus,
    ) -> GameState {
        let mut state = GameState::new(
            width,
            height,
            game_type.clone(),
            QueueMode::Competitive,
            Some(17),
            0,
        );
        let player_count = match game_type {
            GameType::Solo => 1,
            GameType::TeamMatch { per_team } => u32::from(per_team) * 2,
            GameType::FreeForAll { max_players } => u32::from(max_players),
            GameType::Custom { .. } => unreachable!("custom games are not legacy Boost modes"),
        };
        for user_id in 1..=player_count {
            state
                .add_player(user_id, Some(format!("legacy-{user_id}")))
                .unwrap();
        }
        state.status = status;

        // Recreate durable pre-expansion JSON, then deserialize it through the
        // compatibility defaults exactly as the server's history path does.
        let mut persisted = serde_json::to_value(state).unwrap();
        persisted
            .as_object_mut()
            .unwrap()
            .remove("player_action_counts");
        persisted["properties"]["available_food_target"] =
            serde_json::json!(crate::DEFAULT_FOOD_TARGET);
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

    fn legacy_timed_boost_team_snapshot(status: GameStatus) -> GameState {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Competitive,
            Some(17),
            0,
        );
        state.add_player(7, Some("legacy-blue".into())).unwrap();
        state.add_player(8, Some("legacy-red".into())).unwrap();
        state.status = status;
        state.properties.time_limit_ms = Some(90_000);
        state.properties.score_limit = None;
        state
    }

    #[test]
    fn client_snapshot_admission_is_terminal_and_legacy_shape_only() {
        let legacy_modes = [
            (60, 40, GameType::TeamMatch { per_team: 1 }),
            (40, 40, GameType::FreeForAll { max_players: 4 }),
            (40, 40, GameType::Solo),
        ];
        for (width, height, game_type) in legacy_modes {
            let completed = legacy_boostless_snapshot(
                width,
                height,
                game_type.clone(),
                GameStatus::Complete {
                    winning_snake_id: Some(0),
                },
            );
            assert!(completed.validate_boost_invariants().is_err());
            assert!(GameEngine::try_new_from_state(99, completed.clone()).is_err());
            let restored = GameEngine::try_new_from_snapshot_state(99, completed)
                .expect("boostless completed history must remain viewable");
            assert!(restored.committed_state().is_complete());

            let nonterminal = legacy_boostless_snapshot(
                width,
                height,
                game_type,
                GameStatus::Started { server_id: 1 },
            );
            assert!(GameEngine::try_new_from_snapshot_state(99, nonterminal).is_err());
        }

        let completed = legacy_timed_boost_team_snapshot(GameStatus::Complete {
            winning_snake_id: Some(0),
        });
        assert!(completed.validate_boost_invariants().is_err());
        GameEngine::try_new_from_snapshot_state(99, completed.clone())
            .expect("timed Boost-team history must remain viewable");
        let nonterminal = legacy_timed_boost_team_snapshot(GameStatus::Started { server_id: 1 });
        assert!(GameEngine::try_new_from_snapshot_state(99, nonterminal).is_err());

        let mut wrong_quantum = legacy_boostless_snapshot(
            40,
            40,
            GameType::FreeForAll { max_players: 4 },
            GameStatus::Complete {
                winning_snake_id: Some(0),
            },
        );
        wrong_quantum.properties.tick_duration_ms = BOOST_TICK_INTERVAL_MS;
        assert!(GameEngine::try_new_from_snapshot_state(99, wrong_quantum).is_err());

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
        assert!(GameEngine::try_new_from_snapshot_state(99, malformed_current).is_err());

        let mut live_engine = GameEngine::new(99, 0);
        live_engine
            .process_server_event(&GameEventMessage {
                game_id: 99,
                tick: completed.tick,
                sequence: completed.event_sequence,
                stream_seq: 1,
                user_id: None,
                event: GameEvent::Snapshot {
                    game_state: completed,
                },
            })
            .expect("timed terminal history snapshot must re-anchor a client");
        assert!(live_engine.committed_state().is_complete());
    }

    #[test]
    fn authoritative_scheduler_lag_uses_the_committed_lagged_target() {
        let state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        let mut engine = GameEngine::new_from_state(99, state);

        assert_eq!(engine.authoritative_scheduler_lag_ms(500), 0);
        assert_eq!(engine.authoritative_scheduler_lag_ms(650), 150);
        engine.run_until(650).unwrap();
        assert_eq!(engine.current_tick(), 3);
        assert_eq!(engine.authoritative_scheduler_lag_ms(650), 0);
    }

    #[test]
    fn authoritative_boost_observer_keeps_same_quantum_activation_and_depletion() {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        let snake_id = state.add_player(7, None).unwrap().snake_id;
        let snake = &mut state.arena.snakes[snake_id as usize];
        snake.body = vec![Position { x: 30, y: 20 }, Position { x: 29, y: 20 }];
        snake.direction = Direction::Right;
        snake.boost.charge_ms = 50;
        state.schedule_command(&GameCommandMessage {
            command_id_client: CommandId {
                tick: 0,
                user_id: 7,
                sequence_number: 1,
            },
            command_id_server: Some(CommandId {
                tick: 0,
                user_id: 7,
                sequence_number: 1,
            }),
            command: GameCommand::ActivateBoost { snake_id },
        });
        let mut engine = GameEngine::new_from_state(99, state);
        let mut transitions = Vec::new();

        engine
            .run_until_observing_boost(550, &mut |transition| transitions.push(transition))
            .unwrap();

        assert_eq!(
            transitions,
            vec![
                BoostLifecycleTransition::Activated { snake_id },
                BoostLifecycleTransition::Depleted { snake_id },
            ]
        );
        let snake = &engine.committed_state().arena.snakes[snake_id as usize];
        assert!(!snake.boost.active);
        assert_eq!(snake.boost.charge_ms, 0);
        assert_eq!(snake.speed_milli, NORMAL_SNAKE_SPEED_MILLI);
    }

    #[test]
    fn scheduled_noop_activation_is_not_a_lifecycle_activation() {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        let snake_id = state.add_player(7, None).unwrap().snake_id;
        state.schedule_command(&GameCommandMessage {
            command_id_client: CommandId {
                tick: 0,
                user_id: 7,
                sequence_number: 1,
            },
            command_id_server: Some(CommandId {
                tick: 0,
                user_id: 7,
                sequence_number: 1,
            }),
            command: GameCommand::ActivateBoost { snake_id },
        });
        let mut engine = GameEngine::new_from_state(99, state);
        let mut transitions = Vec::new();

        engine
            .run_until_observing_boost(550, &mut |transition| transitions.push(transition))
            .unwrap();

        assert!(transitions.is_empty());
        let snake = &engine.committed_state().arena.snakes[snake_id as usize];
        assert!(!snake.boost.active);
        assert_eq!(snake.boost.charge_ms, 0);
    }

    #[test]
    fn snapshot_tick_mismatch_and_malformed_delta_leave_committed_state_untouched() {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        state.add_player(7, None).unwrap();
        let mut engine = GameEngine::new_from_state(99, state);
        let before = serde_json::to_value(engine.committed_state()).unwrap();

        let mismatched_anchor = engine.committed_state().clone();
        assert!(
            engine
                .process_server_event(&GameEventMessage {
                    game_id: 99,
                    tick: mismatched_anchor.tick + 1,
                    sequence: 1,
                    stream_seq: 1,
                    user_id: None,
                    event: GameEvent::Snapshot {
                        game_state: mismatched_anchor,
                    },
                })
                .is_err()
        );
        assert_eq!(
            serde_json::to_value(engine.committed_state()).unwrap(),
            before
        );
        assert!(engine.sync_status().needs_resync);
        assert_eq!(engine.sync_status().last_stream_seq, 0);

        engine.clear_needs_resync();
        assert!(
            engine
                .process_server_event(&GameEventMessage {
                    game_id: 99,
                    tick: 2,
                    sequence: 2,
                    stream_seq: 1,
                    user_id: None,
                    event: GameEvent::BoostPacketCollected {
                        pad_id: u8::MAX,
                        snake_id: 0,
                        charge_ms_after: 750,
                        respawn_at_tick: 162,
                    },
                })
                .is_err()
        );
        assert_eq!(
            serde_json::to_value(engine.committed_state()).unwrap(),
            before,
            "candidate catch-up and the malformed packet transition must both roll back"
        );
        assert!(engine.sync_status().needs_resync);
    }

    #[test]
    fn initial_engine_snapshot_uses_its_internal_pre_step_tick() {
        let state = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, None, 0);
        let mut engine = GameEngine::new_from_state(1, state);
        let events = engine.run_until(600).expect("first committed quantum");
        let (envelope_tick, _, snapshot_tick) = events
            .iter()
            .find_map(|(tick, sequence, event)| match event {
                GameEvent::Snapshot { game_state } => Some((*tick, *sequence, game_state.tick)),
                _ => None,
            })
            .expect("first quantum emits the initial engine snapshot");
        assert_eq!(envelope_tick, 0);
        assert_eq!(snapshot_tick, 0);
        assert_eq!(envelope_tick, snapshot_tick);
    }

    /// The client-path double-tap: two turns issued within one predicted
    /// tick. Both are stamped on the same tick, and the prediction replays
    /// them through the shared deferral rule — the maneuver plays out as two
    /// steps, never a reversal.
    #[test]
    fn local_double_tap_predicts_two_step_maneuver() {
        let mut state = GameState::new(30, 30, GameType::Solo, QueueMode::Quickmatch, None, 0);
        state.add_player(1, None).expect("add player");
        let snake_id = state.players[&1].snake_id;
        let tick_ms = state.properties.tick_duration_ms as i64;
        let mut engine = GameEngine::new_from_state(1, state);
        engine.set_local_player_id(1);

        engine
            .rebuild_predicted_state(tick_ms * 5)
            .expect("rebuild");
        let snake = &engine.predicted_state().expect("predicted").arena.snakes[snake_id as usize];
        let travel = snake.direction;
        let length_before = snake.length();
        let first_turn = clockwise(travel);
        let second_turn = clockwise(first_turn); // opposite of `travel`

        for direction in [first_turn, second_turn] {
            engine
                .process_local_command(GameCommand::Turn {
                    snake_id,
                    direction,
                })
                .expect("local command");
        }

        engine
            .rebuild_predicted_state(tick_ms * 10)
            .expect("rebuild");

        let snake = &engine.predicted_state().expect("predicted").arena.snakes[snake_id as usize];
        assert!(
            snake.is_alive,
            "predicted snake must not reverse into itself"
        );
        assert_eq!(
            snake.direction, second_turn,
            "prediction must play the double-tap as a two-step maneuver"
        );
        assert_eq!(snake.length(), length_before);
    }

    #[test]
    fn prediction_rebuild_retains_crash_cue_across_multi_tick_catch_up() {
        let (mut engine, snake_id, tick_ms) = engine_with_imminent_wall_crash();

        // One rebuild crosses several simulation ticks. The snake is one cell
        // from the left wall, so it crashes after two cells of travel — well
        // before the horizon — and a one-tick-only cue would have been gone by
        // the final state the renderer observes. Both the horizon and the
        // crash tick are stated in milliseconds of travel so this holds at
        // either simulation quantum.
        let horizon_ms = 5 * DEFAULT_TICK_INTERVAL_MS as i64;
        let crash_tick = (2 * DEFAULT_TICK_INTERVAL_MS as i64 / tick_ms) as u32;
        engine
            .rebuild_predicted_state(horizon_ms)
            .expect("multi-tick prediction rebuild");

        let predicted = engine.predicted_state().expect("predicted state");
        assert_eq!(predicted.current_tick() as i64, horizon_ms / tick_ms);
        assert!(!predicted.arena.snakes[snake_id as usize].is_alive);
        assert!(
            predicted
                .recent_crashes
                .iter()
                .any(|crash| crash.tick == crash_tick && crash.snake_id == snake_id),
            "the tick-{crash_tick} crash must remain visible after catching up to {horizon_ms}ms"
        );

        let committed = engine.committed_state();
        assert_eq!(committed.current_tick(), 0);
        assert!(committed.arena.snakes[snake_id as usize].is_alive);
        assert!(
            committed.recent_crashes.is_empty(),
            "prediction must expose the crash before committed state reaches it"
        );
    }

    #[test]
    fn authoritative_event_forces_same_target_prediction_reconciliation() {
        let (mut engine, snake_id, tick_ms) = engine_with_imminent_wall_crash();
        // Far enough past the two cells of travel that reach the wall.
        let target_ts = 5 * DEFAULT_TICK_INTERVAL_MS as i64;
        let target_tick = (target_ts / tick_ms) as u32;

        engine
            .rebuild_predicted_state(target_ts)
            .expect("initial prediction rebuild");
        assert!(
            engine
                .predicted_state()
                .expect("predicted state")
                .recent_crashes
                .iter()
                .any(|crash| crash.snake_id == snake_id),
            "test setup must first predict the wall crash"
        );

        // Authoritative input turns the snake away before the predicted crash.
        // The wall-clock target remains tick 5, so reconciliation depends on
        // `prediction_needs_rebuild`, not on advancing to tick 6.
        engine
            .process_server_event(&GameEventMessage {
                game_id: 1,
                tick: 0,
                sequence: 1,
                stream_seq: 1,
                user_id: Some(1),
                event: GameEvent::SnakeTurned {
                    snake_id,
                    direction: Direction::Up,
                },
            })
            .expect("authoritative turn");
        assert!(engine.prediction_needs_rebuild);

        engine
            .rebuild_predicted_state(target_ts)
            .expect("same-target reconciliation rebuild");

        let predicted = engine.predicted_state().expect("reconciled prediction");
        assert_eq!(predicted.current_tick(), target_tick);
        assert!(predicted.arena.snakes[snake_id as usize].is_alive);
        assert_eq!(
            predicted.arena.snakes[snake_id as usize].direction,
            Direction::Up
        );
        assert!(
            predicted.recent_crashes.is_empty(),
            "same-target replay must retract the invalid predicted crash cue"
        );
        assert!(!engine.prediction_needs_rebuild);
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

        // Advance the committed state past a few ticks. `run_until` lags the
        // wall-clock target by the 500 ms committed-lag window, so the target
        // is that window plus the ticks we actually want committed — stated in
        // milliseconds so it holds at either simulation quantum.
        engine.run_until(500 + tick_ms * 10).expect("run_until");
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

        // Advance far enough for the rebased pair to execute *and* for the
        // movements that apply them to land. The deferred turn is queued one
        // tick later, but a turn only becomes visible in `direction` when the
        // snake actually steps — and at a 50ms quantum a step is every other
        // tick. Four normal movement intervals covers both rates.
        engine
            .run_until(500 + tick_ms * 10 + 4 * DEFAULT_TICK_INTERVAL_MS as i64)
            .expect("run_until");
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
