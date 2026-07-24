//! Versioned per-game recovery envelopes and logical command outcomes.

use crate::cluster_membership::EXECUTOR_PROTOCOL_VERSION;
use anyhow::{Context, Result, bail};
use common::{ClientCommandIdentityV2, GameCommandMessage, GameEvent, GameEventMessage, GameState};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

pub const RECOVERY_SCHEMA_VERSION: u16 = 2;
pub const DEFAULT_RECOVERY_RETENTION: Duration = Duration::from_secs(30 * 60);
pub const DEFAULT_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(1);
pub const DEFAULT_MAX_CHECKPOINT_AGE: Duration = Duration::from_secs(10);
pub const DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION: usize = 512;
/// A reconnect reuses its in-memory client session, while a page reload may
/// legitimately create a new one. Keep a generous game-wide allowance for
/// those rotations, but never let an authenticated client grow every recovery
/// checkpoint without bound by inventing a fresh session ID per command.
pub const MAX_RECORDED_COMMAND_SESSIONS_PER_GAME: usize = 64;
pub const MAX_CLIENT_GAME_SESSION_ID_BYTES: usize = 128;
pub const RECOVERY_FAILURE_SCHEMA_VERSION: u16 = 1;
pub const COMMAND_DECISION_SCHEMA_VERSION: u16 = 1;
pub const PUBLIC_UNRECOVERABLE_GAME_REASON: &str =
    "The authoritative game state is unavailable after failover";

/// Durable terminal marker for one indexed game whose authoritative recovery
/// envelope cannot be reconstructed. Keeping this separate from the active
/// index lets the partition continue serving healthy games while gateways
/// return a definitive outcome instead of `GameWarming` forever.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryFailureV1 {
    pub schema_version: u16,
    pub game_id: u32,
    pub partition_id: u32,
    pub detected_at_ms: i64,
    pub diagnostic: String,
}

impl RecoveryFailureV1 {
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != RECOVERY_FAILURE_SCHEMA_VERSION {
            bail!("unsupported recovery-failure schema version");
        }
        if self.diagnostic.is_empty() {
            bail!("recovery-failure diagnostic cannot be empty");
        }
        Ok(())
    }
}

pub fn validate_client_command_identity(identity: &ClientCommandIdentityV2) -> Result<()> {
    if identity.sequence == 0 {
        bail!("v2 client command sequence must start at one");
    }
    let session_len = identity.client_game_session_id.len();
    if session_len == 0 || session_len > MAX_CLIENT_GAME_SESSION_ID_BYTES {
        bail!(
            "v2 client game session ID must contain 1..={} bytes",
            MAX_CLIENT_GAME_SESSION_ID_BYTES
        );
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CommandOutcome {
    Scheduled { command: GameCommandMessage },
    Rejected { reason: String },
}

/// Write-ahead record for a client-visible command outcome. The partition
/// executor stores this under the Redis command-stream ID before publishing
/// the event, so a successor can restore the exact server schedule and event
/// watermark instead of recomputing them from an older checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandDecisionV1 {
    pub schema_version: u16,
    pub source_stream_id: String,
    pub next_server_command_sequence: u32,
    pub event: GameEventMessage,
}

impl CommandDecisionV1 {
    pub fn new(
        source_stream_id: String,
        next_server_command_sequence: u32,
        event: GameEventMessage,
    ) -> Self {
        Self {
            schema_version: COMMAND_DECISION_SCHEMA_VERSION,
            source_stream_id,
            next_server_command_sequence,
            event,
        }
    }

    pub fn identity_and_outcome(&self) -> Result<(&ClientCommandIdentityV2, CommandOutcome)> {
        match &self.event.event {
            GameEvent::CommandScheduledV2 {
                command_id,
                command_message,
                ..
            } => Ok((
                command_id,
                CommandOutcome::Scheduled {
                    command: command_message.clone(),
                },
            )),
            GameEvent::CommandRejected { command_id, reason } => Ok((
                command_id,
                CommandOutcome::Rejected {
                    reason: reason.clone(),
                },
            )),
            _ => bail!("command decision must contain a V2 command outcome"),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != COMMAND_DECISION_SCHEMA_VERSION {
            bail!("unsupported command decision schema version");
        }
        validate_stream_id(&self.source_stream_id)?;
        if self.event.stream_seq == 0 {
            bail!("command decision event must have a sequenced stream watermark");
        }
        let (identity, outcome) = self.identity_and_outcome()?;
        validate_client_command_identity(identity)?;
        if identity.game_id != self.event.game_id {
            bail!("command decision game identity does not match its event");
        }
        if let CommandOutcome::Scheduled { command } = outcome {
            let server_id = command
                .command_id_server
                .as_ref()
                .context("scheduled command decision has no server command ID")?;
            if server_id.user_id != identity.user_id
                || command.command_id_client.user_id != identity.user_id
            {
                bail!("scheduled command decision user identity does not match");
            }
            if server_id.sequence_number >= self.next_server_command_sequence {
                bail!("scheduled command decision does not advance the server counter");
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SessionCommandOutcomes {
    /// Highest terminally resolved sequence with no unresolved gap below it.
    pub contiguous_through: u64,
    /// Recent exact results, including sparse outcomes above the watermark.
    /// Old contiguous results may be pruned only beyond the resend guarantee.
    pub outcomes: BTreeMap<u64, CommandOutcome>,
}

impl SessionCommandOutcomes {
    pub fn get(&self, sequence: u64) -> Option<&CommandOutcome> {
        self.outcomes.get(&sequence)
    }

    pub fn is_terminally_resolved(&self, sequence: u64) -> bool {
        sequence > 0
            && (sequence <= self.contiguous_through || self.outcomes.contains_key(&sequence))
    }

    pub fn record(
        &mut self,
        sequence: u64,
        outcome: CommandOutcome,
        max_results: usize,
    ) -> Result<CommandOutcome> {
        if sequence == 0 {
            bail!("v2 client command sequence must start at one");
        }
        if max_results == 0 {
            bail!("resolved command retention must be non-zero");
        }
        if let Some(existing) = self.outcomes.get(&sequence) {
            if existing != &outcome {
                bail!("one client command identity resolved to conflicting outcomes");
            }
            return Ok(existing.clone());
        }
        self.can_record(sequence, max_results)?;
        // The retention pass below is allowed to prune the entry we are
        // inserting when it closes a contiguous gap. Keep the resolved value
        // independently instead of assuming the map still contains it.
        let resolved = outcome.clone();
        self.outcomes.insert(sequence, outcome);
        while let Some(next) = self.contiguous_through.checked_add(1) {
            if !self.outcomes.contains_key(&next) {
                break;
            }
            self.contiguous_through = next;
        }

        // Bound old, already-contiguous results. Sparse entries are never
        // discarded because doing so could falsely advance across a gap.
        while self.outcomes.len() > max_results {
            let Some(oldest) = self.outcomes.keys().next().copied() else {
                break;
            };
            if oldest > self.contiguous_through {
                bail!("too many sparse command outcomes; session must resynchronize");
            }
            self.outcomes.remove(&oldest);
        }
        Ok(resolved)
    }

    pub fn can_record(&self, sequence: u64, max_results: usize) -> Result<()> {
        if sequence == 0 {
            bail!("v2 client command sequence must start at one");
        }
        if max_results == 0 {
            bail!("resolved command retention must be non-zero");
        }
        if self.outcomes.contains_key(&sequence) {
            return Ok(());
        }
        // Do not mutate state when the bounded sparse window is full. The
        // caller can quarantine this one command without taking down its game
        // actor or the partition.
        let next_contiguous = self.contiguous_through.checked_add(1);
        if self.outcomes.len() >= max_results && Some(sequence) != next_contiguous {
            bail!("too many sparse command outcomes; session must resynchronize");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ResolvedCommandState {
    /// Key format is `<user-id>:<client-game-session-id>`.
    pub sessions: BTreeMap<String, SessionCommandOutcomes>,
}

impl ResolvedCommandState {
    pub fn session_key(identity: &ClientCommandIdentityV2) -> String {
        format!("{}:{}", identity.user_id, identity.client_game_session_id)
    }

    pub fn get(&self, identity: &ClientCommandIdentityV2) -> Option<&CommandOutcome> {
        self.sessions
            .get(&Self::session_key(identity))?
            .get(identity.sequence)
    }

    pub fn is_terminally_resolved(&self, identity: &ClientCommandIdentityV2) -> bool {
        self.sessions
            .get(&Self::session_key(identity))
            .is_some_and(|session| session.is_terminally_resolved(identity.sequence))
    }

    pub fn record(
        &mut self,
        identity: &ClientCommandIdentityV2,
        outcome: CommandOutcome,
        max_results: usize,
    ) -> Result<CommandOutcome> {
        // Check the game-wide session bound before `entry` mutates the map.
        // Existing sessions remain usable at the limit; evicting one would
        // forget its watermark and could execute a delayed resend twice.
        self.can_record(identity, max_results)?;
        self.sessions
            .entry(Self::session_key(identity))
            .or_default()
            .record(identity.sequence, outcome, max_results)
    }

    pub fn can_record(&self, identity: &ClientCommandIdentityV2, max_results: usize) -> Result<()> {
        validate_client_command_identity(identity)?;
        match self.sessions.get(&Self::session_key(identity)) {
            Some(session) => session.can_record(identity.sequence, max_results),
            None if max_results == 0 => bail!("resolved command retention must be non-zero"),
            None if self.sessions.len() >= MAX_RECORDED_COMMAND_SESSIONS_PER_GAME => {
                bail!("too many client command sessions for one game; session must resynchronize")
            }
            None => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryEnvelopeV2 {
    pub schema_version: u16,
    pub executor_protocol_version: u16,
    pub game_id: u32,
    pub partition_id: u32,
    pub game_state: GameState,
    /// Highest command-stream entry incorporated for this game.
    pub command_cursor: String,
    pub resolved_client_commands: ResolvedCommandState,
    pub next_server_command_sequence: u32,
    pub next_event_stream_sequence: u64,
    /// Ephemeral takeover floor loaded from the cooperative-handoff marker.
    /// It is deliberately outside the durable recovery schema: the successor
    /// merges it after decision replay, then its first checkpoint persists the
    /// result and atomically clears the marker.
    #[serde(skip)]
    pub planned_handoff_event_stream_watermark: Option<u64>,
    pub checkpointed_at_ms: i64,
    /// Diagnostic only. New writes are controlled by the live lease key.
    pub source_lease_token: String,
}

impl RecoveryEnvelopeV2 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        game_id: u32,
        partition_id: u32,
        game_state: GameState,
        command_cursor: String,
        resolved_client_commands: ResolvedCommandState,
        next_server_command_sequence: u32,
        next_event_stream_sequence: u64,
        checkpointed_at_ms: i64,
        source_lease_token: String,
    ) -> Self {
        Self {
            schema_version: RECOVERY_SCHEMA_VERSION,
            executor_protocol_version: EXECUTOR_PROTOCOL_VERSION,
            game_id,
            partition_id,
            game_state,
            command_cursor,
            resolved_client_commands,
            next_server_command_sequence,
            next_event_stream_sequence,
            planned_handoff_event_stream_watermark: None,
            checkpointed_at_ms,
            source_lease_token,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != RECOVERY_SCHEMA_VERSION {
            bail!("unsupported recovery schema version");
        }
        if self.executor_protocol_version != EXECUTOR_PROTOCOL_VERSION {
            bail!("unsupported executor protocol version");
        }
        validate_stream_id(&self.command_cursor)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RecoveryConfig {
    pub retention: Duration,
    pub checkpoint_interval: Duration,
    pub max_checkpoint_age: Duration,
    pub max_recorded_outcomes_per_session: usize,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            retention: DEFAULT_RECOVERY_RETENTION,
            checkpoint_interval: DEFAULT_CHECKPOINT_INTERVAL,
            max_checkpoint_age: DEFAULT_MAX_CHECKPOINT_AGE,
            max_recorded_outcomes_per_session: DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION,
        }
    }
}

impl RecoveryConfig {
    pub fn from_env() -> Result<Self> {
        fn duration_from_env(name: &str, default: Duration) -> Result<Duration> {
            let Some(value) = std::env::var(name).ok() else {
                return Ok(default);
            };
            let millis: u64 = value
                .parse()
                .with_context(|| format!("{name} must be milliseconds"))?;
            Ok(Duration::from_millis(millis))
        }
        let retention = duration_from_env(
            "SNAKETRON_RECOVERY_RETENTION_MS",
            DEFAULT_RECOVERY_RETENTION,
        )?;
        let checkpoint_interval = duration_from_env(
            "SNAKETRON_CHECKPOINT_INTERVAL_MS",
            DEFAULT_CHECKPOINT_INTERVAL,
        )?;
        let max_checkpoint_age = duration_from_env(
            "SNAKETRON_MAX_CHECKPOINT_AGE_MS",
            DEFAULT_MAX_CHECKPOINT_AGE,
        )?;
        if retention < Duration::from_secs(60) {
            bail!("recovery retention must be at least one minute");
        }
        if checkpoint_interval > Duration::from_secs(1)
            || checkpoint_interval < Duration::from_millis(100)
        {
            bail!("checkpoint interval must be between 100ms and 1s");
        }
        if max_checkpoint_age <= checkpoint_interval || max_checkpoint_age >= retention {
            bail!("maximum checkpoint age must exceed its cadence and remain below retention");
        }
        Ok(Self {
            retention,
            checkpoint_interval,
            max_checkpoint_age,
            ..Self::default()
        })
    }
}

/// Wall-clock cadence deliberately independent of the game's tick duration.
#[derive(Debug, Clone)]
pub struct CheckpointCadence {
    interval: Duration,
    next: Instant,
}

impl CheckpointCadence {
    pub fn new(interval: Duration, now: Instant) -> Result<Self> {
        if interval.is_zero() || interval > Duration::from_secs(1) {
            bail!("checkpoint cadence must be non-zero and no greater than one second");
        }
        Ok(Self {
            interval,
            next: now + interval,
        })
    }

    pub fn due(&self, now: Instant) -> bool {
        now >= self.next
    }

    pub fn mark(&mut self, now: Instant) {
        self.next = now + self.interval;
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }
}

pub fn validate_stream_id(id: &str) -> Result<(u64, u64)> {
    let (ms, sequence) = id
        .split_once('-')
        .ok_or_else(|| anyhow::anyhow!("invalid Redis stream ID"))?;
    let ms = ms.parse().context("invalid Redis stream timestamp")?;
    let sequence = sequence.parse().context("invalid Redis stream sequence")?;
    Ok((ms, sequence))
}

pub fn stream_id_leq(left: &str, right: &str) -> Result<bool> {
    Ok(validate_stream_id(left)? <= validate_stream_id(right)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{
        CommandId, CustomGameSettings, Direction, GameCommand, GameMode, GameType, QueueMode,
    };

    fn identity(sequence: u64) -> ClientCommandIdentityV2 {
        ClientCommandIdentityV2 {
            game_id: 4,
            user_id: 9,
            client_game_session_id: "session-a".into(),
            sequence,
        }
    }

    fn rejected(reason: &str) -> CommandOutcome {
        CommandOutcome::Rejected {
            reason: reason.into(),
        }
    }

    #[test]
    fn watermark_never_crosses_an_unresolved_gap() {
        let mut state = ResolvedCommandState::default();
        state.record(&identity(2), rejected("two"), 16).unwrap();
        let session = &state.sessions["9:session-a"];
        assert_eq!(session.contiguous_through, 0);
        state.record(&identity(1), rejected("one"), 16).unwrap();
        let session = &state.sessions["9:session-a"];
        assert_eq!(session.contiguous_through, 2);
    }

    #[test]
    fn one_identity_cannot_change_outcome() {
        let mut state = ResolvedCommandState::default();
        state.record(&identity(1), rejected("no"), 16).unwrap();
        assert!(
            state
                .record(&identity(1), rejected("different"), 16)
                .is_err()
        );
    }

    #[test]
    fn invalid_identity_is_rejected_without_creating_session_state() {
        let mut state = ResolvedCommandState::default();
        let mut invalid = identity(0);
        assert!(state.record(&invalid, rejected("invalid"), 16).is_err());
        assert!(state.sessions.is_empty());

        invalid.sequence = 1;
        invalid.client_game_session_id = "x".repeat(MAX_CLIENT_GAME_SESSION_ID_BYTES + 1);
        assert!(state.record(&invalid, rejected("invalid"), 16).is_err());
        assert!(state.sessions.is_empty());
    }

    #[test]
    fn game_wide_session_limit_rejects_rotation_without_forgetting_dedupe_state() {
        let mut state = ResolvedCommandState::default();
        for index in 0..MAX_RECORDED_COMMAND_SESSIONS_PER_GAME {
            let mut command = identity(1);
            command.client_game_session_id = format!("session-{index}");
            state.record(&command, rejected("recorded"), 16).unwrap();
        }
        assert_eq!(state.sessions.len(), MAX_RECORDED_COMMAND_SESSIONS_PER_GAME);

        let mut existing = identity(1);
        existing.client_game_session_id = "session-0".into();
        assert_eq!(
            state.record(&existing, rejected("recorded"), 16).unwrap(),
            rejected("recorded")
        );
        let mut next_for_existing = existing;
        next_for_existing.sequence = 2;
        state
            .record(&next_for_existing, rejected("still accepted"), 16)
            .unwrap();

        let mut rotated = identity(1);
        rotated.client_game_session_id = "session-over-limit".into();
        assert!(state.can_record(&rotated, 16).is_err());
        assert!(
            state
                .record(&rotated, rejected("must not be inserted"), 16)
                .is_err()
        );
        assert!(!state.sessions.contains_key("9:session-over-limit"));
        assert_eq!(state.sessions.len(), MAX_RECORDED_COMMAND_SESSIONS_PER_GAME);
    }

    #[test]
    fn sparse_overflow_does_not_mutate_outcomes() {
        let mut outcomes = SessionCommandOutcomes::default();
        outcomes.record(10, rejected("ten"), 1).unwrap();
        assert!(outcomes.record(11, rejected("eleven"), 1).is_err());
        assert_eq!(outcomes.outcomes.len(), 1);
        assert!(outcomes.outcomes.contains_key(&10));
        assert!(!outcomes.outcomes.contains_key(&11));
    }

    #[test]
    fn pruned_exact_outcome_remains_terminally_resolved() {
        let mut outcomes = SessionCommandOutcomes::default();
        outcomes.record(1, rejected("one"), 1).unwrap();
        outcomes.record(2, rejected("two"), 1).unwrap();
        assert_eq!(outcomes.contiguous_through, 2);
        assert!(outcomes.get(1).is_none());
        assert!(outcomes.is_terminally_resolved(1));
    }

    #[test]
    fn closing_sparse_gap_at_retention_limit_returns_pruned_outcome() {
        let mut outcomes = SessionCommandOutcomes::default();
        outcomes.record(2, rejected("two"), 1).unwrap();

        let expected = rejected("one");
        assert_eq!(outcomes.record(1, expected.clone(), 1).unwrap(), expected);
        assert_eq!(outcomes.contiguous_through, 2);
        assert!(outcomes.is_terminally_resolved(1));
        assert!(outcomes.is_terminally_resolved(2));
        assert_eq!(outcomes.outcomes.len(), 1);
    }

    #[test]
    fn checkpoint_cadence_is_wall_clock_not_game_tick_based() {
        let now = Instant::now();
        let cadence = CheckpointCadence::new(Duration::from_secs(1), now).unwrap();
        assert!(!cadence.due(now + Duration::from_millis(999)));
        assert!(cadence.due(now + Duration::from_secs(1)));
    }

    #[test]
    fn recovery_envelope_round_trips_full_state_and_metadata() {
        let mut state = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, Some(7), 0);
        state.command_queue.push(GameCommandMessage {
            command_id_client: common::CommandId {
                tick: 1,
                user_id: 9,
                sequence_number: 1,
            },
            command_id_server: None,
            command: GameCommand::Turn {
                snake_id: 1,
                direction: Direction::Up,
            },
        });
        let envelope = RecoveryEnvelopeV2::new(
            4,
            4,
            state,
            "123-4".into(),
            ResolvedCommandState::default(),
            17,
            91,
            5,
            "token".into(),
        );
        let decoded: RecoveryEnvelopeV2 =
            serde_json::from_slice(&serde_json::to_vec(&envelope).unwrap()).unwrap();
        decoded.validate().unwrap();
        assert_eq!(decoded.next_server_command_sequence, 17);
        assert_eq!(decoded.command_cursor, "123-4");
        assert!(decoded.game_state.rng.is_some());
    }

    #[test]
    fn recovery_preserves_rng_queued_commands_and_slow_tick_execution() {
        let game_id = 44;
        let start_ms = 1_000_000;
        let settings = CustomGameSettings {
            arena_width: 40,
            arena_height: 40,
            tick_duration_ms: 750,
            food_spawn_rate: 3.0,
            max_players: 4,
            game_mode: GameMode::FreeForAll { max_players: 4 },
            is_private: false,
            allow_spectators: true,
            snake_start_length: 4,
        };
        let mut state = GameState::new(
            settings.arena_width,
            settings.arena_height,
            GameType::Custom { settings },
            QueueMode::Quickmatch,
            Some(0x5eed),
            start_ms,
        );
        state.status = common::GameStatus::Started { server_id: 7 };
        let snake_id = state
            .add_player(9, Some("player-9".into()))
            .unwrap()
            .snake_id;
        let mut original = common::GameEngine::new_from_state(game_id, state);
        original
            .process_command(GameCommandMessage {
                command_id_client: CommandId {
                    tick: 8,
                    user_id: 9,
                    sequence_number: 1,
                },
                command_id_server: None,
                command: GameCommand::Turn {
                    snake_id,
                    direction: Direction::Up,
                },
            })
            .unwrap();

        let envelope = RecoveryEnvelopeV2::new(
            game_id,
            4,
            original.get_committed_state().clone(),
            "123-4".into(),
            ResolvedCommandState::default(),
            original.next_server_command_sequence(),
            91,
            start_ms,
            "token".into(),
        );
        let decoded: RecoveryEnvelopeV2 =
            serde_json::from_slice(&serde_json::to_vec(&envelope).unwrap()).unwrap();
        let mut recovered = common::GameEngine::new_from_state_with_command_counter(
            game_id,
            decoded.game_state,
            decoded.next_server_command_sequence,
        );

        assert_eq!(
            serde_json::to_value(original.get_committed_state()).unwrap(),
            serde_json::to_value(recovered.get_committed_state()).unwrap()
        );
        assert!(recovered.get_committed_state().rng.is_some());
        assert!(recovered.get_committed_state().has_scheduled_commands(8));
        assert_eq!(
            recovered.get_committed_state().properties.tick_duration_ms,
            750
        );

        // Five wall-clock slow ticks leave the tick-8 command queued while
        // advancing RNG-driven food generation identically on both engines.
        let target_ms = start_ms + 5 * 750 + 500;
        original.run_until(target_ms).unwrap();
        recovered.run_until(target_ms).unwrap();
        assert_eq!(original.get_committed_state().tick, 5);
        assert_eq!(recovered.get_committed_state().tick, 5);
        assert!(recovered.get_committed_state().has_scheduled_commands(8));
        assert_eq!(
            serde_json::to_value(original.get_committed_state()).unwrap(),
            serde_json::to_value(recovered.get_committed_state()).unwrap()
        );

        // Executor checkpoint cadence is wall-clock based and therefore does
        // not stretch to the custom game's 750 ms tick duration.
        let now = Instant::now();
        let cadence = CheckpointCadence::new(Duration::from_secs(1), now).unwrap();
        assert!(!cadence.due(now + Duration::from_millis(999)));
        assert!(cadence.due(now + Duration::from_secs(1)));
    }
}
