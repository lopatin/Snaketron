//! Versioned per-game recovery envelopes and logical command outcomes.

use crate::cluster_membership::EXECUTOR_PROTOCOL_VERSION;
use anyhow::{Context, Result, bail};
use common::{
    ClientCommandIdentityV2, CommandId, GAME_RECORDING_FORMAT_VERSION, GAMEPLAY_REPLAY_VERSION,
    GameCommandMessage, GameEvent, GameEventMessage, GameRecordingV1, GameState,
    RecordedGameMessage, ReplayAnchor, ReplayVisibility,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

/// Version 8 moves the complete replay archive out of every recovery envelope
/// into a lease-fenced append-only Redis journal. A checkpoint now carries
/// only recorder metadata and the journal cursor it atomically covers.
///
/// Version 7 added deterministic death attribution to serialized crash cues
/// and to events produced after a checkpoint is restored.
///
/// Version 6: food value, physical growth, personal scoring, and team cargo
/// now follow the authoritative per-snake combo state. Resuming a version-5
/// checkpoint would silently switch an in-flight match from the old fixed
/// two-segment growth model to combo scoring, so the recovery gate rejects it.
///
/// Version 5: Solo and free-for-all now carry Boost, which moves them to the
/// 50ms simulation quantum, and 2v2/free-for-all carry double food. A
/// checkpoint written before this change deserializes cleanly — `boost` and
/// `unlimited` both default — but describes a match the current invariants
/// reject, and its `tick_duration_ms` would silently halve the simulation rate
/// of a match already in flight. Reject by version here rather than letting
/// recovery fail later with a confusing validation error.
///
/// (4 was: team matches carry `score_limit` instead of `time_limit_ms`, and
/// snakes carry a latched Boost intent.)
pub const RECOVERY_SCHEMA_VERSION: u16 = 8;
pub const DEFAULT_RECOVERY_RETENTION: Duration = Duration::from_secs(30 * 60);
pub const DEFAULT_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(1);
pub const DEFAULT_MAX_CHECKPOINT_AGE: Duration = Duration::from_secs(10);
/// Shared bounded protocol/storage budget for exact command results. The
/// contiguous watermark permanently fences older contiguous identities.
pub const DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION: usize = 128;
/// A reconnect reuses its in-memory client session, while a page reload may
/// legitimately create a new one. Keep a generous game-wide allowance for
/// those rotations, but never let an authenticated client grow every recovery
/// checkpoint without bound by inventing a fresh session ID per command.
pub const MAX_RECORDED_COMMAND_SESSIONS_PER_GAME: usize = 64;
pub const MAX_CLIENT_GAME_SESSION_ID_BYTES: usize = 128;
pub const RECOVERY_FAILURE_SCHEMA_VERSION: u16 = 1;
pub const COMMAND_DECISION_SCHEMA_VERSION: u16 = 1;
pub const REPLAY_JOURNAL_REFERENCE_SCHEMA_VERSION: u16 = 1;
pub const PUBLIC_UNRECOVERABLE_GAME_REASON: &str =
    "The authoritative game state is unavailable after failover";
pub const SPARSE_COMMAND_WINDOW_REJECTION_REASON: &str =
    "client command session exceeded its recoverable sparse sequence window";

/// Five seconds balances inexpensive seeking with replay-journal volume.
/// Checkpoints carry only the journal cursor; the full append-only history is
/// stored beside the checkpoint under the same partition lease fence.
pub const REPLAY_ANCHOR_INTERVAL_MS: u32 = 5_000;
/// Executor-local budget for the replay view used by the optional
/// Play-of-the-Game scorer. The independently recovery-persisted archive below
/// remains complete, so presentation constraints never shorten replay storage.
pub const POTG_SELECTION_RING_MAX_BYTES: usize = 6 * 1024 * 1024;
const POTG_SELECTION_RING_JSON_OVERHEAD_BYTES: usize = 256;

fn default_potg_selection_ring_max_bytes() -> usize {
    POTG_SELECTION_RING_MAX_BYTES
}

fn replay_recording_allowed_from_lookup(
    state: &GameState,
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> bool {
    if state.is_stress_test {
        return false;
    }
    !lookup("SNAKETRON_TEST_MODE").is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn replay_recording_allowed(state: &GameState) -> bool {
    replay_recording_allowed_from_lookup(state, |name| std::env::var(name).ok())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ReplayJournalEntry {
    Anchor(ReplayAnchor),
    Message(RecordedGameMessage),
}

impl ReplayJournalEntry {
    fn tick(&self) -> u32 {
        match self {
            Self::Anchor(anchor) => anchor.tick,
            Self::Message(message) => message.tick,
        }
    }

    fn encoded_len(&self) -> Result<usize> {
        // Count the serialized entry plus its array separator. Reserving the
        // fixed envelope overhead makes this a conservative hard JSON budget.
        Ok(serde_json::to_vec(self)?.len().saturating_add(1))
    }
}

/// One immutable replay-journal cell. `cursor` is independent of the replay
/// message sequence because an anchor and the latest message may legitimately
/// share a replay sequence boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReplayJournalDelta {
    cursor: u64,
    entry: ReplayJournalEntry,
}

impl ReplayJournalDelta {
    pub(crate) fn cursor(&self) -> u64 {
        self.cursor
    }
}

/// Bounded immutable pointer from a completion record to its retained Redis
/// replay journal. New actors bind the cursor to final tick/sync metadata;
/// PersistGame performs the full semantic verification off the partition hot
/// path. The optional digest fields keep already-written records readable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayJournalReferenceV1 {
    pub schema_version: u16,
    pub game_id: u32,
    pub journal_cursor: u64,
    pub next_sequence: u64,
    /// Final replay boundary captured by the actor without assembling the
    /// complete archive. PersistGame verifies the hydrated journal reaches
    /// exactly this state before it serializes or uploads anything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_tick: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_sync_hash: Option<u64>,
    /// Compatibility binding used by completion records written before full
    /// replay materialization moved out of the partition actor. New records
    /// omit this pair and bind the journal through final tick/sync metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recording_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recording_bytes: Option<u64>,
}

impl ReplayJournalReferenceV1 {
    fn new(game_id: u32, journal_cursor: u64, next_sequence: u64, final_state: &GameState) -> Self {
        Self {
            schema_version: REPLAY_JOURNAL_REFERENCE_SCHEMA_VERSION,
            game_id,
            journal_cursor,
            next_sequence,
            end_tick: Some(final_state.tick),
            end_sync_hash: Some(final_state.sync_hash()),
            recording_sha256: None,
            recording_bytes: None,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.schema_version != REPLAY_JOURNAL_REFERENCE_SCHEMA_VERSION {
            bail!("unsupported replay journal reference schema version");
        }
        if self.journal_cursor == 0 || self.next_sequence == 0 {
            bail!("replay journal reference cannot be empty");
        }
        match (self.end_tick, self.end_sync_hash) {
            (Some(_), Some(_)) | (None, None) => {}
            _ => bail!("replay journal reference has incomplete final-state metadata"),
        }
        match (&self.recording_sha256, self.recording_bytes) {
            (Some(sha256), Some(bytes)) => {
                if bytes == 0 {
                    bail!("replay journal reference recording cannot be empty");
                }
                if sha256.len() != 64 || !sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    bail!("replay journal reference SHA-256 is invalid");
                }
            }
            (None, None) => {}
            _ => bail!("replay journal reference has an incomplete legacy digest"),
        }
        if self.end_tick.is_none() && self.recording_sha256.is_none() {
            bail!("replay journal reference has no immutable recording boundary");
        }
        Ok(())
    }
}

/// Recovery-persisted source for the immutable recording written at
/// completion. Stress/load-test matches are excluded by a server-attested
/// field on `GameState`; usernames are deliberately never used as a trust
/// signal.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplayRecordingState {
    #[serde(default)]
    enabled: bool,
    #[serde(skip, default)]
    anchors: Vec<ReplayAnchor>,
    #[serde(skip, default)]
    messages: Vec<RecordedGameMessage>,
    #[serde(default)]
    next_sequence: u64,
    /// Highest append-only journal cell incorporated by this actor. This is
    /// the only full-history position serialized into recovery checkpoints.
    #[serde(default)]
    journal_cursor: u64,
    /// Executor-local cells not yet covered by a successful fenced checkpoint.
    /// They are passed to the checkpoint Lua script but never serialized into
    /// `RecoveryEnvelopeV2`, keeping retry payloads proportional to one delta.
    #[serde(skip, default)]
    pending_journal: Vec<ReplayJournalDelta>,
    /// Bounded, oldest-first view used only for highlight selection. Keeping
    /// it distinct from `anchors`/`messages` preserves the complete archive
    /// across cap eviction and executor recovery.
    #[serde(skip, default)]
    potg_selection_ring: VecDeque<ReplayJournalEntry>,
    #[serde(skip, default)]
    potg_selection_ring_bytes: usize,
    #[serde(skip, default)]
    potg_ring_truncated: bool,
    #[serde(skip, default)]
    potg_ring_evicted_through_tick: u32,
    #[serde(skip, default)]
    activation_tick: u32,
    #[serde(skip, default = "default_potg_selection_ring_max_bytes")]
    potg_selection_ring_max_bytes: usize,
}

impl ReplayRecordingState {
    pub fn new(activation_state: &GameState) -> Self {
        if !replay_recording_allowed(activation_state) {
            return Self::default();
        }
        let activation_anchor = ReplayAnchor {
            tick: activation_state.tick,
            sequence: 0,
            state: activation_state.clone(),
        };
        let initial_entry = ReplayJournalEntry::Anchor(activation_anchor.clone());
        let initial_bytes = initial_entry
            .encoded_len()
            .unwrap_or(POTG_SELECTION_RING_MAX_BYTES.saturating_add(1));
        let mut recorder = Self {
            enabled: true,
            anchors: vec![activation_anchor],
            messages: Vec::new(),
            next_sequence: 1,
            journal_cursor: 1,
            pending_journal: vec![ReplayJournalDelta {
                cursor: 1,
                entry: initial_entry.clone(),
            }],
            potg_selection_ring: VecDeque::from([initial_entry]),
            potg_selection_ring_bytes: initial_bytes,
            potg_ring_truncated: false,
            potg_ring_evicted_through_tick: activation_state.tick,
            activation_tick: activation_state.tick,
            potg_selection_ring_max_bytes: POTG_SELECTION_RING_MAX_BYTES,
        };
        recorder.enforce_potg_selection_cap();
        recorder
    }

    /// Initial recovery checkpoints are written before a new actor normalizes
    /// `Stopped` to its authoritative `Started` state. Seed only metadata at
    /// that boundary; the actor creates and journals the real activation
    /// anchor before its first live checkpoint.
    fn checkpoint_seed(activation_state: &GameState) -> Self {
        if !replay_recording_allowed(activation_state) {
            Self::default()
        } else {
            Self {
                enabled: true,
                next_sequence: 1,
                potg_selection_ring_max_bytes: POTG_SELECTION_RING_MAX_BYTES,
                ..Self::default()
            }
        }
    }

    /// Lightweight in-process envelope view. Only the metadata fields are
    /// serialized; the bounded pending delta is consumed separately by the
    /// fenced checkpoint script.
    pub(crate) fn checkpoint_view(&self) -> Self {
        Self {
            enabled: self.enabled,
            next_sequence: self.next_sequence,
            journal_cursor: self.journal_cursor,
            pending_journal: self.pending_journal.clone(),
            potg_selection_ring_max_bytes: POTG_SELECTION_RING_MAX_BYTES,
            ..Self::default()
        }
    }

    pub(crate) fn journal_cursor(&self) -> u64 {
        self.journal_cursor
    }

    pub(crate) fn pending_journal(&self) -> &[ReplayJournalDelta] {
        &self.pending_journal
    }

    pub(crate) fn mark_journal_persisted(&mut self, cursor: u64) -> Result<()> {
        if cursor > self.journal_cursor {
            bail!("persisted replay journal cursor exceeds recorder cursor");
        }
        self.pending_journal.retain(|delta| delta.cursor > cursor);
        Ok(())
    }

    pub(crate) fn journal_reference(
        &self,
        game_id: u32,
        final_state: &GameState,
    ) -> Result<Option<ReplayJournalReferenceV1>> {
        if !self.enabled {
            return Ok(None);
        }
        let reference = ReplayJournalReferenceV1::new(
            game_id,
            self.journal_cursor,
            self.next_sequence,
            final_state,
        );
        reference.validate()?;
        Ok(Some(reference))
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Old envelopes without archive state start at their checkpoint. Current
    /// envelopes rebuild the executor-local PotG ring from the full archive.
    pub fn enable_from_checkpoint_if_needed(&mut self, state: &GameState) {
        if !replay_recording_allowed(state) {
            *self = Self::default();
        } else if self.anchors.is_empty()
            && self.journal_cursor == 0
            && (!self.enabled || self.pending_journal.is_empty())
        {
            *self = Self::new(state);
        } else if self.enabled && self.potg_selection_ring.is_empty() {
            // The selection ring is deliberately executor-local and skipped
            // in recovery serialization. Rebuild it from the complete durable
            // archive on takeover so failover preserves the best bounded tail
            // available under the current cap.
            self.rebuild_potg_selection_ring(state);
        } else if self.enabled {
            // Recompute rather than trusting persisted bookkeeping. This also
            // applies the current binary's cap if a predecessor used a
            // different serialized representation.
            self.potg_selection_ring_bytes = self
                .potg_selection_ring
                .iter()
                .map(|entry| entry.encoded_len().unwrap_or(usize::MAX))
                .fold(0usize, usize::saturating_add);
            self.enforce_potg_selection_cap();
        }
    }

    fn rebuild_potg_selection_ring(&mut self, state: &GameState) {
        if self.try_rebuild_potg_selection_ring(state).is_err() {
            // Highlight selection is optional. A representation that cannot
            // be sized degrades to no PotG while the full archive remains
            // available and completion keeps progressing.
            self.potg_selection_ring.clear();
            self.potg_selection_ring_bytes = 0;
            self.potg_ring_truncated = true;
            self.potg_ring_evicted_through_tick = state.tick;
        }
    }

    fn try_rebuild_potg_selection_ring(&mut self, state: &GameState) -> Result<()> {
        self.potg_selection_ring.clear();
        self.potg_selection_ring_bytes = 0;
        self.potg_ring_truncated = false;
        self.activation_tick = self
            .anchors
            .first()
            .map_or(state.tick, |anchor| anchor.tick);
        self.potg_ring_evicted_through_tick = self.activation_tick;

        if self.anchors.is_empty() {
            self.push_potg_entry(ReplayJournalEntry::Anchor(ReplayAnchor {
                tick: state.tick,
                sequence: self.next_sequence.saturating_sub(1),
                state: state.clone(),
            }))?;
            return Ok(());
        }

        // Anchors are taken after the event batch at their tick. Seed the
        // activation anchor first, then merge messages before later same-tick
        // anchors to preserve the original replay boundary.
        self.push_potg_entry(ReplayJournalEntry::Anchor(self.anchors[0].clone()))?;
        let mut anchor_index = 1;
        let mut message_index = 0;
        while anchor_index < self.anchors.len() || message_index < self.messages.len() {
            let message_precedes_anchor = match (
                self.messages.get(message_index),
                self.anchors.get(anchor_index),
            ) {
                (Some(message), Some(anchor)) => {
                    (message.tick, message.sequence) <= (anchor.tick, anchor.sequence)
                }
                (Some(_), None) => true,
                _ => false,
            };
            if message_precedes_anchor {
                self.push_potg_entry(ReplayJournalEntry::Message(
                    self.messages[message_index].clone(),
                ))?;
                message_index += 1;
            } else {
                self.push_potg_entry(ReplayJournalEntry::Anchor(
                    self.anchors[anchor_index].clone(),
                ))?;
                anchor_index += 1;
            }
        }
        Ok(())
    }

    /// A newly created actor changes `Stopped` to `Started` before it becomes
    /// observable. Keep the pristine activation anchor aligned with that
    /// authoritative state so replay does not depend on a transport-only
    /// status announcement.
    pub fn replace_pristine_activation(&mut self, state: &GameState) {
        if self.enabled && self.messages.is_empty() && self.anchors.len() == 1 {
            let anchor = ReplayAnchor {
                tick: state.tick,
                sequence: 0,
                state: state.clone(),
            };
            self.anchors[0] = anchor.clone();
            if let Some(delta) = self
                .pending_journal
                .iter_mut()
                .find(|delta| delta.cursor == 1)
            {
                delta.entry = ReplayJournalEntry::Anchor(anchor.clone());
            }
            self.activation_tick = state.tick;
            self.potg_selection_ring.clear();
            let entry = ReplayJournalEntry::Anchor(anchor);
            self.potg_selection_ring_bytes = entry
                .encoded_len()
                .unwrap_or(self.potg_selection_ring_max_bytes.saturating_add(1));
            self.potg_selection_ring.push_back(entry);
            self.enforce_potg_selection_cap();
        }
    }

    /// Reconstruct complete authority from the lease-fenced append-only Redis
    /// journal referenced by a deserialized checkpoint cursor.
    pub(crate) fn hydrate_journal(
        &mut self,
        mut deltas: Vec<ReplayJournalDelta>,
        checkpoint_state: &GameState,
    ) -> Result<()> {
        if !self.enabled {
            if self.journal_cursor != 0 || !deltas.is_empty() {
                bail!("disabled replay recorder references journal history");
            }
            return Ok(());
        }
        deltas.sort_by_key(ReplayJournalDelta::cursor);
        if deltas.len() as u64 != self.journal_cursor {
            bail!(
                "replay journal has {} cells, checkpoint expects {}",
                deltas.len(),
                self.journal_cursor
            );
        }

        self.anchors.clear();
        self.messages.clear();
        for (index, delta) in deltas.into_iter().enumerate() {
            let expected = index as u64 + 1;
            if delta.cursor != expected {
                bail!(
                    "replay journal cursor gap: expected {expected}, found {}",
                    delta.cursor
                );
            }
            match delta.entry {
                ReplayJournalEntry::Anchor(anchor) => self.anchors.push(anchor),
                ReplayJournalEntry::Message(message) => self.messages.push(message),
            }
        }
        if self.journal_cursor > 0 && self.anchors.is_empty() {
            bail!("replay journal has no activation anchor");
        }
        let expected_next = self
            .messages
            .last()
            .map_or(1, |message| message.sequence.saturating_add(1));
        if self.next_sequence != expected_next {
            bail!(
                "replay journal next sequence {} does not follow {}",
                self.next_sequence,
                expected_next.saturating_sub(1)
            );
        }
        self.pending_journal.clear();
        self.rebuild_potg_selection_ring(checkpoint_state);
        Ok(())
    }

    pub(crate) fn hydrate_reference(
        reference: &ReplayJournalReferenceV1,
        deltas: Vec<ReplayJournalDelta>,
        final_state: &GameState,
    ) -> Result<Self> {
        reference.validate()?;
        let mut recorder = Self {
            enabled: true,
            next_sequence: reference.next_sequence,
            journal_cursor: reference.journal_cursor,
            potg_selection_ring_max_bytes: POTG_SELECTION_RING_MAX_BYTES,
            ..Self::default()
        };
        recorder.hydrate_journal(deltas, final_state)?;
        Ok(recorder)
    }

    pub fn record_event(&mut self, tick: u32, event: GameEvent) -> Result<()> {
        if !self.enabled
            || matches!(
                event,
                GameEvent::Snapshot { .. } | GameEvent::TickHash { .. }
            )
        {
            return Ok(());
        }
        let sequence = self.next_sequence;
        self.next_sequence = sequence
            .checked_add(1)
            .context("replay recording sequence overflow")?;
        let message = RecordedGameMessage {
            tick,
            sequence,
            event,
        };
        let entry = ReplayJournalEntry::Message(message.clone());
        self.append_journal(entry.clone())?;
        self.messages.push(message);
        self.push_potg_entry(entry)?;
        Ok(())
    }

    pub fn record_events(&mut self, events: &[(u32, u64, GameEvent)]) -> Result<()> {
        for (tick, _, event) in events {
            self.record_event(*tick, event.clone())?;
        }
        Ok(())
    }

    pub fn maybe_anchor(&mut self, state: &GameState) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let interval_ticks = REPLAY_ANCHOR_INTERVAL_MS
            .div_ceil(state.properties.tick_duration_ms.max(1))
            .max(1);
        let should_anchor = self
            .anchors
            .last()
            .is_none_or(|anchor| state.tick.saturating_sub(anchor.tick) >= interval_ticks);
        if should_anchor {
            let anchor = ReplayAnchor {
                tick: state.tick,
                sequence: self.next_sequence.saturating_sub(1),
                state: state.clone(),
            };
            let entry = ReplayJournalEntry::Anchor(anchor.clone());
            self.append_journal(entry.clone())?;
            self.anchors.push(anchor);
            self.push_potg_entry(entry)?;
        }
        Ok(())
    }

    fn append_journal(&mut self, entry: ReplayJournalEntry) -> Result<()> {
        let cursor = self
            .journal_cursor
            .checked_add(1)
            .context("replay journal cursor overflow")?;
        self.pending_journal
            .push(ReplayJournalDelta { cursor, entry });
        self.journal_cursor = cursor;
        Ok(())
    }

    fn push_potg_entry(&mut self, entry: ReplayJournalEntry) -> Result<()> {
        self.potg_selection_ring_bytes = self
            .potg_selection_ring_bytes
            .saturating_add(entry.encoded_len()?);
        self.potg_selection_ring.push_back(entry);
        self.enforce_potg_selection_cap();
        Ok(())
    }

    fn enforce_potg_selection_cap(&mut self) {
        let entry_budget = self
            .potg_selection_ring_max_bytes
            .saturating_sub(POTG_SELECTION_RING_JSON_OVERHEAD_BYTES);
        while self.potg_selection_ring_bytes > entry_budget {
            let Some(evicted) = self.potg_selection_ring.pop_front() else {
                self.potg_selection_ring_bytes = 0;
                break;
            };
            self.potg_selection_ring_bytes = self
                .potg_selection_ring_bytes
                .saturating_sub(evicted.encoded_len().unwrap_or(0));
            self.potg_ring_truncated = true;
            self.potg_ring_evicted_through_tick =
                self.potg_ring_evicted_through_tick.max(evicted.tick());
        }

        // A replay tail must begin at a state anchor. Once the oldest anchor
        // is evicted, discard the now-unreplayable messages before the next
        // anchor too. This is still strictly oldest-first eviction.
        while matches!(
            self.potg_selection_ring.front(),
            Some(ReplayJournalEntry::Message(_))
        ) {
            let evicted = self
                .potg_selection_ring
                .pop_front()
                .expect("front was present");
            self.potg_selection_ring_bytes = self
                .potg_selection_ring_bytes
                .saturating_sub(evicted.encoded_len().unwrap_or(0));
            self.potg_ring_truncated = true;
            self.potg_ring_evicted_through_tick =
                self.potg_ring_evicted_through_tick.max(evicted.tick());
        }
    }

    pub fn potg_ring_truncated(&self) -> bool {
        self.potg_ring_truncated
    }

    pub fn potg_ring_evicted_seconds(&self, tick_duration_ms: u32) -> u64 {
        if !self.potg_ring_truncated {
            return 0;
        }
        let retained_tick = self.potg_selection_ring.front().map_or(
            self.potg_ring_evicted_through_tick,
            ReplayJournalEntry::tick,
        );
        u64::from(retained_tick.saturating_sub(self.activation_tick))
            .saturating_mul(u64::from(tick_duration_ms))
            .div_ceil(1_000)
    }

    pub fn potg_selection_ring_bytes(&self) -> usize {
        self.potg_selection_ring_bytes
            .saturating_add(POTG_SELECTION_RING_JSON_OVERHEAD_BYTES)
    }

    /// Builds the bounded recording used by the optional scorer. Structural
    /// validation, scoring, and selected-clip verification all run once in the
    /// budgeted worker; the partition actor only copies this bounded view.
    pub fn finish_potg_selection(
        &self,
        game_id: u32,
        final_state: &GameState,
    ) -> Result<Option<GameRecordingV1>> {
        if !self.enabled || self.potg_selection_ring.is_empty() {
            return Ok(None);
        }
        let mut anchors = Vec::new();
        let mut messages = Vec::new();
        for entry in &self.potg_selection_ring {
            match entry {
                ReplayJournalEntry::Anchor(anchor) => anchors.push(anchor.clone()),
                ReplayJournalEntry::Message(message) => messages.push(message.clone()),
            }
        }
        if anchors.is_empty() {
            return Ok(None);
        }
        let recording = GameRecordingV1 {
            format_version: GAME_RECORDING_FORMAT_VERSION,
            gameplay_version: GAMEPLAY_REPLAY_VERSION,
            game_id,
            visibility: ReplayVisibility::Public,
            anchors,
            messages,
            end_tick: final_state.tick,
            end_sync_hash: final_state.sync_hash(),
        };
        Ok(Some(recording))
    }

    pub fn finish(&self, game_id: u32, final_state: &GameState) -> Result<Option<GameRecordingV1>> {
        if !self.enabled {
            return Ok(None);
        }
        let recording = self
            .assemble(game_id, final_state)
            .expect("enabled replay recorder assembles a recording");
        recording.verify_end_hash()?;
        Ok(Some(recording))
    }

    pub(crate) fn assemble(
        &self,
        game_id: u32,
        final_state: &GameState,
    ) -> Option<GameRecordingV1> {
        self.enabled.then(|| GameRecordingV1 {
            format_version: GAME_RECORDING_FORMAT_VERSION,
            gameplay_version: GAMEPLAY_REPLAY_VERSION,
            game_id,
            visibility: ReplayVisibility::Public,
            anchors: self.anchors.clone(),
            messages: self.messages.clone(),
            end_tick: final_state.tick,
            end_sync_hash: final_state.sync_hash(),
        })
    }
}

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
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum CommandOutcome {
    Scheduled {
        command: GameCommandMessage,
    },
    Rejected {
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        command_id_client: Option<CommandId>,
    },
}

impl CommandOutcome {
    pub fn rejection_reason(&self) -> Option<&str> {
        match self {
            Self::Rejected { reason, .. } => Some(reason),
            Self::Scheduled { .. } => None,
        }
    }
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
            GameEvent::CommandRejected {
                command_id,
                reason,
                command_id_client,
                ..
            } => Ok((
                command_id,
                CommandOutcome::Rejected {
                    reason: reason.clone(),
                    command_id_client: command_id_client.clone(),
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
        if let GameEvent::CommandRejected {
            session_rejected_from: Some(from_sequence),
            ..
        } = &self.event.event
            && (*from_sequence == 0 || *from_sequence > identity.sequence)
        {
            bail!("command-session rejection fence does not cover its decision identity");
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct SessionCommandRejectionFence {
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub from_sequence: u64,
    pub reason: String,
}

impl SessionCommandRejectionFence {
    fn validate(&self) -> Result<()> {
        if self.from_sequence == 0 {
            bail!("command-session rejection fence must start at one");
        }
        if self.reason.is_empty() {
            bail!("command-session rejection fence reason cannot be empty");
        }
        Ok(())
    }

    pub fn covers(&self, sequence: u64) -> bool {
        sequence >= self.from_sequence
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SessionCommandOutcomes {
    /// Highest terminally resolved sequence with no unresolved gap below it.
    pub contiguous_through: u64,
    /// Recent exact results, including sparse outcomes above the watermark.
    /// Old contiguous results may be pruned only beyond the resend guarantee.
    pub outcomes: BTreeMap<u64, CommandOutcome>,
    /// Bounded terminal disposition used only when the sparse exact-result
    /// window is exhausted. Exact outcomes and the contiguous watermark win.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rejection_fence: Option<SessionCommandRejectionFence>,
}

impl SessionCommandOutcomes {
    fn validate(&self) -> Result<()> {
        if self.outcomes.contains_key(&0) {
            bail!("resolved client command sequence must start at one");
        }
        if let Some(fence) = &self.rejection_fence {
            fence.validate()?;
        }
        Ok(())
    }

    pub fn get(&self, sequence: u64) -> Option<&CommandOutcome> {
        self.outcomes.get(&sequence)
    }

    pub fn is_terminally_resolved(&self, sequence: u64) -> bool {
        sequence > 0
            && (sequence <= self.contiguous_through
                || self.outcomes.contains_key(&sequence)
                || self
                    .rejection_fence
                    .as_ref()
                    .is_some_and(|fence| fence.covers(sequence)))
    }

    pub fn rejection_fence_for(&self, sequence: u64) -> Option<&SessionCommandRejectionFence> {
        if sequence == 0
            || sequence <= self.contiguous_through
            || self.outcomes.contains_key(&sequence)
        {
            return None;
        }
        self.rejection_fence
            .as_ref()
            .filter(|fence| fence.covers(sequence))
    }

    fn sparse_window_is_full(&self, sequence: u64, max_results: usize) -> bool {
        max_results > 0
            && !self.outcomes.contains_key(&sequence)
            && self.rejection_fence_for(sequence).is_none()
            && Some(sequence) != self.contiguous_through.checked_add(1)
            && self
                .outcomes
                .range((
                    std::ops::Bound::Excluded(self.contiguous_through),
                    std::ops::Bound::Unbounded,
                ))
                .count()
                >= max_results
    }

    pub fn install_rejection_fence(
        &mut self,
        from_sequence: u64,
        reason: &str,
    ) -> Result<SessionCommandRejectionFence> {
        let proposed = SessionCommandRejectionFence {
            from_sequence,
            reason: reason.to_owned(),
        };
        proposed.validate()?;
        match &mut self.rejection_fence {
            Some(existing) => {
                if existing.reason != proposed.reason {
                    bail!("client command session has conflicting rejection fences");
                }
                existing.from_sequence = existing.from_sequence.min(proposed.from_sequence);
                Ok(existing.clone())
            }
            None => {
                self.rejection_fence = Some(proposed.clone());
                Ok(proposed)
            }
        }
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
        if self
            .rejection_fence
            .as_ref()
            .is_some_and(|fence| fence.covers(sequence))
        {
            bail!("client command identity is covered by its session rejection fence");
        }
        // Do not mutate state when the bounded sparse window is full.
        if self.sparse_window_is_full(sequence, max_results) {
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

    pub fn rejection_fence_for(
        &self,
        identity: &ClientCommandIdentityV2,
    ) -> Option<&SessionCommandRejectionFence> {
        self.sessions
            .get(&Self::session_key(identity))?
            .rejection_fence_for(identity.sequence)
    }

    pub fn sparse_window_is_full(
        &self,
        identity: &ClientCommandIdentityV2,
        max_results: usize,
    ) -> bool {
        self.sessions
            .get(&Self::session_key(identity))
            .is_some_and(|session| session.sparse_window_is_full(identity.sequence, max_results))
    }

    pub fn install_rejection_fence(
        &mut self,
        identity: &ClientCommandIdentityV2,
        from_sequence: u64,
        reason: &str,
    ) -> Result<SessionCommandRejectionFence> {
        validate_client_command_identity(identity)?;
        if from_sequence == 0 || from_sequence > identity.sequence {
            bail!("command-session rejection fence does not cover its command identity");
        }
        let key = Self::session_key(identity);
        let Some(session) = self.sessions.get_mut(&key) else {
            bail!("cannot fence an unrecorded client command session");
        };
        session.install_rejection_fence(from_sequence, reason)
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
    /// Replay enablement, sequence, and append-only journal cursor. Complete
    /// history is hydrated from the adjacent partition-local journal before
    /// an actor is constructed.
    #[serde(default)]
    pub replay_recording: ReplayRecordingState,
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
        let replay_recording = ReplayRecordingState::checkpoint_seed(&game_state);
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
            replay_recording,
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
        for session in self.resolved_client_commands.sessions.values() {
            session.validate()?;
        }
        self.game_state.validate_boost_invariants()?;
        if self.game_state.is_stress_test && self.replay_recording.is_enabled() {
            bail!("stress-test recovery envelope cannot contain a replay recording");
        }
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
            command_id_client: None,
        }
    }

    #[test]
    fn legacy_digest_only_replay_journal_reference_remains_readable() {
        let reference: ReplayJournalReferenceV1 = serde_json::from_value(serde_json::json!({
            "schema_version": REPLAY_JOURNAL_REFERENCE_SCHEMA_VERSION,
            "game_id": 17,
            "journal_cursor": 4,
            "next_sequence": 3,
            "recording_sha256": "00".repeat(32),
            "recording_bytes": 42
        }))
        .unwrap();
        assert_eq!(reference.end_tick, None);
        assert_eq!(reference.end_sync_hash, None);
        assert_eq!(reference.recording_bytes, Some(42));
        reference.validate().unwrap();
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
    fn old_contiguous_results_do_not_consume_sparse_capacity() {
        let mut outcomes = SessionCommandOutcomes::default();
        for sequence in 1..=DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION as u64 {
            outcomes
                .record(
                    sequence,
                    rejected("contiguous"),
                    DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION,
                )
                .unwrap();
        }

        outcomes
            .record(
                DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION as u64 + 2,
                rejected("first sparse"),
                DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION,
            )
            .unwrap();
        assert_eq!(
            outcomes.outcomes.len(),
            DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION
        );
        assert_eq!(
            outcomes.contiguous_through,
            DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION as u64
        );
        assert!(
            outcomes
                .outcomes
                .contains_key(&(DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION as u64 + 2))
        );
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
    fn default_sparse_window_fails_without_mutation_then_recovers() {
        assert_eq!(DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION, 128);
        let mut outcomes = SessionCommandOutcomes::default();
        for sequence in 2..=(DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION as u64 + 1) {
            outcomes
                .record(
                    sequence,
                    rejected("resolved"),
                    DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION,
                )
                .unwrap();
        }
        assert_eq!(
            outcomes.outcomes.len(),
            DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION
        );
        let before = outcomes.clone();
        assert!(
            outcomes
                .record(
                    DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION as u64 + 2,
                    rejected("overflow"),
                    DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION,
                )
                .is_err()
        );
        assert_eq!(outcomes, before);

        outcomes
            .record(
                1,
                rejected("gap closed"),
                DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION,
            )
            .unwrap();
        outcomes
            .record(
                DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION as u64 + 2,
                rejected("accepted after gap closed"),
                DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION,
            )
            .unwrap();
        assert_eq!(
            outcomes.contiguous_through,
            DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION as u64 + 2
        );
        assert_eq!(
            outcomes.outcomes.len(),
            DEFAULT_MAX_RECORDED_OUTCOMES_PER_SESSION
        );
        assert!(outcomes.is_terminally_resolved(1));
    }

    #[test]
    fn rejection_fence_is_constant_size_and_exact_results_take_precedence() {
        let mut outcomes = SessionCommandOutcomes::default();
        outcomes.record(10, rejected("exact ten"), 2).unwrap();
        outcomes.record(12, rejected("exact twelve"), 2).unwrap();

        let fence = outcomes
            .install_rejection_fence(11, SPARSE_COMMAND_WINDOW_REJECTION_REASON)
            .unwrap();
        assert_eq!(fence.from_sequence, 11);
        assert_eq!(
            outcomes.get(12),
            Some(&rejected("exact twelve")),
            "an exact result must win even when it overlaps the fence"
        );
        assert!(
            outcomes.rejection_fence_for(12).is_none(),
            "the reconnect protocol must not replace an exact result with the fence"
        );
        assert_eq!(
            outcomes.rejection_fence_for(11),
            Some(&SessionCommandRejectionFence {
                from_sequence: 11,
                reason: SPARSE_COMMAND_WINDOW_REJECTION_REASON.to_owned(),
            })
        );
        assert_eq!(
            outcomes.rejection_fence_for(u64::MAX),
            outcomes.rejection_fence_for(11),
            "one fence resolves an unbounded tail without adding exact entries"
        );
        assert_eq!(outcomes.outcomes.len(), 2);
    }

    #[test]
    fn contiguous_watermark_takes_precedence_over_rejection_fence() {
        let mut outcomes = SessionCommandOutcomes::default();
        outcomes.record(2, rejected("exact two"), 2).unwrap();
        outcomes
            .install_rejection_fence(2, SPARSE_COMMAND_WINDOW_REJECTION_REASON)
            .unwrap();
        outcomes.record(1, rejected("gap closed"), 2).unwrap();

        assert_eq!(outcomes.contiguous_through, 2);
        assert!(outcomes.rejection_fence_for(1).is_none());
        assert!(outcomes.rejection_fence_for(2).is_none());
        assert!(outcomes.rejection_fence_for(3).is_some());
    }

    #[test]
    fn recovery_envelope_rejects_a_malformed_persisted_fence() {
        let mut resolved = ResolvedCommandState::default();
        resolved.sessions.insert(
            "9:session-a".into(),
            SessionCommandOutcomes {
                rejection_fence: Some(SessionCommandRejectionFence {
                    from_sequence: 0,
                    reason: String::new(),
                }),
                ..SessionCommandOutcomes::default()
            },
        );
        let state = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, Some(7), 0);
        let envelope =
            RecoveryEnvelopeV2::new(4, 4, state, "0-0".into(), resolved, 0, 0, 5, "token".into());

        assert!(envelope.validate().is_err());
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

    #[test]
    fn recovery_preserves_mid_boost_state_and_exact_next_quantum() {
        let game_id = 45;
        let start_ms = 1_000_000;
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            Some(0xB0057),
            start_ms,
        );
        state.status = common::GameStatus::Started { server_id: 7 };
        state.rng = None;
        let snake_id = state
            .add_player(9, Some("boost-player".into()))
            .unwrap()
            .snake_id;
        let pad_position = state.arena.boost_pads[0].position;
        let pad_charge_ms = state.arena.boost_pads[0].charge_ms;
        state.arena.snakes[snake_id as usize].body = vec![
            pad_position,
            common::Position {
                x: pad_position.x - 3,
                y: pad_position.y,
            },
        ];
        state.arena.snakes[snake_id as usize].direction = Direction::Right;
        state.validate_boost_invariants().unwrap();

        let mut original = common::GameEngine::new_from_state(game_id, state);

        // First 50 ms quantum: normal speed accrues half a cell and the head
        // stores one packet without activating.
        original.run_until(start_ms + 500 + 50).unwrap();
        let collected = &original.get_committed_state().arena.snakes[snake_id as usize];
        assert_eq!(collected.boost().charge_ms, pad_charge_ms);
        assert!(!collected.boost().active);
        assert_eq!(collected.movement_credit(), 50_000);
        assert!(
            original.get_committed_state().arena.boost_pads[0]
                .respawn_at_tick
                .is_some()
        );

        original
            .process_command(GameCommandMessage {
                command_id_client: CommandId {
                    tick: original.current_tick(),
                    user_id: 9,
                    sequence_number: 1,
                },
                command_id_server: None,
                command: GameCommand::ActivateBoost { snake_id },
            })
            .unwrap();

        // Second quantum activates before credit, reserves exactly 50 ms of
        // fuel, moves once, and leaves a nonzero residual. This is the state a
        // successor must restore without adding or losing a cell.
        original.run_until(start_ms + 500 + 100).unwrap();
        let checkpoint_snake = &original.get_committed_state().arena.snakes[snake_id as usize];
        assert!(checkpoint_snake.boost().active);
        assert_eq!(
            checkpoint_snake.boost().charge_ms,
            pad_charge_ms - common::BOOST_TICK_INTERVAL_MS
        );
        assert_eq!(
            checkpoint_snake.speed_milli(),
            original
                .get_committed_state()
                .properties
                .boost
                .as_ref()
                .unwrap()
                .speed_milli
        );
        assert_eq!(checkpoint_snake.movement_credit(), 25_000);
        let checkpoint_head = *checkpoint_snake.head().unwrap();

        let envelope = RecoveryEnvelopeV2::new(
            game_id,
            4,
            original.get_committed_state().clone(),
            "123-4".into(),
            ResolvedCommandState::default(),
            original.next_server_command_sequence(),
            91,
            start_ms + 600,
            "token".into(),
        );
        let decoded: RecoveryEnvelopeV2 =
            serde_json::from_slice(&serde_json::to_vec(&envelope).unwrap()).unwrap();
        decoded.validate().unwrap();
        let mut recovered = common::GameEngine::try_new_from_state_with_command_counter(
            game_id,
            decoded.game_state,
            decoded.next_server_command_sequence,
        )
        .unwrap();

        assert_eq!(
            serde_json::to_value(original.get_committed_state()).unwrap(),
            serde_json::to_value(recovered.get_committed_state()).unwrap(),
            "checkpoint round trip must preserve fuel, speed, credit, and pad cooldown"
        );

        // One more 50 ms quantum must produce the same funded movement on the
        // incumbent and recovered successor.
        original.run_until(start_ms + 500 + 150).unwrap();
        recovered.run_until(start_ms + 500 + 150).unwrap();
        assert_eq!(
            serde_json::to_value(original.get_committed_state()).unwrap(),
            serde_json::to_value(recovered.get_committed_state()).unwrap()
        );
        let recovered_snake = &recovered.get_committed_state().arena.snakes[snake_id as usize];
        assert!(recovered_snake.boost().active);
        assert_eq!(
            recovered_snake.boost().charge_ms,
            pad_charge_ms - 2 * common::BOOST_TICK_INTERVAL_MS
        );
        assert_eq!(recovered_snake.movement_credit(), 0);
        assert_ne!(*recovered_snake.head().unwrap(), checkpoint_head);
    }

    #[test]
    fn replay_recording_survives_checkpoint_and_reconstructs_authority() {
        let game_id = 44;
        let mut activation = GameState::new(
            40,
            40,
            common::GameType::Solo,
            common::QueueMode::Quickmatch,
            Some(7),
            0,
        );
        activation.add_player(9, Some("recorder".into())).unwrap();
        activation.status = common::GameStatus::Started { server_id: 1 };
        let mut engine = common::GameEngine::new_from_state(game_id, activation.clone());
        let mut recorder = ReplayRecordingState::new(&activation);

        for now_ms in [100, 200, 300, 400, 500] {
            let events = engine.run_until(now_ms).unwrap();
            recorder.record_events(&events).unwrap();
            recorder.maybe_anchor(engine.get_committed_state()).unwrap();
        }

        let durable_journal = recorder.pending_journal.clone();
        let encoded = serde_json::to_vec(&recorder.checkpoint_view()).unwrap();
        let mut recovered: ReplayRecordingState = serde_json::from_slice(&encoded).unwrap();
        recovered
            .hydrate_journal(durable_journal, engine.get_committed_state())
            .unwrap();
        let recording = recovered
            .finish(game_id, engine.get_committed_state())
            .unwrap()
            .expect("production game should be recorded");
        recording.verify_end_hash().unwrap();
    }

    #[test]
    fn trusted_stress_marker_excludes_recording() {
        let mut state = GameState::new(
            40,
            40,
            common::GameType::Solo,
            common::QueueMode::Quickmatch,
            Some(7),
            0,
        );
        state.is_stress_test = true;
        let recorder = ReplayRecordingState::new(&state);
        assert!(!recorder.is_enabled());
        assert!(recorder.finish(1, &state).unwrap().is_none());
    }

    #[test]
    fn server_test_runtime_excludes_recording_without_mutating_process_env() {
        let state = GameState::new(
            40,
            40,
            common::GameType::Solo,
            common::QueueMode::Quickmatch,
            Some(7),
            0,
        );
        assert!(replay_recording_allowed_from_lookup(&state, |_| None));
        for marker in ["1", "true", "YES", " on "] {
            assert!(!replay_recording_allowed_from_lookup(&state, |name| {
                (name == "SNAKETRON_TEST_MODE").then(|| marker.to_owned())
            }));
        }

        let mut stress = state;
        stress.is_stress_test = true;
        assert!(!replay_recording_allowed_from_lookup(&stress, |_| None));
    }

    #[test]
    fn checkpoint_metadata_stays_bounded_for_long_recordings() {
        let mut state = GameState::new(
            40,
            40,
            common::GameType::Solo,
            common::QueueMode::Quickmatch,
            Some(7),
            0,
        );
        state.add_player(9, Some("recorder".into())).unwrap();
        state.status = common::GameStatus::Started { server_id: 1 };
        let mut recorder = ReplayRecordingState::new(&state);
        let baseline = serde_json::to_vec(&recorder.checkpoint_view()).unwrap();
        recorder
            .mark_journal_persisted(recorder.journal_cursor())
            .unwrap();

        for tick in 1..=160_000 {
            recorder
                .record_event(
                    tick,
                    GameEvent::ScoreUpdated {
                        snake_id: 0,
                        score: tick,
                    },
                )
                .unwrap();
            // Model the ordinary periodic fenced checkpoint: pending memory is
            // bounded by recent deltas while the Redis journal keeps history.
            if tick % 20 == 0 {
                recorder
                    .mark_journal_persisted(recorder.journal_cursor())
                    .unwrap();
            }
        }

        let encoded = serde_json::to_vec(&recorder.checkpoint_view()).unwrap();
        let logical_archive_bytes = serde_json::to_vec(&recorder.messages).unwrap().len();
        assert!(
            logical_archive_bytes > 8 * 1024 * 1024,
            "fixture must exercise a recording above the public one-shot limit"
        );
        assert!(encoded.len() <= baseline.len() + 64);
        let text = String::from_utf8(encoded).unwrap();
        assert!(!text.contains("ScoreUpdated"));
        assert!(!text.contains("anchors"));
        assert!(!text.contains("messages"));
    }

    #[test]
    fn potg_ring_evicts_oldest_without_shortening_durable_archive() {
        let game_id = 45;
        let mut state = GameState::new(
            40,
            40,
            common::GameType::Solo,
            common::QueueMode::Quickmatch,
            Some(7),
            0,
        );
        state.add_player(9, Some("recorder".into())).unwrap();
        state.status = common::GameStatus::Started { server_id: 1 };
        let mut recorder = ReplayRecordingState::new(&state);
        let one_anchor_budget = recorder
            .potg_selection_ring_bytes
            .saturating_add(POTG_SELECTION_RING_JSON_OVERHEAD_BYTES)
            .saturating_add(64);
        recorder.potg_selection_ring_max_bytes = one_anchor_budget;

        let interval_ticks =
            REPLAY_ANCHOR_INTERVAL_MS.div_ceil(state.properties.tick_duration_ms.max(1));
        state.tick = state.tick.saturating_add(interval_ticks);
        recorder.maybe_anchor(&state).unwrap();

        assert!(recorder.potg_ring_truncated());
        assert!(recorder.potg_selection_ring_bytes() <= one_anchor_budget);
        assert_eq!(recorder.anchors.len(), 2, "full archive keeps both anchors");
        let archive = recorder.finish(game_id, &state).unwrap().unwrap();
        assert_eq!(archive.anchors.len(), 2);
        archive.verify_end_hash().unwrap();
        let selection = recorder
            .finish_potg_selection(game_id, &state)
            .unwrap()
            .unwrap();
        assert_eq!(selection.anchors.len(), 1);
        assert_eq!(selection.anchors[0].tick, state.tick);
        selection.verify_end_hash().unwrap();
        assert!(serde_json::to_vec(&selection).unwrap().len() <= one_anchor_budget);
        assert_eq!(
            recorder.potg_ring_evicted_seconds(state.properties.tick_duration_ms),
            u64::from(REPLAY_ANCHOR_INTERVAL_MS).div_ceil(1_000)
        );
    }

    #[test]
    fn recovery_rebuilds_selection_ring_from_complete_archive() {
        let mut activation = GameState::new(
            40,
            40,
            common::GameType::Solo,
            common::QueueMode::Quickmatch,
            Some(7),
            0,
        );
        activation.add_player(9, Some("recorder".into())).unwrap();
        activation.status = common::GameStatus::Started { server_id: 1 };
        let mut recorder = ReplayRecordingState::new(&activation);
        let interval_ticks =
            REPLAY_ANCHOR_INTERVAL_MS.div_ceil(activation.properties.tick_duration_ms.max(1));
        let mut checkpoint = activation.clone();
        checkpoint.tick = checkpoint.tick.saturating_add(interval_ticks);
        recorder.maybe_anchor(&checkpoint).unwrap();
        let durable_journal = recorder.pending_journal.clone();
        let encoded = serde_json::to_vec(&recorder.checkpoint_view()).unwrap();
        assert!(!String::from_utf8_lossy(&encoded).contains("potg_selection_ring"));

        let mut recovered: ReplayRecordingState = serde_json::from_slice(&encoded).unwrap();
        assert!(recovered.potg_selection_ring.is_empty());
        recovered
            .hydrate_journal(durable_journal, &checkpoint)
            .unwrap();

        assert!(!recovered.potg_ring_truncated());
        let selection = recovered
            .finish_potg_selection(46, &checkpoint)
            .unwrap()
            .unwrap();
        assert_eq!(selection.anchors.len(), 2);
        assert_eq!(selection.anchors[0].tick, activation.tick);
        assert_eq!(selection.anchors[1].tick, checkpoint.tick);
        selection.verify_end_hash().unwrap();
    }
}
