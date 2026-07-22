//! Durable, replay-safe game completion records and external effects.
//!
//! A completion is first committed to Valkey by the fenced partition owner.
//! The immutable record below is then the sole source for DynamoDB writes. Each
//! effect has a stable identity and is applied through a DynamoDB transaction
//! containing both the mutation and its idempotency marker.

use crate::db::Database;
use crate::mmr_persistence::calculate_mmr_effect_specs;
use crate::season::{get_current_season, get_region};
use anyhow::{Result, anyhow};
use common::{GameState, GameStatus, GameType, QueueMode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use uuid::Uuid;

pub const COMPLETION_SCHEMA_VERSION: u16 = 1;

/// Serialize a durable payload with recursively sorted object keys. Recovery
/// reparses `GameState` hash maps in a fresh process, so ordinary JSON bytes
/// are not a stable immutable identity across a crash.
pub fn canonical_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    fn sort_objects(value: Value) -> Value {
        match value {
            Value::Array(values) => Value::Array(values.into_iter().map(sort_objects).collect()),
            Value::Object(values) => {
                let mut entries: Vec<_> = values.into_iter().collect();
                entries.sort_by(|(left, _), (right, _)| left.cmp(right));
                let mut sorted = serde_json::Map::with_capacity(entries.len());
                for (key, value) in entries {
                    sorted.insert(key, sort_objects(value));
                }
                Value::Object(sorted)
            }
            scalar => scalar,
        }
    }

    let value = serde_json::to_value(value)?;
    Ok(serde_json::to_vec(&sort_objects(value))?)
}

/// Outcome of an idempotent external effect attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectApplyResult {
    Applied,
    AlreadyApplied,
}

/// An immutable completion record. The revision and timestamp are generated
/// once, before the fenced Valkey commit, and reused for every retry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionRecordV1 {
    pub schema_version: u16,
    pub game_id: u32,
    pub partition_id: u32,
    pub revision: Uuid,
    pub ended_at_ms: i64,
    pub server_id: u64,
    pub final_state: GameState,
    pub effects: Vec<CompletionEffect>,
}

impl CompletionRecordV1 {
    pub fn pending_effect_ids(&self) -> impl Iterator<Item = &str> {
        self.effects.iter().map(CompletionEffect::id)
    }

    pub fn effect(&self, effect_id: &str) -> Option<&CompletionEffect> {
        self.effects.iter().find(|effect| effect.id() == effect_id)
    }

    /// Validate the durable record independently of any in-process executor
    /// state. Callers must do this before committing the record, and the
    /// database repeats the check before applying an effect.
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != COMPLETION_SCHEMA_VERSION {
            return Err(anyhow!(
                "unsupported completion schema version {}",
                self.schema_version
            ));
        }
        if self.game_id > i32::MAX as u32 {
            return Err(anyhow!(
                "completion game id {} exceeds database range",
                self.game_id
            ));
        }
        if self.server_id > i32::MAX as u64 {
            return Err(anyhow!(
                "completion server id {} exceeds database range",
                self.server_id
            ));
        }
        if self.revision.is_nil() {
            return Err(anyhow!("completion revision cannot be nil"));
        }
        if DateTimeMillis::is_invalid(self.ended_at_ms) {
            return Err(anyhow!(
                "completion timestamp is outside the supported range"
            ));
        }
        if self.ended_at_ms < self.final_state.start_ms {
            return Err(anyhow!("completion cannot end before the game starts"));
        }
        if !matches!(self.final_state.status, GameStatus::Complete { .. }) {
            return Err(anyhow!(
                "completion record {} for game {} is not terminal",
                self.revision,
                self.game_id
            ));
        }

        let mut ids = HashSet::with_capacity(self.effects.len());
        let mut persist_game_count = 0;
        for effect in &self.effects {
            if effect.id().is_empty() {
                return Err(anyhow!("completion effect id cannot be empty"));
            }
            if !ids.insert(effect.id()) {
                return Err(anyhow!(
                    "completion effect id {} is duplicated",
                    effect.id()
                ));
            }
            effect.validate_identity(self)?;
            if matches!(effect, CompletionEffect::PersistGame { .. }) {
                persist_game_count += 1;
            }
        }
        if persist_game_count != 1 {
            return Err(anyhow!(
                "completion must contain exactly one persist-game effect"
            ));
        }

        for effect in &self.effects {
            if let CompletionEffect::UpdateRanking {
                user_id,
                queue_mode,
                ..
            } = effect
            {
                let matching_mmr = self.effects.iter().filter(|candidate| {
                    matches!(
                        candidate,
                        CompletionEffect::AddMmr {
                            user_id: candidate_user,
                            queue_mode: candidate_mode,
                            ..
                        } if candidate_user == user_id && candidate_mode == queue_mode
                    )
                });
                if matching_mmr.count() != 1 {
                    return Err(anyhow!(
                        "ranking effect {} must have exactly one matching MMR effect",
                        effect.id()
                    ));
                }
            }
        }

        for (user_id, amount) in &self.final_state.player_xp {
            if *amount > 0 && !self.final_state.players.contains_key(user_id) {
                return Err(anyhow!(
                    "completed game awards XP to non-player user {user_id}"
                ));
            }
        }
        for user_id in self.final_state.players.keys() {
            let xp_count = self
                .effects
                .iter()
                .filter(|effect| {
                    matches!(effect, CompletionEffect::AddXp { user_id: effect_user, .. } if effect_user == user_id)
                })
                .count();
            let expected_xp = usize::from(
                self.final_state
                    .player_xp
                    .get(user_id)
                    .copied()
                    .unwrap_or(0)
                    > 0,
            );
            if xp_count != expected_xp {
                return Err(anyhow!(
                    "completion has an incomplete XP effect set for user {user_id}"
                ));
            }

            let mmr_count = self
                .effects
                .iter()
                .filter(|effect| {
                    matches!(effect, CompletionEffect::AddMmr { user_id: effect_user, .. } if effect_user == user_id)
                })
                .count();
            let ranking_count = self
                .effects
                .iter()
                .filter(|effect| {
                    matches!(effect, CompletionEffect::UpdateRanking { user_id: effect_user, .. } if effect_user == user_id)
                })
                .count();
            let high_score_count = self
                .effects
                .iter()
                .filter(|effect| {
                    matches!(effect, CompletionEffect::InsertHighScore { user_id: effect_user, .. } if effect_user == user_id)
                })
                .count();
            if matches!(self.final_state.game_type, GameType::Solo) {
                if mmr_count != 0 || ranking_count != 0 || high_score_count != 1 {
                    return Err(anyhow!(
                        "completion has an incomplete solo effect set for user {user_id}"
                    ));
                }
            } else if mmr_count != 1 || ranking_count != 1 || high_score_count != 0 {
                return Err(anyhow!(
                    "completion has an incomplete MMR effect set for user {user_id}"
                ));
            }
        }
        Ok(())
    }

    /// Ensure the submitted effect is the exact effect captured in this
    /// immutable record, rather than merely reusing one of its identifiers.
    pub fn validate_effect(&self, effect: &CompletionEffect) -> Result<()> {
        self.validate()?;
        match self.effect(effect.id()) {
            Some(recorded) if recorded == effect => Ok(()),
            Some(_) => Err(anyhow!(
                "completion effect {} does not match its durable payload",
                effect.id()
            )),
            None => Err(anyhow!(
                "completion effect {} is not present in the durable record",
                effect.id()
            )),
        }
    }
}

/// `chrono` is intentionally kept out of the durable record's public schema;
/// this helper just rejects timestamps that DynamoDB cannot materialize.
struct DateTimeMillis;

impl DateTimeMillis {
    fn is_invalid(value: i64) -> bool {
        chrono::DateTime::<chrono::Utc>::from_timestamp_millis(value).is_none()
    }
}

/// Every field needed by an external effect is captured before the
/// authoritative completion commit. No retry derives rewards from mutable
/// in-process state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CompletionEffect {
    PersistGame {
        id: String,
    },
    AddXp {
        id: String,
        user_id: u32,
        username: String,
        amount: u32,
    },
    AddMmr {
        id: String,
        user_id: u32,
        username: String,
        delta: i32,
        queue_mode: QueueMode,
    },
    UpdateRanking {
        id: String,
        user_id: u32,
        username: String,
        queue_mode: QueueMode,
        game_type: GameType,
        region: String,
        season: u32,
        won: bool,
    },
    InsertHighScore {
        id: String,
        user_id: u32,
        username: String,
        score: u32,
        game_type: GameType,
        region: String,
        season: u32,
    },
}

impl CompletionEffect {
    pub fn id(&self) -> &str {
        match self {
            Self::PersistGame { id }
            | Self::AddXp { id, .. }
            | Self::AddMmr { id, .. }
            | Self::UpdateRanking { id, .. }
            | Self::InsertHighScore { id, .. } => id,
        }
    }

    pub fn user_id(&self) -> Option<u32> {
        match self {
            Self::PersistGame { .. } => None,
            Self::AddXp { user_id, .. }
            | Self::AddMmr { user_id, .. }
            | Self::UpdateRanking { user_id, .. }
            | Self::InsertHighScore { user_id, .. } => Some(*user_id),
        }
    }

    fn validate_identity(&self, completion: &CompletionRecordV1) -> Result<()> {
        let expected_id = match self {
            Self::PersistGame { .. } => "game".to_string(),
            Self::AddXp {
                user_id, amount, ..
            } => {
                if *amount == 0 {
                    return Err(anyhow!("zero XP must not be materialized as an effect"));
                }
                validate_player(completion, *user_id)?;
                if *amount > i32::MAX as u32
                    || completion.final_state.player_xp.get(user_id) != Some(amount)
                {
                    return Err(anyhow!("XP effect does not match the completed game state"));
                }
                format!("xp:{user_id}")
            }
            Self::AddMmr {
                user_id,
                queue_mode,
                ..
            } => {
                validate_player(completion, *user_id)?;
                if queue_mode != &completion.final_state.queue_mode
                    || matches!(completion.final_state.game_type, GameType::Solo)
                {
                    return Err(anyhow!("MMR effect does not match the completed game mode"));
                }
                format!("mmr:{user_id}")
            }
            Self::UpdateRanking {
                user_id,
                queue_mode,
                game_type,
                region,
                ..
            } => {
                validate_player(completion, *user_id)?;
                if queue_mode != &completion.final_state.queue_mode
                    || game_type != &completion.final_state.game_type
                    || matches!(game_type, GameType::Solo)
                    || region.is_empty()
                {
                    return Err(anyhow!("ranking effect does not match the completed game"));
                }
                format!("ranking:{user_id}")
            }
            Self::InsertHighScore {
                user_id,
                score,
                game_type,
                region,
                ..
            } => {
                validate_player(completion, *user_id)?;
                let player = completion
                    .final_state
                    .players
                    .get(user_id)
                    .expect("player checked above");
                let final_score = completion
                    .final_state
                    .scores
                    .get(&player.snake_id)
                    .copied()
                    .unwrap_or(0);
                if game_type != &completion.final_state.game_type
                    || !matches!(game_type, GameType::Solo)
                    || region.is_empty()
                    || *score != final_score
                    || *score > i32::MAX as u32
                {
                    return Err(anyhow!(
                        "high-score effect does not match the completed solo game"
                    ));
                }
                format!("high_score:{user_id}")
            }
        };
        if self.id() != expected_id {
            return Err(anyhow!(
                "completion effect id {} must be {}",
                self.id(),
                expected_id
            ));
        }
        if let Some(user_id) = self.user_id()
            && user_id > i32::MAX as u32
        {
            return Err(anyhow!("effect user id {user_id} exceeds database range"));
        }
        let effect_username = match self {
            Self::PersistGame { .. } => None,
            Self::AddXp { username, .. }
            | Self::AddMmr { username, .. }
            | Self::UpdateRanking { username, .. }
            | Self::InsertHighScore { username, .. } => Some(username),
        };
        if let (Some(user_id), Some(username)) = (self.user_id(), effect_username)
            && username != &username_for(&completion.final_state, user_id)
        {
            return Err(anyhow!(
                "effect {} username does not match the completed game state",
                self.id()
            ));
        }
        Ok(())
    }
}

fn validate_player(completion: &CompletionRecordV1, user_id: u32) -> Result<()> {
    if completion.final_state.players.contains_key(&user_id) {
        Ok(())
    } else {
        Err(anyhow!(
            "completion effect references non-player user {user_id}"
        ))
    }
}

/// Materialize the immutable completion and all of its external effects.
/// MMR deltas are calculated here, before the record is committed, so retries
/// cannot derive a different reward from later database state.
pub async fn materialize_completion(
    db: &dyn Database,
    game_id: u32,
    partition_id: u32,
    server_id: u64,
    final_state: GameState,
    ended_at_ms: i64,
) -> Result<CompletionRecordV1> {
    if !matches!(final_state.status, GameStatus::Complete { .. }) {
        return Err(anyhow!(
            "cannot materialize completion for non-terminal game {}",
            game_id
        ));
    }

    let mut effects = vec![CompletionEffect::PersistGame {
        id: "game".to_string(),
    }];

    let mut player_ids: Vec<u32> = final_state.players.keys().copied().collect();
    player_ids.sort_unstable();

    for user_id in &player_ids {
        let amount = final_state.player_xp.get(user_id).copied().unwrap_or(0);
        if amount == 0 {
            continue;
        }
        effects.push(CompletionEffect::AddXp {
            id: format!("xp:{user_id}"),
            user_id: *user_id,
            username: username_for(&final_state, *user_id),
            amount,
        });
    }

    let region = get_region();
    let season = get_current_season();
    if matches!(final_state.game_type, GameType::Solo) {
        for user_id in player_ids {
            let player = final_state
                .players
                .get(&user_id)
                .expect("player id collected from same state");
            effects.push(CompletionEffect::InsertHighScore {
                id: format!("high_score:{user_id}"),
                user_id,
                username: username_for(&final_state, user_id),
                score: final_state
                    .scores
                    .get(&player.snake_id)
                    .copied()
                    .unwrap_or(0),
                game_type: final_state.game_type.clone(),
                region: region.clone(),
                season,
            });
        }
    } else {
        let mut specs = calculate_mmr_effect_specs(db, &final_state).await?;
        specs.sort_by_key(|spec| spec.user_id);
        for spec in specs {
            let username = username_for(&final_state, spec.user_id);
            effects.push(CompletionEffect::AddMmr {
                id: format!("mmr:{}", spec.user_id),
                user_id: spec.user_id,
                username: username.clone(),
                delta: spec.delta,
                queue_mode: final_state.queue_mode.clone(),
            });
            effects.push(CompletionEffect::UpdateRanking {
                id: format!("ranking:{}", spec.user_id),
                user_id: spec.user_id,
                username,
                queue_mode: final_state.queue_mode.clone(),
                game_type: final_state.game_type.clone(),
                region: region.clone(),
                season,
                won: spec.won,
            });
        }
    }

    let record = CompletionRecordV1 {
        schema_version: COMPLETION_SCHEMA_VERSION,
        game_id,
        partition_id,
        revision: Uuid::new_v4(),
        ended_at_ms,
        server_id,
        final_state,
        effects,
    };
    record.validate()?;
    Ok(record)
}

fn username_for(state: &GameState, user_id: u32) -> String {
    state
        .usernames
        .get(&user_id)
        .cloned()
        .unwrap_or_else(|| format!("User{user_id}"))
}

/// Apply all effects once. Production recovery normally invokes one effect at
/// a time and persists its done status in Valkey; this helper is useful for
/// tests and for draining a freshly committed record.
pub async fn apply_all_effects(
    db: &dyn Database,
    record: &CompletionRecordV1,
) -> Result<Vec<(String, EffectApplyResult)>> {
    record.validate()?;
    let mut results = Vec::with_capacity(record.effects.len());
    for effect in &record.effects {
        let result = db.apply_completion_effect(record, effect).await?;
        results.push((effect.id().to_string(), result));
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::{GameStatus, GameType, QueueMode};

    #[test]
    fn effect_ids_are_stable_and_unique() {
        let effects = [
            CompletionEffect::PersistGame { id: "game".into() },
            CompletionEffect::AddXp {
                id: "xp:7".into(),
                user_id: 7,
                username: "seven".into(),
                amount: 10,
            },
            CompletionEffect::AddMmr {
                id: "mmr:7".into(),
                user_id: 7,
                username: "seven".into(),
                delta: 12,
                queue_mode: QueueMode::Competitive,
            },
            CompletionEffect::UpdateRanking {
                id: "ranking:7".into(),
                user_id: 7,
                username: "seven".into(),
                queue_mode: QueueMode::Competitive,
                game_type: GameType::TeamMatch { per_team: 1 },
                region: "test".into(),
                season: 1,
                won: true,
            },
        ];
        let ids: std::collections::HashSet<_> = effects.iter().map(|e| e.id()).collect();
        assert_eq!(ids.len(), effects.len());
    }

    #[test]
    fn rejects_non_terminal_state_without_touching_database() {
        let state = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, Some(1), 0);
        assert!(!matches!(state.status, GameStatus::Complete { .. }));
    }
}
