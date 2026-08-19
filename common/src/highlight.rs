//! Durable replay archives and deterministic Play-of-the-Game selection.

use crate::{
    DeathCause, Direction, GameEvent, GameEventMessage, GameState, GameType, Position, TeamGoal,
    advance_and_apply_replicated_message,
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

/// JSON has no lossless integer representation above 2^53. Replay hashes are
/// arbitrary u64 values and cross a JavaScript boundary, so new payloads use
/// decimal strings while the reader continues accepting legacy JSON numbers.
mod json_u64_string {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Repr {
        String(String),
        Number(u64),
    }

    pub fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Repr::deserialize(deserializer)? {
            Repr::Number(value) => Ok(value),
            Repr::String(value) => value
                .parse::<u64>()
                .map_err(|_| D::Error::custom("invalid decimal u64 string")),
        }
    }
}

pub const GAME_RECORDING_FORMAT_VERSION: u32 = 1;
pub const HIGHLIGHT_CLIP_FORMAT_VERSION: u32 = 1;
/// Simulation compatibility gate shared by archives and browser playback.
/// Must equal `WS_PROTOCOL_VERSION` / `GAMEPLAY_PROTOCOL_VERSION`: the browser
/// rejects any clip whose `gameplay_version` differs from the protocol it
/// speaks, so a clip written under a stale value is silently unplayable.
pub const GAMEPLAY_REPLAY_VERSION: u16 = 12;
/// Completion scoring is deliberately bounded. Longer archives remain fully
/// replayable from S3, but an unexpectedly long or event-dense match degrades
/// to the no-highlight/banner path instead of monopolizing its executor.
pub const MAX_HIGHLIGHT_SELECTION_TICKS: u32 = 24_000;
pub const MAX_HIGHLIGHT_SELECTION_MESSAGES: usize = 32_000;

fn recording_format_version() -> u32 {
    GAME_RECORDING_FORMAT_VERSION
}

fn clip_format_version() -> u32 {
    HIGHLIGHT_CLIP_FORMAT_VERSION
}

fn gameplay_replay_version() -> u16 {
    GAMEPLAY_REPLAY_VERSION
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum ReplayVisibility {
    #[default]
    Public,
    Private,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct ReplayAnchor {
    pub tick: u32,
    /// Highest recorder sequence already reflected in `state`. Messages are
    /// ordered by `(tick, sequence)`, so a command accepted later in the same
    /// tick remains replayable after a periodic anchor.
    #[serde(default)]
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub sequence: u64,
    pub state: GameState,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct RecordedGameMessage {
    pub tick: u32,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub sequence: u64,
    pub event: GameEvent,
}

impl RecordedGameMessage {
    pub fn envelope(&self, game_id: u32) -> GameEventMessage {
        GameEventMessage {
            game_id,
            tick: self.tick,
            sequence: self.sequence,
            stream_seq: 0,
            user_id: None,
            event: self.event.clone(),
        }
    }
}

/// Complete replay source persisted for every real production match.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct GameRecordingV1 {
    #[serde(default = "recording_format_version")]
    pub format_version: u32,
    #[serde(default = "gameplay_replay_version")]
    pub gameplay_version: u16,
    pub game_id: u32,
    #[serde(default)]
    pub visibility: ReplayVisibility,
    pub anchors: Vec<ReplayAnchor>,
    pub messages: Vec<RecordedGameMessage>,
    pub end_tick: u32,
    #[serde(with = "json_u64_string")]
    #[cfg_attr(feature = "ts-gen", ts(type = "string"))]
    pub end_sync_hash: u64,
}

impl GameRecordingV1 {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.format_version == GAME_RECORDING_FORMAT_VERSION,
            "unsupported recording format {}, expected {}",
            self.format_version,
            GAME_RECORDING_FORMAT_VERSION
        );
        ensure!(
            self.gameplay_version == GAMEPLAY_REPLAY_VERSION,
            "recording gameplay version {} is incompatible with {}",
            self.gameplay_version,
            GAMEPLAY_REPLAY_VERSION
        );
        ensure!(
            !self.anchors.is_empty(),
            "recording needs an activation anchor"
        );
        ensure!(
            self.anchors
                .windows(2)
                .all(|pair| pair[0].tick < pair[1].tick),
            "recording anchors must be strictly tick ordered"
        );
        for anchor in &self.anchors {
            ensure!(
                anchor.tick == anchor.state.tick,
                "anchor tick/state mismatch"
            );
            anchor
                .state
                .validate_boost_invariants()
                .context("recording anchor violates engine invariants")?;
        }
        ensure!(
            self.messages.windows(2).all(|pair| {
                (pair[0].tick, pair[0].sequence) < (pair[1].tick, pair[1].sequence)
            }),
            "recording messages must be strictly tick/sequence ordered"
        );
        let first_tick = self.anchors[0].tick;
        ensure!(
            self.messages
                .iter()
                .all(|message| message.tick >= first_tick && message.tick <= self.end_tick),
            "recording message lies outside the replay span"
        );
        ensure!(
            self.anchors
                .iter()
                .all(|anchor| anchor.tick <= self.end_tick),
            "recording anchor lies beyond end_tick"
        );
        Ok(())
    }

    pub fn state_at_tick(&self, target_tick: u32) -> Result<GameState> {
        self.validate()?;
        self.state_at_tick_validated(target_tick)
    }

    /// Replay after the caller has already validated the immutable archive.
    /// Highlight scoring asks for many historical geometry snapshots; doing a
    /// full O(messages) structural validation for every one would defeat the
    /// scorer's hard work budget.
    fn state_at_tick_validated(&self, target_tick: u32) -> Result<GameState> {
        ensure!(
            target_tick <= self.end_tick,
            "target tick is beyond recording end"
        );
        let anchor = self
            .anchors
            .iter()
            .rev()
            .find(|anchor| anchor.tick <= target_tick)
            .context("no replay anchor at or before target tick")?;
        let mut state = anchor.state.clone();
        let first = self.messages.partition_point(|message| {
            (message.tick, message.sequence) <= (anchor.tick, anchor.sequence)
        });
        let last = self
            .messages
            .partition_point(|message| message.tick <= target_tick);
        for message in &self.messages[first..last] {
            state = advance_and_apply_replicated_message(&state, &message.envelope(self.game_id))?;
        }
        while state.tick < target_tick && !state.is_complete() {
            state.tick_forward(true)?;
        }
        Ok(state)
    }

    pub fn verify_end_hash(&self) -> Result<()> {
        self.verified_end_state().map(|_| ())
    }

    fn verified_end_state(&self) -> Result<GameState> {
        self.validate()?;
        let state = self.state_at_tick_validated(self.end_tick)?;
        ensure!(
            state.sync_hash() == self.end_sync_hash,
            "recording end hash mismatch: expected {:#018x}, got {:#018x}",
            self.end_sync_hash,
            state.sync_hash()
        );
        Ok(state)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum HighlightReason {
    BoostedCutoff { kills: u32 },
    TrapKill { kills: u32 },
    Demolition { kills: u32 },
    GoalRun { points: u32 },
    ComboFrenzy { max_chain: u32 },
    FeedingFrenzy { pickups: u32 },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct HighlightWindow {
    pub start_tick: u32,
    pub end_tick: u32,
    pub focus_tick: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct HighlightSpeedSegment {
    pub until_tick: u32,
    pub time_scale: f32,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct HighlightPresentation {
    pub rotation: i32,
    pub follow_snake_id: u32,
    pub segments: Vec<HighlightSpeedSegment>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct HighlightConfig {
    pub rules_version: u16,
    pub minimum_score: i32,
    pub elimination: i32,
    pub mutual_trade: i32,
    pub boosted_kill: i32,
    pub cutoff: i32,
    pub proximity: i32,
    pub trap: i32,
    pub laden_per_point: i32,
    pub laden_cap: i32,
    pub teammate_kill: i32,
    pub banked_per_point: i32,
    pub big_carry: i32,
    pub combo_step: i32,
    pub nick_of_time: i32,
    pub boosted_pickup: i32,
    pub feeding_frenzy: i32,
    pub death: i32,
}

impl Default for HighlightConfig {
    fn default() -> Self {
        Self {
            // Calibration round 1 (200 deterministic authoritative bot
            // games) raised only the two cooperative-play weights below.
            // The resulting non-trivial production rate is 75%, with no
            // category above 50.4% of winners.
            rules_version: 2,
            minimum_score: 120,
            elimination: 90,
            mutual_trade: 60,
            boosted_kill: 40,
            cutoff: 30,
            proximity: 20,
            trap: 30,
            laden_per_point: 4,
            laden_cap: 50,
            teammate_kill: -80,
            banked_per_point: 6,
            big_carry: 50,
            combo_step: 21,
            nick_of_time: 10,
            boosted_pickup: 5,
            feeding_frenzy: 20,
            death: -60,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct HighlightClip {
    #[serde(default = "clip_format_version")]
    pub clip_format_version: u32,
    #[serde(default = "gameplay_replay_version")]
    pub gameplay_version: u16,
    pub game_id: u32,
    pub star_user_id: u32,
    pub star_snake_id: u32,
    pub star_name: String,
    pub reason: HighlightReason,
    pub score: i32,
    pub breakdown: HighlightScoreBreakdown,
    pub window: HighlightWindow,
    pub anchor: GameState,
    pub messages: Vec<RecordedGameMessage>,
    #[serde(with = "json_u64_string")]
    #[cfg_attr(feature = "ts-gen", ts(type = "string"))]
    pub end_sync_hash: u64,
    pub presentation: HighlightPresentation,
    pub config: HighlightConfig,
}

impl HighlightClip {
    pub fn viewer_duration_ms(&self) -> u32 {
        let tick_ms = self.anchor.properties.tick_duration_ms.max(1);
        let mut prior = self.window.start_tick;
        let mut total = 0.0_f64;
        for segment in &self.presentation.segments {
            let end = segment.until_tick.min(self.window.end_tick).max(prior);
            total += f64::from(end - prior) * f64::from(tick_ms) / f64::from(segment.time_scale);
            prior = end;
        }
        if prior < self.window.end_tick {
            total += f64::from(self.window.end_tick - prior) * f64::from(tick_ms);
        }
        total.round() as u32
    }

    pub fn replay_and_verify(&self) -> Result<GameState> {
        ensure!(
            self.clip_format_version == HIGHLIGHT_CLIP_FORMAT_VERSION,
            "unsupported highlight clip version"
        );
        ensure!(
            self.gameplay_version == GAMEPLAY_REPLAY_VERSION,
            "highlight gameplay version mismatch"
        );
        let recording = GameRecordingV1 {
            format_version: GAME_RECORDING_FORMAT_VERSION,
            gameplay_version: self.gameplay_version,
            game_id: self.game_id,
            visibility: ReplayVisibility::Public,
            anchors: vec![ReplayAnchor {
                tick: self.anchor.tick,
                sequence: 0,
                state: self.anchor.clone(),
            }],
            messages: self.messages.clone(),
            end_tick: self.window.end_tick,
            end_sync_hash: self.end_sync_hash,
        };
        recording.verified_end_state()
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct HighlightScoreBreakdown {
    pub total: i32,
    pub focus_tick: u32,
    pub kills: u32,
    pub boosted_cutoff_kills: u32,
    pub trap_kills: u32,
    pub banked_points: u32,
    pub max_chain: u32,
    pub pickups: u32,
    pub demolition_points: i32,
    pub banking_points: i32,
    pub combo_points: i32,
    #[serde(skip)]
    #[cfg_attr(feature = "ts-gen", ts(skip))]
    focus_value: i32,
}

impl HighlightScoreBreakdown {
    fn add_focus(&mut self, tick: u32, value: i32) {
        if value > self.focus_value || (value == self.focus_value && tick < self.focus_tick) {
            self.focus_tick = tick;
            self.focus_value = value;
        }
    }

    fn reason(&self) -> HighlightReason {
        if self.demolition_points >= self.banking_points
            && self.demolition_points >= self.combo_points
            && self.kills > 0
        {
            if self.boosted_cutoff_kills > 0 {
                HighlightReason::BoostedCutoff { kills: self.kills }
            } else if self.trap_kills > 0 {
                HighlightReason::TrapKill { kills: self.kills }
            } else {
                HighlightReason::Demolition { kills: self.kills }
            }
        } else if self.banking_points >= self.combo_points && self.banked_points > 0 {
            HighlightReason::GoalRun {
                points: self.banked_points,
            }
        } else if self.max_chain > 1 {
            HighlightReason::ComboFrenzy {
                max_chain: self.max_chain,
            }
        } else {
            HighlightReason::FeedingFrenzy {
                pickups: self.pickups,
            }
        }
    }
}

#[derive(Clone)]
struct Observation {
    tick: u32,
    event: GameEvent,
    before: GameState,
}

/// Select and cut one deterministic highlight. Custom games remain ineligible
/// at launch; every canonical queue/mode, including Solo, is eligible.
pub fn select_highlight(
    recording: &GameRecordingV1,
    config: &HighlightConfig,
) -> Result<Option<HighlightClip>> {
    Ok(score_highlight_candidate(recording, config)?
        .filter(|candidate| candidate.score >= config.minimum_score))
}

/// Return the same deterministic, re-centered top candidate used by
/// [`select_highlight`] without applying `minimum_score`. The offline tuning
/// harness uses this to explain below-threshold games; production continues to
/// call `select_highlight` and therefore cannot publish a weak candidate.
pub fn score_highlight_candidate(
    recording: &GameRecordingV1,
    config: &HighlightConfig,
) -> Result<Option<HighlightClip>> {
    recording.validate()?;
    ensure!(
        recording.end_tick.saturating_sub(recording.anchors[0].tick)
            <= MAX_HIGHLIGHT_SELECTION_TICKS,
        "recording exceeds highlight tick budget"
    );
    ensure!(
        recording.messages.len() <= MAX_HIGHLIGHT_SELECTION_MESSAGES,
        "recording exceeds highlight message budget"
    );
    if matches!(
        recording.anchors[0].state.game_type,
        GameType::Custom { .. }
    ) {
        return Ok(None);
    }
    let observations = replay_observations(recording)?;
    let anchor_state = &recording.anchors[0].state;
    let tick_ms = anchor_state.properties.tick_duration_ms.max(1);
    let window_ticks = (10_000 / tick_ms).max(1);
    let step_ticks = (1_000 / tick_ms).max(1);
    let lead_ticks = 6_000 / tick_ms;
    let earliest = recording.anchors[0].tick;
    let latest = recording.end_tick;
    let mut snakes: Vec<u32> = anchor_state
        .players
        .values()
        .map(|player| player.snake_id)
        .collect();
    snakes.sort_unstable();
    snakes.dedup();

    let mut raw_best: Option<(u32, HighlightScoreBreakdown)> = None;
    // A scored event appears in several overlapping windows. Cache the
    // comparatively expensive flood-fill/resimulation result so the launch
    // scorer's work is proportional to unique deaths, not windows × deaths.
    let mut trap_cache = HashMap::new();
    for star in snakes {
        let mut start = earliest;
        loop {
            let end = start
                .saturating_add(window_ticks)
                .min(latest.saturating_add(1));
            let score = score_range(
                recording,
                &observations,
                star,
                start,
                end,
                config,
                &mut trap_cache,
            )?;
            // The published clip is re-centered around the scored focus with a
            // six-second lead-in. Keep the scoring domain to focus ticks that
            // can actually retain that lead-in from the oldest anchor. An
            // earlier (and even higher-scoring) event must not suppress a
            // valid later candidate and make the whole match ineligible.
            if score.focus_tick >= earliest.saturating_add(lead_ticks)
                && better_candidate(star, &score, raw_best.as_ref())
            {
                raw_best = Some((star, score));
            }
            if end > latest || start.saturating_add(step_ticks) > latest {
                break;
            }
            start = start.saturating_add(step_ticks);
        }
    }
    let Some((star, raw)) = raw_best else {
        return Ok(None);
    };
    let tail_ticks = 3_000 / tick_ms;
    let start_tick = raw.focus_tick - lead_ticks;
    let end_tick = raw.focus_tick + tail_ticks;
    let rescored = score_range(
        recording,
        &observations,
        star,
        start_tick,
        end_tick.saturating_add(1),
        config,
        &mut trap_cache,
    )?;
    let anchor = recording
        .anchors
        .iter()
        .rev()
        .find(|candidate| candidate.tick <= start_tick)
        .context("highlight has no retained anchor")?;
    let mut anchor_state = anchor.state.clone();
    anchor_state.rng = None;
    let messages = recording
        .messages
        .iter()
        .filter(|message| {
            (message.tick, message.sequence) > (anchor.tick, anchor.sequence)
                && message.tick <= end_tick
        })
        .cloned()
        .collect();
    // Terminal plays intentionally keep three seconds of post-roll on screen.
    // Once a match is complete the replay engine holds the immutable final
    // frame, so the padded clip remains deterministic without fake events.
    let end_sync_hash = recording
        .state_at_tick_validated(end_tick.min(recording.end_tick))?
        .sync_hash();
    let star_user_id = anchor_state
        .players
        .iter()
        .filter_map(|(user_id, player)| (player.snake_id == star).then_some(*user_id))
        .min()
        .context("highlight star has no player")?;
    let star_name = anchor_state
        .usernames
        .get(&star_user_id)
        .cloned()
        .unwrap_or_else(|| format!("Player {star_user_id}"));
    let focus_tick = rescored.focus_tick;
    let slow_start = focus_tick - (2_000 / tick_ms);
    let slow_end = focus_tick + (1_500 / tick_ms);
    let rotation = star_rotation(&anchor_state, star);

    Ok(Some(HighlightClip {
        clip_format_version: HIGHLIGHT_CLIP_FORMAT_VERSION,
        gameplay_version: GAMEPLAY_REPLAY_VERSION,
        game_id: recording.game_id,
        star_user_id,
        star_snake_id: star,
        star_name,
        reason: rescored.reason(),
        score: rescored.total,
        breakdown: rescored.clone(),
        window: HighlightWindow {
            start_tick,
            end_tick,
            focus_tick,
        },
        anchor: anchor_state,
        messages,
        end_sync_hash,
        presentation: HighlightPresentation {
            rotation,
            follow_snake_id: star,
            segments: vec![
                HighlightSpeedSegment {
                    until_tick: slow_start,
                    time_scale: 1.0,
                },
                HighlightSpeedSegment {
                    until_tick: slow_end,
                    time_scale: 0.5,
                },
                HighlightSpeedSegment {
                    until_tick: end_tick,
                    time_scale: 1.0,
                },
            ],
        },
        config: config.clone(),
    }))
}

fn replay_observations(recording: &GameRecordingV1) -> Result<Vec<Observation>> {
    let anchor = &recording.anchors[0];
    let mut state = anchor.state.clone();
    let mut observations = Vec::with_capacity(recording.messages.len());
    for message in &recording.messages {
        if (message.tick, message.sequence) <= (anchor.tick, anchor.sequence) {
            continue;
        }
        let mut before = state.clone();
        while before.tick < message.tick && !before.is_complete() {
            before.tick_forward(true)?;
        }
        state = advance_and_apply_replicated_message(&state, &message.envelope(recording.game_id))?;
        observations.push(Observation {
            tick: message.tick,
            event: message.event.clone(),
            before,
        });
    }
    Ok(observations)
}

fn better_candidate(
    star: u32,
    candidate: &HighlightScoreBreakdown,
    current: Option<&(u32, HighlightScoreBreakdown)>,
) -> bool {
    let Some((current_star, current)) = current else {
        return true;
    };
    candidate.total > current.total
        || (candidate.total == current.total
            && (candidate.focus_tick < current.focus_tick
                || (candidate.focus_tick == current.focus_tick && star < *current_star)))
}

fn score_range(
    recording: &GameRecordingV1,
    observations: &[Observation],
    star: u32,
    start: u32,
    end: u32,
    config: &HighlightConfig,
    trap_cache: &mut HashMap<(u32, u32, u32), bool>,
) -> Result<HighlightScoreBreakdown> {
    let mut result = HighlightScoreBreakdown {
        focus_tick: start,
        ..Default::default()
    };
    let in_window = observations_in_tick_range(observations, start, end);
    let mut base_elimination_points = 0;
    let mut kill_ticks = HashSet::new();
    let mut star_death_ticks = Vec::new();
    let unlimited = recording.anchors[0]
        .state
        .properties
        .boost
        .as_ref()
        .is_some_and(|boost| boost.unlimited);

    for observation in in_window {
        if let GameEvent::SnakeDied { snake_id, cause } = &observation.event {
            if *snake_id == star {
                if !matches!(cause, DeathCause::Banked) {
                    star_death_ticks.push(observation.tick);
                }
                continue;
            }
            let killer = match cause {
                DeathCause::SnakeBody { killer_snake_id } => Some(*killer_snake_id),
                DeathCause::HeadToHead { other_snake_id } => Some(*other_snake_id),
                _ => None,
            };
            if killer != Some(star) {
                continue;
            }
            if same_team(&observation.before, star, *snake_id) {
                result.total += config.teammate_kill;
                result.demolition_points += config.teammate_kill;
                continue;
            }

            let mutual = matches!(cause, DeathCause::HeadToHead { .. })
                && in_window.iter().any(|other| {
                    other.tick == observation.tick
                        && matches!(
                            &other.event,
                            GameEvent::SnakeDied {
                                snake_id: other_victim,
                                cause: DeathCause::HeadToHead { other_snake_id },
                            } if *other_victim == star && *other_snake_id == *snake_id
                        )
                });
            let base = if mutual {
                config.mutual_trade
            } else {
                config.elimination
            };
            let repeat =
                prior_kills_of_victim(observations, star, *snake_id, observation.tick, 30_000);
            let victim_active = victim_was_active(
                observations,
                &observation.before,
                *snake_id,
                observation.tick,
            );

            let mut contribution = base;
            result.kills += 1;
            kill_ticks.insert(observation.tick);
            let boosting = !unlimited
                && observation
                    .before
                    .arena
                    .snakes
                    .get(star as usize)
                    .is_some_and(|snake| snake.boost().active);
            if boosting {
                contribution += config.boosted_kill;
            }
            let cutoff = recent_cutoff(
                observations,
                &observation.before,
                star,
                *snake_id,
                observation.tick,
            );
            if cutoff {
                contribution += config.cutoff;
            }
            if heads_within(&observation.before, star, *snake_id, 3) {
                contribution += config.proximity;
            }
            let trap_key = (star, *snake_id, observation.tick);
            let trapped = if let Some(trapped) = trap_cache.get(&trap_key) {
                *trapped
            } else {
                let trapped = trap_signature(
                    recording,
                    &observation.before,
                    star,
                    *snake_id,
                    observation.tick,
                )?;
                trap_cache.insert(trap_key, trapped);
                trapped
            };
            if trapped {
                contribution += config.trap;
            }
            let carried = observation
                .before
                .arena
                .snakes
                .get(*snake_id as usize)
                .map(|snake| observation.before.carried_food(snake))
                .unwrap_or_default();
            contribution += (carried as i32 * config.laden_per_point).min(config.laden_cap);
            // Farming discounts apply to the complete positive elimination,
            // not only its base points. Otherwise a third repeat or an AFK
            // victim could still clear the threshold through full Boost,
            // cutoff, proximity, trap, and laden modifiers.
            let discounted_base = discount_elimination(base, repeat, victim_active);
            contribution = discount_elimination(contribution, repeat, victim_active);
            base_elimination_points += discounted_base;
            result.total += contribution;
            result.demolition_points += contribution;
            if contribution > 0 {
                result.boosted_cutoff_kills += u32::from(boosting && cutoff);
                result.trap_kills += u32::from(trapped);
            }
            result.add_focus(observation.tick, contribution);
        }
    }
    if result.kills >= 2 {
        let multikill_bonus = base_elimination_points / 2;
        result.total += multikill_bonus;
        result.demolition_points += multikill_bonus;
    }

    let goals = goals_in_range(in_window, star, start, end);
    for goal in goals.values() {
        let mut points = goal.points as i32 * config.banked_per_point;
        if goal.points >= 15 {
            points += config.big_carry;
        }
        result.total += points;
        result.banking_points += points;
        result.banked_points = result.banked_points.saturating_add(goal.points);
        result.add_focus(goal.tick, points);
    }

    let mut combo_focus_tick = start;
    let mut frenzy_focus_tick = start;
    for observation in in_window {
        if let GameEvent::FoodEaten {
            snake_id,
            combo_chain,
            combo_remaining_ms_before,
            boost_active,
            ..
        } = &observation.event
            && *snake_id == star
        {
            result.pickups += 1;
            if *combo_chain > result.max_chain {
                result.max_chain = *combo_chain;
                combo_focus_tick = observation.tick;
            }
            if result.pickups == 8 {
                frenzy_focus_tick = observation.tick;
            }
            let mut contribution = 0;
            if *combo_remaining_ms_before > 0 && *combo_remaining_ms_before < 250 {
                contribution += config.nick_of_time;
            }
            if *boost_active && !unlimited {
                contribution += config.boosted_pickup;
            }
            result.total += contribution;
            result.combo_points += contribution;
            result.add_focus(observation.tick, contribution);
        }
    }
    if result.max_chain > 1 {
        let points = (result.max_chain - 1) as i32 * config.combo_step;
        result.total += points;
        result.combo_points += points;
        result.add_focus(combo_focus_tick, points);
    }
    if result.pickups >= 8 {
        result.total += config.feeding_frenzy;
        result.combo_points += config.feeding_frenzy;
        result.add_focus(frenzy_focus_tick, config.feeding_frenzy);
    }
    for death_tick in star_death_ticks {
        if !kill_ticks.contains(&death_tick) {
            result.total += config.death;
            result.demolition_points += config.death;
        }
    }
    Ok(result)
}

/// Observations are validated in `(tick, sequence)` order, so binary slicing
/// avoids repeatedly walking the entire match for every one-second window.
fn observations_in_tick_range(
    observations: &[Observation],
    start: u32,
    end: u32,
) -> &[Observation] {
    let first = observations.partition_point(|observation| observation.tick < start);
    let last = observations.partition_point(|observation| observation.tick < end);
    &observations[first..last]
}

fn discount_elimination(value: i32, repeat: usize, victim_active: bool) -> i32 {
    let repeat_discounted = match repeat {
        0 => value,
        1 => value / 2,
        _ => 0,
    };
    if victim_active {
        repeat_discounted
    } else {
        repeat_discounted / 4
    }
}

fn same_team(state: &GameState, left: u32, right: u32) -> bool {
    let left = state
        .arena
        .snakes
        .get(left as usize)
        .and_then(|snake| snake.team_id);
    let right = state
        .arena
        .snakes
        .get(right as usize)
        .and_then(|snake| snake.team_id);
    left.is_some() && left == right
}

fn prior_kills_of_victim(
    observations: &[Observation],
    star: u32,
    victim: u32,
    tick: u32,
    window_ms: u32,
) -> usize {
    let tick_ms = observations
        .first()
        .map(|observation| observation.before.properties.tick_duration_ms.max(1))
        .unwrap_or(100);
    let min_tick = tick.saturating_sub(window_ms / tick_ms);
    observations_in_tick_range(observations, min_tick, tick)
        .iter()
        .filter(|observation| {
            matches!(
                &observation.event,
                GameEvent::SnakeDied {
                    snake_id,
                    cause: DeathCause::SnakeBody { killer_snake_id },
                } if *snake_id == victim && *killer_snake_id == star
            ) || matches!(
                &observation.event,
                GameEvent::SnakeDied {
                    snake_id,
                    cause: DeathCause::HeadToHead { other_snake_id },
                } if *snake_id == victim && *other_snake_id == star
            )
        })
        .count()
}

fn victim_was_active(
    observations: &[Observation],
    state: &GameState,
    victim: u32,
    tick: u32,
) -> bool {
    let Some(user_id) = state
        .players
        .iter()
        .filter_map(|(user_id, player)| (player.snake_id == victim).then_some(*user_id))
        .min()
    else {
        return false;
    };
    let min_tick = tick.saturating_sub(10_000 / state.properties.tick_duration_ms.max(1));
    observations_in_tick_range(observations, min_tick, tick.saturating_add(1))
        .iter()
        .any(|observation| {
            matches!(
                &observation.event,
                GameEvent::CommandScheduled { command_message }
                    if command_message.id().user_id == user_id
            ) || matches!(
                &observation.event,
                GameEvent::CommandScheduledV2 { command_message, .. }
                    if command_message.id().user_id == user_id
            )
        })
}

fn recent_cutoff(
    observations: &[Observation],
    state: &GameState,
    star: u32,
    victim: u32,
    tick: u32,
) -> bool {
    let lookback = (600 / state.properties.tick_duration_ms.max(1)).max(1);
    let turned_recently = observations_in_tick_range(
        observations,
        tick.saturating_sub(lookback),
        tick.saturating_add(1),
    )
    .iter()
    .any(|observation| {
        matches!(
            observation.event,
            GameEvent::SnakeTurned { snake_id, .. } if snake_id == star
        )
    });
    if !turned_recently {
        return false;
    }
    let Some(star_snake) = state.arena.snakes.get(star as usize) else {
        return false;
    };
    let Some(victim_snake) = state.arena.snakes.get(victim as usize) else {
        return false;
    };
    let Ok(head) = victim_snake.head() else {
        return false;
    };
    let star_cells = expanded_body_cells(&star_snake.body);
    (0..=12).any(|distance| {
        let point = advance_position(*head, victim_snake.direction, distance);
        star_cells.contains(&point)
    })
}

fn heads_within(state: &GameState, left: u32, right: u32, distance: i16) -> bool {
    let Some(left) = state
        .arena
        .snakes
        .get(left as usize)
        .and_then(|snake| snake.head().ok())
    else {
        return false;
    };
    let Some(right) = state
        .arena
        .snakes
        .get(right as usize)
        .and_then(|snake| snake.head().ok())
    else {
        return false;
    };
    (left.x - right.x).abs().max((left.y - right.y).abs()) <= distance
}

fn trap_signature(
    recording: &GameRecordingV1,
    current: &GameState,
    star: u32,
    victim: u32,
    tick: u32,
) -> Result<bool> {
    let tick_ms = current.properties.tick_duration_ms.max(1);
    let prior_tick = tick
        .saturating_sub(2_000 / tick_ms)
        .max(recording.anchors[0].tick);
    let prior = recording.state_at_tick_validated(prior_tick)?;
    let current_region = reachable_cells(current, victim, 64);
    let current_reach = current_region.len();
    let prior_reach = reachable_cells(&prior, victim, 64).len();
    if current_reach >= 15 || prior_reach == 0 || current_reach * 100 > prior_reach * 40 {
        return Ok(false);
    }
    let star_cells = current
        .arena
        .snakes
        .get(star as usize)
        .map(|snake| expanded_body_cells(&snake.body))
        .unwrap_or_default();
    let all_obstacles = obstacle_cells(current, victim);
    let mut frontier = 0_u32;
    let mut star_frontier = 0_u32;
    for point in current_region {
        for direction in [
            Direction::Up,
            Direction::Down,
            Direction::Left,
            Direction::Right,
        ] {
            let neighbor = advance_position(point, direction, 1);
            if all_obstacles.contains(&neighbor) || outside(current, neighbor) {
                frontier += 1;
                if star_cells.contains(&neighbor) {
                    star_frontier += 1;
                }
            }
        }
    }
    Ok(frontier > 0 && star_frontier * 100 >= frontier * 40)
}

fn reachable_cells(state: &GameState, victim: u32, limit: usize) -> HashSet<Position> {
    let Some(start) = state
        .arena
        .snakes
        .get(victim as usize)
        .and_then(|snake| snake.head().ok())
        .copied()
    else {
        return HashSet::new();
    };
    let obstacles = obstacle_cells(state, victim);
    let mut seen = HashSet::from([start]);
    let mut queue = VecDeque::from([start]);
    while let Some(point) = queue.pop_front() {
        if seen.len() >= limit {
            return seen;
        }
        for direction in [
            Direction::Up,
            Direction::Down,
            Direction::Left,
            Direction::Right,
        ] {
            let next = advance_position(point, direction, 1);
            if !outside(state, next) && !obstacles.contains(&next) && seen.insert(next) {
                queue.push_back(next);
            }
        }
    }
    seen
}

fn obstacle_cells(state: &GameState, victim: u32) -> HashSet<Position> {
    let mut cells = HashSet::new();
    for snake in &state.arena.snakes {
        if snake.is_alive {
            cells.extend(expanded_body_cells(&snake.body));
        }
    }
    let victim_team = state
        .arena
        .snakes
        .get(victim as usize)
        .and_then(|snake| snake.team_id);
    for y in 0..state.arena.height as i16 {
        for x in 0..state.arena.width as i16 {
            let point = Position { x, y };
            if state.arena.is_wall_position(&point)
                || victim_team.is_some_and(|team_id| state.arena.is_in_enemy_base(&point, team_id))
            {
                cells.insert(point);
            }
        }
    }
    cells
}

fn outside(state: &GameState, point: Position) -> bool {
    point.x < 0
        || point.y < 0
        || point.x >= state.arena.width as i16
        || point.y >= state.arena.height as i16
}

fn expanded_body_cells(body: &[Position]) -> HashSet<Position> {
    let mut cells = HashSet::new();
    for pair in body.windows(2) {
        let (mut x, mut y) = (pair[0].x, pair[0].y);
        let dx = (pair[1].x - pair[0].x).signum();
        let dy = (pair[1].y - pair[0].y).signum();
        cells.insert(Position { x, y });
        while (x, y) != (pair[1].x, pair[1].y) {
            x += dx;
            y += dy;
            cells.insert(Position { x, y });
        }
    }
    if body.len() == 1 {
        cells.insert(body[0]);
    }
    cells
}

fn advance_position(mut point: Position, direction: Direction, distance: i16) -> Position {
    match direction {
        Direction::Up => point.y -= distance,
        Direction::Down => point.y += distance,
        Direction::Left => point.x -= distance,
        Direction::Right => point.x += distance,
    }
    point
}

fn goals_in_range(
    observations: &[Observation],
    star: u32,
    start: u32,
    end: u32,
) -> HashMap<(u32, u32), TeamGoal> {
    let mut goals = HashMap::new();
    for observation in observations {
        for goal in &observation.before.recent_goals {
            if goal.snake_id == star && goal.tick >= start && goal.tick < end {
                goals
                    .entry((goal.tick, goal.snake_id))
                    .or_insert_with(|| goal.clone());
            }
        }
    }
    goals
}

fn star_rotation(state: &GameState, star: u32) -> i32 {
    match state
        .arena
        .snakes
        .get(star as usize)
        .and_then(|snake| snake.team_id)
    {
        Some(team) if team.0 == 0 => 270,
        Some(_) => 90,
        None => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CommandId, GameCommand, GameCommandMessage, GameStatus, QueueMode, ScenarioScript, TeamId,
    };

    fn recording_from_scenario(json: &str, game_id: u32) -> GameRecordingV1 {
        let loaded = ScenarioScript::from_json(json).unwrap().load().unwrap();
        let run = loaded.run().unwrap();
        let mut sequence = 0_u64;
        let mut messages = Vec::new();
        for (tick, _, event) in &run.events {
            if matches!(event, GameEvent::Snapshot { .. }) {
                continue;
            }
            sequence += 1;
            messages.push(RecordedGameMessage {
                tick: *tick,
                sequence,
                event: event.clone(),
            });
        }
        GameRecordingV1 {
            format_version: GAME_RECORDING_FORMAT_VERSION,
            gameplay_version: GAMEPLAY_REPLAY_VERSION,
            game_id,
            visibility: ReplayVisibility::Public,
            anchors: vec![ReplayAnchor {
                tick: loaded.initial_state.tick,
                sequence: 0,
                state: loaded.initial_state,
            }],
            messages,
            end_tick: run.final_state.tick,
            end_sync_hash: run.final_state.sync_hash(),
        }
    }

    #[test]
    fn archive_replays_and_detects_hash_corruption() {
        let mut recording = recording_from_scenario(
            include_str!("../../client/web/scenarios/combo-frenzy.json"),
            7,
        );
        recording.verify_end_hash().unwrap();
        recording.end_sync_hash ^= 1;
        assert!(recording.verify_end_hash().is_err());
    }

    #[test]
    fn periodic_anchor_replays_only_later_same_tick_sequences() {
        let mut initial = GameState::new(40, 40, GameType::Solo, QueueMode::Quickmatch, Some(7), 0);
        let player = initial.add_player(1, Some("star".into())).unwrap();
        initial.status = GameStatus::Started { server_id: 1 };

        let scheduled = |sequence_number, direction| GameEvent::CommandScheduled {
            command_message: GameCommandMessage {
                command_id_client: CommandId {
                    tick: sequence_number,
                    user_id: 1,
                    sequence_number,
                },
                command_id_server: Some(CommandId {
                    tick: sequence_number,
                    user_id: 1,
                    sequence_number,
                }),
                command: GameCommand::Turn {
                    snake_id: player.snake_id,
                    direction,
                },
            },
        };
        let first = RecordedGameMessage {
            tick: initial.tick,
            sequence: 1,
            event: scheduled(1, Direction::Down),
        };
        let second = RecordedGameMessage {
            tick: initial.tick,
            sequence: 2,
            event: scheduled(2, Direction::Left),
        };
        let anchored = advance_and_apply_replicated_message(&initial, &first.envelope(88)).unwrap();
        let expected =
            advance_and_apply_replicated_message(&anchored, &second.envelope(88)).unwrap();
        let recording = GameRecordingV1 {
            format_version: GAME_RECORDING_FORMAT_VERSION,
            gameplay_version: GAMEPLAY_REPLAY_VERSION,
            game_id: 88,
            visibility: ReplayVisibility::Public,
            anchors: vec![ReplayAnchor {
                tick: anchored.tick,
                sequence: first.sequence,
                state: anchored,
            }],
            messages: vec![first, second],
            end_tick: expected.tick,
            end_sync_hash: expected.sync_hash(),
        };

        recording.verify_end_hash().unwrap();
        assert_eq!(
            recording.state_at_tick(expected.tick).unwrap().sync_hash(),
            expected.sync_hash()
        );
    }

    #[test]
    fn replay_hashes_cross_json_losslessly_and_read_legacy_numbers() {
        let recording = recording_from_scenario(
            include_str!("../../client/web/scenarios/combo-frenzy.json"),
            8,
        );
        let encoded = serde_json::to_value(&recording).unwrap();
        assert_eq!(
            encoded["end_sync_hash"],
            serde_json::Value::String(recording.end_sync_hash.to_string())
        );

        let mut legacy = encoded;
        legacy["end_sync_hash"] = serde_json::Value::Number(recording.end_sync_hash.into());
        let decoded: GameRecordingV1 = serde_json::from_value(legacy).unwrap();
        assert_eq!(decoded.end_sync_hash, recording.end_sync_hash);
    }

    #[test]
    fn clip_timing_is_nine_source_seconds_and_twelve_and_a_half_viewer_seconds() {
        let state = GameState::new(40, 40, GameType::Solo, QueueMode::Quickmatch, None, 0);
        let clip = HighlightClip {
            clip_format_version: HIGHLIGHT_CLIP_FORMAT_VERSION,
            gameplay_version: GAMEPLAY_REPLAY_VERSION,
            game_id: 1,
            star_user_id: 1,
            star_snake_id: 0,
            star_name: "Star".into(),
            reason: HighlightReason::ComboFrenzy { max_chain: 8 },
            score: 125,
            breakdown: HighlightScoreBreakdown {
                total: 125,
                max_chain: 8,
                ..Default::default()
            },
            window: HighlightWindow {
                start_tick: 0,
                end_tick: 180,
                focus_tick: 120,
            },
            anchor: state,
            messages: vec![],
            end_sync_hash: 0,
            presentation: HighlightPresentation {
                rotation: 0,
                follow_snake_id: 0,
                segments: vec![
                    HighlightSpeedSegment {
                        until_tick: 80,
                        time_scale: 1.0,
                    },
                    HighlightSpeedSegment {
                        until_tick: 150,
                        time_scale: 0.5,
                    },
                    HighlightSpeedSegment {
                        until_tick: 180,
                        time_scale: 1.0,
                    },
                ],
            },
            config: HighlightConfig::default(),
        };
        assert_eq!(clip.viewer_duration_ms(), 12_500);
        let payoff_ms = 4_000 + 2 * 2_000;
        assert_eq!(payoff_ms, 8_000);
    }

    #[test]
    fn clip_replay_returns_the_verified_end_state() {
        let state = GameState::new(40, 40, GameType::Solo, QueueMode::Quickmatch, Some(9), 0);
        let clip = HighlightClip {
            clip_format_version: HIGHLIGHT_CLIP_FORMAT_VERSION,
            gameplay_version: GAMEPLAY_REPLAY_VERSION,
            game_id: 77,
            star_user_id: 1,
            star_snake_id: 0,
            star_name: "Star".into(),
            reason: HighlightReason::FeedingFrenzy { pickups: 8 },
            score: 120,
            breakdown: HighlightScoreBreakdown {
                total: 120,
                pickups: 8,
                ..Default::default()
            },
            window: HighlightWindow {
                start_tick: state.tick,
                end_tick: state.tick,
                focus_tick: state.tick,
            },
            anchor: state.clone(),
            messages: Vec::new(),
            end_sync_hash: state.sync_hash(),
            presentation: HighlightPresentation {
                rotation: 0,
                follow_snake_id: 0,
                segments: Vec::new(),
            },
            config: HighlightConfig::default(),
        };

        let verified = clip.replay_and_verify().unwrap();
        assert_eq!(verified.sync_hash(), clip.end_sync_hash);
    }

    #[test]
    fn banked_death_is_scored_as_support_not_anti_style() {
        let recording =
            recording_from_scenario(include_str!("../../client/web/scenarios/team-bank.json"), 9);
        let observations = replay_observations(&recording).unwrap();
        let mut trap_cache = HashMap::new();
        let score = score_range(
            &recording,
            &observations,
            0,
            0,
            recording.end_tick + 1,
            &HighlightConfig::default(),
            &mut trap_cache,
        )
        .unwrap();
        assert_eq!(score.banked_points, 15);
        assert!(score.total >= 125);
    }

    #[test]
    fn combo_and_solo_frenzy_clear_threshold_without_free_boost_points() {
        let recording = recording_from_scenario(
            include_str!("../../client/web/scenarios/combo-frenzy.json"),
            10,
        );
        let observations = replay_observations(&recording).unwrap();
        let mut trap_cache = HashMap::new();
        let score = score_range(
            &recording,
            &observations,
            0,
            0,
            recording.end_tick + 1,
            &HighlightConfig::default(),
            &mut trap_cache,
        )
        .unwrap();
        assert_eq!(score.pickups, 8);
        assert_eq!(score.max_chain, 8);
        let config = HighlightConfig::default();
        assert_eq!(score.total, 7 * config.combo_step + config.feeding_frenzy);
        assert!(
            score.focus_tick > recording.anchors[0].tick,
            "combo-only highlights must focus a pickup, not the window start"
        );
    }

    #[test]
    fn deterministic_tie_break_prefers_earlier_focus_then_lower_snake() {
        let early = HighlightScoreBreakdown {
            total: 120,
            focus_tick: 10,
            ..Default::default()
        };
        let late = HighlightScoreBreakdown {
            total: 120,
            focus_tick: 11,
            ..Default::default()
        };
        assert!(better_candidate(3, &early, Some(&(1, late))));
        assert!(better_candidate(1, &early, Some(&(2, early.clone()))));
    }

    #[test]
    fn public_archive_validation_rejects_out_of_order_messages() {
        let mut recording = recording_from_scenario(
            include_str!("../../client/web/scenarios/combo-frenzy.json"),
            11,
        );
        recording.messages.swap(0, 1);
        assert!(recording.validate().is_err());
    }

    #[test]
    fn selection_budget_degrades_pathological_archives_before_resimulation() {
        let mut recording = recording_from_scenario(
            include_str!("../../client/web/scenarios/combo-frenzy.json"),
            12,
        );
        recording.end_tick = recording.anchors[0]
            .tick
            .saturating_add(MAX_HIGHLIGHT_SELECTION_TICKS)
            .saturating_add(1);
        let error = select_highlight(&recording, &HighlightConfig::default()).unwrap_err();
        assert!(error.to_string().contains("highlight tick budget"));
    }

    #[test]
    fn command_activity_is_detected_from_accepted_decisions_only() {
        let mut state = GameState::new(
            40,
            40,
            GameType::FreeForAll { max_players: 2 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        state.add_player(1, Some("star".into())).unwrap();
        state.add_player(2, Some("victim".into())).unwrap();
        state.status = GameStatus::Started { server_id: 1 };
        let victim_snake = state.players[&2].snake_id;
        let command = GameCommandMessage {
            command_id_client: CommandId {
                tick: 1,
                user_id: 2,
                sequence_number: 1,
            },
            command_id_server: Some(CommandId {
                tick: 1,
                user_id: 2,
                sequence_number: 1,
            }),
            command: GameCommand::Turn {
                snake_id: victim_snake,
                direction: Direction::Down,
            },
        };
        let observation = Observation {
            tick: 1,
            before: state.clone(),
            event: GameEvent::CommandScheduled {
                command_message: command,
            },
        };
        assert!(victim_was_active(&[observation], &state, victim_snake, 1));
    }

    #[test]
    fn bare_kill_does_not_clear_the_interestingness_threshold() {
        let config = HighlightConfig::default();
        assert!(config.elimination < config.minimum_score);
        assert!(config.elimination + config.proximity < config.minimum_score);
        assert_eq!(config.elimination + config.cutoff, config.minimum_score);
    }

    fn adversarial_kill_state() -> (GameState, u32, u32, u32) {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            Some(17),
            0,
        );
        let star = state.add_player(1, Some("star".into())).unwrap();
        let victim = state.add_player(2, Some("victim".into())).unwrap();
        state.status = GameStatus::Started { server_id: 1 };
        state.tick = 3;
        state.arena.snakes[victim.snake_id as usize].food = 20;
        (state, star.snake_id, victim.snake_id, victim.user_id)
    }

    fn recording_at_state(state: &GameState) -> GameRecordingV1 {
        GameRecordingV1 {
            format_version: GAME_RECORDING_FORMAT_VERSION,
            gameplay_version: GAMEPLAY_REPLAY_VERSION,
            game_id: 90,
            visibility: ReplayVisibility::Public,
            anchors: vec![ReplayAnchor {
                tick: state.tick,
                sequence: 0,
                state: state.clone(),
            }],
            messages: Vec::new(),
            end_tick: state.tick,
            end_sync_hash: state.sync_hash(),
        }
    }

    fn victim_activity(state: &GameState, victim: u32, user_id: u32) -> Observation {
        Observation {
            tick: state.tick,
            before: state.clone(),
            event: GameEvent::CommandScheduled {
                command_message: GameCommandMessage {
                    command_id_client: CommandId {
                        tick: state.tick,
                        user_id,
                        sequence_number: 1,
                    },
                    command_id_server: Some(CommandId {
                        tick: state.tick,
                        user_id,
                        sequence_number: 1,
                    }),
                    command: GameCommand::Turn {
                        snake_id: victim,
                        direction: Direction::Up,
                    },
                },
            },
        }
    }

    fn victim_death(state: &GameState, star: u32, victim: u32, tick: u32) -> Observation {
        Observation {
            tick,
            before: state.clone(),
            event: GameEvent::SnakeDied {
                snake_id: victim,
                cause: DeathCause::SnakeBody {
                    killer_snake_id: star,
                },
            },
        }
    }

    #[test]
    fn afk_discount_scales_the_entire_elimination_including_laden_modifier() {
        let (state, star, victim, victim_user) = adversarial_kill_state();
        let recording = recording_at_state(&state);
        let death = victim_death(&state, star, victim, state.tick);
        let mut active_cache = HashMap::new();
        let active = score_range(
            &recording,
            &[victim_activity(&state, victim, victim_user), death.clone()],
            star,
            state.tick,
            state.tick + 1,
            &HighlightConfig::default(),
            &mut active_cache,
        )
        .unwrap();
        let mut afk_cache = HashMap::new();
        let afk = score_range(
            &recording,
            &[death],
            star,
            state.tick,
            state.tick + 1,
            &HighlightConfig::default(),
            &mut afk_cache,
        )
        .unwrap();

        assert!(active.demolition_points > 0);
        assert_eq!(afk.demolition_points, active.demolition_points / 4);
        assert!(afk.total < HighlightConfig::default().minimum_score);
    }

    #[test]
    fn repeat_victim_discount_scales_modifiers_and_zeroes_third_kill() {
        let (state, star, victim, victim_user) = adversarial_kill_state();
        let recording = recording_at_state(&state);
        let prior_one = victim_death(&state, star, victim, 1);
        let prior_two = victim_death(&state, star, victim, 2);
        let activity = victim_activity(&state, victim, victim_user);
        let current = victim_death(&state, star, victim, state.tick);
        let mut first_cache = HashMap::new();
        let first = score_range(
            &recording,
            &[activity.clone(), current.clone()],
            star,
            state.tick,
            state.tick + 1,
            &HighlightConfig::default(),
            &mut first_cache,
        )
        .unwrap();
        let mut second_cache = HashMap::new();
        let second = score_range(
            &recording,
            &[prior_one.clone(), activity.clone(), current.clone()],
            star,
            state.tick,
            state.tick + 1,
            &HighlightConfig::default(),
            &mut second_cache,
        )
        .unwrap();
        let mut third_cache = HashMap::new();
        let third = score_range(
            &recording,
            &[prior_one, prior_two, activity, current],
            star,
            state.tick,
            state.tick + 1,
            &HighlightConfig::default(),
            &mut third_cache,
        )
        .unwrap();

        assert_eq!(second.demolition_points, first.demolition_points / 2);
        assert_eq!(third.demolition_points, 0);
        assert_eq!(third.total, 0);
    }

    #[test]
    fn persisted_goal_cues_are_filtered_to_the_half_open_scoring_window() {
        let (mut state, star, _, _) = adversarial_kill_state();
        state.recent_goals = vec![
            TeamGoal {
                tick: 9,
                team_id: TeamId(0),
                snake_id: star,
                position: Position { x: 0, y: 0 },
                points: 20,
            },
            TeamGoal {
                tick: 10,
                team_id: TeamId(0),
                snake_id: star,
                position: Position { x: 0, y: 0 },
                points: 15,
            },
            TeamGoal {
                tick: 20,
                team_id: TeamId(0),
                snake_id: star,
                position: Position { x: 0, y: 0 },
                points: 25,
            },
        ];
        let observations = vec![Observation {
            tick: 10,
            before: state,
            event: GameEvent::TickHash {
                hash: 0,
                server_ts_ms: 0,
            },
        }];

        let goals = goals_in_range(&observations, star, 10, 20);
        assert_eq!(goals.len(), 1);
        assert_eq!(goals[&(10, star)].points, 15);
    }
}
