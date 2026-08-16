//! Deterministic offline corpus generation for Play-of-the-Game calibration.
//!
//! This runs the authoritative `GameState` simulation with the exact decision
//! function used by the networked bot. It deliberately avoids the HTTP and
//! persistence layers: a fixed seed and corpus index are enough to reproduce
//! every accepted command, engine event, replay hash, and highlight result.

use anyhow::{Context, Result, bail, ensure};
use common::{
    ClientCommandIdentityV2, CommandId, DeathCause, GAME_RECORDING_FORMAT_VERSION,
    GAMEPLAY_REPLAY_VERSION, GameCommand, GameCommandMessage, GameEvent, GameRecordingV1,
    GameState, GameStatus, GameType, HighlightClip, HighlightConfig, HighlightReason,
    HighlightScoreBreakdown, HighlightWindow, MAX_HIGHLIGHT_SELECTION_TICKS, QueueMode,
    RecordedGameMessage, ReplayAnchor, ReplayVisibility, calculate_ai_command,
    score_highlight_candidate,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

pub const CALIBRATION_SCHEMA_VERSION: u16 = 1;
pub const DEFAULT_CORPUS_GAME_COUNT: usize = 200;
pub const DEFAULT_CORPUS_SEED: u64 = 0x534e_414b_4554_524f;
pub const DEFAULT_REVIEW_COUNT: usize = 20;
pub const MIN_PRODUCTION_RATE_BPS: u32 = 7_000;
pub const MAX_CATEGORY_SHARE_BPS: u32 = 6_000;
const BOT_COMMAND_INTERVAL_MS: u32 = 100;
const LONG_FORM_AI_START_DELAY_MS: u32 = 60_000;
const REPLAY_ANCHOR_INTERVAL_MS: u32 = 5_000;
const NON_TRIVIAL_DURATION_MS: u64 = 120_000;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum HighlightCategory {
    Demolition,
    Banking,
    Combo,
    Frenzy,
}

impl HighlightCategory {
    fn from_reason(reason: &HighlightReason) -> Self {
        match reason {
            HighlightReason::BoostedCutoff { .. }
            | HighlightReason::TrapKill { .. }
            | HighlightReason::Demolition { .. } => Self::Demolition,
            HighlightReason::GoalRun { .. } => Self::Banking,
            HighlightReason::ComboFrenzy { .. } => Self::Combo,
            HighlightReason::FeedingFrenzy { .. } => Self::Frenzy,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BotCorpusSpec {
    pub games: usize,
    pub seed_base: u64,
    pub review_count: usize,
}

impl Default for BotCorpusSpec {
    fn default() -> Self {
        Self {
            games: DEFAULT_CORPUS_GAME_COUNT,
            seed_base: DEFAULT_CORPUS_SEED,
            review_count: DEFAULT_REVIEW_COUNT,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SelectedHighlightSummary {
    pub star_user_id: u32,
    pub star_snake_id: u32,
    pub reason: HighlightReason,
    pub category: HighlightCategory,
    pub score: i32,
    pub window: HighlightWindow,
    pub breakdown: HighlightScoreBreakdown,
}

impl From<&HighlightClip> for SelectedHighlightSummary {
    fn from(clip: &HighlightClip) -> Self {
        Self {
            star_user_id: clip.star_user_id,
            star_snake_id: clip.star_snake_id,
            reason: clip.reason.clone(),
            category: HighlightCategory::from_reason(&clip.reason),
            score: clip.score,
            window: clip.window.clone(),
            breakdown: clip.breakdown.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CorpusGameSummary {
    pub corpus_index: usize,
    pub game_id: u32,
    pub seed: u64,
    pub mode: String,
    pub players: usize,
    pub active_players: usize,
    pub completed: bool,
    pub duration_ms: u64,
    pub command_interval_ms: u32,
    pub ai_start_delay_ms: u32,
    pub non_trivial: bool,
    pub message_count: usize,
    pub anchor_count: usize,
    pub candidate: Option<SelectedHighlightSummary>,
    pub selected: Option<SelectedHighlightSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CalibrationThresholds {
    pub minimum_production_rate_bps: u32,
    pub maximum_category_share_bps: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AutomaticGate {
    pub production_rate_pass: bool,
    pub category_balance_pass: bool,
    pub passed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HumanReviewStatus {
    pub required_reviews: usize,
    pub required_deserved: usize,
    pub completed_reviews: usize,
    pub deserved_reviews: usize,
    pub status: String,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CalibrationSummary {
    pub schema_version: u16,
    pub corpus_id: String,
    pub generator: String,
    pub config: HighlightConfig,
    pub games_requested: usize,
    pub games_generated: usize,
    pub completed_games: usize,
    pub truncated_games: usize,
    pub non_trivial_games: usize,
    pub non_trivial_games_with_potg: usize,
    pub production_rate_bps: u32,
    pub team_mode_winners: usize,
    pub category_counts: BTreeMap<HighlightCategory, usize>,
    pub category_share_bps: BTreeMap<HighlightCategory, u32>,
    pub largest_category_share_bps: u32,
    pub thresholds: CalibrationThresholds,
    pub automatic_gate: AutomaticGate,
    pub human_review: HumanReviewStatus,
    pub games: Vec<CorpusGameSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReviewRubricItem {
    pub id: String,
    pub question: String,
    pub pass_rule: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReviewEvidence {
    pub tick: u32,
    pub clip_elapsed_ms: i64,
    pub kind: String,
    pub details: Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct HumanReviewFields {
    pub causal_clarity: Option<bool>,
    pub visible_skill: Option<bool>,
    pub fair_credit: Option<bool>,
    pub clip_integrity: Option<bool>,
    pub proud_to_show: Option<bool>,
    pub verdict: Option<String>,
    pub rejection_codes: Vec<String>,
    pub reviewer: Option<String>,
    pub notes: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReviewEntry {
    pub rank: usize,
    pub corpus_index: usize,
    pub game_id: u32,
    pub seed: u64,
    pub mode: String,
    pub duration_ms: u64,
    pub clip_file: String,
    pub selected: SelectedHighlightSummary,
    pub evidence: Vec<ReviewEvidence>,
    pub review: HumanReviewFields,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReviewManifest {
    pub schema_version: u16,
    pub corpus_id: String,
    pub minimum_deserved_reviews: usize,
    pub instruction: String,
    pub rejection_codes: BTreeMap<String, String>,
    pub rubric: Vec<ReviewRubricItem>,
    pub entries: Vec<ReviewEntry>,
}

#[derive(Debug)]
pub struct CalibrationRun {
    pub summary: CalibrationSummary,
    pub review_manifest: ReviewManifest,
    review_clips: Vec<(String, HighlightClip)>,
}

#[derive(Debug)]
struct GeneratedGame {
    summary: CorpusGameSummary,
    recording: GameRecordingV1,
    clip: Option<HighlightClip>,
}

#[derive(Debug)]
struct ReviewCandidate {
    summary: CorpusGameSummary,
    clip: HighlightClip,
    evidence: Vec<ReviewEvidence>,
}

/// Generate and score a deterministic corpus. Corpus indices alternate
/// canonical Duel/2v2 and Quickmatch/Competitive combinations. Four fifths
/// run the networked bot's exact 100 ms policy immediately; one fifth holds a
/// declared 60-second presence-only prelude before enabling that same policy.
/// The latter supplies the PRD's two-minute cohort because the stock bot
/// finishes every sampled match in under two minutes. Score limits, engine
/// ticks, commands, collisions, and replay events remain canonical.
pub fn run_bot_corpus(spec: &BotCorpusSpec, config: &HighlightConfig) -> Result<CalibrationRun> {
    ensure!(spec.games > 0, "bot corpus needs at least one game");
    ensure!(spec.review_count > 0, "review count needs to be positive");

    let mut games = Vec::with_capacity(spec.games);
    let mut review_candidates = Vec::new();
    for corpus_index in 0..spec.games {
        let seed = corpus_seed(spec.seed_base, corpus_index);
        let generated = generate_game(corpus_index, seed, config)?;
        if let Some(clip) = generated.clip {
            review_candidates.push(ReviewCandidate {
                summary: generated.summary.clone(),
                evidence: review_evidence(&generated.recording, &clip),
                clip,
            });
        }
        games.push(generated.summary);
    }

    let completed_games = games.iter().filter(|game| game.completed).count();
    let non_trivial_games = games.iter().filter(|game| game.non_trivial).count();
    let non_trivial_games_with_potg = games
        .iter()
        .filter(|game| game.non_trivial && game.selected.is_some())
        .count();
    let production_rate_bps = ratio_bps(non_trivial_games_with_potg, non_trivial_games);

    let mut category_counts = BTreeMap::new();
    for category in [
        HighlightCategory::Demolition,
        HighlightCategory::Banking,
        HighlightCategory::Combo,
        HighlightCategory::Frenzy,
    ] {
        category_counts.insert(category, 0);
    }
    for game in &games {
        if let Some(selected) = &game.selected {
            *category_counts.entry(selected.category).or_default() += 1;
        }
    }
    let team_mode_winners = category_counts.values().sum();
    let category_share_bps = category_counts
        .iter()
        .map(|(category, count)| (*category, ratio_bps(*count, team_mode_winners)))
        .collect::<BTreeMap<_, _>>();
    let largest_category_share_bps = category_share_bps
        .values()
        .copied()
        .max()
        .unwrap_or_default();
    let production_rate_pass =
        non_trivial_games > 0 && production_rate_bps >= MIN_PRODUCTION_RATE_BPS;
    let category_balance_pass =
        team_mode_winners > 0 && largest_category_share_bps <= MAX_CATEGORY_SHARE_BPS;

    let corpus_id = format!(
        "bot-v1-{}-{seed:016x}-rules-{}",
        spec.games,
        config.rules_version,
        seed = spec.seed_base
    );
    let review_count = spec.review_count.min(team_mode_winners);
    let required_deserved = review_count.saturating_mul(80).div_ceil(100);

    let summary = CalibrationSummary {
        schema_version: CALIBRATION_SCHEMA_VERSION,
        corpus_id: corpus_id.clone(),
        generator: "authoritative GameState + common::calculate_ai_command at live 100 ms cadence (80% immediate; 20% declared 60 s presence-only prelude for long-form cohort)".into(),
        config: config.clone(),
        games_requested: spec.games,
        games_generated: games.len(),
        completed_games,
        truncated_games: games.len().saturating_sub(completed_games),
        non_trivial_games,
        non_trivial_games_with_potg,
        production_rate_bps,
        team_mode_winners,
        category_counts,
        category_share_bps,
        largest_category_share_bps,
        thresholds: CalibrationThresholds {
            minimum_production_rate_bps: MIN_PRODUCTION_RATE_BPS,
            maximum_category_share_bps: MAX_CATEGORY_SHARE_BPS,
        },
        automatic_gate: AutomaticGate {
            production_rate_pass,
            category_balance_pass,
            passed: production_rate_pass && category_balance_pass,
        },
        human_review: HumanReviewStatus {
            required_reviews: review_count,
            required_deserved,
            completed_reviews: 0,
            deserved_reviews: 0,
            status: "pending_human_review".into(),
            note: "No human verdicts are synthesized. Record verdicts in the review manifest; the gate passes at 80% deserved.".into(),
        },
        games,
    };

    let (review_manifest, review_clips) =
        build_review_manifest(&corpus_id, review_candidates, review_count);
    Ok(CalibrationRun {
        summary,
        review_manifest,
        review_clips,
    })
}

fn corpus_seed(seed_base: u64, corpus_index: usize) -> u64 {
    // SplitMix64's output transform gives neighboring corpus indices unrelated
    // engine streams while staying stable across Rust/rand releases.
    let mut value =
        seed_base.wrapping_add((corpus_index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn generate_game(
    corpus_index: usize,
    seed: u64,
    config: &HighlightConfig,
) -> Result<GeneratedGame> {
    let (per_team, queue_mode, ai_start_delay_ms) = match corpus_index % 5 {
        0 => (1, QueueMode::Quickmatch, 0),
        1 => (2, QueueMode::Quickmatch, 0),
        2 => (1, QueueMode::Competitive, 0),
        3 => (2, QueueMode::Competitive, 0),
        _ => (1, QueueMode::Competitive, LONG_FORM_AI_START_DELAY_MS),
    };
    let player_count = usize::from(per_team) * 2;
    let game_id = 8_000_000_u32
        .checked_add(u32::try_from(corpus_index).context("corpus index exceeds u32")?)
        .context("corpus game id overflow")?;
    let mut state = GameState::new(
        60,
        40,
        GameType::TeamMatch { per_team },
        queue_mode.clone(),
        Some(seed),
        0,
    );
    let mut player_snakes = Vec::with_capacity(player_count);
    for player_index in 0..player_count {
        let user_id = 10_000_u32
            .checked_add(u32::try_from(player_index).context("player index exceeds u32")?)
            .context("calibration user id overflow")?;
        let player = state
            .add_player(user_id, Some(format!("Corpus Bot {}", player_index + 1)))
            .context("failed to add corpus bot")?;
        player_snakes.push((user_id, player.snake_id));
    }
    state.spawn_initial_food();
    state.status = GameStatus::Started { server_id: 1 };

    let activation = state.clone();
    let tick_ms = state.properties.tick_duration_ms.max(1);
    let command_interval_ticks = BOT_COMMAND_INTERVAL_MS.div_ceil(tick_ms).max(1);
    let ai_start_tick = ai_start_delay_ms.div_ceil(tick_ms);
    let anchor_interval_ticks = REPLAY_ANCHOR_INTERVAL_MS.div_ceil(tick_ms).max(1);
    let mut anchors = vec![ReplayAnchor {
        tick: activation.tick,
        sequence: 0,
        state: activation,
    }];
    let mut messages = Vec::new();
    let mut recording_sequence = 1_u64;
    let mut server_command_sequence = 0_u32;
    let mut client_sequences = HashMap::<u32, u32>::new();

    while !state.is_complete() && state.tick < MAX_HIGHLIGHT_SELECTION_TICKS {
        let current_tick = state.tick;
        if current_tick.is_multiple_of(command_interval_ticks) {
            // Every live bot decides from the same authoritative pre-admission
            // geometry. Admitting one command only mutates queues/activity, so
            // this also matches concurrent clients reaching the executor in a
            // deterministic user-id order.
            let decisions = player_snakes
                .iter()
                .filter_map(|(user_id, snake_id)| {
                    let command = if current_tick < ai_start_tick {
                        state
                            .arena
                            .snakes
                            .get(*snake_id as usize)
                            .and_then(|snake| {
                                snake.is_alive.then_some(GameCommand::PlayerActivity {
                                    snake_id: *snake_id,
                                })
                            })
                    } else {
                        calculate_ai_command(&state, *snake_id)
                    };
                    command.map(|command| (*user_id, *snake_id, command))
                })
                .collect::<Vec<_>>();
            for (user_id, _snake_id, command) in decisions {
                let client_sequence = client_sequences.entry(user_id).or_default();
                *client_sequence = client_sequence
                    .checked_add(1)
                    .context("client command sequence overflow")?;
                let command_message = GameCommandMessage {
                    command_id_client: CommandId {
                        tick: current_tick,
                        user_id,
                        sequence_number: *client_sequence,
                    },
                    command_id_server: Some(CommandId {
                        tick: current_tick,
                        user_id,
                        sequence_number: server_command_sequence,
                    }),
                    command,
                };
                server_command_sequence = server_command_sequence
                    .checked_add(1)
                    .context("server command sequence overflow")?;
                let event = GameEvent::CommandScheduledV2 {
                    command_id: ClientCommandIdentityV2 {
                        game_id,
                        user_id,
                        client_game_session_id: format!("calibration-{game_id}-{user_id}"),
                        sequence: u64::from(*client_sequence),
                    },
                    command_message,
                    deduplicated_replay: false,
                };
                state.apply_event(event.clone(), None);
                push_message(&mut messages, &mut recording_sequence, current_tick, event)?;
            }
        }

        let events = state.tick_forward(false)?;
        let post_tick = state.tick;
        for (_, event) in events {
            if matches!(
                event,
                GameEvent::Snapshot { .. } | GameEvent::TickHash { .. }
            ) {
                continue;
            }
            push_message(&mut messages, &mut recording_sequence, post_tick, event)?;
        }
        if post_tick.is_multiple_of(anchor_interval_ticks) {
            anchors.push(ReplayAnchor {
                tick: post_tick,
                sequence: recording_sequence.saturating_sub(1),
                state: state.clone(),
            });
        }
    }

    let end_tick = state.tick;
    let recording = GameRecordingV1 {
        format_version: GAME_RECORDING_FORMAT_VERSION,
        gameplay_version: GAMEPLAY_REPLAY_VERSION,
        game_id,
        visibility: ReplayVisibility::Public,
        anchors,
        messages,
        end_tick,
        end_sync_hash: state.sync_hash(),
    };
    recording
        .verify_end_hash()
        .context("generated corpus recording is invalid or does not replay")?;
    // Production only selects after completion. A seed that reaches the hard
    // scorer span without finishing is reported as truncated and cannot
    // contribute a synthetic winner to either acceptance metric.
    let candidate = if state.is_complete() {
        score_highlight_candidate(&recording, config).context("corpus highlight scoring failed")?
    } else {
        None
    };
    let clip = candidate
        .clone()
        .filter(|candidate| candidate.score >= config.minimum_score);
    if let Some(clip) = &clip {
        clip.replay_and_verify()
            .context("selected corpus clip does not replay")?;
    }

    let active_players = state
        .players
        .keys()
        .filter(|user_id| {
            !state.is_player_idle_kicked(**user_id) && state.player_action_count(**user_id) > 0
        })
        .count();
    let duration_ms = state.elapsed_match_ms();
    let non_trivial =
        state.is_complete() && duration_ms >= NON_TRIVIAL_DURATION_MS && active_players >= 2;
    let roster = if per_team == 1 { "duel" } else { "2v2" };
    let queue = match queue_mode {
        QueueMode::Quickmatch => "quickmatch",
        QueueMode::Competitive => "competitive",
    };
    let profile = if ai_start_delay_ms == 0 {
        "bot_100ms"
    } else {
        "long_form_60s_prelude_then_bot_100ms"
    };
    let mode = format!("{roster}_{queue}_{profile}");
    let summary = CorpusGameSummary {
        corpus_index,
        game_id,
        seed,
        mode,
        players: player_count,
        active_players,
        completed: state.is_complete(),
        duration_ms,
        command_interval_ms: BOT_COMMAND_INTERVAL_MS,
        ai_start_delay_ms,
        non_trivial,
        message_count: recording.messages.len(),
        anchor_count: recording.anchors.len(),
        candidate: candidate.as_ref().map(SelectedHighlightSummary::from),
        selected: clip.as_ref().map(SelectedHighlightSummary::from),
    };
    Ok(GeneratedGame {
        summary,
        recording,
        clip,
    })
}

fn push_message(
    messages: &mut Vec<RecordedGameMessage>,
    next_sequence: &mut u64,
    tick: u32,
    event: GameEvent,
) -> Result<()> {
    messages.push(RecordedGameMessage {
        tick,
        sequence: *next_sequence,
        event,
    });
    *next_sequence = next_sequence
        .checked_add(1)
        .context("recording sequence overflow")?;
    Ok(())
}

fn ratio_bps(numerator: usize, denominator: usize) -> u32 {
    if denominator == 0 {
        return 0;
    }
    u32::try_from(numerator.saturating_mul(10_000) / denominator).unwrap_or(u32::MAX)
}

fn build_review_manifest(
    corpus_id: &str,
    mut selected: Vec<ReviewCandidate>,
    review_count: usize,
) -> (ReviewManifest, Vec<(String, HighlightClip)>) {
    selected.sort_by(|left, right| {
        right
            .clip
            .score
            .cmp(&left.clip.score)
            .then_with(|| left.summary.game_id.cmp(&right.summary.game_id))
    });
    selected.truncate(review_count);

    let mut entries = Vec::with_capacity(selected.len());
    let mut clips = Vec::with_capacity(selected.len());
    for (rank_index, candidate) in selected.into_iter().enumerate() {
        let ReviewCandidate {
            summary: game,
            clip,
            evidence,
        } = candidate;
        let clip_file = format!("clips/{:02}-game-{}.json", rank_index + 1, game.game_id);
        entries.push(ReviewEntry {
            rank: rank_index + 1,
            corpus_index: game.corpus_index,
            game_id: game.game_id,
            seed: game.seed,
            mode: game.mode,
            duration_ms: game.duration_ms,
            clip_file: clip_file.clone(),
            selected: SelectedHighlightSummary::from(&clip),
            evidence,
            review: HumanReviewFields::default(),
        });
        clips.push((clip_file, clip));
    }

    let minimum_deserved_reviews = entries.len().saturating_mul(80).div_ceil(100);
    (
        ReviewManifest {
            schema_version: CALIBRATION_SCHEMA_VERSION,
            corpus_id: corpus_id.to_owned(),
            minimum_deserved_reviews,
            instruction: "Watch each clip, answer every rubric item independently, then set verdict to deserved only when all five answers are true. At least 80% deserved is required; leave fields null until a human reviews them.".into(),
            rejection_codes: BTreeMap::from([
                ("UNCLEAR_CAUSE".into(), "The star's causal role is not visually legible.".into()),
                ("ORDINARY_PLAY".into(), "The play clears math thresholds but lacks visible skill or drama.".into()),
                ("MISATTRIBUTED".into(), "Credit is unfair, including teammate grief, AFK farming, or staged repeat trading.".into()),
                ("CLIP_MISSING_FACT".into(), "A caption fact or decisive setup/payoff lies outside the clip.".into()),
                ("REPLAY_DEFECT".into(), "Desync, camera, legibility, or playback behavior compromises review.".into()),
            ]),
            rubric: review_rubric(),
            entries,
        },
        clips,
    )
}

fn review_rubric() -> Vec<ReviewRubricItem> {
    vec![
        ReviewRubricItem {
            id: "causal_clarity".into(),
            question: "Can a viewer identify the star's action as the cause of the featured elimination, bank, or chain?".into(),
            pass_rule: "The causal action and payoff are both visible; coincidence or an off-screen cause fails.".into(),
        },
        ReviewRubricItem {
            id: "visible_skill".into(),
            question: "Does the clip visibly demonstrate more than an ordinary bare elimination or routine pickup?".into(),
            pass_rule: "A cutoff, trap, meaningful boost, laden takedown, large bank, sustained combo, or comparable readable skill is visible.".into(),
        },
        ReviewRubricItem {
            id: "fair_credit".into(),
            question: "Is the attribution fair and resistant to griefing or farming?".into(),
            pass_rule: "Fail teammate grief, inactive-victim farming, or staged/repeated kill trading even if the numeric score is high.".into(),
        },
        ReviewRubricItem {
            id: "clip_integrity".into(),
            question: "Are setup, all caption facts, and the payoff contained and legible in the clip?".into(),
            pass_rule: "The payoff is visible, the replay is coherent, and no caption claim depends on an event outside the clip.".into(),
        },
        ReviewRubricItem {
            id: "proud_to_show".into(),
            question: "Would you be proud to show this as the match's Play of the Game?".into(),
            pass_rule: "Use a holistic yes only after the other four checks pass; this is the subjective launch gate.".into(),
        },
    ]
}

fn review_evidence(recording: &GameRecordingV1, clip: &HighlightClip) -> Vec<ReviewEvidence> {
    let tick_ms = i64::from(clip.anchor.properties.tick_duration_ms.max(1));
    recording
        .messages
        .iter()
        .filter(|message| {
            message.tick >= clip.window.start_tick && message.tick <= clip.window.end_tick
        })
        .filter_map(|message| {
            let (kind, include) = match &message.event {
                GameEvent::SnakeDied { snake_id, cause } => {
                    let related = *snake_id == clip.star_snake_id
                        || matches!(cause, DeathCause::SnakeBody { killer_snake_id } if *killer_snake_id == clip.star_snake_id)
                        || matches!(cause, DeathCause::HeadToHead { other_snake_id } if *other_snake_id == clip.star_snake_id);
                    ("death", related)
                }
                GameEvent::FoodEaten { snake_id, .. } => {
                    ("star_food", *snake_id == clip.star_snake_id)
                }
                GameEvent::SnakeTurned { snake_id, .. } => {
                    let lookback_ticks = 600_u32
                        .div_ceil(clip.anchor.properties.tick_duration_ms.max(1))
                        .max(1);
                    (
                        "star_turn_near_focus",
                        *snake_id == clip.star_snake_id
                            && message.tick.saturating_add(lookback_ticks)
                                >= clip.window.focus_tick
                            && message.tick <= clip.window.focus_tick,
                    )
                }
                GameEvent::TeamScoreUpdated { .. } => ("team_score", true),
                _ => ("", false),
            };
            include.then(|| ReviewEvidence {
                tick: message.tick,
                clip_elapsed_ms: i64::from(message.tick)
                    .saturating_sub(i64::from(clip.window.start_tick))
                    .saturating_mul(tick_ms),
                kind: kind.into(),
                details: serde_json::to_value(&message.event)
                    .unwrap_or_else(|_| json!({ "serialization_error": true })),
            })
        })
        .collect()
}

pub fn write_calibration_artifacts(run: &CalibrationRun, output_dir: &Path) -> Result<()> {
    fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create {}", output_dir.display()))?;
    let clips_dir = output_dir.join("clips");
    fs::create_dir_all(&clips_dir)
        .with_context(|| format!("failed to create {}", clips_dir.display()))?;

    write_json(output_dir.join("corpus-summary.json"), &run.summary)?;
    write_json(output_dir.join("top-20-review.json"), &run.review_manifest)?;
    for (relative, clip) in &run.review_clips {
        write_json(output_dir.join(relative), clip)?;
    }
    fs::write(
        output_dir.join("top-20-review.html"),
        render_review_html(&run.review_manifest),
    )
    .context("failed to write review HTML")?;
    fs::write(
        output_dir.join("review-template.csv"),
        render_review_csv(&run.review_manifest),
    )
    .context("failed to write review CSV")?;
    Ok(())
}

fn write_json(path: PathBuf, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(&path, bytes).with_context(|| format!("failed to write {}", path.display()))
}

fn render_review_csv(manifest: &ReviewManifest) -> String {
    let mut output = String::from(
        "rank,game_id,causal_clarity,visible_skill,fair_credit,clip_integrity,proud_to_show,verdict,rejection_codes,reviewer,notes\n",
    );
    for entry in &manifest.entries {
        output.push_str(&format!("{},{},,,,,,,,,\n", entry.rank, entry.game_id));
    }
    output
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn render_review_html(manifest: &ReviewManifest) -> String {
    let mut rows = String::new();
    for entry in &manifest.entries {
        let evidence = serde_json::to_string_pretty(&entry.evidence).unwrap_or_default();
        rows.push_str(&format!(
            "<tr><td>{rank}</td><td>{game}</td><td>{mode}</td><td>{reason}</td><td>{score}</td><td><a href=\"{clip}\">clip JSON</a></td><td><details><summary>{count} events</summary><pre>{evidence}</pre></details></td><td class=\"checks\">&#9744; causal<br>&#9744; skill<br>&#9744; fair<br>&#9744; integrity<br>&#9744; proud</td></tr>",
            rank = entry.rank,
            game = entry.game_id,
            mode = html_escape(&entry.mode),
            reason = html_escape(&format!("{:?}", entry.selected.reason)),
            score = entry.selected.score,
            clip = html_escape(&entry.clip_file),
            count = entry.evidence.len(),
            evidence = html_escape(&evidence),
        ));
    }
    let rubric = manifest
        .rubric
        .iter()
        .map(|item| {
            format!(
                "<li><strong>{}</strong>: {} <em>{}</em></li>",
                html_escape(&item.id),
                html_escape(&item.question),
                html_escape(&item.pass_rule)
            )
        })
        .collect::<String>();
    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>Snaketron PotG review</title><style>body{{font:15px system-ui;margin:24px;background:#08110f;color:#e9fff5}}h1{{color:#5fffc0}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #315044;padding:8px;vertical-align:top}}th{{background:#173128}}pre{{max-width:52rem;white-space:pre-wrap}}a{{color:#77dfff}}.checks{{white-space:nowrap}}</style></head><body><h1>Snaketron PotG top-{count} review</h1><p>{instruction}</p><p><strong>Required:</strong> {required} deserved clips.</p><ol>{rubric}</ol><table><thead><tr><th>#</th><th>Game</th><th>Mode</th><th>Reason</th><th>Score</th><th>Payload</th><th>Evidence</th><th>Human checks</th></tr></thead><tbody>{rows}</tbody></table></body></html>",
        count = manifest.entries.len(),
        instruction = html_escape(&manifest.instruction),
        required = manifest.minimum_deserved_reviews,
    )
}

/// Assert only the objective corpus gates. Human review intentionally remains
/// outside this function so automation can never manufacture the 80% verdict.
pub fn assert_automatic_acceptance(summary: &CalibrationSummary) -> Result<()> {
    ensure!(
        summary.games_generated == summary.games_requested,
        "generated {} of {} requested games",
        summary.games_generated,
        summary.games_requested
    );
    ensure!(
        summary.non_trivial_games > 0,
        "corpus contains no completed non-trivial matches"
    );
    ensure!(
        summary.production_rate_bps >= MIN_PRODUCTION_RATE_BPS,
        "PotG production rate is {} bps, below {}",
        summary.production_rate_bps,
        MIN_PRODUCTION_RATE_BPS
    );
    ensure!(
        summary.largest_category_share_bps <= MAX_CATEGORY_SHARE_BPS,
        "largest category share is {} bps, above {}",
        summary.largest_category_share_bps,
        MAX_CATEGORY_SHARE_BPS
    );
    if !summary.automatic_gate.passed {
        bail!("automatic calibration gate is not marked passed");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_derivation_is_stable_and_distinct() {
        assert_eq!(corpus_seed(DEFAULT_CORPUS_SEED, 0), 0x4361_c9af_1839_902b);
        assert_ne!(
            corpus_seed(DEFAULT_CORPUS_SEED, 0),
            corpus_seed(DEFAULT_CORPUS_SEED, 1)
        );
    }

    #[test]
    fn human_review_starts_blank_and_requires_eighty_percent() {
        let spec = BotCorpusSpec {
            games: 1,
            seed_base: DEFAULT_CORPUS_SEED,
            review_count: 1,
        };
        let run = run_bot_corpus(&spec, &HighlightConfig::default()).unwrap();
        assert!(
            run.review_manifest
                .entries
                .iter()
                .all(|entry| entry.review.verdict.is_none())
        );
        assert_eq!(
            run.review_manifest.minimum_deserved_reviews,
            run.review_manifest.entries.len()
        );
        assert_eq!(run.summary.human_review.status, "pending_human_review");
    }

    #[test]
    fn a_fixed_bot_game_replays_and_scores_identically() {
        let config = HighlightConfig::default();
        let left = generate_game(0, corpus_seed(DEFAULT_CORPUS_SEED, 0), &config).unwrap();
        let right = generate_game(0, corpus_seed(DEFAULT_CORPUS_SEED, 0), &config).unwrap();
        assert_eq!(left.summary, right.summary);
        assert_eq!(left.recording.end_sync_hash, right.recording.end_sync_hash);
        assert_eq!(
            left.recording.messages.len(),
            right.recording.messages.len()
        );
    }

    #[test]
    fn declared_long_form_cohort_is_completed_active_and_non_trivial() {
        let config = HighlightConfig::default();
        let game = generate_game(4, corpus_seed(DEFAULT_CORPUS_SEED, 4), &config).unwrap();
        let interesting = game
            .recording
            .messages
            .iter()
            .filter(|message| {
                matches!(
                    message.event,
                    GameEvent::FoodEaten { .. }
                        | GameEvent::TeamScoreUpdated { .. }
                        | GameEvent::SnakeDied { .. }
                )
            })
            .count();
        assert!(game.summary.completed);
        assert!(game.summary.active_players >= 2);
        assert!(game.summary.non_trivial);
        assert!(interesting > 0);
        assert!(game.summary.candidate.is_some());
    }

    #[test]
    #[ignore = "runs the full deterministic 200-game launch calibration corpus"]
    fn launch_corpus_meets_automatic_acceptance() {
        let run = run_bot_corpus(&BotCorpusSpec::default(), &HighlightConfig::default()).unwrap();
        assert_automatic_acceptance(&run.summary).unwrap();
    }
}
