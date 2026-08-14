//! Public, cacheable news ticker assembled from authoritative game results.
//!
//! The feed intentionally has no hand-authored player achievements. Each
//! supported recent-game statistic becomes a [`MetricObservation`] in a
//! comparable cohort (persisted season, queue, game mode, metric), and the
//! same robust scorer decides whether it is exceptional. The scorer combines
//! an empirical tail percentile with median/MAD outlier magnitude, sample
//! confidence, and recency. Metric-specific code only extracts and truthfully
//! formats values; it does not contain achievement thresholds.

use axum::{Json, extract::State};
use chrono::{DateTime, Utc};
use common::{GameState, GameStatus, GameType, QueueMode, TeamId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::warn;

use crate::db::Database;
use crate::db::models::{Game, HighScoreEntry, NewsHighScoreSnapshot, NewsLeaderboardCoverage};
use crate::season::{Season, get_current_season};

const CACHE_TTL: Duration = Duration::from_secs(60);
const CACHE_TTL_SECONDS: u32 = 60;
const RECENT_GAME_LIMIT: usize = 256;
const MIN_COHORT_SIZE: usize = 20;
const MAX_FEED_ITEMS: usize = 8;
const MAX_STAT_ITEMS: usize = MAX_FEED_ITEMS - 1;
const MAX_ITEMS_PER_METRIC: usize = 2;
const MAX_ITEMS_PER_MODE: usize = 3;
const MIN_RATE_DURATION_MS: u64 = 30_000;
const SECONDS_PER_DAY: f64 = 86_400.0;
const FLAT_COHORT_MINIMUM_FOLD: f64 = 4.0;

/// A category the client may use for restrained visual treatment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum NewsTickerKind {
    System,
    Record,
    Performance,
    Ranking,
}

/// A semantic ticker action. The browser maps this closed set to local UI
/// state; the server remains the sole author of both factual copy and CTA.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum NewsTickerCtaAction {
    ViewLeaderboards,
    PlaySolo,
    PlayRankedSolo,
    PlayDuel,
    PlayTwoVsTwo,
    PlayFfa,
    PlayRankedDuel,
    PlayRankedTwoVsTwo,
    PlayRankedFfa,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct NewsTickerCta {
    pub label: String,
    pub action: NewsTickerCtaAction,
}

/// One already-formatted, truthful ticker headline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct NewsTickerItem {
    pub id: String,
    pub kind: NewsTickerKind,
    pub text: String,
    pub occurred_at: String,
    pub cta: Option<NewsTickerCta>,
}

/// Public response returned by `GET /api/news`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct NewsTickerResponse {
    pub items: Vec<NewsTickerItem>,
    pub generated_at: String,
    pub refresh_after_seconds: u32,
}

#[derive(Clone)]
pub struct NewsState {
    db: Arc<dyn Database>,
    cache: Arc<Mutex<Option<CachedNews>>>,
}

impl NewsState {
    pub fn new(db: Arc<dyn Database>) -> Self {
        Self {
            db,
            cache: Arc::new(Mutex::new(None)),
        }
    }
}

struct CachedNews {
    expires_at: Instant,
    response: NewsTickerResponse,
}

/// Return the current ticker feed. The mutex deliberately remains held while
/// an expired feed is rebuilt so a burst of home-page loads coalesces into one
/// set of DynamoDB reads rather than a cache stampede.
pub async fn get_news(State(state): State<NewsState>) -> Json<NewsTickerResponse> {
    let mut cache = state.cache.lock().await;
    if let Some(cached) = cache.as_ref()
        && cached.expires_at > Instant::now()
    {
        return Json(cached.response.clone());
    }

    let response = build_news_response(state.db.as_ref(), Utc::now()).await;
    *cache = Some(CachedNews {
        expires_at: Instant::now() + CACHE_TTL,
        response: response.clone(),
    });
    Json(response)
}

async fn build_news_response(db: &dyn Database, now: DateTime<Utc>) -> NewsTickerResponse {
    let season = get_current_season();
    let solo = GameType::Solo;

    let (games, high_score_snapshot) = tokio::join!(
        db.get_recent_completed_games(RECENT_GAME_LIMIT),
        db.get_news_high_score_snapshot(&solo, season),
    );

    let games = unwrap_source("recent completed games", games);
    let high_score_snapshot = unwrap_high_score_snapshot("Solo high scores", high_score_snapshot);

    assemble_news_response(&games, &high_score_snapshot, season, now)
}

fn assemble_news_response(
    games: &[Game],
    high_score_snapshot: &NewsHighScoreSnapshot,
    season: Season,
    now: DateTime<Utc>,
) -> NewsTickerResponse {
    let observations = observations_from_games(games, season);

    let exceptional_candidates = score_observations(&observations, now);
    let mut selected = select_diverse_candidates(exceptional_candidates, MAX_STAT_ITEMS);

    // Ordinary fallback headlines are still facts, never generic hype. Leader
    // claims come only from raw, ordered GSI heads; recent-result copy comes
    // only from completion records with verified public provenance.
    let mut fallback_candidates = Vec::new();
    if high_score_snapshot.coverage == NewsLeaderboardCoverage::OrderedGlobalIndex
        && let Some(leader) = high_score_snapshot.leader.as_ref()
        && let Some(candidate) = solo_leader_candidate(leader, season, now)
    {
        fallback_candidates.push(candidate);
    }
    fallback_candidates.extend(
        observations
            .iter()
            .filter_map(|observation| literal_result_candidate(observation, now)),
    );

    let remaining = MAX_STAT_ITEMS.saturating_sub(selected.len());
    selected.extend(select_diverse_candidates_after(
        fallback_candidates,
        remaining,
        &selected,
    ));

    let mut items = Vec::with_capacity(MAX_FEED_ITEMS);
    let system_count = match selected.len() {
        0 => 3,
        1 => 2,
        _ => 1,
    };
    items.extend(system_items(season, now, system_count));
    items.extend(selected.into_iter().map(|candidate| candidate.item));

    NewsTickerResponse {
        items,
        generated_at: now.to_rfc3339(),
        refresh_after_seconds: CACHE_TTL_SECONDS,
    }
}

fn unwrap_high_score_snapshot(
    source: &str,
    result: anyhow::Result<NewsHighScoreSnapshot>,
) -> NewsHighScoreSnapshot {
    match result {
        Ok(snapshot) => snapshot,
        Err(error) => {
            warn!(%error, source, "News ticker source was unavailable");
            NewsHighScoreSnapshot {
                leader: None,
                coverage: NewsLeaderboardCoverage::BoundedSample,
            }
        }
    }
}

fn unwrap_source<T>(source: &str, result: anyhow::Result<Vec<T>>) -> Vec<T> {
    match result {
        Ok(values) => values,
        Err(error) => {
            warn!(%error, source, "News ticker source was unavailable");
            Vec::new()
        }
    }
}

#[cfg(test)]
fn news_eligible_high_scores(entries: Vec<HighScoreEntry>) -> Vec<HighScoreEntry> {
    entries
        .into_iter()
        .filter(|entry| entry.news_eligible)
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum QueueKey {
    Casual,
    Ranked,
}

impl QueueKey {
    fn from_queue_mode(queue_mode: &QueueMode) -> Self {
        match queue_mode {
            QueueMode::Quickmatch => Self::Casual,
            QueueMode::Competitive => Self::Ranked,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Casual => "casual",
            Self::Ranked => "ranked",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ModeKey {
    Solo,
    Duel,
    TwoVsTwo,
    FreeForAll,
}

impl ModeKey {
    fn from_game_type(game_type: &GameType) -> Option<Self> {
        match game_type {
            GameType::Solo => Some(Self::Solo),
            GameType::TeamMatch { per_team: 1 } => Some(Self::Duel),
            GameType::TeamMatch { per_team: 2 } => Some(Self::TwoVsTwo),
            GameType::FreeForAll { .. } => Some(Self::FreeForAll),
            GameType::TeamMatch { .. } | GameType::Custom { .. } => None,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Solo => "Solo",
            Self::Duel => "Duel",
            Self::TwoVsTwo => "2v2",
            Self::FreeForAll => "FFA",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum MetricKey {
    SoloScore,
    PointsPerMinute,
    ActionsPerMinute,
    FastWin,
    TeamWinningMargin,
}

impl MetricKey {
    fn direction(self) -> Direction {
        match self {
            Self::FastWin => Direction::LowerIsBetter,
            Self::SoloScore
            | Self::PointsPerMinute
            | Self::ActionsPerMinute
            | Self::TeamWinningMargin => Direction::HigherIsBetter,
        }
    }

    fn kind(self) -> NewsTickerKind {
        NewsTickerKind::Performance
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    HigherIsBetter,
    LowerIsBetter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct CohortKey {
    season: Season,
    queue: QueueKey,
    mode: ModeKey,
    metric: MetricKey,
}

#[derive(Debug, Clone)]
enum CopyValue {
    SoloScore { score: u32 },
    Rate { per_minute: f64, is_team: bool },
    FastWin { duration_ms: u64 },
    Margin { points: u32 },
}

#[derive(Debug, Clone)]
struct MetricObservation {
    id: String,
    dedupe_key: String,
    subject_key: String,
    subject_name: String,
    cohort: CohortKey,
    value: f64,
    sample_confidence: f64,
    occurred_at: DateTime<Utc>,
    copy_value: CopyValue,
}

#[derive(Debug, Clone)]
struct Candidate {
    item: NewsTickerItem,
    dedupe_key: String,
    subject_key: String,
    metric: MetricKey,
    mode: ModeKey,
    exceptional_score: f64,
}

fn observations_from_games(games: &[Game], season: Season) -> Vec<MetricObservation> {
    let mut observations = Vec::new();
    for game in games {
        let Some((state, occurred_at)) = eligible_game_state(game, season) else {
            continue;
        };
        let Some(mode) = ModeKey::from_game_type(&state.game_type) else {
            continue;
        };
        let queue = QueueKey::from_queue_mode(&state.queue_mode);
        let duration_ms = state.elapsed_match_ms();

        match &state.game_type {
            GameType::TeamMatch { .. } => observations.extend(team_game_observations(
                game.id,
                &state,
                occurred_at,
                season,
                queue,
                mode,
                duration_ms,
            )),
            GameType::Solo | GameType::FreeForAll { .. } => {
                observations.extend(field_game_observations(
                    game.id,
                    &state,
                    occurred_at,
                    season,
                    queue,
                    mode,
                    duration_ms,
                ))
            }
            GameType::Custom { .. } => {}
        }

        if duration_ms >= MIN_RATE_DURATION_MS {
            for (user_id, actions) in &state.player_action_counts {
                if state.is_player_idle_kicked(*user_id) {
                    continue;
                }
                let subject_name = username_for(&state, *user_id);
                let per_minute = per_minute(u64::from(*actions), duration_ms);
                observations.push(MetricObservation {
                    id: format!("game:{}:user:{}:actions-per-minute", game.id, user_id),
                    dedupe_key: format!(
                        "season:{season}:{}:{}:user:{user_id}:actions-per-minute",
                        queue.label(),
                        mode.label()
                    ),
                    subject_key: format!("user:{user_id}"),
                    subject_name,
                    cohort: CohortKey {
                        season,
                        queue,
                        mode,
                        metric: MetricKey::ActionsPerMinute,
                    },
                    value: per_minute,
                    sample_confidence: 1.0,
                    occurred_at,
                    copy_value: CopyValue::Rate {
                        per_minute,
                        is_team: false,
                    },
                });
            }
        }
    }
    observations
}

fn eligible_game_state(game: &Game, season: Season) -> Option<(GameState, DateTime<Utc>)> {
    if !game.news_eligible || game.is_private || game.season != Some(season) {
        return None;
    }
    let state: GameState = serde_json::from_value(game.game_state.clone()?).ok()?;
    if state.is_stress_test
        || state.completed_by_inactivity
        || state.game_code.is_some()
        || !matches!(state.status, GameStatus::Complete { .. })
    {
        return None;
    }
    Some((state, game.ended_at.unwrap_or(game.last_activity)))
}

#[allow(clippy::too_many_arguments)]
fn field_game_observations(
    game_id: i32,
    state: &GameState,
    occurred_at: DateTime<Utc>,
    season: Season,
    queue: QueueKey,
    mode: ModeKey,
    duration_ms: u64,
) -> Vec<MetricObservation> {
    let mut observations = Vec::new();
    let mut scores: Vec<(u32, u32)> = state
        .players
        .iter()
        .filter(|(user_id, _)| !state.is_player_idle_kicked(**user_id))
        .map(|(user_id, player)| {
            (
                *user_id,
                state.scores.get(&player.snake_id).copied().unwrap_or(0),
            )
        })
        .collect();
    scores.sort_unstable_by_key(|(user_id, _)| *user_id);

    for (user_id, score) in &scores {
        let subject_name = username_for(state, *user_id);
        if mode == ModeKey::Solo {
            observations.push(MetricObservation {
                id: format!("game:{game_id}:user:{user_id}:solo-score"),
                dedupe_key: format!("season:{season}:solo:user:{user_id}:solo-score"),
                subject_key: format!("user:{user_id}"),
                subject_name: subject_name.clone(),
                cohort: CohortKey {
                    season,
                    queue,
                    mode,
                    metric: MetricKey::SoloScore,
                },
                value: f64::from(*score),
                sample_confidence: 1.0,
                occurred_at,
                copy_value: CopyValue::SoloScore { score: *score },
            });
        }

        if duration_ms >= MIN_RATE_DURATION_MS {
            let points_per_minute = per_minute(u64::from(*score), duration_ms);
            observations.push(MetricObservation {
                id: format!("game:{game_id}:user:{user_id}:points-per-minute"),
                dedupe_key: format!(
                    "season:{season}:{}:{}:user:{user_id}:points-per-minute",
                    queue.label(),
                    mode.label()
                ),
                subject_key: format!("user:{user_id}"),
                subject_name,
                cohort: CohortKey {
                    season,
                    queue,
                    mode,
                    metric: MetricKey::PointsPerMinute,
                },
                value: points_per_minute,
                sample_confidence: 1.0,
                occurred_at,
                copy_value: CopyValue::Rate {
                    per_minute: points_per_minute,
                    is_team: false,
                },
            });
        }
    }

    // FFA has a score-defined winner. A tie is deliberately not presented as
    // a fast win because there is no single player for the copy to credit.
    if mode == ModeKey::FreeForAll
        && duration_ms > 0
        && let Some((winner_id, _)) = unique_highest(&scores)
    {
        observations.push(fast_win_observation(
            game_id,
            format!("user:{winner_id}"),
            username_for(state, winner_id),
            occurred_at,
            season,
            queue,
            mode,
            duration_ms,
        ));
    }

    observations
}

#[allow(clippy::too_many_arguments)]
fn team_game_observations(
    game_id: i32,
    state: &GameState,
    occurred_at: DateTime<Utc>,
    season: Season,
    queue: QueueKey,
    mode: ModeKey,
    duration_ms: u64,
) -> Vec<MetricObservation> {
    let Some(team_scores) = state.team_scores.as_ref() else {
        return Vec::new();
    };
    let GameStatus::Complete { winning_snake_id } = &state.status else {
        return Vec::new();
    };
    let winning_team = (*winning_snake_id).and_then(|snake_id| {
        state
            .arena
            .snakes
            .get(snake_id as usize)
            .and_then(|snake| snake.team_id)
    });
    let mut observations = Vec::new();

    let mut ordered_scores: Vec<(TeamId, u32)> = team_scores
        .iter()
        .map(|(team, score)| (*team, *score))
        .collect();
    ordered_scores.sort_unstable_by_key(|(team, _)| *team);
    for (team, score) in &ordered_scores {
        if duration_ms < MIN_RATE_DURATION_MS {
            continue;
        }
        let subject = team_subject(state, *team, game_id);
        let points_per_minute = per_minute(u64::from(*score), duration_ms);
        observations.push(MetricObservation {
            id: format!("game:{game_id}:team:{}:points-per-minute", team.0),
            dedupe_key: format!(
                "season:{season}:{}:{}:{}:points-per-minute",
                queue.label(),
                mode.label(),
                subject.key
            ),
            subject_key: subject.key,
            subject_name: subject.name,
            cohort: CohortKey {
                season,
                queue,
                mode,
                metric: MetricKey::PointsPerMinute,
            },
            value: points_per_minute,
            sample_confidence: 1.0,
            occurred_at,
            copy_value: CopyValue::Rate {
                per_minute: points_per_minute,
                is_team: true,
            },
        });
    }

    if let Some(winning_team) = winning_team {
        let winner_score = team_scores.get(&winning_team).copied().unwrap_or(0);
        let runner_up = team_scores
            .iter()
            .filter_map(|(team, score)| (*team != winning_team).then_some(*score))
            .max();
        let subject = team_subject(state, winning_team, game_id);
        if let Some(runner_up) = runner_up
            && winner_score > runner_up
        {
            let margin = winner_score - runner_up;
            observations.push(MetricObservation {
                id: format!("game:{game_id}:team:{}:winning-margin", winning_team.0),
                dedupe_key: format!(
                    "season:{season}:{}:{}:{}:winning-margin",
                    queue.label(),
                    mode.label(),
                    subject.key
                ),
                subject_key: subject.key.clone(),
                subject_name: subject.name.clone(),
                cohort: CohortKey {
                    season,
                    queue,
                    mode,
                    metric: MetricKey::TeamWinningMargin,
                },
                value: f64::from(margin),
                sample_confidence: 1.0,
                occurred_at,
                copy_value: CopyValue::Margin { points: margin },
            });
        }
        if duration_ms > 0 {
            observations.push(fast_win_observation(
                game_id,
                subject.key,
                subject.name,
                occurred_at,
                season,
                queue,
                mode,
                duration_ms,
            ));
        }
    }

    observations
}

#[allow(clippy::too_many_arguments)]
fn fast_win_observation(
    game_id: i32,
    subject_key: String,
    subject_name: String,
    occurred_at: DateTime<Utc>,
    season: Season,
    queue: QueueKey,
    mode: ModeKey,
    duration_ms: u64,
) -> MetricObservation {
    MetricObservation {
        id: format!("game:{game_id}:{subject_key}:fast-win"),
        dedupe_key: format!(
            "season:{season}:{}:{}:{subject_key}:fast-win",
            queue.label(),
            mode.label()
        ),
        subject_key,
        subject_name,
        cohort: CohortKey {
            season,
            queue,
            mode,
            metric: MetricKey::FastWin,
        },
        value: duration_ms as f64,
        sample_confidence: 1.0,
        occurred_at,
        copy_value: CopyValue::FastWin { duration_ms },
    }
}

struct TeamSubject {
    key: String,
    name: String,
}

fn team_subject(state: &GameState, team_id: TeamId, game_id: i32) -> TeamSubject {
    let mut members: Vec<(u32, String)> = state
        .players
        .iter()
        .filter_map(|(user_id, player)| {
            let same_team = state
                .arena
                .snakes
                .get(player.snake_id as usize)
                .is_some_and(|snake| snake.team_id == Some(team_id));
            (same_team && !state.is_player_idle_kicked(*user_id))
                .then(|| (*user_id, username_for(state, *user_id)))
        })
        .collect();
    members.sort_unstable_by_key(|(user_id, _)| *user_id);
    let name = match members.as_slice() {
        [] => format!("Team {}", team_id.0 + 1),
        [(_, name)] => name.clone(),
        _ => members
            .iter()
            .map(|(_, name)| name.as_str())
            .collect::<Vec<_>>()
            .join(" & "),
    };
    TeamSubject {
        key: format!("game:{game_id}:team:{}", team_id.0),
        name,
    }
}

/// Classify all observations with one metric-agnostic robust rule.
///
/// An observation must be in a cohort of at least 20 finite values, occupy the
/// unique extreme 2.5% tail in the metric's declared direction, and sit at
/// least three robust standard deviations beyond the cohort median. Median
/// absolute deviation (with IQR and relative floors) prevents one extreme
/// value from inflating its own baseline. A cohort with no observed dispersion
/// is not assigned certainty from a synthetic numeric floor. Confidence
/// prevents weak source samples from qualifying, while recency ranks
/// already-qualified items; it never turns an ordinary value into an
/// achievement.
fn score_observations(observations: &[MetricObservation], now: DateTime<Utc>) -> Vec<Candidate> {
    let mut cohorts: HashMap<CohortKey, Vec<usize>> = HashMap::new();
    for (index, observation) in observations.iter().enumerate() {
        if observation.value.is_finite()
            && observation.sample_confidence.is_finite()
            && observation.occurred_at <= now
        {
            cohorts.entry(observation.cohort).or_default().push(index);
        }
    }

    let mut candidates = Vec::new();
    for indices in cohorts.values() {
        if indices.len() < MIN_COHORT_SIZE {
            continue;
        }
        let cohort_values: Vec<f64> = indices
            .iter()
            .map(|index| observations[*index].value)
            .collect();
        for index in indices {
            let observation = &observations[*index];
            let percentile = empirical_percentile(
                observation.value,
                &cohort_values,
                observation.cohort.metric.direction(),
            );
            // Require a unique directional extreme in addition to the
            // percentile cutoff. At some cohort sizes a shared maximum's
            // mid-rank lands exactly on 97.5%, and must still not become an
            // exclusive achievement claim.
            if percentile + f64::EPSILON < 0.975
                || !is_unique_directional_extreme(
                    observation.value,
                    &cohort_values,
                    observation.cohort.metric.direction(),
                )
            {
                continue;
            }

            let mut baseline = Vec::with_capacity(indices.len() - 1);
            for other_index in indices {
                if other_index != index {
                    baseline.push(observations[*other_index].value);
                }
            }
            let robust_z = robust_outlier_magnitude(
                observation.value,
                &baseline,
                observation.cohort.metric.direction(),
            );
            if robust_z < 3.0 {
                continue;
            }

            let cohort_confidence = ((indices.len() as f64) / 80.0).sqrt().min(1.0);
            let confidence = cohort_confidence * observation.sample_confidence.clamp(0.0, 1.0);
            if confidence < 0.35 {
                continue;
            }
            let recency = recency_score(observation.occurred_at, now);
            let magnitude = (robust_z / 6.0).tanh().clamp(0.0, 1.0);
            let exceptional_score =
                0.48 * percentile + 0.30 * magnitude + 0.14 * confidence + 0.08 * recency;
            if let Some(candidate) = candidate_from_observation(observation, exceptional_score) {
                candidates.push(candidate);
            }
        }
    }
    candidates
}

fn is_unique_directional_extreme(value: f64, values: &[f64], direction: Direction) -> bool {
    let extreme = match direction {
        Direction::HigherIsBetter => values.iter().copied().max_by(f64::total_cmp),
        Direction::LowerIsBetter => values.iter().copied().min_by(f64::total_cmp),
    };
    extreme == Some(value) && values.iter().filter(|sample| **sample == value).count() == 1
}

fn empirical_percentile(value: f64, values: &[f64], direction: Direction) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let (worse, tied) = values
        .iter()
        .fold((0usize, 0usize), |(worse, tied), other| {
            let is_worse = match direction {
                Direction::HigherIsBetter => *other < value,
                Direction::LowerIsBetter => *other > value,
            };
            if is_worse {
                (worse + 1, tied)
            } else if *other == value {
                (worse, tied + 1)
            } else {
                (worse, tied)
            }
        });
    (worse as f64 + 0.5 * tied as f64) / values.len() as f64
}

fn robust_outlier_magnitude(value: f64, baseline: &[f64], direction: Direction) -> f64 {
    if baseline.is_empty() || !value.is_finite() {
        return 0.0;
    }
    let center = median(baseline);
    let improvement = match direction {
        Direction::HigherIsBetter => value - center,
        Direction::LowerIsBetter => center - value,
    };
    if improvement <= 0.0 {
        return 0.0;
    }
    let deviations: Vec<f64> = baseline
        .iter()
        .map(|sample| (sample - center).abs())
        .collect();
    let mad_scale = 1.4826 * median(&deviations);
    let iqr_scale = (quantile(baseline, 0.75) - quantile(baseline, 0.25)).abs() / 1.349;
    let observed_scale = mad_scale.max(iqr_scale);
    if observed_scale <= f64::EPSILON {
        // Discrete cohorts often have a zero MAD/IQR even when their upper
        // tail establishes a useful unit (for example, mostly 0s with a few
        // 1s). In that case use one algorithm-wide multiplicative materiality
        // rule. A 2-vs-1 increment stays ordinary, while 100 against a
        // zero-heavy 0/1 baseline can qualify. If the entire baseline is zero,
        // there is no generic unit with which to distinguish 1 from 1,000, so
        // fail closed rather than inventing a metric-specific threshold.
        let magnitudes: Vec<f64> = baseline.iter().map(|sample| sample.abs()).collect();
        let reference = quantile(&magnitudes, 0.9).max(center.abs());
        if reference <= f64::EPSILON {
            return 0.0;
        }
        let fold = match direction {
            Direction::HigherIsBetter => value.abs() / reference,
            Direction::LowerIsBetter => {
                reference
                    / value
                        .abs()
                        .max(reference / FLAT_COHORT_MINIMUM_FOLD.powi(2))
            }
        };
        return if fold + f64::EPSILON >= FLAT_COHORT_MINIMUM_FOLD {
            fold
        } else {
            0.0
        };
    }
    let scale_floor = (center.abs() * 0.02).max(0.01);
    let scale = observed_scale.max(scale_floor);
    (improvement / scale).max(0.0)
}

fn median(values: &[f64]) -> f64 {
    quantile(values, 0.5)
}

fn quantile(values: &[f64], fraction: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted: Vec<f64> = values
        .iter()
        .copied()
        .filter(|value| value.is_finite())
        .collect();
    if sorted.is_empty() {
        return 0.0;
    }
    sorted.sort_by(f64::total_cmp);
    let position = fraction.clamp(0.0, 1.0) * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    if lower == upper {
        sorted[lower]
    } else {
        let weight = position - lower as f64;
        sorted[lower] * (1.0 - weight) + sorted[upper] * weight
    }
}

fn recency_score(occurred_at: DateTime<Utc>, now: DateTime<Utc>) -> f64 {
    let age_seconds = (now - occurred_at).num_seconds();
    if age_seconds < 0 {
        return 0.0;
    }
    let age_seconds = age_seconds as f64;
    0.5_f64.powf(age_seconds / (7.0 * SECONDS_PER_DAY))
}

fn candidate_from_observation(
    observation: &MetricObservation,
    exceptional_score: f64,
) -> Option<Candidate> {
    let variant = deterministic_variant(&observation.id, 2);
    let text = match (&observation.cohort.metric, &observation.copy_value, variant) {
        (MetricKey::SoloScore, CopyValue::SoloScore { score }, 0) => format!(
            "{} put up {} points in Solo!",
            observation.subject_name, score
        ),
        (MetricKey::SoloScore, CopyValue::SoloScore { score }, _) => format!(
            "{} went huge in Solo: {} points!",
            observation.subject_name, score
        ),
        (
            MetricKey::PointsPerMinute,
            CopyValue::Rate {
                per_minute,
                is_team,
            },
            0,
        ) => format!(
            "{} tore through {} {}: {} {}points/min!",
            observation.subject_name,
            observation.cohort.queue.label(),
            observation.cohort.mode.label(),
            format_decimal(*per_minute),
            if *is_team { "team " } else { "" }
        ),
        (
            MetricKey::PointsPerMinute,
            CopyValue::Rate {
                per_minute,
                is_team,
            },
            _,
        ) => format!(
            "{} hit overdrive in {} {}: {} {}points/min!",
            observation.subject_name,
            observation.cohort.queue.label(),
            observation.cohort.mode.label(),
            format_decimal(*per_minute),
            if *is_team { "team " } else { "" }
        ),
        (MetricKey::ActionsPerMinute, CopyValue::Rate { per_minute, .. }, 0) => format!(
            "{} was everywhere in {} {}: {} actions/min!",
            observation.subject_name,
            observation.cohort.queue.label(),
            observation.cohort.mode.label(),
            format_decimal(*per_minute)
        ),
        (MetricKey::ActionsPerMinute, CopyValue::Rate { per_minute, .. }, _) => format!(
            "{} went full send in {} {}: {} actions/min!",
            observation.subject_name,
            observation.cohort.queue.label(),
            observation.cohort.mode.label(),
            format_decimal(*per_minute)
        ),
        (MetricKey::FastWin, CopyValue::FastWin { duration_ms }, 0) => format!(
            "{} blitzed {} {} in {}!",
            observation.subject_name,
            observation.cohort.queue.label(),
            observation.cohort.mode.label(),
            format_duration(*duration_ms)
        ),
        (MetricKey::FastWin, CopyValue::FastWin { duration_ms }, _) => format!(
            "{} made {} {} look easy: a {} win!",
            observation.subject_name,
            observation.cohort.queue.label(),
            observation.cohort.mode.label(),
            format_duration(*duration_ms)
        ),
        (MetricKey::TeamWinningMargin, CopyValue::Margin { points }, 0) => format!(
            "{} steamrolled {} {} by {} points!",
            observation.subject_name,
            observation.cohort.queue.label(),
            observation.cohort.mode.label(),
            points
        ),
        (MetricKey::TeamWinningMargin, CopyValue::Margin { points }, _) => format!(
            "{} blew {} {} wide open: +{} points!",
            observation.subject_name,
            observation.cohort.queue.label(),
            observation.cohort.mode.label(),
            points
        ),
        // Observation construction keeps metric/copy pairs aligned. If a
        // future extractor violates that invariant, fail closed rather than
        // publishing an unsupported generic achievement.
        _ => return None,
    };
    Some(Candidate {
        item: NewsTickerItem {
            id: public_item_id(&observation.id),
            kind: observation.cohort.metric.kind(),
            text,
            occurred_at: observation.occurred_at.to_rfc3339(),
            cta: Some(play_cta(observation.cohort.queue, observation.cohort.mode)),
        },
        dedupe_key: observation.dedupe_key.clone(),
        subject_key: observation.subject_key.clone(),
        metric: observation.cohort.metric,
        mode: observation.cohort.mode,
        exceptional_score,
    })
}

/// The Solo score row is immutable. Once an ordered global read proves this
/// row is the unique current top score, its timestamp is a safe lower bound on
/// how long that score has remained unbeaten.
fn solo_leader_candidate(
    leader: &HighScoreEntry,
    season: Season,
    now: DateTime<Utc>,
) -> Option<Candidate> {
    if leader.season != season || leader.score <= 0 || !leader.news_eligible {
        return None;
    }
    let tenure = format_elapsed(leader.timestamp, now)?;
    let subject_name = safe_name(&leader.username);
    Some(Candidate {
        item: NewsTickerItem {
            id: public_item_id(&format!(
                "leader:season:{season}:solo:game:{}",
                leader.game_id
            )),
            kind: NewsTickerKind::Ranking,
            text: format!(
                "{} has held Solo's top score for at least {} — {} points!",
                subject_name,
                tenure,
                format_integer(leader.score)
            ),
            occurred_at: leader.timestamp.to_rfc3339(),
            cta: Some(NewsTickerCta {
                label: "Take a run.".to_string(),
                action: NewsTickerCtaAction::PlaySolo,
            }),
        },
        dedupe_key: format!("season:{season}:solo:user:{}:solo-score", leader.user_id),
        subject_key: format!("user:{}", leader.user_id),
        metric: MetricKey::SoloScore,
        mode: ModeKey::Solo,
        exceptional_score: 0.80 + 0.10 * recency_score(leader.timestamp, now),
    })
}

/// Turn a verified observation into plain recent-result copy for quiet news
/// periods. Unlike exceptional copy, this makes no comparison to other
/// players; every verb is supported directly by the stored value.
fn literal_result_candidate(
    observation: &MetricObservation,
    now: DateTime<Utc>,
) -> Option<Candidate> {
    let age = format_age_ago(observation.occurred_at, now)?;
    let queue = observation.cohort.queue.label();
    let mode = observation.cohort.mode.label();
    let text = match (&observation.cohort.metric, &observation.copy_value) {
        (MetricKey::SoloScore, CopyValue::SoloScore { score }) if *score > 0 => format!(
            "{} dropped {} points in Solo {}!",
            observation.subject_name,
            format_integer(*score),
            age
        ),
        (MetricKey::FastWin, CopyValue::FastWin { duration_ms }) if *duration_ms > 0 => format!(
            "{} won {} {} in {} — {}!",
            observation.subject_name,
            queue,
            mode,
            format_duration(*duration_ms),
            age
        ),
        (MetricKey::TeamWinningMargin, CopyValue::Margin { points }) if *points > 0 => format!(
            "{} won {} {} by {} points {}!",
            observation.subject_name,
            queue,
            mode,
            format_integer(*points),
            age
        ),
        _ => return None,
    };

    Some(Candidate {
        item: NewsTickerItem {
            id: public_item_id(&format!("literal:{}", observation.id)),
            kind: observation.cohort.metric.kind(),
            text,
            occurred_at: observation.occurred_at.to_rfc3339(),
            cta: Some(play_cta(observation.cohort.queue, observation.cohort.mode)),
        },
        dedupe_key: observation.dedupe_key.clone(),
        subject_key: observation.subject_key.clone(),
        metric: observation.cohort.metric,
        mode: observation.cohort.mode,
        exceptional_score: 0.25 + 0.15 * recency_score(observation.occurred_at, now),
    })
}

fn select_diverse_candidates(candidates: Vec<Candidate>, limit: usize) -> Vec<Candidate> {
    select_diverse_candidates_after(candidates, limit, &[])
}

fn select_diverse_candidates_after(
    mut candidates: Vec<Candidate>,
    limit: usize,
    existing: &[Candidate],
) -> Vec<Candidate> {
    candidates.sort_by(|left, right| {
        right
            .exceptional_score
            .total_cmp(&left.exceptional_score)
            .then_with(|| right.item.occurred_at.cmp(&left.item.occurred_at))
            .then_with(|| left.item.id.cmp(&right.item.id))
    });

    // First collapse equivalent claims (for example, the same Solo score
    // arriving through both recent-game and leaderboard data).
    let mut seen_dedupe: HashSet<String> = existing
        .iter()
        .map(|candidate| candidate.dedupe_key.clone())
        .collect();
    candidates.retain(|candidate| seen_dedupe.insert(candidate.dedupe_key.clone()));

    let mut selected = Vec::with_capacity(limit);
    let mut seen_ids: HashSet<String> = existing
        .iter()
        .map(|candidate| candidate.item.id.clone())
        .collect();
    let mut subjects: HashSet<String> = existing
        .iter()
        .map(|candidate| candidate.subject_key.clone())
        .collect();
    let mut metric_counts: HashMap<MetricKey, usize> = HashMap::new();
    let mut mode_counts: HashMap<ModeKey, usize> = HashMap::new();
    for candidate in existing {
        *metric_counts.entry(candidate.metric).or_default() += 1;
        *mode_counts.entry(candidate.mode).or_default() += 1;
    }

    // The first pass gives different players/teams and stat families room to
    // appear. A second pass fills spare slots without the one-subject limit.
    for unique_subjects_only in [true, false] {
        for candidate in &candidates {
            if selected.len() >= limit
                || seen_ids.contains(&candidate.item.id)
                || (unique_subjects_only && subjects.contains(&candidate.subject_key))
                || metric_counts.get(&candidate.metric).copied().unwrap_or(0)
                    >= MAX_ITEMS_PER_METRIC
                || mode_counts.get(&candidate.mode).copied().unwrap_or(0) >= MAX_ITEMS_PER_MODE
            {
                continue;
            }
            selected.push(candidate.clone());
            seen_ids.insert(candidate.item.id.clone());
            subjects.insert(candidate.subject_key.clone());
            *metric_counts.entry(candidate.metric).or_default() += 1;
            *mode_counts.entry(candidate.mode).or_default() += 1;
        }
    }
    selected
}

fn system_items(season: Season, now: DateTime<Utc>, limit: usize) -> Vec<NewsTickerItem> {
    const VARIANT_COUNT: usize = 8;
    let rotation_key = format!("season:{season}:minute:{}", now.timestamp() / 60);
    let start = deterministic_variant(&rotation_key, VARIANT_COUNT);
    (0..limit.min(VARIANT_COUNT))
        .map(|offset| system_item(season, now, (start + offset) % VARIANT_COUNT))
        .collect()
}

fn system_item(season: Season, now: DateTime<Utc>, variant: usize) -> NewsTickerItem {
    // The configured season is the only asserted fact here. Everything else
    // is an imperative prompt whose action maps to a supported lobby or route,
    // so an upstream database outage cannot manufacture an availability or
    // leaderboard claim.
    let (text, cta) = match variant % 8 {
        0 => (
            format!("Season {season} is on!"),
            NewsTickerCta {
                label: "Open the leaderboard.".to_string(),
                action: NewsTickerCtaAction::ViewLeaderboards,
            },
        ),
        1 => (
            format!("Season {season}: climb the ranks!"),
            NewsTickerCta {
                label: "Play Ranked.".to_string(),
                action: NewsTickerCtaAction::PlayRankedDuel,
            },
        ),
        2 => (
            format!("Season {season}: go big in Solo!"),
            NewsTickerCta {
                label: "Take a run.".to_string(),
                action: NewsTickerCtaAction::PlaySolo,
            },
        ),
        3 => (
            format!("Season {season}: pick a fight!"),
            NewsTickerCta {
                label: "Play Duel.".to_string(),
                action: NewsTickerCtaAction::PlayDuel,
            },
        ),
        4 => (
            format!("Season {season}: bring the backup!"),
            NewsTickerCta {
                label: "Play 2v2.".to_string(),
                action: NewsTickerCtaAction::PlayTwoVsTwo,
            },
        ),
        5 => (
            format!("Season {season}: own the arena!"),
            NewsTickerCta {
                label: "Play FFA.".to_string(),
                action: NewsTickerCtaAction::PlayFfa,
            },
        ),
        6 => (
            format!("Season {season}: make your move!"),
            NewsTickerCta {
                label: "Open the leaderboard.".to_string(),
                action: NewsTickerCtaAction::ViewLeaderboards,
            },
        ),
        _ => (
            format!("Season {season}: hit Ranked Solo!"),
            NewsTickerCta {
                label: "Take a ranked run.".to_string(),
                action: NewsTickerCtaAction::PlayRankedSolo,
            },
        ),
    };
    NewsTickerItem {
        id: public_item_id(&format!("system:season:{season}:variant:{}", variant % 8)),
        kind: NewsTickerKind::System,
        text,
        occurred_at: now.to_rfc3339(),
        cta: Some(cta),
    }
}

fn play_cta(queue: QueueKey, mode: ModeKey) -> NewsTickerCta {
    let action = match (queue, mode) {
        (QueueKey::Casual, ModeKey::Solo) => NewsTickerCtaAction::PlaySolo,
        (QueueKey::Ranked, ModeKey::Solo) => NewsTickerCtaAction::PlayRankedSolo,
        (QueueKey::Casual, ModeKey::Duel) => NewsTickerCtaAction::PlayDuel,
        (QueueKey::Casual, ModeKey::TwoVsTwo) => NewsTickerCtaAction::PlayTwoVsTwo,
        (QueueKey::Casual, ModeKey::FreeForAll) => NewsTickerCtaAction::PlayFfa,
        (QueueKey::Ranked, ModeKey::Duel) => NewsTickerCtaAction::PlayRankedDuel,
        (QueueKey::Ranked, ModeKey::TwoVsTwo) => NewsTickerCtaAction::PlayRankedTwoVsTwo,
        (QueueKey::Ranked, ModeKey::FreeForAll) => NewsTickerCtaAction::PlayRankedFfa,
    };
    NewsTickerCta {
        label: match queue {
            QueueKey::Ranked => "Queue up.",
            QueueKey::Casual if mode == ModeKey::Solo => "Take a run.",
            QueueKey::Casual => "Jump in.",
        }
        .to_string(),
        action,
    }
}

fn username_for(state: &GameState, user_id: u32) -> String {
    safe_name(
        state
            .usernames
            .get(&user_id)
            .map(String::as_str)
            .unwrap_or("Player"),
    )
}

fn safe_name(name: &str) -> String {
    let filtered = crate::chat_filter::filter_chat_message(name.trim());
    if filtered.is_empty() {
        "Player".to_string()
    } else {
        filtered
    }
}

fn per_minute(value: u64, duration_ms: u64) -> f64 {
    value as f64 * 60_000.0 / duration_ms as f64
}

fn unique_highest(values: &[(u32, u32)]) -> Option<(u32, u32)> {
    let highest = values.iter().map(|(_, value)| *value).max()?;
    let mut leaders = values
        .iter()
        .filter(|(_, value)| *value == highest)
        .copied();
    let winner = leaders.next()?;
    leaders.next().is_none().then_some(winner)
}

fn format_decimal(value: f64) -> String {
    if (value - value.round()).abs() < 0.05 {
        format!("{value:.0}")
    } else {
        format!("{value:.1}")
    }
}

fn format_integer(value: impl Into<i64>) -> String {
    let value = value.into();
    let digits = value.unsigned_abs().to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, character) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(character);
    }
    if value < 0 {
        grouped.insert(0, '-');
    }
    grouped
}

fn format_elapsed(occurred_at: DateTime<Utc>, now: DateTime<Utc>) -> Option<String> {
    let seconds = (now - occurred_at).num_seconds();
    if seconds < 0 {
        return None;
    }
    let (value, unit) = if seconds < 60 {
        return Some("under a minute".to_string());
    } else if seconds < 60 * 60 {
        (seconds / 60, "minute")
    } else if seconds < 24 * 60 * 60 {
        (seconds / (60 * 60), "hour")
    } else {
        (seconds / (24 * 60 * 60), "day")
    };
    Some(format!(
        "{value} {unit}{}",
        if value == 1 { "" } else { "s" }
    ))
}

fn format_age_ago(occurred_at: DateTime<Utc>, now: DateTime<Utc>) -> Option<String> {
    let elapsed = format_elapsed(occurred_at, now)?;
    Some(if elapsed == "under a minute" {
        "just now".to_string()
    } else {
        format!("{elapsed} ago")
    })
}

fn format_duration(duration_ms: u64) -> String {
    let total_seconds = duration_ms / 1_000;
    if total_seconds >= 60 {
        format!("{}:{:02}", total_seconds / 60, total_seconds % 60)
    } else {
        format!("{:.1} seconds", duration_ms as f64 / 1_000.0)
    }
}

fn deterministic_variant(key: &str, variants: usize) -> usize {
    if variants <= 1 {
        return 0;
    }
    // FNV-1a is tiny, stable across processes, and sufficient for copy choice.
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash as usize) % variants
}

fn public_item_id(internal_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"snaketron-news-item-v1\0");
    hasher.update(internal_id.as_bytes());
    let digest = hasher.finalize();
    format!("news-{}", hex::encode(&digest[..12]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn test_time() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 14, 12, 0, 0)
            .single()
            .unwrap()
    }

    fn observation(id: usize, value: f64, metric: MetricKey, confidence: f64) -> MetricObservation {
        MetricObservation {
            id: format!("observation:{id}"),
            dedupe_key: format!("dedupe:{id}"),
            subject_key: format!("user:{id}"),
            subject_name: format!("Player{id}"),
            cohort: CohortKey {
                season: 0,
                queue: QueueKey::Ranked,
                mode: if metric == MetricKey::SoloScore {
                    ModeKey::Solo
                } else {
                    ModeKey::Duel
                },
                metric,
            },
            value,
            sample_confidence: confidence,
            occurred_at: test_time(),
            copy_value: match metric {
                MetricKey::FastWin => CopyValue::FastWin {
                    duration_ms: value.max(0.0) as u64,
                },
                MetricKey::SoloScore => CopyValue::SoloScore {
                    score: value.max(0.0) as u32,
                },
                MetricKey::TeamWinningMargin => CopyValue::Margin {
                    points: value.max(0.0) as u32,
                },
                MetricKey::PointsPerMinute | MetricKey::ActionsPerMinute => CopyValue::Rate {
                    per_minute: value,
                    is_team: false,
                },
            },
        }
    }

    fn completed_solo_game(news_eligible: bool, is_private: bool) -> Game {
        let occurred_at = test_time();
        let mut state = GameState::new(
            10,
            10,
            GameType::Solo,
            QueueMode::Quickmatch,
            Some(7),
            occurred_at.timestamp_millis() - 60_000,
        );
        let player = state.add_player(7, Some("Troncat89".to_string())).unwrap();
        state.scores.insert(player.snake_id, 77);
        state.status = GameStatus::Complete {
            winning_snake_id: Some(player.snake_id),
        };
        Game {
            id: 7,
            server_id: Some(1),
            season: Some(0),
            game_type: serde_json::to_value(&state.game_type).unwrap(),
            game_state: Some(serde_json::to_value(state).unwrap()),
            status: "complete".to_string(),
            ended_at: Some(occurred_at),
            last_activity: occurred_at,
            created_at: occurred_at,
            game_mode: "matchmaking".to_string(),
            is_private,
            game_code: None,
            news_eligible,
        }
    }

    #[test]
    fn recent_game_metrics_require_verified_public_provenance() {
        assert!(eligible_game_state(&completed_solo_game(true, false), 0).is_some());
        assert!(eligible_game_state(&completed_solo_game(false, false), 0).is_none());
        assert!(eligible_game_state(&completed_solo_game(true, true), 0).is_none());

        let mut wrong_season = completed_solo_game(true, false);
        wrong_season.season = Some(1);
        assert!(eligible_game_state(&wrong_season, 0).is_none());

        let mut legacy_without_season = completed_solo_game(true, false);
        legacy_without_season.season = None;
        assert!(eligible_game_state(&legacy_without_season, 0).is_none());
    }

    #[test]
    fn response_withholds_unproven_leaders_and_still_has_a_truthful_system_item() {
        let response = assemble_news_response(
            &[],
            &NewsHighScoreSnapshot {
                leader: Some(high_score(7, "sampled", 1_240)),
                coverage: NewsLeaderboardCoverage::BoundedSample,
            },
            0,
            test_time(),
        );

        assert_eq!(response.items.len(), 3);
        assert!(
            response
                .items
                .iter()
                .all(|item| item.kind == NewsTickerKind::System && !item.text.contains("Player7"))
        );
    }

    #[test]
    fn response_uses_only_current_season_results_and_proven_solo_tenure() {
        let mut leader = high_score(9, "record-game", 1_240);
        leader.username = "Lopatron33".to_string();
        leader.timestamp = test_time() - chrono::Duration::days(35);
        let snapshot = NewsHighScoreSnapshot {
            leader: Some(leader),
            coverage: NewsLeaderboardCoverage::OrderedGlobalIndex,
        };

        let current = assemble_news_response(
            &[completed_solo_game(true, false)],
            &snapshot,
            0,
            test_time(),
        );
        assert!(
            current
                .items
                .iter()
                .any(|item| item.text.contains("Lopatron33")
                    && item.text.contains("at least 35 days"))
        );
        assert!(
            current
                .items
                .iter()
                .any(|item| item.text.contains("Troncat89") && item.text.contains("77 points"))
        );

        let mut old_game = completed_solo_game(true, false);
        old_game.season = Some(1);
        let without_old_result = assemble_news_response(
            &[old_game],
            &NewsHighScoreSnapshot {
                leader: None,
                coverage: NewsLeaderboardCoverage::OrderedGlobalIndex,
            },
            0,
            test_time(),
        );
        assert_eq!(without_old_result.items.len(), 3);
        assert!(
            without_old_result
                .items
                .iter()
                .all(|item| item.kind == NewsTickerKind::System)
        );
    }

    #[test]
    fn robust_scorer_selects_only_the_true_high_outlier() {
        let mut observations: Vec<_> = (0..39)
            .map(|index| {
                observation(
                    index,
                    98.0 + (index % 5) as f64,
                    MetricKey::PointsPerMinute,
                    1.0,
                )
            })
            .collect();
        observations.push(observation(99, 180.0, MetricKey::PointsPerMinute, 1.0));

        let candidates = score_observations(&observations, test_time());
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].item.id, public_item_id("observation:99"));
    }

    #[test]
    fn ordinary_extreme_without_outlier_magnitude_does_not_qualify() {
        let observations: Vec<_> = (0..40)
            .map(|index| observation(index, index as f64, MetricKey::SoloScore, 1.0))
            .collect();
        assert!(score_observations(&observations, test_time()).is_empty());
    }

    #[test]
    fn sparse_cohort_never_manufactures_an_exception() {
        let mut observations: Vec<_> = (0..MIN_COHORT_SIZE - 1)
            .map(|index| observation(index, 10.0, MetricKey::SoloScore, 1.0))
            .collect();
        observations[0].value = 10_000.0;
        assert!(score_observations(&observations, test_time()).is_empty());
    }

    #[test]
    fn flat_cohort_does_not_turn_a_trivial_increment_into_news() {
        let mut observations: Vec<_> = (0..MIN_COHORT_SIZE)
            .map(|index| observation(index, 1.0, MetricKey::SoloScore, 1.0))
            .collect();
        observations[MIN_COHORT_SIZE - 1].value = 2.0;

        assert!(score_observations(&observations, test_time()).is_empty());
    }

    #[test]
    fn zero_heavy_cohort_uses_generic_fold_materiality_when_iqr_is_zero() {
        let mut observations: Vec<_> = (0..MIN_COHORT_SIZE)
            .map(|index| {
                let value = if index < 15 { 0.0 } else { 1.0 };
                observation(index, value, MetricKey::SoloScore, 1.0)
            })
            .collect();
        observations[MIN_COHORT_SIZE - 1].value = 100.0;

        let candidates = score_observations(&observations, test_time());
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].item.id,
            public_item_id(&format!("observation:{}", MIN_COHORT_SIZE - 1))
        );
    }

    #[test]
    fn all_zero_baseline_still_fails_closed_without_a_generic_unit() {
        let mut observations: Vec<_> = (0..MIN_COHORT_SIZE)
            .map(|index| observation(index, 0.0, MetricKey::SoloScore, 1.0))
            .collect();
        observations[MIN_COHORT_SIZE - 1].value = 100.0;

        assert!(score_observations(&observations, test_time()).is_empty());
    }

    #[test]
    fn tied_extreme_values_are_not_exclusive_outliers() {
        let mut observations: Vec<_> = (0..40)
            .map(|index| observation(index, 100.0, MetricKey::SoloScore, 1.0))
            .collect();
        observations[38].value = 1_000.0;
        observations[39].value = 1_000.0;
        assert!(score_observations(&observations, test_time()).is_empty());
    }

    #[test]
    fn lower_is_better_metrics_use_the_lower_tail() {
        let mut observations: Vec<_> = (0..39)
            .map(|index| {
                observation(
                    index,
                    300_000.0 + (index % 4) as f64 * 1_000.0,
                    MetricKey::FastWin,
                    1.0,
                )
            })
            .collect();
        observations.push(observation(99, 30_000.0, MetricKey::FastWin, 1.0));
        let candidates = score_observations(&observations, test_time());
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].item.id, public_item_id("observation:99"));
        assert!(candidates[0].item.text.contains("30.0 seconds"));
    }

    #[test]
    fn non_finite_and_low_confidence_values_cannot_qualify() {
        let mut observations: Vec<_> = (0..39)
            .map(|index| {
                observation(
                    index,
                    0.4 + (index % 5) as f64 * 0.01,
                    MetricKey::PointsPerMinute,
                    1.0,
                )
            })
            .collect();
        observations.push(observation(98, f64::NAN, MetricKey::PointsPerMinute, 1.0));
        observations.push(observation(99, 0.99, MetricKey::PointsPerMinute, 0.1));
        assert!(score_observations(&observations, test_time()).is_empty());
    }

    #[test]
    fn future_dated_observations_cannot_qualify() {
        let mut observations: Vec<_> = (0..39)
            .map(|index| observation(index, 100.0, MetricKey::SoloScore, 1.0))
            .collect();
        let mut future = observation(99, 10_000.0, MetricKey::SoloScore, 1.0);
        future.occurred_at = test_time() + chrono::Duration::seconds(1);
        observations.push(future);

        assert!(score_observations(&observations, test_time()).is_empty());
    }

    fn high_score(user_id: i32, game_id: &str, score: i32) -> HighScoreEntry {
        HighScoreEntry {
            game_id: game_id.to_string(),
            user_id,
            username: format!("Player{user_id}"),
            score,
            region: "test".to_string(),
            game_type: "solo".to_string(),
            season: 0,
            timestamp: test_time(),
            news_eligible: true,
        }
    }

    #[test]
    fn public_ticker_sources_require_verified_news_provenance() {
        let public_score = high_score(1, "public", 100);
        let mut private_score = high_score(2, "private", 200);
        private_score.news_eligible = false;
        let scores = news_eligible_high_scores(vec![private_score, public_score]);
        assert_eq!(scores.len(), 1);
        assert_eq!(scores[0].game_id, "public");
    }

    #[test]
    fn solo_leader_uses_immutable_score_age_as_a_lower_bound() {
        let mut score = high_score(7, "record-game", 1_240);
        score.username = "Lopatron33".to_string();
        score.timestamp = test_time() - chrono::Duration::days(35);

        let leader = solo_leader_candidate(&score, 0, test_time()).unwrap();
        assert_eq!(leader.item.kind, NewsTickerKind::Ranking);
        assert_eq!(
            leader.item.text,
            "Lopatron33 has held Solo's top score for at least 35 days — 1,240 points!"
        );
        assert_eq!(
            leader.item.cta,
            Some(NewsTickerCta {
                label: "Take a run.".to_string(),
                action: NewsTickerCtaAction::PlaySolo,
            })
        );

        score.timestamp = test_time() + chrono::Duration::seconds(1);
        assert!(solo_leader_candidate(&score, 0, test_time()).is_none());
    }

    #[test]
    fn elapsed_copy_uses_human_sized_whole_units_and_rejects_the_future() {
        let now = test_time();
        assert_eq!(format_elapsed(now, now).as_deref(), Some("under a minute"));
        assert_eq!(
            format_elapsed(now - chrono::Duration::minutes(1), now).as_deref(),
            Some("1 minute")
        );
        assert_eq!(
            format_elapsed(now - chrono::Duration::hours(2), now).as_deref(),
            Some("2 hours")
        );
        assert_eq!(
            format_elapsed(now - chrono::Duration::days(35), now).as_deref(),
            Some("35 days")
        );
        assert_eq!(format_age_ago(now, now).as_deref(), Some("just now"));
        assert!(format_elapsed(now + chrono::Duration::seconds(1), now).is_none());
    }

    #[test]
    fn generated_performance_copy_contains_the_observed_truth() {
        let cases = [
            observation(1, 77.0, MetricKey::SoloScore, 1.0),
            observation(2, 12.5, MetricKey::PointsPerMinute, 1.0),
            observation(3, 9.5, MetricKey::ActionsPerMinute, 1.0),
            observation(4, 45_000.0, MetricKey::FastWin, 1.0),
            observation(5, 8.0, MetricKey::TeamWinningMargin, 1.0),
        ];
        for case in cases {
            let candidate = candidate_from_observation(&case, 1.0).unwrap();
            let lower = candidate.item.text.to_lowercase();
            assert!(candidate.item.text.contains(&case.subject_name));
            assert!(candidate.item.text.contains(case.cohort.mode.label()));
            if case.cohort.metric == MetricKey::SoloScore {
                assert!(!candidate.item.text.contains("Season"));
            }
            assert!(!lower.contains("world record"));
            assert!(!lower.contains("season record"));
            assert!(!lower.contains("#1"));
            assert!(!lower.contains("demo"));
            assert!(!lower.contains("streak"));
        }
    }

    #[test]
    fn quiet_result_copy_rejects_zero_and_non_winner_rate_stats() {
        let zero = observation(1, 0.0, MetricKey::SoloScore, 1.0);
        assert!(literal_result_candidate(&zero, test_time()).is_none());

        let rate = observation(2, 12.5, MetricKey::PointsPerMinute, 1.0);
        assert!(literal_result_candidate(&rate, test_time()).is_none());

        let actions = observation(3, 9.5, MetricKey::ActionsPerMinute, 1.0);
        assert!(literal_result_candidate(&actions, test_time()).is_none());
    }

    #[test]
    fn ranked_solo_rows_publish_an_exact_ranked_solo_cta() {
        let mut game = completed_solo_game(true, false);
        let mut state: GameState = serde_json::from_value(game.game_state.take().unwrap()).unwrap();
        state.queue_mode = QueueMode::Competitive;
        game.game_state = Some(serde_json::to_value(state).unwrap());

        let observations = observations_from_games(&[game], 0);
        assert!(!observations.is_empty());
        assert!(
            observations
                .iter()
                .all(|observation| observation.cohort.queue == QueueKey::Ranked)
        );
        let solo_score = observations
            .iter()
            .find(|observation| observation.cohort.metric == MetricKey::SoloScore)
            .unwrap();
        assert_eq!(
            literal_result_candidate(solo_score, test_time())
                .unwrap()
                .item
                .cta
                .unwrap()
                .action,
            NewsTickerCtaAction::PlayRankedSolo
        );
        assert_eq!(
            candidate_from_observation(solo_score, 1.0)
                .unwrap()
                .item
                .cta
                .unwrap()
                .action,
            NewsTickerCtaAction::PlayRankedSolo
        );
    }

    #[test]
    fn selection_deduplicates_and_diversifies() {
        let make_candidate = |id: &str,
                              dedupe: &str,
                              subject: &str,
                              metric: MetricKey,
                              mode: ModeKey,
                              score: f64| Candidate {
            item: NewsTickerItem {
                id: id.to_string(),
                kind: metric.kind(),
                text: id.to_string(),
                occurred_at: test_time().to_rfc3339(),
                cta: None,
            },
            dedupe_key: dedupe.to_string(),
            subject_key: subject.to_string(),
            metric,
            mode,
            exceptional_score: score,
        };
        let candidates = vec![
            make_candidate(
                "record",
                "same",
                "alice",
                MetricKey::SoloScore,
                ModeKey::Solo,
                2.0,
            ),
            make_candidate(
                "duplicate",
                "same",
                "alice",
                MetricKey::SoloScore,
                ModeKey::Solo,
                1.0,
            ),
            make_candidate(
                "alice-second",
                "other",
                "alice",
                MetricKey::PointsPerMinute,
                ModeKey::Duel,
                1.5,
            ),
            make_candidate(
                "bob",
                "bob",
                "bob",
                MetricKey::ActionsPerMinute,
                ModeKey::Duel,
                1.4,
            ),
        ];
        let selected = select_diverse_candidates(candidates, 3);
        assert_eq!(selected.len(), 3);
        assert_eq!(selected[0].item.id, "record");
        assert!(selected.iter().any(|candidate| candidate.item.id == "bob"));
        assert!(
            !selected
                .iter()
                .any(|candidate| candidate.item.id == "duplicate")
        );

        let existing = vec![
            make_candidate(
                "existing-1",
                "existing-1",
                "eve",
                MetricKey::SoloScore,
                ModeKey::Solo,
                2.0,
            ),
            make_candidate(
                "existing-2",
                "existing-2",
                "mallory",
                MetricKey::SoloScore,
                ModeKey::Solo,
                1.9,
            ),
        ];
        let additions = select_diverse_candidates_after(
            vec![make_candidate(
                "would-break-cap",
                "new",
                "trent",
                MetricKey::SoloScore,
                ModeKey::Solo,
                1.0,
            )],
            1,
            &existing,
        );
        assert!(additions.is_empty());
    }

    #[test]
    fn public_item_ids_are_stable_and_do_not_embed_internal_ids() {
        let internal = "game:424242:user:31337:solo-score";
        let public = public_item_id(internal);

        assert_eq!(public, public_item_id(internal));
        assert_ne!(public, public_item_id("game:424243:user:31337:solo-score"));
        assert!(public.starts_with("news-"));
        assert!(!public.contains("424242"));
        assert!(!public.contains("31337"));
        assert!(!public.contains("game"));
        assert!(!public.contains("user"));
    }

    #[test]
    fn system_copy_is_dynamic_and_contains_no_unimplemented_claims() {
        let items: Vec<_> = (0..8)
            .map(|variant| system_item(7, test_time(), variant))
            .collect();
        assert!(items.iter().all(|item| {
            [
                "Season 7 is on!",
                "Season 7: climb the ranks!",
                "Season 7: go big in Solo!",
                "Season 7: pick a fight!",
                "Season 7: bring the backup!",
                "Season 7: own the arena!",
                "Season 7: make your move!",
                "Season 7: hit Ranked Solo!",
            ]
            .contains(&item.text.as_str())
        }));
        assert!(items.iter().all(|item| item.cta.is_some()));
        assert!(
            items
                .iter()
                .all(|item| !item.text.to_lowercase().contains("skin"))
        );
        assert!(
            items
                .iter()
                .map(|item| item.text.as_str())
                .collect::<HashSet<_>>()
                .len()
                == 8
        );
        // System copy may use the configured season and imperative prompts,
        // but it must not invent availability or leaderboard state when all
        // database reads have failed.
        assert!(items.iter().all(|item| {
            let lower = item.text.to_lowercase();
            !lower.contains(" is live")
                && !lower.contains(" is open")
                && !lower.contains(" are live")
                && !lower.contains("standings")
                && !lower.contains("leader")
        }));

        let quiet_feed = system_items(7, test_time(), 3);
        assert_eq!(quiet_feed.len(), 3);
        assert_eq!(
            quiet_feed
                .iter()
                .map(|item| item.id.as_str())
                .collect::<HashSet<_>>()
                .len(),
            3
        );
    }
}
