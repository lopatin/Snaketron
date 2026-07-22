//! Load-test reporting primitives.
//!
//! The coordinator records one [`SessionRecord`] per virtual user and hands the
//! completed [`LoadTestRun`] to [`write_report`]. The writer produces:
//!
//! - `summary.json` with aggregate metrics and compact per-session summaries;
//! - `index.html`, a self-contained human-readable report; and
//! - one JSON file under `failures/` for every non-completed session.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Write as _};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const REPORT_SCHEMA_VERSION: u32 = 2;

/// Milliseconds since the Unix epoch, suitable for report timestamps.
pub fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

/// A lifecycle phase shared by progress events and failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionPhase {
    Created,
    GuestAuthentication,
    WebSocketConnect,
    WebSocketAuthentication,
    LobbyCreate,
    LobbyJoin,
    LobbyReady,
    Matchmaking,
    GameJoin,
    GameSnapshot,
    Playing,
    Cleanup,
    Complete,
}

impl SessionPhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::GuestAuthentication => "guest_authentication",
            Self::WebSocketConnect => "websocket_connect",
            Self::WebSocketAuthentication => "websocket_authentication",
            Self::LobbyCreate => "lobby_create",
            Self::LobbyJoin => "lobby_join",
            Self::LobbyReady => "lobby_ready",
            Self::Matchmaking => "matchmaking",
            Self::GameJoin => "game_join",
            Self::GameSnapshot => "game_snapshot",
            Self::Playing => "playing",
            Self::Cleanup => "cleanup",
            Self::Complete => "complete",
        }
    }
}

impl fmt::Display for SessionPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Terminal state of a virtual-user session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionOutcome {
    Completed,
    Failed,
    Cancelled,
    Incomplete,
}

impl SessionOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Incomplete => "incomplete",
        }
    }
}

impl fmt::Display for SessionOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// One timestamped event in a session's lifecycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionLifecycleRecord {
    pub phase: SessionPhase,
    pub at_unix_ms: u64,
    /// Duration of the operation that produced this event, when applicable.
    pub elapsed_ms: Option<u64>,
    pub message: Option<String>,
}

impl SessionLifecycleRecord {
    pub fn new(phase: SessionPhase, at_unix_ms: u64) -> Self {
        Self {
            phase,
            at_unix_ms,
            elapsed_ms: None,
            message: None,
        }
    }

    pub fn with_elapsed_ms(mut self, elapsed_ms: u64) -> Self {
        self.elapsed_ms = Some(elapsed_ms);
        self
    }

    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }
}

/// Structured information retained for a failed session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionFailureRecord {
    pub phase: SessionPhase,
    pub at_unix_ms: u64,
    pub message: String,
    pub retryable: bool,
    /// Small diagnostic values such as close code, last message type, or game ID.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub context: BTreeMap<String, String>,
}

impl SessionFailureRecord {
    pub fn new(phase: SessionPhase, at_unix_ms: u64, message: impl Into<String>) -> Self {
        Self {
            phase,
            at_unix_ms,
            message: message.into(),
            retryable: false,
            context: BTreeMap::new(),
        }
    }

    pub fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }

    pub fn with_context(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.context.insert(key.into(), value.into());
        self
    }
}

/// Timing and traffic measurements collected by one virtual user.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMetrics {
    pub guest_auth_ms: Option<u64>,
    pub websocket_connect_ms: Option<u64>,
    pub lobby_ready_ms: Option<u64>,
    pub matchmaking_wait_ms: Option<u64>,
    pub game_join_ms: Option<u64>,
    pub game_duration_ms: Option<u64>,
    #[serde(default)]
    pub websocket_rtt_ms: Vec<u64>,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub game_events_received: u64,
    pub commands_sent: u64,
    pub disconnects: u64,
    pub reconnects: u64,
}

/// What happened while the coordinator held one configured concurrency stage.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RampStageRecord {
    pub stage_index: u32,
    pub target_concurrency: usize,
    pub started_at_unix_ms: u64,
    pub finished_at_unix_ms: u64,
    pub sessions_launched: usize,
    pub active_sessions_at_start: usize,
    pub active_sessions_at_end: usize,
    /// Whether the coordinator ever observed the requested number of logical
    /// sessions whose initial WebSocket had sent an authentication token during
    /// this bounded ramp-and-hold window.
    pub target_reached: bool,
    pub target_reached_at_unix_ms: Option<u64>,
}

/// In-band infrastructure sample. Backend count is a load-balancer hint, not
/// a claim about the cloud provider's task count.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InfrastructureSample {
    pub observed_at_unix_ms: u64,
    pub regional_user_counts: BTreeMap<String, u32>,
    pub regional_server_counts: BTreeMap<String, u32>,
    pub observed_backend_hints: usize,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GameRunCounts {
    pub expected: usize,
    pub observed: usize,
    /// Games that reached an authoritative server completion event.
    pub completed: usize,
    /// Untimed games deliberately left after the configured active-play window.
    #[serde(default)]
    pub timeboxed: usize,
    pub pairing_violations: usize,
}

/// Full record for one virtual-user session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecord {
    pub session_id: String,
    pub username: String,
    pub wave_index: u32,
    /// Coordinator-defined grouping key for sessions intended to share a match.
    pub match_group: String,
    pub lobby_code: Option<String>,
    pub game_id: Option<u32>,
    pub started_at_unix_ms: u64,
    pub finished_at_unix_ms: Option<u64>,
    pub outcome: SessionOutcome,
    #[serde(default)]
    pub lifecycle: Vec<SessionLifecycleRecord>,
    pub failure: Option<SessionFailureRecord>,
    #[serde(default)]
    pub metrics: SessionMetrics,
    /// Additional session-level diagnostics retained in failure JSON.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub diagnostics: BTreeMap<String, String>,
}

impl SessionRecord {
    pub fn new(
        session_id: impl Into<String>,
        username: impl Into<String>,
        wave_index: u32,
        match_group: impl Into<String>,
        started_at_unix_ms: u64,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            username: username.into(),
            wave_index,
            match_group: match_group.into(),
            lobby_code: None,
            game_id: None,
            started_at_unix_ms,
            finished_at_unix_ms: None,
            outcome: SessionOutcome::Incomplete,
            lifecycle: vec![SessionLifecycleRecord::new(
                SessionPhase::Created,
                started_at_unix_ms,
            )],
            failure: None,
            metrics: SessionMetrics::default(),
            diagnostics: BTreeMap::new(),
        }
    }

    pub fn record_lifecycle(&mut self, record: SessionLifecycleRecord) {
        self.lifecycle.push(record);
    }

    pub fn complete(&mut self, finished_at_unix_ms: u64) {
        self.finished_at_unix_ms = Some(finished_at_unix_ms);
        self.outcome = SessionOutcome::Completed;
        self.lifecycle.push(SessionLifecycleRecord::new(
            SessionPhase::Complete,
            finished_at_unix_ms,
        ));
    }

    pub fn fail(&mut self, failure: SessionFailureRecord) {
        self.finished_at_unix_ms = Some(failure.at_unix_ms);
        self.outcome = SessionOutcome::Failed;
        self.lifecycle.push(
            SessionLifecycleRecord::new(failure.phase, failure.at_unix_ms)
                .with_message(failure.message.clone()),
        );
        self.failure = Some(failure);
    }

    pub fn cancel(&mut self, finished_at_unix_ms: u64, reason: impl Into<String>) {
        self.finished_at_unix_ms = Some(finished_at_unix_ms);
        self.outcome = SessionOutcome::Cancelled;
        self.lifecycle.push(
            SessionLifecycleRecord::new(SessionPhase::Cleanup, finished_at_unix_ms)
                .with_message(reason),
        );
    }

    pub fn duration_ms(&self) -> Option<u64> {
        self.finished_at_unix_ms
            .map(|finished| finished.saturating_sub(self.started_at_unix_ms))
    }

    pub fn is_failed(&self) -> bool {
        self.outcome == SessionOutcome::Failed || self.failure.is_some()
    }

    pub fn needs_detail_artifact(&self) -> bool {
        self.outcome != SessionOutcome::Completed || self.failure.is_some()
    }
}

/// Complete coordinator input used to build and write a report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadTestRun {
    pub run_id: String,
    pub target: String,
    pub started_at_unix_ms: u64,
    pub finished_at_unix_ms: u64,
    pub configured_max_concurrency: usize,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    #[serde(default)]
    pub ramp_stages: Vec<RampStageRecord>,
    #[serde(default)]
    pub infrastructure_samples: Vec<InfrastructureSample>,
    #[serde(default)]
    pub games: GameRunCounts,
    #[serde(default)]
    pub pairing_violation_details: Vec<String>,
    pub sessions: Vec<SessionRecord>,
}

impl LoadTestRun {
    pub fn new(
        run_id: impl Into<String>,
        target: impl Into<String>,
        started_at_unix_ms: u64,
        configured_max_concurrency: usize,
    ) -> Self {
        Self {
            run_id: run_id.into(),
            target: target.into(),
            started_at_unix_ms,
            finished_at_unix_ms: started_at_unix_ms,
            configured_max_concurrency,
            metadata: BTreeMap::new(),
            ramp_stages: Vec::new(),
            infrastructure_samples: Vec::new(),
            games: GameRunCounts::default(),
            pairing_violation_details: Vec::new(),
            sessions: Vec::new(),
        }
    }
}

/// A nearest-rank percentile. `percentile` is expressed from `0.0` to `100.0`.
/// Values outside that range are clamped; NaN and empty samples return `None`.
pub fn percentile(values: &[u64], percentile: f64) -> Option<u64> {
    if values.is_empty() || !percentile.is_finite() {
        return None;
    }

    let mut sorted = values.to_vec();
    sorted.sort_unstable();

    let percentile = percentile.clamp(0.0, 100.0);
    if percentile == 0.0 {
        return sorted.first().copied();
    }

    let rank = ((percentile / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted.get(rank.saturating_sub(1)).copied()
}

/// Summary of a collection of millisecond samples.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DistributionSummary {
    pub samples: usize,
    pub min_ms: Option<u64>,
    pub max_ms: Option<u64>,
    pub mean_ms: Option<f64>,
    pub p50_ms: Option<u64>,
    pub p95_ms: Option<u64>,
    pub p99_ms: Option<u64>,
}

impl DistributionSummary {
    pub fn from_samples(samples: &[u64]) -> Self {
        let mean_ms = if samples.is_empty() {
            None
        } else {
            let sum = samples.iter().map(|value| *value as u128).sum::<u128>();
            Some(sum as f64 / samples.len() as f64)
        };

        Self {
            samples: samples.len(),
            min_ms: samples.iter().min().copied(),
            max_ms: samples.iter().max().copied(),
            mean_ms,
            p50_ms: percentile(samples, 50.0),
            p95_ms: percentile(samples, 95.0),
            p99_ms: percentile(samples, 99.0),
        }
    }
}

impl Default for DistributionSummary {
    fn default() -> Self {
        Self::from_samples(&[])
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrafficTotals {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub game_events_received: u64,
    pub commands_sent: u64,
    pub disconnects: u64,
    pub reconnects: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AggregateMetrics {
    pub session_duration_ms: DistributionSummary,
    pub guest_auth_ms: DistributionSummary,
    pub websocket_connect_ms: DistributionSummary,
    pub lobby_ready_ms: DistributionSummary,
    pub matchmaking_wait_ms: DistributionSummary,
    pub game_join_ms: DistributionSummary,
    pub game_duration_ms: DistributionSummary,
    pub websocket_rtt_ms: DistributionSummary,
    pub traffic: TrafficTotals,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionCounts {
    pub total: usize,
    pub completed: usize,
    pub failed: usize,
    pub cancelled: usize,
    pub incomplete: usize,
    pub success_rate_percent: f64,
    pub failure_rate_percent: f64,
    /// Peak overlapping logical sessions after their authentication token was
    /// written to the initial WebSocket. A logical session remains active across
    /// short reconnect gaps; this does not claim server-side authentication was
    /// confirmed or that one transport socket stayed continuously open.
    pub peak_token_sent_concurrency: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummary {
    pub session_id: String,
    pub username: String,
    pub wave_index: u32,
    pub match_group: String,
    pub lobby_code: Option<String>,
    pub game_id: Option<u32>,
    pub outcome: SessionOutcome,
    /// `timeboxed` when a healthy untimed game reached its configured play window.
    pub completion_kind: Option<String>,
    pub duration_ms: Option<u64>,
    pub failure_phase: Option<SessionPhase>,
    pub failure_message: Option<String>,
    /// Relative link from `index.html` to the detailed non-completed session record.
    pub detail_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateReport {
    pub schema_version: u32,
    pub generated_at_unix_ms: u64,
    pub run_id: String,
    pub target: String,
    pub started_at_unix_ms: u64,
    pub finished_at_unix_ms: u64,
    pub duration_ms: u64,
    pub configured_max_concurrency: usize,
    pub metadata: BTreeMap<String, String>,
    pub ramp_stages: Vec<RampStageRecord>,
    pub infrastructure_samples: Vec<InfrastructureSample>,
    pub games: GameRunCounts,
    pub pairing_violation_details: Vec<String>,
    pub session_counts: SessionCounts,
    pub metrics: AggregateMetrics,
    pub failures_by_phase: BTreeMap<String, usize>,
    pub failures_by_message: BTreeMap<String, usize>,
    pub sessions: Vec<SessionSummary>,
}

impl AggregateReport {
    pub fn from_run(run: &LoadTestRun) -> Self {
        aggregate_report(run)
    }
}

/// Build the in-memory aggregate without writing any files.
pub fn aggregate_report(run: &LoadTestRun) -> AggregateReport {
    let mut completed = 0;
    let mut failed = 0;
    let mut cancelled = 0;
    let mut incomplete = 0;
    let mut failures_by_phase = BTreeMap::new();
    let mut failures_by_message = BTreeMap::new();
    let mut detail_ordinal = 0usize;

    let mut session_durations = Vec::new();
    let mut guest_auth = Vec::new();
    let mut websocket_connect = Vec::new();
    let mut lobby_ready = Vec::new();
    let mut matchmaking_wait = Vec::new();
    let mut game_join = Vec::new();
    let mut game_duration = Vec::new();
    let mut websocket_rtt = Vec::new();
    let mut traffic = TrafficTotals::default();

    let sessions = run
        .sessions
        .iter()
        .map(|session| {
            match session.outcome {
                SessionOutcome::Completed => completed += 1,
                SessionOutcome::Failed => failed += 1,
                SessionOutcome::Cancelled => cancelled += 1,
                SessionOutcome::Incomplete => incomplete += 1,
            }

            if let Some(value) = session.duration_ms() {
                session_durations.push(value);
            }
            push_option(&mut guest_auth, session.metrics.guest_auth_ms);
            push_option(&mut websocket_connect, session.metrics.websocket_connect_ms);
            push_option(&mut lobby_ready, session.metrics.lobby_ready_ms);
            push_option(&mut matchmaking_wait, session.metrics.matchmaking_wait_ms);
            push_option(&mut game_join, session.metrics.game_join_ms);
            push_option(&mut game_duration, session.metrics.game_duration_ms);
            websocket_rtt.extend(session.metrics.websocket_rtt_ms.iter().copied());

            traffic.messages_sent = traffic
                .messages_sent
                .saturating_add(session.metrics.messages_sent);
            traffic.messages_received = traffic
                .messages_received
                .saturating_add(session.metrics.messages_received);
            traffic.game_events_received = traffic
                .game_events_received
                .saturating_add(session.metrics.game_events_received);
            traffic.commands_sent = traffic
                .commands_sent
                .saturating_add(session.metrics.commands_sent);
            traffic.disconnects = traffic
                .disconnects
                .saturating_add(session.metrics.disconnects);
            traffic.reconnects = traffic
                .reconnects
                .saturating_add(session.metrics.reconnects);

            let detail_file = if session.needs_detail_artifact() {
                detail_ordinal += 1;
                Some(detail_relative_path(detail_ordinal, &session.session_id))
            } else {
                None
            };

            if let Some(failure) = &session.failure {
                *failures_by_phase
                    .entry(failure.phase.as_str().to_owned())
                    .or_insert(0) += 1;
                *failures_by_message
                    .entry(failure.message.clone())
                    .or_insert(0) += 1;
            }

            SessionSummary {
                session_id: session.session_id.clone(),
                username: session.username.clone(),
                wave_index: session.wave_index,
                match_group: session.match_group.clone(),
                lobby_code: session.lobby_code.clone(),
                game_id: session.game_id,
                outcome: session.outcome,
                completion_kind: session.diagnostics.get("completion_kind").cloned(),
                duration_ms: session.duration_ms(),
                failure_phase: session.failure.as_ref().map(|value| value.phase),
                failure_message: session.failure.as_ref().map(|value| value.message.clone()),
                detail_file,
            }
        })
        .collect();

    let total = run.sessions.len();
    let denominator = total.max(1) as f64;

    AggregateReport {
        schema_version: REPORT_SCHEMA_VERSION,
        generated_at_unix_ms: unix_time_ms(),
        run_id: run.run_id.clone(),
        target: run.target.clone(),
        started_at_unix_ms: run.started_at_unix_ms,
        finished_at_unix_ms: run.finished_at_unix_ms,
        duration_ms: run
            .finished_at_unix_ms
            .saturating_sub(run.started_at_unix_ms),
        configured_max_concurrency: run.configured_max_concurrency,
        metadata: run.metadata.clone(),
        ramp_stages: run.ramp_stages.clone(),
        infrastructure_samples: run.infrastructure_samples.clone(),
        games: run.games.clone(),
        pairing_violation_details: run.pairing_violation_details.clone(),
        session_counts: SessionCounts {
            total,
            completed,
            failed,
            cancelled,
            incomplete,
            success_rate_percent: if total == 0 {
                0.0
            } else {
                completed as f64 * 100.0 / denominator
            },
            failure_rate_percent: if total == 0 {
                0.0
            } else {
                failed as f64 * 100.0 / denominator
            },
            peak_token_sent_concurrency: peak_token_sent_concurrency(run),
        },
        metrics: AggregateMetrics {
            session_duration_ms: DistributionSummary::from_samples(&session_durations),
            guest_auth_ms: DistributionSummary::from_samples(&guest_auth),
            websocket_connect_ms: DistributionSummary::from_samples(&websocket_connect),
            lobby_ready_ms: DistributionSummary::from_samples(&lobby_ready),
            matchmaking_wait_ms: DistributionSummary::from_samples(&matchmaking_wait),
            game_join_ms: DistributionSummary::from_samples(&game_join),
            game_duration_ms: DistributionSummary::from_samples(&game_duration),
            websocket_rtt_ms: DistributionSummary::from_samples(&websocket_rtt),
            traffic,
        },
        failures_by_phase,
        failures_by_message,
        sessions,
    }
}

fn push_option(values: &mut Vec<u64>, value: Option<u64>) {
    if let Some(value) = value {
        values.push(value);
    }
}

fn peak_token_sent_concurrency(run: &LoadTestRun) -> usize {
    // Token-sent logical sessions are half-open intervals [token, finish).
    // Finish events sort before starts at the same timestamp, avoiding a false
    // overlap. Sessions that never wrote a token do not inflate this metric.
    let mut events = Vec::with_capacity(run.sessions.len() * 2);
    for session in &run.sessions {
        let Some(connected_at) = session
            .lifecycle
            .iter()
            .find(|event| event.phase == SessionPhase::WebSocketAuthentication)
            .map(|event| event.at_unix_ms)
        else {
            continue;
        };
        let finish = session
            .finished_at_unix_ms
            .unwrap_or(run.finished_at_unix_ms);
        if finish <= connected_at {
            // Preserve visibility for zero-duration records.
            events.push((connected_at, 1i8));
            events.push((connected_at.saturating_add(1), -1i8));
        } else {
            events.push((connected_at, 1i8));
            events.push((finish, -1i8));
        }
    }
    events.sort_unstable_by_key(|(at, delta)| (*at, *delta));

    let mut current = 0usize;
    let mut peak = 0usize;
    for (_, delta) in events {
        if delta < 0 {
            current = current.saturating_sub(1);
        } else {
            current = current.saturating_add(1);
            peak = peak.max(current);
        }
    }
    peak
}

/// Files written by [`write_report`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrittenReport {
    pub summary_json: PathBuf,
    pub index_html: PathBuf,
    pub session_detail_json: Vec<PathBuf>,
}

#[derive(Debug)]
pub enum ReportError {
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl fmt::Display for ReportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "report I/O error: {error}"),
            Self::Json(error) => write!(formatter, "report JSON error: {error}"),
        }
    }
}

impl Error for ReportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Json(error) => Some(error),
        }
    }
}

impl From<std::io::Error> for ReportError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<serde_json::Error> for ReportError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

/// Write aggregate and per-non-completed-session artifacts beneath `output_dir`.
pub fn write_report(
    output_dir: impl AsRef<Path>,
    run: &LoadTestRun,
) -> Result<WrittenReport, ReportError> {
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir)?;
    let failures_dir = output_dir.join("failures");
    clear_existing_failure_artifacts(&failures_dir)?;

    let report = aggregate_report(run);
    let summary_json = output_dir.join("summary.json");
    fs::write(&summary_json, serde_json::to_vec_pretty(&report)?)?;

    let mut session_detail_json = Vec::new();
    let mut detail_ordinal = 0usize;
    for session in &run.sessions {
        if !session.needs_detail_artifact() {
            continue;
        }

        detail_ordinal += 1;
        fs::create_dir_all(&failures_dir)?;
        let path = output_dir.join(detail_relative_path(detail_ordinal, &session.session_id));
        fs::write(&path, serde_json::to_vec_pretty(session)?)?;
        session_detail_json.push(path);
    }

    let index_html = output_dir.join("index.html");
    fs::write(&index_html, render_html(&report))?;

    Ok(WrittenReport {
        summary_json,
        index_html,
        session_detail_json,
    })
}

fn clear_existing_failure_artifacts(failures_dir: &Path) -> Result<(), std::io::Error> {
    let metadata = match fs::symlink_metadata(failures_dir) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if metadata.file_type().is_dir() {
        fs::remove_dir_all(failures_dir)
    } else {
        // `symlink_metadata` does not follow a symlink, so this only removes the
        // exact run-scoped path rather than anything it may point at.
        fs::remove_file(failures_dir)
    }
}

fn detail_relative_path(ordinal: usize, session_id: &str) -> String {
    format!(
        "failures/{ordinal:05}-{}.json",
        safe_filename_component(session_id)
    )
}

fn safe_filename_component(value: &str) -> String {
    let sanitized: String = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .take(80)
        .collect();

    if sanitized.is_empty() {
        "session".to_owned()
    } else {
        sanitized
    }
}

fn html_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(character),
        }
    }
    escaped
}

fn render_html(report: &AggregateReport) -> String {
    let counts = &report.session_counts;
    let mut html = String::with_capacity(24_000);
    html.push_str(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">",
    );
    let _ = write!(
        html,
        "<title>Snaketron load test {}</title>",
        html_escape(&report.run_id)
    );
    html.push_str(
        "<style>\
         :root{color-scheme:light;font-family:Inter,ui-sans-serif,system-ui,sans-serif;\
         background:#f4f5f7;color:#15171a}body{margin:0;padding:32px}main{max-width:1280px;margin:auto}\
         h1{margin:0 0 6px;font-size:30px}h2{margin-top:32px}.sub{color:#62666d;margin:0 0 24px}\
         .cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px}\
         .card,section{background:#fff;border:1px solid #dedfe2;border-radius:10px;padding:18px}\
         .card b{display:block;font-size:28px;margin-top:6px}.label{font-size:12px;text-transform:uppercase;\
         letter-spacing:.08em;color:#62666d}table{width:100%;border-collapse:collapse;background:#fff}\
         th,td{text-align:left;border-bottom:1px solid #e6e7e9;padding:10px 12px;font-size:13px}\
         th{background:#f8f8f9}.scroll{overflow:auto;border:1px solid #dedfe2;border-radius:10px}\
         .completed{color:#087a44}.failed{color:#b42318}.cancelled,.incomplete{color:#8a5200}\
         a{color:#155eef}code{font-size:12px}@media(max-width:700px){body{padding:18px}th,td{white-space:nowrap}}\
         </style></head><body><main>",
    );

    let _ = write!(
        html,
        "<h1>Snaketron load test</h1><p class=\"sub\">Run <code>{}</code> against {} · \
         <a href=\"summary.json\">summary.json</a></p>",
        html_escape(&report.run_id),
        html_escape(&report.target),
    );
    let _ = write!(
        html,
        "<div class=\"cards\">\
         <div class=\"card\"><span class=\"label\">Sessions</span><b>{}</b></div>\
         <div class=\"card\"><span class=\"label\">Completed</span><b class=\"completed\">{}</b></div>\
         <div class=\"card\"><span class=\"label\">Failed</span><b class=\"failed\">{}</b></div>\
         <div class=\"card\"><span class=\"label\">Success rate</span><b>{:.1}%</b></div>\
         <div class=\"card\"><span class=\"label\">Peak token-sent sessions</span><b>{}</b></div>\
         <div class=\"card\"><span class=\"label\">Configured max</span><b>{}</b></div>\
         <div class=\"card\"><span class=\"label\">Games observed</span><b>{}/{}</b></div>\
         <div class=\"card\"><span class=\"label\">Authoritative games</span><b>{}</b></div>\
         <div class=\"card\"><span class=\"label\">Timeboxed games</span><b>{}</b></div>\
         <div class=\"card\"><span class=\"label\">Pairing violations</span><b class=\"{}\">{}</b></div>\
         </div>",
        counts.total,
        counts.completed,
        counts.failed,
        counts.success_rate_percent,
        counts.peak_token_sent_concurrency,
        report.configured_max_concurrency,
        report.games.observed,
        report.games.expected,
        report.games.completed,
        report.games.timeboxed,
        if report.games.pairing_violations == 0 {
            "completed"
        } else {
            "failed"
        },
        report.games.pairing_violations,
    );
    if let Some(threshold) = report.metadata.get("threshold_result") {
        let autoscaling = report
            .metadata
            .get("autoscaling_signal")
            .map(String::as_str)
            .unwrap_or("not_recorded");
        let _ = write!(
            html,
            "<p class=\"sub\">Threshold result: <code>{}</code> · Autoscaling: <code>{}</code></p>",
            html_escape(threshold),
            html_escape(autoscaling),
        );
        if let Some(failures) = report.metadata.get("threshold_failures") {
            let _ = write!(
                html,
                "<p class=\"failed\">Threshold failures: {}</p>",
                html_escape(failures),
            );
        }
    }

    html.push_str(
        "<h2>Latency and duration</h2><div class=\"scroll\"><table><thead><tr>\
         <th>Metric</th><th>Samples</th><th>Mean</th><th>P50</th><th>P95</th><th>P99</th><th>Max</th>\
         </tr></thead><tbody>",
    );
    append_distribution_row(
        &mut html,
        "Session duration",
        &report.metrics.session_duration_ms,
    );
    append_distribution_row(
        &mut html,
        "Guest authentication",
        &report.metrics.guest_auth_ms,
    );
    append_distribution_row(
        &mut html,
        "WebSocket connect",
        &report.metrics.websocket_connect_ms,
    );
    append_distribution_row(&mut html, "Lobby ready", &report.metrics.lobby_ready_ms);
    append_distribution_row(
        &mut html,
        "Matchmaking wait",
        &report.metrics.matchmaking_wait_ms,
    );
    append_distribution_row(&mut html, "Game join", &report.metrics.game_join_ms);
    append_distribution_row(&mut html, "Game duration", &report.metrics.game_duration_ms);
    append_distribution_row(&mut html, "WebSocket RTT", &report.metrics.websocket_rtt_ms);
    html.push_str("</tbody></table></div>");

    let traffic = &report.metrics.traffic;
    let _ = write!(
        html,
        "<h2>Traffic</h2><div class=\"cards\">\
         <div class=\"card\"><span class=\"label\">Messages sent</span><b>{}</b></div>\
         <div class=\"card\"><span class=\"label\">Messages received</span><b>{}</b></div>\
         <div class=\"card\"><span class=\"label\">Game events</span><b>{}</b></div>\
         <div class=\"card\"><span class=\"label\">Commands sent</span><b>{}</b></div>\
         <div class=\"card\"><span class=\"label\">Disconnects</span><b>{}</b></div>\
         <div class=\"card\"><span class=\"label\">Reconnects</span><b>{}</b></div></div>",
        traffic.messages_sent,
        traffic.messages_received,
        traffic.game_events_received,
        traffic.commands_sent,
        traffic.disconnects,
        traffic.reconnects,
    );

    if !report.ramp_stages.is_empty() {
        html.push_str(
            "<h2>Ramp stages</h2><div class=\"scroll\"><table><thead><tr>\
             <th>Stage</th><th>Token-sent target</th><th>Reached</th><th>Launched</th><th>Token-sent start</th><th>Token-sent end</th>\
             <th>Duration</th></tr></thead><tbody>",
        );
        for stage in &report.ramp_stages {
            let _ = write!(
                html,
                "<tr><td>{}</td><td>{}</td><td class=\"{}\">{}</td><td>{}</td><td>{}</td><td>{}</td><td>{} ms</td></tr>",
                stage.stage_index + 1,
                stage.target_concurrency,
                if stage.target_reached {
                    "completed"
                } else {
                    "failed"
                },
                if stage.target_reached { "yes" } else { "no" },
                stage.sessions_launched,
                stage.active_sessions_at_start,
                stage.active_sessions_at_end,
                stage
                    .finished_at_unix_ms
                    .saturating_sub(stage.started_at_unix_ms),
            );
        }
        html.push_str("</tbody></table></div>");
    }

    if !report.infrastructure_samples.is_empty() {
        let selected_region = report.metadata.get("region");
        let peak_users = selected_region
            .and_then(|region| {
                report
                    .infrastructure_samples
                    .iter()
                    .filter_map(|sample| sample.regional_user_counts.get(region).copied())
                    .max()
            })
            .unwrap_or(0);
        let baseline_servers = selected_region
            .and_then(|region| {
                report
                    .infrastructure_samples
                    .first()
                    .and_then(|sample| sample.regional_server_counts.get(region))
                    .copied()
            })
            .unwrap_or(0);
        let peak_servers = selected_region
            .and_then(|region| {
                report
                    .infrastructure_samples
                    .iter()
                    .filter_map(|sample| sample.regional_server_counts.get(region).copied())
                    .max()
            })
            .unwrap_or(0);
        let peak_backends = report
            .infrastructure_samples
            .iter()
            .map(|sample| sample.observed_backend_hints)
            .max()
            .unwrap_or(0);
        let sample_errors = report
            .infrastructure_samples
            .iter()
            .filter(|sample| sample.error.is_some())
            .count();
        let _ = write!(
            html,
            "<h2>Infrastructure observations</h2><div class=\"cards\">\
             <div class=\"card\"><span class=\"label\">Peak selected-region users</span><b>{peak_users}</b></div>\
             <div class=\"card\"><span class=\"label\">Active servers baseline</span><b>{baseline_servers}</b></div>\
             <div class=\"card\"><span class=\"label\">Active servers peak</span><b>{peak_servers}</b></div>\
             <div class=\"card\"><span class=\"label\">Backend hints seen</span><b>{peak_backends}</b></div>\
             <div class=\"card\"><span class=\"label\">Observer errors</span><b>{sample_errors}</b></div></div>\
             <p class=\"sub\">Active servers come from TTL-backed service registrations; backend hints are only a secondary routing signal.</p>",
        );
    }

    html.push_str(
        "<h2>Sessions</h2><div class=\"scroll\"><table><thead><tr>\
         <th>Session</th><th>User</th><th>Wave</th><th>Match group</th><th>Lobby</th>\
         <th>Game</th><th>Outcome</th><th>Completion</th><th>Duration</th><th>Details</th></tr></thead><tbody>",
    );
    for session in &report.sessions {
        let detail_cell = match (&session.detail_file, &session.failure_message) {
            (Some(path), Some(message)) => format!(
                "<a href=\"{}\">{}</a>",
                html_escape(path),
                html_escape(message),
            ),
            (Some(path), None) => format!("<a href=\"{}\">details</a>", html_escape(path),),
            _ => "&mdash;".to_owned(),
        };
        let _ = write!(
            html,
            "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td>\
             <td>{}</td><td class=\"{}\">{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            html_escape(&session.session_id),
            html_escape(&session.username),
            session.wave_index,
            html_escape(&session.match_group),
            session
                .lobby_code
                .as_deref()
                .map(html_escape)
                .unwrap_or_else(|| "&mdash;".to_owned()),
            session
                .game_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "&mdash;".to_owned()),
            session.outcome.as_str(),
            session.outcome,
            session
                .completion_kind
                .as_deref()
                .map(html_escape)
                .unwrap_or_else(|| {
                    if session.outcome == SessionOutcome::Completed {
                        "authoritative".to_owned()
                    } else {
                        "&mdash;".to_owned()
                    }
                }),
            format_optional_ms(session.duration_ms),
            detail_cell,
        );
    }
    html.push_str("</tbody></table></div></main></body></html>");
    html
}

fn append_distribution_row(html: &mut String, label: &str, summary: &DistributionSummary) {
    let mean = summary
        .mean_ms
        .map(|value| format!("{value:.1} ms"))
        .unwrap_or_else(|| "&mdash;".to_owned());
    let _ = write!(
        html,
        "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
        html_escape(label),
        summary.samples,
        mean,
        format_optional_ms(summary.p50_ms),
        format_optional_ms(summary.p95_ms),
        format_optional_ms(summary.p99_ms),
        format_optional_ms(summary.max_ms),
    );
}

fn format_optional_ms(value: Option<u64>) -> String {
    value
        .map(|value| format!("{value} ms"))
        .unwrap_or_else(|| "&mdash;".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_uses_nearest_rank_and_handles_edges() {
        let descending: Vec<u64> = (1..=100).rev().collect();
        assert_eq!(percentile(&descending, 0.0), Some(1));
        assert_eq!(percentile(&descending, 50.0), Some(50));
        assert_eq!(percentile(&descending, 95.0), Some(95));
        assert_eq!(percentile(&descending, 99.0), Some(99));
        assert_eq!(percentile(&descending, 100.0), Some(100));
        assert_eq!(percentile(&descending, -10.0), Some(1));
        assert_eq!(percentile(&descending, 110.0), Some(100));
        assert_eq!(percentile(&[], 95.0), None);
        assert_eq!(percentile(&descending, f64::NAN), None);
    }

    #[test]
    fn aggregation_counts_outcomes_percentiles_traffic_and_concurrency() {
        let mut first = SessionRecord::new("s1", "user1", 0, "m1", 0);
        first.metrics.guest_auth_ms = Some(10);
        first.metrics.matchmaking_wait_ms = Some(20);
        first.metrics.websocket_rtt_ms = vec![5, 15];
        first.metrics.messages_sent = 7;
        first.record_lifecycle(SessionLifecycleRecord::new(
            SessionPhase::WebSocketAuthentication,
            1,
        ));
        first
            .diagnostics
            .insert("completion_kind".to_owned(), "timeboxed".to_owned());
        first.complete(100);

        let mut second = SessionRecord::new("s2", "user2", 0, "m1", 50);
        second.metrics.guest_auth_ms = Some(20);
        second.metrics.matchmaking_wait_ms = Some(100);
        second.metrics.websocket_rtt_ms = vec![25];
        second.metrics.messages_sent = 11;
        second.record_lifecycle(SessionLifecycleRecord::new(
            SessionPhase::WebSocketAuthentication,
            51,
        ));
        second.fail(SessionFailureRecord::new(
            SessionPhase::Matchmaking,
            250,
            "match timed out",
        ));

        let mut third = SessionRecord::new("s3", "user3", 1, "m2", 200);
        third.metrics.guest_auth_ms = Some(30);
        third.metrics.matchmaking_wait_ms = Some(60);
        third.metrics.messages_sent = 13;
        third.record_lifecycle(SessionLifecycleRecord::new(
            SessionPhase::WebSocketAuthentication,
            201,
        ));
        third.complete(400);

        let run = LoadTestRun {
            run_id: "run-1".to_owned(),
            target: "wss://example.test/ws".to_owned(),
            started_at_unix_ms: 0,
            finished_at_unix_ms: 400,
            configured_max_concurrency: 4,
            metadata: BTreeMap::new(),
            ramp_stages: Vec::new(),
            infrastructure_samples: Vec::new(),
            games: GameRunCounts {
                expected: 2,
                observed: 2,
                completed: 1,
                timeboxed: 1,
                pairing_violations: 0,
            },
            pairing_violation_details: Vec::new(),
            sessions: vec![first, second, third],
        };

        let report = aggregate_report(&run);
        assert_eq!(report.session_counts.total, 3);
        assert_eq!(report.session_counts.completed, 2);
        assert_eq!(report.session_counts.failed, 1);
        assert_eq!(report.session_counts.peak_token_sent_concurrency, 2);
        assert!((report.session_counts.success_rate_percent - 66.666_666).abs() < 0.001);
        assert_eq!(report.metrics.guest_auth_ms.p50_ms, Some(20));
        assert_eq!(report.metrics.guest_auth_ms.p95_ms, Some(30));
        assert_eq!(report.metrics.matchmaking_wait_ms.p50_ms, Some(60));
        assert_eq!(report.metrics.websocket_rtt_ms.p95_ms, Some(25));
        assert_eq!(report.metrics.traffic.messages_sent, 31);
        assert_eq!(report.games.completed, 1);
        assert_eq!(report.games.timeboxed, 1);
        assert_eq!(
            report.sessions[0].completion_kind.as_deref(),
            Some("timeboxed")
        );
        assert_eq!(
            report.failures_by_phase.get("matchmaking").copied(),
            Some(1)
        );
        assert_eq!(
            report.failures_by_message.get("match timed out").copied(),
            Some(1)
        );
        assert_eq!(
            report.sessions[1].detail_file.as_deref(),
            Some("failures/00001-s2.json")
        );
    }

    #[test]
    fn html_escape_covers_text_and_attribute_metacharacters() {
        assert_eq!(html_escape("<&>\"' safe"), "&lt;&amp;&gt;&quot;&#39; safe");

        let mut session = SessionRecord::new(
            "<session>",
            "<script>alert(1)</script>",
            0,
            "match & one",
            0,
        );
        session.fail(SessionFailureRecord::new(
            SessionPhase::Playing,
            10,
            "bad <message> & \"quote\"",
        ));
        let report = aggregate_report(&LoadTestRun {
            run_id: "<run>".to_owned(),
            target: "https://example.test/?a=1&b=2".to_owned(),
            started_at_unix_ms: 0,
            finished_at_unix_ms: 10,
            configured_max_concurrency: 1,
            metadata: BTreeMap::new(),
            ramp_stages: Vec::new(),
            infrastructure_samples: Vec::new(),
            games: GameRunCounts::default(),
            pairing_violation_details: Vec::new(),
            sessions: vec![session],
        });
        let rendered = render_html(&report);
        assert!(!rendered.contains("<script>alert(1)</script>"));
        assert!(rendered.contains("&lt;script&gt;alert(1)&lt;/script&gt;"));
        assert!(rendered.contains("bad &lt;message&gt; &amp; &quot;quote&quot;"));
        assert!(rendered.contains("Authoritative games"));
        assert!(rendered.contains("Timeboxed games"));
    }

    #[test]
    fn writer_replaces_run_scoped_and_links_noncompleted_artifacts() {
        let mut failed = SessionRecord::new("session-1", "user1", 0, "group-1", 1);
        failed.fail(SessionFailureRecord::new(
            SessionPhase::GameJoin,
            2,
            "snapshot unavailable",
        ));
        let mut run = LoadTestRun::new("artifact-test", "http://example.test", 1, 1);
        run.finished_at_unix_ms = 2;
        run.sessions.push(failed);

        let output = std::env::temp_dir().join(format!(
            "snaketron-loadtest-report-{}-{}",
            std::process::id(),
            unix_time_ms()
        ));
        let written = write_report(&output, &run).unwrap();

        assert!(written.summary_json.is_file());
        assert!(written.index_html.is_file());
        assert_eq!(written.session_detail_json.len(), 1);
        assert!(written.session_detail_json[0].is_file());
        let html = fs::read_to_string(&written.index_html).unwrap();
        assert!(html.contains("failures/00001-session-1.json"));

        let first_failure_path = written.session_detail_json[0].clone();
        let stale_path = output.join("failures/stale.json");
        fs::write(&stale_path, b"stale").unwrap();
        let mut cancelled = SessionRecord::new("session-2", "user2", 1, "group-2", 3);
        cancelled.cancel(4, "drain timeout");
        let incomplete = SessionRecord::new("session-3", "user3", 1, "group-2", 3);
        let mut replacement_run = LoadTestRun::new("artifact-test", "http://example.test", 3, 2);
        replacement_run.finished_at_unix_ms = 4;
        replacement_run.sessions = vec![cancelled, incomplete];

        let rewritten = write_report(&output, &replacement_run).unwrap();
        assert_eq!(rewritten.session_detail_json.len(), 2);
        assert!(!first_failure_path.exists());
        assert!(!stale_path.exists());
        assert!(
            rewritten
                .session_detail_json
                .iter()
                .all(|path| path.is_file())
        );
        let html = fs::read_to_string(&rewritten.index_html).unwrap();
        assert!(html.contains("failures/00001-session-2.json"));
        assert!(html.contains("failures/00002-session-3.json"));

        fs::remove_dir_all(output).unwrap();
    }
}
