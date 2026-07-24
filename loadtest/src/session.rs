//! One coordinated match group and its virtual-user sessions.

use crate::config::{CommandProfile, Population};
use crate::report::{
    HardRecoveryObservation, SessionFailureRecord, SessionLifecycleRecord, SessionMetrics,
    SessionOutcome, SessionPhase, SessionRecord, unix_time_ms,
};
use crate::target::BackendHintRegistry;
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use common::{
    ClientCommandIdentityV2, Direction, GameCommand, GameCommandMessage, GameEngine, GameEvent,
    GameEventMessage, GameState, GameStatus, GameType, QueueMode, calculate_ai_move,
};
use futures_util::{SinkExt, StreamExt, future::join_all};
use reqwest::{Client, Url};
use serde::Deserialize;
use server::lobby_manager::LobbyPreferences;
use server::recovery::CommandOutcome;
use server::ws_server::WSMessage;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::{Barrier, mpsc, watch};
use tokio::time::{Interval, MissedTickBehavior, interval, interval_at};
use tokio_tungstenite::tungstenite::Error as WebSocketError;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{COOKIE, ORIGIN};
use tokio_tungstenite::tungstenite::http::{HeaderMap, Request, StatusCode};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tokio_util::sync::CancellationToken;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

const PING_INTERVAL: Duration = Duration::from_secs(5);
const RECONNECT_DELAY: Duration = Duration::from_secs(2);
const MAX_RECONNECTS: u32 = 2;
const RECENT_EVENT_LIMIT: usize = 32;
const GAME_TIMEOUT_MARGIN: Duration = Duration::from_secs(45);
const PLANNED_HANDOFF_RETRY_DELAY: Duration = Duration::from_millis(100);
const ADMISSION_RETRY_DELAY: Duration = Duration::from_secs(1);
const POPULATION_CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);
const REQUIRED_SERVER_CAPABILITIES: [&str; 6] = [
    "explicit-auth-v1",
    "planned-drain-v1",
    "socket-generation-v1",
    "command-delivery-v2",
    "command-outcomes-v1",
    "command-outcome-barrier-v1",
];

fn validate_required_server_capabilities(capabilities: &BTreeSet<String>) -> Result<()> {
    let missing = REQUIRED_SERVER_CAPABILITIES
        .iter()
        .copied()
        .filter(|capability| !capabilities.contains(*capability))
        .collect::<Vec<_>>();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "server is missing required WebSocket capabilities: {}",
            missing.join(",")
        ))
    }
}

#[derive(Debug, Clone)]
pub struct SessionSettings {
    pub api_origin: Url,
    pub websocket_url: Url,
    pub origin: String,
    pub game_type: GameType,
    pub queue_mode: QueueMode,
    pub selected_mode: String,
    pub competitive: bool,
    pub population: Population,
    pub connect_timeout: Duration,
    pub lobby_timeout: Duration,
    pub queue_timeout: Duration,
    pub untimed_play_duration: Duration,
    pub command_profile: CommandProfile,
    pub backend_hints: BackendHintRegistry,
}

#[derive(Debug, Clone)]
pub struct MatchGroupSpec {
    pub run_id: String,
    pub wave_index: u32,
    pub group_index: u64,
    pub session_indices: Vec<u64>,
}

impl MatchGroupSpec {
    pub fn group_id(&self) -> String {
        format!("wave-{:04}-game-{:06}", self.wave_index, self.group_index)
    }
}

#[derive(Debug)]
pub struct MatchGroupResult {
    pub sessions: Vec<SessionRecord>,
    pub expected_game_count: usize,
    pub observed_game_ids: BTreeSet<u32>,
    pub pairing_violation: Option<String>,
}

/// Per-session activity emitted before the enclosing match-group task ends.
/// The coordinator uses this to maintain server-authenticated session
/// concurrency, including across short reconnect gaps within one lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionActivityEvent {
    Connected { session_index: u64 },
    Terminal { session_index: u64 },
}

struct SessionActivityLease {
    session_index: u64,
    sender: mpsc::UnboundedSender<SessionActivityEvent>,
    connected: bool,
}

impl SessionActivityLease {
    fn new(session_index: u64, sender: mpsc::UnboundedSender<SessionActivityEvent>) -> Self {
        Self {
            session_index,
            sender,
            connected: false,
        }
    }

    fn mark_connected(&mut self) {
        if self.connected {
            return;
        }
        self.connected = true;
        let _ = self.sender.send(SessionActivityEvent::Connected {
            session_index: self.session_index,
        });
    }
}

impl Drop for SessionActivityLease {
    fn drop(&mut self) {
        let _ = self.sender.send(SessionActivityEvent::Terminal {
            session_index: self.session_index,
        });
    }
}

#[derive(Debug, Deserialize)]
struct GuestResponse {
    token: String,
    user: GuestUser,
}

#[derive(Debug, Deserialize)]
struct GuestUser {
    id: i32,
    username: String,
}

struct PendingCommand {
    message: GameCommandMessage,
    sent_at_unix_ms: u64,
    sent_at: Instant,
}

struct LiveSession {
    record: SessionRecord,
    user_id: u32,
    token: String,
    socket: Socket,
    websocket_url: Url,
    origin: String,
    backend_hints: BackendHintRegistry,
    // Kept only in memory so reconnect handshakes behave like a browser cookie
    // jar. The transport value must never enter diagnostics or reports.
    sticky_cookie: Option<String>,
    last_lobby_members: BTreeSet<u32>,
    last_lobby_state: Option<String>,
    recent_events: VecDeque<String>,
    clock_offset_ms: i64,
    last_ping_client_time: Option<i64>,
    current_task_boot_id: Option<String>,
    current_socket_generation: Option<u64>,
    reconnects: u32,
    client_game_session_id: String,
    next_command_sequence: u64,
    pending_commands: BTreeMap<u64, PendingCommand>,
    server_capabilities: BTreeSet<String>,
    activity_lease: SessionActivityLease,
}

struct PlayedSession {
    record: SessionRecord,
    snapshot_user_ids: BTreeSet<u32>,
}

struct GameRuntime {
    engine: GameEngine,
    snake_id: u32,
    snapshot_user_ids: BTreeSet<u32>,
    ai_interval: Interval,
    last_decision_tick: Option<u32>,
    pending_direction: Option<Direction>,
    outstanding_ping: Option<(i64, Instant)>,
    promotion_suppression_floor: Option<u64>,
}

#[derive(Default)]
struct PlayingWarmupState {
    waiting_for_snapshot: bool,
    waiting_for_outcome_barrier: bool,
    join_retry_at: Option<tokio::time::Instant>,
}

impl PlayingWarmupState {
    fn observe_warming(
        &mut self,
        now: tokio::time::Instant,
        deadline: tokio::time::Instant,
        retry_after_ms: u64,
    ) {
        self.waiting_for_snapshot = true;
        self.waiting_for_outcome_barrier = true;
        self.join_retry_at = bounded_game_join_retry_at(now, deadline, retry_after_ms);
    }

    fn observe_snapshot(&mut self) -> bool {
        let was_paused = self.commands_paused();
        self.waiting_for_snapshot = false;
        self.finish_if_ready(was_paused)
    }

    fn observe_outcome_barrier(&mut self) -> bool {
        let was_paused = self.commands_paused();
        self.waiting_for_outcome_barrier = false;
        self.finish_if_ready(was_paused)
    }

    fn finish_if_ready(&mut self, was_paused: bool) -> bool {
        let ready = was_paused && !self.commands_paused();
        if ready {
            self.join_retry_at = None;
        }
        ready
    }

    fn finish_recovery(&mut self) {
        self.waiting_for_snapshot = false;
        self.waiting_for_outcome_barrier = false;
        self.join_retry_at = None;
    }

    fn mark_retry_sent(&mut self) {
        self.join_retry_at = None;
    }

    fn commands_paused(&self) -> bool {
        self.waiting_for_snapshot || self.waiting_for_outcome_barrier
    }
}

impl GameRuntime {
    fn from_snapshot(
        game_id: u32,
        user_id: u32,
        game_state: GameState,
        clock_offset_ms: i64,
    ) -> Result<Self> {
        let (snapshot_user_ids, snake_id) = snapshot_identity(game_id, user_id, &game_state)?;
        let ai_interval = ai_interval_for(&game_state, clock_offset_ms);
        let mut engine = GameEngine::new_from_state(game_id, game_state);
        engine.set_local_player_id(user_id);
        Ok(Self {
            engine,
            snake_id,
            snapshot_user_ids,
            ai_interval,
            last_decision_tick: None,
            pending_direction: None,
            outstanding_ping: None,
            promotion_suppression_floor: None,
        })
    }

    fn suppress_covered_promotion_event(&mut self, event: &GameEventMessage) -> bool {
        let Some(floor) = self.promotion_suppression_floor else {
            return false;
        };
        if matches!(
            &event.event,
            GameEvent::Snapshot { game_state }
                if matches!(game_state.status, GameStatus::Complete { .. })
        ) {
            self.promotion_suppression_floor = None;
            return false;
        }
        if event.stream_seq > floor {
            self.promotion_suppression_floor = None;
            return false;
        }
        true
    }

    /// Replace all client-predicted state with one authoritative snapshot.
    /// Returns true when that snapshot is already terminal.
    fn apply_snapshot(
        &mut self,
        game_id: u32,
        user_id: u32,
        game_state: GameState,
        clock_offset_ms: i64,
    ) -> Result<bool> {
        self.promotion_suppression_floor = None;
        let (snapshot_user_ids, snake_id) = snapshot_identity(game_id, user_id, &game_state)?;
        self.snapshot_user_ids = snapshot_user_ids;
        if matches!(&game_state.status, GameStatus::Complete { .. }) {
            return Ok(true);
        }

        let ai_interval = ai_interval_for(&game_state, clock_offset_ms);
        let mut engine = GameEngine::new_from_state(game_id, game_state);
        engine.set_local_player_id(user_id);
        self.engine = engine;
        self.snake_id = snake_id;
        self.ai_interval = ai_interval;
        self.last_decision_tick = None;
        self.pending_direction = None;
        self.outstanding_ping = None;
        Ok(false)
    }
}

enum SnapshotWaitError {
    Retryable(anyhow::Error),
    Fatal(anyhow::Error),
}

enum PreGameWaitError {
    Recoverable(anyhow::Error),
    Fatal(anyhow::Error),
}

impl PreGameWaitError {
    fn into_anyhow(self) -> anyhow::Error {
        match self {
            Self::Recoverable(error) | Self::Fatal(error) => error,
        }
    }
}

enum DriveAiError {
    Engine(anyhow::Error),
    Transport(anyhow::Error),
}

impl SnapshotWaitError {
    fn into_anyhow(self) -> anyhow::Error {
        match self {
            Self::Retryable(error) | Self::Fatal(error) => error,
        }
    }
}

/// Create a complete party, release it at the wave barrier, queue its host,
/// and drive every snake until authoritative completion or, for inherently
/// untimed modes, the configured successful active-play window.
pub async fn run_match_group(
    spec: MatchGroupSpec,
    settings: SessionSettings,
    http_client: Client,
    activity_events: mpsc::UnboundedSender<SessionActivityEvent>,
    wave_barrier: std::sync::Arc<Barrier>,
    cancellation: CancellationToken,
) -> MatchGroupResult {
    let expected_game_count = usize::from(settings.population == Population::Game);
    let group_id = spec.group_id();
    let wave_timeout = settings
        .connect_timeout
        .saturating_mul(2)
        .saturating_add(settings.lobby_timeout.saturating_mul(3));
    let mut prepared = join_all(spec.session_indices.iter().map(|session_index| {
        prepare_session(
            &spec.run_id,
            spec.wave_index,
            &group_id,
            *session_index,
            &settings,
            &http_client,
            activity_events.clone(),
            &cancellation,
        )
    }))
    .await;

    if prepared.iter().any(Result::is_err) {
        // Failed groups still arrive at the barrier so healthy groups in the
        // same wave can be released instead of deadlocking.
        wait_at_wave_barrier(&wave_barrier, wave_timeout, &cancellation).await;
        let mut sessions = Vec::with_capacity(prepared.len());
        for result in prepared.drain(..) {
            match result {
                Ok(mut live) => {
                    if cancellation.is_cancelled() {
                        live.record.cancel(
                            unix_time_ms(),
                            "load-test cancellation interrupted group preparation",
                        );
                    } else {
                        live.fail(
                            SessionPhase::LobbyReady,
                            "another member of this deterministic match group failed preparation",
                        );
                    }
                    let _ = live.socket.close(None).await;
                    sessions.push(live.into_record());
                }
                Err(record) => sessions.push(record),
            }
        }
        return MatchGroupResult {
            sessions,
            expected_game_count,
            observed_game_ids: BTreeSet::new(),
            pairing_violation: None,
        };
    }

    let mut sessions: Vec<LiveSession> = prepared
        .drain(..)
        .map(|result| result.expect("preparation errors handled above"))
        .collect();

    if settings.population == Population::Idle {
        if !wait_at_wave_barrier(&wave_barrier, wave_timeout, &cancellation).await {
            let records = fail_or_cancel_population(
                sessions,
                &cancellation,
                SessionPhase::WebSocketAuthentication,
                "wave coordination timed out before the idle hold",
            )
            .await;
            return population_group_result(records);
        }
        let records =
            hold_population_sessions(sessions, settings, BTreeSet::new(), cancellation).await;
        return population_group_result(records);
    }

    let lobby_result = prepare_lobby(&mut sessions, &settings, &cancellation).await;
    if let Err(error) = lobby_result {
        wait_at_wave_barrier(&wave_barrier, wave_timeout, &cancellation).await;
        let records = if cancellation.is_cancelled() {
            cancel_and_close_all(
                sessions,
                "load-test cancellation interrupted lobby preparation",
            )
            .await
        } else {
            let message = format!("lobby preparation failed: {error:#}");
            fail_and_close_all(sessions, SessionPhase::LobbyReady, &message).await
        };
        return MatchGroupResult {
            sessions: records,
            expected_game_count,
            observed_game_ids: BTreeSet::new(),
            pairing_violation: None,
        };
    }

    if !wait_at_wave_barrier(&wave_barrier, wave_timeout, &cancellation).await {
        let records = if cancellation.is_cancelled() {
            cancel_and_close_all(
                sessions,
                "load-test cancellation interrupted wave coordination",
            )
            .await
        } else {
            fail_and_close_all(
                sessions,
                SessionPhase::Matchmaking,
                "wave coordination barrier timed out",
            )
            .await
        };
        return MatchGroupResult {
            sessions: records,
            expected_game_count,
            observed_game_ids: BTreeSet::new(),
            pairing_violation: None,
        };
    }

    if settings.population == Population::Lobby {
        let expected_user_ids = sessions.iter().map(|session| session.user_id).collect();
        let records =
            hold_population_sessions(sessions, settings, expected_user_ids, cancellation).await;
        return population_group_result(records);
    }

    let queue_started = Instant::now();
    for session in &mut sessions {
        session.record.record_lifecycle(
            SessionLifecycleRecord::new(SessionPhase::Matchmaking, unix_time_ms())
                .with_message("coordinated wave released"),
        );
    }

    let expected_user_ids: BTreeSet<u32> = sessions.iter().map(|session| session.user_id).collect();
    let (group_game_id, _) = watch::channel(None::<u32>);
    let preferences = LobbyPreferences {
        selected_modes: vec![settings.selected_mode.clone()],
        competitive: settings.competitive,
    };
    let lobby_code = sessions[0]
        .record
        .lobby_code
        .clone()
        .expect("prepared lobby host must retain its lobby code");
    if let Err(error) = queue_lobby_with_recovery(
        &mut sessions[0],
        &settings,
        &lobby_code,
        &preferences,
        &expected_user_ids,
        &cancellation,
    )
    .await
    {
        let records = if cancellation.is_cancelled() {
            cancel_and_close_all(
                sessions,
                "load-test cancellation interrupted matchmaking queue entry",
            )
            .await
        } else {
            fail_and_close_all(
                sessions,
                SessionPhase::Matchmaking,
                &format!("failed to queue lobby host: {error:#}"),
            )
            .await
        };
        return MatchGroupResult {
            sessions: records,
            expected_game_count,
            observed_game_ids: BTreeSet::new(),
            pairing_violation: None,
        };
    }

    if settings.population == Population::Matchmaking {
        if let Err(error) = confirm_matchmaking_waiter(
            &mut sessions[0],
            &settings,
            &lobby_code,
            &preferences,
            &expected_user_ids,
            &cancellation,
        )
        .await
        {
            let records = fail_or_cancel_population(
                sessions,
                &cancellation,
                SessionPhase::Matchmaking,
                &format!("failed to confirm durable queued state: {error:#}"),
            )
            .await;
            return population_group_result(records);
        }
        let records =
            hold_population_sessions(sessions, settings, expected_user_ids, cancellation).await;
        return population_group_result(records);
    }

    let played = join_all(sessions.into_iter().map(|session| {
        play_session(
            session,
            settings.clone(),
            expected_user_ids.clone(),
            group_game_id.clone(),
            queue_started,
            cancellation.clone(),
        )
    }))
    .await;

    validate_group(played, &expected_user_ids, expected_game_count)
}

fn population_group_result(sessions: Vec<SessionRecord>) -> MatchGroupResult {
    MatchGroupResult {
        sessions,
        expected_game_count: 0,
        observed_game_ids: BTreeSet::new(),
        pairing_violation: None,
    }
}

async fn fail_or_cancel_population(
    sessions: Vec<LiveSession>,
    cancellation: &CancellationToken,
    phase: SessionPhase,
    message: &str,
) -> Vec<SessionRecord> {
    if cancellation.is_cancelled() {
        cancel_and_close_all(sessions, message).await
    } else {
        fail_and_close_all(sessions, phase, message).await
    }
}

async fn hold_population_sessions(
    sessions: Vec<LiveSession>,
    settings: SessionSettings,
    expected_user_ids: BTreeSet<u32>,
    cancellation: CancellationToken,
) -> Vec<SessionRecord> {
    join_all(sessions.into_iter().map(|session| {
        hold_population_session(
            session,
            settings.clone(),
            expected_user_ids.clone(),
            cancellation.clone(),
        )
    }))
    .await
}

async fn hold_population_session(
    mut session: LiveSession,
    settings: SessionSettings,
    expected_user_ids: BTreeSet<u32>,
    cancellation: CancellationToken,
) -> SessionRecord {
    let result =
        hold_population_session_inner(&mut session, &settings, &expected_user_ids, &cancellation)
            .await;
    if let Err(error) = result {
        // SIGTERM and coordinator cancellation are expected staging cleanup
        // paths. Use a fresh bounded token so the durable queue/lobby cleanup
        // is not itself pre-cancelled by the run-level token.
        let cleanup_token = CancellationToken::new();
        let cleanup_result = tokio::time::timeout(
            POPULATION_CLEANUP_TIMEOUT,
            cleanup_population_session(&mut session, &settings, &cleanup_token),
        )
        .await;
        match cleanup_result {
            Ok(Ok(())) => {}
            Ok(Err(cleanup_error)) => {
                session.record.diagnostics.insert(
                    "population_cleanup_error".to_owned(),
                    format!("{cleanup_error:#}"),
                );
            }
            Err(_) => {
                session.record.diagnostics.insert(
                    "population_cleanup_error".to_owned(),
                    "cleanup timed out".to_owned(),
                );
            }
        }
        if cancellation.is_cancelled() {
            session.record.cancel(
                unix_time_ms(),
                "load-test cancellation interrupted population hold",
            );
        } else if session.record.failure.is_none() {
            let phase = match settings.population {
                Population::Idle => SessionPhase::WebSocketAuthentication,
                Population::Lobby => SessionPhase::LobbyReady,
                Population::Matchmaking => SessionPhase::Matchmaking,
                Population::Game => SessionPhase::Playing,
            };
            session.fail(phase, format!("{error:#}"));
        }
    }
    let _ = session.socket.close(None).await;
    session.into_record()
}

async fn hold_population_session_inner(
    session: &mut LiveSession,
    settings: &SessionSettings,
    expected_user_ids: &BTreeSet<u32>,
    cancellation: &CancellationToken,
) -> Result<()> {
    debug_assert_ne!(settings.population, Population::Game);
    let hold_deadline = tokio::time::Instant::now() + settings.untimed_play_duration;
    let mut ping_interval = interval(PING_INTERVAL);
    ping_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let preferences = LobbyPreferences {
        selected_modes: vec![settings.selected_mode.clone()],
        competitive: settings.competitive,
    };

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(anyhow!("population hold cancelled"));
            }
            _ = tokio::time::sleep_until(hold_deadline) => {
                cleanup_population_session(session, settings, cancellation).await?;
                session.record.complete(unix_time_ms());
                return Ok(());
            }
            _ = ping_interval.tick() => {
                let client_time = next_ping_client_time(session);
                if let Err(error) = session
                    .send_cancellable(WSMessage::Ping { client_time }, cancellation)
                    .await
                {
                    recover_population_session(
                        session,
                        settings,
                        expected_user_ids,
                        &preferences,
                        cancellation,
                        error.context("sending population-hold ping"),
                    )
                    .await?;
                    ping_interval = interval(PING_INTERVAL);
                    ping_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                }
            }
            incoming = session.next_message() => {
                match incoming {
                    Ok(WSMessage::Pong { client_time, server_time }) => {
                        let now_ms = Utc::now().timestamp_millis();
                        session.clock_offset_ms = server_time.saturating_sub(
                            client_time.saturating_add(now_ms.saturating_sub(client_time) / 2),
                        );
                    }
                    Ok(WSMessage::Drain { task_boot_id, deadline_unix_ms }) => {
                        perform_pre_game_planned_handoff(
                            session,
                            settings,
                            expected_user_ids,
                            &preferences,
                            &task_boot_id,
                            deadline_unix_ms,
                            cancellation,
                        )
                        .await?;
                        ping_interval = interval(PING_INTERVAL);
                        ping_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                    }
                    Ok(WSMessage::MatchFound { game_id })
                        if settings.population == Population::Matchmaking =>
                    {
                        return Err(anyhow!(
                            "matchmaking waiter unexpectedly entered game {game_id}"
                        ));
                    }
                    Ok(WSMessage::LobbyUpdate { state, .. })
                        if settings.population == Population::Matchmaking && state != "queued" =>
                    {
                        return Err(anyhow!(
                            "matchmaking waiter left queued state for {state:?}"
                        ));
                    }
                    Ok(WSMessage::AccessDenied { reason }) => {
                        return Err(anyhow!("server denied population session: {reason}"));
                    }
                    Ok(WSMessage::LobbyRegionMismatch { target_region, .. }) => {
                        return Err(anyhow!("population lobby moved to region {target_region}"));
                    }
                    Ok(_) => {}
                    Err(error) => {
                        recover_population_session(
                            session,
                            settings,
                            expected_user_ids,
                            &preferences,
                            cancellation,
                            error.context("population-hold socket failed"),
                        )
                        .await?;
                        ping_interval = interval(PING_INTERVAL);
                        ping_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                    }
                }
            }
        }
    }
}

async fn recover_population_session(
    session: &mut LiveSession,
    settings: &SessionSettings,
    expected_user_ids: &BTreeSet<u32>,
    preferences: &LobbyPreferences,
    cancellation: &CancellationToken,
    cause: anyhow::Error,
) -> Result<()> {
    if settings.population == Population::Idle {
        recover_pre_game_socket(session, settings, cancellation, cause, true).await
    } else {
        let lobby_code = session
            .record
            .lobby_code
            .clone()
            .context("population session lost its lobby code")?;
        restore_lobby_membership(
            session,
            settings,
            &lobby_code,
            preferences,
            Some(expected_user_ids),
            cancellation,
            cause,
        )
        .await?;
        if settings.population == Population::Matchmaking
            && session.last_lobby_state.as_deref() != Some("queued")
        {
            return Err(anyhow!(
                "recovered matchmaking waiter was not durably queued"
            ));
        }
        Ok(())
    }
}

async fn confirm_matchmaking_waiter(
    session: &mut LiveSession,
    settings: &SessionSettings,
    lobby_code: &str,
    preferences: &LobbyPreferences,
    expected_user_ids: &BTreeSet<u32>,
    cancellation: &CancellationToken,
) -> Result<()> {
    loop {
        match session
            .wait_for_pre_game(
                settings.queue_timeout,
                cancellation,
                |message| match message {
                    WSMessage::LobbyUpdate {
                        lobby_code: updated,
                        members,
                        state,
                        ..
                    } if updated == lobby_code
                        && state == "queued"
                        && members
                            .iter()
                            .map(|member| member.user_id)
                            .collect::<BTreeSet<_>>()
                            == *expected_user_ids =>
                    {
                        Some(Ok(()))
                    }
                    WSMessage::MatchFound { game_id } => Some(Err(anyhow!(
                        "matchmaking waiter unexpectedly entered game {game_id}"
                    ))),
                    _ => None,
                },
            )
            .await
        {
            Ok(result) => {
                result?;
                let queued_at = unix_time_ms();
                session
                    .record
                    .diagnostics
                    .insert("queued_at_unix_ms".to_owned(), queued_at.to_string());
                session.record.record_lifecycle(
                    SessionLifecycleRecord::new(SessionPhase::Matchmaking, queued_at)
                        .with_message("durable queued state observed"),
                );
                return Ok(());
            }
            Err(PreGameWaitError::Recoverable(error)) => {
                restore_lobby_membership(
                    session,
                    settings,
                    lobby_code,
                    preferences,
                    Some(expected_user_ids),
                    cancellation,
                    error,
                )
                .await?;
                if session.last_lobby_state.as_deref() == Some("queued") {
                    session
                        .record
                        .diagnostics
                        .insert("queued_at_unix_ms".to_owned(), unix_time_ms().to_string());
                    return Ok(());
                }
            }
            Err(PreGameWaitError::Fatal(error)) => return Err(error),
        }
    }
}

async fn cleanup_population_session(
    session: &mut LiveSession,
    settings: &SessionSettings,
    cancellation: &CancellationToken,
) -> Result<()> {
    session.record.record_lifecycle(
        SessionLifecycleRecord::new(SessionPhase::Cleanup, unix_time_ms())
            .with_message("population hold completed"),
    );
    if settings.population == Population::Matchmaking {
        session
            .send_cancellable(WSMessage::LeaveQueue, cancellation)
            .await?;
        // LeaveQueue has no dedicated acknowledgement. The following ordered
        // application pong proves the server processed the cancellation first.
        send_tagged_ping_and_wait(session, None, settings.connect_timeout, cancellation).await?;
    }
    if settings.population != Population::Idle {
        session
            .send_cancellable(WSMessage::LeaveLobby, cancellation)
            .await?;
        session
            .wait_for_pre_game(settings.connect_timeout, cancellation, |message| {
                matches!(message, WSMessage::LeftLobby).then_some(())
            })
            .await
            .map_err(PreGameWaitError::into_anyhow)?;
    }
    Ok(())
}

struct ReadyPreGameCandidate {
    socket: Socket,
    backend: Option<String>,
    sticky_cookie: Option<String>,
    capabilities: BTreeSet<String>,
    task_boot_id: String,
    socket_generation: u64,
    lobby_members: BTreeSet<u32>,
    lobby_state: Option<String>,
    auth_ms: u64,
    lobby_rejoin_ms: Option<u64>,
    ready_at: Instant,
    old_usable_through: Instant,
}

enum PreGameHandoffAttemptError {
    Candidate(anyhow::Error),
    Active(anyhow::Error),
    Fatal(anyhow::Error),
}

async fn next_pre_game_candidate_message(
    session: &mut LiveSession,
    socket: &mut Socket,
    deadline: tokio::time::Instant,
    cancellation: &CancellationToken,
) -> std::result::Result<WSMessage, PreGameHandoffAttemptError> {
    let message = tokio::select! {
        _ = cancellation.cancelled() => {
            return Err(PreGameHandoffAttemptError::Fatal(anyhow!("operation cancelled")));
        }
        _ = tokio::time::sleep_until(deadline) => {
            return Err(PreGameHandoffAttemptError::Candidate(anyhow!(
                "planned handoff candidate did not become ready before its deadline"
            )));
        }
        result = next_socket_message(socket) => {
            result.map_err(PreGameHandoffAttemptError::Candidate)?
        }
    };
    session.observe_received(&message);
    Ok(message)
}

#[allow(clippy::too_many_arguments)]
async fn prepare_pre_game_candidate(
    session: &mut LiveSession,
    settings: &SessionSettings,
    expected_user_ids: &BTreeSet<u32>,
    preferences: &LobbyPreferences,
    departing_task_boot_id: &str,
    deadline: tokio::time::Instant,
    cancellation: &CancellationToken,
) -> std::result::Result<ReadyPreGameCandidate, PreGameHandoffAttemptError> {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return Err(PreGameHandoffAttemptError::Candidate(anyhow!(
            "planned handoff deadline reached before candidate connection"
        )));
    }
    let connect_timeout = settings.connect_timeout.min(remaining);
    let (mut socket, backend, sticky_cookie) = connect_socket(
        &session.websocket_url,
        &session.origin,
        connect_timeout,
        &session.backend_hints,
        None,
    )
    .await
    .map_err(PreGameHandoffAttemptError::Candidate)?;

    let auth_started = Instant::now();
    send_candidate_message(
        session,
        &mut socket,
        WSMessage::Token(session.token.clone()),
        cancellation,
    )
    .await
    .map_err(PreGameHandoffAttemptError::Candidate)?;

    let (task_boot_id, socket_generation, capabilities) = loop {
        match next_pre_game_candidate_message(session, &mut socket, deadline, cancellation).await? {
            WSMessage::Authenticated {
                task_boot_id,
                protocol_version: _,
                capabilities,
                socket_generation,
            } => {
                let capabilities: BTreeSet<_> = capabilities.into_iter().collect();
                validate_required_server_capabilities(&capabilities)
                    .context("planned handoff candidate is incompatible")
                    .map_err(PreGameHandoffAttemptError::Candidate)?;
                if task_boot_id == departing_task_boot_id {
                    return Err(PreGameHandoffAttemptError::Candidate(anyhow!(
                        "replacement connection returned to departing task {task_boot_id}"
                    )));
                }
                break (task_boot_id, socket_generation, capabilities);
            }
            WSMessage::Drain { .. } => {
                return Err(PreGameHandoffAttemptError::Candidate(anyhow!(
                    "replacement connection reached another draining task"
                )));
            }
            WSMessage::AccessDenied { reason } => {
                return Err(PreGameHandoffAttemptError::Candidate(anyhow!(
                    "replacement authentication was denied: {reason}"
                )));
            }
            _ => {}
        }
    };
    let auth_ms = elapsed_ms(auth_started);

    let mut lobby_members = BTreeSet::new();
    let mut lobby_state = None;
    let mut lobby_rejoin_ms = None;
    if settings.population != Population::Idle {
        let lobby_code = session
            .record
            .lobby_code
            .clone()
            .context("planned handoff session lost its lobby code")
            .map_err(PreGameHandoffAttemptError::Fatal)?;
        let rejoin_started = Instant::now();
        send_candidate_message(
            session,
            &mut socket,
            WSMessage::JoinLobby {
                lobby_code: lobby_code.clone(),
                preferences: Some(preferences.clone()),
            },
            cancellation,
        )
        .await
        .map_err(PreGameHandoffAttemptError::Candidate)?;
        loop {
            match next_pre_game_candidate_message(session, &mut socket, deadline, cancellation)
                .await?
            {
                WSMessage::LobbyUpdate {
                    lobby_code: updated,
                    members,
                    state,
                    ..
                } if updated == lobby_code => {
                    lobby_members = members.iter().map(|member| member.user_id).collect();
                    if lobby_members != *expected_user_ids {
                        continue;
                    }
                    if settings.population == Population::Matchmaking && state != "queued" {
                        return Err(PreGameHandoffAttemptError::Fatal(anyhow!(
                            "replacement observed matchmaking state {state:?}, expected queued"
                        )));
                    }
                    lobby_state = Some(state);
                    lobby_rejoin_ms = Some(elapsed_ms(rejoin_started));
                    break;
                }
                WSMessage::MatchFound { game_id }
                    if settings.population == Population::Matchmaking =>
                {
                    return Err(PreGameHandoffAttemptError::Fatal(anyhow!(
                        "matchmaking waiter unexpectedly entered game {game_id}"
                    )));
                }
                WSMessage::Drain { .. } => {
                    return Err(PreGameHandoffAttemptError::Candidate(anyhow!(
                        "replacement connection began draining during lobby restoration"
                    )));
                }
                WSMessage::AccessDenied { reason } => {
                    return Err(PreGameHandoffAttemptError::Candidate(anyhow!(
                        "replacement lobby restoration was denied: {reason}"
                    )));
                }
                WSMessage::LobbyRegionMismatch { target_region, .. } => {
                    return Err(PreGameHandoffAttemptError::Fatal(anyhow!(
                        "replacement lobby moved to region {target_region}"
                    )));
                }
                _ => {}
            }
        }
    }

    let ready_at = Instant::now();
    let continuity_client_time = next_ping_client_time(session);
    session
        .send_cancellable(
            WSMessage::Ping {
                client_time: continuity_client_time,
            },
            cancellation,
        )
        .await
        .map_err(PreGameHandoffAttemptError::Active)?;

    let old_usable_through = loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(PreGameHandoffAttemptError::Fatal(anyhow!("operation cancelled")));
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(PreGameHandoffAttemptError::Candidate(anyhow!(
                    "old socket did not prove continuity before the handoff deadline"
                )));
            }
            active = session.next_message() => {
                match active.map_err(PreGameHandoffAttemptError::Active)? {
                    WSMessage::Pong { client_time, .. } if client_time == continuity_client_time => {
                        break Instant::now();
                    }
                    WSMessage::Drain { .. } => {}
                    WSMessage::MatchFound { game_id }
                        if settings.population == Population::Matchmaking =>
                    {
                        return Err(PreGameHandoffAttemptError::Fatal(anyhow!(
                            "matchmaking waiter unexpectedly entered game {game_id}"
                        )));
                    }
                    WSMessage::AccessDenied { reason } => {
                        return Err(PreGameHandoffAttemptError::Active(anyhow!(
                            "active connection was denied during handoff: {reason}"
                        )));
                    }
                    _ => {}
                }
            }
            candidate = next_socket_message(&mut socket) => {
                let message = candidate.map_err(PreGameHandoffAttemptError::Candidate)?;
                session.observe_received(&message);
                match message {
                    WSMessage::Drain { .. } => {
                        return Err(PreGameHandoffAttemptError::Candidate(anyhow!(
                            "ready replacement connection began draining"
                        )));
                    }
                    WSMessage::MatchFound { game_id }
                        if settings.population == Population::Matchmaking =>
                    {
                        return Err(PreGameHandoffAttemptError::Fatal(anyhow!(
                            "matchmaking waiter unexpectedly entered game {game_id}"
                        )));
                    }
                    WSMessage::AccessDenied { reason } => {
                        return Err(PreGameHandoffAttemptError::Candidate(anyhow!(
                            "ready replacement connection was denied: {reason}"
                        )));
                    }
                    _ => {}
                }
            }
        }
    };

    Ok(ReadyPreGameCandidate {
        socket,
        backend,
        sticky_cookie,
        capabilities,
        task_boot_id,
        socket_generation,
        lobby_members,
        lobby_state,
        auth_ms,
        lobby_rejoin_ms,
        ready_at,
        old_usable_through,
    })
}

#[allow(clippy::too_many_arguments)]
async fn perform_pre_game_planned_handoff(
    session: &mut LiveSession,
    settings: &SessionSettings,
    expected_user_ids: &BTreeSet<u32>,
    preferences: &LobbyPreferences,
    departing_task_boot_id: &str,
    deadline_unix_ms: i64,
    cancellation: &CancellationToken,
) -> Result<()> {
    let started = Instant::now();
    session.record.metrics.planned_handoff_attempts = session
        .record
        .metrics
        .planned_handoff_attempts
        .saturating_add(1);
    session.record.record_lifecycle(
        SessionLifecycleRecord::new(SessionPhase::WebSocketConnect, unix_time_ms())
            .with_message("planned pre-game make-before-break handoff started"),
    );
    let remaining_ms = deadline_unix_ms.saturating_sub(Utc::now().timestamp_millis());
    if remaining_ms <= 0 {
        let error = anyhow!("planned handoff deadline was already expired");
        record_planned_handoff_failure(session, started, &error);
        return Err(error);
    }
    let deadline = tokio::time::Instant::now() + Duration::from_millis(remaining_ms as u64);
    let mut candidate_failures = 0_u64;

    loop {
        match prepare_pre_game_candidate(
            session,
            settings,
            expected_user_ids,
            preferences,
            departing_task_boot_id,
            deadline,
            cancellation,
        )
        .await
        {
            Ok(mut candidate) => {
                let mut old_socket = std::mem::replace(&mut session.socket, candidate.socket);
                if candidate.sticky_cookie.is_some() {
                    session.sticky_cookie = candidate.sticky_cookie.take();
                }
                session.server_capabilities = candidate.capabilities;
                session.last_lobby_members = candidate.lobby_members;
                session.last_lobby_state = candidate.lobby_state;
                let ordinal = session.record.metrics.planned_handoff_successes + 1;
                session.record.diagnostics.insert(
                    format!("planned_handoff_task_boot_id_{ordinal}"),
                    candidate.task_boot_id,
                );
                session.record.diagnostics.insert(
                    format!("planned_handoff_socket_generation_{ordinal}"),
                    candidate.socket_generation.to_string(),
                );
                if let Some(backend) = candidate.backend {
                    session
                        .record
                        .diagnostics
                        .insert(format!("planned_handoff_backend_{ordinal}"), backend);
                }
                session
                    .record
                    .metrics
                    .websocket_auth_ms
                    .push(candidate.auth_ms);
                if let Some(value) = candidate.lobby_rejoin_ms {
                    session.record.metrics.rejoin_lobby_ms.push(value);
                }
                session.record.metrics.planned_handoff_successes = session
                    .record
                    .metrics
                    .planned_handoff_successes
                    .saturating_add(1);
                session.record.metrics.planned_handoff_continuity_proofs = session
                    .record
                    .metrics
                    .planned_handoff_continuity_proofs
                    .saturating_add(1);
                session
                    .record
                    .metrics
                    .planned_handoff_duration_ms
                    .push(elapsed_ms(started));
                session.record.metrics.usable_session_gap_ms.push(0);
                session
                    .record
                    .metrics
                    .planned_handoff_active_overlap_ms
                    .push(duration_between_ms(
                        candidate.old_usable_through,
                        candidate.ready_at,
                    ));
                session.record.record_lifecycle(
                    SessionLifecycleRecord::new(
                        SessionPhase::WebSocketAuthentication,
                        unix_time_ms(),
                    )
                    .with_message("planned pre-game handoff promoted a ready candidate"),
                );
                let _ = old_socket.close(None).await;
                return Ok(());
            }
            Err(PreGameHandoffAttemptError::Candidate(error)) => {
                candidate_failures = candidate_failures.saturating_add(1);
                session.record.diagnostics.insert(
                    "planned_handoff_candidate_failures".to_owned(),
                    candidate_failures.to_string(),
                );
                session.record.diagnostics.insert(
                    "last_planned_candidate_failure".to_owned(),
                    format!("{error:#}"),
                );
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    record_planned_handoff_failure(session, started, &error);
                    return Err(error);
                }
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        let error = anyhow!("operation cancelled during planned handoff retry");
                        record_planned_handoff_failure(session, started, &error);
                        return Err(error);
                    }
                    _ = tokio::time::sleep(PLANNED_HANDOFF_RETRY_DELAY.min(remaining)) => {}
                }
            }
            Err(PreGameHandoffAttemptError::Active(error))
            | Err(PreGameHandoffAttemptError::Fatal(error)) => {
                record_planned_handoff_failure(session, started, &error);
                return Err(error);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn prepare_session(
    run_id: &str,
    wave_index: u32,
    group_id: &str,
    session_index: u64,
    settings: &SessionSettings,
    http_client: &Client,
    activity_events: mpsc::UnboundedSender<SessionActivityEvent>,
    cancellation: &CancellationToken,
) -> std::result::Result<LiveSession, SessionRecord> {
    // Created before any fallible work so every planned session produces a
    // terminal activity event, including guest-authentication failures.
    let activity_lease = SessionActivityLease::new(session_index, activity_events);
    let started_at = unix_time_ms();
    let session_id = format!("session-{session_index:08}");
    let username = deterministic_username(run_id, session_index);
    let mut record = SessionRecord::new(
        session_id,
        username.clone(),
        wave_index,
        group_id,
        started_at,
    );

    record.record_lifecycle(SessionLifecycleRecord::new(
        SessionPhase::GuestAuthentication,
        unix_time_ms(),
    ));
    let auth_started = Instant::now();
    let guest_result = tokio::select! {
        _ = cancellation.cancelled() => {
            record.cancel(
                unix_time_ms(),
                "load-test cancellation interrupted guest authentication",
            );
            return Err(record);
        }
        result = create_guest(
            http_client,
            &settings.api_origin,
            &username,
            settings.connect_timeout,
        ) => result,
    };
    let guest = match guest_result {
        Ok(guest) => guest,
        Err(error) => {
            fail_record(
                &mut record,
                SessionPhase::GuestAuthentication,
                format!("{error:#}"),
            );
            return Err(record);
        }
    };
    record.metrics.guest_auth_ms = Some(elapsed_ms(auth_started));
    record.username = guest.user.username.clone();

    let user_id = match u32::try_from(guest.user.id) {
        Ok(user_id) => user_id,
        Err(_) => {
            fail_record(
                &mut record,
                SessionPhase::GuestAuthentication,
                "guest API returned an invalid user ID",
            );
            return Err(record);
        }
    };
    record
        .diagnostics
        .insert("user_id".to_owned(), user_id.to_string());

    record.record_lifecycle(SessionLifecycleRecord::new(
        SessionPhase::WebSocketConnect,
        unix_time_ms(),
    ));
    // One admission budget starts at the first connection attempt and ends only
    // after an ordered application pong. A draining task can race Traefik route
    // withdrawal and return HTTP 503, while the bounded ingress limiter can
    // return HTTP 429 during a valid make-before-break burst. Retry only those
    // transient responses while retaining this session's guest token and
    // logical identity.
    let admission_started = Instant::now();
    let admission_deadline = tokio::time::Instant::now() + settings.connect_timeout;
    let mut admission_attempts = 0u32;
    let (socket, backend, sticky_cookie) = loop {
        admission_attempts = admission_attempts.saturating_add(1);
        let remaining = admission_deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            fail_record(
                &mut record,
                SessionPhase::WebSocketConnect,
                "WebSocket admission exhausted its bounded connection-to-pong budget",
            );
            return Err(record);
        }
        let connect_result = tokio::select! {
            _ = cancellation.cancelled() => {
                record.cancel(
                    unix_time_ms(),
                    "load-test cancellation interrupted WebSocket connection",
                );
                return Err(record);
            }
            result = connect_socket(
                &settings.websocket_url,
                &settings.origin,
                remaining,
                &settings.backend_hints,
                None,
            ) => result,
        };
        match connect_result {
            Ok(value) => break value,
            Err(error) if is_retryable_websocket_admission(&error) => {
                record.record_lifecycle(
                    SessionLifecycleRecord::new(SessionPhase::WebSocketConnect, unix_time_ms())
                        .with_message(
                            "transient HTTP 429/503 admission response; retrying the same guest token",
                        ),
                );
                let remaining =
                    admission_deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    fail_record(
                        &mut record,
                        SessionPhase::WebSocketConnect,
                        format!(
                            "transient HTTP admission retries exhausted the bounded budget: {error:#}"
                        ),
                    );
                    return Err(record);
                }
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        record.cancel(
                            unix_time_ms(),
                            "load-test cancellation interrupted WebSocket admission retry",
                        );
                        return Err(record);
                    }
                    _ = tokio::time::sleep(ADMISSION_RETRY_DELAY.min(remaining)) => {}
                }
            }
            Err(error) => {
                fail_record(
                    &mut record,
                    SessionPhase::WebSocketConnect,
                    format!("{error:#}"),
                );
                return Err(record);
            }
        }
    };
    record.metrics.websocket_connect_ms = Some(elapsed_ms(admission_started));
    record.diagnostics.insert(
        "initial_admission_attempts".to_owned(),
        admission_attempts.to_string(),
    );
    if let Some(backend) = backend {
        record
            .diagnostics
            .insert("websocket_backend".to_owned(), backend);
    }

    let mut session = LiveSession {
        record,
        user_id,
        token: guest.token,
        socket,
        websocket_url: settings.websocket_url.clone(),
        origin: settings.origin.clone(),
        backend_hints: settings.backend_hints.clone(),
        sticky_cookie,
        last_lobby_members: BTreeSet::new(),
        last_lobby_state: None,
        recent_events: VecDeque::new(),
        clock_offset_ms: 0,
        last_ping_client_time: None,
        current_task_boot_id: None,
        current_socket_generation: None,
        reconnects: 0,
        client_game_session_id: format!("loadtest-{run_id}-{session_index}"),
        next_command_sequence: 1,
        pending_commands: BTreeMap::new(),
        server_capabilities: BTreeSet::new(),
        activity_lease,
    };

    let websocket_auth_started = Instant::now();
    let token = session.token.clone();
    let token_result = tokio::time::timeout_at(
        admission_deadline,
        session.send_cancellable(WSMessage::Token(token), cancellation),
    )
    .await;
    if let Err(error) = token_result
        .map_err(|_| anyhow!("bounded admission deadline expired while sending the token"))
        .and_then(|result| result)
    {
        if cancellation.is_cancelled() {
            session.record.cancel(
                unix_time_ms(),
                "load-test cancellation interrupted WebSocket authentication",
            );
            return Err(session.into_record());
        }
        session.fail(
            SessionPhase::WebSocketAuthentication,
            format!("failed to send authentication token: {error:#}"),
        );
        return Err(session.into_record());
    }
    // The token has crossed the initial socket, which is the report's logical
    // concurrency boundary. The ordered ping below then confirms processing
    // and establishes the server clock offset before any game timing occurs.
    session.record.record_lifecycle(
        SessionLifecycleRecord::new(SessionPhase::WebSocketAuthentication, unix_time_ms())
            .with_message("token sent; awaiting explicit authentication"),
    );
    let remaining = admission_deadline.saturating_duration_since(tokio::time::Instant::now());
    if let Err(error) = session
        .wait_for_authenticated(remaining, cancellation)
        .await
    {
        session.fail(
            SessionPhase::WebSocketAuthentication,
            format!("authentication acknowledgement failed: {error:#}"),
        );
        return Err(session.into_record());
    }
    session
        .record
        .metrics
        .websocket_auth_ms
        .push(elapsed_ms(websocket_auth_started));
    session.activity_lease.mark_connected();
    let remaining = admission_deadline.saturating_duration_since(tokio::time::Instant::now());
    if let Err(error) = send_tagged_ping_and_wait(&mut session, None, remaining, cancellation).await
    {
        if cancellation.is_cancelled() {
            session.record.cancel(
                unix_time_ms(),
                "load-test cancellation interrupted WebSocket authentication",
            );
            return Err(session.into_record());
        }
        session.fail(
            SessionPhase::WebSocketAuthentication,
            format!("authentication clock-sync ping failed: {error:#}"),
        );
        return Err(session.into_record());
    }
    session.record.metrics.initial_admission_ready_ms = Some(elapsed_ms(admission_started));
    session.record.record_lifecycle(
        SessionLifecycleRecord::new(SessionPhase::WebSocketAuthentication, unix_time_ms())
            .with_message("token processed; server clock synchronized"),
    );
    Ok(session)
}

async fn prepare_lobby(
    sessions: &mut [LiveSession],
    settings: &SessionSettings,
    cancellation: &CancellationToken,
) -> Result<()> {
    if sessions.is_empty() {
        return Err(anyhow!("match group has no sessions"));
    }
    let lobby_started = Instant::now();

    sessions[0]
        .record
        .record_lifecycle(SessionLifecycleRecord::new(
            SessionPhase::LobbyCreate,
            unix_time_ms(),
        ));
    let lobby_code = create_lobby_with_recovery(&mut sessions[0], settings, cancellation).await?;
    sessions[0].record.lobby_code = Some(lobby_code.clone());

    let preferences = LobbyPreferences {
        selected_modes: vec![settings.selected_mode.clone()],
        competitive: settings.competitive,
    };
    send_preferences_with_recovery(
        &mut sessions[0],
        settings,
        &lobby_code,
        &preferences,
        None,
        cancellation,
    )
    .await?;

    let joins = sessions.iter_mut().skip(1).map(|session| {
        let lobby_code = lobby_code.clone();
        let preferences = preferences.clone();
        let cancellation = cancellation.clone();
        async move {
            session.record.record_lifecycle(SessionLifecycleRecord::new(
                SessionPhase::LobbyJoin,
                unix_time_ms(),
            ));
            join_lobby_with_recovery(session, settings, &lobby_code, &preferences, &cancellation)
                .await
        }
    });
    for result in join_all(joins).await {
        result?;
    }

    // Followers subscribe to lobby updates as part of JoinLobby. Re-broadcast the
    // authoritative preferences after every JoinedLobby acknowledgement so none
    // can miss the host's first update during that subscription window.
    send_preferences_with_recovery(
        &mut sessions[0],
        settings,
        &lobby_code,
        &preferences,
        None,
        cancellation,
    )
    .await?;

    let expected: BTreeSet<u32> = sessions.iter().map(|session| session.user_id).collect();
    let ready = sessions.iter_mut().map(|session| {
        let expected = expected.clone();
        let lobby_code = lobby_code.clone();
        let preferences = preferences.clone();
        let cancellation = cancellation.clone();
        async move {
            wait_for_lobby_roster_with_recovery(
                session,
                settings,
                &lobby_code,
                &preferences,
                &expected,
                &cancellation,
            )
            .await?;
            session.record.metrics.lobby_ready_ms = Some(elapsed_ms(lobby_started));
            session.record.record_lifecycle(
                SessionLifecycleRecord::new(SessionPhase::LobbyReady, unix_time_ms())
                    .with_elapsed_ms(elapsed_ms(lobby_started)),
            );
            Ok::<(), anyhow::Error>(())
        }
    });
    for result in join_all(ready).await {
        result?;
    }
    Ok(())
}

async fn create_lobby_with_recovery(
    session: &mut LiveSession,
    settings: &SessionSettings,
    cancellation: &CancellationToken,
) -> Result<String> {
    loop {
        if let Err(error) = session
            .send_cancellable(WSMessage::CreateLobby, cancellation)
            .await
        {
            recover_pre_game_socket(session, settings, cancellation, error, true).await?;
            continue;
        }

        match session
            .wait_for_pre_game(
                settings.lobby_timeout,
                cancellation,
                |message| match message {
                    WSMessage::LobbyCreated { lobby_code } => Some(lobby_code.clone()),
                    _ => None,
                },
            )
            .await
        {
            Ok(lobby_code) => return Ok(lobby_code),
            Err(PreGameWaitError::Recoverable(error)) => {
                // The acknowledgement may have been lost after creation. A
                // retry can leave a short-lived orphan lobby, but never mixes
                // this deterministic group with an unknown lobby.
                recover_pre_game_socket(session, settings, cancellation, error, true).await?;
            }
            Err(error) => return Err(error.into_anyhow()).context("waiting for LobbyCreated"),
        }
    }
}

async fn join_lobby_once(
    session: &mut LiveSession,
    settings: &SessionSettings,
    lobby_code: &str,
    preferences: &LobbyPreferences,
    cancellation: &CancellationToken,
) -> std::result::Result<(), PreGameWaitError> {
    session
        .send_cancellable(
            WSMessage::JoinLobby {
                lobby_code: lobby_code.to_owned(),
                preferences: Some(preferences.clone()),
            },
            cancellation,
        )
        .await
        .map_err(PreGameWaitError::Recoverable)?;
    let joined_code = session
        .wait_for_pre_game(
            settings.lobby_timeout,
            cancellation,
            |message| match message {
                WSMessage::JoinedLobby { lobby_code } => Some(lobby_code.clone()),
                _ => None,
            },
        )
        .await?;
    if joined_code != lobby_code {
        return Err(PreGameWaitError::Fatal(anyhow!(
            "server joined session to lobby {joined_code}, expected {lobby_code}"
        )));
    }
    session.record.lobby_code = Some(joined_code);
    Ok(())
}

async fn join_lobby_with_recovery(
    session: &mut LiveSession,
    settings: &SessionSettings,
    lobby_code: &str,
    preferences: &LobbyPreferences,
    cancellation: &CancellationToken,
) -> Result<()> {
    match join_lobby_once(session, settings, lobby_code, preferences, cancellation).await {
        Ok(()) => Ok(()),
        Err(PreGameWaitError::Recoverable(error)) => {
            restore_lobby_membership(
                session,
                settings,
                lobby_code,
                preferences,
                None,
                cancellation,
                error,
            )
            .await
        }
        Err(error) => Err(error.into_anyhow()),
    }
}

fn preferences_message(preferences: &LobbyPreferences) -> WSMessage {
    WSMessage::UpdateLobbyPreferences {
        selected_modes: preferences.selected_modes.clone(),
        competitive: preferences.competitive,
    }
}

async fn send_preferences_with_recovery(
    session: &mut LiveSession,
    settings: &SessionSettings,
    lobby_code: &str,
    preferences: &LobbyPreferences,
    expected_user_ids: Option<&BTreeSet<u32>>,
    cancellation: &CancellationToken,
) -> Result<()> {
    match session
        .send_cancellable(preferences_message(preferences), cancellation)
        .await
    {
        Ok(()) => Ok(()),
        Err(error) => {
            restore_lobby_membership(
                session,
                settings,
                lobby_code,
                preferences,
                expected_user_ids,
                cancellation,
                error,
            )
            .await
        }
    }
}

async fn wait_for_lobby_roster_once(
    session: &mut LiveSession,
    settings: &SessionSettings,
    lobby_code: &str,
    expected_user_ids: &BTreeSet<u32>,
    cancellation: &CancellationToken,
) -> std::result::Result<(), PreGameWaitError> {
    if &session.last_lobby_members == expected_user_ids {
        return Ok(());
    }
    session
        .wait_for_pre_game(
            settings.lobby_timeout,
            cancellation,
            |message| match message {
                WSMessage::LobbyUpdate {
                    lobby_code: actual_code,
                    members,
                    ..
                } if actual_code == lobby_code => {
                    let actual: BTreeSet<u32> =
                        members.iter().map(|member| member.user_id).collect();
                    (actual == *expected_user_ids).then_some(())
                }
                _ => None,
            },
        )
        .await
}

async fn wait_for_lobby_roster_with_recovery(
    session: &mut LiveSession,
    settings: &SessionSettings,
    lobby_code: &str,
    preferences: &LobbyPreferences,
    expected_user_ids: &BTreeSet<u32>,
    cancellation: &CancellationToken,
) -> Result<()> {
    match wait_for_lobby_roster_once(
        session,
        settings,
        lobby_code,
        expected_user_ids,
        cancellation,
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(PreGameWaitError::Recoverable(error)) => {
            restore_lobby_membership(
                session,
                settings,
                lobby_code,
                preferences,
                Some(expected_user_ids),
                cancellation,
                error,
            )
            .await
        }
        Err(error) => Err(error.into_anyhow()),
    }
}

async fn restore_lobby_membership(
    session: &mut LiveSession,
    settings: &SessionSettings,
    lobby_code: &str,
    preferences: &LobbyPreferences,
    expected_user_ids: Option<&BTreeSet<u32>>,
    cancellation: &CancellationToken,
    mut cause: anyhow::Error,
) -> Result<()> {
    let rejoin_started = Instant::now();
    loop {
        recover_pre_game_socket(session, settings, cancellation, cause, false).await?;
        session.last_lobby_members.clear();
        session.last_lobby_state = None;

        if let Err(error) =
            join_lobby_once(session, settings, lobby_code, preferences, cancellation).await
        {
            match error {
                PreGameWaitError::Recoverable(error) => {
                    cause = error;
                    continue;
                }
                PreGameWaitError::Fatal(error) => return Err(error),
            }
        }

        if let Err(error) = session
            .send_cancellable(preferences_message(preferences), cancellation)
            .await
        {
            cause = error;
            continue;
        }

        if let Some(expected_user_ids) = expected_user_ids {
            match wait_for_lobby_roster_once(
                session,
                settings,
                lobby_code,
                expected_user_ids,
                cancellation,
            )
            .await
            {
                Ok(()) => {}
                Err(PreGameWaitError::Recoverable(error)) => {
                    cause = error;
                    continue;
                }
                Err(PreGameWaitError::Fatal(error)) => return Err(error),
            }
        }

        session.record.record_lifecycle(
            SessionLifecycleRecord::new(SessionPhase::LobbyReady, unix_time_ms())
                .with_message("reconnected and restored exact lobby membership"),
        );
        session
            .record
            .metrics
            .rejoin_lobby_ms
            .push(elapsed_ms(rejoin_started));
        session
            .record
            .metrics
            .usable_session_gap_ms
            .push(elapsed_ms(rejoin_started));
        return Ok(());
    }
}

async fn recover_pre_game_socket(
    session: &mut LiveSession,
    settings: &SessionSettings,
    cancellation: &CancellationToken,
    cause: anyhow::Error,
    usable_after_authentication: bool,
) -> Result<()> {
    session.record.metrics.disconnects = session.record.metrics.disconnects.saturating_add(1);
    let recovery_started = Instant::now();
    let mut last_error = cause;
    loop {
        if cancellation.is_cancelled() {
            return Err(anyhow!(
                "operation cancelled while restoring pre-game session"
            ));
        }
        if session.reconnects >= MAX_RECONNECTS {
            return Err(anyhow!(
                "websocket recovery budget exhausted before game assignment: {last_error:#}"
            ));
        }

        session.reconnects += 1;
        session.record.metrics.reconnects = session.record.metrics.reconnects.saturating_add(1);
        session
            .record
            .diagnostics
            .insert("reconnects".to_owned(), session.reconnects.to_string());
        tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(anyhow!("operation cancelled while restoring pre-game session"));
            }
            _ = tokio::time::sleep(RECONNECT_DELAY) => {}
        }

        session.record.record_lifecycle(
            SessionLifecycleRecord::new(SessionPhase::WebSocketConnect, unix_time_ms())
                .with_message("reconnecting before game assignment"),
        );
        match session
            .reconnect(settings.connect_timeout, cancellation)
            .await
        {
            Ok(()) => {
                let duration_ms = elapsed_ms(recovery_started);
                session
                    .record
                    .metrics
                    .reconnect_duration_ms
                    .push(duration_ms);
                if usable_after_authentication {
                    session
                        .record
                        .metrics
                        .usable_session_gap_ms
                        .push(duration_ms);
                }
                session.record.record_lifecycle(
                    SessionLifecycleRecord::new(
                        SessionPhase::WebSocketAuthentication,
                        unix_time_ms(),
                    )
                    .with_message("re-authenticated before game assignment"),
                );
                return Ok(());
            }
            Err(error) => {
                last_error = error.context("reconnecting and re-authenticating pre-game socket");
            }
        }
    }
}

async fn queue_lobby_with_recovery(
    session: &mut LiveSession,
    settings: &SessionSettings,
    lobby_code: &str,
    preferences: &LobbyPreferences,
    expected_user_ids: &BTreeSet<u32>,
    cancellation: &CancellationToken,
) -> Result<()> {
    loop {
        match session
            .send_cancellable(
                WSMessage::QueueForMatch {
                    game_type: settings.game_type.clone(),
                    queue_mode: settings.queue_mode.clone(),
                },
                cancellation,
            )
            .await
        {
            Ok(()) => return Ok(()),
            Err(error) => {
                restore_lobby_membership(
                    session,
                    settings,
                    lobby_code,
                    preferences,
                    Some(expected_user_ids),
                    cancellation,
                    error.context("sending QueueForMatch before disconnect"),
                )
                .await?;
            }
        }
    }
}

async fn play_session(
    mut session: LiveSession,
    settings: SessionSettings,
    expected_user_ids: BTreeSet<u32>,
    group_game_id: watch::Sender<Option<u32>>,
    queue_started: Instant,
    cancellation: CancellationToken,
) -> PlayedSession {
    let result = play_session_inner(
        &mut session,
        &settings,
        &expected_user_ids,
        &group_game_id,
        queue_started,
        &cancellation,
    )
    .await;
    let snapshot_user_ids = match result {
        Ok(ids) => ids,
        Err(error) => {
            if cancellation.is_cancelled() {
                session
                    .record
                    .cancel(unix_time_ms(), "load-test cancellation interrupted session");
                BTreeSet::new()
            } else {
                if session.record.failure.is_none() {
                    let phase = if session.record.game_id.is_none() {
                        SessionPhase::Matchmaking
                    } else if session
                        .record
                        .lifecycle
                        .iter()
                        .any(|event| event.phase == SessionPhase::GameSnapshot)
                    {
                        SessionPhase::Playing
                    } else {
                        SessionPhase::GameJoin
                    };
                    session.fail(phase, format!("{error:#}"));
                }
                BTreeSet::new()
            }
        }
    };
    let _ = session.socket.close(None).await;
    PlayedSession {
        record: session.into_record(),
        snapshot_user_ids,
    }
}

async fn play_session_inner(
    session: &mut LiveSession,
    settings: &SessionSettings,
    expected_user_ids: &BTreeSet<u32>,
    group_game_id: &watch::Sender<Option<u32>>,
    queue_started: Instant,
    cancellation: &CancellationToken,
) -> Result<BTreeSet<u32>> {
    let game_id = wait_for_match_with_recovery(
        session,
        settings,
        expected_user_ids,
        group_game_id,
        settings.queue_timeout,
        cancellation,
    )
    .await?;
    session.record.game_id = Some(game_id);
    session.record.metrics.matchmaking_wait_ms = Some(elapsed_ms(queue_started));
    session.record.record_lifecycle(
        SessionLifecycleRecord::new(SessionPhase::GameJoin, unix_time_ms())
            .with_elapsed_ms(elapsed_ms(queue_started)),
    );
    let snapshot_started = Instant::now();
    let initial_snapshot = match session
        .send_cancellable(WSMessage::JoinGame(game_id), cancellation)
        .await
    {
        Ok(()) => {
            wait_for_game_snapshot(session, game_id, settings.queue_timeout, cancellation).await
        }
        Err(error) => Err(SnapshotWaitError::Retryable(
            error.context("sending initial JoinGame"),
        )),
    };
    let game_state = match initial_snapshot {
        Ok(game_state) => game_state,
        Err(SnapshotWaitError::Retryable(error)) => {
            recover_game_snapshot(session, settings, game_id, cancellation, error).await?
        }
        Err(error) => {
            return Err(error.into_anyhow()).context("waiting for initial game snapshot");
        }
    };

    session.record.metrics.game_join_ms = Some(elapsed_ms(snapshot_started));
    session.record.record_lifecycle(
        SessionLifecycleRecord::new(SessionPhase::GameSnapshot, unix_time_ms())
            .with_elapsed_ms(elapsed_ms(snapshot_started)),
    );
    let server_now_ms = Utc::now()
        .timestamp_millis()
        .saturating_add(session.clock_offset_ms);
    let start_delay = duration_until_server_time(game_state.start_ms, server_now_ms);
    let (active_game_window, timeboxed_untimed_game) = configured_game_window(
        game_state.properties.time_limit_ms,
        settings.untimed_play_duration,
    );
    let game_deadline = tokio::time::Instant::now() + start_delay + active_game_window;
    let (initial_user_ids, _) = snapshot_identity(game_id, session.user_id, &game_state)?;
    if matches!(&game_state.status, GameStatus::Complete { .. }) {
        session.record.metrics.game_duration_ms = Some(0);
        session.record.complete(unix_time_ms());
        return Ok(initial_user_ids);
    }

    let mut runtime = GameRuntime::from_snapshot(
        game_id,
        session.user_id,
        game_state,
        session.clock_offset_ms,
    )?;
    session.record.record_lifecycle(SessionLifecycleRecord::new(
        SessionPhase::Playing,
        unix_time_ms(),
    ));

    let game_started = Instant::now();
    let mut ping_interval = interval(PING_INTERVAL);
    ping_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut warmup = PlayingWarmupState::default();

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                session.record.cancel(unix_time_ms(), "load-test drain timeout reached");
                return Ok(runtime.snapshot_user_ids.clone());
            }
            _ = tokio::time::sleep_until(game_deadline) => {
                if timeboxed_untimed_game {
                    let previous_ping = runtime
                        .outstanding_ping
                        .as_ref()
                        .map(|(client_time, _)| *client_time);
                    leave_game_and_confirm(
                        session,
                        previous_ping,
                        settings.connect_timeout,
                        cancellation,
                    )
                    .await
                    .context("leaving a successfully timeboxed untimed game")?;
                    let finished_at = unix_time_ms();
                    session.record.metrics.game_duration_ms = Some(elapsed_ms(game_started));
                    session.record.diagnostics.insert(
                        "completion_kind".to_owned(),
                        "timeboxed".to_owned(),
                    );
                    session.record.record_lifecycle(
                        SessionLifecycleRecord::new(SessionPhase::Cleanup, finished_at)
                            .with_message("configured untimed play window completed"),
                    );
                    session.record.complete(finished_at);
                    return Ok(runtime.snapshot_user_ids.clone());
                }
                return Err(anyhow!("game {game_id} exceeded its authoritative time limit plus margin"));
            }
            _ = ping_interval.tick() => {
                let client_time = Utc::now().timestamp_millis();
                match session.send(WSMessage::Ping { client_time }).await {
                    Ok(()) => {
                        runtime.outstanding_ping = Some((client_time, Instant::now()));
                    }
                    Err(error) => {
                        let snapshot_complete = synchronize_game_runtime(
                            session,
                            &mut runtime,
                            settings,
                            game_id,
                            cancellation,
                            error.context("sending game-session ping"),
                            "write-error recovery snapshot synchronized",
                        )
                        .await?;
                        if snapshot_complete {
                            session.record.metrics.game_duration_ms = Some(elapsed_ms(game_started));
                            session.record.complete(unix_time_ms());
                            return Ok(runtime.snapshot_user_ids.clone());
                        }
                        warmup.finish_recovery();
                        ping_interval = interval(PING_INTERVAL);
                        ping_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                    }
                }
            }
            _ = runtime.ai_interval.tick(), if !warmup.commands_paused() => {
                match drive_ai(
                    session,
                    &mut runtime.engine,
                    runtime.snake_id,
                    settings.command_profile,
                    &mut runtime.last_decision_tick,
                    &mut runtime.pending_direction,
                ).await {
                    Ok(()) => {}
                    Err(DriveAiError::Engine(error)) => return Err(error),
                    Err(DriveAiError::Transport(error)) => {
                        let snapshot_complete = synchronize_game_runtime(
                            session,
                            &mut runtime,
                            settings,
                            game_id,
                            cancellation,
                            error,
                            "command-write recovery snapshot synchronized",
                        )
                        .await?;
                        if snapshot_complete {
                            session.record.metrics.game_duration_ms = Some(elapsed_ms(game_started));
                            session.record.complete(unix_time_ms());
                            return Ok(runtime.snapshot_user_ids.clone());
                        }
                        warmup.finish_recovery();
                        ping_interval = interval(PING_INTERVAL);
                        ping_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                    }
                }
            }
            _ = tokio::time::sleep_until(warmup.join_retry_at.unwrap_or(game_deadline)),
                if warmup.join_retry_at.is_some() =>
            {
                warmup.mark_retry_sent();
                if let Err(error) = session
                    .send_cancellable(WSMessage::JoinGame(game_id), cancellation)
                    .await
                {
                    let snapshot_complete = synchronize_game_runtime(
                        session,
                        &mut runtime,
                        settings,
                        game_id,
                        cancellation,
                        error.context("sending same-socket JoinGame after GameWarming"),
                        "GameWarming retry transport recovery snapshot synchronized",
                    )
                    .await?;
                    if snapshot_complete {
                        session.record.metrics.game_duration_ms = Some(elapsed_ms(game_started));
                        session.record.complete(unix_time_ms());
                        return Ok(runtime.snapshot_user_ids.clone());
                    }
                    warmup.finish_recovery();
                    ping_interval = interval(PING_INTERVAL);
                    ping_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                }
            }
            incoming = session.next_message() => {
                let message = match incoming {
                    Ok(message) => message,
                    Err(error) => {
                        let snapshot_complete = synchronize_game_runtime(
                            session,
                            &mut runtime,
                            settings,
                            game_id,
                            cancellation,
                            error,
                            "reconnect snapshot synchronized",
                        )
                        .await?;
                        if snapshot_complete {
                            session.record.metrics.game_duration_ms = Some(elapsed_ms(game_started));
                            session.record.complete(unix_time_ms());
                            return Ok(runtime.snapshot_user_ids.clone());
                        }
                        warmup.finish_recovery();
                        ping_interval = interval(PING_INTERVAL);
                        ping_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                        continue;
                    }
                };

                match message {
                    WSMessage::Pong { client_time, server_time } => {
                        if let Some((sent_client_time, sent_at)) = runtime.outstanding_ping.take()
                            && sent_client_time == client_time
                        {
                            let rtt = elapsed_ms(sent_at);
                            session.record.metrics.websocket_rtt_ms.push(rtt);
                            let midpoint = client_time.saturating_add((rtt / 2) as i64);
                            session.clock_offset_ms = server_time.saturating_sub(midpoint);
                        }
                    }
                    WSMessage::GameEvent(event) if event.game_id == game_id => {
                        session.record.metrics.game_events_received = session.record.metrics.game_events_received.saturating_add(1);
                        if runtime.suppress_covered_promotion_event(&event) {
                            continue;
                        }
                        match &event.event {
                            GameEvent::Snapshot { game_state } => {
                                let snapshot_complete = runtime.apply_snapshot(
                                    game_id,
                                    session.user_id,
                                    game_state.clone(),
                                    session.clock_offset_ms,
                                )?;
                                if snapshot_complete {
                                    session.record.metrics.game_duration_ms = Some(elapsed_ms(game_started));
                                    session.record.complete(unix_time_ms());
                                    return Ok(runtime.snapshot_user_ids.clone());
                                }
                                if warmup.observe_snapshot() {
                                    session
                                        .resend_pending_commands(game_id)
                                        .await
                                        .context(
                                            "resending unresolved commands after same-socket warm-up barrier",
                                        )?;
                                }
                            }
                            GameEvent::StatusUpdated { status: GameStatus::Complete { .. } } => {
                                runtime.engine.process_server_event(&event)?;
                                session.record.metrics.game_duration_ms = Some(elapsed_ms(game_started));
                                session.record.complete(unix_time_ms());
                                return Ok(runtime.snapshot_user_ids.clone());
                            }
                            _ => runtime.engine.process_server_event(&event)?,
                        }
                    }
                    WSMessage::CommandOutcomesComplete { game_id: completed }
                        if completed == game_id =>
                    {
                        if warmup.observe_outcome_barrier() {
                            session
                                .resend_pending_commands(game_id)
                                .await
                                .context(
                                    "resending unresolved commands after same-socket warm-up barrier",
                                )?;
                        }
                    }
                    WSMessage::GameLoadFailed { game_id: failed, reason } if failed == game_id => {
                        return Err(anyhow!("server could not load matched game {failed}: {reason}"));
                    }
                    WSMessage::GameWarming {
                        game_id: warming,
                        retry_after_ms,
                    } if warming == game_id => {
                        warmup.observe_warming(
                            tokio::time::Instant::now(),
                            game_deadline,
                            retry_after_ms,
                        );
                    }
                    WSMessage::AccessDenied { reason } => {
                        return Err(anyhow!("server denied game session: {reason}"));
                    }
                    WSMessage::Drain { task_boot_id, deadline_unix_ms } => {
                        let snapshot_complete = match perform_planned_handoff(
                            session,
                            &mut runtime,
                            settings,
                            game_id,
                            deadline_unix_ms,
                            cancellation,
                        )
                        .await
                        {
                            Ok(snapshot_complete) => snapshot_complete,
                            Err(error) => synchronize_game_runtime(
                                session,
                                &mut runtime,
                                settings,
                                game_id,
                                cancellation,
                                error.context(format!(
                                    "planned handoff from task {task_boot_id} failed; using crash recovery"
                                )),
                                "planned-handoff fallback snapshot synchronized",
                            )
                            .await?,
                        };
                        if snapshot_complete {
                            session.record.metrics.game_duration_ms = Some(elapsed_ms(game_started));
                            session.record.complete(unix_time_ms());
                            return Ok(runtime.snapshot_user_ids.clone());
                        }
                        warmup.finish_recovery();
                        ping_interval = interval(PING_INTERVAL);
                        ping_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                    }
                    _ => {}
                }
            }
        }
    }
}

fn configured_game_window(
    authoritative_time_limit_ms: Option<u32>,
    untimed_play_duration: Duration,
) -> (Duration, bool) {
    match authoritative_time_limit_ms {
        Some(value) => (
            Duration::from_millis(value as u64) + GAME_TIMEOUT_MARGIN,
            false,
        ),
        None => (untimed_play_duration, true),
    }
}

fn duration_until_server_time(target_ms: i64, server_now_ms: i64) -> Duration {
    Duration::from_millis(target_ms.saturating_sub(server_now_ms).max(0) as u64)
}

/// Send a leave followed by a uniquely tagged ping. A matching pong proves the
/// server processed both frames in order; the WebSocket protocol has no
/// dedicated LeaveGame acknowledgement.
async fn leave_game_and_confirm(
    session: &mut LiveSession,
    previous_ping: Option<i64>,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<()> {
    session
        .send_cancellable(WSMessage::LeaveGame, cancellation)
        .await?;
    send_tagged_ping_and_wait(session, previous_ping, timeout, cancellation).await
}

async fn send_tagged_ping_and_wait(
    session: &mut LiveSession,
    previous_ping: Option<i64>,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<()> {
    let client_time = match previous_ping {
        None => next_ping_client_time(session),
        Some(previous) => {
            let client_time = next_ping_client_time(session).max(previous.saturating_add(1));
            session.last_ping_client_time = Some(client_time);
            client_time
        }
    };
    let ping_started = Instant::now();
    session
        .send_cancellable(WSMessage::Ping { client_time }, cancellation)
        .await?;
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let message = tokio::select! {
            _ = cancellation.cancelled() => return Err(anyhow!("operation cancelled")),
            _ = tokio::time::sleep_until(deadline) => {
                return Err(anyhow!(
                    "server did not acknowledge the application ping within {timeout:?}"
                ));
            }
            incoming = session.next_message() => incoming?,
        };
        match message {
            WSMessage::Pong {
                client_time: echoed,
                server_time,
            } if echoed == client_time => {
                let rtt = elapsed_ms(ping_started);
                session.record.metrics.websocket_rtt_ms.push(rtt);
                let midpoint = client_time.saturating_add((rtt / 2) as i64);
                session.clock_offset_ms = server_time.saturating_sub(midpoint);
                return Ok(());
            }
            WSMessage::GameEvent(_) => {
                session.record.metrics.game_events_received = session
                    .record
                    .metrics
                    .game_events_received
                    .saturating_add(1);
            }
            WSMessage::AccessDenied { reason } => {
                return Err(anyhow!("server denied the session: {reason}"));
            }
            WSMessage::Drain { .. } => {
                return Err(anyhow!("server closed the connection before the pong"));
            }
            _ => {}
        }
    }
}

fn next_ping_client_time(session: &mut LiveSession) -> i64 {
    let now = Utc::now().timestamp_millis();
    let client_time = session
        .last_ping_client_time
        .map_or(now, |previous| now.max(previous.saturating_add(1)));
    session.last_ping_client_time = Some(client_time);
    client_time
}

async fn wait_for_game_snapshot(
    session: &mut LiveSession,
    game_id: u32,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> std::result::Result<GameState, SnapshotWaitError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let message = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(SnapshotWaitError::Fatal(anyhow!("operation cancelled")));
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(SnapshotWaitError::Fatal(anyhow!(
                    "waiting for game {game_id} snapshot timed out after {timeout:?}"
                )));
            }
            incoming = session.next_message() => {
                incoming.map_err(SnapshotWaitError::Retryable)?
            }
        };

        match message {
            WSMessage::GameEvent(event) if event.game_id == game_id => {
                session.record.metrics.game_events_received = session
                    .record
                    .metrics
                    .game_events_received
                    .saturating_add(1);
                if let GameEvent::Snapshot { game_state } = event.event {
                    return Ok(game_state);
                }
            }
            WSMessage::GameLoadFailed {
                game_id: failed,
                reason,
            } if failed == game_id => {
                return Err(SnapshotWaitError::Fatal(anyhow!(
                    "server could not load matched game {failed}: {reason}"
                )));
            }
            WSMessage::GameWarming {
                game_id: warming_game,
                retry_after_ms,
            } if warming_game == game_id => {
                tokio::time::sleep(Duration::from_millis(retry_after_ms.clamp(100, 2_000))).await;
                session
                    .send_cancellable(WSMessage::JoinGame(game_id), cancellation)
                    .await
                    .map_err(SnapshotWaitError::Retryable)?;
            }
            WSMessage::AccessDenied { reason } => {
                return Err(SnapshotWaitError::Fatal(anyhow!(
                    "server denied game session: {reason}"
                )));
            }
            WSMessage::Drain {
                task_boot_id,
                deadline_unix_ms,
            } => {
                return Err(SnapshotWaitError::Retryable(anyhow!(
                    "task {task_boot_id} requested WebSocket drain by {deadline_unix_ms} while loading game {game_id}"
                )));
            }
            _ => {}
        }
    }
}

async fn wait_for_command_outcome_barrier(
    session: &mut LiveSession,
    game_id: u32,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> std::result::Result<(), SnapshotWaitError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let message = tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(SnapshotWaitError::Fatal(anyhow!("operation cancelled")));
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(SnapshotWaitError::Retryable(anyhow!(
                    "waiting for game {game_id} command outcome barrier timed out after {timeout:?}"
                )));
            }
            incoming = session.next_message() => {
                incoming.map_err(SnapshotWaitError::Retryable)?
            }
        };
        match message {
            WSMessage::CommandOutcomesComplete { game_id: completed } if completed == game_id => {
                return Ok(());
            }
            WSMessage::GameEvent(event) if event.game_id == game_id => {
                // The gateway currently queues the barrier before subscribing
                // the socket to live deltas. Count defensively, but never use a
                // later event as an implicit outcome acknowledgement.
                session.record.metrics.game_events_received = session
                    .record
                    .metrics
                    .game_events_received
                    .saturating_add(1);
            }
            WSMessage::Drain { task_boot_id, .. } => {
                return Err(SnapshotWaitError::Retryable(anyhow!(
                    "task {task_boot_id} drained before the command outcome barrier"
                )));
            }
            WSMessage::GameLoadFailed {
                game_id: failed,
                reason,
            } if failed == game_id => {
                return Err(SnapshotWaitError::Fatal(anyhow!(
                    "server could not load game {failed} while replaying outcomes: {reason}"
                )));
            }
            WSMessage::AccessDenied { reason } => {
                return Err(SnapshotWaitError::Fatal(anyhow!(
                    "server denied outcome recovery: {reason}"
                )));
            }
            _ => {}
        }
    }
}

async fn wait_for_recovered_game_ready(
    session: &mut LiveSession,
    game_id: u32,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> std::result::Result<(GameState, u64), SnapshotWaitError> {
    let snapshot_started = Instant::now();
    let game_state = wait_for_game_snapshot(session, game_id, timeout, cancellation).await?;
    let snapshot_ms = elapsed_ms(snapshot_started);
    if !matches!(game_state.status, GameStatus::Complete { .. }) {
        if !session
            .server_capabilities
            .contains("command-outcome-barrier-v1")
        {
            return Err(SnapshotWaitError::Fatal(anyhow!(
                "server did not advertise the required command outcome barrier"
            )));
        }
        wait_for_command_outcome_barrier(session, game_id, timeout, cancellation).await?;
    }
    Ok((game_state, snapshot_ms))
}

async fn recover_game_snapshot(
    session: &mut LiveSession,
    settings: &SessionSettings,
    game_id: u32,
    cancellation: &CancellationToken,
    cause: anyhow::Error,
) -> Result<GameState> {
    session.record.metrics.disconnects = session.record.metrics.disconnects.saturating_add(1);
    let detected_at_unix_ms = unix_time_ms();
    let from_task_boot_id = session
        .current_task_boot_id
        .clone()
        .context("hard recovery began without an authenticated task identity")?;
    let from_socket_generation = session
        .current_socket_generation
        .context("hard recovery began without an authenticated socket generation")?;
    let pending_commands_at_detection =
        u64::try_from(session.pending_commands.len()).unwrap_or(u64::MAX);
    let recovery_started = Instant::now();
    let mut last_error = cause;

    loop {
        if cancellation.is_cancelled() {
            return Err(anyhow!(
                "operation cancelled while reconnecting to game {game_id}"
            ));
        }
        if session.reconnects >= MAX_RECONNECTS {
            return Err(anyhow!(
                "websocket recovery budget exhausted for game {game_id}: {last_error:#}"
            ));
        }

        session.reconnects += 1;
        session.record.metrics.reconnects = session.record.metrics.reconnects.saturating_add(1);
        session
            .record
            .diagnostics
            .insert("reconnects".to_owned(), session.reconnects.to_string());

        tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(anyhow!("operation cancelled while reconnecting to game {game_id}"));
            }
            _ = tokio::time::sleep(RECONNECT_DELAY) => {}
        }

        if let Err(error) = session
            .reconnect(settings.connect_timeout, cancellation)
            .await
        {
            last_error = error.context("reconnecting and re-authenticating WebSocket");
            continue;
        }
        if let Err(error) = session
            .send_cancellable(WSMessage::JoinGame(game_id), cancellation)
            .await
        {
            last_error = error.context("sending JoinGame after reconnect");
            continue;
        }

        match wait_for_recovered_game_ready(
            session,
            game_id,
            settings.connect_timeout,
            cancellation,
        )
        .await
        {
            Ok((game_state, snapshot_rejoin_ms)) => {
                session
                    .record
                    .metrics
                    .rejoin_snapshot_ms
                    .push(snapshot_rejoin_ms);
                let duration_ms = elapsed_ms(recovery_started);
                session
                    .record
                    .metrics
                    .reconnect_duration_ms
                    .push(duration_ms);
                session
                    .record
                    .metrics
                    .usable_session_gap_ms
                    .push(duration_ms);
                let to_task_boot_id = session
                    .current_task_boot_id
                    .clone()
                    .context("hard recovery completed without an authenticated task identity")?;
                let to_socket_generation = session.current_socket_generation.context(
                    "hard recovery completed without an authenticated socket generation",
                )?;
                session
                    .record
                    .hard_recoveries
                    .push(HardRecoveryObservation {
                        detected_at_unix_ms,
                        ready_at_unix_ms: unix_time_ms(),
                        from_task_boot_id,
                        to_task_boot_id,
                        from_socket_generation,
                        to_socket_generation,
                        game_id,
                        fresh_snapshot_received: true,
                        pending_commands_at_detection,
                        pending_commands_after_outcome_barrier: u64::try_from(
                            session.pending_commands.len(),
                        )
                        .unwrap_or(u64::MAX),
                    });
                return Ok(game_state);
            }
            Err(SnapshotWaitError::Retryable(error)) => {
                session.record.metrics.disconnects =
                    session.record.metrics.disconnects.saturating_add(1);
                last_error = error;
            }
            Err(SnapshotWaitError::Fatal(error)) => return Err(error),
        }
    }
}

struct ReadyPlannedCandidate {
    socket: Socket,
    backend: Option<String>,
    sticky_cookie: Option<String>,
    capabilities: BTreeSet<String>,
    task_boot_id: String,
    socket_generation: u64,
    snapshot: GameEventMessage,
    buffered_events: Vec<GameEventMessage>,
    auth_ms: u64,
    lobby_rejoin_ms: Option<u64>,
    snapshot_rejoin_ms: u64,
    outcome_barrier_observed: bool,
    usable_gap_ms: u64,
    active_overlap_ms: u64,
}

enum PlannedHandoffAttemptError {
    Candidate(anyhow::Error),
    Active(anyhow::Error),
    GameComplete,
    Fatal(anyhow::Error),
}

enum ActiveHandoffObservation {
    Continue,
    Pong(i64),
    GameComplete,
}

async fn send_candidate_message(
    session: &mut LiveSession,
    socket: &mut Socket,
    message: WSMessage,
    cancellation: &CancellationToken,
) -> Result<()> {
    let kind = message_kind(&message);
    tokio::select! {
        _ = cancellation.cancelled() => Err(anyhow!("operation cancelled")),
        result = send_socket_message(socket, &message) => result,
    }?;
    session.observe_sent(kind);
    Ok(())
}

fn process_active_handoff_message(
    session: &mut LiveSession,
    runtime: &mut GameRuntime,
    game_id: u32,
    active_watermark: &mut u64,
    message: WSMessage,
) -> Result<ActiveHandoffObservation> {
    match message {
        WSMessage::Pong {
            client_time,
            server_time,
        } => {
            if runtime
                .outstanding_ping
                .as_ref()
                .is_some_and(|(sent_client_time, _)| *sent_client_time == client_time)
                && let Some((_, sent_at)) = runtime.outstanding_ping.take()
            {
                let rtt = elapsed_ms(sent_at);
                session.record.metrics.websocket_rtt_ms.push(rtt);
                let midpoint = client_time.saturating_add((rtt / 2) as i64);
                session.clock_offset_ms = server_time.saturating_sub(midpoint);
            }
            return Ok(ActiveHandoffObservation::Pong(client_time));
        }
        WSMessage::GameEvent(event) if event.game_id == game_id => {
            if matches!(&event.event, GameEvent::Snapshot { .. }) && event.stream_seq > 0 {
                // A crash-recovery snapshot starts a new transport stream. The
                // planned-handoff marker keeps cooperative transfers monotonic,
                // but the load client must still accept a later crash re-anchor.
                *active_watermark = event.stream_seq;
            } else {
                *active_watermark = (*active_watermark).max(event.stream_seq);
            }
            session.record.metrics.game_events_received = session
                .record
                .metrics
                .game_events_received
                .saturating_add(1);
            match &event.event {
                GameEvent::Snapshot { game_state } => {
                    let complete = runtime.apply_snapshot(
                        game_id,
                        session.user_id,
                        game_state.clone(),
                        session.clock_offset_ms,
                    )?;
                    // `apply_snapshot` rebuilds the engine. Feed the envelope
                    // through once so its transport watermark is retained for
                    // duplicate suppression across atomic promotion.
                    runtime.engine.process_server_event(&event)?;
                    if complete {
                        return Ok(ActiveHandoffObservation::GameComplete);
                    }
                }
                GameEvent::StatusUpdated {
                    status: GameStatus::Complete { .. },
                } => {
                    runtime.engine.process_server_event(&event)?;
                    return Ok(ActiveHandoffObservation::GameComplete);
                }
                _ => runtime.engine.process_server_event(&event)?,
            }
        }
        WSMessage::GameLoadFailed {
            game_id: failed,
            reason,
        } if failed == game_id => {
            return Err(anyhow!(
                "active gateway could no longer load game {failed}: {reason}"
            ));
        }
        WSMessage::AccessDenied { reason } => {
            return Err(anyhow!(
                "active gateway denied the session during planned handoff: {reason}"
            ));
        }
        // Replayed drain announcements are expected while the candidate is
        // being prepared and do not invalidate the still-usable old stream.
        WSMessage::Drain { .. } => {}
        _ => {}
    }
    Ok(ActiveHandoffObservation::Continue)
}

async fn prepare_planned_candidate(
    session: &mut LiveSession,
    runtime: &mut GameRuntime,
    settings: &SessionSettings,
    game_id: u32,
    active_watermark: &mut u64,
    deadline: tokio::time::Instant,
    cancellation: &CancellationToken,
) -> std::result::Result<ReadyPlannedCandidate, PlannedHandoffAttemptError> {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return Err(PlannedHandoffAttemptError::Candidate(anyhow!(
            "planned handoff deadline reached before candidate connection"
        )));
    }
    let connect_timeout = settings.connect_timeout.min(remaining);
    let websocket_url = session.websocket_url.clone();
    let origin = session.origin.clone();
    let backend_hints = session.backend_hints.clone();
    let sticky_cookie = session.sticky_cookie.clone();
    let connect = connect_socket(
        &websocket_url,
        &origin,
        connect_timeout,
        &backend_hints,
        sticky_cookie.as_deref(),
    );
    tokio::pin!(connect);
    let connect_result = loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(PlannedHandoffAttemptError::Fatal(anyhow!("operation cancelled")));
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(PlannedHandoffAttemptError::Candidate(anyhow!(
                    "planned handoff deadline reached during candidate connection"
                )));
            }
            result = &mut connect => break result,
            active = session.next_message() => {
                let message = active.map_err(PlannedHandoffAttemptError::Active)?;
                match process_active_handoff_message(
                    session,
                    runtime,
                    game_id,
                    active_watermark,
                    message,
                )
                .map_err(PlannedHandoffAttemptError::Active)?
                {
                    ActiveHandoffObservation::GameComplete => {
                        return Err(PlannedHandoffAttemptError::GameComplete);
                    }
                    ActiveHandoffObservation::Continue
                    | ActiveHandoffObservation::Pong(_) => {}
                }
            }
            _ = runtime.ai_interval.tick() => {
                match drive_ai(
                    session,
                    &mut runtime.engine,
                    runtime.snake_id,
                    settings.command_profile,
                    &mut runtime.last_decision_tick,
                    &mut runtime.pending_direction,
                ).await {
                    Ok(()) => {}
                    Err(DriveAiError::Engine(error)) => {
                        return Err(PlannedHandoffAttemptError::Fatal(error));
                    }
                    Err(DriveAiError::Transport(error)) => {
                        return Err(PlannedHandoffAttemptError::Active(error));
                    }
                }
            }
        }
    };
    let (mut socket, backend, sticky_cookie) =
        connect_result.map_err(PlannedHandoffAttemptError::Candidate)?;

    let auth_started = Instant::now();
    let token = session.token.clone();
    send_candidate_message(session, &mut socket, WSMessage::Token(token), cancellation)
        .await
        .map_err(PlannedHandoffAttemptError::Candidate)?;

    let lobby_code = session.record.lobby_code.clone();
    let preferences = LobbyPreferences {
        selected_modes: vec![settings.selected_mode.clone()],
        competitive: settings.competitive,
    };
    let mut authenticated = false;
    let mut capabilities = BTreeSet::new();
    let mut task_boot_id = String::new();
    let mut socket_generation = 0;
    let mut auth_ms = 0;
    let mut restore_started: Option<Instant> = None;
    let mut snapshot_started: Option<Instant> = None;
    let mut lobby_ready = lobby_code.is_none();
    let mut lobby_rejoin_ms = None;
    let mut snapshot_rejoin_ms = None;
    let mut snapshot: Option<GameEventMessage> = None;
    let mut snapshot_terminal = false;
    let mut candidate_watermark: Option<u64> = None;
    let mut buffered_events = Vec::new();
    let mut outcomes_ready = false;
    let mut outcome_barrier_observed = false;
    let mut candidate_ready_at = None;
    let mut continuity_probe_client_time = None;
    let mut continuity_confirmed_at = None;
    let mut promotion_frontier = None;
    let mut game_join_retry_at = None;

    loop {
        let required_watermark = promotion_frontier.unwrap_or(*active_watermark);
        let candidate_is_ready = authenticated
            && lobby_ready
            && (snapshot_terminal || outcomes_ready)
            && (snapshot_terminal || candidate_watermark.unwrap_or(0) >= required_watermark)
            && snapshot.is_some();
        if candidate_is_ready {
            let ready_at = *candidate_ready_at.get_or_insert_with(Instant::now);
            if continuity_probe_client_time.is_none() {
                // Do not infer continuity merely because neither socket has
                // returned an error. Once the candidate is game-ready, prove
                // the old gateway is still processing ordered application
                // traffic. Every old-socket frame ordered before the matching
                // pong becomes part of the fixed promotion frontier.
                let client_time = runtime.outstanding_ping.as_ref().map_or_else(
                    || Utc::now().timestamp_millis(),
                    |(previous, _)| {
                        Utc::now()
                            .timestamp_millis()
                            .max(previous.saturating_add(1))
                    },
                );
                session
                    .send_cancellable(WSMessage::Ping { client_time }, cancellation)
                    .await
                    .map_err(PlannedHandoffAttemptError::Active)?;
                runtime.outstanding_ping = Some((client_time, Instant::now()));
                continuity_probe_client_time = Some(client_time);
            }

            if let Some(old_usable_through) = continuity_confirmed_at {
                let ready_snapshot = snapshot
                    .take()
                    .expect("candidate readiness requires an authoritative snapshot");
                return Ok(ReadyPlannedCandidate {
                    socket,
                    backend,
                    sticky_cookie,
                    capabilities,
                    task_boot_id,
                    socket_generation,
                    snapshot: ready_snapshot,
                    buffered_events,
                    auth_ms,
                    lobby_rejoin_ms,
                    snapshot_rejoin_ms: snapshot_rejoin_ms.unwrap_or_default(),
                    outcome_barrier_observed,
                    // The old application stream remains visible and writable
                    // through the atomic promotion, including while the
                    // candidate catches the fixed post-pong frontier.
                    usable_gap_ms: 0,
                    active_overlap_ms: duration_between_ms(old_usable_through, ready_at),
                });
            }
        }

        tokio::select! {
            _ = cancellation.cancelled() => {
                return Err(PlannedHandoffAttemptError::Fatal(anyhow!("operation cancelled")));
            }
            _ = tokio::time::sleep_until(deadline) => {
                return Err(PlannedHandoffAttemptError::Candidate(anyhow!(
                    "planned handoff candidate did not become ready before its deadline"
                )));
            }
            active = session.next_message() => {
                let message = active.map_err(PlannedHandoffAttemptError::Active)?;
                match process_active_handoff_message(
                    session,
                    runtime,
                    game_id,
                    active_watermark,
                    message,
                )
                .map_err(PlannedHandoffAttemptError::Active)?
                {
                    ActiveHandoffObservation::GameComplete => {
                        return Err(PlannedHandoffAttemptError::GameComplete);
                    }
                    ActiveHandoffObservation::Pong(client_time)
                        if continuity_probe_client_time == Some(client_time) =>
                    {
                        continuity_confirmed_at = Some(Instant::now());
                        // WebSocket ordering makes this the exact old-stream
                        // watermark observed through the continuity pong. Later
                        // old-stream frames remain authoritative and visible,
                        // but do not turn candidate catch-up into a moving target.
                        promotion_frontier = Some(*active_watermark);
                    }
                    ActiveHandoffObservation::Continue
                    | ActiveHandoffObservation::Pong(_) => {}
                }
            }
            _ = runtime.ai_interval.tick() => {
                match drive_ai(
                    session,
                    &mut runtime.engine,
                    runtime.snake_id,
                    settings.command_profile,
                    &mut runtime.last_decision_tick,
                    &mut runtime.pending_direction,
                ).await {
                    Ok(()) => {}
                    Err(DriveAiError::Engine(error)) => {
                        return Err(PlannedHandoffAttemptError::Fatal(error));
                    }
                    Err(DriveAiError::Transport(error)) => {
                        return Err(PlannedHandoffAttemptError::Active(error));
                    }
                }
            }
            _ = tokio::time::sleep_until(game_join_retry_at.unwrap_or(deadline)), if game_join_retry_at.is_some() => {
                game_join_retry_at = None;
                tokio::time::timeout_at(
                    deadline,
                    send_candidate_message(
                        session,
                        &mut socket,
                        WSMessage::JoinGame(game_id),
                        cancellation,
                    ),
                )
                .await
                .map_err(|_| PlannedHandoffAttemptError::Candidate(anyhow!(
                    "planned handoff deadline reached during GameWarming retry",
                )))?
                .map_err(PlannedHandoffAttemptError::Candidate)?;
            }
            candidate = next_socket_message(&mut socket) => {
                let message = candidate.map_err(PlannedHandoffAttemptError::Candidate)?;
                session.observe_received(&message);
                match message {
                    WSMessage::Authenticated {
                        task_boot_id: candidate_task_boot_id,
                        protocol_version: _,
                        capabilities: candidate_capabilities,
                        socket_generation: candidate_socket_generation,
                    } => {
                        if authenticated {
                            continue;
                        }
                        capabilities = candidate_capabilities.into_iter().collect();
                        validate_required_server_capabilities(&capabilities)
                            .context("planned handoff candidate is incompatible")
                            .map_err(PlannedHandoffAttemptError::Candidate)?;
                        outcomes_ready = false;
                        authenticated = true;
                        task_boot_id = candidate_task_boot_id;
                        socket_generation = candidate_socket_generation;
                        auth_ms = elapsed_ms(auth_started);
                        let restore_now = Instant::now();
                        restore_started = Some(restore_now);
                        if let Some(code) = &lobby_code {
                            send_candidate_message(
                                session,
                                &mut socket,
                                WSMessage::JoinLobby {
                                    lobby_code: code.clone(),
                                    preferences: Some(preferences.clone()),
                                },
                                cancellation,
                            )
                            .await
                            .map_err(PlannedHandoffAttemptError::Candidate)?;
                        }
                        snapshot_started = Some(Instant::now());
                        send_candidate_message(
                            session,
                            &mut socket,
                            WSMessage::JoinGame(game_id),
                            cancellation,
                        )
                        .await
                        .map_err(PlannedHandoffAttemptError::Candidate)?;
                    }
                    WSMessage::JoinedLobby { lobby_code: joined }
                        if lobby_code.as_deref() == Some(joined.as_str()) =>
                    {
                        lobby_ready = true;
                        lobby_rejoin_ms = restore_started.map(elapsed_ms);
                    }
                    WSMessage::LobbyUpdate { lobby_code: updated, .. }
                        if lobby_code.as_deref() == Some(updated.as_str()) =>
                    {
                        lobby_ready = true;
                        lobby_rejoin_ms.get_or_insert_with(|| {
                            restore_started.map(elapsed_ms).unwrap_or_default()
                        });
                    }
                    WSMessage::GameEvent(event) if event.game_id == game_id => {
                        session.record.metrics.game_events_received = session
                            .record
                            .metrics
                            .game_events_received
                            .saturating_add(1);
                        if let GameEvent::Snapshot { game_state } = &event.event {
                            game_join_retry_at = None;
                            snapshot_terminal = matches!(game_state.status, GameStatus::Complete { .. });
                            snapshot = Some(event.clone());
                            candidate_watermark = Some(event.stream_seq);
                            buffered_events.clear();
                            snapshot_rejoin_ms = snapshot_started.map(elapsed_ms);
                            if !snapshot_terminal {
                                // The server follows every live snapshot with
                                // outcomes from the same recovery envelope. A
                                // takeover snapshot invalidates an older barrier.
                                outcomes_ready = false;
                            }
                        } else if let Some(watermark) = candidate_watermark {
                            if event.stream_seq == 0 || event.stream_seq == watermark.saturating_add(1) {
                                if event.stream_seq > 0 {
                                    candidate_watermark = Some(event.stream_seq);
                                }
                                buffered_events.push(event);
                            } else if event.stream_seq > watermark {
                                snapshot = None;
                                candidate_watermark = None;
                                buffered_events.clear();
                                outcomes_ready = false;
                                send_candidate_message(
                                    session,
                                    &mut socket,
                                    WSMessage::RequestResync { game_id },
                                    cancellation,
                                )
                                .await
                                .map_err(PlannedHandoffAttemptError::Candidate)?;
                            }
                        }
                    }
                    WSMessage::CommandOutcomesComplete { game_id: completed }
                        if completed == game_id =>
                    {
                        outcomes_ready = true;
                        outcome_barrier_observed = true;
                    }
                    WSMessage::GameWarming {
                        game_id: warming,
                        retry_after_ms,
                    } if warming == game_id => {
                        game_join_retry_at = bounded_game_join_retry_at(
                            tokio::time::Instant::now(),
                            deadline,
                            retry_after_ms,
                        );
                    }
                    WSMessage::Drain { .. } | WSMessage::GameLoadFailed { .. } => {
                        return Err(PlannedHandoffAttemptError::Candidate(anyhow!(
                            "candidate could not restore the game before promotion"
                        )));
                    }
                    WSMessage::AccessDenied { reason } => {
                        return Err(PlannedHandoffAttemptError::Candidate(anyhow!(
                            "candidate denied context restoration: {reason}"
                        )));
                    }
                    WSMessage::LobbyRegionMismatch { target_region, .. } => {
                        return Err(PlannedHandoffAttemptError::Fatal(anyhow!(
                            "candidate lobby moved to region {target_region}"
                        )));
                    }
                    _ => {}
                }
            }
        }
    }
}

fn bounded_game_join_retry_at(
    now: tokio::time::Instant,
    deadline: tokio::time::Instant,
    retry_after_ms: u64,
) -> Option<tokio::time::Instant> {
    let retry_at = now + Duration::from_millis(retry_after_ms.clamp(100, 2_000));
    (retry_at < deadline).then_some(retry_at)
}

fn record_planned_handoff_failure(
    session: &mut LiveSession,
    started: Instant,
    reason: &anyhow::Error,
) {
    session.record.metrics.planned_handoff_failures = session
        .record
        .metrics
        .planned_handoff_failures
        .saturating_add(1);
    session
        .record
        .metrics
        .planned_handoff_duration_ms
        .push(elapsed_ms(started));
    session.record.diagnostics.insert(
        "last_planned_handoff_failure".to_owned(),
        format!("{reason:#}"),
    );
}

fn record_planned_handoff_commands(session: &mut LiveSession, commands_at_start: u64) {
    session.record.metrics.planned_handoff_commands_sent = session
        .record
        .metrics
        .planned_handoff_commands_sent
        .saturating_add(
            session
                .record
                .metrics
                .commands_sent
                .saturating_sub(commands_at_start),
        );
}

fn record_planned_terminal_completion(
    session: &mut LiveSession,
    started: Instant,
    commands_at_start: u64,
) {
    session.record.metrics.planned_handoff_successes = session
        .record
        .metrics
        .planned_handoff_successes
        .saturating_add(1);
    session.record.metrics.planned_handoff_terminal_completions = session
        .record
        .metrics
        .planned_handoff_terminal_completions
        .saturating_add(1);
    session
        .record
        .metrics
        .planned_handoff_duration_ms
        .push(elapsed_ms(started));
    // Completion arrived over the still-active old socket. No gateway
    // promotion was needed, so there is no handoff-gap sample; the terminal
    // event itself is the continuity proof.
    session.record.metrics.planned_handoff_continuity_proofs = session
        .record
        .metrics
        .planned_handoff_continuity_proofs
        .saturating_add(1);
    record_planned_handoff_commands(session, commands_at_start);
}

async fn perform_planned_handoff(
    session: &mut LiveSession,
    runtime: &mut GameRuntime,
    settings: &SessionSettings,
    game_id: u32,
    deadline_unix_ms: i64,
    cancellation: &CancellationToken,
) -> Result<bool> {
    let started = Instant::now();
    let commands_at_start = session.record.metrics.commands_sent;
    session.record.metrics.planned_handoff_attempts = session
        .record
        .metrics
        .planned_handoff_attempts
        .saturating_add(1);
    session.record.record_lifecycle(
        SessionLifecycleRecord::new(SessionPhase::WebSocketConnect, unix_time_ms())
            .with_message("planned make-before-break handoff started"),
    );
    let remaining_ms = deadline_unix_ms.saturating_sub(Utc::now().timestamp_millis());
    if remaining_ms <= 0 {
        let error = anyhow!("planned handoff deadline was already expired");
        record_planned_handoff_failure(session, started, &error);
        return Err(error);
    }
    let deadline = tokio::time::Instant::now() + Duration::from_millis(remaining_ms as u64);
    let mut candidate_failures = 0_u64;
    let mut active_watermark = runtime.engine.sync_status().last_stream_seq;

    loop {
        match prepare_planned_candidate(
            session,
            runtime,
            settings,
            game_id,
            &mut active_watermark,
            deadline,
            cancellation,
        )
        .await
        {
            Ok(mut candidate) => {
                let snapshot_event = candidate.snapshot;
                let GameEvent::Snapshot { game_state } = &snapshot_event.event else {
                    unreachable!("planned candidate stores only authoritative snapshots");
                };
                runtime.promotion_suppression_floor = Some(active_watermark);
                let snapshot_complete =
                    if !runtime.suppress_covered_promotion_event(&snapshot_event) {
                        let complete = runtime.apply_snapshot(
                            game_id,
                            session.user_id,
                            game_state.clone(),
                            session.clock_offset_ms,
                        )?;
                        runtime.engine.process_server_event(&snapshot_event)?;
                        complete
                    } else {
                        // The old application stream stayed authoritative after
                        // Pong and may be ahead of this buffered candidate
                        // snapshot. Preserve it and discard the covered prefix,
                        // including an unsequenced nonterminal recovery bridge.
                        false
                    };
                if !snapshot_complete {
                    for event in candidate.buffered_events {
                        if !runtime.suppress_covered_promotion_event(&event) {
                            runtime.engine.process_server_event(&event)?;
                        }
                    }
                }
                let handoff_complete = snapshot_complete
                    || matches!(
                        runtime.engine.committed_state().status,
                        GameStatus::Complete { .. }
                    );

                let mut old_socket = std::mem::replace(&mut session.socket, candidate.socket);
                if candidate.sticky_cookie.is_some() {
                    session.sticky_cookie = candidate.sticky_cookie.take();
                }
                session.server_capabilities = candidate.capabilities;
                session.current_task_boot_id = Some(candidate.task_boot_id.clone());
                session.current_socket_generation = Some(candidate.socket_generation);
                let ordinal = session.record.metrics.planned_handoff_successes + 1;
                session.record.diagnostics.insert(
                    format!("planned_handoff_task_boot_id_{ordinal}"),
                    candidate.task_boot_id,
                );
                session.record.diagnostics.insert(
                    format!("planned_handoff_socket_generation_{ordinal}"),
                    candidate.socket_generation.to_string(),
                );
                if let Some(backend) = candidate.backend {
                    session
                        .record
                        .diagnostics
                        .insert(format!("planned_handoff_backend_{ordinal}"), backend);
                }
                session
                    .record
                    .metrics
                    .websocket_auth_ms
                    .push(candidate.auth_ms);
                if let Some(value) = candidate.lobby_rejoin_ms {
                    session.record.metrics.rejoin_lobby_ms.push(value);
                }
                session
                    .record
                    .metrics
                    .rejoin_snapshot_ms
                    .push(candidate.snapshot_rejoin_ms);
                session.record.metrics.planned_handoff_successes = session
                    .record
                    .metrics
                    .planned_handoff_successes
                    .saturating_add(1);
                if handoff_complete {
                    session.record.metrics.planned_handoff_terminal_completions = session
                        .record
                        .metrics
                        .planned_handoff_terminal_completions
                        .saturating_add(1);
                } else if candidate.outcome_barrier_observed {
                    session.record.metrics.planned_handoff_outcome_barriers = session
                        .record
                        .metrics
                        .planned_handoff_outcome_barriers
                        .saturating_add(1);
                }
                session
                    .record
                    .metrics
                    .planned_handoff_duration_ms
                    .push(elapsed_ms(started));
                session
                    .record
                    .metrics
                    .usable_session_gap_ms
                    .push(candidate.usable_gap_ms);
                session
                    .record
                    .metrics
                    .planned_handoff_active_overlap_ms
                    .push(candidate.active_overlap_ms);
                session.record.metrics.planned_handoff_continuity_proofs = session
                    .record
                    .metrics
                    .planned_handoff_continuity_proofs
                    .saturating_add(1);
                record_planned_handoff_commands(session, commands_at_start);
                session.record.diagnostics.insert(
                    format!("planned_handoff_usable_gap_ms_{ordinal}"),
                    candidate.usable_gap_ms.to_string(),
                );
                session.record.diagnostics.insert(
                    format!("planned_handoff_active_overlap_ms_{ordinal}"),
                    candidate.active_overlap_ms.to_string(),
                );
                session.record.record_lifecycle(
                    SessionLifecycleRecord::new(SessionPhase::GameSnapshot, unix_time_ms())
                        .with_message(if handoff_complete {
                            "planned handoff observed terminal state"
                        } else {
                            "planned handoff promoted a fully synchronized candidate"
                        }),
                );
                let _ = old_socket.close(None).await;
                if !handoff_complete {
                    session
                        .resend_pending_commands(game_id)
                        .await
                        .context("resending unresolved commands after planned outcome barrier")?;
                }
                return Ok(handoff_complete);
            }
            Err(PlannedHandoffAttemptError::GameComplete) => {
                record_planned_terminal_completion(session, started, commands_at_start);
                return Ok(true);
            }
            Err(PlannedHandoffAttemptError::Candidate(error)) => {
                candidate_failures = candidate_failures.saturating_add(1);
                session.record.diagnostics.insert(
                    "planned_handoff_candidate_failures".to_owned(),
                    candidate_failures.to_string(),
                );
                session.record.diagnostics.insert(
                    "last_planned_candidate_failure".to_owned(),
                    format!("{error:#}"),
                );
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    record_planned_handoff_failure(session, started, &error);
                    return Err(error);
                }
                let retry_until =
                    tokio::time::Instant::now() + PLANNED_HANDOFF_RETRY_DELAY.min(remaining);
                loop {
                    tokio::select! {
                        _ = cancellation.cancelled() => {
                            let error = anyhow!("operation cancelled during planned handoff retry");
                            record_planned_handoff_failure(session, started, &error);
                            return Err(error);
                        }
                        _ = tokio::time::sleep_until(retry_until) => break,
                        active = session.next_message() => {
                            let message = match active {
                                Ok(message) => message,
                                Err(error) => {
                                    record_planned_handoff_failure(session, started, &error);
                                    return Err(error);
                                }
                            };
                            match process_active_handoff_message(
                                session,
                                runtime,
                                game_id,
                                &mut active_watermark,
                                message,
                            ) {
                                Ok(ActiveHandoffObservation::GameComplete) => {
                                    record_planned_terminal_completion(
                                        session,
                                        started,
                                        commands_at_start,
                                    );
                                    return Ok(true);
                                }
                                Ok(ActiveHandoffObservation::Continue)
                                | Ok(ActiveHandoffObservation::Pong(_)) => {}
                                Err(error) => {
                                    record_planned_handoff_failure(session, started, &error);
                                    return Err(error);
                                }
                            }
                        }
                        _ = runtime.ai_interval.tick() => {
                            match drive_ai(
                                session,
                                &mut runtime.engine,
                                runtime.snake_id,
                                settings.command_profile,
                                &mut runtime.last_decision_tick,
                                &mut runtime.pending_direction,
                            ).await {
                                Ok(()) => {}
                                Err(DriveAiError::Engine(error))
                                | Err(DriveAiError::Transport(error)) => {
                                    record_planned_handoff_failure(session, started, &error);
                                    return Err(error);
                                }
                            }
                        }
                    }
                }
            }
            Err(PlannedHandoffAttemptError::Active(error))
            | Err(PlannedHandoffAttemptError::Fatal(error)) => {
                record_planned_handoff_failure(session, started, &error);
                return Err(error);
            }
        }
    }
}

async fn synchronize_game_runtime(
    session: &mut LiveSession,
    runtime: &mut GameRuntime,
    settings: &SessionSettings,
    game_id: u32,
    cancellation: &CancellationToken,
    cause: anyhow::Error,
    lifecycle_message: &'static str,
) -> Result<bool> {
    let game_state = recover_game_snapshot(session, settings, game_id, cancellation, cause).await?;
    let snapshot_complete = runtime.apply_snapshot(
        game_id,
        session.user_id,
        game_state,
        session.clock_offset_ms,
    )?;
    if !snapshot_complete {
        session
            .resend_pending_commands(game_id)
            .await
            .context("resending unresolved commands after recovery snapshot")?;
    }
    session.record.record_lifecycle(
        SessionLifecycleRecord::new(SessionPhase::GameSnapshot, unix_time_ms())
            .with_message(lifecycle_message),
    );
    Ok(snapshot_complete)
}

fn snapshot_identity(
    game_id: u32,
    user_id: u32,
    game_state: &GameState,
) -> Result<(BTreeSet<u32>, u32)> {
    let snapshot_user_ids = game_state.players.keys().copied().collect();
    let snake_id = game_state
        .players
        .get(&user_id)
        .map(|player| player.snake_id)
        .ok_or_else(|| anyhow!("snapshot for game {game_id} omitted user {user_id}"))?;
    Ok((snapshot_user_ids, snake_id))
}

fn ai_interval_for(game_state: &GameState, clock_offset_ms: i64) -> Interval {
    let tick_ms = game_state.properties.tick_duration_ms.max(25) as u64;
    let ai_period = Duration::from_millis(tick_ms.min(100));
    let now_ms = Utc::now()
        .timestamp_millis()
        .saturating_add(clock_offset_ms);
    let delay_to_next_tick = if now_ms < game_state.start_ms {
        game_state.start_ms.saturating_sub(now_ms) as u64 + tick_ms
    } else {
        let elapsed_since_start = now_ms.saturating_sub(game_state.start_ms) as u64;
        tick_ms.saturating_sub(elapsed_since_start % tick_ms)
    };
    let mut ai_interval = interval_at(
        tokio::time::Instant::now() + Duration::from_millis(delay_to_next_tick),
        ai_period,
    );
    ai_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ai_interval
}

async fn wait_for_match_with_recovery(
    session: &mut LiveSession,
    settings: &SessionSettings,
    expected_user_ids: &BTreeSet<u32>,
    group_game_id: &watch::Sender<Option<u32>>,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> Result<u32> {
    let lobby_code = session
        .record
        .lobby_code
        .clone()
        .ok_or_else(|| anyhow!("session lost its lobby code before matchmaking"))?;
    let preferences = LobbyPreferences {
        selected_modes: vec![settings.selected_mode.clone()],
        competitive: settings.competitive,
    };
    let deadline = tokio::time::Instant::now() + timeout;
    let mut group_game_updates = group_game_id.subscribe();

    loop {
        if let Some(game_id) = *group_game_id.borrow() {
            return Ok(game_id);
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(anyhow!(
                "waiting for matchmaking notification timed out after {timeout:?}"
            ));
        }
        let wait_result = tokio::select! {
            result = session.wait_for_pre_game(
                remaining,
                cancellation,
                |message| match message {
                    WSMessage::JoinGame(game_id) => Some(*game_id),
                    WSMessage::MatchFound { game_id } => Some(*game_id),
                    _ => None,
                },
            ) => result,
            changed = group_game_updates.changed() => {
                if changed.is_err() {
                    return Err(anyhow!(
                        "deterministic match-group assignment channel closed"
                    ));
                }
                if let Some(game_id) = *group_game_updates.borrow_and_update() {
                    return Ok(game_id);
                }
                continue;
            }
        };
        match wait_result {
            Ok(game_id) => return share_group_game_id(group_game_id, game_id),
            Err(PreGameWaitError::Fatal(error)) => {
                return Err(error).context("waiting for matchmaking notification");
            }
            Err(PreGameWaitError::Recoverable(error)) => {
                let restore = restore_lobby_membership(
                    session,
                    settings,
                    &lobby_code,
                    &preferences,
                    Some(expected_user_ids),
                    cancellation,
                    error.context("matchmaking connection was interrupted"),
                );
                tokio::select! {
                    result = tokio::time::timeout_at(deadline, restore) => {
                        match result {
                            Ok(result) => result?,
                            Err(_) => {
                                return Err(anyhow!(
                                    "restoring lobby while waiting for a match exceeded {timeout:?}"
                                ));
                            }
                        }
                    }
                    changed = group_game_updates.changed() => {
                        changed.context("deterministic match-group assignment channel closed")?;
                        if let Some(game_id) = *group_game_updates.borrow_and_update() {
                            return Ok(game_id);
                        }
                    }
                }
            }
        }
    }
}

fn share_group_game_id(
    group_game_id: &watch::Sender<Option<u32>>,
    observed_game_id: u32,
) -> Result<u32> {
    let mut selected_game_id = observed_game_id;
    let mut conflict = None;
    group_game_id.send_if_modified(|current| {
        if let Some(existing_game_id) = *current {
            selected_game_id = existing_game_id;
            if existing_game_id != observed_game_id {
                conflict = Some(existing_game_id);
            }
            false
        } else {
            *current = Some(observed_game_id);
            true
        }
    });
    if let Some(existing_game_id) = conflict {
        return Err(anyhow!(
            "deterministic match group observed conflicting game IDs {existing_game_id} and {observed_game_id}"
        ));
    }
    Ok(selected_game_id)
}

async fn drive_ai(
    session: &mut LiveSession,
    engine: &mut GameEngine,
    snake_id: u32,
    command_profile: CommandProfile,
    last_decision_tick: &mut Option<u32>,
    pending_direction: &mut Option<Direction>,
) -> std::result::Result<(), DriveAiError> {
    let now = Utc::now()
        .timestamp_millis()
        .saturating_add(session.clock_offset_ms);
    engine
        .rebuild_predicted_state(now)
        .map_err(DriveAiError::Engine)?;
    let predicted_tick = engine.get_predicted_tick();
    if *last_decision_tick == Some(predicted_tick) {
        return Ok(());
    }
    *last_decision_tick = Some(predicted_tick);

    let Some(state) = engine.predicted_state() else {
        return Ok(());
    };
    let Some(snake) = state.arena.snakes.get(snake_id as usize) else {
        return Ok(());
    };
    if !snake.is_alive {
        return Ok(());
    }
    let current_direction = snake.direction;
    if *pending_direction == Some(current_direction) {
        *pending_direction = None;
    }
    let direction =
        calculate_ai_move(state, snake_id, current_direction).unwrap_or(current_direction);
    if direction == current_direction && !command_profile.sends_unchanged_turns() {
        return Ok(());
    }
    if !command_profile.sends_unchanged_turns() && pending_direction.is_some() {
        return Ok(());
    }

    let command = engine
        .process_local_command(GameCommand::Turn {
            snake_id,
            direction,
        })
        .map_err(DriveAiError::Engine)?;
    session
        .send_game_command(engine.game_id(), command)
        .await
        .map_err(|error| {
            DriveAiError::Transport(error.context("sending AI game command over WebSocket"))
        })?;
    if !command_profile.sends_unchanged_turns() {
        *pending_direction = Some(direction);
    }
    Ok(())
}

fn record_pending_command_resolution(
    pending_commands: &mut BTreeMap<u64, PendingCommand>,
    metrics: &mut SessionMetrics,
    sequence: u64,
) -> Option<u64> {
    let command = pending_commands.remove(&sequence)?;
    let sent_second = command.sent_at_unix_ms / 1_000;
    let count = metrics
        .command_outcome_counts_by_sent_unix_second
        .entry(sent_second)
        .or_default();
    *count = count.saturating_add(1);
    let maximum = metrics
        .command_outcome_max_latency_ms_by_sent_unix_second
        .entry(sent_second)
        .or_default();
    *maximum = (*maximum).max(elapsed_ms(command.sent_at));
    Some(sent_second)
}

fn record_all_pending_command_resolutions(
    pending_commands: &mut BTreeMap<u64, PendingCommand>,
    metrics: &mut SessionMetrics,
) {
    let sequences: Vec<u64> = pending_commands.keys().copied().collect();
    for sequence in sequences {
        record_pending_command_resolution(pending_commands, metrics, sequence);
    }
}

fn record_scheduled_pending_command_resolution(
    pending_commands: &mut BTreeMap<u64, PendingCommand>,
    metrics: &mut SessionMetrics,
    sequence: u64,
) -> bool {
    let Some(sent_second) = record_pending_command_resolution(pending_commands, metrics, sequence)
    else {
        return false;
    };
    let total = metrics
        .scheduled_command_counts_by_sent_unix_second
        .entry(sent_second)
        .or_default();
    *total = total.saturating_add(1);
    true
}

impl LiveSession {
    async fn send_game_command(&mut self, game_id: u32, command: GameCommandMessage) -> Result<()> {
        if !self.server_capabilities.contains("command-delivery-v2") {
            return Err(anyhow!(
                "server did not advertise the required command-delivery-v2 capability"
            ));
        }
        let sequence = self.next_command_sequence;
        self.next_command_sequence = self
            .next_command_sequence
            .checked_add(1)
            .ok_or_else(|| anyhow!("load-test command sequence exhausted"))?;
        let command_id = ClientCommandIdentityV2 {
            game_id,
            user_id: self.user_id,
            client_game_session_id: self.client_game_session_id.clone(),
            sequence,
        };
        let sent_at_unix_ms = unix_time_ms();
        self.pending_commands.insert(
            sequence,
            PendingCommand {
                message: command.clone(),
                sent_at_unix_ms,
                sent_at: Instant::now(),
            },
        );
        // This is one logical client submission even when the socket write is
        // ambiguous. Recovery resends the same identity, so counting only a
        // successful first write would make its eventual outcome unmatched.
        self.record.metrics.commands_sent = self.record.metrics.commands_sent.saturating_add(1);
        let commands_in_second = self
            .record
            .metrics
            .command_counts_by_unix_second
            .entry(sent_at_unix_ms / 1_000)
            .or_default();
        *commands_in_second = commands_in_second.saturating_add(1);
        self.send(WSMessage::GameCommandV2 {
            command_id,
            command,
        })
        .await
    }

    async fn resend_pending_commands(&mut self, game_id: u32) -> Result<()> {
        let pending: Vec<_> = self
            .pending_commands
            .iter()
            .map(|(sequence, command)| (*sequence, command.message.clone()))
            .collect();
        for (sequence, command) in pending {
            self.send(WSMessage::GameCommandV2 {
                command_id: ClientCommandIdentityV2 {
                    game_id,
                    user_id: self.user_id,
                    client_game_session_id: self.client_game_session_id.clone(),
                    sequence,
                },
                command,
            })
            .await?;
        }
        Ok(())
    }

    async fn send(&mut self, message: WSMessage) -> Result<()> {
        let kind = message_kind(&message);
        let payload = serde_json::to_string(&message)?;
        self.socket.send(Message::Text(payload)).await?;
        self.observe_sent(kind);
        Ok(())
    }

    fn observe_sent(&mut self, kind: &'static str) {
        self.record.metrics.messages_sent = self.record.metrics.messages_sent.saturating_add(1);
        self.remember(format!("sent:{kind}"));
    }

    fn record_command_resolution(&mut self, sequence: u64) -> Option<u64> {
        record_pending_command_resolution(
            &mut self.pending_commands,
            &mut self.record.metrics,
            sequence,
        )
    }

    fn record_terminal_game_resolutions(&mut self) {
        record_all_pending_command_resolutions(
            &mut self.pending_commands,
            &mut self.record.metrics,
        );
    }

    fn record_scheduled_command_resolution(&mut self, sequence: u64) -> bool {
        record_scheduled_pending_command_resolution(
            &mut self.pending_commands,
            &mut self.record.metrics,
            sequence,
        )
    }

    fn observe_received(&mut self, message: &WSMessage) {
        self.record.metrics.messages_received =
            self.record.metrics.messages_received.saturating_add(1);
        let kind = message_kind(message);
        self.remember(format!("received:{kind}"));
        if matches!(message, WSMessage::GameEvent(_)) {
            self.record
                .diagnostics
                .entry("first_game_event_at_unix_ms".to_owned())
                .or_insert_with(|| unix_time_ms().to_string());
        }
        if matches!(message, WSMessage::CommandOutcomesComplete { .. }) {
            self.record.metrics.command_outcome_barriers_received = self
                .record
                .metrics
                .command_outcome_barriers_received
                .saturating_add(1);
        }
        if let WSMessage::LobbyUpdate { members, state, .. } = message {
            self.last_lobby_members = members.iter().map(|member| member.user_id).collect();
            self.last_lobby_state = Some(state.clone());
        }
        match message {
            WSMessage::GameEvent(event) => {
                if terminal_event_completes_current_game(
                    self.record.game_id,
                    event.game_id,
                    &event.event,
                ) {
                    self.record_terminal_game_resolutions();
                }
                match &event.event {
                    GameEvent::CommandScheduledV2 { command_id, .. }
                        if command_id.user_id == self.user_id
                            && command_id.client_game_session_id == self.client_game_session_id =>
                    {
                        if self.record_scheduled_command_resolution(command_id.sequence) {
                            let received_total = self
                                .record
                                .metrics
                                .scheduled_command_counts_by_unix_second
                                .entry(unix_time_ms() / 1_000)
                                .or_default();
                            *received_total = received_total.saturating_add(1);
                        }
                    }
                    GameEvent::CommandRejected { command_id, .. }
                        if command_id.user_id == self.user_id
                            && command_id.client_game_session_id == self.client_game_session_id =>
                    {
                        self.record_command_resolution(command_id.sequence);
                    }
                    _ => {}
                }
            }
            WSMessage::CommandOutcomes {
                game_id: _,
                client_game_session_id,
                contiguous_through,
                outcomes,
            } if client_game_session_id == &self.client_game_session_id => {
                let resolved: Vec<u64> = self
                    .pending_commands
                    .keys()
                    .copied()
                    .filter(|sequence| {
                        *sequence <= *contiguous_through || outcomes.contains_key(sequence)
                    })
                    .collect();
                for sequence in resolved {
                    let scheduled = matches!(
                        outcomes.get(&sequence),
                        Some(CommandOutcome::Scheduled { .. })
                    );
                    if scheduled {
                        self.record_scheduled_command_resolution(sequence);
                    } else {
                        self.record_command_resolution(sequence);
                    }
                }
            }
            _ => {}
        }
    }

    async fn send_cancellable(
        &mut self,
        message: WSMessage,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        tokio::select! {
            _ = cancellation.cancelled() => Err(anyhow!("operation cancelled")),
            result = self.send(message) => result,
        }
    }

    async fn next_message(&mut self) -> Result<WSMessage> {
        loop {
            let next = self
                .socket
                .next()
                .await
                .ok_or_else(|| anyhow!("websocket stream ended"))??;
            match next {
                Message::Text(text) => {
                    let message: WSMessage = serde_json::from_str(&text).with_context(|| {
                        format!("unrecognized websocket payload ({} bytes)", text.len())
                    })?;
                    self.observe_received(&message);
                    return Ok(message);
                }
                Message::Ping(payload) => {
                    self.socket.send(Message::Pong(payload)).await?;
                }
                Message::Close(frame) => {
                    return Err(anyhow!("websocket closed: {frame:?}"));
                }
                _ => {}
            }
        }
    }

    async fn wait_for_pre_game<T, F>(
        &mut self,
        timeout: Duration,
        cancellation: &CancellationToken,
        mut matcher: F,
    ) -> std::result::Result<T, PreGameWaitError>
    where
        F: FnMut(&WSMessage) -> Option<T>,
    {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => {
                    return Err(PreGameWaitError::Fatal(anyhow!("operation cancelled")));
                }
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(PreGameWaitError::Fatal(anyhow!(
                        "operation timed out after {timeout:?}"
                    )));
                }
                message = self.next_message() => {
                    let message = message.map_err(PreGameWaitError::Recoverable)?;
                    match &message {
                        WSMessage::AccessDenied { reason } => {
                            return Err(PreGameWaitError::Fatal(anyhow!(
                                "server denied request: {reason}"
                            )));
                        }
                        WSMessage::Drain { task_boot_id, deadline_unix_ms } => {
                            return Err(PreGameWaitError::Recoverable(anyhow!(
                                "task {task_boot_id} requested WebSocket drain by {deadline_unix_ms}"
                            )));
                        }
                        WSMessage::LobbyRegionMismatch {
                            target_region,
                            ws_url,
                            lobby_code,
                        } => {
                            return Err(PreGameWaitError::Fatal(anyhow!(
                                "lobby {lobby_code} moved to region {target_region} at {ws_url}"
                            )));
                        }
                        _ => {}
                    }
                    if let Some(value) = matcher(&message) {
                        return Ok(value);
                    }
                }
            }
        }
    }

    async fn reconnect(
        &mut self,
        timeout: Duration,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let connect_result = tokio::select! {
            _ = cancellation.cancelled() => return Err(anyhow!("operation cancelled")),
            result = connect_socket(
                &self.websocket_url,
                &self.origin,
                timeout,
                &self.backend_hints,
                self.sticky_cookie.as_deref(),
            ) => result,
        };
        let (socket, backend, sticky_cookie) = connect_result?;
        self.socket = socket;
        if sticky_cookie.is_some() {
            self.sticky_cookie = sticky_cookie;
        }
        if let Some(backend) = backend {
            self.record
                .diagnostics
                .insert(format!("reconnect_backend_{}", self.reconnects), backend);
        }
        let websocket_auth_started = Instant::now();
        let token = self.token.clone();
        self.send_cancellable(WSMessage::Token(token), cancellation)
            .await?;
        self.wait_for_authenticated(timeout, cancellation).await?;
        self.record
            .metrics
            .websocket_auth_ms
            .push(elapsed_ms(websocket_auth_started));
        Ok(())
    }

    async fn wait_for_authenticated(
        &mut self,
        timeout: Duration,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let message = tokio::select! {
                _ = cancellation.cancelled() => return Err(anyhow!("operation cancelled")),
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(anyhow!("server did not authenticate the socket within {timeout:?}"));
                }
                incoming = self.next_message() => incoming?,
            };
            match message {
                WSMessage::Authenticated {
                    task_boot_id,
                    protocol_version,
                    capabilities,
                    socket_generation,
                } => {
                    let suffix = self.reconnects.to_string();
                    self.current_task_boot_id = Some(task_boot_id.clone());
                    self.current_socket_generation = Some(socket_generation);
                    self.record
                        .diagnostics
                        .insert(format!("task_boot_id_{suffix}"), task_boot_id);
                    self.record.diagnostics.insert(
                        format!("protocol_version_{suffix}"),
                        protocol_version.to_string(),
                    );
                    self.record.diagnostics.insert(
                        format!("socket_generation_{suffix}"),
                        socket_generation.to_string(),
                    );
                    self.record.diagnostics.insert(
                        format!("protocol_capabilities_{suffix}"),
                        capabilities.join(","),
                    );
                    let capabilities = capabilities.into_iter().collect();
                    validate_required_server_capabilities(&capabilities)
                        .context("authenticated server is incompatible")?;
                    self.server_capabilities = capabilities;
                    return Ok(());
                }
                WSMessage::AccessDenied { reason } => {
                    return Err(anyhow!("server denied authentication: {reason}"));
                }
                WSMessage::Drain { .. } => {
                    return Err(anyhow!(
                        "task began draining before authentication completed"
                    ));
                }
                _ => {}
            }
        }
    }

    fn remember(&mut self, event: String) {
        if self.recent_events.len() == RECENT_EVENT_LIMIT {
            self.recent_events.pop_front();
        }
        self.recent_events.push_back(event);
    }

    fn fail(&mut self, phase: SessionPhase, message: impl Into<String>) {
        let mut failure = SessionFailureRecord::new(phase, unix_time_ms(), message);
        if let Some(game_id) = self.record.game_id {
            failure = failure.with_context("game_id", game_id.to_string());
        }
        if !self.recent_events.is_empty() {
            self.record.diagnostics.insert(
                "recent_protocol_events".to_owned(),
                self.recent_events
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }
        self.record.fail(failure);
    }

    fn into_record(mut self) -> SessionRecord {
        let pending_commands = self.pending_commands.len() as u64;
        self.record.metrics.pending_commands_at_finish = pending_commands;
        if pending_commands > 0 && self.record.outcome == SessionOutcome::Completed {
            self.fail(
                SessionPhase::Cleanup,
                format!(
                    "{pending_commands} game commands lacked a terminal executor outcome at session finish"
                ),
            );
        }
        if !self.recent_events.is_empty() {
            self.record.diagnostics.insert(
                "recent_protocol_events".to_owned(),
                self.recent_events.into_iter().collect::<Vec<_>>().join(","),
            );
        }
        self.record
    }
}

fn terminal_event_completes_current_game(
    current_game_id: Option<u32>,
    event_game_id: u32,
    event: &GameEvent,
) -> bool {
    let status = match event {
        GameEvent::Snapshot { game_state } => &game_state.status,
        GameEvent::StatusUpdated { status } => status,
        _ => return false,
    };
    // Terminal authoritative state makes every unresolved command for this
    // game a definitive no-op, including after reconnect snapshot replay.
    current_game_id == Some(event_game_id) && matches!(status, GameStatus::Complete { .. })
}

async fn create_guest(
    client: &Client,
    api_origin: &Url,
    nickname: &str,
    timeout: Duration,
) -> Result<GuestResponse> {
    let endpoint = api_origin.join("/api/auth/guest")?;
    let response = tokio::time::timeout(
        timeout,
        client
            .post(endpoint.clone())
            .json(&serde_json::json!({ "nickname": nickname }))
            .send(),
    )
    .await
    .map_err(|_| anyhow!("guest authentication timed out after {timeout:?}"))??;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow!(
            "guest authentication at {endpoint} returned HTTP {status}: {}",
            bounded(&body, 300)
        ));
    }
    response
        .json::<GuestResponse>()
        .await
        .context("decoding guest authentication response")
}

async fn connect_socket(
    websocket_url: &Url,
    origin: &str,
    timeout: Duration,
    backend_hints: &BackendHintRegistry,
    sticky_cookie: Option<&str>,
) -> Result<(Socket, Option<String>, Option<String>)> {
    let request = websocket_request(websocket_url, origin, sticky_cookie)?;
    let (socket, response) = tokio::time::timeout(timeout, connect_async(request))
        .await
        .map_err(|_| anyhow!("websocket connection timed out after {timeout:?}"))??;
    let sticky_cookie = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .find_map(sticky_cookie_value)
        .map(ToOwned::to_owned);
    let backend = sticky_cookie
        .as_deref()
        .and_then(|raw| backend_hints.observe_sticky_value(raw))
        .map(|hint| hint.identifier);
    Ok((socket, backend, sticky_cookie))
}

fn is_retryable_websocket_admission(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<WebSocketError>().is_some_and(|error| {
            matches!(error, WebSocketError::Http(response)
            if matches!(
                response.status(),
                StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
            ))
        })
    })
}

async fn send_socket_message(socket: &mut Socket, message: &WSMessage) -> Result<()> {
    let payload = serde_json::to_string(message)?;
    socket.send(Message::Text(payload)).await?;
    Ok(())
}

async fn next_socket_message(socket: &mut Socket) -> Result<WSMessage> {
    loop {
        let next = socket
            .next()
            .await
            .ok_or_else(|| anyhow!("websocket stream ended"))??;
        match next {
            Message::Text(text) => {
                return serde_json::from_str(&text).with_context(|| {
                    format!("unrecognized websocket payload ({} bytes)", text.len())
                });
            }
            Message::Ping(payload) => socket.send(Message::Pong(payload)).await?,
            Message::Close(frame) => return Err(anyhow!("websocket closed: {frame:?}")),
            _ => {}
        }
    }
}

fn websocket_request(
    websocket_url: &Url,
    origin: &str,
    sticky_cookie: Option<&str>,
) -> Result<Request<()>> {
    let mut request = websocket_url
        .as_str()
        .into_client_request()
        .context("building websocket handshake request")?;
    request
        .headers_mut()
        .insert(ORIGIN, origin.parse().context("invalid Origin header")?);
    apply_sticky_cookie(request.headers_mut(), sticky_cookie)?;
    Ok(request)
}

fn apply_sticky_cookie(headers: &mut HeaderMap, sticky_cookie: Option<&str>) -> Result<()> {
    if let Some(value) = sticky_cookie {
        let cookie = format!("snaketron_sticky={value}");
        headers.insert(
            COOKIE,
            cookie
                .parse()
                .context("sticky cookie was not a valid request header")?,
        );
    }
    Ok(())
}

fn validate_group(
    mut played: Vec<PlayedSession>,
    expected_user_ids: &BTreeSet<u32>,
    expected_game_count: usize,
) -> MatchGroupResult {
    let observed_game_ids: BTreeSet<u32> = played
        .iter()
        .filter_map(|session| session.record.game_id)
        .collect();
    if played
        .iter()
        .any(|session| session.record.outcome == SessionOutcome::Cancelled)
    {
        return MatchGroupResult {
            sessions: played.into_iter().map(|session| session.record).collect(),
            expected_game_count,
            observed_game_ids,
            pairing_violation: None,
        };
    }
    let mut violations = Vec::new();
    if observed_game_ids.len() != expected_game_count {
        violations.push(format!(
            "expected {expected_game_count} game ID, observed {} ({observed_game_ids:?})",
            observed_game_ids.len()
        ));
    }
    for session in &played {
        if !session.snapshot_user_ids.is_empty() && &session.snapshot_user_ids != expected_user_ids
        {
            violations.push(format!(
                "{} snapshot users {:?}, expected {:?}",
                session.record.session_id, session.snapshot_user_ids, expected_user_ids
            ));
        }
    }
    violations.sort();
    violations.dedup();
    let pairing_violation = (!violations.is_empty()).then(|| violations.join("; "));
    if let Some(message) = &pairing_violation {
        for session in &mut played {
            if session.record.failure.is_some() {
                session
                    .record
                    .diagnostics
                    .insert("pairing_validation_error".to_owned(), message.clone());
            } else {
                session.record.fail(
                    SessionFailureRecord::new(
                        SessionPhase::GameSnapshot,
                        unix_time_ms(),
                        format!("deterministic matchmaking validation failed: {message}"),
                    )
                    .with_context("expected_user_ids", format!("{expected_user_ids:?}"))
                    .with_context("observed_game_ids", format!("{observed_game_ids:?}")),
                )
            }
        }
    }
    MatchGroupResult {
        sessions: played.into_iter().map(|session| session.record).collect(),
        expected_game_count,
        observed_game_ids,
        pairing_violation,
    }
}

async fn fail_and_close_all(
    sessions: Vec<LiveSession>,
    phase: SessionPhase,
    message: &str,
) -> Vec<SessionRecord> {
    join_all(sessions.into_iter().map(|mut session| async move {
        session.fail(phase, message);
        let _ = session.socket.close(None).await;
        session.into_record()
    }))
    .await
}

async fn cancel_and_close_all(sessions: Vec<LiveSession>, reason: &str) -> Vec<SessionRecord> {
    join_all(sessions.into_iter().map(|mut session| async move {
        session.record.cancel(unix_time_ms(), reason);
        let _ = session.socket.close(None).await;
        session.into_record()
    }))
    .await
}

async fn wait_at_wave_barrier(
    barrier: &Barrier,
    timeout: Duration,
    cancellation: &CancellationToken,
) -> bool {
    tokio::select! {
        _ = cancellation.cancelled() => false,
        result = tokio::time::timeout(timeout, barrier.wait()) => result.is_ok(),
    }
}

fn fail_record(record: &mut SessionRecord, phase: SessionPhase, message: impl Into<String>) {
    record.fail(SessionFailureRecord::new(phase, unix_time_ms(), message));
}

pub fn deterministic_username(run_id: &str, session_index: u64) -> String {
    // Guest nicknames are capped at 20 characters. A stable FNV-1a suffix keeps
    // names deterministic without leaking a potentially long run identifier.
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in run_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!(
        "lt{:06x}{:08}",
        hash & 0xff_ffff,
        session_index % 100_000_000
    )
}

fn sticky_cookie_value(header: &str) -> Option<&str> {
    header.split(';').find_map(|part| {
        let (name, value) = part.trim().split_once('=')?;
        name.eq_ignore_ascii_case("snaketron_sticky")
            .then_some(value)
    })
}

fn message_kind(message: &WSMessage) -> &'static str {
    match message {
        WSMessage::Token(_) => "Token",
        WSMessage::JoinGame(_) => "JoinGame",
        WSMessage::LeaveGame => "LeaveGame",
        WSMessage::GameCommandV2 { .. } => "GameCommandV2",
        WSMessage::GameEvent(_) => "GameEvent",
        WSMessage::CommandOutcomes { .. } => "CommandOutcomes",
        WSMessage::CommandOutcomesComplete { .. } => "CommandOutcomesComplete",
        WSMessage::Ping { .. } => "Ping",
        WSMessage::Pong { .. } => "Pong",
        WSMessage::QueueForMatch { .. } => "QueueForMatch",
        WSMessage::QueueForMatchMulti { .. } => "QueueForMatchMulti",
        WSMessage::LeaveQueue => "LeaveQueue",
        WSMessage::MatchFound { .. } => "MatchFound",
        WSMessage::QueueUpdate { .. } => "QueueUpdate",
        WSMessage::QueueLeft => "QueueLeft",
        WSMessage::AccessDenied { .. } => "AccessDenied",
        WSMessage::GameLoadFailed { .. } => "GameLoadFailed",
        WSMessage::GameWarming { .. } => "GameWarming",
        WSMessage::Authenticated { .. } => "Authenticated",
        WSMessage::Drain { .. } => "Drain",
        WSMessage::CreateLobby => "CreateLobby",
        WSMessage::LobbyCreated { .. } => "LobbyCreated",
        WSMessage::JoinLobby { .. } => "JoinLobby",
        WSMessage::JoinedLobby { .. } => "JoinedLobby",
        WSMessage::LeaveLobby => "LeaveLobby",
        WSMessage::LeftLobby => "LeftLobby",
        WSMessage::LobbyUpdate { .. } => "LobbyUpdate",
        WSMessage::UpdateLobbyPreferences { .. } => "UpdateLobbyPreferences",
        _ => "Other",
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

fn duration_between_ms(later: Instant, earlier: Instant) -> u64 {
    later
        .saturating_duration_since(earlier)
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn bounded(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_transient_websocket_http_statuses_are_retryable_admission() {
        let unavailable = tokio_tungstenite::tungstenite::http::Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(None)
            .unwrap();
        let unavailable = anyhow::Error::new(WebSocketError::Http(unavailable));
        assert!(is_retryable_websocket_admission(&unavailable));

        let rate_limited = tokio_tungstenite::tungstenite::http::Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .body(None)
            .unwrap();
        let rate_limited = anyhow::Error::new(WebSocketError::Http(rate_limited));
        assert!(is_retryable_websocket_admission(&rate_limited));

        let forbidden = tokio_tungstenite::tungstenite::http::Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(None)
            .unwrap();
        let forbidden = anyhow::Error::new(WebSocketError::Http(forbidden));
        assert!(!is_retryable_websocket_admission(&forbidden));
        assert!(!is_retryable_websocket_admission(&anyhow!(
            "transport failed"
        )));
    }

    #[test]
    fn current_websocket_capabilities_are_required_as_one_contract() {
        let capabilities = REQUIRED_SERVER_CAPABILITIES
            .iter()
            .map(|capability| (*capability).to_owned())
            .collect();
        assert!(validate_required_server_capabilities(&capabilities).is_ok());

        let mut capabilities = capabilities;
        capabilities.remove("command-outcomes-v1");
        let error = validate_required_server_capabilities(&capabilities).unwrap_err();
        assert!(error.to_string().contains("command-outcomes-v1"));
    }

    fn duel_snapshot(user_ids: &[u32]) -> GameState {
        let mut game_state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Competitive,
            None,
            Utc::now().timestamp_millis() + 1_000,
        );
        for user_id in user_ids {
            game_state
                .add_player(*user_id, Some(format!("user-{user_id}")))
                .expect("test player should fit in duel snapshot");
        }
        game_state
    }

    #[test]
    fn only_current_game_terminal_events_resolve_pending_commands() {
        let nonterminal = GameEvent::Snapshot {
            game_state: duel_snapshot(&[7, 8]),
        };
        assert!(!terminal_event_completes_current_game(
            Some(42),
            42,
            &nonterminal
        ));

        let terminal_status = GameEvent::StatusUpdated {
            status: GameStatus::Complete {
                winning_snake_id: None,
            },
        };
        assert!(!terminal_event_completes_current_game(
            Some(42),
            99,
            &terminal_status
        ));
        assert!(terminal_event_completes_current_game(
            Some(42),
            42,
            &terminal_status
        ));

        let mut terminal_snapshot = duel_snapshot(&[7, 8]);
        terminal_snapshot.status = GameStatus::Complete {
            winning_snake_id: None,
        };
        assert!(terminal_event_completes_current_game(
            Some(42),
            42,
            &GameEvent::Snapshot {
                game_state: terminal_snapshot,
            },
        ));
    }

    #[test]
    fn command_resolution_is_exactly_once_and_terminal_bulk_drains_remaining() {
        use common::CommandId;

        let message = GameCommandMessage {
            command_id_client: CommandId {
                tick: 1,
                user_id: 7,
                sequence_number: 1,
            },
            command_id_server: None,
            command: GameCommand::Turn {
                snake_id: 0,
                direction: Direction::Up,
            },
        };
        let sent_at = Instant::now()
            .checked_sub(Duration::from_millis(5))
            .unwrap();
        let mut pending_commands = BTreeMap::from([
            (
                1,
                PendingCommand {
                    message: message.clone(),
                    sent_at_unix_ms: 123_456,
                    sent_at,
                },
            ),
            (
                2,
                PendingCommand {
                    message: message.clone(),
                    sent_at_unix_ms: 123_456,
                    sent_at,
                },
            ),
            (
                3,
                PendingCommand {
                    message,
                    sent_at_unix_ms: 123_456,
                    sent_at,
                },
            ),
        ]);
        let mut metrics = SessionMetrics::default();

        assert!(record_scheduled_pending_command_resolution(
            &mut pending_commands,
            &mut metrics,
            1,
        ));
        assert!(!record_scheduled_pending_command_resolution(
            &mut pending_commands,
            &mut metrics,
            1,
        ));
        assert_eq!(
            metrics.command_outcome_counts_by_sent_unix_second,
            BTreeMap::from([(123, 1)])
        );
        assert_eq!(
            metrics.scheduled_command_counts_by_sent_unix_second,
            BTreeMap::from([(123, 1)])
        );

        record_all_pending_command_resolutions(&mut pending_commands, &mut metrics);
        record_all_pending_command_resolutions(&mut pending_commands, &mut metrics);
        assert!(pending_commands.is_empty());
        assert_eq!(
            metrics.command_outcome_counts_by_sent_unix_second,
            BTreeMap::from([(123, 3)])
        );
        assert!(metrics.command_outcome_max_latency_ms_by_sent_unix_second[&123] >= 5);
    }

    #[test]
    fn deterministic_names_are_stable_valid_and_unique() {
        let first = deterministic_username("autoscale-2026", 1);
        let repeated = deterministic_username("autoscale-2026", 1);
        let second = deterministic_username("autoscale-2026", 2);
        assert_eq!(first, repeated);
        assert_ne!(first, second);
        assert!(first.len() <= 20);
        assert!(
            first
                .chars()
                .all(|character| character.is_ascii_alphanumeric())
        );
    }

    #[test]
    fn planned_handoff_gap_uses_observed_interval_ordering() {
        let old_usable_through = Instant::now();
        let candidate_ready_late = old_usable_through + Duration::from_millis(17);
        assert_eq!(
            duration_between_ms(candidate_ready_late, old_usable_through),
            17
        );
        assert_eq!(
            duration_between_ms(old_usable_through, candidate_ready_late),
            0
        );
    }

    #[test]
    fn game_warming_pauses_commands_until_snapshot_and_outcome_barrier() {
        let now = tokio::time::Instant::now();
        let deadline = now + Duration::from_secs(5);
        let mut warmup = PlayingWarmupState::default();
        assert!(!warmup.commands_paused());

        warmup.observe_warming(now, deadline, 1);
        assert!(warmup.commands_paused());
        assert_eq!(warmup.join_retry_at, Some(now + Duration::from_millis(100)),);

        warmup.mark_retry_sent();
        assert!(warmup.commands_paused());
        assert_eq!(warmup.join_retry_at, None);

        assert!(!warmup.observe_snapshot());
        assert!(warmup.commands_paused());
        assert!(warmup.observe_outcome_barrier());
        assert!(!warmup.commands_paused());

        // The wire loop must also handle the barrier arriving before the
        // snapshot without resuming or resending early.
        warmup.observe_warming(now, deadline, 1);
        assert!(!warmup.observe_outcome_barrier());
        assert!(warmup.commands_paused());
        assert!(warmup.observe_snapshot());
        assert!(!warmup.commands_paused());
    }

    #[test]
    fn game_warming_retry_never_competes_with_its_deadline() {
        let now = tokio::time::Instant::now();
        assert_eq!(
            bounded_game_join_retry_at(now, now + Duration::from_millis(101), 100),
            Some(now + Duration::from_millis(100)),
        );
        assert_eq!(
            bounded_game_join_retry_at(now, now + Duration::from_millis(100), 100),
            None,
        );
        assert_eq!(
            bounded_game_join_retry_at(now, now + Duration::from_millis(50), 2_000),
            None,
        );
    }

    #[test]
    fn extracts_only_the_sticky_cookie() {
        assert_eq!(
            sticky_cookie_value("snaketron_sticky=backend-token; Path=/; Secure"),
            Some("backend-token")
        );
        assert_eq!(sticky_cookie_value("other=value; Path=/"), None);
    }

    #[test]
    fn untimed_games_use_the_configured_window_while_timed_games_keep_the_margin() {
        let untimed = Duration::from_secs(37);
        assert_eq!(configured_game_window(None, untimed), (untimed, true));
        assert_eq!(
            configured_game_window(Some(60_000), untimed),
            (Duration::from_secs(60) + GAME_TIMEOUT_MARGIN, false)
        );
        assert_eq!(
            duration_until_server_time(1_250, 1_000),
            Duration::from_millis(250)
        );
        assert_eq!(duration_until_server_time(1_000, 1_250), Duration::ZERO);
    }

    #[tokio::test]
    async fn timeboxed_leave_is_confirmed_by_an_ordered_ping() {
        use tokio::net::TcpListener;
        use tokio_tungstenite::accept_async;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let websocket_url = Url::parse(&format!("ws://{address}/ws")).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let leave = socket.next().await.unwrap().unwrap();
            assert!(matches!(
                serde_json::from_str::<WSMessage>(leave.to_text().unwrap()).unwrap(),
                WSMessage::LeaveGame
            ));
            let ping = socket.next().await.unwrap().unwrap();
            let client_time =
                match serde_json::from_str::<WSMessage>(ping.to_text().unwrap()).unwrap() {
                    WSMessage::Ping { client_time } => client_time,
                    other => panic!("expected Ping after LeaveGame, received {other:?}"),
                };
            socket
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::Pong {
                        client_time,
                        server_time: client_time,
                    })
                    .unwrap(),
                ))
                .await
                .unwrap();
        });

        let (socket, _) = connect_async(websocket_url.as_str()).await.unwrap();
        let (activity_sender, _activity_receiver) = mpsc::unbounded_channel();
        let mut session = LiveSession {
            record: SessionRecord::new("session-1", "test-user", 0, "group", 0),
            user_id: 1,
            token: "test-token".to_owned(),
            socket,
            websocket_url,
            origin: "http://127.0.0.1".to_owned(),
            backend_hints: BackendHintRegistry::default(),
            sticky_cookie: None,
            last_lobby_members: BTreeSet::new(),
            last_lobby_state: None,
            recent_events: VecDeque::new(),
            clock_offset_ms: 0,
            last_ping_client_time: None,
            current_task_boot_id: None,
            current_socket_generation: None,
            reconnects: 0,
            client_game_session_id: "test-session-1".to_owned(),
            next_command_sequence: 1,
            pending_commands: BTreeMap::new(),
            server_capabilities: BTreeSet::new(),
            activity_lease: SessionActivityLease::new(1, activity_sender),
        };

        leave_game_and_confirm(
            &mut session,
            Some(Utc::now().timestamp_millis()),
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(session.record.metrics.messages_sent, 2);
        assert_eq!(session.record.metrics.websocket_rtt_ms.len(), 1);
        server.await.unwrap();
    }

    #[test]
    fn reconnect_request_replays_only_the_private_sticky_cookie() {
        let url = Url::parse("wss://use1.snaketron.io/ws").unwrap();
        let request =
            websocket_request(&url, "https://snaketron.io", Some("opaque-backend-token")).unwrap();

        assert_eq!(
            request.headers().get(ORIGIN).unwrap(),
            "https://snaketron.io"
        );
        assert_eq!(
            request.headers().get(COOKIE).unwrap(),
            "snaketron_sticky=opaque-backend-token"
        );
        assert!(
            websocket_request(&url, "https://snaketron.io", None)
                .unwrap()
                .headers()
                .get(COOKIE)
                .is_none()
        );
    }

    #[test]
    fn activity_lease_emits_connected_once_and_terminal_on_drop() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        {
            let mut lease = SessionActivityLease::new(17, sender);
            lease.mark_connected();
            lease.mark_connected();
            assert_eq!(
                receiver.try_recv().unwrap(),
                SessionActivityEvent::Connected { session_index: 17 }
            );
            assert!(receiver.try_recv().is_err());
        }
        assert_eq!(
            receiver.try_recv().unwrap(),
            SessionActivityEvent::Terminal { session_index: 17 }
        );
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn activity_lease_reports_terminal_before_connection_too() {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        drop(SessionActivityLease::new(23, sender));
        assert_eq!(
            receiver.try_recv().unwrap(),
            SessionActivityEvent::Terminal { session_index: 23 }
        );
    }

    #[test]
    fn group_game_assignment_is_idempotent_and_rejects_conflicts() {
        let (group_game_id, _) = watch::channel(None);
        assert_eq!(share_group_game_id(&group_game_id, 42).unwrap(), 42);
        assert_eq!(share_group_game_id(&group_game_id, 42).unwrap(), 42);

        let error = share_group_game_id(&group_game_id, 99).unwrap_err();
        assert!(error.to_string().contains("conflicting game IDs 42 and 99"));
    }

    #[test]
    fn simultaneous_group_game_assignments_cannot_overwrite_each_other() {
        use std::sync::{Arc, Barrier as ThreadBarrier};

        let (group_game_id, _) = watch::channel(None);
        let barrier = Arc::new(ThreadBarrier::new(2));
        let assignments = [42, 99].map(|observed_game_id| {
            let group_game_id = group_game_id.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                share_group_game_id(&group_game_id, observed_game_id)
            })
        });
        let results = assignments.map(|assignment| assignment.join().unwrap());

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
        let selected = (*group_game_id.borrow()).expect("one game ID must remain selected");
        assert!(matches!(selected, 42 | 99));
        assert_eq!(
            results.iter().find_map(|result| result.as_ref().ok()),
            Some(&selected)
        );
    }

    #[tokio::test]
    async fn matchmaking_disconnect_reauthenticates_rejoins_and_preserves_roster() {
        use server::lobby_manager::LobbyMember;
        use tokio::net::TcpListener;
        use tokio_tungstenite::accept_async;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let websocket_url = Url::parse(&format!("ws://{address}/ws")).unwrap();

        let first_server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            accept_async(stream).await.unwrap()
        });
        let (socket, _) = connect_async(websocket_url.as_str()).await.unwrap();
        let mut first_server = first_server.await.unwrap();
        first_server.close(None).await.unwrap();

        let listener = TcpListener::bind(address).await.unwrap();
        let preferences = LobbyPreferences {
            selected_modes: vec!["solo".to_owned()],
            competitive: false,
        };
        let server_preferences = preferences.clone();
        let recovery_server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();

            let token = socket.next().await.unwrap().unwrap();
            assert!(matches!(
                serde_json::from_str::<WSMessage>(token.to_text().unwrap()).unwrap(),
                WSMessage::Token(value) if value == "test-token"
            ));
            socket
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::Authenticated {
                        task_boot_id: "replacement-task".to_owned(),
                        protocol_version: 2,
                        capabilities: REQUIRED_SERVER_CAPABILITIES
                            .iter()
                            .map(|capability| (*capability).to_owned())
                            .collect(),
                        socket_generation: 2,
                    })
                    .unwrap(),
                ))
                .await
                .unwrap();
            let join = socket.next().await.unwrap().unwrap();
            assert!(matches!(
                serde_json::from_str::<WSMessage>(join.to_text().unwrap()).unwrap(),
                WSMessage::JoinLobby { lobby_code, .. } if lobby_code == "TEST-LOBBY"
            ));
            socket
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::JoinedLobby {
                        lobby_code: "TEST-LOBBY".to_owned(),
                    })
                    .unwrap(),
                ))
                .await
                .unwrap();

            let update_preferences = socket.next().await.unwrap().unwrap();
            assert!(matches!(
                serde_json::from_str::<WSMessage>(update_preferences.to_text().unwrap()).unwrap(),
                WSMessage::UpdateLobbyPreferences { .. }
            ));
            socket
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::LobbyUpdate {
                        lobby_code: "TEST-LOBBY".to_owned(),
                        members: vec![LobbyMember {
                            user_id: 7,
                            username: "test-user".to_owned(),
                            ts: 0.0,
                        }],
                        host_user_id: 7,
                        state: "queued".to_owned(),
                        preferences: server_preferences,
                    })
                    .unwrap(),
                ))
                .await
                .unwrap();
            socket
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::JoinGame(99)).unwrap(),
                ))
                .await
                .unwrap();
        });

        let settings = SessionSettings {
            api_origin: Url::parse("http://127.0.0.1/").unwrap(),
            websocket_url,
            origin: "http://127.0.0.1".to_owned(),
            game_type: GameType::Solo,
            queue_mode: QueueMode::Quickmatch,
            selected_mode: "solo".to_owned(),
            competitive: false,
            population: Population::Game,
            connect_timeout: Duration::from_secs(1),
            lobby_timeout: Duration::from_secs(1),
            queue_timeout: Duration::from_secs(5),
            untimed_play_duration: Duration::from_secs(1),
            command_profile: CommandProfile::Realistic,
            backend_hints: BackendHintRegistry::default(),
        };
        let (activity_sender, _activity_receiver) = mpsc::unbounded_channel();
        let mut record = SessionRecord::new("session-7", "test-user", 0, "group", 0);
        record.lobby_code = Some("TEST-LOBBY".to_owned());
        let mut session = LiveSession {
            record,
            user_id: 7,
            token: "test-token".to_owned(),
            socket,
            websocket_url: settings.websocket_url.clone(),
            origin: settings.origin.clone(),
            backend_hints: settings.backend_hints.clone(),
            sticky_cookie: Some("opaque-backend-token".to_owned()),
            last_lobby_members: BTreeSet::new(),
            last_lobby_state: None,
            recent_events: VecDeque::new(),
            clock_offset_ms: 0,
            last_ping_client_time: None,
            current_task_boot_id: None,
            current_socket_generation: None,
            reconnects: 0,
            client_game_session_id: "test-session-7".to_owned(),
            next_command_sequence: 1,
            pending_commands: BTreeMap::new(),
            server_capabilities: BTreeSet::new(),
            activity_lease: SessionActivityLease::new(7, activity_sender),
        };
        let (group_game_id, _) = watch::channel(None);

        let game_id = wait_for_match_with_recovery(
            &mut session,
            &settings,
            &BTreeSet::from([7]),
            &group_game_id,
            settings.queue_timeout,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(game_id, 99);
        assert_eq!(session.last_lobby_members, BTreeSet::from([7]));
        assert_eq!(session.record.metrics.disconnects, 1);
        assert_eq!(session.record.metrics.reconnects, 1);
        assert_eq!(session.record.metrics.websocket_auth_ms.len(), 1);
        assert_eq!(session.record.metrics.reconnect_duration_ms.len(), 1);
        assert_eq!(session.record.metrics.rejoin_lobby_ms.len(), 1);
        assert_eq!(session.record.metrics.usable_session_gap_ms.len(), 1);
        recovery_server.await.unwrap();
    }

    #[tokio::test]
    async fn queued_population_handoff_restores_context_before_closing_old_socket() {
        use server::lobby_manager::LobbyMember;
        use tokio::net::TcpListener;
        use tokio_tungstenite::accept_async;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let websocket_url = Url::parse(&format!("ws://{address}/ws")).unwrap();
        let preferences = LobbyPreferences {
            selected_modes: vec!["2v2".to_owned()],
            competitive: false,
        };
        let server_preferences = preferences.clone();
        let server = tokio::spawn(async move {
            let (old_stream, _) = listener.accept().await.unwrap();
            let mut old_socket = accept_async(old_stream).await.unwrap();
            let (candidate_stream, _) = listener.accept().await.unwrap();
            let mut candidate_socket = accept_async(candidate_stream).await.unwrap();

            let token = candidate_socket.next().await.unwrap().unwrap();
            assert!(matches!(
                serde_json::from_str::<WSMessage>(token.to_text().unwrap()).unwrap(),
                WSMessage::Token(value) if value == "test-token"
            ));
            candidate_socket
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::Authenticated {
                        task_boot_id: "replacement-task".to_owned(),
                        protocol_version: 2,
                        capabilities: REQUIRED_SERVER_CAPABILITIES
                            .iter()
                            .map(|capability| (*capability).to_owned())
                            .collect(),
                        socket_generation: 2,
                    })
                    .unwrap(),
                ))
                .await
                .unwrap();
            let join = candidate_socket.next().await.unwrap().unwrap();
            assert!(matches!(
                serde_json::from_str::<WSMessage>(join.to_text().unwrap()).unwrap(),
                WSMessage::JoinLobby { lobby_code, .. } if lobby_code == "TEST-LOBBY"
            ));
            candidate_socket
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::LobbyUpdate {
                        lobby_code: "TEST-LOBBY".to_owned(),
                        members: vec![LobbyMember {
                            user_id: 7,
                            username: "test-user".to_owned(),
                            ts: 0.0,
                        }],
                        host_user_id: 7,
                        state: "queued".to_owned(),
                        preferences: server_preferences,
                    })
                    .unwrap(),
                ))
                .await
                .unwrap();

            let continuity = old_socket.next().await.unwrap().unwrap();
            let WSMessage::Ping { client_time } =
                serde_json::from_str::<WSMessage>(continuity.to_text().unwrap()).unwrap()
            else {
                panic!("old socket did not receive the continuity ping");
            };
            old_socket
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::Pong {
                        client_time,
                        server_time: Utc::now().timestamp_millis(),
                    })
                    .unwrap(),
                ))
                .await
                .unwrap();
            assert!(matches!(
                old_socket.next().await.unwrap().unwrap(),
                Message::Close(_)
            ));
            assert!(matches!(
                candidate_socket.next().await.unwrap().unwrap(),
                Message::Close(_)
            ));
        });

        let server_url = websocket_url.clone();
        let connect =
            tokio::spawn(async move { connect_async(server_url.as_str()).await.unwrap() });
        let (socket, _) = connect.await.unwrap();
        let settings = SessionSettings {
            api_origin: Url::parse("http://127.0.0.1/").unwrap(),
            websocket_url: websocket_url.clone(),
            origin: "http://127.0.0.1".to_owned(),
            game_type: GameType::TeamMatch { per_team: 2 },
            queue_mode: QueueMode::Quickmatch,
            selected_mode: "2v2".to_owned(),
            competitive: false,
            population: Population::Matchmaking,
            connect_timeout: Duration::from_secs(1),
            lobby_timeout: Duration::from_secs(1),
            queue_timeout: Duration::from_secs(1),
            untimed_play_duration: Duration::from_secs(1),
            command_profile: CommandProfile::Realistic,
            backend_hints: BackendHintRegistry::default(),
        };
        let (activity_sender, _activity_receiver) = mpsc::unbounded_channel();
        let mut record = SessionRecord::new("session-7", "test-user", 0, "group", 0);
        record.lobby_code = Some("TEST-LOBBY".to_owned());
        let mut session = LiveSession {
            record,
            user_id: 7,
            token: "test-token".to_owned(),
            socket,
            websocket_url,
            origin: settings.origin.clone(),
            backend_hints: settings.backend_hints.clone(),
            sticky_cookie: Some("departing-backend".to_owned()),
            last_lobby_members: BTreeSet::from([7]),
            last_lobby_state: Some("queued".to_owned()),
            recent_events: VecDeque::new(),
            clock_offset_ms: 0,
            last_ping_client_time: None,
            current_task_boot_id: None,
            current_socket_generation: None,
            reconnects: 0,
            client_game_session_id: "test-session-7".to_owned(),
            next_command_sequence: 1,
            pending_commands: BTreeMap::new(),
            server_capabilities: REQUIRED_SERVER_CAPABILITIES
                .iter()
                .map(|capability| (*capability).to_owned())
                .collect(),
            activity_lease: SessionActivityLease::new(7, activity_sender),
        };

        perform_pre_game_planned_handoff(
            &mut session,
            &settings,
            &BTreeSet::from([7]),
            &preferences,
            "old-task",
            Utc::now().timestamp_millis() + 5_000,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert_eq!(session.last_lobby_members, BTreeSet::from([7]));
        assert_eq!(session.last_lobby_state.as_deref(), Some("queued"));
        assert_eq!(session.record.metrics.planned_handoff_attempts, 1);
        assert_eq!(session.record.metrics.planned_handoff_successes, 1);
        assert_eq!(session.record.metrics.planned_handoff_failures, 0);
        assert_eq!(session.record.metrics.planned_handoff_continuity_proofs, 1);
        assert_eq!(session.record.metrics.usable_session_gap_ms, vec![0]);
        assert_eq!(session.record.metrics.disconnects, 0);
        assert_eq!(session.record.metrics.reconnects, 0);
        session.socket.close(None).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn planned_handoff_freezes_post_pong_frontier_while_old_stream_keeps_advancing() {
        use common::CommandId;
        use tokio::net::TcpListener;
        use tokio_tungstenite::accept_async;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let websocket_url = Url::parse(&format!("ws://{address}/ws")).unwrap();
        let mut snapshot = duel_snapshot(&[7, 8]);
        snapshot.start_ms = Utc::now().timestamp_millis().saturating_sub(1_000);
        let server_snapshot = snapshot.clone();

        let server = tokio::spawn(async move {
            let (old_stream, _) = listener.accept().await.unwrap();
            let mut old_socket = accept_async(old_stream).await.unwrap();
            let (candidate_stream, _) = listener.accept().await.unwrap();
            let mut candidate_socket = accept_async(candidate_stream).await.unwrap();

            let token = candidate_socket.next().await.unwrap().unwrap();
            assert!(matches!(
                serde_json::from_str::<WSMessage>(token.to_text().unwrap()).unwrap(),
                WSMessage::Token(value) if value == "test-token"
            ));

            old_socket
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::GameEvent(GameEventMessage {
                        game_id: 42,
                        tick: server_snapshot.tick,
                        sequence: 1,
                        stream_seq: 1,
                        user_id: Some(7),
                        event: GameEvent::TickHash {
                            hash: server_snapshot.sync_hash(),
                            server_ts_ms: Utc::now().timestamp_millis(),
                        },
                    }))
                    .unwrap(),
                ))
                .await
                .unwrap();
            candidate_socket
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::Authenticated {
                        task_boot_id: "candidate-task".to_owned(),
                        protocol_version: 2,
                        capabilities: REQUIRED_SERVER_CAPABILITIES
                            .iter()
                            .map(|capability| (*capability).to_owned())
                            .collect(),
                        socket_generation: 2,
                    })
                    .unwrap(),
                ))
                .await
                .unwrap();

            let join_lobby = candidate_socket.next().await.unwrap().unwrap();
            assert!(matches!(
                serde_json::from_str::<WSMessage>(join_lobby.to_text().unwrap()).unwrap(),
                WSMessage::JoinLobby { lobby_code, .. } if lobby_code == "TEST-LOBBY"
            ));
            let join_game = candidate_socket.next().await.unwrap().unwrap();
            assert!(matches!(
                serde_json::from_str::<WSMessage>(join_game.to_text().unwrap()).unwrap(),
                WSMessage::JoinGame(42)
            ));
            candidate_socket
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::GameWarming {
                        game_id: 42,
                        retry_after_ms: 100,
                    })
                    .unwrap(),
                ))
                .await
                .unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(50), candidate_socket.next())
                    .await
                    .is_err(),
                "GameWarming retry ignored the server delay"
            );
            let retried_join_game =
                tokio::time::timeout(Duration::from_millis(500), candidate_socket.next())
                    .await
                    .expect("GameWarming scheduled a same-socket JoinGame retry")
                    .unwrap()
                    .unwrap();
            assert!(matches!(
                serde_json::from_str::<WSMessage>(retried_join_game.to_text().unwrap()).unwrap(),
                WSMessage::JoinGame(42)
            ));
            candidate_socket
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::JoinedLobby {
                        lobby_code: "TEST-LOBBY".to_owned(),
                    })
                    .unwrap(),
                ))
                .await
                .unwrap();
            candidate_socket
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::GameEvent(GameEventMessage {
                        game_id: 42,
                        tick: server_snapshot.tick,
                        sequence: 0,
                        stream_seq: 1,
                        user_id: Some(7),
                        event: GameEvent::Snapshot {
                            game_state: server_snapshot.clone(),
                        },
                    }))
                    .unwrap(),
                ))
                .await
                .unwrap();
            candidate_socket
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::CommandOutcomes {
                        game_id: 42,
                        client_game_session_id: "test-session-7".to_owned(),
                        contiguous_through: 1,
                        outcomes: BTreeMap::new(),
                    })
                    .unwrap(),
                ))
                .await
                .unwrap();

            let mut commands_during_handoff = Vec::new();
            let command_window = tokio::time::Instant::now() + Duration::from_millis(150);
            loop {
                tokio::select! {
                    _ = tokio::time::sleep_until(command_window) => break,
                    frame = old_socket.next() => {
                        let frame = frame.expect("old socket stayed open").unwrap();
                        assert!(!matches!(frame, Message::Close(_)), "old socket closed before the delayed outcome barrier");
                        if let Message::Text(payload) = frame
                            && let WSMessage::GameCommandV2 { command_id, command } =
                                serde_json::from_str::<WSMessage>(&payload).unwrap()
                        {
                            commands_during_handoff.push((command_id, command));
                        }
                    }
                }
            }
            assert!(
                !commands_during_handoff.is_empty(),
                "every-tick input stopped while the planned candidate was prepared"
            );
            candidate_socket
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::CommandOutcomesComplete { game_id: 42 })
                        .unwrap(),
                ))
                .await
                .unwrap();

            let continuity_client_time = loop {
                let frame = old_socket.next().await.unwrap().unwrap();
                match frame {
                    Message::Text(payload) => {
                        match serde_json::from_str::<WSMessage>(&payload).unwrap() {
                            WSMessage::GameCommandV2 {
                                command_id,
                                command,
                            } => {
                                commands_during_handoff.push((command_id, command));
                            }
                            WSMessage::Ping { client_time } => {
                                break client_time;
                            }
                            _ => {}
                        }
                    }
                    Message::Close(_) => panic!("old socket closed before continuity pong"),
                    _ => {}
                }
            };
            // A ready candidate alone must not close the old socket. Hold the
            // continuity pong back while every-tick input continues.
            let proof_delay = tokio::time::Instant::now() + Duration::from_millis(50);
            loop {
                tokio::select! {
                    _ = tokio::time::sleep_until(proof_delay) => break,
                    frame = old_socket.next() => {
                        let frame = frame.expect("old socket stayed open until continuity proof").unwrap();
                        match frame {
                            Message::Text(payload) => {
                                if let WSMessage::GameCommandV2 { command_id, command } =
                                    serde_json::from_str::<WSMessage>(&payload).unwrap()
                                {
                                    commands_during_handoff.push((command_id, command));
                                }
                            }
                            Message::Close(_) => panic!("candidate readiness was mistaken for continuity proof"),
                            _ => {}
                        }
                    }
                }
            }
            // These frames are ordered before the pong and therefore must all
            // be covered by the candidate before promotion.
            for stream_seq in 2..=5 {
                old_socket
                    .send(Message::Text(
                        serde_json::to_string(&WSMessage::GameEvent(GameEventMessage {
                            game_id: 42,
                            tick: server_snapshot.tick,
                            sequence: server_snapshot.event_sequence,
                            stream_seq,
                            user_id: Some(7),
                            event: GameEvent::TickHash {
                                hash: server_snapshot.sync_hash(),
                                server_ts_ms: Utc::now().timestamp_millis(),
                            },
                        }))
                        .unwrap(),
                    ))
                    .await
                    .unwrap();
            }
            old_socket
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::Pong {
                        client_time: continuity_client_time,
                        server_time: Utc::now().timestamp_millis(),
                    })
                    .unwrap(),
                ))
                .await
                .unwrap();

            // The old application stream keeps advancing after Pong while the
            // candidate catches only the fixed frontier at 5.
            for stream_seq in 6..=10 {
                old_socket
                    .send(Message::Text(
                        serde_json::to_string(&WSMessage::GameEvent(GameEventMessage {
                            game_id: 42,
                            tick: server_snapshot.tick,
                            sequence: server_snapshot.event_sequence,
                            stream_seq,
                            user_id: Some(7),
                            event: GameEvent::TickHash {
                                hash: server_snapshot.sync_hash(),
                                server_ts_ms: Utc::now().timestamp_millis(),
                            },
                        }))
                        .unwrap(),
                    ))
                    .await
                    .unwrap();
            }

            // A candidate snapshot reaches the frozen frontier but invalidates
            // the earlier outcome barrier. The old socket must remain the
            // command owner until the paired barrier is delivered.
            candidate_socket
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::GameEvent(GameEventMessage {
                        game_id: 42,
                        tick: server_snapshot.tick,
                        sequence: server_snapshot.event_sequence,
                        stream_seq: 5,
                        user_id: Some(7),
                        event: GameEvent::Snapshot {
                            game_state: server_snapshot.clone(),
                        },
                    }))
                    .unwrap(),
                ))
                .await
                .unwrap();

            let barrier_delay = tokio::time::Instant::now() + Duration::from_millis(50);
            loop {
                tokio::select! {
                    _ = tokio::time::sleep_until(barrier_delay) => break,
                    frame = old_socket.next() => {
                        let frame = frame
                            .expect("old socket stayed open while takeover outcomes loaded")
                            .unwrap();
                        match frame {
                            Message::Text(payload) => {
                                if let WSMessage::GameCommandV2 { command_id, command } =
                                    serde_json::from_str::<WSMessage>(&payload).unwrap()
                                {
                                    commands_during_handoff.push((command_id, command));
                                }
                            }
                            Message::Close(_) => {
                                panic!("takeover snapshot promoted before its outcome barrier")
                            }
                            _ => {}
                        }
                    }
                }
            }
            candidate_socket
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::CommandOutcomesComplete { game_id: 42 })
                        .unwrap(),
                ))
                .await
                .unwrap();

            loop {
                let frame = old_socket
                    .next()
                    .await
                    .expect("old socket stayed open through candidate promotion")
                    .unwrap();
                match frame {
                    Message::Text(payload) => {
                        if let WSMessage::GameCommandV2 {
                            command_id,
                            command,
                        } = serde_json::from_str::<WSMessage>(&payload).unwrap()
                        {
                            commands_during_handoff.push((command_id, command));
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }

            // This covered snapshot is deliberately sent only after the old
            // socket closes. Filtering the candidate's preparation buffer is
            // therefore insufficient; the promoted stream must retain the old
            // applied watermark as a suppression floor.
            candidate_socket
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::GameEvent(GameEventMessage {
                        game_id: 42,
                        tick: server_snapshot.tick,
                        sequence: server_snapshot.event_sequence,
                        stream_seq: 5,
                        user_id: Some(7),
                        event: GameEvent::Snapshot {
                            game_state: server_snapshot.clone(),
                        },
                    }))
                    .unwrap(),
                ))
                .await
                .unwrap();

            // The replacement gateway can still deliver its buffered copy of
            // the post-Pong suffix. The promoted runtime must deduplicate it
            // against state already applied from the old socket.
            for stream_seq in 6..=10 {
                candidate_socket
                    .send(Message::Text(
                        serde_json::to_string(&WSMessage::GameEvent(GameEventMessage {
                            game_id: 42,
                            tick: server_snapshot.tick,
                            sequence: server_snapshot.event_sequence,
                            stream_seq,
                            user_id: Some(7),
                            event: GameEvent::TickHash {
                                hash: server_snapshot.sync_hash(),
                                server_ts_ms: Utc::now().timestamp_millis(),
                            },
                        }))
                        .unwrap(),
                    ))
                    .await
                    .unwrap();
            }

            for (index, (expected_id, expected_command)) in
                commands_during_handoff.into_iter().enumerate()
            {
                let resent = candidate_socket.next().await.unwrap().unwrap();
                assert!(matches!(
                    serde_json::from_str::<WSMessage>(resent.to_text().unwrap()).unwrap(),
                    WSMessage::GameCommandV2 { command_id, command }
                        if command_id == expected_id && command == expected_command
                ));
                candidate_socket
                    .send(Message::Text(
                        serde_json::to_string(&WSMessage::GameEvent(GameEventMessage {
                            game_id: 42,
                            tick: server_snapshot.tick,
                            sequence: server_snapshot.event_sequence,
                            stream_seq: index as u64 + 11,
                            user_id: Some(7),
                            event: GameEvent::CommandScheduledV2 {
                                command_id: expected_id,
                                command_message: expected_command,
                                deduplicated_replay: true,
                            },
                        }))
                        .unwrap(),
                    ))
                    .await
                    .unwrap();
            }
            assert!(matches!(
                candidate_socket.next().await.unwrap().unwrap(),
                Message::Close(_)
            ));
        });

        let (socket, _) = connect_async(websocket_url.as_str()).await.unwrap();
        let settings = SessionSettings {
            api_origin: Url::parse("http://127.0.0.1/").unwrap(),
            websocket_url: websocket_url.clone(),
            origin: "http://127.0.0.1".to_owned(),
            game_type: GameType::TeamMatch { per_team: 1 },
            queue_mode: QueueMode::Competitive,
            selected_mode: "duel".to_owned(),
            competitive: true,
            population: Population::Game,
            connect_timeout: Duration::from_secs(1),
            lobby_timeout: Duration::from_secs(1),
            queue_timeout: Duration::from_secs(1),
            untimed_play_duration: Duration::from_secs(1),
            command_profile: CommandProfile::EveryTick,
            backend_hints: BackendHintRegistry::default(),
        };
        let (activity_sender, _activity_receiver) = mpsc::unbounded_channel();
        let mut record = SessionRecord::new("session-7", "test-user", 0, "group", 0);
        record.lobby_code = Some("TEST-LOBBY".to_owned());
        let pending = GameCommandMessage {
            command_id_client: CommandId {
                tick: snapshot.tick,
                user_id: 7,
                sequence_number: 1,
            },
            command_id_server: None,
            command: GameCommand::Turn {
                snake_id: 0,
                direction: Direction::Up,
            },
        };
        let mut session = LiveSession {
            record,
            user_id: 7,
            token: "test-token".to_owned(),
            socket,
            websocket_url,
            origin: settings.origin.clone(),
            backend_hints: settings.backend_hints.clone(),
            sticky_cookie: None,
            last_lobby_members: BTreeSet::new(),
            last_lobby_state: None,
            recent_events: VecDeque::new(),
            clock_offset_ms: 0,
            last_ping_client_time: None,
            current_task_boot_id: None,
            current_socket_generation: None,
            reconnects: 0,
            client_game_session_id: "test-session-7".to_owned(),
            next_command_sequence: 2,
            pending_commands: BTreeMap::from([(
                1,
                PendingCommand {
                    message: pending,
                    sent_at_unix_ms: unix_time_ms(),
                    sent_at: Instant::now(),
                },
            )]),
            server_capabilities: REQUIRED_SERVER_CAPABILITIES
                .iter()
                .map(|capability| (*capability).to_owned())
                .collect(),
            activity_lease: SessionActivityLease::new(7, activity_sender),
        };
        let mut runtime = GameRuntime::from_snapshot(42, 7, snapshot, 0).unwrap();

        let complete = perform_planned_handoff(
            &mut session,
            &mut runtime,
            &settings,
            42,
            Utc::now().timestamp_millis() + 5_000,
            &CancellationToken::new(),
        )
        .await
        .unwrap();

        assert!(!complete);
        assert_eq!(
            runtime.engine.sync_status().last_stream_seq,
            10,
            "promotion must preserve old events applied after the frozen Pong frontier"
        );
        let mut covered_post_promotion_events = 0_u64;
        while !session.pending_commands.is_empty() {
            let message = tokio::time::timeout(Duration::from_secs(1), session.next_message())
                .await
                .expect("candidate returned terminal command outcomes")
                .unwrap();
            if let WSMessage::GameEvent(event) = message {
                if runtime.suppress_covered_promotion_event(&event) {
                    covered_post_promotion_events += 1;
                    continue;
                }
                if let GameEvent::Snapshot { game_state } = &event.event {
                    runtime
                        .apply_snapshot(42, 7, game_state.clone(), 0)
                        .unwrap();
                }
                runtime.engine.process_server_event(&event).unwrap();
            }
        }
        assert!(session.pending_commands.is_empty());
        assert!(
            covered_post_promotion_events >= 6,
            "the delayed snapshot and replacement suffix through the old watermark must be suppressed"
        );
        assert!(runtime.engine.sync_status().last_stream_seq >= 11);
        assert_eq!(session.record.metrics.planned_handoff_attempts, 1);
        assert_eq!(session.record.metrics.planned_handoff_successes, 1);
        assert_eq!(session.record.metrics.planned_handoff_failures, 0);
        assert_eq!(
            session
                .record
                .diagnostics
                .get("planned_handoff_candidate_failures"),
            None
        );
        assert_eq!(session.record.metrics.planned_handoff_outcome_barriers, 1);
        assert_eq!(
            session.record.metrics.planned_handoff_terminal_completions,
            0
        );
        assert_eq!(session.record.metrics.planned_handoff_continuity_proofs, 1);
        assert!(session.record.metrics.planned_handoff_commands_sent > 0);
        assert!(
            session
                .record
                .metrics
                .scheduled_command_counts_by_unix_second
                .values()
                .sum::<u64>()
                > 0
        );
        assert!(
            session
                .record
                .metrics
                .scheduled_command_counts_by_sent_unix_second
                .values()
                .sum::<u64>()
                > 0
        );
        assert!(
            session
                .record
                .metrics
                .command_outcome_counts_by_sent_unix_second
                .values()
                .sum::<u64>()
                > 0
        );
        assert!(
            session
                .record
                .metrics
                .command_outcome_max_latency_ms_by_sent_unix_second
                .values()
                .next()
                .is_some()
        );
        assert_eq!(session.record.metrics.usable_session_gap_ms, vec![0]);
        assert_eq!(
            session
                .record
                .metrics
                .planned_handoff_active_overlap_ms
                .len(),
            1
        );
        assert!(session.record.metrics.planned_handoff_active_overlap_ms[0] >= 40);
        session.socket.close(None).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn complete_reconnect_snapshot_is_terminal_and_replaces_membership() {
        let initial = duel_snapshot(&[10, 11]);
        let mut runtime = GameRuntime::from_snapshot(42, 10, initial, 0).unwrap();

        let mut completed = duel_snapshot(&[10, 99]);
        completed.status = GameStatus::Complete {
            winning_snake_id: None,
        };

        assert!(runtime.apply_snapshot(42, 10, completed, 0).unwrap());
        assert_eq!(runtime.snapshot_user_ids, BTreeSet::from([10, 99]));
    }

    #[test]
    fn snapshot_identity_requires_the_local_player() {
        let snapshot = duel_snapshot(&[20, 21]);
        let error = snapshot_identity(7, 99, &snapshot).unwrap_err();
        assert!(error.to_string().contains("omitted user 99"));
    }

    #[test]
    fn validation_rejects_a_foreign_player_in_snapshot() {
        let mut first = SessionRecord::new("s1", "u1", 0, "g1", 1);
        first.game_id = Some(42);
        first.complete(2);
        let mut second = SessionRecord::new("s2", "u2", 0, "g1", 1);
        second.game_id = Some(42);
        second.complete(2);
        let played = vec![
            PlayedSession {
                record: first,
                snapshot_user_ids: BTreeSet::from([10, 99]),
            },
            PlayedSession {
                record: second,
                snapshot_user_ids: BTreeSet::from([10, 99]),
            },
        ];

        let result = validate_group(played, &BTreeSet::from([10, 11]), 1);

        assert!(result.pairing_violation.is_some());
        assert!(result.sessions.iter().all(SessionRecord::is_failed));
    }
}
