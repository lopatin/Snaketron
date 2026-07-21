//! One coordinated match group and its virtual-user sessions.

use crate::config::CommandProfile;
use crate::report::{
    SessionFailureRecord, SessionLifecycleRecord, SessionOutcome, SessionPhase, SessionRecord,
    unix_time_ms,
};
use crate::target::BackendHintRegistry;
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use common::{
    Direction, GameCommand, GameEngine, GameEvent, GameState, GameStatus, GameType, QueueMode,
    calculate_ai_move,
};
use futures_util::{SinkExt, StreamExt, future::join_all};
use reqwest::{Client, Url};
use serde::Deserialize;
use server::lobby_manager::LobbyPreferences;
use server::ws_server::WSMessage;
use std::collections::{BTreeSet, VecDeque};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::{Barrier, mpsc, watch};
use tokio::time::{Interval, MissedTickBehavior, interval, interval_at};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{COOKIE, ORIGIN};
use tokio_tungstenite::tungstenite::http::{HeaderMap, Request};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tokio_util::sync::CancellationToken;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

const PING_INTERVAL: Duration = Duration::from_secs(5);
const RECONNECT_DELAY: Duration = Duration::from_secs(2);
const MAX_RECONNECTS: u32 = 2;
const RECENT_EVENT_LIMIT: usize = 32;
const GAME_TIMEOUT_MARGIN: Duration = Duration::from_secs(45);

#[derive(Debug, Clone)]
pub struct SessionSettings {
    pub api_origin: Url,
    pub websocket_url: Url,
    pub origin: String,
    pub game_type: GameType,
    pub queue_mode: QueueMode,
    pub selected_mode: String,
    pub competitive: bool,
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
/// The coordinator uses this to maintain token-sent logical-session concurrency,
/// including across the short reconnect gaps within one session lifecycle.
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
    recent_events: VecDeque<String>,
    clock_offset_ms: i64,
    reconnects: u32,
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
        })
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
    let expected_game_count = 1;
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
    let connect_started = Instant::now();
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
            settings.connect_timeout,
            &settings.backend_hints,
            None,
        ) => result,
    };
    let (socket, backend, sticky_cookie) = match connect_result {
        Ok(value) => value,
        Err(error) => {
            fail_record(
                &mut record,
                SessionPhase::WebSocketConnect,
                format!("{error:#}"),
            );
            return Err(record);
        }
    };
    record.metrics.websocket_connect_ms = Some(elapsed_ms(connect_started));
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
        recent_events: VecDeque::new(),
        clock_offset_ms: 0,
        reconnects: 0,
        activity_lease,
    };

    let token = session.token.clone();
    if let Err(error) = session
        .send_cancellable(WSMessage::Token(token), cancellation)
        .await
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
            .with_message("token sent; awaiting ordered clock-sync pong"),
    );
    session.activity_lease.mark_connected();
    if let Err(error) =
        send_tagged_ping_and_wait(&mut session, None, settings.connect_timeout, cancellation).await
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
            recover_pre_game_socket(session, settings, cancellation, error).await?;
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
                recover_pre_game_socket(session, settings, cancellation, error).await?;
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
    loop {
        recover_pre_game_socket(session, settings, cancellation, cause).await?;
        session.last_lobby_members.clear();

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
        return Ok(());
    }
}

async fn recover_pre_game_socket(
    session: &mut LiveSession,
    settings: &SessionSettings,
    cancellation: &CancellationToken,
    cause: anyhow::Error,
) -> Result<()> {
    session.record.metrics.disconnects = session.record.metrics.disconnects.saturating_add(1);
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
                        ping_interval = interval(PING_INTERVAL);
                        ping_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
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
                        ping_interval = interval(PING_INTERVAL);
                        ping_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                    }
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
                    WSMessage::GameLoadFailed { game_id: failed, reason } if failed == game_id => {
                        return Err(anyhow!("server could not load matched game {failed}: {reason}"));
                    }
                    WSMessage::AccessDenied { reason } => {
                        return Err(anyhow!("server denied game session: {reason}"));
                    }
                    WSMessage::Shutdown => {
                        let snapshot_complete = synchronize_game_runtime(
                            session,
                            &mut runtime,
                            settings,
                            game_id,
                            cancellation,
                            anyhow!("server requested WebSocket shutdown during game {game_id}"),
                            "shutdown recovery snapshot synchronized",
                        )
                        .await?;
                        if snapshot_complete {
                            session.record.metrics.game_duration_ms = Some(elapsed_ms(game_started));
                            session.record.complete(unix_time_ms());
                            return Ok(runtime.snapshot_user_ids.clone());
                        }
                        ping_interval = interval(PING_INTERVAL);
                        ping_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
                    }
                    WSMessage::ServerShutdown { reason, grace_period_seconds } => {
                        let snapshot_complete = synchronize_game_runtime(
                            session,
                            &mut runtime,
                            settings,
                            game_id,
                            cancellation,
                            anyhow!(
                                "server shutdown during game {game_id}: {reason} (grace {grace_period_seconds}s)"
                            ),
                            "server-shutdown recovery snapshot synchronized",
                        )
                        .await?;
                        if snapshot_complete {
                            session.record.metrics.game_duration_ms = Some(elapsed_ms(game_started));
                            session.record.complete(unix_time_ms());
                            return Ok(runtime.snapshot_user_ids.clone());
                        }
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
    let client_time = previous_ping.map_or_else(
        || Utc::now().timestamp_millis(),
        |previous| {
            Utc::now()
                .timestamp_millis()
                .max(previous.saturating_add(1))
        },
    );
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
            WSMessage::Shutdown => {
                return Err(anyhow!("server closed the connection before the pong"));
            }
            WSMessage::ServerShutdown { reason, .. } => {
                return Err(anyhow!("server shutdown before the pong: {reason}"));
            }
            _ => {}
        }
    }
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
            WSMessage::AccessDenied { reason } => {
                return Err(SnapshotWaitError::Fatal(anyhow!(
                    "server denied game session: {reason}"
                )));
            }
            WSMessage::Shutdown => {
                return Err(SnapshotWaitError::Retryable(anyhow!(
                    "server requested WebSocket shutdown while loading game {game_id}"
                )));
            }
            WSMessage::ServerShutdown {
                reason,
                grace_period_seconds,
            } => {
                return Err(SnapshotWaitError::Retryable(anyhow!(
                    "server shutdown while loading game {game_id}: {reason} (grace {grace_period_seconds}s)"
                )));
            }
            _ => {}
        }
    }
}

async fn recover_game_snapshot(
    session: &mut LiveSession,
    settings: &SessionSettings,
    game_id: u32,
    cancellation: &CancellationToken,
    cause: anyhow::Error,
) -> Result<GameState> {
    session.record.metrics.disconnects = session.record.metrics.disconnects.saturating_add(1);
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

        match wait_for_game_snapshot(session, game_id, settings.connect_timeout, cancellation).await
        {
            Ok(game_state) => return Ok(game_state),
            Err(SnapshotWaitError::Retryable(error)) => {
                session.record.metrics.disconnects =
                    session.record.metrics.disconnects.saturating_add(1);
                last_error = error;
            }
            Err(SnapshotWaitError::Fatal(error)) => return Err(error),
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
        .send(WSMessage::GameCommand(command))
        .await
        .map_err(|error| {
            DriveAiError::Transport(error.context("sending AI game command over WebSocket"))
        })?;
    session.record.metrics.commands_sent = session.record.metrics.commands_sent.saturating_add(1);
    if !command_profile.sends_unchanged_turns() {
        *pending_direction = Some(direction);
    }
    Ok(())
}

impl LiveSession {
    async fn send(&mut self, message: WSMessage) -> Result<()> {
        let kind = message_kind(&message);
        let payload = serde_json::to_string(&message)?;
        self.socket.send(Message::Text(payload)).await?;
        self.record.metrics.messages_sent = self.record.metrics.messages_sent.saturating_add(1);
        self.remember(format!("sent:{kind}"));
        Ok(())
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
                    self.record.metrics.messages_received =
                        self.record.metrics.messages_received.saturating_add(1);
                    let kind = message_kind(&message);
                    self.remember(format!("received:{kind}"));
                    if let WSMessage::LobbyUpdate { members, .. } = &message {
                        self.last_lobby_members =
                            members.iter().map(|member| member.user_id).collect();
                    }
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
                        WSMessage::Shutdown => {
                            return Err(PreGameWaitError::Recoverable(anyhow!(
                                "server requested WebSocket shutdown"
                            )));
                        }
                        WSMessage::ServerShutdown { reason, grace_period_seconds } => {
                            return Err(PreGameWaitError::Recoverable(anyhow!(
                                "server shutdown: {reason} (grace {grace_period_seconds}s)"
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
        let token = self.token.clone();
        self.send_cancellable(WSMessage::Token(token), cancellation)
            .await?;
        Ok(())
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
        if !self.recent_events.is_empty() {
            self.record.diagnostics.insert(
                "recent_protocol_events".to_owned(),
                self.recent_events.into_iter().collect::<Vec<_>>().join(","),
            );
        }
        self.record
    }
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
        WSMessage::GameCommand(_) => "GameCommand",
        WSMessage::GameEvent(_) => "GameEvent",
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
        WSMessage::Shutdown => "Shutdown",
        WSMessage::ServerShutdown { .. } => "ServerShutdown",
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

fn bounded(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

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
            recent_events: VecDeque::new(),
            clock_offset_ms: 0,
            reconnects: 0,
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
            recent_events: VecDeque::new(),
            clock_offset_ms: 0,
            reconnects: 0,
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
        recovery_server.await.unwrap();
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
