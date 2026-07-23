use crate::api::auth::validate_username;
use crate::cluster_membership::ClusterNamespace;
use crate::db::Database;
use crate::game_bus::GameBus;
use crate::game_executor::PARTITION_COUNT;
use crate::game_executor::StreamEvent;
use crate::lifecycle::{DrainNotice, TaskLifecycle, WS_PROTOCOL_VERSION};
use crate::lobby_manager;
use crate::lobby_manager::{LeaveLobbyResult, LobbyJoinHandle, LobbyMember};
use crate::matchmaking_manager::MatchmakingManager;
use crate::pubsub_manager::PubSubManager;
use crate::recovery::{
    CommandOutcome, RecoveryEnvelopeV2, ResolvedCommandState, SessionCommandOutcomes,
    validate_client_command_identity,
};
use crate::redis_keys::RedisKeys;
use crate::redis_utils::RedisConnection;
use crate::replication::GameStateReader;
use crate::user_cache::UserCache;
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use common::{
    ClientCommandIdentityV2, GameCommandMessage, GameEvent, GameEventMessage, GameState, GameStatus,
};
use futures_util::SinkExt;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

// Snapshot-bearing messages are serialized envelopes; boxing would add churn without a win.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Serialize, Deserialize)]
pub enum WSMessage {
    Token(String),
    JoinGame(u32),
    LeaveGame,
    /// At-least-once client command. The gateway canonicalizes `game_id` and
    /// `user_id` from the authenticated connection before publishing it.
    GameCommandV2 {
        command_id: ClientCommandIdentityV2,
        command: GameCommandMessage,
    },
    GameEvent(GameEventMessage),
    /// Executor-authored terminal command outcomes adjacent to a fresh
    /// snapshot. This is user/session filtered and never part of shared state.
    CommandOutcomes {
        game_id: u32,
        client_game_session_id: String,
        contiguous_through: u64,
        outcomes: BTreeMap<u64, CommandOutcome>,
    },
    /// Ordered barrier emitted only after every outcome batch for the
    /// immediately preceding snapshot has reached this socket's send queue.
    /// Planned handoff clients use it instead of guessing from timing or from
    /// the absence of a per-session outcome batch.
    CommandOutcomesComplete {
        game_id: u32,
    },
    Chat(String),
    LobbyChatMessage {
        lobby_code: String,
        message_id: String,
        user_id: i32,
        username: String,
        message: String,
        timestamp_ms: i64,
    },
    GameChatMessage {
        game_id: u32,
        message_id: String,
        user_id: i32,
        username: String,
        message: String,
        timestamp_ms: i64,
    },
    LobbyChatHistory {
        lobby_code: String,
        messages: Vec<LobbyChatBroadcast>,
    },
    GameChatHistory {
        game_id: u32,
        messages: Vec<GameChatBroadcast>,
    },
    /// Server -> client acknowledgement sent only after token verification and
    /// user loading have completed.
    Authenticated {
        task_boot_id: String,
        protocol_version: u16,
        capabilities: Vec<String>,
        socket_generation: u64,
    },
    /// Client -> server: the client detected message loss or state divergence
    /// (stream_seq gap, repeated TickHash mismatch, or a silent feed) and
    /// needs its event subscription restarted with a fresh snapshot.
    RequestResync {
        game_id: u32,
    },
    Ping {
        client_time: i64,
    },
    Pong {
        client_time: i64,
        server_time: i64,
    },
    // Matchmaking messages
    QueueForMatch {
        game_type: common::GameType,
        queue_mode: common::QueueMode, // Quickmatch or Competitive
    },
    QueueForMatchMulti {
        game_types: Vec<common::GameType>,
        queue_mode: common::QueueMode, // Quickmatch or Competitive
    },
    LeaveQueue,
    // Real-time matchmaking updates
    MatchFound {
        game_id: u32,
    },
    QueueUpdate {
        position: u32,
        estimated_wait_seconds: u32,
    },
    QueueLeft,
    UpdateNickname {
        nickname: String,
    },
    SpectatorJoined,
    AccessDenied {
        reason: String,
    },
    GameLoadFailed {
        game_id: u32,
        reason: String,
    },
    /// The game is known, but this ready gateway's local replica is still
    /// warming through an executor ownership gap. Clients retry the same join
    /// without surfacing a terminal error.
    GameWarming {
        game_id: u32,
        retry_after_ms: u64,
    },
    // Solo game responses
    SoloGameCreated {
        game_id: u32,
    },
    // Planned gateway handoff. Executor ownership is intentionally absent:
    // the replacement connection uses the same regional URL.
    Drain {
        task_boot_id: String,
        deadline_unix_ms: i64,
    },
    // Region user count updates
    UserCountUpdate {
        region_counts: std::collections::HashMap<String, u32>,
    },
    // Lobby messages
    CreateLobby,
    LobbyCreated {
        lobby_code: String,
    },
    JoinLobby {
        lobby_code: String,
        preferences: Option<lobby_manager::LobbyPreferences>,
    },
    JoinedLobby {
        lobby_code: String,
    },
    LeaveLobby,
    LeftLobby,
    LobbyUpdate {
        lobby_code: String,
        members: Vec<lobby_manager::LobbyMember>,
        host_user_id: i32,
        state: String,
        preferences: lobby_manager::LobbyPreferences,
    },
    UpdateLobbyPreferences {
        selected_modes: Vec<String>,
        competitive: bool,
    },
    LobbyRegionMismatch {
        target_region: String,
        ws_url: String,
        lobby_code: String,
    },
    // NicknameUpdated {
    //     username: String,
    // },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserToken {
    pub user_id: i32,
    pub username: String,
    pub is_guest: bool,
}

// Player metadata to store additional user information
#[derive(Debug, Clone)]
pub struct PlayerMetadata {
    pub user_id: i32,
    pub username: String,
    pub token: String,
    pub is_guest: bool,
}

const MAX_CHAT_MESSAGE_LENGTH: usize = 200;
const CHAT_HISTORY_LIMIT: usize = 200;
const LOBBY_MATCH_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(5);
const LOBBY_MATCH_SUBSCRIBE_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LobbyChatBroadcast {
    lobby_code: String,
    message_id: String,
    user_id: i32,
    username: String,
    message: String,
    timestamp_ms: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct GameChatBroadcast {
    game_id: u32,
    message_id: String,
    user_id: i32,
    username: String,
    message: String,
    timestamp_ms: i64,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum LobbyMatchHint {
    MatchFound {
        game_id: u32,
        #[serde(default)]
        partition_id: Option<u32>,
    },
}

async fn handle_guest_nickname_update(
    db: &Arc<dyn Database>,
    lobby_manager: &Arc<crate::lobby_manager::LobbyManager>,
    user_cache: UserCache,
    lobby: &Option<LobbyJoinHandle>,
    metadata: &PlayerMetadata,
    ws_tx: &mpsc::Sender<Message>,
    nickname: String,
) -> Result<()> {
    let trimmed = nickname.trim().to_string();

    let validation_errors = validate_username(&trimmed);
    if !validation_errors.is_empty() {
        let response = WSMessage::AccessDenied {
            reason: format!("Invalid nickname: {}", validation_errors.join(", ")),
        };
        let json_msg = serde_json::to_string(&response)?;
        ws_tx.send(Message::Text(json_msg.into())).await?;
        return Ok(());
    }

    if !metadata.is_guest {
        let response = WSMessage::AccessDenied {
            reason: "Only guest users can change their nickname".to_string(),
        };
        let json_msg = serde_json::to_string(&response)?;
        ws_tx.send(Message::Text(json_msg.into())).await?;
        return Ok(());
    }

    db.update_guest_username(metadata.user_id, &trimmed).await?;
    user_cache
        .remove_from_redis(metadata.user_id as u32)
        .await?;

    if let Some(lobby) = lobby {
        lobby_manager
            .publish_lobby_update(&lobby.lobby_code)
            .await?;
    }

    Ok(())
}

// JWT verification trait for dependency injection
#[async_trait::async_trait]
pub trait JwtVerifier: Send + Sync {
    async fn verify(&self, token: &str) -> Result<UserToken>;
}

// Test implementation that accepts any token and creates users as needed
pub struct TestJwtVerifier {
    db: Arc<dyn Database>,
}

impl TestJwtVerifier {
    pub fn new(db: Arc<dyn Database>) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl JwtVerifier for TestJwtVerifier {
    async fn verify(&self, token: &str) -> Result<UserToken> {
        // In test mode, accept any token and create user if needed
        // Extract username from token or use default
        let username = if token.starts_with("test-token-") {
            format!(
                "test_user_{}",
                token.strip_prefix("test-token-").unwrap_or("default")
            )
        } else {
            "test_user_default".to_string()
        };

        // Try to find existing user first
        let existing_user = self.db.get_user_by_username(&username).await?;

        let user_id = match existing_user {
            Some(user) => user.id,
            None => {
                // Create new test user
                let new_user = self
                    .db
                    .create_user(&username, "test_password_hash", 1000)
                    .await?;
                info!("Created test user {} with ID {}", username, new_user.id);
                new_user.id
            }
        };

        Ok(UserToken {
            user_id,
            username: username.clone(),
            is_guest: false,
        })
    }
}

// Connection state machine - simplified to 2 states
enum ConnectionState {
    // Initial state - waiting for authentication
    Unauthenticated,

    // Authenticated state with optional context (lobby, game)
    Authenticated {
        metadata: PlayerMetadata,
        lobby_handle: Option<LobbyJoinHandle>,
        game_id: Option<u32>, // Some when user is in a game
        websocket_id: String, // Unique ID for this websocket connection
    },
}

fn queue_planned_drain_notice(
    drain_tx: &mpsc::Sender<Message>,
    notice: &DrainNotice,
) -> Result<()> {
    let message = WSMessage::Drain {
        task_boot_id: notice.task_boot_id.clone(),
        deadline_unix_ms: notice.deadline_unix_ms,
    };
    drain_tx
        .try_send(Message::Text(serde_json::to_string(&message)?.into()))
        .context("WebSocket drain control channel unavailable")
}

/// Receive control traffic ahead of the bounded gameplay queue. The sink
/// remains owned by one task, so this changes only queueing priority: at most
/// the single frame already being written can precede a drain notice.
async fn next_outbound_message(
    drain_rx: &mut mpsc::Receiver<Message>,
    ws_rx: &mut mpsc::Receiver<Message>,
    drain_open: &mut bool,
    ws_open: &mut bool,
) -> Option<Message> {
    loop {
        if !*drain_open && !*ws_open {
            return None;
        }

        tokio::select! {
            biased;
            message = drain_rx.recv(), if *drain_open => {
                match message {
                    Some(message) => return Some(message),
                    None => *drain_open = false,
                }
            }
            message = ws_rx.recv(), if *ws_open => {
                match message {
                    Some(message) => return Some(message),
                    None => *ws_open = false,
                }
            }
        }
    }
}

/// Handle WebSocket connection from Axum
#[allow(clippy::too_many_arguments)]
pub async fn handle_websocket(
    socket: axum::extract::ws::WebSocket,
    db: Arc<dyn Database>,
    user_cache: UserCache,
    jwt_verifier: Arc<dyn JwtVerifier>,
    redis: RedisConnection,
    redis_url: String,
    pubsub_manager: Arc<PubSubManager>,
    game_bus: Arc<GameBus>,
    matchmaking_manager: Arc<Mutex<MatchmakingManager>>,
    replication_manager: Arc<crate::replication::ReplicationManager>,
    cancellation_token: CancellationToken,
    lobby_manager: Arc<crate::lobby_manager::LobbyManager>,
    region: String,
    lifecycle: TaskLifecycle,
    cluster_namespace: ClusterNamespace,
) {
    info!("New WebSocket connection established");

    // Process the WebSocket connection
    if let Err(e) = handle_websocket_connection(
        socket,
        db,
        user_cache.clone(),
        pubsub_manager,
        game_bus,
        matchmaking_manager,
        jwt_verifier,
        cancellation_token,
        replication_manager,
        redis,
        redis_url,
        lobby_manager,
        region,
        lifecycle,
        cluster_namespace,
    )
    .await
    {
        error!("WebSocket connection error: {}", e);
    }
}

/// Internal function to handle the WebSocket connection logic
#[allow(clippy::too_many_arguments)]
async fn handle_websocket_connection(
    ws_stream: axum::extract::ws::WebSocket,
    db: Arc<dyn Database>,
    user_cache: UserCache,
    pubsub_manager: Arc<PubSubManager>,
    game_bus: Arc<GameBus>,
    matchmaking_manager: Arc<Mutex<MatchmakingManager>>,
    jwt_verifier: Arc<dyn JwtVerifier>,
    cancellation_token: CancellationToken,
    replication_manager: Arc<crate::replication::ReplicationManager>,
    redis: RedisConnection,
    redis_url: String,
    lobby_manager: Arc<crate::lobby_manager::LobbyManager>,
    region: String,
    lifecycle: TaskLifecycle,
    cluster_namespace: ClusterNamespace,
) -> Result<()> {
    // Split the WebSocket into send and receive parts using futures_util
    let (mut ws_sink, mut ws_stream) = futures_util::StreamExt::split(ws_stream);

    // Gameplay and ordinary protocol traffic retain the existing bounded
    // backpressure queue. Drain has a one-slot priority path so a saturated
    // gameplay queue cannot consume the handoff window before the client sees
    // the notice.
    let (ws_tx, mut ws_rx) = mpsc::channel::<Message>(1024);
    let (drain_tx, mut priority_drain_rx) = mpsc::channel::<Message>(1);

    // Generate a unique websocket ID for this connection
    let websocket_id = uuid::Uuid::new_v4().to_string();
    let socket_generation = lifecycle.next_socket_generation();
    let mut drain_rx = lifecycle.subscribe_to_drain();

    // Start in unauthenticated state
    let mut state = ConnectionState::Unauthenticated;

    // Create a shutdown timeout that starts as a never-completing future
    let shutdown_timeout = tokio::time::sleep(Duration::from_secs(u64::MAX));
    tokio::pin!(shutdown_timeout);
    let mut shutdown_started = false;

    // Will be used to track Redis stream subscription for game events
    let mut game_event_handle: Option<JoinHandle<()>> = None;
    // Rate limit for client-initiated resyncs (RequestResync).
    let mut last_resync_at: Option<tokio::time::Instant> = None;

    // Will be used to track lobby update forwarding to the websocket
    let mut lobby_update_handle: Option<JoinHandle<()>> = None;

    // Will be used to track Redis pub/sub subscription for lobby match notifications
    let mut lobby_match_handle: Option<JoinHandle<()>> = None;

    // Will be used to track lobby chat subscription
    let mut lobby_chat_handle: Option<JoinHandle<()>> = None;

    // Will be used to track game chat subscription
    let mut game_chat_handle: Option<JoinHandle<()>> = None;

    // Spawn task to forward messages from channel to WebSocket
    let forward_task = tokio::spawn(async move {
        let mut drain_open = true;
        let mut ws_open = true;
        while let Some(msg) = next_outbound_message(
            &mut priority_drain_rx,
            &mut ws_rx,
            &mut drain_open,
            &mut ws_open,
        )
        .await
        {
            // Convert to Axum WebSocket message
            let axum_msg = match msg {
                Message::Text(text) => axum::extract::ws::Message::Text(text.to_string()),
                Message::Binary(bin) => axum::extract::ws::Message::Binary(bin.to_vec()),
                Message::Ping(data) => axum::extract::ws::Message::Ping(data.to_vec()),
                Message::Pong(data) => axum::extract::ws::Message::Pong(data.to_vec()),
                Message::Close(frame) => {
                    let close = frame.map(|f| axum::extract::ws::CloseFrame {
                        code: f.code.into(),
                        reason: f.reason.to_string().into(),
                    });
                    axum::extract::ws::Message::Close(close)
                }
                _ => continue,
            };

            if let Err(e) = ws_sink.send(axum_msg).await {
                error!("Failed to send message to WebSocket: {}", e);
                break;
            }
        }
    });

    // Spawn task to subscribe to user count updates and forward to client
    let ws_tx_for_counts = ws_tx.clone();
    let pubsub_manager_for_counts = pubsub_manager.clone();
    let _user_count_task = tokio::spawn(async move {
        if let Err(e) =
            subscribe_to_user_count_updates(pubsub_manager_for_counts, ws_tx_for_counts).await
        {
            error!("User count subscription task failed: {}", e);
        }
    });

    // A WebSocket can pass the readiness check immediately before the task
    // flips to draining and finish upgrading after the broadcast. Replay the
    // process-local notice so that narrow race cannot leave a late socket on
    // the departing task until forced termination.
    if let Some(notice) = lifecycle.current_drain_notice() {
        let remaining_ms = notice
            .deadline_unix_ms
            .saturating_sub(Utc::now().timestamp_millis())
            .max(1) as u64;
        shutdown_timeout
            .as_mut()
            .reset(tokio::time::Instant::now() + Duration::from_millis(remaining_ms));
        shutdown_started = true;
        queue_planned_drain_notice(&drain_tx, &notice)?;
    }

    loop {
        // let state_name = match &state {
        //     ConnectionState::Unauthenticated => "Unauthenticated".to_string(),
        //     ConnectionState::Authenticated { lobby_code: Some(code), game_id: Some(gid), .. } => {
        //         format!("Authenticated(lobby:{}, game:{})", code, gid)
        //     }
        //     ConnectionState::Authenticated { lobby_code: Some(code), game_id: None, .. } => {
        //         format!("Authenticated(lobby:{})", code)
        //     }
        //     ConnectionState::Authenticated { lobby_code: None, game_id: Some(gid), .. } => {
        //         format!("Authenticated(game:{})", gid)
        //     }
        //     ConnectionState::Authenticated { .. } => "Authenticated".to_string(),
        // };
        // debug!("WS: Select loop iteration, current state: {}", state_name);

        tokio::select! {
            // Handle shutdown timeout
            _ = &mut shutdown_timeout, if shutdown_started => {
                warn!("Shutdown timeout reached, closing connection");
                break;
            }
            // A planned drain is independent from final process cancellation:
            // the old socket remains usable until the replacement is ready.
            notice = drain_rx.recv(), if !shutdown_started => {
                let notice = match notice {
                    Ok(notice) => notice,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                };
                info!("Sending planned drain message to client");
                let remaining_ms = notice.deadline_unix_ms
                    .saturating_sub(Utc::now().timestamp_millis())
                    .max(1) as u64;
                shutdown_timeout.as_mut().reset(
                    tokio::time::Instant::now() + Duration::from_millis(remaining_ms),
                );
                shutdown_started = true;
                if let Err(e) = queue_planned_drain_notice(&drain_tx, &notice) {
                    error!("Failed to queue planned drain message: {}", e);
                }
            }
            // Final cancellation is a fallback for crashes during a planned
            // drain setup. Normal SIGTERM announces through drain_rx first.
            _ = cancellation_token.cancelled(), if !shutdown_started => {
                break;
            }
            // Handle incoming WebSocket messages
            Some(result) = ws_stream.next() => {
                match result {
                    Ok(msg) => {
                        // Convert Axum message to tokio-tungstenite message for processing
                        let tungstenite_msg = match msg {
                            axum::extract::ws::Message::Text(text) => Message::Text(text.into()),
                            axum::extract::ws::Message::Binary(bin) => Message::Binary(bin.into()),
                            axum::extract::ws::Message::Ping(data) => Message::Ping(data.into()),
                            axum::extract::ws::Message::Pong(data) => Message::Pong(data.into()),
                            axum::extract::ws::Message::Close(_frame) => {
                                info!("Client initiated close");
                                break;
                            }
                        };

                        // Process the message
                        if let Message::Text(text) = tungstenite_msg {
                            match serde_json::from_str::<WSMessage>(&text) {
                                Ok(WSMessage::RequestResync { game_id: resync_game_id }) => {
                                    // The client detected loss or divergence (stream
                                    // gap, repeated fingerprint mismatch, or a dead
                                    // feed). Restart its event forwarder — which
                                    // sends a fresh watermarked snapshot as its
                                    // first message — instead of trusting whatever
                                    // subscription state it had. Rate-limited so a
                                    // stuck client cannot spam resubscriptions.
                                    let in_this_game = matches!(
                                        &state,
                                        ConnectionState::Authenticated { game_id: Some(g), .. } if *g == resync_game_id
                                    );
                                    let now = tokio::time::Instant::now();
                                    let allowed = last_resync_at
                                        .map(|t| now.duration_since(t) >= Duration::from_millis(500))
                                        .unwrap_or(true);
                                    if in_this_game && allowed {
                                        last_resync_at = Some(now);
                                        if let ConnectionState::Authenticated { metadata, .. } = &state {
                                            info!(
                                                "Resync requested by user {} for game {}; restarting event subscription",
                                                metadata.user_id, resync_game_id
                                            );
                                            if let Some(handle) = game_event_handle.take() {
                                                handle.abort();
                                            }
                                            let user_id = metadata.user_id as u32;
                                            let ws_tx_clone = ws_tx.clone();
                                            let replication_manager_clone = replication_manager.clone();
                                            let db_clone = db.clone();
                                            let game_bus_clone = game_bus.clone();
                                            let cluster_namespace_clone = cluster_namespace.clone();
                                            game_event_handle = Some(tokio::spawn(async move {
                                                subscribe_to_game_events(
                                                    resync_game_id,
                                                    user_id,
                                                    ws_tx_clone,
                                                    replication_manager_clone,
                                                    db_clone,
                                                    game_bus_clone,
                                                    cluster_namespace_clone,
                                                ).await;
                                            }));
                                        }
                                    } else if !in_this_game {
                                        debug!(
                                            "Ignoring resync request for game {} from connection not in that game",
                                            resync_game_id
                                        );
                                    }
                                }
                                Ok(ws_message) => {
                                    // Check state before consuming it
                                    let was_in_game = matches!(&state, ConnectionState::Authenticated { game_id: Some(_), .. });
                                    let was_in_lobby = matches!(&state, ConnectionState::Authenticated { lobby_handle: Some(_), .. });
                                    // Keep the requested id so a denied switch does not look like a
                                    // successful re-entry into the connection's previously authorized
                                    // game. Successful JoinGame retries still restart subscriptions.
                                    let requested_game_id = match &ws_message {
                                        WSMessage::JoinGame(game_id) => Some(*game_id),
                                        _ => None,
                                    };

                                    match process_ws_message(
                                        state,
                                        ws_message,
                                        &jwt_verifier,
                                        &db,
                                        user_cache.clone(),
                                        &ws_tx,
                                        &game_bus,
                                        &matchmaking_manager,
                                        &replication_manager,
                                        &redis,
                                        &redis_url,
                                        &lobby_manager,
                                        &websocket_id,
                                        &region,
                                        &lifecycle,
                                        socket_generation,
                                        &cluster_namespace,
                                    ).await {
                                        Ok(new_state) => {
                                            // Check if we're entering a game or lobby
                                            let entered_game_id = match &new_state {
                                                ConnectionState::Authenticated { game_id, .. } => *game_id,
                                                ConnectionState::Unauthenticated => None,
                                            };
                                            let entering_game = match requested_game_id {
                                                Some(requested_game_id) => {
                                                    entered_game_id == Some(requested_game_id)
                                                }
                                                None => entered_game_id.is_some() && !was_in_game,
                                            };
                                            let entering_lobby = matches!(&new_state, ConnectionState::Authenticated { lobby_handle: Some(_), .. }) && !was_in_lobby;
                                            let leaving_lobby = was_in_lobby && !matches!(&new_state, ConnectionState::Authenticated { lobby_handle: Some(_), .. });
                                            let leaving_game = was_in_game && !matches!(&new_state, ConnectionState::Authenticated { game_id: Some(_), .. });
                                            debug!("State transitioned to: entering_game: {}, entering_lobby: {}, leaving_lobby: {}, leaving_game: {}",
                                                entering_game, entering_lobby, leaving_lobby, leaving_game);

                                            // Handle state transitions
                                            if entering_game
                                                && let ConnectionState::Authenticated { game_id: Some(game_id), metadata, .. } = &new_state {
                                                    // Subscribe to game events if entering a game
                                                    if let Some(handle) = game_event_handle.take() {
                                                        handle.abort();
                                                    }
                                                    if let Some(handle) = game_chat_handle.take() {
                                                        handle.abort();
                                                    }

                                                    let game_id = *game_id;
                                                    let user_id = metadata.user_id as u32;
                                                    let ws_tx_clone = ws_tx.clone();
                                                    let replication_manager_clone = replication_manager.clone();
                                                    let db_clone = db.clone();
                                                    let game_bus_clone = game_bus.clone();
                                                    let cluster_namespace_clone = cluster_namespace.clone();

                                                    game_event_handle = Some(tokio::spawn(async move {
                                                        subscribe_to_game_events(
                                                            game_id,
                                                            user_id,
                                                            ws_tx_clone,
                                                            replication_manager_clone,
                                                            db_clone,
                                                            game_bus_clone,
                                                            cluster_namespace_clone,
                                                        ).await;
                                                    }));

                                                    let ws_tx_clone = ws_tx.clone();
                                                    let pubsub_manager_clone = pubsub_manager.clone();

                                                    game_chat_handle = Some(tokio::spawn(async move {
                                                        if let Err(e) = subscribe_to_game_chat(
                                                            game_id,
                                                            pubsub_manager_clone,
                                                            ws_tx_clone,
                                                        )
                                                        .await
                                                        {
                                                            error!("Game chat subscription failed: {}", e);
                                                        }
                                                    }));

                                                    match load_game_chat_history(redis.clone(), game_id).await {
                                                        Ok(history) if !history.is_empty() => {
                                                            let history_message = WSMessage::GameChatHistory {
                                                                game_id,
                                                                messages: history,
                                                            };
                                                            match serde_json::to_string(&history_message) {
                                                                Ok(json) => {
                                                                    if let Err(e) = ws_tx
                                                                        .send(Message::Text(json.into()))
                                                                        .await
                                                                    {
                                                                        debug!(
                                                                            "Failed to send initial game chat history for game {}: {}",
                                                                            game_id, e
                                                                        );
                                                                    }
                                                                }
                                                                Err(e) => {
                                                                    error!(
                                                                        "Failed to serialize game chat history for game {}: {}",
                                                                        game_id, e
                                                                    );
                                                                }
                                                            }
                                                        }
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            warn!(
                                                                "Failed to load game chat history for game {}: {}",
                                                                game_id, e
                                                            );
                                                        }
                                                    }
                                                }

                                            // Handle lobby state transitions
                                            if entering_lobby
                                                && let ConnectionState::Authenticated { lobby_handle: Some(lobby_handle), .. } = &new_state {
                                                    if let Some(handle) = lobby_update_handle.take() {
                                                        handle.abort();
                                                    }
                                                    if let Some(handle) = lobby_chat_handle.take() {
                                                        handle.abort();
                                                    }

                                                    let mut lobby_rx = lobby_handle.rx.resubscribe();
                                                    let lobby_code_for_updates = lobby_handle.lobby_code.clone();
                                                    let lobby_code_for_match = lobby_handle.lobby_code.clone();
                                                    let ws_tx_clone = ws_tx.clone();
                                                    let cancellation_token_clone = cancellation_token.clone();

                                                    lobby_update_handle = Some(tokio::spawn(async move {
                                                        loop {
                                                            tokio::select! {
                                                                _ = cancellation_token_clone.cancelled() => {
                                                                    debug!("Lobby update task cancelled for lobby {}", lobby_code_for_updates);
                                                                    break;
                                                                }
                                                                update = lobby_rx.recv() => {
                                                                    match update {
                                                                        Ok(lobby) => {
                                                                            debug!("Received lobby update for lobby {}", lobby.lobby_code);
                                                                            let ws_message = WSMessage::LobbyUpdate {
                                                                                lobby_code: lobby.lobby_code,
                                                                                members: lobby.members.into_values().collect(),
                                                                                host_user_id: lobby.host_user_id,
                                                                                state: lobby.state,
                                                                                preferences: lobby.preferences,
                                                                            };

                                                                            let json_msg = match serde_json::to_string(&ws_message) {
                                                                                Ok(json) => json,
                                                                                Err(e) => {
                                                                                    error!("Failed to serialize lobby update: {}", e);
                                                                                    continue;
                                                                                }
                                                                            };

                                                                            if ws_tx_clone.send(Message::Text(json_msg.into())).await.is_err() {
                                                                                debug!("WebSocket channel closed while sending lobby update for {}", lobby_code_for_updates);
                                                                                break;
                                                                            }
                                                                        }
                                                                        Err(broadcast::error::RecvError::Closed) => {
                                                                            debug!("Lobby update channel closed for lobby {}", lobby_code_for_updates);
                                                                            break;
                                                                        }
                                                                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                                                            warn!("Missed {} lobby updates for lobby {}", skipped, lobby_code_for_updates);
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }));

                                                    // Subscribe to lobby match notifications
                                                    if let Some(handle) = lobby_match_handle.take() {
                                                        handle.abort();
                                                    }

                                                    let ws_tx_clone_for_match = ws_tx.clone();
                                                    let pubsub_manager_clone_for_match = pubsub_manager.clone();
                                                    let redis_clone_for_match = redis.clone();
                                                    let cancellation_token_clone_for_match = cancellation_token.clone();

                                                    lobby_match_handle = Some(tokio::spawn(async move {
                                                        subscribe_to_lobby_match_notifications(
                                                            lobby_code_for_match,
                                                            pubsub_manager_clone_for_match,
                                                            redis_clone_for_match,
                                                            ws_tx_clone_for_match,
                                                            cancellation_token_clone_for_match,
                                                        )
                                                        .await;
                                                    }));

                                                    // Subscribe to lobby chat
                                                    let ws_tx_clone = ws_tx.clone();
                                                    let pubsub_manager_clone = pubsub_manager.clone();

                                                    let lobby_code_for_chat = lobby_handle.lobby_code.clone();
                                                    lobby_chat_handle = Some(tokio::spawn(async move {
                                                        if let Err(e) = subscribe_to_lobby_chat(
                                                            lobby_code_for_chat,
                                                            pubsub_manager_clone,
                                                            ws_tx_clone,
                                                        )
                                                        .await
                                                        {
                                                            error!("Lobby chat subscription failed: {}", e);
                                                        }
                                                    }));

                                                    let lobby_code_for_history = lobby_handle.lobby_code.clone();
                                                    match load_lobby_chat_history(redis.clone(), &lobby_code_for_history).await {
                                                        Ok(history) if !history.is_empty() => {
                                                            let history_message = WSMessage::LobbyChatHistory {
                                                                lobby_code: lobby_code_for_history.clone(),
                                                                messages: history,
                                                            };
                                                            match serde_json::to_string(&history_message) {
                                                                Ok(json) => {
                                                                    if let Err(e) = ws_tx
                                                                        .send(Message::Text(json.into()))
                                                                        .await
                                                                    {
                                                                        debug!(
                                                                            "Failed to send initial lobby chat history for lobby '{}': {}",
                                                                            lobby_code_for_history, e
                                                                        );
                                                                    }
                                                                }
                                                                Err(e) => {
                                                                    error!(
                                                                        "Failed to serialize lobby chat history for lobby '{}': {}",
                                                                        lobby_code_for_history, e
                                                                    );
                                                                }
                                                            }
                                                        }
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            warn!(
                                                                "Failed to load lobby chat history for lobby '{}': {}",
                                                                lobby_code_for_history, e
                                                            );
                                                        }
                                                    }
                                                }

                                            // Abort lobby subscription when leaving lobby
                                            // BUT keep lobby_match_handle active if entering Authenticated with a lobby_code (for Play Again notifications)
                                                if leaving_lobby {
                                                    let keep_match_subscription = matches!(&new_state, ConnectionState::Authenticated { lobby_handle: Some(_), .. });

                                                    if let Some(handle) = lobby_update_handle.take() {
                                                        handle.abort();
                                                        debug!("Aborted lobby update subscription");
                                                    }

                                                    // Only abort match notification if NOT entering game with lobby_id
                                                    if !keep_match_subscription
                                                    && let Some(handle) = lobby_match_handle.take() {
                                                        handle.abort();
                                                        debug!("Aborted lobby match notification subscription");
                                                    }

                                                if let Some(handle) = lobby_chat_handle.take() {
                                                    handle.abort();
                                                    debug!("Aborted lobby chat subscription");
                                                }
                                            }

                                            if leaving_game {
                                                if let Some(handle) = game_event_handle.take() {
                                                    handle.abort();
                                                    debug!("Aborted game event subscription");
                                                }
                                                if let Some(handle) = game_chat_handle.take() {
                                                    handle.abort();
                                                    debug!("Aborted game chat subscription");
                                                }
                                            }

                                            state = new_state;
                                        }
                                        Err(e) => {
                                            error!("Error processing message: {}", e);
                                            // State was consumed, need to reset
                                            state = ConnectionState::Unauthenticated;
                                            break;
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to parse WebSocket message: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("WebSocket error: {}", e);
                        break;
                    }
                }
            }
        }
    }

    // Cleanup

    // Transport loss is not an explicit LeaveLobby. Stop heartbeating and let
    // the short presence lease expire; a replacement connection may already
    // have installed a newer websocket-specific presence.
    if let ConnectionState::Authenticated {
        lobby_handle: Some(lobby_handle),
        ..
    } = state
    {
        lobby_handle.detach_transport();
    }

    // Note: Game subscriptions are now handled differently
    // No need to manually close game_handle as it's not part of ConnectionState anymore

    // Abort subscription tasks
    if let Some(handle) = game_event_handle {
        handle.abort();
    }
    if let Some(handle) = lobby_update_handle {
        handle.abort();
    }
    if let Some(handle) = lobby_match_handle {
        handle.abort();
    }
    if let Some(handle) = lobby_chat_handle {
        handle.abort();
    }
    if let Some(handle) = game_chat_handle {
        handle.abort();
    }
    forward_task.abort();

    info!("WebSocket connection closed");
    Ok(())
}

async fn publish_lobby_chat_message(
    mut redis: RedisConnection,
    payload: LobbyChatBroadcast,
) -> Result<()> {
    let channel = RedisKeys::lobby_chat_channel(&payload.lobby_code);
    let history_key = RedisKeys::lobby_chat_history_key(&payload.lobby_code);
    let serialized =
        serde_json::to_string(&payload).context("Failed to serialize lobby chat payload")?;

    redis
        .publish::<_, _, ()>(&channel, serialized.clone())
        .await
        .context("Failed to publish lobby chat message")?;

    let _: i64 = redis
        .rpush(&history_key, serialized.clone())
        .await
        .context("Failed to append lobby chat history")?;
    let start: isize = -(CHAT_HISTORY_LIMIT as isize);
    let _: () = redis
        .ltrim(&history_key, start, -1)
        .await
        .context("Failed to trim lobby chat history")?;
    Ok(())
}

async fn publish_game_chat_message(
    mut redis: RedisConnection,
    payload: GameChatBroadcast,
) -> Result<()> {
    let channel = RedisKeys::game_chat_channel(payload.game_id);
    let history_key = RedisKeys::game_chat_history_key(payload.game_id);
    let serialized =
        serde_json::to_string(&payload).context("Failed to serialize game chat payload")?;

    redis
        .publish::<_, _, ()>(&channel, serialized.clone())
        .await
        .context("Failed to publish game chat message")?;

    let _: i64 = redis
        .rpush(&history_key, serialized.clone())
        .await
        .context("Failed to append game chat history")?;
    let start: isize = -(CHAT_HISTORY_LIMIT as isize);
    let _: () = redis
        .ltrim(&history_key, start, -1)
        .await
        .context("Failed to trim game chat history")?;
    Ok(())
}

async fn queue_existing_lobby_for_game_types(
    lobby_handle: &LobbyJoinHandle,
    game_types: &[common::GameType],
    queue_mode: &common::QueueMode,
    db: &Arc<dyn Database>,
    lobby_manager: &Arc<crate::lobby_manager::LobbyManager>,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
    requesting_user_id: u32,
) -> Result<()> {
    if game_types.is_empty() {
        return Err(anyhow!("Must specify at least one game type to queue"));
    }

    let members_map = lobby_manager
        .get_lobby_members(&lobby_handle.lobby_code)
        .await
        .context("Failed to load lobby members before queueing")?;

    if members_map.is_empty() {
        return Err(anyhow!("Lobby has no active members to queue"));
    }

    let members: Vec<LobbyMember> = members_map.into_values().collect();
    let avg_mmr = compute_lobby_avg_mmr(db, &members).await?;

    let mut mm_guard = matchmaking_manager.lock().await;
    mm_guard
        .add_lobby_to_queue(
            &lobby_handle.lobby_code,
            members,
            avg_mmr,
            game_types.to_vec(),
            queue_mode.clone(),
            requesting_user_id,
        )
        .await
        .context("Failed to add lobby to matchmaking queue")?;
    drop(mm_guard);

    if let Err(error) = lobby_manager
        .publish_lobby_update(&lobby_handle.lobby_code)
        .await
    {
        warn!(
            lobby_code = lobby_handle.lobby_code,
            %error,
            "Failed to publish queued lobby state"
        );
    }

    Ok(())
}

async fn compute_lobby_avg_mmr(db: &Arc<dyn Database>, members: &[LobbyMember]) -> Result<i32> {
    let mut total = 0;
    let mut count = 0;

    for member in members {
        match db.get_user_by_id(member.user_id as i32).await? {
            Some(user) => {
                total += user.mmr;
                count += 1;
            }
            None => {
                warn!(
                    user_id = member.user_id,
                    "Skipping lobby member without DB record while calculating MMR"
                );
            }
        }
    }

    if count == 0 {
        Err(anyhow!(
            "Unable to calculate lobby MMR - no valid members found"
        ))
    } else {
        Ok(total / count)
    }
}

async fn load_lobby_chat_history(
    mut redis: RedisConnection,
    lobby_code: &str,
) -> Result<Vec<LobbyChatBroadcast>> {
    let key = RedisKeys::lobby_chat_history_key(lobby_code);
    let entries: Vec<String> = redis
        .lrange(&key, 0, -1)
        .await
        .context("Failed to load lobby chat history")?;

    let mut messages = Vec::with_capacity(entries.len());
    for entry in entries {
        match serde_json::from_str::<LobbyChatBroadcast>(&entry) {
            Ok(chat) => messages.push(chat),
            Err(e) => {
                warn!(
                    "Failed to deserialize lobby chat history entry for lobby '{}': {}",
                    lobby_code, e
                );
            }
        }
    }

    Ok(messages)
}

async fn load_game_chat_history(
    mut redis: RedisConnection,
    game_id: u32,
) -> Result<Vec<GameChatBroadcast>> {
    let key = RedisKeys::game_chat_history_key(game_id);
    let entries: Vec<String> = redis
        .lrange(&key, 0, -1)
        .await
        .context("Failed to load game chat history")?;

    let mut messages = Vec::with_capacity(entries.len());
    for entry in entries {
        match serde_json::from_str::<GameChatBroadcast>(&entry) {
            Ok(chat) => messages.push(chat),
            Err(e) => {
                warn!(
                    "Failed to deserialize game chat history entry for game {}: {}",
                    game_id, e
                );
            }
        }
    }

    Ok(messages)
}

fn game_state_records_user(game_state: &GameState, user_id: u32) -> bool {
    game_state.players.contains_key(&user_id) || game_state.spectators.contains(&user_id)
}

const COLD_JOIN_WARMUP_TIMEOUT: Duration = Duration::from_secs(4);
const GAME_WARMING_RETRY_MS: u64 = 500;
const GAME_JOIN_AUTHORIZATION_TIMEOUT: Duration = Duration::from_secs(6);
const ACTIVE_GAME_MAPPING_TIMEOUT: Duration = Duration::from_secs(1);
const COMMAND_OUTCOME_LOAD_TIMEOUT: Duration = Duration::from_secs(4);
const COMMAND_OUTCOME_READ_TIMEOUT: Duration = Duration::from_millis(750);
const COMMAND_OUTCOME_RETRY_DELAY: Duration = Duration::from_millis(100);

#[derive(Debug)]
enum GameJoinAuthorizationError {
    /// A dependency or authoritative live-game artifact may still converge.
    /// This maps only to `GameWarming`, never `GameLoadFailed`.
    Warming,
    /// The available authoritative evidence proves this join cannot recover.
    /// This is the only branch that maps to `GameLoadFailed`.
    Denied(String),
}

impl std::fmt::Display for GameJoinAuthorizationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Warming => formatter.write_str("game replica is warming"),
            Self::Denied(reason) => formatter.write_str(reason),
        }
    }
}

fn game_join_denied(reason: impl Into<String>) -> GameJoinAuthorizationError {
    GameJoinAuthorizationError::Denied(reason.into())
}

fn game_join_failure_message(game_id: u32, failure: GameJoinAuthorizationError) -> WSMessage {
    match failure {
        GameJoinAuthorizationError::Warming => WSMessage::GameWarming {
            game_id,
            retry_after_ms: GAME_WARMING_RETRY_MS,
        },
        GameJoinAuthorizationError::Denied(reason) => WSMessage::GameLoadFailed { game_id, reason },
    }
}

fn missing_game_join_failure(
    requested_game_id: u32,
    mapped_game_id: Option<u32>,
) -> GameJoinAuthorizationError {
    if mapped_game_id == Some(requested_game_id) {
        GameJoinAuthorizationError::Warming
    } else {
        game_join_denied("This game was not found or has expired")
    }
}

async fn load_durable_active_game(
    user_id: u32,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
) -> Result<Option<u32>> {
    tokio::time::timeout(ACTIVE_GAME_MAPPING_TIMEOUT, async {
        let mut manager = matchmaking_manager.lock().await;
        manager.get_user_active_game(user_id).await
    })
    .await
    .context("timed out resolving durable active-game mapping")?
    .context("failed to resolve durable active-game mapping")
}

async fn has_durable_recovery_failure(
    game_id: u32,
    game_bus: &Arc<GameBus>,
    cluster_namespace: &ClusterNamespace,
) -> bool {
    match game_bus
        .get_recovery_failure(cluster_namespace, game_id)
        .await
    {
        Ok(Some(_)) => true,
        Ok(None) => false,
        Err(error) => {
            warn!(game_id, %error, "Failed to inspect durable recovery-failure marker");
            false
        }
    }
}

/// A partition-wide snapshot request can be missed while no executor owns the
/// partition. Keep requesting through the bounded takeover window, while the
/// replication manager coalesces concurrent requests from reconnecting users.
async fn wait_for_live_game_after_snapshot_request(
    game_id: u32,
    replication_manager: &Arc<crate::replication::ReplicationManager>,
    game_bus: &Arc<GameBus>,
    cluster_namespace: &ClusterNamespace,
) -> Option<GameState> {
    let partition_id = game_id % PARTITION_COUNT;
    let deadline = tokio::time::Instant::now() + COLD_JOIN_WARMUP_TIMEOUT;
    let mut check_recovery = true;

    loop {
        if let Some(game_state) = replication_manager.get_game_state_when_ready(game_id).await {
            return Some(game_state);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }

        match replication_manager
            .request_partition_snapshots(partition_id)
            .await
        {
            Ok(published) => check_recovery |= published,
            Err(error) => {
                warn!(game_id, partition_id, %error, "Failed to request cold-join snapshots");
            }
        }

        if check_recovery {
            match game_bus.get_recovery(cluster_namespace, game_id).await {
                Ok(Some(envelope)) => return Some(envelope.game_state),
                Ok(None) => {}
                Err(error) => {
                    warn!(game_id, %error, "Failed to load recovery during cold-join warm-up");
                }
            }
            check_recovery = false;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn canonical_command_identity(
    command_id: ClientCommandIdentityV2,
    game_id: u32,
    user_id: u32,
) -> ClientCommandIdentityV2 {
    ClientCommandIdentityV2 {
        game_id,
        user_id,
        client_game_session_id: command_id.client_game_session_id,
        sequence: command_id.sequence,
    }
}

fn snapshot_requires_command_outcomes(event: &GameEvent) -> bool {
    matches!(event, GameEvent::Snapshot { .. })
}

fn command_outcomes_for_user(
    resolved: ResolvedCommandState,
    user_id: u32,
) -> Vec<(String, SessionCommandOutcomes)> {
    let prefix = format!("{user_id}:");
    resolved
        .sessions
        .into_iter()
        .filter_map(|(session_key, outcomes)| {
            let client_game_session_id = session_key.strip_prefix(&prefix)?;
            (!client_game_session_id.is_empty())
                .then(|| (client_game_session_id.to_owned(), outcomes))
        })
        .collect()
}

/// Resolve and authorize a JoinGame request before it changes connection state.
///
/// Live games are authoritative in replication memory. Completed games may instead live in the
/// short Redis reload cache or DynamoDB. Returning success means the requested user was present in
/// the canonical state from one of those sources; callers may then enable game events and chat.
async fn authorize_game_join_inner(
    game_id: u32,
    user_id: u32,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
    replication_manager: &Arc<crate::replication::ReplicationManager>,
    game_bus: &Arc<GameBus>,
    cluster_namespace: &ClusterNamespace,
    db: &Arc<dyn Database>,
) -> std::result::Result<(), GameJoinAuthorizationError> {
    if let Some(game_state) = replication_manager.get_game_state_when_ready(game_id).await {
        if game_state_records_user(&game_state, user_id) {
            return Ok(());
        }

        warn!(
            "Denied live game {} join to user {}: user is not a recorded participant",
            game_id, user_id
        );
        return Err(game_join_denied("This game is unavailable"));
    }

    // Ask the current (or soon-to-be) owner to republish this partition. The
    // startup request may have landed while no executor held the lease, so a
    // cold join must be able to self-heal independently.
    let partition_id = game_id % PARTITION_COUNT;
    if let Err(error) = replication_manager
        .request_partition_snapshots(partition_id)
        .await
    {
        warn!(game_id, partition_id, %error, "Failed to request cold-join snapshots");
    }

    if has_durable_recovery_failure(game_id, game_bus, cluster_namespace).await {
        return Err(game_join_denied(
            crate::recovery::PUBLIC_UNRECOVERABLE_GAME_REASON,
        ));
    }

    // During executor failover the authoritative recovery envelope can be
    // available before this gateway's replica has consumed the takeover
    // snapshot. It is sufficient for participant authorization; the event
    // subscription below still waits for a fresh replica or uses this same
    // recovery snapshot as its bridge.
    match game_bus.get_recovery(cluster_namespace, game_id).await {
        Ok(Some(envelope)) => {
            if game_state_records_user(&envelope.game_state, user_id) {
                return Ok(());
            }
            warn!(
                game_id,
                user_id, "Denied recovery-backed game join to non-participant"
            );
            return Err(game_join_denied("This game is unavailable"));
        }
        Ok(None) => {}
        Err(error) => {
            warn!(game_id, user_id, %error, "Failed to load recovery while authorizing game join");
        }
    }

    let cached_active_state = match replication_manager.get_stored_snapshot(game_id).await {
        Ok(Some(game_state)) if matches!(game_state.status, GameStatus::Complete { .. }) => {
            if game_state_records_user(&game_state, user_id) {
                return Ok(());
            }

            warn!(
                "Denied stored Redis game {} join to user {}: user is not a recorded participant",
                game_id, user_id
            );
            return Err(game_join_denied("This game is unavailable"));
        }
        Ok(Some(cached_game_state)) => Some(cached_game_state),
        Ok(None) => None,
        Err(e) => {
            warn!(
                "Failed to load stored Redis snapshot while authorizing game {}: {}",
                game_id, e
            );
            None
        }
    };

    // Completion persistence can win the race with removal/replacement of the
    // preceding active Redis reload snapshot. A durable terminal state is the
    // authority for a completed game, so do not let that stale cache force the
    // participant into an endless GameWarming retry. Failure, absence, malformed
    // data, or a non-terminal database state is not proof that the live game is
    // gone: retain the normal bounded replica warm-up in all of those cases.
    if cached_active_state.is_some()
        && let Ok(database_game_id) = i32::try_from(game_id)
    {
        match db.get_game_by_id(database_game_id).await {
            Ok(Some(game)) => {
                if let Some(game_state_json) = game.game_state {
                    match serde_json::from_value::<GameState>(game_state_json) {
                        Ok(game_state)
                            if matches!(game_state.status, GameStatus::Complete { .. }) =>
                        {
                            if game_state_records_user(&game_state, user_id) {
                                return Ok(());
                            }
                            warn!(
                                game_id,
                                user_id,
                                "Denied durable completed-game join to non-participant while Redis held a stale active snapshot"
                            );
                            return Err(game_join_denied("This game is unavailable"));
                        }
                        Ok(_) => {}
                        Err(error) => warn!(
                            game_id,
                            user_id,
                            %error,
                            "Ignoring malformed durable game state while warming a cached active game"
                        ),
                    }
                }
            }
            Ok(None) => {}
            Err(error) => warn!(
                game_id,
                user_id,
                %error,
                "Durable game lookup failed while warming a cached active game"
            ),
        }
    }

    // Repeat the request while waiting: a request written during the lease gap
    // is intentionally not relied upon. This also covers the short interval
    // after atomic matchmaking commit but before GameCreated is consumed.
    if let Some(live_game_state) = wait_for_live_game_after_snapshot_request(
        game_id,
        replication_manager,
        game_bus,
        cluster_namespace,
    )
    .await
    {
        if let Some(cached_game_state) = cached_active_state.as_ref()
            && (live_game_state.start_ms != cached_game_state.start_ms
                || live_game_state.event_sequence < cached_game_state.event_sequence)
        {
            warn!(
                "Refusing game {} join because cached and live runtime identities differ (cached start {}, sequence {}; live start {}, sequence {})",
                game_id,
                cached_game_state.start_ms,
                cached_game_state.event_sequence,
                live_game_state.start_ms,
                live_game_state.event_sequence
            );
            return Err(game_join_denied("This game is unavailable"));
        }

        if game_state_records_user(&live_game_state, user_id) {
            return Ok(());
        }

        warn!(
            "Denied warmed game {} join to user {}: user is not a recorded participant",
            game_id, user_id
        );
        return Err(game_join_denied("This game is unavailable"));
    }

    if cached_active_state.is_some() {
        debug!(
            "Live game {} did not reach replication during the bounded authorization wait; asking the client to retry",
            game_id
        );
        if has_durable_recovery_failure(game_id, game_bus, cluster_namespace).await {
            return Err(game_join_denied(
                crate::recovery::PUBLIC_UNRECOVERABLE_GAME_REASON,
            ));
        }
        return Err(GameJoinAuthorizationError::Warming);
    }

    let database_game_id = i32::try_from(game_id)
        .map_err(|_| game_join_denied("This game was not found or has expired"))?;
    let game = db.get_game_by_id(database_game_id).await.map_err(|e| {
        error!(
            "Failed to fetch game {} while authorizing user {}: {}",
            game_id, user_id, e
        );
        GameJoinAuthorizationError::Warming
    })?;
    let Some(game) = game else {
        let mapped_game_id = match load_durable_active_game(user_id, matchmaking_manager).await {
            Ok(mapped_game_id) => mapped_game_id,
            Err(error) => {
                warn!(game_id, user_id, %error, "Active-game lookup failed while classifying a missing durable game");
                return Err(GameJoinAuthorizationError::Warming);
            }
        };
        return Err(missing_game_join_failure(game_id, mapped_game_id));
    };
    let Some(game_state_json) = game.game_state else {
        if has_durable_recovery_failure(game_id, game_bus, cluster_namespace).await {
            return Err(game_join_denied(
                crate::recovery::PUBLIC_UNRECOVERABLE_GAME_REASON,
            ));
        }
        return Err(GameJoinAuthorizationError::Warming);
    };
    let game_state = serde_json::from_value::<GameState>(game_state_json).map_err(|e| {
        error!(
            "Failed to deserialize game {} while authorizing user {}: {}",
            game_id, user_id, e
        );
        game_join_denied("The saved game data could not be loaded")
    })?;

    if !matches!(game_state.status, GameStatus::Complete { .. }) {
        warn!(
            "Refusing database-only non-complete game {} join for user {}",
            game_id, user_id
        );
        if has_durable_recovery_failure(game_id, game_bus, cluster_namespace).await {
            return Err(game_join_denied(
                crate::recovery::PUBLIC_UNRECOVERABLE_GAME_REASON,
            ));
        }
        return Err(GameJoinAuthorizationError::Warming);
    }

    if !game_state_records_user(&game_state, user_id) {
        warn!(
            "Denied database game {} join to user {}: user is not a recorded participant",
            game_id, user_id
        );
        return Err(game_join_denied("This game is unavailable"));
    }

    Ok(())
}

async fn authorize_game_join(
    game_id: u32,
    user_id: u32,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
    replication_manager: &Arc<crate::replication::ReplicationManager>,
    game_bus: &Arc<GameBus>,
    cluster_namespace: &ClusterNamespace,
    db: &Arc<dyn Database>,
) -> std::result::Result<(), GameJoinAuthorizationError> {
    match tokio::time::timeout(
        GAME_JOIN_AUTHORIZATION_TIMEOUT,
        authorize_game_join_inner(
            game_id,
            user_id,
            matchmaking_manager,
            replication_manager,
            game_bus,
            cluster_namespace,
            db,
        ),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            warn!(
                game_id,
                user_id, "Game join authorization timed out; returning retryable warm-up"
            );
            Err(GameJoinAuthorizationError::Warming)
        }
    }
}

/// Recover a committed matchmaking result without relying on its best-effort
/// Pub/Sub notification. The per-user mapping is written atomically with the
/// game command, so every participant can discover it on any gateway after a
/// reconnect. Authorization still comes from game state; a stale or malformed
/// mapping is never enough to disclose a game, and only fenced completion owns
/// deletion of the mapping.
async fn notify_durable_active_game_after_auth(
    user_id: u32,
    ws_tx: &mpsc::Sender<Message>,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
    replication_manager: &Arc<crate::replication::ReplicationManager>,
    game_bus: &Arc<GameBus>,
    cluster_namespace: &ClusterNamespace,
    db: &Arc<dyn Database>,
) -> Result<()> {
    let mapped_game_id = match tokio::time::timeout(ACTIVE_GAME_MAPPING_TIMEOUT, async {
        let mut manager = matchmaking_manager.lock().await;
        manager.get_user_active_game(user_id).await
    })
    .await
    {
        Ok(Ok(game_id)) => game_id,
        Ok(Err(error)) => {
            warn!(user_id, %error, "Failed to resolve durable active-game mapping");
            None
        }
        Err(_) => {
            warn!(user_id, "Timed out resolving durable active-game mapping");
            None
        }
    };
    let Some(game_id) = mapped_game_id else {
        return Ok(());
    };

    match authorize_game_join(
        game_id,
        user_id,
        matchmaking_manager,
        replication_manager,
        game_bus,
        cluster_namespace,
        db,
    )
    .await
    {
        Ok(()) => {
            info!(
                user_id,
                game_id, "Recovered committed match from durable user mapping"
            );
            ws_tx
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::JoinGame(game_id))?.into(),
                ))
                .await
                .context("WebSocket closed while restoring committed match")?;
        }
        Err(GameJoinAuthorizationError::Warming) => {
            info!(
                user_id,
                game_id, "Committed match replica is still warming; client will retry"
            );
            ws_tx
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::GameWarming {
                        game_id,
                        retry_after_ms: GAME_WARMING_RETRY_MS,
                    })?
                    .into(),
                ))
                .await
                .context("WebSocket closed while reporting committed match warm-up")?;
        }
        Err(GameJoinAuthorizationError::Denied(reason)) => {
            warn!(
                user_id,
                game_id,
                reason,
                "Durable active-game mapping did not pass participant authorization; leaving cleanup to fenced completion"
            );
        }
    }

    Ok(())
}

fn recovery_bridge_snapshot(envelope: &RecoveryEnvelopeV2, user_id: u32) -> GameEventMessage {
    // Despite its historical name, `next_event_stream_sequence` is the last
    // sequence already emitted and checkpointed. The actor increments it
    // before its next publish, so subtracting one here manufactures a gap.
    GameEventMessage {
        game_id: envelope.game_id,
        tick: envelope.game_state.tick,
        sequence: envelope.game_state.event_sequence,
        stream_seq: envelope.next_event_stream_sequence,
        user_id: Some(user_id),
        event: GameEvent::Snapshot {
            game_state: envelope.game_state.clone(),
        },
    }
}

async fn send_recovery_bridge_snapshot(
    ws_tx: &mpsc::Sender<Message>,
    envelope: &RecoveryEnvelopeV2,
    user_id: u32,
) -> bool {
    let recovery_snapshot = recovery_bridge_snapshot(envelope, user_id);
    let Ok(json) = serde_json::to_string(&WSMessage::GameEvent(recovery_snapshot)) else {
        return false;
    };
    ws_tx.send(Message::Text(json.into())).await.is_ok()
}

async fn send_recovery_bridge_if_available(
    game_id: u32,
    user_id: u32,
    ws_tx: &mpsc::Sender<Message>,
    game_bus: &Arc<GameBus>,
    cluster_namespace: &ClusterNamespace,
) -> bool {
    match game_bus.get_recovery(cluster_namespace, game_id).await {
        Ok(Some(envelope))
            if !matches!(envelope.game_state.status, GameStatus::Complete { .. })
                && game_state_records_user(&envelope.game_state, user_id) =>
        {
            // Bridge the replica warm-up window immediately from the fenced
            // recovery envelope, then attach to the live replica. Deliberately
            // withhold CommandOutcomesComplete until that live subscription
            // exists: a planned replacement socket treats the barrier as its
            // promotion signal and must not retire the old usable socket for a
            // bridge that might have no subsequent event stream.
            send_recovery_bridge_snapshot(ws_tx, &envelope, user_id).await
        }
        Ok(_) => false,
        Err(error) => {
            warn!(game_id, user_id, %error, "Failed to load recovery during replica warm-up");
            false
        }
    }
}

// Helper function to subscribe to game events
async fn subscribe_to_game_events(
    game_id: u32,
    user_id: u32,
    ws_tx: mpsc::Sender<Message>,
    replication_manager: Arc<crate::replication::ReplicationManager>,
    db: Arc<dyn Database>,
    game_bus: Arc<GameBus>,
    cluster_namespace: ClusterNamespace,
) {
    info!(
        "Subscribing to game {} events for user {}",
        game_id, user_id
    );

    let mut initial_subscription = replication_manager.subscribe_to_game(game_id).await;
    if initial_subscription.is_err() {
        let partition_id = game_id % PARTITION_COUNT;
        if let Err(error) = replication_manager
            .request_partition_snapshots(partition_id)
            .await
        {
            warn!(game_id, partition_id, %error, "Failed to request subscription snapshots");
        }

        let mut recovery_bridge_sent = send_recovery_bridge_if_available(
            game_id,
            user_id,
            &ws_tx,
            &game_bus,
            &cluster_namespace,
        )
        .await;

        // Completed games intentionally leave replication memory. Avoid
        // spending the takeover wait on a game that already has its terminal
        // Redis snapshot.
        if !recovery_bridge_sent {
            match replication_manager.get_stored_snapshot(game_id).await {
                Ok(Some(game_state))
                    if matches!(game_state.status, GameStatus::Complete { .. })
                        && game_state_records_user(&game_state, user_id) =>
                {
                    send_completed_game_snapshot(
                        &ws_tx,
                        game_id,
                        user_id,
                        &game_state,
                        "stored Redis snapshot",
                    )
                    .await;
                    return;
                }
                Ok(_) => {}
                Err(error) => {
                    warn!(game_id, %error, "Failed to inspect stored snapshot during warm-up");
                }
            }
        }

        // Reissue requests throughout the lease gap. A request appended before
        // the new owner anchors its request reader is not a correctness signal.
        let deadline = tokio::time::Instant::now() + COLD_JOIN_WARMUP_TIMEOUT;
        while initial_subscription.is_err() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
            initial_subscription = replication_manager.subscribe_to_game(game_id).await;
            if initial_subscription.is_ok() {
                break;
            }

            match replication_manager
                .request_partition_snapshots(partition_id)
                .await
            {
                Ok(published) if published && !recovery_bridge_sent => {
                    recovery_bridge_sent = send_recovery_bridge_if_available(
                        game_id,
                        user_id,
                        &ws_tx,
                        &game_bus,
                        &cluster_namespace,
                    )
                    .await;
                }
                Ok(_) => {}
                Err(error) => {
                    warn!(game_id, partition_id, %error, "Failed to retry subscription snapshots");
                }
            }
        }
        if initial_subscription.is_err() && recovery_bridge_sent {
            warn!(
                game_id,
                user_id,
                "Replica did not become subscribable after recovery snapshot; returning retryable warm-up"
            );
            send_game_warming(&ws_tx, game_id).await;
            return;
        }
    }

    let (game_state, stream_watermark, mut rx) = match initial_subscription {
        Ok(result) => result,
        Err(e) => {
            // Durably completed games are eventually evicted from replication memory. Their
            // final snapshot remains in Redis briefly, so check that grace-period cache before
            // the durable database fallback.
            info!(
                "Failed to subscribe to game {} from memory, checking stored snapshot: {}",
                game_id, e
            );

            match replication_manager.get_stored_snapshot(game_id).await {
                Ok(Some(game_state))
                    if matches!(game_state.status, GameStatus::Complete { .. }) =>
                {
                    send_completed_game_snapshot(
                        &ws_tx,
                        game_id,
                        user_id,
                        &game_state,
                        "stored Redis snapshot",
                    )
                    .await;
                    return;
                }
                Ok(Some(_)) => {
                    // During a rolling deploy or a failed terminal cache write, Redis can still
                    // contain the preceding active snapshot. Prefer the durable completed record
                    // instead of stranding the client on a stale, non-terminal frame with no live
                    // subscription.
                    debug!(
                        "Ignoring non-complete stored Redis snapshot for game {}, checking database",
                        game_id
                    );
                }
                Ok(None) => {
                    debug!("No stored Redis snapshot found for game {}", game_id);
                }
                Err(e) => {
                    warn!(
                        "Failed to load stored Redis snapshot for game {}, checking database: {}",
                        game_id, e
                    );
                }
            }

            let Ok(database_game_id) = i32::try_from(game_id) else {
                warn!("Game ID {} is outside the durable database range", game_id);
                send_game_load_failed(&ws_tx, game_id, "This game was not found or has expired")
                    .await;
                return;
            };

            match db.get_game_by_id(database_game_id).await {
                Ok(Some(game)) => {
                    if let Some(game_state_json) = game.game_state {
                        match serde_json::from_value::<GameState>(game_state_json) {
                            Ok(game_state)
                                if matches!(game_state.status, GameStatus::Complete { .. }) =>
                            {
                                send_completed_game_snapshot(
                                    &ws_tx,
                                    game_id,
                                    user_id,
                                    &game_state,
                                    "database snapshot",
                                )
                                .await;

                                // Return early - we can't subscribe to future events without memory state
                                return;
                            }
                            Ok(_) => {
                                info!(
                                    game_id,
                                    "Durable game is non-terminal while its replica is unavailable; returning retryable warm-up"
                                );
                                send_game_warming(&ws_tx, game_id).await;
                                return;
                            }
                            Err(e) => {
                                error!("Failed to deserialize game state from database: {}", e);
                                send_game_load_failed(
                                    &ws_tx,
                                    game_id,
                                    "The saved game data could not be loaded",
                                )
                                .await;
                                return;
                            }
                        }
                    } else {
                        info!(
                            game_id,
                            "Durable game has no terminal state while its replica is unavailable; returning retryable warm-up"
                        );
                        send_game_warming(&ws_tx, game_id).await;
                        return;
                    }
                }
                Ok(None) => {
                    // Authorization already proved this game from live/recovery
                    // state. Missing completion persistence here is therefore a
                    // failover race, not definitive evidence that it expired.
                    info!(
                        game_id,
                        "Authorized game is not yet durable while its replica is unavailable; returning retryable warm-up"
                    );
                    send_game_warming(&ws_tx, game_id).await;
                    return;
                }
                Err(e) => {
                    error!("Failed to fetch game {} from database: {}", game_id, e);
                    send_game_warming(&ws_tx, game_id).await;
                    return;
                }
            }
        }
    };

    if matches!(game_state.status, GameStatus::Complete { .. }) {
        send_completed_game_snapshot(&ws_tx, game_id, user_id, &game_state, "replication cache")
            .await;
        return;
    }

    // Send the snapshot, stamped with the replica's transport watermark so
    // the client's gap detection starts from the right point.
    let snapshot_event = GameEventMessage {
        game_id,
        tick: game_state.tick,
        sequence: 0,
        stream_seq: stream_watermark,
        user_id: Some(user_id),
        event: GameEvent::Snapshot {
            game_state: game_state.clone(),
        },
    };
    let json = serde_json::to_string(&WSMessage::GameEvent(snapshot_event)).unwrap();
    if let Err(e) = ws_tx.try_send(Message::Text(json.into())) {
        match e {
            mpsc::error::TrySendError::Full(msg) => {
                warn!(
                    "WebSocket send channel full (capacity 1024) for game {}, blocking send",
                    game_id
                );
                if ws_tx.send(msg).await.is_err() {
                    error!(
                        "WebSocket send channel closed for game {}, stopping event subscription",
                        game_id
                    );
                    return;
                }
            }
            mpsc::error::TrySendError::Closed(_) => {
                debug!(
                    "WebSocket send channel closed for game {}, stopping event subscription",
                    game_id
                );
                return;
            }
        }
    }

    if !send_command_outcomes(&ws_tx, &game_bus, &cluster_namespace, game_id, user_id).await {
        return;
    }

    loop {
        let event_msg = match rx.recv().await {
            Ok(event_msg) => event_msg,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                // This connection fell behind the broadcast and lost events.
                // Recover by sending a fresh snapshot (with its watermark) so
                // the client re-anchors instead of silently diverging.
                warn!(
                    "Event forwarder for game {} (user {}) lagged, {} events lost; resyncing client with fresh snapshot",
                    game_id, user_id, skipped
                );
                match replication_manager.subscribe_to_game(game_id).await {
                    Ok((state, watermark, new_rx)) => {
                        rx = new_rx;
                        let resync = GameEventMessage {
                            game_id,
                            tick: state.tick,
                            sequence: 0,
                            stream_seq: watermark,
                            user_id: Some(user_id),
                            event: GameEvent::Snapshot { game_state: state },
                        };
                        let json = serde_json::to_string(&WSMessage::GameEvent(resync)).unwrap();
                        if ws_tx.send(Message::Text(json.into())).await.is_err() {
                            debug!(
                                "WebSocket send channel closed for game {} during lag resync",
                                game_id
                            );
                            return;
                        }
                        if !send_command_outcomes(
                            &ws_tx,
                            &game_bus,
                            &cluster_namespace,
                            game_id,
                            user_id,
                        )
                        .await
                        {
                            return;
                        }
                        continue;
                    }
                    Err(e) => {
                        // Game likely completed and was evicted mid-lag.
                        error!(
                            "Failed to resubscribe to game {} after lag: {}; ending subscription",
                            game_id, e
                        );
                        return;
                    }
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                // Broadcaster dropped. If the game is still live in the
                // replica, hand the client one last authoritative snapshot so
                // it can at least resync via RequestResync; either way, log
                // loudly — a silent exit here is how ghost games are born.
                if let Some(state) = replication_manager.get_game_state(game_id).await {
                    error!(
                        "Event broadcaster closed for live game {} (user {}); sending final snapshot",
                        game_id, user_id
                    );
                    let final_snapshot = GameEventMessage {
                        game_id,
                        tick: state.tick,
                        sequence: 0,
                        stream_seq: replication_manager.get_stream_seq(game_id).await,
                        user_id: Some(user_id),
                        event: GameEvent::Snapshot { game_state: state },
                    };
                    let json =
                        serde_json::to_string(&WSMessage::GameEvent(final_snapshot)).unwrap();
                    let _ = ws_tx.send(Message::Text(json.into())).await;
                    let _ = send_command_outcomes(
                        &ws_tx,
                        &game_bus,
                        &cluster_namespace,
                        game_id,
                        user_id,
                    )
                    .await;
                } else {
                    info!(
                        "Event broadcaster closed for game {} (game evicted); ending subscription",
                        game_id
                    );
                }
                return;
            }
        };

        // Check if the game has ended
        let is_final = matches!(
            &event_msg.event,
            GameEvent::StatusUpdated { status } if matches!(status, GameStatus::Complete { .. })
        );
        let is_snapshot = snapshot_requires_command_outcomes(&event_msg.event);

        let json = serde_json::to_string(&WSMessage::GameEvent(event_msg)).unwrap();
        let msg = Message::Text(json.into());
        if let Err(e) = ws_tx.try_send(msg.clone()) {
            match e {
                mpsc::error::TrySendError::Full(_) => {
                    warn!(
                        "WebSocket send channel full for game {}, blocking send",
                        game_id
                    );
                    if ws_tx.send(msg).await.is_err() {
                        debug!(
                            "WebSocket send channel closed for game {}, stopping event subscription",
                            game_id
                        );
                        break;
                    }
                }
                mpsc::error::TrySendError::Closed(_) => {
                    debug!(
                        "WebSocket send channel closed for game {}, stopping event subscription",
                        game_id
                    );
                    break;
                }
            }
        }

        if is_snapshot {
            // A takeover snapshot can replace terminal command events that
            // were published by the old owner but never reached this socket.
            // Reconcile before forwarding any later live delta.
            if !send_command_outcomes(&ws_tx, &game_bus, &cluster_namespace, game_id, user_id).await
            {
                return;
            }
        }

        if is_final {
            info!("Game {} completed, stopping event subscription", game_id);
            break;
        }
    }
}

async fn send_command_outcomes(
    ws_tx: &mpsc::Sender<Message>,
    game_bus: &GameBus,
    cluster_namespace: &ClusterNamespace,
    game_id: u32,
    user_id: u32,
) -> bool {
    let deadline = tokio::time::Instant::now() + COMMAND_OUTCOME_LOAD_TIMEOUT;
    let envelope = loop {
        match tokio::time::timeout(
            COMMAND_OUTCOME_READ_TIMEOUT,
            game_bus.get_recovery(cluster_namespace, game_id),
        )
        .await
        {
            Ok(Ok(Some(envelope))) => break envelope,
            Ok(Ok(None)) => {
                debug!(game_id, user_id, "Recovery envelope is not visible yet");
            }
            Ok(Err(error)) => {
                warn!(game_id, user_id, %error, "Failed to load command outcomes for snapshot; retrying");
            }
            Err(_) => {
                warn!(
                    game_id,
                    user_id, "Timed out loading command outcomes for snapshot; retrying"
                );
            }
        }

        if tokio::time::Instant::now() >= deadline {
            warn!(
                game_id,
                user_id, "Command outcomes did not become readable before the warm-up deadline"
            );
            send_game_warming(ws_tx, game_id).await;
            return false;
        }
        tokio::select! {
            _ = ws_tx.closed() => return false,
            _ = tokio::time::sleep(COMMAND_OUTCOME_RETRY_DELAY) => {}
        }
    };

    send_command_outcomes_from_resolved(ws_tx, game_id, user_id, envelope.resolved_client_commands)
        .await
}

async fn send_command_outcomes_from_resolved(
    ws_tx: &mpsc::Sender<Message>,
    game_id: u32,
    user_id: u32,
    resolved: ResolvedCommandState,
) -> bool {
    for (client_game_session_id, session) in command_outcomes_for_user(resolved, user_id) {
        let response = WSMessage::CommandOutcomes {
            game_id,
            client_game_session_id,
            contiguous_through: session.contiguous_through,
            outcomes: session.outcomes,
        };
        let json = match serde_json::to_string(&response) {
            Ok(json) => json,
            Err(error) => {
                error!(game_id, user_id, %error, "Failed to serialize command outcomes");
                return false;
            }
        };
        if ws_tx.send(Message::Text(json.into())).await.is_err() {
            debug!(
                game_id,
                user_id, "WebSocket closed while sending command outcomes"
            );
            return false;
        }
    }

    // A user can legitimately have no recorded command session. The explicit
    // barrier distinguishes that case from a delayed/failed recovery read, so
    // make-before-break never promotes based on a timing assumption.
    send_command_outcome_barrier(ws_tx, game_id, user_id).await
}

async fn send_command_outcome_barrier(
    ws_tx: &mpsc::Sender<Message>,
    game_id: u32,
    user_id: u32,
) -> bool {
    let response = WSMessage::CommandOutcomesComplete { game_id };
    let json = match serde_json::to_string(&response) {
        Ok(json) => json,
        Err(error) => {
            error!(game_id, user_id, %error, "Failed to serialize command outcome barrier");
            return false;
        }
    };
    if ws_tx.send(Message::Text(json.into())).await.is_err() {
        debug!(
            game_id,
            user_id, "WebSocket closed while sending command outcome barrier"
        );
        return false;
    }
    true
}

async fn send_game_snapshot(
    ws_tx: &mpsc::Sender<Message>,
    game_id: u32,
    user_id: u32,
    game_state: &GameState,
) -> Result<()> {
    let snapshot_event = GameEventMessage {
        game_id,
        tick: game_state.tick,
        sequence: game_state.event_sequence,
        stream_seq: 0, // terminal snapshot; no live stream follows
        user_id: Some(user_id),
        event: GameEvent::Snapshot {
            game_state: game_state.clone(),
        },
    };
    let json = serde_json::to_string(&WSMessage::GameEvent(snapshot_event))?;
    ws_tx
        .send(Message::Text(json.into()))
        .await
        .context("WebSocket channel closed while sending game snapshot")
}

async fn send_completed_game_snapshot(
    ws_tx: &mpsc::Sender<Message>,
    game_id: u32,
    user_id: u32,
    game_state: &GameState,
    source: &str,
) {
    if !matches!(game_state.status, GameStatus::Complete { .. }) {
        error!(
            "Refusing to send non-complete {} for game {} to user {}",
            source, game_id, user_id
        );
        send_game_load_failed(ws_tx, game_id, "The saved game data is unavailable").await;
        return;
    }

    // Completed snapshots can include the full player and arena state. Only users recorded
    // as participants in the canonical GameState may reload them; guessed IDs do not grant
    // access once the live subscription is gone.
    if !game_state_records_user(game_state, user_id) {
        warn!(
            "Denied {} reload for game {} to user {}: user is not a recorded participant",
            source, game_id, user_id
        );
        send_game_load_failed(ws_tx, game_id, "This game is unavailable").await;
        return;
    }

    info!(
        "Loaded game {} state from {} for user {}",
        game_id, source, user_id
    );
    if let Err(e) = send_game_snapshot(ws_tx, game_id, user_id, game_state).await {
        error!(
            "Failed to send {} for game {} to user {}: {}",
            source, game_id, user_id, e
        );
        return;
    }
    // Terminal state itself clears every pending command, but replacement
    // sockets still require the explicit protocol barrier before promotion.
    let _ = send_command_outcome_barrier(ws_tx, game_id, user_id).await;
}

async fn send_game_load_failed(
    ws_tx: &mpsc::Sender<Message>,
    game_id: u32,
    reason: impl Into<String>,
) {
    let response = WSMessage::GameLoadFailed {
        game_id,
        reason: reason.into(),
    };

    match serde_json::to_string(&response) {
        Ok(json) => {
            if let Err(e) = ws_tx.send(Message::Text(json.into())).await {
                debug!(
                    "WebSocket channel closed while reporting load failure for game {}: {}",
                    game_id, e
                );
            }
        }
        Err(e) => {
            error!(
                "Failed to serialize load failure response for game {}: {}",
                game_id, e
            );
        }
    }
}

async fn send_game_warming(ws_tx: &mpsc::Sender<Message>, game_id: u32) {
    let response = WSMessage::GameWarming {
        game_id,
        retry_after_ms: GAME_WARMING_RETRY_MS,
    };
    match serde_json::to_string(&response) {
        Ok(json) => {
            if let Err(error) = ws_tx.send(Message::Text(json.into())).await {
                debug!(game_id, %error, "WebSocket closed while reporting game warm-up");
            }
        }
        Err(error) => {
            error!(game_id, %error, "Failed to serialize game warm-up response");
        }
    }
}

fn unsent_lobby_match(mapped_game_id: Option<u32>, last_sent_game_id: Option<u32>) -> Option<u32> {
    mapped_game_id.filter(|game_id| Some(*game_id) != last_sent_game_id)
}

async fn reconcile_lobby_match(
    lobby_code: &str,
    redis: &mut RedisConnection,
    ws_tx: &mpsc::Sender<Message>,
    last_sent_game_id: &mut Option<u32>,
) -> bool {
    let mapping_key = RedisKeys::matchmaking_lobby_active_game(lobby_code);
    let raw_game_id: Option<String> = match redis.get(&mapping_key).await {
        Ok(game_id) => game_id,
        Err(error) => {
            warn!(
                lobby_code,
                %error,
                "Failed to reconcile durable lobby match mapping"
            );
            return true;
        }
    };
    let mapped_game_id = match raw_game_id {
        Some(raw_game_id) => match raw_game_id.parse::<u32>() {
            Ok(game_id) => Some(game_id),
            Err(error) => {
                error!(
                    lobby_code,
                    mapping_key,
                    raw_game_id,
                    %error,
                    "Ignoring malformed durable lobby match mapping"
                );
                return true;
            }
        },
        None => None,
    };
    let Some(game_id) = unsent_lobby_match(mapped_game_id, *last_sent_game_id) else {
        return true;
    };

    let message = match serde_json::to_string(&WSMessage::JoinGame(game_id)) {
        Ok(message) => message,
        Err(error) => {
            error!(lobby_code, game_id, %error, "Failed to serialize lobby match join");
            return true;
        }
    };
    if let Err(error) = ws_tx.send(Message::Text(message.into())).await {
        debug!(
            lobby_code,
            game_id,
            %error,
            "WebSocket closed while forwarding durable lobby match"
        );
        return false;
    }

    *last_sent_game_id = Some(game_id);
    info!(
        lobby_code,
        game_id, "Forwarded durable lobby match to WebSocket"
    );
    true
}

/// Subscribe first, then read the durable mapping. A commit before SUBSCRIBE is
/// recovered by the GET; a commit after SUBSCRIBE is observed as a low-latency
/// hint. Periodic reconciliation covers a lagged push receiver while the
/// WebSocket itself remains healthy.
async fn subscribe_to_lobby_match_notifications(
    lobby_code: String,
    pubsub_manager: Arc<PubSubManager>,
    redis: impl Into<RedisConnection>,
    ws_tx: mpsc::Sender<Message>,
    cancellation_token: CancellationToken,
) {
    let mut redis = redis.into();
    let channel = RedisKeys::matchmaking_lobby_notification_channel(&lobby_code);
    let mut manager = (*pubsub_manager).clone();
    let mut last_sent_game_id = None;

    loop {
        let mut receiver = match manager.subscribe_to_channel(&channel).await {
            Ok(receiver) => receiver,
            Err(error) => {
                warn!(
                    lobby_code,
                    channel,
                    %error,
                    "Failed to subscribe to lobby match hints; retrying"
                );
                tokio::select! {
                    _ = cancellation_token.cancelled() => return,
                    _ = tokio::time::sleep(LOBBY_MATCH_SUBSCRIBE_RETRY_DELAY) => continue,
                }
            }
        };

        info!(lobby_code, channel, "Subscribed to lobby match hints");
        if !reconcile_lobby_match(&lobby_code, &mut redis, &ws_tx, &mut last_sent_game_id).await {
            return;
        }

        let mut reconciliation = tokio::time::interval(LOBBY_MATCH_RECONCILIATION_INTERVAL);
        reconciliation.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // The immediate durable read above already covers the interval's first tick.
        reconciliation.tick().await;

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => return,
                _ = reconciliation.tick() => {
                    if !reconcile_lobby_match(
                        &lobby_code,
                        &mut redis,
                        &ws_tx,
                        &mut last_sent_game_id,
                    ).await {
                        return;
                    }
                }
                hint = receiver.recv::<LobbyMatchHint>() => {
                    match hint {
                        Ok(LobbyMatchHint::MatchFound { game_id, partition_id }) => {
                            debug!(
                                lobby_code,
                                hinted_game_id = game_id,
                                hinted_partition_id = partition_id,
                                "Received lobby MatchFound hint; reconciling durable mapping"
                            );
                            if !reconcile_lobby_match(
                                &lobby_code,
                                &mut redis,
                                &ws_tx,
                                &mut last_sent_game_id,
                            ).await {
                                return;
                            }
                        }
                        Err(error) => {
                            warn!(
                                lobby_code,
                                channel,
                                %error,
                                "Lobby match hint receiver closed; resubscribing"
                            );
                            break;
                        }
                    }
                }
            }
        }

        tokio::select! {
            _ = cancellation_token.cancelled() => return,
            _ = tokio::time::sleep(LOBBY_MATCH_SUBSCRIBE_RETRY_DELAY) => {}
        }
    }
}

async fn subscribe_to_game_chat(
    game_id: u32,
    pubsub_manager: Arc<PubSubManager>,
    ws_tx: mpsc::Sender<Message>,
) -> Result<()> {
    info!("Subscribing to game {} chat", game_id);

    let channel = RedisKeys::game_chat_channel(game_id);
    let mut manager = (*pubsub_manager).clone();
    let mut receiver = manager
        .subscribe_to_channel(&channel)
        .await
        .context("Failed to subscribe to game chat channel")?;

    loop {
        let chat_payload: GameChatBroadcast = match receiver.recv().await {
            Ok(payload) => payload,
            Err(e) => {
                warn!("Failed to receive game chat payload: {}", e);
                break;
            }
        };

        let ws_message = WSMessage::GameChatMessage {
            game_id: chat_payload.game_id,
            message_id: chat_payload.message_id.clone(),
            user_id: chat_payload.user_id,
            username: chat_payload.username.clone(),
            message: chat_payload.message.clone(),
            timestamp_ms: chat_payload.timestamp_ms,
        };

        let json_msg = match serde_json::to_string(&ws_message) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to serialize game chat message: {}", e);
                continue;
            }
        };

        if ws_tx.send(Message::Text(json_msg.into())).await.is_err() {
            debug!(
                "WebSocket channel closed while forwarding game {} chat, stopping subscription",
                game_id
            );
            break;
        }
    }

    info!("Stopped subscribing to game {} chat", game_id);
    Ok(())
}

async fn subscribe_to_lobby_chat(
    lobby_code: String,
    pubsub_manager: Arc<PubSubManager>,
    ws_tx: mpsc::Sender<Message>,
) -> Result<()> {
    info!("Subscribing to lobby '{}' chat", lobby_code);

    let channel = RedisKeys::lobby_chat_channel(&lobby_code);
    let mut manager = (*pubsub_manager).clone();
    let mut receiver = manager
        .subscribe_to_channel(&channel)
        .await
        .context("Failed to subscribe to lobby chat channel")?;

    loop {
        let chat_payload: LobbyChatBroadcast = match receiver.recv().await {
            Ok(payload) => payload,
            Err(e) => {
                warn!("Failed to receive lobby chat payload: {}", e);
                break;
            }
        };

        let ws_message = WSMessage::LobbyChatMessage {
            lobby_code: chat_payload.lobby_code.clone(),
            message_id: chat_payload.message_id.clone(),
            user_id: chat_payload.user_id,
            username: chat_payload.username.clone(),
            message: chat_payload.message.clone(),
            timestamp_ms: chat_payload.timestamp_ms,
        };

        let json_msg = match serde_json::to_string(&ws_message) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to serialize lobby chat message: {}", e);
                continue;
            }
        };

        if ws_tx.send(Message::Text(json_msg.into())).await.is_err() {
            debug!(
                "WebSocket channel closed while forwarding lobby '{}' chat, stopping subscription",
                lobby_code
            );
            break;
        }
    }

    info!("Stopped subscribing to lobby '{}' chat", lobby_code);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_ws_message(
    state: ConnectionState,
    ws_message: WSMessage,
    jwt_verifier: &Arc<dyn JwtVerifier>,
    db: &Arc<dyn Database>,
    user_cache: UserCache,
    ws_tx: &mpsc::Sender<Message>,
    game_bus: &Arc<GameBus>,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
    replication_manager: &Arc<crate::replication::ReplicationManager>,
    redis: &RedisConnection,
    _redis_url: &str,
    lobby_manager: &Arc<crate::lobby_manager::LobbyManager>,
    websocket_id: &str,
    region: &str,
    lifecycle: &TaskLifecycle,
    socket_generation: u64,
    cluster_namespace: &ClusterNamespace,
) -> Result<ConnectionState> {
    use tracing::debug;
    let state_str = match &state {
        ConnectionState::Unauthenticated => "Unauthenticated",
        ConnectionState::Authenticated {
            lobby_handle: Some(lobby_handle),
            game_id: Some(gid),
            ..
        } => {
            debug!(
                "Processing message in Authenticated(lobby:{}, game:{})",
                lobby_handle.lobby_code, gid
            );
            "Authenticated(InLobby+InGame)"
        }
        ConnectionState::Authenticated {
            lobby_handle: Some(lobby_handle),
            game_id: None,
            ..
        } => {
            debug!(
                "Processing message in Authenticated(lobby:{})",
                lobby_handle.lobby_code
            );
            "Authenticated(InLobby)"
        }
        ConnectionState::Authenticated {
            lobby_handle: None,
            game_id: Some(gid),
            ..
        } => {
            debug!("Processing message in Authenticated(game:{})", gid);
            "Authenticated(InGame)"
        }
        ConnectionState::Authenticated { .. } => "Authenticated",
    };
    debug!(
        "Processing message: {:?} in state: {}",
        ws_message, state_str
    );

    match state {
        ConnectionState::Unauthenticated => {
            match ws_message {
                WSMessage::Token(jwt_token) => {
                    debug!("Received WebSocket authentication request");
                    match jwt_verifier.verify(&jwt_token).await {
                        Ok(user_token) => {
                            info!(
                                "Token verified successfully, user_id: {}",
                                user_token.user_id
                            );

                            let user = db
                                .get_user_by_id(user_token.user_id)
                                .await?
                                .ok_or_else(|| anyhow::anyhow!("User not found"))?;

                            let metadata = PlayerMetadata {
                                user_id: user_token.user_id,
                                username: user.username.clone(),
                                token: jwt_token.clone(),
                                is_guest: user.is_guest,
                            };

                            info!(
                                "User authenticated: {} (id: {})",
                                metadata.username, metadata.user_id
                            );

                            let authenticated = WSMessage::Authenticated {
                                task_boot_id: lifecycle.task_boot_id().to_owned(),
                                protocol_version: WS_PROTOCOL_VERSION,
                                capabilities: lifecycle.protocol_capabilities(),
                                socket_generation,
                            };
                            ws_tx
                                .send(Message::Text(serde_json::to_string(&authenticated)?.into()))
                                .await
                                .context(
                                    "WebSocket closed before authentication acknowledgement",
                                )?;
                            if let Ok(user_id) = u32::try_from(metadata.user_id) {
                                notify_durable_active_game_after_auth(
                                    user_id,
                                    ws_tx,
                                    matchmaking_manager,
                                    replication_manager,
                                    game_bus,
                                    cluster_namespace,
                                    db,
                                )
                                .await?;
                            }
                            Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: None,
                                game_id: None,
                                websocket_id: websocket_id.to_string(),
                            })
                        }
                        Err(e) => {
                            error!("Failed to verify token: {}", e);
                            Err(anyhow::anyhow!("Authentication failed"))
                        }
                    }
                }
                WSMessage::Ping { client_time } => {
                    // Respond with Pong even in unauthenticated state to keep connection alive
                    let server_time = chrono::Utc::now().timestamp_millis();
                    let pong_msg = Message::Text(
                        serde_json::to_string(&WSMessage::Pong {
                            client_time,
                            server_time,
                        })?
                        .into(),
                    );
                    ws_tx.send(pong_msg).await?;
                    Ok(ConnectionState::Unauthenticated)
                }
                _ => {
                    warn!("Cannot process message in unauthenticated state");
                    Ok(ConnectionState::Unauthenticated)
                }
            }
        }
        ConnectionState::Authenticated {
            metadata,
            lobby_handle: lobby,
            game_id,
            websocket_id,
        } => {
            match ws_message {
                WSMessage::UpdateNickname { nickname } => {
                    if let Err(e) = handle_guest_nickname_update(
                        db,
                        lobby_manager,
                        user_cache.clone(),
                        &lobby,
                        &metadata,
                        ws_tx,
                        nickname,
                    )
                    .await
                    {
                        error!(
                            "Failed to update guest nickname for user {}: {}",
                            metadata.user_id, e
                        );
                    }
                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }
                WSMessage::UpdateLobbyPreferences {
                    selected_modes,
                    competitive,
                } => {
                    {
                        if let Some(ref lobby_handle) = lobby {
                            lobby_manager
                                .set_lobby_preferences(
                                    &lobby_handle.lobby_code,
                                    &lobby_manager::LobbyPreferences {
                                        selected_modes,
                                        competitive,
                                    },
                                )
                                .await?;
                        }
                    }
                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }
                WSMessage::QueueForMatch {
                    game_type,
                    queue_mode,
                } => {
                    info!(
                        "User {} ({}) queuing for match type: {:?}, mode: {:?}",
                        metadata.username, metadata.user_id, game_type, queue_mode
                    );

                    if let Some(ref lobby_handle) = lobby {
                        if let Err(e) = queue_existing_lobby_for_game_types(
                            lobby_handle,
                            std::slice::from_ref(&game_type),
                            &queue_mode,
                            db,
                            lobby_manager,
                            matchmaking_manager,
                            metadata.user_id as u32,
                        )
                        .await
                        {
                            error!(
                                "Failed to queue existing lobby {}: {}",
                                lobby_handle.lobby_code, e
                            );
                            let response = WSMessage::AccessDenied {
                                reason: format!("Failed to queue lobby: {}", e),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                        } else {
                            info!(
                                "Queued existing lobby {} for game type {:?}",
                                lobby_handle.lobby_code, game_type
                            );
                        }

                        return Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: lobby,
                            game_id,
                            websocket_id,
                        });
                    }

                    // Fetch user's MMR from database
                    let user = db
                        .get_user_by_id(metadata.user_id)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("User not found"))?;
                    let mmr = user.mmr;
                    info!("User {} has MMR: {}", metadata.user_id, mmr);

                    // Auto-create a single-member lobby for this solo player (just like guest lobby creation)
                    info!("Creating auto-lobby for user {}", metadata.user_id);
                    match lobby_manager.create_lobby(metadata.user_id, region).await {
                        Ok(lobby) => {
                            info!(
                                "Auto-created lobby {} for user {}",
                                lobby.lobby_code(),
                                metadata.user_id
                            );
                            // Join the newly created lobby
                            let lobby_handle = match lobby_manager
                                .join_lobby(
                                    Some(lobby.lobby_code()),
                                    metadata.user_id,
                                    metadata.username.clone(),
                                    websocket_id.to_string(),
                                    region.to_string(),
                                    None,
                                )
                                .await
                            {
                                Ok(handle) => handle,
                                Err(e) => {
                                    error!("Failed to join auto-created lobby: {}", e);
                                    let response = WSMessage::AccessDenied {
                                        reason: format!(
                                            "Failed to create matchmaking lobby: {}",
                                            e
                                        ),
                                    };
                                    let json_msg = serde_json::to_string(&response)?;
                                    ws_tx.send(Message::Text(json_msg.into())).await?;
                                    return Ok(ConnectionState::Authenticated {
                                        metadata,
                                        lobby_handle: None,
                                        game_id,
                                        websocket_id,
                                    });
                                }
                            };

                            // Fetch lobby members (should be just this user)
                            let members = match lobby_manager
                                .get_lobby_members(&lobby_handle.lobby_code)
                                .await
                            {
                                Ok(m) => {
                                    info!(
                                        lobby_id = lobby_handle.lobby_code,
                                        member_count = m.len(),
                                        "Fetched lobby members for auto-created lobby"
                                    );
                                    for (idx, (_user_id, member)) in m.iter().enumerate() {
                                        info!(
                                            idx = idx,
                                            user_id = member.user_id,
                                            username = %member.username,
                                            "Lobby member"
                                        );
                                    }
                                    m
                                }
                                Err(e) => {
                                    error!("Failed to get lobby members: {}", e);
                                    return Ok(ConnectionState::Authenticated {
                                        metadata,
                                        lobby_handle: Some(lobby_handle),
                                        game_id,
                                        websocket_id,
                                    });
                                }
                            };

                            // Add the auto-created lobby to matchmaking queue
                            let mut mm_guard = matchmaking_manager.lock().await;
                            if let Err(e) = mm_guard
                                .add_lobby_to_queue(
                                    &lobby_handle.lobby_code,
                                    members.into_values().collect(),
                                    mmr,
                                    vec![game_type.clone()],
                                    queue_mode.clone(),
                                    metadata.user_id as u32, // Solo player is the requesting user
                                )
                                .await
                            {
                                error!("Failed to add lobby to matchmaking queue: {}", e);
                                let response = WSMessage::AccessDenied {
                                    reason: format!("Failed to queue for match: {}", e),
                                };
                                let json_msg = serde_json::to_string(&response)?;
                                ws_tx.send(Message::Text(json_msg.into())).await?;
                                return Ok(ConnectionState::Authenticated {
                                    metadata,
                                    lobby_handle: Some(lobby_handle),
                                    game_id,
                                    websocket_id,
                                });
                            }
                            drop(mm_guard);

                            if let Err(error) = lobby_manager
                                .publish_lobby_update(&lobby_handle.lobby_code)
                                .await
                            {
                                warn!(
                                    lobby_code = lobby_handle.lobby_code,
                                    %error,
                                    "Failed to publish queued lobby state"
                                );
                            }

                            info!(
                                "Auto-created lobby {} for solo player {} and added to matchmaking queue",
                                lobby_handle.lobby_code, metadata.user_id
                            );

                            // Transition to InLobby state - lobby match notifications will be handled automatically
                            Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: Some(lobby_handle),
                                game_id: None,
                                websocket_id: websocket_id.to_string(),
                            })
                        }
                        Err(e) => {
                            error!("Failed to create lobby: {}", e);
                            let response = WSMessage::AccessDenied {
                                reason: format!("Failed to create matchmaking lobby: {}", e),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: lobby,
                                game_id,
                                websocket_id,
                            })
                        }
                    }
                }
                WSMessage::QueueForMatchMulti {
                    game_types,
                    queue_mode,
                } => {
                    info!(
                        "User {} ({}) queuing for multiple match types: {:?}, mode: {:?}",
                        metadata.username, metadata.user_id, game_types, queue_mode
                    );

                    if let Some(ref lobby_handle) = lobby {
                        if let Err(e) = queue_existing_lobby_for_game_types(
                            lobby_handle,
                            &game_types,
                            &queue_mode,
                            db,
                            lobby_manager,
                            matchmaking_manager,
                            metadata.user_id as u32,
                        )
                        .await
                        {
                            error!(
                                "Failed to queue existing lobby {} for multiple types: {}",
                                lobby_handle.lobby_code, e
                            );
                            let response = WSMessage::AccessDenied {
                                reason: format!("Failed to queue lobby: {}", e),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                        } else {
                            info!(
                                "Queued existing lobby {} for multiple game types {:?}",
                                lobby_handle.lobby_code, game_types
                            );
                        }

                        return Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: lobby,
                            game_id,
                            websocket_id,
                        });
                    }

                    // Fetch user's MMR from database
                    let user = db
                        .get_user_by_id(metadata.user_id)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("User not found"))?;
                    let mmr = user.mmr;
                    info!("User {} has MMR: {}", metadata.user_id, mmr);

                    // Auto-create a single-member lobby for this solo player
                    info!("Creating auto-lobby for user {}", metadata.user_id);
                    match lobby_manager.create_lobby(metadata.user_id, region).await {
                        Ok(lobby) => {
                            info!(
                                "Auto-created lobby {} for user {}",
                                lobby.lobby_code(),
                                metadata.user_id
                            );
                            // Join the newly created lobby
                            let lobby_handle = match lobby_manager
                                .join_lobby(
                                    Some(lobby.lobby_code()),
                                    metadata.user_id,
                                    metadata.username.clone(),
                                    websocket_id.to_string(),
                                    region.to_string(),
                                    None,
                                )
                                .await
                            {
                                Ok(handle) => handle,
                                Err(e) => {
                                    error!("Failed to join auto-created lobby: {}", e);
                                    let response = WSMessage::AccessDenied {
                                        reason: format!(
                                            "Failed to create matchmaking lobby: {}",
                                            e
                                        ),
                                    };
                                    let json_msg = serde_json::to_string(&response)?;
                                    ws_tx.send(Message::Text(json_msg.into())).await?;
                                    return Ok(ConnectionState::Authenticated {
                                        metadata,
                                        lobby_handle: None,
                                        game_id,
                                        websocket_id,
                                    });
                                }
                            };

                            // Fetch lobby members (should be just this user)
                            let members = match lobby_manager
                                .get_lobby_members(&lobby_handle.lobby_code)
                                .await
                            {
                                Ok(m) => {
                                    info!(
                                        lobby_id = lobby_handle.lobby_code,
                                        member_count = m.len(),
                                        "Fetched lobby members for auto-created lobby"
                                    );
                                    for (idx, (_user_id, member)) in m.iter().enumerate() {
                                        info!(
                                            idx = idx,
                                            user_id = member.user_id,
                                            username = %member.username,
                                            "Lobby member"
                                        );
                                    }
                                    m
                                }
                                Err(e) => {
                                    error!("Failed to get lobby members: {}", e);
                                    return Ok(ConnectionState::Authenticated {
                                        metadata,
                                        lobby_handle: Some(lobby_handle),
                                        game_id,
                                        websocket_id,
                                    });
                                }
                            };

                            // Add the auto-created lobby to matchmaking queue with multiple game types
                            let mut mm_guard = matchmaking_manager.lock().await;
                            if let Err(e) = mm_guard
                                .add_lobby_to_queue(
                                    &lobby_handle.lobby_code,
                                    members.into_values().collect(),
                                    mmr,
                                    game_types, // Use game_types directly instead of wrapping
                                    queue_mode.clone(),
                                    metadata.user_id as u32, // Solo player is the requesting user
                                )
                                .await
                            {
                                error!("Failed to add lobby to matchmaking queue: {}", e);
                                let response = WSMessage::AccessDenied {
                                    reason: format!("Failed to queue for match: {}", e),
                                };
                                let json_msg = serde_json::to_string(&response)?;
                                ws_tx.send(Message::Text(json_msg.into())).await?;
                                return Ok(ConnectionState::Authenticated {
                                    metadata,
                                    lobby_handle: Some(lobby_handle),
                                    game_id,
                                    websocket_id,
                                });
                            }
                            drop(mm_guard);

                            if let Err(error) = lobby_manager
                                .publish_lobby_update(&lobby_handle.lobby_code)
                                .await
                            {
                                warn!(
                                    lobby_code = lobby_handle.lobby_code,
                                    %error,
                                    "Failed to publish queued lobby state"
                                );
                            }

                            info!(
                                "Auto-created lobby {} for solo player {} and added to matchmaking queue for multiple game types",
                                lobby_handle.lobby_code, metadata.user_id
                            );

                            // Transition to InLobby state - lobby match notifications will be handled automatically
                            Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: Some(lobby_handle),
                                game_id: None,
                                websocket_id: websocket_id.to_string(),
                            })
                        }
                        Err(e) => {
                            error!("Failed to create lobby: {}", e);
                            let response = WSMessage::AccessDenied {
                                reason: format!("Failed to create matchmaking lobby: {}", e),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: lobby,
                                game_id,
                                websocket_id,
                            })
                        }
                    }
                }
                WSMessage::JoinGame(requested_game_id) => {
                    info!(
                        "User {} ({}) joining game {}",
                        metadata.username, metadata.user_id, requested_game_id
                    );

                    let user_id = match u32::try_from(metadata.user_id) {
                        Ok(user_id) => user_id,
                        Err(_) => {
                            send_game_load_failed(
                                ws_tx,
                                requested_game_id,
                                "This game is unavailable",
                            )
                            .await;
                            return Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: lobby,
                                game_id: None,
                                websocket_id,
                            });
                        }
                    };

                    if let Err(failure) = authorize_game_join(
                        requested_game_id,
                        user_id,
                        matchmaking_manager,
                        replication_manager,
                        game_bus,
                        cluster_namespace,
                        db,
                    )
                    .await
                    {
                        let response = game_join_failure_message(requested_game_id, failure);
                        ws_tx
                            .send(Message::Text(serde_json::to_string(&response)?.into()))
                            .await?;
                        return Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: lobby,
                            game_id: None,
                            websocket_id,
                        });
                    }

                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id: Some(requested_game_id),
                        websocket_id,
                    })
                }
                WSMessage::LeaveGame => {
                    if let Some(current_game_id) = game_id {
                        info!(
                            "User {} ({}) leaving game {}",
                            metadata.username, metadata.user_id, current_game_id
                        );
                    } else {
                        debug!(
                            "Received LeaveGame from user {} ({}) but no active game was set",
                            metadata.username, metadata.user_id
                        );
                    }

                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id: None,
                        websocket_id,
                    })
                }
                WSMessage::LeaveQueue => {
                    info!(
                        "User {} ({}) leaving matchmaking queue",
                        metadata.username, metadata.user_id
                    );

                    // Queue admission is lobby-authoritative. Remove only the
                    // exact currently admitted lobby identity; there is no
                    // secondary per-player queue to reconcile.
                    let mut matchmaking_manager = matchmaking_manager.lock().await;
                    if let Some(lobby_handle) = &lobby {
                        let lobby_code = lobby_handle.lobby_code.clone();
                        match matchmaking_manager
                            .remove_lobby_from_all_queues_by_code(&lobby_code)
                            .await
                        {
                            Ok(removed) => {
                                if removed {
                                    info!(
                                        lobby_code = lobby_code,
                                        "Removed lobby from matchmaking queues after cancel"
                                    );
                                } else {
                                    info!(
                                        lobby_code = lobby_code,
                                        "Lobby was not present in matchmaking queues on cancel"
                                    );
                                }
                                if let Err(error) =
                                    lobby_manager.publish_lobby_update(&lobby_code).await
                                {
                                    warn!(
                                        lobby_code = lobby_code,
                                        %error,
                                        "Failed to publish reconciled lobby state after cancel"
                                    );
                                }
                            }
                            Err(e) => {
                                error!(
                                    lobby_code = lobby_code,
                                    error = %e,
                                    "Failed to remove lobby from matchmaking queues on cancel"
                                );
                            }
                        }
                    }

                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }
                WSMessage::Ping { client_time } => {
                    // Respond with Pong including server time for clock synchronization
                    let server_time = chrono::Utc::now().timestamp_millis();
                    let response = WSMessage::Pong {
                        client_time,
                        server_time,
                    };
                    let json_msg = serde_json::to_string(&response)?;
                    ws_tx.send(Message::Text(json_msg.into())).await?;
                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }
                WSMessage::GameEvent(event_msg) => {
                    // Forward game events to the client
                    warn!(
                        "Received game event in authenticated state: {:?}",
                        event_msg
                    );
                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }

                WSMessage::CreateLobby => {
                    info!(
                        "User {} ({}) creating lobby in region {}",
                        metadata.username, metadata.user_id, region
                    );

                    match lobby_manager.create_lobby(metadata.user_id, region).await {
                        Ok(lobby) => {
                            // Join the lobby
                            let lobby_handle = match lobby_manager
                                .join_lobby(
                                    Some(lobby.lobby_code()),
                                    metadata.user_id,
                                    metadata.username.clone(),
                                    websocket_id.to_string(),
                                    region.to_string(),
                                    None,
                                )
                                .await
                            {
                                Ok(handle) => handle,
                                Err(e) => {
                                    error!("Failed to join newly created lobby: {}", e);
                                    let response = WSMessage::AccessDenied {
                                        reason: format!("Failed to join lobby: {}", e),
                                    };
                                    let json_msg = serde_json::to_string(&response)?;
                                    ws_tx.send(Message::Text(json_msg.into())).await?;
                                    return Ok(ConnectionState::Authenticated {
                                        metadata,
                                        lobby_handle: None,
                                        game_id,
                                        websocket_id,
                                    });
                                }
                            };

                            // Send success response
                            let response = WSMessage::LobbyCreated {
                                lobby_code: lobby_handle.lobby_code.clone(),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;

                            // Transition to InLobby state
                            Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: Some(lobby_handle),
                                game_id: None,
                                websocket_id: websocket_id.to_string(),
                            })
                        }
                        Err(e) => {
                            error!("Failed to create lobby: {}", e);
                            let response = WSMessage::AccessDenied {
                                reason: format!("Failed to create lobby: {}", e),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: lobby,
                                game_id,
                                websocket_id,
                            })
                        }
                    }
                }
                WSMessage::JoinLobby {
                    lobby_code,
                    preferences,
                } => {
                    info!(
                        "User {} ({}) joining lobby with code: {}",
                        metadata.username, metadata.user_id, lobby_code
                    );

                    let lobby_metadata = match lobby_manager.get_lobby_metadata(&lobby_code).await {
                        Ok(meta) => meta,
                        Err(e) => {
                            error!("Failed to get lobby by code: {}", e);
                            let response = WSMessage::AccessDenied {
                                reason: format!("Failed to find lobby: {}", e),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            return Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: None,
                                game_id,
                                websocket_id,
                            });
                        }
                    };

                    if let Some(lobby_metadata) = &lobby_metadata {
                        if lobby_metadata.region != region {
                            warn!(
                                "Lobby '{}' is in region {}, user is in region {}",
                                lobby_code, lobby_metadata.region, region
                            );

                            // Get WebSocket URL for the target region from database
                            let ws_url = match db.get_region_ws_url(&lobby_metadata.region).await? {
                                Some(url) => url,
                                None => {
                                    let response = WSMessage::AccessDenied {
                                        reason: format!(
                                            "No servers available in region {}",
                                            lobby_metadata.region
                                        ),
                                    };
                                    let json_msg = serde_json::to_string(&response)?;
                                    ws_tx.send(Message::Text(json_msg.into())).await?;
                                    return Ok(ConnectionState::Authenticated {
                                        metadata,
                                        lobby_handle: None,
                                        game_id,
                                        websocket_id,
                                    });
                                }
                            };

                            let response = WSMessage::LobbyRegionMismatch {
                                target_region: lobby_metadata.region.clone(),
                                ws_url,
                                lobby_code: lobby_code.clone(),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            return Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: None,
                                game_id,
                                websocket_id,
                            });
                        }
                    } else {
                        info!(
                            "Lobby '{}' missing; auto-creating default lobby for user {}",
                            lobby_code, metadata.user_id
                        );
                    }

                    // Join (and auto-create if needed) the lobby
                    let lobby_handle = match lobby_manager
                        .join_lobby(
                            Some(&lobby_code),
                            metadata.user_id,
                            metadata.username.clone(),
                            websocket_id.to_string(),
                            region.to_string(),
                            preferences,
                        )
                        .await
                    {
                        Ok(handle) => handle,
                        Err(e) => {
                            let err_text = e.to_string();
                            error!("Failed to join lobby '{}': {}", lobby_code, err_text);
                            let response = WSMessage::AccessDenied {
                                reason: format!("Failed to join lobby: {}", err_text),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            return Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: None,
                                game_id,
                                websocket_id,
                            });
                        }
                    };

                    // Send success response
                    let response = WSMessage::JoinedLobby {
                        lobby_code: lobby_handle.lobby_code.clone(),
                    };
                    let json_msg = serde_json::to_string(&response)?;
                    ws_tx.send(Message::Text(json_msg.into())).await?;

                    // Transition to InLobby state
                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: Some(lobby_handle),
                        game_id: None,
                        websocket_id: websocket_id.to_string(),
                    })
                }
                WSMessage::LeaveLobby => {
                    if let Some(mut lobby_handle) = lobby {
                        let lobby_code = lobby_handle.lobby_code.clone();
                        match lobby_handle.close().await {
                            Ok(result) => {
                                if let LeaveLobbyResult::LobbyDeleted = result {
                                    let mut mm = matchmaking_manager.lock().await;
                                    match mm.remove_lobby_from_all_queues_by_code(&lobby_code).await
                                    {
                                        Ok(true) => {
                                            info!(
                                                "Removed empty lobby {} from matchmaking queues",
                                                lobby_code
                                            );
                                        }
                                        Ok(false) => {
                                            info!(
                                                "Lobby {} was not present in matchmaking queues",
                                                lobby_code
                                            );
                                        }
                                        Err(e) => {
                                            warn!(
                                                "Failed to remove lobby {} from matchmaking queues: {}",
                                                lobby_code, e
                                            );
                                        }
                                    }
                                }

                                let response = WSMessage::LeftLobby;
                                let json_msg = serde_json::to_string(&response)?;
                                ws_tx.send(Message::Text(json_msg.into())).await?;
                                Ok(ConnectionState::Authenticated {
                                    metadata,
                                    lobby_handle: None,
                                    game_id,
                                    websocket_id,
                                })
                            }
                            Err(e) => {
                                error!(
                                    "Failed to leave lobby {} for user {}: {}",
                                    lobby_code, metadata.user_id, e
                                );
                                let response = WSMessage::AccessDenied {
                                    reason: format!("Failed to leave lobby: {}", e),
                                };
                                let json_msg = serde_json::to_string(&response)?;
                                ws_tx.send(Message::Text(json_msg.into())).await?;
                                Ok(ConnectionState::Authenticated {
                                    metadata,
                                    lobby_handle: Some(lobby_handle),
                                    game_id,
                                    websocket_id,
                                })
                            }
                        }
                    } else {
                        let response = WSMessage::AccessDenied {
                            reason: "You are not currently in a lobby".to_string(),
                        };
                        let json_msg = serde_json::to_string(&response)?;
                        ws_tx.send(Message::Text(json_msg.into())).await?;
                        Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: None,
                            game_id,
                            websocket_id,
                        })
                    }
                }
                WSMessage::Chat(message) => {
                    let trimmed = message.trim();
                    if trimmed.is_empty() {
                        return Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: lobby,
                            game_id,
                            websocket_id,
                        });
                    }

                    if trimmed.chars().count() > MAX_CHAT_MESSAGE_LENGTH {
                        let response = WSMessage::AccessDenied {
                            reason: format!(
                                "Chat messages must be {} characters or fewer",
                                MAX_CHAT_MESSAGE_LENGTH
                            ),
                        };
                        let json_msg = serde_json::to_string(&response)?;
                        ws_tx.send(Message::Text(json_msg.into())).await?;
                        return Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: lobby,
                            game_id,
                            websocket_id,
                        });
                    }

                    let mut publish_error = false;
                    if let Some(current_game_id) = game_id {
                        let payload = GameChatBroadcast {
                            game_id: current_game_id,
                            message_id: uuid::Uuid::new_v4().to_string(),
                            user_id: metadata.user_id,
                            username: metadata.username.clone(),
                            message: trimmed.to_string(),
                            timestamp_ms: Utc::now().timestamp_millis(),
                        };

                        if let Err(e) = publish_game_chat_message(redis.clone(), payload).await {
                            error!(
                                "Failed to publish game {} chat message for user {}: {}",
                                current_game_id, metadata.user_id, e
                            );
                            publish_error = true;
                        }
                    } else if let Some(ref lobby_handle) = lobby {
                        let payload = LobbyChatBroadcast {
                            lobby_code: lobby_handle.lobby_code.clone(),
                            message_id: uuid::Uuid::new_v4().to_string(),
                            user_id: metadata.user_id,
                            username: metadata.username.clone(),
                            message: trimmed.to_string(),
                            timestamp_ms: Utc::now().timestamp_millis(),
                        };

                        if let Err(e) = publish_lobby_chat_message(redis.clone(), payload).await {
                            error!(
                                "Failed to publish lobby '{}' chat message for user {}: {}",
                                lobby_handle.lobby_code, metadata.user_id, e
                            );
                            publish_error = true;
                        }
                    } else {
                        let response = WSMessage::AccessDenied {
                            reason: "Chat is only available in a lobby or game".to_string(),
                        };
                        let json_msg = serde_json::to_string(&response)?;
                        ws_tx.send(Message::Text(json_msg.into())).await?;
                        return Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: lobby,
                            game_id,
                            websocket_id,
                        });
                    }

                    if publish_error {
                        let response = WSMessage::AccessDenied {
                            reason: "Failed to send chat message".to_string(),
                        };
                        let json_msg = serde_json::to_string(&response)?;
                        ws_tx.send(Message::Text(json_msg.into())).await?;
                    }

                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }
                WSMessage::GameCommandV2 {
                    command_id,
                    command,
                } => {
                    if let Some(game_id) = game_id {
                        let user_id = metadata.user_id as u32;
                        if command_id.game_id != game_id || command_id.user_id != user_id {
                            warn!(
                                claimed_game_id = command_id.game_id,
                                claimed_user_id = command_id.user_id,
                                authenticated_game_id = game_id,
                                authenticated_user_id = user_id,
                                "Canonicalizing untrusted v2 command identity"
                            );
                        }
                        let command_id = canonical_command_identity(command_id, game_id, user_id);
                        if let Err(error) = validate_client_command_identity(&command_id) {
                            warn!(game_id, user_id, %error, "Rejecting invalid v2 command identity");
                            let response = WSMessage::AccessDenied {
                                reason: "Invalid game command identity".to_owned(),
                            };
                            ws_tx
                                .send(Message::Text(serde_json::to_string(&response)?.into()))
                                .await?;
                            return Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: lobby,
                                game_id: Some(game_id),
                                websocket_id,
                            });
                        }
                        let partition_id = game_id % PARTITION_COUNT;
                        let event = StreamEvent::GameCommandSubmittedV2 {
                            game_id,
                            user_id,
                            command_id,
                            command,
                        };
                        if let Err(error) = game_bus.publish_command(partition_id, &event).await {
                            error!(game_id, user_id, %error, "Failed to publish v2 game command");
                        }
                        Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: lobby,
                            game_id: Some(game_id),
                            websocket_id,
                        })
                    } else {
                        warn!(
                            user_id = metadata.user_id,
                            "Ignoring v2 game command from a connection with no active game"
                        );
                        Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: lobby,
                            game_id,
                            websocket_id,
                        })
                    }
                }
                _ => {
                    warn!(
                        "Unexpected message in authenticated state: {:?}",
                        ws_message
                    );
                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }
            }
        }
    }
}

pub async fn register_server(
    db: &Arc<dyn Database>,
    grpc_address: &str,
    region: &str,
    origin: &str,
    ws_url: &str,
) -> Result<u64> {
    info!("Registering server instance");

    // Insert a new record and return the generated ID
    let id = db
        .register_server(grpc_address, region, origin, ws_url)
        .await
        .context("Failed to register server in database")?;

    let id_u64 = id as u64;
    info!(id = id_u64, "Server registered with ID: {}", id_u64);
    Ok(id_u64)
}

/// Subscribe to user count updates from Redis and forward to WebSocket client
async fn subscribe_to_user_count_updates(
    pubsub_manager: Arc<PubSubManager>,
    ws_tx: mpsc::Sender<Message>,
) -> Result<()> {
    let mut manager = (*pubsub_manager).clone();
    let mut receiver = manager
        .subscribe_to_channel("user_count_updates")
        .await
        .context("Failed to subscribe to user_count_updates channel")?;

    info!("Subscribed to user count updates");

    loop {
        let region_counts: HashMap<String, u32> = match receiver.recv().await {
            Ok(counts) => counts,
            Err(e) => {
                warn!("Failed to receive user count update: {}", e);
                break;
            }
        };

        let ws_message = WSMessage::UserCountUpdate { region_counts };
        let json_msg = match serde_json::to_string(&ws_message) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to serialize user count update: {}", e);
                continue;
            }
        };

        if ws_tx.send(Message::Text(json_msg.into())).await.is_err() {
            debug!("WebSocket channel closed, stopping user count subscription");
            break;
        }
    }

    Ok(())
}

#[allow(dead_code)] // custom-game/lobby feature scaffolding, not wired up yet
#[derive(Debug, Deserialize)]
struct LobbyUpdatePayload {
    lobby_code: String,
    members: BTreeMap<u32, lobby_manager::LobbyMember>,
    host_user_id: i32,
    state: String,
    preferences: lobby_manager::LobbyPreferences,
}

/// Subscribe to lobby updates and forward to WebSocket client
#[allow(dead_code)] // custom-game/lobby feature scaffolding, not wired up yet
async fn subscribe_to_lobby_updates(
    lobby_code: String,
    pubsub_manager: Arc<PubSubManager>,
    ws_tx: mpsc::Sender<Message>,
) -> Result<()> {
    info!("Subscribing to lobby '{}' updates", lobby_code);

    let channel = RedisKeys::lobby_updates_channel();
    let mut manager = (*pubsub_manager).clone();
    let mut receiver = manager
        .subscribe_to_channel(&channel)
        .await
        .context("Failed to subscribe to lobby updates channel")?;

    info!(
        "Subscribed to lobby updates on '{}' for lobby '{}'",
        channel, lobby_code
    );

    while let Ok(payload) = receiver.recv::<String>().await {
        match serde_json::from_str::<LobbyUpdatePayload>(&payload) {
            Ok(update) => {
                if update.lobby_code != lobby_code {
                    continue;
                }

                let LobbyUpdatePayload {
                    lobby_code,
                    members,
                    host_user_id,
                    state,
                    preferences,
                } = update;

                let ws_message = WSMessage::LobbyUpdate {
                    lobby_code,
                    members: members.into_values().collect(),
                    host_user_id,
                    state,
                    preferences,
                };

                let json_msg = match serde_json::to_string(&ws_message) {
                    Ok(json) => json,
                    Err(e) => {
                        error!("Failed to serialize lobby update: {}", e);
                        continue;
                    }
                };

                if ws_tx.send(Message::Text(json_msg.into())).await.is_err() {
                    debug!("WebSocket channel closed, stopping lobby subscription");
                    break;
                }
            }
            Err(e) => {
                // Handle lobby deletion notifications or malformed payloads
                match serde_json::from_str::<serde_json::Value>(&payload) {
                    Ok(value) => {
                        let payload_code = value
                            .get("lobby_code")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default();
                        if payload_code != lobby_code {
                            continue;
                        }

                        match value.get("state").and_then(|v| v.as_str()) {
                            Some("deleted") => {
                                info!(
                                    "Received deletion notice for lobby '{}', stopping subscription",
                                    lobby_code
                                );
                                break;
                            }
                            _ => {
                                warn!(
                                    "Unsupported lobby update payload for '{}': {} ({})",
                                    lobby_code, payload, e
                                );
                            }
                        }
                    }
                    Err(value_err) => {
                        warn!(
                            "Failed to parse lobby update payload for '{}': {} ({})",
                            lobby_code, payload, value_err
                        );
                    }
                }
            }
        }
    }

    info!("Stopped subscribing to lobby '{}' updates", lobby_code);
    Ok(())
}

pub async fn discover_peers(db: &Arc<dyn Database>, region: &str) -> Result<Vec<(u64, String)>> {
    info!("Discovering peers in region: {}", region);

    // Query to find all servers in the specified region
    let servers = db
        .get_active_servers(region)
        .await
        .context("Failed to fetch server records")?;

    if servers.is_empty() {
        warn!("No servers found in region: {}", region);
        return Ok(vec![]);
    }

    info!(
        "Found {} servers in region {}: {:?}",
        servers.len(),
        region,
        servers
    );
    Ok(servers
        .into_iter()
        .map(|(id, address)| (id as u64, address))
        .collect())
}

// Helper function to generate unique game codes
#[allow(dead_code)] // custom-game/lobby feature scaffolding, not wired up yet
fn generate_game_code() -> String {
    use rand::{Rng, thread_rng};
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = thread_rng();

    (0..8)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

#[allow(dead_code)] // custom-game/lobby feature scaffolding, not wired up yet
async fn join_custom_game(db: &Arc<dyn Database>, user_id: i32, game_code: &str) -> Result<u32> {
    // Find the game by code
    let game = db
        .get_game_by_code(game_code)
        .await?
        .context("Game not found or already started")?;

    // Check that game is waiting
    if game.status != "waiting" {
        return Err(anyhow::anyhow!("Game already started"));
    }

    let game_id = game.id;

    // Check if game is full
    let player_count = db.get_player_count(game_id).await?;

    // Get max players from game settings
    let max_players = game
        .game_type
        .get("settings")
        .and_then(|s| s.get("max_players"))
        .and_then(|v| v.as_i64())
        .unwrap_or(4) as i64;

    if player_count >= max_players {
        return Err(anyhow::anyhow!("Game is full"));
    }

    // For now, we need to handle player joining differently since GameState
    // only allows adding players on tick 0. We'll need to implement a proper
    // lobby system or modify the game engine to support late joins.

    // Add player to the game
    db.add_player_to_game(game_id, user_id, 0).await?;

    // TODO: Implement proper player joining through Redis events when game hasn't started yet
    warn!("Player joining for custom games needs proper implementation");

    Ok(game_id as u32)
}

#[allow(dead_code)] // custom-game/lobby feature scaffolding, not wired up yet
async fn check_game_host(db: &Arc<dyn Database>, game_id: u32, user_id: i32) -> Result<bool> {
    let host_user_id = db.get_custom_lobby_host(game_id as i32).await?;
    Ok(host_user_id == Some(user_id))
}

#[allow(dead_code)] // custom-game/lobby feature scaffolding, not wired up yet
async fn spectate_game(
    db: &Arc<dyn Database>,
    user_id: i32,
    game_id: u32,
    game_code: Option<&str>,
) -> Result<u32> {
    // If game_code is provided, look up game by code
    let actual_game_id = if let Some(code) = game_code {
        let game = db
            .get_game_by_code(code)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Invalid game code"))?;

        // Check if spectators are allowed for private games
        if game.is_private {
            let lobby = db.get_custom_lobby_by_code(code).await?;

            if let Some(lobby) = lobby {
                let allow_spectators = lobby
                    .settings
                    .get("allow_spectators")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);

                if !allow_spectators {
                    return Err(anyhow::anyhow!("Spectators are not allowed for this game"));
                }
            } else {
                // Private game without lobby, no spectators allowed
                return Err(anyhow::anyhow!("Spectators are not allowed for this game"));
            }
        }
        game.id as u32
    } else {
        // Direct game_id access - check if game exists and is public
        let game = db.get_game_by_id(game_id as i32).await?;

        match game {
            Some(g) if !g.is_private => game_id, // Public game, allow spectating
            Some(_) => return Err(anyhow::anyhow!("Cannot spectate private game without code")),
            None => return Err(anyhow::anyhow!("Game not found")),
        }
    };

    // Add spectator to the game
    db.add_spectator_to_game(actual_game_id as i32, user_id)
        .await?;

    info!(
        "User {} joined as spectator for game {}",
        user_id, actual_game_id
    );
    Ok(actual_game_id)
}

#[cfg(test)]
mod lifecycle_protocol_tests {
    use super::{
        GameJoinAuthorizationError, WSMessage, canonical_command_identity,
        command_outcomes_for_user, game_join_denied, game_join_failure_message,
        missing_game_join_failure, next_outbound_message, queue_planned_drain_notice,
        recovery_bridge_snapshot, send_command_outcomes_from_resolved,
        send_completed_game_snapshot, send_recovery_bridge_snapshot,
        snapshot_requires_command_outcomes, subscribe_to_lobby_match_notifications,
        unsent_lobby_match,
    };
    use crate::lifecycle::DrainNotice;
    use crate::pubsub_manager::PubSubManager;
    use crate::recovery::{RecoveryEnvelopeV2, ResolvedCommandState, SessionCommandOutcomes};
    use crate::redis_keys::RedisKeys;
    use crate::redis_utils::create_connection_manager;
    use common::{ClientCommandIdentityV2, GameEvent, GameState, GameStatus, GameType, QueueMode};
    use redis::{AsyncCommands, Client};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tokio::sync::{broadcast, mpsc};
    use tokio::time::{Duration, timeout};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn gateway_canonicalizes_untrusted_command_scope() {
        let identity = canonical_command_identity(
            ClientCommandIdentityV2 {
                game_id: 999,
                user_id: 888,
                client_game_session_id: "session-a".to_owned(),
                sequence: 7,
            },
            42,
            5,
        );
        assert_eq!(identity.game_id, 42);
        assert_eq!(identity.user_id, 5);
        assert_eq!(identity.client_game_session_id, "session-a");
        assert_eq!(identity.sequence, 7);
    }

    #[test]
    fn only_fresh_snapshots_require_adjacent_command_outcomes() {
        let state = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, Some(1), 0);
        assert!(snapshot_requires_command_outcomes(&GameEvent::Snapshot {
            game_state: state,
        }));
        assert!(!snapshot_requires_command_outcomes(&GameEvent::TickHash {
            hash: 1,
            server_ts_ms: 2,
        }));
    }

    #[test]
    fn recovery_outcomes_are_filtered_to_the_authenticated_user() {
        let resolved = ResolvedCommandState {
            sessions: BTreeMap::from([
                ("5:session-a".to_owned(), SessionCommandOutcomes::default()),
                ("6:session-b".to_owned(), SessionCommandOutcomes::default()),
            ]),
        };
        let filtered = command_outcomes_for_user(resolved, 5);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].0, "session-a");
    }

    #[test]
    fn authentication_ack_advertises_an_explicit_capability_envelope() {
        let value = serde_json::to_value(WSMessage::Authenticated {
            task_boot_id: "task-a".to_owned(),
            protocol_version: 2,
            capabilities: vec!["planned-drain-v1".to_owned()],
            socket_generation: 3,
        })
        .unwrap();
        assert_eq!(value["Authenticated"]["task_boot_id"], "task-a");
        assert_eq!(value["Authenticated"]["socket_generation"], 3);
    }

    #[tokio::test]
    async fn planned_drain_bypasses_a_saturated_gameplay_queue() {
        let (ws_tx, mut ws_rx) = mpsc::channel(1024);
        for sequence in 0..1024 {
            ws_tx
                .try_send(Message::Text(format!("gameplay-{sequence}").into()))
                .unwrap();
        }
        let (drain_tx, mut drain_rx) = mpsc::channel(1);
        queue_planned_drain_notice(
            &drain_tx,
            &DrainNotice {
                task_boot_id: "departing-task".to_owned(),
                deadline_unix_ms: 123_456,
            },
        )
        .unwrap();

        let mut drain_open = true;
        let mut ws_open = true;
        let first = next_outbound_message(&mut drain_rx, &mut ws_rx, &mut drain_open, &mut ws_open)
            .await
            .unwrap();
        assert!(matches!(
            decode_ws_message(first),
            WSMessage::Drain {
                task_boot_id,
                deadline_unix_ms: 123_456,
            } if task_boot_id == "departing-task"
        ));

        let second =
            next_outbound_message(&mut drain_rx, &mut ws_rx, &mut drain_open, &mut ws_open)
                .await
                .unwrap();
        assert_eq!(second, Message::Text("gameplay-0".into()));
    }

    #[test]
    fn recovery_bridge_uses_the_exact_checkpointed_event_watermark() {
        let state = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, Some(5), 0);
        let envelope = RecoveryEnvelopeV2::new(
            42,
            2,
            state,
            "123-0".to_owned(),
            ResolvedCommandState::default(),
            7,
            41,
            1_000,
            "lease-token".to_owned(),
        );

        let bridge = recovery_bridge_snapshot(&envelope, 5);
        assert_eq!(bridge.stream_seq, 41);
        assert_eq!(bridge.game_id, 42);
    }

    #[tokio::test]
    async fn recovery_bridge_withholds_the_handoff_promotion_barrier() {
        let state = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, Some(5), 0);
        let envelope = RecoveryEnvelopeV2::new(
            42,
            2,
            state,
            "123-0".to_owned(),
            ResolvedCommandState::default(),
            7,
            41,
            1_000,
            "lease-token".to_owned(),
        );
        let (tx, mut rx) = mpsc::channel(2);

        assert!(send_recovery_bridge_snapshot(&tx, &envelope, 5).await);
        assert!(matches!(
            decode_ws_message(rx.recv().await.unwrap()),
            WSMessage::GameEvent(_)
        ));
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn cold_game_response_is_explicitly_retryable() {
        let warming = serde_json::to_value(game_join_failure_message(
            42,
            GameJoinAuthorizationError::Warming,
        ))
        .unwrap();
        assert_eq!(warming["GameWarming"]["game_id"], 42);
        assert_eq!(warming["GameWarming"]["retry_after_ms"], 500);
        assert!(warming.get("GameLoadFailed").is_none());

        let terminal = serde_json::to_value(game_join_failure_message(
            42,
            game_join_denied("This game was not found or has expired"),
        ))
        .unwrap();
        assert_eq!(terminal["GameLoadFailed"]["game_id"], 42);
        assert_eq!(
            terminal["GameLoadFailed"]["reason"],
            "This game was not found or has expired"
        );
        assert!(terminal.get("GameWarming").is_none());
    }

    #[test]
    fn durable_active_mapping_keeps_precreation_gap_retryable() {
        assert!(matches!(
            missing_game_join_failure(42, Some(42)),
            GameJoinAuthorizationError::Warming
        ));
        assert!(matches!(
            missing_game_join_failure(42, None),
            GameJoinAuthorizationError::Denied(_)
        ));
        assert!(matches!(
            missing_game_join_failure(42, Some(41)),
            GameJoinAuthorizationError::Denied(_)
        ));
    }

    #[test]
    fn lobby_match_reconciliation_covers_commit_before_subscribe() {
        assert_eq!(unsent_lobby_match(Some(42), None), Some(42));
    }

    #[test]
    fn lobby_match_reconciliation_deduplicates_hints_and_allows_play_again() {
        assert_eq!(unsent_lobby_match(Some(42), Some(42)), None);
        assert_eq!(unsent_lobby_match(Some(43), Some(42)), Some(43));
        assert_eq!(unsent_lobby_match(None, Some(42)), None);
    }

    #[tokio::test]
    async fn live_lobby_listener_recovers_committed_and_missed_hints_once() {
        let redis_url = "redis://127.0.0.1:6379/1?protocol=resp3";
        let client = Client::open(redis_url).unwrap();
        let (pubsub_tx, _pubsub_rx) = broadcast::channel(128);
        let redis = create_connection_manager(client.clone(), pubsub_tx.clone())
            .await
            .unwrap();
        let pubsub_manager = Arc::new(PubSubManager::new(redis.clone(), pubsub_tx));
        let mut control = client.get_multiplexed_async_connection().await.unwrap();
        let lobby_code = format!("LISTENER-{}", uuid::Uuid::new_v4());
        let mapping_key = RedisKeys::matchmaking_lobby_active_game(&lobby_code);
        let channel = RedisKeys::matchmaking_lobby_notification_channel(&lobby_code);
        let first_game_id = 42_001_u32;
        let second_game_id = 42_002_u32;

        // This commit predates SUBSCRIBE. The listener's subscribe-then-GET
        // ordering must still deliver it.
        control
            .set::<_, _, ()>(&mapping_key, first_game_id)
            .await
            .unwrap();
        let (ws_tx, mut ws_rx) = mpsc::channel(8);
        let cancellation = CancellationToken::new();
        let listener = tokio::spawn(subscribe_to_lobby_match_notifications(
            lobby_code,
            pubsub_manager,
            redis,
            ws_tx,
            cancellation.clone(),
        ));

        let first = timeout(Duration::from_secs(2), ws_rx.recv())
            .await
            .expect("listener did not reconcile the preexisting mapping")
            .expect("listener closed before delivering the preexisting mapping");
        assert!(matches!(
            decode_ws_message(first),
            WSMessage::JoinGame(game_id) if game_id == first_game_id
        ));

        let duplicate_hint = serde_json::json!({
            "type": "MatchFound",
            "game_id": first_game_id,
            "partition_id": 1,
        })
        .to_string();
        for _ in 0..2 {
            control
                .publish::<_, _, ()>(&channel, &duplicate_hint)
                .await
                .unwrap();
        }
        assert!(
            timeout(Duration::from_millis(250), ws_rx.recv())
                .await
                .is_err(),
            "duplicate hints must not forward a second JoinGame"
        );

        // Deliberately publish no hint for the later game. The periodic
        // durable read is the recovery path for an at-most-once Pub/Sub loss.
        control
            .set::<_, _, ()>(&mapping_key, second_game_id)
            .await
            .unwrap();
        let second = timeout(Duration::from_secs(6), ws_rx.recv())
            .await
            .expect("periodic reconciliation did not recover the missed hint")
            .expect("listener closed before periodic reconciliation");
        assert!(matches!(
            decode_ws_message(second),
            WSMessage::JoinGame(game_id) if game_id == second_game_id
        ));
        assert!(
            timeout(Duration::from_millis(250), ws_rx.recv())
                .await
                .is_err(),
            "periodic reads must not repeat the same JoinGame"
        );

        cancellation.cancel();
        timeout(Duration::from_secs(1), listener)
            .await
            .expect("listener ignored cancellation")
            .expect("listener task panicked");
        control.del::<_, ()>(&mapping_key).await.unwrap();
    }

    #[test]
    fn command_outcome_barrier_has_an_explicit_game_scope() {
        let value =
            serde_json::to_value(WSMessage::CommandOutcomesComplete { game_id: 42 }).unwrap();
        assert_eq!(value["CommandOutcomesComplete"]["game_id"], 42);
    }

    fn decode_ws_message(message: Message) -> WSMessage {
        let Message::Text(text) = message else {
            panic!("expected a text WebSocket message");
        };
        serde_json::from_str(&text).unwrap()
    }

    #[tokio::test]
    async fn empty_recovery_outcomes_still_emit_the_promotion_barrier() {
        let (tx, mut rx) = mpsc::channel(2);
        assert!(
            send_command_outcomes_from_resolved(&tx, 42, 5, ResolvedCommandState::default(),).await
        );
        assert!(matches!(
            decode_ws_message(rx.recv().await.unwrap()),
            WSMessage::CommandOutcomesComplete { game_id: 42 }
        ));
    }

    #[tokio::test]
    async fn completed_snapshot_also_emits_the_promotion_barrier() {
        let mut state = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, Some(1), 0);
        state.add_player(5, None).unwrap();
        state.status = GameStatus::Complete {
            winning_snake_id: Some(0),
        };
        let (tx, mut rx) = mpsc::channel(3);

        send_completed_game_snapshot(&tx, 42, 5, &state, "test snapshot").await;

        assert!(matches!(
            decode_ws_message(rx.recv().await.unwrap()),
            WSMessage::GameEvent(_)
        ));
        assert!(matches!(
            decode_ws_message(rx.recv().await.unwrap()),
            WSMessage::CommandOutcomesComplete { game_id: 42 }
        ));
    }
}
