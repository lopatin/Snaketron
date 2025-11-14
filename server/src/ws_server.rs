use crate::api::auth::validate_username;
use crate::db::Database;
use crate::game_executor::PARTITION_COUNT;
use crate::game_executor::StreamEvent;
use crate::lobby_manager;
use crate::lobby_manager::{LeaveLobbyResult, Lobby, LobbyJoinHandle, LobbyMember};
use crate::matchmaking_manager::MatchmakingManager;
use crate::pubsub_manager::PubSubManager;
use crate::redis_keys::RedisKeys;
use crate::user_cache::UserCache;
use crate::ws_matchmaking::remove_from_matchmaking_queue;
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use common::{
    DEFAULT_TICK_INTERVAL_MS, GameCommandMessage, GameEvent, GameEventMessage, GameState,
    GameStatus,
};
use futures_util::future::join_all;
use futures_util::{SinkExt, Stream};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock, broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Sleep, sleep};
use tokio_stream::StreamExt;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use tungstenite::Utf8Bytes;

#[derive(Debug, Serialize, Deserialize)]
pub enum WSMessage {
    Token(String),
    JoinGame(u32),
    LeaveGame,
    GameCommand(GameCommandMessage),
    GameEvent(GameEventMessage),
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
    Shutdown,
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
    // Solo game responses
    SoloGameCreated {
        game_id: u32,
    },
    // High availability messages
    ServerShutdown {
        reason: String,
        grace_period_seconds: u32,
    },
    AuthorityTransfer {
        game_id: u32,
        new_server_url: String,
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

#[derive(Debug, Serialize, Deserialize, Clone)]
struct LobbyChatBroadcast {
    lobby_code: String,
    message_id: String,
    user_id: i32,
    username: String,
    message: String,
    timestamp_ms: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct GameChatBroadcast {
    game_id: u32,
    message_id: String,
    user_id: i32,
    username: String,
    message: String,
    timestamp_ms: i64,
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

    if let (Some(lobby)) = lobby {
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

/// Handle WebSocket connection from Axum
pub async fn handle_websocket(
    socket: axum::extract::ws::WebSocket,
    db: Arc<dyn Database>,
    user_cache: UserCache,
    jwt_verifier: Arc<dyn JwtVerifier>,
    redis: ConnectionManager,
    redis_url: String,
    pubsub_manager: Arc<PubSubManager>,
    matchmaking_manager: Arc<Mutex<MatchmakingManager>>,
    replication_manager: Arc<crate::replication::ReplicationManager>,
    cancellation_token: CancellationToken,
    lobby_manager: Arc<crate::lobby_manager::LobbyManager>,
    region: String,
) {
    info!("New WebSocket connection established");

    // Process the WebSocket connection
    if let Err(e) = handle_websocket_connection(
        socket,
        db,
        user_cache.clone(),
        pubsub_manager,
        matchmaking_manager,
        jwt_verifier,
        cancellation_token,
        replication_manager,
        redis,
        redis_url,
        lobby_manager,
        region,
    )
    .await
    {
        error!("WebSocket connection error: {}", e);
    }
}

/// Internal function to handle the WebSocket connection logic
async fn handle_websocket_connection(
    ws_stream: axum::extract::ws::WebSocket,
    db: Arc<dyn Database>,
    user_cache: UserCache,
    pubsub_manager: Arc<PubSubManager>,
    matchmaking_manager: Arc<Mutex<MatchmakingManager>>,
    jwt_verifier: Arc<dyn JwtVerifier>,
    cancellation_token: CancellationToken,
    replication_manager: Arc<crate::replication::ReplicationManager>,
    redis: ConnectionManager,
    redis_url: String,
    lobby_manager: Arc<crate::lobby_manager::LobbyManager>,
    region: String,
) -> Result<()> {
    // Split the WebSocket into send and receive parts using futures_util
    let (mut ws_sink, mut ws_stream) = futures_util::StreamExt::split(ws_stream);

    // Create a channel for sending messages to the WebSocket
    let (ws_tx, mut ws_rx) = mpsc::channel::<Message>(1024);

    // Generate a unique websocket ID for this connection
    let websocket_id = uuid::Uuid::new_v4().to_string();

    // Start in unauthenticated state
    let mut state = ConnectionState::Unauthenticated;

    // Create a shutdown timeout that starts as a never-completing future
    let shutdown_timeout = tokio::time::sleep(Duration::from_secs(u64::MAX));
    tokio::pin!(shutdown_timeout);
    let mut shutdown_started = false;

    // Will be used to track Redis stream subscription for game events
    let mut game_event_handle: Option<JoinHandle<()>> = None;

    // Will be used to track Redis pub/sub subscription for lobby updates
    // let mut lobby_update_handle: Option<JoinHandle<()>> = None;

    // Will be used to track Redis pub/sub subscription for lobby match notifications
    let mut lobby_match_handle: Option<JoinHandle<()>> = None;

    // Will be used to track lobby chat subscription
    let mut lobby_chat_handle: Option<JoinHandle<()>> = None;

    // Will be used to track game chat subscription
    let mut game_chat_handle: Option<JoinHandle<()>> = None;

    // Spawn task to forward messages from channel to WebSocket
    let ws_tx_clone = ws_tx.clone();
    let forward_task = tokio::spawn(async move {
        while let Some(msg) = ws_rx.recv().await {
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

        let next_lobby_update = async {
            match &mut state {
                ConnectionState::Authenticated {
                    lobby_handle: Some(lobby),
                    ..
                } => lobby.rx.recv().await,
                _ => future::pending().await,
            }
        };

        tokio::select! {
            // Handle shutdown timeout
            _ = &mut shutdown_timeout, if shutdown_started => {
                warn!("Shutdown timeout reached, closing connection");
                break;
            }
            // Handle cancellation token
            _ = cancellation_token.cancelled(), if !shutdown_started => {
                // Send a Shutdown message to the client
                info!("Sending shutdown message to client");
                let json_msg = serde_json::json!(WSMessage::Shutdown);
                let shutdown_msg = Message::Text(Utf8Bytes::from(json_msg.to_string()));
                if let Err(e) = ws_tx.send(shutdown_msg).await {
                    error!("Failed to send shutdown message: {}", e);
                }
                // Start shutdown timeout
                shutdown_timeout.as_mut().reset(tokio::time::Instant::now() + Duration::from_secs(10));
                shutdown_started = true;

                // No need for ShuttingDown state anymore, shutdown_started flag is sufficient
            }

            // Lobby updates get sent directly to the user
            lobby_update = next_lobby_update => {
                match lobby_update {
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

                        if ws_tx.send(Message::Text(json_msg.into())).await.is_err() {
                            warn!("WebSocket channel closed, dropping lobby update");
                            continue;
                        }
                    }
                    Err(e) => {
                        error!("Failed to receive lobby update: {}", e);
                        continue;
                    }
                }

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
                            axum::extract::ws::Message::Close(frame) => {
                                info!("Client initiated close");
                                break;
                            }
                        };

                        // Process the message
                        if let Message::Text(text) = tungstenite_msg {
                            match serde_json::from_str::<WSMessage>(&text) {
                                Ok(ws_message) => {
                                    // Check state before consuming it
                                    let was_in_game = matches!(&state, ConnectionState::Authenticated { game_id: Some(_), .. });
                                    let was_in_lobby = matches!(&state, ConnectionState::Authenticated { lobby_handle: Some(_), .. });

                                    match process_ws_message(
                                        state,
                                        ws_message,
                                        &jwt_verifier,
                                        &db,
                                        user_cache.clone(),
                                        &ws_tx,
                                        &pubsub_manager,
                                        &matchmaking_manager,
                                        &replication_manager,
                                        &redis,
                                        &redis_url,
                                        &lobby_manager,
                                        &websocket_id,
                                        &region,
                                    ).await {
                                        Ok(new_state) => {
                                            // Check if we're entering a game or lobby
                                            let entering_game = matches!(&new_state, ConnectionState::Authenticated { game_id: Some(_), .. }) && !was_in_game;
                                            let entering_lobby = matches!(&new_state, ConnectionState::Authenticated { lobby_handle: Some(_), .. }) && !was_in_lobby;
                                            let leaving_lobby = was_in_lobby && !matches!(&new_state, ConnectionState::Authenticated { lobby_handle: Some(_), .. });
                                            let leaving_game = was_in_game && !matches!(&new_state, ConnectionState::Authenticated { game_id: Some(_), .. });
                                            debug!("State transitioned to: entering_game: {}, entering_lobby: {}, leaving_lobby: {}, leaving_game: {}",
                                                entering_game, entering_lobby, leaving_lobby, leaving_game);

                                            // Handle state transitions
                                            if entering_game {
                                                if let ConnectionState::Authenticated { game_id: Some(game_id), metadata, .. } = &new_state {
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

                                                    game_event_handle = Some(tokio::spawn(async move {
                                                        subscribe_to_game_events(
                                                            game_id,
                                                            user_id,
                                                            ws_tx_clone,
                                                            replication_manager_clone,
                                                            db_clone,
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
                                            }

                                            // Handle lobby state transitions
                                            if entering_lobby {
                                                if let ConnectionState::Authenticated { lobby_handle: Some(lobby_handle), .. } = &new_state {
                                                    // Subscribe to lobby updates if entering a lobby
                                                    // if let Some(handle) = lobby_update_handle.take() {
                                                    //     handle.abort();
                                                    // }
                                                    if let Some(handle) = lobby_chat_handle.take() {
                                                        handle.abort();
                                                    }

                                                    let lobby_code_for_updates = lobby_handle.lobby_code.clone();
                                                    let lobby_code_for_match = lobby_handle.lobby_code.clone();
                                                    let ws_tx_clone = ws_tx.clone();
                                                    let redis_url_clone = redis_url.clone();

                                                    // lobby_update_handle = Some(tokio::spawn(async move {
                                                    //     if let Err(e) = subscribe_to_lobby_updates(
                                                    //         lobby_code_for_updates,
                                                    //         redis_url_clone,
                                                    //         ws_tx_clone,
                                                    //     ).await {
                                                    //         error!("Lobby update subscription failed: {}", e);
                                                    //     }
                                                    // }));

                                                    // Subscribe to lobby match notifications
                                                    if let Some(handle) = lobby_match_handle.take() {
                                                        handle.abort();
                                                    }

                                                    let ws_tx_clone_for_match = ws_tx.clone();
                                                    let redis_url_clone_for_match = redis_url.clone();
                                                    let replication_manager_clone_for_match = replication_manager.clone();

                                                    lobby_match_handle = Some(tokio::spawn(async move {
                                                        let channel = crate::redis_keys::RedisKeys::matchmaking_lobby_notification_channel(&lobby_code_for_match);
                                                        info!("Member subscribing to lobby match notifications on channel: {}", channel);

                                                        if let Ok(client) = redis::Client::open(redis_url_clone_for_match.as_ref()) {
                                                            if let Ok(mut pubsub) = client.get_async_pubsub().await {
                                                                if pubsub.subscribe(&channel).await.is_ok() {
                                                                    info!("Successfully subscribed to lobby match notifications for lobby '{}'", lobby_code_for_match);

                                                                    let mut pubsub_stream = pubsub.on_message();
                                                                    // Loop to handle multiple notifications (MatchFound, etc.)
                                                                    while let Some(msg) = futures_util::StreamExt::next(&mut pubsub_stream).await {
                                                                        if let Ok(payload) = msg.get_payload::<String>() {
                                                                            // Parse the notification
                                                                            if let Ok(notification) = serde_json::from_str::<serde_json::Value>(&payload) {
                                                                                let notification_type = notification["type"].as_str().unwrap_or("");

                                                                                match notification_type {
                                                                                    "MatchFound" => {
                                                                                        if let Some(game_id) = notification["game_id"].as_u64() {
                                                                                            info!("Lobby '{}' member matched to game {}, waiting for game to be available", lobby_code_for_match, game_id);

                                                                                            // Wait for the game to become available in the replication manager
                                                                                            match replication_manager_clone_for_match.wait_for_game(game_id as u32, 10).await {
                                                                                                Ok(_game_state) => {
                                                                                                    info!("Game {} is now available, sending JoinGame message to lobby '{}' member", game_id, lobby_code_for_match);
                                                                                                    // Send JoinGame message to this client
                                                                                                    let join_msg = WSMessage::JoinGame(game_id as u32);
                                                                                                    let json_msg = serde_json::to_string(&join_msg).unwrap();
                                                                                                    let _ = ws_tx_clone_for_match.send(Message::Text(json_msg.into())).await;
                                                                                                }
                                                                                                Err(e) => {
                                                                                                    error!("Failed to wait for game {} to become available: {}", game_id, e);
                                                                                                }
                                                                                            }
                                                                                        }
                                                                                    }
                                                                                    _ => {
                                                                                        warn!("Unknown notification type: {}", notification_type);
                                                                                    }
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                } else {
                                                                    error!("Failed to subscribe to lobby channel {} for lobby '{}'", channel, lobby_code_for_match);
                                                                }
                                                            }
                                                        }
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
                                            }

                                            // Abort lobby subscription when leaving lobby
                                            // BUT keep lobby_match_handle active if entering Authenticated with a lobby_code (for Play Again notifications)
                                            if leaving_lobby {
                                                let keep_match_subscription = matches!(&new_state, ConnectionState::Authenticated { lobby_handle: Some(_), .. });

                                                // if let Some(handle) = lobby_update_handle.take() {
                                                //     handle.abort();
                                                //     debug!("Aborted lobby update subscription");
                                                // }

                                                // Only abort match notification if NOT entering game with lobby_id
                                                if !keep_match_subscription {
                                                    if let Some(handle) = lobby_match_handle.take() {
                                                        handle.abort();
                                                        debug!("Aborted lobby match notification subscription");
                                                    }
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

    // Leave lobby if still in one
    match state {
        ConnectionState::Authenticated {
            lobby_handle: Some(mut lobby_handle),
            ..
        } => {
            let lobby_code = lobby_handle.lobby_code.clone();
            if let LeaveLobbyResult::LobbyDeleted = lobby_handle.close().await? {
                let mut mm = matchmaking_manager.lock().await;
                if let Err(e) = mm.remove_lobby_from_all_queues_by_code(&lobby_code).await {
                    warn!(
                        "Failed to remove empty lobby {} from matchmaking queues during cleanup: {}",
                        lobby_code, e
                    );
                } else {
                    info!(
                        "Removed empty lobby {} from all matchmaking queues during cleanup",
                        lobby_code
                    );
                }
            }
        }
        _ => {}
    }

    // Note: Game subscriptions are now handled differently
    // No need to manually close game_handle as it's not part of ConnectionState anymore

    // Abort subscription tasks
    if let Some(handle) = game_event_handle {
        handle.abort();
    }
    // if let Some(handle) = lobby_update_handle {
    //     handle.abort();
    // }
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
    mut redis: ConnectionManager,
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
    mut redis: ConnectionManager,
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

    lobby_manager
        .update_lobby_state(&lobby_handle.lobby_code, "queued")
        .await
        .context("Failed to update lobby state before queueing")?;

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
        Ok(total / count as i32)
    }
}

async fn load_lobby_chat_history(
    mut redis: ConnectionManager,
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
    mut redis: ConnectionManager,
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

// Helper function to subscribe to game events
async fn subscribe_to_game_events(
    game_id: u32,
    user_id: u32,
    ws_tx: mpsc::Sender<Message>,
    replication_manager: Arc<crate::replication::ReplicationManager>,
    db: Arc<dyn Database>,
) {
    info!(
        "Subscribing to game {} events for user {}",
        game_id, user_id
    );

    let (game_state, mut rx) = match replication_manager.subscribe_to_game(game_id).await {
        Ok(result) => result,
        Err(e) => {
            // If failed to get from memory, try to get from database
            info!(
                "Failed to subscribe to game {} from memory, checking database: {}",
                game_id, e
            );

            match db.get_game_by_id(game_id as i32).await {
                Ok(Some(game)) => {
                    if let Some(game_state_json) = game.game_state {
                        match serde_json::from_value::<GameState>(game_state_json) {
                            Ok(game_state) => {
                                info!("Loaded game {} state from database", game_id);
                                // Send the loaded state as an initial snapshot
                                let snapshot_event = GameEventMessage {
                                    game_id: game_id,
                                    tick: game_state.tick,
                                    sequence: 0,
                                    user_id: Some(user_id),
                                    event: GameEvent::Snapshot {
                                        game_state: game_state.clone(),
                                    },
                                };
                                let json =
                                    serde_json::to_string(&WSMessage::GameEvent(snapshot_event))
                                        .unwrap();
                                let _ = ws_tx.send(Message::Text(json.into())).await;

                                // Return early - we can't subscribe to future events without memory state
                                return;
                            }
                            Err(e) => {
                                error!("Failed to deserialize game state from database: {}", e);
                                return;
                            }
                        }
                    } else {
                        error!("Game {} found in database but has no game_state", game_id);
                        return;
                    }
                }
                Ok(None) => {
                    error!("Game {} not found in database", game_id);
                    return;
                }
                Err(e) => {
                    error!("Failed to fetch game {} from database: {}", game_id, e);
                    return;
                }
            }
        }
    };

    // Send the snapshot
    let snapshot_event = GameEventMessage {
        game_id: game_id,
        tick: game_state.tick,
        sequence: 0,
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

    while let Ok(event_msg) = rx.recv().await {
        // Check if the game has ended
        if let GameEvent::StatusUpdated { status } = &event_msg.event {
            if matches!(status, GameStatus::Complete { .. }) {
                info!("Game {} completed, stopping event subscription", game_id);
                // Send the final event before breaking
                let json = serde_json::to_string(&WSMessage::GameEvent(event_msg)).unwrap();
                if let Err(e) = ws_tx.try_send(Message::Text(json.into())) {
                    match e {
                        mpsc::error::TrySendError::Full(msg) => {
                            warn!(
                                "WebSocket send channel full on game complete for game {}, blocking send",
                                game_id
                            );
                            let _ = ws_tx.send(msg).await;
                        }
                        mpsc::error::TrySendError::Closed(_) => {
                            debug!(
                                "WebSocket send channel closed on game complete for game {}",
                                game_id
                            );
                        }
                    }
                }
                break;
            }
        }

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

async fn process_ws_message(
    state: ConnectionState,
    ws_message: WSMessage,
    jwt_verifier: &Arc<dyn JwtVerifier>,
    db: &Arc<dyn Database>,
    user_cache: UserCache,
    ws_tx: &mpsc::Sender<Message>,
    pubsub_manager: &Arc<PubSubManager>,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
    replication_manager: &Arc<crate::replication::ReplicationManager>,
    redis: &ConnectionManager,
    redis_url: &str,
    lobby_manager: &Arc<crate::lobby_manager::LobbyManager>,
    websocket_id: &str,
    region: &str,
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
                    info!("Received jwt token: {}", jwt_token);
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
                        &db,
                        &lobby_manager,
                        user_cache.clone(),
                        &lobby,
                        &metadata,
                        &ws_tx,
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
                            if lobby_manager
                                .is_lobby_host(&lobby_handle.lobby_code, metadata.user_id)
                                .await?
                            {
                                lobby_manager
                                    .set_lobby_preferences(
                                        &lobby_handle.lobby_code,
                                        &lobby_manager::LobbyPreferences {
                                            selected_modes,
                                            competitive,
                                        },
                                    )
                                    .await?;
                            } else {
                                let json_msg = serde_json::to_string(&WSMessage::AccessDenied {
                                    reason: "Only the host can update lobby settings".to_string(),
                                })?;
                                ws_tx.send(Message::Text(json_msg.into())).await?;
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
                WSMessage::QueueForMatch {
                    game_type,
                    queue_mode,
                } => {
                    info!(
                        "User {} ({}) queuing for match type: {:?}, mode: {:?}",
                        metadata.username, metadata.user_id, game_type, queue_mode
                    );

                    if let Some(ref lobby_handle) = lobby {
                        if !lobby_manager
                            .is_lobby_host(&lobby_handle.lobby_code, metadata.user_id)
                            .await?
                        {
                            let response = WSMessage::AccessDenied {
                                reason: "Only the host can queue the lobby for matchmaking"
                                    .to_string(),
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

                        if let Err(e) = queue_existing_lobby_for_game_types(
                            lobby_handle,
                            &[game_type.clone()],
                            &queue_mode,
                            &db,
                            &lobby_manager,
                            &matchmaking_manager,
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

                            // Update lobby state to "queued"
                            if let Err(e) = lobby_manager
                                .update_lobby_state(&lobby_handle.lobby_code, "queued")
                                .await
                            {
                                error!("Failed to update lobby state: {}", e);
                                let response = WSMessage::AccessDenied {
                                    reason: format!("Failed to queue lobby: {}", e),
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
                                // Revert lobby state
                                let _ = lobby_manager
                                    .update_lobby_state(&lobby_handle.lobby_code, "waiting")
                                    .await;
                                return Ok(ConnectionState::Authenticated {
                                    metadata,
                                    lobby_handle: Some(lobby_handle),
                                    game_id,
                                    websocket_id,
                                });
                            }
                            drop(mm_guard);

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
                        if !lobby_manager
                            .is_lobby_host(&lobby_handle.lobby_code, metadata.user_id)
                            .await?
                        {
                            let response = WSMessage::AccessDenied {
                                reason: "Only the host can queue the lobby for matchmaking"
                                    .to_string(),
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

                        if let Err(e) = queue_existing_lobby_for_game_types(
                            lobby_handle,
                            &game_types,
                            &queue_mode,
                            &db,
                            &lobby_manager,
                            &matchmaking_manager,
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

                            // Update lobby state to "queued"
                            if let Err(e) = lobby_manager
                                .update_lobby_state(&lobby_handle.lobby_code, "queued")
                                .await
                            {
                                error!("Failed to update lobby state: {}", e);
                                let response = WSMessage::AccessDenied {
                                    reason: format!("Failed to queue lobby: {}", e),
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
                                // Revert lobby state
                                let _ = lobby_manager
                                    .update_lobby_state(&lobby_handle.lobby_code, "waiting")
                                    .await;
                                return Ok(ConnectionState::Authenticated {
                                    metadata,
                                    lobby_handle: Some(lobby_handle),
                                    game_id,
                                    websocket_id,
                                });
                            }
                            drop(mm_guard);

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
                WSMessage::JoinGame(game_id) => {
                    info!(
                        "User {} ({}) joining game {}",
                        metadata.username, metadata.user_id, game_id
                    );

                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id: Some(game_id),
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

                    // Remove from matchmaking queue using Redis-based matchmaking
                    let mut matchmaking_manager = matchmaking_manager.lock().await;
                    match remove_from_matchmaking_queue(
                        &mut *matchmaking_manager,
                        metadata.user_id as u32,
                    )
                    .await
                    {
                        Ok(()) => {
                            info!("User {} removed from matchmaking queue", metadata.user_id);
                        }
                        Err(e) => {
                            error!("Failed to remove user from matchmaking queue: {}", e);
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
                                return Ok(ConnectionState::Authenticated {
                                    metadata,
                                    lobby_handle: None,
                                    game_id,
                                    websocket_id,
                                });
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
                                return Ok(ConnectionState::Authenticated {
                                    metadata,
                                    lobby_handle: Some(lobby_handle),
                                    game_id,
                                    websocket_id,
                                });
                            }
                        }
                    } else {
                        let response = WSMessage::AccessDenied {
                            reason: "You are not currently in a lobby".to_string(),
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
                WSMessage::GameCommand(command_message) => {
                    if let Some(game_id) = game_id {
                        // Submit command via PubSub
                        let partition_id = game_id % PARTITION_COUNT;

                        let event = StreamEvent::GameCommandSubmitted {
                            game_id,
                            user_id: metadata.user_id as u32,
                            command: command_message,
                        };

                        // Send command via PubSub
                        match pubsub_manager.publish_command(partition_id, &event).await {
                            Ok(_) => {
                                debug!(
                                    "Successfully submitted game command via PubSub: {:?}",
                                    event
                                );
                                Ok(ConnectionState::Authenticated {
                                    metadata,
                                    lobby_handle: lobby,
                                    game_id: Some(game_id),
                                    websocket_id,
                                })
                            }
                            Err(e) => {
                                error!("Failed to submit command via PubSub: {}", e);
                                Ok(ConnectionState::Authenticated {
                                    metadata,
                                    lobby_handle: lobby,
                                    game_id: Some(game_id),
                                    websocket_id,
                                })
                            }
                        }
                    } else {
                        warn!(
                            "Received GameCommand there is no game id in websocket state: {:?}",
                            command_message
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

#[derive(Debug, Deserialize)]
struct LobbyUpdatePayload {
    lobby_code: String,
    members: BTreeMap<u32, lobby_manager::LobbyMember>,
    host_user_id: i32,
    state: String,
    preferences: lobby_manager::LobbyPreferences,
}

/// Subscribe to lobby updates and forward to WebSocket client
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

async fn check_game_host(db: &Arc<dyn Database>, game_id: u32, user_id: i32) -> Result<bool> {
    let host_user_id = db.get_custom_lobby_host(game_id as i32).await?;
    Ok(host_user_id == Some(user_id))
}

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
