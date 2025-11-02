use std::future;
use crate::api::auth::validate_username;
use crate::db::Database;
use crate::game_executor::{PARTITION_COUNT, StreamEvent};
use crate::matchmaking_manager::MatchmakingManager;
use crate::pubsub_manager::PubSubManager;
use crate::redis_keys::RedisKeys;
use crate::ws_matchmaking::remove_from_matchmaking_queue;
use crate::lobby_manager;
use anyhow::{Context, Result};
use chrono::Utc;
use common::{
    DEFAULT_TICK_INTERVAL_MS, GameCommandMessage, GameEvent, GameEventMessage, GameState,
    GameStatus,
};
use futures_util::future::join_all;
use futures_util::{SinkExt, Stream};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
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
use crate::lobby_manager::LobbyJoinHandle;
use crate::user_cache::UserCache;

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
    Ping,
    Pong,
    // Clock synchronization messages
    ClockSyncRequest {
        client_time: i64,
    },
    ClockSyncResponse {
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
    // Custom game messages
    CreateCustomGame {
        settings: common::CustomGameSettings,
    },
    JoinCustomGame {
        game_code: String,
    },
    UpdateCustomGameSettings {
        settings: common::CustomGameSettings,
    },
    StartCustomGame,
    SpectateGame {
        game_id: u32,
        game_code: Option<String>,
    },
    // Solo game messages
    CreateSoloGame,
    // Custom game responses
    CustomGameCreated {
        game_id: u32,
        game_code: String,
    },
    CustomGameJoined {
        game_id: u32,
    },
    CustomGameSettingsUpdated {
        settings: common::CustomGameSettings,
    },
    CustomGameStarting,
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
    // Play Again / Requeue lobby messages
    RequeueLobby,
    LobbyRequeued {
        lobby_code: String,
    },
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
    user_cache.remove_from_redis(metadata.user_id as u32).await?;

    if let(Some(lobby)) = lobby {
        lobby_manager.publish_lobby_update(lobby.lobby_code.as_str()).await?;
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
        game_id: Option<u32>,   // Some when user is in a game
        websocket_id: String,   // Unique ID for this websocket connection
    },
}

/// Handle WebSocket connection from Axum
pub async fn handle_websocket(
    socket: axum::extract::ws::WebSocket,
    db: Arc<dyn Database>,
    jwt_verifier: Arc<dyn JwtVerifier>,
    redis_url: String,
    replication_manager: Arc<crate::replication::ReplicationManager>,
    cancellation_token: CancellationToken,
    lobby_manager: Arc<crate::lobby_manager::LobbyManager>,
    region: String,
) {
    info!("New WebSocket connection established");

    // Create PubSub manager
    let pubsub = match PubSubManager::new(&redis_url).await {
        Ok(ps) => ps,
        Err(e) => {
            error!("Failed to create pubsub manager: {}", e);
            return;
        }
    };

    // Create matchmaking manager
    let matchmaking_manager = match MatchmakingManager::new(&redis_url).await {
        Ok(mgr) => Arc::new(Mutex::new(mgr)),
        Err(e) => {
            error!("Failed to create matchmaking manager: {}", e);
            return;
        }
    };

    // Process the WebSocket connection
    if let Err(e) = handle_websocket_connection(
        socket,
        db,
        pubsub,
        matchmaking_manager,
        jwt_verifier,
        cancellation_token,
        replication_manager,
        redis_url,
        lobby_manager,
        region,
    ).await {
        error!("WebSocket connection error: {}", e);
    }
}

/// Internal function to handle the WebSocket connection logic
async fn handle_websocket_connection(
    ws_stream: axum::extract::ws::WebSocket,
    db: Arc<dyn Database>,
    mut pubsub: PubSubManager,
    matchmaking_manager: Arc<Mutex<MatchmakingManager>>,
    jwt_verifier: Arc<dyn JwtVerifier>,
    cancellation_token: CancellationToken,
    replication_manager: Arc<crate::replication::ReplicationManager>,
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
    let mut lobby_update_handle: Option<JoinHandle<()>> = None;

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
    let redis_url_for_counts = redis_url.clone();
    let _user_count_task = tokio::spawn(async move {
        if let Err(e) =
            subscribe_to_user_count_updates(redis_url_for_counts, ws_tx_for_counts).await
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
                ConnectionState::Authenticated { lobby_handle: Some(lobby), .. } => {
                    lobby.rx.recv().await
                }
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
                                    let was_in_lobby = matches!(&state, ConnectionState::Authenticated { lobby_code: Some(_), .. });

                                    match process_ws_message(
                                        state,
                                        ws_message,
                                        &jwt_verifier,
                                        &db,
                                        &ws_tx,
                                        &mut pubsub,
                                        &matchmaking_manager,
                                        &replication_manager,
                                        &redis_url,
                                        &lobby_manager,
                                        &websocket_id,
                                        &region,
                                    ).await {
                                        Ok(new_state) => {
                                            // Check if we're entering a game or lobby
                                            let entering_game = matches!(&new_state, ConnectionState::Authenticated { game_id: Some(_), .. }) && !was_in_game;
                                            let entering_lobby = matches!(&new_state, ConnectionState::Authenticated { lobby_code: Some(_), .. }) && !was_in_lobby;
                                            let leaving_lobby = was_in_lobby && !matches!(&new_state, ConnectionState::Authenticated { lobby_code: Some(_), .. });
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
                                                    let redis_url_clone = redis_url.clone();

                                                    game_chat_handle = Some(tokio::spawn(async move {
                                                        if let Err(e) = subscribe_to_game_chat(
                                                            game_id,
                                                            redis_url_clone,
                                                            ws_tx_clone,
                                                        )
                                                        .await
                                                        {
                                                            error!("Game chat subscription failed: {}", e);
                                                        }
                                                    }));

                                                    match load_game_chat_history(&redis_url, game_id).await {
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
                                                if let ConnectionState::Authenticated { lobby_code: Some(lobby_code), .. } = &new_state {
                                                    // Subscribe to lobby updates if entering a lobby
                                                    if let Some(handle) = lobby_update_handle.take() {
                                                        handle.abort();
                                                    }
                                                    if let Some(handle) = lobby_chat_handle.take() {
                                                        handle.abort();
                                                    }

                                                    let lobby_code_for_updates = lobby_code.clone();
                                                    let lobby_code_for_match = lobby_code.clone();
                                                    let ws_tx_clone = ws_tx.clone();
                                                    let redis_url_clone = redis_url.clone();
                                                    let lobby_manager_clone = lobby_manager.clone();

                                                    lobby_update_handle = Some(tokio::spawn(async move {
                                                        if let Err(e) = subscribe_to_lobby_updates(
                                                            lobby_code_for_updates,
                                                            redis_url_clone,
                                                            ws_tx_clone,
                                                            lobby_manager_clone,
                                                        ).await {
                                                            error!("Lobby update subscription failed: {}", e);
                                                        }
                                                    }));

                                                    // Subscribe to lobby match notifications
                                                    if let Some(handle) = lobby_match_handle.take() {
                                                        handle.abort();
                                                    }

                                                    let ws_tx_clone_for_match = ws_tx.clone();
                                                    let redis_url_clone_for_match = redis_url.clone();
                                                    let replication_manager_clone_for_match = replication_manager.clone();

                                                    lobby_match_handle = Some(tokio::spawn(async move {
                                                        let redis_keys = crate::redis_keys::RedisKeys::new();
                                                        let channel = redis_keys.matchmaking_lobby_notification_channel(&lobby_code_for_match);
                                                        info!("Member subscribing to lobby match notifications on channel: {}", channel);

                                                        if let Ok(client) = redis::Client::open(redis_url_clone_for_match.as_ref()) {
                                                            if let Ok(mut pubsub) = client.get_async_pubsub().await {
                                                                if pubsub.subscribe(&channel).await.is_ok() {
                                                                    info!("Successfully subscribed to lobby match notifications for lobby '{}'", lobby_code_for_match);

                                                                    let mut pubsub_stream = pubsub.on_message();
                                                                    // Loop to handle multiple notifications (MatchFound, LobbyRequeued, etc.)
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
                                                                                    "LobbyRequeued" => {
                                                                                        if let Some(requeue_lobby_code) = notification["lobby_code"].as_str() {
                                                                                            info!("Lobby '{}' has been requeued by host, sending notification to member", requeue_lobby_code);
                                                                                            // Send LobbyRequeued message to this client
                                                                                            let requeue_msg = WSMessage::LobbyRequeued {
                                                                                                lobby_code: requeue_lobby_code.to_string(),
                                                                                            };
                                                                                            let json_msg = serde_json::to_string(&requeue_msg).unwrap();
                                                                                            let _ = ws_tx_clone_for_match.send(Message::Text(json_msg.into())).await;
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
                                                    let redis_url_clone = redis_url.clone();

                                                    let lobby_code_for_chat = lobby_code.clone();
                                                    lobby_chat_handle = Some(tokio::spawn(async move {
                                                        if let Err(e) = subscribe_to_lobby_chat(
                                                            lobby_code_for_chat,
                                                            redis_url_clone,
                                                            ws_tx_clone,
                                                        )
                                                        .await
                                                        {
                                                            error!("Lobby chat subscription failed: {}", e);
                                                        }
                                                    }));

                                                    match load_lobby_chat_history(&redis_url, &lobby_code).await {
                                                        Ok(history) if !history.is_empty() => {
                                                            let history_message = WSMessage::LobbyChatHistory {
                                                                lobby_code: lobby_code.clone(),
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
                                                                            lobby_code, e
                                                                        );
                                                                    }
                                                                }
                                                                Err(e) => {
                                                                    error!(
                                                                        "Failed to serialize lobby chat history for lobby '{}': {}",
                                                                        lobby_code, e
                                                                    );
                                                                }
                                                            }
                                                        }
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            warn!(
                                                                "Failed to load lobby chat history for lobby '{}': {}",
                                                                lobby_code, e
                                                            );
                                                        }
                                                    }
                                                }
                                            }

                                            // Abort lobby subscription when leaving lobby
                                            // BUT keep lobby_match_handle active if entering Authenticated with a lobby_code (for Play Again notifications)
                                            if leaving_lobby {
                                                let keep_match_subscription = matches!(&new_state, ConnectionState::Authenticated { lobby_code: Some(_), .. });

                                                if let Some(handle) = lobby_update_handle.take() {
                                                    handle.abort();
                                                    debug!("Aborted lobby update subscription");
                                                }

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
    match &state {
        ConnectionState::Authenticated { lobby_handle: Some(lobby_handle), .. } => {
            lobby_handle.close().await?;
        }
        _ => {}
    }
    
    // Close game subscription if still in a game
    match &state {
        ConnectionState::Authenticated { game_handle: Some(game_handle), ..} => {
            game_handle.close().await?;
        }
        _ => {}
    }

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

async fn publish_lobby_chat_message(redis_url: &str, payload: LobbyChatBroadcast) -> Result<()> {
    let client = redis::Client::open(redis_url)
        .context("Failed to open Redis client for lobby chat publish")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("Failed to get Redis connection for lobby chat publish")?;

    let redis_keys = RedisKeys::new();
    let channel = redis_keys.lobby_chat_channel(&payload.lobby_code);
    let history_key = redis_keys.lobby_chat_history_key(&payload.lobby_code);
    let serialized =
        serde_json::to_string(&payload).context("Failed to serialize lobby chat payload")?;

    conn.publish::<_, _, ()>(&channel, serialized.clone())
        .await
        .context("Failed to publish lobby chat message")?;

    let _: i64 = conn
        .rpush(&history_key, serialized.clone())
        .await
        .context("Failed to append lobby chat history")?;
    let start: isize = -(CHAT_HISTORY_LIMIT as isize);
    let _: () = conn
        .ltrim(&history_key, start, -1)
        .await
        .context("Failed to trim lobby chat history")?;
    Ok(())
}

async fn publish_game_chat_message(redis_url: &str, payload: GameChatBroadcast) -> Result<()> {
    let client = redis::Client::open(redis_url)
        .context("Failed to open Redis client for game chat publish")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("Failed to get Redis connection for game chat publish")?;

    let redis_keys = RedisKeys::new();
    let channel = redis_keys.game_chat_channel(payload.game_id);
    let history_key = redis_keys.game_chat_history_key(payload.game_id);
    let serialized =
        serde_json::to_string(&payload).context("Failed to serialize game chat payload")?;

    conn.publish::<_, _, ()>(&channel, serialized.clone())
        .await
        .context("Failed to publish game chat message")?;

    let _: i64 = conn
        .rpush(&history_key, serialized.clone())
        .await
        .context("Failed to append game chat history")?;
    let start: isize = -(CHAT_HISTORY_LIMIT as isize);
    let _: () = conn
        .ltrim(&history_key, start, -1)
        .await
        .context("Failed to trim game chat history")?;
    Ok(())
}

async fn load_lobby_chat_history(
    redis_url: &str,
    lobby_code: &str,
) -> Result<Vec<LobbyChatBroadcast>> {
    let client = redis::Client::open(redis_url)
        .context("Failed to open Redis client for lobby chat history")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("Failed to get Redis connection for lobby chat history")?;

    let key = RedisKeys::new().lobby_chat_history_key(lobby_code);
    let entries: Vec<String> = conn
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

async fn load_game_chat_history(redis_url: &str, game_id: u32) -> Result<Vec<GameChatBroadcast>> {
    let client = redis::Client::open(redis_url)
        .context("Failed to open Redis client for game chat history")?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("Failed to get Redis connection for game chat history")?;

    let key = RedisKeys::new().game_chat_history_key(game_id);
    let entries: Vec<String> = conn
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
    redis_url: String,
    ws_tx: mpsc::Sender<Message>,
) -> Result<()> {
    info!("Subscribing to game {} chat", game_id);

    let client = redis::Client::open(redis_url.as_str())
        .context("Failed to create Redis client for game chat subscription")?;
    let mut pubsub_conn = client
        .get_async_pubsub()
        .await
        .context("Failed to create Redis pub/sub connection for game chat")?;

    let channel = RedisKeys::new().game_chat_channel(game_id);
    pubsub_conn
        .subscribe(&channel)
        .await
        .context("Failed to subscribe to game chat channel")?;

    let mut stream = pubsub_conn.on_message();
    while let Some(msg) = stream.next().await {
        let payload: String = match msg.get_payload() {
            Ok(payload) => payload,
            Err(e) => {
                warn!("Failed to read payload from game chat message: {}", e);
                continue;
            }
        };

        let chat_payload: GameChatBroadcast = match serde_json::from_str(&payload) {
            Ok(payload) => payload,
            Err(e) => {
                warn!("Failed to deserialize game chat payload: {}", e);
                continue;
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
    redis_url: String,
    ws_tx: mpsc::Sender<Message>,
) -> Result<()> {
    info!("Subscribing to lobby '{}' chat", lobby_code);

    let client = redis::Client::open(redis_url.as_str())
        .context("Failed to create Redis client for lobby chat subscription")?;
    let mut pubsub_conn = client
        .get_async_pubsub()
        .await
        .context("Failed to create Redis pub/sub connection for lobby chat")?;

    let channel = RedisKeys::new().lobby_chat_channel(&lobby_code);
    pubsub_conn
        .subscribe(&channel)
        .await
        .context("Failed to subscribe to lobby chat channel")?;

    let mut stream = pubsub_conn.on_message();
    while let Some(msg) = stream.next().await {
        let payload: String = match msg.get_payload() {
            Ok(payload) => payload,
            Err(e) => {
                warn!("Failed to read payload from lobby chat message: {}", e);
                continue;
            }
        };

        let chat_payload: LobbyChatBroadcast = match serde_json::from_str(&payload) {
            Ok(payload) => payload,
            Err(e) => {
                warn!("Failed to deserialize lobby chat payload: {}", e);
                continue;
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
    pubsub: &mut PubSubManager,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
    replication_manager: &Arc<crate::replication::ReplicationManager>,
    redis_url: &str,
    lobby_manager: &Arc<crate::lobby_manager::LobbyManager>,
    websocket_id: &str,
    region: &str,
) -> Result<ConnectionState> {
    use tracing::debug;
    let state_str = match &state {
        ConnectionState::Unauthenticated => "Unauthenticated",
        ConnectionState::Authenticated { lobby_code: Some(code), game_id: Some(gid), .. } => {
            debug!("Processing message in Authenticated(lobby:{}, game:{})", code, gid);
            "Authenticated(InLobby+InGame)"
        }
        ConnectionState::Authenticated { lobby_code: Some(code), game_id: None, .. } => {
            debug!("Processing message in Authenticated(lobby:{})", code);
            "Authenticated(InLobby)"
        }
        ConnectionState::Authenticated { lobby_code: None, game_id: Some(gid), .. } => {
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
                                lobby_code: None,
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
                WSMessage::Ping => {
                    // Respond with Pong even in unauthenticated state
                    let pong_msg = Message::Text(serde_json::to_string(&WSMessage::Pong)?.into());
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
            websocket_id
        } => {
            match ws_message {
                WSMessage::UpdateNickname { nickname } => {
                    if let Err(e) = handle_guest_nickname_update(
                        &db,
                        lobby_manager,
                        user_cache.clone(),
                        lobby,
                        metadata,
                        &ws_tx,
                        nickname,
                    ).await {
                        error!(
                            "Failed to update guest nickname for user {}: {}",
                            metadata.user_id, e
                        );
                    }
                    Ok(ConnectionState::Authenticated { metadata, lobby_handle: lobby, game_id, websocket_id })
                }
                WSMessage::UpdateLobbyPreferences { selected_modes, competitive } => {
                    {
                        if lobby_manager.is_lobby_host(&lobby_code, metadata.user_id).await? {
                            lobby_manager
                                .set_lobby_preferences(
                                    &lobby_code,
                                    &lobby_manager::LobbyPreferences {
                                        selected_modes,
                                        competitive,
                                    }
                                )
                                .await?;
                        } else {
                            let json_msg = serde_json::to_string(&WSMessage::AccessDenied {
                                reason: "Only the host can update lobby settings".to_string(),
                            })?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                        }
                    }
                    Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id })
                }
                WSMessage::QueueForMatch {
                    game_type,
                    queue_mode,
                } => {
                    info!(
                        "User {} ({}) queuing for match type: {:?}, mode: {:?}",
                        metadata.username, metadata.user_id, game_type, queue_mode
                    );

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
                                lobby.lobby_code, metadata.user_id
                            );
                            // Join the newly created lobby
                            if let Err(e) = lobby_manager
                                .join_lobby(
                                    &lobby.lobby_code,
                                    metadata.user_id,
                                    metadata.username.clone(),
                                    websocket_id.to_string(),
                                    region.to_string(),
                                )
                                .await
                            {
                                error!("Failed to join auto-created lobby: {}", e);
                                let response = WSMessage::AccessDenied {
                                    reason: format!("Failed to create matchmaking lobby: {}", e),
                                };
                                let json_msg = serde_json::to_string(&response)?;
                                ws_tx.send(Message::Text(json_msg.into())).await?;
                                return Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id });
                            }

                            // Fetch lobby members (should be just this user)
                            let members = match lobby_manager.get_lobby_members(&lobby.lobby_code).await {
                                Ok(m) => {
                                    info!(
                                        lobby_id = lobby.lobby_code,
                                        member_count = m.len(),
                                        "Fetched lobby members for auto-created lobby"
                                    );
                                    for (idx, member) in m.iter().enumerate() {
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
                                    return Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id });
                                }
                            };

                            // Update lobby state to "queued"
                            if let Err(e) =
                                lobby_manager.update_lobby_state(&lobby.lobby_code, "queued").await
                            {
                                error!("Failed to update lobby state: {}", e);
                                let response = WSMessage::AccessDenied {
                                    reason: format!("Failed to queue lobby: {}", e),
                                };
                                let json_msg = serde_json::to_string(&response)?;
                                ws_tx.send(Message::Text(json_msg.into())).await?;
                                return Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id });
                            }

                            // Add the auto-created lobby to matchmaking queue
                            let mut mm_guard = matchmaking_manager.lock().await;
                            if let Err(e) = mm_guard
                                .add_lobby_to_queue(
                                    &lobby.lobby_code,
                                    members.clone(),
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
                                let _ = lobby_manager.update_lobby_state(&lobby.lobby_code, "waiting").await;
                                return Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id });
                            }
                            drop(mm_guard);

                            info!(
                                "Auto-created lobby {} for solo player {} and added to matchmaking queue",
                                lobby.lobby_code, metadata.user_id
                            );

                            // Transition to InLobby state - lobby match notifications will be handled automatically
                            Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_code: Some(lobby.lobby_code),
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
                            Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id })
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
                                lobby.lobby_code, metadata.user_id
                            );
                            // Join the newly created lobby
                            if let Err(e) = lobby_manager
                                .join_lobby(
                                    &lobby.lobby_code,
                                    metadata.user_id,
                                    metadata.username.clone(),
                                    websocket_id.to_string(),
                                    region.to_string(),
                                )
                                .await
                            {
                                error!("Failed to join auto-created lobby: {}", e);
                                let response = WSMessage::AccessDenied {
                                    reason: format!("Failed to create matchmaking lobby: {}", e),
                                };
                                let json_msg = serde_json::to_string(&response)?;
                                ws_tx.send(Message::Text(json_msg.into())).await?;
                                return Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id });
                            }

                            // Fetch lobby members (should be just this user)
                            let members = match lobby_manager.get_lobby_members(&lobby.lobby_code).await {
                                Ok(m) => {
                                    info!(
                                        lobby_id = lobby.lobby_code,
                                        member_count = m.len(),
                                        "Fetched lobby members for auto-created lobby"
                                    );
                                    for (idx, member) in m.iter().enumerate() {
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
                                    return Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id });
                                }
                            };

                            // Update lobby state to "queued"
                            if let Err(e) =
                                lobby_manager.update_lobby_state(&lobby.lobby_code, "queued").await
                            {
                                error!("Failed to update lobby state: {}", e);
                                let response = WSMessage::AccessDenied {
                                    reason: format!("Failed to queue lobby: {}", e),
                                };
                                let json_msg = serde_json::to_string(&response)?;
                                ws_tx.send(Message::Text(json_msg.into())).await?;
                                return Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id });
                            }

                            // Add the auto-created lobby to matchmaking queue with multiple game types
                            let mut mm_guard = matchmaking_manager.lock().await;
                            if let Err(e) = mm_guard
                                .add_lobby_to_queue(
                                    &lobby.lobby_code,
                                    members.clone(),
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
                                let _ = lobby_manager.update_lobby_state(&lobby.lobby_code, "waiting").await;
                                return Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id });
                            }
                            drop(mm_guard);

                            info!(
                                "Auto-created lobby {} for solo player {} and added to matchmaking queue for multiple game types",
                                lobby.lobby_code, metadata.user_id
                            );

                            // Transition to InLobby state - lobby match notifications will be handled automatically
                            Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_code: Some(lobby.lobby_code),
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
                            Ok(ConnectionState::Authenticated { metadata, lobby_handle: lobby, game_id, websocket_id })
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

                    Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id })
                }
                WSMessage::Ping => {
                    // Respond with Pong
                    let pong_msg = Message::Text(serde_json::to_string(&WSMessage::Pong)?.into());
                    ws_tx.send(pong_msg).await?;
                    Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id })
                }
                WSMessage::ClockSyncRequest { client_time } => {
                    // Respond with server time for clock synchronization
                    let server_time = chrono::Utc::now().timestamp_millis();
                    let response = WSMessage::ClockSyncResponse {
                        client_time,
                        server_time,
                    };
                    let json_msg = serde_json::to_string(&response)?;
                    ws_tx.send(Message::Text(json_msg.into())).await?;
                    Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id })
                }
                WSMessage::GameEvent(event_msg) => {
                    // Forward game events to the client
                    warn!(
                        "Received game event in authenticated state: {:?}",
                        event_msg
                    );
                    Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id })
                }
                WSMessage::CreateCustomGame { settings } => {
                    info!(
                        "User {} ({}) creating custom game",
                        metadata.username, metadata.user_id
                    );

                    match create_custom_game(
                        db,
                        pubsub,
                        metadata.user_id,
                        metadata.username.clone(),
                        settings,
                    )
                    .await
                    {
                        Ok((game_id, game_code)) => {
                            // Send success response
                            let response = WSMessage::CustomGameCreated { game_id, game_code };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;

                            // Transition to in-game state
                            Ok(ConnectionState::Authenticated {
                                metadata,
                                game_id: Some(game_id),
                                lobby_code: None,
                                websocket_id: websocket_id.to_string(), // Custom games don't use lobbies
                            })
                        }
                        Err(e) => {
                            error!("Failed to create custom game: {}", e);
                            let response = WSMessage::AccessDenied {
                                reason: format!("Failed to create game: {}", e),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id })
                        }
                    }
                }
                WSMessage::JoinCustomGame { game_code } => {
                    info!(
                        "User {} ({}) joining custom game with code: {}",
                        metadata.username, metadata.user_id, game_code
                    );

                    match join_custom_game(db, metadata.user_id, &game_code).await {
                        Ok(game_id) => {
                            // Send success response
                            let response = WSMessage::CustomGameJoined { game_id };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;

                            // Transition to in-game state
                            Ok(ConnectionState::Authenticated {
                                metadata,
                                game_id: Some(game_id),
                                lobby_code: None,
                                websocket_id: websocket_id.to_string(), // Custom games don't use lobbies
                            })
                        }
                        Err(e) => {
                            error!("Failed to join custom game: {}", e);
                            let response = WSMessage::AccessDenied {
                                reason: format!("Failed to join game: {}", e),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id })
                        }
                    }
                }
                WSMessage::SpectateGame { game_id: spectate_game_id, game_code } => {
                    info!(
                        "User {} ({}) attempting to spectate game {}",
                        metadata.username, metadata.user_id, spectate_game_id
                    );

                    match spectate_game(db, metadata.user_id, spectate_game_id, game_code.as_deref()).await {
                        Ok(actual_game_id) => {
                            // Send success response
                            let response = WSMessage::SpectatorJoined;
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;

                            // Transition to in-game state as spectator
                            Ok(ConnectionState::Authenticated {
                                metadata,
                                game_id: Some(actual_game_id),
                                lobby_code: None, // Spectators don't use lobbies
                                websocket_id: websocket_id.to_string(),
                            })
                        }
                        Err(e) => {
                            error!("Failed to spectate game: {}", e);
                            let response = WSMessage::AccessDenied {
                                reason: format!("Failed to spectate game: {}", e),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id })
                        }
                    }
                }
                WSMessage::CreateSoloGame => {
                    info!(
                        "User {} ({}) creating solo game",
                        metadata.username, metadata.user_id
                    );

                    match create_solo_game(db, pubsub, metadata.user_id, metadata.username.clone())
                        .await
                    {
                        Ok(created_game_id) => {
                            // Send success response
                            let response = WSMessage::SoloGameCreated { game_id: created_game_id };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;

                            Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id: Some(created_game_id), websocket_id: ws_id })
                        }
                        Err(e) => {
                            error!("Failed to create solo game: {}", e);
                            let response = WSMessage::AccessDenied {
                                reason: format!("Failed to create solo game: {}", e),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id })
                        }
                    }
                }
                WSMessage::CreateLobby => {
                    info!(
                        "User {} ({}) creating lobby in region {}",
                        metadata.username, metadata.user_id, region
                    );

                    match lobby_manager.create_lobby(metadata.user_id, region).await {
                        Ok(lobby) => {
                            // Join the lobby
                            if let Err(e) = lobby_manager
                                .join_lobby(
                                    &lobby.lobby_code,
                                    metadata.user_id,
                                    metadata.username.clone(),
                                    websocket_id.to_string(),
                                    region.to_string(),
                                )
                                .await
                            {
                                error!("Failed to join newly created lobby: {}", e);
                                let response = WSMessage::AccessDenied {
                                    reason: format!("Failed to join lobby: {}", e),
                                };
                                let json_msg = serde_json::to_string(&response)?;
                                ws_tx.send(Message::Text(json_msg.into())).await?;
                                return Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id });
                            }

                            // Send success response
                            let response = WSMessage::LobbyCreated {
                                lobby_code: lobby.lobby_code.clone(),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;

                            // Transition to InLobby state
                            Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_code: Some(lobby.lobby_code),
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
                            Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id })
                        }
                    }
                }
                WSMessage::JoinLobby { lobby_code } => {
                    info!(
                        "User {} ({}) joining lobby with code: {}",
                        metadata.username, metadata.user_id, lobby_code
                    );

                    let lobby = lobby_manager.join_lobby()?;

                    match lobby_manager.get_lobby_metadata(&lobby_code).await {
                        Ok(Some(lobby)) => {
                            // Check region match
                            if lobby.region != region {
                                warn!(
                                    "Lobby '{}' is in region {}, user is in region {}",
                                    lobby.lobby_code, lobby.region, region
                                );

                                // Get WebSocket URL for the target region from database
                                let ws_url = match db.get_region_ws_url(&lobby.region).await? {
                                    Some(url) => url,
                                    None => {
                                        let response = WSMessage::AccessDenied {
                                            reason: format!(
                                                "No servers available in region {}",
                                                lobby.region
                                            ),
                                        };
                                        let json_msg = serde_json::to_string(&response)?;
                                        ws_tx.send(Message::Text(json_msg.into())).await?;
                                        return Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id });
                                    }
                                };

                                let response = WSMessage::LobbyRegionMismatch {
                                    target_region: lobby.region.clone(),
                                    ws_url,
                                    lobby_code: lobby_code.clone(),
                                };
                                let json_msg = serde_json::to_string(&response)?;
                                ws_tx.send(Message::Text(json_msg.into())).await?;
                                return Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id });
                            }

                            // Join the lobby
                            if let Err(e) = lobby_manager
                                .join_lobby(
                                    &lobby.lobby_code,
                                    metadata.user_id,
                                    metadata.username.clone(),
                                    websocket_id.to_string(),
                                    region.to_string(),
                                ).await
                            {
                                error!("Failed to join lobby: {}", e);
                                let response = WSMessage::AccessDenied {
                                    reason: format!("Failed to join lobby: {}", e),
                                };
                                let json_msg = serde_json::to_string(&response)?;
                                ws_tx.send(Message::Text(json_msg.into())).await?;
                                return Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id });
                            }

                            // Send success response
                            let response = WSMessage::JoinedLobby {
                                lobby_code: lobby.lobby_code.clone(),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;

                            // Transition to InLobby state
                            Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_code: Some(lobby.lobby_code),
                                game_id: None,
                                websocket_id: websocket_id.to_string(),
                            })
                        }
                        Ok(None) => {
                            let response = WSMessage::AccessDenied {
                                reason: "Lobby not found".to_string(),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id })
                        }
                        Err(e) => {
                            error!("Failed to get lobby by code: {}", e);
                            let response = WSMessage::AccessDenied {
                                reason: format!("Failed to find lobby: {}", e),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id })
                        }
                    }
                }
                WSMessage::Chat(_) => {
                    let response = WSMessage::AccessDenied {
                        reason: "Chat is only available in a lobby or game".to_string(),
                    };
                    let json_msg = serde_json::to_string(&response)?;
                    ws_tx.send(Message::Text(json_msg.into())).await?;
                    Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id })
                }
                _ => {
                    warn!(
                        "Unexpected message in authenticated state: {:?}",
                        ws_message
                    );
                    Ok(ConnectionState::Authenticated { metadata, lobby_code: Some(lobby_code), game_id, websocket_id: ws_id })
                }
            }
        }
        ConnectionState::Authenticated { metadata, lobby_code: None, game_id, websocket_id: ws_id } => {
            // Handle authenticated users not in a lobby (can create/join lobbies)
            match ws_message {
                WSMessage::Ping => {
                    let pong_msg = Message::Text(serde_json::to_string(&WSMessage::Pong)?.into());
                    ws_tx.send(pong_msg).await?;
                    Ok(ConnectionState::Authenticated { metadata, lobby_code: None, game_id, websocket_id: ws_id })
                }
                _ => {
                    // Most other messages require being in a lobby
                    warn!("Received message {:?} while not in a lobby", ws_message);
                    Ok(ConnectionState::Authenticated { metadata, lobby_code: None, game_id, websocket_id: ws_id })
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
    redis_url: String,
    ws_tx: mpsc::Sender<Message>,
) -> Result<()> {
    use redis::aio::Connection;

    // Create Redis client for pub/sub
    let client = redis::Client::open(redis_url.as_str())
        .context("Failed to create Redis client for user count subscription")?;

    let mut pubsub_conn = client
        .get_async_pubsub()
        .await
        .context("Failed to create Redis pub/sub connection")?;

    // Subscribe to user count updates channel
    pubsub_conn
        .subscribe("user_count_updates")
        .await
        .context("Failed to subscribe to user_count_updates channel")?;

    info!("Subscribed to user count updates");

    // Listen for messages
    let mut pubsub_stream = pubsub_conn.on_message();

    while let Some(msg) = pubsub_stream.next().await {
        let payload: String = match msg.get_payload() {
            Ok(p) => p,
            Err(e) => {
                warn!("Failed to get payload from user count update: {}", e);
                continue;
            }
        };

        // Parse the region counts
        let region_counts: std::collections::HashMap<String, u32> =
            match serde_json::from_str(&payload) {
                Ok(counts) => counts,
                Err(e) => {
                    warn!("Failed to parse user count update: {}", e);
                    continue;
                }
            };

        // Create WebSocket message
        let ws_message = WSMessage::UserCountUpdate { region_counts };
        let json_msg = match serde_json::to_string(&ws_message) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to serialize user count update: {}", e);
                continue;
            }
        };

        // Send to WebSocket client
        if ws_tx.send(Message::Text(json_msg.into())).await.is_err() {
            debug!("WebSocket channel closed, stopping user count subscription");
            break;
        }
    }

    Ok(())
}

/// Subscribe to lobby updates and forward to WebSocket client
async fn subscribe_to_lobby_updates(
    lobby_code: String,
    redis_url: String,
    ws_tx: mpsc::Sender<Message>,
    lobby_manager: Arc<crate::lobby_manager::LobbyManager>,
) -> Result<()> {
    info!("Subscribing to lobby '{}' updates", lobby_code);

    // Create Redis client for pub/sub
    let client = redis::Client::open(redis_url.as_str())
        .context("Failed to create Redis client for lobby subscription")?;

    let mut pubsub_conn = client
        .get_async_pubsub()
        .await
        .context("Failed to create Redis pub/sub connection")?;

    // Subscribe to lobby update channel
    let channel = format!("lobby:{}:updates", lobby_code);
    pubsub_conn
        .subscribe(&channel)
        .await
        .context("Failed to subscribe to lobby updates channel")?;

    info!("Subscribed to lobby updates for lobby '{}'", lobby_code);

    // Listen for messages
    let mut pubsub_stream = pubsub_conn.on_message();

    while let Some(_msg) = pubsub_stream.next().await {
        // When we receive an update notification, fetch the current lobby members
        match lobby_manager.get_lobby_members(&lobby_code).await {
            Ok(members) => {
                // Get the lobby info for host_user_id
                match lobby_manager.get_lobby_metadata(&lobby_code).await {
                    Ok(Some(lobby)) => {
                        let preferences = match lobby_manager.get_lobby_preferences(&lobby_code).await
                        {
                            Ok(prefs) => prefs,
                            Err(e) => {
                                warn!(
                                    "Failed to load lobby preferences for lobby '{}': {}",
                                    lobby_code, e
                                );
                                crate::lobby_manager::LobbyPreferences::default()
                            }
                        };

                        // Send lobby update to client
                        let ws_message = WSMessage::LobbyUpdate {
                            lobby_code: lobby_code.clone(),
                            members,
                            host_user_id: lobby.host_user_id,
                            state: lobby.state.clone(),
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
                    Ok(None) => {
                        warn!("Lobby '{}' not found, stopping subscription", lobby_code);
                        break;
                    }
                    Err(e) => {
                        error!("Failed to get lobby info: {}", e);
                    }
                }
            }
            Err(e) => {
                error!("Failed to get lobby members: {}", e);
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

async fn create_custom_game(
    db: &Arc<dyn Database>,
    pubsub: &mut PubSubManager,
    user_id: i32,
    username: String,
    settings: common::CustomGameSettings,
) -> Result<(u32, String)> {
    let game_code = generate_game_code();

    // Get current server ID from database
    let server_id = db.get_server_for_load_balancing("default").await?;

    // Create lobby entry
    let lobby_id = db
        .create_custom_lobby(&game_code, user_id, &serde_json::to_value(&settings)?)
        .await?;

    // Create game entry
    let game_id = db
        .create_game(
            server_id,
            &serde_json::to_value(&common::GameType::Custom {
                settings: settings.clone(),
            })?,
            "custom",
            settings.is_private,
            Some(&game_code),
        )
        .await?;

    // Update lobby with game_id
    db.update_custom_lobby_game_id(lobby_id, game_id).await?;

    // Create game state
    let start_ms = chrono::Utc::now().timestamp_millis();
    let mut game_state = common::GameState::new(
        settings.arena_width,
        settings.arena_height,
        common::GameType::Custom { settings },
        Some(rand::random::<u64>()),
        start_ms,
    );
    game_state.game_code = Some(game_code.clone());
    game_state.host_user_id = Some(user_id as u32);

    // Add the host as the first player
    game_state.add_player(user_id as u32, Some(username))?;

    // Spawn initial food items
    game_state.spawn_initial_food();

    // Publish GameCreated event to Redis stream
    let game_id_u32 = game_id as u32;
    let partition_id = game_id_u32 % PARTITION_COUNT;

    let event = StreamEvent::GameCreated {
        game_id: game_id_u32,
        game_state: game_state.clone(),
    };

    // Publish initial snapshot when game is created
    let partition_id = game_id_u32 % crate::game_executor::PARTITION_COUNT;
    pubsub
        .publish_snapshot(partition_id, game_id_u32, &game_state)
        .await
        .context("Failed to publish initial game snapshot")?;

    // Also send GameCreated event via partition command channel
    let serialized = serde_json::to_vec(&event).context("Failed to serialize GameCreated event")?;
    pubsub
        .publish_command(partition_id, &serialized)
        .await
        .context("Failed to publish GameCreated event")?;

    Ok((game_id as u32, game_code))
}

async fn create_solo_game(
    db: &Arc<dyn Database>,
    pubsub: &mut PubSubManager,
    user_id: i32,
    username: String,
) -> Result<u32> {
    // Get current server ID from database - use the region from environment or default
    let region = std::env::var("SNAKETRON_REGION").unwrap_or_else(|_| "default".to_string());
    let server_id = db.get_server_for_load_balancing(&region).await?;

    // Create game settings for solo game
    let settings = common::CustomGameSettings {
        arena_width: 40,
        arena_height: 40,
        tick_duration_ms: DEFAULT_TICK_INTERVAL_MS,
        food_spawn_rate: 3.0,
        max_players: 1, // Solo game
        game_mode: common::GameMode::Solo,
        is_private: true,
        allow_spectators: false,
        snake_start_length: 4,
    };

    // Create game entry
    let game_id = db
        .create_game(
            server_id,
            &serde_json::to_value(&common::GameType::Custom {
                settings: settings.clone(),
            })?,
            "solo",
            true, // Solo games are private
            None, // No game code for solo games
        )
        .await?;

    // Create game state with one player
    let start_ms = chrono::Utc::now().timestamp_millis();
    let mut game_state = common::GameState::new(
        settings.arena_width,
        settings.arena_height,
        common::GameType::Custom {
            settings: settings.clone(),
        },
        Some(rand::random::<u64>()),
        start_ms,
    );

    // Add the player (only one player for solo mode)
    game_state.add_player(user_id as u32, Some(username))?;

    // Spawn initial food items
    game_state.spawn_initial_food();

    // Publish GameCreated event to Redis stream
    let game_id_u32 = game_id as u32;
    let partition_id = game_id_u32 % PARTITION_COUNT;

    let event = StreamEvent::GameCreated {
        game_id: game_id_u32,
        game_state: game_state.clone(),
    };

    // Publish initial snapshot when game is created
    let partition_id = game_id_u32 % crate::game_executor::PARTITION_COUNT;
    pubsub
        .publish_snapshot(partition_id, game_id_u32, &game_state)
        .await
        .context("Failed to publish initial game snapshot")?;

    // Also send GameCreated event via partition command channel
    let serialized = serde_json::to_vec(&event).context("Failed to serialize GameCreated event")?;
    pubsub
        .publish_command(partition_id, &serialized)
        .await
        .context("Failed to publish GameCreated event")?;

    // Start the game immediately (no waiting in solo mode)
    let status_event = StreamEvent::StatusUpdated {
        game_id: game_id as u32,
        status: GameStatus::Started {
            server_id: server_id as u64,
        },
    };

    let status_serialized =
        serde_json::to_vec(&status_event).context("Failed to serialize StatusUpdated event")?;
    pubsub
        .publish_command(partition_id, &status_serialized)
        .await
        .context("Failed to publish StatusUpdated event")?;

    Ok(game_id as u32)
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
