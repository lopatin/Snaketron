use std::pin::Pin;
use tracing::{debug, error, info, warn};
use tokio::net::{TcpListener, TcpStream};
use anyhow::{Context, Result};
use crate::db::Database;
use chrono::Utc;
use std::time::Duration;
use futures_util::future::join_all;
use futures_util::{SinkExt, Stream};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, oneshot, mpsc, Mutex, RwLock};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time::{Sleep, sleep};
use tokio_stream::StreamExt;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tungstenite::Utf8Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use common::{GameCommandMessage, GameEvent, GameEventMessage, GameStatus, GameState, DEFAULT_TICK_INTERVAL_MS};
use crate::game_executor::{StreamEvent, PARTITION_COUNT};
use crate::pubsub_manager::PubSubManager;
use crate::matchmaking_manager::MatchmakingManager;
use crate::ws_matchmaking::{add_to_matchmaking_queue, remove_from_matchmaking_queue};

#[derive(Debug, Serialize, Deserialize)]
pub enum WSMessage {
    Token(String),
    JoinGame(u32),
    LeaveGame,
    GameCommand(GameCommandMessage),
    GameEvent(GameEventMessage),
    Chat(String),
    Shutdown,
    Ping,
    Pong,
    // Clock synchronization messages
    ClockSyncRequest { client_time: i64 },
    ClockSyncResponse { client_time: i64, server_time: i64 },
    // Matchmaking messages
    QueueForMatch {
        game_type: common::GameType,
        queue_mode: common::QueueMode,  // Quickmatch or Competitive
    },
    LeaveQueue,
    // Real-time matchmaking updates
    MatchFound { game_id: u32 },
    QueueUpdate { position: u32, estimated_wait_seconds: u32 },
    QueueLeft,
    // Custom game messages
    CreateCustomGame { settings: common::CustomGameSettings },
    JoinCustomGame { game_code: String },
    UpdateCustomGameSettings { settings: common::CustomGameSettings },
    StartCustomGame,
    SpectateGame { game_id: u32, game_code: Option<String> },
    // Solo game messages
    CreateSoloGame,
    // Custom game responses
    CustomGameCreated { game_id: u32, game_code: String },
    CustomGameJoined { game_id: u32 },
    CustomGameSettingsUpdated { settings: common::CustomGameSettings },
    CustomGameStarting,
    SpectatorJoined,
    AccessDenied { reason: String },
    // Solo game responses
    SoloGameCreated { game_id: u32 },
    // High availability messages
    ServerShutdown { 
        reason: String,
        grace_period_seconds: u32,
    },
    AuthorityTransfer {
        game_id: u32,
        new_server_url: String,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserToken {
    pub user_id: i32,
}

// Player metadata to store additional user information
#[derive(Debug, Clone)]
pub struct PlayerMetadata {
    pub user_id: i32,
    pub username: String,
    pub token: String,
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
            format!("test_user_{}", token.strip_prefix("test-token-").unwrap_or("default"))
        } else {
            "test_user_default".to_string()
        };
        
        // Try to find existing user first
        let existing_user = self.db.get_user_by_username(&username).await?;
        
        let user_id = match existing_user {
            Some(user) => user.id,
            None => {
                // Create new test user
                let new_user = self.db.create_user(&username, "test_password_hash", 1000).await?;
                info!("Created test user {} with ID {}", username, new_user.id);
                new_user.id
            }
        };
        
        Ok(UserToken { user_id })
    }
}



// Connection state machine
enum ConnectionState {
    // Initial state - waiting for authentication
    Unauthenticated,
    
    // Authenticated but not in a game
    Authenticated { 
        metadata: PlayerMetadata,
    },
    
    // Authenticated and connected to a game
    InGame {
        metadata: PlayerMetadata,
        game_id: u32,
        // command_tx: mpsc::Sender<GameCommandMessage>,
        // event_rx: broadcast::Receiver<GameEventMessage>,
    },
    
    // Connection is shutting down
    ShuttingDown {
        timeout: Pin<Box<Sleep>>,
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
) -> Result<()> {
    // Split the WebSocket into send and receive parts using futures_util
    let (mut ws_sink, mut ws_stream) = futures_util::StreamExt::split(ws_stream);

    // Create a channel for sending messages to the WebSocket
    let (ws_tx, mut ws_rx) = mpsc::channel::<Message>(1024);

    // Start in unauthenticated state
    let mut state = ConnectionState::Unauthenticated;

    // Create a shutdown timeout that starts as a never-completing future
    let shutdown_timeout = tokio::time::sleep(Duration::from_secs(u64::MAX));
    tokio::pin!(shutdown_timeout);
    let mut shutdown_started = false;

    // Will be used to track Redis stream subscription for game events
    let mut game_event_handle: Option<JoinHandle<()>> = None;

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
                },
                _ => continue,
            };

            if let Err(e) = ws_sink.send(axum_msg).await {
                error!("Failed to send message to WebSocket: {}", e);
                break;
            }
        }
    });

    loop {
        let state_name = match &state {
            ConnectionState::Unauthenticated => "Unauthenticated".to_string(),
            ConnectionState::Authenticated { .. } => "Authenticated".to_string(),
            ConnectionState::InGame { game_id, .. } => format!("InGame({})", game_id),
            ConnectionState::ShuttingDown { .. } => "ShuttingDown".to_string(),
        };
        debug!("WS: Select loop iteration, current state: {}", state_name);

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

                // Transition to shutting down state
                state = ConnectionState::ShuttingDown {
                    timeout: Box::pin(tokio::time::sleep(Duration::from_secs(10))),
                };
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
                                    let was_in_game = matches!(&state, ConnectionState::InGame { .. });

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
                                    ).await {
                                        Ok(new_state) => {
                                            // Check if we're entering a game
                                            let entering_game = matches!(&new_state, ConnectionState::InGame { .. }) && !was_in_game;
                                            debug!("State transitioned to: entering_game: {}", entering_game);

                                            // Handle state transitions
                                            if entering_game {
                                                if let ConnectionState::InGame { game_id, metadata, .. } = &new_state {
                                                    // Subscribe to game events if entering a game
                                                    if let Some(handle) = game_event_handle.take() {
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
    if let Some(handle) = game_event_handle {
        handle.abort();
    }
    forward_task.abort();

    info!("WebSocket connection closed");
    Ok(())
}

// Helper function to subscribe to game events
async fn subscribe_to_game_events(
    game_id: u32,
    user_id: u32,
    ws_tx: mpsc::Sender<Message>,
    replication_manager: Arc<crate::replication::ReplicationManager>,
    db: Arc<dyn Database>,
) {
    info!("Subscribing to game {} events for user {}", game_id, user_id);

    let (game_state, mut rx) = match replication_manager.subscribe_to_game(game_id).await {
        Ok(result) => result,
        Err(e) => {
            // If failed to get from memory, try to get from database
            info!("Failed to subscribe to game {} from memory, checking database: {}", game_id, e);

            match db.get_game_by_id(game_id as i32).await {
                Ok(Some(game)) => {
                    if let Some(game_state_json) = game.game_state {
                        match serde_json::from_value::<GameState>(game_state_json) {
                            Ok(game_state) => {
                                info!("Loaded game {} state from database", game_id);
                                // Send the loaded state as an initial snapshot
                                let snapshot_event = GameEventMessage {
                                    game_id,
                                    tick: game_state.tick,
                                    sequence: 0,
                                    user_id: Some(user_id),
                                    event: GameEvent::Snapshot { game_state: game_state.clone() },
                                };
                                let json = serde_json::to_string(&WSMessage::GameEvent(snapshot_event)).unwrap();
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
        game_id,
        tick: game_state.tick,
        sequence: 0,
        user_id: Some(user_id),
        event: GameEvent::Snapshot { game_state: game_state.clone() },
    };
    let json = serde_json::to_string(&WSMessage::GameEvent(snapshot_event)).unwrap();
    let _ = ws_tx.send(Message::Text(json.into())).await;

    while let Ok(event_msg) = rx.recv().await {
        // Check if the game has ended
        if let GameEvent::StatusUpdated { status } = &event_msg.event {
            if matches!(status, GameStatus::Complete { .. }) {
                info!("Game {} completed, stopping event subscription", game_id);
                // Send the final event before breaking
                let json = serde_json::to_string(&WSMessage::GameEvent(event_msg)).unwrap();
                let _ = ws_tx.send(Message::Text(json.into())).await;
                break;
            }
        }

        let json = serde_json::to_string(&WSMessage::GameEvent(event_msg)).unwrap();
        if ws_tx.send(Message::Text(json.into())).await.is_err() {
            break;
        }
    }
}


async fn process_ws_message(
    state: ConnectionState,
    ws_message: WSMessage,
    jwt_verifier: &Arc<dyn JwtVerifier>,
    db: &Arc<dyn Database>,
    ws_tx: &mpsc::Sender<Message>,
    pubsub: &mut PubSubManager,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
    replication_manager: &Arc<crate::replication::ReplicationManager>,
    redis_url: &str,
) -> Result<ConnectionState> {
    use tracing::debug;
    let state_str = match &state {
        ConnectionState::Unauthenticated => "Unauthenticated",
        ConnectionState::Authenticated { .. } => "Authenticated",
        ConnectionState::InGame { game_id, .. } => {
            debug!("Processing message in InGame({})", game_id);
            "InGame"
        },
        ConnectionState::ShuttingDown { .. } => "ShuttingDown",
    };
    debug!("Processing message: {:?} in state: {}", ws_message, state_str);

    match state {
        ConnectionState::Unauthenticated => {
            match ws_message {
                WSMessage::Token(jwt_token) => {
                    info!("Received jwt token: {}", jwt_token);
                    match jwt_verifier.verify(&jwt_token).await {
                        Ok(user_token) => {
                            info!("Token verified successfully, user_id: {}", user_token.user_id);
                            
                            // Fetch username from database
                            let user = db.get_user_by_id(user_token.user_id)
                                .await?
                                .ok_or_else(|| anyhow::anyhow!("User not found"))?;
                            let username = user.username;
                            
                            // Create player metadata
                            let metadata = PlayerMetadata {
                                user_id: user_token.user_id,
                                username,
                                token: jwt_token.clone(),
                            };
                            
                            info!("User authenticated: {} (id: {})", metadata.username, metadata.user_id);
                            Ok(ConnectionState::Authenticated { metadata })
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
        ConnectionState::Authenticated { metadata } => {
            match ws_message {
                WSMessage::QueueForMatch { game_type, queue_mode } => {
                    info!("User {} ({}) queuing for match type: {:?}, mode: {:?}", metadata.username, metadata.user_id, game_type, queue_mode);

                    // Fetch user's MMR from database
                    let user = db.get_user_by_id(metadata.user_id)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("User not found"))?;
                    let mmr = user.mmr;

                    // Add to matchmaking queue using Redis-based matchmaking
                    let matchmaking_manager_clone = matchmaking_manager.clone(); // Clone the Arc before locking
                    let mut matchmaking_manager_guard = matchmaking_manager.lock().await;
                    match add_to_matchmaking_queue(
                        &mut *matchmaking_manager_guard,
                        metadata.user_id as u32,
                        metadata.username.clone(),
                        mmr,
                        game_type,
                        queue_mode,
                    ).await {
                        Ok(()) => {
                            info!("User {} added to matchmaking queue", metadata.user_id);

                            // Start listening for match notifications and renewing queue position
                            let user_id = metadata.user_id;
                            let ws_tx_clone = ws_tx.clone();
                            let replication_manager_clone = replication_manager.clone();
                            let redis_url_clone = redis_url.to_string();
                            tokio::spawn(async move {
                                // Spawn a task to periodically renew queue position
                                let renewal_handle = {
                                    let matchmaking_manager_clone2 = matchmaking_manager_clone.clone();
                                    let user_id_copy = user_id;
                                    tokio::spawn(async move {
                                        let mut interval = tokio::time::interval(Duration::from_secs(30));
                                        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                                        loop {
                                            interval.tick().await;
                                            let mut mm_guard = matchmaking_manager_clone2.lock().await;
                                            match mm_guard.renew_queue_position(user_id_copy as u32).await {
                                                Ok(renewed) => {
                                                    if renewed {
                                                        debug!("Renewed queue position for user {}", user_id_copy);
                                                    } else {
                                                        // User no longer in queue, stop renewing
                                                        break;
                                                    }
                                                }
                                                Err(e) => {
                                                    error!("Failed to renew queue position for user {}: {}", user_id_copy, e);
                                                }
                                            }
                                        }
                                    })
                                };
                                // Subscribe to match notifications
                                let redis_keys = crate::redis_keys::RedisKeys::new();
                                let channel = redis_keys.matchmaking_notification_channel(user_id as u32);
                                info!("Subscribing to match notifications on channel: {}", channel);
                                if let Ok(client) = redis::Client::open(redis_url_clone.as_ref()) {
                                    if let Ok(mut pubsub) = client.get_async_pubsub().await {
                                        if pubsub.subscribe(&channel).await.is_ok() {
                                            info!("Successfully subscribed to match notifications for user {}", user_id);
                                            // Wait for match notification in a loop
                                            let mut pubsub_stream = pubsub.on_message();
                                            while let Some(msg) = futures_util::StreamExt::next(&mut pubsub_stream).await {
                                                if let Ok(payload) = msg.get_payload::<String>() {
                                                    // Parse the notification
                                                    if let Ok(notification) = serde_json::from_str::<serde_json::Value>(&payload) {
                                                        if let Some(game_id) = notification["game_id"].as_u64() {
                                                            info!("User {} matched to game {}, waiting for game to be available", user_id, game_id);
                                                            
                                                            // Wait for the game to become available in the replication manager
                                                            match replication_manager_clone.wait_for_game(game_id as u32, 10).await {
                                                                Ok(_game_state) => {
                                                                    info!("Game {} is now available in replication manager, sending JoinGame message to user {}", game_id, user_id);
                                                                    // Send JoinGame message to client
                                                                    let join_msg = WSMessage::JoinGame(game_id as u32);
                                                                    let json_msg = serde_json::to_string(&join_msg).unwrap();
                                                                    if ws_tx_clone.send(Message::Text(json_msg.into())).await.is_ok() {
                                                                        info!("JoinGame message sent to user {}", user_id);
                                                                        // Cancel the renewal task
                                                                        renewal_handle.abort();
                                                                        // Exit after sending the match notification
                                                                        break;
                                                                    }
                                                                }
                                                                Err(e) => {
                                                                    error!("Failed to wait for game {} to become available: {}", game_id, e);
                                                                    // Continue listening for other notifications
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        } else {
                                            error!("Failed to subscribe to channel {} for user {}", channel, user_id);
                                        }
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            error!("Failed to add user to matchmaking queue: {}", e);
                        }
                    }

                    Ok(ConnectionState::Authenticated { metadata })
                }
                WSMessage::JoinGame(game_id) => {
                    info!("User {} ({}) joining game {}", metadata.username, metadata.user_id, game_id);
                    
                    // Transition to InGame state
                    // TODO: Validate that the user can join.
                    //  Most games will be joinable except for private custom games.
                    Ok(ConnectionState::InGame {
                        metadata,
                        game_id,
                    })
                }
                WSMessage::LeaveQueue => {
                    info!("User {} ({}) leaving matchmaking queue", metadata.username, metadata.user_id);

                    // Remove from matchmaking queue using Redis-based matchmaking
                    let mut matchmaking_manager = matchmaking_manager.lock().await;
                    match remove_from_matchmaking_queue(
                        &mut *matchmaking_manager,
                        metadata.user_id as u32,
                    ).await {
                        Ok(()) => {
                            info!("User {} removed from matchmaking queue", metadata.user_id);
                        }
                        Err(e) => {
                            error!("Failed to remove user from matchmaking queue: {}", e);
                        }
                    }

                    Ok(ConnectionState::Authenticated { metadata })
                }
                WSMessage::Ping => {
                    // Respond with Pong
                    let pong_msg = Message::Text(serde_json::to_string(&WSMessage::Pong)?.into());
                    ws_tx.send(pong_msg).await?;
                    Ok(ConnectionState::Authenticated { metadata })
                }
                WSMessage::ClockSyncRequest { client_time } => {
                    // Respond with server time for clock synchronization
                    let server_time = chrono::Utc::now().timestamp_millis();
                    let response = WSMessage::ClockSyncResponse { client_time, server_time };
                    let json_msg = serde_json::to_string(&response)?;
                    ws_tx.send(Message::Text(json_msg.into())).await?;
                    Ok(ConnectionState::Authenticated { metadata })
                }
                WSMessage::GameEvent(event_msg) => {
                    // Forward game events to the client
                    warn!("Received game event in authenticated state: {:?}", event_msg);
                    Ok(ConnectionState::Authenticated { metadata })
                }
                WSMessage::CreateCustomGame { settings } => {
                    info!("User {} ({}) creating custom game", metadata.username, metadata.user_id);
                    
                    match create_custom_game(db, pubsub, metadata.user_id, metadata.username.clone(), settings).await {
                        Ok((game_id, game_code)) => {
                            // Send success response
                            let response = WSMessage::CustomGameCreated { game_id, game_code };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            
                            // Transition to in-game state
                            Ok(ConnectionState::InGame { 
                                metadata,
                                game_id,
                            })
                        }
                        Err(e) => {
                            error!("Failed to create custom game: {}", e);
                            let response = WSMessage::AccessDenied { 
                                reason: format!("Failed to create game: {}", e) 
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            Ok(ConnectionState::Authenticated { metadata })
                        }
                    }
                }
                WSMessage::JoinCustomGame { game_code } => {
                    info!("User {} ({}) joining custom game with code: {}", metadata.username, metadata.user_id, game_code);
                    
                    match join_custom_game(db, metadata.user_id, &game_code).await {
                        Ok(game_id) => {
                            // Send success response
                            let response = WSMessage::CustomGameJoined { game_id };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            
                            // Transition to in-game state
                            Ok(ConnectionState::InGame { 
                                metadata,
                                game_id,
                            })
                        }
                        Err(e) => {
                            error!("Failed to join custom game: {}", e);
                            let response = WSMessage::AccessDenied { 
                                reason: format!("Failed to join game: {}", e) 
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            Ok(ConnectionState::Authenticated { metadata })
                        }
                    }
                }
                WSMessage::SpectateGame { game_id, game_code } => {
                    info!("User {} ({}) attempting to spectate game {}", metadata.username, metadata.user_id, game_id);
                    
                    match spectate_game(db, metadata.user_id, game_id, game_code.as_deref()).await {
                        Ok(actual_game_id) => {
                            // Send success response
                            let response = WSMessage::SpectatorJoined;
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            
                            // Transition to in-game state as spectator
                            Ok(ConnectionState::InGame { 
                                metadata,
                                game_id: actual_game_id,
                            })
                        }
                        Err(e) => {
                            error!("Failed to spectate game: {}", e);
                            let response = WSMessage::AccessDenied { 
                                reason: format!("Failed to spectate game: {}", e) 
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            Ok(ConnectionState::Authenticated { metadata })
                        }
                    }
                }
                WSMessage::CreateSoloGame => {
                    info!("User {} ({}) creating solo game", metadata.username, metadata.user_id);
                    
                    match create_solo_game(db, pubsub, metadata.user_id, metadata.username.clone()).await {
                        Ok(game_id) => {
                            // Send success response
                            let response = WSMessage::SoloGameCreated { game_id };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            
                            Ok(ConnectionState::Authenticated { metadata })
                        }
                        Err(e) => {
                            error!("Failed to create solo game: {}", e);
                            let response = WSMessage::AccessDenied { 
                                reason: format!("Failed to create solo game: {}", e) 
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            Ok(ConnectionState::Authenticated { metadata })
                        }
                    }
                }
                _ => {
                    warn!("Unexpected message in authenticated state: {:?}", ws_message);
                    Ok(ConnectionState::Authenticated { metadata })
                }
            }
        }
        ConnectionState::InGame { metadata, game_id } => {
            match ws_message {
                WSMessage::GameCommand(command_message) => {
                    // Submit command via PubSub
                    let partition_id = game_id % PARTITION_COUNT;
                    
                    let event = StreamEvent::GameCommandSubmitted {
                        game_id,
                        user_id: metadata.user_id as u32,
                        command: command_message,
                    };
                    
                    // Send command via PubSub
                    let serialized = serde_json::to_vec(&event)
                        .context("Failed to serialize command")?;
                    match pubsub.publish_command(partition_id, &serialized).await {
                        Ok(_) => {
                            debug!("Successfully submitted game command via PubSub: {:?}", event);
                            Ok(ConnectionState::InGame { metadata, game_id })
                        }
                        Err(e) => {
                            error!("Failed to submit command via PubSub: {}", e);
                            // Keep the connection in game state, don't disconnect
                            Ok(ConnectionState::InGame { metadata, game_id })
                        }
                    }
                }
                WSMessage::Ping => {
                    // Respond with Pong
                    let pong_msg = Message::Text(serde_json::to_string(&WSMessage::Pong)?.into());
                    ws_tx.send(pong_msg).await?;
                    Ok(ConnectionState::InGame { metadata, game_id })
                }
                WSMessage::ClockSyncRequest { client_time } => {
                    // Respond with server time for clock synchronization
                    let server_time = chrono::Utc::now().timestamp_millis();
                    let response = WSMessage::ClockSyncResponse { client_time, server_time };
                    let json_msg = serde_json::to_string(&response)?;
                    ws_tx.send(Message::Text(json_msg.into())).await?;
                    Ok(ConnectionState::InGame { metadata, game_id })
                }
                WSMessage::LeaveGame => {
                    info!("User {} ({}) leaving game {}", metadata.username, metadata.user_id, game_id);
                    // Transition back to authenticated state
                    Ok(ConnectionState::Authenticated { metadata })
                }
                WSMessage::StartCustomGame => {
                    info!("User {} ({}) starting custom game {}", metadata.username, metadata.user_id, game_id);
                    
                    // Check if user is the host
                    let is_host = check_game_host(db, game_id, metadata.user_id).await?;
                    if !is_host {
                        let response = WSMessage::AccessDenied { 
                            reason: "Only the host can start the game".to_string() 
                        };
                        let json_msg = serde_json::to_string(&response)?;
                        ws_tx.send(Message::Text(json_msg.into())).await?;
                        return Ok(ConnectionState::InGame { metadata, game_id });
                    }
                    
                    // Get server ID from database
                    let server_id = db.get_server_for_load_balancing("default")
                        .await?;
                    
                    // Start the game by publishing StatusUpdated event via PubSub
                    let _partition_id = game_id % PARTITION_COUNT;
                    
                    let _status_event = StreamEvent::StatusUpdated { 
                        game_id,
                        status: GameStatus::Started { server_id: server_id as u64 },
                    };
                    
                    // Publish game started event via PubSub
                    let event_msg = GameEventMessage {
                        game_id,
                        tick: 0,
                        sequence: 0,
                        user_id: None,
                        event: GameEvent::StatusUpdated { status: GameStatus::Started { server_id: server_id as u64 } },
                    };
                    let partition_id = game_id % crate::game_executor::PARTITION_COUNT;
                    match pubsub.publish_event(partition_id, &event_msg).await {
                        Ok(_) => {
                            let response = WSMessage::CustomGameStarting;
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            Ok(ConnectionState::InGame { metadata, game_id })
                        }
                        Err(e) => {
                            error!("Failed to start game: {}", e);
                            let response = WSMessage::AccessDenied { 
                                reason: format!("Failed to start game: {}", e) 
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            Ok(ConnectionState::InGame { metadata, game_id })
                        }
                    }
                }
                _ => {
                    warn!("Unexpected message in game state: {:?}", ws_message);
                    Ok(ConnectionState::InGame { metadata, game_id })
                }
            }
        }
        ConnectionState::ShuttingDown { timeout } => {
            // Ignore all messages during shutdown
            Ok(ConnectionState::ShuttingDown { timeout })
        }
    }
}




pub async fn register_server(db: &Arc<dyn Database>, grpc_address: &str, region: &str) -> Result<u64> {
    info!("Registering server instance");

    // Insert a new record and return the generated ID
    let id = db.register_server(grpc_address, region)
        .await
        .context("Failed to register server in database")?;

    let id_u64 = id as u64;
    info!(id = id_u64, "Server registered with ID: {}", id_u64);
    Ok(id_u64)
}

pub async fn discover_peers(db: &Arc<dyn Database>, region: &str) -> Result<Vec<(u64, String)>> {
    info!("Discovering peers in region: {}", region);
    
    // Query to find all servers in the specified region
    let servers = db.get_active_servers(region)
        .await
        .context("Failed to fetch server records")?;
    
    if servers.is_empty() {
        warn!("No servers found in region: {}", region);
        return Ok(vec![]);
    }
    
    info!("Found {} servers in region {}: {:?}", servers.len(), region, servers);
    Ok(servers.into_iter()
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
    let lobby_id = db.create_custom_lobby(
        &game_code,
        user_id,
        &serde_json::to_value(&settings)?,
    ).await?;
    
    // Create game entry
    let game_id = db.create_game(
        server_id,
        &serde_json::to_value(&common::GameType::Custom { settings: settings.clone() })?,
        "custom",
        settings.is_private,
        Some(&game_code),
    ).await?;
    
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
    pubsub.publish_snapshot(partition_id, game_id_u32, &game_state).await
        .context("Failed to publish initial game snapshot")?;
    
    // Also send GameCreated event via partition command channel
    let serialized = serde_json::to_vec(&event)
        .context("Failed to serialize GameCreated event")?;
    pubsub.publish_command(partition_id, &serialized).await
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
        max_players: 1,  // Solo game
        game_mode: common::GameMode::Solo,
        is_private: true,
        allow_spectators: false,
        snake_start_length: 4,
    };
    
    // Create game entry
    let game_id = db.create_game(
        server_id,
        &serde_json::to_value(&common::GameType::Custom { settings: settings.clone() })?,
        "solo",
        true,  // Solo games are private
        None,  // No game code for solo games
    ).await?;
    
    // Create game state with one player
    let start_ms = chrono::Utc::now().timestamp_millis();
    let mut game_state = common::GameState::new(
        settings.arena_width,
        settings.arena_height,
        common::GameType::Custom { settings: settings.clone() },
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
    pubsub.publish_snapshot(partition_id, game_id_u32, &game_state).await
        .context("Failed to publish initial game snapshot")?;
    
    // Also send GameCreated event via partition command channel
    let serialized = serde_json::to_vec(&event)
        .context("Failed to serialize GameCreated event")?;
    pubsub.publish_command(partition_id, &serialized).await
        .context("Failed to publish GameCreated event")?;
    
    // Start the game immediately (no waiting in solo mode)
    let status_event = StreamEvent::StatusUpdated { 
        game_id: game_id as u32,
        status: GameStatus::Started { server_id: server_id as u64 },
    };
    
    let status_serialized = serde_json::to_vec(&status_event)
        .context("Failed to serialize StatusUpdated event")?;
    pubsub.publish_command(partition_id, &status_serialized).await
        .context("Failed to publish StatusUpdated event")?;
    
    Ok(game_id as u32)
}

async fn join_custom_game(
    db: &Arc<dyn Database>,
    user_id: i32,
    game_code: &str,
) -> Result<u32> {
    // Find the game by code
    let game = db.get_game_by_code(game_code).await?
        .context("Game not found or already started")?;
    
    // Check that game is waiting
    if game.status != "waiting" {
        return Err(anyhow::anyhow!("Game already started"));
    }
    
    let game_id = game.id;
    
    // Check if game is full
    let player_count = db.get_player_count(game_id).await?;
    
    // Get max players from game settings
    let max_players = game.game_type
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

async fn check_game_host(
    db: &Arc<dyn Database>,
    game_id: u32,
    user_id: i32,
) -> Result<bool> {
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
        let game = db.get_game_by_code(code).await?
            .ok_or_else(|| anyhow::anyhow!("Invalid game code"))?;
        
        // Check if spectators are allowed for private games
        if game.is_private {
            let lobby = db.get_custom_lobby_by_code(code).await?;
            
            if let Some(lobby) = lobby {
                let allow_spectators = lobby.settings
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
    db.add_spectator_to_game(actual_game_id as i32, user_id).await?;
    
    info!("User {} joined as spectator for game {}", user_id, actual_game_id);
    Ok(actual_game_id)
}

/// Subscribe to game events via replication manager and forward to the WebSocket client
async fn subscribe_to_game_events_via_replication(
    replication_manager: Arc<crate::replication::ReplicationManager>,
    game_id: u32,
    ws_tx: mpsc::Sender<Message>,
    db: Arc<dyn Database>,
) -> Result<()> {
    info!("Subscribing to game {} via replication manager", game_id);

    // Try to subscribe to the game through replication manager
    // If the game is not in memory (e.g., completed game), load from database
    let (game_state, mut event_receiver) = match replication_manager.subscribe_to_game(game_id).await {
        Ok(result) => result,
        Err(e) => {
            // Game not in replication manager, try to load from database
            info!("Game {} not in replication manager, loading from database: {}", game_id, e);

            // Load game from database
            let game = db.get_game_by_id(game_id as i32).await?
                .ok_or_else(|| anyhow::anyhow!("Game {} not found in database", game_id))?;

            // Extract and deserialize game state
            let game_state_json = game.game_state
                .ok_or_else(|| anyhow::anyhow!("Game {} has no stored game state", game_id))?;

            let game_state: GameState = serde_json::from_value(game_state_json)
                .context("Failed to deserialize game state from database")?;

            // For completed games, we just need to send the snapshot
            // Create a dummy receiver that will never produce events
            let (_tx, rx) = broadcast::channel(1);
            let filtered_receiver = crate::replication::FilteredEventReceiver::new(rx, game_state.event_sequence, game_id);

            (game_state, filtered_receiver)
        }
    };
    
    // Send the initial snapshot
    let snapshot_event = GameEventMessage {
        game_id,
        tick: game_state.tick,
        sequence: game_state.event_sequence,
        user_id: None, // System-generated snapshot
        event: GameEvent::Snapshot { game_state },
    };
    
    let ws_msg = WSMessage::GameEvent(snapshot_event);
    let json_msg = serde_json::to_string(&ws_msg)?;
    let msg = Message::Text(Utf8Bytes::from(json_msg));
    
    // Send the initial snapshot
    if ws_tx.send(msg).await.is_err() {
        // Channel closed, client disconnected
        return Ok(());
    }
    
    info!("Sent initial snapshot for game {} to WebSocket client", game_id);
    
    // Listen for further events
    loop {
        tokio::select! {
            event_result = event_receiver.recv() => {
                match event_result {
                    Ok(event_msg) => {
                        debug!("Received game event for game {}: {:?}", game_id, event_msg);
                        
                        // Forward to WebSocket client
                        let ws_msg = WSMessage::GameEvent(event_msg.clone());
                        let json_msg = serde_json::to_string(&ws_msg)?;
                        let msg = Message::Text(Utf8Bytes::from(json_msg));
                        
                        // Send through the channel
                        if ws_tx.send(msg).await.is_err() {
                            // Channel closed, client disconnected
                            info!("WebSocket channel closed for game {}, stopping subscription", game_id);
                            return Ok(());
                        }
                        
                        // Check if game ended
                        match &event_msg.event {
                            GameEvent::StatusUpdated { status: GameStatus::Complete { .. } } => {
                                info!("Game {} ended, stopping event subscription", game_id);
                                return Ok(());
                            }
                            _ => {}
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!("WebSocket subscription lagged by {} messages for game {}", n, game_id);
                        // Continue - we'll catch up with the next messages
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        info!("Game event broadcast closed for game {}", game_id);
                        return Ok(());
                    }
                }
            }
        }
    }
}


