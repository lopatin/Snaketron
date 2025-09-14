use std::pin::Pin;
use tracing::{debug, error, info, warn};
use tokio::net::{TcpListener, TcpStream};
use anyhow::{Context, Result};
use sqlx::PgPool;
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
use common::{GameCommandMessage, GameEvent, GameEventMessage, GameStatus, DEFAULT_TICK_INTERVAL_MS};
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
    QueueForMatch { game_type: common::GameType },
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
    db_pool: PgPool,
}

impl TestJwtVerifier {
    pub fn new(db_pool: PgPool) -> Self {
        Self { db_pool }
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
        let existing_user: Option<i32> = sqlx::query_scalar(
            "SELECT id FROM users WHERE username = $1"
        )
        .bind(&username)
        .fetch_optional(&self.db_pool)
        .await?;
        
        let user_id = match existing_user {
            Some(id) => id,
            None => {
                // Create new test user
                let new_id: i32 = sqlx::query_scalar(
                    r#"
                    INSERT INTO users (username, password_hash, mmr)
                    VALUES ($1, 'test_password_hash', 1000)
                    RETURNING id
                    "#
                )
                .bind(&username)
                .fetch_one(&self.db_pool)
                .await?;
                
                info!("Created test user {} with ID {}", username, new_id);
                new_id
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


pub async fn run_websocket_server(
    addr: &str,
    db_pool: PgPool,
    redis_url: String,
    environment: String,
    cancellation_token: CancellationToken,
    jwt_verifier: Arc<dyn JwtVerifier>,
    replication_manager: Arc<crate::replication::ReplicationManager>,
) -> Result<()> {

    // Create shared Redis connection manager
    let pubsub = PubSubManager::new(&redis_url, &environment).await
        .context("Failed to create PubSub manager")?;
    
    // Create matchmaking manager
    let matchmaking_manager = Arc::new(Mutex::new(
        MatchmakingManager::new(&redis_url, &environment).await
            .context("Failed to create matchmaking manager")?
    ));

    let listener = TcpListener::bind(addr).await?;
    info!("WebSocket server listening on {}", addr);

    let mut connection_handles: Vec<JoinHandle<Result<()>>> = Vec::new();

    // Accept incoming connections in a loop until cancellation is requested
    loop {
        tokio::select! {
            biased;
            _ = cancellation_token.cancelled() => {
                info!("External WebSocket server shutdown received");
                break;
            }

            result = listener.accept() => {
                match result {
                    Ok((stream, peer_addr)) => {
                        info!("Accepted connection from: {}", peer_addr);
                        let connection_token = cancellation_token.child_token();
                        let jwt_verifier_clone = jwt_verifier.clone();
                        let db_pool_clone = db_pool.clone();
                        let pubsub_clone = pubsub.clone();
                        let replication_manager_clone = replication_manager.clone();
                        let matchmaking_manager_clone = matchmaking_manager.clone();
                        let handle = tokio::spawn(handle_websocket_connection(db_pool_clone, pubsub_clone, matchmaking_manager_clone, stream, jwt_verifier_clone, connection_token, replication_manager_clone));
                        connection_handles.push(handle);
                    }

                    Err(e) => {
                        error!("Failed to accept connection: {}", e);
                    }
                }
            }
        }
    }

    // Graceful shutdown of all connections
    info!("Waiting for {} active connections to shut down...", connection_handles.len());
    let results = join_all(connection_handles).await;

    for result in results {
        match result {
            Ok(Ok(_)) => {
                info!("Connection shut down");
            }
            Ok(Err(e)) => {
                error!("Connection handler failed: {}", e);
            }
            Err(e) => {
                error!("Connection handler panicked: {}", e);
            }
        }
    }

    info!("All connections shut down. WebSocket server exiting.");
    Ok(())
}


async fn handle_websocket_connection(
    db_pool: PgPool,
    pubsub: PubSubManager,
    matchmaking_manager: Arc<Mutex<MatchmakingManager>>,
    stream: TcpStream,
    jwt_verifier: Arc<dyn JwtVerifier>,
    cancellation_token: CancellationToken,
    replication_manager: Arc<crate::replication::ReplicationManager>,
) -> Result<()> {
    let peer_addr = stream.peer_addr()?;
    info!("Handling WebSocket connection from: {}", peer_addr);

    let mut ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .expect("Failed to accept WebSocket connection");

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
                if let Err(e) = ws_stream.send(shutdown_msg).await {
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

            // Handle messages from the channel (to send to client)
            Some(msg) = ws_rx.recv() => {
                if let Err(e) = ws_stream.send(msg).await {
                    error!("Failed to send message to client: {}", e);
                    break;
                }
            }
            
            
            // Handle incoming WebSocket messages
            message = ws_stream.next() => {
                match message {
                    Some(Ok(msg)) => {
                        // Check if it's a close message
                        if msg.is_close() {
                            info!("Received close message from client");
                            break;
                        }
                        
                        // Try to parse as text message
                        let text = match msg.to_text() {
                            Ok(text) => text,
                            Err(_) => {
                                // Not a text message, ignore
                                continue;
                            }
                        };
                        
                        let ws_message: WSMessage = match serde_json::from_str(text) {
                            Ok(msg) => msg,
                            Err(e) => {
                                error!("Failed to parse WebSocket message: {}", e);
                                continue;
                            }
                        };
                        
                        // Process message based on current state using the new helper function
                        debug!("WS: Processing message: {:?}", ws_message);
                        
                        // Check current state before processing
                        let was_in_game = matches!(&state, ConnectionState::InGame { .. });
                        
                        // Clone the connection manager for this call
                        let mut pubsub_clone = pubsub.clone();
                        
                        let process_result = process_ws_message(
                            state,
                            ws_message,
                            &jwt_verifier,
                            &db_pool,
                            &ws_tx,
                            &mut ws_stream,
                            &mut pubsub_clone,
                            &matchmaking_manager,
                            &replication_manager,
                        ).await;
                        
                        match process_result {
                            Ok(new_state) => {
                                // Check if we're transitioning to InGame state
                                let entering_game = matches!(&new_state, ConnectionState::InGame { .. }) 
                                    && !was_in_game;
                                let leaving_game = was_in_game 
                                    && !matches!(&new_state, ConnectionState::InGame { .. });
                                
                                state = new_state;
                                
                                // Handle game state transitions
                                if entering_game {
                                    if let ConnectionState::InGame { game_id, .. } = &state {
                                        // Start subscription via replication manager
                                        let game_id_filter = *game_id;
                                        let ws_tx_clone = ws_tx.clone();
                                        let replication_manager_clone = replication_manager.clone();
                                        
                                        // Abort any existing subscription
                                        if let Some(handle) = game_event_handle.take() {
                                            handle.abort();
                                        }
                                        
                                        // Start new subscription through replication manager
                                        game_event_handle = Some(tokio::spawn(async move {
                                            if let Err(e) = subscribe_to_game_events_via_replication(
                                                replication_manager_clone,
                                                game_id_filter,
                                                ws_tx_clone
                                            ).await {
                                                error!("Game event subscription error: {}", e);
                                            }
                                        }));
                                    }
                                } else if leaving_game {
                                    // Stop Redis stream subscription
                                    if let Some(handle) = game_event_handle.take() {
                                        handle.abort();
                                    }
                                }
                                
                                let new_state_name = match &state {
                                    ConnectionState::Unauthenticated => "Unauthenticated".to_string(),
                                    ConnectionState::Authenticated { .. } => "Authenticated".to_string(),
                                    ConnectionState::InGame { game_id, .. } => format!("InGame({})", game_id),
                                    ConnectionState::ShuttingDown { .. } => "ShuttingDown".to_string(),
                                };
                                debug!("WS: State after processing message: {}", new_state_name);
                            }
                            Err(e) => {
                                error!("Error processing message: {}", e);
                                // Need to handle the error without losing state
                                // Since process_ws_message consumed state, we need to set it to a valid value
                                state = ConnectionState::Unauthenticated;
                                break;
                            }
                        }
                        
                    }
                    Some(Err(e)) => {
                        error!("Error receiving message: {}", e);
                        break;
                    }
                    None => {
                        info!("WebSocket stream closed");
                        break;
                    }
                }
            }
        }
    }

    // Clean up any active Redis stream subscription
    if let Some(handle) = game_event_handle {
        handle.abort();
    }
    
    // Clean up connection
    let _ = ws_stream.close(None).await;
    
    Ok(())
}


async fn process_ws_message(
    state: ConnectionState,
    ws_message: WSMessage,
    jwt_verifier: &Arc<dyn JwtVerifier>,
    db_pool: &PgPool,
    ws_tx: &mpsc::Sender<Message>,
    ws_stream: &mut WebSocketStream<TcpStream>,
    pubsub: &mut PubSubManager,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
    replication_manager: &Arc<crate::replication::ReplicationManager>,
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
                            let username: String = match sqlx::query_scalar(
                                "SELECT username FROM users WHERE id = $1"
                            )
                            .bind(user_token.user_id)
                            .fetch_optional(db_pool)
                            .await? {
                                Some(name) => name,
                                None => {
                                    error!("User {} not found in database", user_token.user_id);
                                    return Err(anyhow::anyhow!("User not found"));
                                }
                            };
                            
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
                    ws_stream.send(pong_msg).await?;
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
                WSMessage::QueueForMatch { game_type } => {
                    info!("User {} ({}) queuing for match type: {:?}", metadata.username, metadata.user_id, game_type);

                    // Fetch user's MMR from database
                    let mmr: i32 = sqlx::query_scalar(
                        "SELECT mmr FROM users WHERE id = $1"
                    )
                    .bind(metadata.user_id)
                    .fetch_one(db_pool)
                    .await
                    .unwrap_or(1500); // Default MMR if not found

                    // Add to matchmaking queue using Redis-based matchmaking
                    let mut matchmaking_manager = matchmaking_manager.lock().await;
                    match add_to_matchmaking_queue(
                        &mut *matchmaking_manager,
                        metadata.user_id as u32,
                        metadata.username.clone(),
                        mmr,
                        game_type,
                    ).await {
                        Ok(()) => {
                            info!("User {} added to matchmaking queue", metadata.user_id);
                            
                            // Start listening for match notifications
                            let user_id = metadata.user_id;
                            let ws_tx_clone = ws_tx.clone();
                            let replication_manager_clone = replication_manager.clone();
                            tokio::spawn(async move {
                                // Subscribe to match notifications
                                let channel = format!("matchmaking:notification:{}", user_id);
                                info!("Subscribing to match notifications on channel: {}", channel);
                                if let Ok(client) = redis::Client::open("redis://127.0.0.1:6379") {
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
                    ws_stream.send(pong_msg).await?;
                    Ok(ConnectionState::Authenticated { metadata })
                }
                WSMessage::ClockSyncRequest { client_time } => {
                    // Respond with server time for clock synchronization
                    let server_time = chrono::Utc::now().timestamp_millis();
                    let response = WSMessage::ClockSyncResponse { client_time, server_time };
                    let json_msg = serde_json::to_string(&response)?;
                    ws_stream.send(Message::Text(json_msg.into())).await?;
                    Ok(ConnectionState::Authenticated { metadata })
                }
                WSMessage::GameEvent(event_msg) => {
                    // Forward game events to the client
                    warn!("Received game event in authenticated state: {:?}", event_msg);
                    Ok(ConnectionState::Authenticated { metadata })
                }
                WSMessage::CreateCustomGame { settings } => {
                    info!("User {} ({}) creating custom game", metadata.username, metadata.user_id);
                    
                    match create_custom_game(db_pool, pubsub, metadata.user_id, metadata.username.clone(), settings).await {
                        Ok((game_id, game_code)) => {
                            // Send success response
                            let response = WSMessage::CustomGameCreated { game_id, game_code };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_stream.send(Message::Text(json_msg.into())).await?;
                            
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
                            ws_stream.send(Message::Text(json_msg.into())).await?;
                            Ok(ConnectionState::Authenticated { metadata })
                        }
                    }
                }
                WSMessage::JoinCustomGame { game_code } => {
                    info!("User {} ({}) joining custom game with code: {}", metadata.username, metadata.user_id, game_code);
                    
                    match join_custom_game(db_pool, metadata.user_id, &game_code).await {
                        Ok(game_id) => {
                            // Send success response
                            let response = WSMessage::CustomGameJoined { game_id };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_stream.send(Message::Text(json_msg.into())).await?;
                            
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
                            ws_stream.send(Message::Text(json_msg.into())).await?;
                            Ok(ConnectionState::Authenticated { metadata })
                        }
                    }
                }
                WSMessage::SpectateGame { game_id, game_code } => {
                    info!("User {} ({}) attempting to spectate game {}", metadata.username, metadata.user_id, game_id);
                    
                    match spectate_game(db_pool, metadata.user_id, game_id, game_code.as_deref()).await {
                        Ok(actual_game_id) => {
                            // Send success response
                            let response = WSMessage::SpectatorJoined;
                            let json_msg = serde_json::to_string(&response)?;
                            ws_stream.send(Message::Text(json_msg.into())).await?;
                            
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
                            ws_stream.send(Message::Text(json_msg.into())).await?;
                            Ok(ConnectionState::Authenticated { metadata })
                        }
                    }
                }
                WSMessage::CreateSoloGame => {
                    info!("User {} ({}) creating solo game", metadata.username, metadata.user_id);
                    
                    match create_solo_game(db_pool, pubsub, metadata.user_id, metadata.username.clone()).await {
                        Ok(game_id) => {
                            // Send success response
                            let response = WSMessage::SoloGameCreated { game_id };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_stream.send(Message::Text(json_msg.into())).await?;
                            
                            Ok(ConnectionState::Authenticated { metadata })
                        }
                        Err(e) => {
                            error!("Failed to create solo game: {}", e);
                            let response = WSMessage::AccessDenied { 
                                reason: format!("Failed to create solo game: {}", e) 
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_stream.send(Message::Text(json_msg.into())).await?;
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
                    ws_stream.send(pong_msg).await?;
                    Ok(ConnectionState::InGame { metadata, game_id })
                }
                WSMessage::ClockSyncRequest { client_time } => {
                    // Respond with server time for clock synchronization
                    let server_time = chrono::Utc::now().timestamp_millis();
                    let response = WSMessage::ClockSyncResponse { client_time, server_time };
                    let json_msg = serde_json::to_string(&response)?;
                    ws_stream.send(Message::Text(json_msg.into())).await?;
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
                    let is_host = check_game_host(db_pool, game_id, metadata.user_id).await?;
                    if !is_host {
                        let response = WSMessage::AccessDenied { 
                            reason: "Only the host can start the game".to_string() 
                        };
                        let json_msg = serde_json::to_string(&response)?;
                        ws_stream.send(Message::Text(json_msg.into())).await?;
                        return Ok(ConnectionState::InGame { metadata, game_id });
                    }
                    
                    // Get server ID from database
                    let server_id: i32 = sqlx::query_scalar(
                        "SELECT id FROM servers WHERE last_heartbeat > NOW() - INTERVAL '30 seconds' ORDER BY current_game_count ASC LIMIT 1"
                    )
                    .fetch_one(db_pool)
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
                            ws_stream.send(Message::Text(json_msg.into())).await?;
                            Ok(ConnectionState::InGame { metadata, game_id })
                        }
                        Err(e) => {
                            error!("Failed to start game: {}", e);
                            let response = WSMessage::AccessDenied { 
                                reason: format!("Failed to start game: {}", e) 
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_stream.send(Message::Text(json_msg.into())).await?;
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




pub async fn register_server(pool: &PgPool, grpc_address: &str, region: &str) -> Result<u64> {
    info!("Registering server instance");

    // Insert a new record and return the generated ID
    let id: i32 = sqlx::query_scalar(
        r#"
        INSERT INTO servers (grpc_address, last_heartbeat, region, created_at)
        VALUES ($1, NULL, $2, $3)
        RETURNING id
        "#
    )
        .bind(grpc_address)
        .bind(region)
        .bind(Utc::now())
        .fetch_one(pool)
        .await
        .context("Failed to register server in database")?;

    let id_u64 = id as u64;
    info!(id = id_u64, "Server registered with ID: {}", id_u64);
    Ok(id_u64)
}

pub async fn discover_peers(pool: &PgPool, region: &str) -> Result<Vec<(u64, String)>> {
    info!("Discovering peers in region: {}", region);
    
    let now = Utc::now();
    
    // Query to find all servers in the specified region
    let servers = sqlx::query_as::<_, (i32, String)>(
        r#"
        SELECT id, grpc_address FROM servers
        WHERE region = $1 AND last_heartbeat > $2 - INTERVAL '30 seconds'
        "#
    )
    .bind(region)
    .bind(now)
    .fetch_all(pool)
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
    pool: &PgPool,
    pubsub: &mut PubSubManager,
    user_id: i32,
    username: String,
    settings: common::CustomGameSettings,
) -> Result<(u32, String)> {
    let game_code = generate_game_code();
    
    // Get current server ID from database
    let server_id: i32 = sqlx::query_scalar(
        "SELECT id FROM servers WHERE last_heartbeat > NOW() - INTERVAL '30 seconds' ORDER BY current_game_count ASC LIMIT 1"
    )
    .fetch_one(pool)
    .await?;
    
    // Create lobby entry
    let lobby_id: i32 = sqlx::query_scalar(
        r#"
        INSERT INTO custom_game_lobbies (game_code, host_user_id, settings, created_at, expires_at, state)
        VALUES ($1, $2, $3, NOW(), NOW() + INTERVAL '1 hour', 'waiting')
        RETURNING id
        "#
    )
    .bind(&game_code)
    .bind(user_id)
    .bind(serde_json::to_value(&settings)?)
    .fetch_one(pool)
    .await?;
    
    // Create game entry
    let game_id: i32 = sqlx::query_scalar(
        r#"
        INSERT INTO games (server_id, game_type, status, game_mode, is_private, game_code)
        VALUES ($1, $2, 'waiting', 'custom', $3, $4)
        RETURNING id
        "#
    )
    .bind(server_id)
    .bind(serde_json::to_value(&common::GameType::Custom { settings: settings.clone() })?)
    .bind(settings.is_private)
    .bind(&game_code)
    .fetch_one(pool)
    .await?;
    
    // Update lobby with game_id
    sqlx::query("UPDATE custom_game_lobbies SET game_id = $1 WHERE id = $2")
        .bind(game_id)
        .bind(lobby_id)
        .execute(pool)
        .await?;
    
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
    pool: &PgPool,
    pubsub: &mut PubSubManager,
    user_id: i32,
    username: String,
) -> Result<u32> {
    // Get current server ID from database
    let server_id: i32 = sqlx::query_scalar(
        "SELECT id FROM servers WHERE last_heartbeat > NOW() - INTERVAL '30 seconds' ORDER BY current_game_count ASC LIMIT 1"
    )
    .fetch_one(pool)
    .await?;
    
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
    let game_id: i32 = sqlx::query_scalar(
        r#"
        INSERT INTO games (server_id, game_type, status, game_mode, is_private)
        VALUES ($1, $2, 'waiting', 'solo', true)
        RETURNING id
        "#
    )
    .bind(server_id)
    .bind(serde_json::to_value(&common::GameType::Custom { settings: settings.clone() })?)
    .fetch_one(pool)
    .await?;
    
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
    pool: &PgPool,
    user_id: i32,
    game_code: &str,
) -> Result<u32> {
    // Find the game by code
    let (game_id, is_private): (i32, bool) = sqlx::query_as(
        "SELECT id, is_private FROM games WHERE game_code = $1 AND status = 'waiting'"
    )
    .bind(game_code)
    .fetch_one(pool)
    .await
    .context("Game not found or already started")?;
    
    // Check if game is full
    let player_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM game_players WHERE game_id = $1"
    )
    .bind(game_id)
    .fetch_one(pool)
    .await?;
    
    // Get max players from game settings
    let max_players: i32 = sqlx::query_scalar(
        r#"
        SELECT (game_type->'settings'->>'max_players')::int
        FROM games
        WHERE id = $1
        "#
    )
    .bind(game_id)
    .fetch_one(pool)
    .await?;
    
    if player_count >= max_players as i64 {
        return Err(anyhow::anyhow!("Game is full"));
    }
    
    // For now, we need to handle player joining differently since GameState
    // only allows adding players on tick 0. We'll need to implement a proper
    // lobby system or modify the game engine to support late joins.
    
    // Add player to the game_players table
    sqlx::query(
        "INSERT INTO game_players (game_id, user_id, team_id) VALUES ($1, $2, 0)"
    )
    .bind(game_id)
    .bind(user_id)
    .execute(pool)
    .await?;
    
    // TODO: Implement proper player joining through Redis events when game hasn't started yet
    warn!("Player joining for custom games needs proper implementation");
    
    Ok(game_id as u32)
}

async fn check_game_host(
    pool: &PgPool,
    game_id: u32,
    user_id: i32,
) -> Result<bool> {
    let host_user_id: Option<i32> = sqlx::query_scalar(
        r#"
        SELECT host_user_id
        FROM custom_game_lobbies
        WHERE game_id = $1
        "#
    )
    .bind(game_id as i32)
    .fetch_optional(pool)
    .await?;
    
    Ok(host_user_id == Some(user_id))
}

async fn spectate_game(
    pool: &PgPool,
    user_id: i32,
    game_id: u32,
    game_code: Option<&str>,
) -> Result<u32> {
    // If game_code is provided, look up game by code
    let actual_game_id = if let Some(code) = game_code {
        let result: Option<(i32, bool)> = sqlx::query_as(
            r#"
            SELECT g.id, g.is_private
            FROM games g
            WHERE g.game_code = $1
            "#
        )
        .bind(code)
        .fetch_optional(pool)
        .await?;
        
        if let Some((id, is_private)) = result {
            // Check if spectators are allowed for private games
            if is_private {
                let allow_spectators: Option<bool> = sqlx::query_scalar(
                    r#"
                    SELECT allow_spectators
                    FROM custom_game_lobbies
                    WHERE game_id = $1
                    "#
                )
                .bind(id)
                .fetch_optional(pool)
                .await?;
                
                if !allow_spectators.unwrap_or(false) {
                    return Err(anyhow::anyhow!("Spectators are not allowed for this game"));
                }
            }
            id as u32
        } else {
            return Err(anyhow::anyhow!("Invalid game code"));
        }
    } else {
        // Direct game_id access - check if game exists and is public
        let is_private: Option<bool> = sqlx::query_scalar(
            r#"
            SELECT is_private
            FROM games
            WHERE id = $1
            "#
        )
        .bind(game_id as i32)
        .fetch_optional(pool)
        .await?;
        
        match is_private {
            Some(false) => game_id, // Public game, allow spectating
            Some(true) => return Err(anyhow::anyhow!("Cannot spectate private game without code")),
            None => return Err(anyhow::anyhow!("Game not found")),
        }
    };
    
    // Add spectator to the game_spectators table
    sqlx::query(
        r#"
        INSERT INTO game_spectators (game_id, user_id, joined_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (game_id, user_id) DO NOTHING
        "#
    )
    .bind(actual_game_id as i32)
    .bind(user_id)
    .execute(pool)
    .await?;
    
    info!("User {} joined as spectator for game {}", user_id, actual_game_id);
    Ok(actual_game_id)
}

/// Subscribe to game events via replication manager and forward to the WebSocket client
async fn subscribe_to_game_events_via_replication(
    replication_manager: Arc<crate::replication::ReplicationManager>,
    game_id: u32,
    ws_tx: mpsc::Sender<Message>,
) -> Result<()> {
    info!("Subscribing to game {} via replication manager", game_id);
    
    // Subscribe to the game through replication manager
    let (game_state, mut event_receiver) = replication_manager.subscribe_to_game(game_id).await?;
    
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
    
    // Listen for subsequent events
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


