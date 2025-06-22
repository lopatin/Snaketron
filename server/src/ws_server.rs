use std::pin::Pin;
use tracing::{debug, error, info, warn};
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::wrappers::BroadcastStream;
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
use common::{GameCommand, GameCommandMessage, GameEvent, GameEventMessage, GameStatus};
use crate::raft::{RaftNode, StateChangeEvent};

#[derive(Debug, Serialize, Deserialize)]
pub enum WSMessage {
    Token(String),
    JoinGame(u32),
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
    // Custom game messages
    CreateCustomGame { settings: common::CustomGameSettings },
    JoinCustomGame { game_code: String },
    UpdateCustomGameSettings { settings: common::CustomGameSettings },
    StartCustomGame,
    SpectateGame { game_id: u32, game_code: Option<String> },
    // Solo game messages
    CreateSoloGame { mode: common::SoloMode },
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

async fn add_to_matchmaking_queue(
    pool: &PgPool,
    user_id: i32,
    game_type: common::GameType,
) -> Result<()> {
    // Get the first available server from the database
    let server_id: i32 = sqlx::query_scalar(
        "SELECT id FROM servers WHERE status = 'active' LIMIT 1"
    )
    .fetch_one(pool)
    .await
    .context("No active servers available")?;
    
    // Remove any existing queue entry for this user
    sqlx::query(
        "DELETE FROM game_requests WHERE user_id = $1"
    )
    .bind(user_id)
    .execute(pool)
    .await?;
    
    // Insert new queue entry
    sqlx::query(
        r#"
        INSERT INTO game_requests (server_id, user_id, game_type, request_time)
        VALUES ($1, $2, $3, NOW())
        "#
    )
    .bind(server_id)
    .bind(user_id)
    .bind(serde_json::to_value(&game_type)?)
    .execute(pool)
    .await?;
    
    info!("User {} added to matchmaking queue for game type {:?}", user_id, game_type);
    Ok(())
}

async fn remove_from_matchmaking_queue(
    pool: &PgPool,
    user_id: i32,
) -> Result<()> {
    let result = sqlx::query(
        "DELETE FROM game_requests WHERE user_id = $1"
    )
    .bind(user_id)
    .execute(pool)
    .await?;
    
    if result.rows_affected() > 0 {
        info!("User {} removed from matchmaking queue", user_id);
    } else {
        info!("User {} was not in matchmaking queue", user_id);
    }
    
    Ok(())
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
    raft: Arc<RaftNode>,
    db_pool: PgPool,
    cancellation_token: CancellationToken,
    jwt_verifier: Arc<dyn JwtVerifier>,
) -> Result<()> {

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
                        let raft_clone = raft.clone();
                        let handle = tokio::spawn(handle_websocket_connection(raft_clone, db_pool_clone, stream, jwt_verifier_clone, connection_token));
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
    raft: Arc<RaftNode>,
    db_pool: PgPool,
    stream: TcpStream,
    jwt_verifier: Arc<dyn JwtVerifier>,
    cancellation_token: CancellationToken,
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
    
    // Subscribe to Raft state changes to get notified when games are created
    
    let mut state_event_stream = BroadcastStream::new(raft.subscribe_state_events());
    tokio::pin!(state_event_stream);
    
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
            
            // Handle Raft state change events
            Some(Ok(event)) = state_event_stream.next() => {
                match event {
                    StateChangeEvent::GameCreated { game_id } => {
                        if let ConnectionState::Authenticated { metadata } = &state {
                            // Get the game state from Raft
                            if let Some(game_state) = raft.get_game_state(game_id).await {
                                if game_state.players.contains_key(&(metadata.user_id as u32)) {
                                    state = ConnectionState::InGame {
                                        metadata: metadata.clone(),
                                        game_id,
                                        // command_tx: game_state.command_tx.clone(),
                                        // event_rx: game_state.event_rx.clone(),
                                    };
                                }
                            }
                        }
                    }
                    
                    StateChangeEvent::GameEvent { event } => {
                        if let ConnectionState::InGame { metadata, game_id, .. } = &state {
                            if event.game_id == *game_id {
                                // Forward game events to the client
                                let event_msg = WSMessage::GameEvent(event.clone());
                                let json_msg = serde_json::to_string(&event_msg)?;
                                let ws_msg = Message::Text(Utf8Bytes::from(json_msg));
                                if let Err(e) = ws_stream.send(ws_msg).await {
                                    error!("Failed to send game event message: {}", e);
                                    break;
                                }
                                
                                // Check if the game has ended
                                match &event.event {
                                    GameEvent::SoloGameEnded { .. } => {
                                        info!("Solo game {} has ended, transitioning back to Authenticated state", game_id);
                                        // Transition back to Authenticated state
                                        state = ConnectionState::Authenticated { 
                                            metadata: metadata.clone() 
                                        };
                                    }
                                    GameEvent::StatusUpdated { status: GameStatus::Complete { .. } } => {
                                        // For multiplayer games, transition immediately on Complete
                                        // For solo games, we'll transition on SoloGameEnded instead
                                        // We can check if this is a solo game by looking at the game state
                                        if let Some(game_state) = raft.get_game_state(*game_id).await {
                                            if !game_state.game_type.is_solo() {
                                                info!("Multiplayer game {} has ended, transitioning back to Authenticated state", game_id);
                                                state = ConnectionState::Authenticated { 
                                                    metadata: metadata.clone() 
                                                };
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                    
                    _ => {}
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
                        
                        let process_result = process_ws_message(
                            state,
                            ws_message,
                            &jwt_verifier,
                            &db_pool,
                            &ws_tx,
                            &mut ws_stream,
                            &raft,
                        ).await;
                        
                        match process_result {
                            Ok(new_state) => {
                                state = new_state;
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
    raft: &Arc<RaftNode>,
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

                    // Add to matchmaking queue
                    if let Err(e) = add_to_matchmaking_queue(db_pool, metadata.user_id, game_type).await {
                        error!("Failed to add user to matchmaking queue: {}", e);
                    }

                    // Polling for match is now handled by the matchmaking service
                    // No need to poll here

                    Ok(ConnectionState::Authenticated { metadata })
                }
                WSMessage::LeaveQueue => {
                    info!("User {} ({}) leaving matchmaking queue", metadata.username, metadata.user_id);

                    // Remove from matchmaking queue
                    if let Err(e) = remove_from_matchmaking_queue(db_pool, metadata.user_id).await {
                        error!("Failed to remove user from matchmaking queue: {}", e);
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
                    
                    match create_custom_game(db_pool, raft, metadata.user_id, settings).await {
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
                    
                    match join_custom_game(db_pool, raft, metadata.user_id, &game_code).await {
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
                WSMessage::CreateSoloGame { mode } => {
                    info!("User {} ({}) creating solo game with mode: {:?}", metadata.username, metadata.user_id, mode);
                    
                    match create_solo_game(db_pool, raft, metadata.user_id, mode).await {
                        Ok(game_id) => {
                            // Send success response
                            let response = WSMessage::SoloGameCreated { game_id };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_stream.send(Message::Text(json_msg.into())).await?;
                            
                            // Transition to in-game state
                            Ok(ConnectionState::InGame { 
                                metadata,
                                game_id,
                            })
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
                    // Submit command to Raft
                    let request = crate::raft::ClientRequest::SubmitGameCommand {
                        game_id,
                        user_id: metadata.user_id as u32,
                        command: command_message,
                    };
                    
                    match raft.propose(request).await {
                        Ok(_) => {
                            debug!("Successfully submitted game command to Raft");
                            Ok(ConnectionState::InGame { metadata, game_id })
                        }
                        Err(e) => {
                            error!("Failed to submit command to Raft: {}", e);
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
                    
                    // Start the game via Raft
                    let request = crate::raft::ClientRequest::StartGame { 
                        game_id,
                        server_id: raft.id,
                    };
                    match raft.propose(request).await {
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
    raft: &Arc<RaftNode>,
    user_id: i32,
    settings: common::CustomGameSettings,
) -> Result<(u32, String)> {
    let game_code = generate_game_code();
    
    // Get current server ID from Raft node ID
    let server_id = raft.id as i32;
    
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
    game_state.add_player(user_id as u32)?;
    
    // Submit to Raft
    let request = crate::raft::ClientRequest::CreateGame {
        game_id: game_id as u32,
        game_state,
    };
    
    raft.propose(request).await?;
    
    Ok((game_id as u32, game_code))
}

async fn create_solo_game(
    pool: &PgPool,
    raft: &Arc<RaftNode>,
    user_id: i32,
    mode: common::SoloMode,
) -> Result<u32> {
    // Get current server ID from Raft node ID
    let server_id = raft.id as i32;
    
    // Create game settings based on solo mode
    let settings = common::CustomGameSettings {
        arena_width: 40,
        arena_height: 40,
        tick_duration_ms: 300,
        food_spawn_rate: 3.0,
        max_players: 1,  // Solo game
        game_mode: common::GameMode::Solo,
        is_private: true,
        allow_spectators: false,
        snake_start_length: 4,
        tactical_mode: mode == common::SoloMode::Tactical,
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
    game_state.add_player(user_id as u32)?;
    
    // Submit to Raft
    let request = crate::raft::ClientRequest::CreateGame {
        game_id: game_id as u32,
        game_state,
    };
    
    raft.propose(request).await?;
    
    // Start the game immediately (no waiting in solo mode)
    let start_request = crate::raft::ClientRequest::StartGame { 
        game_id: game_id as u32,
        server_id: raft.id,
    };
    raft.propose(start_request).await?;
    
    Ok(game_id as u32)
}

async fn join_custom_game(
    pool: &PgPool,
    raft: &Arc<RaftNode>,
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
    
    // TODO: Implement proper player joining through Raft when game hasn't started yet
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


