use std::pin::Pin;
use tracing::{error, info, instrument, trace, warn};
use tokio::net::{TcpListener, TcpStream};
use anyhow::{Context, Result};
use sqlx::PgPool;
use chrono::{DateTime, Utc};
use uuid::Uuid;
use std::time::Duration;
use futures_util::future::join_all;
use futures_util::{SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, oneshot, mpsc, Mutex, RwLock};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time::Sleep;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tungstenite::Utf8Bytes;
use common::{GameCommand, GameCommandMessage, GameEvent, GameEventMessage};
use crate::game_manager::GameManager;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum WSMessage {
    Token(String),
    JoinGame(u32),
    GameCommand(GameCommand),
    GameEvent(GameEventMessage),
    Chat(String),
    Shutdown,
    Ping,
    Pong,
    // Matchmaking messages
    QueueForMatch { game_type: common::GameType },
    LeaveQueue,
    MatchFound { game_id: u32 },
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

// JWT verification trait for dependency injection
#[async_trait::async_trait]
pub trait JwtVerifier: Send + Sync {
    async fn verify(&self, token: &str) -> Result<UserToken>;
}

// Default implementation that always fails
pub struct DefaultJwtVerifier;

#[async_trait::async_trait]
impl JwtVerifier for DefaultJwtVerifier {
    async fn verify(&self, _token: &str) -> Result<UserToken> {
        Err(anyhow::anyhow!("JWT verification not implemented"))
    }
}

async fn add_to_matchmaking_queue(
    pool: &PgPool,
    user_id: i32,
    game_type: common::GameType,
) -> Result<()> {
    // First, get the current server ID (we need to know which server the user is connected to)
    // For now, we'll use a placeholder. In a real implementation, this would be passed down.
    let server_id = get_current_server_id(pool).await?;
    
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

async fn poll_for_match(pool: PgPool, user_id: i32, ws_tx: mpsc::Sender<Message>) {
    info!("Starting match polling for user {}", user_id);
    let mut interval = tokio::time::interval(Duration::from_millis(500));
    let timeout = tokio::time::sleep(Duration::from_secs(30)); // Stop polling after 30 seconds
    tokio::pin!(timeout);
    
    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Check if user has been matched
                match check_for_match(&pool, user_id).await {
                    Ok(Some(game_id)) => {
                        info!("User {} matched to game {}", user_id, game_id);
                        // Send MatchFound message
                        let msg = WSMessage::MatchFound { game_id: game_id as u32 };
                        if let Ok(json) = serde_json::to_string(&msg) {
                            let _ = ws_tx.send(Message::Text(json.into())).await;
                        }
                        break;
                    }
                    Ok(None) => {
                        // No match yet, continue polling
                        trace!("No match yet for user {}", user_id);
                    }
                    Err(e) => {
                        error!("Error checking for match: {}", e);
                        break;
                    }
                }
            }
            _ = &mut timeout => {
                info!("Match polling timeout for user {}", user_id);
                break;
            }
        }
    }
    info!("Stopped polling for user {}", user_id);
}

async fn check_for_match(pool: &PgPool, user_id: i32) -> Result<Option<i32>> {
    let game_id: Option<i32> = sqlx::query_scalar(
        r#"
        SELECT game_id
        FROM game_requests
        WHERE user_id = $1 AND game_id IS NOT NULL
        ORDER BY request_time DESC
        LIMIT 1
        "#
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await?;
    
    Ok(game_id)
}

async fn get_current_server_id(pool: &PgPool) -> Result<Uuid> {
    // In a real implementation, this would be stored when the server starts
    // For now, get the first server or create one
    let server_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM servers ORDER BY created_at DESC LIMIT 1"
    )
    .fetch_optional(pool)
    .await?;
    
    match server_id {
        Some(id) => Ok(id),
        None => {
            // Create a default server for testing
            let id = Uuid::new_v4();
            sqlx::query(
                r#"
                INSERT INTO servers (id, address, last_heartbeat, current_game_count, max_game_capacity)
                VALUES ($1, $2, NOW(), 0, 100)
                "#
            )
            .bind(id)
            .bind("127.0.0.1:8080")
            .execute(pool)
            .await?;
            Ok(id)
        }
    }
}

// Connection state machine
enum ConnectionState {
    // Initial state - waiting for authentication
    Unauthenticated,
    
    // Authenticated but not in a game
    Authenticated { 
        user_token: UserToken 
    },
    
    // Authenticated and connected to a game
    InGame {
        user_token: UserToken,
        game_id: u32,
        command_tx: mpsc::Sender<GameCommandMessage>,
        event_rx: broadcast::Receiver<GameEventMessage>,
    },
    
    // Connection is shutting down
    ShuttingDown {
        timeout: Pin<Box<Sleep>>,
    },
}

impl ConnectionState {
    // Extract game channels if in game state
    fn take_game_channels(&mut self) -> Option<(mpsc::Sender<GameCommandMessage>, broadcast::Receiver<GameEventMessage>)> {
        match std::mem::replace(self, ConnectionState::Unauthenticated) {
            ConnectionState::InGame { user_token, command_tx, event_rx, .. } => {
                *self = ConnectionState::Authenticated { user_token };
                Some((command_tx, event_rx))
            }
            other => {
                *self = other;
                None
            }
        }
    }
}



pub async fn run_websocket_server(
    addr: &str,
    games_manager: Arc<RwLock<GameManager>>,
    db_pool: PgPool,
    cancellation_token: CancellationToken,
    jwt_verifier: Arc<dyn JwtVerifier>,
    player_connections: Arc<crate::player_connections::PlayerConnectionManager>
) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("WebSocket server listening on {}", addr);
    run_websocket_server_with_listener(listener, games_manager, db_pool, cancellation_token, jwt_verifier, player_connections).await
}

pub async fn run_websocket_server_with_listener(
    listener: TcpListener,
    games_manager: Arc<RwLock<GameManager>>,
    db_pool: PgPool,
    cancellation_token: CancellationToken,
    jwt_verifier: Arc<dyn JwtVerifier>,
    player_connections: Arc<crate::player_connections::PlayerConnectionManager>
) -> Result<()> {

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
                        let games_manager_clone = games_manager.clone();
                        let jwt_verifier_clone = jwt_verifier.clone();
                        let db_pool_clone = db_pool.clone();
                        let player_connections_clone = player_connections.clone();
                        let handle = tokio::spawn(handle_websocket_connection(games_manager_clone, db_pool_clone, stream, connection_token, jwt_verifier_clone, player_connections_clone));
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
    games_manager: Arc<RwLock<GameManager>>,
    db_pool: PgPool,
    stream: TcpStream,
    cancellation_token: CancellationToken,
    jwt_verifier: Arc<dyn JwtVerifier>,
    player_connections: Arc<crate::player_connections::PlayerConnectionManager>
) -> Result<()> {
    let peer_addr = stream.peer_addr()?;
    info!("Handling WebSocket connection from: {}", peer_addr);

    let mut ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .expect("Failed to accept WebSocket connection");

    // Create a channel for sending messages to the WebSocket
    let (ws_tx, mut ws_rx) = mpsc::channel::<Message>(32);
    
    // Start in unauthenticated state
    let mut state = ConnectionState::Unauthenticated;
    
    // Create a shutdown timeout that starts as a never-completing future
    let shutdown_timeout = tokio::time::sleep(Duration::from_secs(u64::MAX));
    tokio::pin!(shutdown_timeout);
    let mut shutdown_started = false;

    loop {
        tokio::select! {
            biased;
            
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
                        
                        // Process message based on current state
                        state = match std::mem::replace(&mut state, ConnectionState::Unauthenticated) {
                            ConnectionState::Unauthenticated => {
                                match ws_message {
                                    WSMessage::Token(jwt_token) => {
                                        info!("Received jwt token: {}", jwt_token);
                                        match jwt_verifier.verify(&jwt_token).await {
                                            Ok(user_token) => {
                                                info!("Token verified successfully, user_id: {}", user_token.user_id);
                                                // Register the player connection
                                                player_connections.register(user_token.user_id, ws_tx.clone()).await;
                                                ConnectionState::Authenticated { user_token }
                                            }
                                            Err(e) => {
                                                error!("Failed to verify token: {}", e);
                                                break;
                                            }
                                        }
                                    }
                                    WSMessage::Ping => {
                                        // Respond with Pong even in unauthenticated state
                                        let pong_msg = Message::Text(serde_json::to_string(&WSMessage::Pong)?.into());
                                        if let Err(e) = ws_stream.send(pong_msg).await {
                                            error!("Failed to send pong: {}", e);
                                            break;
                                        }
                                        ConnectionState::Unauthenticated
                                    }
                                    _ => {
                                        warn!("Cannot process message in unauthenticated state");
                                        ConnectionState::Unauthenticated
                                    }
                                }
                            }
                            
                            ConnectionState::Authenticated { user_token } => {
                                match ws_message {
                                    WSMessage::JoinGame(game_id) => {
                                        info!("Joining game ID: {}", game_id);
                                        let games_mgr = games_manager.read().await;
                                        match games_mgr.join_game(game_id).await {
                                            Ok((command_tx, event_rx)) => {
                                                // Try to get the current game snapshot (for local games)
                                                match games_mgr.get_game_snapshot(game_id).await {
                                                    Ok(game_state) => {
                                                        // Send snapshot to client
                                                        let snapshot_event = GameEventMessage {
                                                            game_id,
                                                            tick: game_state.tick,
                                                            user_id: None,
                                                            event: GameEvent::Snapshot { game_state },
                                                        };
                                                        
                                                        let snapshot_msg = Message::Text(serde_json::to_string(&snapshot_event)?.into());
                                                        if let Err(e) = ws_stream.send(snapshot_msg).await {
                                                            error!("Failed to send snapshot: {}", e);
                                                            ConnectionState::Authenticated { user_token }
                                                        } else {
                                                            info!("WS: Transitioning to InGame state for game {} (local)", game_id);
                                                            ConnectionState::InGame {
                                                                user_token,
                                                                game_id,
                                                                command_tx,
                                                                event_rx,
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        // For remote games, request a snapshot
                                                        info!("Could not get local snapshot ({}), requesting snapshot for remote game {}", e, game_id);
                                                        
                                                        // Send RequestSnapshot command
                                                        let snapshot_request = GameCommandMessage {
                                                            tick: 0,
                                                            received_order: 0,
                                                            user_id: user_token.user_id as u32,
                                                            command: GameCommand::RequestSnapshot,
                                                        };
                                                        
                                                        if let Err(e) = command_tx.send(snapshot_request).await {
                                                            error!("Failed to request snapshot: {}", e);
                                                        }
                                                        
                                                        ConnectionState::InGame {
                                                            user_token,
                                                            game_id,
                                                            command_tx,
                                                            event_rx,
                                                        }
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                error!("Failed to join game: {}", e);
                                                ConnectionState::Authenticated { user_token }
                                            }
                                        }
                                    }
                                    WSMessage::QueueForMatch { game_type } => {
                                        info!("User {} queuing for match type: {:?}", user_token.user_id, game_type);
                                        
                                        // Add to matchmaking queue
                                        if let Err(e) = add_to_matchmaking_queue(&db_pool, user_token.user_id, game_type).await {
                                            error!("Failed to add user to matchmaking queue: {}", e);
                                        }
                                        
                                        // Start polling for match
                                        let poll_pool = db_pool.clone();
                                        let poll_user_id = user_token.user_id;
                                        let poll_tx = ws_tx.clone();
                                        tokio::spawn(async move {
                                            poll_for_match(poll_pool, poll_user_id, poll_tx).await;
                                        });
                                        
                                        ConnectionState::Authenticated { user_token }
                                    }
                                    WSMessage::LeaveQueue => {
                                        info!("User {} leaving matchmaking queue", user_token.user_id);
                                        
                                        // Remove from matchmaking queue
                                        if let Err(e) = remove_from_matchmaking_queue(&db_pool, user_token.user_id).await {
                                            error!("Failed to remove user from matchmaking queue: {}", e);
                                        }
                                        
                                        ConnectionState::Authenticated { user_token }
                                    }
                                    WSMessage::Ping => {
                                        // Respond with Pong
                                        let pong_msg = Message::Text(serde_json::to_string(&WSMessage::Pong)?.into());
                                        if let Err(e) = ws_stream.send(pong_msg).await {
                                            error!("Failed to send pong: {}", e);
                                            break;
                                        }
                                        ConnectionState::Authenticated { user_token }
                                    }
                                    WSMessage::GameEvent(event_msg) => {
                                        // Handle game events - specifically initial snapshot from matchmaking
                                        if let GameEvent::Snapshot { ref game_state } = event_msg.event {
                                            info!("Received game snapshot for game {} - auto-joining", event_msg.game_id);
                                            
                                            // Join the game to get the channels
                                            let games_mgr = games_manager.read().await;
                                            match games_mgr.join_game(event_msg.game_id).await {
                                                Ok((command_tx, event_rx)) => {
                                                    // Forward the snapshot to the client
                                                    let snapshot_msg = Message::Text(serde_json::to_string(&event_msg)?.into());
                                                    if let Err(e) = ws_stream.send(snapshot_msg).await {
                                                        error!("Failed to forward snapshot: {}", e);
                                                        ConnectionState::Authenticated { user_token }
                                                    } else {
                                                        info!("WS: Auto-joined game {} after matchmaking", event_msg.game_id);
                                                        ConnectionState::InGame {
                                                            user_token,
                                                            game_id: event_msg.game_id,
                                                            command_tx,
                                                            event_rx,
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    error!("Failed to auto-join game {}: {}", event_msg.game_id, e);
                                                    ConnectionState::Authenticated { user_token }
                                                }
                                            }
                                        } else {
                                            warn!("Received non-snapshot game event while authenticated: {:?}", event_msg.event);
                                            ConnectionState::Authenticated { user_token }
                                        }
                                    }
                                    _ => {
                                        warn!("Unexpected message in authenticated state: {:?}", ws_message);
                                        ConnectionState::Authenticated { user_token }
                                    }
                                }
                            }
                            
                            ConnectionState::InGame { user_token, game_id, command_tx, event_rx } => {
                                match ws_message {
                                    WSMessage::GameCommand(command) => {
                                        info!("Received command: {:?}", command);
                                        
                                        let cmd_msg = GameCommandMessage {
                                            tick: 0, // Will be set by game engine
                                            received_order: 0, // Will be set by game engine
                                            user_id: user_token.user_id as u32,
                                            command,
                                        };
                                        
                                        if let Err(e) = command_tx.send(cmd_msg).await {
                                            warn!("Failed to send game command: {}", e);
                                        }
                                        
                                        ConnectionState::InGame { user_token, game_id, command_tx, event_rx }
                                    }
                                    WSMessage::JoinGame(new_game_id) => {
                                        info!("Switching from game {} to game {}", game_id, new_game_id);
                                        
                                        // Leave current game by dropping channels
                                        drop(command_tx);
                                        drop(event_rx);
                                        
                                        // Join new game
                                        match games_manager.read().await.join_game(new_game_id).await {
                                            Ok((command_tx, event_rx)) => {
                                                ConnectionState::InGame {
                                                    user_token,
                                                    game_id: new_game_id,
                                                    command_tx,
                                                    event_rx,
                                                }
                                            }
                                            Err(e) => {
                                                error!("Failed to join new game: {}", e);
                                                ConnectionState::Authenticated { user_token }
                                            }
                                        }
                                    }
                                    WSMessage::Ping => {
                                        // Respond with Pong
                                        let pong_msg = Message::Text(serde_json::to_string(&WSMessage::Pong)?.into());
                                        if let Err(e) = ws_stream.send(pong_msg).await {
                                            error!("Failed to send pong: {}", e);
                                            break;
                                        }
                                        ConnectionState::InGame { user_token, game_id, command_tx, event_rx }
                                    }
                                    _ => {
                                        warn!("Unexpected message in game state: {:?}", ws_message);
                                        ConnectionState::InGame { user_token, game_id, command_tx, event_rx }
                                    }
                                }
                            }
                            
                            ConnectionState::ShuttingDown { timeout } => {
                                // Ignore all messages during shutdown
                                ConnectionState::ShuttingDown { timeout }
                            }
                        };
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

            // Handle game events from the game engine
            game_event = async {
                match &mut state {
                    ConnectionState::InGame { event_rx, .. } => event_rx.recv().await,
                    _ => std::future::pending().await,
                }
            } => {
                match game_event {
                    Ok(event) => {
                        info!("WS: Received game event for game {}: {:?}", event.game_id, event.event);
                        let json_msg = serde_json::json!(event);
                        let game_event_msg = Message::Text(Utf8Bytes::from(json_msg.to_string()));
                        if let Err(e) = ws_stream.send(game_event_msg).await {
                            error!("Failed to send game event message: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Error receiving game event: {}", e);
                        // Game channel closed, transition back to authenticated state
                        if let ConnectionState::InGame { user_token, .. } = state {
                            state = ConnectionState::Authenticated { user_token };
                        }
                    }
                }
            }
        }
    }

    // Clean up connection
    let _ = ws_stream.close(None).await;
    
    // Unregister the player connection if authenticated
    match &state {
        ConnectionState::Authenticated { user_token } | ConnectionState::InGame { user_token, .. } => {
            player_connections.unregister(user_token.user_id).await;
            info!("Unregistered player connection for user {}", user_token.user_id);
        }
        _ => {}
    }
    
    // Drop any active game channels
    if let Some((command_tx, event_rx)) = state.take_game_channels() {
        info!("Dropping game channels on disconnect");
        drop(command_tx);
        drop(event_rx);
    }
    
    Ok(())
}


pub async fn register_server(pool: &PgPool, _region: &str) -> Result<Uuid> {
    info!("Registering server instance");
    
    let server_id = Uuid::new_v4();
    let address = "127.0.0.1:8080"; // In production, this would be the actual server address

    // Insert a new record and return the generated ID
    sqlx::query(
        r#"
        INSERT INTO servers (id, address, last_heartbeat, current_game_count, max_game_capacity)
        VALUES ($1, $2, NOW(), 0, 100)
        "#
    )
        .bind(server_id)
        .bind(address)
        .execute(pool)
        .await
        .context("Failed to register server in database")?;

    info!(?server_id, address, "Server registered successfully");
    Ok(server_id)
}

pub async fn run_heartbeat_loop(
    pool: PgPool,
    server_id: Uuid,
    cancellation_token: CancellationToken
) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    info!(?server_id, "Starting heartbeat loop");

    loop {
        tokio::select! {
            biased;
            _ = cancellation_token.cancelled() => {
                info!(?server_id, "Heartbeat shutdown received");
                break;
            }

            _ = interval.tick() => {
                let now = Utc::now();

                match sqlx::query(
                    r#"
                    UPDATE servers
                    SET last_heartbeat = $1
                    WHERE id = $2
                    "#
                )
                    .bind::<DateTime<Utc>>(now)
                    .bind(server_id)
                    .execute(&pool)
                    .await
                {
                    Ok(result) => {
                        if result.rows_affected() == 1 {
                            trace!(?server_id, timestamp = %now, "Heartbeat sent successfully.");
                        } else {
                            warn!(?server_id, "Heartbeat update affected {} rows (expected 1). Server record might be missing.", result.rows_affected());
                        }
                    }
                    Err(e) => {
                        error!(?server_id, error = %e, "Failed to send heartbeat");
                    }
                }
            }
        }
    }
}

