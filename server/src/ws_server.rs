use std::pin::Pin;
use tracing::{error, info, instrument, trace, warn};
use tokio::net::{TcpListener, TcpStream};
use anyhow::{Context, Result};
use sqlx::PgPool;
use chrono::{DateTime, Utc};
use std::time::Duration;
use futures_util::future::join_all;
use futures_util::{SinkExt, Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, oneshot, mpsc, Mutex};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time::Sleep;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tungstenite::Utf8Bytes;
use common::{GameCommand, GameCommandMessage, Direction, GameEvent, GameEventMessage};
use crate::games_manager::GamesManager;

#[derive(Debug, Serialize, Deserialize)]
pub enum WSMessage {
    Token(String),
    JoinGame(u32),
    GameCommand(GameCommand),
    Chat(String),
    Shutdown,
    Ping,
    Pong,
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
        command_tx: broadcast::Sender<GameCommandMessage>,
        event_rx: broadcast::Receiver<GameEventMessage>,
    },
    
    // Connection is shutting down
    ShuttingDown {
        timeout: Pin<Box<Sleep>>,
    },
}

impl ConnectionState {
    // Extract game channels if in game state
    fn take_game_channels(&mut self) -> Option<(broadcast::Sender<GameCommandMessage>, broadcast::Receiver<GameEventMessage>)> {
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
    games_manager: Arc<Mutex<GamesManager>>,
    cancellation_token: CancellationToken,
    jwt_verifier: Arc<dyn JwtVerifier>
) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("WebSocket server listening on {}", addr);
    run_websocket_server_with_listener(listener, games_manager, cancellation_token, jwt_verifier).await
}

pub async fn run_websocket_server_with_listener(
    listener: TcpListener,
    games_manager: Arc<Mutex<GamesManager>>,
    cancellation_token: CancellationToken,
    jwt_verifier: Arc<dyn JwtVerifier>
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
                        let handle = tokio::spawn(handle_websocket_connection(games_manager_clone, stream, connection_token, jwt_verifier_clone));
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
    games_manager: Arc<Mutex<GamesManager>>,
    stream: TcpStream,
    cancellation_token: CancellationToken,
    jwt_verifier: Arc<dyn JwtVerifier>
) -> Result<()> {
    let peer_addr = stream.peer_addr()?;
    info!("Handling WebSocket connection from: {}", peer_addr);

    let mut ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .expect("Failed to accept WebSocket connection");

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
                                                info!("Token verified successfully");
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
                                        let games_mgr = games_manager.lock().await;
                                        match games_mgr.join_game(game_id).await {
                                            Ok((command_tx, event_rx)) => {
                                                // Get the current game snapshot
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
                                                            ConnectionState::InGame {
                                                                user_token,
                                                                game_id,
                                                                command_tx,
                                                                event_rx,
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        error!("Failed to get game snapshot: {}", e);
                                                        ConnectionState::Authenticated { user_token }
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                error!("Failed to join game: {}", e);
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
                                        ConnectionState::Authenticated { user_token }
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
                                        
                                        if let Err(e) = command_tx.send(cmd_msg) {
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
                                        match games_manager.lock().await.join_game(new_game_id).await {
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
                        info!("Received game event: {:?}", event);
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
    
    // Drop any active game channels
    if let Some((command_tx, event_rx)) = state.take_game_channels() {
        info!("Dropping game channels on disconnect");
        drop(command_tx);
        drop(event_rx);
    }
    
    Ok(())
}


pub async fn register_server(pool: &PgPool, region: &str) -> Result<i32> {
    info!("Registering server instance");
    let hostname = gethostname::gethostname()
        .into_string()
        .unwrap_or_else(|_| "unknown".to_string());

    // Insert a new record and return the generated ID
    let server_id: i32 = sqlx::query_scalar(
        r#"
        INSERT INTO servers (hostname, region)
        VALUES ($1, $2)
        RETURNING id
        "#
    )
        .bind::<&str>(&hostname)
        .bind::<&str>(region)
        .fetch_one(pool)
        .await
        .context("Failed to register server in database")?;

    info!(server_id, %hostname, "Server registered successfully with ID");
    Ok(server_id)
}

pub async fn run_heartbeat_loop(
    pool: PgPool,
    server_id: i32,
    cancellation_token: CancellationToken
) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    info!(server_id, "Starting heartbeat loop");

    loop {
        tokio::select! {
            biased;
            _ = cancellation_token.cancelled() => {
                info!(server_id, "Heartbeat shutdown received");
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
                    .bind::<i32>(server_id)
                    .execute(&pool)
                    .await
                {
                    Ok(result) => {
                        if result.rows_affected() == 1 {
                            trace!(server_id, timestamp = %now, "Heartbeat sent successfully.");
                        } else {
                            warn!(server_id, "Heartbeat update affected {} rows (expected 1). Server record might be missing.", result.rows_affected());
                        }
                    }
                    Err(e) => {
                        error!(server_id, error = %e, "Failed to send heartbeat");
                    }
                }
            }
        }
    }
}