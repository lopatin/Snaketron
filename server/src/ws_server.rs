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
use tokio::sync::{broadcast, oneshot, mpsc};
use tokio::task::JoinHandle;
use tokio::time::Sleep;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tungstenite::Utf8Bytes;
use common::{GameCommand, Direction, GameEventMessage};
use crate::games_manager::GamesManager;

#[derive(Debug, Serialize, Deserialize)]
pub enum WSMessage {
    Token(String),
    JoinGame(u32),
    GameCommand(GameCommand),
    Chat(String),
    Shutdown,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UserToken {
    pub user_id: i32,
}


pub async fn run_websocket_server(
    addr: &str,
    cancellation_token: CancellationToken
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
                        let handle = tokio::spawn(handle_websocket_connection(stream, connection_token));
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
    games_manager: GamesManager,
    stream: TcpStream,
    cancellation_token: CancellationToken
) -> Result<()> {
    let peer_addr = stream.peer_addr()?;
    info!("Handling WebSocket connection from: {}", peer_addr);

    let mut ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .expect("Failed to accept WebSocket connection");

    let mut game_command_tx: Option<broadcast::Sender<GameCommand>> = None;
    let mut game_event_rx: Option<broadcast::Receiver<GameEventMessage>> = None;

    let drop_channels = || {
        if let Some(tx) = game_command_tx.take() {
            info!("Dropping game command sender");
            drop(tx);
        }
        if let Some(rx) = game_event_rx.take() {
            info!("Dropping game event receiver");
            drop(rx);
        }
    };

    let mut timeout_future: Option<Pin<Box<Sleep>>> = None;
    let mut user_token: Option<UserToken> = None;
    let mut game_id: Option<u32> = None;

    loop {
        tokio::select! {
            biased;

            // The client hasn't closed the connection after being asked to do so. Force close it.
            _ = async { timeout_future.as_mut().unwrap().await } , if timeout_future.is_some() => {
                warn!("Timeout reached, closing connection");
                break;
            }

            _ = cancellation_token.cancelled() => {
                // Send a Shutdown message to the client so that it can close this
                // connection and gracefully rotate to a new one.
                info!("Sending shutdown message to client");
                let json_msg = serde_json::json!(WSMessage::Shutdown);
                let shutdown_msg = Message::Text(Utf8Bytes::from(json_msg.to_string()));
                if let Err(e) = ws_stream.send(shutdown_msg).await {
                    error!("Failed to send shutdown message: {}", e);
                }

                timeout_future = Some(Box::pin(tokio::time::sleep(Duration::from_secs(10))));
            }

            // Handle messages
            message = ws_stream.next() => {
                match message {
                    Some(Ok(msg)) => {
                        let ws_message: WSMessage = serde_json::from_str(msg.to_text()?)?;
                        match ws_message {
                            WSMessage::Token(jwt_token) => {
                                info!("Received jwt token: {}", jwt_token);
                                match verify_jwt_token(jwt_token) {
                                    Ok(ut) => {
                                        info!("Token verified successfully");
                                        user_token = Some(ut);
                                    }
                                    Err(e) => {
                                        error!("Failed to verify token: {}", e);
                                        break;
                                    }
                                }
                            }
                            _ if user_token.is_none() => {
                                warn!("User token not set. Cannot process message.");
                                continue;
                            }
                            WSMessage::JoinGame(id) => {
                                info!("Joining game ID: {}", id);

                                drop_channels();

                                let (tx, rx) = games_manager.join_game(id).await?;
                                game_command_tx = Some(tx);
                                game_event_rx = Some(rx);
                                game_id = Some(id);
                            }
                            WSMessage::GameCommand(command) => {
                                info!("Received command: {:?}", command);

                                if let Some(tx)  = game_command_tx.as_ref() {
                                    tx.send(command)
                                        .context("Failed to send game command")
                                        .await?;
                                } else {
                                    warn!("Game command sender not initialized");
                                }
                            }
                            WSMessage::Chat(chat_message) => {
                                info!("Received chat message: {}", chat_message);
                            }
                            _ => {
                                warn!("Unexpected message type: {:?}", ws_message);
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

            // Handle game events
            game_event = game_event_rx.as_ref().unwrap().recv(), if game_event_rx.is_some() => {
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
                        break;
                    }
                }
            }

        }
    }

    let _ = ws_stream.close(None).await;
    drop_channels();
    Ok(())
}

fn verify_jwt_token(token: String) -> Result<UserToken> {
    // TODO: Implement JWT verification logic
    // Ok(UserToken { user_id: 0 })
    Err(anyhow::anyhow!("JWT verification not implemented"))
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