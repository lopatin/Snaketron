use std::pin::Pin;
use tracing::{error, info, instrument, trace, warn};
use tokio::net::{TcpListener, TcpStream};
use anyhow::{Context, Result};
use sqlx::PgPool;
use chrono::{DateTime, Utc};
use std::time::Duration;
use futures_util::future::join_all;
use futures_util::{SinkExt, Stream, StreamExt};
use tokio::sync::{broadcast, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Sleep;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tungstenite::Utf8Bytes;

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
    stream: TcpStream,
    cancellation_token: CancellationToken
) -> Result<()> {
    let peer_addr = stream.peer_addr()?;
    info!("Handling WebSocket connection from: {}", peer_addr);

    let mut ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .expect("Failed to accept WebSocket connection");

    let mut timeout_future: Option<Pin<Box<Sleep>>> = None;

    loop {
        tokio::select! {
            biased;
            _ = cancellation_token.cancelled() => {
                // Send a Shutdown message to the client so that it can close this
                // connection and gracefully rotate to a new one.
                info!("Sending shutdown message to client");
                let json_msg = serde_json::json!({"type": "shutdown"});
                if let Err(e) = ws_stream.send(Message::Text(Utf8Bytes::from(json_msg.to_string()))).await {
                    error!("Failed to send shutdown message: {}", e);
                }

                timeout_future = Some(Box::pin(tokio::time::sleep(Duration::from_secs(10))));
            }

            // Handle messages
            message = ws_stream.next() => {
                match message {
                    Some(Ok(msg)) => {
                        info!("Received message: {:?}", msg);
                        // Handle the message (e.g., process it, send a response, etc.)
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

            // The client hasn't closed the connection after being asked to do so. Force close it.
            _ = async { timeout_future.as_mut().unwrap().await } , if timeout_future.is_some() => {
                warn!("Timeout reached, closing connection");
                break;
            }
        }
    }

    let _ = ws_stream.close(None).await;
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