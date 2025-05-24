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
use common::{GameCommand, GameCommandMessage, Direction, GameEventMessage};
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
                                        match games_manager.lock().await.join_game(game_id).await {
                                            Ok((command_tx, event_rx)) => {
                                                ConnectionState::InGame {
                                                    user_token,
                                                    game_id,
                                                    command_tx,
                                                    event_rx,
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

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
    use tokio::net::TcpStream as TokioTcpStream;
    use std::collections::HashMap;
    use tokio::task::JoinHandle;
    
    /// Mock JWT verifier for testing
    pub struct MockJwtVerifier {
        expected_tokens: HashMap<String, UserToken>,
    }
    
    impl MockJwtVerifier {
        pub fn new() -> Self {
            Self {
                expected_tokens: HashMap::new(),
            }
        }
        
        pub fn with_token(mut self, token: &str, user_id: i32) -> Self {
            self.expected_tokens.insert(token.to_string(), UserToken { user_id });
            self
        }
        
        /// Creates a mock verifier that accepts any token
        pub fn accept_any() -> Self {
            Self {
                expected_tokens: HashMap::new(),
            }
        }
    }
    
    #[async_trait::async_trait]
    impl JwtVerifier for MockJwtVerifier {
        async fn verify(&self, token: &str) -> Result<UserToken> {
            if self.expected_tokens.is_empty() {
                // Accept any token mode
                Ok(UserToken { user_id: 1 })
            } else if let Some(user_token) = self.expected_tokens.get(token) {
                Ok(user_token.clone())
            } else {
                Err(anyhow::anyhow!("Invalid token"))
            }
        }
    }
    
    /// Test server builder for configuring test scenarios
    pub struct TestServerBuilder {
        port: u16,
        jwt_verifier: Option<Arc<dyn JwtVerifier>>,
    }
    
    impl TestServerBuilder {
        pub fn new() -> Self {
            Self {
                port: 0, // 0 means random available port
                jwt_verifier: None,
            }
        }
        
        pub fn with_port(mut self, port: u16) -> Self {
            self.port = port;
            self
        }
        
        pub fn with_mock_auth(mut self) -> Self {
            self.jwt_verifier = Some(Arc::new(MockJwtVerifier::accept_any()));
            self
        }
        
        pub fn with_jwt_verifier(mut self, verifier: Arc<dyn JwtVerifier>) -> Self {
            self.jwt_verifier = Some(verifier);
            self
        }
        
        pub async fn build(self) -> Result<TestServer> {
            let addr = format!("127.0.0.1:{}", self.port);
            let listener = TcpListener::bind(&addr).await?;
            let actual_addr = listener.local_addr()?;
            
            let games_manager = Arc::new(Mutex::new(GamesManager::new()));
            let cancellation_token = CancellationToken::new();
            let jwt_verifier = self.jwt_verifier.unwrap_or_else(|| Arc::new(DefaultJwtVerifier));
            
            let games_manager_clone = games_manager.clone();
            let cancellation_token_clone = cancellation_token.clone();
            
            // Spawn the server in a separate task
            let server_handle = tokio::spawn(async move {
                run_websocket_server_with_listener(
                    listener,
                    games_manager_clone,
                    cancellation_token_clone,
                    jwt_verifier,
                ).await
            });
            
            // Give the server a moment to start
            tokio::time::sleep(Duration::from_millis(100)).await;
            
            let ws_addr = format!("ws://{}", actual_addr);
            info!("Test server started at {}", ws_addr);
            
            Ok(TestServer {
                addr: ws_addr,
                games_manager,
                cancellation_token,
                server_handle,
            })
        }
    }
    
    /// Represents a running test server
    pub struct TestServer {
        pub addr: String,
        pub games_manager: Arc<Mutex<GamesManager>>,
        cancellation_token: CancellationToken,
        server_handle: JoinHandle<Result<()>>,
    }
    
    impl TestServer {
        pub async fn connect_client(&self) -> Result<TestClient> {
            let (ws_stream, _) = connect_async(&self.addr).await?;
            Ok(TestClient {
                ws: ws_stream,
                user_id: None,
            })
        }
        
        pub async fn shutdown(self) -> Result<()> {
            self.cancellation_token.cancel();
            self.server_handle.await??;
            Ok(())
        }
    }
    
    /// Test client wrapper for easier testing
    pub struct TestClient {
        ws: WebSocketStream<MaybeTlsStream<TokioTcpStream>>,
        pub user_id: Option<i32>,
    }
    
    impl TestClient {
        pub async fn authenticate(&mut self, token: &str) -> Result<()> {
            self.send_message(WSMessage::Token(token.to_string())).await?;
            // In a real test, we'd wait for a response or check connection state
            self.user_id = Some(1); // Mock user ID
            Ok(())
        }
        
        pub async fn send_ping(&mut self) -> Result<()> {
            self.send_message(WSMessage::Ping).await
        }
        
        pub async fn expect_pong(&mut self) -> Result<()> {
            let msg = self.receive_message().await?;
            match msg {
                WSMessage::Pong => Ok(()),
                _ => Err(anyhow::anyhow!("Expected Pong, got {:?}", msg)),
            }
        }
        
        pub async fn join_game(&mut self, game_id: u32) -> Result<()> {
            self.send_message(WSMessage::JoinGame(game_id)).await
        }
        
        pub async fn send_message(&mut self, msg: WSMessage) -> Result<()> {
            let json = serde_json::to_string(&msg)?;
            self.ws.send(Message::Text(json.into())).await?;
            Ok(())
        }
        
        pub async fn receive_message(&mut self) -> Result<WSMessage> {
            let timeout = tokio::time::timeout(Duration::from_secs(5), self.ws.next()).await;
            match timeout {
                Ok(Some(msg)) => {
                    match msg? {
                        Message::Text(text) => {
                            Ok(serde_json::from_str(&text)?)
                        }
                        _ => Err(anyhow::anyhow!("Unexpected message type")),
                    }
                }
                Ok(None) => Err(anyhow::anyhow!("Connection closed")),
                Err(_) => Err(anyhow::anyhow!("Timeout waiting for message")),
            }
        }
        
        pub async fn disconnect(mut self) -> Result<()> {
            self.ws.close(None).await?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::test_utils::*;
    
    #[tokio::test]
    async fn test_simple_connection() -> Result<()> {
        // Initialize tracing for tests
        let _ = tracing_subscriber::fmt::try_init();
        
        // Test just starting and stopping a server
        tokio::time::timeout(Duration::from_secs(5), async {
            info!("Creating test server");
            
            let games_manager = Arc::new(Mutex::new(GamesManager::new()));
            let cancellation_token = CancellationToken::new();
            let jwt_verifier = Arc::new(MockJwtVerifier::accept_any()) as Arc<dyn JwtVerifier>;
            
            let cancellation_token_clone = cancellation_token.clone();
            let server_handle = tokio::spawn(async move {
                run_websocket_server(
                    "127.0.0.1:8888",
                    games_manager,
                    cancellation_token_clone,
                    jwt_verifier,
                ).await
            });
            
            // Give server time to start
            tokio::time::sleep(Duration::from_millis(100)).await;
            
            info!("Server started, shutting down");
            
            // Shutdown
            cancellation_token.cancel();
            
            // Wait for server to finish
            let result = server_handle.await?;
            info!("Server shutdown result: {:?}", result);
            
            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out"))?
    }
    
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_ping_pong() -> Result<()> {
        // Wrap the entire test in a timeout
        tokio::time::timeout(Duration::from_secs(10), async {
            // Initialize tracing for tests
            let _ = tracing_subscriber::fmt::try_init();
            
            info!("Starting ping/pong test");
            
            // Create and start test server
            let server = TestServerBuilder::new()
                .with_port(0)  // Random available port
                .with_mock_auth()  // Accept any token
                .build()
                .await?;
            
            info!("Test server built, connecting client to {}", server.addr);
            
            // Connect a client
            let mut client = server.connect_client().await?;
            
            info!("Client connected, sending ping");
            
            // Send ping
            client.send_ping().await?;
            
            info!("Ping sent, expecting pong");
            
            // Expect pong response
            client.expect_pong().await?;
            
            info!("Pong received, test successful");
            
            // Cleanup
            client.disconnect().await?;
            server.shutdown().await?;
            
            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 10 seconds"))?
    }
}