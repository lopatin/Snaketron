use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;
use tracing::{info, error};

use crate::{
    ws_server::{register_server, run_heartbeat_loop, run_websocket_server, JwtVerifier},
    game_manager::GameManager,
    game_broker::{GameMessageBroker, LocalBroker, DistributedBroker},
    matchmaking::run_matchmaking_loop,
    game_cleanup::run_cleanup_service,
    player_connections::PlayerConnectionManager,
    grpc_server::run_game_relay_server,
};


/// Configuration for a game server instance
pub struct GameServerConfig {
    /// Database connection pool
    pub db_pool: PgPool,
    /// WebSocket server address (e.g., "127.0.0.1:8080")
    pub ws_addr: String,
    /// gRPC server address for game relay (e.g., "127.0.0.1:50051")
    pub grpc_addr: Option<String>,
    /// Region identifier for the server
    pub region: String,
    /// JWT verifier for authentication
    pub jwt_verifier: Arc<dyn JwtVerifier>,
    /// Whether to use distributed broker (requires gRPC)
    pub use_distributed_broker: bool,
}

/// A complete game server instance with all components
pub struct GameServer {
    /// Unique server ID in the database
    pub server_id: Uuid,
    /// WebSocket server address
    pub ws_addr: String,
    /// gRPC server address (if enabled)
    pub grpc_addr: Option<String>,
    /// Database pool
    db_pool: PgPool,
    /// Cancellation token for graceful shutdown
    cancellation_token: CancellationToken,
    /// Handles for all spawned tasks
    handles: Vec<JoinHandle<()>>,
}

impl GameServer {
    /// Create and start a new game server instance
    pub async fn start(config: GameServerConfig) -> Result<Self> {
        let GameServerConfig {
            db_pool,
            ws_addr,
            grpc_addr,
            region,
            jwt_verifier,
            use_distributed_broker,
        } = config;

        // Register server in database
        info!("Registering server in database for region: {}", region);
        let server_id = register_server(&db_pool, &region).await
            .context("Failed to register server")?;
        info!("Server registered with ID: {}", server_id);

        // Create cancellation token for graceful shutdown
        let cancellation_token = CancellationToken::new();
        let mut handles = Vec::new();

        // Start heartbeat loop
        let heartbeat_pool = db_pool.clone();
        let heartbeat_server_id = server_id.clone();
        let heartbeat_token = cancellation_token.clone();
        handles.push(tokio::spawn(async move {
            let _ = run_heartbeat_loop(heartbeat_pool, heartbeat_server_id, heartbeat_token).await;
        }));

        // Create game message broker
        let broker: Arc<dyn GameMessageBroker> = if use_distributed_broker && grpc_addr.is_some() {
            info!("Creating distributed broker");
            Arc::new(DistributedBroker::new(
                db_pool.clone(),
                server_id.to_string(),
            ))
        } else {
            info!("Creating local broker");
            Arc::new(LocalBroker::new())
        };

        // Create games manager
        let games_manager = Arc::new(Mutex::new(GameManager::new_with_broker(broker.clone())));

        // Create player connection manager
        let player_connections = Arc::new(PlayerConnectionManager::new());

        // Start gRPC server if configured
        if let Some(grpc_addr_str) = &grpc_addr {
            if use_distributed_broker {
                info!("Starting gRPC server on {}", grpc_addr_str);
                
                let grpc_broker = broker.clone();
                let grpc_token = cancellation_token.clone();
                let grpc_addr_clone = grpc_addr_str.clone();
                
                handles.push(tokio::spawn(async move {
                    if let Err(e) = run_game_relay_server(&grpc_addr_clone, grpc_broker, grpc_token).await {
                        error!("Game relay gRPC server error: {}", e);
                    }
                }));
            }
        }

        // Start WebSocket server
        let ws_games_manager = games_manager.clone();
        let ws_pool = db_pool.clone();
        let ws_token = cancellation_token.clone();
        let ws_addr_clone = ws_addr.clone();
        let ws_player_connections = player_connections.clone();
        let ws_jwt_verifier = jwt_verifier.clone();
        handles.push(tokio::spawn(async move {
            let _ = run_websocket_server(
                &ws_addr_clone,
                ws_games_manager,
                ws_pool,
                ws_token,
                ws_jwt_verifier,
                ws_player_connections,
            ).await;
        }));

        // Start matchmaking service
        info!("Starting matchmaking service");
        let matchmaking_pool = db_pool.clone();
        let matchmaking_server_id = server_id.clone();
        let matchmaking_games_manager = games_manager.clone();
        let matchmaking_player_connections = player_connections.clone();
        let matchmaking_token = cancellation_token.clone();
        handles.push(tokio::spawn(async move {
            run_matchmaking_loop(
                matchmaking_pool,
                matchmaking_server_id,
                matchmaking_games_manager,
                matchmaking_player_connections,
                matchmaking_token,
            ).await;
        }));

        // Start cleanup service
        let cleanup_pool = db_pool.clone();
        let cleanup_token = cancellation_token.clone();
        handles.push(tokio::spawn(async move {
            let _ = run_cleanup_service(cleanup_pool, cleanup_token).await;
        }));

        // Wait a moment for all services to start
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        info!("Game server {} started successfully", server_id);

        Ok(Self {
            server_id,
            ws_addr,
            grpc_addr,
            db_pool,
            cancellation_token,
            handles,
        })
    }

    /// Get the server's unique ID
    pub fn id(&self) -> Uuid {
        self.server_id
    }

    /// Get the WebSocket server address
    pub fn ws_addr(&self) -> &str {
        &self.ws_addr
    }

    /// Get the gRPC server address (if enabled)
    pub fn grpc_addr(&self) -> Option<&str> {
        self.grpc_addr.as_deref()
    }

    /// Get a reference to the database pool
    pub fn db_pool(&self) -> &PgPool {
        &self.db_pool
    }

    /// Shutdown the server gracefully
    pub async fn shutdown(self) -> Result<()> {
        info!("Shutting down game server {}", self.server_id);
        
        // Signal all services to stop
        self.cancellation_token.cancel();

        // Wait for all services to complete
        for handle in self.handles {
            match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                handle
            ).await {
                Ok(Ok(())) => {},
                Ok(Err(e)) => error!("Service panicked during shutdown: {:?}", e),
                Err(_) => error!("Service shutdown timed out"),
            }
        }

        info!("Game server {} shut down successfully", self.server_id);
        Ok(())
    }
}

/// Builder for creating GameServer instances with custom configuration
pub struct GameServerBuilder {
    db_url: Option<String>,
    ws_port: Option<u16>,
    grpc_port: Option<u16>,
    region: String,
    jwt_verifier: Option<Arc<dyn JwtVerifier>>,
    use_distributed_broker: bool,
}

impl GameServerBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self {
            db_url: None,
            ws_port: None,
            grpc_port: None,
            region: "default".to_string(),
            jwt_verifier: None,
            use_distributed_broker: false,
        }
    }

    /// Set the database URL
    pub fn with_db_url(mut self, url: String) -> Self {
        self.db_url = Some(url);
        self
    }

    /// Set the WebSocket port (will bind to 127.0.0.1)
    pub fn with_ws_port(mut self, port: u16) -> Self {
        self.ws_port = Some(port);
        self
    }

    /// Set the gRPC port (will bind to 127.0.0.1)
    pub fn with_grpc_port(mut self, port: u16) -> Self {
        self.grpc_port = Some(port);
        self
    }

    /// Set the server region
    pub fn with_region(mut self, region: String) -> Self {
        self.region = region;
        self
    }

    /// Set the JWT verifier
    pub fn with_jwt_verifier(mut self, verifier: Arc<dyn JwtVerifier>) -> Self {
        self.jwt_verifier = Some(verifier);
        self
    }

    /// Enable distributed broker (requires gRPC)
    pub fn with_distributed_broker(mut self, enabled: bool) -> Self {
        self.use_distributed_broker = enabled;
        self
    }

    /// Build and start the game server
    pub async fn build(self) -> Result<GameServer> {
        // Create database pool
        let db_url = self.db_url
            .unwrap_or_else(|| std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://snaketron:snaketron@localhost:5432/snaketron".to_string()));
        
        let db_pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&db_url)
            .await
            .context("Failed to connect to database")?;

        // Determine WebSocket address
        let ws_port = self.ws_port.unwrap_or_else(|| get_available_port());
        let ws_addr = format!("127.0.0.1:{}", ws_port);

        // Determine gRPC address if needed
        let grpc_addr = if self.use_distributed_broker || self.grpc_port.is_some() {
            let grpc_port = self.grpc_port.unwrap_or_else(|| get_available_port());
            Some(format!("127.0.0.1:{}", grpc_port))
        } else {
            None
        };

        // Get JWT verifier
        let jwt_verifier = self.jwt_verifier
            .expect("JWT verifier must be provided");

        let config = GameServerConfig {
            db_pool,
            ws_addr,
            grpc_addr,
            region: self.region,
            jwt_verifier,
            use_distributed_broker: self.use_distributed_broker,
        };

        GameServer::start(config).await
    }
}

/// Get an available port by binding to port 0
fn get_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    std::thread::sleep(std::time::Duration::from_millis(10));
    port
}