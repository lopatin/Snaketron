use anyhow::{Context, Result};
use std::sync::Arc;
use std::time::Duration;
use chrono::{DateTime, Utc};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;
use tracing::{info, error, warn, trace};

use crate::{
    ws_server::{register_server, run_websocket_server, JwtVerifier},
    game_manager::GameManager,
    game_broker::{GameMessageBroker, GameBroker},
    matchmaking::run_matchmaking_loop,
    game_cleanup::run_cleanup_service,
    game_discovery::{run_game_discovery_loop, run_game_discovery_with_raft},
    game_executor::run_game_executor,
    player_connections::PlayerConnectionManager,
    grpc_server::run_game_relay_server,
    service_manager::ServiceManager,
    replica_manager::ReplicaManager,
    authority_transfer::AuthorityTransferManager,
    raft::{RaftNode},
    learner_join::LearnerJoinProtocol,
};
use crate::ws_server::discover_peers;

/// Configuration for a game server instance
pub struct GameServerConfig {
    /// Database connection pool
    pub db_pool: PgPool,
    /// WebSocket server address (e.g., "127.0.0.1:8080")
    pub ws_addr: String,
    /// gRPC server address for game relay (e.g., "127.0.0.1:50051")
    pub grpc_addr: String,
    /// Region identifier for the server
    pub region: String,
    /// JWT verifier for authentication
    pub jwt_verifier: Arc<dyn JwtVerifier>,
}

/// A complete game server instance with all components
pub struct GameServer {
    /// Unique server ID in the database
    pub server_id: u64,
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
    /// Raft consensus node
    raft_node: Option<Arc<RaftNode>>,
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
        } = config;

        // Register server in database
        info!("Registering server in database for region: {}", region);
        let server_id: u64 = register_server(&db_pool, &region).await
            .context("Failed to register server")?;
        info!("Server registered with ID: {}", server_id);
        
        // Create cancellation token for graceful shutdown
        let cancellation_token = CancellationToken::new();
        let mut handles = Vec::new();

        // Start the heartbeat loop
        handles.push(tokio::spawn(async move {
            let _ = run_heartbeat_loop(
                db_pool.clone(), 
                server_id.clone(), 
                cancellation_token.clone()
            ).await;
        }));
       
        // Initialize Raft node
        info!("Initializing Raft consensus node");
        let raft_peers: Vec<String> = discover_peers(&db_pool, &region).await
            .context("Failed to discover Raft peers")?
            .map(|(_, addr)| addr);
        
        let mut join_as_learner = false;
        let raft = if raft_peers.is_empty() {
            info!("Starting as first node in cluster");
            Arc::new(
                RaftNode::new(
                    server_id,
                    vec![server_id],
                ).await.context("Failed to create Raft node")?
            )
        } else {
            // Join existing cluster as learner
            info!("Joining existing cluster as learner");
            let raft_node = Arc::new(
                RaftNode::new(
                    server_id,
                    vec![], // Start with empty membership, will be added as learner
                ).await.context("Failed to create Raft node")?
            );
            join_as_learner = true;
            raft_node
        };

        // Start gRPC server
        info!("Starting gRPC server on {}", grpc_addr);
        handles.push(tokio::spawn(async move {
            if let Err(e) = run_game_relay_server(
                &grpc_addr.clone(),
                raft.clone(),
                server_id,
                cancellation_token.clone()
            ).await {
                error!("Game relay gRPC server error: {}", e);
            }
        }));
        
        if join_as_learner {
            // Execute join protocol in the background
            let join_protocol = LearnerJoinProtocol::new(
                server_id.to_string(),
                grpc_addr,
                raft.clone(),
            );
            
            // Give the gRPC server time to start
            tokio::time::sleep(Duration::from_secs(2)).await;

            join_protocol
                .execute_join(raft_peers)
                .await
                .context("Failed to execute learner join protocol")?;
        }

        // Start WebSocket server
        let ws_pool = db_pool.clone();
        let ws_token = cancellation_token.clone();
        let ws_addr_clone = ws_addr.clone();
        let ws_jwt_verifier = jwt_verifier.clone();
        handles.push(tokio::spawn(async move {
            let _ = run_websocket_server(
                &ws_addr_clone,
                raft.clone(),
                ws_pool,
                ws_token,
                ws_jwt_verifier,
            ).await;
        }));

        // Start the matchmaking service
        info!("Starting matchmaking service");
        handles.push(tokio::spawn(async move {
            run_matchmaking_loop(
                db_pool.clone(),
                raft.clone(),
                server_id.clone(),
                cancellation_token.clone(),
            ).await;
        }));

        // Start game the execution loop
        info!("Starting game executor service");
        handles.push(tokio::spawn(async move {
            if let Err(e) = run_game_executor(
                server_id,
                raft.clone(),
                cancellation_token.clone(),
            ).await {
                error!("Game executor service error: {}", e);
            }
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
            raft_node,
        })
    }

    /// Get the server's unique ID
    pub fn id(&self) -> u64 {
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
    pub async fn shutdown(mut self) -> Result<()> {
        info!("Starting graceful shutdown of game server {}", self.server_id);
        
        // Step 1: Stop accepting new games
        info!("Updating server status to 'draining'");
        sqlx::query("UPDATE servers SET status = 'draining' WHERE server_id = $1")
            .bind(self.server_id)
            .execute(&self.db_pool)
            .await
            .context("Failed to update server status")?;

        // Step 2: Notify all WebSocket clients
        info!("Broadcasting shutdown notice to all WebSocket clients");
        if let (Some(service_manager), Some(authority_transfer)) = (&self.service_manager, &self.authority_transfer) {
            // Get list of games we're hosting
            let hosted_games = sqlx::query_as::<_, (i32,)>(
                r#"
                SELECT id 
                FROM games 
                WHERE host_server_id = $1 
                AND status = 'active'
                "#
            )
            .bind(&self.server_id.to_string())
            .fetch_all(&self.db_pool)
            .await
            .context("Failed to query hosted games")?;
            
            let game_ids: Vec<u32> = hosted_games.into_iter().map(|r| r.0 as u32).collect();
            
            // Broadcast shutdown notification to all servers
            service_manager.broadcast_shutdown(30000, game_ids.clone()).await?;
            
            // Send shutdown notices to connected clients
            self.player_connections.broadcast_shutdown_notice(30).await;
            
            // Step 3: Transfer games to other servers
            info!("Transferring {} games to other servers", game_ids.len());
            authority_transfer.transfer_all_games().await?;
            
            // Wait for transfers to complete
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
        
        // Step 4: Signal all services to stop
        info!("Stopping all services");
        self.cancellation_token.cancel();

        // Step 5: Wait for all services to complete
        for handle in &self.handles {
            match tokio::time::timeout(Duration::from_secs(5), handle).await {
                Ok(Ok(())) => {},
                Ok(Err(e)) => error!("Service panicked during shutdown: {:?}", e),
                Err(_) => error!("Service shutdown timed out"),
            }
        }

        // Update server status to offline
        sqlx::query("UPDATE servers SET status = 'offline' WHERE server_id = $1")
            .bind(self.server_id)
            .execute(&self.db_pool)
            .await
            .context("Failed to update server status to offline")?;

        info!("Game server {} shut down gracefully", self.server_id);
        Ok(())
    }
}

/// Helper function to start a game server for testing
/// Creates a database pool and determines ports automatically
pub async fn start_test_server(
    db_url: &str,
    jwt_verifier: Arc<dyn JwtVerifier>,
) -> Result<GameServer> {
    start_test_server_with_grpc(db_url, jwt_verifier, false).await
}

/// Helper function to start a game server for testing with optional gRPC
pub async fn start_test_server_with_grpc(
    db_url: &str,
    jwt_verifier: Arc<dyn JwtVerifier>,
    enable_grpc: bool,
) -> Result<GameServer> {
    // Create database pool
    let db_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(db_url)
        .await
        .context("Failed to connect to database")?;

    // Get available ports
    let ws_port = get_available_port();
    let ws_addr = format!("127.0.0.1:{}", ws_port);

    // Enable gRPC if requested
    let grpc_addr = format!("127.0.0.1:{}", get_available_port());

    let config = GameServerConfig {
        db_pool,
        ws_addr,
        grpc_addr,
        region: "test-region".to_string(),
        jwt_verifier,
    };

    GameServer::start(config).await
}

/// Get an available port by binding to port 0
pub fn get_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    std::thread::sleep(std::time::Duration::from_millis(10));
    port
}

/// Run a loop to update last_heartbeat in the database
pub async fn run_heartbeat_loop(
    pool: PgPool,
    server_id: u64,
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
