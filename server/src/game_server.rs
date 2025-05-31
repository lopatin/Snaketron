use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;
use tracing::{info, error, warn};

use crate::{
    ws_server::{register_server, run_heartbeat_loop, run_websocket_server, JwtVerifier},
    game_manager::GameManager,
    game_broker::{GameMessageBroker, GameBroker},
    matchmaking::run_matchmaking_loop,
    game_cleanup::run_cleanup_service,
    game_discovery::run_game_discovery_loop,
    player_connections::PlayerConnectionManager,
    grpc_server::run_game_relay_server,
    service_manager::ServiceManager,
    replica_manager::ReplicaManager,
    authority_transfer::AuthorityTransferManager,
    raft::{RaftNode, RaftNodeId},
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
    /// Initial Raft cluster members (empty for first node)
    pub raft_peers: Vec<String>,
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
    /// Service manager for cluster topology
    service_manager: Option<Arc<ServiceManager>>,
    /// Replica manager for game state replication
    replica_manager: Option<Arc<ReplicaManager>>,
    /// Authority transfer manager
    authority_transfer: Option<Arc<AuthorityTransferManager>>,
    /// Player connection manager
    player_connections: Arc<PlayerConnectionManager>,
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
            raft_peers,
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
        info!("Creating game broker");
        let broker: Arc<dyn GameMessageBroker> = Arc::new(GameBroker::new(
            db_pool.clone(),
            server_id.to_string(),
        ));

        // Create player connection manager
        let player_connections = Arc::new(PlayerConnectionManager::new());

        // Create service manager for cluster topology
        let service_manager = Arc::new(ServiceManager::new(
            server_id.to_string(),
            db_pool.clone(),
            cancellation_token.clone(),
        ));

        // Create replica manager for game state replication
        let replica_manager = Arc::new(ReplicaManager::new(
            server_id.to_string(),
            db_pool.clone(),
            cancellation_token.clone(),
        ));

        // Create games manager with replica manager
        let games_manager = Arc::new(RwLock::new(
            GameManager::new(broker.clone(), server_id.to_string())
                .with_replica_manager(replica_manager.clone())
        ));

        // Create authority transfer manager
        let authority_transfer = Arc::new(AuthorityTransferManager::new(
            server_id.to_string(),
            db_pool.clone(),
            service_manager.clone(),
            replica_manager.clone(),
            games_manager.clone(),
        ));
        
        // Initialize Raft node
        let raft_node = if grpc_addr.is_some() {
            info!("Initializing Raft consensus node");
            
            // Convert peer addresses to RaftNodeIds
            let initial_members: Vec<RaftNodeId> = if raft_peers.is_empty() {
                // This is the first node
                vec![RaftNodeId(server_id.to_string())]
            } else {
                // Join existing cluster
                raft_peers.into_iter()
                    .map(|peer| RaftNodeId(peer))
                    .collect()
            };
            
            let raft_node = Arc::new(
                RaftNode::new(
                    server_id.to_string(),
                    games_manager.clone(),
                    replica_manager.clone(),
                    initial_members,
                ).await.context("Failed to create Raft node")?
            );
            
            // async-raft manages its own internal timers, no tick needed
            
            Some(raft_node)
        } else {
            warn!("Raft not initialized: gRPC address not configured");
            None
        };

        // Start gRPC server if configured
        if let Some(grpc_addr_str) = &grpc_addr {
            info!("Starting gRPC server on {}", grpc_addr_str);
            
            let grpc_broker = broker.clone();
            let grpc_player_connections = player_connections.clone();
            let grpc_token = cancellation_token.clone();
            let grpc_addr_clone = grpc_addr_str.clone();
            
            handles.push(tokio::spawn(async move {
                if let Err(e) = run_game_relay_server(&grpc_addr_clone, grpc_broker, grpc_player_connections, grpc_token).await {
                    error!("Game relay gRPC server error: {}", e);
                }
            }));
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
        let matchmaking_token = cancellation_token.clone();
        handles.push(tokio::spawn(async move {
            run_matchmaking_loop(
                matchmaking_pool,
                matchmaking_server_id,
                matchmaking_token,
            ).await;
        }));

        // Start game discovery service
        info!("Starting game discovery service");
        let discovery_pool = db_pool.clone();
        let discovery_server_id = server_id.clone();
        let discovery_games_manager = games_manager.clone();
        let discovery_player_connections = player_connections.clone();
        let discovery_token = cancellation_token.clone();
        handles.push(tokio::spawn(async move {
            run_game_discovery_loop(
                discovery_pool,
                discovery_server_id,
                discovery_games_manager,
                discovery_player_connections,
                discovery_token,
            ).await;
        }));

        // Start cleanup service
        let cleanup_pool = db_pool.clone();
        let cleanup_token = cancellation_token.clone();
        handles.push(tokio::spawn(async move {
            let _ = run_cleanup_service(cleanup_pool, cleanup_token).await;
        }));

        // Start service manager
        info!("Starting service manager for cluster topology");
        let service_manager_clone = service_manager.clone();
        handles.push(tokio::spawn(async move {
            if let Err(e) = service_manager_clone.start().await {
                error!("Service manager error: {}", e);
            }
        }));

        // Start replica manager
        info!("Starting replica manager for game state replication");
        let replica_manager_clone = replica_manager.clone();
        handles.push(tokio::spawn(async move {
            if let Err(e) = replica_manager_clone.start().await {
                error!("Replica manager error: {}", e);
            }
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
            service_manager: Some(service_manager),
            replica_manager: Some(replica_manager),
            authority_transfer: Some(authority_transfer),
            player_connections,
            raft_node,
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
    pub async fn shutdown(mut self) -> Result<()> {
        // Perform graceful shutdown steps
        if let Err(e) = self.perform_graceful_shutdown().await {
            error!("Error during graceful shutdown: {}", e);
        }
        
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

    /// Perform graceful shutdown with game migration
    async fn perform_graceful_shutdown(&self) -> Result<()> {
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
            // Note: We can't consume the handles here since we don't own self
            // In a real implementation, we'd need to refactor this
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
    let grpc_addr = if enable_grpc {
        let grpc_port = get_available_port();
        Some(format!("127.0.0.1:{}", grpc_port))
    } else {
        None
    };

    let config = GameServerConfig {
        db_pool,
        ws_addr,
        grpc_addr,
        region: "test-region".to_string(),
        jwt_verifier,
        raft_peers: vec![], // Single node for tests
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