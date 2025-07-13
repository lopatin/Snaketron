use anyhow::{Context, Result};
use std::sync::Arc;
use std::time::Duration;
use chrono::{DateTime, Utc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tracing::{info, error, warn, trace, debug};

use crate::{
    ws_server::{register_server, run_websocket_server, JwtVerifier},
    game_executor::{run_game_executor, StreamEvent},
    grpc_server::run_game_relay_server,
    matchmaking::run_matchmaking_loop,
    learner_join::LearnerJoinProtocol,
    replay::ReplayListener,
    cluster_singleton::ClusterSingleton,
    replication::ReplicationManager,
};
use crate::ws_server::discover_peers;
use std::path::PathBuf;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use crate::game_executor::PARTITION_COUNT;

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
    /// Optional directory for saving game replays
    pub replay_dir: Option<PathBuf>,
    /// Redis URL for cluster singleton coordination (e.g., "redis://127.0.0.1:6379")
    pub redis_url: String,
}

/// A complete game server instance with all components
pub struct GameServer {
    /// Unique server ID in the database
    pub server_id: u64,
    /// WebSocket server address
    pub ws_addr: String,
    /// gRPC server address (if enabled)
    pub grpc_addr: String,
    /// Database pool
    db_pool: PgPool,
    /// Cancellation token for graceful shutdown
    cancellation_token: CancellationToken,
    /// Handles for all spawned tasks
    handles: Vec<JoinHandle<()>>,
    /// Optional replay listener
    // replay_listener: Option<Arc<ReplayListener>>,
    /// Replication manager for game state
    replication_manager: Option<Arc<ReplicationManager>>,
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
            replay_dir,
            redis_url,
        } = config;

        // Register server in database
        info!("Registering server in database for region: {}", region);
        let server_id: u64 = register_server(&db_pool, &grpc_addr, &region).await
            .context("Failed to register server")?;
        info!("Server registered with ID: {}", server_id);
        
        // Create cancellation token for graceful shutdown
        let cancellation_token = CancellationToken::new();
        let mut handles = Vec::new();

        // Redis connection manager
        let client = redis::Client::open(redis_url.as_str())
            .context("Failed to create Redis client")?;
        let redis_conn = ConnectionManager::new(client).await
            .context("Failed to create Redis connection manager")?;

        // WebSocket server will be started after ReplicationManager is created

        // Start the matchmaking service
        info!("Starting matchmaking service");
        let match_pool = db_pool.clone();
        let match_redis_url = redis_url.clone();
        let match_token = cancellation_token.clone();
        handles.push(tokio::spawn(async move {
            if let Err(e) = run_matchmaking_loop(
                match_pool,
                match_redis_url,
                server_id,
                match_token,
            ).await {
                error!("Matchmaking loop error: {}", e);
            }
        }));

        // Start replication manager for all partitions BEFORE game executors
        info!("Starting replication manager for game state replication");
        let replication_partitions: Vec<u32> = (1..=10).collect();
        let replication_manager = Arc::new(
            ReplicationManager::new(
                replication_partitions,
                cancellation_token.clone(),
                redis_conn.clone(),
            ).await.context("Failed to create replication manager")?
        );
        
        // Wait for replication to be ready
        let replication_start = std::time::Instant::now();
        loop {
            if replication_manager.is_ready().await {
                info!("Replication manager is ready after {:?}", replication_start.elapsed());
                break;
            }
            if replication_start.elapsed() > Duration::from_secs(30) {
                warn!("Replication manager taking longer than expected to catch up");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        
        // Start WebSocket server after replication manager is ready
        info!("Starting WebSocket server");
        let ws_pool = db_pool.clone();
        let ws_token = cancellation_token.clone();
        let ws_addr_clone = ws_addr.clone();
        let ws_jwt_verifier = jwt_verifier.clone();
        let ws_redis_url = redis_url.clone();
        let ws_replication_manager = replication_manager.clone();
        handles.push(tokio::spawn(async move {
            let _ = run_websocket_server(
                &ws_addr_clone,
                ws_pool,
                ws_redis_url,
                ws_token,
                ws_jwt_verifier,
                ws_replication_manager,
            ).await;
        }));

        // Start game executors for each partition as cluster singletons
        // This provides automatic failover - if one server goes down, another will
        // automatically take over its partitions
        info!("Starting game executor services for 10 partitions");
        for partition_id in 0..PARTITION_COUNT {
            let exec_token = cancellation_token.clone();
            let exec_redis_url = redis_url.clone();
            let exec_replication_manager = replication_manager.clone();
            
            handles.push(tokio::spawn(async move {
                info!("Starting cluster singleton for game executor partition {}", partition_id);
                
                let singleton = match ClusterSingleton::new(
                    &exec_redis_url,
                    server_id,
                    format!("snaketron:game-executor:partition-{}", partition_id),
                    Duration::from_secs(1),
                    exec_token.clone(),
                ).await {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Failed to create cluster singleton for partition {}: {}", partition_id, e);
                        return;
                    }
                };
                
                // Service that runs the game executor for this partition
                let service = move |token: CancellationToken| {
                    let redis_url_clone = exec_redis_url.clone();
                    let replication_manager_clone = exec_replication_manager.clone();
                    Box::pin(async move {
                        info!("Game executor for partition {} is now active", partition_id);
                        
                        if let Err(e) = run_game_executor(
                            server_id,
                            partition_id,
                            redis_url_clone,
                            replication_manager_clone,
                            token,
                        ).await {
                            error!("Game executor service error for partition {}: {}", partition_id, e);
                        }
                        
                        Ok::<(), anyhow::Error>(())
                    }) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
                };
                
                if let Err(e) = singleton.run(service).await {
                    error!("Cluster singleton error for game executor partition {}: {}", partition_id, e);
                }
            }));
        }

        // Start replay listener if configured
        // let replay_listener = if let Some(replay_dir) = replay_dir {
        //     info!("Starting replay listener, saving to {:?}", replay_dir);
        //     let listener = Arc::new(ReplayListener::new(replay_dir));
        //     let replay_raft = raft.clone();
        //     let replay_listener_clone = listener.clone();
        //     handles.push(tokio::spawn(async move {
        //         let rx = replay_raft.subscribe_state_events();
        //         replay_listener_clone.subscribe_to_raft(rx).await;
        //     }));
        //     Some(listener)
        // } else {
        //     info!("Replay recording disabled (no replay directory configured)");
        //     None
        // };

        // Redis cluster singleton
        info!("Starting cluster singleton service with Redis");
        let lock_token = cancellation_token.clone();
        let lock_region = region.clone();
        handles.push(tokio::spawn(async move {
            // Example: Run a simple service that logs when it holds the lock
            let cluster_singleton = match ClusterSingleton::new(
                &redis_url,
                server_id,
                format!("snaketron:singleton:{}", lock_region),
                Duration::from_secs(2),
                lock_token,
            ).await {
                Ok(e) => e,
                Err(e) => {
                    error!("Failed to create cluster singleton: {}", e);
                    return;
                }
            };
            
            // Example service that runs as a singleton across the cluster
            let service = |token: CancellationToken| Box::pin(async move {
                info!("This server is now running the singleton service!");
                
                // Example of how to use the cancellation token in your service
                loop {
                    tokio::select! {
                        _ = token.cancelled() => {
                            info!("Singleton service received cancellation signal");
                            break;
                        }
                        _ = tokio::time::sleep(Duration::from_secs(10)) => {
                            // Your actual service logic would go here
                            debug!("Singleton service is running...");
                        }
                    }
                }
                
                Ok::<(), anyhow::Error>(())
            }) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>;
            
            if let Err(e) = cluster_singleton.run(service).await {
                error!("Cluster singleton error: {}", e);
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
            // replay_listener,
            replication_manager: Some(replication_manager),
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
        if self.grpc_addr.is_empty() {
            None
        } else {
            Some(&self.grpc_addr)
        }
    }

    /// Get a reference to the database pool
    pub fn db_pool(&self) -> &PgPool {
        &self.db_pool
    }

    /// Get a reference to the replay listener
    // pub fn replay_listener(&self) -> Option<&Arc<ReplayListener>> {
    //     self.replay_listener.as_ref()
    // }
    
    /// Get the replication manager
    pub fn replication_manager(&self) -> Option<&Arc<ReplicationManager>> {
        self.replication_manager.as_ref()
    }

    /// Shutdown the server gracefully
    pub async fn shutdown(mut self) -> Result<()> {
        info!("Starting graceful shutdown of game server {}", self.server_id);
        
        // Step 1: Stop accepting new games
        info!("Updating server status to 'draining'");
        sqlx::query("UPDATE servers SET status = 'draining' WHERE id = $1")
            .bind(self.server_id as i32)
            .execute(&self.db_pool)
            .await
            .context("Failed to update server status")?;
        
        // Signal all services to stop
        info!("Stopping all services");
        self.cancellation_token.cancel();

        // Step 5: Wait for all services to complete
        while let Some(handle) = self.handles.pop() {
            match tokio::time::timeout(Duration::from_secs(5), handle).await {
                Ok(Ok(())) => {},
                Ok(Err(e)) => error!("Service panicked during shutdown: {:?}", e),
                Err(_) => error!("Service shutdown timed out"),
            }
        }

        // Update server status to offline
        sqlx::query("UPDATE servers SET status = 'offline' WHERE id = $1")
            .bind(self.server_id as i32)
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
    _enable_grpc: bool,
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

    // Use centralized replay directory for tests
    let test_name = format!("test_{}", uuid::Uuid::new_v4());
    let replay_path = crate::replay::directory::get_test_replay_directory(&test_name);
    std::fs::create_dir_all(&replay_path).ok();
    let replay_dir = Some(replay_path);
    
    let config = GameServerConfig {
        db_pool,
        ws_addr,
        grpc_addr,
        region: "test-region".to_string(),
        jwt_verifier,
        replay_dir,
        redis_url: "redis://127.0.0.1:6379".to_string(),
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
                    .bind(server_id as i32)
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
