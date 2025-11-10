use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use std::env;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

use crate::api::jwt::JwtManager;
use crate::game_executor::PARTITION_COUNT;
use crate::game_executor::StreamEvent;
use crate::http_server::run_http_server;
use crate::lobby_manager::LobbyManager;
use crate::matchmaking_manager::MatchmakingManager;
use crate::pubsub_manager::PubSubManager;
use crate::redis_utils::create_connection_manager;
use crate::region_cache::RegionCache;
use crate::ws_server::discover_peers;
use crate::{
    cluster_singleton::ClusterSingleton, db::Database, game_executor::run_game_executor,
    grpc_server::run_game_relay_server, matchmaking::run_matchmaking_loop, redis_keys::RedisKeys,
    redis_utils, replay::ReplayListener, replication::ReplicationManager, ws_server::JwtVerifier,
};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client};
use std::path::PathBuf;

/// Configuration for a game server instance
pub struct GameServerConfig {
    /// Database connection
    pub db: Arc<dyn Database>,
    /// HTTP server address (e.g., "127.0.0.1:8080")
    pub http_addr: String,
    /// gRPC server address for game relay (e.g., "127.0.0.1:50051")
    pub grpc_addr: String,
    /// Region identifier for the server
    pub region: String,
    /// HTTP origin for client connections (e.g., "http://localhost:8080")
    pub origin: String,
    /// WebSocket URL for client connections (e.g., "ws://localhost:8080/ws")
    pub ws_url: String,
    /// JWT manager
    pub jwt_manager: Arc<JwtManager>,
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
    /// HTTP server address
    pub http_addr: String,
    /// gRPC server address (if enabled)
    pub grpc_addr: String,
    /// Database connection
    db: Arc<dyn Database>,
    /// Cancellation token for graceful shutdown
    cancellation_token: CancellationToken,
    /// Handles for all spawned tasks
    handles: Vec<JoinHandle<()>>,
    /// Optional replay listener
    // replay_listener: Option<Arc<ReplayListener>>,
    /// Replication manager for game state
    replication_manager: Arc<ReplicationManager>,
}

impl GameServer {
    /// Get the HTTP server address
    pub fn http_addr(&self) -> &str {
        &self.http_addr
    }

    /// Get the server ID
    pub fn id(&self) -> u64 {
        self.server_id
    }

    /// Get the gRPC server address (if enabled)
    pub fn grpc_addr(&self) -> Option<&str> {
        if self.grpc_addr.is_empty() {
            None
        } else {
            Some(&self.grpc_addr)
        }
    }
    /// Create and start a new game server instance
    pub async fn start(config: GameServerConfig) -> Result<Self> {
        let GameServerConfig {
            db,
            http_addr,
            grpc_addr,
            region,
            origin,
            ws_url,
            jwt_manager,
            jwt_verifier,
            replay_dir,
            redis_url,
        } = config;

        // Register server in database
        info!("Registering server in database for region: {}", region);
        let server_id = db
            .register_server(&grpc_addr, &region, &origin, &ws_url)
            .await
            .context("Failed to register server")? as u64;
        info!("Server registered with ID: {}", server_id);

        // Create cancellation token for graceful shutdown
        let cancellation_token = CancellationToken::new();
        let mut handles = Vec::new();

        // Start heartbeat loop to keep server registration alive
        let heartbeat_db = db.clone();
        let heartbeat_token = cancellation_token.clone();
        handles.push(tokio::spawn(async move {
            run_heartbeat_loop(heartbeat_db, server_id, heartbeat_token).await;
        }));

        // Create the broadcast channel for Redis Pub/Sub
        let (pubsub_tx, pubsub_rx) = tokio::sync::broadcast::channel(5000);
        // Drop the default receiver to avoid filling up the channel
        drop(pubsub_rx);

        // Ensure RESP3 protocol is enabled for push notifications
        let redis_url = if !redis_url.contains("protocol=resp3") && !redis_url.contains("protocol=3") {
            let separator = if redis_url.contains('?') { "&" } else { "?" };
            format!("{}{}protocol=resp3", redis_url, separator)
        } else {
            redis_url
        };
        info!("Using Redis URL: {}", redis_url);

        // Create the Redis client and connection manager
        let redis_client =
            Client::open(redis_url.clone()).context("Failed to create Redis client")?;
        let redis = create_connection_manager(redis_client, pubsub_tx.clone()).await?;
        info!("Redis connection manager created successfully");

        // Create the PubsubManager
        let pubsub_manager = Arc::new(PubSubManager::new(
            redis.clone(),
            pubsub_tx.clone(),
            cancellation_token.clone(),
        ));

        // Create the LobbyManager
        let lobby_manager = Arc::new(LobbyManager::new(redis.clone(), db.clone()));

        // Create the matchmaking manager
        let matchmaking_manager = Arc::new(tokio::sync::Mutex::new(
            MatchmakingManager::new(redis.clone())
                .context("Failed to create matchmaking manager")?,
        ));

        // Create RegionCache for dynamic region discovery
        let aws_config = aws_config::load_from_env().await;
        let dynamodb_client = aws_sdk_dynamodb::Client::new(&aws_config);
        let table_prefix =
            env::var("DYNAMODB_TABLE_PREFIX").unwrap_or_else(|_| "snaketron".to_string());
        let region_cache = Arc::new(RegionCache::new(dynamodb_client, table_prefix));
        region_cache
            .clone()
            .spawn_refresh_task(cancellation_token.clone());
        info!("Region cache refresh task started");

        // Start the matchmaking service
        info!("Starting matchmaking service");
        let match_token = cancellation_token.clone();
        let match_pubsub_manager = (*pubsub_manager).clone();
        let match_matchmaking_manager = matchmaking_manager.clone();
        // TODO: Shouldn't this be a cluster singleton?
        handles.push(tokio::spawn(async move {
            let mm = match_matchmaking_manager.lock().await.clone();
            drop(match_matchmaking_manager); // Drop the lock
            if let Err(e) = run_matchmaking_loop(mm, match_pubsub_manager, match_token).await {
                error!("Matchmaking loop error: {}", e);
            }
        }));

        // Start replication manager for all partitions BEFORE game executors
        info!("Starting replication manager for game state replication");
        let replication_partitions: Vec<u32> = (0..PARTITION_COUNT).collect();
        let replication_manager = Arc::new(
            ReplicationManager::new(
                replication_partitions,
                cancellation_token.clone(),
                &redis_url,
            )
            .await
            .context("Failed to create replication manager")?,
        );

        // Wait for replication to be ready
        let replication_start = std::time::Instant::now();
        loop {
            if replication_manager.is_ready().await {
                info!(
                    "Replication manager is ready after {:?}",
                    replication_start.elapsed()
                );
                break;
            }
            if replication_start.elapsed() > Duration::from_secs(30) {
                warn!("Replication manager taking longer than expected to catch up");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Note: HTTP server will be started separately in main.rs
        // This is because it needs both the replication manager and JWT verifier
        info!("HTTP server will be started externally at {}", http_addr);

        // Start game executors for each partition as cluster singletons
        // This provides automatic failover - if one server goes down, another will
        // automatically take over its partitions
        info!("Starting game executor services for 10 partitions");
        for partition_id in 0..PARTITION_COUNT {
            let exec_token = cancellation_token.clone();
            let exec_redis_clone = redis.clone();
            let exec_db = db.clone();
            let exec_replication_manager = replication_manager.clone();
            let exec_pubsub_manager = pubsub_manager.clone();

            handles.push(tokio::spawn(async move {
                info!(
                    "Starting cluster singleton for game executor partition {}",
                    partition_id
                );

                let singleton = ClusterSingleton::new(
                    exec_redis_clone.clone(),
                    server_id,
                    RedisKeys::partition_executor_lease(partition_id),
                    Duration::from_secs(1),
                    exec_token.clone(),
                );

                // Service that runs the game executor for this partition
                let service = |token: CancellationToken| {
                    // Clone inside the closure so it can be called multiple times (Fn trait)
                    let server_id_clone = server_id;
                    let partition_id_clone = partition_id;
                    let exec_redis_clone = exec_redis_clone.clone();
                    let exec_pubsub_manager_clone = (*exec_pubsub_manager).clone();
                    let exec_db = exec_db.clone();
                    let exec_replication_manager = exec_replication_manager.clone();

                    Box::pin(async move {
                        info!(
                            "Game executor for partition {} is now active",
                            partition_id_clone
                        );

                        if let Err(e) = run_game_executor(
                            server_id_clone,
                            partition_id_clone,
                            exec_redis_clone,
                            exec_pubsub_manager_clone,
                            exec_db,
                            exec_replication_manager,
                            token,
                        )
                        .await
                        {
                            error!(
                                "Game executor service error for partition {}: {}",
                                partition_id_clone, e
                            );
                        }

                        Ok::<(), anyhow::Error>(())
                    })
                        as std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send>>
                };

                if let Err(e) = singleton.run(service).await {
                    error!(
                        "Cluster singleton error for game executor partition {}: {}",
                        partition_id, e
                    );
                }
            }));
        }

        // Wait a moment for all services to start
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Start the unified HTTP server (API + WebSocket)
        let http_addr_clone = http_addr.clone();
        let http_db = db.clone();
        let http_jwt_manager = jwt_manager.clone();
        let http_jwt_verifier = jwt_verifier.clone();
        let http_redis = redis.clone();
        let http_redis_url = redis_url.clone();
        let http_pubsub_manager = pubsub_manager.clone();
        let http_matchmaking_manager = matchmaking_manager.clone();
        let http_replication_manager = replication_manager.clone();
        let http_cancellation_token = cancellation_token.clone();
        let http_server_id = server_id;
        let http_region = region.clone();
        let http_region_cache = region_cache.clone();
        let http_lobby_manager = lobby_manager.clone();
        let http_handle = tokio::spawn(async move {
            if let Err(e) = run_http_server(
                &http_addr_clone,
                http_db,
                http_jwt_manager,
                http_jwt_verifier,
                http_redis,
                http_redis_url,
                http_pubsub_manager,
                http_matchmaking_manager,
                http_replication_manager,
                http_cancellation_token,
                http_server_id,
                http_region,
                http_region_cache,
                http_lobby_manager,
            )
            .await
            {
                error!("HTTP server error: {}", e);
            }
        });

        handles.push(http_handle);

        info!("Game server {} started successfully", server_id);

        Ok(Self {
            server_id,
            http_addr,
            grpc_addr,
            db,
            cancellation_token,
            handles,
            // replay_listener,
            replication_manager,
        })
    }

    /// Get a reference to the database
    pub fn db(&self) -> &Arc<dyn Database> {
        &self.db
    }

    /// Get the cancellation token
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    /// Get the replication manager
    pub fn replication_manager(&self) -> &Arc<ReplicationManager> {
        &self.replication_manager
    }

    /// Shutdown the server gracefully
    pub async fn shutdown(mut self) -> Result<()> {
        info!(
            "Starting graceful shutdown of game server {}",
            self.server_id
        );

        // Step 1: Stop accepting new games
        info!("Updating server status to 'draining'");
        self.db
            .update_server_status(self.server_id as i32, "draining")
            .await
            .context("Failed to update server status")?;

        // Signal all services to stop
        info!("Stopping all services");
        self.cancellation_token.cancel();

        // Step 5: Wait for all services to complete
        while let Some(handle) = self.handles.pop() {
            match tokio::time::timeout(Duration::from_secs(5), handle).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => error!("Service panicked during shutdown: {:?}", e),
                Err(_) => error!("Service shutdown timed out"),
            }
        }

        // Update server status to offline
        self.db
            .update_server_status(self.server_id as i32, "offline")
            .await
            .context("Failed to update server status to offline")?;

        info!("Game server {} shut down gracefully", self.server_id);
        Ok(())
    }
}

/// Helper function to start a game server for testing
/// Creates a database connection and determines ports automatically
pub async fn start_test_server(
    db: Arc<dyn Database>,
    jwt_manager: JwtManager,
    jwt_verifier: Arc<dyn JwtVerifier>,
) -> Result<GameServer> {
    start_test_server_with_grpc(db, jwt_manager, jwt_verifier, false).await
}

/// Helper function to start a game server for testing with optional gRPC
pub async fn start_test_server_with_grpc(
    db: Arc<dyn Database>,
    jwt_manager: JwtManager,
    jwt_verifier: Arc<dyn JwtVerifier>,
    _enable_grpc: bool,
) -> Result<GameServer> {
    // Get available ports
    let http_port = get_available_port();
    let http_addr = format!("127.0.0.1:{}", http_port);

    // Enable gRPC if requested
    let grpc_addr = format!("127.0.0.1:{}", get_available_port());

    // Use centralized replay directory for tests
    let test_name = format!("test_{}", uuid::Uuid::new_v4());
    let replay_path = crate::replay::directory::get_test_replay_directory(&test_name);
    std::fs::create_dir_all(&replay_path).ok();
    let replay_dir = Some(replay_path);

    // Use environment variable if set, otherwise use default
    // Note: protocol=resp3 is required for push notifications support
    let mut redis_url = std::env::var("SNAKETRON_REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    // Ensure RESP3 protocol is enabled for push notifications
    if !redis_url.contains("protocol=resp3") && !redis_url.contains("protocol=3") {
        let separator = if redis_url.contains('?') { "&" } else { "?" };
        redis_url = format!("{}{}protocol=resp3", redis_url, separator);
    }
    info!("Using Redis URL: {}", redis_url);

    let jwt_manager_arc = Arc::new(jwt_manager);

    let config = GameServerConfig {
        db: db.clone(),
        http_addr: http_addr.clone(),
        grpc_addr,
        region: "test-region".to_string(),
        origin: format!("http://{}", http_addr),
        ws_url: format!("ws://{}/ws", http_addr),
        jwt_manager: jwt_manager_arc.clone(),
        jwt_verifier: jwt_verifier.clone(),
        replay_dir,
        redis_url: redis_url.clone(),
    };

    let game_server = GameServer::start(config).await?;

    // Give the HTTP server a moment to start listening
    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(game_server)
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
    db: Arc<dyn Database>,
    server_id: u64,
    cancellation_token: CancellationToken,
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
                match db.update_server_heartbeat(server_id as i32).await {
                    Ok(()) => {
                        trace!(?server_id, "Heartbeat sent successfully.");
                    }
                    Err(e) => {
                        error!(?server_id, error = %e, "Failed to send heartbeat");
                    }
                }
            }
        }
    }
}
