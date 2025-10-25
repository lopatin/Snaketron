use anyhow::{Context, Result};
use server::api::jwt::{JwtManager, ProductionJwtVerifier};
use server::db::{Database, dynamodb::DynamoDatabase};
use server::game_server::{GameServer, GameServerConfig};
use server::http_server::run_http_server;
use server::region_cache::RegionCache;
use server::ws_server::TestJwtVerifier;
use std::env;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Print the current working directory
    let current_dir = env::current_dir().context("Failed to get current directory")?;
    println!("Current directory: {:?}", current_dir);

    // Load .env file if exists
    dotenv::dotenv().ok();

    // Initialize tracing with environment filter and log compatibility
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    // Set up tracing subscriber with log compatibility
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug"));

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Database setup - now using DynamoDB
    let db: Arc<dyn Database> = Arc::new(
        DynamoDatabase::new()
            .await
            .context("Failed to initialize DynamoDB client")?,
    );
    info!("DynamoDB client initialized");

    // Create RegionCache for dynamic region discovery
    let aws_config = aws_config::load_from_env().await;
    let dynamodb_client = aws_sdk_dynamodb::Client::new(&aws_config);
    let table_prefix =
        env::var("DYNAMODB_TABLE_PREFIX").unwrap_or_else(|_| "snaketron".to_string());
    let region_cache = Arc::new(RegionCache::new(dynamodb_client, table_prefix));

    let region = env::var("SNAKETRON_REGION")
        .context("SNAKETRON_REGION environment variable is required")?;

    // Server configuration
    let http_port = env::var("SNAKETRON_HTTP_PORT")
        .or_else(|_| env::var("SNAKETRON_WS_PORT")) // Fallback to old WS_PORT for compatibility
        .unwrap_or_else(|_| "8080".to_string());
    let http_addr = format!("0.0.0.0:{}", http_port);

    let grpc_addr = env::var("SNAKETRON_GRPC_PORT").unwrap_or_else(|_| "50051".to_string());

    // Get JWT secret from environment or use a default for development
    let jwt_secret = env::var("SNAKETRON_JWT_SECRET").unwrap_or_else(|_| {
        tracing::warn!("SNAKETRON_JWT_SECRET not set, using default secret (NOT FOR PRODUCTION!)");
        "your-secret-key-change-this-in-production".to_string()
    });

    // Create JWT verifier - use test mode if SNAKETRON_TEST_MODE is set
    let jwt_verifier: Arc<dyn server::ws_server::JwtVerifier> =
        if env::var("SNAKETRON_TEST_MODE").unwrap_or_default() == "true" {
            info!("Running in TEST MODE - JWT verification disabled");
            Arc::new(TestJwtVerifier::new(db.clone()))
        } else {
            let jwt_manager = Arc::new(JwtManager::new(&jwt_secret));
            Arc::new(ProductionJwtVerifier::new(jwt_manager))
        };

    // Set up replay directory - use environment variable or default to centralized location
    let replay_dir = if let Ok(custom_dir) = env::var("SNAKETRON_REPLAY_DIR") {
        let path = std::path::PathBuf::from(custom_dir);
        info!("Using custom replay directory: {:?}", path);
        Some(path)
    } else {
        // Use centralized replay directory
        match server::replay::directory::ensure_replay_directory() {
            Ok(path) => {
                info!("Replay recording enabled, saving to: {:?}", path);
                Some(path)
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to create replay directory: {}. Replay recording disabled.",
                    e
                );
                None
            }
        }
    };

    // Raft peers are now discovered automatically from the database

    // Get Redis URL from environment (required)
    let redis_url = env::var("SNAKETRON_REDIS_URL")
        .context("SNAKETRON_REDIS_URL environment variable is required")?;
    info!("Redis leader election enabled at {}", redis_url);

    // Calculate origin and WebSocket URL for client connections
    let origin =
        env::var("SNAKETRON_ORIGIN").unwrap_or_else(|_| format!("http://localhost:{}", http_port));
    let ws_url =
        env::var("SNAKETRON_WS_URL").unwrap_or_else(|_| format!("ws://localhost:{}/ws", http_port));

    // Create server configuration
    let config = GameServerConfig {
        db: db.clone(),
        http_addr: http_addr.clone(),
        grpc_addr,
        region: region.clone(),
        origin: origin.clone(),
        ws_url: ws_url.clone(),
        jwt_verifier: jwt_verifier.clone(),
        replay_dir,
        redis_url: redis_url.clone(),
    };

    // Start the game server
    let game_server = GameServer::start(config).await?;
    info!("Server {} started successfully", game_server.id());
    info!(
        "HTTP server (API + WebSocket) will listen on: {}",
        game_server.http_addr()
    );
    if let Some(grpc_addr) = game_server.grpc_addr() {
        info!("gRPC server listening on: {}", grpc_addr);
    }

    // Start region cache refresh task
    let region_cache_clone = region_cache.clone();
    let cache_cancellation_token = game_server.cancellation_token().clone();
    region_cache_clone.spawn_refresh_task(cache_cancellation_token);
    info!("Region cache refresh task started");

    // Start the unified HTTP server (API + WebSocket)
    let http_db = db.clone();
    let http_jwt_verifier = jwt_verifier.clone();
    let http_redis_url = redis_url.clone();
    let http_replication_manager = game_server
        .replication_manager()
        .ok_or_else(|| anyhow::anyhow!("No replication manager available"))?
        .clone();
    let http_cancellation_token = game_server.cancellation_token().clone();
    let http_server_id = game_server.id();
    let http_region = region;
    let http_region_cache = region_cache.clone();
    let http_handle = tokio::spawn(async move {
        if let Err(e) = run_http_server(
            &http_addr,
            http_db,
            &jwt_secret,
            http_jwt_verifier,
            http_redis_url,
            http_replication_manager,
            http_cancellation_token,
            http_server_id,
            http_region,
            http_region_cache,
        )
        .await
        {
            tracing::error!("HTTP server error: {}", e);
        }
    });

    // Wait for shutdown signal
    info!("Server started. Waiting for shutdown signal (Ctrl+C)...");
    tokio::signal::ctrl_c().await?;

    info!("Received shutdown signal. Shutting down gracefully...");

    // Shutdown servers
    http_handle.abort();
    game_server.shutdown().await?;

    info!("Server shut down successfully");
    Ok(())
}
