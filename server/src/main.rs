use anyhow::{Context, Result};
use server::api::jwt::{JwtManager, ProductionJwtVerifier};
use server::db::{Database, dynamodb::DynamoDatabase};
use server::game_server::{GameServer, GameServerConfig};
use server::http_server::run_http_server;
use server::region_cache::RegionCache;
use server::ws_server::TestJwtVerifier;
use std::env;
use std::sync::Arc;
use redis::Client;
use tokio::sync::broadcast::Receiver;
use tracing::info;
use server::pubsub_manager::PubSubManager;
use server::redis_utils::create_connection_manager;

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

    // Get Redis URL from environment
    let redis_url = env::var("SNAKETRON_REDIS_URL")
        .context("SNAKETRON_REDIS_URL environment variable is required")?;

    let region = env::var("SNAKETRON_REGION")
        .context("SNAKETRON_REGION environment variable is required")?;

    // Server configuration
    let http_port = env::var("SNAKETRON_HTTP_PORT")
        .or_else(|_| env::var("SNAKETRON_WS_PORT")) // Fallback to old WS_PORT for compatibility
        .unwrap_or_else(|_| "8080".to_string());
    let http_addr = format!("0.0.0.0:{}", http_port);

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
    
    let jwt_manager = Arc::new(JwtManager::new(&jwt_secret));

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

    // Calculate origin and WebSocket URL for client connections
    let origin =
        env::var("SNAKETRON_ORIGIN").unwrap_or_else(|_| format!("http://localhost:{}", http_port));
    let ws_url =
        env::var("SNAKETRON_WS_URL").unwrap_or_else(|_| format!("ws://localhost:{}/ws", http_port));

    // gRPC is currently not used, but the config expects it
    let grpc_addr = String::new();

    // Create server configuration
    let config = GameServerConfig {
        db: db.clone(),
        http_addr: http_addr.clone(),
        grpc_addr,
        region: region.clone(),
        origin: origin.clone(),
        ws_url: ws_url.clone(),
        jwt_manager,
        jwt_verifier,
        replay_dir,
        redis_url: redis_url.clone(),
    };

    let game_server = GameServer::start(config).await?;

    info!("Server started. Waiting for shutdown signal (Ctrl+C)...");
    tokio::signal::ctrl_c().await?;

    info!("Received shutdown signal. Shutting down gracefully...");
    game_server.shutdown().await?;

    info!("Server shut down successfully");
    Ok(())
}
