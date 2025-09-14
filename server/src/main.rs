use std::env;
use anyhow::{Context, Result};
use tracing::info;
use std::sync::Arc;
use server::game_server::{GameServer, GameServerConfig};
use server::ws_server::TestJwtVerifier;
use server::api::{jwt::{JwtManager, ProductionJwtVerifier}, run_api_server};
use server::db::{Database, dynamodb::DynamoDatabase};

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
            .context("Failed to initialize DynamoDB client")?
    );
    info!("DynamoDB client initialized");

    let region = env::var("SNAKETRON_REGION").unwrap_or_else(|_| "default".to_string());

    // Server configuration
    let ws_port = env::var("SNAKETRON_WS_PORT").unwrap_or_else(|_| "8080".to_string());
    let ws_addr = format!("0.0.0.0:{}", ws_port);
    
    let grpc_addr = env::var("SNAKETRON_GRPC_PORT").unwrap_or_else(|_| "50051".to_string());

    // Get JWT secret from environment or use a default for development
    let jwt_secret = env::var("SNAKETRON_JWT_SECRET")
        .unwrap_or_else(|_| {
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
                tracing::warn!("Failed to create replay directory: {}. Replay recording disabled.", e);
                None
            }
        }
    };

    // Raft peers are now discovered automatically from the database

    // Get Redis URL from environment (required)
    let redis_url = env::var("SNAKETRON_REDIS_URL")
        .context("SNAKETRON_REDIS_URL environment variable is required")?;
    info!("Redis leader election enabled at {}", redis_url);

    // Create server configuration
    let config = GameServerConfig {
        db: db.clone(),
        ws_addr,
        grpc_addr,
        region,
        jwt_verifier,
        replay_dir,
        redis_url,
    };

    // Clone db for API server
    let api_db = db.clone();

    // Start the game server
    let game_server = GameServer::start(config).await?;
    info!("Server {} started successfully", game_server.id());
    info!("WebSocket server listening on: {}", game_server.ws_addr());
    if let Some(grpc_addr) = game_server.grpc_addr() {
        info!("gRPC server listening on: {}", grpc_addr);
    }

    // Start the API server
    let api_port = env::var("SNAKETRON_API_PORT").unwrap_or_else(|_| "3001".to_string());
    let api_addr = format!("0.0.0.0:{}", api_port);
    
    let api_handle = tokio::spawn(async move {
        if let Err(e) = run_api_server(&api_addr, api_db, &jwt_secret).await {
            tracing::error!("API server error: {}", e);
        }
    });

    // Wait for shutdown signal
    info!("Server started. Waiting for shutdown signal (Ctrl+C)...");
    tokio::signal::ctrl_c().await?;

    info!("Received shutdown signal. Shutting down gracefully...");
    
    // Shutdown both servers
    api_handle.abort();
    game_server.shutdown().await?;
    
    info!("Server shut down successfully");
    Ok(())
}

