use std::env;
use anyhow::{Context, Result};
use tracing::info;
use sqlx::postgres::PgPoolOptions;
use refinery::config::{Config, ConfigDbType};
use std::sync::Arc;
use server::game_server::{GameServer, GameServerConfig};
use server::ws_server::TestJwtVerifier;
use server::api::{jwt::{JwtManager, ProductionJwtVerifier}, run_api_server};

mod migrations {
    use refinery::embed_migrations;
    embed_migrations!("./migrations");
}

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

    // Database setup
    let db_host = env::var("SNAKETRON_DB_HOST")
        .context("SNAKETRON_DB_HOST must be set in environment or .env file")?;
    let db_port = env::var("SNAKETRON_DB_PORT")
        .context("SNAKETRON_DB_PORT must be set in environment or .env file")?;
    let db_user = env::var("SNAKETRON_DB_USER")
        .context("SNAKETRON_DB_USER must be set in environment or .env file")?;
    let db_pass = env::var("SNAKETRON_DB_PASS")
        .context("SNAKETRON_DB_PASS must be set in environment or .env file")?;
    let db_name = env::var("SNAKETRON_DB_NAME")
        .context("SNAKETRON_DB_NAME must be set in environment or .env file")?;

    let region = env::var("SNAKETRON_REGION").unwrap_or_else(|_| "default".to_string());

    // Build database connection string
    let db_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        db_user, db_pass, db_host, db_port, db_name
    );

    // Run migrations
    let mut db_config = Config::new(ConfigDbType::Postgres)
        .set_db_host(&db_host)
        .set_db_port(&db_port)
        .set_db_user(&db_user)
        .set_db_pass(&db_pass)
        .set_db_name(&db_name);

    let _migrations_report = migrations::migrations::runner().run_async(&mut db_config).await?;
    info!("Database migrations completed");

    // Create database pool
    let db_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .context("Failed to create PostgreSQL connection pool")?;

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
            Arc::new(TestJwtVerifier::new(db_pool.clone()))
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

    // Get Redis URL from environment (optional)
    let redis_url = env::var("SNAKETRON_REDIS_URL").ok();
    if redis_url.is_some() {
        info!("Redis leader election enabled");
    }

    // Create server configuration
    let config = GameServerConfig {
        db_pool,
        ws_addr,
        grpc_addr,
        region,
        jwt_verifier,
        replay_dir,
        redis_url,
    };

    // Clone db_pool for API server
    let api_db_pool = config.db_pool.clone();

    // Start the game server
    let game_server = GameServer::start(config).await?;
    info!("Server {} started successfully", game_server.id());
    info!("WebSocket server listening on: {}", game_server.ws_addr());
    if let Some(grpc_addr) = game_server.grpc_addr() {
        info!("gRPC server listening on: {}", grpc_addr);
    }

    // Determine web directory
    let web_dir = env::var("SNAKETRON_WEB_DIR").ok()
        .or_else(|| {
            // In production (Docker), serve from /app/web
            if std::path::Path::new("/app/web").exists() {
                Some("/app/web".to_string())
            } else {
                None
            }
        });
    
    if let Some(ref dir) = web_dir {
        info!("Web static files will be served from: {}", dir);
    } else {
        info!("Web static files serving disabled (directory not found)");
    }
    
    // Start the API server with static file serving
    let api_port = env::var("SNAKETRON_API_PORT").unwrap_or_else(|_| "3001".to_string());
    let api_addr = format!("0.0.0.0:{}", api_port);
    
    let api_handle = tokio::spawn(async move {
        if let Err(e) = run_api_server(&api_addr, api_db_pool, &jwt_secret, web_dir.as_deref()).await {
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

