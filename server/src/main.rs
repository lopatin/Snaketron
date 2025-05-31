use std::env;
use anyhow::{Context, Result};
use tracing::info;
use sqlx::postgres::PgPoolOptions;
use refinery::config::{Config, ConfigDbType};
use std::sync::Arc;
use server::game_server::{GameServer, GameServerConfig};
use server::ws_server::DefaultJwtVerifier;

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

    // Initialize tracing
    tracing_subscriber::fmt::init();

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
    
    let grpc_addr = env::var("SNAKETRON_GRPC_PORT").ok()
        .map(|port| format!("0.0.0.0:{}", port));

    // Create JWT verifier
    let jwt_verifier = Arc::new(DefaultJwtVerifier) as Arc<dyn server::ws_server::JwtVerifier>;

    // Parse Raft peers from environment
    let raft_peers: Vec<String> = env::var("SNAKETRON_RAFT_PEERS")
        .ok()
        .map(|peers| peers.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default();
    
    if !raft_peers.is_empty() {
        info!("Joining Raft cluster with peers: {:?}", raft_peers);
    } else {
        info!("Starting as first Raft node");
    }

    // Create server configuration
    let config = GameServerConfig {
        db_pool,
        ws_addr,
        grpc_addr,
        region,
        jwt_verifier,
        raft_peers,
    };

    // Start the game server
    let game_server = GameServer::start(config).await?;
    info!("Server {} started successfully", game_server.id());
    info!("WebSocket server listening on: {}", game_server.ws_addr());
    if let Some(grpc_addr) = game_server.grpc_addr() {
        info!("gRPC server listening on: {}", grpc_addr);
    }

    // Wait for shutdown signal
    info!("Server started. Waiting for shutdown signal (Ctrl+C)...");
    tokio::signal::ctrl_c().await?;

    info!("Received shutdown signal. Shutting down gracefully...");
    game_server.shutdown().await?;
    
    info!("Server shut down successfully");
    Ok(())
}

