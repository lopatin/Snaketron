mod ws_server;
mod matchmaking;
mod grpc_server;
mod games_manager;

use std::env;
use common::*;
use chrono::{DateTime, Utc};
use anyhow::{Context, Result};
use tracing::{error, info, trace, warn};
use tokio::time::Duration;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use refinery::config::{Config, ConfigDbType};
use tokio::sync::{mpsc, oneshot, watch, broadcast, Mutex};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use ws_server::*;
use crate::games_manager::GamesManager;

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
        .context("SNAKETRON_DB_URL must be set in environment or .env file")?;
    let db_port = env::var("SNAKETRON_DB_PORT")
        .context("SNAKETRON_DB_PORT must be set in environment or .env file")?;
    let db_user = env::var("SNAKETRON_DB_USER")
        .context("SNAKETRON_DB_USER must be set in environment or .env file")?;
    let db_pass = env::var("SNAKETRON_DB_PASS")
        .context("SNAKETRON_DB_PASS must be set in environment or .env file")?;
    let db_name = env::var("SNAKETRON_DB_NAME")
        .context("SNAKETRON_DB_NAME must be set in environment or .env file")?;

    let region = env::var("SNAKETRON_REGION").unwrap_or_else(|_| "default".to_string());

    let mut db_config = Config::new(ConfigDbType::Postgres)
        .set_db_host(&db_host)
        .set_db_port(&db_port)
        .set_db_user(&db_user)
        .set_db_pass(&db_pass)
        .set_db_name(&db_name);

    // Run migrations
    let migrations_report = migrations::migrations::runner().run_async(&mut db_config).await?;

    let db_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!(
            "postgres://{}:{}@{}:{}/{}",
            db_user, db_pass, db_host, db_port, db_name
        ))
        .await
        .context("Failed to create PostgreSQL connection pool")?;

    let ws_port = env::var("SNAKETRON_WS_PORT").unwrap_or_else(|_| "8080".to_string());
    let ws_addr = format!("0.0.0.0:{}", ws_port);


    // Register server
    let server_id = register_server(&db_pool, region.as_str()).await?;
    info!(server_id, "Server registered in database");

    let cancellation_token = CancellationToken::new();

    // Start heartbeat loop
    let heartbeat_pool = db_pool.clone();
    let heartbeat_server_id = server_id.clone();
    let heartbeat_cancellation_token = cancellation_token.clone();
    let heartbeat_loop= tokio::spawn(
        run_heartbeat_loop(heartbeat_pool, heartbeat_server_id, heartbeat_cancellation_token));

    // GamesManager
    let games_manager = Arc::new(Mutex::new(GamesManager::new()));

    // Websocket server
    let websocket_cancellation_token = cancellation_token.clone();
    let external_server_handle = tokio::spawn(async move {
        run_websocket_server(&ws_addr, games_manager, websocket_cancellation_token).await
    });


    info!("Server started. Waiting for shutdown signal.");
    tokio::signal::ctrl_c().await?;

    info!("Received shutdown signal. Shutting down.");
    cancellation_token.cancel();
    heartbeat_loop.await?;
    external_server_handle.await?
}

