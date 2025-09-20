use anyhow::{Result, Context};
use redis::{Client, aio::{ConnectionManager, ConnectionManagerConfig}};
use std::time::Duration;

/// Creates a ConnectionManager with standardized configuration for the application.
///
/// Configuration:
/// - Connection timeout: 30 seconds
/// - Response timeout: 30 seconds
/// - Retries: 10 attempts with exponential backoff
/// - Backoff delays: 1s, 2s, 4s, 8s, 16s, 32s, 60s (capped), 60s, 60s, 60s
/// - Maximum delay between retries: 60 seconds (1 minute)
pub async fn create_connection_manager(client: Client) -> Result<ConnectionManager> {
    let config = ConnectionManagerConfig::new()
        .set_connection_timeout(Duration::from_secs(30))
        .set_response_timeout(Duration::from_secs(30))
        .set_number_of_retries(10)
        .set_exponent_base(2)
        .set_factor(1000) // Factor of 1000 means delays are in seconds (base^n * 1000ms)
        .set_max_delay(60000); // Maximum 60 second (1 minute) delay

    ConnectionManager::new_with_config(client, config)
        .await
        .context("Failed to create Redis connection manager with config")
}