use anyhow::{Context, Result};
use redis::{Client, PushInfo};
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use std::time::Duration;

/// Creates a ConnectionManager with standardized configuration for the application.
///
/// Note: The client must be created with a Redis URL that includes `protocol=resp3`
/// parameter to enable RESP3 protocol, which is required for push notifications.
/// Example: `redis://127.0.0.1:6379?protocol=resp3`
pub async fn create_connection_manager(
    client: Client,
    pubsub_tx: tokio::sync::broadcast::Sender<PushInfo>,
) -> Result<ConnectionManager> {
    let config = ConnectionManagerConfig::new()
        .set_push_sender(pubsub_tx)
        .set_automatic_resubscription()
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
