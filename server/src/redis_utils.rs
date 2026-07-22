use anyhow::{Context, Result};
use redis::aio::{ConnectionManager, ConnectionManagerConfig};
use redis::{Client, PushInfo};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::warn;

const BOOTSTRAP_CONNECTION_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
const BOOTSTRAP_CONNECTION_RETRY_DELAY: Duration = Duration::from_millis(250);

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

/// Establish the application's initial Redis connection without making a
/// temporary outage process-fatal. Callers bind their dependency-free
/// liveness listener before awaiting this function; once a connection is
/// established the returned manager keeps the normal reconnect policy above.
pub async fn create_connection_manager_until_available(
    client: Client,
    pubsub_tx: tokio::sync::broadcast::Sender<PushInfo>,
    cancellation: CancellationToken,
) -> Result<ConnectionManager> {
    loop {
        let attempt = tokio::time::timeout(
            BOOTSTRAP_CONNECTION_ATTEMPT_TIMEOUT,
            create_connection_manager(client.clone(), pubsub_tx.clone()),
        );
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => anyhow::bail!("Redis bootstrap cancelled"),
            result = attempt => match result {
                Ok(Ok(manager)) => return Ok(manager),
                Ok(Err(error)) => {
                    warn!(%error, "Redis bootstrap connection failed; task remains live/unready");
                }
                Err(_) => {
                    warn!("Redis bootstrap connection timed out; task remains live/unready");
                }
            },
        }

        tokio::select! {
            biased;
            _ = cancellation.cancelled() => anyhow::bail!("Redis bootstrap cancelled"),
            _ = tokio::time::sleep(BOOTSTRAP_CONNECTION_RETRY_DELAY) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io;
    use tokio::net::{TcpListener, TcpStream};

    #[tokio::test]
    async fn cold_boot_connection_waits_for_redis_and_recovers_in_place() -> Result<()> {
        let reservation = TcpListener::bind("127.0.0.1:0").await?;
        let proxy_address = reservation.local_addr()?;
        drop(reservation);

        let client = Client::open(format!("redis://{proxy_address}/1?protocol=resp3"))?;
        let (pubsub_tx, _pubsub_rx) = tokio::sync::broadcast::channel(8);
        let cancellation = CancellationToken::new();
        let connection_cancellation = cancellation.clone();
        let connection = tokio::spawn(async move {
            create_connection_manager_until_available(client, pubsub_tx, connection_cancellation)
                .await
        });

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !connection.is_finished(),
            "cold Redis outage escaped the local bootstrap retry"
        );

        let listener = TcpListener::bind(proxy_address).await?;
        let proxy_cancellation = CancellationToken::new();
        let proxy_exit = proxy_cancellation.clone();
        let proxy = tokio::spawn(async move {
            loop {
                let (mut inbound, _) = tokio::select! {
                    biased;
                    _ = proxy_exit.cancelled() => return,
                    accepted = listener.accept() => match accepted {
                        Ok(accepted) => accepted,
                        Err(_) => return,
                    },
                };
                tokio::spawn(async move {
                    let Ok(mut outbound) = TcpStream::connect("127.0.0.1:6379").await else {
                        return;
                    };
                    let _ = io::copy_bidirectional(&mut inbound, &mut outbound).await;
                });
            }
        });

        let mut manager = tokio::time::timeout(Duration::from_secs(5), connection)
            .await
            .context("Redis bootstrap did not recover after the endpoint appeared")???;
        let pong: String = redis::cmd("PING").query_async(&mut manager).await?;
        assert_eq!(pong, "PONG");

        cancellation.cancel();
        proxy_cancellation.cancel();
        proxy.await?;
        Ok(())
    }
}
