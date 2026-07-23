use anyhow::{Context, Result, bail};
use redis::aio::{ConnectionLike, ConnectionManager, ConnectionManagerConfig};
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::{Client, PushInfo, RedisFuture, Value};
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::warn;

const BOOTSTRAP_CONNECTION_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
const BOOTSTRAP_CONNECTION_RETRY_DELAY: Duration = Duration::from_millis(250);

/// Parsed application endpoint. `cluster=true` belongs to Snaketron rather
/// than the Redis URL grammar, so it is always removed before redis-rs sees
/// the URL.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RedisEndpoint {
    canonical_url: String,
    cluster: bool,
}

impl RedisEndpoint {
    fn parse(input: &str) -> Result<Self> {
        let mut url = redis::parse_redis_url(input)
            .with_context(|| format!("invalid Redis URL scheme in {input:?}"))?;
        let mut cluster = false;
        let mut retained = Vec::new();
        for (key, value) in url.query_pairs() {
            if key == "cluster" {
                cluster = match value.as_ref() {
                    "true" | "1" => true,
                    "false" | "0" => false,
                    other => {
                        bail!("Redis URL cluster flag must be true/false or 1/0, got {other:?}")
                    }
                };
            } else {
                retained.push((key.into_owned(), value.into_owned()));
            }
        }
        url.query_pairs_mut().clear().extend_pairs(retained);

        if cluster {
            let database = match url.path().trim_matches('/') {
                "" => 0,
                value => value
                    .parse::<i64>()
                    .context("Redis URL contains an invalid database number")?,
            };
            if database != 0 {
                bail!("Redis Cluster supports only database 0, not database {database}");
            }
        }

        Ok(Self {
            canonical_url: url.to_string(),
            cluster,
        })
    }
}

/// Cloneable application client that supports both the standalone Valkey
/// used by local tests and ElastiCache Serverless' cluster endpoint.
#[derive(Clone)]
pub enum RedisClient {
    Standalone {
        client: Client,
        push_sender: Option<tokio::sync::broadcast::Sender<PushInfo>>,
    },
    Cluster(ClusterClient),
}

impl RedisClient {
    pub fn open(
        url: &str,
        push_sender: Option<tokio::sync::broadcast::Sender<PushInfo>>,
    ) -> Result<Self> {
        let endpoint = RedisEndpoint::parse(url)?;
        if endpoint.cluster {
            let mut builder = ClusterClient::builder([endpoint.canonical_url])
                .retries(10)
                .min_retry_wait(100)
                .max_retry_wait(60_000)
                .retry_wait_formula(100, 2)
                .connection_timeout(Duration::from_secs(30))
                .response_timeout(Duration::from_secs(30));
            if let Some(push_sender) = push_sender {
                builder = builder.push_sender(push_sender);
            }
            Ok(Self::Cluster(
                builder
                    .build()
                    .context("failed to create Redis Cluster client")?,
            ))
        } else {
            Ok(Self::Standalone {
                client: Client::open(endpoint.canonical_url)
                    .context("failed to create standalone Redis client")?,
                push_sender,
            })
        }
    }

    pub fn is_cluster(&self) -> bool {
        matches!(self, Self::Cluster(_))
    }

    /// Create the shared reconnecting command connection.
    pub async fn get_managed_connection(&self) -> Result<RedisConnection> {
        match self {
            Self::Standalone {
                client,
                push_sender,
            } => {
                let config = standalone_manager_config(push_sender.clone());
                Ok(RedisConnection::StandaloneManaged(
                    ConnectionManager::new_with_config(client.clone(), config)
                        .await
                        .context("failed to create standalone Redis connection manager")?,
                ))
            }
            Self::Cluster(client) => Ok(RedisConnection::Cluster(
                client
                    .get_async_connection()
                    .await
                    .context("failed to create Redis Cluster connection")?,
            )),
        }
    }

    /// Create a connection dedicated to one blocking Streams reader. Cluster
    /// routing and MOVED retries remain active, while an XREAD can never park
    /// the application's shared publisher connection.
    pub async fn get_dedicated_connection(&self) -> Result<RedisConnection> {
        match self {
            Self::Standalone { client, .. } => Ok(RedisConnection::StandaloneMultiplexed(
                client
                    .get_multiplexed_async_connection()
                    .await
                    .context("failed to create dedicated standalone Redis connection")?,
            )),
            Self::Cluster(client) => Ok(RedisConnection::Cluster(
                client
                    .get_async_connection()
                    .await
                    .context("failed to create dedicated Redis Cluster connection")?,
            )),
        }
    }
}

impl From<Client> for RedisClient {
    fn from(client: Client) -> Self {
        Self::Standalone {
            client,
            push_sender: None,
        }
    }
}

/// One concrete async connection type for application components. Implementing
/// redis-rs' connection trait preserves `AsyncCommands`, `Script`, pipelines,
/// and existing call sites while cluster routing stays encapsulated here.
#[derive(Clone)]
pub enum RedisConnection {
    StandaloneManaged(ConnectionManager),
    StandaloneMultiplexed(redis::aio::MultiplexedConnection),
    Cluster(ClusterConnection),
}

impl From<ConnectionManager> for RedisConnection {
    fn from(connection: ConnectionManager) -> Self {
        Self::StandaloneManaged(connection)
    }
}

impl RedisConnection {
    pub async fn subscribe(&mut self, channel: &str) -> redis::RedisResult<()> {
        match self {
            Self::StandaloneManaged(connection) => connection.subscribe(channel).await,
            Self::Cluster(connection) => connection.subscribe(channel).await,
            Self::StandaloneMultiplexed(connection) => {
                redis::cmd("SUBSCRIBE")
                    .arg(channel)
                    .query_async(connection)
                    .await
            }
        }
    }
}

impl ConnectionLike for RedisConnection {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a redis::Cmd) -> RedisFuture<'a, Value> {
        match self {
            Self::StandaloneManaged(connection) => connection.req_packed_command(cmd),
            Self::StandaloneMultiplexed(connection) => connection.req_packed_command(cmd),
            Self::Cluster(connection) => connection.req_packed_command(cmd),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        match self {
            Self::StandaloneManaged(connection) => {
                connection.req_packed_commands(cmd, offset, count)
            }
            Self::StandaloneMultiplexed(connection) => {
                connection.req_packed_commands(cmd, offset, count)
            }
            Self::Cluster(connection) => connection.req_packed_commands(cmd, offset, count),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            Self::StandaloneManaged(connection) => connection.get_db(),
            Self::StandaloneMultiplexed(connection) => connection.get_db(),
            Self::Cluster(connection) => connection.get_db(),
        }
    }
}

fn standalone_manager_config(
    push_sender: Option<tokio::sync::broadcast::Sender<PushInfo>>,
) -> ConnectionManagerConfig {
    let mut config = ConnectionManagerConfig::new()
        .set_connection_timeout(Duration::from_secs(30))
        .set_response_timeout(Duration::from_secs(30))
        .set_number_of_retries(10)
        .set_exponent_base(2)
        .set_factor(1000)
        .set_max_delay(60000);
    if let Some(push_sender) = push_sender {
        config = config
            .set_push_sender(push_sender)
            .set_automatic_resubscription();
    }
    config
}

/// Creates a ConnectionManager with standardized configuration for the application.
///
/// Note: The client must be created with a Redis URL that includes `protocol=resp3`
/// parameter to enable RESP3 protocol, which is required for push notifications.
/// Example: `redis://127.0.0.1:6379?protocol=resp3`
pub async fn create_connection_manager(
    client: Client,
    pubsub_tx: tokio::sync::broadcast::Sender<PushInfo>,
) -> Result<ConnectionManager> {
    ConnectionManager::new_with_config(client, standalone_manager_config(Some(pubsub_tx)))
        .await
        .context("failed to create standalone Redis connection manager")
}

/// Establish the application's initial Redis connection without making a
/// temporary outage process-fatal. Callers bind their dependency-free
/// liveness listener before awaiting this function; once a connection is
/// established the returned manager keeps the normal reconnect policy above.
pub async fn create_connection_manager_until_available(
    client: RedisClient,
    cancellation: CancellationToken,
) -> Result<RedisConnection> {
    loop {
        let attempt = tokio::time::timeout(
            BOOTSTRAP_CONNECTION_ATTEMPT_TIMEOUT,
            client.get_managed_connection(),
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

    #[test]
    fn cluster_selector_is_removed_without_losing_tls_or_resp3() {
        let endpoint =
            RedisEndpoint::parse("rediss://cache.example:6379/?protocol=resp3&cluster=true")
                .unwrap();
        assert!(endpoint.cluster);
        assert!(
            endpoint
                .canonical_url
                .starts_with("rediss://cache.example:6379/")
        );
        assert!(endpoint.canonical_url.contains("protocol=resp3"));
        assert!(!endpoint.canonical_url.contains("cluster="));
    }

    #[test]
    fn cluster_mode_rejects_nonzero_database() {
        let error = RedisEndpoint::parse("redis://127.0.0.1:6379/1?cluster=true")
            .expect_err("Redis Cluster has no database 1");
        assert!(error.to_string().contains("database 0"));
    }

    #[test]
    fn standalone_database_and_protocol_are_preserved() {
        let endpoint = RedisEndpoint::parse("redis://127.0.0.1:6379/1?protocol=resp3").unwrap();
        assert!(!endpoint.cluster);
        assert!(endpoint.canonical_url.contains("/1"));
        assert!(endpoint.canonical_url.contains("protocol=resp3"));
    }

    #[tokio::test]
    async fn cold_boot_connection_waits_for_redis_and_recovers_in_place() -> Result<()> {
        let reservation = TcpListener::bind("127.0.0.1:0").await?;
        let proxy_address = reservation.local_addr()?;
        drop(reservation);

        let redis_url = format!("redis://{proxy_address}/1?protocol=resp3");
        let (pubsub_tx, _pubsub_rx) = tokio::sync::broadcast::channel(8);
        let client = RedisClient::open(&redis_url, Some(pubsub_tx))?;
        let cancellation = CancellationToken::new();
        let connection_cancellation = cancellation.clone();
        let connection = tokio::spawn(async move {
            create_connection_manager_until_available(client, connection_cancellation).await
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
