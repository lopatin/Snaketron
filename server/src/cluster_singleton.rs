use anyhow::{Context, Result, anyhow};
use common::CLUSTER_RENEWAL_INTERVAL_MS;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use redis::aio::ConnectionManager;
use redis::{
    AsyncCommands, ExistenceCheck, RedisResult, Script, ScriptInvocation, SetExpiry, SetOptions,
    TypedCommands,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::{interval, sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// A cluster singleton ensures that only one instance of a service runs across the entire cluster
/// It uses Redis-based leader election internally to manage exclusivity
///
/// This can be used for various patterns:
/// - Global singleton services (e.g., a single matchmaking service for the entire region)
/// - Partitioned singletons (e.g., one game executor per partition, with automatic failover)
///
/// The lease_key parameter determines the scope of the singleton - different keys create
/// independent singletons that can coexist on the same cluster.
///
/// The cancellation_token passed to the constructor represents the server shutdown signal.
/// When this token is cancelled, the cluster singleton will:
/// 1. Stop trying to acquire/maintain leadership
/// 2. Gracefully shut down any running service (via a child cancellation token)
/// 3. Exit the run_as_singleton loop
pub struct ClusterSingleton {
    redis: ConnectionManager,
    server_id: u64,
    lease_key: String,
    lease_duration_ms: u64,
    is_leader: Arc<AtomicBool>,
    cancellation_token: CancellationToken,
}

impl ClusterSingleton {
    pub fn new(
        redis: ConnectionManager,
        server_id: u64,
        lease_key: String,
        lease_duration: Duration,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            redis,
            server_id,
            lease_key,
            lease_duration_ms: lease_duration.as_millis() as u64,
            is_leader: Arc::new(AtomicBool::new(false)),
            cancellation_token,
        }
    }

    /// Runs the provided service as a cluster singleton
    /// The service will only run on one node at a time across the cluster
    ///
    /// The service function receives a CancellationToken that will be canceled when:
    /// - The server is shutting down (parent token cancelled)
    /// - This instance loses leadership
    ///
    /// The service should monitor this token and shut down gracefully when canceled.
    pub async fn run(
        mut self,
        user_service: impl Fn(
            CancellationToken,
        ) -> std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send>>,
    ) -> Result<()> {
        info!(
            "Starting cluster singleton for server_id={}",
            self.server_id
        );

        let mut renew_interval = interval(Duration::from_millis(CLUSTER_RENEWAL_INTERVAL_MS));
        renew_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Add a health check interval to detect connection issues proactively
        let mut health_check_interval = interval(Duration::from_secs(5));
        health_check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut service_handle: Option<tokio::task::JoinHandle<Result<()>>> = None;
        let mut service_token: Option<CancellationToken> = None;
        let mut rng = StdRng::from_entropy();

        // Create the initial claim sleep
        let claim_duration_ms = rng.gen_range(500..=1500);
        let mut claim_sleep = Box::pin(sleep(Duration::from_millis(claim_duration_ms)));

        loop {
            tokio::select! {
                _ = self.cancellation_token.cancelled() => {
                    info!("Cluster singleton shutdown received");
                    self.is_leader.store(false, Ordering::Release);

                    // Gracefully shut down the service if running
                    self.stop_service(&mut service_token, &mut service_handle).await;

                    break;
                }
                _ = &mut claim_sleep => {
                    if !self.is_leader() {
                        // debug!("Attempting to become leader for server_id={}", self.server_id);
                        match self.try_acquire_lease().await {
                            Ok(true) => {
                                info!("Became leader for server_id={}", self.server_id);
                                self.is_leader.store(true, Ordering::Release);

                                // Start the user service
                                if service_handle.is_none() {
                                    // Create a child cancellation token for this service instance
                                    // This token will be automatically canceled if the parent (server shutdown) token is canceled
                                    // It can also be manually canceled when we lose leadership
                                    let token = self.cancellation_token.child_token();
                                    service_token = Some(token.clone());
                                    let service_future = user_service(token);
                                    service_handle = Some(tokio::spawn(service_future));
                                }
                            }
                            Ok(false) => {
                                debug!("Failed to become leader - another server is already leader");
                            }
                            Err(e) => {
                                warn!("Error trying to acquire lease: {}", e);
                            }
                        }
                    }

                    // Reset claim sleep with a new random duration
                    let claim_duration_ms = rng.gen_range(500..=1500);
                    claim_sleep = Box::pin(sleep(Duration::from_millis(claim_duration_ms)));
                    // debug!("Next claim attempt in {}ms", claim_duration_ms);
                }
                _ = renew_interval.tick() => {
                    if self.is_leader() {
                        // debug!("Renewing lease for server_id={}", self.server_id);
                        match self.renew_lease().await {
                            Ok(true) => {
                                // debug!("Successfully renewed lease");
                            }
                            Ok(false) => {
                                warn!("Lost leadership - failed to renew lease");
                                self.is_leader.store(false, Ordering::Release);

                                // Stop the user service
                                self.stop_service(&mut service_token, &mut service_handle).await;
                            }
                            Err(e) => {
                                error!("Error renewing lease: {}", e);
                                self.is_leader.store(false, Ordering::Release);

                                // Stop the user service on error
                                self.stop_service(&mut service_token, &mut service_handle).await;
                            }
                        }
                    }
                }
                _ = health_check_interval.tick() => {
                    let ping_timeout = Duration::from_secs(1);
                    let ping_result = timeout(ping_timeout, self.redis.ping::<String>()).await
                        .map_err(|_| anyhow!("Redis PING timed out after {:?}", ping_timeout))?;

                    match ping_result {
                        Ok(rsp) => if rsp != "PONG" {
                            warn!("Unexpected PING response from Redis: {}", rsp);
                        }
                        Err(e) => {
                            error!("Redis PING failed: {}", e);
                            if self.is_leader() {
                                error!("Lost Redis connection while being leader. Stepping down.");
                                self.is_leader.store(false, Ordering::Release);
                                self.stop_service(&mut service_token, &mut service_handle).await;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn try_acquire_lease(&mut self) -> RedisResult<bool> {
        let acquired: bool = self
            .redis
            .set_options(
                &self.lease_key,
                self.server_id.to_string(),
                SetOptions::default()
                    .conditional_set(ExistenceCheck::NX)
                    .with_expiration(SetExpiry::PX(self.lease_duration_ms)),
            )
            .await?;

        Ok(acquired)
    }

    async fn renew_lease(&mut self) -> Result<bool> {
        // Lua script to atomically check ownership and renew lease
        let lua_script = r#"
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("pexpire", KEYS[1], ARGV[2])
            else
                return 0
            end
        "#;

        let script = Script::new(lua_script);

        let _: String = self.redis.load_script(&script).await?;

        let result = self
            .redis
            .invoke_script::<i32>(
                script
                    .key(self.lease_key.clone())
                    .arg(self.server_id.to_string())
                    .arg(self.lease_duration_ms),
            )
            .await?;

        Ok(result == 1)
    }

    /// Returns true if this instance is currently the leader (i.e., running as the singleton)
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Acquire)
    }

    /// Gracefully stops the running service
    async fn stop_service(
        &self,
        service_token: &mut Option<CancellationToken>,
        service_handle: &mut Option<tokio::task::JoinHandle<Result<()>>>,
    ) {
        // First, send a cancellation signal to the service
        if let Some(token) = service_token.take() {
            debug!("Cancelling service token");
            token.cancel();
        }

        // Then wait for the service to finish gracefully
        if let Some(handle) = service_handle.take() {
            debug!("Waiting for service to shut down gracefully");
            match tokio::time::timeout(Duration::from_secs(10), handle).await {
                Ok(Ok(Ok(()))) => info!("Service shut down gracefully"),
                Ok(Ok(Err(e))) => error!("Service returned error: {:?}", e),
                Ok(Err(e)) => error!("Service task panicked: {:?}", e),
                Err(_) => {
                    warn!("Service shutdown timed out after 10 seconds");
                    // The task will continue running but we won't wait for it
                }
            }
        }
    }
}
