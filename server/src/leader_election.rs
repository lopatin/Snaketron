use anyhow::{anyhow, Result, Context};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Script};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::{interval, sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn, error, debug};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

pub struct LeaderElection {
    redis_url: String,
    redis_client: Option<ConnectionManager>,
    server_id: u64,
    lease_key: String,
    lease_duration_ms: u64,
    is_leader: Arc<AtomicBool>,
    cancellation_token: CancellationToken,
}

impl LeaderElection {
    pub async fn new(
        redis_url: &str,
        server_id: u64,
        lease_key: String,
        lease_duration: Duration,
        cancellation_token: CancellationToken,
    ) -> Result<Self> {
        // Try to establish initial connection but don't fail if Redis is down
        let redis_client = match Self::create_connection(redis_url).await {
            Ok(client) => {
                info!("Successfully connected to Redis for leader election");
                Some(client)
            }
            Err(e) => {
                warn!("Failed to connect to Redis initially: {}. Will retry during election loop.", e);
                None
            }
        };
        
        Ok(Self {
            redis_url: redis_url.to_string(),
            redis_client,
            server_id,
            lease_key,
            lease_duration_ms: lease_duration.as_millis() as u64,
            is_leader: Arc::new(AtomicBool::new(false)),
            cancellation_token,
        })
    }
    
    async fn create_connection(redis_url: &str) -> Result<ConnectionManager> {
        let client = redis::Client::open(redis_url)
            .context("Failed to create Redis client")?;
        
        // Add a timeout to prevent blocking forever on connection attempts
        let connection_timeout = Duration::from_secs(2);
        let redis_client = timeout(connection_timeout, ConnectionManager::new(client))
            .await
            .context("Connection attempt timed out")?
            .context("Failed to create Redis connection manager")?;
            
        Ok(redis_client)
    }
    
    async fn ensure_connection(&mut self) -> Result<()> {
        if self.redis_client.is_none() {
            debug!("Attempting to reconnect to Redis...");
            match Self::create_connection(&self.redis_url).await {
                Ok(client) => {
                    info!("Successfully reconnected to Redis");
                    self.redis_client = Some(client);
                }
                Err(e) => {
                    // Log the error but don't block - we'll retry on next operation
                    debug!("Failed to reconnect to Redis: {}. Will retry later.", e);
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    pub async fn run_election_loop(mut self, user_service: impl Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>) -> Result<()> {
        info!("Starting leader election loop for server_id={}", self.server_id);
        
        let mut renew_interval = interval(Duration::from_millis(300));
        renew_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        
        // Add a health check interval to detect connection issues proactively
        let mut health_check_interval = interval(Duration::from_secs(5));
        health_check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        
        let mut service_handle: Option<tokio::task::JoinHandle<Result<()>>> = None;
        let mut rng = StdRng::from_entropy();
        
        // Create the initial claim sleep
        let claim_duration_ms = rng.gen_range(500..=1500);
        let mut claim_sleep = Box::pin(sleep(Duration::from_millis(claim_duration_ms)));
        
        loop {
            tokio::select! {
                _ = self.cancellation_token.cancelled() => {
                    info!("Leader election shutdown received");
                    self.is_leader.store(false, Ordering::Release);
                    if let Some(handle) = service_handle.take() {
                        handle.abort();
                    }
                    break;
                }
                _ = &mut claim_sleep => {
                    debug!("Attempting to claim leadership for server_id={}", self.server_id);
                    if !self.is_leader() {
                        match self.try_acquire_lease().await {
                            Ok(true) => {
                                info!("Acquired leadership for server_id={}", self.server_id);
                                self.is_leader.store(true, Ordering::Release);
                                
                                // Start the user service
                                if service_handle.is_none() {
                                    let service_future = user_service();
                                    service_handle = Some(tokio::spawn(service_future));
                                }
                            }
                            Ok(false) => {
                                debug!("Failed to acquire leadership - another server holds the lease");
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
                    debug!("Renewing lease for server_id={}", self.server_id);
                    if self.is_leader() {
                        match self.renew_lease().await {
                            Ok(true) => {
                                // debug!("Successfully renewed lease");
                            }
                            Ok(false) => {
                                warn!("Lost leadership - failed to renew lease");
                                self.is_leader.store(false, Ordering::Release);
                                
                                // Stop the user service
                                if let Some(handle) = service_handle.take() {
                                    handle.abort();
                                }
                            }
                            Err(e) => {
                                error!("Error renewing lease: {}", e);
                                self.is_leader.store(false, Ordering::Release);
                                
                                // Stop the user service on error
                                if let Some(handle) = service_handle.take() {
                                    handle.abort();
                                }
                            }
                        }
                    }
                }
                _ = health_check_interval.tick() => {
                    debug!("Performing Redis health check for server_id={}", self.server_id);
                    // Periodic health check to ensure connection is still valid
                    if self.redis_client.is_some() {
                        if let Some(ref mut client) = self.redis_client {
                            // Try a simple PING command with timeout
                            let ping_timeout = Duration::from_secs(1);
                            let ping_result: Result<Result<String, _>, _> = timeout(
                                ping_timeout, 
                                redis::cmd("PING").query_async(client)
                            ).await;
                            
                            match ping_result {
                                Ok(Ok(_)) => {
                                    // Connection is healthy
                                }
                                Ok(Err(e)) => {
                                    error!("Redis health check failed: {}. Invalidating connection.", e);
                                    self.redis_client = None;
                                }
                                Err(_) => {
                                    error!("Redis health check timed out. Invalidating connection.");
                                    self.redis_client = None;
                                }
                            }
                            
                            // If connection was lost and we were the leader, step down
                            if self.redis_client.is_none() && self.is_leader() {
                                warn!("Lost Redis connection while being leader. Stepping down.");
                                self.is_leader.store(false, Ordering::Release);
                                
                                if let Some(handle) = service_handle.take() {
                                    handle.abort();
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }

    async fn try_acquire_lease(&mut self) -> Result<bool> {
        // Ensure we have a connection
        self.ensure_connection().await?;
        
        if let Some(ref mut client) = self.redis_client {
            let result: Result<Option<String>, _> = client
                .set_options(
                    &self.lease_key,
                    self.server_id.to_string(),
                    redis::SetOptions::default()
                        .conditional_set(redis::ExistenceCheck::NX)
                        .with_expiration(redis::SetExpiry::PX(self.lease_duration_ms))
                )
                .await;
            match result {
                Ok(result) => Ok(result.is_some()),
                Err(e) => {
                    // Connection might be broken, invalidate it
                    error!("Failed to execute SET NX command: {}. Invalidating connection.", e);
                    self.redis_client = None;
                    Err(anyhow::anyhow!("Redis connection error: {}", e))
                }
            }
        } else {
            Err(anyhow::anyhow!("No Redis connection available"))
        }
    }

    async fn renew_lease(&mut self) -> Result<bool> {
        // Ensure we have a connection
        self.ensure_connection().await?;
        
        if let Some(ref mut client) = self.redis_client {
            // Lua script to atomically check ownership and renew lease
            let lua_script = r#"
                if redis.call("get", KEYS[1]) == ARGV[1] then
                    return redis.call("pexpire", KEYS[1], ARGV[2])
                else
                    return 0
                end
            "#;
            
            let script = Script::new(lua_script);
            let result: Result<i32, _> = script
                .key(&self.lease_key)
                .arg(self.server_id.to_string())
                .arg(self.lease_duration_ms)
                .invoke_async(client)
                .await;
            match result {
                Ok(result) => Ok(result == 1),
                Err(e) => {
                    // Connection might be broken, invalidate it
                    error!("Failed to execute lease renewal script: {}. Invalidating connection.", e);
                    self.redis_client = None;
                    Err(anyhow::anyhow!("Redis connection error: {}", e))
                }
            }
        } else {
            Err(anyhow::anyhow!("No Redis connection available"))
        }
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Acquire)
    }
}