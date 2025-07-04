use anyhow::{Result, Context};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Script};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn, error, debug};

pub struct LeaderElection {
    redis_client: ConnectionManager,
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
        let client = redis::Client::open(redis_url)
            .context("Failed to create Redis client")?;
        
        let redis_client = ConnectionManager::new(client).await
            .context("Failed to create Redis connection manager")?;
        
        Ok(Self {
            redis_client,
            server_id,
            lease_key,
            lease_duration_ms: lease_duration.as_millis() as u64,
            is_leader: Arc::new(AtomicBool::new(false)),
            cancellation_token,
        })
    }

    pub async fn run_election_loop(mut self, user_service: impl Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>) -> Result<()> {
        info!("Starting leader election loop for server_id={}", self.server_id);
        
        let mut claim_interval = interval(Duration::from_secs(1));
        let mut renew_interval = interval(Duration::from_millis(300));
        let mut service_handle: Option<tokio::task::JoinHandle<Result<()>>> = None;
        
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
                _ = claim_interval.tick() => {
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
                }
                _ = renew_interval.tick() => {
                    if self.is_leader() {
                        match self.renew_lease().await {
                            Ok(true) => {
                                debug!("Successfully renewed lease");
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
            }
        }
        
        Ok(())
    }

    async fn try_acquire_lease(&mut self) -> Result<bool> {
        let result: Option<String> = self.redis_client
            .set_options(
                &self.lease_key,
                self.server_id.to_string(),
                redis::SetOptions::default()
                    .conditional_set(redis::ExistenceCheck::NX)
                    .with_expiration(redis::SetExpiry::PX(self.lease_duration_ms))
            )
            .await
            .context("Failed to execute SET NX command")?;
        
        Ok(result.is_some())
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
        let result: i32 = script
            .key(&self.lease_key)
            .arg(self.server_id.to_string())
            .arg(self.lease_duration_ms)
            .invoke_async(&mut self.redis_client)
            .await
            .context("Failed to execute lease renewal script")?;
        
        Ok(result == 1)
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::Acquire)
    }
}