use anyhow::{Context, Result};
use futures_util::stream::{FuturesUnordered, StreamExt};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, trace, warn};

use crate::api::jwt::JwtManager;
use crate::cluster_membership::{BootIdentity, ClusterNamespace};
use crate::executor_cluster::{ExecutorClusterHandle, start_executor_cluster};
use crate::game_bus::GameBus;
use crate::game_executor::PARTITION_COUNT;
use crate::http_server::{DeferredHttpServer, install_http_application};
use crate::lifecycle::TaskLifecycle;
use crate::lobby_manager::LobbyManager;
use crate::matchmaking_manager::MatchmakingManager;
use crate::pubsub_manager::PubSubManager;
use crate::redis_utils::create_connection_manager_until_available;
use crate::region_cache::RegionCache;
use crate::resilience_metrics;
use crate::resilience_metrics::spawn_resilience_metrics;
use crate::{
    db::{Database, ServerRegistration},
    matchmaking::run_matchmaking_loop,
    redis_keys::RedisKeys,
    replication::ReplicationManager,
    ws_server::JwtVerifier,
};
use redis::Client;
use serde::Deserialize;
use std::path::PathBuf;

const ECS_METADATA_LOOKUP_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Deserialize)]
struct EcsTaskMetadata {
    #[serde(rename = "TaskARN")]
    task_arn: Option<String>,
}

fn ecs_task_id_from_arn(task_arn: &str) -> Option<String> {
    task_arn
        .trim()
        .rsplit_once('/')
        .map(|(_, task_id)| task_id.trim())
        .filter(|task_id| !task_id.is_empty())
        .map(str::to_owned)
}

/// Resolve ECS identity for membership diagnostics without making the
/// metadata service a startup dependency. CDK supplies the task-definition
/// ARN directly; the task ID comes from metadata v4 when available and falls
/// back to the process boot UUID for local/test and degraded startup.
async fn resolve_executor_task_metadata(
    boot_identity: &BootIdentity,
) -> (Option<String>, Option<String>) {
    let task_definition = env::var("ECS_TASK_DEFINITION")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let mut task_id = env::var("ECS_TASK_ID")
        .ok()
        .filter(|value| !value.trim().is_empty());

    if task_id.is_none()
        && let Ok(metadata_uri) = env::var("ECS_CONTAINER_METADATA_URI_V4")
        && !metadata_uri.trim().is_empty()
    {
        let endpoint = format!("{}/task", metadata_uri.trim_end_matches('/'));
        let lookup = async {
            reqwest::Client::builder()
                .timeout(ECS_METADATA_LOOKUP_TIMEOUT)
                .build()?
                .get(endpoint)
                .send()
                .await?
                .error_for_status()?
                .json::<EcsTaskMetadata>()
                .await
        };
        match tokio::time::timeout(ECS_METADATA_LOOKUP_TIMEOUT, lookup).await {
            Ok(Ok(metadata)) => {
                task_id = metadata.task_arn.as_deref().and_then(ecs_task_id_from_arn);
            }
            Ok(Err(error)) => {
                warn!(%error, "ECS task metadata lookup failed; using boot identity");
            }
            Err(_) => {
                warn!("ECS task metadata lookup timed out; using boot identity");
            }
        }
    }

    let task_id = task_id.or_else(|| Some(format!("boot:{boot_identity}")));
    (task_id, task_definition)
}

/// Configuration for a game server instance
pub struct GameServerConfig {
    /// Database connection
    pub db: Arc<dyn Database>,
    /// HTTP server address (e.g., "127.0.0.1:8080")
    pub http_addr: String,
    /// gRPC server address for game relay (e.g., "127.0.0.1:50051")
    pub grpc_addr: String,
    /// Region identifier for the server
    pub region: String,
    /// HTTP origin for client connections (e.g., "http://localhost:8080")
    pub origin: String,
    /// WebSocket URL for client connections (e.g., "ws://localhost:8080/ws")
    pub ws_url: String,
    /// JWT manager
    pub jwt_manager: Arc<JwtManager>,
    /// JWT verifier for authentication
    pub jwt_verifier: Arc<dyn JwtVerifier>,
    /// Optional directory for saving game replays
    pub replay_dir: Option<PathBuf>,
    /// Redis URL for membership, assignment, leases, and durable streams
    /// (e.g., "redis://127.0.0.1:6379")
    pub redis_url: String,
}

/// A complete game server instance with all components
pub struct GameServer {
    /// Unique server ID in the database
    pub server_id: u64,
    /// HTTP server address
    pub http_addr: String,
    /// gRPC server address (if enabled)
    pub grpc_addr: String,
    /// Database connection
    db: Arc<dyn Database>,
    /// Cancellation token for graceful shutdown
    cancellation_token: CancellationToken,
    /// Handles for all spawned tasks
    handles: Vec<JoinHandle<()>>,
    /// Optional replay listener
    // replay_listener: Option<Arc<ReplayListener>>,
    /// Replication manager for game state
    replication_manager: Arc<ReplicationManager>,
    /// Shared health/drain state.
    lifecycle: TaskLifecycle,
    /// Any critical task exiting before cancellation is process-fatal.
    fatal_rx: mpsc::UnboundedReceiver<anyhow::Error>,
    #[allow(dead_code)]
    fatal_tx: mpsc::UnboundedSender<anyhow::Error>,
    /// Test servers skip the load-balancer convergence delay.
    route_withdrawal_delay: Duration,
    /// Membership, assignment, and partition-executor manager.
    executor_cluster: ExecutorClusterHandle,
}

impl GameServer {
    /// Get the HTTP server address
    pub fn http_addr(&self) -> &str {
        &self.http_addr
    }

    /// Get the server ID
    pub fn id(&self) -> u64 {
        self.server_id
    }

    /// Get the gRPC server address (if enabled)
    pub fn grpc_addr(&self) -> Option<&str> {
        if self.grpc_addr.is_empty() {
            None
        } else {
            Some(&self.grpc_addr)
        }
    }
    /// Create and start a new game server instance
    pub async fn start(config: GameServerConfig) -> Result<Self> {
        let GameServerConfig {
            db,
            http_addr,
            grpc_addr,
            region,
            origin,
            ws_url,
            jwt_manager,
            jwt_verifier,
            replay_dir: _,
            redis_url,
        } = config;

        // Register server in database
        info!("Registering server in database for region: {}", region);
        let server_id = db
            .register_server(&grpc_addr, &region, &origin, &ws_url)
            .await
            .context("Failed to register server")? as u64;
        info!("Server registered with ID: {}", server_id);

        let boot_identity = BootIdentity::new();
        let task_boot_id = format!("{}:{}", server_id, boot_identity);
        let lifecycle = TaskLifecycle::new(task_boot_id);
        let cluster_namespace = ClusterNamespace::new(region.clone())?;
        let (fatal_tx, fatal_rx) = mpsc::unbounded_channel();

        // Create cancellation token for graceful shutdown
        let cancellation_token = CancellationToken::new();
        let mut handles = Vec::new();

        // Bind the dependency-free health shell before attempting Valkey.
        // Replacement tasks therefore stay live (but unready and unavailable
        // for application traffic) throughout a regional cache outage.
        let (deferred_http, http_task) =
            DeferredHttpServer::bind(&http_addr, lifecycle.clone(), cancellation_token.clone())
                .await
                .context("Failed to bind HTTP liveness listener")?;
        let http_lifecycle = lifecycle.clone();
        let http_fatal_tx = fatal_tx.clone();
        let http_exit_token = cancellation_token.clone();
        handles.push(tokio::spawn(async move {
            let result = http_task
                .await
                .context("HTTP server task panicked")
                .and_then(|result| result);
            if !http_exit_token.is_cancelled() {
                http_lifecycle.mark_critical_failure();
                let reason = match result {
                    Ok(()) => anyhow::anyhow!("HTTP server exited unexpectedly"),
                    Err(error) => error.context("HTTP server failed"),
                };
                error!("{}", reason);
                let _ = http_fatal_tx.send(reason);
            }
        }));

        // Start heartbeat loop to keep server registration alive
        let heartbeat_db = db.clone();
        let heartbeat_token = cancellation_token.clone();
        let heartbeat_registration = ServerRegistration {
            grpc_address: grpc_addr.clone(),
            region: region.clone(),
            origin: origin.clone(),
            ws_url: ws_url.clone(),
        };
        handles.push(tokio::spawn(async move {
            run_heartbeat_loop(
                heartbeat_db,
                server_id,
                heartbeat_registration,
                heartbeat_token,
            )
            .await;
        }));

        // Create the broadcast channel for Redis Pub/Sub
        let (pubsub_tx, pubsub_rx) = tokio::sync::broadcast::channel(5000);
        // Drop the default receiver to avoid filling up the channel
        drop(pubsub_rx);

        // Ensure RESP3 protocol is enabled for push notifications
        let redis_url =
            if !redis_url.contains("protocol=resp3") && !redis_url.contains("protocol=3") {
                let separator = if redis_url.contains('?') { "&" } else { "?" };
                format!("{}{}protocol=resp3", redis_url, separator)
            } else {
                redis_url
            };
        info!("Using Redis URL: {}", redis_url);

        // Create the Redis client and connection manager
        let redis_client =
            Client::open(redis_url.clone()).context("Failed to create Redis client")?;
        let redis = create_connection_manager_until_available(
            redis_client,
            pubsub_tx.clone(),
            cancellation_token.clone(),
        )
        .await?;
        info!("Redis connection manager created successfully");

        // Create the PubsubManager for loss-tolerant fan-out
        // (chat/lobby/counters).
        let pubsub_manager = Arc::new(PubSubManager::new(redis.clone(), pubsub_tx.clone()));

        // Create the game-critical message bus (Redis Streams).
        let game_bus = Arc::new(GameBus::new(
            redis.clone(),
            Client::open(redis_url.clone())
                .context("Failed to create Redis client for game bus")?,
            cancellation_token.clone(),
        ));

        // Create the LobbyManager
        let lobby_manager = Arc::new(LobbyManager::new(
            redis.clone(),
            db.clone(),
            pubsub_manager.clone(),
        ));
        lobby_manager.start_lobby_update_forwarder();

        // Create the matchmaking manager
        let matchmaking_manager = Arc::new(tokio::sync::Mutex::new(
            MatchmakingManager::new(redis.clone())
                .context("Failed to create matchmaking manager")?,
        ));

        // Create RegionCache for dynamic region discovery
        let dynamodb_client = crate::db::dynamodb::dynamodb_client().await;
        let table_prefix =
            env::var("DYNAMODB_TABLE_PREFIX").unwrap_or_else(|_| "snaketron".to_string());
        let region_cache = Arc::new(RegionCache::new(dynamodb_client, table_prefix));
        region_cache
            .clone()
            .spawn_refresh_task(cancellation_token.clone());
        info!("Region cache refresh task started");

        // Start the matchmaking service
        info!("Starting matchmaking service");
        let match_token = cancellation_token.clone();
        let match_matchmaking_manager = matchmaking_manager.clone();
        let match_lobby_manager = lobby_manager.clone();
        let match_db = db.clone();
        let match_lifecycle = lifecycle.clone();
        let match_fatal_tx = fatal_tx.clone();
        let match_exit_token = match_token.clone();
        handles.push(tokio::spawn(async move {
            let mm = match_matchmaking_manager.lock().await.clone();
            drop(match_matchmaking_manager); // Drop the lock
            let result = run_matchmaking_loop(mm, match_token, match_lobby_manager, match_db).await;
            if !match_exit_token.is_cancelled() {
                match_lifecycle.mark_critical_failure();
                let reason = match result {
                    Ok(()) => anyhow::anyhow!("matchmaking loop exited unexpectedly"),
                    Err(error) => error.context("matchmaking loop failed"),
                };
                error!("{}", reason);
                let _ = match_fatal_tx.send(reason);
            }
        }));

        // Start replication manager for all partitions BEFORE game executors
        info!("Starting replication manager for game state replication");
        let replication_partitions: Vec<u32> = (0..PARTITION_COUNT).collect();
        let replication_manager = Arc::new(
            ReplicationManager::new(
                replication_partitions,
                cancellation_token.clone(),
                &redis_url,
            )
            .await
            .context("Failed to create replication manager")?,
        );

        // Keep replica readiness truthful while workers anchor asynchronously.
        // A task launched during a Valkey outage stays live/unready and the
        // workers retry locally; an actually terminated reader remains fatal.
        let replication_monitor = replication_manager.clone();
        let replication_lifecycle = lifecycle.clone();
        let replication_token = cancellation_token.clone();
        let replication_fatal_tx = fatal_tx.clone();
        handles.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            loop {
                tokio::select! {
                    _ = replication_token.cancelled() => break,
                    _ = interval.tick() => {
                        let ready = replication_monitor.is_ready().await;
                        replication_lifecycle.mark_replicas_ready(ready);
                        if replication_monitor.has_failed_worker() {
                            replication_lifecycle.mark_critical_failure();
                            let _ = replication_fatal_tx.send(anyhow::anyhow!(
                                "a partition replication worker exited unexpectedly"
                            ));
                            break;
                        }
                    }
                }
            }
        }));

        let (ecs_task_id, ecs_task_definition) =
            resolve_executor_task_metadata(&boot_identity).await;
        let executor_cluster_token = cancellation_token.child_token();
        let (executor_cluster, executor_cluster_task) = start_executor_cluster(
            server_id,
            boot_identity.clone(),
            cluster_namespace.clone(),
            redis.clone(),
            game_bus.clone(),
            db.clone(),
            lifecycle.clone(),
            ecs_task_id,
            ecs_task_definition,
            executor_cluster_token,
        )?;
        let cluster_lifecycle = lifecycle.clone();
        let cluster_fatal_tx = fatal_tx.clone();
        let cluster_exit_token = cancellation_token.clone();
        handles.push(tokio::spawn(async move {
            let result = executor_cluster_task
                .await
                .context("executor cluster task panicked")
                .and_then(|result| result);
            if !cluster_exit_token.is_cancelled() && !cluster_lifecycle.is_draining() {
                cluster_lifecycle.mark_critical_failure();
                let reason = match result {
                    Ok(()) => anyhow::anyhow!("executor cluster exited unexpectedly"),
                    Err(error) => error.context("executor cluster failed"),
                };
                error!("{}", reason);
                let _ = cluster_fatal_tx.send(reason);
            } else if let Err(error) = result {
                warn!(
                    "Executor cluster stopped with error during drain: {}",
                    error
                );
            }
        }));

        // Emit bounded-cardinality regional resilience gauges and local event
        // counters through CloudWatch Embedded Metric Format. Telemetry is
        // intentionally best effort and never participates in authority.
        handles.push(spawn_resilience_metrics(
            redis.clone(),
            cluster_namespace.clone(),
            lifecycle.clone(),
            server_id,
            cancellation_token.child_token(),
        ));

        // A bounded, independent Valkey probe drives readiness. Liveness is
        // intentionally unaffected so an outage cannot cause an ECS restart
        // storm. Use a write canary rather than PING: under noeviction memory
        // pressure reads can remain healthy while checkpoints/XADD fail.
        let mut readiness_redis = redis.clone();
        let redis_lifecycle = lifecycle.clone();
        let redis_token = cancellation_token.clone();
        let redis_exit_token = redis_token.clone();
        let redis_failure_lifecycle = lifecycle.clone();
        let redis_fatal_tx = fatal_tx.clone();
        let readiness_canary_key =
            RedisKeys::readiness_write_canary(&region, lifecycle.task_boot_id());
        let readiness_probe = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = redis_token.cancelled() => break,
                    _ = interval.tick() => {
                        let write_canary = tokio::time::timeout(
                            Duration::from_millis(500),
                            redis::cmd("SET")
                                .arg(&readiness_canary_key)
                                .arg(chrono::Utc::now().timestamp_millis())
                                .arg("PX")
                                .arg(10_000)
                                .query_async::<()>(&mut readiness_redis),
                        ).await;
                        if matches!(write_canary, Ok(Ok(()))) {
                            redis_lifecycle.mark_redis_success_now();
                        }
                    }
                }
            }
        });
        handles.push(tokio::spawn(async move {
            let result = readiness_probe.await;
            if !redis_exit_token.is_cancelled() {
                redis_failure_lifecycle.mark_critical_failure();
                let reason = match result {
                    Ok(()) => anyhow::anyhow!("Valkey readiness probe exited unexpectedly"),
                    Err(error) => anyhow::anyhow!("Valkey readiness probe panicked: {error}"),
                };
                error!("{}", reason);
                let _ = redis_fatal_tx.send(reason);
            }
        }));

        // Note: HTTP server will be started separately in main.rs
        // This is because it needs both the replication manager and JWT verifier
        info!("HTTP server will be started externally at {}", http_addr);

        // Wait a moment for all services to start
        tokio::time::sleep(Duration::from_millis(1000)).await;

        // Atomically expose API/WebSocket routes on the listener that has
        // served liveness throughout Redis bootstrap.
        install_http_application(
            &deferred_http,
            db.clone(),
            jwt_manager.clone(),
            jwt_verifier.clone(),
            redis.clone(),
            redis_url.clone(),
            pubsub_manager.clone(),
            game_bus.clone(),
            matchmaking_manager.clone(),
            replication_manager.clone(),
            cancellation_token.clone(),
            server_id,
            region.clone(),
            region_cache.clone(),
            lobby_manager.clone(),
            lifecycle.clone(),
            cluster_namespace.clone(),
        )
        .await?;

        lifecycle.activate();
        executor_cluster.activate().await?;
        // Startup/liveness must not depend on readiness converging. A Valkey
        // or cluster-coordination outage can begin after the listener and
        // critical workers are running; in that case the task stays alive and
        // out of routing until the existing workers reconnect and make the
        // readiness predicates true.
        info!(
            ready = lifecycle.is_ready(),
            "Game server {} listener and critical workers started", server_id
        );

        let route_withdrawal_delay = if region == "test-region" {
            Duration::ZERO
        } else {
            Duration::from_millis(
                env::var("SNAKETRON_ROUTE_WITHDRAWAL_MS")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(8_000),
            )
        };

        Ok(Self {
            server_id,
            http_addr,
            grpc_addr,
            db,
            cancellation_token,
            handles,
            // replay_listener,
            replication_manager,
            lifecycle,
            fatal_rx,
            fatal_tx,
            route_withdrawal_delay,
            executor_cluster,
        })
    }

    /// Get a reference to the database
    pub fn db(&self) -> &Arc<dyn Database> {
        &self.db
    }

    /// Get the cancellation token
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    /// Get the replication manager
    pub fn replication_manager(&self) -> &Arc<ReplicationManager> {
        &self.replication_manager
    }

    pub fn lifecycle(&self) -> &TaskLifecycle {
        &self.lifecycle
    }

    /// Resolves only when a critical local worker exits before shutdown.
    pub async fn wait_for_fatal(&mut self) -> Option<anyhow::Error> {
        self.fatal_rx.recv().await
    }

    /// Shutdown the server gracefully
    pub async fn shutdown(mut self) -> Result<()> {
        info!(
            "Starting graceful shutdown of game server {}",
            self.server_id
        );

        let shutdown_budget_ms = env::var("SNAKETRON_SHUTDOWN_DEADLINE_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(45_000);
        let shutdown_started = tokio::time::Instant::now();
        let shutdown_deadline = shutdown_started + Duration::from_millis(shutdown_budget_ms);
        let shutdown_deadline_unix_ms = chrono::Utc::now()
            .timestamp_millis()
            .saturating_add(shutdown_budget_ms.min(i64::MAX as u64) as i64);

        // Step 1: withdraw from new traffic. This local transition is the
        // correctness path; the Dynamo status is only an operational hint.
        // Flip readiness immediately. The client-facing socket deadline is
        // published later, once route withdrawal has consumed its actual
        // share of the one global shutdown budget.
        self.lifecycle.begin_draining(shutdown_deadline_unix_ms);
        let executor_cluster = self.executor_cluster.clone();
        let executor_handoff_deadline =
            (tokio::time::Instant::now() + Duration::from_secs(20)).min(shutdown_deadline);
        let executor_drain =
            tokio::spawn(async move { executor_cluster.drain(executor_handoff_deadline).await });
        info!("Updating server status to 'draining'");
        match tokio::time::timeout(
            Duration::from_secs(2),
            self.db
                .update_server_status(self.server_id as i32, "draining"),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => warn!("Failed to publish draining status: {}", error),
            Err(_) => warn!("Timed out publishing draining status"),
        }

        // Give Traefik's active health check plus provider poll time to remove
        // this backend. Existing sockets continue to receive game/lobby data.
        if !self.route_withdrawal_delay.is_zero() {
            tokio::time::sleep_until(
                (tokio::time::Instant::now() + self.route_withdrawal_delay).min(shutdown_deadline),
            )
            .await;
        }

        // Step 2: ask supported clients to establish a replacement through the
        // same regional URL. Do not wait for a game to finish.
        let client_handoff_limit = tokio::time::Instant::now() + Duration::from_secs(20);
        let handoff_deadline = client_handoff_limit.min(shutdown_deadline);
        let handoff_remaining_ms = handoff_deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .as_millis()
            .min(i64::MAX as u128) as i64;
        let handoff_deadline_unix_ms = chrono::Utc::now()
            .timestamp_millis()
            .saturating_add(handoff_remaining_ms);
        let drain_notice = self.lifecycle.begin_draining(handoff_deadline_unix_ms);
        self.lifecycle.announce_drain(drain_notice);
        while self.lifecycle.active_websockets() > 0
            && tokio::time::Instant::now() < handoff_deadline
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if self.lifecycle.active_websockets() > 0 {
            resilience_metrics::record_planned_drain_failure(1);
            warn!(
                remaining_websockets = self.lifecycle.active_websockets(),
                "Planned WebSocket handoff window ended with sockets still attached"
            );
        }

        match tokio::time::timeout_at(executor_handoff_deadline, executor_drain).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => {
                resilience_metrics::record_planned_drain_failure(1);
                warn!(
                    "Executor handoff failed; lease expiry will recover: {}",
                    error
                );
            }
            Ok(Err(error)) => {
                resilience_metrics::record_planned_drain_failure(1);
                warn!("Executor handoff task panicked: {}", error);
            }
            Err(_) => {
                resilience_metrics::record_planned_drain_failure(1);
                warn!("Executor handoff reached the global shutdown deadline");
            }
        }

        // Step 3: one final cancellation stops HTTP and every remaining worker.
        // Every join below shares the one ECS-bounded deadline.
        info!("Stopping all services after planned handoff window");
        self.lifecycle.begin_stopping();
        self.cancellation_token.cancel();

        let mut pending: FuturesUnordered<_> = self.handles.drain(..).collect();
        let join_result = tokio::time::timeout_at(shutdown_deadline, async {
            while let Some(result) = pending.next().await {
                if let Err(error) = result {
                    error!("Service panicked during shutdown: {:?}", error);
                }
            }
        })
        .await;
        if join_result.is_err() {
            error!("Global service shutdown deadline reached");
            // Dropping JoinHandles detaches them. Abort anything that did not
            // honor cancellation so the runtime cannot keep shutdown alive.
            for handle in pending.iter() {
                handle.abort();
            }
        }

        // Best-effort only: crash recovery and lease expiry do not depend on it.
        if tokio::time::Instant::now() < shutdown_deadline {
            match tokio::time::timeout_at(
                shutdown_deadline,
                self.db
                    .update_server_status(self.server_id as i32, "offline"),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => warn!("Failed to publish offline status: {}", error),
                Err(_) => warn!("Timed out publishing offline status"),
            }
        }

        info!("Game server {} shut down gracefully", self.server_id);
        Ok(())
    }
}

/// Helper function to start a game server for testing
/// Creates a database connection and determines ports automatically
pub async fn start_test_server(
    db: Arc<dyn Database>,
    jwt_manager: JwtManager,
    jwt_verifier: Arc<dyn JwtVerifier>,
) -> Result<GameServer> {
    start_test_server_with_grpc(db, jwt_manager, jwt_verifier, false).await
}

/// Helper function to start a game server for testing with optional gRPC
pub async fn start_test_server_with_grpc(
    db: Arc<dyn Database>,
    jwt_manager: JwtManager,
    jwt_verifier: Arc<dyn JwtVerifier>,
    _enable_grpc: bool,
) -> Result<GameServer> {
    // Get available ports
    let http_port = get_available_port();
    let http_addr = format!("127.0.0.1:{}", http_port);

    // Enable gRPC if requested
    let grpc_addr = format!("127.0.0.1:{}", get_available_port());

    // Use centralized replay directory for tests
    let test_name = format!("test_{}", uuid::Uuid::new_v4());
    let replay_path = crate::replay::directory::get_test_replay_directory(&test_name);
    std::fs::create_dir_all(&replay_path).ok();
    let replay_dir = Some(replay_path);

    // Use environment variable if set, otherwise use default
    // Note: protocol=resp3 is required for push notifications support
    let mut redis_url = std::env::var("SNAKETRON_REDIS_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    // Ensure RESP3 protocol is enabled for push notifications
    if !redis_url.contains("protocol=resp3") && !redis_url.contains("protocol=3") {
        let separator = if redis_url.contains('?') { "&" } else { "?" };
        redis_url = format!("{}{}protocol=resp3", redis_url, separator);
    }
    info!("Using Redis URL: {}", redis_url);

    let jwt_manager_arc = Arc::new(jwt_manager);

    let config = GameServerConfig {
        db: db.clone(),
        http_addr: http_addr.clone(),
        grpc_addr,
        region: "test-region".to_string(),
        origin: format!("http://{}", http_addr),
        ws_url: format!("ws://{}/ws", http_addr),
        jwt_manager: jwt_manager_arc.clone(),
        jwt_verifier: jwt_verifier.clone(),
        replay_dir,
        redis_url: redis_url.clone(),
    };

    let game_server = GameServer::start(config).await?;

    // Production startup deliberately returns once the listener and workers
    // are running, even while dependency readiness is false. Integration-test
    // callers generally need a routable server, so keep that stronger
    // precondition in this test-only helper instead.
    tokio::time::timeout(Duration::from_secs(30), async {
        while !game_server.lifecycle.is_ready() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("Test game server did not become ready within 30 seconds")?;

    Ok(game_server)
}

/// Get an available port by binding to port 0
pub fn get_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    std::thread::sleep(std::time::Duration::from_millis(10));
    port
}

/// Each heartbeat write must be bounded: a single hung request would otherwise
/// silently block every later heartbeat until the region drops out of the
/// region cache. Kept below the 5s interval so a stall never delays the next tick.
const HEARTBEAT_WRITE_TIMEOUT: Duration = Duration::from_secs(4);

/// Run a loop to update last_heartbeat in the database
pub async fn run_heartbeat_loop(
    db: Arc<dyn Database>,
    server_id: u64,
    registration: ServerRegistration,
    cancellation_token: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    info!(?server_id, "Starting heartbeat loop");

    loop {
        tokio::select! {
            biased;
            _ = cancellation_token.cancelled() => {
                info!(?server_id, "Heartbeat shutdown received");
                break;
            }

            _ = interval.tick() => {
                match tokio::time::timeout(
                    HEARTBEAT_WRITE_TIMEOUT,
                    db.update_server_heartbeat(server_id as i32, &registration),
                )
                .await
                {
                    Ok(Ok(())) => {
                        trace!(?server_id, "Heartbeat sent successfully.");
                    }
                    Ok(Err(e)) => {
                        error!(?server_id, error = %e, "Failed to send heartbeat");
                    }
                    Err(_) => {
                        error!(
                            ?server_id,
                            timeout_secs = HEARTBEAT_WRITE_TIMEOUT.as_secs(),
                            "Heartbeat write timed out"
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod ecs_metadata_tests {
    use super::ecs_task_id_from_arn;

    #[test]
    fn extracts_task_id_from_long_and_short_ecs_arns() {
        assert_eq!(
            ecs_task_id_from_arn("arn:aws:ecs:us-east-1:123456789012:task/snaketron/abc123")
                .as_deref(),
            Some("abc123")
        );
        assert_eq!(
            ecs_task_id_from_arn("arn:aws:ecs:us-east-1:123456789012:task/def456").as_deref(),
            Some("def456")
        );
        assert_eq!(ecs_task_id_from_arn("not-an-arn"), None);
    }
}
