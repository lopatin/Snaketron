use anyhow::{Context, Result};
use axum::{
    Router,
    extract::{State, ws::WebSocketUpgrade},
    http::StatusCode,
    middleware,
    response::IntoResponse,
    routing::{get, options, post},
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use crate::api::auth::{self, AuthState};
use crate::api::jwt::JwtManager;
use crate::api::leaderboard::{self, LeaderboardState};
use crate::api::middleware::auth_middleware;
use crate::api::rate_limit::{rate_limit_layer, rate_limit_middleware};
use crate::api::regions;
use crate::db::Database;
use crate::lobby_manager::LobbyManager;
use crate::region_cache::RegionCache;
use crate::replication::ReplicationManager;
use crate::user_cache::UserCache;
use crate::ws_server::{JwtVerifier, handle_websocket};

use crate::redis_utils::create_connection_manager;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Combined HTTP server state containing both API and WebSocket dependencies
#[derive(Clone)]
pub struct HttpServerState {
    /// Database connection
    pub db: Arc<dyn Database>,
    /// JWT manager for API authentication
    pub jwt_manager: Arc<JwtManager>,
    /// JWT verifier for WebSocket authentication
    pub jwt_verifier: Arc<dyn JwtVerifier>,
    /// Cloneable Redis connection manager
    pub redis: ConnectionManager,
    /// Redis URL for creating new connections
    pub redis_url: String,
    /// PubSub manager for Redis pub/sub operations
    pub pubsub_manager: Arc<crate::pubsub_manager::PubSubManager>,
    /// Matchmaking manager for queue operations
    pub matchmaking_manager:
        Arc<tokio::sync::Mutex<crate::matchmaking_manager::MatchmakingManager>>,
    /// Replication manager for game state
    pub replication_manager: Arc<ReplicationManager>,
    /// Cancellation token for graceful shutdown
    pub cancellation_token: tokio_util::sync::CancellationToken,
    /// Active WebSocket connection count
    pub connection_count: Arc<AtomicUsize>,
    /// Server ID for Redis metrics
    pub server_id: u64,
    /// Region name for Redis metrics
    pub region: String,
    /// Region cache for dynamic region discovery
    pub region_cache: Arc<RegionCache>,
    /// Lobby manager for pre-game grouping
    pub lobby_manager: Arc<LobbyManager>,
    /// User cache for quick user lookups
    pub user_cache: UserCache,
}

/// Run the combined HTTP server with both API and WebSocket endpoints
pub async fn run_http_server(
    addr: &str,
    db: Arc<dyn Database>,
    jwt_manager: Arc<JwtManager>,
    jwt_verifier: Arc<dyn JwtVerifier>,
    redis: ConnectionManager,
    redis_url: String,
    pubsub_manager: Arc<crate::pubsub_manager::PubSubManager>,
    matchmaking_manager: Arc<tokio::sync::Mutex<crate::matchmaking_manager::MatchmakingManager>>,
    replication_manager: Arc<ReplicationManager>,
    cancellation_token: tokio_util::sync::CancellationToken,
    server_id: u64,
    region: String,
    region_cache: Arc<RegionCache>,
    lobby_manager: Arc<LobbyManager>,
) -> Result<()> {
    let connection_count = Arc::new(AtomicUsize::new(0));
    let user_cache = UserCache::new(redis.clone(), db.clone());

    // Create state for both API and WebSocket handlers
    let state = HttpServerState {
        db: db.clone(),
        jwt_manager: jwt_manager.clone(),
        jwt_verifier,
        redis: redis.clone(),
        redis_url,
        pubsub_manager,
        matchmaking_manager,
        replication_manager,
        cancellation_token: cancellation_token.clone(),
        connection_count: connection_count.clone(),
        server_id,
        region: region.clone(),
        region_cache,
        lobby_manager,
        user_cache,
    };

    // Start background task to update user count in Redis every 5 seconds
    spawn_metrics_updater(
        redis.clone(),
        server_id,
        region.clone(),
        connection_count,
        cancellation_token.clone(),
    );

    // Start background task to broadcast user counts to WebSocket clients every 5 seconds
    spawn_user_count_broadcaster(redis.clone(), cancellation_token.clone());

    // Create auth state for API routes
    let auth_state = AuthState {
        db: db.clone(),
        jwt_manager: jwt_manager.clone(),
    };

    // Configure CORS
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Create rate limiter for username check endpoint
    let username_check_limiter = rate_limit_layer(1000, 60);

    // Build protected API routes
    let protected_routes = Router::new()
        .route("/api/auth/me", get(auth::get_current_user))
        .layer(middleware::from_fn_with_state(
            jwt_manager.clone(),
            auth_middleware,
        ))
        .with_state(auth_state.clone());

    // Build region routes with HttpServerState (for Redis access)
    let region_routes = Router::new()
        .route("/api/regions", get(regions::list_regions))
        .route("/api/regions/user-counts", get(regions::get_user_counts))
        .with_state(state.clone());

    // Build leaderboard routes with LeaderboardState
    let leaderboard_state = LeaderboardState { db: db.clone() };
    let leaderboard_routes = Router::new()
        .route("/api/leaderboard", get(leaderboard::get_leaderboard))
        .route("/api/seasons", get(leaderboard::list_seasons))
        .with_state(leaderboard_state.clone());

    // Build protected leaderboard routes (requires authentication)
    let protected_leaderboard_routes = Router::new()
        .route("/api/leaderboard/me", get(leaderboard::get_my_ranking))
        .layer(middleware::from_fn_with_state(
            jwt_manager.clone(),
            auth_middleware,
        ))
        .with_state(leaderboard_state);

    // Build API routes with AuthState
    let api_routes = Router::new()
        .route("/api/health", get(regions::health_check_json))
        .route("/api/auth/register", post(auth::register))
        .route("/api/auth/login", post(auth::login))
        .route("/api/auth/guest", post(auth::create_guest))
        .route(
            "/api/auth/check-username",
            post(auth::check_username).layer(middleware::from_fn_with_state(
                username_check_limiter,
                rate_limit_middleware,
            )),
        )
        // Catch-all preflight for all API routes to avoid 500s on OPTIONS
        .route("/api/*path", options(|| async { StatusCode::NO_CONTENT }))
        .merge(protected_routes)
        .merge(region_routes)
        .merge(leaderboard_routes)
        .merge(protected_leaderboard_routes)
        .with_state(auth_state);

    // Build main router combining API and WebSocket endpoints
    let app = Router::new()
        // Health check endpoint
        .route("/health", get(health_check))
        // WebSocket endpoint
        .route("/ws", get(websocket_handler))
        // Nest API routes
        .nest("/", api_routes)
        .layer(cors)
        .with_state(state);

    // Start server
    let listener = TcpListener::bind(addr).await?;
    info!("HTTP server (API + WebSocket) listening on {}", addr);

    // Serve with graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancellation_token.cancelled().await;
            info!("HTTP server received shutdown signal");
        })
        .await
        .map_err(|e| anyhow::anyhow!("HTTP server error: {}", e))
}

/// WebSocket upgrade handler
async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<HttpServerState>,
) -> impl IntoResponse {
    // Increment connection count
    let count = state.connection_count.fetch_add(1, Ordering::Relaxed) + 1;
    tracing::debug!("WebSocket connection opened, total connections: {}", count);

    let connection_count = state.connection_count.clone();

    ws.on_upgrade(move |socket| async move {
        // Handle the WebSocket connection
        handle_websocket(
            socket,
            state.db,
            state.user_cache,
            state.jwt_verifier,
            state.redis,
            state.redis_url,
            state.pubsub_manager,
            state.matchmaking_manager,
            state.replication_manager,
            state.cancellation_token,
            state.lobby_manager,
            state.region,
        )
        .await;

        // Decrement connection count when connection closes
        let count = connection_count.fetch_sub(1, Ordering::Relaxed) - 1;
        tracing::debug!("WebSocket connection closed, total connections: {}", count);
    })
}

/// Health check handler
async fn health_check() -> &'static str {
    "OK"
}

/// Background task to update Redis metrics every 5 seconds
fn spawn_metrics_updater(
    redis: ConnectionManager,
    server_id: u64,
    region: String,
    connection_count: Arc<AtomicUsize>,
    cancellation_token: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("Metrics updater shutting down");
                    break;
                }
                _ = interval.tick() => {
                    let count = connection_count.load(Ordering::Relaxed);

                    if let Err(e) = update_redis_metrics(redis.clone(), server_id, &region, count).await {
                        tracing::error!("Failed to update Redis metrics: {}", e);
                    } else {
                        tracing::trace!("Updated Redis metrics: server_id={}, region={}, count={}", server_id, region, count);
                    }
                }
            }
        }
    });
}

/// Update server metrics in Redis
async fn update_redis_metrics(
    mut redis: ConnectionManager,
    server_id: u64,
    region: &str,
    count: usize,
) -> Result<()> {
    // let mut redis = redis;

    // Set user count with 10-second TTL (auto-cleanup for dead servers)
    let _: () = redis
        .set_ex(format!("server:{}:user_count", server_id), count, 10)
        .await
        .context("Failed to set user count in Redis")?;

    // Set region (no TTL, persistent)
    let _: () = redis
        .set(format!("server:{}:region", server_id), region)
        .await
        .context("Failed to set region in Redis")?;

    Ok(())
}

/// Background task to broadcast user count updates every 5 seconds
fn spawn_user_count_broadcaster(
    redis: ConnectionManager,
    cancellation_token: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("User count broadcaster shutting down");
                    break;
                }
                _ = interval.tick() => {
                    if let Err(e) = broadcast_user_counts(redis.clone()).await {
                        tracing::error!("Failed to broadcast user counts: {}", e);
                    }
                }
            }
        }
    });
}

/// Aggregate user counts from Redis and broadcast to all WebSocket clients
async fn broadcast_user_counts(mut redis: ConnectionManager) -> Result<()> {
    use redis::AsyncCommands;
    use std::collections::HashMap;

    // Query all server user count keys
    let server_keys: Vec<String> = redis::cmd("KEYS")
        .arg("server:*:user_count")
        .query_async(&mut redis)
        .await
        .context("Failed to query server keys")?;

    let mut region_counts: HashMap<String, u32> = HashMap::new();

    for key in server_keys {
        // Get user count for this server
        let count: u32 = match redis::cmd("GET").arg(&key).query_async(&mut redis).await {
            Ok(count) => count,
            Err(e) => {
                tracing::warn!("Failed to get user count for {}: {}", key, e);
                continue;
            }
        };

        // Extract server_id from key "server:{server_id}:user_count"
        let server_id = match key.split(':').nth(1) {
            Some(id) => id,
            None => {
                tracing::warn!("Invalid key format: {}", key);
                continue;
            }
        };

        // Get region for this server
        let region_key = format!("server:{}:region", server_id);
        let region: String = match redis::cmd("GET")
            .arg(&region_key)
            .query_async(&mut redis)
            .await
        {
            Ok(region) => region,
            Err(_) => continue, // Skip if no region set
        };

        // Aggregate counts by region
        *region_counts.entry(region).or_insert(0) += count;
    }

    // Serialize and publish to Redis channel
    let message =
        serde_json::to_string(&region_counts).context("Failed to serialize user counts")?;

    let _: () = redis
        .publish("user_count_updates", message)
        .await
        .context("Failed to publish user counts")?;

    tracing::trace!("Broadcasted user counts: {:?}", region_counts);
    Ok(())
}
