use anyhow::Result;
use axum::{
    extract::{ws::WebSocketUpgrade, State},
    middleware,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use crate::db::Database;
use crate::api::auth::{self, AuthState};
use crate::api::jwt::JwtManager;
use crate::api::middleware::auth_middleware;
use crate::api::rate_limit::{rate_limit_layer, rate_limit_middleware};
use crate::ws_server::{JwtVerifier, handle_websocket};
use crate::replication::ReplicationManager;

/// Combined HTTP server state containing both API and WebSocket dependencies
#[derive(Clone)]
pub struct HttpServerState {
    /// Database connection
    pub db: Arc<dyn Database>,
    /// JWT manager for API authentication
    pub jwt_manager: Arc<JwtManager>,
    /// JWT verifier for WebSocket authentication
    pub jwt_verifier: Arc<dyn JwtVerifier>,
    /// Redis URL for pubsub and matchmaking
    pub redis_url: String,
    /// Replication manager for game state
    pub replication_manager: Arc<ReplicationManager>,
    /// Cancellation token for graceful shutdown
    pub cancellation_token: tokio_util::sync::CancellationToken,
}

/// Run the combined HTTP server with both API and WebSocket endpoints
pub async fn run_http_server(
    addr: &str,
    db: Arc<dyn Database>,
    jwt_secret: &str,
    jwt_verifier: Arc<dyn JwtVerifier>,
    redis_url: String,
    replication_manager: Arc<ReplicationManager>,
    cancellation_token: tokio_util::sync::CancellationToken,
) -> Result<()> {
    let jwt_manager = Arc::new(JwtManager::new(jwt_secret));

    // Create state for both API and WebSocket handlers
    let state = HttpServerState {
        db: db.clone(),
        jwt_manager: jwt_manager.clone(),
        jwt_verifier,
        redis_url,
        replication_manager,
        cancellation_token: cancellation_token.clone(),
    };

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

    // Build API routes with AuthState
    let api_routes = Router::new()
        .route("/api/health", get(health_check))
        .route("/api/auth/register", post(auth::register))
        .route("/api/auth/login", post(auth::login))
        .route("/api/auth/check-username",
            post(auth::check_username)
                .layer(middleware::from_fn_with_state(
                    username_check_limiter,
                    rate_limit_middleware,
                ))
        )
        .merge(protected_routes)
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
    ws.on_upgrade(move |socket| handle_websocket(
        socket,
        state.db,
        state.jwt_verifier,
        state.redis_url,
        state.replication_manager,
        state.cancellation_token,
    ))
}

/// Health check handler
async fn health_check() -> &'static str {
    "OK"
}