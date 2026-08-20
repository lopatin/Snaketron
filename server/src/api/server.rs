use anyhow::Result;
use axum::{
    Router, middleware,
    routing::{get, post, put},
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use crate::db::Database;

use super::admin;
use super::auth::{self, AuthState};
use super::crazygames;
use super::jwt::JwtManager;
use super::middleware::{AuthMiddlewareState, admin_middleware, auth_middleware};
use super::rate_limit::{rate_limit_layer, rate_limit_middleware};

pub async fn run_api_server(addr: &str, db: Arc<dyn Database>, jwt_secret: &str) -> Result<()> {
    let jwt_manager = Arc::new(JwtManager::new(jwt_secret));

    let auth_state = AuthState {
        analytics: None,
        db: db.clone(),
        jwt_manager: jwt_manager.clone(),
        user_cache: None,
        crazygames_verifier: crazygames::configured_verifier_from_env()?,
        texture_store: None,
    };
    let auth_middleware_state = AuthMiddlewareState {
        jwt_manager: jwt_manager.clone(),
        db,
    };

    // Configure CORS
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Create rate limiter for username check endpoint (10 requests per minute)
    let username_check_limiter = rate_limit_layer(10, 60);
    let crazygames_exchange_limiter = rate_limit_layer(60, 60);

    // Build router with protected routes
    let protected_routes = Router::new()
        .route("/api/auth/me", get(auth::get_current_user))
        .route("/api/history", get(admin::get_user_history))
        .route(
            "/api/auth/crazygames/preferences",
            put(crazygames::save_preferences)
                .layer(axum::extract::DefaultBodyLimit::max(64 * 1024)),
        )
        .layer(middleware::from_fn_with_state(
            auth_middleware_state.clone(),
            auth_middleware,
        ));

    let admin_routes = Router::new()
        .route("/api/admin/history", get(admin::get_admin_history))
        .route(
            "/api/admin/config",
            get(admin::get_admin_config)
                .put(admin::update_admin_config)
                .layer(axum::extract::DefaultBodyLimit::max(16 * 1024)),
        )
        .route("/api/admin/config/audit", get(admin::get_config_audit))
        .layer(middleware::from_fn(admin_middleware))
        .layer(middleware::from_fn_with_state(
            auth_middleware_state,
            auth_middleware,
        ));

    let app = Router::new()
        .route("/api/health", get(health_check))
        .route("/api/config", get(admin::get_public_config))
        .route("/api/auth/register", post(auth::register))
        .route("/api/auth/login", post(auth::login))
        .route(
            "/api/auth/crazygames/exchange",
            post(crazygames::exchange)
                .layer(axum::extract::DefaultBodyLimit::max(64 * 1024))
                .layer(middleware::from_fn_with_state(
                    crazygames_exchange_limiter,
                    rate_limit_middleware,
                )),
        )
        .route(
            "/api/auth/check-username",
            post(auth::check_username).layer(middleware::from_fn_with_state(
                username_check_limiter,
                rate_limit_middleware,
            )),
        )
        .merge(protected_routes)
        .merge(admin_routes)
        .layer(cors)
        .with_state(auth_state);

    // Start server
    let listener = TcpListener::bind(addr).await?;
    info!("API server listening on {}", addr);

    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow::anyhow!("API server error: {}", e))
}

async fn health_check() -> &'static str {
    "OK"
}
