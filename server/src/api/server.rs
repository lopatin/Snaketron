use anyhow::Result;
use axum::{
    middleware,
    routing::{get, post},
    Router,
};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::{
    cors::{Any, CorsLayer},
    services::{ServeDir, ServeFile},
};
use tracing::info;

use super::auth::{self, AuthState};
use super::jwt::JwtManager;
use super::middleware::auth_middleware;
use super::rate_limit::{rate_limit_layer, rate_limit_middleware};

pub async fn run_api_server(
    addr: &str,
    db_pool: PgPool,
    jwt_secret: &str,
    web_dir: Option<&str>,
) -> Result<()> {
    let jwt_manager = Arc::new(JwtManager::new(jwt_secret));
    
    let auth_state = AuthState {
        db_pool,
        jwt_manager: jwt_manager.clone(),
    };

    // Configure CORS
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Create rate limiter for username check endpoint (10 requests per minute)
    let username_check_limiter = rate_limit_layer(10, 60);

    // Build router with protected routes
    let protected_routes = Router::new()
        .route("/api/auth/me", get(auth::get_current_user))
        .layer(middleware::from_fn_with_state(
            jwt_manager.clone(),
            auth_middleware,
        ));

    let mut app = Router::new()
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
        .layer(cors)
        .with_state(auth_state);

    // Add static file serving if web_dir is provided
    if let Some(dir) = web_dir {
        let index_path = format!("{}/index.html", dir);
        let serve_dir = ServeDir::new(dir)
            .not_found_service(ServeFile::new(&index_path));
        
        app = Router::new()
            .nest("/api", app)
            .fallback_service(serve_dir);
        
        info!("Serving static files from: {}", dir);
    }

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