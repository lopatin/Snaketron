use anyhow::{Context, Result};
use axum::{
    extract::{Extension, Json, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use bcrypt::{hash, verify, DEFAULT_COST};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{error, info};

use super::jwt::JwtManager;

#[derive(Debug, Clone)]
pub struct AuthState {
    pub db_pool: PgPool,
    pub jwt_manager: Arc<JwtManager>,
}

#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize)]
pub struct AuthResponse {
    pub token: String,
    pub user: UserInfo,
}

#[derive(Debug, Serialize)]
pub struct UserInfo {
    pub id: i32,
    pub username: String,
    pub mmr: i32,
}

#[derive(Debug, Deserialize)]
pub struct CheckUsernameRequest {
    pub username: String,
}

#[derive(Debug, Serialize)]
pub struct CheckUsernameResponse {
    pub available: bool,
    pub errors: Vec<String>,
}

#[derive(Debug)]
pub struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        error!("API error: {:?}", self.0);
        
        let (status, message) = match self.0.to_string().as_str() {
            msg if msg.contains("Username already exists") => {
                (StatusCode::CONFLICT, "Username already exists")
            }
            msg if msg.contains("Invalid username or password") => {
                (StatusCode::UNAUTHORIZED, "Invalid username or password")
            }
            msg if msg.contains("User not found") => {
                (StatusCode::NOT_FOUND, "User not found")
            }
            _ => (StatusCode::INTERNAL_SERVER_ERROR, "Internal server error"),
        };

        let body = Json(serde_json::json!({
            "error": message
        }));

        (status, body).into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

/// Validates username format
/// - Must be 3-20 characters long
/// - Can only contain alphanumeric characters, underscores, and hyphens
fn validate_username(username: &str) -> Vec<String> {
    let mut errors = Vec::new();
    
    if username.len() < 3 {
        errors.push("Username must be at least 3 characters long".to_string());
    }
    
    if username.len() > 20 {
        errors.push("Username must be at most 20 characters long".to_string());
    }
    
    if !username.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-') {
        errors.push("Username can only contain letters, numbers, underscores, and hyphens".to_string());
    }
    
    if username.starts_with('_') || username.starts_with('-') {
        errors.push("Username cannot start with underscore or hyphen".to_string());
    }
    
    if username.ends_with('_') || username.ends_with('-') {
        errors.push("Username cannot end with underscore or hyphen".to_string());
    }
    
    errors
}

pub async fn register(
    State(state): State<AuthState>,
    Json(req): Json<RegisterRequest>,
) -> Result<Json<AuthResponse>, AppError> {
    // Validate username format
    let username_errors = validate_username(&req.username);
    if !username_errors.is_empty() {
        return Err(anyhow::anyhow!("Invalid username: {}", username_errors.join(", ")).into());
    }
    
    if req.password.is_empty() || req.password.len() < 6 {
        return Err(anyhow::anyhow!("Password must be at least 6 characters").into());
    }

    // Check if username already exists
    let existing: Option<i32> = sqlx::query_scalar(
        "SELECT id FROM users WHERE username = $1"
    )
    .bind(&req.username)
    .fetch_optional(&state.db_pool)
    .await?;

    if existing.is_some() {
        return Err(anyhow::anyhow!("Username already exists").into());
    }

    // Hash password
    let password_hash = hash(&req.password, DEFAULT_COST)
        .context("Failed to hash password")?;

    // Create user
    let user: (i32, String, i32) = sqlx::query_as(
        r#"
        INSERT INTO users (username, password_hash, mmr)
        VALUES ($1, $2, 1000)
        RETURNING id, username, mmr
        "#
    )
    .bind(&req.username)
    .bind(&password_hash)
    .fetch_one(&state.db_pool)
    .await?;

    let user_info = UserInfo {
        id: user.0,
        username: user.1,
        mmr: user.2,
    };

    // Generate JWT token
    let token = state.jwt_manager.generate_token(user_info.id, &user_info.username)?;

    info!("User registered successfully: {}", user_info.username);

    Ok(Json(AuthResponse {
        token,
        user: user_info,
    }))
}

pub async fn login(
    State(state): State<AuthState>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<AuthResponse>, AppError> {
    // Find user by username
    let user: Option<(i32, String, String, i32)> = sqlx::query_as(
        "SELECT id, username, password_hash, mmr FROM users WHERE username = $1"
    )
    .bind(&req.username)
    .fetch_optional(&state.db_pool)
    .await?;

    let user = user.ok_or_else(|| anyhow::anyhow!("Invalid username or password"))?;

    // Verify password
    let is_valid = verify(&req.password, &user.2)
        .context("Failed to verify password")?;

    if !is_valid {
        return Err(anyhow::anyhow!("Invalid username or password").into());
    }

    let user_info = UserInfo {
        id: user.0,
        username: user.1,
        mmr: user.3,
    };

    // Generate JWT token
    let token = state.jwt_manager.generate_token(user_info.id, &user_info.username)?;

    info!("User logged in successfully: {}", user_info.username);

    Ok(Json(AuthResponse {
        token,
        user: user_info,
    }))
}

pub async fn get_current_user(
    State(state): State<AuthState>,
    Extension(user_id): Extension<i32>, // This will be extracted from JWT by middleware
) -> Result<Json<UserInfo>, AppError> {
    let user: Option<(i32, String, i32)> = sqlx::query_as(
        "SELECT id, username, mmr FROM users WHERE id = $1"
    )
    .bind(user_id)
    .fetch_optional(&state.db_pool)
    .await?;

    let user = user.ok_or_else(|| anyhow::anyhow!("User not found"))?;

    Ok(Json(UserInfo {
        id: user.0,
        username: user.1,
        mmr: user.2,
    }))
}

pub async fn check_username(
    State(state): State<AuthState>,
    Json(req): Json<CheckUsernameRequest>,
) -> Result<Json<CheckUsernameResponse>, AppError> {
    // Validate username format
    let mut errors = validate_username(&req.username);
    
    // If format is valid, check if username exists in database
    if errors.is_empty() {
        let existing: Option<i32> = sqlx::query_scalar(
            "SELECT id FROM users WHERE username = $1"
        )
        .bind(&req.username)
        .fetch_optional(&state.db_pool)
        .await?;
        
        if existing.is_some() {
            errors.push("Username is already taken".to_string());
        }
    }
    
    let available = errors.is_empty();
    
    info!("Username availability check for '{}': available={}, errors={:?}", 
          req.username, available, errors);
    
    Ok(Json(CheckUsernameResponse {
        available,
        errors,
    }))
}