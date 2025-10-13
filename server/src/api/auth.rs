use anyhow::{Context, Result};
use axum::{
    extract::{Extension, Json, State},
    http::{StatusCode, header, HeaderValue},
    response::{IntoResponse, Response},
};
use bcrypt::{hash, verify, DEFAULT_COST};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};

use crate::db::Database;

use super::jwt::JwtManager;
use super::middleware::AuthUser;

#[derive(Clone)]
pub struct AuthState {
    pub db: Arc<dyn Database>,
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
    #[serde(rename = "isGuest")]
    pub is_guest: bool,
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

#[derive(Debug, Deserialize)]
pub struct CreateGuestRequest {
    pub nickname: String,
}

#[derive(Debug, Serialize)]
pub struct CreateGuestResponse {
    pub token: String,
    pub user: GuestUserInfo,
}

#[derive(Debug, Serialize)]
pub struct GuestUserInfo {
    pub id: i32,
    pub username: String,
    pub mmr: i32,
    #[serde(rename = "isGuest")]
    pub is_guest: bool,
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
) -> Result<Response, AppError> {
    // Validate username format
    let username_errors = validate_username(&req.username);
    if !username_errors.is_empty() {
        return Err(anyhow::anyhow!("Invalid username: {}", username_errors.join(", ")).into());
    }

    if req.password.is_empty() || req.password.len() < 6 {
        return Err(anyhow::anyhow!("Password must be at least 6 characters").into());
    }

    // Check if username already exists
    let existing_user = state.db.get_user_by_username(&req.username).await?;
    if existing_user.is_some() {
        return Err(anyhow::anyhow!("Username already exists").into());
    }

    // Hash password
    let password_hash = hash(&req.password, DEFAULT_COST)
        .context("Failed to hash password")?;

    // Create user
    let user = state.db.create_user(&req.username, &password_hash, 1000).await?;

    let user_info = UserInfo {
        id: user.id,
        username: user.username,
        mmr: user.mmr,
        is_guest: false,
    };

    // Generate JWT token
    let token = state.jwt_manager.generate_token(user_info.id, &user_info.username)?;

    info!("User registered successfully: {}", user_info.username);

    // Build response with cache-control headers
    let mut response = Json(AuthResponse {
        token,
        user: user_info,
    }).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, must-revalidate, private")
    );
    response.headers_mut().insert(
        header::PRAGMA,
        HeaderValue::from_static("no-cache")
    );

    Ok(response)
}

pub async fn login(
    State(state): State<AuthState>,
    Json(req): Json<LoginRequest>,
) -> Result<Response, AppError> {
    // Find user by username
    let user = state.db.get_user_by_username(&req.username).await?
        .ok_or_else(|| anyhow::anyhow!("Invalid username or password"))?;

    // Verify password
    let is_valid = verify(&req.password, &user.password_hash)
        .context("Failed to verify password")?;

    if !is_valid {
        return Err(anyhow::anyhow!("Invalid username or password").into());
    }

    let user_info = UserInfo {
        id: user.id,
        username: user.username,
        mmr: user.mmr,
        is_guest: false,
    };

    // Generate JWT token
    let token = state.jwt_manager.generate_token(user_info.id, &user_info.username)?;

    info!("User logged in successfully: {}", user_info.username);

    // Build response with cache-control headers
    let mut response = Json(AuthResponse {
        token,
        user: user_info,
    }).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, must-revalidate, private")
    );
    response.headers_mut().insert(
        header::PRAGMA,
        HeaderValue::from_static("no-cache")
    );

    Ok(response)
}

pub async fn get_current_user(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>, // Extract AuthUser from JWT by middleware
) -> Result<Response, AppError> {
    let user = state.db.get_user_by_id(auth_user.user_id).await?
        .ok_or_else(|| anyhow::anyhow!("User not found"))?;

    let user_info = UserInfo {
        id: user.id,
        username: user.username,
        mmr: user.mmr,
        is_guest: auth_user.is_guest,
    };

    // Build response with cache-control headers to prevent caching
    let mut response = Json(user_info).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, must-revalidate, private")
    );
    response.headers_mut().insert(
        header::PRAGMA,
        HeaderValue::from_static("no-cache")
    );
    response.headers_mut().insert(
        header::EXPIRES,
        HeaderValue::from_static("0")
    );

    Ok(response)
}

pub async fn check_username(
    State(state): State<AuthState>,
    Json(req): Json<CheckUsernameRequest>,
) -> Result<Json<CheckUsernameResponse>, AppError> {
    // Validate username format
    let mut errors = validate_username(&req.username);
    
    // If format is valid, check if username exists in database
    if errors.is_empty() {
        let existing_user = state.db.get_user_by_username(&req.username).await?;
        if existing_user.is_some() {
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

pub async fn create_guest(
    State(state): State<AuthState>,
    Json(req): Json<CreateGuestRequest>,
) -> Result<Response, AppError> {
    // Validate nickname (same rules as username but no uniqueness check)
    let nickname_errors = validate_username(&req.nickname);
    if !nickname_errors.is_empty() {
        return Err(anyhow::anyhow!("Invalid nickname: {}", nickname_errors.join(", ")).into());
    }

    // Generate a unique guest token (UUID-based)
    let guest_token = uuid::Uuid::new_v4().to_string();

    // Create guest user (starting MMR of 1000)
    let user = state.db.create_guest_user(&req.nickname, &guest_token, 1000).await?;

    let user_info = GuestUserInfo {
        id: user.id,
        username: user.username.clone(),
        mmr: user.mmr,
        is_guest: true,
    };

    // Generate JWT token (includes guest flag)
    let token = state.jwt_manager.generate_token_with_guest(user_info.id, &user_info.username, true)?;

    info!("Guest user created successfully: {} (id: {})", user_info.username, user_info.id);

    // Build response with cache-control headers
    let mut response = Json(CreateGuestResponse {
        token,
        user: user_info,
    }).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, must-revalidate, private")
    );
    response.headers_mut().insert(
        header::PRAGMA,
        HeaderValue::from_static("no-cache")
    );

    Ok(response)
}