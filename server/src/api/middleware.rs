use axum::{
    Json,
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde_json::json;
use std::sync::Arc;

use crate::db::Database;
use crate::matchmaking_pool::MatchmakingPool;

use super::jwt::JwtManager;

/// Authenticated user information extracted from JWT token
#[derive(Clone, Debug)]
pub struct AuthUser {
    pub user_id: i32,
    pub is_guest: bool,
}

#[derive(Clone)]
pub struct AuthMiddlewareState {
    pub jwt_manager: Arc<JwtManager>,
    pub db: Arc<dyn Database>,
}

pub async fn auth_middleware(
    State(state): State<AuthMiddlewareState>,
    mut request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // Extract the Authorization header
    let auth_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok());

    let token = match auth_header {
        Some(header) if header.starts_with("Bearer ") => &header[7..],
        _ => {
            return Ok((
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "Missing or invalid authorization header" })),
            )
                .into_response());
        }
    };

    // Verify the token
    match state.jwt_manager.verify_token(token) {
        Ok(claims) => {
            // Parse user_id from claims
            if let Ok(user_id) = claims.sub.parse::<i32>() {
                let user = match state.db.get_user_by_id(user_id).await {
                    Ok(Some(user)) => user,
                    Ok(None) => {
                        return Ok((
                            StatusCode::UNAUTHORIZED,
                            Json(json!({ "error": "Invalid or expired token" })),
                        )
                            .into_response());
                    }
                    Err(_) => {
                        return Ok((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(json!({ "error": "Authentication service unavailable" })),
                        )
                            .into_response());
                    }
                };
                let database_pool = if user.is_stress_test {
                    MatchmakingPool::Stress
                } else {
                    MatchmakingPool::Public
                };
                if user.is_guest != claims.is_guest || database_pool != claims.matchmaking_pool {
                    return Ok((
                        StatusCode::UNAUTHORIZED,
                        Json(json!({ "error": "Invalid or expired token" })),
                    )
                        .into_response());
                }

                // Insert AuthUser (with both user_id and is_guest) into request extensions
                let auth_user = AuthUser {
                    user_id,
                    is_guest: user.is_guest,
                };
                request.extensions_mut().insert(auth_user);
                Ok(next.run(request).await)
            } else {
                Ok((
                    StatusCode::UNAUTHORIZED,
                    Json(json!({ "error": "Invalid token claims" })),
                )
                    .into_response())
            }
        }
        Err(_) => Ok((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Invalid or expired token" })),
        )
            .into_response()),
    }
}
