use axum::{
    extract::{Request, State},
    http::{header, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::sync::Arc;

use super::jwt::JwtManager;

pub async fn auth_middleware(
    State(jwt_manager): State<Arc<JwtManager>>,
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
    match jwt_manager.verify_token(token) {
        Ok(claims) => {
            // Parse user_id from claims
            if let Ok(user_id) = claims.sub.parse::<i32>() {
                // Insert user_id into request extensions
                request.extensions_mut().insert(user_id);
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