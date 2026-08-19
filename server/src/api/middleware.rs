use axum::{
    Json,
    extract::{Request, State},
    http::{HeaderValue, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde_json::json;
use std::sync::Arc;

use crate::db::Database;
use crate::db::models::User;
use crate::matchmaking_pool::MatchmakingPool;

use super::jwt::JwtManager;

/// Authenticated user information extracted from JWT token
#[derive(Clone, Debug)]
pub struct AuthUser {
    pub user_id: i32,
    pub username: String,
    pub is_guest: bool,
    pub is_admin: bool,
}

pub const ADMIN_USER_IDS_ENV: &str = "SNAKETRON_ADMIN_USER_IDS";

/// Resolve administrative access exclusively from the database-loaded user.
///
/// Entries in `SNAKETRON_ADMIN_USER_IDS` are comma-separated durable numeric
/// user IDs. Guests and stress-test users can never be administrators, even if
/// their ID appears in the list.
pub fn is_admin_user(user: &User) -> bool {
    std::env::var(ADMIN_USER_IDS_ENV)
        .ok()
        .is_some_and(|value| admin_allowlist_matches(&value, user))
}

fn admin_allowlist_matches(value: &str, user: &User) -> bool {
    if user.is_guest || user.is_stress_test {
        return false;
    }

    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .any(|entry| entry.parse::<i32>().ok() == Some(user.id))
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

                // Authorization is derived from the current database row, not
                // from long-lived token claims or client-controlled data.
                let auth_user = AuthUser {
                    user_id,
                    username: user.username.clone(),
                    is_guest: user.is_guest,
                    is_admin: is_admin_user(&user),
                };
                request.extensions_mut().insert(auth_user);
                let mut response = next.run(request).await;
                apply_private_no_store(&mut response);
                Ok(response)
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

/// Require the authenticated identity installed by [`auth_middleware`] to be
/// present in the server-side allowlist.
pub async fn admin_middleware(request: Request, next: Next) -> Response {
    let is_admin = request
        .extensions()
        .get::<AuthUser>()
        .is_some_and(|user| user.is_admin);
    if !is_admin {
        let mut response = (
            StatusCode::FORBIDDEN,
            Json(json!({ "error": "Administrator access required" })),
        )
            .into_response();
        apply_private_no_store(&mut response);
        return response;
    }

    let mut response = next.run(request).await;
    apply_private_no_store(&mut response);
    response
}

pub fn apply_private_no_store(response: &mut Response) {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, must-revalidate, private"),
    );
    response
        .headers_mut()
        .insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn user(id: i32, username: &str) -> User {
        User {
            id,
            username: username.to_string(),
            password_hash: String::new(),
            mmr: 1_000,
            ranked_mmr: 1_000,
            casual_mmr: 1_000,
            xp: 0,
            games_played: 0,
            created_at: Utc::now(),
            is_guest: false,
            guest_token: None,
            is_stress_test: false,
            auth_provider: None,
            crazygames_user_id: None,
            profile_picture_url: None,
            profile_iat: None,
            selected_skin: None,
            selected_base: None,
        }
    }

    #[test]
    fn admin_allowlist_accepts_only_numeric_user_ids() {
        let allowlist = "7, 11";
        assert!(admin_allowlist_matches(allowlist, &user(7, "someone")));
        assert!(admin_allowlist_matches(
            allowlist,
            &user(11, "someone-else")
        ));
        assert!(!admin_allowlist_matches(allowlist, &user(12, "ordinary")));
        assert!(!admin_allowlist_matches("admin", &user(9, "admin")));
        assert!(!admin_allowlist_matches(
            "username:admin",
            &user(9, "admin")
        ));
        assert!(!admin_allowlist_matches("id:9", &user(9, "admin")));
    }

    #[test]
    fn guest_and_stress_users_are_never_admins() {
        let mut guest = user(7, "admin");
        guest.is_guest = true;
        assert!(!admin_allowlist_matches("7", &guest));
        guest.is_guest = false;
        guest.is_stress_test = true;
        assert!(!admin_allowlist_matches("7", &guest));
    }
}
