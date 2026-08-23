use axum::{
    Json,
    extract::{Request, State},
    http::{HeaderValue, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use chrono::Utc;
use serde_json::json;
use std::sync::Arc;

use crate::db::Database;
use crate::db::models::User;
use crate::factory_service::{
    FACTORY_SERVICE_TOKEN_PREFIX, FactoryServiceAuth, factory_service_credential_id,
    factory_service_route_allowed, verify_factory_service_token_at,
};
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

enum FactoryAuthenticationError {
    Invalid,
    Unavailable,
}

async fn authenticate_factory_service(
    state: &AuthMiddlewareState,
    token: &str,
) -> Result<(AuthUser, FactoryServiceAuth), FactoryAuthenticationError> {
    let credential_id =
        factory_service_credential_id(token).ok_or(FactoryAuthenticationError::Invalid)?;
    let credential = state
        .db
        .get_factory_service_credential(credential_id)
        .await
        .map_err(|_| FactoryAuthenticationError::Unavailable)?
        .ok_or(FactoryAuthenticationError::Invalid)?;
    if !verify_factory_service_token_at(&credential, token, Utc::now()) {
        return Err(FactoryAuthenticationError::Invalid);
    }
    let user = state
        .db
        .get_user_by_id(credential.user_id)
        .await
        .map_err(|_| FactoryAuthenticationError::Unavailable)?
        .ok_or(FactoryAuthenticationError::Invalid)?;
    // A service credential is deliberately narrower than the account it is
    // attached to, and it fails closed if the durable identity ever drifts to
    // guest, stress-test, or administrator status.
    if user.is_guest || user.is_stress_test || is_admin_user(&user) {
        return Err(FactoryAuthenticationError::Invalid);
    }
    Ok((
        AuthUser {
            user_id: user.id,
            username: user.username,
            is_guest: false,
            is_admin: false,
        },
        FactoryServiceAuth {
            credential_id: credential.credential_id,
        },
    ))
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

    if token.starts_with(FACTORY_SERVICE_TOKEN_PREFIX) {
        let (auth_user, factory_auth) = match authenticate_factory_service(&state, token).await {
            Ok(auth) => auth,
            Err(FactoryAuthenticationError::Invalid) => {
                return Ok((
                    StatusCode::UNAUTHORIZED,
                    Json(json!({ "error": "Invalid or revoked factory service token" })),
                )
                    .into_response());
            }
            Err(FactoryAuthenticationError::Unavailable) => {
                return Ok((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "Authentication service unavailable" })),
                )
                    .into_response());
            }
        };
        if !factory_service_route_allowed(request.method().as_str(), request.uri().path()) {
            let mut response = (
                StatusCode::FORBIDDEN,
                Json(json!({ "error": "Factory service credential is not authorized for this route" })),
            )
                .into_response();
            apply_private_no_store(&mut response);
            return Ok(response);
        }
        request.extensions_mut().insert(auth_user);
        request.extensions_mut().insert(factory_auth);
        let mut response = next.run(request).await;
        apply_private_no_store(&mut response);
        return Ok(response);
    }

    // Verify an ordinary interactive JWT.
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

/// Identify the caller if they present a valid token, and carry on if they do
/// not.
///
/// For routes that serve everyone but serve *more* to someone signed in: the
/// Skins page is browsable anonymously, yet "my own skins" and "my own
/// unpublished draft" are questions only answerable about a known viewer. A
/// token that does not verify is ignored rather than refused, because these
/// routes are public — the worst outcome of a bad token is the anonymous view,
/// and the handlers apply their own visibility rules on top of whatever this
/// installs.
pub async fn optional_auth_middleware(
    State(state): State<AuthMiddlewareState>,
    mut request: Request,
    next: Next,
) -> Response {
    let token = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .and_then(|header| header.strip_prefix("Bearer "))
        .map(str::to_string);

    let mut identified = false;
    if let Some(token) = token.as_deref()
        && token.starts_with(FACTORY_SERVICE_TOKEN_PREFIX)
        && factory_service_route_allowed(request.method().as_str(), request.uri().path())
        && let Ok((auth_user, factory_auth)) = authenticate_factory_service(&state, token).await
    {
        request.extensions_mut().insert(auth_user);
        request.extensions_mut().insert(factory_auth);
        identified = true;
    } else if let Some(token) = token
        && let Ok(claims) = state.jwt_manager.verify_token(&token)
        && let Ok(user_id) = claims.sub.parse::<i32>()
        && let Ok(Some(user)) = state.db.get_user_by_id(user_id).await
    {
        let database_pool = if user.is_stress_test {
            MatchmakingPool::Stress
        } else {
            MatchmakingPool::Public
        };
        // The same consistency check the strict middleware makes: a token
        // whose claims disagree with the row identifies nobody.
        if user.is_guest == claims.is_guest && database_pool == claims.matchmaking_pool {
            request.extensions_mut().insert(AuthUser {
                user_id,
                username: user.username.clone(),
                is_guest: user.is_guest,
                is_admin: is_admin_user(&user),
            });
            identified = true;
        }
    }

    let mut response = next.run(request).await;
    // A response shaped by who asked must never land in a shared cache under a
    // key that does not include them.
    if identified {
        apply_private_no_store(&mut response);
    }
    response
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
