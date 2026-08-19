use anyhow::{Context, Result};
use axum::{
    extract::{Extension, Json, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use bcrypt::{DEFAULT_COST, hash, verify};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::db::Database;
use crate::matchmaking_pool::MatchmakingPool;
use crate::user_cache::UserCache;

use super::jwt::JwtManager;
use super::middleware::{AuthUser, is_admin_user};

#[derive(Clone)]
pub struct AuthState {
    pub db: Arc<dyn Database>,
    pub jwt_manager: Arc<JwtManager>,
    pub user_cache: Option<UserCache>,
    pub crazygames_verifier: Option<Arc<super::crazygames::CrazyGamesJwtVerifier>>,
    /// Optional so tests and any deployment without analytics construct this
    /// unchanged. Emitting is always non-blocking and drops under pressure, so
    /// an auth handler never waits on it.
    pub analytics: Option<AnalyticsHandle>,
}

/// Everything an HTTP handler needs to emit an analytics event.
#[derive(Clone)]
pub struct AnalyticsHandle {
    pub emitter: crate::analytics::AnalyticsEmitter,
    pub origin: std::sync::Arc<crate::analytics::EventOrigin>,
}

impl AnalyticsHandle {
    /// Fire-and-forget. Deliberately returns nothing: no auth path may branch
    /// on whether analytics accepted an event.
    pub fn emit(
        &self,
        identity: crate::analytics::EventIdentity,
        payload: crate::analytics::proto::event::Payload,
    ) {
        self.emitter
            .emit(crate::analytics::envelope(&self.origin, identity, payload));
    }
}

/// Reads the client-supplied anonymous id, which is how a returning browser is
/// recognised before any account exists. Untrusted and advisory only.
fn anon_id_from(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-snaketron-anon-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
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
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct AuthResponse {
    pub token: String,
    pub user: UserInfo,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct UserInfo {
    pub id: i32,
    pub username: String,
    pub mmr: i32,
    #[serde(rename = "isGuest")]
    pub is_guest: bool,
    #[serde(rename = "isAdmin")]
    pub is_admin: bool,
    /// What this player is wearing.
    ///
    /// Carried here rather than behind its own request because the client needs
    /// it on the very first paint — the Skins page has to know which row is
    /// already equipped, and the arena has to know what to draw before the
    /// player touches anything.
    #[serde(rename = "selectedSkin", skip_serializing_if = "Option::is_none")]
    pub selected_skin: Option<String>,
    #[serde(rename = "selectedBase", skip_serializing_if = "Option::is_none")]
    pub selected_base: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CheckUsernameRequest {
    pub username: String,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct CheckUsernameResponse {
    pub available: bool,
    pub errors: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateGuestRequest {
    pub nickname: String,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct CreateGuestResponse {
    pub token: String,
    pub user: GuestUserInfo,
}

#[derive(Debug, Serialize)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct GuestUserInfo {
    pub id: i32,
    pub username: String,
    pub mmr: i32,
    #[serde(rename = "isGuest")]
    pub is_guest: bool,
    #[serde(rename = "isAdmin")]
    pub is_admin: bool,
    #[serde(rename = "matchmakingPool")]
    pub matchmaking_pool: MatchmakingPool,
}

pub const STRESS_TEST_KEY_HEADER: &str = "x-snaketron-stress-test-key";

#[derive(Debug)]
pub struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        error!("API error: {:?}", self.0);

        let (status, message) = match self.0.to_string().as_str() {
            msg if msg.contains("Username already exists") => {
                (StatusCode::CONFLICT, "Username already exists")
            }
            msg if msg.contains("Account is already registered") => {
                (StatusCode::CONFLICT, "Account is already registered")
            }
            msg if msg.contains("Invalid username or password") => {
                (StatusCode::UNAUTHORIZED, "Invalid username or password")
            }
            msg if msg.contains("Invalid or expired guest session")
                || msg.contains("Guest account not found")
                || msg.contains("Guest account has already been upgraded") =>
            {
                (StatusCode::UNAUTHORIZED, "Invalid or expired guest session")
            }
            msg if msg.contains("Stress-test guest accounts cannot be upgraded") => (
                StatusCode::FORBIDDEN,
                "Stress-test guest accounts cannot be upgraded",
            ),
            msg if msg.contains("Invalid stress test key") => {
                (StatusCode::UNAUTHORIZED, "Invalid stress test key")
            }
            msg if msg.contains("Invalid username") => {
                (StatusCode::BAD_REQUEST, "Invalid username")
            }
            msg if msg.contains("Password must be at least 6 characters") => (
                StatusCode::BAD_REQUEST,
                "Password must be at least 6 characters",
            ),
            msg if msg.contains("User not found") => (StatusCode::NOT_FOUND, "User not found"),
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
pub fn validate_username(username: &str) -> Vec<String> {
    let mut errors = Vec::new();

    if username.len() < 3 {
        errors.push("Username must be at least 3 characters long".to_string());
    }

    if username.len() > 20 {
        errors.push("Username must be at most 20 characters long".to_string());
    }

    if !username
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
    {
        errors.push(
            "Username can only contain letters, numbers, underscores, and hyphens".to_string(),
        );
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
    headers: HeaderMap,
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

    // Registration is also the guest-upgrade endpoint. The browser already
    // sends its current bearer token; fail closed when a presented token is
    // malformed or stale so an upgrade can never silently create a second ID.
    let guest_claims = match headers.get(header::AUTHORIZATION) {
        None => None,
        Some(value) => {
            let value = value
                .to_str()
                .map_err(|_| anyhow::anyhow!("Invalid or expired guest session"))?;
            let token = value
                .strip_prefix("Bearer ")
                .filter(|token| !token.is_empty())
                .ok_or_else(|| anyhow::anyhow!("Invalid or expired guest session"))?;
            let claims = state
                .jwt_manager
                .verify_token(token)
                .map_err(|_| anyhow::anyhow!("Invalid or expired guest session"))?;
            Some(claims)
        }
    };

    let upgraded_guest_id = guest_claims
        .as_ref()
        .map(|claims| {
            if !claims.is_guest {
                return Err(anyhow::anyhow!("Account is already registered"));
            }
            if claims.matchmaking_pool != MatchmakingPool::Public {
                return Err(anyhow::anyhow!(
                    "Stress-test guest accounts cannot be upgraded"
                ));
            }
            claims
                .sub
                .parse::<i32>()
                .map_err(|_| anyhow::anyhow!("Invalid or expired guest session"))
        })
        .transpose()?;

    let user = if let Some(guest_id) = upgraded_guest_id {
        let current = state
            .db
            .get_user_by_id(guest_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Guest account not found"))?;

        if current.is_stress_test {
            return Err(anyhow::anyhow!("Stress-test guest accounts cannot be upgraded").into());
        }

        if !current.is_guest {
            // If DynamoDB committed but the prior HTTP response was lost, the
            // browser still holds its old guest token. Allow an exact retry
            // only when the submitted credentials already authenticate the
            // converted account.
            let is_exact_retry = current.username == req.username
                && verify(&req.password, &current.password_hash)
                    .context("Failed to verify upgraded account password")?;
            if !is_exact_retry {
                return Err(anyhow::anyhow!("Guest account has already been upgraded").into());
            }
            current
        } else {
            let password_hash =
                hash(&req.password, DEFAULT_COST).context("Failed to hash password")?;
            state
                .db
                .upgrade_guest_to_account(guest_id, &req.username, &password_hash)
                .await?
        }
    } else {
        // Signed-out registration retains the original new-account behavior.
        if state
            .db
            .get_user_by_username(&req.username)
            .await?
            .is_some()
        {
            return Err(anyhow::anyhow!("Username already exists").into());
        }
        let password_hash = hash(&req.password, DEFAULT_COST).context("Failed to hash password")?;
        state
            .db
            .create_user(&req.username, &password_hash, 1000)
            .await?
    };

    if upgraded_guest_id.is_some()
        && let Some(user_cache) = &state.user_cache
        && let Err(cache_error) = user_cache.replace_after_guest_upgrade(&user).await
    {
        // The durable conversion already committed. A cache outage must not
        // turn that success into an apparent failure that encourages retries.
        warn!(
            "Failed to replace user cache after upgrading guest {}: {}",
            user.id, cache_error
        );
    }

    let is_admin = is_admin_user(&user);
    let user_info = UserInfo {
        id: user.id,
        username: user.username,
        mmr: user.mmr,
        is_guest: false,
        is_admin,
        selected_skin: user.selected_skin,
        selected_base: user.selected_base,
    };

    // Generate JWT token
    let token = state
        .jwt_manager
        .generate_token(user_info.id, &user_info.username)?;

    info!("User registered successfully: {}", user_info.username);

    // Build response with cache-control headers
    let mut response = Json(AuthResponse {
        token,
        user: user_info,
    })
    .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, must-revalidate, private"),
    );
    response
        .headers_mut()
        .insert(header::PRAGMA, HeaderValue::from_static("no-cache"));

    Ok(response)
}

pub async fn login(
    State(state): State<AuthState>,
    // Must precede the body extractor: `Json` consumes the request.
    headers: HeaderMap,
    Json(req): Json<LoginRequest>,
) -> Result<Response, AppError> {
    // Find user by username
    let user = state
        .db
        .get_user_by_username(&req.username)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Invalid username or password"))?;

    // Verify password
    let is_valid =
        verify(&req.password, &user.password_hash).context("Failed to verify password")?;

    if !is_valid {
        return Err(anyhow::anyhow!("Invalid username or password").into());
    }

    let is_admin = is_admin_user(&user);
    let user_info = UserInfo {
        id: user.id,
        username: user.username,
        mmr: user.mmr,
        is_guest: false,
        is_admin,
        selected_skin: user.selected_skin,
        selected_base: user.selected_base,
    };

    // Generate JWT token
    let token = state
        .jwt_manager
        .generate_token(user_info.id, &user_info.username)?;

    info!("User logged in successfully: {}", user_info.username);

    if let Some(analytics) = state.analytics.as_ref() {
        analytics.emit(
            crate::analytics::EventIdentity {
                user_id: Some(i64::from(user_info.id)),
                anon_id: anon_id_from(&headers),
                ..Default::default()
            },
            crate::analytics::proto::event::Payload::UserLogin(
                crate::analytics::proto::UserLogin {},
            ),
        );
    }

    // Build response with cache-control headers
    let mut response = Json(AuthResponse {
        token,
        user: user_info,
    })
    .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, must-revalidate, private"),
    );
    response
        .headers_mut()
        .insert(header::PRAGMA, HeaderValue::from_static("no-cache"));

    Ok(response)
}

pub async fn get_current_user(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>, // Extract AuthUser from JWT by middleware
) -> Result<Response, AppError> {
    let user = state
        .db
        .get_user_by_id(auth_user.user_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("User not found"))?;

    if user.is_guest != auth_user.is_guest {
        return Err(anyhow::anyhow!("Invalid or expired guest session").into());
    }

    let is_admin = is_admin_user(&user);
    let user_info = UserInfo {
        id: user.id,
        username: user.username,
        mmr: user.mmr,
        is_guest: user.is_guest,
        is_admin,
        selected_skin: user.selected_skin,
        selected_base: user.selected_base,
    };

    // Build response with cache-control headers to prevent caching
    let mut response = Json(user_info).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, must-revalidate, private"),
    );
    response
        .headers_mut()
        .insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
    response
        .headers_mut()
        .insert(header::EXPIRES, HeaderValue::from_static("0"));

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

    info!(
        "Username availability check for '{}': available={}, errors={:?}",
        req.username, available, errors
    );

    Ok(Json(CheckUsernameResponse { available, errors }))
}

pub async fn create_guest(
    State(state): State<AuthState>,
    headers: HeaderMap,
    Json(req): Json<CreateGuestRequest>,
) -> Result<Response, AppError> {
    // Validate nickname (same rules as username but no uniqueness check)
    let nickname_errors = validate_username(&req.nickname);
    if !nickname_errors.is_empty() {
        return Err(anyhow::anyhow!("Invalid nickname: {}", nickname_errors.join(", ")).into());
    }

    // The pool can only be elevated by the environment-scoped derived key.
    // A present but invalid header fails closed and never creates a public
    // fallback user that could enter real matchmaking.
    let matchmaking_pool = match headers.get(STRESS_TEST_KEY_HEADER) {
        None => MatchmakingPool::Public,
        Some(value) => {
            let candidate = value
                .to_str()
                .map_err(|_| anyhow::anyhow!("Invalid stress test key"))?;
            if !state.jwt_manager.verify_stress_test_key(candidate) {
                return Err(anyhow::anyhow!("Invalid stress test key").into());
            }
            MatchmakingPool::Stress
        }
    };

    // Generate a unique guest token (UUID-based)
    let guest_token = uuid::Uuid::new_v4().to_string();

    // Create guest user (starting MMR of 1000)
    let user = state
        .db
        .create_guest_user(
            &req.nickname,
            &guest_token,
            1000,
            matchmaking_pool == MatchmakingPool::Stress,
        )
        .await?;

    let user_info = GuestUserInfo {
        id: user.id,
        username: user.username.clone(),
        mmr: user.mmr,
        is_guest: true,
        is_admin: false,
        matchmaking_pool,
    };

    // Generate JWT token (includes guest flag)
    let token = state.jwt_manager.generate_token_with_guest_and_pool(
        user_info.id,
        &user_info.username,
        true,
        matchmaking_pool,
    )?;

    info!(
        "Guest user created successfully: {} (id: {}, pool: {})",
        user_info.username, user_info.id, matchmaking_pool
    );

    if let Some(analytics) = state.analytics.as_ref() {
        analytics.emit(
            crate::analytics::EventIdentity {
                user_id: Some(i64::from(user_info.id)),
                anon_id: anon_id_from(&headers),
                is_guest: true,
                is_stress_test: matchmaking_pool == MatchmakingPool::Stress,
                ..Default::default()
            },
            crate::analytics::proto::event::Payload::GuestCreated(
                crate::analytics::proto::GuestCreated {
                    mmr: i64::from(user_info.mmr),
                    matchmaking_pool: matchmaking_pool.to_string(),
                },
            ),
        );
    }

    // Build response with cache-control headers
    let mut response = Json(CreateGuestResponse {
        token,
        user: user_info,
    })
    .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, must-revalidate, private"),
    );
    response
        .headers_mut()
        .insert(header::PRAGMA, HeaderValue::from_static("no-cache"));

    Ok(response)
}
