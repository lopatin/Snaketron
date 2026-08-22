use anyhow::{Context, Result};
use axum::{
    extract::{Extension, Json, Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use bcrypt::{DEFAULT_COST, hash, verify};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::db::Database;
use crate::db::models::User;
use crate::factory_service::{
    FactoryCredentialEnvelope, FactoryServiceAuth, FactoryServiceCredential,
    issue_factory_service_credential,
};
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
    /// Where texture pixels live, when a deployment stores any.
    ///
    /// The upload route needs it: it puts the bytes away and records a job
    /// naming their digest, so the worker reads from the store rather than
    /// being handed megabytes through a queue. `None` is a deployment that
    /// accepts no textures, and the route says so rather than half-working.
    pub texture_store: Option<Arc<dyn crate::texture_store::TextureStore>>,
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

/// Side-effect-free authorization envelope for the scheduled Skin Factory.
///
/// This is intentionally derived from the same database-loaded `AuthUser`
/// used by the mutating skin and texture routes.  It is not a claim embedded
/// in the bearer token and it does not create a canary object.  Operators can
/// therefore prove that the dedicated account has the write capabilities the
/// factory needs while also proving that it has no publication authority.
#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FactoryCapabilitiesResponse {
    pub schema_version: u32,
    pub identity: FactoryServiceIdentity,
    pub credential: FactoryCredentialEnvelope,
    pub capabilities: FactoryServiceCapabilities,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FactoryServiceIdentity {
    pub user_id: i32,
    pub username: String,
    pub registered_account: bool,
    pub is_guest: bool,
    pub is_admin: bool,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FactoryServiceCapabilities {
    pub create_private_skins: bool,
    pub create_evaluation_skins: bool,
    pub upload_private_forge_textures: bool,
    pub request_publication_review: bool,
    pub publish_skins: bool,
    pub administer_skins: bool,
}

fn factory_capabilities(
    auth_user: &AuthUser,
    factory_auth: &FactoryServiceAuth,
    texture_store_enabled: bool,
) -> FactoryCapabilitiesResponse {
    let registered_account = !auth_user.is_guest;
    FactoryCapabilitiesResponse {
        schema_version: 1,
        identity: FactoryServiceIdentity {
            user_id: auth_user.user_id,
            username: auth_user.username.clone(),
            registered_account,
            is_guest: auth_user.is_guest,
            is_admin: auth_user.is_admin,
        },
        credential: FactoryCredentialEnvelope {
            credential_type: "factoryService",
            credential_id: factory_auth.credential_id.clone(),
            revocable: true,
            expires_at: None,
        },
        capabilities: FactoryServiceCapabilities {
            // These mirror the durable-account checks at the start of
            // `skins::create_skin` and `textures::ingest_forge_manifest`.
            create_private_skins: registered_account,
            create_evaluation_skins: registered_account,
            upload_private_forge_textures: registered_account && texture_store_enabled,
            request_publication_review: registered_account,
            // Both mutation routes live behind `admin_middleware`, whose
            // decision is the same DB-derived `is_admin` bit exposed here.
            publish_skins: auth_user.is_admin,
            administer_skins: auth_user.is_admin,
        },
    }
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

/// Report the exact least-privilege envelope of an authenticated factory
/// account without writing a skin, texture, job, or publication request.
pub async fn get_factory_capabilities(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    factory_auth: Option<Extension<FactoryServiceAuth>>,
) -> Response {
    let Some(Extension(factory_auth)) = factory_auth else {
        let mut response = (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({
                "error": "A dedicated factory service credential is required"
            })),
        )
            .into_response();
        super::middleware::apply_private_no_store(&mut response);
        return response;
    };
    let mut response = Json(factory_capabilities(
        &auth_user,
        &factory_auth,
        state.texture_store.is_some(),
    ))
    .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, must-revalidate, private"),
    );
    response
        .headers_mut()
        .insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
    response
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateFactoryCredentialRequest {
    pub user_id: i32,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FactoryCredentialIssueResponse {
    /// Returned once. The database stores only its digest.
    pub token: String,
    pub credential: FactoryCredentialEnvelope,
    pub user_id: i32,
    pub username: String,
    pub created_at: String,
}

fn private_json<T: Serialize>(status: StatusCode, value: T) -> Response {
    let mut response = (status, Json(value)).into_response();
    super::middleware::apply_private_no_store(&mut response);
    response
}

fn credential_issue_response(
    issued: crate::factory_service::IssuedFactoryServiceCredential,
    user: &User,
) -> Response {
    private_json(
        StatusCode::CREATED,
        FactoryCredentialIssueResponse {
            token: issued.token,
            credential: FactoryCredentialEnvelope {
                credential_type: "factoryService",
                credential_id: issued.record.credential_id,
                revocable: true,
                expires_at: None,
            },
            user_id: user.id,
            username: user.username.clone(),
            created_at: issued.record.created_at.to_rfc3339(),
        },
    )
}

async fn eligible_factory_user(state: &AuthState, user_id: i32) -> Result<User, Response> {
    let user = match state.db.get_user_by_id(user_id).await {
        Ok(Some(user)) => user,
        Ok(None) => {
            return Err(private_json(
                StatusCode::NOT_FOUND,
                serde_json::json!({ "error": "Factory service account not found" }),
            ));
        }
        Err(error) => {
            tracing::error!(?error, "failed to load factory service account");
            return Err(private_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::json!({ "error": "Authentication service unavailable" }),
            ));
        }
    };
    if user.is_guest || user.is_stress_test || is_admin_user(&user) {
        return Err(private_json(
            StatusCode::UNPROCESSABLE_ENTITY,
            serde_json::json!({
                "error": "Factory service account must be registered, non-stress, and non-admin"
            }),
        ));
    }
    Ok(user)
}

/// Provision a durable service credential for an existing least-privilege
/// account. This route is admin-only; the raw token is returned exactly once.
pub async fn create_factory_credential(
    State(state): State<AuthState>,
    Extension(actor): Extension<AuthUser>,
    Json(request): Json<CreateFactoryCredentialRequest>,
) -> Response {
    let user = match eligible_factory_user(&state, request.user_id).await {
        Ok(user) => user,
        Err(response) => return response,
    };
    let issued = issue_factory_service_credential(user.id, actor.user_id, Utc::now());
    if let Err(error) = state
        .db
        .create_factory_service_credential(&issued.record)
        .await
    {
        tracing::error!(?error, "failed to persist factory service credential");
        return private_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({ "error": "Credential provisioning failed" }),
        );
    }
    credential_issue_response(issued, &user)
}

/// Atomically replace one credential. The old secret is revoked in the same
/// DynamoDB transaction that activates the new digest.
pub async fn rotate_factory_credential(
    State(state): State<AuthState>,
    Extension(actor): Extension<AuthUser>,
    Path(credential_id): Path<String>,
) -> Response {
    let current: FactoryServiceCredential = match state
        .db
        .get_factory_service_credential(&credential_id)
        .await
    {
        Ok(Some(record)) => record,
        Ok(None) => {
            return private_json(
                StatusCode::NOT_FOUND,
                serde_json::json!({ "error": "Factory service credential not found" }),
            );
        }
        Err(error) => {
            tracing::error!(?error, "failed to load factory service credential");
            return private_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                serde_json::json!({ "error": "Authentication service unavailable" }),
            );
        }
    };
    if current.revoked_at.is_some() {
        return private_json(
            StatusCode::CONFLICT,
            serde_json::json!({ "error": "Factory service credential is already revoked" }),
        );
    }
    let user = match eligible_factory_user(&state, current.user_id).await {
        Ok(user) => user,
        Err(response) => return response,
    };
    let now = Utc::now();
    let replacement = issue_factory_service_credential(user.id, actor.user_id, now);
    if let Err(error) = state
        .db
        .rotate_factory_service_credential(&credential_id, &replacement.record, actor.user_id, now)
        .await
    {
        tracing::error!(?error, "failed to rotate factory service credential");
        return private_json(
            StatusCode::CONFLICT,
            serde_json::json!({ "error": "Credential rotation conflicted; retry from current state" }),
        );
    }
    credential_issue_response(replacement, &user)
}

/// Revoke a credential without issuing a replacement. This is idempotent.
pub async fn revoke_factory_credential(
    State(state): State<AuthState>,
    Extension(actor): Extension<AuthUser>,
    Path(credential_id): Path<String>,
) -> Response {
    if let Err(error) = state
        .db
        .revoke_factory_service_credential(&credential_id, actor.user_id, Utc::now())
        .await
    {
        tracing::error!(?error, "failed to revoke factory service credential");
        return private_json(
            StatusCode::NOT_FOUND,
            serde_json::json!({ "error": "Factory service credential not found" }),
        );
    }
    private_json(
        StatusCode::OK,
        serde_json::json!({ "credentialId": credential_id, "revoked": true }),
    )
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

#[cfg(test)]
mod factory_capability_tests {
    use super::*;

    fn account(guest: bool, admin: bool) -> AuthUser {
        AuthUser {
            user_id: 41,
            username: "skin-factory".to_string(),
            is_guest: guest,
            is_admin: admin,
        }
    }

    fn credential() -> FactoryServiceAuth {
        FactoryServiceAuth {
            credential_id: "0123456789abcdef0123456789abcdef".to_string(),
        }
    }

    #[test]
    fn registered_non_admin_has_factory_writes_but_no_publish_authority() {
        let report = factory_capabilities(&account(false, false), &credential(), true);

        assert!(report.identity.registered_account);
        assert!(!report.identity.is_guest);
        assert!(!report.identity.is_admin);
        assert!(report.capabilities.create_private_skins);
        assert!(report.capabilities.create_evaluation_skins);
        assert!(report.capabilities.upload_private_forge_textures);
        assert!(report.capabilities.request_publication_review);
        assert!(!report.capabilities.publish_skins);
        assert!(!report.capabilities.administer_skins);
        assert_eq!(report.credential.credential_type, "factoryService");
        assert!(report.credential.revocable);
        assert_eq!(report.credential.expires_at, None);

        let wire = serde_json::to_value(report).expect("capability envelope serializes");
        assert_eq!(wire["schemaVersion"], 1);
        assert_eq!(wire["identity"]["registeredAccount"], true);
        assert_eq!(wire["capabilities"]["createEvaluationSkins"], true);
        assert_eq!(wire["capabilities"]["publishSkins"], false);
        assert_eq!(wire["credential"]["credentialType"], "factoryService");
        assert_eq!(wire["credential"]["expiresAt"], serde_json::Value::Null);
    }

    #[test]
    fn capability_probe_fails_closed_for_guest_or_missing_texture_store() {
        let guest = factory_capabilities(&account(true, false), &credential(), true);
        assert!(!guest.identity.registered_account);
        assert!(!guest.capabilities.create_private_skins);
        assert!(!guest.capabilities.create_evaluation_skins);
        assert!(!guest.capabilities.upload_private_forge_textures);

        let storage_disabled = factory_capabilities(&account(false, false), &credential(), false);
        assert!(storage_disabled.capabilities.create_private_skins);
        assert!(!storage_disabled.capabilities.upload_private_forge_textures);
    }
}
