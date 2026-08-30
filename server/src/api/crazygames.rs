use anyhow::{Context, Result};
use axum::{
    Json,
    extract::{Extension, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use chrono::Utc;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};
use url::Url;

use crate::{
    db::models::{
        CrazyGamesAccountOutcome, CrazyGamesAccountResolution, CrazyGamesGuestPromotion,
        CrazyGamesPreferences, CrazyGamesProfile,
    },
    matchmaking_pool::MatchmakingPool,
};

use super::{auth::AuthState, middleware::AuthUser};

pub const CRAZYGAMES_AUTH_ENABLED_ENV: &str = "SNAKETRON_CRAZYGAMES_AUTH_ENABLED";
pub const CRAZYGAMES_GAME_ID_ENV: &str = "SNAKETRON_CRAZYGAMES_GAME_ID";
const CRAZYGAMES_PUBLIC_KEY_URL: &str = "https://sdk.crazygames.com/publicKey.json";
const PUBLIC_KEY_CACHE_TTL: Duration = Duration::from_secs(15 * 60);
// Invalid signatures are attacker-controlled. Limit forced rotation checks so
// sequential forged tokens cannot turn the official key endpoint into an
// outbound request oracle; a real rotation can be delayed only briefly.
const SIGNATURE_REFRESH_COOLDOWN: Duration = Duration::from_secs(30);
const PUBLIC_KEY_FETCH_FAILURE_BACKOFF: Duration = Duration::from_secs(5);
const MAX_PUBLIC_KEY_RESPONSE_BYTES: usize = 32 * 1024;
const MAX_TOKEN_BYTES: usize = 16 * 1024;
const MAX_TOKEN_LIFETIME_SECONDS: i64 = 2 * 60 * 60;
const CLOCK_SKEW_SECONDS: i64 = 60;
const MAX_PREFERENCES_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrazyGamesTokenClaims {
    pub user_id: String,
    pub game_id: String,
    pub username: String,
    pub profile_picture_url: String,
    pub iat: i64,
    pub exp: i64,
}

impl CrazyGamesTokenClaims {
    fn validate(&self, expected_game_id: &str) -> std::result::Result<(), VerifyError> {
        let now = Utc::now().timestamp();
        if self.game_id != expected_game_id
            || self.user_id.is_empty()
            || self.user_id.len() > 256
            || self.user_id.chars().any(char::is_control)
            || self.username.is_empty()
            || self.username.len() > 128
            || self.username.chars().any(char::is_control)
            || self.profile_picture_url.len() > 2048
            || Url::parse(&self.profile_picture_url)
                .ok()
                .is_none_or(|url| url.scheme() != "https" || url.host_str().is_none())
            || self.iat > now + CLOCK_SKEW_SECONDS
            || self.exp <= self.iat
            || self
                .exp
                .checked_sub(self.iat)
                .is_none_or(|lifetime| lifetime > MAX_TOKEN_LIFETIME_SECONDS)
            || self.exp <= now - CLOCK_SKEW_SECONDS
        {
            return Err(VerifyError::InvalidToken);
        }
        Ok(())
    }

    fn profile(&self) -> CrazyGamesProfile {
        CrazyGamesProfile {
            provider_user_id: self.user_id.clone(),
            username: self.username.clone(),
            avatar_url: self.profile_picture_url.clone(),
            profile_iat: self.iat,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    #[error("invalid CrazyGames token")]
    InvalidToken,
    #[error("CrazyGames public key is unavailable")]
    PublicKeyUnavailable,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PublicKeyDocument {
    public_key: String,
}

#[derive(Clone)]
struct CachedPublicKey {
    pem: String,
    fetched_at: std::time::Instant,
    generation: u64,
}

/// Verifies CrazyGames JWTs without introducing a dependency into ordinary
/// Snaketron auth. The key is fetched lazily, cached briefly, and forcibly
/// refreshed once after an RS256 signature failure to handle key rotation.
pub struct CrazyGamesJwtVerifier {
    expected_game_id: String,
    client: reqwest::Client,
    #[cfg(test)]
    public_key_url: String,
    cache: RwLock<Option<CachedPublicKey>>,
    refresh_lock: Mutex<()>,
    last_network_fetch_attempt: RwLock<Option<std::time::Instant>>,
    fetch_public_key: bool,
}

impl CrazyGamesJwtVerifier {
    pub fn new(expected_game_id: String) -> Result<Self> {
        if expected_game_id.trim().is_empty() {
            anyhow::bail!("{CRAZYGAMES_GAME_ID_ENV} must not be empty");
        }
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5))
            .build()
            .context("Failed to construct CrazyGames public-key client")?;
        Ok(Self {
            expected_game_id,
            client,
            #[cfg(test)]
            public_key_url: CRAZYGAMES_PUBLIC_KEY_URL.to_string(),
            cache: RwLock::new(None),
            refresh_lock: Mutex::new(()),
            last_network_fetch_attempt: RwLock::new(None),
            fetch_public_key: true,
        })
    }

    #[cfg(test)]
    fn with_static_key(expected_game_id: &str, public_key: &str) -> Self {
        Self {
            expected_game_id: expected_game_id.to_string(),
            client: reqwest::Client::new(),
            public_key_url: CRAZYGAMES_PUBLIC_KEY_URL.to_string(),
            cache: RwLock::new(Some(CachedPublicKey {
                pem: public_key.to_string(),
                fetched_at: std::time::Instant::now(),
                generation: 1,
            })),
            refresh_lock: Mutex::new(()),
            last_network_fetch_attempt: RwLock::new(Some(std::time::Instant::now())),
            fetch_public_key: false,
        }
    }

    #[cfg(test)]
    fn with_fetching_key_url(expected_game_id: &str, public_key_url: String) -> Self {
        Self {
            expected_game_id: expected_game_id.to_string(),
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(1))
                .timeout(Duration::from_secs(1))
                .build()
                .expect("test public-key client"),
            public_key_url,
            cache: RwLock::new(None),
            refresh_lock: Mutex::new(()),
            last_network_fetch_attempt: RwLock::new(None),
            fetch_public_key: true,
        }
    }

    fn public_key_url(&self) -> &str {
        #[cfg(test)]
        {
            &self.public_key_url
        }
        #[cfg(not(test))]
        {
            CRAZYGAMES_PUBLIC_KEY_URL
        }
    }

    async fn public_key(
        &self,
        refresh_after_generation: Option<u64>,
    ) -> std::result::Result<CachedPublicKey, VerifyError> {
        if refresh_after_generation.is_none()
            && let Some(cached) = self.cache.read().await.as_ref()
            && cached.fetched_at.elapsed() < PUBLIC_KEY_CACHE_TTL
        {
            return Ok(cached.clone());
        }

        let _refresh_guard = self.refresh_lock.lock().await;
        if let Some(cached) = self.cache.read().await.as_ref()
            && (refresh_after_generation.is_some_and(|observed| cached.generation != observed)
                || (refresh_after_generation.is_none()
                    && cached.fetched_at.elapsed() < PUBLIC_KEY_CACHE_TTL))
        {
            // Another request already refreshed this generation while we
            // waited for the single-flight lock.
            return Ok(cached.clone());
        }
        if let Some(last_attempt) = *self.last_network_fetch_attempt.read().await {
            let retry_window = if refresh_after_generation.is_some() {
                SIGNATURE_REFRESH_COOLDOWN
            } else {
                PUBLIC_KEY_FETCH_FAILURE_BACKOFF
            };
            if last_attempt.elapsed() < retry_window {
                // Return a cached key when possible so valid tokens signed by
                // it remain available. With no key, preserve a short negative
                // cache after network failure instead of retrying per request.
                return self
                    .cache
                    .read()
                    .await
                    .as_ref()
                    .cloned()
                    .ok_or(VerifyError::PublicKeyUnavailable);
            }
        }
        if !self.fetch_public_key {
            return self
                .cache
                .read()
                .await
                .as_ref()
                .cloned()
                .ok_or(VerifyError::PublicKeyUnavailable);
        }

        *self.last_network_fetch_attempt.write().await = Some(std::time::Instant::now());

        let mut response = self
            .client
            .get(self.public_key_url())
            .send()
            .await
            .map_err(|error| {
                warn!("Failed to fetch CrazyGames public key: {error}");
                VerifyError::PublicKeyUnavailable
            })?
            .error_for_status()
            .map_err(|error| {
                warn!("CrazyGames public-key endpoint failed: {error}");
                VerifyError::PublicKeyUnavailable
            })?;
        if response
            .content_length()
            .is_some_and(|size| size > MAX_PUBLIC_KEY_RESPONSE_BYTES as u64)
        {
            return Err(VerifyError::PublicKeyUnavailable);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            warn!("Failed to read CrazyGames public key: {error}");
            VerifyError::PublicKeyUnavailable
        })? {
            let next_len = body
                .len()
                .checked_add(chunk.len())
                .ok_or(VerifyError::PublicKeyUnavailable)?;
            if next_len > MAX_PUBLIC_KEY_RESPONSE_BYTES {
                return Err(VerifyError::PublicKeyUnavailable);
            }
            body.extend_from_slice(&chunk);
        }
        let document: PublicKeyDocument = serde_json::from_slice(&body).map_err(|error| {
            warn!("Invalid CrazyGames public-key document: {error}");
            VerifyError::PublicKeyUnavailable
        })?;
        DecodingKey::from_rsa_pem(document.public_key.as_bytes()).map_err(|error| {
            warn!("Invalid CrazyGames RSA public key: {error}");
            VerifyError::PublicKeyUnavailable
        })?;
        let generation = self
            .cache
            .read()
            .await
            .as_ref()
            .map_or(1, |cached| cached.generation.saturating_add(1));
        let cached = CachedPublicKey {
            pem: document.public_key.clone(),
            fetched_at: std::time::Instant::now(),
            generation,
        };
        *self.cache.write().await = Some(cached.clone());
        Ok(cached)
    }

    fn decode_with_key(
        &self,
        token: &str,
        public_key: &str,
    ) -> std::result::Result<CrazyGamesTokenClaims, jsonwebtoken::errors::Error> {
        let key = DecodingKey::from_rsa_pem(public_key.as_bytes())?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.algorithms = vec![Algorithm::RS256];
        validation.leeway = CLOCK_SKEW_SECONDS as u64;
        validation.validate_exp = true;
        validation.set_required_spec_claims(&["exp"]);
        decode::<CrazyGamesTokenClaims>(token, &key, &validation).map(|data| data.claims)
    }

    pub async fn verify(
        &self,
        token: &str,
    ) -> std::result::Result<CrazyGamesTokenClaims, VerifyError> {
        if token.is_empty() || token.len() > MAX_TOKEN_BYTES {
            return Err(VerifyError::InvalidToken);
        }
        let header = decode_header(token).map_err(|_| VerifyError::InvalidToken)?;
        if header.alg != Algorithm::RS256 {
            return Err(VerifyError::InvalidToken);
        }

        let key = self.public_key(None).await?;
        let claims = match self.decode_with_key(token, &key.pem) {
            Ok(claims) => claims,
            Err(error)
                if matches!(
                    error.kind(),
                    jsonwebtoken::errors::ErrorKind::InvalidSignature
                ) =>
            {
                let refreshed = self.public_key(Some(key.generation)).await?;
                self.decode_with_key(token, &refreshed.pem)
                    .map_err(|_| VerifyError::InvalidToken)?
            }
            Err(_) => return Err(VerifyError::InvalidToken),
        };
        claims.validate(&self.expected_game_id)?;
        Ok(claims)
    }
}

pub fn configured_verifier_from_env() -> Result<Option<Arc<CrazyGamesJwtVerifier>>> {
    let enabled = match std::env::var(CRAZYGAMES_AUTH_ENABLED_ENV) {
        Err(std::env::VarError::NotPresent) => false,
        Err(error) => return Err(error).context("Invalid CrazyGames auth enabled setting"),
        Ok(value) if value.eq_ignore_ascii_case("true") => true,
        Ok(value) if value.is_empty() || value.eq_ignore_ascii_case("false") => false,
        Ok(value) => anyhow::bail!(
            "{CRAZYGAMES_AUTH_ENABLED_ENV} must be either true or false, got '{value}'"
        ),
    };
    if !enabled {
        return Ok(None);
    }
    let game_id = std::env::var(CRAZYGAMES_GAME_ID_ENV)
        .with_context(|| format!("{CRAZYGAMES_GAME_ID_ENV} is required when auth is enabled"))?;
    Ok(Some(Arc::new(CrazyGamesJwtVerifier::new(game_id)?)))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExchangeRequest {
    pub token: String,
    #[serde(default)]
    pub guest_promotion: CrazyGamesGuestPromotion,
    #[serde(default)]
    pub initial_preferences: Option<CrazyGamesPreferences>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExchangeResponse {
    pub token: String,
    pub expires_at: i64,
    pub resolution: CrazyGamesAccountResolution,
    pub user: CrazyGamesUserInfo,
    pub preferences: CrazyGamesPreferences,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CrazyGamesUserInfo {
    pub id: i32,
    pub username: String,
    pub mmr: i32,
    pub ranked_mmr: i32,
    pub casual_mmr: i32,
    pub xp: i32,
    pub is_guest: bool,
    pub is_admin: bool,
    pub auth_source: &'static str,
    pub avatar_url: String,
    /// What this player is wearing, exactly as `UserInfo` carries it.
    ///
    /// A portal session never calls `/api/auth/me`, so this exchange is the
    /// only chance the client gets to learn it. Without these the Skins page
    /// would show Classic as equipped for the whole session while every
    /// opponent saw the real skin the server read off the account.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_skin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_base: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct PreferencesResponse {
    pub preferences: CrazyGamesPreferences,
}

#[derive(Debug)]
pub enum CrazyGamesApiError {
    Disabled,
    InvalidToken,
    PublicKeyUnavailable,
    InvalidPreferences,
    GuestLinkConsentRequired,
    NotLinked,
    Internal(anyhow::Error),
}

impl IntoResponse for CrazyGamesApiError {
    fn into_response(self) -> Response {
        let (status, message, code) = match self {
            Self::Disabled => (
                StatusCode::SERVICE_UNAVAILABLE,
                "CrazyGames account integration is unavailable",
                None,
            ),
            Self::InvalidToken => (StatusCode::UNAUTHORIZED, "Invalid CrazyGames token", None),
            Self::PublicKeyUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "CrazyGames authentication is temporarily unavailable",
                None,
            ),
            Self::InvalidPreferences => (
                StatusCode::BAD_REQUEST,
                "Invalid CrazyGames preferences",
                None,
            ),
            Self::GuestLinkConsentRequired => (
                StatusCode::CONFLICT,
                "Guest progress can be linked to this CrazyGames account",
                Some("guestLinkConsentRequired"),
            ),
            Self::NotLinked => (
                StatusCode::FORBIDDEN,
                "User is not linked to CrazyGames",
                None,
            ),
            Self::Internal(error) => {
                warn!("CrazyGames account integration error: {error:?}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error",
                    None,
                )
            }
        };
        let body = match code {
            Some(code) => serde_json::json!({ "error": message, "code": code }),
            None => serde_json::json!({ "error": message }),
        };
        let mut response = (status, Json(body)).into_response();
        apply_no_store_headers(&mut response);
        response
    }
}

fn apply_no_store_headers(response: &mut Response) {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, must-revalidate, private"),
    );
    response
        .headers_mut()
        .insert(header::PRAGMA, HeaderValue::from_static("no-cache"));
}

fn guest_candidate(headers: &HeaderMap, state: &AuthState) -> Option<i32> {
    let token = headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")?;
    let claims = state.jwt_manager.verify_token(token).ok()?;
    if !claims.is_guest || claims.matchmaking_pool != MatchmakingPool::Public {
        return None;
    }
    claims.sub.parse().ok()
}

fn validate_preferences(
    preferences: &CrazyGamesPreferences,
) -> std::result::Result<(), CrazyGamesApiError> {
    if serde_json::to_vec(preferences)
        .map_err(|_| CrazyGamesApiError::InvalidPreferences)?
        .len()
        > MAX_PREFERENCES_BYTES
    {
        return Err(CrazyGamesApiError::InvalidPreferences);
    }
    if let Some(tutorials) = &preferences.tutorial_seen
        && (tutorials.len() > 128
            || tutorials
                .keys()
                .any(|key| key.is_empty() || key.len() > 128 || key.chars().any(char::is_control)))
    {
        return Err(CrazyGamesApiError::InvalidPreferences);
    }
    if let Some(lobby) = &preferences.lobby_preferences {
        if lobby.selected_modes.len() > 32 {
            return Err(CrazyGamesApiError::InvalidPreferences);
        }
        let mut unique = HashSet::new();
        if lobby.selected_modes.iter().any(|mode| {
            mode.is_empty()
                || mode.len() > 64
                || mode.chars().any(char::is_control)
                || !unique.insert(mode)
        }) {
            return Err(CrazyGamesApiError::InvalidPreferences);
        }
    }
    Ok(())
}

pub async fn exchange(
    State(state): State<AuthState>,
    headers: HeaderMap,
    Json(request): Json<ExchangeRequest>,
) -> std::result::Result<Response, CrazyGamesApiError> {
    if let Some(preferences) = &request.initial_preferences {
        validate_preferences(preferences)?;
    }
    let verifier = state
        .crazygames_verifier
        .as_ref()
        .ok_or(CrazyGamesApiError::Disabled)?;
    let claims = verifier
        .verify(&request.token)
        .await
        .map_err(|error| match error {
            VerifyError::InvalidToken => CrazyGamesApiError::InvalidToken,
            VerifyError::PublicKeyUnavailable => CrazyGamesApiError::PublicKeyUnavailable,
        })?;
    let guest_candidate_user_id = guest_candidate(&headers, &state);
    let outcome = state
        .db
        .resolve_crazygames_account(
            &claims.profile(),
            guest_candidate_user_id,
            request.guest_promotion,
            request.initial_preferences.as_ref(),
        )
        .await
        .map_err(CrazyGamesApiError::Internal)?;
    let account = match outcome {
        CrazyGamesAccountOutcome::Resolved(account) => *account,
        CrazyGamesAccountOutcome::GuestLinkConsentRequired => {
            return Err(CrazyGamesApiError::GuestLinkConsentRequired);
        }
    };

    if let Some(user_cache) = &state.user_cache
        && let Err(error) = user_cache.replace_after_guest_upgrade(&account.user).await
    {
        // The durable identity transaction has committed. Cache failure must
        // not turn a successful exchange into a retry that looks like failure.
        warn!(
            "Failed to refresh user cache after CrazyGames exchange for {}: {}",
            account.user.id, error
        );
    }

    let token = state
        .jwt_manager
        .generate_token(account.user.id, &account.profile.username)
        .map_err(CrazyGamesApiError::Internal)?;
    let expires_at = state
        .jwt_manager
        .verify_token(&token)
        .map_err(CrazyGamesApiError::Internal)?
        .exp;
    info!(
        "Resolved verified CrazyGames account as Snaketron user {} ({:?})",
        account.user.id, account.resolution
    );
    let mut response = Json(ExchangeResponse {
        token,
        expires_at,
        resolution: account.resolution,
        user: CrazyGamesUserInfo {
            id: account.user.id,
            username: account.profile.username,
            mmr: account.user.mmr,
            ranked_mmr: account.user.ranked_mmr,
            casual_mmr: account.user.casual_mmr,
            xp: account.user.xp,
            is_guest: false,
            is_admin: super::middleware::is_admin_user(&account.user),
            auth_source: "crazygames",
            avatar_url: account.profile.avatar_url,
            selected_skin: account.user.selected_skin.clone(),
            selected_base: account.user.selected_base.clone(),
        },
        preferences: account.preferences,
    })
    .into_response();
    apply_no_store_headers(&mut response);
    Ok(response)
}

pub async fn save_preferences(
    State(state): State<AuthState>,
    Extension(auth_user): Extension<AuthUser>,
    Json(preferences): Json<CrazyGamesPreferences>,
) -> std::result::Result<Response, CrazyGamesApiError> {
    if state.crazygames_verifier.is_none() {
        return Err(CrazyGamesApiError::Disabled);
    }
    if auth_user.is_guest {
        return Err(CrazyGamesApiError::NotLinked);
    }
    validate_preferences(&preferences)?;
    let saved = state
        .db
        .save_crazygames_preferences(auth_user.user_id, &preferences)
        .await
        .map_err(|error| {
            if error.to_string().contains("not linked to CrazyGames") {
                CrazyGamesApiError::NotLinked
            } else {
                CrazyGamesApiError::Internal(error)
            }
        })?;
    let mut response = Json(PreferencesResponse { preferences: saved }).into_response();
    apply_no_store_headers(&mut response);
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Router, body::to_bytes, http::StatusCode, response::IntoResponse, routing::get};
    use jsonwebtoken::{EncodingKey, Header, encode};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    const TEST_PRIVATE_KEY: &str = r#"-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA0fkUJZ4L7ESiMaM1QTb3VymgfxIIEMyCVHI5BtlNiqH7sMi0
3PYoJif4XAV8O0iixOaQlI+dZv0qaT+hnj5wXoUfXaOOAzQUsQbwtlMeZ0psVVYy
ap7YmfCxd9LsbUimNE0yPGwg3HH3I38njUo75SDHOmTI62b+DnXr1gDCmQ4ULLmM
JnBweleemanxbxskDhMMtgupN7BjJViYETU0q6AeqWY0hA3LRtt4gaWGTk6USk/L
E4qYrAxdQqJU63eb3D0Nhs/4EJ72x6BgNXb/sQ1O/0Xv67v8BcQk7aodmvj+LfvT
sERu6+XuUdDU7Moa/+LFx6vk38IanFp5WusCUwIDAQABAoIBAEssOHlLPwuEeuj4
fG1vp1euUIaNxxN0lEh1aFM6Yxd57KkbAh2Fh1Q5xSH02MhEtfl2blaxn/GqO4/Y
txz3T7WXRWZ50rL051+Fk5JC6cSjEWvv4zbmzbc3Q+IZQonRk6dv58dYEt+5cXhk
4p0j8ZOTi6AtSv10LwqwTxGbzg+LI8alyBHOalNt1BWa3S1Xph24NpHWwez4kHuW
b0e3u5Rk0iw65u8OtzBW0829Fcvhi/AuOex07AMAEFJkZWlubbYhKgAFNYLrIvgs
9T6xFgGwMbcitEabpXjAo5Onjg7926Xwkj/4zlKCpmQGPluYyXVETv05Sjnkas3G
c75ZZckCgYEA6/kKmNhVXq2SyUNrtf4ELMPr/KOlobWTvqlhA8yzGvXxc0NUHzNb
CS78Nj5egy1T7xuhbhfHRJ+yVXCx4Powf9ExVYrsTX4kjnifJOiSQH3MVPH+QwcO
p4rAP8+/Vmt4csTQm+8+fqlSL1j5KoR39GsFLPFc4YieYo2wpbdtK9UCgYEA48sk
F4AuVihfzgAGy/xYma6Sa/6oSEGA85BknLjIz1+BJLyzmA9KuJbfo5/jskIk55w5
RHWdz9fN8X3aU/Q0H4oXqiLZeEWjI4BxQlSrWTVj4EFR5PBJeTlGxkpo7ou1zdlO
vcPvjqjYJZd06GpQ5TbfJMBaY1p4oU5hMBlQ0YcCgYEAsJHucwZVgv3ge0c+vrRv
TUvhNm8Bjio/fohhdqViU8c6v6Peu2YDNbD5umEa+Y8eYinLtoSGb/wLRlGIWK79
QXc4Mik8vpOoKQ9rDgQVA7rd/aYCOwd52LZDOrxqEPFj9IT/D9+KZN6wB4vNDhqH
Y9X8zm9gr8Y5tccOKkJBp20CgYEAnetk2A37Eavnzy5hh+Unn1NRGyFulLkkprZB
qgzI2ksBgvB3KUHgsVuXKx5bgmcsoozBft5zS3X2xiZTx8QSppLbmQ2T6jeMw731
xuBf8fZ7iSp/ldGnfizhDfLkEAw3O8AdQJ2nZCVVw6neWInsDxwdUqMvhpVf76Qg
6HGEf90CgYB39NYGiBEuoqsc05P2xrKmmy9jbqifmFHpo5HX2dS5II2st0YVb7Ry
TXX3uIt8x1/XA7OYMmV85ZLNM0sArOZaVVAaqSmjgda3wdHGDDA3ikB+zNhARzs2
eVXr0jfz5DkADm1FdWRGdMNvL+1OJmGkalvIY3ts1yyzScgrGenMBg==
-----END RSA PRIVATE KEY-----"#;
    const TEST_PUBLIC_KEY: &str = r#"-----BEGIN RSA PUBLIC KEY-----
MIIBCgKCAQEA0fkUJZ4L7ESiMaM1QTb3VymgfxIIEMyCVHI5BtlNiqH7sMi03PYo
Jif4XAV8O0iixOaQlI+dZv0qaT+hnj5wXoUfXaOOAzQUsQbwtlMeZ0psVVYyap7Y
mfCxd9LsbUimNE0yPGwg3HH3I38njUo75SDHOmTI62b+DnXr1gDCmQ4ULLmMJnBw
eleemanxbxskDhMMtgupN7BjJViYETU0q6AeqWY0hA3LRtt4gaWGTk6USk/LE4qY
rAxdQqJU63eb3D0Nhs/4EJ72x6BgNXb/sQ1O/0Xv67v8BcQk7aodmvj+LfvTsERu
6+XuUdDU7Moa/+LFx6vk38IanFp5WusCUwIDAQAB
-----END RSA PUBLIC KEY-----"#;

    fn claims(game_id: &str, iat: i64, exp: i64) -> CrazyGamesTokenClaims {
        CrazyGamesTokenClaims {
            user_id: "User_token-123".to_string(),
            game_id: game_id.to_string(),
            username: "Player_1".to_string(),
            profile_picture_url: "https://images.crazygames.com/avatar.png".to_string(),
            iat,
            exp,
        }
    }

    fn rs256_token(claims: &CrazyGamesTokenClaims) -> String {
        encode(
            &Header::new(Algorithm::RS256),
            claims,
            &EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY.as_bytes()).expect("test private key"),
        )
        .expect("test token")
    }

    fn corrupt_signature(token: String) -> String {
        let signature_start = token.rfind('.').expect("JWT signature separator") + 1;
        let mut token = token.into_bytes();
        token[signature_start] = if token[signature_start] == b'A' {
            b'B'
        } else {
            b'A'
        };
        String::from_utf8(token).expect("JWT remains UTF-8")
    }

    #[tokio::test]
    async fn verifier_accepts_valid_rs256_token() {
        let now = Utc::now().timestamp();
        let verifier = CrazyGamesJwtVerifier::with_static_key("60112", TEST_PUBLIC_KEY);
        let verified = verifier
            .verify(&rs256_token(&claims("60112", now, now + 3600)))
            .await
            .expect("valid token");
        assert_eq!(verified.user_id, "User_token-123");
    }

    #[tokio::test]
    async fn verifier_rejects_wrong_game_expiry_and_algorithm_confusion() {
        let now = Utc::now().timestamp();
        let verifier = CrazyGamesJwtVerifier::with_static_key("60112", TEST_PUBLIC_KEY);
        assert!(matches!(
            verifier
                .verify(&rs256_token(&claims("different", now, now + 3600)))
                .await,
            Err(VerifyError::InvalidToken)
        ));
        assert!(matches!(
            verifier
                .verify(&rs256_token(&claims("60112", now - 3600, now - 120)))
                .await,
            Err(VerifyError::InvalidToken)
        ));

        let hs_token = encode(
            &Header::new(Algorithm::HS256),
            &claims("60112", now, now + 3600),
            &EncodingKey::from_secret(b"not-the-rsa-public-key"),
        )
        .expect("HS256 token");
        assert!(matches!(
            verifier.verify(&hs_token).await,
            Err(VerifyError::InvalidToken)
        ));
    }

    #[tokio::test]
    async fn key_fetches_are_bounded_during_invalid_signatures_and_outages() {
        let fetch_count = Arc::new(AtomicUsize::new(0));
        let unavailable = Arc::new(AtomicBool::new(false));
        let app = Router::new().route(
            "/public-key",
            get({
                let fetch_count = fetch_count.clone();
                let unavailable = unavailable.clone();
                move || {
                    let fetch_count = fetch_count.clone();
                    let unavailable = unavailable.clone();
                    async move {
                        fetch_count.fetch_add(1, Ordering::SeqCst);
                        if unavailable.load(Ordering::SeqCst) {
                            return (StatusCode::SERVICE_UNAVAILABLE, "unavailable")
                                .into_response();
                        }
                        Json(serde_json::json!({ "publicKey": TEST_PUBLIC_KEY })).into_response()
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test key server");
        let public_key_url = format!(
            "http://{}/public-key",
            listener.local_addr().expect("test key server address")
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve test public key")
        });

        let now = Utc::now().timestamp();
        let verifier =
            CrazyGamesJwtVerifier::with_fetching_key_url("60112", public_key_url.clone());
        let valid = rs256_token(&claims("60112", now, now + 3600));
        verifier.verify(&valid).await.expect("valid token");
        assert_eq!(fetch_count.load(Ordering::SeqCst), 1);

        // Simulate a cache older than the forced-refresh cooldown. The first
        // invalid signature may check for rotation; the next one must reuse
        // that generation instead of causing another outbound request.
        *verifier.last_network_fetch_attempt.write().await =
            Some(std::time::Instant::now() - SIGNATURE_REFRESH_COOLDOWN - Duration::from_millis(1));
        let invalid = corrupt_signature(valid.clone());
        assert!(matches!(
            verifier.verify(&invalid).await,
            Err(VerifyError::InvalidToken)
        ));
        assert_eq!(fetch_count.load(Ordering::SeqCst), 2);
        assert!(matches!(
            verifier.verify(&invalid).await,
            Err(VerifyError::InvalidToken)
        ));
        assert_eq!(fetch_count.load(Ordering::SeqCst), 2);

        // With no cached key, a provider failure is negatively cached for the
        // short backoff instead of being retried once per incoming request.
        unavailable.store(true, Ordering::SeqCst);
        let unavailable_verifier =
            CrazyGamesJwtVerifier::with_fetching_key_url("60112", public_key_url);
        assert!(matches!(
            unavailable_verifier.verify(&valid).await,
            Err(VerifyError::PublicKeyUnavailable)
        ));
        assert!(matches!(
            unavailable_verifier.verify(&valid).await,
            Err(VerifyError::PublicKeyUnavailable)
        ));
        assert_eq!(fetch_count.load(Ordering::SeqCst), 3);

        server.abort();
    }

    #[test]
    fn preference_merge_keeps_completed_tutorials_monotonic() {
        let current = CrazyGamesPreferences {
            tutorial_seen: Some(
                [("movement".to_string(), true), ("boost".to_string(), false)]
                    .into_iter()
                    .collect(),
            ),
            ..Default::default()
        };
        let incoming = CrazyGamesPreferences {
            tutorial_seen: Some(
                [("movement".to_string(), false), ("boost".to_string(), true)]
                    .into_iter()
                    .collect(),
            ),
            ..Default::default()
        };
        let merged = current.merge(&incoming);
        let tutorials = merged.tutorial_seen.as_ref().expect("tutorial snapshot");
        assert!(tutorials["movement"]);
        assert!(tutorials["boost"]);
    }

    #[test]
    fn preference_validation_rejects_duplicate_modes() {
        let preferences = CrazyGamesPreferences {
            lobby_preferences: Some(crate::db::models::CrazyGamesLobbyPreferences {
                selected_modes: vec!["solo".to_string(), "solo".to_string()],
                competitive: false,
            }),
            ..Default::default()
        };
        assert!(validate_preferences(&preferences).is_err());
    }

    #[test]
    fn exchange_request_defaults_missing_guest_promotion_to_decline() {
        let request: ExchangeRequest = serde_json::from_value(serde_json::json!({
            "token": "provider-token"
        }))
        .expect("request without guestPromotion");
        assert_eq!(request.guest_promotion, CrazyGamesGuestPromotion::Decline);

        for (value, expected) in [
            ("check", CrazyGamesGuestPromotion::Check),
            ("allow", CrazyGamesGuestPromotion::Allow),
            ("decline", CrazyGamesGuestPromotion::Decline),
        ] {
            let request: ExchangeRequest = serde_json::from_value(serde_json::json!({
                "token": "provider-token",
                "guestPromotion": value
            }))
            .expect("valid guestPromotion decision");
            assert_eq!(request.guest_promotion, expected);
        }
    }

    #[tokio::test]
    async fn guest_link_consent_response_is_stable_conflict_and_no_store() {
        let response = CrazyGamesApiError::GuestLinkConsentRequired.into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        assert_eq!(
            response
                .headers()
                .get(header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some("no-cache, no-store, must-revalidate, private")
        );
        assert_eq!(
            response
                .headers()
                .get(header::PRAGMA)
                .and_then(|value| value.to_str().ok()),
            Some("no-cache")
        );
        let body = to_bytes(response.into_body(), 1024)
            .await
            .expect("read consent response");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("consent response JSON");
        assert_eq!(body["code"], "guestLinkConsentRequired");
    }

    #[test]
    fn exchange_response_contract_is_camel_case() {
        let value = serde_json::to_value(ExchangeResponse {
            token: "internal-token".to_string(),
            expires_at: 42,
            resolution: CrazyGamesAccountResolution::GuestClaimed,
            user: CrazyGamesUserInfo {
                id: 7,
                username: "Exact.Name".to_string(),
                mmr: 1_000,
                ranked_mmr: 1_010,
                casual_mmr: 990,
                xp: 12,
                is_guest: false,
                is_admin: false,
                auth_source: "crazygames",
                avatar_url: "https://images.crazygames.com/avatar.png".to_string(),
                selected_skin: Some("aurora@1".to_string()),
                selected_base: Some("base:aurora@1".to_string()),
            },
            preferences: CrazyGamesPreferences::default(),
        })
        .expect("serialize response");
        assert_eq!(value["expiresAt"], 42);
        assert_eq!(value["resolution"], "guestClaimed");
        assert_eq!(value["user"]["isGuest"], false);
        assert_eq!(value["user"]["isAdmin"], false);
        assert_eq!(value["user"]["authSource"], "crazygames");
        assert_eq!(
            value["user"]["avatarUrl"],
            "https://images.crazygames.com/avatar.png"
        );
        assert!(value["user"].get("ranked_mmr").is_none());
        assert_eq!(value["user"]["rankedMmr"], 1_010);
        // The account is the only place equipment lives, and a portal session
        // never calls `/api/auth/me`, so this response has to carry it.
        assert_eq!(value["user"]["selectedSkin"], "aurora@1");
        assert_eq!(value["user"]["selectedBase"], "base:aurora@1");
    }

    #[test]
    fn exchange_response_omits_equipment_the_account_does_not_have() {
        let value = serde_json::to_value(CrazyGamesUserInfo {
            id: 7,
            username: "Exact.Name".to_string(),
            mmr: 1_000,
            ranked_mmr: 1_010,
            casual_mmr: 990,
            xp: 12,
            is_guest: false,
            is_admin: false,
            auth_source: "crazygames",
            avatar_url: "https://images.crazygames.com/avatar.png".to_string(),
            selected_skin: None,
            selected_base: None,
        })
        .expect("serialize user");
        // Absent rather than null, matching `UserInfo` — the client reads a
        // missing slot as "wearing the default", never as an explicit clear.
        assert!(value.get("selectedSkin").is_none());
        assert!(value.get("selectedBase").is_none());
    }
}
