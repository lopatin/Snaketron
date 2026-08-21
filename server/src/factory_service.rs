//! Durable, revocable credentials for the unattended Skin Factory.
//!
//! A factory token is not a long-lived JWT. The opaque 256-bit secret is
//! returned exactly once, only its SHA-256 digest is stored, and every request
//! reloads both the credential and its user from DynamoDB. Revocation and
//! accidental guest/admin drift therefore take effect immediately.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use rand::{RngCore, rngs::OsRng};
use serde::Serialize;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use uuid::Uuid;

pub const FACTORY_SERVICE_TOKEN_PREFIX: &str = "snk_factory_v1";
const FACTORY_SERVICE_SECRET_BYTES: usize = 32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FactoryServiceCredential {
    pub credential_id: String,
    pub user_id: i32,
    /// `sha256:<lowercase hex>`. Raw bearer material is never persisted.
    pub token_hash: String,
    pub created_at: DateTime<Utc>,
    pub created_by: i32,
    pub revoked_at: Option<DateTime<Utc>>,
    pub revoked_by: Option<i32>,
    pub replaced_by: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IssuedFactoryServiceCredential {
    pub token: String,
    pub record: FactoryServiceCredential,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FactoryServiceAuth {
    pub credential_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FactoryCredentialEnvelope {
    pub credential_type: &'static str,
    pub credential_id: String,
    pub revocable: bool,
    /// Deliberately absent: the credential remains valid until rotation or
    /// revocation instead of expiring with an interactive login session.
    pub expires_at: Option<String>,
}

pub fn issue_factory_service_credential(
    user_id: i32,
    created_by: i32,
    now: DateTime<Utc>,
) -> IssuedFactoryServiceCredential {
    let credential_id = Uuid::new_v4().simple().to_string();
    let mut secret = [0_u8; FACTORY_SERVICE_SECRET_BYTES];
    OsRng.fill_bytes(&mut secret);
    let token = format!(
        "{FACTORY_SERVICE_TOKEN_PREFIX}.{credential_id}.{}",
        URL_SAFE_NO_PAD.encode(secret)
    );
    let token_hash = factory_service_token_hash(&token);
    IssuedFactoryServiceCredential {
        token,
        record: FactoryServiceCredential {
            credential_id,
            user_id,
            token_hash,
            created_at: now,
            created_by,
            revoked_at: None,
            revoked_by: None,
            replaced_by: None,
        },
    }
}

pub fn factory_service_credential_id(token: &str) -> Option<&str> {
    let mut parts = token.split('.');
    if parts.next()? != FACTORY_SERVICE_TOKEN_PREFIX {
        return None;
    }
    let credential_id = parts.next()?;
    let secret = parts.next()?;
    if parts.next().is_some()
        || credential_id.len() != 32
        || !credential_id.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return None;
    }
    let decoded = URL_SAFE_NO_PAD.decode(secret).ok()?;
    (decoded.len() == FACTORY_SERVICE_SECRET_BYTES).then_some(credential_id)
}

pub fn factory_service_token_hash(token: &str) -> String {
    format!("sha256:{:x}", Sha256::digest(token.as_bytes()))
}

pub fn verify_factory_service_token_at(
    record: &FactoryServiceCredential,
    token: &str,
    _now: DateTime<Utc>,
) -> bool {
    if factory_service_credential_id(token) != Some(record.credential_id.as_str())
        // A recorded revocation is final even if a server clock subsequently
        // moves backwards across the audit timestamp.
        || record.revoked_at.is_some()
    {
        return false;
    }
    let candidate = factory_service_token_hash(token);
    candidate
        .as_bytes()
        .ct_eq(record.token_hash.as_bytes())
        .into()
}

/// The service credential can reach only the exact server surfaces the
/// factory driver needs. In particular it cannot equip, purchase, generate at
/// server expense, cancel a human request, or enter any administrator route.
pub fn factory_service_route_allowed(method: &str, path: &str) -> bool {
    match (method, path) {
        ("GET", "/api/factory/capabilities") => true,
        ("POST", "/api/skins") | ("POST", "/api/textures/forge") => true,
        ("GET", path) if path.starts_with("/api/skins/by-ref/") => true,
        ("GET", path) if path.starts_with("/api/textures/by-ref/") => true,
        ("GET", path) if numeric_skin_path(path) => true,
        ("PUT", path) if numeric_skin_path(path) => true,
        ("POST", path) if publication_request_path(path) => true,
        _ => false,
    }
}

fn numeric_skin_path(path: &str) -> bool {
    path.strip_prefix("/api/skins/")
        .is_some_and(|tail| !tail.is_empty() && tail.bytes().all(|byte| byte.is_ascii_digit()))
}

fn publication_request_path(path: &str) -> bool {
    path.strip_prefix("/api/skins/")
        .and_then(|tail| tail.strip_suffix("/publish-request"))
        .is_some_and(|skin_id| {
            !skin_id.is_empty() && skin_id.bytes().all(|byte| byte.is_ascii_digit())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::jwt::JwtManager;
    use chrono::Duration;

    #[test]
    fn durable_token_outlives_an_ordinary_login_but_revocation_is_immediate() {
        let now = Utc::now();
        let issued = issue_factory_service_credential(41, 7, now);
        let jwt = JwtManager::new("a-test-secret-that-is-long-enough")
            .generate_token(41, "skin-factory")
            .expect("ordinary login JWT");
        let login_expiry = JwtManager::new("a-test-secret-that-is-long-enough")
            .verify_token(&jwt)
            .expect("JWT verifies now")
            .exp;
        let after_login_expiry = DateTime::from_timestamp(login_expiry + 1, 0).unwrap();

        assert!(verify_factory_service_token_at(
            &issued.record,
            &issued.token,
            after_login_expiry
        ));
        assert!(after_login_expiry >= now + Duration::hours(24));

        let mut revoked = issued.record.clone();
        revoked.revoked_at = Some(after_login_expiry);
        revoked.revoked_by = Some(7);
        assert!(!verify_factory_service_token_at(
            &revoked,
            &issued.token,
            after_login_expiry
        ));
    }

    #[test]
    fn storage_contains_only_a_digest_and_rotation_invalidates_the_old_secret() {
        let now = Utc::now();
        let old = issue_factory_service_credential(41, 7, now);
        let replacement = issue_factory_service_credential(41, 7, now + Duration::minutes(1));
        assert!(!old.record.token_hash.contains(&old.token));
        assert!(old.record.token_hash.starts_with("sha256:"));
        assert_ne!(old.record.token_hash, replacement.record.token_hash);

        let mut rotated = old.record.clone();
        rotated.revoked_at = Some(replacement.record.created_at);
        rotated.revoked_by = Some(7);
        rotated.replaced_by = Some(replacement.record.credential_id.clone());
        assert!(!verify_factory_service_token_at(
            &rotated,
            &old.token,
            replacement.record.created_at
        ));
        assert!(verify_factory_service_token_at(
            &replacement.record,
            &replacement.token,
            replacement.record.created_at
        ));
    }

    #[test]
    fn service_scope_is_an_explicit_allowlist_without_human_or_admin_actions() {
        for (method, path) in [
            ("GET", "/api/factory/capabilities"),
            ("POST", "/api/skins"),
            ("PUT", "/api/skins/12"),
            ("POST", "/api/skins/12/publish-request"),
            ("POST", "/api/textures/forge"),
            ("GET", "/api/skins/by-ref/sha256:abc"),
        ] {
            assert!(
                factory_service_route_allowed(method, path),
                "{method} {path}"
            );
        }
        for (method, path) in [
            ("GET", "/api/auth/me"),
            ("GET", "/api/wallet"),
            ("PUT", "/api/users/me/equipped"),
            ("POST", "/api/textures/generate"),
            ("DELETE", "/api/skins/12/publish-request"),
            ("PUT", "/api/admin/skins/12/status"),
            ("POST", "/api/skins/12/purchase"),
            ("POST", "/api/skins/12/report"),
        ] {
            assert!(
                !factory_service_route_allowed(method, path),
                "{method} {path}"
            );
        }
    }
}
