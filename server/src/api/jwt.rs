use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use hmac::{Hmac, Mac};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::sync::Arc;

use crate::matchmaking_pool::MatchmakingPool;
use crate::ws_server::{JwtVerifier, UserToken};

pub const STRESS_TEST_KEY_DERIVATION_CONTEXT: &[u8] = b"snaketron-stress-test-v1";

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,      // Subject (user_id as string)
    pub username: String, // Username
    pub exp: i64,         // Expiration time
    pub iat: i64,         // Issued at
    #[serde(default)]
    pub is_guest: bool, // Whether this is a guest user
    #[serde(default)]
    pub matchmaking_pool: MatchmakingPool,
}

pub struct JwtManager {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    algorithm: Algorithm,
    stress_test_hmac_key: Vec<u8>,
}

impl std::fmt::Debug for JwtManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtManager")
            .field("algorithm", &self.algorithm)
            .finish()
    }
}

impl JwtManager {
    pub fn new(secret: &str) -> Self {
        Self {
            encoding_key: EncodingKey::from_secret(secret.as_bytes()),
            decoding_key: DecodingKey::from_secret(secret.as_bytes()),
            algorithm: Algorithm::HS256,
            stress_test_hmac_key: secret.as_bytes().to_vec(),
        }
    }

    pub fn generate_token(&self, user_id: i32, username: &str) -> Result<String> {
        self.generate_token_with_guest(user_id, username, false)
    }

    pub fn generate_token_with_guest(
        &self,
        user_id: i32,
        username: &str,
        is_guest: bool,
    ) -> Result<String> {
        self.generate_token_with_guest_and_pool(
            user_id,
            username,
            is_guest,
            MatchmakingPool::Public,
        )
    }

    pub fn generate_token_with_guest_and_pool(
        &self,
        user_id: i32,
        username: &str,
        is_guest: bool,
        matchmaking_pool: MatchmakingPool,
    ) -> Result<String> {
        let now = Utc::now();
        let exp = now + Duration::hours(24); // Token expires in 24 hours

        let claims = Claims {
            sub: user_id.to_string(),
            username: username.to_string(),
            exp: exp.timestamp(),
            iat: now.timestamp(),
            is_guest,
            matchmaking_pool,
        };

        let header = Header::new(self.algorithm);
        encode(&header, &claims, &self.encoding_key).context("Failed to encode JWT token")
    }

    pub fn verify_token(&self, token: &str) -> Result<Claims> {
        let validation = Validation::new(self.algorithm);
        let token_data = decode::<Claims>(token, &self.decoding_key, &validation)
            .context("Failed to decode JWT token")?;

        Ok(token_data.claims)
    }

    /// Verify the derived stress-test admission credential without comparing
    /// secret material with ordinary string equality. The workflow receives
    /// only this one-way HMAC value; it never sends the JWT signing key over
    /// the public API.
    pub fn verify_stress_test_key(&self, candidate_hex: &str) -> bool {
        let Ok(candidate) = hex::decode(candidate_hex) else {
            return false;
        };
        let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(&self.stress_test_hmac_key) else {
            return false;
        };
        mac.update(STRESS_TEST_KEY_DERIVATION_CONTEXT);
        mac.verify_slice(&candidate).is_ok()
    }
}

// Implementation of JwtVerifier trait for production use
pub struct ProductionJwtVerifier {
    jwt_manager: Arc<JwtManager>,
}

impl ProductionJwtVerifier {
    pub fn new(jwt_manager: Arc<JwtManager>) -> Self {
        Self { jwt_manager }
    }
}

#[async_trait]
impl JwtVerifier for ProductionJwtVerifier {
    async fn verify(&self, token: &str) -> Result<UserToken> {
        let claims = self.jwt_manager.verify_token(token)?;
        let user_id = claims
            .sub
            .parse::<i32>()
            .context("Failed to parse user_id from JWT claims")?;

        Ok(UserToken {
            user_id,
            username: claims.username,
            is_guest: claims.is_guest,
            matchmaking_pool: claims.matchmaking_pool,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stress_test_key_is_a_one_way_hmac_and_is_verified_exactly() {
        let secret = "test-secret-with-at-least-thirty-two-bytes";
        let manager = JwtManager::new(secret);
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(STRESS_TEST_KEY_DERIVATION_CONTEXT);
        let derived = hex::encode(mac.finalize().into_bytes());

        assert!(manager.verify_stress_test_key(&derived));
        assert!(!manager.verify_stress_test_key(secret));
        assert!(!manager.verify_stress_test_key(&format!("{derived}00")));
    }

    #[test]
    fn legacy_claims_default_to_the_public_pool() {
        #[derive(Serialize)]
        struct LegacyClaims<'a> {
            sub: &'a str,
            username: &'a str,
            exp: i64,
            iat: i64,
            is_guest: bool,
        }

        let secret = "test-secret-with-at-least-thirty-two-bytes";
        let now = Utc::now();
        let token = encode(
            &Header::new(Algorithm::HS256),
            &LegacyClaims {
                sub: "7",
                username: "legacy",
                exp: (now + Duration::hours(1)).timestamp(),
                iat: now.timestamp(),
                is_guest: true,
            },
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap();

        let claims = JwtManager::new(secret).verify_token(&token).unwrap();
        assert_eq!(claims.matchmaking_pool, MatchmakingPool::Public);
    }
}
