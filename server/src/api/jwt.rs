use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{Duration, Utc};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::ws_server::{JwtVerifier, UserToken};

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,      // Subject (user_id as string)
    pub username: String, // Username
    pub exp: i64,         // Expiration time
    pub iat: i64,         // Issued at
    #[serde(default)]
    pub is_guest: bool, // Whether this is a guest user
}

pub struct JwtManager {
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    algorithm: Algorithm,
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
        let now = Utc::now();
        let exp = now + Duration::hours(24); // Token expires in 24 hours

        let claims = Claims {
            sub: user_id.to_string(),
            username: username.to_string(),
            exp: exp.timestamp(),
            iat: now.timestamp(),
            is_guest,
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
        })
    }
}
