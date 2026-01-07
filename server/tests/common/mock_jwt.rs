use anyhow::Result;
use async_trait::async_trait;
use server::ws_server::{JwtVerifier, UserToken};
use std::collections::HashMap;

/// Mock JWT verifier for testing
///
/// This verifier can operate in two modes:
/// 1. **Strict mode**: Only accepts specific pre-configured tokens
///    ```
///    MockJwtVerifier::new()
///        .with_token("valid_token", 42)  // Maps "valid_token" to user_id 42
///    ```
///
/// 2. **Accept-any mode**: Accepts any token string and maps it to user_id 1
///    ```
///    MockJwtVerifier::accept_any()
///    ```
///    
/// For most tests, use accept_any mode with TestClient::authenticate(user_id).
/// For auth-specific tests, use strict mode with TestClient::authenticate_with_token(token).
pub struct MockJwtVerifier {
    expected_tokens: HashMap<String, UserToken>,
}

impl MockJwtVerifier {
    pub fn new() -> Self {
        Self {
            expected_tokens: HashMap::new(),
        }
    }

    pub fn with_token(mut self, token: &str, user_id: i32) -> Self {
        self.expected_tokens.insert(
            token.to_string(),
            UserToken {
                user_id,
                username: format!("user_{}", user_id),
                is_guest: false,
            },
        );
        self
    }

    /// Creates a mock verifier that accepts any token as user_id 1
    /// Use this for most tests where authentication details don't matter
    pub fn accept_any() -> Self {
        Self {
            expected_tokens: HashMap::new(),
        }
    }
}

#[async_trait]
impl JwtVerifier for MockJwtVerifier {
    async fn verify(&self, token: &str) -> Result<UserToken> {
        if self.expected_tokens.is_empty() {
            // Accept any token mode - try to parse token as user_id
            if let Ok(user_id) = token.parse::<i32>() {
                Ok(UserToken {
                    user_id,
                    username: format!("user_{}", user_id),
                    is_guest: false,
                })
            } else {
                // If not a number, just use user_id 1
                Ok(UserToken {
                    user_id: 1,
                    username: "user_1".to_string(),
                    is_guest: false,
                })
            }
        } else if let Some(user_token) = self.expected_tokens.get(token) {
            Ok(user_token.clone())
        } else {
            Err(anyhow::anyhow!("Invalid token"))
        }
    }
}
