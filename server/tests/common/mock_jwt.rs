use std::collections::HashMap;
use anyhow::Result;
use async_trait::async_trait;
use server::ws_server::{JwtVerifier, UserToken};

/// Mock JWT verifier for testing
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
        self.expected_tokens.insert(token.to_string(), UserToken { user_id });
        self
    }
    
    /// Creates a mock verifier that accepts any token
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
            // Accept any token mode
            Ok(UserToken { user_id: 1 })
        } else if let Some(user_token) = self.expected_tokens.get(token) {
            Ok(user_token.clone())
        } else {
            Err(anyhow::anyhow!("Invalid token"))
        }
    }
}