use anyhow::{Context, Result};
use std::sync::Arc;
use server::{
    game_server::{GameServer, GameServerBuilder},
    ws_server::JwtVerifier,
};
use super::{mock_jwt::MockJwtVerifier, test_database::TestDatabaseGuard};
use tracing::info;

/// A test environment that manages game servers and database isolation
pub struct TestEnvironment {
    /// Test database (automatically cleaned up)
    db_guard: TestDatabaseGuard,
    /// Game servers running in this environment
    servers: Vec<GameServer>,
    /// User IDs created for testing
    user_ids: Vec<i32>,
    /// Test name for debugging
    test_name: String,
}

impl TestEnvironment {
    /// Get the WebSocket address for a server by index
    pub fn ws_addr(&self, index: usize) -> Option<String> {
        self.servers.get(index).map(|s| format!("ws://{}", s.ws_addr()))
    }
    
    /// Get the gRPC address for a server by index
    pub fn grpc_addr(&self, index: usize) -> Option<String> {
        self.servers.get(index)
            .and_then(|s| s.grpc_addr())
            .map(|addr| addr.to_string())
    }
    
    /// Get a reference to a server by index
    pub fn server(&self, index: usize) -> Option<&GameServer> {
        self.servers.get(index)
    }
    
    /// Get all user IDs created for this test
    pub fn user_ids(&self) -> &[i32] {
        &self.user_ids
    }
    
    /// Shutdown all servers and clean up the database
    pub async fn shutdown(mut self) -> Result<()> {
        info!("Shutting down test environment: {}", self.test_name);
        
        // Shutdown all servers
        for server in self.servers.drain(..) {
            server.shutdown().await?;
        }
        
        // Database cleanup happens automatically when db_guard is dropped
        Ok(())
    }
}

/// Builder for creating test environments with proper isolation
pub struct TestEnvironmentBuilder {
    test_name: String,
    num_servers: usize,
    num_users: usize,
    use_distributed_broker: bool,
    jwt_verifier: Option<Arc<dyn JwtVerifier>>,
}

impl TestEnvironmentBuilder {
    /// Create a new test environment builder
    pub fn new(test_name: &str) -> Self {
        Self {
            test_name: test_name.to_string(),
            num_servers: 1,
            num_users: 0,
            use_distributed_broker: false,
            jwt_verifier: None,
        }
    }
    
    /// Set the number of servers to create
    pub fn with_servers(mut self, count: usize) -> Self {
        self.num_servers = count;
        self
    }
    
    /// Set the number of test users to create
    pub fn with_users(mut self, count: usize) -> Self {
        self.num_users = count;
        self
    }
    
    /// Enable distributed broker for multi-server tests
    pub fn with_distributed_broker(mut self) -> Self {
        self.use_distributed_broker = true;
        self
    }
    
    /// Set a custom JWT verifier (defaults to MockJwtVerifier)
    pub fn with_jwt_verifier(mut self, verifier: Arc<dyn JwtVerifier>) -> Self {
        self.jwt_verifier = Some(verifier);
        self
    }
    
    /// Build and start the test environment
    pub async fn build(self) -> Result<TestEnvironment> {
        info!("Building test environment for: {}", self.test_name);
        
        // Create isolated test database
        let db_guard = TestDatabaseGuard::new(&self.test_name).await
            .context("Failed to create test database")?;
        
        // Get default JWT verifier if not provided
        let jwt_verifier = self.jwt_verifier
            .unwrap_or_else(|| Arc::new(MockJwtVerifier::accept_any()) as Arc<dyn JwtVerifier>);
        
        // Create servers
        let mut servers = Vec::new();
        for i in 0..self.num_servers {
            let server = GameServerBuilder::new()
                .with_db_url(db_guard.url().to_string())
                .with_region("test-region".to_string())
                .with_jwt_verifier(jwt_verifier.clone())
                .with_distributed_broker(self.use_distributed_broker)
                .build()
                .await
                .context(format!("Failed to start server {}", i))?;
            
            info!("Started server {} with ID {} on {}", i, server.id(), server.ws_addr());
            
            servers.push(server);
        }
        
        // Create test users
        let mut user_ids = Vec::new();
        if self.num_users > 0 {
            let pool = db_guard.pool();
            
            for i in 0..self.num_users {
                let user_id = create_test_user(pool, i).await
                    .context(format!("Failed to create test user {}", i))?;
                user_ids.push(user_id);
            }
            
            info!("Created {} test users", user_ids.len());
        }
        
        Ok(TestEnvironment {
            db_guard,
            servers,
            user_ids,
            test_name: self.test_name,
        })
    }
}

/// Create a test user in the database
async fn create_test_user(pool: &sqlx::PgPool, index: usize) -> Result<i32> {
    let username = format!("test_user_{}", index);
    let user_id: i32 = sqlx::query_scalar(
        r#"
        INSERT INTO users (username, password_hash, mmr)
        VALUES ($1, 'test_hash', 1000)
        RETURNING id
        "#
    )
    .bind(&username)
    .fetch_one(pool)
    .await?;
    
    Ok(user_id)
}

/// Convenience type alias for the builder
pub type TestBuilder = TestEnvironmentBuilder;