use anyhow::{Context, Result};
use std::sync::Arc;
use server::{
    game_server::{GameServer, start_test_server, start_test_server_with_grpc},
    ws_server::JwtVerifier,
    db::{Database, dynamodb::DynamoDatabase},
};
use super::mock_jwt::MockJwtVerifier;
use tracing::info;

/// A test environment that manages game servers and database isolation
pub struct TestEnvironment {
    /// Database instance
    db: Arc<dyn Database>,
    /// Game servers running in this environment
    servers: Vec<GameServer>,
    /// User IDs created for testing
    user_ids: Vec<i32>,
    /// Test name for debugging
    test_name: String,
}

impl TestEnvironment {
    /// Create a new test environment with an isolated database
    pub async fn new(test_name: &str) -> Result<Self> {
        info!("Creating test environment for: {}", test_name);

        // Environment variables for test dependencies are set via .cargo/config.toml
        // Use a unique table prefix for this test to avoid collisions
        let unique_prefix = format!("test_{}", uuid::Uuid::new_v4().to_string().replace("-", "").chars().take(8).collect::<String>());
        // SAFETY: This is safe in tests because each test runs sequentially with TestEnvironment::new()
        // being called at the start of the test before any other code accesses DYNAMODB_TABLE_PREFIX
        unsafe {
            std::env::set_var("DYNAMODB_TABLE_PREFIX", &unique_prefix);
            // Use Redis database 1 for tests (tests flush database 1, so server should use it too)
            std::env::set_var("SNAKETRON_REDIS_URL", "redis://127.0.0.1:6379/1");
        }
        info!("Using unique table prefix for test: {}", unique_prefix);

        // Create DynamoDB instance for testing
        let db = Arc::new(DynamoDatabase::new().await
            .context("Failed to create DynamoDB instance")?) as Arc<dyn Database>;

        Ok(Self {
            db,
            servers: Vec::new(),
            user_ids: Vec::new(),
            test_name: test_name.to_string(),
        })
    }
    
    /// Get the database instance for this test environment
    pub fn db(&self) -> Arc<dyn Database> {
        Arc::clone(&self.db)
    }
    
    /// Add a server to this test environment
    pub async fn add_server(&mut self) -> Result<(usize, u64)> {
        self.add_server_with_grpc(false).await
    }
    
    /// Add a server to this test environment with optional gRPC
    pub async fn add_server_with_grpc(&mut self, enable_grpc: bool) -> Result<(usize, u64)> {
        let jwt_verifier = Arc::new(MockJwtVerifier::accept_any()) as Arc<dyn JwtVerifier>;
        
        let server = start_test_server_with_grpc(
            self.db(),
            jwt_verifier,
            enable_grpc
        )
        .await
        .context("Failed to start server")?;
        
        let index = self.servers.len();
        let server_id = server.id();
        info!(
            "Started server {} with ID {} on {} (gRPC: {:?})", 
            index, 
            server_id, 
            server.http_addr(),
            server.grpc_addr()
        );
        
        // If gRPC is enabled, update the database with the gRPC address
        if let Some(grpc_addr) = server.grpc_addr() {
            // For DynamoDB, we'd need to update the server record
            // This would require implementing an update_server_grpc_address method
            // For now, we'll skip this as it's not critical for the test
            info!("Server {} has gRPC address: {}", server.id(), grpc_addr);
        }
        
        self.servers.push(server);
        Ok((index, server_id))
    }
    
    /// Add a server with custom JWT verifier
    pub async fn add_server_with_jwt(&mut self, jwt_verifier: Arc<dyn JwtVerifier>) -> Result<usize> {
        let server = start_test_server(
            self.db(),
            jwt_verifier
        )
        .await
        .context("Failed to start server")?;
        
        let index = self.servers.len();
        info!("Started server {} with ID {} on {}", index, server.id(), server.http_addr());
        
        self.servers.push(server);
        Ok(index)
    }
    
    /// Create a test user in the database
    pub async fn create_user(&mut self) -> Result<i32> {
        self.create_user_with_mmr(1000).await
    }
    
    /// Create a test user with specific MMR
    pub async fn create_user_with_mmr(&mut self, mmr: i32) -> Result<i32> {
        let index = self.user_ids.len();
        let username = format!("test_user_{}", index);
        
        // Use the Database trait method to create a user
        let user = self.db.create_user(
            &username,
            "test_hash",
            mmr
        ).await?;
        let user_id = user.id;
        
        self.user_ids.push(user_id);
        info!("Created test user {} with ID {}", username, user_id);
        Ok(user_id)
    }
    
    /// Get the WebSocket address for a server by index
    pub fn ws_addr(&self, index: usize) -> Option<String> {
        self.servers.get(index).map(|s| format!("ws://{}/ws", s.http_addr()))
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

