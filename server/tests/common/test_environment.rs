use anyhow::{Context, Result};
use std::sync::Arc;
use server::{
    game_server::{GameServer, start_test_server, start_test_server_with_grpc},
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
    /// Create a new test environment with an isolated database
    pub async fn new(test_name: &str) -> Result<Self> {
        info!("Creating test environment for: {}", test_name);
        
        // Create isolated test database
        let db_guard = TestDatabaseGuard::new(test_name).await
            .context("Failed to create test database")?;
        
        Ok(Self {
            db_guard,
            servers: Vec::new(),
            user_ids: Vec::new(),
            test_name: test_name.to_string(),
        })
    }
    
    /// Get the database URL for this test environment
    pub fn db_url(&self) -> &str {
        self.db_guard.url()
    }
    
    /// Get the database pool for this test environment
    pub fn db_pool(&self) -> &sqlx::PgPool {
        self.db_guard.pool()
    }
    
    /// Add a server to this test environment
    pub async fn add_server(&mut self) -> Result<(usize, u64)> {
        self.add_server_with_grpc(false).await
    }
    
    /// Add a server to this test environment with optional gRPC
    pub async fn add_server_with_grpc(&mut self, enable_grpc: bool) -> Result<(usize, u64)> {
        let jwt_verifier = Arc::new(MockJwtVerifier::accept_any()) as Arc<dyn JwtVerifier>;
        
        let server = start_test_server_with_grpc(
            self.db_url(),
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
            server.ws_addr(),
            server.grpc_addr()
        );
        
        // If gRPC is enabled, update the database with the gRPC address
        if let Some(grpc_addr) = server.grpc_addr() {
            sqlx::query(
                r#"
                UPDATE servers 
                SET grpc_address = $1 
                WHERE id = $2
                "#
            )
            .bind(grpc_addr)
            .bind(server.id() as i32)
            .execute(self.db_pool())
            .await?;
        }
        
        self.servers.push(server);
        Ok((index, server_id))
    }
    
    /// Add a server with custom JWT verifier
    pub async fn add_server_with_jwt(&mut self, jwt_verifier: Arc<dyn JwtVerifier>) -> Result<usize> {
        let server = start_test_server(
            self.db_url(),
            jwt_verifier
        )
        .await
        .context("Failed to start server")?;
        
        let index = self.servers.len();
        info!("Started server {} with ID {} on {}", index, server.id(), server.ws_addr());
        
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
        let user_id: i32 = sqlx::query_scalar(
            r#"
            INSERT INTO users (username, password_hash, mmr)
            VALUES ($1, 'test_hash', $2)
            RETURNING id
            "#
        )
        .bind(&username)
        .bind(mmr)
        .fetch_one(self.db_pool())
        .await?;
        
        self.user_ids.push(user_id);
        info!("Created test user {} with ID {} and MMR {}", username, user_id, mmr);
        Ok(user_id)
    }
    
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

