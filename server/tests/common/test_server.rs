use std::sync::Arc;
use anyhow::Result;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;
use server::ws_server::{JwtVerifier, DefaultJwtVerifier, run_websocket_server_with_listener};
use server::games_manager::GamesManager;
use super::mock_jwt::MockJwtVerifier;
use super::test_client::TestClient;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

/// Test server builder for configuring test scenarios
pub struct TestServerBuilder {
    port: u16,
    jwt_verifier: Option<Arc<dyn JwtVerifier>>,
}

impl TestServerBuilder {
    pub fn new() -> Self {
        Self {
            port: 0, // 0 means random available port
            jwt_verifier: None,
        }
    }
    
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }
    
    pub fn with_mock_auth(mut self) -> Self {
        self.jwt_verifier = Some(Arc::new(MockJwtVerifier::accept_any()));
        self
    }
    
    pub fn with_jwt_verifier(mut self, verifier: Arc<dyn JwtVerifier>) -> Self {
        self.jwt_verifier = Some(verifier);
        self
    }
    
    /// Get a random available port
    pub fn get_random_port() -> u16 {
        // Create a temporary TcpListener to get an available port
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        port
    }
    
    /// Create a test database pool
    pub async fn create_test_db() -> Result<PgPool> {
        // Use test database credentials from environment or defaults
        let db_url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://snaketron:snaketron@localhost:5432/snaketron".to_string());
        
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&db_url)
            .await?;
        
        // Run migrations using the embedded migrations from main.rs pattern
        mod migrations {
            use refinery::embed_migrations;
            embed_migrations!("./migrations");
        }
        
        let mut config = refinery::config::Config::new(refinery::config::ConfigDbType::Postgres)
            .set_db_user("snaketron")
            .set_db_pass("snaketron")
            .set_db_host("localhost")
            .set_db_port("5432")
            .set_db_name("snaketron");
        
        migrations::migrations::runner().run_async(&mut config).await?;
        
        // Clean up test data
        sqlx::query("DELETE FROM games").execute(&pool).await?;
        sqlx::query("DELETE FROM servers").execute(&pool).await?;
        
        Ok(pool)
    }
    
    pub async fn build(self) -> Result<TestServer> {
        let addr = format!("127.0.0.1:{}", self.port);
        let listener = TcpListener::bind(&addr).await?;
        let actual_addr = listener.local_addr()?;
        
        let games_manager = Arc::new(Mutex::new(GamesManager::new()));
        let cancellation_token = CancellationToken::new();
        let jwt_verifier = self.jwt_verifier.unwrap_or_else(|| Arc::new(DefaultJwtVerifier));
        
        let games_manager_clone = games_manager.clone();
        let cancellation_token_clone = cancellation_token.clone();
        
        // Spawn the server in a separate task
        let server_handle = tokio::spawn(async move {
            run_websocket_server_with_listener(
                listener,
                games_manager_clone,
                cancellation_token_clone,
                jwt_verifier,
            ).await
        });
        
        // Give the server a moment to start
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        let ws_addr = format!("ws://{}", actual_addr);
        info!("Test server started at {}", ws_addr);
        
        Ok(TestServer {
            addr: ws_addr,
            games_manager,
            cancellation_token,
            server_handle,
        })
    }
}

/// Represents a running test server
pub struct TestServer {
    pub addr: String,
    pub games_manager: Arc<Mutex<GamesManager>>,
    cancellation_token: CancellationToken,
    server_handle: JoinHandle<Result<()>>,
}

impl TestServer {
    pub async fn connect_client(&self) -> Result<TestClient> {
        TestClient::connect(&self.addr).await
    }
    
    pub async fn create_game(&self, game_id: u32) -> Result<()> {
        let mut games_manager = self.games_manager.lock().await;
        games_manager.start_game(game_id).await
    }
    
    pub async fn shutdown(self) -> Result<()> {
        self.cancellation_token.cancel();
        self.server_handle.await??;
        Ok(())
    }
}