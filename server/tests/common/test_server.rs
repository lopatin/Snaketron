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
use std::sync::atomic::{AtomicU32, Ordering};

/// Test context that ensures proper cleanup of test data
pub struct TestContext {
    pub db_pool: PgPool,
    pub server_ids: Vec<Uuid>,
    pub game_ids: Vec<u32>,
    pub user_ids: Vec<i32>,
}

impl TestContext {
    pub async fn new() -> Result<Self> {
        let db_pool = TestServerBuilder::create_test_db().await?;
        Ok(Self {
            db_pool,
            server_ids: Vec::new(),
            game_ids: Vec::new(),
            user_ids: Vec::new(),
        })
    }
    
    pub fn track_server(&mut self, server_id: Uuid) {
        self.server_ids.push(server_id);
    }
    
    pub fn track_game(&mut self, game_id: u32) {
        self.game_ids.push(game_id);
    }
    
    pub fn track_user(&mut self, user_id: i32) {
        self.user_ids.push(user_id);
    }
}

// Global counter for generating unique game IDs across all tests
// Start with a high value that includes timestamp to ensure uniqueness across test runs
static GAME_ID_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Generate a unique game ID for tests to avoid conflicts
pub fn generate_unique_game_id() -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    
    // Initialize counter with timestamp on first use to ensure uniqueness across test runs
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        // Use lower 20 bits of timestamp + shift to ensure we're in valid i32 range
        let base = (timestamp & 0xFFFFF) << 10;
        GAME_ID_COUNTER.store(base, Ordering::SeqCst);
    });
    
    GAME_ID_COUNTER.fetch_add(1, Ordering::SeqCst)
}

impl Drop for TestContext {
    fn drop(&mut self) {
        // Clean up test data when the context is dropped
        let pool = self.db_pool.clone();
        let server_ids = self.server_ids.clone();
        let game_ids = self.game_ids.clone();
        let user_ids = self.user_ids.clone();
        
        // Spawn a task to clean up since Drop is sync
        tokio::spawn(async move {
            // Clean up in correct order due to foreign key constraints
            
            // First clean up game_players
            for game_id in &game_ids {
                let _ = sqlx::query("DELETE FROM game_players WHERE game_id = $1")
                    .bind(*game_id as i32)
                    .execute(&pool)
                    .await;
            }
            
            // Then clean up game_requests
            for user_id in &user_ids {
                let _ = sqlx::query("DELETE FROM game_requests WHERE user_id = $1")
                    .bind(*user_id)
                    .execute(&pool)
                    .await;
            }
            
            // Clean up games
            for game_id in game_ids {
                let _ = sqlx::query("DELETE FROM games WHERE id = $1")
                    .bind(game_id as i32)
                    .execute(&pool)
                    .await;
            }
            
            // Clean up users
            for user_id in user_ids {
                let _ = sqlx::query("DELETE FROM users WHERE id = $1")
                    .bind(user_id)
                    .execute(&pool)
                    .await;
            }
            
            // Finally clean up servers
            for server_id in server_ids {
                let _ = sqlx::query("DELETE FROM servers WHERE id = $1")
                    .bind(server_id)
                    .execute(&pool)
                    .await;
            }
        });
    }
}

/// Test server builder for configuring test scenarios
pub struct TestServerBuilder {
    port: u16,
    jwt_verifier: Option<Arc<dyn JwtVerifier>>,
    db_pool: Option<PgPool>,
}

impl TestServerBuilder {
    pub fn new() -> Self {
        Self {
            port: 0, // 0 means random available port
            jwt_verifier: None,
            db_pool: None,
        }
    }
    
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }
    
    /// Use mock authentication that accepts any token
    /// When a client sends a numeric token (e.g., "123"), it maps to that user_id
    /// When a client sends a non-numeric token, it maps to user_id 1
    /// This is the recommended mode for most tests
    pub fn with_mock_auth(mut self) -> Self {
        self.jwt_verifier = Some(Arc::new(MockJwtVerifier::accept_any()));
        self
    }
    
    pub fn with_jwt_verifier(mut self, verifier: Arc<dyn JwtVerifier>) -> Self {
        self.jwt_verifier = Some(verifier);
        self
    }
    
    pub fn with_db_pool(mut self, pool: PgPool) -> Self {
        self.db_pool = Some(pool);
        self
    }
    
    /// Get a random available port
    pub fn get_random_port() -> u16 {
        // Create a temporary TcpListener to get an available port
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        // Add a small delay to ensure OS has released the port
        std::thread::sleep(std::time::Duration::from_millis(10));
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
        
        // Clean up any stale test data from previous runs
        Self::cleanup_stale_test_data(&pool).await?;
        
        Ok(pool)
    }
    
    /// Clean up stale test data from previous test runs that may have failed
    async fn cleanup_stale_test_data(pool: &PgPool) -> Result<()> {
        // Clean up old test data (older than 1 hour)
        let _ = sqlx::query(
            r#"
            DELETE FROM game_players 
            WHERE game_id IN (
                SELECT id FROM games 
                WHERE created_at < NOW() - INTERVAL '1 hour'
            )
            "#
        )
        .execute(pool)
        .await;
        
        let _ = sqlx::query(
            r#"
            DELETE FROM game_requests 
            WHERE request_time < NOW() - INTERVAL '1 hour'
            "#
        )
        .execute(pool)
        .await;
        
        let _ = sqlx::query(
            r#"
            DELETE FROM games 
            WHERE created_at < NOW() - INTERVAL '1 hour'
            "#
        )
        .execute(pool)
        .await;
        
        let _ = sqlx::query(
            r#"
            DELETE FROM servers 
            WHERE last_heartbeat < NOW() - INTERVAL '1 hour'
            "#
        )
        .execute(pool)
        .await;
        
        // Clean up test users (those with ID >= 100000)
        let _ = sqlx::query(
            r#"
            DELETE FROM users 
            WHERE id >= 100000
            "#
        )
        .execute(pool)
        .await;
        
        Ok(())
    }
    
    pub async fn build(self) -> Result<TestServer> {
        let addr = format!("127.0.0.1:{}", self.port);
        let listener = TcpListener::bind(&addr).await?;
        let actual_addr = listener.local_addr()?;
        
        // Get or create database pool
        let db_pool = match self.db_pool {
            Some(pool) => pool,
            None => Self::create_test_db().await?,
        };
        
        // Create a server record in the database
        let server_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO servers (id, address, last_heartbeat, current_game_count, max_game_capacity)
            VALUES ($1, $2, NOW(), 0, 100)
            "#
        )
        .bind(server_id)
        .bind(actual_addr.to_string())
        .execute(&db_pool)
        .await?;
        
        let games_manager = Arc::new(Mutex::new(GamesManager::new()));
        let cancellation_token = CancellationToken::new();
        let jwt_verifier = self.jwt_verifier.unwrap_or_else(|| Arc::new(DefaultJwtVerifier));
        
        let games_manager_clone = games_manager.clone();
        let cancellation_token_clone = cancellation_token.clone();
        let db_pool_clone = db_pool.clone();
        
        // Spawn the server in a separate task
        let server_handle = tokio::spawn(async move {
            run_websocket_server_with_listener(
                listener,
                games_manager_clone,
                db_pool_clone,
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
            db_pool,
            server_id,
        })
    }
}

/// Represents a running test server
pub struct TestServer {
    pub addr: String,
    pub games_manager: Arc<Mutex<GamesManager>>,
    cancellation_token: CancellationToken,
    server_handle: JoinHandle<Result<()>>,
    db_pool: PgPool,
    server_id: Uuid,
}

impl TestServer {
    pub async fn connect_client(&self) -> Result<TestClient> {
        TestClient::connect(&self.addr).await
    }
    
    pub async fn create_game(&self, game_id: u32) -> Result<()> {
        let mut games_manager = self.games_manager.lock().await;
        games_manager.start_game(game_id).await
    }
    
    pub fn get_db_pool(&self) -> PgPool {
        self.db_pool.clone()
    }
    
    pub fn get_server_id(&self) -> Result<Uuid> {
        Ok(self.server_id)
    }
    
    pub async fn shutdown(self) -> Result<()> {
        // Clean up all test data created by this server
        // First clean up game_players for any games on this server
        let _ = sqlx::query(
            r#"
            DELETE FROM game_players 
            WHERE game_id IN (SELECT id FROM games WHERE server_id = $1)
            "#
        )
        .bind(self.server_id)
        .execute(&self.db_pool)
        .await;
        
        // Clean up games on this server
        let _ = sqlx::query(
            "DELETE FROM games WHERE server_id = $1"
        )
        .bind(self.server_id)
        .execute(&self.db_pool)
        .await;
        
        // Clean up game requests on this server
        let _ = sqlx::query(
            "DELETE FROM game_requests WHERE server_id = $1"
        )
        .bind(self.server_id)
        .execute(&self.db_pool)
        .await;
        
        // Clean up server record
        let _ = sqlx::query(
            "DELETE FROM servers WHERE id = $1"
        )
        .bind(self.server_id)
        .execute(&self.db_pool)
        .await;
        
        self.cancellation_token.cancel();
        
        // Give the server a moment to shut down gracefully
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        // Wait for server to finish with a timeout
        tokio::time::timeout(Duration::from_secs(5), self.server_handle).await??;
        Ok(())
    }
}