use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;
use server::{
    ws_server::{register_server, run_heartbeat_loop, run_websocket_server, JwtVerifier},
    games_manager::GamesManager,
    matchmaking::run_matchmaking_loop,
    game_cleanup::run_cleanup_service,
};
use super::mock_jwt::MockJwtVerifier;
use std::time::Duration;

/// A test environment that runs real server instances
pub struct TestEnvironment {
    servers: Vec<TestServerInstance>,
    db_pool: PgPool,
}

/// A single server instance in the test environment
pub struct TestServerInstance {
    pub server_id: Uuid,
    pub ws_port: u16,
    pub ws_addr: String,
    cancellation_token: CancellationToken,
    handles: Vec<JoinHandle<()>>,
}

impl TestEnvironment {
    /// Create a new test environment with the specified number of servers
    pub async fn new(num_servers: usize) -> Result<Self> {
        // Initialize database
        let db_pool = Self::setup_test_database().await?;
        
        let mut servers = Vec::new();
        
        for i in 0..num_servers {
            let server = TestServerInstance::start(
                db_pool.clone(),
                format!("test-server-{}", i),
            ).await?;
            servers.push(server);
        }
        
        // Give servers time to fully start
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        Ok(Self {
            servers,
            db_pool,
        })
    }
    
    /// Get a reference to a server by index
    pub fn server(&self, index: usize) -> Option<&TestServerInstance> {
        self.servers.get(index)
    }
    
    /// Get the WebSocket address for a server
    pub fn ws_addr(&self, index: usize) -> Option<String> {
        self.servers.get(index).map(|s| s.ws_addr.clone())
    }
    
    /// Get the database pool (for assertions only, not for data manipulation)
    pub fn db_pool(&self) -> &PgPool {
        &self.db_pool
    }
    
    /// Shutdown all servers gracefully
    pub async fn shutdown(mut self) -> Result<()> {
        for server in self.servers.drain(..) {
            server.shutdown().await?;
        }
        Ok(())
    }
    
    async fn setup_test_database() -> Result<PgPool> {
        // Use test database credentials from environment or defaults
        let db_url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://snaketron:snaketron@localhost:5432/snaketron".to_string());
        
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&db_url)
            .await?;
        
        // Run migrations
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
        
        Ok(pool)
    }
}

impl TestServerInstance {
    async fn start(db_pool: PgPool, region: String) -> Result<Self> {
        // Find an available port
        let ws_port = Self::get_available_port();
        let ws_addr = format!("127.0.0.1:{}", ws_port);
        
        // Register server in database
        let server_id = register_server(&db_pool, &region).await?;
        
        let cancellation_token = CancellationToken::new();
        let mut handles = Vec::new();
        
        // Start heartbeat loop
        let heartbeat_pool = db_pool.clone();
        let heartbeat_server_id = server_id.clone();
        let heartbeat_token = cancellation_token.clone();
        handles.push(tokio::spawn(async move {
            let _ = run_heartbeat_loop(heartbeat_pool, heartbeat_server_id, heartbeat_token).await;
        }));
        
        // Start games manager
        let games_manager = Arc::new(Mutex::new(GamesManager::new()));
        
        // Create mock JWT verifier for tests
        let jwt_verifier = Arc::new(MockJwtVerifier::accept_any()) as Arc<dyn JwtVerifier>;
        
        // Start WebSocket server
        let ws_games_manager = games_manager.clone();
        let ws_pool = db_pool.clone();
        let ws_token = cancellation_token.clone();
        let ws_addr_clone = ws_addr.clone();
        handles.push(tokio::spawn(async move {
            let _ = run_websocket_server(&ws_addr_clone, ws_games_manager, ws_pool, ws_token, jwt_verifier).await;
        }));
        
        // Start matchmaking service
        let matchmaking_pool = db_pool.clone();
        let matchmaking_server_id = server_id.clone();
        handles.push(tokio::spawn(async move {
            run_matchmaking_loop(matchmaking_pool, matchmaking_server_id).await;
        }));
        
        // Start cleanup service
        let cleanup_pool = db_pool.clone();
        let cleanup_token = cancellation_token.clone();
        handles.push(tokio::spawn(async move {
            let _ = run_cleanup_service(cleanup_pool, cleanup_token).await;
        }));
        
        Ok(Self {
            server_id,
            ws_port,
            ws_addr: format!("ws://{}", ws_addr),
            cancellation_token,
            handles,
        })
    }
    
    async fn shutdown(self) -> Result<()> {
        // Cancel all services
        self.cancellation_token.cancel();
        
        // Wait for all services to complete with timeout
        for handle in self.handles {
            match tokio::time::timeout(Duration::from_secs(5), handle).await {
                Ok(Ok(())) => {},
                Ok(Err(e)) => eprintln!("Service panicked during shutdown: {:?}", e),
                Err(_) => eprintln!("Service shutdown timed out"),
            }
        }
        
        Ok(())
    }
    
    fn get_available_port() -> u16 {
        // Create a temporary TcpListener to get an available port
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        // Add a small delay to ensure OS has released the port
        std::thread::sleep(std::time::Duration::from_millis(10));
        port
    }
}