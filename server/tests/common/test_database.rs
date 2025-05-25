use anyhow::{Context, Result};
use sqlx::{PgPool, postgres::PgPoolOptions, Executor};
use std::sync::atomic::{AtomicU32, Ordering};
use tracing::info;

/// Manages test database creation and cleanup
pub struct TestDatabase {
    /// Name of the test database
    pub name: String,
    /// Connection string to the test database
    pub url: String,
    /// Pool connected to the test database
    pub pool: PgPool,
    /// Connection to postgres database for cleanup
    admin_pool: PgPool,
}

// Global counter for unique database names
static DB_COUNTER: AtomicU32 = AtomicU32::new(0);

impl TestDatabase {
    /// Create a new test database with a unique name
    pub async fn new(test_name: &str) -> Result<Self> {
        // Generate unique database name
        let counter = DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let name = format!("test_{}_{}", timestamp, counter);
        
        info!("Creating test database: {} for test: {}", name, test_name);

        // Connect to postgres database for admin operations
        let admin_url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://snaketron:snaketron@localhost:5432/postgres".to_string());
        
        let admin_pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&admin_url)
            .await
            .context("Failed to connect to postgres database")?;

        // Create the test database
        let create_query = format!("CREATE DATABASE \"{}\"", name);
        admin_pool.execute(create_query.as_str()).await
            .context("Failed to create test database")?;

        // Connect to the new test database
        let test_db_url = admin_url.replace("/postgres", &format!("/{}", name));
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&test_db_url)
            .await
            .context("Failed to connect to test database")?;

        let mut db = Self {
            name: name.clone(),
            url: test_db_url,
            pool,
            admin_pool,
        };
        
        // Run migrations
        db.run_migrations().await?;

        Ok(db)
    }

    /// Run database migrations
    async fn run_migrations(&self) -> Result<()> {
        // Embed migrations
        mod migrations {
            use refinery::embed_migrations;
            embed_migrations!("./migrations");
        }

        // For test databases, we know the URL from creation
        let db_url = &self.url;

        // Create refinery config
        let mut config = refinery::config::Config::new(refinery::config::ConfigDbType::Postgres);
        
        // Parse the URL to extract components
        let url = url::Url::parse(&db_url)?;
        if let Some(host) = url.host_str() {
            config = config.set_db_host(host);
        }
        if let Some(port) = url.port() {
            config = config.set_db_port(&port.to_string());
        }
        let username = url.username();
        if !username.is_empty() {
            config = config.set_db_user(username);
        }
        if let Some(password) = url.password() {
            config = config.set_db_pass(password);
        }
        if let Some(db_name) = url.path_segments().and_then(|segments| segments.last()) {
            if !db_name.is_empty() {
                config = config.set_db_name(db_name);
            }
        }

        // Run migrations
        migrations::migrations::runner()
            .run_async(&mut config)
            .await
            .context("Failed to run migrations")?;

        Ok(())
    }

    /// Drop the test database
    pub async fn cleanup(self) -> Result<()> {
        info!("Cleaning up test database: {}", self.name);
        
        // Close all connections to the test database
        self.pool.close().await;
        
        // Drop the database
        let drop_query = format!("DROP DATABASE \"{}\" WITH (FORCE)", self.name);
        self.admin_pool.execute(drop_query.as_str()).await
            .context("Failed to drop test database")?;
        
        self.admin_pool.close().await;
        
        Ok(())
    }
}

/// Create a test database and automatically clean it up when the guard is dropped
pub struct TestDatabaseGuard {
    db: Option<TestDatabase>,
}

impl TestDatabaseGuard {
    pub async fn new(test_name: &str) -> Result<Self> {
        let db = TestDatabase::new(test_name).await?;
        Ok(Self { db: Some(db) })
    }

    pub fn pool(&self) -> &PgPool {
        &self.db.as_ref().unwrap().pool
    }

    pub fn url(&self) -> &str {
        &self.db.as_ref().unwrap().url
    }
}

impl Drop for TestDatabaseGuard {
    fn drop(&mut self) {
        if let Some(db) = self.db.take() {
            // Spawn a task to clean up the database
            tokio::spawn(async move {
                if let Err(e) = db.cleanup().await {
                    eprintln!("Failed to cleanup test database: {}", e);
                }
            });
        }
    }
}