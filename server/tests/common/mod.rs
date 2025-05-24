pub mod mock_jwt;
pub mod test_server;
pub mod test_client;
pub mod test_environment;

pub use mock_jwt::MockJwtVerifier;
pub use test_server::{TestServer, TestServerBuilder, TestContext, generate_unique_game_id};
pub use test_client::TestClient;
pub use test_environment::{TestEnvironment, TestServerInstance};

/// Macro to wrap test functions with a timeout to prevent hanging tests
#[macro_export]
macro_rules! timeout_test {
    ($duration:expr, $body:expr) => {
        tokio::time::timeout($duration, $body)
            .await
            .map_err(|_| anyhow::anyhow!("Test timed out after {:?}", $duration))?
    };
}