// Shared across many test binaries; each binary uses a different subset,
// so items unused in one binary are load-bearing in another.
#[allow(dead_code)]
pub mod mock_jwt;
#[allow(dead_code)]
pub mod test_client;
#[allow(dead_code)]
pub mod test_environment;

// Each integration-test binary compiles this module independently, so these
// re-exports are unused in some binaries but required in others.
#[allow(unused_imports)]
pub use mock_jwt::MockJwtVerifier;
#[allow(unused_imports)]
pub use test_client::TestClient;
#[allow(unused_imports)]
pub use test_environment::TestEnvironment;

/// Macro to wrap test functions with a timeout to prevent hanging tests
#[macro_export]
macro_rules! timeout_test {
    ($duration:expr, $body:expr) => {
        tokio::time::timeout($duration, $body)
            .await
            .map_err(|_| anyhow::anyhow!("Test timed out after {:?}", $duration))?
    };
}
