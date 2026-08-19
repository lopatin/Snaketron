//! Runtime for externally-registered hosted services.
//!
//! See `snaketron/specs/hosted-services.md`. The public API lives in the
//! `snaketron-service-api` crate so an operator never depends on this crate's
//! internals; everything here is the host-side implementation of that contract.

pub mod dynamo_kv;
pub mod lease;
pub mod lifecycle_view;
pub mod supervisor;
pub mod valkey_kv;

#[cfg(test)]
mod tests;

pub use dynamo_kv::DynamoKeyValueStore;
pub use lease::ExclusionLeaseStore;
pub use lifecycle_view::TaskLifecycleView;
pub use supervisor::{SupervisorConfig, spawn_supervisor};
pub use valkey_kv::ValkeyKeyValueStore;

use std::collections::HashMap;

/// Reads the `SNAKETRON_SERVICE_<NAME>_*` environment block for one service,
/// stripping the prefix so a service sees stable keys regardless of how they
/// are namespaced in the environment.
pub fn service_config_from_env(name: &str) -> snaketron_service_api::ServiceConfig {
    let prefix = format!(
        "SNAKETRON_SERVICE_{}_",
        name.to_uppercase().replace(['-', '.', '/'], "_")
    );
    let values: HashMap<String, String> = std::env::vars()
        .filter_map(|(key, value)| {
            key.strip_prefix(&prefix)
                .map(|stripped| (stripped.to_owned(), value))
        })
        .collect();
    snaketron_service_api::ServiceConfig::from_map(values)
}

#[cfg(test)]
mod tests_env {
    use super::*;

    #[test]
    fn service_config_is_namespaced_and_stripped() {
        // SAFETY: single-threaded test, and the key is unique to this test.
        unsafe {
            std::env::set_var("SNAKETRON_SERVICE_ICEBERG_COMMITTER_BATCH", "42");
            std::env::set_var("SNAKETRON_SERVICE_OTHER_BATCH", "99");
        }
        let config = service_config_from_env("iceberg-committer");
        assert_eq!(config.get("BATCH"), Some("42"));
        unsafe {
            std::env::remove_var("SNAKETRON_SERVICE_ICEBERG_COMMITTER_BATCH");
            std::env::remove_var("SNAKETRON_SERVICE_OTHER_BATCH");
        }
    }
}
