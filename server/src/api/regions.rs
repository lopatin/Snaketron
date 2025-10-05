use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::error;

use crate::http_server::HttpServerState;

/// Region metadata returned by /api/regions endpoint
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RegionMetadata {
    pub id: String,
    pub name: String,
    pub origin: String,
    pub ws_url: String,
}

/// Health check response for ping measurement
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
}

/// List all available regions with connection metadata
/// Regions are dynamically discovered from DynamoDB by scanning for active servers
pub async fn list_regions(
    State(state): State<HttpServerState>,
) -> Json<Vec<RegionMetadata>> {
    let regions = state.region_cache.get_regions().await;
    Json(regions)
}

/// Get aggregated user counts per region from Redis
///
/// Queries Redis for all server user counts and aggregates them by region.
/// Redis schema:
/// - Key: server:{server_id}:user_count -> Value: <count>
/// - Key: server:{server_id}:region -> Value: <region_id>
pub async fn get_user_counts(
    State(state): State<HttpServerState>,
) -> Json<HashMap<String, u32>> {
    // Create Redis client
    let redis_client = match redis::Client::open(state.redis_url.as_str()) {
        Ok(client) => client,
        Err(e) => {
            error!("Failed to open Redis client: {}", e);
            return Json(HashMap::new());
        }
    };

    let mut conn = match redis_client.get_multiplexed_async_connection().await {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to get Redis connection: {}", e);
            return Json(HashMap::new());
        }
    };

    // Query all server user count keys
    let server_keys: Vec<String> = match redis::cmd("KEYS")
        .arg("server:*:user_count")
        .query_async(&mut conn)
        .await
    {
        Ok(keys) => keys,
        Err(e) => {
            error!("Failed to query Redis keys: {}", e);
            return Json(HashMap::new());
        }
    };

    let mut region_counts: HashMap<String, u32> = HashMap::new();

    for key in server_keys {
        // Get user count for this server
        let count: u32 = match redis::cmd("GET")
            .arg(&key)
            .query_async(&mut conn)
            .await
        {
            Ok(count) => count,
            Err(e) => {
                error!("Failed to get user count for {}: {}", key, e);
                continue;
            }
        };

        // Extract server_id from key "server:{server_id}:user_count"
        let server_id = match key.split(':').nth(1) {
            Some(id) => id,
            None => {
                error!("Invalid key format: {}", key);
                continue;
            }
        };

        // Get region for this server
        let region_key = format!("server:{}:region", server_id);
        let region: String = match redis::cmd("GET")
            .arg(&region_key)
            .query_async(&mut conn)
            .await
        {
            Ok(region) => region,
            Err(_) => {
                // If no region is set, default to "us"
                "us".to_string()
            }
        };

        // Aggregate counts by region
        *region_counts.entry(region).or_insert(0) += count;
    }

    Json(region_counts)
}

/// Simple health check endpoint for client-side ping measurement
/// Returns JSON with status
pub async fn health_check_json() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
    })
}
