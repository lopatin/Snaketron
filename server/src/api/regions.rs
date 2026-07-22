use axum::{Json, extract::State, http::StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tracing::{error, warn};

use crate::http_server::HttpServerState;

const ACTIVE_SERVER_USER_COUNT_PATTERN: &str = "server:*:user_count";

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
pub async fn list_regions(State(state): State<HttpServerState>) -> Json<Vec<RegionMetadata>> {
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
) -> Result<Json<HashMap<String, u32>>, StatusCode> {
    let redis_client = match redis::Client::open(state.redis_url.as_str()) {
        Ok(client) => client,
        Err(e) => {
            error!("Failed to open Redis client: {}", e);
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    let mut conn = match redis_client.get_multiplexed_async_connection().await {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to get Redis connection: {}", e);
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    let server_keys = match scan_active_server_keys(&mut conn).await {
        Ok(keys) => keys,
        Err(e) => {
            error!("Failed to query active server metric keys: {}", e);
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    let mut region_counts: HashMap<String, u32> = HashMap::new();

    for key in server_keys {
        // Get user count for this server
        let count: Option<u32> = match redis::cmd("GET").arg(&key).query_async(&mut conn).await {
            Ok(count) => count,
            Err(e) => {
                error!("Failed to read an active server's user count: {}", e);
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
        };
        let Some(count) = count else {
            // The TTL-backed key can legitimately expire after SCAN.
            continue;
        };

        let Some(region_key) = region_key_for_active_server(&key) else {
            error!("Redis returned a malformed active server metric key");
            continue;
        };

        let region: Option<String> = match redis::cmd("GET")
            .arg(&region_key)
            .query_async(&mut conn)
            .await
        {
            Ok(region) => region,
            Err(e) => {
                error!("Failed to resolve an active server's region: {}", e);
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
        };
        let Some(region) = region.filter(|region| !region.trim().is_empty()) else {
            warn!("Ignoring an active server metric without region metadata");
            continue;
        };

        // Aggregate counts by region
        let regional_count = region_counts.entry(region).or_insert(0_u32);
        *regional_count = regional_count.saturating_add(count);
    }

    Ok(Json(region_counts))
}

/// Get active server-instance counts per region from Redis.
///
/// A server is considered active while its TTL-backed
/// `server:{server_id}:user_count` key exists. Persistent region metadata is
/// consulted only for those active keys, and server identifiers are never
/// included in the response.
pub async fn get_server_counts(
    State(state): State<HttpServerState>,
) -> Result<Json<HashMap<String, u32>>, StatusCode> {
    let redis_client = match redis::Client::open(state.redis_url.as_str()) {
        Ok(client) => client,
        Err(e) => {
            error!("Failed to open Redis client: {}", e);
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    let mut conn = match redis_client.get_multiplexed_async_connection().await {
        Ok(conn) => conn,
        Err(e) => {
            error!("Failed to get Redis connection: {}", e);
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    let active_server_keys = match scan_active_server_keys(&mut conn).await {
        Ok(keys) => keys,
        Err(e) => {
            error!("Failed to query active server metric keys: {}", e);
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    let mut region_counts = HashMap::new();
    for active_server_key in active_server_keys {
        let Some(region_key) = region_key_for_active_server(&active_server_key) else {
            error!("Redis returned a malformed active server metric key");
            continue;
        };
        let region: Option<String> = match redis::cmd("GET")
            .arg(region_key)
            .query_async(&mut conn)
            .await
        {
            Ok(region) => region,
            Err(e) => {
                error!("Failed to resolve an active server's region: {}", e);
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
        };
        let Some(region) = region.filter(|region| !region.trim().is_empty()) else {
            warn!("Ignoring an active server metric without region metadata");
            continue;
        };

        record_active_server(&mut region_counts, region);
    }

    Ok(Json(region_counts))
}

async fn scan_active_server_keys(
    conn: &mut redis::aio::MultiplexedConnection,
) -> redis::RedisResult<Vec<String>> {
    let mut cursor = 0_u64;
    let mut keys = HashSet::new();

    loop {
        let (next_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(ACTIVE_SERVER_USER_COUNT_PATTERN)
            .arg("COUNT")
            .arg(100_u32)
            .query_async(conn)
            .await?;
        keys.extend(batch);
        cursor = next_cursor;
        if cursor == 0 {
            break;
        }
    }

    Ok(keys.into_iter().collect())
}

fn record_active_server(region_counts: &mut HashMap<String, u32>, region: String) {
    let count = region_counts.entry(region).or_insert(0);
    *count = count.saturating_add(1);
}

fn region_key_for_active_server(user_count_key: &str) -> Option<String> {
    let mut parts = user_count_key.split(':');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some("server"), Some(server_id), Some("user_count"), None) if !server_id.is_empty() => {
            Some(format!("server:{server_id}:region"))
        }
        _ => None,
    }
}

/// Simple health check endpoint for client-side ping measurement
/// Returns JSON with status
pub async fn health_check_json() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::{record_active_server, region_key_for_active_server};
    use std::collections::HashMap;

    #[test]
    fn counts_instances_by_region() {
        let mut counts = HashMap::new();
        record_active_server(&mut counts, "use1".to_string());
        record_active_server(&mut counts, "use1".to_string());
        record_active_server(&mut counts, "euw1".to_string());

        assert_eq!(counts.get("use1"), Some(&2));
        assert_eq!(counts.get("euw1"), Some(&1));
        assert_eq!(counts.len(), 2);
    }

    #[test]
    fn derives_region_key_from_an_active_server_metric() {
        assert_eq!(
            region_key_for_active_server("server:instance-123:user_count").as_deref(),
            Some("server:instance-123:region")
        );
    }

    #[test]
    fn rejects_malformed_active_server_metrics() {
        for key in [
            "",
            "server::user_count",
            "server:instance-123",
            "server:instance-123:connections",
            "server:instance-123:user_count:extra",
        ] {
            assert_eq!(region_key_for_active_server(key), None, "accepted {key}");
        }
    }
}
