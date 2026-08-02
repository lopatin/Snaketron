use anyhow::Context;
use axum::{Json, extract::State, http::StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{error, warn};

use crate::http_server::HttpServerState;
use crate::redis_keys::RedisKeys;
use crate::redis_utils::RedisConnection;

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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub(crate) struct ActiveServerMetric {
    pub(crate) region: String,
    pub(crate) user_count: u32,
}

/// List all available regions with connection metadata
/// Regions are dynamically discovered from DynamoDB by scanning for active servers
pub async fn list_regions(State(state): State<HttpServerState>) -> Json<Vec<RegionMetadata>> {
    let regions = state.region_cache.get_regions().await;
    Json(regions)
}

/// Get aggregated user counts per region from Redis
///
/// Loads the single-slot active-server registry and aggregates it by region.
pub async fn get_user_counts(
    State(state): State<HttpServerState>,
) -> Result<Json<HashMap<String, u32>>, StatusCode> {
    let mut conn = state.redis.clone();

    let metrics = match load_active_server_metrics(&mut conn).await {
        Ok(metrics) => metrics,
        Err(e) => {
            error!("Failed to query active server metrics: {}", e);
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    let mut region_counts: HashMap<String, u32> = HashMap::new();
    for metric in metrics {
        let regional_count = region_counts.entry(metric.region).or_insert(0_u32);
        *regional_count = regional_count.saturating_add(metric.user_count);
    }

    Ok(Json(region_counts))
}

/// Get active server-instance counts per region from Redis.
///
/// A server is considered active while its expiry-index entry is newer than
/// Valkey's server clock. Server identifiers are never included in the response.
pub async fn get_server_counts(
    State(state): State<HttpServerState>,
) -> Result<Json<HashMap<String, u32>>, StatusCode> {
    let mut conn = state.redis.clone();

    let metrics = match load_active_server_metrics(&mut conn).await {
        Ok(metrics) => metrics,
        Err(e) => {
            error!("Failed to query active server metrics: {}", e);
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    let mut region_counts = HashMap::new();
    for metric in metrics {
        record_active_server(&mut region_counts, metric.region);
    }

    Ok(Json(region_counts))
}

/// Load the complete active registry from one hash slot. SCAN is deliberately
/// avoided: redis-rs routes a cluster SCAN to one node, which can silently
/// omit tasks stored on other Serverless shards.
pub(crate) async fn load_active_server_metrics(
    conn: &mut RedisConnection,
) -> anyhow::Result<Vec<ActiveServerMetric>> {
    let entries: HashMap<String, String> = redis::Script::new(
        r#"
        local function key_type(key)
            local response = redis.call('TYPE', key)
            if type(response) == 'table' then return response['ok'] end
            return response
        end
        local metrics_type = key_type(KEYS[1])
        local expiry_type = key_type(KEYS[2])
        if metrics_type ~= 'none' and metrics_type ~= 'hash' then
            return redis.error_reply('active server metrics key has wrong type')
        end
        if expiry_type ~= 'none' and expiry_type ~= 'zset' then
            return redis.error_reply('active server expiry key has wrong type')
        end
        local now = redis.call('TIME')
        local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
        local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now_ms)
        if #expired > 0 then
            redis.call('HDEL', KEYS[1], unpack(expired))
            redis.call('ZREM', KEYS[2], unpack(expired))
        end
        return redis.call('HGETALL', KEYS[1])
        "#,
    )
    .key(RedisKeys::active_server_metrics())
    .key(RedisKeys::active_server_metrics_expiry())
    .invoke_async(conn)
    .await
    .context("failed to load and prune active server metrics")?;

    let mut metrics = Vec::with_capacity(entries.len());
    for (server_id, payload) in entries {
        match serde_json::from_str::<ActiveServerMetric>(&payload) {
            Ok(metric) if !metric.region.trim().is_empty() => metrics.push(metric),
            Ok(_) => warn!(server_id, "Ignoring active server metric with empty region"),
            Err(error) => warn!(server_id, %error, "Ignoring malformed active server metric"),
        }
    }
    Ok(metrics)
}

fn record_active_server(region_counts: &mut HashMap<String, u32>, region: String) {
    let count = region_counts.entry(region).or_insert(0);
    *count = count.saturating_add(1);
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
    use super::record_active_server;
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
}
