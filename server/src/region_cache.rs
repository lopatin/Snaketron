use anyhow::{Context, Result};
use aws_sdk_dynamodb::Client;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

use crate::api::regions::RegionMetadata;

/// Cache for region metadata scanned from DynamoDB
pub struct RegionCache {
    cache: Arc<RwLock<HashMap<String, RegionMetadata>>>,
    dynamodb_client: Client,
    table_prefix: String,
}

impl RegionCache {
    /// Create a new region cache
    pub fn new(dynamodb_client: Client, table_prefix: String) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            dynamodb_client,
            table_prefix,
        }
    }

    /// Get the main table name
    fn main_table(&self) -> String {
        format!("{}-main", self.table_prefix)
    }

    /// Scan DynamoDB for all active servers and extract unique regions
    pub async fn refresh(&self) -> Result<()> {
        debug!("Refreshing region cache from DynamoDB");

        // Calculate cutoff time (30 seconds ago)
        let thirty_seconds_ago = chrono::Utc::now() - chrono::Duration::seconds(30);

        // Query GSI1 for all servers
        // Note: "region" is a DynamoDB reserved keyword, so we use expression attribute names
        let response = self
            .dynamodb_client
            .query()
            .table_name(self.main_table())
            .index_name("GSI1")
            .key_condition_expression("gsi1pk = :pk AND gsi1sk > :cutoff")
            .expression_attribute_values(
                ":pk",
                aws_sdk_dynamodb::types::AttributeValue::S("SERVER".to_string()),
            )
            .expression_attribute_values(
                ":cutoff",
                aws_sdk_dynamodb::types::AttributeValue::S(thirty_seconds_ago.to_rfc3339()),
            )
            .projection_expression("id, #region, origin, wsUrl")
            .expression_attribute_names("#region", "region")
            .send()
            .await
            .context("Failed to query servers from DynamoDB")?;

        let items = response.items.unwrap_or_default();

        // Group servers by region
        let mut regions: HashMap<String, RegionMetadata> = HashMap::new();

        for item in items {
            // Extract fields
            let region_id = match item.get("region").and_then(|v| v.as_s().ok()) {
                Some(r) => r.clone(),
                None => continue,
            };

            let origin = match item.get("origin").and_then(|v| v.as_s().ok()) {
                Some(o) => o.clone(),
                None => continue,
            };

            let ws_url = match item.get("wsUrl").and_then(|v| v.as_s().ok()) {
                Some(w) => w.clone(),
                None => continue,
            };

            // If we haven't seen this region yet, add it
            if !regions.contains_key(&region_id) {
                regions.insert(
                    region_id.clone(),
                    RegionMetadata {
                        id: region_id.clone(),
                        name: format_region_name(&region_id),
                        origin,
                        ws_url,
                    },
                );
            }
        }

        // Update cache
        let mut cache = self.cache.write().await;
        *cache = regions.clone();

        info!("Region cache refreshed: {} regions found", regions.len());
        debug!("Available regions: {:?}", regions.keys().collect::<Vec<_>>());

        Ok(())
    }

    /// Get all cached regions
    pub async fn get_regions(&self) -> Vec<RegionMetadata> {
        let cache = self.cache.read().await;
        cache.values().cloned().collect()
    }

    /// Start background refresh task that runs every 30 seconds
    pub fn spawn_refresh_task(
        self: Arc<Self>,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));

            // Do initial refresh
            if let Err(e) = self.refresh().await {
                error!("Failed initial region cache refresh: {}", e);
            }

            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        info!("Region cache refresh task shutting down");
                        break;
                    }
                    _ = interval.tick() => {
                        if let Err(e) = self.refresh().await {
                            error!("Failed to refresh region cache: {}", e);
                        }
                    }
                }
            }
        });
    }
}

/// Format region ID into human-readable name
fn format_region_name(region_id: &str) -> String {
    match region_id {
        "us" => "US".to_string(),
        "europe" => "Europe".to_string(),
        "asia" => "Asia".to_string(),
        _ => region_id.to_uppercase(),
    }
}
