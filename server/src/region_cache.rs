use anyhow::{Context, Result};
use aws_sdk_dynamodb::Client;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::api::regions::RegionMetadata;
use crate::db::SERVER_HEARTBEAT_FRESHNESS_SECONDS;

/// A cached region is only dropped after it has been absent from this many
/// consecutive refreshes, so a single stale query result can't flap the list.
const MAX_CONSECUTIVE_MISSES: u32 = 2;

/// Upper bound on a single refresh, kept below the 30s refresh interval so a
/// hung DynamoDB query can never wedge the refresh loop.
const REFRESH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(25);

/// Cache for region metadata scanned from DynamoDB
pub struct RegionCache {
    state: Arc<RwLock<CacheState>>,
    dynamodb_client: Client,
    table_prefix: String,
}

#[derive(Default)]
struct CacheState {
    regions: HashMap<String, RegionMetadata>,
    /// Consecutive refreshes each cached region has been missing from query results.
    miss_counts: HashMap<String, u32>,
}

impl RegionCache {
    /// Create a new region cache
    pub fn new(dynamodb_client: Client, table_prefix: String) -> Self {
        Self {
            state: Arc::new(RwLock::new(CacheState::default())),
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

        let cutoff =
            chrono::Utc::now() - chrono::Duration::seconds(SERVER_HEARTBEAT_FRESHNESS_SECONDS);

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
                aws_sdk_dynamodb::types::AttributeValue::S(cutoff.to_rfc3339()),
            )
            .projection_expression("id, #region, origin, wsUrl")
            .expression_attribute_names("#region", "region")
            .send()
            .await
            .context("Failed to query servers from DynamoDB")?;

        let items = response.items.unwrap_or_default();

        // Group servers by region
        let mut fresh: HashMap<String, RegionMetadata> = HashMap::new();

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
            if !fresh.contains_key(&region_id) {
                fresh.insert(
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
        let mut state = self.state.write().await;
        merge_refresh(&mut state, fresh);

        info!(
            "Region cache refreshed: {} regions found",
            state.regions.len()
        );
        debug!(
            "Available regions: {:?}",
            state.regions.keys().collect::<Vec<_>>()
        );

        Ok(())
    }

    /// Get all cached regions
    pub async fn get_regions(&self) -> Vec<RegionMetadata> {
        let state = self.state.read().await;
        state.regions.values().cloned().collect()
    }

    /// Start background refresh task that runs every 30 seconds
    pub fn spawn_refresh_task(
        self: Arc<Self>,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));

            // Do initial refresh
            match tokio::time::timeout(REFRESH_TIMEOUT, self.refresh()).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => error!("Failed initial region cache refresh: {}", e),
                Err(_) => error!("Initial region cache refresh timed out"),
            }

            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        info!("Region cache refresh task shutting down");
                        break;
                    }
                    _ = interval.tick() => {
                        match tokio::time::timeout(REFRESH_TIMEOUT, self.refresh()).await {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => error!("Failed to refresh region cache: {}", e),
                            Err(_) => error!("Region cache refresh timed out"),
                        }
                    }
                }
            }
        });
    }
}

/// Fold a refresh result into the cache. Regions present in `fresh` are updated
/// and their miss counters reset; cached regions absent from `fresh` are kept
/// until they have been missing for MAX_CONSECUTIVE_MISSES refreshes.
fn merge_refresh(state: &mut CacheState, fresh: HashMap<String, RegionMetadata>) {
    let missing: Vec<String> = state
        .regions
        .keys()
        .filter(|id| !fresh.contains_key(*id))
        .cloned()
        .collect();

    for id in missing {
        let misses = state.miss_counts.entry(id.clone()).or_insert(0);
        *misses += 1;
        if *misses >= MAX_CONSECUTIVE_MISSES {
            state.regions.remove(&id);
            state.miss_counts.remove(&id);
            info!(
                "Region {} dropped from cache after {} consecutive missing refreshes",
                id, MAX_CONSECUTIVE_MISSES
            );
        } else {
            warn!(
                "Region {} missing from refresh ({} consecutive); keeping cached entry",
                id, misses
            );
        }
    }

    for (id, meta) in fresh {
        state.miss_counts.remove(&id);
        state.regions.insert(id, meta);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(id: &str) -> RegionMetadata {
        RegionMetadata {
            id: id.to_string(),
            name: format_region_name(id),
            origin: format!("https://{}.example.com", id),
            ws_url: format!("wss://{}.example.com/ws", id),
        }
    }

    fn fresh(ids: &[&str]) -> HashMap<String, RegionMetadata> {
        ids.iter().map(|id| (id.to_string(), meta(id))).collect()
    }

    fn cached_ids(state: &CacheState) -> Vec<String> {
        let mut ids: Vec<String> = state.regions.keys().cloned().collect();
        ids.sort();
        ids
    }

    #[test]
    fn adds_new_regions_and_updates_existing() {
        let mut state = CacheState::default();
        merge_refresh(&mut state, fresh(&["use1"]));
        assert_eq!(cached_ids(&state), vec!["use1"]);

        merge_refresh(&mut state, fresh(&["use1", "euw1"]));
        assert_eq!(cached_ids(&state), vec!["euw1", "use1"]);
    }

    #[test]
    fn keeps_region_through_a_single_missing_refresh() {
        let mut state = CacheState::default();
        merge_refresh(&mut state, fresh(&["use1", "euw1"]));

        // use1 missing once: kept, miss counter at 1
        merge_refresh(&mut state, fresh(&["euw1"]));
        assert_eq!(cached_ids(&state), vec!["euw1", "use1"]);
        assert_eq!(state.miss_counts.get("use1"), Some(&1));
    }

    #[test]
    fn drops_region_after_consecutive_misses() {
        let mut state = CacheState::default();
        merge_refresh(&mut state, fresh(&["use1", "euw1"]));

        merge_refresh(&mut state, fresh(&["euw1"]));
        merge_refresh(&mut state, fresh(&["euw1"]));
        assert_eq!(cached_ids(&state), vec!["euw1"]);
        assert!(!state.miss_counts.contains_key("use1"));
    }

    #[test]
    fn reappearing_region_resets_miss_counter() {
        let mut state = CacheState::default();
        merge_refresh(&mut state, fresh(&["use1", "euw1"]));

        merge_refresh(&mut state, fresh(&["euw1"]));
        merge_refresh(&mut state, fresh(&["use1", "euw1"]));
        assert!(!state.miss_counts.contains_key("use1"));

        // A later single miss starts counting from zero again
        merge_refresh(&mut state, fresh(&["euw1"]));
        assert_eq!(cached_ids(&state), vec!["euw1", "use1"]);
        assert_eq!(state.miss_counts.get("use1"), Some(&1));
    }

    #[test]
    fn empty_refresh_eventually_empties_cache() {
        let mut state = CacheState::default();
        merge_refresh(&mut state, fresh(&["use1"]));

        merge_refresh(&mut state, HashMap::new());
        assert_eq!(cached_ids(&state), vec!["use1"]);
        merge_refresh(&mut state, HashMap::new());
        assert!(state.regions.is_empty());
        assert!(state.miss_counts.is_empty());
    }
}
