use anyhow::{anyhow, Context, Result};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{Duration, interval};
use tracing::{debug, error, info, warn};

use crate::db::{Database, models::Lobby};

/// Lobby member information stored in Redis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LobbyMember {
    pub user_id: i32,
    pub username: String,
    pub joined_at: i64,
    pub is_host: bool,
}

/// Handle for a lobby join that manages the heartbeat task
pub struct LobbyJoinHandle {
    task: JoinHandle<()>,
    pub lobby_id: i32,
    pub user_id: i32,
    pub websocket_id: String,
}

impl Drop for LobbyJoinHandle {
    fn drop(&mut self) {
        // Automatically cancel heartbeat when handle is dropped
        self.task.abort();
        debug!(
            "Dropped LobbyJoinHandle for user {} in lobby {} (websocket: {})",
            self.user_id, self.lobby_id, self.websocket_id
        );
    }
}

/// Manages lobby membership and presence using Redis heartbeats
pub struct LobbyManager {
    redis_url: String,
    db: Arc<dyn Database>,
    /// Tracks active heartbeat tasks for this server's websockets
    /// Key: (lobby_id, user_id, websocket_id)
    active_joins: Arc<RwLock<HashMap<(i32, i32, String), LobbyJoinHandle>>>,
}

impl LobbyManager {
    pub fn new(redis_url: String, db: Arc<dyn Database>) -> Self {
        Self {
            redis_url,
            db,
            active_joins: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new lobby for a user
    pub async fn create_lobby(&self, host_user_id: i32, region: &str) -> Result<Lobby> {
        let lobby = self.db.create_lobby(host_user_id, region).await?;
        info!(
            "Created lobby {} with code '{}' for user {} in region {}",
            lobby.id, lobby.lobby_code, host_user_id, region
        );
        Ok(lobby)
    }

    /// Start heartbeat loop for user in lobby
    /// Returns a handle that automatically cancels the heartbeat on drop
    pub async fn join_lobby(
        &self,
        lobby_id: i32,
        user_id: i32,
        username: String,
        websocket_id: String,
        region: String,
    ) -> Result<()> {
        // Get lobby to verify it exists and get host info
        let lobby = self.get_lobby(lobby_id).await?
            .ok_or_else(|| anyhow!("Lobby {} not found", lobby_id))?;

        let is_host = lobby.host_user_id == user_id;

        // Spawn heartbeat task
        let redis_url = self.redis_url.clone();
        let websocket_id_for_task = websocket_id.clone();
        let websocket_id_for_handle = websocket_id.clone();
        let websocket_id_for_map = websocket_id.clone();

        let task = tokio::spawn(async move {
            if let Err(e) = heartbeat_loop(
                redis_url,
                lobby_id,
                user_id,
                username,
                websocket_id_for_task,
                region,
                is_host,
            ).await {
                error!(
                    "Heartbeat loop failed for user {} in lobby {}: {}",
                    user_id, lobby_id, e
                );
            }
        });

        // Store the handle
        let handle = LobbyJoinHandle {
            task,
            lobby_id,
            user_id,
            websocket_id: websocket_id_for_handle,
        };

        let mut joins = self.active_joins.write().await;
        joins.insert((lobby_id, user_id, websocket_id_for_map), handle);

        info!(
            "User {} joined lobby {} (websocket: {}, is_host: {})",
            user_id, lobby_id, websocket_id, is_host
        );

        Ok(())
    }

    /// Stop heartbeat and remove from Redis
    pub async fn leave_lobby(
        &self,
        lobby_id: i32,
        user_id: i32,
        websocket_id: &str,
    ) -> Result<()> {
        // Remove the join handle (this will abort the heartbeat task via Drop)
        let mut joins = self.active_joins.write().await;
        let key = (lobby_id, user_id, websocket_id.to_string());

        if joins.remove(&key).is_some() {
            info!(
                "User {} left lobby {} (websocket: {})",
                user_id, lobby_id, websocket_id
            );

            // Immediately remove from Redis
            let redis_key = format!("lobby:{}:member:{}:{}", lobby_id, user_id, websocket_id);
            if let Err(e) = self.remove_from_redis(&redis_key).await {
                warn!("Failed to remove lobby member from Redis: {}", e);
            }
        }

        Ok(())
    }

    /// Get all active members of a lobby from Redis
    pub async fn get_lobby_members(&self, lobby_id: i32) -> Result<Vec<LobbyMember>> {
        let client = redis::Client::open(self.redis_url.as_str())
            .context("Failed to open Redis client")?;

        let mut conn = client.get_multiplexed_async_connection().await
            .context("Failed to get Redis connection")?;

        // Get all keys matching the pattern
        let pattern = format!("lobby:{}:member:*", lobby_id);
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut conn)
            .await
            .context("Failed to query lobby member keys")?;

        let mut members = Vec::new();

        for key in keys {
            // Get the value for this key
            let value: String = match conn.get(&key).await {
                Ok(v) => v,
                Err(e) => {
                    warn!("Failed to get value for key {}: {}", key, e);
                    continue;
                }
            };

            // Parse the JSON value
            let member: LobbyMember = match serde_json::from_str(&value) {
                Ok(m) => m,
                Err(e) => {
                    warn!("Failed to parse lobby member JSON: {}", e);
                    continue;
                }
            };

            members.push(member);
        }

        // Sort by joined_at timestamp
        members.sort_by_key(|m| m.joined_at);

        debug!("Found {} members in lobby {}", members.len(), lobby_id);
        Ok(members)
    }

    /// Get lobby by ID from DynamoDB
    pub async fn get_lobby(&self, lobby_id: i32) -> Result<Option<Lobby>> {
        self.db.get_lobby_by_id(lobby_id).await
    }

    /// Get lobby by code from DynamoDB
    pub async fn get_lobby_by_code(&self, lobby_code: &str) -> Result<Option<Lobby>> {
        self.db.get_lobby_by_code(lobby_code).await
    }

    /// Update lobby state in DynamoDB
    pub async fn update_lobby_state(&self, lobby_id: i32, state: &str) -> Result<()> {
        self.db.update_lobby_state(lobby_id, state).await
    }

    /// Helper to remove a key from Redis
    async fn remove_from_redis(&self, key: &str) -> Result<()> {
        let client = redis::Client::open(self.redis_url.as_str())
            .context("Failed to open Redis client")?;

        let mut conn = client.get_multiplexed_async_connection().await
            .context("Failed to get Redis connection")?;

        let _: () = conn.del(key).await
            .context("Failed to delete Redis key")?;

        Ok(())
    }

    /// Publish a lobby update to the lobby's Redis pub/sub channel
    pub async fn publish_lobby_update(&self, lobby_id: i32) -> Result<()> {
        let client = redis::Client::open(self.redis_url.as_str())
            .context("Failed to open Redis client")?;

        let mut conn = client.get_multiplexed_async_connection().await
            .context("Failed to get Redis connection")?;

        let channel = format!("lobby:{}:updates", lobby_id);

        // Publish a simple update notification
        let _: () = conn.publish(&channel, "update").await
            .context("Failed to publish lobby update")?;

        debug!("Published update notification to lobby {}", lobby_id);
        Ok(())
    }
}

/// Background heartbeat loop that refreshes lobby membership in Redis
async fn heartbeat_loop(
    redis_url: String,
    lobby_id: i32,
    user_id: i32,
    username: String,
    websocket_id: String,
    region: String,
    is_host: bool,
) -> Result<()> {
    let client = redis::Client::open(redis_url.as_str())
        .context("Failed to open Redis client for heartbeat")?;

    let mut conn = client.get_multiplexed_async_connection().await
        .context("Failed to get Redis connection for heartbeat")?;

    let key = format!("lobby:{}:member:{}:{}", lobby_id, user_id, websocket_id);
    let value = json!({
        "user_id": user_id,
        "username": username,
        "joined_at": chrono::Utc::now().timestamp_millis(),
        "is_host": is_host,
    });

    let update_channel = format!("lobby:{}:updates", lobby_id);
    let mut interval = interval(Duration::from_secs(10));

    loop {
        interval.tick().await;

        // Set key with 30-second TTL
        match conn.set_ex::<_, _, ()>(&key, value.to_string(), 30).await {
            Ok(_) => {
                debug!(
                    "Refreshed lobby presence for user {} in lobby {} (websocket: {})",
                    user_id, lobby_id, websocket_id
                );

                // Publish update notification to lobby channel
                if let Err(e) = conn.publish::<_, _, ()>(&update_channel, "update").await {
                    warn!("Failed to publish lobby update: {}", e);
                }
            }
            Err(e) => {
                error!("Failed to refresh lobby presence: {}", e);
                break;
            }
        }
    }

    Ok(())
}
