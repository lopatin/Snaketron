use anyhow::{Context, Result, anyhow};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::{self, Value as JsonValue, json};
use std::collections::{HashMap, HashSet};
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
    pub username: Arc<RwLock<String>>,
}

/// Host-selected matchmaking preferences for a lobby
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LobbyPreferences {
    pub selected_modes: Vec<String>,
    pub competitive: bool,
}

impl Default for LobbyPreferences {
    fn default() -> Self {
        Self {
            selected_modes: vec!["duel".to_string()],
            competitive: false,
        }
    }
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

        if let Err(e) = self
            .set_lobby_preferences(lobby.id, &LobbyPreferences::default())
            .await
        {
            warn!(
                "Failed to initialize preferences for lobby {}: {}",
                lobby.id, e
            );
        }
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
        let lobby = self
            .get_lobby(lobby_id)
            .await?
            .ok_or_else(|| anyhow!("Lobby {} not found", lobby_id))?;

        let is_host = lobby.host_user_id == user_id;
        let joined_at = chrono::Utc::now().timestamp_millis();
        let username_state = Arc::new(RwLock::new(username.clone()));

        // Write member to Redis immediately (before spawning background task)
        // This ensures get_lobby_members() will find the member right away
        let client =
            redis::Client::open(self.redis_url.as_str()).context("Failed to open Redis client")?;
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .context("Failed to get Redis connection")?;

        let key = format!("lobby:{}:member:{}:{}", lobby_id, user_id, websocket_id);
        let value = build_member_value(user_id, &username, joined_at, is_host);

        conn.set_ex::<_, _, ()>(&key, value, 30)
            .await
            .context("Failed to write initial lobby member to Redis")?;

        debug!(
            "Wrote initial lobby presence for user {} in lobby {} (websocket: {})",
            user_id, lobby_id, websocket_id
        );

        // Spawn heartbeat task
        let redis_url = self.redis_url.clone();
        let websocket_id_for_task = websocket_id.clone();
        let websocket_id_for_handle = websocket_id.clone();
        let websocket_id_for_map = websocket_id.clone();
        let username_for_task = username_state.clone();

        let task = tokio::spawn(async move {
            if let Err(e) = heartbeat_loop(
                redis_url,
                lobby_id,
                user_id,
                username_for_task,
                websocket_id_for_task,
                region,
                is_host,
                joined_at,
            )
            .await
            {
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
            username: username_state.clone(),
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
    pub async fn leave_lobby(&self, lobby_id: i32, user_id: i32, websocket_id: &str) -> Result<()> {
        // Remove the join handle (this will abort the heartbeat task via Drop)
        let mut joins = self.active_joins.write().await;
        let key = (lobby_id, user_id, websocket_id.to_string());
        let was_present = joins.remove(&key).is_some();
        drop(joins);

        if was_present {
            info!(
                "User {} left lobby {} (websocket: {})",
                user_id, lobby_id, websocket_id
            );
        }

        // Immediately remove from Redis
        let redis_key = format!("lobby:{}:member:{}:{}", lobby_id, user_id, websocket_id);
        if let Err(e) = self.remove_from_redis(&redis_key).await {
            warn!("Failed to remove lobby member from Redis: {}", e);
        }

        if let Err(e) = self.publish_lobby_update(lobby_id).await {
            warn!("Failed to publish lobby update after leave: {}", e);
        }

        Ok(())
    }

    /// Update the username for a lobby member across active connections and Redis state
    pub async fn update_member_username(&self, user_id: i32, new_username: &str) -> Result<()> {
        let mut affected_lobbies = HashSet::new();

        {
            let mut joins = self.active_joins.write().await;
            for ((lobby_id, join_user_id, _), handle) in joins.iter_mut() {
                if *join_user_id == user_id {
                    {
                        let mut username = handle.username.write().await;
                        *username = new_username.to_string();
                    }
                    affected_lobbies.insert(*lobby_id);
                }
            }
        }

        if affected_lobbies.is_empty() {
            return Ok(());
        }

        for lobby_id in affected_lobbies {
            if let Err(e) = self
                .update_member_username_in_redis(lobby_id, user_id, new_username)
                .await
            {
                warn!(
                    "Failed to update Redis entry for user {} in lobby {}: {}",
                    user_id, lobby_id, e
                );
            }

            if let Err(e) = self.publish_lobby_update(lobby_id).await {
                warn!(
                    "Failed to publish lobby update after nickname change for lobby {}: {}",
                    lobby_id, e
                );
            }
        }

        Ok(())
    }

    /// Get all active members of a lobby from Redis
    pub async fn get_lobby_members(&self, lobby_id: i32) -> Result<Vec<LobbyMember>> {
        let client =
            redis::Client::open(self.redis_url.as_str()).context("Failed to open Redis client")?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
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

    /// Persist host-selected matchmaking preferences for the lobby
    pub async fn set_lobby_preferences(
        &self,
        lobby_id: i32,
        preferences: &LobbyPreferences,
    ) -> Result<()> {
        let client =
            redis::Client::open(self.redis_url.as_str()).context("Failed to open Redis client")?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .context("Failed to get Redis connection")?;

        let key = format!("lobby:{}:preferences", lobby_id);
        let payload =
            serde_json::to_string(preferences).context("Failed to serialize lobby preferences")?;

        conn.set::<_, _, ()>(&key, payload)
            .await
            .context("Failed to store lobby preferences")?;

        // Broadcast the update so connected clients refresh
        if let Err(e) = self.publish_lobby_update(lobby_id).await {
            warn!(
                "Failed to publish lobby update after preferences change for lobby {}: {}",
                lobby_id, e
            );
        }

        Ok(())
    }

    /// Retrieve matchmaking preferences for the lobby, falling back to defaults
    pub async fn get_lobby_preferences(&self, lobby_id: i32) -> Result<LobbyPreferences> {
        let client =
            redis::Client::open(self.redis_url.as_str()).context("Failed to open Redis client")?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .context("Failed to get Redis connection")?;

        let key = format!("lobby:{}:preferences", lobby_id);
        let raw: Option<String> = conn
            .get(&key)
            .await
            .context("Failed to load lobby preferences")?;

        if let Some(json) = raw {
            match serde_json::from_str::<LobbyPreferences>(&json) {
                Ok(preferences) => Ok(preferences),
                Err(e) => {
                    warn!(
                        "Failed to parse lobby preferences for lobby {}: {}",
                        lobby_id, e
                    );
                    Ok(LobbyPreferences::default())
                }
            }
        } else {
            Ok(LobbyPreferences::default())
        }
    }

    /// Update lobby state in DynamoDB
    pub async fn update_lobby_state(&self, lobby_id: i32, state: &str) -> Result<()> {
        self.db.update_lobby_state(lobby_id, state).await?;

        if let Err(e) = self.publish_lobby_update(lobby_id).await {
            warn!(
                "Failed to publish lobby update after state change for lobby {}: {}",
                lobby_id, e
            );
        }

        Ok(())
    }

    /// Helper to remove a key from Redis
    async fn remove_from_redis(&self, key: &str) -> Result<()> {
        let client =
            redis::Client::open(self.redis_url.as_str()).context("Failed to open Redis client")?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .context("Failed to get Redis connection")?;

        let _: () = conn.del(key).await.context("Failed to delete Redis key")?;

        Ok(())
    }

    async fn update_member_username_in_redis(
        &self,
        lobby_id: i32,
        user_id: i32,
        username: &str,
    ) -> Result<()> {
        let client = redis::Client::open(self.redis_url.as_str())
            .context("Failed to open Redis client for username update")?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .context("Failed to get Redis connection for username update")?;

        let pattern = format!("lobby:{}:member:{}:*", lobby_id, user_id);
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut conn)
            .await
            .context("Failed to query lobby member keys for username update")?;

        for key in keys {
            let value: String = match conn.get(&key).await {
                Ok(v) => v,
                Err(e) => {
                    warn!("Failed to fetch lobby member value for key {}: {}", key, e);
                    continue;
                }
            };

            let mut payload: JsonValue = match serde_json::from_str(&value) {
                Ok(json) => json,
                Err(e) => {
                    warn!("Failed to parse lobby member JSON for key {}: {}", key, e);
                    continue;
                }
            };

            if let Some(obj) = payload.as_object_mut() {
                obj.insert(
                    "username".to_string(),
                    JsonValue::String(username.to_string()),
                );
            }

            let serialized = match serde_json::to_string(&payload) {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        "Failed to serialize updated lobby member payload for key {}: {}",
                        key, e
                    );
                    continue;
                }
            };

            if let Err(e) = conn.set_ex::<_, _, ()>(&key, serialized, 30).await {
                warn!(
                    "Failed to write updated lobby member for key {}: {}",
                    key, e
                );
            }
        }

        Ok(())
    }

    /// Publish a lobby update to the lobby's Redis pub/sub channel
    pub async fn publish_lobby_update(&self, lobby_id: i32) -> Result<()> {
        let client =
            redis::Client::open(self.redis_url.as_str()).context("Failed to open Redis client")?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .context("Failed to get Redis connection")?;

        let channel = format!("lobby:{}:updates", lobby_id);

        // Publish a simple update notification
        let _: () = conn
            .publish(&channel, "update")
            .await
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
    username_state: Arc<RwLock<String>>,
    websocket_id: String,
    region: String,
    is_host: bool,
    joined_at: i64,
) -> Result<()> {
    let client = redis::Client::open(redis_url.as_str())
        .context("Failed to open Redis client for heartbeat")?;

    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("Failed to get Redis connection for heartbeat")?;

    let key = format!("lobby:{}:member:{}:{}", lobby_id, user_id, websocket_id);
    let value = {
        let username = username_state.read().await.clone();
        build_member_value(user_id, &username, joined_at, is_host)
    };

    let update_channel = format!("lobby:{}:updates", lobby_id);

    // Write to Redis immediately (don't wait for first interval tick)
    // This ensures get_lobby_members() will find the member right away
    if let Err(e) = conn.set_ex::<_, _, ()>(&key, value, 30).await {
        warn!("Failed to write initial lobby member to Redis: {}", e);
        return Err(e.into());
    }

    // Publish initial update
    if let Err(e) = conn.publish::<_, _, ()>(&update_channel, "update").await {
        warn!("Failed to publish initial lobby update: {}", e);
    }

    debug!(
        "Initial lobby presence written for user {} in lobby {} (websocket: {})",
        user_id, lobby_id, websocket_id
    );

    let mut interval = interval(Duration::from_secs(10));

    loop {
        interval.tick().await;

        // Set key with 30-second TTL
        let refreshed_value = {
            let username = username_state.read().await.clone();
            build_member_value(user_id, &username, joined_at, is_host)
        };

        match conn.set_ex::<_, _, ()>(&key, refreshed_value, 30).await {
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

fn build_member_value(user_id: i32, username: &str, joined_at: i64, is_host: bool) -> String {
    json!({
        "user_id": user_id,
        "username": username,
        "joined_at": joined_at,
        "is_host": is_host,
    })
    .to_string()
}
