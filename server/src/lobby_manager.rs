use anyhow::{Context, Result, anyhow, bail};
use redis::{AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use serde_json::{self, Value as JsonValue, json};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};
use std::sync::RwLock;
use indexmap::IndexMap;
use redis::aio::ConnectionManager;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::{Duration, interval};
use tracing::{debug, error, info, warn};

use crate::db::{Database, models::LobbyMetadata};
use crate::lobby_manager;
use crate::redis_keys::RedisKeys;
use crate::redis_utils::create_connection_manager;
use crate::user_cache::UserCache;

/// Lobby member information stored in Redis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LobbyMember {
    pub user_id: u32,
    pub username: String,
    pub ts: f64,
}

/// A struct that represents the value stored for a lobby member in Redis
#[derive(Debug, Clone)]
struct MemberValue {
    user_id: u32,
    websocket_id: String,
}

impl std::fmt::Display for MemberValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.user_id, self.websocket_id)
    }
}

impl std::str::FromStr for MemberValue {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (user_id, websocket_id) = s
            .split_once(':')
            .ok_or(anyhow!("Invalid member value format"))?;
        
        let user_id: u32 = user_id
            .parse()
            .map_err(|_| anyhow!("Invalid user_id in member value"))?;
        
        Ok(MemberValue {
            user_id,
            websocket_id: websocket_id.to_string(),
        })
    }
}

/// Lobby information stored in Redis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lobby {
    lobby_code: String,
    members: BTreeMap<u32, LobbyMember>,
    host_user_id: i32,
    state: String,
    preferences: LobbyPreferences,
}

impl Lobby {
    pub fn lobby_code(&self) -> &str {
        &self.lobby_code
    }
}

/// Handle for a lobby join that manages the heartbeat task
pub struct LobbyJoinHandle {
    heartbeat_task: JoinHandle<()>,
    lobby_manager: Arc<LobbyManager>,
    returned: RwLock<bool>,
    pub rx: Receiver<Lobby>,
    pub lobby_code: String,
    pub user_id: i32,
    pub websocket_id: String,
}

impl LobbyJoinHandle {
    pub async fn close(&mut self) -> Result<()> {
        self.heartbeat_task.abort();
        self.return_to_manager();
        self.lobby_manager
            .leave_lobby(&self.lobby_code, self.user_id, &self.websocket_id)
            .await
    }

    fn return_to_manager(&mut self) {
        let mut returned = self.returned.write().unwrap();
        if !*returned {
            self.lobby_manager.return_handle(self);
            *returned = true;
        }
    }
}

impl Drop for LobbyJoinHandle {
    fn drop(&mut self) {
        self.heartbeat_task.abort();
        self.return_to_manager();
    }
}

/// Host-selected matchmaking preferences for a lobby
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LobbyPreferences {
    // TODO: Use an enum for selected modes instead of a string
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

struct LobbyBroadcaster {
    tx: Sender<Lobby>,
    receiver_count: usize
}

type LobbyBroadcasters = RwLock<HashMap<String, LobbyBroadcaster>>;

/// Manages lobby membership and presence using Redis heartbeats
pub struct LobbyManager {
    redis: ConnectionManager,
    db: Arc<dyn Database>,
    lobby_broadcasters: LobbyBroadcasters,
    user_cache: Arc<UserCache>,
}

impl LobbyManager {
    pub fn new(redis: ConnectionManager, db: Arc<dyn Database>) -> Self {
        Self {
            redis: redis.clone(),
            db: db.clone(),
            lobby_broadcasters: RwLock::new(HashMap::new()),
            user_cache: Arc::new(UserCache::new(
                redis.clone(),
                db.clone(),
            )),
        }
    }

    /// Create a new lobby. Generates an id and assigns the host user.
    pub async fn create_lobby(&self, host_user_id: i32, region: &str) -> Result<Lobby> {
        use chrono::{Duration, Utc};

        let now = Utc::now();
        let lobby_code = self.generate_unique_lobby_code(region, 10).await?;

        let lobby_metadata = LobbyMetadata {
            lobby_code,
            host_user_id,
            region: region.to_string(),
            created_at: now,
            state: "waiting".to_string(),
        };

        // Store lobby metadata in Redis
        self.save_lobby_metadata(&lobby_metadata)
            .await
            .context("Failed to store lobby metadata")?;

        // Initialize lobby preferences
        let preferences = LobbyPreferences::default();
        self.set_lobby_preferences(&lobby_metadata.lobby_code, &preferences)
            .await
            .context("Failed to initialize lobby preferences")?;

        self.touch_lobby(&lobby_metadata.lobby_code, None)
            .await
            .context("Failed to touch lobby on creation")?;

        info!(
            "Created lobby '{}' for user {} in region {}",
            lobby_metadata.lobby_code, host_user_id, region
        );

        Ok(Lobby {
            lobby_code: lobby_metadata.lobby_code,
            members: BTreeMap::new(),
            host_user_id: lobby_metadata.host_user_id,
            state: lobby_metadata.state,
            preferences,
        })
    }

    /// Save lobby metadata to Redis
    async fn save_lobby_metadata(&self, metadata: &LobbyMetadata) -> Result<()> {
        use redis::AsyncCommands;

        let mut redis = self.redis.clone();
        let metadata_key = RedisKeys::lobby_metadata(&metadata.lobby_code);

        redis.hset_multiple::<_, _, _, ()>(
            &metadata_key,
            &[
                ("hostUserId", metadata.host_user_id.to_string()),
                ("region", metadata.region.to_string()),
                ("createdAt", metadata.created_at.to_rfc3339()),
                ("state", metadata.state.to_string()),
            ],
        )
        .await
        .context("Failed to store lobby metadata")?;

        Ok(())
    }

    /// Start heartbeat loop for user in lobby
    /// Returns a handle that automatically cancels the heartbeat on drop
    pub async fn join_lobby(
        self: &Arc<Self>,
        lobby_code: Option<&str>,
        user_id: i32,
        username: String,
        websocket_id: String,
        region: String,
    ) -> Result<LobbyJoinHandle> {
        let lobby = if let Some(lobby_code) = lobby_code {
            self.get_lobby(lobby_code).await?
        } else {
            self.create_lobby(user_id, &region).await?
        };

        let member_value = MemberValue { 
            user_id: user_id as u32, 
            websocket_id: websocket_id.clone()
        };
        
        self.touch_lobby(&lobby.lobby_code, Some(member_value.clone()))
            .await
            .context("Failed to touch lobby on join")?;

        // Heartbeat task
        let self_for_heartbeat = self.clone();
        let lobby_code_for_heartbeat = lobby.lobby_code.clone();
        let task = tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                if let Err(err) = self_for_heartbeat
                    .touch_lobby(lobby_code_for_heartbeat.as_str(), Some(member_value.clone()))
                    .await
                {
                    error!(
                        "Failed to send heartbeat for user {}: {}",
                        user_id, err
                    );
                }
            }
        });

        info!("User {} joined lobby '{}'", user_id, lobby.lobby_code);

        // Subscribe to lobby updates
        let rx = {
            let broadcasters = &mut self.lobby_broadcasters.write().unwrap();
            let broadcaster = broadcasters.entry(lobby.lobby_code.clone()).or_insert_with(|| {
                let (tx, _) = tokio::sync::broadcast::channel(100);
                LobbyBroadcaster { tx, receiver_count: 0 }
            });
            broadcaster.receiver_count += 1;
            broadcaster.tx.subscribe()
        };

        // Store the handle
        let handle = LobbyJoinHandle {
            heartbeat_task: task,
            lobby_manager: self.clone(),
            returned: RwLock::new(false),
            rx,
            lobby_code: lobby.lobby_code,
            user_id,
            websocket_id,
        };

        Ok(handle)
    }

    pub fn return_handle(&self, handle: &LobbyJoinHandle) {
        let mut broadcasters = self.lobby_broadcasters.write().unwrap();
        if let Some(broadcaster) = broadcasters.get_mut(&handle.lobby_code) {
            if broadcaster.receiver_count > 0 {
                broadcaster.receiver_count -= 1;
            }
            if broadcaster.receiver_count == 0 {
                broadcasters.remove(&handle.lobby_code);
            }
        }
    }

    pub async fn get_lobby(&self, lobby_code: &str) -> Result<Lobby> {
        self.get_lobby_opt(lobby_code)
            .await?
            .ok_or_else(|| anyhow!("Lobby '{}' not found", lobby_code))
    }

    pub async fn get_lobby_opt(&self, lobby_code: &str) -> Result<Option<Lobby>> {
        if let Some(lobby_model) = self.get_lobby_metadata(lobby_code).await? {
            let members = self.get_lobby_members(lobby_code).await?;
            let preferences = self.get_lobby_preferences(lobby_code).await?;
            Ok(Some(Lobby {
                lobby_code: lobby_model.lobby_code,
                members,
                host_user_id: lobby_model.host_user_id,
                state: lobby_model.state,
                preferences,
            }))
        } else {
            Ok(None)
        }
    }

    /// Get lobby by code from Redis
    pub async fn get_lobby_metadata(&self, lobby_code: &str) -> Result<Option<LobbyMetadata>> {
        use chrono::{DateTime, Utc};
        use redis::AsyncCommands;
        use std::collections::HashMap;

        let metadata_key = RedisKeys::lobby_metadata(lobby_code);
        let mut redis = self.redis.clone();

        // Check if lobby exists
        if !redis
            .exists(&metadata_key)
            .await
            .context("Failed to check lobby existence")? 
        {
            return Ok(None);
        }

        // Fetch all metadata fields
        let data: HashMap<String, String> = redis
            .hgetall(&metadata_key)
            .await
            .context("Failed to fetch lobby metadata")?;

        // Parse and construct Lobby
        let lobby = LobbyMetadata {
            lobby_code: lobby_code.to_string(),
            host_user_id: data
                .get("hostUserId")
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| anyhow!("Invalid or missing hostUserId"))?,
            region: data
                .get("region")
                .ok_or_else(|| anyhow!("Missing region"))?
                .to_string(),
            created_at: data
                .get("createdAt")
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&Utc))
                .ok_or_else(|| anyhow!("Invalid or missing createdAt"))?,
            state: data
                .get("state")
                .ok_or_else(|| anyhow!("Missing state"))?
                .to_string(),
        };

        Ok(Some(lobby))
    }

    /// Delete a lobby and all associated Redis keys
    pub async fn delete_lobby(&self, lobby_code: &str) -> Result<()> {
        use redis::AsyncCommands;

        // Delete all lobby-related keys
        let keys = vec![
            RedisKeys::lobby_metadata(lobby_code),
            RedisKeys::lobby_members_set(lobby_code),
            RedisKeys::lobby_preferences(lobby_code),
            RedisKeys::lobby_chat_history_key(lobby_code),
        ];

        self.redis.clone()
            .del::<_, ()>(keys)
            .await
            .context("Failed to delete lobby keys from Redis")?;

        info!("Deleted lobby '{}'", lobby_code);
        Ok(())
    }

    /// Stop heartbeat and remove from Redis
    pub async fn leave_lobby(&self, lobby_code: &str, user_id: i32, websocket_id: &str) -> Result<()> {
        let mut redis = self.redis.clone();

        // Remove from Redis sorted set
        let members_key = RedisKeys::lobby_members_set(lobby_code);
        let member_value = format!("{}:{}", user_id, websocket_id);
        redis.zrem::<_, _, ()>(&members_key, &member_value).await?;
        
        // Publish lobby update
        self.publish_lobby_update(lobby_code).await
    }

    /// Get all active members of a lobby from Redis
    pub async fn get_lobby_members(&self, lobby_code: &str) -> Result<BTreeMap<u32, LobbyMember>> {
        let mut redis = self.redis.clone();
        let members_key = RedisKeys::lobby_members_set(lobby_code);
        let current_time = chrono::Utc::now().timestamp_millis();

        // Remove expired members (score < current_time)
        let _: () = redis
            .zrembyscore(&members_key, "-inf", current_time)
            .await
            .context("Failed to remove expired lobby members")?;

        // Get all remaining members with scores
        let members_with_scores: Vec<(String, f64)> = redis
            .zrange_withscores(&members_key, 0, -1)
            .await
            .context("Failed to get lobby members from sorted set")?;
        
        let user_ids: Vec<u32> = members_with_scores.iter()
            .map(|(member_value, score)| {
                member_value.splitn(2, ':')
                    .nth(0)
                    .and_then(|id_str| id_str.parse::<u32>().ok())
            })
            .flatten()
            .collect();
        
        let users = self.user_cache
            .get_all(&user_ids).await?.iter().flatten() 
            .map(|u| (u.id as u32, u.username.clone()))
            .collect::<HashMap<u32, String>>();

        // Parse and deduplicate by user_id (keeping highest score = latest heartbeat)
        let mut members: BTreeMap<u32, LobbyMember> = BTreeMap::new();

        for (member_value, score) in members_with_scores {
            let user_id: Option<u32> = member_value.splitn(2, ':')
                .nth(0)
                .map(|id_str| id_str.parse::<u32>().ok())
                .flatten();
            
            if let(Some(user_id)) = user_id {
                if let Some(user) = users.get(&user_id) {
                    let username = user.clone();
                    
                    // Keep entry with highest score (most recent heartbeat)
                    members.entry(user_id)
                        .and_modify(|existing| {
                            if score > existing.ts {
                                *existing = LobbyMember { user_id, username: username.clone(), ts: score, }
                            }
                        })
                        .or_insert(LobbyMember { user_id, username: username.clone(), ts: score, });
                } else {
                    warn!("Username not found in cache for user_id {} in lobby '{}'", user_id, lobby_code);
                    continue;
                }
            } else {
                warn!("Invalid member value format in lobby '{}': {}", lobby_code, member_value);
                continue;
            }
        }

        debug!("Found {} unique members in lobby '{}'", members.len(), lobby_code);
        Ok(members)
    }

    /// Persist host-selected matchmaking preferences for the lobby
    pub async fn set_lobby_preferences(
        &self,
        lobby_code: &str,
        preferences: &LobbyPreferences,
    ) -> Result<()> {
        let mut redis = self.redis.clone();
        let key = RedisKeys::lobby_preferences(lobby_code);
        let payload =
            serde_json::to_string(preferences).context("Failed to serialize lobby preferences")?;

        redis.set::<_, _, ()>(&key, payload)
            .await
            .context("Failed to store lobby preferences")?;

        if let Err(e) = self.publish_lobby_update(lobby_code).await {
            warn!(
                "Failed to publish lobby update after preferences change for lobby '{}': {}",
                lobby_code, e
            );
        }

        Ok(())
    }

    /// Retrieve matchmaking preferences for the lobby, falling back to defaults
    pub async fn get_lobby_preferences(&self, lobby_code: &str) -> Result<LobbyPreferences> {
        let mut redis = self.redis.clone();
        let key = RedisKeys::lobby_preferences(lobby_code);
        let raw: Option<String> = redis
            .get(&key)
            .await
            .context("Failed to load lobby preferences")?;

        if let Some(json) = raw {
            match serde_json::from_str::<LobbyPreferences>(&json) {
                Ok(preferences) => Ok(preferences),
                Err(e) => {
                    warn!(
                        "Failed to parse lobby preferences for lobby '{}': {}",
                        lobby_code, e
                    );
                    Ok(LobbyPreferences::default())
                }
            }
        } else {
            Ok(LobbyPreferences::default())
        }
    }
    
    async fn touch_lobby(&self, lobby_code: &str, member: Option<MemberValue>) -> Result<()> {
        let mut redis = self.redis.clone();
        let expires_at = chrono::Utc::now().timestamp_millis() + 30000;
        
        let members_key = RedisKeys::lobby_members_set(lobby_code);

        // Add member to sorted set with expiration timestamp as score
        if let Some(member) = member {
            redis.zadd::<_, _, _, ()>(&members_key, member.to_string(), expires_at)
                .await
                .context("Failed to add lobby member to sorted set")?;
        }

        redis.expire::<_, ()>(&members_key, 30)
            .await
            .context("Failed to set TTL on lobby members set")?;

        let metadata_key = RedisKeys::lobby_metadata(lobby_code);
        redis.expire::<_, ()>(&metadata_key, 30)
            .await
            .context("Failed to set TTL on lobby metadata")?;

        let preferences_key = RedisKeys::lobby_preferences(lobby_code);
        redis.expire::<_, ()>(&preferences_key, 30)
            .await
            .context("Failed to set TTL on lobby preferences")?;

        let chat_history_key = RedisKeys::lobby_chat_history_key(lobby_code);
        redis.expire::<_, ()>(&chat_history_key, 30)
            .await
            .context("Failed to set TTL on lobby chat history")?;
        
        Ok(())
    }

    /// Update lobby state in Redis
    pub async fn update_lobby_state(&self, lobby_code: &str, state: &str) -> Result<()> {
        use redis::AsyncCommands;
        let metadata_key = RedisKeys::lobby_metadata(lobby_code);

        let mut redis = self.redis.clone();
        redis
            .hset::<_, _, _, ()>(&metadata_key, "state", state)
            .await
            .context("Failed to update lobby state in Redis")?;

        info!("Updated lobby '{}' state to {}", lobby_code, state);

        if let Err(e) = self.publish_lobby_update(lobby_code).await {
            warn!(
                "Failed to publish lobby update after state change for lobby '{}': {}",
                lobby_code, e
            );
        }

        Ok(())
    }

    /// Helper to remove a key from Redis
    async fn remove_from_redis(&mut self, key: &str) -> Result<()> {
        let _: () = self.redis.del(key).await.context("Failed to delete Redis key")?;
        Ok(())
    }

    /// Map AWS region to 4-character code
    fn region_to_code(region: &str) -> String {
        // Configurable region code mapping
        // Format: AWS region string -> 4-character code
        match region {
            "us-east-1" => "USE1".to_string(),
            "eu-west-1" => "EUW1".to_string(),
            "ap-southeast-2" => "APS2".to_string(),
            "us-west-2" => "USW2".to_string(),
            // Add more regions as needed
            _ => {
                // For unknown regions, generate a code from the first letters
                // Example: "eu-central-1" -> "EUC1"
                let parts: Vec<&str> = region.split('-').collect();
                if parts.len() >= 2 {
                    let prefix: String = parts[0].chars().take(2).collect();
                    let suffix: String = parts[1].chars().take(1).collect();
                    let number = parts.get(2).unwrap_or(&"1");
                    format!("{}{}{}", prefix.to_uppercase(), suffix.to_uppercase(), number)
                } else {
                    // Fallback to first 4 characters
                    region.chars().take(4).collect::<String>().to_uppercase()
                }
            }
        }
    }

    /// Generate a random lobby code with region prefix
    /// Format: {REGION_CODE}-{8_CHAR_HASH} (e.g., USE1-A3B2C4D5)
    fn generate_lobby_code(region: &str) -> String {
        use rand::Rng;
        const CHARSET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // Exclude confusing chars
        let mut rng = rand::thread_rng();

        let region_code = Self::region_to_code(region);
        let hash: String = (0..8)
            .map(|_| {
                let idx = rng.gen_range(0..CHARSET.len());
                CHARSET[idx] as char
            })
            .collect();

        format!("{}-{}", region_code, hash)
    }

    /// Generate a unique lobby code with collision detection
    /// Retries up to max_attempts times if a collision is detected
    async fn generate_unique_lobby_code(&self, region: &str, max_attempts: usize) -> Result<String> {
        use redis::AsyncCommands;
        let mut redis = self.redis.clone();

        for attempt in 0..max_attempts {
            let code = Self::generate_lobby_code(region);
            let metadata_key = RedisKeys::lobby_metadata(&code);

            // Check if this code already exists in Redis by checking metadata key
            let exists: bool = redis
                .exists(&metadata_key)
                .await
                .context("Failed to check lobby existence")?;

            if !exists {
                debug!("Generated unique lobby code '{}' on attempt {}", code, attempt + 1);
                return Ok(code);
            }

            warn!(
                "Lobby code collision on attempt {}/{}: {}",
                attempt + 1,
                max_attempts,
                code
            );
        }

        Err(anyhow!(
            "Failed to generate unique lobby code after {} attempts",
            max_attempts
        ))
    }

    /// Publish a lobby update to the lobby's Redis pub/sub channel
    pub async fn publish_lobby_update(&self, lobby_code: &str) -> Result<()> {
        let lobby = self.get_lobby(lobby_code).await?;
        let lobby_json = serde_json::to_string(&lobby)
            .context("Failed to serialize lobby for update notification")?;
        let _: () = self.redis.clone()
            .publish(RedisKeys::lobby_updates_channel(), lobby_json)
            .await
            .context("Failed to publish lobby update")?;
        debug!("Published update notification to lobby '{}'", lobby_code);
        Ok(())
    }

    /// Check if a user is the host of a lobby
    pub async fn is_lobby_host(&self, lobby_code: &str, user_id: i32) -> Result<bool> {
        if let Some(lobby) = self.get_lobby_metadata(lobby_code).await? {
            Ok(lobby.host_user_id == user_id)
        } else {
            Ok(false)
        }
    }
}

