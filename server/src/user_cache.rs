use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use redis::{AsyncCommands, Client};
use redis::aio::ConnectionManager;
use crate::db::Database;
use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use tracing::{error, info};
use crate::db::models::{LobbyMetadata, User};
use crate::redis_keys::RedisKeys;

const EXPIRATION_SECONDS: u64 = 3600; // 1 hour

#[derive(Clone)]
pub struct UserCache {
    redis: ConnectionManager,
    db: Arc<dyn Database>
}

impl UserCache {
    pub fn new(redis: ConnectionManager, db: Arc<dyn Database>) -> Self {
        Self { redis, db }
    }

    pub async fn get(&self, user_id: u32) -> Result<Option<User>> {
        if let Some(user) = self.get_from_redis(user_id).await? {
            self.touch(user_id).await?;
            return Ok(Some(user));
        }

        if let Some(user) = self.get_from_db(user_id).await? {
            self.put_to_redis(&user).await?;
            return Ok(Some(user));
        }

        Ok(None)
    }
    
    // use redis mget
    pub async fn get_all(&self, user_ids: &[u32]) -> Result<Vec<Option<User>>> {
        let mut results = HashMap::new();
        let mut missing_ids = Vec::new();
        
        if user_ids.is_empty() {
            return Ok(Vec::new());
        }

        // First try to get from Redis
        let mut redis = self.redis.clone();
        let keys: Vec<String> = user_ids.iter().map(|&id| RedisKeys::user(id)).collect();
        let user_jsons: Vec<Option<String>> = redis.mget(keys).await
            .map_err(|e| anyhow!("Failed to mget user jsons from Redis: {}", e))?;

        for (i, user_json_opt) in user_jsons.into_iter().enumerate() {
            if let Some(user_json) = user_json_opt {
                let user: User = serde_json::from_str(&user_json)
                    .context("Failed to deserialize user json from Redis")?;
                self.touch(user.id as u32).await?;
                results.insert(user.id as u32, user);
            } else {
                missing_ids.push(user_ids[i]);
            }
        }

        // For missing IDs, get from DB and put to Redis
        for &user_id in &missing_ids {
            if let Some(user) = self.get_from_db(user_id).await? {
                self.put_to_redis(&user).await?;
                results.insert(user.id as u32, user);
            }
        }

        user_ids.iter()
            .map(|&id| Ok(results.remove(&id)))
            .collect()
    }
    
    pub async fn get_force(&self, user_id: u32) -> Result<Option<User>> {
        self.remove_from_redis(user_id).await?;
        self.get(user_id).await
    }
    
    pub async fn get_all_force(&self, user_ids: &[u32]) -> Result<Vec<Option<User>>> {
        for &user_id in user_ids {
            self.remove_from_redis(user_id).await?;
        }
        self.get_all(user_ids).await
    }

    async fn get_from_db(&self, user_id: u32) -> Result<Option<User>> {
        self.db.get_user_by_id(user_id as i32).await
    }

    async fn get_from_redis(&self, user_id: u32) -> Result<Option<User>> {
        let mut redis = self.redis.clone();
        let user_key = RedisKeys::user(user_id);
        let user_json = redis.get::<_, Option<String>>(user_key)
            .await
            .context("Failed to get user json from Redis")?;
        let user = if let Some(user_json) = user_json {
            let user: User = serde_json::from_str(&user_json)
                .context("Failed to deserialize user json from Redis")?;
            Some(user)
        } else {
            None
        };
        Ok(user)
    }
    
    async fn put_to_redis(&self, user: &User) -> Result<()> {
        self.redis.clone()
            .set_ex::<_, _, ()>(
                RedisKeys::user(user.id as u32), 
                serde_json::to_string(user) 
                    .context("Failed to serialize user to json for Redis")?, 
                EXPIRATION_SECONDS)
            .await
            .context("Failed to put user json to Redis with expiration")
    }
    
    pub async fn remove_from_redis(&self, user_id: u32) -> Result<()> {
        self.redis.clone()
            .del::<_, ()>(RedisKeys::user(user_id))
            .await
            .context("Failed to remove user from Redis")
    }

    async fn touch(&self, user_id: u32) -> Result<()> {
        self.redis.clone()
            .expire::<_, ()>(RedisKeys::user(user_id), EXPIRATION_SECONDS as i64)
            .await
            .context("Failed to touch user cache expiration")
    }

}
