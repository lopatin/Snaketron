use crate::db::Database;
use crate::db::models::User;
use crate::redis_keys::RedisKeys;
use crate::redis_utils::RedisConnection;
use anyhow::{Context, Result, anyhow};
use redis::AsyncCommands;
use std::collections::HashMap;
use std::sync::Arc;

const EXPIRATION_SECONDS: u64 = 3600; // 1 hour

// A guest-to-account conversion is monotonic. A cache miss may read the guest
// record immediately before that conversion commits, then reach Redis after the
// upgraded account has already been written through. Keep the comparison and
// write atomic so that late guest fill can never restore the stale identity.
const PUT_USER_SCRIPT: &str = r#"
local cached = redis.call('GET', KEYS[1])
if cached and ARGV[2] == '1' then
    local decoded_ok, cached_user = pcall(cjson.decode, cached)
    if decoded_ok then
        local cached_is_guest = cached_user['is_guest']
        if cached_is_guest == nil then
            cached_is_guest = cached_user['isGuest']
        end
        if cached_is_guest == false then
            redis.call('EXPIRE', KEYS[1], ARGV[3])
            return 0
        end
    end
end

redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[3])
return 1
"#;

#[derive(Clone)]
pub struct UserCache {
    redis: RedisConnection,
    db: Arc<dyn Database>,
}

impl UserCache {
    pub fn new(redis: RedisConnection, db: Arc<dyn Database>) -> Self {
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

    pub async fn get_all(&self, user_ids: &[u32]) -> Result<Vec<Option<User>>> {
        let mut results = HashMap::new();
        let mut missing_ids = Vec::new();

        if user_ids.is_empty() {
            return Ok(Vec::new());
        }

        // First try to get from Redis
        let mut redis = self.redis.clone();
        // User cache keys intentionally distribute across slots. Independent
        // GETs preserve that distribution without relying on a cross-slot
        // MGET implementation.
        let mut user_jsons: Vec<Option<String>> = Vec::with_capacity(user_ids.len());
        for user_id in user_ids {
            user_jsons.push(
                redis
                    .get(RedisKeys::user(*user_id))
                    .await
                    .map_err(|e| anyhow!("Failed to get user JSON from Redis: {e}"))?,
            );
        }

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

        user_ids.iter().map(|&id| Ok(results.remove(&id))).collect()
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
        let user_json = redis
            .get::<_, Option<String>>(user_key)
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
        let user_json =
            serde_json::to_string(user).context("Failed to serialize user to json for Redis")?;
        let mut redis = self.redis.clone();
        redis::Script::new(PUT_USER_SCRIPT)
            .key(RedisKeys::user(user.id as u32))
            .arg(user_json)
            .arg(if user.is_guest { 1 } else { 0 })
            .arg(EXPIRATION_SECONDS)
            .invoke_async::<i32>(&mut redis)
            .await
            .context("Failed to put user json to Redis with monotonic guest state")?;
        Ok(())
    }

    /// Write a newly upgraded account through to Redis. Combined with the
    /// monotonic cache-fill script, this prevents a concurrent stale guest read
    /// from overwriting the account after the durable conversion commits.
    pub async fn replace_after_guest_upgrade(&self, user: &User) -> Result<()> {
        if user.is_guest {
            return Err(anyhow!(
                "Cannot cache a guest as the result of an account upgrade"
            ));
        }
        self.put_to_redis(user).await
    }

    pub async fn remove_from_redis(&self, user_id: u32) -> Result<()> {
        self.redis
            .clone()
            .del::<_, ()>(RedisKeys::user(user_id))
            .await
            .context("Failed to remove user from Redis")
    }

    async fn touch(&self, user_id: u32) -> Result<()> {
        self.redis
            .clone()
            .expire::<_, ()>(RedisKeys::user(user_id), EXPIRATION_SECONDS as i64)
            .await
            .context("Failed to touch user cache expiration")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redis_utils::RedisClient;
    use serde_json::json;

    async fn script_put(
        redis: &mut RedisConnection,
        key: &str,
        value: serde_json::Value,
        is_guest: bool,
    ) -> Result<i32> {
        redis::Script::new(PUT_USER_SCRIPT)
            .key(key)
            .arg(value.to_string())
            .arg(if is_guest { 1 } else { 0 })
            .arg(EXPIRATION_SECONDS)
            .invoke_async(redis)
            .await
            .context("Failed to invoke monotonic user-cache script")
    }

    #[tokio::test]
    async fn stale_guest_fill_cannot_replace_cached_account() -> Result<()> {
        let client = RedisClient::open("redis://127.0.0.1:6379/15?protocol=resp3", None)?;
        let mut redis = client.get_managed_connection().await?;
        let key = format!("test:user-cache-upgrade:{}", uuid::Uuid::new_v4());

        assert_eq!(
            script_put(
                &mut redis,
                &key,
                json!({ "id": 42, "username": "Guest42", "is_guest": true }),
                true,
            )
            .await?,
            1
        );
        assert_eq!(
            script_put(
                &mut redis,
                &key,
                json!({ "id": 42, "username": "Player42", "is_guest": false }),
                false,
            )
            .await?,
            1
        );
        assert_eq!(
            script_put(
                &mut redis,
                &key,
                json!({ "id": 42, "username": "Guest42", "is_guest": true }),
                true,
            )
            .await?,
            0
        );

        let cached: String = redis.get(&key).await?;
        let cached: serde_json::Value = serde_json::from_str(&cached)?;
        assert_eq!(cached["username"], "Player42");
        assert_eq!(cached["is_guest"], false);

        redis.del::<_, ()>(&key).await?;
        Ok(())
    }
}
