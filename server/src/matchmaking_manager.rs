use anyhow::{Result, Context, anyhow};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use common::GameType;
use chrono::Utc;
use crate::redis_keys::RedisKeys;
use crate::redis_utils;

// Data structures for Redis storage
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueuedPlayer {
    pub user_id: u32,
    pub mmr: i32,
    pub username: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserQueueStatus {
    pub game_type: GameType,
    pub request_time: i64,
    pub mmr: i32,
    pub username: String,
    pub matched_game_id: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ActiveMatch {
    pub players: Vec<QueuedPlayer>,
    pub game_type: GameType,
    pub status: MatchStatus,
    pub partition_id: u32,
    pub created_at: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum MatchStatus {
    Waiting,
    Active,
    Finished,
}

// Match notification sent via Pub/Sub
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum MatchNotification {
    MatchFound {
        game_id: u32,
        partition_id: u32,
        players: Vec<QueuedPlayer>,
    },
    QueueJoined {
        position: usize,
        estimated_wait_seconds: u32,
    },
    QueueLeft,
}

/// Redis-based matchmaking manager
#[derive(Clone)]
pub struct MatchmakingManager {
    conn: ConnectionManager,
    redis_keys: RedisKeys,
    max_retries: u32,
    retry_delay: Duration,
}

impl MatchmakingManager {
    /// Create a new Redis matchmaking manager
    pub async fn new(redis_url: &str) -> Result<Self> {
        let client = Client::open(redis_url)
            .context("Failed to create Redis client for matchmaking")?;
        
        let conn = Self::connect_with_retry(&client, 3).await?;
        
        Ok(Self {
            conn,
            redis_keys: RedisKeys::new(),
            max_retries: 3,
            retry_delay: Duration::from_millis(500),
        })
    }
    
    
    /// Connect to Redis with retry logic
    async fn connect_with_retry(client: &Client, _max_attempts: u32) -> Result<ConnectionManager> {
        // Use the standardized connection manager with built-in retry logic
        redis_utils::create_connection_manager(client.clone()).await
            .context("Failed to connect to Redis for matchmaking")
    }
    
    /// Add a player to the matchmaking queue
    pub async fn add_to_queue(&mut self, user_id: u32, username: String, mmr: i32, game_type: GameType) -> Result<()> {
        let queue_key = self.redis_keys.matchmaking_queue(&game_type);
        let mmr_key = self.redis_keys.matchmaking_mmr_index(&game_type);
        let user_key = self.redis_keys.matchmaking_user_status(user_id);
        
        let player = QueuedPlayer {
            user_id,
            mmr,
            username: username.clone(),
        };
        
        let player_json = serde_json::to_string(&player)?;
        let timestamp = Utc::now().timestamp_millis();
        
        // Start a transaction
        let mut pipe = redis::pipe();
        pipe.atomic();
        
        // Add to queue sorted set (score = timestamp for FIFO)
        pipe.zadd(&queue_key, player_json, timestamp);
        
        // Add to MMR index (score = MMR for range queries)
        pipe.zadd(&mmr_key, user_id.to_string(), mmr);
        
        // Store user status
        let status = UserQueueStatus {
            game_type: game_type.clone(),
            request_time: timestamp,
            mmr,
            username,
            matched_game_id: None,
        };
        let status_json = serde_json::to_string(&status)?;
        pipe.hset(&user_key, "status", status_json);
        
        // Execute transaction with retries
        let mut attempts = 0;
        let mut delay = self.retry_delay;
        
        loop {
            attempts += 1;
            match pipe.clone().query_async(&mut self.conn).await {
                Ok(()) => break,
                Err(e) if attempts < self.max_retries => {
                    warn!("Failed to add to queue (attempt {}/{}): {}", attempts, self.max_retries, e);
                    sleep(delay).await;
                    delay *= 2;
                }
                Err(e) => {
                    error!("Failed to add to queue after {} attempts", self.max_retries);
                    return Err(anyhow!("Failed to add to queue: {}", e));
                }
            }
        }
        
        info!("Added user {} to matchmaking queue for {:?}", user_id, game_type);
        Ok(())
    }
    
    /// Remove a player from the matchmaking queue
    pub async fn remove_from_queue(&mut self, user_id: u32) -> Result<Option<GameType>> {
        let user_key = self.redis_keys.matchmaking_user_status(user_id);
        
        // Get user status to find their game type
        let status_json: Option<String> = self.conn.hget(&user_key, "status").await?;
        
        if let Some(json) = status_json {
            let status: UserQueueStatus = serde_json::from_str(&json)?;
            let queue_key = self.redis_keys.matchmaking_queue(&status.game_type);
            let mmr_key = self.redis_keys.matchmaking_mmr_index(&status.game_type);
            
            // Find and remove the player from queue
            let members: Vec<(String, f64)> = self.conn.zrange_withscores(&queue_key, 0, -1).await?;
            
            for (member_json, _score) in members {
                if let Ok(player) = serde_json::from_str::<QueuedPlayer>(&member_json) {
                    if player.user_id == user_id {
                        // Remove from queue and MMR index
                        let mut pipe = redis::pipe();
                        pipe.atomic();
                        pipe.zrem(&queue_key, &member_json);
                        pipe.zrem(&mmr_key, user_id.to_string());
                        pipe.del(&user_key);
                        
                        let _: () = pipe.query_async(&mut self.conn).await?;
                        
                        info!("Removed user {} from matchmaking queue", user_id);
                        return Ok(Some(status.game_type));
                    }
                }
            }
        }
        
        Ok(None)
    }
    
    /// Check if a user is in queue and get their status
    pub async fn get_queue_status(&mut self, user_id: u32) -> Result<Option<UserQueueStatus>> {
        let user_key = self.redis_keys.matchmaking_user_status(user_id);
        
        let status_json: Option<String> = self.conn.hget(&user_key, "status").await?;
        
        match status_json {
            Some(json) => Ok(Some(serde_json::from_str(&json)?)),
            None => Ok(None),
        }
    }
    
    /// Get queue position for a user
    pub async fn get_queue_position(&mut self, user_id: u32, game_type: &GameType) -> Result<Option<usize>> {
        let queue_key = self.redis_keys.matchmaking_queue(game_type);
        
        // Get all queued players
        let members: Vec<String> = self.conn.zrange(&queue_key, 0, -1).await?;
        
        for (position, member_json) in members.iter().enumerate() {
            if let Ok(player) = serde_json::from_str::<QueuedPlayer>(member_json) {
                if player.user_id == user_id {
                    return Ok(Some(position + 1)); // 1-indexed position
                }
            }
        }
        
        Ok(None)
    }
    
    /// Generate a new game ID atomically
    pub async fn generate_game_id(&mut self) -> Result<u32> {
        let counter_key = self.redis_keys.game_id_counter();
        let id: u32 = self.conn.incr(&counter_key, 1).await?;
        Ok(id)
    }
    
    /// Health check for Redis connection
    pub async fn health_check(&mut self) -> Result<()> {
        // Use a simple SET/GET command as health check
        let test_key = "redis:health:check";
        let test_value = "OK";
        
        let _: () = self.conn.set_ex(test_key, test_value, 10).await?;
        let result: Option<String> = self.conn.get(test_key).await?;
        
        if result.as_deref() == Some(test_value) {
            Ok(())
        } else {
            Err(anyhow!("Health check failed: unexpected response"))
        }
    }
    
    /// Get connection for pubsub operations
    pub fn connection(&self) -> ConnectionManager {
        self.conn.clone()
    }
    
    /// Clean up expired queue entries (maintenance task)
    pub async fn cleanup_expired_entries(&mut self, max_age_seconds: i64) -> Result<usize> {
        let _cutoff_time = Utc::now().timestamp_millis() - (max_age_seconds * 1000);
        let removed_count = 0;
        
        // Get all game type queues (this would need to track active game types)
        // For now, this is a placeholder - in production, we'd maintain a set of active game types
        
        debug!("Cleaned up {} expired queue entries", removed_count);
        Ok(removed_count)
    }
    
    /// Get all players in queue for a game type
    pub async fn get_queued_players(&mut self, game_type: &GameType) -> Result<Vec<QueuedPlayer>> {
        let queue_key = self.redis_keys.matchmaking_queue(game_type);
        
        let members: Vec<String> = self.conn.zrange(&queue_key, 0, -1).await?;
        let mut players = Vec::new();
        
        for member_json in members {
            if let Ok(player) = serde_json::from_str::<QueuedPlayer>(&member_json) {
                players.push(player);
            }
        }
        
        Ok(players)
    }
    
    /// Get players in MMR range
    pub async fn get_players_in_mmr_range(&mut self, game_type: &GameType, min_mmr: i32, max_mmr: i32) -> Result<Vec<u32>> {
        let mmr_key = self.redis_keys.matchmaking_mmr_index(game_type);
        
        let user_ids: Vec<String> = self.conn.zrangebyscore(&mmr_key, min_mmr, max_mmr).await?;
        
        Ok(user_ids.into_iter()
            .filter_map(|s| s.parse::<u32>().ok())
            .collect())
    }
    
    /// Store active match information
    pub async fn store_active_match(&mut self, game_id: u32, match_info: ActiveMatch) -> Result<()> {
        let matches_key = self.redis_keys.matchmaking_active_matches();
        let match_json = serde_json::to_string(&match_info)?;
        
        let _: () = self.conn.hset(&matches_key, game_id.to_string(), match_json).await?;
        
        Ok(())
    }
    
    /// Get active match information
    pub async fn get_active_match(&mut self, game_id: u32) -> Result<Option<ActiveMatch>> {
        let matches_key = self.redis_keys.matchmaking_active_matches();
        
        let match_json: Option<String> = self.conn.hget(&matches_key, game_id.to_string()).await?;
        
        match match_json {
            Some(json) => Ok(Some(serde_json::from_str(&json)?)),
            None => Ok(None),
        }
    }
    
    /// Remove players from queue (for match creation)
    pub async fn remove_players_from_queue(&mut self, game_type: &GameType, user_ids: &[u32]) -> Result<()> {
        let queue_key = self.redis_keys.matchmaking_queue(game_type);
        let mmr_key = self.redis_keys.matchmaking_mmr_index(game_type);
        
        // Get all members to find which ones to remove
        let members: Vec<(String, f64)> = self.conn.zrange_withscores(&queue_key, 0, -1).await?;
        
        let mut pipe = redis::pipe();
        pipe.atomic();
        
        for (member_json, _score) in members {
            if let Ok(player) = serde_json::from_str::<QueuedPlayer>(&member_json) {
                if user_ids.contains(&player.user_id) {
                    pipe.zrem(&queue_key, &member_json);
                    pipe.zrem(&mmr_key, player.user_id.to_string());
                    pipe.del(self.redis_keys.matchmaking_user_status(player.user_id));
                }
            }
        }
        
        let _: () = pipe.query_async(&mut self.conn).await?;
        
        Ok(())
    }
}

/// Connection pool for Redis matchmaking
pub struct MatchmakingPool {
    managers: Vec<MatchmakingManager>,
    current: std::sync::atomic::AtomicUsize,
}

impl MatchmakingPool {
    /// Create a new connection pool
    pub async fn new(redis_url: &str, pool_size: usize) -> Result<Self> {
        let mut managers = Vec::with_capacity(pool_size);
        
        for _ in 0..pool_size {
            managers.push(MatchmakingManager::new(redis_url).await?);
        }
        
        Ok(Self {
            managers,
            current: std::sync::atomic::AtomicUsize::new(0),
        })
    }
    
    /// Get a connection from the pool (round-robin)
    pub fn get(&self) -> &MatchmakingManager {
        let idx = self.current.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % self.managers.len();
        &self.managers[idx]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_redis_connection() {
        // This test requires Redis to be running
        let redis_url = "redis://localhost:6379";
        
        match MatchmakingManager::new_from_env(redis_url).await {
            Ok(mut manager) => {
                assert!(manager.health_check().await.is_ok());
            }
            Err(e) => {
                eprintln!("Redis not available for testing: {}", e);
            }
        }
    }
    
    #[tokio::test]
    async fn test_queue_operations() {
        let redis_url = "redis://localhost:6379";
        
        let mut manager = match MatchmakingManager::new(redis_url, "test").await {
            Ok(m) => m,
            Err(_) => {
                eprintln!("Redis not available for testing");
                return;
            }
        };
        
        // Test add to queue
        let user_id = 12345;
        let username = "test_user".to_string();
        let mmr = 1500;
        let game_type = GameType::FreeForAll { max_players: 4 };
        
        // Clean up first
        let _ = manager.remove_from_queue(user_id).await;
        
        // Add to queue
        assert!(manager.add_to_queue(user_id, username.clone(), mmr, game_type.clone()).await.is_ok());
        
        // Check status
        let status = manager.get_queue_status(user_id).await.unwrap();
        assert!(status.is_some());
        let status = status.unwrap();
        assert_eq!(status.username, username);
        assert_eq!(status.mmr, mmr);
        
        // Check position
        let position = manager.get_queue_position(user_id, &game_type).await.unwrap();
        assert_eq!(position, Some(1));
        
        // Remove from queue
        let removed_type = manager.remove_from_queue(user_id).await.unwrap();
        assert_eq!(removed_type, Some(game_type));
        
        // Check status again
        let status = manager.get_queue_status(user_id).await.unwrap();
        assert!(status.is_none());
    }
}