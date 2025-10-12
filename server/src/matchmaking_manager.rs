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
pub struct QueuedLobby {
    pub lobby_id: i32,
    pub lobby_code: String,
    pub members: Vec<crate::lobby_manager::LobbyMember>,
    pub avg_mmr: i32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserQueueStatus {
    pub game_type: GameType,
    pub queue_mode: common::QueueMode,
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
    pub async fn add_to_queue(&mut self, user_id: u32, username: String, mmr: i32, game_type: GameType, queue_mode: common::QueueMode) -> Result<()> {
        let queue_key = self.redis_keys.matchmaking_queue(&game_type, &queue_mode);
        let mmr_key = self.redis_keys.matchmaking_mmr_index(&game_type, &queue_mode);
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
            queue_mode: queue_mode.clone(),
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
                    delay = (delay * 2).min(Duration::from_secs(10));
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

    /// Add a lobby to the matchmaking queue
    pub async fn add_lobby_to_queue(
        &mut self,
        lobby_id: i32,
        lobby_code: &str,
        members: Vec<crate::lobby_manager::LobbyMember>,
        avg_mmr: i32,
        game_type: GameType,
        queue_mode: common::QueueMode,
    ) -> Result<()> {
        let lobby_queue_key = self.redis_keys.matchmaking_lobby_queue(&game_type, &queue_mode);
        let lobby_mmr_key = self.redis_keys.matchmaking_lobby_mmr_index(&game_type, &queue_mode);

        let lobby = QueuedLobby {
            lobby_id,
            lobby_code: lobby_code.to_string(),
            members,
            avg_mmr,
        };

        let lobby_json = serde_json::to_string(&lobby)?;
        let timestamp = Utc::now().timestamp_millis();

        // Start a transaction
        let mut pipe = redis::pipe();
        pipe.atomic();

        // Add to lobby queue sorted set (score = timestamp for FIFO)
        pipe.zadd(&lobby_queue_key, lobby_json, timestamp);

        // Add to MMR index (score = average MMR for range queries)
        pipe.zadd(&lobby_mmr_key, lobby_id.to_string(), avg_mmr);

        // Execute transaction with retries
        let mut attempts = 0;
        let mut delay = self.retry_delay;

        loop {
            attempts += 1;
            match pipe.clone().query_async(&mut self.conn).await {
                Ok(()) => break,
                Err(e) if attempts < self.max_retries => {
                    warn!("Failed to add lobby to queue (attempt {}/{}): {}", attempts, self.max_retries, e);
                    sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(10));
                }
                Err(e) => {
                    error!("Failed to add lobby to queue after {} attempts", self.max_retries);
                    return Err(anyhow!("Failed to add lobby to queue: {}", e));
                }
            }
        }

        info!("Added lobby {} to matchmaking queue for {:?} with {} members and avg MMR {}",
            lobby_id, game_type, lobby.members.len(), avg_mmr);
        Ok(())
    }

    /// Renew a player's position in queue (update timestamp to prevent expiration)
    pub async fn renew_queue_position(&mut self, user_id: u32) -> Result<bool> {
        let user_key = self.redis_keys.matchmaking_user_status(user_id);

        // Get user status to find their game type
        let status_json: Option<String> = self.conn.hget(&user_key, "status").await?;

        if let Some(json) = status_json {
            let status: UserQueueStatus = serde_json::from_str(&json)?;
            let queue_key = self.redis_keys.matchmaking_queue(&status.game_type, &status.queue_mode);

            // Find the player's entry in the queue
            let members: Vec<(String, f64)> = self.conn.zrange_withscores(&queue_key, 0, -1).await?;

            for (member_json, _old_score) in members {
                if let Ok(player) = serde_json::from_str::<QueuedPlayer>(&member_json) {
                    if player.user_id == user_id {
                        // Update timestamp to current time
                        let new_timestamp = Utc::now().timestamp_millis();
                        let _: () = self.conn.zadd(&queue_key, &member_json, new_timestamp).await?;

                        debug!("Renewed queue position for user {}", user_id);
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    /// Remove a player from the matchmaking queue
    pub async fn remove_from_queue(&mut self, user_id: u32) -> Result<Option<GameType>> {
        let user_key = self.redis_keys.matchmaking_user_status(user_id);
        
        // Get user status to find their game type
        let status_json: Option<String> = self.conn.hget(&user_key, "status").await?;
        
        if let Some(json) = status_json {
            let status: UserQueueStatus = serde_json::from_str(&json)?;
            let queue_key = self.redis_keys.matchmaking_queue(&status.game_type, &status.queue_mode);
            let mmr_key = self.redis_keys.matchmaking_mmr_index(&status.game_type, &status.queue_mode);
            
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
    pub async fn get_queue_position(&mut self, user_id: u32, game_type: &GameType, queue_mode: &common::QueueMode) -> Result<Option<usize>> {
        let queue_key = self.redis_keys.matchmaking_queue(game_type, queue_mode);
        
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
    /// More efficient version that processes in batches and stops at first non-expired entry
    pub async fn cleanup_expired_entries(&mut self, game_type: &GameType, queue_mode: &common::QueueMode, max_age_seconds: i64) -> Result<usize> {
        let cutoff_time = Utc::now().timestamp_millis() - (max_age_seconds * 1000);
        let queue_key = self.redis_keys.matchmaking_queue(game_type, queue_mode);
        let mmr_key = self.redis_keys.matchmaking_mmr_index(game_type, queue_mode);

        let mut removed_count = 0;
        let mut offset = 0;
        const BATCH_SIZE: isize = 100; // Process 100 entries at a time

        loop {
            // Get a batch of queued players with their timestamps
            // Since the sorted set is ordered by timestamp, older entries come first
            let members: Vec<(String, f64)> = self.conn.zrange_withscores(
                &queue_key,
                offset,
                offset + BATCH_SIZE - 1
            ).await?;

            if members.is_empty() {
                break; // No more entries
            }

            let mut pipe = redis::pipe();
            pipe.atomic();
            let mut batch_removed = 0;
            let mut found_non_expired = false;

            for (member_json, timestamp) in members {
                // Check if entry is expired (timestamp is in milliseconds)
                if timestamp < cutoff_time as f64 {
                    if let Ok(player) = serde_json::from_str::<QueuedPlayer>(&member_json) {
                        // Remove from queue and MMR index
                        pipe.zrem(&queue_key, &member_json);
                        pipe.zrem(&mmr_key, player.user_id.to_string());
                        pipe.del(self.redis_keys.matchmaking_user_status(player.user_id));
                        batch_removed += 1;

                        // This is a warning because websockets should really be renewing
                        // or cleaning up their own entries.
                        warn!("Removing expired queue entry for user {}", player.user_id);
                    }
                } else {
                    // Found a non-expired entry, stop processing
                    found_non_expired = true;
                    break;
                }
            }

            if batch_removed > 0 {
                let _: () = pipe.query_async(&mut self.conn).await?;
                removed_count += batch_removed;
            }

            if found_non_expired {
                // All remaining entries are non-expired (since sorted by timestamp)
                break;
            }

            // Move to next batch
            // Note: we don't increment offset since removed items shift the indices
            // We always start from 0 after removals
            if batch_removed == 0 {
                // No items were removed in this batch, move to the next
                offset += BATCH_SIZE;
            }
        }

        Ok(removed_count)
    }
    
    /// Get all players in queue for a game type (limited to 5000)
    pub async fn get_queued_players(&mut self, game_type: &GameType, queue_mode: &common::QueueMode) -> Result<Vec<QueuedPlayer>> {
        let queue_key = self.redis_keys.matchmaking_queue(game_type, queue_mode);

        // Limit to 5000 records to prevent excessive memory usage
        const MAX_QUEUE_FETCH: isize = 4999; // 0-indexed, so 4999 = 5000 items
        let members: Vec<String> = self.conn.zrange(&queue_key, 0, MAX_QUEUE_FETCH).await?;
        let mut players = Vec::new();

        for member_json in members {
            if let Ok(player) = serde_json::from_str::<QueuedPlayer>(&member_json) {
                players.push(player);
            }
        }

        Ok(players)
    }

    /// Get all lobbies in queue for a game type (limited to 100)
    pub async fn get_queued_lobbies(&mut self, game_type: &GameType, queue_mode: &common::QueueMode) -> Result<Vec<QueuedLobby>> {
        let lobby_queue_key = self.redis_keys.matchmaking_lobby_queue(game_type, queue_mode);

        // Limit to 100 lobbies to prevent excessive memory usage
        const MAX_LOBBY_FETCH: isize = 99; // 0-indexed, so 99 = 100 items
        let members: Vec<String> = self.conn.zrange(&lobby_queue_key, 0, MAX_LOBBY_FETCH).await?;
        let mut lobbies = Vec::new();

        for member_json in members {
            if let Ok(lobby) = serde_json::from_str::<QueuedLobby>(&member_json) {
                lobbies.push(lobby);
            }
        }

        Ok(lobbies)
    }

    /// Remove a lobby from the matchmaking queue
    pub async fn remove_lobby_from_queue(&mut self, game_type: &GameType, queue_mode: &common::QueueMode, lobby_id: i32) -> Result<()> {
        let lobby_queue_key = self.redis_keys.matchmaking_lobby_queue(game_type, queue_mode);
        let lobby_mmr_key = self.redis_keys.matchmaking_lobby_mmr_index(game_type, queue_mode);

        // Get all lobby members to find the one to remove
        let members: Vec<(String, f64)> = self.conn.zrange_withscores(&lobby_queue_key, 0, -1).await?;

        let mut pipe = redis::pipe();
        pipe.atomic();

        for (member_json, _score) in members {
            if let Ok(lobby) = serde_json::from_str::<QueuedLobby>(&member_json) {
                if lobby.lobby_id == lobby_id {
                    pipe.zrem(&lobby_queue_key, &member_json);
                    pipe.zrem(&lobby_mmr_key, lobby_id.to_string());
                    break;
                }
            }
        }

        let _: () = pipe.query_async(&mut self.conn).await?;

        info!("Removed lobby {} from matchmaking queue", lobby_id);
        Ok(())
    }

    /// Get the longest waiting users (up to 5000) with their queue timestamps
    pub async fn get_longest_waiting_users(&mut self, game_type: &GameType, queue_mode: &common::QueueMode) -> Result<Vec<(QueuedPlayer, i64)>> {
        let queue_key = self.redis_keys.matchmaking_queue(game_type, queue_mode);

        // Get oldest 5000 entries (score = timestamp)
        const MAX_FETCH: isize = 4999; // 0-indexed, so 4999 = 5000 items
        let members: Vec<(String, f64)> = self.conn.zrange_withscores(&queue_key, 0, MAX_FETCH).await?;

        let mut players = Vec::new();
        for (member_json, timestamp) in members {
            if let Ok(player) = serde_json::from_str::<QueuedPlayer>(&member_json) {
                players.push((player, timestamp as i64));
            }
        }

        Ok(players)
    }

    /// Get users with lowest MMR (up to 5000)
    pub async fn get_lowest_mmr_users(&mut self, game_type: &GameType, queue_mode: &common::QueueMode) -> Result<Vec<u32>> {
        let mmr_key = self.redis_keys.matchmaking_mmr_index(game_type, queue_mode);

        // Get lowest 5000 MMR entries
        const MAX_FETCH: isize = 4999;
        let user_ids: Vec<String> = self.conn.zrange(&mmr_key, 0, MAX_FETCH).await?;

        Ok(user_ids.into_iter()
            .filter_map(|s| s.parse::<u32>().ok())
            .collect())
    }

    /// Get users with highest MMR (up to 5000)
    pub async fn get_highest_mmr_users(&mut self, game_type: &GameType, queue_mode: &common::QueueMode) -> Result<Vec<u32>> {
        let mmr_key = self.redis_keys.matchmaking_mmr_index(game_type, queue_mode);

        // Get highest 5000 MMR entries (reverse range)
        const MAX_FETCH: isize = 4999;
        let user_ids: Vec<String> = self.conn.zrevrange(&mmr_key, 0, MAX_FETCH).await?;

        Ok(user_ids.into_iter()
            .filter_map(|s| s.parse::<u32>().ok())
            .collect())
    }

    /// Batch get user status for multiple users
    pub async fn batch_get_user_status(&mut self, user_ids: &[u32]) -> Result<Vec<(u32, UserQueueStatus)>> {
        if user_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Build pipeline to fetch all user statuses
        let mut pipe = redis::pipe();
        for user_id in user_ids {
            let user_key = self.redis_keys.matchmaking_user_status(*user_id);
            pipe.hget(&user_key, "status");
        }

        // Execute pipeline
        let results: Vec<Option<String>> = pipe.query_async(&mut self.conn).await?;

        // Parse results
        let mut statuses = Vec::new();
        for (user_id, status_json) in user_ids.iter().zip(results.into_iter()) {
            if let Some(json) = status_json {
                if let Ok(status) = serde_json::from_str::<UserQueueStatus>(&json) {
                    statuses.push((*user_id, status));
                }
            }
        }

        Ok(statuses)
    }
    
    /// Get players in MMR range (limited to 5000)
    pub async fn get_players_in_mmr_range(&mut self, game_type: &GameType, queue_mode: &common::QueueMode, min_mmr: i32, max_mmr: i32) -> Result<Vec<u32>> {
        let mmr_key = self.redis_keys.matchmaking_mmr_index(game_type, queue_mode);

        // Use ZRANGEBYSCORE with LIMIT to prevent excessive memory usage
        let user_ids: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(&mmr_key)
            .arg(min_mmr)
            .arg(max_mmr)
            .arg("LIMIT")
            .arg(0)
            .arg(5000)
            .query_async(&mut self.conn)
            .await?;

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
    pub async fn remove_players_from_queue(&mut self, game_type: &GameType, queue_mode: &common::QueueMode, user_ids: &[u32]) -> Result<()> {
        let queue_key = self.redis_keys.matchmaking_queue(game_type, queue_mode);
        let mmr_key = self.redis_keys.matchmaking_mmr_index(game_type, queue_mode);
        
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

        match MatchmakingManager::new(redis_url).await {
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

        let mut manager = match MatchmakingManager::new(redis_url).await {
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
        assert!(manager.add_to_queue(user_id, username.clone(), mmr, game_type.clone(), common::QueueMode::Quickmatch).await.is_ok());
        
        // Check status
        let status = manager.get_queue_status(user_id).await.unwrap();
        assert!(status.is_some());
        let status = status.unwrap();
        assert_eq!(status.username, username);
        assert_eq!(status.mmr, mmr);
        
        // Check position
        let position = manager.get_queue_position(user_id, &game_type, &common::QueueMode::Quickmatch).await.unwrap();
        assert_eq!(position, Some(1));
        
        // Remove from queue
        let removed_type = manager.remove_from_queue(user_id).await.unwrap();
        assert_eq!(removed_type, Some(game_type));
        
        // Check status again
        let status = manager.get_queue_status(user_id).await.unwrap();
        assert!(status.is_none());
    }
}