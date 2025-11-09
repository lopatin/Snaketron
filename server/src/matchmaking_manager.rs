use crate::redis_keys::RedisKeys;
use anyhow::{Result, anyhow};
use chrono::Utc;
use common::GameType;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

// Data structures for Redis storage
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueuedPlayer {
    pub user_id: u32,
    pub mmr: i32,
    pub username: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueuedLobby {
    pub lobby_code: String,
    pub members: Vec<crate::lobby_manager::LobbyMember>,
    pub avg_mmr: i32,
    pub game_types: Vec<GameType>, // Lobbies can queue for multiple game types
    pub queue_mode: common::QueueMode,
    pub queued_at: i64,
    pub requesting_user_id: u32, // Who initiated the queue request (for spectator preference)
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
    redis: ConnectionManager,
    max_retries: u32,
    retry_delay: Duration,
}

impl MatchmakingManager {
    /// Create a new Redis matchmaking manager
    pub fn new(redis: ConnectionManager) -> Result<Self> {
        Ok(Self {
            redis,
            max_retries: 3,
            retry_delay: Duration::from_millis(500),
        })
    }

    /// Add a lobby to the matchmaking queue for multiple game types
    pub async fn add_lobby_to_queue(
        &mut self,
        lobby_code: &str,
        members: Vec<crate::lobby_manager::LobbyMember>,
        avg_mmr: i32,
        game_types: Vec<GameType>, // Can queue for multiple game types
        queue_mode: common::QueueMode,
        requesting_user_id: u32, // Who initiated the queue request
    ) -> Result<()> {
        if game_types.is_empty() {
            return Err(anyhow!("Must specify at least one game type"));
        }

        let timestamp = Utc::now().timestamp_millis();

        let lobby = QueuedLobby {
            lobby_code: lobby_code.to_string(),
            members,
            avg_mmr,
            game_types: game_types.clone(),
            queue_mode: queue_mode.clone(),
            queued_at: timestamp,
            requesting_user_id,
        };

        let lobby_json = serde_json::to_string(&lobby)?;

        // Start a transaction to add lobby to all game type queues
        let mut pipe = redis::pipe();
        pipe.atomic();

        // Add to each game type's queue
        for game_type in &game_types {
            let lobby_queue_key = RedisKeys::matchmaking_lobby_queue(game_type, &queue_mode);
            let lobby_mmr_key = RedisKeys::matchmaking_lobby_mmr_index(game_type, &queue_mode);

            // Add to lobby queue sorted set (score = timestamp for FIFO)
            pipe.zadd(&lobby_queue_key, &lobby_json, timestamp);

            // Add to MMR index (score = average MMR for range queries)
            // Store full lobby JSON to enable efficient retrieval by MMR
            pipe.zadd(&lobby_mmr_key, &lobby_json, avg_mmr);
        }

        // Execute transaction with retries
        let mut attempts = 0;
        let mut delay = self.retry_delay;

        loop {
            attempts += 1;
            match pipe.clone().query_async(&mut self.redis).await {
                Ok(()) => break,
                Err(e) if attempts < self.max_retries => {
                    warn!(
                        "Failed to add lobby to queue (attempt {}/{}): {}",
                        attempts, self.max_retries, e
                    );
                    sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(10));
                }
                Err(e) => {
                    error!(
                        "Failed to add lobby to queue after {} attempts",
                        self.max_retries
                    );
                    return Err(anyhow!("Failed to add lobby to queue: {}", e));
                }
            }
        }

        info!(
            "Added lobby {} to matchmaking queue for {:?} with {} members and avg MMR {}",
            lobby_code,
            game_types,
            lobby.members.len(),
            avg_mmr
        );
        Ok(())
    }

    /// Renew a player's position in queue (update timestamp to prevent expiration)
    pub async fn renew_queue_position(&mut self, user_id: u32) -> Result<bool> {
        let user_key = RedisKeys::matchmaking_user_status(user_id);

        // Get user status to find their game type
        let status_json: Option<String> = self.redis.hget(&user_key, "status").await?;

        if let Some(json) = status_json {
            let status: UserQueueStatus = serde_json::from_str(&json)?;
            let queue_key = RedisKeys::matchmaking_queue(&status.game_type, &status.queue_mode);

            // Find the player's entry in the queue
            let members: Vec<(String, f64)> =
                self.redis.zrange_withscores(&queue_key, 0, -1).await?;

            for (member_json, _old_score) in members {
                if let Ok(player) = serde_json::from_str::<QueuedPlayer>(&member_json) {
                    if player.user_id == user_id {
                        // Update timestamp to current time
                        let new_timestamp = Utc::now().timestamp_millis();
                        let _: () = self
                            .redis
                            .zadd(&queue_key, &member_json, new_timestamp)
                            .await?;

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
        let user_key = RedisKeys::matchmaking_user_status(user_id);

        // Get user status to find their game type
        let status_json: Option<String> = self.redis.hget(&user_key, "status").await?;

        if let Some(json) = status_json {
            let status: UserQueueStatus = serde_json::from_str(&json)?;
            let queue_key = RedisKeys::matchmaking_queue(&status.game_type, &status.queue_mode);
            let mmr_key = RedisKeys::matchmaking_mmr_index(&status.game_type, &status.queue_mode);

            // Find and remove the player from queue
            let members: Vec<(String, f64)> =
                self.redis.zrange_withscores(&queue_key, 0, -1).await?;

            for (member_json, _score) in members {
                if let Ok(player) = serde_json::from_str::<QueuedPlayer>(&member_json) {
                    if player.user_id == user_id {
                        // Remove from queue and MMR index
                        let mut pipe = redis::pipe();
                        pipe.atomic();
                        pipe.zrem(&queue_key, &member_json);
                        pipe.zrem(&mmr_key, user_id.to_string());
                        pipe.del(&user_key);

                        let _: () = pipe.query_async(&mut self.redis).await?;

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
        let user_key = RedisKeys::matchmaking_user_status(user_id);

        let status_json: Option<String> = self.redis.hget(&user_key, "status").await?;

        match status_json {
            Some(json) => Ok(Some(serde_json::from_str(&json)?)),
            None => Ok(None),
        }
    }

    /// Get queue position for a user
    pub async fn get_queue_position(
        &mut self,
        user_id: u32,
        game_type: &GameType,
        queue_mode: &common::QueueMode,
    ) -> Result<Option<usize>> {
        let queue_key = RedisKeys::matchmaking_queue(game_type, queue_mode);

        // Get all queued players
        let members: Vec<String> = self.redis.zrange(&queue_key, 0, -1).await?;

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
        let counter_key = RedisKeys::game_id_counter();
        let id: u32 = self.redis.incr(&counter_key, 1).await?;
        Ok(id)
    }

    /// Health check for Redis connection
    pub async fn health_check(&mut self) -> Result<()> {
        // Use a simple SET/GET command as health check
        let test_key = "redis:health:check";
        let test_value = "OK";

        let _: () = self.redis.set_ex(test_key, test_value, 10).await?;
        let result: Option<String> = self.redis.get(test_key).await?;

        if result.as_deref() == Some(test_value) {
            Ok(())
        } else {
            Err(anyhow!("Health check failed: unexpected response"))
        }
    }

    /// Clean up expired queue entries (maintenance task)
    /// More efficient version that processes in batches and stops at first non-expired entry
    pub async fn cleanup_expired_entries(
        &mut self,
        game_type: &GameType,
        queue_mode: &common::QueueMode,
        max_age_seconds: i64,
    ) -> Result<usize> {
        let cutoff_time = Utc::now().timestamp_millis() - (max_age_seconds * 1000);
        let queue_key = RedisKeys::matchmaking_queue(game_type, queue_mode);
        let mmr_key = RedisKeys::matchmaking_mmr_index(game_type, queue_mode);

        let mut removed_count = 0;
        let mut offset = 0;
        const BATCH_SIZE: isize = 100; // Process 100 entries at a time

        loop {
            // Get a batch of queued players with their timestamps
            // Since the sorted set is ordered by timestamp, older entries come first
            let members: Vec<(String, f64)> = self
                .redis
                .zrange_withscores(&queue_key, offset, offset + BATCH_SIZE - 1)
                .await?;

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
                        pipe.del(RedisKeys::matchmaking_user_status(player.user_id));
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
                let _: () = pipe.query_async(&mut self.redis).await?;
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

    /// Get strategic subset of lobbies in queue for a game type
    /// Fetches up to 2,000 unique lobbies distributed across:
    /// - 500 longest waiting (by timestamp)
    /// - 500 highest MMR
    /// - 500 lowest MMR
    /// - 500 mid-range MMR
    pub async fn get_queued_lobbies(
        &mut self,
        game_type: &GameType,
        queue_mode: &common::QueueMode,
    ) -> Result<Vec<QueuedLobby>> {
        use std::collections::HashSet;

        let lobby_queue_key = RedisKeys::matchmaking_lobby_queue(game_type, queue_mode);
        let lobby_mmr_key = RedisKeys::matchmaking_lobby_mmr_index(game_type, queue_mode);

        const SUBSET_SIZE: isize = 499; // 0-indexed, so 499 = 500 items

        // 1. Fetch 500 longest waiting (oldest timestamps first)
        let longest_waiting: Vec<String> =
            self.redis.zrange(&lobby_queue_key, 0, SUBSET_SIZE).await?;

        // 2. Fetch 500 highest MMR (reverse order from MMR index)
        let highest_mmr: Vec<String> = self.redis.zrevrange(&lobby_mmr_key, 0, SUBSET_SIZE).await?;

        // 3. Fetch 500 lowest MMR (from MMR index)
        let lowest_mmr: Vec<String> = self.redis.zrange(&lobby_mmr_key, 0, SUBSET_SIZE).await?;

        // 4. Fetch 500 mid-range MMR
        let mid_range: Vec<String> = {
            // Get total count
            let total: isize = self.redis.zcard(&lobby_mmr_key).await?;

            if total <= SUBSET_SIZE + 1 {
                // Not enough lobbies for a distinct mid-range, return empty
                Vec::new()
            } else {
                // Calculate middle range
                let mid_start = (total / 2) - (SUBSET_SIZE / 2);
                let mid_end = mid_start + SUBSET_SIZE;

                self.redis.zrange(&lobby_mmr_key, mid_start, mid_end).await?
            }
        };

        // Deduplicate and collect unique lobbies
        let mut seen_lobby_codes = HashSet::new();
        let mut unique_lobbies = Vec::new();

        // Helper to process lobby JSON and add if unique
        let mut process_lobby = |member_json: &str| {
            if let Ok(lobby) = serde_json::from_str::<QueuedLobby>(member_json) {
                if seen_lobby_codes.insert(lobby.lobby_code.clone()) {
                    unique_lobbies.push(lobby);
                }
            }
        };

        // Process all subsets
        for member_json in longest_waiting.iter() {
            process_lobby(member_json);
        }
        for member_json in highest_mmr.iter() {
            process_lobby(member_json);
        }
        for member_json in lowest_mmr.iter() {
            process_lobby(member_json);
        }
        for member_json in mid_range.iter() {
            process_lobby(member_json);
        }

        // debug!(
        //     "Fetched {} unique lobbies from strategic sampling (game_type: {:?}, queue_mode: {:?})",
        //     unique_lobbies.len(),
        //     game_type,
        //     queue_mode
        // );

        Ok(unique_lobbies)
    }

    /// Remove a lobby from the matchmaking queue for a single game type
    pub async fn remove_lobby_from_queue(
        &mut self,
        game_type: &GameType,
        queue_mode: &common::QueueMode,
        lobby_code: &str,
    ) -> Result<()> {
        let lobby_queue_key = RedisKeys::matchmaking_lobby_queue(game_type, queue_mode);
        let lobby_mmr_key = RedisKeys::matchmaking_lobby_mmr_index(game_type, queue_mode);

        // Get all lobby members to find the one to remove
        let members: Vec<(String, f64)> =
            self.redis.zrange_withscores(&lobby_queue_key, 0, -1).await?;

        let mut pipe = redis::pipe();
        pipe.atomic();

        for (member_json, _score) in members {
            if let Ok(lobby) = serde_json::from_str::<QueuedLobby>(&member_json) {
                if lobby.lobby_code == lobby_code {
                    // Remove from both sorted sets using the same lobby JSON
                    pipe.zrem(&lobby_queue_key, &member_json);
                    pipe.zrem(&lobby_mmr_key, &member_json);
                    break;
                }
            }
        }

        let _: () = pipe.query_async(&mut self.redis).await?;

        info!(
            "Removed lobby {} from matchmaking queue for game type {:?}",
            lobby_code, game_type
        );
        Ok(())
    }

    /// Locate a queued lobby by code across all matchmaking queues
    pub async fn get_queued_lobby_by_code(&mut self, lobby_code: &str) -> Result<Option<QueuedLobby>> {
        let mut cursor: u64 = 0;

        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg("matchmaking:lobby:queue:*")
                .arg("COUNT")
                .arg(50)
                .query_async(&mut self.redis)
                .await?;

            for key in keys {
                let member_entries: Vec<String> = self.redis.zrange(&key, 0, -1).await?;

                for member_json in member_entries {
                    if let Ok(lobby) = serde_json::from_str::<QueuedLobby>(&member_json) {
                        if lobby.lobby_code == lobby_code {
                            return Ok(Some(lobby));
                        }
                    }
                }
            }

            if next_cursor == 0 {
                break;
            }

            cursor = next_cursor;
        }

        Ok(None)
    }

    /// Remove a lobby from every queue it is present in, returning whether a lobby was removed
    pub async fn remove_lobby_from_all_queues_by_code(&mut self, lobby_code: &str) -> Result<bool> {
        if let Some(lobby) = self.get_queued_lobby_by_code(lobby_code).await? {
            self.remove_lobby_from_all_queues(&lobby).await?;
            return Ok(true);
        }

        Ok(false)
    }

    /// Remove a lobby from all matchmaking queues it was queued for
    /// This is used when a lobby is matched to prevent it from being matched again
    pub async fn remove_lobby_from_all_queues(&mut self, lobby: &QueuedLobby) -> Result<()> {
        // Build a single atomic transaction to remove from all queues
        let mut pipe = redis::pipe();
        pipe.atomic();

        // For each game type the lobby was queued for, remove it from that queue
        for game_type in &lobby.game_types {
            let lobby_queue_key = RedisKeys::matchmaking_lobby_queue(game_type, &lobby.queue_mode);
            let lobby_mmr_key = RedisKeys::matchmaking_lobby_mmr_index(game_type, &lobby.queue_mode);

            // We need to find the exact JSON string to remove
            // Since the lobby JSON is stored in Redis, we'll fetch and match
            let members: Vec<String> = self.redis.zrange(&lobby_queue_key, 0, -1).await?;

            for member_json in members {
                if let Ok(queued_lobby) = serde_json::from_str::<QueuedLobby>(&member_json) {
                    if queued_lobby.lobby_code == lobby.lobby_code {
                        // Remove from both sorted sets using the same lobby JSON
                        pipe.zrem(&lobby_queue_key, &member_json);
                        pipe.zrem(&lobby_mmr_key, &member_json);
                        break;
                    }
                }
            }
        }

        // Execute the transaction
        let _: () = pipe.query_async(&mut self.redis).await?;

        info!(
            "Removed lobby {} from all matchmaking queues (was queued for {:?})",
            lobby.lobby_code, lobby.game_types
        );
        Ok(())
    }

    /// Store active match information
    pub async fn store_active_match(
        &mut self,
        game_id: u32,
        match_info: ActiveMatch,
    ) -> Result<()> {
        let matches_key = RedisKeys::matchmaking_active_matches();
        let match_json = serde_json::to_string(&match_info)?;

        let _: () = self
            .redis
            .hset(&matches_key, game_id.to_string(), match_json)
            .await?;

        Ok(())
    }

    /// Get active match information
    pub async fn get_active_match(&mut self, game_id: u32) -> Result<Option<ActiveMatch>> {
        let matches_key = RedisKeys::matchmaking_active_matches();

        let match_json: Option<String> = self.redis.hget(&matches_key, game_id.to_string()).await?;

        match match_json {
            Some(json) => Ok(Some(serde_json::from_str(&json)?)),
            None => Ok(None),
        }
    }

    /// Remove players from queue (for match creation)
    pub async fn remove_players_from_queue(
        &mut self,
        game_type: &GameType,
        queue_mode: &common::QueueMode,
        user_ids: &[u32],
    ) -> Result<()> {
        let queue_key = RedisKeys::matchmaking_queue(game_type, queue_mode);
        let mmr_key = RedisKeys::matchmaking_mmr_index(game_type, queue_mode);

        // Get all members to find which ones to remove
        let members: Vec<(String, f64)> = self.redis.zrange_withscores(&queue_key, 0, -1).await?;

        let mut pipe = redis::pipe();
        pipe.atomic();

        for (member_json, _score) in members {
            if let Ok(player) = serde_json::from_str::<QueuedPlayer>(&member_json) {
                if user_ids.contains(&player.user_id) {
                    pipe.zrem(&queue_key, &member_json);
                    pipe.zrem(&mmr_key, player.user_id.to_string());
                    pipe.del(RedisKeys::matchmaking_user_status(player.user_id));
                }
            }
        }

        let _: () = pipe.query_async(&mut self.redis).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::Client;
    use crate::redis_utils;

    #[tokio::test]
    async fn test_redis_connection() {
        // This test requires Redis to be running
        let redis_url = "redis://localhost:6379";

        let client = match Client::open(redis_url) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to create Redis client: {}", e);
                return;
            }
        };

        let (pubsub_tx, _pubsub_rx) = tokio::sync::broadcast::channel(100);

        let conn = match redis_utils::create_connection_manager(client, pubsub_tx).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Redis not available for testing: {}", e);
                return;
            }
        };

        match MatchmakingManager::new(conn) {
            Ok(mut manager) => {
                assert!(manager.health_check().await.is_ok());
            }
            Err(e) => {
                eprintln!("Failed to create MatchmakingManager: {}", e);
            }
        }
    }
}
