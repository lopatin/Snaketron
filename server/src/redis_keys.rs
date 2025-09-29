/// Utility module for managing Redis keys
/// Isolation between environments is handled by Redis database selection

use common::GameType;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Redis key builder
#[derive(Clone, Debug, Default)]
pub struct RedisKeys;

impl RedisKeys {
    /// Create a new RedisKeys instance
    pub fn new() -> Self {
        Self
    }
    
    // === Matchmaking Keys ===
    
    /// Hash a game type to a consistent identifier
    fn hash_game_type(game_type: &GameType) -> u64 {
        let mut hasher = DefaultHasher::new();
        let json = serde_json::to_string(game_type).unwrap_or_default();
        json.hash(&mut hasher);
        hasher.finish()
    }
    
    /// Queue for a specific game type (by hash)
    pub fn matchmaking_queue_hash(&self, game_type_hash: u64) -> String {
        format!("matchmaking:queue:{}", game_type_hash)
    }
    
    /// Queue for a specific game type and queue mode
    pub fn matchmaking_queue(&self, game_type: &GameType, queue_mode: &common::QueueMode) -> String {
        let hash = Self::hash_game_type(game_type);
        let mode_str = match queue_mode {
            common::QueueMode::Quickmatch => "quick",
            common::QueueMode::Competitive => "comp",
        };
        format!("matchmaking:queue:{}:{}", mode_str, hash)
    }

    /// Queue for a specific game type (default to quickmatch for backward compatibility)
    pub fn matchmaking_queue_default(&self, game_type: &GameType) -> String {
        self.matchmaking_queue(game_type, &common::QueueMode::Quickmatch)
    }
    
    /// MMR index for a game type (by hash)
    pub fn matchmaking_mmr_index_hash(&self, game_type_hash: u64) -> String {
        format!("matchmaking:mmr:{}", game_type_hash)
    }
    
    /// MMR index for a game type and queue mode
    pub fn matchmaking_mmr_index(&self, game_type: &GameType, queue_mode: &common::QueueMode) -> String {
        let hash = Self::hash_game_type(game_type);
        let mode_str = match queue_mode {
            common::QueueMode::Quickmatch => "quick",
            common::QueueMode::Competitive => "comp",
        };
        format!("matchmaking:mmr:{}:{}", mode_str, hash)
    }

    /// MMR index for a game type (default to quickmatch for backward compatibility)
    pub fn matchmaking_mmr_index_default(&self, game_type: &GameType) -> String {
        self.matchmaking_mmr_index(game_type, &common::QueueMode::Quickmatch)
    }
    
    /// User status in matchmaking
    pub fn matchmaking_user_status(&self, user_id: u32) -> String {
        format!("matchmaking:user:{}", user_id)
    }
    
    /// Active matches
    pub fn matchmaking_active_matches(&self) -> String {
        "matchmaking:matches:active".to_string()
    }
    
    /// Game ID counter
    pub fn game_id_counter(&self) -> String {
        "game:id:counter".to_string()
    }
    
    /// Match notification channel for a user
    pub fn matchmaking_notification_channel(&self, user_id: u32) -> String {
        format!("matchmaking:notification:{}", user_id)
    }
    
    // === PubSub Channels ===
    
    /// Partition events channel
    pub fn partition_events(&self, partition_id: u32) -> String {
        format!("snaketron:events:partition:{}", partition_id)
    }
    
    /// Partition commands channel
    pub fn partition_commands(&self, partition_id: u32) -> String {
        format!("snaketron:commands:partition:{}", partition_id)
    }
    
    /// Snapshot requests channel
    pub fn snapshot_requests(&self, partition_id: u32) -> String {
        format!("snaketron:snapshot-requests:partition:{}", partition_id)
    }
    
    /// Game snapshot key
    pub fn game_snapshot(&self, game_id: u32) -> String {
        format!("game:snapshot:{}", game_id)
    }
    
    // === Cluster Singleton Keys ===
    
    /// Lease key for a singleton service
    pub fn singleton_lease(&self, service_name: &str) -> String {
        format!("singleton:lease:{}", service_name)
    }
    
    /// Matchmaking singleton lease
    pub fn matchmaking_singleton_lease(&self) -> String {
        self.singleton_lease("matchmaking")
    }
    
    /// Partition executor singleton lease
    pub fn partition_executor_lease(&self, partition_id: u32) -> String {
        self.singleton_lease(&format!("partition:{}", partition_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_key_generation() {
        let keys = RedisKeys::new();
        
        // Test that keys are generated correctly without prefixes
        assert_eq!(keys.game_id_counter(), "game:id:counter");
        assert_eq!(keys.matchmaking_active_matches(), "matchmaking:matches:active");
        assert_eq!(keys.matchmaking_user_status(123), "matchmaking:user:123");
        assert_eq!(keys.partition_events(0), "snaketron:events:partition:0");
        
        // Test game type hashing
        let game_type = common::GameType::FreeForAll { max_players: 2 };
        let queue_key = keys.matchmaking_queue(&game_type);
        assert!(queue_key.starts_with("matchmaking:queue:"));
    }
}