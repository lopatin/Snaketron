/// Utility module for managing environment-prefixed Redis keys
/// This ensures complete isolation between dev, test, and prod environments

use common::GameType;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Redis key builder that adds environment prefix to all keys
#[derive(Clone, Debug)]
pub struct RedisKeys {
    environment: String,
}

impl RedisKeys {
    /// Create a new RedisKeys instance for the given environment
    pub fn new(environment: impl Into<String>) -> Self {
        Self {
            environment: environment.into(),
        }
    }
    
    /// Create a RedisKeys instance from environment variable (SNAKETRON_ENV)
    /// Defaults to "dev" if not set
    pub fn from_env() -> Self {
        let env = std::env::var("SNAKETRON_ENV").unwrap_or_else(|_| "dev".to_string());
        Self::new(env)
    }
    
    /// Get the environment name
    pub fn environment(&self) -> &str {
        &self.environment
    }
    
    /// Prefix a key with the environment namespace
    fn prefix(&self, key: &str) -> String {
        format!("{}:{}", self.environment, key)
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
        self.prefix(&format!("matchmaking:queue:{}", game_type_hash))
    }
    
    /// Queue for a specific game type
    pub fn matchmaking_queue(&self, game_type: &GameType) -> String {
        let hash = Self::hash_game_type(game_type);
        self.matchmaking_queue_hash(hash)
    }
    
    /// MMR index for a game type (by hash)
    pub fn matchmaking_mmr_index_hash(&self, game_type_hash: u64) -> String {
        self.prefix(&format!("matchmaking:mmr:{}", game_type_hash))
    }
    
    /// MMR index for a game type
    pub fn matchmaking_mmr_index(&self, game_type: &GameType) -> String {
        let hash = Self::hash_game_type(game_type);
        self.matchmaking_mmr_index_hash(hash)
    }
    
    /// User status in matchmaking
    pub fn matchmaking_user_status(&self, user_id: u32) -> String {
        self.prefix(&format!("matchmaking:user:{}", user_id))
    }
    
    /// Active matches
    pub fn matchmaking_active_matches(&self) -> String {
        self.prefix("matchmaking:matches:active")
    }
    
    /// Game ID counter
    pub fn game_id_counter(&self) -> String {
        self.prefix("game:id:counter")
    }
    
    /// Match notification channel for a user
    pub fn matchmaking_notification_channel(&self, user_id: u32) -> String {
        self.prefix(&format!("matchmaking:notification:{}", user_id))
    }
    
    // === PubSub Channels ===
    
    /// Partition events channel
    pub fn partition_events(&self, partition_id: u32) -> String {
        self.prefix(&format!("snaketron:events:partition:{}", partition_id))
    }
    
    /// Partition commands channel
    pub fn partition_commands(&self, partition_id: u32) -> String {
        self.prefix(&format!("snaketron:commands:partition:{}", partition_id))
    }
    
    /// Snapshot requests channel
    pub fn snapshot_requests(&self, partition_id: u32) -> String {
        self.prefix(&format!("snaketron:snapshot-requests:partition:{}", partition_id))
    }
    
    /// Game snapshot key
    pub fn game_snapshot(&self, game_id: u32) -> String {
        self.prefix(&format!("game:snapshot:{}", game_id))
    }
    
    // === Cluster Singleton Keys ===
    
    /// Lease key for a singleton service
    pub fn singleton_lease(&self, service_name: &str) -> String {
        self.prefix(&format!("singleton:lease:{}", service_name))
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
    fn test_environment_isolation() {
        let dev_keys = RedisKeys::new("dev");
        let test_keys = RedisKeys::new("test");
        let prod_keys = RedisKeys::new("prod");
        
        // Same logical key should have different prefixes
        assert_eq!(dev_keys.game_id_counter(), "dev:game:id:counter");
        assert_eq!(test_keys.game_id_counter(), "test:game:id:counter");
        assert_eq!(prod_keys.game_id_counter(), "prod:game:id:counter");
        
        // Ensure no overlap between environments
        let game_type = common::GameType::FreeForAll { max_players: 2 };
        assert_ne!(dev_keys.matchmaking_queue(&game_type), test_keys.matchmaking_queue(&game_type));
        assert_ne!(test_keys.partition_events(0), prod_keys.partition_events(0));
    }
}