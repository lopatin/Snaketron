use common::GameType;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct RedisKeys;

impl RedisKeys {
    // === Matchmaking Keys ===

    /// Hash a game type to a consistent identifier
    fn hash_game_type(game_type: &GameType) -> u64 {
        let mut hasher = DefaultHasher::new();
        let json = serde_json::to_string(game_type).unwrap_or_default();
        json.hash(&mut hasher);
        hasher.finish()
    }

    /// Queue for a specific game type (by hash)
    pub fn matchmaking_queue_hash(game_type_hash: u64) -> String {
        format!("matchmaking:queue:{}", game_type_hash)
    }

    /// Queue for a specific game type and queue mode
    pub fn matchmaking_queue(game_type: &GameType, queue_mode: &common::QueueMode) -> String {
        let hash = Self::hash_game_type(game_type);
        let mode_str = match queue_mode {
            common::QueueMode::Quickmatch => "quick",
            common::QueueMode::Competitive => "comp",
        };
        format!("matchmaking:queue:{}:{}", mode_str, hash)
    }

    /// Queue for a specific game type (default to quickmatch for backward compatibility)
    pub fn matchmaking_queue_default(game_type: &GameType) -> String {
        Self::matchmaking_queue(game_type, &common::QueueMode::Quickmatch)
    }

    /// MMR index for a game type (by hash)
    pub fn matchmaking_mmr_index_hash(game_type_hash: u64) -> String {
        format!("matchmaking:mmr:{}", game_type_hash)
    }

    /// MMR index for a game type and queue mode
    pub fn matchmaking_mmr_index(game_type: &GameType, queue_mode: &common::QueueMode) -> String {
        let hash = Self::hash_game_type(game_type);
        let mode_str = match queue_mode {
            common::QueueMode::Quickmatch => "quick",
            common::QueueMode::Competitive => "comp",
        };
        format!("matchmaking:mmr:{}:{}", mode_str, hash)
    }

    /// MMR index for a game type (default to quickmatch for backward compatibility)
    pub fn matchmaking_mmr_index_default(game_type: &GameType) -> String {
        Self::matchmaking_mmr_index(game_type, &common::QueueMode::Quickmatch)
    }

    /// User status in matchmaking
    pub fn matchmaking_user_status(user_id: u32) -> String {
        format!("matchmaking:user:{}", user_id)
    }

    /// Active matches
    pub fn matchmaking_active_matches() -> String {
        "matchmaking:matches:active".to_string()
    }

    /// Game ID counter
    pub fn game_id_counter() -> String {
        "game:id:counter".to_string()
    }

    /// Match notification channel for a user
    pub fn matchmaking_notification_channel(user_id: u32) -> String {
        format!("matchmaking:notification:{}", user_id)
    }

    /// Lobby queue for a specific game type and queue mode
    pub fn matchmaking_lobby_queue(game_type: &GameType, queue_mode: &common::QueueMode) -> String {
        let hash = Self::hash_game_type(game_type);
        let mode_str = match queue_mode {
            common::QueueMode::Quickmatch => "quick",
            common::QueueMode::Competitive => "comp",
        };
        format!("matchmaking:lobby:queue:{}:{}", mode_str, hash)
    }

    /// Lobby MMR index for a game type and queue mode
    pub fn matchmaking_lobby_mmr_index(
        game_type: &GameType,
        queue_mode: &common::QueueMode,
    ) -> String {
        let hash = Self::hash_game_type(game_type);
        let mode_str = match queue_mode {
            common::QueueMode::Quickmatch => "quick",
            common::QueueMode::Competitive => "comp",
        };
        format!("matchmaking:lobby:mmr:{}:{}", mode_str, hash)
    }

    /// Lobby notification channel for all members of a lobby
    pub fn matchmaking_lobby_notification_channel(lobby_code: &str) -> String {
        format!("matchmaking:lobby:notification:{}", lobby_code)
    }

    // === User Cache ===
    pub fn user(user_id: u32) -> String {
        format!("user:{}", user_id)
    }

    // === Lobby Keys ===

    /// Lobby metadata hash (stores lobby details)
    pub fn lobby_metadata(lobby_code: &str) -> String {
        format!("lobby:{}:metadata", lobby_code)
    }

    /// Lobby members sorted set (score = expires_at timestamp)
    pub fn lobby_members_set(lobby_code: &str) -> String {
        format!("lobby:{}:members", lobby_code)
    }

    /// Lobby preferences
    pub fn lobby_preferences(lobby_code: &str) -> String {
        format!("lobby:{}:preferences", lobby_code)
    }

    /// Lobby updates channel
    pub fn lobby_updates_channel() -> String {
        "lobby-updates".to_string()
    }

    // === PubSub Channels ===

    /// Partition events channel
    pub fn partition_events(partition_id: u32) -> String {
        format!("snaketron:events:partition:{}", partition_id)
    }

    /// Partition commands channel
    pub fn partition_commands(partition_id: u32) -> String {
        format!("snaketron:commands:partition:{}", partition_id)
    }

    /// Lobby chat channel
    pub fn lobby_chat_channel(lobby_code: &str) -> String {
        format!("lobby:{}:chat", lobby_code)
    }

    /// Lobby chat history key
    pub fn lobby_chat_history_key(lobby_code: &str) -> String {
        format!("lobby:{}:chat:history", lobby_code)
    }

    /// Game chat channel
    pub fn game_chat_channel(game_id: u32) -> String {
        format!("game:{}:chat", game_id)
    }

    /// Game chat history key
    pub fn game_chat_history_key(game_id: u32) -> String {
        format!("game:{}:chat:history", game_id)
    }

    /// Snapshot requests channel
    pub fn snapshot_requests(partition_id: u32) -> String {
        format!("snaketron:snapshot-requests:partition:{}", partition_id)
    }

    /// Game snapshot key
    pub fn game_snapshot(game_id: u32) -> String {
        format!("game:snapshot:{}", game_id)
    }

    // === Cluster Singleton Keys ===

    /// Lease key for a singleton service
    pub fn singleton_lease(service_name: &str) -> String {
        format!("singleton:lease:{}", service_name)
    }

    /// Matchmaking singleton lease
    pub fn matchmaking_singleton_lease() -> String {
        Self::singleton_lease("matchmaking")
    }

    /// Partition executor singleton lease
    pub fn partition_executor_lease(partition_id: u32) -> String {
        Self::singleton_lease(&format!("partition:{}", partition_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_generation() {
        // Test that keys are generated correctly without prefixes
        assert_eq!(RedisKeys::game_id_counter(), "game:id:counter");
        assert_eq!(
            RedisKeys::matchmaking_active_matches(),
            "matchmaking:matches:active"
        );
        assert_eq!(
            RedisKeys::matchmaking_user_status(123),
            "matchmaking:user:123"
        );
        assert_eq!(
            RedisKeys::partition_events(0),
            "snaketron:events:partition:0"
        );

        // Test game type hashing
        let game_type = common::GameType::FreeForAll { max_players: 2 };
        let queue_key = RedisKeys::matchmaking_queue(&game_type, &common::QueueMode::Quickmatch);
        assert!(queue_key.starts_with("matchmaking:queue:"));
    }
}
