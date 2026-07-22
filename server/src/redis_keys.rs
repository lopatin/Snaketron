use common::GameType;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct RedisKeys;

impl RedisKeys {
    pub const MATCHMAKING_USER_ACTIVE_GAME_PREFIX: &'static str = "matchmaking:user:";
    pub const MATCHMAKING_LOBBY_ACTIVE_GAME_PREFIX: &'static str = "matchmaking:lobby:";
    pub const MATCHMAKING_ACTIVE_GAME_SUFFIX: &'static str = ":active-game";

    // === Matchmaking Keys ===

    /// Hash a game type to a consistent identifier
    fn hash_game_type(game_type: &GameType) -> u64 {
        let mut hasher = DefaultHasher::new();
        let json = serde_json::to_string(game_type).unwrap_or_default();
        json.hash(&mut hasher);
        hasher.finish()
    }

    /// User status in matchmaking
    pub fn matchmaking_user_status(user_id: u32) -> String {
        format!("matchmaking:user:{}", user_id)
    }

    /// Active matches
    pub fn matchmaking_active_matches() -> String {
        "matchmaking:matches:active".to_string()
    }

    /// Durable pointer used to recover a committed match when Pub/Sub delivery is missed.
    pub fn matchmaking_user_active_game(user_id: u32) -> String {
        format!(
            "{}{}{}",
            Self::MATCHMAKING_USER_ACTIVE_GAME_PREFIX,
            user_id,
            Self::MATCHMAKING_ACTIVE_GAME_SUFFIX
        )
    }

    /// Durable pointer used to recover a committed match for every member of a lobby.
    pub fn matchmaking_lobby_active_game(lobby_code: &str) -> String {
        format!(
            "{}{}{}",
            Self::MATCHMAKING_LOBBY_ACTIVE_GAME_PREFIX,
            lobby_code,
            Self::MATCHMAKING_ACTIVE_GAME_SUFFIX
        )
    }

    /// Exact serialized queue entry currently admitted for a lobby.
    pub fn matchmaking_lobby_queue_identity(lobby_code: &str) -> String {
        format!("matchmaking:lobby:{}:queue-identity", lobby_code)
    }

    /// Exact lobby admission currently reserving a user for matchmaking.
    pub fn matchmaking_user_queue_identity(user_id: u32) -> String {
        format!("matchmaking:user:{}:queue-identity", user_id)
    }

    /// Short-lived write canary used by task readiness. It is scoped to one
    /// boot so concurrent tasks never contend and disappears automatically.
    pub fn readiness_write_canary(region: &str, task_boot_id: &str) -> String {
        format!("snaketron:readiness:{region}:{task_boot_id}")
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

    // === PubSub Channels (loss-tolerant fan-out) ===

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

    /// Game snapshot key
    pub fn game_snapshot(game_id: u32) -> String {
        format!("game:snapshot:{}", game_id)
    }

    // === Game Bus Streams Keys ===

    /// Stream carrying game events for a partition
    pub fn stream_events(partition_id: u32) -> String {
        format!("snaketron:stream:events:{}", partition_id)
    }

    /// Stream carrying authoritative commands for a partition.
    pub fn stream_commands(partition_id: u32) -> String {
        format!("snaketron:stream:commands:{}", partition_id)
    }

    /// Stream carrying snapshot requests for a partition
    pub fn stream_snapshot_requests(partition_id: u32) -> String {
        format!("snaketron:stream:snapreq:{}", partition_id)
    }

    // === Region-scoped executor-v2 protocol keys ===

    pub fn cluster_members(region: &str) -> String {
        format!("snaketron:cluster:{region}:members:v2")
    }

    pub fn cluster_member(region: &str, boot_id: &str) -> String {
        format!("snaketron:cluster:{region}:member:v2:{boot_id}")
    }

    pub fn cluster_assignment(region: &str) -> String {
        format!("snaketron:cluster:{region}:assignment:v2")
    }

    pub fn cluster_assignment_lease(region: &str) -> String {
        format!("snaketron:cluster:{region}:assignment:lease:v2")
    }

    pub fn cluster_partition_lease(region: &str, partition: u32) -> String {
        format!("snaketron:cluster:{region}:partition:{partition}:lease:v2")
    }

    pub fn cluster_active_games(region: &str, partition: u32) -> String {
        format!("snaketron:cluster:{region}:partition:{partition}:active-games:v2")
    }

    pub fn cluster_recovery(region: &str, game_id: u32) -> String {
        format!("snaketron:cluster:{region}:game:{game_id}:recovery:v2")
    }

    pub fn cluster_recovery_failure(region: &str, game_id: u32) -> String {
        format!("snaketron:cluster:{region}:game:{game_id}:recovery-failure:v1")
    }

    pub fn executor_command_group(region: &str, partition: u32) -> String {
        format!("snaketron-executor-v2:{region}:{partition}")
    }

    pub fn cluster_command_quarantine(region: &str, partition: u32) -> String {
        format!("snaketron:cluster:{region}:partition:{partition}:command-quarantine:v2")
    }

    pub fn cluster_command_decisions(region: &str, partition: u32) -> String {
        format!("snaketron:cluster:{region}:partition:{partition}:command-decisions:v1")
    }

    pub fn cluster_completion(region: &str, game_id: u32) -> String {
        format!("snaketron:cluster:{region}:game:{game_id}:completion:v1")
    }

    pub fn cluster_pending_completions(region: &str, partition: u32) -> String {
        format!("snaketron:cluster:{region}:partition:{partition}:pending-completions:v1")
    }

    pub fn cluster_completion_effects_done(region: &str, game_id: u32) -> String {
        format!("snaketron:cluster:{region}:game:{game_id}:completion-effects-done:v1")
    }

    pub fn cluster_completion_terminal_notified(region: &str, game_id: u32) -> String {
        format!("snaketron:cluster:{region}:game:{game_id}:completion:terminal-notified:v1")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_generation() {
        // Test that keys are generated correctly without prefixes
        assert_eq!(
            RedisKeys::matchmaking_active_matches(),
            "matchmaking:matches:active"
        );
        assert_eq!(
            RedisKeys::matchmaking_user_status(123),
            "matchmaking:user:123"
        );
        assert_eq!(
            RedisKeys::matchmaking_user_active_game(123),
            "matchmaking:user:123:active-game"
        );
        assert_eq!(
            RedisKeys::matchmaking_lobby_active_game("ABC123"),
            "matchmaking:lobby:ABC123:active-game"
        );
        assert_eq!(
            RedisKeys::readiness_write_canary("use1", "task:boot"),
            "snaketron:readiness:use1:task:boot"
        );
        assert_eq!(RedisKeys::stream_events(0), "snaketron:stream:events:0");
        assert_eq!(RedisKeys::game_snapshot(123), "game:snapshot:123");

        // Test game type hashing
        let game_type = common::GameType::FreeForAll { max_players: 2 };
        let queue_key =
            RedisKeys::matchmaking_lobby_queue(&game_type, &common::QueueMode::Quickmatch);
        assert!(queue_key.starts_with("matchmaking:lobby:queue:"));
    }
}
