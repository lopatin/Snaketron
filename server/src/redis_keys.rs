use common::GameType;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct RedisKeys;

impl RedisKeys {
    /// Matchmaking must atomically compare queue, lobby, and user identities.
    /// It is intentionally one low-volume control-plane slot; authoritative
    /// game execution is spread over the partition tags below.
    pub const MATCHMAKING_TAG: &'static str = "snaketron:mm";
    pub const MATCHMAKING_USER_ACTIVE_GAME_PREFIX: &'static str =
        "matchmaking:{snaketron:mm}:user:";
    pub const MATCHMAKING_LOBBY_ACTIVE_GAME_PREFIX: &'static str =
        "matchmaking:{snaketron:mm}:lobby:";
    pub const MATCHMAKING_ACTIVE_GAME_SUFFIX: &'static str = ":active-game";
    const EXECUTOR_PARTITION_COUNT: u32 = 10;

    fn executor_tag(partition_id: u32) -> String {
        format!("{{snaketron:exec:{partition_id}}}")
    }

    fn game_partition(game_id: u32) -> u32 {
        game_id % Self::EXECUTOR_PARTITION_COUNT
    }

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
        format!("matchmaking:{{{}}}:user:{}", Self::MATCHMAKING_TAG, user_id)
    }

    /// Active matches
    pub fn matchmaking_active_matches() -> String {
        format!("matchmaking:{{{}}}:matches:active", Self::MATCHMAKING_TAG)
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
        format!(
            "matchmaking:{{{}}}:lobby:{}:queue-identity",
            Self::MATCHMAKING_TAG,
            lobby_code
        )
    }

    /// Exact lobby admission currently reserving a user for matchmaking.
    pub fn matchmaking_user_queue_identity(user_id: u32) -> String {
        format!(
            "matchmaking:{{{}}}:user:{}:queue-identity",
            Self::MATCHMAKING_TAG,
            user_id
        )
    }

    /// Durable cross-slot work awaiting idempotent delivery to a partition.
    pub fn matchmaking_game_created_outbox() -> String {
        format!(
            "matchmaking:{{{}}}:game-created-outbox:v1",
            Self::MATCHMAKING_TAG
        )
    }

    /// Partition-local idempotency marker for one outbox delivery.
    pub fn matchmaking_game_created_delivery(game_id: u32) -> String {
        let partition = Self::game_partition(game_id);
        format!(
            "snaketron:{}:game:{}:created-delivery:v1",
            Self::executor_tag(partition),
            game_id
        )
    }

    /// Short-lived write canary used by task readiness. It is scoped to one
    /// boot so concurrent tasks never contend and disappears automatically.
    pub fn readiness_write_canary(region: &str, task_boot_id: &str) -> String {
        format!("snaketron:readiness:{region}:{task_boot_id}")
    }

    /// Current per-task gateway metrics. The hash and expiry index share one
    /// slot so refresh and crash-expiry cleanup remain atomic in cluster mode.
    pub fn active_server_metrics() -> String {
        "snaketron:{snaketron:server-metrics}:active:v1".to_string()
    }

    pub fn active_server_metrics_expiry() -> String {
        "snaketron:{snaketron:server-metrics}:expiry:v1".to_string()
    }

    /// Lobby queue for a specific game type and queue mode
    pub fn matchmaking_lobby_queue(game_type: &GameType, queue_mode: &common::QueueMode) -> String {
        let hash = Self::hash_game_type(game_type);
        let mode_str = match queue_mode {
            common::QueueMode::Quickmatch => "quick",
            common::QueueMode::Competitive => "comp",
        };
        format!(
            "matchmaking:{{{}}}:lobby:queue:{}:{}",
            Self::MATCHMAKING_TAG,
            mode_str,
            hash
        )
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
        format!(
            "matchmaking:{{{}}}:lobby:mmr:{}:{}",
            Self::MATCHMAKING_TAG,
            mode_str,
            hash
        )
    }

    /// Lobby notification channel for all members of a lobby
    pub fn matchmaking_lobby_notification_channel(lobby_code: &str) -> String {
        // PUBLISH is part of the atomic matchmaking Lua commit. ElastiCache
        // Serverless therefore requires the channel to share the script's
        // matchmaking hash slot just like every data key it touches.
        format!(
            "matchmaking:{{{}}}:lobby:notification:{}",
            Self::MATCHMAKING_TAG,
            lobby_code
        )
    }

    // === User Cache ===
    pub fn user(user_id: u32) -> String {
        format!("user:{}", user_id)
    }

    // === Lobby Keys ===

    /// Lobby metadata hash (stores lobby details)
    pub fn lobby_metadata(lobby_code: &str) -> String {
        format!(
            "lobby:{{{}}}:{}:metadata",
            Self::MATCHMAKING_TAG,
            lobby_code
        )
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
        let partition = Self::game_partition(game_id);
        format!(
            "game:{}:snapshot:{}",
            Self::executor_tag(partition),
            game_id
        )
    }

    // === Game Bus Streams Keys ===

    /// Stream carrying game events for a partition
    pub fn stream_events(partition_id: u32) -> String {
        format!(
            "snaketron:{}:stream:events",
            Self::executor_tag(partition_id)
        )
    }

    /// Stream carrying authoritative commands for a partition.
    pub fn stream_commands(partition_id: u32) -> String {
        format!(
            "snaketron:{}:stream:commands",
            Self::executor_tag(partition_id)
        )
    }

    /// Stream carrying snapshot requests for a partition
    pub fn stream_snapshot_requests(partition_id: u32) -> String {
        format!(
            "snaketron:{}:stream:snapreq",
            Self::executor_tag(partition_id)
        )
    }

    // === Region-scoped executor-v2 protocol keys ===

    pub fn cluster_members(region: &str) -> String {
        format!("snaketron:{{snaketron:members:{region}}}:members:v2")
    }

    pub fn cluster_member(region: &str, boot_id: &str) -> String {
        format!("snaketron:{{snaketron:members:{region}}}:member:v2:{boot_id}")
    }

    pub fn cluster_assignment(region: &str) -> String {
        format!("snaketron:{{snaketron:assignment:{region}}}:assignment:v2")
    }

    pub fn cluster_assignment_lease(region: &str) -> String {
        format!("snaketron:{{snaketron:assignment:{region}}}:assignment:lease:v2")
    }

    pub fn cluster_partition_assignment(region: &str, partition: u32) -> String {
        format!(
            "snaketron:{}:cluster:{region}:assignment-view:v2",
            Self::executor_tag(partition)
        )
    }

    pub fn cluster_partition_lease(region: &str, partition: u32) -> String {
        format!(
            "snaketron:{}:cluster:{region}:lease:v2",
            Self::executor_tag(partition)
        )
    }

    pub fn cluster_active_games(region: &str, partition: u32) -> String {
        format!(
            "snaketron:{}:cluster:{region}:active-games:v2",
            Self::executor_tag(partition)
        )
    }

    pub fn cluster_recovery(region: &str, game_id: u32) -> String {
        let partition = Self::game_partition(game_id);
        format!(
            "snaketron:{}:cluster:{region}:game:{game_id}:recovery:v2",
            Self::executor_tag(partition)
        )
    }

    pub fn cluster_recovery_failure(region: &str, game_id: u32) -> String {
        let partition = Self::game_partition(game_id);
        format!(
            "snaketron:{}:cluster:{region}:game:{game_id}:recovery-failure:v1",
            Self::executor_tag(partition)
        )
    }

    pub fn executor_command_group(region: &str, partition: u32) -> String {
        format!("snaketron-executor-v2:{region}:{partition}")
    }

    pub fn cluster_command_quarantine(region: &str, partition: u32) -> String {
        format!(
            "snaketron:{}:cluster:{region}:command-quarantine:v2",
            Self::executor_tag(partition)
        )
    }

    pub fn cluster_command_decisions(region: &str, partition: u32) -> String {
        format!(
            "snaketron:{}:cluster:{region}:command-decisions:v1",
            Self::executor_tag(partition)
        )
    }

    pub fn cluster_completion(region: &str, game_id: u32) -> String {
        let partition = Self::game_partition(game_id);
        format!(
            "snaketron:{}:cluster:{region}:game:{game_id}:completion:v1",
            Self::executor_tag(partition)
        )
    }

    pub fn cluster_pending_completions(region: &str, partition: u32) -> String {
        format!(
            "snaketron:{}:cluster:{region}:pending-completions:v1",
            Self::executor_tag(partition)
        )
    }

    pub fn cluster_completion_effects_done(region: &str, game_id: u32) -> String {
        let partition = Self::game_partition(game_id);
        format!(
            "snaketron:{}:cluster:{region}:game:{game_id}:completion-effects-done:v1",
            Self::executor_tag(partition)
        )
    }

    pub fn cluster_completion_terminal_notified(region: &str, game_id: u32) -> String {
        let partition = Self::game_partition(game_id);
        format!(
            "snaketron:{}:cluster:{region}:game:{game_id}:completion:terminal-notified:v1",
            Self::executor_tag(partition)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash_tag(key: &str) -> &str {
        let start = key.find('{').expect("cluster key has a hash tag") + 1;
        let end = key[start..].find('}').expect("cluster key closes hash tag") + start;
        assert!(end > start, "cluster hash tag is non-empty");
        &key[start..end]
    }

    fn assert_same_slot(keys: &[String]) {
        let expected = hash_tag(&keys[0]);
        for key in &keys[1..] {
            assert_eq!(hash_tag(key), expected, "{key} is in the wrong slot family");
        }
    }

    #[test]
    fn test_key_generation() {
        assert!(RedisKeys::matchmaking_active_matches().contains("{snaketron:mm}"));
        assert!(RedisKeys::matchmaking_user_status(123).contains("{snaketron:mm}"));
        assert!(RedisKeys::matchmaking_user_active_game(123).ends_with("123:active-game"));
        assert!(RedisKeys::matchmaking_lobby_active_game("ABC123").ends_with("ABC123:active-game"));
        assert_eq!(
            RedisKeys::readiness_write_canary("use1", "task:boot"),
            "snaketron:readiness:use1:task:boot"
        );
        assert_eq!(hash_tag(&RedisKeys::stream_events(0)), "snaketron:exec:0");
        assert_eq!(hash_tag(&RedisKeys::game_snapshot(123)), "snaketron:exec:3");

        // Test game type hashing
        let game_type = common::GameType::FreeForAll { max_players: 2 };
        let queue_key =
            RedisKeys::matchmaking_lobby_queue(&game_type, &common::QueueMode::Quickmatch);
        assert!(queue_key.contains("{snaketron:mm}"));
    }

    #[test]
    fn every_atomic_key_family_is_single_slot_and_partitions_stay_distributed() {
        assert_eq!(
            RedisKeys::EXECUTOR_PARTITION_COUNT,
            crate::game_executor::PARTITION_COUNT
        );
        assert_same_slot(&[
            RedisKeys::cluster_members("use1"),
            RedisKeys::cluster_member("use1", "boot"),
        ]);
        assert_same_slot(&[
            RedisKeys::cluster_assignment("use1"),
            RedisKeys::cluster_assignment_lease("use1"),
        ]);
        assert_same_slot(&[
            RedisKeys::active_server_metrics(),
            RedisKeys::active_server_metrics_expiry(),
        ]);
        assert_same_slot(&[
            RedisKeys::matchmaking_active_matches(),
            RedisKeys::matchmaking_game_created_outbox(),
            RedisKeys::matchmaking_user_status(1),
            RedisKeys::matchmaking_user_active_game(1),
            RedisKeys::matchmaking_user_queue_identity(1),
            RedisKeys::matchmaking_lobby_active_game("ABC"),
            RedisKeys::matchmaking_lobby_queue_identity("ABC"),
            RedisKeys::matchmaking_lobby_notification_channel("ABC"),
            RedisKeys::lobby_metadata("ABC"),
        ]);

        let mut tags = std::collections::BTreeSet::new();
        for partition in 0..RedisKeys::EXECUTOR_PARTITION_COUNT {
            let game_id = partition;
            let keys = [
                RedisKeys::cluster_partition_assignment("use1", partition),
                RedisKeys::cluster_partition_lease("use1", partition),
                RedisKeys::cluster_active_games("use1", partition),
                RedisKeys::cluster_recovery("use1", game_id),
                RedisKeys::cluster_recovery_failure("use1", game_id),
                RedisKeys::cluster_command_quarantine("use1", partition),
                RedisKeys::cluster_command_decisions("use1", partition),
                RedisKeys::cluster_completion("use1", game_id),
                RedisKeys::cluster_pending_completions("use1", partition),
                RedisKeys::cluster_completion_effects_done("use1", game_id),
                RedisKeys::cluster_completion_terminal_notified("use1", game_id),
                RedisKeys::stream_events(partition),
                RedisKeys::stream_commands(partition),
                RedisKeys::stream_snapshot_requests(partition),
                RedisKeys::game_snapshot(game_id),
                RedisKeys::matchmaking_game_created_delivery(game_id),
            ];
            assert_same_slot(&keys);
            tags.insert(hash_tag(&keys[0]).to_string());
        }
        assert_eq!(tags.len(), RedisKeys::EXECUTOR_PARTITION_COUNT as usize);
    }
}
