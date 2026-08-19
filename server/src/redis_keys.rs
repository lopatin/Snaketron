use common::GameType;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::matchmaking_pool::MatchmakingPool;

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
    const EXECUTOR_PARTITION_COUNT: u32 = crate::game_executor::PARTITION_COUNT;

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

    /// Per-member delivery handoff for a committed lobby match.
    ///
    /// Unlike the active-game mappings, terminal cleanup does not remove this
    /// key. The gateway compare-deletes it only after this user successfully
    /// authorizes the matching `JoinGame`, so a missed Pub/Sub notification
    /// remains recoverable even when a short round has already completed.
    pub fn matchmaking_lobby_user_pending_game(lobby_code: &str, user_id: u32) -> String {
        format!(
            "matchmaking:{{{}}}:lobby:{}:user:{}:pending-game",
            Self::MATCHMAKING_TAG,
            lobby_code,
            user_id
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

    /// Exact queued generation liveness lease, refreshed only by an active
    /// member heartbeat and compared against the immutable queue identity.
    pub fn matchmaking_lobby_queue_lease(lobby_code: &str) -> String {
        format!(
            "matchmaking:{{{}}}:lobby:{}:queue-lease",
            Self::MATCHMAKING_TAG,
            lobby_code
        )
    }

    /// Short-lived terminal outcome for one immutable queue operation. This
    /// prevents an ambiguous admission retry from resurrecting a generation
    /// after cancellation, expiry, or match commit removed its identity.
    pub fn matchmaking_lobby_queue_outcome(lobby_code: &str) -> String {
        format!(
            "matchmaking:{{{}}}:lobby:{}:queue-outcome",
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

    /// Creation-time index for exact oldest-age observation of the durable
    /// GameCreated outbox. It shares the outbox hash slot so commit and
    /// acknowledgement can maintain both structures atomically.
    pub fn matchmaking_game_created_outbox_age() -> String {
        format!(
            "matchmaking:{{{}}}:game-created-outbox-age:v1",
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
        Self::matchmaking_lobby_queue_for_pool(game_type, queue_mode, MatchmakingPool::Public)
    }

    /// Lobby queue physically partitioned by the server-attested pool.
    ///
    /// The public key intentionally retains its historical shape so rolling
    /// deployments and legacy queued entries remain in the public boundary.
    pub fn matchmaking_lobby_queue_for_pool(
        game_type: &GameType,
        queue_mode: &common::QueueMode,
        matchmaking_pool: MatchmakingPool,
    ) -> String {
        let hash = Self::hash_game_type(game_type);
        let mode_str = match queue_mode {
            common::QueueMode::Quickmatch => "quick",
            common::QueueMode::Competitive => "comp",
        };
        match matchmaking_pool {
            MatchmakingPool::Public => format!(
                "matchmaking:{{{}}}:lobby:queue:{}:{}",
                Self::MATCHMAKING_TAG,
                mode_str,
                hash
            ),
            MatchmakingPool::Stress => format!(
                "matchmaking:{{{}}}:lobby:queue:stress:{}:{}",
                Self::MATCHMAKING_TAG,
                mode_str,
                hash
            ),
        }
    }

    /// Lobby MMR index for a game type and queue mode
    pub fn matchmaking_lobby_mmr_index(
        game_type: &GameType,
        queue_mode: &common::QueueMode,
    ) -> String {
        Self::matchmaking_lobby_mmr_index_for_pool(game_type, queue_mode, MatchmakingPool::Public)
    }

    pub fn matchmaking_lobby_mmr_index_for_pool(
        game_type: &GameType,
        queue_mode: &common::QueueMode,
        matchmaking_pool: MatchmakingPool,
    ) -> String {
        let hash = Self::hash_game_type(game_type);
        let mode_str = match queue_mode {
            common::QueueMode::Quickmatch => "quick",
            common::QueueMode::Competitive => "comp",
        };
        match matchmaking_pool {
            MatchmakingPool::Public => format!(
                "matchmaking:{{{}}}:lobby:mmr:{}:{}",
                Self::MATCHMAKING_TAG,
                mode_str,
                hash
            ),
            MatchmakingPool::Stress => format!(
                "matchmaking:{{{}}}:lobby:mmr:stress:{}:{}",
                Self::MATCHMAKING_TAG,
                mode_str,
                hash
            ),
        }
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

    // === Social presence and challenges ===

    /// Region roster hash: `user_id` -> serialized `OnlinePlayer`.
    ///
    /// Shares a per-region hash tag with the expiry index below so one Lua
    /// script can prune stale leases and read the surviving roster atomically,
    /// exactly as the active-server registry does. SCAN is not an option here
    /// for the same cluster-safety reason documented on that registry.
    pub fn presence_roster(region: &str) -> String {
        format!("presence:{{snaketron:presence:{region}}}:roster")
    }

    /// Region presence expiry index (score = absolute lease deadline in ms).
    pub fn presence_expiry(region: &str) -> String {
        format!("presence:{{snaketron:presence:{region}}}:expiry")
    }

    /// One user's challenge expiry index (score = absolute expiry in ms).
    ///
    /// Challenges are stored per participant rather than once per pair: each
    /// side reads only its own two keys, which keeps every read and prune
    /// inside a single hash slot even though a challenge spans two users.
    pub fn user_challenge_index(user_id: u32) -> String {
        format!("challenge:{{snaketron:ch:{user_id}}}:index")
    }

    /// One user's challenge records: `challenge_id` -> serialized challenge.
    pub fn user_challenge_data(user_id: u32) -> String {
        format!("challenge:{{snaketron:ch:{user_id}}}:data")
    }

    /// Rolling count of challenges one user has issued, for rate limiting.
    /// Shares the user's challenge slot so it can be read alongside them.
    pub fn user_challenge_rate(user_id: u32) -> String {
        format!("challenge:{{snaketron:ch:{user_id}}}:rate")
    }

    /// Per-user loss-tolerant hint channel. Pub/Sub is at-most-once, so this
    /// only ever says "your challenge state moved, re-read it" — the durable
    /// keys above stay authoritative and a periodic reconcile covers a drop.
    pub fn user_notifications_channel(user_id: u32) -> String {
        format!("user:{user_id}:notifications")
    }

    /// Region-wide roster hint channel, published when the roster digest moves.
    pub fn presence_updates_channel(region: &str) -> String {
        format!("presence:{region}:updates")
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

    /// Short-lived membership write reservations. This key deliberately
    /// shares the matchmaking hash slot with lobby metadata so admission can
    /// inspect it atomically without scanning metadata fields.
    pub fn lobby_membership_reservations(lobby_code: &str) -> String {
        format!(
            "lobby:{{{}}}:{}:membership-reservations",
            Self::MATCHMAKING_TAG,
            lobby_code
        )
    }

    /// Short-lived idempotency result for one membership finalizer token.
    pub fn lobby_membership_finalization_outcome(lobby_code: &str, token: &str) -> String {
        format!(
            "lobby:{{{}}}:{}:membership-outcome:{}",
            Self::MATCHMAKING_TAG,
            lobby_code,
            token
        )
    }

    /// Short ownership lease for expensive ad-break finalization work.
    pub fn lobby_ad_break_finalization_claim(lobby_code: &str) -> String {
        format!(
            "lobby:{{{}}}:{}:ad-break-finalization",
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

    /// Append-only replay cells covered by the adjacent recovery checkpoint.
    /// The partition hash tag is deliberately identical to recovery, lease,
    /// and command keys so one fenced Lua transaction can advance all of them.
    pub fn cluster_replay_journal(region: &str, game_id: u32) -> String {
        let partition = Self::game_partition(game_id);
        format!(
            "snaketron:{}:cluster:{region}:game:{game_id}:replay-journal:v1",
            Self::executor_tag(partition)
        )
    }

    /// Highest event watermark published by an incumbent immediately before a
    /// cooperative handoff. The successor uses it only until its first
    /// checkpoint makes the merged watermark authoritative.
    pub fn cluster_planned_handoff_watermark(region: &str, game_id: u32) -> String {
        let partition = Self::game_partition(game_id);
        format!(
            "snaketron:{}:cluster:{region}:game:{game_id}:planned-handoff-watermark:v1",
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
        assert!(
            RedisKeys::matchmaking_lobby_user_pending_game("ABC123", 123)
                .ends_with("ABC123:user:123:pending-game")
        );
        assert_eq!(
            RedisKeys::readiness_write_canary("use1", "task:boot"),
            "snaketron:readiness:use1:task:boot"
        );
        assert_eq!(hash_tag(&RedisKeys::stream_events(0)), "snaketron:exec:0");
        let game_partition_tag = format!(
            "snaketron:exec:{}",
            123 % crate::game_executor::PARTITION_COUNT,
        );
        assert_eq!(hash_tag(&RedisKeys::game_snapshot(123)), game_partition_tag);
        assert_eq!(
            hash_tag(&RedisKeys::cluster_planned_handoff_watermark("use1", 123)),
            game_partition_tag
        );

        // Test game type hashing
        let game_type = common::GameType::FreeForAll { max_players: 2 };
        let queue_key =
            RedisKeys::matchmaking_lobby_queue(&game_type, &common::QueueMode::Quickmatch);
        assert!(queue_key.contains("{snaketron:mm}"));
        let stress_queue_key = RedisKeys::matchmaking_lobby_queue_for_pool(
            &game_type,
            &common::QueueMode::Quickmatch,
            MatchmakingPool::Stress,
        );
        assert_ne!(queue_key, stress_queue_key);
        assert_eq!(hash_tag(&queue_key), hash_tag(&stress_queue_key));
        assert_eq!(
            queue_key,
            RedisKeys::matchmaking_lobby_queue_for_pool(
                &game_type,
                &common::QueueMode::Quickmatch,
                MatchmakingPool::Public,
            )
        );
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
            RedisKeys::presence_roster("use1"),
            RedisKeys::presence_expiry("use1"),
        ]);
        assert_ne!(
            hash_tag(&RedisKeys::presence_roster("use1")),
            hash_tag(&RedisKeys::presence_roster("euw1")),
            "regions must not share a presence slot"
        );
        assert_same_slot(&[
            RedisKeys::user_challenge_index(7),
            RedisKeys::user_challenge_data(7),
            RedisKeys::user_challenge_rate(7),
        ]);
        assert_ne!(
            hash_tag(&RedisKeys::user_challenge_index(7)),
            hash_tag(&RedisKeys::user_challenge_index(8)),
            "challenge stores must stay distributed across users"
        );
        assert_same_slot(&[
            RedisKeys::matchmaking_active_matches(),
            RedisKeys::matchmaking_game_created_outbox(),
            RedisKeys::matchmaking_game_created_outbox_age(),
            RedisKeys::matchmaking_user_status(1),
            RedisKeys::matchmaking_user_active_game(1),
            RedisKeys::matchmaking_user_queue_identity(1),
            RedisKeys::matchmaking_lobby_active_game("ABC"),
            RedisKeys::matchmaking_lobby_user_pending_game("ABC", 1),
            RedisKeys::matchmaking_lobby_queue_identity("ABC"),
            RedisKeys::matchmaking_lobby_queue_lease("ABC"),
            RedisKeys::matchmaking_lobby_queue_outcome("ABC"),
            RedisKeys::matchmaking_lobby_notification_channel("ABC"),
            RedisKeys::lobby_metadata("ABC"),
            RedisKeys::lobby_membership_reservations("ABC"),
            RedisKeys::lobby_membership_finalization_outcome("ABC", "token"),
            RedisKeys::lobby_ad_break_finalization_claim("ABC"),
        ]);

        let mut tags = std::collections::BTreeSet::new();
        for partition in 0..RedisKeys::EXECUTOR_PARTITION_COUNT {
            let game_id = partition;
            let keys = [
                RedisKeys::cluster_partition_assignment("use1", partition),
                RedisKeys::cluster_partition_lease("use1", partition),
                RedisKeys::cluster_active_games("use1", partition),
                RedisKeys::cluster_recovery("use1", game_id),
                RedisKeys::cluster_replay_journal("use1", game_id),
                RedisKeys::cluster_planned_handoff_watermark("use1", game_id),
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
