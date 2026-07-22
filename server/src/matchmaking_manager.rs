use crate::db::Database;
use crate::redis_keys::RedisKeys;
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use common::GameType;
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

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
    pub queue_token: String,
    pub members: Vec<crate::lobby_manager::LobbyMember>,
    pub avg_mmr: i32,
    pub game_types: Vec<GameType>, // Lobbies can queue for multiple game types
    pub queue_mode: common::QueueMode,
    pub queued_at: i64,
    pub requesting_user_id: u32, // Who initiated the queue request (for spectator preference)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ActiveMatch {
    pub players: Vec<QueuedPlayer>,
    pub spectators: Vec<QueuedPlayer>,
    /// Stable reverse identity used by fenced completion cleanup.
    pub lobby_codes: Vec<String>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchCommitOutcome {
    Committed { stream_id: String },
    AlreadyCommitted,
    Conflict { reason: String },
}

#[derive(Serialize)]
struct MatchCommitQueuePair {
    queue_key: String,
    mmr_key: String,
}

#[derive(Serialize)]
struct MatchCommitLobby {
    lobby_code: String,
    member_json: String,
    active_game_key: String,
    metadata_key: String,
    queue_identity_key: String,
    queue_pairs: Vec<MatchCommitQueuePair>,
}

#[derive(Serialize)]
struct MatchCommitUser {
    active_game_key: String,
    queue_status_key: String,
    queue_identity_key: String,
    queue_identity_value: String,
}

#[derive(Serialize)]
struct MatchCommitPlan {
    active_matches_key: String,
    command_stream_key: String,
    game_id: String,
    active_match_json: String,
    game_created_payload: String,
    lobbies: Vec<MatchCommitLobby>,
    users: Vec<MatchCommitUser>,
    notifications: Vec<MatchCommitNotification>,
}

#[derive(Serialize)]
struct MatchCommitNotification {
    channel: String,
    payload: String,
}

#[derive(Serialize)]
struct LobbyAdmissionPlan {
    lobby_active_game_key: String,
    lobby_metadata_key: String,
    queue_identity_key: String,
    user_active_game_keys: Vec<String>,
    user_queue_claims: Vec<QueueIdentityClaim>,
    member_json: String,
    queued_at: i64,
    avg_mmr: i32,
    queue_pairs: Vec<MatchCommitQueuePair>,
}

#[derive(Serialize)]
struct LobbyRemovalPlan {
    lobby_active_game_key: String,
    lobby_metadata_key: String,
    queue_identity_key: String,
    member_json: String,
    user_queue_claims: Vec<QueueIdentityClaim>,
    queue_pairs: Vec<MatchCommitQueuePair>,
}

#[derive(Serialize)]
struct QueueIdentityClaim {
    key: String,
    value: String,
}

fn lobby_queue_claim(lobby: &QueuedLobby) -> String {
    format!("{}:{}", lobby.lobby_code, lobby.queue_token)
}

/// Atomically reject admission for an already-matched lobby or member before
/// adding every queue identity. This is the same durable mapping checked by
/// the match commit, so reconnect and concurrent matchmakers agree.
const ADMIT_LOBBY_SCRIPT: &str = r#"
local plan = cjson.decode(ARGV[1])

local function key_type(key)
    local response = redis.call('TYPE', key)
    if type(response) == 'table' then return response['ok'] end
    return response
end

local lobby_mapping_type = key_type(plan.lobby_active_game_key)
if lobby_mapping_type ~= 'none' and lobby_mapping_type ~= 'string' then
    return {0, 'lobby-mapping-wrong-type'}
end
if redis.call('GET', plan.lobby_active_game_key) then
    return {0, 'lobby-already-matched'}
end
if key_type(plan.lobby_metadata_key) ~= 'hash' then
    return {0, 'lobby-metadata-missing-or-wrong-type'}
end
for _, key in ipairs(plan.user_active_game_keys) do
    local mapping_type = key_type(key)
    if mapping_type ~= 'none' and mapping_type ~= 'string' then
        return {0, 'user-mapping-wrong-type'}
    end
    if redis.call('GET', key) then return {0, 'user-already-matched'} end
end

local identity_type = key_type(plan.queue_identity_key)
if identity_type ~= 'none' and identity_type ~= 'string' then
    return {0, 'queue-identity-wrong-type'}
end
local existing_identity = redis.call('GET', plan.queue_identity_key)
if existing_identity and existing_identity ~= plan.member_json then
    redis.call('HSET', plan.lobby_metadata_key, 'state', 'queued')
    return {2, 'already-queued'}
end

for _, claim in ipairs(plan.user_queue_claims) do
    local claim_type = key_type(claim.key)
    if claim_type ~= 'none' and claim_type ~= 'string' then
        return {0, 'user-queue-identity-wrong-type'}
    end
    local existing_claim = redis.call('GET', claim.key)
    if existing_claim and existing_claim ~= claim.value then
        return {0, 'user-already-queued'}
    end
end

for _, pair in ipairs(plan.queue_pairs) do
    local queue_type = key_type(pair.queue_key)
    local mmr_type = key_type(pair.mmr_key)
    if queue_type ~= 'none' and queue_type ~= 'zset' then
        return {0, 'queue-wrong-type'}
    end
    if mmr_type ~= 'none' and mmr_type ~= 'zset' then
        return {0, 'mmr-index-wrong-type'}
    end
end

for _, pair in ipairs(plan.queue_pairs) do
    redis.call('ZADD', pair.queue_key, plan.queued_at, plan.member_json)
    redis.call('ZADD', pair.mmr_key, plan.avg_mmr, plan.member_json)
end
redis.call('SET', plan.queue_identity_key, plan.member_json)
for _, claim in ipairs(plan.user_queue_claims) do
    redis.call('SET', claim.key, claim.value)
end
redis.call('HSET', plan.lobby_metadata_key, 'state', 'queued')
return {1, 'queued'}
"#;

/// Remove only the queue generation the caller observed. A stale cancellation
/// must not delete a later admission for the same lobby code.
const REMOVE_LOBBY_SCRIPT: &str = r#"
local plan = cjson.decode(ARGV[1])

local function key_type(key)
    local response = redis.call('TYPE', key)
    if type(response) == 'table' then return response['ok'] end
    return response
end

local identity_type = key_type(plan.queue_identity_key)
if identity_type ~= 'none' and identity_type ~= 'string' then
    return {0, 'queue-identity-wrong-type'}
end
local active_mapping_type = key_type(plan.lobby_active_game_key)
if active_mapping_type ~= 'none' and active_mapping_type ~= 'string' then
    return {0, 'lobby-mapping-wrong-type'}
end
local metadata_type = key_type(plan.lobby_metadata_key)
if metadata_type ~= 'none' and metadata_type ~= 'hash' then
    return {0, 'lobby-metadata-wrong-type'}
end
local existing_identity = redis.call('GET', plan.queue_identity_key)
if not existing_identity then
    if not redis.call('GET', plan.lobby_active_game_key) and metadata_type == 'hash' then
        redis.call('HSET', plan.lobby_metadata_key, 'state', 'waiting')
    end
    return {2, 'not-queued'}
end
if existing_identity ~= plan.member_json then return {2, 'queue-entry-changed'} end

for _, claim in ipairs(plan.user_queue_claims) do
    local claim_type = key_type(claim.key)
    if claim_type ~= 'none' and claim_type ~= 'string' then
        return {0, 'user-queue-identity-wrong-type'}
    end
end

for _, pair in ipairs(plan.queue_pairs) do
    local queue_type = key_type(pair.queue_key)
    local mmr_type = key_type(pair.mmr_key)
    if queue_type ~= 'none' and queue_type ~= 'zset' then
        return {0, 'queue-wrong-type'}
    end
    if mmr_type ~= 'none' and mmr_type ~= 'zset' then
        return {0, 'mmr-index-wrong-type'}
    end
end

for _, pair in ipairs(plan.queue_pairs) do
    redis.call('ZREM', pair.queue_key, plan.member_json)
    redis.call('ZREM', pair.mmr_key, plan.member_json)
end
redis.call('DEL', plan.queue_identity_key)
for _, claim in ipairs(plan.user_queue_claims) do
    if redis.call('GET', claim.key) == claim.value then
        redis.call('DEL', claim.key)
    end
end
if not redis.call('GET', plan.lobby_active_game_key) and metadata_type == 'hash' then
    redis.call('HSET', plan.lobby_metadata_key, 'state', 'waiting')
end
return {1, 'removed'}
"#;

// Redis scripts are isolated but do not roll back commands that precede a
// runtime script error. Validate every key type and every claim predicate
// before issuing the first write; with the service's non-evicting Valkey
// policy, the write phase then consists only of type-safe commands.
const COMMIT_MATCH_SCRIPT: &str = r#"
local plan = cjson.decode(ARGV[1])

local function key_type(key)
    local response = redis.call('TYPE', key)
    if type(response) == 'table' then
        return response['ok']
    end
    return response
end

local active_type = key_type(plan.active_matches_key)
if active_type ~= 'none' and active_type ~= 'hash' then
    return {0, 'active-matches-wrong-type'}
end

local stream_type = key_type(plan.command_stream_key)
if stream_type ~= 'none' and stream_type ~= 'stream' then
    return {0, 'command-stream-wrong-type'}
end

local existing = redis.call('HGET', plan.active_matches_key, plan.game_id)
if existing then
    if existing == plan.active_match_json then
        return {2, 'already-committed'}
    end
    return {0, 'game-id-already-committed'}
end

for _, lobby in ipairs(plan.lobbies) do
    local mapping_type = key_type(lobby.active_game_key)
    if mapping_type ~= 'none' and mapping_type ~= 'string' then
        return {0, 'lobby-mapping-wrong-type:' .. lobby.lobby_code}
    end
    if redis.call('GET', lobby.active_game_key) then
        return {0, 'lobby-already-matched:' .. lobby.lobby_code}
    end

    if key_type(lobby.queue_identity_key) ~= 'string' then
        return {0, 'queue-identity-missing-or-wrong-type:' .. lobby.lobby_code}
    end
    if redis.call('GET', lobby.queue_identity_key) ~= lobby.member_json then
        return {0, 'queue-entry-changed:' .. lobby.lobby_code}
    end

    for _, pair in ipairs(lobby.queue_pairs) do
        if key_type(pair.queue_key) ~= 'zset' or key_type(pair.mmr_key) ~= 'zset' then
            return {0, 'queue-missing-or-wrong-type:' .. lobby.lobby_code}
        end
        if not redis.call('ZSCORE', pair.queue_key, lobby.member_json) then
            return {0, 'queue-entry-changed:' .. lobby.lobby_code}
        end
        if not redis.call('ZSCORE', pair.mmr_key, lobby.member_json) then
            return {0, 'mmr-entry-changed:' .. lobby.lobby_code}
        end
    end
end

for _, user in ipairs(plan.users) do
    local mapping_type = key_type(user.active_game_key)
    if mapping_type ~= 'none' and mapping_type ~= 'string' then
        return {0, 'user-mapping-wrong-type'}
    end
    if redis.call('GET', user.active_game_key) then
        return {0, 'user-already-matched'}
    end
    if key_type(user.queue_identity_key) ~= 'string' then
        return {0, 'user-queue-identity-missing-or-wrong-type'}
    end
    if redis.call('GET', user.queue_identity_key) ~= user.queue_identity_value then
        return {0, 'user-queue-entry-changed'}
    end
end

for _, lobby in ipairs(plan.lobbies) do
    for _, pair in ipairs(lobby.queue_pairs) do
        redis.call('ZREM', pair.queue_key, lobby.member_json)
        redis.call('ZREM', pair.mmr_key, lobby.member_json)
    end
    redis.call('DEL', lobby.queue_identity_key)
end

redis.call('HSET', plan.active_matches_key, plan.game_id, plan.active_match_json)
for _, lobby in ipairs(plan.lobbies) do
    redis.call('SET', lobby.active_game_key, plan.game_id)
    if key_type(lobby.metadata_key) == 'hash' then
        redis.call('HSET', lobby.metadata_key, 'state', 'matched')
    end
end
for _, user in ipairs(plan.users) do
    redis.call('SET', user.active_game_key, plan.game_id)
    redis.call('DEL', user.queue_status_key)
    redis.call('DEL', user.queue_identity_key)
end

local stream_id = redis.call(
    'XADD', plan.command_stream_key, '*', 'data', plan.game_created_payload
)
for _, notification in ipairs(plan.notifications) do
    redis.call('PUBLISH', notification.channel, notification.payload)
end
return {1, stream_id}
"#;

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
            queue_token: uuid::Uuid::new_v4().to_string(),
            members,
            avg_mmr,
            game_types: game_types.clone(),
            queue_mode: queue_mode.clone(),
            queued_at: timestamp,
            requesting_user_id,
        };

        let lobby_json = serde_json::to_string(&lobby)?;
        let queue_claim = lobby_queue_claim(&lobby);

        let plan = LobbyAdmissionPlan {
            lobby_active_game_key: RedisKeys::matchmaking_lobby_active_game(lobby_code),
            lobby_metadata_key: RedisKeys::lobby_metadata(lobby_code),
            queue_identity_key: RedisKeys::matchmaking_lobby_queue_identity(lobby_code),
            user_active_game_keys: lobby
                .members
                .iter()
                .map(|member| RedisKeys::matchmaking_user_active_game(member.user_id))
                .collect(),
            user_queue_claims: lobby
                .members
                .iter()
                .map(|member| QueueIdentityClaim {
                    key: RedisKeys::matchmaking_user_queue_identity(member.user_id),
                    value: queue_claim.clone(),
                })
                .collect(),
            member_json: lobby_json,
            queued_at: timestamp,
            avg_mmr,
            queue_pairs: game_types
                .iter()
                .map(|game_type| MatchCommitQueuePair {
                    queue_key: RedisKeys::matchmaking_lobby_queue(game_type, &queue_mode),
                    mmr_key: RedisKeys::matchmaking_lobby_mmr_index(game_type, &queue_mode),
                })
                .collect(),
        };
        let plan_json = serde_json::to_string(&plan)?;
        let script = redis::Script::new(ADMIT_LOBBY_SCRIPT);

        // Retrying the immutable queue identity is safe after an ambiguous
        // response because ZADD overwrites the same member.
        let mut attempts = 0;
        let mut delay = self.retry_delay;

        let (code, detail) = loop {
            attempts += 1;
            match script
                .arg(&plan_json)
                .invoke_async::<(i64, String)>(&mut self.redis)
                .await
            {
                Ok(result) => break result,
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
        };
        match code {
            1 => {
                info!(
                    "Added lobby {} to matchmaking queue for {:?} with {} members and avg MMR {}",
                    lobby_code,
                    game_types,
                    lobby.members.len(),
                    avg_mmr
                );
            }
            2 => {
                info!(
                    lobby_code,
                    "Lobby already had an admitted queue identity; kept the first request"
                );
            }
            0 => return Err(anyhow!("Lobby admission was rejected: {detail}")),
            other => {
                return Err(anyhow!(
                    "Lobby admission returned unknown status {other}: {detail}"
                ));
            }
        }
        Ok(())
    }

    /// Allocate every authoritative game ID from the durable database.
    pub async fn generate_game_id(&mut self, db: &dyn Database) -> Result<u32> {
        let durable_id = db.allocate_game_id().await?;
        u32::try_from(durable_id).map_err(|_| anyhow!("Durable game ID was outside the u32 range"))
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

                self.redis
                    .zrange(&lobby_mmr_key, mid_start, mid_end)
                    .await?
            }
        };

        // Deduplicate and collect unique lobbies
        let mut seen_lobby_codes = HashSet::new();
        let mut unique_lobbies = Vec::new();

        // Helper to process lobby JSON and add if unique
        let mut process_lobby = |member_json: &str| {
            if let Ok(lobby) = serde_json::from_str::<QueuedLobby>(member_json)
                && seen_lobby_codes.insert(lobby.lobby_code.clone())
            {
                unique_lobbies.push(lobby);
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

    /// Locate the one queue identity admitted for a lobby code.
    pub async fn get_queued_lobby_by_code(
        &mut self,
        lobby_code: &str,
    ) -> Result<Option<QueuedLobby>> {
        let identity_key = RedisKeys::matchmaking_lobby_queue_identity(lobby_code);
        let member_json: Option<String> = self.redis.get(&identity_key).await?;
        member_json
            .map(|member_json| {
                let lobby: QueuedLobby = serde_json::from_str(&member_json).with_context(|| {
                    format!("Malformed lobby queue identity at Redis key {identity_key}")
                })?;
                if lobby.lobby_code != lobby_code {
                    return Err(anyhow!(
                        "Lobby queue identity {} belongs to {}",
                        identity_key,
                        lobby.lobby_code
                    ));
                }
                Ok(lobby)
            })
            .transpose()
    }

    /// Remove a lobby from every queue it is present in, returning whether a lobby was removed
    pub async fn remove_lobby_from_all_queues_by_code(&mut self, lobby_code: &str) -> Result<bool> {
        if let Some(lobby) = self.get_queued_lobby_by_code(lobby_code).await? {
            return self.remove_exact_lobby_identity(&lobby).await;
        }

        self.execute_lobby_removal(LobbyRemovalPlan {
            lobby_active_game_key: RedisKeys::matchmaking_lobby_active_game(lobby_code),
            lobby_metadata_key: RedisKeys::lobby_metadata(lobby_code),
            queue_identity_key: RedisKeys::matchmaking_lobby_queue_identity(lobby_code),
            member_json: String::new(),
            user_queue_claims: Vec::new(),
            queue_pairs: Vec::new(),
        })
        .await
    }

    /// Remove a lobby from all matchmaking queues it was queued for
    /// This is used when a lobby is matched to prevent it from being matched again
    pub async fn remove_lobby_from_all_queues(&mut self, lobby: &QueuedLobby) -> Result<()> {
        if self.remove_exact_lobby_identity(lobby).await? {
            info!(
                "Removed lobby {} from all matchmaking queues (was queued for {:?})",
                lobby.lobby_code, lobby.game_types
            );
        }
        Ok(())
    }

    async fn remove_exact_lobby_identity(&mut self, lobby: &QueuedLobby) -> Result<bool> {
        let queue_claim = lobby_queue_claim(lobby);
        let plan = LobbyRemovalPlan {
            lobby_active_game_key: RedisKeys::matchmaking_lobby_active_game(&lobby.lobby_code),
            lobby_metadata_key: RedisKeys::lobby_metadata(&lobby.lobby_code),
            queue_identity_key: RedisKeys::matchmaking_lobby_queue_identity(&lobby.lobby_code),
            member_json: serde_json::to_string(lobby)?,
            user_queue_claims: lobby
                .members
                .iter()
                .map(|member| QueueIdentityClaim {
                    key: RedisKeys::matchmaking_user_queue_identity(member.user_id),
                    value: queue_claim.clone(),
                })
                .collect(),
            queue_pairs: lobby
                .game_types
                .iter()
                .map(|game_type| MatchCommitQueuePair {
                    queue_key: RedisKeys::matchmaking_lobby_queue(game_type, &lobby.queue_mode),
                    mmr_key: RedisKeys::matchmaking_lobby_mmr_index(game_type, &lobby.queue_mode),
                })
                .collect(),
        };
        self.execute_lobby_removal(plan).await
    }

    async fn execute_lobby_removal(&mut self, plan: LobbyRemovalPlan) -> Result<bool> {
        let plan_json = serde_json::to_string(&plan)?;
        let (code, detail): (i64, String) = redis::Script::new(REMOVE_LOBBY_SCRIPT)
            .arg(plan_json)
            .invoke_async(&mut self.redis)
            .await
            .context("Failed to atomically remove lobby queue identity")?;
        match code {
            1 => Ok(true),
            2 => Ok(false),
            0 => Err(anyhow!("Lobby queue removal was rejected: {detail}")),
            other => Err(anyhow!(
                "Lobby queue removal returned unknown status {other}: {detail}"
            )),
        }
    }

    /// Atomically claim queued lobbies and publish their complete GameCreated
    /// command. Selection/scoring stays in Rust; this operation is only the
    /// compare-and-commit boundary shared by every matchmaker task.
    #[allow(clippy::too_many_arguments)]
    pub async fn commit_match(
        &mut self,
        game_id: u32,
        partition_id: u32,
        selected_game_type: &GameType,
        selected_queue_mode: &common::QueueMode,
        match_info: &ActiveMatch,
        game_created_payload: &str,
        lobbies: &[QueuedLobby],
    ) -> Result<MatchCommitOutcome> {
        if lobbies.is_empty() {
            return Err(anyhow!("Cannot commit a match without lobbies"));
        }
        if game_created_payload.is_empty() {
            return Err(anyhow!("Cannot commit a match without GameCreated payload"));
        }

        let mut lobby_codes = HashSet::new();
        let mut user_ids = HashSet::new();
        let mut commit_lobbies = Vec::with_capacity(lobbies.len());
        let mut commit_users = Vec::new();

        for lobby in lobbies {
            if lobby.game_types.is_empty() {
                return Err(anyhow!(
                    "Lobby {} has no queue identities to claim",
                    lobby.lobby_code
                ));
            }
            if lobby.queue_mode != *selected_queue_mode
                || !lobby.game_types.contains(selected_game_type)
            {
                return Err(anyhow!(
                    "Lobby {} no longer identifies the selected queue",
                    lobby.lobby_code
                ));
            }
            if !lobby_codes.insert(lobby.lobby_code.clone()) {
                return Err(anyhow!(
                    "Lobby {} appears more than once in one match",
                    lobby.lobby_code
                ));
            }

            let member_json = serde_json::to_string(lobby)?;
            let queue_pairs = lobby
                .game_types
                .iter()
                .map(|game_type| MatchCommitQueuePair {
                    queue_key: RedisKeys::matchmaking_lobby_queue(game_type, &lobby.queue_mode),
                    mmr_key: RedisKeys::matchmaking_lobby_mmr_index(game_type, &lobby.queue_mode),
                })
                .collect();

            let queue_identity_value = lobby_queue_claim(lobby);
            for member in &lobby.members {
                if !user_ids.insert(member.user_id) {
                    return Err(anyhow!(
                        "User {} appears more than once in one match",
                        member.user_id
                    ));
                }
                commit_users.push(MatchCommitUser {
                    active_game_key: RedisKeys::matchmaking_user_active_game(member.user_id),
                    queue_status_key: RedisKeys::matchmaking_user_status(member.user_id),
                    queue_identity_key: RedisKeys::matchmaking_user_queue_identity(member.user_id),
                    queue_identity_value: queue_identity_value.clone(),
                });
            }

            commit_lobbies.push(MatchCommitLobby {
                lobby_code: lobby.lobby_code.clone(),
                member_json,
                active_game_key: RedisKeys::matchmaking_lobby_active_game(&lobby.lobby_code),
                metadata_key: RedisKeys::lobby_metadata(&lobby.lobby_code),
                queue_identity_key: RedisKeys::matchmaking_lobby_queue_identity(&lobby.lobby_code),
                queue_pairs,
            });
        }

        let active_match_json = serde_json::to_string(match_info)?;
        let notification_payload = serde_json::json!({
            "type": "MatchFound",
            "game_id": game_id,
            "partition_id": partition_id,
        })
        .to_string();
        let notifications = lobbies
            .iter()
            .map(|lobby| MatchCommitNotification {
                channel: RedisKeys::matchmaking_lobby_notification_channel(&lobby.lobby_code),
                payload: notification_payload.clone(),
            })
            .collect();
        let plan = MatchCommitPlan {
            active_matches_key: RedisKeys::matchmaking_active_matches(),
            command_stream_key: RedisKeys::stream_commands(partition_id),
            game_id: game_id.to_string(),
            active_match_json: active_match_json.clone(),
            game_created_payload: game_created_payload.to_string(),
            lobbies: commit_lobbies,
            users: commit_users,
            notifications,
        };
        let plan_json = serde_json::to_string(&plan)?;
        let script = redis::Script::new(COMMIT_MATCH_SCRIPT);
        let mut attempts = 0;
        let mut delay = self.retry_delay;
        let (code, detail) = loop {
            attempts += 1;
            match script
                .arg(&plan_json)
                .invoke_async::<(i64, String)>(&mut self.redis)
                .await
            {
                Ok(result) => break result,
                Err(error) if attempts < self.max_retries => {
                    warn!(
                        game_id,
                        attempt = attempts,
                        max_attempts = self.max_retries,
                        error = %error,
                        "Atomic match commit response was ambiguous; retrying the same claim"
                    );
                    sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(10));
                }
                Err(error) => {
                    // A connection can fail after Valkey has committed the script. A
                    // strong read through the same regional primary distinguishes that
                    // success whenever connectivity has recovered; otherwise the durable
                    // mappings still let reconnect recover a missed Pub/Sub notification.
                    let existing: redis::RedisResult<Option<String>> = self
                        .redis
                        .hget(RedisKeys::matchmaking_active_matches(), game_id.to_string())
                        .await;
                    if matches!(existing, Ok(Some(ref value)) if value == &active_match_json) {
                        return Ok(MatchCommitOutcome::AlreadyCommitted);
                    }
                    return Err(error).context("Failed to atomically commit matchmaking claim");
                }
            }
        };

        match code {
            1 => Ok(MatchCommitOutcome::Committed { stream_id: detail }),
            2 => Ok(MatchCommitOutcome::AlreadyCommitted),
            0 => {
                crate::resilience_metrics::record_match_claim_conflicts(1);
                Ok(MatchCommitOutcome::Conflict { reason: detail })
            }
            other => Err(anyhow!(
                "Atomic matchmaking script returned unknown status {} ({})",
                other,
                detail
            )),
        }
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

    /// Resolve a committed match without relying on best-effort Pub/Sub.
    pub async fn get_user_active_game(&mut self, user_id: u32) -> Result<Option<u32>> {
        self.get_active_game_mapping(RedisKeys::matchmaking_user_active_game(user_id), "user")
            .await
    }

    /// Resolve a committed lobby match without relying on best-effort Pub/Sub.
    pub async fn get_lobby_active_game(&mut self, lobby_code: &str) -> Result<Option<u32>> {
        self.get_active_game_mapping(
            RedisKeys::matchmaking_lobby_active_game(lobby_code),
            "lobby",
        )
        .await
    }

    async fn get_active_game_mapping(&mut self, key: String, kind: &str) -> Result<Option<u32>> {
        let game_id: Option<String> = self.redis.get(&key).await?;
        game_id
            .map(|value| {
                value.parse::<u32>().with_context(|| {
                    format!("Malformed {kind} active-game mapping at Redis key {key}")
                })
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redis_utils;
    use redis::Client;

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
