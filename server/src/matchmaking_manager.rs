use crate::db::Database;
use crate::lobby_manager::{MAX_LOBBY_MEMBERS, lobby_membership_valid_until_ms};
use crate::matchmaking_pool::MatchmakingPool;
use crate::player_idle::PlayerIdleConfig;
use crate::redis_keys::RedisKeys;
use crate::redis_utils::RedisConnection;
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use common::{BoostConfig, GameType, MATCH_READY_WINDOW_MS};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

/// Fixed queue families consumed by the production matchmaking loop. Metrics
/// aggregate these ten queues without adding game-type or mode dimensions.
pub const MATCHMAKING_GAME_TYPES: [GameType; 5] = [
    GameType::Solo,
    GameType::FreeForAll { max_players: 2 },
    GameType::FreeForAll { max_players: 4 },
    GameType::TeamMatch { per_team: 1 },
    GameType::TeamMatch { per_team: 2 },
];
pub const MATCHMAKING_QUEUE_MODES: [common::QueueMode; 2] = [
    common::QueueMode::Quickmatch,
    common::QueueMode::Competitive,
];

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
    #[serde(default)]
    pub matchmaking_pool: MatchmakingPool,
    /// Exact immutable Redis member observed by the sampler/by-code lookup.
    /// It is runtime provenance, never part of the wire/storage schema.
    #[serde(skip)]
    pub(crate) queue_identity_json: Option<String>,
}

impl QueuedLobby {
    /// Preserve unknown fields and legacy/defaulted encodings when fencing an
    /// observed Redis identity. If a caller mutates the parsed lobby, fall
    /// back to its new canonical JSON so stale snapshots cannot borrow the
    /// original generation's authority.
    fn exact_queue_identity_json(&self) -> Result<String> {
        if let Some(raw) = self.queue_identity_json.as_deref()
            && let Ok(observed) = serde_json::from_str::<Self>(raw)
            && serde_json::to_value(&observed)? == serde_json::to_value(self)?
        {
            return Ok(raw.to_owned());
        }
        Ok(serde_json::to_string(self)?)
    }
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
    #[serde(default)]
    pub matchmaking_pool: MatchmakingPool,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum MatchStatus {
    Waiting,
    Active,
    Finished,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchCommitOutcome {
    Committed { outbox_id: String },
    AlreadyCommitted,
    Conflict { reason: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AtomicMatchmakingOutcome {
    IntegrityError,
    Applied,
    Idempotent,
    ExpectedConflict,
}

fn classify_atomic_matchmaking_outcome(code: i64) -> Option<AtomicMatchmakingOutcome> {
    match code {
        0 => Some(AtomicMatchmakingOutcome::IntegrityError),
        1 => Some(AtomicMatchmakingOutcome::Applied),
        2 => Some(AtomicMatchmakingOutcome::Idempotent),
        3 => Some(AtomicMatchmakingOutcome::ExpectedConflict),
        _ => None,
    }
}

/// The analytics label for an admission outcome. Reporting the classification
/// rather than only the successes is what makes a rejection rate visible at
/// all; the constants live in the sink so the emitted string and the sink's own
/// queue-wait bookkeeping cannot drift apart.
fn admission_result_label(outcome: Option<AtomicMatchmakingOutcome>) -> &'static str {
    use crate::analytics::sink;
    match outcome {
        Some(AtomicMatchmakingOutcome::Applied) => sink::ADMISSION_APPLIED,
        Some(AtomicMatchmakingOutcome::Idempotent) => sink::ADMISSION_IDEMPOTENT,
        Some(AtomicMatchmakingOutcome::ExpectedConflict) => sink::ADMISSION_REJECTED,
        Some(AtomicMatchmakingOutcome::IntegrityError) => sink::ADMISSION_INTEGRITY_ERROR,
        None => sink::ADMISSION_UNKNOWN,
    }
}

#[derive(Serialize)]
struct MatchCommitQueuePair {
    queue_key: String,
    mmr_key: String,
}

#[derive(Serialize)]
struct MatchCommitLobby {
    lobby_code: String,
    queue_token: String,
    member_json: String,
    active_game_key: String,
    metadata_key: String,
    queue_identity_key: String,
    queue_lease_key: String,
    queue_outcome_key: String,
    queue_pairs: Vec<MatchCommitQueuePair>,
}

#[derive(Serialize)]
struct MatchCommitUser {
    active_game_key: String,
    pending_game_key: String,
    queue_status_key: String,
    queue_identity_key: String,
    queue_identity_value: String,
}

#[derive(Serialize)]
struct MatchCommitPlan {
    active_matches_key: String,
    outbox_key: String,
    outbox_age_key: String,
    outbox_payload: String,
    created_at_ms: i64,
    game_id: String,
    pending_game_ttl_ms: i64,
    queue_outcome_ttl_ms: i64,
    enforce_queue_liveness: bool,
    active_match_json: String,
    matchmaking_pool: String,
    lobbies: Vec<MatchCommitLobby>,
    users: Vec<MatchCommitUser>,
    notifications: Vec<MatchCommitNotification>,
}

const GAME_CREATED_OUTBOX_SCHEMA_VERSION: u16 = 1;
/// A pending match assignment outlives the readiness gate and normal short
/// rounds, while still self-cleaning if a client disappears before it can
/// acknowledge the handoff.
const LOBBY_MATCH_HANDOFF_TTL_MS: i64 = 15 * 60 * 1_000;
pub(crate) const LOBBY_QUEUE_LEASE_TTL_MS: i64 = 5 * 60 * 1_000;
pub(crate) const LOBBY_QUEUE_OUTCOME_TTL_MS: i64 = 60 * 60 * 1_000;
pub const MATCHMAKING_QUEUE_LEASE_ENFORCEMENT_ENV: &str =
    "SNAKETRON_MATCHMAKING_QUEUE_LEASE_ENFORCEMENT";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GameCreatedOutboxRecord {
    pub schema_version: u16,
    pub game_id: u32,
    pub partition_id: u32,
    pub game_created_payload: String,
}

impl GameCreatedOutboxRecord {
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != GAME_CREATED_OUTBOX_SCHEMA_VERSION {
            return Err(anyhow!("unsupported game-created outbox schema"));
        }
        if self.partition_id != self.game_id % crate::game_executor::PARTITION_COUNT {
            return Err(anyhow!("game-created outbox partition mismatch"));
        }
        match serde_json::from_str::<crate::game_executor::StreamEvent>(&self.game_created_payload)?
        {
            crate::game_executor::StreamEvent::GameCreated { game_id, .. }
                if game_id == self.game_id =>
            {
                Ok(())
            }
            _ => Err(anyhow!("game-created outbox payload identity mismatch")),
        }
    }
}

#[derive(Serialize)]
struct MatchCommitNotification {
    channel: String,
    payload: String,
}

#[derive(Debug, Clone, Copy)]
pub struct LobbyMembershipFence {
    pub revision: i64,
    pub valid_until_ms: i64,
}

#[derive(Debug)]
pub struct LobbyAdmissionRejected {
    pub detail: String,
}

impl LobbyAdmissionRejected {
    pub fn is_retryable_membership_conflict(&self) -> bool {
        matches!(
            self.detail.as_str(),
            "lobby-membership-reservation-expired"
                | "lobby-membership-changing"
                | "lobby-membership-changed"
                | "lobby-membership-lease-expired"
        )
    }
}

impl std::fmt::Display for LobbyAdmissionRejected {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "Lobby admission was rejected: {}", self.detail)
    }
}

impl std::error::Error for LobbyAdmissionRejected {}

#[derive(Serialize)]
struct LobbyAdmissionPlan {
    lobby_active_game_key: String,
    lobby_metadata_key: String,
    membership_reservations_key: String,
    queue_identity_key: String,
    queue_lease_key: String,
    queue_outcome_key: String,
    queue_token: String,
    user_active_game_keys: Vec<String>,
    user_queue_claims: Vec<QueueIdentityClaim>,
    member_user_ids: Vec<u32>,
    member_json: String,
    queued_at: i64,
    avg_mmr: i32,
    matchmaking_pool: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    expected_ad_break_id: Option<String>,
    expected_membership_revision: i64,
    membership_valid_until_ms: i64,
    metadata_idle_ttl_ms: i64,
    queue_lease_ttl_ms: i64,
    queue_outcome_ttl_ms: i64,
    queue_pairs: Vec<MatchCommitQueuePair>,
}

#[derive(Serialize)]
struct LobbyRemovalPlan {
    lobby_active_game_key: String,
    lobby_metadata_key: String,
    queue_identity_key: String,
    queue_lease_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue_outcome_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue_token: Option<String>,
    member_json: String,
    metadata_idle_ttl_ms: i64,
    queue_outcome_ttl_ms: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    requesting_user_id: Option<u32>,
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
local redis_time = redis.call('TIME')
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)

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
    return {3, 'lobby-already-matched'}
end
local lobby_metadata_type = key_type(plan.lobby_metadata_key)
if lobby_metadata_type == 'none' then
    return {3, 'lobby-metadata-missing'}
end
if lobby_metadata_type ~= 'hash' then
    return {0, 'lobby-metadata-wrong-type'}
end
local lobby_pool = redis.call('HGET', plan.lobby_metadata_key, 'matchmakingPool')
if not lobby_pool or lobby_pool == '' then lobby_pool = 'public' end
if lobby_pool ~= plan.matchmaking_pool then
    return {3, 'lobby-pool-mismatch'}
end
local reservations_type = key_type(plan.membership_reservations_key)
if reservations_type ~= 'none' and reservations_type ~= 'zset' then
    return {0, 'membership-reservations-wrong-type'}
end
for _, key in ipairs(plan.user_active_game_keys) do
    local mapping_type = key_type(key)
    if mapping_type ~= 'none' and mapping_type ~= 'string' then
        return {0, 'user-mapping-wrong-type'}
    end
    if redis.call('GET', key) then return {3, 'user-already-matched'} end
end

local identity_type = key_type(plan.queue_identity_key)
if identity_type ~= 'none' and identity_type ~= 'string' then
    return {0, 'queue-identity-wrong-type'}
end
local lease_type = key_type(plan.queue_lease_key)
if lease_type ~= 'none' and lease_type ~= 'string' then
    return {0, 'queue-lease-wrong-type'}
end
local outcome_type = key_type(plan.queue_outcome_key)
if outcome_type ~= 'none' and outcome_type ~= 'string' then
    return {0, 'queue-outcome-wrong-type'}
end
local existing_identity = redis.call('GET', plan.queue_identity_key)
if existing_identity and existing_identity ~= plan.member_json then
    return {3, 'queue-entry-changed'}
end
local is_replay = existing_identity ~= false
local admitted_outcome = 'admitted:' .. plan.queue_token
local queue_outcome = redis.call('GET', plan.queue_outcome_key)
if queue_outcome then
    local outcome_status, outcome_token = string.match(queue_outcome, '^([^:]+):(.+)$')
    if not outcome_status or not outcome_token then
        return {0, 'queue-outcome-malformed'}
    end
    if outcome_token == plan.queue_token and outcome_status ~= 'admitted' then
        return {3, 'queue-operation-' .. outcome_status}
    end
end
if queue_outcome == admitted_outcome and not is_replay then
    return {3, 'queue-operation-finished'}
end

if not is_replay then
    -- Every waiting->queued transition uses the same cross-slot membership
    -- seqlock as ad-break admission. Expired abandoned writers advance the
    -- revision before the caller retries with a fresh member snapshot.
    local expired_reservations = redis.call(
        'ZREMRANGEBYSCORE', plan.membership_reservations_key, '-inf', now_ms
    )
    if expired_reservations > 0 then
        redis.call('HINCRBY', plan.lobby_metadata_key, 'membershipRevision', 1)
        return {3, 'lobby-membership-reservation-expired'}
    end
    if redis.call('ZCARD', plan.membership_reservations_key) > 0 then
        return {3, 'lobby-membership-changing'}
    end
    local current_revision = tonumber(
        redis.call('HGET', plan.lobby_metadata_key, 'membershipRevision')
    ) or 0
    if current_revision ~= tonumber(plan.expected_membership_revision) then
        return {3, 'lobby-membership-changed'}
    end
    if now_ms >= tonumber(plan.membership_valid_until_ms) then
        return {3, 'lobby-membership-lease-expired'}
    end

    local lobby_state = redis.call('HGET', plan.lobby_metadata_key, 'state')
    if plan.expected_ad_break_id then
        if lobby_state ~= 'ad_break' then
            return {3, 'ad-break-no-longer-active'}
        end
        local raw_ad_break = redis.call('HGET', plan.lobby_metadata_key, 'adBreak')
        if not raw_ad_break then return {3, 'ad-break-missing'} end
        local ad_break = cjson.decode(raw_ad_break)
        if ad_break.id ~= plan.expected_ad_break_id then
            return {3, 'ad-break-replaced'}
        end
        if #ad_break.participant_user_ids ~= #plan.member_user_ids then
            return {3, 'ad-break-roster-changed'}
        end
        for index, participant in ipairs(ad_break.participant_user_ids) do
            if tonumber(participant) ~= tonumber(plan.member_user_ids[index]) then
                return {3, 'ad-break-roster-changed'}
            end
        end
        if not ad_break.resolutions then return {3, 'ad-break-unresolved'} end
        for _, participant in ipairs(ad_break.participant_user_ids) do
            if not ad_break.resolutions[tostring(participant)] then
                return {3, 'ad-break-unresolved'}
            end
        end
    elseif lobby_state ~= 'waiting' then
        return {3, 'lobby-not-waiting'}
    end
end

for _, claim in ipairs(plan.user_queue_claims) do
    local claim_type = key_type(claim.key)
    if claim_type ~= 'none' and claim_type ~= 'string' then
        return {0, 'user-queue-identity-wrong-type'}
    end
    local existing_claim = redis.call('GET', claim.key)
    if existing_claim and existing_claim ~= claim.value then
        return {3, 'user-already-queued'}
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
redis.call('SET', plan.queue_lease_key, plan.member_json, 'PX', plan.queue_lease_ttl_ms)
redis.call('SET', plan.queue_outcome_key, admitted_outcome, 'PX', plan.queue_outcome_ttl_ms)
for _, claim in ipairs(plan.user_queue_claims) do
    redis.call('SET', claim.key, claim.value)
end
redis.call('HSET', plan.lobby_metadata_key, 'state', 'queued')
if plan.expected_ad_break_id then
    redis.call('HDEL', plan.lobby_metadata_key, 'adBreak')
end
-- Queue identity and user claims are durable. Keep the authoritative lobby
-- metadata durable for the same generation so a gateway restart can restore
-- a queued member's handle and the matcher never wedges on missing metadata.
redis.call('PERSIST', plan.lobby_metadata_key)
if is_replay then return {2, 'already-queued'} end
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
local lease_type = key_type(plan.queue_lease_key)
if lease_type ~= 'none' and lease_type ~= 'string' then
    return {0, 'queue-lease-wrong-type'}
end
if plan.queue_outcome_key then
    local outcome_type = key_type(plan.queue_outcome_key)
    if outcome_type ~= 'none' and outcome_type ~= 'string' then
        return {0, 'queue-outcome-wrong-type'}
    end
end
local active_mapping_type = key_type(plan.lobby_active_game_key)
if active_mapping_type ~= 'none' and active_mapping_type ~= 'string' then
    return {0, 'lobby-mapping-wrong-type'}
end
local metadata_type = key_type(plan.lobby_metadata_key)
if metadata_type ~= 'none' and metadata_type ~= 'hash' then
    return {0, 'lobby-metadata-wrong-type'}
end
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

local existing_identity = redis.call('GET', plan.queue_identity_key)
local existing_active_mapping = redis.call('GET', plan.lobby_active_game_key)
if existing_identity and existing_active_mapping then
    return {2, 'lobby-already-matched'}
end
local outcome_status = false
local outcome_token = false
if plan.queue_outcome_key then
    local existing_outcome = redis.call('GET', plan.queue_outcome_key)
    if existing_outcome then
        outcome_status, outcome_token = string.match(existing_outcome, '^([^:]+):(.+)$')
        if not outcome_status or not outcome_token then
            return {0, 'queue-outcome-malformed'}
        end
    end
end

local function restore_idle_metadata()
    if not redis.call('GET', plan.lobby_active_game_key) and metadata_type == 'hash' then
        if redis.call('HGET', plan.lobby_metadata_key, 'state') == 'queued' then
            redis.call('HSET', plan.lobby_metadata_key, 'state', 'waiting')
        end
        redis.call('PEXPIRE', plan.lobby_metadata_key, plan.metadata_idle_ttl_ms)
    end
end

local function finish_exact_cancellation()
    -- The exact-token terminal value is a write-ahead log. A retry repairs
    -- every source/claim even if queue_identity was already deleted.
    redis.call('SET', plan.queue_outcome_key, 'cancelled:' .. plan.queue_token, 'PX', plan.queue_outcome_ttl_ms)
    redis.call('DEL', plan.queue_identity_key)
    if redis.call('GET', plan.queue_lease_key) == plan.member_json then
        redis.call('DEL', plan.queue_lease_key)
    end
    for _, claim in ipairs(plan.user_queue_claims) do
        if redis.call('GET', claim.key) == claim.value then
            redis.call('DEL', claim.key)
        end
    end
    restore_idle_metadata()
    -- Queue indexes are the discoverable repair worklist. Remove them only
    -- after every identity, claim, and metadata side effect has converged.
    for _, pair in ipairs(plan.queue_pairs) do
        redis.call('ZREM', pair.queue_key, plan.member_json)
        redis.call('ZREM', pair.mmr_key, plan.member_json)
    end
end

if not existing_identity then
    if plan.queue_token then
        if outcome_token and outcome_token ~= plan.queue_token then
            return {2, 'queue-generation-advanced'}
        end
        if outcome_status and outcome_status ~= 'admitted' and outcome_status ~= 'cancelled' then
            return {2, 'queue-operation-' .. outcome_status}
        end
        finish_exact_cancellation()
        return {2, 'cancellation-repaired'}
    end

    -- No exact identity was observed. This path only handles an authorized
    -- ad-break cancellation or restores orphaned queued metadata.
    if not redis.call('GET', plan.lobby_active_game_key) and metadata_type == 'hash' then
        if plan.requesting_user_id and redis.call('HGET', plan.lobby_metadata_key, 'state') == 'ad_break' then
            local raw_ad_break = redis.call('HGET', plan.lobby_metadata_key, 'adBreak')
            if raw_ad_break then
                local ad_break = cjson.decode(raw_ad_break)
                local participant_found = false
                for _, participant in ipairs(ad_break.participant_user_ids) do
                    if tonumber(participant) == tonumber(plan.requesting_user_id) then
                        participant_found = true
                        break
                    end
                end
                if not participant_found then return {0, 'not-an-ad-break-participant'} end
                redis.call('HSET', plan.lobby_metadata_key, 'state', 'waiting')
                redis.call('HDEL', plan.lobby_metadata_key, 'adBreak')
                redis.call('PEXPIRE', plan.lobby_metadata_key, plan.metadata_idle_ttl_ms)
                return {1, 'cancelled-ad-break'}
            end
        end
        restore_idle_metadata()
    end
    return {2, 'not-queued'}
end

if existing_identity ~= plan.member_json then
    return {2, 'queue-entry-changed'}
end
if outcome_status and outcome_status ~= 'admitted' and outcome_status ~= 'cancelled' then
    if outcome_token == plan.queue_token then
        return {2, 'queue-operation-' .. outcome_status}
    end
end
if plan.requesting_user_id then
    local queued_lobby = cjson.decode(existing_identity)
    local participant_found = false
    for _, member in ipairs(queued_lobby.members) do
        if tonumber(member.user_id) == tonumber(plan.requesting_user_id) then
            participant_found = true
            break
        end
    end
    if not participant_found then return {0, 'not-a-queued-member'} end
end

finish_exact_cancellation()
return {1, 'removed'}
"#;

/// Reap an expired queue generation with a second, atomic lease check. A
/// heartbeat that renews the exact identity after the sampler's MGET wins and
/// prevents cleanup. If the lobby code has already advanced to a different
/// generation, only the old exact ZSET members are removed.
const REAP_STALE_LOBBY_SCRIPT: &str = r#"
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
local lease_type = key_type(plan.queue_lease_key)
if lease_type ~= 'none' and lease_type ~= 'string' then
    return {0, 'queue-lease-wrong-type'}
end
if plan.queue_outcome_key then
    local outcome_type = key_type(plan.queue_outcome_key)
    if outcome_type ~= 'none' and outcome_type ~= 'string' then
        return {0, 'queue-outcome-wrong-type'}
    end
end
local active_mapping_type = key_type(plan.lobby_active_game_key)
if active_mapping_type ~= 'none' and active_mapping_type ~= 'string' then
    return {0, 'lobby-mapping-wrong-type'}
end
local metadata_type = key_type(plan.lobby_metadata_key)
if metadata_type ~= 'none' and metadata_type ~= 'hash' then
    return {0, 'lobby-metadata-wrong-type'}
end
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

local existing_identity = redis.call('GET', plan.queue_identity_key)
local existing_lease = redis.call('GET', plan.queue_lease_key)
local existing_outcome = false
local outcome_status = false
local outcome_token = false
if plan.queue_outcome_key then
    existing_outcome = redis.call('GET', plan.queue_outcome_key)
    if existing_outcome then
        outcome_status, outcome_token = string.match(existing_outcome, '^([^:]+):(.+)$')
        if not outcome_status or not outcome_token then
            return {0, 'queue-outcome-malformed'}
        end
    end
end
local same_operation_terminal = outcome_token == plan.queue_token and outcome_status ~= 'admitted'
if existing_identity == plan.member_json
    and existing_lease == plan.member_json
    and not same_operation_terminal then
    return {2, 'queue-lease-renewed'}
end
if existing_identity and existing_identity ~= plan.member_json then
    for _, pair in ipairs(plan.queue_pairs) do
        redis.call('ZREM', pair.queue_key, plan.member_json)
        redis.call('ZREM', pair.mmr_key, plan.member_json)
    end
    return {1, 'stale-index-members-pruned'}
end
if not existing_identity and outcome_token and outcome_token ~= plan.queue_token then
    for _, pair in ipairs(plan.queue_pairs) do
        redis.call('ZREM', pair.queue_key, plan.member_json)
        redis.call('ZREM', pair.mmr_key, plan.member_json)
    end
    return {1, 'stale-index-members-pruned'}
end

-- This exact-token terminal value is the cleanup write-ahead log. Heartbeats
-- refuse to renew it, while a retry recognizes it and resumes idempotently.
if plan.queue_outcome_key then
    redis.call('SET', plan.queue_outcome_key, 'expired:' .. plan.queue_token, 'PX', plan.queue_outcome_ttl_ms)
end
redis.call('DEL', plan.queue_identity_key)
if existing_lease == plan.member_json then
    redis.call('DEL', plan.queue_lease_key)
end
for _, claim in ipairs(plan.user_queue_claims) do
    if redis.call('GET', claim.key) == claim.value then
        redis.call('DEL', claim.key)
    end
end
if not redis.call('GET', plan.lobby_active_game_key) and metadata_type == 'hash' then
    if redis.call('HGET', plan.lobby_metadata_key, 'state') == 'queued' then
        redis.call('HSET', plan.lobby_metadata_key, 'state', 'waiting')
    end
    redis.call('PEXPIRE', plan.lobby_metadata_key, plan.metadata_idle_ttl_ms)
end
for _, pair in ipairs(plan.queue_pairs) do
    redis.call('ZREM', pair.queue_key, plan.member_json)
    redis.call('ZREM', pair.mmr_key, plan.member_json)
end
return {1, 'expired-generation-reaped'}
"#;

// Redis scripts are isolated but do not roll back commands that precede a
// runtime script error. Validate every key type and every claim predicate
// before issuing the first write. The write phase then uses only validated
// key types; any Serverless capacity rejection is surfaced to the caller and
// the match is treated as uncommitted unless the durable record is observed.
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

local outbox_type = key_type(plan.outbox_key)
if outbox_type ~= 'none' and outbox_type ~= 'hash' then
    return {0, 'game-created-outbox-wrong-type'}
end

local outbox_age_type = key_type(plan.outbox_age_key)
if outbox_age_type ~= 'none' and outbox_age_type ~= 'zset' then
    return {0, 'game-created-outbox-age-wrong-type'}
end

local existing_active = redis.call('HGET', plan.active_matches_key, plan.game_id)
if existing_active and existing_active ~= plan.active_match_json then
    return {0, 'game-id-already-committed'}
end
local existing_outbox = redis.call('HGET', plan.outbox_key, plan.game_id)
if existing_outbox and existing_outbox ~= plan.outbox_payload then
    return {0, 'game-created-outbox-conflict'}
end
local existing_outbox_age = redis.call('ZSCORE', plan.outbox_age_key, plan.game_id)
if existing_outbox_age and tonumber(existing_outbox_age) ~= tonumber(plan.created_at_ms) then
    return {0, 'game-created-outbox-age-conflict'}
end

-- Destination mappings are the atomic, cross-lobby write-ahead lock. MSET
-- below installs all of them in one Redis command before any source deletion.
-- If a later command fails, exact mappings or the active-match record put the
-- retry in repair mode; a competing match observes a different game ID.
local repairing = existing_active ~= false
local any_destination_mapping = false
local all_destination_mappings = true
for _, lobby in ipairs(plan.lobbies) do
    local mapping_type = key_type(lobby.active_game_key)
    if mapping_type ~= 'none' and mapping_type ~= 'string' then
        return {0, 'lobby-mapping-wrong-type:' .. lobby.lobby_code}
    end
    local mapping = redis.call('GET', lobby.active_game_key)
    if mapping then
        if mapping ~= plan.game_id then
            return {3, 'lobby-already-matched:' .. lobby.lobby_code}
        end
        any_destination_mapping = true
    else
        all_destination_mappings = false
    end
end
for _, user in ipairs(plan.users) do
    local mapping_type = key_type(user.active_game_key)
    if mapping_type ~= 'none' and mapping_type ~= 'string' then
        return {0, 'user-mapping-wrong-type'}
    end
    local mapping = redis.call('GET', user.active_game_key)
    if mapping then
        if mapping ~= plan.game_id then return {3, 'user-already-matched'} end
        any_destination_mapping = true
    else
        all_destination_mappings = false
    end
    local pending_type = key_type(user.pending_game_key)
    if pending_type ~= 'none' and pending_type ~= 'string' then
        return {0, 'user-pending-game-wrong-type'}
    end
    local pending = redis.call('GET', user.pending_game_key)
    if pending then
        if pending ~= plan.game_id then return {3, 'user-pending-game-changed'} end
    end
    local queue_status_type = key_type(user.queue_status_key)
    if queue_status_type ~= 'none' and queue_status_type ~= 'hash' then
        return {0, 'user-queue-status-wrong-type'}
    end
end
if any_destination_mapping and all_destination_mappings then
    repairing = true
elseif any_destination_mapping and not existing_active then
    return {0, 'incomplete-destination-write-ahead-lock'}
end

-- A pending assignment without the destination WAL belongs to an older or
-- conflicting operation. Repair may recreate an absent assignment or retain
-- the exact one, but a fresh commit must begin with no pending assignment.
for _, user in ipairs(plan.users) do
    if redis.call('GET', user.pending_game_key) and not repairing then
        return {3, 'user-pending-game-already-set'}
    end
end

-- A durable outbox record can outlive terminal cleanup of active mappings.
-- It proves this exact game command committed; do not resurrect a completed
-- match merely because a very late caller retried.
if existing_outbox and not repairing then
    redis.call('ZADD', plan.outbox_age_key, plan.created_at_ms, plan.game_id)
    return {2, 'already-committed'}
end

for _, lobby in ipairs(plan.lobbies) do
    local metadata_type = key_type(lobby.metadata_key)
    if metadata_type == 'none' and not repairing then
        return {3, 'lobby-metadata-missing:' .. lobby.lobby_code}
    end
    if metadata_type ~= 'none' and metadata_type ~= 'hash' then
        return {0, 'lobby-metadata-wrong-type:' .. lobby.lobby_code}
    end
    if metadata_type == 'hash' then
        local lobby_pool = redis.call('HGET', lobby.metadata_key, 'matchmakingPool')
        if not lobby_pool or lobby_pool == '' then lobby_pool = 'public' end
        if lobby_pool ~= plan.matchmaking_pool then
            return {0, 'lobby-pool-mismatch:' .. lobby.lobby_code}
        end
    end

    local queue_identity_type = key_type(lobby.queue_identity_key)
    if queue_identity_type ~= 'none' and queue_identity_type ~= 'string' then
        return {0, 'queue-identity-wrong-type:' .. lobby.lobby_code}
    end
    local queue_identity = redis.call('GET', lobby.queue_identity_key)
    if queue_identity and queue_identity ~= lobby.member_json then
        return {3, 'queue-entry-changed:' .. lobby.lobby_code}
    end
    if not queue_identity and not repairing then
        return {3, 'queue-identity-missing:' .. lobby.lobby_code}
    end

    local queue_outcome_type = key_type(lobby.queue_outcome_key)
    if queue_outcome_type ~= 'none' and queue_outcome_type ~= 'string' then
        return {0, 'queue-outcome-wrong-type:' .. lobby.lobby_code}
    end
    local queue_outcome = redis.call('GET', lobby.queue_outcome_key)
    local outcome_status = false
    local outcome_token = false
    if queue_outcome then
        outcome_status, outcome_token = string.match(queue_outcome, '^([^:]+):(.+)$')
        if not outcome_status or not outcome_token then
            return {0, 'queue-outcome-malformed:' .. lobby.lobby_code}
        end
        if outcome_status ~= 'admitted'
            and outcome_status ~= 'cancelled'
            and outcome_status ~= 'expired'
            and outcome_status ~= 'matched' then
            return {0, 'queue-outcome-unknown:' .. lobby.lobby_code}
        end
        if outcome_token ~= lobby.queue_token then
            -- During the explicitly configured phase-one migration, an old
            -- gateway can admit a legacy generation without replacing this
            -- stable marker. Exact identity/claims/index members remain the
            -- authority in that mode. Repair and enforced mode stay strict.
            if repairing or plan.enforce_queue_liveness then
                return {3, 'queue-generation-changed:' .. lobby.lobby_code}
            end
            queue_outcome = false
            outcome_status = false
            outcome_token = false
        end
    end
    if not repairing and outcome_status and outcome_status ~= 'admitted' then
        return {3, 'queue-operation-' .. outcome_status .. ':' .. lobby.lobby_code}
    end
    if not repairing and plan.enforce_queue_liveness and not queue_outcome then
        return {3, 'queue-schema-marker-missing:' .. lobby.lobby_code}
    end
    local queue_lease_type = key_type(lobby.queue_lease_key)
    if queue_lease_type ~= 'none' and queue_lease_type ~= 'string' then
        return {0, 'queue-lease-wrong-type:' .. lobby.lobby_code}
    end
    local queue_lease = redis.call('GET', lobby.queue_lease_key)
    if queue_lease and queue_lease ~= lobby.member_json then
        return {3, 'queue-lease-changed:' .. lobby.lobby_code}
    end
    if not repairing then
        local current_schema = outcome_status == 'admitted'
        if (plan.enforce_queue_liveness or current_schema) and not queue_lease then
            return {3, 'queue-lease-expired:' .. lobby.lobby_code}
        end
    end

    for _, pair in ipairs(lobby.queue_pairs) do
        local queue_type = key_type(pair.queue_key)
        local mmr_type = key_type(pair.mmr_key)
        if queue_type ~= 'none' and queue_type ~= 'zset' then
            return {0, 'queue-wrong-type:' .. lobby.lobby_code}
        end
        if mmr_type ~= 'none' and mmr_type ~= 'zset' then
            return {0, 'mmr-index-wrong-type:' .. lobby.lobby_code}
        end
        if not repairing then
            if queue_type ~= 'zset' or not redis.call('ZSCORE', pair.queue_key, lobby.member_json) then
                return {3, 'queue-entry-changed:' .. lobby.lobby_code}
            end
            if mmr_type ~= 'zset' or not redis.call('ZSCORE', pair.mmr_key, lobby.member_json) then
                return {3, 'mmr-entry-changed:' .. lobby.lobby_code}
            end
        end
    end
end

for _, user in ipairs(plan.users) do
    local user_queue_identity_type = key_type(user.queue_identity_key)
    if user_queue_identity_type ~= 'none' and user_queue_identity_type ~= 'string' then
        return {0, 'user-queue-identity-wrong-type'}
    end
    local user_queue_identity = redis.call('GET', user.queue_identity_key)
    if user_queue_identity and user_queue_identity ~= user.queue_identity_value then
        return {3, 'user-queue-entry-changed'}
    end
    if not user_queue_identity and not repairing then
        return {3, 'user-queue-identity-missing'}
    end
end

local mapping_args = {}
for _, lobby in ipairs(plan.lobbies) do
    table.insert(mapping_args, lobby.active_game_key)
    table.insert(mapping_args, plan.game_id)
end
for _, user in ipairs(plan.users) do
    table.insert(mapping_args, user.active_game_key)
    table.insert(mapping_args, plan.game_id)
end
redis.call('MSET', unpack(mapping_args))
redis.call('HSET', plan.active_matches_key, plan.game_id, plan.active_match_json)
redis.call('HSET', plan.outbox_key, plan.game_id, plan.outbox_payload)
redis.call('ZADD', plan.outbox_age_key, plan.created_at_ms, plan.game_id)

for _, lobby in ipairs(plan.lobbies) do
    redis.call('SET', lobby.queue_outcome_key, 'matched:' .. lobby.queue_token, 'PX', plan.queue_outcome_ttl_ms)
    redis.call('DEL', lobby.queue_identity_key)
    redis.call('DEL', lobby.queue_lease_key)
    if key_type(lobby.metadata_key) == 'hash' then
        redis.call('HSET', lobby.metadata_key, 'state', 'matched')
        redis.call('PEXPIRE', lobby.metadata_key, plan.pending_game_ttl_ms)
    end
end
for _, user in ipairs(plan.users) do
    redis.call('SET', user.active_game_key, plan.game_id)
    redis.call('SET', user.pending_game_key, plan.game_id, 'PX', plan.pending_game_ttl_ms)
    redis.call('DEL', user.queue_status_key)
    redis.call('DEL', user.queue_identity_key)
end

-- Queue indexes are the repair worklist. They are removed only after every
-- durable mapping, outbox record, identity, claim, and metadata write is done.
for _, lobby in ipairs(plan.lobbies) do
    for _, pair in ipairs(lobby.queue_pairs) do
        redis.call('ZREM', pair.queue_key, lobby.member_json)
        redis.call('ZREM', pair.mmr_key, lobby.member_json)
    end
end

if not repairing then
    for _, notification in ipairs(plan.notifications) do
        redis.call('PUBLISH', notification.channel, notification.payload)
    end
end
if repairing then return {2, 'already-committed'} end
return {1, plan.game_id}
"#;

impl LobbyAdmissionPlan {
    fn redis_keys(&self) -> Vec<&str> {
        let mut keys = vec![
            self.lobby_active_game_key.as_str(),
            self.lobby_metadata_key.as_str(),
            self.membership_reservations_key.as_str(),
            self.queue_identity_key.as_str(),
            self.queue_lease_key.as_str(),
            self.queue_outcome_key.as_str(),
        ];
        keys.extend(self.user_active_game_keys.iter().map(String::as_str));
        keys.extend(
            self.user_queue_claims
                .iter()
                .map(|claim| claim.key.as_str()),
        );
        for pair in &self.queue_pairs {
            keys.push(&pair.queue_key);
            keys.push(&pair.mmr_key);
        }
        keys
    }
}

impl LobbyRemovalPlan {
    fn redis_keys(&self) -> Vec<&str> {
        let mut keys = vec![
            self.lobby_active_game_key.as_str(),
            self.lobby_metadata_key.as_str(),
            self.queue_identity_key.as_str(),
            self.queue_lease_key.as_str(),
        ];
        if let Some(queue_outcome_key) = self.queue_outcome_key.as_deref() {
            keys.push(queue_outcome_key);
        }
        keys.extend(
            self.user_queue_claims
                .iter()
                .map(|claim| claim.key.as_str()),
        );
        for pair in &self.queue_pairs {
            keys.push(&pair.queue_key);
            keys.push(&pair.mmr_key);
        }
        keys
    }
}

impl MatchCommitPlan {
    fn redis_keys(&self) -> Vec<&str> {
        let mut keys = vec![
            self.active_matches_key.as_str(),
            self.outbox_key.as_str(),
            self.outbox_age_key.as_str(),
        ];
        for lobby in &self.lobbies {
            keys.push(&lobby.active_game_key);
            keys.push(&lobby.metadata_key);
            keys.push(&lobby.queue_identity_key);
            keys.push(&lobby.queue_lease_key);
            keys.push(&lobby.queue_outcome_key);
            for pair in &lobby.queue_pairs {
                keys.push(&pair.queue_key);
                keys.push(&pair.mmr_key);
            }
        }
        for user in &self.users {
            keys.push(&user.active_game_key);
            keys.push(&user.pending_game_key);
            keys.push(&user.queue_status_key);
            keys.push(&user.queue_identity_key);
        }
        keys.extend(
            self.notifications
                .iter()
                .map(|notification| notification.channel.as_str()),
        );
        keys
    }
}

/// Redis-based matchmaking manager
#[derive(Clone)]
pub struct MatchmakingManager {
    redis: RedisConnection,
    max_retries: u32,
    retry_delay: Duration,
    boost_config: BoostConfig,
    player_idle_config: PlayerIdleConfig,
    match_ready_window_ms: i64,
    enforce_queue_liveness: bool,
}

impl MatchmakingManager {
    /// Create a new Redis matchmaking manager
    pub fn new(redis: impl Into<RedisConnection>) -> Result<Self> {
        Self::new_with_gameplay_config(redis, BoostConfig::default(), PlayerIdleConfig::default())
    }

    /// Create a matchmaking manager with the Boost balance resolved at server
    /// startup. Eligible matches clone this value into their immutable game
    /// properties and never re-read process configuration while active.
    pub fn new_with_boost_config(
        redis: impl Into<RedisConnection>,
        boost_config: BoostConfig,
    ) -> Result<Self> {
        Self::new_with_gameplay_config(redis, boost_config, PlayerIdleConfig::default())
    }

    /// Create a matchmaking manager with every server-owned gameplay policy
    /// resolved at startup. New matches snapshot these values and active games
    /// never re-read process configuration.
    pub fn new_with_gameplay_config(
        redis: impl Into<RedisConnection>,
        boost_config: BoostConfig,
        player_idle_config: PlayerIdleConfig,
    ) -> Result<Self> {
        Self::new_with_gameplay_config_and_ready_window(
            redis,
            boost_config,
            player_idle_config,
            MATCH_READY_WINDOW_MS,
        )
    }

    /// Create a matchmaking manager with an explicitly resolved pre-match
    /// readiness window. Production supplies [`MATCH_READY_WINDOW_MS`]; tests
    /// may use a shorter window while exercising the same durable deadline and
    /// executor transition.
    pub fn new_with_gameplay_config_and_ready_window(
        redis: impl Into<RedisConnection>,
        boost_config: BoostConfig,
        player_idle_config: PlayerIdleConfig,
        match_ready_window_ms: i64,
    ) -> Result<Self> {
        boost_config
            .validate()
            .context("Invalid matchmaking Boost configuration")?;
        if match_ready_window_ms <= 0 {
            return Err(anyhow!("match readiness window must be positive"));
        }
        let enforce_queue_liveness = match std::env::var(MATCHMAKING_QUEUE_LEASE_ENFORCEMENT_ENV) {
            Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => true,
                "false" | "0" | "no" | "off" => false,
                _ => {
                    return Err(anyhow!(
                        "{MATCHMAKING_QUEUE_LEASE_ENFORCEMENT_ENV} must be true or false"
                    ));
                }
            },
            Err(std::env::VarError::NotPresent) => true,
            Err(error) => {
                return Err(anyhow!(
                    "Failed to read {MATCHMAKING_QUEUE_LEASE_ENFORCEMENT_ENV}: {error}"
                ));
            }
        };
        Ok(Self {
            redis: redis.into(),
            max_retries: 3,
            retry_delay: Duration::from_millis(500),
            boost_config,
            player_idle_config,
            match_ready_window_ms,
            enforce_queue_liveness,
        })
    }

    pub(crate) fn boost_config(&self) -> &BoostConfig {
        &self.boost_config
    }

    pub(crate) fn player_idle_config(&self) -> PlayerIdleConfig {
        self.player_idle_config
    }

    pub(crate) fn match_ready_window_ms(&self) -> i64 {
        self.match_ready_window_ms
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
        self.add_lobby_to_queue_in_pool(
            lobby_code,
            members,
            avg_mmr,
            game_types,
            queue_mode,
            requesting_user_id,
            MatchmakingPool::Public,
            None,
        )
        .await
    }

    /// Add a lobby only to the physical queue family for its attested pool.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_lobby_to_queue_in_pool(
        &mut self,
        lobby_code: &str,
        members: Vec<crate::lobby_manager::LobbyMember>,
        avg_mmr: i32,
        game_types: Vec<GameType>,
        queue_mode: common::QueueMode,
        requesting_user_id: u32,
        matchmaking_pool: MatchmakingPool,
        expected_ad_break_id: Option<&str>,
    ) -> Result<()> {
        let revision: Option<i64> = self
            .redis
            .hget(RedisKeys::lobby_metadata(lobby_code), "membershipRevision")
            .await
            .context("Failed to read lobby membership revision")?;
        let fence = LobbyMembershipFence {
            revision: revision.unwrap_or(0),
            valid_until_ms: lobby_membership_valid_until_ms(&members)?,
        };
        self.add_lobby_to_queue_in_pool_with_membership_fence(
            lobby_code,
            members,
            avg_mmr,
            game_types,
            queue_mode,
            requesting_user_id,
            matchmaking_pool,
            expected_ad_break_id,
            fence,
        )
        .await
    }

    /// Admit a lobby using a revision captured before the member-slot
    /// snapshot. Production callers use this entry point so Redis can reject
    /// any join/leave that overlapped roster construction.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_lobby_to_queue_in_pool_with_membership_fence(
        &mut self,
        lobby_code: &str,
        members: Vec<crate::lobby_manager::LobbyMember>,
        avg_mmr: i32,
        game_types: Vec<GameType>,
        queue_mode: common::QueueMode,
        requesting_user_id: u32,
        matchmaking_pool: MatchmakingPool,
        expected_ad_break_id: Option<&str>,
        membership_fence: LobbyMembershipFence,
    ) -> Result<()> {
        if members.is_empty() || members.len() > MAX_LOBBY_MEMBERS {
            return Err(anyhow!(
                "Lobby admission requires 1 to {MAX_LOBBY_MEMBERS} members"
            ));
        }
        if game_types.is_empty() {
            return Err(anyhow!("Must specify at least one game type"));
        }
        if game_types.len() > MATCHMAKING_GAME_TYPES.len()
            || game_types
                .iter()
                .any(|game_type| !MATCHMAKING_GAME_TYPES.contains(game_type))
            || game_types
                .iter()
                .enumerate()
                .any(|(index, game_type)| game_types[..index].contains(game_type))
        {
            return Err(anyhow!(
                "Game types must be unique supported matchmaking queue families"
            ));
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
            matchmaking_pool,
            queue_identity_json: None,
        };

        let lobby_json = serde_json::to_string(&lobby)?;
        let queue_claim = lobby_queue_claim(&lobby);
        let mut member_user_ids: Vec<u32> =
            lobby.members.iter().map(|member| member.user_id).collect();
        member_user_ids.sort_unstable();
        member_user_ids.dedup();

        let plan = LobbyAdmissionPlan {
            lobby_active_game_key: RedisKeys::matchmaking_lobby_active_game(lobby_code),
            lobby_metadata_key: RedisKeys::lobby_metadata(lobby_code),
            membership_reservations_key: RedisKeys::lobby_membership_reservations(lobby_code),
            queue_identity_key: RedisKeys::matchmaking_lobby_queue_identity(lobby_code),
            queue_lease_key: RedisKeys::matchmaking_lobby_queue_lease(lobby_code),
            queue_outcome_key: RedisKeys::matchmaking_lobby_queue_outcome(lobby_code),
            queue_token: lobby.queue_token.clone(),
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
            member_user_ids,
            member_json: lobby_json,
            queued_at: timestamp,
            avg_mmr,
            matchmaking_pool: matchmaking_pool.to_string(),
            expected_ad_break_id: expected_ad_break_id.map(str::to_owned),
            expected_membership_revision: membership_fence.revision,
            membership_valid_until_ms: membership_fence.valid_until_ms,
            metadata_idle_ttl_ms: crate::lobby_manager::LOBBY_METADATA_IDLE_TTL_MS,
            queue_lease_ttl_ms: LOBBY_QUEUE_LEASE_TTL_MS,
            queue_outcome_ttl_ms: LOBBY_QUEUE_OUTCOME_TTL_MS,
            queue_pairs: game_types
                .iter()
                .map(|game_type| MatchCommitQueuePair {
                    queue_key: RedisKeys::matchmaking_lobby_queue_for_pool(
                        game_type,
                        &queue_mode,
                        matchmaking_pool,
                    ),
                    mmr_key: RedisKeys::matchmaking_lobby_mmr_index_for_pool(
                        game_type,
                        &queue_mode,
                        matchmaking_pool,
                    ),
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
            let mut invocation = script.prepare_invoke();
            for key in plan.redis_keys() {
                invocation.key(key);
            }
            match invocation
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
                    crate::resilience_metrics::record_matchmaking_error(1);
                    return Err(anyhow!("Failed to add lobby to queue: {}", e));
                }
            }
        };
        let outcome = classify_atomic_matchmaking_outcome(code);
        crate::analytics::sink::record_queue_entered(&lobby, admission_result_label(outcome));
        match outcome {
            Some(AtomicMatchmakingOutcome::Applied) => {
                crate::resilience_metrics::record_matchmaking_admission(1);
                info!(
                    "Added lobby {} to {} matchmaking queue for {:?} with {} members and avg MMR {}",
                    lobby_code,
                    matchmaking_pool,
                    game_types,
                    lobby.members.len(),
                    avg_mmr
                );
            }
            Some(AtomicMatchmakingOutcome::Idempotent) => {
                crate::resilience_metrics::record_matchmaking_admission_deduplication(1);
                info!(
                    lobby_code,
                    "Lobby already had an admitted queue identity; kept the first request"
                );
            }
            Some(AtomicMatchmakingOutcome::ExpectedConflict) => {
                crate::resilience_metrics::record_matchmaking_admission_rejection(1);
                return Err(LobbyAdmissionRejected { detail }.into());
            }
            Some(AtomicMatchmakingOutcome::IntegrityError) => {
                crate::resilience_metrics::record_matchmaking_error(1);
                crate::resilience_metrics::record_matchmaking_integrity_error(1);
                return Err(anyhow!("Lobby admission integrity failure: {detail}"));
            }
            None => {
                crate::resilience_metrics::record_matchmaking_error(1);
                crate::resilience_metrics::record_matchmaking_integrity_error(1);
                return Err(anyhow!(
                    "Lobby admission returned unknown status {code}: {detail}"
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
        self.get_queued_lobbies_in_pool(game_type, queue_mode, MatchmakingPool::Public)
            .await
    }

    pub async fn get_queued_lobbies_in_pool(
        &mut self,
        game_type: &GameType,
        queue_mode: &common::QueueMode,
        matchmaking_pool: MatchmakingPool,
    ) -> Result<Vec<QueuedLobby>> {
        let lobby_queue_key =
            RedisKeys::matchmaking_lobby_queue_for_pool(game_type, queue_mode, matchmaking_pool);
        let lobby_mmr_key = RedisKeys::matchmaking_lobby_mmr_index_for_pool(
            game_type,
            queue_mode,
            matchmaking_pool,
        );

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

        // Keep the exact serialized ZSET member alongside the parsed value.
        // Reaping and queue commits are fenced by that immutable identity, so
        // reserializing a forward-compatible record must never change which
        // generation we operate on.
        let mut seen_member_json = HashSet::new();
        let mut candidates = Vec::new();
        let mut mismatched_lobby = None;

        // Helper to process lobby JSON and add each exact identity once.
        let mut process_lobby = |member_json: &str| {
            if let Ok(mut lobby) = serde_json::from_str::<QueuedLobby>(member_json) {
                lobby.queue_identity_json = Some(member_json.to_owned());
                if lobby.matchmaking_pool != matchmaking_pool {
                    mismatched_lobby.get_or_insert(lobby.lobby_code);
                } else if seen_member_json.insert(member_json.to_owned()) {
                    candidates.push((lobby, member_json.to_owned()));
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

        if let Some(lobby_code) = mismatched_lobby {
            crate::resilience_metrics::record_matchmaking_integrity_error(1);
            return Err(anyhow!(
                "Lobby {} was stored in the wrong matchmaking pool queue",
                lobby_code
            ));
        }

        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        // A durable queue identity is intentionally reconnectable, but only
        // while at least one admitted member continues refreshing this exact
        // generation's lease. During upgrade phase 1, only records without
        // the new durable `admitted` marker are grandfathered; new-schema
        // records remain fully enforced and cannot starve the queue. MGET is
        // cluster-safe because every key shares the matchmaking hash slot.
        let mut liveness_keys = Vec::with_capacity(candidates.len() * 3);
        for (lobby, _) in &candidates {
            liveness_keys.push(RedisKeys::matchmaking_lobby_queue_identity(
                &lobby.lobby_code,
            ));
            liveness_keys.push(RedisKeys::matchmaking_lobby_queue_lease(&lobby.lobby_code));
            liveness_keys.push(RedisKeys::matchmaking_lobby_queue_outcome(
                &lobby.lobby_code,
            ));
        }
        let liveness: Vec<Option<String>> = redis::cmd("MGET")
            .arg(&liveness_keys)
            .query_async(&mut self.redis)
            .await
            .context("Failed to read lobby queue identity and liveness snapshot")?;
        if liveness.len() != candidates.len() * 3 {
            return Err(anyhow!(
                "Redis returned an incomplete queue liveness snapshot"
            ));
        }

        let mut live_lobby_codes = HashSet::new();
        let mut live_lobbies = Vec::new();
        let mut stale_lobbies = Vec::new();
        for (index, (lobby, member_json)) in candidates.into_iter().enumerate() {
            let identity = &liveness[index * 3];
            let lease = &liveness[index * 3 + 1];
            let outcome = &liveness[index * 3 + 2];
            let exact_identity = identity.as_deref() == Some(member_json.as_str());
            let exact_lease = lease.as_deref() == Some(member_json.as_str());
            let current_outcome_status = outcome.as_deref().and_then(|outcome| {
                outcome
                    .split_once(':')
                    .filter(|(_, token)| *token == lobby.queue_token)
                    .map(|(status, _)| status)
            });
            let same_operation_terminal =
                current_outcome_status.is_some_and(|status| status != "admitted");
            let current_schema = current_outcome_status == Some("admitted");
            let legacy_or_stale_marker = current_outcome_status.is_none();
            let liveness_satisfied =
                exact_lease || (!self.enforce_queue_liveness && legacy_or_stale_marker);
            if exact_identity
                && !same_operation_terminal
                && (current_schema || legacy_or_stale_marker)
                && liveness_satisfied
            {
                if live_lobby_codes.insert(lobby.lobby_code.clone()) {
                    live_lobbies.push(lobby);
                }
            } else {
                stale_lobbies.push((lobby, member_json));
            }
        }

        // Filtering happens before matching, so an expired generation can
        // never create a ghost game. Cleanup is deliberately bounded: each
        // matchmaking pass makes progress without allowing a corrupted or
        // attacker-inflated queue to monopolize the loop.
        const MAX_STALE_REAPS_PER_SAMPLE: usize = 16;
        for (lobby, member_json) in stale_lobbies.into_iter().take(MAX_STALE_REAPS_PER_SAMPLE) {
            if let Err(error) = self.reap_stale_lobby_identity(&lobby, &member_json).await {
                warn!(
                    lobby_code = lobby.lobby_code,
                    %error,
                    "Failed to reap an expired lobby queue generation"
                );
            }
        }

        Ok(live_lobbies)
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
                let mut lobby: QueuedLobby =
                    serde_json::from_str(&member_json).with_context(|| {
                        format!("Malformed lobby queue identity at Redis key {identity_key}")
                    })?;
                if lobby.lobby_code != lobby_code {
                    return Err(anyhow!(
                        "Lobby queue identity {} belongs to {}",
                        identity_key,
                        lobby.lobby_code
                    ));
                }
                lobby.queue_identity_json = Some(member_json);
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
            queue_lease_key: RedisKeys::matchmaking_lobby_queue_lease(lobby_code),
            queue_outcome_key: None,
            queue_token: None,
            member_json: String::new(),
            metadata_idle_ttl_ms: crate::lobby_manager::LOBBY_METADATA_IDLE_TTL_MS,
            queue_outcome_ttl_ms: LOBBY_QUEUE_OUTCOME_TTL_MS,
            requesting_user_id: None,
            user_queue_claims: Vec::new(),
            queue_pairs: Vec::new(),
        })
        .await
    }

    /// User-initiated cancellation must be scoped to a queue identity that
    /// actually contains that user. Exact identity comparison in the removal
    /// script prevents this authorization snapshot from removing a newer
    /// generation queued concurrently without them.
    pub async fn remove_lobby_from_all_queues_by_code_for_user(
        &mut self,
        lobby_code: &str,
        user_id: u32,
    ) -> Result<bool> {
        if let Some(lobby) = self.get_queued_lobby_by_code(lobby_code).await? {
            if !lobby.members.iter().any(|member| member.user_id == user_id) {
                return Err(anyhow!(
                    "User is not a member of the current lobby queue identity"
                ));
            }
            return self
                .remove_exact_lobby_identity_for_user(&lobby, Some(user_id))
                .await;
        }

        self.execute_lobby_removal(LobbyRemovalPlan {
            lobby_active_game_key: RedisKeys::matchmaking_lobby_active_game(lobby_code),
            lobby_metadata_key: RedisKeys::lobby_metadata(lobby_code),
            queue_identity_key: RedisKeys::matchmaking_lobby_queue_identity(lobby_code),
            queue_lease_key: RedisKeys::matchmaking_lobby_queue_lease(lobby_code),
            queue_outcome_key: None,
            queue_token: None,
            member_json: String::new(),
            metadata_idle_ttl_ms: crate::lobby_manager::LOBBY_METADATA_IDLE_TTL_MS,
            queue_outcome_ttl_ms: LOBBY_QUEUE_OUTCOME_TTL_MS,
            requesting_user_id: Some(user_id),
            user_queue_claims: Vec::new(),
            queue_pairs: Vec::new(),
        })
        .await
    }

    /// Recover queue cancellation after a gateway restart, when the socket no
    /// longer has an in-memory LobbyJoinHandle. The per-user durable claim is
    /// only authority to remove the exact immutable lobby identity it names.
    pub async fn remove_lobby_from_queue_for_user_claim(
        &mut self,
        user_id: u32,
    ) -> Result<Option<String>> {
        let claim_key = RedisKeys::matchmaking_user_queue_identity(user_id);
        let Some(claim): Option<String> = self
            .redis
            .get(&claim_key)
            .await
            .context("Failed to read durable user lobby queue claim")?
        else {
            return Ok(None);
        };
        let (lobby_code, queue_token) = claim
            .rsplit_once(':')
            .ok_or_else(|| anyhow!("Malformed durable user lobby queue claim"))?;
        uuid::Uuid::parse_str(queue_token)
            .context("Malformed queue token in durable user lobby queue claim")?;
        let Some(lobby) = self.get_queued_lobby_by_code(lobby_code).await? else {
            return Err(anyhow!(
                "Durable user queue claim references a missing lobby identity"
            ));
        };
        if lobby.queue_token != queue_token
            || lobby_queue_claim(&lobby) != claim
            || !lobby.members.iter().any(|member| member.user_id == user_id)
        {
            return Err(anyhow!(
                "Durable user queue claim does not authorize the current lobby identity"
            ));
        }
        if self
            .remove_exact_lobby_identity_for_user(&lobby, Some(user_id))
            .await?
        {
            Ok(Some(lobby.lobby_code))
        } else {
            Ok(None)
        }
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
        self.remove_exact_lobby_identity_for_user(lobby, None).await
    }

    async fn remove_exact_lobby_identity_for_user(
        &mut self,
        lobby: &QueuedLobby,
        requesting_user_id: Option<u32>,
    ) -> Result<bool> {
        let member_json = lobby.exact_queue_identity_json()?;
        self.remove_exact_lobby_identity_json_for_user(lobby, member_json, requesting_user_id)
            .await
    }

    async fn remove_exact_lobby_identity_json_for_user(
        &mut self,
        lobby: &QueuedLobby,
        member_json: String,
        requesting_user_id: Option<u32>,
    ) -> Result<bool> {
        let queue_claim = lobby_queue_claim(lobby);
        let plan = LobbyRemovalPlan {
            lobby_active_game_key: RedisKeys::matchmaking_lobby_active_game(&lobby.lobby_code),
            lobby_metadata_key: RedisKeys::lobby_metadata(&lobby.lobby_code),
            queue_identity_key: RedisKeys::matchmaking_lobby_queue_identity(&lobby.lobby_code),
            queue_lease_key: RedisKeys::matchmaking_lobby_queue_lease(&lobby.lobby_code),
            queue_outcome_key: Some(RedisKeys::matchmaking_lobby_queue_outcome(
                &lobby.lobby_code,
            )),
            queue_token: Some(lobby.queue_token.clone()),
            member_json,
            metadata_idle_ttl_ms: crate::lobby_manager::LOBBY_METADATA_IDLE_TTL_MS,
            queue_outcome_ttl_ms: LOBBY_QUEUE_OUTCOME_TTL_MS,
            requesting_user_id,
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
                    queue_key: RedisKeys::matchmaking_lobby_queue_for_pool(
                        game_type,
                        &lobby.queue_mode,
                        lobby.matchmaking_pool,
                    ),
                    mmr_key: RedisKeys::matchmaking_lobby_mmr_index_for_pool(
                        game_type,
                        &lobby.queue_mode,
                        lobby.matchmaking_pool,
                    ),
                })
                .collect(),
        };
        self.execute_lobby_removal(plan).await
    }

    /// Remove an expired exact queue generation without racing a member
    /// heartbeat that renewed the lease after the sampling snapshot. If the
    /// lobby code now points at another generation, only the stale exact ZSET
    /// members are pruned; newer identity, claims, and metadata are untouched.
    async fn reap_stale_lobby_identity(
        &mut self,
        lobby: &QueuedLobby,
        member_json: &str,
    ) -> Result<bool> {
        let queue_claim = lobby_queue_claim(lobby);
        let plan = LobbyRemovalPlan {
            lobby_active_game_key: RedisKeys::matchmaking_lobby_active_game(&lobby.lobby_code),
            lobby_metadata_key: RedisKeys::lobby_metadata(&lobby.lobby_code),
            queue_identity_key: RedisKeys::matchmaking_lobby_queue_identity(&lobby.lobby_code),
            queue_lease_key: RedisKeys::matchmaking_lobby_queue_lease(&lobby.lobby_code),
            queue_outcome_key: Some(RedisKeys::matchmaking_lobby_queue_outcome(
                &lobby.lobby_code,
            )),
            queue_token: Some(lobby.queue_token.clone()),
            member_json: member_json.to_owned(),
            metadata_idle_ttl_ms: crate::lobby_manager::LOBBY_METADATA_IDLE_TTL_MS,
            queue_outcome_ttl_ms: LOBBY_QUEUE_OUTCOME_TTL_MS,
            requesting_user_id: None,
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
                    queue_key: RedisKeys::matchmaking_lobby_queue_for_pool(
                        game_type,
                        &lobby.queue_mode,
                        lobby.matchmaking_pool,
                    ),
                    mmr_key: RedisKeys::matchmaking_lobby_mmr_index_for_pool(
                        game_type,
                        &lobby.queue_mode,
                        lobby.matchmaking_pool,
                    ),
                })
                .collect(),
        };
        let plan_json = serde_json::to_string(&plan)?;
        let script = redis::Script::new(REAP_STALE_LOBBY_SCRIPT);
        let mut invocation = script.prepare_invoke();
        for key in plan.redis_keys() {
            invocation.key(key);
        }
        let (code, detail): (i64, String) = invocation
            .arg(plan_json)
            .invoke_async(&mut self.redis)
            .await
            .context("Failed to atomically reap stale lobby queue identity")?;
        match code {
            1 => Ok(true),
            2 => Ok(false),
            0 => Err(anyhow!("Lobby queue reaping was rejected: {detail}")),
            other => Err(anyhow!(
                "Lobby queue reaping returned unknown status {other}: {detail}"
            )),
        }
    }

    async fn execute_lobby_removal(&mut self, plan: LobbyRemovalPlan) -> Result<bool> {
        let plan_json = serde_json::to_string(&plan)?;
        let script = redis::Script::new(REMOVE_LOBBY_SCRIPT);
        let mut invocation = script.prepare_invoke();
        for key in plan.redis_keys() {
            invocation.key(key);
        }
        let (code, detail): (i64, String) = invocation
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

        let matchmaking_pool = match_info.matchmaking_pool;

        let mut lobby_codes = HashSet::new();
        let mut user_ids = HashSet::new();
        let mut commit_lobbies = Vec::with_capacity(lobbies.len());
        let mut commit_users = Vec::new();

        for lobby in lobbies {
            if lobby.matchmaking_pool != matchmaking_pool {
                return Err(anyhow!(
                    "Lobby {} belongs to {} pool but match belongs to {} pool",
                    lobby.lobby_code,
                    lobby.matchmaking_pool,
                    matchmaking_pool
                ));
            }
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

            let member_json = lobby.exact_queue_identity_json()?;
            let queue_pairs = lobby
                .game_types
                .iter()
                .map(|game_type| MatchCommitQueuePair {
                    queue_key: RedisKeys::matchmaking_lobby_queue_for_pool(
                        game_type,
                        &lobby.queue_mode,
                        lobby.matchmaking_pool,
                    ),
                    mmr_key: RedisKeys::matchmaking_lobby_mmr_index_for_pool(
                        game_type,
                        &lobby.queue_mode,
                        lobby.matchmaking_pool,
                    ),
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
                    pending_game_key: RedisKeys::matchmaking_lobby_user_pending_game(
                        &lobby.lobby_code,
                        member.user_id,
                    ),
                    queue_status_key: RedisKeys::matchmaking_user_status(member.user_id),
                    queue_identity_key: RedisKeys::matchmaking_user_queue_identity(member.user_id),
                    queue_identity_value: queue_identity_value.clone(),
                });
            }

            commit_lobbies.push(MatchCommitLobby {
                lobby_code: lobby.lobby_code.clone(),
                queue_token: lobby.queue_token.clone(),
                member_json,
                active_game_key: RedisKeys::matchmaking_lobby_active_game(&lobby.lobby_code),
                metadata_key: RedisKeys::lobby_metadata(&lobby.lobby_code),
                queue_identity_key: RedisKeys::matchmaking_lobby_queue_identity(&lobby.lobby_code),
                queue_lease_key: RedisKeys::matchmaking_lobby_queue_lease(&lobby.lobby_code),
                queue_outcome_key: RedisKeys::matchmaking_lobby_queue_outcome(&lobby.lobby_code),
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
        let outbox_record = GameCreatedOutboxRecord {
            schema_version: GAME_CREATED_OUTBOX_SCHEMA_VERSION,
            game_id,
            partition_id,
            game_created_payload: game_created_payload.to_string(),
        };
        outbox_record.validate()?;
        let plan = MatchCommitPlan {
            active_matches_key: RedisKeys::matchmaking_active_matches(),
            outbox_key: RedisKeys::matchmaking_game_created_outbox(),
            outbox_age_key: RedisKeys::matchmaking_game_created_outbox_age(),
            outbox_payload: serde_json::to_string(&outbox_record)?,
            created_at_ms: match_info.created_at,
            game_id: game_id.to_string(),
            pending_game_ttl_ms: LOBBY_MATCH_HANDOFF_TTL_MS,
            queue_outcome_ttl_ms: LOBBY_QUEUE_OUTCOME_TTL_MS,
            enforce_queue_liveness: self.enforce_queue_liveness,
            active_match_json: active_match_json.clone(),
            matchmaking_pool: matchmaking_pool.to_string(),
            lobbies: commit_lobbies,
            users: commit_users,
            notifications,
        };
        let plan_json = serde_json::to_string(&plan)?;
        let matched_players = match_info.players.len();
        let matched_lobbies = lobbies.len();
        let committed_at_ms = Utc::now().timestamp_millis();
        let wait_ms = lobbies
            .iter()
            .map(|lobby| committed_at_ms.saturating_sub(lobby.queued_at).max(0) as u64)
            .max()
            .unwrap_or(0);
        let script = redis::Script::new(COMMIT_MATCH_SCRIPT);
        let mut attempts = 0;
        let mut delay = self.retry_delay;
        let (code, detail) = loop {
            attempts += 1;
            let mut invocation = script.prepare_invoke();
            for key in plan.redis_keys() {
                invocation.key(key);
            }
            match invocation
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
                    crate::resilience_metrics::record_matchmaking_error(1);
                    return Err(error).context("Failed to atomically commit matchmaking claim");
                }
            }
        };

        match classify_atomic_matchmaking_outcome(code) {
            Some(AtomicMatchmakingOutcome::Applied) => {
                crate::resilience_metrics::record_matchmaking_commit(
                    wait_ms,
                    matched_players,
                    matched_lobbies,
                );
                crate::analytics::sink::record_match_committed(
                    game_id,
                    wait_ms as i64,
                    matched_players,
                    matchmaking_pool,
                );
                Ok(MatchCommitOutcome::Committed { outbox_id: detail })
            }
            Some(AtomicMatchmakingOutcome::Idempotent) => Ok(MatchCommitOutcome::AlreadyCommitted),
            Some(AtomicMatchmakingOutcome::ExpectedConflict) => {
                crate::resilience_metrics::record_match_claim_conflicts(1);
                Ok(MatchCommitOutcome::Conflict { reason: detail })
            }
            Some(AtomicMatchmakingOutcome::IntegrityError) => {
                crate::resilience_metrics::record_matchmaking_error(1);
                crate::resilience_metrics::record_matchmaking_integrity_error(1);
                Err(anyhow!("Atomic matchmaking integrity failure: {detail}"))
            }
            None => {
                crate::resilience_metrics::record_matchmaking_error(1);
                crate::resilience_metrics::record_matchmaking_integrity_error(1);
                Err(anyhow!(
                    "Atomic matchmaking script returned unknown status {} ({})",
                    code,
                    detail
                ))
            }
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

    pub async fn scan_game_created_outbox(
        &mut self,
        cursor: u64,
    ) -> Result<(u64, Vec<(String, String)>)> {
        redis::cmd("HSCAN")
            .arg(RedisKeys::matchmaking_game_created_outbox())
            .arg(cursor)
            .arg("COUNT")
            .arg(100_u32)
            .query_async(&mut self.redis)
            .await
            .context("failed to scan game-created outbox")
    }

    pub async fn acknowledge_game_created_outbox(
        &mut self,
        game_id: u32,
        expected_payload: &str,
    ) -> Result<bool> {
        let result: i32 = redis::Script::new(
            r#"
            local function key_type(key)
                local response = redis.call('TYPE', key)
                if type(response) == 'table' then return response['ok'] end
                return response
            end
            local outbox_type = key_type(KEYS[1])
            if outbox_type ~= 'none' and outbox_type ~= 'hash' then return -2 end
            local age_type = key_type(KEYS[2])
            if age_type ~= 'none' and age_type ~= 'zset' then return -3 end
            local current = redis.call('HGET', KEYS[1], ARGV[1])
            if not current then
                redis.call('ZREM', KEYS[2], ARGV[1])
                return 0
            end
            if current ~= ARGV[2] then return -1 end
            redis.call('HDEL', KEYS[1], ARGV[1])
            redis.call('ZREM', KEYS[2], ARGV[1])
            return 1
            "#,
        )
        .key(RedisKeys::matchmaking_game_created_outbox())
        .key(RedisKeys::matchmaking_game_created_outbox_age())
        .arg(game_id)
        .arg(expected_payload)
        .invoke_async(&mut self.redis)
        .await
        .context("failed to acknowledge game-created outbox record")?;
        match result {
            1 => Ok(true),
            0 => Ok(false),
            -1 => Err(anyhow!(
                "game-created outbox payload changed before acknowledgement"
            )),
            -2 => Err(anyhow!("game-created outbox has the wrong Redis type")),
            -3 => Err(anyhow!(
                "game-created outbox age index has the wrong Redis type"
            )),
            other => Err(anyhow!(
                "game-created outbox acknowledgement returned {other}"
            )),
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

    #[test]
    fn atomic_script_codes_separate_expected_conflicts_from_integrity_errors() {
        assert_eq!(
            classify_atomic_matchmaking_outcome(0),
            Some(AtomicMatchmakingOutcome::IntegrityError)
        );
        assert_eq!(
            classify_atomic_matchmaking_outcome(1),
            Some(AtomicMatchmakingOutcome::Applied)
        );
        assert_eq!(
            classify_atomic_matchmaking_outcome(2),
            Some(AtomicMatchmakingOutcome::Idempotent)
        );
        assert_eq!(
            classify_atomic_matchmaking_outcome(3),
            Some(AtomicMatchmakingOutcome::ExpectedConflict)
        );
        assert_eq!(classify_atomic_matchmaking_outcome(4), None);
    }

    #[test]
    fn legacy_queue_and_match_records_default_to_public() {
        let queued = QueuedLobby {
            lobby_code: "LEGACY".to_owned(),
            queue_token: "token".to_owned(),
            members: Vec::new(),
            avg_mmr: 1_000,
            game_types: vec![GameType::Solo],
            queue_mode: common::QueueMode::Quickmatch,
            queued_at: 1,
            requesting_user_id: 1,
            matchmaking_pool: MatchmakingPool::Stress,
            queue_identity_json: None,
        };
        let mut queued_json = serde_json::to_value(queued).unwrap();
        queued_json
            .as_object_mut()
            .unwrap()
            .remove("matchmaking_pool");
        let decoded_queue: QueuedLobby = serde_json::from_value(queued_json).unwrap();
        assert_eq!(decoded_queue.matchmaking_pool, MatchmakingPool::Public);

        let active = ActiveMatch {
            players: Vec::new(),
            spectators: Vec::new(),
            lobby_codes: vec!["LEGACY".to_owned()],
            game_type: GameType::Solo,
            status: MatchStatus::Waiting,
            partition_id: 0,
            created_at: 1,
            matchmaking_pool: MatchmakingPool::Stress,
        };
        let mut active_json = serde_json::to_value(active).unwrap();
        active_json
            .as_object_mut()
            .unwrap()
            .remove("matchmaking_pool");
        let decoded_match: ActiveMatch = serde_json::from_value(active_json).unwrap();
        assert_eq!(decoded_match.matchmaking_pool, MatchmakingPool::Public);
    }

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
