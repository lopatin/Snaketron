use crate::redis_utils::RedisConnection;
use anyhow::{Context, Result, anyhow};
use redis::{AsyncCommands, Script};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::{Duration, interval, sleep, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::ads::{AdBreakResolution, ClientDistribution, LobbyAdBreak};
use crate::db::{Database, models::LobbyMetadata};
use crate::lobby_manager::LobbyEvent::{LobbyDelete, LobbyUpdate};
use crate::matchmaking_manager::{
    LOBBY_QUEUE_LEASE_TTL_MS, LOBBY_QUEUE_OUTCOME_TTL_MS, QueuedLobby,
};
use crate::matchmaking_pool::MatchmakingPool;
use crate::pubsub_manager::PubSubManager;
use crate::redis_keys::RedisKeys;
use crate::user_cache::UserCache;

/// Lobby member information stored in Redis
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct LobbyMember {
    pub user_id: u32,
    pub username: String,
    pub ts: f64,
    /// Whether this member's authenticated client can resolve a v1 ad break.
    /// Missing values from older gateways deliberately fail closed.
    #[serde(default, skip_serializing)]
    #[cfg_attr(feature = "ts-gen", ts(skip))]
    pub supports_ad_break: bool,
    /// Whether this authenticated distribution has deployment capability for
    /// pre-match video. Live runtime policy selects the actual targets when a
    /// lobby requests matchmaking.
    #[serde(default, skip_serializing)]
    #[cfg_attr(feature = "ts-gen", ts(skip))]
    pub can_show_video_ad: bool,
    /// Authenticated session distribution used to apply live runtime policy.
    #[serde(default, skip_serializing)]
    #[cfg_attr(feature = "ts-gen", ts(skip))]
    pub distribution: Option<ClientDistribution>,
}

/// A struct that represents the value stored for a lobby member in Redis
#[derive(Debug, Clone)]
struct MemberValue {
    user_id: u32,
    websocket_id: String,
    supports_ad_break: bool,
    can_show_video_ad: bool,
    distribution: Option<ClientDistribution>,
}

impl std::fmt::Display for MemberValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}:{}:{}:{}",
            self.user_id,
            self.websocket_id,
            self.distribution.map_or("none", ClientDistribution::as_str),
            u8::from(self.can_show_video_ad),
            u8::from(self.supports_ad_break)
        )
    }
}

impl std::str::FromStr for MemberValue {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (user_id, remainder) = s
            .split_once(':')
            .ok_or(anyhow!("Invalid member value format"))?;

        let user_id: u32 = user_id
            .parse()
            .map_err(|_| anyhow!("Invalid user_id in member value"))?;

        let (websocket_and_capability, supports_ad_break) = match remainder.rsplit_once(':') {
            Some((websocket_id, "1")) => (websocket_id, true),
            Some((websocket_id, "0")) => (websocket_id, false),
            // Legacy values are `user_id:websocket_id` and therefore carry no
            // proof that the client understands the barrier protocol.
            _ => {
                return Ok(MemberValue {
                    user_id,
                    websocket_id: remainder.to_string(),
                    supports_ad_break: false,
                    can_show_video_ad: false,
                    distribution: None,
                });
            }
        };

        // The previous format ended after the barrier capability. Treat those
        // members as unable to show video until they refresh through a new
        // gateway; this makes rolling deploys fail open into matchmaking.
        let (websocket_and_distribution, can_show_video_ad) =
            match websocket_and_capability.rsplit_once(':') {
                Some((websocket_id, "1")) => (websocket_id, supports_ad_break),
                Some((websocket_id, "0")) => (websocket_id, false),
                _ => (websocket_and_capability, false),
            };

        // v9 values end after the video capability. v10+ adds the session
        // distribution while retaining the two capability bits at the end.
        let (websocket_id, distribution) = match websocket_and_distribution.rsplit_once(':') {
            Some((websocket_id, "web")) => (websocket_id, Some(ClientDistribution::Web)),
            Some((websocket_id, "crazygames")) => {
                (websocket_id, Some(ClientDistribution::CrazyGames))
            }
            Some((websocket_id, "itch")) => (websocket_id, Some(ClientDistribution::Itch)),
            Some((websocket_id, "none")) => (websocket_id, None),
            _ => (websocket_and_distribution, None),
        };

        // Keep `supports_ad_break` as the final field so older servers parsing
        // the extended representation still observe the correct capability.
        let can_show_video_ad = supports_ad_break && can_show_video_ad;

        Ok(MemberValue {
            user_id,
            websocket_id: websocket_id.to_string(),
            supports_ad_break,
            can_show_video_ad,
            distribution,
        })
    }
}

/// Lobby information stored in Redis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lobby {
    pub lobby_code: String,
    pub members: BTreeMap<u32, LobbyMember>,
    pub host_user_id: i32,
    pub state: String,
    pub preferences: LobbyPreferences,
    #[serde(default)]
    pub ad_break: Option<LobbyAdBreak>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum LobbyEvent {
    LobbyUpdate { lobby: Lobby },
    LobbyDelete { lobby_code: String, state: String },
}

impl Lobby {
    pub fn lobby_code(&self) -> &str {
        &self.lobby_code
    }
}

/// Handle for a lobby join that manages the heartbeat task
pub struct LobbyJoinHandle {
    heartbeat_task: JoinHandle<()>,
    scope_cancellation: CancellationToken,
    lobby_manager: Arc<LobbyManager>,
    returned: RwLock<bool>,
    pub rx: Receiver<Lobby>,
    pub lobby_code: String,
    pub user_id: i32,
    pub websocket_id: String,
}

/// Owns a tentative join heartbeat until every fallible admission await is
/// complete. Cancellation or panic drops this guard, stopping the task rather
/// than detaching an unfenced member refresher.
struct ProvisionalLobbyHeartbeat {
    task: Option<JoinHandle<()>>,
    cancellation: Option<CancellationToken>,
}

impl ProvisionalLobbyHeartbeat {
    fn stop(&mut self) {
        if let Some(cancellation) = self.cancellation.take() {
            cancellation.cancel();
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }

    fn into_parts(mut self) -> (JoinHandle<()>, CancellationToken) {
        let task = self.task.take().expect("provisional heartbeat task");
        let cancellation = self
            .cancellation
            .take()
            .expect("provisional heartbeat cancellation token");
        (task, cancellation)
    }
}

impl Drop for ProvisionalLobbyHeartbeat {
    fn drop(&mut self) {
        if let Some(cancellation) = self.cancellation.take() {
            cancellation.cancel();
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl LobbyJoinHandle {
    pub async fn close(&mut self) -> Result<LeaveLobbyResult> {
        let result = self
            .lobby_manager
            .leave_lobby(&self.lobby_code, self.user_id, &self.websocket_id)
            .await?;
        self.scope_cancellation.cancel();
        self.heartbeat_task.abort();
        self.return_to_manager();
        Ok(result)
    }

    /// Stop this transport's heartbeat without interpreting transport loss as
    /// an explicit user intent to leave. The Redis presence lease expires on
    /// its own unless a replacement socket refreshes a newer presence.
    pub fn detach_transport(mut self) {
        self.scope_cancellation.cancel();
        self.heartbeat_task.abort();
        self.return_to_manager();
    }

    fn return_to_manager(&mut self) {
        let mut returned = self.returned.write().unwrap();
        if !*returned {
            self.lobby_manager.return_handle(self);
            *returned = true;
        }
    }

    pub fn scope_cancellation_token(&self) -> CancellationToken {
        self.scope_cancellation.clone()
    }
}

impl Drop for LobbyJoinHandle {
    fn drop(&mut self) {
        self.scope_cancellation.cancel();
        self.heartbeat_task.abort();
        self.return_to_manager();
    }
}

pub struct AdBreakFinalizationLease {
    lobby_manager: Arc<LobbyManager>,
    claim_key: String,
    owner_token: String,
    renewal_task: Option<JoinHandle<()>>,
    released: bool,
}

impl AdBreakFinalizationLease {
    pub async fn release(mut self) {
        if let Some(task) = self.renewal_task.take() {
            task.abort();
        }
        match self
            .lobby_manager
            .release_ad_break_finalization(&self.claim_key, &self.owner_token)
            .await
        {
            Ok(()) => self.released = true,
            Err(error) => {
                warn!(%error, "Failed to release lobby ad-break finalization lease");
            }
        }
    }
}

impl Drop for AdBreakFinalizationLease {
    fn drop(&mut self) {
        if let Some(task) = self.renewal_task.take() {
            task.abort();
        }
        if !self.released
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let lobby_manager = self.lobby_manager.clone();
            let claim_key = self.claim_key.clone();
            let owner_token = self.owner_token.clone();
            runtime.spawn(async move {
                if let Err(error) = lobby_manager
                    .release_ad_break_finalization(&claim_key, &owner_token)
                    .await
                {
                    warn!(%error, "Failed to release dropped ad-break finalization lease");
                }
            });
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaveLobbyResult {
    StillActive,
    LobbyDeleted,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AdBreakResolutionResult {
    Pending(LobbyAdBreak),
    Ready(LobbyAdBreak),
    NotDue(LobbyAdBreak),
    NoChange(LobbyAdBreak),
    Stale,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BeginAdBreakResult {
    Active {
        ad_break: LobbyAdBreak,
        created: bool,
    },
    MembershipChanged,
}

const BEGIN_AD_BREAK_SCRIPT: &str = r#"
local metadata_type = redis.call('TYPE', KEYS[1])
if type(metadata_type) == 'table' then metadata_type = metadata_type['ok'] end
if metadata_type == 'none' then return {0, 'lobby-metadata-missing'} end
if metadata_type ~= 'hash' then return {0, 'lobby-metadata-wrong-type'} end

local state = redis.call('HGET', KEYS[1], 'state')
local existing = redis.call('HGET', KEYS[1], 'adBreak')
if state == 'ad_break' and existing then return {2, existing} end
if state ~= 'waiting' then return {3, state or 'missing'} end

-- Do not make a player watch an ad for an admission that is already known to
-- be impossible. All participant queue claims and active-game mappings share
-- this metadata slot, so the bounded roster preflight is atomic with BEGIN.
for index = 3, #KEYS do
    local key_type = redis.call('TYPE', KEYS[index])
    if type(key_type) == 'table' then key_type = key_type['ok'] end
    if key_type ~= 'none' and key_type ~= 'string' then
        return {0, 'participant-claim-wrong-type'}
    end
    if redis.call('GET', KEYS[index]) then
        return {7, 'participant-already-queued-or-matched'}
    end
end

-- Lobby membership is stored in a different Redis Cluster slot. A bounded
-- reservation ZSET in this metadata slot closes the snapshot/start race
-- without making admission scan attacker-controlled hash fields.
local redis_time = redis.call('TIME')
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local expired_reservations = redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now_ms)
if expired_reservations > 0 then
    redis.call('HINCRBY', KEYS[1], 'membershipRevision', 1)
    return {5, 'lobby-membership-reservation-expired'}
end
if redis.call('ZCARD', KEYS[2]) > 0 then
    return {4, 'lobby-membership-changing'}
end

local current_revision = tonumber(redis.call('HGET', KEYS[1], 'membershipRevision')) or 0
if current_revision ~= tonumber(ARGV[2]) then
    return {5, 'lobby-membership-changed'}
end
-- The membership ZSET lives in another cluster slot. Its snapshot is only
-- authoritative until the first captured presence lease expires. Refuse to
-- cross into ad_break at that exact boundary, matching ZREMRANGEBYSCORE's
-- inclusive expiry semantics.
if tonumber(ARGV[3]) <= now_ms then
    return {6, 'lobby-membership-lease-expired'}
end

local ad_break = cjson.decode(ARGV[1])
ad_break.expires_at_ms = now_ms + tonumber(ARGV[4])
local payload = cjson.encode(ad_break)
redis.call('HSET', KEYS[1], 'state', 'ad_break', 'adBreak', payload)
-- The barrier is durable beyond connected gateway tasks. Preserve metadata
-- through its Redis-authored deadline plus one member-lease recovery window;
-- normal heartbeats only extend and never shorten this TTL.
local desired_ttl_ms = tonumber(ARGV[4]) + tonumber(ARGV[5])
local current_ttl_ms = redis.call('PTTL', KEYS[1])
if current_ttl_ms >= 0 and current_ttl_ms < desired_ttl_ms then
    redis.call('PEXPIRE', KEYS[1], desired_ttl_ms)
end
return {1, payload}
"#;

const RESOLVE_AD_BREAK_SCRIPT: &str = r#"
local metadata_type = redis.call('TYPE', KEYS[1])
if type(metadata_type) == 'table' then metadata_type = metadata_type['ok'] end
if metadata_type ~= 'hash' then return {0, 'stale'} end
if redis.call('HGET', KEYS[1], 'state') ~= 'ad_break' then return {0, 'stale'} end

local raw = redis.call('HGET', KEYS[1], 'adBreak')
if not raw then return {0, 'stale'} end
local ad_break = cjson.decode(raw)
if ad_break.id ~= ARGV[1] then return {0, 'stale'} end
if not ad_break.resolutions then ad_break.resolutions = {} end

local redis_time = redis.call('TIME')
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local user_id = tonumber(ARGV[2])
local force_timeout = ARGV[4] == '1'
if force_timeout and now_ms < tonumber(ad_break.expires_at_ms) then
    return {3, raw}
end
if force_timeout or now_ms >= tonumber(ad_break.expires_at_ms) then
    local changed = false
    for _, participant in ipairs(ad_break.participant_user_ids) do
        local key = tostring(participant)
        if not ad_break.resolutions[key] then
            ad_break.resolutions[key] = 'timed_out'
            changed = true
        end
    end
    if not changed then return {4, raw} end
else
    local participant_found = false
    for _, participant in ipairs(ad_break.participant_user_ids) do
        if tonumber(participant) == user_id then
            participant_found = true
            break
        end
    end
    if not participant_found then return {0, 'stale'} end
    local resolution_key = tostring(user_id)
    if ad_break.resolutions[resolution_key] then return {4, raw} end
    ad_break.resolutions[resolution_key] = ARGV[3]
end

local resolved = true
for _, participant in ipairs(ad_break.participant_user_ids) do
    if not ad_break.resolutions[tostring(participant)] then
        resolved = false
        break
    end
end
local updated = cjson.encode(ad_break)
redis.call('HSET', KEYS[1], 'adBreak', updated)
if resolved then return {2, updated} end
return {1, updated}
"#;

const CANCEL_AD_BREAK_SCRIPT: &str = r#"
local raw = redis.call('HGET', KEYS[1], 'adBreak')
if not raw or redis.call('HGET', KEYS[1], 'state') ~= 'ad_break' then return 0 end
local ad_break = cjson.decode(raw)
if ad_break.id ~= ARGV[1] then return 0 end
redis.call('HSET', KEYS[1], 'state', 'waiting')
redis.call('HDEL', KEYS[1], 'adBreak')
return 1
"#;

const CANCEL_AD_BREAK_FOR_PARTICIPANT_SCRIPT: &str = r#"
local raw = redis.call('HGET', KEYS[1], 'adBreak')
if not raw or redis.call('HGET', KEYS[1], 'state') ~= 'ad_break' then return 0 end
local ad_break = cjson.decode(raw)
if ad_break.id ~= ARGV[1] then return 0 end
local user_id = tonumber(ARGV[2])
local participant_found = false
for _, participant in ipairs(ad_break.participant_user_ids) do
    if tonumber(participant) == user_id then
        participant_found = true
        break
    end
end
if not participant_found then return 0 end
redis.call('HSET', KEYS[1], 'state', 'waiting')
redis.call('HDEL', KEYS[1], 'adBreak')
return 1
"#;

const CLEAR_AD_BREAK_SCRIPT: &str = r#"
local raw = redis.call('HGET', KEYS[1], 'adBreak')
if not raw then return 0 end
-- A live break must only be removed through cancellation. Successful queue
-- admission changes the state before this compare-and-clear cleanup runs.
if redis.call('HGET', KEYS[1], 'state') == 'ad_break' then return 0 end
local ad_break = cjson.decode(raw)
if ad_break.id ~= ARGV[1] then return 0 end
redis.call('HDEL', KEYS[1], 'adBreak')
return 1
"#;

const CLAIM_AD_BREAK_FINALIZATION_SCRIPT: &str = r#"
if redis.call('HGET', KEYS[1], 'state') ~= 'ad_break' then return 0 end
local raw = redis.call('HGET', KEYS[1], 'adBreak')
if not raw then return 0 end
local ad_break = cjson.decode(raw)
if ad_break.id ~= ARGV[1] then return 0 end
local claimed = redis.call('SET', KEYS[2], ARGV[2], 'NX', 'PX', ARGV[3])
if claimed then return 1 end
return 0
"#;

const RENEW_AD_BREAK_FINALIZATION_SCRIPT: &str = r#"
if redis.call('GET', KEYS[2]) ~= ARGV[2] then return 0 end
local raw = redis.call('HGET', KEYS[1], 'adBreak')
if redis.call('HGET', KEYS[1], 'state') ~= 'ad_break' or not raw then
    redis.call('DEL', KEYS[2])
    return 0
end
local ad_break = cjson.decode(raw)
if ad_break.id ~= ARGV[1] then
    redis.call('DEL', KEYS[2])
    return 0
end
return redis.call('PEXPIRE', KEYS[2], ARGV[3])
"#;

const RELEASE_AD_BREAK_FINALIZATION_SCRIPT: &str = r#"
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])
"#;

const RESERVE_JOIN_MEMBERSHIP_SCRIPT: &str = r#"
local metadata_type = redis.call('TYPE', KEYS[1])
if type(metadata_type) == 'table' then metadata_type = metadata_type['ok'] end
if metadata_type == 'none' then return {0, 'lobby-metadata-missing'} end
if metadata_type ~= 'hash' then return {0, 'lobby-metadata-wrong-type'} end
local queue_identity_type = redis.call('TYPE', KEYS[3])
if type(queue_identity_type) == 'table' then queue_identity_type = queue_identity_type['ok'] end
if queue_identity_type ~= 'none' and queue_identity_type ~= 'string' then
    return {0, 'queue-identity-wrong-type'}
end
local queue_lease_type = redis.call('TYPE', KEYS[4])
if type(queue_lease_type) == 'table' then queue_lease_type = queue_lease_type['ok'] end
if queue_lease_type ~= 'none' and queue_lease_type ~= 'string' then
    return {0, 'queue-lease-wrong-type'}
end
local queue_outcome_type = redis.call('TYPE', KEYS[5])
if type(queue_outcome_type) == 'table' then queue_outcome_type = queue_outcome_type['ok'] end
if queue_outcome_type ~= 'none' and queue_outcome_type ~= 'string' then
    return {0, 'queue-outcome-wrong-type'}
end

local state = redis.call('HGET', KEYS[1], 'state')
local detail = 'reserved'
local queue_identity_to_renew = false
local queue_outcome_to_renew = false
if state == 'ad_break' then
    local raw = redis.call('HGET', KEYS[1], 'adBreak')
    if not raw then return {0, 'ad-break-metadata-missing'} end
    local ad_break = cjson.decode(raw)
    local user_id = tonumber(ARGV[3])
    for _, participant in ipairs(ad_break.participant_user_ids) do
        if tonumber(participant) == user_id then
            detail = 'participant-reconnect'
            break
        end
    end
    if detail ~= 'participant-reconnect' then return {0, 'ad-break-active'} end
elseif state == 'queued' then
    local raw_queue_identity = redis.call('GET', KEYS[3])
    if not raw_queue_identity then return {0, 'queue-identity-missing'} end
    local queued_lobby = cjson.decode(raw_queue_identity)
    local user_id = tonumber(ARGV[3])
    for _, member in ipairs(queued_lobby.members) do
        if tonumber(member.user_id) == user_id then
            detail = 'queued-reconnect'
            break
        end
    end
    if detail ~= 'queued-reconnect' then return {0, 'not-a-queued-member'} end
    queue_identity_to_renew = raw_queue_identity
    queue_outcome_to_renew = 'admitted:' .. queued_lobby.queue_token
    local existing_outcome = redis.call('GET', KEYS[5])
    if existing_outcome then
        local outcome_status, outcome_token = string.match(existing_outcome, '^([^:]+):(.+)$')
        if not outcome_status or not outcome_token then return {0, 'queue-outcome-malformed'} end
        if outcome_token == queued_lobby.queue_token and outcome_status ~= 'admitted' then
            return {0, 'queue-operation-terminal'}
        end
    end
elseif state ~= 'waiting' then
    return {0, state or 'missing'}
end

local redis_time = redis.call('TIME')
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local expired_reservations = redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now_ms)
if expired_reservations > 0 then
    redis.call('HINCRBY', KEYS[1], 'membershipRevision', 1)
end
if redis.call('ZCARD', KEYS[2]) >= tonumber(ARGV[4]) then
    return {0, 'too-many-concurrent-membership-changes'}
end
local reservation_expiry_ms = now_ms + tonumber(ARGV[2])
redis.call('HINCRBY', KEYS[1], 'membershipRevision', 1)
redis.call('ZADD', KEYS[2], reservation_expiry_ms, ARGV[1])
local desired_metadata_ttl_ms = tonumber(ARGV[2]) + 5000
local current_metadata_ttl_ms = redis.call('PTTL', KEYS[1])
if current_metadata_ttl_ms >= 0 and current_metadata_ttl_ms < desired_metadata_ttl_ms then
    redis.call('PEXPIRE', KEYS[1], desired_metadata_ttl_ms)
end
redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[2]) + 5000)
if queue_identity_to_renew then
    redis.call('SET', KEYS[4], queue_identity_to_renew, 'PX', ARGV[5])
    redis.call('SET', KEYS[5], queue_outcome_to_renew, 'PX', ARGV[6])
end
return {1, detail}
"#;

const RESERVE_LEAVE_MEMBERSHIP_SCRIPT: &str = r#"
local metadata_type = redis.call('TYPE', KEYS[1])
if type(metadata_type) == 'table' then metadata_type = metadata_type['ok'] end
if metadata_type == 'none' then return {2, 'lobby-metadata-missing'} end
if metadata_type ~= 'hash' then return {0, 'lobby-metadata-wrong-type'} end

local redis_time = redis.call('TIME')
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local expired_reservations = redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now_ms)
if expired_reservations > 0 then
    redis.call('HINCRBY', KEYS[1], 'membershipRevision', 1)
end

local state = redis.call('HGET', KEYS[1], 'state')
local cancelled = 'not-active'
if ARGV[5] == '1' and state ~= 'waiting' and state ~= 'ad_break' then
    return {0, 'lobby-' .. (state or 'missing') .. '; leave matchmaking first'}
end
if state == 'ad_break' then
    local raw = redis.call('HGET', KEYS[1], 'adBreak')
    if not raw then return {0, 'ad-break-metadata-missing'} end
    local ad_break = cjson.decode(raw)
    local user_id = tonumber(ARGV[4])
    local participant_found = false
    for _, participant in ipairs(ad_break.participant_user_ids) do
        if tonumber(participant) == user_id then
            participant_found = true
            break
        end
    end
    if ARGV[5] == '1' and not participant_found then
        return {0, 'not-an-ad-break-participant'}
    end
    -- A compensating removal can race with a newer, unrelated break after a
    -- failed join finalizer. Only tear down the barrier when the member being
    -- removed is actually part of that exact break's immutable roster.
    if participant_found then
        redis.call('HSET', KEYS[1], 'state', 'waiting')
        redis.call('HDEL', KEYS[1], 'adBreak')
        cancelled = 'cancelled'
        state = 'waiting'
    end
end
-- Explicit participant departure must fail open even under reservation
-- pressure. Capacity rejection may delay member removal, but it cannot leave
-- the lobby trapped in the barrier.
if redis.call('ZCARD', KEYS[2]) >= tonumber(ARGV[3]) then
    return {0, 'too-many-concurrent-membership-changes'}
end
local reservation_expiry_ms = now_ms + tonumber(ARGV[2])
redis.call('HINCRBY', KEYS[1], 'membershipRevision', 1)
redis.call('ZADD', KEYS[2], reservation_expiry_ms, ARGV[1])
local desired_metadata_ttl_ms = tonumber(ARGV[2]) + 5000
local current_metadata_ttl_ms = redis.call('PTTL', KEYS[1])
if current_metadata_ttl_ms >= 0 and current_metadata_ttl_ms < desired_metadata_ttl_ms then
    redis.call('PEXPIRE', KEYS[1], desired_metadata_ttl_ms)
end
redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[2]) + 5000)
return {1, cancelled}
"#;

const FINALIZE_JOIN_MEMBERSHIP_SCRIPT: &str = r#"
local cached_outcome = redis.call('GET', KEYS[3])
local metadata_type = redis.call('TYPE', KEYS[1])
if type(metadata_type) == 'table' then metadata_type = metadata_type['ok'] end
local reservations_type = redis.call('TYPE', KEYS[2])
if type(reservations_type) == 'table' then reservations_type = reservations_type['ok'] end

-- A successful outcome is written before its fence is removed. Redis Lua
-- errors do not roll back earlier writes, so an outcome replay also repairs a
-- reservation left behind by a mid-script failure.
if cached_outcome == '1' then
    if reservations_type == 'zset' and redis.call('ZSCORE', KEYS[2], ARGV[1]) then
        if metadata_type == 'hash' then
            redis.call('HINCRBY', KEYS[1], 'membershipRevision', 1)
        end
        redis.call('ZREM', KEYS[2], ARGV[1])
    end
    return 1
end
if cached_outcome == '0' then return 0 end
if metadata_type ~= 'hash' or (reservations_type ~= 'none' and reservations_type ~= 'zset') then
    redis.call('SET', KEYS[3], '0', 'PX', ARGV[3])
    return 0
end

local reservation_expiry = tonumber(redis.call('ZSCORE', KEYS[2], ARGV[1]))
local redis_time = redis.call('TIME')
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local state = redis.call('HGET', KEYS[1], 'state')
local valid_state = state == 'waiting'
if state == 'ad_break' then
    local raw = redis.call('HGET', KEYS[1], 'adBreak')
    if raw then
        local ad_break = cjson.decode(raw)
        local user_id = tonumber(ARGV[2])
        for _, participant in ipairs(ad_break.participant_user_ids) do
            if tonumber(participant) == user_id then
                valid_state = true
                break
            end
        end
    end
elseif state == 'queued' then
    local queue_identity_type = redis.call('TYPE', KEYS[4])
    if type(queue_identity_type) == 'table' then queue_identity_type = queue_identity_type['ok'] end
    if queue_identity_type == 'string' then
        local queued_lobby = cjson.decode(redis.call('GET', KEYS[4]))
        local user_id = tonumber(ARGV[2])
        for _, member in ipairs(queued_lobby.members) do
            if tonumber(member.user_id) == user_id then
                valid_state = true
                break
            end
        end
    end
end
if not reservation_expiry or reservation_expiry <= now_ms or not valid_state then
    -- A rejected or ambiguous join keeps its original write fence. The
    -- caller will either prove exact admission, or remove the exact member
    -- value and release this same reservation afterward.
    local compensation_expiry_ms = now_ms + tonumber(ARGV[4])
    redis.call('HINCRBY', KEYS[1], 'membershipRevision', 1)
    redis.call('ZADD', KEYS[2], compensation_expiry_ms, ARGV[1])
    local desired_metadata_ttl_ms = tonumber(ARGV[4]) + 5000
    local current_metadata_ttl_ms = redis.call('PTTL', KEYS[1])
    if current_metadata_ttl_ms >= 0 and current_metadata_ttl_ms < desired_metadata_ttl_ms then
        redis.call('PEXPIRE', KEYS[1], desired_metadata_ttl_ms)
    end
    redis.call('PEXPIRE', KEYS[2], desired_metadata_ttl_ms)
    redis.call('SET', KEYS[3], '0', 'PX', ARGV[3])
    return 0
end
redis.call('SET', KEYS[3], '1', 'PX', ARGV[3])
redis.call('HINCRBY', KEYS[1], 'membershipRevision', 1)
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
"#;

const RENEW_PROVISIONAL_JOIN_RESERVATION_SCRIPT: &str = r#"
local metadata_type = redis.call('TYPE', KEYS[1])
if type(metadata_type) == 'table' then metadata_type = metadata_type['ok'] end
local reservations_type = redis.call('TYPE', KEYS[2])
if type(reservations_type) == 'table' then reservations_type = reservations_type['ok'] end
if metadata_type ~= 'hash' or reservations_type ~= 'zset' then return 0 end
-- Finalizer writes its durable result before removing the reservation, so a
-- concurrent renewal can never recreate a completed join fence.
local outcome = redis.call('GET', KEYS[3])
if outcome == '1' then return 2 end
if outcome and outcome ~= '0' then return 0 end
if not redis.call('ZSCORE', KEYS[2], ARGV[1]) then return 0 end
local redis_time = redis.call('TIME')
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
redis.call('ZADD', KEYS[2], now_ms + tonumber(ARGV[2]), ARGV[1])
local desired_metadata_ttl_ms = tonumber(ARGV[2]) + 5000
local current_metadata_ttl_ms = redis.call('PTTL', KEYS[1])
if current_metadata_ttl_ms >= 0 and current_metadata_ttl_ms < desired_metadata_ttl_ms then
    redis.call('PEXPIRE', KEYS[1], desired_metadata_ttl_ms)
end
redis.call('PEXPIRE', KEYS[2], desired_metadata_ttl_ms)
return 1
"#;

const PREPARE_JOIN_COMPENSATION_SCRIPT: &str = r#"
local metadata_type = redis.call('TYPE', KEYS[1])
if type(metadata_type) == 'table' then metadata_type = metadata_type['ok'] end
local reservations_type = redis.call('TYPE', KEYS[2])
if type(reservations_type) == 'table' then reservations_type = reservations_type['ok'] end
if reservations_type ~= 'none' and reservations_type ~= 'zset' then
    return {0, 'membership-reservations-wrong-type'}
end

local cached_outcome = redis.call('GET', KEYS[3])
if cached_outcome == '1' then
    if redis.call('ZSCORE', KEYS[2], ARGV[1]) then
        if metadata_type == 'hash' then
            redis.call('HINCRBY', KEYS[1], 'membershipRevision', 1)
        end
        redis.call('ZREM', KEYS[2], ARGV[1])
    end
    return {2, 'committed'}
end

if metadata_type == 'none' then return {3, 'metadata-missing'} end
if metadata_type ~= 'hash' then return {0, 'lobby-metadata-wrong-type'} end

local redis_time = redis.call('TIME')
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local compensation_expiry_ms = now_ms + tonumber(ARGV[2])
redis.call('HINCRBY', KEYS[1], 'membershipRevision', 1)
redis.call('ZADD', KEYS[2], compensation_expiry_ms, ARGV[1])
local desired_metadata_ttl_ms = tonumber(ARGV[2]) + 5000
local current_metadata_ttl_ms = redis.call('PTTL', KEYS[1])
if current_metadata_ttl_ms >= 0 and current_metadata_ttl_ms < desired_metadata_ttl_ms then
    redis.call('PEXPIRE', KEYS[1], desired_metadata_ttl_ms)
end
redis.call('PEXPIRE', KEYS[2], desired_metadata_ttl_ms)
-- Fence any finalizer command whose response was delayed until after the
-- compensating removal. A later retry observes false and cannot re-admit it.
redis.call('SET', KEYS[3], '0', 'PX', ARGV[4])

local state = redis.call('HGET', KEYS[1], 'state') or 'missing'
if state == 'ad_break' then
    local raw = redis.call('HGET', KEYS[1], 'adBreak')
    if raw then
        local ad_break = cjson.decode(raw)
        local user_id = tonumber(ARGV[3])
        for _, participant in ipairs(ad_break.participant_user_ids) do
            if tonumber(participant) == user_id then
                return {1, 'ad-break-participant'}
            end
        end
    end
end
return {1, state}
"#;

const RELEASE_JOIN_COMPENSATION_SCRIPT: &str = r#"
local metadata_type = redis.call('TYPE', KEYS[1])
if type(metadata_type) == 'table' then metadata_type = metadata_type['ok'] end
local reservations_type = redis.call('TYPE', KEYS[2])
if type(reservations_type) == 'table' then reservations_type = reservations_type['ok'] end
if reservations_type ~= 'none' and reservations_type ~= 'zset' then return 0 end
if ARGV[2] == '1' then
    -- Exact queue/break/game inclusion is a durable successful join outcome.
    -- Record it first so a lost release reply remains recoverable.
    redis.call('SET', KEYS[3], '1', 'PX', ARGV[3])
end
local removed = redis.call('ZREM', KEYS[2], ARGV[1])
if removed > 0 and metadata_type == 'hash' then
    redis.call('HINCRBY', KEYS[1], 'membershipRevision', 1)
end
if metadata_type == 'none' or metadata_type == 'hash' then return 1 end
return 0
"#;

const READ_JOIN_ADMISSION_PROOF_SCRIPT: &str = r#"
local values = {}
for index, key in ipairs(KEYS) do
    local key_type = redis.call('TYPE', key)
    if type(key_type) == 'table' then key_type = key_type['ok'] end
    if key_type ~= 'none' and key_type ~= 'string' then
        return {0, 'wrong-type:' .. tostring(index), '', '', ''}
    end
    values[index] = redis.call('GET', key) or ''
end
return {1, values[1], values[2], values[3], values[4]}
"#;

const FINALIZE_LEAVE_MEMBERSHIP_SCRIPT: &str = r#"
local metadata_type = redis.call('TYPE', KEYS[1])
if type(metadata_type) == 'table' then metadata_type = metadata_type['ok'] end
if metadata_type == 'none' then return 2 end
if metadata_type ~= 'hash' then return 0 end
local state = redis.call('HGET', KEYS[1], 'state')
local reservation_exists = redis.call('ZSCORE', KEYS[2], ARGV[1]) ~= false
if state == 'ad_break' and reservation_exists then
    local raw = redis.call('HGET', KEYS[1], 'adBreak')
    if raw then
        local ad_break = cjson.decode(raw)
        local user_id = tonumber(ARGV[2])
        for _, participant in ipairs(ad_break.participant_user_ids) do
            if tonumber(participant) == user_id then
                redis.call('HSET', KEYS[1], 'state', 'waiting')
                redis.call('HDEL', KEYS[1], 'adBreak')
                state = 'waiting'
                break
            end
        end
    end
end
-- Always fence the member-slot mutation, including when an expired
-- reservation was cleaned and no BEGIN happened to win the race.
redis.call('HINCRBY', KEYS[1], 'membershipRevision', 1)
redis.call('ZREM', KEYS[2], ARGV[1])
if state == 'waiting' then return 1 end
return 2
"#;

const MEMBERSHIP_RESERVATION_PREFIX: &str = "membership-change:";
// Heartbeats verify metadata before refreshing the cross-slot exact member.
// This lease exceeds the prior member response wait, the metadata precheck
// wait, the 10s cadence, and scheduling margin (30s + 30s + 10s).
const LOBBY_MEMBER_LEASE_TTL_MS: i64 = 90_000;
// Initial cross-slot join writes need to survive finalizer retries and one
// synchronous post-finalization heartbeat verification. A crashed attempt is
// still bounded below its metadata-slot reservation.
const TENTATIVE_LOBBY_MEMBER_LEASE_TTL_MS: i64 = 180_000;
// Metadata must outlive the member lease plus the worst delay between the
// metadata extension and subsequent member commit.
pub(crate) const LOBBY_METADATA_IDLE_TTL_MS: i64 = 180_000;
// Reserve/touch/finalizer responses may each consume the 30s Redis bound. A
// provisional exact-member heartbeat runs during finalization, while this
// fence remains long enough to be re-armed before any compensation.
// This fence spans reserve/touch, two finalizer responses, compensation
// preparation, proof/removal retries, and one final 90s provisional member
// lease after cancellation.
const MEMBERSHIP_RESERVATION_TTL_MS: i64 = 300_000;
const MEMBERSHIP_FINALIZATION_OUTCOME_TTL_MS: i64 = 600_000;
const MAX_ACTIVE_MEMBERSHIP_RESERVATIONS: i64 = 16;
pub const MAX_LOBBY_MEMBERS: usize = 4;
const LOBBY_ANCILLARY_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(2);
const LOBBY_ANCILLARY_TTL_SECS: i64 = 180;
const AD_BREAK_FINALIZATION_LEASE_TTL_MS: i64 = 60_000;
const AD_BREAK_FINALIZATION_RENEW_INTERVAL: Duration = Duration::from_secs(10);
/// Redis Cluster nodes should be NTP-synchronized. Subtracting this allowance
/// makes cross-slot lease admission conservative under bounded residual skew.
pub const LOBBY_LEASE_CLOCK_SKEW_ALLOWANCE_MS: i64 = 2_000;

const TOUCH_LOBBY_MEMBER_SCRIPT: &str = r#"
local redis_time = redis.call('TIME')
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local expires_at_ms = now_ms + tonumber(ARGV[2])
if ARGV[3] == '1' then
    local current_expiry = tonumber(redis.call('ZSCORE', KEYS[1], ARGV[1]))
    -- XX alone is insufficient: an expired-but-not-yet-cleaned member must
    -- never be resurrected by a delayed heartbeat after a fenced leave.
    if not current_expiry or current_expiry <= now_ms then
        return {0, expires_at_ms}
    end
    redis.call('ZADD', KEYS[1], 'XX', expires_at_ms, ARGV[1])
else
    -- A reconnect atomically replaces older transports for this user. This
    -- bounds roster work even if one account repeatedly opens new sockets.
    local user_prefix = ARGV[4]
    redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now_ms)
    for _, existing in ipairs(redis.call('ZRANGE', KEYS[1], 0, -1)) do
        if string.sub(existing, 1, string.len(user_prefix)) == user_prefix then
            redis.call('ZREM', KEYS[1], existing)
        end
    end
    if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[5]) then
        return {2, expires_at_ms}
    end
    redis.call('ZADD', KEYS[1], expires_at_ms, ARGV[1])
end
redis.call('EXPIRE', KEYS[1], math.ceil(tonumber(ARGV[2]) / 1000))
return {1, expires_at_ms}
"#;

const EXTEND_KEY_TTL_SCRIPT: &str = r#"
local current_ttl_ms = redis.call('PTTL', KEYS[1])
local desired_ttl_ms = tonumber(ARGV[1])
if current_ttl_ms == -2 then return 0 end
if current_ttl_ms == -1 then
    local state = redis.call('HGET', KEYS[1], 'state')
    if ARGV[2] == '1' or state == 'queued' or state == 'matched' then return 1 end
    return redis.call('PEXPIRE', KEYS[1], desired_ttl_ms)
end
if current_ttl_ms >= 0 and current_ttl_ms < desired_ttl_ms then
    return redis.call('PEXPIRE', KEYS[1], desired_ttl_ms)
end
return 1
"#;

const REFRESH_LOBBY_QUEUE_LEASE_SCRIPT: &str = r#"
if redis.call('HGET', KEYS[1], 'state') ~= 'queued' then return 2 end
local identity_type = redis.call('TYPE', KEYS[2])
if type(identity_type) == 'table' then identity_type = identity_type['ok'] end
if identity_type ~= 'string' then return 0 end
local outcome_type = redis.call('TYPE', KEYS[4])
if type(outcome_type) == 'table' then outcome_type = outcome_type['ok'] end
if outcome_type ~= 'none' and outcome_type ~= 'string' then return 0 end
local identity = redis.call('GET', KEYS[2])
local queued_lobby = cjson.decode(identity)
local user_id = tonumber(ARGV[1])
local participant_found = false
for _, member in ipairs(queued_lobby.members) do
    if tonumber(member.user_id) == user_id then
        participant_found = true
        break
    end
end
if not participant_found then return 0 end
local admitted_outcome = 'admitted:' .. queued_lobby.queue_token
local existing_outcome = redis.call('GET', KEYS[4])
if existing_outcome then
    local outcome_status, outcome_token = string.match(existing_outcome, '^([^:]+):(.+)$')
    if not outcome_status or not outcome_token then return 0 end
    if outcome_token == queued_lobby.queue_token and outcome_status ~= 'admitted' then return 0 end
end
redis.call('SET', KEYS[3], identity, 'PX', ARGV[2])
redis.call('SET', KEYS[4], admitted_outcome, 'PX', ARGV[3])
return 1
"#;

const SNAPSHOT_LOBBY_MEMBERS_SCRIPT: &str = r#"
local redis_time = redis.call('TIME')
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now_ms)
return redis.call('ZRANGE', KEYS[1], 0, -1, 'WITHSCORES')
"#;

const REMOVE_USER_LOBBY_MEMBERS_SCRIPT: &str = r#"
local user_prefix = ARGV[1]
for _, member in ipairs(redis.call('ZRANGE', KEYS[1], 0, -1)) do
    if string.sub(member, 1, string.len(user_prefix)) == user_prefix then
        redis.call('ZREM', KEYS[1], member)
    end
end
local redis_time = redis.call('TIME')
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now_ms)
return redis.call('ZCARD', KEYS[1])
"#;

/// Promote a new host, but only while the stored host is still the one this
/// gateway observed to be absent.
///
/// Two gateways can notice the same departed host at the same instant. The
/// compare-and-set makes the second one a no-op rather than letting it
/// re-run succession against an already-migrated lobby and hand authority to
/// a different member. Returns the host that is authoritative afterwards, so
/// a losing caller adopts the winner's choice instead of its own.
const MIGRATE_LOBBY_HOST_SCRIPT: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 0 then
    return -1
end
local stored_host = redis.call('HGET', KEYS[1], 'hostUserId')
if stored_host ~= ARGV[1] then
    return tonumber(stored_host)
end
redis.call('HSET', KEYS[1], 'hostUserId', ARGV[2])
return tonumber(ARGV[2])
"#;

/// Who should lead, given the stored host and the live roster.
///
/// `None` means leave the record alone: either the stored host is still here,
/// or there is nobody to promote.
///
/// The successor is the lowest active `user_id`. Every gateway derives the
/// same answer from the same roster, and unlike a heartbeat timestamp it does
/// not change as leases refresh, so the choice cannot flap between reads.
fn lobby_host_successor(
    stored_host_user_id: i32,
    members: &BTreeMap<u32, LobbyMember>,
) -> Option<i32> {
    let host_is_active = u32::try_from(stored_host_user_id)
        .is_ok_and(|host_user_id| members.contains_key(&host_user_id));
    if host_is_active {
        return None;
    }

    // An empty roster is a lobby on its way out. Leaving the record alone
    // keeps authority with the original host if they come back.
    i32::try_from(members.keys().next().copied()?).ok()
}

pub fn lobby_membership_valid_until_ms<'a>(
    members: impl IntoIterator<Item = &'a LobbyMember>,
) -> Result<i64> {
    let mut minimum = None;
    for member in members {
        if !member.ts.is_finite() || member.ts <= 0.0 || member.ts > i64::MAX as f64 {
            return Err(anyhow!("Lobby membership lease is invalid"));
        }
        let expiry_ms = member.ts as i64;
        minimum = Some(minimum.map_or(expiry_ms, |current: i64| current.min(expiry_ms)));
    }
    minimum
        .map(|expiry_ms| expiry_ms.saturating_sub(LOBBY_LEASE_CLOCK_SKEW_ALLOWANCE_MS))
        .ok_or_else(|| anyhow!("Lobby has no membership leases"))
}

/// Host-selected matchmaking preferences for a lobby
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct LobbyPreferences {
    // TODO: Use an enum for selected modes instead of a string
    pub selected_modes: Vec<String>,
    pub competitive: bool,
}

impl Default for LobbyPreferences {
    fn default() -> Self {
        Self {
            selected_modes: vec!["duel".to_string()],
            competitive: false,
        }
    }
}

struct LobbyBroadcaster {
    tx: Sender<Lobby>,
    receiver_count: usize,
}

type LobbyBroadcasters = RwLock<HashMap<String, LobbyBroadcaster>>;

/// Manages lobby membership and presence using Redis heartbeats
pub struct LobbyManager {
    redis: RedisConnection,
    #[allow(dead_code)] // kept alive for future lobby persistence
    db: Arc<dyn Database>,
    lobby_broadcasters: LobbyBroadcasters,
    user_cache: Arc<UserCache>,
    pubsub_manager: Arc<PubSubManager>,
}

impl LobbyManager {
    pub fn new(
        redis: RedisConnection,
        db: Arc<dyn Database>,
        pubsub_manager: Arc<PubSubManager>,
    ) -> Self {
        Self {
            redis: redis.clone(),
            db: db.clone(),
            lobby_broadcasters: RwLock::new(HashMap::new()),
            user_cache: Arc::new(UserCache::new(redis.clone(), db.clone())),
            pubsub_manager,
        }
    }

    /// Start the background task that forwards Redis lobby updates to local subscribers
    pub fn start_lobby_update_forwarder(self: &Arc<Self>) {
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            manager.lobby_update_forwarder_loop().await;
        });
    }

    async fn lobby_update_forwarder_loop(self: Arc<Self>) {
        let channel = RedisKeys::lobby_updates_channel();

        loop {
            let mut pubsub_manager = (*self.pubsub_manager).clone();
            match pubsub_manager.subscribe_to_channel(&channel).await {
                Ok(mut receiver) => {
                    info!(
                        "Subscribed to lobby updates channel '{}' for local forwarding",
                        channel
                    );

                    loop {
                        match receiver.recv::<LobbyEvent>().await {
                            Ok(LobbyUpdate { lobby }) => {
                                debug!(
                                    "Received lobby update for '{}' from Redis",
                                    lobby.lobby_code
                                );
                                self.forward_lobby_to_broadcasters(lobby);
                            }
                            Ok(LobbyDelete { lobby_code, state }) => {
                                debug!("Received lobby deletion for '{}' from Redis", lobby_code);
                                self.handle_lobby_deletion(&lobby_code, &state);
                            }
                            Err(e) => {
                                error!(
                                    "Lobby updates subscription error on channel '{}': {}",
                                    channel, e
                                );
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "Failed to subscribe to lobby updates channel '{}': {}",
                        channel, e
                    );
                }
            }

            sleep(Duration::from_secs(1)).await;
        }
    }

    fn forward_lobby_to_broadcasters(&self, lobby: Lobby) {
        let lobby_code = lobby.lobby_code.clone();
        let broadcasters = self.lobby_broadcasters.read().unwrap();

        debug!(
            "Forwarding lobby update for '{}' to {} local receivers",
            lobby_code,
            broadcasters
                .get(&lobby_code)
                .map(|b| b.receiver_count)
                .unwrap_or(0)
        );

        if let Some(broadcaster) = broadcasters.get(&lobby_code)
            && let Err(err) = broadcaster.tx.send(lobby)
        {
            error!(
                "Failed to forward lobby update for '{}' to local receivers: {}",
                lobby_code, err
            );
        }
    }

    fn handle_lobby_deletion(&self, lobby_code: &str, state: &str) {
        if state != "deleted" {
            debug!(
                "Ignoring unsupported lobby state message for '{}': {}",
                lobby_code, state
            );
            return;
        }

        // Send a terminal update so clients can react to deletion
        let placeholder = Lobby {
            lobby_code: lobby_code.to_string(),
            members: BTreeMap::new(),
            host_user_id: 0,
            state: state.to_string(),
            preferences: LobbyPreferences::default(),
            ad_break: None,
        };
        self.forward_lobby_to_broadcasters(placeholder);

        let mut broadcasters = self.lobby_broadcasters.write().unwrap();
        if broadcasters.remove(lobby_code).is_some() {
            debug!("Removed broadcaster for deleted lobby '{}'", lobby_code);
        }
    }

    /// Create a new lobby. Generates an id and assigns the host user.
    pub async fn create_lobby(&self, host_user_id: i32, region: &str) -> Result<Lobby> {
        self.create_lobby_for_pool(host_user_id, region, MatchmakingPool::Public)
            .await
    }

    /// Create a lobby in the server-attested matchmaking pool of its host.
    pub async fn create_lobby_for_pool(
        &self,
        host_user_id: i32,
        region: &str,
        matchmaking_pool: MatchmakingPool,
    ) -> Result<Lobby> {
        use chrono::Utc;

        let now = Utc::now();
        let lobby_code = self.generate_unique_lobby_code(region, 10).await?;

        let lobby_metadata = LobbyMetadata {
            lobby_code,
            host_user_id,
            region: region.to_string(),
            created_at: now,
            state: "waiting".to_string(),
            matchmaking_pool,
            ad_break: None,
        };

        // Store lobby metadata in Redis
        self.save_lobby_metadata(&lobby_metadata)
            .await
            .context("Failed to store lobby metadata")?;

        // Initialize lobby preferences
        let preferences = LobbyPreferences::default();
        self.set_lobby_preferences(&lobby_metadata.lobby_code, &preferences)
            .await
            .context("Failed to initialize lobby preferences")?;

        self.touch_lobby(&lobby_metadata.lobby_code, None, false)
            .await
            .context("Failed to touch lobby on creation")?;

        info!(
            "Created lobby '{}' for user {} in region {} (pool: {})",
            lobby_metadata.lobby_code, host_user_id, region, matchmaking_pool
        );

        Ok(Lobby {
            lobby_code: lobby_metadata.lobby_code,
            members: BTreeMap::new(),
            host_user_id: lobby_metadata.host_user_id,
            state: lobby_metadata.state,
            preferences,
            ad_break: None,
        })
    }

    /// Save lobby metadata to Redis
    async fn save_lobby_metadata(&self, metadata: &LobbyMetadata) -> Result<()> {
        let mut redis = self.redis.clone();
        let metadata_key = RedisKeys::lobby_metadata(&metadata.lobby_code);
        let saved: i64 = Script::new(
            r#"
            if redis.call('EXISTS', KEYS[1]) == 1
                or redis.call('EXISTS', KEYS[2]) == 1
                or redis.call('EXISTS', KEYS[3]) == 1 then
                return 0
            end
            redis.call('HSET', KEYS[1],
                'hostUserId', ARGV[1],
                'region', ARGV[2],
                'createdAt', ARGV[3],
                'state', ARGV[4],
                'matchmakingPool', ARGV[5],
                'membershipRevision', 0
            )
            redis.call('PEXPIRE', KEYS[1], ARGV[6])
            return 1
            "#,
        )
        .key(&metadata_key)
        .key(RedisKeys::matchmaking_lobby_queue_identity(
            &metadata.lobby_code,
        ))
        .key(RedisKeys::matchmaking_lobby_active_game(
            &metadata.lobby_code,
        ))
        .arg(metadata.host_user_id)
        .arg(&metadata.region)
        .arg(metadata.created_at.to_rfc3339())
        .arg(&metadata.state)
        .arg(metadata.matchmaking_pool.as_str())
        .arg(LOBBY_METADATA_IDLE_TTL_MS)
        .invoke_async(&mut redis)
        .await
        .context("Failed to store lobby metadata")?;
        if saved != 1 {
            return Err(anyhow!(
                "Lobby code '{}' was claimed by another generation",
                metadata.lobby_code
            ));
        }

        Ok(())
    }

    /// Start heartbeat loop for user in lobby
    /// Returns a handle that automatically cancels the heartbeat on drop
    pub async fn join_lobby(
        self: &Arc<Self>,
        lobby_code: Option<&str>,
        user_id: i32,
        _username: String,
        websocket_id: String,
        region: String,
        requested_preferences: Option<LobbyPreferences>,
    ) -> Result<LobbyJoinHandle> {
        self.join_lobby_for_pool(
            lobby_code,
            user_id,
            _username,
            websocket_id,
            region,
            requested_preferences,
            MatchmakingPool::Public,
            None,
            true,
            false,
        )
        .await
    }

    /// Join only a lobby in the authenticated user's server-attested pool.
    #[allow(clippy::too_many_arguments)]
    pub async fn join_lobby_for_pool(
        self: &Arc<Self>,
        lobby_code: Option<&str>,
        user_id: i32,
        _username: String,
        websocket_id: String,
        region: String,
        requested_preferences: Option<LobbyPreferences>,
        matchmaking_pool: MatchmakingPool,
        distribution: Option<ClientDistribution>,
        supports_ad_break: bool,
        can_show_video_ad: bool,
    ) -> Result<LobbyJoinHandle> {
        if let Some(lobby_code) = lobby_code {
            Self::validate_lobby_code(lobby_code)?;
        }
        let lobby = if let Some(lobby_code) = lobby_code {
            self.ensure_joinable_lobby(
                lobby_code,
                user_id,
                &region,
                requested_preferences.as_ref(),
                matchmaking_pool,
            )
            .await?
        } else {
            self.create_lobby_for_pool(user_id, &region, matchmaking_pool)
                .await?
        };

        let member_user_id =
            u32::try_from(user_id).context("Lobby user ID must be non-negative")?;
        let member_value = MemberValue {
            user_id: member_user_id,
            websocket_id: websocket_id.clone(),
            supports_ad_break,
            can_show_video_ad: supports_ad_break && can_show_video_ad,
            distribution,
        };

        // Reserve membership in the metadata slot before changing the presence
        // sorted set. BEGIN_AD_BREAK_SCRIPT observes this reservation, closing
        // the otherwise cross-slot race between roster snapshot and break start.
        // Existing break participants may reconnect without mutating the roster.
        let membership_reservation = self
            .reserve_join_membership_change(&lobby.lobby_code, member_user_id)
            .await?;
        let touch_result = self
            .touch_lobby_member(
                &lobby.lobby_code,
                &member_value,
                false,
                TENTATIVE_LOBBY_MEMBER_LEASE_TTL_MS,
            )
            .await
            .context("Failed to touch lobby on join");
        if let Err(error) = touch_result {
            if let Some(field) = membership_reservation.as_deref() {
                // ZADD may have committed even when its response was lost.
                // Compensate while the original reservation is still live,
                // then close the seqlock with an unconditional revision bump.
                let mut redis = self.redis.clone();
                if let Err(compensation_error) = redis
                    .zrem::<_, _, ()>(
                        RedisKeys::lobby_members_set(&lobby.lobby_code),
                        member_value.to_string(),
                    )
                    .await
                {
                    // Retain the reservation until expiry. It outlives the
                    // maximum Redis response wait plus a complete member
                    // lease, so the ambiguous member cannot outlive its fence.
                    return Err(error.context(format!(
                        "ambiguous lobby join could not be compensated: {compensation_error}"
                    )));
                }
                self.finalize_leave_membership_change(&lobby.lobby_code, field, member_user_id)
                    .await?;
            }
            return Err(error);
        }

        // Keep the exact tentative member alive while the durable finalizer
        // response is retried. The original metadata-slot reservation remains
        // the admission fence until finalization or exact compensation wins.
        let self_for_heartbeat = self.clone();
        let lobby_code_for_heartbeat = lobby.lobby_code.clone();
        let member_for_heartbeat = member_value.clone();
        let reservation_for_heartbeat = membership_reservation.clone();
        let join_finalized = Arc::new(AtomicBool::new(false));
        let join_finalized_for_heartbeat = Arc::clone(&join_finalized);
        let scope_cancellation = CancellationToken::new();
        let heartbeat_scope_cancellation = scope_cancellation.clone();
        let task = tokio::spawn(async move {
            let mut heartbeat_interval = interval(Duration::from_secs(10));
            loop {
                tokio::select! {
                    _ = heartbeat_scope_cancellation.cancelled() => break,
                    _ = heartbeat_interval.tick() => {
                        if !join_finalized_for_heartbeat.load(Ordering::Acquire)
                            && let Some(field) = reservation_for_heartbeat.as_deref()
                            && let Err(err) = self_for_heartbeat
                                .renew_provisional_join_reservation(
                                    &lobby_code_for_heartbeat,
                                    field,
                                )
                                .await
                        {
                            error!(
                                "Failed to renew provisional lobby join for user {}: {}",
                                user_id, err
                            );
                            heartbeat_scope_cancellation.cancel();
                            break;
                        }
                        if let Err(err) = self_for_heartbeat
                            .touch_provisional_lobby_member(
                                lobby_code_for_heartbeat.as_str(),
                                &member_for_heartbeat,
                            )
                            .await
                        {
                            error!("Failed to send heartbeat for user {}: {}", user_id, err);
                            heartbeat_scope_cancellation.cancel();
                            break;
                        }
                    }
                }
            }
        });
        let mut heartbeat_guard = ProvisionalLobbyHeartbeat {
            task: Some(task),
            cancellation: Some(scope_cancellation),
        };

        if let Some(field) = membership_reservation.as_deref() {
            match self
                .finalize_join_membership_change(&lobby.lobby_code, field, member_user_id)
                .await
            {
                Ok(true) => join_finalized.store(true, Ordering::Release),
                Ok(false) => {
                    match self
                        .recover_or_compensate_join(&lobby.lobby_code, field, &member_value)
                        .await
                    {
                        Ok(true) => warn!(
                            lobby_code = lobby.lobby_code,
                            "Recovered rejected join finalization from exact durable admission proof"
                        ),
                        Ok(false) => {
                            return Err(anyhow!(
                                "Lobby changed while membership was being committed; retry the join"
                            ));
                        }
                        Err(recovery_error) => {
                            return Err(recovery_error.context(
                                "Failed to safely resolve a rejected lobby join finalization",
                            ));
                        }
                    }
                    join_finalized.store(true, Ordering::Release);
                }
                Err(finalize_error) => {
                    match self
                        .recover_or_compensate_join(&lobby.lobby_code, field, &member_value)
                        .await
                    {
                        Ok(true) => warn!(
                            lobby_code = lobby.lobby_code,
                            %finalize_error,
                            "Recovered ambiguous join finalization from durable or exact admission proof"
                        ),
                        Ok(false) => {
                            return Err(finalize_error.context(
                                "Lobby join finalization was ambiguous; removed the tentative presence under its original fence",
                            ));
                        }
                        Err(recovery_error) => {
                            return Err(recovery_error.context(format!(
                                "Failed to recover ambiguous lobby join finalization: {finalize_error}"
                            )));
                        }
                    }
                    join_finalized.store(true, Ordering::Release);
                }
            }
        } else {
            join_finalized.store(true, Ordering::Release);
        }

        // Do not transfer a provisional task that may have already exited on
        // a transient failure. Stop it, verify the exact committed member
        // synchronously while the longer tentative lease is still live, then
        // start a fresh ordinary heartbeat with a fresh scope.
        heartbeat_guard.stop();
        let mut last_verification_error = None;
        for attempt in 0..2 {
            match self
                .touch_provisional_lobby_member(&lobby.lobby_code, &member_value)
                .await
            {
                Ok(()) => {
                    last_verification_error = None;
                    break;
                }
                Err(error) => {
                    last_verification_error = Some(error);
                    if attempt == 0 {
                        warn!(
                            lobby_code = lobby.lobby_code,
                            "Retrying post-finalization lobby heartbeat verification"
                        );
                    }
                }
            }
        }
        if let Some(error) = last_verification_error {
            let detail = format!("{error:#}");
            if detail.contains("lease no longer exists")
                || detail.contains("metadata no longer exists")
            {
                return Err(error).context(
                    "Committed lobby join lost its exact presence before heartbeat transfer",
                );
            }
            warn!(
                lobby_code = lobby.lobby_code,
                %error,
                "Heartbeat verification replies remained ambiguous; fresh heartbeat will reconcile"
            );
        }
        drop(heartbeat_guard);

        info!("User {} joined lobby '{}'", user_id, lobby.lobby_code);

        // Subscribe to lobby updates
        let rx = {
            let broadcasters = &mut self.lobby_broadcasters.write().unwrap();
            let broadcaster = broadcasters
                .entry(lobby.lobby_code.clone())
                .or_insert_with(|| {
                    let (tx, _) = tokio::sync::broadcast::channel(100);
                    LobbyBroadcaster {
                        tx,
                        receiver_count: 0,
                    }
                });
            broadcaster.receiver_count += 1;
            broadcaster.tx.subscribe()
        };

        if let Err(error) = self.publish_lobby_update(&lobby.lobby_code).await {
            warn!(
                lobby_code = %lobby.lobby_code,
                %error,
                "Failed to publish lobby join; authoritative reconciliation will recover"
            );
        }

        let self_for_heartbeat = self.clone();
        let lobby_code_for_heartbeat = lobby.lobby_code.clone();
        let member_for_heartbeat = member_value.clone();
        let scope_cancellation = CancellationToken::new();
        let heartbeat_scope_cancellation = scope_cancellation.clone();
        let task = tokio::spawn(async move {
            let mut heartbeat_interval = interval(Duration::from_secs(10));
            // Synchronous verification just refreshed the exact lease. Avoid
            // an immediate post-transfer Redis call that could fail and kill
            // the scope before the client receives its handle.
            heartbeat_interval.tick().await;
            loop {
                tokio::select! {
                    _ = heartbeat_scope_cancellation.cancelled() => break,
                    _ = heartbeat_interval.tick() => {
                        if let Err(err) = self_for_heartbeat
                            .touch_lobby(
                                &lobby_code_for_heartbeat,
                                Some(member_for_heartbeat.clone()),
                                true,
                            )
                            .await
                        {
                            error!("Failed to send heartbeat for user {}: {}", user_id, err);
                            heartbeat_scope_cancellation.cancel();
                            break;
                        }
                    }
                }
            }
        });
        let heartbeat_guard = ProvisionalLobbyHeartbeat {
            task: Some(task),
            cancellation: Some(scope_cancellation),
        };

        // Transfer task ownership only after every fallible provisional join
        // await has completed. Before this point, guard Drop aborts it.
        let (task, scope_cancellation) = heartbeat_guard.into_parts();

        // Store the handle
        let handle = LobbyJoinHandle {
            heartbeat_task: task,
            scope_cancellation,
            lobby_manager: self.clone(),
            returned: RwLock::new(false),
            rx,
            lobby_code: lobby.lobby_code,
            user_id,
            websocket_id,
        };

        Ok(handle)
    }

    pub fn return_handle(&self, handle: &LobbyJoinHandle) {
        let mut broadcasters = self.lobby_broadcasters.write().unwrap();
        if let Some(broadcaster) = broadcasters.get_mut(&handle.lobby_code) {
            if broadcaster.receiver_count > 0 {
                broadcaster.receiver_count -= 1;
            }
            if broadcaster.receiver_count == 0 {
                broadcasters.remove(&handle.lobby_code);
            }
        }
    }

    pub async fn get_lobby(&self, lobby_code: &str) -> Result<Lobby> {
        self.get_lobby_opt(lobby_code)
            .await?
            .ok_or_else(|| anyhow!("Lobby '{}' not found", lobby_code))
    }

    pub async fn get_lobby_opt(&self, lobby_code: &str) -> Result<Option<Lobby>> {
        if let Some(lobby_model) = self.get_lobby_metadata(lobby_code).await? {
            let members = self.get_lobby_members(lobby_code).await?;
            let preferences = self.get_lobby_preferences(lobby_code).await?;
            let host_user_id = self
                .resolve_effective_host(lobby_code, lobby_model.host_user_id, &members)
                .await;
            Ok(Some(Lobby {
                lobby_code: lobby_model.lobby_code,
                members,
                host_user_id,
                state: lobby_model.state,
                preferences,
                ad_break: lobby_model.ad_break,
            }))
        } else {
            Ok(None)
        }
    }

    /// Resolve who actually leads this lobby, migrating the stored host when
    /// the recorded one is no longer an active member.
    ///
    /// Nothing ever rewrote `hostUserId` before leader-gated controls existed,
    /// so a host who left stranded every remaining member behind a permanently
    /// unusable mode selector. Succession is resolved on read rather than on
    /// the leave path because a host can also vanish by simply letting their
    /// membership lease expire, which no code path observes.
    ///
    /// The successor is the lowest active `user_id`: every gateway derives the
    /// same answer from the same roster, and unlike a heartbeat timestamp it
    /// does not change as leases refresh, so the choice cannot flap.
    ///
    /// A failed migration is not fatal — this returns the stored host and the
    /// next read tries again.
    async fn resolve_effective_host(
        &self,
        lobby_code: &str,
        stored_host_user_id: i32,
        members: &BTreeMap<u32, LobbyMember>,
    ) -> i32 {
        let Some(successor) = lobby_host_successor(stored_host_user_id, members) else {
            return stored_host_user_id;
        };

        match self
            .migrate_lobby_host(lobby_code, stored_host_user_id, successor)
            .await
        {
            Ok(Some(host_user_id)) => {
                if host_user_id == successor {
                    info!(
                        "Lobby '{}' host {} is no longer present; promoted {}",
                        lobby_code, stored_host_user_id, successor
                    );
                }
                host_user_id
            }
            // The lobby disappeared underneath us; the caller's snapshot is
            // already terminal.
            Ok(None) => stored_host_user_id,
            Err(error) => {
                warn!(
                    lobby_code,
                    stored_host_user_id,
                    successor,
                    "Failed to migrate absent lobby host: {error:#}"
                );
                stored_host_user_id
            }
        }
    }

    /// Compare-and-set `hostUserId`. `None` means the lobby no longer exists.
    async fn migrate_lobby_host(
        &self,
        lobby_code: &str,
        expected_host_user_id: i32,
        successor_user_id: i32,
    ) -> Result<Option<i32>> {
        let mut redis = self.redis.clone();
        let host_user_id: i64 = Script::new(MIGRATE_LOBBY_HOST_SCRIPT)
            .key(RedisKeys::lobby_metadata(lobby_code))
            .arg(expected_host_user_id.to_string())
            .arg(successor_user_id.to_string())
            .invoke_async(&mut redis)
            .await
            .context("Failed to migrate lobby host")?;

        if host_user_id == -1 {
            return Ok(None);
        }

        Ok(Some(
            i32::try_from(host_user_id).context("Migrated lobby host does not fit a user ID")?,
        ))
    }

    /// Get lobby by code from Redis
    pub async fn get_lobby_metadata(&self, lobby_code: &str) -> Result<Option<LobbyMetadata>> {
        use chrono::{DateTime, Utc};
        use redis::AsyncCommands;
        use std::collections::HashMap;

        let metadata_key = RedisKeys::lobby_metadata(lobby_code);
        let mut redis = self.redis.clone();

        // Check if lobby exists
        if !redis
            .exists(&metadata_key)
            .await
            .context("Failed to check lobby existence")?
        {
            warn!("Lobby '{}' does not exist in Redis", lobby_code);
            return Ok(None);
        }

        debug!("Fetching metadata for lobby '{}'", lobby_code);

        // Fetch all metadata fields
        let data: HashMap<String, String> = redis
            .hgetall(&metadata_key)
            .await
            .context("Failed to fetch lobby metadata")?;

        let state = data
            .get("state")
            .ok_or_else(|| anyhow!("Missing state"))?
            .to_string();
        // Queue admission owns the state transition and may commit before its
        // best-effort payload cleanup. Never expose that stale payload once the
        // lobby has left the active ad-break state.
        let ad_break = if state == "ad_break" {
            let value = data
                .get("adBreak")
                .ok_or_else(|| anyhow!("Lobby is in ad_break state without adBreak metadata"))?;
            let ad_break = serde_json::from_str::<LobbyAdBreak>(value)
                .context("Invalid adBreak lobby metadata")?;
            ad_break
                .validate()
                .context("Invalid persisted adBreak invariants")?;
            Some(ad_break)
        } else {
            None
        };

        // Parse and construct Lobby
        let lobby = LobbyMetadata {
            lobby_code: lobby_code.to_string(),
            host_user_id: data
                .get("hostUserId")
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| anyhow!("Invalid or missing hostUserId"))?,
            region: data
                .get("region")
                .ok_or_else(|| anyhow!("Missing region"))?
                .to_string(),
            created_at: data
                .get("createdAt")
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&Utc))
                .ok_or_else(|| anyhow!("Invalid or missing createdAt"))?,
            state,
            matchmaking_pool: match data.get("matchmakingPool").map(String::as_str) {
                None | Some("public") => MatchmakingPool::Public,
                Some("stress") => MatchmakingPool::Stress,
                Some(value) => {
                    return Err(anyhow!(
                        "Invalid matchmaking pool '{}' for lobby '{}'",
                        value,
                        lobby_code
                    ));
                }
            },
            ad_break,
        };

        Ok(Some(lobby))
    }

    /// Monotonic fence advanced before every cross-slot join/leave mutation.
    /// A queue snapshot must observe the same value again when it commits the
    /// ad break, otherwise its roster may already be stale.
    pub async fn get_lobby_membership_revision(&self, lobby_code: &str) -> Result<i64> {
        let mut redis = self.redis.clone();
        let revision: Option<i64> = redis
            .hget(RedisKeys::lobby_metadata(lobby_code), "membershipRevision")
            .await
            .context("Failed to load lobby membership revision")?;
        Ok(revision.unwrap_or(0))
    }

    /// Delete a lobby and all associated Redis keys
    pub async fn delete_lobby(&self, lobby_code: &str) -> Result<()> {
        use redis::AsyncCommands;

        // Delete all lobby-related keys
        let keys = vec![
            RedisKeys::lobby_metadata(lobby_code),
            RedisKeys::lobby_membership_reservations(lobby_code),
            RedisKeys::lobby_ad_break_finalization_claim(lobby_code),
            RedisKeys::lobby_members_set(lobby_code),
            RedisKeys::lobby_preferences(lobby_code),
            RedisKeys::lobby_chat_history_key(lobby_code),
        ];

        // Lobby metadata participates in the matchmaking slot while the
        // transient members/preferences/chat keys do not. Delete them
        // independently so this remains valid on Redis Cluster/Valkey
        // Serverless, where a multi-key DEL may not cross hash slots.
        let mut redis = self.redis.clone();
        for key in keys {
            redis
                .del::<_, ()>(key)
                .await
                .context("Failed to delete a lobby key from Redis")?;
        }

        info!("Deleted lobby '{}'", lobby_code);
        Ok(())
    }

    async fn ensure_joinable_lobby(
        &self,
        lobby_code: &str,
        host_user_id: i32,
        region: &str,
        requested_preferences: Option<&LobbyPreferences>,
        matchmaking_pool: MatchmakingPool,
    ) -> Result<Lobby> {
        if let Some(lobby) = self.get_lobby_opt(lobby_code).await? {
            self.ensure_matching_pool(lobby_code, matchmaking_pool)
                .await?;
            return Ok(lobby);
        }

        self.create_lobby_with_code_if_absent(
            lobby_code,
            host_user_id,
            region,
            requested_preferences,
            matchmaking_pool,
        )
        .await?;

        // A different task may have won the missing-lobby race. Always reload
        // and validate the stored pool before adding presence.
        self.ensure_matching_pool(lobby_code, matchmaking_pool)
            .await?;
        self.get_lobby(lobby_code).await
    }

    async fn ensure_matching_pool(
        &self,
        lobby_code: &str,
        expected_pool: MatchmakingPool,
    ) -> Result<()> {
        let metadata = self
            .get_lobby_metadata(lobby_code)
            .await?
            .ok_or_else(|| anyhow!("Lobby '{}' not found", lobby_code))?;
        if metadata.matchmaking_pool != expected_pool {
            return Err(anyhow!(
                "Lobby '{}' belongs to a different matchmaking pool",
                lobby_code
            ));
        }
        Ok(())
    }

    async fn reserve_join_membership_change(
        &self,
        lobby_code: &str,
        user_id: u32,
    ) -> Result<Option<String>> {
        let field = format!("{MEMBERSHIP_RESERVATION_PREFIX}{}", uuid::Uuid::new_v4());
        let mut redis = self.redis.clone();
        let (code, detail): (i64, String) = Script::new(RESERVE_JOIN_MEMBERSHIP_SCRIPT)
            .key(RedisKeys::lobby_metadata(lobby_code))
            .key(RedisKeys::lobby_membership_reservations(lobby_code))
            .key(RedisKeys::matchmaking_lobby_queue_identity(lobby_code))
            .key(RedisKeys::matchmaking_lobby_queue_lease(lobby_code))
            .key(RedisKeys::matchmaking_lobby_queue_outcome(lobby_code))
            .arg(&field)
            .arg(MEMBERSHIP_RESERVATION_TTL_MS)
            .arg(user_id)
            .arg(MAX_ACTIVE_MEMBERSHIP_RESERVATIONS)
            .arg(LOBBY_QUEUE_LEASE_TTL_MS)
            .arg(LOBBY_QUEUE_OUTCOME_TTL_MS)
            .invoke_async(&mut redis)
            .await
            .context("Failed to reserve lobby membership before join")?;

        match code {
            1 => Ok(Some(field)),
            0 => Err(anyhow!("Lobby cannot be joined while {detail}")),
            _ => Err(anyhow!(
                "Unknown lobby join reservation result {code}: {detail}"
            )),
        }
    }

    async fn reserve_leave_membership_change(
        &self,
        lobby_code: &str,
        user_id: u32,
        require_active_participant: bool,
    ) -> Result<Option<String>> {
        let field = format!("{MEMBERSHIP_RESERVATION_PREFIX}{}", uuid::Uuid::new_v4());
        let mut redis = self.redis.clone();
        let (code, detail): (i64, String) = Script::new(RESERVE_LEAVE_MEMBERSHIP_SCRIPT)
            .key(RedisKeys::lobby_metadata(lobby_code))
            .key(RedisKeys::lobby_membership_reservations(lobby_code))
            .arg(&field)
            .arg(MEMBERSHIP_RESERVATION_TTL_MS)
            .arg(MAX_ACTIVE_MEMBERSHIP_RESERVATIONS)
            .arg(user_id)
            .arg(if require_active_participant { 1 } else { 0 })
            .invoke_async(&mut redis)
            .await
            .context("Failed to reserve lobby membership before leave")?;

        match code {
            1 => {
                if detail == "cancelled" {
                    info!(lobby_code, "Cancelled ad break because a participant left");
                }
                Ok(Some(field))
            }
            2 => Ok(None),
            0 => Err(anyhow!("Failed to prepare lobby leave: {detail}")),
            _ => Err(anyhow!(
                "Unknown lobby leave reservation result {code}: {detail}"
            )),
        }
    }

    async fn finalize_join_membership_change(
        &self,
        lobby_code: &str,
        field: &str,
        user_id: u32,
    ) -> Result<bool> {
        let mut redis = self.redis.clone();
        let outcome_key = RedisKeys::lobby_membership_finalization_outcome(lobby_code, field);
        let mut last_error = None;
        for attempt in 0..2 {
            match Script::new(FINALIZE_JOIN_MEMBERSHIP_SCRIPT)
                .key(RedisKeys::lobby_metadata(lobby_code))
                .key(RedisKeys::lobby_membership_reservations(lobby_code))
                .key(&outcome_key)
                .key(RedisKeys::matchmaking_lobby_queue_identity(lobby_code))
                .arg(field)
                .arg(user_id)
                .arg(MEMBERSHIP_FINALIZATION_OUTCOME_TTL_MS)
                .arg(MEMBERSHIP_RESERVATION_TTL_MS)
                .invoke_async::<i64>(&mut redis)
                .await
            {
                Ok(finalized) => return Ok(finalized == 1),
                Err(error) => {
                    last_error = Some(error);
                    if attempt == 0 {
                        warn!(lobby_code, "Retrying ambiguous lobby join finalizer");
                    }
                }
            }
        }
        Err(last_error.expect("bounded finalizer loop records an error"))
            .context("Failed to finalize lobby join membership fence")
    }

    async fn renew_provisional_join_reservation(
        &self,
        lobby_code: &str,
        field: &str,
    ) -> Result<()> {
        let mut redis = self.redis.clone();
        let renewed: i64 = Script::new(RENEW_PROVISIONAL_JOIN_RESERVATION_SCRIPT)
            .key(RedisKeys::lobby_metadata(lobby_code))
            .key(RedisKeys::lobby_membership_reservations(lobby_code))
            .key(RedisKeys::lobby_membership_finalization_outcome(
                lobby_code, field,
            ))
            .arg(field)
            .arg(MEMBERSHIP_RESERVATION_TTL_MS)
            .invoke_async(&mut redis)
            .await
            .context("Failed to renew provisional lobby join fence")?;
        match renewed {
            1 | 2 => Ok(()),
            _ => Err(anyhow!(
                "Provisional lobby join fence disappeared before finalization"
            )),
        }
    }

    /// Resolve an uncertain join without using generic lobby state as proof.
    /// The original reservation is re-armed before inspecting the cross-slot
    /// queue identity, so exact member removal cannot interleave a new admit.
    async fn recover_or_compensate_join(
        &self,
        lobby_code: &str,
        field: &str,
        member: &MemberValue,
    ) -> Result<bool> {
        let mut redis = self.redis.clone();
        let outcome_key = RedisKeys::lobby_membership_finalization_outcome(lobby_code, field);
        let prepare_script = Script::new(PREPARE_JOIN_COMPENSATION_SCRIPT);
        let mut last_prepare_error = None;
        let mut preparation = None;
        for attempt in 0..2 {
            match prepare_script
                .key(RedisKeys::lobby_metadata(lobby_code))
                .key(RedisKeys::lobby_membership_reservations(lobby_code))
                .key(&outcome_key)
                .arg(field)
                .arg(MEMBERSHIP_RESERVATION_TTL_MS)
                .arg(member.user_id)
                .arg(MEMBERSHIP_FINALIZATION_OUTCOME_TTL_MS)
                .invoke_async::<(i64, String)>(&mut redis)
                .await
            {
                Ok(result) => {
                    preparation = Some(result);
                    break;
                }
                Err(error) => {
                    last_prepare_error = Some(error);
                    if attempt == 0 {
                        warn!(
                            lobby_code,
                            "Retrying ambiguous join compensation preparation"
                        );
                    }
                }
            }
        }
        let (code, detail) = preparation.ok_or_else(|| {
            anyhow!(
                "Failed to prepare exact lobby join recovery: {}",
                last_prepare_error.expect("bounded preparation loop records an error")
            )
        })?;

        if code == 2 {
            return Ok(true);
        }
        if code != 1 && code != 3 {
            return Err(anyhow!(
                "Lobby join compensation preparation failed: {detail}"
            ));
        }

        let exactly_admitted = detail == "ad-break-participant"
            || self
                .has_exact_join_admission(lobby_code, member.user_id)
                .await?;

        if !exactly_admitted {
            // XX heartbeat refreshes cannot resurrect this exact value after
            // the ZREM, regardless of command ordering across connections.
            let mut last_error = None;
            for attempt in 0..2 {
                match redis
                    .zrem::<_, _, ()>(RedisKeys::lobby_members_set(lobby_code), member.to_string())
                    .await
                {
                    Ok(()) => {
                        last_error = None;
                        break;
                    }
                    Err(error) => {
                        last_error = Some(error);
                        if attempt == 0 {
                            warn!(lobby_code, "Retrying ambiguous exact member compensation");
                        }
                    }
                }
            }
            if let Some(error) = last_error {
                return Err(error).context("Failed to compensate exact tentative lobby member");
            }
        }

        let release_script = Script::new(RELEASE_JOIN_COMPENSATION_SCRIPT);
        let mut last_release_error = None;
        for attempt in 0..2 {
            match release_script
                .key(RedisKeys::lobby_metadata(lobby_code))
                .key(RedisKeys::lobby_membership_reservations(lobby_code))
                .key(&outcome_key)
                .arg(field)
                .arg(if exactly_admitted { 1 } else { 0 })
                .arg(MEMBERSHIP_FINALIZATION_OUTCOME_TTL_MS)
                .invoke_async::<i64>(&mut redis)
                .await
            {
                Ok(1) => return Ok(exactly_admitted),
                Ok(_) => {
                    return Err(anyhow!(
                        "Lobby join recovery fence had an invalid Redis type"
                    ));
                }
                Err(error) => {
                    last_release_error = Some(error);
                    if attempt == 0 {
                        warn!(lobby_code, "Retrying ambiguous join recovery fence release");
                    }
                }
            }
        }
        let release_error = last_release_error.expect("bounded release loop records an error");
        if exactly_admitted {
            warn!(
                lobby_code,
                %release_error,
                "Exact lobby admission was proven but recovery fence release remained ambiguous"
            );
            // Failing the join now would contradict durable queue/break/game
            // inclusion and let its live member expire. A lingering fence is
            // bounded and safe; a lost successful reply is replayable via the
            // durable success outcome written before release.
            return Ok(true);
        }
        Err(release_error).context("Failed to release lobby join recovery fence")
    }

    /// Read one atomic snapshot from the matchmaking slot. Queue membership
    /// or matching user/lobby game mappings are exact proof; a state label is
    /// deliberately insufficient.
    async fn has_exact_join_admission(&self, lobby_code: &str, user_id: u32) -> Result<bool> {
        let mut redis = self.redis.clone();
        let (code, queue_identity, lobby_game, user_game, pending_game): (
            i64,
            String,
            String,
            String,
            String,
        ) = Script::new(READ_JOIN_ADMISSION_PROOF_SCRIPT)
            .key(RedisKeys::matchmaking_lobby_queue_identity(lobby_code))
            .key(RedisKeys::matchmaking_lobby_active_game(lobby_code))
            .key(RedisKeys::matchmaking_user_active_game(user_id))
            .key(RedisKeys::matchmaking_lobby_user_pending_game(
                lobby_code, user_id,
            ))
            .invoke_async(&mut redis)
            .await
            .context("Failed to read exact lobby admission proof")?;
        if code != 1 {
            return Err(anyhow!(
                "Lobby admission proof keys had an invalid Redis type: {queue_identity}"
            ));
        }

        let queued = if queue_identity.is_empty() {
            false
        } else {
            let queued_lobby: QueuedLobby = serde_json::from_str(&queue_identity)
                .context("Malformed exact lobby queue identity")?;
            queued_lobby.lobby_code == lobby_code
                && queued_lobby
                    .members
                    .iter()
                    .any(|queued_member| queued_member.user_id == user_id)
        };
        let matched =
            !lobby_game.is_empty() && (user_game == lobby_game || pending_game == lobby_game);
        Ok(queued || matched)
    }

    async fn finalize_leave_membership_change(
        &self,
        lobby_code: &str,
        field: &str,
        user_id: u32,
    ) -> Result<()> {
        let mut redis = self.redis.clone();
        let finalized: i64 = Script::new(FINALIZE_LEAVE_MEMBERSHIP_SCRIPT)
            .key(RedisKeys::lobby_metadata(lobby_code))
            .key(RedisKeys::lobby_membership_reservations(lobby_code))
            .arg(field)
            .arg(user_id)
            .invoke_async(&mut redis)
            .await
            .context("Failed to finalize lobby leave membership fence")?;
        match finalized {
            1 | 2 => Ok(()),
            _ => Err(anyhow!("Lobby metadata had the wrong type during leave")),
        }
    }

    async fn create_lobby_with_code_if_absent(
        &self,
        lobby_code: &str,
        host_user_id: i32,
        region: &str,
        requested_preferences: Option<&LobbyPreferences>,
        matchmaking_pool: MatchmakingPool,
    ) -> Result<bool> {
        use chrono::Utc;

        let mut redis = self.redis.clone();
        let metadata_key = RedisKeys::lobby_metadata(lobby_code);
        let created_at = Utc::now().to_rfc3339();

        let script = Script::new(
            r#"
            if redis.call('EXISTS', KEYS[1]) == 1 then
                return 0
            end
            if redis.call('EXISTS', KEYS[2]) == 1 or redis.call('EXISTS', KEYS[3]) == 1 then
                return 2
            end
            redis.call('HSET', KEYS[1],
                'hostUserId', ARGV[1],
                'region', ARGV[2],
                'createdAt', ARGV[3],
                'state', ARGV[4],
                'matchmakingPool', ARGV[5],
                'membershipRevision', 0
            )
            redis.call('PEXPIRE', KEYS[1], ARGV[6])
            return 1
        "#,
        );

        let created: i32 = script
            .key(&metadata_key)
            .key(RedisKeys::matchmaking_lobby_queue_identity(lobby_code))
            .key(RedisKeys::matchmaking_lobby_active_game(lobby_code))
            .arg(host_user_id)
            .arg(region)
            .arg(&created_at)
            .arg("waiting")
            .arg(matchmaking_pool.as_str())
            .arg(LOBBY_METADATA_IDLE_TTL_MS)
            .invoke_async(&mut redis)
            .await
            .context("Failed to atomically create lobby metadata")?;

        if created == 0 {
            return Ok(false);
        }
        if created == 2 {
            return Err(anyhow!(
                "Lobby code '{}' still belongs to an active matchmaking generation",
                lobby_code
            ));
        }

        let preferences = Self::resolve_join_preferences(requested_preferences);
        self.set_lobby_preferences(lobby_code, &preferences)
            .await
            .context("Failed to initialize lobby preferences")?;

        self.touch_lobby(lobby_code, None, false)
            .await
            .context("Failed to initialize lobby TTLs")?;

        info!(
            "Auto-created lobby '{}' for user {} in region {} (pool: {})",
            lobby_code, host_user_id, region, matchmaking_pool
        );

        Ok(true)
    }

    fn resolve_join_preferences(preferences: Option<&LobbyPreferences>) -> LobbyPreferences {
        preferences
            .map(Self::sanitize_lobby_preferences)
            .unwrap_or_default()
    }

    fn sanitize_lobby_preferences(preferences: &LobbyPreferences) -> LobbyPreferences {
        let mut seen = HashSet::new();
        let mut sanitized = Vec::new();

        for mode in &preferences.selected_modes {
            let normalized = mode.trim().to_lowercase();
            if normalized.is_empty() || !Self::is_valid_lobby_mode(&normalized) {
                continue;
            }

            if seen.insert(normalized.clone()) {
                sanitized.push(normalized);
            }
        }

        if sanitized.is_empty() {
            sanitized.extend(
                LobbyPreferences::default()
                    .selected_modes
                    .into_iter()
                    .map(|mode| mode.trim().to_lowercase()),
            );
        }

        LobbyPreferences {
            selected_modes: sanitized,
            competitive: preferences.competitive,
        }
    }

    fn is_valid_lobby_mode(mode: &str) -> bool {
        matches!(mode, "duel" | "2v2" | "solo" | "ffa")
    }

    fn validate_lobby_code(lobby_code: &str) -> Result<()> {
        if lobby_code.is_empty()
            || lobby_code.len() > 64
            || !lobby_code
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(anyhow!(
                "Lobby codes must be 1-64 ASCII letters, numbers, '-' or '_'"
            ));
        }
        Ok(())
    }

    /// Stop heartbeat and remove from Redis
    pub async fn leave_lobby(
        &self,
        lobby_code: &str,
        user_id: i32,
        _websocket_id: &str,
    ) -> Result<LeaveLobbyResult> {
        // This reservation atomically cancels a live break and prevents a new
        // one from starting until the cross-slot presence mutation completes.
        let member_user_id =
            u32::try_from(user_id).context("Lobby user ID must be non-negative")?;
        let membership_reservation = self
            .reserve_leave_membership_change(lobby_code, member_user_id, true)
            .await?;
        let leave_result: Result<LeaveLobbyResult> = async {
            let mut redis = self.redis.clone();
            let members_key = RedisKeys::lobby_members_set(lobby_code);

            // Explicit user intent supersedes every transport generation.
            // During make-before-break both the retired and replacement socket
            // may briefly have presence; remove every transport for this user.
            // Expiry cleanup and the remaining count share the member-slot
            // Redis clock that authored the scores.
            let user_prefix = format!("{user_id}:");
            let remaining: i64 = Script::new(REMOVE_USER_LOBBY_MEMBERS_SCRIPT)
                .key(&members_key)
                .arg(user_prefix)
                .invoke_async(&mut redis)
                .await
                .context("Failed to remove lobby member transports")?;

            if remaining == 0 {
                // Do not eagerly DEL cross-slot lobby keys. A concurrent join
                // can legitimately reuse this code between separate deletes,
                // causing an old leave to erase the new generation. Empty
                // lobby keys already carry leases and expire naturally; a
                // concurrent join refreshes them instead.
                Ok(LeaveLobbyResult::LobbyDeleted)
            } else {
                Ok(LeaveLobbyResult::StillActive)
            }
        }
        .await;

        let result = match leave_result {
            Ok(result) => result,
            Err(error) => {
                if let Some(field) = membership_reservation.as_deref() {
                    // The member-slot removal may have committed before an
                    // ambiguous error. Always close the write-side seqlock;
                    // if this fails, the reservation remains until expiry.
                    self.finalize_leave_membership_change(lobby_code, field, member_user_id)
                        .await?;
                }
                return Err(error);
            }
        };
        if let Some(field) = membership_reservation.as_deref() {
            // If the reservation aged out while the member-slot mutation was
            // running, BEGIN may have started a break. This metadata-slot
            // finalizer cancels that break before leave returns.
            self.finalize_leave_membership_change(lobby_code, field, member_user_id)
                .await?;
        }

        // Leaving is explicit intent, so retract the presence record now
        // rather than waiting out its lease and pointing invites at a lobby
        // this user has already left.
        if let Err(error) = self.clear_user_presence(member_user_id, lobby_code).await {
            warn!(
                lobby_code,
                user_id, "Failed to clear user presence on leave: {error:#}"
            );
        }

        if let Err(e) = self.publish_lobby_update(lobby_code).await {
            warn!(
                "Failed to publish lobby update after member leave for lobby '{}': {}",
                lobby_code, e
            );
        }

        Ok(result)
    }

    /// Get all active members of a lobby from Redis
    pub async fn get_lobby_members(&self, lobby_code: &str) -> Result<BTreeMap<u32, LobbyMember>> {
        let mut redis = self.redis.clone();
        let members_key = RedisKeys::lobby_members_set(lobby_code);

        // Cleanup and snapshot use one Redis clock and one member-slot script.
        // The returned minimum score is therefore a precise validity bound
        // for the later metadata-slot ad-break transition.
        let members_with_scores: Vec<(String, f64)> = Script::new(SNAPSHOT_LOBBY_MEMBERS_SCRIPT)
            .key(&members_key)
            .invoke_async(&mut redis)
            .await
            .context("Failed to snapshot active lobby members")?;

        let user_ids: Vec<u32> = members_with_scores
            .iter()
            .filter_map(|(member_value, _score)| {
                member_value
                    .split(':')
                    .nth(0)
                    .and_then(|id_str| id_str.parse::<u32>().ok())
            })
            .collect();

        let users = self
            .user_cache
            .get_all(&user_ids)
            .await?
            .iter()
            .flatten()
            .map(|u| (u.id as u32, u.username.clone()))
            .collect::<HashMap<u32, String>>();

        // Parse and deduplicate by user_id (keeping highest score = latest heartbeat)
        let mut members: BTreeMap<u32, LobbyMember> = BTreeMap::new();

        for (raw_member_value, score) in members_with_scores {
            if let Ok(member_value) = raw_member_value.parse::<MemberValue>() {
                let user_id = member_value.user_id;
                if let Some(user) = users.get(&user_id) {
                    let username = user.clone();

                    // Keep entry with highest score (most recent heartbeat)
                    members
                        .entry(user_id)
                        .and_modify(|existing| {
                            if score > existing.ts {
                                *existing = LobbyMember {
                                    user_id,
                                    username: username.clone(),
                                    ts: score,
                                    supports_ad_break: member_value.supports_ad_break,
                                    can_show_video_ad: member_value.can_show_video_ad,
                                    distribution: member_value.distribution,
                                }
                            }
                        })
                        .or_insert(LobbyMember {
                            user_id,
                            username: username.clone(),
                            ts: score,
                            supports_ad_break: member_value.supports_ad_break,
                            can_show_video_ad: member_value.can_show_video_ad,
                            distribution: member_value.distribution,
                        });
                } else {
                    warn!(
                        "Username not found in cache for user_id {} in lobby '{}'",
                        user_id, lobby_code
                    );
                    continue;
                }
            } else {
                warn!(
                    "Invalid member value format in lobby '{}': {}",
                    lobby_code, raw_member_value
                );
                continue;
            }
        }

        debug!(
            "Found {} unique members in lobby '{}'",
            members.len(),
            lobby_code
        );
        Ok(members)
    }

    /// Persist host-selected matchmaking preferences for the lobby
    pub async fn set_lobby_preferences(
        &self,
        lobby_code: &str,
        preferences: &LobbyPreferences,
    ) -> Result<()> {
        let mut redis = self.redis.clone();
        let key = RedisKeys::lobby_preferences(lobby_code);
        let payload =
            serde_json::to_string(preferences).context("Failed to serialize lobby preferences")?;

        redis
            .set_ex::<_, _, ()>(
                &key,
                payload,
                u64::try_from(LOBBY_ANCILLARY_TTL_SECS).expect("positive ancillary lobby TTL"),
            )
            .await
            .context("Failed to store lobby preferences")?;

        if let Err(e) = self.publish_lobby_update(lobby_code).await {
            warn!(
                "Failed to publish lobby update after preferences change for lobby '{}': {}",
                lobby_code, e
            );
        }

        Ok(())
    }

    /// Retrieve matchmaking preferences for the lobby, falling back to defaults
    pub async fn get_lobby_preferences(&self, lobby_code: &str) -> Result<LobbyPreferences> {
        let mut redis = self.redis.clone();
        let key = RedisKeys::lobby_preferences(lobby_code);
        let raw: Option<String> = redis
            .get(key)
            .await
            .context("Failed to load lobby preferences")?;

        if let Some(json) = raw {
            match serde_json::from_str::<LobbyPreferences>(&json) {
                Ok(preferences) => Ok(preferences),
                Err(e) => {
                    warn!(
                        "Failed to parse lobby preferences for lobby '{}': {}",
                        lobby_code, e
                    );
                    Ok(LobbyPreferences::default())
                }
            }
        } else {
            Ok(LobbyPreferences::default())
        }
    }

    /// Atomically begin one durable ad break. Concurrent or replayed queue
    /// requests adopt the first break instead of replacing its roster/request.
    pub async fn begin_ad_break(
        &self,
        lobby_code: &str,
        ad_break: &LobbyAdBreak,
        expected_membership_revision: i64,
        membership_valid_until_ms: i64,
        timeout: Duration,
    ) -> Result<BeginAdBreakResult> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        ad_break
            .validate_new(now_ms)
            .context("Refusing to persist an invalid lobby ad break")?;
        let metadata_key = RedisKeys::lobby_metadata(lobby_code);
        let payload =
            serde_json::to_string(ad_break).context("Failed to serialize lobby ad break")?;
        let mut redis = self.redis.clone();
        let script = Script::new(BEGIN_AD_BREAK_SCRIPT);
        let mut invocation = script.prepare_invoke();
        invocation
            .key(metadata_key)
            .key(RedisKeys::lobby_membership_reservations(lobby_code))
            .key(RedisKeys::matchmaking_lobby_active_game(lobby_code));
        for participant_user_id in &ad_break.participant_user_ids {
            invocation
                .key(RedisKeys::matchmaking_user_queue_identity(
                    *participant_user_id,
                ))
                .key(RedisKeys::matchmaking_user_active_game(
                    *participant_user_id,
                ));
        }
        let (code, detail): (i64, String) = invocation
            .arg(&payload)
            .arg(expected_membership_revision)
            .arg(membership_valid_until_ms)
            .arg(i64::try_from(timeout.as_millis()).context("Ad-break timeout is too large")?)
            .arg(LOBBY_MEMBER_LEASE_TTL_MS)
            .invoke_async(&mut redis)
            .await
            .context("Failed to begin lobby ad break")?;

        let active: LobbyAdBreak = match code {
            1 | 2 => {
                serde_json::from_str(&detail).context("Failed to decode active lobby ad break")?
            }
            3 => return Err(anyhow!("Lobby cannot start an ad break while {detail}")),
            4..=6 => return Ok(BeginAdBreakResult::MembershipChanged),
            7 => {
                return Err(anyhow!(
                    "Lobby cannot start an ad break because a participant is already queued or matched"
                ));
            }
            _ => return Err(anyhow!("Failed to begin lobby ad break: {detail}")),
        };
        active
            .validate()
            .context("Redis returned invalid lobby ad-break state")?;

        if code == 1
            && let Err(error) = self.publish_lobby_update(lobby_code).await
        {
            // Pub/sub is only a latency hint. Returning the committed break is
            // essential so the caller still arms its deadline task.
            warn!(
                lobby_code,
                %error,
                "Failed to publish new ad break; authoritative reconciliation will recover"
            );
        }
        Ok(BeginAdBreakResult::Active {
            ad_break: active,
            created: code == 1,
        })
    }

    /// Record one terminal provider outcome. Once the deadline passes, every
    /// unresolved participant is resolved as timed out so legacy, blocked, or
    /// disconnected clients can never hold matchmaking forever.
    pub async fn resolve_ad_break(
        &self,
        lobby_code: &str,
        break_id: &str,
        user_id: u32,
        resolution: AdBreakResolution,
    ) -> Result<AdBreakResolutionResult> {
        let metadata_key = RedisKeys::lobby_metadata(lobby_code);
        let force_timeout = user_id == 0 && resolution == AdBreakResolution::TimedOut;
        let mut redis = self.redis.clone();
        let (code, detail): (i64, String) = Script::new(RESOLVE_AD_BREAK_SCRIPT)
            .key(metadata_key)
            .arg(break_id)
            .arg(user_id)
            .arg(resolution.as_str())
            .arg(if force_timeout { 1 } else { 0 })
            .invoke_async(&mut redis)
            .await
            .context("Failed to resolve lobby ad break")?;

        let result = match code {
            0 => AdBreakResolutionResult::Stale,
            1..=4 => {
                let ad_break: LobbyAdBreak = serde_json::from_str(&detail)
                    .context("Failed to decode resolved lobby ad break")?;
                ad_break
                    .validate()
                    .context("Redis returned invalid resolved ad-break state")?;
                match code {
                    1 => AdBreakResolutionResult::Pending(ad_break),
                    2 => AdBreakResolutionResult::Ready(ad_break),
                    3 => AdBreakResolutionResult::NotDue(ad_break),
                    4 => AdBreakResolutionResult::NoChange(ad_break),
                    _ => unreachable!(),
                }
            }
            _ => return Err(anyhow!("Unknown lobby ad-break result {code}: {detail}")),
        };

        if matches!(
            &result,
            AdBreakResolutionResult::Pending(_) | AdBreakResolutionResult::Ready(_)
        ) && let Err(error) = self.publish_lobby_update(lobby_code).await
        {
            warn!(
                lobby_code,
                %error,
                "Failed to publish ad-break resolution; authoritative reconciliation will recover"
            );
        }
        Ok(result)
    }

    pub async fn cancel_ad_break(&self, lobby_code: &str, break_id: &str) -> Result<bool> {
        let mut redis = self.redis.clone();
        let cancelled: i64 = Script::new(CANCEL_AD_BREAK_SCRIPT)
            .key(RedisKeys::lobby_metadata(lobby_code))
            .arg(break_id)
            .invoke_async(&mut redis)
            .await
            .context("Failed to cancel lobby ad break")?;
        if cancelled == 1 {
            if let Err(error) = self.publish_lobby_update(lobby_code).await {
                warn!(
                    lobby_code,
                    %error,
                    "Failed to publish ad-break cancellation; authoritative reconciliation will recover"
                );
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Cancel only when the authenticated caller belongs to the persisted
    /// break roster. This prevents a superseded lobby transport from using a
    /// stale handle to cancel a later generation's barrier.
    pub async fn cancel_ad_break_for_participant(
        &self,
        lobby_code: &str,
        break_id: &str,
        user_id: u32,
    ) -> Result<bool> {
        let mut redis = self.redis.clone();
        let cancelled: i64 = Script::new(CANCEL_AD_BREAK_FOR_PARTICIPANT_SCRIPT)
            .key(RedisKeys::lobby_metadata(lobby_code))
            .arg(break_id)
            .arg(user_id)
            .invoke_async(&mut redis)
            .await
            .context("Failed to cancel participant lobby ad break")?;
        if cancelled == 1 {
            if let Err(error) = self.publish_lobby_update(lobby_code).await {
                warn!(
                    lobby_code,
                    %error,
                    "Failed to publish participant ad-break cancellation; authoritative reconciliation will recover"
                );
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Remove the completed break payload after atomic matchmaking admission.
    /// The queue script owns the state transition to `queued`.
    pub async fn clear_ad_break(&self, lobby_code: &str, break_id: &str) -> Result<bool> {
        let mut redis = self.redis.clone();
        let cleared: i64 = Script::new(CLEAR_AD_BREAK_SCRIPT)
            .key(RedisKeys::lobby_metadata(lobby_code))
            .arg(break_id)
            .invoke_async(&mut redis)
            .await
            .context("Failed to clear completed lobby ad break")?;
        Ok(cleared == 1)
    }

    /// Claim a short distributed lease before roster/database work. Every
    /// connected socket may notice a resolved deadline, but only one gateway
    /// should fan that observation into expensive admission finalization.
    pub async fn claim_ad_break_finalization(
        self: &Arc<Self>,
        lobby_code: &str,
        break_id: &str,
    ) -> Result<Option<AdBreakFinalizationLease>> {
        let claim_key = RedisKeys::lobby_ad_break_finalization_claim(lobby_code);
        let metadata_key = RedisKeys::lobby_metadata(lobby_code);
        let owner_token = uuid::Uuid::new_v4().to_string();
        let mut redis = self.redis.clone();
        let claimed: i64 = Script::new(CLAIM_AD_BREAK_FINALIZATION_SCRIPT)
            .key(&metadata_key)
            .key(&claim_key)
            .arg(break_id)
            .arg(&owner_token)
            .arg(AD_BREAK_FINALIZATION_LEASE_TTL_MS)
            .invoke_async(&mut redis)
            .await
            .context("Failed to claim lobby ad-break finalization")?;
        if claimed != 1 {
            return Ok(None);
        }

        let manager_for_renewal = Arc::clone(self);
        let metadata_for_renewal = metadata_key.clone();
        let key_for_renewal = claim_key.clone();
        let break_for_renewal = break_id.to_owned();
        let token_for_renewal = owner_token.clone();
        let renewal_task = tokio::spawn(async move {
            loop {
                sleep(AD_BREAK_FINALIZATION_RENEW_INTERVAL).await;
                match manager_for_renewal
                    .renew_ad_break_finalization(
                        &metadata_for_renewal,
                        &key_for_renewal,
                        &break_for_renewal,
                        &token_for_renewal,
                    )
                    .await
                {
                    Ok(true) => {}
                    Ok(false) => break,
                    Err(error) => {
                        warn!(%error, "Failed to renew lobby ad-break finalization lease");
                    }
                }
            }
        });
        Ok(Some(AdBreakFinalizationLease {
            lobby_manager: Arc::clone(self),
            claim_key,
            owner_token,
            renewal_task: Some(renewal_task),
            released: false,
        }))
    }

    async fn renew_ad_break_finalization(
        &self,
        metadata_key: &str,
        claim_key: &str,
        break_id: &str,
        owner_token: &str,
    ) -> Result<bool> {
        let mut redis = self.redis.clone();
        let renewed: i64 = Script::new(RENEW_AD_BREAK_FINALIZATION_SCRIPT)
            .key(metadata_key)
            .key(claim_key)
            .arg(break_id)
            .arg(owner_token)
            .arg(AD_BREAK_FINALIZATION_LEASE_TTL_MS)
            .invoke_async(&mut redis)
            .await
            .context("Failed to renew ad-break finalization claim")?;
        Ok(renewed == 1)
    }

    async fn release_ad_break_finalization(
        &self,
        claim_key: &str,
        owner_token: &str,
    ) -> Result<()> {
        let mut redis = self.redis.clone();
        Script::new(RELEASE_AD_BREAK_FINALIZATION_SCRIPT)
            .key(claim_key)
            .arg(owner_token)
            .invoke_async::<i64>(&mut redis)
            .await
            .context("Failed to release ad-break finalization claim")?;
        Ok(())
    }

    /// Refresh the exact tentative member without shortening its longer join
    /// lease. The metadata check remains first to preserve the generation
    /// boundary across Redis slots.
    async fn touch_provisional_lobby_member(
        &self,
        lobby_code: &str,
        member: &MemberValue,
    ) -> Result<()> {
        let mut redis = self.redis.clone();
        let extended: i64 = Script::new(EXTEND_KEY_TTL_SCRIPT)
            .key(RedisKeys::lobby_metadata(lobby_code))
            .arg(LOBBY_METADATA_IDLE_TTL_MS)
            .arg(1)
            .invoke_async(&mut redis)
            .await
            .context("Failed to extend provisional lobby metadata TTL")?;
        if extended != 1 {
            return Err(anyhow!("Lobby metadata no longer exists"));
        }
        self.touch_lobby_member(
            lobby_code,
            member,
            true,
            TENTATIVE_LOBBY_MEMBER_LEASE_TTL_MS,
        )
        .await?;
        self.refresh_lobby_queue_lease(lobby_code, member.user_id)
            .await
    }

    async fn touch_lobby(
        &self,
        lobby_code: &str,
        member: Option<MemberValue>,
        refresh_only: bool,
    ) -> Result<()> {
        let mut redis = self.redis.clone();
        let members_key = RedisKeys::lobby_members_set(lobby_code);
        let metadata_key = RedisKeys::lobby_metadata(lobby_code);
        let heartbeat_user_id = member.as_ref().map(|member| member.user_id);

        // For heartbeats, preserve and verify the metadata generation before
        // extending a cross-slot member. The longer metadata lease guarantees
        // a late member write expires before this lobby code can be recreated.
        if refresh_only {
            let extended: i64 = Script::new(EXTEND_KEY_TTL_SCRIPT)
                .key(&metadata_key)
                .arg(LOBBY_METADATA_IDLE_TTL_MS)
                .arg(1)
                .invoke_async(&mut redis)
                .await
                .context("Failed to extend lobby metadata TTL")?;
            if extended != 1 {
                return Err(anyhow!("Lobby metadata no longer exists"));
            }
        }

        if let Some(member) = member {
            let lease_ttl_ms = if refresh_only {
                LOBBY_MEMBER_LEASE_TTL_MS
            } else {
                TENTATIVE_LOBBY_MEMBER_LEASE_TTL_MS
            };
            self.touch_lobby_member(lobby_code, &member, refresh_only, lease_ttl_ms)
                .await?;
        } else {
            redis
                .expire::<_, ()>(&members_key, 30)
                .await
                .context("Failed to set TTL on lobby members set")?;
        }

        if !refresh_only {
            let extended: i64 = Script::new(EXTEND_KEY_TTL_SCRIPT)
                .key(&metadata_key)
                .arg(LOBBY_METADATA_IDLE_TTL_MS)
                .arg(0)
                .invoke_async(&mut redis)
                .await
                .context("Failed to extend lobby metadata TTL")?;
            if extended != 1 {
                return Err(anyhow!("Lobby metadata no longer exists"));
            }
        }

        if refresh_only && let Some(user_id) = heartbeat_user_id {
            match timeout(
                LOBBY_ANCILLARY_HEARTBEAT_TIMEOUT,
                self.refresh_lobby_queue_lease(lobby_code, user_id),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => warn!(
                    lobby_code,
                    %error,
                    "Failed to refresh exact lobby queue lease"
                ),
                Err(_) => warn!(lobby_code, "Timed out refreshing exact lobby queue lease"),
            }
        }

        let preferences_key = RedisKeys::lobby_preferences(lobby_code);
        let chat_history_key = RedisKeys::lobby_chat_history_key(lobby_code);
        let refresh_ancillary_ttls = async {
            redis
                .expire::<_, ()>(&preferences_key, LOBBY_ANCILLARY_TTL_SECS)
                .await
                .context("Failed to set TTL on lobby preferences")?;
            redis
                .expire::<_, ()>(&chat_history_key, LOBBY_ANCILLARY_TTL_SECS)
                .await
                .context("Failed to set TTL on lobby chat history")?;
            Result::<()>::Ok(())
        };

        if refresh_only {
            // Presence and metadata are the heartbeat's safety-critical
            // leases. Preferences/chat retention is useful but must neither
            // consume the member lease during a slow Redis response nor tear
            // down an otherwise healthy websocket scope.
            match timeout(LOBBY_ANCILLARY_HEARTBEAT_TIMEOUT, refresh_ancillary_ttls).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => warn!(
                    lobby_code,
                    %error,
                    "Failed to refresh ancillary lobby TTLs"
                ),
                Err(_) => warn!(lobby_code, "Timed out refreshing ancillary lobby TTLs"),
            }
        } else {
            refresh_ancillary_ttls.await?;
        }

        Ok(())
    }

    async fn refresh_lobby_queue_lease(&self, lobby_code: &str, user_id: u32) -> Result<()> {
        let mut redis = self.redis.clone();
        let refreshed: i64 = Script::new(REFRESH_LOBBY_QUEUE_LEASE_SCRIPT)
            .key(RedisKeys::lobby_metadata(lobby_code))
            .key(RedisKeys::matchmaking_lobby_queue_identity(lobby_code))
            .key(RedisKeys::matchmaking_lobby_queue_lease(lobby_code))
            .key(RedisKeys::matchmaking_lobby_queue_outcome(lobby_code))
            .arg(user_id)
            .arg(LOBBY_QUEUE_LEASE_TTL_MS)
            .arg(LOBBY_QUEUE_OUTCOME_TTL_MS)
            .invoke_async(&mut redis)
            .await
            .context("Failed to refresh lobby queue lease")?;
        match refreshed {
            1 | 2 => Ok(()),
            _ => Err(anyhow!(
                "Queued lobby identity no longer contains this heartbeat user"
            )),
        }
    }

    /// Commit only the cross-slot presence write. Join finalization calls this
    /// directly so ancillary TTL refreshes cannot consume multiple Redis
    /// response windows while the membership reservation is open.
    async fn touch_lobby_member(
        &self,
        lobby_code: &str,
        member: &MemberValue,
        refresh_only: bool,
        lease_ttl_ms: i64,
    ) -> Result<()> {
        let mut redis = self.redis.clone();
        let user_prefix = format!("{}:", member.user_id);
        let (touched, expires_at): (i64, i64) = Script::new(TOUCH_LOBBY_MEMBER_SCRIPT)
            .key(RedisKeys::lobby_members_set(lobby_code))
            .arg(member.to_string())
            .arg(lease_ttl_ms)
            .arg(if refresh_only { 1 } else { 0 })
            .arg(user_prefix)
            .arg(MAX_LOBBY_MEMBERS)
            .invoke_async(&mut redis)
            .await
            .context("Failed to update lobby member lease")?;
        match touched {
            1 => {}
            2 => return Err(anyhow!("Lobby is full")),
            _ => return Err(anyhow!("Lobby member lease no longer exists")),
        }
        debug!(lobby_code, expires_at, "Touched lobby member lease");

        // Presence rides the membership lease it describes: renewed by the
        // same heartbeat, expiring on the same silence. It is deliberately
        // best-effort — a `/play/<username>` invite going cold is not a reason
        // to tear down a healthy lobby membership.
        if let Err(error) = self
            .publish_user_presence(member.user_id, lobby_code, lease_ttl_ms)
            .await
        {
            warn!(
                lobby_code,
                user_id = member.user_id,
                "Failed to refresh user presence: {error:#}"
            );
        }

        Ok(())
    }

    /// Point a user's presence record at the lobby they are currently in.
    async fn publish_user_presence(
        &self,
        user_id: u32,
        lobby_code: &str,
        lease_ttl_ms: i64,
    ) -> Result<()> {
        let lease_ttl_ms =
            u64::try_from(lease_ttl_ms).context("Presence lease must be positive")?;
        self.redis
            .clone()
            .pset_ex::<_, _, ()>(RedisKeys::user_presence(user_id), lobby_code, lease_ttl_ms)
            .await
            .context("Failed to write user presence")
    }

    /// Drop a user's presence record, but only while it still points at the
    /// lobby they are leaving.
    ///
    /// A player who leaves one lobby and joins another can have the new
    /// join's presence write land before the old leave's clear. Comparing
    /// before deleting stops that late clear from erasing presence for a lobby
    /// the user is legitimately in.
    async fn clear_user_presence(&self, user_id: u32, lobby_code: &str) -> Result<()> {
        const CLEAR_PRESENCE_SCRIPT: &str = r#"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            end
            return 0
        "#;

        let mut redis = self.redis.clone();
        let _: i64 = Script::new(CLEAR_PRESENCE_SCRIPT)
            .key(RedisKeys::user_presence(user_id))
            .arg(lobby_code)
            .invoke_async(&mut redis)
            .await
            .context("Failed to clear user presence")?;
        Ok(())
    }

    /// The lobby a user is currently present in, if any.
    pub async fn get_user_lobby_code(&self, user_id: u32) -> Result<Option<String>> {
        self.redis
            .clone()
            .get::<_, Option<String>>(RedisKeys::user_presence(user_id))
            .await
            .context("Failed to read user presence")
    }

    /// Update lobby state in Redis
    pub async fn update_lobby_state(&self, lobby_code: &str, state: &str) -> Result<()> {
        use redis::AsyncCommands;
        let metadata_key = RedisKeys::lobby_metadata(lobby_code);

        let mut redis = self.redis.clone();
        redis
            .hset::<_, _, _, ()>(&metadata_key, "state", state)
            .await
            .context("Failed to update lobby state in Redis")?;

        info!("Updated lobby '{}' state to {}", lobby_code, state);

        if let Err(e) = self.publish_lobby_update(lobby_code).await {
            warn!(
                "Failed to publish lobby update after state change for lobby '{}': {}",
                lobby_code, e
            );
        }

        Ok(())
    }

    /// Helper to remove a key from Redis
    #[allow(dead_code)]
    async fn remove_from_redis(&mut self, key: &str) -> Result<()> {
        let _: () = self
            .redis
            .del(key)
            .await
            .context("Failed to delete Redis key")?;
        Ok(())
    }

    /// Map AWS region to 4-character code
    fn region_to_code(region: &str) -> String {
        // Configurable region code mapping
        // Format: AWS region string -> 4-character code
        match region {
            "us-east-1" => "USE1".to_string(),
            "eu-west-1" => "EUW1".to_string(),
            "ap-southeast-2" => "APS2".to_string(),
            "us-west-2" => "USW2".to_string(),
            // Add more regions as needed
            _ => {
                // For unknown regions, generate a code from the first letters
                // Example: "eu-central-1" -> "EUC1"
                let parts: Vec<&str> = region.split('-').collect();
                if parts.len() >= 2 {
                    let prefix: String = parts[0].chars().take(2).collect();
                    let suffix: String = parts[1].chars().take(1).collect();
                    let number = parts.get(2).unwrap_or(&"1");
                    format!(
                        "{}{}{}",
                        prefix.to_uppercase(),
                        suffix.to_uppercase(),
                        number
                    )
                } else {
                    // Fallback to first 4 characters
                    region.chars().take(4).collect::<String>().to_uppercase()
                }
            }
        }
    }

    /// Generate a random lobby code with region prefix
    /// Format: {REGION_CODE}-{8_CHAR_HASH} (e.g., USE1-A3B2C4D5)
    fn generate_lobby_code(region: &str) -> String {
        use rand::Rng;
        const CHARSET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789"; // Exclude confusing chars
        let mut rng = rand::thread_rng();

        let region_code = Self::region_to_code(region);
        let hash: String = (0..8)
            .map(|_| {
                let idx = rng.gen_range(0..CHARSET.len());
                CHARSET[idx] as char
            })
            .collect();

        format!("{}-{}", region_code, hash)
    }

    /// Generate a unique lobby code with collision detection
    /// Retries up to max_attempts times if a collision is detected
    async fn generate_unique_lobby_code(
        &self,
        region: &str,
        max_attempts: usize,
    ) -> Result<String> {
        let mut redis = self.redis.clone();

        for attempt in 0..max_attempts {
            let code = Self::generate_lobby_code(region);
            let generation_keys = [
                RedisKeys::lobby_metadata(&code),
                RedisKeys::matchmaking_lobby_queue_identity(&code),
                RedisKeys::matchmaking_lobby_active_game(&code),
            ];

            let existing_generation_keys: i64 = redis::cmd("EXISTS")
                .arg(&generation_keys)
                .query_async(&mut redis)
                .await
                .context("Failed to check lobby generation existence")?;

            if existing_generation_keys == 0 {
                debug!(
                    "Generated unique lobby code '{}' on attempt {}",
                    code,
                    attempt + 1
                );
                return Ok(code);
            }

            warn!(
                "Lobby code collision on attempt {}/{}: {}",
                attempt + 1,
                max_attempts,
                code
            );
        }

        Err(anyhow!(
            "Failed to generate unique lobby code after {} attempts",
            max_attempts
        ))
    }

    /// Publish a lobby update to the lobby's Redis pub/sub channel
    pub async fn publish_lobby_update(&self, lobby_code: &str) -> Result<()> {
        let payload = match self.get_lobby_opt(lobby_code).await? {
            Some(lobby) => serde_json::to_string(&LobbyUpdate { lobby })
                .context("Failed to serialize lobby for update notification")?,
            None => serde_json::to_string(&LobbyDelete {
                lobby_code: lobby_code.to_string(),
                state: "deleted".to_string(),
            })
            .context("Failed to serialize lobby deletion notification")?,
        };
        let _: () = self
            .redis
            .clone()
            .publish(RedisKeys::lobby_updates_channel(), payload)
            .await
            .context("Failed to publish lobby update")?;
        debug!("Published update notification to lobby '{}'", lobby_code);
        Ok(())
    }

    /// Check if a user leads a lobby.
    ///
    /// This resolves the *effective* host via [`Self::get_lobby_opt`] rather
    /// than reading `hostUserId` directly, so authorization agrees with the
    /// `host_user_id` clients were last shown in a `LobbyUpdate`. Reading the
    /// raw field would deny every member of a lobby whose host has left but
    /// whose succession has not been resolved yet.
    ///
    /// A lobby that no longer exists has no host, so this is `false` rather
    /// than an error: the caller's own missing-lobby handling is the better
    /// place to report that.
    pub async fn is_lobby_host(&self, lobby_code: &str, user_id: i32) -> Result<bool> {
        Ok(self
            .get_lobby_opt(lobby_code)
            .await?
            .is_some_and(|lobby| lobby.host_user_id == user_id))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BTreeMap, LOBBY_LEASE_CLOCK_SKEW_ALLOWANCE_MS, LobbyMember, MemberValue,
        lobby_host_successor, lobby_membership_valid_until_ms,
    };

    #[test]
    fn capability_is_in_memory_only_and_preserves_queued_identity_json() {
        let legacy = r#"{"user_id":7,"username":"snake","ts":123.0}"#;
        let mut member: LobbyMember = serde_json::from_str(legacy).unwrap();
        assert!(!member.supports_ad_break);
        assert!(!member.can_show_video_ad);
        assert!(member.distribution.is_none());

        member.supports_ad_break = true;
        member.can_show_video_ad = true;
        member.distribution = Some(crate::ads::ClientDistribution::Web);
        assert_eq!(serde_json::to_string(&member).unwrap(), legacy);

        let round_trip: LobbyMember =
            serde_json::from_str(&serde_json::to_string(&member).unwrap()).unwrap();
        assert!(!round_trip.supports_ad_break);
        assert!(!round_trip.can_show_video_ad);
        assert!(round_trip.distribution.is_none());
    }

    #[test]
    fn redis_member_value_preserves_video_capability_and_reads_old_formats_safely() {
        let member = MemberValue {
            user_id: 7,
            websocket_id: "socket-a".into(),
            supports_ad_break: true,
            can_show_video_ad: true,
            distribution: Some(crate::ads::ClientDistribution::Web),
        };
        assert_eq!(member.to_string(), "7:socket-a:web:1:1");
        let decoded: MemberValue = member.to_string().parse().unwrap();
        assert!(decoded.supports_ad_break);
        assert!(decoded.can_show_video_ad);
        assert_eq!(
            decoded.distribution,
            Some(crate::ads::ClientDistribution::Web)
        );

        let previous: MemberValue = "7:socket-a:1".parse().unwrap();
        assert!(previous.supports_ad_break);
        assert!(!previous.can_show_video_ad);
        assert!(previous.distribution.is_none());

        let legacy: MemberValue = "7:socket-a".parse().unwrap();
        assert!(!legacy.supports_ad_break);
        assert!(!legacy.can_show_video_ad);
        assert!(legacy.distribution.is_none());

        let inconsistent: MemberValue = "7:socket-a:1:0".parse().unwrap();
        assert!(!inconsistent.supports_ad_break);
        assert!(!inconsistent.can_show_video_ad);
        assert!(inconsistent.distribution.is_none());
    }

    #[test]
    fn membership_snapshot_bound_is_conservative_and_rejects_bad_scores() {
        let members = [
            LobbyMember {
                user_id: 1,
                username: "one".into(),
                ts: 50_000.0,
                supports_ad_break: true,
                can_show_video_ad: true,
                distribution: Some(crate::ads::ClientDistribution::Web),
            },
            LobbyMember {
                user_id: 2,
                username: "two".into(),
                ts: 49_000.0,
                supports_ad_break: true,
                can_show_video_ad: false,
                distribution: Some(crate::ads::ClientDistribution::Itch),
            },
        ];
        assert_eq!(
            lobby_membership_valid_until_ms(&members).unwrap(),
            49_000 - LOBBY_LEASE_CLOCK_SKEW_ALLOWANCE_MS
        );

        let mut invalid = members[0].clone();
        invalid.ts = f64::NAN;
        assert!(lobby_membership_valid_until_ms([&invalid]).is_err());
        assert!(lobby_membership_valid_until_ms(std::iter::empty()).is_err());
    }

    fn roster(user_ids: &[u32]) -> BTreeMap<u32, LobbyMember> {
        user_ids
            .iter()
            .map(|&user_id| {
                (
                    user_id,
                    LobbyMember {
                        user_id,
                        username: format!("user{user_id}"),
                        ts: 50_000.0,
                        supports_ad_break: true,
                        can_show_video_ad: false,
                        distribution: None,
                    },
                )
            })
            .collect()
    }

    /// Leader-gated controls are only safe because leadership always lands
    /// somewhere: a lobby whose host left must still be able to pick a mode.
    #[test]
    fn host_succession_promotes_only_when_the_stored_host_is_gone() {
        // Present host: nothing to do, even with lower ids alongside them.
        assert_eq!(lobby_host_successor(7, &roster(&[3, 7, 9])), None);

        // Absent host: the lowest active id inherits.
        assert_eq!(lobby_host_successor(7, &roster(&[3, 9])), Some(3));
        assert_eq!(lobby_host_successor(7, &roster(&[9])), Some(9));

        // The choice is a property of the roster, not of iteration order or
        // of which gateway happened to observe the departure, so every server
        // reaches the same answer.
        assert_eq!(lobby_host_successor(7, &roster(&[9, 3, 5])), Some(3));
    }

    #[test]
    fn host_succession_leaves_an_empty_lobby_alone() {
        // Nobody to promote. Keeping the record means a host who reconnects
        // into their own draining lobby is still its leader.
        assert_eq!(lobby_host_successor(7, &roster(&[])), None);
    }

    /// Host ids come from `i32` while the roster is keyed by `u32`. A stored
    /// host that cannot be a member id is treated as absent rather than
    /// panicking or silently matching.
    #[test]
    fn host_succession_treats_an_unrepresentable_host_as_absent() {
        assert_eq!(lobby_host_successor(-1, &roster(&[4])), Some(4));
        assert_eq!(lobby_host_successor(-1, &roster(&[])), None);
    }
}
