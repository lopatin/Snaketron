//! Who is online, per region.
//!
//! Before this existed the only "players online" signal was an aggregate
//! integer derived from per-task socket counters. Naming the actual people
//! needs a real registry, and the registry has to survive a task dying without
//! running cleanup — so it is a *lease*, refreshed while the socket lives,
//! never a set-on-connect/delete-on-disconnect pair. That is the same shape
//! every other liveness structure here uses (server metrics, lobby membership,
//! partition leases), for the same reason.
//!
//! Storage is one hash plus one expiry ZSET per region, sharing a hash tag so
//! a single Lua script can prune and read atomically. Enumerating with SCAN is
//! deliberately avoided: redis-rs routes a cluster SCAN to one node and would
//! silently omit users held on another shard.

use anyhow::{Context, Result};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::matchmaking_pool::MatchmakingPool;
use crate::redis_keys::RedisKeys;
use crate::redis_utils::RedisConnection;

/// How long a presence lease survives without a refresh. Comfortably longer
/// than the refresh interval so an ordinary scheduling hiccup never blinks a
/// player out of the roster.
pub const PRESENCE_LEASE_MS: u64 = 30_000;
/// Cadence at which a live socket re-asserts its lease.
pub const PRESENCE_REFRESH_INTERVAL_MS: u64 = 10_000;
/// Upper bound on how many players one roster frame names. The panel is a
/// social prompt, not a directory; an unbounded roster in a busy region would
/// be a large frame pushed to every socket.
pub const MAX_ROSTER_PLAYERS: usize = 60;

/// What a player is doing right now, so the panel can say whether challenging
/// them is likely to land.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum PresenceActivity {
    /// Connected and not committed to anything.
    Idle,
    /// Sitting in a lobby, or queued for a match.
    Lobby,
    /// In a live match.
    Playing,
}

impl PresenceActivity {
    fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Lobby => "lobby",
            Self::Playing => "playing",
        }
    }
}

/// One entry in the region roster. Keyed on `user_id`, because a nickname is
/// not a stable identifier — guests may rename themselves at will.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct OnlinePlayer {
    pub user_id: u32,
    pub username: String,
    pub is_guest: bool,
    pub activity: PresenceActivity,
}

/// The roster as sent to one client, with the viewer already removed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct RegionRoster {
    pub region: String,
    pub players: Vec<OnlinePlayer>,
    /// Total live presences in the region, which may exceed `players.len()`
    /// once the roster is capped.
    pub total_online: u32,
}

/// Prune expired leases, then return the surviving roster. Writing the expiry
/// index before the record means a crash between the two leaves an orphaned
/// index member (harmless, self-pruning) rather than a record that is counted
/// forever.
const REFRESH_PRESENCE_SCRIPT: &str = r#"
local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
redis.call('ZADD', KEYS[2], now_ms + tonumber(ARGV[3]), ARGV[1])
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
-- Ownership is recorded beside the record, so a retiring socket can tell
-- whether it is still the one holding this lease.
redis.call('HSET', KEYS[1], ARGV[4], ARGV[5])
-- Keep the region's keys from outliving the last player in it.
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[3]) * 4)
redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[3]) * 4)
return 1
"#;

/// Give up a lease only if this connection still owns it.
///
/// Sockets hand over mid-session: a make-before-break replacement connects and
/// claims the lease before the retiring one tears down. Without this check the
/// old socket's cleanup would delete the new socket's presence and blink a
/// player who never left out of the roster.
const RELEASE_PRESENCE_SCRIPT: &str = r#"
if redis.call('HGET', KEYS[1], ARGV[2]) ~= ARGV[3] then
    return 0
end
redis.call('HDEL', KEYS[1], ARGV[1], ARGV[2])
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
"#;

const READ_PRESENCE_SCRIPT: &str = r#"
local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now_ms)
if #expired > 0 then
    redis.call('HDEL', KEYS[1], unpack(expired))
    redis.call('ZREM', KEYS[2], unpack(expired))
end
return redis.call('HGETALL', KEYS[1])
"#;

/// Hash field holding which socket currently owns a user's lease.
fn owner_field(user_id: u32) -> String {
    format!("owner:{user_id}")
}

#[derive(Clone)]
pub struct PresenceRegistry {
    redis: RedisConnection,
    region: String,
}

impl PresenceRegistry {
    pub fn new(redis: RedisConnection, region: impl Into<String>) -> Self {
        Self {
            redis,
            region: region.into(),
        }
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    /// Assert (or re-assert) that this user is online. Idempotent by design:
    /// the refresh loop, an activity change, and a reconnect all take the same
    /// path, and the last writer simply wins.
    pub async fn refresh(
        &self,
        user_id: u32,
        websocket_id: &str,
        username: &str,
        is_guest: bool,
        activity: PresenceActivity,
        pool: MatchmakingPool,
    ) -> Result<()> {
        // Stress traffic is a separate matchmaking universe; letting it into
        // the public roster would put unchallengeable load-test identities in
        // front of real players.
        if pool == MatchmakingPool::Stress {
            return Ok(());
        }

        let record = OnlinePlayer {
            user_id,
            username: username.to_string(),
            is_guest,
            activity,
        };
        let payload =
            serde_json::to_string(&record).context("failed to serialize a presence record")?;

        let mut connection = self.redis.clone();
        let result: i64 = redis::Script::new(REFRESH_PRESENCE_SCRIPT)
            .key(RedisKeys::presence_roster(&self.region))
            .key(RedisKeys::presence_expiry(&self.region))
            .arg(user_id.to_string())
            .arg(payload)
            .arg(PRESENCE_LEASE_MS)
            .arg(owner_field(user_id))
            .arg(websocket_id)
            .invoke_async(&mut connection)
            .await
            .context("failed to refresh a presence lease")?;
        anyhow::ensure!(result == 1, "presence refresh returned {result}");
        Ok(())
    }

    /// Drop a user from the roster on a clean disconnect, but only if this
    /// connection is still the one holding the lease.
    ///
    /// Returns whether the user actually left. A missed call is not a
    /// correctness problem — the lease expires on its own — it just makes the
    /// roster take up to `PRESENCE_LEASE_MS` to catch up. Returning `false`
    /// means a replacement socket has already taken over, which is the case
    /// callers must not treat as "this player went offline".
    pub async fn release(&self, user_id: u32, websocket_id: &str) -> Result<bool> {
        let mut connection = self.redis.clone();
        let released: i64 = redis::Script::new(RELEASE_PRESENCE_SCRIPT)
            .key(RedisKeys::presence_roster(&self.region))
            .key(RedisKeys::presence_expiry(&self.region))
            .arg(user_id.to_string())
            .arg(owner_field(user_id))
            .arg(websocket_id)
            .invoke_async(&mut connection)
            .await
            .context("failed to release a presence lease")?;
        Ok(released == 1)
    }

    /// The live roster, pruned of expired leases and ordered deterministically
    /// so two servers rendering the same region produce the same frame.
    pub async fn roster(&self) -> Result<RegionRoster> {
        let mut connection = self.redis.clone();
        let entries: Vec<(String, String)> = redis::Script::new(READ_PRESENCE_SCRIPT)
            .key(RedisKeys::presence_roster(&self.region))
            .key(RedisKeys::presence_expiry(&self.region))
            .invoke_async(&mut connection)
            .await
            .context("failed to read the region roster")?;

        Ok(build_roster(&self.region, entries))
    }

    /// Publish a roster-changed hint, but only when the roster actually moved.
    ///
    /// Every task in a region runs this loop, so an unconditional publish would
    /// fan N copies of the same frame to every socket every tick. The digest is
    /// compared with a Redis-side GETSET, which makes "did anything change"
    /// a cluster-wide question rather than a per-task one.
    pub async fn publish_roster_if_changed(&self, roster: &RegionRoster) -> Result<bool> {
        let digest = roster_digest(roster);
        let key = format!("{}:digest", RedisKeys::presence_roster(&self.region));
        let mut connection = self.redis.clone();
        let previous: Option<String> = redis::cmd("SET")
            .arg(&key)
            .arg(&digest)
            .arg("PX")
            .arg(PRESENCE_LEASE_MS * 4)
            .arg("GET")
            .query_async(&mut connection)
            .await
            .context("failed to compare the roster digest")?;
        if previous.as_deref() == Some(digest.as_str()) {
            return Ok(false);
        }

        let payload =
            serde_json::to_string(roster).context("failed to serialize the region roster")?;
        let _: () = connection
            .publish(RedisKeys::presence_updates_channel(&self.region), payload)
            .await
            .context("failed to publish the region roster")?;
        Ok(true)
    }

    /// Whether a specific user currently holds a live lease in this region.
    /// Used to reject a challenge aimed at someone who just went offline.
    pub async fn is_online(&self, user_id: u32) -> Result<bool> {
        let mut connection = self.redis.clone();
        let expires_at_ms: Option<f64> = connection
            .zscore(
                RedisKeys::presence_expiry(&self.region),
                user_id.to_string(),
            )
            .await
            .context("failed to read a presence lease")?;
        Ok(expires_at_ms
            .is_some_and(|deadline| deadline > chrono::Utc::now().timestamp_millis() as f64))
    }
}

/// Order by activity (idle players are the ones worth challenging), then by
/// name, then by id. Deterministic ordering is what lets the digest below mean
/// "the roster changed" rather than "the hash iterated differently this time".
pub(crate) fn build_roster(region: &str, entries: Vec<(String, String)>) -> RegionRoster {
    let mut players: Vec<OnlinePlayer> = entries
        .into_iter()
        // Ownership bookkeeping shares the hash with the records; it is not a
        // player and never parses as one, but skipping it by key is cheaper
        // and says why.
        .filter(|(field, _)| !field.starts_with("owner:"))
        .filter_map(|(_, payload)| serde_json::from_str::<OnlinePlayer>(&payload).ok())
        .collect();
    players.sort_by(|left, right| {
        activity_rank(left.activity)
            .cmp(&activity_rank(right.activity))
            .then_with(|| {
                left.username
                    .to_lowercase()
                    .cmp(&right.username.to_lowercase())
            })
            .then_with(|| left.user_id.cmp(&right.user_id))
    });
    let total_online = u32::try_from(players.len()).unwrap_or(u32::MAX);
    players.truncate(MAX_ROSTER_PLAYERS);

    RegionRoster {
        region: region.to_string(),
        players,
        total_online,
    }
}

fn activity_rank(activity: PresenceActivity) -> u8 {
    match activity {
        PresenceActivity::Idle => 0,
        PresenceActivity::Lobby => 1,
        PresenceActivity::Playing => 2,
    }
}

fn roster_digest(roster: &RegionRoster) -> String {
    let mut hasher = DefaultHasher::new();
    roster.total_online.hash(&mut hasher);
    for player in &roster.players {
        player.user_id.hash(&mut hasher);
        player.username.hash(&mut hasher);
        player.is_guest.hash(&mut hasher);
        player.activity.as_str().hash(&mut hasher);
    }
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(user_id: u32, username: &str, activity: PresenceActivity) -> (String, String) {
        let player = OnlinePlayer {
            user_id,
            username: username.to_string(),
            is_guest: false,
            activity,
        };
        (
            user_id.to_string(),
            serde_json::to_string(&player).expect("serializes"),
        )
    }

    #[test]
    fn roster_order_is_deterministic_and_prefers_available_players() {
        let roster = build_roster(
            "use1",
            vec![
                entry(3, "zoe", PresenceActivity::Idle),
                entry(1, "Ada", PresenceActivity::Playing),
                entry(2, "bob", PresenceActivity::Lobby),
                entry(4, "ada", PresenceActivity::Idle),
            ],
        );

        let order: Vec<u32> = roster.players.iter().map(|player| player.user_id).collect();
        assert_eq!(order, vec![4, 3, 2, 1]);
        assert_eq!(roster.total_online, 4);
    }

    #[test]
    fn roster_reports_the_true_population_when_capped() {
        let entries: Vec<(String, String)> = (0..MAX_ROSTER_PLAYERS as u32 + 25)
            .map(|id| entry(id, &format!("player{id:03}"), PresenceActivity::Idle))
            .collect();

        let roster = build_roster("use1", entries);
        assert_eq!(roster.players.len(), MAX_ROSTER_PLAYERS);
        assert_eq!(roster.total_online, MAX_ROSTER_PLAYERS as u32 + 25);
    }

    #[test]
    fn unreadable_records_are_skipped_rather_than_failing_the_read() {
        let roster = build_roster(
            "use1",
            vec![
                entry(1, "Ada", PresenceActivity::Idle),
                ("2".to_string(), "not json".to_string()),
            ],
        );
        assert_eq!(roster.players.len(), 1);
        assert_eq!(roster.total_online, 1);
    }

    /// The roster hash also carries per-user ownership bookkeeping. It must
    /// never surface as a phantom player.
    #[test]
    fn ownership_bookkeeping_is_not_a_player() {
        let roster = build_roster(
            "use1",
            vec![
                entry(1, "Ada", PresenceActivity::Idle),
                ("owner:1".to_string(), "socket-abc".to_string()),
            ],
        );
        assert_eq!(roster.players.len(), 1);
        assert_eq!(roster.total_online, 1);
    }

    /// The digest is what suppresses N duplicate publishes per region per tick,
    /// so it must be stable for an unchanged roster and move for any change a
    /// viewer would see.
    #[test]
    fn digest_tracks_visible_roster_changes_only() {
        let base = build_roster("use1", vec![entry(1, "Ada", PresenceActivity::Idle)]);
        let same = build_roster("use1", vec![entry(1, "Ada", PresenceActivity::Idle)]);
        assert_eq!(roster_digest(&base), roster_digest(&same));

        let renamed = build_roster("use1", vec![entry(1, "Ada2", PresenceActivity::Idle)]);
        assert_ne!(roster_digest(&base), roster_digest(&renamed));

        let busy = build_roster("use1", vec![entry(1, "Ada", PresenceActivity::Playing)]);
        assert_ne!(roster_digest(&base), roster_digest(&busy));

        let joined = build_roster(
            "use1",
            vec![
                entry(1, "Ada", PresenceActivity::Idle),
                entry(2, "Bob", PresenceActivity::Idle),
            ],
        );
        assert_ne!(roster_digest(&base), roster_digest(&joined));
    }
}
