//! Player-to-player challenges.
//!
//! A challenge is an invitation to the challenger's lobby, addressed at one
//! person instead of at whoever holds the link. That framing is deliberate: it
//! reuses the lobby the product already knows how to create, join, region-check
//! and matchmake from, so accepting a challenge lands both players somewhere
//! the rest of the system already understands.
//!
//! Delivery follows the pattern this codebase uses everywhere a message must
//! not be lost across servers: a **durable per-user record**, a **loss-tolerant
//! Pub/Sub hint** that only says "re-read your state", and a **periodic
//! reconcile** on the receiving socket. Pub/Sub alone is at-most-once, so it is
//! never what a state transition depends on.
//!
//! Each challenge is written into *both* participants' stores under the same
//! id. Every read, prune, and update then touches one user's two keys, which
//! share a hash tag — so no operation ever spans slots, even though a challenge
//! spans people.

use anyhow::{Context, Result};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

use crate::redis_keys::RedisKeys;
use crate::redis_utils::RedisConnection;

/// How long an unanswered challenge stands. Long enough to alt-tab back, short
/// enough that a stale invitation is not still sitting in an inbox an hour on.
pub const CHALLENGE_TTL_MS: i64 = 120_000;
/// How long a resolved challenge lingers so the other side can see the outcome
/// even if it was mid-reconnect when the answer landed.
pub const RESOLVED_CHALLENGE_LINGER_MS: i64 = 20_000;
/// Most challenges one player may have outstanding at once. This is the
/// anti-spam ceiling: it bounds both the challenger's fan-out and any one
/// inbox's size.
pub const MAX_OUTGOING_CHALLENGES: usize = 5;
pub const MAX_INCOMING_CHALLENGES: usize = 20;
/// Challenges one player may *issue* in a rolling window.
///
/// The concurrency ceilings above are not a rate limit: cancelling or being
/// declined frees a slot immediately, so `issue -> cancel -> issue` could
/// hammer a named victim indefinitely. This bounds the churn itself.
pub const CHALLENGE_RATE_LIMIT: usize = 12;
pub const CHALLENGE_RATE_WINDOW_MS: usize = 60_000;
/// The hint body, as a JSON string so subscribers can decode it.
pub const CHALLENGE_HINT_PAYLOAD: &str = "\"challenges\"";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum ChallengeState {
    Pending,
    Accepted,
    Declined,
    Cancelled,
}

/// One challenge, stored identically in both participants' stores. The client
/// decides whether it is incoming or outgoing by comparing `from_user_id` with
/// itself, so there is exactly one record shape and no direction flag to get
/// out of sync.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct Challenge {
    pub challenge_id: String,
    pub from_user_id: u32,
    pub from_username: String,
    pub to_user_id: u32,
    pub to_username: String,
    /// The challenger's lobby. Accepting means joining it.
    pub lobby_code: String,
    pub state: ChallengeState,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub created_at_ms: i64,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub expires_at_ms: i64,
}

impl Challenge {
    pub fn is_pending(&self, now_ms: i64) -> bool {
        self.state == ChallengeState::Pending && self.expires_at_ms > now_ms
    }
}

/// The complete challenge state for one user, sent as a snapshot rather than a
/// delta. A snapshot is idempotent across a socket handoff, a missed hint, and
/// a reconnect — all three simply re-render.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct ChallengeInbox {
    /// Challenges addressed at this user.
    pub incoming: Vec<Challenge>,
    /// Challenges this user issued.
    pub outgoing: Vec<Challenge>,
}

/// Why a challenge could not be issued. These are surfaced to the challenger
/// verbatim, so each one has to read as an explanation rather than a code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChallengeRejection {
    Self_,
    TargetOffline,
    AlreadyChallenged,
    TooManyOutgoing,
    TargetInboxFull,
    RateLimited,
}

impl ChallengeRejection {
    pub fn reason(self) -> &'static str {
        match self {
            Self::Self_ => "You cannot challenge yourself.",
            Self::TargetOffline => "That player just went offline.",
            Self::AlreadyChallenged => "You have already challenged that player.",
            Self::TooManyOutgoing => {
                "You have too many challenges out. Wait for a reply or cancel one."
            }
            Self::TargetInboxFull => "That player has too many challenges pending.",
            Self::RateLimited => "You are sending challenges too quickly. Wait a moment.",
        }
    }
}

/// Prune expired members, then return the survivors. Identical in shape to the
/// presence and active-server registries: expiry index and record hash share a
/// slot, so one script keeps them consistent.
const READ_CHALLENGES_SCRIPT: &str = r#"
local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now_ms)
if #expired > 0 then
    redis.call('HDEL', KEYS[1], unpack(expired))
    redis.call('ZREM', KEYS[2], unpack(expired))
end
return redis.call('HGETALL', KEYS[1])
"#;

/// Count one issued challenge inside a rolling window, and report whether the
/// caller is over budget. The window is a plain expiring counter rather than a
/// sorted set: the exact eviction boundary does not matter for an anti-spam
/// backstop, and one key beats one member per attempt.
const RATE_LIMIT_SCRIPT: &str = r#"
local count = redis.call('INCR', KEYS[1])
if count == 1 then
    redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[1]))
end
if count > tonumber(ARGV[2]) then
    return 0
end
return 1
"#;

const WRITE_CHALLENGE_SCRIPT: &str = r#"
redis.call('ZADD', KEYS[2], tonumber(ARGV[3]), ARGV[1])
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
local horizon = tonumber(ARGV[4])
redis.call('PEXPIRE', KEYS[1], horizon)
redis.call('PEXPIRE', KEYS[2], horizon)
return 1
"#;

#[derive(Clone)]
pub struct ChallengeStore {
    redis: RedisConnection,
}

impl ChallengeStore {
    pub fn new(redis: RedisConnection) -> Self {
        Self { redis }
    }

    /// Read one user's live challenges, pruning anything past its deadline.
    pub async fn inbox(&self, user_id: u32) -> Result<ChallengeInbox> {
        let mut connection = self.redis.clone();
        let entries: Vec<(String, String)> = redis::Script::new(READ_CHALLENGES_SCRIPT)
            .key(RedisKeys::user_challenge_data(user_id))
            .key(RedisKeys::user_challenge_index(user_id))
            .invoke_async(&mut connection)
            .await
            .context("failed to read challenges")?;

        Ok(build_inbox(user_id, entries))
    }

    async fn store(&self, user_id: u32, challenge: &Challenge) -> Result<()> {
        // Resolved records stay readable a little past their answer so the
        // other side can render the outcome; the index score is what actually
        // reaps them, which is why it is not simply `expires_at_ms`.
        let retain_until_ms = match challenge.state {
            ChallengeState::Pending => challenge.expires_at_ms,
            _ => now_ms().saturating_add(RESOLVED_CHALLENGE_LINGER_MS),
        };
        let payload =
            serde_json::to_string(challenge).context("failed to serialize a challenge")?;
        let horizon_ms = retain_until_ms
            .saturating_sub(now_ms())
            .max(1_000)
            .saturating_add(CHALLENGE_TTL_MS);

        let mut connection = self.redis.clone();
        let result: i64 = redis::Script::new(WRITE_CHALLENGE_SCRIPT)
            .key(RedisKeys::user_challenge_data(user_id))
            .key(RedisKeys::user_challenge_index(user_id))
            .arg(&challenge.challenge_id)
            .arg(payload)
            .arg(retain_until_ms)
            .arg(horizon_ms)
            .invoke_async(&mut connection)
            .await
            .context("failed to store a challenge")?;
        anyhow::ensure!(result == 1, "challenge write returned {result}");
        Ok(())
    }

    async fn forget(&self, user_id: u32, challenge_id: &str) -> Result<()> {
        let mut connection = self.redis.clone();
        let _: () = redis::pipe()
            .atomic()
            .hdel(RedisKeys::user_challenge_data(user_id), challenge_id)
            .zrem(RedisKeys::user_challenge_index(user_id), challenge_id)
            .query_async(&mut connection)
            .await
            .context("failed to forget a challenge")?;
        Ok(())
    }

    /// Issue a challenge, writing it to both participants and hinting both.
    ///
    /// The challenger's copy is written first: if the target's write then
    /// fails, the challenger sees their own outgoing entry and can cancel it,
    /// which is a strictly better failure than a challenge the target can see
    /// and the challenger cannot.
    pub async fn issue(
        &self,
        challenge: Challenge,
    ) -> Result<std::result::Result<Challenge, ChallengeRejection>> {
        if challenge.from_user_id == challenge.to_user_id {
            return Ok(Err(ChallengeRejection::Self_));
        }

        // Charged before any of the ceilings below, so a rejected attempt still
        // costs budget — otherwise the cheapest way to spam is to keep making
        // attempts that fail.
        let mut connection = self.redis.clone();
        let within_budget: i64 = redis::Script::new(RATE_LIMIT_SCRIPT)
            .key(RedisKeys::user_challenge_rate(challenge.from_user_id))
            .arg(CHALLENGE_RATE_WINDOW_MS)
            .arg(CHALLENGE_RATE_LIMIT)
            .invoke_async(&mut connection)
            .await
            .context("failed to record a challenge attempt")?;
        if within_budget != 1 {
            return Ok(Err(ChallengeRejection::RateLimited));
        }

        let now = now_ms();
        let challenger = self.inbox(challenge.from_user_id).await?;
        if challenger
            .outgoing
            .iter()
            .any(|existing| existing.to_user_id == challenge.to_user_id && existing.is_pending(now))
        {
            return Ok(Err(ChallengeRejection::AlreadyChallenged));
        }
        if challenger
            .outgoing
            .iter()
            .filter(|existing| existing.is_pending(now))
            .count()
            >= MAX_OUTGOING_CHALLENGES
        {
            return Ok(Err(ChallengeRejection::TooManyOutgoing));
        }

        let target = self.inbox(challenge.to_user_id).await?;
        if target
            .incoming
            .iter()
            .filter(|existing| existing.is_pending(now))
            .count()
            >= MAX_INCOMING_CHALLENGES
        {
            return Ok(Err(ChallengeRejection::TargetInboxFull));
        }

        self.store(challenge.from_user_id, &challenge).await?;
        if let Err(error) = self.store(challenge.to_user_id, &challenge).await {
            // Roll the challenger's copy back so a half-written challenge does
            // not occupy one of their outgoing slots forever.
            let _ = self
                .forget(challenge.from_user_id, &challenge.challenge_id)
                .await;
            return Err(error);
        }

        self.hint(challenge.to_user_id).await;
        Ok(Ok(challenge))
    }

    /// Move a challenge to a terminal state on behalf of `actor_user_id`.
    ///
    /// Returns `None` when the actor has no live record of it — an expired,
    /// already-answered, or fabricated id. Authorization is positional: only
    /// the target may accept or decline, only the challenger may cancel.
    pub async fn resolve(
        &self,
        actor_user_id: u32,
        challenge_id: &str,
        state: ChallengeState,
    ) -> Result<Option<Challenge>> {
        let inbox = self.inbox(actor_user_id).await?;
        let Some(mut challenge) = inbox
            .incoming
            .into_iter()
            .chain(inbox.outgoing)
            .find(|candidate| candidate.challenge_id == challenge_id)
        else {
            return Ok(None);
        };
        if challenge.state != ChallengeState::Pending {
            return Ok(None);
        }

        let authorized = match state {
            ChallengeState::Accepted | ChallengeState::Declined => {
                challenge.to_user_id == actor_user_id
            }
            ChallengeState::Cancelled => challenge.from_user_id == actor_user_id,
            ChallengeState::Pending => false,
        };
        if !authorized {
            return Ok(None);
        }

        challenge.state = state;
        // Both copies are updated; whichever write lands first, each side reads
        // its own store and converges on the same terminal record.
        self.store(challenge.from_user_id, &challenge).await?;
        self.store(challenge.to_user_id, &challenge).await?;

        let counterparty = if actor_user_id == challenge.from_user_id {
            challenge.to_user_id
        } else {
            challenge.from_user_id
        };
        self.hint(counterparty).await;
        Ok(Some(challenge))
    }

    /// Nudge one user's sockets to re-read their challenges. Best effort by
    /// construction — the durable record is what matters, and a socket that
    /// misses this reconciles on its own timer.
    ///
    /// The payload is a JSON string, not a bare word: subscribers decode every
    /// Pub/Sub message with `serde_json`, and an unquoted `challenges` fails
    /// to parse and is silently skipped, which would leave the feature running
    /// at reconcile latency while looking instantaneous in code review.
    async fn hint(&self, user_id: u32) {
        let mut connection = self.redis.clone();
        let published: Result<(), _> = connection
            .publish(
                RedisKeys::user_notifications_channel(user_id),
                CHALLENGE_HINT_PAYLOAD,
            )
            .await;
        if let Err(error) = published {
            tracing::debug!(user_id, %error, "challenge hint publish failed; reconcile will cover it");
        }
    }
}

pub fn new_challenge_id(from_user_id: u32, to_user_id: u32, now_ms: i64) -> String {
    // Ids only need to be unguessable enough that one player cannot answer
    // another's challenge by construction; authorization is checked anyway.
    let salt: u64 = rand::random();
    format!("{from_user_id:x}-{to_user_id:x}-{now_ms:x}-{salt:016x}")
}

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub(crate) fn build_inbox(user_id: u32, entries: Vec<(String, String)>) -> ChallengeInbox {
    let mut incoming = Vec::new();
    let mut outgoing = Vec::new();
    for (_, payload) in entries {
        let Ok(challenge) = serde_json::from_str::<Challenge>(&payload) else {
            continue;
        };
        if challenge.to_user_id == user_id {
            incoming.push(challenge);
        } else if challenge.from_user_id == user_id {
            outgoing.push(challenge);
        }
    }
    // Newest first: an inbox is read top-down, and the newest challenge is the
    // one the notification was about.
    incoming.sort_by_key(|challenge| std::cmp::Reverse(challenge.created_at_ms));
    outgoing.sort_by_key(|challenge| std::cmp::Reverse(challenge.created_at_ms));
    ChallengeInbox { incoming, outgoing }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn challenge(id: &str, from: u32, to: u32, created_at_ms: i64) -> Challenge {
        Challenge {
            challenge_id: id.to_string(),
            from_user_id: from,
            from_username: format!("user{from}"),
            to_user_id: to,
            to_username: format!("user{to}"),
            lobby_code: "USE1-ABCD1234".to_string(),
            state: ChallengeState::Pending,
            created_at_ms,
            expires_at_ms: created_at_ms + CHALLENGE_TTL_MS,
        }
    }

    fn entry(challenge: &Challenge) -> (String, String) {
        (
            challenge.challenge_id.clone(),
            serde_json::to_string(challenge).expect("serializes"),
        )
    }

    #[test]
    fn one_record_shape_splits_by_viewer() {
        let inbound = challenge("a", 2, 1, 100);
        let outbound = challenge("b", 1, 3, 200);
        let entries = vec![entry(&inbound), entry(&outbound)];

        let mine = build_inbox(1, entries.clone());
        assert_eq!(mine.incoming.len(), 1);
        assert_eq!(mine.incoming[0].challenge_id, "a");
        assert_eq!(mine.outgoing.len(), 1);
        assert_eq!(mine.outgoing[0].challenge_id, "b");

        // The same two records read by the other party swap sides, which is
        // exactly why there is no stored direction flag.
        let theirs = build_inbox(2, entries);
        assert_eq!(theirs.outgoing.len(), 1);
        assert_eq!(theirs.outgoing[0].challenge_id, "a");
        assert!(theirs.incoming.is_empty());
    }

    #[test]
    fn inbox_is_newest_first() {
        let older = challenge("old", 2, 1, 100);
        let newer = challenge("new", 3, 1, 900);
        let inbox = build_inbox(1, vec![entry(&older), entry(&newer)]);
        assert_eq!(
            inbox
                .incoming
                .iter()
                .map(|c| c.challenge_id.as_str())
                .collect::<Vec<_>>(),
            vec!["new", "old"]
        );
    }

    #[test]
    fn records_a_user_is_not_party_to_are_ignored() {
        let unrelated = challenge("x", 7, 8, 100);
        let inbox = build_inbox(1, vec![entry(&unrelated), ("y".into(), "junk".into())]);
        assert!(inbox.incoming.is_empty());
        assert!(inbox.outgoing.is_empty());
    }

    #[test]
    fn pending_requires_both_state_and_a_live_deadline() {
        let live = challenge("a", 2, 1, 100);
        assert!(live.is_pending(live.expires_at_ms - 1));
        assert!(!live.is_pending(live.expires_at_ms));

        let mut answered = live.clone();
        answered.state = ChallengeState::Declined;
        assert!(!answered.is_pending(0));
    }

    #[test]
    fn challenge_ids_do_not_collide_within_a_millisecond() {
        let first = new_challenge_id(1, 2, 1_700_000_000_000);
        let second = new_challenge_id(1, 2, 1_700_000_000_000);
        assert_ne!(first, second);
    }

    /// Subscribers decode Pub/Sub payloads with serde_json and silently skip
    /// anything that does not parse, so a malformed hint degrades the feature
    /// to reconcile latency without any visible error.
    #[test]
    fn the_hint_payload_decodes_as_the_subscriber_reads_it() {
        assert_eq!(
            serde_json::from_str::<String>(CHALLENGE_HINT_PAYLOAD).unwrap(),
            "challenges"
        );
    }

    #[test]
    fn every_rejection_reads_as_an_explanation() {
        for rejection in [
            ChallengeRejection::Self_,
            ChallengeRejection::TargetOffline,
            ChallengeRejection::AlreadyChallenged,
            ChallengeRejection::TooManyOutgoing,
            ChallengeRejection::TargetInboxFull,
            ChallengeRejection::RateLimited,
        ] {
            let reason = rejection.reason();
            assert!(reason.ends_with('.'), "{reason:?} is not a sentence");
            assert!(reason.len() > 12, "{reason:?} is too terse to act on");
        }
    }
}
