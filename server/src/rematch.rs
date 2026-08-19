//! "Run it back" — everyone who ticks Rematch on the results card gets put in
//! the same next game.
//!
//! The constraint that shapes this: a match cannot be minted for an arbitrary
//! set of users. `COMMIT_MATCH_SCRIPT` only commits lobbies that are genuinely
//! admitted to the queue, so the rematch cannot conjure a game — it has to
//! converge the opted-in players onto **one elected lobby** and let the normal
//! queue do what it already does. `SET NX` on the lobby key is what makes that
//! election exactly-once across the cluster.
//!
//! Opt-in and presence live in one hash keyed on the finished game, because the
//! participant set is fixed and shared. Both are leases: presence expires on
//! its own if a player closes the tab, so "still here" decays to the truth
//! without anyone having to send a goodbye.

use anyhow::{Context, Result};
use common::{GameType, QueueMode};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

use crate::redis_keys::RedisKeys;
use crate::redis_utils::RedisConnection;

/// How long the offer stands. Deliberately the same as a challenge, and
/// comfortably inside the 180s lobby-metadata TTL that has to outlive it for
/// the elected lobby to still exist when the last player opts in.
pub const REMATCH_WINDOW_MS: i64 = 120_000;
/// How long a participant counts as "still here" without a heartbeat. Longer
/// than the 10s refresh so an ordinary hiccup does not blink someone out of
/// the roster mid-decision.
pub const REMATCH_PRESENCE_LEASE_MS: i64 = 30_000;

/// One player's standing on the results card.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct RematchParticipant {
    pub user_id: u32,
    pub username: String,
    /// Still on the results screen, by an expiring lease rather than a promise.
    pub present: bool,
    pub opted_in: bool,
}

/// The live state of one game's rematch, as every participant sees it.
///
/// Not `Eq`: it carries a `GameType`, whose `Custom` variant holds settings
/// that are only partially comparable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct RematchState {
    pub game_id: u32,
    pub participants: Vec<RematchParticipant>,
    /// Set once the lobby has been elected; everyone converges on it.
    pub lobby_code: Option<String>,
    /// Whose client queues the rematch. Only meaningful with `lobby_code`.
    pub host_user_id: Option<u32>,
    /// The queue family this many players can actually form, if any.
    pub game_type: Option<GameType>,
    pub queue_mode: QueueMode,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub expires_at_ms: i64,
}

impl RematchState {
    pub fn opted_in_user_ids(&self) -> Vec<u32> {
        self.participants
            .iter()
            .filter(|participant| participant.opted_in)
            .map(|participant| participant.user_id)
            .collect()
    }

    /// The elector is the lowest opted-in id. Deterministic, so every socket
    /// that observes readiness agrees on who acts without a negotiation — and
    /// `SET NX` still settles it if two disagree.
    pub fn elected_host(&self) -> Option<u32> {
        self.opted_in_user_ids().into_iter().min()
    }
}

/// The queue family a given number of players can form.
///
/// Only 1, 2, and 4 are exactly satisfiable. A 3-player lobby queued for
/// four-player free-for-all would be padded with a stranger, which is the one
/// thing a rematch must never do — so an unsatisfiable count has no game type
/// and the UI says the rematch cannot go ahead rather than silently inviting
/// someone who was not in the match.
pub fn rematch_game_type(original: &GameType, opted_in: usize) -> Option<GameType> {
    match opted_in {
        // Running a duel back as a solo game is not what anyone ticking
        // "Rematch" meant. One player only forms a game when the match they
        // just played was itself solo.
        1 => matches!(original, GameType::Solo).then_some(GameType::Solo),
        2 => Some(match original {
            GameType::TeamMatch { per_team: 1 } => GameType::TeamMatch { per_team: 1 },
            _ => GameType::FreeForAll { max_players: 2 },
        }),
        4 => Some(match original {
            GameType::TeamMatch { per_team: 2 } => GameType::TeamMatch { per_team: 2 },
            _ => GameType::FreeForAll { max_players: 4 },
        }),
        _ => None,
    }
}

/// Prune expired presence leases, then return the whole record.
const READ_REMATCH_SCRIPT: &str = r#"
local now = redis.call('TIME')
local now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local entries = redis.call('HGETALL', KEYS[1])
local out = {}
for i = 1, #entries, 2 do
    local field = entries[i]
    local value = entries[i + 1]
    if string.sub(field, 1, 8) == 'present:' then
        if tonumber(value) ~= nil and tonumber(value) > now_ms then
            table.insert(out, field)
            table.insert(out, value)
        else
            redis.call('HDEL', KEYS[1], field)
        end
    else
        table.insert(out, field)
        table.insert(out, value)
    end
end
return out
"#;

const WRITE_REMATCH_FIELD_SCRIPT: &str = r#"
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[3]))
return 1
"#;

fn opt_in_field(user_id: u32) -> String {
    format!("optin:{user_id}")
}

fn presence_field(user_id: u32) -> String {
    format!("present:{user_id}")
}

#[derive(Clone)]
pub struct RematchStore {
    redis: RedisConnection,
}

impl RematchStore {
    pub fn new(redis: RedisConnection) -> Self {
        Self { redis }
    }

    /// Record that this player wants — or no longer wants — to run it back.
    pub async fn set_intent(&self, game_id: u32, user_id: u32, opt_in: bool) -> Result<()> {
        self.write_field(
            game_id,
            &opt_in_field(user_id),
            if opt_in { "1" } else { "0" },
        )
        .await
    }

    /// Re-assert that this player is still looking at the results card.
    pub async fn touch_presence(&self, game_id: u32, user_id: u32) -> Result<()> {
        let until_ms = now_ms().saturating_add(REMATCH_PRESENCE_LEASE_MS);
        self.write_field(game_id, &presence_field(user_id), &until_ms.to_string())
            .await
    }

    async fn write_field(&self, game_id: u32, field: &str, value: &str) -> Result<()> {
        let mut connection = self.redis.clone();
        let result: i64 = redis::Script::new(WRITE_REMATCH_FIELD_SCRIPT)
            .key(RedisKeys::rematch_record(game_id))
            .arg(field)
            .arg(value)
            .arg(REMATCH_WINDOW_MS)
            .invoke_async(&mut connection)
            .await
            .context("failed to write a rematch field")?;
        anyhow::ensure!(result == 1, "rematch write returned {result}");
        Ok(())
    }

    /// Read the record and project it against the match's real participants.
    ///
    /// `participants` comes from the completed game, never from a client, so a
    /// spectator cannot appear in the roster no matter what they send.
    pub async fn state(
        &self,
        game_id: u32,
        participants: &[(u32, String)],
        original_game_type: &GameType,
        queue_mode: QueueMode,
    ) -> Result<RematchState> {
        let mut connection = self.redis.clone();
        let entries: Vec<(String, String)> = redis::Script::new(READ_REMATCH_SCRIPT)
            .key(RedisKeys::rematch_record(game_id))
            .invoke_async(&mut connection)
            .await
            .context("failed to read the rematch record")?;
        let lobby_code: Option<String> = connection
            .get(RedisKeys::rematch_lobby(game_id))
            .await
            .context("failed to read the elected rematch lobby")?;

        Ok(build_state(
            game_id,
            participants,
            entries,
            lobby_code,
            original_game_type,
            queue_mode,
        ))
    }

    /// Claim the right to host, exactly once per game.
    ///
    /// Returns the code that won, which is this caller's only when the claim
    /// succeeded — a loser is told the winner's code and joins that instead.
    pub async fn elect_lobby(&self, game_id: u32, lobby_code: &str) -> Result<String> {
        let mut connection = self.redis.clone();
        let claimed: Option<String> = redis::cmd("SET")
            .arg(RedisKeys::rematch_lobby(game_id))
            .arg(lobby_code)
            .arg("NX")
            .arg("PX")
            .arg(REMATCH_WINDOW_MS)
            .arg("GET")
            .query_async(&mut connection)
            .await
            .context("failed to elect a rematch lobby")?;
        Ok(claimed.unwrap_or_else(|| lobby_code.to_string()))
    }

    /// Nudge one participant's sockets to re-read the rematch record.
    pub async fn hint(&self, user_id: u32) {
        let mut connection = self.redis.clone();
        let published: Result<(), _> = connection
            .publish(
                RedisKeys::user_notifications_channel(user_id),
                REMATCH_HINT_PAYLOAD,
            )
            .await;
        if let Err(error) = published {
            tracing::debug!(user_id, %error, "rematch hint publish failed; reconcile will cover it");
        }
    }
}

/// JSON string, for the same reason the challenge hint is one: subscribers
/// decode every payload with serde and silently skip anything that will not
/// parse.
pub const REMATCH_HINT_PAYLOAD: &str = "\"rematch\"";

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub(crate) fn build_state(
    game_id: u32,
    participants: &[(u32, String)],
    entries: Vec<(String, String)>,
    lobby_code: Option<String>,
    original_game_type: &GameType,
    queue_mode: QueueMode,
) -> RematchState {
    let opted_in = |user_id: u32| {
        entries
            .iter()
            .any(|(field, value)| *field == opt_in_field(user_id) && value == "1")
    };
    let present = |user_id: u32| {
        entries
            .iter()
            .any(|(field, _)| *field == presence_field(user_id))
    };

    let mut roster: Vec<RematchParticipant> = participants
        .iter()
        .map(|(user_id, username)| RematchParticipant {
            user_id: *user_id,
            username: username.clone(),
            present: present(*user_id),
            opted_in: opted_in(*user_id),
        })
        .collect();
    roster.sort_by_key(|participant| participant.user_id);

    let opted_in_count = roster
        .iter()
        .filter(|participant| participant.opted_in)
        .count();
    let game_type = rematch_game_type(original_game_type, opted_in_count);
    let host_user_id = roster
        .iter()
        .filter(|participant| participant.opted_in)
        .map(|participant| participant.user_id)
        .min();

    RematchState {
        game_id,
        participants: roster,
        // A lobby only means anything once the count can actually form a game;
        // surfacing a stale one would send players into a lobby that will
        // never be queued.
        lobby_code: game_type.as_ref().and(lobby_code),
        host_user_id: game_type.as_ref().and(host_user_id),
        game_type,
        queue_mode,
        expires_at_ms: now_ms().saturating_add(REMATCH_WINDOW_MS),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn participants() -> Vec<(u32, String)> {
        vec![(7, "Ada".to_string()), (9, "Grace".to_string())]
    }

    fn entry(field: &str, value: &str) -> (String, String) {
        (field.to_string(), value.to_string())
    }

    #[test]
    fn only_exactly_satisfiable_counts_get_a_game_type() {
        let duel = GameType::TeamMatch { per_team: 1 };
        // One is only a game if the match being run back was already solo;
        // otherwise the player is simply waiting for someone to join them.
        assert_eq!(rematch_game_type(&duel, 1), None);
        assert_eq!(rematch_game_type(&GameType::Solo, 1), Some(GameType::Solo));
        assert_eq!(rematch_game_type(&duel, 2), Some(duel.clone()));
        assert_eq!(
            rematch_game_type(&duel, 4),
            Some(GameType::FreeForAll { max_players: 4 })
        );
        // Three is the trap: a three-player lobby queued for a four-player
        // family would be padded with a stranger who was never in the match.
        assert_eq!(rematch_game_type(&duel, 3), None);
        assert_eq!(rematch_game_type(&duel, 0), None);
        assert_eq!(rematch_game_type(&duel, 5), None);
    }

    #[test]
    fn a_team_match_runs_back_as_the_same_family() {
        let team = GameType::TeamMatch { per_team: 2 };
        assert_eq!(rematch_game_type(&team, 4), Some(team));

        let ffa = GameType::FreeForAll { max_players: 4 };
        assert_eq!(rematch_game_type(&ffa, 4), Some(ffa));
    }

    #[test]
    fn the_roster_comes_from_the_match_not_the_record() {
        // A stray record for someone who never played must not invent a row.
        let state = build_state(
            1,
            &participants(),
            vec![
                entry("optin:404", "1"),
                entry("present:404", "99999999999999"),
            ],
            None,
            &GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
        );
        assert_eq!(
            state
                .participants
                .iter()
                .map(|p| p.user_id)
                .collect::<Vec<_>>(),
            vec![7, 9]
        );
        assert!(state.participants.iter().all(|p| !p.opted_in && !p.present));
    }

    #[test]
    fn opt_out_is_recorded_as_a_value_not_an_absence() {
        let state = build_state(
            1,
            &participants(),
            vec![entry("optin:7", "1"), entry("optin:9", "0")],
            None,
            &GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
        );
        assert!(state.participants[0].opted_in);
        assert!(!state.participants[1].opted_in);
        assert_eq!(state.opted_in_user_ids(), vec![7]);
    }

    #[test]
    fn the_lobby_is_withheld_until_the_count_can_form_a_game() {
        let one_in = build_state(
            1,
            &participants(),
            vec![entry("optin:7", "1")],
            Some("USE1-ABCD1234".to_string()),
            &GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
        );
        // One of two cannot run a duel back, so no lobby is offered yet.
        assert_eq!(one_in.game_type, None);
        assert_eq!(one_in.lobby_code, None);

        let three = vec![
            (7, "Ada".to_string()),
            (9, "Grace".to_string()),
            (11, "Vector".to_string()),
        ];
        let unsatisfiable = build_state(
            1,
            &three,
            vec![
                entry("optin:7", "1"),
                entry("optin:9", "1"),
                entry("optin:11", "1"),
            ],
            Some("USE1-ABCD1234".to_string()),
            &GameType::FreeForAll { max_players: 4 },
            QueueMode::Quickmatch,
        );
        assert_eq!(unsatisfiable.game_type, None);
        assert_eq!(unsatisfiable.lobby_code, None, "a lobby nobody can queue");
        assert_eq!(unsatisfiable.host_user_id, None);
    }

    #[test]
    fn the_host_is_the_lowest_opted_in_id_so_every_socket_agrees() {
        let state = build_state(
            1,
            &participants(),
            vec![entry("optin:7", "1"), entry("optin:9", "1")],
            None,
            &GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
        );
        assert_eq!(state.elected_host(), Some(7));
        assert_eq!(state.host_user_id, Some(7));
    }

    #[test]
    fn presence_is_a_lease_the_reader_never_has_to_interpret() {
        // The read script drops expired leases, so anything that survives to
        // `build_state` is live by construction.
        let state = build_state(
            1,
            &participants(),
            vec![entry("present:7", "99999999999999")],
            None,
            &GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
        );
        assert!(state.participants[0].present);
        assert!(!state.participants[1].present);
    }
}
