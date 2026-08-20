//! Process-global analytics sink.
//!
//! Deliberately shaped like `resilience_metrics`, which is already a
//! process-global facade called from the same completion site. The alternative
//! — threading an emitter through `GameActor`'s constructor and every
//! partition dispatch path — would put analytics plumbing inside the most
//! correctness-critical code in the repo for no behavioural gain.
//!
//! Safe as a global precisely because emission is fire-and-forget: it drops
//! under pressure, returns nothing actionable, and no caller may branch on it.
//!
//! Every `record_*` function below is one line at its call site, takes only
//! data that site already holds, and is split into a pure `*_event` builder
//! plus a thin recorder so the projection can be unit tested without a global.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};

use common::GameState;

use crate::completion::CompletionRecordV1;
use crate::lobby_manager::{LobbyMember, LobbyPreferences};
use crate::matchmaking_manager::QueuedLobby;
use crate::matchmaking_pool::MatchmakingPool;

use super::emitter::AnalyticsEmitter;
use super::event::{EventIdentity, EventOrigin, envelope, now_ms};
use super::proto;

/// Upper bound on tracked queue entries. A queue entry is a few dozen bytes and
/// the live queue is orders of magnitude smaller than this, so the cap only
/// exists so a leak can never become an unbounded one inside a gameplay task.
const MAX_TRACKED_QUEUE_ENTRIES: usize = 8_192;

/// How long a queue-entry stamp stays interesting. No real queue wait
/// approaches this; anything older is a lobby that left by some path other than
/// an explicit cancel, and is swept the next time the map is under pressure.
const QUEUE_ENTRY_STALE_MS: i64 = 600_000;

struct Sink {
    emitter: AnalyticsEmitter,
    origin: EventOrigin,
    /// When each lobby was admitted to a queue, so `queue_left` can report an
    /// honest `waited_ms`.
    ///
    /// Kept here rather than on the connection because the admitting task and
    /// the cancelling websocket are the same process, and because the
    /// alternative — a field on `ConnectionState`, or a second Valkey read on
    /// the cancel path — would either restructure gameplay state or put an
    /// `await` on an analytics path. Both are forbidden by the PRD's invariant
    /// I1.
    queue_entries: Mutex<HashMap<String, i64>>,
}

static SINK: OnceLock<Sink> = OnceLock::new();

/// Installs the sink. Called once during server start.
///
/// A second call is ignored rather than panicking: tests start several servers
/// in one process, and a panic there would be a test-only failure mode with no
/// production meaning.
pub fn install(emitter: AnalyticsEmitter, origin: EventOrigin) {
    let _ = SINK.set(Sink {
        emitter,
        origin,
        queue_entries: Mutex::new(HashMap::new()),
    });
}

pub fn is_installed() -> bool {
    SINK.get().is_some()
}

/// Emits `game_completed` plus one `game_player_result` per player.
///
/// A no-op when analytics is not configured, so a deployment without it runs
/// unchanged.
pub fn record_game_completed(record: &CompletionRecordV1) {
    let Some(sink) = SINK.get() else { return };
    for event in super::completion_events::project(record, &sink.origin) {
        sink.emitter.emit(event);
    }
}

pub fn record_lobby_created(lobby_code: &str, host_user_id: i32, pool: MatchmakingPool) {
    let Some(sink) = SINK.get() else { return };
    sink.emitter.emit(lobby_created_event(
        &sink.origin,
        lobby_code,
        host_user_id,
        pool,
    ));
}

/// `members_before` is the membership the join is about to be added to, so the
/// reported count is the lobby as it stands once this member is present.
pub fn record_lobby_joined(
    lobby_code: &str,
    user_id: i32,
    members_before: &BTreeMap<u32, LobbyMember>,
) {
    let Some(sink) = SINK.get() else { return };
    sink.emitter.emit(lobby_joined_event(
        &sink.origin,
        lobby_code,
        user_id,
        members_before,
    ));
}

pub fn record_lobby_left(lobby_code: &str, user_id: i32, remaining_members: i64) {
    let Some(sink) = SINK.get() else { return };
    sink.emitter.emit(lobby_left_event(
        &sink.origin,
        lobby_code,
        user_id,
        remaining_members,
    ));
}

pub fn record_lobby_preferences_set(preferences: &LobbyPreferences) {
    let Some(sink) = SINK.get() else { return };
    sink.emitter
        .emit(lobby_preferences_set_event(&sink.origin, preferences));
}

/// `admission_result` is the classified outcome of the atomic admission
/// script, so a rejected or deduplicated request is visible as such rather than
/// disappearing.
pub fn record_queue_entered(lobby: &QueuedLobby, admission_result: &str) {
    let Some(sink) = SINK.get() else { return };
    remember_queue_entry(&sink.queue_entries, lobby, admission_result);
    sink.emitter
        .emit(queue_entered_event(&sink.origin, lobby, admission_result));
}

/// `waited_ms` is `0` when this task never observed the matching admission —
/// the lobby was queued by another task, or the entry aged out. A zero is the
/// proto default and must be read as "unknown", never as an instant cancel.
pub fn record_queue_left(lobby_code: &str, user_id: i32) {
    let Some(sink) = SINK.get() else { return };
    let waited_ms =
        take_queue_entry(&sink.queue_entries, lobby_code).map_or(0, |at| (now_ms() - at).max(0));
    sink.emitter
        .emit(queue_left_event(&sink.origin, user_id, waited_ms));
}

pub fn record_match_committed(
    game_id: u32,
    wait_ms: i64,
    player_count: usize,
    pool: MatchmakingPool,
) {
    let Some(sink) = SINK.get() else { return };
    sink.emitter.emit(match_committed_event(
        &sink.origin,
        game_id,
        wait_ms,
        player_count,
        pool,
    ));
}

pub fn record_game_started(game_id: u32, state: &GameState, player_count: usize) {
    let Some(sink) = SINK.get() else { return };
    sink.emitter.emit(game_started_event(
        &sink.origin,
        game_id,
        state,
        player_count,
    ));
}

/// Emitted at the moment a session id is minted, which is the first
/// identity-bearing moment of a websocket. The account behind it is not known
/// until verification completes, so this event carries the session and the
/// pseudonymous browser id only.
///
/// It stays that way deliberately. Emitting it here — before the JWT is
/// verified — is what makes it the funnel's denominator: it counts every
/// attempt, including the ones that go on to fail verification. The account
/// is still reachable from it, by joining on `session_id` to this session's
/// `websocket_message` rows, which carry both the session and the account
/// once authentication has completed.
pub fn record_session_started(session_id: &str, anon_id: Option<&str>, protocol_version: u16) {
    let Some(sink) = SINK.get() else { return };
    sink.emitter.emit(session_started_event(
        &sink.origin,
        session_id,
        anon_id,
        protocol_version,
    ));
}

/// Emitted where the socket future finishes, which is the only place that knows
/// the full session duration. Nothing about the user survives to that point, so
/// this event is a duration and a reason, with no identity to join on.
pub fn record_session_ended(duration_ms: i64, close_reason: &str) {
    let Some(sink) = SINK.get() else { return };
    sink.emitter
        .emit(session_ended_event(&sink.origin, duration_ms, close_reason));
}

// ---------------------------------------------------------------------------
// Projections. Pure: origin plus site data in, one event out.
// ---------------------------------------------------------------------------

fn lobby_created_event(
    origin: &EventOrigin,
    lobby_code: &str,
    host_user_id: i32,
    pool: MatchmakingPool,
) -> proto::Event {
    envelope(
        origin,
        EventIdentity {
            user_id: Some(i64::from(host_user_id)),
            is_stress_test: pool == MatchmakingPool::Stress,
            ..Default::default()
        },
        proto::event::Payload::LobbyCreated(proto::LobbyCreated {
            lobby_id: lobby_code.to_owned(),
            pool: pool.as_str().to_owned(),
        }),
    )
}

fn lobby_joined_event(
    origin: &EventOrigin,
    lobby_code: &str,
    user_id: i32,
    members_before: &BTreeMap<u32, LobbyMember>,
) -> proto::Event {
    // A rejoin from a second transport must not double-count the same person.
    let member_count = members_before
        .keys()
        .filter(|existing| i64::from(**existing) != i64::from(user_id))
        .count()
        + 1;
    envelope(
        origin,
        EventIdentity {
            user_id: Some(i64::from(user_id)),
            ..Default::default()
        },
        proto::event::Payload::LobbyJoined(proto::LobbyJoined {
            lobby_id: lobby_code.to_owned(),
            member_count: member_count as i64,
        }),
    )
}

fn lobby_left_event(
    origin: &EventOrigin,
    lobby_code: &str,
    user_id: i32,
    remaining_members: i64,
) -> proto::Event {
    envelope(
        origin,
        EventIdentity {
            user_id: Some(i64::from(user_id)),
            ..Default::default()
        },
        proto::event::Payload::LobbyLeft(proto::LobbyLeft {
            lobby_id: lobby_code.to_owned(),
            member_count: remaining_members.max(0),
        }),
    )
}

fn lobby_preferences_set_event(
    origin: &EventOrigin,
    preferences: &LobbyPreferences,
) -> proto::Event {
    envelope(
        origin,
        EventIdentity::default(),
        proto::event::Payload::LobbyPreferencesSet(proto::LobbyPreferencesSet {
            game_type: join_modes(preferences.selected_modes.iter().map(String::as_str)),
            queue_mode: queue_mode_label(preferences.competitive).to_owned(),
        }),
    )
}

fn queue_entered_event(
    origin: &EventOrigin,
    lobby: &QueuedLobby,
    admission_result: &str,
) -> proto::Event {
    envelope(
        origin,
        EventIdentity {
            user_id: Some(i64::from(lobby.requesting_user_id)),
            is_stress_test: lobby.matchmaking_pool == MatchmakingPool::Stress,
            ..Default::default()
        },
        proto::event::Payload::QueueEntered(proto::QueueEntered {
            // A lobby queues for a set of game types at once. Each is rendered
            // exactly as `game_completed` renders it, so the strings join.
            game_type: join_modes(lobby.game_types.iter().map(|t| format!("{t:?}"))),
            queue_mode: format!("{:?}", lobby.queue_mode),
            admission_result: admission_result.to_owned(),
        }),
    )
}

fn queue_left_event(origin: &EventOrigin, user_id: i32, waited_ms: i64) -> proto::Event {
    envelope(
        origin,
        EventIdentity {
            user_id: Some(i64::from(user_id)),
            ..Default::default()
        },
        proto::event::Payload::QueueLeft(proto::QueueLeft { waited_ms }),
    )
}

fn match_committed_event(
    origin: &EventOrigin,
    game_id: u32,
    wait_ms: i64,
    player_count: usize,
    pool: MatchmakingPool,
) -> proto::Event {
    envelope(
        origin,
        EventIdentity {
            is_stress_test: pool == MatchmakingPool::Stress,
            ..Default::default()
        },
        proto::event::Payload::MatchCommitted(proto::MatchCommitted {
            game_id: i64::from(game_id),
            wait_ms: wait_ms.max(0),
            player_count: player_count as i64,
        }),
    )
}

fn game_started_event(
    origin: &EventOrigin,
    game_id: u32,
    state: &GameState,
    player_count: usize,
) -> proto::Event {
    envelope(
        origin,
        EventIdentity {
            is_stress_test: state.is_stress_test,
            ..Default::default()
        },
        proto::event::Payload::GameStarted(proto::GameStarted {
            game_id: i64::from(game_id),
            game_type: format!("{:?}", state.game_type),
            queue_mode: format!("{:?}", state.queue_mode),
            player_count: player_count as i64,
        }),
    )
}

fn session_started_event(
    origin: &EventOrigin,
    session_id: &str,
    anon_id: Option<&str>,
    protocol_version: u16,
) -> proto::Event {
    envelope(
        origin,
        EventIdentity {
            anon_id: anon_id.map(str::to_owned),
            session_id: Some(session_id.to_owned()),
            ..Default::default()
        },
        proto::event::Payload::SessionStarted(proto::SessionStarted {
            protocol_version: i64::from(protocol_version),
        }),
    )
}

fn session_ended_event(origin: &EventOrigin, duration_ms: i64, close_reason: &str) -> proto::Event {
    envelope(
        origin,
        EventIdentity::default(),
        proto::event::Payload::SessionEnded(proto::SessionEnded {
            duration_ms: duration_ms.max(0),
            close_reason: close_reason.to_owned(),
        }),
    )
}

// ---------------------------------------------------------------------------
// Queue-wait bookkeeping.
// ---------------------------------------------------------------------------

/// Records when a lobby entered a queue, if it actually entered one.
///
/// `applied` overwrites, because a fresh admission replaced the queue identity.
/// `idempotent` keeps the earlier stamp, because the wait started with the
/// first request, not the retry. Every other outcome queued nothing.
fn remember_queue_entry(
    ledger: &Mutex<HashMap<String, i64>>,
    lobby: &QueuedLobby,
    admission_result: &str,
) {
    let Ok(mut entries) = ledger.lock() else {
        return;
    };
    match admission_result {
        ADMISSION_APPLIED => {
            evict_if_full(&mut entries);
            entries.insert(lobby.lobby_code.clone(), lobby.queued_at);
        }
        ADMISSION_IDEMPOTENT => {
            evict_if_full(&mut entries);
            entries
                .entry(lobby.lobby_code.clone())
                .or_insert(lobby.queued_at);
        }
        _ => {}
    }
}

fn take_queue_entry(ledger: &Mutex<HashMap<String, i64>>, lobby_code: &str) -> Option<i64> {
    ledger.lock().ok()?.remove(lobby_code)
}

/// Sweeps stale stamps once the map is at its cap, and refuses to grow past it.
/// Losing a `waited_ms` is a reporting gap; growing without a bound inside a
/// gameplay task is not acceptable at any size.
fn evict_if_full(entries: &mut HashMap<String, i64>) {
    if entries.len() < MAX_TRACKED_QUEUE_ENTRIES {
        return;
    }
    let cutoff = now_ms() - QUEUE_ENTRY_STALE_MS;
    entries.retain(|_, queued_at| *queued_at > cutoff);
    if entries.len() >= MAX_TRACKED_QUEUE_ENTRIES {
        entries.clear();
    }
}

/// The admission classifications `matchmaking_manager` reports. Named here so
/// the queue-wait bookkeeping and the emitted string cannot drift apart.
pub const ADMISSION_APPLIED: &str = "applied";
pub const ADMISSION_IDEMPOTENT: &str = "idempotent";
pub const ADMISSION_REJECTED: &str = "rejected";
pub const ADMISSION_INTEGRITY_ERROR: &str = "integrity_error";
pub const ADMISSION_UNKNOWN: &str = "unknown";

/// `|` rather than `,` because a game type renders as `TeamMatch { per_team: 1 }`,
/// which already contains a comma.
fn join_modes<T: AsRef<str>>(modes: impl Iterator<Item = T>) -> String {
    modes
        .map(|mode| mode.as_ref().to_owned())
        .collect::<Vec<_>>()
        .join("|")
}

/// Matches `format!("{:?}", QueueMode::…)`, which is what `game_completed`
/// writes, so preference and completion rows join on the same value.
fn queue_mode_label(competitive: bool) -> &'static str {
    if competitive {
        "Competitive"
    } else {
        "Quickmatch"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analytics::emitter::EmitterConfig;
    use common::{GameType, QueueMode};

    fn origin() -> EventOrigin {
        EventOrigin {
            environment: "test".to_owned(),
            region: "use1".to_owned(),
            aws_region: "us-east-1".to_owned(),
            instance_id: "1:boot".to_owned(),
        }
    }

    fn member(user_id: u32) -> LobbyMember {
        LobbyMember {
            can_show_video_ad: false,
            supports_ad_break: false,
            distribution: None,
            user_id,
            username: format!("u{user_id}"),
            ts: 0.0,
        }
    }

    fn members(ids: &[u32]) -> BTreeMap<u32, LobbyMember> {
        ids.iter().map(|id| (*id, member(*id))).collect()
    }

    fn queued_lobby(game_types: Vec<GameType>, pool: MatchmakingPool) -> QueuedLobby {
        QueuedLobby {
            queue_identity_json: None,
            lobby_code: "ABCDEF".to_owned(),
            queue_token: "token".to_owned(),
            members: vec![member(1)],
            avg_mmr: 1000,
            game_types,
            queue_mode: QueueMode::Competitive,
            queued_at: 1_000,
            requesting_user_id: 1,
            matchmaking_pool: pool,
        }
    }

    fn payload(event: &proto::Event) -> proto::event::Payload {
        event.payload.as_ref().unwrap().clone()
    }

    /// Without a sink installed this must be a silent no-op, never a panic:
    /// gameplay runs in deployments that have no analytics at all.
    #[test]
    fn recording_without_a_sink_is_a_no_op() {
        // Uses whatever global state the test binary has; the assertion is
        // simply that this cannot panic.
        let state = common::GameState::new(
            10,
            10,
            common::GameType::Solo,
            common::QueueMode::Quickmatch,
            Some(1),
            0,
        );
        let record = CompletionRecordV1 {
            season: None,
            schema_version: crate::completion::COMPLETION_SCHEMA_VERSION,
            game_id: 1,
            partition_id: 0,
            revision: uuid::Uuid::new_v4(),
            ended_at_ms: 1,
            server_id: 1,
            recording: None,
            recording_canonical_bytes: None,
            recording_journal: None,
            play_of_the_game: None,
            final_state: state,
            effects: Vec::new(),
        };
        record_game_completed(&record);
    }

    /// Every non-completion recorder must be equally inert without a sink.
    /// These are called from lobby, matchmaking, and websocket paths that run
    /// in deployments with no analytics configured at all.
    #[test]
    fn every_recorder_is_inert_without_a_sink() {
        let state = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, Some(1), 0);
        record_lobby_created("ABCDEF", 7, MatchmakingPool::Public);
        record_lobby_joined("ABCDEF", 7, &members(&[3]));
        record_lobby_left("ABCDEF", 7, 1);
        record_lobby_preferences_set(&LobbyPreferences::default());
        record_queue_entered(
            &queued_lobby(vec![GameType::Solo], MatchmakingPool::Public),
            ADMISSION_APPLIED,
        );
        record_queue_left("ABCDEF", 7);
        record_match_committed(1, 5, 2, MatchmakingPool::Public);
        record_game_started(1, &state, 2);
        record_session_started("s_1", None, 12);
        record_session_ended(10, "socket_closed");
    }

    #[test]
    fn installing_twice_is_ignored_rather_than_fatal() {
        let (first, _rx1) = AnalyticsEmitter::new(EmitterConfig { buffer: 4 });
        let (second, _rx2) = AnalyticsEmitter::new(EmitterConfig { buffer: 4 });
        let origin = EventOrigin {
            environment: "test".to_owned(),
            region: "use1".to_owned(),
            aws_region: "us-east-1".to_owned(),
            instance_id: "1:boot".to_owned(),
        };
        install(first, origin.clone());
        install(second, origin);
        assert!(is_installed());
    }

    #[test]
    fn a_created_lobby_carries_its_code_pool_and_host() {
        let event = lobby_created_event(&origin(), "ABCDEF", 7, MatchmakingPool::Public);
        assert_eq!(event.event_name, "lobby_created");
        assert_eq!(event.identity.as_ref().unwrap().user_id, Some(7));
        let proto::event::Payload::LobbyCreated(created) = payload(&event) else {
            panic!("expected lobby_created");
        };
        assert_eq!(created.lobby_id, "ABCDEF");
        assert_eq!(created.pool, "public");
    }

    /// A stress lobby must be labelled as such at creation, so load-test
    /// traffic can be excluded from every downstream funnel.
    #[test]
    fn a_stress_pool_lobby_is_flagged_on_the_identity() {
        let event = lobby_created_event(&origin(), "ABCDEF", 7, MatchmakingPool::Stress);
        assert!(event.identity.as_ref().unwrap().is_stress_test);
    }

    /// The count describes the lobby once the joiner is in it, which is the
    /// fact a funnel wants — not the membership a moment before.
    #[test]
    fn a_join_counts_the_membership_including_the_joiner() {
        let event = lobby_joined_event(&origin(), "ABCDEF", 9, &members(&[3, 4]));
        let proto::event::Payload::LobbyJoined(joined) = payload(&event) else {
            panic!("expected lobby_joined");
        };
        assert_eq!(joined.lobby_id, "ABCDEF");
        assert_eq!(joined.member_count, 3);
    }

    /// A make-before-break reconnect re-joins a lobby the user is already in.
    /// Counting them twice would invent a member that does not exist.
    #[test]
    fn rejoining_does_not_double_count_the_same_user() {
        let event = lobby_joined_event(&origin(), "ABCDEF", 3, &members(&[3, 4]));
        let proto::event::Payload::LobbyJoined(joined) = payload(&event) else {
            panic!("expected lobby_joined");
        };
        assert_eq!(joined.member_count, 2);
    }

    #[test]
    fn a_leave_reports_the_membership_that_remains() {
        let event = lobby_left_event(&origin(), "ABCDEF", 3, 1);
        assert_eq!(event.event_name, "lobby_left");
        let proto::event::Payload::LobbyLeft(left) = payload(&event) else {
            panic!("expected lobby_left");
        };
        assert_eq!(left.lobby_id, "ABCDEF");
        assert_eq!(left.member_count, 1);
    }

    /// Preferences are a set of modes plus a competitive flag. They are
    /// rendered as the same strings a completed game reports, so a preference
    /// row joins against an outcome row without a translation table.
    #[test]
    fn preferences_render_as_joinable_mode_and_queue_strings() {
        let event = lobby_preferences_set_event(
            &origin(),
            &LobbyPreferences {
                selected_modes: vec!["duel".to_owned(), "ffa".to_owned()],
                competitive: true,
            },
        );
        assert_eq!(event.event_name, "lobby_preferences_set");
        let proto::event::Payload::LobbyPreferencesSet(prefs) = payload(&event) else {
            panic!("expected lobby_preferences_set");
        };
        assert_eq!(prefs.game_type, "duel|ffa");
        assert_eq!(prefs.queue_mode, format!("{:?}", QueueMode::Competitive));
    }

    #[test]
    fn a_casual_preference_reports_the_quickmatch_queue() {
        let event = lobby_preferences_set_event(&origin(), &LobbyPreferences::default());
        let proto::event::Payload::LobbyPreferencesSet(prefs) = payload(&event) else {
            panic!("expected lobby_preferences_set");
        };
        assert_eq!(prefs.queue_mode, format!("{:?}", QueueMode::Quickmatch));
    }

    /// A lobby queues for several game types at once; all of them belong in
    /// the event, and each renders exactly as `game_completed` renders it.
    #[test]
    fn queue_entry_carries_every_queued_game_type_and_the_admission() {
        let event = queue_entered_event(
            &origin(),
            &queued_lobby(
                vec![GameType::Solo, GameType::TeamMatch { per_team: 1 }],
                MatchmakingPool::Public,
            ),
            ADMISSION_APPLIED,
        );
        assert_eq!(event.event_name, "queue_entered");
        let proto::event::Payload::QueueEntered(entered) = payload(&event) else {
            panic!("expected queue_entered");
        };
        assert_eq!(
            entered.game_type,
            format!(
                "{:?}|{:?}",
                GameType::Solo,
                GameType::TeamMatch { per_team: 1 }
            )
        );
        assert_eq!(entered.queue_mode, format!("{:?}", QueueMode::Competitive));
        assert_eq!(entered.admission_result, "applied");
    }

    /// A refused admission is still a queue attempt. Recording it is the only
    /// way a rejection rate is visible at all.
    #[test]
    fn a_rejected_admission_is_still_recorded_as_such() {
        let event = queue_entered_event(
            &origin(),
            &queued_lobby(vec![GameType::Solo], MatchmakingPool::Public),
            ADMISSION_REJECTED,
        );
        let proto::event::Payload::QueueEntered(entered) = payload(&event) else {
            panic!("expected queue_entered");
        };
        assert_eq!(entered.admission_result, "rejected");
    }

    #[test]
    fn a_queue_cancel_reports_the_wait_it_ended() {
        let event = queue_left_event(&origin(), 7, 4_200);
        assert_eq!(event.event_name, "queue_left");
        assert_eq!(event.identity.as_ref().unwrap().user_id, Some(7));
        let proto::event::Payload::QueueLeft(left) = payload(&event) else {
            panic!("expected queue_left");
        };
        assert_eq!(left.waited_ms, 4_200);
    }

    #[test]
    fn a_commit_reports_the_game_wait_and_roster_size() {
        let event = match_committed_event(&origin(), 42, 1_500, 4, MatchmakingPool::Public);
        assert_eq!(event.event_name, "match_committed");
        let proto::event::Payload::MatchCommitted(committed) = payload(&event) else {
            panic!("expected match_committed");
        };
        assert_eq!(committed.game_id, 42);
        assert_eq!(committed.wait_ms, 1_500);
        assert_eq!(committed.player_count, 4);
    }

    /// A clock that stepped backwards must not produce a negative duration,
    /// which would poison every aggregate built on the column.
    #[test]
    fn a_negative_wait_is_clamped_rather_than_recorded() {
        let event = match_committed_event(&origin(), 42, -5, 2, MatchmakingPool::Public);
        let proto::event::Payload::MatchCommitted(committed) = payload(&event) else {
            panic!("expected match_committed");
        };
        assert_eq!(committed.wait_ms, 0);
    }

    #[test]
    fn a_started_game_projects_its_type_queue_and_roster() {
        let state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 2 },
            QueueMode::Competitive,
            Some(1),
            0,
        );
        let event = game_started_event(&origin(), 42, &state, 4);
        assert_eq!(event.event_name, "game_started");
        let proto::event::Payload::GameStarted(started) = payload(&event) else {
            panic!("expected game_started");
        };
        assert_eq!(started.game_id, 42);
        assert_eq!(
            started.game_type,
            format!("{:?}", GameType::TeamMatch { per_team: 2 })
        );
        assert_eq!(started.queue_mode, format!("{:?}", QueueMode::Competitive));
        assert_eq!(started.player_count, 4);
    }

    /// A stress game must carry the flag from its own state, so a load test
    /// never lands in a product funnel.
    #[test]
    fn a_stress_game_start_is_flagged_from_the_game_state() {
        let mut state = GameState::new(40, 40, GameType::Solo, QueueMode::Quickmatch, Some(1), 0);
        state.is_stress_test = true;
        let event = game_started_event(&origin(), 42, &state, 1);
        assert!(event.identity.as_ref().unwrap().is_stress_test);
    }

    /// The session id is the only thing that ties later events on a socket
    /// together, so it must be on the identity, not buried in the payload.
    #[test]
    fn a_session_start_carries_the_session_and_anon_ids() {
        let event = session_started_event(&origin(), "s_abc", Some("anon-1"), 12);
        assert_eq!(event.event_name, "session_started");
        let identity = event.identity.as_ref().unwrap();
        assert_eq!(identity.session_id.as_deref(), Some("s_abc"));
        assert_eq!(identity.anon_id.as_deref(), Some("anon-1"));
        assert_eq!(identity.user_id, None, "the account is not known yet");
        let proto::event::Payload::SessionStarted(started) = payload(&event) else {
            panic!("expected session_started");
        };
        assert_eq!(started.protocol_version, 12);
    }

    /// A client that predates the anon id still authenticates, and its session
    /// event must simply omit the field rather than invent one.
    #[test]
    fn a_session_without_an_anon_id_omits_it() {
        let event = session_started_event(&origin(), "s_abc", None, 12);
        assert_eq!(event.identity.as_ref().unwrap().anon_id, None);
    }

    #[test]
    fn a_session_end_carries_its_duration_and_reason() {
        let event = session_ended_event(&origin(), 9_000, "socket_closed");
        assert_eq!(event.event_name, "session_ended");
        let proto::event::Payload::SessionEnded(ended) = payload(&event) else {
            panic!("expected session_ended");
        };
        assert_eq!(ended.duration_ms, 9_000);
        assert_eq!(ended.close_reason, "socket_closed");
    }

    // -----------------------------------------------------------------------
    // Queue-wait bookkeeping.
    // -----------------------------------------------------------------------

    fn ledger() -> Mutex<HashMap<String, i64>> {
        Mutex::new(HashMap::new())
    }

    #[test]
    fn an_applied_admission_starts_the_clock_for_a_later_cancel() {
        let entries = ledger();
        let lobby = queued_lobby(vec![GameType::Solo], MatchmakingPool::Public);
        remember_queue_entry(&entries, &lobby, ADMISSION_APPLIED);
        assert_eq!(take_queue_entry(&entries, "ABCDEF"), Some(1_000));
    }

    /// A deduplicated retry must not restart the wait: the user has been
    /// queueing since the first request.
    #[test]
    fn a_deduplicated_admission_keeps_the_original_wait_start() {
        let entries = ledger();
        let mut lobby = queued_lobby(vec![GameType::Solo], MatchmakingPool::Public);
        remember_queue_entry(&entries, &lobby, ADMISSION_APPLIED);
        lobby.queued_at = 9_999;
        remember_queue_entry(&entries, &lobby, ADMISSION_IDEMPOTENT);
        assert_eq!(take_queue_entry(&entries, "ABCDEF"), Some(1_000));
    }

    /// A refused admission queued nothing, so there is no wait to remember and
    /// a later cancel must not attribute one.
    #[test]
    fn a_refused_admission_records_no_wait() {
        let entries = ledger();
        let lobby = queued_lobby(vec![GameType::Solo], MatchmakingPool::Public);
        remember_queue_entry(&entries, &lobby, ADMISSION_REJECTED);
        remember_queue_entry(&entries, &lobby, ADMISSION_INTEGRITY_ERROR);
        remember_queue_entry(&entries, &lobby, ADMISSION_UNKNOWN);
        assert_eq!(take_queue_entry(&entries, "ABCDEF"), None);
    }

    /// Taking the entry must consume it, so a second cancel for the same lobby
    /// cannot report a wait that already ended.
    #[test]
    fn a_wait_is_consumed_by_the_cancel_that_reports_it() {
        let entries = ledger();
        let lobby = queued_lobby(vec![GameType::Solo], MatchmakingPool::Public);
        remember_queue_entry(&entries, &lobby, ADMISSION_APPLIED);
        assert!(take_queue_entry(&entries, "ABCDEF").is_some());
        assert_eq!(take_queue_entry(&entries, "ABCDEF"), None);
    }

    /// The map lives inside a gameplay process. It must be bounded no matter
    /// how many lobbies leave a queue by a path that never cancels.
    #[test]
    fn the_wait_ledger_never_grows_past_its_cap() {
        let mut entries: HashMap<String, i64> = HashMap::new();
        for index in 0..MAX_TRACKED_QUEUE_ENTRIES {
            evict_if_full(&mut entries);
            // Stamped long ago, so the sweep is what has to reclaim them.
            entries.insert(format!("lobby-{index}"), 0);
        }
        let before_sweep = entries.len();
        evict_if_full(&mut entries);
        assert_eq!(before_sweep, MAX_TRACKED_QUEUE_ENTRIES);
        assert!(
            entries.is_empty(),
            "stale stamps must be reclaimed once the cap is reached"
        );
        entries.insert("fresh".to_owned(), now_ms());
        assert!(entries.contains_key("fresh"));
        assert!(entries.len() <= MAX_TRACKED_QUEUE_ENTRIES);
    }
}
