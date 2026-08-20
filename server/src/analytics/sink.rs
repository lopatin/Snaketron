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
use std::sync::{Arc, Mutex, OnceLock};

use common::GameState;

use crate::completion::CompletionRecordV1;
use crate::lobby_manager::{LobbyMember, LobbyPreferences};
use crate::matchmaking_manager::QueuedLobby;
use crate::matchmaking_pool::MatchmakingPool;

use super::emitter::AnalyticsEmitter;
use super::event::{EventIdentity, EventOrigin, envelope, now_ms};
use super::proto;
use super::ws_sink::{Account, CloseReason, WsConnection};

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

/// Emitted the moment a websocket is accepted, before a single frame is read.
///
/// Carries no account, because at accept nothing is known about who is on the
/// socket and a placeholder would be a join key to nothing. It carries the
/// connection id, which is the one thing that IS known: it is what lets this
/// accept be joined to its own close, and without it a socket that never
/// authenticated has no key at all.
///
/// Its job is to be COUNTED: it is the denominator every connection-level
/// funnel divides by, and it is the one arm no rejection downstream can
/// suppress.
pub fn record_connection_started(connection: &WsConnection) {
    let Some(sink) = SINK.get() else { return };
    sink.emitter.emit(connection_started_event(
        &sink.origin,
        connection.connection_id(),
    ));
}

/// Emitted when authentication SUCCEEDS, and only then.
///
/// This is what makes the event mean what its name says. It used to fire the
/// moment a session id was minted — BEFORE the token was verified — which made
/// it a count of attempts that could carry no account. The attempts are now
/// counted by `connection_ended`, whose close reason names the refusal and
/// whose `protocol_version` names the cohort, so moving this behind
/// verification deletes nothing from the funnel and lets it carry the full
/// identity it always should have had.
pub fn record_session_started(connection: &WsConnection, account: Account) {
    // Recorded on the connection FIRST and unconditionally, sink or no sink:
    // this is what pairs the eventual `session_ended` with this event, and a
    // deployment without analytics must not leave a connection believing it
    // never had a session.
    connection.start_session(account);
    let Some(sink) = SINK.get() else { return };
    sink.emitter.emit(session_started_event(
        &sink.origin,
        &SessionStart::observed(connection, account),
    ));
}

/// Emitted where the socket's own task finishes — for EVERY connection.
///
/// Reads the lifecycle off the connection context instead of taking it as
/// arguments, because the facts were learned at four different places and the
/// close site is none of them. That is also the defect this replaces: the old
/// call sat above `handle_websocket`, which returns `()`, so the event could
/// name neither the account nor the session it was ending.
///
/// `session_ended` rides along, but only for a connection that actually
/// carried a session. Emitting one for every socket would put unverified
/// attempts straight back into the authenticated-session count this split
/// exists to clean.
pub fn record_connection_closed(connection: &WsConnection) {
    let Some(sink) = SINK.get() else { return };
    for event in connection_close_events(&sink.origin, &ConnectionClose::observed(connection)) {
        sink.emitter.emit(event);
    }
}

/// Everything a closing websocket is described by, read off the connection
/// context in exactly one place.
#[derive(Debug, Clone)]
pub struct ConnectionClose {
    /// The same id the matching `connection_started` reported. Read off the
    /// connection, so the two halves cannot name different sockets.
    pub connection_id: String,
    pub duration_ms: i64,
    pub close_reason: CloseReason,
    pub protocol_version: Option<u16>,
    pub session_id: Option<Arc<str>>,
    pub anon_id: Option<Arc<str>>,
    /// The account the session authenticated as. `None` means this connection
    /// never started a session, and is what suppresses `session_ended`.
    pub session_account: Option<Account>,
}

impl ConnectionClose {
    fn observed(connection: &WsConnection) -> Self {
        Self {
            connection_id: connection.connection_id().to_owned(),
            duration_ms: connection.elapsed_ms(),
            close_reason: connection.close_reason(),
            protocol_version: connection.protocol_version(),
            session_id: connection.session_id(),
            anon_id: connection.anon_id(),
            session_account: connection.session_account(),
        }
    }
}

/// The handshake facts a started session is described by.
#[derive(Debug, Clone)]
pub struct SessionStart {
    pub session_id: Option<Arc<str>>,
    pub anon_id: Option<Arc<str>>,
    pub protocol_version: Option<u16>,
    pub account: Account,
}

impl SessionStart {
    fn observed(connection: &WsConnection, account: Account) -> Self {
        Self {
            session_id: connection.session_id(),
            anon_id: connection.anon_id(),
            protocol_version: connection.protocol_version(),
            account,
        }
    }
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

fn connection_started_event(origin: &EventOrigin, connection_id: &str) -> proto::Event {
    envelope(
        origin,
        // Not "unknown identity": there is genuinely nothing to say about WHO
        // is on this socket yet. The handshake has not been read.
        //
        // `is_guest` therefore stays `false`, which is the convention every
        // durable event in `raw/game-events/` uses — `true` is a positive claim
        // about a verified guest account, never a stand-in for "unknown".
        // `websocket_message` inverts that, and this event must not inherit the
        // inversion by proximity: an accept marked `is_guest = true` would make
        // every connection look like a guest one.
        EventIdentity::default(),
        proto::event::Payload::ConnectionStarted(proto::ConnectionStarted {
            connection_id: connection_id.to_owned(),
        }),
    )
}

fn session_started_event(origin: &EventOrigin, start: &SessionStart) -> proto::Event {
    envelope(
        origin,
        EventIdentity {
            user_id: Some(i64::from(start.account.user_id)),
            anon_id: start.anon_id.as_deref().map(str::to_owned),
            session_id: start.session_id.as_deref().map(str::to_owned),
            is_guest: start.account.is_guest,
            is_stress_test: start.account.is_stress_test,
        },
        proto::event::Payload::SessionStarted(proto::SessionStarted {
            // Zero only for a handshake shape that carried no version at all.
            // Every such shape is refused before it can authenticate, so in
            // practice this is always the version the client reported.
            protocol_version: start.protocol_version.map_or(0, i64::from),
        }),
    )
}

/// The identity a closing connection ended up with.
///
/// The account and the session id are reported only when a session actually
/// STARTED. A session id minted for a handshake that then failed verification
/// names no `session_started` row, so carrying it would be a join key to
/// nothing that looks exactly like a join key to something — and it would make
/// "did this connection have a session?" unanswerable from the row itself.
///
/// The anon id is carried either way, because the refused connections are
/// precisely where it earns its keep: it is what says whether the clients a
/// rollout is rejecting are returning browsers or first-time ones.
///
/// `is_guest` is `false` unless a verified guest account is behind the session,
/// matching `connection_started` and every other durable event. It is NOT
/// `websocket_message`'s inverted convention, where `true` means "no account
/// known": the two halves of this pair have to agree, or a guest split taken
/// across accepts and closes would disagree with itself.
fn close_identity(close: &ConnectionClose) -> EventIdentity {
    EventIdentity {
        user_id: close
            .session_account
            .map(|account| i64::from(account.user_id)),
        anon_id: close.anon_id.as_deref().map(str::to_owned),
        session_id: close
            .session_account
            .and_then(|_| close.session_id.as_deref().map(str::to_owned)),
        is_guest: close
            .session_account
            .is_some_and(|account| account.is_guest),
        is_stress_test: close
            .session_account
            .is_some_and(|account| account.is_stress_test),
    }
}

/// The events one closing websocket produces.
///
/// Always a `connection_ended`; a `session_ended` as well, and only, when the
/// connection actually carried a session. The session is the inner scope, so
/// it is reported as ending first.
fn connection_close_events(origin: &EventOrigin, close: &ConnectionClose) -> Vec<proto::Event> {
    let identity = close_identity(close);
    // Clamped rather than trusted: a duration is a subtraction, and a negative
    // one would be read downstream as a real (impossibly fast) session.
    let duration_ms = close.duration_ms.max(0);
    let close_reason = close.close_reason.as_str().to_owned();

    let mut events = Vec::with_capacity(2);
    if close.session_account.is_some() {
        events.push(envelope(
            origin,
            identity.clone(),
            proto::event::Payload::SessionEnded(proto::SessionEnded {
                duration_ms,
                close_reason: close_reason.clone(),
            }),
        ));
    }
    events.push(envelope(
        origin,
        identity,
        proto::event::Payload::ConnectionEnded(proto::ConnectionEnded {
            duration_ms,
            close_reason,
            protocol_version: close.protocol_version.map(i64::from),
            // Unconditional. A close that sometimes reports the id would make
            // the accept/close join sometimes possible, which is the same as
            // never for a query that has to be right.
            connection_id: close.connection_id.clone(),
        }),
    ));
    events
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
        let connection = WsConnection::new("inert");
        record_connection_started(&connection);
        record_session_started(&connection, account(7));
        record_connection_closed(&connection);
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

    fn account(user_id: i32) -> Account {
        Account {
            user_id,
            is_guest: false,
            is_stress_test: false,
        }
    }

    fn session_start(account: Account) -> SessionStart {
        SessionStart {
            session_id: Some(Arc::from("s_abc")),
            anon_id: Some(Arc::from("anon-1")),
            protocol_version: Some(12),
            account,
        }
    }

    /// A close with nothing behind it: the shape a refused handshake produces.
    fn refused_close(protocol_version: Option<u16>, close_reason: CloseReason) -> ConnectionClose {
        ConnectionClose {
            connection_id: "conn-1".to_owned(),
            duration_ms: 40,
            close_reason,
            protocol_version,
            // A session id IS minted before verification, so a refused
            // connection genuinely holds one. It must not be reported.
            session_id: Some(Arc::from("s_abc")),
            anon_id: Some(Arc::from("anon-1")),
            session_account: None,
        }
    }

    fn named(events: &[proto::Event], name: &str) -> Option<usize> {
        events.iter().position(|event| event.event_name == name)
    }

    /// The point of moving this event behind verification: it can finally name
    /// the account. A `session_started` without a `user_id` is the defect.
    #[test]
    fn a_session_start_carries_the_account_it_authenticated_as() {
        let event = session_started_event(
            &origin(),
            &session_start(Account {
                user_id: 4242,
                is_guest: true,
                is_stress_test: false,
            }),
        );
        assert_eq!(event.event_name, "session_started");
        let identity = event.identity.as_ref().unwrap();
        assert_eq!(identity.user_id, Some(4242));
        assert!(identity.is_guest);
        assert_eq!(identity.session_id.as_deref(), Some("s_abc"));
        assert_eq!(identity.anon_id.as_deref(), Some("anon-1"));
        let proto::event::Payload::SessionStarted(started) = payload(&event) else {
            panic!("expected session_started");
        };
        assert_eq!(started.protocol_version, 12);
    }

    /// A client that predates the anon id still authenticates, and its session
    /// event must simply omit the field rather than invent one.
    #[test]
    fn a_session_without_an_anon_id_omits_it() {
        let event = session_started_event(
            &origin(),
            &SessionStart {
                anon_id: None,
                ..session_start(account(7))
            },
        );
        assert_eq!(event.identity.as_ref().unwrap().anon_id, None);
    }

    /// A load-test connection has to be labelled at the session, or every
    /// funnel built on these events silently counts synthetic traffic.
    #[test]
    fn a_stress_account_is_flagged_on_the_session() {
        let event = session_started_event(
            &origin(),
            &session_start(Account {
                user_id: 9,
                is_guest: false,
                is_stress_test: true,
            }),
        );
        assert!(event.identity.as_ref().unwrap().is_stress_test);
    }

    /// The funnel-deletion trap, at the projection level: a connection that
    /// never authenticated produces a `connection_ended` and NOTHING else. If a
    /// `session_ended` appeared here, every unverified attempt would be counted
    /// as an authenticated session that ended.
    #[test]
    fn a_close_without_a_session_reports_only_the_socket() {
        let events = connection_close_events(
            &origin(),
            &refused_close(Some(7), CloseReason::AuthenticationFailed),
        );
        assert_eq!(
            events
                .iter()
                .map(|e| e.event_name.as_str())
                .collect::<Vec<_>>(),
            vec!["connection_ended"],
        );
        let identity = events[0].identity.as_ref().unwrap();
        assert_eq!(identity.user_id, None);
        assert_eq!(
            identity.session_id, None,
            "a session id minted for a handshake that failed names no \
             session_started row, so reporting it would be a join key to nothing"
        );
        assert_eq!(
            identity.anon_id.as_deref(),
            Some("anon-1"),
            "the browser behind a refused client is exactly what a rollout needs"
        );
    }

    /// The whole reason the version lives on the connection instead of in the
    /// handshake's scope: for a rejected client this row is the ONLY evidence
    /// of which version was rejected.
    #[test]
    fn a_rejected_client_still_reports_the_version_it_asked_for() {
        let events = connection_close_events(
            &origin(),
            &refused_close(Some(3), CloseReason::ProtocolRejected),
        );
        let proto::event::Payload::ConnectionEnded(ended) = payload(&events[0]) else {
            panic!("expected connection_ended");
        };
        assert_eq!(ended.protocol_version, Some(3));
        assert_eq!(ended.close_reason, "protocol_rejected");
        assert_eq!(ended.duration_ms, 40);
    }

    /// Absent, never zero: a socket that closed before its handshake has no
    /// version, and a zero-defaulted column would show it as a real version-0
    /// cohort in exactly the rollout query this field exists for.
    #[test]
    fn a_socket_that_never_handshook_reports_no_version_rather_than_zero() {
        let events =
            connection_close_events(&origin(), &refused_close(None, CloseReason::SocketClosed));
        let proto::event::Payload::ConnectionEnded(ended) = payload(&events[0]) else {
            panic!("expected connection_ended");
        };
        assert_eq!(ended.protocol_version, None);
    }

    /// A connection that HAD a session ends both, and both name the account —
    /// the attribution the old, unattributed `session_ended` could not carry.
    #[test]
    fn a_close_with_a_session_ends_both_and_both_name_the_account() {
        let events = connection_close_events(
            &origin(),
            &ConnectionClose {
                connection_id: "conn-9".to_owned(),
                duration_ms: 9_000,
                close_reason: CloseReason::SocketClosed,
                protocol_version: Some(12),
                session_id: Some(Arc::from("s_abc")),
                anon_id: Some(Arc::from("anon-1")),
                session_account: Some(Account {
                    user_id: 77,
                    is_guest: true,
                    is_stress_test: false,
                }),
            },
        );
        assert!(named(&events, "session_ended").is_some());
        assert!(named(&events, "connection_ended").is_some());
        assert_eq!(events.len(), 2, "exactly one of each");
        for event in &events {
            let identity = event.identity.as_ref().unwrap();
            assert_eq!(
                identity.user_id,
                Some(77),
                "{} lost the account",
                event.event_name
            );
            assert!(
                identity.is_guest,
                "{} lost the guest flag",
                event.event_name
            );
            assert_eq!(identity.session_id.as_deref(), Some("s_abc"));
        }
        let proto::event::Payload::SessionEnded(ended) =
            payload(&events[named(&events, "session_ended").unwrap()])
        else {
            panic!("expected session_ended");
        };
        assert_eq!(ended.duration_ms, 9_000);
        assert_eq!(ended.close_reason, "socket_closed");
    }

    /// The account is read from the SESSION, not from the connection's live
    /// per-frame attribution — that one is cleared when a connection falls back
    /// to unauthenticated, and the session still belonged to somebody.
    #[test]
    fn a_session_that_ended_unauthenticated_still_names_who_it_was() {
        let connection = WsConnection::new("close-after-deauth");
        connection.bind_session("s_abc");
        record_session_started(&connection, account(77));
        // Exactly what the connection loop does when a message fails: publish
        // the reset state, clearing the live account.
        connection.set_account(None);

        let close = ConnectionClose::observed(&connection);
        assert_eq!(close.session_account, Some(account(77)));
        let events = connection_close_events(&origin(), &close);
        assert!(named(&events, "session_ended").is_some());
        assert_eq!(
            events[0].identity.as_ref().unwrap().user_id,
            Some(77),
            "the session's identity must survive the connection losing it"
        );
    }

    /// A duration is a subtraction, and a negative one would read downstream as
    /// a real, impossibly fast session.
    #[test]
    fn a_negative_duration_is_clamped_rather_than_emitted() {
        let events = connection_close_events(
            &origin(),
            &ConnectionClose {
                duration_ms: -5,
                ..refused_close(Some(12), CloseReason::SocketClosed)
            },
        );
        let proto::event::Payload::ConnectionEnded(ended) = payload(&events[0]) else {
            panic!("expected connection_ended");
        };
        assert_eq!(ended.duration_ms, 0);
    }

    /// The accept knows the socket, and nothing else. `is_guest` staying
    /// `false` is the load-bearing half: `websocket_message` stamps `true` for
    /// "no account known", and an accept that inherited that convention would
    /// make every connection in `raw/game-events/` look like a guest one.
    #[test]
    fn a_connection_start_names_the_socket_and_nobody_on_it() {
        let event = connection_started_event(&origin(), "conn-1");
        assert_eq!(event.event_name, "connection_started");
        let identity = event.identity.as_ref().unwrap();
        assert_eq!(identity.user_id, None);
        assert_eq!(identity.session_id, None);
        assert_eq!(identity.anon_id, None);
        assert!(
            !identity.is_guest,
            "an accept knows no account, and `false` — not `true` — is what \
             this tier means by that"
        );
        let proto::event::Payload::ConnectionStarted(started) = payload(&event) else {
            panic!("expected connection_started");
        };
        assert_eq!(
            started.connection_id, "conn-1",
            "the id is the only join key an unauthenticated socket ever has"
        );
    }

    /// The pairing, at the projection level: one connection's accept and its
    /// own close must carry the same id, and it must be the id the connection
    /// itself was built with — not one minted at either end.
    #[test]
    fn an_accept_and_its_own_close_carry_the_same_connection_id() {
        let connection = WsConnection::new("ws-pairing");
        let started = connection_started_event(&origin(), connection.connection_id());
        let closed = connection_close_events(&origin(), &ConnectionClose::observed(&connection));

        let proto::event::Payload::ConnectionStarted(start) = payload(&started) else {
            panic!("expected connection_started");
        };
        let proto::event::Payload::ConnectionEnded(end) = payload(&closed[0]) else {
            panic!("expected connection_ended");
        };
        assert_eq!(start.connection_id, "ws-pairing");
        assert_eq!(
            end.connection_id, start.connection_id,
            "an accept that cannot be joined to its own close answers no \
             per-connection question at all"
        );
    }

    /// Two live connections must not share an id. If the id came from anywhere
    /// process-wide these would collide, and every per-connection query would
    /// silently fold two sockets into one.
    #[test]
    fn two_connections_never_share_an_id() {
        let first = WsConnection::new("ws-first");
        let second = WsConnection::new("ws-second");
        let ends: Vec<String> = [&first, &second]
            .into_iter()
            .map(|connection| {
                let events =
                    connection_close_events(&origin(), &ConnectionClose::observed(connection));
                match payload(&events[0]) {
                    proto::event::Payload::ConnectionEnded(ended) => ended.connection_id,
                    other => panic!("expected connection_ended, got {other:?}"),
                }
            })
            .collect();
        assert_eq!(ends, vec!["ws-first".to_owned(), "ws-second".to_owned()]);
    }

    /// A close with a verified guest account is the ONE shape that may set
    /// `is_guest`, and a close with no account may not — the inverted
    /// `websocket_message` convention would report every refused handshake as
    /// a guest.
    #[test]
    fn is_guest_is_a_claim_about_a_verified_account_not_about_ignorance() {
        let refused = connection_close_events(
            &origin(),
            &refused_close(Some(12), CloseReason::AuthenticationFailed),
        );
        assert!(
            !refused[0].identity.as_ref().unwrap().is_guest,
            "nobody was verified, so nothing may be claimed about a guest"
        );

        let guest = connection_close_events(
            &origin(),
            &ConnectionClose {
                session_account: Some(Account {
                    user_id: 5,
                    is_guest: true,
                    is_stress_test: false,
                }),
                ..refused_close(Some(12), CloseReason::SocketClosed)
            },
        );
        for event in &guest {
            assert!(
                event.identity.as_ref().unwrap().is_guest,
                "{} lost the verified guest claim",
                event.event_name
            );
        }
    }

    /// Every reason renders as a stable label, because these become column
    /// values that a saved query filters on.
    #[test]
    fn close_reasons_render_as_stable_labels() {
        assert_eq!(CloseReason::SocketClosed.as_str(), "socket_closed");
        assert_eq!(CloseReason::ProtocolRejected.as_str(), "protocol_rejected");
        assert_eq!(
            CloseReason::AuthenticationFailed.as_str(),
            "authentication_failed"
        );
        assert_eq!(CloseReason::ConnectionError.as_str(), "connection_error");
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
