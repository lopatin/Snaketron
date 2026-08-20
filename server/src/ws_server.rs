use crate::ads::{
    AdBreakResolution, AdsConfig, ClientAdsConfig, ClientDistribution, LobbyAdBreak,
    LobbyAdBreakView, MAX_AD_BREAK_PARTICIPANTS, lobby_meets_game_threshold,
};
use crate::api::auth::validate_username;
use crate::challenges::{
    CHALLENGE_TTL_MS, Challenge, ChallengeInbox, ChallengeState, ChallengeStore, new_challenge_id,
    now_ms,
};
use crate::chat_filter::filter_chat_message;
use crate::cluster_membership::ClusterNamespace;
use crate::db::{Database, models::RuntimeAdsConfig};
use crate::game_bus::GameBus;
use crate::game_executor::PARTITION_COUNT;
use crate::game_executor::StreamEvent;
use crate::lifecycle::{DrainNotice, TaskLifecycle, WS_PROTOCOL_VERSION};
use crate::lobby_manager;
use crate::lobby_manager::{
    AdBreakResolutionResult, BeginAdBreakResult, LobbyJoinHandle, LobbyMember, MAX_LOBBY_MEMBERS,
    lobby_membership_valid_until_ms,
};
use crate::matchmaking_manager::{
    ActiveMatch, LobbyAdmissionRejected, LobbyMembershipFence, MATCHMAKING_GAME_TYPES,
    MatchmakingManager,
};
use crate::matchmaking_pool::MatchmakingPool;
use crate::presence::{PresenceActivity, PresenceRegistry, RegionRoster};
use crate::pubsub_manager::PubSubManager;
use crate::recovery::{
    CommandOutcome, RecoveryEnvelopeV2, ResolvedCommandState, SessionCommandOutcomes,
    SessionCommandRejectionFence, validate_client_command_identity,
};
use crate::redis_keys::RedisKeys;
use crate::redis_utils::RedisConnection;
use crate::rematch::{RematchState, RematchStore};
use crate::user_cache::UserCache;
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use common::{
    ClientCommandIdentityV2, GameCommandMessage, GameEvent, GameEventMessage, GameState, GameStatus,
};
use futures_util::SinkExt;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::sync::{Mutex, broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Deterministic simulation requires both peers to run the same gameplay
/// rules. In particular, protocol 8 changes scoring and physical growth, so an
/// older predictive engine cannot safely continue against this server.
fn validate_client_protocol_version(protocol_version: Option<u16>) -> Result<()> {
    match protocol_version {
        Some(version) if version == WS_PROTOCOL_VERSION => Ok(()),
        Some(version) => Err(anyhow!(
            "Gameplay update required: client protocol {version}, server protocol {WS_PROTOCOL_VERSION}"
        )),
        None => Err(anyhow!(
            "Gameplay update required: client did not report a protocol version; server protocol {WS_PROTOCOL_VERSION}"
        )),
    }
}

/// Canonical lowercase hyphenated UUID, matching `isValidAnonId` in
/// `client/web/utils/anonId.ts`.
fn is_canonical_uuid(value: &str) -> bool {
    const GROUPS: [usize; 5] = [8, 4, 4, 4, 12];
    let mut groups = value.split('-');
    for expected in GROUPS {
        match groups.next() {
            Some(group)
                if group.len() == expected
                    && group
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) => {}
            _ => return false,
        }
    }
    groups.next().is_none()
}

/// Accepts the client-reported analytics identifier only in its exact expected
/// shape, dropping anything else.
///
/// This is untrusted, attacker-controlled input that is destined for an
/// analytics event, so it is validated at the boundary rather than downstream:
/// an unbounded string here would become an unbounded column value, and a
/// non-UUID value would silently pollute retention analysis. Rejection is
/// deliberately silent — a malformed id is an analytics gap, never a reason to
/// refuse a player's connection.
/// A session identifier, minted server-side at authentication.
///
/// UUIDv7 so it sorts by creation time, which makes a session's events cluster
/// naturally in the analytics table.
pub fn new_session_id() -> String {
    format!("s_{}", uuid::Uuid::now_v7())
}

fn sanitize_anon_id(anon_id: Option<String>) -> Option<String> {
    anon_id.filter(|candidate| is_canonical_uuid(candidate))
}

// Snapshot-bearing messages are serialized envelopes; boxing would add churn without a win.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum WSMessage {
    /// Legacy authentication shape. It remains parseable so the server can
    /// return an explicit update-required denial instead of a malformed-frame
    /// error, but it is no longer admitted to deterministic gameplay.
    Token(String),
    /// Client -> server authentication. The reported version must exactly
    /// match [`WS_PROTOCOL_VERSION`].
    Authenticate {
        token: String,
        protocol_version: u16,
        /// Advisory pseudonymous browser identifier for product analytics.
        /// Never used for authentication or authorization, and never trusted:
        /// `sanitize_anon_id` validates it before anything downstream sees it.
        /// Optional and defaulted so a client that predates the field — an
        /// itch.io bundle cannot update itself — still authenticates.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "ts-gen", ts(optional))]
        anon_id: Option<String>,
        /// Session build channel. A missing value resolves to a disabled ad
        /// policy because the client's available SDK is unknown.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        distribution: Option<ClientDistribution>,
    },
    JoinGame(u32),
    LeaveGame,
    /// At-least-once client command. The gateway canonicalizes `game_id` and
    /// `user_id` from the authenticated connection before publishing it.
    GameCommandV2 {
        command_id: ClientCommandIdentityV2,
        command: GameCommandMessage,
    },
    GameEvent(GameEventMessage),
    /// Executor-authored terminal command outcomes adjacent to a fresh
    /// snapshot. This is user/session filtered and never part of shared state.
    CommandOutcomes {
        game_id: u32,
        client_game_session_id: String,
        #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
        contiguous_through: u64,
        #[cfg_attr(feature = "ts-gen", ts(as = "BTreeMap<u32, CommandOutcome>"))]
        outcomes: BTreeMap<u64, CommandOutcome>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        rejection_fence: Option<SessionCommandRejectionFence>,
    },
    /// Ordered barrier emitted only after every outcome batch for the
    /// immediately preceding snapshot has reached this socket's send queue.
    /// Planned handoff clients use it instead of guessing from timing or from
    /// the absence of a per-session outcome batch. A terminal barrier also
    /// explicitly rejects every identity still unresolved at the client:
    /// commands can cross the bidirectional WebSocket while completion is
    /// already queued in the opposite direction.
    CommandOutcomesComplete {
        game_id: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        terminal_rejection_reason: Option<String>,
    },
    Chat(String),
    LobbyChatMessage {
        lobby_code: String,
        message_id: String,
        user_id: i32,
        username: String,
        message: String,
        #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
        timestamp_ms: i64,
    },
    GameChatMessage {
        game_id: u32,
        message_id: String,
        user_id: i32,
        username: String,
        message: String,
        #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
        timestamp_ms: i64,
    },
    LobbyChatHistory {
        lobby_code: String,
        messages: Vec<LobbyChatBroadcast>,
    },
    GameChatHistory {
        game_id: u32,
        messages: Vec<GameChatBroadcast>,
    },
    /// Server -> client acknowledgement sent only after token verification and
    /// user loading have completed.
    Authenticated {
        task_boot_id: String,
        protocol_version: u16,
        capabilities: Vec<String>,
        #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
        socket_generation: u64,
    },
    /// Server -> client distribution capability. Live pre-match authorization
    /// is carried by each lobby break's targeted user IDs.
    AdConfiguration(ClientAdsConfig),
    /// Client -> server: this player has read the pre-match briefing and is
    /// ready. `game_id` is echoed back for client-side routing only — the
    /// gateway canonicalizes it, and the player's identity, from the
    /// authenticated connection before publishing anything.
    PlayerReady {
        game_id: u32,
    },
    /// Client -> server: the client detected message loss or state divergence
    /// (stream_seq gap, repeated TickHash mismatch, or a silent feed) and
    /// needs its event subscription restarted with a fresh snapshot.
    RequestResync {
        game_id: u32,
    },
    Ping {
        #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
        client_time: i64,
    },
    Pong {
        #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
        client_time: i64,
        #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
        server_time: i64,
    },
    // Matchmaking messages
    QueueForMatch {
        game_type: common::GameType,
        queue_mode: common::QueueMode, // Quickmatch or Competitive
    },
    QueueForMatchMulti {
        game_types: Vec<common::GameType>,
        queue_mode: common::QueueMode, // Quickmatch or Competitive
    },
    LeaveQueue,
    // Real-time matchmaking updates
    MatchFound {
        game_id: u32,
    },
    QueueUpdate {
        position: u32,
        estimated_wait_seconds: u32,
    },
    QueueLeft,
    /// Client -> server terminal resolution for the current lobby ad break.
    /// Every outcome releases this participant; ad blocking is not an error.
    AdBreakResolved {
        break_id: String,
        resolution: AdBreakResolution,
    },
    UpdateNickname {
        nickname: String,
    },
    SpectatorJoined,
    AccessDenied {
        reason: String,
    },
    GameLoadFailed {
        game_id: u32,
        reason: String,
    },
    /// The game is known, but this ready gateway's local replica is still
    /// warming through an executor ownership gap. Clients retry the same join
    /// without surfacing a terminal error.
    GameWarming {
        game_id: u32,
        #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
        retry_after_ms: u64,
    },
    // Solo game responses
    SoloGameCreated {
        game_id: u32,
    },
    // Planned gateway handoff. Executor ownership is intentionally absent:
    // the replacement connection uses the same regional URL.
    Drain {
        task_boot_id: String,
        #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
        deadline_unix_ms: i64,
    },
    // Region user count updates
    UserCountUpdate {
        region_counts: std::collections::HashMap<String, u32>,
    },
    // Lobby messages
    CreateLobby,
    LobbyCreated {
        lobby_code: String,
    },
    JoinLobby {
        lobby_code: String,
        preferences: Option<lobby_manager::LobbyPreferences>,
    },
    JoinedLobby {
        lobby_code: String,
    },
    LeaveLobby,
    LeftLobby,
    LobbyUpdate {
        lobby_code: String,
        members: Vec<lobby_manager::LobbyMember>,
        host_user_id: i32,
        state: String,
        preferences: lobby_manager::LobbyPreferences,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ad_break: Option<LobbyAdBreakView>,
    },
    UpdateLobbyPreferences {
        selected_modes: Vec<String>,
        competitive: bool,
    },
    LobbyRegionMismatch {
        target_region: String,
        ws_url: String,
        lobby_code: String,
    },

    // === Social layer (protocol 11, capability `social-presence-v1`) ===
    /// Server -> client: everyone currently online in this connection's region.
    /// Pushed on authentication and whenever the region roster changes; the
    /// viewer is always absent from `players`.
    OnlinePlayers(RegionRoster),
    /// Client -> server: challenge one online player to a match. The server
    /// takes the challenger's identity from the connection — only the target
    /// is client-supplied.
    ChallengePlayer {
        user_id: u32,
    },
    /// Client -> server: answer a challenge addressed to this user. The server
    /// enforces that only the target may accept or decline.
    RespondToChallenge {
        challenge_id: String,
        accept: bool,
    },
    /// Client -> server: withdraw a challenge this user issued.
    CancelChallenge {
        challenge_id: String,
    },
    /// Server -> client: the complete challenge state for this user. Always a
    /// full snapshot rather than a delta, so it is idempotent across a socket
    /// handoff, a dropped hint, or a reconnect.
    Challenges(ChallengeInbox),
    /// Server -> client: an accepted challenge, addressed to both players. The
    /// client joins `lobby_code` to land in the challenger's lobby.
    ChallengeAccepted {
        challenge_id: String,
        lobby_code: String,
    },
    /// Server -> client: a challenge could not be issued, with a reason meant
    /// to be shown verbatim.
    ChallengeFailed {
        reason: String,
    },

    // === Rematch (protocol 12, capability `rematch-v1`) ===
    /// Client -> server: tick or untick Rematch on the results card. The
    /// server takes the player's identity from the connection and checks the
    /// game id against the one this socket actually joined.
    SetRematchIntent {
        game_id: u32,
        opt_in: bool,
    },
    /// Server -> client: who is still on the results card, who has opted in,
    /// and — once enough have — the lobby they all converge on.
    Rematch(RematchState),
    // NicknameUpdated {
    //     username: String,
    // },
}

/// Declares [`WSMessage::variant_name`] and the closed set of names it can
/// return, from one list.
///
/// One list rather than two: the outbound hook needs the reverse direction — a
/// wire tag mapped back onto the same `&'static str` — and a separately
/// maintained table of the same names would be free to drift from the match
/// without anything saying so. The match stays exhaustive, so a new variant
/// still does not compile until it is listed here.
macro_rules! ws_message_variant_names {
    ($($variant:ident),+ $(,)?) => {
        impl WSMessage {
            /// The variant's own name, for the analytics `message_type` column.
            ///
            /// Derived from the variant rather than supplied by the caller, on
            /// the same discipline as `analytics::event::payload_name`: a
            /// caller-supplied string is free to drift from the message it
            /// labels, and nothing would say so.
            ///
            /// The names are the variant identifiers, which is also what
            /// serde's default external tagging puts on the wire, so the two
            /// hooks agree; see
            /// `a_serialized_frame_reports_its_own_variant_name`.
            pub fn variant_name(&self) -> &'static str {
                match self {
                    $(Self::$variant { .. } => stringify!($variant),)+
                }
            }
        }

        /// Every name an application frame can report, and therefore the whole
        /// vocabulary of the `message_type` column for one.
        const WS_MESSAGE_TYPE_NAMES: &[&str] = &[$(stringify!($variant)),+];
    };
}

ws_message_variant_names![
    Token,
    Authenticate,
    JoinGame,
    LeaveGame,
    GameCommandV2,
    GameEvent,
    CommandOutcomes,
    CommandOutcomesComplete,
    Chat,
    LobbyChatMessage,
    GameChatMessage,
    LobbyChatHistory,
    GameChatHistory,
    Authenticated,
    AdConfiguration,
    PlayerReady,
    RequestResync,
    Ping,
    Pong,
    QueueForMatch,
    QueueForMatchMulti,
    LeaveQueue,
    MatchFound,
    QueueUpdate,
    QueueLeft,
    AdBreakResolved,
    UpdateNickname,
    SpectatorJoined,
    AccessDenied,
    GameLoadFailed,
    GameWarming,
    SoloGameCreated,
    Drain,
    UserCountUpdate,
    CreateLobby,
    LobbyCreated,
    JoinLobby,
    JoinedLobby,
    LeaveLobby,
    LeftLobby,
    LobbyUpdate,
    UpdateLobbyPreferences,
    LobbyRegionMismatch,
    OnlinePlayers,
    ChallengePlayer,
    RespondToChallenge,
    CancelChallenge,
    Challenges,
    ChallengeAccepted,
    ChallengeFailed,
    SetRematchIntent,
    Rematch,
];

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserToken {
    pub user_id: i32,
    pub username: String,
    pub is_guest: bool,
    pub matchmaking_pool: MatchmakingPool,
}

// Player metadata to store additional user information
#[derive(Debug, Clone)]
pub struct PlayerMetadata {
    pub user_id: i32,
    pub username: String,
    pub token: String,
    pub is_guest: bool,
    pub matchmaking_pool: MatchmakingPool,
    /// An ad break is only safe when every lobby member can acknowledge it.
    /// Keeping this explicit also makes mixed-version durable lobby state fail
    /// closed during a rolling deployment.
    pub supports_ad_break: bool,
    /// Deployment capability resolved from the authenticated distribution.
    /// Live runtime policy is applied again when matchmaking targets a break.
    pub can_show_video_ad: bool,
    pub distribution: Option<ClientDistribution>,
}

const MAX_CHAT_MESSAGE_LENGTH: usize = 200;
const CHAT_HISTORY_LIMIT: usize = 200;
const CHAT_CONTENT_FILTER_VERSION: u8 = 1;
const REPAIR_LEGACY_CHAT_HISTORY_SCRIPT: &str = r#"
local replacements = {}
for i = 1, #ARGV, 2 do
    replacements[ARGV[i]] = ARGV[i + 1]
end

local entries = redis.call('LRANGE', KEYS[1], 0, -1)
local changed = 0
for index, entry in ipairs(entries) do
    local replacement = replacements[entry]
    if replacement then
        redis.call('LSET', KEYS[1], index - 1, replacement)
        changed = changed + 1
    end
end
return changed
"#;
const LOBBY_STATE_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(1);
const LOBBY_MATCH_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(5);
const LOBBY_MATCH_SUBSCRIBE_RETRY_DELAY: Duration = Duration::from_secs(1);
const SLOW_COMMAND_PUBLISH_THRESHOLD: Duration = Duration::from_secs(1);

fn slow_command_publish_wait_ms(publish_wait: Duration) -> Option<u64> {
    if publish_wait <= SLOW_COMMAND_PUBLISH_THRESHOLD {
        return None;
    }
    Some(u64::try_from(publish_wait.as_millis()).unwrap_or(u64::MAX))
}

/// A command publication error has an ambiguous outcome: the write may have
/// reached the stream even though the gateway did not receive confirmation.
/// Fail the socket closed so its ordered outbox retries before a later command
/// can overtake the ambiguous one on this connection.
fn require_game_command_publication(publish_result: Result<bool>) -> Result<bool> {
    publish_result.context("v2 game command publication failed")
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct LobbyChatBroadcast {
    lobby_code: String,
    message_id: String,
    user_id: i32,
    username: String,
    message: String,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    timestamp_ms: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct GameChatBroadcast {
    game_id: u32,
    message_id: String,
    user_id: i32,
    username: String,
    message: String,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    timestamp_ms: i64,
}

/// Redis-only envelopes let rolling deployments distinguish messages that
/// were filtered at ingress from legacy payloads that still need filtering.
/// The flattened chat fields preserve compatibility with older servers.
#[derive(Debug, Serialize, Deserialize)]
struct RedisLobbyChatPayload {
    #[serde(flatten)]
    chat: LobbyChatBroadcast,
    #[serde(default)]
    content_filter_version: u8,
}

impl RedisLobbyChatPayload {
    fn from_filtered(chat: LobbyChatBroadcast) -> Self {
        Self {
            chat,
            content_filter_version: CHAT_CONTENT_FILTER_VERSION,
        }
    }

    fn filter_legacy(&mut self) -> bool {
        // Version zero is the only unfiltered legacy format. Any positive
        // version has already been sanitized and must not be run through a
        // non-idempotent detector again during future filter upgrades.
        if self.content_filter_version == 0 {
            self.chat.message = filter_chat_message(&self.chat.message);
            self.content_filter_version = CHAT_CONTENT_FILTER_VERSION;
            true
        } else {
            false
        }
    }

    fn into_filtered(mut self) -> LobbyChatBroadcast {
        self.filter_legacy();
        self.chat
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RedisGameChatPayload {
    #[serde(flatten)]
    chat: GameChatBroadcast,
    #[serde(default)]
    content_filter_version: u8,
}

impl RedisGameChatPayload {
    fn from_filtered(chat: GameChatBroadcast) -> Self {
        Self {
            chat,
            content_filter_version: CHAT_CONTENT_FILTER_VERSION,
        }
    }

    fn filter_legacy(&mut self) -> bool {
        if self.content_filter_version == 0 {
            self.chat.message = filter_chat_message(&self.chat.message);
            self.content_filter_version = CHAT_CONTENT_FILTER_VERSION;
            true
        } else {
            false
        }
    }

    fn into_filtered(mut self) -> GameChatBroadcast {
        self.filter_legacy();
        self.chat
    }
}

#[cfg(test)]
mod chat_payload_tests {
    use super::*;

    const RAW_MESSAGE: &str = "so many a^s hole sin this server";

    fn lobby_chat(message: String) -> LobbyChatBroadcast {
        LobbyChatBroadcast {
            lobby_code: "ABC123".to_owned(),
            message_id: "lobby-message".to_owned(),
            user_id: 7,
            username: "player".to_owned(),
            message,
            timestamp_ms: 1,
        }
    }

    fn game_chat(message: String) -> GameChatBroadcast {
        GameChatBroadcast {
            game_id: 42,
            message_id: "game-message".to_owned(),
            user_id: 7,
            username: "player".to_owned(),
            message,
            timestamp_ms: 1,
        }
    }

    #[test]
    fn current_lobby_and_game_payloads_are_not_filtered_twice() {
        let filtered = filter_chat_message(RAW_MESSAGE);

        let lobby_json = serde_json::to_string(&RedisLobbyChatPayload::from_filtered(lobby_chat(
            filtered.clone(),
        )))
        .unwrap();
        let lobby: RedisLobbyChatPayload = serde_json::from_str(&lobby_json).unwrap();
        assert_eq!(lobby.content_filter_version, CHAT_CONTENT_FILTER_VERSION);
        assert_eq!(lobby.into_filtered().message, filtered);

        let game_json = serde_json::to_string(&RedisGameChatPayload::from_filtered(game_chat(
            filtered.clone(),
        )))
        .unwrap();
        let game: RedisGameChatPayload = serde_json::from_str(&game_json).unwrap();
        assert_eq!(game.content_filter_version, CHAT_CONTENT_FILTER_VERSION);
        assert_eq!(game.into_filtered().message, filtered);

        let future = RedisLobbyChatPayload {
            chat: lobby_chat(filtered.clone()),
            content_filter_version: CHAT_CONTENT_FILTER_VERSION + 1,
        };
        assert_eq!(future.into_filtered().message, filtered);
    }

    #[test]
    fn legacy_lobby_and_game_payloads_are_filtered_on_read() {
        let expected = filter_chat_message(RAW_MESSAGE);

        let lobby_json = serde_json::to_string(&lobby_chat(RAW_MESSAGE.to_owned())).unwrap();
        let lobby: RedisLobbyChatPayload = serde_json::from_str(&lobby_json).unwrap();
        assert_eq!(lobby.content_filter_version, 0);
        assert_eq!(lobby.into_filtered().message, expected);

        let game_json = serde_json::to_string(&game_chat(RAW_MESSAGE.to_owned())).unwrap();
        let game: RedisGameChatPayload = serde_json::from_str(&game_json).unwrap();
        assert_eq!(game.content_filter_version, 0);
        assert_eq!(game.into_filtered().message, expected);
    }

    #[test]
    fn current_envelopes_remain_readable_by_legacy_servers() {
        let filtered = filter_chat_message(RAW_MESSAGE);

        let lobby_json = serde_json::to_string(&RedisLobbyChatPayload::from_filtered(lobby_chat(
            filtered.clone(),
        )))
        .unwrap();
        let legacy_lobby: LobbyChatBroadcast = serde_json::from_str(&lobby_json).unwrap();
        assert_eq!(legacy_lobby.message, filtered);

        let game_json = serde_json::to_string(&RedisGameChatPayload::from_filtered(game_chat(
            filtered.clone(),
        )))
        .unwrap();
        let legacy_game: GameChatBroadcast = serde_json::from_str(&game_json).unwrap();
        assert_eq!(legacy_game.message, filtered);
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum LobbyMatchHint {
    MatchFound {
        game_id: u32,
        #[serde(default)]
        partition_id: Option<u32>,
    },
}

fn refresh_connection_username(metadata: &mut PlayerMetadata, username: String) {
    metadata.username = username;
}

/// Issue a challenge from this connection, creating the challenger's lobby if
/// they do not already have one.
///
/// Returns the (possibly new) lobby handle and, when a lobby was created here,
/// its code — the caller sends `LobbyCreated` so the client's lobby state
/// tracks the one the challenge points at.
#[allow(clippy::too_many_arguments)]
async fn issue_challenge(
    challenger_id: u32,
    metadata: &PlayerMetadata,
    target_user_id: u32,
    lobby: Option<LobbyJoinHandle>,
    lobby_manager: &Arc<crate::lobby_manager::LobbyManager>,
    user_cache: UserCache,
    redis: &RedisConnection,
    region: &str,
    websocket_id: &str,
    ws_tx: &mpsc::Sender<Message>,
) -> Result<(Option<LobbyJoinHandle>, Option<String>)> {
    if target_user_id == challenger_id {
        send_challenge_failure(crate::challenges::ChallengeRejection::Self_.reason(), ws_tx)
            .await?;
        return Ok((lobby, None));
    }

    let presence = PresenceRegistry::new(redis.clone(), region.to_string());
    match presence.is_online(target_user_id).await {
        Ok(true) => {}
        Ok(false) => {
            send_challenge_failure(
                crate::challenges::ChallengeRejection::TargetOffline.reason(),
                ws_tx,
            )
            .await?;
            return Ok((lobby, None));
        }
        Err(error) => {
            warn!(target_user_id, %error, "failed to check challenge target presence");
            send_challenge_failure("Could not reach that player. Try again.", ws_tx).await?;
            return Ok((lobby, None));
        }
    }

    // Resolve the target's name at send time rather than trusting the roster
    // frame the client was looking at: nicknames change, and the record is
    // read back by both sides for the life of the challenge.
    let target_username = match user_cache.get(target_user_id).await {
        Ok(Some(user)) => user.username,
        Ok(None) => {
            send_challenge_failure(
                crate::challenges::ChallengeRejection::TargetOffline.reason(),
                ws_tx,
            )
            .await?;
            return Ok((lobby, None));
        }
        Err(error) => {
            warn!(target_user_id, %error, "failed to resolve a challenge target");
            send_challenge_failure("Could not reach that player. Try again.", ws_tx).await?;
            return Ok((lobby, None));
        }
    };

    let (lobby, created_code) = match lobby {
        Some(handle) => (Some(handle), None),
        None => {
            let lobby = match lobby_manager
                .create_lobby_for_pool(metadata.user_id, region, metadata.matchmaking_pool)
                .await
            {
                Ok(lobby) => lobby,
                Err(error) => {
                    warn!(challenger_id, %error, "failed to create a lobby for a challenge");
                    send_challenge_failure("Could not open a lobby to play in.", ws_tx).await?;
                    return Ok((None, None));
                }
            };
            match lobby_manager
                .join_lobby_for_pool(
                    Some(lobby.lobby_code()),
                    metadata.user_id,
                    metadata.username.clone(),
                    websocket_id.to_string(),
                    region.to_string(),
                    None,
                    metadata.matchmaking_pool,
                    metadata.distribution,
                    metadata.supports_ad_break,
                    metadata.can_show_video_ad,
                )
                .await
            {
                Ok(handle) => {
                    let code = handle.lobby_code.clone();
                    (Some(handle), Some(code))
                }
                Err(error) => {
                    warn!(challenger_id, %error, "failed to join a lobby created for a challenge");
                    send_challenge_failure("Could not open a lobby to play in.", ws_tx).await?;
                    return Ok((None, None));
                }
            }
        }
    };

    let Some(handle) = lobby else {
        return Ok((None, created_code));
    };

    let created_at_ms = now_ms();
    let challenge = Challenge {
        challenge_id: new_challenge_id(challenger_id, target_user_id, created_at_ms),
        from_user_id: challenger_id,
        from_username: metadata.username.clone(),
        to_user_id: target_user_id,
        to_username: target_username,
        lobby_code: handle.lobby_code.clone(),
        state: ChallengeState::Pending,
        created_at_ms,
        expires_at_ms: created_at_ms.saturating_add(CHALLENGE_TTL_MS),
    };

    let store = ChallengeStore::new(redis.clone());
    match store.issue(challenge).await {
        Ok(Ok(_)) => {}
        Ok(Err(rejection)) => {
            send_challenge_failure(rejection.reason(), ws_tx).await?;
        }
        Err(error) => {
            warn!(challenger_id, target_user_id, %error, "failed to issue a challenge");
            send_challenge_failure("Could not send that challenge. Try again.", ws_tx).await?;
        }
    }
    // The challenger's own outgoing list is refreshed from the durable record,
    // so a rejected challenge cannot leave a phantom entry on their screen.
    let _ = send_challenge_inbox(challenger_id, &store, ws_tx).await;

    Ok((Some(handle), created_code))
}

async fn handle_guest_nickname_update(
    db: &Arc<dyn Database>,
    lobby_manager: &Arc<crate::lobby_manager::LobbyManager>,
    user_cache: UserCache,
    lobby: &Option<LobbyJoinHandle>,
    metadata: &mut PlayerMetadata,
    ws_tx: &mpsc::Sender<Message>,
    nickname: String,
) -> Result<()> {
    let trimmed = nickname.trim().to_string();

    let validation_errors = validate_username(&trimmed);
    if !validation_errors.is_empty() {
        let response = WSMessage::AccessDenied {
            reason: format!("Invalid nickname: {}", validation_errors.join(", ")),
        };
        let json_msg = serde_json::to_string(&response)?;
        ws_tx.send(Message::Text(json_msg.into())).await?;
        return Ok(());
    }

    if !metadata.is_guest {
        let response = WSMessage::AccessDenied {
            reason: "Only guest users can change their nickname".to_string(),
        };
        let json_msg = serde_json::to_string(&response)?;
        ws_tx.send(Message::Text(json_msg.into())).await?;
        return Ok(());
    }

    db.update_guest_username(metadata.user_id, &trimmed).await?;

    // The database is authoritative, but this connection's metadata is what
    // subsequent chat and lobby actions use. Keep it in sync immediately once
    // the durable rename commits, even if a later cache invalidation or lobby
    // update notification fails.
    refresh_connection_username(metadata, trimmed.clone());

    user_cache
        .remove_from_redis(metadata.user_id as u32)
        .await?;

    if let Some(lobby) = lobby {
        lobby_manager
            .publish_lobby_update(&lobby.lobby_code)
            .await?;
    }

    Ok(())
}

// JWT verification trait for dependency injection
#[async_trait::async_trait]
pub trait JwtVerifier: Send + Sync {
    async fn verify(&self, token: &str) -> Result<UserToken>;
}

// Test implementation that accepts any token and creates users as needed
pub struct TestJwtVerifier {
    db: Arc<dyn Database>,
}

impl TestJwtVerifier {
    pub fn new(db: Arc<dyn Database>) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl JwtVerifier for TestJwtVerifier {
    async fn verify(&self, token: &str) -> Result<UserToken> {
        // In test mode, accept any token and create user if needed
        // Extract username from token or use default
        let username = if token.starts_with("test-token-") {
            format!(
                "test_user_{}",
                token.strip_prefix("test-token-").unwrap_or("default")
            )
        } else {
            "test_user_default".to_string()
        };

        // Try to find existing user first
        let existing_user = self.db.get_user_by_username(&username).await?;

        let user_id = match existing_user {
            Some(user) => user.id,
            None => {
                // Create new test user
                let new_user = self
                    .db
                    .create_user(&username, "test_password_hash", 1000)
                    .await?;
                info!("Created test user {} with ID {}", username, new_user.id);
                new_user.id
            }
        };

        Ok(UserToken {
            user_id,
            username: username.clone(),
            is_guest: false,
            matchmaking_pool: MatchmakingPool::Public,
        })
    }
}

// Connection state machine - simplified to 2 states. Keeping the authenticated
// fields inline preserves the existing single-owner lobby-handle lifecycle;
// boxing that handle would complicate detach/close paths for negligible gain
// at the gateway's connection counts.
#[allow(clippy::large_enum_variant)]
enum ConnectionState {
    // Initial state - waiting for authentication
    Unauthenticated,

    // Authenticated state with optional context (lobby, game)
    Authenticated {
        metadata: PlayerMetadata,
        lobby_handle: Option<LobbyJoinHandle>,
        game_id: Option<u32>, // Some when user is in a game
        websocket_id: String, // Unique ID for this websocket connection
    },
}

fn queue_planned_drain_notice(
    drain_tx: &mpsc::Sender<Message>,
    notice: &DrainNotice,
) -> Result<()> {
    let message = WSMessage::Drain {
        task_boot_id: notice.task_boot_id.clone(),
        deadline_unix_ms: notice.deadline_unix_ms,
    };
    drain_tx
        .try_send(Message::Text(serde_json::to_string(&message)?.into()))
        .context("WebSocket drain control channel unavailable")
}

/// Receive control traffic ahead of the bounded gameplay queue. The sink
/// remains owned by one task, so this changes only queueing priority: at most
/// the single frame already being written can precede a drain notice.
async fn next_outbound_message(
    drain_rx: &mut mpsc::Receiver<Message>,
    ws_rx: &mut mpsc::Receiver<Message>,
    drain_open: &mut bool,
    ws_open: &mut bool,
) -> Option<Message> {
    loop {
        if !*drain_open && !*ws_open {
            return None;
        }

        tokio::select! {
            biased;
            message = drain_rx.recv(), if *drain_open => {
                match message {
                    Some(message) => return Some(message),
                    None => *drain_open = false,
                }
            }
            message = ws_rx.recv(), if *ws_open => {
                match message {
                    Some(message) => return Some(message),
                    None => *ws_open = false,
                }
            }
        }
    }
}

/// Move the receiver created before `join_lobby` publishes its initial
/// snapshot into the WebSocket forwarder. `Receiver::resubscribe()` starts at
/// the current tail, so using only that new receiver would discard an initial
/// update that reached this task while the join response was being assembled.
fn take_lobby_update_receiver(
    receiver: &mut broadcast::Receiver<lobby_manager::Lobby>,
) -> broadcast::Receiver<lobby_manager::Lobby> {
    let replacement = receiver.resubscribe();
    std::mem::replace(receiver, replacement)
}

#[derive(PartialEq, Eq)]
struct LobbyUpdateFingerprint {
    lobby_code: String,
    members: Vec<(u32, String)>,
    host_user_id: i32,
    state: String,
    preferences: lobby_manager::LobbyPreferences,
    ad_break: Option<LobbyAdBreakView>,
}

/// Publish the durable lobby snapshot, rather than trusting the at-most-once
/// Pub/Sub payload as authoritative state. A missing lobby is represented by
/// the same terminal update used by the lobby manager's deletion forwarder.
async fn send_authoritative_lobby_update(
    lobby_manager: &Arc<lobby_manager::LobbyManager>,
    lobby_code: &str,
    ws_tx: &mpsc::Sender<Message>,
    last_sent_update: &mut Option<LobbyUpdateFingerprint>,
) -> Result<bool> {
    let lobby = lobby_manager
        .get_lobby_opt(lobby_code)
        .await?
        .unwrap_or_else(|| lobby_manager::Lobby {
            lobby_code: lobby_code.to_owned(),
            members: BTreeMap::new(),
            host_user_id: 0,
            state: "deleted".to_owned(),
            preferences: lobby_manager::LobbyPreferences::default(),
            ad_break: None,
        });
    let ad_break = lobby.ad_break.as_ref().map(LobbyAdBreak::view);
    let fingerprint = LobbyUpdateFingerprint {
        lobby_code: lobby.lobby_code.clone(),
        members: lobby
            .members
            .values()
            .map(|member| (member.user_id, member.username.clone()))
            .collect(),
        host_user_id: lobby.host_user_id,
        state: lobby.state.clone(),
        preferences: lobby.preferences.clone(),
        ad_break: ad_break.clone(),
    };
    if last_sent_update.as_ref() == Some(&fingerprint) {
        return Ok(true);
    }
    let ws_message = WSMessage::LobbyUpdate {
        lobby_code: lobby.lobby_code,
        members: lobby.members.into_values().collect(),
        host_user_id: lobby.host_user_id,
        state: lobby.state,
        preferences: lobby.preferences,
        ad_break,
    };
    let json_msg = serde_json::to_string(&ws_message)?;
    if ws_tx.send(Message::Text(json_msg.into())).await.is_err() {
        return Ok(false);
    }
    *last_sent_update = Some(fingerprint);
    Ok(true)
}

/// Handle WebSocket connection from Axum
#[allow(clippy::too_many_arguments)]
pub async fn handle_websocket(
    socket: axum::extract::ws::WebSocket,
    db: Arc<dyn Database>,
    user_cache: UserCache,
    jwt_verifier: Arc<dyn JwtVerifier>,
    redis: RedisConnection,
    redis_url: String,
    pubsub_manager: Arc<PubSubManager>,
    game_bus: Arc<GameBus>,
    matchmaking_manager: Arc<Mutex<MatchmakingManager>>,
    event_router: Arc<crate::replication::GameEventRouter>,
    cancellation_token: CancellationToken,
    lobby_manager: Arc<crate::lobby_manager::LobbyManager>,
    region: String,
    lifecycle: TaskLifecycle,
    cluster_namespace: ClusterNamespace,
    ads_config: Arc<AdsConfig>,
) {
    info!("New WebSocket connection established");

    // Process the WebSocket connection
    if let Err(e) = handle_websocket_connection(
        socket,
        db,
        user_cache.clone(),
        pubsub_manager,
        game_bus,
        matchmaking_manager,
        jwt_verifier,
        cancellation_token,
        event_router,
        redis,
        redis_url,
        lobby_manager,
        region,
        lifecycle,
        cluster_namespace,
        ads_config,
    )
    .await
    {
        crate::resilience_metrics::record_websocket_process_error(1);
        error!("WebSocket connection error: {}", e);
    }
}

fn tungstenite_message_bytes(message: &Message) -> usize {
    match message {
        Message::Text(text) => text.len(),
        Message::Binary(data) | Message::Ping(data) | Message::Pong(data) => data.len(),
        Message::Close(frame) => frame
            .as_ref()
            .map(|frame| frame.reason.len().saturating_add(2))
            .unwrap_or(0),
        _ => 0,
    }
}

fn axum_message_bytes(message: &axum::extract::ws::Message) -> usize {
    match message {
        axum::extract::ws::Message::Text(text) => text.len(),
        axum::extract::ws::Message::Binary(data)
        | axum::extract::ws::Message::Ping(data)
        | axum::extract::ws::Message::Pong(data) => data.len(),
        axum::extract::ws::Message::Close(frame) => frame
            .as_ref()
            .map(|frame| frame.reason.len().saturating_add(2))
            .unwrap_or(0),
    }
}

/// Longest variant name the wire-tag reader will accept.
///
/// Every name in [`WSMessage::variant_name`] is far shorter; the bound exists
/// so a malformed frame can never turn an unbounded string into an unbounded
/// analytics column value.
const MAX_WIRE_TAG_LEN: usize = 64;

/// The label recorded when a frame carries no readable variant name.
const UNNAMED_MESSAGE_TYPE: &str = "unknown";

/// The variant name of an outbound frame, read back from the frame itself.
///
/// The forwarder is handed serialized frames rather than `WSMessage` values —
/// it is a separate task draining a `Sender<Message>` — so the name has to
/// come from the wire. Application frames therefore report a PascalCase
/// variant name and transport frames report a snake_case label, which keeps
/// the two kinds distinguishable in the `message_type` column.
///
/// The returned name is `&'static str`, never a slice of the frame: the tag is
/// looked up in `WS_MESSAGE_TYPE_NAMES` and the matching static is returned, so
/// describing a frame costs no allocation and the column's vocabulary is closed
/// rather than whatever the frame happened to contain.
fn outbound_message_type(message: &Message) -> &'static str {
    match message {
        Message::Text(text) => wire_tag(text)
            .and_then(interned_message_type)
            .unwrap_or(UNNAMED_MESSAGE_TYPE),
        Message::Binary(_) => "binary_frame",
        Message::Ping(_) => "ping_frame",
        Message::Pong(_) => "pong_frame",
        Message::Close(_) => "close_frame",
        _ => UNNAMED_MESSAGE_TYPE,
    }
}

/// The `WSMessage` name equal to `tag`, or `None` if there is none.
///
/// Sorted once and binary-searched rather than scanned, because this runs on
/// every outbound frame; the table is built from `WS_MESSAGE_TYPE_NAMES` rather
/// than kept sorted by hand so adding a variant cannot silently break the
/// search's precondition.
fn interned_message_type(tag: &str) -> Option<&'static str> {
    static SORTED: OnceLock<Vec<&'static str>> = OnceLock::new();
    let sorted = SORTED.get_or_init(|| {
        let mut names = WS_MESSAGE_TYPE_NAMES.to_vec();
        names.sort_unstable();
        names
    });
    sorted
        .binary_search_by(|candidate| (**candidate).cmp(tag))
        .ok()
        .map(|index| sorted[index])
}

/// The externally-tagged variant name at the head of a serialized `WSMessage`.
///
/// `WSMessage` carries no serde container attribute, so it uses the default
/// external tagging: a unit variant is `"Name"` and every other variant is
/// `{"Name":…}`. Either way the first JSON string in the frame is the variant
/// name.
///
/// Scanning that one token is O(name) where a full parse would be O(frame),
/// and the frames this runs on include whole game snapshots — the send path
/// must not pay to describe itself.
fn wire_tag(text: &str) -> Option<&str> {
    let after_brace = text.strip_prefix('{').unwrap_or(text);
    let opened = after_brace.trim_start().strip_prefix('"')?;
    let tag = &opened[..opened.find('"')?];
    // Variant names need no JSON escaping, so an escaped or oversized tag did
    // not come from `WSMessage` and is reported as unnamed rather than
    // recorded verbatim.
    (!tag.is_empty()
        && tag.len() <= MAX_WIRE_TAG_LEN
        && tag
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'))
    .then_some(tag)
}

async fn abort_and_join_game_event_forwarder(handle: &mut Option<JoinHandle<()>>) {
    if let Some(handle) = handle.take() {
        handle.abort();
        // CommandOutcomesComplete is intentionally compact and has no wire
        // generation. Joining guarantees the old forwarder cannot enqueue its
        // barrier after a replacement forwarder enqueues a newer snapshot.
        let _ = handle.await;
    }
}

/// The seat a connection is in after a state transition, published to its
/// analytics context as it is read.
///
/// One function rather than a read at the top of the transition and a publish
/// at the bottom, because the publish has to happen BEFORE anything acts on the
/// transition. Entering a game spawns the game-event forwarder, and that
/// forwarder's first frame is the anchor snapshot; it runs on its own task and
/// can learn the seat only from here. A seat published afterwards would stamp
/// the whole entry burst — the snapshot included — with the game this
/// connection was in before, which on a rematch or a game switch is a wrong
/// join key rather than a missing one.
///
/// Returning the value is what keeps that order: the transition below cannot
/// decide whether it is entering a game without calling this first.
fn publish_seat(
    state: &ConnectionState,
    analytics: &crate::analytics::ws_sink::WsConnection,
) -> Option<u32> {
    let seat = match state {
        ConnectionState::Authenticated { game_id, .. } => *game_id,
        ConnectionState::Unauthenticated => None,
    };
    analytics.set_game_id(seat);
    seat
}

/// Internal function to handle the WebSocket connection logic
#[allow(clippy::too_many_arguments)]
async fn handle_websocket_connection(
    ws_stream: axum::extract::ws::WebSocket,
    db: Arc<dyn Database>,
    user_cache: UserCache,
    pubsub_manager: Arc<PubSubManager>,
    game_bus: Arc<GameBus>,
    matchmaking_manager: Arc<Mutex<MatchmakingManager>>,
    jwt_verifier: Arc<dyn JwtVerifier>,
    cancellation_token: CancellationToken,
    event_router: Arc<crate::replication::GameEventRouter>,
    redis: RedisConnection,
    redis_url: String,
    lobby_manager: Arc<crate::lobby_manager::LobbyManager>,
    region: String,
    lifecycle: TaskLifecycle,
    cluster_namespace: ClusterNamespace,
    ads_config: Arc<AdsConfig>,
) -> Result<()> {
    // Split the WebSocket into send and receive parts using futures_util
    let (mut ws_sink, mut ws_stream) = futures_util::StreamExt::split(ws_stream);

    // Gameplay and ordinary protocol traffic retain the existing bounded
    // backpressure queue. Drain has a one-slot priority path so a saturated
    // gameplay queue cannot consume the handoff window before the client sees
    // the notice.
    let (ws_tx, mut ws_rx) = mpsc::channel::<Message>(1024);
    let (drain_tx, mut priority_drain_rx) = mpsc::channel::<Message>(1);

    // Generate a unique websocket ID for this connection
    let websocket_id = uuid::Uuid::new_v4().to_string();
    let socket_generation = lifecycle.next_socket_generation();
    let mut drain_rx = lifecycle.subscribe_to_drain();

    // One analytics context for the connection, keyed on an id that already
    // exists before the first frame. The forwarder task below sees only
    // serialized frames, so the identity both directions record has to be
    // shared rather than derived independently on each side.
    let ws_analytics = Arc::new(crate::analytics::ws_sink::WsConnection::new(&websocket_id));

    // Start in unauthenticated state
    let mut state = ConnectionState::Unauthenticated;

    // Create a shutdown timeout that starts as a never-completing future
    let shutdown_timeout = tokio::time::sleep(Duration::from_secs(u64::MAX));
    tokio::pin!(shutdown_timeout);
    let mut shutdown_started = false;

    // Will be used to track Redis stream subscription for game events
    let mut game_event_handle: Option<JoinHandle<()>> = None;
    // Rate limit for client-initiated resyncs (RequestResync).
    let mut last_resync_at: Option<tokio::time::Instant> = None;

    // Will be used to track lobby update forwarding to the websocket
    let mut lobby_update_handle: Option<JoinHandle<()>> = None;

    // Will be used to track Redis pub/sub subscription for lobby match notifications
    let mut lobby_match_handle: Option<JoinHandle<()>> = None;

    // Will be used to track lobby chat subscription
    let mut lobby_chat_handle: Option<JoinHandle<()>> = None;

    // Will be used to track game chat subscription
    let mut game_chat_handle: Option<JoinHandle<()>> = None;

    // Presence lease + challenge delivery, claimed once this connection has an
    // authenticated identity to be present *as*.
    let mut social_session: Option<SocialSession> = None;

    // Spawn task to forward messages from channel to WebSocket
    let ws_analytics_for_forwarder = ws_analytics.clone();
    let forward_task = tokio::spawn(async move {
        let mut drain_open = true;
        let mut ws_open = true;
        while let Some(msg) = next_outbound_message(
            &mut priority_drain_rx,
            &mut ws_rx,
            &mut drain_open,
            &mut ws_open,
        )
        .await
        {
            let is_close = matches!(msg, Message::Close(_));
            let outbound_bytes = tungstenite_message_bytes(&msg);
            // Every one of this gateway's outbound sends funnels through this
            // drain, so recording here covers them all. Recorded before the
            // write rather than after it — as the throughput metric below is —
            // because the conversion consumes the frame the type name is read
            // from; the two differ only for the single frame in flight when a
            // socket dies.
            //
            // The gate is here rather than inside `record_outbound` because
            // reading the type off the wire is an ARGUMENT: inside the call it
            // would already have happened. A connection outside the sample, or
            // a deployment with no sink installed, pays one bool per frame.
            if ws_analytics_for_forwarder.records() {
                crate::analytics::ws_sink::record_outbound(
                    &ws_analytics_for_forwarder,
                    outbound_message_type(&msg),
                    outbound_bytes,
                );
            }
            // Convert to Axum WebSocket message
            let axum_msg = match msg {
                Message::Text(text) => axum::extract::ws::Message::Text(text.to_string()),
                Message::Binary(bin) => axum::extract::ws::Message::Binary(bin.to_vec()),
                Message::Ping(data) => axum::extract::ws::Message::Ping(data.to_vec()),
                Message::Pong(data) => axum::extract::ws::Message::Pong(data.to_vec()),
                Message::Close(frame) => {
                    let close = frame.map(|f| axum::extract::ws::CloseFrame {
                        code: f.code.into(),
                        reason: f.reason.to_string().into(),
                    });
                    axum::extract::ws::Message::Close(close)
                }
                _ => continue,
            };

            if let Err(e) = ws_sink.send(axum_msg).await {
                crate::resilience_metrics::record_websocket_send_error(1);
                error!("Failed to send message to WebSocket: {}", e);
                break;
            }
            crate::resilience_metrics::record_websocket_outbound_message(outbound_bytes);
            if is_close {
                break;
            }
        }
    });

    // Spawn task to subscribe to user count updates and forward to client
    let ws_tx_for_counts = ws_tx.clone();
    let pubsub_manager_for_counts = pubsub_manager.clone();
    let _user_count_task = tokio::spawn(async move {
        if let Err(e) =
            subscribe_to_user_count_updates(pubsub_manager_for_counts, ws_tx_for_counts).await
        {
            error!("User count subscription task failed: {}", e);
        }
    });

    // A WebSocket can pass the readiness check immediately before the task
    // flips to draining and finish upgrading after the broadcast. Replay the
    // process-local notice so that narrow race cannot leave a late socket on
    // the departing task until forced termination.
    if let Some(notice) = lifecycle.current_drain_notice() {
        let remaining_ms = notice
            .deadline_unix_ms
            .saturating_sub(Utc::now().timestamp_millis())
            .max(1) as u64;
        shutdown_timeout
            .as_mut()
            .reset(tokio::time::Instant::now() + Duration::from_millis(remaining_ms));
        shutdown_started = true;
        queue_planned_drain_notice(&drain_tx, &notice)?;
    }

    loop {
        // let state_name = match &state {
        //     ConnectionState::Unauthenticated => "Unauthenticated".to_string(),
        //     ConnectionState::Authenticated { lobby_code: Some(code), game_id: Some(gid), .. } => {
        //         format!("Authenticated(lobby:{}, game:{})", code, gid)
        //     }
        //     ConnectionState::Authenticated { lobby_code: Some(code), game_id: None, .. } => {
        //         format!("Authenticated(lobby:{})", code)
        //     }
        //     ConnectionState::Authenticated { lobby_code: None, game_id: Some(gid), .. } => {
        //         format!("Authenticated(game:{})", gid)
        //     }
        //     ConnectionState::Authenticated { .. } => "Authenticated".to_string(),
        // };
        // debug!("WS: Select loop iteration, current state: {}", state_name);

        tokio::select! {
            // Handle shutdown timeout
            _ = &mut shutdown_timeout, if shutdown_started => {
                warn!("Shutdown timeout reached, closing connection");
                break;
            }
            // A planned drain is independent from final process cancellation:
            // the old socket remains usable until the replacement is ready.
            notice = drain_rx.recv(), if !shutdown_started => {
                let notice = match notice {
                    Ok(notice) => notice,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                };
                info!("Sending planned drain message to client");
                let remaining_ms = notice.deadline_unix_ms
                    .saturating_sub(Utc::now().timestamp_millis())
                    .max(1) as u64;
                shutdown_timeout.as_mut().reset(
                    tokio::time::Instant::now() + Duration::from_millis(remaining_ms),
                );
                shutdown_started = true;
                if let Err(e) = queue_planned_drain_notice(&drain_tx, &notice) {
                    error!("Failed to queue planned drain message: {}", e);
                }
            }
            // Final cancellation is a fallback for crashes during a planned
            // drain setup. Normal SIGTERM announces through drain_rx first.
            _ = cancellation_token.cancelled(), if !shutdown_started => {
                break;
            }
            // Handle incoming WebSocket messages
            Some(result) = ws_stream.next() => {
                match result {
                    Ok(msg) => {
                        crate::resilience_metrics::record_websocket_inbound_message(
                            axum_message_bytes(&msg),
                        );
                        // Convert Axum message to tokio-tungstenite message for processing
                        let tungstenite_msg = match msg {
                            axum::extract::ws::Message::Text(text) => Message::Text(text.into()),
                            axum::extract::ws::Message::Binary(bin) => Message::Binary(bin.into()),
                            axum::extract::ws::Message::Ping(data) => Message::Ping(data.into()),
                            axum::extract::ws::Message::Pong(data) => Message::Pong(data.into()),
                            axum::extract::ws::Message::Close(_frame) => {
                                info!("Client initiated close");
                                break;
                            }
                        };

                        // Process the message
                        if let Message::Text(text) = tungstenite_msg {
                            let parsed = serde_json::from_str::<WSMessage>(&text);
                            // Above the match rather than inside it, so the
                            // arms that never reach `process_ws_message` — the
                            // resync fast path and the in-lobby denial below —
                            // are recorded too. A frame that failed to parse is
                            // deliberately not recorded: its type name would be
                            // attacker-supplied rather than a known variant.
                            // The gate is the call site's, not `record_inbound`'s,
                            // for the same reason as on the outbound side: the
                            // hook's arguments are evaluated before the call.
                            if ws_analytics.records()
                                && let Ok(message) = &parsed
                            {
                                crate::analytics::ws_sink::record_inbound(
                                    &ws_analytics,
                                    message.variant_name(),
                                    text.len(),
                                );
                            }
                            match parsed {
                                Ok(WSMessage::RequestResync { game_id: resync_game_id }) => {
                                    crate::resilience_metrics::record_websocket_resync_requested(1);
                                    // The client detected loss or divergence (stream
                                    // gap, repeated fingerprint mismatch, or a dead
                                    // feed). Restart its event forwarder — which
                                    // sends a fresh watermarked snapshot as its
                                    // first message — instead of trusting whatever
                                    // subscription state it had. Rate-limited so a
                                    // stuck client cannot spam resubscriptions.
                                    let in_this_game = matches!(
                                        &state,
                                        ConnectionState::Authenticated { game_id: Some(g), .. } if *g == resync_game_id
                                    );
                                    let now = tokio::time::Instant::now();
                                    let allowed = last_resync_at
                                        .map(|t| now.duration_since(t) >= Duration::from_millis(500))
                                        .unwrap_or(true);
                                    if in_this_game && allowed {
                                        crate::resilience_metrics::record_websocket_resync_accepted(1);
                                        last_resync_at = Some(now);
                                        if let ConnectionState::Authenticated { metadata, .. } = &state {
                                            let user_id = metadata.user_id as u32;
                                            info!(
                                                "Resync requested by user {} for game {}; restarting event subscription",
                                                metadata.user_id, resync_game_id
                                            );
                                            abort_and_join_game_event_forwarder(
                                                &mut game_event_handle,
                                            )
                                            .await;
                                            let ws_tx_clone = ws_tx.clone();
                                            let event_router_clone = event_router.clone();
                                            let db_clone = db.clone();
                                            let game_bus_clone = game_bus.clone();
                                            let cluster_namespace_clone = cluster_namespace.clone();
                                            game_event_handle = Some(tokio::spawn(async move {
                                                subscribe_to_game_events(
                                                    resync_game_id,
                                                    user_id,
                                                    ws_tx_clone,
                                                    event_router_clone,
                                                    db_clone,
                                                    game_bus_clone,
                                                    cluster_namespace_clone,
                                                ).await;
                                            }));
                                        }
                                    } else {
                                        crate::resilience_metrics::record_websocket_resync_rejected(1);
                                        if !in_this_game {
                                            debug!(
                                                "Ignoring resync request for game {} from connection not in that game",
                                                resync_game_id
                                            );
                                        }
                                    }
                                }
                                Ok(ws_message) => {
                                    // Check state before consuming it
                                    let was_in_game = matches!(&state, ConnectionState::Authenticated { game_id: Some(_), .. });
                                    let was_in_lobby = matches!(&state, ConnectionState::Authenticated { lobby_handle: Some(_), .. });
                                    if was_in_lobby
                                        && matches!(
                                            &ws_message,
                                            WSMessage::CreateLobby | WSMessage::JoinLobby { .. }
                                        )
                                    {
                                        let denial = WSMessage::AccessDenied {
                                            reason: "Leave your current lobby before creating or joining another lobby".to_owned(),
                                        };
                                        if ws_tx
                                            .send(Message::Text(
                                                serde_json::to_string(&denial)?.into(),
                                            ))
                                            .await
                                            .is_err()
                                        {
                                            break;
                                        }
                                        continue;
                                    }
                                    // Keep the requested id so a denied switch does not look like a
                                    // successful re-entry into the connection's previously authorized
                                    // game. Successful JoinGame retries still restart subscriptions.
                                    let requested_game_id = match &ws_message {
                                        WSMessage::JoinGame(game_id) => Some(*game_id),
                                        _ => None,
                                    };

                                    match process_ws_message(
                                        state,
                                        ws_message,
                                        &jwt_verifier,
                                        &db,
                                        user_cache.clone(),
                                        &ws_tx,
                                        &game_bus,
                                        &matchmaking_manager,
                                        &event_router,
                                        &redis,
                                        &redis_url,
                                        &lobby_manager,
                                        &websocket_id,
                                        &region,
                                        &lifecycle,
                                        socket_generation,
                                        &cluster_namespace,
                                        &cancellation_token,
                                        &ads_config,
                                        &ws_analytics,
                                    ).await {
                                        Ok(mut new_state) => {
                                            // Check if we're entering a game or lobby.
                                            // Reading the seat and publishing it
                                            // are one step, and everything below
                                            // that acts on the transition needs
                                            // this value — so nothing can spawn a
                                            // subscription or send a frame before
                                            // the forwarder has been told which
                                            // game it is now in.
                                            let entered_game_id =
                                                publish_seat(&new_state, &ws_analytics);
                                            let entering_game = match requested_game_id {
                                                Some(requested_game_id) => {
                                                    entered_game_id == Some(requested_game_id)
                                                }
                                                None => entered_game_id.is_some() && !was_in_game,
                                            };
                                            let entering_lobby = matches!(&new_state, ConnectionState::Authenticated { lobby_handle: Some(_), .. }) && !was_in_lobby;

                                            // Join the social layer the moment there is an identity to be
                                            // present as, and keep the roster's activity honest afterwards.
                                            // A failed claim (a Redis blip at exactly the wrong moment)
                                            // retries on the next inbound message rather than leaving this
                                            // connection socially invisible for its whole life.
                                            if let ConnectionState::Authenticated { metadata, .. } = &new_state {
                                                if social_session.is_none() && social_layer_admits(metadata) {
                                                    social_session = start_social_session(
                                                        metadata,
                                                        &websocket_id,
                                                        &region,
                                                        &redis,
                                                        &pubsub_manager,
                                                        &event_router,
                                                        &db,
                                                        &ws_tx,
                                                    )
                                                    .await;
                                                } else if social_session.is_some() {
                                                    let activity = match &new_state {
                                                        ConnectionState::Authenticated { game_id: Some(_), .. } => {
                                                            PresenceActivity::Playing
                                                        }
                                                        ConnectionState::Authenticated { lobby_handle: Some(_), .. } => {
                                                            PresenceActivity::Lobby
                                                        }
                                                        _ => PresenceActivity::Idle,
                                                    };
                                                    let seated_game_id = match &new_state {
                                                        ConnectionState::Authenticated { game_id, .. } => *game_id,
                                                        ConnectionState::Unauthenticated => None,
                                                    };
                                                    record_presence_activity(
                                                        social_session.as_ref(),
                                                        metadata,
                                                        activity,
                                                        seated_game_id,
                                                    )
                                                    .await;
                                                }
                                            }
                                            let leaving_lobby = was_in_lobby && !matches!(&new_state, ConnectionState::Authenticated { lobby_handle: Some(_), .. });
                                            let leaving_game = was_in_game && !matches!(&new_state, ConnectionState::Authenticated { game_id: Some(_), .. });
                                            debug!("State transitioned to: entering_game: {}, entering_lobby: {}, leaving_lobby: {}, leaving_game: {}",
                                                entering_game, entering_lobby, leaving_lobby, leaving_game);

                                            // Handle state transitions
                                            if entering_game
                                                && let ConnectionState::Authenticated { game_id: Some(game_id), metadata, .. } = &new_state {
                                                    let game_id = *game_id;
                                                    let user_id = metadata.user_id as u32;
                                                    // Subscribe to game events if entering a game
                                                    abort_and_join_game_event_forwarder(
                                                        &mut game_event_handle,
                                                    )
                                                    .await;
                                                    if let Some(handle) = game_chat_handle.take() {
                                                        handle.abort();
                                                    }

                                                    let ws_tx_clone = ws_tx.clone();
                                                    let event_router_clone = event_router.clone();
                                                    let db_clone = db.clone();
                                                    let game_bus_clone = game_bus.clone();
                                                    let cluster_namespace_clone = cluster_namespace.clone();

                                                    game_event_handle = Some(tokio::spawn(async move {
                                                        subscribe_to_game_events(
                                                            game_id,
                                                            user_id,
                                                            ws_tx_clone,
                                                            event_router_clone,
                                                            db_clone,
                                                            game_bus_clone,
                                                            cluster_namespace_clone,
                                                        ).await;
                                                    }));

                                                    let ws_tx_clone = ws_tx.clone();
                                                    let pubsub_manager_clone = pubsub_manager.clone();

                                                    game_chat_handle = Some(tokio::spawn(async move {
                                                        if let Err(e) = subscribe_to_game_chat(
                                                            game_id,
                                                            pubsub_manager_clone,
                                                            ws_tx_clone,
                                                        )
                                                        .await
                                                        {
                                                            error!("Game chat subscription failed: {}", e);
                                                        }
                                                    }));

                                                    match load_game_chat_history(redis.clone(), game_id).await {
                                                        Ok(history) if !history.is_empty() => {
                                                            let history_message = WSMessage::GameChatHistory {
                                                                game_id,
                                                                messages: history,
                                                            };
                                                            match serde_json::to_string(&history_message) {
                                                                Ok(json) => {
                                                                    if let Err(e) = ws_tx
                                                                        .send(Message::Text(json.into()))
                                                                        .await
                                                                    {
                                                                        debug!(
                                                                            "Failed to send initial game chat history for game {}: {}",
                                                                            game_id, e
                                                                        );
                                                                    }
                                                                }
                                                                Err(e) => {
                                                                    error!(
                                                                        "Failed to serialize game chat history for game {}: {}",
                                                                        game_id, e
                                                                    );
                                                                }
                                                            }
                                                        }
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            warn!(
                                                                "Failed to load game chat history for game {}: {}",
                                                                game_id, e
                                                            );
                                                        }
                                                    }
                                                }

                                            // Handle lobby state transitions
                                            if entering_lobby
                                                && let ConnectionState::Authenticated { lobby_handle: Some(lobby_handle), metadata, .. } = &mut new_state {
                                                if let Some(handle) = lobby_update_handle.take() {
                                                    handle.abort();
                                                }
                                                if let Some(handle) = lobby_chat_handle.take() {
                                                    handle.abort();
                                                }

                                                    let mut lobby_rx = take_lobby_update_receiver(&mut lobby_handle.rx);
                                                    let lobby_code_for_updates = lobby_handle.lobby_code.clone();
                                                    let lobby_code_for_match = lobby_handle.lobby_code.clone();
                                                    let lobby_user_id_for_match = metadata.user_id as u32;
                                                    let lobby_scope_cancellation = lobby_handle.scope_cancellation_token();
                                                    let ws_tx_clone = ws_tx.clone();
                                                    let cancellation_token_clone = cancellation_token.clone();
                                                    let lobby_scope_for_updates = lobby_scope_cancellation.clone();
                                                    let lobby_manager_clone = lobby_manager.clone();
                                                    let db_for_ad_break_recovery = db.clone();
                                                    let matchmaking_for_ad_break_recovery = matchmaking_manager.clone();

                                                    lobby_update_handle = Some(tokio::spawn(async move {
                                                        let mut last_sent_update = None;
                                                        let mut reconciliation = tokio::time::interval(
                                                            LOBBY_STATE_RECONCILIATION_INTERVAL,
                                                        );
                                                        reconciliation.set_missed_tick_behavior(
                                                            tokio::time::MissedTickBehavior::Skip,
                                                        );
                                                        loop {
                                                            tokio::select! {
                                                                _ = cancellation_token_clone.cancelled() => {
                                                                    debug!("Lobby update task cancelled for lobby {}", lobby_code_for_updates);
                                                                    break;
                                                                }
                                                                _ = lobby_scope_for_updates.cancelled() => {
                                                                    debug!("Superseded lobby scope stopped reconciliation for {}", lobby_code_for_updates);
                                                                    break;
                                                                }
                                                                update = lobby_rx.recv() => {
                                                                    match update {
                                                                        Ok(_) => {
                                                                            debug!("Received lobby update hint for lobby {}", lobby_code_for_updates);
                                                                            match send_authoritative_lobby_update(
                                                                                &lobby_manager_clone,
                                                                                &lobby_code_for_updates,
                                                                                &ws_tx_clone,
                                                                                &mut last_sent_update,
                                                                            ).await {
                                                                                Ok(true) => {}
                                                                                Ok(false) => {
                                                                                    debug!("WebSocket channel closed while sending lobby update for {}", lobby_code_for_updates);
                                                                                    break;
                                                                                }
                                                                                Err(error) => {
                                                                                    warn!("Failed to reconcile lobby {} after update hint: {}", lobby_code_for_updates, error);
                                                                                }
                                                                            }
                                                                        }
                                                                        Err(broadcast::error::RecvError::Closed) => {
                                                                            debug!("Lobby update channel closed for lobby {}", lobby_code_for_updates);
                                                                            break;
                                                                        }
                                                                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                                                            warn!("Missed {} lobby updates for lobby {}", skipped, lobby_code_for_updates);
                                                                        }
                                                                    }
                                                                }
                                                                _ = reconciliation.tick() => {
                                                                    if let Err(error) = expire_lobby_ad_break_if_due(
                                                                        &lobby_code_for_updates,
                                                                        &db_for_ad_break_recovery,
                                                                        &lobby_manager_clone,
                                                                        &matchmaking_for_ad_break_recovery,
                                                                    ).await {
                                                                        warn!(
                                                                            lobby_code = lobby_code_for_updates,
                                                                            %error,
                                                                            "Failed to recover an expired lobby ad break"
                                                                        );
                                                                    }
                                                                    match send_authoritative_lobby_update(
                                                                        &lobby_manager_clone,
                                                                        &lobby_code_for_updates,
                                                                        &ws_tx_clone,
                                                                        &mut last_sent_update,
                                                                    ).await {
                                                                        Ok(true) => {}
                                                                        Ok(false) => {
                                                                            debug!("WebSocket channel closed while reconciling lobby {}", lobby_code_for_updates);
                                                                            break;
                                                                        }
                                                                        Err(error) => {
                                                                            debug!("Periodic lobby reconciliation failed for {}: {}", lobby_code_for_updates, error);
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }));

                                                    // Subscribe to lobby match notifications
                                                    if let Some(handle) = lobby_match_handle.take() {
                                                        handle.abort();
                                                    }

                                                    let ws_tx_clone_for_match = ws_tx.clone();
                                                    let pubsub_manager_clone_for_match = pubsub_manager.clone();
                                                    let redis_clone_for_match = redis.clone();
                                                    let cancellation_token_clone_for_match = cancellation_token.clone();
                                                    let lobby_scope_for_match = lobby_scope_cancellation.clone();

                                                    lobby_match_handle = Some(tokio::spawn(async move {
                                                        tokio::select! {
                                                            _ = lobby_scope_for_match.cancelled() => {}
                                                            _ = subscribe_to_lobby_match_notifications(
                                                                lobby_code_for_match,
                                                                lobby_user_id_for_match,
                                                                pubsub_manager_clone_for_match,
                                                                redis_clone_for_match,
                                                                ws_tx_clone_for_match,
                                                                cancellation_token_clone_for_match,
                                                                LOBBY_MATCH_RECONCILIATION_INTERVAL,
                                                            ) => {}
                                                        }
                                                    }));

                                                    // Subscribe to lobby chat
                                                    let ws_tx_clone = ws_tx.clone();
                                                    let pubsub_manager_clone = pubsub_manager.clone();

                                                    let lobby_code_for_chat = lobby_handle.lobby_code.clone();
                                                    let lobby_scope_for_chat = lobby_scope_cancellation.clone();
                                                    lobby_chat_handle = Some(tokio::spawn(async move {
                                                        tokio::select! {
                                                            _ = lobby_scope_for_chat.cancelled() => {}
                                                            result = subscribe_to_lobby_chat(
                                                                lobby_code_for_chat,
                                                                pubsub_manager_clone,
                                                                ws_tx_clone,
                                                            ) => {
                                                                if let Err(e) = result {
                                                                    error!("Lobby chat subscription failed: {}", e);
                                                                }
                                                            }
                                                        }
                                                    }));

                                                    let lobby_code_for_history = lobby_handle.lobby_code.clone();
                                                    match load_lobby_chat_history(redis.clone(), &lobby_code_for_history).await {
                                                        Ok(history) if !history.is_empty() => {
                                                            let history_message = WSMessage::LobbyChatHistory {
                                                                lobby_code: lobby_code_for_history.clone(),
                                                                messages: history,
                                                            };
                                                            match serde_json::to_string(&history_message) {
                                                                Ok(json) => {
                                                                    if let Err(e) = ws_tx
                                                                        .send(Message::Text(json.into()))
                                                                        .await
                                                                    {
                                                                        debug!(
                                                                            "Failed to send initial lobby chat history for lobby '{}': {}",
                                                                            lobby_code_for_history, e
                                                                        );
                                                                    }
                                                                }
                                                                Err(e) => {
                                                                    error!(
                                                                        "Failed to serialize lobby chat history for lobby '{}': {}",
                                                                        lobby_code_for_history, e
                                                                    );
                                                                }
                                                            }
                                                        }
                                                        Ok(_) => {}
                                                        Err(e) => {
                                                            warn!(
                                                                "Failed to load lobby chat history for lobby '{}': {}",
                                                                lobby_code_for_history, e
                                                            );
                                                        }
                                                    }
                                            }

                                            // Abort lobby subscription when leaving lobby
                                            // BUT keep lobby_match_handle active if entering Authenticated with a lobby_code (for Play Again notifications)
                                                if leaving_lobby {
                                                    let keep_match_subscription = matches!(&new_state, ConnectionState::Authenticated { lobby_handle: Some(_), .. });

                                                    if let Some(handle) = lobby_update_handle.take() {
                                                        handle.abort();
                                                        debug!("Aborted lobby update subscription");
                                                    }

                                                    // Only abort match notification if NOT entering game with lobby_id
                                                    if !keep_match_subscription
                                                    && let Some(handle) = lobby_match_handle.take() {
                                                        handle.abort();
                                                        debug!("Aborted lobby match notification subscription");
                                                    }

                                                if let Some(handle) = lobby_chat_handle.take() {
                                                    handle.abort();
                                                    debug!("Aborted lobby chat subscription");
                                                }
                                            }

                                            if leaving_game {
                                                if let Some(handle) = game_event_handle.take() {
                                                    handle.abort();
                                                    debug!("Aborted game event subscription");
                                                }
                                                if let Some(handle) = game_chat_handle.take() {
                                                    handle.abort();
                                                    debug!("Aborted game chat subscription");
                                                }
                                            }

                                            state = new_state;
                                        }
                                        Err(e) => {
                                            crate::resilience_metrics::record_websocket_process_error(1);
                                            error!("Error processing message: {}", e);
                                            // State was consumed, need to reset.
                                            // There is no new state to read the
                                            // seat from, so it is cleared directly.
                                            state = ConnectionState::Unauthenticated;
                                            ws_analytics.set_game_id(None);
                                            break;
                                        }
                                    }
                                }
                                Err(e) => {
                                    crate::resilience_metrics::record_websocket_malformed_message(1);
                                    error!("Failed to parse WebSocket message: {}", e);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        crate::resilience_metrics::record_websocket_transport_error(1);
                        error!("WebSocket error: {}", e);
                        break;
                    }
                }
            }
        }
    }

    // Cleanup

    // Transport loss is not an explicit LeaveLobby. Stop heartbeating and let
    // the short presence lease expire; a replacement connection may already
    // have installed a newer websocket-specific presence.
    if let ConnectionState::Authenticated {
        lobby_handle: Some(lobby_handle),
        ..
    } = state
    {
        lobby_handle.detach_transport();
    }

    // Note: Game subscriptions are now handled differently
    // No need to manually close game_handle as it's not part of ConnectionState anymore

    // Abort subscription tasks
    if let Some(handle) = game_event_handle {
        handle.abort();
    }
    if let Some(handle) = lobby_update_handle {
        handle.abort();
    }
    if let Some(handle) = lobby_match_handle {
        handle.abort();
    }
    if let Some(handle) = lobby_chat_handle {
        handle.abort();
    }
    if let Some(handle) = game_chat_handle {
        handle.abort();
    }
    // Give up the presence lease and withdraw anything still pending, so this
    // player leaves the roster immediately instead of at lease expiry and
    // nobody is left holding an unanswerable invitation.
    if let Some(session) = social_session.take() {
        session.close().await;
    }
    forward_task.abort();

    info!("WebSocket connection closed");
    Ok(())
}

async fn publish_lobby_chat_message(
    mut redis: RedisConnection,
    payload: LobbyChatBroadcast,
) -> Result<()> {
    let payload = RedisLobbyChatPayload::from_filtered(payload);
    let channel = RedisKeys::lobby_chat_channel(&payload.chat.lobby_code);
    let history_key = RedisKeys::lobby_chat_history_key(&payload.chat.lobby_code);
    let serialized =
        serde_json::to_string(&payload).context("Failed to serialize lobby chat payload")?;

    redis
        .publish::<_, _, ()>(&channel, serialized.clone())
        .await
        .context("Failed to publish lobby chat message")?;

    let _: i64 = redis
        .rpush(&history_key, serialized.clone())
        .await
        .context("Failed to append lobby chat history")?;
    let start: isize = -(CHAT_HISTORY_LIMIT as isize);
    let _: () = redis
        .ltrim(&history_key, start, -1)
        .await
        .context("Failed to trim lobby chat history")?;
    Ok(())
}

async fn publish_game_chat_message(
    mut redis: RedisConnection,
    payload: GameChatBroadcast,
) -> Result<()> {
    let payload = RedisGameChatPayload::from_filtered(payload);
    let channel = RedisKeys::game_chat_channel(payload.chat.game_id);
    let history_key = RedisKeys::game_chat_history_key(payload.chat.game_id);
    let serialized =
        serde_json::to_string(&payload).context("Failed to serialize game chat payload")?;

    redis
        .publish::<_, _, ()>(&channel, serialized.clone())
        .await
        .context("Failed to publish game chat message")?;

    let _: i64 = redis
        .rpush(&history_key, serialized.clone())
        .await
        .context("Failed to append game chat history")?;
    let start: isize = -(CHAT_HISTORY_LIMIT as isize);
    let _: () = redis
        .ltrim(&history_key, start, -1)
        .await
        .context("Failed to trim game chat history")?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn queue_existing_lobby_for_game_types(
    lobby_code: &str,
    game_types: &[common::GameType],
    queue_mode: &common::QueueMode,
    db: &Arc<dyn Database>,
    lobby_manager: &Arc<crate::lobby_manager::LobbyManager>,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
    requesting_user_id: u32,
    matchmaking_pool: MatchmakingPool,
    expected_ad_break_id: Option<&str>,
) -> Result<()> {
    validate_matchmaking_game_types(game_types)?;

    let lobby_metadata = lobby_manager
        .get_lobby_metadata(lobby_code)
        .await
        .context("Failed to load lobby metadata before queueing")?
        .ok_or_else(|| anyhow!("Lobby no longer exists"))?;
    if lobby_metadata.matchmaking_pool != matchmaking_pool {
        return Err(anyhow!("Lobby belongs to a different matchmaking pool"));
    }

    let members_map = lobby_manager
        .get_lobby_members(lobby_code)
        .await
        .context("Failed to load lobby members before queueing")?;

    if members_map.is_empty() {
        return Err(anyhow!("Lobby has no active members to queue"));
    }
    if members_map.len() > MAX_LOBBY_MEMBERS {
        return Err(anyhow!("Lobby exceeds the supported member limit"));
    }
    if !members_map.contains_key(&requesting_user_id) {
        return Err(anyhow!(
            "The requesting user is not an active member of this lobby"
        ));
    }

    let expected_user_ids: Vec<u32> = members_map.keys().copied().collect();
    let initial_members: Vec<LobbyMember> = members_map.into_values().collect();
    let avg_mmr = compute_lobby_avg_mmr(db, &initial_members, matchmaking_pool).await?;

    // MMR reads may consume most of a short presence lease. Re-snapshot while
    // holding the admission mutex, and retry only typed membership conflicts;
    // this keeps the roster fence fresh without making users watch another ad.
    let mut admitted = false;
    for attempt in 0..3_u64 {
        let mut mm_guard = matchmaking_manager.lock().await;
        let membership_revision = lobby_manager
            .get_lobby_membership_revision(lobby_code)
            .await
            .context("Failed to refresh lobby membership revision")?;
        let fresh_members_map = lobby_manager
            .get_lobby_members(lobby_code)
            .await
            .context("Failed to refresh lobby members before admission")?;
        let fresh_user_ids: Vec<u32> = fresh_members_map.keys().copied().collect();
        if fresh_user_ids != expected_user_ids
            || !fresh_members_map.contains_key(&requesting_user_id)
        {
            return Err(anyhow!(
                "Lobby membership changed while matchmaking was being prepared"
            ));
        }
        let membership_fence = LobbyMembershipFence {
            revision: membership_revision,
            valid_until_ms: lobby_membership_valid_until_ms(fresh_members_map.values())?,
        };
        let fresh_members: Vec<LobbyMember> = fresh_members_map.into_values().collect();
        let admission = mm_guard
            .add_lobby_to_queue_in_pool_with_membership_fence(
                lobby_code,
                fresh_members,
                avg_mmr,
                game_types.to_vec(),
                queue_mode.clone(),
                requesting_user_id,
                matchmaking_pool,
                expected_ad_break_id,
                membership_fence,
            )
            .await;
        drop(mm_guard);

        match admission {
            Ok(()) => {
                admitted = true;
                break;
            }
            Err(error)
                if attempt < 2
                    && error
                        .downcast_ref::<LobbyAdmissionRejected>()
                        .is_some_and(LobbyAdmissionRejected::is_retryable_membership_conflict) =>
            {
                tokio::time::sleep(Duration::from_millis(10 * (attempt + 1))).await;
            }
            Err(error) => {
                return Err(error).context("Failed to add lobby to matchmaking queue");
            }
        }
    }
    if !admitted {
        return Err(anyhow!(
            "Lobby membership remained unstable during matchmaking admission"
        ));
    }

    if let Err(error) = lobby_manager.publish_lobby_update(lobby_code).await {
        warn!(
            lobby_code,
            %error,
            "Failed to publish queued lobby state"
        );
    }

    Ok(())
}

fn validate_matchmaking_game_types(game_types: &[common::GameType]) -> Result<()> {
    if game_types.is_empty() {
        return Err(anyhow!("Must specify at least one game type to queue"));
    }
    if game_types.len() > MATCHMAKING_GAME_TYPES.len() {
        return Err(anyhow!(
            "At most {} game types may be queued",
            MATCHMAKING_GAME_TYPES.len()
        ));
    }
    for (index, game_type) in game_types.iter().enumerate() {
        if !MATCHMAKING_GAME_TYPES.contains(game_type) {
            return Err(anyhow!("Unsupported matchmaking game type"));
        }
        if game_types[..index].contains(game_type) {
            return Err(anyhow!("Duplicate matchmaking game type"));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn finalize_lobby_ad_break(
    ad_break: &LobbyAdBreak,
    lobby_code: &str,
    db: &Arc<dyn Database>,
    lobby_manager: &Arc<crate::lobby_manager::LobbyManager>,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
) -> Result<()> {
    let Some(finalization_lease) = lobby_manager
        .claim_ad_break_finalization(lobby_code, &ad_break.id)
        .await?
    else {
        return Ok(());
    };

    let current_members = lobby_manager
        .get_lobby_members(lobby_code)
        .await
        .context("Failed to reload lobby members after ad break")?;
    let current_user_ids: Vec<u32> = current_members.keys().copied().collect();
    if current_user_ids != ad_break.participant_user_ids {
        lobby_manager
            .cancel_ad_break(lobby_code, &ad_break.id)
            .await?;
        return Err(anyhow!(
            "Lobby membership changed during the ad break; start matchmaking again"
        ));
    }

    let admission = queue_existing_lobby_for_game_types(
        lobby_code,
        &ad_break.game_types,
        &ad_break.queue_mode,
        db,
        lobby_manager,
        matchmaking_manager,
        ad_break.requesting_user_id,
        ad_break.matchmaking_pool,
        Some(&ad_break.id),
    )
    .await;

    match admission {
        Ok(()) => {
            lobby_manager
                .clear_ad_break(lobby_code, &ad_break.id)
                .await?;
            lobby_manager.publish_lobby_update(lobby_code).await?;
            finalization_lease.release().await;
            Ok(())
        }
        Err(error) => {
            if error
                .downcast_ref::<LobbyAdmissionRejected>()
                .is_some_and(LobbyAdmissionRejected::is_retryable_membership_conflict)
            {
                // Keep the already-resolved break durable. Releasing the
                // claim lets the one-second reconciler retry with a fresh
                // roster/lease without showing another ad.
                finalization_lease.release().await;
                return Err(error);
            }
            // If admission committed but its response was lost, the queue Lua
            // script already changed state to queued and this compare/cancel
            // is harmless. Otherwise return the lobby to waiting.
            let _ = lobby_manager
                .cancel_ad_break(lobby_code, &ad_break.id)
                .await;
            Err(error)
        }
    }
}

async fn expire_lobby_ad_break_if_due(
    lobby_code: &str,
    db: &Arc<dyn Database>,
    lobby_manager: &Arc<crate::lobby_manager::LobbyManager>,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
) -> Result<bool> {
    let Some(metadata) = lobby_manager.get_lobby_metadata(lobby_code).await? else {
        return Ok(false);
    };
    let Some(active) = metadata.ad_break else {
        return Ok(false);
    };
    // A crash can occur after the final durable resolution but before queue
    // admission. Recover that state immediately; only unresolved breaks wait
    // for the safety deadline authored by the metadata-slot Redis clock.
    if active.is_resolved() {
        finalize_lobby_ad_break(&active, lobby_code, db, lobby_manager, matchmaking_manager)
            .await?;
        return Ok(true);
    }

    match lobby_manager
        .resolve_ad_break(lobby_code, &active.id, 0, AdBreakResolution::TimedOut)
        .await?
    {
        AdBreakResolutionResult::Ready(resolved) => {
            finalize_lobby_ad_break(
                &resolved,
                lobby_code,
                db,
                lobby_manager,
                matchmaking_manager,
            )
            .await?;
            Ok(true)
        }
        AdBreakResolutionResult::Pending(_)
        | AdBreakResolutionResult::NotDue(_)
        | AdBreakResolutionResult::NoChange(_)
        | AdBreakResolutionResult::Stale => Ok(false),
    }
}

#[allow(clippy::too_many_arguments)]
async fn queue_lobby_or_begin_ad_break(
    lobby_code: &str,
    game_types: &[common::GameType],
    queue_mode: &common::QueueMode,
    db: &Arc<dyn Database>,
    lobby_manager: &Arc<crate::lobby_manager::LobbyManager>,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
    requesting_user_id: u32,
    matchmaking_pool: MatchmakingPool,
    ads_config: &Arc<AdsConfig>,
) -> Result<bool> {
    // Validate the client-controlled fan-out before eligibility work or an ad
    // break. Users must never watch an ad for queue families no worker drains.
    validate_matchmaking_game_types(game_types)?;

    if !ads_config.any_pre_match_video_enabled() || matchmaking_pool != MatchmakingPool::Public {
        queue_existing_lobby_for_game_types(
            lobby_code,
            game_types,
            queue_mode,
            db,
            lobby_manager,
            matchmaking_manager,
            requesting_user_id,
            matchmaking_pool,
            None,
        )
        .await?;
        return Ok(false);
    }

    // Runtime policy is strongly consistent and fail-closed for advertising.
    // A control-plane outage must never delay gameplay or reuse stale ad
    // authorization, so it falls through directly to normal matchmaking.
    let runtime_record = match db.get_runtime_config().await {
        Ok(record) if record.config.ads.enabled => record,
        Ok(_) => {
            queue_existing_lobby_for_game_types(
                lobby_code,
                game_types,
                queue_mode,
                db,
                lobby_manager,
                matchmaking_manager,
                requesting_user_id,
                matchmaking_pool,
                None,
            )
            .await?;
            return Ok(false);
        }
        Err(error) => {
            warn!(lobby_code, %error, "Runtime ad policy unavailable; skipping ad break");
            queue_existing_lobby_for_game_types(
                lobby_code,
                game_types,
                queue_mode,
                db,
                lobby_manager,
                matchmaking_manager,
                requesting_user_id,
                matchmaking_pool,
                None,
            )
            .await?;
            return Ok(false);
        }
    };
    let runtime_ads = &runtime_record.config.ads;

    for snapshot_attempt in 0..3_u64 {
        let membership_revision = lobby_manager
            .get_lobby_membership_revision(lobby_code)
            .await
            .context("Failed to load lobby membership fence for ad eligibility")?;
        let members = lobby_manager
            .get_lobby_members(lobby_code)
            .await
            .context("Failed to load lobby members for ad eligibility")?;
        if members.is_empty() {
            return Err(anyhow!("Lobby has no active members to queue"));
        }
        if !members.contains_key(&requesting_user_id) {
            return Err(anyhow!(
                "The requesting user is not an active member of this lobby"
            ));
        }
        if members.len() > MAX_AD_BREAK_PARTICIPANTS {
            queue_existing_lobby_for_game_types(
                lobby_code,
                game_types,
                queue_mode,
                db,
                lobby_manager,
                matchmaking_manager,
                requesting_user_id,
                matchmaking_pool,
                None,
            )
            .await?;
            return Ok(false);
        }
        let membership_valid_until_ms = lobby_membership_valid_until_ms(members.values())?;

        let Some(ad_user_ids) = lobby_video_ad_targets(members.values(), runtime_ads) else {
            queue_existing_lobby_for_game_types(
                lobby_code,
                game_types,
                queue_mode,
                db,
                lobby_manager,
                matchmaking_manager,
                requesting_user_id,
                matchmaking_pool,
                None,
            )
            .await?;
            return Ok(false);
        };

        let mut users = Vec::with_capacity(members.len());
        for member in members.values() {
            match db.get_user_by_id(member.user_id as i32).await {
                Ok(Some(user)) => users.push(user),
                Ok(None) => {
                    // Missing history is treated as a newcomer. Skipping the ad
                    // is conservative and never blocks matchmaking.
                    users.clear();
                    break;
                }
                Err(error) => {
                    warn!(
                        lobby_code,
                        user_id = member.user_id,
                        %error,
                        "Could not load ad eligibility; skipping the lobby ad break"
                    );
                    users.clear();
                    break;
                }
            }
        }

        let eligible = users.len() == members.len()
            && lobby_meets_game_threshold(
                users.iter().map(|user| &user.games_played),
                runtime_ads.minimum_games_played,
            );
        if !eligible {
            queue_existing_lobby_for_game_types(
                lobby_code,
                game_types,
                queue_mode,
                db,
                lobby_manager,
                matchmaking_manager,
                requesting_user_id,
                matchmaking_pool,
                None,
            )
            .await?;
            return Ok(false);
        }

        let now_ms = Utc::now().timestamp_millis();
        let break_id = uuid::Uuid::new_v4().to_string();
        let minimum_interval_ms =
            i64::from(runtime_ads.minimum_interval_minutes).saturating_mul(60_000);
        match db
            .try_claim_pre_match_ad_break(
                &break_id,
                &ad_user_ids,
                now_ms,
                minimum_interval_ms,
                runtime_record.version,
            )
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                queue_existing_lobby_for_game_types(
                    lobby_code,
                    game_types,
                    queue_mode,
                    db,
                    lobby_manager,
                    matchmaking_manager,
                    requesting_user_id,
                    matchmaking_pool,
                    None,
                )
                .await?;
                return Ok(false);
            }
            Err(error) => {
                warn!(lobby_code, %error, "Could not claim ad cooldown; skipping ad break");
                queue_existing_lobby_for_game_types(
                    lobby_code,
                    game_types,
                    queue_mode,
                    db,
                    lobby_manager,
                    matchmaking_manager,
                    requesting_user_id,
                    matchmaking_pool,
                    None,
                )
                .await?;
                return Ok(false);
            }
        }
        let timeout_ms = i64::try_from(ads_config.ad_break_timeout.as_millis()).unwrap_or(i64::MAX);
        let requested = LobbyAdBreak {
            id: break_id,
            expires_at_ms: now_ms.saturating_add(timeout_ms),
            participant_user_ids: members.keys().copied().collect(),
            ad_user_ids,
            resolutions: BTreeMap::new(),
            game_types: game_types.to_vec(),
            queue_mode: queue_mode.clone(),
            requesting_user_id,
            matchmaking_pool,
        };
        match lobby_manager
            .begin_ad_break(
                lobby_code,
                &requested,
                membership_revision,
                membership_valid_until_ms,
                ads_config.ad_break_timeout,
            )
            .await?
        {
            BeginAdBreakResult::Active { .. } => {
                // Every connected lobby session runs the authoritative
                // one-second reconciliation loop. Avoid retaining one
                // detached timer task per rapidly-cycled break.
                return Ok(true);
            }
            BeginAdBreakResult::MembershipChanged if snapshot_attempt < 2 => {
                tokio::time::sleep(Duration::from_millis(10 * (snapshot_attempt + 1))).await;
            }
            BeginAdBreakResult::MembershipChanged => {
                return Err(anyhow!(
                    "Lobby membership kept changing while matchmaking was preparing; retry shortly"
                ));
            }
        }
    }
    unreachable!("bounded lobby snapshot loop always returns")
}

/// All members must understand the barrier, while only one needs an enabled
/// video provider. This permits mixed web/CrazyGames/itch lobbies: no-ad
/// sessions resolve `unavailable`, and ad-enabled sessions use their own
/// distribution-specific adapter.
fn lobby_video_ad_targets<'a>(
    members: impl IntoIterator<Item = &'a LobbyMember>,
    runtime_ads: &RuntimeAdsConfig,
) -> Option<Vec<u32>> {
    let mut targets = Vec::new();
    for member in members {
        if !member.supports_ad_break {
            return None;
        }
        let runtime_distribution_enabled = match member.distribution {
            Some(ClientDistribution::Web) => runtime_ads.distributions.web.enabled,
            Some(ClientDistribution::CrazyGames) => runtime_ads.distributions.crazygames.enabled,
            Some(ClientDistribution::Itch) => runtime_ads.distributions.itch.enabled,
            None => false,
        };
        if member.can_show_video_ad && runtime_distribution_enabled {
            targets.push(member.user_id);
        }
    }
    (!targets.is_empty()).then_some(targets)
}

async fn compute_lobby_avg_mmr(
    db: &Arc<dyn Database>,
    members: &[LobbyMember],
    matchmaking_pool: MatchmakingPool,
) -> Result<i32> {
    let mut total = 0;
    let mut count = 0;

    for member in members {
        match db.get_user_by_id(member.user_id as i32).await? {
            Some(user) => {
                let user_pool = if user.is_stress_test {
                    MatchmakingPool::Stress
                } else {
                    MatchmakingPool::Public
                };
                if user_pool != matchmaking_pool {
                    return Err(anyhow!(
                        "Lobby contains a member from a different matchmaking pool"
                    ));
                }
                total += user.mmr;
                count += 1;
            }
            None => {
                warn!(
                    user_id = member.user_id,
                    "Skipping lobby member without DB record while calculating MMR"
                );
            }
        }
    }

    if count == 0 {
        Err(anyhow!(
            "Unable to calculate lobby MMR - no valid members found"
        ))
    } else {
        Ok(total / count)
    }
}

async fn load_lobby_chat_history(
    mut redis: RedisConnection,
    lobby_code: &str,
) -> Result<Vec<LobbyChatBroadcast>> {
    let key = RedisKeys::lobby_chat_history_key(lobby_code);
    let entries: Vec<String> = redis
        .lrange(&key, 0, -1)
        .await
        .context("Failed to load lobby chat history")?;

    let mut messages = Vec::with_capacity(entries.len());
    let mut repairs = Vec::new();
    for entry in entries {
        match serde_json::from_str::<RedisLobbyChatPayload>(&entry) {
            Ok(mut payload) => {
                if payload.filter_legacy() {
                    match serde_json::to_string(&payload) {
                        Ok(replacement) => repairs.push((entry, replacement)),
                        Err(error) => warn!(
                            "Failed to serialize repaired lobby chat history entry for lobby '{}': {}",
                            lobby_code, error
                        ),
                    }
                }
                messages.push(payload.chat);
            }
            Err(e) => {
                warn!(
                    "Failed to deserialize lobby chat history entry for lobby '{}': {}",
                    lobby_code, e
                );
            }
        }
    }

    if let Err(error) = repair_legacy_chat_history(&mut redis, &key, &repairs).await {
        warn!(
            "Failed to repair {} legacy lobby chat history entries for lobby '{}': {}",
            repairs.len(),
            lobby_code,
            error
        );
    }

    Ok(messages)
}

async fn load_game_chat_history(
    mut redis: RedisConnection,
    game_id: u32,
) -> Result<Vec<GameChatBroadcast>> {
    let key = RedisKeys::game_chat_history_key(game_id);
    let entries: Vec<String> = redis
        .lrange(&key, 0, -1)
        .await
        .context("Failed to load game chat history")?;

    let mut messages = Vec::with_capacity(entries.len());
    let mut repairs = Vec::new();
    for entry in entries {
        match serde_json::from_str::<RedisGameChatPayload>(&entry) {
            Ok(mut payload) => {
                if payload.filter_legacy() {
                    match serde_json::to_string(&payload) {
                        Ok(replacement) => repairs.push((entry, replacement)),
                        Err(error) => warn!(
                            "Failed to serialize repaired game chat history entry for game {}: {}",
                            game_id, error
                        ),
                    }
                }
                messages.push(payload.chat);
            }
            Err(e) => {
                warn!(
                    "Failed to deserialize game chat history entry for game {}: {}",
                    game_id, e
                );
            }
        }
    }

    if let Err(error) = repair_legacy_chat_history(&mut redis, &key, &repairs).await {
        warn!(
            "Failed to repair {} legacy game chat history entries for game {}: {}",
            repairs.len(),
            game_id,
            error
        );
    }

    Ok(messages)
}

async fn repair_legacy_chat_history(
    redis: &mut RedisConnection,
    key: &str,
    repairs: &[(String, String)],
) -> Result<()> {
    if repairs.is_empty() {
        return Ok(());
    }

    let script = redis::Script::new(REPAIR_LEGACY_CHAT_HISTORY_SCRIPT);
    let mut invocation = script.prepare_invoke();
    invocation.key(key);
    for (legacy, replacement) in repairs {
        invocation.arg(legacy).arg(replacement);
    }
    invocation
        .invoke_async::<i64>(redis)
        .await
        .context("Failed to atomically repair legacy chat history")?;
    Ok(())
}

fn game_state_records_user(game_state: &GameState, user_id: u32) -> bool {
    game_state.players.contains_key(&user_id) || game_state.spectators.contains(&user_id)
}

const COLD_JOIN_WARMUP_TIMEOUT: Duration = Duration::from_secs(4);
const GAME_WARMING_RETRY_MS: u64 = 500;
const GAME_JOIN_AUTHORIZATION_TIMEOUT: Duration = Duration::from_secs(6);
const ACTIVE_GAME_MAPPING_TIMEOUT: Duration = Duration::from_secs(1);
const COMMAND_OUTCOME_LOAD_TIMEOUT: Duration = Duration::from_secs(4);
const COMMAND_OUTCOME_READ_TIMEOUT: Duration = Duration::from_millis(750);
const COMMAND_OUTCOME_RETRY_DELAY: Duration = Duration::from_millis(100);
const TERMINAL_COMMAND_REJECTION_REASON: &str = "game completed";

type CommandOutcomeReplay =
    Pin<Box<dyn Future<Output = Option<ResolvedCommandState>> + Send + 'static>>;

// This short-lived select result is stack-local. Boxing the event variant would
// add a heap allocation to every live game event only to reduce stack padding.
#[allow(clippy::large_enum_variant)]
enum GameSubscriptionInput {
    SocketClosed,
    CommandOutcomes(Option<ResolvedCommandState>),
    Update(Option<crate::replication::SubscriptionUpdate>),
}

fn start_command_outcome_replay(
    game_bus: Arc<GameBus>,
    cluster_namespace: ClusterNamespace,
    game_id: u32,
    user_id: u32,
) -> CommandOutcomeReplay {
    Box::pin(
        async move { load_command_outcomes(&game_bus, &cluster_namespace, game_id, user_id).await },
    )
}

async fn wait_for_command_outcome_replay(
    replay: &mut Option<CommandOutcomeReplay>,
) -> Option<ResolvedCommandState> {
    match replay {
        Some(replay) => replay.as_mut().await,
        None => pending().await,
    }
}

async fn next_game_subscription_input(
    ws_tx: &mpsc::Sender<Message>,
    subscription: &mut crate::replication::GameEventSubscription,
    replay: &mut Option<CommandOutcomeReplay>,
) -> GameSubscriptionInput {
    tokio::select! {
        _ = ws_tx.closed() => GameSubscriptionInput::SocketClosed,
        outcomes = wait_for_command_outcome_replay(replay) => {
            GameSubscriptionInput::CommandOutcomes(outcomes)
        }
        update = subscription.next() => GameSubscriptionInput::Update(update),
    }
}

#[derive(Debug)]
enum GameJoinAuthorizationError {
    /// A dependency or authoritative live-game artifact may still converge.
    /// This maps only to `GameWarming`, never `GameLoadFailed`.
    Warming,
    /// The available authoritative evidence proves this join cannot recover.
    /// This is the only branch that maps to `GameLoadFailed`.
    Denied(String),
}

impl std::fmt::Display for GameJoinAuthorizationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Warming => formatter.write_str("game stream is warming"),
            Self::Denied(reason) => formatter.write_str(reason),
        }
    }
}

fn game_join_denied(reason: impl Into<String>) -> GameJoinAuthorizationError {
    GameJoinAuthorizationError::Denied(reason.into())
}

fn validate_game_matchmaking_pool(
    expected_pool: MatchmakingPool,
    active_match: Option<&ActiveMatch>,
) -> std::result::Result<(), GameJoinAuthorizationError> {
    match active_match {
        Some(active_match) if active_match.matchmaking_pool == expected_pool => Ok(()),
        Some(_) => Err(game_join_denied("This game is unavailable")),
        // Legacy and custom games predate pool attestation and are public-only.
        None if expected_pool == MatchmakingPool::Public => Ok(()),
        // Stress identities fail closed when a direct game ID cannot be tied
        // to a currently attested stress match.
        None => Err(game_join_denied("This game is unavailable")),
    }
}

fn game_join_failure_message(game_id: u32, failure: GameJoinAuthorizationError) -> WSMessage {
    match failure {
        GameJoinAuthorizationError::Warming => WSMessage::GameWarming {
            game_id,
            retry_after_ms: GAME_WARMING_RETRY_MS,
        },
        GameJoinAuthorizationError::Denied(reason) => WSMessage::GameLoadFailed { game_id, reason },
    }
}

fn missing_game_join_failure(
    requested_game_id: u32,
    mapped_game_id: Option<u32>,
) -> GameJoinAuthorizationError {
    if mapped_game_id == Some(requested_game_id) {
        GameJoinAuthorizationError::Warming
    } else {
        game_join_denied("This game was not found or has expired")
    }
}

async fn load_durable_active_game(
    user_id: u32,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
) -> Result<Option<u32>> {
    tokio::time::timeout(ACTIVE_GAME_MAPPING_TIMEOUT, async {
        let mut manager = {
            let manager = matchmaking_manager.lock().await;
            manager.clone()
        };
        manager.get_user_active_game(user_id).await
    })
    .await
    .context("timed out resolving durable active-game mapping")?
    .context("failed to resolve durable active-game mapping")
}

async fn load_active_match_for_pool_authorization(
    game_id: u32,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
) -> Result<Option<ActiveMatch>> {
    tokio::time::timeout(ACTIVE_GAME_MAPPING_TIMEOUT, async {
        let mut manager = {
            let manager = matchmaking_manager.lock().await;
            manager.clone()
        };
        manager.get_active_match(game_id).await
    })
    .await
    .context("timed out resolving active match pool")?
    .context("failed to resolve active match pool")
}

async fn has_durable_recovery_failure(
    game_id: u32,
    game_bus: &Arc<GameBus>,
    cluster_namespace: &ClusterNamespace,
) -> bool {
    match game_bus
        .get_recovery_failure(cluster_namespace, game_id)
        .await
    {
        Ok(Some(_)) => true,
        Ok(None) => false,
        Err(error) => {
            warn!(game_id, %error, "Failed to inspect durable recovery-failure marker");
            false
        }
    }
}

/// A targeted snapshot request can be missed while no executor owns the
/// partition, and a just-committed match has no recovery envelope until its
/// `GameCreated` is consumed and checkpointed. Keep requesting the game and
/// polling the fenced envelope through the bounded takeover window; the
/// envelope is the only authoritative state a gateway can read directly.
async fn wait_for_authoritative_state_after_snapshot_request(
    game_id: u32,
    event_router: &Arc<crate::replication::GameEventRouter>,
    game_bus: &Arc<GameBus>,
    cluster_namespace: &ClusterNamespace,
) -> Option<GameState> {
    let partition_id = game_id % PARTITION_COUNT;
    let deadline = tokio::time::Instant::now() + COLD_JOIN_WARMUP_TIMEOUT;

    loop {
        // The router coalesces to one publish per game per 500 ms across
        // every caller on this gateway.
        if let Err(error) = event_router.request_game_snapshot(game_id).await {
            warn!(game_id, partition_id, %error, "Failed to request cold-join snapshot");
        }

        match game_bus.get_recovery(cluster_namespace, game_id).await {
            Ok(Some(envelope)) => return Some(envelope.game_state),
            Ok(None) => {}
            Err(error) => {
                warn!(game_id, %error, "Failed to load recovery during cold-join warm-up");
            }
        }

        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn canonical_command_identity(
    command_id: ClientCommandIdentityV2,
    game_id: u32,
    user_id: u32,
) -> ClientCommandIdentityV2 {
    ClientCommandIdentityV2 {
        game_id,
        user_id,
        client_game_session_id: command_id.client_game_session_id,
        sequence: command_id.sequence,
    }
}

fn snapshot_requires_command_outcomes(event: &GameEvent) -> bool {
    matches!(event, GameEvent::Snapshot { .. })
}

fn command_outcomes_for_user(
    resolved: ResolvedCommandState,
    user_id: u32,
) -> Vec<(String, SessionCommandOutcomes)> {
    let prefix = format!("{user_id}:");
    resolved
        .sessions
        .into_iter()
        .filter_map(|(session_key, outcomes)| {
            let client_game_session_id = session_key.strip_prefix(&prefix)?;
            (!client_game_session_id.is_empty())
                .then(|| (client_game_session_id.to_owned(), outcomes))
        })
        .collect()
}

/// Returns whether the durable `ActiveMatch` roster records this user as a
/// participant (player or lobby-split spectator). The roster is written
/// atomically at match commit, so it authorizes matchmade joins without any
/// game-state read. Legacy and custom games have no roster and fall through
/// to the recovery envelope.
fn active_match_records_user(active_match: Option<&ActiveMatch>, user_id: u32) -> bool {
    active_match.is_some_and(|active_match| {
        active_match
            .players
            .iter()
            .chain(active_match.spectators.iter())
            .any(|player| player.user_id == user_id)
    })
}

/// Resolve and authorize a JoinGame request before it changes connection state.
///
/// Gateways hold no game state, so authorization comes from durable sources
/// only: the `ActiveMatch` roster for matchmade games, the fenced recovery
/// envelope for anything live, and the short Redis reload cache or DynamoDB
/// for completed games. Returning success means the requested user was present
/// in one of those canonical sources; callers may then enable game events and
/// chat.
#[allow(clippy::too_many_arguments)]
async fn authorize_game_join_inner(
    game_id: u32,
    user_id: u32,
    matchmaking_pool: MatchmakingPool,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
    event_router: &Arc<crate::replication::GameEventRouter>,
    game_bus: &Arc<GameBus>,
    cluster_namespace: &ClusterNamespace,
    db: &Arc<dyn Database>,
) -> std::result::Result<(), GameJoinAuthorizationError> {
    let active_match = load_active_match_for_pool_authorization(game_id, matchmaking_manager)
        .await
        .map_err(|error| {
            warn!(game_id, user_id, %error, "Failed to attest game matchmaking pool");
            GameJoinAuthorizationError::Warming
        })?;
    validate_game_matchmaking_pool(matchmaking_pool, active_match.as_ref())?;

    if active_match_records_user(active_match.as_ref(), user_id) {
        return Ok(());
    }

    if has_durable_recovery_failure(game_id, game_bus, cluster_namespace).await {
        return Err(game_join_denied(
            crate::recovery::PUBLIC_UNRECOVERABLE_GAME_REASON,
        ));
    }

    // The fenced recovery envelope is the authoritative participant record
    // for live games without a roster (legacy/custom), and for any user the
    // roster does not list. The event subscription later uses this same
    // envelope as its bridge snapshot.
    match game_bus.get_recovery(cluster_namespace, game_id).await {
        Ok(Some(envelope)) => {
            if game_state_records_user(&envelope.game_state, user_id) {
                return Ok(());
            }
            warn!(
                game_id,
                user_id, "Denied recovery-backed game join to non-participant"
            );
            return Err(game_join_denied("This game is unavailable"));
        }
        Ok(None) => {}
        Err(error) => {
            warn!(game_id, user_id, %error, "Failed to load recovery while authorizing game join");
        }
    }

    let cached_active_state = match event_router.get_stored_snapshot(game_id).await {
        Ok(Some(game_state)) if matches!(game_state.status, GameStatus::Complete { .. }) => {
            if game_state_records_user(&game_state, user_id) {
                return Ok(());
            }

            warn!(
                "Denied stored Redis game {} join to user {}: user is not a recorded participant",
                game_id, user_id
            );
            return Err(game_join_denied("This game is unavailable"));
        }
        Ok(Some(cached_game_state)) => Some(cached_game_state),
        Ok(None) => None,
        Err(e) => {
            warn!(
                "Failed to load stored Redis snapshot while authorizing game {}: {}",
                game_id, e
            );
            None
        }
    };

    // Completion persistence can win the race with removal/replacement of the
    // preceding active Redis reload snapshot. A durable terminal state is the
    // authority for a completed game, so do not let that stale cache force the
    // participant into an endless GameWarming retry. Failure, absence, malformed
    // data, or a non-terminal database state is not proof that the live game is
    // gone: retain the normal bounded replica warm-up in all of those cases.
    if cached_active_state.is_some()
        && let Ok(database_game_id) = i32::try_from(game_id)
    {
        match db.get_game_by_id(database_game_id).await {
            Ok(Some(game)) => {
                if let Some(game_state_json) = game.game_state {
                    match serde_json::from_value::<GameState>(game_state_json) {
                        Ok(game_state)
                            if matches!(game_state.status, GameStatus::Complete { .. }) =>
                        {
                            if game_state_records_user(&game_state, user_id) {
                                return Ok(());
                            }
                            warn!(
                                game_id,
                                user_id,
                                "Denied durable completed-game join to non-participant while Redis held a stale active snapshot"
                            );
                            return Err(game_join_denied("This game is unavailable"));
                        }
                        Ok(_) => {}
                        Err(error) => warn!(
                            game_id,
                            user_id,
                            %error,
                            "Ignoring malformed durable game state while warming a cached active game"
                        ),
                    }
                }
            }
            Ok(None) => {}
            Err(error) => warn!(
                game_id,
                user_id,
                %error,
                "Durable game lookup failed while warming a cached active game"
            ),
        }
    }

    // Repeat the request while waiting: a request written during the lease gap
    // is intentionally not relied upon. This also covers the short interval
    // after atomic matchmaking commit but before GameCreated is consumed and
    // its initial envelope checkpointed.
    if let Some(live_game_state) = wait_for_authoritative_state_after_snapshot_request(
        game_id,
        event_router,
        game_bus,
        cluster_namespace,
    )
    .await
    {
        if let Some(cached_game_state) = cached_active_state.as_ref()
            && (live_game_state.start_ms != cached_game_state.start_ms
                || live_game_state.event_sequence < cached_game_state.event_sequence)
        {
            warn!(
                "Refusing game {} join because cached and live runtime identities differ (cached start {}, sequence {}; live start {}, sequence {})",
                game_id,
                cached_game_state.start_ms,
                cached_game_state.event_sequence,
                live_game_state.start_ms,
                live_game_state.event_sequence
            );
            return Err(game_join_denied("This game is unavailable"));
        }

        if game_state_records_user(&live_game_state, user_id) {
            return Ok(());
        }

        warn!(
            "Denied warmed game {} join to user {}: user is not a recorded participant",
            game_id, user_id
        );
        return Err(game_join_denied("This game is unavailable"));
    }

    if cached_active_state.is_some() {
        debug!(
            "Live game {} did not reach replication during the bounded authorization wait; asking the client to retry",
            game_id
        );
        if has_durable_recovery_failure(game_id, game_bus, cluster_namespace).await {
            return Err(game_join_denied(
                crate::recovery::PUBLIC_UNRECOVERABLE_GAME_REASON,
            ));
        }
        return Err(GameJoinAuthorizationError::Warming);
    }

    let database_game_id = i32::try_from(game_id)
        .map_err(|_| game_join_denied("This game was not found or has expired"))?;
    let game = db.get_game_by_id(database_game_id).await.map_err(|e| {
        error!(
            "Failed to fetch game {} while authorizing user {}: {}",
            game_id, user_id, e
        );
        GameJoinAuthorizationError::Warming
    })?;
    let Some(game) = game else {
        let mapped_game_id = match load_durable_active_game(user_id, matchmaking_manager).await {
            Ok(mapped_game_id) => mapped_game_id,
            Err(error) => {
                warn!(game_id, user_id, %error, "Active-game lookup failed while classifying a missing durable game");
                return Err(GameJoinAuthorizationError::Warming);
            }
        };
        return Err(missing_game_join_failure(game_id, mapped_game_id));
    };
    let Some(game_state_json) = game.game_state else {
        if has_durable_recovery_failure(game_id, game_bus, cluster_namespace).await {
            return Err(game_join_denied(
                crate::recovery::PUBLIC_UNRECOVERABLE_GAME_REASON,
            ));
        }
        return Err(GameJoinAuthorizationError::Warming);
    };
    let game_state = serde_json::from_value::<GameState>(game_state_json).map_err(|e| {
        error!(
            "Failed to deserialize game {} while authorizing user {}: {}",
            game_id, user_id, e
        );
        game_join_denied("The saved game data could not be loaded")
    })?;

    if !matches!(game_state.status, GameStatus::Complete { .. }) {
        warn!(
            "Refusing database-only non-complete game {} join for user {}",
            game_id, user_id
        );
        if has_durable_recovery_failure(game_id, game_bus, cluster_namespace).await {
            return Err(game_join_denied(
                crate::recovery::PUBLIC_UNRECOVERABLE_GAME_REASON,
            ));
        }
        return Err(GameJoinAuthorizationError::Warming);
    }

    if !game_state_records_user(&game_state, user_id) {
        warn!(
            "Denied database game {} join to user {}: user is not a recorded participant",
            game_id, user_id
        );
        return Err(game_join_denied("This game is unavailable"));
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn authorize_game_join(
    game_id: u32,
    user_id: u32,
    matchmaking_pool: MatchmakingPool,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
    event_router: &Arc<crate::replication::GameEventRouter>,
    game_bus: &Arc<GameBus>,
    cluster_namespace: &ClusterNamespace,
    db: &Arc<dyn Database>,
) -> std::result::Result<(), GameJoinAuthorizationError> {
    match tokio::time::timeout(
        GAME_JOIN_AUTHORIZATION_TIMEOUT,
        authorize_game_join_inner(
            game_id,
            user_id,
            matchmaking_pool,
            matchmaking_manager,
            event_router,
            game_bus,
            cluster_namespace,
            db,
        ),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            warn!(
                game_id,
                user_id, "Game join authorization timed out; returning retryable warm-up"
            );
            Err(GameJoinAuthorizationError::Warming)
        }
    }
}

/// Recover a committed matchmaking result without relying on its best-effort
/// Pub/Sub notification. The per-user mapping is written atomically with the
/// game command, so every participant can discover it on any gateway after a
/// reconnect. Authorization still comes from game state; a stale or malformed
/// mapping is never enough to disclose a game, and only fenced completion owns
/// deletion of the mapping.
#[allow(clippy::too_many_arguments)]
async fn notify_durable_active_game_after_auth(
    user_id: u32,
    matchmaking_pool: MatchmakingPool,
    ws_tx: &mpsc::Sender<Message>,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
    event_router: &Arc<crate::replication::GameEventRouter>,
    game_bus: &Arc<GameBus>,
    cluster_namespace: &ClusterNamespace,
    db: &Arc<dyn Database>,
) -> Result<()> {
    let mapped_game_id = match tokio::time::timeout(ACTIVE_GAME_MAPPING_TIMEOUT, async {
        let mut manager = {
            let manager = matchmaking_manager.lock().await;
            manager.clone()
        };
        manager.get_user_active_game(user_id).await
    })
    .await
    {
        Ok(Ok(game_id)) => game_id,
        Ok(Err(error)) => {
            warn!(user_id, %error, "Failed to resolve durable active-game mapping");
            None
        }
        Err(_) => {
            warn!(user_id, "Timed out resolving durable active-game mapping");
            None
        }
    };
    let Some(game_id) = mapped_game_id else {
        return Ok(());
    };

    match authorize_game_join(
        game_id,
        user_id,
        matchmaking_pool,
        matchmaking_manager,
        event_router,
        game_bus,
        cluster_namespace,
        db,
    )
    .await
    {
        Ok(()) => {
            info!(
                user_id,
                game_id, "Recovered committed match from durable user mapping"
            );
            ws_tx
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::JoinGame(game_id))?.into(),
                ))
                .await
                .context("WebSocket closed while restoring committed match")?;
        }
        Err(GameJoinAuthorizationError::Warming) => {
            info!(
                user_id,
                game_id, "Committed match replica is still warming; client will retry"
            );
            ws_tx
                .send(Message::Text(
                    serde_json::to_string(&WSMessage::GameWarming {
                        game_id,
                        retry_after_ms: GAME_WARMING_RETRY_MS,
                    })?
                    .into(),
                ))
                .await
                .context("WebSocket closed while reporting committed match warm-up")?;
        }
        Err(GameJoinAuthorizationError::Denied(reason)) => {
            warn!(
                user_id,
                game_id,
                reason,
                "Durable active-game mapping did not pass participant authorization; leaving cleanup to fenced completion"
            );
        }
    }

    Ok(())
}

/// Publish a readiness confirmation once the game provably exists.
///
/// A hint-driven client can confirm readiness milliseconds after the
/// matchmaking commit — before the outbox scanner has delivered `GameCreated`
/// into the partition command stream. Publishing immediately would let the
/// confirmation precede `GameCreated` in that stream and be quarantined as
/// targeting an inactive game. The fenced recovery envelope is written by the
/// same incorporation that creates the actor, so its existence is durable
/// proof the confirmation can no longer outrun creation. The wait fails open
/// at its deadline: a confirmation the executor cannot attribute costs the
/// player nothing worse than waiting out the readiness deadline.
///
/// The wait deliberately outlives its socket — a player who confirms and
/// immediately reconnects has still confirmed — but not its server: it is
/// bound to the task cancellation token so shutdown cannot be raced by a
/// publish on a retiring gateway's connections. Abandoning is safe because a
/// drained client re-joins elsewhere and re-confirms well inside
/// `MATCH_READY_WINDOW_MS`.
async fn publish_player_ready_after_game_exists(
    game_bus: Arc<GameBus>,
    cluster_namespace: ClusterNamespace,
    game_id: u32,
    user_id: u32,
    cancellation_token: CancellationToken,
) {
    let partition_id = game_id % PARTITION_COUNT;
    let deadline = tokio::time::Instant::now() + COLD_JOIN_WARMUP_TIMEOUT;
    loop {
        let recovery = tokio::select! {
            biased;
            _ = cancellation_token.cancelled() => return,
            recovery = game_bus.get_recovery(&cluster_namespace, game_id) => recovery,
        };
        match recovery {
            Ok(Some(_)) => break,
            Ok(None) => {}
            Err(error) => {
                warn!(game_id, user_id, %error, "Failed to check game existence before readiness");
            }
        }
        if tokio::time::Instant::now() >= deadline {
            warn!(
                game_id,
                user_id, "Publishing readiness without game-existence proof after bounded wait"
            );
            break;
        }
        tokio::select! {
            biased;
            _ = cancellation_token.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_millis(100)) => {}
        }
    }

    let event = StreamEvent::PlayerReadySubmitted { game_id, user_id };
    match game_bus
        .publish_player_ready_unless_completed(&cluster_namespace, partition_id, game_id, &event)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            debug!(
                game_id,
                user_id, "Readiness arrived after the game completed"
            );
        }
        Err(error) => {
            warn!(game_id, user_id, %error, "Failed to publish readiness confirmation");
        }
    }
}

fn recovery_bridge_snapshot(envelope: &RecoveryEnvelopeV2, user_id: u32) -> GameEventMessage {
    // Despite its historical name, `next_event_stream_sequence` is the last
    // sequence already emitted and checkpointed. The actor increments it
    // before its next publish, so subtracting one here manufactures a gap.
    GameEventMessage {
        game_id: envelope.game_id,
        tick: envelope.game_state.tick,
        sequence: envelope.game_state.event_sequence,
        stream_seq: envelope.next_event_stream_sequence,
        user_id: Some(user_id),
        event: GameEvent::Snapshot {
            game_state: envelope.game_state.clone(),
        },
    }
}

async fn send_recovery_bridge_snapshot(
    ws_tx: &mpsc::Sender<Message>,
    envelope: &RecoveryEnvelopeV2,
    user_id: u32,
) -> bool {
    let recovery_snapshot = recovery_bridge_snapshot(envelope, user_id);
    let Ok(json) = serde_json::to_string(&WSMessage::GameEvent(recovery_snapshot)) else {
        return false;
    };
    ws_tx.send(Message::Text(json.into())).await.is_ok()
}

/// Load the game's durable terminal state, if one exists. Absence, failure,
/// malformed data, or a non-terminal record all return `None`: none of those
/// prove anything about a live game, so callers fall back to normal warm-up.
/// Everything a rematch needs to know about the match that just ended.
///
/// The roster is read from the authoritative terminal state, never from the
/// client, so a spectator cannot put themselves on the results card. Same two
/// sources the join path uses: the Redis terminal snapshot first, then the
/// durable record once persistence has caught up.
async fn resolve_rematch_context(
    event_router: &Arc<crate::replication::GameEventRouter>,
    db: &Arc<dyn Database>,
    game_id: u32,
) -> Option<(Vec<(u32, String)>, common::GameType, common::QueueMode)> {
    let terminal_state = match event_router.get_stored_snapshot(game_id).await {
        Ok(Some(state)) if matches!(state.status, GameStatus::Complete { .. }) => Some(state),
        _ => load_durable_terminal_state(db, game_id).await,
    }?;

    let mut participants: Vec<(u32, String)> = terminal_state
        .players
        .keys()
        .copied()
        // Someone removed for inactivity forfeited this match; putting them on
        // the rematch card would let a player who walked away hold up the
        // group that stayed.
        .filter(|user_id| !terminal_state.is_player_idle_kicked(*user_id))
        .map(|user_id| {
            let username = terminal_state
                .usernames
                .get(&user_id)
                .cloned()
                .unwrap_or_else(|| format!("User{user_id}"));
            (user_id, username)
        })
        .collect();
    participants.sort_by_key(|(user_id, _)| *user_id);

    Some((
        participants,
        terminal_state.game_type.clone(),
        terminal_state.queue_mode,
    ))
}

/// Record-then-elect: refresh this socket's view, and if the group is now big
/// enough and this player is the elected host, stand up the lobby everyone
/// converges on.
///
/// Returns the connection's (possibly new) lobby handle. The error variant
/// carries it back too — a failure here must not lose a lobby the connection
/// has already joined.
#[allow(clippy::too_many_arguments)]
async fn advance_rematch(
    game_id: u32,
    user_id: u32,
    rematch: &RematchStore,
    event_router: &Arc<crate::replication::GameEventRouter>,
    db: &Arc<dyn Database>,
    lobby_manager: &Arc<crate::lobby_manager::LobbyManager>,
    metadata: &PlayerMetadata,
    lobby: Option<LobbyJoinHandle>,
    region: &str,
    websocket_id: &str,
    ws_tx: &mpsc::Sender<Message>,
) -> std::result::Result<Option<LobbyJoinHandle>, (Option<LobbyJoinHandle>, anyhow::Error)> {
    let state = match send_rematch_state(game_id, user_id, rematch, event_router, db, ws_tx).await {
        Ok(Some(state)) => state,
        Ok(None) => return Ok(lobby),
        Err(error) => return Err((lobby, error)),
    };

    // Everyone else is told to re-read; the record is what they read, so a
    // dropped hint only costs them the reconcile interval.
    for participant in &state.participants {
        if participant.user_id != user_id {
            rematch.hint(participant.user_id).await;
        }
    }

    // Only the elected host stands up the lobby, and only once the count can
    // actually form a game. `SET NX` settles any disagreement anyway.
    if state.game_type.is_none()
        || state.host_user_id != Some(user_id)
        || state.lobby_code.is_some()
    {
        return Ok(lobby);
    }

    let (lobby, code) = match lobby {
        Some(handle) => {
            let code = handle.lobby_code.clone();
            (Some(handle), code)
        }
        None => {
            let created = match lobby_manager
                .create_lobby_for_pool(metadata.user_id, region, metadata.matchmaking_pool)
                .await
            {
                Ok(created) => created,
                Err(error) => return Err((None, error)),
            };
            match lobby_manager
                .join_lobby_for_pool(
                    Some(created.lobby_code()),
                    metadata.user_id,
                    metadata.username.clone(),
                    websocket_id.to_string(),
                    region.to_string(),
                    None,
                    metadata.matchmaking_pool,
                    metadata.distribution,
                    metadata.supports_ad_break,
                    metadata.can_show_video_ad,
                )
                .await
            {
                Ok(handle) => {
                    let code = handle.lobby_code.clone();
                    (Some(handle), code)
                }
                Err(error) => return Err((None, error)),
            }
        }
    };

    let elected = match rematch.elect_lobby(game_id, &code).await {
        Ok(elected) => elected,
        Err(error) => return Err((lobby, error)),
    };
    if elected == code
        && let Some(handle) = &lobby
    {
        // The client needs to know it holds this lobby, the same way it would
        // after an explicit CreateLobby.
        let created = WSMessage::LobbyCreated {
            lobby_code: handle.lobby_code.clone(),
        };
        if let Ok(frame) = serde_json::to_string(&created) {
            let _ = ws_tx.send(Message::Text(frame.into())).await;
        }
    }

    // Re-read so this socket and everyone it hints see the elected lobby.
    if let Err(error) = send_rematch_state(game_id, user_id, rematch, event_router, db, ws_tx).await
    {
        return Err((lobby, error));
    }
    for participant in &state.participants {
        if participant.user_id != user_id {
            rematch.hint(participant.user_id).await;
        }
    }

    Ok(lobby)
}

/// Read the rematch record and push it to this socket.
async fn send_rematch_state(
    game_id: u32,
    user_id: u32,
    rematch: &RematchStore,
    event_router: &Arc<crate::replication::GameEventRouter>,
    db: &Arc<dyn Database>,
    ws_tx: &mpsc::Sender<Message>,
) -> Result<Option<RematchState>> {
    let Some((participants, game_type, queue_mode)) =
        resolve_rematch_context(event_router, db, game_id).await
    else {
        return Ok(None);
    };
    if !participants.iter().any(|(id, _)| *id == user_id) {
        return Ok(None);
    }

    let state = rematch
        .state(game_id, &participants, &game_type, queue_mode)
        .await
        .context("failed to read the rematch state")?;
    let frame = serde_json::to_string(&WSMessage::Rematch(state.clone()))
        .context("failed to serialize the rematch state")?;
    ws_tx
        .send(Message::Text(frame.into()))
        .await
        .context("WebSocket closed before the rematch state")?;
    Ok(Some(state))
}

async fn load_durable_terminal_state(db: &Arc<dyn Database>, game_id: u32) -> Option<GameState> {
    let database_game_id = i32::try_from(game_id).ok()?;
    match db.get_game_by_id(database_game_id).await {
        Ok(Some(game)) => {
            let game_state_json = game.game_state?;
            match serde_json::from_value::<GameState>(game_state_json) {
                Ok(game_state) if matches!(game_state.status, GameStatus::Complete { .. }) => {
                    Some(game_state)
                }
                Ok(_) => None,
                Err(error) => {
                    warn!(game_id, %error, "Ignoring malformed durable game state during warm-up");
                    None
                }
            }
        }
        Ok(None) => None,
        Err(error) => {
            warn!(game_id, %error, "Durable game lookup failed during warm-up");
            None
        }
    }
}

/// How the join warm-up anchored the socket's first authoritative frame.
enum FirstFrame {
    /// A frame was sent; enter the forwarding loop. `live_proven` is false
    /// when the frame was the recovery-envelope bridge: the command-outcome
    /// replay (whose `CommandOutcomesComplete` barrier is a make-before-break
    /// promotion signal) must then wait for the first live event, so a
    /// candidate socket never retires the old usable socket for a bridge that
    /// might have no subsequent event stream.
    Anchored { live_proven: bool },
    /// The game is durably complete and was fully served; end the task.
    Served,
    /// No authoritative frame arrived in the bounded window; the client was
    /// told to retry (or that the game failed) and the task ends.
    Unavailable,
}

/// Produce the socket's first authoritative frame and anchor the subscription.
///
/// Order of preference: the durable terminal snapshot (completed reload), the
/// fenced recovery-envelope bridge (bounded staleness of one checkpoint
/// interval, anchors contiguous live deltas immediately), then a live
/// `Snapshot` arriving through the already-registered subscription. If none
/// arrives inside the bounded window, fall back to the database for terminal
/// state, else tell the client to retry with `GameWarming`.
#[allow(clippy::too_many_arguments)]
async fn anchor_first_frame(
    game_id: u32,
    user_id: u32,
    ws_tx: &mpsc::Sender<Message>,
    subscription: &mut crate::replication::GameEventSubscription,
    event_router: &Arc<crate::replication::GameEventRouter>,
    db: &Arc<dyn Database>,
    game_bus: &Arc<GameBus>,
    cluster_namespace: &ClusterNamespace,
) -> FirstFrame {
    // Completed games have no live event flow to wait for; serve the durable
    // terminal snapshot immediately.
    match event_router.get_stored_snapshot(game_id).await {
        Ok(Some(game_state))
            if matches!(game_state.status, GameStatus::Complete { .. })
                && game_state_records_user(&game_state, user_id) =>
        {
            send_completed_game_snapshot(
                ws_tx,
                game_bus,
                cluster_namespace,
                game_id,
                user_id,
                &game_state,
                "stored Redis snapshot",
            )
            .await;
            return FirstFrame::Served;
        }
        Ok(_) => {}
        Err(error) => {
            warn!(game_id, %error, "Failed to inspect stored snapshot during warm-up");
        }
    }

    // Bridge from the fenced recovery envelope: an immediate first frame with
    // a trusted watermark. Contiguous live deltas then forward without any
    // extra snapshot; a lost event in between surfaces as a gap and re-anchors
    // through the targeted-request path.
    match game_bus.get_recovery(cluster_namespace, game_id).await {
        Ok(Some(envelope))
            if !matches!(envelope.game_state.status, GameStatus::Complete { .. })
                && game_state_records_user(&envelope.game_state, user_id) =>
        {
            // Completion persistence can win the race with cleanup of the
            // preceding active Redis state. The durable terminal record is
            // the authority for a completed game, so a non-terminal envelope
            // may bridge only after that record is confirmed absent or
            // non-terminal — otherwise the client would be stranded on a
            // stale active frame with no live stream behind it.
            if let Some(terminal_state) = load_durable_terminal_state(db, game_id).await {
                send_completed_game_snapshot(
                    ws_tx,
                    game_bus,
                    cluster_namespace,
                    game_id,
                    user_id,
                    &terminal_state,
                    "database snapshot",
                )
                .await;
                return FirstFrame::Served;
            }
            if !send_recovery_bridge_snapshot(ws_tx, &envelope, user_id).await {
                return FirstFrame::Unavailable;
            }
            subscription.anchor(envelope.next_event_stream_sequence);
            return FirstFrame::Anchored { live_proven: false };
        }
        Ok(_) => {}
        Err(error) => {
            warn!(game_id, user_id, %error, "Failed to load recovery during warm-up");
        }
    }

    // No envelope yet (creation race or ownership gap). Request a targeted
    // snapshot and wait for the live stream to anchor us; the subscription
    // re-paces the request internally while cold.
    if let Err(error) = event_router.request_game_snapshot(game_id).await {
        warn!(game_id, %error, "Failed to request warm-up snapshot");
    }
    let warmup = tokio::time::timeout(COLD_JOIN_WARMUP_TIMEOUT, async {
        loop {
            match subscription.next().await {
                Some(crate::replication::SubscriptionUpdate::Event(event_msg)) => {
                    if matches!(event_msg.event, GameEvent::Snapshot { .. }) {
                        break Some(event_msg);
                    }
                    // A zero-seq terminal rejection still reaches the player
                    // during warm-up; nothing else passes while cold.
                    let json = serde_json::to_string(&WSMessage::GameEvent(event_msg)).unwrap();
                    if ws_tx.send(Message::Text(json.into())).await.is_err() {
                        break None;
                    }
                }
                Some(crate::replication::SubscriptionUpdate::WentCold) => {}
                None => break None,
            }
        }
    })
    .await;

    if let Ok(Some(mut snapshot_event)) = warmup {
        snapshot_event.user_id = Some(user_id);
        let terminal = matches!(
            &snapshot_event.event,
            GameEvent::Snapshot { game_state }
                if matches!(game_state.status, GameStatus::Complete { .. })
        );
        let json = serde_json::to_string(&WSMessage::GameEvent(snapshot_event)).unwrap();
        if ws_tx.send(Message::Text(json.into())).await.is_err() {
            return FirstFrame::Unavailable;
        }
        if terminal {
            if send_command_outcomes(
                ws_tx,
                game_bus,
                cluster_namespace,
                game_id,
                user_id,
                Some(TERMINAL_COMMAND_REJECTION_REASON),
            )
            .await
            {
                info!(game_id, "Warm-up reached terminal state directly");
            }
            return FirstFrame::Served;
        }
        return FirstFrame::Anchored { live_proven: true };
    }

    // Nothing live arrived. Durably completed games remain readable from the
    // database after their Redis grace period.
    let Ok(database_game_id) = i32::try_from(game_id) else {
        warn!("Game ID {} is outside the durable database range", game_id);
        send_game_load_failed(ws_tx, game_id, "This game was not found or has expired").await;
        return FirstFrame::Unavailable;
    };
    match db.get_game_by_id(database_game_id).await {
        Ok(Some(game)) => {
            if let Some(game_state_json) = game.game_state {
                match serde_json::from_value::<GameState>(game_state_json) {
                    Ok(game_state) if matches!(game_state.status, GameStatus::Complete { .. }) => {
                        send_completed_game_snapshot(
                            ws_tx,
                            game_bus,
                            cluster_namespace,
                            game_id,
                            user_id,
                            &game_state,
                            "database snapshot",
                        )
                        .await;
                        return FirstFrame::Served;
                    }
                    Ok(_) => {
                        info!(
                            game_id,
                            "Durable game is non-terminal with no live stream; returning retryable warm-up"
                        );
                        send_game_warming(ws_tx, game_id).await;
                        return FirstFrame::Unavailable;
                    }
                    Err(e) => {
                        error!("Failed to deserialize game state from database: {}", e);
                        send_game_load_failed(
                            ws_tx,
                            game_id,
                            "The saved game data could not be loaded",
                        )
                        .await;
                        return FirstFrame::Unavailable;
                    }
                }
            }
            info!(
                game_id,
                "Durable game has no terminal state and no live stream; returning retryable warm-up"
            );
            send_game_warming(ws_tx, game_id).await;
            FirstFrame::Unavailable
        }
        Ok(None) => {
            // Authorization already proved this game from durable evidence.
            // Missing completion persistence here is therefore a failover
            // race, not definitive proof that it expired.
            info!(
                game_id,
                "Authorized game is not yet durable and has no live stream; returning retryable warm-up"
            );
            send_game_warming(ws_tx, game_id).await;
            FirstFrame::Unavailable
        }
        Err(e) => {
            error!("Failed to fetch game {} from database: {}", game_id, e);
            send_game_warming(ws_tx, game_id).await;
            FirstFrame::Unavailable
        }
    }
}

// Helper function to subscribe to game events
async fn subscribe_to_game_events(
    game_id: u32,
    user_id: u32,
    ws_tx: mpsc::Sender<Message>,
    event_router: Arc<crate::replication::GameEventRouter>,
    db: Arc<dyn Database>,
    game_bus: Arc<GameBus>,
    cluster_namespace: ClusterNamespace,
) {
    info!(
        "Subscribing to game {} events for user {}",
        game_id, user_id
    );

    // Register the receiver BEFORE any snapshot work: a broadcast receiver
    // only sees messages sent after it exists, so subscribing first
    // guarantees no event between first frame and subscription can be
    // missed; the subscription's continuity rules drop the overlap instead.
    let mut subscription = event_router.subscribe_to_game(game_id).await;

    let anchor_result = anchor_first_frame(
        game_id,
        user_id,
        &ws_tx,
        &mut subscription,
        &event_router,
        &db,
        &game_bus,
        &cluster_namespace,
    )
    .await;
    let mut live_proven = match anchor_result {
        FirstFrame::Anchored { live_proven } => live_proven,
        FirstFrame::Served | FirstFrame::Unavailable => return,
    };

    // Loading recovery outcomes can take several bounded Redis attempts. Keep
    // that I/O concurrent with the live broadcast receiver so a fresh
    // snapshot never stalls CommandScheduled delivery. The replay result still
    // returns through this one forwarding loop, which preserves socket order.
    // A bridge-anchored socket starts the replay only once the first live
    // event proves the stream flows (see `FirstFrame::Anchored`).
    let mut command_outcome_replay = live_proven.then(|| {
        start_command_outcome_replay(
            game_bus.clone(),
            cluster_namespace.clone(),
            game_id,
            user_id,
        )
    });

    loop {
        let input =
            next_game_subscription_input(&ws_tx, &mut subscription, &mut command_outcome_replay)
                .await;
        let event_msg = match input {
            GameSubscriptionInput::SocketClosed => {
                debug!(
                    "WebSocket send channel closed for game {}, stopping event subscription",
                    game_id
                );
                return;
            }
            GameSubscriptionInput::CommandOutcomes(resolved) => {
                // Taking the completed future makes the next select ignore
                // replay until another snapshot explicitly starts one.
                drop(command_outcome_replay.take());
                let Some(resolved) = resolved else {
                    send_game_warming(&ws_tx, game_id).await;
                    return;
                };
                if !send_command_outcomes_from_resolved(&ws_tx, game_id, user_id, resolved, None)
                    .await
                {
                    return;
                }
                continue;
            }
            GameSubscriptionInput::Update(Some(crate::replication::SubscriptionUpdate::Event(
                event_msg,
            ))) => event_msg,
            GameSubscriptionInput::Update(Some(
                crate::replication::SubscriptionUpdate::WentCold,
            )) => {
                // The subscription lost continuity (gap or broadcast lag) and
                // already paced a targeted snapshot request; nothing is
                // forwarded until the fresh snapshot re-anchors the client.
                // Any barrier for the previous snapshot is now stale, and
                // dropping the in-flight future cancels its Redis read; only
                // the replay paired with the replacement snapshot may emit a
                // barrier.
                drop(command_outcome_replay.take());
                continue;
            }
            GameSubscriptionInput::Update(None) => {
                drop(command_outcome_replay.take());
                // The game's channel is gone. Terminal teardown always
                // delivers the terminal snapshot first (handled below), so
                // reaching this arm means the reader worker failed — which is
                // task-fatal — or the terminal frame raced this receiver.
                // Log loudly; the client's liveness watchdog and
                // RequestResync path recover the session.
                warn!(
                    game_id,
                    user_id, "Game event channel closed; ending subscription"
                );
                return;
            }
        };

        // Check if the game has ended
        let is_terminal = matches!(
            &event_msg.event,
            GameEvent::StatusUpdated { status } if matches!(status, GameStatus::Complete { .. })
        ) || matches!(
            &event_msg.event,
            GameEvent::Snapshot { game_state }
                if matches!(game_state.status, GameStatus::Complete { .. })
        );
        let is_snapshot = snapshot_requires_command_outcomes(&event_msg.event);

        if is_snapshot || is_terminal {
            // A later snapshot (or terminal frontier) invalidates a barrier
            // for any earlier snapshot. Cancel before enqueuing the new
            // frontier so a stale replay can never follow it.
            drop(command_outcome_replay.take());
        }

        let json = serde_json::to_string(&WSMessage::GameEvent(event_msg)).unwrap();
        let msg = Message::Text(json.into());
        if let Err(e) = ws_tx.try_send(msg.clone()) {
            match e {
                mpsc::error::TrySendError::Full(_) => {
                    warn!(
                        "WebSocket send channel full for game {}, blocking send",
                        game_id
                    );
                    if ws_tx.send(msg).await.is_err() {
                        debug!(
                            "WebSocket send channel closed for game {}, stopping event subscription",
                            game_id
                        );
                        break;
                    }
                }
                mpsc::error::TrySendError::Closed(_) => {
                    debug!(
                        "WebSocket send channel closed for game {}, stopping event subscription",
                        game_id
                    );
                    break;
                }
            }
        }

        if is_terminal {
            // A takeover can replace terminal command events that were
            // published by the old owner but never reached this socket.
            // Terminal state requires durable replay before the client may
            // clear its command outbox or finish certification. No later live
            // event needs forwarding, so waiting here cannot stall gameplay.
            if !send_command_outcomes(
                &ws_tx,
                &game_bus,
                &cluster_namespace,
                game_id,
                user_id,
                is_terminal.then_some(TERMINAL_COMMAND_REJECTION_REASON),
            )
            .await
            {
                return;
            }
            info!("Game {} completed, stopping event subscription", game_id);
            break;
        }

        if is_snapshot || !live_proven {
            // Every snapshot restarts the replay so its barrier pairs with
            // the newest frontier. The first live event after a bridge anchor
            // also starts it: live delivery is now proven, so the
            // make-before-break promotion signal may flow.
            live_proven = true;
            command_outcome_replay = Some(start_command_outcome_replay(
                game_bus.clone(),
                cluster_namespace.clone(),
                game_id,
                user_id,
            ));
        }
    }
}

async fn send_command_outcomes(
    ws_tx: &mpsc::Sender<Message>,
    game_bus: &GameBus,
    cluster_namespace: &ClusterNamespace,
    game_id: u32,
    user_id: u32,
    terminal_rejection_reason: Option<&str>,
) -> bool {
    let resolved = tokio::select! {
        _ = ws_tx.closed() => return false,
        resolved = load_command_outcomes(game_bus, cluster_namespace, game_id, user_id) => resolved,
    };
    let Some(resolved) = resolved else {
        send_game_warming(ws_tx, game_id).await;
        return false;
    };
    send_command_outcomes_from_resolved(
        ws_tx,
        game_id,
        user_id,
        resolved,
        terminal_rejection_reason,
    )
    .await
}

async fn load_command_outcomes(
    game_bus: &GameBus,
    cluster_namespace: &ClusterNamespace,
    game_id: u32,
    user_id: u32,
) -> Option<ResolvedCommandState> {
    let deadline = tokio::time::Instant::now() + COMMAND_OUTCOME_LOAD_TIMEOUT;
    let envelope = loop {
        match tokio::time::timeout(
            COMMAND_OUTCOME_READ_TIMEOUT,
            game_bus.get_recovery(cluster_namespace, game_id),
        )
        .await
        {
            Ok(Ok(Some(envelope))) => break envelope,
            Ok(Ok(None)) => {
                debug!(game_id, user_id, "Recovery envelope is not visible yet");
            }
            Ok(Err(error)) => {
                warn!(game_id, user_id, %error, "Failed to load command outcomes for snapshot; retrying");
            }
            Err(_) => {
                warn!(
                    game_id,
                    user_id, "Timed out loading command outcomes for snapshot; retrying"
                );
            }
        }

        if tokio::time::Instant::now() >= deadline {
            warn!(
                game_id,
                user_id, "Command outcomes did not become readable before the warm-up deadline"
            );
            return None;
        }
        tokio::time::sleep(COMMAND_OUTCOME_RETRY_DELAY).await;
    };

    Some(envelope.resolved_client_commands)
}

async fn load_completed_command_outcomes(
    game_bus: &GameBus,
    cluster_namespace: &ClusterNamespace,
    game_id: u32,
    user_id: u32,
) -> Option<RecoveryEnvelopeV2> {
    match tokio::time::timeout(
        COMMAND_OUTCOME_READ_TIMEOUT,
        game_bus.get_recovery(cluster_namespace, game_id),
    )
    .await
    {
        Ok(Ok(Some(envelope))) => Some(envelope),
        Ok(Ok(None)) => {
            warn!(
                game_id,
                user_id,
                "Terminal recovery outcomes are no longer retained; terminal fallback will remain non-dispositive"
            );
            None
        }
        Ok(Err(error)) => {
            warn!(
                game_id,
                user_id,
                %error,
                "Failed to load terminal recovery outcomes; terminal fallback will remain non-dispositive"
            );
            None
        }
        Err(_) => {
            warn!(
                game_id,
                user_id,
                "Timed out loading terminal recovery outcomes; terminal fallback will remain non-dispositive"
            );
            None
        }
    }
}

async fn send_command_outcomes_from_resolved(
    ws_tx: &mpsc::Sender<Message>,
    game_id: u32,
    user_id: u32,
    resolved: ResolvedCommandState,
    terminal_rejection_reason: Option<&str>,
) -> bool {
    for (client_game_session_id, session) in command_outcomes_for_user(resolved, user_id) {
        let response = WSMessage::CommandOutcomes {
            game_id,
            client_game_session_id,
            contiguous_through: session.contiguous_through,
            outcomes: session.outcomes,
            rejection_fence: session.rejection_fence,
        };
        let json = match serde_json::to_string(&response) {
            Ok(json) => json,
            Err(error) => {
                error!(game_id, user_id, %error, "Failed to serialize command outcomes");
                return false;
            }
        };
        if ws_tx.send(Message::Text(json.into())).await.is_err() {
            debug!(
                game_id,
                user_id, "WebSocket closed while sending command outcomes"
            );
            return false;
        }
    }

    // A user can legitimately have no recorded command session. The explicit
    // barrier distinguishes that case from a delayed/failed recovery read, so
    // make-before-break never promotes based on a timing assumption.
    send_command_outcome_barrier(ws_tx, game_id, user_id, terminal_rejection_reason).await
}

async fn send_command_outcome_barrier(
    ws_tx: &mpsc::Sender<Message>,
    game_id: u32,
    user_id: u32,
    terminal_rejection_reason: Option<&str>,
) -> bool {
    let response = WSMessage::CommandOutcomesComplete {
        game_id,
        terminal_rejection_reason: terminal_rejection_reason.map(str::to_owned),
    };
    let json = match serde_json::to_string(&response) {
        Ok(json) => json,
        Err(error) => {
            error!(game_id, user_id, %error, "Failed to serialize command outcome barrier");
            return false;
        }
    };
    if ws_tx.send(Message::Text(json.into())).await.is_err() {
        debug!(
            game_id,
            user_id, "WebSocket closed while sending command outcome barrier"
        );
        return false;
    }
    true
}

async fn send_game_snapshot(
    ws_tx: &mpsc::Sender<Message>,
    game_id: u32,
    user_id: u32,
    game_state: &GameState,
) -> Result<()> {
    let snapshot_event = GameEventMessage {
        game_id,
        tick: game_state.tick,
        sequence: game_state.event_sequence,
        stream_seq: 0, // terminal snapshot; no live stream follows
        user_id: Some(user_id),
        event: GameEvent::Snapshot {
            game_state: game_state.clone(),
        },
    };
    let json = serde_json::to_string(&WSMessage::GameEvent(snapshot_event))?;
    ws_tx
        .send(Message::Text(json.into()))
        .await
        .context("WebSocket channel closed while sending game snapshot")
}

async fn send_completed_game_snapshot(
    ws_tx: &mpsc::Sender<Message>,
    game_bus: &GameBus,
    cluster_namespace: &ClusterNamespace,
    game_id: u32,
    user_id: u32,
    game_state: &GameState,
    source: &str,
) {
    if !matches!(game_state.status, GameStatus::Complete { .. }) {
        error!(
            "Refusing to send non-complete {} for game {} to user {}",
            source, game_id, user_id
        );
        send_game_load_failed(ws_tx, game_id, "The saved game data is unavailable").await;
        return;
    }

    // Completed snapshots can include the full player and arena state. Only users recorded
    // as participants in the canonical GameState may reload them; guessed IDs do not grant
    // access once the live subscription is gone.
    if !game_state_records_user(game_state, user_id) {
        warn!(
            "Denied {} reload for game {} to user {}: user is not a recorded participant",
            source, game_id, user_id
        );
        send_game_load_failed(ws_tx, game_id, "This game is unavailable").await;
        return;
    }

    info!(
        "Loaded game {} state from {} for user {}",
        game_id, source, user_id
    );
    // The terminal snapshot remains useful after the short recovery envelope
    // expires. Only a real recovery envelope proves which exact identities
    // were processed before immutable completion; without it, keep the
    // barrier non-dispositive so an in-memory outbox fails closed.
    let retained =
        load_completed_command_outcomes(game_bus, cluster_namespace, game_id, user_id).await;
    let (resolved, terminal_rejection_reason) = match retained {
        Some(envelope) if matches!(envelope.game_state.status, GameStatus::Complete { .. }) => (
            envelope.resolved_client_commands,
            Some(TERMINAL_COMMAND_REJECTION_REASON),
        ),
        Some(_) => {
            warn!(
                game_id,
                user_id,
                "Ignoring nonterminal recovery outcomes beside a durable completed snapshot"
            );
            (ResolvedCommandState::default(), None)
        }
        None => (ResolvedCommandState::default(), None),
    };
    if !send_completed_game_snapshot_from_resolved(
        ws_tx,
        game_id,
        user_id,
        game_state,
        resolved,
        terminal_rejection_reason,
    )
    .await
    {
        error!(
            "Failed to send {} and its terminal command outcomes for game {} to user {}",
            source, game_id, user_id
        );
    }
}

async fn send_completed_game_snapshot_from_resolved(
    ws_tx: &mpsc::Sender<Message>,
    game_id: u32,
    user_id: u32,
    game_state: &GameState,
    resolved: ResolvedCommandState,
    terminal_rejection_reason: Option<&str>,
) -> bool {
    if let Err(error) = send_game_snapshot(ws_tx, game_id, user_id, game_state).await {
        error!(game_id, user_id, %error, "Failed to send completed game snapshot");
        return false;
    }
    send_command_outcomes_from_resolved(
        ws_tx,
        game_id,
        user_id,
        resolved,
        terminal_rejection_reason,
    )
    .await
}

async fn send_game_load_failed(
    ws_tx: &mpsc::Sender<Message>,
    game_id: u32,
    reason: impl Into<String>,
) {
    let response = WSMessage::GameLoadFailed {
        game_id,
        reason: reason.into(),
    };

    match serde_json::to_string(&response) {
        Ok(json) => {
            if let Err(e) = ws_tx.send(Message::Text(json.into())).await {
                debug!(
                    "WebSocket channel closed while reporting load failure for game {}: {}",
                    game_id, e
                );
            }
        }
        Err(e) => {
            error!(
                "Failed to serialize load failure response for game {}: {}",
                game_id, e
            );
        }
    }
}

async fn send_game_warming(ws_tx: &mpsc::Sender<Message>, game_id: u32) {
    let response = WSMessage::GameWarming {
        game_id,
        retry_after_ms: GAME_WARMING_RETRY_MS,
    };
    match serde_json::to_string(&response) {
        Ok(json) => {
            if let Err(error) = ws_tx.send(Message::Text(json.into())).await {
                debug!(game_id, %error, "WebSocket closed while reporting game warm-up");
            }
        }
        Err(error) => {
            error!(game_id, %error, "Failed to serialize game warm-up response");
        }
    }
}

fn next_lobby_match(
    active_game_id: Option<u32>,
    pending_game_id: Option<u32>,
    last_sent_game_id: Option<u32>,
    retry_unacknowledged: bool,
) -> Option<u32> {
    let mapped_game_id = active_game_id.or(pending_game_id)?;
    let is_unacknowledged = pending_game_id == Some(mapped_game_id);
    (Some(mapped_game_id) != last_sent_game_id || (retry_unacknowledged && is_unacknowledged))
        .then_some(mapped_game_id)
}

fn parse_lobby_match_mapping(
    raw_game_id: Option<String>,
    mapping_key: &str,
    lobby_code: &str,
    user_id: u32,
) -> Option<u32> {
    raw_game_id.and_then(|raw_game_id| match raw_game_id.parse::<u32>() {
        Ok(game_id) => Some(game_id),
        Err(error) => {
            error!(
                lobby_code,
                user_id,
                mapping_key,
                raw_game_id,
                %error,
                "Ignoring malformed durable lobby match mapping"
            );
            None
        }
    })
}

/// A successful, participant-authorized `JoinGame` is the delivery
/// acknowledgement. Compare-delete prevents a delayed acknowledgement for an
/// earlier round from consuming a newer assignment written to the same key.
async fn acknowledge_lobby_match_handoff(
    redis: &RedisConnection,
    lobby_code: &str,
    user_id: u32,
    game_id: u32,
) -> Result<bool> {
    let pending_mapping_key = RedisKeys::matchmaking_lobby_user_pending_game(lobby_code, user_id);
    let mut redis = redis.clone();
    let acknowledged: i32 = redis::Script::new(
        r#"
        local value_type = redis.call('TYPE', KEYS[1])
        if type(value_type) == 'table' then value_type = value_type.ok end
        if value_type ~= 'none' and value_type ~= 'string' then return -1 end
        if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
        redis.call('DEL', KEYS[1])
        return 1
        "#,
    )
    .key(&pending_mapping_key)
    .arg(game_id)
    .invoke_async(&mut redis)
    .await
    .context("failed to acknowledge lobby match handoff")?;
    match acknowledged {
        1 => Ok(true),
        0 => Ok(false),
        -1 => Err(anyhow::anyhow!(
            "lobby match handoff has an unexpected Redis type"
        )),
        other => Err(anyhow::anyhow!(
            "unexpected lobby match handoff acknowledgement result {other}"
        )),
    }
}

async fn reconcile_lobby_match(
    lobby_code: &str,
    user_id: u32,
    redis: &mut RedisConnection,
    ws_tx: &mpsc::Sender<Message>,
    last_sent_game_id: &mut Option<u32>,
    retry_unacknowledged: bool,
) -> bool {
    let active_mapping_key = RedisKeys::matchmaking_lobby_active_game(lobby_code);
    let pending_mapping_key = RedisKeys::matchmaking_lobby_user_pending_game(lobby_code, user_id);
    let (raw_active_game_id, raw_pending_game_id): (Option<String>, Option<String>) =
        match redis::cmd("MGET")
            .arg(&active_mapping_key)
            .arg(&pending_mapping_key)
            .query_async(redis)
            .await
        {
            Ok(game_ids) => game_ids,
            Err(error) => {
                warn!(
                    lobby_code,
                    user_id,
                    %error,
                    "Failed to reconcile durable lobby match mappings"
                );
                return true;
            }
        };
    let active_game_id =
        parse_lobby_match_mapping(raw_active_game_id, &active_mapping_key, lobby_code, user_id);
    let pending_game_id = parse_lobby_match_mapping(
        raw_pending_game_id,
        &pending_mapping_key,
        lobby_code,
        user_id,
    );
    let Some(game_id) = next_lobby_match(
        active_game_id,
        pending_game_id,
        *last_sent_game_id,
        retry_unacknowledged,
    ) else {
        return true;
    };

    let message = match serde_json::to_string(&WSMessage::JoinGame(game_id)) {
        Ok(message) => message,
        Err(error) => {
            error!(lobby_code, user_id, game_id, %error, "Failed to serialize lobby match join");
            return true;
        }
    };
    if let Err(error) = ws_tx.send(Message::Text(message.into())).await {
        debug!(
            lobby_code,
            user_id,
            game_id,
            %error,
            "WebSocket closed while forwarding durable lobby match"
        );
        return false;
    }

    *last_sent_game_id = Some(game_id);
    info!(
        lobby_code,
        user_id, game_id, "Forwarded durable lobby match to WebSocket"
    );
    true
}

/// Subscribe first, then read the durable mapping. A commit before SUBSCRIBE is
/// recovered by the GET; a commit after SUBSCRIBE is observed as a low-latency
/// hint. Periodic reconciliation covers a lagged push receiver while the
/// WebSocket itself remains healthy.
async fn subscribe_to_lobby_match_notifications(
    lobby_code: String,
    user_id: u32,
    pubsub_manager: Arc<PubSubManager>,
    redis: impl Into<RedisConnection>,
    ws_tx: mpsc::Sender<Message>,
    cancellation_token: CancellationToken,
    reconciliation_interval: Duration,
) {
    let mut redis = redis.into();
    let channel = RedisKeys::matchmaking_lobby_notification_channel(&lobby_code);
    let mut manager = (*pubsub_manager).clone();
    let mut last_sent_game_id = None;

    loop {
        let mut receiver = match manager.subscribe_to_channel(&channel).await {
            Ok(receiver) => receiver,
            Err(error) => {
                warn!(
                    lobby_code,
                    channel,
                    %error,
                    "Failed to subscribe to lobby match hints; retrying"
                );
                tokio::select! {
                    _ = cancellation_token.cancelled() => return,
                    _ = tokio::time::sleep(LOBBY_MATCH_SUBSCRIBE_RETRY_DELAY) => continue,
                }
            }
        };

        info!(lobby_code, channel, "Subscribed to lobby match hints");
        if !reconcile_lobby_match(
            &lobby_code,
            user_id,
            &mut redis,
            &ws_tx,
            &mut last_sent_game_id,
            false,
        )
        .await
        {
            return;
        }

        let mut reconciliation = tokio::time::interval(reconciliation_interval);
        reconciliation.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // The immediate durable read above already covers the interval's first tick.
        reconciliation.tick().await;

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => return,
                _ = reconciliation.tick() => {
                    if !reconcile_lobby_match(
                        &lobby_code,
                        user_id,
                        &mut redis,
                        &ws_tx,
                        &mut last_sent_game_id,
                        true,
                    ).await {
                        return;
                    }
                }
                hint = receiver.recv::<LobbyMatchHint>() => {
                    match hint {
                        Ok(LobbyMatchHint::MatchFound { game_id, partition_id }) => {
                            debug!(
                                lobby_code,
                                hinted_game_id = game_id,
                                hinted_partition_id = partition_id,
                                "Received lobby MatchFound hint; reconciling durable mapping"
                            );
                            if !reconcile_lobby_match(
                                &lobby_code,
                                user_id,
                                &mut redis,
                                &ws_tx,
                                &mut last_sent_game_id,
                                false,
                            ).await {
                                return;
                            }
                        }
                        Err(error) => {
                            warn!(
                                lobby_code,
                                channel,
                                %error,
                                "Lobby match hint receiver closed; resubscribing"
                            );
                            break;
                        }
                    }
                }
            }
        }

        tokio::select! {
            _ = cancellation_token.cancelled() => return,
            _ = tokio::time::sleep(LOBBY_MATCH_SUBSCRIBE_RETRY_DELAY) => {}
        }
    }
}

async fn subscribe_to_game_chat(
    game_id: u32,
    pubsub_manager: Arc<PubSubManager>,
    ws_tx: mpsc::Sender<Message>,
) -> Result<()> {
    info!("Subscribing to game {} chat", game_id);

    let channel = RedisKeys::game_chat_channel(game_id);
    let mut manager = (*pubsub_manager).clone();
    let mut receiver = manager
        .subscribe_to_channel(&channel)
        .await
        .context("Failed to subscribe to game chat channel")?;

    loop {
        let chat_payload: RedisGameChatPayload = match receiver.recv().await {
            Ok(payload) => payload,
            Err(e) => {
                warn!("Failed to receive game chat payload: {}", e);
                break;
            }
        };
        let chat_payload = chat_payload.into_filtered();

        let ws_message = WSMessage::GameChatMessage {
            game_id: chat_payload.game_id,
            message_id: chat_payload.message_id.clone(),
            user_id: chat_payload.user_id,
            username: chat_payload.username.clone(),
            message: chat_payload.message.clone(),
            timestamp_ms: chat_payload.timestamp_ms,
        };

        let json_msg = match serde_json::to_string(&ws_message) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to serialize game chat message: {}", e);
                continue;
            }
        };

        if ws_tx.send(Message::Text(json_msg.into())).await.is_err() {
            debug!(
                "WebSocket channel closed while forwarding game {} chat, stopping subscription",
                game_id
            );
            break;
        }
    }

    info!("Stopped subscribing to game {} chat", game_id);
    Ok(())
}

async fn subscribe_to_lobby_chat(
    lobby_code: String,
    pubsub_manager: Arc<PubSubManager>,
    ws_tx: mpsc::Sender<Message>,
) -> Result<()> {
    info!("Subscribing to lobby '{}' chat", lobby_code);

    let channel = RedisKeys::lobby_chat_channel(&lobby_code);
    let mut manager = (*pubsub_manager).clone();
    let mut receiver = manager
        .subscribe_to_channel(&channel)
        .await
        .context("Failed to subscribe to lobby chat channel")?;

    loop {
        let chat_payload: RedisLobbyChatPayload = match receiver.recv().await {
            Ok(payload) => payload,
            Err(e) => {
                warn!("Failed to receive lobby chat payload: {}", e);
                break;
            }
        };
        let chat_payload = chat_payload.into_filtered();

        let ws_message = WSMessage::LobbyChatMessage {
            lobby_code: chat_payload.lobby_code.clone(),
            message_id: chat_payload.message_id.clone(),
            user_id: chat_payload.user_id,
            username: chat_payload.username.clone(),
            message: chat_payload.message.clone(),
            timestamp_ms: chat_payload.timestamp_ms,
        };

        let json_msg = match serde_json::to_string(&ws_message) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to serialize lobby chat message: {}", e);
                continue;
            }
        };

        if ws_tx.send(Message::Text(json_msg.into())).await.is_err() {
            debug!(
                "WebSocket channel closed while forwarding lobby '{}' chat, stopping subscription",
                lobby_code
            );
            break;
        }
    }

    info!("Stopped subscribing to lobby '{}' chat", lobby_code);
    Ok(())
}

/// Shared admission path for both authentication shapes. A legacy `Token` has
/// no version and receives the same explicit update-required denial as any
/// other incompatible predictive client.
#[allow(clippy::too_many_arguments)]
async fn authenticate_ws_connection(
    jwt_token: String,
    protocol_version: Option<u16>,
    distribution: Option<ClientDistribution>,
    jwt_verifier: &Arc<dyn JwtVerifier>,
    db: &Arc<dyn Database>,
    ws_tx: &mpsc::Sender<Message>,
    game_bus: &Arc<GameBus>,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
    event_router: &Arc<crate::replication::GameEventRouter>,
    websocket_id: &str,
    lifecycle: &TaskLifecycle,
    socket_generation: u64,
    cluster_namespace: &ClusterNamespace,
    ads_config: &AdsConfig,
) -> Result<ConnectionState> {
    if let Err(error) = validate_client_protocol_version(protocol_version) {
        warn!(%error, "Rejecting incompatible gameplay client");
        let denial = WSMessage::AccessDenied {
            reason: error.to_string(),
        };
        ws_tx
            .send(Message::Text(serde_json::to_string(&denial)?.into()))
            .await
            .context("WebSocket closed before protocol mismatch denial")?;
        return Ok(ConnectionState::Unauthenticated);
    }
    debug!("Received WebSocket authentication request");
    match jwt_verifier.verify(&jwt_token).await {
        Ok(user_token) => {
            info!(
                "Token verified successfully, user_id: {}",
                user_token.user_id
            );

            let user = db
                .get_user_by_id(user_token.user_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("User not found"))?;

            if user.is_guest != user_token.is_guest {
                return Err(anyhow::anyhow!(
                    "Authentication failed: guest claim does not match user record"
                ));
            }

            let database_pool = if user.is_stress_test {
                MatchmakingPool::Stress
            } else {
                MatchmakingPool::Public
            };
            if database_pool != user_token.matchmaking_pool {
                return Err(anyhow::anyhow!(
                    "Authentication failed: matchmaking pool claim does not match user record"
                ));
            }

            // Distribution routing entered the protocol in v9. Do not let an
            // older shape accidentally inherit a provider based on a default
            // or on its account's authentication method.
            let distribution = protocol_version
                .is_some_and(|version| version >= 9)
                .then_some(distribution)
                .flatten();
            let client_ads_config = ads_config.client_config(distribution);
            let supports_ad_break = protocol_version.is_some_and(|version| version >= 9);

            let metadata = PlayerMetadata {
                user_id: user_token.user_id,
                username: user.username.clone(),
                token: jwt_token.clone(),
                is_guest: user.is_guest,
                matchmaking_pool: database_pool,
                supports_ad_break,
                can_show_video_ad: supports_ad_break
                    && client_ads_config.enabled
                    && client_ads_config.video.pre_match,
                distribution,
            };

            info!(
                "User authenticated: {} (id: {})",
                metadata.username, metadata.user_id
            );

            // Do not emit a new enum variant until the peer has advertised a
            // protocol that can decode it. This keeps legacy strict-Serde
            // clients compatible even when ads are globally disabled.
            if metadata.supports_ad_break {
                ws_tx
                    .send(Message::Text(
                        serde_json::to_string(&WSMessage::AdConfiguration(client_ads_config))?
                            .into(),
                    ))
                    .await
                    .context("WebSocket closed before advertisement configuration")?;
            }

            let authenticated = WSMessage::Authenticated {
                task_boot_id: lifecycle.task_boot_id().to_owned(),
                protocol_version: WS_PROTOCOL_VERSION,
                capabilities: lifecycle.protocol_capabilities(),
                socket_generation,
            };
            ws_tx
                .send(Message::Text(serde_json::to_string(&authenticated)?.into()))
                .await
                .context("WebSocket closed before authentication acknowledgement")?;
            if let Ok(user_id) = u32::try_from(metadata.user_id) {
                notify_durable_active_game_after_auth(
                    user_id,
                    metadata.matchmaking_pool,
                    ws_tx,
                    matchmaking_manager,
                    event_router,
                    game_bus,
                    cluster_namespace,
                    db,
                )
                .await?;
            }
            Ok(ConnectionState::Authenticated {
                metadata,
                lobby_handle: None,
                game_id: None,
                websocket_id: websocket_id.to_string(),
            })
        }
        Err(e) => {
            error!("Failed to verify token: {}", e);
            Err(anyhow::anyhow!("Authentication failed"))
        }
    }
}

/// Gate a lobby-wide mutation on leadership.
///
/// Returns `true` when the caller may proceed. Otherwise an `AccessDenied`
/// naming `action` has already been queued and the caller must do nothing —
/// the client mirrors this rule in its UI, so reaching a denial means either a
/// stale roster or a hand-crafted frame.
///
/// A failure to *determine* leadership denies the action. Matchmaking and
/// preferences are shared state for every member of the lobby, so an
/// unverifiable request is not safe to admit.
async fn authorize_lobby_leader(
    lobby_manager: &Arc<crate::lobby_manager::LobbyManager>,
    ws_tx: &mpsc::Sender<Message>,
    lobby_code: &str,
    user_id: i32,
    action: &str,
) -> Result<bool> {
    let is_host = match lobby_manager.is_lobby_host(lobby_code, user_id).await {
        Ok(is_host) => is_host,
        Err(error) => {
            warn!(
                lobby_code,
                user_id, "Failed to resolve lobby leadership: {error:#}"
            );
            false
        }
    };

    if is_host {
        return Ok(true);
    }

    let response = WSMessage::AccessDenied {
        reason: format!("Only the lobby leader can {action}"),
    };
    ws_tx
        .send(Message::Text(serde_json::to_string(&response)?.into()))
        .await?;
    Ok(false)
}

#[allow(clippy::too_many_arguments)]
async fn process_ws_message(
    state: ConnectionState,
    ws_message: WSMessage,
    jwt_verifier: &Arc<dyn JwtVerifier>,
    db: &Arc<dyn Database>,
    user_cache: UserCache,
    ws_tx: &mpsc::Sender<Message>,
    game_bus: &Arc<GameBus>,
    matchmaking_manager: &Arc<Mutex<MatchmakingManager>>,
    event_router: &Arc<crate::replication::GameEventRouter>,
    redis: &RedisConnection,
    _redis_url: &str,
    lobby_manager: &Arc<crate::lobby_manager::LobbyManager>,
    websocket_id: &str,
    region: &str,
    lifecycle: &TaskLifecycle,
    socket_generation: u64,
    cluster_namespace: &ClusterNamespace,
    cancellation_token: &CancellationToken,
    ads_config: &Arc<AdsConfig>,
    // Taken only so the session id can be attached to the connection: it is
    // minted inside this function and nowhere else.
    ws_analytics: &crate::analytics::ws_sink::WsConnection,
) -> Result<ConnectionState> {
    use tracing::debug;
    let state_str = match &state {
        ConnectionState::Unauthenticated => "Unauthenticated",
        ConnectionState::Authenticated {
            lobby_handle: Some(lobby_handle),
            game_id: Some(gid),
            ..
        } => {
            debug!(
                "Processing message in Authenticated(lobby:{}, game:{})",
                lobby_handle.lobby_code, gid
            );
            "Authenticated(InLobby+InGame)"
        }
        ConnectionState::Authenticated {
            lobby_handle: Some(lobby_handle),
            game_id: None,
            ..
        } => {
            debug!(
                "Processing message in Authenticated(lobby:{})",
                lobby_handle.lobby_code
            );
            "Authenticated(InLobby)"
        }
        ConnectionState::Authenticated {
            lobby_handle: None,
            game_id: Some(gid),
            ..
        } => {
            debug!("Processing message in Authenticated(game:{})", gid);
            "Authenticated(InGame)"
        }
        ConnectionState::Authenticated { .. } => "Authenticated",
    };
    match &ws_message {
        WSMessage::Chat(_)
        | WSMessage::LobbyChatMessage { .. }
        | WSMessage::GameChatMessage { .. }
        | WSMessage::LobbyChatHistory { .. }
        | WSMessage::GameChatHistory { .. } => {
            debug!("Processing chat-bearing message: <redacted> in state: {state_str}")
        }
        _ => debug!(
            "Processing message: {:?} in state: {}",
            ws_message, state_str
        ),
    }

    match state {
        ConnectionState::Unauthenticated => {
            match ws_message {
                WSMessage::Token(jwt_token) => {
                    authenticate_ws_connection(
                        jwt_token,
                        None,
                        None,
                        jwt_verifier,
                        db,
                        ws_tx,
                        game_bus,
                        matchmaking_manager,
                        event_router,
                        websocket_id,
                        lifecycle,
                        socket_generation,
                        cluster_namespace,
                        ads_config,
                    )
                    .await
                }
                WSMessage::Authenticate {
                    token: jwt_token,
                    protocol_version,
                    anon_id,
                    distribution,
                } => {
                    // Validated at the boundary so a malformed or unbounded
                    // client string can never reach an analytics event.
                    let anon_id = sanitize_anon_id(anon_id);
                    // Minted here because this is the first identity-bearing
                    // moment of a connection: the socket exists earlier, but
                    // nothing is known about who is on it until now.
                    let session_id = new_session_id();
                    debug!(
                        "websocket session {session_id} authenticated (anon_id present: {})",
                        anon_id.is_some()
                    );
                    crate::analytics::sink::record_session_started(
                        &session_id,
                        anon_id.as_deref(),
                        protocol_version,
                    );
                    // From here on this connection's frames carry the same
                    // session id as the event above, so the two join. The
                    // handshake frames that preceded this one carry none,
                    // because until now there was no session to name.
                    ws_analytics.bind_session(&session_id);
                    authenticate_ws_connection(
                        jwt_token,
                        Some(protocol_version),
                        distribution,
                        jwt_verifier,
                        db,
                        ws_tx,
                        game_bus,
                        matchmaking_manager,
                        event_router,
                        websocket_id,
                        lifecycle,
                        socket_generation,
                        cluster_namespace,
                        ads_config,
                    )
                    .await
                }
                WSMessage::Ping { client_time } => {
                    // Respond with Pong even in unauthenticated state to keep connection alive
                    let server_time = chrono::Utc::now().timestamp_millis();
                    let pong_msg = Message::Text(
                        serde_json::to_string(&WSMessage::Pong {
                            client_time,
                            server_time,
                        })?
                        .into(),
                    );
                    ws_tx.send(pong_msg).await?;
                    Ok(ConnectionState::Unauthenticated)
                }
                _ => {
                    warn!("Cannot process message in unauthenticated state");
                    Ok(ConnectionState::Unauthenticated)
                }
            }
        }
        ConnectionState::Authenticated {
            metadata,
            lobby_handle: lobby,
            game_id,
            websocket_id,
        } => {
            // Replacement transports and failed exact-presence heartbeats
            // cancel the lobby scope. A retained handle is not authorization:
            // normalize it before any inbound lobby mutation or chat action.
            let mut lobby = lobby;
            if lobby
                .as_ref()
                .is_some_and(|handle| handle.scope_cancellation_token().is_cancelled())
                && let Some(handle) = lobby.take()
            {
                handle.detach_transport();
            }

            match ws_message {
                WSMessage::SetRematchIntent {
                    game_id: requested_game_id,
                    opt_in,
                } => {
                    // Authorized off the connection, not the payload: this
                    // socket only holds a game id because `JoinGame` already
                    // authorized it against the match roster.
                    if game_id == Some(requested_game_id)
                        && let Ok(user_id) = u32::try_from(metadata.user_id)
                    {
                        let rematch = RematchStore::new(redis.clone());
                        if let Err(error) =
                            rematch.set_intent(requested_game_id, user_id, opt_in).await
                        {
                            warn!(user_id, requested_game_id, %error, "failed to record a rematch intent");
                        }
                        let lobby = match advance_rematch(
                            requested_game_id,
                            user_id,
                            &rematch,
                            event_router,
                            db,
                            lobby_manager,
                            &metadata,
                            lobby,
                            region,
                            &websocket_id,
                            ws_tx,
                        )
                        .await
                        {
                            Ok(lobby) => lobby,
                            Err((lobby, error)) => {
                                warn!(user_id, requested_game_id, %error, "failed to advance the rematch");
                                lobby
                            }
                        };
                        return Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: lobby,
                            game_id,
                            websocket_id,
                        });
                    }
                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }

                WSMessage::ChallengePlayer {
                    user_id: target_user_id,
                } => {
                    let Ok(challenger_id) = u32::try_from(metadata.user_id) else {
                        return Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: lobby,
                            game_id,
                            websocket_id,
                        });
                    };
                    // A challenge is an invitation to the challenger's lobby,
                    // so there has to be one. Creating it here rather than
                    // asking the client to sequence CreateLobby first keeps
                    // the whole exchange a single round trip.
                    let (lobby, outcome) = issue_challenge(
                        challenger_id,
                        &metadata,
                        target_user_id,
                        lobby,
                        lobby_manager,
                        user_cache.clone(),
                        redis,
                        region,
                        websocket_id.as_str(),
                        ws_tx,
                    )
                    .await?;
                    if let Some(lobby_code) = outcome {
                        let created = WSMessage::LobbyCreated { lobby_code };
                        ws_tx
                            .send(Message::Text(serde_json::to_string(&created)?.into()))
                            .await?;
                    }
                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }

                WSMessage::RespondToChallenge {
                    challenge_id,
                    accept,
                } => {
                    if let Ok(user_id) = u32::try_from(metadata.user_id) {
                        let store = ChallengeStore::new(redis.clone());
                        let state = if accept {
                            ChallengeState::Accepted
                        } else {
                            ChallengeState::Declined
                        };
                        match store.resolve(user_id, &challenge_id, state).await {
                            Ok(Some(challenge)) => {
                                if accept {
                                    let accepted = WSMessage::ChallengeAccepted {
                                        challenge_id: challenge.challenge_id.clone(),
                                        lobby_code: challenge.lobby_code.clone(),
                                    };
                                    ws_tx
                                        .send(Message::Text(
                                            serde_json::to_string(&accepted)?.into(),
                                        ))
                                        .await?;
                                }
                            }
                            Ok(None) => {
                                send_challenge_failure(
                                    "That challenge is no longer available.",
                                    ws_tx,
                                )
                                .await?;
                            }
                            Err(error) => {
                                warn!(user_id, %error, "failed to resolve a challenge");
                                send_challenge_failure(
                                    "Could not answer that challenge. Try again.",
                                    ws_tx,
                                )
                                .await?;
                            }
                        }
                        // Whatever happened, the answerer's own view is
                        // refreshed from the durable record rather than guessed
                        // at from the outcome.
                        let _ = send_challenge_inbox(user_id, &store, ws_tx).await;
                    }
                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }

                WSMessage::CancelChallenge { challenge_id } => {
                    if let Ok(user_id) = u32::try_from(metadata.user_id) {
                        let store = ChallengeStore::new(redis.clone());
                        if let Err(error) = store
                            .resolve(user_id, &challenge_id, ChallengeState::Cancelled)
                            .await
                        {
                            warn!(user_id, %error, "failed to cancel a challenge");
                        }
                        let _ = send_challenge_inbox(user_id, &store, ws_tx).await;
                    }
                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }

                WSMessage::UpdateNickname { nickname } => {
                    let mut metadata = metadata;
                    if let Err(e) = handle_guest_nickname_update(
                        db,
                        lobby_manager,
                        user_cache.clone(),
                        &lobby,
                        &mut metadata,
                        ws_tx,
                        nickname,
                    )
                    .await
                    {
                        error!(
                            "Failed to update guest nickname for user {}: {}",
                            metadata.user_id, e
                        );
                    }
                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }
                WSMessage::UpdateLobbyPreferences {
                    selected_modes,
                    competitive,
                } => {
                    {
                        if let Some(ref lobby_handle) = lobby {
                            // Preferences are lobby-wide: one member's choice
                            // decides what every member queues for.
                            if authorize_lobby_leader(
                                lobby_manager,
                                ws_tx,
                                &lobby_handle.lobby_code,
                                metadata.user_id,
                                "change the game mode",
                            )
                            .await?
                            {
                                lobby_manager
                                    .set_lobby_preferences(
                                        &lobby_handle.lobby_code,
                                        &lobby_manager::LobbyPreferences {
                                            selected_modes,
                                            competitive,
                                        },
                                    )
                                    .await?;
                            }
                        }
                    }
                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }
                WSMessage::QueueForMatch {
                    game_type,
                    queue_mode,
                } => {
                    info!(
                        "User {} ({}) queuing for match type: {:?}, mode: {:?}",
                        metadata.username, metadata.user_id, game_type, queue_mode
                    );

                    if let Some(ref lobby_handle) = lobby {
                        if !authorize_lobby_leader(
                            lobby_manager,
                            ws_tx,
                            &lobby_handle.lobby_code,
                            metadata.user_id,
                            "start matchmaking",
                        )
                        .await?
                        {
                            return Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: lobby,
                                game_id,
                                websocket_id,
                            });
                        }

                        if let Err(e) = queue_lobby_or_begin_ad_break(
                            &lobby_handle.lobby_code,
                            std::slice::from_ref(&game_type),
                            &queue_mode,
                            db,
                            lobby_manager,
                            matchmaking_manager,
                            metadata.user_id as u32,
                            metadata.matchmaking_pool,
                            ads_config,
                        )
                        .await
                        {
                            error!(
                                "Failed to queue existing lobby {}: {}",
                                lobby_handle.lobby_code, e
                            );
                            let response = WSMessage::AccessDenied {
                                reason: format!("Failed to queue lobby: {}", e),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                        } else {
                            info!(
                                "Queued existing lobby {} for game type {:?}",
                                lobby_handle.lobby_code, game_type
                            );
                        }

                        return Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: lobby,
                            game_id,
                            websocket_id,
                        });
                    }

                    let response = WSMessage::AccessDenied {
                        reason: "Join a lobby before queueing for matchmaking".to_string(),
                    };
                    let json_msg = serde_json::to_string(&response)?;
                    ws_tx.send(Message::Text(json_msg.into())).await?;
                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }
                WSMessage::QueueForMatchMulti {
                    game_types,
                    queue_mode,
                } => {
                    info!(
                        "User {} ({}) queuing for multiple match types: {:?}, mode: {:?}",
                        metadata.username, metadata.user_id, game_types, queue_mode
                    );

                    if let Some(ref lobby_handle) = lobby {
                        if !authorize_lobby_leader(
                            lobby_manager,
                            ws_tx,
                            &lobby_handle.lobby_code,
                            metadata.user_id,
                            "start matchmaking",
                        )
                        .await?
                        {
                            return Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: lobby,
                                game_id,
                                websocket_id,
                            });
                        }

                        if let Err(e) = queue_lobby_or_begin_ad_break(
                            &lobby_handle.lobby_code,
                            &game_types,
                            &queue_mode,
                            db,
                            lobby_manager,
                            matchmaking_manager,
                            metadata.user_id as u32,
                            metadata.matchmaking_pool,
                            ads_config,
                        )
                        .await
                        {
                            error!(
                                "Failed to queue existing lobby {} for multiple types: {}",
                                lobby_handle.lobby_code, e
                            );
                            let response = WSMessage::AccessDenied {
                                reason: format!("Failed to queue lobby: {}", e),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                        } else {
                            info!(
                                "Queued existing lobby {} for multiple game types {:?}",
                                lobby_handle.lobby_code, game_types
                            );
                        }

                        return Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: lobby,
                            game_id,
                            websocket_id,
                        });
                    }

                    let response = WSMessage::AccessDenied {
                        reason: "Join a lobby before queueing for matchmaking".to_string(),
                    };
                    let json_msg = serde_json::to_string(&response)?;
                    ws_tx.send(Message::Text(json_msg.into())).await?;
                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }
                WSMessage::AdBreakResolved {
                    break_id,
                    resolution,
                } => {
                    if let (Some(lobby_handle), Ok(user_id)) =
                        (lobby.as_ref(), u32::try_from(metadata.user_id))
                    {
                        match lobby_manager
                            .resolve_ad_break(
                                &lobby_handle.lobby_code,
                                &break_id,
                                user_id,
                                resolution,
                            )
                            .await
                        {
                            Ok(AdBreakResolutionResult::Ready(ad_break)) => {
                                if let Err(error) = finalize_lobby_ad_break(
                                    &ad_break,
                                    &lobby_handle.lobby_code,
                                    db,
                                    lobby_manager,
                                    matchmaking_manager,
                                )
                                .await
                                {
                                    warn!(
                                        lobby_code = lobby_handle.lobby_code,
                                        break_id,
                                        %error,
                                        "Failed to finalize resolved lobby ad break"
                                    );
                                }
                            }
                            Ok(AdBreakResolutionResult::Pending(ad_break)) => {
                                debug!(
                                    lobby_code = lobby_handle.lobby_code,
                                    break_id,
                                    resolved = ad_break.resolutions.len(),
                                    participants = ad_break.participant_user_ids.len(),
                                    "Lobby ad break is still waiting for participants"
                                );
                            }
                            Ok(AdBreakResolutionResult::NotDue(_)) => {
                                // Only the internal timeout sentinel can
                                // produce this result; client ACKs never do.
                            }
                            Ok(AdBreakResolutionResult::NoChange(_)) => {
                                debug!(
                                    lobby_code = lobby_handle.lobby_code,
                                    break_id, "Ignored duplicate lobby ad-break resolution"
                                );
                            }
                            Ok(AdBreakResolutionResult::Stale) => {
                                debug!(
                                    lobby_code = lobby_handle.lobby_code,
                                    break_id,
                                    user_id,
                                    "Ignored stale or non-participant ad-break resolution"
                                );
                            }
                            Err(error) => {
                                warn!(
                                    lobby_code = lobby_handle.lobby_code,
                                    break_id,
                                    user_id,
                                    %error,
                                    "Failed to record lobby ad-break resolution"
                                );
                            }
                        }
                    } else {
                        debug!(
                            break_id,
                            user_id = metadata.user_id,
                            "Ignored ad-break resolution outside a valid lobby session"
                        );
                    }

                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }
                WSMessage::JoinGame(requested_game_id) => {
                    info!(
                        "User {} ({}) joining game {}",
                        metadata.username, metadata.user_id, requested_game_id
                    );

                    let user_id = match u32::try_from(metadata.user_id) {
                        Ok(user_id) => user_id,
                        Err(_) => {
                            send_game_load_failed(
                                ws_tx,
                                requested_game_id,
                                "This game is unavailable",
                            )
                            .await;
                            return Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: lobby,
                                game_id: None,
                                websocket_id,
                            });
                        }
                    };

                    if let Err(failure) = authorize_game_join(
                        requested_game_id,
                        user_id,
                        metadata.matchmaking_pool,
                        matchmaking_manager,
                        event_router,
                        game_bus,
                        cluster_namespace,
                        db,
                    )
                    .await
                    {
                        let response = game_join_failure_message(requested_game_id, failure);
                        ws_tx
                            .send(Message::Text(serde_json::to_string(&response)?.into()))
                            .await?;
                        return Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: lobby,
                            game_id: None,
                            websocket_id,
                        });
                    }

                    if let Some(lobby_handle) = lobby.as_ref() {
                        match acknowledge_lobby_match_handoff(
                            redis,
                            &lobby_handle.lobby_code,
                            user_id,
                            requested_game_id,
                        )
                        .await
                        {
                            Ok(true) => debug!(
                                lobby_code = lobby_handle.lobby_code,
                                user_id,
                                game_id = requested_game_id,
                                "Acknowledged lobby match handoff"
                            ),
                            Ok(false) => {}
                            Err(error) => warn!(
                                lobby_code = lobby_handle.lobby_code,
                                user_id,
                                game_id = requested_game_id,
                                %error,
                                "Authorized game join but could not acknowledge its lobby handoff"
                            ),
                        }
                    }

                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id: Some(requested_game_id),
                        websocket_id,
                    })
                }
                WSMessage::LeaveGame => {
                    if let Some(current_game_id) = game_id {
                        info!(
                            "User {} ({}) leaving game {}",
                            metadata.username, metadata.user_id, current_game_id
                        );
                    } else {
                        debug!(
                            "Received LeaveGame from user {} ({}) but no active game was set",
                            metadata.username, metadata.user_id
                        );
                    }

                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id: None,
                        websocket_id,
                    })
                }
                WSMessage::LeaveQueue => {
                    info!(
                        "User {} ({}) leaving matchmaking queue",
                        metadata.username, metadata.user_id
                    );

                    let authenticated_user_id =
                        u32::try_from(metadata.user_id).context("User ID must be non-negative")?;

                    // Queue admission is lobby-authoritative. Remove only the
                    // exact currently admitted lobby identity; there is no
                    // secondary per-player queue to reconcile. If a gateway
                    // restart lost the in-memory handle, recover authority
                    // from this authenticated user's exact durable claim.
                    let in_memory_lobby_code =
                        lobby.as_ref().map(|handle| handle.lobby_code.clone());

                    // Cancelling drops the whole lobby out of the queue, so it
                    // belongs to whoever was allowed to start it. Without the
                    // handle there is no lobby to check leadership against, and
                    // the durable-claim path below only ever reaches this
                    // user's own admission, so that case stays permitted.
                    if let Some(ref lobby_code) = in_memory_lobby_code
                        && !authorize_lobby_leader(
                            lobby_manager,
                            ws_tx,
                            lobby_code,
                            metadata.user_id,
                            "cancel matchmaking",
                        )
                        .await?
                    {
                        return Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: lobby,
                            game_id,
                            websocket_id,
                        });
                    }
                    let mut matchmaking_manager = matchmaking_manager.lock().await;
                    let mut removal_result = Ok(None);
                    for attempt in 0..2 {
                        removal_result = match in_memory_lobby_code.as_deref() {
                            Some(lobby_code) => match matchmaking_manager
                                .remove_lobby_from_all_queues_by_code_for_user(
                                    lobby_code,
                                    authenticated_user_id,
                                )
                                .await
                            {
                                Ok(true) => Ok(Some(lobby_code.to_owned())),
                                Ok(false) => {
                                    matchmaking_manager
                                        .remove_lobby_from_queue_for_user_claim(
                                            authenticated_user_id,
                                        )
                                        .await
                                }
                                Err(error) => Err(error),
                            },
                            None => {
                                matchmaking_manager
                                    .remove_lobby_from_queue_for_user_claim(authenticated_user_id)
                                    .await
                            }
                        };
                        if removal_result.is_ok() {
                            break;
                        }
                        if attempt == 0 {
                            warn!("Retrying ambiguous lobby matchmaking cancellation");
                        }
                    }
                    match removal_result {
                        Ok(Some(lobby_code)) => {
                            // Only this arm means the lobby was genuinely in a
                            // queue and left it; the Ok(None) arm below is a
                            // cancel with nothing to cancel.
                            crate::analytics::sink::record_queue_left(
                                &lobby_code,
                                authenticated_user_id as i32,
                            );
                            info!(
                                lobby_code,
                                "Removed lobby from matchmaking queues after cancel"
                            );
                            if let Err(error) =
                                lobby_manager.publish_lobby_update(&lobby_code).await
                            {
                                warn!(
                                    lobby_code,
                                    %error,
                                    "Failed to publish reconciled lobby state after cancel"
                                );
                            }
                        }
                        Ok(None) => {
                            info!("No authorized lobby queue identity was present on cancel");
                        }
                        Err(error) => {
                            error!(
                                %error,
                                "Failed to remove lobby from matchmaking queues on cancel"
                            );
                            let response = WSMessage::AccessDenied {
                                reason: format!(
                                    "Could not confirm queue cancellation; retry: {error}"
                                ),
                            };
                            ws_tx
                                .send(Message::Text(serde_json::to_string(&response)?.into()))
                                .await?;
                        }
                    }

                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }
                WSMessage::Ping { client_time } => {
                    // Respond with Pong including server time for clock synchronization
                    let server_time = chrono::Utc::now().timestamp_millis();
                    let response = WSMessage::Pong {
                        client_time,
                        server_time,
                    };
                    let json_msg = serde_json::to_string(&response)?;
                    ws_tx.send(Message::Text(json_msg.into())).await?;
                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }
                WSMessage::GameEvent(event_msg) => {
                    // Forward game events to the client
                    warn!(
                        "Received game event in authenticated state: {:?}",
                        event_msg
                    );
                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }

                WSMessage::CreateLobby => {
                    info!(
                        "User {} ({}) creating lobby in region {}",
                        metadata.username, metadata.user_id, region
                    );

                    match lobby_manager
                        .create_lobby_for_pool(metadata.user_id, region, metadata.matchmaking_pool)
                        .await
                    {
                        Ok(lobby) => {
                            // Join the lobby
                            let lobby_handle = match lobby_manager
                                .join_lobby_for_pool(
                                    Some(lobby.lobby_code()),
                                    metadata.user_id,
                                    metadata.username.clone(),
                                    websocket_id.to_string(),
                                    region.to_string(),
                                    None,
                                    metadata.matchmaking_pool,
                                    metadata.distribution,
                                    metadata.supports_ad_break,
                                    metadata.can_show_video_ad,
                                )
                                .await
                            {
                                Ok(handle) => handle,
                                Err(e) => {
                                    error!("Failed to join newly created lobby: {}", e);
                                    let response = WSMessage::AccessDenied {
                                        reason: format!("Failed to join lobby: {}", e),
                                    };
                                    let json_msg = serde_json::to_string(&response)?;
                                    ws_tx.send(Message::Text(json_msg.into())).await?;
                                    return Ok(ConnectionState::Authenticated {
                                        metadata,
                                        lobby_handle: None,
                                        game_id,
                                        websocket_id,
                                    });
                                }
                            };

                            // Send success response
                            let response = WSMessage::LobbyCreated {
                                lobby_code: lobby_handle.lobby_code.clone(),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;

                            // Transition to InLobby state
                            Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: Some(lobby_handle),
                                game_id: None,
                                websocket_id: websocket_id.to_string(),
                            })
                        }
                        Err(e) => {
                            error!("Failed to create lobby: {}", e);
                            let response = WSMessage::AccessDenied {
                                reason: format!("Failed to create lobby: {}", e),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: lobby,
                                game_id,
                                websocket_id,
                            })
                        }
                    }
                }
                WSMessage::JoinLobby {
                    lobby_code,
                    preferences,
                } => {
                    info!(
                        "User {} ({}) joining lobby with code: {}",
                        metadata.username, metadata.user_id, lobby_code
                    );

                    let lobby_metadata = match lobby_manager.get_lobby_metadata(&lobby_code).await {
                        Ok(meta) => meta,
                        Err(e) => {
                            error!("Failed to get lobby by code: {}", e);
                            let response = WSMessage::AccessDenied {
                                reason: format!("Failed to find lobby: {}", e),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            return Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: None,
                                game_id,
                                websocket_id,
                            });
                        }
                    };

                    if let Some(lobby_metadata) = &lobby_metadata {
                        if lobby_metadata.matchmaking_pool != metadata.matchmaking_pool {
                            let response = WSMessage::AccessDenied {
                                reason: "Lobby belongs to a different matchmaking pool".to_string(),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            return Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: None,
                                game_id,
                                websocket_id,
                            });
                        }

                        if lobby_metadata.region != region {
                            warn!(
                                "Lobby '{}' is in region {}, user is in region {}",
                                lobby_code, lobby_metadata.region, region
                            );

                            // Get WebSocket URL for the target region from database
                            let ws_url = match db.get_region_ws_url(&lobby_metadata.region).await? {
                                Some(url) => url,
                                None => {
                                    let response = WSMessage::AccessDenied {
                                        reason: format!(
                                            "No servers available in region {}",
                                            lobby_metadata.region
                                        ),
                                    };
                                    let json_msg = serde_json::to_string(&response)?;
                                    ws_tx.send(Message::Text(json_msg.into())).await?;
                                    return Ok(ConnectionState::Authenticated {
                                        metadata,
                                        lobby_handle: None,
                                        game_id,
                                        websocket_id,
                                    });
                                }
                            };

                            let response = WSMessage::LobbyRegionMismatch {
                                target_region: lobby_metadata.region.clone(),
                                ws_url,
                                lobby_code: lobby_code.clone(),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            return Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: None,
                                game_id,
                                websocket_id,
                            });
                        }
                    } else {
                        info!(
                            "Lobby '{}' missing; auto-creating default lobby for user {}",
                            lobby_code, metadata.user_id
                        );
                    }

                    // Join (and auto-create if needed) the lobby
                    let lobby_handle = match lobby_manager
                        .join_lobby_for_pool(
                            Some(&lobby_code),
                            metadata.user_id,
                            metadata.username.clone(),
                            websocket_id.to_string(),
                            region.to_string(),
                            preferences,
                            metadata.matchmaking_pool,
                            metadata.distribution,
                            metadata.supports_ad_break,
                            metadata.can_show_video_ad,
                        )
                        .await
                    {
                        Ok(handle) => handle,
                        Err(e) => {
                            let err_text = e.to_string();
                            error!("Failed to join lobby '{}': {}", lobby_code, err_text);
                            let response = WSMessage::AccessDenied {
                                reason: format!("Failed to join lobby: {}", err_text),
                            };
                            let json_msg = serde_json::to_string(&response)?;
                            ws_tx.send(Message::Text(json_msg.into())).await?;
                            return Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: None,
                                game_id,
                                websocket_id,
                            });
                        }
                    };

                    // Send success response
                    let response = WSMessage::JoinedLobby {
                        lobby_code: lobby_handle.lobby_code.clone(),
                    };
                    let json_msg = serde_json::to_string(&response)?;
                    ws_tx.send(Message::Text(json_msg.into())).await?;

                    // Transition to InLobby state
                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: Some(lobby_handle),
                        game_id: None,
                        websocket_id: websocket_id.to_string(),
                    })
                }
                WSMessage::LeaveLobby => {
                    if let Some(mut lobby_handle) = lobby {
                        let lobby_code = lobby_handle.lobby_code.clone();
                        match lobby_handle.close().await {
                            Ok(_) => {
                                let response = WSMessage::LeftLobby;
                                let json_msg = serde_json::to_string(&response)?;
                                ws_tx.send(Message::Text(json_msg.into())).await?;
                                Ok(ConnectionState::Authenticated {
                                    metadata,
                                    lobby_handle: None,
                                    game_id,
                                    websocket_id,
                                })
                            }
                            Err(e) => {
                                error!(
                                    "Failed to leave lobby {} for user {}: {}",
                                    lobby_code, metadata.user_id, e
                                );
                                let response = WSMessage::AccessDenied {
                                    reason: format!("Failed to leave lobby: {}", e),
                                };
                                let json_msg = serde_json::to_string(&response)?;
                                ws_tx.send(Message::Text(json_msg.into())).await?;
                                Ok(ConnectionState::Authenticated {
                                    metadata,
                                    lobby_handle: Some(lobby_handle),
                                    game_id,
                                    websocket_id,
                                })
                            }
                        }
                    } else {
                        let response = WSMessage::AccessDenied {
                            reason: "You are not currently in a lobby".to_string(),
                        };
                        let json_msg = serde_json::to_string(&response)?;
                        ws_tx.send(Message::Text(json_msg.into())).await?;
                        Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: None,
                            game_id,
                            websocket_id,
                        })
                    }
                }
                WSMessage::Chat(message) => {
                    let trimmed = message.trim();
                    if trimmed.is_empty() {
                        return Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: lobby,
                            game_id,
                            websocket_id,
                        });
                    }

                    if trimmed.chars().count() > MAX_CHAT_MESSAGE_LENGTH {
                        let response = WSMessage::AccessDenied {
                            reason: format!(
                                "Chat messages must be {} characters or fewer",
                                MAX_CHAT_MESSAGE_LENGTH
                            ),
                        };
                        let json_msg = serde_json::to_string(&response)?;
                        ws_tx.send(Message::Text(json_msg.into())).await?;
                        return Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: lobby,
                            game_id,
                            websocket_id,
                        });
                    }

                    let filtered_message = filter_chat_message(trimmed);
                    if filtered_message.trim().is_empty() {
                        return Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: lobby,
                            game_id,
                            websocket_id,
                        });
                    }

                    let mut publish_error = false;
                    if let Some(current_game_id) = game_id {
                        let payload = GameChatBroadcast {
                            game_id: current_game_id,
                            message_id: uuid::Uuid::new_v4().to_string(),
                            user_id: metadata.user_id,
                            username: metadata.username.clone(),
                            message: filtered_message,
                            timestamp_ms: Utc::now().timestamp_millis(),
                        };

                        if let Err(e) = publish_game_chat_message(redis.clone(), payload).await {
                            error!(
                                "Failed to publish game {} chat message for user {}: {}",
                                current_game_id, metadata.user_id, e
                            );
                            publish_error = true;
                        }
                    } else if let Some(ref lobby_handle) = lobby {
                        let payload = LobbyChatBroadcast {
                            lobby_code: lobby_handle.lobby_code.clone(),
                            message_id: uuid::Uuid::new_v4().to_string(),
                            user_id: metadata.user_id,
                            username: metadata.username.clone(),
                            message: filtered_message,
                            timestamp_ms: Utc::now().timestamp_millis(),
                        };

                        if let Err(e) = publish_lobby_chat_message(redis.clone(), payload).await {
                            error!(
                                "Failed to publish lobby '{}' chat message for user {}: {}",
                                lobby_handle.lobby_code, metadata.user_id, e
                            );
                            publish_error = true;
                        }
                    } else {
                        let response = WSMessage::AccessDenied {
                            reason: "Chat is only available in a lobby or game".to_string(),
                        };
                        let json_msg = serde_json::to_string(&response)?;
                        ws_tx.send(Message::Text(json_msg.into())).await?;
                        return Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: lobby,
                            game_id,
                            websocket_id,
                        });
                    }

                    if publish_error {
                        let response = WSMessage::AccessDenied {
                            reason: "Failed to send chat message".to_string(),
                        };
                        let json_msg = serde_json::to_string(&response)?;
                        ws_tx.send(Message::Text(json_msg.into())).await?;
                    }

                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }
                WSMessage::PlayerReady {
                    game_id: claimed_game_id,
                } => {
                    // `game_id: Some(g)` on this connection means
                    // `authorize_game_join` already proved this user belongs to
                    // game g. The claimed id is logged and discarded; whether
                    // the user is a *player* rather than a spectator is decided
                    // by the executor, which holds the authoritative state.
                    if let Some(game_id) = game_id {
                        let user_id = metadata.user_id as u32;
                        if claimed_game_id != game_id {
                            warn!(
                                claimed_game_id,
                                authenticated_game_id = game_id,
                                user_id,
                                "Discarding untrusted game id on a readiness confirmation"
                            );
                        }
                        // Runs detached: the game-existence wait below must not
                        // stall this connection's message loop, and a dropped
                        // confirmation costs the player nothing worse than
                        // waiting out the readiness deadline.
                        tokio::spawn(publish_player_ready_after_game_exists(
                            game_bus.clone(),
                            cluster_namespace.clone(),
                            game_id,
                            user_id,
                            cancellation_token.clone(),
                        ));
                    } else {
                        warn!(
                            user_id = metadata.user_id,
                            claimed_game_id, "Readiness confirmation outside a joined game"
                        );
                    }

                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }
                WSMessage::GameCommandV2 {
                    command_id,
                    command,
                } => {
                    if let Some(game_id) = game_id {
                        let user_id = metadata.user_id as u32;
                        if command_id.game_id != game_id || command_id.user_id != user_id {
                            warn!(
                                claimed_game_id = command_id.game_id,
                                claimed_user_id = command_id.user_id,
                                authenticated_game_id = game_id,
                                authenticated_user_id = user_id,
                                "Canonicalizing untrusted v2 command identity"
                            );
                        }
                        let command_id = canonical_command_identity(command_id, game_id, user_id);
                        if let Err(error) = validate_client_command_identity(&command_id) {
                            warn!(game_id, user_id, %error, "Rejecting invalid v2 command identity");
                            let response = WSMessage::AccessDenied {
                                reason: "Invalid game command identity".to_owned(),
                            };
                            ws_tx
                                .send(Message::Text(serde_json::to_string(&response)?.into()))
                                .await?;
                            return Ok(ConnectionState::Authenticated {
                                metadata,
                                lobby_handle: lobby,
                                game_id: Some(game_id),
                                websocket_id,
                            });
                        }
                        let partition_id = game_id % PARTITION_COUNT;
                        let command_sequence = command_id.sequence;
                        let event = StreamEvent::GameCommandSubmittedV2 {
                            game_id,
                            user_id,
                            command_id,
                            command,
                        };
                        let publish_started = tokio::time::Instant::now();
                        let publish_result = game_bus
                            .publish_game_command_unless_completed(
                                cluster_namespace,
                                partition_id,
                                game_id,
                                &event,
                            )
                            .await;
                        let publish_wait = publish_started.elapsed();
                        if let Some(publish_wait_ms) = slow_command_publish_wait_ms(publish_wait) {
                            warn!(
                                game_id,
                                user_id,
                                partition_id,
                                command_sequence,
                                gateway_task_boot_id = lifecycle.task_boot_id(),
                                socket_generation,
                                publish_wait_ms,
                                publish_succeeded = publish_result.is_ok(),
                                "Slow v2 game command publication"
                            );
                        }
                        let published = require_game_command_publication(publish_result).map_err(
                            |error| {
                                error!(
                                    game_id,
                                    user_id,
                                    %error,
                                    "Failed to publish v2 game command; closing socket for ordered retry"
                                );
                                error
                            },
                        )?;
                        if !published {
                            debug!(
                                game_id,
                                user_id, "Discarded command after immutable game completion"
                            );
                        }
                        Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: lobby,
                            game_id: Some(game_id),
                            websocket_id,
                        })
                    } else {
                        warn!(
                            user_id = metadata.user_id,
                            "Ignoring v2 game command from a connection with no active game"
                        );
                        Ok(ConnectionState::Authenticated {
                            metadata,
                            lobby_handle: lobby,
                            game_id,
                            websocket_id,
                        })
                    }
                }
                _ => {
                    warn!(
                        "Unexpected message in authenticated state: {:?}",
                        ws_message
                    );
                    Ok(ConnectionState::Authenticated {
                        metadata,
                        lobby_handle: lobby,
                        game_id,
                        websocket_id,
                    })
                }
            }
        }
    }
}

pub async fn register_server(
    db: &Arc<dyn Database>,
    grpc_address: &str,
    region: &str,
    origin: &str,
    ws_url: &str,
) -> Result<u64> {
    info!("Registering server instance");

    // Insert a new record and return the generated ID
    let id = db
        .register_server(grpc_address, region, origin, ws_url)
        .await
        .context("Failed to register server in database")?;

    let id_u64 = id as u64;
    info!(id = id_u64, "Server registered with ID: {}", id_u64);
    Ok(id_u64)
}

/// Subscribe to user count updates from Redis and forward to WebSocket client
/// Everything one authenticated socket needs to take part in the social layer.
///
/// Held for the life of the connection. Dropping it aborts the background
/// tasks; `close` additionally gives up the presence lease and withdraws the
/// user's pending challenges, so nobody is left waiting on an answer that can
/// no longer arrive.
struct SocialSession {
    user_id: u32,
    /// Which connection holds this user's presence lease. A make-before-break
    /// replacement claims it before this one tears down, and cleanup must not
    /// undo the newer socket's work.
    websocket_id: String,
    presence: PresenceRegistry,
    /// What the lease should currently assert. Shared with the refresh loop so
    /// a heartbeat re-states the player's *current* name and activity rather
    /// than the ones captured when the connection authenticated.
    intent: Arc<std::sync::Mutex<PresenceIntent>>,
    tasks: Vec<JoinHandle<()>>,
}

#[derive(Clone)]
struct PresenceIntent {
    username: String,
    is_guest: bool,
    activity: PresenceActivity,
    pool: MatchmakingPool,
    /// The game whose results card this socket is sitting on, if any. The
    /// heartbeat refreshes a rematch presence lease for it, which is what lets
    /// the other players see who is still there without anyone announcing it.
    game_id: Option<u32>,
}

impl SocialSession {
    async fn close(mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
        // Only the socket that still owns the lease may drop it. A planned
        // handoff — and, far more commonly, a second browser tab — leaves the
        // replacement owning the lease, and this must not delete its presence.
        //
        // Nothing is withdrawn here. A disconnect cannot tell "this player
        // left" from "one of this player's sockets went away", so cancelling
        // their challenges on it would cancel live invitations for someone
        // still sitting in another tab. Challenges expire on their own two
        // minute deadline, and a stale one fails honestly at accept time.
        if let Err(error) = self
            .presence
            .release(self.user_id, &self.websocket_id)
            .await
        {
            debug!(user_id = self.user_id, %error, "failed to release presence on disconnect");
        }
    }
}

/// Send the region roster with the viewer removed.
///
/// A roster frame is published once per region and forwarded by every socket,
/// so the "not me" filter has to happen here rather than at publish time.
async fn send_online_players(
    user_id: u32,
    roster: RegionRoster,
    ws_tx: &mpsc::Sender<Message>,
) -> Result<()> {
    let mut roster = roster;
    roster.players.retain(|player| player.user_id != user_id);
    let frame = serde_json::to_string(&WSMessage::OnlinePlayers(roster))
        .context("failed to serialize the online-player roster")?;
    ws_tx
        .send(Message::Text(frame.into()))
        .await
        .context("WebSocket closed before the online-player roster")
}

/// Send this user's complete challenge state. Always a snapshot: a client that
/// missed a hint, handed its socket over, or reconnected simply re-renders.
async fn send_challenge_inbox(
    user_id: u32,
    challenges: &ChallengeStore,
    ws_tx: &mpsc::Sender<Message>,
) -> Result<()> {
    send_challenge_inbox_if_changed(user_id, challenges, ws_tx, &mut None).await
}

/// Send the snapshot, optionally suppressing one that is byte-identical to the
/// last.
///
/// The reconcile timer runs whether or not anything moved, and every snapshot
/// the client receives replaces its state and re-renders both social panels.
/// Passing `Some(last_frame)` makes a quiet inbox cost nothing on the wire and
/// nothing on the client. `None` always sends, which is what the initial
/// snapshot and every post-action refresh want.
async fn send_challenge_inbox_if_changed(
    user_id: u32,
    challenges: &ChallengeStore,
    ws_tx: &mpsc::Sender<Message>,
    last_frame: &mut Option<String>,
) -> Result<()> {
    let inbox = challenges
        .inbox(user_id)
        .await
        .context("failed to read the challenge inbox")?;
    let frame = serde_json::to_string(&WSMessage::Challenges(inbox))
        .context("failed to serialize the challenge inbox")?;
    if last_frame.as_deref() == Some(frame.as_str()) {
        return Ok(());
    }
    let outcome = ws_tx
        .send(Message::Text(frame.clone().into()))
        .await
        .context("WebSocket closed before the challenge inbox");
    if outcome.is_ok() {
        *last_frame = Some(frame);
    }
    outcome
}

/// Whether this identity belongs in the social layer at all.
///
/// Separate from `start_social_session` because the answer never changes for a
/// connection: a failed *claim* is worth retrying on the next message, but a
/// stress identity being ineligible is not. Stress traffic is a separate
/// matchmaking universe, and putting load-test names in everyone's roster
/// would be visible to real players.
fn social_layer_admits(metadata: &PlayerMetadata) -> bool {
    u32::try_from(metadata.user_id).is_ok() && metadata.matchmaking_pool != MatchmakingPool::Stress
}

/// Bring one authenticated socket into the social layer: claim a presence
/// lease, send the current roster and challenge state, and keep both live.
#[allow(clippy::too_many_arguments)]
async fn start_social_session(
    metadata: &PlayerMetadata,
    websocket_id: &str,
    region: &str,
    redis: &RedisConnection,
    pubsub_manager: &Arc<PubSubManager>,
    event_router: &Arc<crate::replication::GameEventRouter>,
    db: &Arc<dyn Database>,
    ws_tx: &mpsc::Sender<Message>,
) -> Option<SocialSession> {
    let Ok(user_id) = u32::try_from(metadata.user_id) else {
        return None;
    };

    let presence = PresenceRegistry::new(redis.clone(), region.to_string());
    let challenges = ChallengeStore::new(redis.clone());

    if let Err(error) = presence
        .claim(
            user_id,
            websocket_id,
            &metadata.username,
            metadata.is_guest,
            PresenceActivity::Idle,
            metadata.matchmaking_pool,
        )
        .await
    {
        warn!(user_id, %error, "failed to claim a presence lease");
        return None;
    }

    match presence.roster().await {
        Ok(roster) => {
            if let Err(error) = send_online_players(user_id, roster, ws_tx).await {
                debug!(user_id, %error, "failed to send the initial roster");
            }
        }
        Err(error) => warn!(user_id, %error, "failed to read the initial roster"),
    }
    if let Err(error) = send_challenge_inbox(user_id, &challenges, ws_tx).await {
        debug!(user_id, %error, "failed to send the initial challenge inbox");
    }

    let intent = Arc::new(std::sync::Mutex::new(PresenceIntent {
        username: metadata.username.clone(),
        is_guest: metadata.is_guest,
        activity: PresenceActivity::Idle,
        pool: metadata.matchmaking_pool,
        game_id: None,
    }));
    let mut tasks = Vec::new();

    // Keep the lease alive. A lease rather than a delete-on-disconnect pair is
    // what makes the roster correct when a task dies without cleaning up.
    let lease_presence = presence.clone();
    let lease_intent = intent.clone();
    let lease_websocket_id = websocket_id.to_string();
    let lease_rematch = RematchStore::new(redis.clone());
    tasks.push(tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(
            crate::presence::PRESENCE_REFRESH_INTERVAL_MS,
        ));
        interval.tick().await;
        loop {
            interval.tick().await;
            let current = match lease_intent.lock() {
                Ok(intent) => intent.clone(),
                Err(poisoned) => poisoned.into_inner().clone(),
            };
            if let Err(error) = lease_presence
                .refresh(
                    user_id,
                    &lease_websocket_id,
                    &current.username,
                    current.is_guest,
                    current.activity,
                    current.pool,
                )
                .await
            {
                debug!(user_id, %error, "presence lease refresh failed");
            }
            if let Some(game_id) = current.game_id
                && let Err(error) = lease_rematch.touch_presence(game_id, user_id).await
            {
                debug!(user_id, game_id, %error, "rematch presence refresh failed");
            }
        }
    }));

    // Roster fan-out. Loss-tolerant by design: a dropped frame is corrected by
    // the next roster change, and nothing depends on having seen every one.
    let roster_tx = ws_tx.clone();
    let roster_channel = RedisKeys::presence_updates_channel(region);
    let mut roster_pubsub = (**pubsub_manager).clone();
    tasks.push(tokio::spawn(async move {
        let Ok(mut receiver) = roster_pubsub.subscribe_to_channel(&roster_channel).await else {
            warn!(channel = %roster_channel, "failed to subscribe to region roster updates");
            return;
        };
        loop {
            let Ok(roster) = receiver.recv::<RegionRoster>().await else {
                break;
            };
            if send_online_players(user_id, roster, &roster_tx)
                .await
                .is_err()
            {
                break;
            }
        }
    }));

    // Challenge delivery. The Pub/Sub hint only says "re-read your state"; the
    // durable records are authoritative and the timer covers a dropped hint,
    // which is the same durable-key + hint + reconcile shape matchmaking uses.
    let challenge_tx = ws_tx.clone();
    let challenge_store = challenges.clone();
    let notification_channel = RedisKeys::user_notifications_channel(user_id);
    let mut challenge_pubsub = (**pubsub_manager).clone();
    let notify_intent = intent.clone();
    let notify_rematch = RematchStore::new(redis.clone());
    let notify_event_router = event_router.clone();
    let notify_db = db.clone();
    tasks.push(tokio::spawn(async move {
        let receiver = challenge_pubsub
            .subscribe_to_channel(&notification_channel)
            .await;
        let mut reconcile = tokio::time::interval(CHALLENGE_RECONCILE_INTERVAL);
        reconcile.tick().await;
        let mut last_frame: Option<String> = None;

        // One channel carries both kinds of nudge, so the payload decides what
        // to re-read. A reconcile tick re-reads everything, which is what
        // makes a dropped hint cost latency rather than correctness.
        let settle = |hint: Option<String>| -> (bool, bool) {
            match hint.as_deref() {
                Some("rematch") => (false, true),
                Some(_) => (true, false),
                None => (true, true),
            }
        };

        // A missing hint channel is a degradation, not a failure: the loop
        // falls back to pure reconcile polling and says so once.
        let mut receiver = match receiver {
            Ok(receiver) => Some(receiver),
            Err(error) => {
                warn!(user_id, %error, "social hints unavailable; falling back to polling");
                None
            }
        };

        loop {
            let hint = match receiver.as_mut() {
                Some(receiver) => {
                    tokio::select! {
                        received = receiver.recv::<String>() => match received {
                            Ok(payload) => Some(payload),
                            Err(_) => break,
                        },
                        _ = reconcile.tick() => None,
                    }
                }
                None => {
                    reconcile.tick().await;
                    None
                }
            };
            let (read_challenges, read_rematch) = settle(hint);

            if read_challenges
                && send_challenge_inbox_if_changed(
                    user_id,
                    &challenge_store,
                    &challenge_tx,
                    &mut last_frame,
                )
                .await
                .is_err()
            {
                break;
            }

            if read_rematch {
                let seated_game_id = match notify_intent.lock() {
                    Ok(intent) => intent.game_id,
                    Err(poisoned) => poisoned.into_inner().game_id,
                };
                if let Some(game_id) = seated_game_id
                    && send_rematch_state(
                        game_id,
                        user_id,
                        &notify_rematch,
                        &notify_event_router,
                        &notify_db,
                        &challenge_tx,
                    )
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }));

    Some(SocialSession {
        user_id,
        websocket_id: websocket_id.to_string(),
        presence,
        intent,
        tasks,
    })
}

/// How often a socket re-reads its challenges regardless of hints. Short
/// enough that a dropped Pub/Sub message is not user-visible as a stuck panel.
const CHALLENGE_RECONCILE_INTERVAL: Duration = Duration::from_secs(15);

/// Re-assert the presence lease with a new activity, so the roster can say
/// whether someone is free to play.
async fn record_presence_activity(
    session: Option<&SocialSession>,
    metadata: &PlayerMetadata,
    activity: PresenceActivity,
    game_id: Option<u32>,
) {
    let Some(session) = session else {
        return;
    };
    let next = PresenceIntent {
        username: metadata.username.clone(),
        is_guest: metadata.is_guest,
        activity,
        pool: metadata.matchmaking_pool,
        game_id,
    };
    // Writing the shared intent first means even a failed refresh converges on
    // the next heartbeat instead of leaving the roster permanently stale.
    let unchanged = match session.intent.lock() {
        Ok(mut current) => {
            let unchanged = current.username == next.username
                && current.is_guest == next.is_guest
                && current.activity == next.activity
                && current.game_id == next.game_id;
            *current = next.clone();
            unchanged
        }
        Err(poisoned) => {
            *poisoned.into_inner() = next.clone();
            false
        }
    };
    if unchanged {
        return;
    }

    if let Err(error) = session
        .presence
        .refresh(
            session.user_id,
            &session.websocket_id,
            &next.username,
            next.is_guest,
            next.activity,
            next.pool,
        )
        .await
    {
        debug!(user_id = session.user_id, %error, "failed to record presence activity");
    }
}

async fn send_challenge_failure(reason: &str, ws_tx: &mpsc::Sender<Message>) -> Result<()> {
    let frame = serde_json::to_string(&WSMessage::ChallengeFailed {
        reason: reason.to_string(),
    })?;
    ws_tx
        .send(Message::Text(frame.into()))
        .await
        .context("WebSocket closed before a challenge failure")
}

async fn subscribe_to_user_count_updates(
    pubsub_manager: Arc<PubSubManager>,
    ws_tx: mpsc::Sender<Message>,
) -> Result<()> {
    let mut manager = (*pubsub_manager).clone();
    let mut receiver = manager
        .subscribe_to_channel("user_count_updates")
        .await
        .context("Failed to subscribe to user_count_updates channel")?;

    info!("Subscribed to user count updates");

    loop {
        let region_counts: HashMap<String, u32> = match receiver.recv().await {
            Ok(counts) => counts,
            Err(e) => {
                warn!("Failed to receive user count update: {}", e);
                break;
            }
        };

        let ws_message = WSMessage::UserCountUpdate { region_counts };
        let json_msg = match serde_json::to_string(&ws_message) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to serialize user count update: {}", e);
                continue;
            }
        };

        if ws_tx.send(Message::Text(json_msg.into())).await.is_err() {
            debug!("WebSocket channel closed, stopping user count subscription");
            break;
        }
    }

    Ok(())
}

#[allow(dead_code)] // custom-game/lobby feature scaffolding, not wired up yet
#[derive(Debug, Deserialize)]
struct LobbyUpdatePayload {
    lobby_code: String,
    members: BTreeMap<u32, lobby_manager::LobbyMember>,
    host_user_id: i32,
    state: String,
    preferences: lobby_manager::LobbyPreferences,
    #[serde(default)]
    ad_break: Option<LobbyAdBreakView>,
}

/// Subscribe to lobby updates and forward to WebSocket client
#[allow(dead_code)] // custom-game/lobby feature scaffolding, not wired up yet
async fn subscribe_to_lobby_updates(
    lobby_code: String,
    pubsub_manager: Arc<PubSubManager>,
    ws_tx: mpsc::Sender<Message>,
) -> Result<()> {
    info!("Subscribing to lobby '{}' updates", lobby_code);

    let channel = RedisKeys::lobby_updates_channel();
    let mut manager = (*pubsub_manager).clone();
    let mut receiver = manager
        .subscribe_to_channel(&channel)
        .await
        .context("Failed to subscribe to lobby updates channel")?;

    info!(
        "Subscribed to lobby updates on '{}' for lobby '{}'",
        channel, lobby_code
    );

    while let Ok(payload) = receiver.recv::<String>().await {
        match serde_json::from_str::<LobbyUpdatePayload>(&payload) {
            Ok(update) => {
                if update.lobby_code != lobby_code {
                    continue;
                }

                let LobbyUpdatePayload {
                    lobby_code,
                    members,
                    host_user_id,
                    state,
                    preferences,
                    ad_break,
                } = update;

                let ws_message = WSMessage::LobbyUpdate {
                    lobby_code,
                    members: members.into_values().collect(),
                    host_user_id,
                    state,
                    preferences,
                    ad_break,
                };

                let json_msg = match serde_json::to_string(&ws_message) {
                    Ok(json) => json,
                    Err(e) => {
                        error!("Failed to serialize lobby update: {}", e);
                        continue;
                    }
                };

                if ws_tx.send(Message::Text(json_msg.into())).await.is_err() {
                    debug!("WebSocket channel closed, stopping lobby subscription");
                    break;
                }
            }
            Err(e) => {
                // Handle lobby deletion notifications or malformed payloads
                match serde_json::from_str::<serde_json::Value>(&payload) {
                    Ok(value) => {
                        let payload_code = value
                            .get("lobby_code")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default();
                        if payload_code != lobby_code {
                            continue;
                        }

                        match value.get("state").and_then(|v| v.as_str()) {
                            Some("deleted") => {
                                info!(
                                    "Received deletion notice for lobby '{}', stopping subscription",
                                    lobby_code
                                );
                                break;
                            }
                            _ => {
                                warn!(
                                    "Unsupported lobby update payload for '{}': {} ({})",
                                    lobby_code, payload, e
                                );
                            }
                        }
                    }
                    Err(value_err) => {
                        warn!(
                            "Failed to parse lobby update payload for '{}': {} ({})",
                            lobby_code, payload, value_err
                        );
                    }
                }
            }
        }
    }

    info!("Stopped subscribing to lobby '{}' updates", lobby_code);
    Ok(())
}

pub async fn discover_peers(db: &Arc<dyn Database>, region: &str) -> Result<Vec<(u64, String)>> {
    info!("Discovering peers in region: {}", region);

    // Query to find all servers in the specified region
    let servers = db
        .get_active_servers(region)
        .await
        .context("Failed to fetch server records")?;

    if servers.is_empty() {
        warn!("No servers found in region: {}", region);
        return Ok(vec![]);
    }

    info!(
        "Found {} servers in region {}: {:?}",
        servers.len(),
        region,
        servers
    );
    Ok(servers
        .into_iter()
        .map(|(id, address)| (id as u64, address))
        .collect())
}

// Helper function to generate unique game codes
#[allow(dead_code)] // custom-game/lobby feature scaffolding, not wired up yet
fn generate_game_code() -> String {
    use rand::{Rng, thread_rng};
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut rng = thread_rng();

    (0..8)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

#[allow(dead_code)] // custom-game/lobby feature scaffolding, not wired up yet
async fn join_custom_game(
    db: &Arc<dyn Database>,
    user_id: i32,
    game_code: &str,
    matchmaking_pool: MatchmakingPool,
) -> Result<u32> {
    ensure_custom_game_access(matchmaking_pool)?;

    // Find the game by code
    let game = db
        .get_game_by_code(game_code)
        .await?
        .context("Game not found or already started")?;

    // Check that game is waiting
    if game.status != "waiting" {
        return Err(anyhow::anyhow!("Game already started"));
    }

    let game_id = game.id;

    // Check if game is full
    let player_count = db.get_player_count(game_id).await?;

    // Get max players from game settings
    let max_players = game
        .game_type
        .get("settings")
        .and_then(|s| s.get("max_players"))
        .and_then(|v| v.as_i64())
        .unwrap_or(4) as i64;

    if player_count >= max_players {
        return Err(anyhow::anyhow!("Game is full"));
    }

    // For now, we need to handle player joining differently since GameState
    // only allows adding players on tick 0. We'll need to implement a proper
    // lobby system or modify the game engine to support late joins.

    // Add player to the game
    db.add_player_to_game(game_id, user_id, 0).await?;

    // TODO: Implement proper player joining through Redis events when game hasn't started yet
    warn!("Player joining for custom games needs proper implementation");

    Ok(game_id as u32)
}

#[allow(dead_code)] // custom-game/lobby feature scaffolding, not wired up yet
async fn check_game_host(db: &Arc<dyn Database>, game_id: u32, user_id: i32) -> Result<bool> {
    let host_user_id = db.get_custom_lobby_host(game_id as i32).await?;
    Ok(host_user_id == Some(user_id))
}

#[allow(dead_code)] // custom-game/lobby feature scaffolding, not wired up yet
async fn spectate_game(
    db: &Arc<dyn Database>,
    user_id: i32,
    game_id: u32,
    game_code: Option<&str>,
    matchmaking_pool: MatchmakingPool,
) -> Result<u32> {
    ensure_custom_game_access(matchmaking_pool)?;

    // If game_code is provided, look up game by code
    let actual_game_id = if let Some(code) = game_code {
        let game = db
            .get_game_by_code(code)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Invalid game code"))?;

        // Check if spectators are allowed for private games
        if game.is_private {
            let lobby = db.get_custom_lobby_by_code(code).await?;

            if let Some(lobby) = lobby {
                let allow_spectators = lobby
                    .settings
                    .get("allow_spectators")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);

                if !allow_spectators {
                    return Err(anyhow::anyhow!("Spectators are not allowed for this game"));
                }
            } else {
                // Private game without lobby, no spectators allowed
                return Err(anyhow::anyhow!("Spectators are not allowed for this game"));
            }
        }
        game.id as u32
    } else {
        // Direct game_id access - check if game exists and is public
        let game = db.get_game_by_id(game_id as i32).await?;

        match game {
            Some(g) if !g.is_private => game_id, // Public game, allow spectating
            Some(_) => return Err(anyhow::anyhow!("Cannot spectate private game without code")),
            None => return Err(anyhow::anyhow!("Game not found")),
        }
    };

    // Add spectator to the game
    db.add_spectator_to_game(actual_game_id as i32, user_id)
        .await?;

    info!(
        "User {} joined as spectator for game {}",
        user_id, actual_game_id
    );
    Ok(actual_game_id)
}

fn ensure_custom_game_access(matchmaking_pool: MatchmakingPool) -> Result<()> {
    if matchmaking_pool == MatchmakingPool::Stress {
        return Err(anyhow!(
            "Stress-test identities cannot join or spectate custom games"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod lifecycle_protocol_tests {
    use super::{
        CommandOutcomeReplay, GameChatBroadcast, GameJoinAuthorizationError, GameSubscriptionInput,
        PlayerMetadata, TERMINAL_COMMAND_REJECTION_REASON, WSMessage,
        abort_and_join_game_event_forwarder, acknowledge_lobby_match_handoff,
        canonical_command_identity, command_outcomes_for_user, ensure_custom_game_access,
        game_join_denied, game_join_failure_message, load_game_chat_history,
        lobby_video_ad_targets, missing_game_join_failure, next_game_subscription_input,
        next_lobby_match, next_outbound_message, publish_game_chat_message,
        queue_planned_drain_notice, recovery_bridge_snapshot, refresh_connection_username,
        repair_legacy_chat_history, require_game_command_publication, sanitize_anon_id,
        send_command_outcomes_from_resolved, send_completed_game_snapshot_from_resolved,
        send_recovery_bridge_snapshot, slow_command_publish_wait_ms,
        snapshot_requires_command_outcomes, subscribe_to_game_chat,
        subscribe_to_lobby_match_notifications, take_lobby_update_receiver,
        validate_client_protocol_version, validate_game_matchmaking_pool,
    };
    use crate::ads::ClientDistribution;
    use crate::db::models::RuntimeAdsConfig;
    use crate::lifecycle::{DrainNotice, WS_PROTOCOL_VERSION};
    use crate::lobby_manager::{Lobby, LobbyMember, LobbyPreferences};
    use crate::matchmaking_manager::{ActiveMatch, MatchStatus};
    use crate::matchmaking_pool::MatchmakingPool;
    use crate::pubsub_manager::PubSubManager;
    use crate::recovery::{
        RecoveryEnvelopeV2, ResolvedCommandState, SPARSE_COMMAND_WINDOW_REJECTION_REASON,
        SessionCommandOutcomes, SessionCommandRejectionFence,
    };
    use crate::redis_keys::RedisKeys;
    use crate::redis_utils::{RedisConnection, create_connection_manager};
    use common::{
        ClientCommandIdentityV2, GameEvent, GameEventMessage, GameState, GameStatus, GameType,
        QueueMode,
    };
    use redis::{AsyncCommands, Client};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tokio::sync::{broadcast, mpsc, oneshot};
    use tokio::time::{Duration, timeout};
    use tokio_tungstenite::tungstenite::Message;
    use tokio_util::sync::CancellationToken;

    fn ad_capability_member(
        user_id: u32,
        distribution: Option<ClientDistribution>,
        supports_ad_break: bool,
        can_show_video_ad: bool,
    ) -> LobbyMember {
        LobbyMember {
            user_id,
            username: format!("player-{user_id}"),
            ts: 1.0,
            supports_ad_break,
            can_show_video_ad,
            distribution,
        }
    }

    #[test]
    fn mixed_distribution_lobby_runs_only_when_someone_can_show_video() {
        let web = ad_capability_member(1, Some(ClientDistribution::Web), true, true);
        let itch = ad_capability_member(2, Some(ClientDistribution::Itch), true, true);
        let runtime_ads = RuntimeAdsConfig {
            enabled: true,
            distributions: crate::db::models::RuntimeAdsDistributionsConfig {
                web: crate::db::models::RuntimeDistributionAdsConfig { enabled: true },
                crazygames: crate::db::models::RuntimeDistributionAdsConfig { enabled: true },
                itch: crate::db::models::RuntimeDistributionAdsConfig { enabled: false },
            },
            ..RuntimeAdsConfig::default()
        };
        assert_eq!(
            lobby_video_ad_targets([&web, &itch], &runtime_ads),
            Some(vec![1])
        );

        let another_no_ad_build =
            ad_capability_member(3, Some(ClientDistribution::Itch), true, false);
        assert!(lobby_video_ad_targets([&itch, &another_no_ad_build], &runtime_ads).is_none());

        let legacy = ad_capability_member(4, None, false, false);
        assert!(lobby_video_ad_targets([&web, &legacy], &runtime_ads).is_none());
        assert!(lobby_video_ad_targets(std::iter::empty(), &runtime_ads).is_none());
    }

    #[test]
    fn successful_guest_rename_refreshes_same_socket_identity() {
        let mut metadata = PlayerMetadata {
            user_id: 7,
            username: "Guest1234".to_owned(),
            token: "session-token".to_owned(),
            is_guest: true,
            matchmaking_pool: MatchmakingPool::Public,
            supports_ad_break: true,
            can_show_video_ad: false,
            distribution: Some(ClientDistribution::Web),
        };

        refresh_connection_username(&mut metadata, "CrazyPlayer".to_owned());

        assert_eq!(metadata.username, "CrazyPlayer");
        assert_eq!(metadata.user_id, 7);
        assert_eq!(metadata.token, "session-token");
    }

    #[test]
    fn slow_command_publish_logging_uses_a_strict_one_second_threshold() {
        assert_eq!(
            slow_command_publish_wait_ms(Duration::from_millis(999)),
            None
        );
        assert_eq!(slow_command_publish_wait_ms(Duration::from_secs(1)), None);
        assert_eq!(
            slow_command_publish_wait_ms(Duration::from_millis(1_001)),
            Some(1_001)
        );
    }

    #[test]
    fn command_publication_errors_fail_closed_while_completion_is_not_an_error() {
        assert!(require_game_command_publication(Ok(true)).unwrap());
        assert!(!require_game_command_publication(Ok(false)).unwrap());

        let error =
            require_game_command_publication(Err(anyhow::anyhow!("ambiguous write"))).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("v2 game command publication failed")
        );
    }

    #[test]
    fn stress_direct_and_custom_game_joins_fail_closed() {
        let public_match = ActiveMatch {
            players: Vec::new(),
            spectators: Vec::new(),
            lobby_codes: vec!["PUBLIC".to_owned()],
            game_type: GameType::Solo,
            status: MatchStatus::Active,
            partition_id: 0,
            created_at: 0,
            matchmaking_pool: MatchmakingPool::Public,
        };
        let stress_match = ActiveMatch {
            matchmaking_pool: MatchmakingPool::Stress,
            ..public_match.clone()
        };

        assert!(validate_game_matchmaking_pool(MatchmakingPool::Public, None).is_ok());
        assert!(validate_game_matchmaking_pool(MatchmakingPool::Stress, None).is_err());
        assert!(
            validate_game_matchmaking_pool(MatchmakingPool::Stress, Some(&public_match)).is_err()
        );
        assert!(
            validate_game_matchmaking_pool(MatchmakingPool::Stress, Some(&stress_match)).is_ok()
        );
        assert!(ensure_custom_game_access(MatchmakingPool::Public).is_ok());
        assert!(ensure_custom_game_access(MatchmakingPool::Stress).is_err());
    }

    #[test]
    fn gateway_canonicalizes_untrusted_command_scope() {
        let identity = canonical_command_identity(
            ClientCommandIdentityV2 {
                game_id: 999,
                user_id: 888,
                client_game_session_id: "session-a".to_owned(),
                sequence: 7,
            },
            42,
            5,
        );
        assert_eq!(identity.game_id, 42);
        assert_eq!(identity.user_id, 5);
        assert_eq!(identity.client_game_session_id, "session-a");
        assert_eq!(identity.sequence, 7);
    }

    #[tokio::test]
    async fn lobby_forwarder_keeps_the_initial_update_published_during_join() {
        let (updates, mut join_receiver) = broadcast::channel(4);
        let initial = Lobby {
            lobby_code: "USE1-INITIAL".to_owned(),
            members: BTreeMap::new(),
            host_user_id: 7,
            state: "open".to_owned(),
            preferences: LobbyPreferences::default(),
            ad_break: None,
        };
        updates
            .send(initial)
            .expect("join receiver should retain the initial update");

        let mut forwarder_receiver = take_lobby_update_receiver(&mut join_receiver);
        assert_eq!(
            forwarder_receiver
                .recv()
                .await
                .expect("forwarder lost the queued initial update")
                .lobby_code,
            "USE1-INITIAL"
        );
        assert!(matches!(
            join_receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));

        let next = Lobby {
            lobby_code: "USE1-NEXT".to_owned(),
            members: BTreeMap::new(),
            host_user_id: 7,
            state: "open".to_owned(),
            preferences: LobbyPreferences::default(),
            ad_break: None,
        };
        updates
            .send(next)
            .expect("both receivers should remain subscribed");
        assert_eq!(
            forwarder_receiver
                .recv()
                .await
                .expect("forwarder lost a later update")
                .lobby_code,
            "USE1-NEXT"
        );
    }

    #[test]
    fn only_fresh_snapshots_require_adjacent_command_outcomes() {
        let state = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, Some(1), 0);
        assert!(snapshot_requires_command_outcomes(&GameEvent::Snapshot {
            game_state: state,
        }));
        assert!(!snapshot_requires_command_outcomes(&GameEvent::TickHash {
            hash: 1,
            server_ts_ms: 2,
        }));
    }

    #[test]
    fn recovery_outcomes_are_filtered_to_the_authenticated_user() {
        let resolved = ResolvedCommandState {
            sessions: BTreeMap::from([
                ("5:session-a".to_owned(), SessionCommandOutcomes::default()),
                ("6:session-b".to_owned(), SessionCommandOutcomes::default()),
            ]),
        };
        let filtered = command_outcomes_for_user(resolved, 5);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].0, "session-a");
    }

    #[test]
    fn authentication_ack_advertises_an_explicit_capability_envelope() {
        let value = serde_json::to_value(WSMessage::Authenticated {
            task_boot_id: "task-a".to_owned(),
            protocol_version: WS_PROTOCOL_VERSION,
            capabilities: vec!["planned-drain-v1".to_owned()],
            socket_generation: 3,
        })
        .unwrap();
        assert_eq!(value["Authenticated"]["task_boot_id"], "task-a");
        assert_eq!(
            value["Authenticated"]["protocol_version"],
            WS_PROTOCOL_VERSION
        );
        assert_eq!(value["Authenticated"]["socket_generation"], 3);
    }

    #[test]
    fn authentication_request_reports_the_required_protocol_version() {
        let value = serde_json::to_value(WSMessage::Authenticate {
            token: "jwt".to_owned(),
            protocol_version: WS_PROTOCOL_VERSION,
            anon_id: None,
            distribution: Some(ClientDistribution::Web),
        })
        .unwrap();
        assert_eq!(value["Authenticate"]["token"], "jwt");
        assert_eq!(
            value["Authenticate"]["protocol_version"],
            WS_PROTOCOL_VERSION
        );
        // Absent rather than null, so the frame a version-less client sends is
        // byte-identical to the one this server produces.
        assert!(value["Authenticate"].get("anon_id").is_none());
        assert_eq!(value["Authenticate"]["distribution"], "web");
    }

    /// The analytics identifier is additive in both directions: a client that
    /// predates it still authenticates, and a client that sends it is not
    /// refused by a server that ignores it. A shipped bundle cannot update
    /// itself, so neither direction may ever become a hard failure.
    #[test]
    fn the_anon_id_is_optional_in_both_directions() {
        let without = serde_json::json!({
            "Authenticate": { "token": "jwt", "protocol_version": WS_PROTOCOL_VERSION }
        });
        let parsed: WSMessage = serde_json::from_value(without).unwrap();
        assert!(matches!(
            parsed,
            WSMessage::Authenticate { anon_id: None, .. }
        ));

        let with = serde_json::json!({
            "Authenticate": {
                "token": "jwt",
                "protocol_version": WS_PROTOCOL_VERSION,
                "anon_id": "3f1a2b4c-5d6e-4f70-8a91-b2c3d4e5f607"
            }
        });
        let parsed: WSMessage = serde_json::from_value(with).unwrap();
        assert!(matches!(
            parsed,
            WSMessage::Authenticate { anon_id: Some(ref id), .. }
                if id == "3f1a2b4c-5d6e-4f70-8a91-b2c3d4e5f607"
        ));
    }

    /// Untrusted client input destined for an analytics event. Anything that is
    /// not a canonical lowercase UUID is dropped rather than propagated, and a
    /// rejection never affects admission.
    #[test]
    fn only_canonical_uuids_survive_anon_id_sanitization() {
        let good = "3f1a2b4c-5d6e-4f70-8a91-b2c3d4e5f607";
        assert_eq!(
            sanitize_anon_id(Some(good.to_owned())),
            Some(good.to_owned())
        );

        for bad in [
            "",
            "not-a-uuid",
            "3F1A2B4C-5D6E-4F70-8A91-B2C3D4E5F607", // uppercase
            "3f1a2b4c5d6e4f708a91b2c3d4e5f607",     // unhyphenated
            "3f1a2b4c-5d6e-4f70-8a91-b2c3d4e5f60",  // short group
            "3f1a2b4c-5d6e-4f70-8a91-b2c3d4e5f607-x", // trailing group
            "3f1a2b4c-5d6e-4f70-8a91-b2c3d4e5g607", // non-hex
        ] {
            assert_eq!(
                sanitize_anon_id(Some(bad.to_owned())),
                None,
                "accepted {bad:?}"
            );
        }
        assert_eq!(sanitize_anon_id(None), None);
    }

    /// An unbounded string must never reach a column value.
    #[test]
    fn an_oversized_anon_id_is_rejected() {
        assert_eq!(sanitize_anon_id(Some("a".repeat(100_000))), None);
    }

    #[test]
    fn authentication_request_can_omit_distribution() {
        let value: WSMessage = serde_json::from_value(serde_json::json!({
            "Authenticate": {
                "token": "jwt",
                "protocol_version": WS_PROTOCOL_VERSION
            }
        }))
        .unwrap();
        assert!(matches!(
            value,
            WSMessage::Authenticate {
                distribution: None,
                ..
            }
        ));
    }

    #[test]
    fn only_the_exact_gameplay_protocol_is_admitted() {
        assert!(validate_client_protocol_version(Some(WS_PROTOCOL_VERSION)).is_ok());
        for version in [
            WS_PROTOCOL_VERSION.saturating_sub(1),
            WS_PROTOCOL_VERSION.saturating_add(1),
            0,
        ] {
            let error = validate_client_protocol_version(Some(version)).unwrap_err();
            assert!(error.to_string().contains("Gameplay update required"));
        }
        let error = validate_client_protocol_version(None).unwrap_err();
        assert!(error.to_string().contains("did not report"));
    }

    #[test]
    fn the_legacy_token_shape_remains_parseable_for_an_explicit_denial() {
        let value = serde_json::to_value(WSMessage::Token("jwt".to_owned())).unwrap();
        assert_eq!(value["Token"], "jwt");

        let parsed: WSMessage = serde_json::from_value(value).unwrap();
        assert!(matches!(parsed, WSMessage::Token(token) if token == "jwt"));
    }

    #[tokio::test]
    async fn planned_drain_bypasses_a_saturated_gameplay_queue() {
        let (ws_tx, mut ws_rx) = mpsc::channel(1024);
        for sequence in 0..1024 {
            ws_tx
                .try_send(Message::Text(format!("gameplay-{sequence}").into()))
                .unwrap();
        }
        let (drain_tx, mut drain_rx) = mpsc::channel(1);
        queue_planned_drain_notice(
            &drain_tx,
            &DrainNotice {
                task_boot_id: "departing-task".to_owned(),
                deadline_unix_ms: 123_456,
            },
        )
        .unwrap();

        let mut drain_open = true;
        let mut ws_open = true;
        let first = next_outbound_message(&mut drain_rx, &mut ws_rx, &mut drain_open, &mut ws_open)
            .await
            .unwrap();
        assert!(matches!(
            decode_ws_message(first),
            WSMessage::Drain {
                task_boot_id,
                deadline_unix_ms: 123_456,
            } if task_boot_id == "departing-task"
        ));

        let second =
            next_outbound_message(&mut drain_rx, &mut ws_rx, &mut drain_open, &mut ws_open)
                .await
                .unwrap();
        assert_eq!(second, Message::Text("gameplay-0".into()));
    }

    #[test]
    fn recovery_bridge_uses_the_exact_checkpointed_event_watermark() {
        let state = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, Some(5), 0);
        let envelope = RecoveryEnvelopeV2::new(
            42,
            2,
            state,
            "123-0".to_owned(),
            ResolvedCommandState::default(),
            7,
            41,
            1_000,
            "lease-token".to_owned(),
        );

        let bridge = recovery_bridge_snapshot(&envelope, 5);
        assert_eq!(bridge.stream_seq, 41);
        assert_eq!(bridge.game_id, 42);
    }

    #[tokio::test]
    async fn recovery_bridge_withholds_the_handoff_promotion_barrier() {
        let state = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, Some(5), 0);
        let envelope = RecoveryEnvelopeV2::new(
            42,
            2,
            state,
            "123-0".to_owned(),
            ResolvedCommandState::default(),
            7,
            41,
            1_000,
            "lease-token".to_owned(),
        );
        let (tx, mut rx) = mpsc::channel(2);

        assert!(send_recovery_bridge_snapshot(&tx, &envelope, 5).await);
        assert!(matches!(
            decode_ws_message(rx.recv().await.unwrap()),
            WSMessage::GameEvent(_)
        ));
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn pending_outcome_replay_does_not_block_live_game_events() {
        let (ws_tx, _ws_rx) = mpsc::channel(2);
        let (events_tx, events_rx) = broadcast::channel(2);
        let mut events = crate::replication::GameEventSubscription::for_test(events_rx, 42);
        events.anchor(4);
        let (release_tx, release_rx) = oneshot::channel();
        let mut replay: Option<CommandOutcomeReplay> = Some(Box::pin(async move {
            release_rx.await.expect("test replay release was dropped");
            Some(ResolvedCommandState::default())
        }));
        let live_event = GameEventMessage {
            game_id: 42,
            tick: 3,
            sequence: 4,
            stream_seq: 5,
            user_id: None,
            event: GameEvent::TickHash {
                hash: 6,
                server_ts_ms: 7,
            },
        };
        events_tx
            .send(live_event)
            .expect("subscription should remain attached");

        let input = timeout(
            Duration::from_secs(1),
            next_game_subscription_input(&ws_tx, &mut events, &mut replay),
        )
        .await
        .expect("a pending Redis replay blocked a ready live event");
        assert!(matches!(
            input,
            GameSubscriptionInput::Update(Some(crate::replication::SubscriptionUpdate::Event(
                event
            ))) if event.game_id == 42 && event.stream_seq == 5
        ));

        release_tx.send(()).expect("replay future disappeared");
        let input = timeout(
            Duration::from_secs(1),
            next_game_subscription_input(&ws_tx, &mut events, &mut replay),
        )
        .await
        .expect("completed outcome replay was not returned to the forwarding loop");
        assert!(matches!(
            input,
            GameSubscriptionInput::CommandOutcomes(Some(_))
        ));
    }

    #[tokio::test]
    async fn replacement_joins_the_cancelled_game_event_forwarder() {
        struct NotifyOnDrop(Option<oneshot::Sender<()>>);

        impl Drop for NotifyOnDrop {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }

        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        let mut handle = Some(tokio::spawn(async move {
            let _notify = NotifyOnDrop(Some(dropped_tx));
            started_tx.send(()).expect("test starter was dropped");
            std::future::pending::<()>().await;
        }));
        started_rx
            .await
            .expect("forwarder did not reach its pending replay");

        abort_and_join_game_event_forwarder(&mut handle).await;

        assert!(handle.is_none());
        timeout(Duration::from_secs(1), dropped_rx)
            .await
            .expect("replacement returned before the cancelled forwarder was dropped")
            .expect("forwarder drop notification was lost");
    }

    #[test]
    fn cold_game_response_is_explicitly_retryable() {
        let warming = serde_json::to_value(game_join_failure_message(
            42,
            GameJoinAuthorizationError::Warming,
        ))
        .unwrap();
        assert_eq!(warming["GameWarming"]["game_id"], 42);
        assert_eq!(warming["GameWarming"]["retry_after_ms"], 500);
        assert!(warming.get("GameLoadFailed").is_none());

        let terminal = serde_json::to_value(game_join_failure_message(
            42,
            game_join_denied("This game was not found or has expired"),
        ))
        .unwrap();
        assert_eq!(terminal["GameLoadFailed"]["game_id"], 42);
        assert_eq!(
            terminal["GameLoadFailed"]["reason"],
            "This game was not found or has expired"
        );
        assert!(terminal.get("GameWarming").is_none());
    }

    #[test]
    fn durable_active_mapping_keeps_precreation_gap_retryable() {
        assert!(matches!(
            missing_game_join_failure(42, Some(42)),
            GameJoinAuthorizationError::Warming
        ));
        assert!(matches!(
            missing_game_join_failure(42, None),
            GameJoinAuthorizationError::Denied(_)
        ));
        assert!(matches!(
            missing_game_join_failure(42, Some(41)),
            GameJoinAuthorizationError::Denied(_)
        ));
    }

    #[test]
    fn lobby_match_reconciliation_covers_commit_before_subscribe() {
        assert_eq!(next_lobby_match(Some(42), Some(42), None, false), Some(42));
        assert_eq!(next_lobby_match(None, Some(42), None, false), Some(42));
    }

    #[test]
    fn lobby_match_reconciliation_retries_until_ack_and_allows_play_again() {
        assert_eq!(
            next_lobby_match(Some(42), Some(42), Some(42), false),
            None,
            "push hints must be deduplicated"
        );
        assert_eq!(
            next_lobby_match(None, Some(42), Some(42), true),
            Some(42),
            "periodic reconciliation must retry an unacknowledged handoff after active cleanup"
        );
        assert_eq!(
            next_lobby_match(Some(42), None, Some(42), true),
            None,
            "acknowledged active matches must stay deduplicated"
        );
        assert_eq!(
            next_lobby_match(Some(43), Some(43), Some(42), false),
            Some(43)
        );
        assert_eq!(next_lobby_match(None, None, Some(42), true), None);
    }

    #[tokio::test]
    async fn live_lobby_listener_survives_active_cleanup_until_member_acknowledges() {
        let redis_url = "redis://127.0.0.1:6379/1?protocol=resp3";
        let client = Client::open(redis_url).unwrap();
        let (pubsub_tx, _pubsub_rx) = broadcast::channel(128);
        let redis = create_connection_manager(client.clone(), pubsub_tx.clone())
            .await
            .unwrap();
        let pubsub_redis = create_connection_manager(client.clone(), pubsub_tx.clone())
            .await
            .unwrap();
        let ack_redis = RedisConnection::from(
            create_connection_manager(client.clone(), pubsub_tx.clone())
                .await
                .unwrap(),
        );
        let pubsub_manager = Arc::new(PubSubManager::new(pubsub_redis, pubsub_tx));
        let mut control = client.get_multiplexed_async_connection().await.unwrap();
        let lobby_code = format!("LISTENER-{}", uuid::Uuid::new_v4());
        let user_id = 77_u32;
        let mapping_key = RedisKeys::matchmaking_lobby_active_game(&lobby_code);
        let pending_key = RedisKeys::matchmaking_lobby_user_pending_game(&lobby_code, user_id);
        let channel = RedisKeys::matchmaking_lobby_notification_channel(&lobby_code);
        let first_game_id = 42_001_u32;
        let second_game_id = 42_002_u32;

        // This commit predates SUBSCRIBE. The listener's subscribe-then-GET
        // ordering must still deliver it.
        control
            .set::<_, _, ()>(&mapping_key, first_game_id)
            .await
            .unwrap();
        control
            .set::<_, _, ()>(&pending_key, first_game_id)
            .await
            .unwrap();
        let (ws_tx, mut ws_rx) = mpsc::channel(8);
        let cancellation = CancellationToken::new();
        let listener = tokio::spawn(subscribe_to_lobby_match_notifications(
            lobby_code.clone(),
            user_id,
            pubsub_manager,
            redis,
            ws_tx,
            cancellation.clone(),
            Duration::from_millis(100),
        ));

        let first = timeout(Duration::from_secs(2), ws_rx.recv())
            .await
            .expect("listener did not reconcile the preexisting mapping")
            .expect("listener closed before delivering the preexisting mapping");
        assert!(matches!(
            decode_ws_message(first),
            WSMessage::JoinGame(game_id) if game_id == first_game_id
        ));
        assert!(
            acknowledge_lobby_match_handoff(&ack_redis, &lobby_code, user_id, first_game_id)
                .await
                .unwrap()
        );

        let duplicate_hint = serde_json::json!({
            "type": "MatchFound",
            "game_id": first_game_id,
            "partition_id": 1,
        })
        .to_string();
        for _ in 0..2 {
            control
                .publish::<_, _, ()>(&channel, &duplicate_hint)
                .await
                .unwrap();
        }
        assert!(
            timeout(Duration::from_millis(250), ws_rx.recv())
                .await
                .is_err(),
            "duplicate hints must not forward a second JoinGame"
        );

        // Deliberately publish no hint for the later game, then simulate a
        // short round completing before the listener's next five-second read.
        // The admission lock is gone; only the per-member handoff remains.
        redis::pipe()
            .atomic()
            .set(&mapping_key, second_game_id)
            .set(&pending_key, second_game_id)
            .del(&mapping_key)
            .query_async::<()>(&mut control)
            .await
            .unwrap();
        let second = timeout(Duration::from_secs(1), ws_rx.recv())
            .await
            .expect("periodic reconciliation did not recover the missed hint")
            .expect("listener closed before periodic reconciliation");
        assert!(matches!(
            decode_ws_message(second),
            WSMessage::JoinGame(game_id) if game_id == second_game_id
        ));

        assert!(
            !acknowledge_lobby_match_handoff(&ack_redis, &lobby_code, user_id, first_game_id)
                .await
                .unwrap(),
            "a delayed acknowledgement must not consume the newer round"
        );
        assert!(
            acknowledge_lobby_match_handoff(&ack_redis, &lobby_code, user_id, second_game_id)
                .await
                .unwrap()
        );
        control
            .publish::<_, _, ()>(
                &channel,
                serde_json::json!({
                    "type": "MatchFound",
                    "game_id": second_game_id,
                    "partition_id": 2,
                })
                .to_string(),
            )
            .await
            .unwrap();
        assert!(
            timeout(Duration::from_millis(250), ws_rx.recv())
                .await
                .is_err(),
            "an acknowledged handoff must not repeat JoinGame on later hints"
        );

        cancellation.cancel();
        timeout(Duration::from_secs(1), listener)
            .await
            .expect("listener ignored cancellation")
            .expect("listener task panicked");
        control
            .del::<_, ()>((mapping_key, pending_key))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn current_game_chat_envelope_persists_replays_and_forwards_live() {
        let redis_url = "redis://127.0.0.1:6379/1?protocol=resp3";
        let client = Client::open(redis_url).unwrap();
        let (pubsub_tx, _pubsub_rx) = broadcast::channel(128);
        let redis = create_connection_manager(client.clone(), pubsub_tx.clone())
            .await
            .unwrap();
        let pubsub_redis = create_connection_manager(client.clone(), pubsub_tx.clone())
            .await
            .unwrap();
        let pubsub_manager = Arc::new(PubSubManager::new(pubsub_redis, pubsub_tx));
        let mut control = client.get_multiplexed_async_connection().await.unwrap();
        let game_id = uuid::Uuid::new_v4().as_u128() as u32;
        let history_key = RedisKeys::game_chat_history_key(game_id);
        let channel = RedisKeys::game_chat_channel(game_id);
        control.del::<_, ()>(&history_key).await.unwrap();

        let filtered_message = "so many ******** sin this server";
        let message_id = "game-chat-filter-regression";
        publish_game_chat_message(
            redis.clone().into(),
            GameChatBroadcast {
                game_id,
                message_id: message_id.to_owned(),
                user_id: 7,
                username: "player".to_owned(),
                message: filtered_message.to_owned(),
                timestamp_ms: 1,
            },
        )
        .await
        .unwrap();

        let stored_entries: Vec<String> = control.lrange(&history_key, 0, -1).await.unwrap();
        let stored = stored_entries
            .last()
            .expect("published game chat should be retained");
        let stored_json: serde_json::Value = serde_json::from_str(stored).unwrap();
        assert_eq!(stored_json["game_id"], game_id);
        assert_eq!(stored_json["message"], filtered_message);
        assert_eq!(stored_json["content_filter_version"], 1);

        let legacy_message_id = "legacy-game-chat-filter-regression";
        let legacy_entry = serde_json::json!({
            "game_id": game_id,
            "message_id": legacy_message_id,
            "user_id": 7,
            "username": "legacy-player",
            "message": "fuck",
            "timestamp_ms": 2,
        })
        .to_string();
        control
            .rpush::<_, _, ()>(&history_key, legacy_entry)
            .await
            .unwrap();

        let history = load_game_chat_history(redis.clone().into(), game_id)
            .await
            .unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].message, filtered_message);
        assert_eq!(history[1].message_id, legacy_message_id);
        assert_eq!(history[1].message, "****");

        let repaired_entries: Vec<String> = control.lrange(&history_key, 0, -1).await.unwrap();
        let repaired_legacy = repaired_entries
            .iter()
            .map(|entry| serde_json::from_str::<serde_json::Value>(entry).unwrap())
            .find(|entry| entry["message_id"] == legacy_message_id)
            .expect("legacy game chat should remain after atomic read repair");
        assert_eq!(repaired_legacy["message"], "****");
        assert_eq!(repaired_legacy["content_filter_version"], 1);

        let (ws_tx, mut ws_rx) = mpsc::channel(4);
        let listener = tokio::spawn(subscribe_to_game_chat(game_id, pubsub_manager, ws_tx));

        let live = timeout(Duration::from_secs(2), async {
            loop {
                control.publish::<_, _, ()>(&channel, stored).await.unwrap();
                if let Ok(Some(message)) = timeout(Duration::from_millis(50), ws_rx.recv()).await {
                    return message;
                }
            }
        })
        .await
        .expect("game chat subscriber did not forward the current envelope");
        assert!(matches!(
            decode_ws_message(live),
            WSMessage::GameChatMessage {
                game_id: delivered_game_id,
                message_id: delivered_message_id,
                message,
                ..
            } if delivered_game_id == game_id
                && delivered_message_id == message_id
                && message == filtered_message
        ));

        listener.abort();
        let _ = listener.await;
        control.del::<_, ()>(&history_key).await.unwrap();
    }

    #[tokio::test]
    async fn legacy_chat_read_repair_compares_current_entries_before_replacing() {
        let redis_url = "redis://127.0.0.1:6379/1?protocol=resp3";
        let client = Client::open(redis_url).unwrap();
        let (pubsub_tx, _pubsub_rx) = broadcast::channel(8);
        let redis = create_connection_manager(client.clone(), pubsub_tx)
            .await
            .unwrap();
        let mut control = client.get_multiplexed_async_connection().await.unwrap();
        let key = format!(
            "chat-history-repair-race:{}",
            uuid::Uuid::new_v4().as_u128()
        );
        control.del::<_, ()>(&key).await.unwrap();
        control
            .rpush::<_, _, ()>(&key, &["legacy-a", "legacy-b"])
            .await
            .unwrap();
        control.expire::<_, ()>(&key, 120).await.unwrap();

        let stale_repairs = vec![
            ("legacy-a".to_owned(), "filtered-a".to_owned()),
            ("legacy-b".to_owned(), "filtered-b".to_owned()),
        ];
        control.rpush::<_, _, ()>(&key, "current-c").await.unwrap();
        control.ltrim::<_, ()>(&key, -2, -1).await.unwrap();

        let mut repair_redis = redis.into();
        repair_legacy_chat_history(&mut repair_redis, &key, &stale_repairs)
            .await
            .unwrap();

        let repaired: Vec<String> = control.lrange(&key, 0, -1).await.unwrap();
        assert_eq!(repaired, ["filtered-b", "current-c"]);
        let ttl: i64 = control.ttl(&key).await.unwrap();
        assert!(ttl > 0, "read repair should preserve the existing TTL");
        control.del::<_, ()>(&key).await.unwrap();
    }

    #[test]
    fn command_outcome_barrier_has_an_explicit_game_scope() {
        let value = serde_json::to_value(WSMessage::CommandOutcomesComplete {
            game_id: 42,
            terminal_rejection_reason: None,
        })
        .unwrap();
        assert_eq!(value["CommandOutcomesComplete"]["game_id"], 42);
        assert!(
            value["CommandOutcomesComplete"]
                .get("terminal_rejection_reason")
                .is_none()
        );
    }

    fn decode_ws_message(message: Message) -> WSMessage {
        let Message::Text(text) = message else {
            panic!("expected a text WebSocket message");
        };
        serde_json::from_str(&text).unwrap()
    }

    #[tokio::test]
    async fn empty_recovery_outcomes_still_emit_the_promotion_barrier() {
        let (tx, mut rx) = mpsc::channel(2);
        assert!(
            send_command_outcomes_from_resolved(&tx, 42, 5, ResolvedCommandState::default(), None,)
                .await
        );
        assert!(matches!(
            decode_ws_message(rx.recv().await.unwrap()),
            WSMessage::CommandOutcomesComplete {
                game_id: 42,
                terminal_rejection_reason: None,
            }
        ));
    }

    #[tokio::test]
    async fn recovery_outcomes_include_the_session_rejection_fence() {
        let (tx, mut rx) = mpsc::channel(2);
        let resolved = ResolvedCommandState {
            sessions: BTreeMap::from([(
                "5:session-a".to_owned(),
                SessionCommandOutcomes {
                    contiguous_through: 7,
                    outcomes: BTreeMap::new(),
                    rejection_fence: Some(SessionCommandRejectionFence {
                        from_sequence: 9,
                        reason: SPARSE_COMMAND_WINDOW_REJECTION_REASON.to_owned(),
                    }),
                },
            )]),
        };

        assert!(send_command_outcomes_from_resolved(&tx, 42, 5, resolved, None).await);
        assert!(matches!(
            decode_ws_message(rx.recv().await.unwrap()),
            WSMessage::CommandOutcomes {
                game_id: 42,
                client_game_session_id,
                contiguous_through: 7,
                rejection_fence: Some(SessionCommandRejectionFence {
                    from_sequence: 9,
                    reason,
                }),
                ..
            } if client_game_session_id == "session-a"
                && reason == SPARSE_COMMAND_WINDOW_REJECTION_REASON
        ));
        assert!(matches!(
            decode_ws_message(rx.recv().await.unwrap()),
            WSMessage::CommandOutcomesComplete {
                game_id: 42,
                terminal_rejection_reason: None,
            }
        ));
    }

    #[tokio::test]
    async fn completed_snapshot_is_followed_by_terminal_recovery_fence_and_barrier() {
        let mut state = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, Some(1), 0);
        state.add_player(5, None).unwrap();
        state.status = GameStatus::Complete {
            winning_snake_id: Some(0),
        };
        let (tx, mut rx) = mpsc::channel(3);
        let resolved = ResolvedCommandState {
            sessions: BTreeMap::from([(
                "5:terminal-session".to_owned(),
                SessionCommandOutcomes {
                    contiguous_through: 3,
                    outcomes: BTreeMap::new(),
                    rejection_fence: Some(SessionCommandRejectionFence {
                        from_sequence: 4,
                        reason: SPARSE_COMMAND_WINDOW_REJECTION_REASON.to_owned(),
                    }),
                },
            )]),
        };

        assert!(
            send_completed_game_snapshot_from_resolved(
                &tx,
                42,
                5,
                &state,
                resolved,
                Some(TERMINAL_COMMAND_REJECTION_REASON),
            )
            .await
        );

        assert!(matches!(
            decode_ws_message(rx.recv().await.unwrap()),
            WSMessage::GameEvent(event)
                if matches!(
                    &event.event,
                    GameEvent::Snapshot { game_state }
                        if matches!(game_state.status, GameStatus::Complete { .. })
                )
        ));
        assert!(matches!(
            decode_ws_message(rx.recv().await.unwrap()),
            WSMessage::CommandOutcomes {
                game_id: 42,
                client_game_session_id,
                contiguous_through: 3,
                rejection_fence: Some(SessionCommandRejectionFence {
                    from_sequence: 4,
                    reason,
                }),
                ..
            } if client_game_session_id == "terminal-session"
                && reason == SPARSE_COMMAND_WINDOW_REJECTION_REASON
        ));
        assert!(matches!(
            decode_ws_message(rx.recv().await.unwrap()),
            WSMessage::CommandOutcomesComplete {
                game_id: 42,
                terminal_rejection_reason: Some(reason),
            } if reason == TERMINAL_COMMAND_REJECTION_REASON
        ));
    }

    #[tokio::test]
    async fn completed_snapshot_without_retained_outcomes_has_no_default_disposition() {
        let mut state = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, Some(1), 0);
        state.add_player(5, None).unwrap();
        state.status = GameStatus::Complete {
            winning_snake_id: None,
        };
        let (tx, mut rx) = mpsc::channel(2);

        assert!(
            send_completed_game_snapshot_from_resolved(
                &tx,
                42,
                5,
                &state,
                ResolvedCommandState::default(),
                None,
            )
            .await
        );
        assert!(matches!(
            decode_ws_message(rx.recv().await.unwrap()),
            WSMessage::GameEvent(_)
        ));
        assert!(matches!(
            decode_ws_message(rx.recv().await.unwrap()),
            WSMessage::CommandOutcomesComplete {
                game_id: 42,
                terminal_rejection_reason: None,
            }
        ));
    }
}

#[cfg(test)]
mod session_identity_tests {
    use super::new_session_id;

    /// Session ids must be unique and time-ordered so a session's events
    /// cluster in the analytics table.
    #[test]
    fn session_ids_are_unique_prefixed_and_sortable() {
        let mut ids = Vec::new();
        for _ in 0..20 {
            ids.push(new_session_id());
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert!(ids.iter().all(|id| id.starts_with("s_")));
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), ids.len(), "ids must be unique");
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(ids, sorted, "must sort in creation order");
    }
}

#[cfg(test)]
mod message_type_naming_tests {
    use super::*;
    use crate::ads::{AdBreakResolution, ClientAdsConfig};
    use crate::challenges::ChallengeInbox;
    use crate::presence::RegionRoster;
    use common::{CommandId, Direction, GameCommand, GameType, QueueMode};

    /// One value of every `WSMessage` variant.
    ///
    /// A variant added without a name does not reach this test at all —
    /// `WSMessage::variant_name` matches exhaustively, so the crate stops
    /// compiling first. This roster is what proves the names it returns are
    /// the ones that actually appear on the wire.
    fn every_variant() -> Vec<WSMessage> {
        vec![
            WSMessage::Token("jwt".to_owned()),
            WSMessage::Authenticate {
                token: "jwt".to_owned(),
                protocol_version: WS_PROTOCOL_VERSION,
                anon_id: None,
                distribution: None,
            },
            WSMessage::JoinGame(1),
            WSMessage::LeaveGame,
            WSMessage::GameCommandV2 {
                command_id: ClientCommandIdentityV2 {
                    game_id: 1,
                    user_id: 2,
                    client_game_session_id: "session".to_owned(),
                    sequence: 3,
                },
                command: GameCommandMessage {
                    command_id_client: CommandId {
                        tick: 1,
                        user_id: 2,
                        sequence_number: 3,
                    },
                    command_id_server: None,
                    command: GameCommand::ActivateBoost { snake_id: 1 },
                },
            },
            WSMessage::GameEvent(GameEventMessage {
                game_id: 1,
                tick: 2,
                sequence: 3,
                stream_seq: 4,
                user_id: None,
                event: GameEvent::SnakeTurned {
                    snake_id: 1,
                    direction: Direction::Up,
                },
            }),
            WSMessage::CommandOutcomes {
                game_id: 1,
                client_game_session_id: "session".to_owned(),
                contiguous_through: 0,
                outcomes: BTreeMap::new(),
                rejection_fence: None,
            },
            WSMessage::CommandOutcomesComplete {
                game_id: 1,
                terminal_rejection_reason: None,
            },
            WSMessage::Chat("hello".to_owned()),
            WSMessage::LobbyChatMessage {
                lobby_code: "ABCD".to_owned(),
                message_id: "m".to_owned(),
                user_id: 1,
                username: "u".to_owned(),
                message: "hi".to_owned(),
                timestamp_ms: 0,
            },
            WSMessage::GameChatMessage {
                game_id: 1,
                message_id: "m".to_owned(),
                user_id: 1,
                username: "u".to_owned(),
                message: "hi".to_owned(),
                timestamp_ms: 0,
            },
            WSMessage::LobbyChatHistory {
                lobby_code: "ABCD".to_owned(),
                messages: Vec::new(),
            },
            WSMessage::GameChatHistory {
                game_id: 1,
                messages: Vec::new(),
            },
            WSMessage::Authenticated {
                task_boot_id: "1:boot".to_owned(),
                protocol_version: WS_PROTOCOL_VERSION,
                capabilities: Vec::new(),
                socket_generation: 1,
            },
            WSMessage::AdConfiguration(ClientAdsConfig::default()),
            WSMessage::PlayerReady { game_id: 1 },
            WSMessage::RequestResync { game_id: 1 },
            WSMessage::Ping { client_time: 0 },
            WSMessage::Pong {
                client_time: 0,
                server_time: 0,
            },
            WSMessage::QueueForMatch {
                game_type: GameType::Solo,
                queue_mode: QueueMode::Quickmatch,
            },
            WSMessage::QueueForMatchMulti {
                game_types: Vec::new(),
                queue_mode: QueueMode::Competitive,
            },
            WSMessage::LeaveQueue,
            WSMessage::MatchFound { game_id: 1 },
            WSMessage::QueueUpdate {
                position: 1,
                estimated_wait_seconds: 2,
            },
            WSMessage::QueueLeft,
            WSMessage::AdBreakResolved {
                break_id: "b".to_owned(),
                resolution: AdBreakResolution::Completed,
            },
            WSMessage::UpdateNickname {
                nickname: "n".to_owned(),
            },
            WSMessage::SpectatorJoined,
            WSMessage::AccessDenied {
                reason: "no".to_owned(),
            },
            WSMessage::GameLoadFailed {
                game_id: 1,
                reason: "no".to_owned(),
            },
            WSMessage::GameWarming {
                game_id: 1,
                retry_after_ms: 250,
            },
            WSMessage::SoloGameCreated { game_id: 1 },
            WSMessage::Drain {
                task_boot_id: "1:boot".to_owned(),
                deadline_unix_ms: 0,
            },
            WSMessage::UserCountUpdate {
                region_counts: HashMap::new(),
            },
            WSMessage::CreateLobby,
            WSMessage::LobbyCreated {
                lobby_code: "ABCD".to_owned(),
            },
            WSMessage::JoinLobby {
                lobby_code: "ABCD".to_owned(),
                preferences: None,
            },
            WSMessage::JoinedLobby {
                lobby_code: "ABCD".to_owned(),
            },
            WSMessage::LeaveLobby,
            WSMessage::LeftLobby,
            WSMessage::LobbyUpdate {
                lobby_code: "ABCD".to_owned(),
                members: Vec::new(),
                host_user_id: 1,
                state: "waiting".to_owned(),
                preferences: lobby_manager::LobbyPreferences::default(),
                ad_break: None,
            },
            WSMessage::UpdateLobbyPreferences {
                selected_modes: Vec::new(),
                competitive: false,
            },
            WSMessage::LobbyRegionMismatch {
                target_region: "euw1".to_owned(),
                ws_url: "wss://euw1.example/ws".to_owned(),
                lobby_code: "ABCD".to_owned(),
            },
            WSMessage::OnlinePlayers(RegionRoster {
                region: "use1".to_owned(),
                players: Vec::new(),
                total_online: 0,
            }),
            WSMessage::ChallengePlayer { user_id: 1 },
            WSMessage::RespondToChallenge {
                challenge_id: "c".to_owned(),
                accept: true,
            },
            WSMessage::CancelChallenge {
                challenge_id: "c".to_owned(),
            },
            WSMessage::Challenges(ChallengeInbox::default()),
            WSMessage::ChallengeAccepted {
                challenge_id: "c".to_owned(),
                lobby_code: "ABCD".to_owned(),
            },
            WSMessage::ChallengeFailed {
                reason: "no".to_owned(),
            },
            WSMessage::SetRematchIntent {
                game_id: 1,
                opt_in: true,
            },
            WSMessage::Rematch(RematchState {
                game_id: 1,
                participants: Vec::new(),
                lobby_code: None,
                host_user_id: None,
                game_type: None,
                queue_mode: QueueMode::Quickmatch,
                expires_at_ms: 0,
            }),
        ]
    }

    /// The load-bearing property for the outbound hook: the forwarder never
    /// sees a `WSMessage`, so the name it reads off the wire has to be the
    /// same name the inbound hook reads off the value. If serde's
    /// representation ever changes — a container attribute, a renamed variant
    /// — this fails instead of quietly relabelling a column.
    #[test]
    fn a_serialized_frame_reports_its_own_variant_name() {
        for message in every_variant() {
            let expected = message.variant_name();
            let frame = Message::Text(
                serde_json::to_string(&message)
                    .expect("every variant must serialize")
                    .into(),
            );
            assert_eq!(
                outbound_message_type(&frame),
                expected,
                "the wire tag and the variant name disagree for {expected}"
            );
        }
    }

    #[test]
    fn every_variant_has_a_distinct_name() {
        let names: Vec<&str> = every_variant()
            .iter()
            .map(WSMessage::variant_name)
            .collect();
        let unique: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(
            unique.len(),
            names.len(),
            "names must be distinct: {names:?}"
        );
        assert_eq!(names.len(), 52, "every variant must be covered");
    }

    /// The names go into an analytics column, so they must stay inside the
    /// shape the wire-tag reader accepts — otherwise the two directions would
    /// label the same message differently.
    #[test]
    fn every_name_survives_the_wire_tag_reader() {
        for message in every_variant() {
            let name = message.variant_name();
            assert!(!name.is_empty());
            assert!(name.len() <= MAX_WIRE_TAG_LEN, "{name} is too long");
            assert_ne!(
                name, UNNAMED_MESSAGE_TYPE,
                "{name} collides with the fallback"
            );
        }
    }

    /// Transport frames carry no variant, and must not be confusable with the
    /// application message that shares their word.
    #[test]
    fn transport_frames_are_labelled_apart_from_application_messages() {
        let application_frame = Message::Text(
            serde_json::to_string(&WSMessage::Ping { client_time: 0 })
                .unwrap()
                .into(),
        );
        let transport_frame = Message::Ping(Vec::new().into());
        let application = outbound_message_type(&application_frame);
        let transport = outbound_message_type(&transport_frame);
        assert_eq!(application, "Ping");
        assert_eq!(transport, "ping_frame");
        assert_ne!(application, transport);
        assert_eq!(outbound_message_type(&Message::Close(None)), "close_frame");
    }

    /// The interning table and the match must describe the same set. An
    /// outbound frame is named by looking its wire tag up in the table, so a
    /// name the table was missing would be reported as unknown while the
    /// inbound half still named it — the two directions of one message would
    /// stop joining.
    #[test]
    fn the_interning_table_holds_exactly_the_variant_names() {
        let from_variants: std::collections::BTreeSet<&str> = every_variant()
            .iter()
            .map(WSMessage::variant_name)
            .collect();
        let from_table: std::collections::BTreeSet<&str> =
            WS_MESSAGE_TYPE_NAMES.iter().copied().collect();
        assert_eq!(from_variants, from_table);
    }

    /// A well-formed tag this gateway never authors is not a `WSMessage`, and
    /// collapses into the one bounded label rather than becoming a column
    /// value of its own.
    #[test]
    fn a_well_formed_tag_that_is_not_a_variant_is_not_recorded_verbatim() {
        assert_eq!(interned_message_type("GameEvent"), Some("GameEvent"));
        assert_eq!(interned_message_type("NotAWsMessage"), None);
        assert_eq!(
            outbound_message_type(&Message::Text("{\"NotAWsMessage\":1}".into())),
            UNNAMED_MESSAGE_TYPE
        );
    }

    /// A frame this gateway did not author must not become an unbounded or
    /// attacker-chosen column value.
    #[test]
    fn an_unreadable_frame_falls_back_to_one_bounded_label() {
        let oversized = format!("{{\"{}\":1}}", "A".repeat(MAX_WIRE_TAG_LEN + 1));
        for text in [
            String::new(),
            "not json".to_owned(),
            "{}".to_owned(),
            "{123:1}".to_owned(),
            "{\"has space\":1}".to_owned(),
            "{\"\":1}".to_owned(),
            oversized,
        ] {
            assert_eq!(
                outbound_message_type(&Message::Text(text.clone().into())),
                UNNAMED_MESSAGE_TYPE,
                "{text} must not be recorded verbatim"
            );
        }
    }
}

#[cfg(test)]
mod seat_publication_tests {
    use super::*;
    use crate::analytics::ws_sink::WsConnection;

    fn seated(game_id: Option<u32>) -> ConnectionState {
        ConnectionState::Authenticated {
            metadata: PlayerMetadata {
                user_id: 7,
                username: "Player".to_owned(),
                token: "session-token".to_owned(),
                is_guest: false,
                matchmaking_pool: MatchmakingPool::Public,
                supports_ad_break: true,
                can_show_video_ad: false,
                distribution: Some(ClientDistribution::Web),
            },
            lobby_handle: None,
            game_id,
            websocket_id: "ws-seat".to_owned(),
        }
    }

    /// The transition that reads this value goes on to spawn the game-event
    /// forwarder, whose very first frame is the anchor snapshot. Publishing
    /// therefore has to be complete before the value is returned: any window
    /// between the two would stamp the entire game-entry burst with the game
    /// this connection was in BEFORE, which on a rematch is a wrong join key
    /// rather than a missing one.
    #[test]
    fn entering_a_game_publishes_the_new_seat_before_returning_it() {
        let analytics = WsConnection::new("seat-order");
        analytics.set_game_id(Some(11));

        let entered = publish_seat(&seated(Some(77)), &analytics);

        assert_eq!(entered, Some(77));
        assert_eq!(
            analytics.game_id(),
            Some(77),
            "the caller cannot learn the new seat without the forwarder having \
             learned it too"
        );
    }

    #[test]
    fn leaving_a_game_clears_the_seat() {
        let analytics = WsConnection::new("seat-clear");
        analytics.set_game_id(Some(11));
        assert_eq!(publish_seat(&seated(None), &analytics), None);
        assert_eq!(analytics.game_id(), None);
    }

    /// A transition back to unauthenticated is a seat change too, and the
    /// frames that follow it belong to no game.
    #[test]
    fn an_unauthenticated_transition_clears_the_seat() {
        let analytics = WsConnection::new("seat-unauth");
        analytics.set_game_id(Some(11));
        assert_eq!(
            publish_seat(&ConnectionState::Unauthenticated, &analytics),
            None
        );
        assert_eq!(analytics.game_id(), None);
    }
}
