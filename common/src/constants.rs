/// Default tick interval in milliseconds for game loops
pub const DEFAULT_TICK_INTERVAL_MS: u32 = 100;

/// Fixed simulation quantum for every Boost-enabled matchmade mode.
pub const BOOST_TICK_INTERVAL_MS: u32 = 50;

/// Snake speed is represented in milli-normal units.
pub const NORMAL_SNAKE_SPEED_MILLI: u16 = 1_000;

/// V1's validated technical and gameplay ceiling is 2x normal speed.
pub const MAX_BOOST_SPEED_MILLI: u16 = 2_000;

/// Recommended Boost rules snapshotted into each eligible match.
pub const DEFAULT_BOOST_SPEED_MILLI: u16 = 1_500;
pub const DEFAULT_BOOST_CAPACITY_MS: u32 = 3_000;
/// Inner 1x1 packets hold exactly one quarter of the configured tank.
pub const DEFAULT_BOOST_PACKET_CHARGE_MS: u32 = DEFAULT_BOOST_CAPACITY_MS / 4;
pub const DEFAULT_BOOST_PAD_RESPAWN_MS: u32 = 8_000;
/// Boost pad layouts are identified, not ranked: the version selects which
/// geometry a match was built with, so several can coexist and a persisted
/// match keeps the map it started on. A layout's number is never reused for a
/// different shape — that is what lets validation reject a state whose pads do
/// not match the layout it claims.
///
/// v3 — the canonical 60x40 team map, drawn inside the end-zone-inset field.
/// v4 — the teamless 40x40 free-for-all map, drawn on the whole arena.
pub const BOOST_SPOT_LAYOUT_VERSION_TEAM: u16 = 3;
pub const BOOST_SPOT_LAYOUT_VERSION_FIELD: u16 = 4;
/// Layout carried by an unlimited tank, which has no pickups to place.
pub const BOOST_SPOT_LAYOUT_VERSION_NONE: u16 = 0;
pub const BOOST_RULES_VERSION: u16 = 2;

/// Production v2 actor poll interval in milliseconds.
///
/// Simulation cadence is stored per game; this only controls how frequently
/// the executor checks whether another quantum is due.
pub const EXECUTOR_POLL_INTERVAL_MS: u64 = 10;

/// Default tick duration for custom games in milliseconds
pub const DEFAULT_CUSTOM_GAME_TICK_MS: u32 = 100;

/// Compatibility fallback for snapshots that predate the inactivity fields
/// and games constructed outside server matchmaking. The production server
/// overwrites it with its resolved, snapshotted policy for every new match.
pub const DEFAULT_PLAYER_IDLE_TIMEOUT_MS: u32 = 60_000;

/// Compatibility countdown paired with `DEFAULT_PLAYER_IDLE_TIMEOUT_MS`.
/// Live clients derive the countdown from the authoritative match snapshot and
/// never use this value as a UI fallback.
pub const DEFAULT_PLAYER_IDLE_WARNING_MS: u32 = 10_000;

/// Default available food target
pub const DEFAULT_FOOD_TARGET: usize = 10;

/// Countdown between the moment a match is cleared to begin and its first
/// simulated tick. Matchmaking uses it to anchor `GameState::start_ms`, and the
/// executor reuses it when the pre-match readiness gate resolves, so both
/// paths present the same "Starting in 3..." countdown to players.
pub const GAME_START_COUNTDOWN_MS: i64 = 3_000;

/// How long the pre-match readiness gate waits for every player to confirm
/// before starting anyway. A player who never answers — closed tab, lost
/// focus, walked away — must not be able to hold a match hostage.
pub const MATCH_READY_WINDOW_MS: i64 = 15_000;

/// Team matches are raced to a score rather than played against a clock, so
/// they have no time limit and no maximum duration; the match clock counts up.
/// A team wins the moment its banked score reaches its queue's target.
pub const DEFAULT_QUICKMATCH_TEAM_SCORE_LIMIT: u32 = 25;
pub const DEFAULT_COMPETITIVE_TEAM_SCORE_LIMIT: u32 = 50;
