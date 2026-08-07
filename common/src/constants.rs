/// Default tick interval in milliseconds for game loops
pub const DEFAULT_TICK_INTERVAL_MS: u32 = 100;

/// Fixed simulation quantum for Boost-enabled duel and 2v2 team matches.
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
pub const BOOST_SPOT_LAYOUT_VERSION: u16 = 3;
pub const BOOST_RULES_VERSION: u16 = 2;

/// Production v2 actor poll interval in milliseconds.
///
/// Simulation cadence is stored per game; this only controls how frequently
/// the executor checks whether another quantum is due.
pub const EXECUTOR_POLL_INTERVAL_MS: u64 = 10;

/// Default tick duration for custom games in milliseconds
pub const DEFAULT_CUSTOM_GAME_TICK_MS: u32 = 100;

/// Default available food target
pub const DEFAULT_FOOD_TARGET: usize = 10;

/// Default time limit for team games in milliseconds (1 minute 30 seconds)
pub const DEFAULT_TEAM_TIME_LIMIT_MS: u32 = 90_000;

/// Countdown between the moment a match is cleared to begin and its first
/// simulated tick. Matchmaking uses it to anchor `GameState::start_ms`, and the
/// executor reuses it when the pre-match readiness gate resolves, so both
/// paths present the same "Starting in 3..." countdown to players.
pub const GAME_START_COUNTDOWN_MS: i64 = 3_000;

/// How long the pre-match readiness gate waits for every player to confirm
/// before starting anyway. A player who never answers — closed tab, lost
/// focus, walked away — must not be able to hold a match hostage.
pub const MATCH_READY_WINDOW_MS: i64 = 15_000;

/// Quickmatch time limit for team games in milliseconds (1 minute 30 seconds)
pub const DEFAULT_QUICKMATCH_TEAM_TIME_LIMIT_MS: u32 = 90_000;
