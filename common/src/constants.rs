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

/// A player who sends no gameplay input for this long is removed from the
/// match. Keeping this in authoritative simulation time makes the decision
/// deterministic across executor failover and client prediction.
pub const DEFAULT_PLAYER_IDLE_TIMEOUT_MS: u32 = 60_000;

/// The local client begins warning a player this long before the authoritative
/// inactivity deadline. The warning is derived from snapshotted match state,
/// so it cannot drift away from the server's decision.
pub const DEFAULT_PLAYER_IDLE_WARNING_MS: u32 = 10_000;

/// Default available food target
pub const DEFAULT_FOOD_TARGET: usize = 10;

/// Team matches are raced to a score rather than played against a clock, so
/// they have no time limit and no maximum duration; the match clock counts up.
/// A team wins the moment its banked score reaches its queue's target.
pub const DEFAULT_QUICKMATCH_TEAM_SCORE_LIMIT: u32 = 25;
pub const DEFAULT_COMPETITIVE_TEAM_SCORE_LIMIT: u32 = 50;
