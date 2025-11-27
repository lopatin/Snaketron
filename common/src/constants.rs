/// Default tick interval in milliseconds for game loops
pub const DEFAULT_TICK_INTERVAL_MS: u32 = 100;

/// Default tick interval for game executor polling in milliseconds
pub const EXECUTOR_POLL_INTERVAL_MS: u64 = 50;

/// Default tick duration for custom games in milliseconds
pub const DEFAULT_CUSTOM_GAME_TICK_MS: u32 = 100;

/// Default available food target
pub const DEFAULT_FOOD_TARGET: usize = 10;

/// Default time limit for team games in milliseconds (1 minute 30 seconds)
pub const DEFAULT_TEAM_TIME_LIMIT_MS: u32 = 90_000;

/// Quickmatch time limit for team games in milliseconds (1 minute 30 seconds)
pub const DEFAULT_QUICKMATCH_TEAM_TIME_LIMIT_MS: u32 = 90_000;

/// Default interval for cluster singleton renewal in milliseconds
pub const CLUSTER_RENEWAL_INTERVAL_MS: u64 = 150;
