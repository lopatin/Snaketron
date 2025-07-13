// Game tick interval constants (matching Rust constants)
export const DEFAULT_TICK_INTERVAL_MS = 300;
export const EXECUTOR_POLL_INTERVAL_MS = 100;
export const DEFAULT_CUSTOM_GAME_TICK_MS = 200;
export const CLUSTER_RENEWAL_INTERVAL_MS = 300;

// Game speed mappings
export const GAME_SPEED_TO_MS = {
  slow: 500,
  normal: DEFAULT_TICK_INTERVAL_MS,
  fast: 200,
  extreme: 100,
} as const;

// Replay viewer constants
export const SECONDS_PER_TICK = 1.0 / (1000 / DEFAULT_TICK_INTERVAL_MS);  // Based on default tick interval