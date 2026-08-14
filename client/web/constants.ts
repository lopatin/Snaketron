// Game tick interval constants (matching Rust constants)
export const DEFAULT_TICK_INTERVAL_MS = 100;
// Informational parity with the production v2 actor. Gameplay timing always
// comes from GameProperties.tick_duration_ms, never from this poll cadence.
export const EXECUTOR_POLL_INTERVAL_MS = 10;
export const DEFAULT_CUSTOM_GAME_TICK_MS = 100;
// Gameplay protocol version. Predictive simulation requires an exact match:
// protocol 8 changes scoring and physical growth, so continuing with a stale
// engine would produce permanent body-geometry divergence.
// Tracks WS_PROTOCOL_VERSION in server/src/lifecycle.rs.
export const GAMEPLAY_PROTOCOL_VERSION = 8;
export const isGameplayProtocolCompatible = (serverVersion: unknown): boolean =>
  Number(serverVersion) === GAMEPLAY_PROTOCOL_VERSION;
export const GAMEPLAY_UPDATE_REQUIRED_PREFIX = 'Gameplay update required';
export const isGameplayUpdateRequiredReason = (reason: unknown): boolean =>
  typeof reason === 'string' && reason.startsWith(GAMEPLAY_UPDATE_REQUIRED_PREFIX);
export const buildGameplayAuthentication = (token: string) => ({
  Authenticate: {
    token,
    protocol_version: GAMEPLAY_PROTOCOL_VERSION,
  },
} as const);
// Game speed mappings
export const GAME_SPEED_TO_MS = {
  slow: 200,
  normal: DEFAULT_TICK_INTERVAL_MS,
  fast: 75,
  extreme: 50,
} as const;

// Replay viewer constants
export const SECONDS_PER_TICK = 1.0 / (1000 / DEFAULT_TICK_INTERVAL_MS);  // Based on default tick interval
