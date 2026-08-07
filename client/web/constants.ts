// Game tick interval constants (matching Rust constants)
export const DEFAULT_TICK_INTERVAL_MS = 100;
// Informational parity with the production v2 actor. Gameplay timing always
// comes from GameProperties.tick_duration_ms, never from this poll cadence.
export const EXECUTOR_POLL_INTERVAL_MS = 10;
export const DEFAULT_CUSTOM_GAME_TICK_MS = 100;
// Hard-cutover gameplay protocol. The server accepts this exact version only;
// stale tabs receive "client update required" and must reload.
export const GAMEPLAY_PROTOCOL_VERSION = 6;
export const CLIENT_UPDATE_REQUIRED_REASON = 'Client update required';
export const isClientUpdateRequiredReason = (reason: unknown): boolean => (
  typeof reason === 'string' &&
  reason.trim().toLowerCase() === CLIENT_UPDATE_REQUIRED_REASON.toLowerCase()
);
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
