// Game tick interval constants (matching Rust constants)
export const DEFAULT_TICK_INTERVAL_MS = 100;
// Informational parity with the production v2 actor. Gameplay timing always
// comes from GameProperties.tick_duration_ms, never from this poll cadence.
export const EXECUTOR_POLL_INTERVAL_MS = 10;
export const DEFAULT_CUSTOM_GAME_TICK_MS = 100;
// Gameplay protocol version, reported to the server for observability only.
// It is advisory on both ends: the server admits every version, and a client
// is never blocked or asked to reload over a mismatch. A shipped build cannot
// update itself — an itch.io bundle has no reload-to-upgrade path — so every
// gameplay protocol change must stay backwards compatible instead.
// Tracks WS_PROTOCOL_VERSION in server/src/lifecycle.rs.
export const GAMEPLAY_PROTOCOL_VERSION = 7;
// `anon_id` is additive and optional on both ends: an older server ignores the
// unknown field, and this server defaults it to absent for older clients. That
// keeps the rule above intact — a shipped bundle that cannot update itself must
// never be broken by a protocol addition, so the version is deliberately NOT
// bumped for a backwards-compatible field.
export const buildGameplayAuthentication = (token: string, anonId?: string) => ({
  Authenticate: {
    token,
    protocol_version: GAMEPLAY_PROTOCOL_VERSION,
    ...(anonId ? { anon_id: anonId } : {}),
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
