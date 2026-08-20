import type { ClientDistribution } from './types/generated/ClientDistribution';
import type { WSMessage } from './types/generated/WSMessage';

// Game tick interval constants (matching Rust constants)
export const DEFAULT_TICK_INTERVAL_MS = 100;
// Informational parity with the production v2 actor. Gameplay timing always
// comes from GameProperties.tick_duration_ms, never from this poll cadence.
export const EXECUTOR_POLL_INTERVAL_MS = 10;
export const DEFAULT_CUSTOM_GAME_TICK_MS = 100;
// Gameplay protocol version. Predictive simulation requires an exact match:
// Protocol 12 adds the rematch opt-in on the results card and its live roster.
// Protocol 11 adds the social layer — the per-region online-player roster and
// player-to-player challenges, both pushed by the server.
// Protocol 10 is a merge collision resolved upward: this branch and the
// advertising branch each shipped a wire change and each independently
// claimed 9, so 9 would name two mutually unintelligible protocols. 10
// carries both — deterministic death attribution on SnakeDied events, and
// per-session distribution routing for server-owned advertisement policy.
// (Protocol 8 changed scoring and physical growth.)
// Tracks WS_PROTOCOL_VERSION in server/src/lifecycle.rs.
export const GAMEPLAY_PROTOCOL_VERSION = 12;
export const isGameplayProtocolCompatible = (serverVersion: unknown): boolean =>
  Number(serverVersion) === GAMEPLAY_PROTOCOL_VERSION;
export const GAMEPLAY_UPDATE_REQUIRED_PREFIX = 'Gameplay update required';
export const isGameplayUpdateRequiredReason = (reason: unknown): boolean =>
  typeof reason === 'string' && reason.startsWith(GAMEPLAY_UPDATE_REQUIRED_PREFIX);

/**
 * Identify the release surface represented by this bundle. This is sent to
 * the server as routing context only: the server remains authoritative for
 * whether ads are enabled and which provider/configuration the session gets.
 */
export const resolveClientDistribution = (
  crazyGamesBuild: boolean,
  itchBuild: boolean,
): ClientDistribution => {
  if (crazyGamesBuild && itchBuild) {
    throw new Error('ITCH_BUILD and CRAZYGAMES_BUILD are mutually exclusive release targets');
  }
  if (crazyGamesBuild) {
    return 'crazygames';
  }
  if (itchBuild) {
    return 'itch';
  }
  return 'web';
};

export const CLIENT_DISTRIBUTION: ClientDistribution = resolveClientDistribution(
  process.env.CRAZYGAMES_BUILD === 'true',
  process.env.ITCH_BUILD === 'true',
);

// `anon_id` is additive and optional on both ends: an older server ignores the
// unknown field, and this server defaults it to absent for older clients, so it
// is deliberately NOT part of the version gate above.
export const buildGameplayAuthentication = (token: string, anonId?: string) => ({
  Authenticate: {
    token,
    protocol_version: GAMEPLAY_PROTOCOL_VERSION,
    distribution: CLIENT_DISTRIBUTION,
    ...(anonId ? { anon_id: anonId } : {}),
  },
} as const satisfies WSMessage);
// Game speed mappings
export const GAME_SPEED_TO_MS = {
  slow: 200,
  normal: DEFAULT_TICK_INTERVAL_MS,
  fast: 75,
  extreme: 50,
} as const;

// Replay viewer constants
export const SECONDS_PER_TICK = 1.0 / (1000 / DEFAULT_TICK_INTERVAL_MS);  // Based on default tick interval
