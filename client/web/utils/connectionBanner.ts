/**
 * Timing and predicate rules for the "Connecting to game server…" badge.
 *
 * Kept pure and DOM-free so the behaviour is unit-testable: the badge's whole
 * job is to be honest about whether the player can act, and to be calm enough
 * that a momentary transport gap never reads as a fault.
 */

/**
 * How long the client must look unusable before the badge appears.
 *
 * A healthy socket opens well inside this window, so a normal page load and a
 * normal reconnect both stay silent.
 */
export const CONNECTION_BANNER_SHOW_DELAY_MS = 800;

/**
 * How long the badge stays up once shown, even if the client recovers
 * immediately. Without a floor, a recovery landing just after the badge
 * appears produces exactly the blink the badge is meant to explain.
 */
export const CONNECTION_BANNER_MIN_VISIBLE_MS = 1200;

export interface ConnectionReadinessInput {
  /** The transport is open. */
  isConnected: boolean;
  /** The server has answered `Authenticate` for the current identity. */
  isSessionAuthenticated: boolean;
  /** There is a player identity to authenticate (a guest or a real account). */
  hasIdentity: boolean;
}

/**
 * Whether the client is ready for what the player can do right now.
 *
 * An anonymous visitor has nothing to authenticate — the socket exists to carry
 * player counts — so an open transport is ready. Once there is an identity, an
 * open-but-unauthenticated socket cannot carry lobby or matchmaking commands,
 * so it is not ready and saying otherwise would be a lie.
 */
export function isConnectionReady(input: ConnectionReadinessInput): boolean {
  return input.hasIdentity
    ? input.isConnected && input.isSessionAuthenticated
    : input.isConnected;
}

/**
 * Remaining hold time before a visible badge may be taken down. Returns 0 when
 * the badge is not showing or its minimum has already elapsed, and is clamped
 * so a backwards clock jump cannot pin the badge for longer than the floor.
 */
export function connectionBannerHideDelayMs(shownAtMs: number | null, nowMs: number): number {
  if (shownAtMs === null || !Number.isFinite(shownAtMs) || !Number.isFinite(nowMs)) {
    return 0;
  }
  const elapsedMs = nowMs - shownAtMs;
  return Math.max(
    0,
    Math.min(CONNECTION_BANNER_MIN_VISIBLE_MS, CONNECTION_BANNER_MIN_VISIBLE_MS - elapsedMs),
  );
}
