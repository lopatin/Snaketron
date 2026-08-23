/**
 * Build-time GameAnalytics configuration.
 *
 * The keys are compiled in by webpack's DefinePlugin rather than fetched, so a
 * bundle either has analytics or provably does not. A checkout with no keys —
 * every developer machine, CI, and any fork — produces a bundle that never
 * loads the SDK at all, which is what keeps local play out of the live game's
 * numbers without anyone having to remember a switch.
 */

import { CLIENT_DISTRIBUTION } from '../../constants.ts';
import type { ClientDistribution } from '../../types/generated/ClientDistribution';

export interface AnalyticsBuildConfig {
  gameKey: string;
  secretKey: string;
  /**
   * The build label every event is tagged with, so a regression can be traced
   * to the release that introduced it. GameAnalytics wants a version-shaped
   * string here.
   */
  build: string;
}

/**
 * Whether this distribution reports at all.
 *
 * Every distribution does by default, including the itch and CrazyGames
 * packages: both portals permit third-party game analytics, and CrazyGames
 * publishes an analytics partner of its own. Portal traffic is the larger
 * audience, so excluding it would hide most of the players.
 *
 * `GAME_ANALYTICS_DISABLE_EMBEDDED=true` takes it back out of the reviewed
 * release packages without touching the ordinary website build — for a portal
 * whose policy changes, or a submission that must carry no third-party SDK.
 * The website build is never affected by that switch.
 */
export const resolveEmbeddedAnalyticsSupport = (input: {
  distribution: ClientDistribution;
  disableEmbedded: boolean;
}): boolean => !(input.distribution !== 'web' && input.disableEmbedded);

export const ANALYTICS_SUPPORTED_DISTRIBUTION = resolveEmbeddedAnalyticsSupport({
  distribution: CLIENT_DISTRIBUTION,
  disableEmbedded: process.env.GAME_ANALYTICS_DISABLE_EMBEDDED === 'true',
});

const readKey = (value: string | undefined): string => (value ?? '').trim();

/**
 * The configured keys, or `null` when this bundle has none.
 *
 * Both keys are required: GameAnalytics signs every payload with the secret,
 * so a bundle carrying only one of them would fail every request at runtime
 * instead of staying quietly inert.
 */
export const resolveAnalyticsBuildConfig = (
  gameKey: string | undefined,
  secretKey: string | undefined,
  build: string | undefined,
): AnalyticsBuildConfig | null => {
  const resolvedGameKey = readKey(gameKey);
  const resolvedSecretKey = readKey(secretKey);
  if (!resolvedGameKey || !resolvedSecretKey) {
    return null;
  }

  return {
    gameKey: resolvedGameKey,
    secretKey: resolvedSecretKey,
    build: readKey(build) || '0.0.0',
  };
};

export const ANALYTICS_BUILD_CONFIG = resolveAnalyticsBuildConfig(
  process.env.GAME_ANALYTICS_GAME_KEY,
  process.env.GAME_ANALYTICS_SECRET_KEY,
  process.env.GAME_ANALYTICS_BUILD,
);

/**
 * Custom dimensions must be declared before `initialize`, and GameAnalytics
 * rejects any value not in the declared set. Keeping the vocabularies here
 * means a new value cannot be sent without also being declared.
 */
export const ACCOUNT_DIMENSIONS = ['guest', 'registered'] as const;
export const INPUT_DIMENSIONS = ['keyboard', 'touch'] as const;

export type AccountDimension = (typeof ACCOUNT_DIMENSIONS)[number];
export type InputDimension = (typeof INPUT_DIMENSIONS)[number];
