import type { NewsTickerCtaAction } from '../types/generated';
import type { LobbyGameMode, LobbyPreferences } from '../types';

const DEFAULT_POLL_INTERVAL_MS = 60_000;
const MIN_POLL_INTERVAL_MS = 30_000;
const MAX_POLL_INTERVAL_MS = 300_000;

export type NewsTickerPlayAction = Exclude<
  NewsTickerCtaAction,
  'viewLeaderboards'
>;

/**
 * The mode half of a play CTA. The casual and ranked forms of an action name
 * the same mode; only the queue the headline described differs, and a CTA does
 * not replay that queue.
 *
 * Deliberately exhaustive with no `default`, so a new server-side action fails
 * the type check here instead of silently falling through to a wrong mode.
 */
const getTickerPlayMode = (action: NewsTickerPlayAction): LobbyGameMode => {
  switch (action) {
    case 'playSolo':
    case 'playRankedSolo':
      return 'solo';
    case 'playDuel':
    case 'playRankedDuel':
      return 'duel';
    case 'playTwoVsTwo':
    case 'playRankedTwoVsTwo':
      return '2v2';
    case 'playFfa':
    case 'playRankedFfa':
      return 'ffa';
  }
};

/**
 * Translate a server-authored play CTA into one exact lobby configuration.
 *
 * Every play CTA is a one-click entry into ranked, so only the mode is read
 * off the action: a headline about a casual game still invites the player into
 * the competitive queue for that mode. The server keeps authoring the
 * casual/ranked halves of the action set because its own copy is written from
 * the observed cohort — the client is what decides where the link lands.
 */
export const getTickerPlayPreferences = (
  action: NewsTickerPlayAction,
): LobbyPreferences => ({
  selectedModes: [getTickerPlayMode(action)],
  competitive: true,
});

export const getTickerPollIntervalMs = (
  refreshAfterSeconds: number,
): number => {
  if (!Number.isFinite(refreshAfterSeconds)) {
    return DEFAULT_POLL_INTERVAL_MS;
  }

  return Math.min(
    MAX_POLL_INTERVAL_MS,
    Math.max(MIN_POLL_INTERVAL_MS, refreshAfterSeconds * 1000),
  );
};

export const getTickerGroupCopies = (
  groupWidth: number,
  viewportWidth: number,
): number => {
  if (groupWidth <= 0 || viewportWidth <= 0) {
    return 2;
  }

  // One group scrolls away during each loop. Everything after it must still
  // span the viewport, including when the feed contains a single short item.
  return Math.max(2, Math.ceil(viewportWidth / groupWidth) + 1);
};
