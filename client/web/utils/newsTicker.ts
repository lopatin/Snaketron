import type { NewsTickerCtaAction } from '../types/generated';
import type { LobbyPreferences } from '../types';

const DEFAULT_POLL_INTERVAL_MS = 60_000;
const MIN_POLL_INTERVAL_MS = 30_000;
const MAX_POLL_INTERVAL_MS = 300_000;

export type NewsTickerPlayAction = Exclude<
  NewsTickerCtaAction,
  'viewLeaderboards'
>;

/** Translate a server-authored play CTA into one exact lobby configuration. */
export const getTickerPlayPreferences = (
  action: NewsTickerPlayAction,
): LobbyPreferences => {
  switch (action) {
    case 'playSolo':
      return { selectedModes: ['solo'], competitive: false };
    case 'playRankedSolo':
      return { selectedModes: ['solo'], competitive: true };
    case 'playDuel':
      return { selectedModes: ['duel'], competitive: false };
    case 'playTwoVsTwo':
      return { selectedModes: ['2v2'], competitive: false };
    case 'playFfa':
      return { selectedModes: ['ffa'], competitive: false };
    case 'playRankedDuel':
      return { selectedModes: ['duel'], competitive: true };
    case 'playRankedTwoVsTwo':
      return { selectedModes: ['2v2'], competitive: true };
    case 'playRankedFfa':
      return { selectedModes: ['ffa'], competitive: true };
  }
};

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
