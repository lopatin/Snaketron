import type { GameType, QueueMode, Rank, UserRankingResponse } from '../types';
import {
  crossedTier,
  getRankFromMMR,
  rankBandProgress,
  rankMovement,
  type RankMovement,
} from './rank.ts';

/**
 * Data flow behind the post-match rating reveal.
 *
 * The server persists MMR as a completion effect with no push to the client,
 * so the reveal is reconstructed from the REST ranking endpoint: a baseline
 * snapshot taken while the match is still running, then a short poll after
 * completion until the persisted numbers move. Everything here is pure so the
 * scheduling hook stays a thin shell around timers.
 */

/** Ladder game types the ranking API can answer for. */
export type RatedGameTypeParam = 'duel' | '2v2' | 'ffa';

export const ratedGameTypeParam = (gameType: GameType | undefined): RatedGameTypeParam | null => {
  if (!gameType || gameType === 'Solo' || typeof gameType !== 'object') {
    return null;
  }
  if ('TeamMatch' in gameType) {
    if (gameType.TeamMatch.per_team === 1) return 'duel';
    if (gameType.TeamMatch.per_team === 2) return '2v2';
    return null;
  }
  if ('FreeForAll' in gameType) {
    return 'ffa';
  }
  // Custom games persist under their own ranking bucket that the API cannot
  // query, so the reveal stays out of the way rather than guessing.
  return null;
};

export const queueModeParam = (queueMode: QueueMode): 'competitive' | 'quickmatch' => (
  queueMode === 'Competitive' ? 'competitive' : 'quickmatch'
);

export interface RatingSnapshot {
  mmr: number;
  wins: number;
  losses: number;
  /** Leaderboard position, when the API computed one. */
  position: number | null;
}

export const snapshotFromResponse = (
  response: UserRankingResponse,
): RatingSnapshot | null => (
  response.mmr == null
    ? null
    : {
      mmr: response.mmr,
      wins: response.wins ?? 0,
      losses: response.losses ?? 0,
      position: response.rank,
    }
);

/**
 * Whether a polled snapshot shows the match's persistence landing. Any of the
 * three counters moving counts: a drawn match can leave MMR untouched while
 * still recording the game, and a zero-delta write records nothing at all —
 * that case exits via the poll budget instead.
 */
export const ratingHasSettled = (
  baseline: RatingSnapshot | null,
  latest: RatingSnapshot | null,
): boolean => {
  if (latest === null) {
    return false;
  }
  if (baseline === null) {
    // First rated match on this ladder: the row appearing is the signal.
    return true;
  }
  return (
    latest.mmr !== baseline.mmr ||
    latest.wins !== baseline.wins ||
    latest.losses !== baseline.losses
  );
};

export const RATING_POLL_INTERVAL_MS = 900;
export const RATING_POLL_MAX_ATTEMPTS = 7;

/** Odometer pacing: small nudges read quickly, big swings get room to breathe. */
export const countDurationMs = (delta: number): number => (
  Math.min(1600, Math.max(900, 700 + Math.abs(delta) * 18))
);

export interface RatingReveal {
  queueMode: QueueMode;
  gameTypeParam: RatedGameTypeParam;
  after: RatingSnapshot;
  /** Null when this was the player's first rated match on the ladder. */
  before: RatingSnapshot | null;
  /** Null exactly when `before` is null. */
  delta: number | null;
  /** Division movement; only announced for competitive matches. */
  movement: RankMovement;
  crossedTier: boolean;
  fromRank: Rank;
  toRank: Rank;
  /** Fill fractions of the division meter, in [0, 1). */
  fromProgress: number;
  toProgress: number;
}

export const buildRatingReveal = (
  queueMode: QueueMode,
  gameTypeParam: RatedGameTypeParam,
  before: RatingSnapshot | null,
  after: RatingSnapshot,
): RatingReveal => {
  const beforeMmr = before?.mmr ?? after.mmr;
  const competitive = queueMode === 'Competitive';
  return {
    queueMode,
    gameTypeParam,
    after,
    before,
    delta: before === null ? null : after.mmr - before.mmr,
    movement: competitive ? rankMovement(beforeMmr, after.mmr) : 'unchanged',
    crossedTier: competitive && crossedTier(beforeMmr, after.mmr),
    fromRank: getRankFromMMR(beforeMmr),
    toRank: getRankFromMMR(after.mmr),
    fromProgress: rankBandProgress(beforeMmr),
    toProgress: rankBandProgress(after.mmr),
  };
};

export type MatchRatingState =
  /** Not a rated ladder match, or no authenticated player. */
  | { phase: 'idle' }
  /** Match finished; waiting for the persisted numbers to land. */
  | { phase: 'pending' }
  | { phase: 'ready'; reveal: RatingReveal }
  /** Ranking API unreachable or never produced a row. */
  | { phase: 'unavailable' };
