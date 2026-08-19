/**
 * Pure translation from Snaketron game state into GameAnalytics event shapes.
 *
 * Everything here is deliberately free of the SDK and of side effects, because
 * GameAnalytics silently drops malformed events and reports them back as SDK
 * errors rather than failing loudly. The taxonomy is worth pinning in tests,
 * so it lives apart from the transport that sends it.
 */

import type { DeathCause } from '../../types/generated/DeathCause';
import type { GameState } from '../../types/generated/GameState';
import type { GameType } from '../../types/generated/GameType';
import type { QueueMode } from '../../types/generated/QueueMode';

/**
 * GameAnalytics accepts only this alphabet in an event part, caps each part at
 * 64 characters, and rejects the whole event otherwise.
 * Mirrors `validateEventPartCharacters` in the SDK bundle.
 */
const GA_ALLOWED_CHARACTERS = /[^A-Za-z0-9\s\-_.()!?]/g;
const GA_MAX_PART_LENGTH = 64;
/** A design event id is at most five colon-separated parts. */
const GA_MAX_EVENT_ID_PARTS = 5;

/**
 * Force one identifier into the alphabet GameAnalytics accepts.
 *
 * Returns `null` for anything that cannot survive as a non-empty part, which
 * callers treat as "do not send this event" — an event with a placeholder
 * identifier is worse than a missing one, because it silently pollutes a
 * dimension that looks trustworthy in the dashboard.
 */
export const sanitizeEventPart = (raw: string): string | null => {
  const cleaned = raw
    .replace(GA_ALLOWED_CHARACTERS, '-')
    .slice(0, GA_MAX_PART_LENGTH)
    .trim();
  return cleaned.length > 0 ? cleaned : null;
};

/**
 * Join parts into a design event id, dropping the event entirely if any part
 * is unusable or if it would exceed GameAnalytics' five-part ceiling.
 */
export const buildEventId = (parts: readonly string[]): string | null => {
  if (parts.length === 0 || parts.length > GA_MAX_EVENT_ID_PARTS) {
    return null;
  }
  const sanitized = parts.map(sanitizeEventPart);
  return sanitized.every((part): part is string => part !== null)
    ? sanitized.join(':')
    : null;
};

/**
 * The match shape, as the top-level progression identifier. Custom games
 * report the mode their creator chose so a custom duel is comparable with a
 * matchmade one.
 */
export const gameTypeSlug = (gameType: GameType): string => {
  if (gameType === 'Solo') {
    return 'solo';
  }
  if ('TeamMatch' in gameType) {
    const perTeam = gameType.TeamMatch.per_team;
    return perTeam === 1 ? 'duel' : `team-${perTeam}v${perTeam}`;
  }
  if ('FreeForAll' in gameType) {
    return 'ffa';
  }

  const customMode = gameType.Custom.settings.game_mode;
  if (customMode === 'Solo') {
    return 'custom-solo';
  }
  if (customMode === 'Duel') {
    return 'custom-duel';
  }
  return 'custom-ffa';
};

/**
 * The queue a match came from. Custom games never enter matchmaking, so their
 * nominal `queue_mode` would otherwise misreport them as quickmatch volume.
 */
export const queueSlug = (gameType: GameType, queueMode: QueueMode): string => {
  if (typeof gameType === 'object' && 'Custom' in gameType) {
    return 'custom';
  }
  return queueMode === 'Competitive' ? 'competitive' : 'quickmatch';
};

/** GameAnalytics progression identifiers for one match. */
export interface MatchProgression {
  progression01: string;
  progression02: string;
}

export const matchProgression = (
  gameType: GameType,
  queueMode: QueueMode,
): MatchProgression | null => {
  const progression01 = sanitizeEventPart(gameTypeSlug(gameType));
  const progression02 = sanitizeEventPart(queueSlug(gameType, queueMode));
  if (!progression01 || !progression02) {
    return null;
  }
  return { progression01, progression02 };
};

/**
 * Collapse a death cause into a bounded, non-identifying slug.
 *
 * The killer's snake id is deliberately dropped: it is per-match and would
 * make the dimension unbounded without answering any question the aggregate
 * "how do players die" view is asking.
 */
export const deathCauseSlug = (cause: DeathCause): string => {
  if (typeof cause === 'string') {
    switch (cause) {
      case 'Wall':
        return 'wall';
      case 'OutOfBounds':
        return 'out-of-bounds';
      case 'EnemyBase':
        return 'enemy-base';
      case 'SelfCollision':
        return 'self';
      case 'Banked':
        return 'banked';
      default:
        return 'unknown';
    }
  }
  return 'SnakeBody' in cause ? 'enemy-body' : 'head-to-head';
};

/** One `queue:request` design event, before it is joined into an id. */
export interface QueueIntentEvent {
  queue: string;
  mode: string;
}

/**
 * Describe a matchmaking request as one event per game type the player
 * selected.
 *
 * Multi-mode queueing is a single socket message but several statements of
 * intent, and per-mode demand is the question this event exists to answer —
 * collapsing it to one "multi" bucket would hide exactly that.
 */
export const queueIntentEvents = (message: unknown): QueueIntentEvent[] => {
  if (!message || typeof message !== 'object') {
    return [];
  }

  const single = (message as { QueueForMatch?: { game_type: GameType; queue_mode: QueueMode } })
    .QueueForMatch;
  if (single) {
    return [{
      queue: queueSlug(single.game_type, single.queue_mode),
      mode: gameTypeSlug(single.game_type),
    }];
  }

  const multi = (message as {
    QueueForMatchMulti?: { game_types: GameType[]; queue_mode: QueueMode };
  }).QueueForMatchMulti;
  if (multi) {
    return multi.game_types.map((gameType) => ({
      queue: queueSlug(gameType, multi.queue_mode),
      mode: gameTypeSlug(gameType),
    }));
  }

  return [];
};

/** Whether the local player won the match they just finished. */
export type MatchOutcome = 'complete' | 'fail';

/**
 * Everything one finished match contributes to analytics.
 *
 * `score` is the local player's own score. GameAnalytics stores a progression
 * score as an integer, so a fractional value is rounded before it leaves here
 * rather than being silently truncated by the SDK.
 */
export interface MatchResultEvent extends MatchProgression {
  outcome: MatchOutcome;
  score: number;
  /** Match length in whole seconds, reported as a design-event value. */
  durationSeconds: number;
}

export interface LocalMatchSummary {
  score: number;
  isWinner: boolean;
}

/**
 * Build the finished-match event, or `null` when this session has nothing to
 * report — a spectator, an abandoned load, or a state whose identifiers do not
 * survive sanitization.
 */
export const buildMatchResultEvent = (
  gameState: Pick<GameState, 'game_type' | 'queue_mode'>,
  local: LocalMatchSummary | null,
  elapsedMs: number,
): MatchResultEvent | null => {
  if (!local) {
    return null;
  }
  const progression = matchProgression(gameState.game_type, gameState.queue_mode);
  if (!progression) {
    return null;
  }

  return {
    ...progression,
    outcome: local.isWinner ? 'complete' : 'fail',
    score: Math.max(0, Math.round(local.score)),
    durationSeconds: Math.max(0, Math.round(elapsedMs / 1000)),
  };
};
