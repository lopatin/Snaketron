import { useEffect, useRef, useState } from 'react';
import type { GameState } from '../types';
import { api } from '../services/api';
import { loadRegionPreference } from '../utils/regionPreference';
import {
  buildRatingReveal,
  queueModeParam,
  ratedGameTypeParam,
  ratingHasSettled,
  snapshotFromResponse,
  RATING_POLL_INTERVAL_MS,
  RATING_POLL_MAX_ATTEMPTS,
  type MatchRatingState,
  type RatingSnapshot,
} from '../utils/ratingReveal';

/**
 * Drives the post-match rating reveal for one game.
 *
 * A baseline ranking snapshot is fetched while the match is still running;
 * after completion the endpoint is polled until the persisted MMR write
 * lands (see `ratingHasSettled`). Joining a game that is already complete
 * yields no trustworthy baseline, so the reveal downgrades to showing the
 * current standing without inventing a delta.
 */
export const useMatchRating = (
  gameId: string,
  gameState: GameState | null,
  isComplete: boolean,
  userId: number | undefined,
): MatchRatingState => {
  const [state, setState] = useState<MatchRatingState>({ phase: 'idle' });

  const gameTypeParam = ratedGameTypeParam(gameState?.game_type);
  const queueMode = gameState?.queue_mode ?? null;
  const eligible = gameTypeParam !== null && queueMode !== null && userId !== undefined;

  // Baseline for the current gameId. `undefined` = not fetched yet;
  // null = fetched but the player has no row on this ladder (or the fetch
  // happened too late to trust) — distinguished by `baselineTrusted`.
  const baselineRef = useRef<{
    gameId: string;
    snapshot: RatingSnapshot | null;
    trusted: boolean;
  } | null>(null);

  useEffect(() => {
    if (!eligible || gameTypeParam === null || queueMode === null) {
      return;
    }
    if (baselineRef.current?.gameId === gameId) {
      return;
    }

    // Joining an already-complete game (reconnect, shared link): the DB may
    // already hold the post-match value, so a snapshot taken now cannot
    // anchor a delta.
    const trusted = !isComplete;
    const controller = new AbortController();
    baselineRef.current = { gameId, snapshot: null, trusted };

    api
      .getMyRanking(
        queueModeParam(queueMode),
        gameTypeParam,
        undefined,
        loadRegionPreference()?.regionId,
      )
      .then((response) => {
        if (controller.signal.aborted) return;
        baselineRef.current = {
          gameId,
          snapshot: snapshotFromResponse(response),
          trusted,
        };
      })
      .catch(() => {
        // Keep the null baseline: the reveal will present the post-match
        // standing without a delta rather than blocking on this fetch.
      });

    return () => controller.abort();
  }, [eligible, gameId, gameTypeParam, isComplete, queueMode]);

  useEffect(() => {
    if (!eligible || gameTypeParam === null || queueMode === null || !isComplete) {
      setState({ phase: 'idle' });
      return;
    }
    const ladderQueueMode = queueMode;
    const ladderGameType = gameTypeParam;

    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | null = null;
    setState({ phase: 'pending' });

    const baseline = baselineRef.current?.gameId === gameId
      ? baselineRef.current
      : { gameId, snapshot: null, trusted: false };

    const finish = (latest: RatingSnapshot | null) => {
      if (cancelled) return;
      if (latest === null) {
        // No row even after the poll budget: a rating for this ladder does
        // not exist (e.g. the write failed server-side). Nothing to reveal.
        setState({ phase: 'unavailable' });
        return;
      }
      const before = baseline.trusted ? baseline.snapshot : null;
      setState({
        phase: 'ready',
        reveal: buildRatingReveal(ladderQueueMode, ladderGameType, before, latest),
      });
    };

    const poll = (attempt: number, lastSeen: RatingSnapshot | null) => {
      api
        .getMyRanking(
          queueModeParam(ladderQueueMode),
          ladderGameType,
          undefined,
          loadRegionPreference()?.regionId,
        )
        .then((response) => {
          if (cancelled) return;
          const latest = snapshotFromResponse(response);
          const settled = baseline.trusted
            ? ratingHasSettled(baseline.snapshot, latest)
            : latest !== null;
          if (settled || attempt >= RATING_POLL_MAX_ATTEMPTS) {
            // A zero-delta draw never writes, so the poll budget running out
            // with an unchanged row still ends in a (unchanged) reveal.
            finish(latest ?? lastSeen);
            return;
          }
          timer = setTimeout(() => poll(attempt + 1, latest), RATING_POLL_INTERVAL_MS);
        })
        .catch(() => {
          if (cancelled) return;
          if (attempt >= RATING_POLL_MAX_ATTEMPTS) {
            finish(lastSeen);
            return;
          }
          timer = setTimeout(() => poll(attempt + 1, lastSeen), RATING_POLL_INTERVAL_MS);
        });
    };

    poll(1, null);

    return () => {
      cancelled = true;
      if (timer !== null) {
        clearTimeout(timer);
      }
    };
  }, [eligible, gameId, gameTypeParam, isComplete, queueMode]);

  return state;
};
