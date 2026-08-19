import { useEffect, useState } from 'react';
import type { GameState, Rank } from '../types';
import { api } from '../services/api';
import { loadRegionPreference } from '../utils/regionPreference';
import { getRankFromMMR } from '../utils/rank';
import {
  postMatchRatingGameTypeParam,
  queueModeParam,
} from '../utils/ratingReveal';

/**
 * The ladder rank of whoever earned the Play of the Game.
 *
 * A highlight clip carries the star's name but not their standing, and the
 * star is usually not the local player, so the badge cannot come from the
 * local rating reveal. Rankings are public — the leaderboard already
 * publishes MMR next to a username — so this reads the star's row directly.
 *
 * `localRank` short-circuits the common self-star case: the rating reveal has
 * already fetched exactly this number, and re-asking for it would spend a
 * round trip on data the card is holding.
 *
 * A failed or absent read yields null and the caption simply drops the badge.
 * Rendering "unranked" on a timeout is how a real Grand Master ends up
 * advertised as unranked in their own highlight.
 */
export const useStarRank = (
  starUserId: number | undefined,
  gameState: GameState | null,
  localUserId: number | undefined,
  localRank: Rank | null,
): Rank | null => {
  const [rank, setRank] = useState<Rank | null>(null);

  const queueMode = gameState?.queue_mode ?? null;
  const gameTypeParam = postMatchRatingGameTypeParam(
    queueMode ?? undefined,
    gameState?.game_type,
  );
  const isLocalStar = starUserId !== undefined && starUserId === localUserId;

  useEffect(() => {
    if (starUserId === undefined) {
      setRank(null);
      return undefined;
    }
    if (isLocalStar) {
      setRank(localRank);
      return undefined;
    }
    if (gameTypeParam === null || queueMode === null) {
      setRank(null);
      return undefined;
    }

    let active = true;
    setRank(null);
    void api
      .getUserRanking(
        starUserId,
        queueModeParam(queueMode),
        gameTypeParam,
        undefined,
        loadRegionPreference()?.regionId,
      )
      .then((response) => {
        if (!active) return;
        setRank(response.mmr == null ? null : getRankFromMMR(response.mmr));
      })
      .catch(() => {
        if (active) setRank(null);
      });

    return () => {
      active = false;
    };
  }, [gameTypeParam, isLocalStar, localRank, queueMode, starUserId]);

  return rank;
};
