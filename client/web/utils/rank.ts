import type { Rank, RankDivision, RankTier } from '../types';

/**
 * Competitive ladder bands, shared by the leaderboard and the post-match
 * rating reveal. Both surfaces must agree on where a division starts and
 * ends, or the reveal could announce a promotion the leaderboard denies.
 */
export interface RankBand {
  min: number;
  /** Absent only on the open-ended top band. */
  max?: number;
  tier: RankTier;
  division: RankDivision;
}

export const RANK_BANDS: RankBand[] = [
  { min: 0, max: 200, tier: 'bronze', division: 1 },
  { min: 200, max: 400, tier: 'bronze', division: 2 },
  { min: 400, max: 600, tier: 'bronze', division: 3 },
  { min: 600, max: 800, tier: 'silver', division: 1 },
  { min: 800, max: 1000, tier: 'silver', division: 2 },
  { min: 1000, max: 1200, tier: 'silver', division: 3 },
  { min: 1200, max: 1300, tier: 'gold', division: 1 },
  { min: 1300, max: 1400, tier: 'gold', division: 2 },
  { min: 1400, max: 1500, tier: 'gold', division: 3 },
  // Elite tiers carry three divisions each; the tier boundaries
  // (1500/1900/2300) are unchanged, the bands inside are re-sliced.
  { min: 1500, max: 1633, tier: 'platinum', division: 1 },
  { min: 1633, max: 1766, tier: 'platinum', division: 2 },
  { min: 1766, max: 1900, tier: 'platinum', division: 3 },
  { min: 1900, max: 2033, tier: 'diamond', division: 1 },
  { min: 2033, max: 2166, tier: 'diamond', division: 2 },
  { min: 2166, max: 2300, tier: 'diamond', division: 3 },
  { min: 2300, max: 2400, tier: 'grandmaster', division: 1 },
  { min: 2400, max: 2500, tier: 'grandmaster', division: 2 },
  { min: 2500, tier: 'grandmaster', division: 3 },
];

export const rankBandIndexForMmr = (mmr: number): number => {
  const normalizedMmr = Math.max(0, mmr);
  const index = RANK_BANDS.findIndex(
    ({ min, max }) => normalizedMmr >= min && (max == null || normalizedMmr < max),
  );
  return index === -1 ? RANK_BANDS.length - 1 : index;
};

export const getRankFromMMR = (mmr: number): Rank => {
  const band = RANK_BANDS[rankBandIndexForMmr(mmr)];
  return {
    tier: band.tier,
    division: band.division,
    mmr: Math.max(0, mmr),
  };
};

const TIER_LABELS: Record<RankTier, string> = {
  bronze: 'Bronze',
  silver: 'Silver',
  gold: 'Gold',
  platinum: 'Platinum',
  diamond: 'Diamond',
  master: 'Master',
  grandmaster: 'Grand Master',
};

const DIVISION_LABELS = ['I', 'II', 'III'];

export const formatRankLabel = (rank: Rank): string => {
  const divisionLabel = DIVISION_LABELS[rank.division - 1] ?? '';
  return `${TIER_LABELS[rank.tier]} ${divisionLabel}`.trim();
};

/**
 * Where an MMR value sits inside its division band, in [0, 1). The
 * open-ended top band reuses the previous band's width so the meter keeps
 * moving rather than pinning at full.
 */
export const rankBandProgress = (mmr: number): number => {
  const band = RANK_BANDS[rankBandIndexForMmr(mmr)];
  const width = band.max != null
    ? band.max - band.min
    : RANK_BANDS[RANK_BANDS.length - 2].max! - RANK_BANDS[RANK_BANDS.length - 2].min;
  const offset = Math.max(0, mmr) - band.min;
  return Math.min(offset / width, 0.999);
};

export type RankMovement = 'promoted' | 'demoted' | 'unchanged';

/** Division-level movement between two MMR values. */
export const rankMovement = (beforeMmr: number, afterMmr: number): RankMovement => {
  const before = rankBandIndexForMmr(beforeMmr);
  const after = rankBandIndexForMmr(afterMmr);
  if (after > before) return 'promoted';
  if (after < before) return 'demoted';
  return 'unchanged';
};

/** Whether the movement crossed into a different tier, not just a division. */
export const crossedTier = (beforeMmr: number, afterMmr: number): boolean => (
  RANK_BANDS[rankBandIndexForMmr(beforeMmr)].tier !==
  RANK_BANDS[rankBandIndexForMmr(afterMmr)].tier
);
