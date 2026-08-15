import assert from 'node:assert/strict';
import test from 'node:test';
import {
  crossedTier,
  formatRankLabel,
  getRankFromMMR,
  rankBandIndexForMmr,
  rankBandProgress,
  rankMovement,
  RANK_BANDS,
} from '../../utils/rank.ts';
import {
  buildRatingReveal,
  countDurationMs,
  postMatchRatingGameTypeParam,
  queueModeParam,
  ratedGameTypeParam,
  ratingHasSettled,
  snapshotFromResponse,
  type RatingSnapshot,
} from '../../utils/ratingReveal.ts';
import type { GameType } from '../../types/index.ts';

test('rank bands tile the ladder with no gaps or overlaps', () => {
  for (let i = 0; i < RANK_BANDS.length - 1; i++) {
    assert.equal(
      RANK_BANDS[i].max,
      RANK_BANDS[i + 1].min,
      `band ${i} must end exactly where band ${i + 1} starts`,
    );
  }
  assert.equal(RANK_BANDS[0].min, 0);
  assert.equal(RANK_BANDS[RANK_BANDS.length - 1].max, undefined);
});

test('band boundaries resolve to the higher band, matching the leaderboard', () => {
  assert.deepEqual(getRankFromMMR(0), { tier: 'bronze', division: 1, mmr: 0 });
  assert.deepEqual(getRankFromMMR(199), { tier: 'bronze', division: 1, mmr: 199 });
  assert.deepEqual(getRankFromMMR(200), { tier: 'bronze', division: 2, mmr: 200 });
  assert.deepEqual(getRankFromMMR(1199), { tier: 'silver', division: 3, mmr: 1199 });
  assert.deepEqual(getRankFromMMR(1200), { tier: 'gold', division: 1, mmr: 1200 });
  assert.deepEqual(getRankFromMMR(2600), { tier: 'grandmaster', division: 3, mmr: 2600 });
  assert.deepEqual(getRankFromMMR(9999), { tier: 'grandmaster', division: 3, mmr: 9999 });
  // Negative MMR cannot leave the ladder.
  assert.deepEqual(getRankFromMMR(-50), { tier: 'bronze', division: 1, mmr: 0 });
});

test('rank labels use roman numeral divisions', () => {
  assert.equal(formatRankLabel(getRankFromMMR(1250)), 'Gold I');
  assert.equal(formatRankLabel(getRankFromMMR(1450)), 'Gold III');
  assert.equal(formatRankLabel(getRankFromMMR(2650)), 'Grand Master III');
});

test('band progress is the fraction of the current division climbed', () => {
  assert.equal(rankBandProgress(1250), 0.5); // gold I spans 1200–1300
  assert.equal(rankBandProgress(600), 0); // exact promotion floor
  assert.ok(rankBandProgress(1299) > 0.98);
  // The open-ended top band borrows the previous band's width and never
  // reports full, so the meter always has somewhere left to go.
  assert.ok(rankBandProgress(2500) === 0);
  assert.ok(rankBandProgress(999999) < 1);
});

test('division movement and tier crossings are detected from raw MMR', () => {
  assert.equal(rankMovement(1195, 1210), 'promoted');
  assert.equal(rankMovement(1210, 1195), 'demoted');
  assert.equal(rankMovement(1210, 1250), 'unchanged');
  assert.equal(rankMovement(1250, 1250), 'unchanged');

  assert.equal(crossedTier(1195, 1210), true); // silver → gold
  assert.equal(crossedTier(1250, 1350), false); // gold I → gold II
  assert.equal(
    rankBandIndexForMmr(1250) + 1,
    rankBandIndexForMmr(1350),
    'gold I and gold II are adjacent bands',
  );
});

test('only ladder game types map to a ranking query parameter', () => {
  const duel: GameType = { TeamMatch: { per_team: 1 } };
  const twoVTwo: GameType = { TeamMatch: { per_team: 2 } };
  const bigTeams: GameType = { TeamMatch: { per_team: 3 } };
  const ffa: GameType = { FreeForAll: { max_players: 8 } };

  assert.equal(ratedGameTypeParam(duel), 'duel');
  assert.equal(ratedGameTypeParam(twoVTwo), '2v2');
  assert.equal(ratedGameTypeParam(ffa), 'ffa');
  assert.equal(ratedGameTypeParam('Solo'), null);
  assert.equal(ratedGameTypeParam(bigTeams), null);
  assert.equal(ratedGameTypeParam(undefined), null);

  assert.equal(queueModeParam('Competitive'), 'competitive');
  assert.equal(queueModeParam('Quickmatch'), 'quickmatch');
});

test('post-match rating progress is limited to competitive ladder matches', () => {
  const duel: GameType = { TeamMatch: { per_team: 1 } };
  const twoVTwo: GameType = { TeamMatch: { per_team: 2 } };
  const ffa: GameType = { FreeForAll: { max_players: 8 } };

  assert.equal(postMatchRatingGameTypeParam('Competitive', duel), 'duel');
  assert.equal(postMatchRatingGameTypeParam('Competitive', twoVTwo), '2v2');
  assert.equal(postMatchRatingGameTypeParam('Competitive', ffa), 'ffa');
  assert.equal(postMatchRatingGameTypeParam('Quickmatch', duel), null);
  assert.equal(postMatchRatingGameTypeParam('Quickmatch', twoVTwo), null);
  assert.equal(postMatchRatingGameTypeParam('Quickmatch', ffa), null);
  assert.equal(postMatchRatingGameTypeParam(undefined, duel), null);
});

test('snapshots require a persisted MMR value', () => {
  assert.equal(
    snapshotFromResponse({ rank: null, mmr: null, wins: null, losses: null, winRate: null }),
    null,
  );
  assert.deepEqual(
    snapshotFromResponse({ rank: 12, mmr: 1480, wins: 9, losses: 4, winRate: 69.2 }),
    { mmr: 1480, wins: 9, losses: 4, position: 12 },
  );
});

test('settlement fires on any persisted counter moving', () => {
  const baseline: RatingSnapshot = { mmr: 1480, wins: 9, losses: 4, position: 12 };

  assert.equal(ratingHasSettled(baseline, null), false);
  assert.equal(ratingHasSettled(baseline, { ...baseline }), false);
  assert.equal(ratingHasSettled(baseline, { ...baseline, mmr: 1495 }), true);
  assert.equal(ratingHasSettled(baseline, { ...baseline, wins: 10 }), true);
  assert.equal(ratingHasSettled(baseline, { ...baseline, losses: 5 }), true);
  // Leaderboard position shifting alone is other players moving, not us.
  assert.equal(ratingHasSettled(baseline, { ...baseline, position: 11 }), false);
  // First rated match: the row appearing at all is the signal.
  assert.equal(ratingHasSettled(null, { ...baseline }), true);
  assert.equal(ratingHasSettled(null, null), false);
});

test('a competitive reveal carries delta, movement, and meter positions', () => {
  const before: RatingSnapshot = { mmr: 1190, wins: 9, losses: 4, position: 20 };
  const after: RatingSnapshot = { mmr: 1215, wins: 10, losses: 4, position: 16 };

  const reveal = buildRatingReveal('Competitive', 'duel', before, after);
  assert.equal(reveal.delta, 25);
  assert.equal(reveal.movement, 'promoted');
  assert.equal(reveal.crossedTier, true);
  assert.equal(formatRankLabel(reveal.fromRank), 'Silver III');
  assert.equal(formatRankLabel(reveal.toRank), 'Gold I');
  assert.equal(reveal.fromProgress, 0.95);
  assert.equal(reveal.toProgress, 0.15);
});

test('a first rated match has no delta and no movement to announce', () => {
  const after: RatingSnapshot = { mmr: 1020, wins: 1, losses: 0, position: 44 };
  const reveal = buildRatingReveal('Competitive', 'ffa', null, after);

  assert.equal(reveal.before, null);
  assert.equal(reveal.delta, null);
  assert.equal(reveal.movement, 'unchanged');
  assert.equal(reveal.crossedTier, false);
  assert.deepEqual(reveal.fromRank, reveal.toRank);
});

test('quickmatch reveals never announce division movement', () => {
  const before: RatingSnapshot = { mmr: 1190, wins: 3, losses: 1, position: null };
  const after: RatingSnapshot = { mmr: 1240, wins: 4, losses: 1, position: null };

  const reveal = buildRatingReveal('Quickmatch', '2v2', before, after);
  assert.equal(reveal.delta, 50);
  assert.equal(reveal.movement, 'unchanged');
  assert.equal(reveal.crossedTier, false);
});

test('odometer duration scales with the swing and stays bounded', () => {
  assert.equal(countDurationMs(0), 900);
  assert.equal(countDurationMs(12), 916);
  assert.equal(countDurationMs(-12), 916);
  assert.equal(countDurationMs(500), 1600);
});
