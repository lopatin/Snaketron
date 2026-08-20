import assert from 'node:assert/strict';
import test from 'node:test';

import {
  getTickerGroupCopies,
  getTickerPollIntervalMs,
  getTickerPlayPreferences,
} from '../../utils/newsTicker.ts';
import type { NewsTickerPlayAction } from '../../utils/newsTicker.ts';

test('repeats a short ticker group enough to cover the viewport after one copy scrolls away', () => {
  const copies = getTickerGroupCopies(260, 704);

  assert.equal(copies, 4);
  assert.ok((copies - 1) * 260 >= 704);
});

test('uses two copies when one group already spans the viewport', () => {
  assert.equal(getTickerGroupCopies(900, 704), 2);
});

test('bounds server-directed ticker polling and handles an invalid value', () => {
  assert.equal(getTickerPollIntervalMs(5), 30_000);
  assert.equal(getTickerPollIntervalMs(60), 60_000);
  assert.equal(getTickerPollIntervalMs(900), 300_000);
  assert.equal(getTickerPollIntervalMs(Number.NaN), 60_000);
});

test('maps every ticker play action to one exact lobby configuration', () => {
  assert.deepEqual(getTickerPlayPreferences('playSolo'), {
    selectedModes: ['solo'],
    competitive: true,
  });
  assert.deepEqual(getTickerPlayPreferences('playRankedSolo'), {
    selectedModes: ['solo'],
    competitive: true,
  });
  assert.deepEqual(getTickerPlayPreferences('playDuel'), {
    selectedModes: ['duel'],
    competitive: true,
  });
  assert.deepEqual(getTickerPlayPreferences('playTwoVsTwo'), {
    selectedModes: ['2v2'],
    competitive: true,
  });
  assert.deepEqual(getTickerPlayPreferences('playFfa'), {
    selectedModes: ['ffa'],
    competitive: true,
  });
  assert.deepEqual(getTickerPlayPreferences('playRankedDuel'), {
    selectedModes: ['duel'],
    competitive: true,
  });
  assert.deepEqual(getTickerPlayPreferences('playRankedTwoVsTwo'), {
    selectedModes: ['2v2'],
    competitive: true,
  });
  assert.deepEqual(getTickerPlayPreferences('playRankedFfa'), {
    selectedModes: ['ffa'],
    competitive: true,
  });
});

/**
 * Every ticker CTA drops the player into ranked, so the casual half of the
 * action set must not survive as a quick-match shortcut: the queue named by
 * the headline only ever selects the mode.
 */
test('a casual ticker action queues the same competitive lobby as its ranked twin', () => {
  const pairs: Array<[NewsTickerPlayAction, NewsTickerPlayAction]> = [
    ['playSolo', 'playRankedSolo'],
    ['playDuel', 'playRankedDuel'],
    ['playTwoVsTwo', 'playRankedTwoVsTwo'],
    ['playFfa', 'playRankedFfa'],
  ];

  for (const [casual, ranked] of pairs) {
    assert.deepEqual(
      getTickerPlayPreferences(casual),
      getTickerPlayPreferences(ranked),
      `${casual} must queue the same lobby as ${ranked}`,
    );
    assert.equal(getTickerPlayPreferences(casual).competitive, true);
  }
});
