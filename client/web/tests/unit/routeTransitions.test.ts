import assert from 'node:assert/strict';
import test from 'node:test';

import {
  isGameRoutePath,
  shouldSwapRouteImmediately,
} from '../../utils/routeTransitions.ts';

test('a retained party swaps directly from the completed match to its next assignment', () => {
  assert.equal(shouldSwapRouteImmediately('/play/42', '/play/43'), true);
  assert.equal(shouldSwapRouteImmediately('/play/42', '/play/42'), false);
});

test('ordinary page transitions keep their existing fade timing', () => {
  assert.equal(shouldSwapRouteImmediately('/', '/play/42'), false);
  assert.equal(shouldSwapRouteImmediately('/play/42', '/'), false);
  assert.equal(shouldSwapRouteImmediately('/lobby/ROOM', '/play/42'), false);
  assert.equal(isGameRoutePath('/play/42'), true);
  assert.equal(isGameRoutePath('/leaderboards'), false);
});
