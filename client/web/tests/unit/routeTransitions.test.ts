import assert from 'node:assert/strict';
import test from 'node:test';

import {
  isGameRoutePath,
  isSkinsCataloguePath,
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

test('opening or closing a skin page toggles a modal, not a page, so it swaps directly', () => {
  assert.equal(shouldSwapRouteImmediately('/skins', '/skins/classic'), true);
  assert.equal(shouldSwapRouteImmediately('/skins/classic', '/skins'), true);
  assert.equal(shouldSwapRouteImmediately('/skins/skin%3A12', '/skins'), true);
  assert.equal(shouldSwapRouteImmediately('/skins', '/skins'), false);
});

test('the builder is a different screen and keeps the fade', () => {
  assert.equal(isSkinsCataloguePath('/skins/builder'), false);
  assert.equal(isSkinsCataloguePath('/skins/builder/12'), false);
  assert.equal(shouldSwapRouteImmediately('/skins', '/skins/builder'), false);
  assert.equal(shouldSwapRouteImmediately('/', '/skins'), false);
});
