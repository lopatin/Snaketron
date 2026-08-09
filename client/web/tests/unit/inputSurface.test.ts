import assert from 'node:assert/strict';
import test from 'node:test';
import { resolveInputSurface } from '../../utils/inputSurface.ts';

test('a coarse primary pointer is a touch surface regardless of the portal', () => {
  assert.equal(resolveInputSurface(true, null), 'touch');
  assert.equal(resolveInputSurface(true, { systemInfo: null }), 'touch');
  assert.equal(
    resolveInputSurface(true, { systemInfo: { device: { type: 'desktop' } } }),
    'touch',
    'the portal cannot demote a coarse-pointer device to keyboard',
  );
});

test('a fine pointer stays keyboard unless CrazyGames reports a mobile embed', () => {
  assert.equal(resolveInputSurface(false, null), 'keyboard');
  assert.equal(resolveInputSurface(false, { systemInfo: null }), 'keyboard');
  assert.equal(
    resolveInputSurface(false, { systemInfo: { device: { type: 'desktop' } } }),
    'keyboard',
  );
  assert.equal(
    resolveInputSurface(false, { systemInfo: {} }),
    'keyboard',
  );
});

test('CrazyGames mobile and tablet embeds force the touch surface', () => {
  for (const type of ['mobile', 'tablet'] as const) {
    assert.equal(
      resolveInputSurface(false, { systemInfo: { device: { type } } }),
      'touch',
      `${type} embeds must show touch controls even if pointer detection fails in the iframe`,
    );
  }
});
