import assert from 'node:assert/strict';
import test from 'node:test';

import { resolvePanelPlacement } from '../../utils/panelPlacement.ts';

const base = {
  panelWidth: 176,
  edgeGap: 16,
  modalOpen: false,
};

test('the panel takes the roomier margin', () => {
  // Content pushed right: the left margin is the only one that can hold it.
  const left = resolvePanelPlacement({
    ...base,
    viewportWidth: 1400,
    contentLeft: 500,
    contentRight: 1380,
  });
  assert.equal(left.side, 'left');
  assert.equal(left.fits, true);

  const right = resolvePanelPlacement({
    ...base,
    viewportWidth: 1400,
    contentLeft: 20,
    contentRight: 900,
  });
  assert.equal(right.side, 'right');
  assert.equal(right.fits, true);
});

test('a tie goes right, where the rest of the floating chrome lives', () => {
  const placement = resolvePanelPlacement({
    ...base,
    viewportWidth: 1400,
    contentLeft: 400,
    contentRight: 1000,
  });
  assert.equal(placement.side, 'right');
});

test('a margin too narrow for the panel does not fit', () => {
  // 1000 - 810 = 190 of margin, against a 176 panel needing 16 either side.
  const tight = resolvePanelPlacement({
    ...base,
    viewportWidth: 1000,
    contentLeft: 190,
    contentRight: 810,
  });
  assert.equal(tight.fits, false);

  const exact = resolvePanelPlacement({
    ...base,
    viewportWidth: 1000,
    contentLeft: 208,
    contentRight: 792,
  });
  assert.equal(exact.fits, true, '176 + 16 + 16 should be exactly enough');
});

test('a phone-width viewport never fits the panel', () => {
  const placement = resolvePanelPlacement({
    ...base,
    viewportWidth: 390,
    contentLeft: 12,
    contentRight: 378,
  });
  assert.equal(placement.fits, false);
});

/// A modal owns the screen even when the margins are wide open.
test('an open modal makes the panel not fit regardless of space', () => {
  const placement = resolvePanelPlacement({
    ...base,
    viewportWidth: 1920,
    contentLeft: 700,
    contentRight: 1220,
    modalOpen: true,
  });
  assert.equal(placement.fits, false);
  // The side is still resolved, so reopening it by hand lands somewhere sane.
  assert.equal(placement.side, 'right');
});

test('content wider than the viewport yields no margin rather than a negative one', () => {
  const placement = resolvePanelPlacement({
    ...base,
    viewportWidth: 800,
    contentLeft: -40,
    contentRight: 840,
  });
  assert.equal(placement.fits, false);
});
