import assert from 'node:assert/strict';
import test from 'node:test';
import {
  COMBO_CALLOUT_NEAR_OPACITY,
  comboCalloutProximityOpacity,
} from '../../utils/comboProximity.ts';

const CELL = 20;

// A 40x40 arena drawn at 20px cells, with the canvas parked at the viewport
// origin, so a grid cell's rendered center is (cell * 20 + 11) in both axes.
const canvasRect = { left: 0, top: 0, right: 802, bottom: 802 };

// The live callout sits at the top center of that canvas: six cells wide and
// two and a half tall, which is about what it measures on a desktop arena.
const calloutRect = { left: 340, top: 12, right: 460, bottom: 62 };

const opacityFor = (
  head: { x: number; y: number },
  rotation: 0 | 90 | 180 | 270 = 0,
) => comboCalloutProximityOpacity({
  head,
  arenaWidth: 40,
  arenaHeight: 40,
  rotation,
  cellSize: CELL,
  canvasRect,
  calloutRect,
});

test('the callout is untouched unless the head is right next to it', () => {
  assert.equal(opacityFor({ x: 20, y: 30 }), 1);
  assert.equal(opacityFor({ x: 2, y: 2 }), 1);
  // Directly below the callout but three cells clear of its edge: the whole
  // approach still reads at full strength.
  assert.equal(opacityFor({ x: 20, y: 6 }), 1);
  // Alongside it on the same row, three cells past its left edge.
  assert.equal(opacityFor({ x: 13, y: 1 }), 1);
});

test('a head under the callout fades it to the near opacity', () => {
  // Screen cell (20, 1) renders at (411, 31), inside the callout box.
  assert.equal(opacityFor({ x: 20, y: 1 }), COMBO_CALLOUT_NEAR_OPACITY);
  // Half a cell clear of the bottom edge is still fully faded.
  assert.equal(opacityFor({ x: 20, y: 3 }), COMBO_CALLOUT_NEAR_OPACITY);
});

test('the fade ramps monotonically over the last cells of the approach', () => {
  const approach = [10, 8, 6, 5, 4, 3, 2].map((y) => opacityFor({ x: 20, y }));

  for (let index = 1; index < approach.length; index += 1) {
    assert.ok(
      approach[index] <= approach[index - 1],
      `opacity rose while closing in: ${approach.join(', ')}`,
    );
  }
  assert.equal(approach[0], 1);
  assert.equal(approach[approach.length - 1], COMBO_CALLOUT_NEAR_OPACITY);
  // The ramp is short but real, so a head skimming the edge does not snap.
  const partial = approach.filter(
    (value) => value > COMBO_CALLOUT_NEAR_OPACITY && value < 1,
  );
  assert.ok(partial.length > 0, 'expected a partially faded step');
});

test('proximity follows the rotated arena, not raw grid coordinates', () => {
  // At 270 degrees the renderer maps grid (x, y) to screen (y, 39 - x), so
  // the cell that lands under the callout is a completely different one.
  assert.equal(opacityFor({ x: 38, y: 20 }, 270), COMBO_CALLOUT_NEAR_OPACITY);
  assert.equal(opacityFor({ x: 20, y: 1 }, 270), 1);

  // 90 degrees maps grid (x, y) to screen (39 - y, x).
  assert.equal(opacityFor({ x: 1, y: 19 }, 90), COMBO_CALLOUT_NEAR_OPACITY);
});

test('an unmeasured callout or arena never dims', () => {
  const collapsed = { left: 400, top: 20, right: 400, bottom: 20 };
  assert.equal(
    comboCalloutProximityOpacity({
      head: { x: 20, y: 1 },
      arenaWidth: 40,
      arenaHeight: 40,
      rotation: 0,
      cellSize: CELL,
      canvasRect,
      calloutRect: collapsed,
    }),
    1,
  );
  assert.equal(
    comboCalloutProximityOpacity({
      head: { x: 20, y: 1 },
      arenaWidth: 40,
      arenaHeight: 40,
      rotation: 0,
      cellSize: 0,
      canvasRect,
      calloutRect,
    }),
    1,
  );
});
