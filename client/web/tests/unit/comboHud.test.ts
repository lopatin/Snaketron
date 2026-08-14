import assert from 'node:assert/strict';
import test from 'node:test';
import {
  COMBO_HUD_SEGMENT_COUNT,
  buildComboHudView,
  buildComboHudSegments,
} from '../../utils/comboHud.ts';

const config = { window_ms: 1000, max_food_value: 3 };

const snake = (
  chainCount: number,
  remainingMs: number,
  isAlive = true,
) => ({
  is_alive: isAlive,
  combo: { chain_count: chainCount, remaining_ms: remainingMs },
});

test('combo HUD arms +2 after the first food and drains in snake-cell segments', () => {
  const full = buildComboHudView(config, snake(1, 1000));
  const half = buildComboHudView(config, snake(1, 500));
  const finalSlice = buildComboHudView(config, snake(1, 1));

  assert.equal(full.active, true);
  assert.equal(full.nextFoodValue, 2);
  assert.equal(full.nextLabel, 'NEXT +2');
  assert.equal(full.tone, 'building');
  assert.equal(full.filledSegments, COMBO_HUD_SEGMENT_COUNT);
  assert.equal(half.filledSegments, COMBO_HUD_SEGMENT_COUNT / 2);
  assert.equal(finalSlice.filledSegments, 1);
  assert.equal(half.percent, 50);
  assert.match(half.ariaValueText, /next food is worth 2 points/);
});

test('combo cells disappear from the tail while the leading head stays filled', () => {
  assert.deepEqual(buildComboHudSegments(3), [
    false, false, false, false, false, false, false, true, true, true,
  ]);
  assert.deepEqual(buildComboHudSegments(0), Array(COMBO_HUD_SEGMENT_COUNT).fill(false));
  assert.deepEqual(buildComboHudSegments(99), Array(COMBO_HUD_SEGMENT_COUNT).fill(true));
});

test('combo HUD caps the next item at +3 and marks the maximum tier', () => {
  for (const chainCount of [2, 3, 99]) {
    const hud = buildComboHudView(config, snake(chainCount, 750));
    assert.equal(hud.nextFoodValue, 3);
    assert.equal(hud.nextLabel, '+3 MAX');
    assert.equal(hud.tone, 'maxed');
    assert.match(hud.ariaValueText, /maximum value/);
  }
});

test('expired and dead snakes keep a stable but inactive HUD', () => {
  for (const localSnake of [snake(0, 0), snake(2, 0), snake(2, 900, false)]) {
    const hud = buildComboHudView(config, localSnake);
    assert.equal(hud.active, false);
    assert.equal(hud.nextFoodValue, 1);
    assert.equal(hud.nextLabel, 'EAT TO START');
    assert.equal(hud.tone, 'idle');
    assert.equal(hud.filledSegments, 0);
    assert.equal(hud.percent, 0);
    assert.match(hud.ariaValueText, /inactive/);
  }
});

test('custom combo windows are read from the snapshot', () => {
  const hud = buildComboHudView(
    { window_ms: 1250, max_food_value: 3 },
    snake(1, 625),
  );

  assert.equal(hud.fillRatio, 0.5);
  assert.equal(hud.percent, 50);
  assert.equal(hud.nextFoodValue, 2);
  assert.equal(hud.nextLabel, 'NEXT +2');
  assert.equal(hud.tone, 'building');

  const capped = buildComboHudView(
    { window_ms: 1250, max_food_value: 3 },
    snake(2, 625),
  );
  assert.equal(capped.nextLabel, '+3 MAX');
  assert.equal(capped.tone, 'maxed');
});

test('malformed countdown values are clamped instead of breaking the rail', () => {
  const overfull = buildComboHudView(config, snake(1, 5000));
  assert.equal(overfull.remainingMs, 1000);
  assert.equal(overfull.fillRatio, 1);
  assert.equal(overfull.percent, 100);

  const invalid = buildComboHudView(
    { window_ms: Number.NaN, max_food_value: Number.POSITIVE_INFINITY },
    snake(Number.NaN, Number.NEGATIVE_INFINITY),
  );
  assert.equal(invalid.active, false);
  assert.equal(invalid.nextFoodValue, 1);
  assert.equal(invalid.percent, 0);
});
