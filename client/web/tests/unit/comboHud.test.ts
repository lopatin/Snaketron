import assert from 'node:assert/strict';
import test from 'node:test';
import { buildComboHudView } from '../../utils/comboHud.ts';

const config = { window_ms: 1000, max_food_value: 3 };

const snake = (
  chainCount: number,
  remainingMs: number,
  isAlive = true,
) => ({
  is_alive: isAlive,
  combo: { chain_count: chainCount, remaining_ms: remainingMs },
});

test('combo callout announces +2 after the first food', () => {
  const full = buildComboHudView(config, snake(1, 1000));
  const half = buildComboHudView(config, snake(1, 500));
  const finalSlice = buildComboHudView(config, snake(1, 1));

  assert.equal(full.active, true);
  assert.equal(full.fillRatio, 1);
  assert.equal(full.nextFoodValue, 2);
  assert.equal(full.nextLabel, '+2 Combo!');
  assert.equal(full.tone, 'building');
  assert.equal(half.active, true);
  assert.equal(half.fillRatio, 0.5);
  assert.equal(finalSlice.active, true);
  assert.equal(finalSlice.fillRatio, 0.001);
  assert.equal(full.ariaValueText, half.ariaValueText);
  assert.equal(half.ariaValueText, finalSlice.ariaValueText);
  assert.match(half.ariaValueText, /next food is worth 2 points/);
});

test('combo callout upgrades to +3 and stays capped there', () => {
  for (const chainCount of [2, 3, 99]) {
    const hud = buildComboHudView(config, snake(chainCount, 750));
    assert.equal(hud.nextFoodValue, 3);
    assert.equal(hud.nextLabel, '+3 Combo!');
    assert.equal(hud.tone, 'maxed');
    assert.match(hud.ariaValueText, /maximum value/);
  }
});

test('expired and dead snakes produce no visible callout text', () => {
  for (const localSnake of [snake(0, 0), snake(2, 0), snake(2, 900, false)]) {
    const hud = buildComboHudView(config, localSnake);
    assert.equal(hud.active, false);
    assert.equal(hud.fillRatio, 0);
    assert.equal(hud.nextFoodValue, 1);
    assert.equal(hud.nextLabel, '');
    assert.equal(hud.tone, 'idle');
    assert.match(hud.ariaValueText, /inactive/);
  }
});

test('custom combo windows are read from the snapshot', () => {
  const hud = buildComboHudView(
    { window_ms: 1250, max_food_value: 3 },
    snake(1, 625),
  );

  assert.equal(hud.remainingMs, 625);
  assert.equal(hud.fillRatio, 0.5);
  assert.equal(hud.nextFoodValue, 2);
  assert.equal(hud.nextLabel, '+2 Combo!');
  assert.equal(hud.tone, 'building');

  const capped = buildComboHudView(
    { window_ms: 1250, max_food_value: 3 },
    snake(2, 625),
  );
  assert.equal(capped.nextLabel, '+3 Combo!');
  assert.equal(capped.tone, 'maxed');
});

test('malformed countdown values are clamped instead of breaking the callout', () => {
  const overfull = buildComboHudView(config, snake(1, 5000));
  assert.equal(overfull.remainingMs, 1000);
  assert.equal(overfull.fillRatio, 1);
  assert.equal(overfull.active, true);

  const invalid = buildComboHudView(
    { window_ms: Number.NaN, max_food_value: Number.POSITIVE_INFINITY },
    snake(Number.NaN, Number.NEGATIVE_INFINITY),
  );
  assert.equal(invalid.active, false);
  assert.equal(invalid.fillRatio, 0);
  assert.equal(invalid.nextFoodValue, 1);
  assert.equal(invalid.nextLabel, '');
});
