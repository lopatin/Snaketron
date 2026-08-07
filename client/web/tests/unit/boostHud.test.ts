import assert from 'node:assert/strict';
import test from 'node:test';
import { buildBoostHudView } from '../../utils/boostHud.ts';

const config = { speed_milli: 1500, capacity_ms: 3000 };

test('Boost HUD reports empty, partial, and full predicted charge', () => {
  const empty = buildBoostHudView(
    config,
    { is_alive: true, boost: { charge_ms: 0, active: false } },
    true,
    false,
  );
  const partial = buildBoostHudView(
    config,
    { is_alive: true, boost: { charge_ms: 1000, active: false } },
    true,
    false,
  );
  const full = buildBoostHudView(
    config,
    { is_alive: true, boost: { charge_ms: 3000, active: false } },
    true,
    false,
  );

  assert.deepEqual(
    [empty.percent, partial.percent, full.percent],
    [0, 33, 100],
  );
  assert.deepEqual(
    [empty.fillRatio, partial.fillRatio, full.fillRatio],
    [0, 1 / 3, 1],
  );
  // An empty meter must leave the control usable: holding it on empty is how a
  // player arms the next packet, so disabling it would swallow the press.
  assert.equal(empty.buttonDisabled, false);
  assert.equal(empty.ready, false);
  assert.equal(partial.buttonDisabled, false);
  assert.equal(partial.ready, false);
  assert.equal(full.ready, true);
  assert.equal(full.multiplier, 1.5);
});

test('Boost ready state requires exact full capacity and an inactive snake', () => {
  const roundedOnly = buildBoostHudView(
    config,
    { is_alive: true, boost: { charge_ms: 2999, active: false } },
    true,
    false,
  );
  const fullAndActive = buildBoostHudView(
    config,
    { is_alive: true, boost: { charge_ms: 3000, active: true } },
    true,
    false,
  );

  assert.equal(roundedOnly.percent, 99);
  assert.equal(roundedOnly.fillRatio, 2999 / 3000);
  assert.equal(roundedOnly.ready, false);
  assert.equal(fullAndActive.percent, 100);
  assert.equal(fullAndActive.ready, false);
});

test('Boost button can stop active Boost and is disabled when interaction is unavailable', () => {
  const view = (isAlive: boolean, active: boolean, interactive: boolean, gameOver: boolean) => (
    buildBoostHudView(
      config,
      { is_alive: isAlive, boost: { charge_ms: 1000, active } },
      interactive,
      gameOver,
    )
  );

  assert.equal(view(true, true, true, false).buttonDisabled, false);
  assert.equal(view(false, false, true, false).buttonDisabled, true);
  assert.equal(view(true, false, false, false).buttonDisabled, true);
  assert.equal(view(true, false, true, true).buttonDisabled, true);
});

test('Boost HUD defensively clamps malformed snapshot charge', () => {
  const overCapacity = buildBoostHudView(
    config,
    { is_alive: true, boost: { charge_ms: 4000, active: false } },
    true,
    false,
  );
  assert.equal(overCapacity.percent, 100);
  assert.equal(overCapacity.fillRatio, 1);
  assert.equal(overCapacity.chargeMs, 3000);
  assert.equal(overCapacity.ready, true);
  assert.equal(buildBoostHudView(
    config,
    { is_alive: true, boost: { charge_ms: -100, active: false } },
    true,
    false,
  ).percent, 0);
});

test('Boost HUD reports the supported 2x edge and funded final quantum', () => {
  const finalQuantum = buildBoostHudView(
    { speed_milli: 2000, capacity_ms: 3000 },
    { is_alive: true, boost: { charge_ms: 50, active: true } },
    true,
    false,
  );

  assert.equal(finalQuantum.multiplier, 2);
  assert.equal(finalQuantum.percent, 2);
  assert.equal(finalQuantum.chargeMs, 50);
  assert.equal(finalQuantum.active, true);
  assert.equal(finalQuantum.ready, false);
  assert.equal(finalQuantum.buttonDisabled, false);
});
