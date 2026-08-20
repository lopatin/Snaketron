import assert from 'node:assert/strict';
import test from 'node:test';
import type { BoostHudView } from '../../utils/boostHud.ts';
import {
  boostMeterControlLabel,
  boostMeterValueText,
} from '../../utils/boostMeterPresentation.ts';

function view(overrides: Partial<BoostHudView> = {}): BoostHudView {
  return {
    chargeMs: 1500,
    percent: 50,
    fillRatio: 0.5,
    active: false,
    ready: false,
    multiplier: 1.5,
    buttonDisabled: false,
    unlimited: false,
    ...overrides,
  };
}

test('Boost meter progress text describes charge and active state', () => {
  assert.equal(boostMeterValueText(view()), '50%');
  assert.equal(boostMeterValueText(view({ active: true })), '50%, active');
  assert.equal(boostMeterValueText(view({ unlimited: true })), 'Unlimited');
  assert.equal(
    boostMeterValueText(view({ active: true, unlimited: true })),
    'Unlimited, active',
  );
});

test('Boost meter control label preserves hold and toggle actions', () => {
  assert.equal(boostMeterControlLabel(view(), 'hold'), 'Hold to Boost, 50% charged');
  assert.equal(
    boostMeterControlLabel(view({ active: true }), 'hold'),
    'Release Boost, 50% remaining',
  );
  assert.equal(boostMeterControlLabel(view(), 'toggle'), 'Activate Boost, 50% charged');
  assert.equal(
    boostMeterControlLabel(view({ active: true }), 'toggle'),
    'Stop Boost, 50% remaining',
  );
});

test('Boost meter control label identifies unlimited Boost', () => {
  assert.equal(
    boostMeterControlLabel(view({ unlimited: true }), 'hold'),
    'Hold to Boost, unlimited',
  );
  assert.equal(
    boostMeterControlLabel(view({ active: true, unlimited: true }), 'toggle'),
    'Stop Boost, unlimited',
  );
});
