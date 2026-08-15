import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildGameplayAuthentication,
  CLIENT_DISTRIBUTION,
  GAMEPLAY_PROTOCOL_VERSION,
  isGameplayProtocolCompatible,
  isGameplayUpdateRequiredReason,
  resolveClientDistribution,
} from '../../constants.ts';

test('gameplay authentication reports the protocol version and build distribution', () => {
  const expectedDistribution = resolveClientDistribution(
    process.env.CRAZYGAMES_BUILD === 'true',
    process.env.ITCH_BUILD === 'true',
  );
  assert.equal(GAMEPLAY_PROTOCOL_VERSION, 9);
  assert.equal(CLIENT_DISTRIBUTION, expectedDistribution);
  assert.deepEqual(buildGameplayAuthentication('test-token'), {
    Authenticate: {
      token: 'test-token',
      protocol_version: 9,
      distribution: expectedDistribution,
    },
  });
});

test('predictive gameplay requires an exact protocol match', () => {
  assert.equal(isGameplayProtocolCompatible(9), true);
  assert.equal(isGameplayProtocolCompatible(8), false);
  assert.equal(isGameplayProtocolCompatible(10), false);
  assert.equal(isGameplayProtocolCompatible(undefined), false);
  assert.equal(isGameplayProtocolCompatible('9'), true);
  assert.equal(
    isGameplayUpdateRequiredReason('Gameplay update required: client protocol 8'),
    true,
  );
  assert.equal(isGameplayUpdateRequiredReason('Access denied'), false);
});

test('build targets map to their explicit client distributions', () => {
  assert.equal(resolveClientDistribution(false, false), 'web');
  assert.equal(resolveClientDistribution(true, false), 'crazygames');
  assert.equal(resolveClientDistribution(false, true), 'itch');
  assert.throws(
    () => resolveClientDistribution(true, true),
    /mutually exclusive release targets/,
  );
});
