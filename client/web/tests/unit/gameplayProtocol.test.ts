import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildGameplayAuthentication,
  GAMEPLAY_PROTOCOL_VERSION,
  isGameplayProtocolCompatible,
  isGameplayUpdateRequiredReason,
} from '../../constants.ts';

test('gameplay authentication reports the protocol version', () => {
  assert.equal(GAMEPLAY_PROTOCOL_VERSION, 9);
  assert.deepEqual(buildGameplayAuthentication('test-token'), {
    Authenticate: {
      token: 'test-token',
      protocol_version: 9,
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
    isGameplayUpdateRequiredReason('Gameplay update required: client protocol 7'),
    true,
  );
  assert.equal(isGameplayUpdateRequiredReason('Access denied'), false);
});
