import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildGameplayAuthentication,
  isClientUpdateRequiredReason,
  GAMEPLAY_PROTOCOL_VERSION,
} from '../../constants.ts';

test('gameplay authentication carries the exact hard-cutover protocol', () => {
  assert.equal(GAMEPLAY_PROTOCOL_VERSION, 7);
  assert.deepEqual(buildGameplayAuthentication('test-token'), {
    Authenticate: {
      token: 'test-token',
      protocol_version: 7,
    },
  });
});

test('the hard-cutover denial is recognized without reconnecting forever', () => {
  assert.equal(isClientUpdateRequiredReason('Client update required'), true);
  assert.equal(isClientUpdateRequiredReason(' client UPDATE required '), true);
  assert.equal(isClientUpdateRequiredReason('Access denied'), false);
});
