import assert from 'node:assert/strict';
import test from 'node:test';
import * as constants from '../../constants.ts';
import {
  buildGameplayAuthentication,
  GAMEPLAY_PROTOCOL_VERSION,
} from '../../constants.ts';

test('gameplay authentication reports the protocol version', () => {
  assert.equal(GAMEPLAY_PROTOCOL_VERSION, 7);
  assert.deepEqual(buildGameplayAuthentication('test-token'), {
    Authenticate: {
      token: 'test-token',
      protocol_version: 7,
    },
  });
});

// A shipped build cannot update itself — an itch.io bundle has no
// reload-to-upgrade path at all — so a protocol mismatch must never produce a
// dead end the player cannot act on. Nothing may reintroduce that gate.
test('no client-update gate survives anywhere in the gameplay protocol constants', () => {
  assert.equal('isClientUpdateRequiredReason' in constants, false);
  assert.equal('CLIENT_UPDATE_REQUIRED_REASON' in constants, false);
});
