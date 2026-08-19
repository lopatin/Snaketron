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

test('gameplay authentication carries an anon id when one is supplied', () => {
  assert.deepEqual(
    buildGameplayAuthentication('test-token', '3f1a2b4c-5d6e-4f70-8a91-b2c3d4e5f607'),
    {
      Authenticate: {
        token: 'test-token',
        protocol_version: 7,
        anon_id: '3f1a2b4c-5d6e-4f70-8a91-b2c3d4e5f607',
      },
    }
  );
});

// The anon id is additive and optional. Omitting it must produce exactly the
// frame an older client sends, so a server that has never seen the field is
// byte-for-byte unaffected — the protocol version is deliberately not bumped
// for a backwards-compatible addition.
test('omitting the anon id leaves the legacy frame unchanged', () => {
  assert.equal('anon_id' in buildGameplayAuthentication('t').Authenticate, false);
  assert.equal('anon_id' in buildGameplayAuthentication('t', '').Authenticate, false);
});

// A shipped build cannot update itself — an itch.io bundle has no
// reload-to-upgrade path at all — so a protocol mismatch must never produce a
// dead end the player cannot act on. Nothing may reintroduce that gate.
test('no client-update gate survives anywhere in the gameplay protocol constants', () => {
  assert.equal('isClientUpdateRequiredReason' in constants, false);
  assert.equal('CLIENT_UPDATE_REQUIRED_REASON' in constants, false);
});
