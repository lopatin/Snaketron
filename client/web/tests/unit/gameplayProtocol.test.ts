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
  assert.equal(GAMEPLAY_PROTOCOL_VERSION, 12);
  assert.equal(CLIENT_DISTRIBUTION, expectedDistribution);
  assert.deepEqual(buildGameplayAuthentication('test-token'), {
    Authenticate: {
      token: 'test-token',
      protocol_version: 12,
      distribution: expectedDistribution,
    },
  });
});

test('predictive gameplay requires an exact protocol match', () => {
  assert.equal(isGameplayProtocolCompatible(12), true);
  assert.equal(isGameplayProtocolCompatible(11), false);
  assert.equal(isGameplayProtocolCompatible(13), false);
  assert.equal(isGameplayProtocolCompatible(undefined), false);
  assert.equal(isGameplayProtocolCompatible('12'), true);
  assert.equal(
    isGameplayUpdateRequiredReason('Gameplay update required: client protocol 9'),
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

test('gameplay authentication carries an anon id when one is supplied', () => {
  assert.deepEqual(
    buildGameplayAuthentication('test-token', '3f1a2b4c-5d6e-4f70-8a91-b2c3d4e5f607'),
    {
      Authenticate: {
        token: 'test-token',
        protocol_version: GAMEPLAY_PROTOCOL_VERSION,
        distribution: CLIENT_DISTRIBUTION,
        anon_id: '3f1a2b4c-5d6e-4f70-8a91-b2c3d4e5f607',
      },
    }
  );
});

// The anon id is additive and optional. Omitting it must produce exactly the
// frame a client that predates the field sends, so a server that has never
// seen it is byte-for-byte unaffected — which is why this addition does NOT
// bump the protocol version, unlike the gameplay changes that do.
test('omitting the anon id leaves the frame unchanged', () => {
  assert.equal('anon_id' in buildGameplayAuthentication('t').Authenticate, false);
  assert.equal('anon_id' in buildGameplayAuthentication('t', '').Authenticate, false);
});
