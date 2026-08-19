import assert from 'node:assert/strict';
import test from 'node:test';
import { userIdFromSessionToken } from '../../services/sessionIdentity.ts';

const NOW = 1_760_000_000_000;
const base64url = (value: string): string => Buffer.from(value, 'utf8')
  .toString('base64')
  .replace(/\+/g, '-')
  .replace(/\//g, '_')
  .replace(/=+$/, '');

/** A token shaped like the server's, with an unverifiable signature segment. */
const token = (claims: Record<string, unknown>): string => (
  `${base64url('{"alg":"HS256","typ":"JWT"}')}.${base64url(JSON.stringify(claims))}.sig`
);

const live = { exp: Math.floor(NOW / 1000) + 3_600 };

test('the durable user id is read from the subject claim', () => {
  assert.equal(
    userIdFromSessionToken(token({ sub: '4711', username: 'alex', ...live }), NOW),
    '4711',
  );
  // Guests hold ordinary session tokens too, and are reported like anyone else.
  assert.equal(
    userIdFromSessionToken(token({ sub: '90210', is_guest: true, ...live }), NOW),
    '90210',
  );
  // A token with no expiry claim is still usable.
  assert.equal(userIdFromSessionToken(token({ sub: '8' }), NOW), '8');
});

/**
 * An expired token describes a session that is about to be replaced, possibly
 * by a different user. Reporting under a stale identity is worse than falling
 * back to GameAnalytics' own device id.
 */
test('an expired token yields no identity', () => {
  assert.equal(
    userIdFromSessionToken(token({ sub: '4711', exp: Math.floor(NOW / 1000) - 1 }), NOW),
    null,
  );
  assert.equal(
    userIdFromSessionToken(token({ sub: '4711', exp: Math.floor(NOW / 1000) + 1 }), NOW),
    '4711',
  );
});

/**
 * Only the server's own `sub` shape is accepted. That rejects a malformed or
 * tampered token, and simultaneously guarantees the value passes
 * GameAnalytics' user-id validation, which refuses anything empty or over 64
 * characters.
 */
test('only a durable numeric subject is accepted', () => {
  assert.equal(userIdFromSessionToken(token({ sub: '', ...live }), NOW), null);
  assert.equal(userIdFromSessionToken(token({ sub: 'alex', ...live }), NOW), null);
  assert.equal(userIdFromSessionToken(token({ sub: '12ab', ...live }), NOW), null);
  assert.equal(userIdFromSessionToken(token({ sub: 4711, ...live }), NOW), null);
  assert.equal(userIdFromSessionToken(token({ username: 'alex', ...live }), NOW), null);
  assert.equal(userIdFromSessionToken(token({ sub: '9'.repeat(65), ...live }), NOW), null);
  assert.equal(userIdFromSessionToken(token({ sub: '9'.repeat(64), ...live }), NOW), '9'.repeat(64));
});

test('a malformed token is not an error, just no identity', () => {
  assert.equal(userIdFromSessionToken('', NOW), null);
  assert.equal(userIdFromSessionToken('not-a-token', NOW), null);
  assert.equal(userIdFromSessionToken('header.%%%not-base64%%%.sig', NOW), null);
  assert.equal(userIdFromSessionToken(`header.${base64url('not json')}.sig`, NOW), null);
  assert.equal(userIdFromSessionToken(`header.${base64url('[1,2,3]')}.sig`, NOW), null);
});

/**
 * The payload is decoded as UTF-8 rather than as latin-1 bytes, so a non-ASCII
 * username sitting beside `sub` cannot corrupt the JSON and lose the id.
 */
test('a non-ASCII claim elsewhere in the payload does not break decoding', () => {
  assert.equal(
    userIdFromSessionToken(token({ sub: '77', username: 'Ελλάδα-🐍', ...live }), NOW),
    '77',
  );
});
