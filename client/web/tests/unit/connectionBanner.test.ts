import assert from 'node:assert/strict';
import test from 'node:test';

import {
  CONNECTION_BANNER_MIN_VISIBLE_MS,
  connectionBannerHideDelayMs,
  isConnectionReady,
} from '../../utils/connectionBanner.ts';

test('an anonymous visitor is ready as soon as the transport is open', () => {
  // Nothing to authenticate: the socket carries player counts, so an open
  // transport is the whole of readiness. Requiring a session here would pin
  // the banner on screen forever for every visitor who has not pressed Play.
  assert.equal(
    isConnectionReady({ isConnected: true, isSessionAuthenticated: false, hasIdentity: false }),
    true,
  );
  assert.equal(
    isConnectionReady({ isConnected: false, isSessionAuthenticated: false, hasIdentity: false }),
    false,
  );
});

test('a player with an identity is ready only once the session is authenticated', () => {
  // An open but unauthenticated socket cannot carry lobby or matchmaking
  // commands, so reporting it as connected would be a lie.
  assert.equal(
    isConnectionReady({ isConnected: true, isSessionAuthenticated: false, hasIdentity: true }),
    false,
  );
  assert.equal(
    isConnectionReady({ isConnected: true, isSessionAuthenticated: true, hasIdentity: true }),
    true,
  );
  assert.equal(
    isConnectionReady({ isConnected: false, isSessionAuthenticated: true, hasIdentity: true }),
    false,
  );
});

test('a hidden badge has nothing to hold', () => {
  assert.equal(connectionBannerHideDelayMs(null, 1_000), 0);
});

test('a badge that just appeared is held for its full minimum', () => {
  assert.equal(
    connectionBannerHideDelayMs(1_000, 1_000),
    CONNECTION_BANNER_MIN_VISIBLE_MS,
  );
  assert.equal(
    connectionBannerHideDelayMs(1_000, 1_000 + CONNECTION_BANNER_MIN_VISIBLE_MS / 2),
    CONNECTION_BANNER_MIN_VISIBLE_MS / 2,
  );
});

test('a badge past its minimum comes down immediately', () => {
  assert.equal(connectionBannerHideDelayMs(1_000, 1_000 + CONNECTION_BANNER_MIN_VISIBLE_MS), 0);
  assert.equal(connectionBannerHideDelayMs(1_000, 60_000), 0);
});

test('a clock jump can neither pin the badge nor take it down early', () => {
  // Clamped to the floor rather than trusting a negative elapsed time.
  assert.equal(connectionBannerHideDelayMs(60_000, 1_000), CONNECTION_BANNER_MIN_VISIBLE_MS);
  assert.equal(connectionBannerHideDelayMs(Number.NaN, 1_000), 0);
  assert.equal(connectionBannerHideDelayMs(1_000, Number.POSITIVE_INFINITY), 0);
});
