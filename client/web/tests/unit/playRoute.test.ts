import assert from 'node:assert/strict';
import test from 'node:test';

import { parseU32GameId } from '../../utils/gameId.ts';
import { activeGameIdFromPath } from '../../services/websocketLifecycle.ts';
import { homeNoticeState, readHomeNotice } from '../../utils/homeNotice.ts';

// `/play/:gameId` serves both the arena and player invite links, and PlayRoute
// picks between them with exactly this predicate. These pin the split itself,
// since getting it wrong sends players to the wrong screen in both directions.

test('a numeric play segment is a game id, so the arena keeps every real match URL', () => {
  assert.equal(parseU32GameId('42'), 42);
  assert.equal(parseU32GameId('1'), 1);
  assert.equal(parseU32GameId('4294967295'), 4294967295);
});

test('a name-shaped play segment is not a game id, so it falls through to the invite page', () => {
  assert.equal(parseU32GameId('lopatron'), null);
  assert.equal(parseU32GameId('snake_master'), null);
  assert.equal(parseU32GameId('player-one'), null);
  // Out of u32 range: not a reachable game, so it is treated as a name and
  // the invite page reports no such player rather than the arena erroring.
  assert.equal(parseU32GameId('4294967296'), null);
});

test('the gameplay-route predicate ignores invite links', () => {
  // Drives the backdrop, the runtime announcement, and ad gameplay treatment
  // in App.tsx. An invite page is an ordinary screen and must not be mistaken
  // for a live match.
  assert.equal(activeGameIdFromPath('/play/42'), 42);
  assert.equal(activeGameIdFromPath('/play/lopatron'), null);
  assert.equal(activeGameIdFromPath('/play/lopatron?ref=x'), null);
});

// The home notice is how a redirect explains itself. It travels in router
// state, which is untyped and already carries unrelated payloads.

test('a well-formed notice round-trips through router state', () => {
  const { state, replace } = homeNoticeState('lopatron is not online right now.');
  assert.equal(replace, true);
  assert.deepEqual(readHomeNotice(state), {
    message: 'lopatron is not online right now.',
    tone: 'info',
  });

  assert.deepEqual(readHomeNotice(homeNoticeState('Something broke.', 'error').state), {
    message: 'Something broke.',
    tone: 'error',
  });
});

test('unrelated or malformed router state yields no notice', () => {
  // ProtectedRoute writes `{ from }` into the same slot.
  assert.equal(readHomeNotice({ from: { pathname: '/play/42' } }), null);
  assert.equal(readHomeNotice(null), null);
  assert.equal(readHomeNotice(undefined), null);
  assert.equal(readHomeNotice('a string'), null);
  assert.equal(readHomeNotice({ homeNotice: null }), null);
  assert.equal(readHomeNotice({ homeNotice: { message: '' } }), null);
  assert.equal(readHomeNotice({ homeNotice: { message: '   ' } }), null);
  assert.equal(readHomeNotice({ homeNotice: { tone: 'error' } }), null);
});

test('an unrecognized tone falls back to info rather than styling as an error', () => {
  assert.deepEqual(readHomeNotice({ homeNotice: { message: 'hi', tone: 'shouty' } }), {
    message: 'hi',
    tone: 'info',
  });
});
