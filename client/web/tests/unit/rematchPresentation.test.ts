import assert from 'node:assert/strict';
import test from 'node:test';

import {
  canRematch,
  hasOptedIntoRematch,
  rematchBadgeFor,
  rematchBlockReason,
} from '../../utils/rematchPresentation.ts';
import type { RematchState } from '../../types/generated/index.ts';

const participant = (
  user_id: number,
  overrides: Partial<RematchState['participants'][number]> = {},
): RematchState['participants'][number] => ({
  user_id,
  username: `user${user_id}`,
  present: true,
  opted_in: false,
  ...overrides,
});

const state = (overrides: Partial<RematchState> = {}): RematchState => ({
  game_id: 1,
  participants: [participant(7), participant(9)],
  lobby_code: null,
  host_user_id: null,
  game_type: null,
  queue_mode: 'Quickmatch',
  expires_at_ms: 0,
  ...overrides,
});

test('nobody opted in yet is not a blocked rematch', () => {
  assert.equal(rematchBlockReason(state()), null);
});

test('a count the server could form is not blocked', () => {
  const formed = state({
    participants: [participant(7, { opted_in: true }), participant(9, { opted_in: true })],
    game_type: { TeamMatch: { per_team: 1 } } as RematchState['game_type'],
  });
  assert.equal(rematchBlockReason(formed), null);
});

/// Three is the count that would otherwise be padded with a stranger, so the
/// server withholds a game type and the panel has to explain why.
test('a count the server refused to form says so, with the count', () => {
  const stuck = state({
    participants: [
      participant(7, { opted_in: true }),
      participant(9, { opted_in: true }),
      participant(11, { opted_in: true }),
    ],
    game_type: null,
  });
  assert.equal(rematchBlockReason(stuck), "3 players can't form a match — needs 2 or 4.");
});

test('the badge answers the rematch question in one word, or says nothing', () => {
  const base = state({
    participants: [
      participant(7, { opted_in: true }),
      participant(9),
      participant(11, { present: false }),
    ],
  });

  assert.equal(rematchBadgeFor(base, 7), 'rematch');
  // Still deciding earns no badge: an empty row is quieter than "Deciding".
  assert.equal(rematchBadgeFor(base, 9), null);
  assert.equal(rematchBadgeFor(base, 11), 'left');
});

test('leaving outranks having ticked the box on the way out', () => {
  const gone = state({
    participants: [participant(7, { present: false, opted_in: true })],
  });
  assert.equal(rematchBadgeFor(gone, 7), 'left');
});

test('a viewer who was not in the match gets no badge and no checkbox', () => {
  const base = state();
  assert.equal(rematchBadgeFor(base, 404), null);
  assert.equal(rematchBadgeFor(base, null), null);
  assert.equal(canRematch(base, 404), false);
  assert.equal(canRematch(base, 7), true);
  assert.equal(canRematch(null, 7), false);
});

test('the checkbox reflects only this viewer', () => {
  const mixed = state({
    participants: [participant(7, { opted_in: true }), participant(9)],
  });
  assert.equal(hasOptedIntoRematch(mixed, 7), true);
  assert.equal(hasOptedIntoRematch(mixed, 9), false);
  assert.equal(hasOptedIntoRematch(mixed, undefined), false);
});

test('one taker reads as waiting, not as a broken count', () => {
  const alone = state({
    participants: [participant(7, { opted_in: true }), participant(9)],
  });
  assert.equal(rematchBlockReason(alone), 'Waiting for someone else to run it back.');
});
