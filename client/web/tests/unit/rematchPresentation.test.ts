import assert from 'node:assert/strict';
import test from 'node:test';

import { rematchBlockReason, rematchVerdict } from '../../utils/rematchPresentation.ts';
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
  assert.equal(rematchBlockReason(stuck), "3 players can't form a match — needs 1, 2, or 4.");
});

test('the count reads as a sentence at every size', () => {
  const one = state({
    participants: [participant(7, { opted_in: true })],
    game_type: null,
  });
  assert.equal(rematchBlockReason(one), "1 player can't form a match — needs 1, 2, or 4.");
});

test('a row says what that player did, with leaving taking precedence', () => {
  assert.equal(rematchVerdict(participant(7)), 'Deciding…');
  assert.equal(rematchVerdict(participant(7, { opted_in: true })), 'Ready');
  assert.equal(rematchVerdict(participant(7, { present: false })), 'Left');
  // Someone who opted in and then closed the tab is gone, not ready.
  assert.equal(rematchVerdict(participant(7, { present: false, opted_in: true })), 'Left');
});
