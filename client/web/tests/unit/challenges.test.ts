import assert from 'node:assert/strict';
import test from 'node:test';

import {
  challengeOutcomeLabel,
  findPendingChallenge,
  secondsRemaining,
  visibleChallenges,
} from '../../utils/challengePresentation.ts';
import type { Challenge, ChallengeInbox } from '../../types/generated/index.ts';

const NOW = 1_700_000_000_000;
const TTL_MS = 120_000;

function challenge(overrides: Partial<Challenge> = {}): Challenge {
  return {
    challenge_id: 'c1',
    from_user_id: 2,
    from_username: 'Grace',
    to_user_id: 1,
    to_username: 'Ada',
    lobby_code: 'USE1-ABCD1234',
    state: 'pending',
    created_at_ms: NOW,
    expires_at_ms: NOW + TTL_MS,
    ...overrides,
  };
}

const inbox = (incoming: Challenge[], outgoing: Challenge[]): ChallengeInbox => ({
  incoming,
  outgoing,
});

test('an expired pending challenge disappears without waiting for the server', () => {
  const live = challenge();
  const lapsed = challenge({ challenge_id: 'c2', expires_at_ms: NOW - 1 });

  const visible = visibleChallenges(inbox([live, lapsed], []), NOW);
  assert.deepEqual(
    visible.incoming.map((entry) => entry.challenge_id),
    ['c1'],
  );
});

test('an answered challenge lingers briefly so the outcome is visible', () => {
  const declined = challenge({ state: 'declined' });

  // Just after the answer: still shown, so the decline is acknowledged.
  assert.equal(visibleChallenges(inbox([declined], []), NOW + 1_000).incoming.length, 1);
  // Well past its window: gone.
  assert.equal(
    visibleChallenges(inbox([declined], []), NOW + TTL_MS + 30_000).incoming.length,
    0,
  );
});

test('the countdown floors at zero rather than going negative', () => {
  assert.equal(secondsRemaining(challenge(), NOW), 120);
  assert.equal(secondsRemaining(challenge(), NOW + 119_400), 1);
  assert.equal(secondsRemaining(challenge(), NOW + TTL_MS + 5_000), 0);
});

test('a player already involved in a live challenge cannot be challenged again', () => {
  const outgoing = challenge({ challenge_id: 'out', from_user_id: 1, to_user_id: 9 });
  const found = findPendingChallenge(inbox([], [outgoing]), 9, NOW);
  assert.equal(found?.direction, 'outgoing');

  const incoming = challenge({ challenge_id: 'in', from_user_id: 9, to_user_id: 1 });
  const other = findPendingChallenge(inbox([incoming], []), 9, NOW);
  assert.equal(other?.direction, 'incoming');
});

test('a resolved or expired challenge frees the player up again', () => {
  const declined = challenge({ challenge_id: 'out', from_user_id: 1, to_user_id: 9, state: 'declined' });
  assert.equal(findPendingChallenge(inbox([], [declined]), 9, NOW), null);

  const lapsed = challenge({
    challenge_id: 'out',
    from_user_id: 1,
    to_user_id: 9,
    expires_at_ms: NOW - 1,
  });
  assert.equal(findPendingChallenge(inbox([], [lapsed]), 9, NOW), null);
});

test('challenges with other players do not block this one', () => {
  const elsewhere = challenge({ challenge_id: 'out', from_user_id: 1, to_user_id: 5 });
  assert.equal(findPendingChallenge(inbox([], [elsewhere]), 9, NOW), null);
});

test('outcome labels read as answers, and an open challenge has none', () => {
  assert.equal(challengeOutcomeLabel(challenge()), null);
  assert.equal(challengeOutcomeLabel(challenge({ state: 'accepted' })), 'Accepted');
  assert.equal(challengeOutcomeLabel(challenge({ state: 'declined' })), 'Declined');
  assert.equal(challengeOutcomeLabel(challenge({ state: 'cancelled' })), 'Withdrawn');
});
