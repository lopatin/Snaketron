import type { Challenge, ChallengeInbox } from '../types/generated';

/**
 * View logic for the challenge panels, kept out of the components so it can be
 * tested on plain objects — the same split every other HUD builder here uses.
 *
 * The server sends complete snapshots and prunes on its own timers, but the
 * client still has to reason about time locally: a challenge lapses two
 * minutes after it is issued, and nobody wants to watch a dead invitation sit
 * there until the next server push arrives.
 */

/** How long an answered challenge stays visible so the outcome registers. */
export const RESOLVED_CHALLENGE_GRACE_MS = 8000;

export interface PendingChallengeMatch {
  challenge: Challenge;
  direction: 'incoming' | 'outgoing';
}

/** Seconds left before a challenge lapses, floored at zero. */
export function secondsRemaining(challenge: Challenge, nowMs: number): number {
  return Math.max(0, Math.ceil((challenge.expires_at_ms - nowMs) / 1000));
}

function isVisible(challenge: Challenge, nowMs: number): boolean {
  if (challenge.state === 'pending') {
    return challenge.expires_at_ms > nowMs;
  }
  // Resolved: keep it for a beat past the window it would have had, so an
  // accept or a decline is acknowledged rather than just vanishing.
  const window = challenge.expires_at_ms - challenge.created_at_ms;
  return nowMs - challenge.created_at_ms < window + RESOLVED_CHALLENGE_GRACE_MS;
}

/**
 * The challenges worth rendering: everything still pending, plus answers that
 * landed moments ago.
 */
export function visibleChallenges(inbox: ChallengeInbox, nowMs: number): ChallengeInbox {
  return {
    incoming: inbox.incoming.filter((challenge) => isVisible(challenge, nowMs)),
    outgoing: inbox.outgoing.filter((challenge) => isVisible(challenge, nowMs)),
  };
}

/**
 * The live challenge already covering this player, in either direction.
 *
 * This is what stops the roster offering a second challenge to someone you are
 * already waiting on — and what turns the button into a "they challenged you"
 * hint when the invitation is pointing the other way.
 */
export function findPendingChallenge(
  inbox: ChallengeInbox,
  userId: number,
  nowMs: number,
): PendingChallengeMatch | null {
  const live = (challenge: Challenge): boolean =>
    challenge.state === 'pending' && challenge.expires_at_ms > nowMs;

  const outgoing = inbox.outgoing.find(
    (challenge) => challenge.to_user_id === userId && live(challenge),
  );
  if (outgoing) {
    return { challenge: outgoing, direction: 'outgoing' };
  }
  const incoming = inbox.incoming.find(
    (challenge) => challenge.from_user_id === userId && live(challenge),
  );
  return incoming ? { challenge: incoming, direction: 'incoming' } : null;
}

/** Terminal-state label, or null while a challenge is still open. */
export function challengeOutcomeLabel(challenge: Challenge): string | null {
  switch (challenge.state) {
    case 'accepted':
      return 'Accepted';
    case 'declined':
      return 'Declined';
    case 'cancelled':
      return 'Withdrawn';
    default:
      return null;
  }
}
