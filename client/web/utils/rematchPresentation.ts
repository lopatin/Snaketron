import type { RematchState } from '../types';

/**
 * View logic for the rematch panel, kept out of the component so it can be
 * tested on plain objects — the same split every other HUD builder here uses.
 */

/**
 * Why the rematch cannot go ahead with the current opt-in count, if it cannot.
 *
 * The server is the authority: it sets `game_type` only for counts that form a
 * real queue family. This turns that absence into the sentence a player needs,
 * rather than restating the rule and risking the two drifting apart.
 */
export function rematchBlockReason(state: RematchState): string | null {
  const optedIn = state.participants.filter((participant) => participant.opted_in).length;
  if (optedIn === 0 || state.game_type !== null) {
    return null;
  }
  if (optedIn === 1) {
    return 'Waiting for someone else to run it back.';
  }
  return `${optedIn} players can't form a match — needs 2 or 4.`;
}

/**
 * The badge one player earns on the standings list, if any.
 *
 * It sits beside `Winner` in the same row, so it has to answer the rematch
 * question in one word or say nothing at all. "Left" outranks "Rematch":
 * someone who walked away cannot be counted on even if they ticked the box on
 * their way out.
 */
export type RematchBadge = 'rematch' | 'left' | null;

export function rematchBadgeFor(
  state: RematchState | null | undefined,
  userId: number | null,
): RematchBadge {
  if (!state || userId === null) {
    return null;
  }
  const participant = state.participants.find((entry) => entry.user_id === userId);
  if (!participant) {
    return null;
  }
  if (!participant.present) {
    return 'left';
  }
  return participant.opted_in ? 'rematch' : null;
}

/** Whether this viewer has opted in, for the checkbox's own state. */
export function hasOptedIntoRematch(
  state: RematchState | null | undefined,
  userId: number | undefined,
): boolean {
  if (!state || userId === undefined) {
    return false;
  }
  return state.participants.some(
    (participant) => participant.user_id === userId && participant.opted_in,
  );
}

/** Whether this viewer played in the match, and so has something to run back. */
export function canRematch(
  state: RematchState | null | undefined,
  userId: number | undefined,
): boolean {
  if (!state || userId === undefined) {
    return false;
  }
  return state.participants.some((participant) => participant.user_id === userId);
}
