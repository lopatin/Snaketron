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
  const noun = optedIn === 1 ? 'player' : 'players';
  return `${optedIn} ${noun} can't form a match — needs 1, 2, or 4.`;
}

/** What one other participant's row should say. */
export function rematchVerdict(participant: RematchState['participants'][number]): string {
  if (!participant.present) {
    return 'Left';
  }
  return participant.opted_in ? 'Ready' : 'Deciding…';
}
