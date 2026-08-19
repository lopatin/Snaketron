import React from 'react';
import type { RematchState } from '../types';
import { rematchBlockReason, rematchVerdict } from '../utils/rematchPresentation';

/**
 * "Run it back" on the results card.
 *
 * The checkbox is the whole interaction; everything else is the answer to the
 * question you actually have while deciding — is anyone else still here, and
 * has anyone else said yes. Both are live, so the decision is made with the
 * same information everyone else has.
 *
 * A player who has left is shown greyed rather than removed: knowing someone
 * walked away is what tells you the rematch is not going to happen.
 */
export interface RematchPanelProps {
  state: RematchState;
  currentUserId?: number;
  onToggle: (optIn: boolean) => void;
}

export const RematchPanel: React.FC<RematchPanelProps> = ({
  state,
  currentUserId,
  onToggle,
}) => {
  const me = state.participants.find(
    (participant) => participant.user_id === currentUserId,
  );
  const others = state.participants.filter(
    (participant) => participant.user_id !== currentUserId,
  );
  const blockReason = rematchBlockReason(state);
  const waiting = state.lobby_code !== null;

  // Someone who was not in the match has nothing to run back.
  if (!me) {
    return null;
  }

  return (
    <section className="game-over-rematch" data-testid="rematch-panel">
      <label className="game-over-rematch-toggle">
        <input
          type="checkbox"
          checked={me.opted_in}
          onChange={(event) => onToggle(event.target.checked)}
          data-testid="rematch-checkbox"
        />
        <span className="game-over-rematch-label">Rematch</span>
        {waiting && (
          <span className="game-over-rematch-status" data-testid="rematch-status">
            Setting up…
          </span>
        )}
      </label>

      {others.length > 0 && (
        <ul className="game-over-rematch-roster" aria-label="Rematch">
          {others.map((participant) => (
            <li
              key={participant.user_id}
              className={`game-over-rematch-player${participant.present ? '' : ' is-gone'}${
                participant.opted_in ? ' is-in' : ''
              }`}
              data-testid={`rematch-player-${participant.user_id}`}
            >
              <span className="game-over-rematch-name">{participant.username}</span>
              <span className="game-over-rematch-verdict">
                {rematchVerdict(participant)}
              </span>
            </li>
          ))}
        </ul>
      )}

      {blockReason && (
        <p className="game-over-rematch-blocked" role="status">
          {blockReason}
        </p>
      )}
    </section>
  );
};

export default RematchPanel;
