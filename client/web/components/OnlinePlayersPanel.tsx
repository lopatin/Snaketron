import React, { useMemo, useState } from 'react';
import type { OnlinePlayer } from '../types';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import { findPendingChallenge } from '../utils/challengePresentation';
import { ChallengeIcon } from './Icons';

/**
 * Everyone online in this region, as a floating horizontal strip.
 *
 * Deliberately a black translucent bar rather than the app's usual white
 * paper: it floats over whatever screen you are on, and a white panel there
 * would read as another page element competing for the same plane. It scrolls
 * horizontally so a busy region stays one line tall.
 *
 * The roster is server-pushed and already excludes the viewer. `null` means it
 * has not arrived yet — distinct from an empty region, which is worth saying
 * out loud rather than rendering as nothing.
 */
export interface OnlinePlayersPanelProps {
  /** Hidden entirely on screens where a floating strip would be in the way. */
  enabled?: boolean;
}

const activityLabel = (activity: OnlinePlayer['activity']): string => {
  switch (activity) {
    case 'playing':
      return 'In a match';
    case 'lobby':
      return 'In a lobby';
    default:
      return 'Available';
  }
};

export const OnlinePlayersPanel: React.FC<OnlinePlayersPanelProps> = ({ enabled = true }) => {
  const { user } = useAuth();
  const { onlinePlayers, challenges, challengeError, challengePlayer, dismissChallengeError } =
    useWebSocket();
  const [isCollapsed, setIsCollapsed] = useState(false);

  const players = useMemo(() => onlinePlayers?.players ?? [], [onlinePlayers]);
  const nowMs = Date.now();

  if (!enabled || !onlinePlayers) {
    return null;
  }

  const othersOnline = Math.max(0, onlinePlayers.total_online - 1);

  return (
    <section
      className={`online-players${isCollapsed ? ' is-collapsed' : ''}`}
      aria-label="Players online in your region"
      data-testid="online-players-panel"
    >
      <button
        type="button"
        className="online-players-toggle"
        onClick={() => setIsCollapsed((collapsed) => !collapsed)}
        aria-expanded={!isCollapsed}
        data-testid="online-players-toggle"
      >
        <span className="online-players-dot" aria-hidden="true" />
        <span className="online-players-title">Online</span>
        <span className="online-players-count">{othersOnline}</span>
      </button>

      {!isCollapsed && (
        <div className="online-players-list" data-testid="online-players-list">
          {players.length === 0 ? (
            <p className="online-players-empty">No one else is online right now.</p>
          ) : (
            <ul>
              {players.map((player) => {
                const pending = findPendingChallenge(challenges, player.user_id, nowMs);
                const isSelf = user?.id === player.user_id;
                return (
                  <li key={player.user_id} className="online-player">
                    <span
                      className={`online-player-status is-${player.activity}`}
                      aria-hidden="true"
                    />
                    <span className="online-player-name" title={player.username}>
                      {player.username}
                    </span>
                    <span className="online-player-activity">{activityLabel(player.activity)}</span>
                    <button
                      type="button"
                      className={`online-player-challenge${pending ? ' is-pending' : ''}`}
                      onClick={() => challengePlayer(player.user_id)}
                      disabled={isSelf || pending !== null}
                      aria-label={
                        pending
                          ? pending.direction === 'outgoing'
                            ? `Challenge to ${player.username} is pending`
                            : `${player.username} has already challenged you`
                          : `Challenge ${player.username}`
                      }
                      title={
                        pending
                          ? pending.direction === 'outgoing'
                            ? 'Waiting for a reply'
                            : 'They challenged you — answer in Challenges'
                          : `Challenge ${player.username}`
                      }
                      data-testid={`challenge-player-${player.user_id}`}
                    >
                      <ChallengeIcon className="online-player-challenge-icon" />
                    </button>
                  </li>
                );
              })}
            </ul>
          )}
          {players.length < othersOnline && (
            <p className="online-players-overflow">
              +{othersOnline - players.length} more
            </p>
          )}
        </div>
      )}

      {challengeError && (
        <p className="online-players-error" role="alert">
          <span>{challengeError}</span>
          <button type="button" onClick={dismissChallengeError} aria-label="Dismiss">
            ✕
          </button>
        </p>
      )}
    </section>
  );
};

export default OnlinePlayersPanel;
