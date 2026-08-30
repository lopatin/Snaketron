import React from 'react';
import type { MatchPlayerPresentation, MatchPresentation } from '../utils/gamePresentation';
import RosterSnakeCanvas from './RosterSnakeCanvas';

export interface MatchRosterBandProps {
  presentation: MatchPresentation;
  isVisible: boolean;
  onMenu?: () => void;
  onScoreCard?: () => void;
  scoreCardOpen?: boolean;
}

const StaredownSnake: React.FC<{
  player: MatchPlayerPresentation;
  faces: 'left' | 'right';
}> = ({ player, faces }) => (
  <RosterSnakeCanvas
    name={player.name}
    skin={player.skin}
    facing={faces}
    isReady={player.isReady === true}
  />
);

const PlayerLegend: React.FC<{
  player: MatchPlayerPresentation;
  side?: 'left' | 'right';
}> = ({ player, side = 'right' }) => {
  // `isReady` is null once the gate has resolved, which is how the roster
  // knows to stop showing readiness rather than showing everyone as unready.
  const readyState =
    player.isReady === null ? '' : player.isReady ? ' is-ready' : ' is-awaiting-ready';
  const readyLabel =
    player.isReady === null ? '' : player.isReady ? ', ready' : ', not ready yet';
  // An idle kick can only land after the gate has resolved, so readiness and
  // inactivity never annotate the same player at the same time.
  const statusLabel = player.isIdleKicked
    ? ', removed for inactivity'
    : player.isAlive
      ? ''
      : ', out';

  return (
    <span
      className={`game-roster-player${player.isCurrentPlayer ? ' is-current' : ''}${player.isAlive ? '' : ' is-out'}${player.isIdleKicked ? ' is-idle-kicked' : ''}${readyState}`}
      role="img"
      aria-label={`${player.name}${readyLabel}${statusLabel}`}
      title={player.isIdleKicked ? `${player.name} — removed for inactivity` : player.name}
    >
      <StaredownSnake player={player} faces={side === 'left' ? 'right' : 'left'} />
      {player.isIdleKicked && (
        <span className="game-roster-idle-badge" aria-hidden="true">Idle</span>
      )}
    </span>
  );
};

const MatchRosterBand: React.FC<MatchRosterBandProps> = ({
  presentation,
  isVisible,
  onMenu,
  onScoreCard,
  scoreCardOpen = false,
}) => {
  const leftSide = presentation.sides[0];
  const rightSide = presentation.sides[1];
  const showActions = presentation.isComplete && Boolean(onMenu || onScoreCard);
  // The band carries no share control. It is a 44px row with `overflow: hidden`
  // and a transform of its own (index.css:613-628, :421-428), so a popover
  // anchored inside it cannot paint in either direction — and mid-match the
  // slot also drove the roster onto a second grid row. Sharing lives where the
  // result does: the score card's footer, and the permanent /g/:id page.

  return (
    <section
      className={`game-roster-band${isVisible ? ' is-visible' : ''}${presentation.isTeamGame ? ' is-team' : ' is-field'}${presentation.isComplete ? ' is-complete' : ' is-live'}`}
      aria-label="Players and match status"
      aria-hidden={!isVisible}
      data-testid="game-roster-band"
    >
      {showActions && (
        <div className="game-roster-action-slot is-left">
          {onMenu && (
            <div className="game-roster-actions is-left">
              <button
                type="button"
                onClick={onMenu}
                className="game-shell-button is-menu"
              >
                Menu
              </button>
            </div>
          )}
        </div>
      )}

      {presentation.isTeamGame ? (
        <div
          className="game-roster-matchup"
          data-testid="game-roster-matchup"
          data-roster-viewport="true"
        >
          <div
            className="game-roster-side is-blue"
            role="group"
            aria-label={leftSide?.label ?? 'Blue side'}
          >
            <div className="game-roster-players">
              {leftSide?.players.map((player) => (
                <PlayerLegend key={player.snakeId} player={player} side="left" />
              ))}
            </div>
          </div>

          <div className="game-roster-versus">
            <span className="game-roster-versus-label" aria-hidden="true">VS</span>
          </div>

          <div
            className="game-roster-side is-red"
            role="group"
            aria-label={rightSide?.label ?? 'Red side'}
          >
            <div className="game-roster-players">
              {rightSide?.players.map((player) => (
                <PlayerLegend key={player.snakeId} player={player} side="right" />
              ))}
            </div>
          </div>
        </div>
      ) : (
        <div className="game-roster-field" data-roster-viewport="true">
          <div className="game-roster-players">
            {presentation.players.map((player) => (
              <PlayerLegend key={player.snakeId} player={player} />
            ))}
          </div>
        </div>
      )}

      {showActions && (
        <div className="game-roster-action-slot is-right">
          <div className="game-roster-actions is-right">
            {onScoreCard && (
              <button
                type="button"
                onClick={onScoreCard}
                className="game-shell-button is-scorecard"
                aria-expanded={scoreCardOpen}
                aria-controls="game-over-scorecard"
              >
                Score card
              </button>
            )}
          </div>
        </div>
      )}
    </section>
  );
};

export default MatchRosterBand;
