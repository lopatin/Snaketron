import React from 'react';
import type { GameState, QueueMode } from '../types';
import { buildMatchPresentation } from '../utils/gamePresentation';

export interface ScoreboardProps {
  gameState: GameState | null;
  isVisible: boolean;
  currentUserId?: number;
  queueMode?: QueueMode;
  onMenu?: () => void;
}

const Scoreboard: React.FC<ScoreboardProps> = ({
  gameState,
  isVisible,
  currentUserId,
  queueMode,
  onMenu,
}) => {
  const presentation = gameState
    ? buildMatchPresentation(gameState, currentUserId, queueMode)
    : null;
  const leftSide = presentation?.sides[0];
  const rightSide = presentation?.sides[1];
  const brand = <img src="SnaketronLogo.png" alt="Snaketron" />;

  return (
    <header
      className={`game-scoreboard${isVisible ? ' is-visible' : ''}`}
      aria-hidden={!isVisible}
      data-testid="game-scoreboard"
    >
      {presentation?.isComplete && onMenu ? (
        <button
          type="button"
          className="game-scoreboard-brand is-home-action"
          onClick={onMenu}
          aria-label="Main menu"
          title="Main menu"
          data-testid="scoreboard-home"
        >
          {brand}
        </button>
      ) : (
        <div className="game-scoreboard-brand">{brand}</div>
      )}

      <span className="game-scoreboard-divider" aria-hidden="true" />

      <div
        className={`game-scoreboard-match${presentation?.isTeamGame ? ' is-team' : ' is-solo'}`}
        aria-label={presentation?.isTeamGame ? 'Team score and match clock' : 'Score and match clock'}
      >
        {presentation?.isTeamGame ? (
          <strong
            className="game-scoreboard-team-score is-blue"
            aria-label={`${leftSide?.label ?? 'Blue side'} ${leftSide?.score ?? 0}`}
            data-testid="scoreboard-left-score"
          >
            {leftSide?.score ?? 0}
          </strong>
        ) : (
          <div className="game-scoreboard-solo-score">
            <span>Score</span>
            <strong data-testid="scoreboard-left-score">{presentation?.soloScore ?? 0}</strong>
          </div>
        )}

        <div className="game-scoreboard-time">
          <span>{presentation?.timeLabel ?? 'Time'}</span>
          <time data-testid="scoreboard-clock">{presentation?.timeValue ?? '00:00'}</time>
        </div>

        {presentation?.isTeamGame && (
          <strong
            className="game-scoreboard-team-score is-red"
            aria-label={`${rightSide?.label ?? 'Red side'} ${rightSide?.score ?? 0}`}
            data-testid="scoreboard-right-score"
          >
            {rightSide?.score ?? 0}
          </strong>
        )}
      </div>

      <span className="game-scoreboard-divider" aria-hidden="true" />

      <div className="game-scoreboard-mode" aria-label={`Mode: ${presentation?.modeLabel ?? 'Match'}`}>
        <strong>{presentation?.modeLabel ?? 'Match'}</strong>
      </div>
    </header>
  );
};

export default Scoreboard;
