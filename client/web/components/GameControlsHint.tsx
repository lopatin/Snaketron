import React from 'react';
import type { BoostInputMode } from '../utils/boostInput';

interface GameControlsHintProps {
  showBoost: boolean;
  boostInputMode: BoostInputMode;
  onBoostInputModeChange: (mode: BoostInputMode) => void;
  /** Re-opens this mode's pre-match briefing. Omitted when there isn't one. */
  onOpenHelp?: () => void;
}

/**
 * Quiet, glanceable controls legend. It deliberately sits outside the arena
 * so input help never competes with the playfield or the arena utility rail.
 */
export function GameControlsHint({
  showBoost,
  boostInputMode,
  onBoostInputModeChange,
  onOpenHelp,
}: GameControlsHintProps) {
  return (
    <aside className="game-controls-hint" aria-label="Game controls" data-testid="game-controls-hint">
      <div className="game-controls-hint__group is-move">
        <span className="game-controls-hint__label">Move</span>
        <span className="arrow-key-cluster" aria-hidden="true">
          <kbd className="control-key control-key--up">↑</kbd>
          <kbd className="control-key control-key--left">←</kbd>
          <kbd className="control-key control-key--down">↓</kbd>
          <kbd className="control-key control-key--right">→</kbd>
        </span>
        <span className="sr-only">with the arrow keys</span>
      </div>

      {showBoost && (
        <div className="game-controls-hint__group is-boost">
          <span className="game-controls-hint__label">Boost</span>
          <kbd className="control-key control-key--space" aria-label="Space key">
            <span aria-hidden="true" />
          </kbd>
          <label
            className="boost-input-mode"
            data-testid="boost-input-mode"
            title="Checked: boost while Space is held. Unchecked: press Space to start or stop Boost."
          >
            <input
              type="checkbox"
              name="boost-input-mode"
              checked={boostInputMode === 'hold'}
              onChange={(event) =>
                onBoostInputModeChange(event.target.checked ? 'hold' : 'toggle')}
            />
            <span className="boost-input-mode__box" aria-hidden="true" />
            <span className="boost-input-mode__text">Hold to boost</span>
          </label>
          <span className="sr-only">
            {boostInputMode === 'hold'
              ? 'Hold the Space key to boost'
              : 'Press the Space key to start or stop boost'}
          </span>
        </div>
      )}

      {onOpenHelp && (
        <div className="game-controls-hint__group is-help">
          <button
            type="button"
            className="game-controls-hint__help"
            onClick={onOpenHelp}
            aria-label="How this mode works"
            title="How this mode works"
            data-testid="tutorial-help-button"
          >
            <span aria-hidden="true">?</span>
          </button>
        </div>
      )}
    </aside>
  );
}

export default GameControlsHint;
