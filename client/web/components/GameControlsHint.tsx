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
          <fieldset
            className="boost-input-mode"
            role="radiogroup"
            aria-labelledby="boost-input-mode-label"
            data-testid="boost-input-mode"
          >
            <legend id="boost-input-mode-label">Space behavior</legend>
            <div className="boost-input-mode__choices">
              {(['hold', 'toggle'] as const).map((mode) => (
                <label
                  key={mode}
                  className="boost-input-mode__option"
                  title={mode === 'hold'
                    ? 'Boost while the Space key is held'
                    : 'Press the Space key to start or stop Boost'}
                >
                  <input
                    type="radio"
                    name="boost-input-mode"
                    value={mode}
                    checked={boostInputMode === mode}
                    onChange={() => onBoostInputModeChange(mode)}
                  />
                  <span>
                    <strong>{mode === 'hold' ? 'Hold' : 'Press'}</strong>
                    <small>{mode === 'hold' ? 'to boost' : 'to toggle'}</small>
                  </span>
                </label>
              ))}
            </div>
          </fieldset>
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
