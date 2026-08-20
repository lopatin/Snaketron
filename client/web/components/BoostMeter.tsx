import React from 'react';
import type { BoostHudView } from '../utils/boostHud';
import type { BoostInputMode } from '../utils/boostInput';
import {
  boostMeterControlLabel,
  boostMeterValueText,
} from '../utils/boostMeterPresentation';
import BoostCanisterMark from './BoostCanisterMark';

export interface BoostMeterInteraction {
  inputMode: BoostInputMode;
  onClick: React.MouseEventHandler<HTMLButtonElement>;
  onKeyDown: React.KeyboardEventHandler<HTMLButtonElement>;
  onPointerDown: React.PointerEventHandler<HTMLButtonElement>;
  onPointerRelease: React.PointerEventHandler<HTMLButtonElement>;
}

export type BoostMeterOrientation = 'horizontal' | 'vertical';

interface BoostMeterBaseProps {
  hud: BoostHudView;
  isVisible?: boolean;
  location?: string;
  /**
   * The live arena docks the meter under the field, so it reads horizontally
   * and its top edge is open into the arena border. A replay is framed 21:9
   * and cannot spare the vertical band, so it stands the same meter on its
   * right edge as a closed rectangle. Only the fill axis differs in markup;
   * everything else is CSS.
   */
  orientation?: BoostMeterOrientation;
}

export type BoostMeterProps = BoostMeterBaseProps & (
  | {
    mode?: 'interactive';
    interaction: BoostMeterInteraction;
  }
  | {
    mode: 'display';
    interaction?: never;
  }
);

/**
 * The shared Boost HUD used by the live arena and deterministic scenarios.
 * Display mode keeps the production visual and progress semantics while
 * making the button-shaped surface inert.
 */
const BoostMeter: React.FC<BoostMeterProps> = (props) => {
  const {
    hud,
    isVisible = true,
    location = 'arena-bottom',
    orientation = 'horizontal',
  } = props;
  const interaction = props.mode === 'display' ? null : props.interaction;
  const vertical = orientation === 'vertical';

  return (
    <div
      className={`game-boost-hud${isVisible ? ' is-visible' : ''}${hud.active ? ' is-active' : ''}${hud.ready ? ' is-ready' : ''}${vertical ? ' is-vertical' : ''}`}
      data-testid="boost-hud"
      data-location={location}
      data-orientation={orientation}
      data-ready={hud.ready ? 'true' : 'false'}
    >
      <span
        className="game-boost-meter__track"
        role="progressbar"
        aria-label="Stored Boost charge"
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={hud.percent}
        aria-valuetext={boostMeterValueText(hud)}
      >
        <span
          className="game-boost-meter__fill"
          style={{
            transform: vertical
              ? `scaleY(${hud.fillRatio})`
              : `scaleX(${hud.fillRatio})`,
          }}
        />
      </span>
      <button
        type="button"
        onClick={interaction?.onClick}
        onKeyDown={interaction?.onKeyDown}
        onPointerDown={interaction?.onPointerDown}
        onPointerUp={interaction?.onPointerRelease}
        onPointerCancel={interaction?.onPointerRelease}
        onLostPointerCapture={interaction?.onPointerRelease}
        disabled={interaction ? hud.buttonDisabled : true}
        aria-label={interaction
          ? boostMeterControlLabel(hud, interaction.inputMode)
          : undefined}
        aria-keyshortcuts={interaction ? 'Space' : undefined}
        aria-hidden={interaction ? undefined : true}
        tabIndex={interaction ? undefined : -1}
        className="game-boost-meter"
        data-testid="boost-button"
      >
        <span className="game-boost-meter__canister-dock" aria-hidden="true">
          <BoostCanisterMark />
        </span>
        <span className="game-boost-meter__reservoir" aria-hidden="true" />
        <strong className="game-boost-meter__value">
          {hud.unlimited ? '∞' : `${hud.percent}%`}
        </strong>
      </button>
    </div>
  );
};

export default BoostMeter;
