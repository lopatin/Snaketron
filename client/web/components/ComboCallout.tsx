import React from 'react';
import type { ComboHudView } from '../utils/comboHud';

export interface ComboCalloutProps {
  hud: ComboHudView;
  isVisible: boolean;
  pickupIdentity: string;
}

/**
 * A transient arena announcement driven entirely by predicted engine state.
 * The live-region wrapper stays mounted between chains while the visual burst
 * remounts for every pickup, including repeated food at the capped +3 tier.
 */
const ComboCallout: React.FC<ComboCalloutProps> = ({
  hud,
  isVisible,
  pickupIdentity,
}) => (
  <div
    className="game-combo-callout"
    role="status"
    aria-live="polite"
    aria-atomic="true"
    data-testid="combo-callout"
    data-active={hud.active && isVisible ? 'true' : 'false'}
  >
    <span className="sr-only" data-testid="combo-callout-announcement">
      {hud.active && isVisible ? hud.ariaValueText : ''}
    </span>
    {hud.active && isVisible && (
      <strong
        key={pickupIdentity}
        className={`game-combo-callout__burst is-${hud.tone}`}
        data-testid="combo-callout-burst"
        data-animation-key={pickupIdentity}
        data-next-food-value={hud.nextFoodValue}
        aria-hidden="true"
      >
        <span className="game-combo-callout__value">+{hud.nextFoodValue}</span>
        <span className="game-combo-callout__label">Combo!</span>
      </strong>
    )}
  </div>
);

export default ComboCallout;
