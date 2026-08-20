import React from 'react';
import type { ComboHudView } from '../utils/comboHud';

export interface ComboCalloutProps {
  hud: ComboHudView;
  isVisible: boolean;
  pickupIdentity: string;
  /**
   * Measured by the arena so it can fade the callout while the local head
   * drives underneath it. Capture surfaces leave it unset and stay opaque.
   */
  containerRef?: React.Ref<HTMLDivElement>;
  /**
   * Capture-only deterministic animation clock. Live playback deliberately
   * leaves this unset so the production CSS animation follows wall time.
   */
  animationElapsedMs?: number;
}

/**
 * A transient arena announcement driven entirely by predicted engine state.
 * The live-region wrapper stays mounted between chains while the visual burst
 * appears once two quick setup pickups unlock +2, then remounts for every
 * enhanced pickup, including repeated food at the capped +3 tier.
 */
const ComboCallout: React.FC<ComboCalloutProps> = ({
  hud,
  isVisible,
  pickupIdentity,
  containerRef,
  animationElapsedMs,
}) => (
  <div
    ref={containerRef}
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
        style={animationElapsedMs === undefined ? undefined : {
          animationDelay: `${-Math.min(360, Math.max(0, animationElapsedMs))}ms`,
          animationPlayState: 'paused',
        }}
      >
        <span className="game-combo-callout__value">+{hud.nextFoodValue}</span>
        <span className="game-combo-callout__label">Combo!</span>
        <span
          className="game-combo-callout__meter"
          data-testid="combo-callout-meter"
          aria-hidden="true"
        >
          <span
            className="game-combo-callout__meter-fill"
            data-testid="combo-callout-meter-fill"
            data-fill-ratio={hud.fillRatio.toFixed(3)}
            style={{ transform: `scaleX(${hud.fillRatio})` }}
          />
        </span>
      </strong>
    )}
  </div>
);

export default ComboCallout;
