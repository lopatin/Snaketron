import React from 'react';
import {
  COMBO_HUD_SEGMENT_COUNT,
  buildComboHudSegments,
  type ComboHudView,
} from '../utils/comboHud';

export interface ComboChaseRailProps {
  hud: ComboHudView;
  isVisible: boolean;
}

/**
 * A read-only countdown attached to the arena. Filled cells collect at the
 * leading edge, so the tail disappears as the combo window runs out.
 */
const ComboChaseRail: React.FC<ComboChaseRailProps> = ({ hud, isVisible }) => (
  <div
    className={`game-combo-hud is-${hud.tone}${hud.active ? ' is-active' : ''}${
      isVisible ? ' is-visible' : ''
    }`}
    data-testid="combo-hud"
    data-location="arena-bottom"
    data-active={hud.active ? 'true' : 'false'}
    data-next-food-value={hud.nextFoodValue}
  >
    <div
      className="game-combo-rail"
      role="progressbar"
      aria-label="Combo countdown"
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={hud.percent}
      aria-valuetext={hud.ariaValueText}
    >
      <strong className="game-combo-rail__title">COMBO</strong>
      <span className="game-combo-rail__cells" aria-hidden="true">
        {buildComboHudSegments(hud.filledSegments).map((filled, index) => (
          <span
            className={`game-combo-rail__cell${filled ? ' is-filled' : ''}${
              index === COMBO_HUD_SEGMENT_COUNT - 1 ? ' is-head' : ''
            }`}
            key={index}
          />
        ))}
      </span>
      <strong className="game-combo-rail__next">{hud.nextLabel}</strong>
    </div>
  </div>
);

export default ComboChaseRail;
