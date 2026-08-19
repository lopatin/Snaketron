import type { BoostHudView } from './boostHud';
import type { BoostInputMode } from './boostInput';

/** Human-readable state for the meter's progressbar semantics. */
export function boostMeterValueText(hud: BoostHudView): string {
  return hud.unlimited
    ? `Unlimited${hud.active ? ', active' : ''}`
    : `${hud.percent}%${hud.active ? ', active' : ''}`;
}

/** Action-oriented label for the live Boost control. */
export function boostMeterControlLabel(
  hud: BoostHudView,
  inputMode: BoostInputMode,
): string {
  const charge = hud.unlimited
    ? 'unlimited'
    : `${hud.percent}% ${hud.active ? 'remaining' : 'charged'}`;

  if (inputMode === 'hold') {
    return `${hud.active ? 'Release' : 'Hold to'} Boost, ${charge}`;
  }

  return `${hud.active ? 'Stop' : 'Activate'} Boost, ${charge}`;
}
