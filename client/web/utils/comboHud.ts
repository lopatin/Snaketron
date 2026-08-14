/**
 * The deliberately small structural views keep this utility easy to exercise
 * without manufacturing a complete generated GameState in unit tests.
 */
interface ComboConfigView {
  window_ms: number;
  max_food_value: number;
}

interface ComboSnakeView {
  is_alive: boolean;
  combo: {
    chain_count: number;
    remaining_ms: number;
  };
}

export type ComboHudTone = 'idle' | 'building' | 'maxed';

export interface ComboHudView {
  active: boolean;
  remainingMs: number;
  fillRatio: number;
  nextFoodValue: number;
  nextLabel: string;
  tone: ComboHudTone;
  ariaValueText: string;
}

const finiteNonNegative = (value: number): number => (
  Number.isFinite(value) ? Math.max(0, value) : 0
);

/**
 * Build the local callout from predicted engine state. The engine owns time:
 * this function never starts a wall-clock timer that could drift through a
 * pause, reconciliation, reconnect, or custom tick duration.
 */
export function buildComboHudView(
  config: ComboConfigView,
  snake: ComboSnakeView,
): ComboHudView {
  const windowMs = finiteNonNegative(config.window_ms);
  const maxFoodValue = Math.max(1, Math.floor(finiteNonNegative(config.max_food_value)));
  const rawRemainingMs = finiteNonNegative(snake.combo.remaining_ms);
  const remainingMs = Math.min(windowMs, rawRemainingMs);
  const timerActive = snake.is_alive && windowMs > 0 && remainingMs > 0;
  const chainCount = Math.floor(finiteNonNegative(snake.combo.chain_count));
  // Pickup one primes the timer and pickup two is still worth one point. Only
  // after that second pickup does the player enter the visible +2 state.
  const nextFoodValue = timerActive
    ? Math.min(maxFoodValue, Math.max(1, chainCount))
    : 1;
  const active = timerActive && nextFoodValue > 1;
  const fillRatio = active ? remainingMs / windowMs : 0;
  const maxed = active && nextFoodValue >= maxFoodValue && maxFoodValue > 1;
  const tone: ComboHudTone = !active ? 'idle' : maxed ? 'maxed' : 'building';
  const nextLabel = active ? `+${nextFoodValue} Combo!` : '';
  const ariaValueText = !active
    ? 'Combo inactive'
    : `Combo active; next food is worth ${nextFoodValue} points${
        maxed ? ', maximum value' : ''
      }`;

  return {
    active,
    remainingMs,
    fillRatio,
    nextFoodValue,
    nextLabel,
    tone,
    ariaValueText,
  };
}
