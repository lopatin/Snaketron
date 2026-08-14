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

export const COMBO_HUD_SEGMENT_COUNT = 10;

export type ComboHudTone = 'idle' | 'building' | 'maxed';

export interface ComboHudView {
  active: boolean;
  remainingMs: number;
  fillRatio: number;
  percent: number;
  filledSegments: number;
  nextFoodValue: number;
  nextLabel: string;
  tone: ComboHudTone;
  ariaValueText: string;
}

const finiteNonNegative = (value: number): number => (
  Number.isFinite(value) ? Math.max(0, value) : 0
);

/** Decorative segment occupancy, tail-to-head from left to right. */
export function buildComboHudSegments(filledSegments: number): boolean[] {
  const boundedFill = Math.min(
    COMBO_HUD_SEGMENT_COUNT,
    Math.max(0, Math.floor(finiteNonNegative(filledSegments))),
  );
  return Array.from(
    { length: COMBO_HUD_SEGMENT_COUNT },
    (_, index) => index >= COMBO_HUD_SEGMENT_COUNT - boundedFill,
  );
}

/**
 * Build the local countdown from predicted engine state. The engine owns time:
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
  const active = snake.is_alive && windowMs > 0 && remainingMs > 0;
  const chainCount = Math.floor(finiteNonNegative(snake.combo.chain_count));
  const nextFoodValue = active
    ? Math.min(maxFoodValue, chainCount + 1)
    : 1;
  const fillRatio = active ? remainingMs / windowMs : 0;
  const percent = active ? Math.max(1, Math.ceil(fillRatio * 100)) : 0;
  const filledSegments = active
    ? Math.max(1, Math.min(
        COMBO_HUD_SEGMENT_COUNT,
        Math.ceil(fillRatio * COMBO_HUD_SEGMENT_COUNT),
      ))
    : 0;
  const maxed = active && nextFoodValue >= maxFoodValue && maxFoodValue > 1;
  const tone: ComboHudTone = !active ? 'idle' : maxed ? 'maxed' : 'building';
  const nextLabel = !active
    ? 'EAT TO START'
    : maxed
      ? `+${nextFoodValue} MAX`
      : `NEXT +${nextFoodValue}`;
  const ariaValueText = !active
    ? 'Combo inactive; eat food to start'
    : `${remainingMs} milliseconds remaining; next food is worth ${nextFoodValue} points${
        maxed ? ', maximum value' : ''
      }`;

  return {
    active,
    remainingMs,
    fillRatio,
    percent,
    filledSegments,
    nextFoodValue,
    nextLabel,
    tone,
    ariaValueText,
  };
}
