interface BoostConfigView {
  speed_milli: number;
  capacity_ms: number;
}

interface BoostSnakeView {
  is_alive: boolean;
  boost: {
    charge_ms: number;
    active: boolean;
  };
}

export interface BoostHudView {
  chargeMs: number;
  percent: number;
  fillRatio: number;
  active: boolean;
  ready: boolean;
  multiplier: number;
  buttonDisabled: boolean;
}

/** Build the local HUD from predicted engine state; no client countdown. */
export function buildBoostHudView(
  config: BoostConfigView,
  snake: BoostSnakeView,
  interactionActive: boolean,
  gameOver: boolean,
): BoostHudView {
  const capacityMs = Math.max(0, config.capacity_ms);
  const chargeMs = Math.min(capacityMs, Math.max(0, snake.boost.charge_ms));
  const fillRatio = capacityMs > 0 ? chargeMs / capacityMs : 0;
  // Rounded display values must not claim the tank is full while the exact
  // charge (and full-width fill) still has room remaining.
  const percent = capacityMs <= 0
    ? 0
    : chargeMs >= capacityMs
      ? 100
      : Math.min(99, Math.max(0, Math.round(fillRatio * 100)));
  const active = snake.boost.active;
  const ready = capacityMs > 0 && chargeMs === capacityMs && !active;

  return {
    chargeMs,
    percent,
    fillRatio,
    active,
    ready,
    multiplier: Number((config.speed_milli / 1000).toFixed(2)),
    // An empty meter must not disable the control. Boost is a held level, and
    // holding it on empty is how a player arms the next packet — disabling the
    // button would swallow the press exactly like the old input bug did.
    buttonDisabled: !interactionActive || !snake.is_alive || gameOver,
  };
}
