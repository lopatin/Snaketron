export type BoostInputMode = 'hold' | 'toggle';
export type BoostInputCommand = 'ActivateBoost' | 'DeactivateBoost';
export type BoostKeyAction = 'ignore' | 'prevent-default' | 'activate';

export const DEFAULT_BOOST_INPUT_MODE: BoostInputMode = 'hold';
export const BOOST_INPUT_MODE_STORAGE_KEY = 'snaketron:boost-input-mode:v1';

interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

interface BoostKeyboardEventLike {
  code: string;
  repeat: boolean;
  target: EventTarget | null;
}

export interface BoostInputContext {
  active: boolean;
  canActivate: boolean;
  interactionActive: boolean;
  gameOver: boolean;
}

export interface BoostInputDecision {
  preventDefault: boolean;
  command: BoostInputCommand | null;
}

const IGNORE_DECISION: BoostInputDecision = {
  preventDefault: false,
  command: null,
};

const SUPPRESS_DECISION: BoostInputDecision = {
  preventDefault: true,
  command: null,
};

function browserStorage(): StorageLike | null {
  if (typeof window === 'undefined') {
    return null;
  }

  try {
    return window.localStorage;
  } catch {
    return null;
  }
}

export function loadBoostInputMode(
  storage: StorageLike | null = browserStorage(),
): BoostInputMode {
  if (!storage) {
    return DEFAULT_BOOST_INPUT_MODE;
  }

  try {
    const stored = storage.getItem(BOOST_INPUT_MODE_STORAGE_KEY);
    return stored === 'toggle' || stored === 'hold'
      ? stored
      : DEFAULT_BOOST_INPUT_MODE;
  } catch {
    return DEFAULT_BOOST_INPUT_MODE;
  }
}

export function persistBoostInputMode(
  mode: BoostInputMode,
  storage: StorageLike | null = browserStorage(),
): void {
  if (!storage) {
    return;
  }

  try {
    storage.setItem(BOOST_INPUT_MODE_STORAGE_KEY, mode);
  } catch {
    // A gameplay preference should never make the arena unusable when storage
    // is unavailable (private mode, quota, or an embedded browser policy).
  }
}

function closestInteractiveOwner(target: EventTarget & { closest?: unknown }): boolean {
  if (typeof target.closest !== 'function') {
    return false;
  }

  try {
    return Boolean(target.closest([
      'input',
      'textarea',
      'select',
      'button',
      'a[href]',
      '[contenteditable]:not([contenteditable="false"])',
      '[role="button"]',
      '[role="switch"]',
      '[role="radio"]',
      '[role="textbox"]',
      '[role="combobox"]',
      '[role="slider"]',
    ].join(',')));
  } catch {
    return false;
  }
}

/**
 * Interactive controls own gameplay keys. This applies to arrows as well as
 * Space so changing a radio option or editing chat cannot steer the snake.
 */
export function targetOwnsGameplayKeys(target: EventTarget | null): boolean {
  if (!target || typeof target !== 'object') {
    return false;
  }

  const element = target as EventTarget & {
    tagName?: unknown;
    isContentEditable?: unknown;
    closest?: unknown;
  };
  const tagName = typeof element.tagName === 'string'
    ? element.tagName.toUpperCase()
    : '';

  return (
    ['INPUT', 'TEXTAREA', 'SELECT', 'BUTTON', 'A'].includes(tagName) ||
    element.isContentEditable === true ||
    closestInteractiveOwner(element)
  );
}

/** Kept as the narrower public helper used by existing shortcut tests. */
export function isTextEntryTarget(target: EventTarget | null): boolean {
  if (!target || typeof target !== 'object') {
    return false;
  }

  const element = target as EventTarget & {
    tagName?: unknown;
    isContentEditable?: unknown;
  };
  const tagName = typeof element.tagName === 'string'
    ? element.tagName.toUpperCase()
    : '';

  return (
    tagName === 'INPUT' ||
    tagName === 'TEXTAREA' ||
    tagName === 'SELECT' ||
    element.isContentEditable === true
  );
}

/**
 * Legacy one-shot classifier retained for callers outside the controller.
 * Repeats suppress scrolling, while any interactive target keeps ownership.
 */
export function getBoostKeyAction(event: BoostKeyboardEventLike): BoostKeyAction {
  if (event.code !== 'Space' || targetOwnsGameplayKeys(event.target)) {
    return 'ignore';
  }

  return event.repeat ? 'prevent-default' : 'activate';
}

/**
 * Pure mutable input controller. It tracks physical Space edges separately
 * from predicted Boost state, which makes keyup-after-focus-change and rapid
 * Toggle presses deterministic without waiting for React's next paint.
 */
export class BoostInputController {
  private mode: BoostInputMode;
  private physicalSpaceDown = false;
  private physicalPointerDown = false;
  private desiredTarget: boolean | null = null;
  private lastSentTarget: boolean | null = null;
  private releasePending = false;

  constructor(mode: BoostInputMode = DEFAULT_BOOST_INPUT_MODE) {
    this.mode = mode;
  }

  getMode(): BoostInputMode {
    return this.mode;
  }

  isSpaceDown(): boolean {
    return this.physicalSpaceDown;
  }

  private issue(target: boolean): BoostInputCommand | null {
    if (this.lastSentTarget === target) {
      return null;
    }

    this.lastSentTarget = target;
    this.desiredTarget = target;
    return target ? 'ActivateBoost' : 'DeactivateBoost';
  }

  handleKeyDown(
    event: BoostKeyboardEventLike,
    context: BoostInputContext,
  ): BoostInputDecision {
    if (
      event.code !== 'Space' ||
      context.gameOver ||
      targetOwnsGameplayKeys(event.target)
    ) {
      return IGNORE_DECISION;
    }

    if (event.repeat || this.physicalSpaceDown) {
      return SUPPRESS_DECISION;
    }

    if (!context.interactionActive) {
      return SUPPRESS_DECISION;
    }

    this.physicalSpaceDown = true;

    if (this.mode === 'hold') {
      if (context.active) {
        this.desiredTarget = true;
        return SUPPRESS_DECISION;
      }
      if (!context.canActivate) {
        this.desiredTarget = false;
        return SUPPRESS_DECISION;
      }
      return {
        preventDefault: true,
        command: this.issue(true),
      };
    }

    const currentlyDesired = this.desiredTarget ?? context.active;
    const nextTarget = !currentlyDesired;
    if (nextTarget && !context.canActivate) {
      this.desiredTarget = false;
      return SUPPRESS_DECISION;
    }

    return {
      preventDefault: true,
      command: this.issue(nextTarget),
    };
  }

  handleKeyUp(
    event: BoostKeyboardEventLike,
    context: BoostInputContext,
  ): BoostInputDecision {
    if (event.code !== 'Space') {
      return IGNORE_DECISION;
    }

    // A keyup that began in a text/control target never entered gameplay.
    // Conversely, once gameplay owns the keydown, release must be honored even
    // if focus moved into a control while Space was held.
    if (!this.physicalSpaceDown) {
      return IGNORE_DECISION;
    }

    this.physicalSpaceDown = false;
    if (this.mode === 'toggle') {
      return SUPPRESS_DECISION;
    }

    const shouldStop = this.desiredTarget === true;
    this.desiredTarget = false;
    if (!shouldStop) {
      return SUPPRESS_DECISION;
    }

    if (!context.interactionActive || context.gameOver) {
      this.releasePending = !context.gameOver;
      return SUPPRESS_DECISION;
    }

    return {
      preventDefault: true,
      command: this.issue(false),
    };
  }

  handleButtonPress(context: BoostInputContext): BoostInputDecision {
    // In Hold mode pointer edges own activation. The click synthesized after
    // pointerup must not turn a momentary hold into a latch.
    if (this.mode !== 'toggle' || !context.interactionActive || context.gameOver) {
      return IGNORE_DECISION;
    }

    const currentlyDesired = this.desiredTarget ?? context.active;
    if (!currentlyDesired && !context.canActivate) {
      return IGNORE_DECISION;
    }

    return {
      preventDefault: false,
      command: this.issue(!currentlyDesired),
    };
  }

  handlePointerDown(context: BoostInputContext): BoostInputDecision {
    if (this.mode !== 'hold' || !context.interactionActive || context.gameOver) {
      return IGNORE_DECISION;
    }

    if (this.physicalPointerDown) {
      return SUPPRESS_DECISION;
    }
    this.physicalPointerDown = true;

    if (context.active) {
      this.desiredTarget = true;
      return SUPPRESS_DECISION;
    }
    if (!context.canActivate) {
      this.desiredTarget = false;
      return SUPPRESS_DECISION;
    }

    return {
      preventDefault: true,
      command: this.issue(true),
    };
  }

  handlePointerUp(context: BoostInputContext): BoostInputDecision {
    if (this.mode !== 'hold' || !this.physicalPointerDown) {
      return IGNORE_DECISION;
    }

    this.physicalPointerDown = false;
    const shouldStop = this.desiredTarget === true;
    this.desiredTarget = false;
    if (!shouldStop) {
      return SUPPRESS_DECISION;
    }

    if (!context.interactionActive || context.gameOver) {
      this.releasePending = !context.gameOver;
      return SUPPRESS_DECISION;
    }

    return {
      preventDefault: true,
      command: this.issue(false),
    };
  }

  /**
   * Safety release for loss of physical-key ownership. Hold stops; Toggle
   * keeps its explicit latched intent but clears the physical key edge so the
   * next press is accepted even when the browser swallowed keyup.
   */
  releaseHeld(context: BoostInputContext): BoostInputDecision {
    this.physicalSpaceDown = false;
    this.physicalPointerDown = false;
    if (this.mode === 'toggle') {
      return IGNORE_DECISION;
    }

    return this.cleanup(context);
  }

  /** Stop either mode's current intent during explicit arena teardown. */
  cleanup(context: BoostInputContext): BoostInputDecision {
    const shouldStop = this.desiredTarget === true;
    this.physicalSpaceDown = false;
    this.physicalPointerDown = false;
    this.desiredTarget = false;

    if (!shouldStop || context.gameOver) {
      this.releasePending = false;
      return IGNORE_DECISION;
    }

    if (!context.interactionActive) {
      this.releasePending = true;
      return IGNORE_DECISION;
    }

    this.releasePending = false;
    return {
      preventDefault: false,
      command: this.issue(false),
    };
  }

  /** Explicit route/arena teardown stops authoritative or pending Boost before
   * LeaveGame clears the command channel's game identity. */
  teardown(context: BoostInputContext): BoostInputDecision {
    const shouldStop = (
      this.desiredTarget === true ||
      this.lastSentTarget === true ||
      this.releasePending ||
      context.active
    );
    this.physicalSpaceDown = false;
    this.physicalPointerDown = false;
    this.desiredTarget = false;
    this.releasePending = false;

    // Route transitions render with the next route identity before effects
    // tear down the old arena, so `interactionActive` may already be false
    // while the old socket still has a valid game identity. Teardown is the
    // last safe opportunity to publish the stop before LeaveGame clears it.
    if (!shouldStop || context.gameOver) {
      return IGNORE_DECISION;
    }

    return {
      preventDefault: false,
      command: this.issue(false),
    };
  }

  reconcile(context: BoostInputContext): BoostInputDecision {
    if (this.lastSentTarget !== null && context.active === this.lastSentTarget) {
      this.lastSentTarget = null;
    }

    if (this.mode === 'toggle' && this.lastSentTarget === null) {
      this.desiredTarget = context.active;
    }

    const physicalHoldContinues = this.physicalSpaceDown || this.physicalPointerDown;
    const depletedHoldIntent = (
      this.mode === 'hold' &&
      physicalHoldContinues &&
      this.desiredTarget === true &&
      !context.active &&
      context.interactionActive &&
      !context.gameOver
    );

    // Depletion ends the current activation, not the physical Hold intent. A
    // render may skip the final active frame for a one-quantum tank, so clear a
    // still-pending start once the held snake is observably inactive and empty.
    // This lets a later packet collection create one fresh activation edge.
    if (depletedHoldIntent && !context.canActivate && this.lastSentTarget === true) {
      this.lastSentTarget = null;
    }

    if (depletedHoldIntent && context.canActivate) {
      return {
        preventDefault: false,
        command: this.issue(true),
      };
    }

    // Hold has no durable on-state. If a fresh/reconnected snapshot says the
    // snake is active while no physical hold or pending activation owns it,
    // repair the missed release immediately.
    if (
      this.mode === 'hold' &&
      context.active &&
      !this.physicalSpaceDown &&
      this.desiredTarget !== true &&
      !this.releasePending &&
      context.interactionActive &&
      !context.gameOver
    ) {
      return {
        preventDefault: false,
        command: this.issue(false),
      };
    }

    if (!this.releasePending) {
      return IGNORE_DECISION;
    }

    if (!context.active) {
      this.releasePending = false;
      return IGNORE_DECISION;
    }

    if (!context.interactionActive || context.gameOver) {
      return IGNORE_DECISION;
    }

    this.releasePending = false;
    return {
      preventDefault: false,
      command: this.issue(false),
    };
  }

  setMode(mode: BoostInputMode, context: BoostInputContext): BoostInputDecision {
    if (mode === this.mode) {
      return IGNORE_DECISION;
    }

    const decision = this.cleanup(context);
    this.mode = mode;
    return decision;
  }

  reset(): void {
    this.physicalSpaceDown = false;
    this.physicalPointerDown = false;
    this.desiredTarget = null;
    this.lastSentTarget = null;
    this.releasePending = false;
  }
}
