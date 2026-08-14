import { gameStorage } from '../services/gameStorage.ts';

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

/**
 * What the controller is allowed to know.
 *
 * Deliberately absent: anything about whether Boost is *currently possible*
 * (fuel level, cooldown, respawn timing). The original bug — hold Space on an
 * empty meter, collect a packet, and stay slow until you release and press
 * again — came from gating the player's intent on a transient condition, which
 * threw the intent away instead of deferring it. The engine now latches intent
 * and starts Boost the moment it can, so the client only has to publish the
 * level. Keeping fuel out of this type makes the old mistake unstateable.
 */
export interface BoostInputContext {
  /** Predicted/authoritative: is the snake boosting right now? */
  active: boolean;
  /** The engine's latched copy of what this player last asked for. */
  intent: boolean;
  /** Commands for this game can reach the server right now. */
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
  return typeof window === 'undefined' ? null : gameStorage;
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

const ARROW_NAVIGABLE_SELECTOR = [
  'input',
  'textarea',
  'select',
  '[contenteditable]:not([contenteditable="false"])',
  '[role="textbox"]',
  '[role="combobox"]',
  '[role="listbox"]',
  '[role="radio"]',
  '[role="radiogroup"]',
  '[role="slider"]',
  '[role="spinbutton"]',
  '[role="menu"]',
  '[role="menuitem"]',
  '[role="tablist"]',
  '[role="tree"]',
].join(',');

/**
 * Arrow keys have a narrower owner set than Space.
 *
 * A focused plain button or link does nothing with arrow keys, but clicking one
 * — Boost, Menu, Play Again — leaves it focused, and treating it as an owner
 * silently killed every turn until focus moved elsewhere. Text entry and
 * arrow-navigable widgets (the Hold/Press radios, selects, sliders) genuinely
 * own arrows and keep them.
 */
export function targetOwnsArrowKeys(target: EventTarget | null): boolean {
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

  if (
    ['INPUT', 'TEXTAREA', 'SELECT'].includes(tagName) ||
    element.isContentEditable === true
  ) {
    return true;
  }

  if (typeof element.closest !== 'function') {
    return false;
  }

  try {
    return Boolean(element.closest(ARROW_NAVIGABLE_SELECTOR));
  } catch {
    return false;
  }
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
 * Pure mutable input controller.
 *
 * The controller holds only *physical* facts — is the key down, is the pointer
 * down, is the Toggle latched — and derives a single desired level from them.
 * Every entry point funnels through `sync`, which publishes a command only when
 * the desired level disagrees with the engine's latched intent. There is no
 * path that inspects fuel, and therefore no path that can silently discard what
 * the player asked for.
 */
export class BoostInputController {
  private mode: BoostInputMode;
  private physicalSpaceDown = false;
  /**
   * Pointer identities, rather than a count, keep the controller and each DOM
   * binding on one source of truth. Mobile browsers can omit a button-level
   * release when the page blurs, the control is disabled, or pointer capture
   * is unavailable. Safety resets clear this set; a binding can then recognize
   * that its cached pointer id is stale and accept the next physical press.
   */
  private physicalPointerHolds = new Set<number>();
  /** Toggle mode's durable latch. Hold mode derives its level from the edges. */
  private toggleLatched = false;
  /** The level already published and not yet echoed back by the engine. */
  private pendingLevel: boolean | null = null;
  /** Edge detector for reconnects, which are when a resend is worthwhile. */
  private interactionWasActive = false;
  /** A terminal server rejection must wait for a fresh physical input edge. */
  private reconciliationSuppressed = false;

  constructor(mode: BoostInputMode = DEFAULT_BOOST_INPUT_MODE) {
    this.mode = mode;
  }

  getMode(): BoostInputMode {
    return this.mode;
  }

  isSpaceDown(): boolean {
    return this.physicalSpaceDown;
  }

  /** Whether a DOM binding's cached pointer still owns a live Hold edge. */
  isPointerHeld(pointerId: number): boolean {
    return this.physicalPointerHolds.has(pointerId);
  }

  /** The one definition of what the player is currently asking for. */
  private desiredLevel(): boolean {
    return this.mode === 'hold'
      ? this.physicalSpaceDown || this.physicalPointerHolds.size > 0
      : this.toggleLatched;
  }

  /**
   * Publish the desired level if the engine does not already hold it.
   *
   * `force` is for arena teardown, which must get a release out while the
   * socket still knows which game it belongs to even though the next route has
   * already made interaction inactive.
   */
  private sync(context: BoostInputContext, force = false): BoostInputCommand | null {
    const reconnected = context.interactionActive && !this.interactionWasActive;
    this.interactionWasActive = context.interactionActive;

    if (context.gameOver) {
      this.pendingLevel = null;
      return null;
    }

    if (this.reconciliationSuppressed && !force) {
      return null;
    }

    // Interaction resuming is the one moment a resend is worth it: whatever we
    // last published may never have reached the engine. Forgetting it here
    // makes the comparison below republish the level if it still disagrees.
    if (reconnected) {
      this.pendingLevel = null;
    }

    if (!context.interactionActive && !force) {
      // Hold the intent rather than dropping it: whatever the player wants is
      // still true, and the next sync after reconnect publishes it.
      return null;
    }

    // The engine has caught up with our last request; stop tracking it.
    if (this.pendingLevel !== null && context.intent === this.pendingLevel) {
      this.pendingLevel = null;
    }

    // Compare against what the engine will hold once anything in flight lands,
    // not against what it holds right now. A press and a quick release inside
    // one round trip would otherwise look like "already correct" and cancel
    // into silence, stranding the engine on the press.
    const desired = this.desiredLevel();
    const effective = this.pendingLevel ?? context.intent;
    if (effective === desired) {
      return null;
    }

    this.pendingLevel = desired;
    return desired ? 'ActivateBoost' : 'DeactivateBoost';
  }

  private decide(
    context: BoostInputContext,
    preventDefault: boolean,
    force = false,
  ): BoostInputDecision {
    return { preventDefault, command: this.sync(context, force) };
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

    this.reconciliationSuppressed = false;
    // The key is down whether or not the game can act on it yet. Recording it
    // unconditionally is what lets a press made during a countdown, a respawn,
    // or a reconnect take effect the moment play resumes.
    this.physicalSpaceDown = true;
    if (this.mode === 'toggle') {
      this.toggleLatched = !this.toggleLatched;
    }

    return this.decide(context, true);
  }

  /**
   * Space pressed while a modal owns the screen is never gameplay intent.
   * Native controls keep their Space behavior; a press on the dialog/body is
   * suppressed and clears local edges so it cannot be replayed when the modal
   * closes and interaction becomes active again.
   */
  suppressModalKeyDown(event: BoostKeyboardEventLike): BoostInputDecision {
    if (event.code !== 'Space' || targetOwnsGameplayKeys(event.target)) {
      return IGNORE_DECISION;
    }

    this.reset();
    return SUPPRESS_DECISION;
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

    this.reconciliationSuppressed = false;
    this.physicalSpaceDown = false;
    return this.decide(context, true);
  }

  handleButtonPress(context: BoostInputContext): BoostInputDecision {
    // In Hold mode pointer edges own activation. The click synthesized after
    // pointerup must not turn a momentary hold into a latch.
    if (this.mode !== 'toggle' || context.gameOver) {
      return IGNORE_DECISION;
    }

    this.reconciliationSuppressed = false;
    this.toggleLatched = !this.toggleLatched;
    return this.decide(context, false);
  }

  handlePointerDown(
    pointerId: number,
    context: BoostInputContext,
  ): BoostInputDecision {
    if (this.mode !== 'hold' || context.gameOver) {
      return IGNORE_DECISION;
    }

    if (this.physicalPointerHolds.has(pointerId)) {
      return SUPPRESS_DECISION;
    }

    this.reconciliationSuppressed = false;
    // A second button's press while another is held records a real physical
    // fact; sync() publishes nothing because the level is already true, and
    // the hold now ends only when the LAST button is released.
    this.physicalPointerHolds.add(pointerId);
    return this.decide(context, true);
  }

  handlePointerUp(
    pointerId: number,
    context: BoostInputContext,
  ): BoostInputDecision {
    if (this.mode !== 'hold' || !this.physicalPointerHolds.delete(pointerId)) {
      return IGNORE_DECISION;
    }

    this.reconciliationSuppressed = false;
    return this.decide(context, true);
  }

  /**
   * Safety release for loss of physical-key ownership (blur, tab hidden).
   * Hold stops because the key edges are gone; Toggle keeps its explicit
   * latched intent but clears the physical edge so the next press is accepted
   * even when the browser swallowed keyup.
   */
  releaseHeld(context: BoostInputContext): BoostInputDecision {
    this.reconciliationSuppressed = false;
    this.physicalSpaceDown = false;
    this.physicalPointerHolds.clear();
    return this.decide(context, false);
  }

  /** Stop either mode's current intent during explicit arena teardown. */
  cleanup(context: BoostInputContext): BoostInputDecision {
    this.reconciliationSuppressed = false;
    this.physicalSpaceDown = false;
    this.physicalPointerHolds.clear();
    this.toggleLatched = false;
    return this.decide(context, false);
  }

  /**
   * Explicit route/arena teardown. Route transitions render with the next
   * route identity before effects tear down the old arena, so
   * `interactionActive` may already be false while the old socket still has a
   * valid game identity; this is the last safe moment to publish the stop
   * before LeaveGame clears it.
   */
  teardown(context: BoostInputContext): BoostInputDecision {
    this.reconciliationSuppressed = false;
    this.physicalSpaceDown = false;
    this.physicalPointerHolds.clear();
    this.toggleLatched = false;
    return this.decide(context, false, true);
  }

  /**
   * Called on every render. This is the whole recovery story: any disagreement
   * between what the player is doing and what the engine has latched — a lost
   * command, a reconnect, a snapshot from before the press — is republished
   * here, and depletion/refuel needs no client involvement at all because the
   * engine resumes a still-held Boost by itself.
   */
  reconcile(context: BoostInputContext): BoostInputDecision {
    return this.decide(context, false);
  }

  /**
   * A semantic rejection is terminal for that input edge. Adopt the level the
   * server retained and suppress automatic reconciliation until the player
   * supplies a new edge; otherwise the render loop immediately resends the
   * command the server just rejected.
   */
  handleRejectedCommand(command: BoostInputCommand): void {
    this.physicalSpaceDown = false;
    this.physicalPointerHolds.clear();
    this.toggleLatched = command === 'DeactivateBoost';
    this.pendingLevel = null;
    this.reconciliationSuppressed = true;
  }

  setMode(mode: BoostInputMode, context: BoostInputContext): BoostInputDecision {
    if (mode === this.mode) {
      return IGNORE_DECISION;
    }

    const decision = this.cleanup(context);
    this.mode = mode;
    return decision;
  }

  /** Drop all local state, e.g. when the arena switches to a different game. */
  reset(): void {
    this.physicalSpaceDown = false;
    this.physicalPointerHolds.clear();
    this.toggleLatched = false;
    this.pendingLevel = null;
    this.interactionWasActive = false;
    this.reconciliationSuppressed = false;
  }
}
