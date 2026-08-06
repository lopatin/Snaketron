import assert from 'node:assert/strict';
import test from 'node:test';
import {
  BOOST_INPUT_MODE_STORAGE_KEY,
  BoostInputController,
  getBoostKeyAction,
  isTextEntryTarget,
  loadBoostInputMode,
  persistBoostInputMode,
  targetOwnsGameplayKeys,
  type BoostInputContext,
} from '../../utils/boostInput.ts';

const target = (
  tagName: string,
  isContentEditable = false,
  closest = false,
): EventTarget => ({
  tagName,
  isContentEditable,
  closest: () => closest ? {} : null,
}) as unknown as EventTarget;

const key = (
  code = 'Space',
  repeat = false,
  eventTarget: EventTarget | null = null,
) => ({ code, repeat, target: eventTarget });

const context = (
  overrides: Partial<BoostInputContext> = {},
): BoostInputContext => ({
  active: false,
  canActivate: true,
  interactionActive: true,
  gameOver: false,
  ...overrides,
});

test('Boost input mode defaults defensively to Hold and persists Toggle', () => {
  const values = new Map<string, string>();
  const storage = {
    getItem: (name: string) => values.get(name) ?? null,
    setItem: (name: string, value: string) => { values.set(name, value); },
  };

  assert.equal(loadBoostInputMode(storage), 'hold');
  values.set(BOOST_INPUT_MODE_STORAGE_KEY, 'unexpected');
  assert.equal(loadBoostInputMode(storage), 'hold');
  persistBoostInputMode('toggle', storage);
  assert.equal(loadBoostInputMode(storage), 'toggle');

  const blocked = {
    getItem: () => { throw new Error('blocked'); },
    setItem: () => { throw new Error('blocked'); },
  };
  assert.equal(loadBoostInputMode(blocked), 'hold');
  assert.doesNotThrow(() => persistBoostInputMode('toggle', blocked));
});

test('Space activates exactly once per physical press in the legacy classifier', () => {
  assert.equal(getBoostKeyAction(key()), 'activate');
  assert.equal(getBoostKeyAction(key('Space', true)), 'prevent-default');
});

test('text and interactive controls retain Space and arrow-key ownership', () => {
  for (const tagName of ['input', 'TEXTAREA', 'Select']) {
    const focused = target(tagName);
    assert.equal(isTextEntryTarget(focused), true);
    assert.equal(targetOwnsGameplayKeys(focused), true);
    assert.equal(getBoostKeyAction(key('Space', false, focused)), 'ignore');
  }

  assert.equal(targetOwnsGameplayKeys(target('button')), true);
  assert.equal(targetOwnsGameplayKeys(target('span', false, true)), true);
  assert.equal(targetOwnsGameplayKeys(target('span', true)), true);
  assert.equal(targetOwnsGameplayKeys(target('canvas')), false);
  assert.equal(getBoostKeyAction(key('ArrowUp')), 'ignore');
});

test('Hold starts on keydown, suppresses repeats, and stops on keyup', () => {
  const controller = new BoostInputController();

  assert.deepEqual(controller.handleKeyDown(key(), context()), {
    preventDefault: true,
    command: 'ActivateBoost',
  });
  assert.equal(controller.isSpaceDown(), true);
  assert.deepEqual(controller.handleKeyDown(key('Space', true), context({ active: true })), {
    preventDefault: true,
    command: null,
  });
  assert.deepEqual(controller.handleKeyUp(key(), context({ active: true })), {
    preventDefault: true,
    command: 'DeactivateBoost',
  });
  assert.equal(controller.isSpaceDown(), false);
  assert.deepEqual(controller.handleKeyUp(key(), context()), {
    preventDefault: false,
    command: null,
  });
});

test('Hold release is honored after focus moves and can cancel pending activation', () => {
  const controller = new BoostInputController('hold');
  assert.equal(controller.handleKeyDown(key(), context()).command, 'ActivateBoost');

  const focusedInput = target('input');
  assert.equal(
    controller.handleKeyUp(key('Space', false, focusedInput), context()).command,
    'DeactivateBoost',
  );
});

test('Hold at zero charge has no latent activation or unnecessary release command', () => {
  const controller = new BoostInputController('hold');
  assert.deepEqual(controller.handleKeyDown(key(), context({ canActivate: false })), {
    preventDefault: true,
    command: null,
  });
  assert.deepEqual(controller.handleKeyUp(key(), context({ canActivate: false })), {
    preventDefault: true,
    command: null,
  });
});

test('continuous Space Hold resumes Boost when depleted charge is replenished', () => {
  const controller = new BoostInputController('hold');

  assert.equal(controller.handleKeyDown(key(), context()).command, 'ActivateBoost');
  assert.equal(
    controller.reconcile(context({ active: true, canActivate: false })).command,
    null,
  );
  assert.equal(
    controller.reconcile(context({ active: false, canActivate: false })).command,
    null,
  );
  assert.equal(
    controller.reconcile(context({ active: false, canActivate: true })).command,
    'ActivateBoost',
  );
  assert.equal(
    controller.reconcile(context({ active: false, canActivate: true })).command,
    null,
    'one replenishment must create only one activation edge',
  );
});

test('continuous pointer Hold resumes Boost after depletion and recharge', () => {
  const controller = new BoostInputController('hold');

  assert.equal(controller.handlePointerDown(context()).command, 'ActivateBoost');
  controller.reconcile(context({ active: true, canActivate: false }));
  assert.equal(
    controller.reconcile(context({ active: false, canActivate: false })).command,
    null,
  );
  assert.equal(
    controller.reconcile(context({ active: false, canActivate: true })).command,
    'ActivateBoost',
  );
});

test('releasing Hold while empty cancels intent before charge is replenished', () => {
  const controller = new BoostInputController('hold');

  controller.handleKeyDown(key(), context());
  controller.reconcile(context({ active: true, canActivate: false }));
  controller.reconcile(context({ active: false, canActivate: false }));
  assert.equal(
    controller.handleKeyUp(key(), context({ active: false, canActivate: false })).command,
    'DeactivateBoost',
  );
  assert.equal(
    controller.reconcile(context({ active: false, canActivate: true })).command,
    null,
  );
});

test('Toggle changes target once per complete physical press', () => {
  const controller = new BoostInputController('toggle');

  assert.equal(controller.handleKeyDown(key(), context()).command, 'ActivateBoost');
  assert.equal(controller.handleKeyDown(key('Space', true), context()).command, null);
  assert.equal(controller.handleKeyUp(key(), context()).command, null);

  // The second press uses the remembered target even before a React render
  // publishes predicted active state.
  assert.equal(controller.handleKeyDown(key(), context()).command, 'DeactivateBoost');
  assert.equal(controller.handleKeyUp(key(), context()).command, null);
});

test('Toggle does not arm future Boost while charge is unavailable', () => {
  const controller = new BoostInputController('toggle');
  assert.equal(
    controller.handleKeyDown(key(), context({ canActivate: false })).command,
    null,
  );
  controller.handleKeyUp(key(), context({ canActivate: false }));
  assert.equal(controller.handleKeyDown(key(), context()).command, 'ActivateBoost');
});

test('Hold pointer edges start and stop Boost while its synthesized click is inert', () => {
  const controller = new BoostInputController('hold');

  assert.deepEqual(controller.handlePointerDown(context()), {
    preventDefault: true,
    command: 'ActivateBoost',
  });
  assert.equal(controller.handlePointerDown(context()).command, null);
  assert.equal(controller.handleButtonPress(context({ active: true })).command, null);
  assert.deepEqual(controller.handlePointerUp(context({ active: true })), {
    preventDefault: true,
    command: 'DeactivateBoost',
  });
  assert.equal(controller.handlePointerUp(context()).command, null);
});

test('Toggle pointer edges are inert and click changes the durable target', () => {
  const controller = new BoostInputController('toggle');

  assert.equal(controller.handlePointerDown(context()).command, null);
  assert.equal(controller.handlePointerUp(context()).command, null);
  assert.equal(controller.handleButtonPress(context()).command, 'ActivateBoost');
  assert.equal(
    controller.handleButtonPress(context({ active: true, canActivate: false })).command,
    'DeactivateBoost',
  );
});

test('cleanup and mode changes stop once and clear the physical latch', () => {
  const controller = new BoostInputController('hold');
  controller.handleKeyDown(key(), context());
  controller.reconcile(context({ active: true, canActivate: false }));

  assert.equal(controller.cleanup(context({ active: true, canActivate: false })).command, 'DeactivateBoost');
  assert.equal(controller.cleanup(context({ active: true, canActivate: false })).command, null);
  assert.equal(controller.isSpaceDown(), false);

  controller.reconcile(context());
  controller.handleKeyDown(key(), context());
  assert.equal(
    controller.setMode('toggle', context({ active: true, canActivate: false })).command,
    'DeactivateBoost',
  );
  assert.equal(controller.getMode(), 'toggle');
  assert.equal(controller.isSpaceDown(), false);
});

test('explicit teardown stops an authoritative or pending Boost exactly once', () => {
  const authoritative = new BoostInputController('toggle');
  assert.equal(
    authoritative.teardown(context({ active: true, canActivate: false })).command,
    'DeactivateBoost',
  );
  assert.equal(
    authoritative.teardown(context({ active: true, canActivate: false })).command,
    null,
  );

  const pending = new BoostInputController('hold');
  pending.handlePointerDown(context());
  assert.equal(
    pending.teardown(context({ interactionActive: false })).command,
    'DeactivateBoost',
    'route teardown must release pending Boost after the next route makes interaction inactive',
  );

  const deferredRelease = new BoostInputController('hold');
  deferredRelease.handlePointerDown(context());
  deferredRelease.handlePointerUp(context({ interactionActive: false }));
  assert.equal(
    deferredRelease.teardown(context({ interactionActive: false })).command,
    'DeactivateBoost',
    'teardown must flush a release deferred during route transition',
  );
});

test('blur safety releases Hold without inventing intent from snapshot state', () => {
  const passive = new BoostInputController('hold');
  assert.equal(
    passive.releaseHeld(context({ active: true, canActivate: false })).command,
    null,
  );

  const held = new BoostInputController('hold');
  held.handleKeyDown(key(), context());
  held.reconcile(context({ active: true, canActivate: false }));
  assert.equal(
    held.releaseHeld(context({ active: true, canActivate: false })).command,
    'DeactivateBoost',
  );
});

test('blur clears the physical Toggle edge without cancelling Toggle intent', () => {
  const controller = new BoostInputController('toggle');
  assert.equal(controller.handleKeyDown(key(), context()).command, 'ActivateBoost');
  controller.reconcile(context({ active: true, canActivate: false }));

  assert.equal(
    controller.releaseHeld(context({ active: true, canActivate: false })).command,
    null,
  );
  assert.equal(controller.isSpaceDown(), false);
  assert.equal(
    controller.handleKeyDown(key(), context({ active: true, canActivate: false })).command,
    'DeactivateBoost',
  );
});

test('changing from Toggle to Hold explicitly stops latched Toggle intent', () => {
  const controller = new BoostInputController('toggle');
  controller.handleKeyDown(key(), context());
  controller.handleKeyUp(key(), context({ active: true, canActivate: false }));
  controller.reconcile(context({ active: true, canActivate: false }));

  assert.equal(
    controller.setMode('hold', context({ active: true, canActivate: false })).command,
    'DeactivateBoost',
  );
  assert.equal(controller.getMode(), 'hold');
});

test('fresh Hold state repairs an authoritative active Boost with no held key', () => {
  const controller = new BoostInputController('hold');
  assert.equal(
    controller.reconcile(context({ active: true, canActivate: false })).command,
    'DeactivateBoost',
  );
  assert.equal(
    controller.reconcile(context({ active: true, canActivate: false })).command,
    null,
  );
});

test('Toggle preserves its authoritative active state across blur and reconnect', () => {
  const controller = new BoostInputController('toggle');
  controller.handleKeyDown(key(), context());
  controller.handleKeyUp(key(), context({ active: true, canActivate: false }));
  controller.reconcile(context({ active: true, canActivate: false }));

  assert.equal(controller.releaseHeld(context({
    active: true,
    canActivate: false,
    interactionActive: false,
  })).command, null);
  assert.equal(controller.reconcile(context({
    active: true,
    canActivate: false,
    interactionActive: true,
  })).command, null);
});

test('cleanup while disconnected defers one release until interaction resumes', () => {
  const controller = new BoostInputController('hold');
  controller.handleKeyDown(key(), context());
  controller.reconcile(context({ active: true, canActivate: false }));

  assert.equal(controller.cleanup(context({
    active: true,
    canActivate: false,
    interactionActive: false,
  })).command, null);
  assert.equal(controller.reconcile(context({
    active: true,
    canActivate: false,
    interactionActive: true,
  })).command, 'DeactivateBoost');
  assert.equal(controller.reconcile(context({
    active: true,
    canActivate: false,
    interactionActive: true,
  })).command, null);
});

test('game-over Space is ignored so the score card keeps its shortcut', () => {
  const controller = new BoostInputController('hold');
  assert.deepEqual(controller.handleKeyDown(key(), context({ gameOver: true })), {
    preventDefault: false,
    command: null,
  });
});
