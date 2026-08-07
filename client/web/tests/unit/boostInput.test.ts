import assert from 'node:assert/strict';
import test from 'node:test';
import {
  BOOST_INPUT_MODE_STORAGE_KEY,
  BoostInputController,
  getBoostKeyAction,
  isTextEntryTarget,
  loadBoostInputMode,
  persistBoostInputMode,
  targetOwnsArrowKeys,
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
  intent: false,
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

// Clicking Boost, Menu, or Play Again leaves that button focused. Buttons do
// nothing with arrow keys, so they must not swallow turns — but text entry and
// arrow-navigable widgets still own them.
test('arrow keys survive a focused button but yield to text entry and radios', () => {
  const targetWithRole = (role: string): EventTarget => ({
    tagName: 'div',
    isContentEditable: false,
    closest: (selector: string) => selector.includes(`[role="${role}"]`) ? {} : null,
  }) as unknown as EventTarget;

  for (const tagName of ['button', 'A']) {
    const focused = target(tagName);
    assert.equal(targetOwnsGameplayKeys(focused), true, 'Space still yields');
    assert.equal(targetOwnsArrowKeys(focused), false, 'arrows must not yield');
  }

  for (const tagName of ['input', 'TEXTAREA', 'Select']) {
    assert.equal(targetOwnsArrowKeys(target(tagName)), true);
  }
  assert.equal(targetOwnsArrowKeys(target('span', true)), true);
  assert.equal(targetOwnsArrowKeys(targetWithRole('radio')), true);
  assert.equal(targetOwnsArrowKeys(targetWithRole('slider')), true);
  assert.equal(targetOwnsArrowKeys(targetWithRole('combobox')), true);
  assert.equal(targetOwnsArrowKeys(targetWithRole('button')), false);
  assert.equal(targetOwnsArrowKeys(target('canvas')), false);
  assert.equal(targetOwnsArrowKeys(null), false);
});

test('Hold starts on keydown, suppresses repeats, and stops on keyup', () => {
  const controller = new BoostInputController();

  assert.deepEqual(controller.handleKeyDown(key(), context()), {
    preventDefault: true,
    command: 'ActivateBoost',
  });
  assert.equal(controller.isSpaceDown(), true);
  assert.deepEqual(controller.handleKeyDown(key('Space', true), context({ intent: true })), {
    preventDefault: true,
    command: null,
  });
  assert.deepEqual(controller.handleKeyUp(key(), context({ intent: true, active: true })), {
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
    controller.handleKeyUp(key('Space', false, focusedInput), context({ intent: true })).command,
    'DeactivateBoost',
  );
});

// The reported bug, at the layer that caused it: an empty meter must not stop
// the press from being published, because the engine is what decides when the
// held Boost can start.
test('Hold on an empty meter still publishes intent so fuel starts Boost', () => {
  const controller = new BoostInputController('hold');

  assert.deepEqual(controller.handleKeyDown(key(), context()), {
    preventDefault: true,
    command: 'ActivateBoost',
  });

  // The engine has the intent and is simply waiting for fuel. The client sends
  // nothing further, and in particular never retracts the request.
  assert.equal(
    controller.reconcile(context({ intent: true, active: false })).command,
    null,
  );
  assert.equal(
    controller.reconcile(context({ intent: true, active: true })).command,
    null,
  );
});

test('depletion and refuel while held need no client commands at all', () => {
  const controller = new BoostInputController('hold');
  controller.handleKeyDown(key(), context());

  // Boosting, then the tank runs dry, then a packet lands: the latch never
  // moves, so the controller stays quiet throughout.
  for (const active of [true, false, true]) {
    assert.equal(
      controller.reconcile(context({ intent: true, active })).command,
      null,
    );
  }
});

test('releasing Hold while empty retracts the intent', () => {
  const controller = new BoostInputController('hold');

  controller.handleKeyDown(key(), context());
  controller.reconcile(context({ intent: true, active: false }));
  assert.equal(
    controller.handleKeyUp(key(), context({ intent: true, active: false })).command,
    'DeactivateBoost',
  );
  assert.equal(
    controller.reconcile(context({ intent: false, active: false })).command,
    null,
  );
});

// Losing the socket is not a release. The key is still down, so the level must
// survive the outage and be republished, not require a fresh press.
test('a hold survives a disconnect and is republished on reconnect', () => {
  const controller = new BoostInputController('hold');
  assert.equal(controller.handleKeyDown(key(), context()).command, 'ActivateBoost');
  controller.reconcile(context({ intent: true, active: true }));

  assert.equal(
    controller.reconcile(context({ intent: true, interactionActive: false })).command,
    null,
  );
  assert.equal(controller.isSpaceDown(), true, 'the key is still physically down');

  // The engine lost the intent across the outage; the still-held key restores it.
  assert.equal(
    controller.reconcile(context({ intent: false, interactionActive: true })).command,
    'ActivateBoost',
  );

  // And the eventual release is still honored.
  assert.equal(
    controller.handleKeyUp(key(), context({ intent: true })).command,
    'DeactivateBoost',
  );
});

test('a press made before play starts is published once interaction opens', () => {
  const controller = new BoostInputController('hold');

  // Countdown, respawn, or a dropped socket: the key is still physically down.
  assert.deepEqual(controller.handleKeyDown(key(), context({ interactionActive: false })), {
    preventDefault: true,
    command: null,
  });
  assert.equal(controller.isSpaceDown(), true);

  assert.equal(
    controller.reconcile(context({ interactionActive: true })).command,
    'ActivateBoost',
  );
});

test('modal-owned Space cannot queue Boost for when the briefing closes', () => {
  for (const mode of ['hold', 'toggle'] as const) {
    const controller = new BoostInputController(mode);

    assert.deepEqual(
      controller.suppressModalKeyDown(key('Space', false, target('div'))),
      { preventDefault: true, command: null },
    );
    assert.equal(controller.isSpaceDown(), false);
    assert.equal(
      controller.reconcile(context({ interactionActive: true })).command,
      null,
      `${mode} must stay off when modal ownership ends`,
    );

    // The Ready button retains native keyboard activation, but still does not
    // enter the gameplay controller.
    assert.deepEqual(
      controller.suppressModalKeyDown(key('Space', false, target('button'))),
      { preventDefault: false, command: null },
    );
  }
});

test('an unacknowledged command is republished when interaction resumes', () => {
  const controller = new BoostInputController('hold');
  assert.equal(controller.handleKeyDown(key(), context()).command, 'ActivateBoost');

  // While the request is in flight the controller waits rather than spamming.
  assert.equal(controller.reconcile(context({ intent: false })).command, null);
  assert.equal(controller.reconcile(context({ intent: false })).command, null);

  // A reconnect is the moment to doubt delivery and say it again.
  assert.equal(controller.reconcile(context({ interactionActive: false })).command, null);
  assert.equal(
    controller.reconcile(context({ intent: false, interactionActive: true })).command,
    'ActivateBoost',
  );
});

// A tap can complete inside one round trip. The release must still be sent, or
// the engine keeps the press forever and the snake boosts with nothing held.
test('a press and release inside one round trip still publishes the release', () => {
  const controller = new BoostInputController('hold');
  assert.equal(controller.handleKeyDown(key(), context()).command, 'ActivateBoost');

  // Released before the engine has echoed the press back.
  assert.equal(
    controller.handleKeyUp(key(), context({ intent: false })).command,
    'DeactivateBoost',
  );

  // Both echoes arrive in order and neither produces a spurious command.
  assert.equal(controller.reconcile(context({ intent: true, active: true })).command, null);
  assert.equal(controller.reconcile(context({ intent: false })).command, null);
});

test('Toggle changes level once per complete physical press', () => {
  const controller = new BoostInputController('toggle');

  assert.equal(controller.handleKeyDown(key(), context()).command, 'ActivateBoost');
  assert.equal(controller.handleKeyDown(key('Space', true), context()).command, null);
  assert.equal(controller.handleKeyUp(key(), context({ intent: true })).command, null);

  // The second press uses the remembered latch even before a React render
  // publishes predicted state.
  assert.equal(
    controller.handleKeyDown(key(), context({ intent: true })).command,
    'DeactivateBoost',
  );
  assert.equal(controller.handleKeyUp(key(), context()).command, null);
});

test('Toggle arms Boost on an empty meter and keeps it armed', () => {
  const controller = new BoostInputController('toggle');
  assert.equal(controller.handleKeyDown(key(), context()).command, 'ActivateBoost');
  controller.handleKeyUp(key(), context({ intent: true }));
  assert.equal(controller.reconcile(context({ intent: true, active: false })).command, null);
  assert.equal(
    controller.handleKeyDown(key(), context({ intent: true })).command,
    'DeactivateBoost',
  );
});

test('Hold pointer edges start and stop Boost while its synthesized click is inert', () => {
  const controller = new BoostInputController('hold');

  assert.deepEqual(controller.handlePointerDown(context()), {
    preventDefault: true,
    command: 'ActivateBoost',
  });
  assert.equal(controller.handlePointerDown(context({ intent: true })).command, null);
  assert.equal(controller.handleButtonPress(context({ intent: true, active: true })).command, null);
  assert.deepEqual(controller.handlePointerUp(context({ intent: true, active: true })), {
    preventDefault: true,
    command: 'DeactivateBoost',
  });
  assert.equal(controller.handlePointerUp(context()).command, null);
});

// The pointer equivalent of the disconnect case: the press is recorded even
// when it cannot be published, and the release is still honored afterwards.
test('a pointer hold started while disconnected still releases cleanly', () => {
  const controller = new BoostInputController('hold');

  assert.deepEqual(controller.handlePointerDown(context({ interactionActive: false })), {
    preventDefault: true,
    command: null,
  });
  assert.equal(
    controller.reconcile(context({ interactionActive: true })).command,
    'ActivateBoost',
  );
  assert.equal(
    controller.handlePointerUp(context({ intent: true, active: true })).command,
    'DeactivateBoost',
  );
});

test('Toggle pointer edges are inert and click changes the durable level', () => {
  const controller = new BoostInputController('toggle');

  assert.equal(controller.handlePointerDown(context()).command, null);
  assert.equal(controller.handlePointerUp(context()).command, null);
  assert.equal(controller.handleButtonPress(context()).command, 'ActivateBoost');
  assert.equal(
    controller.handleButtonPress(context({ intent: true, active: true })).command,
    'DeactivateBoost',
  );
});

test('cleanup and mode changes stop once and clear the physical latch', () => {
  const controller = new BoostInputController('hold');
  controller.handleKeyDown(key(), context());
  controller.reconcile(context({ intent: true, active: true }));

  assert.equal(
    controller.cleanup(context({ intent: true, active: true })).command,
    'DeactivateBoost',
  );
  assert.equal(controller.cleanup(context({ intent: true, active: true })).command, null);
  assert.equal(controller.isSpaceDown(), false);

  controller.reconcile(context());
  controller.handleKeyDown(key(), context());
  assert.equal(
    controller.setMode('toggle', context({ intent: true, active: true })).command,
    'DeactivateBoost',
  );
  assert.equal(controller.getMode(), 'toggle');
  assert.equal(controller.isSpaceDown(), false);
});

test('explicit teardown stops a latched Boost exactly once', () => {
  const authoritative = new BoostInputController('toggle');
  assert.equal(
    authoritative.teardown(context({ intent: true, active: true })).command,
    'DeactivateBoost',
  );
  assert.equal(
    authoritative.teardown(context({ intent: true, active: true })).command,
    null,
  );

  const pending = new BoostInputController('hold');
  pending.handlePointerDown(context());
  assert.equal(
    pending.teardown(context({ intent: true, interactionActive: false })).command,
    'DeactivateBoost',
    'route teardown must release Boost after the next route makes interaction inactive',
  );

  const deferredRelease = new BoostInputController('hold');
  deferredRelease.handlePointerDown(context());
  deferredRelease.handlePointerUp(context({ intent: true, interactionActive: false }));
  assert.equal(
    deferredRelease.teardown(context({ intent: true, interactionActive: false })).command,
    'DeactivateBoost',
    'teardown must flush a release deferred during route transition',
  );
});

test('blur safety releases Hold without inventing intent from snapshot state', () => {
  const passive = new BoostInputController('hold');
  assert.equal(
    passive.releaseHeld(context({ intent: false, active: false })).command,
    null,
  );

  const held = new BoostInputController('hold');
  held.handleKeyDown(key(), context());
  held.reconcile(context({ intent: true, active: true }));
  assert.equal(
    held.releaseHeld(context({ intent: true, active: true })).command,
    'DeactivateBoost',
  );
});

test('blur clears the physical Toggle edge without cancelling Toggle intent', () => {
  const controller = new BoostInputController('toggle');
  assert.equal(controller.handleKeyDown(key(), context()).command, 'ActivateBoost');
  controller.reconcile(context({ intent: true, active: true }));

  assert.equal(
    controller.releaseHeld(context({ intent: true, active: true })).command,
    null,
  );
  assert.equal(controller.isSpaceDown(), false);
  assert.equal(
    controller.handleKeyDown(key(), context({ intent: true, active: true })).command,
    'DeactivateBoost',
  );
});

test('changing from Toggle to Hold explicitly stops latched Toggle intent', () => {
  const controller = new BoostInputController('toggle');
  controller.handleKeyDown(key(), context());
  controller.handleKeyUp(key(), context({ intent: true, active: true }));
  controller.reconcile(context({ intent: true, active: true }));

  assert.equal(
    controller.setMode('hold', context({ intent: true, active: true })).command,
    'DeactivateBoost',
  );
  assert.equal(controller.getMode(), 'hold');
});

test('fresh Hold state repairs a latched Boost with no held key', () => {
  const controller = new BoostInputController('hold');
  assert.equal(
    controller.reconcile(context({ intent: true, active: true })).command,
    'DeactivateBoost',
  );
  assert.equal(
    controller.reconcile(context({ intent: true, active: true })).command,
    null,
  );
});

test('Toggle preserves its latched state across blur and reconnect', () => {
  const controller = new BoostInputController('toggle');
  controller.handleKeyDown(key(), context());
  controller.handleKeyUp(key(), context({ intent: true, active: true }));
  controller.reconcile(context({ intent: true, active: true }));

  assert.equal(controller.releaseHeld(context({
    intent: true,
    active: true,
    interactionActive: false,
  })).command, null);
  assert.equal(controller.reconcile(context({
    intent: true,
    active: true,
    interactionActive: true,
  })).command, null);
});

test('cleanup while disconnected defers one release until interaction resumes', () => {
  const controller = new BoostInputController('hold');
  controller.handleKeyDown(key(), context());
  controller.reconcile(context({ intent: true, active: true }));

  assert.equal(controller.cleanup(context({
    intent: true,
    active: true,
    interactionActive: false,
  })).command, null);
  assert.equal(controller.reconcile(context({
    intent: true,
    active: true,
    interactionActive: true,
  })).command, 'DeactivateBoost');
  assert.equal(controller.reconcile(context({
    intent: false,
    active: false,
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

test('reset drops every local fact so a new game starts clean', () => {
  const controller = new BoostInputController('toggle');
  controller.handleKeyDown(key(), context());
  controller.reset();

  assert.equal(controller.isSpaceDown(), false);
  assert.equal(controller.reconcile(context({ intent: false })).command, null);
  assert.equal(controller.handleKeyDown(key(), context()).command, 'ActivateBoost');
});
