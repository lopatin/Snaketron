import assert from 'node:assert/strict';
import test from 'node:test';

const makeStorage = () => {
  const values = new Map<string, string>();
  return {
    values,
    storage: {
      get length() { return values.size; },
      clear: () => values.clear(),
      getItem: (key: string) => values.get(key) ?? null,
      key: (index: number) => [...values.keys()][index] ?? null,
      removeItem: (key: string) => { values.delete(key); },
      setItem: (key: string, value: string) => { values.set(key, String(value)); },
    },
  };
};

test('CrazyGames preferences sanitize local values and apply the server snapshot', async () => {
  const hadWindow = 'window' in globalThis;
  const previousWindow = (globalThis as any).window;
  const { values, storage } = makeStorage();
  (globalThis as any).window = { localStorage: storage };

  const manyTutorials = Object.fromEntries(
    Array.from({ length: 140 }, (_unused, index) => [`tutorial-${index}`, true]),
  );
  values.set('snaketron:tutorial-seen:v1', JSON.stringify({
    movement: true,
    boost: false,
    ...manyTutorials,
    ['x'.repeat(65)]: true,
    ['bad\u0000key']: true,
  }));
  values.set('lastLobbyPreferences', JSON.stringify({
    selectedModes: ['duel', 'bad-mode', 'duel', 'solo'],
    competitive: true,
  }));
  values.set('snaketron:boost-input-mode:v1', 'toggle');

  try {
    const {
      applyCrazyGamesPreferences,
      clearLinkedCrazyGamesPreferences,
      crazyGamesPreferenceOwner,
      markCrazyGamesPreferencesOwnedBy,
      readCrazyGamesPreferences,
    } = await import(`../../services/crazyGamesPreferences.ts?prefs=${Date.now()}`);

    const sanitized = readCrazyGamesPreferences();
    assert.equal(Object.keys(sanitized.tutorialSeen!).length, 128);
    assert.equal(sanitized.tutorialSeen?.movement, true);
    assert.equal(sanitized.tutorialSeen?.boost, undefined);
    assert.equal(sanitized.tutorialSeen?.['x'.repeat(65)], undefined);
    assert.equal(sanitized.tutorialSeen?.['bad\u0000key'], undefined);
    assert.deepEqual(sanitized.lobbyPreferences, {
      selectedModes: ['duel', 'solo'],
      competitive: true,
    });
    assert.equal(sanitized.boostInputMode, 'toggle');

    applyCrazyGamesPreferences({
      tutorialSeen: { movement: true, boost: true },
      lobbyPreferences: { selectedModes: ['ffa'], competitive: false },
      boostInputMode: 'hold',
    });
    assert.deepEqual(JSON.parse(values.get('snaketron:tutorial-seen:v1')!), {
      movement: true,
      boost: true,
    });
    assert.deepEqual(JSON.parse(values.get('lastLobbyPreferences')!), {
      selectedModes: ['ffa'],
      competitive: false,
    });
    assert.equal(values.get('snaketron:boost-input-mode:v1'), 'hold');

    markCrazyGamesPreferencesOwnedBy(912);
    assert.equal(crazyGamesPreferenceOwner(), 912);
    clearLinkedCrazyGamesPreferences();
    assert.equal(crazyGamesPreferenceOwner(), null);
    assert.equal(values.has('snaketron:tutorial-seen:v1'), false);
    assert.equal(values.has('lastLobbyPreferences'), false);
    assert.equal(values.has('snaketron:boost-input-mode:v1'), false);

    // Missing fields in a canonical server snapshot are deletions, not an
    // instruction to leak a prior account's browser-local values.
    values.set('lastLobbyPreferences', JSON.stringify({
      selectedModes: ['duel'],
      competitive: false,
    }));
    applyCrazyGamesPreferences({}, true);
    assert.equal(values.has('lastLobbyPreferences'), false);
  } finally {
    if (hadWindow) {
      (globalThis as any).window = previousWindow;
    } else {
      delete (globalThis as any).window;
    }
  }
});
