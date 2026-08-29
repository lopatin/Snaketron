import assert from 'node:assert/strict';
import test from 'node:test';

import {
  DEFAULT_SKIN_REF,
  isPlausibleSkinRef,
} from '../../utils/skinPreference.ts';
import {
  CLASSIC_CELEBRATION_THEME,
  getScoreEffectTeamColor,
  getScoreReadoutColor,
  type CelebrationTheme,
} from '../../utils/scoreEffects.ts';

test('the default skin is one every client can draw', () => {
  assert.equal(DEFAULT_SKIN_REF, 'classic@1');
});

test('a stored preference that could not be a catalogue id is refused', () => {
  // Hygiene only — the server is what decides whether an id is real. This just
  // keeps obvious junk off the wire.
  for (const value of [
    '',
    '   ',
    'has spaces',
    'semi;colon',
    'quote"mark',
    '<script>',
    'a'.repeat(97),
    null,
    undefined,
    42,
    {},
  ]) {
    assert.equal(
      isPlausibleSkinRef(value),
      false,
      `${JSON.stringify(value)} should not look like a skin ref`,
    );
  }
});

test('real catalogue ids and future content hashes are accepted', () => {
  for (const value of [
    'classic@1',
    'aurora@1',
    'ember@1',
    `sha256:${'a'.repeat(64)}`,
  ]) {
    assert.equal(isPlausibleSkinRef(value), true, `${value} should be allowed`);
  }
});

const EMBER: CelebrationTheme = {
  effect: 'goal-impact-wave',
  friendly_accent: '#4a95f0',
  enemy_accent: '#f26b2e',
  readout_friendly: '#1f4f8c',
  readout_enemy: '#9c3d12',
};

test('a celebration keeps the classic colours when no theme travelled with it', () => {
  // Cues from an older client, or from a scorer wearing an unknown skin, have
  // no dressing — they must look exactly like they always did.
  assert.equal(getScoreEffectTeamColor(0, 0), '#5299bb');
  assert.equal(getScoreEffectTeamColor(1, 0), '#d45454');
  assert.equal(getScoreReadoutColor(0, 0), '#2b6f8c');
  assert.equal(getScoreReadoutColor(1, 0), '#a83232');
  assert.equal(
    getScoreEffectTeamColor(0, 0, CLASSIC_CELEBRATION_THEME),
    '#5299bb',
  );
});

test('the scorer supplies the colours; the viewer decides which side they are on', () => {
  // Same theme, opposite viewers: whose goal it is flips, the palette does not.
  assert.equal(getScoreEffectTeamColor(0, 0, EMBER), EMBER.friendly_accent);
  assert.equal(getScoreEffectTeamColor(0, 1, EMBER), EMBER.enemy_accent);
  assert.equal(getScoreReadoutColor(0, 0, EMBER), EMBER.readout_friendly);
  assert.equal(getScoreReadoutColor(0, 1, EMBER), EMBER.readout_enemy);
});

test('a spectator sees team 0 as the friendly side', () => {
  assert.equal(getScoreEffectTeamColor(0, null, EMBER), EMBER.friendly_accent);
  assert.equal(getScoreEffectTeamColor(1, null, EMBER), EMBER.enemy_accent);
});

/**
 * Equipping has to survive getting an account, because for much of a first
 * session there is not one: accounts are created lazily, so a choice made
 * while browsing lives only in local storage until something can write it.
 */
const withStoredPreferences = async (
  stored: Record<string, string>,
  body: (module: typeof import('../../utils/skinPreference.ts')) => Promise<void> | void,
) => {
  const hadWindow = 'window' in globalThis;
  const previousWindow = (globalThis as any).window;
  const values = new Map<string, string>(Object.entries(stored));
  (globalThis as any).window = {
    localStorage: {
      get length() { return values.size; },
      clear: () => values.clear(),
      getItem: (key: string) => values.get(key) ?? null,
      key: (index: number) => [...values.keys()][index] ?? null,
      removeItem: (key: string) => { values.delete(key); },
      setItem: (key: string, value: string) => { values.set(key, String(value)); },
    },
  };
  try {
    // Cache-busted so each case reads its own storage stub.
    await body(await import(`../../utils/skinPreference.ts?adopt=${values.size}-${Math.random()}`));
  } finally {
    if (hadWindow) {
      (globalThis as any).window = previousWindow;
    } else {
      delete (globalThis as any).window;
    }
  }
};

test('an empty account inherits the skin chosen before it existed', async () => {
  await withStoredPreferences(
    { 'snaketron:skin:v1': 'voltage@1' },
    ({ equipmentToAdopt }) => {
      // The exact shape a fresh guest arrives in.
      assert.deepEqual(equipmentToAdopt({}), { selectedSkin: 'voltage@1' });
      assert.deepEqual(
        equipmentToAdopt({ selectedSkin: null, selectedBase: null }),
        { selectedSkin: 'voltage@1' },
      );
    },
  );
});

test('an account that already dressed itself is never overwritten', async () => {
  await withStoredPreferences(
    { 'snaketron:skin:v1': 'voltage@1', 'snaketron:base:v1': 'base:ember@1' },
    ({ equipmentToAdopt }) => {
      // The account is the authority: this fills a blank, it never argues.
      assert.equal(
        equipmentToAdopt({ selectedSkin: 'aurora@1', selectedBase: 'base:aurora@1' }),
        null,
      );
      // Each slot is judged on its own.
      assert.deepEqual(equipmentToAdopt({ selectedSkin: 'aurora@1' }), {
        selectedBase: 'base:ember@1',
      });
    },
  );
});

test('an untouched browser has nothing worth saying', async () => {
  await withStoredPreferences({}, ({ equipmentToAdopt }) => {
    assert.equal(equipmentToAdopt({}), null);
  });
  // The default carries no intent, so it is not worth a write either.
  await withStoredPreferences(
    { 'snaketron:skin:v1': DEFAULT_SKIN_REF },
    ({ equipmentToAdopt }) => {
      assert.equal(equipmentToAdopt({}), null);
    },
  );
});
