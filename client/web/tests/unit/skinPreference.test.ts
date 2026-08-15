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
