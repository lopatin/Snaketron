import assert from 'node:assert/strict';
import test from 'node:test';
import type { GameState, GameType } from '../../types/index.ts';
import {
  ALL_TUTORIAL_KEYS,
  TUTORIAL_SCENE_IDS,
  tutorialContent,
  tutorialContentForGame,
  tutorialKey,
  tutorialModeForGameType,
} from '../../utils/tutorial.ts';
import type { TutorialMode } from '../../utils/tutorial.ts';

const MODES: TutorialMode[] = ['duel', '2v2', 'ffa', 'solo'];

test('every mode x ranked combination has its own tutorial, and there are eight', () => {
  assert.equal(ALL_TUTORIAL_KEYS.length, 8);
  assert.equal(new Set(ALL_TUTORIAL_KEYS).size, 8);

  const seen = new Set<string>();
  for (const mode of MODES) {
    for (const queueMode of ['Quickmatch', 'Competitive'] as const) {
      const content = tutorialContent(mode, queueMode);
      assert.equal(content.key, tutorialKey(mode, queueMode));
      assert.equal(content.bullets.length, 3, `${content.key} must have three bullets`);
      for (const bullet of content.bullets) {
        assert.ok(bullet.text.length > 0);
        assert.ok(bullet.scene.length > 0);
      }
      assert.ok(!seen.has(content.key));
      seen.add(content.key);
    }
  }
  assert.deepEqual([...seen].sort(), [...ALL_TUTORIAL_KEYS].sort());
});

test('the tutorial mode is derived from authoritative state, not from queue-time choices', () => {
  assert.equal(tutorialModeForGameType({ TeamMatch: { per_team: 1 } }), 'duel');
  assert.equal(tutorialModeForGameType({ TeamMatch: { per_team: 2 } }), '2v2');
  assert.equal(tutorialModeForGameType({ FreeForAll: { max_players: 4 } }), 'ffa');
  assert.equal(tutorialModeForGameType('Solo'), 'solo');
});

test('custom games get no tutorial rather than a wrong one', () => {
  // Custom is not reachable through matchmaking and has no fixed rule set, so
  // there is no honest briefing to show for it.
  const custom = {
    Custom: {
      settings: {
        arena_width: 40,
        arena_height: 40,
        tick_duration_ms: 100,
        food_spawn_rate: 3,
        max_players: 4,
        game_mode: 'Solo',
        is_private: true,
        allow_spectators: true,
        snake_start_length: 4,
      },
    },
  } as unknown as GameType;

  assert.equal(tutorialModeForGameType(custom), null);
  assert.equal(
    tutorialContentForGame({ game_type: custom, queue_mode: 'Quickmatch' } as GameState),
    null,
  );
});

test('only boost-enabled modes mention NOS and Space', () => {
  for (const queueMode of ['Quickmatch', 'Competitive'] as const) {
    for (const mode of ['duel', '2v2'] as const) {
      const text = tutorialContent(mode, queueMode).bullets.map((b) => b.text).join(' ');
      assert.match(text, /NOS/, `${mode} must explain NOS`);
      assert.match(text, /Space/, `${mode} must name the boost key`);
    }
    // Boost only exists for duel and 2v2 in the engine; promising it anywhere
    // else would be a rule the player could check and find false.
    for (const mode of ['ffa', 'solo'] as const) {
      const text = tutorialContent(mode, queueMode).bullets.map((b) => b.text).join(' ');
      assert.doesNotMatch(text, /NOS/, `${mode} has no boost`);
      assert.doesNotMatch(text, /Space/, `${mode} has no boost key`);
    }
  }
});

test('no mode claims a score target, because the engine has none', () => {
  for (const key of ALL_TUTORIAL_KEYS) {
    const [mode, rank] = key.split(':') as [TutorialMode, 'ranked' | 'casual'];
    const content = tutorialContent(mode, rank === 'ranked' ? 'Competitive' : 'Quickmatch');
    const text = content.bullets.map((b) => b.text).join(' ');
    assert.doesNotMatch(
      text,
      /first to \d+/i,
      `${key} must not promise a first-to-N win condition`,
    );
  }
});

test('team modes state the real 90-second win condition', () => {
  for (const mode of ['duel', '2v2'] as const) {
    const text = tutorialContent(mode, 'Quickmatch').bullets.map((b) => b.text).join(' ');
    assert.match(text, /90 seconds/);
    assert.match(text, /enemy base/);
  }
});

test('ranked copy claims a rank is at stake only where one actually is', () => {
  for (const mode of ['duel', '2v2', 'ffa'] as const) {
    const ranked = tutorialContent(mode, 'Competitive');
    assert.equal(ranked.kicker, 'Ranked');
    assert.match(ranked.bullets.map((b) => b.text).join(' '), /rank/);

    const casual = tutorialContent(mode, 'Quickmatch');
    assert.equal(casual.kicker, 'Casual');
    assert.doesNotMatch(casual.bullets.map((b) => b.text).join(' '), /rank/);
  }

  // Solo never touches MMR in either queue mode, so competitive Solo must not
  // be dressed up as ranked.
  const rankedSolo = tutorialContent('solo', 'Competitive');
  assert.equal(rankedSolo.kicker, 'Casual');
  assert.doesNotMatch(rankedSolo.bullets.map((b) => b.text).join(' '), /rank/);
});

test('every bullet points at a scene the renderer can actually draw', () => {
  const known = new Set<string>(TUTORIAL_SCENE_IDS);
  const used = new Set<string>();
  for (const mode of MODES) {
    for (const queueMode of ['Quickmatch', 'Competitive'] as const) {
      for (const bullet of tutorialContent(mode, queueMode).bullets) {
        assert.ok(
          known.has(bullet.scene),
          `${bullet.scene} is not in the renderer's scene registry`,
        );
        used.add(bullet.scene);
      }
    }
  }
  // A scene nobody references is dead weight in the WASM binary.
  assert.deepEqual([...used].sort(), [...known].sort());
});

test('the persistence key distinguishes ranked from casual for the same mode', () => {
  assert.notEqual(tutorialKey('duel', 'Quickmatch'), tutorialKey('duel', 'Competitive'));
  assert.equal(tutorialKey('duel', 'Competitive'), 'duel:ranked');
  assert.equal(tutorialKey('duel', 'Quickmatch'), 'duel:casual');
});
