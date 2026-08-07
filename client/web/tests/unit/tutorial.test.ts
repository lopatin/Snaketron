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
      assert.equal(content.steps.length, 3, `${content.key} must have three steps`);
      for (const step of content.steps) {
        assert.ok(step.title.length > 0);
        assert.ok(step.body.length > 0);
        assert.ok(step.visualLabel.length > 0);
        assert.ok(step.scene.length > 0);
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

test('every mode names the boost key, and only collectible modes mention NOS', () => {
  for (const queueMode of ['Quickmatch', 'Competitive'] as const) {
    // Every matchmade mode has Boost on the same key.
    for (const mode of ['duel', '2v2', 'ffa', 'solo'] as const) {
      const text = tutorialContent(mode, queueMode).steps.map((step) => step.body).join(' ');
      assert.match(text, /Space/, `${mode} must name the boost key`);
    }
    // The three contested modes fuel from pickups on the map...
    for (const mode of ['duel', '2v2', 'ffa'] as const) {
      const text = tutorialContent(mode, queueMode).steps.map((step) => step.body).join(' ');
      assert.match(text, /NOS/, `${mode} must explain NOS`);
    }
    // ...but a solo tank never empties and has nothing to collect, so telling
    // a solo player to look for canisters would send them hunting for
    // objectives the map does not contain.
    const solo = tutorialContent('solo', queueMode).steps.map((step) => step.body).join(' ');
    assert.doesNotMatch(solo, /NOS|canister/i, 'solo has no boost pickups');
    assert.match(solo, /never empties|never runs out/i, 'solo must say the tank is unlimited');
  }
});

test('Boost instructions follow the configured hold or toggle input mode', () => {
  for (const mode of MODES) {
    const hold = tutorialContent(mode, 'Quickmatch', { scoreLimit: 25 }, 'hold')
      .steps.map((step) => step.body)
      .join(' ');
    assert.match(hold, /hold Space to boost/i);
    assert.doesNotMatch(hold, /toggle boost/);

    const toggle = tutorialContent(mode, 'Quickmatch', { scoreLimit: 25 }, 'toggle')
      .steps.map((step) => step.body)
      .join(' ');
    assert.match(toggle, /press Space to toggle boost/i);
    assert.doesNotMatch(toggle, /hold Space to boost/i);
  }

  const toggleGame = tutorialContentForGame(
    {
      game_type: 'Solo',
      queue_mode: 'Quickmatch',
      properties: { score_limit: null },
    } as GameState,
    'toggle',
  );
  assert.ok(toggleGame);
  assert.match(
    toggleGame.steps.map((step) => step.body).join(' '),
    /press Space to toggle boost/i,
  );
});

test('no mode claims a clock, because no mode has one', () => {
  for (const key of ALL_TUTORIAL_KEYS) {
    const [mode, rank] = key.split(':') as [TutorialMode, 'ranked' | 'casual'];
    const content = tutorialContent(mode, rank === 'ranked' ? 'Competitive' : 'Quickmatch', {
      scoreLimit: 25,
    });
    const text = content.steps.map((step) => step.body).join(' ');
    assert.doesNotMatch(
      text,
      /\d+\s*seconds|time limit|time runs out/i,
      `${key} must not promise a timed finish — team matches race to a score and the rest run until everyone is dead`,
    );
  }
});

test('FFA teaches its score-based result rather than a last-survivor win', () => {
  for (const queueMode of ['Quickmatch', 'Competitive'] as const) {
    const text = tutorialContent('ffa', queueMode).steps.map((step) => step.body).join(' ');
    assert.match(text, /when all snakes are out/i);
    assert.match(text, /highest score wins/i);
    assert.doesNotMatch(text, /last snake standing/i);
  }
});

test('team copy races to the score limit it is handed, never a baked-in number', () => {
  // The engine's real targets are 25 Quickmatch / 50 Competitive
  // (`team_score_limit` in common/src/game_state.rs). The invariant that
  // actually protects the player is that the briefing prints whatever the
  // match carries, so an engine-side change cannot leave stale copy behind.
  for (const mode of ['duel', '2v2'] as const) {
    for (const [queueMode, limit] of [
      ['Quickmatch', 25],
      ['Competitive', 50],
      ['Quickmatch', 7],
    ] as const) {
      const text = tutorialContent(mode, queueMode, { scoreLimit: limit })
        .steps.map((step) => step.body)
        .join(' ');
      assert.match(text, new RegExp(`First to ${limit}\\b`));
      assert.match(text, /rival base/);
      // No other target may appear alongside it.
      const numbers = text.match(/\b\d+\b/g) ?? [];
      assert.deepEqual(
        numbers.filter((n) => n !== String(limit)),
        [],
        `${mode}/${queueMode} leaked a number that is not the score limit`,
      );
    }
  }
});

test('a team match with no score limit describes the rule without inventing a number', () => {
  const text = tutorialContent('duel', 'Quickmatch', { scoreLimit: null })
    .steps.map((step) => step.body)
    .join(' ');
  assert.doesNotMatch(text, /\b\d+\b/);
  assert.match(text, /score target/i);
});

test('queue context stays in the kicker instead of repeating in step copy', () => {
  for (const mode of ['duel', '2v2', 'ffa'] as const) {
    const competitive = tutorialContent(mode, 'Competitive');
    assert.equal(competitive.kicker, 'COMPETITIVE');
    assert.doesNotMatch(competitive.steps.map((step) => step.body).join(' '), /rank/);

    const casual = tutorialContent(mode, 'Quickmatch');
    assert.equal(casual.kicker, 'QUICK MATCH');
    assert.doesNotMatch(casual.steps.map((step) => step.body).join(' '), /rank/);
  }

  // Solo never touches MMR in either queue mode, so competitive Solo must not
  // be dressed up as ranked.
  const competitiveSolo = tutorialContent('solo', 'Competitive');
  assert.equal(competitiveSolo.kicker, 'HIGH SCORE');
  assert.doesNotMatch(competitiveSolo.steps.map((step) => step.body).join(' '), /rank/);
  assert.equal(tutorialContent('solo', 'Quickmatch').kicker, 'HIGH SCORE');
});

test('every step points at a scene the renderer can actually draw', () => {
  const known = new Set<string>(TUTORIAL_SCENE_IDS);
  const used = new Set<string>();
  for (const mode of MODES) {
    for (const queueMode of ['Quickmatch', 'Competitive'] as const) {
      for (const step of tutorialContent(mode, queueMode).steps) {
        assert.ok(
          known.has(step.scene),
          `${step.scene} is not in the renderer's scene registry`,
        );
        used.add(step.scene);
      }
    }
  }
  // A scene nobody references is dead weight in the WASM binary.
  assert.deepEqual([...used].sort(), [...known].sort());
});

test('progressive steps stay concise enough to scan one at a time', () => {
  const wordCount = (text: string): number => text.trim().split(/\s+/).filter(Boolean).length;

  for (const mode of MODES) {
    for (const queueMode of ['Quickmatch', 'Competitive'] as const) {
      const scoreLimit = queueMode === 'Competitive' ? 50 : 25;
      const content = tutorialContent(mode, queueMode, { scoreLimit });
      const totalBodyWords = content.steps.reduce(
        (total, step) => total + wordCount(step.body),
        0,
      );

      assert.ok(totalBodyWords <= 34, `${content.key} has ${totalBodyWords} body words`);
      for (const step of content.steps) {
        assert.ok(wordCount(step.title) <= 3, `${content.key}/${step.title} title is too long`);
        assert.ok(
          wordCount(step.body) <= 17,
          `${content.key}/${step.title} has ${wordCount(step.body)} body words`,
        );
      }
    }
  }
});

test('the persistence key distinguishes ranked from casual for the same mode', () => {
  assert.notEqual(tutorialKey('duel', 'Quickmatch'), tutorialKey('duel', 'Competitive'));
  assert.equal(tutorialKey('duel', 'Competitive'), 'duel:ranked');
  assert.equal(tutorialKey('duel', 'Quickmatch'), 'duel:casual');
});
