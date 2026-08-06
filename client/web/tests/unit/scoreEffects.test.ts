import assert from 'node:assert/strict';
import test from 'node:test';

import {
  DEFAULT_SCORE_EFFECT_ID,
  MAX_ACTIVE_SCORE_EFFECTS,
  REDUCED_MOTION_SCORE_WAVE_DURATION_MS,
  SCORE_WAVE_DURATION_MS,
  createScoreEffectRegistry,
  createScoreEffectRuntime,
  drawScoreEffects,
  getScoringGoalOrigin,
  pruneExpiredScoreEffects,
  resetScoreEffects,
  sampleScoreWaveCells,
  smoothstep,
  syncScoreEffects,
  transformScoreEffectPosition,
} from '../../utils/scoreEffects.ts';
import type {
  ScoreEffectActivation,
  ScoreEffectObservation,
  ScoreEffectRenderer,
} from '../../utils/scoreEffects.ts';

const observation = (
  teamScores: Record<number, number> | null,
  overrides: Partial<ScoreEffectObservation> = {},
): ScoreEffectObservation => ({
  gameId: '42',
  engineEpoch: 1,
  tick: 10,
  teamScores,
  arenaWidth: 60,
  arenaHeight: 40,
  endZoneDepth: 10,
  nowMs: 1_000,
  ...overrides,
});

const activation: ScoreEffectActivation = {
  eventId: '42:1:11:0:1',
  effectId: DEFAULT_SCORE_EFFECT_ID,
  teamId: 0,
  previousScore: 0,
  score: 1,
  tick: 11,
  origin: { x: 9, y: 20 },
  startedAtMs: 1_000,
};

test('smoothstep clamps cleanly and is symmetric at its midpoint', () => {
  assert.equal(smoothstep(0, 1, -1), 0);
  assert.equal(smoothstep(0, 1, 0.5), 0.5);
  assert.equal(smoothstep(0, 1, 2), 1);
  assert.equal(smoothstep(4, 4, 3), 0);
  assert.equal(smoothstep(4, 4, 4), 1);
});

test('score cells use the same four rotation transforms as the Rust renderer', () => {
  const position = { x: 5, y: 7 };

  assert.deepEqual(transformScoreEffectPosition(position, 60, 40, 0), { x: 5, y: 7 });
  assert.deepEqual(transformScoreEffectPosition(position, 60, 40, 90), { x: 32, y: 5 });
  assert.deepEqual(transformScoreEffectPosition(position, 60, 40, 180), { x: 54, y: 32 });
  assert.deepEqual(transformScoreEffectPosition(position, 60, 40, 270), { x: 7, y: 54 });
});

test('score origins sit at each home-zone goal boundary and arena center', () => {
  assert.deepEqual(getScoringGoalOrigin(0, 60, 40, 10), { x: 9, y: 20 });
  assert.deepEqual(getScoringGoalOrigin(1, 60, 40, 10), { x: 50, y: 20 });
  assert.equal(getScoringGoalOrigin(2, 60, 40, 10), null);
  assert.equal(getScoringGoalOrigin(0, 20, 40, 10), null);
});

test('wave sampling is deterministic, bounded, and expires from timestamp alone', () => {
  const frame = {
    nowMs: 1_450,
    arenaWidth: 60,
    arenaHeight: 40,
    rotation: 0 as const,
    reducedMotion: false,
  };
  const first = sampleScoreWaveCells(activation, frame);
  const second = sampleScoreWaveCells(activation, frame);

  assert.deepEqual(first, second);
  assert.ok(first.length > 0);
  assert.ok(first.length < 625, 'a wave must never scan or paint the whole arena');
  assert.ok(first.every(cell => cell.opacity > 0 && cell.opacity <= 0.34));
  assert.ok(first.every(cell => cell.position.x >= 0 && cell.position.x < 60));
  assert.ok(first.every(cell => cell.position.y >= 0 && cell.position.y < 40));
  assert.deepEqual(
    sampleScoreWaveCells(activation, { ...frame, nowMs: 1_000 + SCORE_WAVE_DURATION_MS }),
    [],
  );
});

test('the sampled origin follows every rotation', () => {
  for (const rotation of [0, 90, 180, 270] as const) {
    const cells = sampleScoreWaveCells(activation, {
      nowMs: activation.startedAtMs,
      arenaWidth: 60,
      arenaHeight: 40,
      rotation,
      reducedMotion: false,
    });
    const transformedOrigin = transformScoreEffectPosition(
      activation.origin,
      60,
      40,
      rotation,
    );
    assert.ok(cells.some(cell => (
      cell.position.x === transformedOrigin.x &&
      cell.position.y === transformedOrigin.y
    )));
  }
});

test('the default renderer paints rotated, inset grid cells with balanced canvas state', () => {
  const runtime = createScoreEffectRuntime();
  resetScoreEffects(runtime, observation({ 0: 0, 1: 0 }));
  syncScoreEffects(runtime, observation({ 0: 1, 1: 0 }, { tick: 11 }));

  const fills: Array<[number, number, number, number]> = [];
  let saves = 0;
  let restores = 0;
  const context = {
    fillStyle: '',
    globalAlpha: 1,
    save() { saves += 1; },
    restore() { restores += 1; },
    fillRect(x: number, y: number, width: number, height: number) {
      fills.push([x, y, width, height]);
    },
  } as unknown as CanvasRenderingContext2D;

  drawScoreEffects(context, runtime, {
    nowMs: 1_000,
    cellSize: 10,
    arenaWidth: 60,
    arenaHeight: 40,
    rotation: 90,
    localTeamId: 0,
    reducedMotion: false,
  });

  // Team 0 origin (9, 20) rotates to (19, 9); canvas padding is 1px and
  // the 10px cell receives a restrained 0.8px inset on every edge.
  assert.ok(fills.some(([x, y, width, height]) => (
    Math.abs(x - 191.8) < 1e-9 &&
    Math.abs(y - 91.8) < 1e-9 &&
    Math.abs(width - 8.4) < 1e-9 &&
    Math.abs(height - 8.4) < 1e-9
  )));
  assert.equal(saves, 1);
  assert.equal(restores, 1);
});

test('reduced motion is a brief stationary goal wash, not a travelling wave', () => {
  const cells = sampleScoreWaveCells(activation, {
    nowMs: 1_080,
    arenaWidth: 60,
    arenaHeight: 40,
    rotation: 0,
    reducedMotion: true,
  });

  assert.ok(cells.length > 0);
  assert.ok(cells.every(cell => (
    Math.hypot(
      cell.position.x - activation.origin.x,
      cell.position.y - activation.origin.y,
    ) <= 2
  )));
  assert.deepEqual(
    sampleScoreWaveCells(activation, {
      nowMs: 1_000 + REDUCED_MOTION_SCORE_WAVE_DURATION_MS,
      arenaWidth: 60,
      arenaHeight: 40,
      rotation: 0,
      reducedMotion: true,
    }),
    [],
  );
});

test('only authoritative score increases start effects', () => {
  const runtime = createScoreEffectRuntime();

  assert.deepEqual(
    syncScoreEffects(runtime, observation({ 0: 0, 1: 0 })),
    { started: 0, reset: true },
  );
  assert.deepEqual(
    syncScoreEffects(runtime, observation({ 0: 3, 1: 0 }, { tick: 11, nowMs: 1_100 })),
    { started: 1, reset: false },
  );
  assert.equal(runtime.active.length, 1);
  assert.equal(runtime.active[0].previousScore, 0);
  assert.equal(runtime.active[0].score, 3);
  assert.deepEqual(runtime.active[0].origin, { x: 9, y: 20 });

  assert.deepEqual(
    syncScoreEffects(runtime, observation({ 0: 3, 1: 0 }, { tick: 12, nowMs: 1_200 })),
    { started: 0, reset: false },
  );
  assert.equal(runtime.active.length, 1);
});

test('a resync, game change, tick rewind, or score correction resets without replay', () => {
  const runtime = createScoreEffectRuntime();
  syncScoreEffects(runtime, observation({ 0: 0, 1: 0 }));
  syncScoreEffects(runtime, observation({ 0: 1, 1: 0 }, { tick: 11 }));
  assert.equal(runtime.active.length, 1);

  const resync = syncScoreEffects(
    runtime,
    observation({ 0: 2, 1: 0 }, { engineEpoch: 2, tick: 20 }),
  );
  assert.deepEqual(resync, { started: 0, reset: true });
  assert.equal(runtime.active.length, 0);
  assert.equal(runtime.scores[0], 2);

  syncScoreEffects(runtime, observation({ 0: 3, 1: 0 }, { engineEpoch: 2, tick: 21 }));
  assert.equal(runtime.active.length, 1);
  assert.deepEqual(
    syncScoreEffects(runtime, observation({ 0: 0, 1: 0 }, { gameId: '43', tick: 1 })),
    { started: 0, reset: true },
  );
  assert.equal(runtime.active.length, 0);

  syncScoreEffects(runtime, observation({ 0: 1, 1: 0 }, { gameId: '43', tick: 2 }));
  assert.deepEqual(
    syncScoreEffects(runtime, observation({ 0: 1, 1: 0 }, { gameId: '43', tick: 1 })),
    { started: 0, reset: true },
  );

  syncScoreEffects(runtime, observation({ 0: 2, 1: 0 }, { gameId: '43', tick: 3 }));
  assert.deepEqual(
    syncScoreEffects(runtime, observation({ 0: 1, 1: 0 }, { gameId: '43', tick: 4 })),
    { started: 0, reset: true },
  );
  assert.equal(runtime.active.length, 0);
});

test('active score effects remain bounded during repeated scoring', () => {
  const runtime = createScoreEffectRuntime();
  syncScoreEffects(runtime, observation({ 0: 0, 1: 0 }));

  for (let score = 1; score <= MAX_ACTIVE_SCORE_EFFECTS + 3; score += 1) {
    syncScoreEffects(runtime, observation(
      { 0: score, 1: 0 },
      { tick: 10 + score, nowMs: 1_000 + score },
    ));
  }

  assert.equal(runtime.active.length, MAX_ACTIVE_SCORE_EFFECTS);
  assert.equal(runtime.active.at(-1)?.score, MAX_ACTIVE_SCORE_EFFECTS + 3);
});

test('the registry supports a swapped renderer and owns lifecycle expiry', () => {
  const renderer: ScoreEffectRenderer = {
    id: 'quiet-flash',
    durationMs: 50,
    reducedMotionDurationMs: 20,
    draw() {},
  };
  const registry = createScoreEffectRegistry([renderer]);
  const runtime = createScoreEffectRuntime();
  resetScoreEffects(runtime, observation({ 0: 0, 1: 0 }));
  syncScoreEffects(runtime, observation(
    { 0: 1, 1: 0 },
    { tick: 11, effectId: renderer.id },
  ));

  assert.equal(registry.resolve(renderer.id), renderer);
  assert.equal(pruneExpiredScoreEffects(runtime, 1_049, false, registry), 0);
  assert.equal(pruneExpiredScoreEffects(runtime, 1_050, false, registry), 1);
  assert.equal(runtime.active.length, 0);
});
