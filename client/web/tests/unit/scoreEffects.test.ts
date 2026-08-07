import assert from 'node:assert/strict';
import test from 'node:test';

import {
  DEFAULT_SCORE_EFFECT_ID,
  GOAL_CUE_RETRACTION_WINDOW_MS,
  MAX_ACTIVE_SCORE_EFFECTS,
  MAX_SCORE_MAGNITUDE,
  MIN_SCORE_MAGNITUDE,
  REDUCED_MOTION_SCORE_CELEBRATION_DURATION_MS,
  REDUCED_MOTION_SCORE_WAVE_DURATION_MS,
  SCORE_CELEBRATION_DURATION_MS,
  SCORE_WAVE_DURATION_MS,
  createScoreEffectRegistry,
  createScoreEffectRuntime,
  drawScoreEffects,
  getScoreEffectTeamColor,
  getScoreReadoutColor,
  pruneExpiredScoreEffects,
  resetScoreEffects,
  sampleScoreReadout,
  sampleScoreWaveCells,
  scoreEffectMagnitude,
  smoothstep,
  syncPredictedScoreEffects,
  transformScoreEffectPosition,
} from '../../utils/scoreEffects.ts';
import type {
  PredictedGoalCue,
  PredictedScoreVisualState,
  ScoreEffectActivation,
  ScoreEffectRenderer,
} from '../../utils/scoreEffects.ts';

const goalCue = (overrides: Partial<PredictedGoalCue> = {}): PredictedGoalCue => ({
  tick: 10,
  team_id: 0,
  snake_id: 1,
  position: { x: 9, y: 12 },
  points: 12,
  ...overrides,
});

const visualState = (
  goals: readonly PredictedGoalCue[],
  overrides: Partial<PredictedScoreVisualState> = {},
): PredictedScoreVisualState => ({
  predicted_tick: 10,
  committed_tick: 6,
  tick_duration_ms: 50,
  goals,
  ...overrides,
});

const activation: ScoreEffectActivation = {
  eventId: '42:goal:10:0:1:9:12:12',
  effectId: DEFAULT_SCORE_EFFECT_ID,
  teamId: 0,
  snakeId: 1,
  points: 12,
  tick: 10,
  origin: { x: 9, y: 12 },
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

test('a predicted goal cue starts a celebration at the cell it was scored on', () => {
  const runtime = createScoreEffectRuntime();

  const result = syncPredictedScoreEffects(
    runtime,
    '42',
    visualState([goalCue()], { predicted_tick: 14 }),
    10_000,
  );

  assert.deepEqual(result, { started: 1, cancelled: 0 });
  assert.equal(runtime.active.length, 1);
  const [started] = runtime.active;
  assert.deepEqual(started.origin, { x: 9, y: 12 });
  assert.equal(started.points, 12);
  assert.equal(started.teamId, 0);
  assert.equal(started.snakeId, 1);
  // Four ticks of 50ms already elapsed inside prediction, so the celebration
  // resumes at its true phase rather than restarting from zero.
  assert.equal(started.startedAtMs, 9_800);
});

test('normal forward history retention neither cancels nor restarts a celebration', () => {
  const runtime = createScoreEffectRuntime();
  const cue = goalCue();

  syncPredictedScoreEffects(runtime, '42', visualState([cue]), 1_000);
  const original = runtime.active[0];

  const result = syncPredictedScoreEffects(
    runtime,
    '42',
    visualState([cue], { predicted_tick: 14, committed_tick: 12 }),
    1_200,
  );

  assert.deepEqual(result, { started: 0, cancelled: 0 });
  assert.equal(runtime.active.length, 1);
  assert.equal(runtime.active[0], original);
});

test('a reconciliation that drops the cue retracts the celebration immediately', () => {
  const runtime = createScoreEffectRuntime();
  syncPredictedScoreEffects(runtime, '42', visualState([goalCue()]), 1_000);
  assert.equal(runtime.active.length, 1);

  const result = syncPredictedScoreEffects(
    runtime,
    '42',
    visualState([], { predicted_tick: 11 }),
    1_050,
  );

  assert.deepEqual(result, { started: 0, cancelled: 1 });
  assert.equal(runtime.active.length, 0);
  assert.equal(runtime.seenEventIds.size, 0, 'a retracted identity may replay');

  // The same goal predicted again after a rollback plays once more.
  const replay = syncPredictedScoreEffects(runtime, '42', visualState([goalCue()]), 1_100);
  assert.deepEqual(replay, { started: 1, cancelled: 0 });
});

test('a cue relocated by replay retracts the old celebration and starts the new one', () => {
  const runtime = createScoreEffectRuntime();
  syncPredictedScoreEffects(runtime, '42', visualState([goalCue()]), 1_000);

  const result = syncPredictedScoreEffects(
    runtime,
    '42',
    visualState([goalCue({ position: { x: 9, y: 16 }, points: 4 })]),
    1_000,
  );

  assert.deepEqual(result, { started: 1, cancelled: 1 });
  assert.equal(runtime.active.length, 1);
  assert.deepEqual(runtime.active[0].origin, { x: 9, y: 16 });
  assert.equal(runtime.active[0].points, 4);
});

test('stale, empty, and baseline cues never start a celebration', () => {
  const runtime = createScoreEffectRuntime();

  // Older than the whole celebration.
  const staleTicks = Math.ceil(SCORE_CELEBRATION_DURATION_MS / 50);
  assert.deepEqual(
    syncPredictedScoreEffects(
      runtime,
      '42',
      visualState([goalCue()], { predicted_tick: 10 + staleTicks }),
      1_000,
    ),
    { started: 0, cancelled: 0 },
  );

  // A pointless cue cannot produce a "+0".
  assert.deepEqual(
    syncPredictedScoreEffects(
      runtime,
      '42',
      visualState([goalCue({ tick: 11, points: 0 })]),
      1_000,
    ),
    { started: 0, cancelled: 0 },
  );

  // Snapshot baseline history is not a fresh visual event.
  assert.deepEqual(
    syncPredictedScoreEffects(
      runtime,
      '42',
      visualState([goalCue({ tick: 9 })], { predicted_tick: 10 }),
      1_000,
      9,
    ),
    { started: 0, cancelled: 0 },
  );
  assert.equal(runtime.active.length, 0);

  // A cue past the baseline in the same replay still plays.
  assert.deepEqual(
    syncPredictedScoreEffects(
      runtime,
      '42',
      visualState([goalCue({ tick: 9 }), goalCue({ tick: 10, snake_id: 3 })], {
        predicted_tick: 10,
      }),
      1_000,
      9,
    ),
    { started: 1, cancelled: 0 },
  );
});

test('a visual state without goals is inert and leaves crash-only payloads alone', () => {
  const runtime = createScoreEffectRuntime();

  assert.deepEqual(
    syncPredictedScoreEffects(
      runtime,
      '42',
      { predicted_tick: 10, committed_tick: 6, tick_duration_ms: 50 },
      1_000,
    ),
    { started: 0, cancelled: 0 },
  );
  assert.equal(runtime.active.length, 0);
});

test('active celebrations remain bounded and a dropped one never restarts', () => {
  const runtime = createScoreEffectRuntime();
  const cues: PredictedGoalCue[] = [];

  for (let index = 0; index < MAX_ACTIVE_SCORE_EFFECTS + 3; index += 1) {
    cues.push(goalCue({ snake_id: index, position: { x: 9, y: 12 + index } }));
    const result = syncPredictedScoreEffects(runtime, '42', visualState(cues), 1_000);
    // Each round re-presents every earlier cue; only the new one may start.
    assert.deepEqual(
      result,
      { started: 1, cancelled: 0 },
      `round ${index} must start exactly one celebration`,
    );
  }

  assert.equal(runtime.active.length, MAX_ACTIVE_SCORE_EFFECTS);
  assert.equal(runtime.active.at(-1)?.snakeId, MAX_ACTIVE_SCORE_EFFECTS + 2);

  // The three that lost their slot keep their dedupe entries, so replaying the
  // same cue set cannot resurrect them.
  const replay = syncPredictedScoreEffects(runtime, '42', visualState(cues), 1_000);
  assert.deepEqual(replay, { started: 0, cancelled: 0 });
  assert.equal(runtime.active.length, MAX_ACTIVE_SCORE_EFFECTS);
});

test('a cue that ages out of engine retention never retracts a live celebration', () => {
  const runtime = createScoreEffectRuntime();
  syncPredictedScoreEffects(runtime, '42', visualState([goalCue()]), 1_000);
  assert.equal(runtime.active.length, 1);

  // Retention in common/src/game_state.rs is wider than the celebration, so a
  // cue disappearing this late is an ordinary expiry, not a rollback.
  const late = syncPredictedScoreEffects(
    runtime,
    '42',
    visualState([], { predicted_tick: 60 }),
    1_000 + GOAL_CUE_RETRACTION_WINDOW_MS,
  );

  assert.deepEqual(late, { started: 0, cancelled: 0 });
  assert.equal(runtime.active.length, 1);
  assert.ok(
    runtime.seenEventIds.has(runtime.active[0].eventId),
    'a still-running celebration keeps its identity so it cannot be enqueued twice',
  );
});

test('the retraction window outlives the celebration it guards', () => {
  // If this ever inverts, a rollback late in the animation would stop being
  // retractable. If GOAL_CUE_RETRACTION_WINDOW_MS ever exceeds the Rust-side
  // RECENT_GOAL_RETENTION_MS, ordinary expiry would start popping effects.
  assert.ok(GOAL_CUE_RETRACTION_WINDOW_MS >= SCORE_CELEBRATION_DURATION_MS);
  assert.ok(GOAL_CUE_RETRACTION_WINDOW_MS < 1_800);
});

test('resetting clears both live celebrations and their dedupe identities', () => {
  const runtime = createScoreEffectRuntime();
  syncPredictedScoreEffects(runtime, '42', visualState([goalCue()]), 1_000);
  assert.equal(runtime.active.length, 1);

  resetScoreEffects(runtime);

  assert.equal(runtime.active.length, 0);
  assert.equal(runtime.seenEventIds.size, 0);
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

test('the wave is centered on the scoring cell under every rotation', () => {
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

test('effect magnitude grows with the points banked, sub-linearly and clamped', () => {
  const magnitudes = [1, 2, 4, 8, 12].map(scoreEffectMagnitude);
  for (let i = 1; i < magnitudes.length; i += 1) {
    assert.ok(
      magnitudes[i] >= magnitudes[i - 1],
      'a bigger goal must never render smaller than a smaller one',
    );
  }

  assert.equal(scoreEffectMagnitude(4), 1, 'the reference goal renders nominally');
  // Twelve points is three times four, but must not be three times the size.
  assert.ok(scoreEffectMagnitude(12) < 3 * scoreEffectMagnitude(4));

  assert.equal(scoreEffectMagnitude(1), MIN_SCORE_MAGNITUDE);
  assert.equal(scoreEffectMagnitude(500), MAX_SCORE_MAGNITUDE);
  // The scale must not saturate before the goals players actually score: a
  // twelve-point bank has to still out-size a nine-point one.
  assert.ok(scoreEffectMagnitude(12) > scoreEffectMagnitude(9));
  // Degenerate input can never produce a zero-size or NaN celebration.
  assert.equal(scoreEffectMagnitude(0), MIN_SCORE_MAGNITUDE);
  assert.equal(scoreEffectMagnitude(-3), MIN_SCORE_MAGNITUDE);
  assert.equal(scoreEffectMagnitude(Number.NaN), MIN_SCORE_MAGNITUDE);
});

test('a bigger goal paints a wider wave and a larger number', () => {
  const frame = {
    nowMs: 1_400,
    cellSize: 12,
    arenaWidth: 60,
    arenaHeight: 40,
    rotation: 0 as const,
    reducedMotion: false,
  };
  const at = (points: number): ScoreEffectActivation => ({
    ...activation,
    points,
    origin: { x: 30, y: 20 },
  });

  const spread = (points: number): number => {
    const cells = sampleScoreWaveCells(at(points), frame);
    assert.ok(cells.length > 0, `expected a wave for +${points}`);
    return Math.max(...cells.map(cell => Math.hypot(cell.position.x - 30, cell.position.y - 20)));
  };
  const fontSize = (points: number): number => {
    const readout = sampleScoreReadout(at(points), frame);
    assert.ok(readout);
    return readout.fontSize;
  };

  assert.ok(spread(1) < spread(4), 'a one-point tap must ripple less than a four-point goal');
  assert.ok(spread(4) < spread(12), 'a twelve-point bank must ripple further still');
  assert.ok(fontSize(1) < fontSize(4));
  assert.ok(fontSize(4) < fontSize(12));

  // The readout still tracks cell size, so the scaling composes rather than
  // replacing the existing responsive sizing.
  const small = sampleScoreReadout(at(4), { ...frame, cellSize: 6 });
  assert.ok(small);
  assert.ok(small.fontSize < fontSize(4));
});

test('even the largest goal keeps the wave bounded and off the whole arena', () => {
  const cells = sampleScoreWaveCells(
    { ...activation, points: 999, origin: { x: 30, y: 20 } },
    {
      nowMs: 1_600,
      arenaWidth: 60,
      arenaHeight: 40,
      rotation: 0,
      reducedMotion: false,
    },
  );

  assert.ok(cells.length > 0);
  assert.ok(cells.length < 60 * 40 * 0.5, 'a wave must never wash half the arena');
  assert.ok(cells.every(cell => cell.opacity > 0 && cell.opacity <= 0.34));
  assert.ok(cells.every(cell => (
    cell.position.x >= 0 && cell.position.x < 60 &&
    cell.position.y >= 0 && cell.position.y < 40
  )));
});

test('the points readout rises from the scoring cell, fades out, and outlives the wave', () => {
  const frame = {
    cellSize: 10,
    arenaWidth: 60,
    arenaHeight: 40,
    rotation: 0 as const,
    reducedMotion: false,
  };

  const start = sampleScoreReadout(activation, { ...frame, nowMs: 1_000 });
  assert.ok(start);
  assert.equal(start.text, '+12');
  // Origin (9, 12) with a 10px cell and 1px canvas padding.
  assert.equal(start.centerX, 96);
  assert.equal(start.centerY, 126);
  assert.equal(start.opacity, 1, 'a goal must never lose a frame to a fade-in');
  assert.ok(start.scale < 1, 'and pops in from a smaller scale instead');

  const mid = sampleScoreReadout(activation, { ...frame, nowMs: 1_300 });
  assert.ok(mid);
  assert.ok(mid.centerY < start.centerY, 'the readout must travel upward');
  assert.equal(mid.centerX, start.centerX, 'and only upward');
  assert.ok(mid.opacity > 0.9);

  const late = sampleScoreReadout(activation, { ...frame, nowMs: 1_950 });
  assert.ok(late);
  assert.ok(late.centerY < mid.centerY);
  assert.ok(late.opacity > 0 && late.opacity < mid.opacity, 'it must fade as it rises');

  // "Fades upward" means still climbing while fading, not rising to a stop and
  // then fading in place. An ease-out rise spends ~90% of its travel before the
  // fade begins; a substantial share must remain.
  const end = sampleScoreReadout(activation, {
    ...frame,
    nowMs: 1_000 + SCORE_CELEBRATION_DURATION_MS - 1,
  });
  assert.ok(end);
  const totalRise = start.centerY - end.centerY;
  const fadeStart = sampleScoreReadout(activation, {
    ...frame,
    nowMs: 1_000 + SCORE_CELEBRATION_DURATION_MS * 0.55,
  });
  assert.ok(fadeStart);
  const riseDuringFade = fadeStart.centerY - end.centerY;
  assert.ok(
    riseDuringFade / totalRise > 0.35,
    `only ${((riseDuringFade / totalRise) * 100).toFixed(0)}% of the rise happens while fading`,
  );

  // The wave is gone well before the number is.
  assert.deepEqual(
    sampleScoreWaveCells(activation, { ...frame, nowMs: 1_950, arenaWidth: 60 }),
    [],
  );
  assert.equal(
    sampleScoreReadout(activation, {
      ...frame,
      nowMs: 1_000 + SCORE_CELEBRATION_DURATION_MS,
    }),
    null,
  );
});

test('the readout follows the rotated cell and stays inside the canvas', () => {
  const topEdge: ScoreEffectActivation = {
    ...activation,
    origin: { x: 9, y: 0 },
  };
  const readout = sampleScoreReadout(topEdge, {
    nowMs: 1_900,
    cellSize: 10,
    arenaWidth: 60,
    arenaHeight: 40,
    rotation: 0,
    reducedMotion: false,
  });

  assert.ok(readout);
  assert.ok(readout.centerY >= (readout.fontSize * readout.scale) / 2 - 1e-9);

  // Rotated 90°, cell (9, 12) renders at (27, 9) on a 40x60 cell canvas.
  const rotated = sampleScoreReadout(activation, {
    nowMs: 1_000,
    cellSize: 10,
    arenaWidth: 60,
    arenaHeight: 40,
    rotation: 90,
    reducedMotion: false,
  });
  assert.ok(rotated);
  assert.equal(rotated.centerX, 1 + 27 * 10 + 5);
  assert.equal(rotated.centerY, 1 + 9 * 10 + 5);
});

test('reduced motion holds the readout in place and shortens the celebration', () => {
  const frame = {
    cellSize: 10,
    arenaWidth: 60,
    arenaHeight: 40,
    rotation: 0 as const,
    reducedMotion: true,
  };

  const early = sampleScoreReadout(activation, { ...frame, nowMs: 1_050 });
  const later = sampleScoreReadout(activation, { ...frame, nowMs: 1_400 });
  assert.ok(early && later);
  assert.equal(early.centerY, later.centerY, 'reduced motion must not travel');
  assert.equal(early.scale, 1);
  assert.ok(later.opacity < early.opacity);
  assert.equal(
    sampleScoreReadout(activation, {
      ...frame,
      nowMs: 1_000 + REDUCED_MOTION_SCORE_CELEBRATION_DURATION_MS,
    }),
    null,
  );
});

test('the default renderer paints rotated inset cells and a stroked readout', () => {
  const runtime = createScoreEffectRuntime();
  syncPredictedScoreEffects(
    runtime,
    '42',
    visualState([goalCue()], { predicted_tick: 10 }),
    1_000,
  );

  const fills: Array<[number, number, number, number]> = [];
  const translations: Array<[number, number]> = [];
  const scales: Array<[number, number]> = [];
  const strokedText: Array<[string, number, number, string]> = [];
  const filledText: Array<[string, number, number, string]> = [];
  let saves = 0;
  let restores = 0;
  const context = {
    fillStyle: '',
    strokeStyle: '',
    globalAlpha: 1,
    font: '',
    lineWidth: 0,
    lineJoin: 'miter',
    miterLimit: 10,
    textAlign: 'start',
    textBaseline: 'alphabetic',
    save() { saves += 1; },
    restore() { restores += 1; },
    translate(x: number, y: number) { translations.push([x, y]); },
    scale(x: number, y: number) { scales.push([x, y]); },
    fillRect(x: number, y: number, width: number, height: number) {
      fills.push([x, y, width, height]);
    },
    strokeText(text: string, x: number, y: number) {
      strokedText.push([text, x, y, this.strokeStyle as string]);
    },
    fillText(text: string, x: number, y: number) {
      filledText.push([text, x, y, this.fillStyle as string]);
    },
  } as unknown as CanvasRenderingContext2D;

  drawScoreEffects(context, runtime, {
    nowMs: 1_100,
    cellSize: 10,
    arenaWidth: 60,
    arenaHeight: 40,
    rotation: 90,
    localTeamId: 0,
    reducedMotion: false,
  });

  // Origin (9, 12) rotates to (27, 9); canvas padding is 1px and the 10px cell
  // receives a restrained 0.8px inset on every edge.
  assert.ok(fills.some(([x, y, width, height]) => (
    Math.abs(x - 271.8) < 1e-9 &&
    Math.abs(y - 91.8) < 1e-9 &&
    Math.abs(width - 8.4) < 1e-9 &&
    Math.abs(height - 8.4) < 1e-9
  )));
  // The readout is stroked before it is filled, so the halo never covers it,
  // and it uses the deeper team tone rather than the wave's lighter colour.
  const readout = sampleScoreReadout(runtime.active[0], {
    nowMs: 1_100,
    cellSize: 10,
    arenaWidth: 60,
    arenaHeight: 40,
    rotation: 90,
    reducedMotion: false,
  });
  assert.ok(readout);
  assert.deepEqual(translations, [[readout.centerX, readout.centerY]]);
  assert.deepEqual(scales, [[readout.scale, readout.scale]]);
  assert.deepEqual(strokedText, [['+12', 0, 0, '#ffffff']]);
  assert.deepEqual(filledText, [['+12', 0, 0, getScoreReadoutColor(0, 0)]]);
  assert.notEqual(
    getScoreReadoutColor(0, 0),
    getScoreEffectTeamColor(0, 0),
    'the readout must be darker than the wave to stay legible on a tinted end zone',
  );
  assert.equal(saves, 2);
  assert.equal(restores, 2);
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
  syncPredictedScoreEffects(
    runtime,
    '42',
    visualState([goalCue()], { predicted_tick: 10 }),
    1_000,
    undefined,
    renderer.id,
  );

  assert.equal(registry.resolve(renderer.id), renderer);
  assert.equal(pruneExpiredScoreEffects(runtime, 1_049, false, registry), 0);
  assert.equal(pruneExpiredScoreEffects(runtime, 1_050, false, registry), 1);
  assert.equal(runtime.active.length, 0);
});
