import assert from 'node:assert/strict';
import test from 'node:test';

import {
  CRASH_EXPLOSION_DURATION_MS,
  CRASH_EXPLOSION_FRAME_COUNT,
  REDUCED_MOTION_EXPLOSION_DURATION_MS,
  REDUCED_MOTION_FRAME_INDEX,
  createCrashExplosion,
  enqueueCrashExplosion,
  getCrashExplosionFrameIndex,
  getCrashExplosionRenderState,
  syncPredictedCrashExplosions,
  transformExplosionPosition,
} from '../../utils/crashExplosion.ts';

test('crash positions use the same four rotation transforms as the Rust renderer', () => {
  const position = { x: 5, y: 7 };

  assert.deepEqual(transformExplosionPosition(position, 60, 40, 0), { x: 5, y: 7 });
  assert.deepEqual(transformExplosionPosition(position, 60, 40, 90), { x: 32, y: 5 });
  assert.deepEqual(transformExplosionPosition(position, 60, 40, 180), { x: 54, y: 32 });
  assert.deepEqual(transformExplosionPosition(position, 60, 40, 270), { x: 7, y: 54 });
});

test('the full sprite sequence is time-based and expires without a stale frame', () => {
  assert.equal(getCrashExplosionFrameIndex(0), 0);
  assert.equal(
    getCrashExplosionFrameIndex(CRASH_EXPLOSION_DURATION_MS / 2),
    CRASH_EXPLOSION_FRAME_COUNT / 2,
  );
  assert.equal(
    getCrashExplosionFrameIndex(CRASH_EXPLOSION_DURATION_MS - 0.01),
    CRASH_EXPLOSION_FRAME_COUNT - 1,
  );
  assert.equal(getCrashExplosionFrameIndex(CRASH_EXPLOSION_DURATION_MS), null);
  assert.equal(getCrashExplosionFrameIndex(-1), null);
});

test('reduced motion shows one brief peak frame', () => {
  assert.equal(getCrashExplosionFrameIndex(0, true), REDUCED_MOTION_FRAME_INDEX);
  assert.equal(
    getCrashExplosionFrameIndex(REDUCED_MOTION_EXPLOSION_DURATION_MS - 1, true),
    REDUCED_MOTION_FRAME_INDEX,
  );
  assert.equal(
    getCrashExplosionFrameIndex(REDUCED_MOTION_EXPLOSION_DURATION_MS, true),
    null,
  );
});

test('render state includes canvas padding and stays centered on the transformed cell', () => {
  const explosion = createCrashExplosion('game:7', 2, { x: 5, y: 7 }, 1_000);
  assert.ok(explosion);

  const state = getCrashExplosionRenderState(
    explosion,
    1_000,
    10,
    60,
    40,
    90,
  );
  assert.ok(state);
  assert.equal(state.centerX, 326);
  assert.equal(state.centerY, 56);
});

test('wall explosions remain exactly centered on edge cells without an inward clamp', () => {
  const explosion = createCrashExplosion('game:edge', 0, { x: 0, y: 20 }, 1_000);
  assert.ok(explosion);

  const leftEdgeState = getCrashExplosionRenderState(
    explosion,
    1_000,
    15,
    40,
    40,
    0,
  );
  assert.ok(leftEdgeState);
  assert.equal(leftEdgeState.centerX, 8.5);

  const rotatedEdgeState = getCrashExplosionRenderState(
    explosion,
    1_000,
    15,
    40,
    40,
    270,
  );
  assert.ok(rotatedEdgeState);
  assert.equal(rotatedEdgeState.centerY, 593.5);
  assert.equal(rotatedEdgeState.drawSize, 108);
});

test('event IDs dedupe replays while distinct crashes can animate together', () => {
  const explosions = [];
  const seen = new Set<string>();

  assert.equal(
    enqueueCrashExplosion(explosions, seen, '42:10', 0, { x: 2, y: 3 }, 100),
    true,
  );
  assert.equal(
    enqueueCrashExplosion(explosions, seen, '42:10', 0, { x: 2, y: 3 }, 101),
    false,
  );
  assert.equal(
    enqueueCrashExplosion(explosions, seen, '42:11', 1, { x: 8, y: 9 }, 102),
    true,
  );
  assert.equal(explosions.length, 2);
});

test('a retained crash cue starts backdated to its predicted tick', () => {
  const explosions = [];
  const seen = new Set<string>();

  const result = syncPredictedCrashExplosions(
    explosions,
    seen,
    '42',
    {
      predicted_tick: 13,
      committed_tick: 9,
      tick_duration_ms: 100,
      cues: [{ tick: 10, snake_id: 0, position: { x: 2, y: 3 } }],
    },
    10_000,
  );

  assert.deepEqual(result, { started: 1, cancelled: 0 });
  assert.equal(explosions.length, 1);
  assert.equal(explosions[0].startedAt, 9_700);
  assert.equal(explosions[0].predictionTick, 10);
});

test('normal forward history retention neither cancels nor restarts an explosion', () => {
  const explosions = [];
  const seen = new Set<string>();
  const cue = { tick: 10, snake_id: 0, position: { x: 2, y: 3 } };

  syncPredictedCrashExplosions(
    explosions,
    seen,
    '42',
    {
      predicted_tick: 10,
      committed_tick: 8,
      tick_duration_ms: 100,
      cues: [cue],
    },
    1_000,
  );
  const originalExplosion = explosions[0];

  const result = syncPredictedCrashExplosions(
    explosions,
    seen,
    '42',
    {
      predicted_tick: 12,
      committed_tick: 10,
      tick_duration_ms: 100,
      cues: [cue],
    },
    1_200,
  );

  assert.deepEqual(result, { started: 0, cancelled: 0 });
  assert.equal(explosions.length, 1);
  assert.equal(explosions[0], originalExplosion);
  assert.equal(explosions[0].startedAt, 1_000);
});

test('reconciliation absence cancels the prediction and releases its dedupe identity', () => {
  const explosions = [];
  const seen = new Set<string>();
  const cue = { tick: 10, snake_id: 0, position: { x: 2, y: 3 } };

  syncPredictedCrashExplosions(
    explosions,
    seen,
    '42',
    {
      predicted_tick: 10,
      committed_tick: 8,
      tick_duration_ms: 100,
      cues: [cue],
    },
    1_000,
  );
  const eventId = explosions[0].eventId;

  const result = syncPredictedCrashExplosions(
    explosions,
    seen,
    '42',
    {
      predicted_tick: 10,
      committed_tick: 10,
      tick_duration_ms: 100,
      cues: [],
    },
    1_010,
  );

  assert.deepEqual(result, { started: 0, cancelled: 1 });
  assert.equal(explosions.length, 0);
  assert.equal(seen.has(eventId), false);

  const replay = syncPredictedCrashExplosions(
    explosions,
    seen,
    '42',
    {
      predicted_tick: 10,
      committed_tick: 8,
      tick_duration_ms: 100,
      cues: [cue],
    },
    1_020,
  );
  assert.deepEqual(replay, { started: 1, cancelled: 0 });
});

test('a corrected crash position cancels the old effect and starts the moved cue', () => {
  const explosions = [];
  const seen = new Set<string>();

  syncPredictedCrashExplosions(
    explosions,
    seen,
    '42',
    {
      predicted_tick: 10,
      committed_tick: 8,
      tick_duration_ms: 100,
      cues: [{ tick: 10, snake_id: 0, position: { x: 2, y: 3 } }],
    },
    1_000,
  );
  const oldEventId = explosions[0].eventId;

  const result = syncPredictedCrashExplosions(
    explosions,
    seen,
    '42',
    {
      predicted_tick: 10,
      committed_tick: 9,
      tick_duration_ms: 100,
      cues: [{ tick: 10, snake_id: 0, position: { x: 3, y: 3 } }],
    },
    1_010,
  );

  assert.deepEqual(result, { started: 1, cancelled: 1 });
  assert.equal(explosions.length, 1);
  assert.deepEqual(explosions[0].position, { x: 3, y: 3 });
  assert.equal(seen.has(oldEventId), false);
});

test('a Team-style cue remains active without consulting the already-respawned snake', () => {
  const explosions = [];
  const seen = new Set<string>();
  const cue = { tick: 20, snake_id: 1, position: { x: 39, y: 12 } };

  const started = syncPredictedCrashExplosions(
    explosions,
    seen,
    'team-game',
    {
      predicted_tick: 20,
      committed_tick: 19,
      tick_duration_ms: 100,
      cues: [cue],
    },
    2_000,
  );
  const retained = syncPredictedCrashExplosions(
    explosions,
    seen,
    'team-game',
    {
      predicted_tick: 21,
      committed_tick: 20,
      tick_duration_ms: 100,
      cues: [cue],
    },
    2_100,
  );

  assert.deepEqual(started, { started: 1, cancelled: 0 });
  assert.deepEqual(retained, { started: 0, cancelled: 0 });
  assert.equal(explosions.length, 1);
});

test('a new engine baseline suppresses committed history but permits newer prediction', () => {
  const explosions = [];
  const seen = new Set<string>();

  const result = syncPredictedCrashExplosions(
    explosions,
    seen,
    '42',
    {
      predicted_tick: 22,
      committed_tick: 20,
      tick_duration_ms: 100,
      cues: [
        { tick: 18, snake_id: 0, position: { x: 1, y: 1 } },
        { tick: 20, snake_id: 1, position: { x: 2, y: 2 } },
        { tick: 21, snake_id: 2, position: { x: 3, y: 3 } },
      ],
    },
    5_000,
    20,
  );

  assert.deepEqual(result, { started: 1, cancelled: 0 });
  assert.equal(explosions.length, 1);
  assert.equal(explosions[0].snakeId, 2);
  assert.equal(explosions[0].startedAt, 4_900);
  assert.equal(seen.size, 3);
});

test('a terminal engine baseline records its final cue without replaying it', () => {
  const explosions = [];
  const seen = new Set<string>();
  const terminalState = {
    predicted_tick: 42,
    committed_tick: 42,
    tick_duration_ms: 100,
    cues: [{ tick: 42, snake_id: 0, position: { x: 0, y: 20 } }],
  };

  const baseline = syncPredictedCrashExplosions(
    explosions,
    seen,
    '42',
    terminalState,
    10_000,
    terminalState.committed_tick,
  );
  const repeated = syncPredictedCrashExplosions(
    explosions,
    seen,
    '42',
    terminalState,
    10_010,
  );

  assert.deepEqual(baseline, { started: 0, cancelled: 0 });
  assert.deepEqual(repeated, { started: 0, cancelled: 0 });
  assert.equal(explosions.length, 0);
  assert.equal(seen.size, 1);
});

test('invalid impact data never enters the animation queue', () => {
  assert.equal(
    createCrashExplosion('42:12', 0, { x: Number.NaN, y: 3 }, 100),
    null,
  );
});
