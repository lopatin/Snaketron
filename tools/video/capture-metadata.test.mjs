import assert from 'node:assert/strict';
import test from 'node:test';
import {
  buildStarHeadFrames,
  CAPTURE_BROWSER_ARGS,
  FALLBACK_SCREENSHOT_OPTIONS,
} from './capture-metadata.mjs';

test('fallback screenshots preserve CSS animation time', () => {
  assert.deepEqual(FALLBACK_SCREENSHOT_OPTIONS, { type: 'png' });
  assert.equal('animations' in FALLBACK_SCREENSHOT_OPTIONS, false);
});

test('capture pins platform-neutral canvas font rasterization', () => {
  assert.ok(CAPTURE_BROWSER_ARGS.includes('--disable-font-subpixel-positioning'));
  assert.ok(CAPTURE_BROWSER_ARGS.includes('--disable-lcd-text'));
  assert.ok(CAPTURE_BROWSER_ARGS.includes('--font-render-hinting=none'));
  assert.ok(CAPTURE_BROWSER_ARGS.includes('--force-device-scale-factor=1'));
});

const position = (value) => ({ x: value, y: value });
const headFrame = (tick, head, isAlive) => ({
  tick,
  snakes: [{ snake_id: 0, head, is_alive: isAlive }],
});

const cueTrack = {
  tick_duration_ms: 100,
  start_tick: 0,
  end_tick: 6,
  heads: [
    headFrame(0, position(0), true),
    headFrame(1, null, false),
    headFrame(2, position(2), true),
    headFrame(3, position(3), true),
    headFrame(4, position(4), true),
    headFrame(5, null, false),
    headFrame(6, position(6), true),
  ],
  deaths: [
    { tick: 1, sequence: 1, snake_id: 0, hold_position: position(10) },
    { tick: 5, sequence: 2, snake_id: 0, hold_position: position(50) },
  ],
};

test('highlight head metadata releases a pre-focus death and holds the payoff death', () => {
  const frames = buildStarHeadFrames(cueTrack, 0, 6, 100, 600, 4);

  assert.deepEqual(
    frames.map(({ tick, head, is_alive: isAlive }) => ({ tick, head, isAlive })),
    [
      { tick: 1, head: null, isAlive: false },
      { tick: 2, head: position(2), isAlive: true },
      { tick: 3, head: position(3), isAlive: true },
      { tick: 4, head: position(4), isAlive: true },
      { tick: 5, head: position(50), isAlive: false },
      { tick: 6, head: position(50), isAlive: false },
    ],
  );
});

test('scenario head metadata preserves the no-focus final-death hold', () => {
  const frames = buildStarHeadFrames(cueTrack, 0, 3, 100, 600);

  assert.deepEqual(frames[1], {
    frame: 1,
    master_seconds: 0.2,
    virtual_ms: 200,
    tick: 2,
    head: position(10),
    is_alive: false,
  });
});

test('viewer-timed head metadata uses the supplied source-time projection', () => {
  const frames = buildStarHeadFrames(
    cueTrack,
    0,
    3,
    100,
    600,
    4,
    [100, 300, 600],
  );

  assert.deepEqual(
    frames.map(({ master_seconds: masterSeconds, virtual_ms: virtualMs, tick }) => ({
      masterSeconds,
      virtualMs,
      tick,
    })),
    [
      { masterSeconds: 0.1, virtualMs: 100, tick: 1 },
      { masterSeconds: 0.2, virtualMs: 300, tick: 3 },
      { masterSeconds: 0.3, virtualMs: 600, tick: 6 },
    ],
  );
});
