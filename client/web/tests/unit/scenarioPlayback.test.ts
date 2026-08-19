import assert from 'node:assert/strict';
import test from 'node:test';
import {
  advanceScenarioVirtualTime,
  clampScenarioTimeScale,
  formatScenarioTimecode,
  scenarioTimingFromHighlight,
  scenarioTimeScaleAt,
  scenarioTimingFromPresentation,
  scenarioViewerDurationMs,
  scenarioViewerElapsedMs,
} from '../../utils/scenarioPlayback.ts';

const timing = scenarioTimingFromPresentation({
  default_time_scale: 1,
  segments: [
    { until_ms: 1_000, time_scale: 1 },
    { until_ms: 2_000, time_scale: 0.5 },
  ],
});

test('scenario time scales are bounded to the authored 0.1–4x contract', () => {
  assert.equal(clampScenarioTimeScale(0), 0.1);
  assert.equal(clampScenarioTimeScale(0.25), 0.25);
  assert.equal(clampScenarioTimeScale(12), 4);
  assert.equal(clampScenarioTimeScale(Number.NaN), 1);
});

test('the virtual clock integrates exactly across speed-segment boundaries', () => {
  assert.equal(scenarioTimeScaleAt(timing, 999), 1);
  assert.equal(scenarioTimeScaleAt(timing, 1_000), 0.5);
  assert.equal(scenarioTimeScaleAt(timing, 2_000), 1);

  assert.equal(advanceScenarioVirtualTime(0, 2_000, 4_000, timing), 1_500);
  assert.equal(advanceScenarioVirtualTime(900, 400, 4_000, timing), 1_150);
  assert.equal(advanceScenarioVirtualTime(1_900, 300, 4_000, timing), 2_100);
});

test('an explicit embed rate overrides authored segments and clamps at the end', () => {
  assert.equal(
    advanceScenarioVirtualTime(0, 2_000, 4_000, timing, 0.25),
    500,
  );
  assert.equal(
    advanceScenarioVirtualTime(3_900, 1_000, 4_000, timing, 4),
    4_000,
  );
});

test('source timestamps map exactly onto the authored viewer timeline', () => {
  assert.equal(scenarioViewerElapsedMs(1_000, 4_000, timing), 1_000);
  assert.equal(scenarioViewerElapsedMs(1_500, 4_000, timing), 2_000);
  assert.equal(scenarioViewerElapsedMs(2_000, 4_000, timing), 3_000);
  assert.equal(scenarioViewerDurationMs(4_000, timing), 5_000);
  assert.equal(scenarioViewerDurationMs(4_000, timing, 0.25), 16_000);
});

test('canonical PotG viewer timing lasts 12.5s and lands focus at 8s', () => {
  const canonical = scenarioTimingFromPresentation({
    default_time_scale: 1,
    segments: [
      { until_ms: 4_000, time_scale: 1 },
      { until_ms: 7_500, time_scale: 0.5 },
      { until_ms: 9_000, time_scale: 1 },
    ],
  });
  assert.equal(scenarioViewerElapsedMs(6_000, 9_000, canonical), 8_000);
  assert.equal(scenarioViewerDurationMs(9_000, canonical), 12_500);
  assert.equal(advanceScenarioVirtualTime(0, 8_000, 9_000, canonical), 6_000);
  assert.equal(advanceScenarioVirtualTime(0, 12_500, 9_000, canonical), 9_000);
});

test('highlight tick segments become clip-relative viewer milliseconds', () => {
  const highlightTiming = scenarioTimingFromHighlight({
    anchor: { properties: { tick_duration_ms: 50 } },
    window: { start_tick: 100 },
    presentation: {
      segments: [
        { until_tick: 105, time_scale: 0.25 },
        { until_tick: 112, time_scale: 2 },
      ],
    },
  } as Parameters<typeof scenarioTimingFromHighlight>[0]);

  assert.deepEqual(highlightTiming, {
    defaultTimeScale: 1,
    segments: [
      { until_ms: 250, time_scale: 0.25 },
      { until_ms: 600, time_scale: 2 },
    ],
  });
});

test('broadcast timecodes retain centisecond precision without rounding ahead', () => {
  assert.equal(formatScenarioTimecode(0), '00:00:00');
  assert.equal(formatScenarioTimecode(61_239), '01:01:23');
  assert.equal(formatScenarioTimecode(-10), '00:00:00');
});
