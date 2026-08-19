import assert from 'node:assert/strict';
import test from 'node:test';
import type { HighlightClip } from '../../types/generated/HighlightClip.ts';
import {
  canAutoplayHighlight,
  formatHighlightReason,
  highlightFocusViewerMs,
  isCompatibleHighlightClip,
} from '../../utils/highlightPresentation.ts';

const eligibleAutoplayGate = {
  playerReady: true,
  ratingSettled: true,
  substantiallyVisible: true,
  documentVisible: true,
  motionAllowed: true,
  adState: 'idle' as const,
};

const clipFixture = (): HighlightClip => ({
  clip_format_version: 1,
  gameplay_version: 11,
  game_id: 7,
  star_user_id: 11,
  star_snake_id: 0,
  star_name: 'Vector',
  reason: { BoostedCutoff: { kills: 2 } },
  score: 180,
  breakdown: {
    total: 180,
    focus_tick: 160,
    kills: 2,
    boosted_cutoff_kills: 2,
    trap_kills: 0,
    banked_points: 0,
    max_chain: 0,
    pickups: 0,
    demolition_points: 180,
    banking_points: 0,
    combo_points: 0,
  },
  window: { start_tick: 100, end_tick: 190, focus_tick: 160 },
  anchor: {
    tick: 100,
    properties: { tick_duration_ms: 100 },
  } as HighlightClip['anchor'],
  messages: [],
  end_sync_hash: '1',
  presentation: {
    rotation: 90,
    follow_snake_id: 0,
    segments: [
      { until_tick: 140, time_scale: 1 },
      { until_tick: 175, time_scale: 0.5 },
      { until_tick: 190, time_scale: 1 },
    ],
  },
  config: {} as HighlightClip['config'],
});

test('highlight compatibility rejects deploy skew and malformed windows', () => {
  const clip = clipFixture();
  assert.equal(isCompatibleHighlightClip(clip), true);
  assert.equal(isCompatibleHighlightClip({ ...clip, gameplay_version: 10 }), false);
  assert.equal(isCompatibleHighlightClip({
    ...clip,
    window: { ...clip.window, focus_tick: 191 },
  }), false);
});

test('highlight captions are human and plural-aware', () => {
  assert.equal(
    formatHighlightReason({ BoostedCutoff: { kills: 2 } }),
    'Boosted cut-off — 2 eliminations',
  );
  assert.equal(
    formatHighlightReason({ GoalRun: { points: 1 } }),
    'Goal run — 1 point',
  );
  assert.equal(
    formatHighlightReason({ ComboFrenzy: { max_chain: 6 } }),
    'Combo frenzy — 6× chain',
  );
});

test('canonical speed ramp puts the focus payoff eight seconds into playback', () => {
  assert.equal(highlightFocusViewerMs(clipFixture()), 8_000);
});

test('highlight autoplay waits for rating, viewport, foreground, motion, and ads', () => {
  assert.equal(canAutoplayHighlight(eligibleAutoplayGate), true);
  for (const blocked of [
    { playerReady: false },
    { ratingSettled: false },
    { substantiallyVisible: false },
    { documentVisible: false },
    { motionAllowed: false },
    { adState: 'requesting' as const },
    { adState: 'playing' as const },
  ]) {
    assert.equal(
      canAutoplayHighlight({ ...eligibleAutoplayGate, ...blocked }),
      false,
    );
  }
});
