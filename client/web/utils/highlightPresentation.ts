import { GAMEPLAY_PROTOCOL_VERSION } from '../constants.ts';
import type { CrazyGamesAdState } from '../services/crazyGames.ts';
import type { HighlightClip, HighlightReason } from '../types/generated';

export const HIGHLIGHT_CLIP_FORMAT_VERSION = 1;
export const HIGHLIGHT_POLL_INTERVAL_MS = 900;
// Replay persistence includes an object-store write before the highlight is
// queryable. Allow roughly 30 seconds so a healthy write is not mistaken for
// an unavailable highlight after only a few seconds.
export const HIGHLIGHT_POLL_MAX_ATTEMPTS = 34;

export type MatchHighlightState =
  | { phase: 'idle' }
  | { phase: 'pending' }
  | { phase: 'ready'; clip: HighlightClip }
  | { phase: 'unavailable'; reason: 'absent' | 'incompatible' | 'network' };

export interface HighlightAutoplayGate {
  playerReady: boolean;
  ratingSettled: boolean;
  substantiallyVisible: boolean;
  documentVisible: boolean;
  motionAllowed: boolean;
  adState: CrazyGamesAdState;
}

/** Every prerequisite must be true at the instant autoplay begins. Keeping
 * this policy pure makes it hard for loading, scrolling, or ad races to burn
 * the only automatic showing while the player cannot actually see it. */
export const canAutoplayHighlight = (gate: HighlightAutoplayGate): boolean => (
  gate.playerReady &&
  gate.ratingSettled &&
  gate.substantiallyVisible &&
  gate.documentVisible &&
  gate.motionAllowed &&
  gate.adState === 'idle'
);

const finiteInteger = (value: unknown): value is number => (
  typeof value === 'number' && Number.isFinite(value) && Number.isInteger(value)
);

/** Client-side compatibility and structural gate before a clip crosses into
 * WASM. The WASM boundary repeats the full validation and end-hash assertion. */
export const isCompatibleHighlightClip = (value: unknown): value is HighlightClip => {
  if (!value || typeof value !== 'object') {
    return false;
  }
  const clip = value as Partial<HighlightClip>;
  if (
    clip.clip_format_version !== HIGHLIGHT_CLIP_FORMAT_VERSION ||
    clip.gameplay_version !== GAMEPLAY_PROTOCOL_VERSION ||
    !finiteInteger(clip.game_id) ||
    !finiteInteger(clip.star_user_id) ||
    !finiteInteger(clip.star_snake_id) ||
    typeof clip.star_name !== 'string' ||
    clip.star_name.trim() === '' ||
    typeof clip.end_sync_hash !== 'string' ||
    !/^\d+$/.test(clip.end_sync_hash) ||
    !clip.window ||
    !clip.anchor ||
    !Array.isArray(clip.messages) ||
    !clip.presentation
  ) {
    return false;
  }
  const { start_tick: start, end_tick: end, focus_tick: focus } = clip.window;
  return (
    finiteInteger(start) &&
    finiteInteger(end) &&
    finiteInteger(focus) &&
    start < end &&
    focus >= start &&
    focus <= end &&
    clip.anchor.tick <= start
  );
};

const countLabel = (count: number, singular: string, plural = `${singular}s`): string => (
  `${count} ${count === 1 ? singular : plural}`
);

export const formatHighlightReason = (reason: HighlightReason): string => {
  if ('BoostedCutoff' in reason) {
    return `Boosted cut-off — ${countLabel(reason.BoostedCutoff.kills, 'elimination')}`;
  }
  if ('TrapKill' in reason) {
    return `Perfect trap — ${countLabel(reason.TrapKill.kills, 'elimination')}`;
  }
  if ('Demolition' in reason) {
    return `Demolition — ${countLabel(reason.Demolition.kills, 'elimination')}`;
  }
  if ('GoalRun' in reason) {
    return `Goal run — ${countLabel(reason.GoalRun.points, 'point')}`;
  }
  if ('ComboFrenzy' in reason) {
    return `Combo frenzy — ${reason.ComboFrenzy.max_chain}× chain`;
  }
  return `Feeding frenzy — ${countLabel(reason.FeedingFrenzy.pickups, 'pickup')}`;
};

/** Viewer-time position of the focus point after integrating server-authored
 * speed segments. Used by QA to pin the payoff pacing contract. */
export const highlightFocusViewerMs = (clip: HighlightClip): number => {
  const tickMs = Math.max(1, clip.anchor.properties.tick_duration_ms);
  const focus = clip.window.focus_tick;
  let cursor = clip.window.start_tick;
  let viewerMs = 0;
  for (const segment of clip.presentation.segments) {
    const end = Math.min(focus, Math.max(cursor, segment.until_tick));
    if (end > cursor) {
      viewerMs += ((end - cursor) * tickMs) / Math.max(0.1, segment.time_scale);
      cursor = end;
    }
    if (cursor >= focus) break;
  }
  if (cursor < focus) {
    viewerMs += (focus - cursor) * tickMs;
  }
  return viewerMs;
};
