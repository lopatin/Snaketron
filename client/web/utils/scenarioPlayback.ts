import type {
  HighlightClip,
  ScenarioPlaybackSegment,
  ScenarioPresentation,
} from '../types';

export const MIN_SCENARIO_TIME_SCALE = 0.1;
export const MAX_SCENARIO_TIME_SCALE = 4;

export interface ScenarioTiming {
  defaultTimeScale: number;
  segments: readonly ScenarioPlaybackSegment[];
}

const finiteNonNegative = (value: number): number => (
  Number.isFinite(value) ? Math.max(0, value) : 0
);

export const clampScenarioTimeScale = (value: number, fallback = 1): number => {
  const candidate = Number.isFinite(value) ? value : fallback;
  return Math.min(
    MAX_SCENARIO_TIME_SCALE,
    Math.max(MIN_SCENARIO_TIME_SCALE, candidate),
  );
};

export const scenarioTimingFromPresentation = (
  presentation: Partial<ScenarioPresentation> | null | undefined,
): ScenarioTiming => ({
  defaultTimeScale: clampScenarioTimeScale(
    presentation?.default_time_scale ?? 1,
  ),
  segments: [...(presentation?.segments ?? [])]
    .filter((segment) => Number.isFinite(segment.until_ms) && segment.until_ms > 0)
    .sort((left, right) => left.until_ms - right.until_ms)
    .map((segment) => ({
      until_ms: finiteNonNegative(segment.until_ms),
      time_scale: clampScenarioTimeScale(segment.time_scale),
    })),
});

export const scenarioTimingFromHighlight = (
  clip: Pick<HighlightClip, 'anchor' | 'presentation' | 'window'>,
): ScenarioTiming => {
  const tickDurationMs = Math.max(
    1,
    finiteNonNegative(clip.anchor.properties.tick_duration_ms),
  );
  const startTick = finiteNonNegative(clip.window.start_tick);
  return scenarioTimingFromPresentation({
    default_time_scale: 1,
    segments: clip.presentation.segments.map((segment) => ({
      until_ms: Math.max(0, segment.until_tick - startTick) * tickDurationMs,
      time_scale: segment.time_scale,
    })),
  });
};

export const scenarioTimeScaleAt = (
  timing: ScenarioTiming,
  elapsedMs: number,
  override?: number,
): number => {
  if (override !== undefined) {
    return clampScenarioTimeScale(override);
  }

  const boundedElapsed = finiteNonNegative(elapsedMs);
  const segment = timing.segments.find(({ until_ms: untilMs }) => (
    boundedElapsed < untilMs
  ));
  return segment
    ? clampScenarioTimeScale(segment.time_scale)
    : clampScenarioTimeScale(timing.defaultTimeScale);
};

/**
 * Advance a virtual gameplay clock by viewer time. Segment boundaries are
 * integrated exactly instead of applying whichever rate happened to be active
 * at the start of a browser frame.
 */
export const advanceScenarioVirtualTime = (
  elapsedMs: number,
  viewerDeltaMs: number,
  durationMs: number,
  timing: ScenarioTiming,
  override?: number,
): number => {
  const duration = finiteNonNegative(durationMs);
  let elapsed = Math.min(duration, finiteNonNegative(elapsedMs));
  let remainingViewerMs = finiteNonNegative(viewerDeltaMs);

  if (elapsed >= duration || remainingViewerMs <= 0) {
    return elapsed;
  }

  if (override !== undefined) {
    return Math.min(
      duration,
      elapsed + remainingViewerMs * clampScenarioTimeScale(override),
    );
  }

  while (remainingViewerMs > 0 && elapsed < duration) {
    const scale = scenarioTimeScaleAt(timing, elapsed);
    const nextBoundary = timing.segments
      .map(({ until_ms: untilMs }) => untilMs)
      .find((untilMs) => untilMs > elapsed);
    const virtualBoundary = Math.min(duration, nextBoundary ?? duration);
    const virtualDistance = virtualBoundary - elapsed;
    const viewerDistance = virtualDistance / scale;

    if (remainingViewerMs < viewerDistance || virtualDistance <= 0) {
      elapsed = Math.min(duration, elapsed + remainingViewerMs * scale);
      break;
    }

    elapsed = virtualBoundary;
    remainingViewerMs -= viewerDistance;
  }

  return elapsed;
};

/**
 * Convert a source/gameplay timestamp to elapsed viewer time under the same
 * piecewise speed map used by playback. This is the exact inverse timebase
 * needed by product-view capture metadata and duration calculations.
 */
export const scenarioViewerElapsedMs = (
  sourceElapsedMs: number,
  durationMs: number,
  timing: ScenarioTiming,
  override?: number,
): number => {
  const duration = finiteNonNegative(durationMs);
  const target = Math.min(duration, finiteNonNegative(sourceElapsedMs));
  if (target <= 0) {
    return 0;
  }
  if (override !== undefined) {
    return target / clampScenarioTimeScale(override);
  }

  let sourceElapsed = 0;
  let viewerElapsed = 0;
  while (sourceElapsed < target) {
    const scale = scenarioTimeScaleAt(timing, sourceElapsed);
    const nextBoundary = timing.segments
      .map(({ until_ms: untilMs }) => untilMs)
      .find((untilMs) => untilMs > sourceElapsed);
    const sourceBoundary = Math.min(target, nextBoundary ?? target);
    const sourceDistance = sourceBoundary - sourceElapsed;
    if (sourceDistance <= 0) {
      break;
    }
    viewerElapsed += sourceDistance / scale;
    sourceElapsed = sourceBoundary;
  }
  return viewerElapsed;
};

export const scenarioViewerDurationMs = (
  durationMs: number,
  timing: ScenarioTiming,
  override?: number,
): number => scenarioViewerElapsedMs(durationMs, durationMs, timing, override);

export const formatScenarioTimecode = (milliseconds: number): string => {
  const bounded = finiteNonNegative(milliseconds);
  const totalCentiseconds = Math.floor(bounded / 10);
  const minutes = Math.floor(totalCentiseconds / 6000);
  const seconds = Math.floor(totalCentiseconds / 100) % 60;
  const centiseconds = totalCentiseconds % 100;
  return `${minutes.toString().padStart(2, '0')}:${seconds
    .toString()
    .padStart(2, '0')}:${centiseconds.toString().padStart(2, '0')}`;
};
