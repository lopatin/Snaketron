import type { ArenaRotation, Position } from '../types';

export const DEFAULT_SCORE_EFFECT_ID = 'goal-impact-wave';
export const SCORE_WAVE_DURATION_MS = 900;
export const REDUCED_MOTION_SCORE_WAVE_DURATION_MS = 160;
export const MAX_ACTIVE_SCORE_EFFECTS = 6;

const CANVAS_PADDING_PX = 1;
const MAX_WAVE_RADIUS_CELLS = 12;
const MIN_WAVE_RADIUS_CELLS = 5;
const WAVE_RADIUS_ARENA_FRACTION = 0.2;
const WAVE_THICKNESS_CELLS = 1.35;
const MIN_VISIBLE_OPACITY = 0.012;

export interface ScoreEffectActivation {
  eventId: string;
  effectId: string;
  teamId: number;
  previousScore: number;
  score: number;
  tick: number;
  origin: Position;
  startedAtMs: number;
}

export interface ScoreEffectRuntime {
  gameId: string | null;
  engineEpoch: number | null;
  lastTick: number | null;
  scores: Record<number, number>;
  active: ScoreEffectActivation[];
}

export interface ScoreEffectObservation {
  gameId: string;
  engineEpoch: number;
  tick: number;
  teamScores: Record<number, number> | null;
  arenaWidth: number;
  arenaHeight: number;
  endZoneDepth: number | null;
  nowMs: number;
  effectId?: string;
}

export interface ScoreEffectSyncResult {
  started: number;
  reset: boolean;
}

export interface ScoreEffectFrame {
  nowMs: number;
  cellSize: number;
  arenaWidth: number;
  arenaHeight: number;
  rotation: ArenaRotation;
  localTeamId: number | null;
  reducedMotion: boolean;
}

export interface ScoreWaveCell {
  position: Position;
  opacity: number;
}

/**
 * A renderer is intentionally smaller than the lifecycle that invokes it.
 * Future score treatments can be registered without changing score detection,
 * resync cancellation, rotation, or the bounded active-effect queue.
 */
export interface ScoreEffectRenderer {
  readonly id: string;
  readonly durationMs: number;
  readonly reducedMotionDurationMs: number;
  draw(
    context: CanvasRenderingContext2D,
    activation: ScoreEffectActivation,
    frame: ScoreEffectFrame,
  ): void;
}

export interface ScoreEffectRegistry {
  register(renderer: ScoreEffectRenderer): void;
  resolve(id: string): ScoreEffectRenderer | undefined;
}

export const createScoreEffectRegistry = (
  initialRenderers: readonly ScoreEffectRenderer[] = [],
): ScoreEffectRegistry => {
  const renderers = new Map<string, ScoreEffectRenderer>();
  for (const renderer of initialRenderers) {
    renderers.set(renderer.id, renderer);
  }

  return {
    register(renderer) {
      renderers.set(renderer.id, renderer);
    },
    resolve(id) {
      return renderers.get(id);
    },
  };
};

const clamp = (value: number, min: number, max: number): number =>
  Math.min(max, Math.max(min, value));

export const smoothstep = (
  edge0: number,
  edge1: number,
  value: number,
): number => {
  if (edge0 === edge1) {
    return value < edge0 ? 0 : 1;
  }

  const t = clamp((value - edge0) / (edge1 - edge0), 0, 1);
  return t * t * (3 - 2 * t);
};

/** Mirrors client/src/render.rs for every supported arena orientation. */
export const transformScoreEffectPosition = (
  position: Position,
  arenaWidth: number,
  arenaHeight: number,
  rotation: ArenaRotation,
): Position => {
  switch (rotation) {
    case 90:
      return { x: arenaHeight - position.y - 1, y: position.x };
    case 180:
      return {
        x: arenaWidth - position.x - 1,
        y: arenaHeight - position.y - 1,
      };
    case 270:
      return { x: position.y, y: arenaWidth - position.x - 1 };
    default:
      return position;
  }
};

/** The same home-goal boundary and vertical center used by Arena::goal_bounds. */
export const getScoringGoalOrigin = (
  teamId: number,
  arenaWidth: number,
  arenaHeight: number,
  endZoneDepth: number | null,
): Position | null => {
  if (
    (teamId !== 0 && teamId !== 1) ||
    !Number.isInteger(arenaWidth) ||
    !Number.isInteger(arenaHeight) ||
    !Number.isInteger(endZoneDepth) ||
    arenaWidth <= 0 ||
    arenaHeight <= 0 ||
    endZoneDepth === null ||
    endZoneDepth <= 0 ||
    endZoneDepth * 2 >= arenaWidth
  ) {
    return null;
  }

  return {
    x: teamId === 0 ? endZoneDepth - 1 : arenaWidth - endZoneDepth,
    y: Math.floor(arenaHeight / 2),
  };
};

const normalizeScores = (
  teamScores: Record<number, number> | null,
): Record<number, number> => {
  const normalized: Record<number, number> = {};
  if (!teamScores) {
    return normalized;
  }

  for (const [rawTeamId, rawScore] of Object.entries(teamScores)) {
    const teamId = Number(rawTeamId);
    if (
      Number.isInteger(teamId) &&
      teamId >= 0 &&
      Number.isFinite(rawScore) &&
      rawScore >= 0
    ) {
      normalized[teamId] = Math.floor(rawScore);
    }
  }
  return normalized;
};

export const createScoreEffectRuntime = (): ScoreEffectRuntime => ({
  gameId: null,
  engineEpoch: null,
  lastTick: null,
  scores: {},
  active: [],
});

export const resetScoreEffects = (
  runtime: ScoreEffectRuntime,
  baseline?: Pick<
    ScoreEffectObservation,
    'gameId' | 'engineEpoch' | 'tick' | 'teamScores'
  >,
): void => {
  runtime.active.length = 0;
  runtime.gameId = baseline?.gameId ?? null;
  runtime.engineEpoch = baseline?.engineEpoch ?? null;
  runtime.lastTick = baseline?.tick ?? null;
  runtime.scores = normalizeScores(baseline?.teamScores ?? null);
};

/**
 * Observe authoritative/committed scores. The first observation after a game
 * change or engine snapshot is a baseline, never a replayed celebration.
 */
export const syncScoreEffects = (
  runtime: ScoreEffectRuntime,
  observation: ScoreEffectObservation,
): ScoreEffectSyncResult => {
  const nextScores = normalizeScores(observation.teamScores);
  const changedAnchor =
    runtime.gameId !== observation.gameId ||
    runtime.engineEpoch !== observation.engineEpoch;
  const tickRewound =
    runtime.lastTick !== null && observation.tick < runtime.lastTick;
  const scoreRegressed = Object.entries(runtime.scores).some(
    ([rawTeamId, score]) => (nextScores[Number(rawTeamId)] ?? 0) < score,
  );

  if (changedAnchor || tickRewound || scoreRegressed) {
    resetScoreEffects(runtime, observation);
    return { started: 0, reset: true };
  }

  let started = 0;
  const effectId = observation.effectId ?? DEFAULT_SCORE_EFFECT_ID;
  const orderedScores = Object.entries(nextScores).sort(
    ([left], [right]) => Number(left) - Number(right),
  );

  for (const [rawTeamId, score] of orderedScores) {
    const teamId = Number(rawTeamId);
    const previousScore = runtime.scores[teamId];
    // A key first appearing is baseline data, not proof of a score event.
    if (previousScore === undefined || score <= previousScore) {
      continue;
    }

    const origin = getScoringGoalOrigin(
      teamId,
      observation.arenaWidth,
      observation.arenaHeight,
      observation.endZoneDepth,
    );
    if (!origin || !Number.isFinite(observation.nowMs)) {
      continue;
    }

    runtime.active.push({
      eventId: `${observation.gameId}:${observation.engineEpoch}:${observation.tick}:${teamId}:${score}`,
      effectId,
      teamId,
      previousScore,
      score,
      tick: observation.tick,
      origin,
      startedAtMs: observation.nowMs,
    });
    started += 1;
  }

  if (runtime.active.length > MAX_ACTIVE_SCORE_EFFECTS) {
    runtime.active.splice(
      0,
      runtime.active.length - MAX_ACTIVE_SCORE_EFFECTS,
    );
  }
  runtime.gameId = observation.gameId;
  runtime.engineEpoch = observation.engineEpoch;
  runtime.lastTick = observation.tick;
  runtime.scores = nextScores;
  return { started, reset: false };
};

const scoreWaveDuration = (reducedMotion: boolean): number =>
  reducedMotion
    ? REDUCED_MOTION_SCORE_WAVE_DURATION_MS
    : SCORE_WAVE_DURATION_MS;

/** Pure timestamp-driven sampler used by both the canvas renderer and tests. */
export const sampleScoreWaveCells = (
  activation: ScoreEffectActivation,
  frame: Pick<
    ScoreEffectFrame,
    | 'nowMs'
    | 'arenaWidth'
    | 'arenaHeight'
    | 'rotation'
    | 'reducedMotion'
  >,
): ScoreWaveCell[] => {
  const durationMs = scoreWaveDuration(frame.reducedMotion);
  const elapsedMs = frame.nowMs - activation.startedAtMs;
  if (
    !Number.isFinite(elapsedMs) ||
    elapsedMs < 0 ||
    elapsedMs >= durationMs ||
    frame.arenaWidth <= 0 ||
    frame.arenaHeight <= 0
  ) {
    return [];
  }

  const progress = clamp(elapsedMs / durationMs, 0, 1);
  const maxRadius = clamp(
    Math.hypot(frame.arenaWidth, frame.arenaHeight) *
      WAVE_RADIUS_ARENA_FRACTION,
    MIN_WAVE_RADIUS_CELLS,
    MAX_WAVE_RADIUS_CELLS,
  );
  const radius = frame.reducedMotion
    ? 0
    : maxRadius * smoothstep(0, 0.86, progress);
  const reach = frame.reducedMotion
    ? 2
    : Math.min(maxRadius + WAVE_THICKNESS_CELLS * 2.5, radius + 4.5);
  const fade = frame.reducedMotion
    ? 1 - smoothstep(0.2, 1, progress)
    : 1 - smoothstep(0.62, 1, progress);
  const coreFade = 1 - smoothstep(0.05, 0.38, progress);
  const minX = Math.max(0, Math.floor(activation.origin.x - reach));
  const maxX = Math.min(
    frame.arenaWidth - 1,
    Math.ceil(activation.origin.x + reach),
  );
  const minY = Math.max(0, Math.floor(activation.origin.y - reach));
  const maxY = Math.min(
    frame.arenaHeight - 1,
    Math.ceil(activation.origin.y + reach),
  );
  const cells: ScoreWaveCell[] = [];

  for (let x = minX; x <= maxX; x += 1) {
    for (let y = minY; y <= maxY; y += 1) {
      const distance = Math.hypot(x - activation.origin.x, y - activation.origin.y);
      if (distance > reach) {
        continue;
      }
      let opacity: number;

      if (frame.reducedMotion) {
        // A brief, stationary goal wash communicates the score without travel.
        opacity = Math.exp(-0.5 * Math.pow(distance / 1.1, 2)) * fade * 0.24;
      } else {
        const ringOffset = (distance - radius) / WAVE_THICKNESS_CELLS;
        const ring = Math.exp(-0.5 * ringOffset * ringOffset);
        const core = Math.exp(-0.5 * Math.pow(distance / 1.25, 2));
        opacity = (ring * 0.27 * fade) + (core * 0.16 * coreFade);
      }

      if (opacity < MIN_VISIBLE_OPACITY) {
        continue;
      }
      cells.push({
        position: transformScoreEffectPosition(
          { x, y },
          frame.arenaWidth,
          frame.arenaHeight,
          frame.rotation,
        ),
        opacity: clamp(opacity, 0, 0.34),
      });
    }
  }

  return cells;
};

export const getScoreEffectTeamColor = (
  teamId: number,
  localTeamId: number | null,
): string => {
  const isBlue = localTeamId === null ? teamId === 0 : teamId === localTeamId;
  return isBlue ? '#5299bb' : '#d45454';
};

export const goalImpactWaveRenderer: ScoreEffectRenderer = {
  id: DEFAULT_SCORE_EFFECT_ID,
  durationMs: SCORE_WAVE_DURATION_MS,
  reducedMotionDurationMs: REDUCED_MOTION_SCORE_WAVE_DURATION_MS,
  draw(context, activation, frame) {
    const cells = sampleScoreWaveCells(activation, frame);
    if (cells.length === 0 || frame.cellSize <= 0) {
      return;
    }

    const inset = clamp(frame.cellSize * 0.08, 0.5, 1.25);
    const drawSize = Math.max(0, frame.cellSize - inset * 2);
    context.save();
    context.fillStyle = getScoreEffectTeamColor(
      activation.teamId,
      frame.localTeamId,
    );
    for (const cell of cells) {
      context.globalAlpha = cell.opacity;
      context.fillRect(
        CANVAS_PADDING_PX + cell.position.x * frame.cellSize + inset,
        CANVAS_PADDING_PX + cell.position.y * frame.cellSize + inset,
        drawSize,
        drawSize,
      );
    }
    context.restore();
  },
};

export const defaultScoreEffectRegistry = createScoreEffectRegistry([
  goalImpactWaveRenderer,
]);

export const pruneExpiredScoreEffects = (
  runtime: ScoreEffectRuntime,
  nowMs: number,
  reducedMotion: boolean,
  registry: ScoreEffectRegistry = defaultScoreEffectRegistry,
): number => {
  let removed = 0;
  for (let index = runtime.active.length - 1; index >= 0; index -= 1) {
    const activation = runtime.active[index];
    const renderer = registry.resolve(activation.effectId);
    const duration = reducedMotion
      ? renderer?.reducedMotionDurationMs
      : renderer?.durationMs;
    const elapsed = nowMs - activation.startedAtMs;
    if (
      !renderer ||
      !Number.isFinite(elapsed) ||
      elapsed < 0 ||
      duration === undefined ||
      elapsed >= duration
    ) {
      runtime.active.splice(index, 1);
      removed += 1;
    }
  }
  return removed;
};

export const drawScoreEffects = (
  context: CanvasRenderingContext2D,
  runtime: ScoreEffectRuntime,
  frame: ScoreEffectFrame,
  registry: ScoreEffectRegistry = defaultScoreEffectRegistry,
): void => {
  pruneExpiredScoreEffects(
    runtime,
    frame.nowMs,
    frame.reducedMotion,
    registry,
  );
  for (const activation of runtime.active) {
    registry.resolve(activation.effectId)?.draw(context, activation, frame);
  }
};
