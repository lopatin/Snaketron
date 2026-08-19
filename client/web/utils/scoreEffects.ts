import type { ArenaRotation, Position } from '../types';

export const DEFAULT_SCORE_EFFECT_ID = 'goal-impact-wave';
/**
 * The full celebration: an impact wave that resolves early and a floating
 * points readout that outlives it. Cue retention in `GameState`
 * (`RECENT_GOAL_RETENTION_MS`) is deliberately wider than this, so a cue
 * expiring never looks like a rollback retraction.
 */
export const SCORE_CELEBRATION_DURATION_MS = 1200;
export const REDUCED_MOTION_SCORE_CELEBRATION_DURATION_MS = 600;
/**
 * Mirrors `RECENT_GOAL_RETENTION_MS` in common/src/game_state.rs, less a margin
 * for prediction lead and clock skew. Beyond this age a cue has legitimately
 * aged out of the engine's history, so its absence no longer means "rolled
 * back" and must not retract a celebration that is still on screen. This keeps
 * the two durations independent: raising the celebration past the retention
 * window degrades to "not retractable near the end", never to a visible pop.
 */
export const GOAL_CUE_RETRACTION_WINDOW_MS = 1500;
export const SCORE_WAVE_DURATION_MS = 900;
export const REDUCED_MOTION_SCORE_WAVE_DURATION_MS = 220;
export const MAX_ACTIVE_SCORE_EFFECTS = 6;

const CANVAS_PADDING_PX = 1;
const MAX_WAVE_RADIUS_CELLS = 20;
const MIN_WAVE_RADIUS_CELLS = 4;
const WAVE_RADIUS_ARENA_FRACTION = 0.155;
const WAVE_THICKNESS_CELLS = 1.35;
const MIN_VISIBLE_OPACITY = 0.012;

const READOUT_RISE_CELLS = 3.4;
const READOUT_MIN_FONT_PX = 14;
const READOUT_MAX_FONT_PX = 62;
const READOUT_FONT_CELL_FRACTION = 2.4;
const READOUT_INITIAL_SCALE = 0.55;
const READOUT_POP_GAIN = 0.62;
const READOUT_SETTLE = 0.17;
const READOUT_PEAK_SCALE = READOUT_INITIAL_SCALE + READOUT_POP_GAIN;
// The score sits beside the scoring snake rather than on top of it. Half a
// cell clears the snake itself; the remaining gap accommodates the italic
// face's overhang and keeps the two silhouettes visually distinct.
const READOUT_SNAKE_RADIUS_CELLS = 0.5;
const READOUT_SNAKE_GAP_CELLS = 0.35;
// The head has already entered the scoring cell when its cue is recorded, so
// its guaranteed neck is one field-facing cell away. Clearing that cell along
// the entry axis also clears a perpendicular body run that turns at the neck.
const READOUT_CROSSING_NECK_CELLS = 1;
// Mirrors the active snake's outer Boost layer (`BOOST_OUTER_EXTRA / 2`).
const READOUT_BOOST_OUTLINE_PX = 3;
// A 2v2 goal of width nine respawns its two snakes one row either side of
// centre. Reserve that entire lane band along the goal mouth in every view.
const READOUT_2V2_RESPAWN_LANE_SPREAD_CELLS = 1;

/**
 * Points at which a goal renders at its nominal size. Goals worth less are
 * scaled down from here, goals worth more scaled up.
 */
const SCORE_MAGNITUDE_REFERENCE_POINTS = 4;
export const MIN_SCORE_MAGNITUDE = 0.6;
/** Saturates just above a twelve-point bank, so the biggest goals a player
 *  realistically lands still read as distinct from a merely large one. */
export const MAX_SCORE_MAGNITUDE = 1.7;
/** Conservative advance-per-character for `Impact`-class faces, used to keep
 *  the readout inside the canvas without measuring text in the pure sampler. */
const READOUT_GLYPH_ADVANCE_EM = 0.62;

export interface ScoreEffectActivation {
  eventId: string;
  effectId: string;
  teamId: number;
  snakeId: number;
  points: number;
  tick: number;
  origin: Position;
  startedAtMs: number;
  /**
   * The scorer's celebration dressing, captured when the cue was seen. Held on
   * the activation rather than looked up while drawing so a celebration in
   * flight keeps the colours it started with.
   */
  celebration: CelebrationTheme;
}

/**
 * How a goal celebration looks, supplied by the *scoring* player's skin.
 *
 * Celebration is the surface everyone watching sees, so it is attributed to
 * whoever earned it. `effect` names a first-party renderer — a skin chooses
 * which effect plays and what colour it is, never what code runs.
 */
export interface CelebrationTheme {
  effect: string;
  friendly_accent: string;
  enemy_accent: string;
  readout_friendly: string;
  readout_enemy: string;
}

/** The dressing every client can always draw. */
export const CLASSIC_CELEBRATION_THEME: CelebrationTheme = {
  effect: DEFAULT_SCORE_EFFECT_ID,
  friendly_accent: '#5299bb',
  enemy_accent: '#d45454',
  readout_friendly: '#2b6f8c',
  readout_enemy: '#a83232',
};

export interface ScoreEffectRuntime {
  active: ScoreEffectActivation[];
  seenEventIds: Set<string>;
}

/** One predicted goal, mirroring `common::game_state::TeamGoal`. */
export interface PredictedGoalCue {
  tick: number;
  team_id: number;
  snake_id: number;
  position: Position;
  points: number;
  /** Resolved from the scorer's skin by the Rust side. */
  celebration?: CelebrationTheme;
}

export interface PredictedScoreVisualState {
  predicted_tick: number;
  committed_tick: number;
  tick_duration_ms: number;
  goals?: readonly PredictedGoalCue[];
}

export interface ScoreEffectSyncResult {
  started: number;
  cancelled: number;
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

export interface ScoreReadoutRenderState {
  text: string;
  centerX: number;
  centerY: number;
  fontSize: number;
  scale: number;
  opacity: number;
}

/**
 * A renderer is intentionally smaller than the lifecycle that invokes it.
 * Future score treatments can be registered without changing cue detection,
 * rollback retraction, rotation, or the bounded active-effect queue.
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

/**
 * How large a goal renders, relative to its nominal size, given what it was
 * worth. Deliberately sub-linear: banking twelve points should look clearly
 * bigger than banking one without swamping the arena, and the clamp keeps an
 * unusually large haul from running off the end zone entirely.
 */
export const scoreEffectMagnitude = (points: number): number => {
  if (!Number.isFinite(points) || points <= 0) {
    return MIN_SCORE_MAGNITUDE;
  }

  return clamp(
    Math.sqrt(points / SCORE_MAGNITUDE_REFERENCE_POINTS),
    MIN_SCORE_MAGNITUDE,
    MAX_SCORE_MAGNITUDE,
  );
};

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

export const createScoreEffectRuntime = (): ScoreEffectRuntime => ({
  active: [],
  seenEventIds: new Set<string>(),
});

export const resetScoreEffects = (runtime: ScoreEffectRuntime): void => {
  runtime.active.length = 0;
  runtime.seenEventIds.clear();
};

const goalEventId = (gameId: string, cue: PredictedGoalCue): string =>
  `${gameId}:goal:${cue.tick}:${cue.team_id}:${cue.snake_id}:${cue.position.x}:${cue.position.y}:${cue.points}`;

/**
 * Starts celebrations from rollback-visible predicted goal history, so a goal
 * plays the moment prediction simulates it instead of waiting for committed
 * state to catch up. A prediction replay supplies the complete recent cue set,
 * so an absent identity is an actual correction — the rare case where the goal
 * never happened — and the celebration is retracted immediately.
 */
export const syncPredictedScoreEffects = (
  runtime: ScoreEffectRuntime,
  gameId: string,
  visualState: PredictedScoreVisualState,
  nowMs: number,
  suppressStartsAtOrBeforeTick?: number,
  effectId: string = DEFAULT_SCORE_EFFECT_ID,
): ScoreEffectSyncResult => {
  const currentCueIds = new Set<string>();
  let started = 0;
  let cancelled = 0;

  const tickDurationMs = Math.max(1, visualState.tick_duration_ms);
  for (const cue of visualState.goals ?? []) {
    const eventId = goalEventId(gameId, cue);
    currentCueIds.add(eventId);
    const elapsedMs = Math.max(
      0,
      (visualState.predicted_tick - cue.tick) * tickDurationMs,
    );

    // A new engine epoch begins from an authoritative snapshot. Baseline cues
    // at or before that committed tick are history, not a new visual event;
    // speculative cues produced while catching up beyond it still play.
    if (
      runtime.seenEventIds.has(eventId) ||
      elapsedMs >= SCORE_CELEBRATION_DURATION_MS ||
      cue.points <= 0 ||
      !Number.isFinite(nowMs) ||
      !Number.isFinite(cue.position.x) ||
      !Number.isFinite(cue.position.y) ||
      (suppressStartsAtOrBeforeTick !== undefined &&
        cue.tick <= suppressStartsAtOrBeforeTick)
    ) {
      runtime.seenEventIds.add(eventId);
      continue;
    }

    runtime.seenEventIds.add(eventId);
    runtime.active.push({
      eventId,
      effectId,
      teamId: cue.team_id,
      snakeId: cue.snake_id,
      points: cue.points,
      tick: cue.tick,
      origin: { ...cue.position },
      startedAtMs: nowMs - elapsedMs,
      // A cue from an older client, or one whose scorer wore an unknown skin,
      // arrives without dressing and gets the classic look.
      celebration: cue.celebration ?? CLASSIC_CELEBRATION_THEME,
    });
    started += 1;
  }

  // Retract before trimming, so a rolled-back celebration can never hold a
  // slot that a real one is then evicted from.
  for (let index = runtime.active.length - 1; index >= 0; index -= 1) {
    const activation = runtime.active[index];
    if (currentCueIds.has(activation.eventId)) {
      continue;
    }
    // Absence only proves a rollback while the engine is still retaining the
    // cue. Past that window the cue aged out on its own, and retracting here
    // would pop a celebration of a goal that really happened.
    if (nowMs - activation.startedAtMs >= GOAL_CUE_RETRACTION_WINDOW_MS) {
      continue;
    }

    runtime.active.splice(index, 1);
    runtime.seenEventIds.delete(activation.eventId);
    cancelled += 1;
  }

  // Oldest survivors lose their slot first. Their dedupe entries stay, so a
  // dropped celebration is not restarted by the next frame's cue set.
  if (runtime.active.length > MAX_ACTIVE_SCORE_EFFECTS) {
    runtime.active.splice(0, runtime.active.length - MAX_ACTIVE_SCORE_EFFECTS);
  }

  // Effects remove themselves when their time elapses. Release dedupe entries
  // once a cue also leaves prediction history so a genuine rollback-and-replay
  // can trigger the same deterministic identity again. An identity still held
  // by a running effect keeps its entry, so a cue that ages out mid-animation
  // and later reappears cannot enqueue the same celebration twice.
  const goalPrefix = `${gameId}:goal:`;
  const liveEventIds = new Set(runtime.active.map(effect => effect.eventId));
  for (const eventId of runtime.seenEventIds) {
    if (
      eventId.startsWith(goalPrefix) &&
      !currentCueIds.has(eventId) &&
      !liveEventIds.has(eventId)
    ) {
      runtime.seenEventIds.delete(eventId);
    }
  }

  return { started, cancelled };
};

const scoreWaveDuration = (reducedMotion: boolean): number =>
  reducedMotion
    ? REDUCED_MOTION_SCORE_WAVE_DURATION_MS
    : SCORE_WAVE_DURATION_MS;

const scoreCelebrationDuration = (reducedMotion: boolean): number =>
  reducedMotion
    ? REDUCED_MOTION_SCORE_CELEBRATION_DURATION_MS
    : SCORE_CELEBRATION_DURATION_MS;

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
      WAVE_RADIUS_ARENA_FRACTION *
      scoreEffectMagnitude(activation.points),
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

/**
 * The floating points readout: it pops in beside the cell where the goal was
 * scored, rises, and fades. Side-goal readouts sit field-side of the crossing;
 * top/bottom readouts sit above the boundary and away from the respawn lane.
 * The circular wave stays centered on the exact crossing cell. Sampled purely
 * from the activation timestamp so the renderer and its tests agree exactly,
 * and clamped to the canvas so a goal near an edge still shows its number.
 */
export const sampleScoreReadout = (
  activation: ScoreEffectActivation,
  frame: Pick<
    ScoreEffectFrame,
    | 'nowMs'
    | 'cellSize'
    | 'arenaWidth'
    | 'arenaHeight'
    | 'rotation'
    | 'reducedMotion'
  >,
): ScoreReadoutRenderState | null => {
  const durationMs = scoreCelebrationDuration(frame.reducedMotion);
  const elapsedMs = frame.nowMs - activation.startedAtMs;
  if (
    !Number.isFinite(elapsedMs) ||
    elapsedMs < 0 ||
    elapsedMs >= durationMs ||
    activation.points <= 0 ||
    frame.cellSize <= 0 ||
    frame.arenaWidth <= 0 ||
    frame.arenaHeight <= 0
  ) {
    return null;
  }

  // The readout snaps in at full opacity — the scale pop below carries the
  // "appear" — so a goal never loses a frame to a zero-alpha ramp-in.
  const progress = clamp(elapsedMs / durationMs, 0, 1);
  const opacity = 1 - smoothstep(0.55, 1, progress);
  if (opacity <= 0) {
    return null;
  }

  const isVertical = frame.rotation === 90 || frame.rotation === 270;
  const screenColumns = isVertical ? frame.arenaHeight : frame.arenaWidth;
  const screenRows = isVertical ? frame.arenaWidth : frame.arenaHeight;
  const transformed = transformScoreEffectPosition(
    activation.origin,
    frame.arenaWidth,
    frame.arenaHeight,
    frame.rotation,
  );

  const fontSize = clamp(
    frame.cellSize *
      READOUT_FONT_CELL_FRACTION *
      scoreEffectMagnitude(activation.points),
    READOUT_MIN_FONT_PX,
    READOUT_MAX_FONT_PX,
  );
  // The rise is deliberately linear. An ease-out would spend ~90% of its
  // travel before the fade begins at 0.55, so the number would rise and then
  // fade in place; constant drift keeps it visibly climbing as it fades out.
  // Reduced motion keeps it still instead, fading without travelling.
  const rise = frame.reducedMotion
    ? 0
    : frame.cellSize * READOUT_RISE_CELLS * progress;
  const scale = frame.reducedMotion
    ? 1
    : READOUT_INITIAL_SCALE +
      READOUT_POP_GAIN * smoothstep(0, 0.14, progress) -
      READOUT_SETTLE * smoothstep(0.14, 0.4, progress);

  const text = `+${activation.points}`;
  const peakHalfWidth =
    (fontSize * READOUT_PEAK_SCALE * READOUT_GLYPH_ADVANCE_EM * text.length) / 2;
  const peakHalfHeight = (fontSize * READOUT_PEAK_SCALE) / 2;
  const peakStrokeHalfWidth =
    (Math.max(2, fontSize * 0.18) * READOUT_PEAK_SCALE) / 2;
  const peakPaintedHalfWidth = peakHalfWidth + peakStrokeHalfWidth;
  const peakPaintedHalfHeight = peakHalfHeight + peakStrokeHalfWidth;
  const canvasWidth = CANVAS_PADDING_PX * 2 + screenColumns * frame.cellSize;
  const canvasHeight = CANVAS_PADDING_PX * 2 + screenRows * frame.cellSize;

  const waveCenterX =
    CANVAS_PADDING_PX + transformed.x * frame.cellSize + frame.cellSize / 2;
  const waveCenterY =
    CANVAS_PADDING_PX +
    transformed.y * frame.cellSize +
    frame.cellSize / 2;

  // A score immediately respawns its snake one cell inside the base, while a
  // predicted cue can still be painted over the scoring snake's old crossing
  // body. Put the number diagonally beside both: field-facing to clear the
  // goal wall, and away from the centre lanes along the goal mouth. For a
  // downward goal-mouth offset, reserve the entire future rise as well so the
  // number never drifts back through the snake on its way up.
  const goalMouthIsHorizontal = frame.rotation === 90 || frame.rotation === 270;
  const offsetDirectionX = goalMouthIsHorizontal
    ? waveCenterX < canvasWidth / 2
      ? -1
      : waveCenterX > canvasWidth / 2
        ? 1
        : activation.teamId === 0 ? -1 : 1
    : frame.rotation === 180
      ? activation.teamId === 0 ? -1 : 1
      : activation.teamId === 0 ? 1 : -1;
  const snakeClearance =
    frame.cellSize * (READOUT_SNAKE_RADIUS_CELLS + READOUT_SNAKE_GAP_CELLS) +
    READOUT_BOOST_OUTLINE_PX;
  const respawnLaneClearance =
    frame.cellSize * READOUT_2V2_RESPAWN_LANE_SPREAD_CELLS;
  const entryAxisClearance = snakeClearance +
    frame.cellSize * READOUT_CROSSING_NECK_CELLS;
  const offset = peakPaintedHalfWidth +
    (goalMouthIsHorizontal
      ? snakeClearance + respawnLaneClearance
      : entryAxisClearance);
  const horizontalGoalVerticalOffset = goalMouthIsHorizontal
    ? peakPaintedHalfHeight + entryAxisClearance
    : 0;
  const sideGoalDirectionY = waveCenterY < canvasHeight / 2
    ? -1
    : waveCenterY > canvasHeight / 2
      ? 1
      : activation.teamId === 0 ? -1 : 1;
  const maximumRise = frame.reducedMotion ? 0 : frame.cellSize * READOUT_RISE_CELLS;
  const sideGoalVerticalOffset = !goalMouthIsHorizontal
    ? peakPaintedHalfHeight + snakeClearance + respawnLaneClearance +
      (sideGoalDirectionY > 0 ? maximumRise : 0)
    : 0;
  const rawCenterX = waveCenterX + offsetDirectionX * offset;
  const rawCenterY = goalMouthIsHorizontal
    ? waveCenterY - horizontalGoalVerticalOffset - rise
    : waveCenterY + sideGoalDirectionY * sideGoalVerticalOffset - rise;

  return {
    text,
    centerX: clamp(
      rawCenterX,
      Math.min(peakPaintedHalfWidth, canvasWidth / 2),
      Math.max(canvasWidth - peakPaintedHalfWidth, canvasWidth / 2),
    ),
    centerY: clamp(
      rawCenterY,
      Math.min(peakPaintedHalfHeight, canvasHeight / 2),
      Math.max(canvasHeight - peakPaintedHalfHeight, canvasHeight / 2),
    ),
    fontSize,
    scale,
    opacity,
  };
};

export const getScoreEffectTeamColor = (
  teamId: number,
  localTeamId: number | null,
  celebration: CelebrationTheme = CLASSIC_CELEBRATION_THEME,
): string => {
  // Whether this goal reads as "ours" is the viewer's question; what the two
  // sides look like is the scorer's skin's answer.
  const isFriendly = localTeamId === null ? teamId === 0 : teamId === localTeamId;
  return isFriendly ? celebration.friendly_accent : celebration.enemy_accent;
};

/**
 * A deeper team tone for the points readout. The wave stays restrained, but a
 * goal is always scored inside a tinted end zone, where the wave's colour is
 * only ~2.8:1 against the background — too weak for text even with a halo.
 */
export const getScoreReadoutColor = (
  teamId: number,
  localTeamId: number | null,
  celebration: CelebrationTheme = CLASSIC_CELEBRATION_THEME,
): string => {
  const isFriendly = localTeamId === null ? teamId === 0 : teamId === localTeamId;
  return isFriendly
    ? celebration.readout_friendly
    : celebration.readout_enemy;
};

export const goalImpactWaveRenderer: ScoreEffectRenderer = {
  id: DEFAULT_SCORE_EFFECT_ID,
  durationMs: SCORE_CELEBRATION_DURATION_MS,
  reducedMotionDurationMs: REDUCED_MOTION_SCORE_CELEBRATION_DURATION_MS,
  draw(context, activation, frame) {
    if (frame.cellSize <= 0) {
      return;
    }

    const teamColor = getScoreEffectTeamColor(
      activation.teamId,
      frame.localTeamId,
      activation.celebration,
    );
    const cells = sampleScoreWaveCells(activation, frame);

    if (cells.length > 0) {
      const inset = clamp(frame.cellSize * 0.08, 0.5, 1.25);
      const drawSize = Math.max(0, frame.cellSize - inset * 2);
      context.save();
      try {
        context.fillStyle = teamColor;
        for (const cell of cells) {
          context.globalAlpha = cell.opacity;
          context.fillRect(
            CANVAS_PADDING_PX + cell.position.x * frame.cellSize + inset,
            CANVAS_PADDING_PX + cell.position.y * frame.cellSize + inset,
            drawSize,
            drawSize,
          );
        }
      } finally {
        context.restore();
      }
    }

    const readout = sampleScoreReadout(activation, frame);
    if (!readout) {
      return;
    }

    context.save();
    try {
      context.globalAlpha = readout.opacity;
      context.translate(readout.centerX, readout.centerY);
      context.scale(readout.scale, readout.scale);
      context.textAlign = 'center';
      context.textBaseline = 'middle';
      context.font = typeof document !== 'undefined' &&
        document.documentElement.dataset.scenarioCapture === 'true'
        ? `italic 800 ${readout.fontSize}px 'Snaketron Capture Black', sans-serif`
        : `italic 900 ${readout.fontSize}px Impact, 'Arial Black', sans-serif`;
      // A white halo keeps the readout legible over tinted end zones and the
      // fading cell wave without dimming the team colour.
      context.lineWidth = Math.max(2, readout.fontSize * 0.18);
      context.lineJoin = 'round';
      context.miterLimit = 2;
      context.strokeStyle = '#ffffff';
      context.strokeText(readout.text, 0, 0);
      context.fillStyle = getScoreReadoutColor(
        activation.teamId,
        frame.localTeamId,
        activation.celebration,
      );
      context.fillText(readout.text, 0, 0);
    } finally {
      context.restore();
    }
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
    const renderer = registry.resolve(activation.effectId);
    if (!renderer) {
      continue;
    }
    context.save();
    try {
      renderer.draw(context, activation, frame);
    } catch (error) {
      // Score effects are cosmetic and execute in the middle of the gameplay
      // render. Keep a bad swappable renderer from suppressing snakes/walls.
      console.error('Score celebration renderer failed', error);
    } finally {
      // A swappable renderer may fail after changing compositing, filters,
      // shadows, or transforms. Isolate every invocation so both subsequent
      // effects and the gameplay layers receive the caller's original state.
      context.restore();
    }
  }
};
