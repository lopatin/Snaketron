import type { ArenaRotation, Position } from '../types';

export const CRASH_EXPLOSION_SPRITE_URL = '/images/crash-explosion.png';
export const CRASH_EXPLOSION_COLUMNS = 8;
export const CRASH_EXPLOSION_ROWS = 8;
export const CRASH_EXPLOSION_FRAME_COUNT =
  CRASH_EXPLOSION_COLUMNS * CRASH_EXPLOSION_ROWS;
export const CRASH_EXPLOSION_DURATION_MS = 780;
export const REDUCED_MOTION_EXPLOSION_DURATION_MS = 180;
export const REDUCED_MOTION_FRAME_INDEX = 24;

const CANVAS_PADDING_PX = 1;
const MAX_SIMULTANEOUS_EXPLOSIONS = 12;

export interface CrashExplosion {
  eventId: string;
  snakeId: number;
  position: Position;
  startedAt: number;
  rotationRadians: number;
  predictionTick?: number;
}

export interface CrashExplosionRenderState {
  centerX: number;
  centerY: number;
  drawSize: number;
  frameIndex: number;
  opacity: number;
  progress: number;
}

const clamp = (value: number, min: number, max: number): number =>
  Math.min(max, Math.max(min, value));

const easeOutCubic = (value: number): number => 1 - Math.pow(1 - value, 3);

export interface PredictedCrashCue {
  tick: number;
  snake_id: number;
  position: Position;
}

export interface PredictedCrashVisualState {
  predicted_tick: number;
  committed_tick: number;
  tick_duration_ms: number;
  cues: readonly PredictedCrashCue[];
}

export interface PredictedCrashSyncResult {
  started: number;
  cancelled: number;
}

/** Mirrors client/src/render.rs so cosmetic overlays cannot drift from the arena. */
export const transformExplosionPosition = (
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

export const getCrashExplosionFrameIndex = (
  elapsedMs: number,
  reducedMotion = false,
): number | null => {
  const duration = reducedMotion
    ? REDUCED_MOTION_EXPLOSION_DURATION_MS
    : CRASH_EXPLOSION_DURATION_MS;

  if (!Number.isFinite(elapsedMs) || elapsedMs < 0 || elapsedMs >= duration) {
    return null;
  }

  if (reducedMotion) {
    return REDUCED_MOTION_FRAME_INDEX;
  }

  return Math.min(
    CRASH_EXPLOSION_FRAME_COUNT - 1,
    Math.floor((elapsedMs / duration) * CRASH_EXPLOSION_FRAME_COUNT),
  );
};

export const createCrashExplosion = (
  eventId: string,
  snakeId: number,
  position: Position,
  startedAt: number,
  prediction?: { tick: number },
): CrashExplosion | null => {
  if (
    !eventId ||
    !Number.isFinite(position.x) ||
    !Number.isFinite(position.y) ||
    !Number.isFinite(startedAt)
  ) {
    return null;
  }

  // A tiny deterministic cant keeps simultaneous blasts from looking stamped
  // while preserving the source animation's temporal coherence.
  const rotationSeed = snakeId * 5 + position.x * 3 + position.y;
  const rotationDegrees = ((rotationSeed % 9) + 9) % 9 - 4;

  return {
    eventId,
    snakeId,
    position: { ...position },
    startedAt,
    rotationRadians: (rotationDegrees * Math.PI) / 180,
    predictionTick: prediction?.tick,
  };
};

export const enqueueCrashExplosion = (
  explosions: CrashExplosion[],
  seenEventIds: Set<string>,
  eventId: string,
  snakeId: number,
  position: Position,
  startedAt: number,
  prediction?: { tick: number },
): boolean => {
  if (seenEventIds.has(eventId)) {
    return false;
  }

  const explosion = createCrashExplosion(
    eventId,
    snakeId,
    position,
    startedAt,
    prediction,
  );
  if (!explosion) {
    return false;
  }

  seenEventIds.add(eventId);
  explosions.push(explosion);
  if (explosions.length > MAX_SIMULTANEOUS_EXPLOSIONS) {
    explosions.splice(0, explosions.length - MAX_SIMULTANEOUS_EXPLOSIONS);
  }
  return true;
};

/**
 * Starts effects from rollback-visible predicted crash history. A prediction
 * replay supplies the complete recent cue set, so an absent identity is an
 * actual correction (not merely the next normal tick) and can be retracted.
 */
export const syncPredictedCrashExplosions = (
  explosions: CrashExplosion[],
  seenEventIds: Set<string>,
  gameId: string,
  visualState: PredictedCrashVisualState,
  now: number,
  suppressStartsAtOrBeforeTick?: number,
): PredictedCrashSyncResult => {
  const currentCueIds = new Set<string>();
  let started = 0;
  let cancelled = 0;

  const tickDurationMs = Math.max(1, visualState.tick_duration_ms);
  for (const cue of visualState.cues) {
    const eventId = `${gameId}:predicted:${cue.tick}:${cue.snake_id}:${cue.position.x}:${cue.position.y}`;
    currentCueIds.add(eventId);
    const elapsedMs = Math.max(
      0,
      (visualState.predicted_tick - cue.tick) * tickDurationMs,
    );

    // A new engine epoch begins from an authoritative snapshot. Baseline cues
    // at or before that committed tick are history, not a new visual event;
    // speculative cues produced while catching up beyond it still play.
    if (
      seenEventIds.has(eventId) ||
      elapsedMs >= CRASH_EXPLOSION_DURATION_MS ||
      (suppressStartsAtOrBeforeTick !== undefined &&
        cue.tick <= suppressStartsAtOrBeforeTick)
    ) {
      seenEventIds.add(eventId);
      continue;
    }

    if (enqueueCrashExplosion(
      explosions,
      seenEventIds,
      eventId,
      cue.snake_id,
      cue.position,
      now - elapsedMs,
      { tick: cue.tick },
    )) {
      started += 1;
    }
  }

  for (let index = explosions.length - 1; index >= 0; index -= 1) {
    const explosion = explosions[index];
    if (explosion.predictionTick === undefined || currentCueIds.has(explosion.eventId)) {
      continue;
    }

    explosions.splice(index, 1);
    seenEventIds.delete(explosion.eventId);
    cancelled += 1;
  }

  // Effects remove themselves when their time elapses. Release dedupe entries
  // once a cue also leaves prediction history so a genuine rollback-and-replay
  // can trigger the same deterministic identity again.
  const predictedPrefix = `${gameId}:predicted:`;
  for (const eventId of seenEventIds) {
    if (eventId.startsWith(predictedPrefix) && !currentCueIds.has(eventId)) {
      seenEventIds.delete(eventId);
    }
  }

  return { started, cancelled };
};

export const getCrashExplosionRenderState = (
  explosion: CrashExplosion,
  now: number,
  cellSize: number,
  arenaWidth: number,
  arenaHeight: number,
  rotation: ArenaRotation,
  reducedMotion = false,
): CrashExplosionRenderState | null => {
  const elapsedMs = now - explosion.startedAt;
  const frameIndex = getCrashExplosionFrameIndex(elapsedMs, reducedMotion);
  if (frameIndex === null) {
    return null;
  }

  const duration = reducedMotion
    ? REDUCED_MOTION_EXPLOSION_DURATION_MS
    : CRASH_EXPLOSION_DURATION_MS;
  const progress = elapsedMs / duration;
  const transformed = transformExplosionPosition(
    explosion.position,
    arenaWidth,
    arenaHeight,
    rotation,
  );
  const baseSize = clamp(cellSize * 7.5, 72, 120);
  const swell = reducedMotion ? 1 : 0.96 + easeOutCubic(progress) * 0.08;
  const fadeProgress = clamp((progress - 0.8) / 0.2, 0, 1);
  const drawSize = baseSize * swell;
  const centerX = CANVAS_PADDING_PX + transformed.x * cellSize + cellSize / 2;
  const centerY = CANVAS_PADDING_PX + transformed.y * cellSize + cellSize / 2;

  return {
    centerX,
    centerY,
    drawSize,
    frameIndex,
    opacity: reducedMotion ? 1 - progress : 1 - fadeProgress * fadeProgress,
    progress,
  };
};

export const drawCrashExplosions = (
  context: CanvasRenderingContext2D,
  sprite: HTMLImageElement | null,
  explosions: CrashExplosion[],
  now: number,
  cellSize: number,
  arenaWidth: number,
  arenaHeight: number,
  rotation: ArenaRotation,
  reducedMotion = false,
): void => {
  const spriteReady = Boolean(
    sprite?.complete && sprite.naturalWidth > 0 && sprite.naturalHeight > 0,
  );

  for (let index = explosions.length - 1; index >= 0; index -= 1) {
    const explosion = explosions[index];
    const renderState = getCrashExplosionRenderState(
      explosion,
      now,
      cellSize,
      arenaWidth,
      arenaHeight,
      rotation,
      reducedMotion,
    );

    if (!renderState) {
      explosions.splice(index, 1);
      continue;
    }

    const {
      centerX,
      centerY,
      drawSize,
      frameIndex,
      opacity,
      progress,
    } = renderState;

    // One precise pressure ring bridges the realistic fireball to the game's
    // crisp geometry without diluting the visual joke with extra decoration.
    if (!reducedMotion && progress < 0.2) {
      const ringProgress = progress / 0.2;
      const radius = drawSize * (0.12 + easeOutCubic(ringProgress) * 0.48);
      context.save();
      context.globalAlpha = Math.pow(1 - ringProgress, 2) * 0.62;
      context.strokeStyle = '#ff8a2a';
      context.lineWidth = Math.max(1.5, cellSize * 0.16);
      context.beginPath();
      context.arc(centerX, centerY, radius, 0, Math.PI * 2);
      context.stroke();
      context.restore();
    }

    if (!spriteReady || !sprite) {
      continue;
    }

    const sourceWidth = sprite.naturalWidth / CRASH_EXPLOSION_COLUMNS;
    const sourceHeight = sprite.naturalHeight / CRASH_EXPLOSION_ROWS;
    const sourceX = (frameIndex % CRASH_EXPLOSION_COLUMNS) * sourceWidth;
    const sourceY = Math.floor(frameIndex / CRASH_EXPLOSION_COLUMNS) * sourceHeight;

    context.save();
    context.translate(centerX, centerY);
    if (!reducedMotion) {
      context.rotate(explosion.rotationRadians);
    }
    context.globalAlpha = opacity;
    context.imageSmoothingEnabled = true;
    context.imageSmoothingQuality = 'high';
    context.filter = 'saturate(1.12) contrast(1.04)';
    context.shadowColor = 'rgba(255, 105, 32, 0.55)';
    context.shadowBlur = reducedMotion ? 0 : Math.max(4, drawSize * 0.08);
    context.drawImage(
      sprite,
      sourceX,
      sourceY,
      sourceWidth,
      sourceHeight,
      -drawSize / 2,
      -drawSize / 2,
      drawSize,
      drawSize,
    );
    context.restore();
  }
};
