import type { ArenaRotation, Position } from '../types';
// The explicit extension keeps this import resolvable through webpack and
// through the Node test runner alike, which is what lets the rotation mapping
// stay shared with the renderer instead of duplicated here.
import { transformScoreEffectPosition } from './scoreEffects.ts';

/** Mirrors the 1px canvas padding the Rust renderer draws inside of. */
const CANVAS_PADDING_PX = 1;

/**
 * Opacity the callout fades to once the head is right up against it. The
 * announcement stays readable at a glance while the cells it covers become
 * visible again, which is all the player needs while steering through that
 * corner of the arena.
 */
export const COMBO_CALLOUT_NEAR_OPACITY = 0.2;

/**
 * Distance, in arena cells from the callout's box, at which it is fully faded.
 */
const NEAR_CELLS = 1;
/**
 * Distance at which the callout is fully opaque again. Deliberately close: the
 * callout should read at full strength for the whole approach and only get out
 * of the way for the last couple of cells, rather than dimming across half the
 * field whenever the chain happens to be live.
 */
const FAR_CELLS = 2.5;

export interface ViewportRect {
  left: number;
  top: number;
  right: number;
  bottom: number;
}

export interface ComboProximityInputs {
  /** Local snake head, in arena grid coordinates. */
  head: Position;
  arenaWidth: number;
  arenaHeight: number;
  rotation: ArenaRotation;
  cellSize: number;
  /** Viewport rect of the arena canvas, whose pixels are 1:1 with the grid. */
  canvasRect: ViewportRect;
  /** Viewport rect of the callout itself, measured after layout. */
  calloutRect: ViewportRect;
}

const clamp01 = (value: number): number => Math.min(1, Math.max(0, value));

/** Shortest distance from a point to a rectangle; zero when inside it. */
const distanceToRect = (x: number, y: number, rect: ViewportRect): number => {
  const dx = Math.max(rect.left - x, 0, x - rect.right);
  const dy = Math.max(rect.top - y, 0, y - rect.bottom);
  return Math.hypot(dx, dy);
};

/**
 * Fade the combo callout out of the way of its own player's head.
 *
 * The callout is parked at the top center of the field, which is a place the
 * snake genuinely has to drive through. Distance is measured in viewport
 * pixels between the head's rendered center and the callout's measured box —
 * rather than in grid cells against an assumed layout — so the fade tracks
 * whatever the responsive callout actually grew to at this arena scale, and
 * through every arena rotation.
 *
 * Returns a multiplier for the callout's opacity: a flat 1 unless the head is
 * within a couple of cells of the box, then down to
 * [`COMBO_CALLOUT_NEAR_OPACITY`] once it is right on top of it.
 */
export function comboCalloutProximityOpacity(inputs: ComboProximityInputs): number {
  const {
    head,
    arenaWidth,
    arenaHeight,
    rotation,
    cellSize,
    canvasRect,
    calloutRect,
  } = inputs;

  if (
    !Number.isFinite(cellSize) ||
    cellSize <= 0 ||
    arenaWidth <= 0 ||
    arenaHeight <= 0 ||
    // A callout that has not been laid out yet has nothing to move out of.
    calloutRect.right <= calloutRect.left ||
    calloutRect.bottom <= calloutRect.top
  ) {
    return 1;
  }

  const screenCell = transformScoreEffectPosition(head, arenaWidth, arenaHeight, rotation);
  const headX =
    canvasRect.left + CANVAS_PADDING_PX + (screenCell.x + 0.5) * cellSize;
  const headY =
    canvasRect.top + CANVAS_PADDING_PX + (screenCell.y + 0.5) * cellSize;

  const distance = distanceToRect(headX, headY, calloutRect);
  const nearPx = NEAR_CELLS * cellSize;
  const farPx = FAR_CELLS * cellSize;
  const clearness = clamp01((distance - nearPx) / (farPx - nearPx));

  return COMBO_CALLOUT_NEAR_OPACITY + (1 - COMBO_CALLOUT_NEAR_OPACITY) * clearness;
}
