import React, { useEffect, useRef } from 'react';
import {
  defaultFlowFieldSpacing,
  drawArenaFlowField,
} from '../utils/arenaFlowField';

const clamp = (value: number, min: number, max: number): number =>
  Math.min(max, Math.max(min, value));

// Kill switch for the backdrop during gameplay (/play/*), while investigating
// rendering freezes and input delay. Flip to true to show it there again.
export const SHOW_BACKDROP_DURING_GAMEPLAY = false;

const FRAME_INTERVAL_MS = 1000 / 6;

interface PointerPosition {
  x: number;
  y: number;
  targetX: number;
  targetY: number;
  activity: number;
  influence: number;
  isInside: boolean;
  isInitialized: boolean;
}

export const ArenaBackdrop: React.FC = () => {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    const context = canvas?.getContext('2d');

    if (!canvas || !context) {
      return;
    }

    const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    const pointer: PointerPosition = {
      x: 0,
      y: 0,
      targetX: 0,
      targetY: 0,
      activity: 0,
      influence: 0,
      isInside: false,
      isInitialized: false,
    };
    let frameId = 0;
    let width = 0;
    let height = 0;
    let devicePixelRatio = 1;
    let previousTimestamp = 0;

    const resizeCanvas = (): void => {
      width = window.innerWidth;
      height = window.innerHeight;
      devicePixelRatio = Math.min(window.devicePixelRatio || 1, 2);
      canvas.width = Math.round(width * devicePixelRatio);
      canvas.height = Math.round(height * devicePixelRatio);
      canvas.style.width = `${width}px`;
      canvas.style.height = `${height}px`;
    };

    const draw = (timestamp: number): void => {
      // The dots drift slowly, so redrawing at ~6fps is indistinguishable
      // from 60fps and keeps the backdrop cheap, including during gameplay.
      if (previousTimestamp && timestamp - previousTimestamp < FRAME_INTERVAL_MS) {
        frameId = window.requestAnimationFrame(draw);
        return;
      }

      const elapsedSeconds = previousTimestamp
        ? clamp((timestamp - previousTimestamp) / 1000, 0, 0.25)
        : 1 / 60;
      previousTimestamp = timestamp;

      if (pointer.isInitialized) {
        const positionEase = 1 - Math.exp(-elapsedSeconds * 2.3);
        const influenceEase = 1 - Math.exp(-elapsedSeconds * 2.5);
        pointer.x += (pointer.targetX - pointer.x) * positionEase;
        pointer.y += (pointer.targetY - pointer.y) * positionEase;
        pointer.activity *= Math.exp(-elapsedSeconds * 0.62);
        const influenceTarget = pointer.isInside ? pointer.activity : 0;
        pointer.influence += (influenceTarget - pointer.influence) * influenceEase;
      }

      context.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);
      context.clearRect(0, 0, width, height);
      drawArenaFlowField({
        context,
        width,
        height,
        time: reducedMotion ? 0 : timestamp / 1000,
        pointer,
        spacing: defaultFlowFieldSpacing(width),
      });

      if (!reducedMotion) {
        frameId = window.requestAnimationFrame(draw);
      }
    };

    const handlePointerMove = (event: PointerEvent): void => {
      if (event.pointerType !== 'mouse' && event.pointerType !== 'pen') {
        return;
      }

      if (!pointer.isInitialized) {
        pointer.x = event.clientX;
        pointer.y = event.clientY;
        pointer.targetX = event.clientX;
        pointer.targetY = event.clientY;
        pointer.activity = 0.52;
        pointer.isInitialized = true;
      } else {
        const travelDistance = Math.hypot(
          event.clientX - pointer.targetX,
          event.clientY - pointer.targetY,
        );
        const movementEnergy = clamp(0.46 + travelDistance / 36, 0, 1);
        pointer.activity = Math.max(pointer.activity, movementEnergy);
      }

      pointer.targetX = event.clientX;
      pointer.targetY = event.clientY;
      pointer.isInside = true;
    };

    const handlePointerLeave = (): void => {
      pointer.isInside = false;
    };

    const handleResize = (): void => {
      resizeCanvas();
      if (reducedMotion) {
        draw(0);
      }
    };

    resizeCanvas();
    window.addEventListener('resize', handleResize);
    window.addEventListener('pointermove', handlePointerMove, { passive: true });
    window.addEventListener('blur', handlePointerLeave);
    document.documentElement.addEventListener('pointerleave', handlePointerLeave);
    if (reducedMotion) {
      draw(0);
    } else {
      frameId = window.requestAnimationFrame(draw);
    }

    return () => {
      window.cancelAnimationFrame(frameId);
      window.removeEventListener('resize', handleResize);
      window.removeEventListener('pointermove', handlePointerMove);
      window.removeEventListener('blur', handlePointerLeave);
      document.documentElement.removeEventListener('pointerleave', handlePointerLeave);
    };
  }, []);

  return (
    <canvas
      ref={canvasRef}
      className="home-arena-backdrop"
      data-background-concept="flow"
      aria-hidden="true"
    />
  );
};
