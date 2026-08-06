import React, { useLayoutEffect, useRef } from 'react';
import {
  drawRosterSnakeCanvas,
  getRosterSnakeLabelColor,
} from '../utils/rosterSnakeCanvas';
import type { RosterSnakeFacing } from '../utils/rosterSnakeCanvas';

export interface RosterSnakeCanvasProps {
  name: string;
  fill: string;
  outline: string;
  facing: RosterSnakeFacing;
}

const RosterSnakeCanvas: React.FC<RosterSnakeCanvasProps> = ({
  name,
  fill,
  outline,
  facing,
}) => {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const labelColor = getRosterSnakeLabelColor(fill);

  useLayoutEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return undefined;

    let animationFrame = 0;
    let disposed = false;
    let resolutionQuery: MediaQueryList | null = null;

    const draw = () => {
      if (disposed) return;
      const computedStyle = window.getComputedStyle(canvas);
      drawRosterSnakeCanvas(canvas, {
        facing,
        name,
        fill,
        outline,
        labelColor,
        fontFamily: computedStyle.fontFamily,
      }, window.devicePixelRatio || 1);
    };

    const scheduleDraw = () => {
      if (animationFrame) window.cancelAnimationFrame(animationFrame);
      animationFrame = window.requestAnimationFrame(() => {
        animationFrame = 0;
        draw();
      });
    };

    const handleResolutionChange = () => {
      bindResolutionQuery();
      scheduleDraw();
    };

    const bindResolutionQuery = () => {
      resolutionQuery?.removeEventListener('change', handleResolutionChange);
      resolutionQuery = window.matchMedia?.(`(resolution: ${window.devicePixelRatio || 1}dppx)`)
        ?? null;
      resolutionQuery?.addEventListener('change', handleResolutionChange);
    };

    const resizeObserver = typeof ResizeObserver === 'undefined'
      ? null
      : new ResizeObserver(scheduleDraw);
    resizeObserver?.observe(canvas);
    window.addEventListener('resize', scheduleDraw);
    window.visualViewport?.addEventListener('resize', scheduleDraw);
    bindResolutionQuery();
    draw();

    void document.fonts?.ready.then(() => {
      if (!disposed) scheduleDraw();
    });

    return () => {
      disposed = true;
      if (animationFrame) window.cancelAnimationFrame(animationFrame);
      resizeObserver?.disconnect();
      window.removeEventListener('resize', scheduleDraw);
      window.visualViewport?.removeEventListener('resize', scheduleDraw);
      resolutionQuery?.removeEventListener('change', handleResolutionChange);
    };
  }, [facing, fill, labelColor, name, outline]);

  return (
    <span
      className={`game-roster-snake is-facing-${facing}`}
      data-player-name={name}
      data-facing={facing}
      style={{
        '--snake-fill': fill,
        '--snake-outline': outline,
        '--snake-label': labelColor,
      } as React.CSSProperties}
      aria-hidden="true"
    >
      <canvas
        ref={canvasRef}
        className="game-roster-snake-canvas"
        width={1}
        height={1}
        data-player-name={name}
        data-facing={facing}
        aria-hidden="true"
        style={{
          display: 'block',
          width: '100%',
          height: '100%',
          pointerEvents: 'none',
        }}
      />
    </span>
  );
};

export default RosterSnakeCanvas;
