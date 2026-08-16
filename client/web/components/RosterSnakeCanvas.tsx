import React, { useLayoutEffect, useRef } from 'react';
import { getWasm, initWasm, whenSkinAssetsSettle } from '../wasm';
import type { SnakeSkinColors, SnakeSkinInputs } from '../utils/snakeSkin';

export type RosterSnakeFacing = 'left' | 'right';

export interface RosterSnakeCanvasProps {
  name: string;
  skin: SnakeSkinInputs;
  facing: RosterSnakeFacing;
  isReady?: boolean;
}

/**
 * A player's snake, drawn by the arena renderer itself.
 *
 * The whole glyph — palette, outline, head gradient, dark head core, and the
 * name set inside the body against the head — is produced by
 * `renderRosterSnake` in client/src/render.rs, which shares its skin routine
 * with the arena draw loop. Nothing about a snake's appearance is duplicated
 * here, so the roster cannot fall out of step with the game.
 */
const RosterSnakeCanvas: React.FC<RosterSnakeCanvasProps> = ({
  name,
  skin,
  facing,
  isReady = false,
}) => {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const wrapperRef = useRef<HTMLSpanElement>(null);
  // `skin` is rebuilt on every game-state tick, so depend on its value rather
  // than its identity; otherwise the roster would repaint every frame.
  const skinKey = JSON.stringify(skin);

  useLayoutEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return undefined;

    let animationFrame = 0;
    let disposed = false;
    let resolutionQuery: MediaQueryList | null = null;

    const draw = () => {
      if (disposed) return;
      const wasm = getWasm();
      if (!wasm) return;

      const bounds = canvas.getBoundingClientRect();
      const width = bounds.width || canvas.clientWidth;
      const height = bounds.height || canvas.clientHeight;
      if (width <= 0 || height <= 0) return;

      const request = JSON.stringify({
        ...(JSON.parse(skinKey) as SnakeSkinInputs),
        facing,
        name,
        is_ready: isReady,
        font_family: window.getComputedStyle(canvas).fontFamily,
      });

      try {
        const painted = JSON.parse(wasm.renderRosterSnake(
          canvas,
          width,
          height,
          window.devicePixelRatio || 1,
          request,
        )) as SnakeSkinColors;

        // Advertise the colours the renderer actually used, so styling hooks
        // and layout tests read the painted skin rather than a second guess.
        wrapperRef.current?.style.setProperty('--snake-fill', painted.fill);
        wrapperRef.current?.style.setProperty('--snake-outline', painted.outline);
        wrapperRef.current?.style.setProperty('--snake-label', painted.label);
      } catch (error) {
        console.warn('Failed to render roster snake:', error);
      }
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

    // The renderer lives in WASM, so the first paint may have to wait for it.
    void initWasm().then(() => {
      if (disposed) return undefined;
      // Painted now rather than on the next frame, because the paint is what
      // *requests* a textured skin's pixels — and the wait below has to start
      // after that request or it finds nothing pending and resolves at once.
      draw();
      // A textured skin cannot show its coat until the pixels decode, and
      // nothing else would ever make this canvas repaint: the roster is drawn
      // once per state change, not per frame.
      return whenSkinAssetsSettle().then(() => {
        if (!disposed) scheduleDraw();
      });
    }).catch(() => undefined);

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
  }, [facing, isReady, name, skinKey]);

  return (
    <span
      ref={wrapperRef}
      className={`game-roster-snake is-facing-${facing}`}
      data-player-name={name}
      data-facing={facing}
      data-ready={isReady ? 'true' : 'false'}
      aria-hidden="true"
    >
      <canvas
        ref={canvasRef}
        className="game-roster-snake-canvas"
        width={1}
        height={1}
        data-player-name={name}
        data-facing={facing}
        data-ready={isReady ? 'true' : 'false'}
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
