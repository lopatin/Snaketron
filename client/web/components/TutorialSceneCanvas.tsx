import React, { useEffect, useRef, useState } from 'react';
import { getWasm, initWasm } from '../wasm';

export interface TutorialSceneCanvasProps {
  /** Scene id from `tutorialSceneIds()` in the WASM module. */
  scene: string;
  /** Increment to replay the scene without changing tutorial steps. */
  replayToken?: number;
  /** Play the authored timeline, or render only its authored poster frame. */
  playback?: 'play' | 'poster';
}

const FRAME_QUANTUM_MS = 50;

/**
 * Plays one focused tutorial beat through the production Rust arena renderer.
 * The WASM player owns a reusable full-arena scratch canvas; this component
 * only sizes the visible crop and schedules real 50ms game-style frames.
 */
const TutorialSceneCanvas: React.FC<TutorialSceneCanvasProps> = ({
  scene,
  replayToken = 0,
  playback = 'play',
}) => {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const [reducedMotion, setReducedMotion] = useState(() => (
    typeof window !== 'undefined' &&
    typeof window.matchMedia === 'function' &&
    window.matchMedia('(prefers-reduced-motion: reduce)').matches
  ));

  useEffect(() => {
    if (typeof window.matchMedia !== 'function') {
      return undefined;
    }
    const query = window.matchMedia('(prefers-reduced-motion: reduce)');
    const handleChange = () => setReducedMotion(query.matches);
    handleChange();
    query.addEventListener('change', handleChange);
    return () => query.removeEventListener('change', handleChange);
  }, []);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) {
      return undefined;
    }

    let disposed = false;
    let animationFrame = 0;
    let player: InstanceType<
      NonNullable<ReturnType<typeof getWasm>>['TutorialScenePlayer']
    > | null = null;
    let startedAt = 0;
    let elapsedMs = 0;
    let lastFrameMs = -1;
    let forceDraw = true;
    let renderFailed = false;
    canvas.dataset.playback = 'loading';

    const sizeCanvas = () => {
      const bounds = canvas.getBoundingClientRect();
      if (bounds.width <= 0 || bounds.height <= 0) {
        return false;
      }
      const pixelRatio = Math.min(window.devicePixelRatio || 1, 2);
      const width = Math.round(bounds.width * pixelRatio);
      const height = Math.round(bounds.height * pixelRatio);
      if (canvas.width !== width || canvas.height !== height) {
        canvas.width = width;
        canvas.height = height;
        forceDraw = true;
      }
      return true;
    };

    const schedule = () => {
      if (!disposed && !animationFrame) {
        animationFrame = window.requestAnimationFrame(draw);
      }
    };

    const draw = (now: number) => {
      animationFrame = 0;
      if (disposed || renderFailed || !player || document.hidden || !sizeCanvas()) {
        return;
      }

      const durationMs = player.durationMs();
      const showPoster = playback === 'poster' || reducedMotion;
      elapsedMs = showPoster
        ? player.posterMs()
        : Math.min(durationMs, Math.max(0, now - startedAt));
      const quantizedMs = showPoster
        ? elapsedMs
        : Math.min(durationMs, Math.floor(elapsedMs / FRAME_QUANTUM_MS) * FRAME_QUANTUM_MS);

      if (forceDraw || quantizedMs !== lastFrameMs) {
        try {
          player.renderFrame(quantizedMs, canvas);
        } catch (error) {
          renderFailed = true;
          canvas.dataset.playback = 'error';
          console.error('Failed to render tutorial scene', scene, error);
          return;
        }
        lastFrameMs = quantizedMs;
        forceDraw = false;
      }

      if (!showPoster && elapsedMs < durationMs) {
        canvas.dataset.playback = 'playing';
        schedule();
      } else {
        canvas.dataset.playback = 'complete';
      }
    };

    const handleVisibility = () => {
      if (!document.hidden) {
        startedAt = performance.now() - elapsedMs;
        forceDraw = true;
        schedule();
      }
    };

    const observer = new ResizeObserver(() => {
      forceDraw = true;
      schedule();
    });
    observer.observe(canvas);
    document.addEventListener('visibilitychange', handleVisibility);

    void initWasm().then((wasm) => {
      if (disposed) {
        return;
      }
      player = new wasm.TutorialScenePlayer(scene);
      startedAt = performance.now();
      schedule();
    }).catch((error) => {
      canvas.dataset.playback = 'error';
      console.error('Failed to initialize tutorial scene', scene, error);
    });

    return () => {
      disposed = true;
      observer.disconnect();
      document.removeEventListener('visibilitychange', handleVisibility);
      if (animationFrame) {
        window.cancelAnimationFrame(animationFrame);
      }
      player?.free();
    };
  }, [playback, reducedMotion, replayToken, scene]);

  return (
    <canvas
      ref={canvasRef}
      className="tutorial-scene-canvas"
      data-scene={scene}
      data-motion={reducedMotion ? 'reduced' : 'animated'}
      data-playback-mode={playback}
      data-testid="tutorial-scene-canvas"
      aria-hidden="true"
    />
  );
};

export default TutorialSceneCanvas;
