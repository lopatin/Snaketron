import React, { useEffect, useRef, useState } from 'react';
import { getWasm, initWasm } from '../wasm';
import {
  CLASSIC_CELEBRATION_THEME,
  DEFAULT_SCORE_EFFECT_ID,
  goalImpactWaveRenderer,
} from '../utils/scoreEffects';

export interface TutorialSceneCanvasProps {
  /** Scene id from `tutorialSceneIds()` in the WASM module. */
  scene: string;
  /** Increment to replay the scene without changing tutorial steps. */
  replayToken?: number;
}

const FRAME_QUANTUM_MS = 50;
const REDUCED_MOTION_CELEBRATION_FRAME_MS = 120;

interface TutorialScoreEffect {
  startMs: number;
  teamId: number;
  snakeId: number;
  points: number;
  origin: { x: number; y: number };
  arenaWidth: number;
  arenaHeight: number;
}

/**
 * Plays one focused tutorial beat through the production Rust arena renderer.
 * The WASM player owns a reusable full-arena scratch canvas; this component
 * only sizes the visible crop and schedules real 50ms game-style frames.
 */
const TutorialSceneCanvas: React.FC<TutorialSceneCanvasProps> = ({
  scene,
  replayToken = 0,
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
    let scoreEffect: TutorialScoreEffect | null = null;
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
      elapsedMs = reducedMotion
        ? player.posterMs()
        : Math.min(durationMs, Math.max(0, now - startedAt));
      const quantizedMs = reducedMotion
        ? elapsedMs
        : Math.min(durationMs, Math.floor(elapsedMs / FRAME_QUANTUM_MS) * FRAME_QUANTUM_MS);

      if (forceDraw || quantizedMs !== lastFrameMs) {
        try {
          player.renderFrameWithCelebration(
            quantizedMs,
            canvas,
            (scratchCanvas: HTMLCanvasElement, scratchCellSize: number) => {
              if (!scoreEffect) {
                return;
              }
              const context = scratchCanvas.getContext('2d');
              if (!context) {
                return;
              }
              const effectNowMs = reducedMotion
                ? scoreEffect.startMs + REDUCED_MOTION_CELEBRATION_FRAME_MS
                : quantizedMs;
              goalImpactWaveRenderer.draw(
                context,
                {
                  eventId: `tutorial:${scene}:score`,
                  effectId: DEFAULT_SCORE_EFFECT_ID,
                  // Tutorials always teach the canonical look.
                  celebration: CLASSIC_CELEBRATION_THEME,
                  teamId: scoreEffect.teamId,
                  snakeId: scoreEffect.snakeId,
                  points: scoreEffect.points,
                  tick: Math.floor(scoreEffect.startMs / FRAME_QUANTUM_MS),
                  origin: scoreEffect.origin,
                  startedAtMs: scoreEffect.startMs,
                },
                {
                  nowMs: effectNowMs,
                  cellSize: scratchCellSize,
                  arenaWidth: scoreEffect.arenaWidth,
                  arenaHeight: scoreEffect.arenaHeight,
                  rotation: 0,
                  localTeamId: 0,
                  reducedMotion,
                },
              );
            },
          );
        } catch (error) {
          renderFailed = true;
          canvas.dataset.playback = 'error';
          console.error('Failed to render tutorial scene', scene, error);
          return;
        }
        lastFrameMs = quantizedMs;
        forceDraw = false;
      }

      if (!reducedMotion && elapsedMs < durationMs) {
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
      const scoreEffectJson = player.scoreEffectJson();
      scoreEffect = scoreEffectJson
        ? JSON.parse(scoreEffectJson) as TutorialScoreEffect
        : null;
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
  }, [reducedMotion, replayToken, scene]);

  return (
    <canvas
      ref={canvasRef}
      className="tutorial-scene-canvas"
      data-scene={scene}
      data-motion={reducedMotion ? 'reduced' : 'animated'}
      data-testid="tutorial-scene-canvas"
      aria-hidden="true"
    />
  );
};

export default TutorialSceneCanvas;
