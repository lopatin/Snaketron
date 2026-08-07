import React, { useEffect, useRef } from 'react';
import { getWasm, initWasm } from '../wasm';

export interface TutorialSceneCanvasProps {
  /** Scene id from `tutorialSceneIds()` in the WASM module. */
  scene: string;
}

/**
 * Draws one tutorial illustration using the real game renderer.
 *
 * The frame is produced by the same Rust function that paints the arena during
 * play, from a real `GameState`, so these are not screenshots and cannot drift
 * from the game — they render identically on every platform and stay correct
 * when the renderer changes. Sizing follows the arena canvas convention: the
 * backing store is device pixels, so the illustration is crisp on HiDPI.
 */
const TutorialSceneCanvas: React.FC<TutorialSceneCanvasProps> = ({ scene }) => {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) {
      return undefined;
    }

    let disposed = false;
    let animationFrame = 0;

    const draw = () => {
      animationFrame = 0;
      if (disposed) {
        return;
      }
      const wasm = getWasm();
      if (!wasm) {
        return;
      }
      const bounds = canvas.getBoundingClientRect();
      if (bounds.width <= 0 || bounds.height <= 0) {
        return;
      }
      const pixelRatio = Math.min(window.devicePixelRatio || 1, 2);
      const width = Math.round(bounds.width * pixelRatio);
      const height = Math.round(bounds.height * pixelRatio);
      if (canvas.width !== width || canvas.height !== height) {
        canvas.width = width;
        canvas.height = height;
      }
      try {
        wasm.renderTutorialScene(scene, canvas);
      } catch (error) {
        // A missing scene must not take the arena down with it: the bullet
        // still reads fine without its picture.
        console.error('Failed to render tutorial scene', scene, error);
      }
    };

    const scheduleDraw = () => {
      if (animationFrame) {
        return;
      }
      animationFrame = window.requestAnimationFrame(draw);
    };

    // The modal can open before the WASM module has finished loading on a cold
    // page load, so wait for it rather than rendering nothing.
    void initWasm().then(scheduleDraw).catch(() => {});

    const observer = new ResizeObserver(scheduleDraw);
    observer.observe(canvas);
    scheduleDraw();

    return () => {
      disposed = true;
      observer.disconnect();
      if (animationFrame) {
        window.cancelAnimationFrame(animationFrame);
      }
    };
  }, [scene]);

  return (
    <canvas
      ref={canvasRef}
      className="tutorial-scene-canvas"
      data-scene={scene}
      aria-hidden="true"
    />
  );
};

export default TutorialSceneCanvas;
