import React, { useEffect, useRef } from 'react';

const clamp = (value: number, min: number, max: number): number =>
  Math.min(max, Math.max(min, value));

const smoothstep = (edgeStart: number, edgeEnd: number, value: number): number => {
  const progress = clamp((value - edgeStart) / (edgeEnd - edgeStart), 0, 1);
  return progress * progress * (3 - 2 * progress);
};

interface Color {
  red: number;
  green: number;
  blue: number;
}

const FRAME_INTERVAL_MS = 1000 / 6;

const INK: Color = { red: 71, green: 78, blue: 90 };
const SKY: Color = { red: 91, green: 184, blue: 224 };
const CORAL: Color = { red: 246, green: 112, blue: 123 };

const mixColor = (from: Color, to: Color, amount: number): Color => ({
  red: Math.round(from.red + (to.red - from.red) * amount),
  green: Math.round(from.green + (to.green - from.green) * amount),
  blue: Math.round(from.blue + (to.blue - from.blue) * amount),
});

const rgba = (color: Color, alpha: number): string =>
  `rgba(${color.red}, ${color.green}, ${color.blue}, ${alpha})`;

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

interface DrawContext {
  context: CanvasRenderingContext2D;
  width: number;
  height: number;
  time: number;
  pointer: PointerPosition;
  spacing: number;
}

const getQuietZone = (x: number, y: number, width: number, height: number): number => {
  const horizontalRadius = width < 720 ? 235 : 340;
  const verticalRadius = height < 720 ? 250 : 360;
  const normalizedDistance = Math.hypot(
    (x - width / 2) / horizontalRadius,
    (y - height / 2) / verticalRadius,
  );

  return 0.04 + smoothstep(0.58, 1.18, normalizedDistance) * 0.96;
};

const getPointerEffect = (
  x: number,
  y: number,
  pointer: PointerPosition,
  influenceRadius: number,
): { offsetX: number; offsetY: number; phaseShift: number; intensity: number } => {
  if (!pointer.isInitialized || pointer.influence < 0.001) {
    return { offsetX: 0, offsetY: 0, phaseShift: 0, intensity: 0 };
  }

  const deltaX = x - pointer.x;
  const deltaY = y - pointer.y;
  const distance = Math.hypot(deltaX, deltaY);
  const safeDistance = Math.max(distance, 1);
  const gaussianFalloff = Math.exp(
    -(distance * distance) / (2 * influenceRadius * influenceRadius),
  );
  const intensity = pointer.influence * gaussianFalloff;
  const tangentialDrift = Math.min(distance / influenceRadius, 1.4) * intensity * 2.1;

  return {
    offsetX: (-deltaY / safeDistance) * tangentialDrift,
    offsetY: (deltaX / safeDistance) * tangentialDrift,
    phaseShift: intensity * 0.28,
    intensity,
  };
};

const drawDot = (
  context: CanvasRenderingContext2D,
  x: number,
  y: number,
  radius: number,
  color: Color,
  alpha: number,
): void => {
  context.beginPath();
  context.arc(x, y, radius, 0, Math.PI * 2);
  context.fillStyle = rgba(color, alpha);
  context.fill();
};

const drawFlowField = ({
  context,
  width,
  height,
  time,
  pointer,
  spacing,
}: DrawContext): void => {
  const pointerInfluenceRadius = clamp(Math.min(width, height) * 0.42, 260, 380);

  for (let y = spacing / 2; y < height; y += spacing) {
    for (let x = spacing / 2; x < width; x += spacing) {
      const pointerEffect = getPointerEffect(x, y, pointer, pointerInfluenceRadius);
      const diagonalPhase =
        x * 0.009 + y * 0.006 - time * 0.52 + pointerEffect.phaseShift;
      const crossPhase =
        x * 0.003 - y * 0.011 + time * 0.28 - pointerEffect.phaseShift * 0.55;
      const wave = Math.sin(diagonalPhase) * 0.68 + Math.cos(crossPhase) * 0.32;
      const waveBand = Math.pow(clamp(0.5 + wave * 0.5, 0, 1), 2);
      const quietZone = getQuietZone(x, y, width, height);
      const colorBias = clamp((x / width + (1 - y / height)) / 2, 0, 1);
      const accent = colorBias > 0.52 ? CORAL : SKY;
      const accentStrength = 0.32 + waveBand * 0.4 + pointerEffect.intensity * 0.09;
      const color = mixColor(INK, accent, accentStrength);
      const alpha = (0.15 + waveBand * 0.38 + pointerEffect.intensity * 0.085) * quietZone;
      const radius = 0.68 + waveBand * 0.64 + pointerEffect.intensity * 0.22;

      drawDot(
        context,
        x + Math.cos(crossPhase) * 1.8 + pointerEffect.offsetX,
        y + wave * 3.6 + pointerEffect.offsetY,
        radius,
        color,
        alpha,
      );
    }
  }
};

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
      drawFlowField({
        context,
        width,
        height,
        time: reducedMotion ? 0 : timestamp / 1000,
        pointer,
        spacing: width < 700 ? 20 : 23,
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
