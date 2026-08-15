/**
 * The home screen's drifting dot field, extracted so anything that needs
 * SnakeTron's signature background renders the *same* field rather than an
 * approximation.
 *
 * `ArenaBackdrop` drives it from rAF for the live site. Trailer capture drives
 * it from a virtual clock at a higher `intensity`, because the site tunes the
 * field to sit quietly behind copy while a title card wants it to read as a
 * deliberate visual.
 *
 * Time is always a parameter — this module never reads a clock — so a captured
 * frame at t is bit-identical to the same t on any other run.
 */

const clamp = (value: number, min: number, max: number): number =>
  Math.min(max, Math.max(min, value));

const smoothstep = (edgeStart: number, edgeEnd: number, value: number): number => {
  const progress = clamp((value - edgeStart) / (edgeEnd - edgeStart), 0, 1);
  return progress * progress * (3 - 2 * progress);
};

export interface FlowFieldColor {
  red: number;
  green: number;
  blue: number;
}

export const FLOW_FIELD_INK: FlowFieldColor = { red: 71, green: 78, blue: 90 };
export const FLOW_FIELD_SKY: FlowFieldColor = { red: 91, green: 184, blue: 224 };
export const FLOW_FIELD_CORAL: FlowFieldColor = { red: 246, green: 112, blue: 123 };

const mixColor = (
  from: FlowFieldColor,
  to: FlowFieldColor,
  amount: number,
): FlowFieldColor => ({
  red: Math.round(from.red + (to.red - from.red) * amount),
  green: Math.round(from.green + (to.green - from.green) * amount),
  blue: Math.round(from.blue + (to.blue - from.blue) * amount),
});

const rgba = (color: FlowFieldColor, alpha: number): string =>
  `rgba(${color.red}, ${color.green}, ${color.blue}, ${alpha})`;

export interface FlowFieldPointer {
  x: number;
  y: number;
  influence: number;
  isInitialized: boolean;
}

export const NEUTRAL_POINTER: FlowFieldPointer = {
  x: 0,
  y: 0,
  influence: 0,
  isInitialized: false,
};

const getQuietZone = (
  x: number,
  y: number,
  width: number,
  height: number,
  quietStrength: number,
): number => {
  const horizontalRadius = width < 720 ? 235 : 340;
  const verticalRadius = height < 720 ? 250 : 360;
  const normalizedDistance = Math.hypot(
    (x - width / 2) / horizontalRadius,
    (y - height / 2) / verticalRadius,
  );
  const quiet = 0.04 + smoothstep(0.58, 1.18, normalizedDistance) * 0.96;
  // quietStrength 1 keeps the site's hollow centre; 0 fills the frame evenly.
  return quiet + (1 - quiet) * (1 - quietStrength);
};

const getPointerEffect = (
  x: number,
  y: number,
  pointer: FlowFieldPointer,
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

export interface FlowFieldOptions {
  context: CanvasRenderingContext2D;
  width: number;
  height: number;
  /** Field time in seconds. Pure input — no clock is read here. */
  time: number;
  spacing?: number;
  pointer?: FlowFieldPointer;
  /** Alpha multiplier. 1 matches the live site; >1 makes the field assertive. */
  intensity?: number;
  /** 1 keeps the site's hollow centre, 0 fills the frame evenly. */
  quietStrength?: number;
}

export const defaultFlowFieldSpacing = (width: number): number =>
  width < 700 ? 20 : 23;

export const drawArenaFlowField = ({
  context,
  width,
  height,
  time,
  spacing = defaultFlowFieldSpacing(width),
  pointer = NEUTRAL_POINTER,
  intensity = 1,
  quietStrength = 1,
}: FlowFieldOptions): void => {
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
      const quietZone = getQuietZone(x, y, width, height, quietStrength);
      const colorBias = clamp((x / width + (1 - y / height)) / 2, 0, 1);
      const accent = colorBias > 0.52 ? FLOW_FIELD_CORAL : FLOW_FIELD_SKY;
      const accentStrength = 0.32 + waveBand * 0.4 + pointerEffect.intensity * 0.09;
      const color = mixColor(FLOW_FIELD_INK, accent, accentStrength);
      const alpha = clamp(
        (0.15 + waveBand * 0.38 + pointerEffect.intensity * 0.085) *
          quietZone *
          intensity,
        0,
        1,
      );
      const radius =
        (0.68 + waveBand * 0.64 + pointerEffect.intensity * 0.22) *
        clamp(1 + (intensity - 1) * 0.35, 0.5, 2.2);

      context.beginPath();
      context.arc(
        x + Math.cos(crossPhase) * 1.8 + pointerEffect.offsetX,
        y + wave * 3.6 + pointerEffect.offsetY,
        radius,
        0,
        Math.PI * 2,
      );
      context.fillStyle = rgba(color, alpha);
      context.fill();
    }
  }
};
