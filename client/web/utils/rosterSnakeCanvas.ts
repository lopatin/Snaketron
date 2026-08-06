export type RosterSnakeFacing = 'left' | 'right';

export interface RosterSnakeDrawInput {
  width: number;
  height: number;
  facing: RosterSnakeFacing;
  name: string;
  fill: string;
  outline: string;
  labelColor: string;
  fontFamily?: string;
}

export interface RosterSnakeDrawPlan {
  width: number;
  height: number;
  facing: RosterSnakeFacing;
  fill: string;
  outline: string;
  body: {
    tailX: number;
    headX: number;
    centerY: number;
    outerWidth: number;
    innerWidth: number;
  };
  head: {
    centerX: number;
    centerY: number;
    outerRadius: number;
    innerRadius: number;
    faceRadius: number;
  };
  highlight: {
    startX: number;
    endX: number;
    y: number;
    width: number;
  };
  label: {
    text: string;
    x: number;
    y: number;
    maxWidth: number;
    color: string;
    shadowColor: string;
    font: string;
  };
}

const finitePositive = (value: number, fallback: number): number => (
  Number.isFinite(value) && value > 0 ? value : fallback
);

const relativeLuminance = (hex: string): number => {
  const normalized = hex.replace('#', '');
  if (!/^[\da-f]{6}$/i.test(normalized)) {
    return 1;
  }
  const channels = [0, 2, 4].map((offset) => {
    const value = Number.parseInt(normalized.slice(offset, offset + 2), 16) / 255;
    return value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4;
  });
  return channels[0] * 0.2126 + channels[1] * 0.7152 + channels[2] * 0.0722;
};

export const getRosterSnakeLabelColor = (fill: string): string => {
  const fillLuminance = relativeLuminance(fill);
  // The deeper slate clears WCAG AA even on the darkest authored red skin at
  // the roster's small label size; white cannot clear 4.5:1 on these mid-tone
  // team colors.
  const darkInk = '#0f172a';
  const darkContrast = (fillLuminance + 0.05) / (relativeLuminance(darkInk) + 0.05);
  const lightContrast = 1.05 / (fillLuminance + 0.05);
  return darkContrast >= lightContrast ? darkInk : '#ffffff';
};

export const createRosterSnakeDrawPlan = (
  input: RosterSnakeDrawInput,
): RosterSnakeDrawPlan => {
  const width = finitePositive(input.width, 1);
  const height = finitePositive(input.height, 1);
  const diameter = Math.min(width, height);
  const outerRadius = diameter / 2;
  const outlineThickness = diameter <= 17.5 ? 1.5 : 2;
  const innerWidth = Math.max(1, diameter - outlineThickness * 2);
  const innerRadius = innerWidth / 2;
  const faceRadius = innerWidth * 0.38;
  const centerY = height / 2;
  const isFacingRight = input.facing === 'right';
  const tailX = isFacingRight ? outerRadius : width - outerRadius;
  const headX = isFacingRight ? width - outerRadius : outerRadius;
  const direction = isFacingRight ? 1 : -1;

  // Keep text clear of both rounded caps and the arena-style dark head core.
  // A restrained eight-percent bias toward the head makes the two snakes read
  // as a staredown without pinning names against the face.
  const tailTextEdge = tailX + direction * Math.max(4, innerRadius * 0.62);
  const headTextEdge = headX - direction * (faceRadius + 2);
  const labelStart = Math.min(tailTextEdge, headTextEdge);
  const labelEnd = Math.max(tailTextEdge, headTextEdge);
  const labelSpan = Math.max(0, labelEnd - labelStart);
  const labelX = (labelStart + labelEnd) / 2 + direction * labelSpan * 0.08;
  const labelMaxWidth = Math.max(
    0,
    2 * Math.min(labelX - labelStart, labelEnd - labelX),
  );
  const fontSize = diameter <= 17.5 ? 7.5 : 8.5;
  const fontFamily = input.fontFamily?.trim()
    || '-apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif';
  const highlightInset = Math.max(outerRadius * 0.72, 5);

  return {
    width,
    height,
    facing: input.facing,
    fill: input.fill,
    outline: input.outline,
    body: {
      tailX,
      headX,
      centerY,
      outerWidth: diameter,
      innerWidth,
    },
    head: {
      centerX: headX,
      centerY,
      outerRadius,
      innerRadius,
      faceRadius,
    },
    highlight: {
      startX: tailX + direction * highlightInset,
      endX: headX - direction * highlightInset,
      y: centerY - innerRadius * 0.48,
      width: Math.max(0.6, diameter * 0.045),
    },
    label: {
      text: input.name,
      x: labelX,
      y: centerY + 0.25,
      maxWidth: labelMaxWidth,
      color: input.labelColor,
      shadowColor: input.labelColor.toLowerCase() === '#ffffff'
        ? 'rgb(23 32 51 / 34%)'
        : 'rgb(255 255 255 / 38%)',
      font: `900 ${fontSize}px ${fontFamily}`,
    },
  };
};

export const truncateRosterSnakeName = (
  name: string,
  maxWidth: number,
  measure: (candidate: string) => number,
): string => {
  if (maxWidth <= 0) return '';
  if (measure(name) <= maxWidth) return name;

  const ellipsis = '…';
  if (measure(ellipsis) > maxWidth) return '';

  const characters = Array.from(name);
  let low = 0;
  let high = characters.length;
  while (low < high) {
    const middle = Math.ceil((low + high) / 2);
    const candidate = `${characters.slice(0, middle).join('').trimEnd()}${ellipsis}`;
    if (measure(candidate) <= maxWidth) {
      low = middle;
    } else {
      high = middle - 1;
    }
  }
  return `${characters.slice(0, low).join('').trimEnd()}${ellipsis}`;
};

export const drawRosterSnakePlan = (
  context: CanvasRenderingContext2D,
  plan: RosterSnakeDrawPlan,
): void => {
  const { body, head, highlight, label } = plan;

  context.save();
  context.lineCap = 'round';
  context.lineJoin = 'round';

  context.beginPath();
  context.moveTo(body.tailX, body.centerY);
  context.lineTo(body.headX, body.centerY);
  context.lineWidth = body.outerWidth;
  context.strokeStyle = plan.outline;
  context.stroke();

  context.beginPath();
  context.moveTo(body.tailX, body.centerY);
  context.lineTo(body.headX, body.centerY);
  context.lineWidth = body.innerWidth;
  context.strokeStyle = plan.fill;
  context.stroke();

  context.beginPath();
  context.moveTo(highlight.startX, highlight.y);
  context.lineTo(highlight.endX, highlight.y);
  context.lineWidth = highlight.width;
  context.strokeStyle = 'rgb(255 255 255 / 34%)';
  context.stroke();

  context.beginPath();
  context.arc(head.centerX, head.centerY, head.faceRadius, 0, Math.PI * 2);
  context.fillStyle = '#333333';
  context.fill();
  context.lineWidth = Math.max(0.6, body.outerWidth * 0.04);
  context.strokeStyle = 'rgb(255 255 255 / 18%)';
  context.stroke();

  context.font = label.font;
  context.textAlign = 'center';
  context.textBaseline = 'middle';
  const visibleName = truncateRosterSnakeName(
    label.text,
    label.maxWidth,
    candidate => context.measureText(candidate).width,
  );
  if (visibleName) {
    context.fillStyle = label.color;
    context.shadowColor = label.shadowColor;
    context.shadowBlur = 0;
    context.shadowOffsetX = 0;
    context.shadowOffsetY = 1;
    context.fillText(visibleName, label.x, label.y);
  }
  context.restore();
};

export const drawRosterSnakeCanvas = (
  canvas: HTMLCanvasElement,
  input: Omit<RosterSnakeDrawInput, 'width' | 'height'>,
  devicePixelRatio = 1,
): RosterSnakeDrawPlan | null => {
  const bounds = canvas.getBoundingClientRect();
  const width = bounds.width || canvas.clientWidth;
  const height = bounds.height || canvas.clientHeight;
  if (width <= 0 || height <= 0) return null;

  const ratio = finitePositive(devicePixelRatio, 1);
  const pixelWidth = Math.max(1, Math.round(width * ratio));
  const pixelHeight = Math.max(1, Math.round(height * ratio));
  if (canvas.width !== pixelWidth) canvas.width = pixelWidth;
  if (canvas.height !== pixelHeight) canvas.height = pixelHeight;

  const context = canvas.getContext('2d');
  if (!context) return null;
  context.setTransform(1, 0, 0, 1, 0, 0);
  context.clearRect(0, 0, pixelWidth, pixelHeight);
  context.setTransform(pixelWidth / width, 0, 0, pixelHeight / height, 0, 0);

  const plan = createRosterSnakeDrawPlan({ ...input, width, height });
  drawRosterSnakePlan(context, plan);
  return plan;
};
