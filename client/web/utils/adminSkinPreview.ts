export const DEFAULT_SKIN_PERIOD_MS = 2_000;

export type AdminSkinPreviewState = 'loading' | 'ready' | 'error';

export interface SkinAssetStatus {
  requested: number;
  pending: number;
  ready: number;
  failed: number;
  drawnImages: number;
  drawCalls: number;
}

const finitePositive = (value: unknown): number | null => (
  typeof value === 'number' && Number.isFinite(value) && value > 0 ? value : null
);

/** Read one full animation cycle from either supported SkinDoc version. */
export const skinAnimationPeriodMs = (document: unknown): number => {
  if (!document || typeof document !== 'object') return DEFAULT_SKIN_PERIOD_MS;
  const record = document as Record<string, unknown>;
  const v2Period = finitePositive(record.period_ms);
  if (v2Period !== null) return v2Period;

  const animation = record.animation;
  if (animation && typeof animation === 'object') {
    const v1Period = finitePositive((animation as Record<string, unknown>).period_ms);
    if (v1Period !== null) return v1Period;
  }
  return DEFAULT_SKIN_PERIOD_MS;
};

/** Advance a review scrubber while keeping its value inside one exact cycle. */
export const advanceSkinTimeline = (
  currentMs: number,
  elapsedMs: number,
  periodMs: number,
): number => {
  const period = finitePositive(periodMs) ?? DEFAULT_SKIN_PERIOD_MS;
  const next = currentMs + Math.max(0, elapsedMs);
  return ((next % period) + period) % period;
};

/** A queue may hold dozens of skins, so motion begins only on explicit review. */
export const initialSkinPreviewPlaying = (): boolean => false;

/** Name the operation precisely when the pending bytes amend a published skin. */
export const adminRejectActionLabel = (publishedRevision: number | null): string => (
  publishedRevision === null ? 'Reject draft' : 'Reject pending edit'
);

export const adminPublishDisabled = (
  busy: boolean,
  previewState: AdminSkinPreviewState | undefined,
): boolean => busy || previewState !== 'ready';

export const adminPreviewTargetKey = (
  skinId: number,
  revision: number,
  contentRef: string,
): string => `${skinId}:${revision}:${contentRef}`;

/** Image layers must prove their decoded pixels reached the exact preview canvas. */
export const skinDocumentUsesImages = (document: unknown): boolean => {
  if (!document || typeof document !== 'object') return false;
  const record = document as Record<string, unknown>;
  if (record.type === 'image') return true;
  return Object.values(record).some((value) => {
    if (Array.isArray(value)) return value.some(skinDocumentUsesImages);
    return skinDocumentUsesImages(value);
  });
};

/** Return why a painted preview is unsafe to approve, or null when it is evidence. */
export const skinPreviewAssetError = (
  status: SkinAssetStatus,
  usesImages: boolean,
  drawCallsBeforePaint: number,
): string | null => {
  if (!usesImages) return null;
  if (status.pending > 0) return 'Texture loading timed out.';
  if (status.failed > 0) return 'A texture could not be loaded or decoded.';
  if (status.drawCalls > drawCallsBeforePaint) return null;
  return 'Decoded texture pixels never reached the preview canvas.';
};

export const shortContentRef = (contentRef: string): string => {
  const digest = contentRef.startsWith('sha256:') ? contentRef.slice(7) : contentRef;
  return `sha256:${digest.slice(0, 8)}…${digest.slice(-8)}`;
};
