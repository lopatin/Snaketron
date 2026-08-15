import type { AdBannerOutcome } from './types';

const DEFAULT_RETRY_MS = 30_000;
const MIN_RETRY_MS = 1_000;

/** Returns a safe delay only for provider-declared transient outcomes. */
export const bannerRetryDelayMs = (outcome: AdBannerOutcome): number | null => {
  if (outcome.status !== 'retryable') {
    return null;
  }
  const requested = Number(outcome.retryAfterMs);
  return Number.isFinite(requested)
    ? Math.max(MIN_RETRY_MS, requested)
    : DEFAULT_RETRY_MS;
};
