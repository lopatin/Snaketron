import type { AdBreakResolution } from '../../types/generated';
import type { AdRequestAdmission } from './types';

type Clock = () => number;

export const resolveAdAttemptBeforeDeadline = async (
  expiresAtMs: number,
  attempt: (admission: AdRequestAdmission) => Promise<AdBreakResolution>,
  admissionLeadMs: number,
  now: Clock = Date.now,
): Promise<AdBreakResolution> => {
  // Invalid provider budgets fail closed. Negative budgets cannot extend the
  // admission window beyond the authoritative server deadline.
  const requiredLeadMs = Number.isFinite(admissionLeadMs)
    ? Math.max(0, admissionLeadMs)
    : Number.POSITIVE_INFINITY;
  const admission: AdRequestAdmission = {
    isOpen: () => now() < expiresAtMs - requiredLeadMs,
  };

  if (!admission.isOpen()) {
    return 'timed_out';
  }

  // The deadline gates admission, not playback. The attempt must re-check the
  // admission after asynchronous preflight and the provider checks once more
  // immediately before SDK submission. After that boundary, only the SDK's
  // real finished/error callback may resolve the attempt.
  return attempt(admission);
};
