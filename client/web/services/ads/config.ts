import type { ClientAdsConfig, LobbyAdBreakView } from '../../types/generated';

export const DISABLED_AD_CONFIGURATION: Readonly<ClientAdsConfig> = Object.freeze({
  enabled: false,
  provider: 'none',
  banners: Object.freeze({ bottom: false, sides: false }),
  video: Object.freeze({ pre_match: false }),
});

export const normalizeAdConfiguration = (input: unknown): ClientAdsConfig => {
  if (!input || typeof input !== 'object') {
    return { ...DISABLED_AD_CONFIGURATION, banners: { bottom: false, sides: false }, video: { pre_match: false } };
  }

  const raw = input as Record<string, unknown>;
  const enabled = raw.enabled === true;
  const banners = raw.banners && typeof raw.banners === 'object'
    ? raw.banners as Record<string, unknown>
    : {};
  const video = raw.video && typeof raw.video === 'object'
    ? raw.video as Record<string, unknown>
    : {};
  const provider = typeof raw.provider === 'string' && raw.provider.trim()
    ? raw.provider.trim().toLowerCase()
    : 'none';

  // The global switch always wins, including against malformed or stale
  // placement flags from a rolling deployment.
  return {
    enabled,
    provider,
    banners: {
      bottom: enabled && banners.bottom === true,
      sides: enabled && banners.sides === true,
    },
    video: {
      pre_match: enabled && video.pre_match === true,
    },
  };
};

export const normalizeLobbyAdBreak = (input: unknown): LobbyAdBreakView | null => {
  if (!input || typeof input !== 'object') {
    return null;
  }
  const raw = input as Record<string, unknown>;
  const id = typeof raw.id === 'string' ? raw.id.trim() : '';
  const expiresAt = Number(raw.expires_at_ms);
  if (!id || !Number.isFinite(expiresAt)) {
    return null;
  }

  const resolvedUserIds = [...new Set(Array.isArray(raw.resolved_user_ids)
    ? raw.resolved_user_ids
      .map(Number)
      .filter((value): value is number => Number.isSafeInteger(value) && value >= 0)
    : [])];
  const adUserIds = [...new Set(Array.isArray(raw.ad_user_ids)
    ? raw.ad_user_ids
      .map(Number)
      .filter((value): value is number => Number.isSafeInteger(value) && value >= 0)
    : [])];

  const participantCount = Math.max(0, Math.trunc(Number(raw.participant_count) || 0));
  const resolvedCount = Math.max(
    resolvedUserIds.length,
    Math.trunc(Number(raw.resolved_count) || 0),
  );

  return {
    id,
    expires_at_ms: expiresAt,
    participant_count: participantCount,
    resolved_count: Math.min(participantCount || resolvedCount, resolvedCount),
    resolved_user_ids: resolvedUserIds,
    ad_user_ids: adUserIds,
  };
};
