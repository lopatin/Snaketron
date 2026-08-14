import assert from 'node:assert/strict';
import test from 'node:test';
import { AdBreakResolutionOutbox } from '../../services/ads/adBreakOutbox.ts';
import { resolveAdAttemptBeforeDeadline } from '../../services/ads/adBreakCoordinator.ts';
import { bannerRetryDelayMs } from '../../services/ads/bannerLifecycle.ts';
import {
  bottomBannerReservePx,
  isDurableBannerRoute,
  planBannerPlacements,
} from '../../services/ads/bannerPlan.ts';
import {
  normalizeAdConfiguration,
  normalizeLobbyAdBreak,
} from '../../services/ads/config.ts';
import {
  CrazyGamesAdProvider,
  mapCrazyGamesBannerResult,
  mapCrazyGamesVideoResult,
} from '../../services/ads/providers/crazyGamesAdProvider.ts';
import type { AdProviderCapabilities } from '../../services/ads/types.ts';
import type { AdBreakResolution } from '../../types/generated/AdBreakResolution.ts';

const threeBannerProvider: AdProviderCapabilities = {
  video: true,
  videoAdmissionLeadMs: 0,
  banners: true,
  maxConcurrentBanners: 3,
  bannersDuringGameplay: false,
};

const enabledConfig = normalizeAdConfiguration({
  enabled: true,
  provider: 'CrazyGames',
  banners: { bottom: true, sides: true },
  video: { pre_match: true },
});

test('ad configuration fails closed and the global server switch wins', () => {
  assert.deepEqual(normalizeAdConfiguration(undefined), {
    enabled: false,
    provider: 'none',
    banners: { bottom: false, sides: false },
    video: { pre_match: false },
  });
  assert.deepEqual(normalizeAdConfiguration({
    enabled: false,
    provider: 'CrazyGames',
    banners: { bottom: true, sides: true },
    video: { pre_match: true },
  }), {
    enabled: false,
    provider: 'crazygames',
    banners: { bottom: false, sides: false },
    video: { pre_match: false },
  });
  assert.equal(enabledConfig.provider, 'crazygames');
});

test('lobby ad break normalization preserves reconnect-safe resolved users', () => {
  assert.deepEqual(normalizeLobbyAdBreak({
    id: 'break-1',
    expires_at_ms: 1_900_000_000_000,
    participant_count: 3,
    resolved_count: 2,
    resolved_user_ids: [8, '9', 8, -1, 'bad'],
    ad_user_ids: [8, '10', 8, -1, 'bad'],
  }), {
    id: 'break-1',
    expires_at_ms: 1_900_000_000_000,
    participant_count: 3,
    resolved_count: 2,
    resolved_user_ids: [8, 9],
    ad_user_ids: [8, 10],
  });
  assert.equal(normalizeLobbyAdBreak({ id: '', expires_at_ms: 123 }), null);
});

test('the ad resolution outbox is idempotent until server confirmation', () => {
  const outbox = new AdBreakResolutionOutbox();
  const first = outbox.enqueue('break-1', 'blocked');
  const duplicate = outbox.enqueue('break-1', 'completed');
  assert.equal(duplicate, first);
  assert.equal(outbox.get('break-1')?.resolution, 'blocked');

  outbox.markAttempt('break-1', 100);
  outbox.markAttempt('break-1', 200);
  assert.deepEqual(outbox.get('break-1'), {
    breakId: 'break-1',
    userId: null,
    resolution: 'blocked',
    attempts: 2,
    lastAttemptAt: 200,
  });

  outbox.confirm('break-1');
  assert.equal(outbox.get('break-1'), null);
});

test('terminal outcomes survive reload without crossing user identities', () => {
  const values = new Map<string, string>();
  const storage = {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => { values.set(key, value); },
    removeItem: (key: string) => { values.delete(key); },
  };

  const beforeReload = new AdBreakResolutionOutbox(storage);
  beforeReload.enqueue('break-reload', 'completed', 17);

  const afterReload = new AdBreakResolutionOutbox(storage);
  assert.equal(afterReload.get('break-reload', 17)?.resolution, 'completed');
  assert.equal(afterReload.get('break-reload', 18), null);

  afterReload.confirm('break-reload');
  assert.equal(new AdBreakResolutionOutbox(storage).get('break-reload', 17), null);
});

test('the lobby deadline is an SDK admission cutoff, not a playback timeout', async () => {
  let now = 100;
  let attempted = false;
  assert.equal(await resolveAdAttemptBeforeDeadline(100, async () => {
    attempted = true;
    return 'completed';
  }, 0, () => now), 'timed_out');
  assert.equal(attempted, false, 'an expired break does not initialize an SDK');

  now = 10;
  assert.equal(await resolveAdAttemptBeforeDeadline(100, async (admission) => {
    assert.equal(admission.isOpen(), true);
    now = 100;
    return admission.isOpen() ? 'completed' : 'timed_out';
  }, 0, () => now), 'timed_out', 'expiry during preflight fails open before submission');

  now = 10;
  let finishAttempt: ((resolution: AdBreakResolution) => void) | null = null;
  let settled = false;
  const admittedAttempt = resolveAdAttemptBeforeDeadline(100, async (admission) => {
    assert.equal(admission.isOpen(), true);
    return new Promise<AdBreakResolution>((resolve) => {
      finishAttempt = resolve;
    });
  }, 0, () => now).then((resolution) => {
    settled = true;
    return resolution;
  });
  await Promise.resolve();
  now = 1_000;
  await Promise.resolve();
  assert.equal(settled, false, 'an admitted SDK request is not synthetically completed');
  assert.ok(finishAttempt);
  finishAttempt('completed');
  assert.equal(await admittedAttempt, 'completed');
});

test('provider admission lead rejects short server windows before SDK preflight', async () => {
  let attempted = false;
  assert.equal(await resolveAdAttemptBeforeDeadline(80_000, async () => {
    attempted = true;
    return 'completed';
  }, 90_000, () => 0), 'timed_out');
  assert.equal(attempted, false);

  let now = 29_999;
  assert.equal(await resolveAdAttemptBeforeDeadline(120_000, async (admission) => {
    assert.equal(admission.isOpen(), true);
    now = 30_000;
    return admission.isOpen() ? 'completed' : 'timed_out';
  }, 90_000, () => now), 'timed_out', 'the lead remains enforced through preflight');

  const provider = new CrazyGamesAdProvider();
  assert.equal(provider.capabilities.videoAdmissionLeadMs, 90_000);
});

test('the CrazyGames provider refuses an expired admission before touching the SDK', async () => {
  const provider = new CrazyGamesAdProvider();
  assert.deepEqual(await provider.playInterstitial({
    placement: 'pre-match',
    admission: { isOpen: () => false },
  }), {
    resolution: 'timed_out',
    code: 'admission_expired',
  });
});

test('banner planning is mobile-bottom-only and desktop-provider-aware', () => {
  assert.deepEqual(planBannerPlacements({
    viewportWidth: 390,
    viewportHeight: 844,
    config: enabledConfig,
    capabilities: threeBannerProvider,
    isScreenEligible: true,
    isGameplayActive: false,
  }), [{ slot: 'bottom', width: 320, height: 50 }]);

  assert.deepEqual(planBannerPlacements({
    viewportWidth: 1_440,
    viewportHeight: 900,
    config: enabledConfig,
    capabilities: threeBannerProvider,
    isScreenEligible: true,
    isGameplayActive: false,
  }).map(({ slot }) => slot), ['bottom', 'left-rail', 'right-rail']);

  assert.deepEqual(planBannerPlacements({
    viewportWidth: 1_440,
    viewportHeight: 900,
    config: enabledConfig,
    capabilities: { ...threeBannerProvider, maxConcurrentBanners: 2 },
    isScreenEligible: true,
    isGameplayActive: false,
  }).map(({ slot }) => slot), ['bottom', 'left-rail']);

  assert.deepEqual(planBannerPlacements({
    viewportWidth: 1_440,
    viewportHeight: 700,
    config: enabledConfig,
    capabilities: threeBannerProvider,
    isScreenEligible: true,
    isGameplayActive: false,
  }).map(({ slot }) => slot), ['bottom']);
});

test('only the bottom banner reserves route layout space', () => {
  assert.equal(bottomBannerReservePx([
    { slot: 'left-rail', width: 160, height: 600 },
    { slot: 'right-rail', width: 160, height: 600 },
  ]), 0, 'floating desktop rails do not narrow or offset page content');

  assert.equal(bottomBannerReservePx([
    { slot: 'bottom', width: 970, height: 90 },
    { slot: 'left-rail', width: 160, height: 600 },
    { slot: 'right-rail', width: 160, height: 600 },
  ]), 114, 'the bottom banner and its chrome reserve a row');
});

test('providers can prohibit banners during gameplay', () => {
  assert.deepEqual(planBannerPlacements({
    viewportWidth: 1_440,
    viewportHeight: 900,
    config: enabledConfig,
    capabilities: threeBannerProvider,
    isScreenEligible: true,
    isGameplayActive: true,
  }), []);
});

test('banners are limited to durable screens with the shared reserve contract', () => {
  assert.equal(isDurableBannerRoute('/'), true);
  assert.equal(isDurableBannerRoute('/leaderboards'), true);
  assert.equal(isDurableBannerRoute('/leaderboards/'), true);
  assert.equal(isDurableBannerRoute('/lobby/USE1-ABC'), false);
  assert.equal(isDurableBannerRoute('/game/ABC123'), false);
  assert.equal(isDurableBannerRoute('/privacy'), false);

  assert.deepEqual(planBannerPlacements({
    viewportWidth: 1_440,
    viewportHeight: 900,
    config: enabledConfig,
    capabilities: threeBannerProvider,
    isScreenEligible: false,
    isGameplayActive: false,
  }), []);
});

test('CrazyGames banner outcomes preserve cooldowns as retryable provider state', () => {
  const cooldown = mapCrazyGamesBannerResult({
    status: 'error',
    error: { code: 'bannerCooldown', message: 'wait before refreshing' },
  });
  assert.deepEqual(cooldown, {
    status: 'retryable',
    code: 'bannercooldown',
    retryAfterMs: 30_000,
  });
  assert.equal(bannerRetryDelayMs(cooldown), 30_000);
  assert.equal(bannerRetryDelayMs({ status: 'retryable', retryAfterMs: 50 }), 1_000);
  assert.equal(bannerRetryDelayMs({ status: 'unavailable', code: 'unfilled' }), null);
  assert.deepEqual(mapCrazyGamesBannerResult({ status: 'disabled' }), {
    status: 'unavailable',
    code: 'sdk_unavailable',
  });
});

test('CrazyGames video results fail open with provider-neutral resolutions', () => {
  assert.deepEqual(mapCrazyGamesVideoResult({ status: 'finished' }), { resolution: 'completed' });
  assert.deepEqual(mapCrazyGamesVideoResult({
    status: 'error',
    error: { code: 'adblock', message: 'blocked' },
  }), { resolution: 'blocked', code: 'adblock' });
  assert.deepEqual(mapCrazyGamesVideoResult({
    status: 'error',
    error: { code: 'unfilled', message: 'no fill' },
  }), { resolution: 'unavailable', code: 'unfilled' });
  assert.deepEqual(mapCrazyGamesVideoResult({
    status: 'error',
    error: { code: 'requestTimeout', message: 'late' },
  }), { resolution: 'timed_out', code: 'requesttimeout' });
  assert.deepEqual(mapCrazyGamesVideoResult({ status: 'disabled' }), {
    resolution: 'unavailable',
    code: 'sdk_unavailable',
  });
});
