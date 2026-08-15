import {
  crazyGames,
  type CrazyGamesAdError,
  type CrazyGamesAdResult,
  type CrazyGamesBannerResult,
} from '../../crazyGames.ts';
import type {
  AdBannerOutcome,
  AdBannerRequest,
  AdBlockStatus,
  AdProvider,
  AdVideoOutcome,
  AdVideoRequest,
} from '../types';

const normalizedCode = (error?: CrazyGamesAdError): string => (
  typeof error?.code === 'string'
    ? error.code.replace(/[\s_-]+/g, '').toLowerCase()
    : ''
);

const CRAZY_GAMES_BANNER_COOLDOWN_MS = 30_000;

export const mapCrazyGamesVideoResult = (result: CrazyGamesAdResult): AdVideoOutcome => {
  if (result.status === 'finished') {
    return { resolution: 'completed' };
  }
  if (result.status === 'disabled') {
    return { resolution: 'unavailable', code: 'sdk_unavailable' };
  }

  const code = normalizedCode(result.error);
  if (code.includes('adblock')) {
    return { resolution: 'blocked', code: code || 'adblock' };
  }
  if (code.includes('timeout')) {
    return { resolution: 'timed_out', code: code || 'timeout' };
  }
  if (
    code.includes('unfilled') ||
    code.includes('cooldown') ||
    code.includes('disabledbasiclaunch') ||
    code.includes('notavailable')
  ) {
    return { resolution: 'unavailable', code };
  }
  return { resolution: 'error', code: code || 'unknown' };
};

export const mapCrazyGamesBannerResult = (result: CrazyGamesBannerResult): AdBannerOutcome => {
  if (result.status === 'filled') {
    return { status: 'filled' };
  }
  if (result.status === 'disabled') {
    return { status: 'unavailable', code: 'sdk_unavailable' };
  }

  const code = normalizedCode(result.error);
  if (code.includes('bannercooldown')) {
    return {
      status: 'retryable',
      code: code || 'banner_cooldown',
      retryAfterMs: CRAZY_GAMES_BANNER_COOLDOWN_MS,
    };
  }
  if (code.includes('adblock')) {
    return { status: 'blocked', code: code || 'adblock' };
  }
  if (
    code.includes('unfilled') ||
    code.includes('disabledbasiclaunch') ||
    code.includes('disabledmobileapp') ||
    code.includes('maxrefreshreached') ||
    code.includes('noavailablesizes')
  ) {
    return { status: 'unavailable', code };
  }
  return { status: 'error', code: code || 'unknown' };
};

export class CrazyGamesAdProvider implements AdProvider {
  readonly id = 'crazygames';
  readonly capabilities = {
    video: true,
    // Reserve enough of the server window for CrazyGames request lifecycle
    // overhead before handing completion ownership to the SDK callbacks.
    videoAdmissionLeadMs: 90_000,
    banners: true,
    // CrazyGames permits no more than two banners on one screen.
    maxConcurrentBanners: 2,
    bannersDuringGameplay: false,
  } as const;
  private adblockFlight: Promise<AdBlockStatus> | null = null;

  async initialize(): Promise<void> {
    await crazyGames.init();
  }

  async getAdBlockStatus(): Promise<AdBlockStatus> {
    const cached = crazyGames.getSnapshot().hasAdblock;
    if (cached !== null) {
      return cached ? 'blocked' : 'clear';
    }
    if (this.adblockFlight) {
      return this.adblockFlight;
    }
    const flight = crazyGames.detectAdblock().then((blocked) => (
      blocked === true ? 'blocked' : blocked === false ? 'clear' : 'unknown'
    ));
    this.adblockFlight = flight;
    try {
      return await flight;
    } finally {
      if (this.adblockFlight === flight) this.adblockFlight = null;
    }
  }

  async playInterstitial(request: AdVideoRequest): Promise<AdVideoOutcome> {
    if (!request.admission.isOpen()) {
      return { resolution: 'timed_out', code: 'admission_expired' };
    }

    let started = false;
    const unsubscribe = crazyGames.subscribe((snapshot) => {
      if (!started && snapshot.adState === 'playing') {
        started = true;
        request.onStarted?.();
      }
    });
    try {
      return mapCrazyGamesVideoResult(await crazyGames.requestAd('midgame'));
    } finally {
      unsubscribe();
    }
  }

  async mountBanner(request: AdBannerRequest): Promise<AdBannerOutcome> {
    if (await this.getAdBlockStatus() === 'blocked') {
      return { status: 'blocked', code: 'adblock' };
    }
    // The desktop rail and wide leaderboard dimensions are part of the
    // CrazyGames responsive-size set, not its much smaller static-size set.
    // Let the SDK select the fitting creative for the already-sized host.
    return mapCrazyGamesBannerResult(
      await crazyGames.requestResponsiveBanner(request.containerId),
    );
  }

  clearBanner(containerId: string): void {
    crazyGames.clearBanner(containerId);
  }

  dispose(): void {
    crazyGames.clearAllBanners();
  }
}
