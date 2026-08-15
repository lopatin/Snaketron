import type { ClientAdsConfig } from '../../types/generated';
import type {
  AdBannerSlot,
  AdProviderCapabilities,
} from './types';

export interface BannerPlacement {
  slot: AdBannerSlot;
  width: number;
  height: number;
}

export interface BannerPlanInput {
  viewportWidth: number;
  viewportHeight: number;
  config: ClientAdsConfig;
  capabilities: AdProviderCapabilities;
  isScreenEligible: boolean;
  isGameplayActive: boolean;
}

const BOTTOM_BANNER_CHROME_HEIGHT = 24;

const DURABLE_BANNER_ROUTES = new Set(['/', '/leaderboards']);

/**
 * Product-owned placement safety, independent from the server's global switch.
 * Only stable, useful screens with the shared reserve layout may host banners.
 */
export const isDurableBannerRoute = (pathname: string): boolean => {
  const trimmed = pathname.trim();
  const normalized = trimmed.length > 1 ? trimmed.replace(/\/+$/, '') : trimmed;
  return DURABLE_BANNER_ROUTES.has(normalized || '/');
};

/**
 * Only the bottom placement participates in document layout. Desktop rails
 * are fixed viewport overlays and must never narrow or offset page content.
 */
export const bottomBannerReservePx = (placements: BannerPlacement[]): number => {
  const bottom = placements.find(({ slot }) => slot === 'bottom');
  return bottom ? bottom.height + BOTTOM_BANNER_CHROME_HEIGHT : 0;
};

const bottomSize = (viewportWidth: number): { width: number; height: number } => {
  if (viewportWidth >= 970) return { width: 970, height: 90 };
  if (viewportWidth >= 728) return { width: 728, height: 90 };
  if (viewportWidth >= 468) return { width: 468, height: 60 };
  return { width: 320, height: 50 };
};

export const planBannerPlacements = ({
  viewportWidth,
  viewportHeight,
  config,
  capabilities,
  isScreenEligible,
  isGameplayActive,
}: BannerPlanInput): BannerPlacement[] => {
  if (
    !isScreenEligible ||
    !config.enabled ||
    !capabilities.banners ||
    capabilities.maxConcurrentBanners <= 0 ||
    (isGameplayActive && !capabilities.bannersDuringGameplay)
  ) {
    return [];
  }

  const placements: BannerPlacement[] = [];
  if (config.banners.bottom && viewportWidth >= 320) {
    placements.push({ slot: 'bottom', ...bottomSize(viewportWidth) });
  }

  const canShowRails = config.banners.sides && viewportWidth >= 1_000 && viewportHeight >= 760;
  if (canShowRails) {
    const width = viewportWidth >= 1_280 ? 160 : 120;
    placements.push(
      { slot: 'left-rail', width, height: 600 },
      { slot: 'right-rail', width, height: 600 },
    );
  }

  return placements.slice(0, capabilities.maxConcurrentBanners);
};
