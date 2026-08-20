import type {
  AdBannerOutcome,
  AdBannerRequest,
  AdBlockStatus,
  AdProvider,
  AdVideoOutcome,
  AdVideoRequest,
} from '../types';

export class NullAdProvider implements AdProvider {
  readonly id: string;
  readonly capabilities = {
    video: false,
    videoAdmissionLeadMs: 0,
    banners: false,
    maxConcurrentBanners: 0,
    bannersDuringGameplay: false,
  } as const;

  constructor(id = 'none') {
    this.id = id;
  }

  async initialize(): Promise<void> {}

  async getAdBlockStatus(): Promise<AdBlockStatus> {
    return 'unknown';
  }

  async playInterstitial(_request: AdVideoRequest): Promise<AdVideoOutcome> {
    return { resolution: 'unavailable', code: 'provider_unavailable' };
  }

  async mountBanner(_request: AdBannerRequest): Promise<AdBannerOutcome> {
    return { status: 'unavailable', code: 'provider_unavailable' };
  }

  clearBanner(_containerId: string): void {}

  dispose(): void {}
}
