import type { AdBreakResolution } from '../../types/generated';

export type AdBlockStatus = 'blocked' | 'clear' | 'unknown';
export type AdBannerSlot = 'bottom' | 'left-rail' | 'right-rail';

export interface AdProviderCapabilities {
  video: boolean;
  /** Minimum time that must remain before a video request may reach the SDK. */
  videoAdmissionLeadMs: number;
  banners: boolean;
  maxConcurrentBanners: number;
  bannersDuringGameplay: boolean;
}

export interface AdRequestAdmission {
  /**
   * Providers must check this immediately before submitting the request to
   * their SDK. Once submitted, the SDK's terminal callback owns completion.
   */
  isOpen(): boolean;
}

export interface AdVideoRequest {
  placement: 'pre-match';
  admission: AdRequestAdmission;
  onStarted?: () => void;
}

export interface AdVideoOutcome {
  resolution: AdBreakResolution;
  code?: string;
}

export interface AdBannerRequest {
  slot: AdBannerSlot;
  containerId: string;
  width: number;
  height: number;
}

export interface AdBannerOutcome {
  status: 'filled' | 'blocked' | 'unavailable' | 'retryable' | 'error';
  code?: string;
  /** Earliest provider-safe retry. Retryable outcomes keep the neutral slot mounted. */
  retryAfterMs?: number;
}

/**
 * SDK boundary for all browser advertising. Server policy never belongs here:
 * providers only report what this particular build and SDK can do.
 */
export interface AdProvider {
  readonly id: string;
  readonly capabilities: AdProviderCapabilities;
  initialize(): Promise<void>;
  getAdBlockStatus(): Promise<AdBlockStatus>;
  playInterstitial(request: AdVideoRequest): Promise<AdVideoOutcome>;
  mountBanner(request: AdBannerRequest): Promise<AdBannerOutcome>;
  clearBanner(containerId: string): void;
  dispose(): void;
}
