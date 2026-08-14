import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useAds } from '../contexts/AdsContext';
import { bannerRetryDelayMs } from '../services/ads/bannerLifecycle';
import {
  planBannerPlacements,
  type BannerPlacement,
} from '../services/ads/bannerPlan';
import type { AdBannerSlot, AdProvider } from '../services/ads/types';

interface AdBannerLayoutProps {
  isGameplayActive: boolean;
  isScreenEligible: boolean;
}

type BannerMountStatus = 'loading' | 'filled' | 'empty';
type SlotVisibility = Partial<Record<AdBannerSlot, {
  signature: string;
  visible: boolean;
}>>;

const slotId = (slot: BannerPlacement['slot']): string => `snaketron-ad-${slot}`;
const placementSignature = ({ slot, width, height }: BannerPlacement): string => (
  `${slot}:${width}x${height}`
);

const useViewport = (): { width: number; height: number } => {
  const [viewport, setViewport] = useState(() => ({
    width: typeof window === 'undefined' ? 0 : window.innerWidth,
    height: typeof window === 'undefined' ? 0 : window.innerHeight,
  }));

  useEffect(() => {
    let frame: number | null = null;
    const update = () => {
      if (frame !== null) return;
      frame = window.requestAnimationFrame(() => {
        frame = null;
        setViewport({ width: window.innerWidth, height: window.innerHeight });
      });
    };
    window.addEventListener('resize', update);
    return () => {
      window.removeEventListener('resize', update);
      if (frame !== null) window.cancelAnimationFrame(frame);
    };
  }, []);

  return viewport;
};

const BannerHost: React.FC<{ placement: BannerPlacement }> = ({ placement }) => (
  <aside
    className={`ad-banner-slot ad-banner-slot--${placement.slot}`}
    aria-label="Advertisement"
    data-ad-slot={placement.slot}
  >
    <span className="ad-banner-label">Advertisement</span>
    <div
      id={slotId(placement.slot)}
      className="ad-banner-host"
      style={{ width: placement.width, height: placement.height }}
      data-testid={`ad-banner-${placement.slot}`}
    >
      <span className="ad-banner-placeholder" aria-hidden="true">Sponsor space</span>
    </div>
  </aside>
);

const ManagedBannerHost: React.FC<{
  placement: BannerPlacement;
  provider: AdProvider;
  providerReady: boolean;
  onVisibilityChange: (slot: AdBannerSlot, signature: string, visible: boolean) => void;
}> = ({ placement, provider, providerReady, onVisibilityChange }) => {
  const signature = placementSignature(placement);
  const [mount, setMount] = useState<{
    signature: string;
    status: BannerMountStatus;
  }>({ signature, status: 'loading' });
  const status = mount.signature === signature ? mount.status : 'loading';

  useEffect(() => {
    let disposed = false;
    let retryTimer: number | null = null;
    let requestGeneration = 0;

    setMount({ signature, status: 'loading' });
    onVisibilityChange(placement.slot, signature, true);
    if (!providerReady) {
      return;
    }

    const request = async () => {
      const generation = ++requestGeneration;
      setMount({ signature, status: 'loading' });
      onVisibilityChange(placement.slot, signature, true);

      try {
        const outcome = await provider.mountBanner({
          ...placement,
          containerId: slotId(placement.slot),
        });
        if (disposed || generation !== requestGeneration) return;

        if (outcome.status === 'filled') {
          setMount({ signature, status: 'filled' });
          return;
        }

        const retryAfterMs = bannerRetryDelayMs(outcome);
        if (retryAfterMs !== null) {
          retryTimer = window.setTimeout(() => {
            retryTimer = null;
            void request();
          }, retryAfterMs);
          return;
        }

        provider.clearBanner(slotId(placement.slot));
        setMount({ signature, status: 'empty' });
        onVisibilityChange(placement.slot, signature, false);
      } catch (error) {
        if (disposed || generation !== requestGeneration) return;
        console.info(`Banner ${placement.slot} unavailable:`, error);
        provider.clearBanner(slotId(placement.slot));
        setMount({ signature, status: 'empty' });
        onVisibilityChange(placement.slot, signature, false);
      }
    };

    void request();
    return () => {
      disposed = true;
      requestGeneration += 1;
      if (retryTimer !== null) window.clearTimeout(retryTimer);
      provider.clearBanner(slotId(placement.slot));
    };
  }, [
    onVisibilityChange,
    placement.height,
    placement.slot,
    placement.width,
    provider,
    providerReady,
    signature,
  ]);

  return status === 'empty' ? null : <BannerHost placement={placement} />;
};

export const AdBannerLayout: React.FC<AdBannerLayoutProps> = ({
  isGameplayActive,
  isScreenEligible,
}) => {
  const { capabilities, config, provider, providerReady } = useAds();
  const viewport = useViewport();
  const placements = useMemo(() => planBannerPlacements({
    viewportWidth: viewport.width,
    viewportHeight: viewport.height,
    config,
    capabilities,
    isScreenEligible,
    isGameplayActive,
  }), [
    capabilities,
    config,
    isGameplayActive,
    isScreenEligible,
    viewport.height,
    viewport.width,
  ]);
  const [slotVisibility, setSlotVisibility] = useState<SlotVisibility>({});
  useEffect(() => {
    if (placements.length === 0) {
      setSlotVisibility((previous) => (
        Object.keys(previous).length === 0 ? previous : {}
      ));
    }
  }, [placements.length]);
  const onVisibilityChange = useCallback((
    slot: AdBannerSlot,
    signature: string,
    visible: boolean,
  ) => {
    setSlotVisibility((previous) => {
      const current = previous[slot];
      if (current?.signature === signature && current.visible === visible) {
        return previous;
      }
      return { ...previous, [slot]: { signature, visible } };
    });
  }, []);
  const visiblePlacements = placements.filter((placement) => {
    const visibility = slotVisibility[placement.slot];
    return visibility?.signature !== placementSignature(placement) || visibility.visible;
  });
  const visiblePlacementKey = visiblePlacements.map(placementSignature).join('|');

  useEffect(() => {
    const bottom = visiblePlacements.find(({ slot }) => slot === 'bottom');
    const rails = visiblePlacements.filter(({ slot }) => slot !== 'bottom');
    const root = document.documentElement;
    if (bottom) {
      root.dataset.adBottomVisible = 'true';
      root.style.setProperty('--ad-bottom-reserve', `${bottom.height + 24}px`);
    } else {
      delete root.dataset.adBottomVisible;
      root.style.setProperty('--ad-bottom-reserve', '0px');
    }
    if (rails.length > 0) {
      const railWidth = Math.max(...rails.map(({ width }) => width));
      root.dataset.adSideVisible = 'true';
      root.style.setProperty('--ad-side-reserve', `${railWidth + 20}px`);
    } else {
      delete root.dataset.adSideVisible;
      root.style.setProperty('--ad-side-reserve', '0px');
    }
    return () => {
      delete root.dataset.adBottomVisible;
      delete root.dataset.adSideVisible;
      root.style.setProperty('--ad-bottom-reserve', '0px');
      root.style.setProperty('--ad-side-reserve', '0px');
    };
  }, [visiblePlacementKey]);

  // Keep terminal children mounted so a later provider/config transition can
  // reactivate the same slot signature; an empty layout has no visible chrome.
  if (placements.length === 0) {
    return null;
  }

  return (
    <div className="ad-banner-layout" data-testid="ad-banner-layout">
      {placements.map((placement) => (
        <ManagedBannerHost
          key={placement.slot}
          placement={placement}
          provider={provider}
          providerReady={providerReady}
          onVisibilityChange={onVisibilityChange}
        />
      ))}
    </div>
  );
};
