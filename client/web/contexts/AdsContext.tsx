import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import type {
  AdBreakResolution,
  ClientAdsConfig,
  LobbyAdBreakView,
} from '../types';
import { AdBreakResolutionOutbox } from '../services/ads/adBreakOutbox';
import { resolveAdAttemptBeforeDeadline } from '../services/ads/adBreakCoordinator';
import { analytics } from '../services/analytics';
import { createAdProvider } from '../services/ads/providerFactory';
import type { AdProvider, AdProviderCapabilities } from '../services/ads/types';
import { getServerClockOffsetMs } from '../utils/clockSync';
import { useAuth } from './AuthContext';
import { useWebSocket } from './WebSocketContext';

export type AdBreakPhase = 'idle' | 'requesting' | 'playing' | 'waiting';

interface AdsContextValue {
  config: ClientAdsConfig;
  provider: AdProvider;
  providerReady: boolean;
  capabilities: AdProviderCapabilities;
  activeBreak: LobbyAdBreakView | null;
  isLobbyInAdBreak: boolean;
  phase: AdBreakPhase;
}

const AdsContext = createContext<AdsContextValue | null>(null);
const ACK_RETRY_MS = 1_500;

type BreakRun =
  | { status: 'running' }
  | { status: 'resolved'; resolution: AdBreakResolution };

export const AdsProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const {
    adConfiguration,
    currentLobby,
    isConnected,
    isSessionAuthenticated,
    sendMessage,
  } = useWebSocket();
  const { user } = useAuth();
  const providerId = adConfiguration.enabled ? adConfiguration.provider : 'none';
  const provider = useMemo(() => createAdProvider(providerId), [providerId]);
  const [providerReady, setProviderReady] = useState(false);
  const [presentation, setPresentation] = useState<{
    breakId: string | null;
    phase: AdBreakPhase;
  }>({ breakId: null, phase: 'idle' });
  const [outboxRevision, setOutboxRevision] = useState(0);
  const runsRef = useRef(new Map<string, BreakRun>());
  const outboxRef = useRef(new AdBreakResolutionOutbox());
  const activeBreakRef = useRef<LobbyAdBreakView | null>(null);
  const currentUserIdRef = useRef<number | null>(null);

  const isLobbyInAdBreak = currentLobby?.state === 'ad_break';
  const activeBreak = isLobbyInAdBreak ? currentLobby?.adBreak ?? null : null;
  const currentUserId = Number.isSafeInteger(user?.id) ? user!.id : null;
  const currentUserResolved = Boolean(
    activeBreak &&
    currentUserId !== null &&
    activeBreak.resolved_user_ids.includes(currentUserId),
  );
  const currentUserTargeted = Boolean(
    activeBreak &&
    currentUserId !== null &&
    activeBreak.ad_user_ids.includes(currentUserId),
  );

  activeBreakRef.current = activeBreak;
  currentUserIdRef.current = currentUserId;

  useEffect(() => {
    let cancelled = false;
    setProviderReady(false);
    void provider.initialize()
      .catch((error) => {
        console.info(`Advertisement provider ${provider.id} unavailable:`, error);
      })
      .finally(() => {
        if (!cancelled) {
          setProviderReady(true);
        }
      });
    return () => {
      cancelled = true;
      provider.dispose();
    };
  }, [provider]);

  const enqueueResolution = useCallback((
    breakId: string,
    resolution: AdBreakResolution,
  ) => {
    outboxRef.current.enqueue(breakId, resolution, currentUserIdRef.current);
    setOutboxRevision((value) => value + 1);
  }, []);

  useEffect(() => {
    if (!isLobbyInAdBreak) {
      if (presentation.breakId) {
        outboxRef.current.confirm(presentation.breakId);
      }
      setPresentation((previous) => (
        previous.phase === 'idle' ? previous : { breakId: null, phase: 'idle' }
      ));
      return;
    }

    // A rolling deployment may briefly report the state before the optional
    // payload. Keep controls covered; the next authoritative update supplies
    // the break id needed for acknowledgement.
    if (!activeBreak) {
      setPresentation({ breakId: null, phase: 'requesting' });
      return;
    }

    // Authentication hydration can trail lobby restoration by a render. Wait
    // for the stable identity before consulting the identity-bound outbox or
    // starting an SDK request, otherwise a reload could replay a finished ad.
    if (currentUserId === null) {
      setPresentation({ breakId: activeBreak.id, phase: 'requesting' });
      return;
    }

    if (currentUserResolved) {
      outboxRef.current.confirm(activeBreak.id);
      runsRef.current.set(activeBreak.id, { status: 'resolved', resolution: 'completed' });
      setPresentation({ breakId: activeBreak.id, phase: 'waiting' });
      return;
    }

    const restoredResolution = outboxRef.current.get(activeBreak.id, currentUserId);
    if (restoredResolution) {
      runsRef.current.set(activeBreak.id, {
        status: 'resolved',
        resolution: restoredResolution.resolution,
      });
      setOutboxRevision((value) => value + 1);
      setPresentation({ breakId: activeBreak.id, phase: 'waiting' });
      return;
    }

    const existing = runsRef.current.get(activeBreak.id);
    if (existing?.status === 'resolved') {
      enqueueResolution(activeBreak.id, existing.resolution);
      setPresentation({ breakId: activeBreak.id, phase: 'waiting' });
      return;
    }
    if (existing?.status === 'running') {
      return;
    }

    runsRef.current.set(activeBreak.id, { status: 'running' });
    setPresentation({ breakId: activeBreak.id, phase: 'requesting' });
    const breakId = activeBreak.id;
    const expiresAtMs = activeBreak.expires_at_ms;

    const resolveBreak = async (): Promise<AdBreakResolution> => {
      if (
        !currentUserTargeted ||
        !adConfiguration.enabled ||
        !adConfiguration.video.pre_match ||
        !provider.capabilities.video
      ) {
        return 'unavailable';
      }

      const serverClockOffsetMs = getServerClockOffsetMs() ?? 0;
      const synchronizedNow = () => Date.now() + serverClockOffsetMs;
      return resolveAdAttemptBeforeDeadline(expiresAtMs, async (admission) => {
        try {
          await provider.initialize();
          if (!admission.isOpen()) {
            return 'timed_out';
          }
          if (await provider.getAdBlockStatus() === 'blocked') {
            return 'blocked';
          }
          if (!admission.isOpen()) {
            return 'timed_out';
          }
          const result = await provider.playInterstitial({
            placement: 'pre-match',
            admission,
            onStarted: () => {
              if (activeBreakRef.current?.id === breakId) {
                setPresentation({ breakId, phase: 'playing' });
              }
            },
          });
          return result.resolution;
        } catch (error) {
          console.info('Pre-match advertisement unavailable:', error);
          return 'error';
        }
      }, provider.capabilities.videoAdmissionLeadMs, synchronizedNow);
    };

    void resolveBreak().then((resolution) => {
      // Reported for every outcome, before any of the lobby bookkeeping below
      // can return early: fill rate is only meaningful if the misses are
      // counted too, and a break resolved for a player who already answered
      // still consumed an ad opportunity.
      analytics.trackAdBreak(resolution, provider.id, 'pre_match');

      const latestBreak = activeBreakRef.current;
      if (!latestBreak || latestBreak.id !== breakId) {
        return;
      }
      const latestUserId = currentUserIdRef.current;
      if (
        latestUserId !== null &&
        latestBreak.resolved_user_ids.includes(latestUserId)
      ) {
        runsRef.current.set(breakId, { status: 'resolved', resolution });
        setPresentation({ breakId, phase: 'waiting' });
        return;
      }
      runsRef.current.set(breakId, { status: 'resolved', resolution });
      enqueueResolution(breakId, resolution);
      setPresentation({ breakId, phase: 'waiting' });
    });

    // Bound long sessions without removing the active run.
    if (runsRef.current.size > 24) {
      for (const id of runsRef.current.keys()) {
        if (id !== breakId) {
          runsRef.current.delete(id);
          if (runsRef.current.size <= 16) {
            break;
          }
        }
      }
    }
  }, [
    activeBreak,
    adConfiguration.enabled,
    adConfiguration.video.pre_match,
    currentUserResolved,
    currentUserTargeted,
    currentUserId,
    enqueueResolution,
    isLobbyInAdBreak,
    presentation.breakId,
    provider,
  ]);

  const flushResolution = useCallback(() => {
    if (
      !activeBreak ||
      currentUserResolved ||
      !isConnected ||
      !isSessionAuthenticated
    ) {
      return;
    }
    const pending = outboxRef.current.get(activeBreak.id, currentUserId);
    if (!pending) {
      return;
    }
    if (sendMessage({
      AdBreakResolved: {
        break_id: pending.breakId,
        resolution: pending.resolution,
      },
    })) {
      outboxRef.current.markAttempt(activeBreak.id);
    }
  }, [
    activeBreak,
    currentUserResolved,
    currentUserId,
    isConnected,
    isSessionAuthenticated,
    sendMessage,
  ]);

  useEffect(() => {
    if (
      !activeBreak ||
      currentUserResolved ||
      !outboxRef.current.get(activeBreak.id, currentUserId)
    ) {
      return;
    }
    flushResolution();
    const retry = window.setInterval(flushResolution, ACK_RETRY_MS);
    return () => window.clearInterval(retry);
  }, [activeBreak, currentUserId, currentUserResolved, flushResolution, outboxRevision]);

  useEffect(() => {
    if (presentation.phase !== 'playing') {
      return;
    }
    const previousMute = new Map<HTMLMediaElement, boolean>();
    const muteMedia = () => {
      for (const media of document.querySelectorAll<HTMLMediaElement>('audio, video')) {
        if (!previousMute.has(media)) {
          previousMute.set(media, media.muted);
        }
        media.muted = true;
      }
    };
    muteMedia();
    document.documentElement.dataset.adPlaybackActive = 'true';
    const observer = new MutationObserver(muteMedia);
    observer.observe(document.body, { childList: true, subtree: true });
    return () => {
      observer.disconnect();
      delete document.documentElement.dataset.adPlaybackActive;
      for (const [media, wasMuted] of previousMute) {
        if (media.isConnected) {
          media.muted = document.documentElement.dataset.crazygamesMuteAudio === 'true'
            ? true
            : wasMuted;
        }
      }
    };
  }, [presentation.phase]);

  const phase: AdBreakPhase = !isLobbyInAdBreak
    ? 'idle'
    : activeBreak && presentation.breakId === activeBreak.id
      ? presentation.phase
      : 'requesting';

  const value = useMemo<AdsContextValue>(() => ({
    config: adConfiguration,
    provider,
    providerReady,
    capabilities: provider.capabilities,
    activeBreak,
    isLobbyInAdBreak,
    phase,
  }), [
    activeBreak,
    adConfiguration,
    isLobbyInAdBreak,
    phase,
    provider,
    providerReady,
  ]);

  return <AdsContext.Provider value={value}>{children}</AdsContext.Provider>;
};

/**
 * Ads policy for surfaces that may legitimately render outside `AdsProvider`,
 * such as the design-review harnesses, which mount a single component with no
 * socket or auth behind it. `null` means "policy unknown", and a caller that
 * cannot confirm policy must not request inventory.
 */
export const useAdsOptional = (): AdsContextValue | null => useContext(AdsContext);

export const useAds = (): AdsContextValue => {
  const value = useAdsOptional();
  if (!value) {
    throw new Error('useAds must be used within AdsProvider');
  }
  return value;
};
