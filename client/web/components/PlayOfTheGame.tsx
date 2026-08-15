import React, {
  useCallback,
  useEffect,
  useId,
  useRef,
  useState,
} from 'react';
import { useCrazyGames } from '../contexts/CrazyGamesContext';
import { useAdsOptional } from '../contexts/AdsContext';
import { crazyGames } from '../services/crazyGames';
import type { HighlightClip } from '../types';
import { resolveSnakeSkinColors } from '../utils/snakeSkin';
import {
  canAutoplayHighlight,
  formatHighlightReason,
  type MatchHighlightState,
} from '../utils/highlightPresentation';
import ScenarioCanvas, {
  type ScenarioCanvasFrame,
  type ScenarioCanvasHandle,
  type ScenarioPlaybackStatus,
} from './ScenarioCanvas';
import './PlayOfTheGame.css';

const VISIBLE_THRESHOLD = 0.62;

interface SponsorSlotProps {
  reason: 'absent' | 'incompatible' | 'network';
}

const SponsorSlot: React.FC<SponsorSlotProps> = ({ reason }) => {
  const { available, adState } = useCrazyGames();
  // Ad enablement is server-owned policy, not a build flag: it arrives through
  // AdsProvider. Outside that provider (the QA harness) policy is unknown, and
  // a slot that cannot confirm policy must not request inventory.
  const ads = useAdsOptional();
  const bannerAllowed = Boolean(ads?.config.enabled && ads.capabilities.banners);
  const reactId = useId();
  const containerIdRef = useRef(`potg-sponsor-${reactId.replace(/[^a-zA-Z0-9_-]/g, '')}`);
  const attemptedRef = useRef(false);
  const [bannerLive, setBannerLive] = useState(false);
  const containerId = containerIdRef.current;

  useEffect(() => {
    if (!bannerAllowed || !available || adState !== 'idle' || attemptedRef.current) {
      return undefined;
    }
    let active = true;
    // Completion may request an interstitial in a parent effect from the same
    // commit. Yield once, then consult the adapter's live snapshot so a stale
    // context render cannot start a banner underneath that commercial break.
    const timer = window.setTimeout(() => {
      if (!active || crazyGames.getSnapshot().adState !== 'idle') return;
      attemptedRef.current = true;
      void crazyGames.requestResponsiveBanner(containerId).then((result) => {
        if (active) setBannerLive(result.status === 'filled');
      });
    }, 0);
    return () => {
      active = false;
      window.clearTimeout(timer);
    };
  }, [adState, available, bannerAllowed, containerId]);

  useEffect(() => () => crazyGames.clearBanner(containerId), [containerId]);

  return (
    <aside
      className={`potg-sponsor${bannerLive ? ' has-live-banner' : ' is-house-slot'}`}
      aria-label="Advertisement"
      data-testid="potg-sponsor"
      data-unavailable-reason={reason}
    >
      <span className="potg-sponsor__label">Sponsored</span>
      <div id={containerId} className="potg-sponsor__sdk" />
      {!bannerLive && (
        <div className="potg-sponsor__fallback">
          <span className="potg-sponsor__mark" aria-hidden="true">ST</span>
          <span>
            <strong>Replay booth</strong>
            <small>Advertisement placement</small>
          </span>
        </div>
      )}
    </aside>
  );
};

const HighlightSkeleton: React.FC = () => (
  <section
    className="potg-band is-loading"
    aria-label="Play of the game loading"
    data-testid="potg-loading"
  >
    <span className="potg-kicker">Play of the game</span>
    <div className="potg-skeleton__arena" aria-hidden="true">
      <i /><i /><i />
    </div>
    <span className="potg-skeleton__caption">Cutting the replay…</span>
  </section>
);

interface HighlightReplayProps {
  clip: HighlightClip;
  ratingSettled: boolean;
  autoplayAllowed: boolean;
  onAutoplayStarted: (gameId: number) => void;
}

const HighlightReplay: React.FC<HighlightReplayProps> = ({
  clip,
  ratingSettled,
  autoplayAllowed,
  onAutoplayStarted,
}) => {
  const playerRef = useRef<ScenarioCanvasHandle>(null);
  const bandRef = useRef<HTMLElement>(null);
  const autoplayStartedRef = useRef(false);
  const suspendedByGateRef = useRef(false);
  const [playerReady, setPlayerReady] = useState(false);
  const [substantiallyVisible, setSubstantiallyVisible] = useState(false);
  const [documentVisible, setDocumentVisible] = useState(() => !document.hidden);
  const [motionAllowed, setMotionAllowed] = useState(() => (
    typeof window.matchMedia !== 'function' ||
    !window.matchMedia('(prefers-reduced-motion: reduce)').matches
  ));
  const [playbackStatus, setPlaybackStatus] = useState<ScenarioPlaybackStatus>('loading');
  const [renderFailed, setRenderFailed] = useState(false);
  const { adState } = useCrazyGames();

  useEffect(() => {
    const element = bandRef.current;
    if (!element) return undefined;
    if (typeof IntersectionObserver !== 'function') {
      const bounds = element.getBoundingClientRect();
      setSubstantiallyVisible(bounds.bottom > 0 && bounds.top < window.innerHeight);
      return undefined;
    }
    const observer = new IntersectionObserver(([entry]) => {
      setSubstantiallyVisible(
        entry.isIntersecting && entry.intersectionRatio >= VISIBLE_THRESHOLD,
      );
    }, { threshold: [0, VISIBLE_THRESHOLD, 1] });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    const onVisibilityChange = () => setDocumentVisible(!document.hidden);
    document.addEventListener('visibilitychange', onVisibilityChange);
    return () => document.removeEventListener('visibilitychange', onVisibilityChange);
  }, []);

  useEffect(() => {
    if (typeof window.matchMedia !== 'function') return undefined;
    const query = window.matchMedia('(prefers-reduced-motion: reduce)');
    const update = () => setMotionAllowed(!query.matches);
    update();
    query.addEventListener('change', update);
    return () => query.removeEventListener('change', update);
  }, []);

  useEffect(() => {
    const lifecycleAllowsPlayback = substantiallyVisible && documentVisible && adState === 'idle';
    if (!lifecycleAllowsPlayback) {
      if (playbackStatus === 'playing') {
        suspendedByGateRef.current = true;
        void playerRef.current?.pause().catch(() => setRenderFailed(true));
      }
      return;
    }

    if (suspendedByGateRef.current && playbackStatus !== 'complete') {
      suspendedByGateRef.current = false;
      void playerRef.current?.play().catch(() => setRenderFailed(true));
      return;
    }

    if (
      autoplayAllowed &&
      !autoplayStartedRef.current &&
      canAutoplayHighlight({
        playerReady,
        ratingSettled,
        substantiallyVisible,
        documentVisible,
        motionAllowed,
        adState,
      })
    ) {
      autoplayStartedRef.current = true;
      onAutoplayStarted(clip.game_id);
      void playerRef.current?.play().catch(() => setRenderFailed(true));
    }
  }, [
    adState,
    autoplayAllowed,
    clip.game_id,
    documentVisible,
    motionAllowed,
    onAutoplayStarted,
    playbackStatus,
    playerReady,
    ratingSettled,
    substantiallyVisible,
  ]);

  useEffect(() => () => {
    void playerRef.current?.pause().catch(() => undefined);
  }, []);

  const handleFrame = useCallback((frame: ScenarioCanvasFrame) => {
    // ScenarioCanvas commits its terminal frame while the animation loop is
    // technically still running, then freezes it immediately afterward.
    // Elapsed time is therefore the authoritative end signal for this custom
    // results-card transport.
    const status = frame.durationMs > 0 && frame.elapsedMs >= frame.durationMs
      ? 'complete'
      : frame.status;
    setPlaybackStatus((current) => current === status ? current : status);
  }, []);

  const replay = useCallback(() => {
    setPlaybackStatus('playing');
    void playerRef.current?.replay().catch(() => setRenderFailed(true));
  }, []);

  const reason = formatHighlightReason(clip.reason);
  const starSnake = clip.anchor.arena.snakes[clip.star_snake_id];
  const starSkin = starSnake ? resolveSnakeSkinColors({
    snake_index: clip.star_snake_id,
    team_id: starSnake.team_id,
    team_member_slot: clip.anchor.arena.snakes
      .slice(0, clip.star_snake_id)
      .filter((snake) => snake.team_id === starSnake.team_id)
      .length,
    snake_count: clip.anchor.arena.snakes.length,
    is_team_game: clip.anchor.arena.team_zone_config !== null,
    local_snake_id: clip.star_snake_id,
    local_team_id: starSnake.team_id,
  }) : null;
  const starSkinStyle = {
    '--potg-skin-fill': starSkin?.fill ?? '#3b82f6',
    '--potg-skin-outline': starSkin?.outline ?? '#20232a',
  } as React.CSSProperties;

  if (renderFailed) {
    return (
      <section
        className="potg-band is-poster"
        aria-label="Play of the game poster"
        data-testid="potg-render-fallback"
      >
        <div className="potg-poster__grid" aria-hidden="true" />
        <img src="SnaketronLogo.png" alt="Snaketron" className="potg-poster__logo" />
        <span className="potg-kicker">Play of the game</span>
        <div className="potg-poster__caption">
          <strong>
            <i className="potg-star__skin" style={starSkinStyle} aria-hidden="true" />
            {clip.star_name}
          </strong>
          <span>{reason}</span>
          <small>Replay renderer unavailable</small>
        </div>
      </section>
    );
  }

  const complete = playbackStatus === 'complete';

  return (
    <section
      ref={bandRef}
      className={`potg-band${complete ? ' is-complete' : ''}`}
      aria-label={`Play of the game: ${clip.star_name}. ${reason}`}
      data-testid="play-of-the-game"
      data-playback={playbackStatus}
    >
      <ScenarioCanvas
        ref={playerRef}
        clip={clip}
        autoPlay={false}
        loop={false}
        controls={false}
        aspectRatio={21 / 9}
        label={`Play of the game by ${clip.star_name}`}
        className="potg-canvas"
        onReady={() => setPlayerReady(true)}
        onFrame={handleFrame}
        onError={() => setRenderFailed(true)}
      />

      <div className="potg-broadcast" aria-hidden="true">
        <span className="potg-kicker"><i /> Play of the game</span>
        <span className="potg-live-bug">Replay</span>
      </div>
      <div className="potg-lower-third">
        <span className="potg-star">
          <i className="potg-star__skin" style={starSkinStyle} aria-hidden="true" />
          <span className="potg-star__name">{clip.star_name}</span>
        </span>
        <span className="potg-reason">{reason}</span>
      </div>

      {complete && (
        <button
          type="button"
          className="potg-replay-overlay"
          onClick={replay}
          aria-label={`Replay play of the game by ${clip.star_name}`}
          data-testid="potg-replay"
        >
          <span aria-hidden="true">↺</span>
          Watch again
        </button>
      )}
    </section>
  );
};

export interface PlayOfTheGameProps {
  highlight: MatchHighlightState;
  ratingSettled: boolean;
  autoplayAllowed: boolean;
  onAutoplayStarted: (gameId: number) => void;
}

const PlayOfTheGame: React.FC<PlayOfTheGameProps> = ({
  highlight,
  ratingSettled,
  autoplayAllowed,
  onAutoplayStarted,
}) => {
  if (highlight.phase === 'idle' || highlight.phase === 'pending') {
    return <HighlightSkeleton />;
  }
  if (highlight.phase === 'unavailable') {
    return <SponsorSlot reason={highlight.reason} />;
  }
  return (
    <HighlightReplay
      key={`${highlight.clip.game_id}:${highlight.clip.window.start_tick}`}
      clip={highlight.clip}
      ratingSettled={ratingSettled}
      autoplayAllowed={autoplayAllowed}
      onAutoplayStarted={onAutoplayStarted}
    />
  );
};

export default PlayOfTheGame;
