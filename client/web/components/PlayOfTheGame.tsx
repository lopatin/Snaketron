import React, {
  useCallback,
  useEffect,
  useId,
  useLayoutEffect,
  useRef,
  useState,
} from 'react';
import { useCrazyGames } from '../contexts/CrazyGamesContext';
import { useAdsOptional } from '../contexts/AdsContext';
import { crazyGames } from '../services/crazyGames';
import type { HighlightClip, Rank } from '../types';
import {
  canAutoplayHighlight,
  formatHighlightReason,
  type MatchHighlightState,
} from '../utils/highlightPresentation';
import RankIcon from './RankIcon';
import ScenarioCanvas, {
  type ScenarioCanvasFrame,
  type ScenarioCanvasHandle,
  type ScenarioPlaybackStatus,
} from './ScenarioCanvas';
import { ReplayIcon } from './TransportIcons';
import './PlayOfTheGame.css';

const VISIBLE_THRESHOLD = 0.62;

/** How long the title holds at centre before it travels to its corner. */
const INTRO_TITLE_HOLD_MS = 850;
/** The journey itself. Kept in lockstep with the CSS transition duration. */
const INTRO_TRAVEL_MS = 620;
/** Height the title is blown up to at centre, relative to its resting size. */
const INTRO_TITLE_SCALE = 3.1;

type IntroPhase = 'idle' | 'title' | 'travelling' | 'done';

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
          <strong>Replay booth</strong>
          <small>Advertisement placement</small>
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
  starRank: Rank | null;
  ratingSettled: boolean;
  autoplayAllowed: boolean;
  onAutoplayStarted: (gameId: number) => void;
}

const HighlightReplay: React.FC<HighlightReplayProps> = ({
  clip,
  starRank,
  ratingSettled,
  autoplayAllowed,
  onAutoplayStarted,
}) => {
  const playerRef = useRef<ScenarioCanvasHandle>(null);
  const bandRef = useRef<HTMLElement>(null);
  const kickerRef = useRef<HTMLSpanElement>(null);
  const autoplayStartedRef = useRef(false);
  const suspendedByGateRef = useRef(false);
  const [introPhase, setIntroPhase] = useState<IntroPhase>('idle');
  const [lowerThirdOpen, setLowerThirdOpen] = useState(true);
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
      // The title card runs first and starts playback when it lands. Reduced
      // motion never reaches this branch (canAutoplayHighlight gates on it),
      // so the intro cannot strand a viewer who asked for no animation.
      setIntroPhase('title');
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

  // Park the title at the centre of the band, scaled up, before the browser
  // paints the frame that reveals it. Measuring both boxes (rather than
  // hard-coding a translation) keeps the landing exact at any band size, and
  // the resting position stays the element's real layout position so nothing
  // has to be un-transformed afterwards.
  useLayoutEffect(() => {
    if (introPhase !== 'title') return;
    const kicker = kickerRef.current;
    const band = bandRef.current;
    if (!kicker || !band) {
      setIntroPhase('done');
      return;
    }
    const from = kicker.getBoundingClientRect();
    const into = band.getBoundingClientRect();
    if (from.width === 0 || into.width === 0) {
      setIntroPhase('done');
      return;
    }
    const dx = (into.left + into.width / 2) - (from.left + from.width / 2);
    const dy = (into.top + into.height / 2) - (from.top + from.height / 2);
    kicker.style.transform =
      `translate(${dx.toFixed(2)}px, ${dy.toFixed(2)}px) scale(${INTRO_TITLE_SCALE})`;
  }, [introPhase]);

  useEffect(() => {
    if (introPhase !== 'title') return undefined;
    const timer = window.setTimeout(() => setIntroPhase('travelling'), INTRO_TITLE_HOLD_MS);
    return () => window.clearTimeout(timer);
  }, [introPhase]);

  useEffect(() => {
    if (introPhase !== 'travelling') return undefined;
    // Releasing the inline transform lets the CSS transition carry it home.
    if (kickerRef.current) kickerRef.current.style.transform = '';
    const timer = window.setTimeout(() => {
      setIntroPhase('done');
      setPlaybackStatus('playing');
      void playerRef.current?.play().catch(() => setRenderFailed(true));
    }, INTRO_TRAVEL_MS);
    return () => window.clearTimeout(timer);
  }, [introPhase]);

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
  const starBadge = starRank
    ? (
      <RankIcon
        tier={starRank.tier}
        division={starRank.division}
        className="potg-star__rank"
      />
    )
    : null;

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
            {starBadge}
            {clip.star_name}
          </strong>
          <span>{reason}</span>
          <small>Replay renderer unavailable</small>
        </div>
      </section>
    );
  }

  const complete = playbackStatus === 'complete';
  const introRunning = introPhase === 'title' || introPhase === 'travelling';

  return (
    <section
      ref={bandRef}
      className={`potg-band${complete ? ' is-complete' : ''}`}
      aria-label={`Play of the game: ${clip.star_name}. ${reason}`}
      data-testid="play-of-the-game"
      data-playback={playbackStatus}
      data-intro={introPhase}
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

      <span ref={kickerRef} className="potg-kicker is-title" aria-hidden="true">
        Play of the game
      </span>

      <button
        type="button"
        className="potg-replay"
        onClick={replay}
        disabled={!playerReady || introRunning}
        aria-label={`Replay play of the game by ${clip.star_name}`}
        data-testid="potg-replay"
      >
        <ReplayIcon />
      </button>

      <div
        className={`potg-lower-third${lowerThirdOpen ? '' : ' is-collapsed'}`}
        data-testid="potg-lower-third"
      >
        {lowerThirdOpen ? (
          <>
            <span className="potg-star">
              {starBadge}
              <span className="potg-star__name">{clip.star_name}</span>
            </span>
            <span className="potg-reason">{reason}</span>
          </>
        ) : (
          <span className="potg-summary">
            {starBadge}
            <span className="potg-star__name">{clip.star_name}</span>
            <span className="potg-summary__reason">{reason}</span>
          </span>
        )}
        <button
          type="button"
          className="potg-lower-third__toggle"
          onClick={() => setLowerThirdOpen((open) => !open)}
          aria-expanded={lowerThirdOpen}
          aria-label={lowerThirdOpen
            ? 'Minimise the play of the game caption'
            : 'Expand the play of the game caption'}
          data-testid="potg-lower-third-toggle"
        >
          <span aria-hidden="true">{lowerThirdOpen ? '▾' : '▴'}</span>
        </button>
      </div>
    </section>
  );
};

export interface PlayOfTheGameProps {
  highlight: MatchHighlightState;
  /**
   * The star's ladder rank, when this client can actually know it — today
   * that means the star is the local player and their post-match rating has
   * landed. A clip carries no rank for other players, and an invented badge
   * would be a false claim about someone's standing, so the caption simply
   * drops the badge rather than guessing.
   */
  starRank?: Rank | null;
  ratingSettled: boolean;
  autoplayAllowed: boolean;
  onAutoplayStarted: (gameId: number) => void;
}

const PlayOfTheGame: React.FC<PlayOfTheGameProps> = ({
  highlight,
  starRank = null,
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
      starRank={starRank}
      ratingSettled={ratingSettled}
      autoplayAllowed={autoplayAllowed}
      onAutoplayStarted={onAutoplayStarted}
    />
  );
};

export default PlayOfTheGame;
