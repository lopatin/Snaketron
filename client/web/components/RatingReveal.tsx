import RankIcon from './RankIcon';
import React, { useEffect, useRef, useState } from 'react';
import type { RankTier } from '../types';
import {
  formatRankLabel,
  getRankFromMMR,
  rankBandIndexForMmr,
  rankBandProgress,
  RANK_BANDS,
} from '../utils/rank';
import {
  countDurationMs,
  type MatchRatingState,
  type RatingReveal as RatingRevealModel,
} from '../utils/ratingReveal';

/**
 * The post-match rating band: an odometer sweep from the pre-match MMR to
 * the persisted result, a division meter that tracks the sweep, and a
 * promotion/demotion ribbon when the sweep crosses a division boundary.
 *
 * One animated MMR value drives every element — badge, meter, caption all
 * derive from it — so a promotion is *seen* happening: the meter fills, the
 * badge stamps over, the ribbon lands. No element can disagree with another.
 */

/** Accent per tier, sampled from the medallion artwork. */
const TIER_ACCENTS: Record<RankTier, string> = {
  bronze: '#b06a35',
  silver: '#8b96a3',
  gold: '#d49a00',
  platinum: '#7f95a8',
  diamond: '#1f9ce8',
  master: '#e2a70f',
  grandmaster: '#e2a70f',
};

const clamp01 = (value: number): number => Math.min(1, Math.max(0, value));
const easeOutCubic = (t: number): number => 1 - (1 - clamp01(t)) ** 3;
const easeOutBack = (t: number): number => {
  const c = 1.9;
  const p = clamp01(t) - 1;
  return 1 + (c + 1) * p ** 3 + c * p ** 2;
};

/**
 * The odometer curve. Deliberately ease-*in*-out rather than ease-out: with a
 * front-loaded curve a promotion is crossed in the first fifth of the sweep
 * and the remaining 80% is a slow crawl with nothing left to happen. Symmetric
 * easing puts the division boundary near the middle of the count, which is
 * where the beat belongs — the meter fills, the badge turns over, and there is
 * still a moment of run-out to land the ribbon on.
 */
const easeInOutCubic = (t: number): number => {
  const p = clamp01(t);
  return p < 0.5 ? 4 * p ** 3 : 1 - (-2 * p + 2) ** 3 / 2;
};

/** How long the badge turnover runs, from the frame the band changes. */
const PROMOTION_MS = 640;
/** The badge leans in over this window before the boundary is reached. */
const PROMOTION_ANTICIPATION_MS = 420;

/** Pause before the sweep starts, letting the score card land first. */
const COUNT_START_DELAY_MS = 550;

const usePrefersReducedMotion = (): boolean => {
  const [reduced, setReduced] = useState<boolean>(() => (
    typeof window.matchMedia === 'function'
      ? window.matchMedia('(prefers-reduced-motion: reduce)').matches
      : false
  ));

  useEffect(() => {
    if (typeof window.matchMedia !== 'function') {
      return;
    }
    const query = window.matchMedia('(prefers-reduced-motion: reduce)');
    const onChange = (event: MediaQueryListEvent) => setReduced(event.matches);
    query.addEventListener('change', onChange);
    return () => query.removeEventListener('change', onChange);
  }, []);

  return reduced;
};

type RevealStage = 'counting' | 'settled';

/** The sweep as a pure function of elapsed time, shared by both clocks. */
const sweepAt = (
  reveal: RatingRevealModel,
  elapsedMs: number,
): { displayMmr: number; stage: RevealStage } => {
  const from = reveal.before?.mmr ?? reveal.after.mmr;
  const to = reveal.after.mmr;
  const t = clamp01((elapsedMs - COUNT_START_DELAY_MS) / countDurationMs(to - from));
  return {
    displayMmr: Math.round(from + (to - from) * easeInOutCubic(t)),
    stage: t >= 1 ? 'settled' : 'counting',
  };
};

/**
 * The instant, in elapsed ms, at which the sweep first lands in the band it
 * finishes in — or null when the reveal does not change division.
 *
 * Solved rather than observed: the badge turnover has to be able to *lean in*
 * before the boundary and burst on it, which means knowing when the crossing
 * happens before it does. `easeInOutCubic` is monotonic, so inverting it for
 * the boundary value is exact.
 */
const bandCrossMs = (reveal: RatingRevealModel): number | null => {
  const from = reveal.before?.mmr ?? reveal.after.mmr;
  const to = reveal.after.mmr;
  const fromBand = rankBandIndexForMmr(from);
  const toBand = rankBandIndexForMmr(to);
  if (from === to || fromBand === toBand) {
    return null;
  }

  // The first value on the far side of the boundary, in the direction of
  // travel: a promotion enters the new band at its floor; a demotion leaves
  // the old band one point below its own.
  const boundary = to > from
    ? RANK_BANDS[toBand].min
    : RANK_BANDS[fromBand].min - 1;
  const share = clamp01((boundary - from) / (to - from));

  // Invert easeInOutCubic: p < 0.5 uses 4t³, p >= 0.5 the mirrored branch.
  const t = share < 0.5
    ? Math.cbrt(share / 4)
    : 1 - Math.cbrt((1 - share) * 2) / 2;
  return COUNT_START_DELAY_MS + t * countDurationMs(to - from);
};

interface SweepState {
  displayMmr: number | null;
  stage: RevealStage;
  /**
   * Elapsed time on whichever clock is driving, so the badge turnover can be
   * placed against the same timeline the odometer runs on. `null` means the
   * sweep is not running (settled instantly, or no reveal).
   */
  elapsedMs: number | null;
}

const useAnimatedMmr = (
  reveal: RatingRevealModel | null,
  reducedMotion: boolean,
  clockMs: number | null,
): SweepState => {
  const [displayMmr, setDisplayMmr] = useState<number | null>(null);
  const [stage, setStage] = useState<RevealStage>('counting');
  const [elapsedMs, setElapsedMs] = useState<number | null>(null);
  const driven = clockMs !== null;

  useEffect(() => {
    if (reveal === null || driven) {
      return;
    }
    const from = reveal.before?.mmr ?? reveal.after.mmr;
    const to = reveal.after.mmr;
    if (reducedMotion || from === to) {
      setDisplayMmr(to);
      setStage('settled');
      setElapsedMs(null);
      return;
    }

    setDisplayMmr(from);
    setStage('counting');
    let frame = 0;
    let startedAt: number | null = null;
    const step = (now: number) => {
      if (startedAt === null) {
        startedAt = now;
      }
      const elapsed = now - startedAt;
      const swept = sweepAt(reveal, elapsed);
      setDisplayMmr(swept.displayMmr);
      setElapsedMs(elapsed);
      if (swept.stage === 'settled') {
        setStage('settled');
        return;
      }
      frame = requestAnimationFrame(step);
    };
    frame = requestAnimationFrame(step);
    return () => cancelAnimationFrame(frame);
  }, [driven, reveal, reducedMotion]);

  if (reveal !== null && driven) {
    const from = reveal.before?.mmr ?? reveal.after.mmr;
    return reducedMotion || from === reveal.after.mmr
      ? { displayMmr: reveal.after.mmr, stage: 'settled', elapsedMs: null }
      : { ...sweepAt(reveal, clockMs), elapsedMs: clockMs };
  }

  return { displayMmr, stage, elapsedMs };
};

/** Inline transforms for the badge turnover; null outside the transition. */
interface PromotionFrame {
  /** The badge being replaced, drawn over the incoming one as it blows out. */
  outgoing: { opacity: number; transform: string } | null;
  incoming: { opacity: number; transform: string };
  burst: { opacity: number; transform: string } | null;
}

/**
 * The badge turnover, as pure geometry over the sweep's own clock.
 *
 * Written as inline transforms rather than CSS keyframes on purpose. A
 * keyframed stamp only plays if the element remounts at exactly the right
 * moment and the compositor is running on wall time — neither is true when
 * the trailer harness drives the page frame by frame, which is how the
 * promotion previously got captured as a still of its own aftermath.
 */
const promotionFrameAt = (
  elapsedMs: number | null,
  crossMs: number | null,
): PromotionFrame | null => {
  if (elapsedMs === null || crossMs === null) {
    return null;
  }

  // Before the boundary: the badge leans in, nothing else exists yet.
  if (elapsedMs < crossMs) {
    const lean = clamp01(1 - (crossMs - elapsedMs) / PROMOTION_ANTICIPATION_MS);
    return {
      outgoing: null,
      incoming: {
        opacity: 1,
        transform: `scale(${(1 + 0.14 * easeOutCubic(lean)).toFixed(4)})`,
      },
      burst: null,
    };
  }

  const phase = clamp01((elapsedMs - crossMs) / PROMOTION_MS);
  if (phase >= 1) {
    return null;
  }

  const out = clamp01(phase / 0.3);
  const inn = clamp01((phase - 0.1) / 0.55);
  const ring = clamp01(phase / 0.8);

  return {
    outgoing: out >= 1
      ? null
      : {
        opacity: 1 - out,
        transform: `scale(${(1.14 + 0.9 * easeOutCubic(out)).toFixed(4)}) `
          + `rotate(${(-14 * out).toFixed(2)}deg)`,
      },
    incoming: {
      opacity: clamp01((phase - 0.1) / 0.22),
      transform: `scale(${(0.4 + 0.6 * easeOutBack(inn)).toFixed(4)}) `
        + `rotate(${(10 * (1 - easeOutCubic(inn))).toFixed(2)}deg)`,
    },
    burst: ring >= 1
      ? null
      : {
        opacity: 0.9 * (1 - ring) ** 1.6,
        transform: `scale(${(0.45 + 2.0 * easeOutCubic(ring)).toFixed(4)})`,
      },
  };
};

const ChevronsGlyph: React.FC<{ direction: 'up' | 'down' }> = ({ direction }) => (
  <svg
    className={`rating-reveal-chevrons is-${direction}`}
    viewBox="0 0 12 12"
    aria-hidden="true"
    focusable="false"
  >
    <path d="M1 5 6 0l5 5-2 2-3-3-3 3Zm0 5 5-5 5 5-2 2-3-3-3 3Z" />
  </svg>
);

export interface RatingRevealProps {
  state: MatchRatingState;
  /** Fires once after the odometer and any rank movement have landed. */
  onSettled?: () => void;
  /**
   * Milliseconds since the reveal began, supplied by an external clock. When
   * set, the sweep is a pure function of this value and `requestAnimationFrame`
   * is not used at all.
   *
   * The trailer capture harness drives the page frame-by-frame off a virtual
   * clock, where rAF either never fires or advances on wall time that has
   * nothing to do with the frame being captured — either way the odometer
   * finished long before frame 0 and the promotion was captured as a still.
   * Product surfaces omit this and keep the rAF path.
   */
  clockMs?: number;
}

const RatingReveal: React.FC<RatingRevealProps> = ({ state, onSettled, clockMs }) => {
  const reducedMotion = usePrefersReducedMotion();
  const reveal = state.phase === 'ready' ? state.reveal : null;
  const { displayMmr, stage, elapsedMs } = useAnimatedMmr(
    reveal,
    reducedMotion,
    clockMs ?? null,
  );
  const announcedRevealRef = useRef<RatingRevealModel | null>(null);

  useEffect(() => {
    if (state.phase !== 'ready') {
      announcedRevealRef.current = null;
      return;
    }
    if (stage === 'settled' && announcedRevealRef.current !== state.reveal) {
      announcedRevealRef.current = state.reveal;
      onSettled?.();
    }
  }, [onSettled, stage, state]);

  if (state.phase === 'idle' || state.phase === 'unavailable') {
    return null;
  }

  const isCompetitive = reveal?.queueMode !== 'Quickmatch';
  const ladderLabel = isCompetitive ? 'Competitive rating' : 'Casual rating';

  if (state.phase === 'pending' || reveal === null || displayMmr === null) {
    return (
      <section
        className="rating-reveal is-pending"
        aria-label={`${ladderLabel}: tallying`}
        data-testid="rating-reveal"
        data-phase="pending"
      >
        <div className="rating-reveal-main">
          <span className="rating-reveal-label">{ladderLabel}</span>
          <div className="rating-reveal-valueline">
            <strong className="rating-reveal-value is-placeholder">····</strong>
          </div>
          <div className="rating-reveal-meter is-indeterminate" aria-hidden="true">
            <span className="rating-reveal-meter-fill" />
          </div>
          <span className="rating-reveal-caption">
            <span className="rating-reveal-tallying">Tallying result…</span>
          </span>
        </div>
      </section>
    );
  }

  const settled = stage === 'settled';
  const delta = reveal.delta;
  const isPlacement = reveal.before === null;
  const rank = getRankFromMMR(displayMmr);
  const bandIndex = rankBandIndexForMmr(displayMmr);
  const band = RANK_BANDS[bandIndex];
  const accent = TIER_ACCENTS[rank.tier];
  const progress = rankBandProgress(displayMmr);
  const movement = reveal.movement;
  const showRibbon = settled && isCompetitive && !isPlacement && movement !== 'unchanged';
  const deltaTone = delta === null || delta === 0 ? 'zero' : delta > 0 ? 'up' : 'down';
  const positionsClimbed =
    reveal.before?.position != null && reveal.after.position != null
      ? reveal.before.position - reveal.after.position
      : null;

  const promotion = reducedMotion
    ? null
    : promotionFrameAt(elapsedMs, bandCrossMs(reveal));
  // The badge on the far side of the turnover is whichever one the sweep is
  // *not* showing yet: before the crossing that is the incoming rank, after it
  // the one being replaced.
  const otherRank = promotion?.outgoing
    ? reveal.fromRank
    : reveal.toRank;

  const announcement = showRibbon
    ? `${movement === 'promoted' ? 'Promoted to' : 'Demoted to'} ${formatRankLabel(reveal.toRank)}. `
    : '';
  const ariaSummary = `${ladderLabel}: ${reveal.after.mmr}` +
    (delta === null ? '' : ` (${delta >= 0 ? '+' : ''}${delta})`) +
    `. ${announcement}`;

  return (
    <section
      className={`rating-reveal${isCompetitive ? ' is-competitive' : ' is-casual'}${settled ? ' is-settled' : ' is-counting'}`}
      aria-label={ariaSummary}
      data-testid="rating-reveal"
      data-phase={settled ? 'settled' : 'counting'}
      data-movement={showRibbon ? movement : undefined}
      style={{ '--rating-accent': accent } as React.CSSProperties}
    >
      {showRibbon && movement === 'promoted' && (
        <span className="rating-reveal-beam" aria-hidden="true" />
      )}

      {isCompetitive && (
        // The turnover is inline geometry over the sweep's clock — see
        // `promotionFrameAt`. `rank` follows the odometer, so the badge that
        // survives the transition is always the one the number agrees with.
        <div
          className={`rating-reveal-medallion${promotion ? ' is-turning' : ''}`}
          aria-hidden="true"
        >
          {promotion?.burst && (
            <span
              className="rating-reveal-medallion-burst"
              style={promotion.burst}
            />
          )}
          {promotion?.outgoing && (
            <span
              className="rating-reveal-medallion-layer is-outgoing"
              style={promotion.outgoing}
            >
              <RankIcon tier={otherRank.tier} division={otherRank.division} />
            </span>
          )}
          <span
            className="rating-reveal-medallion-layer"
            style={promotion?.incoming}
          >
            <RankIcon tier={rank.tier} division={rank.division} />
          </span>
        </div>
      )}

      <div className="rating-reveal-main">
        <span className="rating-reveal-label">
          {ladderLabel}
          {isPlacement && <em className="rating-reveal-placed">Placed</em>}
        </span>

        <div className="rating-reveal-valueline">
          <strong className="rating-reveal-value" data-testid="rating-reveal-value">
            {displayMmr}
          </strong>
          {delta !== null && (
            <span
              className={`rating-reveal-delta is-${deltaTone}${settled ? ' is-shown' : ''}`}
              data-testid="rating-reveal-delta"
            >
              {delta > 0 ? `+${delta}` : delta === 0 ? '±0' : `${delta}`}
            </span>
          )}
          {settled && positionsClimbed !== null && reveal.after.position !== null && (
            <span className="rating-reveal-position">
              #{reveal.after.position}
              {positionsClimbed !== 0 && (
                <em className={positionsClimbed > 0 ? 'is-up' : 'is-down'}>
                  {positionsClimbed > 0 ? '▲' : '▼'}{Math.abs(positionsClimbed)}
                </em>
              )}
            </span>
          )}
        </div>

        {isCompetitive && (
          <>
            <div className="rating-reveal-meter" aria-hidden="true">
              <span
                className="rating-reveal-meter-fill"
                style={{ width: `${Math.max(1.5, progress * 100)}%` }}
              />
            </div>
            <span className="rating-reveal-caption" aria-hidden="true">
              {showRibbon ? (
                <span className={`rating-reveal-ribbon is-${movement}`}>
                  <ChevronsGlyph direction={movement === 'promoted' ? 'up' : 'down'} />
                  <span>
                    {movement === 'promoted' ? 'Promoted' : 'Demoted'}
                    {' — '}
                    {formatRankLabel(reveal.toRank)}
                  </span>
                  <ChevronsGlyph direction={movement === 'promoted' ? 'up' : 'down'} />
                </span>
              ) : (
                <>
                  <span className="rating-reveal-rank">{formatRankLabel(rank)}</span>
                  <span className="rating-reveal-bounds">
                    {band.min}–{band.max ?? '∞'}
                  </span>
                </>
              )}
            </span>
          </>
        )}
      </div>
    </section>
  );
};

export default RatingReveal;
