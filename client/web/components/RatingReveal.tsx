import React, { useEffect, useState } from 'react';
import type { RankTier } from '../types';
import {
  formatRankLabel,
  getRankFromMMR,
  rankBandIndexForMmr,
  rankBandProgress,
  RANK_BANDS,
} from '../utils/rank';
import RankIcon from './RankIcon';
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

const easeOutCubic = (t: number): number => 1 - (1 - t) ** 3;

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

const useAnimatedMmr = (
  reveal: RatingRevealModel | null,
  reducedMotion: boolean,
): { displayMmr: number | null; stage: RevealStage } => {
  const [displayMmr, setDisplayMmr] = useState<number | null>(null);
  const [stage, setStage] = useState<RevealStage>('counting');

  useEffect(() => {
    if (reveal === null) {
      return;
    }
    const from = reveal.before?.mmr ?? reveal.after.mmr;
    const to = reveal.after.mmr;
    if (reducedMotion || from === to) {
      setDisplayMmr(to);
      setStage('settled');
      return;
    }

    setDisplayMmr(from);
    setStage('counting');
    const duration = countDurationMs(to - from);
    let frame = 0;
    let startedAt: number | null = null;
    const step = (now: number) => {
      if (startedAt === null) {
        startedAt = now;
      }
      const t = Math.min(1, Math.max(0, (now - startedAt - COUNT_START_DELAY_MS) / duration));
      setDisplayMmr(Math.round(from + (to - from) * easeOutCubic(t)));
      if (t >= 1) {
        setStage('settled');
        return;
      }
      frame = requestAnimationFrame(step);
    };
    frame = requestAnimationFrame(step);
    return () => cancelAnimationFrame(frame);
  }, [reveal, reducedMotion]);

  return { displayMmr, stage };
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
}

const RatingReveal: React.FC<RatingRevealProps> = ({ state }) => {
  const reducedMotion = usePrefersReducedMotion();
  const reveal = state.phase === 'ready' ? state.reveal : null;
  const { displayMmr, stage } = useAnimatedMmr(reveal, reducedMotion);

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
        // Keyed on the band so crossing a division remounts the badge and
        // replays its stamp-in — the promotion is visible mid-sweep.
        <div className="rating-reveal-medallion" key={bandIndex} aria-hidden="true">
          <RankIcon tier={rank.tier} division={rank.division} />
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
