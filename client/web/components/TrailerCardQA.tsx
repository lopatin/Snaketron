import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { useSearchParams } from 'react-router-dom';
import RatingReveal from './RatingReveal';
import { buildRatingReveal } from '../utils/ratingReveal';
import { getRankImage, getRankFromMMR } from '../utils/rank';
import {
  defaultFlowFieldSpacing,
  drawArenaFlowField,
} from '../utils/arenaFlowField';
import './TrailerCardQA.css';

/**
 * Dev-only capture surface for the trailer's non-gameplay frames.
 *
 * These cards exist so the trailer never draws a look-alike of the product
 * (see .claude/skills/snaketron-create-video/references/brand.md): the rank-up
 * card mounts the **real** `RatingReveal`, the rankings card uses the real rank
 * artwork, and the logo slates use the same drifting field as the home screen.
 *
 * Every card is a pure function of `elapsedMs` and exposes the standard capture
 * contract on `window.__SNAKETRON_CAPTURE__`, so a frame is reproducible and the
 * harness owns the clock. Nothing here self-schedules rAF.
 */

const clamp01 = (value: number): number => Math.min(1, Math.max(0, value));

/** Non-linear easings — the trailer never moves anything linearly. */
const easeOutCubic = (t: number): number => 1 - Math.pow(1 - clamp01(t), 3);
const easeInCubic = (t: number): number => Math.pow(clamp01(t), 3);
const easeOutBack = (t: number): number => {
  const c = 1.70158;
  const p = clamp01(t) - 1;
  return 1 + (c + 1) * p * p * p + c * p * p;
};

interface CardDefinition {
  durationMs: number;
  anchors: Record<string, number>;
}

const CARDS: Record<string, CardDefinition> = {
  'logo-intro': { durationMs: 2600, anchors: { logo: 0.45 } },
  'logo-outro': { durationMs: 3000, anchors: { logo: 0.2 } },
  'rank-up': { durationMs: 5000, anchors: { reveal: 0.6, settle: 2.4 } },
  rankings: { durationMs: 5000, anchors: { icons: 0.5 } },
};

const RANK_SHOWCASE = [700, 1250, 1550, 1950, 2350];

const FlowFieldBackdrop: React.FC<{ elapsedMs: number; intensity?: number }> = ({
  elapsedMs,
  intensity = 2.3,
}) => {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    const context = canvas?.getContext('2d');
    if (!canvas || !context) return;
    const rect = canvas.getBoundingClientRect();
    const ratio = window.devicePixelRatio || 1;
    canvas.width = Math.max(1, Math.round(rect.width * ratio));
    canvas.height = Math.max(1, Math.round(rect.height * ratio));
    context.setTransform(ratio, 0, 0, ratio, 0, 0);
    context.clearRect(0, 0, rect.width, rect.height);
    // The field fades up rather than snapping on, and runs hotter than the
    // site so it reads as a designed backdrop instead of texture behind copy.
    const fade = easeOutCubic(elapsedMs / 900);
    drawArenaFlowField({
      context,
      width: rect.width,
      height: rect.height,
      time: elapsedMs / 1000,
      spacing: defaultFlowFieldSpacing(rect.width),
      intensity: intensity * fade,
      quietStrength: 0.45,
    });
  }, [elapsedMs, intensity]);

  return <canvas ref={canvasRef} className="trailer-card__field" />;
};

const LogoSlate: React.FC<{ elapsedMs: number; mode: 'intro' | 'outro'; durationMs: number }> = ({
  elapsedMs,
  mode,
  durationMs,
}) => {
  const intro = mode === 'intro';
  const t = intro
    ? easeOutBack((elapsedMs - 250) / 900)
    : 1 - easeInCubic((elapsedMs - (durationMs - 900)) / 900);
  const drift = intro ? (1 - easeOutCubic((elapsedMs - 250) / 1100)) * 26 : 0;
  const opacity = intro
    ? easeOutCubic((elapsedMs - 250) / 700)
    : 1 - easeInCubic((elapsedMs - (durationMs - 900)) / 900);

  return (
    <div className="trailer-card trailer-card--logo">
      <FlowFieldBackdrop elapsedMs={elapsedMs} />
      <div
        className="trailer-card__lockup"
        style={{
          opacity,
          transform: `translateY(${drift}px) scale(${0.94 + 0.06 * clamp01(t)})`,
        }}
      >
        <img src="SnaketronLogo.png" alt="SnakeTron" />
        <span
          className="trailer-card__tagline"
          style={{ opacity: intro ? easeOutCubic((elapsedMs - 900) / 700) : opacity }}
        >
          Competitive multiplayer Snake
        </span>
      </div>
    </div>
  );
};

const RankUpCard: React.FC<{ elapsedMs: number }> = ({ elapsedMs }) => {
  // Diamond IV (2280) -> Grand Master I (2320): a real band crossing, so the
  // component runs its genuine promotion path rather than a posed frame.
  const state = useMemo(
    () => ({
      phase: 'ready' as const,
      reveal: buildRatingReveal(
        'Competitive',
        '2v2',
        { mmr: 2280, wins: 61, losses: 40, position: 12 },
        { mmr: 2320, wins: 62, losses: 40, position: 9 },
      ),
    }),
    [],
  );
  const enter = easeOutCubic((elapsedMs - 120) / 520);
  return (
    <div className="trailer-card trailer-card--rank">
      <FlowFieldBackdrop elapsedMs={elapsedMs} intensity={1.5} />
      <div
        className="trailer-card__reveal"
        style={{
          opacity: enter,
          transform: `translateY(${(1 - enter) * 22}px) scale(${(0.97 + 0.03 * enter) * 2.9})`,
        }}
      >
        <RatingReveal state={state} />
      </div>
    </div>
  );
};

const RankingsCard: React.FC<{ elapsedMs: number }> = ({ elapsedMs }) => (
  <div className="trailer-card trailer-card--rankings">
    <FlowFieldBackdrop elapsedMs={elapsedMs} intensity={1.5} />
    <ol className="trailer-card__ranks">
      {RANK_SHOWCASE.map((mmr, index) => {
        const rank = getRankFromMMR(mmr);
        const t = easeOutBack((elapsedMs - 260 - index * 130) / 620);
        return (
          <li
            key={mmr}
            style={{
              opacity: easeOutCubic((elapsedMs - 260 - index * 130) / 420),
              transform: `translateY(${(1 - clamp01(t)) * 26}px) scale(${
                0.86 + 0.14 * clamp01(t)
              })`,
            }}
          >
            <img src={getRankImage(rank.tier)} alt={rank.tier} />
          </li>
        );
      })}
      <li
        className="trailer-card__trophy"
        style={{
          opacity: easeOutCubic((elapsedMs - 260 - RANK_SHOWCASE.length * 130) / 420),
          transform: `scale(${
            0.86 +
            0.14 * clamp01(easeOutBack((elapsedMs - 260 - RANK_SHOWCASE.length * 130) / 620))
          })`,
        }}
      >
        {/* The Classic/solo ladder marks its leader with this trophy rather
            than a rank badge (Leaderboard.tsx:226-240) — same path data, so
            the trailer shows the real mark. */}
        <svg
          viewBox="0 0 64 64"
          fill="none"
          stroke="currentColor"
          strokeWidth="2.5"
          strokeLinecap="round"
          strokeLinejoin="round"
          role="img"
          aria-label="Solo trophy"
        >
          <path d="M19 10h26v11c0 10-5.8 17-13 17s-13-7-13-17V10Z" />
          <path d="M19 15H9v4c0 7.8 4.5 12.5 11.8 13.4" />
          <path d="M45 15h10v4c0 7.8-4.5 12.5-11.8 13.4" />
          <path d="M32 38v9" />
          <path d="M25 47h14l3 7H22l3-7Z" />
        </svg>
      </li>
    </ol>
  </div>
);

const TrailerCardQA: React.FC = () => {
  const [params] = useSearchParams();
  const card = params.get('card') ?? 'logo-intro';
  const definition = CARDS[card] ?? CARDS['logo-intro'];
  // The harness may override the card's length (--duration-ms); animations
  // must follow it, or a longer capture just holds a finished frame.
  const durationMs = Number(params.get('ms')) || definition.durationMs;
  const [elapsedMs, setElapsedMs] = useState(0);
  const elapsedRef = useRef(0);

  // flushSync + a forced layout read, deliberately instead of awaiting rAF:
  // the capture harness may be driving a virtual clock where rAF never fires,
  // and a hidden browser pane freezes it outright. The frame must be fully
  // committed by the time stepMs resolves.
  const step = useCallback((deltaMs: number) => {
    elapsedRef.current += deltaMs;
    flushSync(() => setElapsedMs(elapsedRef.current));
    void document.body.offsetHeight;
  }, []);

  useEffect(() => {
    const api = {
      ready: async (): Promise<void> => {
        await (document as Document & { fonts?: FontFaceSet }).fonts?.ready;
        await new Promise((resolve) => window.setTimeout(resolve, 60));
      },
      durationMs: () => durationMs,
      stepMs: async (ms: number): Promise<void> => {
        step(Number(ms));
      },
      renderedTick: () => Math.round(elapsedRef.current),
      cueTrack: () => ({
        anchors: definition.anchors,
        duration: definition.durationMs / 1000,
        capture_vfps: 60,
        encoded_fps: 60,
      }),
    };
    (window as unknown as Record<string, unknown>).__SNAKETRON_CAPTURE__ = api;
    (window as unknown as Record<string, unknown>).__snaketronCapture = api;
  }, [definition, durationMs, step]);

  if (card === 'rank-up') return <RankUpCard elapsedMs={elapsedMs} />;
  if (card === 'rankings') return <RankingsCard elapsedMs={elapsedMs} />;
  return (
    <LogoSlate
      elapsedMs={elapsedMs}
      mode={card === 'logo-outro' ? 'outro' : 'intro'}
      durationMs={durationMs}
    />
  );
};

export default TrailerCardQA;
