import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { flushSync } from 'react-dom';
import { useSearchParams } from 'react-router-dom';
import RatingReveal from './RatingReveal';
import { buildRatingReveal } from '../utils/ratingReveal';
import { getRankFromMMR } from '../utils/rank';
import RankIcon from './RankIcon';
import SoloTrophyIcon from './SoloTrophyIcon';
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
const easeInOutCubic = (t: number): number => {
  const p = clamp01(t);
  return p < 0.5 ? 4 * p * p * p : 1 - Math.pow(-2 * p + 2, 3) / 2;
};
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
  // Three beats: the logo drops in, the call to action snaps in after it,
  // then both leave together (see OUTRO).
  'logo-outro': { durationMs: 3600, anchors: { logo: 0.22, cta: 0.98 } },
  // The sweep runs 0.55s–1.97s, crossing into Grand Master at ~1.26s (the
  // odometer eases in *and* out, so the boundary lands mid-count rather than
  // in the first fifth). The card is that plus a beat to read the ribbon.
  'rank-up': { durationMs: 2900, anchors: { reveal: 0.55, promote: 1.26, settle: 1.97 } },
  rankings: { durationMs: 8000, anchors: { icons: 0.5, ladder: 3.4 } },
};

const RANK_SHOWCASE = [900, 1350, 1700, 2100, 2450];

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

/**
 * The end slate is a sequence on the way in — the logo lands, then the call to
 * action snaps in after it — and then it simply stays. Nothing animates out.
 *
 * A trailer's last frame is the one a paused or looping player sits on, so it
 * has to be the finished lockup and not whatever a fade happened to leave
 * behind. That is the opposite of every other card here, where a held frame is
 * the defect.
 *
 * `OUTRO` is expressed in ms rather than fractions so the beats keep their
 * feel if the card's length is retimed.
 */
const OUTRO = {
  logoIn: 220,
  logoInMs: 780,
  ctaIn: 980,
  ctaInMs: 620,
};

/**
 * The logo is *dropped* in — one fast vertical move on a single axis, married
 * to the fade. It is never lifted back out; see `OUTRO`.
 *
 * It used to enter on a slow rise combined with a scale ramp, which read as a
 * diagonal parallax float: two simultaneous transforms at different rates make
 * the eye infer depth, and a wordmark drifting through space is the wrong
 * register for a game whose whole language is snapped-to-grid. Vertical only,
 * fast, with a small overshoot to give it weight.
 */
const LOGO_DROP_PX = 190;
const LOGO_DROP_MS = 560;

const LogoSlate: React.FC<{ elapsedMs: number; mode: 'intro' | 'outro'; durationMs: number }> = ({
  elapsedMs,
  mode,
  durationMs,
}) => {
  const intro = mode === 'intro';
  const dropStart = intro ? 250 : OUTRO.logoIn;
  // easeOutBack lands it slightly past centre and settles back: the overshoot
  // is what makes a drop read as a drop rather than a slide.
  const drop = easeOutBack((elapsedMs - dropStart) / LOGO_DROP_MS);
  const offsetY = -(1 - drop) * LOGO_DROP_PX;
  const opacity = easeOutCubic((elapsedMs - dropStart) / 320);

  // The call to action arrives on a damped wiggle — the one deliberately
  // playful move in the film, and the last thing a viewer sees.
  const ctaT = clamp01((elapsedMs - OUTRO.ctaIn) / OUTRO.ctaInMs);
  const ctaSettle = easeOutBack(ctaT);
  const ctaWiggle = (1 - ctaT) ** 2 * Math.sin(ctaT * 22) * 9;
  // The accent bar wipes out from the centre *first*: the words are knocked
  // out in white, so any part of them that lands ahead of the bar is white on
  // paper and simply missing.
  const ctaBar = easeOutCubic((elapsedMs - OUTRO.ctaIn) / 240);
  const ctaOpacity = clamp01((elapsedMs - OUTRO.ctaIn - 170) / 200);

  return (
    <div className="trailer-card trailer-card--logo">
      <FlowFieldBackdrop elapsedMs={elapsedMs} />
      <div
        className="trailer-card__lockup"
        style={{
          opacity,
          transform: `translateY(${offsetY.toFixed(2)}px)`,
        }}
      >
        <img src="SnaketronLogo.png" alt="SnakeTron" />
        <span
          className="trailer-card__tagline"
          style={{
            opacity: intro
              ? easeOutCubic((elapsedMs - dropStart - 480) / 520)
              : opacity,
          }}
        >
          Competitive multiplayer Snake
        </span>
      </div>
      {!intro && (
        <div
          className="trailer-card__cta"
          style={{
            opacity: clamp01((elapsedMs - OUTRO.ctaIn) / 120),
            transform: `translateY(${((1 - ctaSettle) * 30).toFixed(2)}px) `
              + `rotate(${ctaWiggle.toFixed(2)}deg) `
              + `scale(${(0.82 + 0.18 * ctaSettle).toFixed(3)})`,
          }}
        >
          <span
            className="trailer-card__cta-bar"
            style={{ transform: `scaleX(${clamp01(ctaBar).toFixed(3)})` }}
          />
          <span className="trailer-card__cta-text" style={{ opacity: ctaOpacity }}>
            Play free
          </span>
        </div>
      )}
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
        { mmr: 2280, wins: 61, losses: 40 },
        { mmr: 2320, wins: 62, losses: 40 },
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
        {/* The odometer must run on the harness clock, not rAF: see the
            `clockMs` prop docs. Without it the promotion is captured as a
            still of its own aftermath. */}
        <RatingReveal state={state} clockMs={elapsedMs} />
      </div>
    </div>
  );
};

/**
 * Ladder showcase. Two beats:
 *
 *  1. the rank badges stamp in under the caption, exactly as before;
 *  2. the whole lockup lifts to the top and shrinks to make room for a real
 *     leaderboard, which scrolls away under an accelerating ramp until the
 *     rows smear into a vertical blur — the point being *how many* players
 *     are ranked, not any individual row.
 *
 * The caption lives inside the card (rather than being burnt on by ffmpeg)
 * precisely because it has to move with the lockup. It is styled to match the
 * global caption band so the cut reads consistently.
 */

const LADDER_TOTAL = 12480;

const NAME_PARTS_A = [
  'Grid', 'Neon', 'Apex', 'Viper', 'Coil', 'Flux', 'Nova', 'Byte',
  'Quantum', 'Rogue', 'Echo', 'Pulse', 'Cinder', 'Vector', 'Onyx', 'Halo',
];
const NAME_PARTS_B = [
  'rider', 'fang', 'loop', 'strike', 'wire', 'drift', 'coil', 'runner',
  'byte', 'spark', 'trail', 'snap', 'weave', 'dash', 'flare', 'lock',
];

/** Deterministic filler ladder — no clock, no RNG, identical every capture. */
const LADDER_ROWS = Array.from({ length: 90 }, (_, index) => {
  const a = NAME_PARTS_A[(index * 7 + 3) % NAME_PARTS_A.length];
  const b = NAME_PARTS_B[(index * 11 + 5) % NAME_PARTS_B.length];
  const suffix = index % 4 === 0 ? String(((index * 37) % 89) + 10) : '';
  return {
    place: index + 1,
    name: `${a}${b}${suffix}`,
    mmr: 2570 - index * 13 - ((index * 17) % 9),
  };
});

const ROW_HEIGHT_PX = 58;

const RankingsCard: React.FC<{ elapsedMs: number; durationMs: number }> = ({
  elapsedMs,
  durationMs,
}) => {
  // Beat boundaries, as fractions of the card's runtime.
  const liftStart = durationMs * 0.3;
  const liftEnd = durationMs * 0.42;
  // The ladder appears and is already moving *before* the badges get out of
  // its way: waiting for the lift to finish left a dead beat where the card
  // had visibly changed its mind but nothing was happening yet.
  const ladderStart = liftStart - durationMs * 0.06;
  const scrollStart = ladderStart + durationMs * 0.02;
  const scrollEnd = durationMs * 0.9;

  const lift = easeInOutCubic((elapsedMs - liftStart) / (liftEnd - liftStart));
  const scroll = clamp01((elapsedMs - scrollStart) / (scrollEnd - scrollStart));

  // Speed ramps up rather than running flat: gentle enough at first to read a
  // few names, then fast enough that the rows become texture.
  const travel = Math.pow(scroll, 2.7);
  const offset = travel * ROW_HEIGHT_PX * (LADDER_ROWS.length - 6);
  // Blur tracks the derivative of the ramp, so it appears only once the list
  // is genuinely moving fast.
  const speed = 2.7 * Math.pow(Math.max(scroll, 0), 1.7);
  const blur = clamp01(speed / 2.2) * 26;
  // The ladder leaves on opacity alone. Pulling it back at the same time it
  // smeared read as two competing moves — the rows were already dissolving,
  // and shrinking the frame around them just made the shot feel retracted.
  const listOpacity =
    easeOutCubic((elapsedMs - ladderStart) / 520) *
    (1 - easeInCubic((elapsedMs - scrollEnd) / (durationMs - scrollEnd)));

  return (
    <div className="trailer-card trailer-card--rankings">
      <FlowFieldBackdrop elapsedMs={elapsedMs} intensity={1.5} />
      <svg className="trailer-card__filters" aria-hidden="true">
        <defs>
          <filter id="ladder-vblur" x="-10%" y="-30%" width="120%" height="160%">
            <feGaussianBlur stdDeviation={`0 ${blur.toFixed(2)}`} />
          </filter>
        </defs>
      </svg>

      <div
        className="trailer-card__ladder-lockup"
        style={{ transform: `translateY(${-lift * 16}vh)` }}
      >
        <p className="trailer-card__caption">
          Global multiplayer and Classic Snake Leaderboards
        </p>
        <ol
          className="trailer-card__ranks"
          style={{ transform: `scale(${1 - lift * 0.46})` }}
        >
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
                <RankIcon tier={rank.tier} division={rank.division} label={rank.tier} />
              </li>
            );
          })}
          <li className="trailer-card__trophy">
            <SoloTrophyIcon label="Solo trophy" />
          </li>
        </ol>
      </div>

      <div
        className="trailer-card__ladder"
        style={{ opacity: clamp01(listOpacity) }}
      >
        <div className="trailer-card__ladder-head">
          <span>Global · Season 1</span>
          <strong>{LADDER_TOTAL.toLocaleString('en-US')} ranked players</strong>
        </div>
        <div className="trailer-card__ladder-window">
          <ol
            className="trailer-card__ladder-rows"
            style={{
              transform: `translateY(${-offset}px)`,
              filter: blur > 0.4 ? 'url(#ladder-vblur)' : undefined,
            }}
          >
            {LADDER_ROWS.map((row) => {
              const rank = getRankFromMMR(row.mmr);
              return (
                <li key={row.place}>
                  <span className="trailer-card__ladder-place">{row.place}</span>
                  <RankIcon
                    tier={rank.tier}
                    division={rank.division}
                    className="trailer-card__ladder-icon"
                  />
                  <span className="trailer-card__ladder-name">{row.name}</span>
                  <span className="trailer-card__ladder-mmr">{row.mmr}</span>
                </li>
              );
            })}
          </ol>
        </div>
      </div>
    </div>
  );
};

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
  if (card === 'rankings')
    return <RankingsCard elapsedMs={elapsedMs} durationMs={durationMs} />;
  return (
    <LogoSlate
      elapsedMs={elapsedMs}
      mode={card === 'logo-outro' ? 'outro' : 'intro'}
      durationMs={durationMs}
    />
  );
};

export default TrailerCardQA;
