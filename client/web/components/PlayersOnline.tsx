import React, { useEffect, useRef, useState } from 'react';

/**
 * Live global presence readout: a tag notched into a break in the nickname
 * field's top border, the way a legend sits in a fieldset, binding the current
 * player population to the form as a single object.
 *
 * Renders inside the field's positioned wrapper and is absolutely placed, so
 * it costs the stack no vertical space.
 *
 * `count` is `null` while the population is still unknown (regions loading or
 * unreachable); the tag keeps its footprint so nothing shifts when it lands.
 */
interface PlayersOnlineProps {
  count: number | null;
}

const COUNT_UP_DURATION_MS = 760;
/** Deltas smaller than this snap — a single player joining shouldn't tween. */
const MIN_ANIMATED_DELTA = 3;
const TICK_HIGHLIGHT_MS = 420;

const prefersReducedMotion = (): boolean =>
  typeof window !== 'undefined' &&
  typeof window.matchMedia === 'function' &&
  window.matchMedia('(prefers-reduced-motion: reduce)').matches;

const easeOutCubic = (progress: number): number => 1 - Math.pow(1 - progress, 3);

/** Tween the rendered figure toward `target`, snapping on small or no-motion updates. */
function useCountUp(target: number | null): number {
  const [displayed, setDisplayed] = useState(0);
  const displayedRef = useRef(0);
  const frameRef = useRef<number | null>(null);

  useEffect(() => {
    if (target === null) {
      return;
    }

    const from = displayedRef.current;
    const delta = target - from;
    if (delta === 0) {
      return;
    }

    const commit = (value: number): void => {
      displayedRef.current = value;
      setDisplayed(value);
    };

    if (Math.abs(delta) < MIN_ANIMATED_DELTA || prefersReducedMotion()) {
      commit(target);
      return;
    }

    let startedAt: number | null = null;
    const step = (now: number): void => {
      if (startedAt === null) {
        startedAt = now;
      }
      const progress = Math.min(1, (now - startedAt) / COUNT_UP_DURATION_MS);
      commit(Math.round(from + delta * easeOutCubic(progress)));
      frameRef.current = progress < 1 ? requestAnimationFrame(step) : null;
    };

    frameRef.current = requestAnimationFrame(step);
    return () => {
      if (frameRef.current !== null) {
        cancelAnimationFrame(frameRef.current);
        frameRef.current = null;
      }
    };
  }, [target]);

  return displayed;
}

/** Brief accent flash whenever the authoritative count moves. */
function useTickHighlight(target: number | null): boolean {
  const [isTicking, setIsTicking] = useState(false);
  const previousRef = useRef<number | null>(null);

  useEffect(() => {
    const previous = previousRef.current;
    previousRef.current = target;

    if (target === null || previous === null || previous === target) {
      return;
    }

    setIsTicking(true);
    const timer = setTimeout(() => setIsTicking(false), TICK_HIGHLIGHT_MS);
    return () => clearTimeout(timer);
  }, [target]);

  return isTicking;
}

export const PlayersOnline: React.FC<PlayersOnlineProps> = ({ count }) => {
  const isLive = count !== null;
  const displayed = useCountUp(count);
  const isTicking = useTickHighlight(count);

  const noun = count === 1 ? 'player' : 'players';
  const rootClass = ['players-online', isLive ? 'is-live' : '', count === 0 ? 'is-empty' : '']
    .filter(Boolean)
    .join(' ');

  return (
    <div className={rootClass} role="status" aria-live="polite">
      <span className="players-online-readout">
        <span className="players-online-dot" aria-hidden="true" />
        {isLive ? (
          <span className="players-online-figures" aria-hidden="true">
            <span className={`players-online-count${isTicking ? ' is-ticking' : ''}`}>
              {displayed.toLocaleString()}
            </span>
            <span className="players-online-label">{noun} online</span>
          </span>
        ) : (
          <span className="players-online-label" aria-hidden="true">
            Connecting
          </span>
        )}
        {isLive ? <span className="sr-only">{`${count} ${noun} online`}</span> : null}
      </span>
    </div>
  );
};
