import React, { useEffect, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { api } from '../services/api';
import {
  getTickerGroupCopies,
  getTickerPollIntervalMs,
} from '../utils/newsTicker';
import type { NewsTickerPlayAction } from '../utils/newsTicker';
import type { NewsTickerItem } from '../types/generated';

const DEFAULT_NEWS_POLL_INTERVAL_MS = 60 * 1000;
const INITIAL_RETRY_INTERVAL_MS = 10 * 1000;
const MAX_RETRY_INTERVAL_MS = 60 * 1000;
const TICKER_SPEED_PX_PER_SECOND = 42;
const DEFAULT_TICKER_DURATION_SECONDS = 48;

interface TickerItemContentProps {
  item: NewsTickerItem;
  isVisualCopy?: boolean;
  onPlay?: (action: NewsTickerPlayAction) => void;
}

const TickerItemContent: React.FC<TickerItemContentProps> = ({
  item,
  isVisualCopy = false,
  onPlay,
}) => {
  const copy = <span className="news-ticker-copy">{item.text}</span>;
  const tabIndex = isVisualCopy ? -1 : undefined;
  const cta = item.cta;
  const action = cta?.action;

  return (
    <>
      {copy}
      {cta && action === 'viewLeaderboards' && (
        <Link
          className="news-ticker-link"
          to="/leaderboards"
          tabIndex={tabIndex}
        >
          <span className="news-ticker-cta">{cta.label}</span>
        </Link>
      )}
      {cta && action && action !== 'viewLeaderboards' && onPlay && (
        <button
          type="button"
          className="news-ticker-link"
          onClick={() => onPlay(action)}
          tabIndex={tabIndex}
        >
          <span className="news-ticker-cta">{cta.label}</span>
        </button>
      )}
    </>
  );
};

interface TickerGroupProps {
  items: NewsTickerItem[];
  groupRef?: React.Ref<HTMLDivElement>;
  copyIndex: number;
  onPlay?: (action: NewsTickerPlayAction) => void;
}

const TickerGroup: React.FC<TickerGroupProps> = ({
  items,
  groupRef,
  copyIndex,
  onPlay,
}) => (
  <div ref={groupRef} className="news-ticker-group">
    {items.map((item, index) => (
      <span
        className="news-ticker-item"
        data-kind={item.kind}
        key={`${copyIndex}-${item.id}-${index}`}
      >
        <span className="news-ticker-signal" />
        <TickerItemContent
          item={item}
          isVisualCopy
          onPlay={onPlay}
        />
      </span>
    ))}
  </div>
);

interface NewsTickerProps {
  onPlay?: (action: NewsTickerPlayAction) => void;
}

/**
 * Live arena headlines. The moving track is hidden from assistive technology;
 * its static list is the sole accessible copy and deliberately is not live, so
 * polling never interrupts a player who is navigating the start form.
 */
export const NewsTicker: React.FC<NewsTickerProps> = ({ onPlay }) => {
  const [items, setItems] = useState<NewsTickerItem[]>([]);
  const [durationSeconds, setDurationSeconds] = useState(
    DEFAULT_TICKER_DURATION_SECONDS,
  );
  const [groupCopies, setGroupCopies] = useState(2);
  const viewportRef = useRef<HTMLDivElement>(null);
  const firstGroupRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let active = true;
    let requestInFlight = false;
    let retryCount = 0;
    let timeoutId: number | undefined;

    const clearScheduledRefresh = (): void => {
      if (timeoutId !== undefined) {
        window.clearTimeout(timeoutId);
        timeoutId = undefined;
      }
    };

    const scheduleRefresh = (delayMs: number): void => {
      clearScheduledRefresh();
      if (!active) {
        return;
      }
      timeoutId = window.setTimeout(() => {
        if (document.visibilityState === 'visible') {
          void refresh();
        } else {
          scheduleRefresh(DEFAULT_NEWS_POLL_INTERVAL_MS);
        }
      }, delayMs);
    };

    const refresh = async (): Promise<void> => {
      if (!active || requestInFlight) {
        return;
      }

      requestInFlight = true;
      try {
        const response = await api.getNewsTicker();
        if (!active) {
          return;
        }

        const nextItems = response.items.filter(
          (item) => item.text.trim().length > 0,
        );
        setItems(nextItems);
        retryCount = 0;
        scheduleRefresh(getTickerPollIntervalMs(response.refreshAfterSeconds));
      } catch {
        // News is ambient, not blocking. Keep the last successful response and
        // retry quickly enough that a transient initial failure is invisible.
        if (active) {
          const delay = Math.min(
            MAX_RETRY_INTERVAL_MS,
            INITIAL_RETRY_INTERVAL_MS * 2 ** retryCount,
          );
          retryCount += 1;
          scheduleRefresh(delay);
        }
      } finally {
        requestInFlight = false;
      }
    };

    const handleVisibilityChange = (): void => {
      if (document.visibilityState === 'visible') {
        clearScheduledRefresh();
        void refresh();
      }
    };

    void refresh();
    document.addEventListener('visibilitychange', handleVisibilityChange);

    return () => {
      active = false;
      clearScheduledRefresh();
      document.removeEventListener('visibilitychange', handleVisibilityChange);
    };
  }, []);

  useEffect(() => {
    const group = firstGroupRef.current;
    const viewport = viewportRef.current;
    if (!group || !viewport || items.length === 0) {
      return undefined;
    }

    const measure = (): void => {
      const groupWidth = group.getBoundingClientRect().width;
      const viewportWidth = viewport.getBoundingClientRect().width;
      if (groupWidth <= 0 || viewportWidth <= 0) {
        return;
      }

      setDurationSeconds(groupWidth / TICKER_SPEED_PX_PER_SECOND);
      // After the leading group scrolls away, enough complete copies must
      // remain to cover the viewport. This also keeps a one-item feed seamless.
      setGroupCopies(getTickerGroupCopies(groupWidth, viewportWidth));
    };

    measure();
    if (typeof ResizeObserver === 'undefined') {
      window.addEventListener('resize', measure);
      return () => window.removeEventListener('resize', measure);
    }

    const observer = new ResizeObserver(measure);
    observer.observe(group);
    observer.observe(viewport);
    return () => observer.disconnect();
  }, [items]);

  const trackStyle: React.CSSProperties & {
    '--news-ticker-duration': string;
    '--news-ticker-shift': string;
  } = {
    '--news-ticker-duration': `${durationSeconds}s`,
    '--news-ticker-shift': `${-100 / groupCopies}%`,
  };

  return (
    <section
      className={`news-ticker${items.length === 0 ? ' is-empty' : ''}`}
      aria-label="Recent arena news"
      data-testid="news-ticker"
    >
      <ul className="visually-hidden news-ticker-accessible">
        {items.map((item, index) => (
          <li key={`accessible-${item.id}-${index}`}>
            <TickerItemContent item={item} onPlay={onPlay} />
          </li>
        ))}
      </ul>

      <div ref={viewportRef} className="news-ticker-viewport" aria-hidden="true">
        <div className="news-ticker-track" style={trackStyle}>
          {Array.from({ length: groupCopies }, (_, copyIndex) => (
            <TickerGroup
              key={`ticker-group-${copyIndex}`}
              items={items}
              copyIndex={copyIndex}
              groupRef={copyIndex === 0 ? firstGroupRef : undefined}
              onPlay={onPlay}
            />
          ))}
        </div>
      </div>
    </section>
  );
};
