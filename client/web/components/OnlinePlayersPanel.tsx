import React, { useCallback, useEffect, useMemo, useState } from 'react';
import type { OnlinePlayer } from '../types';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import { findPendingChallenge } from '../utils/challengePresentation';
import {
  measureContentBand,
  resolvePanelPlacement,
  type PanelSide,
} from '../utils/panelPlacement';
import { fallbackRegionName, resolveRegionName } from '../utils/regionName';

/**
 * Everyone online in this region, as a floating column of names.
 *
 * Black and translucent because it floats over whichever screen you are on —
 * the app's white paper surfaces are page furniture, and this is not part of
 * the page. Square-cornered and unadorned for the same reason: it is a list,
 * not a card, and every rule or radius around a name is one more thing between
 * the reader and the names.
 *
 * It lives in whichever margin the centered content leaves free, and minimizes
 * itself when there is no margin to live in — a narrow window, or a modal that
 * owns the screen.
 */
export interface OnlinePlayersPanelProps {
  /** Hidden entirely on screens where a floating panel would be in the way. */
  enabled?: boolean;
}

/** Kept in step with `.online-players` in index.css. */
const PANEL_WIDTH = 176;
const EDGE_GAP = 16;

const ACTIVITY_LABELS: Record<OnlinePlayer['activity'], string> = {
  idle: 'Available',
  lobby: 'In a lobby',
  playing: 'In a match',
};

/** Chevron that points at what a click will do: down to open, up to close. */
const Caret: React.FC<{ expanded: boolean }> = ({ expanded }) => (
  <svg
    className={`online-players-caret${expanded ? ' is-expanded' : ''}`}
    viewBox="0 0 12 12"
    aria-hidden="true"
    focusable="false"
  >
    <path
      d="M2.5 4.5 6 8l3.5-3.5"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </svg>
);

export const OnlinePlayersPanel: React.FC<OnlinePlayersPanelProps> = ({ enabled = true }) => {
  const { user } = useAuth();
  const { onlinePlayers, challenges, challengeError, challengePlayer, dismissChallengeError } =
    useWebSocket();
  const [side, setSide] = useState<PanelSide>('right');
  const [fits, setFits] = useState(true);
  // `null` means the reader has not decided, so the fit measurement is what
  // governs. Once they collapse or expand it by hand, that choice sticks.
  const [manuallyCollapsed, setManuallyCollapsed] = useState<boolean | null>(null);

  const regionId = onlinePlayers?.region ?? '';
  const [regionName, setRegionName] = useState(() => fallbackRegionName(regionId));

  const players = useMemo(() => onlinePlayers?.players ?? [], [onlinePlayers]);
  const nowMs = Date.now();

  useEffect(() => {
    if (!regionId) {
      return undefined;
    }
    let cancelled = false;
    setRegionName(fallbackRegionName(regionId));
    void resolveRegionName(regionId).then((name) => {
      if (!cancelled) {
        setRegionName(name);
      }
    });
    return () => {
      cancelled = true;
    };
  }, [regionId]);

  const remeasure = useCallback(() => {
    if (typeof window === 'undefined') {
      return;
    }
    const viewportWidth = window.innerWidth;
    const { contentLeft, contentRight } = measureContentBand(
      document.querySelector('main'),
      viewportWidth,
    );
    const placement = resolvePanelPlacement({
      viewportWidth,
      contentLeft,
      contentRight,
      panelWidth: PANEL_WIDTH,
      edgeGap: EDGE_GAP,
      modalOpen: document.querySelector('[aria-modal="true"]') !== null,
    });
    setSide(placement.side);
    setFits(placement.fits);
  }, []);

  useEffect(() => {
    if (!enabled) {
      return undefined;
    }
    remeasure();

    let frame: number | null = null;
    const schedule = () => {
      if (frame !== null) {
        return;
      }
      frame = window.requestAnimationFrame(() => {
        frame = null;
        remeasure();
      });
    };

    window.addEventListener('resize', schedule);
    // A modal opening or the content reflowing changes the answer, and neither
    // fires a resize. Coalesced to one measurement per frame so a chatty
    // subtree cannot turn this into a layout thrash.
    const observer = new MutationObserver(schedule);
    observer.observe(document.body, { childList: true, subtree: true });

    return () => {
      window.removeEventListener('resize', schedule);
      observer.disconnect();
      if (frame !== null) {
        window.cancelAnimationFrame(frame);
      }
    };
  }, [enabled, remeasure]);

  if (!enabled || !onlinePlayers) {
    return null;
  }

  const isCollapsed = manuallyCollapsed ?? !fits;
  const othersOnline = Math.max(0, onlinePlayers.total_online - 1);

  return (
    <section
      className={`online-players is-${side}${isCollapsed ? ' is-collapsed' : ''}`}
      aria-label={`Players online in ${regionName || 'your region'}`}
      data-testid="online-players-panel"
      data-side={side}
    >
      <button
        type="button"
        className="online-players-toggle"
        onClick={() => setManuallyCollapsed(!isCollapsed)}
        aria-expanded={!isCollapsed}
        data-testid="online-players-toggle"
      >
        <span className="online-players-dot" aria-hidden="true" />
        <span className="online-players-region">
          {regionName}
          {` (${othersOnline})`}
        </span>
        <Caret expanded={!isCollapsed} />
      </button>

      {!isCollapsed && (
        <div className="online-players-list" data-testid="online-players-list">
          {players.length === 0 ? (
            <p className="online-players-empty">No one else is here right now.</p>
          ) : (
            <ul>
              {players.map((player) => {
                const pending = findPendingChallenge(challenges, player.user_id, nowMs);
                const isSelf = user?.id === player.user_id;
                const activity = ACTIVITY_LABELS[player.activity];
                return (
                  <li key={player.user_id} className={`online-player is-${player.activity}`}>
                    {/* Being in the list is what says "online"; the name's own
                        weight says whether they can play right now. The words
                        stay available on hover for anyone who wants them. */}
                    <span className="online-player-name" title={`${player.username} — ${activity}`}>
                      {player.username}
                    </span>
                    {pending ? (
                      <span className="online-player-pending">
                        {pending.direction === 'outgoing' ? 'Sent' : 'Waiting'}
                      </span>
                    ) : (
                      !isSelf && (
                        <button
                          type="button"
                          className="online-player-challenge"
                          onClick={() => challengePlayer(player.user_id)}
                          data-testid={`challenge-player-${player.user_id}`}
                        >
                          Challenge
                        </button>
                      )
                    )}
                  </li>
                );
              })}
            </ul>
          )}
          {players.length < othersOnline && (
            <p className="online-players-overflow">+{othersOnline - players.length} more</p>
          )}
        </div>
      )}

      {challengeError && !isCollapsed && (
        <p className="online-players-error" role="alert">
          <span>{challengeError}</span>
          <button type="button" onClick={dismissChallengeError} aria-label="Dismiss">
            ✕
          </button>
        </p>
      )}
    </section>
  );
};

export default OnlinePlayersPanel;
