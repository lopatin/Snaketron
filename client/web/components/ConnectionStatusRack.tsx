import React from 'react';
import { useConnectionBanner } from '../hooks/useConnectionBanner';
import type { HomeNotice } from '../utils/homeNotice';

interface ConnectionStatusRackProps {
  /** From `isConnectionReady` — whether the client can act right now. */
  isReady: boolean;
  regionsLoading: boolean;
  regionsError: string | null;
  hasSelectedRegion: boolean;
  /**
   * A one-shot message from whatever redirected here, shown as one more badge.
   *
   * Unlike the connection badges it is not derived from live state, so it
   * cannot clear itself — hence the dismiss control.
   */
  notice?: HomeNotice | null;
  onDismissNotice?: () => void;
}

/**
 * The status badges above the home and leaderboard content.
 *
 * The rack itself is always mounted; only the badges inside it come and go. A
 * live region has to be a stable node to announce reliably, and the rack is
 * absolutely positioned with `pointer-events: none`, so an empty one costs
 * nothing and shifts nothing.
 */
export const ConnectionStatusRack: React.FC<ConnectionStatusRackProps> = ({
  isReady,
  regionsLoading,
  regionsError,
  hasSelectedRegion,
  notice = null,
  onDismissNotice,
}) => {
  const showConnecting = useConnectionBanner(isReady);
  const showRegionError = Boolean(regionsError);
  const showRegionReminder = !regionsLoading && !showRegionError && !hasSelectedRegion;

  return (
    <div className="home-status-rack" aria-live="polite" aria-label="Connection status">
      {regionsLoading && (
        <div className="home-status-badge">
          <span className="home-status-spinner" aria-hidden="true" />
          <span>Loading region data…</span>
        </div>
      )}
      {showRegionError && !regionsLoading && (
        <div className="home-status-badge is-warning">
          <span className="home-status-spinner" aria-hidden="true" />
          <span>Retrying region data…</span>
        </div>
      )}
      {showRegionReminder && (
        <div className="home-status-badge is-warning">
          <span>Select a region to continue</span>
        </div>
      )}
      {showConnecting && (
        <div className="home-status-badge is-warning">
          <span className="home-status-dot" aria-hidden="true" />
          <span>Connecting to game server…</span>
        </div>
      )}
      {notice && (
        <div
          data-testid="home-notice"
          className={`home-status-badge is-prose${notice.tone === 'error' ? ' is-error' : ''}`}
        >
          <span>{notice.message}</span>
          {onDismissNotice && (
            <button
              type="button"
              aria-label="Dismiss notification"
              onClick={onDismissNotice}
              className="home-status-dismiss"
            >
              ×
            </button>
          )}
        </div>
      )}
    </div>
  );
};
