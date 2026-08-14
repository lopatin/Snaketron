import React, { useEffect, useMemo, useRef, useState } from 'react';
import type {
  MatchHistoryPage,
  MatchHistoryPlayer,
  MatchHistorySummary,
} from '../types';
import { HistoryIcon } from './Icons';

interface MatchHistoryListProps {
  variant: 'compact' | 'admin';
  loadPage: (cursor: string | null) => Promise<MatchHistoryPage>;
  currentUserId?: number;
  resetKey?: string;
  emptyMessage?: string;
}

const formatDateTime = (timestampMs: number): string => {
  if (!Number.isFinite(timestampMs)) return 'Unknown time';
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: 'medium',
    timeStyle: 'short',
  }).format(new Date(timestampMs));
};

const formatDuration = (durationMs: number): string => {
  const totalSeconds = Math.max(0, Math.round(durationMs / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}:${seconds.toString().padStart(2, '0')}`;
};

const signed = (value: number): string => (value > 0 ? `+${value}` : String(value));

const normalizeOutcome = (outcome: string): string => {
  const normalized = outcome.trim().toLowerCase();
  if (normalized === 'win' || normalized === 'won' || normalized === 'winner') return 'win';
  if (normalized === 'loss' || normalized === 'lost' || normalized === 'defeat') return 'loss';
  if (normalized === 'draw' || normalized === 'tie') return 'draw';
  return 'complete';
};

const displayOutcome = (outcome: string): string => {
  const normalized = normalizeOutcome(outcome);
  if (normalized === 'win') return 'Victory';
  if (normalized === 'loss') return 'Defeat';
  if (normalized === 'draw') return 'Draw';
  return outcome.trim() || 'Complete';
};

const HistoryPlayerRow: React.FC<{
  player: MatchHistoryPlayer;
  isCurrentUser: boolean;
}> = ({ player, isCurrentUser }) => (
  <div className={`match-history-player${isCurrentUser ? ' is-current-user' : ''}`}>
    <div className="match-history-player-identity">
      <span
        className={`match-history-result is-${normalizeOutcome(player.outcome)}`}
      >
        {displayOutcome(player.outcome)}
      </span>
      <strong>{player.username}</strong>
      {player.teamId !== null && <small>Team {player.teamId}</small>}
    </div>
    <dl className="match-history-metrics">
      <div>
        <dt>Score</dt>
        <dd>{player.score.toLocaleString()}</dd>
      </div>
      {player.teamScore !== null && (
        <div>
          <dt>Team</dt>
          <dd>{player.teamScore.toLocaleString()}</dd>
        </div>
      )}
      <div>
        <dt>XP</dt>
        <dd className="is-positive">{signed(player.xpGained)}</dd>
      </div>
      <div>
        <dt>MMR</dt>
        <dd className={player.mmrDelta === null
          ? undefined
          : player.mmrDelta >= 0 ? 'is-positive' : 'is-negative'}>
          {player.mmrDelta === null ? '—' : signed(player.mmrDelta)}
        </dd>
      </div>
    </dl>
  </div>
);

const HistoryEntry: React.FC<{
  entry: MatchHistorySummary;
  variant: MatchHistoryListProps['variant'];
  currentUserId?: number;
}> = ({ entry, variant, currentUserId }) => {
  const players = variant === 'admin'
    ? entry.players
    : entry.players.filter((player) => player.userId === currentUserId);

  return (
    <article className="match-history-entry">
      <header className="match-history-entry-header">
        <div>
          <h3>{entry.modeLabel || entry.mode}</h3>
          <p>
            <time dateTime={new Date(entry.endedAtMs).toISOString()}>
              {formatDateTime(entry.endedAtMs)}
            </time>
            <span aria-hidden="true"> · </span>
            <span>{formatDuration(entry.durationMs)}</span>
          </p>
        </div>
        <div className="match-history-entry-tags" aria-label="Match details">
          <span>{entry.queueMode}</span>
          {entry.isPrivate && <span>Private</span>}
          {entry.completedByInactivity && <span>Inactive finish</span>}
          {variant === 'admin' && entry.isStressTest && <span>Stress test</span>}
          {variant === 'admin' && <span>#{entry.gameId}</span>}
        </div>
      </header>
      <div className="match-history-players">
        {players.map((player) => (
          <HistoryPlayerRow
            key={player.userId}
            player={player}
            isCurrentUser={player.userId === currentUserId}
          />
        ))}
      </div>
    </article>
  );
};

export const MatchHistoryList: React.FC<MatchHistoryListProps> = ({
  variant,
  loadPage,
  currentUserId,
  resetKey = '',
  emptyMessage = 'No completed matches yet.',
}) => {
  const [entries, setEntries] = useState<MatchHistorySummary[]>([]);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const requestSequence = useRef(0);

  const fetchPage = async (cursor: string | null, append: boolean) => {
    const sequence = ++requestSequence.current;
    append ? setLoadingMore(true) : setLoading(true);
    setError(null);
    try {
      const page = await loadPage(cursor);
      if (sequence !== requestSequence.current) return;
      setEntries((previous) => append ? [...previous, ...page.entries] : page.entries);
      setNextCursor(page.nextCursor);
    } catch (nextError) {
      if (sequence !== requestSequence.current) return;
      setError(nextError instanceof Error ? nextError.message : 'Match history could not be loaded.');
    } finally {
      if (sequence === requestSequence.current) {
        setLoading(false);
        setLoadingMore(false);
      }
    }
  };

  useEffect(() => {
    setEntries([]);
    setNextCursor(null);
    void fetchPage(null, false);
    return () => {
      requestSequence.current += 1;
    };
    // loadPage is expected to be memoized by the caller; resetKey handles
    // filter changes without remounting the entire admin section.
  }, [loadPage, resetKey]);

  const status = useMemo(() => {
    if (loading) {
      return (
        <div className="match-history-status" role="status">
          <span className="match-history-spinner" aria-hidden="true" />
          Loading match history…
        </div>
      );
    }
    if (error && entries.length === 0) {
      return (
        <div className="match-history-status is-error" role="alert">
          <strong>History unavailable</strong>
          <span>{error}</span>
          <button type="button" onClick={() => void fetchPage(null, false)}>Try again</button>
        </div>
      );
    }
    if (entries.length === 0) {
      return (
        <div className="match-history-status is-empty">
          <HistoryIcon className="account-history-icon" />
          <strong>{emptyMessage}</strong>
          <span>Your finished games will appear here.</span>
        </div>
      );
    }
    return null;
  }, [emptyMessage, entries.length, error, loading]);

  return (
    <section
      className={`match-history-list is-${variant}`}
      aria-label="Match history"
      aria-busy={loading || loadingMore}
    >
      {status ?? entries.map((entry) => (
        <HistoryEntry
          key={entry.gameId}
          entry={entry}
          variant={variant}
          currentUserId={currentUserId}
        />
      ))}

      {!loading && entries.length > 0 && (
        <footer className="match-history-pagination">
          {error && <span role="alert">{error}</span>}
          {nextCursor ? (
            <button
              type="button"
              disabled={loadingMore}
              onClick={() => void fetchPage(nextCursor, true)}
            >
              {loadingMore ? 'Loading…' : 'Load more matches'}
            </button>
          ) : (
            <span>End of history</span>
          )}
        </footer>
      )}
    </section>
  );
};
