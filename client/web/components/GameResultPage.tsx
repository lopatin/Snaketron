import React, { useEffect, useMemo, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import type { PublicGameSummary } from '../types/generated';
import { api } from '../services/api';
import ShareGame from './ShareGame';

/**
 * The public page behind every shared match link.
 *
 * Unauthenticated on purpose, and backed by the canonical match summary rather
 * than the completed-game snapshot: the snapshot ages out under retention,
 * while the summary is permanent, so this page resolves for as long as the
 * product exists. That is the whole point of the route — a link posted
 * anywhere keeps working.
 *
 * The server renders the same path as a crawlable HTML document with Open
 * Graph metadata for machines. This is the version people see when the app is
 * what served the request.
 */

type LoadState =
  | { phase: 'loading' }
  | { phase: 'ready'; summary: PublicGameSummary }
  | { phase: 'pending'; gameId: number }
  | { phase: 'missing' }
  | { phase: 'error' };

/** How often an unfinished match re-checks for its result. */
const PENDING_POLL_MS = 15000;

export function formatMatchDuration(durationMs: number): string {
  const totalSeconds = Math.max(0, Math.floor(durationMs / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return minutes === 0 ? `${seconds}s` : `${minutes}m ${String(seconds).padStart(2, '0')}s`;
}

export function formatMatchDate(endedAtMs: number): string {
  const moment = new Date(endedAtMs);
  if (Number.isNaN(moment.getTime())) {
    return '';
  }
  return moment.toLocaleDateString(undefined, {
    year: 'numeric',
    month: 'long',
    day: 'numeric',
  });
}

/** Winners first, then by score — the order a scoreboard is read in. */
export function rankPlayers(summary: PublicGameSummary): PublicGameSummary['players'] {
  return [...summary.players].sort(
    (left, right) =>
      Number(right.isWinner) - Number(left.isWinner) ||
      right.score - left.score ||
      left.username.localeCompare(right.username),
  );
}

const isValidGameId = (value: string | undefined): value is string =>
  typeof value === 'string' && /^\d+$/.test(value);

export const GameResultPage: React.FC = () => {
  const { gameId } = useParams<{ gameId: string }>();
  const [state, setState] = useState<LoadState>({ phase: 'loading' });

  useEffect(() => {
    if (!isValidGameId(gameId)) {
      setState({ phase: 'missing' });
      return undefined;
    }

    const controller = new AbortController();
    let poll: ReturnType<typeof setTimeout> | null = null;
    setState({ phase: 'loading' });

    const load = () => {
      api
        .getPublicGameSummary(gameId, controller.signal)
        .then((response) => {
          if (response.status === 'final') {
            setState({ phase: 'ready', summary: response.summary });
            return;
          }
          // The match is still being played. Keep checking so the page fills
          // in for whoever is already looking at it.
          setState({ phase: 'pending', gameId: response.game_id });
          poll = setTimeout(load, PENDING_POLL_MS);
        })
        .catch((error: unknown) => {
          if (controller.signal.aborted) {
            return;
          }
          // A 404 is a real answer ("no such match"); anything else is a fault
          // worth offering a retry for.
          const message = error instanceof Error ? error.message : '';
          setState({ phase: /404|not found/i.test(message) ? 'missing' : 'error' });
        });
    };

    load();
    return () => {
      controller.abort();
      if (poll) {
        clearTimeout(poll);
      }
    };
  }, [gameId]);

  const summary = state.phase === 'ready' ? state.summary : null;
  const ranked = useMemo(() => (summary ? rankPlayers(summary) : []), [summary]);

  // Crawlers do not run this, but a person who lands here from a shared link
  // and then bookmarks it should get a title that says what they are looking
  // at rather than the generic app title.
  useEffect(() => {
    if (typeof document === 'undefined') {
      return undefined;
    }
    const previous = document.title;
    if (summary) {
      document.title = `${summary.modeLabel} · Snaketron match #${summary.gameId}`;
    }
    return () => {
      document.title = previous;
    };
  }, [summary]);

  return (
    <main className="game-result-page" data-testid="game-result-page">
      <div className="game-result-card">
        {state.phase === 'loading' && (
          <p className="game-result-status" aria-busy="true">
            Loading match…
          </p>
        )}

        {state.phase === 'pending' && (
          <>
            <p className="game-result-kicker">Snaketron</p>
            <h1 className="game-result-headline">This match is still being played.</h1>
            <p className="game-result-meta">
              Match #{state.gameId} · results appear here as soon as it finishes.
            </p>
            <div className="game-result-actions">
              <ShareGame gameId={state.gameId} />
            </div>
          </>
        )}

        {state.phase === 'missing' && (
          <>
            <p className="game-result-kicker">Snaketron</p>
            <h1 className="game-result-headline">This match could not be found.</h1>
            <p className="game-result-meta">
              Match links are permanent, so a missing one usually means the id was mistyped.
            </p>
          </>
        )}

        {state.phase === 'error' && (
          <>
            <p className="game-result-kicker">Snaketron</p>
            <h1 className="game-result-headline">Match results are temporarily unavailable.</h1>
            <p className="game-result-meta">Try again in a moment.</p>
          </>
        )}

        {summary && (
          <>
            <p className="game-result-kicker">
              Snaketron · {summary.modeLabel}
              {summary.queueMode === 'competitive' ? ' · Ranked' : ''}
            </p>
            <h1 className="game-result-headline">{summary.headline}</h1>
            <p className="game-result-meta">
              Match #{summary.gameId} · {formatMatchDuration(summary.durationMs)} ·{' '}
              {formatMatchDate(summary.endedAtMs)}
            </p>

            <ol className="game-result-standings" aria-label="Final standings">
              {ranked.map((player) => (
                <li key={player.userId} data-testid={`game-result-player-${player.userId}`}>
                  <span className="game-result-player">
                    {player.username}
                    {player.isWinner && <span className="game-result-winner">Winner</span>}
                  </span>
                  <strong>{player.score}</strong>
                </li>
              ))}
            </ol>

            <div className="game-result-actions">
              <Link className="game-result-cta" to="/">
                Play Snaketron
              </Link>
              <ShareGame gameId={summary.gameId} headline={summary.headline} />
            </div>
          </>
        )}

        {state.phase !== 'ready' && (
          <div className="game-result-actions">
            <Link className="game-result-cta" to="/">
              Play Snaketron
            </Link>
          </div>
        )}
      </div>
    </main>
  );
};

export default GameResultPage;
