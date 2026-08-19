import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import { api } from '../services/api';
import { homeNoticeState } from '../utils/homeNotice';
import type { PlayerLobbyResponse } from '../types/generated';
import { User } from '../types';

const generateGuestNickname = (username: string) => {
  const nameSegment = username.slice(0, 4).replace(/[^A-Za-z0-9]/g, '');
  const randomDigits = Math.floor(1000 + Math.random() * 9000);
  return `Guest${nameSegment || 'Player'}${randomDigits}`;
};

/**
 * `snaketron.io/play/<username>` — join whatever lobby that player is in.
 *
 * Deliberately outside `ProtectedRoute`, like the lobby-code invite page: the
 * whole point of a shareable link is that it works for someone who has never
 * played, so this mints a guest session itself rather than bouncing an
 * anonymous visitor home.
 *
 * When the target is not reachable this redirects home with an explanation
 * rather than parking the visitor on a dead-end error page — there is nothing
 * to retry, because the outcome depends on the other player coming online.
 */
interface PlayerInvitePageProps {
  /**
   * The name from the link. Passed in rather than read from `useParams`,
   * because the route this renders under is declared as `/play/:gameId` —
   * reading a `:username` param here would silently always be undefined.
   */
  username: string;
}

const PlayerInvitePage: React.FC<PlayerInvitePageProps> = ({ username: rawUsername }) => {
  const username = (rawUsername ?? '').trim();
  const navigate = useNavigate();
  const { user, ensurePlayableSession, loading: authLoading, getToken } = useAuth();
  const {
    isConnected,
    joinLobby,
    leaveLobby,
    currentLobby,
    lobbyRestorationComplete,
    waitForSessionReady,
  } = useWebSocket();

  const inFlightRef = useRef(false);
  const hasSucceededRef = useRef(false);
  const latestUserRef = useRef<User | null>(user);
  const currentLobbyRef = useRef(currentLobby);
  currentLobbyRef.current = currentLobby;

  const [target, setTarget] = useState<PlayerLobbyResponse | null>(null);
  const [statusMessage, setStatusMessage] = useState('Looking up player…');
  const [error, setError] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    latestUserRef.current = user;
  }, [user]);

  const ensureAuthenticatedSession = useCallback(async () => {
    setStatusMessage(
      latestUserRef.current ? 'Verifying player session…' : 'Creating player profile…',
    );
    const { user: activeUser, token: resolvedToken } = await ensurePlayableSession(
      generateGuestNickname(username),
    );
    latestUserRef.current = activeUser;

    setStatusMessage('Authenticating session…');
    const token = resolvedToken ?? getToken();
    if (!token) {
      throw new Error('Missing authentication token');
    }

    await waitForSessionReady();
  }, [ensurePlayableSession, getToken, username, waitForSessionReady]);

  /**
   * Phase one: work out whether there is anything to join.
   *
   * Plain HTTP, so it deliberately does not wait on the game socket, the
   * auth session, or lobby restoration. An unknown or offline target is a
   * dead end whose answer does not depend on any of them, and making the
   * visitor watch a spinner reconnect before being told "they're offline"
   * would be a worse answer arriving later.
   */
  useEffect(() => {
    if (hasSucceededRef.current || target) {
      return;
    }

    if (!username) {
      setError('This invite link is missing a player name.');
      setStatusMessage('Unable to join.');
      return;
    }

    let cancelled = false;
    setStatusMessage('Looking up player…');

    api.getPlayerLobby(username).then((resolved) => {
      if (cancelled || hasSucceededRef.current) {
        return;
      }

      if (resolved.status !== 'online' || !resolved.lobbyCode) {
        const message = resolved.status === 'notFound'
          ? `We couldn't find a player called ${username}.`
          : `${resolved.username} is not online right now.`;
        hasSucceededRef.current = true;
        navigate('/', homeNoticeState(message, 'info'));
        return;
      }

      setTarget(resolved);
    });

    return () => {
      cancelled = true;
    };
  }, [attempt, username, target, navigate]);

  /**
   * Phase two: actually join. This is the part that needs a live session.
   */
  useEffect(() => {
    if (hasSucceededRef.current || !target?.lobbyCode) {
      return;
    }

    if (authLoading) {
      setStatusMessage('Checking your account…');
      return;
    }

    if (!isConnected) {
      setStatusMessage('Connecting to game server…');
      return;
    }

    // A returning player is auto-rejoined into their previous lobby on
    // connect, and the server refuses JoinLobby while one is held. Wait for
    // that restore to settle so the leave below sees the real lobby rather
    // than racing it.
    if (!lobbyRestorationComplete) {
      setStatusMessage('Restoring your session…');
      return;
    }

    if (inFlightRef.current) {
      return;
    }

    const lobbyCode = target.lobbyCode;
    let cancelled = false;

    const attemptJoin = async () => {
      inFlightRef.current = true;
      setError(null);

      try {
        await ensureAuthenticatedSession();

        if (cancelled) {
          return;
        }

        // Following an invite means switching lobbies, but the server admits
        // only one at a time and rejects a join while another is held. Give
        // up the current one first — unless it is already the target, which
        // happens when a player follows their own link or one they have
        // already accepted.
        const held = currentLobbyRef.current;
        if (held) {
          if (held.code.toUpperCase() === lobbyCode.toUpperCase()) {
            hasSucceededRef.current = true;
            navigate('/', homeNoticeState(
              `You are already in ${target.username}'s lobby.`,
            ));
            return;
          }

          setStatusMessage('Leaving your current lobby…');
          await leaveLobby();

          if (cancelled) {
            return;
          }
        }

        setStatusMessage(`Joining ${target.username}'s lobby…`);
        await joinLobby(lobbyCode);

        if (cancelled) {
          return;
        }

        hasSucceededRef.current = true;
        setStatusMessage('Joined lobby! Redirecting…');
        setTimeout(() => {
          if (!cancelled) {
            navigate('/', { replace: true });
          }
        }, 900);
      } catch (err: unknown) {
        if (cancelled) {
          return;
        }

        console.error('Failed to join player lobby:', err);
        let message = 'Failed to join. Please check that the invite is still valid.';

        if (err instanceof Error && err.message) {
          if (err.message.includes('Access denied')) {
            message = 'You do not have permission to join this lobby.';
          } else if (err.message.includes('Timeout waiting to join lobby')) {
            message = 'Joining is taking longer than expected. Please retry in a moment.';
          } else {
            message = err.message;
          }
        }

        setError(message);
        setStatusMessage('Unable to join.');
      } finally {
        if (!cancelled) {
          inFlightRef.current = false;
        }
      }
    };

    attemptJoin();

    return () => {
      cancelled = true;
    };
  }, [
    attempt,
    target,
    authLoading,
    isConnected,
    lobbyRestorationComplete,
    ensureAuthenticatedSession,
    joinLobby,
    leaveLobby,
    navigate,
  ]);

  const handleRetry = () => {
    if (!username) {
      return;
    }
    setError(null);
    // Re-resolve as well as re-join: the lobby may be gone by now, or the
    // player may have come online since the first attempt.
    setTarget(null);
    setStatusMessage('Retrying…');
    setAttempt((prev) => prev + 1);
  };

  const handleGoHome = () => {
    navigate('/');
  };

  return (
    <div className="min-h-screen flex items-center justify-center px-6">
      <div className="max-w-md w-full text-center space-y-6">
        <img src="/SnaketronLogo.png" alt="Snaketron" className="h-10 mx-auto opacity-80" />
        <div className="space-y-1">
          <h1 className="text-2xl font-black italic uppercase tracking-1 text-black-70">
            Joining Game
          </h1>
          <p className="text-sm text-black-70 opacity-70" data-testid="player-invite-status">
            {statusMessage}
          </p>
          <p className="text-xs uppercase tracking-1 text-black-40">
            Player: {username || 'UNKNOWN'}
          </p>
        </div>

        {error ? (
          <div className="space-y-4">
            <div className="px-4 py-3 bg-red-50 border border-red-200 rounded text-sm text-red-700">
              {error}
            </div>
            <div className="flex gap-3">
              <button
                onClick={handleRetry}
                className="flex-1 px-5 py-3 border-2 border-black-70 rounded-lg font-black italic uppercase tracking-1 text-black-70 hover:bg-gray-50 transition-colors"
              >
                Retry
              </button>
              <button
                onClick={handleGoHome}
                className="flex-1 px-5 py-3 border-2 border-transparent rounded-lg font-black italic uppercase tracking-1 text-white bg-black-70 hover:opacity-80 transition-opacity"
              >
                Home
              </button>
            </div>
          </div>
        ) : (
          <div className="flex flex-col items-center gap-4">
            <span className="inline-block w-10 h-10 border-4 border-gray-300 border-t-black-70 rounded-full animate-spin" />
            <p className="text-xs text-black-40 uppercase tracking-1">
              Hang tight, we&apos;ll take you to the lobby in a moment.
            </p>
          </div>
        )}
      </div>
    </div>
  );
};

export default PlayerInvitePage;
