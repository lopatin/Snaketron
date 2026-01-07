import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import { User } from '../types';

const generateGuestNickname = (lobbyCode: string) => {
  const codeSegment = lobbyCode.slice(0, 4).replace(/[^A-Z0-9]/g, '');
  const randomDigits = Math.floor(1000 + Math.random() * 9000);
  return `Guest${codeSegment || 'Player'}${randomDigits}`;
};

const LobbyInvitePage: React.FC = () => {
  const { lobbyCode: rawCode } = useParams<{ lobbyCode: string }>();
  const lobbyCode = (rawCode ?? '').toUpperCase();
  const navigate = useNavigate();
  const { user, createGuest, loading: authLoading, getToken } = useAuth();
  const { isConnected, joinLobby, sendMessage } = useWebSocket();

  const inFlightRef = useRef(false);
  const hasSucceededRef = useRef(false);
  const latestUserRef = useRef<User | null>(user);

  const [statusMessage, setStatusMessage] = useState('Preparing to join lobby…');
  const [error, setError] = useState<string | null>(null);
  const [attempt, setAttempt] = useState(0);

  useEffect(() => {
    latestUserRef.current = user;
  }, [user]);

  const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

  const ensureAuthenticatedSession = useCallback(async () => {
    let activeUser = latestUserRef.current;
    let resolvedToken: string | null = getToken();

    if (!activeUser) {
      setStatusMessage('Creating guest profile…');
      const { user: guestUser, token } = await createGuest(generateGuestNickname(lobbyCode));
      latestUserRef.current = guestUser;
      activeUser = guestUser;
      resolvedToken = token;
    }

    setStatusMessage('Authenticating session…');
    const token = resolvedToken ?? getToken();
    if (!token) {
      throw new Error('Missing authentication token');
    }

    sendMessage({ Token: token });
    await delay(50);
  }, [createGuest, getToken, lobbyCode, sendMessage]);

  useEffect(() => {
    if (hasSucceededRef.current) {
      return;
    }

    if (!lobbyCode) {
      setError('Invite link is missing a lobby code.');
      setStatusMessage('Unable to join lobby.');
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

    if (inFlightRef.current) {
      return;
    }

    let cancelled = false;

    const attemptJoin = async () => {
      inFlightRef.current = true;
      setError(null);

      try {
        await ensureAuthenticatedSession();

        if (cancelled) {
          return;
        }

        setStatusMessage('Joining lobby…');
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

        console.error('Failed to join lobby:', err);
        let message = 'Failed to join lobby. Please check that the invite is still valid.';

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
        setStatusMessage('Unable to join lobby.');
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
  }, [attempt, lobbyCode, authLoading, isConnected, ensureAuthenticatedSession, joinLobby, navigate]);

  const handleRetry = () => {
    if (!lobbyCode) {
      return;
    }
    setError(null);
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
          <h1 className="text-2xl font-black italic uppercase tracking-1 text-black-70">Joining Lobby</h1>
          <p className="text-sm text-black-70 opacity-70">{statusMessage}</p>
          <p className="text-xs uppercase tracking-1 text-black-40">Code: {lobbyCode || 'UNKNOWN'}</p>
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

export default LobbyInvitePage;
