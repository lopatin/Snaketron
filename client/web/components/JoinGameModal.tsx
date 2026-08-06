import React, { useId, useRef, useState } from 'react';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import { FormEventHandler, JoinGameModalProps } from '../types';
import {
  getLobbyCodeValidationError,
  normalizeLobbyCodeInput,
} from '../utils/lobbyCode';
import { LobbyModal } from './LobbyModal';

const generateGuestNickname = (): string =>
  `Guest${Math.floor(1000 + Math.random() * 9000)}`;

const guestNickname = (): string => {
  try {
    const savedNickname = window.localStorage.getItem('savedUsername')?.trim();
    if (savedNickname && savedNickname.length >= 3) {
      return savedNickname;
    }
  } catch {
    // Storage can be unavailable in privacy modes; a generated name is enough.
  }
  return generateGuestNickname();
};

const joinErrorMessage = (error: unknown): string => {
  const message = error instanceof Error
    ? error.message.trim()
    : error && typeof error === 'object' && 'message' in error && typeof error.message === 'string'
      ? error.message.trim()
      : '';
  const normalizedMessage = message.toLowerCase();

  if (normalizedMessage.includes('leave your current lobby')) {
    return 'Leave your current lobby before joining another one.';
  }
  if (normalizedMessage.includes('timeout waiting to join lobby')) {
    return 'Joining took too long. Check the code and try again.';
  }
  if (
    normalizedMessage.includes('not connected') ||
    normalizedMessage.includes('could not connect') ||
    normalizedMessage.includes('connection was lost')
  ) {
    return 'The game server is still connecting. Try again in a moment.';
  }
  if (normalizedMessage.includes('not found') || normalizedMessage.includes('missing')) {
    return 'That lobby could not be found. Check the code and try again.';
  }
  if (message && !normalizedMessage.includes('access denied')) {
    return message;
  }

  return 'Could not join that lobby. Check the code and try again.';
};

function JoinGameModal({ isOpen, onClose }: JoinGameModalProps) {
  const { user, createGuest, loading: isAuthLoading } = useAuth();
  const { isConnected, joinLobby } = useWebSocket();
  const [codeInput, setCodeInput] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [joinStatus, setJoinStatus] = useState<'idle' | 'creating-guest' | 'joining'>('idle');
  const inputRef = useRef<HTMLInputElement>(null);
  const inputId = useId();
  const hintId = useId();
  const errorId = useId();
  const isJoining = joinStatus !== 'idle';

  const handleDismiss = () => {
    if (!isJoining) {
      onClose();
    }
  };

  const handleSubmit: FormEventHandler = async (event) => {
    event.preventDefault();

    const validationError = getLobbyCodeValidationError(codeInput);
    if (validationError) {
      setError(validationError);
      return;
    }

    if (isAuthLoading) {
      setError('Your player session is still loading. Try again in a moment.');
      return;
    }

    if (!isConnected) {
      setError('The game server is still connecting. Try again in a moment.');
      return;
    }

    const lobbyCode = normalizeLobbyCodeInput(codeInput);
    setError(null);

    try {
      if (!user) {
        setJoinStatus('creating-guest');
        await createGuest(guestNickname());
      }

      setJoinStatus('joining');
      await joinLobby(lobbyCode);

      setCodeInput('');
      setError(null);
      setJoinStatus('idle');
      onClose();
    } catch (joinError: unknown) {
      setError(joinErrorMessage(joinError));
      setJoinStatus('idle');
    }
  };

  const submitLabel = isAuthLoading
    ? 'Checking session…'
    : joinStatus === 'creating-guest'
      ? 'Creating guest…'
      : joinStatus === 'joining'
        ? 'Joining lobby…'
        : 'Join lobby';

  return (
    <LobbyModal
      isOpen={isOpen}
      onClose={handleDismiss}
      title="Join game"
      description="Enter a lobby code or paste an invite link."
      initialFocusRef={inputRef}
      isDismissDisabled={isJoining}
    >
      <form
        className="lobby-modal-form"
        onSubmit={handleSubmit}
        noValidate
        aria-busy={isJoining}
      >
        <div className="lobby-modal-field">
          <label className="lobby-modal-label" htmlFor={inputId}>Lobby code</label>
          <div className="lobby-modal-input-wrap">
            <input
              ref={inputRef}
              id={inputId}
              type="text"
              value={codeInput}
              onChange={(event) => {
                setCodeInput(event.target.value);
                if (error) {
                  setError(null);
                }
              }}
              placeholder="USE1-XXXXXXXX"
              className="lobby-modal-input"
              autoComplete="off"
              autoCapitalize="characters"
              spellCheck={false}
              disabled={isJoining}
              aria-invalid={Boolean(error)}
              aria-describedby={error ? `${hintId} ${errorId}` : hintId}
            />
          </div>
          <p id={hintId} className="lobby-modal-hint">
            Example: USE1-4K7MP9QX.
          </p>
        </div>

        {error && (
          <p id={errorId} className="lobby-modal-error" role="alert">
            {error}
          </p>
        )}

        <div className="lobby-modal-actions">
          <button
            type="button"
            className="lobby-modal-button is-secondary"
            onClick={handleDismiss}
            disabled={isJoining}
          >
            Cancel
          </button>
          <button
            type="submit"
            className="lobby-modal-button is-primary"
            disabled={isJoining || isAuthLoading || !codeInput.trim()}
          >
            {isJoining && <span className="lobby-modal-spinner" aria-hidden="true" />}
            <span>{submitLabel}</span>
          </button>
        </div>
      </form>
    </LobbyModal>
  );
}

export default JoinGameModal;
