import React, { useEffect, useId, useRef, useState } from 'react';
import { useAuth } from '../contexts/AuthContext';
import { useDebouncedValue } from '../hooks/useDebouncedValue';
import { api, isApiError } from '../services/api';
import { AuthModalProps, FormEventHandler } from '../types';
import { LobbyModal } from './LobbyModal';

type AuthOperation = 'login' | 'register';
type AuthErrorTarget = 'username' | 'password' | 'form' | null;
type UsernameLookup =
  | { kind: 'idle' }
  | { kind: 'checking'; value: string }
  | { kind: 'login'; value: string }
  | { kind: 'register'; value: string }
  | { kind: 'invalid'; value: string; message: string }
  | { kind: 'failed'; value: string };

const usernameValidationError = (value: string): string | null => {
  if (!value) {
    return 'Enter your username.';
  }
  if (value.length < 3) {
    return 'Username must be at least 3 characters.';
  }
  if (value.length > 20) {
    return 'Username must be 20 characters or fewer.';
  }
  if (!/^[\p{L}\p{N}_-]+$/u.test(value)) {
    return 'Use only letters, numbers, underscores, or hyphens.';
  }
  if (/^[_-]|[_-]$/.test(value)) {
    return 'Username cannot begin or end with an underscore or hyphen.';
  }
  return null;
};

const withTerminalPunctuation = (message: string): string =>
  /[.!?]$/.test(message) ? message : `${message}.`;

const checkUsername = async (value: string): Promise<UsernameLookup> => {
  const validationError = usernameValidationError(value);
  if (validationError) {
    return { kind: 'invalid', value, message: validationError };
  }

  const result = await api.checkUsername(value);
  if (result.available) {
    return { kind: 'register', value };
  }

  const existingAccount = result.errors.some((message) =>
    message.toLowerCase().includes('already taken'),
  );
  if (existingAccount) {
    return { kind: 'login', value };
  }

  if (result.errors.length > 0) {
    return {
      kind: 'invalid',
      value,
      message: withTerminalPunctuation(result.errors[0]),
    };
  }

  return { kind: 'failed', value };
};

const getInitialUsername = (guestUsername?: string): string => {
  if (guestUsername) {
    return guestUsername;
  }

  try {
    return window.localStorage.getItem('savedUsername')?.trim() ?? '';
  } catch {
    return '';
  }
};

const authenticationErrorMessage = (
  error: unknown,
  operation: AuthOperation,
): string => {
  if (isApiError(error)) {
    if (error.response.status === 401) {
      return 'That username and password do not match.';
    }
    if (error.response.status === 409) {
      return 'That username is already in use. Try signing in instead.';
    }
    if (error.message && error.message !== 'Request failed' && error.message !== 'Internal server error') {
      return error.message;
    }
  }

  if (error instanceof TypeError) {
    return 'Could not reach the server. Check your connection and try again.';
  }

  return operation === 'login'
    ? 'Could not sign in. Try again in a moment.'
    : 'Could not create the account. Try again in a moment.';
};

const lookupOperation = (lookup: UsernameLookup, value: string): AuthOperation | null => {
  if (lookup.kind === 'login' && lookup.value === value) {
    return 'login';
  }
  if (lookup.kind === 'register' && lookup.value === value) {
    return 'register';
  }
  return null;
};

const AuthModal: React.FC<AuthModalProps> = ({ isOpen, onClose }) => {
  const { user, login, register } = useAuth();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [lookup, setLookup] = useState<UsernameLookup>({ kind: 'idle' });
  const [error, setError] = useState<string | null>(null);
  const [errorTarget, setErrorTarget] = useState<AuthErrorTarget>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submittingOperation, setSubmittingOperation] = useState<AuthOperation | null>(null);
  const debouncedUsername = useDebouncedValue(username.trim(), 400);
  const usernameRef = useRef<HTMLInputElement>(null);
  const lookupRequestRef = useRef(0);
  const usernameId = useId();
  const passwordId = useId();
  const statusId = useId();
  const errorId = useId();

  useEffect(() => {
    if (!isOpen) {
      lookupRequestRef.current += 1;
      return;
    }

    const initialUsername = getInitialUsername(user?.isGuest ? user.username : undefined);
    setUsername(initialUsername);
    setPassword('');
    setLookup(initialUsername.length >= 3
      ? { kind: 'checking', value: initialUsername }
      : { kind: 'idle' });
    setError(null);
    setErrorTarget(null);
    setIsSubmitting(false);
    setSubmittingOperation(null);
  }, [isOpen]);

  useEffect(() => {
    if (!isOpen) {
      return;
    }

    const normalizedUsername = username.trim();
    const requestId = lookupRequestRef.current + 1;
    lookupRequestRef.current = requestId;

    if (debouncedUsername !== normalizedUsername) {
      setLookup(normalizedUsername.length >= 3
        ? { kind: 'checking', value: normalizedUsername }
        : { kind: 'idle' });
      return;
    }
    if (!normalizedUsername) {
      setLookup({ kind: 'idle' });
      return;
    }
    if (normalizedUsername.length < 3) {
      setLookup({ kind: 'idle' });
      return;
    }

    setLookup({ kind: 'checking', value: normalizedUsername });
    void checkUsername(normalizedUsername).then((result) => {
      if (lookupRequestRef.current === requestId) {
        setLookup(result);
      }
    });
  }, [debouncedUsername, isOpen, username]);

  const handleDismiss = () => {
    if (!isSubmitting) {
      onClose();
    }
  };

  const clearError = () => {
    if (error) {
      setError(null);
      setErrorTarget(null);
    }
  };

  const handleUsernameChange: React.ChangeEventHandler<HTMLInputElement> = (event) => {
    const nextUsername = event.target.value;
    const normalizedUsername = nextUsername.trim();
    lookupRequestRef.current += 1;
    setUsername(nextUsername);
    setLookup(normalizedUsername.length >= 3
      ? { kind: 'checking', value: normalizedUsername }
      : { kind: 'idle' });
    clearError();
  };

  const handleSubmit: FormEventHandler = async (event) => {
    event.preventDefault();
    const normalizedUsername = username.trim();
    const usernameError = usernameValidationError(normalizedUsername);

    if (usernameError) {
      setError(usernameError);
      setErrorTarget('username');
      return;
    }
    if (!password) {
      setError('Enter your password.');
      setErrorTarget('password');
      return;
    }

    setIsSubmitting(true);
    setError(null);
    setErrorTarget(null);
    let activeOperation: AuthOperation | null = null;

    try {
      let operation = lookupOperation(lookup, normalizedUsername);
      if (!operation) {
        setLookup({ kind: 'checking', value: normalizedUsername });
        const resolvedLookup = await checkUsername(normalizedUsername);
        setLookup(resolvedLookup);
        operation = lookupOperation(resolvedLookup, normalizedUsername);

        if (!operation) {
          if (resolvedLookup.kind === 'invalid') {
            setError(resolvedLookup.message);
            setErrorTarget('username');
          } else {
            setError('Could not check that username. Try again in a moment.');
            setErrorTarget('form');
          }
          return;
        }
      }

      if (operation === 'register' && password.length < 6) {
        setError('Use at least 6 characters to create an account.');
        setErrorTarget('password');
        return;
      }

      activeOperation = operation;
      setSubmittingOperation(operation);
      if (operation === 'login') {
        await login(normalizedUsername, password);
      } else {
        await register(normalizedUsername, password);
      }

      try {
        window.localStorage.setItem('savedUsername', normalizedUsername);
      } catch {
        // Authentication still succeeds when storage is unavailable.
      }

      onClose();
      setPassword('');
      setError(null);
      setErrorTarget(null);
    } catch (authenticationError: unknown) {
      const operation = activeOperation ?? lookupOperation(lookup, normalizedUsername) ?? 'login';
      setError(authenticationErrorMessage(authenticationError, operation));
      setErrorTarget('form');
    } finally {
      setIsSubmitting(false);
      setSubmittingOperation(null);
    }
  };

  const operation = lookupOperation(lookup, username.trim());
  const isCheckingUsername = lookup.kind === 'checking';
  const canRetryLookup = lookup.kind === 'failed';
  const submitLabel = isSubmitting
    ? submittingOperation === 'register'
      ? 'Creating account…'
      : submittingOperation === 'login'
        ? 'Signing in…'
        : 'Checking…'
    : operation === 'register'
      ? 'Create account'
      : operation === 'login'
        ? 'Sign in'
        : isCheckingUsername
          ? 'Checking…'
          : canRetryLookup
            ? 'Try again'
            : 'Continue';

  const status = (() => {
    if (lookup.kind === 'checking') {
      return { text: 'Checking username…', isError: false };
    }
    if (lookup.kind === 'register') {
      return { text: 'New username — enter a password to create your account.', isError: false };
    }
    if (lookup.kind === 'login') {
      return { text: 'Account found — enter your password to sign in.', isError: false };
    }
    if (lookup.kind === 'invalid') {
      return { text: lookup.message, isError: true };
    }
    if (lookup.kind === 'failed') {
      return { text: 'Could not check that username. Try again.', isError: true };
    }
    return { text: 'Usernames are 3–20 characters.', isError: false };
  })();

  const isSubmitDisabled = isSubmitting
    || !username.trim()
    || !password
    || isCheckingUsername
    || lookup.kind === 'idle'
    || lookup.kind === 'invalid';
  const isGuest = Boolean(user?.isGuest);
  const modalDescription = isGuest && user ? (
    <span className="auth-modal-guest-description">
      <span className="auth-modal-guest-identity">
        You’re playing as <strong>{user.username}</strong>{' '}
        <span className="auth-modal-guest-suffix">(guest)</span>.
      </span>{' '}
      <span className="auth-modal-guest-guidance">
        Sign in to an existing account, or create one to keep your name and progress.
      </span>
    </span>
  ) : (
    'Enter your username to sign in or create an account.'
  );

  return (
    <LobbyModal
      isOpen={isOpen}
      onClose={handleDismiss}
      title="Sign in or create account"
      description={modalDescription}
      initialFocusRef={usernameRef}
      isDismissDisabled={isSubmitting}
    >
      <form
        className="lobby-modal-form auth-modal-form"
        onSubmit={handleSubmit}
        noValidate
        aria-busy={isSubmitting}
      >
        <div className="auth-modal-fields">
          <div className="auth-modal-field">
            <label className="visually-hidden" htmlFor={usernameId}>Username</label>
            <input
              ref={usernameRef}
              id={usernameId}
              type="text"
              placeholder="Username"
              value={username}
              onChange={handleUsernameChange}
              className="auth-modal-input"
              autoComplete="username"
              autoCapitalize="none"
              spellCheck={false}
              maxLength={20}
              disabled={isSubmitting}
              aria-invalid={errorTarget === 'username' || lookup.kind === 'invalid'}
              aria-describedby={error ? errorId : statusId}
            />
          </div>

          <div className="auth-modal-field">
            <label className="visually-hidden" htmlFor={passwordId}>Password</label>
            <input
              id={passwordId}
              type="password"
              placeholder={operation === 'register' ? 'Password (6+ characters)' : 'Password'}
              value={password}
              onChange={(event) => {
                setPassword(event.target.value);
                clearError();
              }}
              className="auth-modal-input"
              autoComplete={operation === 'register' ? 'new-password' : 'current-password'}
              disabled={isSubmitting}
              aria-invalid={errorTarget === 'password'}
              aria-describedby={error ? errorId : undefined}
            />
          </div>

          {error ? (
            <p id={errorId} className="auth-modal-status is-error" role="alert">
              {error}
            </p>
          ) : (
            <p
              id={statusId}
              className={`auth-modal-status ${status.isError ? 'is-error' : ''}`}
              aria-live="polite"
            >
              {status.text}
            </p>
          )}
        </div>

        <div className="lobby-modal-actions">
          <button
            type="button"
            className="lobby-modal-button is-secondary"
            onClick={handleDismiss}
            disabled={isSubmitting}
          >
            Cancel
          </button>
          <button
            type="submit"
            className="lobby-modal-button is-primary"
            disabled={isSubmitDisabled}
          >
            {isSubmitting && <span className="lobby-modal-spinner" aria-hidden="true" />}
            <span>{submitLabel}</span>
          </button>
        </div>
      </form>
    </LobbyModal>
  );
};

export default AuthModal;
