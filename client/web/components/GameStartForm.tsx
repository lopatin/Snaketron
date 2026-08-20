import React, { useState, useRef, useEffect } from 'react';
import { useDebouncedValue } from '../hooks/useDebouncedValue';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import { PlayersOnline } from './PlayersOnline';
import { NewsTicker } from './NewsTicker';
import { LobbyPreferences, LobbyGameMode } from '../types';
import {
  getTickerPlayPreferences,
  type NewsTickerPlayAction,
} from '../utils/newsTicker';
import {
  DEFAULT_LOBBY_PREFERENCES,
  loadStoredLobbyPreferences,
  persistStoredLobbyPreferences,
} from '../utils/lobbyPreferencesStorage';
import { useCrazyGames } from '../contexts/CrazyGamesContext';
import { useInputSurface } from '../hooks/useInputSurface';

const areModeSetsEqual = (a: Set<LobbyGameMode> | null, b: Set<LobbyGameMode> | null) => {
  if (a === b) {
    return true;
  }

  if (a === null || b === null) {
    return false;
  }

  if (a.size !== b.size) {
    return false;
  }
  for (const mode of a) {
    if (!b.has(mode)) {
      return false;
    }
  }
  return true;
};

interface GameStartFormProps {
  onStartGame: (gameModes: LobbyGameMode[], nickname: string, isCompetitive: boolean) => void;
  currentUsername?: string;
  isLoading?: boolean;
  isAuthenticated?: boolean;
  isLobbyQueued?: boolean;
  /**
   * Whether this player leads the lobby. Only the leader may change the mode
   * selection or start matchmaking, because both decide what every member of
   * the lobby queues for. Defaults to `true` so a form rendered outside a
   * lobby context behaves as it always did.
   */
  isLobbyLeader?: boolean;
  lobbyPreferences: LobbyPreferences | null;
  onPreferencesChange?: (preferences: LobbyPreferences) => void;
  onSignInClick?: () => void;
  errorMessage?: string | null;
  /** Live global player population; `null` while it is still unknown. */
  playersOnline?: number | null;
}

export const GameStartForm: React.FC<GameStartFormProps> = ({
  onStartGame,
  currentUsername,
  isLoading = false,
  isAuthenticated = false,
  isLobbyQueued = false,
  isLobbyLeader = true,
  lobbyPreferences,
  onPreferencesChange,
  onSignInClick,
  errorMessage = null,
  playersOnline = null,
}) => {
  const { isCrazyGamesBuild, userAccountAvailable } = useCrazyGames();
  const effectiveUsername = currentUsername || '';
  const [nickname, setNickname] = useState(effectiveUsername);
  const [hasAutoSetNickname, setHasAutoSetNickname] = useState(false);
  const [selectedModes, setSelectedModes] = useState<Set<LobbyGameMode> | null>(null);
  const [isCompetitive, setIsCompetitive] = useState<boolean | null>(null);
  const nicknameInputRef = useRef<HTMLInputElement>(null);
  const lastSubmittedNicknameRef = useRef<string | null>(null);
  const { user, updateGuestNickname, crazyGamesSessionStatus } = useAuth();
  const locksNickname = isAuthenticated || (
    isCrazyGamesBuild && crazyGamesSessionStatus === 'linked'
  );
  const { sendMessage } = useWebSocket();
  const prevUsernameRef = useRef<string | null>(null);
  const canEdit = !isLobbyQueued && isLobbyLeader;

  // Debounce nickname validation to avoid showing errors while typing
  const debouncedNickname = useDebouncedValue(nickname, 500);
  const showNicknameError = debouncedNickname.length > 0 && debouncedNickname.length < 3;

  // Auto-focus on nickname field when component mounts. Touch surfaces skip
  // it: focusing a text input there pops the software keyboard over the home
  // screen before the player has asked to type anything.
  const inputSurface = useInputSurface();
  useEffect(() => {
    if (!locksNickname && inputSurface !== 'touch') {
      nicknameInputRef.current?.focus();
    }
  }, [locksNickname, inputSurface]);

  // Keep local selection state in sync with lobby-wide preferences
  useEffect(() => {
    if (lobbyPreferences) {
      const nextModes = lobbyPreferences ? new Set<LobbyGameMode>(lobbyPreferences.selectedModes) : null;
      if (!areModeSetsEqual(selectedModes, nextModes)) {
        setSelectedModes(new Set<LobbyGameMode>(lobbyPreferences.selectedModes));
      }
      if (lobbyPreferences.competitive !== isCompetitive) {
        setIsCompetitive(lobbyPreferences.competitive);
      }
    } else if (selectedModes == null) {
      const stored = loadStoredLobbyPreferences();
      const fallbackPreferences: LobbyPreferences = stored ?? {
        selectedModes: [...DEFAULT_LOBBY_PREFERENCES.selectedModes],
        competitive: DEFAULT_LOBBY_PREFERENCES.competitive,
      };
      setSelectedModes(new Set<LobbyGameMode>(fallbackPreferences.selectedModes));
      if (isCompetitive === null) {
        setIsCompetitive(fallbackPreferences.competitive);
      }
      // Seeding from local storage publishes a selection nobody clicked. Only
      // the leader may do that; for anyone else it would be rejected, and it
      // would race the leader's real choice on arrival.
      if (isLobbyLeader) {
        onPreferencesChange?.(fallbackPreferences);
      }
    } else if (isCompetitive === null) {
      setIsCompetitive(DEFAULT_LOBBY_PREFERENCES.competitive);
    }
  }, [
    lobbyPreferences,
    selectedModes,
    isCompetitive,
    isLobbyLeader,
    setSelectedModes,
    setIsCompetitive,
    onPreferencesChange,
  ]);

  useEffect(() => {
    if (!selectedModes) {
      return;
    }

    const preferencesToPersist: LobbyPreferences = {
      selectedModes: Array.from(selectedModes),
      competitive: Boolean(isCompetitive),
    };
    persistStoredLobbyPreferences(preferencesToPersist);
  }, [selectedModes, isCompetitive]);

  // Sync nickname with currentUsername when it changes (for guest users)
  useEffect(() => {
    if (!effectiveUsername) {
      return;
    }

    if (!hasAutoSetNickname) {
      setHasAutoSetNickname(true);
    }

    if (prevUsernameRef.current !== effectiveUsername) {
      setNickname(effectiveUsername);
      lastSubmittedNicknameRef.current = effectiveUsername === currentUsername
        ? effectiveUsername
        : null;
      prevUsernameRef.current = effectiveUsername;
    }
  }, [currentUsername, effectiveUsername, hasAutoSetNickname]);

  useEffect(() => {
    if (!user || !user.isGuest) {
      lastSubmittedNicknameRef.current = null;
      return;
    }

    const nextNickname = debouncedNickname.trim();
    if (nextNickname.length < 3) {
      return;
    }

    if (nextNickname === user.username) {
      lastSubmittedNicknameRef.current = nextNickname;
      return;
    }

    if (lastSubmittedNicknameRef.current === nextNickname) {
      return;
    }

    if (!sendMessage({ UpdateNickname: { nickname: nextNickname } })) {
      return;
    }
    updateGuestNickname(nextNickname);
    lastSubmittedNicknameRef.current = nextNickname;
  }, [debouncedNickname, user, sendMessage, updateGuestNickname]);

  const gameModes: Array<{ id: LobbyGameMode; label: string }> = [
    { id: 'duel', label: 'DUEL' },
    { id: '2v2', label: '2V2' },
    { id: 'solo', label: 'SOLO' },
    { id: 'ffa', label: 'FFA' }
  ];

  const toggleMode = (mode: LobbyGameMode) => {
    if (!canEdit) {
      return;
    }

    const nextSelection = new Set(selectedModes ?? []);
    if (nextSelection.has(mode)) {
      nextSelection.delete(mode);
    } else {
      nextSelection.add(mode);
    }

    setSelectedModes(nextSelection);

    onPreferencesChange?.({
      selectedModes: Array.from(nextSelection),
      competitive: isCompetitive || false,
    });
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (isLobbyQueued || !isLobbyLeader) {
      return;
    }

    if (selectedModes && selectedModes.size > 0 && nickname.trim().length >= 3) {
      onStartGame(Array.from(selectedModes), nickname.trim(), isCompetitive || false);
    }
  };

  const isFormValid = selectedModes && selectedModes.size > 0 && nickname.trim().length >= 3;
  const startButtonDisabled = isLobbyQueued || !isFormValid || isLoading || !isLobbyLeader;
  // Unlike the Start button this ignores the nickname: a ticker CTA is still
  // useful without one, because it applies the selection and asks for the name.
  const isTickerPlayDisabled = !canEdit || isLoading;
  const startButtonActivating = isLobbyQueued || isLoading;
  const startButtonLabel = isLobbyQueued
    ? 'Finding Match...'
    : isLoading
        ? 'Starting...'
        : 'Start Game';
  const wasStartButtonDisabledRef = useRef(startButtonDisabled);
  const [enableAnimation, setEnableAnimation] = useState({ key: 0, visible: false });

  useEffect(() => {
    const wasDisabled = wasStartButtonDisabledRef.current;
    wasStartButtonDisabledRef.current = startButtonDisabled;

    if (wasDisabled && !startButtonDisabled) {
      setEnableAnimation(({ key }) => ({ key: key + 1, visible: true }));
    } else if (startButtonDisabled) {
      setEnableAnimation((current) => (
        current.visible ? { ...current, visible: false } : current
      ));
    }
  }, [startButtonDisabled]);

  /**
   * A ticker CTA is a shortcut through the whole form: it replaces the current
   * selection with the competitive queue for that mode and immediately starts
   * matchmaking, rather than leaving the player to press Start themselves.
   *
   * Preferences are published before starting so the lobby and the visible
   * chips agree even if the start attempt fails partway (session creation,
   * socket handshake, and lobby creation all precede the queue command).
   */
  const handleTickerPlay = (action: NewsTickerPlayAction) => {
    if (isTickerPlayDisabled) {
      return;
    }

    const preferences = getTickerPlayPreferences(action);
    setSelectedModes(new Set(preferences.selectedModes));
    setIsCompetitive(preferences.competitive);
    onPreferencesChange?.(preferences);

    const trimmedNickname = nickname.trim();
    if (trimmedNickname.length < 3) {
      // The shortcut skips the form, not its one requirement. Apply the
      // selection anyway and hand over the field that is still owed, which is
      // exactly the state the Start button's own guard would leave behind.
      nicknameInputRef.current?.focus();
      return;
    }

    onStartGame(
      preferences.selectedModes,
      trimmedNickname,
      preferences.competitive,
    );
  };

  return (
    <form onSubmit={handleSubmit} className="w-full max-w-md mx-auto">
      {/* Logo */}
      <div className="home-brand-lockup">
        <img src="SnaketronLogo.png" alt="Snaketron" className="h-8 w-auto opacity-80" />
        <p className="mt-3 text-xs font-bold italic uppercase tracking-1 text-gray-500">
          Competitive multiplayer Snake
        </p>
        <NewsTicker
          onPlay={handleTickerPlay}
          isPlayDisabled={isTickerPlayDisabled}
        />
      </div>

      <div className="p-8">
        {/* Nickname Input. The live population tag is notched into this field's
            top border, so it renders inside the field's positioned wrapper. */}
        <div className="mb-7 relative">
          <PlayersOnline count={playersOnline} />
          <input
            ref={nicknameInputRef}
            type="text"
            value={nickname}
            onChange={(e) => setNickname(e.target.value)}
            placeholder="Nickname"
            className={`w-full bg-white px-4 py-3 text-base border-2 rounded-lg transition-colors ${
              locksNickname
                ? 'border-gray-300 cursor-default'
                : 'border-gray-300 focus:outline-none focus:border-blue-500'
            }`}
            disabled={isLoading || locksNickname}
            minLength={3}
            required
            readOnly={locksNickname}
          />

          {/* Guest notice + sign-in link. Hidden entirely once the player has
              a real account, since neither half applies to them. */}
          {!locksNickname && (
            <div className="mt-2 px-1 flex flex-wrap items-baseline justify-between gap-x-3 gap-y-1">
              <span className="text-[13px] text-gray-500 whitespace-nowrap">
                {isCrazyGamesBuild ? 'Playing as CrazyGames guest' : 'Playing as guest'}
              </span>
              {(!isCrazyGamesBuild || userAccountAvailable) && (
                <button
                  type="button"
                  onClick={onSignInClick}
                  className="
                    text-[13px] whitespace-nowrap text-blue-600 cursor-pointer
                    hover:underline focus-visible:underline
                  "
                >
                  {isCrazyGamesBuild ? 'Sign in with CrazyGames' : 'Sign in or create account'}
                </button>
              )}
            </div>
          )}

          {locksNickname && isCrazyGamesBuild && crazyGamesSessionStatus === 'linked' && (
            <div className="mt-2 px-1 text-[13px] font-semibold text-green-700">
              CrazyGames account linked · progress saves automatically
            </div>
          )}

          {/* Error message with absolute positioning and fade animation. It is
              anchored to the bottom of the whole block so it clears the guest
              row when that is present, and sits right under the input when it
              is not. */}
          <div className={`
            absolute left-0 right-0 top-[calc(100%+4px)] px-1
            transition-opacity duration-200
            ${showNicknameError ? 'opacity-100' : 'opacity-0 pointer-events-none'}
          `}>
            <p className="text-[11px] text-red-600">
              Nickname must be at least 3 characters
            </p>
          </div>
        </div>

        {/* Game Mode Selector */}
        <div className="mb-8">
          {/* Disabled controls are only fair if the reason is visible. Shown
              in place of the mode grid's own spacing so the layout does not
              shift when leadership changes hands mid-session. */}
          {!isLobbyLeader && (
            <p
              className="mb-3 text-[13px] text-gray-500 text-center"
              data-testid="leader-only-notice"
            >
              Waiting for lobby host to start matchmaking
            </p>
          )}
          <div className="grid grid-cols-2 gap-3">
            {gameModes.map((mode) => {
              const isSelected = selectedModes && selectedModes.has(mode.id);
              // console.log('canEdit:', canEdit, 'isLoading:', isLoading);
              return (
                <button
                  key={mode.id}
                  type="button"
                  onClick={() => toggleMode(mode.id)}
                  disabled={!canEdit || isLoading}
                  aria-pressed={Boolean(isSelected)}
                  className={`
                    game-mode-choice relative py-4 px-4 rounded-lg font-black uppercase tracking-1 text-base
                    transition-all border-2
                    ${isSelected
                      ? 'border-blue-500 bg-blue-50 text-black-70'
                      : 'border-gray-300 bg-white text-black-70 hover:border-gray-400'
                    }
                    ${isLoading || !canEdit ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}
                  `}
                >
                  {/* Checkbox indicator */}
                  {isSelected && (
                    <div className="absolute top-2 right-2">
                      <svg
                        className="w-5 h-5 text-blue-500"
                        fill="none"
                        viewBox="0 0 24 24"
                        stroke="currentColor"
                        strokeWidth={3}
                      >
                        <path
                          strokeLinecap="round"
                          strokeLinejoin="round"
                          d="M5 13l4 4L19 7"
                        />
                      </svg>
                    </div>
                  )}
                  {mode.label}
                </button>
              );
            })}
          </div>
        </div>

        {/* Competitive Checkbox */}
        <div className="mb-8">
          <label
            className={`flex items-center gap-3 ${canEdit ? 'cursor-pointer' : 'cursor-not-allowed'} group`}
          >
            <div className="relative">
              <input
                type="checkbox"
                checked={isCompetitive || false}
                onChange={(e) => {
                  if (!canEdit) {
                    return;
                  }
                  const nextCompetitive = e.target.checked;
                  if (nextCompetitive === isCompetitive) {
                    return;
                  }
                  setIsCompetitive(nextCompetitive);
                  onPreferencesChange?.({
                    selectedModes: selectedModes ? Array.from(selectedModes) : [],
                    competitive: nextCompetitive,
                  });
                }}
                disabled={!canEdit || isLoading}
                className="sr-only"
              />
              <div
                className={`
                  w-6 h-6 border-2 rounded transition-all
                  ${isCompetitive
                    ? 'bg-blue-500 border-blue-500'
                    : 'bg-white border-gray-300'
                  }
                  ${isLoading || !canEdit ? 'opacity-50' : 'group-hover:border-gray-400'}
                `}
              >
                {isCompetitive && (
                  <svg
                    className="w-full h-full text-white"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                    strokeWidth={3}
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      d="M5 13l4 4L19 7"
                    />
                  </svg>
                )}
              </div>
            </div>
            <span className="text-sm font-black uppercase tracking-1 text-black-70 select-none">
              Competitive
            </span>
          </label>
        </div>

        {/* Start Game Button */}
        <button
          type="submit"
          disabled={startButtonDisabled}
          aria-busy={startButtonActivating}
          className={`
            game-start-button game-primary-motion w-full py-4 rounded-lg font-black uppercase tracking-1 text-lg
            inline-flex items-center justify-center
            border-2
            ${startButtonDisabled
              ? startButtonActivating
                ? 'is-activating cursor-wait'
                : 'is-disabled bg-gray-50 border-gray-200 text-gray-400 cursor-not-allowed'
              : 'cursor-pointer'
            }
          `}
        >
          {enableAnimation.visible && (
            <span
              key={enableAnimation.key}
              className="game-start-enable-sweep"
              aria-hidden="true"
            >
              <svg
                className="game-start-enable-chevron is-primary"
                viewBox="0 0 42 34"
              >
                <path d="M0 0h11l17 17-17 17H0l17-17ZM13 0h11l17 17-17 17H13l17-17Z" />
              </svg>
              <svg
                className="game-start-enable-chevron is-echo"
                viewBox="0 0 42 34"
                onAnimationEnd={(event) => {
                  if (event.animationName !== 'game-start-enable-sweep-motion') {
                    return;
                  }
                  setEnableAnimation((current) => (
                    current.key === enableAnimation.key
                      ? { ...current, visible: false }
                      : current
                  ));
                }}
              >
                <path d="M0 0h11l17 17-17 17H0l17-17ZM13 0h11l17 17-17 17H13l17-17Z" />
              </svg>
            </span>
          )}
          <svg
            className="game-start-chevrons is-left"
            viewBox="0 0 31 28"
            aria-hidden="true"
            focusable="false"
          >
            <path d="M0 2h8l12 12L8 26H0l12-12Z" />
          </svg>
          <span className="game-start-content">
            <span className="game-start-label">{startButtonLabel}</span>
          </span>
          <svg
            className="game-start-chevrons is-right"
            viewBox="0 0 31 28"
            aria-hidden="true"
            focusable="false"
          >
            <path d="M10 2h8l12 12-12 12h-8l12-12Z" />
          </svg>
        </button>
        <div className="min-h-5 mt-3" aria-live="polite">
          {errorMessage && (
            <p className="text-sm text-red-600 text-center" role="alert">
              {errorMessage}
            </p>
          )}
        </div>
      </div>
    </form>
  );
};
