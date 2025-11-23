import React, { useState, useRef, useEffect } from 'react';
import { useDebouncedValue } from '../hooks/useDebouncedValue';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import { LobbyPreferences, LobbyGameMode } from '../types';
import {
  DEFAULT_LOBBY_PREFERENCES,
  loadStoredLobbyPreferences,
  persistStoredLobbyPreferences,
} from '../utils/lobbyPreferencesStorage';

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
  lobbyPreferences: LobbyPreferences | null;
  onPreferencesChange?: (preferences: LobbyPreferences) => void;
}

export const GameStartForm: React.FC<GameStartFormProps> = ({
  onStartGame,
  currentUsername,
  isLoading = false,
  isAuthenticated = false,
  isLobbyQueued = false,
  lobbyPreferences,
  onPreferencesChange,
}) => {
  const [nickname, setNickname] = useState(currentUsername || '');
  const [hasAutoSetNickname, setHasAutoSetNickname] = useState(false);
  const [selectedModes, setSelectedModes] = useState<Set<LobbyGameMode> | null>(null);
  const [isCompetitive, setIsCompetitive] = useState<boolean | null>(null);
  const nicknameInputRef = useRef<HTMLInputElement>(null);
  const lastSubmittedNicknameRef = useRef<string | null>(null);
  const { user } = useAuth();
  const { sendMessage } = useWebSocket();
  const prevUsernameRef = useRef<string | null>(null);
  const canEdit = !isLobbyQueued;

  // Debounce nickname validation to avoid showing errors while typing
  const debouncedNickname = useDebouncedValue(nickname, 500);
  const showNicknameError = debouncedNickname.length > 0 && debouncedNickname.length < 3;

  // Auto-focus on nickname field when component mounts
  useEffect(() => {
    nicknameInputRef.current?.focus();
  }, []);

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
      onPreferencesChange?.(fallbackPreferences);
    } else if (isCompetitive === null) {
      setIsCompetitive(DEFAULT_LOBBY_PREFERENCES.competitive);
    }
  }, [
    lobbyPreferences,
    selectedModes,
    isCompetitive,
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
    if (!currentUsername) {
      return;
    }

    if (!hasAutoSetNickname) {
      setHasAutoSetNickname(true);
    }

    if (prevUsernameRef.current !== currentUsername) {
      setNickname(currentUsername);
      lastSubmittedNicknameRef.current = currentUsername;
      prevUsernameRef.current = currentUsername;
    }
  }, [currentUsername, hasAutoSetNickname]);

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

    sendMessage({ UpdateNickname: { nickname: nextNickname } });
    lastSubmittedNicknameRef.current = nextNickname;
  }, [debouncedNickname, user, sendMessage]);

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
    if (isLobbyQueued) {
      return;
    }

    if (selectedModes && selectedModes.size > 0 && nickname.trim().length >= 3) {
      onStartGame(Array.from(selectedModes), nickname.trim(), isCompetitive || false);
    }
  };

  const isFormValid = selectedModes && selectedModes.size > 0 && nickname.trim().length >= 3;
  const startButtonDisabled = isLobbyQueued || !isFormValid || isLoading;
  const startButtonLabel = isLobbyQueued
    ? 'Finding Match...'
    : isLoading
        ? 'Starting...'
        : 'Start Game';

  return (
    <form onSubmit={handleSubmit} className="w-full max-w-md mx-auto">
      {/* Logo */}
      <div className="flex justify-center mb-8">
        <img src="/SnaketronLogo.png" alt="Snaketron" className="h-8 w-auto opacity-80" />
      </div>

      <div className="p-8">
        {/* Nickname Input */}
        <div className="mb-8 relative">
          <input
            ref={nicknameInputRef}
            type="text"
            value={nickname}
            onChange={(e) => setNickname(e.target.value)}
            placeholder="Nickname"
            className={`w-full px-4 py-3 text-base border-2 rounded-lg transition-colors ${
              isAuthenticated
                ? 'border-gray-300 bg-white cursor-default'
                : 'border-gray-300 focus:outline-none focus:border-blue-500'
            }`}
            disabled={isLoading || isAuthenticated}
            minLength={3}
            required
            readOnly={isAuthenticated}
          />
          {/* Error message with absolute positioning and fade animation */}
          <div className={`
            absolute left-0 right-0 top-[calc(100%+4px)]
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
                  className={`
                    relative py-4 px-4 rounded-lg font-black italic uppercase tracking-1 text-base
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
          className={`
            w-full py-4 rounded-lg font-black italic uppercase tracking-1 text-lg
            transition-all border-2
            ${startButtonDisabled
              ? 'bg-gray-50 border-gray-200 text-gray-400 cursor-not-allowed'
              : 'bg-white border-black-70 text-black-70 hover:bg-gray-50 cursor-pointer'
            }
          `}
        >
          {startButtonLabel}
        </button>
      </div>
    </form>
  );
};
