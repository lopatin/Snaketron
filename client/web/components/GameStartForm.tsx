import React, { useState, useRef, useEffect } from 'react';
import { useDebouncedValue } from '../hooks/useDebouncedValue';

type GameMode = 'duel' | '2v2' | 'solo' | 'ffa';

interface GameStartFormProps {
  onStartGame: (gameModes: GameMode[], nickname: string, isCompetitive: boolean) => void;
  currentUsername?: string;
  isLoading?: boolean;
  isAuthenticated?: boolean;
}

export const GameStartForm: React.FC<GameStartFormProps> = ({
  onStartGame,
  currentUsername,
  isLoading = false,
  isAuthenticated = false
}) => {
  const [nickname, setNickname] = useState(currentUsername || '');
  const [hasAutoSetNickname, setHasAutoSetNickname] = useState(false);
  const [selectedModes, setSelectedModes] = useState<Set<GameMode>>(new Set(['duel']));
  const [isCompetitive, setIsCompetitive] = useState(false);
  const nicknameInputRef = useRef<HTMLInputElement>(null);

  // Debounce nickname validation to avoid showing errors while typing
  const debouncedNickname = useDebouncedValue(nickname, 500);
  const showNicknameError = debouncedNickname.length > 0 && debouncedNickname.length < 3;

  // Auto-focus on nickname field when component mounts
  useEffect(() => {
    nicknameInputRef.current?.focus();
  }, []);

  // Sync nickname with currentUsername when it changes (for guest users)
  useEffect(() => {
    if (currentUsername && !nickname && !hasAutoSetNickname) {
      setHasAutoSetNickname(true);
      setNickname(currentUsername);
    }
  }, [currentUsername, nickname, hasAutoSetNickname]);

  const gameModes: Array<{ id: GameMode; label: string }> = [
    { id: 'duel', label: 'DUEL' },
    { id: '2v2', label: '2V2' },
    { id: 'solo', label: 'SOLO' },
    { id: 'ffa', label: 'FFA' }
  ];

  const toggleMode = (mode: GameMode) => {
    setSelectedModes((prev) => {
      const newSelection = new Set(prev);
      if (newSelection.has(mode)) {
        newSelection.delete(mode);
      } else {
        newSelection.add(mode);
      }
      return newSelection;
    });
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (selectedModes.size > 0 && nickname.trim().length >= 3) {
      onStartGame(Array.from(selectedModes), nickname.trim(), isCompetitive);
    }
  };

  const isFormValid = selectedModes.size > 0 && nickname.trim().length >= 3;

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
              const isSelected = selectedModes.has(mode.id);
              return (
                <button
                  key={mode.id}
                  type="button"
                  onClick={() => toggleMode(mode.id)}
                  disabled={isLoading}
                  className={`
                    relative py-4 px-4 rounded-lg font-black italic uppercase tracking-1 text-base
                    transition-all border-2
                    ${isSelected
                      ? 'border-blue-500 bg-blue-50 text-black-70'
                      : 'border-gray-300 bg-white text-black-70 hover:border-gray-400'
                    }
                    ${isLoading ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}
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
          <label className="flex items-center gap-3 cursor-pointer group">
            <div className="relative">
              <input
                type="checkbox"
                checked={isCompetitive}
                onChange={(e) => setIsCompetitive(e.target.checked)}
                disabled={isLoading}
                className="sr-only"
              />
              <div
                className={`
                  w-6 h-6 border-2 rounded transition-all
                  ${isCompetitive
                    ? 'bg-blue-500 border-blue-500'
                    : 'bg-white border-gray-300'
                  }
                  ${isLoading ? 'opacity-50' : 'group-hover:border-gray-400'}
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
          disabled={!isFormValid || isLoading}
          className={`
            w-full py-4 rounded-lg font-black italic uppercase tracking-1 text-lg
            transition-all border-2
            ${isFormValid && !isLoading
              ? 'bg-white border-black-70 text-black-70 hover:bg-gray-50 cursor-pointer'
              : 'bg-gray-50 border-gray-200 text-gray-400 cursor-not-allowed'
            }
          `}
        >
          {isLoading ? 'Starting...' :  'Start Game'}
        </button>
      </div>
    </form>
  );
};
