import React, { useState } from 'react';

type GameMode = 'duel' | '2v2' | 'solo' | 'ffa';

interface GameStartFormProps {
  onStartGame: (gameMode: GameMode, nickname: string, isCompetitive: boolean) => void;
  currentUsername?: string;
  isLoading?: boolean;
}

export const GameStartForm: React.FC<GameStartFormProps> = ({
  onStartGame,
  currentUsername,
  isLoading = false
}) => {
  const [nickname, setNickname] = useState(currentUsername || '');
  const [selectedMode, setSelectedMode] = useState<GameMode | null>(null);
  const [isCompetitive, setIsCompetitive] = useState(false);

  const gameModes: Array<{ id: GameMode; label: string }> = [
    { id: 'duel', label: 'DUEL' },
    { id: '2v2', label: '2V2' },
    { id: 'solo', label: 'SOLO' },
    { id: 'ffa', label: 'FFA' }
  ];

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (selectedMode && nickname.trim().length >= 3) {
      onStartGame(selectedMode, nickname.trim(), isCompetitive);
    }
  };

  const isFormValid = selectedMode !== null && nickname.trim().length >= 3;

  return (
    <form onSubmit={handleSubmit} className="w-full max-w-md mx-auto">
      {/* Logo */}
      <div className="flex justify-center mb-8">
        <img src="/SnaketronLogo.png" alt="Snaketron" className="h-8 w-auto opacity-80" />
      </div>

      <div className="p-8">
        {/* Nickname Input */}
        <div className="mb-8">
          <input
            type="text"
            value={nickname}
            onChange={(e) => setNickname(e.target.value)}
            placeholder="Nickname"
            className="w-full px-4 py-3 text-base border-2 border-gray-300 rounded-lg focus:outline-none focus:border-blue-500 transition-colors"
            disabled={isLoading}
            minLength={3}
            required
          />
          {nickname.length > 0 && nickname.length < 3 && (
            <p className="text-xs text-red-600 mt-1">Nickname must be at least 3 characters</p>
          )}
        </div>

        {/* Game Mode Selector */}
        <div className="mb-8">
          <div className="grid grid-cols-2 gap-3">
            {gameModes.map((mode) => (
              <button
                key={mode.id}
                type="button"
                onClick={() => setSelectedMode(mode.id)}
                disabled={isLoading}
                className={`
                  relative py-4 px-4 rounded-lg font-black italic uppercase tracking-1 text-base
                  transition-all border-2
                  ${selectedMode === mode.id
                    ? 'border-blue-500 bg-blue-50 text-black-70'
                    : 'border-gray-300 bg-white text-black-70 hover:border-gray-400'
                  }
                  ${isLoading ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}
                `}
              >
                {/* Checkbox indicator */}
                {selectedMode === mode.id && (
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
            ))}
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
          {isLoading ? 'Starting...' : 'Start Game'}
        </button>
      </div>
    </form>
  );
};
