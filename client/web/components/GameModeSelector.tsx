import React, { useState, useEffect } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { useGameWebSocket } from '../hooks/useGameWebSocket';
import { api } from '../services/api';
import { useDebouncedValue } from '../hooks/useDebouncedValue';
import { UsernameStatus, GameModeId, GameType } from '../types';

const GAME_MODES = {
  'quick-play': {
    title: 'QUICK MATCH',
    modes: [
      { id: 'duel', name: 'DUEL', description: '1v1 battle', gameType: { TeamMatch: { per_team: 1 } } as GameType },
      { id: 'free-for-all', name: 'FREE FOR ALL', description: 'Up to 8 player brawl', gameType: { FreeForAll: { max_players: 8 } } as GameType }
    ]
  },
  'competitive': {
    title: 'COMPETITIVE',
    modes: [
      { id: 'ranked-duel', name: 'RANKED DUEL', description: 'Competitive 1v1', gameType: { TeamMatch: { per_team: 1 } } as GameType },
      { id: 'ranked-team', name: 'RANKED TEAM', description: 'Team battles', gameType: { TeamMatch: { per_team: 2 } } as GameType }
    ]
  },
  'solo': {
    title: 'SOLO',
    modes: [
      { id: 'solo', name: 'SINGLE PLAYER', description: 'Practice your skills', gameType: 'Solo' as GameType }
    ]
  }
};

function GameModeSelector() {
  const { category } = useParams();
  const navigate = useNavigate();
  const { user, login, register } = useAuth();
  const { isConnected, createGame, currentGameId, customGameCode } = useGameWebSocket();
  
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [authError, setAuthError] = useState<string | null>(null);
  const [isAuthenticating, setIsAuthenticating] = useState(false);
  const [checkingUsername, setCheckingUsername] = useState(false);
  const [usernameStatus, setUsernameStatus] = useState<UsernameStatus>(null);
  const [requiresPassword, setRequiresPassword] = useState(false);
  const [selectedModes, setSelectedModes] = useState<Set<string>>(new Set());

  const debouncedUsername = useDebouncedValue(username, 500);

  const gameModeConfig = category ? GAME_MODES[category as keyof typeof GAME_MODES] : undefined;

  // Redirect if invalid category
  useEffect(() => {
    if (!gameModeConfig) {
      navigate('/');
    }
  }, [category, gameModeConfig, navigate]);

  // Load saved username on mount
  useEffect(() => {
    const savedUsername = localStorage.getItem('savedUsername');
    if (savedUsername) {
      setUsername(savedUsername);
    }
  }, []);

  // Check username availability when it changes
  useEffect(() => {
    if (debouncedUsername && debouncedUsername.length >= 3) {
      setCheckingUsername(true);
      setUsernameStatus(null);
      
      console.log('Checking username:', debouncedUsername);
      
      api.checkUsername(debouncedUsername)
        .then(data => {
          console.log('Username check response:', data);
          setUsernameStatus(data.available ? 'available' : 'exists');
          setRequiresPassword(data.requiresPassword || false);
        })
        .catch(err => {
          console.error('Error checking username:', err);
          // On error, assume username is available (for development)
          setUsernameStatus('available');
          setRequiresPassword(false);
        })
        .finally(() => {
          setCheckingUsername(false);
        });
    } else {
      setUsernameStatus(null);
      setRequiresPassword(false);
    }
  }, [debouncedUsername]);

  const handleGameModeClick = (modeId: string) => {
    // Toggle selection
    setSelectedModes((prev) => {
      const newSelection = new Set(prev);
      if (newSelection.has(modeId)) {
        newSelection.delete(modeId);
      } else {
        newSelection.add(modeId);
      }
      return newSelection;
    });
  };

  const handleStartQueue = async () => {
    if (!username || username.length < 3) {
      setAuthError('Please enter a username (at least 3 characters)');
      return;
    }

    if (!isConnected) {
      setAuthError('Not connected to game server');
      return;
    }

    if (selectedModes.size === 0) {
      setAuthError('Please select at least one game mode');
      return;
    }

    setIsAuthenticating(true);
    setAuthError(null);

    try {
      // First, authenticate the user if not already authenticated
      if (!user) {
        // Check if username exists
        const checkData = await api.checkUsername(username);

        if (!checkData.available && checkData.requiresPassword && !password) {
          setAuthError('This username requires a password');
          setIsAuthenticating(false);
          return;
        }

        // Login or register
        if (!checkData.available) {
          await login(username, password);
        } else {
          await register(username, password || null);
        }

        // Save username for next time
        localStorage.setItem('savedUsername', username);

        // Wait a bit for the JWT token to be sent to WebSocket
        await new Promise(resolve => setTimeout(resolve, 500));
      }

      // Get game types for selected modes
      const gameTypes: GameType[] = [];
      if (gameModeConfig) {
        for (const modeId of selectedModes) {
          const mode = gameModeConfig.modes.find(m => m.id === modeId);
          if (mode) {
            gameTypes.push(mode.gameType);
          }
        }
      }

      // Handle solo mode separately (direct game creation)
      if (selectedModes.has('solo')) {
        createGame('solo');
        console.log('Waiting for SoloGameCreated message...');
      } else if (gameTypes.length > 0) {
        // Navigate to queue screen with multiple game types
        navigate('/queue', {
          state: {
            gameTypes: gameTypes,
            autoQueue: true
          }
        });
      }
    } catch (error) {
      setAuthError((error as Error).message || 'Failed to start queue');
    } finally {
      setIsAuthenticating(false);
    }
  };

  if (!gameModeConfig) return null;

  return (
    <div className="flex-1 p-8">
      <div className="max-w-3xl mx-auto">
        <h1 className="panel-heading mb-6">{gameModeConfig.title}</h1>
        
        <div className="panel p-6">
          {/* Username Input Section */}
          {!user && (
            <div className="mt-1 border-gray-200">
              <input
                type="text"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                placeholder="Username"
                className="w-full px-4 py-3 text-base border-2 border-black-70 rounded"
                disabled={isAuthenticating}
              />
              <div className="auth-message ml-2 h-8 mb-1 mt-1 flex items-center">
                {!username && (
                    <p className="text-sm text-gray-700">Choose a username</p>
                )}
                {username && username.length >= 3 && checkingUsername && (
                  <p className="text-sm text-gray-700">Checking username...</p>
                )}
                {username && username.length >= 3 && !checkingUsername && usernameStatus === 'available' && (
                  <p className="text-sm text-gray-700">
                    {username} is available. Create a new password below.
                  </p>
                )}
                {username && username.length >= 3 && !checkingUsername && usernameStatus === 'exists' && (
                  <p className="text-sm text-gray-700">
                    Enter password for "{username}"
                  </p>
                )}
              </div>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="Password"
                className="w-full px-4 py-3 text-base border-2 border-black-70 rounded"
                disabled={isAuthenticating}
              />
              <div className="auth-message ml-2 h-8 mb-1 mt-1 flex items-center">
                {isAuthenticating && (
                  <p className="text-sm text-gray-700">Logging in...</p>
                )}
                {authError && (
                  <p className="text-red-600 text-sm">{authError}</p>
                )}
              </div>
            </div>
          )}

          {/* Game Modes */}
          <div className="space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {gameModeConfig.modes.map((mode: { id: string; name: string; description: string }) => {
                const isSelected = selectedModes.has(mode.id);
                const isEnabled = user || (username && username.length >= 3);
                return (
                  <button
                    key={mode.id}
                    onClick={() => handleGameModeClick(mode.id)}
                    disabled={isAuthenticating || !isEnabled}
                    className={`p-6 text-left border-2 rounded-lg transition-all relative ${
                      isSelected
                        ? 'border-black bg-gray-100'
                        : isEnabled
                        ? 'border-black-70 hover:bg-gray-50 cursor-pointer'
                        : 'border-gray-300 bg-gray-50 cursor-not-allowed opacity-50'
                    }`}
                  >
                    {isSelected && (
                      <div className="absolute top-2 right-2 text-black text-xl">✓</div>
                    )}
                    <h3 className="font-black italic uppercase tracking-1 text-lg mb-1">
                      {mode.name}
                    </h3>
                    <p className="text-sm text-gray-600">{mode.description}</p>
                  </button>
                );
              })}
            </div>

            {/* Start Queue Button */}
            {selectedModes.size > 0 && (
              <button
                onClick={handleStartQueue}
                disabled={isAuthenticating || (!user && (!username || username.length < 3))}
                className="w-full mt-4 px-6 py-4 bg-black text-white font-black italic uppercase tracking-1 rounded-lg hover:bg-gray-800 transition-all disabled:bg-gray-400 disabled:cursor-not-allowed"
              >
                {isAuthenticating ? 'Starting...' : `Start Queue (${selectedModes.size} mode${selectedModes.size > 1 ? 's' : ''})`}
              </button>
            )}
          </div>

          {/* Back Button */}
          <div className="mt-6 pt-6 border-t-2 border-gray-200">
            <button
              onClick={() => navigate('/')}
              className="btn-secondary w-full md:w-auto"
            >
              Back to Main Menu
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

export default GameModeSelector;