import React, { useState, useEffect } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext.jsx';
import { useGameWebSocket } from '../hooks/useGameWebSocket.js';
import { useDebounce } from '../hooks/useDebounce.js';
import { api } from '../services/api.js';
import { CheckIcon, XIcon } from './Icons.jsx';
import Spinner from './Spinner.jsx';

const GAME_MODES = {
  'quick-play': {
    title: 'QUICK MATCH',
    modes: [
      { id: 'duel', name: 'DUEL', description: '1v1 battle' },
      { id: 'free-for-all', name: 'FREE FOR ALL', description: 'Multiple players' }
    ]
  },
  'competitive': {
    title: 'COMPETITIVE',
    modes: [
      { id: 'ranked-duel', name: 'RANKED DUEL', description: 'Competitive 1v1' },
      { id: 'ranked-team', name: 'RANKED TEAM', description: 'Team battles' }
    ]
  },
  'solo': {
    title: 'SOLO',
    modes: [
      { id: 'practice', name: 'PRACTICE', description: 'Improve your skills' },
      { id: 'challenge', name: 'CHALLENGE', description: 'Complete objectives' }
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
  const [requiresPassword, setRequiresPassword] = useState(false);
  const [checkingUsername, setCheckingUsername] = useState(false);
  const [usernameStatus, setUsernameStatus] = useState(null); // 'available', 'taken', 'exists'
  const [authError, setAuthError] = useState(null);
  const [isAuthenticating, setIsAuthenticating] = useState(false);
  const [isAuthenticated, setIsAuthenticated] = useState(false);

  const gameModeConfig = GAME_MODES[category];

  // Load saved username on mount
  useEffect(() => {
    const savedUsername = localStorage.getItem('savedUsername');
    if (savedUsername) {
      setUsername(savedUsername);
      checkUsernameStatus(savedUsername);
    }
    
    // If already logged in, mark as authenticated
    if (user) {
      setUsername(user.username);
      setIsAuthenticated(true);
    }
  }, [user]);

  // Redirect if invalid category
  useEffect(() => {
    if (!gameModeConfig) {
      navigate('/');
    }
  }, [category, gameModeConfig, navigate]);

  // Check username status with debounce
  const checkUsernameStatus = useDebounce(async (username) => {
    if (!username || username.length < 3) {
      setUsernameStatus(null);
      setRequiresPassword(false);
      setIsAuthenticated(false);
      return;
    }

    // If already logged in with this username, don't check
    if (user && user.username === username) {
      setUsernameStatus('authenticated');
      setIsAuthenticated(true);
      return;
    }

    setCheckingUsername(true);
    setAuthError(null);
    
    try {
      const response = await api.checkUsername(username);
      if (response.available) {
        setUsernameStatus('available');
        setRequiresPassword(false);
        setIsAuthenticated(false);
      } else {
        setUsernameStatus('exists');
        setRequiresPassword(response.requiresPassword || false);
        setIsAuthenticated(false);
      }
    } catch (error) {
      console.error('Username check failed:', error);
      setUsernameStatus(null);
    } finally {
      setCheckingUsername(false);
    }
  }, 500);

  const handleUsernameChange = (value) => {
    setUsername(value);
    setPassword('');
    setAuthError(null);
    setIsAuthenticated(false);
    checkUsernameStatus(value);
  };

  const handleAuthenticate = async () => {
    if (!username || username.length < 3) {
      setAuthError('Username must be at least 3 characters');
      return;
    }

    setIsAuthenticating(true);
    setAuthError(null);

    try {
      if (usernameStatus === 'available') {
        // Register as guest (no password)
        await register(username, password || null);
      } else if (usernameStatus === 'exists') {
        if (requiresPassword && !password) {
          setAuthError('Password is required for this username');
          setIsAuthenticating(false);
          return;
        }
        // Login with password
        await login(username, password);
      }

      // Save username for next time
      localStorage.setItem('savedUsername', username);
      setIsAuthenticated(true);
    } catch (error) {
      setAuthError(error.message || 'Authentication failed');
    } finally {
      setIsAuthenticating(false);
    }
  };

  const handleGameModeClick = async (modeId) => {
    if (!isAuthenticated) {
      await handleAuthenticate();
      if (!isAuthenticated) return;
    }

    if (!isConnected) {
      setAuthError('Not connected to game server');
      return;
    }

    // Create the game
    createGame(modeId);
    
    // Navigate to custom game creator for now
    // TODO: Navigate to actual game when implemented
    navigate('/custom');
  };

  const canPlayGame = isAuthenticated || (username.length >= 3 && !checkingUsername);

  if (!gameModeConfig) return null;

  return (
    <div className="flex-1 p-8">
      <div className="max-w-3xl mx-auto">
        <h1 className="panel-heading mb-6">{gameModeConfig.title}</h1>
        
        <div className="panel p-6">
          {/* Authentication Section */}
          <div className="mb-8 pb-8 border-b-2 border-gray-200">
            <div className="space-y-4">
              {/* Username Field */}
              <div>
                <label className="block text-sm font-bold uppercase tracking-1 mb-2">
                  Username
                </label>
                <div className="relative">
                  <input
                    type="text"
                    value={username}
                    onChange={(e) => handleUsernameChange(e.target.value)}
                    className="w-full px-4 py-2 text-sm border-2 border-black-70 rounded"
                    placeholder="Enter username"
                    disabled={isAuthenticated}
                  />
                  {username && !isAuthenticated && (
                    <div className="absolute right-3 top-2.5">
                      {checkingUsername ? (
                        <Spinner className="w-4 h-4" />
                      ) : usernameStatus === 'available' ? (
                        <CheckIcon className="w-4 h-4 text-green-600" />
                      ) : usernameStatus === 'exists' ? (
                        <XIcon className="w-4 h-4 text-yellow-600" />
                      ) : null}
                    </div>
                  )}
                  {isAuthenticated && (
                    <div className="absolute right-3 top-2.5">
                      <CheckIcon className="w-4 h-4 text-green-600" />
                    </div>
                  )}
                </div>
                {username && usernameStatus === 'available' && !isAuthenticated && (
                  <p className="text-xs text-green-600 mt-1">
                    Username available - will be created as guest
                  </p>
                )}
                {username && usernameStatus === 'exists' && !isAuthenticated && (
                  <p className="text-xs text-yellow-600 mt-1">
                    {requiresPassword ? 'Username exists - password required' : 'Username exists - no password required'}
                  </p>
                )}
              </div>

              {/* Password Field (only if required) */}
              {requiresPassword && !isAuthenticated && (
                <div>
                  <label className="block text-sm font-bold uppercase tracking-1 mb-2">
                    Password
                  </label>
                  <input
                    type="password"
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    onKeyPress={(e) => e.key === 'Enter' && handleAuthenticate()}
                    className="w-full px-4 py-2 text-sm border-2 border-black-70 rounded"
                    placeholder="Enter password"
                  />
                </div>
              )}

              {/* Error Message */}
              {authError && (
                <p className="text-red-600 text-sm">{authError}</p>
              )}

              {/* Auth Status */}
              {isAuthenticated && (
                <p className="text-green-600 text-sm">
                  ✓ Authenticated as {username}
                </p>
              )}
            </div>
          </div>

          {/* Game Modes */}
          <div className="space-y-4">
            <h2 className="text-lg font-bold uppercase tracking-1">Select Game Mode</h2>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {gameModeConfig.modes.map((mode) => (
                <button
                  key={mode.id}
                  onClick={() => handleGameModeClick(mode.id)}
                  disabled={!canPlayGame || isAuthenticating}
                  className={`p-6 text-left border-2 rounded-lg transition-all ${
                    canPlayGame && !isAuthenticating
                      ? 'border-black-70 hover:bg-gray-50 cursor-pointer'
                      : 'border-gray-300 bg-gray-50 cursor-not-allowed opacity-50'
                  }`}
                >
                  <h3 className="font-black italic uppercase tracking-1 text-lg mb-1">
                    {mode.name}
                  </h3>
                  <p className="text-sm text-gray-600">{mode.description}</p>
                </button>
              ))}
            </div>
            {!canPlayGame && (
              <p className="text-sm text-gray-600 text-center mt-4">
                Please enter a valid username to continue
              </p>
            )}
          </div>

          {/* Back Button */}
          <div className="mt-8 pt-6 border-t-2 border-gray-200">
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