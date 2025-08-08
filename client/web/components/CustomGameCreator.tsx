import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useGameWebSocket } from '../hooks/useGameWebSocket';
import { useAuth } from '../contexts/AuthContext';
import { api } from '../services/api';
import { UIGameSettings, SpeedMap, FoodSpawnMap, GameMode } from '../types';
import { GAME_SPEED_TO_MS } from '../constants';

function CustomGameCreator() {
  const navigate = useNavigate();
  const { createCustomGame, customGameCode, isConnected } = useGameWebSocket();
  const { user, login, register } = useAuth();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [authError, setAuthError] = useState<string | null>(null);
  const [isAuthenticating, setIsAuthenticating] = useState(false);
  // Load saved username on mount
  useEffect(() => {
    const savedUsername = localStorage.getItem('savedUsername');
    if (savedUsername) {
      setUsername(savedUsername);
    }
  }, []);

  const [settings, setSettings] = useState<UIGameSettings>({
    gameMode: 'freeForAll',
    arenaWidth: 40,
    arenaHeight: 40,
    maxPlayers: 4,
    foodSpawnRate: 'medium',
    gameSpeed: 'normal',
    allowJoin: true,
    allowSpectators: true,
    snakeStartLength: 3,
  });

  // Navigate to lobby when game is created
  useEffect(() => {
    if (customGameCode) {
      navigate(`/game/${customGameCode}`);
    }
  }, [customGameCode, navigate]);

  const handleSettingChange = <K extends keyof UIGameSettings>(key: K, value: UIGameSettings[K]) => {
    setSettings(prev => ({ ...prev, [key]: value }));
  };

  const handleCreateGame = async () => {
    if (!username || username.length < 3) {
      setAuthError('Please enter a username (at least 3 characters)');
      return;
    }

    if (!isConnected) {
      setAuthError('Not connected to server');
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
      }

      // Convert UI settings to server format
      const serverSettings = {
        arena_width: settings.arenaWidth,
        arena_height: settings.arenaHeight,
        tick_duration_ms: GAME_SPEED_TO_MS[settings.gameSpeed as keyof typeof GAME_SPEED_TO_MS],
        food_spawn_rate: foodSpawnRates[settings.foodSpawnRate as keyof typeof foodSpawnRates],
        max_players: settings.gameMode === 'duel' ? 2 : settings.gameMode === 'solo' ? 1 : settings.maxPlayers,
        game_mode: settings.gameMode === 'solo' ? 'Solo' as GameMode : 
                   settings.gameMode === 'duel' ? 'Duel' as GameMode : 
                   { FreeForAll: { max_players: settings.maxPlayers } } as GameMode,
        is_private: !settings.allowJoin,
        allow_spectators: settings.allowSpectators,
        snake_start_length: settings.snakeStartLength,
      };

      createCustomGame(serverSettings);
    } catch (error) {
      setAuthError((error as Error).message || 'Failed to create game');
    } finally {
      setIsAuthenticating(false);
    }
  };


  const foodSpawnRates = {
    low: 1.5,
    medium: 3.0,
    high: 5.0,
    extreme: 8.0,
  };

  return (
    <div className="flex-1 p-8">
      <div className="max-w-xl mx-auto">
        <h1 className="panel-heading mb-6">CREATE CUSTOM GAME</h1>
        <div className="panel p-6">
          <div className="space-y-6">
          {/* Username/Authentication Section */}
          {!user && (
            <div>
              <label className="block text-sm font-bold uppercase tracking-1 mb-2">Username</label>
              <input
                type="text"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                placeholder="Enter username"
                className="w-full px-4 py-3 text-base border-2 border-black-70 rounded mb-3"
                disabled={isAuthenticating}
              />
              {username && (
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="Password (optional for new users)"
                  className="w-full px-4 py-3 text-base border-2 border-black-70 rounded"
                  disabled={isAuthenticating}
                />
              )}
              {authError && (
                <p className="text-red-600 text-sm mt-2">{authError}</p>
              )}
            </div>
          )}
          
          {/* Game Mode */}
          <div>
            <label className="block text-sm font-bold uppercase tracking-1 mb-2">Game Mode</label>
            <div className="grid grid-cols-3 gap-2">
              <button
                onClick={() => handleSettingChange('gameMode', 'solo')}
                className={`btn-toggle ${settings.gameMode === 'solo' ? 'active' : ''}`}
              >
                Solo
              </button>
              <button
                onClick={() => handleSettingChange('gameMode', 'duel')}
                className={`btn-toggle ${settings.gameMode === 'duel' ? 'active' : ''}`}
              >
                Duel
              </button>
              <button
                onClick={() => handleSettingChange('gameMode', 'freeForAll')}
                className={`btn-toggle ${settings.gameMode === 'freeForAll' ? 'active' : ''}`}
              >
                Free For All
              </button>
            </div>
          </div>


          {/* Max Players (if not duel or single player) */}
          {settings.gameMode === 'freeForAll' && (
            <div>
              <label className="block text-sm font-bold uppercase tracking-1 mb-2">
                Max Players: <span data-testid="max-players-value">{settings.maxPlayers}</span>
              </label>
              <input
                data-testid="max-players-slider"
                type="range"
                min="2"
                max="8"
                value={settings.maxPlayers}
                onChange={(e) => handleSettingChange('maxPlayers', parseInt(e.target.value))}
                className="w-full"
              />
            </div>
          )}

          {/* Game Speed */}
          <div>
            <label className="block text-sm font-bold uppercase tracking-1 mb-2">Game Speed</label>
            <div className="grid grid-cols-4 gap-2">
              <button
                onClick={() => handleSettingChange('gameSpeed', 'slow')}
                className={`btn-toggle ${settings.gameSpeed === 'slow' ? 'active' : ''}`}
              >
                Slow
              </button>
              <button
                onClick={() => handleSettingChange('gameSpeed', 'normal')}
                className={`btn-toggle ${settings.gameSpeed === 'normal' ? 'active' : ''}`}
              >
                Normal
              </button>
              <button
                onClick={() => handleSettingChange('gameSpeed', 'fast')}
                className={`btn-toggle ${settings.gameSpeed === 'fast' ? 'active' : ''}`}
              >
                Fast
              </button>
              <button
                onClick={() => handleSettingChange('gameSpeed', 'extreme')}
                className={`btn-toggle ${settings.gameSpeed === 'extreme' ? 'active' : ''}`}
              >
                Extreme
              </button>
            </div>
          </div>

          {/* Food Spawn Rate */}
          <div>
            <label className="block text-sm font-bold uppercase tracking-1 mb-2">Food Spawn Rate</label>
            <div className="grid grid-cols-4 gap-2">
              <button
                onClick={() => handleSettingChange('foodSpawnRate', 'low')}
                className={`btn-toggle ${settings.foodSpawnRate === 'low' ? 'active' : ''}`}
              >
                Low
              </button>
              <button
                onClick={() => handleSettingChange('foodSpawnRate', 'medium')}
                className={`btn-toggle ${settings.foodSpawnRate === 'medium' ? 'active' : ''}`}
              >
                Medium
              </button>
              <button
                onClick={() => handleSettingChange('foodSpawnRate', 'high')}
                className={`btn-toggle ${settings.foodSpawnRate === 'high' ? 'active' : ''}`}
              >
                High
              </button>
              <button
                onClick={() => handleSettingChange('foodSpawnRate', 'extreme')}
                className={`btn-toggle ${settings.foodSpawnRate === 'extreme' ? 'active' : ''}`}
              >
                Extreme
              </button>
            </div>
          </div>

          {/* Privacy Settings */}
          <div className="space-y-3">
            <label className="flex items-center gap-3 cursor-pointer">
              <input
                type="checkbox"
                checked={settings.allowJoin}
                onChange={(e) => handleSettingChange('allowJoin', e.target.checked)}
                className="w-4 h-4 border border-black-70"
              />
              <span className="text-sm font-bold uppercase tracking-1">Allow anyone to Join</span>
            </label>
            
            <label className="flex items-center gap-3 cursor-pointer">
              <input
                type="checkbox"
                checked={settings.allowSpectators}
                onChange={(e) => handleSettingChange('allowSpectators', e.target.checked)}
                className="w-4 h-4 border border-black-70"
              />
              <span className="text-sm font-bold uppercase tracking-1">Allow anyone to Spectate</span>
            </label>
          </div>

          {/* Action Buttons */}
          <div className="flex gap-4 mt-8">
            <button
              data-testid="back-button"
              onClick={() => navigate('/')}
              className="flex-1 btn-secondary"
            >
              Cancel
            </button>
            <button
              data-testid="create-game-button"
              onClick={handleCreateGame}
              className="flex-1 btn-primary-straight"
              disabled={isAuthenticating || (!user && (!username || username.length < 3))}
            >
              {isAuthenticating ? 'Creating...' : 'Create Game'}
            </button>
          </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default CustomGameCreator;