import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { useGameWebSocket } from '../hooks/useGameWebSocket.js';

function CustomGameCreator() {
  const navigate = useNavigate();
  const { createCustomGame, customGameCode, isConnected } = useGameWebSocket();
  const [settings, setSettings] = useState({
    gameMode: 'freeForAll',
    arenaWidth: 40,
    arenaHeight: 40,
    maxPlayers: 4,
    foodSpawnRate: 3.0,
    gameSpeed: 'normal',
    tacticalMode: false,
    isPrivate: true,
    allowSpectators: true,
    snakeStartLength: 3,
  });

  // Navigate to lobby when game is created
  useEffect(() => {
    if (customGameCode) {
      navigate(`/game/${customGameCode}`);
    }
  }, [customGameCode, navigate]);

  const handleSettingChange = (key, value) => {
    setSettings(prev => ({ ...prev, [key]: value }));
  };

  const handleCreateGame = async () => {
    if (!isConnected) {
      console.error('Not connected to server');
      return;
    }

    // Convert UI settings to server format
    const serverSettings = {
      arena_width: settings.arenaWidth,
      arena_height: settings.arenaHeight,
      tick_duration_ms: gameSpeedToMs[settings.gameSpeed],
      food_spawn_rate: settings.foodSpawnRate,
      max_players: settings.gameMode === 'duel' ? 2 : settings.gameMode === 'solo' ? 1 : settings.maxPlayers,
      game_mode: settings.gameMode === 'solo' ? 'Solo' : 
                 settings.gameMode === 'duel' ? 'Duel' : 
                 { FreeForAll: { max_players: settings.maxPlayers } },
      is_private: settings.isPrivate,
      allow_spectators: settings.allowSpectators,
      snake_start_length: settings.snakeStartLength,
      tactical_mode: settings.tacticalMode,
    };

    createCustomGame(serverSettings);
  };

  const gameSpeedToMs = {
    slow: 500,
    normal: 300,
    fast: 200,
    extreme: 100,
  };

  return (
    <div className="flex-1 flex justify-center items-center p-5">
      <div className="max-w-2xl w-full bg-white rounded-lg border-2 border-black-70 p-8">
        <h1 className="text-3xl font-black italic uppercase tracking-1 mb-8 text-center">CREATE CUSTOM GAME</h1>
        
        <div className="space-y-6">
          {/* Game Mode */}
          <div>
            <label className="block text-sm font-bold uppercase tracking-1 mb-2">Game Mode</label>
            <select
              data-testid="game-mode-select"
              value={settings.gameMode}
              onChange={(e) => handleSettingChange('gameMode', e.target.value)}
              className="w-full px-3 py-2 border border-black-70 rounded font-bold uppercase tracking-1 bg-white"
            >
              <option value="solo">Solo</option>
              <option value="duel">Duel</option>
              <option value="freeForAll">Free For All</option>
            </select>
            <div className="grid grid-cols-3 gap-2 mt-2">
              <button
                onClick={() => handleSettingChange('gameMode', 'solo')}
                className={`px-4 py-2 border border-black-70 rounded font-bold italic uppercase tracking-1 transition-all ${
                  settings.gameMode === 'solo' ? 'bg-black-70 text-white' : 'bg-white text-black-70 hover:bg-gray-100'
                }`}
              >
                Solo
              </button>
              <button
                onClick={() => handleSettingChange('gameMode', 'duel')}
                className={`px-4 py-2 border border-black-70 rounded font-bold italic uppercase tracking-1 transition-all ${
                  settings.gameMode === 'duel' ? 'bg-black-70 text-white' : 'bg-white text-black-70 hover:bg-gray-100'
                }`}
              >
                Duel
              </button>
              <button
                onClick={() => handleSettingChange('gameMode', 'freeForAll')}
                className={`px-4 py-2 border border-black-70 rounded font-bold italic uppercase tracking-1 transition-all ${
                  settings.gameMode === 'freeForAll' ? 'bg-black-70 text-white' : 'bg-white text-black-70 hover:bg-gray-100'
                }`}
              >
                Free For All
              </button>
            </div>
          </div>

          {/* Arena Size */}
          <div>
            <label className="block text-sm font-bold uppercase tracking-1 mb-2">
              Arena Size: <span data-testid="arena-size-value">{settings.arenaWidth}x{settings.arenaHeight}</span>
            </label>
            <input
              data-testid="arena-size-slider"
              type="range"
              min="20"
              max="60"
              value={settings.arenaWidth}
              onChange={(e) => {
                const size = parseInt(e.target.value);
                handleSettingChange('arenaWidth', size);
                handleSettingChange('arenaHeight', size);
              }}
              className="w-full"
            />
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
            <label className="block text-sm font-bold uppercase tracking-1 mb-2">
              Game Speed: <span data-testid="game-speed-value">{settings.gameSpeed}</span>
            </label>
            <select
              data-testid="game-speed-select"
              value={settings.gameSpeed}
              onChange={(e) => handleSettingChange('gameSpeed', e.target.value)}
              className="w-full px-3 py-2 border border-black-70 rounded font-bold uppercase tracking-1 bg-white mb-2"
            >
              <option value="slow">Slow</option>
              <option value="normal">Normal</option>
              <option value="fast">Fast</option>
              <option value="extreme">Extreme</option>
            </select>
            <div className="grid grid-cols-4 gap-2">
              {Object.keys(gameSpeedToMs).map(speed => (
                <button
                  key={speed}
                  onClick={() => handleSettingChange('gameSpeed', speed)}
                  className={`px-4 py-2 border border-black-70 rounded font-bold italic uppercase tracking-1 transition-all ${
                    settings.gameSpeed === speed ? 'bg-black-70 text-white' : 'bg-white text-black-70 hover:bg-gray-100'
                  }`}
                >
                  {speed}
                </button>
              ))}
            </div>
          </div>

          {/* Food Spawn Rate */}
          <div>
            <label className="block text-sm font-bold uppercase tracking-1 mb-2">
              Food Per Minute: {settings.foodSpawnRate.toFixed(1)}
            </label>
            <input
              type="range"
              min="0.5"
              max="10"
              step="0.5"
              value={settings.foodSpawnRate}
              onChange={(e) => handleSettingChange('foodSpawnRate', parseFloat(e.target.value))}
              className="w-full"
            />
          </div>

          {/* Game Style */}
          <div>
            <label className="block text-sm font-bold uppercase tracking-1 mb-2">Game Style</label>
            <div className="grid grid-cols-2 gap-2">
              <button
                onClick={() => handleSettingChange('tacticalMode', false)}
                className={`px-4 py-2 border border-black-70 rounded font-bold italic uppercase tracking-1 transition-all ${
                  !settings.tacticalMode ? 'bg-black-70 text-white' : 'bg-white text-black-70 hover:bg-gray-100'
                }`}
              >
                Classic
              </button>
              <button
                onClick={() => handleSettingChange('tacticalMode', true)}
                className={`px-4 py-2 border border-black-70 rounded font-bold italic uppercase tracking-1 transition-all ${
                  settings.tacticalMode ? 'bg-black-70 text-white' : 'bg-white text-black-70 hover:bg-gray-100'
                }`}
              >
                Tactical
              </button>
            </div>
          </div>

          {/* Privacy Settings */}
          <div className="space-y-3">
            <label className="flex items-center gap-3 cursor-pointer">
              <input
                type="checkbox"
                checked={settings.isPrivate}
                onChange={(e) => handleSettingChange('isPrivate', e.target.checked)}
                className="w-5 h-5 border-2 border-black-70"
              />
              <span className="font-bold uppercase tracking-1">Private Game</span>
            </label>
            
            <label className="flex items-center gap-3 cursor-pointer">
              <input
                type="checkbox"
                checked={settings.allowSpectators}
                onChange={(e) => handleSettingChange('allowSpectators', e.target.checked)}
                disabled={!settings.isPrivate}
                className="w-5 h-5 border-2 border-black-70 disabled:opacity-50"
              />
              <span className={`font-bold uppercase tracking-1 ${!settings.isPrivate ? 'opacity-50' : ''}`}>
                Allow Spectators
              </span>
            </label>
          </div>

          {/* Action Buttons */}
          <div className="flex gap-4 mt-8">
            <button
              data-testid="back-button"
              onClick={() => navigate('/')}
              className="flex-1 px-6 py-3 border border-black-70 rounded font-black italic uppercase tracking-1 bg-white text-black-70 hover:bg-gray-100 transition-colors"
            >
              Cancel
            </button>
            <button
              data-testid="create-game-button"
              onClick={handleCreateGame}
              className="flex-1 px-6 py-3 border-2 border-black-70 rounded font-black italic uppercase tracking-1 bg-black-70 text-white hover:bg-black transition-colors"
            >
              Create Game
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

export default CustomGameCreator;