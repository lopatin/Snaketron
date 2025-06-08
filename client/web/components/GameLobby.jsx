import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useGameWebSocket } from '../hooks/useGameWebSocket.js';

function GameLobby() {
  const { gameCode } = useParams();
  const navigate = useNavigate();
  const { 
    isConnected, 
    gameState, 
    currentGameId, 
    startCustomGame,
    isHost 
  } = useGameWebSocket();
  
  const [players, setPlayers] = useState([]);
  const [gameSettings, setGameSettings] = useState(null);
  const [isStarting, setIsStarting] = useState(false);
  const [copied, setCopied] = useState(false);

  // Mock data for now - will be replaced with real WebSocket data
  useEffect(() => {
    // TODO: Get real game data from WebSocket
    setPlayers([
      { id: 1, name: 'Player 1 (Host)', isHost: true, isReady: true },
      { id: 2, name: 'Player 2', isHost: false, isReady: false },
    ]);
    
    setGameSettings({
      gameMode: 'FreeForAll',
      maxPlayers: 4,
      arenaSize: '40x40',
      gameSpeed: 'Normal',
      foodSpawnRate: '3.0/min',
      tacticalMode: false,
    });
  }, [gameCode]);

  const handleStartGame = async () => {
    if (!isConnected) {
      console.error('Not connected to server');
      return;
    }
    
    setIsStarting(true);
    try {
      await startCustomGame();
      // Navigation will be handled by WebSocket response
    } catch (error) {
      console.error('Failed to start game:', error);
      setIsStarting(false);
    }
  };

  const handleCopyCode = () => {
    navigator.clipboard.writeText(gameCode);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const handleLeaveGame = () => {
    // TODO: Implement leave game logic
    navigate('/');
  };

  return (
    <div className="flex-1 flex justify-center items-center p-5">
      <div className="max-w-4xl w-full grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Left Column: Players */}
        <div className="lg:col-span-2 bg-white rounded-lg border-2 border-black-70 p-6">
          <h2 className="text-2xl font-black italic uppercase tracking-1 mb-6">Players</h2>
          
          <div className="space-y-3" data-testid="players-list">
            {players.map((player, index) => (
              <div 
                key={player.id}
                data-testid="player-item"
                className="flex items-center justify-between p-4 border border-black-70 rounded"
              >
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 bg-black-70 rounded-full flex items-center justify-center text-white font-bold">
                    {index + 1}
                  </div>
                  <div>
                    <span className="font-bold">{player.name}</span>
                    {player.isHost && (
                      <span data-testid="host-badge" className="ml-2 text-sm font-bold italic uppercase tracking-1 opacity-70">
                        (Host)
                      </span>
                    )}
                  </div>
                </div>
                {player.isReady && (
                  <span className="text-green-600 font-bold uppercase">Ready</span>
                )}
              </div>
            ))}
            
            {/* Empty slots */}
            {gameSettings && Array(gameSettings.maxPlayers - players.length).fill(0).map((_, index) => (
              <div 
                key={`empty-${index}`}
                className="flex items-center p-4 border border-gray-300 rounded opacity-50"
              >
                <div className="w-10 h-10 bg-gray-300 rounded-full flex items-center justify-center text-gray-600 font-bold">
                  {players.length + index + 1}
                </div>
                <span className="ml-3 text-gray-600 italic">Waiting for player...</span>
              </div>
            ))}
          </div>

          {/* Action Buttons */}
          <div className="mt-6 flex gap-3">
            {isHost ? (
              <button
                data-testid="start-game-button"
                onClick={handleStartGame}
                disabled={!isConnected || isStarting || players.length < 2}
                className="flex-1 px-6 py-3 border-2 border-black-70 rounded font-black italic uppercase tracking-1 bg-black-70 text-white hover:bg-black transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {isStarting ? 'Starting...' : 'Start Game'}
              </button>
            ) : (
              <button
                disabled
                className="flex-1 px-6 py-3 border border-gray-400 rounded font-bold italic uppercase tracking-1 bg-gray-100 text-gray-500 cursor-not-allowed"
              >
                Waiting for Host
              </button>
            )}
            
            <button
              data-testid="leave-game-button"
              onClick={handleLeaveGame}
              className="px-6 py-3 border border-black-70 rounded font-bold italic uppercase tracking-1 bg-white text-black-70 hover:bg-gray-100 transition-colors"
            >
              Leave
            </button>
          </div>
        </div>

        {/* Right Column: Game Info */}
        <div className="space-y-6">
          {/* Game Code */}
          <div className="bg-white rounded-lg border-2 border-black-70 p-6">
            <h3 className="text-lg font-black italic uppercase tracking-1 mb-3">Game Code</h3>
            <div className="flex items-center gap-2">
              <div data-testid="game-code" className="flex-1 p-3 bg-gray-100 rounded font-mono text-xl text-center font-bold">
                {gameCode}
              </div>
              <button
                data-testid="copy-code-button"
                onClick={handleCopyCode}
                className="px-4 py-3 border border-black-70 rounded font-bold italic uppercase tracking-1 bg-white text-black-70 hover:bg-gray-100 transition-colors"
              >
                {copied ? '✓' : 'Copy'}
              </button>
            </div>
            <p className="mt-2 text-sm opacity-70">Share this code with friends to join</p>
          </div>

          {/* Game Settings */}
          {gameSettings && (
            <div data-testid="game-settings" className="bg-white rounded-lg border-2 border-black-70 p-6">
              <h3 className="text-lg font-black italic uppercase tracking-1 mb-3">Settings</h3>
              <dl className="space-y-2 text-sm">
                <div className="flex justify-between">
                  <dt className="font-bold uppercase opacity-70">Mode</dt>
                  <dd data-testid="game-mode-value" className="font-mono">{gameSettings.gameMode}</dd>
                </div>
                <div className="flex justify-between">
                  <dt className="font-bold uppercase opacity-70">Max Players</dt>
                  <dd data-testid="max-players-value" className="font-mono">{gameSettings.maxPlayers}</dd>
                </div>
                <div className="flex justify-between">
                  <dt className="font-bold uppercase opacity-70">Arena</dt>
                  <dd data-testid="arena-size-value" className="font-mono">{gameSettings.arenaSize}</dd>
                </div>
                <div className="flex justify-between">
                  <dt className="font-bold uppercase opacity-70">Speed</dt>
                  <dd data-testid="game-speed-value" className="font-mono">{gameSettings.gameSpeed}</dd>
                </div>
                <div className="flex justify-between">
                  <dt className="font-bold uppercase opacity-70">Food Rate</dt>
                  <dd className="font-mono">{gameSettings.foodSpawnRate}</dd>
                </div>
                <div className="flex justify-between">
                  <dt className="font-bold uppercase opacity-70">Style</dt>
                  <dd className="font-mono">{gameSettings.tacticalMode ? 'Tactical' : 'Classic'}</dd>
                </div>
              </dl>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default GameLobby;