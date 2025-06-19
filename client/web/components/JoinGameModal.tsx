import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useGameWebSocket } from '../hooks/useGameWebSocket';
import { JoinGameModalProps, FormEventHandler } from '../types';

function JoinGameModal({ isOpen, onClose }: JoinGameModalProps) {
  const navigate = useNavigate();
  const { joinCustomGame, isConnected } = useGameWebSocket();
  const [gameCode, setGameCode] = useState('');
  const [error, setError] = useState('');
  const [isJoining, setIsJoining] = useState(false);

  const handleSubmit: FormEventHandler = async (e) => {
    e.preventDefault();
    
    if (!gameCode.trim()) {
      setError('Please enter a game code');
      return;
    }
    
    if (!isConnected) {
      setError('Not connected to server');
      return;
    }
    
    setIsJoining(true);
    setError('');
    
    try {
      await joinCustomGame(gameCode.toUpperCase());
      // Navigation will be handled by WebSocket response
      navigate(`/game/${gameCode.toUpperCase()}`);
      onClose();
    } catch (err) {
      setError('Failed to join game. Please check the code and try again.');
      setIsJoining(false);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
      <div className="panel p-6 max-w-md w-full">
        <h2 className="text-2xl font-black italic uppercase tracking-1 mb-4">Join Game</h2>
        
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-bold uppercase tracking-1 mb-2">
              Game Code
            </label>
            <input
              type="text"
              value={gameCode}
              onChange={(e) => setGameCode(e.target.value.toUpperCase())}
              placeholder="ABCD1234"
              maxLength={8}
              className="w-full px-4 py-3 border-2 border-black-70 rounded font-mono text-xl text-center uppercase"
              autoFocus
            />
          </div>
          
          {error && (
            <p className="text-red-600 text-sm">{error}</p>
          )}
          
          <div className="flex gap-3">
            <button
              type="button"
              onClick={onClose}
              className="flex-1 btn-secondary"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isJoining || !gameCode.trim()}
              className="flex-1 btn-primary-straight disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isJoining ? 'Joining...' : 'Join'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

export default JoinGameModal;