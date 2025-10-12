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
    <div
      className="fixed inset-0 flex items-center justify-center p-4 z-50"
      onClick={onClose}
      style={{ backgroundColor: 'rgba(255, 255, 255, 0.7)' }}
    >
      <div
        className="bg-white rounded-lg p-8 w-full max-w-lg"
        onClick={(e) => e.stopPropagation()}
        style={{
          border: '2px solid rgba(0, 0, 0, 0.2)',
          boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1)'
        }}
      >
        <div className="text-center mb-6">
          <h2 className="text-2xl font-black italic uppercase tracking-1 text-black-70 mb-2">
            Join Game
          </h2>
          <p className="text-sm text-black-70 opacity-60">
            Enter the lobby code to join your friends
          </p>
        </div>

        <form onSubmit={handleSubmit}>
          <div className="mb-5">
            <label className="block text-xs font-black italic uppercase tracking-1 text-black-70 mb-2 opacity-50">
              Code
            </label>
            <input
              type="text"
              value={gameCode}
              onChange={(e) => setGameCode(e.target.value.toUpperCase())}
              placeholder="XXXXXXXX"
              maxLength={8}
              className="w-full px-4 py-3 border-2 border-black-70 rounded-lg font-mono text-2xl text-center uppercase tracking-widest text-black-70"
              autoFocus
            />
          </div>

          {error && (
            <p className="text-red-600 text-sm mb-5 text-center">{error}</p>
          )}

          <div className="flex gap-2">
            <button
              type="button"
              onClick={onClose}
              className="flex-1 px-6 py-3 border-2 border-black-70 rounded-lg font-black italic uppercase tracking-1 text-black-70 hover:bg-gray-50 transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isJoining || !gameCode.trim()}
              className="flex-1 px-6 py-3 border-2 border-black-70 rounded-lg font-black italic uppercase tracking-1 text-black-70 hover:bg-gray-50 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
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