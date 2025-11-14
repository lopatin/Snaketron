import React from 'react';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import { useGameWebSocket } from '../hooks/useGameWebSocket';

export const MatchmakingBanner: React.FC = () => {
  const { user } = useAuth();
  const { currentLobby } = useWebSocket();
  const { leaveQueue } = useGameWebSocket();

  if (!currentLobby || currentLobby.state !== 'queued') {
    return null;
  }

  const isHost = user && currentLobby.hostUserId === user.id;

  const handleCancel = () => {
    if (!isHost) {
      return;
    }
    leaveQueue();
  };

  return (
    <div className="fixed top-4 left-0 right-0 z-50 flex justify-center px-4 pointer-events-none">
      <div className="flex items-center gap-3 px-4 py-2 rounded-full bg-white/95 border border-gray-300 shadow-md text-xs font-bold uppercase tracking-1 text-black-70 pointer-events-auto">
        <span className="w-4 h-4 border-2 border-gray-300 border-t-black rounded-full animate-spin" aria-hidden="true" />
        <span>{isHost ? 'Finding match...' : 'Host is finding a match...'}</span>
        {isHost && (
          <button
            type="button"
            onClick={handleCancel}
            className="ml-2 px-3 py-1 text-[10px] font-bold uppercase tracking-1 rounded-full border border-gray-300 text-gray-600 hover:bg-gray-50 transition-colors"
          >
            Cancel
          </button>
        )}
      </div>
    </div>
  );
};
