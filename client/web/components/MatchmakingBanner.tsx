import React from 'react';
import { useWebSocket } from '../contexts/WebSocketContext';
import { useGameWebSocket } from '../hooks/useGameWebSocket';

export const MatchmakingBanner: React.FC = () => {
  const { currentLobby, isLobbyLeader } = useWebSocket();
  const { leaveQueue, isQueued, isJoiningGame } = useGameWebSocket();

  const isLobbyQueued = currentLobby?.state === 'queued';
  const isBannerVisible = isQueued || isJoiningGame || isLobbyQueued;

  if (!isBannerVisible) {
    return null;
  }

  // Cancelling pulls the whole lobby out of the queue, so it belongs to
  // whoever was allowed to start it. A member of someone else's queued lobby
  // still sees the banner — they should know a match is being found — but not
  // a control that would cancel it for everyone.
  const showCancel = (isQueued || isLobbyQueued) && isLobbyLeader;
  const statusText = (() => {
    if (isJoiningGame) {
      return 'Joining game...';
    }

    if (isQueued || isLobbyQueued) {
      return 'Finding match...';
    }

    return 'Joining game...';
  })();

  const handleCancel = () => {
    if (!showCancel) {
      return;
    }
    leaveQueue();
  };

  return (
    <div className="matchmaking-banner fixed top-20 left-0 right-0 z-50 flex justify-center px-4 pointer-events-none">
      <div className="flex items-center gap-3 px-4 py-2 rounded-lg bg-white/95 border-2 border-gray-300 text-xs font-bold uppercase tracking-1 text-gray-600 pointer-events-auto">
        <span className="w-4 h-4 border-2 border-gray-300 border-t-black rounded-full animate-spin" aria-hidden="true" />
        <span>{statusText}</span>
        {showCancel && (
          <button
            type="button"
            onClick={handleCancel}
            className="ml-2 px-3 py-1 text-[10px] font-bold uppercase tracking-1 rounded border border-gray-300 text-gray-600 hover:border-blue-500 hover:bg-blue-50 transition-colors"
          >
            Cancel
          </button>
        )}
      </div>
    </div>
  );
};
