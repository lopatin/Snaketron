import React, { useCallback, useEffect, useRef } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { useGameWebSocket } from '../hooks/useGameWebSocket';
import { useWebSocket } from '../contexts/WebSocketContext';
import LoadingScreen from './LoadingScreen';
import { GameType } from '../types';

export default function Queue() {
  const location = useLocation();
  const navigate = useNavigate();
  const { queueForMatch, queueForMatchMulti, leaveQueue, currentGameId, isQueued } = useGameWebSocket();
  const { currentLobby } = useWebSocket();
  const hasQueuedRef = useRef(false);

  const state = location.state as {
    gameType?: GameType;
    gameTypes?: GameType[];
    autoQueue?: boolean;
    viewLobbyQueue?: boolean;
  } | null;

  const isViewOnlyQueue = Boolean(state?.viewLobbyQueue);
  const shouldAutoQueue = Boolean(state?.autoQueue);

  useEffect(() => {
    if (!state || (!shouldAutoQueue && !isViewOnlyQueue)) {
      navigate('/');
      return;
    }

    if (!shouldAutoQueue) {
      return;
    }

    let didQueue = false;

    // Handle multiple game types
    if (state.gameTypes && state.gameTypes.length > 0) {
      queueForMatchMulti(state.gameTypes);
      didQueue = true;
    } else if (state.gameType) {
      // Handle single game type (backward compatibility)
      queueForMatch(state.gameType);
      didQueue = true;
    } else {
      // No game type provided, navigate back
      navigate('/');
      return;
    }

    if (didQueue) {
      hasQueuedRef.current = true;
    }
  }, [state, shouldAutoQueue, isViewOnlyQueue, navigate, queueForMatch, queueForMatchMulti]);

  // When a game is found, we'll automatically navigate (handled by useGameWebSocket)
  useEffect(() => {
    if (currentGameId) {
      // Game found, navigation will happen automatically
      console.log('Game found, waiting for navigation...');
    }
  }, [currentGameId]);

  useEffect(() => {
    if (!isViewOnlyQueue) {
      return;
    }

    if (!currentLobby || currentLobby.state !== 'queued') {
      navigate('/');
    }
  }, [currentLobby?.state, isViewOnlyQueue, navigate]);

  useEffect(() => {
    if (!isQueued) {
      hasQueuedRef.current = false;
    }
  }, [isQueued]);

  useEffect(() => {
    return () => {
      if (!isViewOnlyQueue && hasQueuedRef.current) {
        leaveQueue();
        hasQueuedRef.current = false;
      }
    };
  }, [isViewOnlyQueue, leaveQueue]);

  const handleCancel = useCallback(() => {
    if (!isViewOnlyQueue && hasQueuedRef.current) {
      leaveQueue();
      hasQueuedRef.current = false;
    }
    navigate('/');
  }, [isViewOnlyQueue, leaveQueue, navigate]);

  return (
    <LoadingScreen
      message="Finding Match..."
      submessage="Please wait while we find opponents"
      showCancelButton={!isViewOnlyQueue}
      onCancel={handleCancel}
    />
  );
}
