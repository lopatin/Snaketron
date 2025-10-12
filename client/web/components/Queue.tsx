import React, { useEffect } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { useGameWebSocket } from '../hooks/useGameWebSocket';
import LoadingScreen from './LoadingScreen';
import { GameType } from '../types';

export default function Queue() {
  const location = useLocation();
  const navigate = useNavigate();
  const { queueForMatch, queueForMatchMulti, currentGameId } = useGameWebSocket();

  useEffect(() => {
    // Get the game type(s) from navigation state
    const state = location.state as {
      gameType?: GameType;
      gameTypes?: GameType[];
      autoQueue?: boolean
    } | null;

    if (!state || !state.autoQueue) {
      // If no state or not auto-queueing, navigate back to home
      navigate('/');
      return;
    }

    // Handle multiple game types
    if (state.gameTypes && state.gameTypes.length > 0) {
      queueForMatchMulti(state.gameTypes);
    } else if (state.gameType) {
      // Handle single game type (backward compatibility)
      queueForMatch(state.gameType);
    } else {
      // No game type provided, navigate back
      navigate('/');
    }
  }, [location.state, navigate, queueForMatch, queueForMatchMulti]);

  // When a game is found, we'll automatically navigate (handled by useGameWebSocket)
  useEffect(() => {
    if (currentGameId) {
      // Game found, navigation will happen automatically
      console.log('Game found, waiting for navigation...');
    }
  }, [currentGameId]);

  return (
    <LoadingScreen
      message="Finding Match..."
      submessage="Please wait while we find opponents"
      showCancelButton={true}
    />
  );
}