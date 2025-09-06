import React, { useEffect } from 'react';
import { useLocation, useNavigate } from 'react-router-dom';
import { useGameWebSocket } from '../hooks/useGameWebSocket';
import LoadingScreen from './LoadingScreen';

export default function Queue() {
  const location = useLocation();
  const navigate = useNavigate();
  const { queueForMatch, createSoloGame, currentGameId } = useGameWebSocket();

  useEffect(() => {
    // Get the game type from navigation state
    const state = location.state as { gameType?: any; autoQueue?: boolean } | null;
    
    if (!state || !state.autoQueue || !state.gameType) {
      // If no state, navigate back to home
      navigate('/');
      return;
    }

    const gameType = state.gameType;

    // Queue for the appropriate game type
    if (gameType === 'Solo' || 
        (typeof gameType === 'object' && 'Custom' in gameType && 
         gameType.Custom?.settings?.game_mode === 'Solo')) {
      createSoloGame();
    } else if (typeof gameType === 'object') {
      queueForMatch(gameType);
    }
  }, [location.state, navigate, queueForMatch, createSoloGame]);

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