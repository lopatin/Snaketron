import { useEffect, useState } from 'react';
import { useWebSocket } from '../contexts/WebSocketContext.jsx';

export const useGameWebSocket = () => {
  const { isConnected, sendMessage, onMessage } = useWebSocket();
  const [gameState, setGameState] = useState(null);
  const [currentGameId, setCurrentGameId] = useState(null);
  const [customGameCode, setCustomGameCode] = useState(null);

  // Handle game-specific messages
  useEffect(() => {
    const unsubscribers = [];

    // Game state updates
    unsubscribers.push(
      onMessage('GameState', (message) => {
        setGameState(message.data);
      })
    );

    // Custom game created
    unsubscribers.push(
      onMessage('CustomGameCreated', (message) => {
        setCurrentGameId(message.data.game_id);
        setCustomGameCode(message.data.game_code);
      })
    );

    // Custom game joined
    unsubscribers.push(
      onMessage('CustomGameJoined', (message) => {
        setCurrentGameId(message.data.game_id);
      })
    );

    // Access denied
    unsubscribers.push(
      onMessage('AccessDenied', (message) => {
        console.error('Access denied:', message.data.reason);
        // TODO: Show error to user
      })
    );

    // Cleanup
    return () => {
      unsubscribers.forEach(unsub => unsub());
    };
  }, [onMessage]);

  // Game actions
  const createCustomGame = (settings) => {
    sendMessage({
      type: 'CreateCustomGame',
      data: { settings }
    });
  };

  const joinCustomGame = (gameCode) => {
    sendMessage({
      type: 'JoinCustomGame',
      data: { game_code: gameCode }
    });
  };

  const updateCustomGameSettings = (settings) => {
    sendMessage({
      type: 'UpdateCustomGameSettings',
      data: { settings }
    });
  };

  const startCustomGame = () => {
    sendMessage({
      type: 'StartCustomGame',
      data: {}
    });
  };

  const spectateGame = (gameId, gameCode = null) => {
    sendMessage({
      type: 'SpectateGame',
      data: { game_id: gameId, game_code: gameCode }
    });
  };

  const sendGameCommand = (command) => {
    sendMessage({
      type: 'GameCommand',
      data: command
    });
  };

  return {
    isConnected,
    gameState,
    currentGameId,
    customGameCode,
    createCustomGame,
    joinCustomGame,
    updateCustomGameSettings,
    startCustomGame,
    spectateGame,
    sendGameCommand,
  };
};