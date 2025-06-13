import { useEffect, useState } from 'react';
import { useWebSocket } from '../contexts/WebSocketContext.jsx';

export const useGameWebSocket = () => {
  const { isConnected, sendMessage, onMessage } = useWebSocket();
  const [gameState, setGameState] = useState(null);
  const [currentGameId, setCurrentGameId] = useState(null);
  const [customGameCode, setCustomGameCode] = useState(null);
  const [isHost, setIsHost] = useState(false);

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
        setIsHost(true); // Creator is always the host
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
      CreateCustomGame: { settings }
    });
  };

  const joinCustomGame = (gameCode) => {
    sendMessage({
      JoinCustomGame: { game_code: gameCode }
    });
  };

  const updateCustomGameSettings = (settings) => {
    sendMessage({
      UpdateCustomGameSettings: { settings }
    });
  };

  const startCustomGame = () => {
    sendMessage('StartCustomGame');
  };

  const spectateGame = (gameId, gameCode = null) => {
    sendMessage({
      SpectateGame: { game_id: gameId, game_code: gameCode }
    });
  };

  const sendGameCommand = (command) => {
    sendMessage({
      GameCommand: command
    });
  };

  // Create a quick match or competitive game
  const createGame = (gameType) => {
    // TODO: Implement actual game creation message
    console.log('Creating game:', gameType);
    // For now, we'll use custom game as a placeholder
    createCustomGame({
      max_players: gameType === 'duel' ? 2 : 8,
      game_speed: 'normal',
      map_size: 'medium'
    });
  };

  return {
    isConnected,
    gameState,
    currentGameId,
    customGameCode,
    isHost,
    createCustomGame,
    createGame,
    joinCustomGame,
    updateCustomGameSettings,
    startCustomGame,
    spectateGame,
    sendGameCommand,
  };
};