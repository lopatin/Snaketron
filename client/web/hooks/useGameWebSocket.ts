import { useEffect, useState, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useWebSocket } from '../contexts/WebSocketContext';
import { GameState, GameType, GameCommand, Command, CustomGameSettings } from '../types';
import { DEFAULT_TICK_INTERVAL_MS } from '../constants';

interface UseGameWebSocketReturn {
  isConnected: boolean;
  currentGameId: string | null;
  customGameCode: string | null;
  isHost: boolean;
  lastGameEvent: any | null;
  isQueued: boolean;
  createCustomGame: (settings: Partial<CustomGameSettings>) => void;
  createGame: (gameType: string) => void;
  createSoloGame: () => void;
  queueForMatch: (gameType: GameType) => void;
  joinCustomGame: (gameCode: string) => void;
  joinGame: (gameId: string, gameCode?: string | null) => void;
  leaveGame: () => void;
  updateCustomGameSettings: (settings: Partial<CustomGameSettings>) => void;
  startCustomGame: () => void;
  spectateGame: (gameId: string, gameCode?: string | null) => void;
  sendGameCommand: (command: GameCommand) => void;
  connected: boolean;
}

export const useGameWebSocket = (): UseGameWebSocketReturn => {
  const { isConnected, sendMessage, onMessage } = useWebSocket();
  const navigate = useNavigate();
  const [currentGameId, setCurrentGameId] = useState<string | null>(null);
  const [customGameCode, setCustomGameCode] = useState<string | null>(null);
  const [isHost, setIsHost] = useState(false);
  const [lastGameEvent, setLastGameEvent] = useState<any | null>(null);
  const [isQueued, setIsQueued] = useState(false);

  // Handle game-specific messages
  useEffect(() => {
    const unsubscribers: (() => void)[] = [];

    console.log('Creating game WebSocket listeners (initial state issue)');

    // Game events (including game state updates)
    unsubscribers.push(
      onMessage('GameEvent', (message: any) => {
        console.log('Received GameEvent message:', message);
        
        // The message contains the full GameEventMessage from the server
        const eventMessage = message.GameEvent || message.data || message;
        
        if (!eventMessage) {
          console.error('Invalid GameEvent message structure:', message);
          return;
        }
        
        // Store the full event message (including tick) for the game engine to process
        setLastGameEvent(eventMessage);
        
        // Also extract just the event for local state updates
        const event = eventMessage.event || eventMessage;
        
        // Handle different event types
        // if (event.Snapshot) {
        //   // Full game state snapshot
        //   console.log('Received Snapshot (initial state):', event.Snapshot);
        //   setGameState(event.Snapshot.game_state);
        // } else if (event.StatusUpdated) {
        //   // Update game status
        //   console.log('StatusUpdated event (initial state):', event.StatusUpdated);
        //   setGameState(prev => prev ? { ...prev, status: event.StatusUpdated.status } : prev);
        // } else if (event.FoodSpawned) {
        //   // Add food to arena
        //   console.log('FoodSpawned event:', event.FoodSpawned);
        //   setGameState(prev => {
        //     if (!prev || !prev.arena) return prev;
        //     return {
        //       ...prev,
        //       arena: {
        //         ...prev.arena,
        //         food: [...(prev.arena.food || []), event.FoodSpawned.position]
        //       }
        //     };
        //   });
        // } else if (event.FoodEaten) {
        //   // Remove food and grow snake
        //   console.log('FoodEaten event:', event.FoodEaten);
        //   setGameState(prev => {
        //     if (!prev || !prev.arena) return prev;
        //     return {
        //       ...prev,
        //       arena: {
        //         ...prev.arena,
        //         food: prev.arena.food.filter(f =>
        //           f.x !== event.FoodEaten.position.x ||
        //           f.y !== event.FoodEaten.position.y
        //         )
        //       }
        //     };
        //   });
        // } else if (event.SnakeTurned || event.SnakeDied) {
        //   // These need full state updates from server
        //   console.log('Snake event (need full state):', event);
        // } else if (event.CommandScheduled) {
        //   // CommandScheduled events need to be passed to the game engine
        //   console.log('CommandScheduled event:', event.CommandScheduled);
        // } else {
        //   // Other events
        //   console.log('Unhandled game event:', event);
        // }
      })
    );

    // Custom game created
    unsubscribers.push(
      onMessage('CustomGameCreated', (message: any) => {
        setCurrentGameId(message.data.game_id);
        setCustomGameCode(message.data.game_code);
        setIsHost(true); // Creator is always the host
      })
    );

    // Custom game joined
    unsubscribers.push(
      onMessage('CustomGameJoined', (message: any) => {
        setCurrentGameId(message.data.game_id);
      })
    );

    // Solo game created
    unsubscribers.push(
      onMessage('SoloGameCreated', (message: any) => {
        console.log('Received SoloGameCreated message:', message);
        
        const gameId = message.data.game_id;
        setCurrentGameId(gameId);
        
        // Navigate to the game arena
        navigate(`/play/${gameId}`);
      })
    );

    // JoinGame message from server (when match is found)
    unsubscribers.push(
      onMessage('JoinGame', (message: any) => {
        console.log('Received JoinGame message from server:', message);
        
        // Extract game ID - it could be directly the number or in a data field
        const gameId = typeof message === 'number' ? message : 
                      (message.data || message.JoinGame || message);
        
        if (gameId) {
          console.log('Match found! Game ID:', gameId);
          setCurrentGameId(gameId.toString());
          setIsQueued(false);
          
          // Acknowledge the join
          sendMessage({ JoinGame: parseInt(gameId.toString()) });
          
          // Navigate to the game arena
          navigate(`/play/${gameId}`);
        }
      })
    );

    // Access denied
    unsubscribers.push(
      onMessage('AccessDenied', (message: any) => {
        console.error('Access denied:', message.data.reason);
        setIsQueued(false);
        // TODO: Show error to user
      })
    );

    // Cleanup
    return () => {
      console.log('Cleaning up game WebSocket listeners (initial state issue)');
      unsubscribers.forEach(unsub => unsub());
    };
  }, [onMessage, navigate, sendMessage]);

  // Game actions
  const createCustomGame = useCallback((settings: Partial<CustomGameSettings>) => {
    sendMessage({
      CreateCustomGame: { settings }
    });
  }, [sendMessage]);

  const joinCustomGame = useCallback((gameCode: string) => {
    sendMessage({
      JoinCustomGame: { game_code: gameCode }
    });
  }, [sendMessage]);

  const joinGame = useCallback((gameId: string, gameCode?: string | null) => {
    // debugger;
    sendMessage({
      JoinGame: parseInt(gameId)
    });
  }, [sendMessage]);

  const updateCustomGameSettings = useCallback((settings: Partial<CustomGameSettings>) => {
    sendMessage({
      UpdateCustomGameSettings: { settings }
    });
  }, [sendMessage]);

  const startCustomGame = useCallback(() => {
    sendMessage('StartCustomGame');
  }, [sendMessage]);

  const spectateGame = useCallback((gameId: string, gameCode?: string | null) => {
    sendMessage({
      SpectateGame: { game_id: gameId, game_code: gameCode || null }
    });
  }, [sendMessage]);

  const sendGameCommand = useCallback((command: GameCommand) => {
    console.log('Sending game command (sendGameCommand):', command);
    sendMessage({
      GameCommand: command
    });
  }, [sendMessage]);

  const createSoloGame = useCallback(() => {
    console.log('Sending CreateSoloGame message');
    sendMessage('CreateSoloGame');
  }, [sendMessage]);

  const queueForMatch = useCallback((gameType: GameType) => {
    console.log('Queueing for match with game type:', gameType);
    setIsQueued(true);
    sendMessage({
      QueueForMatch: { game_type: gameType }
    });
  }, [sendMessage]);

  const leaveGame = useCallback(() => {
    console.log('Sending LeaveGame message (initial state issue):');
    sendMessage('LeaveGame');
    // Clear current game state
    setCurrentGameId(null);
    setIsHost(false);
    setCustomGameCode(null);
    setIsQueued(false);
  }, [sendMessage]);

  // Create a quick match or competitive game
  const createGame = useCallback((gameType: string) => {
    console.log('Creating game:', gameType);
    
    // Handle solo games separately
    if (gameType === 'solo') {
      console.log('Detected solo game type:', gameType);
      createSoloGame();
      return;
    }
    
    // For multiplayer games, use custom game as a placeholder for now
    createCustomGame({
      max_players: gameType === 'duel' ? 2 : 8,
      tick_duration_ms: DEFAULT_TICK_INTERVAL_MS as number, // Normal speed
      arena_width: 40,      // Medium map size
      arena_height: 40
    });
  }, [createSoloGame, createCustomGame]);

  return {
    isConnected,
    currentGameId,
    customGameCode,
    isHost,
    lastGameEvent,
    isQueued,
    createCustomGame,
    createGame,
    createSoloGame,
    queueForMatch,
    joinCustomGame,
    joinGame,
    leaveGame,
    updateCustomGameSettings,
    startCustomGame,
    spectateGame,
    sendGameCommand,
    connected: isConnected,
  };
};