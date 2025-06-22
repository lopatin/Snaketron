import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useWebSocket } from '../contexts/WebSocketContext';
import { GameState, GameType, GameCommand, Command, CustomGameSettings } from '../types';

interface UseGameWebSocketReturn {
  isConnected: boolean;
  gameState: GameState | null;
  currentGameId: string | null;
  customGameCode: string | null;
  isHost: boolean;
  lastGameEvent: any | null;
  createCustomGame: (settings: Partial<CustomGameSettings>) => void;
  createGame: (gameType: string) => void;
  createSoloGame: (mode: 'Classic' | 'Tactical') => void;
  joinCustomGame: (gameCode: string) => void;
  updateCustomGameSettings: (settings: Partial<CustomGameSettings>) => void;
  startCustomGame: () => void;
  spectateGame: (gameId: string, gameCode?: string | null) => void;
  sendGameCommand: (command: GameCommand) => void;
  sendCommand: (command: Command) => void;
  connected: boolean;
}

export const useGameWebSocket = (): UseGameWebSocketReturn => {
  const { isConnected, sendMessage, onMessage } = useWebSocket();
  const navigate = useNavigate();
  const [gameState, setGameState] = useState<GameState | null>(null);
  const [currentGameId, setCurrentGameId] = useState<string | null>(null);
  const [customGameCode, setCustomGameCode] = useState<string | null>(null);
  const [isHost, setIsHost] = useState(false);
  const [lastGameEvent, setLastGameEvent] = useState<any | null>(null);

  // Handle game-specific messages
  useEffect(() => {
    const unsubscribers: (() => void)[] = [];

    // Game events (including game state updates)
    unsubscribers.push(
      onMessage('GameEvent', (message: any) => {
        console.log('Received GameEvent message:', message);
        
        // The message structure might be different - let's handle it safely
        const eventData = message.GameEvent || message.data || message;
        
        if (!eventData) {
          console.error('Invalid GameEvent message structure:', message);
          return;
        }
        
        // Check if event property exists
        const event = eventData.event || eventData;
        
        if (!event) {
          console.error('No event data found in GameEvent:', eventData);
          return;
        }
        
        // Store the event for the game engine to process
        setLastGameEvent(event);
        
        // Handle different event types
        if (event.Snapshot) {
          // Full game state snapshot
          console.log('Received Snapshot:', event.Snapshot);
          setGameState(event.Snapshot.game_state);
        } else if (event.SoloGameEnded) {
          // Solo game ended
          console.log('Received SoloGameEnded event');
          setGameState(prev => prev ? {
            ...prev,
            status: { Ended: {} }
          } : prev);
        } else if (event.StatusUpdated) {
          // Update game status
          console.log('StatusUpdated event:', event.StatusUpdated);
          setGameState(prev => prev ? { ...prev, status: event.StatusUpdated.status } : prev);
        } else if (event.FoodSpawned) {
          // Add food to arena
          console.log('FoodSpawned event:', event.FoodSpawned);
          setGameState(prev => {
            if (!prev || !prev.arena) return prev;
            return {
              ...prev,
              arena: {
                ...prev.arena,
                food: [...(prev.arena.food || []), event.FoodSpawned.position]
              }
            };
          });
        } else if (event.FoodEaten) {
          // Remove food and grow snake
          console.log('FoodEaten event:', event.FoodEaten);
          setGameState(prev => {
            if (!prev || !prev.arena) return prev;
            return {
              ...prev,
              arena: {
                ...prev.arena,
                food: prev.arena.food.filter(f => 
                  f.x !== event.FoodEaten.position.x || 
                  f.y !== event.FoodEaten.position.y
                )
              }
            };
          });
        } else if (event.SnakeTurned || event.SnakeDied) {
          // These need full state updates from server
          console.log('Snake event (need full state):', event);
        } else if (event.CommandScheduled) {
          // CommandScheduled events need to be passed to the game engine
          console.log('CommandScheduled event:', event.CommandScheduled);
        } else {
          // Other events
          console.log('Unhandled game event:', event);
        }
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
        
        // Check if current game is already ended - don't reinitialize
        if (gameState && 'Ended' in gameState.status) {
          console.log('Current game is ended, not reinitializing for new game');
          return;
        }
        
        setCurrentGameId(message.data.game_id);
        
        // Initialize a basic game state since server doesn't send initial snapshot
        // This is a workaround - ideally server should send GameEvent::Snapshot
        const initialGameState: GameState = {
          tick: 0,
          status: { Started: { server_id: 1 } },
          arena: {
            width: 40,
            height: 40,
            snakes: [
              {
                body: [
                  { x: 20, y: 20 },  // head
                  { x: 16, y: 20 }   // tail (snake length 4)
                ],
                direction: "Right" as const,
                is_alive: true,
                food: 0
              }
            ],
            food: []
          },
          game_type: { Custom: { settings: { game_mode: 'Solo' } } } as GameType,
          properties: { available_food_target: 3 },
          players: {
            // Assume the authenticated user is player 0 with snake 0
            // This will be overridden when we get the actual game state
            0: { user_id: 0, snake_id: 0 }
          },
          game_id: String(message.data.game_id)
        };
        
        console.log('Setting initial game state:', initialGameState);
        setGameState(initialGameState);
        
        // Navigate to the game arena
        navigate(`/play/${message.data.game_id}`);
      })
    );

    // Access denied
    unsubscribers.push(
      onMessage('AccessDenied', (message: any) => {
        console.error('Access denied:', message.data.reason);
        // TODO: Show error to user
      })
    );

    // Cleanup
    return () => {
      unsubscribers.forEach(unsub => unsub());
    };
  }, [onMessage, navigate, gameState]);

  // Game actions
  const createCustomGame = (settings: Partial<CustomGameSettings>) => {
    sendMessage({
      CreateCustomGame: { settings }
    });
  };

  const joinCustomGame = (gameCode: string) => {
    sendMessage({
      JoinCustomGame: { game_code: gameCode }
    });
  };

  const updateCustomGameSettings = (settings: Partial<CustomGameSettings>) => {
    sendMessage({
      UpdateCustomGameSettings: { settings }
    });
  };

  const startCustomGame = () => {
    sendMessage('StartCustomGame');
  };

  const spectateGame = (gameId: string, gameCode?: string | null) => {
    sendMessage({
      SpectateGame: { game_id: gameId, game_code: gameCode || null }
    });
  };

  const sendGameCommand = (command: GameCommand) => {
    console.log('Sending game command (sendGameCommand):', command);
    sendMessage({
      GameCommand: command
    });
  };
  
  const sendCommand = (command: Command) => {
    console.log('Sending game command (sendCommand):', command);
    console.log('Current game ID:', currentGameId);
    console.log('Connected:', isConnected);
    
    // For solo games, we need to wrap the command in the proper format
    sendMessage({
      GameCommand: {
        command_id_client: {
          tick: 0, // Will be set by server
          user_id: 0, // Will be set by server
          sequence_number: 0
        },
        command_id_server: null,
        command
      }
    });
  };

  const createSoloGame = (mode: 'Classic' | 'Tactical') => {
    console.log('Sending CreateSoloGame message with mode:', mode);
    sendMessage({
      CreateSoloGame: { mode }
    });
  };

  // Create a quick match or competitive game
  const createGame = (gameType: string) => {
    console.log('Creating game:', gameType);
    
    // Handle solo games separately
    if (gameType.startsWith('solo-')) {
      const mode = gameType === 'solo-tactical' ? 'Tactical' : 'Classic';
      console.log('Detected solo game type:', gameType, 'mode:', mode);
      createSoloGame(mode);
      return;
    }
    
    // For multiplayer games, use custom game as a placeholder for now
    createCustomGame({
      max_players: gameType === 'duel' ? 2 : 8,
      tick_duration_ms: 100, // Normal speed
      arena_width: 40,      // Medium map size
      arena_height: 40
    });
  };

  return {
    isConnected,
    gameState,
    currentGameId,
    customGameCode,
    isHost,
    lastGameEvent,
    createCustomGame,
    createGame,
    createSoloGame,
    joinCustomGame,
    updateCustomGameSettings,
    startCustomGame,
    spectateGame,
    sendGameCommand,
    sendCommand,
    connected: isConnected,
  };
};