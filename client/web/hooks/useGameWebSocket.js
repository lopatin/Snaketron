import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useWebSocket } from '../contexts/WebSocketContext.jsx';

export const useGameWebSocket = () => {
  const { isConnected, sendMessage, onMessage } = useWebSocket();
  const navigate = useNavigate();
  const [gameState, setGameState] = useState(null);
  const [currentGameId, setCurrentGameId] = useState(null);
  const [customGameCode, setCustomGameCode] = useState(null);
  const [isHost, setIsHost] = useState(false);

  // Handle game-specific messages
  useEffect(() => {
    const unsubscribers = [];

    // Game events (including game state updates)
    unsubscribers.push(
      onMessage('GameEvent', (message) => {
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
        
        // Handle different event types
        if (event.Snapshot) {
          // Full game state snapshot
          console.log('Received Snapshot:', event.Snapshot);
          setGameState(event.Snapshot.game_state);
        } else if (event.SoloGameEnded) {
          // Solo game ended
          setGameState(prev => ({
            ...prev,
            game_ended: true,
            final_score: event.SoloGameEnded.score,
            duration: event.SoloGameEnded.duration
          }));
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
        } else {
          // Other events
          console.log('Unhandled game event:', event);
        }
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

    // Solo game created
    unsubscribers.push(
      onMessage('SoloGameCreated', (message) => {
        console.log('Received SoloGameCreated message:', message);
        setCurrentGameId(message.data.game_id);
        
        // Initialize a basic game state since server doesn't send initial snapshot
        // This is a workaround - ideally server should send GameEvent::Snapshot
        const initialGameState = {
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
                direction: "Right",
                is_alive: true,
                food: 0
              }
            ],
            food: []
          },
          game_type: { Custom: { settings: { game_mode: 'Solo' } } },
          properties: { available_food_target: 3 },
          players: {
            // Assume the authenticated user is player 0 with snake 0
            // This will be overridden when we get the actual game state
            0: { user_id: 0, snake_id: 0 }
          },
          game_id: message.data.game_id
        };
        
        console.log('Setting initial game state:', initialGameState);
        setGameState(initialGameState);
        
        // Navigate to the game arena
        navigate(`/play/${message.data.game_id}`);
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
    console.log('Sending game command (sendGameCommand):', command);
    sendMessage({
      GameCommand: command
    });
  };
  
  const sendCommand = (command) => {
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

  const createSoloGame = (mode) => {
    console.log('Sending CreateSoloGame message with mode:', mode);
    sendMessage({
      CreateSoloGame: { mode }
    });
  };

  // Create a quick match or competitive game
  const createGame = (gameType) => {
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