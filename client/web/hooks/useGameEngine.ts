import { useEffect, useRef, useState, useCallback } from 'react';
import { GameClient } from 'wasm-snaketron';
import { GameState, GameCommand, Command } from '../types';
import { getClockDrift } from '../utils/clockSync';

interface UseGameEngineProps {
  gameId: string;
  playerId?: number;
  initialState?: GameState;
  onCommandReady?: (commandMessage: any) => void;
  latencyMs?: number;
}

interface UseGameEngineReturn {
  gameEngine: GameClient | null;
  gameState: GameState | null;
  isRunning: boolean;
  sendCommand: (command: Command) => void;
  processServerEvent: (event: any) => void;
  startEngine: () => void;
  stopEngine: () => void;
}

// Convert TypeScript GameStatus to Rust GameStatus format
const convertGameStatus = (status: any): any => {
  if (status.Ended) {
    return { Complete: { winning_snake_id: null } };
  } else if (status.Waiting) {
    return 'Stopped';
  }
  return status;
};

// Convert game state from TypeScript format to Rust format
const convertGameState = (state: GameState): any => {
  return {
    ...state,
    status: convertGameStatus(state.status)
  };
};

export const useGameEngine = ({
  gameId,
  playerId,
  initialState,
  onCommandReady,
  latencyMs = 0
}: UseGameEngineProps): UseGameEngineReturn => {
  const engineRef = useRef<GameClient | null>(null);
  const animationFrameRef = useRef<number | null>(null);
  const isRunningRef = useRef(false);
  const [gameState, setGameState] = useState<GameState | null>(initialState || null);
  const [isRunning, setIsRunning] = useState(false);
  const initialStateRef = useRef<GameState | undefined>(initialState);
  const engineGameIdRef = useRef<string | null>(null);
  const latencyMsRef = useRef(latencyMs);

  // Update latency ref when it changes
  useEffect(() => {
    latencyMsRef.current = latencyMs;
  }, [latencyMs]);

  // Game loop - no dependencies to avoid recreation
  const runGameLoop = useCallback(() => {
    // Check if we should continue running
    if (!engineRef.current || !isRunningRef.current) {
      console.log('Game loop skipped - engine:', !!engineRef.current, 'isRunning:', isRunningRef.current);
      return;
    }

    try {
      // Apply clock drift compensation to get server-synchronized time
      const clockDrift = getClockDrift();
      const now = BigInt(Date.now() - Math.round(clockDrift));
      
      // Run engine until current time
      const lastTick = engineRef.current.getPredictedTick()
      engineRef.current.rebuildPredictedState(now);
      const currentTick = engineRef.current.getPredictedTick();

      if (currentTick !== lastTick) {
        console.log("predicted tick changed", lastTick, "→", currentTick);
        console.log(JSON.parse(engineRef.current.getGameStateJson()).arena.snakes[0]);

        // Get both committed and predicted states
        const committedStateJson = engineRef.current.getCommittedStateJson();
        const predictedStateJson = engineRef.current.getGameStateJson();
        const eventLogJson = engineRef.current.getEventLogJson();


        // Parse the states
        const committedState = JSON.parse(committedStateJson);
        const predictedState = JSON.parse(predictedStateJson);
        const eventLog = JSON.parse(eventLogJson);
        
        // Extract snake positions (deep clone)
        const committedSnakes = JSON.parse(JSON.stringify(
          committedState.arena.snakes.map((snake: any, idx: number) => ({
            index: idx,
            is_alive: snake.is_alive,
            direction: snake.direction,
            body: snake.body.slice(0, 5), // First 5 positions for brevity
            length: snake.body.length
          }))
        ));
        
        const predictedSnakes = JSON.parse(JSON.stringify(
          predictedState.arena.snakes.map((snake: any, idx: number) => ({
            index: idx,
            is_alive: snake.is_alive,
            direction: snake.direction,
            body: snake.body.slice(0, 5), // First 5 positions for brevity
            length: snake.body.length
          }))
        ));
        
        // Extract pending commands from event log
        const pendingCommands = JSON.parse(JSON.stringify(
          eventLog
            .filter((event: any) => event.event.CommandScheduled)
            .map((event: any) => ({
              tick: event.tick,
              user_id: event.user_id,
              command: event.event.CommandScheduled.command_message.command
            }))
        ));
        
        // Log the detailed state
        console.log(
          `${new Date().toISOString()} Game State Update\n` +
          `Tick: ${lastTick} → ${currentTick}\n` +
          `Now: ${(Number(now))} (clock drift: ${clockDrift} ms)\n` +
          `\n--- COMMITTED STATE (tick ${committedState.tick}, start_ms ${committedState.start_ms}) ---\n` +
          `Snakes: ${JSON.stringify(committedSnakes, null, 2)}\n` +
          `Command queue: ${JSON.stringify(committedState.command_queue, null, 2)}\n` +
          `\n--- PREDICTED STATE (tick ${predictedState.tick}, start_ms ${predictedState.start_ms}) ---\n` +
          `Snakes: ${JSON.stringify(predictedSnakes, null, 2)}\n` +
          `Command queue: ${JSON.stringify(predictedState.command_queue, null, 2)}\n` +
          `\n--- PENDING COMMANDS ---\n` +
          `${pendingCommands.length > 0 ? JSON.stringify(pendingCommands, null, 2) : 'None'}\n`
        );
      }

      // Update game state
      const stateJson = engineRef.current.getGameStateJson();
      const newState = JSON.parse(stateJson);
      setGameState(newState);
      
      // Stop the loop if game is ended
      if ('Ended' in newState.status) {
        console.log('Game ended, stopping game loop');
        isRunningRef.current = false;
        setIsRunning(false);
        return;
      }
      
      // Continue loop if still running
      if (isRunningRef.current) {
        animationFrameRef.current = requestAnimationFrame(runGameLoop);
      }
    } catch (error) {
      console.error('Game loop error:', error);
      isRunningRef.current = false;
      setIsRunning(false);
    }
  }, []); // No dependencies - uses refs instead

  // Update initial state ref when it changes
  useEffect(() => {
    if (initialState && !initialStateRef.current) {
      console.log('Setting initial state for the first time - tick:', initialState.tick, 'status:', initialState.status, 'start_ms:', initialState.start_ms);
      initialStateRef.current = initialState;
    }
  }, [initialState]);

  // Initialize game engine
  useEffect(() => {
    console.log('useGameEngine effect running - gameId:', gameId, 'playerId:', playerId);
    
    const initEngine = async () => {
      try {
        // Check if engine already exists for this game
        if (engineRef.current && engineGameIdRef.current === gameId) {
          console.log('Engine already initialized for game:', gameId);
          return;
        }

        // Wait for WASM to be initialized
        if (!window.wasm) {
          console.log('Waiting for WASM to initialize...');
          setTimeout(initEngine, 100);
          return;
        }

        // Wait for initial state if we don't have it yet
        if (!initialStateRef.current) {
          console.log('Waiting for initial state...');
          setTimeout(initEngine, 100);
          return;
        }

        let engine: GameClient;
        const state = initialStateRef.current;
        console.log('Creating engine from initial state - tick:', state.tick, 'status:', state.status, 'start_ms:', state.start_ms);
        const convertedState = convertGameState(state);
        
        // Use start_ms from the game state directly
        const startMs = BigInt(state.start_ms);
        
        console.log('Using start time from game state - startMs:', startMs);
        
        engine = window.wasm.GameClient.newFromState(
          parseInt(gameId),
          startMs,
          JSON.stringify(convertedState)
        );
        
        if (playerId !== undefined) {
          engine.setLocalPlayerId(playerId);
        }
        
        engineRef.current = engine;
        engineGameIdRef.current = gameId;
        console.log('Game engine initialized successfully for game:', gameId);
        
        // If we're already supposed to be running, start the game loop
        if (isRunningRef.current) {
          console.log('Engine initialized while running - starting game loop');
          runGameLoop();
        }
      } catch (error) {
        console.error('Failed to initialize game engine:', error);
      }
    };

    initEngine();

    return () => {
      console.log('useGameEngine cleanup - gameId:', gameId);
      if (animationFrameRef.current !== null) {
        cancelAnimationFrame(animationFrameRef.current);
        animationFrameRef.current = null;
      }
      // Don't clear the engine here - let the next effect run decide if it needs to reinitialize
    };
  }, [gameId, playerId, runGameLoop]);

  // Start/stop engine
  const startEngine = useCallback(() => {
    console.log('Starting game engine - engine exists:', !!engineRef.current);
    if (!isRunningRef.current) {
      isRunningRef.current = true;
      setIsRunning(true);
      // Start the game loop immediately if engine exists
      if (engineRef.current) {
        console.log('Engine exists, starting game loop immediately');
        runGameLoop();
      } else {
        console.log('Engine not ready yet, will start loop when initialized');
      }
    }
  }, [runGameLoop]);

  const stopEngine = useCallback(() => {
    console.log('Stopping game engine');
    isRunningRef.current = false;
    setIsRunning(false);
    if (animationFrameRef.current !== null) {
      cancelAnimationFrame(animationFrameRef.current);
      animationFrameRef.current = null;
    }
  }, []);

  // Send command with client-side prediction
  const sendCommand = useCallback((command: Command) => {
    console.log('sendCommand called with:', command, 'timestamp:', Date.now());
    
    if (!engineRef.current || playerId === undefined) {
      console.error('Cannot send command - engine:', !!engineRef.current, 'playerId:', playerId);
      return;
    }

    try {
      // For solo games, the player ID is typically the user's ID from auth
      // The snake ID is usually 0 for the first/only snake
      const snakeId = 0; // In solo games, there's typically only one snake with ID 0

      // Process command based on type
      let commandMessageJson: string;
       if (typeof command === 'object' && 'Turn' in command) {
        console.log('Processing turn command:', command.Turn.direction, 'at', Date.now());
        commandMessageJson = engineRef.current.processTurn(snakeId, command.Turn.direction);
        console.log('processTurn returned at', Date.now());
      } else if (command === 'Respawn') {
        console.error('Respawn command not implemented yet');
        return;
      } else {
        console.error('Unsupported command type:', command);
        return;
      }

      // Parse and send to server
      const commandMessage = JSON.parse(commandMessageJson);
      console.log('Command message from engine:', commandMessage, 'at', Date.now());

      onCommandReady?.(commandMessage);
      console.log('Command sent to server at', Date.now());
    } catch (error) {
      console.error('Failed to process command:', error);
    }
  }, [playerId, onCommandReady]);

  // Process server event for reconciliation
  const processServerEvent = useCallback((eventMessage: any) => {
    if (!engineRef.current) return;

    try {
      // Check if it's just an event or a full event message
      let fullEventMessage = eventMessage;
      
      // If we only received the event, we need to wrap it in a GameEventMessage
      // This handles backward compatibility
      if (!eventMessage.game_id && !eventMessage.tick && !eventMessage.event) {
        console.warn('Received bare event, wrapping in GameEventMessage structure');
        fullEventMessage = {
          game_id: parseInt(gameId || '0'),
          tick: 0, // The server should provide the tick
          user_id: null,
          event: eventMessage
        };
      }
      
      // Handle SoloGameEnded event specially
      const event = fullEventMessage.event || fullEventMessage;
      if (event.SoloGameEnded) {
        console.log('Processing SoloGameEnded event');
        // Update the local game state to mark it as ended
        setGameState(prev => prev ? {
          ...prev,
          status: { Complete: { winning_snake_id: null } }
        } : prev);
        // Stop the game loop
        isRunningRef.current = false;
        setIsRunning(false);
        if (animationFrameRef.current !== null) {
          cancelAnimationFrame(animationFrameRef.current);
          animationFrameRef.current = null;
        }
        return;
      }
      
      // Convert the event if it contains a Snapshot with game state
      if (event.Snapshot && event.Snapshot.game_state) {
        fullEventMessage = {
          ...fullEventMessage,
          event: {
            ...event,
            Snapshot: {
              game_state: convertGameState(event.Snapshot.game_state)
            }
          }
        };
      }
      
      // Apply clock drift compensation when processing server events
      const clockDrift = getClockDrift();
      const currentTs = BigInt(Date.now() - Math.round(clockDrift));
      engineRef.current.processServerEvent(JSON.stringify(fullEventMessage), currentTs);
    } catch (error) {
      console.error('Failed to process server event:', error);
    }
  }, [gameId]);

  return {
    gameEngine: engineRef.current,
    gameState,
    isRunning,
    sendCommand,
    processServerEvent,
    startEngine,
    stopEngine,
  };
};