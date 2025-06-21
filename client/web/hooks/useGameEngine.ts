import { useEffect, useRef, useState, useCallback } from 'react';
import { GameClient } from 'wasm-snaketron';
import { GameState, GameCommand, Command } from '../types';

declare global {
  interface Window {
    wasm: any;
  }
}

interface UseGameEngineProps {
  gameId: string;
  playerId?: number;
  initialState?: GameState;
  onCommandReady?: (commandMessage: any) => void;
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
  onCommandReady
}: UseGameEngineProps): UseGameEngineReturn => {
  const engineRef = useRef<GameClient | null>(null);
  const animationFrameRef = useRef<number | null>(null);
  const isRunningRef = useRef(false);
  const [gameState, setGameState] = useState<GameState | null>(initialState || null);
  const [isRunning, setIsRunning] = useState(false);

  // Game loop - no dependencies to avoid recreation
  const runGameLoop = useCallback(() => {
    // Check if we should continue running
    if (!engineRef.current || !isRunningRef.current) {
      console.log('Game loop skipped - engine:', !!engineRef.current, 'isRunning:', isRunningRef.current);
      return;
    }

    try {
      const now = BigInt(Date.now());
      
      // Run engine until current time
      engineRef.current.runUntil(now);
      
      // Update game state
      const stateJson = engineRef.current.getGameStateJson();
      const newState = JSON.parse(stateJson);
      console.log('Game state updated - tick:', newState.tick, 'status:', newState.status);
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

  // Initialize game engine
  useEffect(() => {
    const initEngine = async () => {
      try {
        // Wait for WASM to be initialized
        if (!window.wasm) {
          console.log('Waiting for WASM to initialize...');
          setTimeout(initEngine, 100);
          return;
        }

        let engine: GameClient;
        if (initialState) {
          console.log('Creating engine from initial state - tick:', initialState.tick, 'status:', initialState.status);
          const convertedState = convertGameState(initialState);
          
          // Calculate start time based on current tick and tick duration
          const tickDuration = 
            (typeof initialState.game_type === 'object' && 'Custom' in initialState.game_type) 
              ? initialState.game_type.Custom.settings.tick_duration_ms 
              : 300;
          const currentMs = Date.now();
          const gameElapsedMs = initialState.tick * tickDuration;
          const startMs = BigInt(currentMs - gameElapsedMs);
          
          console.log('Calculated start time - currentMs:', currentMs, 'gameElapsedMs:', gameElapsedMs, 'startMs:', startMs);
          
          engine = window.wasm.GameClient.newFromState(
            parseInt(gameId),
            startMs,
            JSON.stringify(convertedState)
          );
        } else {
          console.log('Creating new engine without initial state');
          const startMs = BigInt(Date.now());
          engine = new window.wasm.GameClient(parseInt(gameId), startMs);
        }
        
        if (playerId !== undefined) {
          engine.setLocalPlayerId(playerId);
        }
        
        engineRef.current = engine;
        console.log('Game engine initialized successfully');
        
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
      if (animationFrameRef.current !== null) {
        cancelAnimationFrame(animationFrameRef.current);
      }
    };
  }, [gameId, playerId, initialState, runGameLoop]);

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
        commandMessageJson = engineRef.current.processTurn(snakeId, command.Turn.direction);
      } else if (command === 'Respawn') {
        console.error('Respawn command not implemented yet');
        return;
      } else {
        console.error('Unsupported command type:', command);
        return;
      }

      // Parse and send to server
      const commandMessage = JSON.parse(commandMessageJson);
      onCommandReady?.(commandMessage);
    } catch (error) {
      console.error('Failed to process command:', error);
    }
  }, [playerId, onCommandReady]);

  // Process server event for reconciliation
  const processServerEvent = useCallback((event: any) => {
    if (!engineRef.current) return;

    try {
      // Handle SoloGameEnded event specially
      if (event.SoloGameEnded) {
        console.log('Processing SoloGameEnded event');
        // Update the local game state to mark it as ended
        setGameState(prev => prev ? {
          ...prev,
          status: { Ended: {} }
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
      let convertedEvent = event;
      if (event.Snapshot && event.Snapshot.game_state) {
        convertedEvent = {
          ...event,
          Snapshot: {
            game_state: convertGameState(event.Snapshot.game_state)
          }
        };
      }
      engineRef.current.processServerEvent(JSON.stringify(convertedEvent));
    } catch (error) {
      console.error('Failed to process server event:', error);
    }
  }, []);

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