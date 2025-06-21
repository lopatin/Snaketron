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

export const useGameEngine = ({
  gameId,
  playerId,
  initialState,
  onCommandReady
}: UseGameEngineProps): UseGameEngineReturn => {
  const engineRef = useRef<GameClient | null>(null);
  const animationFrameRef = useRef<number | null>(null);
  const [gameState, setGameState] = useState<GameState | null>(initialState || null);
  const [isRunning, setIsRunning] = useState(false);

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

        const startMs = BigInt(Date.now());
        
        let engine: GameClient;
        if (initialState) {
          engine = window.wasm.GameClient.newFromState(
            parseInt(gameId),
            startMs,
            JSON.stringify(initialState)
          );
        } else {
          engine = new window.wasm.GameClient(parseInt(gameId), startMs);
        }
        
        if (playerId !== undefined) {
          engine.setLocalPlayerId(playerId);
        }
        
        engineRef.current = engine;
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
  }, [gameId, playerId, initialState]);

  // Game loop
  const runGameLoop = useCallback(() => {
    if (!engineRef.current || !isRunning) return;

    try {
      const now = BigInt(Date.now());
      
      // Run engine until current time
      const eventsJson = engineRef.current.runUntil(now);
      const events = JSON.parse(eventsJson);
      
      // Update game state
      const stateJson = engineRef.current.getGameStateJson();
      const newState = JSON.parse(stateJson);
      setGameState(newState);
      
      // Continue loop
      animationFrameRef.current = requestAnimationFrame(runGameLoop);
    } catch (error) {
      console.error('Game loop error:', error);
      setIsRunning(false);
    }
  }, [isRunning]);

  // Start/stop engine
  const startEngine = useCallback(() => {
    setIsRunning(true);
  }, []);

  const stopEngine = useCallback(() => {
    setIsRunning(false);
    if (animationFrameRef.current !== null) {
      cancelAnimationFrame(animationFrameRef.current);
      animationFrameRef.current = null;
    }
  }, []);

  // Start game loop when running changes
  useEffect(() => {
    if (isRunning) {
      runGameLoop();
    }
  }, [isRunning, runGameLoop]);

  // Send command with client-side prediction
  const sendCommand = useCallback((command: Command) => {
    if (!engineRef.current || !playerId) return;

    try {
      // Get the snake ID for the player
      const currentState = gameState;
      const player = currentState?.players?.[playerId];
      if (!player) {
        console.error('Player not found in game state');
        return;
      }

      const snakeId = player.snake_id;
      
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
  }, [gameState, playerId, onCommandReady]);

  // Process server event for reconciliation
  const processServerEvent = useCallback((event: any) => {
    if (!engineRef.current) return;

    try {
      engineRef.current.processServerEvent(JSON.stringify(event));
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