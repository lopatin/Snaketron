import { useEffect, useRef, useState, useCallback } from 'react';
import { GameClient } from 'wasm-snaketron';
import { GameState, GameCommand, Command } from '../types';
import { getClockDrift } from '../utils/clockSync';
import {useAuth} from "../contexts/AuthContext";

interface UseGameEngineProps {
  gameId: string;
  playerId: number;
  onCommandReady?: (commandMessage: any) => void;
  latencyMs?: number;
}

interface UseGameEngineReturn {
  gameEngine: GameClient | null;
  gameState: GameState | null;
  committedState: GameState | null;
  isGameComplete: boolean;
  sendCommand: (command: Command) => void;
  processServerEvent: (event: any) => void;
  stopEngine: () => void;
}


export const useGameEngine = ({
  gameId,
  playerId,
  onCommandReady,
  latencyMs = 0
}: UseGameEngineProps): UseGameEngineReturn => {
  const engineRef = useRef<GameClient | null>(null);
  const animationFrameRef = useRef<number | null>(null);
  const [gameState, setGameState] = useState<GameState | null>(null);
  const [committedState, setCommittedState] = useState<GameState | null>(null);
  const [isGameComplete, setIsGameComplete] = useState(false);
  const engineGameIdRef = useRef<string | null>(null);
  const latencyMsRef = useRef(latencyMs);

  // console.log('useGameEngine called (initial state:', !!initialState);

  // Update latency ref when it changes
  useEffect(() => {
    latencyMsRef.current = latencyMs;
  }, [latencyMs]);

  const runGameLoop = useCallback(() => {
    if (!engineRef.current) {
      console.log('Game loop skipped - engine:', !!engineRef.current);
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
        // console.log(
        //   `${new Date().toISOString()} Game State Update\n` +
        //   `Tick: ${lastTick} → ${currentTick}\n` +
        //   `Now: ${(Number(now))} (clock drift: ${clockDrift} ms)\n` +
        //   `\n--- COMMITTED STATE (tick ${committedState.tick}, start_ms ${committedState.start_ms}) ---\n` +
        //   `Snakes: ${JSON.stringify(committedSnakes, null, 2)}\n` +
        //   `Command queue: ${JSON.stringify(committedState.command_queue, null, 2)}\n` +
        //   `\n--- PREDICTED STATE (tick ${predictedState.tick}, start_ms ${predictedState.start_ms}) ---\n` +
        //   `Snakes: ${JSON.stringify(predictedSnakes, null, 2)}\n` +
        //   `Command queue: ${JSON.stringify(predictedState.command_queue, null, 2)}\n` +
        //   `\n--- PENDING COMMANDS ---\n` +
        //   `${pendingCommands.length > 0 ? JSON.stringify(pendingCommands, null, 2) : 'None'}\n`
        // );
      }

      // Update game state
      const stateJson = engineRef.current.getGameStateJson();
      const newState = JSON.parse(stateJson);
      setGameState(newState);

      // Check if COMMITTED state is complete (for game over UI)
      const committedStateJson = engineRef.current.getCommittedStateJson();
      const committedState = JSON.parse(committedStateJson);
      setCommittedState(committedState);
      if (typeof committedState.status === 'object' &&
          committedState.status !== null &&
          'Complete' in committedState.status) {
        if (!isGameComplete) {
          console.log('Committed state is complete, triggering game over UI');
          setIsGameComplete(true);
        }
      }

      // Stop the loop if game is complete
      // if (typeof newState.status === 'object' && newState.status !== null && 'Complete' in newState.status) {
      //   console.log('Game completed, stopping game loop');
      //   return;
      // }

      animationFrameRef.current = requestAnimationFrame(runGameLoop);
    } catch (error) {
      console.error('Game loop error:', error);
    }
  }, [isGameComplete]);


  // // Start/stop engine
  // const startEngine = useCallback(() => {
  //   console.log('Starting game engine - engine exists:', !!engineRef.current);
  //   if (!isRunningRef.current) {
  //     isRunningRef.current = true;
  //     setIsRunning(true);
  //     // Start the game loop immediately if engine exists
  //     if (engineRef.current) {
  //       console.log('Engine exists, starting game loop immediately');
  //       runGameLoop();
  //     } else {
  //       console.log('Engine not ready yet, will start loop when initialized');
  //     }
  //   }
  // }, [runGameLoop]);

  const stopEngine = useCallback(() => {
    console.log('Stopping game engine');
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
      // Look up the snake ID for the current player from the game state
      const snakeId = engineRef.current.getSnakeIdForUser(playerId);
      
      if (snakeId === undefined || snakeId === null) {
        console.error('Cannot find snake for player ID:', playerId);
        return;
      }

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
  const processServerEvent = useCallback(async (eventMessage: any) => {
    try {
      // Check if it's just an event or a full event message
      let fullEventMessage = eventMessage;
      
      // If we only received the event, we need to wrap it in a GameEventMessage
      // TODO: Is this necessary? Shouldn't the server always send full messages?
      if (!eventMessage.game_id && !eventMessage.tick && !eventMessage.event) {
        console.warn('Received bare event, wrapping in GameEventMessage structure');
        fullEventMessage = {
          game_id: parseInt(gameId || '0'),
          tick: 0, // The server should provide the tick
          user_id: null,
          event: eventMessage
        };
      }
      
      const event = fullEventMessage.event || fullEventMessage;

      // Ensure WASM runtime is ready before using the game client
      if (typeof window !== 'undefined') {
        if (!window.wasm || !window.wasm.GameClient) {
          if (window.wasmReady) {
            try {
              await window.wasmReady;
            } catch (initError) {
              console.error('WASM initialization failed, cannot process server event:', initError);
              return;
            }
          }
        }

        if (!window.wasm || !window.wasm.GameClient) {
          console.warn('WASM runtime unavailable, skipping server event processing');
          return;
        }
      } else {
        console.warn('Window object is not available; skipping server event');
        return;
      }

      if (event.Snapshot && event.Snapshot.game_state) {
        // Initialize the game engine
        if (!engineRef.current) {
          engineRef.current = window.wasm.GameClient.newFromState(
              parseInt(gameId),
              JSON.stringify(event.Snapshot.game_state)
          );
          engineRef.current.setLocalPlayerId(playerId);
          runGameLoop();
        }
      }

      if (engineRef.current) {
        console.log('Processing server event:', fullEventMessage);

        // // DEBUG: Log XPAwarded events specifically
        // if (fullEventMessage.event && 'XPAwarded' in fullEventMessage.event) {
        //   console.log('🎯 Received XPAwarded event:', fullEventMessage.event.XPAwarded);
        // }

        engineRef.current.processServerEvent(JSON.stringify(fullEventMessage));

        // DEBUG: Log state after processing XPAwarded
        // if (fullEventMessage.event && 'XPAwarded' in fullEventMessage.event) {
        //   const committedState = JSON.parse(engineRef.current.getCommittedStateJson());
        //   console.log('🎯 After processing XPAwarded, committed state player_xp:', committedState.player_xp);
        // }

        console.log('Current game status:', engineRef.current.getCommittedStateJson());
      } else {
        console.error('Game engine not initialized, cannot process server event:', fullEventMessage);
      }
    } catch (error) {
      console.error('Failed to process server event:', error);
    }
  }, [playerId, gameId, runGameLoop, setIsGameComplete]);

  return {
    gameEngine: engineRef.current,
    gameState,
    committedState,
    isGameComplete,
    sendCommand,
    processServerEvent,
    stopEngine,
  };
};
