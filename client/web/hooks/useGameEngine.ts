import { useEffect, useRef, useState, useCallback } from 'react';
import { GameClient } from 'wasm-snaketron';
import { GameState, GameCommand, Command } from '../types';
import { getClockDrift } from '../utils/clockSync';
import { parseU32GameId } from '../utils/gameId';
import { startTrace, record as recordTrace, autoUploadOnce } from '../utils/syncTrace';

interface UseGameEngineProps {
  gameId: string;
  playerId: number;
  onCommandReady?: (commandMessage: any) => void;
  onRequestResync?: () => void;
  latencyMs?: number;
}

interface UseGameEngineReturn {
  gameEngine: GameClient | null;
  gameState: GameState | null;
  committedState: GameState | null;
  isGameComplete: boolean;
  connectionStale: boolean;
  sendCommand: (command: Command) => void;
  processServerEvent: (event: any) => Promise<boolean>;
  stopEngine: () => void;
}

// Liveness watchdog: no server message for this long while the game is
// running means the connection is effectively dead for gameplay purposes.
const WATCHDOG_STALE_MS = 3000;
// RequestResync pacing: needs_resync sends are debounced, watchdog sends
// back off exponentially while the connection stays stale.
const RESYNC_DEBOUNCE_MS = 2000;
const WATCHDOG_BACKOFF_INITIAL_MS = 2000;
const WATCHDOG_BACKOFF_MAX_MS = 10000;

export const useGameEngine = ({
  gameId,
  playerId,
  onCommandReady,
  onRequestResync,
  latencyMs = 0
}: UseGameEngineProps): UseGameEngineReturn => {
  const engineRef = useRef<GameClient | null>(null);
  const animationFrameRef = useRef<number | null>(null);
  const [gameState, setGameState] = useState<GameState | null>(null);
  const [committedState, setCommittedState] = useState<GameState | null>(null);
  const [isGameComplete, setIsGameComplete] = useState(false);
  const [connectionStale, setConnectionStale] = useState(false);
  const engineGameIdRef = useRef<string | null>(null);
  const latencyMsRef = useRef(latencyMs);
  const onRequestResyncRef = useRef(onRequestResync);
  const lastServerMsgAtRef = useRef<number | null>(null);
  const staleRef = useRef(false);
  const lastResyncSentAtRef = useRef(0);
  const watchdogBackoffMsRef = useRef(WATCHDOG_BACKOFF_INITIAL_MS);
  const watchdogNextSendAtRef = useRef(0);
  const prevSyncStatusRef = useRef<any | null>(null);

  // console.log('useGameEngine called (initial state:', !!initialState);

  // Update latency ref when it changes
  useEffect(() => {
    latencyMsRef.current = latencyMs;
  }, [latencyMs]);

  useEffect(() => {
    onRequestResyncRef.current = onRequestResync;
  }, [onRequestResync]);

  // Tear down the current engine whenever the game ID changes so we can initialize from the next snapshot
  useEffect(() => {
    if (engineGameIdRef.current === gameId) {
      return;
    }

    if (engineRef.current) {
      try {
        engineRef.current.free();
      } catch (error) {
        console.warn('Failed to free previous GameClient while switching games:', error);
      }
      engineRef.current = null;
    }

    if (animationFrameRef.current !== null) {
      cancelAnimationFrame(animationFrameRef.current);
      animationFrameRef.current = null;
    }

    engineGameIdRef.current = gameId;
    setGameState(null);
    setCommittedState(null);
    setIsGameComplete(false);
    setConnectionStale(false);
    staleRef.current = false;
    lastServerMsgAtRef.current = null;
    lastResyncSentAtRef.current = 0;
    watchdogBackoffMsRef.current = WATCHDOG_BACKOFF_INITIAL_MS;
    watchdogNextSendAtRef.current = 0;
    prevSyncStatusRef.current = null;
  }, [gameId]);

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

      // Sync health: watch the engine's stream/hash accounting for gaps and
      // divergence, and drive the resync + liveness watchdog paths.
      try {
        const sync = JSON.parse(engineRef.current.getSyncStatusJson());
        const prevSync = prevSyncStatusRef.current;
        const nowMs = Date.now();

        if (prevSync) {
          if (sync.stream_gap_count > prevSync.stream_gap_count) {
            recordTrace({
              Note: {
                ts_ms: nowMs,
                note: `stream gap detected: gaps=${sync.stream_gap_count} missed=${sync.missed_messages} last_seq=${sync.last_stream_seq}`
              }
            });
            autoUploadOnce('stream gap detected');
          }
          if (sync.total_mismatches > prevSync.total_mismatches) {
            recordTrace({
              Note: {
                ts_ms: nowMs,
                note: `hash mismatch at probe tick ${sync.last_probe_tick} (consecutive=${sync.consecutive_hash_mismatches}, total=${sync.total_mismatches})`
              }
            });
          }
          if (sync.consecutive_hash_mismatches >= 2 && prevSync.consecutive_hash_mismatches < 2) {
            autoUploadOnce('2+ consecutive hash mismatches');
          }
        }
        prevSyncStatusRef.current = sync;

        if (sync.needs_resync && nowMs - lastResyncSentAtRef.current >= RESYNC_DEBOUNCE_MS) {
          lastResyncSentAtRef.current = nowMs;
          onRequestResyncRef.current?.();
          engineRef.current.clearNeedsResync();
          recordTrace({ Note: { ts_ms: nowMs, note: 'resync requested (needs_resync)' } });
        }

        // Liveness watchdog: the engine's bounded prediction freezes the
        // simulation when server messages stop; surface that to the UI and
        // nudge the server for a fresh snapshot with exponential backoff.
        const committedStatus = committedState.status;
        const isStarted =
          typeof committedStatus === 'object' &&
          committedStatus !== null &&
          'Started' in committedStatus;
        const lastMsgAt = lastServerMsgAtRef.current;

        if (isStarted && lastMsgAt !== null && nowMs - lastMsgAt > WATCHDOG_STALE_MS) {
          if (!staleRef.current) {
            staleRef.current = true;
            setConnectionStale(true);
            watchdogBackoffMsRef.current = WATCHDOG_BACKOFF_INITIAL_MS;
            watchdogNextSendAtRef.current = nowMs;
            recordTrace({
              Note: {
                ts_ms: nowMs,
                note: `watchdog fired: no server message for ${nowMs - lastMsgAt}ms`
              }
            });
          }
          if (nowMs >= watchdogNextSendAtRef.current) {
            onRequestResyncRef.current?.();
            recordTrace({
              Note: {
                ts_ms: nowMs,
                note: `resync requested (watchdog, next backoff=${watchdogBackoffMsRef.current}ms)`
              }
            });
            watchdogNextSendAtRef.current = nowMs + watchdogBackoffMsRef.current;
            watchdogBackoffMsRef.current = Math.min(
              watchdogBackoffMsRef.current * 2,
              WATCHDOG_BACKOFF_MAX_MS
            );
          }
        }
      } catch (syncError) {
        console.warn('Sync health check failed:', syncError);
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

  const startEngine = useCallback(() => {
    if (!engineRef.current || animationFrameRef.current !== null) {
      return;
    }

    runGameLoop();
  }, [runGameLoop]);

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

      recordTrace({
        CmdOut: {
          ts_ms: Date.now(),
          predicted_tick: commandMessage?.command_id_client?.tick ?? 0,
          cmd: commandMessage
        }
      });

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
          game_id: parseU32GameId(gameId) ?? 0,
          tick: 0, // The server should provide the tick
          user_id: null,
          event: eventMessage
        };
      }
      
      const event = fullEventMessage.event || fullEventMessage;
      const expectedGameId = parseU32GameId(gameId);
      const messageGameId = parseU32GameId(fullEventMessage.game_id);

      if (
        messageGameId === null ||
        expectedGameId === null ||
        messageGameId !== expectedGameId
      ) {
        console.warn('Ignoring server event for previous game:', messageGameId, 'expected:', expectedGameId);
        return false;
      }

      // Ensure WASM runtime is ready before using the game client
      if (typeof window !== 'undefined') {
        if (!window.wasm || !window.wasm.GameClient) {
          if (window.wasmReady) {
            try {
              await window.wasmReady;
            } catch (initError) {
              console.error('WASM initialization failed, cannot process server event:', initError);
              return false;
            }
          }
        }

        if (!window.wasm || !window.wasm.GameClient) {
          console.warn('WASM runtime unavailable, skipping server event processing');
          return false;
        }
      } else {
        console.warn('Window object is not available; skipping server event');
        return false;
      }

      const isSnapshot = Boolean(event.Snapshot && event.Snapshot.game_state);
      if (isSnapshot) {
        // A reconnect Snapshot replaces both committed and predicted state. Applying it to an
        // existing client only replaces committed state, which can leave pre-disconnect
        // prediction visible, so rebuild the client from the authoritative snapshot instead.
        if (animationFrameRef.current !== null) {
          cancelAnimationFrame(animationFrameRef.current);
          animationFrameRef.current = null;
        }
        const isFirstInit = !engineRef.current;
        if (engineRef.current) {
          try {
            engineRef.current.free();
          } catch (error) {
            console.warn('Failed to free GameClient before applying Snapshot:', error);
          }
        }

        engineRef.current = window.wasm.GameClient.newFromState(
            expectedGameId,
            JSON.stringify(event.Snapshot.game_state)
        );
        engineRef.current.setLocalPlayerId(playerId);

        if (isFirstInit) {
          // Use the game's real tick duration (custom games can differ from
          // the default) so RCA clock-drift thresholds are computed correctly.
          startTrace(
            expectedGameId,
            playerId,
            event.Snapshot.game_state?.properties?.tick_duration_ms
          );
        } else {
          // The rebuilt engine starts with fresh sync counters; the snapshot
          // itself re-anchors the stream watermark.
          recordTrace({ Note: { ts_ms: Date.now(), note: 'engine rebuilt from snapshot (resync)' } });
        }
      }

      // Liveness: any accepted server message for this game proves the pipe is alive
      lastServerMsgAtRef.current = Date.now();
      if (staleRef.current) {
        staleRef.current = false;
        setConnectionStale(false);
        watchdogBackoffMsRef.current = WATCHDOG_BACKOFF_INITIAL_MS;
        recordTrace({ Note: { ts_ms: Date.now(), note: 'watchdog cleared: server messages resumed' } });
      }

      recordTrace({
        EventIn: {
          ts_ms: Date.now(),
          committed_tick: engineRef.current ? engineRef.current.getCommittedTick() : 0,
          msg: fullEventMessage
        }
      });

      if (engineRef.current) {
        console.log('Processing server event:', fullEventMessage);

        // // DEBUG: Log XPAwarded events specifically
        // if (fullEventMessage.event && 'XPAwarded' in fullEventMessage.event) {
        //   console.log('🎯 Received XPAwarded event:', fullEventMessage.event.XPAwarded);
        // }

        if (!isSnapshot) {
          engineRef.current.processServerEvent(JSON.stringify(fullEventMessage));
        }

        // DEBUG: Log state after processing XPAwarded
        // if (fullEventMessage.event && 'XPAwarded' in fullEventMessage.event) {
        //   const committedState = JSON.parse(engineRef.current.getCommittedStateJson());
        //   console.log('🎯 After processing XPAwarded, committed state player_xp:', committedState.player_xp);
        // }

        console.log('Current game status:', engineRef.current.getCommittedStateJson());

        if (isSnapshot) {
          // Synchronize React state before the caller dismisses its awaiting-snapshot overlay.
          // This prevents a reconnect or retry from briefly revealing the stale pre-reconnect
          // arena between receipt and the next animation frame.
          const nextGameState = JSON.parse(engineRef.current.getGameStateJson());
          const nextCommittedState = JSON.parse(engineRef.current.getCommittedStateJson());
          const snapshotIsComplete =
            typeof nextCommittedState.status === 'object' &&
            nextCommittedState.status !== null &&
            'Complete' in nextCommittedState.status;
          setGameState(nextGameState);
          setCommittedState(nextCommittedState);
          setIsGameComplete(snapshotIsComplete);
          if (!snapshotIsComplete) {
            startEngine();
          }
        }
      } else {
        console.error('Game engine not initialized, cannot process server event:', fullEventMessage);
        return false;
      }

      return true;
    } catch (error) {
      console.error('Failed to process server event:', error);
      return false;
    }
  }, [playerId, gameId, startEngine]);

  return {
    gameEngine: engineRef.current,
    gameState,
    committedState,
    isGameComplete,
    connectionStale,
    sendCommand,
    processServerEvent,
    stopEngine,
  };
};
