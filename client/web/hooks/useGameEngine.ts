import { useEffect, useRef, useState, useCallback } from 'react';
import { GameClient, getWasm, initWasm } from '../wasm';
import { GameState, GameCommandMessage, Command } from '../types';
import { getServerClockOffsetMs } from '../utils/clockSync';
import { parseU32GameId } from '../utils/gameId';
import { startTrace, record as recordTrace, autoUploadOnce } from '../utils/syncTrace';
import type { QueuedGameEvent } from './useGameWebSocket';

interface UseGameEngineProps {
  gameId: string;
  playerId: number;
  onCommandReady?: (commandMessage: GameCommandMessage) => boolean;
  onRequestResync?: () => void;
  latencyMs?: number;
}

interface UseGameEngineReturn {
  gameEngine: GameClient | null;
  gameState: GameState | null;
  committedState: GameState | null;
  isGameComplete: boolean;
  connectionStale: boolean;
  sendCommand: (command: Command) => boolean;
  processServerEvent: (event: QueuedGameEvent) => Promise<boolean>;
  /** Render predicted state with cosmetic layers below and above the snakes. */
  renderTo: (
    canvas: HTMLCanvasElement,
    cellSize: number,
    rotation: number,
    localUserId: number | undefined,
    drawCelebration: () => void,
    drawPostSnakes: () => void,
  ) => void;
  /** Read compact crash and goal history from the same predicted state used by renderTo. */
  readPredictedVisualState: () => {
    engineEpoch: number;
    baselineTick: number;
    json: string;
  } | null;
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
  const engineEpochRef = useRef(0);
  const engineBaselineTickRef = useRef(0);
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
  const staleStartedAtRef = useRef<number | null>(null);
  const lastResyncSentAtRef = useRef(0);
  const watchdogBackoffMsRef = useRef(WATCHDOG_BACKOFF_INITIAL_MS);
  const watchdogNextSendAtRef = useRef(0);
  const prevSyncStatusRef = useRef<any | null>(null);
  // Byte-identity caches for the engine's per-frame JSON exports. The engine
  // only changes on tick boundaries and server events, so most animation
  // frames serialize to the exact same string; skipping parse + setState on
  // those frames keeps React updates at tick cadence (~10/s) instead of rAF
  // cadence (~60/s).
  const lastGameStateJsonRef = useRef<string | null>(null);
  const lastCommittedStateJsonRef = useRef<string | null>(null);
  const lastSyncStatusJsonRef = useRef<string | null>(null);
  // Latest parsed committed state, for per-frame logic (liveness watchdog)
  // that must not wait for the next JSON change.
  const parsedCommittedStateRef = useRef<GameState | null>(null);

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
    engineEpochRef.current = 0;
    engineBaselineTickRef.current = 0;
    setGameState(null);
    setCommittedState(null);
    setIsGameComplete(false);
    setConnectionStale(false);
    staleRef.current = false;
    staleStartedAtRef.current = null;
    lastServerMsgAtRef.current = null;
    lastResyncSentAtRef.current = 0;
    watchdogBackoffMsRef.current = WATCHDOG_BACKOFF_INITIAL_MS;
    watchdogNextSendAtRef.current = 0;
    prevSyncStatusRef.current = null;
    lastGameStateJsonRef.current = null;
    lastCommittedStateJsonRef.current = null;
    lastSyncStatusJsonRef.current = null;
    parsedCommittedStateRef.current = null;
  }, [gameId]);

  const runGameLoop = useCallback(() => {
    if (!engineRef.current) {
      console.log('Game loop skipped - engine:', !!engineRef.current);
      return;
    }

    try {
      // The sync utility reports server-minus-client offset, so add it to the
      // browser clock. Before the first Pong, fall back to local wall time.
      const serverClockOffsetMs = getServerClockOffsetMs() ?? 0;
      const now = BigInt(Date.now() + Math.round(serverClockOffsetMs));
      
      // Run engine until current time. Keep this loop lean: main-thread
      // stalls delay WS message handling and can batch several server
      // events into one React commit.
      engineRef.current.rebuildPredictedState(now);

      // Update game state. Parse + setState only when the serialization
      // actually changed: a fresh JSON.parse every frame would hand React a
      // new object identity at rAF rate and re-render the arena tree (and
      // re-run every gameState-keyed effect) even though nothing moved.
      const stateJson = engineRef.current.getGameStateJson();
      if (stateJson !== lastGameStateJsonRef.current) {
        lastGameStateJsonRef.current = stateJson;
        setGameState(JSON.parse(stateJson));
      }

      // Check if COMMITTED state is complete (for game over UI)
      const committedStateJson = engineRef.current.getCommittedStateJson();
      if (committedStateJson !== lastCommittedStateJsonRef.current) {
        lastCommittedStateJsonRef.current = committedStateJson;
        const committedState = JSON.parse(committedStateJson);
        parsedCommittedStateRef.current = committedState;
        setCommittedState(committedState);
        if (typeof committedState.status === 'object' &&
            committedState.status !== null &&
            'Complete' in committedState.status) {
          if (!isGameComplete) {
            console.log('Committed state is complete, triggering game over UI');
            setIsGameComplete(true);
          }
        }
      }

      // Sync health: watch the engine's stream/hash accounting for gaps and
      // divergence, and drive the resync + liveness watchdog paths.
      try {
        const nowMs = Date.now();
        // The sync counters only move when a server message is processed, so
        // parse-on-change is safe for the delta detection below. The resync
        // debounce and the watchdog are wall-clock driven and must keep
        // running every frame — the watchdog fires precisely when messages
        // stop, i.e. when this JSON stops changing.
        const syncJson = engineRef.current.getSyncStatusJson();
        if (syncJson !== lastSyncStatusJsonRef.current) {
          lastSyncStatusJsonRef.current = syncJson;
          const sync = JSON.parse(syncJson);
          const prevSync = prevSyncStatusRef.current;

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
        }

        const latestSync = prevSyncStatusRef.current;
        if (latestSync?.needs_resync && nowMs - lastResyncSentAtRef.current >= RESYNC_DEBOUNCE_MS) {
          lastResyncSentAtRef.current = nowMs;
          onRequestResyncRef.current?.();
          engineRef.current.clearNeedsResync();
          recordTrace({ Note: { ts_ms: nowMs, note: 'resync requested (needs_resync)' } });
        }

        // Liveness watchdog: the engine's bounded prediction freezes the
        // simulation when server messages stop; surface that to the UI and
        // nudge the server for a fresh snapshot with exponential backoff.
        const committedStatus = parsedCommittedStateRef.current?.status;
        const isStarted =
          typeof committedStatus === 'object' &&
          committedStatus !== null &&
          'Started' in committedStatus;
        const lastMsgAt = lastServerMsgAtRef.current;

        if (isStarted && lastMsgAt !== null && nowMs - lastMsgAt > WATCHDOG_STALE_MS) {
          if (!staleRef.current) {
            staleRef.current = true;
            staleStartedAtRef.current = nowMs;
            setConnectionStale(true);
            watchdogBackoffMsRef.current = WATCHDOG_BACKOFF_INITIAL_MS;
            watchdogNextSendAtRef.current = nowMs;
            recordTrace({
              Note: {
                ts_ms: nowMs,
                note: `watchdog fired: no server message for ${nowMs - lastMsgAt}ms`
              }
            });
            recordTrace({
              Note: {
                ts_ms: nowMs,
                note: `ws_metric name=stale_overlay_activation count=1 no_message_ms=${Math.max(0, Math.min(5 * 60 * 1000, nowMs - lastMsgAt))}`
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

  // Render the engine's predicted state directly to a canvas. Reads the live
  // engineRef, so it always targets the current GameClient even after a
  // snapshot rebuild swaps the instance; no-ops until the engine exists.
  const renderTo = useCallback(
    (
      canvas: HTMLCanvasElement,
      cellSize: number,
      rotation: number,
      localUserId: number | undefined,
      drawCelebration: () => void,
      drawPostSnakes: () => void,
    ) => {
      engineRef.current?.render(
        canvas,
        cellSize,
        rotation,
        localUserId,
        drawCelebration,
        drawPostSnakes,
      );
    },
    [],
  );

  const readPredictedVisualState = useCallback(() => {
    const engine = engineRef.current;
    if (!engine) {
      return null;
    }
    return {
      engineEpoch: engineEpochRef.current,
      baselineTick: engineBaselineTickRef.current,
      json: engine.getPredictedVisualStateJson(),
    };
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
      return false;
    }

    let commandMessageJson: string | null = null;
    try {
      // Look up the snake ID for the current player from the game state
      const snakeId = engineRef.current.getSnakeIdForUser(playerId);
      
      if (snakeId === undefined || snakeId === null) {
        console.error('Cannot find snake for player ID:', playerId);
        return false;
      }

      // Process command based on type
      if (typeof command === 'object' && 'Turn' in command) {
        console.log('Processing turn command:', command.Turn.direction, 'at', Date.now());
        commandMessageJson = engineRef.current.processTurn(snakeId, command.Turn.direction);
        console.log('processTurn returned at', Date.now());
      } else if (command === 'ActivateBoost') {
        commandMessageJson = engineRef.current.processActivateBoost(snakeId);
      } else if (command === 'DeactivateBoost') {
        commandMessageJson = engineRef.current.processDeactivateBoost(snakeId);
      } else if (command === 'PlayerActivity') {
        commandMessageJson = engineRef.current.processPlayerActivity(snakeId);
      } else if (command === 'Respawn') {
        console.error('Respawn command not implemented yet');
        return false;
      } else {
        console.error('Unsupported command type:', command);
        return false;
      }

      // Parse and send to server. The command envelope contains only u32
      // fields (CommandId tick/user_id/sequence_number), so JSON.parse is
      // lossless here — unlike the inbound event path with its u64 hashes.
      if (!commandMessageJson) {
        console.error('WASM engine did not return a command envelope');
        return false;
      }
      const commandMessage: GameCommandMessage = JSON.parse(commandMessageJson);
      console.log('Command message from engine:', commandMessage, 'at', Date.now());

      const admitted = onCommandReady?.(commandMessage) ?? false;
      if (!admitted) {
        engineRef.current.discardLocalCommand(commandMessageJson);
        console.warn('Command prediction retracted because durable delivery admission failed');
        return false;
      }

      recordTrace({
        CmdOut: {
          ts_ms: Date.now(),
          predicted_tick: commandMessage?.command_id_client?.tick ?? 0,
          cmd: commandMessage
        }
      });

      console.log('Command sent to server at', Date.now());
      return true;
    } catch (error) {
      if (commandMessageJson && engineRef.current) {
        try {
          engineRef.current.discardLocalCommand(commandMessageJson);
        } catch (discardError) {
          console.error('Failed to retract unsent command prediction:', discardError);
        }
      }
      console.error('Failed to process command:', error);
      return false;
    }
  }, [playerId, onCommandReady]);

  // Process server event for reconciliation. `queued.message` is the
  // JS-parsed GameEventMessage envelope, used only for routing (game_id is
  // u32, the event kind is structural). `queued.raw` is the exact frame text,
  // handed to the engine so full-range u64 fields (e.g. TickHash.hash) are
  // parsed in Rust rather than corrupted by a JS JSON round-trip.
  const processServerEvent = useCallback(async (queued: QueuedGameEvent) => {
    try {
      const fullEventMessage = queued.message;
      const event = fullEventMessage.event;
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
      let wasm = getWasm();
      if (!wasm) {
        try {
          wasm = await initWasm();
        } catch (initError) {
          console.error('WASM initialization failed, cannot process server event:', initError);
          return false;
        }
      }

      const snapshotState = 'Snapshot' in event ? event.Snapshot.game_state : null;
      const isSnapshot = snapshotState !== null;
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

        // Rebuild from the raw frame so the snapshot's u64 fields (rng.state)
        // are parsed in Rust rather than corrupted by a JS JSON round-trip.
        engineRef.current = wasm.GameClient.newFromSnapshotFrame(expectedGameId, queued.raw);
        engineEpochRef.current += 1;
        engineBaselineTickRef.current = snapshotState.tick;
        engineRef.current.setLocalPlayerId(playerId);

        if (isFirstInit) {
          // Use the game's real tick duration (custom games can differ from
          // the default) so RCA clock-drift thresholds are computed correctly.
          startTrace(
            expectedGameId,
            playerId,
            snapshotState?.properties?.tick_duration_ms
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
        const nowMs = Date.now();
        staleRef.current = false;
        setConnectionStale(false);
        watchdogBackoffMsRef.current = WATCHDOG_BACKOFF_INITIAL_MS;
        if (staleStartedAtRef.current !== null) {
          recordTrace({
            Note: {
              ts_ms: nowMs,
              note: `ws_metric name=stale_overlay_duration_ms value=${Math.max(0, Math.min(5 * 60 * 1000, nowMs - staleStartedAtRef.current))}`
            }
          });
          staleStartedAtRef.current = null;
        }
        recordTrace({ Note: { ts_ms: nowMs, note: 'watchdog cleared: server messages resumed' } });
      }

      recordTrace({
        EventIn: {
          ts_ms: Date.now(),
          committed_tick: engineRef.current ? engineRef.current.getCommittedTick() : 0,
          msg: fullEventMessage
        }
      });

      if (engineRef.current) {
        // Rebuilding from `game_state` restores the authoritative state but
        // not the transport watermark. Feed the complete Snapshot envelope
        // through the engine as well so `stream_seq` is the baseline for the
        // very next delta (and a missing first delta cannot go undetected).
        // Forwarding the raw frame (rather than JSON.stringify(fullEventMessage))
        // keeps full-range u64 fields intact end-to-end.
        engineRef.current.processServerFrame(queued.raw);
        // Reconcile immediately, even when wall-clock time has not crossed a
        // tick boundary. The arena's next paint must see corrections (and
        // retract invalid crash effects) in the same visual frame.
        const serverClockOffsetMs = getServerClockOffsetMs() ?? 0;
        engineRef.current.rebuildPredictedState(
          BigInt(Date.now() + Math.round(serverClockOffsetMs)),
        );

        if (isSnapshot) {
          // Synchronize React state before the caller dismisses its awaiting-snapshot overlay.
          // This prevents a reconnect or retry from briefly revealing the stale pre-reconnect
          // arena between receipt and the next animation frame.
          const nextGameStateJson = engineRef.current.getGameStateJson();
          const nextCommittedStateJson = engineRef.current.getCommittedStateJson();
          const nextGameState = JSON.parse(nextGameStateJson);
          const nextCommittedState = JSON.parse(nextCommittedStateJson);
          // Keep the loop's byte-identity caches coherent with the state
          // pushed here, outside the loop.
          lastGameStateJsonRef.current = nextGameStateJson;
          lastCommittedStateJsonRef.current = nextCommittedStateJson;
          parsedCommittedStateRef.current = nextCommittedState;
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
    renderTo,
    readPredictedVisualState,
    stopEngine,
  };
};
