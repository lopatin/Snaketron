import { useEffect, useState, useCallback, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { useWebSocket } from '../contexts/WebSocketContext';
import { useAuth } from '../contexts/AuthContext';
import { GameState, GameType, GameCommand, Command, CustomGameSettings, GameLoadFailure } from '../types';
import { DEFAULT_TICK_INTERVAL_MS } from '../constants';
import { INVALID_GAME_ID_REASON, parseU32GameId } from '../utils/gameId';
import {
  clearGameCommandOutbox,
  enqueueGameCommandV2,
  gameEventTerminatesCommandOutbox,
  gameLoadOutboxAction,
  recoveryOutcomesReadyForResend,
  reconcileGameCommandOutcomes,
  resolveGameCommandV2,
  takeGameCommandsDueForRetry,
} from '../services/gameCommandOutbox';

interface UseGameWebSocketReturn {
  isConnected: boolean;
  currentGameId: string | null;
  customGameCode: string | null;
  isHost: boolean;
  /** Bumped whenever new game events are queued; consumers drain with takeGameEvents. */
  gameEventSignal: number;
  /** Returns every queued game event in arrival order and empties the queue. */
  takeGameEvents: () => any[];
  gameLoadFailure: GameLoadFailure | null;
  awaitingGameSnapshotForId: string | null;
  isGameSnapshotSynchronized: boolean;
  isQueued: boolean;
  isJoiningGame: boolean;
  createCustomGame: (settings: Partial<CustomGameSettings>) => void;
  createGame: (gameType: string) => void;
  createSoloGame: () => void;
  queueForMatch: (gameType: GameType, queueMode?: 'Quickmatch' | 'Competitive') => void;
  queueForMatchMulti: (gameTypes: GameType[], queueMode?: 'Quickmatch' | 'Competitive') => void;
  leaveQueue: () => void;
  joinCustomGame: (gameCode: string) => void;
  joinGame: (gameId: string, gameCode?: string | null) => boolean;
  acknowledgeGameSnapshot: (gameId: number) => void;
  leaveGame: () => void;
  updateCustomGameSettings: (settings: Partial<CustomGameSettings>) => void;
  startCustomGame: () => void;
  spectateGame: (gameId: string, gameCode?: string | null) => void;
  sendGameCommand: (command: GameCommand) => void;
  sendRequestResync: (gameId: string) => void;
  connected: boolean;
}

export const useGameWebSocket = (): UseGameWebSocketReturn => {
  const {
    isConnected,
    isSessionAuthenticated,
    serverCapabilities,
    sendMessage,
    onMessage,
  } = useWebSocket();
  const { user } = useAuth();
  const navigate = useNavigate();
  const [currentGameId, setCurrentGameId] = useState<string | null>(null);
  const [customGameCode, setCustomGameCode] = useState<string | null>(null);
  const [isHost, setIsHost] = useState(false);
  // Game events are queued in a ref and drained by the consumer. Delivering
  // them through a single useState slot loses events when React batches
  // multiple WS frames into one commit (last write wins) — a crash tick's
  // SnakeDied/SnakeRespawned burst is exactly such a case, and a dropped
  // event forces a stream-gap resync. The state below is only a wake-up
  // signal; the queue itself is lossless and ordered.
  const gameEventQueueRef = useRef<any[]>([]);
  const [gameEventSignal, setGameEventSignal] = useState(0);
  const [gameLoadFailure, setGameLoadFailure] = useState<GameLoadFailure | null>(null);
  const [awaitingGameSnapshotForId, setAwaitingGameSnapshotForId] = useState<string | null>(null);
  const [isGameSnapshotSynchronized, setIsGameSnapshotSynchronized] = useState(false);
  const [isQueued, setIsQueued] = useState(false);
  const [isJoiningGame, setIsJoiningGame] = useState(false);
  const requestedGameRef = useRef<{ routeGameId: string; gameId: number } | null>(null);
  const serverAssignedGameRef = useRef<number | null>(null);
  const awaitingGameSnapshotRef = useRef<string | null>(null);
  const isGameSnapshotSynchronizedRef = useRef(false);
  const completedOutcomeBarriersRef = useRef<Set<number>>(new Set());

  const updateAwaitingGameSnapshot = useCallback((routeGameId: string | null) => {
    awaitingGameSnapshotRef.current = routeGameId;
    setAwaitingGameSnapshotForId(routeGameId);
  }, []);

  const updateGameSnapshotSynchronization = useCallback((isSynchronized: boolean) => {
    isGameSnapshotSynchronizedRef.current = isSynchronized;
    setIsGameSnapshotSynchronized(isSynchronized);
  }, []);

  const takeGameEvents = useCallback(() => {
    const events = gameEventQueueRef.current;
    gameEventQueueRef.current = [];
    return events;
  }, []);

  // Handle game-specific messages
  useEffect(() => {
    const unsubscribers: (() => void)[] = [];
    let warmingRetryTimeout: ReturnType<typeof setTimeout> | null = null;

    console.log('Creating game WebSocket listeners (initial state issue)');

    // Game events (including game state updates)
    unsubscribers.push(
      onMessage('GameEvent', (message: any) => {
        // The message contains the full GameEventMessage from the server
        const eventMessage = message.GameEvent || message.data || message;
        
        if (!eventMessage) {
          console.error('Invalid GameEvent message structure:', message);
          return;
        }

        const eventGameId = parseU32GameId(eventMessage.game_id);
        const event = eventMessage.event || eventMessage;

        if (event?.CommandScheduledV2?.command_id) {
          resolveGameCommandV2(event.CommandScheduledV2.command_id);
        } else if (event?.CommandRejected?.command_id) {
          resolveGameCommandV2(event.CommandRejected.command_id);
        }
        if (
          eventGameId !== null &&
          user &&
          gameEventTerminatesCommandOutbox(event)
        ) {
          clearGameCommandOutbox(eventGameId, user.id);
        }

        // useGameWebSocket is currently instantiated independently by the global
        // matchmaking banner and the game arena. The banner's instance sees the
        // server's JoinGame notification, but the arena's instance sends the
        // subsequent JoinGame request and acknowledges its Snapshot. Clear the
        // notification-side joining state here when this hook instance sees the
        // matching Snapshot, while keeping arena event processing correlated to
        // requestedGameRef below.
        if (
          eventGameId !== null &&
          serverAssignedGameRef.current === eventGameId &&
          event &&
          typeof event === 'object' &&
          'Snapshot' in event
        ) {
          serverAssignedGameRef.current = null;
          setIsQueued(false);
          setIsJoiningGame(false);
        }

        const requestedGame = requestedGameRef.current;

        if (
          !requestedGame ||
          eventGameId === null ||
          eventGameId !== requestedGame.gameId
        ) {
          console.warn(
            'Ignoring GameEvent for a game other than the active request:',
            eventGameId,
            'requested:',
            requestedGame?.gameId ?? null
          );
          return;
        }
        
        // Queue the full event message (including tick) for the game engine
        // to process, and wake the consumer. Never deliver via a state slot
        // directly: coalesced commits would silently drop events.
        gameEventQueueRef.current.push(eventMessage);
        setGameEventSignal((n) => n + 1);
        
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

    // A game may have existed but no longer have a loadable live or persisted snapshot.
    unsubscribers.push(
      onMessage('GameLoadFailed', (message: any) => {
        const payload = message?.data;
        const gameId = parseU32GameId(payload?.game_id);

        if (gameId === null) {
          console.error('Invalid GameLoadFailed message:', message);
          return;
        }

        const requestedGame = requestedGameRef.current;
        if (
          !requestedGame ||
          gameLoadOutboxAction(
            'GameLoadFailed',
            gameId,
            requestedGame.gameId,
          ) !== 'clear-terminal'
        ) {
          console.warn(
            'Ignoring GameLoadFailed for a game other than the active request:',
            gameId,
            'requested:',
            requestedGame?.gameId ?? null,
          );
          return;
        }

        if (user) {
          clearGameCommandOutbox(gameId, user.id);
        }
        if (serverAssignedGameRef.current === gameId) {
          serverAssignedGameRef.current = null;
          setIsQueued(false);
        }

        const reason =
          typeof payload?.reason === 'string' && payload.reason.trim()
            ? payload.reason.trim()
            : 'This game is no longer available. It may have expired or been removed.';

        console.warn(`Failed to load game ${gameId}: ${reason}`);
        updateGameSnapshotSynchronization(false);
        setIsJoiningGame(false);
        updateAwaitingGameSnapshot(null);
        setGameLoadFailure({
          gameId,
          requestedGameId: requestedGame.routeGameId,
          reason,
        });
      })
    );

    unsubscribers.push(
      onMessage('GameWarming', (message: any) => {
        const payload = message?.data;
        const gameId = parseU32GameId(payload?.game_id);
        if (gameId === null) {
          return;
        }

        const requestedGame = requestedGameRef.current;
        if (!requestedGame) {
          // Authentication recovered a durably committed match before this
          // gateway's replica was warm. Navigate first; GameArena will issue
          // the retry once the route is mounted.
          serverAssignedGameRef.current = gameId;
          setCurrentGameId(gameId.toString());
          setIsQueued(false);
          setIsJoiningGame(true);
          navigate(`/play/${gameId}`);
          return;
        }
        if (
          gameLoadOutboxAction('GameWarming', gameId, requestedGame.gameId) !==
          'preserve-and-retry'
        ) {
          return;
        }

        updateGameSnapshotSynchronization(false);
        updateAwaitingGameSnapshot(requestedGame.routeGameId);
        setGameLoadFailure(null);
        setIsJoiningGame(true);
        if (warmingRetryTimeout) {
          clearTimeout(warmingRetryTimeout);
        }
        const retryAfterMs = Math.max(
          100,
          Math.min(2000, Number(payload?.retry_after_ms) || 500),
        );
        warmingRetryTimeout = setTimeout(() => {
          if (requestedGameRef.current?.gameId === gameId) {
            sendMessage({ JoinGame: gameId });
          }
        }, retryAfterMs);
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

        const parsedGameId = parseU32GameId(gameId);
        if (parsedGameId !== null) {
          console.log('Match found! Game ID:', parsedGameId);
          serverAssignedGameRef.current = parsedGameId;
          setCurrentGameId(parsedGameId.toString());
          setIsQueued(false);
          setIsJoiningGame(true);
          
          // DON'T send JoinGame back - GameArena will handle joining
          // sendMessage({ JoinGame: parseInt(gameId.toString()) });
          
          // Navigate to the game arena
          navigate(`/play/${parsedGameId}`);
        }
      })
    );

    // Access denied
    unsubscribers.push(
      onMessage('AccessDenied', (message: any) => {
        console.error('Access denied:', message.data.reason);
        serverAssignedGameRef.current = null;
        setIsQueued(false);
        setIsJoiningGame(false);
        // TODO: Show error to user
      })
    );

    unsubscribers.push(
      onMessage('QueueLeft', () => {
        console.log('Received QueueLeft message from server, clearing queue state');
        serverAssignedGameRef.current = null;
        setIsQueued(false);
        setIsJoiningGame(false);
      })
    );

    unsubscribers.push(
      onMessage('CommandOutcomes', (message: any) => {
        const payload = message?.data ?? message?.CommandOutcomes ?? message;
        if (user && parseU32GameId(payload?.game_id) !== null) {
          reconcileGameCommandOutcomes(payload, user.id);
        }
      })
    );

    unsubscribers.push(
      onMessage('CommandOutcomesComplete', (message: any) => {
        const payload = message?.data ?? message?.CommandOutcomesComplete ?? message;
        const gameId = parseU32GameId(payload?.game_id);
        if (gameId === null) {
          return;
        }
        completedOutcomeBarriersRef.current.add(gameId);
        if (
          user &&
          requestedGameRef.current?.gameId === gameId &&
          recoveryOutcomesReadyForResend(
            gameId,
            isGameSnapshotSynchronizedRef.current,
            serverCapabilities,
            completedOutcomeBarriersRef.current,
          )
        ) {
          for (const command of takeGameCommandsDueForRetry(gameId, user.id, Date.now(), 0)) {
            sendMessage({ GameCommandV2: command });
          }
        }
      })
    );

    // Cleanup
    return () => {
      console.log('Cleaning up game WebSocket listeners (initial state issue)');
      if (warmingRetryTimeout) {
        clearTimeout(warmingRetryTimeout);
      }
      unsubscribers.forEach(unsub => unsub());
    };
  }, [
    onMessage,
    navigate,
    sendMessage,
    serverCapabilities,
    updateAwaitingGameSnapshot,
    updateGameSnapshotSynchronization,
    user,
  ]);

  useEffect(() => {
    const requestedGame = requestedGameRef.current;
    if (requestedGame && (!isConnected || !isSessionAuthenticated)) {
      completedOutcomeBarriersRef.current.delete(requestedGame.gameId);
      updateGameSnapshotSynchronization(false);
      updateAwaitingGameSnapshot(requestedGame.routeGameId);
      setIsJoiningGame(true);
    }
  }, [
    isConnected,
    isSessionAuthenticated,
    updateAwaitingGameSnapshot,
    updateGameSnapshotSynchronization,
  ]);

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
    const parsedGameId = parseU32GameId(gameId);

    if (parsedGameId === null) {
      requestedGameRef.current = null;
      updateGameSnapshotSynchronization(false);
      gameEventQueueRef.current = [];
      updateAwaitingGameSnapshot(null);
      setIsJoiningGame(false);
      setGameLoadFailure({
        gameId: null,
        requestedGameId: gameId,
        reason: INVALID_GAME_ID_REASON,
      });
      return false;
    }

    requestedGameRef.current = {
      routeGameId: gameId,
      gameId: parsedGameId,
    };
    updateGameSnapshotSynchronization(false);
    completedOutcomeBarriersRef.current.delete(parsedGameId);
    setCurrentGameId(parsedGameId.toString());
    gameEventQueueRef.current = [];
    setGameLoadFailure(null);
    updateAwaitingGameSnapshot(gameId);
    setIsJoiningGame(true);
    sendMessage({
      JoinGame: parsedGameId
    });
    return true;
  }, [
    sendMessage,
    updateAwaitingGameSnapshot,
    updateGameSnapshotSynchronization,
  ]);

  const acknowledgeGameSnapshot = useCallback((gameId: number) => {
    const requestedGame = requestedGameRef.current;
    if (!requestedGame || requestedGame.gameId !== gameId) {
      return;
    }

    updateGameSnapshotSynchronization(true);
    updateAwaitingGameSnapshot(null);
    setGameLoadFailure(null);
    setIsJoiningGame(false);
    if (
      !user ||
      !serverCapabilities.has('command-delivery-v2') ||
      !recoveryOutcomesReadyForResend(
        gameId,
        isGameSnapshotSynchronizedRef.current,
        serverCapabilities,
        completedOutcomeBarriersRef.current,
      )
    ) {
      return;
    }
    for (const command of takeGameCommandsDueForRetry(gameId, user.id, Date.now(), 0)) {
      sendMessage({ GameCommandV2: command });
    }
  }, [sendMessage, serverCapabilities, updateAwaitingGameSnapshot, updateGameSnapshotSynchronization, user]);

  // A semantic executor result is the acknowledgement. Periodic exact resends
  // cover an ambiguous gateway/XADD failure without adding a weaker receipt
  // acknowledgement. The outbox atomically claims due entries, so the two
  // existing hook instances cannot create duplicate retry loops.
  useEffect(() => {
    const gameId = currentGameId === null ? null : parseU32GameId(currentGameId);
    if (
      gameId === null ||
      !user ||
      !isConnected ||
      !isSessionAuthenticated ||
      !serverCapabilities.has('command-delivery-v2')
    ) {
      return;
    }
    const timer = window.setInterval(() => {
      if (
        awaitingGameSnapshotRef.current !== null ||
        !recoveryOutcomesReadyForResend(
          gameId,
          isGameSnapshotSynchronizedRef.current,
          serverCapabilities,
          completedOutcomeBarriersRef.current,
        )
      ) {
        return;
      }
      for (const command of takeGameCommandsDueForRetry(gameId, user.id, Date.now(), 1_000)) {
        sendMessage({ GameCommandV2: command });
      }
    }, 250);
    return () => window.clearInterval(timer);
  }, [currentGameId, isConnected, isSessionAuthenticated, sendMessage, serverCapabilities, user]);

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
    if (
      !isConnected ||
      !isSessionAuthenticated ||
      awaitingGameSnapshotRef.current !== null ||
      !isGameSnapshotSynchronizedRef.current
    ) {
      console.warn('Ignoring game command while the game connection is not synchronized');
      return;
    }

    const gameId = requestedGameRef.current?.gameId;
    if (gameId === undefined) {
      console.warn('Ignoring game command without an active game identity');
      return;
    }
    if (!user) {
      console.warn('Ignoring game command without an authenticated user identity');
      return;
    }
    let stableCommand: ReturnType<typeof enqueueGameCommandV2>;
    try {
      stableCommand = enqueueGameCommandV2(gameId, user.id, command);
    } catch (error) {
      console.error('Cannot queue game command safely:', error);
      return;
    }
    console.log('Sending v2 game command:', stableCommand);
    sendMessage({ GameCommandV2: stableCommand });
  }, [isConnected, isSessionAuthenticated, sendMessage, serverCapabilities, user]);

  // Ask the game executor for a fresh snapshot when the engine detects
  // a stream gap / hash divergence, matching WSMessage::RequestResync
  const sendRequestResync = useCallback((gameId: string) => {
    const numericGameId = parseInt(gameId, 10);
    if (!Number.isFinite(numericGameId)) {
      console.error('Cannot request resync for invalid game ID:', gameId);
      return;
    }
    console.log('Sending RequestResync for game', numericGameId);
    sendMessage({
      RequestResync: { game_id: numericGameId }
    });
  }, [sendMessage]);

  const createSoloGame = useCallback(() => {
    console.log('Queueing for a solo game');
    serverAssignedGameRef.current = null;
    setIsQueued(true);
    setIsJoiningGame(false);
    sendMessage({
      QueueForMatch: { game_type: 'Solo', queue_mode: 'Quickmatch' }
    });
  }, [sendMessage]);

  const queueForMatch = useCallback((gameType: GameType, queueMode: 'Quickmatch' | 'Competitive' = 'Quickmatch') => {
    console.log('Queueing for match with game type:', gameType, 'mode:', queueMode);
    setIsQueued(true);
    setIsJoiningGame(false);
    sendMessage({
      QueueForMatch: { game_type: gameType, queue_mode: queueMode }
    });
  }, [sendMessage]);

  const queueForMatchMulti = useCallback((gameTypes: GameType[], queueMode: 'Quickmatch' | 'Competitive' = 'Quickmatch') => {
    console.log('Queueing for match with multiple game types:', gameTypes, 'mode:', queueMode);
    setIsQueued(true);
    setIsJoiningGame(false);
    sendMessage({
      QueueForMatchMulti: { game_types: gameTypes, queue_mode: queueMode }
    });
  }, [sendMessage]);

  const leaveQueue = useCallback(() => {
    console.log('Sending LeaveQueue message');
    sendMessage('LeaveQueue');
    serverAssignedGameRef.current = null;
    setIsQueued(false);
    setIsJoiningGame(false);
  }, [sendMessage]);

  const leaveGame = useCallback(() => {
    console.log('Sending LeaveGame message (initial state issue):');
    sendMessage('LeaveGame');
    // Clear current game state
    const leavingGameId = requestedGameRef.current?.gameId;
    requestedGameRef.current = null;
    if (leavingGameId !== undefined) {
      completedOutcomeBarriersRef.current.delete(leavingGameId);
    }
    if (leavingGameId !== undefined && user) {
      clearGameCommandOutbox(leavingGameId, user.id);
    }
    serverAssignedGameRef.current = null;
    updateGameSnapshotSynchronization(false);
    gameEventQueueRef.current = [];
    updateAwaitingGameSnapshot(null);
    setCurrentGameId(null);
    setIsHost(false);
    setCustomGameCode(null);
    setGameLoadFailure(null);
    setIsQueued(false);
    setIsJoiningGame(false);
  }, [
    sendMessage,
    updateAwaitingGameSnapshot,
    updateGameSnapshotSynchronization,
    user,
  ]);

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
      max_players: gameType === 'duel' ? 2 : 4,
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
    gameEventSignal,
    takeGameEvents,
    gameLoadFailure,
    awaitingGameSnapshotForId,
    isGameSnapshotSynchronized,
    isQueued,
    isJoiningGame,
    createCustomGame,
    createGame,
    createSoloGame,
    queueForMatch,
    queueForMatchMulti,
    leaveQueue,
    joinCustomGame,
    joinGame,
    acknowledgeGameSnapshot,
    leaveGame,
    updateCustomGameSettings,
    startCustomGame,
    spectateGame,
    sendGameCommand,
    sendRequestResync,
    connected: isConnected,
  };
};
