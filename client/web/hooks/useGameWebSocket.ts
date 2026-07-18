import { useEffect, useState, useCallback, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { useWebSocket } from '../contexts/WebSocketContext';
import { GameState, GameType, GameCommand, Command, CustomGameSettings, GameLoadFailure } from '../types';
import { DEFAULT_TICK_INTERVAL_MS } from '../constants';
import { INVALID_GAME_ID_REASON, parseU32GameId } from '../utils/gameId';

interface UseGameWebSocketReturn {
  isConnected: boolean;
  currentGameId: string | null;
  customGameCode: string | null;
  isHost: boolean;
  lastGameEvent: any | null;
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
  connected: boolean;
}

export const useGameWebSocket = (): UseGameWebSocketReturn => {
  const { isConnected, isSessionAuthenticated, sendMessage, onMessage } = useWebSocket();
  const navigate = useNavigate();
  const [currentGameId, setCurrentGameId] = useState<string | null>(null);
  const [customGameCode, setCustomGameCode] = useState<string | null>(null);
  const [isHost, setIsHost] = useState(false);
  const [lastGameEvent, setLastGameEvent] = useState<any | null>(null);
  const [gameLoadFailure, setGameLoadFailure] = useState<GameLoadFailure | null>(null);
  const [awaitingGameSnapshotForId, setAwaitingGameSnapshotForId] = useState<string | null>(null);
  const [isGameSnapshotSynchronized, setIsGameSnapshotSynchronized] = useState(false);
  const [isQueued, setIsQueued] = useState(false);
  const [isJoiningGame, setIsJoiningGame] = useState(false);
  const requestedGameRef = useRef<{ routeGameId: string; gameId: number } | null>(null);
  const serverAssignedGameRef = useRef<number | null>(null);
  const awaitingGameSnapshotRef = useRef<string | null>(null);
  const isGameSnapshotSynchronizedRef = useRef(false);

  const updateAwaitingGameSnapshot = useCallback((routeGameId: string | null) => {
    awaitingGameSnapshotRef.current = routeGameId;
    setAwaitingGameSnapshotForId(routeGameId);
  }, []);

  const updateGameSnapshotSynchronization = useCallback((isSynchronized: boolean) => {
    isGameSnapshotSynchronizedRef.current = isSynchronized;
    setIsGameSnapshotSynchronized(isSynchronized);
  }, []);

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

        const eventGameId = parseU32GameId(eventMessage.game_id);
        const event = eventMessage.event || eventMessage;

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
        
        // Store the full event message (including tick) for the game engine to process
        setLastGameEvent(eventMessage);
        
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
        const payload = message?.data ?? message?.GameLoadFailed ?? message;
        const gameId = parseU32GameId(payload?.game_id);

        if (gameId === null) {
          console.error('Invalid GameLoadFailed message:', message);
          return;
        }

        if (serverAssignedGameRef.current === gameId) {
          serverAssignedGameRef.current = null;
          setIsQueued(false);
          setIsJoiningGame(false);
        }

        const requestedGame = requestedGameRef.current;
        if (!requestedGame || requestedGame.gameId !== gameId) {
          console.warn(
            'Ignoring GameLoadFailed for a game other than the active request:',
            gameId,
            'requested:',
            requestedGame?.gameId ?? null
          );
          return;
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

    // Cleanup
    return () => {
      console.log('Cleaning up game WebSocket listeners (initial state issue)');
      unsubscribers.forEach(unsub => unsub());
    };
  }, [
    onMessage,
    navigate,
    sendMessage,
    updateAwaitingGameSnapshot,
    updateGameSnapshotSynchronization,
  ]);

  useEffect(() => {
    const requestedGame = requestedGameRef.current;
    if (requestedGame && (!isConnected || !isSessionAuthenticated)) {
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
      setLastGameEvent(null);
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
    setCurrentGameId(parsedGameId.toString());
    setLastGameEvent(null);
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
  }, [updateAwaitingGameSnapshot, updateGameSnapshotSynchronization]);

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

    console.log('Sending game command (sendGameCommand):', command);
    sendMessage({
      GameCommand: command
    });
  }, [isConnected, isSessionAuthenticated, sendMessage]);

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
    requestedGameRef.current = null;
    serverAssignedGameRef.current = null;
    updateGameSnapshotSynchronization(false);
    setLastGameEvent(null);
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
    lastGameEvent,
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
    connected: isConnected,
  };
};
