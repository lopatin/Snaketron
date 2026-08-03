import React, { useEffect, useRef, useState, useCallback, useReducer, useMemo } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useGameWebSocket } from '../hooks/useGameWebSocket';
import { useGameEngine } from '../hooks/useGameEngine';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import { GameState, CanvasRef, ArenaRotation, GameType, LobbyGameMode, QueueMode, GameLoadFailure } from '../types';
import { getWasm } from '../wasm';
import Scoreboard from './Scoreboard';
import LoadingScreen from './LoadingScreen';
import { LobbyChat as ChatPanel } from './LobbyChat';
import { INVALID_GAME_ID_REASON, parseU32GameId } from '../utils/gameId';
import {
  CRASH_EXPLOSION_SPRITE_URL,
  drawCrashExplosions,
  syncPredictedCrashExplosions,
} from '../utils/crashExplosion';
import type {
  CrashExplosion,
  PredictedCrashVisualState,
} from '../utils/crashExplosion';

export default function GameArena() {
  const { gameId } = useParams();
  if (!gameId) {
    throw new Error('GameArena must be used with a gameId parameter');
  }
  const routeGameId = parseU32GameId(gameId);

  const navigate = useNavigate();
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const joinedGameIdRef = useRef<string | null>(null);
  const previousGameIdRef = useRef<string | null>(null);
  const drainingGameEventsRef = useRef(false);
  const crashExplosionSpriteRef = useRef<HTMLImageElement | null>(null);
  const crashExplosionsRef = useRef<CrashExplosion[]>([]);
  const seenCrashEventIdsRef = useRef<Set<string>>(new Set());
  const crashVisualEpochRef = useRef<number | null>(null);
  const lastCrashVisualJsonRef = useRef<string | null>(null);
  const prefersReducedMotionRef = useRef(false);
  
  const {
    connected,
    currentGameId,
    sendGameCommand,
    joinGame,
    gameEventSignal,
    takeGameEvents,
    gameLoadFailure,
    awaitingGameSnapshotForId,
    isGameSnapshotSynchronized,
    acknowledgeGameSnapshot,
    leaveGame,
    queueForMatch,
    queueForMatchMulti,
    isJoiningGame,
    sendRequestResync,
  } = useGameWebSocket();

  const { user, loading: authLoading } = useAuth();
  const {
    latencyMs,
    gameChatMessages,
    sendChatMessage,
    currentLobby,
    lobbyPreferences,
    isSessionAuthenticated,
    createLobby,
  } = useWebSocket();
  const playerId = user?.id ?? 0;
  const queueMode: QueueMode = lobbyPreferences?.competitive ? 'Competitive' : 'Quickmatch';

  const handleRequestResync = useCallback(() => {
    sendRequestResync(gameId);
  }, [sendRequestResync, gameId]);

  // Use game engine for client-side prediction (call unconditionally to keep hook order stable)
  const {
    gameEngine,
    gameState,
    committedState,
    isGameComplete,
    connectionStale,
    // isRunning,
    sendCommand,
    processServerEvent,
    renderTo,
    readPredictedCrashVisualState,
    stopEngine
  } = useGameEngine({
    gameId,
    playerId,
    onCommandReady: sendGameCommand,
    onRequestResync: handleRequestResync,
    latencyMs
  });

  const [gameOver, setGameOver] = useState(false);
  const [showGameOverPanel, setShowGameOverPanel] = useState(false);
  const [cellSize, setCellSize] = useState(15);
  const [canvasSize, setCanvasSize] = useState({ width: 600, height: 600 });
  const [panelSize, setPanelSize] = useState({ width: 610, height: 610 });
  const [isArenaVisible, setIsArenaVisible] = useState(false);
  const [, forceUpdate] = useReducer(x => x + 1, 0);
  const [rotation, setRotation] = useState<ArenaRotation>(0);
  const rotationSetRef = useRef(false); // Track if rotation has been set
  const [isShortWideScreen, setIsShortWideScreen] = useState(false);

  // Reset local arena state when switching to a new game ID (route change without unmount)
  useEffect(() => {
    if (!gameId) {
      return;
    }

    if (previousGameIdRef.current && previousGameIdRef.current !== gameId) {
      console.log('Game ID changed, tearing down previous arena before joining new game:', previousGameIdRef.current, '→', gameId);
      leaveGame();
      stopEngine();
      rotationSetRef.current = false;
      setGameOver(false);
      setShowGameOverPanel(false);
      crashExplosionsRef.current.length = 0;
      seenCrashEventIdsRef.current.clear();
      crashVisualEpochRef.current = null;
      lastCrashVisualJsonRef.current = null;
    }

    previousGameIdRef.current = gameId;
    joinedGameIdRef.current = null;
  }, [gameId, leaveGame, stopEngine]);

  // Join only after the token has been sent on this WebSocket connection. This keeps
  // JoinGame ordered behind authentication during a cold page load or reconnect.
  useEffect(() => {
    if (!connected || !isSessionAuthenticated) {
      // A new socket has no server-side game subscription, even if this component
      // already joined on the previous connection.
      joinedGameIdRef.current = null;
      return;
    }

    if (
      user &&
      gameId &&
      routeGameId !== null &&
      joinedGameIdRef.current !== gameId
    ) {
      console.log('User and WebSocket session authenticated, joining game:', gameId);
      joinedGameIdRef.current = joinGame(gameId) ? gameId : null;
    }
  }, [user, gameId, routeGameId, connected, isSessionAuthenticated, joinGame]);

  const invalidRouteFailure = useMemo<GameLoadFailure | null>(() => (
    routeGameId === null
      ? {
          gameId: null,
          requestedGameId: gameId,
          reason: INVALID_GAME_ID_REASON,
        }
      : null
  ), [gameId, routeGameId]);
  const currentGameLoadFailure =
    invalidRouteFailure ??
    (gameLoadFailure && gameLoadFailure.requestedGameId === gameId
      ? gameLoadFailure
      : null);
  const isRequestForCurrentRoute =
    routeGameId !== null && currentGameId === routeGameId.toString();
  const isAwaitingCurrentSnapshot = awaitingGameSnapshotForId === gameId;
  const isGameInteractionActive =
    connected &&
    isSessionAuthenticated &&
    isRequestForCurrentRoute &&
    isGameSnapshotSynchronized &&
    !isAwaitingCurrentSnapshot &&
    !currentGameLoadFailure &&
    gameState !== null;

  useEffect(() => {
    if (currentGameLoadFailure) {
      // Allow an explicit retry or a future authenticated reconnect to issue JoinGame again.
      joinedGameIdRef.current = null;
    }
  }, [currentGameLoadFailure]);

  useEffect(() => {
    if (!isGameInteractionActive) {
      stopEngine();
    }
  }, [isGameInteractionActive, stopEngine]);

  // Preload and decode the atlas before gameplay starts. The arena countdown
  // gives this a generous head start, while the render loop safely tolerates a
  // cold cache without blocking simulation or input.
  useEffect(() => {
    const sprite = new Image();
    sprite.decoding = 'async';
    sprite.fetchPriority = 'high';
    sprite.src = CRASH_EXPLOSION_SPRITE_URL;
    crashExplosionSpriteRef.current = sprite;
    void sprite.decode().catch((error) => {
      console.warn('Crash explosion sprite could not be decoded:', error);
    });

    const motionPreference = window.matchMedia('(prefers-reduced-motion: reduce)');
    const updateMotionPreference = () => {
      prefersReducedMotionRef.current = motionPreference.matches;
    };
    updateMotionPreference();
    motionPreference.addEventListener('change', updateMotionPreference);

    return () => {
      motionPreference.removeEventListener('change', updateMotionPreference);
      crashExplosionSpriteRef.current = null;
      crashExplosionsRef.current.length = 0;
      seenCrashEventIdsRef.current.clear();
      crashVisualEpochRef.current = null;
      lastCrashVisualJsonRef.current = null;
    };
  }, []);


  useEffect(() => {
    // Trigger fade-in animation when component mounts
    const timer = setTimeout(() => {
      setIsArenaVisible(true);
    }, 300); // Delay to ensure smooth transition after fade-out

    console.log('GAME ARENA MOUNTED, initial state:', gameState);

    return () => {
      clearTimeout(timer);
      console.log('GAME ARENA UNMOUNTED, initial state issue');

      joinedGameIdRef.current = null; // Reset for next mount
      rotationSetRef.current = false; // Reset rotation flag for next game
      leaveGame();
      stopEngine();
    };
  }, []);

  // Track short/wide screens (e.g., mobile landscape) to adjust arena rotation
  useEffect(() => {
    const updateScreenShape = () => {
      const { innerWidth, innerHeight } = window;
      const isLandscape = innerWidth >= innerHeight;
      const shortHeight = innerHeight < 700;
      setIsShortWideScreen(isLandscape && shortHeight);
    };

    updateScreenShape();
    window.addEventListener('resize', updateScreenShape);
    return () => window.removeEventListener('resize', updateScreenShape);
  }, []);


  // Calculate optimal cell size and canvas dimensions
  useEffect(() => {
    const calculateSizes = () => {
      const state = gameState ?? committedState;
      if (!state || !state.arena) return;
      
      const gridWidth = state.arena.width || 40;
      const gridHeight = state.arena.height || 40;
      
      const vh = window.innerHeight;
      const vw = window.innerWidth;
      
      // Account for scoreboard (~120px), bottom padding (40px), 
      // container padding (2*16px), and panel border+shadow (~10px)
      const availableHeight = vh - 200 - 32 - 10;
      const availableWidth = vw - 100 - 32 - 10;
      
      // For vertical orientations (90° and 270°), we need to swap dimensions
      const isVertical = rotation === 90 || rotation === 270;
      const effectiveGridWidth = isVertical ? gridHeight : gridWidth;
      const effectiveGridHeight = isVertical ? gridWidth : gridHeight;
      
      // Start with max cell size and reduce until it fits
      let optimalCellSize = 15;
      let canvasWidth = effectiveGridWidth * optimalCellSize;
      let canvasHeight = effectiveGridHeight * optimalCellSize;
      
      // Reduce cell size by 1px until canvas fits in available space
      while ((canvasWidth > availableWidth || canvasHeight > availableHeight) && optimalCellSize > 5) {
        optimalCellSize--;
        canvasWidth = effectiveGridWidth * optimalCellSize;
        canvasHeight = effectiveGridHeight * optimalCellSize;
      }
      
      setCellSize(optimalCellSize);
      // Add 2px to canvas size to account for 1px padding on each side
      setCanvasSize({ width: canvasWidth + 2, height: canvasHeight + 2 });
      setPanelSize({ 
        width: canvasWidth + 12, // Add space for borders and padding
        height: canvasHeight + 12 
      });
    };

    calculateSizes();
    window.addEventListener('resize', calculateSizes);
    
    return () => window.removeEventListener('resize', calculateSizes);
  }, [gameState, committedState, rotation]);

  // Check for game completion
  useEffect(() => {
    // Check if committed state is Complete (from useGameEngine)
    console.log('isGameComplete:', isGameComplete, 'gameOver:', gameOver);
    if (isGameComplete && !gameOver) {
      console.log('Game complete (from committed state), showing game over UI');
      setGameOver(true);
      stopEngine(); // Stop the engine when game ends
      setShowGameOverPanel(true);

      // Note: Users remain in InGame state on this route after game ends.
      // They must explicitly click "Menu" to leave or wait for host to "Play Again"
    }
  }, [gameState, user?.id, gameOver, isGameComplete, stopEngine]);

  // Screen->game direction mapping now lives in Rust (screenDirectionToGame,
  // client/src/render.rs), sharing the rotation convention with the renderer.

  // Set rotation based on user's team when game state is first available
  useEffect(() => {
    const state = gameState ?? committedState;
    if (!state || !user?.id) {
      return;
    }

    const player = state.players?.[user.id];
    if (!player) {
      return;
    }

    const snakeId = player.snake_id;
    const snake = state.arena?.snakes?.[snakeId];

    // Use the actual team_id from the snake when available; fall back to snake_id parity
    const teamId = snake?.team_id ?? (snakeId % 2);
    const isTeamGame = typeof state.game_type === 'object' && 'TeamMatch' in state.game_type;
    const forceUnrotated = isTeamGame && isShortWideScreen;

    const desiredRotation: ArenaRotation = forceUnrotated
      ? 0
      : teamId === 0
        ? 270
        : 90;

    if (!rotationSetRef.current || desiredRotation !== rotation) {
      setRotation(desiredRotation);
      rotationSetRef.current = true;
    }
  }, [gameState, committedState, user?.id, isShortWideScreen, rotation]); // Recompute when game state, user, rotation, or viewport changes

  useEffect(() => {
    if (!getWasm()) {
      console.log('WASM not loaded yet');
      return;
    }

    // Handle keyboard input
    const handleKeyPress = (e: KeyboardEvent) => {
      // Ignore repeat events
      if (e.repeat) {
        return;
      }
      
      if (
        gameOver ||
        !gameState ||
        !isGameInteractionActive
      ) {
        return;
      }
      
      const status = gameState.status;
      if ((typeof status === 'object' && 'Complete' in status) || status === 'Stopped') {
        return;
      }
      
      let direction = null;
      switch(e.key) {
        case 'ArrowUp': direction = 'Up'; break;
        case 'ArrowDown': direction = 'Down'; break;
        case 'ArrowLeft': direction = 'Left'; break;
        case 'ArrowRight': direction = 'Right'; break;
      }
      
      if (direction) {
        e.preventDefault();
        const originalDirection = direction as 'Up' | 'Down' | 'Left' | 'Right';
        // Map the screen direction to a game direction in Rust, sharing the
        // rotation convention with the renderer (see render.rs) so input and
        // rendering cannot desynchronize.
        const transformedDirection = getWasm()!.screenDirectionToGame(
          originalDirection,
          rotation,
        ) as 'Up' | 'Down' | 'Left' | 'Right';
        console.log('Keydown event - sending turn command:', originalDirection, 'transformed to:', transformedDirection, 'rotation:', rotation, 'timestamp:', Date.now());

        // Send command through game engine (handles both local prediction and server)
        sendCommand({
          Turn: { direction: transformedDirection }
        });
        
        console.log('sendCommand call completed at:', Date.now());
      }
    };
    
    window.addEventListener('keydown', handleKeyPress);
    return () => window.removeEventListener('keydown', handleKeyPress);
  }, [
    sendCommand,
    gameOver,
    isGameInteractionActive,
    gameState,
    rotation,
  ]);
  
  // Keep the existing development-only state probe current without restarting
  // the visual requestAnimationFrame loop on every game tick.
  useEffect(() => {
    if (process.env.NODE_ENV !== 'production' && gameState) {
      (window as any).__gameArenaState = gameState;
    }
  }, [gameState]);

  const renderArenaWidth = gameState?.arena.width ?? committedState?.arena.width ?? 0;
  const renderArenaHeight = gameState?.arena.height ?? committedState?.arena.height ?? 0;
  const hasRenderableGameState = gameState !== null;

  // Render game state. Rendering reads the engine's predicted state directly in
  // Rust via renderTo -> GameClient.render, so there is no per-frame JSON
  // serialize/parse round-trip and no untyped `serde_json::Value` indexing;
  // usernames and teams are resolved inside the renderer from the typed state.
  // Cosmetic crash effects are painted immediately afterwards in this same
  // loop because the Rust renderer clears the canvas at the start of each frame.
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!hasRenderableGameState || !canvas || renderArenaWidth <= 0 || renderArenaHeight <= 0) {
      if (!hasRenderableGameState) console.log('Waiting for game state...');
      return;
    }

    const context = canvas.getContext('2d');
    if (!context) {
      console.error('Unable to create the arena canvas context');
      return;
    }

    let animationId = 0;
    const render = (now: number) => {
      try {
        renderTo(canvas, cellSize, rotation, user?.id ?? undefined);
        const crashSnapshot = readPredictedCrashVisualState();
        if (crashSnapshot) {
          // Suppress durable history only on this arena's first snapshot. On a
          // later resync, a recent unseen cue may be the very prediction frame
          // being reconciled, so it must remain eligible to render.
          const isInitialCrashBaseline = crashVisualEpochRef.current === null;
          const epochChanged = crashVisualEpochRef.current !== crashSnapshot.engineEpoch;
          if (epochChanged || lastCrashVisualJsonRef.current !== crashSnapshot.json) {
            const visualState = JSON.parse(
              crashSnapshot.json,
            ) as PredictedCrashVisualState;
            syncPredictedCrashExplosions(
              crashExplosionsRef.current,
              seenCrashEventIdsRef.current,
              gameId,
              visualState,
              now,
              isInitialCrashBaseline ? crashSnapshot.baselineTick : undefined,
            );
            crashVisualEpochRef.current = crashSnapshot.engineEpoch;
            lastCrashVisualJsonRef.current = crashSnapshot.json;
          }
        }
        drawCrashExplosions(
          context,
          crashExplosionSpriteRef.current,
          crashExplosionsRef.current,
          now,
          cellSize,
          renderArenaWidth,
          renderArenaHeight,
          rotation,
          prefersReducedMotionRef.current,
        );
      } catch (error) {
        console.error('Error rendering game:', error);
      }
      animationId = requestAnimationFrame(render);
    };

    animationId = requestAnimationFrame(render);

    return () => {
      cancelAnimationFrame(animationId);
    };
  }, [
    hasRenderableGameState,
    cellSize,
    rotation,
    user?.id,
    renderTo,
    readPredictedCrashVisualState,
    renderArenaWidth,
    renderArenaHeight,
  ]);
  
  // Process server events through the game engine. Events arrive via a
  // lossless queue (see useGameWebSocket): a single signal bump may cover
  // several queued messages, so drain until empty, strictly in arrival
  // order. Skipping or reordering even one message (e.g. the SnakeRespawned
  // that follows a SnakeDied in the same tick) forks the committed state
  // until a snapshot resync heals it.
  useEffect(() => {
    if (!processServerEvent || drainingGameEventsRef.current) {
      return;
    }
    drainingGameEventsRef.current = true;
    void (async () => {
      try {
        let events = takeGameEvents();
        while (events.length > 0) {
          for (const queued of events) {
            const processed = await processServerEvent(queued);
            if (processed && 'Snapshot' in queued.message.event) {
              const snapshotGameId = parseU32GameId(queued.message.game_id);
              if (snapshotGameId !== null) {
                acknowledgeGameSnapshot(snapshotGameId);
              }
            }
          }
          // Pick up anything that arrived while we were processing.
          events = takeGameEvents();
        }
      } finally {
        drainingGameEventsRef.current = false;
      }
    })();
  }, [gameEventSignal, takeGameEvents, processServerEvent, acknowledgeGameSnapshot]);
  
  // Update countdown display
  useEffect(() => {
    const state = gameState ?? committedState;
    if (!state) return;
    
    const intervalId = setInterval(() => {
      const timeLeft = state.start_ms - Date.now();
      if (timeLeft <= 0) {
        clearInterval(intervalId);
      } else {
        // Force re-render to update countdown
        forceUpdate();
      }
    }, 100); // Update every 100ms for smooth countdown
    
    return () => clearInterval(intervalId);
  }, [gameState, committedState, forceUpdate]);

  const handleSendGameChat = useCallback((message: string) => {
    if (!isGameInteractionActive) {
      return;
    }
    sendChatMessage('game', message);
  }, [isGameInteractionActive, sendChatMessage]);
  
  // Calculate countdown from game start time or round start time
  const countdownState = gameState ?? committedState;
  const isWaitingForSnapshot =
    !isGameInteractionActive ||
    !gameState;
  const waitingMessage = !connected
    ? 'Reconnecting...'
    : !isSessionAuthenticated
      ? 'Authenticating...'
      : isJoiningGame || isAwaitingCurrentSnapshot
        ? 'Joining game...'
        : 'Preparing arena...';
  let timeUntilStart = countdownState ? countdownState.start_ms - Date.now() : 0;

  const countdownSeconds = countdownState ? Math.ceil(timeUntilStart / 1000) : 0;
  const showCountdown = countdownState ? countdownSeconds > 0 : false;
  
  const convertLobbyModeToGameType = (mode: LobbyGameMode): GameType => {
    switch (mode) {
      case 'duel':
        return { TeamMatch: { per_team: 1 } };
      case '2v2':
        return { TeamMatch: { per_team: 2 } };
      case 'ffa':
        return { FreeForAll: { max_players: 4 } };
      case 'solo':
      default:
        return 'Solo';
    }
  };

  // Handle back to menu
  const handleBackToMenu = () => {
    // Leave the game first, then navigate
    leaveGame();
    navigate('/');
  };

  const handleRetryGameLoad = () => {
    if (!connected || !isSessionAuthenticated) {
      return;
    }

    joinedGameIdRef.current = joinGame(gameId) ? gameId : null;
  };
  
  // Determine if user is in a lobby and is the host
  const isInLobby = currentLobby !== null;
  const isLobbyQueued = currentLobby?.state === 'queued';

  // Handle play again
  const handlePlayAgain = async () => {
    const state = gameState ?? committedState;
    if (!state) {
      return;
    }

    if (isLobbyQueued) {
      return;
    }

    const canLobbyQueue =
      isInLobby &&
      lobbyPreferences &&
      lobbyPreferences.selectedModes.length > 0;

    if (canLobbyQueue && lobbyPreferences) {
      const queueMode: 'Quickmatch' | 'Competitive' = lobbyPreferences.competitive
        ? 'Competitive'
        : 'Quickmatch';
      const gameTypes = lobbyPreferences.selectedModes.map(convertLobbyModeToGameType);

      if (gameTypes.length === 1) {
        queueForMatch(gameTypes[0], queueMode);
      } else if (gameTypes.length > 1) {
        queueForMatchMulti(gameTypes, queueMode);
      } else {
        queueForMatch(state.game_type);
      }
      return;
    }

    if (!isInLobby) {
      await createLobby();
    }
    queueForMatch(state.game_type);
  };

  const showAuthLoading = authLoading || !user;

  if (showAuthLoading) {
    return <LoadingScreen message={authLoading ? 'Authenticating...' : 'Please log in to play'} />;
  }
  
  return (
    <div className="fixed inset-0 flex flex-col overflow-hidden">

      <>
        {/* Scoreboard */}
        <Scoreboard
          gameState={committedState}
          isVisible={isArenaVisible}
          currentUserId={user?.id}
          showGameOver={showGameOverPanel}
          onBackToMenu={handleBackToMenu}
          onPlayAgain={handlePlayAgain}
          isLobbyQueued={isLobbyQueued}
          queueMode={queueMode}
        />

        {/* Game Arena Container */}
        <div className="flex-1 flex flex-col items-center justify-center p-4" style={{ paddingTop: '120px', paddingBottom: '40px' }}>
          {/* Game Canvas */}
          <div
            className={`panel game-arena-panel bg-white overflow-hidden transition-opacity duration-400 ease-out ${
              isArenaVisible ? 'opacity-100' : 'opacity-0'
            }`}
            ref={containerRef}
            style={{
              width: `${panelSize.width}px`,
              height: `${panelSize.height}px`,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              position: 'relative'
            }}
          >
            <canvas
              ref={canvasRef}
              width={canvasSize.width}
              height={canvasSize.height}
              className="bg-white block"
              style={{
                border: 'none'
              }}
            />
            {currentGameLoadFailure && (
              <div
                className="absolute inset-0 flex flex-col items-center justify-center bg-white/95 z-30 px-6 text-center"
                role="alert"
                data-testid="game-load-failure"
              >
                <h2 className="text-xl font-black italic uppercase tracking-1 text-black-70 mb-3">
                  Game unavailable
                </h2>
                <p className="text-sm text-gray-600 max-w-md mb-6">
                  {currentGameLoadFailure.reason}
                </p>
                <div className="flex flex-wrap items-center justify-center gap-3">
                  {routeGameId !== null && (
                    <button
                      type="button"
                      onClick={handleRetryGameLoad}
                      disabled={!connected || !isSessionAuthenticated}
                      className="px-5 py-2 text-sm border-2 border-black font-bold uppercase bg-black text-white hover:bg-gray-800 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                    >
                      Retry
                    </button>
                  )}
                  <button
                    type="button"
                    onClick={handleBackToMenu}
                    className="px-5 py-2 text-sm border-2 border-black font-bold uppercase bg-white text-black hover:bg-gray-100 transition-colors"
                  >
                    Back to menu
                  </button>
                </div>
              </div>
            )}
            {isWaitingForSnapshot && !currentGameLoadFailure && (
              <div
                className="absolute inset-0 flex flex-col items-center justify-center bg-white/80 z-20"
                data-testid="game-snapshot-loading"
              >
                <span className="w-6 h-6 border-2 border-gray-300 border-t-black rounded-full animate-spin mb-3" aria-hidden="true" />
                <span className="text-gray-600 font-semibold uppercase tracking-1 text-xs">
                  {waitingMessage}
                </span>
              </div>
            )}
            
            {/* Connection watchdog overlay: prediction is frozen by the engine
                while server messages are missing; explain the freeze */}
            {connectionStale && !gameOver && (
              <div className="absolute inset-0 flex items-center justify-center bg-white/85 z-30">
                <span className="text-black font-black italic uppercase tracking-1 text-xl text-center px-4">
                  CONNECTION LOST — RESYNCING
                </span>
              </div>
            )}

            {/* Countdown Overlay */}
            {showCountdown && countdownState && (
              <div className="absolute inset-0 flex flex-col items-center justify-center bg-black/30 z-10">
                <div className="text-white font-bold text-3xl mb-4" style={{
                  textShadow: '0 2px 4px rgba(0,0,0,0.5)'
                }}>
                  Starting In
                </div>
                <div className="text-white font-black italic uppercase" style={{
                  fontSize: '120px',
                  textShadow: '0 4px 8px rgba(0,0,0,0.5)',
                  letterSpacing: '0.05em'
                }}>
                  {countdownSeconds}
                </div>
              </div>
            )}
          </div>
        </div>

      </>
      <ChatPanel
        title="Game Chat"
        messages={gameChatMessages}
        onSendMessage={handleSendGameChat}
        currentUsername={user?.username}
        isActive={isGameInteractionActive}
        inactiveMessage="Game chat unavailable while the game is synchronizing"
        initialExpanded={true}
        autoOpenEligible={false}
      />
    </div>
  );
}
