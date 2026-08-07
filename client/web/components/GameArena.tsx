import React, { useEffect, useLayoutEffect, useRef, useState, useCallback, useReducer, useMemo } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useGameWebSocket } from '../hooks/useGameWebSocket';
import { useGameEngine } from '../hooks/useGameEngine';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import { GameState, CanvasRef, ArenaRotation, GameType, LobbyGameMode, QueueMode, GameLoadFailure } from '../types';
import { getWasm } from '../wasm';
import GameHudShell from './GameHudShell';
import GameControlsHint from './GameControlsHint';
import LoadingScreen from './LoadingScreen';
import { LobbyChat as ChatPanel } from './LobbyChat';
import { INVALID_GAME_ID_REASON, parseU32GameId } from '../utils/gameId';
import {
  CRASH_EXPLOSION_SPRITE_URL,
  drawCrashExplosions,
  syncPredictedCrashExplosions,
} from '../utils/crashExplosion';
import {
  BoostInputController,
  loadBoostInputMode,
  persistBoostInputMode,
  targetOwnsArrowKeys,
  type BoostInputCommand,
  type BoostInputContext,
  type BoostInputDecision,
  type BoostInputMode,
} from '../utils/boostInput';
import { buildBoostHudView } from '../utils/boostHud';
import {
  createScoreEffectRuntime,
  drawScoreEffects,
  resetScoreEffects,
  syncPredictedScoreEffects,
} from '../utils/scoreEffects';
import type {
  CrashExplosion,
  PredictedCrashVisualState,
} from '../utils/crashExplosion';
import type { PredictedScoreVisualState } from '../utils/scoreEffects';
import './GameArena.css';

function BoostCanisterMark() {
  return (
    <svg
      className="game-boost-meter__canister"
      data-testid="boost-nos-bottle"
      viewBox="0 0 34 24"
      preserveAspectRatio="xMidYMid meet"
      shapeRendering="geometricPrecision"
      aria-hidden="true"
      focusable="false"
    >
      <g className="game-boost-meter__canister-tilt" transform="rotate(-24 17 12)">
        <path
          className="game-boost-meter__canister-base"
          fill="#3b82f6"
          d="M2.8 4.8h18.4l3.2 2.8h2V6.1h4.3v2H33v7.8h-2.3v2h-4.3v-1.5h-2l-3.2 2.8H2.8L.6 17V7l2.2-2.2Z"
        />
        <path
          className="game-boost-meter__canister-body"
          fill="#3b82f6"
          d="M3.2 6.3h17.4l2.2 2v7.4l-2.2 2H3.2L2 16.5v-9l1.2-1.2Z"
        />
        <path
          className="game-boost-meter__canister-highlight"
          fill="#93c5fd"
          d="M2.8 4.8h18.4l3.2 2.8h2v1.1h-2.3l-3.2-2.4H3.2L2 7.5v3H.6V7l2.2-2.2Z"
        />
        <path
          className="game-boost-meter__canister-shade"
          fill="#2563eb"
          d="M.6 13.5H2v3l1.2 1.2h17.4l2.2-2v-2.2h1.6v2.9h2v1.5h-2l-3.2 1.3H2.8L.6 17v-3.5Z"
        />
        <rect
          className="game-boost-meter__pressure-plate-separator"
          x="5"
          y="6.3"
          width="15.8"
          height="11.4"
          fill="#f8fafc"
        />
        <rect
          className="game-boost-meter__pressure-plate"
          x="6.7"
          y="8"
          width="12.4"
          height="8"
          fill="#ff641e"
        />
        <text
          className="game-boost-meter__nos-wordmark"
          x="12.9"
          y="12.25"
          fill="#fff"
          fontFamily="Arial, sans-serif"
          fontSize="5.5"
          fontStyle="normal"
          fontWeight="900"
          letterSpacing="0"
          textAnchor="middle"
          dominantBaseline="middle"
        >
          NOS
        </text>
        <path fill="#f8fafc" d="M24.2 9.2h2.4v5.6h-2.4Z" />
        <path fill="#93c5fd" d="M26.2 7.5h3.1v9h-3.1Z" />
        <path fill="#f8fafc" d="M27 7.5h2.3v4.3H27Z" />
        <path fill="#ff641e" d="M29.3 8.6h2v2.6h-2Z" />
        <path fill="#2563eb" d="M29.3 13h2v2.4h-2Z" />
      </g>
    </svg>
  );
}

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
  const visualEpochRef = useRef<number | null>(null);
  const lastVisualJsonRef = useRef<string | null>(null);
  const prefersReducedMotionRef = useRef(false);
  const scoreEffectsRef = useRef(createScoreEffectRuntime());
  
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
    readPredictedVisualState,
    stopEngine
  } = useGameEngine({
    gameId,
    playerId,
    onCommandReady: sendGameCommand,
    onRequestResync: handleRequestResync,
    latencyMs
  });

  const [gameOver, setGameOver] = useState(false);
  const [boostInputMode, setBoostInputMode] = useState<BoostInputMode>(loadBoostInputMode);
  const boostInputControllerRef = useRef<BoostInputController | null>(null);
  if (boostInputControllerRef.current === null) {
    boostInputControllerRef.current = new BoostInputController(boostInputMode);
  }
  const boostInputContextRef = useRef<BoostInputContext>({
    active: false,
    intent: false,
    interactionActive: false,
    gameOver: false,
  });
  const sendBoostCommandRef = useRef<(command: BoostInputCommand) => void>(() => {});
  const boostInputGameIdRef = useRef(gameId);
  const boostPointerIdRef = useRef<number | null>(null);
  const releaseBoostBeforeLeave = useCallback(() => {
    const controller = boostInputControllerRef.current;
    if (!controller) {
      return;
    }
    const decision = controller.teardown(boostInputContextRef.current);
    if (decision.command) {
      sendBoostCommandRef.current(decision.command);
    }
    boostPointerIdRef.current = null;
  }, []);
  const gameSessionClosedRef = useRef(false);
  const leaveGameRef = useRef(leaveGame);
  const stopEngineRef = useRef(stopEngine);
  leaveGameRef.current = leaveGame;
  stopEngineRef.current = stopEngine;
  const teardownGameSession = useCallback(() => {
    if (gameSessionClosedRef.current) {
      return;
    }
    gameSessionClosedRef.current = true;
    releaseBoostBeforeLeave();
    leaveGameRef.current();
    stopEngineRef.current();
  }, [releaseBoostBeforeLeave]);

  // Run before passive hook cleanup can mark the old socket unsynchronized.
  // This keeps a held Boost stop ordered ahead of LeaveGame on SPA route
  // changes as well as a true arena unmount.
  useLayoutEffect(() => {
    gameSessionClosedRef.current = false;
    return teardownGameSession;
  }, [gameId, teardownGameSession]);
  const [cellSize, setCellSize] = useState(15);
  const [canvasSize, setCanvasSize] = useState({ width: 600, height: 600 });
  const [panelSize, setPanelSize] = useState({ width: 610, height: 610 });
  const [hudUtilityHost, setHudUtilityHost] = useState<HTMLDivElement | null>(null);
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
      rotationSetRef.current = false;
      setGameOver(false);
      crashExplosionsRef.current.length = 0;
      seenCrashEventIdsRef.current.clear();
      visualEpochRef.current = null;
      lastVisualJsonRef.current = null;
      resetScoreEffects(scoreEffectsRef.current);
      boostInputControllerRef.current?.reset();
    }

    previousGameIdRef.current = gameId;
    joinedGameIdRef.current = null;
  }, [gameId]);

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
      visualEpochRef.current = null;
      lastVisualJsonRef.current = null;
      resetScoreEffects(scoreEffectsRef.current);
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
      
      // Read the responsive chrome budget from CSS so arena sizing stays in
      // lockstep with the scoreboard, arena-owned utility rail, and controls.
      const hudHeight = Number.parseFloat(
        getComputedStyle(document.documentElement)
          .getPropertyValue('--game-hud-top-footprint'),
      ) || 128;
      const boostIndicatorHeight = state.properties.boost
        ? Number.parseFloat(
            getComputedStyle(document.documentElement)
              .getPropertyValue('--game-boost-indicator-height'),
          ) || 40
        : 0;
      const availableHeight = vh - hudHeight - boostIndicatorHeight - 58 - 32 - 10;
      const availableWidth = vw - 32 - 10;
      
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
      // Once the match is complete, the score card owns Space (including the
      // native activation behavior of its focused Play Again button).
      // Arrow keys use the narrower owner set: a button that happens to hold
      // focus after a click does nothing with arrows, and letting it swallow
      // them left the snake unsteerable until focus moved.
      if (gameOver || targetOwnsArrowKeys(e.target)) {
        return;
      }

      if (e.repeat) {
        return;
      }
      
      if (
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
  const renderState = gameState ?? committedState;
  const renderLocalPlayer = user?.id !== undefined
    ? renderState?.players?.[user.id]
    : undefined;
  const renderLocalTeamId = renderLocalPlayer
    ? renderState?.arena.snakes?.[renderLocalPlayer.snake_id]?.team_id ?? null
    : null;

  // Render game state. Rendering reads the engine's predicted state directly in
  // Rust via renderTo -> GameClient.render, so there is no per-frame JSON
  // serialize/parse round-trip and no untyped `serde_json::Value` indexing;
  // usernames and teams are resolved inside the renderer from the typed state.
  // Cosmetic effects are painted immediately afterwards in this same loop
  // because the Rust renderer clears the canvas at the start of each frame.
  // Rust currently paints field and snakes atomically, so the restrained cell
  // wave overlays that complete frame; crash effects remain the topmost layer.
  //
  // Both crash and score cues are driven from the same predicted visual state,
  // read here rather than from React committed state, so a celebration starts
  // in the frame prediction simulates the goal and is retracted in the frame a
  // reconciliation replay drops it.
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
        const visualSnapshot = readPredictedVisualState();
        if (visualSnapshot) {
          // Suppress durable history only on this arena's first snapshot. On a
          // later resync, a recent unseen cue may be the very prediction frame
          // being reconciled, so it must remain eligible to render.
          const isInitialBaseline = visualEpochRef.current === null;
          const epochChanged = visualEpochRef.current !== visualSnapshot.engineEpoch;
          if (epochChanged || lastVisualJsonRef.current !== visualSnapshot.json) {
            const visualState = JSON.parse(visualSnapshot.json) as
              PredictedCrashVisualState & PredictedScoreVisualState;
            const baselineTick = isInitialBaseline
              ? visualSnapshot.baselineTick
              : undefined;
            syncPredictedCrashExplosions(
              crashExplosionsRef.current,
              seenCrashEventIdsRef.current,
              gameId,
              visualState,
              now,
              baselineTick,
            );
            syncPredictedScoreEffects(
              scoreEffectsRef.current,
              gameId,
              visualState,
              now,
              baselineTick,
            );
            visualEpochRef.current = visualSnapshot.engineEpoch;
            lastVisualJsonRef.current = visualSnapshot.json;
          }
        }
        drawScoreEffects(context, scoreEffectsRef.current, {
          nowMs: now,
          cellSize,
          arenaWidth: renderArenaWidth,
          arenaHeight: renderArenaHeight,
          rotation,
          localTeamId: renderLocalTeamId,
          reducedMotion: prefersReducedMotionRef.current,
        });
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
    readPredictedVisualState,
    renderArenaWidth,
    renderArenaHeight,
    renderLocalTeamId,
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

  // HUD state is read from predicted Rust state so Space/touch activation is
  // immediate and still retracts naturally if the authoritative server
  // rejects or reschedules the command.
  const localPlayer = user?.id !== undefined ? gameState?.players?.[user.id] : undefined;
  const localSnake = localPlayer
    ? gameState?.arena.snakes?.[localPlayer.snake_id]
    : undefined;
  const boostConfig = gameState?.properties.boost ?? null;
  const boostHud = boostConfig && localSnake
    ? buildBoostHudView(boostConfig, localSnake, isGameInteractionActive, gameOver)
    : null;

  const currentStatus = gameState?.status;
  const isBoostGameTerminal = Boolean(
    gameOver ||
    currentStatus === 'Stopped' ||
    (typeof currentStatus === 'object' && currentStatus !== null && 'Complete' in currentStatus),
  );
  const boostInputContext: BoostInputContext = {
    active: Boolean(localSnake?.boost.active),
    // The engine's latched copy of what this player asked for. Reconciliation
    // compares against this, never against fuel: an empty meter defers Boost,
    // it does not cancel the request.
    intent: Boolean(localSnake?.boost.intent),
    interactionActive: Boolean(
      boostConfig &&
      localSnake?.is_alive &&
      isGameInteractionActive &&
      !isBoostGameTerminal
    ),
    gameOver: isBoostGameTerminal,
  };
  boostInputContextRef.current = boostInputContext;

  const sendBoostInputCommand = useCallback((command: BoostInputCommand) => {
    sendCommand(command);
  }, [sendCommand]);
  sendBoostCommandRef.current = sendBoostInputCommand;

  const sendBoostDecision = useCallback((decision: BoostInputDecision) => {
    if (decision.command) {
      sendBoostInputCommand(decision.command);
    }
  }, [sendBoostInputCommand]);

  const handleBoostButtonPress = useCallback(() => {
    const controller = boostInputControllerRef.current;
    if (!controller) {
      return;
    }
    sendBoostDecision(controller.handleButtonPress(boostInputContextRef.current));
  }, [sendBoostDecision]);

  const handleBoostControlKeyDown = useCallback((event: React.KeyboardEvent<HTMLButtonElement>) => {
    const controller = boostInputControllerRef.current;
    if (!controller || controller.getMode() !== 'hold' || event.code !== 'Space') {
      return;
    }

    // The window-level gameplay listener deliberately leaves focused controls
    // alone. Start the same physical Space hold here; the window keyup path
    // then owns release even if depletion disables the focused button first.
    const decision = controller.handleKeyDown({
      code: event.code,
      repeat: event.repeat,
      target: null,
    }, boostInputContextRef.current);
    if (decision.preventDefault) {
      event.preventDefault();
    }
    sendBoostDecision(decision);
  }, [sendBoostDecision]);

  const handleBoostPointerDown = useCallback((event: React.PointerEvent<HTMLButtonElement>) => {
    const controller = boostInputControllerRef.current;
    if (!controller || controller.getMode() !== 'hold' || event.button !== 0) {
      return;
    }

    const decision = controller.handlePointerDown(boostInputContextRef.current);
    if (decision.preventDefault) {
      event.preventDefault();
    }

    // Always claim the pointer, even while commands cannot be sent. The press
    // is a physical fact the controller has already recorded, and skipping this
    // would drop the matching release and leave the hold latched on forever.
    boostPointerIdRef.current = event.pointerId;
    try {
      event.currentTarget.setPointerCapture(event.pointerId);
    } catch {
      // Synthetic and older embedded browsers may not expose pointer capture;
      // pointerup/cancel still delivers the matching release in the common path.
    }
    sendBoostDecision(decision);
  }, [sendBoostDecision]);

  const handleBoostPointerRelease = useCallback((event: React.PointerEvent<HTMLButtonElement>) => {
    if (boostPointerIdRef.current !== event.pointerId) {
      return;
    }
    boostPointerIdRef.current = null;

    const controller = boostInputControllerRef.current;
    if (!controller) {
      return;
    }
    const decision = controller.handlePointerUp(boostInputContextRef.current);
    if (decision.preventDefault) {
      event.preventDefault();
    }
    try {
      if (event.currentTarget.hasPointerCapture(event.pointerId)) {
        event.currentTarget.releasePointerCapture(event.pointerId);
      }
    } catch {
      // The browser may already have released capture during cancellation.
    }
    sendBoostDecision(decision);
  }, [sendBoostDecision]);

  const handleBoostInputModeChange = useCallback((mode: BoostInputMode) => {
    const controller = boostInputControllerRef.current;
    if (!controller || controller.getMode() === mode) {
      return;
    }

    sendBoostDecision(controller.setMode(mode, boostInputContextRef.current));
    setBoostInputMode(mode);
    persistBoostInputMode(mode);
  }, [sendBoostDecision]);

  // Keep one set of physical-key listeners for the arena lifetime. Mutable
  // refs let keyup use the latest snake state even if focus or connectivity
  // changes between keydown and release.
  useEffect(() => {
    const dispatch = (decision: BoostInputDecision, event?: KeyboardEvent) => {
      if (decision.preventDefault) {
        event?.preventDefault();
      }
      if (decision.command) {
        sendBoostCommandRef.current(decision.command);
      }
    };
    const handleBoostKeyDown = (event: KeyboardEvent) => {
      dispatch(
        boostInputControllerRef.current!.handleKeyDown(
          event,
          boostInputContextRef.current,
        ),
        event,
      );
    };
    const handleBoostKeyUp = (event: KeyboardEvent) => {
      dispatch(
        boostInputControllerRef.current!.handleKeyUp(
          event,
          boostInputContextRef.current,
        ),
        event,
      );
    };
    const releaseHeldBoost = () => {
      dispatch(
        boostInputControllerRef.current!.releaseHeld(boostInputContextRef.current),
      );
    };
    const handleVisibilityChange = () => {
      if (document.visibilityState === 'hidden') {
        releaseHeldBoost();
      }
    };

    window.addEventListener('keydown', handleBoostKeyDown);
    window.addEventListener('keyup', handleBoostKeyUp);
    window.addEventListener('blur', releaseHeldBoost);
    document.addEventListener('visibilitychange', handleVisibilityChange);

    return () => {
      window.removeEventListener('keydown', handleBoostKeyDown);
      window.removeEventListener('keyup', handleBoostKeyUp);
      window.removeEventListener('blur', releaseHeldBoost);
      document.removeEventListener('visibilitychange', handleVisibilityChange);
      const context = boostInputContextRef.current;
      if (context.interactionActive && !context.gameOver) {
        dispatch(boostInputControllerRef.current!.cleanup(context));
      } else {
        boostInputControllerRef.current!.reset();
      }
    };
  }, []);

  // Republish Boost intent whenever the engine's latched copy disagrees with
  // what the player is doing. Losing interaction is deliberately NOT treated as
  // a release: the key is still physically held, so wiping that here would make
  // a brief disconnect require a fresh press. `reconcile` simply publishes
  // nothing until commands can be delivered again.
  useEffect(() => {
    const controller = boostInputControllerRef.current;
    if (!controller) {
      return;
    }

    if (boostInputGameIdRef.current !== gameId) {
      boostInputGameIdRef.current = gameId;
      controller.reset();
      return;
    }

    if (boostInputContext.gameOver) {
      controller.reset();
      return;
    }

    sendBoostDecision(controller.reconcile(boostInputContext));
  }, [
    gameId,
    boostInputContext.active,
    boostInputContext.intent,
    boostInputContext.interactionActive,
    boostInputContext.gameOver,
    sendBoostDecision,
  ]);

  const boostButtonDisabled = !boostHud || boostHud.buttonDisabled;
  
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
    // Keep the stop ordered before LeaveGame clears the command channel.
    teardownGameSession();
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

  const boostControl = boostConfig && localSnake && boostHud && !gameOver ? (
    <div
      className={`game-boost-hud${isArenaVisible ? ' is-visible' : ''}${boostHud.active ? ' is-active' : ''}${boostHud.ready ? ' is-ready' : ''}`}
      data-testid="boost-hud"
      data-location="arena-bottom"
      data-ready={boostHud.ready ? 'true' : 'false'}
    >
      <span
        className="game-boost-meter__track"
        role="progressbar"
        aria-label="Stored Boost charge"
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={boostHud.percent}
        aria-valuetext={`${boostHud.percent}%${boostHud.active ? ', active' : ''}`}
      >
        <span
          className="game-boost-meter__fill"
          style={{ transform: `scaleX(${boostHud.fillRatio})` }}
        />
      </span>
      <button
        type="button"
        onClick={handleBoostButtonPress}
        onKeyDown={handleBoostControlKeyDown}
        onPointerDown={handleBoostPointerDown}
        onPointerUp={handleBoostPointerRelease}
        onPointerCancel={handleBoostPointerRelease}
        onLostPointerCapture={handleBoostPointerRelease}
        disabled={boostButtonDisabled}
        aria-label={boostInputMode === 'hold'
          ? (boostHud.active
              ? `Release Boost, ${boostHud.percent}% remaining`
              : `Hold to Boost, ${boostHud.percent}% charged`)
          : (boostHud.active
              ? `Stop Boost, ${boostHud.percent}% remaining`
              : `Activate Boost, ${boostHud.percent}% charged`)}
        aria-keyshortcuts="Space"
        className="game-boost-meter"
        data-testid="boost-button"
      >
        <span className="game-boost-meter__canister-dock" aria-hidden="true">
          <BoostCanisterMark />
        </span>
        <span className="game-boost-meter__reservoir" aria-hidden="true" />
        <strong className="game-boost-meter__value">
          {boostHud.percent}%
        </strong>
      </button>
    </div>
  ) : null;

  if (showAuthLoading) {
    return <LoadingScreen message={authLoading ? 'Authenticating...' : 'Please sign in to play'} />;
  }
  
  return (
    <div className="game-arena-screen fixed inset-0 flex flex-col overflow-hidden">

      <>
        <GameHudShell
          gameState={committedState}
          isVisible={isArenaVisible}
          arenaWidth={panelSize.width}
          currentUserId={user?.id}
          queueMode={queueMode}
          onMenu={handleBackToMenu}
          onPlayAgain={handlePlayAgain}
          playAgainDisabled={isLobbyQueued}
          utilityHost={hudUtilityHost}
        />

        {/* Game Arena Container */}
        <div
          className="game-arena-stage flex-1 flex flex-col items-center justify-center p-4"
          style={{ '--game-arena-panel-width': `${panelSize.width}px` } as React.CSSProperties}
        >
          <div
            ref={setHudUtilityHost}
            className="game-arena-utility-anchor"
            style={{ width: `${panelSize.width}px` }}
            data-testid="game-arena-utility-anchor"
          />
          {/* Game Canvas */}
          <div
            className={`game-arena-frame${boostControl ? ' has-boost-indicator' : ''}`}
            style={{ width: `${panelSize.width}px` }}
          >
            <div
              className={`panel game-arena-panel bg-white overflow-hidden transition-opacity duration-400 ease-out ${
                isArenaVisible ? 'opacity-100' : 'opacity-0'
              }`}
              ref={containerRef}
              style={{
                width: '100%',
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
            {boostControl}
          </div>
          <GameControlsHint
            showBoost={Boolean(boostConfig)}
            boostInputMode={boostInputMode}
            onBoostInputModeChange={handleBoostInputModeChange}
          />
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
