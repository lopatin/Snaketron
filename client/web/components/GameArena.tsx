import React, { useEffect, useLayoutEffect, useRef, useState, useCallback, useReducer, useMemo } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useGameWebSocket } from '../hooks/useGameWebSocket';
import { useGameEngine } from '../hooks/useGameEngine';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import {
  GameState,
  CanvasRef,
  ArenaRotation,
  GameType,
  LobbyGameMode,
  QueueMode,
  GameLoadFailure,
  type CommandId,
  type GameCommandMessage,
} from '../types';
import { getWasm } from '../wasm';
import GameHudShell from './GameHudShell';
import GameControlsHint from './GameControlsHint';
import IdleKickDialog from './IdleKickDialog';
import IdleWarningBanner from './IdleWarningBanner';
import LoadingScreen from './LoadingScreen';
import TutorialModal from './TutorialModal';
import { buildMatchPresentation, simulationStartMs } from '../utils/gamePresentation';
import { crazyGames } from '../services/crazyGames';
import {
  hasSeenTutorial,
  markTutorialSeen,
  tutorialContentForGame,
} from '../utils/tutorial';
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
import { buildComboHudView } from '../utils/comboHud';
import { buildPlayerIdlePresentation } from '../utils/idlePresentation';
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
import BoostCanisterMark from './BoostCanisterMark';
import ComboCallout from './ComboCallout';
import TouchControls, {
  TOUCH_LANDSCAPE_SIDE_RESERVE_PX,
  TOUCH_PORTRAIT_BOTTOM_RESERVE_PX,
  type ScreenDirection,
} from './TouchControls';
import { useInputSurface } from '../hooks/useInputSurface';
import { useFullscreen } from '../hooks/useFullscreen';
import { useMatchRating } from '../hooks/useMatchRating';
import './GameArena.css';

/**
 * Pointer edges for one physical Boost button, isolated per button so the
 * arena meter and the mobile touch button cannot claim each other's pointer.
 * The shared controller still collapses simultaneous holds into one level.
 */
function useBoostPointerBinding(
  controllerRef: React.MutableRefObject<BoostInputController | null>,
  contextRef: React.MutableRefObject<BoostInputContext>,
  sendBoostDecision: (decision: BoostInputDecision) => void,
) {
  const pointerIdRef = useRef<number | null>(null);
  const pointerTargetRef = useRef<HTMLButtonElement | null>(null);
  const sendBoostDecisionRef = useRef(sendBoostDecision);
  sendBoostDecisionRef.current = sendBoostDecision;

  const finishPointer = useCallback((
    pointerId: number,
    preventDefault?: () => void,
  ) => {
    if (pointerIdRef.current !== pointerId) {
      return;
    }
    pointerIdRef.current = null;

    const pointerTarget = pointerTargetRef.current;
    pointerTargetRef.current = null;
    const controller = controllerRef.current;
    if (!controller) {
      return;
    }

    const decision = controller.handlePointerUp(pointerId, contextRef.current);
    if (decision.preventDefault) {
      preventDefault?.();
    }
    try {
      if (pointerTarget?.hasPointerCapture(pointerId)) {
        pointerTarget.releasePointerCapture(pointerId);
      }
    } catch {
      // The browser may already have released capture during cancellation.
    }
    sendBoostDecisionRef.current(decision);
  }, [contextRef, controllerRef]);

  // Pointer capture is the fast path, but older embedded browsers can reject
  // it and mobile lifecycle changes can retarget the final event. Observe the
  // release at the window as a fallback so leaving or disabling the button
  // cannot strand its Hold edge.
  const finishPointerRef = useRef(finishPointer);
  finishPointerRef.current = finishPointer;
  useEffect(() => {
    const finishGlobalPointer = (event: PointerEvent) => {
      finishPointerRef.current(event.pointerId, () => {
        if (event.cancelable) {
          event.preventDefault();
        }
      });
    };
    const handleGlobalLostPointerCapture = (event: PointerEvent) => {
      if (pointerIdRef.current !== event.pointerId) {
        return;
      }

      const pointerTarget = pointerTargetRef.current;
      try {
        // WebKit can deliver an older lost-capture event after capture has
        // already moved to the button. Do not let that stale event release a
        // newer live hold that reused the same pointer id.
        if (pointerTarget?.hasPointerCapture(event.pointerId)) {
          return;
        }
      } catch {
        // Without capture introspection, target identity is the safe fallback.
      }
      if (event.target !== pointerTarget && event.target !== document) {
        return;
      }
      finishGlobalPointer(event);
    };

    window.addEventListener('pointerup', finishGlobalPointer, true);
    window.addEventListener('pointercancel', finishGlobalPointer, true);
    window.addEventListener('lostpointercapture', handleGlobalLostPointerCapture, true);
    return () => {
      window.removeEventListener('pointerup', finishGlobalPointer, true);
      window.removeEventListener('pointercancel', finishGlobalPointer, true);
      window.removeEventListener('lostpointercapture', handleGlobalLostPointerCapture, true);

      const pointerId = pointerIdRef.current;
      const pointerTarget = pointerTargetRef.current;
      pointerIdRef.current = null;
      pointerTargetRef.current = null;
      try {
        if (pointerId !== null && pointerTarget?.hasPointerCapture(pointerId)) {
          pointerTarget.releasePointerCapture(pointerId);
        }
      } catch {
        // Arena teardown has already cleared the controller's physical holds.
      }
    };
  }, []);

  const onPointerDown = useCallback((event: React.PointerEvent<HTMLButtonElement>) => {
    const controller = controllerRef.current;
    if (!controller || controller.getMode() !== 'hold' || event.button !== 0) {
      return;
    }

    const heldPointerId = pointerIdRef.current;
    if (heldPointerId !== null) {
      // One live hold per button. If a controller-level safety reset already
      // cleared this id, however, the DOM cache is stale and must not poison
      // every later Hold press on this button.
      if (controller.isPointerHeld(heldPointerId)) {
        event.preventDefault();
        return;
      }
      pointerIdRef.current = null;
      pointerTargetRef.current = null;
    }

    const decision = controller.handlePointerDown(event.pointerId, contextRef.current);
    if (decision.preventDefault) {
      event.preventDefault();
    }

    // Always claim the pointer, even while commands cannot be sent. The press
    // is a physical fact the controller has already recorded, and skipping this
    // would drop the matching release and leave the hold latched on forever.
    pointerIdRef.current = event.pointerId;
    pointerTargetRef.current = event.currentTarget;
    try {
      event.currentTarget.setPointerCapture(event.pointerId);
    } catch {
      // Synthetic and older embedded browsers may not expose pointer capture;
      // pointerup/cancel still delivers the matching release in the common path.
    }
    sendBoostDecisionRef.current(decision);
  }, [contextRef, controllerRef]);

  const onPointerRelease = useCallback((event: React.PointerEvent<HTMLButtonElement>) => {
    finishPointer(event.pointerId, () => event.preventDefault());
  }, [finishPointer]);

  return { onPointerDown, onPointerRelease };
}

const commandIdKey = (commandId: CommandId): string => (
  `${commandId.tick}:${commandId.user_id}:${commandId.sequence_number}`
);

const boostCommandFromMessage = (
  commandMessage: GameCommandMessage,
): BoostInputCommand | null => {
  if ('ActivateBoost' in commandMessage.command) {
    return 'ActivateBoost';
  }
  if ('DeactivateBoost' in commandMessage.command) {
    return 'DeactivateBoost';
  }
  return null;
};

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
  const platformResultReportedForGameRef = useRef<string | null>(null);
  const platformGameplayObservedForGameRef = useRef<string | null>(null);
  // The gameplay key listeners are installed once for the arena's lifetime, so
  // they read modal ownership through a ref rather than being torn down and
  // rebuilt every time the briefing opens or closes.
  const isModalOwningInputRef = useRef(false);
  
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
    sendPlayerReady,
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
    leaveLobby,
  } = useWebSocket();
  const playerId = user?.id ?? 0;
  const queueMode: QueueMode = lobbyPreferences?.competitive ? 'Competitive' : 'Quickmatch';
  const pendingBoostCommandsRef = useRef<Map<string, BoostInputCommand>>(new Map());

  // Returns whether durable delivery admitted the command, which is what lets
  // the engine retract its local prediction when the transport refuses it.
  const handleCommandReady = useCallback((commandMessage: GameCommandMessage) => {
    const admitted = sendGameCommand(commandMessage);
    const boostCommand = boostCommandFromMessage(commandMessage);
    if (boostCommand) {
      if (admitted) {
        // Cleared when the server schedules or rejects this command id.
        pendingBoostCommandsRef.current.set(
          commandIdKey(commandMessage.command_id_client),
          boostCommand,
        );
      } else {
        // An unadmitted command never reaches the server, so no scheduled or
        // rejected outcome will ever arrive to clear the optimistic Boost
        // input level. Retract it here exactly as a rejection would, or the
        // controller stays stuck on a level it never actually requested.
        boostInputControllerRef.current?.handleRejectedCommand(boostCommand);
      }
    }
    return admitted;
  }, [sendGameCommand]);

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
    onCommandReady: handleCommandReady,
    onRequestResync: handleRequestResync,
    latencyMs
  });

  const [gameOver, setGameOver] = useState(false);
  const inputSurface = useInputSurface();
  const matchRating = useMatchRating(
    gameId,
    committedState ?? gameState,
    isGameComplete,
    user?.id,
  );
  const isTouchSurface = inputSurface === 'touch';
  const fullscreen = useFullscreen();
  // The touch d-pad reads its gates through this ref so a tap consults the
  // freshest game state at event time, exactly like the keydown listener's
  // ref reads, without re-subscribing anything per simulation tick.
  const steerContextRef = useRef<{ canSteer: boolean; rotation: ArenaRotation }>({
    canSteer: false,
    rotation: 0,
  });
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
  const releaseBoostBeforeLeave = useCallback(() => {
    const controller = boostInputControllerRef.current;
    if (!controller) {
      return;
    }
    // Clearing controller-owned pointer ids also lets every retained DOM
    // binding recognize and replace a stale cached id on its next press.
    const decision = controller.teardown(boostInputContextRef.current);
    if (decision.command) {
      sendBoostCommandRef.current(decision.command);
    }
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

    pendingBoostCommandsRef.current.clear();
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
  const committedLocalIdle = buildPlayerIdlePresentation(committedState, user?.id);
  const localWasIdleKicked = committedLocalIdle.isKicked;
  const isGameObservationActive =
    connected &&
    isSessionAuthenticated &&
    isRequestForCurrentRoute &&
    isGameSnapshotSynchronized &&
    !isAwaitingCurrentSnapshot &&
    !currentGameLoadFailure &&
    gameState !== null;
  const isGameInteractionActive = isGameObservationActive && !localWasIdleKicked;

  useEffect(() => {
    crazyGames.loadingStart();
    return () => {
      crazyGames.loadingStop();
      crazyGames.gameplayStop();
      crazyGames.clearGameContext();
    };
  }, [gameId]);

  useEffect(() => {
    if (isGameObservationActive || currentGameLoadFailure) {
      crazyGames.loadingStop();
    }
  }, [currentGameLoadFailure, isGameObservationActive]);

  const platformGameType = useMemo(() => {
    const gameType = (gameState ?? committedState)?.game_type;
    if (!gameType) {
      return null;
    }
    return typeof gameType === 'string'
      ? gameType
      : Object.keys(gameType)[0] ?? 'Unknown';
  }, [committedState?.game_type, gameState?.game_type]);
  const platformQueueMode = (committedState ?? gameState)?.queue_mode ?? queueMode;

  useEffect(() => {
    if (!platformGameType) {
      return;
    }
    crazyGames.setGameContext({
      gameId,
      gameType: platformGameType,
      queueMode: platformQueueMode,
      lobbyCode: currentLobby?.code ?? 'matchmaking',
    });
  }, [currentLobby?.code, gameId, platformGameType, platformQueueMode]);

  useEffect(() => {
    if (currentGameLoadFailure) {
      // Allow an explicit retry or a future authenticated reconnect to issue JoinGame again.
      joinedGameIdRef.current = null;
    }
  }, [currentGameLoadFailure]);

  useEffect(() => {
    if (!isGameObservationActive) {
      stopEngine();
    }
  }, [isGameObservationActive, stopEngine]);

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
      const rootStyle = getComputedStyle(document.documentElement);
      const hudHeight = Number.parseFloat(
        rootStyle.getPropertyValue('--game-hud-top-footprint'),
      ) || 128;
      const boostIndicatorHeight = state.properties.boost
        ? Number.parseFloat(
            rootStyle.getPropertyValue('--game-boost-indicator-height'),
          ) || 40
        : 0;
      const safeAreaInset = (side: 'bottom' | 'left' | 'right'): number =>
        Number.parseFloat(rootStyle.getPropertyValue(`--safe-area-inset-${side}`)) || 0;
      // Strict comparison to match CSS: `orientation: portrait` matches when
      // height >= width, so an exactly-square viewport is portrait there too.
      const isLandscapeViewport = vw > vh;

      let availableHeight: number;
      let availableWidth: number;
      if (isTouchSurface) {
        // Touch layouts budget the real in-flow chrome instead of the legacy
        // desktop slack: the fixed scoreboard band, the roster anchor row, the
        // charge rail, and the control-cluster reserves that mirror the
        // stage padding in GameArena.css (safe-area insets included, which are
        // only non-zero on notched phones in fullscreen). Every unspent pixel
        // here is arena, which matters on a phone.
        const scoreboardFootprint = Number.parseFloat(
          rootStyle.getPropertyValue('--game-scoreboard-footprint'),
        ) || 64;
        // Short landscape compacts the roster band (GameArena.css) — its
        // 19px snake pills never needed a 44px row, and on a phone the
        // difference is a whole cell-size step for the arena.
        const compactLandscape = isLandscapeViewport && vh <= 560;
        const anchorFootprint = compactLandscape ? 30 : vw <= 760 ? 58 : 68;
        const touchBottomReserve = isLandscapeViewport
          ? 0
          : TOUCH_PORTRAIT_BOTTOM_RESERVE_PX + safeAreaInset('bottom');
        const touchSideReserve = isLandscapeViewport
          ? TOUCH_LANDSCAPE_SIDE_RESERVE_PX * 2 + safeAreaInset('left') + safeAreaInset('right')
          : 0;
        availableHeight = vh - scoreboardFootprint - anchorFootprint -
          boostIndicatorHeight - touchBottomReserve -
          (isLandscapeViewport ? (compactLandscape ? 14 : 22) : 16);
        availableWidth = vw - 28 - touchSideReserve;
      } else {
        availableHeight = vh - hudHeight - boostIndicatorHeight - 58 - 32 - 10;
        availableWidth = vw - 32 - 10;
      }
      
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
  }, [gameState, committedState, rotation, isTouchSurface]);

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

  useEffect(() => {
    const completedState = committedState ?? gameState;
    if (
      !isGameComplete ||
      !completedState ||
      platformResultReportedForGameRef.current === gameId
    ) {
      return;
    }

    platformResultReportedForGameRef.current = gameId;
    const playedThisPageSession = platformGameplayObservedForGameRef.current === gameId;
    const presentation = buildMatchPresentation(
      completedState,
      user?.id,
      completedState.queue_mode,
    );
    if (
      playedThisPageSession &&
      completedState.queue_mode === 'Competitive' &&
      presentation.currentPlayer?.isWinner
    ) {
      crazyGames.happyTime();
    }

    if (playedThisPageSession) {
      // Match completion ends provider gameplay. Advertisement policy is
      // server-owned and, when eligible, starts on the next queue request.
      crazyGames.gameplayStop();
    }
  }, [committedState, gameId, gameState, isGameComplete, user?.id]);

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
        !isGameInteractionActive ||
        // A briefing owns the screen; steering commands queued behind it would
        // fire the instant the match starts, before the player is looking.
        isModalOwningInputRef.current
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
  // The Rust renderer clears the canvas at the start of each frame, paints the
  // field, invokes our score-effect callback, and then paints snakes and walls.
  // This keeps celebrations behind gameplay actors while crash effects remain
  // the intentionally topmost layer.
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
        renderTo(
          canvas,
          cellSize,
          rotation,
          user?.id ?? undefined,
          () => {
            drawScoreEffects(context, scoreEffectsRef.current, {
              nowMs: now,
              cellSize,
              arenaWidth: renderArenaWidth,
              arenaHeight: renderArenaHeight,
              rotation,
              localTeamId: renderLocalTeamId,
              reducedMotion: prefersReducedMotionRef.current,
            });
          },
        );
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
            const event = queued.message.event;
            if (processed) {
              const outcomeCommandId = 'CommandScheduledV2' in event
                ? event.CommandScheduledV2.command_message.command_id_client
                : 'CommandScheduled' in event
                  ? event.CommandScheduled.command_message.command_id_client
                  : 'CommandRejected' in event
                    ? event.CommandRejected.command_id_client
                    : null;
              if (outcomeCommandId) {
                const key = commandIdKey(outcomeCommandId);
                const boostCommand = pendingBoostCommandsRef.current.get(key);
                pendingBoostCommandsRef.current.delete(key);
                if (boostCommand && 'CommandRejected' in event) {
                  boostInputControllerRef.current?.handleRejectedCommand(boostCommand);
                }
              }
            }
            if (processed && 'Snapshot' in event) {
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
  
  // Update countdown display. The clock runs off the resolved simulation
  // epoch, not `start_ms`: with a readiness gate the two differ, and `start_ms`
  // is already in the past by the time everyone has confirmed.
  useEffect(() => {
    const state = gameState ?? committedState;
    if (!state) return;

    // While the gate is pending there is no epoch yet, but the briefing shows
    // its own auto-ready countdown, so keep re-rendering for that instead.
    const intervalId = setInterval(() => {
      const epochMs = simulationStartMs(state);
      if (epochMs !== null && epochMs - Date.now() <= 0) {
        clearInterval(intervalId);
      } else {
        // Force re-render to update countdown
        forceUpdate();
      }
    }, 100); // Update every 100ms for smooth countdown

    return () => clearInterval(intervalId);
  }, [gameState, committedState, forceUpdate]);

  // --- Pre-match briefing and readiness -----------------------------------
  //
  // The match a player lands in is only known once it exists (they can queue
  // for several modes at once), so the briefing is keyed off authoritative
  // state here rather than off anything chosen at queue time.
  const readinessState = committedState ?? gameState;
  const tutorial = useMemo(
    () => (readinessState
      ? tutorialContentForGame(readinessState, boostInputMode, inputSurface)
      : null),
    [
      boostInputMode,
      inputSurface,
      readinessState?.game_type,
      readinessState?.properties.score_limit,
      readinessState?.queue_mode,
    ],
  );
  // Spectators have no snake and the server will not accept a confirmation
  // from them, so showing them a Ready button would be a control that can
  // never do anything. They watch the roster fill in instead.
  const isLocalUserPlaying = Boolean(
    user?.id !== undefined && readinessState?.players?.[user.id],
  );
  const isAwaitingReadiness = readinessState?.readiness != null;
  const localUserIsReady = Boolean(
    user?.id !== undefined &&
    readinessState?.readiness?.ready_user_ids.includes(user.id),
  );
  const readyDeadlineMs = readinessState?.readiness?.deadline_ms ?? null;
  const authoritativePendingReadyCount = readinessState?.readiness
    ? Object.keys(readinessState.players ?? {}).filter(
        (userId) => !readinessState.readiness!.ready_user_ids.includes(Number(userId)),
      ).length
    : 0;

  // Local intent, distinct from the server's record of it. A dropped
  // confirmation or a mid-gate resync would otherwise leave a player who
  // already pressed Ready staring at the briefing again.
  const [readyIntent, setReadyIntent] = useState<{
    gameId: string;
    source: 'manual' | 'automatic';
  } | null>(null);
  const [helpOpen, setHelpOpen] = useState(false);
  const currentReadyIntent = readyIntent?.gameId === gameId ? readyIntent.source : null;
  const hasPressedReady = currentReadyIntent !== null;
  // Read this before the auto-ready effect runs so a returning player never
  // mounts the briefing for a frame. A manual press records its provenance
  // before marking the tutorial seen, preserving the intentional waiting view.
  const shouldAutoReady = currentReadyIntent === 'automatic' || Boolean(
    currentReadyIntent === null &&
    isAwaitingReadiness &&
    tutorial &&
    hasSeenTutorial(tutorial.key),
  );
  // Treat the local click as ready immediately instead of briefly counting the
  // player among the people they are now waiting for while the server echoes it.
  const pendingReadyCount = Math.max(
    0,
    authoritativePendingReadyCount - (
      hasPressedReady && isLocalUserPlaying && !localUserIsReady ? 1 : 0
    ),
  );

  useEffect(() => {
    setReadyIntent((intent) => (intent?.gameId === gameId ? intent : null));
    setHelpOpen(false);
  }, [gameId]);

  const confirmReady = useCallback((source: 'manual' | 'automatic') => {
    setReadyIntent({ gameId, source });
    if (tutorial) {
      markTutorialSeen(tutorial.key);
    }
    sendPlayerReady(gameId);
  }, [gameId, sendPlayerReady, tutorial]);

  // A player who has already seen this mode's briefing is readied for them, so
  // veterans never wait on a screen they have read before. The gate still runs
  // for every match, which is what makes the roster checkmarks meaningful.
  useEffect(() => {
    if (
      !isGameInteractionActive ||
      !isAwaitingReadiness ||
      !isLocalUserPlaying ||
      !tutorial ||
      hasPressedReady ||
      localUserIsReady ||
      !shouldAutoReady
    ) {
      return;
    }
    confirmReady('automatic');
  }, [
    confirmReady,
    hasPressedReady,
    isAwaitingReadiness,
    isGameInteractionActive,
    isLocalUserPlaying,
    localUserIsReady,
    shouldAutoReady,
    tutorial,
  ]);

  // Self-healing resend: the server drops a readiness it has already recorded,
  // so re-asserting a local press the authoritative state does not reflect is
  // safe and closes the gap left by a lost message or a snapshot resync.
  useEffect(() => {
    if (
      !isGameInteractionActive ||
      !isAwaitingReadiness ||
      !isLocalUserPlaying ||
      !hasPressedReady ||
      localUserIsReady
    ) {
      return undefined;
    }
    const resend = window.setInterval(() => sendPlayerReady(gameId), 2000);
    return () => window.clearInterval(resend);
  }, [
    gameId,
    hasPressedReady,
    isAwaitingReadiness,
    isGameInteractionActive,
    isLocalUserPlaying,
    localUserIsReady,
    sendPlayerReady,
  ]);

  const showBriefing = Boolean(
    tutorial &&
    isAwaitingReadiness &&
    isLocalUserPlaying &&
    isGameInteractionActive &&
    !shouldAutoReady &&
    !gameOver,
  );
  // `!localWasIdleKicked` is what the effect below cannot do on its own: state
  // set from an effect lands a frame late, and that frame is one in which the
  // help backdrop covers the removal dialog explaining why the match ended.
  const showHelp = Boolean(tutorial && helpOpen && !showBriefing && !localWasIdleKicked);

  // A briefing supersedes the help screen rather than hiding it. Without this
  // the suppressed help modal would spring back over live gameplay the moment
  // the gate resolved, blocking input with no obvious cause.
  useEffect(() => {
    if (showBriefing && helpOpen) {
      setHelpOpen(false);
    }
  }, [showBriefing, helpOpen]);

  const handleSendGameChat = useCallback((message: string) => {
    if (!isGameInteractionActive) {
      return;
    }
    sendChatMessage('game', message);
  }, [isGameInteractionActive, sendChatMessage]);
  
  // Calculate countdown from game start time or round start time
  const countdownState = gameState ?? committedState;
  const isWaitingForSnapshot =
    !isGameObservationActive ||
    !gameState;
  const waitingMessage = !connected
    ? 'Reconnecting...'
    : !isSessionAuthenticated
      ? 'Authenticating...'
      : isJoiningGame || isAwaitingCurrentSnapshot
        ? 'Joining game...'
        : 'Preparing arena...';
  const countdownEpochMs = countdownState ? simulationStartMs(countdownState) : null;
  const timeUntilStart = countdownEpochMs === null ? 0 : countdownEpochMs - Date.now();

  const countdownSeconds = countdownEpochMs === null ? 0 : Math.ceil(timeUntilStart / 1000);
  // A match still held by the readiness gate has no countdown to show — the
  // briefing owns the screen until it resolves.
  const showCountdown = countdownSeconds > 0;
  const platformStatus = (committedState ?? gameState)?.status;
  const platformGameStarted = Boolean(
    typeof platformStatus === 'object' &&
    platformStatus !== null &&
    'Started' in platformStatus,
  );
  const platformGameplayActive =
    isGameInteractionActive &&
    isLocalUserPlaying &&
    platformGameStarted &&
    !connectionStale &&
    !isAwaitingReadiness &&
    !isGameComplete &&
    !gameOver &&
    !showBriefing &&
    !showHelp &&
    !showCountdown;

  useEffect(() => {
    if (platformGameplayActive) {
      platformGameplayObservedForGameRef.current = gameId;
      crazyGames.gameplayStart();
    } else {
      crazyGames.gameplayStop();
    }
    return () => crazyGames.gameplayStop();
  }, [gameId, platformGameplayActive]);
  const localIdle = buildPlayerIdlePresentation(gameState ?? committedState, user?.id);
  const idleWarning =
    isGameInteractionActive &&
    !connectionStale &&
    !gameOver
      ? localIdle.warning
      : null;
  const showIdleKickDialog = localWasIdleKicked && !isGameComplete && !gameOver;

  // The help screen is the same modal surface as the briefing: it suppresses
  // every steering and Boost command, which are exactly the inputs the
  // inactivity clock reads as presence, and its backdrop covers both the
  // warning banner and the removal dialog. A player reading it would be warned
  // invisibly, lose the "I'm still here" button behind the backdrop, and then
  // be removed into an explanation they could not see. Yield the screen the
  // moment inactivity has something to say — the same way the briefing does.
  const inactivityNeedsTheScreen = Boolean(idleWarning) || localWasIdleKicked;
  useEffect(() => {
    if (helpOpen && inactivityNeedsTheScreen) {
      setHelpOpen(false);
    }
  }, [helpOpen, inactivityNeedsTheScreen]);

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
  const comboConfig = gameState?.properties.combo ?? null;
  const comboHud = comboConfig && localSnake
    ? buildComboHudView(comboConfig, localSnake)
    : null;

  const currentStatus = gameState?.status;
  const isGameTerminal = Boolean(
    gameOver ||
    currentStatus === 'Stopped' ||
    (typeof currentStatus === 'object' && currentStatus !== null && 'Complete' in currentStatus),
  );
  // A modal owns the screen: Space belongs to its Ready button, not to Boost.
  const isModalOwningInput = showBriefing || showHelp;
  isModalOwningInputRef.current = isModalOwningInput;
  // The touch d-pad shares the keyboard steering gates. Modal ownership is
  // deliberately left out and read at event time instead, exactly like the
  // keydown path, so a briefing opening mid-game swallows taps immediately.
  steerContextRef.current = {
    canSteer: Boolean(gameState) && isGameInteractionActive && !isGameTerminal,
    rotation,
  };
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
      !isModalOwningInput &&
      !isGameTerminal
    ),
    gameOver: isGameTerminal,
  };
  boostInputContextRef.current = boostInputContext;

  const sendBoostInputCommand = useCallback((command: BoostInputCommand) => {
    sendCommand(command);
  }, [sendCommand]);
  sendBoostCommandRef.current = sendBoostInputCommand;

  const handleConfirmActivity = useCallback(() => {
    if (!isGameInteractionActive || gameOver || localWasIdleKicked) {
      return;
    }
    sendCommand('PlayerActivity');
  }, [gameOver, isGameInteractionActive, localWasIdleKicked, sendCommand]);

  // Touch steering funnels into the exact same command path as the arrow
  // keys, including the Rust screen->game rotation mapping, so the d-pad can
  // never disagree with what the rotated renderer shows.
  const handleTouchSteer = useCallback((screenDirection: ScreenDirection) => {
    const wasm = getWasm();
    const { canSteer, rotation: steerRotation } = steerContextRef.current;
    if (!wasm || !canSteer || isModalOwningInputRef.current) {
      return;
    }
    const transformedDirection = wasm.screenDirectionToGame(
      screenDirection,
      steerRotation,
    ) as ScreenDirection;
    sendCommand({ Turn: { direction: transformedDirection } });
  }, [sendCommand]);

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

  const hudBoostPointer = useBoostPointerBinding(
    boostInputControllerRef,
    boostInputContextRef,
    sendBoostDecision,
  );
  // One binding per touch Boost button (right and left landscape clusters);
  // the controller counts concurrent holds so Boost ends on the last release.
  const touchBoostPointer = useBoostPointerBinding(
    boostInputControllerRef,
    boostInputContextRef,
    sendBoostDecision,
  );
  const touchBoostPointerLeft = useBoostPointerBinding(
    boostInputControllerRef,
    boostInputContextRef,
    sendBoostDecision,
  );

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
      const controller = boostInputControllerRef.current!;
      dispatch(
        isModalOwningInputRef.current
          ? controller.suppressModalKeyDown(event)
          : controller.handleKeyDown(event, boostInputContextRef.current),
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

  // Retiring the touch surface mid-hold (a tablet docking to a keyboard)
  // unmounts the NOS button together with its pointer-release edge, which
  // would otherwise leave the hold latched on forever. Drop held edges the
  // same way the blur safety net does; with nothing held this is a no-op.
  useEffect(() => {
    if (isTouchSurface) {
      return;
    }
    const controller = boostInputControllerRef.current;
    if (controller) {
      const decision = controller.releaseHeld(boostInputContextRef.current);
      if (decision.command) {
        sendBoostCommandRef.current(decision.command);
      }
    }
  }, [isTouchSurface]);

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

  const handleIdleKickToMenu = async () => {
    teardownGameSession();
    if (currentLobby) {
      try {
        await leaveLobby();
      } catch (error) {
        console.warn('Failed to leave lobby after inactivity removal:', error);
      }
    }
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

  // CrazyGames supplies its own fullscreen chrome and their QA rejects a
  // second in-game prompt; iPhones have no Fullscreen API at all. Everyone
  // else on a touch surface gets the toggle next to the touch controls.
  const showFullscreenControl =
    fullscreen.supported && !crazyGames.getSnapshot().isCrazyGamesBuild;

  const touchControls = isTouchSurface && !gameOver && isLocalUserPlaying ? (
    <TouchControls
      onSteer={handleTouchSteer}
      boost={boostConfig && localSnake && boostHud ? {
        hud: boostHud,
        inputMode: boostInputMode,
        onTap: handleBoostButtonPress,
        primary: touchBoostPointer,
        secondary: touchBoostPointerLeft,
      } : null}
      fullscreen={showFullscreenControl
        ? { active: fullscreen.active, onToggle: fullscreen.toggle }
        : null}
    />
  ) : null;

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
        aria-valuetext={
          boostHud.unlimited
            ? `Unlimited${boostHud.active ? ', active' : ''}`
            : `${boostHud.percent}%${boostHud.active ? ', active' : ''}`
        }
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
        onPointerDown={hudBoostPointer.onPointerDown}
        onPointerUp={hudBoostPointer.onPointerRelease}
        onPointerCancel={hudBoostPointer.onPointerRelease}
        onLostPointerCapture={hudBoostPointer.onPointerRelease}
        disabled={boostButtonDisabled}
        aria-label={boostInputMode === 'hold'
          ? (boostHud.active
              ? `Release Boost, ${boostHud.unlimited ? 'unlimited' : `${boostHud.percent}% remaining`}`
              : `Hold to Boost, ${boostHud.unlimited ? 'unlimited' : `${boostHud.percent}% charged`}`)
          : (boostHud.active
              ? `Stop Boost, ${boostHud.unlimited ? 'unlimited' : `${boostHud.percent}% remaining`}`
              : `Activate Boost, ${boostHud.unlimited ? 'unlimited' : `${boostHud.percent}% charged`}`)}
        aria-keyshortcuts="Space"
        className="game-boost-meter"
        data-testid="boost-button"
      >
        <span className="game-boost-meter__canister-dock" aria-hidden="true">
          <BoostCanisterMark />
        </span>
        <span className="game-boost-meter__reservoir" aria-hidden="true" />
        <strong className="game-boost-meter__value">
          {boostHud.unlimited ? '∞' : `${boostHud.percent}%`}
        </strong>
      </button>
    </div>
  ) : null;

  // Keep the live region mounted, but render its visual burst only while the
  // engine-owned combo window has reached an enhanced tier and the gameplay
  // surface is active. The chain count is the animation identity so every
  // capped +3 pickup still re-pops.
  const comboCallout = localSnake && comboHud ? (
    <ComboCallout
      hud={comboHud}
      isVisible={Boolean(
        isArenaVisible &&
        isGameInteractionActive &&
        !isGameTerminal &&
        !isWaitingForSnapshot &&
        !currentGameLoadFailure &&
        !connectionStale &&
        !idleWarning &&
        !showCountdown &&
        !isModalOwningInput
      )}
      pickupIdentity={`${gameId}:${localSnake.combo.chain_count}`}
    />
  ) : null;

  if (showAuthLoading) {
    return <LoadingScreen message={authLoading ? 'Authenticating...' : 'Please sign in to play'} />;
  }
  
  return (
    <div
      className={`game-arena-screen fixed inset-0 flex flex-col overflow-hidden${
        isTouchSurface ? ' is-touch-surface' : ''
      }`}
      style={isTouchSurface ? {
        '--touch-portrait-reserve': `${TOUCH_PORTRAIT_BOTTOM_RESERVE_PX}px`,
        '--touch-side-reserve': `${TOUCH_LANDSCAPE_SIDE_RESERVE_PX}px`,
      } as React.CSSProperties : undefined}
    >

      <>
        <GameHudShell
          gameState={committedState}
          isVisible={isArenaVisible}
          arenaWidth={panelSize.width}
          currentUserId={user?.id}
          queueMode={queueMode}
          rating={matchRating}
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
              {comboCallout}
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

              {idleWarning && (
                <IdleWarningBanner
                  key={`${gameId}:${idleWarning.deadlineTick}`}
                  warning={idleWarning}
                  onConfirmActivity={handleConfirmActivity}
                />
              )}

              {/* Countdown Overlay */}
              {showCountdown && countdownState && (
                <div
                  className="absolute inset-0 flex flex-col items-center justify-center bg-black/30 z-10"
                  role="status"
                  aria-live="polite"
                  aria-atomic="true"
                >
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
            inputSurface={inputSurface}
            onBoostInputModeChange={handleBoostInputModeChange}
            // Withheld while inactivity owns the screen: the help modal would
            // be closed again on the next commit, so offering it reads as a
            // broken button rather than a deliberate refusal.
            onOpenHelp={tutorial && !inactivityNeedsTheScreen
              ? () => setHelpOpen(true)
              : undefined}
          />
        </div>

      </>
      {touchControls}
      {tutorial && (
        <TutorialModal
          open={showBriefing || showHelp}
          content={tutorial}
          variant={showBriefing ? 'briefing' : 'reference'}
          autoReadySeconds={
            showBriefing && readyDeadlineMs !== null
              ? Math.max(0, Math.ceil((readyDeadlineMs - Date.now()) / 1000))
              : null
          }
          pendingCount={pendingReadyCount}
          isReady={localUserIsReady || hasPressedReady}
          onReady={() => confirmReady('manual')}
          onClose={() => setHelpOpen(false)}
        />
      )}
      <ChatPanel
        title="Game Chat"
        messages={gameChatMessages}
        onSendMessage={handleSendGameChat}
        currentUsername={user?.username}
        isActive={isGameInteractionActive}
        inactiveMessage={localWasIdleKicked
          ? 'Game chat unavailable after removal for inactivity'
          : 'Game chat unavailable while the game is synchronizing'}
        initialExpanded={true}
        autoOpenEligible={false}
      />
      <IdleKickDialog open={showIdleKickDialog} onMenu={handleIdleKickToMenu} />
    </div>
  );
}
