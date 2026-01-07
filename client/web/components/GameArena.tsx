import React, { useEffect, useRef, useState, useCallback, useReducer } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useGameWebSocket } from '../hooks/useGameWebSocket';
import { useGameEngine } from '../hooks/useGameEngine';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import { GameState, CanvasRef, ArenaRotation, GameType, LobbyGameMode, QueueMode } from '../types';
import * as wasm from 'wasm-snaketron';
import Scoreboard from './Scoreboard';
import LoadingScreen from './LoadingScreen';
import { LobbyChat as ChatPanel } from './LobbyChat';

export default function GameArena() {
  const { gameId } = useParams();
  if (!gameId) {
    throw new Error('GameArena must be used with a gameId parameter');
  }

  const navigate = useNavigate();
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const joinedGameIdRef = useRef<string | null>(null);
  const previousGameIdRef = useRef<string | null>(null);
  
  const {
    connected,
    sendGameCommand,
    joinGame,
    lastGameEvent,
    leaveGame,
    queueForMatch,
    queueForMatchMulti,
    isJoiningGame,
  } = useGameWebSocket();

  const { user, loading: authLoading } = useAuth();
  const { latencyMs, gameChatMessages, sendChatMessage, currentLobby, lobbyPreferences } = useWebSocket();
  const playerId = user?.id ?? 0;
  const queueMode: QueueMode = lobbyPreferences?.competitive ? 'Competitive' : 'Quickmatch';

  // Use game engine for client-side prediction (call unconditionally to keep hook order stable)
  const {
    gameEngine,
    gameState,
    committedState,
    isGameComplete,
    // isRunning,
    sendCommand,
    processServerEvent,
    stopEngine
  } = useGameEngine({
    gameId,
    playerId,
    onCommandReady: sendGameCommand,
    latencyMs
  });

  const [gameOver, setGameOver] = useState(false);
  const [showGameOverPanel, setShowGameOverPanel] = useState(false);
  const [cellSize, setCellSize] = useState(15);
  const [canvasSize, setCanvasSize] = useState({ width: 600, height: 600 });
  const [panelSize, setPanelSize] = useState({ width: 610, height: 610 });
  const [isArenaVisible, setIsArenaVisible] = useState(false);
  const [, forceUpdate] = useReducer(x => x + 1, 0);
  const lastHeadPositionRef = useRef<{ x: number; y: number } | null>(null);
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
      lastHeadPositionRef.current = null;
      setGameOver(false);
      setShowGameOverPanel(false);
    }

    previousGameIdRef.current = gameId;
    joinedGameIdRef.current = null;
  }, [gameId, leaveGame, stopEngine]);

  // Join game when user becomes available AND WebSocket is connected
  useEffect(() => {
    if (user && gameId && connected && joinedGameIdRef.current !== gameId) {
      console.log('User authenticated and WebSocket connected, joining game:', gameId);
      joinedGameIdRef.current = gameId;
      joinGame(gameId);
    }
  }, [user, gameId, connected, joinGame]);


  useEffect(() => {
    // Hide background dots
    document.body.classList.add('hide-background-dots');

    // Trigger fade-in animation when component mounts and hide background dots
    const timer = setTimeout(() => {
      setIsArenaVisible(true);
    }, 300); // Delay to ensure smooth transition after fade-out

    console.log('GAME ARENA MOUNTED, initial state:', gameState);

    return () => {
      clearTimeout(timer);
      // Restore background dots when leaving game view
      document.body.classList.remove('hide-background-dots');
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

  // Transform direction based on rotation
  // We need to apply the INVERSE transformation of the coordinate system
  // When arena is rotated 90° CW, UP on screen corresponds to LEFT in game coordinates
  const transformDirection = (direction: 'Up' | 'Down' | 'Left' | 'Right', rotation: ArenaRotation): 'Up' | 'Down' | 'Left' | 'Right' => {
    switch (rotation) {
      case 0:
        return direction;
      case 90:
        // 90° CW rotation: inverse is 270° CW
        // Screen Up → Game Left, Screen Right → Game Up, Screen Down → Game Right, Screen Left → Game Down
        switch (direction) {
          case 'Up': return 'Left';
          case 'Right': return 'Up';
          case 'Down': return 'Right';
          case 'Left': return 'Down';
        }
      case 180:
        // 180° rotation: inverse is also 180°
        // Screen Up → Game Down, Screen Down → Game Up, Screen Left → Game Right, Screen Right → Game Left
        switch (direction) {
          case 'Up': return 'Down';
          case 'Down': return 'Up';
          case 'Left': return 'Right';
          case 'Right': return 'Left';
        }
      case 270:
        // 270° CW rotation: inverse is 90° CW
        // Screen Up → Game Right, Screen Right → Game Down, Screen Down → Game Left, Screen Left → Game Up
        switch (direction) {
          case 'Up': return 'Right';
          case 'Right': return 'Down';
          case 'Down': return 'Left';
          case 'Left': return 'Up';
        }
    }
  };

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
    if (!window.wasm) {
      console.log('WASM not loaded yet');
      return;
    }
    
    // Handle keyboard input
    const handleKeyPress = (e: KeyboardEvent) => {
      // Ignore repeat events
      if (e.repeat) {
        return;
      }
      
      if (gameOver || !gameState) {
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
        const transformedDirection = transformDirection(originalDirection, rotation);
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
  }, [sendCommand, gameOver, connected, gameState, rotation]);
  
  // Render game state
  useEffect(() => {
    if (!gameState || !canvasRef.current || !window.wasm) {
      if (!window.wasm) console.log('Waiting for WASM to load...');
      if (!gameState) console.log('Waiting for game state...');
      return;
    }
    
    // Expose game state for testing
    if (process.env.NODE_ENV !== 'production') {
      (window as any).__gameArenaState = gameState;
    }
    
    let animationId: number;
    const render = () => {
      try {
        // console.log('rendering game state:', JSON.stringify(gameState.arena.snakes[0].body));
        
        // Check head position for non-adjacent movement
        if (gameState.arena.snakes.length > 0 && gameState.arena.snakes[0].body.length > 0) {
          const currentHead = gameState.arena.snakes[0].body[0];
          
          if (lastHeadPositionRef.current) {
            const dx = Math.abs(currentHead.x - lastHeadPositionRef.current.x);
            const dy = Math.abs(currentHead.y - lastHeadPositionRef.current.y);
            
            // Check if the head moved more than 1 cell (not adjacent)
            if ((dx > 1 || dy > 1) || (dx === 1 && dy === 1)) {
              console.error('Non-adjacent head movement detected!', {
                previous: lastHeadPositionRef.current,
                current: currentHead,
                dx,
                dy
              });
            }
          }

          // Update last head position
          lastHeadPositionRef.current = { x: currentHead.x, y: currentHead.y };
        }
        
        // Get opponent username for team games
        let opponentUsername: string | null = null;
        if (gameState.usernames && user?.id) {
          // Find the first opponent (different user ID)
          const opponentEntry = Object.entries(gameState.usernames).find(
            ([userId, _]) => parseInt(userId) !== user.id
          );
          if (opponentEntry) {
            opponentUsername = opponentEntry[1];
          }
        }

        wasm.render_game(
          JSON.stringify(gameState), 
          canvasRef.current!, 
          cellSize, 
          user?.id || null, 
          rotation,
          user?.username || null,
          opponentUsername
        );
      } catch (error) {
        console.error('Error rendering game:', error);
      }
      animationId = requestAnimationFrame(render);
    };
    
    render();
    
    return () => {
      if (animationId) {
        cancelAnimationFrame(animationId);
      }
    };
  }, [gameState, cellSize, rotation, user?.id]);
  
  // Process server events through game engine
  useEffect(() => {
    if (lastGameEvent && processServerEvent) {
      console.log('Processing server event in GameArena:', lastGameEvent);
      processServerEvent(lastGameEvent);
    }
  }, [lastGameEvent, processServerEvent]);
  
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
    if (!connected) {
      return;
    }
    sendChatMessage('game', message);
  }, [connected, sendChatMessage]);
  
  // Calculate countdown from game start time or round start time
  const countdownState = gameState ?? committedState;
  const isWaitingForSnapshot = !gameState;
  const waitingMessage = isJoiningGame ? 'Joining game...' : 'Preparing arena...';
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
  
  // Determine if user is in a lobby and is the host
  const isInLobby = currentLobby !== null;
  const isLobbyQueued = currentLobby?.state === 'queued';

  // Handle play again
  const handlePlayAgain = () => {
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
            {isWaitingForSnapshot && (
              <div className="absolute inset-0 flex flex-col items-center justify-center bg-white/80 z-20">
                <span className="w-6 h-6 border-2 border-gray-300 border-t-black rounded-full animate-spin mb-3" aria-hidden="true" />
                <span className="text-gray-600 font-semibold uppercase tracking-1 text-xs">
                  {waitingMessage}
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
        isActive={connected}
        inactiveMessage="Game chat unavailable"
        initialExpanded={true}
        autoOpenEligible={false}
      />
    </div>
  );
}
