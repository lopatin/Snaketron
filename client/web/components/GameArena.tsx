import React, { useEffect, useRef, useState, useCallback, useReducer } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useGameWebSocket } from '../hooks/useGameWebSocket';
import { useGameEngine } from '../hooks/useGameEngine';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import { GameState, CanvasRef, ArenaRotation } from '../types';
import * as wasm from 'wasm-snaketron';
import Scoreboard from './Scoreboard';
import LoadingScreen from './LoadingScreen';

export default function GameArena() {
  const { gameId } = useParams();
  if (!gameId) {
    throw new Error('GameArena must be used with a gameId parameter');
  }

  const navigate = useNavigate();
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  
  // Track mount state for React strict mode
  const isMountedRef = useRef(false);
  const hasJoinedGameRef = useRef(false);
  
  const {
    connected,
    sendGameCommand,
    joinGame,
    lastGameEvent,
    leaveGame,
    queueForMatch,
    createSoloGame
  } = useGameWebSocket();

  const { user, loading: authLoading } = useAuth();
  const { latencyMs } = useWebSocket();
  
  // Early return if auth is not ready - before useGameEngine
  if (authLoading || !user) {
    return <LoadingScreen message={authLoading ? 'Authenticating...' : 'Please log in to play'} />;
  }
  
  // Use game engine for client-side prediction
  // Now user.id is guaranteed to exist
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
    playerId: user.id,
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

  // Join game when user becomes available AND WebSocket is connected
  useEffect(() => {
    if (user && gameId && connected && !hasJoinedGameRef.current) {
      console.log('User authenticated and WebSocket connected, joining game:', gameId);
      hasJoinedGameRef.current = true;
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

      hasJoinedGameRef.current = false; // Reset for next mount
      rotationSetRef.current = false; // Reset rotation flag for next game
      leaveGame();
      stopEngine();
    };
  }, []);


  // Calculate optimal cell size and canvas dimensions
  useEffect(() => {
    const calculateSizes = () => {
      if (!gameState || !gameState.arena) return;
      
      const gridWidth = gameState.arena.width || 40;
      const gridHeight = gameState.arena.height || 40;
      
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
  }, [gameState, rotation]);

  // Check for game completion
  useEffect(() => {
    // Check if committed state is Complete (from useGameEngine)
    console.log('isGameComplete:', isGameComplete, 'gameOver:', gameOver);
    if (isGameComplete && !gameOver) {
      console.log('Game complete (from committed state), showing game over UI');
      setGameOver(true);
      stopEngine(); // Stop the engine when game ends
      setShowGameOverPanel(true);
      // debugger;
      // Show game over panel after a short delay
      // setTimeout(() => setShowGameOverPanel(true), 500);
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
    if (gameState && user?.id && !rotationSetRef.current) {
      const player = gameState.players?.[user.id];
      if (player) {
        const snakeId = player.snake_id;
        
        // In team games, team 0 has their endzone on the left, team 1 on the right
        // We determine team by snake_id: even indices are team 0, odd indices are team 1
        const teamId = snakeId % 2;
        
        if (teamId === 0) {
          // Team 0: endzone is on the left - rotate 270° so it appears at bottom
          setRotation(270);
        } else {
          // Team 1: endzone is on the right - rotate 90° so it appears at bottom
          setRotation(90);
        }
        
        // Mark rotation as set so we don't recalculate
        rotationSetRef.current = true;
      }
    }
  }, [gameState, user?.id]); // Only run when gameState or user changes

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
              // debugger; // Enter debugger when non-adjacent movement is detected
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
    if (!gameState) return;
    
    const intervalId = setInterval(() => {
      const timeLeft = gameState.start_ms - Date.now();
      if (timeLeft <= 0) {
        clearInterval(intervalId);
      } else {
        // Force re-render to update countdown
        forceUpdate();
      }
    }, 100); // Update every 100ms for smooth countdown
    
    return () => clearInterval(intervalId);
  }, [gameState, forceUpdate]);
  
  // Show loading screen while waiting for game state
  if (!gameState) {
    return <LoadingScreen message="Joining Game..." />;
  }
  
  // Calculate countdown from game start time or round start time
  let timeUntilStart = gameState.start_ms - Date.now();

  // For round transitions, use the latest round start time
  if (gameState.is_transitioning && gameState.round_start_times && gameState.round_start_times.length > 0) {
    const latestRoundStartTime = gameState.round_start_times[gameState.round_start_times.length - 1];
    timeUntilStart = latestRoundStartTime - Date.now();
  }

  const countdownSeconds = Math.ceil(timeUntilStart / 1000);
  const showCountdown = countdownSeconds > 0;
  
  // Handle back to menu
  const handleBackToMenu = () => {
    navigate('/');
  };
  
  // Handle play again
  const handlePlayAgain = () => {
    if (!gameState) return;
    
    const gameType = gameState.game_type;
    
    // Navigate away to trigger unmount and natural cleanup
    // Pass the game type as state so we know what to queue for
    navigate('/queue', { 
      state: { 
        gameType,
        autoQueue: true 
      } 
    });
  };
  
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
        />

        {/* Game Arena Container */}
        <div className="flex-1 flex flex-col items-center justify-center p-4" style={{ paddingTop: '120px', paddingBottom: '40px' }}>
          {/* Game Canvas */}
          <div
            className={`panel bg-white overflow-hidden transition-opacity duration-400 ease-out ${
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
            
            {/* Countdown Overlay */}
            {showCountdown && (
              <div className="absolute inset-0 flex flex-col items-center justify-center bg-black/30 z-10">
                {gameState.is_transitioning && (
                  <div className="text-white font-bold text-3xl mb-4" style={{
                    textShadow: '0 2px 4px rgba(0,0,0,0.5)'
                  }}>
                    {gameState.current_round > 1 ?
                      `Round ${gameState.current_round}` :
                      'Round 1'
                    }
                  </div>
                )}
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
    </div>
  );
}