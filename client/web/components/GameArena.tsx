import React, { useEffect, useRef, useState, useCallback, useReducer } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useGameWebSocket } from '../hooks/useGameWebSocket';
import { useGameEngine } from '../hooks/useGameEngine';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import { GameState, CanvasRef } from '../types';
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
  
  const [score, setScore] = useState(0);
  const [gameOver, setGameOver] = useState(false);
  const [showGameOverPanel, setShowGameOverPanel] = useState(false);
  const [cellSize, setCellSize] = useState(15);
  const [canvasSize, setCanvasSize] = useState({ width: 600, height: 600 });
  const [panelSize, setPanelSize] = useState({ width: 610, height: 610 });
  const [isArenaVisible, setIsArenaVisible] = useState(false);
  const [, forceUpdate] = useReducer(x => x + 1, 0);
  const lastHeadPositionRef = useRef<{ x: number; y: number } | null>(null);

  // Join game when user becomes available
  useEffect(() => {
    if (user && gameId && !hasJoinedGameRef.current) {
      console.log('User authenticated, joining game:', gameId);
      hasJoinedGameRef.current = true;
      joinGame(gameId);
    }
  }, [user, gameId, joinGame]);


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
      
      // Account for scoreboard (~80px), bottom padding (40px), 
      // container padding (2*16px), and panel border+shadow (~10px)
      const availableHeight = vh - 160 - 32 - 10;
      const availableWidth = vw - 100 - 32 - 10;
      
      // Start with max cell size and reduce until it fits
      let optimalCellSize = 15;
      let canvasWidth = gridWidth * optimalCellSize;
      let canvasHeight = gridHeight * optimalCellSize;
      
      // Reduce cell size by 1px until canvas fits in available space
      while ((canvasWidth > availableWidth || canvasHeight > availableHeight) && optimalCellSize > 5) {
        optimalCellSize--;
        canvasWidth = gridWidth * optimalCellSize;
        canvasHeight = gridHeight * optimalCellSize;
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
  }, [gameState]);

  // Update score when game state changes
  useEffect(() => {
    if (gameState && user?.id) {
      const player = gameState.players?.[user.id];
      if (player) {
        const snake = gameState.arena.snakes.find(s => s.body.length > 0);
        if (snake) {
          const newScore = Math.max(0, snake.body.length - 2);
          setScore(newScore);
          
          // Check if snake is dead
          if (!snake.is_alive && !gameOver) {
            setGameOver(true);
            // Show game over panel after a short delay
            setTimeout(() => setShowGameOverPanel(true), 500);
          }
        }
      }
    }
    
    // Also check if game status is Complete
    if (gameState && !gameOver) {
      const status = gameState.status;
      const isComplete = (typeof status === 'object' && 'Complete' in status);
      if (isComplete) {
        setGameOver(true);
        stopEngine(); // Stop the engine when game ends
        // Show game over panel after a short delay
        setTimeout(() => setShowGameOverPanel(true), 500);
      }
    }
  }, [gameState, user?.id, gameOver, stopEngine]);

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
        console.log('Keydown event - sending turn command:', direction, 'repeat:', e.repeat, 'timestamp:', Date.now());
        
        // Send command through game engine (handles both local prediction and server)
        sendCommand({
          Turn: { direction: direction as 'Up' | 'Down' | 'Left' | 'Right' }
        });
        
        console.log('sendCommand call completed at:', Date.now());
      }
    };
    
    window.addEventListener('keydown', handleKeyPress);
    return () => window.removeEventListener('keydown', handleKeyPress);
  }, [sendCommand, gameOver, connected, gameState]);
  
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
        
        wasm.render_game(JSON.stringify(gameState), canvasRef.current!, cellSize, user?.id || null);
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
  }, [gameState, cellSize]);
  
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
  
  // Calculate countdown from game start time
  const timeUntilStart = gameState.start_ms - Date.now();
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
          gameState={gameState} 
          score={score} 
          isVisible={isArenaVisible} 
          currentUserId={user?.id}
          showGameOver={showGameOverPanel}
          onBackToMenu={handleBackToMenu}
          onPlayAgain={handlePlayAgain}
        />

        {/* Game Arena Container */}
        <div className="flex-1 flex flex-col items-center justify-center p-4" style={{ paddingTop: '80px', paddingBottom: '40px' }}>
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
              <div className="absolute inset-0 flex items-center justify-center bg-black/30 z-10">
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