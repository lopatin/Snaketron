import React, { useEffect, useRef, useState, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useGameWebSocket } from '../hooks/useGameWebSocket';
import { useGameEngine } from '../hooks/useGameEngine';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import { GameState, CanvasRef } from '../types';
import * as wasm from 'wasm-snaketron';
import Scoreboard from './Scoreboard';

export default function GameArena() {
  const { gameId } = useParams();
  const navigate = useNavigate();
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  
  // Track mount state for React strict mode
  const isMountedRef = useRef(false);
  const hasJoinedGameRef = useRef(false);
  
  const {
    gameState: serverGameState,
    sendCommand: sendServerCommand,
    connected, sendGameCommand,
    lastGameEvent,
    leaveGame
  } = useGameWebSocket();

  const { user } = useAuth();
  const { latencyMs } = useWebSocket();
  
  // Use game engine for client-side prediction
  const {
    gameEngine,
    gameState,
    isRunning,
    sendCommand,
    processServerEvent,
    startEngine,
    stopEngine
  } = useGameEngine({
    gameId: gameId || '0',
    playerId: user?.id,
    initialState: serverGameState || undefined,
    onCommandReady: (commandMessage) => {
      // Send command to server
      sendGameCommand(commandMessage);
    },
    latencyMs
  });
  
  const [score, setScore] = useState(0);
  const [gameOver, setGameOver] = useState(false);
  const [cellSize, setCellSize] = useState(15);
  const [canvasSize, setCanvasSize] = useState({ width: 600, height: 600 });
  const [panelSize, setPanelSize] = useState({ width: 610, height: 610 });
  const [isArenaVisible, setIsArenaVisible] = useState(false);
  const lastHeadPositionRef = useRef<{ x: number; y: number } | null>(null);
  
  // Start game engine when server state is available and game is not ended
  useEffect(() => {
    console.log('GameArena - serverGameState:', !!serverGameState, 'isRunning:', isRunning);
    if (serverGameState && !isRunning) {
      console.log('GameArena - Starting engine with server state, status:', serverGameState.status);
      // Only start if the game is not completed
      const status = serverGameState.status;
      const isComplete = (typeof status === 'object' && 'Complete' in status) || status === 'Stopped';
      if (!isComplete) {
        startEngine();
      } else {
        console.log('GameArena - Game is stopped or completed, not starting engine');
      }
    }
  }, [serverGameState, isRunning, startEngine]);
  
  // Track mount state and handle cleanup with React strict mode protection
  useEffect(() => {
    // Mark as mounted
    isMountedRef.current = true;
    
    // Track that we've joined this game
    if (gameId) {
      hasJoinedGameRef.current = true;
      console.log('GameArena mounted for game:', gameId);
    }
    
    return () => {
      // Check if this is a real unmount or just strict mode re-render
      isMountedRef.current = false;
      
      // Use a timeout to check if we're really unmounting
      setTimeout(() => {
        if (!isMountedRef.current && hasJoinedGameRef.current) {
          console.log('GameArena unmounting - sending LeaveGame');
          leaveGame();
          hasJoinedGameRef.current = false;
        }
      }, 0);
      
      // Always stop the engine on unmount
      stopEngine();
    };
  }, [gameId, leaveGame, stopEngine]);

  // Trigger fade-in animation when component mounts and hide background dots
  useEffect(() => {
    // Hide background dots
    document.body.classList.add('hide-background-dots');
    
    const timer = setTimeout(() => {
      setIsArenaVisible(true);
    }, 300); // Delay to ensure smooth transition after fade-out

    return () => {
      clearTimeout(timer);
      // Restore background dots when leaving game view
      document.body.classList.remove('hide-background-dots');
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
      
      // Account for scoreboard (~140px), bottom padding (40px), 
      // container padding (2*16px), and panel border+shadow (~10px)
      const availableHeight = vh - 220 - 32 - 10;
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
            // stopEngine(); // Stop the engine when game ends
          }
        }
      }
    }
    
    // Also check if game status is Complete
    if (gameState && !gameOver) {
      const status = gameState.status;
      const isComplete = (typeof status === 'object' && 'Complete' in status) || status === 'Stopped';
      if (isComplete) {
        setGameOver(true);
        // stopEngine(); // Stop the engine when game ends
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
        console.log('rendering game state:', JSON.stringify(gameState.arena.snakes[0].body));
        
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
              debugger; // Enter debugger when non-adjacent movement is detected
            }
          }
          
          // Update last head position
          lastHeadPositionRef.current = { x: currentHead.x, y: currentHead.y };
        }
        
        wasm.render_game(JSON.stringify(gameState), canvasRef.current!, cellSize);
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
  
  // Don't show any loading message - just render empty until game state is ready
  if (!gameState) {
    return null;
  }
  
  return (
    <div className="fixed inset-0 flex flex-col overflow-hidden">
      {/* Scoreboard */}
      <Scoreboard gameState={gameState} score={score} isVisible={isArenaVisible} />
      
      {/* Game Arena */}
      <div className="flex-1 flex items-center justify-center p-4" style={{ paddingTop: '140px', paddingBottom: '40px' }}>
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
            justifyContent: 'center'
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
        </div>
      </div>
      
      {gameOver && (
        <div className="absolute inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="panel bg-white p-8 text-center">
            <h2 className="text-3xl font-black italic uppercase tracking-1 mb-4 text-black-70">Game Over!</h2>
            <p className="text-xl mb-6 text-black-70 font-bold">Final Score: {score}</p>
            <button
              onClick={() => navigate('/')}
              className="btn-primary"
            >
              Play Again
            </button>
          </div>
        </div>
      )}
    </div>
  );
}