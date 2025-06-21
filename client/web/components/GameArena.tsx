import React, { useEffect, useRef, useState, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useGameWebSocket } from '../hooks/useGameWebSocket';
import { useAuth } from '../contexts/AuthContext';
import { GameState, CanvasRef } from '../types';
import * as wasm from 'wasm-snaketron';
import Scoreboard from './Scoreboard';

export default function GameArena() {
  const { gameId } = useParams();
  const navigate = useNavigate();
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const gameClientRef = useRef<any>(null);
  const animationRef = useRef<number | null>(null);
  const gameLoopRef = useRef<number | null>(null);
  const lastUpdateRef = useRef(Date.now());
  const pendingDirectionRef = useRef<string | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  
  const { gameState, sendCommand, connected } = useGameWebSocket();
  const { user } = useAuth();
  const [score, setScore] = useState(0);
  const [gameOver, setGameOver] = useState(false);
  const [localGameState, setLocalGameState] = useState<GameState | null>(null);
  const [cellSize, setCellSize] = useState(15);
  const [canvasSize, setCanvasSize] = useState({ width: 600, height: 600 });
  const [panelSize, setPanelSize] = useState({ width: 610, height: 610 });
  const [isArenaVisible, setIsArenaVisible] = useState(false);
  
  // Initialize local game state from WebSocket game state
  useEffect(() => {
    if (gameState && !localGameState) {
      setLocalGameState(JSON.parse(JSON.stringify(gameState)));
    }
  }, [gameState, localGameState]);

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
      const currentState = localGameState || gameState;
      if (!currentState || !currentState.arena) return;
      
      const gridWidth = currentState.arena.width || 40;
      const gridHeight = currentState.arena.height || 40;
      
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
  }, [localGameState, gameState]);

  // Client-side game loop
  const updateLocalGameState = useCallback(() => {
    if (!localGameState || gameOver) return;

    const newGameState = JSON.parse(JSON.stringify(localGameState));
    
    // Apply pending direction change
    if (pendingDirectionRef.current && newGameState.arena.snakes[0]) {
      const currentDirection = newGameState.arena.snakes[0].direction;
      const newDirection = pendingDirectionRef.current;
      
      // Prevent reverse direction
      const opposites = {
        'Up': 'Down',
        'Down': 'Up', 
        'Left': 'Right',
        'Right': 'Left'
      };
      
      if (opposites[currentDirection as keyof typeof opposites] !== newDirection) {
        newGameState.arena.snakes[0].direction = newDirection;
        console.log('Direction changed to:', newDirection);
      }
      
      pendingDirectionRef.current = null;
    }

    // Move snake
    if (newGameState.arena.snakes[0] && newGameState.arena.snakes[0].is_alive) {
      const snake = newGameState.arena.snakes[0];
      const head = snake.body[0];
      const direction = snake.direction;
      
      let newHead = { ...head };
      
      switch(direction) {
        case 'Up': newHead.y = head.y - 1; break;
        case 'Down': newHead.y = head.y + 1; break;
        case 'Left': newHead.x = head.x - 1; break;
        case 'Right': newHead.x = head.x + 1; break;
      }
      
      // Check wall collision
      if (newHead.x < 0 || newHead.x >= newGameState.arena.width || 
          newHead.y < 0 || newHead.y >= newGameState.arena.height) {
        snake.is_alive = false;
        setGameOver(true);
        console.log('Game over: wall collision');
        return;
      }
      
      // Update snake body (simplified - just move head and tail)
      snake.body = [newHead, ...snake.body.slice(0, -1)];
      
      // Update score based on snake length
      const newScore = Math.max(0, snake.body.length - 2);
      setScore(newScore);
    }
    
    newGameState.tick += 1;
    setLocalGameState(newGameState);
  }, [localGameState, gameOver]);

  // Start game loop
  useEffect(() => {
    if (!localGameState || gameOver) return;
    
    const gameLoop = () => {
      const now = Date.now();
      if (now - lastUpdateRef.current > 200) { // Update every 200ms
        updateLocalGameState();
        lastUpdateRef.current = now;
      }
      gameLoopRef.current = requestAnimationFrame(gameLoop);
    };
    
    gameLoopRef.current = requestAnimationFrame(gameLoop);
    
    return () => {
      if (gameLoopRef.current) {
        cancelAnimationFrame(gameLoopRef.current);
      }
    };
  }, [localGameState, gameOver, updateLocalGameState]);

  useEffect(() => {
    if (!window.wasm) {
      console.log('WASM not loaded yet');
      return;
    }
    
    // Handle keyboard input
    const handleKeyPress = (e: KeyboardEvent) => {
      if (gameOver) return;
      
      let direction = null;
      switch(e.key) {
        case 'ArrowUp': direction = 'Up'; break;
        case 'ArrowDown': direction = 'Down'; break;
        case 'ArrowLeft': direction = 'Left'; break;
        case 'ArrowRight': direction = 'Right'; break;
      }
      
      if (direction) {
        e.preventDefault();
        console.log('Setting pending direction:', direction);
        pendingDirectionRef.current = direction;
        
        // Also send to server if connected
        if (connected) {
          sendCommand({
            Turn: { direction: direction as 'Up' | 'Down' | 'Left' | 'Right' }
          });
        }
      }
    };
    
    window.addEventListener('keydown', handleKeyPress);
    return () => window.removeEventListener('keydown', handleKeyPress);
  }, [sendCommand, gameOver, connected]);
  
  // Render game state
  useEffect(() => {
    const currentState = localGameState || gameState;
    console.log('Render effect - currentState:', !!currentState, 'canvas:', !!canvasRef.current, 'wasm:', !!window.wasm);
    
    if (!currentState || !canvasRef.current || !window.wasm) {
      if (!window.wasm) console.log('Waiting for WASM to load...');
      if (!currentState) console.log('Waiting for game state...');
      return;
    }
    
    const render = () => {
      try {
        wasm.render_game(JSON.stringify(currentState), canvasRef.current!, cellSize);
      } catch (error) {
        console.error('Error rendering game:', error);
      }
      animationRef.current = requestAnimationFrame(render);
    };
    
    render();
    
    return () => {
      if (animationRef.current) {
        cancelAnimationFrame(animationRef.current);
      }
    };
  }, [localGameState, gameState, cellSize]);
  
  // Handle game events from server (if connected)
  useEffect(() => {
    if (!gameState) return;
    
    // If we get updates from server, sync with local state
    if (connected && gameState && !localGameState) {
      console.log('Syncing server game state to local state');
      setLocalGameState(JSON.parse(JSON.stringify(gameState)));
    }
  }, [gameState, connected, localGameState]);
  
  // Don't show any loading message - just render empty until game state is ready
  if (!gameState && !localGameState) {
    return null;
  }
  
  return (
    <div className="fixed inset-0 flex flex-col overflow-hidden">
      {/* Scoreboard */}
      <Scoreboard gameState={localGameState || gameState} score={score} isVisible={isArenaVisible} />
      
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