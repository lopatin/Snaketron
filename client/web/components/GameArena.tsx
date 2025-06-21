import React, { useEffect, useRef, useState, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useGameWebSocket } from '../hooks/useGameWebSocket';
import { useGameEngine } from '../hooks/useGameEngine';
import { useAuth } from '../contexts/AuthContext';
import { GameState, CanvasRef } from '../types';
import * as wasm from 'wasm-snaketron';
import Scoreboard from './Scoreboard';

export default function GameArena() {
  const { gameId } = useParams();
  const navigate = useNavigate();
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  
  const { gameState: serverGameState, sendCommand: sendServerCommand, connected, sendGameCommand } = useGameWebSocket();
  const { user } = useAuth();
  
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
    }
  });
  
  const [score, setScore] = useState(0);
  const [gameOver, setGameOver] = useState(false);
  const [cellSize, setCellSize] = useState(15);
  const [canvasSize, setCanvasSize] = useState({ width: 600, height: 600 });
  const [panelSize, setPanelSize] = useState({ width: 610, height: 610 });
  const [isArenaVisible, setIsArenaVisible] = useState(false);
  
  // Start game engine when server state is available and game is not ended
  useEffect(() => {
    console.log('GameArena - serverGameState:', !!serverGameState, 'isRunning:', isRunning);
    if (serverGameState && !isRunning) {
      console.log('GameArena - Starting engine with server state, status:', serverGameState.status);
      // Only start if the game is not ended
      if (!('Ended' in serverGameState.status)) {
        startEngine();
      } else {
        console.log('GameArena - Game is ended, not starting engine');
      }
    }
  }, [serverGameState, isRunning, startEngine]);
  
  // Stop engine on unmount
  useEffect(() => {
    return () => {
      stopEngine();
    };
  }, [stopEngine]);

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
          }
        }
      }
    }
  }, [gameState, user?.id, gameOver]);

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
        console.log('Sending turn command:', direction);
        
        // Send command through game engine (handles both local prediction and server)
        sendCommand({
          Turn: { direction: direction as 'Up' | 'Down' | 'Left' | 'Right' }
        });
      }
    };
    
    window.addEventListener('keydown', handleKeyPress);
    return () => window.removeEventListener('keydown', handleKeyPress);
  }, [sendCommand, gameOver, connected]);
  
  // Render game state
  useEffect(() => {
    console.log('Render effect - gameState:', !!gameState, 'canvas:', !!canvasRef.current, 'wasm:', !!window.wasm);
    
    if (!gameState || !canvasRef.current || !window.wasm) {
      if (!window.wasm) console.log('Waiting for WASM to load...');
      if (!gameState) console.log('Waiting for game state...');
      return;
    }
    
    let animationId: number;
    const render = () => {
      try {
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
    if (!serverGameState) return;
    
    // The game engine will handle server state updates internally
    // through the WebSocket hook integration
    console.log('Server game state updated');
  }, [serverGameState]);
  
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