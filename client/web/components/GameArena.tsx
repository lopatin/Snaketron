import { useEffect, useRef, useState, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useGameWebSocket } from '../hooks/useGameWebSocket';
import { useAuth } from '../contexts/AuthContext.jsx';

export default function GameArena() {
  const { gameId } = useParams();
  const navigate = useNavigate();
  const canvasRef = useRef(null);
  const gameClientRef = useRef(null);
  const animationRef = useRef(null);
  const gameLoopRef = useRef(null);
  const lastUpdateRef = useRef(Date.now());
  const pendingDirectionRef = useRef(null);
  
  const { gameState, sendCommand, connected, updateGameState } = useGameWebSocket();
  const { user } = useAuth();
  const [score, setScore] = useState(0);
  const [gameOver, setGameOver] = useState(false);
  const [localGameState, setLocalGameState] = useState(null);
  
  // Initialize local game state from WebSocket game state
  useEffect(() => {
    if (gameState && !localGameState) {
      setLocalGameState(JSON.parse(JSON.stringify(gameState)));
    }
  }, [gameState, localGameState]);

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
      
      if (opposites[currentDirection] !== newDirection) {
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
    const handleKeyPress = (e) => {
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
            ChangeDirection: { direction }
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
        // Convert gameState to JSON string as expected by WASM
        const gameStateJson = JSON.stringify(currentState);
        window.wasm.render_game(gameStateJson, canvasRef.current);
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
  }, [localGameState, gameState]);
  
  // Handle game events from server (if connected)
  useEffect(() => {
    if (!gameState) return;
    
    // If we get updates from server, sync with local state
    if (connected && gameState && !localGameState) {
      console.log('Syncing server game state to local state');
      setLocalGameState(JSON.parse(JSON.stringify(gameState)));
    }
  }, [gameState, connected, localGameState]);
  
  // Show connecting message only if we don't have any game state yet
  if (!gameState && !localGameState) {
    return (
      <div className="flex-1 flex items-center justify-center bg-gray-900">
        <div className="text-white text-xl">
          {connected ? "Loading game..." : "Starting offline game..."}
        </div>
      </div>
    );
  }
  
  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-900">
      <div className="text-white mb-4">
        <span className="text-2xl font-bold">Score: {score}</span>
      </div>
      
      <canvas 
        ref={canvasRef}
        width={800} 
        height={800}
        className="border-2 border-gray-600 bg-gray-800"
      />
      
      {gameOver && (
        <div className="absolute inset-0 bg-black/50 flex items-center justify-center">
          <div className="bg-white p-8 rounded-lg text-center">
            <h2 className="text-3xl font-bold mb-4">Game Over!</h2>
            <p className="text-xl mb-6">Final Score: {score}</p>
            <button
              onClick={() => navigate('/')}
              className="px-6 py-3 bg-blue-500 text-white rounded hover:bg-blue-600 transition-colors"
            >
              Play Again
            </button>
          </div>
        </div>
      )}
      
      <div className="text-white mt-4 text-sm">
        Use arrow keys to control your snake
      </div>
    </div>
  );
}