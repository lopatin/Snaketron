import React, { useEffect, useState } from 'react';
import { GameState } from '../types';

interface ScoreboardProps {
  gameState: GameState | null;
  isVisible: boolean;
  currentUserId?: number;
  showGameOver?: boolean;
  onBackToMenu?: () => void;
  onPlayAgain?: () => void;
  isHost?: boolean;
  isInLobby?: boolean;
  isLobbyQueued?: boolean;
}

// Snake colors matching render.rs
const SNAKE_COLORS = [
  '#70bfe3', // Light blue/teal
  '#556270', // Dark gray
  '#ff6b6b', // Coral red
  '#f7b731', // Yellow/gold
];

const Scoreboard: React.FC<ScoreboardProps> = ({
  gameState,
  isVisible,
  currentUserId,
  onBackToMenu,
  onPlayAgain,
  isHost = false,
  isInLobby = false,
  isLobbyQueued = false
}) => {
  const [elapsedTime, setElapsedTime] = useState('00:00');
  const [remainingTime, setRemainingTime] = useState<string | null>(null);
  const [logoHovered, setLogoHovered] = useState(false);
  const [gameOverExpanded, setGameOverExpanded] = useState(false);

  const showGameOver = gameState != null && typeof gameState.status === 'object' &&
      gameState.status !== null &&
      'Complete' in gameState.status;

  // Trigger slide-in animation when showGameOver becomes true
  useEffect(() => {
    if (showGameOver) {
      const timer = setTimeout(() => setGameOverExpanded(true), 50);
      return () => clearTimeout(timer);
    } else {
      setGameOverExpanded(false);
    }
  }, [showGameOver]);

  // Determine if this is a solo game
  const isSoloGame = () => {
    if (!gameState) return true;
    
    // Check if GameType is 'Solo' (string)
    if (gameState.game_type === 'Solo') return true;
    
    // Check if GameType is Custom with Solo game_mode
    if (typeof gameState.game_type === 'object' && 'Custom' in gameState.game_type) {
      const customSettings = gameState.game_type.Custom.settings;
      if (customSettings.game_mode === 'Solo') return true;
    }
    
    // Check if GameType is TeamMatch
    if (typeof gameState.game_type === 'object' && 'TeamMatch' in gameState.game_type) {
      return false;
    }
    
    // Default to solo for other modes (FreeForAll, Duel, QuickPlay, Competitive)
    return true;
  };

  // Get game mode display text
  const getGameModeText = () => {
    if (!gameState) return '';
    
    const gameType = gameState.game_type;
    
    if (gameType === 'Solo') return 'Solo Game';
    
    if (typeof gameType === 'object') {
      if ('TeamMatch' in gameType) {
        const perTeam = gameType.TeamMatch.per_team;
        if (perTeam === 1) return 'Quick Match';
        return `Team Battle`;
      }
      if ('FreeForAll' in gameType) {
        return `Free For All`;
      }
      if ('Custom' in gameType) {
        const mode = gameType.Custom.settings.game_mode;
        if (mode === 'Solo') return 'Solo Game';
        if (mode === 'Duel') return 'Custom Duel';
        return 'Custom Game';
      }
    }
    
    return 'Multiplayer';
  };

  // Get snake info with player mapping
  const getSnakeInfo = () => {
    if (!gameState || !gameState.arena || !gameState.arena.snakes) {
      return [];
    }

    return gameState.arena.snakes.map((snake, index) => {
      // Find player for this snake
      const playerEntry = Object.entries(gameState.players || {}).find(
        ([_, player]) => player.snake_id === index
      );
      const userId = playerEntry ? parseInt(playerEntry[0]) : null;
      const isCurrentPlayer = userId === currentUserId;
      
      // Get username from game state or fall back to generic name
      const username = userId && gameState.usernames ? 
        gameState.usernames[userId] : null;
      
      return {
        index,
        snake,
        color: SNAKE_COLORS[index % SNAKE_COLORS.length],
        userId,
        isCurrentPlayer,
        name: isCurrentPlayer ? 'You' : (username || `Player ${index + 1}`),
        team: snake.team_id !== undefined && snake.team_id !== null ? snake.team_id + 1 : (index % 2 === 0 ? 1 : 2), // Use actual team_id if available (convert 0-based to 1-based)
      };
    });
  };

  // Calculate team stats for team games
  const getTeamStats = () => {
    const snakeInfo = getSnakeInfo();
    const team1Snakes = snakeInfo.filter(info => info.team === 1);
    const team2Snakes = snakeInfo.filter(info => info.team === 2);

    const team1Score = gameState?.team_scores?.[0] || 0;
    const team2Score = gameState?.team_scores?.[1] || 0;

    // Find which team the current player is on
    const currentPlayerInfo = snakeInfo.find(info => info.isCurrentPlayer);
    const currentPlayerTeam = currentPlayerInfo ? currentPlayerInfo.team : null;

    return {
      team1: {
        snakes: team1Snakes,
        score: team1Score,
        alive: team1Snakes.filter(info => info.snake.is_alive).length,
        total: team1Snakes.length
      },
      team2: {
        snakes: team2Snakes,
        score: team2Score,
        alive: team2Snakes.filter(info => info.snake.is_alive).length,
        total: team2Snakes.length
      },
      currentPlayerTeam
    };
  };

  const formatMs = (ms: number) => {
    const totalSeconds = Math.max(0, Math.floor(ms / 1000));
    const minutes = Math.floor(totalSeconds / 60);
    const seconds = totalSeconds % 60;
    return `${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`;
  };

  // Calculate elapsed time from game ticks (pure function of game state)
  useEffect(() => {
    if (!gameState) return;

    // If game hasn't started yet (countdown phase)
    if (Date.now() < gameState.start_ms) {
      setElapsedTime('00:00');
      const limit = gameState.properties?.time_limit_ms ?? null;
      setRemainingTime(limit ? formatMs(limit) : null);
      return;
    }

    // Calculate elapsed time from ticks that have occurred since start
    // The tick count represents the actual game progress
    const tick_duration_ms = gameState.properties?.tick_duration_ms || 100;
    const elapsedMs = gameState.tick * tick_duration_ms;
    setElapsedTime(formatMs(elapsedMs));

    const limit = gameState.properties?.time_limit_ms ?? null;
    if (limit) {
      setRemainingTime(formatMs(limit - elapsedMs));
    } else {
      setRemainingTime(null);
    }
  }, [gameState]);

  // Check if game is complete
  const isGameComplete = () => {
    if (!gameState) return false;
    const status = gameState.status;
    return typeof status === 'object' && 'Complete' in status;
  };

  // Get winning snake ID from game status
  const getWinningSnakeId = (): number | null => {
    if (!gameState) return null;
    const status = gameState.status;
    if (typeof status === 'object' && 'Complete' in status) {
      return status.Complete.winning_snake_id;
    }
    return null;
  };

  // Calculate snake statistics for game over
  const getSnakeStats = () => {
    if (!gameState || !gameState.arena || !gameState.arena.snakes) return [];

    return gameState.arena.snakes.map((snake, index) => {
      // Find player for this snake
      const playerEntry = Object.entries(gameState.players || {}).find(
        ([_, player]) => player.snake_id === index
      );
      const userId = playerEntry ? parseInt(playerEntry[0]) : null;
      const isCurrentPlayer = userId === currentUserId;
      
      // Get username from game state or fall back to generic name
      const username = userId && gameState.usernames ? 
        gameState.usernames[userId] : null;
      
      // Use server-provided score
      const serverScore = gameState?.scores?.[index] || 0;

      // Calculate food eaten based on score
      const foodEaten = serverScore; // Score is already length - 2
      
      return {
        index,
        snake,
        color: SNAKE_COLORS[index % SNAKE_COLORS.length],
        userId,
        isCurrentPlayer,
        name: isCurrentPlayer ? 'You' : (username || `Player ${index + 1}`),
        finalLength: serverScore + 2, // Score is length - 2, so add 2 back
        foodEaten,
        isWinner: index === getWinningSnakeId(),
        team: snake.team_id !== undefined && snake.team_id !== null ? snake.team_id + 1 : (index % 2 === 0 ? 1 : 2), // Use actual team_id if available
      };
    });
  };
  
  // Calculate game stats (XP from server, enemy food eaten)
  const calculateGameStats = () => {
    const stats = getSnakeStats();
    const currentPlayer = stats.find(s => s.isCurrentPlayer);

    if (!currentPlayer) return { xpGained: 0, foodEaten: 0, enemyFoodEaten: 0 };

    // DEBUG: Log what we're checking
    // console.log('=== XP Debug ===');
    // console.log('currentPlayer.userId:', currentPlayer.userId);
    // console.log('gameState:', gameState);
    // console.log('gameState?.player_xp:', gameState?.player_xp);
    // console.log('Type of player_xp:', typeof gameState?.player_xp);
    // console.log('Keys in player_xp:', gameState?.player_xp ? Object.keys(gameState.player_xp) : 'undefined');

    // Get actual XP from server (player_xp map uses user_id as key)
    const xpGained = currentPlayer.userId && gameState?.player_xp
      ? (gameState.player_xp[currentPlayer.userId] || 0)
      : 0;

    // console.log('Calculated xpGained:', xpGained);
    // console.log('================');

    // Calculate enemy food eaten (in multiplayer, count kills as "enemy food")
    // For now, we'll estimate based on whether enemies died
    const enemyFoodEaten = stats.filter(s => !s.isCurrentPlayer && !s.snake.is_alive).length;

    return {
      xpGained,
      foodEaten: currentPlayer.foodEaten,
      enemyFoodEaten
    };
  };

  // Determine game result text
  const getResultText = () => {
    const winningSnakeId = getWinningSnakeId();
    const snakeStats = getSnakeStats();
    const currentPlayerSnake = snakeStats.find(s => s.isCurrentPlayer);
    
    if (isSoloGame()) {
      return 'Game Over';
    }
    
    if (winningSnakeId === null) {
      return 'Draw!';
    }
    
    if (currentPlayerSnake && currentPlayerSnake.index === winningSnakeId) {
      return 'Victory!';
    }
    
    const winner = snakeStats.find(s => s.index === winningSnakeId);
    if (winner) {
      return winner.isCurrentPlayer ? 'Victory!' : `${winner.name} Wins!`;
    }
    
    return 'Game Over';
  };

  const solo = isSoloGame();
  const snakeInfo = getSnakeInfo();
  const teamStats = !solo ? getTeamStats() : null;
  const snakeStats = getSnakeStats();
  const currentPlayerStats = snakeStats.find(s => s.isCurrentPlayer);
  const resultText = getResultText();
  const gameStats = calculateGameStats();
  const timeLimitMs = gameState?.properties?.time_limit_ms ?? null;
  const timeLabel = timeLimitMs ? 'Time Left' : 'Time';
  const timeValue = timeLimitMs && remainingTime ? remainingTime : elapsedTime;


  return (
    <div 
      className={`absolute top-0 left-0 right-0 bg-white z-40 transition-opacity duration-400 ease-out ${
        isVisible ? 'opacity-100' : 'opacity-0'
      }`}
      style={{ 
        margin: '0 20px',
        padding: '10px 18px',
        marginLeft: 'auto',
        marginRight: 'auto',
        maxWidth: solo ? '600px' : '650px',
        background: 'linear-gradient(to bottom, #ffffff, #fafafa)',
        borderLeft: '3px solid white',
        borderRight: '3px solid white',
        borderBottom: '3px solid white',
        borderBottomLeftRadius: '0.5rem',
        borderBottomRightRadius: '0.5rem',
        boxShadow: 'inset -0.5px 0 0 0 rgba(0, 0, 0, 0.7), inset 0.5px 0 0 0 rgba(0, 0, 0, 0.7), inset 0 -0.5px 0 0 rgba(0, 0, 0, 0.7), .5px .5px 0 1.5px rgba(0, 0, 0, 0.7)'
      }}
    >
      {solo ? (
        // Solo Game Scoreboard
        <div className="flex items-center justify-between gap-4">
          {/* Left - Logo with separator */}
          <div className="flex items-center gap-6 ml-1">
            <img 
              src="/SnaketronLogo.png" 
              alt="Snaketron" 
              className="h-5 w-auto transition-all duration-200 cursor-pointer"
              style={{ 
                opacity: logoHovered ? 1 : 0.75
              }}
              onMouseEnter={() => setLogoHovered(true)}
              onMouseLeave={() => setLogoHovered(false)}
            />
            <div className="h-7 w-px bg-gray-300 opacity-40" />
          </div>

          {/* Center - Score and Time */}
          <div className="flex items-center gap-8">
            <div className="flex flex-col items-center">
              <div className="text-gray-500 font-semibold text-xs uppercase tracking-wider">
                Score
              </div>
              <div className="text-black-70 font-black text-2xl -mt-0.5 tabular-nums" style={{ color: '#22c55e' }}>
                {(gameState?.scores?.[0] || 0).toString().padStart(3, '0')}
              </div>
            </div>
            
            <div className="w-px h-8 bg-gray-300 opacity-50" />
            
            <div className="flex flex-col items-center">
              <div className="text-gray-500 font-semibold text-xs uppercase tracking-wider">
                {timeLabel}
              </div>
              <div className="text-black-70 font-black text-2xl -mt-0.5">
                {timeValue}
              </div>
            </div>
          </div>

          {/* Right - Snake Status */}
          {snakeInfo[0] && (
            <div 
              className="flex items-center gap-2 px-3 py-1.5 rounded-md"
              style={{ 
                backgroundColor: snakeInfo[0].snake.is_alive ? 'rgba(34, 197, 94, 0.08)' : 'rgba(156, 163, 175, 0.08)',
                border: `1px solid ${snakeInfo[0].snake.is_alive ? 'rgba(34, 197, 94, 0.2)' : 'rgba(156, 163, 175, 0.2)'}`
              }}
            >
              <div 
                className="w-3.5 h-3.5 rounded-sm flex-shrink-0"
                style={{ backgroundColor: snakeInfo[0].color }}
              />
              <div 
                className={`w-2 h-2 rounded-full flex-shrink-0 ${
                  snakeInfo[0].snake.is_alive ? 'bg-green-500' : 'bg-gray-400'
                }`}
              />
              <span className="text-sm font-bold text-gray-700">
                You
              </span>
            </div>
          )}
        </div>
      ) : (
        // Multiplayer Game Scoreboard - Minimal Design
        <div className="flex items-center justify-center gap-8">
          {/* Left - Logo with separator */}
          <div className="flex items-center gap-6">
            <img 
              src="/SnaketronLogo.png" 
              alt="Snaketron" 
              className="h-5 w-auto transition-all duration-200 cursor-pointer"
              style={{ 
                opacity: logoHovered ? 1 : 0.75
              }}
              onMouseEnter={() => setLogoHovered(true)}
              onMouseLeave={() => setLogoHovered(false)}
            />
            <div className="h-7 w-px bg-gray-300 opacity-40" />
          </div>

          {/* Center Section - Scores flanking Time */}
          <div className="flex items-center gap-8">
            {/* Current Player's Team Score (always left, blue) */}
            <span className="text-2xl font-black tabular-nums" style={{ color: '#3b82f6' }}>
              {teamStats?.currentPlayerTeam === 1 ? (teamStats?.team1.score || 0) : (teamStats?.team2.score || 0)}
            </span>

            {/* Time in the middle */}
            <div className="flex flex-col items-center">
              <div className="text-gray-500 font-semibold text-xs uppercase tracking-wider">
                {timeLabel}
              </div>
              <div className="text-black-70 font-black text-2xl -mt-0.5 tabular-nums">
                {timeValue}
              </div>
            </div>

            {/* Opponent Team Score (always right, red) */}
            <span className="text-2xl font-black tabular-nums" style={{ color: '#ef4444' }}>
              {teamStats?.currentPlayerTeam === 1 ? (teamStats?.team2.score || 0) : (teamStats?.team1.score || 0)}
            </span>
          </div>

          {/* Divider */}
          <div className="w-px h-8 bg-gray-300 opacity-50" />

          {/* Game Mode & Round Info */}
          <div className="flex flex-col items-center">
            <div className="text-gray-500 font-semibold text-xs uppercase tracking-wider">
              {getGameModeText()}
            </div>
            {/*<div className="text-gray-600 font-bold text-lg -mt-0.5">*/}
            {/*  {timeLimitMs ? 'Match ends at time limit' : 'All snakes must fall'}*/}
            {/*</div>*/}
          </div>
        </div>
      )}
      
      {/* Game Over Section */}
      {showGameOver && (
        <div 
          className={`transition-all duration-500 ease-out overflow-hidden ${
            gameOverExpanded ? 'max-h-32' : 'max-h-0'
          }`}
        >
          {/* Separator */}
          <div className="w-full h-px bg-gray-200 opacity-50 my-2" />
          
          {/* Game Over Content */}
          <div className="flex items-center justify-between">
            {/* Left side - Stats */}
            <div className="flex items-center gap-4">
              {solo ? (
                // Solo game stats
                <>
                  <div className="flex flex-col items-center">
                    <div className="text-gray-500 font-semibold text-xs uppercase tracking-wider">
                      XP
                    </div>
                    <div className="text-green-600 font-black text-lg -mt-0.5 tabular-nums">
                      +{gameStats.xpGained}
                    </div>
                  </div>
                  <div className="flex flex-col items-center">
                    <div className="text-gray-500 font-semibold text-xs uppercase tracking-wider">
                      Food
                    </div>
                    <div className="text-black-70 font-black text-lg -mt-0.5 tabular-nums">
                      {gameStats.foodEaten}
                    </div>
                  </div>
                  <div className="flex flex-col items-center">
                    <div className="text-gray-500 font-semibold text-xs uppercase tracking-wider">
                      Length
                    </div>
                    <div className="text-black-70 font-black text-lg -mt-0.5 tabular-nums">
                      {currentPlayerStats?.finalLength || 0}
                    </div>
                  </div>
                </>
              ) : (
                // Multiplayer game stats
                <>
                  <div className="flex flex-col items-center">
                    <div className="text-gray-500 font-semibold text-xs uppercase tracking-wider">
                      XP
                    </div>
                    <div className="text-green-600 font-black text-lg -mt-0.5 tabular-nums">
                      +{gameStats.xpGained}
                    </div>
                  </div>
                  <div className="flex flex-col items-center">
                    <div className="text-gray-500 font-semibold text-xs uppercase tracking-wider">
                      Food
                    </div>
                    <div className="text-black-70 font-black text-lg -mt-0.5 tabular-nums">
                      {gameStats.foodEaten}
                    </div>
                  </div>
                  <div className="flex flex-col items-center">
                    <div className="text-gray-500 font-semibold text-xs uppercase tracking-wider">
                      Kills
                    </div>
                    <div className="text-red-600 font-black text-lg -mt-0.5 tabular-nums">
                      {gameStats.enemyFoodEaten}
                    </div>
                  </div>
                </>
              )}
            </div>

            {/* Center - Result Text */}
            <div className="font-black italic uppercase tracking-1 text-black-70" style={{ fontSize: '18px' }}>
              {resultText}
            </div>

            {/* Right side - Action Buttons */}
            <div className="flex items-center gap-3">
              <button
                onClick={onBackToMenu}
                className="px-3 py-1 text-xs border border-gray-400 rounded font-semibold uppercase bg-white text-gray-600 hover:bg-gray-50 transition-colors cursor-pointer"
                style={{ letterSpacing: '0.5px' }}
              >
                Menu
              </button>
              <button
                onClick={onPlayAgain}
                disabled={(isInLobby && !isHost) || isLobbyQueued}
                className={`px-3 py-1 text-xs border rounded font-semibold uppercase transition-all ${
                  (isInLobby && !isHost) || isLobbyQueued
                    ? 'border-gray-400 bg-gray-300 text-gray-500 cursor-not-allowed'
                    : 'border-green-700 bg-green-600 text-white hover:bg-green-700 cursor-pointer'
                }`}
                style={{
                  letterSpacing: '0.5px',
                }}
                title={
                  isLobbyQueued
                    ? 'Lobby is already queued and waiting for a match'
                    : (isInLobby && !isHost
                        ? 'Waiting for host to start next game'
                        : 'Play again with your lobby')
                }
              >
                {isLobbyQueued ? 'Queued...' : 'Play Again'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default Scoreboard;
