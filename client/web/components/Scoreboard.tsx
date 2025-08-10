import React, { useEffect, useState } from 'react';
import { GameState } from '../types';

interface ScoreboardProps {
  gameState: GameState | null;
  score: number;
  isVisible: boolean;
  currentUserId?: number;
}

// Snake colors matching render.rs
const SNAKE_COLORS = [
  '#70bfe3', // Light blue/teal
  '#556270', // Dark gray
  '#ff6b6b', // Coral red
  '#f7b731', // Yellow/gold
];

const Scoreboard: React.FC<ScoreboardProps> = ({ gameState, score, isVisible, currentUserId }) => {
  const [elapsedTime, setElapsedTime] = useState('00:00');
  const [logoHovered, setLogoHovered] = useState(false);

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
        if (perTeam === 1) return 'Quick Match 1v1';
        return `Team Battle ${perTeam}v${perTeam}`;
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
      
      return {
        index,
        snake,
        color: SNAKE_COLORS[index % SNAKE_COLORS.length],
        userId,
        isCurrentPlayer,
        name: isCurrentPlayer ? 'You' : `Player ${index + 1}`,
        team: index % 2 === 0 ? 1 : 2, // Even indices = team 1, odd = team 2
      };
    });
  };

  // Calculate team stats for team games
  const getTeamStats = () => {
    const snakeInfo = getSnakeInfo();
    const team1Snakes = snakeInfo.filter(info => info.team === 1);
    const team2Snakes = snakeInfo.filter(info => info.team === 2);

    const calculateTeamScore = (teamSnakes: any[]) => {
      return teamSnakes.reduce((total, info) => {
        const snakeScore = info.snake.is_alive ? Math.max(0, info.snake.body.length - 2) : 0;
        return total + snakeScore;
      }, 0);
    };

    return {
      team1: {
        snakes: team1Snakes,
        score: calculateTeamScore(team1Snakes),
        alive: team1Snakes.filter(info => info.snake.is_alive).length,
        total: team1Snakes.length
      },
      team2: {
        snakes: team2Snakes,
        score: calculateTeamScore(team2Snakes),
        alive: team2Snakes.filter(info => info.snake.is_alive).length,
        total: team2Snakes.length
      }
    };
  };

  // Update elapsed time
  useEffect(() => {
    if (!gameState || !gameState.start_ms) return;

    const updateTime = () => {
      const now = Date.now();
      const startTime = gameState.start_ms;
      const elapsed = Math.max(0, Math.floor((now - startTime) / 1000));
      
      const minutes = Math.floor(elapsed / 60);
      const seconds = elapsed % 60;
      
      setElapsedTime(`${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`);
    };

    updateTime();
    const interval = setInterval(updateTime, 1000);
    
    return () => clearInterval(interval);
  }, [gameState]);

  const solo = isSoloGame();
  const snakeInfo = getSnakeInfo();
  const teamStats = !solo ? getTeamStats() : null;

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
        maxWidth: solo ? '600px' : '1000px',
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
                {score.toString().padStart(3, '0')}
              </div>
            </div>
            
            <div className="w-px h-8 bg-gray-300 opacity-50" />
            
            <div className="flex flex-col items-center">
              <div className="text-gray-500 font-semibold text-xs uppercase tracking-wider">
                Time
              </div>
              <div className="text-black-70 font-black text-2xl -mt-0.5">
                {elapsedTime}
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
        // Multiplayer Game Scoreboard - Compact Design
        <div className="flex items-center justify-between gap-3">
          {/* Left - Logo with separator */}
          <div className="flex items-center gap-4 flex-shrink-0 ml-1">
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

          {/* Game Mode & Total Wins */}
          <div className="flex flex-col items-center">
            <div className="text-gray-500 font-semibold text-xs uppercase tracking-wider">
              {getGameModeText()}
            </div>
            <div className="text-gray-600 font-bold text-lg -mt-0.5">
              0 - 0
            </div>
          </div>

          {/* Divider */}
          <div className="w-px h-8 bg-gray-300 opacity-50" />

          {/* Left Team Section */}
          <div className="flex items-center gap-2">
            {/* Team 1 Players */}
            <div className="flex gap-1.5">
              {teamStats?.team1.snakes.map((info, idx) => (
                <div 
                  key={idx}
                  className="flex items-center gap-1 px-2 py-1 rounded"
                  style={{ 
                    backgroundColor: info.snake.is_alive ? 'rgba(59, 130, 246, 0.08)' : 'rgba(156, 163, 175, 0.08)',
                    border: `1px solid ${info.snake.is_alive ? 'rgba(59, 130, 246, 0.2)' : 'rgba(156, 163, 175, 0.2)'}`
                  }}
                >
                  <div 
                    className="w-3 h-3 rounded-sm"
                    style={{ backgroundColor: info.color }}
                  />
                  <span className={`text-sm font-semibold ${info.isCurrentPlayer ? 'text-blue-600' : 'text-gray-700'}`}>
                    {info.name}
                  </span>
                  <div className={`w-2 h-2 rounded-full ${
                    info.snake.is_alive ? 'bg-green-500' : 'bg-gray-400'
                  }`} />
                </div>
              ))}
            </div>
          </div>

          {/* Center Section - Scores flanking Time */}
          <div className="flex items-center gap-8">
            {/* Team 1 Match Score */}
            <span className="text-2xl font-black tabular-nums min-w-[2ch] text-right" style={{ color: '#3b82f6' }}>
              {teamStats?.team1.score || 0}
            </span>

            {/* Time in the middle */}
            <div className="flex flex-col items-center">
              <div className="text-gray-500 font-semibold text-xs uppercase tracking-wider">
                Time
              </div>
              <div className="text-black-70 font-black text-2xl -mt-0.5 tabular-nums">
                {elapsedTime}
              </div>
            </div>

            {/* Team 2 Match Score */}
            <span className="text-2xl font-black tabular-nums min-w-[2ch] text-left" style={{ color: '#ef4444' }}>
              {teamStats?.team2.score || 0}
            </span>
          </div>

          {/* Right Team Section */}
          <div className="flex items-center gap-2">
            {/* Team 2 Players */}
            <div className="flex gap-1.5">
              {teamStats?.team2.snakes.map((info, idx) => (
                <div 
                  key={idx}
                  className="flex items-center gap-1 px-2 py-1 rounded"
                  style={{ 
                    backgroundColor: info.snake.is_alive ? 'rgba(239, 68, 68, 0.08)' : 'rgba(156, 163, 175, 0.08)',
                    border: `1px solid ${info.snake.is_alive ? 'rgba(239, 68, 68, 0.2)' : 'rgba(156, 163, 175, 0.2)'}`
                  }}
                >
                  <div className={`w-2 h-2 rounded-full ${
                    info.snake.is_alive ? 'bg-green-500' : 'bg-gray-400'
                  }`} />
                  <span className={`text-sm font-semibold ${info.isCurrentPlayer ? 'text-red-600' : 'text-gray-700'}`}>
                    {info.name}
                  </span>
                  <div 
                    className="w-3 h-3 rounded-sm"
                    style={{ backgroundColor: info.color }}
                  />
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default Scoreboard;