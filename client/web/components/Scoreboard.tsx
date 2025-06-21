import React from 'react';
import { GameState } from '../types';

interface ScoreboardProps {
  gameState: GameState | null;
  score: number;
  isVisible: boolean;
}

const Scoreboard: React.FC<ScoreboardProps> = ({ gameState, score, isVisible }) => {

  // Calculate team scores and alive snakes
  const getTeamStats = () => {
    if (!gameState || !gameState.arena || !gameState.arena.snakes) {
      return { team1: { alive: 0, total: 0 }, team2: { alive: 0, total: 0 } };
    }

    const team1 = gameState.arena.snakes.filter((snake, index) => index % 2 === 0);
    const team2 = gameState.arena.snakes.filter((snake, index) => index % 2 === 1);

    return {
      team1: {
        alive: team1.filter(snake => snake.is_alive).length,
        total: team1.length
      },
      team2: {
        alive: team2.filter(snake => snake.is_alive).length,
        total: team2.length
      }
    };
  };

  const teamStats = getTeamStats();

  return (
    <div 
      className={`absolute top-0 left-0 right-0 bg-white panel z-40 transition-opacity duration-400 ease-out ${
        isVisible ? 'opacity-100' : 'opacity-0'
      }`}
      style={{ 
        margin: '20px',
        padding: '15px 20px',
        marginLeft: 'auto',
        marginRight: 'auto',
        maxWidth: '1000px'
      }}
    >
      <div className="flex items-center justify-between">
        {/* Left side - Team 1 */}
        <div className="flex-1">
          <div className="text-black-70 font-black italic uppercase text-lg tracking-1">
            TEAM 1
          </div>
          <div className="text-black-70 font-bold text-sm mt-1">
            Alive: {teamStats.team1.alive}/{teamStats.team1.total}
          </div>
        </div>

        {/* Center - Logo and Score */}
        <div className="flex-1 text-center">
          <img 
            src="/SnaketronLogo.png" 
            alt="Snaketron" 
            className="h-6 w-auto mx-auto mb-1 opacity-80" 
          />
          <div className="text-black-70 font-black text-xl">
            SCORE: {score}
          </div>
        </div>

        {/* Right side - Team 2 */}
        <div className="flex-1 text-right">
          <div className="text-black-70 font-black italic uppercase text-lg tracking-1">
            TEAM 2
          </div>
          <div className="text-black-70 font-bold text-sm mt-1">
            Alive: {teamStats.team2.alive}/{teamStats.team2.total}
          </div>
        </div>
      </div>
    </div>
  );
};

export default Scoreboard;