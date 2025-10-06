import React from 'react';

interface NewsItem {
  id: number;
  title: string;
  content: string;
  date: string;
}

interface LeaderboardEntry {
  rank: number;
  username: string;
  score: number;
}

interface SeasonLeaderEntry {
  rank: number;
  username: string;
  mmr: number;
}

const NewsAndLeaderboard: React.FC = () => {
  // Dummy news data
  const newsItems: NewsItem[] = [
    {
      id: 1,
      title: "SnakeTron Beta is Live!",
      content: "Welcome to the SnakeTron beta! We're excited to have you here. Play solo or compete against others in our multiplayer modes. Report any bugs or issues you encounter.",
      date: "October 5, 2025"
    }
  ];

  // Dummy leaderboard data - top solo games
  const topSoloGames: LeaderboardEntry[] = [
    { rank: 1, username: "SnakeMaster", score: 15420 },
    { rank: 2, username: "lopatron00", score: 14850 },
    { rank: 3, username: "VelocityViper", score: 13990 },
    { rank: 4, username: "TurboSnake", score: 12340 },
    { rank: 5, username: "SlitherKing", score: 11780 }
  ];

  // Dummy season leaderboard data
  const seasonLeaders: SeasonLeaderEntry[] = [
    { rank: 1, username: "CompetitivePro", mmr: 2450 },
    { rank: 2, username: "RankedWarrior", mmr: 2380 },
    { rank: 3, username: "SnakeChampion", mmr: 2290 },
    { rank: 4, username: "ElitePlayer", mmr: 2150 },
    { rank: 5, username: "TopSnake", mmr: 2090 }
  ];

  return (
    <div className="w-full max-w-6xl mx-auto px-5 mt-8 mb-10">
      <div className="panel p-0 overflow-hidden">
        <div className="flex">
          {/* Main News Section - 2/3 width */}
          <div className="flex-[2] p-6">
            <div className="space-y-3">
              {newsItems.map((item) => (
                <div key={item.id} className="py-3 border-b border-black/5 last:border-0">
                  <div className="flex items-baseline gap-3 mb-1.5">
                    <h3 className="text-base font-black text-black-80">
                      {item.title}
                    </h3>
                    <span className="text-xs text-black/40 font-bold whitespace-nowrap">
                      {item.date}
                    </span>
                  </div>
                  <p className="text-sm text-black/60 leading-relaxed">
                    {item.content}
                  </p>
                </div>
              ))}
            </div>
          </div>

          {/* Subtle Divider */}
          <div className="w-px my-8" style={{
            background: 'linear-gradient(to bottom, transparent, rgba(0, 0, 0, 0.1) 20%, rgba(0, 0, 0, 0.1) 80%, transparent)'
          }} />

          {/* Leaderboard Sidebar - 1/3 width */}
          <div className="flex-1 p-6">
            {/* Top Solo Games */}
            <div className="mb-8">
              <h3 className="text-xs font-black italic uppercase mb-3 text-black/50 tracking-1">
                Top Solo Games
              </h3>
              <div className="space-y-1.5">
                {topSoloGames.map((entry) => (
                  <div
                    key={entry.rank}
                    className="flex items-center justify-between py-2 px-2.5 hover:bg-black/[0.02] transition-colors"
                  >
                    <div className="flex items-center gap-2.5">
                      <span className={`text-xs font-black min-w-[20px] ${
                        entry.rank === 1 ? 'text-yellow-600' :
                        entry.rank === 2 ? 'text-gray-400' :
                        entry.rank === 3 ? 'text-amber-700' :
                        'text-black/30'
                      }`}>
                        {entry.rank}
                      </span>
                      <span className="text-sm font-bold text-black-70 truncate">
                        {entry.username}
                      </span>
                    </div>
                    <span className="text-xs font-black text-black/40 tabular-nums">
                      {entry.score.toLocaleString()}
                    </span>
                  </div>
                ))}
              </div>
            </div>

            {/* Season Leaders */}
            <div>
              <h3 className="text-xs font-black italic uppercase mb-3 text-black/50 tracking-1">
                Season 0 Leaders
              </h3>
              <div className="space-y-1.5">
                {seasonLeaders.map((entry) => (
                  <div
                    key={entry.rank}
                    className="flex items-center justify-between py-2 px-2.5 hover:bg-black/[0.02] transition-colors"
                  >
                    <div className="flex items-center gap-2.5">
                      <span className={`text-xs font-black min-w-[20px] ${
                        entry.rank === 1 ? 'text-yellow-600' :
                        entry.rank === 2 ? 'text-gray-400' :
                        entry.rank === 3 ? 'text-amber-700' :
                        'text-black/30'
                      }`}>
                        {entry.rank}
                      </span>
                      <span className="text-sm font-bold text-black-70 truncate">
                        {entry.username}
                      </span>
                    </div>
                    <span className="text-xs font-black text-black/40 tabular-nums">
                      {entry.mmr}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default NewsAndLeaderboard;
