import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Sidebar } from './Sidebar';
import { MobileHeader } from './MobileHeader';
import { LobbyChat } from './LobbyChat';
import { InviteFriendsModal } from './InviteFriendsModal';
import JoinGameModal from './JoinGameModal';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import { useRegions } from '../hooks/useRegions';
import { LobbyGameMode, RankTier, RankDivision, Rank, LeaderboardEntry } from '../types';

const generateGuestNickname = () => `Guest${Math.floor(1000 + Math.random() * 9000)}`;

// Dummy data for demonstration
const DUMMY_LEADERBOARD_DATA: Record<string, Record<LobbyGameMode, LeaderboardEntry[]>> = {
  'Season 1': {
    duel: [
      { rank: 1, username: 'SnakeMaster', mmr: 2450, wins: 142, losses: 38, winRate: 78.9 },
      { rank: 2, username: 'Lopatron', mmr: 2380, wins: 128, losses: 42, winRate: 75.3 },
      { rank: 3, username: 'VenomousViper', mmr: 2310, wins: 115, losses: 45, winRate: 71.9 },
      { rank: 4, username: 'SlitherKing', mmr: 2280, wins: 105, losses: 48, winRate: 68.6 },
      { rank: 5, username: 'CobraCommander', mmr: 2245, wins: 98, losses: 52, winRate: 65.3 },
      { rank: 6, username: 'PythonPro', mmr: 2210, wins: 92, losses: 55, winRate: 62.6 },
      { rank: 7, username: 'RattlerRex', mmr: 2180, wins: 88, losses: 58, winRate: 60.3 },
      { rank: 8, username: 'AnacondaAce', mmr: 2150, wins: 85, losses: 61, winRate: 58.2 },
      { rank: 9, username: 'SerpentSage', mmr: 2120, wins: 82, losses: 64, winRate: 56.2 },
      { rank: 10, username: 'ViperVault', mmr: 2090, wins: 78, losses: 67, winRate: 53.8 },
    ],
    '2v2': [
      { rank: 1, username: 'TeamTango', mmr: 2520, wins: 156, losses: 32, winRate: 83.0 },
      { rank: 2, username: 'DuoDestroyer', mmr: 2440, wins: 138, losses: 40, winRate: 77.5 },
      { rank: 3, username: 'PartnerPlay', mmr: 2370, wins: 122, losses: 46, winRate: 72.6 },
      { rank: 4, username: 'SyncSquad', mmr: 2310, wins: 110, losses: 52, winRate: 67.9 },
      { rank: 5, username: 'TagTeamTitan', mmr: 2260, wins: 102, losses: 56, winRate: 64.6 },
      { rank: 6, username: 'CoopChampion', mmr: 2220, wins: 95, losses: 60, winRate: 61.3 },
      { rank: 7, username: 'AllyAces', mmr: 2180, wins: 88, losses: 64, winRate: 57.9 },
      { rank: 8, username: 'BuddyBrawler', mmr: 2150, wins: 82, losses: 68, winRate: 54.7 },
      { rank: 9, username: 'PairPower', mmr: 2110, wins: 76, losses: 72, winRate: 51.4 },
      { rank: 10, username: 'DuoDynamic', mmr: 2080, wins: 72, losses: 75, winRate: 49.0 },
    ],
    solo: [
      { rank: 1, username: 'LoneWolf', mmr: 2380, wins: 135, losses: 40, winRate: 77.1 },
      { rank: 2, username: 'SoloSurvivor', mmr: 2320, wins: 118, losses: 45, winRate: 72.4 },
      { rank: 3, username: 'OneManArmy', mmr: 2260, wins: 105, losses: 50, winRate: 67.7 },
      { rank: 4, username: 'IndieInvader', mmr: 2210, wins: 96, losses: 54, winRate: 64.0 },
      { rank: 5, username: 'RogueSerpent', mmr: 2170, wins: 88, losses: 58, winRate: 60.3 },
      { rank: 6, username: 'FreestyleKing', mmr: 2130, wins: 82, losses: 62, winRate: 56.9 },
      { rank: 7, username: 'SoloSlayer', mmr: 2100, wins: 76, losses: 66, winRate: 53.5 },
      { rank: 8, username: 'LoneStar', mmr: 2070, wins: 72, losses: 70, winRate: 50.7 },
      { rank: 9, username: 'Maverick', mmr: 2040, wins: 68, losses: 74, winRate: 47.9 },
      { rank: 10, username: 'SoloStorm', mmr: 2010, wins: 64, losses: 78, winRate: 45.1 },
    ],
    ffa: [
      { rank: 1, username: 'ChaosKing', mmr: 2490, wins: 148, losses: 36, winRate: 80.4 },
      { rank: 2, username: 'BrawlBoss', mmr: 2410, wins: 132, losses: 42, winRate: 75.9 },
      { rank: 3, username: 'MeleeMaster', mmr: 2340, wins: 118, losses: 48, winRate: 71.1 },
      { rank: 4, username: 'RumbleRuler', mmr: 2280, wins: 106, losses: 52, winRate: 67.1 },
      { rank: 5, username: 'FFAPhenom', mmr: 2230, wins: 98, losses: 56, winRate: 63.6 },
      { rank: 6, username: 'AllOutAttack', mmr: 2190, wins: 90, losses: 60, winRate: 60.0 },
      { rank: 7, username: 'FreeForAllFury', mmr: 2150, wins: 84, losses: 64, winRate: 56.8 },
      { rank: 8, username: 'BattleRoyale', mmr: 2120, wins: 78, losses: 68, winRate: 53.4 },
      { rank: 9, username: 'ScrambleSnake', mmr: 2080, wins: 72, losses: 72, winRate: 50.0 },
      { rank: 10, username: 'MultiMayhem', mmr: 2050, wins: 68, losses: 76, winRate: 47.2 },
    ],
  },
};

const MY_CURRENT_RANK: Rank = {
  tier: 'gold',
  division: 2,
  mmr: 1850,
};

const formatRankDisplay = (rank: Rank): string => {
  const tierName = rank.tier.charAt(0).toUpperCase() + rank.tier.slice(1);
  return `${tierName} ${rank.division}`;
};

const getRankImage = (tier: RankTier): string => {
  // Map master to grandmaster since we don't have a separate master.png
  const imageTier = tier === 'master' ? 'grandmaster' : tier;
  return `/images/${imageTier}.png`;
};

const LeaderboardContent: React.FC<{
  selectedSeason: string;
  setSelectedSeason: (season: string) => void;
  selectedMode: LobbyGameMode;
  setSelectedMode: (mode: LobbyGameMode) => void;
}> = ({ selectedSeason, setSelectedSeason, selectedMode, setSelectedMode }) => {
  const seasons = Object.keys(DUMMY_LEADERBOARD_DATA);
  const leaderboardData = DUMMY_LEADERBOARD_DATA[selectedSeason]?.[selectedMode] || [];

  const gameModes: Array<{ id: LobbyGameMode; label: string }> = [
    { id: 'duel', label: 'DUEL' },
    { id: '2v2', label: '2V2' },
    { id: 'solo', label: 'SOLO' },
    { id: 'ffa', label: 'FFA' },
  ];

  return (
    <div className="w-full max-w-4xl mx-auto px-4 py-8">
      {/* Header row with current rank and selectors */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4 mb-8">
        {/* Current Rank Display */}
        <div className="flex items-center gap-3">
          <img
            src={getRankImage(MY_CURRENT_RANK.tier)}
            alt={MY_CURRENT_RANK.tier}
            className="w-12 h-12 object-contain"
          />
          <div>
            <div className="text-xs uppercase tracking-1 text-black-70 font-bold">Your Rank</div>
            <div className="font-black italic uppercase tracking-1 text-lg">
              {formatRankDisplay(MY_CURRENT_RANK)}
            </div>
            <div className="text-xs text-black-70">{MY_CURRENT_RANK.mmr} MMR</div>
          </div>
        </div>

        {/* Selectors */}
        <div className="flex flex-col sm:flex-row gap-3">
          {/* Season Selector */}
          <div className="relative">
            <select
              value={selectedSeason}
              onChange={(e) => setSelectedSeason(e.target.value)}
              className="w-full sm:w-auto px-4 py-2 pr-8 border-2 border-gray-300 rounded-lg bg-white
                         font-black italic uppercase tracking-1 text-sm text-black-70
                         focus:outline-none focus:border-blue-500 cursor-pointer
                         appearance-none"
            >
              {seasons.map((season) => (
                <option key={season} value={season}>
                  {season}
                </option>
              ))}
            </select>
            <div className="absolute right-2 top-1/2 -translate-y-1/2 pointer-events-none">
              <svg className="w-4 h-4 text-black-70" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
              </svg>
            </div>
          </div>

          {/* Game Mode Selector */}
          <div className="grid grid-cols-4 gap-2">
            {gameModes.map((mode) => {
              const isSelected = selectedMode === mode.id;
              return (
                <button
                  key={mode.id}
                  type="button"
                  onClick={() => setSelectedMode(mode.id)}
                  className={`
                    px-3 py-2 rounded-lg font-black italic uppercase tracking-1 text-xs
                    transition-all border-2
                    ${
                      isSelected
                        ? 'border-blue-500 bg-blue-50 text-black-70'
                        : 'border-gray-300 bg-white text-black-70 hover:border-gray-400'
                    }
                  `}
                >
                  {mode.label}
                </button>
              );
            })}
          </div>
        </div>
      </div>

      {/* Leaderboard Table */}
      <div className="bg-white border-2 border-gray-300 rounded-lg overflow-hidden">
        {/* Table Header */}
        <div className="grid grid-cols-[50px_1fr_100px_80px_80px_80px] gap-2 px-4 py-3 bg-gray-50 border-b-2 border-gray-300">
          <div className="font-black uppercase tracking-1 text-xs text-black-70">#</div>
          <div className="font-black uppercase tracking-1 text-xs text-black-70">Player</div>
          <div className="font-black uppercase tracking-1 text-xs text-black-70 text-right">MMR</div>
          <div className="font-black uppercase tracking-1 text-xs text-black-70 text-right hidden sm:block">Wins</div>
          <div className="font-black uppercase tracking-1 text-xs text-black-70 text-right hidden sm:block">Losses</div>
          <div className="font-black uppercase tracking-1 text-xs text-black-70 text-right">Win %</div>
        </div>

        {/* Table Body */}
        <div className="divide-y divide-gray-200">
          {leaderboardData.map((entry) => (
            <div
              key={entry.rank}
              className="grid grid-cols-[50px_1fr_100px_80px_80px_80px] gap-2 px-4 py-3 hover:bg-gray-50 transition-colors"
            >
              {/* Rank */}
              <div className="flex items-center">
                <span className="font-black text-base text-black-70">{entry.rank}</span>
              </div>

              {/* Username */}
              <div className="flex items-center font-bold text-sm text-black-70 truncate">
                {entry.username}
              </div>

              {/* MMR */}
              <div className="flex items-center justify-end font-black italic text-base text-black-70">
                {entry.mmr}
              </div>

              {/* Wins (hidden on mobile) */}
              <div className="hidden sm:flex items-center justify-end text-sm text-black-70">
                {entry.wins}
              </div>

              {/* Losses (hidden on mobile) */}
              <div className="hidden sm:flex items-center justify-end text-sm text-black-70">
                {entry.losses}
              </div>

              {/* Win Rate */}
              <div className="flex items-center justify-end font-bold text-sm text-black-70">
                {entry.winRate.toFixed(1)}%
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export const Leaderboard: React.FC = () => {
  const navigate = useNavigate();
  const { user, logout } = useAuth();
  const {
    connectToRegion,
    isConnected,
    onMessage,
    currentRegionUrl,
    currentLobby,
    lobbyMembers,
    createLobby,
    leaveLobby,
    lobbyChatMessages,
    sendChatMessage,
  } = useWebSocket();
  const [isMobile, setIsMobile] = useState(false);
  const [showInviteModal, setShowInviteModal] = useState(false);
  const [showJoinModal, setShowJoinModal] = useState(false);
  const [isCreatingInvite, setIsCreatingInvite] = useState(false);
  const [selectedSeason, setSelectedSeason] = useState<string>('Season 1');
  const [selectedMode, setSelectedMode] = useState<LobbyGameMode>('duel');

  // Use regions hook for live data
  const {
    regions,
    selectedRegion,
    selectRegion,
    isLoading: regionsLoading,
    error: regionsError,
  } = useRegions({
    isWebSocketConnected: isConnected,
    onMessage,
  });
  const currentRegionId = selectedRegion?.id ?? regions[0]?.id ?? '';

  // Check if mobile on mount and resize
  useEffect(() => {
    const checkMobile = () => {
      setIsMobile(window.innerWidth < 800);
    };
    checkMobile();
    window.addEventListener('resize', checkMobile);
    return () => window.removeEventListener('resize', checkMobile);
  }, []);

  // Connect to selected region when it changes
  useEffect(() => {
    if (!selectedRegion) {
      return;
    }

    if (currentRegionUrl === selectedRegion.wsUrl) {
      return;
    }

    console.log('Connecting to region:', selectedRegion.name, selectedRegion.wsUrl);
    connectToRegion(selectedRegion.wsUrl, {
      regionId: selectedRegion.id,
      origin: selectedRegion.origin,
    });
  }, [selectedRegion?.id, selectedRegion?.wsUrl, selectedRegion?.origin, connectToRegion, currentRegionUrl]);

  const handleRegionChange = (regionId: string) => {
    selectRegion(regionId);
  };

  const handleSendMessage = (message: string) => {
    sendChatMessage('lobby', message);
  };

  const handleInvite = async () => {
    if (isCreatingInvite) {
      return;
    }

    setIsCreatingInvite(true);
    try {
      if (!currentLobby) {
        await createLobby();
        console.log('Lobby created successfully');
      }

      setShowInviteModal(true);
    } catch (error) {
      console.error('Failed to create lobby:', error);
    } finally {
      setIsCreatingInvite(false);
    }
  };

  const handleLeaveLobby = async () => {
    try {
      await leaveLobby();
      console.log('Left lobby successfully');
    } catch (error) {
      console.error('Failed to leave lobby:', error);
    }
  };

  const handleLoginClick = () => {
    navigate('/auth');
  };

  const desktopLayout = (
    <>
      <div className="w-80 flex-shrink-0">
        <Sidebar
          regions={regions}
          currentRegionId={currentRegionId}
          onRegionChange={handleRegionChange}
          lobbyMembers={lobbyMembers}
          lobbyCode={currentLobby?.code || null}
          currentUserId={user?.id}
          onInvite={handleInvite}
          isInviteDisabled={isCreatingInvite}
          onLeaveLobby={handleLeaveLobby}
          onJoinGame={() => setShowJoinModal(true)}
        />
      </div>
      <div className="flex-1 flex flex-col relative">
        {/* Top Right: Login/Username */}
        <div className="absolute top-8 right-8 z-20">
          {user && !user.isGuest ? (
            <div className="flex items-center gap-3">
              <span className="text-sm text-black-70 font-bold uppercase tracking-1">
                {user.username}
              </span>
              <div className="relative group">
                <button
                  onClick={logout}
                  className="text-black-70 hover:opacity-70 transition-opacity cursor-pointer"
                  aria-label="Logout"
                >
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    className="h-5 w-5"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                    strokeWidth={2}
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1"
                    />
                  </svg>
                </button>
                {/* Tooltip */}
                <div className="absolute right-0 top-full mt-2 px-2 py-1 bg-gray-800 text-white text-xs rounded whitespace-nowrap opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none">
                  Logout
                </div>
              </div>
            </div>
          ) : (
            <button
              onClick={handleLoginClick}
              className="text-sm text-black-70 font-bold uppercase tracking-1 hover:opacity-70 transition-opacity"
            >
              LOGIN
            </button>
          )}
        </div>

        {/* Center: Leaderboard Content */}
        <div className="flex-1 flex items-center justify-center px-8 overflow-y-auto">
          <LeaderboardContent
            selectedSeason={selectedSeason}
            setSelectedSeason={setSelectedSeason}
            selectedMode={selectedMode}
            setSelectedMode={setSelectedMode}
          />
        </div>

        {/* Bottom Right: Lobby Chat */}
        <LobbyChat
          title="Lobby Chat"
          messages={lobbyChatMessages}
          onSendMessage={handleSendMessage}
          currentUsername={user?.username}
          isActive={Boolean(currentLobby)}
          inactiveMessage="Join or create a lobby to chat"
          initialExpanded={true}
        />
      </div>
    </>
  );

  const mobileLayout = (
    <div className="flex-1 flex flex-col">
      <MobileHeader
        regions={regions}
        currentRegionId={currentRegionId}
        onRegionChange={handleRegionChange}
        currentUser={user}
        onLoginClick={handleLoginClick}
        lobbyUsers={lobbyMembers.map(m => m.username)}
        onInvite={handleInvite}
        isInviteDisabled={isCreatingInvite}
      />

      {/* Center: Leaderboard Content */}
      <div className="flex-1 overflow-y-auto px-4 py-8">
        <LeaderboardContent
          selectedSeason={selectedSeason}
          setSelectedSeason={setSelectedSeason}
          selectedMode={selectedMode}
          setSelectedMode={setSelectedMode}
        />
      </div>

      {/* Bottom Right: Lobby Chat */}
      <LobbyChat
        title="Lobby Chat"
        messages={lobbyChatMessages}
        onSendMessage={handleSendMessage}
        currentUsername={user?.username}
        isActive={Boolean(currentLobby)}
        inactiveMessage="Join or create a lobby to chat"
        initialExpanded={false}
      />
    </div>
  );

  return (
    <>
      <div className="min-h-screen flex home-page relative">
        {isMobile ? mobileLayout : desktopLayout}
      </div>

      {/* Invite Friends Modal */}
      <InviteFriendsModal
        isOpen={showInviteModal}
        onClose={() => setShowInviteModal(false)}
        lobbyCode={currentLobby?.code || null}
      />

      {/* Join Game Modal */}
      <JoinGameModal
        isOpen={showJoinModal}
        onClose={() => setShowJoinModal(false)}
      />
    </>
  );
};
