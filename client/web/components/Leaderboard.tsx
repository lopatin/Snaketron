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
import { api } from '../services/api';

const generateGuestNickname = () => `Guest${Math.floor(1000 + Math.random() * 9000)}`;

const LeaderboardContent: React.FC<{
  selectedSeason: string;
  setSelectedSeason: (season: string) => void;
  selectedMode: LobbyGameMode;
  setSelectedMode: (mode: LobbyGameMode) => void;
}> = ({ selectedSeason, setSelectedSeason, selectedMode, setSelectedMode }) => {
  const [selectedRegion, setSelectedRegion] = useState<string>('global');
  const [seasons, setSeasons] = useState<string[]>([]);
  const [leaderboardData, setLeaderboardData] = useState<LeaderboardEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [offset, setOffset] = useState(0);
  const LIMIT = 25;

  // Available regions for filtering
  const regions = [
    { id: 'global', label: 'Global' },
    { id: 'us-east-1', label: 'US East' },
    { id: 'eu-west-1', label: 'EU West' },
  ];

  // Fetch seasons on mount
  useEffect(() => {
    const fetchSeasons = async () => {
      try {
        const data = await api.getSeasons();
        setSeasons(data.seasons);
        if (data.seasons.length > 0 && !selectedSeason) {
          setSelectedSeason(data.current);
        }
      } catch (err) {
        console.error('Failed to fetch seasons:', err);
      }
    };
    fetchSeasons();
  }, []);

  // Fetch leaderboard data when filters change (always use competitive mode)
  useEffect(() => {
    const fetchLeaderboard = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await api.getLeaderboard(
          'competitive', // Only show competitive (ranked) MMR
          selectedMode,
          selectedSeason || undefined,
          LIMIT,
          offset,
          selectedRegion === 'global' ? undefined : selectedRegion
        );
        if (offset === 0) {
          setLeaderboardData(data.entries);
        } else {
          setLeaderboardData(prev => [...prev, ...data.entries]);
        }
        setHasMore(data.hasMore);
      } catch (err) {
        console.error('Failed to fetch leaderboard:', err);
        setError('Failed to load leaderboard data');
        setLeaderboardData([]);
      } finally {
        setLoading(false);
      }
    };

    fetchLeaderboard();
  }, [selectedSeason, selectedMode, selectedRegion, offset]);

  // Reset offset when filters change
  useEffect(() => {
    setOffset(0);
  }, [selectedSeason, selectedMode, selectedRegion]);

  const handleLoadMore = () => {
    setOffset(prev => prev + LIMIT);
  };

  const gameModes: Array<{ id: LobbyGameMode; label: string }> = [
    { id: 'duel', label: 'DUEL' },
    { id: '2v2', label: '2V2' },
    { id: 'solo', label: 'SOLO' },
    { id: 'ffa', label: 'FFA' },
  ];

  return (
    <div className="w-full max-w-4xl mx-auto px-4 py-8">
      {/* Header row with selectors */}
      <div className="flex flex-col md:flex-row md:items-end md:justify-end gap-6 mb-8">
        {/* Region Selector */}
        <div className="flex flex-col gap-1">
          <label className="text-xs font-bold uppercase tracking-wider text-gray-500 px-1">
            Region
          </label>
          <div className="relative h-[38px]">
            <select
              value={selectedRegion}
              onChange={(e) => setSelectedRegion(e.target.value)}
              className="w-full sm:w-auto h-full px-4 pr-8 border-2 border-gray-300 rounded-lg bg-white
                         font-black italic uppercase tracking-1 text-sm text-black-70
                         focus:outline-none focus:border-blue-500 cursor-pointer
                         appearance-none"
            >
              {regions.map((region) => (
                <option key={region.id} value={region.id}>
                  {region.label}
                </option>
              ))}
            </select>
            <div className="absolute right-2 top-1/2 -translate-y-1/2 pointer-events-none">
              <svg className="w-4 h-4 text-black-70" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
              </svg>
            </div>
          </div>
        </div>

        {/* Season Selector */}
        <div className="flex flex-col gap-1">
          <label className="text-xs font-bold uppercase tracking-wider text-gray-500 px-1">
            Season
          </label>
          <div className="relative h-[38px]">
            <select
              value={selectedSeason}
              onChange={(e) => setSelectedSeason(e.target.value)}
              className="w-full sm:w-auto h-full px-4 pr-8 border-2 border-gray-300 rounded-lg bg-white
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
        </div>

        {/* Game Mode Selector */}
        <div className="flex flex-col gap-1">
          <label className="text-xs font-bold uppercase tracking-wider text-gray-500 px-1">
            Game Mode
          </label>
          <div className="grid grid-cols-4 gap-2 h-[38px]">
            {gameModes.map((mode) => {
              const isSelected = selectedMode === mode.id;
              return (
                <button
                  key={mode.id}
                  type="button"
                  onClick={() => setSelectedMode(mode.id)}
                  className={`
                    h-full px-3 rounded-lg font-black italic uppercase tracking-1 text-xs
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
          {loading && offset === 0 ? (
            <div className="px-4 py-12 text-center text-black-70">
              Loading...
            </div>
          ) : error ? (
            <div className="px-4 py-12 text-center text-red-600">
              {error}
            </div>
          ) : leaderboardData.length === 0 ? (
            <div className="px-4 py-12 text-center text-black-70">
              No players have been ranked yet in this mode.
            </div>
          ) : (
            leaderboardData.map((entry) => (
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
            ))
          )}
        </div>
      </div>

      {/* Load More Button */}
      {hasMore && !loading && (
        <div className="mt-6 text-center">
          <button
            type="button"
            onClick={handleLoadMore}
            className="px-6 py-2 border-2 border-gray-300 rounded-lg bg-white text-black-70
                       font-black italic uppercase tracking-1 text-sm
                       hover:border-gray-400 transition-all"
          >
            LOAD MORE
          </button>
        </div>
      )}

      {/* Loading More Indicator */}
      {loading && offset > 0 && (
        <div className="mt-6 text-center text-black-70">
          Loading more...
        </div>
      )}
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
