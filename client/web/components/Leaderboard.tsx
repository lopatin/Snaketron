import React, { useState, useEffect } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { Sidebar } from './Sidebar';
import { MobileHeader } from './MobileHeader';
import { LobbyChat } from './LobbyChat';
import { InviteFriendsModal } from './InviteFriendsModal';
import JoinGameModal from './JoinGameModal';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import { useRegions } from '../hooks/useRegions';
import { LobbyGameMode, RankTier, RankDivision, Rank, LeaderboardEntry, UserRankingResponse, isRankingEntry, isHighScoreEntry } from '../types';
import { api } from '../services/api';

const generateGuestNickname = () => `Guest${Math.floor(1000 + Math.random() * 9000)}`;

const DEFAULT_LEADERBOARD_REGION = 'global';
const DEFAULT_LEADERBOARD_MODE: LobbyGameMode = 'duel';

const LEADERBOARD_REGIONS = [
  { id: 'global', label: 'Global' },
  { id: 'us-east-1', label: 'US East' },
  { id: 'eu-west-1', label: 'EU West' },
];

const GAME_MODES: Array<{ id: LobbyGameMode; label: string }> = [
  { id: 'duel', label: 'DUEL' },
  { id: '2v2', label: '2V2' },
  { id: 'solo', label: 'SOLO' },
  { id: 'ffa', label: 'FFA' },
];

const isValidLeaderboardMode = (mode: string | null): mode is LobbyGameMode =>
  Boolean(mode && GAME_MODES.some(gameMode => gameMode.id === mode));

const isValidLeaderboardRegion = (region: string | null) =>
  Boolean(region && LEADERBOARD_REGIONS.some(availableRegion => availableRegion.id === region));

// Helper to determine rank tier from MMR
const getRankTierFromMMR = (mmr: number): RankTier => {
  if (mmr >= 2400) return 'grandmaster';
  if (mmr >= 2200) return 'master';
  if (mmr >= 2000) return 'diamond';
  if (mmr >= 1800) return 'platinum';
  if (mmr >= 1600) return 'gold';
  if (mmr >= 1400) return 'silver';
  return 'bronze';
};

const getRankImage = (tier: RankTier | 'unranked'): string => {
  if (tier === 'unranked') return '/images/unranked.png';
  const imageTier = tier === 'master' ? 'grandmaster' : tier;
  return `/images/${imageTier}.png`;
};

const LeaderboardContent: React.FC<{
  selectedSeason: string;
  setSelectedSeason: (season: string) => void;
  selectedMode: LobbyGameMode;
  setSelectedMode: (mode: LobbyGameMode) => void;
  selectedRegion: string;
  setSelectedRegion: (region: string) => void;
  seasons: string[];
  isAuthenticated: boolean;
}> = ({
  selectedSeason,
  setSelectedSeason,
  selectedMode,
  setSelectedMode,
  selectedRegion,
  setSelectedRegion,
  seasons,
  isAuthenticated
}) => {
  const [leaderboardData, setLeaderboardData] = useState<LeaderboardEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [offset, setOffset] = useState(0);
  const [userRanking, setUserRanking] = useState<UserRankingResponse | null>(null);
  const LIMIT = 25;

  // Fetch user's ranking when authenticated and filters change
  useEffect(() => {
    if (!isAuthenticated) {
      setUserRanking(null);
      return;
    }

    const fetchUserRanking = async () => {
      try {
        const data = await api.getMyRanking(
          'competitive',
          selectedMode,
          selectedSeason || undefined,
          selectedRegion === 'global' ? undefined : selectedRegion
        );
        setUserRanking(data);
      } catch (err) {
        console.error('Failed to fetch user ranking:', err);
        setUserRanking(null);
      }
    };

    fetchUserRanking();
  }, [isAuthenticated, selectedSeason, selectedMode, selectedRegion]);

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

  return (
    <div className="w-full max-w-4xl mx-auto px-4 py-8">
      {/* Header row with rank and selectors */}
      <div className="flex flex-col md:flex-row md:items-end md:justify-between gap-6 mb-8">
        {/* Your Rank Display (left side) - Not shown for Solo mode */}
        {isAuthenticated && selectedMode !== 'solo' ? (
          <div className="flex items-center gap-3">
            <img
              src={getRankImage(userRanking?.mmr ? getRankTierFromMMR(userRanking.mmr) : 'unranked')}
              alt={userRanking?.mmr ? getRankTierFromMMR(userRanking.mmr) : 'unranked'}
              className="w-12 h-12 object-contain"
            />
            <div>
              <div className="text-xs uppercase tracking-1 text-gray-500 font-bold">Your Rank</div>
              {userRanking?.rank ? (
                <>
                  <div className="font-black italic uppercase tracking-1 text-lg text-black-70">
                    #{userRanking.rank}
                  </div>
                  <div className="text-xs text-black-70">{userRanking.mmr} MMR</div>
                </>
              ) : (
                <div className="font-black italic uppercase tracking-1 text-lg text-black-70">
                  UNRANKED
                </div>
              )}
            </div>
          </div>
        ) : (
          // Empty div to maintain flex layout spacing
          <div className="hidden md:block"></div>
        )}

        {/* Selectors (right side) */}
        <div className="flex flex-col sm:flex-row gap-6">
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
              {LEADERBOARD_REGIONS.map((region) => (
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
              value={selectedSeason || ''}
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
            {GAME_MODES.map((mode) => {
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
      </div>

      {/* Leaderboard Table */}
      <div className="bg-white border-2 border-gray-300 rounded-lg overflow-hidden">
        {/* Table Header */}
        {selectedMode === 'solo' ? (
          // Solo mode header - show Score and Date instead of MMR/Wins/Losses
          <div className="grid grid-cols-[50px_1fr_120px_150px] gap-2 px-4 py-3 bg-gray-50 border-b-2 border-gray-300">
            <div className="font-black uppercase tracking-1 text-xs text-black-70">#</div>
            <div className="font-black uppercase tracking-1 text-xs text-black-70">Player</div>
            <div className="font-black uppercase tracking-1 text-xs text-black-70 text-right">Score</div>
            <div className="font-black uppercase tracking-1 text-xs text-black-70 text-right hidden sm:block">Date</div>
          </div>
        ) : (
          // Other modes header - show MMR, Wins, Losses, Win%
          <div className="grid grid-cols-[50px_1fr_100px_80px_80px_80px] gap-2 px-4 py-3 bg-gray-50 border-b-2 border-gray-300">
            <div className="font-black uppercase tracking-1 text-xs text-black-70">#</div>
            <div className="font-black uppercase tracking-1 text-xs text-black-70">Player</div>
            <div className="font-black uppercase tracking-1 text-xs text-black-70 text-right">MMR</div>
            <div className="font-black uppercase tracking-1 text-xs text-black-70 text-right hidden sm:block">Wins</div>
            <div className="font-black uppercase tracking-1 text-xs text-black-70 text-right hidden sm:block">Losses</div>
            <div className="font-black uppercase tracking-1 text-xs text-black-70 text-right">Win %</div>
          </div>
        )}

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
            leaderboardData.map((entry) => {
              // Check if this is a high score entry (Solo mode) or ranking entry
              if (isHighScoreEntry(entry)) {
                // Render Solo mode entry
                const date = new Date(entry.timestamp);
                const formattedDate = date.toLocaleDateString('en-US', {
                  month: 'short',
                  day: 'numeric',
                  year: 'numeric'
                });

                return (
                  <div
                    key={`${entry.gameId}-${entry.rank}`}
                    className="grid grid-cols-[50px_1fr_120px_150px] gap-2 px-4 py-3 hover:bg-gray-50 transition-colors"
                  >
                    {/* Rank */}
                    <div className="flex items-center">
                      <span className="font-black text-base text-black-70">{entry.rank}</span>
                    </div>

                    {/* Username */}
                    <div className="flex items-center font-bold text-sm text-black-70 truncate">
                      {entry.username}
                    </div>

                    {/* Score */}
                    <div className="flex items-center justify-end font-black italic text-base text-black-70">
                      {entry.score}
                    </div>

                    {/* Date (hidden on mobile) */}
                    <div className="hidden sm:flex items-center justify-end text-sm text-black-70">
                      {formattedDate}
                    </div>
                  </div>
                );
              } else if (isRankingEntry(entry)) {
                // Render ranking entry (Duel, 2v2, FFA)
                return (
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
                );
              }
              return null;
            })
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
  const [searchParams, setSearchParams] = useSearchParams();
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
  const [seasons, setSeasons] = useState<string[]>([]);
  const [selectedSeason, setSelectedSeason] = useState<string>(() => searchParams.get('season') || '');
  const [selectedMode, setSelectedMode] = useState<LobbyGameMode>(() => {
    const queryMode = searchParams.get('mode');
    return isValidLeaderboardMode(queryMode) ? queryMode : DEFAULT_LEADERBOARD_MODE;
  });
  const [selectedLeaderboardRegion, setSelectedLeaderboardRegion] = useState<string>(() => {
    const queryRegion = searchParams.get('region');
    return isValidLeaderboardRegion(queryRegion) ? queryRegion : DEFAULT_LEADERBOARD_REGION;
  });

  // Use regions hook for live data
  const {
    regions,
    selectedRegion: selectedWsRegion,
    selectRegion,
    isLoading: regionsLoading,
    error: regionsError,
  } = useRegions({
    isWebSocketConnected: isConnected,
    onMessage,
  });
  const currentRegionId = selectedWsRegion?.id ?? regions[0]?.id ?? '';

  // Check if mobile on mount and resize
  useEffect(() => {
    const checkMobile = () => {
      setIsMobile(window.innerWidth < 800);
    };
    checkMobile();
    window.addEventListener('resize', checkMobile);
    return () => window.removeEventListener('resize', checkMobile);
  }, []);

  // Fetch seasons once so we can hydrate the query defaults
  useEffect(() => {
    const fetchSeasons = async () => {
      try {
        const data = await api.getSeasons();
        setSeasons(data.seasons);
      } catch (err) {
        console.error('Failed to fetch seasons:', err);
      }
    };

    fetchSeasons();
  }, []);

  // Sync local selections from URL (and season list) whenever the URL changes
  useEffect(() => {
    const modeFromQuery = searchParams.get('mode');
    const regionFromQuery = searchParams.get('region');
    const seasonFromQuery = searchParams.get('season');

    const resolvedMode: LobbyGameMode = isValidLeaderboardMode(modeFromQuery)
      ? modeFromQuery
      : DEFAULT_LEADERBOARD_MODE;

    const resolvedRegion = isValidLeaderboardRegion(regionFromQuery)
      ? regionFromQuery
      : DEFAULT_LEADERBOARD_REGION;

    const resolvedSeason =
      seasons.length === 0
        ? ''
        : seasonFromQuery && seasons.includes(seasonFromQuery)
          ? seasonFromQuery
          : seasons[0];

    setSelectedMode(prev => (prev === resolvedMode ? prev : resolvedMode));
    setSelectedLeaderboardRegion(prev => (prev === resolvedRegion ? prev : resolvedRegion));
    setSelectedSeason(prev => (prev === resolvedSeason ? prev : resolvedSeason));
  }, [searchParams, seasons]);

  // Keep the URL in sync with the current selections
  useEffect(() => {
    if (seasons.length === 0) {
      return;
    }

    const params = new URLSearchParams(searchParams);
    let hasChanged = false;

    if (!isValidLeaderboardMode(params.get('mode'))) {
      params.set('mode', DEFAULT_LEADERBOARD_MODE);
      hasChanged = true;
    }
    if (params.get('mode') !== selectedMode) {
      params.set('mode', selectedMode);
      hasChanged = true;
    }

    if (!isValidLeaderboardRegion(params.get('region'))) {
      params.set('region', DEFAULT_LEADERBOARD_REGION);
      hasChanged = true;
    }
    if (params.get('region') !== selectedLeaderboardRegion) {
      params.set('region', selectedLeaderboardRegion);
      hasChanged = true;
    }

    const season = selectedSeason || params.get('season');
    const resolvedSeason =
      season && seasons.includes(season) ? season : seasons[0];

    if (params.get('season') !== resolvedSeason) {
      params.set('season', resolvedSeason);
      hasChanged = true;
    }

    if (hasChanged) {
      setSearchParams(params, { replace: true });
    }
  }, [selectedSeason, selectedMode, selectedLeaderboardRegion, searchParams, seasons, setSearchParams]);

  // Connect to selected region when it changes
  useEffect(() => {
    if (!selectedWsRegion) {
      return;
    }

    if (currentRegionUrl === selectedWsRegion.wsUrl) {
      return;
    }

    console.log('Connecting to region:', selectedWsRegion.name, selectedWsRegion.wsUrl);
    connectToRegion(selectedWsRegion.wsUrl, {
      regionId: selectedWsRegion.id,
      origin: selectedWsRegion.origin,
    });
  }, [selectedWsRegion?.id, selectedWsRegion?.wsUrl, selectedWsRegion?.origin, connectToRegion, currentRegionUrl]);

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
            selectedRegion={selectedLeaderboardRegion}
            setSelectedRegion={setSelectedLeaderboardRegion}
            seasons={seasons}
            isAuthenticated={Boolean(user && !user.isGuest)}
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
          selectedRegion={selectedLeaderboardRegion}
          setSelectedRegion={setSelectedLeaderboardRegion}
          seasons={seasons}
          isAuthenticated={Boolean(user && !user.isGuest)}
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
