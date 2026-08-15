import React, { useState, useEffect, useRef } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import type { AccountModalView } from './AccountModal';
import { HomeHeader } from './HomeHeader';
import { SocialFooter } from './SocialFooter';
import { LobbyChat } from './LobbyChat';
import { RegionSelector } from './RegionSelector';
import { InviteFriendsModal } from './InviteFriendsModal';
import JoinGameModal from './JoinGameModal';
import { ConnectionStatusRack } from './ConnectionStatusRack';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import { useRegions } from '../hooks/useRegions';
import { isConnectionReady } from '../utils/connectionBanner';
import { LobbyGameMode, LeaderboardEntry, UserRankingResponse, isRankingEntry, isHighScoreEntry, GameType } from '../types';
import { formatRankLabel, getRankFromMMR } from '../utils/rank';
import RankIcon from './RankIcon';
import SoloTrophyIcon from './SoloTrophyIcon';
import { api } from '../services/api';
import { useGameWebSocket } from '../hooks/useGameWebSocket';

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

/**
 * Guest nicknames are not unique and are not reserved, so the same name can
 * belong to several accounts. Naming the guest rows is what lets a player see
 * that a name-alike above them is not their own account.
 */
const GuestTag: React.FC = () => (
  <span className="flex-shrink-0 text-xs font-normal text-gray-500">(guest)</span>
);

const isValidLeaderboardMode = (mode: string | null): mode is LobbyGameMode =>
  Boolean(mode && GAME_MODES.some(gameMode => gameMode.id === mode));

const isValidLeaderboardRegion = (region: string | null): region is string =>
  Boolean(region && LEADERBOARD_REGIONS.some(availableRegion => availableRegion.id === region));

const parseSeasonParam = (value: string | null): number | null => {
  if (value == null) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const LeaderboardContent: React.FC<{
  selectedSeason: number | null;
  setSelectedSeason: React.Dispatch<React.SetStateAction<number | null>>;
  selectedMode: LobbyGameMode;
  setSelectedMode: (mode: LobbyGameMode) => void;
  selectedRegion: string;
  setSelectedRegion: (region: string) => void;
  seasons: number[];
  /**
   * Whether /api/seasons has answered (either way). Until it has, the season
   * selection is provisional, and fetching on it would issue a request whose
   * answer we are about to discard.
   */
  seasonsResolved: boolean;
  isAuthenticated: boolean;
  /**
   * The region this browser's websocket is actually connected to, or null when
   * there is no live connection. It decides which region the player's own rank
   * is reported for while Global is selected.
   */
  connectedRegionId: string | null;
  /**
   * The signed-in account, used to find its row among same-named players.
   */
  currentUserId: number | null;
}> = ({
  selectedSeason,
  setSelectedSeason,
  selectedMode,
  setSelectedMode,
  selectedRegion,
  setSelectedRegion,
  seasons,
  seasonsResolved,
  isAuthenticated,
  connectedRegionId,
  currentUserId
}) => {
  const navigate = useNavigate();
  const { queueForMatch } = useGameWebSocket();
  const { isConnected, currentLobby, createLobby, updateLobbyPreferences } = useWebSocket();
  const [leaderboardData, setLeaderboardData] = useState<LeaderboardEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [offset, setOffset] = useState(0);
  const [userRanking, setUserRanking] = useState<UserRankingResponse | null>(null);
  const LIMIT = 25;
  const [isStartingQueue, setIsStartingQueue] = useState(false);

  // Fetch the user's competitive ranking when it is relevant to the selected mode.
  useEffect(() => {
    if (!isAuthenticated || selectedMode === 'solo') {
      setUserRanking(null);
      return;
    }
    if (!seasonsResolved) {
      return;
    }

    // Selections settle over a couple of renders (the season list arrives
    // after the first paint), so several of these can be in flight at once.
    // Without this guard a slower earlier response lands last and replaces the
    // current rank with a stale one.
    let isCurrent = true;

    // Your rank is always a rank *in one region* — rankings are stored per
    // region, so there is no single global row to report. With Global
    // selected, that region is the one this browser is connected to, so the
    // badge reflects where you are playing; with no live connection the server
    // answers for its own region. An explicit region selection wins outright.
    //
    // A page load that then opens a websocket asks twice: once for a badge to
    // render immediately, and once more when the connected region is known.
    // Waiting for the socket instead would leave the badge blank for as long
    // as the connection takes, and indefinitely when it never arrives. The
    // second ask is a single keyed read, and it supersedes the first.
    const regionForOwnRank =
      selectedRegion === 'global' ? connectedRegionId ?? undefined : selectedRegion;

    const fetchUserRanking = async () => {
      try {
        const data = await api.getMyRanking(
          'competitive',
          selectedMode,
          selectedSeason ?? undefined,
          regionForOwnRank
        );
        if (isCurrent) {
          setUserRanking(data);
        }
      } catch (err) {
        // A failed request says nothing about the player's standing. Clearing
        // it here is what turned a transient error into the badge dropping to
        // UNRANKED and back; keep the last known rank instead.
        console.error('Failed to fetch user ranking:', err);
      }
    };

    fetchUserRanking();

    return () => {
      isCurrent = false;
    };
  }, [
    isAuthenticated,
    seasonsResolved,
    selectedSeason,
    selectedMode,
    selectedRegion,
    connectedRegionId,
  ]);

  // Fetch leaderboard data when filters change (always use competitive mode).
  //
  // Gated on the season list so a page load issues exactly one request. The
  // season starts provisional — parsed from the URL, or absent — and only
  // becomes final once /api/seasons answers; fetching before then produced up
  // to three requests for the same table.
  useEffect(() => {
    if (!seasonsResolved) {
      return;
    }

    const fetchLeaderboard = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await api.getLeaderboard(
          'competitive', // Only show competitive (ranked) MMR
          selectedMode,
          selectedSeason ?? undefined,
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
  }, [seasonsResolved, selectedSeason, selectedMode, selectedRegion, offset]);

  // Reset offset when filters change
  useEffect(() => {
    setOffset(0);
  }, [selectedSeason, selectedMode, selectedRegion]);

  const handleLoadMore = () => {
    setOffset(prev => prev + LIMIT);
  };

  const toGameType = (mode: LobbyGameMode): GameType => {
    switch (mode) {
      case 'duel':
        return { TeamMatch: { per_team: 1 } };
      case '2v2':
        return { TeamMatch: { per_team: 2 } };
      case 'ffa':
        return { FreeForAll: { max_players: 4 } };
      default:
        return 'Solo';
    }
  };

  const startSelectedQueue = async (competitive: boolean) => {
    if (isStartingQueue) {
      return;
    }

    const modeToStart = selectedMode;
    setIsStartingQueue(true);
    try {
      if (!isConnected) {
        throw new Error('Not connected to game server');
      }

      if (!currentLobby) {
        await createLobby();
      }

      updateLobbyPreferences({
        selectedModes: [modeToStart],
        competitive,
      });

      // Small delay so the server registers preference updates
      await new Promise(resolve => setTimeout(resolve, 100));

      queueForMatch(toGameType(modeToStart), competitive ? 'Competitive' : 'Quickmatch');
    } catch (err) {
      console.error('Failed to start matchmaking:', err);
    } finally {
      setIsStartingQueue(false);
    }
  };

  const rank = userRanking?.mmr != null ? getRankFromMMR(userRanking.mmr) : null;
  const rankTier = rank?.tier ?? 'unranked';
  const hasCompetitiveMMR = Boolean(rank);
  const rankLabel = rank ? formatRankLabel(rank) : 'UNRANKED';
  const isSoloMode = selectedMode === 'solo';
  const selectedModeLabel =
    GAME_MODES.find(mode => mode.id === selectedMode)?.label ?? selectedMode.toUpperCase();
  const playActionLabel = isSoloMode ? 'Play Snake Now' : 'Play Ranked Now';
  const startingActionLabel = isSoloMode ? 'Starting game...' : 'Starting matchmaking...';

  const playSelectedMode = () => {
    const competitive = !isSoloMode;

    if (!isAuthenticated) {
      updateLobbyPreferences({
        selectedModes: [selectedMode],
        competitive,
      });
      navigate('/');
      return;
    }

    void startSelectedQueue(competitive);
  };

  return (
    <div className="leaderboard-content w-full max-w-4xl mx-auto px-4 py-8">
      {/* Header row with rank and selectors */}
      <div className="leaderboard-summary flex flex-col md:flex-row md:items-start md:justify-between gap-6 mb-8">
        {/* Keep a stable left summary column as the selected game mode changes. */}
        <div className="leaderboard-mode-summary flex items-start gap-4">
          {isSoloMode ? (
            <SoloTrophyIcon
              className="w-16 h-16 flex-shrink-0"
              label="Classic Snake high scores"
            />
          ) : (
            rank ? (
              <RankIcon
                tier={rank.tier}
                division={rank.division}
                label={rankLabel}
                className="w-16 h-16 flex-shrink-0"
              />
            ) : (
              <RankIcon
                tier="unranked"
                label={rankLabel}
                className="w-16 h-16 flex-shrink-0"
              />
            )
          )}
          <div className="flex flex-col gap-1">
            <div className="text-xs font-bold uppercase tracking-wider text-gray-500 px-1">
              {isSoloMode ? 'Classic Snake' : isAuthenticated ? 'Your Rank' : 'Ranked Play'}
            </div>
            <div className="font-black italic tracking-1 text-lg text-black-70">
              {isSoloMode ? 'SOLO' : isAuthenticated ? rankLabel : selectedModeLabel}
            </div>
            <div className="text-xs text-black-70">
              {!isSoloMode && isAuthenticated && hasCompetitiveMMR && userRanking?.mmr != null ? (
                `${userRanking.mmr} MMR`
              ) : (
                <button
                  type="button"
                  onClick={playSelectedMode}
                  disabled={isStartingQueue}
                  className="font-bold text-blue-600 hover:underline disabled:opacity-60 disabled:cursor-not-allowed"
                >
                  {isStartingQueue ? startingActionLabel : playActionLabel}
                </button>
              )}
            </div>
          </div>
        </div>

        {/* Selectors (right side) */}
        <div className="flex flex-col sm:flex-row gap-6">
        {/* Region Selector */}
        <div className="flex flex-col gap-1">
          <label className="text-xs font-bold uppercase tracking-wider text-gray-500 px-1">
            Region
          </label>
          <div className="leaderboard-control-frame relative h-[38px]">
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
          <div className="leaderboard-control-frame relative h-[38px]">
            <select
              value={selectedSeason != null ? selectedSeason.toString() : ''}
              onChange={(e) => {
                const parsedSeason = parseSeasonParam(e.target.value);
                if (parsedSeason != null) {
                  setSelectedSeason(parsedSeason);
                }
              }}
              className="w-full sm:w-auto h-full px-4 pr-8 border-2 border-gray-300 rounded-lg bg-white
                         font-black italic uppercase tracking-1 text-sm text-black-70
                         focus:outline-none focus:border-blue-500 cursor-pointer
                         appearance-none"
            >
              {seasons.map((season) => (
                <option key={season} value={season.toString()}>
                  {season.toString()}
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
          <div className="leaderboard-control-frame grid grid-cols-4 gap-2 h-[38px]">
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
      <div className="leaderboard-table bg-white border-2 border-gray-300 rounded-lg overflow-hidden">
        {/* Table Header */}
        {selectedMode === 'solo' ? (
          // Solo mode header - show Score and Date instead of MMR/Wins/Losses
          <div className="leaderboard-grid leaderboard-grid--solo grid grid-cols-[50px_1fr_120px_150px] gap-2 px-4 py-3 bg-gray-50 border-b-2 border-gray-300">
            <div className="font-black uppercase tracking-1 text-xs text-black-70">#</div>
            <div className="font-black uppercase tracking-1 text-xs text-black-70">Player</div>
            <div className="font-black uppercase tracking-1 text-xs text-black-70 text-right">Score</div>
            <div className="font-black uppercase tracking-1 text-xs text-black-70 text-right hidden sm:block">Date</div>
          </div>
        ) : (
          // Other modes header - show MMR, Wins, Losses, Win%
          <div className="leaderboard-grid leaderboard-grid--ranked grid grid-cols-[50px_1fr_100px_80px_80px_80px] gap-2 px-4 py-3 bg-gray-50 border-b-2 border-gray-300">
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
                    className="leaderboard-grid leaderboard-grid--solo grid grid-cols-[50px_1fr_120px_150px] gap-2 px-4 py-3 hover:bg-gray-50 transition-colors"
                  >
                    {/* Rank */}
                    <div className="flex items-center">
                      <span className="font-black text-base text-black-70">{entry.rank}</span>
                    </div>

                    {/* Username */}
                    <div className="flex items-center gap-1 font-bold text-sm text-black-70 min-w-0">
                      <span className="truncate">{entry.username}</span>
                      {entry.isGuest && <GuestTag />}
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
                const entryRank = getRankFromMMR(entry.mmr);
                const entryRankLabel = formatRankLabel(entryRank);
                // Matched on the account, never on the rank number: this board
                // may be global while the badge above it reports one region,
                // so their rank numbers describe different ladders.
                const isOwnRow = currentUserId != null && entry.userId === currentUserId;

                return (
                  <div
                    key={entry.rank}
                    data-own-row={isOwnRow ? 'true' : undefined}
                    className={`leaderboard-grid leaderboard-grid--ranked grid grid-cols-[50px_1fr_100px_80px_80px_80px] gap-2 px-4 py-3 transition-colors ${
                      isOwnRow ? 'bg-blue-50' : 'hover:bg-gray-50'
                    }`}
                  >
                    {/* Rank */}
                    <div className="flex items-center">
                      <span className="font-black text-base text-black-70">{entry.rank}</span>
                    </div>

                    {/* Username */}
                    <div className="flex items-center gap-2 font-bold text-sm text-black-70 min-w-0">
                      <RankIcon
                        tier={entryRank.tier}
                        division={entryRank.division}
                        label={`${entryRankLabel} icon`}
                        className="w-6 h-6 flex-shrink-0"
                      />
                      <span className="truncate">{entry.username}</span>
                      {entry.isGuest && <GuestTag />}
                      {isOwnRow && (
                        <span className="flex-shrink-0 px-1.5 py-0.5 rounded bg-blue-600 text-white font-black italic uppercase tracking-1 text-[10px]">
                          You
                        </span>
                      )}
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

interface LeaderboardProps {
  onOpenAuth: () => void;
  onOpenAccount: (view: AccountModalView) => void;
}

export const Leaderboard: React.FC<LeaderboardProps> = ({ onOpenAuth, onOpenAccount }) => {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const { user, logout } = useAuth();
  const {
    connectToRegion,
    isConnected,
    isSessionAuthenticated,
    onMessage,
    currentRegionUrl,
    currentLobby,
    lobbyMembers,
    createLobby,
    leaveLobby,
    lobbyChatMessages,
    sendChatMessage,
  } = useWebSocket();
  const [showInviteModal, setShowInviteModal] = useState(false);
  const [showJoinModal, setShowJoinModal] = useState(false);
  const [isCreatingInvite, setIsCreatingInvite] = useState(false);
  const [seasons, setSeasons] = useState<number[]>([]);
  // Latched on the first answer from /api/seasons, success or failure. A
  // failure still resolves the selection: `null` means "current season", which
  // is what the API defaults to anyway, so the page loads either way.
  const [seasonsResolved, setSeasonsResolved] = useState(false);
  const [selectedSeason, setSelectedSeason] = useState<number | null>(() => parseSeasonParam(searchParams.get('season')));
  const currentSeasonRef = useRef<number | null>(null);
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
  // Where this browser is actually playing, as opposed to which region is
  // merely selected: the socket may still be connecting or connected
  // elsewhere. Only a live connection can speak for the player's own rank.
  const connectedRegionId = isConnected
    ? regions.find(region => region.wsUrl === currentRegionUrl)?.id ?? null
    : null;

  // Refresh this lightweight, clock-derived endpoint so a tab left open over
  // a UTC quarter boundary adopts the newly rolled season without reloading.
  useEffect(() => {
    let active = true;
    let latestRequest = 0;
    const fetchSeasons = async () => {
      const request = ++latestRequest;
      try {
        const data = await api.getSeasons();
        if (!active || request !== latestRequest) {
          return;
        }
        const previousCurrent = currentSeasonRef.current;
        currentSeasonRef.current = data.current;
        setSeasons(data.seasons);
        setSelectedSeason(prev => {
          if (previousCurrent != null && prev === previousCurrent) {
            return data.current;
          }
          if (prev != null && data.seasons.includes(prev)) {
            return prev;
          }
          if (data.current != null) {
            return data.current;
          }
          return data.seasons[0] ?? null;
        });
        if (previousCurrent != null && previousCurrent !== data.current) {
          setSearchParams(previous => {
            if (parseSeasonParam(previous.get('season')) !== previousCurrent) {
              return previous;
            }
            const next = new URLSearchParams(previous);
            next.set('season', data.current.toString());
            return next;
          }, { replace: true });
        }
      } catch (err) {
        console.error('Failed to fetch seasons:', err);
      } finally {
        if (active && request === latestRequest) {
          setSeasonsResolved(true);
        }
      }
    };

    const handleVisibilityChange = () => {
      if (document.visibilityState === 'visible') {
        void fetchSeasons();
      }
    };
    void fetchSeasons();
    const intervalId = window.setInterval(fetchSeasons, 60_000);
    document.addEventListener('visibilitychange', handleVisibilityChange);

    return () => {
      active = false;
      window.clearInterval(intervalId);
      document.removeEventListener('visibilitychange', handleVisibilityChange);
    };
  }, [setSearchParams]);

  // Sync local selections from URL (and season list) whenever the URL changes
  useEffect(() => {
    const modeFromQuery = searchParams.get('mode');
    const regionFromQuery = searchParams.get('region');
    const seasonFromQuery = parseSeasonParam(searchParams.get('season'));

    const resolvedMode: LobbyGameMode = isValidLeaderboardMode(modeFromQuery)
      ? modeFromQuery
      : DEFAULT_LEADERBOARD_MODE;

    const resolvedRegion = isValidLeaderboardRegion(regionFromQuery)
      ? regionFromQuery
      : DEFAULT_LEADERBOARD_REGION;

    setSelectedMode(prev => (prev === resolvedMode ? prev : resolvedMode));
    setSelectedLeaderboardRegion(prev => (prev === resolvedRegion ? prev : resolvedRegion));

    // An empty list means /api/seasons has not answered yet, not that the URL's
    // season is invalid. Discarding it here used to blank the selection on
    // mount and then restore it a moment later, which is what turned one page
    // load into three leaderboard requests.
    if (seasons.length === 0) {
      return;
    }

    const resolvedSeason =
      seasonFromQuery != null && seasons.includes(seasonFromQuery)
        ? seasonFromQuery
        : seasons[0];

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

    const seasonFromParams = parseSeasonParam(params.get('season'));
    const resolvedSeason =
      selectedSeason != null && seasons.includes(selectedSeason)
        ? selectedSeason
        : seasonFromParams != null && seasons.includes(seasonFromParams)
          ? seasonFromParams
          : seasons[0];

    const resolvedSeasonString = resolvedSeason != null ? resolvedSeason.toString() : '';
    if (params.get('season') !== resolvedSeasonString) {
      params.set('season', resolvedSeasonString);
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

  const isReady = isConnectionReady({
    isConnected,
    isSessionAuthenticated,
    hasIdentity: user !== null,
  });

  return (
    <>
      <div className="home-page leaderboard-page">
        <HomeHeader
          activePage="leaderboards"
          currentUser={user}
          lobbyMembers={lobbyMembers}
          hasLobby={Boolean(currentLobby)}
          isInviteDisabled={isCreatingInvite}
          onInvite={handleInvite}
          onJoinGame={() => setShowJoinModal(true)}
          onLeaveLobby={handleLeaveLobby}
          onAuthClick={onOpenAuth}
          onOpenAccount={onOpenAccount}
          onLogout={logout}
        />

        <ConnectionStatusRack
          isReady={isReady}
          regionsLoading={regionsLoading}
          regionsError={regionsError}
          hasSelectedRegion={currentRegionId !== ''}
        />

        <main className="leaderboard-main">
          <LeaderboardContent
            selectedSeason={selectedSeason}
            setSelectedSeason={setSelectedSeason}
            selectedMode={selectedMode}
            setSelectedMode={setSelectedMode}
            selectedRegion={selectedLeaderboardRegion}
            setSelectedRegion={setSelectedLeaderboardRegion}
            seasons={seasons}
            seasonsResolved={seasonsResolved}
            isAuthenticated={Boolean(user)}
            connectedRegionId={connectedRegionId}
            currentUserId={user?.id ?? null}
          />
        </main>

        <SocialFooter />

        <div className="home-utility-dock">
          <RegionSelector
            regions={regions}
            currentRegionId={currentRegionId}
            onRegionChange={handleRegionChange}
            placement="top"
          />
        </div>

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
