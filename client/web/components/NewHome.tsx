import React, { useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { Sidebar } from './Sidebar';
import { MobileHeader } from './MobileHeader';
import { GameStartForm } from './GameStartForm';
import { LobbyChat } from './LobbyChat';
import { InviteFriendsModal } from './InviteFriendsModal';
import JoinGameModal from './JoinGameModal';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import { useRegions } from '../hooks/useRegions';
import { useGameWebSocket } from '../hooks/useGameWebSocket';
import { LobbyGameMode } from '../types';

const generateGuestNickname = () => `Guest${Math.floor(1000 + Math.random() * 9000)}`;

export const NewHome: React.FC = () => {
  const navigate = useNavigate();
  const { user, login, register, createGuest, logout, updateGuestNickname } = useAuth();
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
    lobbyPreferences,
    updateLobbyPreferences,
  } = useWebSocket();
  const { currentGameId, queueForMatch, queueForMatchMulti } = useGameWebSocket();
  const [isMobile, setIsMobile] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [showInviteModal, setShowInviteModal] = useState(false);
  const [showJoinModal, setShowJoinModal] = useState(false);
  const [isCreatingInvite, setIsCreatingInvite] = useState(false);

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
  const isCurrentLobbyHost = currentLobby ? currentLobby.hostUserId === user?.id : false;
  const isGameFormHost = !currentLobby || isCurrentLobbyHost;
  const isLobbyQueued = currentLobby?.state === 'queued';

  // Check if mobile on mount and resize
  useEffect(() => {
    const checkMobile = () => {
      setIsMobile(window.innerHeight > window.innerWidth);
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

  // Navigate to game when created
  useEffect(() => {
    if (currentGameId) {
      navigate(`/play/${currentGameId}`);
    }
  }, [currentGameId, navigate]);

  useEffect(() => {
    const cleanup = onMessage('NicknameUpdated', (message: any) => {
      const updatedName = message.data?.username;
      if (!updatedName) {
        return;
      }
      if (!user || !user.isGuest) {
        return;
      }
      updateGuestNickname(updatedName);
    });

    return cleanup;
  }, [onMessage, updateGuestNickname, user]);

  const handleRegionChange = (regionId: string) => {
    selectRegion(regionId);
  };

  const handleStartGame = async (
    gameModes: LobbyGameMode[],
    nickname: string,
    isCompetitive: boolean
  ) => {
    if (isLobbyQueued || !isGameFormHost) {
      return;
    }

    setIsLoading(true);
    try {
      // If not logged in, create guest user
      if (!user) {
        try {
          await createGuest(nickname);
        } catch (error) {
          console.error('Guest creation failed:', error);
          return;
        }
      }

      // Wait for auth to propagate
      await new Promise(resolve => setTimeout(resolve, 500));

      if (!currentLobby) {
        await createLobby();
      }

      updateLobbyPreferences({
        selectedModes: gameModes,
        competitive: isCompetitive,
      });

      // Give the WebSocket a moment to broadcast lobby preferences before queuing
      await new Promise(resolve => setTimeout(resolve, 100));

      // Convert game modes to GameType format
      const gameTypes = gameModes.map(mode => {
        if (mode === 'duel') {
          return { TeamMatch: { per_team: 1 } };
        } else if (mode === '2v2') {
          return { TeamMatch: { per_team: 2 } };
        } else if (mode === 'ffa') {
          return { FreeForAll: { max_players: 8 } };
        } else {
          return 'Solo';
        }
      });

      const queueMode: 'Quickmatch' | 'Competitive' = isCompetitive ? 'Competitive' : 'Quickmatch';

      if (gameTypes.length === 1) {
        queueForMatch(gameTypes[0], queueMode);
      } else if (gameTypes.length > 1) {
        queueForMatchMulti(gameTypes, queueMode);
      }
    } catch (error) {
      console.error('Failed to start game:', error);
    } finally {
      setIsLoading(false);
    }
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
      if (!user) {
        try {
          await createGuest(generateGuestNickname());
        } catch (error) {
          console.error('Guest creation failed for lobby invite:', error);
          return;
        }

        await new Promise(resolve => setTimeout(resolve, 500));
      }

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

  const handleStartGameFromLobby = () => {
    // TODO: Queue lobby for matchmaking
    console.log('Start game from lobby - TODO: implement matchmaking');
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
          isHost={isCurrentLobbyHost}
          onInvite={handleInvite}
          isInviteDisabled={isCreatingInvite}
          onLeaveLobby={handleLeaveLobby}
          onStartGame={handleStartGameFromLobby}
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

        {/* Center: Game Start Form */}
        <div className="flex-1 flex items-center justify-center px-8">
          <GameStartForm
            onStartGame={handleStartGame}
            currentUsername={user?.username}
            isLoading={isLoading}
            isAuthenticated={user !== null && !user.isGuest}
            isHost={isGameFormHost}
            isLobbyQueued={isLobbyQueued}
            hasActiveLobby={Boolean(currentLobby)}
            lobbyPreferences={lobbyPreferences}
            onPreferencesChange={updateLobbyPreferences}
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

      {/* Center: Game Start Form */}
      <div className="flex-1 flex items-center justify-center px-4 py-8">
        <GameStartForm
          onStartGame={handleStartGame}
          currentUsername={user?.username}
          isLoading={isLoading}
          isAuthenticated={user !== null && !user.isGuest}
          isHost={isGameFormHost}
          isLobbyQueued={isLobbyQueued}
          hasActiveLobby={Boolean(currentLobby)}
          lobbyPreferences={lobbyPreferences}
          onPreferencesChange={updateLobbyPreferences}
        />
      </div>

      {/* Bottom Right: Lobby Chat - Hidden in mobile */}
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

  const shouldShowRegionReminder = !regionsLoading && !regionsError && currentRegionId === '';
  const shouldShowRegionError = Boolean(regionsError);
  const shouldShowConnectionBanner = !isConnected;
  const shouldShowRegionLoading = regionsLoading;
  const shouldShowStatusBanner =
    shouldShowRegionLoading || shouldShowRegionError || shouldShowRegionReminder || shouldShowConnectionBanner;

  return (
    <>
      <div className="min-h-screen flex home-page relative">
        {isMobile ? mobileLayout : desktopLayout}
        {shouldShowStatusBanner && (
          <div className="pointer-events-none absolute top-6 left-1/2 -translate-x-1/2 flex flex-col items-center gap-2">
            {shouldShowRegionLoading && (
              <div className="flex items-center gap-2 px-4 py-2 rounded-full bg-white border border-gray-300 text-xs font-bold uppercase tracking-1 text-black-70 shadow-sm">
                <span className="animate-spin w-4 h-4 border-2 border-gray-300 border-t-transparent rounded-full" />
                <span>Loading region data…</span>
              </div>
            )}
            {shouldShowRegionError && !shouldShowRegionLoading && (
              <div className="px-4 py-2 rounded-full bg-red-50 border border-red-200 text-xs font-bold uppercase tracking-1 text-red-600 shadow-sm">
                Failed to load regions
              </div>
            )}
            {shouldShowRegionReminder && (
              <div className="px-4 py-2 rounded-full bg-yellow-50 border border-yellow-200 text-xs font-bold uppercase tracking-1 text-yellow-700 shadow-sm">
                Select a region to continue
              </div>
            )}
            {shouldShowConnectionBanner && (
              <div className="px-4 py-2 rounded-full bg-yellow-50 border border-yellow-200 text-xs font-bold uppercase tracking-1 text-yellow-700 shadow-sm">
                Connecting to game server…
              </div>
            )}
          </div>
        )}
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
