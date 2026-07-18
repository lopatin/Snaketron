import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { HomeHeader } from './HomeHeader';
import { GameStartForm } from './GameStartForm';
import { SocialFooter } from './SocialFooter';
import { LobbyChat } from './LobbyChat';
import { RegionSelector } from './RegionSelector';
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
  const { user, createGuest, logout, updateGuestNickname } = useAuth();
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
    lobbyPreferences,
    updateLobbyPreferences,
  } = useWebSocket();
  const { currentGameId, queueForMatch, queueForMatchMulti } = useGameWebSocket();
  const [isLoading, setIsLoading] = useState(false);
  const [showInviteModal, setShowInviteModal] = useState(false);
  const [showJoinModal, setShowJoinModal] = useState(false);
  const [isCreatingInvite, setIsCreatingInvite] = useState(false);

  const waitForConnection = async (timeoutMs = 5000) => {
    const start = Date.now();
    // Trigger a reconnect if we have region data but no active socket
    if (!isConnected) {
      const target = selectedRegion ?? regions[0];
      if (target && currentRegionUrl !== target.wsUrl) {
        connectToRegion(target.wsUrl, { regionId: target.id, origin: target.origin });
      }
    }

    while (Date.now() - start < timeoutMs) {
      if (isConnected && (!user || isSessionAuthenticated)) {
        return true;
      }
      await new Promise(resolve => setTimeout(resolve, 200));
    }
    return false;
  };

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
  const isLobbyQueued = currentLobby?.state === 'queued';

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
    if (isLobbyQueued) {
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
          return { FreeForAll: { max_players: 4 } };
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

      const connected = await waitForConnection();
      if (!connected) {
        console.error('Could not establish WebSocket connection for lobby invite');
        return;
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

  const handleLoginClick = () => {
    navigate('/auth');
  };

  const shouldShowRegionReminder = !regionsLoading && !regionsError && currentRegionId === '';
  const shouldShowRegionError = Boolean(regionsError);
  const shouldShowConnectionBanner = !isConnected;
  const shouldShowRegionLoading = regionsLoading;
  const shouldShowStatusBanner =
    shouldShowRegionLoading || shouldShowRegionError || shouldShowRegionReminder || shouldShowConnectionBanner;

  return (
    <>
      <div className="home-page">
        <HomeHeader
          currentUser={user}
          lobbyMembers={lobbyMembers}
          hasLobby={Boolean(currentLobby)}
          isInviteDisabled={isCreatingInvite}
          onInvite={handleInvite}
          onJoinGame={() => setShowJoinModal(true)}
          onLeaveLobby={handleLeaveLobby}
          onLoginClick={handleLoginClick}
          onLogout={logout}
        />

        {shouldShowStatusBanner && (
          <div className="home-status-rack" aria-live="polite" aria-label="Connection status">
            {shouldShowRegionLoading && (
              <div className="home-status-badge">
                <span className="home-status-spinner" aria-hidden="true" />
                <span>Loading region data…</span>
              </div>
            )}
            {shouldShowRegionError && !shouldShowRegionLoading && (
              <div className="home-status-badge is-error">
                <span>Failed to load regions</span>
              </div>
            )}
            {shouldShowRegionReminder && (
              <div className="home-status-badge is-warning">
                <span>Select a region to continue</span>
              </div>
            )}
            {shouldShowConnectionBanner && (
              <div className="home-status-badge is-warning">
                <span className="home-status-dot" aria-hidden="true" />
                <span>Connecting to game server…</span>
              </div>
            )}
          </div>
        )}

        <main className="home-main">
          <div className="home-center-stack">
            <GameStartForm
              onStartGame={handleStartGame}
              currentUsername={user?.username}
              isLoading={isLoading}
              isAuthenticated={user !== null && !user.isGuest}
              isLobbyQueued={isLobbyQueued}
              lobbyPreferences={lobbyPreferences}
              onPreferencesChange={updateLobbyPreferences}
            />
            <SocialFooter />
          </div>
        </main>

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
