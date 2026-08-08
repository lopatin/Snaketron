import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import type { AccountModalView } from './AccountModal';
import { HomeHeader } from './HomeHeader';
import { GameStartForm } from './GameStartForm';
import { SocialFooter } from './SocialFooter';
import { LobbyChat } from './LobbyChat';
import { RegionSelector } from './RegionSelector';
import { InviteFriendsModal } from './InviteFriendsModal';
import JoinGameModal from './JoinGameModal';
import { ConnectionStatusRack } from './ConnectionStatusRack';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import { useRegions } from '../hooks/useRegions';
import { useGameWebSocket } from '../hooks/useGameWebSocket';
import { isConnectionReady } from '../utils/connectionBanner';
import { LobbyGameMode } from '../types';

const generateGuestNickname = () => `Guest${Math.floor(1000 + Math.random() * 9000)}`;

interface NewHomeProps {
  onOpenAuth: () => void;
  onOpenAccount: (view: AccountModalView) => void;
}

export const NewHome: React.FC<NewHomeProps> = ({ onOpenAuth, onOpenAccount }) => {
  const navigate = useNavigate();
  const { user, createGuest, logout } = useAuth();
  const {
    connectToRegion,
    isConnected,
    isSessionAuthenticated,
    waitForSessionReady,
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
  const { currentGameId, isQueued, queueForMatch, queueForMatchMulti } = useGameWebSocket();
  const [isLoading, setIsLoading] = useState(false);
  const [showInviteModal, setShowInviteModal] = useState(false);
  const [showJoinModal, setShowJoinModal] = useState(false);
  const [isCreatingInvite, setIsCreatingInvite] = useState(false);
  const [startError, setStartError] = useState<string | null>(null);

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
  // Global population is the sum of the per-region counts the regions hook
  // already tracks (seeded over HTTP, then kept live by `UserCountUpdate`).
  // `null` until the first counts land so the indicator can stay quiet.
  const playersOnline = regions.length > 0
    ? regions.reduce((total, region) => total + region.userCount, 0)
    : null;
  const isLobbyQueued = isQueued || currentLobby?.state === 'queued';

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

  // NOTE: the server has no `NicknameUpdated` message (it is commented out in
  // server/src/ws_server.rs), so the former handler here was dead code — guest
  // nickname changes are never confirmed over the socket. Removed rather than
  // left as an unreachable listener registered for a nonexistent wire tag.

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
    setStartError(null);
    try {
      // If not logged in, create guest user
      if (!user) {
        await createGuest(nickname);
      }

      // Wait for the active regional socket to acknowledge this exact session
      // before issuing lobby or matchmaking commands.
      await waitForSessionReady();

      if (!currentLobby) {
        await createLobby();
      }

      updateLobbyPreferences({
        selectedModes: gameModes,
        competitive: isCompetitive,
      });

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
      const message =
        error instanceof Error
          ? error.message
          : typeof (error as any)?.message === 'string'
            ? (error as any).message
            : 'Failed to start matchmaking. Please try again.';
      setStartError(message);
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
      }

      await waitForSessionReady();

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
      <div className="home-page">
        <HomeHeader
          activePage="play"
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
              onSignInClick={onOpenAuth}
              errorMessage={startError}
              playersOnline={playersOnline}
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
