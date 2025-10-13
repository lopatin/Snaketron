import React, { useState, useEffect } from 'react';
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

// Dummy chat messages for UI design
const DUMMY_CHAT_MESSAGES = [
  {
    id: '1',
    type: 'system' as const,
    message: 'User3 joined the lobby',
    timestamp: new Date(Date.now() - 120000)
  },
  {
    id: '2',
    type: 'user' as const,
    username: 'Player1',
    message: 'Hey everyone! Ready to play?',
    timestamp: new Date(Date.now() - 60000)
  },
  {
    id: '3',
    type: 'user' as const,
    username: 'SnakeKing',
    message: 'Let\'s do this!',
    timestamp: new Date(Date.now() - 30000)
  }
];

export const NewHome: React.FC = () => {
  const navigate = useNavigate();
  const { user, login, register, createGuest, logout } = useAuth();
  const {
    connectToRegion,
    isConnected,
    onMessage,
    currentLobby,
    lobbyMembers,
    createLobby,
    leaveLobby,
  } = useWebSocket();
  const { createGame, currentGameId } = useGameWebSocket();
  const [isMobile, setIsMobile] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [chatMessages, setChatMessages] = useState(DUMMY_CHAT_MESSAGES);
  const [showInviteModal, setShowInviteModal] = useState(false);
  const [showJoinModal, setShowJoinModal] = useState(false);

  // Use regions hook for live data
  const { regions, selectedRegion, selectRegion, isLoading: regionsLoading } = useRegions({
    isWebSocketConnected: isConnected,
    onMessage,
  });

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
    if (selectedRegion) {
      console.log('Connecting to region:', selectedRegion.name, selectedRegion.wsUrl);
      connectToRegion(selectedRegion.wsUrl);
    }
  }, [selectedRegion?.id, connectToRegion]);

  // Navigate to game when created
  useEffect(() => {
    if (currentGameId) {
      navigate(`/play/${currentGameId}`);
    }
  }, [currentGameId, navigate]);

  const handleRegionChange = (regionId: string) => {
    selectRegion(regionId);
  };

  const handleStartGame = async (
    gameModes: Array<'duel' | '2v2' | 'solo' | 'ffa'>,
    nickname: string,
    isCompetitive: boolean
  ) => {
    setIsLoading(true);
    try {
      // If not logged in, create guest user
      if (!user) {
        try {
          await createGuest(nickname);
        } catch (error) {
          console.error('Guest creation failed:', error);
          setIsLoading(false);
          return;
        }
      }

      // Wait for auth to propagate
      await new Promise(resolve => setTimeout(resolve, 500));

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

      // Navigate to queue with all selected game types
      navigate('/queue', {
        state: {
          gameTypes: gameTypes,
          autoQueue: true
        }
      });
    } catch (error) {
      console.error('Failed to start game:', error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleSendMessage = (message: string) => {
    const newMessage = {
      id: Date.now().toString(),
      type: 'user' as const,
      username: user?.username || 'Guest',
      message,
      timestamp: new Date()
    };
    setChatMessages([...chatMessages, newMessage]);
  };

  const handleInvite = async () => {
    try {
      // If not logged in, create guest user first
      if (!user) {
        console.log('Creating guest user for lobby...');
        // For now, require nickname input - this will be handled by modal later
        return;
      }

      // If lobby doesn't exist yet, create it
      if (!currentLobby) {
        await createLobby();
        console.log('Lobby created successfully');
      }

      // Show the invite modal
      setShowInviteModal(true);
    } catch (error) {
      console.error('Failed to create lobby:', error);
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

  return (
    <>
      <div className="min-h-screen flex home-page">
        {!isMobile && !regionsLoading && selectedRegion ? (
        /* Desktop/Tablet: Sidebar Layout */
        <>
          <div className="w-80 flex-shrink-0">
            <Sidebar
              regions={regions}
              currentRegionId={selectedRegion.id}
              onRegionChange={handleRegionChange}
              lobbyMembers={lobbyMembers}
              lobbyCode={currentLobby?.code || null}
              currentUserId={user?.id}
              isHost={currentLobby ? currentLobby.hostUserId === user?.id : false}
              onInvite={handleInvite}
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
              />
            </div>

            {/* Bottom Right: Lobby Chat */}
            <LobbyChat
              messages={chatMessages}
              onSendMessage={handleSendMessage}
              currentUsername={user?.username}
              initialExpanded={true}
            />
          </div>
        </>
      ) : !regionsLoading && selectedRegion ? (
        /* Mobile: Header Layout */
        <div className="flex-1 flex flex-col">
          <MobileHeader
            regions={regions}
            currentRegionId={selectedRegion.id}
            onRegionChange={handleRegionChange}
            currentUser={user}
            onLoginClick={handleLoginClick}
            lobbyUsers={lobbyMembers.map(m => m.username)}
            onInvite={handleInvite}
          />

          {/* Center: Game Start Form */}
          <div className="flex-1 flex items-center justify-center px-4 py-8">
            <GameStartForm
              onStartGame={handleStartGame}
              currentUsername={user?.username}
              isLoading={isLoading}
              isAuthenticated={user !== null && !user.isGuest}
            />
          </div>

          {/* Bottom Right: Lobby Chat - Hidden in mobile */}
          <LobbyChat
            messages={chatMessages}
            onSendMessage={handleSendMessage}
            currentUsername={user?.username}
            initialExpanded={false}
          />
        </div>
      ) : (
        /* Loading state */
        <div className="flex-1 flex items-center justify-center">
          <div className="text-center">
            <div className="animate-spin w-12 h-12 border-4 border-black-70 border-t-transparent rounded-full mx-auto mb-4" />
            <p className="text-black-70 font-bold uppercase tracking-1">Loading...</p>
          </div>
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
