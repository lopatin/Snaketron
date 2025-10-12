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
  const { user, login, register, createGuest } = useAuth();
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
    gameMode: 'duel' | '2v2' | 'solo' | 'ffa',
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

      // Start game based on mode
      if (gameMode === 'solo') {
        createGame('solo');
      } else if (gameMode === 'duel') {
        navigate('/queue', {
          state: {
            gameType: { TeamMatch: { per_team: 1 } },
            autoQueue: true
          }
        });
      } else if (gameMode === 'ffa') {
        navigate('/queue', {
          state: {
            gameType: { FreeForAll: { max_players: 8 } },
            autoQueue: true
          }
        });
      } else if (gameMode === '2v2') {
        navigate('/queue', {
          state: {
            gameType: { TeamMatch: { per_team: 2 } },
            autoQueue: true
          }
        });
      }
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
              {user ? (
                <span className="text-sm text-black-70 font-bold uppercase tracking-1">
                  {user.username}
                </span>
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
            currentUsername={user?.username}
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
