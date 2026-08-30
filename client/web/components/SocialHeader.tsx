import React, { useCallback, useState } from 'react';
import type { AccountModalView } from './AccountModal';
import { HomeHeader } from './HomeHeader';
import { InviteFriendsModal } from './InviteFriendsModal';
import JoinGameModal from './JoinGameModal';
import { useAuth } from '../contexts/AuthContext';
import { useWebSocket } from '../contexts/WebSocketContext';

const generateGuestNickname = () => `Guest${Math.floor(1000 + Math.random() * 9000)}`;

interface SocialHeaderProps {
  activePage: 'play' | 'leaderboards' | 'skins';
  onOpenAuth: () => void;
  onOpenAccount: (view: AccountModalView) => void;
}

/**
 * The site header together with the lobby state its social menu acts on.
 *
 * The menu is only as good as the handlers behind it: Invite has to be able to
 * mint a player session and a lobby before it has a code to show. Owning that
 * here rather than in each page is what stopped /skins and the skin builder
 * from shipping the menu with no-ops wired to it, and what keeps the home and
 * leaderboard copies from drifting apart again — they already had.
 *
 * A component rather than a hook on purpose: the WebSocket context value is a
 * fresh object on every ping, so every consumer re-renders that often.
 * Confining the subscription to this subtree keeps the skins grid out of it.
 */
export const SocialHeader: React.FC<SocialHeaderProps> = ({
  activePage,
  onOpenAuth,
  onOpenAccount,
}) => {
  const { user, ensurePlayableSession, logout } = useAuth();
  const {
    currentLobby,
    lobbyMembers,
    createLobby,
    leaveLobby,
    waitForSessionReady,
  } = useWebSocket();
  const [showInviteModal, setShowInviteModal] = useState(false);
  const [showJoinModal, setShowJoinModal] = useState(false);
  const [isCreatingInvite, setIsCreatingInvite] = useState(false);

  const handleInvite = useCallback(async () => {
    if (isCreatingInvite) {
      return;
    }

    setIsCreatingInvite(true);
    try {
      // Someone who has never pressed Play has no session, and the socket will
      // not accept CreateLobby without one. Minting a guest here is what lets
      // "Invite friends" be the first thing a visitor ever clicks.
      try {
        await ensurePlayableSession(generateGuestNickname());
      } catch (error) {
        console.error('Player session creation failed for lobby invite:', error);
        return;
      }

      await waitForSessionReady();

      if (!currentLobby) {
        await createLobby();
      }

      setShowInviteModal(true);
    } catch (error) {
      console.error('Failed to create lobby:', error);
    } finally {
      setIsCreatingInvite(false);
    }
  }, [createLobby, currentLobby, ensurePlayableSession, isCreatingInvite, waitForSessionReady]);

  const handleLeaveLobby = useCallback(async () => {
    try {
      await leaveLobby();
    } catch (error) {
      console.error('Failed to leave lobby:', error);
    }
  }, [leaveLobby]);

  return (
    <>
      <HomeHeader
        activePage={activePage}
        currentUser={user}
        lobbyMembers={lobbyMembers}
        hasLobby={Boolean(currentLobby)}
        isInviteDisabled={isCreatingInvite}
        onInvite={() => { void handleInvite(); }}
        onJoinGame={() => setShowJoinModal(true)}
        onLeaveLobby={() => { void handleLeaveLobby(); }}
        onAuthClick={onOpenAuth}
        onOpenAccount={onOpenAccount}
        onLogout={logout}
      />

      <InviteFriendsModal
        isOpen={showInviteModal}
        onClose={() => setShowInviteModal(false)}
        lobbyCode={currentLobby?.code || null}
        region={currentLobby?.region || null}
      />

      <JoinGameModal
        isOpen={showJoinModal}
        onClose={() => setShowJoinModal(false)}
      />
    </>
  );
};

export default SocialHeader;
