import React, { useEffect, useRef, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../contexts/AuthContext';
import { useCrazyGames } from '../contexts/CrazyGamesContext';
import { useWebSocket } from '../contexts/WebSocketContext';
import { crazyGames, crazyGamesGuestNickname } from '../services/crazyGames';
import {
  buildCrazyGamesRoomUpdate,
  enterCrazyGamesInviteTarget,
  resolveCrazyGamesInvite,
} from '../utils/crazyGamesMultiplayer';

/**
 * Translates Snaketron's real Redis lobby lifecycle into CrazyGames room
 * presence, and translates CrazyGames join/instant-multiplayer entry points
 * back into Snaketron's existing lobby flow.
 */
export const CrazyGamesBridge: React.FC = () => {
  const navigate = useNavigate();
  const {
    isCrazyGamesBuild,
    available,
    isInstantMultiplayer,
    inviteParams,
    inviteSequence,
    portalUser,
    settings: { muteAudio },
    adState,
  } = useCrazyGames();
  const { user, loading: authLoading, createGuest, updateGuestNickname } = useAuth();
  const {
    isConnected,
    isSessionAuthenticated,
    lobbyRestorationComplete,
    currentLobby,
    lobbyMembers,
    createLobby,
    leaveLobby,
    sendMessage,
    waitForSessionReady,
  } = useWebSocket();
  const reportedRoomRef = useRef<string | null>(null);
  const handledInviteSequenceRef = useRef(0);
  const instantMultiplayerCompleteRef = useRef(false);
  const instantMultiplayerInFlightRef = useRef(false);
  const [instantRetry, setInstantRetry] = useState(0);

  useEffect(() => {
    if (!isCrazyGamesBuild) {
      return;
    }

    const enforcedMute = muteAudio || adState === 'playing';
    const previousMute = new Map<HTMLMediaElement, boolean>();
    const applyMute = (root: ParentNode) => {
      for (const media of root.querySelectorAll<HTMLMediaElement>('audio, video')) {
        if (!previousMute.has(media)) {
          previousMute.set(media, media.muted);
        }
        media.muted = true;
      }
    };
    const restoreMute = () => {
      for (const [media, wasMuted] of previousMute) {
        if (media.isConnected) {
          media.muted = wasMuted;
        }
      }
      previousMute.clear();
    };

    document.documentElement.dataset.crazygamesMuteAudio = enforcedMute ? 'true' : 'false';
    if (!enforcedMute) {
      return;
    }

    applyMute(document);
    const observer = new MutationObserver(() => applyMute(document));
    observer.observe(document.body, { childList: true, subtree: true });
    return () => {
      observer.disconnect();
      restoreMute();
    };
  }, [adState, isCrazyGamesBuild, muteAudio]);

  useEffect(() => {
    if (
      !isCrazyGamesBuild ||
      !portalUser ||
      !user?.isGuest ||
      !isSessionAuthenticated
    ) {
      return;
    }

    const portalNickname = crazyGamesGuestNickname(portalUser.username);
    if (portalNickname === user.username) {
      return;
    }

    if (sendMessage({ UpdateNickname: { nickname: portalNickname } })) {
      // Keep every same-page roster/chat action aligned while the server
      // durably updates the guest and refreshes this socket's metadata.
      updateGuestNickname(portalNickname);
    }
  }, [
    isCrazyGamesBuild,
    isSessionAuthenticated,
    portalUser,
    sendMessage,
    updateGuestNickname,
    user,
  ]);

  useEffect(() => {
    if (!isCrazyGamesBuild || !available) {
      return;
    }

    if (!currentLobby) {
      if (reportedRoomRef.current) {
        crazyGames.leftRoom();
        reportedRoomRef.current = null;
      }
      return;
    }

    const roomUpdate = buildCrazyGamesRoomUpdate(currentLobby, lobbyMembers.length);
    if (!roomUpdate?.roomId) {
      console.warn('Cannot report an invalid Snaketron lobby to CrazyGames');
      return;
    }

    crazyGames.updateRoom(roomUpdate);
    reportedRoomRef.current = roomUpdate.roomId;
  }, [available, currentLobby, isCrazyGamesBuild, lobbyMembers.length]);

  useEffect(() => () => {
    if (reportedRoomRef.current) {
      crazyGames.leftRoom();
      reportedRoomRef.current = null;
    }
  }, []);

  useEffect(() => {
    if (
      !isCrazyGamesBuild ||
      !available ||
      !inviteParams ||
      inviteSequence <= handledInviteSequenceRef.current
    ) {
      return;
    }

    handledInviteSequenceRef.current = inviteSequence;
    const inviteTarget = resolveCrazyGamesInvite(
      inviteParams,
      currentLobby?.code ?? null,
    );
    if (!inviteTarget) {
      return;
    }

    const acceptedSequence = inviteSequence;
    void enterCrazyGamesInviteTarget(inviteTarget, {
      leaveLobby,
      navigate: (route) => navigate(route),
      isInviteCurrent: () => handledInviteSequenceRef.current === acceptedSequence,
      onLeaveError: (error) => {
        console.warn('Could not leave the current lobby before accepting an invitation', error);
      },
    });
  }, [
    available,
    currentLobby,
    inviteParams,
    inviteSequence,
    isCrazyGamesBuild,
    leaveLobby,
    navigate,
  ]);

  useEffect(() => {
    if (
      !isCrazyGamesBuild ||
      !available ||
      !isInstantMultiplayer ||
      inviteParams?.lobbyCode ||
      inviteParams?.roomCode ||
      authLoading ||
      !isConnected ||
      !lobbyRestorationComplete ||
      (Boolean(user) && !isSessionAuthenticated) ||
      instantMultiplayerCompleteRef.current ||
      instantMultiplayerInFlightRef.current
    ) {
      return;
    }

    if (currentLobby) {
      instantMultiplayerCompleteRef.current = true;
      return;
    }

    let retryTimer: ReturnType<typeof setTimeout> | null = null;
    instantMultiplayerInFlightRef.current = true;

    const createInstantRoom = async () => {
      try {
        if (!user) {
          await createGuest(crazyGamesGuestNickname(portalUser?.username));
        }
        await waitForSessionReady();
        await createLobby();
        instantMultiplayerCompleteRef.current = true;
      } catch (error) {
        console.warn('Could not create the CrazyGames instant multiplayer room', error);
        retryTimer = setTimeout(() => setInstantRetry((value) => value + 1), 1500);
      } finally {
        instantMultiplayerInFlightRef.current = false;
      }
    };
    void createInstantRoom();

    return () => {
      if (retryTimer) {
        clearTimeout(retryTimer);
      }
    };
  }, [
    authLoading,
    available,
    createGuest,
    createLobby,
    currentLobby,
    instantRetry,
    inviteParams,
    isConnected,
    isCrazyGamesBuild,
    isSessionAuthenticated,
    isInstantMultiplayer,
    lobbyRestorationComplete,
    portalUser?.username,
    user,
    waitForSessionReady,
  ]);

  return null;
};

export const CrazyGamesAdOverlay: React.FC = () => {
  const { adState } = useCrazyGames();
  if (adState === 'idle') {
    return null;
  }

  return (
    <div
      className="fixed inset-0 z-[9998] flex items-center justify-center bg-black/55"
      role="status"
      aria-live="assertive"
      aria-label={adState === 'requesting' ? 'Preparing advertisement' : 'Advertisement playing'}
      data-testid="crazygames-ad-overlay"
    >
      {adState === 'requesting' && (
        <div className="flex items-center gap-3 border-2 border-white bg-black px-5 py-4 text-sm font-black uppercase tracking-1 text-white">
          <span className="h-5 w-5 animate-spin rounded-full border-2 border-white/40 border-t-white" aria-hidden="true" />
          Preparing ad…
        </div>
      )}
    </div>
  );
};
