import type {
  CrazyGamesInviteParams,
  CrazyGamesRoomUpdate,
} from '../services/crazyGames';
import {
  getLobbyCodeValidationError,
  normalizeLobbyCodeInput,
} from './lobbyCode.ts';

export interface CrazyGamesInviteTarget {
  lobbyCode: string;
  route: string;
  leaveCurrentLobby: boolean;
}

export interface CrazyGamesInviteActions {
  leaveLobby: () => Promise<void>;
  navigate: (route: string) => void;
  isInviteCurrent: () => boolean;
  onLeaveError?: (error: unknown) => void;
}

/**
 * Report one stable portal room for the lifetime of a Snaketron lobby. Lobby
 * codes already carry their region prefix and are globally unique, so using
 * the code directly avoids unstable or empty client-side region metadata.
 */
export function buildCrazyGamesRoomUpdate(
  lobby: { code: string; state: string },
  memberCount: number,
  maximumMembers = 4,
): CrazyGamesRoomUpdate | null {
  const lobbyCode = normalizeLobbyCodeInput(lobby.code);
  if (getLobbyCodeValidationError(lobbyCode)) {
    return null;
  }

  return {
    roomId: `lobby:${lobbyCode}`,
    isJoinable: lobby.state === 'waiting' && memberCount < maximumMembers,
    inviteParams: { lobbyCode },
  };
}

/**
 * Resolve both current (`lobbyCode`) and legacy (`roomCode`) CrazyGames
 * invitations into the same in-game join route. The route works for cold
 * launches and warm join-listener events; the latter first leaves a different
 * existing lobby so the server can accept the invited lobby.
 */
export function resolveCrazyGamesInvite(
  inviteParams: CrazyGamesInviteParams,
  currentLobbyCode: string | null,
): CrazyGamesInviteTarget | null {
  const rawCode = inviteParams.lobbyCode ?? inviteParams.roomCode ?? '';
  const lobbyCode = normalizeLobbyCodeInput(rawCode);
  if (getLobbyCodeValidationError(lobbyCode)) {
    return null;
  }

  const normalizedCurrentCode = currentLobbyCode
    ? normalizeLobbyCodeInput(currentLobbyCode)
    : null;
  if (normalizedCurrentCode === lobbyCode) {
    return null;
  }

  return {
    lobbyCode,
    route: `/lobby/${encodeURIComponent(lobbyCode)}`,
    leaveCurrentLobby: Boolean(normalizedCurrentCode),
  };
}

/** Execute the complete cold/warm invitation transition in a testable form. */
export async function enterCrazyGamesInviteTarget(
  target: CrazyGamesInviteTarget,
  actions: CrazyGamesInviteActions,
): Promise<boolean> {
  if (target.leaveCurrentLobby) {
    try {
      await actions.leaveLobby();
    } catch (error) {
      actions.onLeaveError?.(error);
    }
  }

  // A newer warm invitation can arrive while leaving the previous lobby.
  // Only the latest accepted sequence is allowed to choose the destination.
  if (!actions.isInviteCurrent()) {
    return false;
  }

  actions.navigate(target.route);
  return true;
}
