import React, { useEffect, useMemo, useRef, useState } from 'react';
import { LobbyModal } from './LobbyModal';
import { useCrazyGames } from '../contexts/CrazyGamesContext';
import { crazyGames } from '../services/crazyGames';
import { useAuth } from '../contexts/AuthContext';

interface InviteFriendsModalProps {
  isOpen: boolean;
  onClose: () => void;
  lobbyCode: string | null;
  region?: string | null;
}

type CopyState = 'idle' | 'copied' | 'failed';
type CopyTarget = 'code' | 'link' | 'player';

const COPY_SUBJECT: Record<CopyTarget, string> = {
  code: 'Lobby code',
  link: 'Invite link',
  player: 'Personal link',
};

const copyFeedbackText = (
  target: CopyTarget,
  state: CopyState,
): string => {
  if (state === 'copied') {
    return `${COPY_SUBJECT[target]} copied.`;
  }
  if (state === 'failed') {
    return `Could not copy the ${COPY_SUBJECT[target].toLowerCase()}. Try again.`;
  }
  return '';
};

export const InviteFriendsModal: React.FC<InviteFriendsModalProps> = ({
  isOpen,
  onClose,
  lobbyCode,
  region,
}) => {
  const { isCrazyGamesBuild, available } = useCrazyGames();
  const { user } = useAuth();
  const [codeCopyState, setCodeCopyState] = useState<CopyState>('idle');
  const [linkCopyState, setLinkCopyState] = useState<CopyState>('idle');
  const [playerCopyState, setPlayerCopyState] = useState<CopyState>('idle');
  const [latestCopyTarget, setLatestCopyTarget] = useState<CopyTarget | null>(null);
  const copyCodeButtonRef = useRef<HTMLButtonElement>(null);
  const resetTimersRef = useRef<Partial<Record<CopyTarget, ReturnType<typeof setTimeout>>>>({});
  const copyOperationRef = useRef<Record<CopyTarget, number>>({ code: 0, link: 0, player: 0 });
  const isMountedRef = useRef(true);
  const isOpenRef = useRef(isOpen);
  isOpenRef.current = isOpen;

  const lobbyUrl = useMemo(() => {
    if (!lobbyCode || typeof window === 'undefined') {
      return '';
    }
    if (isCrazyGamesBuild && available) {
      const inviteParams: Record<string, string> = { lobbyCode };
      if (region) {
        inviteParams.region = region;
      }
      const portalLink = crazyGames.inviteLink(inviteParams);
      if (portalLink) {
        return portalLink;
      }
    }

    // Embedded builds live on a portal static host, so a link to their own
    // origin would strand invitees; send them to the canonical site instead.
    const origin = process.env.ITCH_BUILD === 'true' || isCrazyGamesBuild
      ? 'https://snaketron.io'
      : window.location.origin;
    return `${origin}/lobby/${encodeURIComponent(lobbyCode)}`;
  }, [available, isCrazyGamesBuild, lobbyCode, region]);

  /**
   * A stable link to this player rather than to one lobby: it resolves to
   * whatever lobby they are in when it is followed, so it stays good after
   * this lobby is gone.
   *
   * Only registered accounts get one. Guest names are neither unique nor
   * reserved, so a guest's link could not be resolved back to them — and the
   * server refuses to try.
   */
  const playerUrl = useMemo(() => {
    if (!user || user.isGuest || !user.username || typeof window === 'undefined') {
      return '';
    }
    const origin = process.env.ITCH_BUILD === 'true' || isCrazyGamesBuild
      ? 'https://snaketron.io'
      : window.location.origin;
    return `${origin}/play/${encodeURIComponent(user.username)}`;
  }, [isCrazyGamesBuild, user]);

  const clearResetTimer = (target: CopyTarget) => {
    const timer = resetTimersRef.current[target];
    if (timer) {
      clearTimeout(timer);
      delete resetTimersRef.current[target];
    }
  };

  useEffect(() => {
    isMountedRef.current = true;
    return () => {
      isMountedRef.current = false;
      clearResetTimer('code');
      clearResetTimer('link');
      clearResetTimer('player');
    };
  }, []);

  useEffect(() => {
    if (isOpen) {
      return;
    }
    clearResetTimer('code');
    clearResetTimer('link');
    clearResetTimer('player');
    copyOperationRef.current.code += 1;
    copyOperationRef.current.link += 1;
    copyOperationRef.current.player += 1;
    setCodeCopyState('idle');
    setLinkCopyState('idle');
    setPlayerCopyState('idle');
    setLatestCopyTarget(null);
  }, [isOpen]);

  const copyToClipboard = async (
    target: CopyTarget,
    value: string,
    setCopyState: React.Dispatch<React.SetStateAction<CopyState>>,
  ) => {
    if (!value) {
      return;
    }

    clearResetTimer(target);
    const operation = copyOperationRef.current[target] + 1;
    copyOperationRef.current[target] = operation;
    setLatestCopyTarget(target);
    setCopyState('idle');

    try {
      if (!navigator.clipboard?.writeText) {
        throw new Error('Clipboard API unavailable');
      }
      await navigator.clipboard.writeText(value);
      if (
        !isMountedRef.current ||
        !isOpenRef.current ||
        copyOperationRef.current[target] !== operation
      ) {
        return;
      }
      setCopyState('copied');
    } catch (error: unknown) {
      console.error(`Failed to copy lobby ${target}:`, error);
      if (
        !isMountedRef.current ||
        !isOpenRef.current ||
        copyOperationRef.current[target] !== operation
      ) {
        return;
      }
      setCopyState('failed');
    }

    if (isMountedRef.current && isOpenRef.current) {
      resetTimersRef.current[target] = setTimeout(() => {
        if (
          isMountedRef.current &&
          isOpenRef.current &&
          copyOperationRef.current[target] === operation
        ) {
          setCopyState('idle');
        }
        delete resetTimersRef.current[target];
      }, 2000);
    }
  };

  const copyStateFor: Record<CopyTarget, CopyState> = {
    code: codeCopyState,
    link: linkCopyState,
    player: playerCopyState,
  };
  const statusMessage = latestCopyTarget
    ? copyFeedbackText(latestCopyTarget, copyStateFor[latestCopyTarget])
    : '';

  return (
    <LobbyModal
      isOpen={isOpen}
      onClose={onClose}
      title="Invite friends"
      description="Share the lobby code or send a direct invite link."
      initialFocusRef={copyCodeButtonRef}
    >
      <div className="lobby-modal-share-list">
        <div className="lobby-modal-field">
          <span className="lobby-modal-label">Lobby code</span>
          <div className="lobby-modal-share-row">
            <div className="lobby-modal-value is-code">{lobbyCode || 'Preparing…'}</div>
            <button
              ref={copyCodeButtonRef}
              type="button"
              className="lobby-modal-button is-copy"
              onClick={() => copyToClipboard('code', lobbyCode ?? '', setCodeCopyState)}
              disabled={!lobbyCode}
            >
              {codeCopyState === 'copied' ? 'Copied' : codeCopyState === 'failed' ? 'Try again' : 'Copy'}
            </button>
          </div>
        </div>

        <div className="lobby-modal-field">
          <span className="lobby-modal-label">Invite link</span>
          <div className="lobby-modal-share-row">
            <div className="lobby-modal-value is-link" title={lobbyUrl}>{lobbyUrl || 'Preparing…'}</div>
            <button
              type="button"
              className="lobby-modal-button is-copy"
              onClick={() => copyToClipboard('link', lobbyUrl, setLinkCopyState)}
              disabled={!lobbyUrl}
            >
              {linkCopyState === 'copied' ? 'Copied' : linkCopyState === 'failed' ? 'Try again' : 'Copy'}
            </button>
          </div>
        </div>

        {playerUrl && (
          <div className="lobby-modal-field">
            <span className="lobby-modal-label">Your personal link</span>
            <div className="lobby-modal-share-row">
              <div className="lobby-modal-value is-link" title={playerUrl}>{playerUrl}</div>
              <button
                type="button"
                className="lobby-modal-button is-copy"
                onClick={() => copyToClipboard('player', playerUrl, setPlayerCopyState)}
              >
                {playerCopyState === 'copied' ? 'Copied' : playerCopyState === 'failed' ? 'Try again' : 'Copy'}
              </button>
            </div>
            <p className="lobby-modal-hint">
              Always joins whichever lobby you are in — reuse it any time.
            </p>
          </div>
        )}
      </div>

      <p className="sr-only" aria-live="polite">
        {statusMessage}
      </p>

      <div className="lobby-modal-actions lobby-modal-invite-actions">
        <button type="button" className="lobby-modal-button is-secondary" onClick={onClose}>
          Done
        </button>
      </div>
    </LobbyModal>
  );
};
