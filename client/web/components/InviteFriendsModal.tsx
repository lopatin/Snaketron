import React, { useEffect, useMemo, useRef, useState } from 'react';
import { LobbyModal } from './LobbyModal';

interface InviteFriendsModalProps {
  isOpen: boolean;
  onClose: () => void;
  lobbyCode: string | null;
}

type CopyState = 'idle' | 'copied' | 'failed';
type CopyTarget = 'code' | 'link';

const copyFeedbackText = (
  target: CopyTarget,
  state: CopyState,
): string => {
  if (state === 'copied') {
    return target === 'code' ? 'Lobby code copied.' : 'Invite link copied.';
  }
  if (state === 'failed') {
    return target === 'code'
      ? 'Could not copy the lobby code. Try again.'
      : 'Could not copy the invite link. Try again.';
  }
  return '';
};

export const InviteFriendsModal: React.FC<InviteFriendsModalProps> = ({
  isOpen,
  onClose,
  lobbyCode,
}) => {
  const [codeCopyState, setCodeCopyState] = useState<CopyState>('idle');
  const [linkCopyState, setLinkCopyState] = useState<CopyState>('idle');
  const [latestCopyTarget, setLatestCopyTarget] = useState<CopyTarget | null>(null);
  const copyCodeButtonRef = useRef<HTMLButtonElement>(null);
  const resetTimersRef = useRef<Partial<Record<CopyTarget, ReturnType<typeof setTimeout>>>>({});
  const copyOperationRef = useRef<Record<CopyTarget, number>>({ code: 0, link: 0 });
  const isMountedRef = useRef(true);
  const isOpenRef = useRef(isOpen);
  isOpenRef.current = isOpen;

  const lobbyUrl = useMemo(() => {
    if (!lobbyCode || typeof window === 'undefined') {
      return '';
    }
    // The itch.io embed lives on itch's static host, so a link to its own
    // origin would strand invitees; send them to the canonical site instead.
    const origin = process.env.ITCH_BUILD === 'true'
      ? 'https://snaketron.io'
      : window.location.origin;
    return `${origin}/lobby/${encodeURIComponent(lobbyCode)}`;
  }, [lobbyCode]);

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
    };
  }, []);

  useEffect(() => {
    if (isOpen) {
      return;
    }
    clearResetTimer('code');
    clearResetTimer('link');
    copyOperationRef.current.code += 1;
    copyOperationRef.current.link += 1;
    setCodeCopyState('idle');
    setLinkCopyState('idle');
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

  const statusMessage = latestCopyTarget
    ? copyFeedbackText(
        latestCopyTarget,
        latestCopyTarget === 'code' ? codeCopyState : linkCopyState,
      )
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
