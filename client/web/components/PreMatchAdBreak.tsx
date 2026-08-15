import React, { useEffect, useId, useRef } from 'react';
import { useAds } from '../contexts/AdsContext';
import { lockBodyScroll } from '../utils/bodyScrollLock';

export const PreMatchAdBreak: React.FC = () => {
  const { activeBreak, isLobbyInAdBreak, phase } = useAds();
  const dialogRef = useRef<HTMLDivElement>(null);
  const titleId = useId();
  const descriptionId = useId();

  useEffect(() => {
    if (!isLobbyInAdBreak) {
      return;
    }
    return lockBodyScroll();
  }, [isLobbyInAdBreak]);

  useEffect(() => {
    if (!isLobbyInAdBreak || phase === 'playing') {
      return;
    }
    const previouslyFocused = document.activeElement instanceof HTMLElement
      ? document.activeElement
      : null;
    const focusFrame = window.requestAnimationFrame(() => dialogRef.current?.focus());
    const containFocus = (event: KeyboardEvent) => {
      if (event.key === 'Escape' || event.key === 'Tab') {
        event.preventDefault();
        dialogRef.current?.focus();
      }
    };
    document.addEventListener('keydown', containFocus);
    return () => {
      window.cancelAnimationFrame(focusFrame);
      document.removeEventListener('keydown', containFocus);
      if (previouslyFocused?.isConnected) previouslyFocused.focus();
    };
  }, [isLobbyInAdBreak, phase]);

  if (!isLobbyInAdBreak) {
    return null;
  }

  const participantCount = Math.max(1, activeBreak?.participant_count ?? 1);
  const resolvedCount = Math.min(participantCount, activeBreak?.resolved_count ?? 0);
  const waitingCount = Math.max(0, participantCount - resolvedCount);
  const isPlaying = phase === 'playing';
  const title = phase === 'waiting' ? 'You’re ready' : 'Preparing the next round';
  const description = phase === 'waiting'
    ? waitingCount > 0
      ? `Waiting for ${waitingCount} ${waitingCount === 1 ? 'player' : 'players'} to finish.`
      : 'The lobby is moving to matchmaking.'
    : 'Matchmaking will begin when everyone is ready.';

  return (
    <div
      className={`ad-break-backdrop${isPlaying ? ' is-playing' : ''}`}
      data-testid="pre-match-ad-break"
    >
      <div
        ref={dialogRef}
        className="ad-break-dialog"
        role={isPlaying ? 'status' : 'dialog'}
        aria-modal={isPlaying ? undefined : true}
        aria-labelledby={titleId}
        aria-describedby={descriptionId}
        aria-busy={phase === 'requesting' || phase === 'playing'}
        tabIndex={isPlaying ? undefined : -1}
      >
        {!isPlaying && (
          <>
            <div className="ad-break-rule" aria-hidden="true">
              {Array.from({ length: Math.min(participantCount, 8) }, (_, index) => (
                <span key={index} className={index < resolvedCount ? 'is-resolved' : ''} />
              ))}
            </div>
            <span className="ad-break-kicker">Match break</span>
            <h2 id={titleId}>{title}</h2>
            <p id={descriptionId}>{description}</p>
            <div className="ad-break-status" role="status" aria-live="polite">
              {phase === 'requesting' ? (
                <><span className="ad-break-spinner" aria-hidden="true" />Checking the break</>
              ) : (
                <><span className="ad-break-ready-mark" aria-hidden="true">✓</span>Ready for queue</>
              )}
            </div>
          </>
        )}
        {isPlaying && (
          <>
            <span className="sr-only" id={titleId}>Advertisement playing</span>
            <span className="sr-only" id={descriptionId}>
              Matchmaking will continue when the lobby break finishes.
            </span>
          </>
        )}
      </div>
    </div>
  );
};
