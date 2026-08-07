import React, { useEffect, useId, useRef } from 'react';
import type { TutorialContent } from '../utils/tutorial';
import TutorialSceneCanvas from './TutorialSceneCanvas';

const FOCUSABLE_SELECTOR = [
  'a[href]',
  'button:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(',');

export interface TutorialModalProps {
  open: boolean;
  content: TutorialContent;
  /**
   * Briefing mode gates the match start and shows the readiness roster.
   * Reference mode is the same screen re-opened from the help control during
   * play; it dismisses instead of confirming.
   */
  variant: 'briefing' | 'reference';
  /** Seconds until everyone is readied automatically. Briefing mode only. */
  autoReadySeconds: number | null;
  /** How many players still have to confirm, including this one. */
  pendingCount: number;
  /** True once this player has confirmed and is waiting on the others. */
  isReady: boolean;
  onReady: () => void;
  onClose: () => void;
}

const TutorialModal: React.FC<TutorialModalProps> = ({
  open,
  content,
  variant,
  autoReadySeconds,
  pendingCount,
  isReady,
  onReady,
  onClose,
}) => {
  const dialogRef = useRef<HTMLDivElement>(null);
  const primaryRef = useRef<HTMLButtonElement>(null);
  const onCloseRef = useRef(onClose);
  const titleId = useId();
  const listId = useId();

  onCloseRef.current = onClose;

  useEffect(() => {
    if (!open) {
      return undefined;
    }

    const previouslyFocused =
      document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    const focusFrame = window.requestAnimationFrame(() => {
      (primaryRef.current ?? dialogRef.current)?.focus();
    });

    const handleKeyDown = (event: KeyboardEvent) => {
      // A briefing is not dismissible: closing it would leave the player
      // staring at a match they cannot start. The reference variant is.
      if (event.key === 'Escape' && variant === 'reference') {
        event.preventDefault();
        onCloseRef.current();
        return;
      }
      if (event.key !== 'Tab' || !dialogRef.current) {
        return;
      }

      const controls = Array.from(
        dialogRef.current.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR),
      );
      if (controls.length === 0) {
        event.preventDefault();
        dialogRef.current.focus();
        return;
      }
      const first = controls[0];
      const last = controls[controls.length - 1];
      if (
        event.shiftKey &&
        (document.activeElement === first || document.activeElement === dialogRef.current)
      ) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };

    document.addEventListener('keydown', handleKeyDown, true);
    return () => {
      window.cancelAnimationFrame(focusFrame);
      document.removeEventListener('keydown', handleKeyDown, true);
      document.body.style.overflow = previousOverflow;
      if (previouslyFocused?.isConnected) {
        previouslyFocused.focus();
      }
    };
  }, [open, variant]);

  if (!open) {
    return null;
  }

  const isBriefing = variant === 'briefing';
  const waitingOnOthers = isBriefing && isReady;

  return (
    <div className="tutorial-backdrop" data-testid="tutorial-backdrop">
      <div
        ref={dialogRef}
        className="tutorial-card"
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-describedby={listId}
        tabIndex={-1}
        data-testid="tutorial-modal"
        data-tutorial-key={content.key}
        data-variant={variant}
      >
        <header className="tutorial-header">
          <p className="tutorial-kicker">{content.kicker}</p>
          <h2 className="tutorial-title" id={titleId}>
            {content.title}
          </h2>
        </header>

        <ol className="tutorial-bullets" id={listId}>
          {content.bullets.map((bullet, index) => (
            <li className="tutorial-bullet" key={bullet.scene}>
              <span className="tutorial-bullet-index" aria-hidden="true">
                {index + 1}
              </span>
              <span className="tutorial-bullet-art">
                <TutorialSceneCanvas scene={bullet.scene} />
              </span>
              <p className="tutorial-bullet-text">{bullet.text}</p>
            </li>
          ))}
        </ol>

        <footer className="tutorial-footer">
          {isBriefing ? (
            <>
              <button
                type="button"
                ref={primaryRef}
                className="tutorial-ready-button"
                onClick={onReady}
                disabled={isReady}
                data-testid="tutorial-ready"
              >
                Ready
                {isReady && (
                  <span className="tutorial-ready-check" aria-hidden="true">
                    ✓
                  </span>
                )}
              </button>
              <p className="tutorial-status" role="status" data-testid="tutorial-status">
                {waitingOnOthers
                  ? pendingCount > 0
                    ? `Waiting for ${pendingCount} more ${pendingCount === 1 ? 'player' : 'players'}…`
                    : 'Everyone is ready.'
                  : autoReadySeconds !== null
                    ? `Starting automatically in ${autoReadySeconds}s`
                    : 'Take your time.'}
              </p>
            </>
          ) : (
            <button
              type="button"
              ref={primaryRef}
              className="tutorial-ready-button"
              onClick={onClose}
              data-testid="tutorial-close"
            >
              Back to the game
            </button>
          )}
        </footer>
      </div>
    </div>
  );
};

export default TutorialModal;
