import React, { useEffect, useId, useRef } from 'react';

export interface IdleKickDialogProps {
  open: boolean;
  onMenu: () => void;
}

const IdleKickDialog: React.FC<IdleKickDialogProps> = ({ open, onMenu }) => {
  const dialogRef = useRef<HTMLDivElement>(null);
  const menuButtonRef = useRef<HTMLButtonElement>(null);
  const titleId = useId();
  const descriptionId = useId();

  useEffect(() => {
    if (!open) {
      return;
    }

    const previouslyFocused = document.activeElement instanceof HTMLElement
      ? document.activeElement
      : null;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    const focusFrame = window.requestAnimationFrame(() => {
      (menuButtonRef.current ?? dialogRef.current)?.focus();
    });

    const keepFocusInDialog = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        // Removal is terminal for this player's controls. Do not dismiss the
        // explanation into an arena they can no longer interact with.
        event.preventDefault();
        return;
      }
      if (event.key === 'Tab') {
        event.preventDefault();
        (menuButtonRef.current ?? dialogRef.current)?.focus();
      }
    };

    document.addEventListener('keydown', keepFocusInDialog);
    return () => {
      window.cancelAnimationFrame(focusFrame);
      document.removeEventListener('keydown', keepFocusInDialog);
      document.body.style.overflow = previousOverflow;
      if (previouslyFocused?.isConnected) {
        previouslyFocused.focus();
      }
    };
  }, [open]);

  if (!open) {
    return null;
  }

  return (
    <div className="game-idle-kick-backdrop" data-testid="idle-kick-backdrop">
      <div
        ref={dialogRef}
        className="game-idle-kick-dialog"
        role="alertdialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-describedby={descriptionId}
        tabIndex={-1}
        data-testid="idle-kick-dialog"
      >
        <header className="game-idle-kick-dialog__header">
          <span className="game-idle-kick-dialog__kicker">Match status</span>
          <h2 id={titleId}>Removed for inactivity</h2>
        </header>
        <div className="game-idle-kick-dialog__body">
          <p id={descriptionId}>
            You did not respond before the inactivity timer expired. This match may
            continue without you.
          </p>
        </div>
        <footer className="game-idle-kick-dialog__actions">
          <button
            ref={menuButtonRef}
            type="button"
            onClick={onMenu}
            className="game-shell-button is-primary"
          >
            Main menu
          </button>
        </footer>
      </div>
    </div>
  );
};

export default IdleKickDialog;
