import React, { useEffect, useId, useRef } from 'react';

const FOCUSABLE_SELECTOR = [
  'a[href]',
  'button:not([disabled])',
  'input:not([disabled])',
  'select:not([disabled])',
  'textarea:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(',');

interface LobbyModalProps {
  isOpen: boolean;
  onClose: () => void;
  title: string;
  description: React.ReactNode;
  children: React.ReactNode;
  initialFocusRef?: React.RefObject<HTMLElement | null>;
  isDismissDisabled?: boolean;
}

/**
 * Shared lobby-dialog frame with focus containment and focus restoration.
 * Feature-specific forms remain in the caller so their async state stays local.
 */
export const LobbyModal: React.FC<LobbyModalProps> = ({
  isOpen,
  onClose,
  title,
  description,
  children,
  initialFocusRef,
  isDismissDisabled = false,
}) => {
  const dialogRef = useRef<HTMLDivElement>(null);
  const onCloseRef = useRef(onClose);
  const isDismissDisabledRef = useRef(isDismissDisabled);
  const titleId = useId();
  const descriptionId = useId();

  onCloseRef.current = onClose;
  isDismissDisabledRef.current = isDismissDisabled;

  useEffect(() => {
    if (!isOpen) {
      return;
    }

    const previouslyFocused = document.activeElement instanceof HTMLElement
      ? document.activeElement
      : null;
    const previousBodyOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';

    const focusInitialControl = window.requestAnimationFrame(() => {
      const requestedTarget = initialFocusRef?.current;
      const preferredTarget = requestedTarget?.matches(FOCUSABLE_SELECTOR)
        ? requestedTarget
        : null;
      const fallbackTarget = dialogRef.current?.querySelector<HTMLElement>(FOCUSABLE_SELECTOR);
      (preferredTarget ?? fallbackTarget ?? dialogRef.current)?.focus();
    });

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        if (!isDismissDisabledRef.current) {
          event.preventDefault();
          onCloseRef.current();
        }
        return;
      }

      if (event.key !== 'Tab' || !dialogRef.current) {
        return;
      }

      const focusableElements = Array.from(
        dialogRef.current.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR),
      ).filter((element) => element.getAttribute('aria-hidden') !== 'true');

      if (focusableElements.length === 0) {
        event.preventDefault();
        dialogRef.current.focus();
        return;
      }

      const firstElement = focusableElements[0];
      const lastElement = focusableElements[focusableElements.length - 1];
      const activeElement = document.activeElement;

      if (event.shiftKey && (activeElement === firstElement || activeElement === dialogRef.current)) {
        event.preventDefault();
        lastElement.focus();
      } else if (!event.shiftKey && activeElement === lastElement) {
        event.preventDefault();
        firstElement.focus();
      }
    };

    document.addEventListener('keydown', handleKeyDown);

    return () => {
      window.cancelAnimationFrame(focusInitialControl);
      document.removeEventListener('keydown', handleKeyDown);
      document.body.style.overflow = previousBodyOverflow;
      if (previouslyFocused?.isConnected) {
        previouslyFocused.focus();
      }
    };
  }, [initialFocusRef, isOpen]);

  if (!isOpen) {
    return null;
  }

  const handleBackdropMouseDown: React.MouseEventHandler<HTMLDivElement> = (event) => {
    if (event.target === event.currentTarget && !isDismissDisabled) {
      onClose();
    }
  };

  return (
    <div className="lobby-modal-backdrop" onMouseDown={handleBackdropMouseDown}>
      <div
        ref={dialogRef}
        className="lobby-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-describedby={descriptionId}
        tabIndex={-1}
      >
        <header className="lobby-modal-header">
          <h2 id={titleId} className="lobby-modal-title">{title}</h2>
          <p id={descriptionId} className="lobby-modal-description">{description}</p>
        </header>

        <div className="lobby-modal-body">{children}</div>
      </div>
    </div>
  );
};
