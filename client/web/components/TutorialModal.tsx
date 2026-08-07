import React, { useCallback, useEffect, useId, useRef, useState } from 'react';
import type { TutorialContent } from '../utils/tutorial';
import TutorialSceneCanvas from './TutorialSceneCanvas';

const FOCUSABLE_SELECTOR = [
  'a[href]',
  'button:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(',');

const isRenderedFocusTarget = (control: HTMLElement): boolean => {
  if (control.closest('[hidden], [inert]')) {
    return false;
  }
  const style = window.getComputedStyle(control);
  return (
    style.display !== 'none' &&
    style.visibility !== 'hidden' &&
    control.getClientRects().length > 0
  );
};

export interface TutorialModalProps {
  open: boolean;
  content: TutorialContent;
  /**
   * Briefing mode gates the match start and shows the readiness roster.
   * Reference mode reopens the same guide during play.
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
  const onCloseRef = useRef(onClose);
  const activeStepRef = useRef(0);
  const variantRef = useRef(variant);
  const pendingStepFocusRef = useRef(false);
  const showStepRef = useRef<(nextStep: number) => void>(() => {});
  const titleId = useId();
  const stepBodyId = useId();
  const [activeStep, setActiveStep] = useState(0);
  const [replayToken, setReplayToken] = useState(0);
  const [visitedSteps, setVisitedSteps] = useState<Set<number>>(() => new Set([0]));

  const lastStepIndex = content.steps.length - 1;
  const step = content.steps[activeStep] ?? content.steps[0];
  const activeStepBodyId = `${stepBodyId}-${activeStep}`;
  const isBriefing = variant === 'briefing';
  const waitingOnOthers = isBriefing && isReady;

  onCloseRef.current = onClose;
  activeStepRef.current = activeStep;
  variantRef.current = variant;

  const showStep = useCallback((nextStep: number) => {
    const boundedStep = Math.max(0, Math.min(lastStepIndex, nextStep));
    if (boundedStep !== activeStep) {
      pendingStepFocusRef.current = true;
    }
    setActiveStep(boundedStep);
    setVisitedSteps((current) => {
      if (current.has(boundedStep)) {
        return current;
      }
      const next = new Set(current);
      next.add(boundedStep);
      return next;
    });
    setReplayToken(0);
  }, [activeStep, lastStepIndex]);
  showStepRef.current = showStep;

  useEffect(() => {
    if (open) {
      setActiveStep(0);
      setReplayToken(0);
      setVisitedSteps(new Set([0]));
    }
  }, [content.key, open]);

  useEffect(() => {
    if (!open || !pendingStepFocusRef.current) {
      return undefined;
    }
    pendingStepFocusRef.current = false;
    const frame = window.requestAnimationFrame(() => {
      const target = dialogRef.current?.querySelector<HTMLElement>(
        `[data-tutorial-step-control="${activeStep}"]`,
      );
      target?.focus({ preventScroll: true });
      target?.scrollIntoView({ block: 'nearest', behavior: 'auto' });
    });
    return () => window.cancelAnimationFrame(frame);
  }, [activeStep, open]);

  useEffect(() => {
    if (!open) {
      return undefined;
    }

    const previouslyFocused =
      document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    const focusFrame = window.requestAnimationFrame(() => dialogRef.current?.focus());

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && variantRef.current === 'reference') {
        event.preventDefault();
        onCloseRef.current();
        return;
      }

      if (event.key === 'ArrowLeft') {
        event.preventDefault();
        showStepRef.current(activeStepRef.current - 1);
        return;
      }
      if (event.key === 'ArrowRight') {
        event.preventDefault();
        showStepRef.current(activeStepRef.current + 1);
        return;
      }

      if (event.key !== 'Tab' || !dialogRef.current) {
        return;
      }

      const controls = Array.from(
        dialogRef.current.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR),
      ).filter(isRenderedFocusTarget);
      if (controls.length === 0) {
        event.preventDefault();
        dialogRef.current.focus();
        return;
      }
      const first = controls[0];
      const last = controls[controls.length - 1];
      const activeControl =
        document.activeElement instanceof HTMLElement ? document.activeElement : null;
      if (
        activeControl !== dialogRef.current &&
        (
          !activeControl ||
          !dialogRef.current.contains(activeControl) ||
          !controls.includes(activeControl)
        )
      ) {
        event.preventDefault();
        (event.shiftKey ? last : first).focus();
        return;
      }
      if (
        event.shiftKey &&
        (activeControl === first || activeControl === dialogRef.current)
      ) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && activeControl === last) {
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
  }, [open]);

  if (!open || !step) {
    return null;
  }

  return (
    <div className="tutorial-backdrop" data-testid="tutorial-backdrop">
      <div
        ref={dialogRef}
        className="tutorial-card"
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-describedby={activeStepBodyId}
        tabIndex={-1}
        data-testid="tutorial-modal"
        data-tutorial-key={content.key}
        data-variant={variant}
        data-step={activeStep + 1}
      >
        <header className="tutorial-header">
          <div className="tutorial-heading">
            <p className="tutorial-kicker">{content.kicker}</p>
            <h2 className="tutorial-title" id={titleId}>
              {content.title}
            </h2>
          </div>
          {variant === 'reference' && (
            <button
              type="button"
              className="game-over-close tutorial-close-button"
              onClick={onClose}
              aria-label="Back to the game"
              data-testid="tutorial-header-close"
            >
              <span aria-hidden="true">×</span>
            </button>
          )}
        </header>

        <div className="tutorial-content">
          <nav className="tutorial-progress" aria-label="Tutorial steps">
            {content.steps.map((tutorialStep, index) => (
              <button
                type="button"
                className={`tutorial-progress-segment${index === activeStep ? ' is-active' : ''}${visitedSteps.has(index) && index !== activeStep ? ' is-complete' : ''}`}
                onClick={() => showStep(index)}
                aria-current={index === activeStep ? 'step' : undefined}
                aria-label={`Step ${index + 1} of ${content.steps.length}: ${tutorialStep.title}`}
                data-tutorial-step-control={index}
                key={tutorialStep.scene}
              />
            ))}
          </nav>

          <div className="tutorial-step" data-testid="tutorial-step">
            <div className="tutorial-visual-shell" key={`visual-${step.scene}`}>
              <div
                className="tutorial-visual"
                role="img"
                aria-label={step.visualLabel}
                data-testid="tutorial-visual"
              >
                <TutorialSceneCanvas scene={step.scene} replayToken={replayToken} />
              </div>
              <button
                type="button"
                className="tutorial-replay"
                onClick={() => setReplayToken((token) => token + 1)}
                aria-label={`Replay ${step.title.toLowerCase()} animation`}
              >
                <svg viewBox="0 0 16 16" aria-hidden="true">
                  <path d="M13.2 4.9A5.8 5.8 0 1 0 14 8h-1.7a4.15 4.15 0 1 1-1.05-2.76L9.4 7.1H15V1.5l-1.8 1.8v1.6Z" />
                </svg>
                Replay
              </button>
            </div>

            <div className="tutorial-copy" aria-live="polite" aria-atomic="true">
              <div
                className="tutorial-copy-inner"
                id={activeStepBodyId}
                key={`copy-${step.scene}`}
              >
                <p className="tutorial-step-index">
                  Step {String(activeStep + 1).padStart(2, '0')}
                  <span aria-hidden="true"> / {String(content.steps.length).padStart(2, '0')}</span>
                </p>
                <h3 className="tutorial-step-title">{step.title}</h3>
                <p className="tutorial-step-body">{step.body}</p>
              </div>
            </div>

            <div className="tutorial-step-navigation">
              <button
                type="button"
                className="tutorial-step-button is-back"
                onClick={() => showStep(activeStep - 1)}
                disabled={activeStep === 0}
              >
                <span aria-hidden="true">←</span> Back
              </button>
              <button
                type="button"
                className="tutorial-step-button is-next"
                onClick={() => showStep(activeStep + 1)}
                disabled={activeStep === lastStepIndex}
              >
                Next <span aria-hidden="true">→</span>
              </button>
            </div>
          </div>
        </div>

        <footer className="tutorial-footer">
          <div className="tutorial-status" data-testid="tutorial-status">
            {isBriefing ? (
              waitingOnOthers ? (
                <span role="status">
                  {pendingCount > 0
                    ? `Waiting for ${pendingCount} ${pendingCount === 1 ? 'player' : 'players'}`
                    : 'All players ready'}
                </span>
              ) : autoReadySeconds !== null ? (
                <>
                  <span aria-hidden="true">Match starts in {autoReadySeconds}s</span>
                  <span className="sr-only">The match will start automatically soon.</span>
                </>
              ) : (
                <span>Review at your pace</span>
              )
            ) : (
              <span>Use ← and → to move between steps</span>
            )}
          </div>

          {isBriefing ? (
            <button
              type="button"
              className="game-shell-button is-primary tutorial-ready-button"
              onClick={() => {
                dialogRef.current?.focus();
                onReady();
              }}
              disabled={isReady}
              data-testid="tutorial-ready"
            >
              <span>Ready</span>
              {isReady && <span className="tutorial-ready-check" aria-hidden="true">✓</span>}
            </button>
          ) : (
            <button
              type="button"
              className="game-shell-button is-primary tutorial-ready-button"
              onClick={onClose}
              data-testid="tutorial-close"
            >
              Back to game
            </button>
          )}
        </footer>
      </div>
    </div>
  );
};

export default TutorialModal;
