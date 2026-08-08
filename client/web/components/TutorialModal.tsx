import React, { useCallback, useEffect, useId, useRef, useState } from 'react';
import type { TutorialContent } from '../utils/tutorial';
import { lockBodyScroll } from '../utils/bodyScrollLock';
import TutorialSceneCanvas from './TutorialSceneCanvas';

const FOCUSABLE_SELECTOR = [
  'a[href]',
  'button:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(',');

const STEP_DURATION_MS = 5_000;
const READY_WINDOW_SECONDS = 15;
// The readiness deadline hands off to the arena's existing 3-2-1 countdown.
const MATCH_START_COUNTDOWN_SECONDS = 3;
const AUTO_START_HORIZON_SECONDS =
  READY_WINDOW_SECONDS + MATCH_START_COUNTDOWN_SECONDS;

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
  const showStepRef = useRef<(nextStep: number, focusStepControl?: boolean) => void>(() => {});
  const titleId = useId();
  const stepBodyId = useId();
  const autoStartDescriptionId = useId();
  const [activeStep, setActiveStep] = useState(0);
  const [replayToken, setReplayToken] = useState(0);
  const [progressCycle, setProgressCycle] = useState(0);
  const [isUserPaused, setIsUserPaused] = useState(false);
  const [isDocumentHidden, setIsDocumentHidden] = useState(() => (
    typeof document !== 'undefined' && document.hidden
  ));
  const [reducedMotion, setReducedMotion] = useState(() => (
    typeof window !== 'undefined' &&
    typeof window.matchMedia === 'function' &&
    window.matchMedia('(prefers-reduced-motion: reduce)').matches
  ));

  const lastStepIndex = content.steps.length - 1;
  const step = content.steps[activeStep] ?? content.steps[0];
  const activeStepBodyId = `${stepBodyId}-${activeStep}`;
  const isBriefing = variant === 'briefing';
  const waitingOnOthers = isBriefing && isReady;
  const autoplayEnabled = isBriefing && !reducedMotion;
  const autoplayPaused = isUserPaused || isDocumentHidden;
  const autoStartSeconds = autoReadySeconds === null
    ? null
    : autoReadySeconds + MATCH_START_COUNTDOWN_SECONDS;
  const autoStartProgress = autoReadySeconds === null
    ? 0
    : Math.max(0, Math.min(1, autoStartSeconds! / AUTO_START_HORIZON_SECONDS));

  onCloseRef.current = onClose;
  activeStepRef.current = activeStep;
  variantRef.current = variant;

  const showStep = useCallback((nextStep: number, focusStepControl = true) => {
    const boundedStep = Math.max(0, Math.min(lastStepIndex, nextStep));
    if (focusStepControl && boundedStep !== activeStep) {
      pendingStepFocusRef.current = true;
    }
    setActiveStep(boundedStep);
    setReplayToken(0);
    setProgressCycle((cycle) => cycle + 1);
  }, [activeStep, lastStepIndex]);
  showStepRef.current = showStep;

  const replayStep = useCallback(() => {
    setReplayToken((token) => token + 1);
    setProgressCycle((cycle) => cycle + 1);
  }, []);

  useEffect(() => {
    if (open) {
      setActiveStep(0);
      setReplayToken(0);
      setProgressCycle((cycle) => cycle + 1);
      setIsUserPaused(false);
    }
  }, [content.key, open]);

  useEffect(() => {
    if (typeof window.matchMedia !== 'function') {
      return undefined;
    }
    const query = window.matchMedia('(prefers-reduced-motion: reduce)');
    const handleChange = () => setReducedMotion(query.matches);
    handleChange();
    query.addEventListener('change', handleChange);
    return () => query.removeEventListener('change', handleChange);
  }, []);

  useEffect(() => {
    const handleVisibility = () => setIsDocumentHidden(document.hidden);
    handleVisibility();
    document.addEventListener('visibilitychange', handleVisibility);
    return () => document.removeEventListener('visibilitychange', handleVisibility);
  }, []);

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
    // Counted: the inactivity removal dialog is also modal and can be mounted
    // in the same commit, and two save/restore pairs strand the lock.
    const releaseBodyScroll = lockBodyScroll();
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
      releaseBodyScroll();
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
        aria-describedby={isBriefing
          ? `${activeStepBodyId} ${autoStartDescriptionId}`
          : activeStepBodyId}
        tabIndex={-1}
        data-testid="tutorial-modal"
        data-tutorial-key={content.key}
        data-variant={variant}
        data-step={activeStep + 1}
        data-autoplay={autoplayEnabled ? (autoplayPaused ? 'paused' : 'playing') : 'off'}
        data-reduced-motion={reducedMotion ? 'true' : 'false'}
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
          <div className="tutorial-progress-row">
            <nav className="tutorial-progress" aria-label="Tutorial steps">
              {content.steps.map((tutorialStep, index) => (
                <button
                  type="button"
                  className={`tutorial-progress-segment${index === activeStep ? ' is-active' : ''}${index < activeStep ? ' is-complete' : ''}`}
                  onClick={() => showStep(index)}
                  aria-current={index === activeStep ? 'step' : undefined}
                  aria-label={`Step ${index + 1} of ${content.steps.length}: ${tutorialStep.title}`}
                  data-tutorial-step-control={index}
                  key={tutorialStep.scene}
                >
                  <span
                    className="tutorial-progress-fill"
                    data-testid={index === activeStep ? 'tutorial-step-timer' : undefined}
                    key={`${tutorialStep.scene}-${index === activeStep ? progressCycle : 0}`}
                    onAnimationEnd={(event) => {
                      if (
                        index === activeStep &&
                        autoplayEnabled &&
                        !autoplayPaused &&
                        event.animationName === 'tutorial-progress-countdown' &&
                        activeStep < lastStepIndex
                      ) {
                        showStep(activeStep + 1, false);
                      }
                    }}
                    style={{
                      animationDuration: `${STEP_DURATION_MS}ms`,
                      animationPlayState: autoplayPaused ? 'paused' : 'running',
                    }}
                    aria-hidden="true"
                  />
                </button>
              ))}
            </nav>
            {autoplayEnabled && (
              <button
                type="button"
                className="tutorial-autoplay-toggle"
                onClick={() => setIsUserPaused((paused) => !paused)}
                aria-label={isUserPaused ? 'Resume tutorial' : 'Pause tutorial'}
                data-testid="tutorial-autoplay-toggle"
              >
                {isUserPaused ? (
                  <svg viewBox="0 0 16 16" aria-hidden="true">
                    <path d="M5 3.5v9l7-4.5-7-4.5Z" />
                  </svg>
                ) : (
                  <svg viewBox="0 0 16 16" aria-hidden="true">
                    <path d="M4.5 3.5h2.5v9H4.5zM9 3.5h2.5v9H9z" />
                  </svg>
                )}
              </button>
            )}
          </div>

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
                onClick={replayStep}
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
                <p className="tutorial-step-instruction">{step.body}</p>
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
          {isBriefing && (
            <span className="sr-only" id={autoStartDescriptionId}>
              {reducedMotion
                ? 'Use the step controls to review each lesson. '
                : 'Each lesson advances automatically after five seconds; use Pause to stop it. '}
              The match starts automatically after the readiness timer and a three-second countdown.
            </span>
          )}
          <div className="tutorial-status" data-testid="tutorial-status">
            {isBriefing ? (
              waitingOnOthers ? (
                <span className="tutorial-ready-status" role="status">
                  {pendingCount > 0
                    ? `Waiting for ${pendingCount} ${pendingCount === 1 ? 'player' : 'players'}`
                    : 'All players ready'}
                </span>
              ) : (
                <span className="tutorial-ready-status">Ready when you are</span>
              )
            ) : (
              <span>Use ← and → to move between steps</span>
            )}
            {isBriefing && autoStartSeconds !== null && (
              <span
                className="tutorial-auto-start"
                role="timer"
                aria-label={`Automatic match start in ${autoStartSeconds} seconds`}
                style={{
                  '--tutorial-auto-start-progress': autoStartProgress,
                } as React.CSSProperties}
                data-testid="tutorial-auto-start"
              >
                <span className="tutorial-auto-start-copy" aria-hidden="true">
                  <span>Auto-start</span>
                  <strong>{autoStartSeconds}s</strong>
                </span>
                <span className="tutorial-auto-start-track" aria-hidden="true">
                  <span className="tutorial-auto-start-fill" />
                </span>
              </span>
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
