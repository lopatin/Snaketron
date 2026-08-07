import React, { useCallback, useEffect, useId, useRef, useState } from 'react';
import type { TutorialContent, TutorialStep } from '../utils/tutorial';
import {
  TUTORIAL_PROTOTYPES,
  type TutorialPrototypeId,
} from '../utils/tutorialPrototype';
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

export interface TutorialPrototypeLab {
  value: TutorialPrototypeId;
  onChange: (prototype: TutorialPrototypeId) => void;
}

export interface TutorialModalProps {
  open: boolean;
  content: TutorialContent;
  /**
   * Briefing mode gates the match start and shows the readiness roster.
   * Reference mode reopens the same guide during play.
   */
  variant: 'briefing' | 'reference';
  /** Opt-in review control. Normal players always receive Arena Lens. */
  prototypeLab?: TutorialPrototypeLab;
  /** Seconds until everyone is readied automatically. Briefing mode only. */
  autoReadySeconds: number | null;
  /** How many players still have to confirm, including this one. */
  pendingCount: number;
  /** True once this player has confirmed and is waiting on the others. */
  isReady: boolean;
  onReady: () => void;
  onClose: () => void;
}

interface TutorialVisualProps {
  step: TutorialStep;
  replayToken: number;
  playback?: 'play' | 'poster';
  showReplay?: boolean;
  compact?: boolean;
  focusable?: boolean;
  testId?: string;
  onReplay: () => void;
}

const TutorialVisual: React.FC<TutorialVisualProps> = ({
  step,
  replayToken,
  playback = 'play',
  showReplay = false,
  compact = false,
  focusable = false,
  testId,
  onReplay,
}) => (
  <div className={`tutorial-visual-shell${compact ? ' is-compact' : ''}`}>
    <div
      className="tutorial-visual"
      role="img"
      aria-label={step.visualLabel}
      tabIndex={focusable ? 0 : undefined}
      data-testid={testId}
    >
      <TutorialSceneCanvas
        scene={step.scene}
        replayToken={playback === 'play' ? replayToken : 0}
        playback={playback}
      />
    </div>
    {showReplay && (
      <button
        type="button"
        className="tutorial-replay"
        onClick={onReplay}
        aria-label={`Replay ${step.title.toLowerCase()} animation`}
      >
        <svg viewBox="0 0 16 16" aria-hidden="true">
          <path d="M13.2 4.9A5.8 5.8 0 1 0 14 8h-1.7a4.15 4.15 0 1 1-1.05-2.76L9.4 7.1H15V1.5l-1.8 1.8v1.6Z" />
        </svg>
        Replay
      </button>
    )}
  </div>
);

const TutorialModal: React.FC<TutorialModalProps> = ({
  open,
  content,
  variant,
  prototypeLab,
  autoReadySeconds,
  pendingCount,
  isReady,
  onReady,
  onClose,
}) => {
  const dialogRef = useRef<HTMLDivElement>(null);
  const onCloseRef = useRef(onClose);
  const previousPrototypeRef = useRef<TutorialPrototypeId | null>(null);
  const activeStepRef = useRef(0);
  const variantRef = useRef(variant);
  const pendingStepFocusRef = useRef(false);
  const showStepRef = useRef<(nextStep: number) => void>(() => {});
  const titleId = useId();
  const stepTitleId = useId();
  const stepBodyId = useId();
  const [activeStep, setActiveStep] = useState(0);
  const [revealedCount, setRevealedCount] = useState(1);
  const [replayToken, setReplayToken] = useState(0);
  const [visitedSteps, setVisitedSteps] = useState<Set<number>>(() => new Set([0]));

  const prototype = prototypeLab?.value ?? 'lens';
  const lastStepIndex = content.steps.length - 1;
  const step = content.steps[activeStep] ?? content.steps[0];
  const effectiveRevealedCount = prototype === 'manual'
    ? Math.max(revealedCount, activeStep + 1)
    : revealedCount;
  const activeStepTitleId = `${stepTitleId}-${activeStep}`;
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
    if (prototype === 'manual') {
      setRevealedCount((current) => Math.max(current, boundedStep + 1));
    }
    setReplayToken(0);
  }, [activeStep, lastStepIndex, prototype]);
  showStepRef.current = showStep;

  useEffect(() => {
    if (open) {
      setActiveStep(0);
      setRevealedCount(1);
      setReplayToken(0);
      setVisitedSteps(new Set([0]));
    }
  }, [content.key, open]);

  // Keep the same lesson selected while comparing layouts. The active scene
  // restarts so a reviewer is comparing like with like, and Manual reveals the
  // selected lesson if it was reached in another prototype first.
  useEffect(() => {
    if (previousPrototypeRef.current === null) {
      previousPrototypeRef.current = prototype;
      return;
    }
    if (previousPrototypeRef.current !== prototype) {
      previousPrototypeRef.current = prototype;
      const contentPanel = dialogRef.current?.querySelector<HTMLElement>('.tutorial-content');
      if (contentPanel) {
        contentPanel.scrollTop = 0;
      }
      if (prototype === 'manual') {
        setRevealedCount((current) => Math.max(current, activeStep + 1));
      }
      setReplayToken((token) => token + 1);
    }
  }, [activeStep, prototype]);

  useEffect(() => {
    if (!open || !pendingStepFocusRef.current) {
      return undefined;
    }
    pendingStepFocusRef.current = false;
    const frame = window.requestAnimationFrame(() => {
      const dialog = dialogRef.current;
      if (!dialog) {
        return;
      }
      const compactCoach =
        prototype === 'coach' && window.matchMedia('(width <= 720px)').matches;
      const target = compactCoach
        ? dialog.querySelector<HTMLElement>('.tutorial-coach-stage .tutorial-visual')
        : dialog.querySelector<HTMLElement>(`[data-tutorial-step-control="${activeStep}"]`);
      target?.focus({ preventScroll: true });
      target?.scrollIntoView({ block: 'nearest', behavior: 'auto' });
    });
    return () => window.cancelAnimationFrame(frame);
  }, [activeStep, open, prototype]);

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

      const prototypeSwitchOwnsArrow =
        event.target instanceof Element &&
        event.target.closest('[data-tutorial-prototype-switch]') !== null;
      if (event.key === 'ArrowLeft' && !prototypeSwitchOwnsArrow) {
        event.preventDefault();
        showStepRef.current(activeStepRef.current - 1);
        return;
      }
      if (event.key === 'ArrowRight' && !prototypeSwitchOwnsArrow) {
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

  const replay = () => setReplayToken((token) => token + 1);

  const lensBody = (
    <>
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

      <div
        className="tutorial-step tutorial-lens-step"
        data-testid="tutorial-step"
      >
        <TutorialVisual
          key={`lens-visual-${step.scene}`}
          step={step}
          replayToken={replayToken}
          showReplay
          testId="tutorial-visual"
          onReplay={replay}
        />

        <div
          className="tutorial-copy"
          aria-live="polite"
          aria-atomic="true"
          key={`lens-copy-${step.scene}`}
        >
          <p className="tutorial-step-index">
            Step {String(activeStep + 1).padStart(2, '0')}
            <span aria-hidden="true"> / {String(content.steps.length).padStart(2, '0')}</span>
          </p>
          <h3 className="tutorial-step-title" id={activeStepTitleId}>
            {step.title}
          </h3>
          <p className="tutorial-step-body" id={activeStepBodyId}>
            {step.body}
          </p>
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
          <span className="tutorial-step-position" aria-hidden="true">
            {activeStep + 1} / {content.steps.length}
          </span>
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
    </>
  );

  const manualBody = (
    <div className="tutorial-manual" data-testid="tutorial-manual">
      <ol className="tutorial-manual-lessons" aria-label="Tutorial field manual">
        {content.steps.map((tutorialStep, index) => {
          const isActive = index === activeStep;
          const isRevealed = index < effectiveRevealedCount;
          const isNext = index === effectiveRevealedCount;
          const state = isActive ? 'active' : isRevealed ? 'revealed' : 'upcoming';
          const canSelect = isRevealed || isNext;
          const detailId = `${stepBodyId}-manual-${index}`;
          return (
            <li
              className="tutorial-manual-lesson"
              data-state={state}
              data-step-index={index + 1}
              key={tutorialStep.scene}
            >
              <h3 className="tutorial-manual-heading">
                <button
                  type="button"
                  className="tutorial-manual-select"
                  onClick={() => showStep(index)}
                  disabled={!canSelect}
                  aria-current={isActive ? 'step' : undefined}
                  aria-expanded={isRevealed}
                  aria-controls={detailId}
                  aria-label={`Step ${index + 1}: ${tutorialStep.title}${isRevealed ? '' : ', reveal lesson'}`}
                  data-tutorial-step-control={index}
                >
                  <span className="tutorial-manual-index">{String(index + 1).padStart(2, '0')}</span>
                  <span className="tutorial-manual-title">{tutorialStep.title}</span>
                  <span className="tutorial-manual-state" aria-hidden="true">
                    {isActive ? 'Live' : isRevealed ? 'Seen' : isNext ? 'Next' : 'Later'}
                  </span>
                </button>
              </h3>

              <div
                className="tutorial-manual-detail"
                id={detailId}
                hidden={!isRevealed}
                data-testid={isActive ? 'tutorial-step' : undefined}
                key={`manual-${tutorialStep.scene}-${isActive ? 'active' : 'poster'}`}
              >
                {isRevealed && (
                  <>
                  <TutorialVisual
                    step={tutorialStep}
                    replayToken={isActive ? replayToken : 0}
                    playback={isActive ? 'play' : 'poster'}
                    showReplay={isActive}
                    compact={!isActive}
                    testId={isActive ? 'tutorial-visual' : undefined}
                    onReplay={replay}
                  />
                  <p
                    className="tutorial-manual-body"
                    id={isActive ? activeStepBodyId : undefined}
                  >
                    {tutorialStep.body}
                  </p>
                  {isActive && index < lastStepIndex && (
                    <button
                      type="button"
                      className="tutorial-manual-next"
                      onClick={() => showStep(index + 1)}
                    >
                      Reveal {content.steps[index + 1].title}
                      <span aria-hidden="true"> ↓</span>
                    </button>
                  )}
                  </>
                )}
              </div>
            </li>
          );
        })}
      </ol>
    </div>
  );

  const coachBody = (
    <div
      className="tutorial-step tutorial-coach"
      data-testid="tutorial-step"
    >
      <div className="tutorial-coach-stage">
        <TutorialVisual
          key={`coach-visual-${step.scene}`}
          step={step}
          replayToken={replayToken}
          showReplay
          focusable
          testId="tutorial-visual"
          onReplay={replay}
        />
      </div>

      <div className="tutorial-coach-rail">
        <ol className="tutorial-coach-lessons" aria-label="Tutorial steps">
          {content.steps.map((tutorialStep, index) => {
            const isActive = index === activeStep;
            const isVisited = visitedSteps.has(index);
            const state = isActive ? 'active' : isVisited ? 'complete' : 'upcoming';
            const detailId = `${stepBodyId}-coach-${index}`;
            return (
              <li className="tutorial-coach-lesson" data-state={state} key={tutorialStep.scene}>
                <button
                  type="button"
                  className="tutorial-coach-select"
                  onClick={() => showStep(index)}
                  aria-current={isActive ? 'step' : undefined}
                  aria-expanded={isActive}
                  aria-controls={detailId}
                  aria-label={`Step ${index + 1}: ${tutorialStep.title}${isVisited && !isActive ? ', viewed' : ''}`}
                  data-tutorial-step-control={index}
                >
                  <span className="tutorial-coach-index">
                    Step {String(index + 1).padStart(2, '0')}
                  </span>
                  <span className="tutorial-coach-title">{tutorialStep.title}</span>
                  {isVisited && !isActive && (
                    <span className="tutorial-coach-check" aria-hidden="true">✓</span>
                  )}
                </button>
                <div className="tutorial-coach-detail" id={detailId} hidden={!isActive}>
                  {isActive && (
                  <p
                    className="tutorial-coach-body"
                    id={activeStepBodyId}
                  >
                    {tutorialStep.body}
                  </p>
                  )}
                </div>
              </li>
            );
          })}
        </ol>

        <div className="tutorial-coach-navigation">
          <button
            type="button"
            className="tutorial-step-button is-back"
            onClick={() => showStep(activeStep - 1)}
            disabled={activeStep === 0}
          >
            <span aria-hidden="true">←</span> Previous
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
  );

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
        data-prototype={prototype}
        data-step={activeStep + 1}
        data-revealed={effectiveRevealedCount}
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

        {prototypeLab && (
          <div
            className="tutorial-prototype-switch"
            role="group"
            aria-label="Tutorial design prototype"
            data-testid="tutorial-prototype-switch"
            data-tutorial-prototype-switch
          >
            <span className="tutorial-prototype-label">Prototype</span>
            <div className="tutorial-prototype-options">
              {TUTORIAL_PROTOTYPES.map((option) => (
                <button
                  type="button"
                  className="tutorial-prototype-option"
                  aria-pressed={prototype === option.id}
                  title={option.description}
                  onClick={() => prototypeLab.onChange(option.id)}
                  key={option.id}
                >
                  {option.label}
                </button>
              ))}
            </div>
          </div>
        )}

        <div className="tutorial-content">
          {prototype === 'manual' ? manualBody : prototype === 'coach' ? coachBody : lensBody}
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
