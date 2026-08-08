import React, { useEffect, useId, useRef } from 'react';
import type { MatchPresentation } from '../utils/gamePresentation';
import {
  formatPerMinuteRate,
  getPlayAgainShortcutAction,
} from '../utils/gamePresentation';
import { resolveSnakeSkinColors } from '../utils/snakeSkin';
import GameOverJewel from './GameOverJewel';

const FOCUSABLE_SELECTOR = [
  'a[href]',
  'button:not([disabled])',
  'input:not([disabled])',
  'select:not([disabled])',
  'textarea:not([disabled])',
  '[tabindex]:not([tabindex="-1"])',
].join(',');

export interface GameOverCardProps {
  open: boolean;
  presentation: MatchPresentation;
  onDismiss: () => void;
  onMenu: () => void;
  onPlayAgain: () => void;
  playAgainDisabled?: boolean;
}

interface MetricLabelProps {
  label: string;
  name: string;
  description: string;
  tooltipId: string;
}

const MetricLabel: React.FC<MetricLabelProps> = ({
  label,
  name,
  description,
  tooltipId,
}) => (
  <span className="game-over-metric-label">
    <span>{label}</span>
    <span className="game-over-metric-help">
      <button
        type="button"
        aria-label={`About ${name.toLowerCase()}`}
        aria-describedby={tooltipId}
      >
        <span className="game-over-metric-help-mark" aria-hidden="true">?</span>
      </button>
      <span id={tooltipId} role="tooltip" className="game-over-metric-tooltip">
        {description}
      </span>
    </span>
  </span>
);

const GameOverCard: React.FC<GameOverCardProps> = ({
  open,
  presentation,
  onDismiss,
  onMenu,
  onPlayAgain,
  playAgainDisabled = false,
}) => {
  const dialogRef = useRef<HTMLDivElement>(null);
  const playAgainRef = useRef<HTMLButtonElement>(null);
  const onDismissRef = useRef(onDismiss);
  const onPlayAgainRef = useRef(onPlayAgain);
  const playAgainDisabledRef = useRef(playAgainDisabled);
  const titleId = useId();
  const summaryId = useId();
  const ppmTooltipId = useId();
  const apmTooltipId = useId();

  onDismissRef.current = onDismiss;
  onPlayAgainRef.current = onPlayAgain;
  playAgainDisabledRef.current = playAgainDisabled;

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
      const initialControl = playAgainDisabledRef.current
        ? dialogRef.current?.querySelector<HTMLElement>(FOCUSABLE_SELECTOR)
        : playAgainRef.current;
      (initialControl ?? dialogRef.current)?.focus();
    });

    const handleKeyDown = (event: KeyboardEvent) => {
      const shortcutAction = getPlayAgainShortcutAction(
        event,
        true,
        playAgainDisabledRef.current,
      );
      if (shortcutAction === 'play-again') {
        event.preventDefault();
        onPlayAgainRef.current();
        return;
      }
      if (shortcutAction === 'suppress') {
        event.preventDefault();
        return;
      }
      if (event.key === 'Escape') {
        event.preventDefault();
        onDismissRef.current();
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
      if (event.shiftKey && (document.activeElement === first || document.activeElement === dialogRef.current)) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };

    document.addEventListener('keydown', handleKeyDown);
    return () => {
      window.cancelAnimationFrame(focusFrame);
      document.removeEventListener('keydown', handleKeyDown);
      document.body.style.overflow = previousOverflow;
      if (previouslyFocused?.isConnected) {
        previouslyFocused.focus();
      }
    };
  }, [open]);

  useEffect(() => {
    const dialog = dialogRef.current;
    if (!open || !playAgainDisabled || !dialog) {
      return;
    }

    // Disabling the focused replay button after queueing can move focus back
    // to the document body. Keep keyboard focus inside the still-open modal.
    if (
      document.activeElement === playAgainRef.current ||
      !dialog.contains(document.activeElement)
    ) {
      (dialog.querySelector<HTMLElement>(FOCUSABLE_SELECTOR) ?? dialog).focus();
    }
  }, [open, playAgainDisabled]);

  if (!open) {
    return null;
  }

  const current = presentation.currentPlayer;
  const scoreline = presentation.isTeamGame
    ? `${presentation.sides[0]?.score ?? 0}–${presentation.sides[1]?.score ?? 0}`
    : current?.score.toString() ?? presentation.soloScore.toString();

  return (
    <div
      className="game-over-backdrop"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) {
          onDismiss();
        }
      }}
      data-testid="game-over-backdrop"
    >
      <div
        id="game-over-scorecard"
        ref={dialogRef}
        className={`game-over-card is-${presentation.resultTone}`}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-describedby={summaryId}
        tabIndex={-1}
        data-testid="game-over-card"
      >
        <header
          className={`game-over-header is-${presentation.resultArtwork}`}
          data-result-artwork={presentation.resultArtwork}
        >
          <GameOverJewel artwork={presentation.resultArtwork} />
          <div className="game-over-title-section">
            <span className="game-over-kicker">Final · {presentation.modeLabel}</span>
            <h2 id={titleId}>{presentation.resultTitle}</h2>
            <p id={summaryId}>{presentation.resultSummary}</p>
          </div>
          <div className="game-over-score-panel">
            <strong className="game-over-scoreline" aria-label={`Final score ${scoreline}`}>
              {scoreline}
            </strong>
          </div>
          <button
            type="button"
            className="game-over-close"
            onClick={onDismiss}
            aria-label="Close score card"
          >
            <span aria-hidden="true">✕</span>
          </button>
        </header>

        <div className="game-over-statline" aria-label="Your match statistics">
          <div>
            <span>XP gained</span>
            <strong className="is-xp">+{current?.xpGained ?? 0}</strong>
          </div>
          <div>
            <span>Score</span>
            <strong className="is-score">{current?.score ?? presentation.soloScore}</strong>
          </div>
          <div>
            <span>Time taken</span>
            <strong className="is-time">{presentation.timeTaken}</strong>
          </div>
          <div>
            <MetricLabel
              label="PPM"
              name="Points per minute"
              description="Points per minute: your final points divided by the elapsed match time."
              tooltipId={ppmTooltipId}
            />
            <strong className="is-ppm">{formatPerMinuteRate(presentation.pointsPerMinute)}</strong>
          </div>
          <div>
            <MetricLabel
              label="APM"
              name="Accepted actions per minute"
              description="Accepted actions per minute: valid turns and successful Boost starts or manual stops divided by the elapsed match time. Retries, rejected inputs, and no-ops are excluded."
              tooltipId={apmTooltipId}
            />
            <strong className="is-apm">{formatPerMinuteRate(presentation.actionsPerMinute)}</strong>
          </div>
        </div>

        <div className="game-over-standings">
          <div className="game-over-standings-heading">
            <span>Player</span>
            <span>Points</span>
          </div>
          {presentation.players.map((player) => (
            <div
              key={player.snakeId}
              className="game-over-player"
            >
              <span
                className="game-roster-swatch"
                style={{
                  '--snake-color': resolveSnakeSkinColors(player.skin)?.fill,
                } as React.CSSProperties}
                aria-hidden="true"
              />
              <span className="game-over-player-name">
                {player.name}
                {player.isWinner && <span className="game-over-winner">Winner</span>}
                {player.isIdleKicked && <span className="game-over-idle">Idle</span>}
              </span>
              <strong>{player.score}</strong>
            </div>
          ))}
        </div>

        <footer className="game-over-actions">
          <button type="button" onClick={onMenu} className="game-shell-button is-menu">
            Main menu
          </button>
          <div className="game-over-replay-actions">
            <span className="game-over-shortcut" aria-hidden="true">
              <kbd><span>Space</span></kbd>
              <span>to</span>
            </span>
            <button
              ref={playAgainRef}
              type="button"
              onClick={onPlayAgain}
              disabled={playAgainDisabled}
              className="game-shell-button is-primary game-primary-motion"
            >
              {!playAgainDisabled && (
                <span className="game-start-enable-sweep" aria-hidden="true">
                  <svg className="game-start-enable-chevron is-primary" viewBox="0 0 42 34">
                    <path d="M0 0h11l17 17-17 17H0l17-17ZM13 0h11l17 17-17 17H13l17-17Z" />
                  </svg>
                  <svg className="game-start-enable-chevron is-echo" viewBox="0 0 42 34">
                    <path d="M0 0h11l17 17-17 17H0l17-17ZM13 0h11l17 17-17 17H13l17-17Z" />
                  </svg>
                </span>
              )}
              <span className="game-start-content game-over-play-content">
                <span>{playAgainDisabled ? 'Queued…' : 'Play again'}</span>
                {!playAgainDisabled && (
                  <svg
                    className="game-over-play-chevron"
                    viewBox="0 0 18 14"
                    aria-hidden="true"
                    focusable="false"
                  >
                    <path d="M0 1.4 5.6 7 0 12.6 1.4 14l7-7-7-7L0 1.4Zm8.6 0L14.2 7l-5.6 5.6L10 14l7-7-7-7-1.4 1.4Z" />
                  </svg>
                )}
              </span>
            </button>
          </div>
        </footer>
      </div>
    </div>
  );
};

export default GameOverCard;
