import React, { useRef } from 'react';
import BoostCanisterMark from './BoostCanisterMark';
import { FullscreenEnterIcon, FullscreenExitIcon } from './Icons';
import type { BoostHudView } from '../utils/boostHud';
import type { BoostInputMode } from '../utils/boostInput';

/** A direction as the player sees it on screen, before arena-rotation mapping. */
export type ScreenDirection = 'Up' | 'Down' | 'Left' | 'Right';

/**
 * Viewport room the arena sizing must leave free for the touch clusters.
 * GameArena's `calculateSizes` subtracts these and GameArena.css pads the
 * stage by the same values (via --touch-portrait-reserve/--touch-side-reserve
 * set on the arena root), so the canvas never slides under a cluster.
 *
 * Portrait: one cluster row (Boost beside the 158px d-pad) hugging the bottom
 * edge (18px) with a 12px gap to the arena. Landscape: each side column is
 * 158px wide with a 14px edge margin and a 16px gap to the arena.
 */
export const TOUCH_PORTRAIT_BOTTOM_RESERVE_PX = 188;
export const TOUCH_LANDSCAPE_SIDE_RESERVE_PX = 188;

interface BoostPointerHandlers {
  onPointerDown: (event: React.PointerEvent<HTMLButtonElement>) => void;
  onPointerRelease: (event: React.PointerEvent<HTMLButtonElement>) => void;
}

export interface TouchBoostBinding {
  hud: BoostHudView;
  inputMode: BoostInputMode;
  /** Toggle-mode activation; the controller ignores it in hold mode. */
  onTap: () => void;
  /**
   * Hold-mode pointer edges, one independent binding per rendered button so
   * both landscape Boost buttons can be held at once — the shared controller
   * counts holds and ends Boost on the last release.
   */
  primary: BoostPointerHandlers;
  secondary: BoostPointerHandlers;
}

interface TouchControlsProps {
  onSteer: (direction: ScreenDirection) => void;
  /** Absent when the match has no Boost or the local snake is gone. */
  boost: TouchBoostBinding | null;
  /** Absent in CrazyGames builds (the portal owns fullscreen) and on iPhones. */
  fullscreen: { active: boolean; onToggle: () => void } | null;
}

const DIRECTION_LABELS: Record<ScreenDirection, string> = {
  Up: 'Steer up',
  Down: 'Steer down',
  Left: 'Steer left',
  Right: 'Steer right',
};

/** Chevron pointing up; each button rotates it via CSS. */
function ChevronGlyph() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path
        d="M4.5 14.6 12 7.4l7.5 7.2"
        fill="none"
        stroke="currentColor"
        strokeWidth="3.4"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

interface DPadProps {
  onSteer: (direction: ScreenDirection) => void;
  /** Distinguishes the two landscape pads for tests and ARIA. */
  side: 'left' | 'right';
}

function DPad({ onSteer, side }: DPadProps) {
  // Steering fires on pointerdown for zero-latency response. Assistive
  // technologies drive buttons with click instead, so click is honored too —
  // but a click that immediately follows our own pointerdown on the same
  // direction is the browser's synthesized follow-up, not a second command.
  const recentPointerSteerRef = useRef<{
    direction: ScreenDirection;
    at: number;
  } | null>(null);

  const steerFromPointer =
    (direction: ScreenDirection) => (event: React.PointerEvent<HTMLButtonElement>) => {
      if (event.button !== 0 && event.pointerType === 'mouse') {
        return;
      }
      recentPointerSteerRef.current = { direction, at: event.timeStamp };
      onSteer(direction);
    };

  const steerFromClick =
    (direction: ScreenDirection) => (event: React.MouseEvent<HTMLButtonElement>) => {
      const recent = recentPointerSteerRef.current;
      if (
        recent &&
        recent.direction === direction &&
        event.timeStamp - recent.at < 600
      ) {
        return;
      }
      onSteer(direction);
    };

  return (
    <div
      className="touch-dpad"
      role="group"
      aria-label={side === 'left' ? 'Steering pad (left)' : 'Steering pad'}
      data-testid={`touch-dpad-${side}`}
      onContextMenu={(event) => event.preventDefault()}
    >
      {(['Up', 'Left', 'Right', 'Down'] as const).map((direction) => (
        <button
          key={direction}
          type="button"
          className={`touch-dpad__btn touch-dpad__btn--${direction.toLowerCase()}`}
          aria-label={DIRECTION_LABELS[direction]}
          data-testid={`touch-steer-${direction.toLowerCase()}`}
          tabIndex={-1}
          onPointerDown={steerFromPointer(direction)}
          onClick={steerFromClick(direction)}
        >
          <ChevronGlyph />
        </button>
      ))}
      <span className="touch-dpad__hub" aria-hidden="true" />
    </div>
  );
}

function BoostButton({
  boost,
  handlers,
  testId,
}: {
  boost: TouchBoostBinding;
  handlers: BoostPointerHandlers;
  testId: string;
}) {
  const { hud } = boost;
  const chargeDegrees = Math.round(hud.fillRatio * 360);
  return (
    <button
      type="button"
      className={
        'touch-boost' +
        (hud.active ? ' is-active' : '') +
        (hud.ready ? ' is-ready' : '')
      }
      style={{ '--touch-boost-charge': `${chargeDegrees}deg` } as React.CSSProperties}
      disabled={hud.buttonDisabled}
      aria-label={boost.inputMode === 'hold'
        ? (hud.active
            ? `Release Boost, ${hud.unlimited ? 'unlimited' : `${hud.percent}% remaining`}`
            : `Hold to Boost, ${hud.unlimited ? 'unlimited' : `${hud.percent}% charged`}`)
        : (hud.active
            ? `Stop Boost, ${hud.unlimited ? 'unlimited' : `${hud.percent}% remaining`}`
            : `Activate Boost, ${hud.unlimited ? 'unlimited' : `${hud.percent}% charged`}`)}
      data-testid={testId}
      onClick={boost.onTap}
      onPointerDown={handlers.onPointerDown}
      onPointerUp={handlers.onPointerRelease}
      onPointerCancel={handlers.onPointerRelease}
      onLostPointerCapture={handlers.onPointerRelease}
      onContextMenu={(event) => event.preventDefault()}
    >
      <BoostCanisterMark className="touch-boost__canister" />
    </button>
  );
}

/**
 * On-screen gameplay controls for touch surfaces.
 *
 * Portrait shows one cluster near the bottom-right: the NOS Boost button
 * beside the d-pad. Landscape shows a mirrored cluster on each side — a d-pad
 * at each top corner with a Boost button beneath it — so either hand can
 * steer and boost. The fullscreen toggle is not part of the clusters; it
 * pairs with the chat dock as a second rectangular utility button. Which
 * clusters are visible is pure CSS (orientation media queries), so rotating
 * the device never remounts a control mid-hold.
 */
export function TouchControls({ onSteer, boost, fullscreen }: TouchControlsProps) {
  return (
    <div className="touch-controls" data-testid="touch-controls">
      <div className="touch-controls__cluster touch-controls__cluster--left">
        <DPad onSteer={onSteer} side="left" />
        {boost && (
          <BoostButton
            boost={boost}
            handlers={boost.secondary}
            testId="touch-boost-button-left"
          />
        )}
      </div>
      <div className="touch-controls__cluster touch-controls__cluster--right">
        <DPad onSteer={onSteer} side="right" />
        {boost && (
          <BoostButton
            boost={boost}
            handlers={boost.primary}
            testId="touch-boost-button"
          />
        )}
      </div>
      {fullscreen && (
        <button
          type="button"
          className="touch-fullscreen"
          aria-label={fullscreen.active ? 'Exit full screen' : 'Enter full screen'}
          data-testid="touch-fullscreen-button"
          onClick={fullscreen.onToggle}
        >
          {fullscreen.active
            ? <FullscreenExitIcon className="touch-fullscreen__icon" />
            : <FullscreenEnterIcon className="touch-fullscreen__icon" />}
        </button>
      )}
    </div>
  );
}

export default TouchControls;
