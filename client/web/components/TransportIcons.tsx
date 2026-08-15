import React from 'react';

/**
 * Transport glyphs for replay surfaces.
 *
 * These are drawn rather than typed because the text glyphs they replace
 * (↺ ▶ Ⅱ) render at a different weight and baseline in every font stack, so
 * a control sized to match the results card's close button lined up on one
 * platform and sat crooked on the next.
 */

const base: React.SVGProps<SVGSVGElement> = {
  viewBox: '0 0 16 16',
  fill: 'none',
  'aria-hidden': true,
  focusable: false,
};

/** Reload arrow: a near-closed circle with an arrowhead on the leading end. */
export const ReplayIcon: React.FC<{ className?: string }> = ({ className }) => (
  <svg {...base} className={className}>
    <path
      d="M13.2 8a5.2 5.2 0 1 1-1.72-3.86"
      stroke="currentColor"
      strokeWidth="1.7"
      strokeLinecap="round"
    />
    <path
      d="M13.4 2.3v3.2h-3.2"
      stroke="currentColor"
      strokeWidth="1.7"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </svg>
);

export const PlayIcon: React.FC<{ className?: string }> = ({ className }) => (
  <svg {...base} className={className}>
    <path d="M5.4 3.4 12.2 8l-6.8 4.6z" fill="currentColor" />
  </svg>
);

export const PauseIcon: React.FC<{ className?: string }> = ({ className }) => (
  <svg {...base} className={className}>
    <rect x="4.6" y="3.6" width="2.3" height="8.8" rx="0.6" fill="currentColor" />
    <rect x="9.1" y="3.6" width="2.3" height="8.8" rx="0.6" fill="currentColor" />
  </svg>
);
