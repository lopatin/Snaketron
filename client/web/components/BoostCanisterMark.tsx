import React from 'react';

/**
 * The NOS canister glyph shared by the arena Boost meter and the mobile Boost
 * button. `className` picks the surface-specific sizing; the artwork itself
 * is identical everywhere so the pickup, the meter, and the button all read
 * as the same object.
 */
export function BoostCanisterMark({
  className = 'game-boost-meter__canister',
}: {
  className?: string;
}) {
  return (
    <svg
      className={className}
      data-testid="boost-nos-bottle"
      viewBox="0 0 34 24"
      preserveAspectRatio="xMidYMid meet"
      shapeRendering="geometricPrecision"
      aria-hidden="true"
      focusable="false"
    >
      <g className="game-boost-meter__canister-tilt" transform="rotate(-24 17 12)">
        <path
          className="game-boost-meter__canister-base"
          fill="#3b82f6"
          d="M2.8 4.8h18.4l3.2 2.8h2V6.1h4.3v2H33v7.8h-2.3v2h-4.3v-1.5h-2l-3.2 2.8H2.8L.6 17V7l2.2-2.2Z"
        />
        <path
          className="game-boost-meter__canister-body"
          fill="#3b82f6"
          d="M3.2 6.3h17.4l2.2 2v7.4l-2.2 2H3.2L2 16.5v-9l1.2-1.2Z"
        />
        <path
          className="game-boost-meter__canister-highlight"
          fill="#93c5fd"
          d="M2.8 4.8h18.4l3.2 2.8h2v1.1h-2.3l-3.2-2.4H3.2L2 7.5v3H.6V7l2.2-2.2Z"
        />
        <path
          className="game-boost-meter__canister-shade"
          fill="#2563eb"
          d="M.6 13.5H2v3l1.2 1.2h17.4l2.2-2v-2.2h1.6v2.9h2v1.5h-2l-3.2 1.3H2.8L.6 17v-3.5Z"
        />
        <rect
          className="game-boost-meter__pressure-plate-separator"
          x="5"
          y="6.3"
          width="15.8"
          height="11.4"
          fill="#f8fafc"
        />
        <rect
          className="game-boost-meter__pressure-plate"
          x="6.7"
          y="8"
          width="12.4"
          height="8"
          fill="#ff641e"
        />
        <text
          className="game-boost-meter__nos-wordmark"
          x="12.9"
          y="12.25"
          fill="#fff"
          fontFamily="Arial, sans-serif"
          fontSize="5.5"
          fontStyle="normal"
          fontWeight="900"
          letterSpacing="0"
          textAnchor="middle"
          dominantBaseline="middle"
        >
          NOS
        </text>
        <path fill="#f8fafc" d="M24.2 9.2h2.4v5.6h-2.4Z" />
        <path fill="#93c5fd" d="M26.2 7.5h3.1v9h-3.1Z" />
        <path fill="#f8fafc" d="M27 7.5h2.3v4.3H27Z" />
        <path fill="#ff641e" d="M29.3 8.6h2v2.6h-2Z" />
        <path fill="#2563eb" d="M29.3 13h2v2.4h-2Z" />
      </g>
    </svg>
  );
}

export default BoostCanisterMark;
