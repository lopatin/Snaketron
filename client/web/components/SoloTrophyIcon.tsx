import React from 'react';

/**
 * The Solo (Classic Snake) leaderboard emblem.
 *
 * Solo is a high-score board rather than a rated ladder, so it gets a trophy
 * instead of a rank badge — but it is built from the same grammar as the rank
 * badges so the two sit together: one heavy dark ink outline at a uniform
 * weight, flat faces split down the centre (lighter left, darker right), and a
 * specular sliver along the top-facing rim.
 *
 * Where the rank badges are metal, this one is ink-on-white: the cup reads as
 * white paper with a dark outline and only enough grey to model the form. That
 * keeps Solo visually distinct from the ranked tiers, and matches both the
 * app's black-on-white styling and the unranked badge's treatment.
 *
 * The ink pass strokes each part in the outline colour so the whole silhouette
 * reads as one object; the face pass repaints the interiors, leaving an even
 * outline behind. Outlines are cubic beziers so the cup and handles curve
 * smoothly at any size.
 */

const INK = '#252e33';
const WHITE = '#ffffff';
const SHADE = '#e8ecee';
const DEEP = '#c3cbcf';

const BOWL = 'M60 46C60 84 70 110 86 120L114 120C130 110 140 84 140 46Z';
const RIM = 'M56 40L144 40L144 56L56 56Z';
const STEM = 'M90 120L110 120C110 132 112 138 112 142L88 142C88 138 90 132 90 120Z';
const BASE = 'M74 142L126 142C132 152 136 160 138 168L62 168C64 160 68 152 74 142Z';
const HANDLE_L =
  'M60 52C36 50 20 62 20 80C20 98 36 112 58 116L60 100C46 96 36 90 36 80C36 70 46 64 60 66Z';
const HANDLE_R =
  'M140 52C164 50 180 62 180 80C180 98 164 112 142 116L140 100C154 96 164 90 164 80C164 70 154 64 140 66Z';

const BOWL_L = 'M60 46C60 84 70 110 86 120L100 120L100 46Z';
const BOWL_R = 'M100 46L100 120L114 120C130 110 140 84 140 46Z';
const RIM_L = 'M56 40L100 40L100 56L56 56Z';
const RIM_R = 'M100 40L144 40L144 56L100 56Z';
const STEM_L = 'M90 120L100 120L100 142L88 142C88 138 90 132 90 120Z';
const STEM_R = 'M100 120L110 120C110 132 112 138 112 142L100 142Z';
const BASE_L = 'M74 142L100 142L100 168L62 168C64 160 68 152 74 142Z';
const BASE_R = 'M100 142L126 142C132 152 136 160 138 168L100 168Z';

const INK_PARTS = [BOWL, HANDLE_L, HANDLE_R, STEM, BASE, RIM];

/** Painted after the ink pass, so each fill sits inside its own outline. */
const FACES: Array<[string, string]> = [
  [HANDLE_L, SHADE],
  [HANDLE_R, DEEP],
  [BOWL_L, WHITE],
  [BOWL_R, SHADE],
  [STEM_L, SHADE],
  [STEM_R, DEEP],
  [BASE_L, SHADE],
  [BASE_R, DEEP],
  [RIM_L, WHITE],
  [RIM_R, WHITE],
];

interface SoloTrophyIconProps {
  className?: string;
  /** Accessible name. Omit for purely decorative placements. */
  label?: string;
}

const SoloTrophyIcon: React.FC<SoloTrophyIconProps> = ({ className, label }) => (
  <svg
    viewBox="0 0 200 200"
    className={className}
    role={label ? 'img' : undefined}
    aria-label={label}
    aria-hidden={label ? undefined : true}
    focusable="false"
  >
    {label && <title>{label}</title>}
    {INK_PARTS.map((d, i) => (
      <path
        key={`ink-${i}`}
        d={d}
        fill={INK}
        stroke={INK}
        strokeWidth={14}
        strokeLinejoin="round"
        strokeLinecap="round"
      />
    ))}
    {FACES.map(([d, fill], i) => (
      <path key={`face-${i}`} d={d} fill={fill} />
    ))}
  </svg>
);

export default SoloTrophyIcon;
