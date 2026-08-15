import React, { useId } from 'react';
import type { RankDivision, RankTier } from '../types';
import {
  RANK_ICON_DATA,
  RANK_ICON_VIEW_BOX,
  type RankIconDefinition,
  type RankIconTier,
} from './rankIconData';
import { RANK_ICON_DIVISION_DATA } from './rankIconDivisionData';

/**
 * Vector rank badge. Tier art is traced from the original rasters by
 * client/design/tools/trace_rank_icons.py; the per-division variants come
 * from client/design/tools/build_division_icons.py.
 *
 * Why inline SVG rather than the exported PNGs (public/images/ranks/): the
 * whole 25-icon set costs ~11.6 KB gzipped inside an already-fetched chunk
 * and zero extra requests, where the equivalent rasters are ~1.3 MB of
 * committed binaries and up to 26 image requests (~75 KB) for one cold
 * leaderboard page. Badges also render at 24/38/46/64 px and the reveal
 * medallion animates through ~83 px, so a fixed raster grid would need
 * regenerating for every CSS size change. The rasters exist only for
 * surfaces that cannot inline SVG — share cards and embeds.
 *
 * Ranked tiers must pass a `division`; the base tier art is by design the
 * division-2 look, so an omitted division would silently render a wrong but
 * plausible badge. The prop types make that a compile error instead.
 */
type RankIconProps = {
  className?: string;
  /** Accessible name. Omit for purely decorative placements. */
  label?: string;
} & (
  | { tier: 'unranked'; division?: never }
  | { tier: RankTier; division: RankDivision }
);

const toIconTier = (tier: RankTier | 'unranked'): RankIconTier =>
  tier === 'master' ? 'grandmaster' : tier;

interface BadgeProps {
  definition: RankIconDefinition;
  className?: string;
  label?: string;
}

const Badge: React.FC<BadgeProps> = ({ definition, className, label }) => {
  // Gradient ids must be unique per mounted instance (a leaderboard renders
  // dozens of badges), and colons from useId are not safe in url(#...) refs.
  const uid = useId().replace(/:/g, '');
  const { gradients, shapes } = definition;
  const localFill = (fill: string): string =>
    fill.startsWith('url(#') ? `url(#${uid}-${fill.slice(5)}` : fill;

  return (
    <svg
      viewBox={RANK_ICON_VIEW_BOX}
      className={className}
      role={label ? 'img' : undefined}
      aria-label={label}
      aria-hidden={label ? undefined : true}
      focusable="false"
    >
      {label && <title>{label}</title>}
      {gradients.length > 0 && (
        <defs>
          {gradients.map(g => (
            <linearGradient
              key={g.id}
              id={`${uid}-${g.id}`}
              gradientUnits="userSpaceOnUse"
              x1={g.x1}
              y1={g.y1}
              x2={g.x2}
              y2={g.y2}
            >
              {g.stops.map((s, i) => (
                <stop key={i} offset={s.offset} stopColor={s.color} />
              ))}
            </linearGradient>
          ))}
        </defs>
      )}
      {shapes.map((s, i) => (
        // Painting the stroke in the same fill hides hairline seams between
        // adjacent traced facets without shifting edges visibly.
        <path
          key={i}
          d={s.d}
          fill={localFill(s.fill)}
          stroke={localFill(s.fill)}
          strokeWidth={0.8}
          strokeLinejoin="round"
          paintOrder="stroke"
        />
      ))}
    </svg>
  );
};

const RankIcon: React.FC<RankIconProps> = ({ tier, division, className, label }) => {
  const iconTier = toIconTier(tier);
  const definition =
    iconTier === 'unranked'
      ? RANK_ICON_DATA.unranked
      : RANK_ICON_DIVISION_DATA[iconTier][division as RankDivision];

  return <Badge definition={definition} className={className} label={label} />;
};

/**
 * The tier's base art with no division marks. Only the design-review harness
 * wants this — every product surface shows a specific sub-rank, so it renders
 * `RankIcon` with a division instead.
 */
export const RankTierIcon: React.FC<{
  tier: RankIconTier;
  className?: string;
  label?: string;
}> = ({ tier, className, label }) => (
  <Badge definition={RANK_ICON_DATA[tier]} className={className} label={label} />
);

export default RankIcon;
