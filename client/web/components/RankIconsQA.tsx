import React, { useState } from 'react';
import type { RankDivision } from '../types';
import RankIcon, { RankTierIcon } from './RankIcon';
import { RANK_ICON_DATA, type RankIconTier } from './rankIconData';
import {
  RANK_ICON_DIVISION_DATA,
  type RankIconDivisionTier,
} from './rankIconDivisionData';

/**
 * The original tier rasters, kept only so this harness can diff the traced
 * vectors against the art they reproduce. Product code renders RankIcon.
 */
const getRankImage = (tier: RankIconTier): string => `images/${tier}.png`;

/**
 * Dev-only design-review harness for the rank badges (`/qa/rank-icons`,
 * excluded from production routing). Renders every traced vector badge next
 * to the raster original it reproduces, with a difference-blend overlay to
 * expose any drift, so icon design changes can be reviewed without playing
 * ranked matches at every MMR band.
 */

const TIERS = Object.keys(RANK_ICON_DATA) as RankIconTier[];
const DIVISION_TIERS = Object.keys(RANK_ICON_DIVISION_DATA) as RankIconDivisionTier[];
const DIVISION_LABELS = ['I', 'II', 'III'];

const SMALL_SIZES: Array<{ px: number; cls: string }> = [
  { px: 16, cls: 'w-4 h-4' },
  { px: 24, cls: 'w-6 h-6' },
  { px: 32, cls: 'w-8 h-8' },
  { px: 48, cls: 'w-12 h-12' },
];

const RankIconsQA: React.FC = () => {
  const [showOverlay, setShowOverlay] = useState(true);

  return (
    <div className="min-h-screen bg-white px-8 py-10" data-testid="rank-icons-qa">
      <h1 className="font-black italic tracking-1 text-2xl text-black-70 uppercase">
        Rank Icons — raster vs traced vector
      </h1>
      <p className="mt-1 text-sm text-gray-500 max-w-2xl">
        Left pair: original PNG and its RankIcon SVG reproduction. Overlay:
        both stacked with difference blending on a white tile — solid black
        means the pair is identical; anything visible is drift. Right: PNG
        (top) vs SVG (bottom) at product sizes.
      </p>
      <label className="mt-3 inline-flex items-center gap-2 text-sm font-bold text-black-70">
        <input
          type="checkbox"
          checked={showOverlay}
          onChange={e => setShowOverlay(e.target.checked)}
        />
        Show difference overlay
      </label>

      <div className="mt-6 flex flex-col gap-10">
        {TIERS.map(tier => (
          <div key={tier} className="flex items-start gap-8" data-testid={`qa-row-${tier}`}>
            <div className="w-28 pt-16 font-black italic uppercase tracking-1 text-black-70">
              {tier}
            </div>
            <div className="flex flex-col items-center gap-1">
              <img src={getRankImage(tier)} alt="" className="w-40 h-40" draggable={false} />
              <span className="text-xs text-gray-500">PNG</span>
            </div>
            <div className="flex flex-col items-center gap-1">
              <RankTierIcon tier={tier} className="w-40 h-40" />
              <span className="text-xs text-gray-500">SVG</span>
            </div>
            {showOverlay && (
              <div className="flex flex-col items-center gap-1">
                <div className="relative w-40 h-40 bg-white isolate">
                  <img
                    src={getRankImage(tier)}
                    alt=""
                    className="absolute inset-0 w-40 h-40"
                    draggable={false}
                  />
                  <div className="absolute inset-0 mix-blend-difference">
                    <RankTierIcon tier={tier} className="w-40 h-40" />
                  </div>
                </div>
                <span className="text-xs text-gray-500">overlay</span>
              </div>
            )}
            <div className="flex items-end gap-5 pt-10">
              {SMALL_SIZES.map(({ px, cls }) => (
                <div key={px} className="flex flex-col items-center gap-2">
                  <img src={getRankImage(tier)} alt="" className={cls} draggable={false} />
                  <RankTierIcon tier={tier} className={cls} />
                  <span className="text-[10px] text-gray-400">{px}px</span>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>

      <h2 className="mt-14 font-black italic tracking-1 text-2xl text-black-70 uppercase">
        Division variants
      </h2>
      <p className="mt-1 text-sm text-gray-500 max-w-2xl">
        Each division&apos;s badge carries as many chevrons as its number.
        Division II is the original tier art, unchanged.
      </p>
      <div className="mt-6 flex flex-col gap-8">
        {DIVISION_TIERS.map(tier => (
          <div key={tier} className="flex items-start gap-8" data-testid={`qa-divisions-${tier}`}>
            <div className="w-28 pt-8 font-black italic uppercase tracking-1 text-black-70">
              {tier}
            </div>
            {Object.keys(RANK_ICON_DIVISION_DATA[tier]).map(div => {
              const division = Number(div) as RankDivision;
              return (
                <div key={div} className="flex flex-col items-center gap-1">
                  <RankIcon tier={tier} division={division} className="w-28 h-28" />
                  <span className="text-xs text-gray-500">
                    {DIVISION_LABELS[division - 1]}
                  </span>
                  <div className="mt-1 flex items-end gap-2">
                    <RankIcon tier={tier} division={division} className="w-6 h-6" />
                    <RankIcon tier={tier} division={division} className="w-4 h-4" />
                  </div>
                </div>
              );
            })}
          </div>
        ))}
      </div>
    </div>
  );
};

export default RankIconsQA;
