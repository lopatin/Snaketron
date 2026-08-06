import React from 'react';
import type { MatchResultArtwork } from '../utils/gamePresentation';

const ARTWORK_LAYERS: Record<Exclude<MatchResultArtwork, 'neutral'>, readonly string[]> = {
  'azure-cut': [
    'caustic',
    'blade-light',
    'blade-dark',
    'blade-mid',
    'blade-edge-strong',
    'blade-edge-soft',
    'dark-tail',
    'seams',
    'top-gloss',
    'bottom-shade',
    'rim',
  ],
  'ruby-shatter': [
    'caustic',
    'counter-dark',
    'counter-light',
    'counter-cross',
    'counter-depth',
    'counter-edge',
    'dark-tail',
    'seams',
    'top-gloss',
    'bottom-shade',
    'rim',
  ],
  'topaz-cut': [
    'radial-facets',
    'radial-glint',
    'caustic',
    'cut-light',
    'cut-edge',
    'dark-tail',
    'seams',
    'top-gloss',
    'bottom-shade',
    'rim',
  ],
  'jade-fracture': [
    'caustic',
    'fracture-light',
    'fracture-depth',
    'cracks',
    'dark-tail',
    'top-gloss',
    'bottom-shade',
    'rim',
  ],
};

export interface GameOverJewelProps {
  artwork: MatchResultArtwork;
}

const GameOverJewel: React.FC<GameOverJewelProps> = ({ artwork }) => {
  if (artwork === 'neutral') {
    return null;
  }

  return (
    <div
      className={`game-over-jewel is-${artwork}`}
      data-testid="game-over-jewel"
      data-result-artwork={artwork}
      aria-hidden="true"
    >
      {ARTWORK_LAYERS[artwork].map((layer) => (
        <div key={layer} className={`game-over-jewel-layer is-${layer}`} />
      ))}
    </div>
  );
};

export default GameOverJewel;
