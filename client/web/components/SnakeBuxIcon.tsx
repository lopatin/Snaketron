import React from 'react';

/**
 * The Snakebux coin.
 *
 * One component rather than an `<img>` at each call site so the asset choice
 * and the "it is a picture of the currency, not information" decision are made
 * once. It is always decorative: everywhere it appears the number beside it is
 * the content, and a screen reader announcing "coin" before each balance would
 * be noise.
 */

/**
 * Below this, the small rendering.
 *
 * There are two drawings of this coin, not one file scaled twice. The detailed
 * one carries a double rim and a fine bevel on the letter, and both of those
 * collapse into a smudge once the coin is smaller than roughly a line of text —
 * the letter, which is the only part that says *which* currency this is, went
 * first. The small one is drawn for that size instead: one rim, one glare, and
 * a thicker brighter letter, so what survives the downscale is still legible.
 */
const SMALL_ABOVE_PX = 24;

const SnakeBuxIcon: React.FC<{ size?: number; className?: string }> = ({
  size = 18,
  className,
}) => (
  <img
    src={size <= SMALL_ABOVE_PX ? '/images/snake-bux-small.png' : '/images/snake-bux.png'}
    alt=""
    aria-hidden="true"
    draggable={false}
    width={size}
    height={size}
    className={className ? `snake-bux-icon ${className}` : 'snake-bux-icon'}
  />
);

export default SnakeBuxIcon;
