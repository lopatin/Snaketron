import React from 'react';

/**
 * The Snake Bux coin.
 *
 * One component rather than an `<img>` at each call site so the asset path and
 * the "it is a picture of the currency, not information" decision are made
 * once. It is always decorative: every place it appears, the number beside it
 * is the content, and a screen reader announcing "coin" before each balance
 * would be noise.
 */
const SnakeBuxIcon: React.FC<{ size?: number; className?: string }> = ({
  size = 18,
  className,
}) => (
  <img
    src="/images/snake-bux.png"
    alt=""
    aria-hidden="true"
    draggable={false}
    width={size}
    height={size}
    className={className ? `snake-bux-icon ${className}` : 'snake-bux-icon'}
  />
);

export default SnakeBuxIcon;
