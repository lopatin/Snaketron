export const isGameRoutePath = (pathname: string): boolean =>
  pathname.startsWith('/play/');

/**
 * A retained party moving between matches must render the assigned game
 * immediately. Deferring this route swap to an animation timer can strand a
 * background/throttled browser on the previous match's score card.
 */
export const shouldSwapRouteImmediately = (
  displayedPathname: string,
  nextPathname: string,
): boolean =>
  displayedPathname !== nextPathname &&
  isGameRoutePath(displayedPathname) &&
  isGameRoutePath(nextPathname);
