export const isGameRoutePath = (pathname: string): boolean =>
  pathname.startsWith('/play/');

/**
 * The skins catalogue and a skin's own page within it — but not the builder,
 * which is a genuinely different screen. Opening a skin page is a modal
 * appearing over a catalogue that stays put, so fading the whole route would
 * misdescribe what is happening.
 */
export const isSkinsCataloguePath = (pathname: string): boolean =>
  pathname === '/skins' ||
  (pathname.startsWith('/skins/') && !pathname.startsWith('/skins/builder'));

/**
 * A retained party moving between matches must render the assigned game
 * immediately. Deferring this route swap to an animation timer can strand a
 * background/throttled browser on the previous match's score card.
 *
 * Moves within the skins catalogue also swap immediately: they toggle a
 * modal over an unchanging page rather than replacing the page.
 */
export const shouldSwapRouteImmediately = (
  displayedPathname: string,
  nextPathname: string,
): boolean =>
  displayedPathname !== nextPathname &&
  ((isGameRoutePath(displayedPathname) && isGameRoutePath(nextPathname)) ||
    (isSkinsCataloguePath(displayedPathname) && isSkinsCataloguePath(nextPathname)));
