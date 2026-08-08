/**
 * Reference-counted `document.body` scroll lock.
 *
 * Several arena surfaces are modal — the pre-match briefing, the in-match help
 * screen, the inactivity removal dialog — and more than one can be mounted in
 * the same commit while React settles which of them should own the screen.
 *
 * Each doing its own save/restore of `document.body.style.overflow` is what
 * breaks: the second locker saves the value the first one already wrote, and
 * restores `hidden` on close, leaving the page permanently unscrollable with no
 * modal on screen. Counting instead means only the first lock records the real
 * page value and only the last release puts it back.
 */

let lockCount = 0;
let restoreValue: string | null = null;

/** Lock body scrolling. Returns the matching release, safe to call once. */
export const lockBodyScroll = (): (() => void) => {
  if (lockCount === 0) {
    restoreValue = document.body.style.overflow;
  }
  lockCount += 1;
  document.body.style.overflow = 'hidden';

  let released = false;
  return () => {
    if (released) {
      return;
    }
    released = true;
    lockCount = Math.max(0, lockCount - 1);
    if (lockCount === 0) {
      document.body.style.overflow = restoreValue ?? '';
      restoreValue = null;
    }
  };
};
