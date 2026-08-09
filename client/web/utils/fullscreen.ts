/**
 * Thin, fail-open wrapper over the Fullscreen API.
 *
 * Fullscreen is an enhancement, never a requirement: every entry point here
 * degrades to a no-op on browsers without the API (notably iPhone Safari,
 * which has no element fullscreen at all), and callers hide their buttons
 * when `isFullscreenSupported()` is false rather than showing a dead control.
 *
 * CrazyGames builds never call this — the portal chrome owns fullscreen there
 * and their QA explicitly rejects games that add a second prompt.
 */

interface WebkitDocument extends Document {
  webkitFullscreenElement?: Element | null;
  webkitFullscreenEnabled?: boolean;
  webkitExitFullscreen?: () => Promise<void> | void;
}

interface WebkitElement extends HTMLElement {
  webkitRequestFullscreen?: () => Promise<void> | void;
}

const doc = (): WebkitDocument | null =>
  typeof document === 'undefined' ? null : (document as WebkitDocument);

export function isFullscreenSupported(): boolean {
  const d = doc();
  return Boolean(d && (d.fullscreenEnabled || d.webkitFullscreenEnabled));
}

export function fullscreenElement(): Element | null {
  const d = doc();
  return d ? d.fullscreenElement ?? d.webkitFullscreenElement ?? null : null;
}

export function isFullscreenActive(): boolean {
  return fullscreenElement() !== null;
}

/**
 * Enter or leave fullscreen on the whole page. Must be called from a user
 * gesture; rejections (denied permission, unsupported element) are swallowed
 * because the game is fully playable either way.
 */
export async function toggleFullscreen(): Promise<void> {
  const d = doc();
  if (!d) {
    return;
  }

  try {
    if (fullscreenElement()) {
      if (d.exitFullscreen) {
        await d.exitFullscreen();
      } else if (d.webkitExitFullscreen) {
        await d.webkitExitFullscreen();
      }
      return;
    }

    const root = d.documentElement as WebkitElement;
    if (root.requestFullscreen) {
      await root.requestFullscreen({ navigationUI: 'hide' });
    } else if (root.webkitRequestFullscreen) {
      await root.webkitRequestFullscreen();
    }
  } catch (error) {
    console.info('Fullscreen toggle unavailable:', error);
  }
}

/** Subscribe to fullscreen entry/exit, including the webkit-prefixed event. */
export function subscribeToFullscreenChanges(listener: () => void): () => void {
  const d = doc();
  if (!d) {
    return () => {};
  }

  d.addEventListener('fullscreenchange', listener);
  d.addEventListener('webkitfullscreenchange', listener);
  return () => {
    d.removeEventListener('fullscreenchange', listener);
    d.removeEventListener('webkitfullscreenchange', listener);
  };
}
