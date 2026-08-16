/**
 * Loads the crash sprite before a scenario is reported ready. `decode()` is
 * the strongest signal when the browser provides it: a successful network
 * response can still contain bytes the renderer cannot decode. Older browser
 * surfaces fall back to the corresponding load/error events.
 */
export const loadScenarioSprite = async (
  image: HTMLImageElement,
  source: string,
): Promise<void> => {
  if (typeof image.decode === 'function') {
    image.src = source;
    try {
      await image.decode();
    } catch {
      throw new Error(`Replay sprite failed to decode: ${source}`);
    }
    return;
  }

  await new Promise<void>((resolve, reject) => {
    let settled = false;
    const cleanup = () => {
      image.removeEventListener('load', handleLoad);
      image.removeEventListener('error', handleError);
    };
    const settleLoaded = () => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve();
    };
    const settleFailed = () => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(new Error(`Replay sprite failed to load: ${source}`));
    };
    const handleLoad = () => {
      if (image.naturalWidth > 0) {
        settleLoaded();
      } else {
        settleFailed();
      }
    };
    const handleError = () => settleFailed();

    image.addEventListener('load', handleLoad);
    image.addEventListener('error', handleError);
    image.src = source;

    // Cached images may already be complete before the listeners can observe
    // an event. `naturalWidth` distinguishes a usable cached image from a
    // completed failed request.
    if (image.complete) {
      handleLoad();
    }
  });
};
