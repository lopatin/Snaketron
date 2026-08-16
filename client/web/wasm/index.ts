// Typed singleton around the wasm-bindgen module. Replaces the ad-hoc
// `window.wasm` / `window.wasmReady` globals: app code imports `initWasm`/
// `getWasm` and gets the real generated types (client/pkg/client.d.ts) instead
// of a hand-written ambient declaration that used to shadow them.
import initWasmModule, * as wasmNs from 'wasm-snaketron';

export type WasmModule = typeof wasmNs;
export { GameClient } from 'wasm-snaketron';

declare global {
  interface Window {
    // Debug/e2e-only handle. Application code must use getWasm()/initWasm();
    // this is populated purely so Playwright can probe load state.
    wasm?: WasmModule;
    wasmReady?: Promise<WasmModule>;
  }
}

let loaded: WasmModule | null = null;
let readyPromise: Promise<WasmModule> | null = null;

/**
 * Initialize the WASM runtime exactly once. Safe to call repeatedly — every
 * caller awaits the same promise. Resolves with the module namespace.
 */
export function initWasm(): Promise<WasmModule> {
  if (!readyPromise) {
    readyPromise = initWasmModule().then(() => {
      loaded = wasmNs;
      if (typeof window !== 'undefined') {
        window.wasm = wasmNs;
      }
      return wasmNs;
    });
    if (typeof window !== 'undefined') {
      window.wasmReady = readyPromise;
    }
  }
  return readyPromise;
}

/**
 * The initialized module, or null if init has not completed yet. Use this in
 * per-frame / event-handler hot paths that must not await.
 */
export function getWasm(): WasmModule | null {
  return loaded;
}

/** How long to keep waiting on a texture before giving up on it. */
const SKIN_ASSET_TIMEOUT_MS = 5000;

/**
 * Resolve once every skin texture requested so far has decoded or failed.
 *
 * A textured skin (the animal family) fetches its pixels the first time it
 * paints, so the first paint shows the flat coat underneath. The arena repaints
 * every frame and never notices. Surfaces that paint **once** — the roster
 * glyph, a contact-sheet tile — would keep the flat coat forever, so they paint,
 * await this, and paint once more.
 *
 * Resolves immediately when nothing is pending, which is the common case: only
 * the first appearance of a textured skin in a session ever waits.
 */
export function whenSkinAssetsSettle(): Promise<void> {
  const wasm = getWasm();
  if (!wasm?.skinAssetsPending()) {
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    const deadline = Date.now() + SKIN_ASSET_TIMEOUT_MS;
    const poll = () => {
      if (!getWasm()?.skinAssetsPending() || Date.now() > deadline) {
        resolve();
        return;
      }
      // rAF rather than a timer: a hidden tab has nothing to repaint anyway,
      // and this keeps the wait off the clock until it does.
      requestAnimationFrame(poll);
    };
    requestAnimationFrame(poll);
  });
}
