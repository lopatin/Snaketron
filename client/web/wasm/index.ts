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
