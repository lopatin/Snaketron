import { gameStorage } from '../services/gameStorage.ts';
import { getWasm } from '../wasm/index.ts';

/**
 * Which skin this client wears.
 *
 * Stored locally and sent at join; the server checks it against its catalogue
 * before it reaches anyone else's renderer. A value this build does not
 * recognise is not an error at any layer — the renderer falls back to the
 * classic look — so the only job here is to avoid handing the wire something
 * absurd.
 */
export const SKIN_PREFERENCE_STORAGE_KEY = 'snaketron:skin:v1';

/** The look every client can always render. */
export const DEFAULT_SKIN_REF = 'classic@1';

/** Long enough for `sha256:<64 hex>` once player-authored skins exist. */
const MAX_SKIN_REF_LENGTH = 96;

export interface SkinCatalogEntry {
  id: string;
  name: string;
}

/**
 * Reject anything that could not be a catalogue id. This is hygiene, not
 * security: the server is what decides whether an id is real.
 */
export const isPlausibleSkinRef = (value: unknown): value is string =>
  typeof value === 'string' &&
  value.length > 0 &&
  value.length <= MAX_SKIN_REF_LENGTH &&
  /^[a-z0-9@:._-]+$/i.test(value);

export const readSkinPreference = (): string => {
  const stored = gameStorage.getItem(SKIN_PREFERENCE_STORAGE_KEY);
  return isPlausibleSkinRef(stored) ? stored : DEFAULT_SKIN_REF;
};

export const writeSkinPreference = (skinRef: string): void => {
  if (!isPlausibleSkinRef(skinRef)) {
    return;
  }
  gameStorage.setItem(SKIN_PREFERENCE_STORAGE_KEY, skinRef);
};

/**
 * The skins this build can render, straight from the Rust catalogue so the
 * list can never drift from what the renderer actually knows.
 * Empty until the WASM module has loaded.
 */
export const readSkinCatalog = (): SkinCatalogEntry[] => {
  const wasm = getWasm();
  if (!wasm) {
    return [];
  }
  try {
    return JSON.parse(wasm.skinCatalog()) as SkinCatalogEntry[];
  } catch (error) {
    console.warn('Failed to read the skin catalog:', error);
    return [];
  }
};
