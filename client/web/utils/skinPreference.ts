import { gameStorage } from '../services/gameStorage.ts';
import { getWasm } from '../wasm/index.ts';

/**
 * What this client wears.
 *
 * Two slots, stored locally and mirrored to the account when there is one.
 * The account is the authority — it is what match preparation reads when it
 * decides which skin travels to every other player — and local storage is the
 * echo that lets the arena paint correctly before `/api/auth/me` answers, and
 * that carries a signed-out visitor's choice at all.
 *
 * A value this build does not recognise is not an error at any layer: the
 * renderer falls back to the classic look. So the only job here is to avoid
 * handing the wire something absurd.
 */
export const SKIN_PREFERENCE_STORAGE_KEY = 'snaketron:skin:v1';

/**
 * The base slot.
 *
 * Base dressing is viewer-attributed (`specs/skins-prd.md` section 5.6): it
 * themes the arena its owner is looking at and never reaches another player.
 * That is why this key has no counterpart in game state — unlike the snake
 * slot, nothing about it needs to travel.
 */
export const BASE_PREFERENCE_STORAGE_KEY = 'snaketron:base:v1';

/** The look every client can always render. */
export const DEFAULT_SKIN_REF = 'classic@1';

/** A base reference is a snake reference wearing this prefix. */
export const BASE_REF_PREFIX = 'base:';

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
 * The equipped base, as a bare snake reference.
 *
 * Callers want "which look dresses my arena", not the storage encoding, so the
 * prefix is stripped here and re-applied by {@link writeBasePreference}. Null
 * means no explicit choice, which the renderer reads as "use whatever base
 * theme my snake skin carries".
 */
export const readBasePreference = (): string | null => {
  const stored = gameStorage.getItem(BASE_PREFERENCE_STORAGE_KEY);
  if (!isPlausibleSkinRef(stored) || !stored.startsWith(BASE_REF_PREFIX)) {
    return null;
  }
  const inner = stored.slice(BASE_REF_PREFIX.length);
  return isPlausibleSkinRef(inner) ? inner : null;
};

export const writeBasePreference = (snakeRef: string | null): void => {
  if (snakeRef === null) {
    gameStorage.removeItem(BASE_PREFERENCE_STORAGE_KEY);
    return;
  }
  if (!isPlausibleSkinRef(snakeRef)) {
    return;
  }
  gameStorage.setItem(BASE_PREFERENCE_STORAGE_KEY, `${BASE_REF_PREFIX}${snakeRef}`);
};

/**
 * Adopt what the account says is equipped.
 *
 * Called once the authenticated user resolves. The account wins over whatever
 * this browser had stored, because the account is what other players actually
 * see — a stale local value would otherwise have the picker disagree with the
 * arena for everyone else.
 */
export const adoptServerEquipment = (equipment: {
  selectedSkin?: string | null;
  selectedBase?: string | null;
}): void => {
  if (isPlausibleSkinRef(equipment.selectedSkin)) {
    writeSkinPreference(equipment.selectedSkin);
  }
  if (typeof equipment.selectedBase === 'string' && equipment.selectedBase.startsWith(BASE_REF_PREFIX)) {
    writeBasePreference(equipment.selectedBase.slice(BASE_REF_PREFIX.length));
  } else if (equipment.selectedBase === null) {
    writeBasePreference(null);
  }
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
