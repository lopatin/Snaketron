import { getWasm } from '../wasm/index.ts';

/**
 * What this player is wearing.
 *
 * There is exactly one store: the account record. `selected_skin` is what
 * match preparation reads when it decides which skin travels to every other
 * player (`server/src/matchmaking.rs::apply_player_skin`), so anything else
 * claiming to know the answer can only ever disagree with what opponents
 * actually see — which is what a second, browser-local copy used to do.
 *
 * The functions here are the decoding rules for that one store, not a store
 * of their own: they turn the two nullable fields on `UserInfo` into the
 * values the picker and the arena ask for, and back again for the wire.
 *
 * A value this build does not recognise is not an error at any layer: the
 * renderer falls back to the classic look. So the only job of the validator
 * is to avoid handing the wire something absurd.
 */

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
 * The two slots as an account carries them.
 *
 * Looser than the generated `Equipment` on purpose: the server's `UserInfo`
 * omits a slot it has no value for rather than sending null, so a slot read
 * off a `User` is `undefined` as often as it is null. Both mean the same
 * thing here — wearing the default — and neither is an explicit clear.
 */
export interface EquippedRefs {
  selectedSkin?: string | null;
  selectedBase?: string | null;
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

/**
 * The snake skin an account is wearing. Falls back to the classic look, which
 * is what an account that has never equipped anything is in fact wearing —
 * the server resolves an absent `selected_skin` the same way.
 */
export const equippedSkinRef = (equipment: EquippedRefs | null | undefined): string =>
  isPlausibleSkinRef(equipment?.selectedSkin) ? equipment.selectedSkin : DEFAULT_SKIN_REF;

/**
 * The equipped base, as a bare snake reference.
 *
 * Callers want "which look dresses my arena", not the storage encoding, so
 * the prefix is stripped here and re-applied by {@link toBaseSlotValue}. Null
 * means no explicit choice, which the arena reads as "use whatever base theme
 * my snake skin carries".
 */
export const equippedBaseRef = (equipment: EquippedRefs | null | undefined): string | null => {
  const stored = equipment?.selectedBase;
  if (!isPlausibleSkinRef(stored) || !stored.startsWith(BASE_REF_PREFIX)) {
    return null;
  }
  const inner = stored.slice(BASE_REF_PREFIX.length);
  return isPlausibleSkinRef(inner) ? inner : null;
};

/** The wire form of a base choice: the account stores the prefix. */
export const toBaseSlotValue = (snakeRef: string): string => `${BASE_REF_PREFIX}${snakeRef}`;

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
