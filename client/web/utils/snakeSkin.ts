import { getWasm } from '../wasm';

/**
 * Everything `client/src/render.rs` needs to resolve one snake's colours.
 *
 * The field names are the Rust wire names on purpose: this object is
 * JSON-encoded and handed straight to `snakeSkinColors` / `renderRosterSnake`,
 * so the arena palette has exactly one definition and no snake hex value is
 * mirrored into TypeScript where it could drift from the renderer.
 */
export interface SnakeSkinInputs {
  snake_index: number;
  team_id: number | null;
  team_member_slot: number;
  snake_count: number;
  is_team_game: boolean;
  local_snake_id: number | null;
  local_team_id: number | null;
}

export interface SnakeSkinColors {
  fill: string;
  outline: string;
  label: string;
}

/**
 * The authoritative colours for a snake, for the few places that need a hex
 * string rather than a rendered glyph (the results-table swatch). Returns null
 * until the WASM module has loaded; callers should fall back to a CSS default
 * rather than inventing a colour.
 */
export const resolveSnakeSkinColors = (skin: SnakeSkinInputs): SnakeSkinColors | null => {
  const wasm = getWasm();
  if (!wasm) {
    return null;
  }

  try {
    return JSON.parse(wasm.snakeSkinColors(JSON.stringify(skin))) as SnakeSkinColors;
  } catch (error) {
    console.warn('Failed to resolve snake skin colors:', error);
    return null;
  }
};
