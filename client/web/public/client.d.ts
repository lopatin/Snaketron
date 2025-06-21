/* tslint:disable */
/* eslint-disable */
/**
 * Renders the game state to a canvas element
 * Takes a JSON string representation of the game state
 */
export function render_game(game_state_json: string, canvas: HTMLCanvasElement, cell_size: number): void;
/**
 * The main client-side game interface exposed to JavaScript.
 * This wraps the GameEngine and provides a clean WASM boundary.
 */
export class GameClient {
  free(): void;
  /**
   * Creates a new game client instance
   */
  constructor(game_id: number, start_ms: bigint);
  /**
   * Creates a new game client instance from an existing game state
   */
  static newFromState(game_id: number, start_ms: bigint, state_json: string): GameClient;
  /**
   * Set the local player ID
   */
  setLocalPlayerId(player_id: number): void;
  /**
   * Run the game engine until the specified timestamp
   * Returns a JSON array of game events that occurred
   */
  runUntil(ts_ms: bigint): string;
  /**
   * Process a turn command for a snake with client-side prediction
   * Returns the command message that should be sent to the server
   */
  processTurn(snake_id: number, direction: string): string;
  /**
   * Process a server event for reconciliation
   */
  processServerEvent(event_json: string): void;
  /**
   * Initialize game state from a snapshot
   */
  initializeFromSnapshot(state_json: string): void;
  /**
   * Get the current game state as JSON
   */
  getGameStateJson(): string;
  /**
   * Get the committed (server-authoritative) state as JSON
   */
  getCommittedStateJson(): string;
  /**
   * Get the event log as JSON
   */
  getEventLogJson(): string;
  /**
   * Get the current tick number
   */
  getCurrentTick(): number;
  /**
   * Get the game ID
   */
  getGameId(): number;
}

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
  readonly memory: WebAssembly.Memory;
  readonly render_game: (a: number, b: number, c: any, d: number) => [number, number];
  readonly __wbg_gameclient_free: (a: number, b: number) => void;
  readonly gameclient_new: (a: number, b: bigint) => number;
  readonly gameclient_newFromState: (a: number, b: bigint, c: number, d: number) => [number, number, number];
  readonly gameclient_setLocalPlayerId: (a: number, b: number) => void;
  readonly gameclient_runUntil: (a: number, b: bigint) => [number, number, number, number];
  readonly gameclient_processTurn: (a: number, b: number, c: number, d: number) => [number, number, number, number];
  readonly gameclient_processServerEvent: (a: number, b: number, c: number) => [number, number];
  readonly gameclient_initializeFromSnapshot: (a: number, b: number, c: number) => [number, number];
  readonly gameclient_getGameStateJson: (a: number) => [number, number, number, number];
  readonly gameclient_getCommittedStateJson: (a: number) => [number, number, number, number];
  readonly gameclient_getEventLogJson: (a: number) => [number, number, number, number];
  readonly gameclient_getCurrentTick: (a: number) => number;
  readonly gameclient_getGameId: (a: number) => number;
  readonly __wbindgen_exn_store: (a: number) => void;
  readonly __externref_table_alloc: () => number;
  readonly __wbindgen_export_2: WebAssembly.Table;
  readonly __wbindgen_free: (a: number, b: number, c: number) => void;
  readonly __wbindgen_malloc: (a: number, b: number) => number;
  readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
  readonly __externref_table_dealloc: (a: number) => void;
  readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;
/**
* Instantiates the given `module`, which can either be bytes or
* a precompiled `WebAssembly.Module`.
*
* @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
*
* @returns {InitOutput}
*/
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
* If `module_or_path` is {RequestInfo} or {URL}, makes a request and
* for everything else, calls `WebAssembly.instantiate` directly.
*
* @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
*
* @returns {Promise<InitOutput>}
*/
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
