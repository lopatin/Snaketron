/* tslint:disable */
/* eslint-disable */
export const memory: WebAssembly.Memory;
export const render_game: (a: number, b: number, c: any, d: number) => [number, number];
export const __wbg_gameclient_free: (a: number, b: number) => void;
export const gameclient_new: (a: number, b: bigint) => number;
export const gameclient_newFromState: (a: number, b: bigint, c: number, d: number) => [number, number, number];
export const gameclient_setLocalPlayerId: (a: number, b: number) => void;
export const gameclient_runUntil: (a: number, b: bigint) => [number, number, number, number];
export const gameclient_processTurn: (a: number, b: number, c: number, d: number) => [number, number, number, number];
export const gameclient_processServerEvent: (a: number, b: number, c: number, d: bigint) => [number, number];
export const gameclient_initializeFromSnapshot: (a: number, b: number, c: number, d: bigint) => [number, number];
export const gameclient_getGameStateJson: (a: number) => [number, number, number, number];
export const gameclient_getCommittedStateJson: (a: number) => [number, number, number, number];
export const gameclient_getEventLogJson: (a: number) => [number, number, number, number];
export const gameclient_getCurrentTick: (a: number) => number;
export const gameclient_getGameId: (a: number) => number;
export const __wbindgen_exn_store: (a: number) => void;
export const __externref_table_alloc: () => number;
export const __wbindgen_export_2: WebAssembly.Table;
export const __wbindgen_free: (a: number, b: number, c: number) => void;
export const __wbindgen_malloc: (a: number, b: number) => number;
export const __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
export const __externref_table_dealloc: (a: number) => void;
export const __wbindgen_start: () => void;
