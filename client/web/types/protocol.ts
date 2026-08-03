// Typed WebSocket protocol layer, derived entirely from the ts-rs-generated
// `WSMessage` union (the Rust source of truth in server/src/ws_server.rs).
//
// serde serializes `WSMessage` externally tagged, so every non-unit variant is
// a single-key object `{ [Tag]: Payload }` and unit variants are bare strings.
// The helpers below turn that union into a tag -> payload map so `onMessage`
// hands each handler a correctly-typed payload and `sendMessage` only accepts
// real messages — no more `any` and no `Object.keys` envelope-cracking.

import type { WSMessage, CustomGameSettings } from './generated';

export * from './generated';

/** Object-form (externally tagged) WSMessage variants: `{ [Tag]: Payload }`. */
type TaggedVariant = Extract<WSMessage, object>;
/** Unit WSMessage variants: bare string tags such as `"LeaveGame"`. */
type UnitVariant = Extract<WSMessage, string>;

type UnionToIntersection<U> = (U extends unknown ? (k: U) => void : never) extends (
  k: infer I,
) => void
  ? I
  : never;

/**
 * Merge every single-key tagged variant into one map:
 * `{ Token: string; JoinGame: number; GameEvent: GameEventMessage; ... }`.
 */
type TaggedPayloadMap = UnionToIntersection<TaggedVariant>;

/** Every WSMessage tag (both tagged and unit variants). */
export type WSMessageTag = keyof TaggedPayloadMap | UnitVariant;

/** Payload carried by a given tag; `null` for unit variants. */
export type PayloadOf<K extends WSMessageTag> = K extends keyof TaggedPayloadMap
  ? TaggedPayloadMap[K]
  : null;

/**
 * Envelope handed to `onMessage` handlers: the tag, its typed payload, and the
 * original raw frame text. `raw` lets the game-event path forward the exact
 * bytes to the WASM engine (via GameClient.processServerFrame) so full-range
 * u64 fields are never widened to f64 by a JS `JSON.parse`.
 */
export interface TypedMessage<K extends WSMessageTag = WSMessageTag> {
  type: K;
  data: PayloadOf<K>;
  raw: string;
}

/**
 * Messages the client currently SENDS that the server's `WSMessage` enum does
 * NOT define (verified against server/src/ws_server.rs). serde drops each of
 * these on deserialization, so these features are no-ops on the wire today.
 * They are typed here — rather than hidden behind `any` — so the drift stays
 * explicit and greppable.
 *
 * RESOLUTION NEEDED (product decision): implement these variants server-side,
 * or remove the UI that sends them. Tracked in TYPED_CLIENT_REFACTOR.md §1.
 */
export type UnsupportedClientMessage =
  | { CreateCustomGame: { settings: Partial<CustomGameSettings> } }
  | { JoinCustomGame: { game_code: string } }
  | { UpdateCustomGameSettings: { settings: Partial<CustomGameSettings> } }
  | 'StartCustomGame'
  | { SpectateGame: { game_id: string; game_code: string | null } };

/** Anything the client is allowed to hand to `sendMessage`. */
export type OutboundMessage = WSMessage | UnsupportedClientMessage;

/**
 * Parse a raw WebSocket frame into a tagged envelope. Returns null for frames
 * that are not a bare-string unit variant or a single-key tagged object (the
 * only two shapes serde's external tagging can produce).
 */
export function parseServerMessage(
  raw: string,
): { tag: WSMessageTag; data: unknown; raw: string } | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }

  if (typeof parsed === 'string') {
    return { tag: parsed as WSMessageTag, data: null, raw };
  }

  if (parsed && typeof parsed === 'object') {
    const keys = Object.keys(parsed);
    if (keys.length === 1) {
      const tag = keys[0] as WSMessageTag;
      return { tag, data: (parsed as Record<string, unknown>)[tag], raw };
    }
  }

  return null;
}
