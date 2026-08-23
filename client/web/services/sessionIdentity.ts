/**
 * Reading the durable user id out of a Snaketron session token.
 *
 * This exists because analytics has to know who it is reporting as *before* it
 * initializes, which is earlier than `/auth/me` can answer. The token the
 * browser already holds carries the id, so it can be read with no round trip
 * and no race against the auth provider.
 *
 * The signature is deliberately not verified. Nothing is authorized by this
 * value — it only labels analytics — and a browser that tampers with its own
 * stored token can do nothing worse than mislabel its own events. Verifying
 * would require the server's key, which the client must never hold.
 */

/**
 * The `sub` claim, or `null` for any token that cannot be trusted to name the
 * user this browser is currently acting as.
 */
export const userIdFromSessionToken = (token: string, nowMs: number): string | null => {
  const payload = token.split('.')[1];
  if (!payload) {
    return null;
  }

  let claims: Record<string, unknown>;
  try {
    // base64url is not base64: restore the standard alphabet and the padding
    // `atob` requires. Decoding through TextDecoder rather than treating the
    // bytes as latin-1 keeps a non-ASCII username elsewhere in the payload
    // from corrupting the JSON and losing the id with it.
    const base64 = payload.replace(/-/g, '+').replace(/_/g, '/');
    const padded = base64.padEnd(base64.length + ((4 - (base64.length % 4)) % 4), '=');
    const bytes = Uint8Array.from(atob(padded), (character) => character.charCodeAt(0));
    const decoded: unknown = JSON.parse(new TextDecoder().decode(bytes));
    if (!decoded || typeof decoded !== 'object' || Array.isArray(decoded)) {
      return null;
    }
    claims = decoded as Record<string, unknown>;
  } catch {
    return null;
  }

  // An expired token describes a session that is about to be replaced,
  // possibly by a different user. Reporting under a stale identity is worse
  // than reporting under none.
  const expiresAt = claims.exp;
  if (typeof expiresAt === 'number' && expiresAt * 1000 <= nowMs) {
    return null;
  }

  // The server issues `sub` as the durable numeric user id. Insisting on that
  // exact shape rejects a malformed or tampered token, and simultaneously
  // guarantees the value satisfies GameAnalytics' own user-id validation,
  // which refuses anything empty or over 64 characters.
  const subject = claims.sub;
  return typeof subject === 'string' && /^\d{1,64}$/.test(subject) ? subject : null;
};
