/**
 * Stable pseudonymous browser identifier for product analytics.
 *
 * This exists because nothing else in the client is stable across sessions.
 * The JWT is the only thing persisted today, it expires after 24 hours with no
 * refresh, and on a 401 `AuthContext` clears it — after which the next visit
 * mints a brand-new guest `user_id`. That makes a returning guest structurally
 * indistinguishable from a new one, so retention and top-of-funnel are
 * unmeasurable without a separate identifier.
 *
 * It is NEVER used for authentication or authorization. The server treats it as
 * an untrusted, advisory hint and validates it before use. Losing it (private
 * browsing, cleared storage, a different browser) degrades analytics and
 * nothing else.
 */
export const ANON_ID_STORAGE_KEY = 'snaketron_anon_id';

/** Canonical lowercase hyphenated UUID. The server enforces the same shape. */
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export const isValidAnonId = (value: unknown): value is string =>
  typeof value === 'string' && UUID_PATTERN.test(value);

/**
 * `crypto.randomUUID` is unavailable on insecure origins and in older
 * browsers. Fall back to `getRandomValues`, and only then to `Math.random`,
 * so a missing id never throws — this identifier is not security-sensitive,
 * and an analytics id is not worth breaking a page load over.
 */
const generateUuidV4 = (): string => {
  const cryptoApi: Crypto | undefined =
    typeof globalThis !== 'undefined' ? globalThis.crypto : undefined;

  if (cryptoApi && typeof cryptoApi.randomUUID === 'function') {
    return cryptoApi.randomUUID();
  }

  const bytes = new Uint8Array(16);
  if (cryptoApi && typeof cryptoApi.getRandomValues === 'function') {
    cryptoApi.getRandomValues(bytes);
  } else {
    for (let index = 0; index < bytes.length; index += 1) {
      bytes[index] = Math.floor(Math.random() * 256);
    }
  }
  // Version 4, RFC 4122 variant.
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;

  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0'));
  return [
    hex.slice(0, 4).join(''),
    hex.slice(4, 6).join(''),
    hex.slice(6, 8).join(''),
    hex.slice(8, 10).join(''),
    hex.slice(10, 16).join(''),
  ].join('-');
};

/** In-process cache so a blocked or slow storage backend is read at most once. */
let cached: string | null = null;

/**
 * Returns the persisted anonymous id, creating and storing one on first call.
 *
 * Every storage access is guarded: `localStorage` throws on access in some
 * privacy modes rather than returning null. When storage is unavailable the id
 * still works for the lifetime of the page, it simply does not survive a
 * reload.
 */
export const getOrCreateAnonId = (): string => {
  if (cached !== null) {
    return cached;
  }

  let stored: string | null = null;
  try {
    stored = window.localStorage.getItem(ANON_ID_STORAGE_KEY);
  } catch {
    stored = null;
  }

  if (isValidAnonId(stored)) {
    cached = stored;
    return cached;
  }

  const created = generateUuidV4();
  try {
    window.localStorage.setItem(ANON_ID_STORAGE_KEY, created);
  } catch {
    // Non-fatal: the id lives for this page only.
  }
  cached = created;
  return created;
};

/** Test seam. Not used in production code. */
export const resetAnonIdCacheForTests = (): void => {
  cached = null;
};
