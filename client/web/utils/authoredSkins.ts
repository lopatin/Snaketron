import { getWasm } from '../wasm';

/**
 * Fetching the player-authored skins a match is wearing.
 *
 * Match state carries a content reference per player. Built-ins resolve from
 * the compiled catalogue; anything shaped like `sha256:<hex>` is a document
 * this client has to go and get. Until it arrives that snake paints classic,
 * which is the standing fallback rule doing real work rather than a special
 * case — a frame must never wait on, or fail because of, cosmetics.
 */

const CONTENT_REF_PREFIX = 'sha256:';
const CONTENT_REF_LENGTH = CONTENT_REF_PREFIX.length + 64;

/** Shape only. Whether the document exists is the server's answer to give. */
export const isContentRef = (value: unknown): value is string =>
  typeof value === 'string' &&
  value.length === CONTENT_REF_LENGTH &&
  value.startsWith(CONTENT_REF_PREFIX) &&
  /^[0-9a-f]{64}$/.test(value.slice(CONTENT_REF_PREFIX.length));

/**
 * References this client has already tried and failed to resolve.
 *
 * A reference can fail permanently — the skin was disabled (410), or was never
 * public (404) — and retrying those every time state arrives would be a request
 * per tick for the rest of the match. Failure is remembered; the snake wears
 * classic, which is exactly what it would wear anyway.
 */
const abandoned = new Set<string>();

/** References currently being fetched, so eight players in one skin fetch once. */
const inFlight = new Map<string, Promise<void>>();

const baseUrl = (): string =>
  (process.env.REACT_APP_API_URL ?? 'http://localhost:8080').replace(/\/+$/, '');

/**
 * Fetch and register one authored skin.
 *
 * Registration verifies in Rust that the bytes hash to the reference, so a
 * response that has been substituted in transit is refused rather than drawn.
 */
const fetchAndRegister = async (contentRef: string): Promise<void> => {
  const response = await fetch(`${baseUrl()}/api/skins/by-ref/${contentRef}`);
  if (!response.ok) {
    // 410 means moderated away, 404 means it was never ours to see. Both are
    // permanent, and both mean classic.
    abandoned.add(contentRef);
    return;
  }

  const document = await response.text();
  const wasm = getWasm();
  if (!wasm) {
    return;
  }
  try {
    wasm.registerAuthoredSkin(contentRef, document);
  } catch (cause) {
    // A document that will not compile costs its wearer the classic look and
    // nothing else. Worth a log — it means a skin passed server validation and
    // then failed here, which is a real disagreement — but never worth a throw.
    console.warn(`Could not compile the skin ${contentRef}:`, cause);
    abandoned.add(contentRef);
  }
};

/**
 * Make sure every authored skin in this state is on its way.
 *
 * Safe to call on every state update: everything already registered, already
 * being fetched, or already known to be unavailable is skipped, so the steady
 * state costs one set lookup per player.
 */
export const ensureAuthoredSkins = (
  skins: Record<number, string | undefined> | undefined,
): Promise<void> => {
  if (!skins) {
    return Promise.resolve();
  }
  const wasm = getWasm();
  if (!wasm) {
    return Promise.resolve();
  }
  const started: Array<Promise<void>> = [];

  for (const reference of Object.values(skins)) {
    if (
      !isContentRef(reference) ||
      abandoned.has(reference) ||
      inFlight.has(reference) ||
      wasm.authoredSkinIsRegistered(reference)
    ) {
      continue;
    }

    const request = fetchAndRegister(reference)
      .catch((cause) => {
        // A transient network failure is not remembered: the next state update
        // tries again, and the snake wears classic until it works.
        console.debug(`Could not fetch the skin ${reference}:`, cause);
      })
      .finally(() => {
        inFlight.delete(reference);
      });
    inFlight.set(reference, request);
    started.push(request);
  }

  // Resolves when everything this call started has landed, so a surface that
  // paints once — a browse row, a review tile — knows when to paint again.
  return Promise.all(started).then(() => undefined);
};

/** Test seam: forget what this module has learned. */
export const resetAuthoredSkinCache = (): void => {
  abandoned.clear();
  inFlight.clear();
};
