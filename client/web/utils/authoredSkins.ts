import { getWasm } from '../wasm';
import { api } from '../services/api';

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

/**
 * Who the `abandoned` set was learned as.
 *
 * A 404 means "not yours to see", which is a fact about the viewer and not
 * about the reference — and the viewer changes without the page reloading, at
 * exactly the moment signing in turns "not yours" into "yours". Forgetting the
 * set when the identity changes keeps the never-retry property for a stable
 * session while letting a sign-in recover, instead of leaving the author
 * looking at their own skin painted classic.
 */
let abandonedFor: string | null = null;

const forgetIfViewerChanged = (token: string | null): void => {
  if (token !== abandonedFor) {
    abandoned.clear();
    abandonedFor = token;
  }
};

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
  // Signed, when there is someone to sign as. A skin the viewer has made but
  // not published is served only to them, so an anonymous request for it comes
  // back 404 and the author watches their own skin paint classic.
  const token = api.getAuthToken();
  const response = await fetch(`${baseUrl()}/api/skins/by-ref/${contentRef}`, {
    headers: token ? { Authorization: `Bearer ${token}` } : undefined,
  });
  if (!response.ok) {
    // 410 means moderated away: permanent for everyone, so stop asking.
    //
    // 404 is only permanent for *this* viewer, and who the viewer is can
    // change without the page reloading — signing in is exactly the event that
    // turns "never yours to see" into "yours". Remembering it against the
    // reference alone is what made a skin stay classic for the rest of the
    // session after one unauthenticated miss.
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
  forgetIfViewerChanged(api.getAuthToken());
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
  abandonedFor = null;
  inFlight.clear();
};
