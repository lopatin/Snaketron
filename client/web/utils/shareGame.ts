/**
 * Share-link construction for finished and in-progress matches.
 *
 * Every share URL points at `/g/:gameId` on the canonical site. That path is
 * served two ways on purpose: the single-page app renders it for people, and
 * the API server renders a crawlable HTML document with Open Graph metadata at
 * the same path for machines. Keeping one path shape means a `/g/*` CDN
 * behaviour is all that stands between a shared link and a real link preview.
 *
 * Pure functions only — the repo tests view logic by extracting it here and
 * asserting on plain objects, with no DOM in the loop.
 */

/** Where shared links live. Embedded builds sit on a portal's static host, so
 * a link to their own origin would strand whoever opened it. */
const CANONICAL_SITE_ORIGIN = 'https://snaketron.io';

export interface ShareTarget {
  /** Stable id, also used as the React key and the test id suffix. */
  id: 'x' | 'facebook' | 'reddit' | 'whatsapp';
  label: string;
  /** Fully-formed URL to open in a new tab. */
  href: string;
}

export interface ShareContent {
  url: string;
  title: string;
  text: string;
}

export interface BuildShareUrlOptions {
  gameId: number | string | null | undefined;
  /** `window.location.origin`, or undefined when there is no window. */
  origin?: string | null;
  /** True for the itch.io and CrazyGames bundles. */
  isEmbeddedBuild?: boolean;
}

/**
 * The permanent public address of one match, or null when there is no match to
 * point at yet.
 */
export function buildGameShareUrl({
  gameId,
  origin,
  isEmbeddedBuild = false,
}: BuildShareUrlOptions): string | null {
  const id = normalizeGameId(gameId);
  if (id === null) {
    return null;
  }
  const base = resolveShareOrigin(origin, isEmbeddedBuild);
  return `${base}/g/${id}`;
}

/** Only a decimal id addresses a match; anything else is not shareable. */
function normalizeGameId(gameId: number | string | null | undefined): string | null {
  if (gameId === null || gameId === undefined) {
    return null;
  }
  const raw = typeof gameId === 'number' ? String(gameId) : gameId.trim();
  if (!/^\d+$/.test(raw)) {
    return null;
  }
  const parsed = Number(raw);
  if (!Number.isSafeInteger(parsed) || parsed <= 0 || parsed > 0xffff_ffff) {
    return null;
  }
  return String(parsed);
}

function resolveShareOrigin(origin: string | null | undefined, isEmbeddedBuild: boolean): string {
  if (isEmbeddedBuild) {
    return CANONICAL_SITE_ORIGIN;
  }
  const trimmed = (origin ?? '').trim().replace(/\/+$/, '');
  if (!/^https?:\/\//.test(trimmed)) {
    return CANONICAL_SITE_ORIGIN;
  }
  // A localhost origin is genuinely useful while developing and genuinely
  // useless to a recipient — but silently rewriting it to production would
  // hand a developer a link to a match their local server never played.
  return trimmed;
}

/**
 * The sentence that accompanies a shared link. Falls back to something
 * truthful when the match has no headline yet (a share pressed mid-game).
 */
export function buildShareContent(url: string, headline?: string | null): ShareContent {
  const text = headline?.trim() ? headline.trim() : 'Watch this Snaketron match.';
  return { url, title: 'Snaketron', text };
}

/**
 * Web-intent links for the networks worth one click. Each is a plain URL so it
 * works with a normal anchor — no SDKs, no third-party script, nothing to
 * block. Ordered by how much traffic a game link actually gets from each.
 */
export function buildShareTargets(content: ShareContent): ShareTarget[] {
  const url = encodeURIComponent(content.url);
  const text = encodeURIComponent(content.text);
  return [
    {
      id: 'x',
      label: 'X',
      href: `https://twitter.com/intent/tweet?url=${url}&text=${text}`,
    },
    {
      id: 'reddit',
      label: 'Reddit',
      href: `https://www.reddit.com/submit?url=${url}&title=${text}`,
    },
    {
      id: 'facebook',
      label: 'Facebook',
      href: `https://www.facebook.com/sharer/sharer.php?u=${url}`,
    },
    {
      id: 'whatsapp',
      label: 'WhatsApp',
      href: `https://api.whatsapp.com/send?text=${encodeURIComponent(`${content.text} ${content.url}`)}`,
    },
  ];
}

/**
 * Whether the browser can hand this off to the OS share sheet. Checked as a
 * capability rather than assumed: `navigator.share` throws outside a secure
 * context and is absent on most desktops.
 */
export function canUseNativeShare(nav: Navigator | undefined = typeof navigator !== 'undefined' ? navigator : undefined): boolean {
  return typeof nav?.share === 'function';
}
