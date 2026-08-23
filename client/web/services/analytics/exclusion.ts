/**
 * Deciding whose sessions reach GameAnalytics.
 *
 * GameAnalytics counts an event the moment it arrives and offers no way to
 * retract it, so "don't count me" has to be settled before the SDK is even
 * loaded. Three independent signals feed that decision, because no single one
 * covers an operator playing their own game:
 *
 * - an explicit `?analytics=off`, which is remembered per browser and is the
 *   only mechanism that works on a phone, a VPN, or someone else's network;
 * - the deployment's excluded-address list, answered by the server because a
 *   browser cannot see its own public IP — this covers a fresh profile or an
 *   incognito window at home with no setup at all; and
 * - an administrator account, which the client already knows from `/auth/me`.
 *
 * Any one of them is enough. The decision is a pure function of the three so
 * that the precedence is pinned by tests rather than by call ordering.
 */

import type { AnalyticsConsent } from '../../types/generated/AnalyticsConsent';

export type ExclusionReason =
  /** No game key was compiled into this bundle, so analytics is inert. */
  | 'notConfigured'
  /** An embedded release package; see `ANALYTICS.md` for why. */
  | 'unsupportedDistribution'
  /** This browser was opted out with `?analytics=off`. */
  | 'localOverride'
  /** The server matched this caller against the excluded-address list. */
  | 'excludedAddress'
  /** Signed in as an operator of this deployment. */
  | 'operatorAccount';

export type AnalyticsDecision =
  | { report: true }
  | { report: false; reason: ExclusionReason };

const REPORT: AnalyticsDecision = { report: true };

/** The verdict the server gave for this caller's network, if we have one. */
export type AddressVerdict = 'excluded' | 'counted' | 'unknown';

export interface ExclusionInputs {
  /** False when no game key is configured for this build. */
  configured: boolean;
  /** False for the itch/CrazyGames packages. */
  supportedDistribution: boolean;
  /** The remembered `?analytics=off|on` choice for this browser. */
  localOverride: 'off' | 'on' | null;
  addressVerdict: AddressVerdict;
  /** True once `/auth/me` reports an administrator. */
  operatorAccount: boolean;
}

/**
 * Resolve the three signals into one decision.
 *
 * Exclusions are checked before `localOverride === 'on'` can re-enable
 * anything, so an explicit opt-in is a way to undo a previous `off` on this
 * browser — not a way to defeat the deployment's own address list.
 */
export const resolveAnalyticsDecision = (inputs: ExclusionInputs): AnalyticsDecision => {
  if (!inputs.configured) {
    return { report: false, reason: 'notConfigured' };
  }
  if (!inputs.supportedDistribution) {
    return { report: false, reason: 'unsupportedDistribution' };
  }
  if (inputs.operatorAccount) {
    return { report: false, reason: 'operatorAccount' };
  }
  if (inputs.addressVerdict === 'excluded') {
    return { report: false, reason: 'excludedAddress' };
  }
  if (inputs.localOverride === 'off') {
    return { report: false, reason: 'localOverride' };
  }
  return REPORT;
};

/**
 * Read `?analytics=off|on` from a URL.
 *
 * Both the query string and a hash route's own query are searched: embedded
 * builds route in the hash, and an operator pasting the switch onto a deep
 * link should not have to know which half of the URL owns it.
 */
export const readAnalyticsOverrideFromUrl = (
  search: string,
  hash: string,
): 'off' | 'on' | null => {
  const fromSearch = overrideFromQuery(search);
  if (fromSearch) {
    return fromSearch;
  }
  const hashQueryIndex = hash.indexOf('?');
  return hashQueryIndex === -1 ? null : overrideFromQuery(hash.slice(hashQueryIndex));
};

const overrideFromQuery = (query: string): 'off' | 'on' | null => {
  let value: string | null = null;
  try {
    value = new URLSearchParams(query.startsWith('?') ? query.slice(1) : query).get('analytics');
  } catch {
    return null;
  }
  if (value === null) {
    return null;
  }
  const normalized = value.trim().toLowerCase();
  if (normalized === 'off' || normalized === 'false' || normalized === '0') {
    return 'off';
  }
  if (normalized === 'on' || normalized === 'true' || normalized === '1') {
    return 'on';
  }
  return null;
};

/**
 * Translate a server consent response into a cacheable verdict.
 *
 * Any exclusion the server reports is honored, including a `reason` this
 * client is too old to recognize: a newer server that learns a second way to
 * exclude an operator must not be overridden by a stale bundle.
 */
export const addressVerdictFromConsent = (consent: AnalyticsConsent): AddressVerdict => (
  consent.excluded ? 'excluded' : 'counted'
);

export const OVERRIDE_STORAGE_KEY = 'snaketron:analytics:opt-out';
export const ADDRESS_VERDICT_STORAGE_KEY = 'snaketron:analytics:address';

/**
 * Web Storage is unavailable in some embedded and privacy configurations, and
 * a thrown quota or security error there must never take the game down with
 * it. Every access is best-effort.
 */
const readStorage = (key: string): string | null => {
  try {
    return window.localStorage.getItem(key);
  } catch {
    return null;
  }
};

const writeStorage = (key: string, value: string | null): void => {
  try {
    if (value === null) {
      window.localStorage.removeItem(key);
    } else {
      window.localStorage.setItem(key, value);
    }
  } catch {
    // An un-persisted preference still applies for this page session.
  }
};

export const loadStoredOverride = (): 'off' | 'on' | null => {
  const stored = readStorage(OVERRIDE_STORAGE_KEY);
  return stored === 'off' || stored === 'on' ? stored : null;
};

export const storeOverride = (override: 'off' | 'on' | null): void => {
  writeStorage(OVERRIDE_STORAGE_KEY, override);
};

/**
 * The last address verdict this browser was given.
 *
 * Caching it is what keeps an operator excluded when the consent request fails:
 * without it, every API hiccup would quietly fold their own play back into the
 * numbers. A player who has never been excluded caches `counted` and is
 * unaffected.
 */
export const loadCachedAddressVerdict = (): AddressVerdict => {
  const stored = readStorage(ADDRESS_VERDICT_STORAGE_KEY);
  return stored === 'excluded' || stored === 'counted' ? stored : 'unknown';
};

export const storeAddressVerdict = (verdict: AddressVerdict): void => {
  writeStorage(ADDRESS_VERDICT_STORAGE_KEY, verdict === 'unknown' ? null : verdict);
};
