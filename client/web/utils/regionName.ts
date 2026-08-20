import type { RegionMetadata } from '../types';
import { fetchRegionMetadata } from './regionPreference';

/**
 * Display name for a region id, e.g. `use1` -> `US East`.
 *
 * The roster frame carries only the id — the gateway knows which region it is,
 * not what that region is called — so the name comes from the same public
 * metadata the region selector uses. Fetched once per page and shared, since
 * several components want the same answer.
 */

let cached: Promise<RegionMetadata[]> | null = null;

function regionMetadata(): Promise<RegionMetadata[]> {
  if (!cached) {
    cached = fetchRegionMetadata().catch(() => {
      // A failed lookup must not poison the cache: the id fallback is fine for
      // this load, and the next caller gets a fresh attempt.
      cached = null;
      return [];
    });
  }
  return cached;
}

/** The id, upper-cased — honest and readable when metadata is unavailable. */
export function fallbackRegionName(regionId: string): string {
  return regionId.trim().toUpperCase();
}

export async function resolveRegionName(regionId: string): Promise<string> {
  if (!regionId.trim()) {
    return '';
  }
  const regions = await regionMetadata();
  const match = regions.find((region) => region.id === regionId);
  return match?.name?.trim() || fallbackRegionName(regionId);
}

/** Test seam: drop the memoized fetch. */
export function resetRegionNameCache(): void {
  cached = null;
}
