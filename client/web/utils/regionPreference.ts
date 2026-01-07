import { RegionMetadata, RegionPreference } from '../types';

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8080';
export const REGION_PREFERENCE_KEY = 'snaketron_selected_region';

export interface RegionDetectionResult {
  metadata: RegionMetadata;
  ping: number | null;
  preference: RegionPreference;
}

export const saveRegionPreference = (preference: RegionPreference) => {
  try {
    localStorage.setItem(
      REGION_PREFERENCE_KEY,
      JSON.stringify({
        ...preference,
        timestamp: preference.timestamp ?? Date.now(),
      })
    );
  } catch (error) {
    console.error('Failed to save region preference:', error);
  }
};

export const loadRegionPreference = (): RegionPreference | null => {
  try {
    const saved = localStorage.getItem(REGION_PREFERENCE_KEY);
    if (!saved) {
      return null;
    }

    const parsed = JSON.parse(saved);

    if (typeof parsed === 'string') {
      // Legacy format stored only the region ID
      return {
        regionId: parsed,
        timestamp: Date.now(),
      };
    }

    if (parsed && typeof parsed === 'object' && parsed.regionId) {
      return parsed as RegionPreference;
    }
  } catch (error) {
    console.error('Failed to load region preference:', error);
  }

  return null;
};

export const fetchRegionMetadata = async (): Promise<RegionMetadata[]> => {
  const response = await fetch(`${API_BASE_URL}/api/regions`);
  if (!response.ok) {
    throw new Error('Failed to fetch regions');
  }
  return response.json();
};

export const measureRegionPing = async (origin: string): Promise<number | null> => {
  try {
    const start = performance.now();
    const response = await fetch(`${origin}/api/health`, {
      method: 'GET',
      cache: 'no-cache',
    });
    const end = performance.now();
    return response.ok ? Math.round(end - start) : null;
  } catch (error) {
    console.error(`Failed to ping ${origin}:`, error);
    return null;
  }
};

export const detectBestRegion = async (): Promise<RegionDetectionResult | null> => {
  try {
    const metadata = await fetchRegionMetadata();
    if (metadata.length === 0) {
      return null;
    }

    const pingEntries = await Promise.all(
      metadata.map(async (region) => ({
        region,
        ping: await measureRegionPing(region.origin),
      }))
    );

    const candidates = pingEntries
      .filter(entry => entry.ping !== null)
      .sort((a, b) => (a.ping ?? Infinity) - (b.ping ?? Infinity));

    const best = candidates[0] ?? pingEntries[0];
    if (!best) {
      return null;
    }

    const preference: RegionPreference = {
      regionId: best.region.id,
      wsUrl: best.region.ws_url,
      origin: best.region.origin,
      timestamp: Date.now(),
    };

    return {
      metadata: best.region,
      ping: best.ping,
      preference,
    };
  } catch (error) {
    console.error('Failed to detect best region:', error);
    return null;
  }
};
