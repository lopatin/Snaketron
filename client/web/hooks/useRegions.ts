import { useState, useEffect, useCallback } from 'react';
import { Region, RegionMetadata, RegionPreference } from '../types';

const REGION_PREFERENCE_KEY = 'snaketron_selected_region';

export interface UseRegionsReturn {
  regions: Region[];
  selectedRegion: Region | null;
  selectedRegionId: string | null;
  selectRegion: (regionId: string) => void;
  isLoading: boolean;
  error: string | null;
  refreshRegions: () => Promise<void>;
}

export interface UseRegionsOptions {
  isWebSocketConnected?: boolean;
  onMessage?: (type: string, handler: (message: any) => void) => () => void;
}

export function useRegions(options: UseRegionsOptions = {}): UseRegionsReturn {
  const { isWebSocketConnected = false, onMessage } = options;
  const [regions, setRegions] = useState<Region[]>([]);
  const [selectedRegionId, setSelectedRegionId] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Fetch regions from API
  const fetchRegions = useCallback(async (): Promise<RegionMetadata[]> => {
    const apiUrl = process.env.REACT_APP_API_URL || 'http://localhost:8080';
    const response = await fetch(`${apiUrl}/api/regions`);
    if (!response.ok) throw new Error('Failed to fetch regions');
    return response.json();
  }, []);

  // Measure ping to a region
  const measurePing = useCallback(async (origin: string): Promise<number | null> => {
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
  }, []);

  // Fetch user counts for all regions
  const fetchUserCounts = useCallback(async (): Promise<Record<string, number>> => {
    try {
      const apiUrl = process.env.REACT_APP_API_URL || 'http://localhost:8080';
      const response = await fetch(`${apiUrl}/api/regions/user-counts`);
      if (!response.ok) return {};
      return response.json();
    } catch (error) {
      console.error('Failed to fetch user counts:', error);
      return {};
    }
  }, []);

  // Save region preference to localStorage
  const saveRegionPreference = useCallback((regionId: string) => {
    const preference: RegionPreference = {
      regionId,
      timestamp: Date.now(),
    };
    localStorage.setItem(REGION_PREFERENCE_KEY, JSON.stringify(preference));
  }, []);

  // Load saved preference from localStorage
  const loadRegionPreference = useCallback((): string | null => {
    try {
      const saved = localStorage.getItem(REGION_PREFERENCE_KEY);
      if (saved) {
        const pref: RegionPreference = JSON.parse(saved);
        return pref.regionId;
      }
    } catch (error) {
      console.error('Failed to load region preference:', error);
    }
    return null;
  }, []);

  // Initialize regions with metadata + ping + user counts
  const initializeRegions = useCallback(async () => {
    try {
      setIsLoading(true);
      setError(null);

      // 1. Fetch region metadata
      const metadata = await fetchRegions();

      // 2. Measure ping to all regions in parallel
      const pingPromises = metadata.map(async (region) => ({
        id: region.id,
        ping: await measurePing(region.origin),
      }));
      const pings = await Promise.all(pingPromises);
      const pingMap = Object.fromEntries(pings.map(p => [p.id, p.ping]));

      // 3. Fetch user counts
      const userCounts = await fetchUserCounts();

      // 4. Combine data into Region objects
      const regionsWithData: Region[] = metadata.map(region => ({
        id: region.id,
        name: region.name,
        origin: region.origin,
        wsUrl: region.ws_url,  // Convert snake_case to camelCase
        ping: pingMap[region.id] ?? null,
        userCount: userCounts[region.id] ?? 0,
        isConnected: false,
      }));

      setRegions(regionsWithData);

      // 5. Auto-select region
      const savedPreference = loadRegionPreference();
      if (savedPreference && regionsWithData.some(r => r.id === savedPreference)) {
        // Use saved preference if it exists and is still valid
        setSelectedRegionId(savedPreference);
      } else {
        // Auto-select best region based on ping
        const bestRegion = regionsWithData
          .filter(r => r.ping !== null)
          .sort((a, b) => (a.ping ?? Infinity) - (b.ping ?? Infinity))[0];

        if (bestRegion) {
          setSelectedRegionId(bestRegion.id);
          saveRegionPreference(bestRegion.id);
        } else if (regionsWithData.length > 0) {
          // Fallback to first region if no ping succeeded
          setSelectedRegionId(regionsWithData[0].id);
          saveRegionPreference(regionsWithData[0].id);
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to initialize regions');
    } finally {
      setIsLoading(false);
    }
  }, [fetchRegions, measurePing, fetchUserCounts, loadRegionPreference, saveRegionPreference]);

  // Manual region selection
  const selectRegion = useCallback((regionId: string) => {
    setSelectedRegionId(regionId);
    saveRegionPreference(regionId);
  }, [saveRegionPreference]);

  // Subscribe to WebSocket user count updates
  useEffect(() => {
    if (!onMessage) return;

    const unsubscribe = onMessage('UserCountUpdate', (message) => {
      const regionCounts = message.data.region_counts;

      setRegions(prev => prev.map(region => ({
        ...region,
        userCount: regionCounts[region.id] ?? region.userCount,
      })));
    });

    return unsubscribe;
  }, [onMessage]);

  // Initialize on mount
  useEffect(() => {
    initializeRegions();
  }, [initializeRegions]);

  // Update isConnected status based on WebSocket connection and selected region
  useEffect(() => {
    if (!selectedRegionId) return;

    setRegions(prev => prev.map(region => ({
      ...region,
      isConnected: region.id === selectedRegionId && isWebSocketConnected,
    })));
  }, [selectedRegionId, isWebSocketConnected]);

  const selectedRegion = regions.find(r => r.id === selectedRegionId) ?? null;

  return {
    regions,
    selectedRegion,
    selectedRegionId,
    selectRegion,
    isLoading,
    error,
    refreshRegions: initializeRegions,
  };
}
