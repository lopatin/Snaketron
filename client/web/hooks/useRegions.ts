import { useState, useEffect, useCallback } from 'react';
import { Region } from '../types';
import {
  fetchRegionMetadata,
  loadRegionPreference,
  measureRegionPing,
  saveRegionPreference,
} from '../utils/regionPreference';

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

  // Initialize regions with metadata + ping + user counts
  const initializeRegions = useCallback(async () => {
    try {
      setIsLoading(true);
      setError(null);

      // 1. Fetch region metadata
      const metadata = await fetchRegionMetadata();

      // 2. Measure ping to all regions in parallel
      const pingPromises = metadata.map(async (region) => ({
        id: region.id,
        ping: await measureRegionPing(region.origin),
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
      if (savedPreference && savedPreference.regionId && regionsWithData.some(r => r.id === savedPreference.regionId)) {
        const preferredRegion = regionsWithData.find(r => r.id === savedPreference.regionId);
        if (preferredRegion) {
          const preferenceToPersist = {
            regionId: preferredRegion.id,
            wsUrl: preferredRegion.wsUrl,
            origin: preferredRegion.origin,
            timestamp: Date.now(),
          };
          saveRegionPreference(preferenceToPersist);
        }

        // Use saved preference if it exists and is still valid
        setSelectedRegionId(savedPreference.regionId);
      } else {
        // Auto-select best region based on ping
        const bestRegion = regionsWithData
          .filter(r => r.ping !== null)
          .sort((a, b) => (a.ping ?? Infinity) - (b.ping ?? Infinity))[0];

        if (bestRegion) {
          setSelectedRegionId(bestRegion.id);
          saveRegionPreference({
            regionId: bestRegion.id,
            wsUrl: bestRegion.wsUrl,
            origin: bestRegion.origin,
            timestamp: Date.now(),
          });
        } else if (regionsWithData.length > 0) {
          // Fallback to first region if no ping succeeded
          setSelectedRegionId(regionsWithData[0].id);
          saveRegionPreference({
            regionId: regionsWithData[0].id,
            wsUrl: regionsWithData[0].wsUrl,
            origin: regionsWithData[0].origin,
            timestamp: Date.now(),
          });
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to initialize regions');
    } finally {
      setIsLoading(false);
    }
  }, [fetchUserCounts]);

  // Manual region selection
  const selectRegion = useCallback((regionId: string) => {
    setSelectedRegionId(regionId);
    const region = regions.find(r => r.id === regionId);
    if (region) {
      saveRegionPreference({
        regionId,
        wsUrl: region.wsUrl,
        origin: region.origin,
        timestamp: Date.now(),
      });
    } else {
      // Preserve legacy behaviour with id only if region not found
      saveRegionPreference({
        regionId,
        timestamp: Date.now(),
      });
    }
  }, [regions]);

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
