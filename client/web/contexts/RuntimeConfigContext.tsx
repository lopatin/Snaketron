import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import { api } from '../services/api';
import type { PublicRuntimeConfig, RuntimeConfig, RuntimeConfigRecord } from '../types';
import { crazyGames } from '../services/crazyGames';
import { shouldApplyRuntimeConfigResponse } from '../utils/runtimeConfigOrdering';

export const SAFE_RUNTIME_CONFIG: RuntimeConfig = {
  announcement: {
    enabled: false,
    message: '',
  },
  ads: {
    postMatchEnabled: false,
    minimumIntervalMinutes: 10,
  },
  history: {
    snapshotRetentionDays: 30,
    summaryRetentionDays: 365,
  },
};

interface RuntimeConfigContextValue {
  config: RuntimeConfig;
  record: RuntimeConfigRecord | PublicRuntimeConfig | null;
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
  applyRecord: (record: RuntimeConfigRecord | PublicRuntimeConfig) => void;
}

const RuntimeConfigContext = createContext<RuntimeConfigContextValue | null>(null);

const messageFromError = (error: unknown): string => (
  error instanceof Error ? error.message : 'Runtime configuration is unavailable'
);

export const RuntimeConfigProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [record, setRecord] = useState<RuntimeConfigRecord | PublicRuntimeConfig | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const hasLoadedConfig = useRef(false);
  const appliedVersion = useRef(-1);
  const refreshSequence = useRef(0);

  const applyRecord = useCallback((nextRecord: RuntimeConfigRecord | PublicRuntimeConfig) => {
    if (nextRecord.version < appliedVersion.current) {
      return;
    }
    appliedVersion.current = nextRecord.version;
    setRecord(nextRecord);
    setError(null);
    hasLoadedConfig.current = true;
    crazyGames.configureRuntimeAds(
      'config' in nextRecord ? nextRecord.config.ads : nextRecord.ads,
    );
  }, []);

  const refresh = useCallback(async () => {
    const sequence = ++refreshSequence.current;
    const versionAtStart = appliedVersion.current;
    if (!hasLoadedConfig.current) {
      setLoading(true);
    }
    try {
      const nextRecord = await api.getRuntimeConfig();
      // Once a newer refresh has failed closed, an older response carrying
      // the already-known version must not re-authorize ads. Still accept a
      // genuinely newer durable version regardless of request ordering.
      if (shouldApplyRuntimeConfigResponse({
        requestSequence: sequence,
        latestRequestSequence: refreshSequence.current,
        responseVersion: nextRecord.version,
        appliedVersion: appliedVersion.current,
      })) {
        applyRecord(nextRecord);
      }
    } catch (nextError) {
      // Keep the last-good announcement visible, but treat every failed
      // latest refresh as loss of ad authorization. Ignore an obsolete
      // failure if another request or an admin save already applied a newer
      // configuration while this request was in flight.
      if (
        sequence === refreshSequence.current
        && appliedVersion.current <= versionAtStart
      ) {
        crazyGames.configureRuntimeAds(SAFE_RUNTIME_CONFIG.ads);
        if (!hasLoadedConfig.current) {
          setError(messageFromError(nextError));
        }
      }
    } finally {
      if (sequence === refreshSequence.current) {
        setLoading(false);
      }
    }
  }, [applyRecord]);

  useEffect(() => {
    void refresh();
    const refreshInterval = window.setInterval(() => void refresh(), 60_000);
    const handleVisibilityChange = () => {
      if (document.visibilityState === 'visible') {
        void refresh();
      }
    };
    document.addEventListener('visibilitychange', handleVisibilityChange);
    return () => {
      window.clearInterval(refreshInterval);
      document.removeEventListener('visibilitychange', handleVisibilityChange);
    };
  }, [refresh]);

  const config = useMemo<RuntimeConfig>(() => {
    if (!record) return SAFE_RUNTIME_CONFIG;
    if ('config' in record) return record.config;
    return {
      ...SAFE_RUNTIME_CONFIG,
      announcement: record.announcement,
      ads: record.ads,
    };
  }, [record]);

  const value = useMemo<RuntimeConfigContextValue>(() => ({
    config,
    record,
    loading,
    error,
    refresh,
    applyRecord,
  }), [applyRecord, config, error, loading, record, refresh]);

  return (
    <RuntimeConfigContext.Provider value={value}>
      {children}
    </RuntimeConfigContext.Provider>
  );
};

export const useRuntimeConfig = (): RuntimeConfigContextValue => {
  const context = useContext(RuntimeConfigContext);
  if (!context) {
    throw new Error('useRuntimeConfig must be used within RuntimeConfigProvider');
  }
  return context;
};
