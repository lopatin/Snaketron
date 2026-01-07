import React, { createContext, useContext, useState, useEffect } from 'react';

interface LatencySettings {
  enabled: boolean;
  sendDelayMs: number;
  receiveDelayMs: number;
}

interface LatencyContextType {
  settings: LatencySettings;
  updateSettings: (settings: Partial<LatencySettings>) => void;
}

const defaultSettings: LatencySettings = {
  enabled: false,
  sendDelayMs: 0,
  receiveDelayMs: 0,
};

const LatencyContext = createContext<LatencyContextType | null>(null);

export const useLatency = (): LatencyContextType => {
  const context = useContext(LatencyContext);
  if (!context) {
    throw new Error('useLatency must be used within LatencyProvider');
  }
  return context;
};

export const LatencyProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [settings, setSettings] = useState<LatencySettings>(() => {
    // Load settings from localStorage
    const stored = localStorage.getItem('latencySettings');
    if (stored) {
      try {
        return { ...defaultSettings, ...JSON.parse(stored) };
      } catch (e) {
        console.error('Failed to parse latency settings:', e);
      }
    }
    return defaultSettings;
  });

  // Save settings to localStorage whenever they change
  useEffect(() => {
    localStorage.setItem('latencySettings', JSON.stringify(settings));
  }, [settings]);

  const updateSettings = (updates: Partial<LatencySettings>) => {
    setSettings(prev => ({ ...prev, ...updates }));
  };

  return (
    <LatencyContext.Provider value={{ settings, updateSettings }}>
      {children}
    </LatencyContext.Provider>
  );
};