import { crazyGames } from './crazyGames.ts';

export interface GameStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

type GameStorageListener = (key: string) => void;
const storageListeners = new Set<GameStorageListener>();
const crazyGamesMemoryStorage = new Map<string, string | null>();

const publishStorageChange = (key: string): void => {
  for (const listener of storageListeners) {
    listener(key);
  }
};

export const subscribeGameStorage = (listener: GameStorageListener): (() => void) => {
  storageListeners.add(listener);
  return () => storageListeners.delete(listener);
};

const browserStorage = (kind: 'local' | 'session'): Storage | null => {
  if (typeof window === 'undefined') {
    return null;
  }
  try {
    return kind === 'session' ? window.sessionStorage : window.localStorage;
  } catch {
    return null;
  }
};

const fallbackGetItem = (key: string): string | null => {
  if (!crazyGames.getSnapshot().isCrazyGamesBuild) {
    return browserStorage('local')?.getItem(key) ?? null;
  }
  if (crazyGamesMemoryStorage.has(key)) {
    return crazyGamesMemoryStorage.get(key) ?? null;
  }
  const session = browserStorage('session');
  const current = session?.getItem(key) ?? null;
  if (current !== null) {
    crazyGamesMemoryStorage.set(key, current);
    return current;
  }
  // Migrate the earlier shared-tab preference mirror once. Canonical linked
  // preferences are applied by the backend exchange immediately afterward;
  // all live CG reads/writes remain tab-scoped from this point onward.
  const local = browserStorage('local');
  const legacy = local?.getItem(key) ?? null;
  if (legacy !== null) {
    crazyGamesMemoryStorage.set(key, legacy);
    try {
      if (session) {
        session.setItem(key, legacy);
      }
      local?.removeItem(key);
    } catch {
      try {
        local?.removeItem(key);
      } catch {
        // Memory is still authoritative for this page lifetime.
      }
    }
  }
  return legacy;
};

const fallbackSetItem = (key: string, value: string): void => {
  const isCrazyGamesBuild = crazyGames.getSnapshot().isCrazyGamesBuild;
  if (!isCrazyGamesBuild) {
    browserStorage('local')?.setItem(key, value);
    return;
  }
  // Memory is the live tab authority. Web Storage is only a best-effort
  // reload mirror, so blocked/quota-limited sessionStorage cannot suppress a
  // preference event or make a second script/account replace the live value.
  crazyGamesMemoryStorage.set(key, value);
  try {
    browserStorage('session')?.setItem(key, value);
  } catch {
    // Memory remains authoritative for this page lifetime.
  }
  try {
    browserStorage('local')?.removeItem(key);
  } catch {
    // Best-effort legacy cleanup.
  }
};

const fallbackRemoveItem = (key: string): void => {
  const isCrazyGamesBuild = crazyGames.getSnapshot().isCrazyGamesBuild;
  if (!isCrazyGamesBuild) {
    browserStorage('local')?.removeItem(key);
    return;
  }
  crazyGamesMemoryStorage.set(key, null);
  try {
    browserStorage('session')?.removeItem(key);
  } catch {
    // Memory removal is authoritative.
  }
  try {
    browserStorage('local')?.removeItem(key);
  } catch {
    // Best-effort legacy cleanup.
  }
};

/**
 * Preferences saved through this facade use CrazyGames cloud data when it is
 * explicitly enabled, a tab-scoped browser mirror in the portal build, and
 * ordinary LocalStorage elsewhere. Authentication tokens, reconnect state,
 * and server-authoritative progression intentionally do not use this facade.
 */
export const gameStorage: GameStorage = {
  getItem(key: string): string | null {
    const data = crazyGames.getDataModule();
    if (data) {
      try {
        return data.getItem(key);
      } catch (error) {
        console.warn(`CrazyGames data read failed for ${key}`, error);
      }
    }

    try {
      return fallbackGetItem(key);
    } catch {
      return null;
    }
  },

  setItem(key: string, value: string): void {
    const data = crazyGames.getDataModule();
    if (data) {
      try {
        data.setItem(key, value);
        publishStorageChange(key);
        return;
      } catch (error) {
        console.warn(`CrazyGames data write failed for ${key}`, error);
      }
    }

    try {
      fallbackSetItem(key, value);
      publishStorageChange(key);
    } catch {
      // Preferences are non-critical and must never stop gameplay.
    }
  },

  removeItem(key: string): void {
    const data = crazyGames.getDataModule();
    if (data) {
      try {
        data.removeItem(key);
        publishStorageChange(key);
        return;
      } catch (error) {
        console.warn(`CrazyGames data removal failed for ${key}`, error);
      }
    }

    try {
      fallbackRemoveItem(key);
      publishStorageChange(key);
    } catch {
      // Preferences are non-critical and must never stop gameplay.
    }
  },
};
