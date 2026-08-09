import { crazyGames } from './crazyGames.ts';

export interface GameStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

const browserStorage = (): Storage | null => {
  if (typeof window === 'undefined') {
    return null;
  }
  try {
    return window.localStorage;
  } catch {
    return null;
  }
};

/**
 * Preferences saved through this facade use CrazyGames cloud data in the
 * portal build and ordinary LocalStorage elsewhere. Authentication tokens,
 * reconnect state, and server-authoritative progression intentionally do not
 * use this facade.
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
      return browserStorage()?.getItem(key) ?? null;
    } catch {
      return null;
    }
  },

  setItem(key: string, value: string): void {
    const data = crazyGames.getDataModule();
    if (data) {
      try {
        data.setItem(key, value);
        return;
      } catch (error) {
        console.warn(`CrazyGames data write failed for ${key}`, error);
      }
    }

    try {
      browserStorage()?.setItem(key, value);
    } catch {
      // Preferences are non-critical and must never stop gameplay.
    }
  },

  removeItem(key: string): void {
    const data = crazyGames.getDataModule();
    if (data) {
      try {
        data.removeItem(key);
        return;
      } catch (error) {
        console.warn(`CrazyGames data removal failed for ${key}`, error);
      }
    }

    try {
      browserStorage()?.removeItem(key);
    } catch {
      // Preferences are non-critical and must never stop gameplay.
    }
  },
};
