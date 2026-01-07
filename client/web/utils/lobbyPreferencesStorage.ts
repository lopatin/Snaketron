import { LobbyGameMode, LobbyPreferences } from '../types';

const LAST_LOBBY_PREFERENCES_KEY = 'lastLobbyPreferences';
const LEGACY_LAST_MODES_KEY = 'lastSelectedGameModes';
const VALID_GAME_MODES: LobbyGameMode[] = ['duel', '2v2', 'solo', 'ffa'];

const sanitizeModes = (raw: unknown): LobbyGameMode[] => {
  if (!Array.isArray(raw)) {
    return [];
  }

  const seen = new Set<LobbyGameMode>();
  const sanitized: LobbyGameMode[] = [];

  for (const value of raw) {
    if (typeof value !== 'string') {
      continue;
    }

    const normalized = value.trim().toLowerCase();
    if (!VALID_GAME_MODES.includes(normalized as LobbyGameMode)) {
      continue;
    }

    if (seen.has(normalized as LobbyGameMode)) {
      continue;
    }

    seen.add(normalized as LobbyGameMode);
    sanitized.push(normalized as LobbyGameMode);
  }

  return sanitized;
};

export const DEFAULT_LOBBY_PREFERENCES: LobbyPreferences = {
  selectedModes: ['duel'],
  competitive: false,
};

const clonePreferences = (preferences: LobbyPreferences): LobbyPreferences => ({
  selectedModes: [...preferences.selectedModes],
  competitive: Boolean(preferences.competitive),
});

export const loadStoredLobbyPreferences = (): LobbyPreferences | null => {
  if (typeof window === 'undefined' || !window?.localStorage) {
    return null;
  }

  try {
    const raw = window.localStorage.getItem(LAST_LOBBY_PREFERENCES_KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      const modes = sanitizeModes(parsed?.selectedModes ?? parsed?.selected_modes);
      const competitive = Boolean(parsed?.competitive ?? parsed?.isCompetitive);

      if (modes.length === 0) {
        return {
          selectedModes: [...DEFAULT_LOBBY_PREFERENCES.selectedModes],
          competitive,
        };
      }

      return {
        selectedModes: modes,
        competitive,
      };
    }

    const legacy = window.localStorage.getItem(LEGACY_LAST_MODES_KEY);
    if (legacy) {
      try {
        const parsedLegacy = JSON.parse(legacy);
        const modes = sanitizeModes(parsedLegacy);
        if (modes.length > 0) {
          return {
            selectedModes: modes,
            competitive: false,
          };
        }
      } catch (error) {
        console.warn('Failed to parse legacy lobby modes from localStorage', error);
      }
    }
  } catch (error) {
    console.warn('Failed to load stored lobby preferences', error);
  }

  return null;
};

export const persistStoredLobbyPreferences = (preferences: LobbyPreferences): void => {
  if (typeof window === 'undefined' || !window?.localStorage) {
    return;
  }

  try {
    const sanitized = clonePreferences(preferences);
    if (sanitized.selectedModes.length === 0) {
      sanitized.selectedModes = [...DEFAULT_LOBBY_PREFERENCES.selectedModes];
    }
    window.localStorage.setItem(LAST_LOBBY_PREFERENCES_KEY, JSON.stringify(sanitized));
  } catch (error) {
    console.warn('Failed to persist lobby preferences', error);
  }
};

export const sanitizeClientLobbyPreferences = (
  preferences?: LobbyPreferences | null
): LobbyPreferences | null => {
  if (!preferences) {
    return null;
  }

  const sanitizedModes = sanitizeModes(preferences.selectedModes);
  if (sanitizedModes.length === 0) {
    sanitizedModes.push(...DEFAULT_LOBBY_PREFERENCES.selectedModes);
  }

  return {
    selectedModes: sanitizedModes,
    competitive: Boolean(preferences.competitive),
  };
};
