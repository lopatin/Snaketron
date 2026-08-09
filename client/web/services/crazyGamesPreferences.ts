import type { CrazyGamesPreferences } from './api.ts';
import { gameStorage } from './gameStorage.ts';

export const CRAZY_GAMES_PREFERENCE_KEYS = new Set([
  'snaketron:tutorial-seen:v1',
  'lastLobbyPreferences',
  'snaketron:boost-input-mode:v1',
]);
const CRAZY_GAMES_PREFERENCE_OWNER_KEY = 'snaketron:crazygames:preferences-owner';
const MAX_TUTORIAL_KEYS = 128;
const MAX_TUTORIAL_KEY_LENGTH = 64;

const readJson = (key: string): unknown => {
  const raw = gameStorage.getItem(key);
  if (!raw) {
    return undefined;
  }
  try {
    return JSON.parse(raw);
  } catch {
    return undefined;
  }
};

const tutorialSeen = (raw: unknown): Record<string, boolean> | undefined => {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    return undefined;
  }
  const result: Record<string, boolean> = {};
  for (const [key, value] of Object.entries(raw)) {
    if (
      Object.keys(result).length < MAX_TUTORIAL_KEYS &&
      key.length > 0 &&
      key.length <= MAX_TUTORIAL_KEY_LENGTH &&
      !/[\u0000-\u001f\u007f]/.test(key) &&
      value === true
    ) {
      result[key] = true;
    }
  }
  return Object.keys(result).length > 0 ? result : undefined;
};

const lobbyPreferences = (
  raw: unknown,
): CrazyGamesPreferences['lobbyPreferences'] | undefined => {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    return undefined;
  }
  const record = raw as Record<string, unknown>;
  const rawModes = record.selectedModes ?? record.selected_modes;
  if (!Array.isArray(rawModes)) {
    return undefined;
  }
  const validModes = new Set(['solo', 'duel', '2v2', 'ffa']);
  const selectedModes = [...new Set(rawModes
    .filter((mode): mode is string => typeof mode === 'string')
    .map((mode) => mode.trim().toLowerCase())
    .filter((mode) => validModes.has(mode)))]
    .slice(0, 4);
  if (selectedModes.length === 0) {
    return undefined;
  }
  return {
    selectedModes,
    competitive: record.competitive === true || record.isCompetitive === true,
  };
};

export const readCrazyGamesPreferences = (): CrazyGamesPreferences => {
  const preferences: CrazyGamesPreferences = {};
  const seen = tutorialSeen(readJson('snaketron:tutorial-seen:v1'));
  const lobby = lobbyPreferences(readJson('lastLobbyPreferences'));
  const boost = gameStorage.getItem('snaketron:boost-input-mode:v1');

  if (seen) preferences.tutorialSeen = seen;
  if (lobby) preferences.lobbyPreferences = lobby;
  if (boost === 'hold' || boost === 'toggle') preferences.boostInputMode = boost;
  return preferences;
};

/** Apply the server's canonical preference snapshot before gameplay mounts. */
export const applyCrazyGamesPreferences = (
  preferences?: CrazyGamesPreferences,
  replaceMissing = false,
): void => {
  if (!preferences) {
    return;
  }
  if (preferences.tutorialSeen) {
    gameStorage.setItem(
      'snaketron:tutorial-seen:v1',
      JSON.stringify(preferences.tutorialSeen),
    );
  } else if (replaceMissing) {
    gameStorage.removeItem('snaketron:tutorial-seen:v1');
  }
  if (preferences.lobbyPreferences) {
    gameStorage.setItem(
      'lastLobbyPreferences',
      JSON.stringify(preferences.lobbyPreferences),
    );
  } else if (replaceMissing) {
    gameStorage.removeItem('lastLobbyPreferences');
  }
  if (preferences.boostInputMode === 'hold' || preferences.boostInputMode === 'toggle') {
    gameStorage.setItem('snaketron:boost-input-mode:v1', preferences.boostInputMode);
  } else if (replaceMissing) {
    gameStorage.removeItem('snaketron:boost-input-mode:v1');
  }
};

export const crazyGamesPreferenceOwner = (): number | null => {
  const raw = gameStorage.getItem(CRAZY_GAMES_PREFERENCE_OWNER_KEY);
  if (!raw) return null;
  const parsed = Number(raw);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : null;
};

export const markCrazyGamesPreferencesOwnedBy = (userId: number): void => {
  gameStorage.setItem(CRAZY_GAMES_PREFERENCE_OWNER_KEY, String(userId));
};

export const clearCrazyGamesPreferenceOwner = (): void => {
  gameStorage.removeItem(CRAZY_GAMES_PREFERENCE_OWNER_KEY);
};

/** Remove linked-account data before exposing tab-local guest settings. */
export const clearLinkedCrazyGamesPreferences = (force = false): void => {
  if (!force && crazyGamesPreferenceOwner() === null) {
    return;
  }
  applyCrazyGamesPreferences({}, true);
  clearCrazyGamesPreferenceOwner();
};
