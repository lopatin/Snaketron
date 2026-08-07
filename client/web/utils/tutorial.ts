import type { GameState, GameType, QueueMode } from '../types';

/**
 * One tutorial exists per (game mode x ranked/unranked) combination. There are
 * four modes reachable from the home screen — duel, 2v2, solo, ffa — and a
 * single COMPETITIVE toggle, so eight combinations in total.
 *
 * The player may queue for several modes at once, so which one they end up in
 * is not known until the match exists. Everything here therefore keys off the
 * authoritative `GameState`, never off what was selected at queue time.
 */
export type TutorialMode = 'duel' | '2v2' | 'ffa' | 'solo';
export type TutorialKey = `${TutorialMode}:${'ranked' | 'casual'}`;

export interface TutorialBullet {
  /** Scene id understood by `renderTutorialScene` in the WASM renderer. */
  scene: string;
  text: string;
}

export interface TutorialContent {
  key: TutorialKey;
  title: string;
  /** Short qualifier under the title, e.g. "Ranked". */
  kicker: string;
  bullets: [TutorialBullet, TutorialBullet, TutorialBullet];
}

const MODE_LABELS: Record<TutorialMode, string> = {
  duel: 'Duel',
  '2v2': '2v2',
  ffa: 'Free for all',
  solo: 'Solo run',
};

/**
 * Resolve the tutorial mode from authoritative state. Custom games are not
 * reachable from matchmaking and have no fixed rule set, so they get no
 * tutorial rather than a wrong one.
 */
export const tutorialModeForGameType = (gameType: GameType): TutorialMode | null => {
  if (gameType === 'Solo') {
    return 'solo';
  }
  if (typeof gameType !== 'object' || gameType === null) {
    return null;
  }
  if ('TeamMatch' in gameType) {
    return gameType.TeamMatch.per_team === 1 ? 'duel' : '2v2';
  }
  if ('FreeForAll' in gameType) {
    return 'ffa';
  }
  return null;
};

export const tutorialKey = (mode: TutorialMode, queueMode: QueueMode): TutorialKey =>
  `${mode}:${queueMode === 'Competitive' ? 'ranked' : 'casual'}`;

/**
 * Bullet copy is written from the engine's actual rules, not from the design
 * specs — several of which describe mechanics that were never implemented.
 * Load-bearing facts, with their sources:
 *
 * - Team matches end on a 90-second clock and the higher team score wins; a
 *   tie is a draw. There is no score target in any mode
 *   (`common/src/game_state.rs`, the `TeamMatch` completion branch).
 * - Banking is `carried_segments / 2`, so one food eaten is one team point,
 *   and the snake respawns at starting length afterwards.
 * - Entering the *enemy* end zone kills you; it is not a scoring move.
 * - Food adds two body segments, and personal score is length minus two.
 * - Boost exists only in duel and 2v2. Space is the key; the default binding
 *   is hold-to-boost.
 * - Solo and FFA have no time limit and end only when every snake is dead.
 * - Competitive changes which MMR pool a result is written to, and nothing
 *   about the rules — except in Solo, where it changes nothing at all, so the
 *   Solo copy never claims a rank is at stake.
 */
const teamBullets = (
  mode: 'duel' | '2v2',
  ranked: boolean,
): [TutorialBullet, TutorialBullet, TutorialBullet] => [
  {
    scene: 'team-carry',
    text:
      mode === 'duel'
        ? 'Eat food out in the field, then carry it home through your own gate to bank the points.'
        : 'Eat food out in the field, then carry it home through your gate. Your partner banks into the same base.',
  },
  {
    scene: 'team-boost',
    text: 'Drive over NOS canisters to fill your tank, then hold Space for a burst of speed.',
  },
  {
    scene: 'team-danger',
    text: ranked
      ? 'Never enter the enemy base — it kills you. Highest team score after 90 seconds wins, and your rank moves with it.'
      : 'Never enter the enemy base — it kills you. Highest team score after 90 seconds wins.',
  },
];

const ffaBullets = (ranked: boolean): [TutorialBullet, TutorialBullet, TutorialBullet] => [
  {
    scene: 'ffa-food',
    text: 'Eat food to grow. Every bite is worth two points.',
  },
  {
    scene: 'ffa-crash',
    text: 'One life. Hitting a wall, a rival, or your own tail ends your run for good.',
  },
  {
    scene: 'ffa-rivals',
    text: ranked
      ? 'The match ends when the last snake falls. Highest score takes it, and your rank moves with it.'
      : 'The match ends when the last snake falls. Highest score takes it.',
  },
];

const soloBullets = (): [TutorialBullet, TutorialBullet, TutorialBullet] => [
  {
    scene: 'solo-food',
    text: 'Eat food to grow. Every bite is worth two points.',
  },
  {
    scene: 'solo-steer',
    text: 'Steer with the arrow keys. You can turn left or right, never straight back on yourself.',
  },
  {
    scene: 'solo-run',
    text: 'No clock and no rivals — the run only ends when you crash. Beat your own best.',
  },
];

export const tutorialContent = (
  mode: TutorialMode,
  queueMode: QueueMode,
): TutorialContent => {
  const ranked = queueMode === 'Competitive';
  const key = tutorialKey(mode, queueMode);
  const bullets =
    mode === 'solo'
      ? soloBullets()
      : mode === 'ffa'
        ? ffaBullets(ranked)
        : teamBullets(mode, ranked);

  return {
    key,
    title: MODE_LABELS[mode],
    // Solo never touches MMR in either queue, so calling it "Ranked" would be
    // a lie the player could check.
    kicker: ranked && mode !== 'solo' ? 'Ranked' : 'Casual',
    bullets,
  };
};

export const tutorialContentForGame = (
  gameState: Pick<GameState, 'game_type' | 'queue_mode'>,
): TutorialContent | null => {
  const mode = tutorialModeForGameType(gameState.game_type);
  return mode === null ? null : tutorialContent(mode, gameState.queue_mode);
};

/**
 * Mirrors the `SCENES` registry in `client/src/tutorial.rs`. Kept here so a
 * renamed scene is caught by the unit test rather than by a blank illustration
 * in production; `TutorialSceneCanvas` also logs loudly if the WASM module
 * rejects an id at runtime.
 */
export const TUTORIAL_SCENE_IDS = [
  'team-carry',
  'team-boost',
  'team-danger',
  'ffa-food',
  'ffa-crash',
  'ffa-rivals',
  'solo-food',
  'solo-steer',
  'solo-run',
] as const;

/** Every combination, for exhaustiveness tests and for the help menu. */
export const ALL_TUTORIAL_KEYS: TutorialKey[] = (
  ['duel', '2v2', 'ffa', 'solo'] as TutorialMode[]
).flatMap((mode) => [tutorialKey(mode, 'Quickmatch'), tutorialKey(mode, 'Competitive')]);

const SEEN_STORAGE_KEY = 'snaketron:tutorial-seen:v1';

const readSeen = (): Record<string, boolean> => {
  try {
    const raw = window.localStorage.getItem(SEEN_STORAGE_KEY);
    if (!raw) {
      return {};
    }
    const parsed: unknown = JSON.parse(raw);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed)
      ? (parsed as Record<string, boolean>)
      : {};
  } catch {
    // Private browsing, a disabled storage quota, or corrupt JSON. Showing the
    // tutorial again is a much better failure than crashing the arena.
    return {};
  }
};

export const hasSeenTutorial = (key: TutorialKey): boolean => readSeen()[key] === true;

export const markTutorialSeen = (key: TutorialKey): void => {
  try {
    window.localStorage.setItem(
      SEEN_STORAGE_KEY,
      JSON.stringify({ ...readSeen(), [key]: true }),
    );
  } catch {
    // Nothing to do: the player sees the briefing again next match.
  }
};
