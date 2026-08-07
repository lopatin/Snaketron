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
 * - Team matches are raced to a banked score with no clock and no maximum
 *   duration; both sides level at the target is a draw
 *   (`common/src/game_state.rs`, the `TeamMatch` completion branch). The
 *   target is 25 in Quickmatch and 50 in Competitive, but the copy never
 *   hardcodes either: it reads `properties.score_limit` off the same state the
 *   engine completes against, so the briefing cannot outlive a rule change.
 * - Banking is `carried_segments / 2`, so one food eaten is one team point,
 *   and the snake respawns at starting length afterwards.
 * - Entering the *enemy* end zone kills you; it is not a scoring move.
 * - Food adds two body segments, and personal score is length minus two.
 * - Every matchmade mode has Boost. Space is the key and the default binding
 *   is hold-to-boost. Duel, 2v2 and FFA fill the tank from NOS pickups on the
 *   map; Solo's tank never empties and has nothing to collect
 *   (`boost_config_for` in `common/src/game_state.rs`).
 * - 2v2 and FFA carry double food (`food_target_for`); duel and Solo do not.
 * - Solo and FFA have no clock and no score target, and end only when every
 *   snake is dead.
 * - Competitive duel and 2v2 use the higher score target described above. In
 *   duel, 2v2 and FFA it also selects the ranked MMR pool; Solo affects neither
 *   rules nor rating, so the Solo copy never claims a rank is at stake.
 */
const teamBullets = (
  mode: 'duel' | '2v2',
  ranked: boolean,
  scoreLimit: number | null,
): [TutorialBullet, TutorialBullet, TutorialBullet] => {
  // There is no clock to fall back on, so a match whose state somehow carries
  // no target gets the shape of the rule without inventing a number.
  const race = scoreLimit === null ? 'First to the score target wins' : `First to ${scoreLimit} wins`;
  return [
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
        ? `Never enter the enemy base — it kills you. ${race}, and your rank moves with it.`
        : `Never enter the enemy base — it kills you. ${race}. No clock.`,
    },
  ];
};

const ffaBullets = (ranked: boolean): [TutorialBullet, TutorialBullet, TutorialBullet] => [
  {
    scene: 'ffa-food',
    text: 'Eat food to grow — the field is packed with it. Every bite is worth two points.',
  },
  {
    scene: 'ffa-boost',
    text: 'Drive over NOS canisters to fill your tank, then hold Space for a burst of speed.',
  },
  {
    scene: 'ffa-crash',
    text: ranked
      ? 'One life. Hit a wall, a rival, or your own tail and you are out. The match ends when every snake falls. Highest score wins, and your rank moves with it.'
      : 'One life. Hit a wall, a rival, or your own tail and you are out. The match ends when every snake falls. Highest score wins.',
  },
];

const soloBullets = (): [TutorialBullet, TutorialBullet, TutorialBullet] => [
  {
    scene: 'solo-food',
    text: 'Steer with the arrow keys and eat food to grow. Every bite is worth two points.',
  },
  {
    scene: 'solo-boost',
    text: 'Hold Space to boost. On a solo run the tank never empties, so use it as much as you like.',
  },
  {
    scene: 'solo-run',
    text: 'No clock and no rivals — the run only ends when you crash. Beat your own best.',
  },
];

/**
 * Rule values the copy interpolates. Everything here is read off authoritative
 * state rather than assumed, so a rule change moves the briefing with it.
 */
export interface TutorialFacts {
  /** `properties.score_limit` — the banked team score that ends the match. */
  scoreLimit: number | null;
}

export const tutorialContent = (
  mode: TutorialMode,
  queueMode: QueueMode,
  facts: TutorialFacts = { scoreLimit: null },
): TutorialContent => {
  const ranked = queueMode === 'Competitive';
  const key = tutorialKey(mode, queueMode);
  const bullets =
    mode === 'solo'
      ? soloBullets()
      : mode === 'ffa'
        ? ffaBullets(ranked)
        : teamBullets(mode, ranked, facts.scoreLimit);

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
  gameState: Pick<GameState, 'game_type' | 'queue_mode' | 'properties'>,
): TutorialContent | null => {
  const mode = tutorialModeForGameType(gameState.game_type);
  return mode === null
    ? null
    : tutorialContent(mode, gameState.queue_mode, {
        scoreLimit: gameState.properties.score_limit,
      });
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
  'ffa-boost',
  'ffa-crash',
  'solo-food',
  'solo-boost',
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
