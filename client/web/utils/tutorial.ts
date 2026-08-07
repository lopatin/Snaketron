import type { GameState, GameType, QueueMode } from '../types';
import type { BoostInputMode } from './boostInput';

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

export interface TutorialStep {
  /** Scene id understood by `TutorialScenePlayer` in the WASM renderer. */
  scene: string;
  /** Short action-led heading shown above the instruction. */
  title: string;
  /** One focused gameplay instruction. */
  body: string;
  /** Describes the meaningful motion in the scene without relying on colour. */
  visualLabel: string;
}

export interface TutorialContent {
  key: TutorialKey;
  title: string;
  /** Queue vocabulary already used elsewhere in the app. */
  kicker: string;
  steps: [TutorialStep, TutorialStep, TutorialStep];
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
 * Step copy is written from the engine's actual rules, not from the design
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
const collectibleBoostStep = (inputMode: BoostInputMode): TutorialStep => ({
  scene: 'team-boost',
  title: 'BOOST',
  body:
    inputMode === 'toggle'
      ? 'Collect NOS, then press Space to toggle boost.'
      : 'Collect NOS, then hold Space to boost.',
  visualLabel: 'A snake collects NOS; its fuel fills and it accelerates.',
});

const teamSteps = (
  mode: 'duel' | '2v2',
  ranked: boolean,
  scoreLimit: number | null,
  inputMode: BoostInputMode,
): [TutorialStep, TutorialStep, TutorialStep] => {
  // There is no clock to fall back on, so a match whose state somehow carries
  // no target gets the shape of the rule without inventing a number.
  const race =
    scoreLimit === null ? 'Reach the score target first' : `First to ${scoreLimit} wins`;
  return [
    {
      scene: 'team-carry',
      title: mode === 'duel' ? 'BANK POINTS' : 'SCORE TOGETHER',
      body:
        mode === 'duel'
          ? 'Eat, then return through your gate to bank points.'
          : 'Eat, then return through your gate. Your team shares the score.',
      visualLabel:
        mode === 'duel'
          ? 'A snake returns through the gate labeled YOU; the team score increases.'
          : 'A teammate returns through the gate labeled YOU; the shared team score increases.',
    },
    collectibleBoostStep(inputMode),
    {
      scene: 'team-danger',
      title: 'WIN',
      body: `${race}—no clock. Entering the enemy base kills you.${
        ranked ? ' This match affects your rank.' : ''
      }`,
      visualLabel: 'A snake enters the base labeled RIVAL and crashes.',
    },
  ];
};

const ffaSteps = (
  ranked: boolean,
  inputMode: BoostInputMode,
): [TutorialStep, TutorialStep, TutorialStep] => [
  {
    scene: 'ffa-food',
    title: 'GROW',
    body: 'Eat food. Each bite adds 2 points.',
    visualLabel: 'A snake eats food, grows two segments, and gains two points.',
  },
  { ...collectibleBoostStep(inputMode), scene: 'ffa-boost' },
  {
    scene: 'ffa-crash',
    title: 'ONE LIFE',
    body: `Crash and you’re out. When all snakes are out, highest score wins.${
      ranked ? ' This match affects your rank.' : ''
    }`,
    visualLabel: 'A snake hits a rival and is eliminated.',
  },
];

const soloSteps = (inputMode: BoostInputMode): [TutorialStep, TutorialStep, TutorialStep] => [
  {
    scene: 'solo-food',
    title: 'MOVE & GROW',
    body: 'Steer with the arrow keys. Each bite adds 2 points.',
    visualLabel: 'The solo snake turns toward food and grows.',
  },
  {
    scene: 'solo-boost',
    title: 'UNLIMITED BOOST',
    body:
      inputMode === 'toggle'
        ? 'Press Space to toggle boost. Your tank never runs out.'
        : 'Hold Space to boost. Your tank never runs out.',
    visualLabel: 'The solo snake boosts while its full fuel meter stays full.',
  },
  {
    scene: 'solo-run',
    title: 'BEAT YOUR BEST',
    body: 'No clock. Your run ends when you crash.',
    visualLabel: 'The solo snake hits its tail, ending the run.',
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
  inputMode: BoostInputMode = 'hold',
): TutorialContent => {
  const ranked = queueMode === 'Competitive';
  const key = tutorialKey(mode, queueMode);
  const steps =
    mode === 'solo'
      ? soloSteps(inputMode)
      : mode === 'ffa'
        ? ffaSteps(ranked, inputMode)
        : teamSteps(mode, ranked, facts.scoreLimit, inputMode);

  return {
    key,
    title: MODE_LABELS[mode],
    // Solo records a high score in either queue and never touches MMR, so
    // calling Competitive Solo ranked would be a lie the player could check.
    kicker: mode === 'solo' ? 'HIGH SCORE' : ranked ? 'COMPETITIVE' : 'QUICK MATCH',
    steps,
  };
};

export const tutorialContentForGame = (
  gameState: Pick<GameState, 'game_type' | 'queue_mode' | 'properties'>,
  inputMode: BoostInputMode = 'hold',
): TutorialContent | null => {
  const mode = tutorialModeForGameType(gameState.game_type);
  return mode === null
    ? null
    : tutorialContent(
        mode,
        gameState.queue_mode,
        { scoreLimit: gameState.properties.score_limit },
        inputMode,
      );
};

/**
 * TypeScript-side contract for the `SCENES` registry in
 * `client/src/tutorial.rs`. Unit tests keep every content step inside this set;
 * the Rust registry has its own ID/uniqueness test, and the canvas marks a
 * mismatched WASM constructor call as an error at runtime.
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
