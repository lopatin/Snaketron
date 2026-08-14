import type { GameState, GameType, QueueMode } from '../types';
import { gameStorage } from '../services/gameStorage.ts';
import type { BoostInputMode } from './boostInput';
import type { InputSurface } from './inputSurface';

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
  /** Short action label used by replay and progress controls. */
  title: string;
  /** The single visible, glanceable gameplay instruction. */
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
 * - Each awarded combo point queues one physical growth segment. Team banking
 *   scores every carried awarded segment, then respawns the snake at starting
 *   length.
 * - Entering the *enemy* end zone kills you; it is not a scoring move.
 * - Combo timing and food value come from `properties.combo`; the briefing
 *   describes the +3 cap without baking in the configured window length.
 * - Every matchmade mode has Boost. Space is the key and the default binding
 *   is hold-to-boost. Duel, 2v2 and FFA fill the tank from NOS pickups on the
 *   map; Solo's tank never empties and has nothing to collect
 *   (`boost_config_for` in `common/src/game_state.rs`).
 * - 2v2 and FFA spawn double food density (`food_target_for`); duel and Solo
 *   do not.
 * - Solo and FFA have no clock and no score target, and end only when every
 *   snake is dead.
 * - Competitive duel and 2v2 use the higher score target described above. The
 *   queue context is already named by the tutorial kicker, so step copy stays
 *   focused on what the player needs to do.
 */
const collectibleBoostStep = (
  inputMode: BoostInputMode,
  inputSurface: InputSurface,
): TutorialStep => ({
  scene: 'team-boost',
  title: 'BOOST',
  body:
    inputSurface === 'touch'
      ? (inputMode === 'toggle'
          ? 'Collect NOS, then tap the NOS button to toggle boost.'
          : 'Collect NOS, then hold the NOS button to boost.')
      : (inputMode === 'toggle'
          ? 'Collect NOS, then press Space to toggle boost.'
          : 'Collect NOS, then hold Space to boost.'),
  visualLabel: 'A snake collects NOS; its fuel fills and it accelerates.',
});

const teamSteps = (
  mode: 'duel' | '2v2',
  scoreLimit: number | null,
  inputMode: BoostInputMode,
  inputSurface: InputSurface,
): [TutorialStep, TutorialStep, TutorialStep] => {
  // There is no clock to fall back on, so a match whose state somehow carries
  // no target gets the shape of the rule without inventing a number.
  const race =
    scoreLimit === null ? 'Reach the score target first' : `First to ${scoreLimit} wins`;
  return [
    {
      scene: 'team-carry',
      title: mode === 'duel' ? 'CHAIN & BANK' : 'CHAIN & SCORE',
      body: 'Two foods start COMBO; reach +3 before it drains; bank.',
      visualLabel:
        mode === 'duel'
          ? 'A snake carries a combo through the gate labeled YOU; the team score increases.'
          : 'A teammate carries a combo through the gate labeled YOU; the shared team score increases.',
    },
    collectibleBoostStep(inputMode, inputSurface),
    {
      scene: 'team-danger',
      title: 'STAY OUT',
      body: `Avoid the rival base. ${race}.`,
      visualLabel: 'A snake enters the base labeled RIVAL and crashes.',
    },
  ];
};

const ffaSteps = (
  inputMode: BoostInputMode,
  inputSurface: InputSurface,
): [TutorialStep, TutorialStep, TutorialStep] => [
  {
    scene: 'ffa-food',
    title: 'COMBO & GROW',
    body: 'Two foods start COMBO; reach +3 before it drains.',
    visualLabel: 'A snake eats ordinary food; one more quick pickup starts COMBO.',
  },
  { ...collectibleBoostStep(inputMode, inputSurface), scene: 'ffa-boost' },
  {
    scene: 'ffa-crash',
    title: 'ONE LIFE',
    body: 'Crash once and you’re out. Highest score wins.',
    visualLabel: 'A snake hits a rival and is eliminated.',
  },
];

const soloSteps = (
  inputMode: BoostInputMode,
  inputSurface: InputSurface,
): [TutorialStep, TutorialStep, TutorialStep] => [
  {
    scene: 'solo-food',
    title: 'COMBO & GROW',
    body:
      inputSurface === 'touch'
        ? 'D-pad: two foods start COMBO; reach +3 before it drains.'
        : 'Arrows: two foods start COMBO; reach +3 before it drains.',
    visualLabel: 'The solo snake eats ordinary food; one more quick pickup starts COMBO.',
  },
  {
    scene: 'solo-boost',
    title: 'UNLIMITED BOOST',
    body:
      inputSurface === 'touch'
        ? (inputMode === 'toggle'
            ? 'Tap the NOS button to toggle unlimited boost.'
            : 'Hold the NOS button for unlimited boost.')
        : (inputMode === 'toggle'
            ? 'Press Space to toggle unlimited boost.'
            : 'Hold Space for unlimited boost.'),
    visualLabel: 'The solo snake boosts while its full fuel meter stays full.',
  },
  {
    scene: 'solo-run',
    title: 'BEAT YOUR BEST',
    body: 'Avoid crashing and beat your high score.',
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
  inputSurface: InputSurface = 'keyboard',
): TutorialContent => {
  const ranked = queueMode === 'Competitive';
  const key = tutorialKey(mode, queueMode);
  const steps =
    mode === 'solo'
      ? soloSteps(inputMode, inputSurface)
      : mode === 'ffa'
        ? ffaSteps(inputMode, inputSurface)
        : teamSteps(mode, facts.scoreLimit, inputMode, inputSurface);

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
  inputSurface: InputSurface = 'keyboard',
): TutorialContent | null => {
  const mode = tutorialModeForGameType(gameState.game_type);
  return mode === null
    ? null
    : tutorialContent(
        mode,
        gameState.queue_mode,
        { scoreLimit: gameState.properties.score_limit },
        inputMode,
        inputSurface,
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
    const raw = gameStorage.getItem(SEEN_STORAGE_KEY);
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
    gameStorage.setItem(
      SEEN_STORAGE_KEY,
      JSON.stringify({ ...readSeen(), [key]: true }),
    );
  } catch {
    // Nothing to do: the player sees the briefing again next match.
  }
};
