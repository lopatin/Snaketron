import React, { useMemo } from 'react';
import { useSearchParams } from 'react-router-dom';
import validClipJson from '../fixtures/potg-goal-run.json';
import type { HighlightClip } from '../types';
import type { MatchHighlightState } from '../utils/highlightPresentation';
import type { MatchPresentation } from '../utils/gamePresentation';
import { buildRatingReveal, type MatchRatingState } from '../utils/ratingReveal';
import GameOverCard from './GameOverCard';
import './PlayOfTheGameQA.css';

/**
 * Provider-light, network-free acceptance lab for the post-match replay band.
 * The route is mounted outside auth/websocket providers in App.tsx and is
 * omitted from production bundles entirely.
 */

type QaState =
  | 'ready'
  | 'ranked'
  | 'pending'
  | 'unavailable'
  | 'incompatible'
  | 'network'
  | 'malformed-anchor'
  | 'bad-end-hash';

const STATES: readonly { id: QaState; label: string }[] = [
  { id: 'ready', label: 'Ready replay' },
  { id: 'ranked', label: 'Ranked star' },
  { id: 'pending', label: 'Pending cut' },
  { id: 'unavailable', label: 'Unavailable' },
  { id: 'incompatible', label: 'Version mismatch' },
  { id: 'network', label: 'Network failure' },
  { id: 'malformed-anchor', label: 'Malformed anchor' },
  { id: 'bad-end-hash', label: 'Bad end hash' },
] as const;

const validClip = validClipJson as unknown as HighlightClip;

const cloneClip = (): HighlightClip => (
  JSON.parse(JSON.stringify(validClip)) as HighlightClip
);

const highlightForState = (state: QaState): MatchHighlightState => {
  switch (state) {
    case 'pending':
      return { phase: 'pending' };
    case 'unavailable':
      return { phase: 'unavailable', reason: 'absent' };
    case 'incompatible':
      return { phase: 'unavailable', reason: 'incompatible' };
    case 'network':
      return { phase: 'unavailable', reason: 'network' };
    case 'malformed-anchor': {
      const clip = cloneClip();
      clip.anchor.tick = clip.window.start_tick + 1;
      return { phase: 'ready', clip };
    }
    case 'bad-end-hash': {
      const clip = cloneClip();
      clip.end_sync_hash = (BigInt(clip.end_sync_hash) ^ 1n).toString();
      return { phase: 'ready', clip };
    }
    case 'ready':
    case 'ranked':
      return { phase: 'ready', clip: cloneClip() };
  }
};

/**
 * The 'ranked' fixture is the only one that supplies a rating and a star rank.
 * In the product both arrive independently — the rating from the local
 * player's own ladder read, the star's rank from `useStarRank` — so the lab
 * keeps a state with neither (the badge is simply absent) and one with both,
 * which is also the state that exercises rating-then-replay sequencing.
 */
const rankedRating: Extract<MatchRatingState, { phase: 'ready' }> = {
  phase: 'ready',
  reveal: buildRatingReveal(
    'Competitive',
    'duel',
    { mmr: 2281, wins: 61, losses: 44 },
    { mmr: 2320, wins: 62, losses: 44 },
  ),
};

const basePlayers: MatchPresentation['players'] = [
  {
    snakeId: 0,
    userId: 1,
    name: 'BANKER',
    isCurrentPlayer: true,
    isAlive: true,
    isIdleKicked: false,
    teamId: 0,
    skin: {
      snake_index: 0,
      team_id: 0,
      team_member_slot: 0,
      snake_count: 2,
      is_team_game: true,
      local_snake_id: 0,
      local_team_id: 0,
    },
    score: 25,
    finalLength: 27,
    foodHeld: 0,
    xpGained: 140,
    actionCount: 112,
    isWinner: true,
    deathAttribution: null,
    isReady: null,
  },
  {
    snakeId: 1,
    userId: 2,
    name: 'DEFENDER',
    isCurrentPlayer: false,
    isAlive: false,
    isIdleKicked: false,
    teamId: 1,
    skin: {
      snake_index: 1,
      team_id: 1,
      team_member_slot: 0,
      snake_count: 2,
      is_team_game: true,
      local_snake_id: 0,
      local_team_id: 0,
    },
    score: 19,
    finalLength: 21,
    foodHeld: 0,
    xpGained: 95,
    actionCount: 98,
    isWinner: false,
    deathAttribution: 'Demolished by BANKER',
    isReady: null,
  },
];

const presentation: MatchPresentation = {
  modeLabel: 'Quick Duel',
  isTeamGame: true,
  isSoloGame: false,
  isComplete: true,
  timeLabel: 'First to 25',
  timeValue: '03:41',
  scoreLimit: 25,
  elapsedMs: 221_000,
  timeTaken: '03:41',
  pointsPerMinute: 6.8,
  actionsPerMinute: 30.4,
  spectatorCount: 0,
  players: basePlayers,
  currentPlayer: basePlayers[0],
  sides: [
    {
      teamId: 0,
      label: 'Your team',
      color: '#3b82f6',
      score: 25,
      players: [basePlayers[0]],
      isCurrentSide: true,
      isWinner: true,
    },
    {
      teamId: 1,
      label: 'Opponents',
      color: '#ef4444',
      score: 19,
      players: [basePlayers[1]],
      isCurrentSide: false,
      isWinner: false,
    },
  ],
  soloScore: 25,
  resultTitle: 'Victory',
  resultSummary: 'Your side got there first.',
  shareSummary: 'I won a Snaketron Quick Duel, 25–19.',
  resultTone: 'victory',
  resultArtwork: 'azure-cut',
  isAwaitingReadiness: false,
  readyDeadlineMs: null,
  pendingReadyCount: 0,
};

const isQaState = (value: string | null): value is QaState => (
  STATES.some(({ id }) => id === value)
);

const PlayOfTheGameQA: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const requested = searchParams.get('state');
  const state: QaState = isQaState(requested) ? requested : 'ready';
  const highlight = useMemo(() => highlightForState(state), [state]);
  const hideChrome = searchParams.get('chrome') === '0';

  return (
    <main className="potg-qa" data-testid="potg-qa" data-state={state}>
      {!hideChrome && (
        <nav className="potg-qa__controls" aria-label="Play of the game fixture states">
          <strong>PotG acceptance lab</strong>
          {STATES.map(({ id, label }) => (
            <button
              key={id}
              type="button"
              data-testid={`potg-qa-${id}`}
              aria-pressed={id === state}
              onClick={() => {
                const next = new URLSearchParams(searchParams);
                next.set('state', id);
                setSearchParams(next, { replace: true });
              }}
            >
              {label}
            </button>
          ))}
        </nav>
      )}

      <output className="sr-only" data-testid="potg-qa-state">{state}</output>
      <GameOverCard
        key={state}
        open
        gameId="4242"
        presentation={presentation}
        highlight={highlight}
        rating={state === 'ranked' ? rankedRating : undefined}
        starRank={state === 'ranked' ? rankedRating.reveal.toRank : null}
        onDismiss={() => undefined}
        onMenu={() => undefined}
        onPlayAgain={() => undefined}
      />
    </main>
  );
};

export default PlayOfTheGameQA;
