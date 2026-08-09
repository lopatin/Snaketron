import React, { useState } from 'react';
import type { MatchPresentation } from '../utils/gamePresentation';
import {
  buildRatingReveal,
  type MatchRatingState,
  type RatingSnapshot,
} from '../utils/ratingReveal';
import GameOverCard from './GameOverCard';

/**
 * Dev-only design-review harness for the post-match rating reveal
 * (`/qa/rating-reveal`, excluded from production routing). Renders the real
 * GameOverCard with fixture data and replays each reveal scenario on demand,
 * because the real card only exists for the seconds after a live match ends.
 */

const snapshot = (
  mmr: number,
  wins: number,
  losses: number,
  position: number | null,
): RatingSnapshot => ({ mmr, wins, losses, position });

const SCENARIOS: Array<{ id: string; label: string; state: MatchRatingState | null }> = [
  {
    id: 'promotion',
    label: 'Win · tier promotion',
    state: {
      phase: 'ready',
      reveal: buildRatingReveal(
        'Competitive',
        'duel',
        snapshot(1190, 9, 4, 20),
        snapshot(1215, 10, 4, 16),
      ),
    },
  },
  {
    id: 'gain',
    label: 'Win · no movement',
    state: {
      phase: 'ready',
      reveal: buildRatingReveal(
        'Competitive',
        'duel',
        snapshot(1228, 12, 7, 31),
        snapshot(1246, 13, 7, 29),
      ),
    },
  },
  {
    id: 'loss',
    label: 'Loss · no movement',
    state: {
      phase: 'ready',
      reveal: buildRatingReveal(
        'Competitive',
        'duel',
        snapshot(1246, 13, 8, 29),
        snapshot(1229, 13, 9, 32),
      ),
    },
  },
  {
    id: 'demotion',
    label: 'Loss · demotion',
    state: {
      phase: 'ready',
      reveal: buildRatingReveal(
        'Competitive',
        'duel',
        snapshot(1206, 13, 10, 33),
        snapshot(1187, 13, 11, 38),
      ),
    },
  },
  {
    id: 'big-swing',
    label: 'Win · big climb',
    state: {
      phase: 'ready',
      reveal: buildRatingReveal(
        'Competitive',
        'ffa',
        snapshot(1862, 40, 22, 9),
        snapshot(1911, 41, 22, 7),
      ),
    },
  },
  {
    id: 'draw',
    label: 'Draw · ±0',
    state: {
      phase: 'ready',
      reveal: buildRatingReveal(
        'Competitive',
        'duel',
        snapshot(1246, 13, 8, 29),
        snapshot(1246, 13, 8, 29),
      ),
    },
  },
  {
    id: 'placement',
    label: 'First rated match',
    state: {
      phase: 'ready',
      reveal: buildRatingReveal('Competitive', 'duel', null, snapshot(1020, 1, 0, 44)),
    },
  },
  {
    id: 'quickmatch',
    label: 'Quickmatch · casual',
    state: {
      phase: 'ready',
      reveal: buildRatingReveal(
        'Quickmatch',
        '2v2',
        snapshot(1190, 3, 1, null),
        snapshot(1240, 4, 1, null),
      ),
    },
  },
  { id: 'pending', label: 'Pending · tallying', state: { phase: 'pending' } },
  // Solo and custom matches have no ladder: the card renders without a band.
  { id: 'none', label: 'No rating (solo/custom)', state: null },
];

const fixturePresentation: MatchPresentation = {
  modeLabel: 'Competitive Duel',
  isTeamGame: true,
  isSoloGame: false,
  isComplete: true,
  timeLabel: 'First to 25',
  timeValue: '03:41',
  scoreLimit: 25,
  elapsedMs: 221_000,
  timeTaken: '03:41',
  pointsPerMinute: 6.8,
  actionsPerMinute: 31.2,
  spectatorCount: 0,
  players: [0, 1].map((snakeId) => ({
    snakeId,
    userId: snakeId + 1,
    name: snakeId === 0 ? 'You' : 'Rival',
    isCurrentPlayer: snakeId === 0,
    isAlive: snakeId === 0,
    isIdleKicked: false,
    teamId: snakeId,
    skin: {
      snake_index: snakeId,
      team_id: snakeId,
      team_member_slot: 0,
      snake_count: 2,
      is_team_game: true,
      local_snake_id: 0,
      local_team_id: 0,
    },
    score: snakeId === 0 ? 25 : 19,
    finalLength: snakeId === 0 ? 27 : 21,
    foodHeld: 0,
    xpGained: snakeId === 0 ? 140 : 95,
    actionCount: 115,
    isWinner: snakeId === 0,
    isReady: null,
  })),
  currentPlayer: null,
  sides: [],
  soloScore: 25,
  resultTitle: 'Victory',
  resultSummary: 'Your side got there first.',
  resultTone: 'victory',
  resultArtwork: 'azure-cut',
  isAwaitingReadiness: false,
  readyDeadlineMs: null,
  pendingReadyCount: 0,
};

const presentation: MatchPresentation = {
  ...fixturePresentation,
  currentPlayer: fixturePresentation.players[0],
  sides: fixturePresentation.players.map((player, index) => ({
    teamId: index,
    label: index === 0 ? 'Your team' : 'Opponents',
    color: index === 0 ? '#3C8DDE' : '#EF5A5A',
    score: player.score,
    players: [player],
    isCurrentSide: index === 0,
    isWinner: index === 0,
  })),
};

const RatingRevealQA: React.FC = () => {
  const [scenarioId, setScenarioId] = useState('promotion');
  const [replayKey, setReplayKey] = useState(0);
  const scenario = SCENARIOS.find(({ id }) => id === scenarioId) ?? SCENARIOS[0];

  return (
    <div style={{ position: 'fixed', inset: 0, background: '#F7F9FB' }}>
      <div
        style={{
          position: 'fixed',
          top: 10,
          left: 10,
          zIndex: 200,
          display: 'flex',
          flexWrap: 'wrap',
          gap: 6,
          maxWidth: 380,
        }}
      >
        {SCENARIOS.map(({ id, label }) => (
          <button
            key={id}
            type="button"
            data-testid={`qa-scenario-${id}`}
            onClick={() => {
              setScenarioId(id);
              setReplayKey((key) => key + 1);
            }}
            style={{
              padding: '5px 9px',
              border: '1px solid #172033',
              background: id === scenario.id ? '#172033' : '#fff',
              color: id === scenario.id ? '#fff' : '#172033',
              fontSize: 11,
              fontWeight: 700,
              cursor: 'pointer',
            }}
          >
            {label}
          </button>
        ))}
      </div>
      <GameOverCard
        key={`${scenario.id}:${replayKey}`}
        open
        presentation={presentation}
        rating={scenario.state ?? undefined}
        onDismiss={() => setReplayKey((key) => key + 1)}
        onMenu={() => undefined}
        onPlayAgain={() => setReplayKey((key) => key + 1)}
      />
    </div>
  );
};

export default RatingRevealQA;
