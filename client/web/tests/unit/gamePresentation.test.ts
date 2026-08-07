import assert from 'node:assert/strict';
import test from 'node:test';
import type { GameState } from '../../types/index.ts';
import {
  buildMatchPresentation,
  calculatePerMinuteRate,
  formatMatchClock,
  formatPerMinuteRate,
  getPlayAgainShortcutAction,
  simulationStartMs,
  TEAM_SNAKE_COLORS,
  TEAM_SNAKE_OUTLINE_COLORS,
} from '../../utils/gamePresentation.ts';

const snake = (teamId: number, alive = true) => ({
  body: [{ x: 10, y: 10 }, { x: 9, y: 10 }],
  direction: 'Right' as const,
  is_alive: alive,
  food: 0,
  team_id: teamId,
  speed_milli: 1000,
  movement_credit: 0,
  boost: { charge_ms: 0, active: false },
});

const duelState = (): GameState => ({
  tick: 6,
  status: { Complete: { winning_snake_id: 2 } },
  arena: {
    width: 60,
    height: 40,
    snakes: [snake(0), snake(1), snake(0), snake(1)],
    food: [],
    boost_pads: [],
    team_zone_config: { end_zone_depth: 10, goal_width: 9 },
  },
  game_type: { TeamMatch: { per_team: 2 } },
  queue_mode: 'Competitive',
  is_stress_test: false,
  properties: {
    available_food_target: 3,
    tick_duration_ms: 50,
    time_limit_ms: 90_000,
    boost: null,
  },
  players: {
    7: { user_id: 7, snake_id: 0 },
    8: { user_id: 8, snake_id: 1 },
    9: { user_id: 9, snake_id: 2 },
    10: { user_id: 10, snake_id: 3 },
  },
  game_code: null,
  host_user_id: null,
  start_ms: 0,
  event_sequence: 1,
  usernames: { 7: 'Alex', 8: 'Rival', 9: 'Wing', 10: 'Blocker' },
  spectators: [20, 21],
  scores: { 0: 3, 1: 2, 2: 5, 3: 1 },
  team_scores: { 0: 4, 1: 2 },
  player_xp: { 7: 18, 8: 5, 9: 20, 10: 3 },
  player_action_counts: { 7: 3, 8: 2, 9: 4, 10: 1 },
  readiness: null,
  simulation_epoch_ms: null,
});

const gatedDuelState = (readyUserIds: number[]): GameState => ({
  ...duelState(),
  tick: 0,
  status: { Started: { server_id: 1 } },
  start_ms: 1_000,
  readiness: { deadline_ms: 16_000, ready_user_ids: readyUserIds },
  simulation_epoch_ms: null,
});

test('team presentation puts the local side first and colors its roster like the arena', () => {
  const presentation = buildMatchPresentation(duelState(), 7, 'Competitive');

  assert.equal(presentation.modeLabel, 'Competitive 2v2');
  assert.equal(presentation.sides[0].label, 'Your team');
  assert.equal(presentation.sides[0].score, 4);
  assert.equal(presentation.sides[1].score, 2);
  assert.equal(presentation.spectatorCount, 2);
  assert.deepEqual(
    presentation.sides[0].players.map((player) => [
      player.name,
      player.color,
      player.outlineColor,
    ]),
    [
      ['You', TEAM_SNAKE_COLORS.blue[0], TEAM_SNAKE_OUTLINE_COLORS.blue[0]],
      ['Wing', TEAM_SNAKE_COLORS.blue[1], TEAM_SNAKE_OUTLINE_COLORS.blue[1]],
    ],
  );
  assert.deepEqual(
    presentation.sides[1].players.map((player) => [player.color, player.outlineColor]),
    [
      [TEAM_SNAKE_COLORS.red[0], TEAM_SNAKE_OUTLINE_COLORS.red[0]],
      [TEAM_SNAKE_COLORS.red[1], TEAM_SNAKE_OUTLINE_COLORS.red[1]],
    ],
  );
});

test('a teammate winning produces Victory and retains the current player XP/stat line', () => {
  const presentation = buildMatchPresentation(duelState(), 7);

  assert.equal(presentation.resultTitle, 'Victory');
  assert.equal(presentation.resultTone, 'victory');
  assert.equal(presentation.resultArtwork, 'azure-cut');
  assert.equal(presentation.currentPlayer?.xpGained, 18);
  assert.equal(presentation.currentPlayer?.score, 3);
  assert.equal(presentation.currentPlayer?.finalLength, 5);
  assert.equal(presentation.currentPlayer?.actionCount, 3);
  assert.equal(presentation.timeValue, '01:30');
  assert.equal(presentation.timeTaken, '00:00');
  assert.equal(formatPerMinuteRate(presentation.pointsPerMinute), '600.0');
  assert.equal(formatPerMinuteRate(presentation.actionsPerMinute), '600.0');
});

test('result artwork is selected from outcome state rather than display copy', () => {
  const defeat = buildMatchPresentation(duelState(), 8);
  assert.equal(defeat.resultTitle, 'Defeat');
  assert.equal(defeat.resultArtwork, 'ruby-shatter');

  const drawState = duelState();
  drawState.status = { Complete: { winning_snake_id: null } };
  const draw = buildMatchPresentation(drawState, 7);
  assert.equal(draw.resultTitle, 'Draw');
  assert.equal(draw.resultArtwork, 'topaz-cut');

  const soloState = duelState();
  soloState.game_type = 'Solo';
  soloState.arena.snakes = [snake(0)];
  soloState.players = { 7: { user_id: 7, snake_id: 0 } };
  soloState.scores = { 0: 12 };
  const solo = buildMatchPresentation(soloState, 7);
  assert.equal(solo.resultTitle, 'Run complete');
  assert.equal(solo.resultArtwork, 'jade-fracture');

  const spectator = buildMatchPresentation(duelState());
  assert.equal(spectator.resultTitle, 'Match complete');
  assert.equal(spectator.resultArtwork, 'neutral');
});

test('match clock clamps at zero and rounds remaining partial seconds up', () => {
  assert.equal(formatMatchClock(-1), '00:00');
  assert.equal(formatMatchClock(50, true), '00:01');
  assert.equal(formatMatchClock(60_001, true), '01:01');
});

test('per-minute rates use authoritative elapsed milliseconds and guard invalid durations', () => {
  assert.equal(calculatePerMinuteRate(6, 120_000), 3);
  assert.equal(formatPerMinuteRate(calculatePerMinuteRate(7, 120_000)), '3.5');
  assert.equal(calculatePerMinuteRate(4, 0), 0);
  assert.equal(calculatePerMinuteRate(4, Number.NaN), 0);
  assert.equal(calculatePerMinuteRate(-1, 60_000), 0);
});

test('a zero tick duration does not manufacture elapsed time or per-minute activity', () => {
  const state = duelState();
  state.properties.tick_duration_ms = 0;
  const presentation = buildMatchPresentation(state, 7);

  assert.equal(presentation.elapsedMs, 0);
  assert.equal(presentation.timeTaken, '00:00');
  assert.equal(presentation.pointsPerMinute, 0);
  assert.equal(presentation.actionsPerMinute, 0);
});

test('Space plays again only for an open score card without stealing focused controls', () => {
  const event = (repeat = false, tagName?: string) => ({
    code: 'Space',
    repeat,
    target: tagName ? ({ tagName } as unknown as EventTarget) : null,
  });

  assert.equal(getPlayAgainShortcutAction(event(), false, false), 'ignore');
  assert.equal(getPlayAgainShortcutAction(event(), true, false), 'play-again');
  assert.equal(getPlayAgainShortcutAction(event(true), true, false), 'suppress');
  assert.equal(getPlayAgainShortcutAction(event(), true, true), 'suppress');
  assert.equal(getPlayAgainShortcutAction(event(false, 'input'), true, false), 'ignore');
  assert.equal(getPlayAgainShortcutAction(event(false, 'button'), true, false), 'ignore');
});

test('the roster marks who is ready while the gate is pending', () => {
  const presentation = buildMatchPresentation(gatedDuelState([7, 9]), 7);

  assert.equal(presentation.isAwaitingReadiness, true);
  assert.equal(presentation.readyDeadlineMs, 16_000);
  assert.equal(presentation.pendingReadyCount, 2);
  assert.deepEqual(
    presentation.players.map((player) => [player.userId, player.isReady]),
    [
      [7, true],
      [8, false],
      [9, true],
      [10, false],
    ],
  );
});

test('readiness disappears from the roster once the gate resolves', () => {
  // `isReady: null` is what tells the roster to stop drawing checks, rather
  // than showing a live match with everyone marked unready.
  const presentation = buildMatchPresentation(duelState(), 7);

  assert.equal(presentation.isAwaitingReadiness, false);
  assert.equal(presentation.readyDeadlineMs, null);
  assert.equal(presentation.pendingReadyCount, 0);
  assert.ok(presentation.players.every((player) => player.isReady === null));
});

test('the countdown follows the simulation epoch, never the immutable start_ms', () => {
  // A gated match has no epoch at all: nothing may count down yet.
  assert.equal(simulationStartMs(gatedDuelState([])), null);

  // Once the gate resolves the epoch is authoritative, and start_ms — the
  // durable runtime game identity — is deliberately left behind in the past.
  assert.equal(
    simulationStartMs({ readiness: null, simulation_epoch_ms: 90_000, start_ms: 1_000 }),
    90_000,
  );

  // Matches created before the readiness protocol have neither field and must
  // keep starting off start_ms exactly as they used to.
  assert.equal(
    simulationStartMs({ readiness: null, simulation_epoch_ms: null, start_ms: 4_200 }),
    4_200,
  );
});
