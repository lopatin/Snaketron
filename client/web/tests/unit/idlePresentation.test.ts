import assert from 'node:assert/strict';
import test from 'node:test';
import type { GameState } from '../../types/index.ts';
import {
  buildPlayerIdlePresentation,
  getIdleKickedUserIds,
  wasCompletedByInactivity,
  wasPlayerIdleKicked,
  type IdleStateSnapshot,
} from '../../utils/idlePresentation.ts';

const startedState = (overrides: Partial<IdleStateSnapshot> = {}): IdleStateSnapshot => ({
  tick: 500,
  status: { Started: { server_id: 1 } },
  properties: {
    tick_duration_ms: 100,
    player_idle_timeout_ms: 60_000,
    player_idle_warning_ms: 10_000,
  },
  players: {
    7: { user_id: 7, snake_id: 0 },
    8: { user_id: 8, snake_id: 1 },
  },
  player_last_activity_ticks: { 7: 0 },
  idle_kicked_user_ids: [],
  completed_by_inactivity: false,
  ...overrides,
});

test('idle warning begins at the configured authoritative threshold', () => {
  const before = buildPlayerIdlePresentation(startedState({ tick: 499 }), 7);
  assert.equal(before.warning, null);

  const atThreshold = buildPlayerIdlePresentation(startedState({ tick: 500 }), 7);
  assert.deepEqual(atThreshold, {
    isKicked: false,
    warning: {
      deadlineTick: 600,
      remainingMs: 10_000,
      remainingSeconds: 10,
      progress: 1,
      isUrgent: false,
    },
  });
});

test('server-provided twenty-second policy drives both warning boundaries', () => {
  const serverConfiguredState = startedState({
    tick: 99,
    properties: {
      tick_duration_ms: 100,
      player_idle_timeout_ms: 20_000,
      player_idle_warning_ms: 10_000,
    },
  });

  assert.equal(buildPlayerIdlePresentation(serverConfiguredState, 7).warning, null);
  assert.deepEqual(
    buildPlayerIdlePresentation({ ...serverConfiguredState, tick: 100 }, 7),
    {
      isKicked: false,
      warning: {
        deadlineTick: 200,
        remainingMs: 10_000,
        remainingSeconds: 10,
        progress: 1,
        isUrgent: false,
      },
    },
  );
  assert.equal(
    buildPlayerIdlePresentation({ ...serverConfiguredState, tick: 200 }, 7).warning,
    null,
  );
  assert.equal(
    buildPlayerIdlePresentation({ ...serverConfiguredState, tick: 201 }, 7).warning,
    null,
  );
});

test('idle warning uses simulation ticks, rounds seconds up, and becomes urgent', () => {
  const presentation = buildPlayerIdlePresentation(startedState({ tick: 551 }), 7);

  assert.equal(presentation.warning?.remainingMs, 4_900);
  assert.equal(presentation.warning?.remainingSeconds, 5);
  assert.equal(presentation.warning?.progress, 0.49);
  assert.equal(presentation.warning?.isUrgent, true);
});

test('urgent styling begins halfway through the server-provided warning duration', () => {
  const state = startedState({
    tick: 499,
    properties: {
      tick_duration_ms: 100,
      player_idle_timeout_ms: 60_000,
      player_idle_warning_ms: 20_000,
    },
  });

  assert.equal(buildPlayerIdlePresentation(state, 7).warning?.isUrgent, false);
  assert.equal(
    buildPlayerIdlePresentation({ ...state, tick: 500 }, 7).warning?.isUrgent,
    true,
  );
});

test('idle deadline rounds up to the first authoritative simulation quantum', () => {
  const beforeWarning = startedState({
    tick: 0,
    properties: {
      tick_duration_ms: 100,
      player_idle_timeout_ms: 1_050,
      player_idle_warning_ms: 1_000,
    },
  });
  assert.equal(buildPlayerIdlePresentation(beforeWarning, 7).warning, null);

  const firstWarning = buildPlayerIdlePresentation({ ...beforeWarning, tick: 1 }, 7);
  assert.equal(firstWarning.warning?.deadlineTick, 11);
  assert.equal(firstWarning.warning?.remainingMs, 1_000);
});

test('a predicted activity tick immediately clears the warning', () => {
  const presentation = buildPlayerIdlePresentation(startedState({
    tick: 551,
    player_last_activity_ticks: { 7: 551 },
  }), 7);

  assert.equal(presentation.warning, null);
  assert.equal(presentation.isKicked, false);
});

test('missing legacy tracking and non-started matches never manufacture a warning', () => {
  assert.equal(buildPlayerIdlePresentation(startedState({
    player_last_activity_ticks: {},
  }), 7).warning, null);
  assert.equal(buildPlayerIdlePresentation(startedState({
    status: 'Stopped',
  }), 7).warning, null);
  assert.equal(buildPlayerIdlePresentation(startedState({
    status: { Complete: { winning_snake_id: null } },
  }), 7).warning, null);
});

test('solo and one-player snapshots never show a multiplayer idle warning', () => {
  const solo = startedState({
    game_type: 'Solo',
    players: { 7: { user_id: 7, snake_id: 0 } },
  });

  assert.equal(buildPlayerIdlePresentation(solo, 7).warning, null);
});

test('eliminated field players are quiet while team players awaiting respawn are warned', () => {
  const eliminatedPlayer = {
    user_id: 7,
    snake_id: 0,
  };
  const deadSnake = { is_alive: false };
  const field = startedState({
    game_type: { FreeForAll: { max_players: 4 } },
    players: {
      7: eliminatedPlayer,
      8: { user_id: 8, snake_id: 1 },
    },
    arena: { snakes: [deadSnake] as GameState['arena']['snakes'] },
  });
  assert.equal(buildPlayerIdlePresentation(field, 7).warning, null);

  const team = startedState({
    ...field,
    game_type: { TeamMatch: { per_team: 1 } },
  });
  assert.equal(buildPlayerIdlePresentation(team, 7).warning?.remainingSeconds, 10);

  const customTeam = startedState({
    ...field,
    game_type: {
      Custom: {
        settings: {
          arena_width: 60,
          arena_height: 40,
          tick_duration_ms: 100,
          food_spawn_rate: 10,
          max_players: 2,
          game_mode: 'Duel',
          is_private: true,
          allow_spectators: true,
          snake_start_length: 2,
        },
      },
    },
    team_scores: { 0: 0, 1: 0 },
  });
  assert.equal(buildPlayerIdlePresentation(customTeam, 7).warning?.remainingSeconds, 10);
});

test('kicked users are normalized and suppress their warning', () => {
  const state = startedState({ idle_kicked_user_ids: [9, 7, 9, -1, 8.8] });

  assert.deepEqual(getIdleKickedUserIds(state), [7, 9]);
  assert.equal(wasPlayerIdleKicked(state, 7), true);
  assert.deepEqual(buildPlayerIdlePresentation(state, 7), {
    isKicked: true,
    warning: null,
  });
});

test('inactivity completion is explicit rather than inferred from a draw', () => {
  assert.equal(wasCompletedByInactivity(startedState()), false);
  assert.equal(wasCompletedByInactivity(startedState({ completed_by_inactivity: true })), true);
});
