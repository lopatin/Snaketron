import type { GameState } from '../types';

/**
 * Narrow structural view of the idle fields added to authoritative snapshots.
 * Keeping the helper structural lets it remain usable while generated bindings
 * are refreshed from Rust; callers never need to hand-maintain a wire type.
 */
export interface IdleStateSnapshot {
  tick: number;
  status: GameState['status'];
  game_type?: GameState['game_type'];
  team_scores?: GameState['team_scores'];
  players?: GameState['players'];
  arena?: Pick<GameState['arena'], 'snakes'>;
  properties: {
    tick_duration_ms: number;
    player_idle_timeout_ms?: number;
    player_idle_warning_ms?: number;
  };
  player_last_activity_ticks?: { [key: number]: number | undefined };
  idle_kicked_user_ids?: readonly number[];
  completed_by_inactivity?: boolean;
}

export interface IdleWarningPresentation {
  deadlineTick: number;
  remainingMs: number;
  remainingSeconds: number;
  progress: number;
  isUrgent: boolean;
}

export interface PlayerIdlePresentation {
  isKicked: boolean;
  warning: IdleWarningPresentation | null;
}

const finiteNonNegativeInteger = (value: unknown): number | null => (
  typeof value === 'number' && Number.isSafeInteger(value) && value >= 0
    ? value
    : null
);

const isStarted = (status: GameState['status']): boolean => (
  typeof status === 'object' && status !== null && 'Started' in status
);

const isEliminatedFieldPlayer = (
  state: IdleStateSnapshot,
  userId: number,
): boolean => {
  const isTeamMatch = Boolean(
    (
      state.game_type &&
      typeof state.game_type === 'object' &&
      'TeamMatch' in state.game_type
    ) || state.team_scores,
  );
  if (isTeamMatch) {
    return false;
  }

  const localPlayer = state.players?.[userId];
  const localSnake = localPlayer
    ? state.arena?.snakes?.[localPlayer.snake_id]
    : undefined;
  return localSnake?.is_alive === false;
};

export const getIdleKickedUserIds = (
  state: Pick<IdleStateSnapshot, 'idle_kicked_user_ids'> | null | undefined,
): number[] => {
  if (!Array.isArray(state?.idle_kicked_user_ids)) {
    return [];
  }

  return Array.from(new Set(
    state.idle_kicked_user_ids
      .map(finiteNonNegativeInteger)
      .filter((userId): userId is number => userId !== null),
  )).sort((left, right) => left - right);
};

export const wasPlayerIdleKicked = (
  state: Pick<IdleStateSnapshot, 'idle_kicked_user_ids'> | null | undefined,
  userId: number | null | undefined,
): boolean => (
  userId !== null &&
  userId !== undefined &&
  getIdleKickedUserIds(state).includes(userId)
);

export const wasCompletedByInactivity = (
  state: Pick<IdleStateSnapshot, 'completed_by_inactivity'> | null | undefined,
): boolean => state?.completed_by_inactivity === true;

export const buildPlayerIdlePresentation = (
  state: IdleStateSnapshot | null | undefined,
  userId: number | null | undefined,
): PlayerIdlePresentation => {
  const isKicked = wasPlayerIdleKicked(state, userId);
  const playerCount = state?.players
    ? Object.values(state.players).filter(Boolean).length
    : 0;
  if (
    !state ||
    userId === null ||
    userId === undefined ||
    isKicked ||
    !isStarted(state.status) ||
    playerCount < 2 ||
    isEliminatedFieldPlayer(state, userId)
  ) {
    return { isKicked, warning: null };
  }

  const tick = finiteNonNegativeInteger(state.tick);
  const tickDurationMs = finiteNonNegativeInteger(state.properties.tick_duration_ms);
  const timeoutMs = finiteNonNegativeInteger(state.properties.player_idle_timeout_ms);
  const configuredWarningMs = finiteNonNegativeInteger(state.properties.player_idle_warning_ms);
  const lastActivityTick = finiteNonNegativeInteger(
    state.player_last_activity_ticks?.[userId],
  );

  if (
    tick === null ||
    tickDurationMs === null ||
    tickDurationMs === 0 ||
    timeoutMs === null ||
    timeoutMs === 0 ||
    configuredWarningMs === null ||
    configuredWarningMs === 0 ||
    lastActivityTick === null
  ) {
    return { isKicked, warning: null };
  }

  const warningMs = Math.min(configuredWarningMs, timeoutMs);
  const elapsedTicks = Math.max(0, tick - lastActivityTick);
  const elapsedMs = elapsedTicks * tickDurationMs;
  if (elapsedMs < timeoutMs - warningMs || elapsedMs >= timeoutMs) {
    return { isKicked, warning: null };
  }

  // The server expires on the first whole simulation quantum whose elapsed
  // duration reaches the timeout. Deriving the same tick here avoids both
  // browser-clock drift and cumulative setInterval error.
  const timeoutTicks = Math.ceil(timeoutMs / tickDurationMs);
  const deadlineTick = lastActivityTick + timeoutTicks;
  const remainingTicks = Math.max(0, deadlineTick - tick);
  const remainingMs = remainingTicks * tickDurationMs;
  const remainingSeconds = Math.max(1, Math.ceil(remainingMs / 1000));
  const progress = Math.max(0, Math.min(1, remainingMs / warningMs));

  return {
    isKicked,
    warning: {
      deadlineTick,
      remainingMs,
      remainingSeconds,
      progress,
      isUrgent: remainingMs <= Math.min(5_000, warningMs / 2),
    },
  };
};
