import type { DeathCause, GameState, QueueMode } from '../types';
import type { SnakeSkinInputs } from './snakeSkin';

export const GAME_SHELL_COLORS = {
  graphite: '#172033',
  paper: '#F7F9FB',
  blue: '#3C8DDE',
  red: '#EF5A5A',
  grid: '#C9D3DF',
  boost: '#F8C84A',
} as const;

export interface MatchPlayerPresentation {
  snakeId: number;
  userId: number | null;
  name: string;
  isCurrentPlayer: boolean;
  isAlive: boolean;
  isIdleKicked: boolean;
  teamId: number | null;
  /**
   * The inputs the shared Rust renderer needs to paint this snake. Carrying
   * the selectors rather than resolved hex values is what lets the roster draw
   * a player's real skin instead of a copy that has to be kept in sync.
   */
  skin: SnakeSkinInputs;
  score: number;
  finalLength: number;
  foodHeld: number;
  xpGained: number;
  actionCount: number;
  isWinner: boolean;
  /**
   * Durable explanation of this player's latest death. `null` means there is
   * no death to surface (including banking and legacy/unknown history).
   */
  deathAttribution: string | null;
  /**
   * Pre-match readiness. `null` once the gate has resolved (and for matches
   * that never had one), which is what distinguishes "not ready yet" from
   * "readiness no longer applies" — the roster only draws a check while a
   * gate is actually pending.
   */
  isReady: boolean | null;
}

export interface MatchSidePresentation {
  teamId: number;
  label: string;
  color: string;
  score: number;
  players: MatchPlayerPresentation[];
  isCurrentSide: boolean;
  isWinner: boolean;
}

export type MatchResultTone = 'victory' | 'defeat' | 'draw' | 'complete';

export type MatchResultArtwork =
  | 'azure-cut'
  | 'ruby-shatter'
  | 'jade-fracture'
  | 'topaz-cut'
  | 'neutral';

export interface MatchPresentation {
  modeLabel: string;
  isTeamGame: boolean;
  isSoloGame: boolean;
  isComplete: boolean;
  /**
   * The caption above the match clock. Team matches race to a score with no
   * clock, so the caption carries the target ("First to 25") and the clock
   * below it simply counts up.
   */
  timeLabel: string;
  timeValue: string;
  /** Banked team score that wins the match, when the mode races to one. */
  scoreLimit: number | null;
  elapsedMs: number;
  timeTaken: string;
  pointsPerMinute: number;
  actionsPerMinute: number;
  spectatorCount: number;
  players: MatchPlayerPresentation[];
  currentPlayer: MatchPlayerPresentation | null;
  sides: MatchSidePresentation[];
  soloScore: number;
  resultTitle: string;
  resultSummary: string;
  resultTone: MatchResultTone;
  resultArtwork: MatchResultArtwork;
  /** True while the pre-match readiness gate is still holding the match. */
  isAwaitingReadiness: boolean;
  /** Wall clock the gate gives up waiting, or null when it is not pending. */
  readyDeadlineMs: number | null;
  /** Players who still have to confirm, including the local one. */
  pendingReadyCount: number;
}

/**
 * Wall clock the simulation starts at, mirroring `GameState::simulation_start_ms`
 * in Rust. `null` means the readiness gate still holds the match, so there is
 * no countdown to show yet.
 *
 * `start_ms` is deliberately not used once a gate has resolved: it is the
 * match's durable identity and never moves, so it would keep pointing at a
 * moment that has already passed.
 */
export const simulationStartMs = (
  gameState: Pick<GameState, 'readiness' | 'simulation_epoch_ms' | 'start_ms'>,
): number | null => {
  if (gameState.readiness) {
    return null;
  }
  return gameState.simulation_epoch_ms ?? gameState.start_ms;
};

const valueAt = (
  values: { [key: number]: number | undefined } | null | undefined,
  key: number,
): number => (
  values?.[key] ?? 0
);

export const formatMatchClock = (milliseconds: number, roundUp = false): string => {
  const bounded = Math.max(0, Number.isFinite(milliseconds) ? milliseconds : 0);
  const seconds = roundUp ? Math.ceil(bounded / 1000) : Math.floor(bounded / 1000);
  const minutes = Math.floor(seconds / 60);
  return `${minutes.toString().padStart(2, '0')}:${(seconds % 60).toString().padStart(2, '0')}`;
};

export const calculatePerMinuteRate = (count: number, elapsedMs: number): number => {
  if (!Number.isFinite(count) || count < 0 || !Number.isFinite(elapsedMs) || elapsedMs <= 0) {
    return 0;
  }
  return count * 60_000 / elapsedMs;
};

export const formatPerMinuteRate = (rate: number): string => (
  (Number.isFinite(rate) && rate >= 0 ? rate : 0).toFixed(1)
);

export const isCompleteGameState = (gameState: GameState | null): boolean => Boolean(
  gameState &&
  typeof gameState.status === 'object' &&
  gameState.status !== null &&
  'Complete' in gameState.status,
);

const getWinningSnakeId = (gameState: GameState): number | null => {
  const status = gameState.status;
  if (typeof status === 'object' && status !== null && 'Complete' in status) {
    return status.Complete.winning_snake_id;
  }
  return null;
};

const modeDetails = (gameState: GameState, queueMode: QueueMode) => {
  const gameType = gameState.game_type;
  if (gameType === 'Solo') {
    return { label: 'Solo run', isSolo: true, isTeam: false };
  }
  if ('TeamMatch' in gameType) {
    const size = gameType.TeamMatch.per_team === 1 ? 'Duel' : '2v2';
    return {
      label: queueMode === 'Competitive' ? `Competitive ${size}` : `Quick ${size}`,
      isSolo: false,
      isTeam: true,
    };
  }
  if ('FreeForAll' in gameType) {
    return {
      label: queueMode === 'Competitive' ? 'Competitive FFA' : 'Free for all',
      isSolo: false,
      isTeam: false,
    };
  }

  const customMode = gameType.Custom.settings.game_mode;
  if (customMode === 'Solo') {
    return { label: 'Custom solo', isSolo: true, isTeam: false };
  }
  if (customMode === 'Duel') {
    return { label: 'Custom duel', isSolo: false, isTeam: false };
  }
  return { label: 'Custom FFA', isSolo: false, isTeam: false };
};

/**
 * Turns durable simulation attribution into the compact line rendered below
 * a player's name on the final standings. Team matches respawn, so player-
 * caused deaths are demolitions; field modes end a life and use eliminated.
 */
export const buildDeathAttribution = (
  cause: DeathCause | undefined,
  isRespawningTeamPlay: boolean,
  resolveSnakeName: (snakeId: number) => string,
): string | null => {
  if (!cause || cause === 'Unknown' || cause === 'Banked') {
    return null;
  }

  const action = isRespawningTeamPlay ? 'Demolished' : 'Eliminated';

  if (typeof cause === 'object') {
    const attributedSnakeId = 'SnakeBody' in cause
      ? cause.SnakeBody.killer_snake_id
      : cause.HeadToHead.other_snake_id;
    return `${action} by ${resolveSnakeName(attributedSnakeId)}`;
  }

  switch (cause) {
    case 'Wall':
      return `${action} by wall`;
    case 'OutOfBounds':
      return `${action} by boundary`;
    case 'EnemyBase':
      return `${action} by enemy base`;
    case 'SelfCollision':
      return `${action} by yourself`;
  }

  return null;
};

export const buildMatchPresentation = (
  gameState: GameState,
  currentUserId?: number,
  queueMode: QueueMode = gameState.queue_mode,
): MatchPresentation => {
  const mode = modeDetails(gameState, queueMode);
  const idleKickedUserIds = new Set(gameState.idle_kicked_user_ids ?? []);
  const playerBySnake = new Map<number, number>();
  for (const [rawUserId, player] of Object.entries(gameState.players ?? {})) {
    if (player) {
      playerBySnake.set(player.snake_id, Number(rawUserId));
    }
  }

  const currentSnakeId = currentUserId === undefined
    ? null
    : gameState.players?.[currentUserId]?.snake_id ?? null;
  const resolveSnakeName = (snakeId: number): string => {
    const userId = playerBySnake.get(snakeId) ?? null;
    if (userId !== null && userId === currentUserId) {
      return 'You';
    }
    return (userId === null ? null : gameState.usernames?.[userId]) ?? `Player ${snakeId + 1}`;
  };
  const currentTeamId = currentSnakeId === null
    ? null
    : gameState.arena.snakes[currentSnakeId]?.team_id ?? null;
  const teamIds = Array.from(new Set(
    gameState.arena.snakes
      .map((snake) => snake.team_id)
      .filter((teamId): teamId is number => teamId !== null),
  )).sort((a, b) => a - b);
  const orderedTeamIds = mode.isTeam && currentTeamId !== null
    ? [currentTeamId, ...teamIds.filter((teamId) => teamId !== currentTeamId)]
    : teamIds;
  const readyUserIds = gameState.readiness
    ? new Set(gameState.readiness.ready_user_ids)
    : null;
  const winningSnakeId = getWinningSnakeId(gameState);
  const winningTeamId = winningSnakeId === null
    ? null
    : gameState.arena.snakes[winningSnakeId]?.team_id ?? null;

  // The renderer keys team shades off how many earlier snakes share a team, and
  // decides blue-vs-red from the viewer's own team, so both are passed through
  // verbatim rather than being re-derived into colours here.
  const isTeamSkin = gameState.arena.team_zone_config !== null;

  const players: MatchPlayerPresentation[] = gameState.arena.snakes.map((snake, snakeId) => {
    const userId = playerBySnake.get(snakeId) ?? null;
    const isCurrentPlayer = userId !== null && userId === currentUserId;
    const skinRef = userId === null ? undefined : gameState.skins?.[userId];
    const skin: SnakeSkinInputs = {
      snake_index: snakeId,
      team_id: snake.team_id,
      team_member_slot: gameState.arena.snakes
        .slice(0, snakeId)
        .filter((candidate) => candidate.team_id === snake.team_id)
        .length,
      snake_count: gameState.arena.snakes.length,
      is_team_game: isTeamSkin,
      local_snake_id: currentSnakeId,
      local_team_id: currentTeamId,
      // The skin this player is actually wearing, so the results swatch and the
      // roster portrait agree with what the arena drew. Absent for a snake with
      // no user (a bot, a vacated slot), which reads as the classic look.
      //
      // Spread rather than assigned, so "no skin" is a missing key rather than
      // a present one holding `undefined`. The two serialize identically and
      // are different objects, which is exactly the sort of difference a
      // structural comparison notices and a reader does not.
      ...(skinRef === undefined ? {} : { skin_ref: skinRef }),
    };
    const score = valueAt(gameState.scores, snakeId);
    const isIdleKicked = userId !== null && idleKickedUserIds.has(userId);
    const isWinner = !isIdleKicked && mode.isTeam && winningTeamId !== null
      ? snake.team_id === winningTeamId
      : !isIdleKicked && snakeId === winningSnakeId;

    return {
      snakeId,
      userId,
      name: resolveSnakeName(snakeId),
      isCurrentPlayer,
      isAlive: snake.is_alive,
      isIdleKicked,
      teamId: snake.team_id,
      skin,
      score,
      finalLength: score + 2,
      foodHeld: snake.food,
      xpGained: userId === null ? 0 : valueAt(gameState.player_xp, userId),
      actionCount: userId === null ? 0 : valueAt(gameState.player_action_counts, userId),
      isWinner,
      deathAttribution: buildDeathAttribution(
        gameState.last_death_causes?.[snakeId],
        mode.isTeam,
        resolveSnakeName,
      ),
      isReady: readyUserIds === null || userId === null
        ? null
        : readyUserIds.has(userId),
    };
  });

  const sides: MatchSidePresentation[] = mode.isTeam
    ? orderedTeamIds.slice(0, 2).map((teamId, index) => ({
      teamId,
      label: currentTeamId === null
        ? (index === 0 ? 'Blue side' : 'Red side')
        : (teamId === currentTeamId ? 'Your team' : 'Opponents'),
      color: index === 0 ? GAME_SHELL_COLORS.blue : GAME_SHELL_COLORS.red,
      score: valueAt(gameState.team_scores, teamId),
      players: players.filter((player) => player.teamId === teamId),
      isCurrentSide: teamId === currentTeamId,
      isWinner: winningTeamId === teamId,
    }))
    : [];

  const elapsedMs = gameState.tick * gameState.properties.tick_duration_ms;
  const timeLimitMs = gameState.properties.time_limit_ms;
  const scoreLimit = gameState.properties.score_limit;
  const timeValue = timeLimitMs === null
    ? formatMatchClock(elapsedMs)
    : formatMatchClock(timeLimitMs - elapsedMs, true);
  const currentPlayer = players.find((player) => player.isCurrentPlayer) ?? null;
  const soloScore = currentPlayer?.score ?? players[0]?.score ?? 0;
  const pointsPerMinute = calculatePerMinuteRate(currentPlayer?.score ?? 0, elapsedMs);
  const actionsPerMinute = calculatePerMinuteRate(currentPlayer?.actionCount ?? 0, elapsedMs);

  let resultTitle = 'Match complete';
  let resultSummary = 'Final scores are in.';
  let resultTone: MatchResultTone = 'complete';
  let resultArtwork: MatchResultArtwork = 'neutral';
  if (gameState.completed_by_inactivity === true) {
    if (currentPlayer?.isIdleKicked) {
      resultTitle = 'Removed';
      resultSummary = 'You were removed for inactivity.';
      resultTone = 'defeat';
      resultArtwork = 'ruby-shatter';
    } else if (winningSnakeId === null) {
      resultTitle = 'Match ended';
      resultSummary = 'Every active player was removed for inactivity.';
      resultTone = 'draw';
      resultArtwork = 'topaz-cut';
    } else if (currentPlayer) {
      const didWin = currentPlayer.isWinner;
      resultTitle = didWin ? 'Victory' : 'Match complete';
      resultSummary = didWin
        ? 'The other side was removed for inactivity.'
        : 'Inactivity ended the match.';
      resultTone = didWin ? 'victory' : 'complete';
      resultArtwork = didWin ? 'azure-cut' : 'neutral';
    } else {
      resultSummary = 'Inactivity ended the match.';
    }
  } else if (mode.isSolo) {
    resultTitle = 'Run complete';
    resultSummary = `You finished with ${soloScore} point${soloScore === 1 ? '' : 's'}.`;
    resultArtwork = 'jade-fracture';
  } else if (winningSnakeId === null) {
    resultTitle = 'Draw';
    resultSummary = 'Neither side could pull ahead.';
    resultTone = 'draw';
    resultArtwork = 'topaz-cut';
  } else if (currentPlayer) {
    const didWin = currentPlayer.isWinner;
    resultTitle = didWin ? 'Victory' : 'Defeat';
    resultSummary = didWin ? 'Your side got there first.' : 'The other side got there first.';
    resultTone = didWin ? 'victory' : 'defeat';
    resultArtwork = didWin ? 'azure-cut' : 'ruby-shatter';
  } else {
    const winner = players.find((player) => player.isWinner);
    resultSummary = winner ? `${winner.name} took the match.` : 'Final scores are in.';
  }

  return {
    modeLabel: mode.label,
    isTeamGame: mode.isTeam,
    isSoloGame: mode.isSolo,
    isComplete: isCompleteGameState(gameState),
    timeLabel: scoreLimit !== null
      ? `First to ${scoreLimit}`
      : timeLimitMs === null ? 'Time' : 'Time left',
    timeValue,
    scoreLimit,
    elapsedMs,
    timeTaken: formatMatchClock(elapsedMs),
    pointsPerMinute,
    actionsPerMinute,
    spectatorCount: gameState.spectators?.length ?? 0,
    players,
    currentPlayer,
    sides,
    soloScore,
    resultTitle,
    resultSummary,
    resultTone,
    resultArtwork,
    isAwaitingReadiness: gameState.readiness !== null,
    readyDeadlineMs: gameState.readiness?.deadline_ms ?? null,
    pendingReadyCount: players.filter(
      (player) => player.userId !== null && player.isReady === false,
    ).length,
  };
};

export type PlayAgainShortcutAction = 'ignore' | 'suppress' | 'play-again';

interface ShortcutEventLike {
  code: string;
  repeat: boolean;
  target: EventTarget | null;
}

const targetOwnsSpace = (target: EventTarget | null): boolean => {
  if (!target || typeof target !== 'object') {
    return false;
  }
  const element = target as EventTarget & {
    tagName?: unknown;
    isContentEditable?: unknown;
  };
  const tagName = typeof element.tagName === 'string' ? element.tagName.toUpperCase() : '';
  return ['INPUT', 'TEXTAREA', 'SELECT', 'BUTTON', 'A'].includes(tagName) ||
    element.isContentEditable === true;
};

export const getPlayAgainShortcutAction = (
  event: ShortcutEventLike,
  modalOpen: boolean,
  disabled: boolean,
): PlayAgainShortcutAction => {
  if (!modalOpen || event.code !== 'Space' || targetOwnsSpace(event.target)) {
    return 'ignore';
  }
  if (event.repeat || disabled) {
    return 'suppress';
  }
  return 'play-again';
};
