import type { GameState, QueueMode } from '../types';
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
}

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
    };
    const score = valueAt(gameState.scores, snakeId);
    const isIdleKicked = userId !== null && idleKickedUserIds.has(userId);
    const isWinner = !isIdleKicked && mode.isTeam && winningTeamId !== null
      ? snake.team_id === winningTeamId
      : !isIdleKicked && snakeId === winningSnakeId;

    return {
      snakeId,
      userId,
      name: isCurrentPlayer
        ? 'You'
        : (userId === null ? null : gameState.usernames?.[userId]) ?? `Player ${snakeId + 1}`,
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
