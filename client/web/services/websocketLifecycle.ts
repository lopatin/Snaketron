export const RECONNECT_BASE_MS = 100;
export const RECONNECT_MAX_MS = 2000;
export const PLANNED_HANDOFF_MAX_MS = 20_000;
export const PLANNED_HANDOFF_MIN_MS = 2_000;
export const REQUIRED_SERVER_CAPABILITIES = [
  'explicit-auth-v1',
  'planned-drain-v1',
  'socket-generation-v1',
  'command-delivery-v2',
  'command-outcomes-v1',
  'command-outcome-barrier-v1',
] as const;

export type ReplacementFailureAction = 'retry-candidate' | 'reconnect-active' | 'none';

export interface ReplacementReadiness {
  socketOpen: boolean;
  authenticated: boolean;
  inFlightRequestCount: number;
  lobbyReady: boolean;
  gameReady: boolean;
  gameComplete: boolean;
  commandOutcomesReady: boolean;
  expectsGame: boolean;
  gameStreamWatermark: number | null;
}

export function missingRequiredServerCapabilities(capabilities: readonly string[]): string[] {
  const advertised = new Set(capabilities);
  return REQUIRED_SERVER_CAPABILITIES.filter((capability) => !advertised.has(capability));
}

export function isCommandOwner(
  activeGeneration: number | null,
  targetGeneration: number,
  role: 'active' | 'candidate' | 'retired',
  readyState: number,
): boolean {
  return activeGeneration === targetGeneration && role === 'active' && readyState === 1;
}

/** First retry is immediate; later retries use bounded full-width jitter. */
export function reconnectDelayMs(attempt: number, random: () => number = Math.random): number {
  if (attempt <= 0) {
    return 0;
  }
  const exponential = Math.min(RECONNECT_MAX_MS, RECONNECT_BASE_MS * (2 ** (attempt - 1)));
  const jitter = 0.5 + Math.max(0, Math.min(1, random()));
  return Math.min(RECONNECT_MAX_MS, Math.round(exponential * jitter));
}

/** Decide recovery without ever leaving a connectionless client idle. */
export function replacementFailureAction(
  hasUsableActive: boolean,
  reconnectEnabled: boolean,
  drainDeadlineMs: number | null,
  nowMs: number,
): ReplacementFailureAction {
  if (!reconnectEnabled) {
    return 'none';
  }
  if (!hasUsableActive) {
    return 'reconnect-active';
  }
  return drainDeadlineMs !== null && nowMs < drainDeadlineMs
    ? 'retry-candidate'
    : 'none';
}

export function candidateDeadlineDelayMs(deadlineMs: number, nowMs: number): number {
  return Math.max(0, deadlineMs - nowMs);
}

/**
 * Convert the server's absolute drain deadline into a bounded local duration.
 * A synchronized offset is server-minus-client. Without one, only trust the
 * browser-clock subtraction when it already falls inside the protocol's
 * 20-second handoff window; otherwise use that full window. Every valid notice
 * receives at least one reconnect interval, including genuinely late notices.
 */
export function plannedDrainRemainingMs(
  serverDeadlineMs: number,
  clientNowMs: number,
  serverClockOffsetMs: number | null,
): number | null {
  if (
    !Number.isSafeInteger(serverDeadlineMs) ||
    serverDeadlineMs <= 0 ||
    !Number.isFinite(clientNowMs)
  ) {
    return null;
  }

  const hasSynchronizedClock =
    serverClockOffsetMs !== null && Number.isFinite(serverClockOffsetMs);
  const estimatedRemainingMs = hasSynchronizedClock
    ? serverDeadlineMs - (clientNowMs + serverClockOffsetMs)
    : serverDeadlineMs - clientNowMs;
  const remainingMs = hasSynchronizedClock || (
    estimatedRemainingMs > 0 && estimatedRemainingMs <= PLANNED_HANDOFF_MAX_MS
  )
    ? estimatedRemainingMs
    : PLANNED_HANDOFF_MAX_MS;

  return Math.max(
    PLANNED_HANDOFF_MIN_MS,
    Math.min(PLANNED_HANDOFF_MAX_MS, Math.round(remainingMs)),
  );
}

export function activeGameIdFromPath(pathname: string): number | null {
  const match = pathname.match(/^\/play\/(\d+)(?:\/|$)/);
  if (!match) {
    return null;
  }
  const value = Number(match[1]);
  return Number.isInteger(value) && value >= 0 && value <= 0xffff_ffff ? value : null;
}

export function isSnapshotForGame(rawMessage: any, gameId: number): boolean {
  const event = rawMessage?.GameEvent;
  return (
    Number(event?.game_id) === gameId &&
    event?.event &&
    typeof event.event === 'object' &&
    Object.prototype.hasOwnProperty.call(event.event, 'Snapshot')
  );
}

export function isTerminalSnapshotForGame(rawMessage: any, gameId: number): boolean {
  if (!isSnapshotForGame(rawMessage, gameId)) {
    return false;
  }
  const status = rawMessage.GameEvent.event.Snapshot?.game_state?.status;
  return Boolean(
    status &&
    typeof status === 'object' &&
    Object.prototype.hasOwnProperty.call(status, 'Complete'),
  );
}

export function isFreshSnapshotForGame(
  rawMessage: any,
  gameId: number,
  minimumStreamSequence: number | null,
): boolean {
  if (!isSnapshotForGame(rawMessage, gameId)) {
    return false;
  }
  if (minimumStreamSequence === null) {
    return true;
  }
  if (isTerminalSnapshotForGame(rawMessage, gameId)) {
    return true;
  }
  const streamSequence = Number(rawMessage?.GameEvent?.stream_seq);
  return Number.isSafeInteger(streamSequence) && streamSequence >= minimumStreamSequence;
}

/**
 * Advance only across a contiguous candidate event stream. A snapshot is an
 * authoritative re-anchor; a delta may advance the watermark by exactly one.
 * WebSocket ordering alone is insufficient because the gateway broadcaster
 * can explicitly resnapshot after lag.
 */
export function advanceCandidateGameWatermark(
  current: number | null,
  rawMessage: any,
  gameId: number,
): number | null {
  const event = rawMessage?.GameEvent;
  if (Number(event?.game_id) !== gameId) {
    return current;
  }
  const streamSequence = Number(event?.stream_seq);
  if (!Number.isSafeInteger(streamSequence) || streamSequence < 0) {
    return current;
  }
  if (
    event?.event &&
    typeof event.event === 'object' &&
    Object.prototype.hasOwnProperty.call(event.event, 'Snapshot')
  ) {
    return streamSequence;
  }
  if (current === null || streamSequence <= current) {
    return current;
  }
  return streamSequence === current + 1 ? streamSequence : current;
}

export function candidateCoversActiveWatermark(
  candidateWatermark: number | null,
  activeWatermark: number | null,
): boolean {
  return candidateWatermark !== null && (
    activeWatermark === null || candidateWatermark >= activeWatermark
  );
}

export function replacementReadyForPromotion(
  readiness: ReplacementReadiness,
  activeWatermark: number | null,
): boolean {
  return readiness.socketOpen &&
    readiness.authenticated &&
    readiness.inFlightRequestCount === 0 &&
    readiness.lobbyReady &&
    readiness.gameReady &&
    readiness.commandOutcomesReady &&
    (
      !readiness.expectsGame ||
      readiness.gameComplete ||
      candidateCoversActiveWatermark(readiness.gameStreamWatermark, activeWatermark)
    );
}
