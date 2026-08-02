import type {
  ClientCommandIdentityV2,
  CommandOutcomesPayload,
  GameCommand,
  GameCommandV2,
} from '../types';

interface GameSession {
  id: string;
  nextSequence: number;
  pending: Map<number, PendingCommand>;
  terminalAwaitingOutcomeBarrier?: boolean;
  rejectionFence?: {
    fromSequence: number;
  };
}

interface PendingCommand {
  envelope: GameCommandV2;
  lastSentAtMs: number;
}

type SessionIdFactory = () => string;

export type TerminalOutboxCompletion = 'not-terminal' | 'pending' | 'cleared';

// Shared bounded protocol/storage budget with the server's exact-result
// window. Disconnected or resynchronizing clients do not create identities.
export const MAX_PENDING_COMMANDS_PER_GAME_SESSION = 128;

const defaultSessionIdFactory: SessionIdFactory = () => {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  // `randomUUID` is available in supported browsers. This fallback keeps local
  // development functional in older WebViews without weakening server-side
  // authentication or command ownership.
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
};

function sessionKey(gameId: number, userId: number): string {
  return `${gameId}:${userId}`;
}

function identityMatches(
  left: ClientCommandIdentityV2,
  right: ClientCommandIdentityV2,
): boolean {
  return (
    left.game_id === right.game_id &&
    left.user_id === right.user_id &&
    left.client_game_session_id === right.client_game_session_id &&
    left.sequence === right.sequence
  );
}

/**
 * Recovery resends are safe only after a fresh snapshot and the executor's
 * complete outcome replay. Missing capability is fail-closed because the
 * current client and server are deployed as one protocol.
 */
export function recoveryOutcomesReadyForResend(
  gameId: number,
  snapshotSynchronized: boolean,
  serverCapabilities: ReadonlySet<string>,
  completedOutcomeBarriers: ReadonlySet<number>,
): boolean {
  return snapshotSynchronized
    && serverCapabilities.has('command-outcome-barrier-v1')
    && completedOutcomeBarriers.has(gameId);
}

/**
 * A terminal event parks the outbox until the server's immediately following
 * durable outcome replay and completion barrier have been processed.
 */
export function gameEventTerminatesCommandOutbox(event: unknown): boolean {
  if (!event || typeof event !== 'object') {
    return false;
  }

  const gameEvent = event as {
    Snapshot?: { game_state?: { status?: unknown } };
    StatusUpdated?: { status?: unknown };
  };
  const status = gameEvent.Snapshot?.game_state?.status
    ?? gameEvent.StatusUpdated?.status;
  return Boolean(
    status &&
    typeof status === 'object' &&
    Object.prototype.hasOwnProperty.call(status, 'Complete'),
  );
}

export type GameLoadProtocolMessage = 'GameLoadFailed' | 'GameWarming';
export type GameLoadOutboxAction = 'ignore' | 'preserve-and-retry' | 'clear-terminal';

/**
 * `GameLoadFailed` is the protocol's definitive terminal result; transient
 * dependency failures use `GameWarming`. Neither message may affect a stale
 * game after navigation has changed the active request.
 */
export function gameLoadOutboxAction(
  messageType: GameLoadProtocolMessage,
  messageGameId: number,
  activeGameId: number | null,
): GameLoadOutboxAction {
  if (activeGameId !== messageGameId) {
    return 'ignore';
  }
  return messageType === 'GameLoadFailed' ? 'clear-terminal' : 'preserve-and-retry';
}

/**
 * Per-tab, per-game at-least-once command outbox. Entries are stored before
 * transport send and removed only by an executor-authored terminal result.
 */
export class GameCommandOutbox {
  private readonly sessions = new Map<string, GameSession>();
  private readonly sessionIdFactory: SessionIdFactory;

  constructor(sessionIdFactory: SessionIdFactory = defaultSessionIdFactory) {
    this.sessionIdFactory = sessionIdFactory;
  }

  private getOrCreateSession(gameId: number, userId: number): GameSession {
    const key = sessionKey(gameId, userId);
    let session = this.sessions.get(key);
    if (!session) {
      session = {
        id: this.sessionIdFactory(),
        nextSequence: 1,
        pending: new Map(),
      };
      this.sessions.set(key, session);
    }
    return session;
  }

  private retireFencedSessionIfDrained(
    key: string,
    session: GameSession,
  ): void {
    if (
      session.rejectionFence &&
      session.pending.size === 0 &&
      !session.terminalAwaitingOutcomeBarrier
    ) {
      this.sessions.delete(key);
    }
  }

  private installRejectionFence(
    key: string,
    session: GameSession,
    fromSequence: number,
  ): number {
    if (!Number.isSafeInteger(fromSequence) || fromSequence < 1) {
      this.retireFencedSessionIfDrained(key, session);
      return 0;
    }
    if (
      !session.rejectionFence ||
      fromSequence < session.rejectionFence.fromSequence
    ) {
      session.rejectionFence = { fromSequence };
    }

    let removed = 0;
    const effectiveFrom = session.rejectionFence.fromSequence;
    for (const sequence of session.pending.keys()) {
      if (sequence >= effectiveFrom) {
        session.pending.delete(sequence);
        removed += 1;
      }
    }
    this.retireFencedSessionIfDrained(key, session);
    return removed;
  }

  enqueue(
    gameId: number,
    userId: number,
    command: GameCommand,
    nowMs: number = Date.now(),
  ): GameCommandV2 {
    const session = this.getOrCreateSession(gameId, userId);
    if (session.terminalAwaitingOutcomeBarrier) {
      throw new Error('terminal game is awaiting command outcomes');
    }
    if (session.rejectionFence) {
      throw new Error('client game command session rejected');
    }
    if (session.pending.size >= MAX_PENDING_COMMANDS_PER_GAME_SESSION) {
      throw new Error('pending game command capacity exhausted');
    }
    const earliestPendingSequence = session.pending.keys().next().value;
    if (
      earliestPendingSequence !== undefined &&
      session.nextSequence - earliestPendingSequence >
        MAX_PENDING_COMMANDS_PER_GAME_SESSION
    ) {
      throw new Error('pending game command sequence window exhausted');
    }
    if (session.nextSequence > Number.MAX_SAFE_INTEGER) {
      throw new Error('client command sequence exhausted');
    }
    const identity: ClientCommandIdentityV2 = {
      game_id: gameId,
      user_id: userId,
      client_game_session_id: session.id,
      sequence: session.nextSequence++,
    };
    const envelope: GameCommandV2 = {
      command_id: identity,
      command: {
        ...command,
        command_id_client: { ...command.command_id_client },
      },
    };
    session.pending.set(identity.sequence, { envelope, lastSentAtMs: nowMs });
    return envelope;
  }

  resolve(identity: ClientCommandIdentityV2): boolean {
    const key = sessionKey(identity.game_id, identity.user_id);
    const session = this.sessions.get(key);
    if (!session || session.id !== identity.client_game_session_id) {
      return false;
    }
    const pending = session.pending.get(identity.sequence);
    if (!pending || !identityMatches(pending.envelope.command_id, identity)) {
      return false;
    }
    session.pending.delete(identity.sequence);
    this.retireFencedSessionIfDrained(key, session);
    return true;
  }

  reject(
    identity: ClientCommandIdentityV2,
    sessionRejectedFrom: number | undefined,
  ): number {
    const key = sessionKey(identity.game_id, identity.user_id);
    const session = this.sessions.get(key);
    if (!session || session.id !== identity.client_game_session_id) {
      return 0;
    }

    // The event's exact outcome is authoritative even when it also announces
    // a wider fence.
    let removed = 0;
    const pending = session.pending.get(identity.sequence);
    if (pending && identityMatches(pending.envelope.command_id, identity)) {
      session.pending.delete(identity.sequence);
      removed += 1;
    }
    if (sessionRejectedFrom !== undefined) {
      removed += this.installRejectionFence(
        key,
        session,
        sessionRejectedFrom,
      );
    } else {
      this.retireFencedSessionIfDrained(key, session);
    }
    return removed;
  }

  reconcile(payload: CommandOutcomesPayload, userId: number): number {
    const key = sessionKey(payload.game_id, userId);
    const session = this.sessions.get(key);
    if (!session || session.id !== payload.client_game_session_id) {
      return 0;
    }

    const outcomes = payload.outcomes && typeof payload.outcomes === 'object'
      ? payload.outcomes
      : {};
    const contiguousThrough = Number.isSafeInteger(payload.contiguous_through)
      ? payload.contiguous_through
      : 0;
    const explicitlyResolved = new Set(
      Object.keys(outcomes)
        .map(Number)
        .filter(Number.isSafeInteger),
    );
    let removed = 0;
    for (const sequence of session.pending.keys()) {
      if (sequence <= contiguousThrough || explicitlyResolved.has(sequence)) {
        session.pending.delete(sequence);
        removed += 1;
      }
    }
    // Exact outcomes and the contiguous watermark are applied before the
    // terminal session fence.
    const fence = payload.rejection_fence;
    if (fence) {
      removed += this.installRejectionFence(
        key,
        session,
        fence.from_sequence,
      );
    } else {
      this.retireFencedSessionIfDrained(key, session);
    }
    return removed;
  }

  pending(gameId: number, userId: number): GameCommandV2[] {
    const session = this.sessions.get(sessionKey(gameId, userId));
    return session
      ? [...session.pending.values()].map((pending) => pending.envelope).sort(
          (left, right) => left.command_id.sequence - right.command_id.sequence,
        )
      : [];
  }

  takeDue(
    gameId: number,
    userId: number,
    nowMs: number,
    retryIntervalMs: number,
  ): GameCommandV2[] {
    const session = this.sessions.get(sessionKey(gameId, userId));
    if (!session || session.terminalAwaitingOutcomeBarrier) {
      return [];
    }
    const due: GameCommandV2[] = [];
    for (const pending of session.pending.values()) {
      if (
        session.rejectionFence &&
        pending.envelope.command_id.sequence >=
          session.rejectionFence.fromSequence
      ) {
        continue;
      }
      if (nowMs - pending.lastSentAtMs >= retryIntervalMs) {
        pending.lastSentAtMs = nowMs;
        due.push(pending.envelope);
      }
    }
    return due.sort((left, right) => left.command_id.sequence - right.command_id.sequence);
  }

  markTerminal(gameId: number, userId: number): boolean {
    const key = sessionKey(gameId, userId);
    let session = this.sessions.get(key);
    if (!session) {
      session = {
        id: this.sessionIdFactory(),
        nextSequence: 1,
        pending: new Map(),
      };
      this.sessions.set(key, session);
    }
    session.terminalAwaitingOutcomeBarrier = true;
    return true;
  }

  completeTerminal(
    gameId: number,
    userId: number,
    terminalRejectionReason?: string,
  ): TerminalOutboxCompletion {
    const session = this.sessions.get(sessionKey(gameId, userId));
    if (!session?.terminalAwaitingOutcomeBarrier) {
      return 'not-terminal';
    }
    if (session.pending.size > 0) {
      if (!terminalRejectionReason?.trim()) {
        return 'pending';
      }
      // The terminal barrier is an explicit disposition for identities that
      // crossed completion while terminal delivery was already buffered.
      // Exact scheduled/rejected outcomes were applied first; every identity
      // left here is authoritatively rejected for this terminal reason.
      session.pending.clear();
    }
    // Keep the tombstone after successful reconciliation. React can process
    // this barrier before the queued terminal event reaches the game engine;
    // only explicit leave/navigation may make this game commandable again.
    return 'cleared';
  }

  clear(gameId: number, userId: number): void {
    this.sessions.delete(sessionKey(gameId, userId));
  }
}

const browserOutbox = new GameCommandOutbox();

export function enqueueGameCommandV2(
  gameId: number,
  userId: number,
  command: GameCommand,
): GameCommandV2 {
  return browserOutbox.enqueue(gameId, userId, command);
}

export function resolveGameCommandV2(identity: ClientCommandIdentityV2): boolean {
  return browserOutbox.resolve(identity);
}

export function rejectGameCommandV2(
  identity: ClientCommandIdentityV2,
  sessionRejectedFrom: number | undefined,
): number {
  return browserOutbox.reject(identity, sessionRejectedFrom);
}

export function reconcileGameCommandOutcomes(
  payload: CommandOutcomesPayload,
  userId: number,
): number {
  return browserOutbox.reconcile(payload, userId);
}

export function pendingGameCommandsV2(gameId: number, userId: number): GameCommandV2[] {
  return browserOutbox.pending(gameId, userId);
}

export function takeGameCommandsDueForRetry(
  gameId: number,
  userId: number,
  nowMs: number,
  retryIntervalMs: number,
): GameCommandV2[] {
  return browserOutbox.takeDue(gameId, userId, nowMs, retryIntervalMs);
}

export function markGameCommandOutboxTerminal(
  gameId: number,
  userId: number,
): boolean {
  return browserOutbox.markTerminal(gameId, userId);
}

export function completeGameCommandOutboxTerminal(
  gameId: number,
  userId: number,
  terminalRejectionReason?: string,
): TerminalOutboxCompletion {
  return browserOutbox.completeTerminal(gameId, userId, terminalRejectionReason);
}

export function clearGameCommandOutbox(gameId: number, userId: number): void {
  browserOutbox.clear(gameId, userId);
}
