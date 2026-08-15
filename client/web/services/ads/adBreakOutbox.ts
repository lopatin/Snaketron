import type { AdBreakResolution } from '../../types/generated';

export interface PendingAdBreakResolution {
  breakId: string;
  userId: number | null;
  resolution: AdBreakResolution;
  attempts: number;
  lastAttemptAt: number | null;
}

interface AdBreakOutboxStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

const DEFAULT_STORAGE_KEY = 'snaketron:ad-break-resolution-outbox:v1';
const MAX_PERSISTED_ENTRIES = 32;

/**
 * Idempotent, session-backed outbox. It deliberately keeps an entry after a
 * successful socket write: only an authoritative LobbyUpdate confirming this
 * user resolved (or the lobby leaving the break) proves delivery. Session
 * storage prevents a reload between video completion and ACK delivery from
 * replaying the advertisement.
 */
export class AdBreakResolutionOutbox {
  private readonly entries = new Map<string, PendingAdBreakResolution>();
  private readonly storage: AdBreakOutboxStorage | null;
  private readonly storageKey: string;

  constructor(
    storage: AdBreakOutboxStorage | null = AdBreakResolutionOutbox.browserStorage(),
    storageKey = DEFAULT_STORAGE_KEY,
  ) {
    this.storage = storage;
    this.storageKey = storageKey;
    this.restore();
  }

  enqueue(
    breakId: string,
    resolution: AdBreakResolution,
    userId: number | null = null,
  ): PendingAdBreakResolution {
    const existing = this.entries.get(breakId);
    if (existing && existing.userId === userId) {
      return existing;
    }
    const entry: PendingAdBreakResolution = {
      breakId,
      userId,
      resolution,
      attempts: 0,
      lastAttemptAt: null,
    };
    this.entries.set(breakId, entry);
    while (this.entries.size > MAX_PERSISTED_ENTRIES) {
      const oldest = this.entries.keys().next().value;
      if (typeof oldest !== 'string') break;
      this.entries.delete(oldest);
    }
    this.persist();
    return entry;
  }

  get(breakId: string, userId: number | null = null): PendingAdBreakResolution | null {
    const entry = this.entries.get(breakId);
    return entry && entry.userId === userId ? entry : null;
  }

  markAttempt(breakId: string, now = Date.now()): void {
    const entry = this.entries.get(breakId);
    if (!entry) {
      return;
    }
    entry.attempts += 1;
    entry.lastAttemptAt = now;
    this.persist();
  }

  confirm(breakId: string): void {
    this.entries.delete(breakId);
    this.persist();
  }

  clear(): void {
    this.entries.clear();
    this.persist();
  }

  private static browserStorage(): AdBreakOutboxStorage | null {
    if (typeof window === 'undefined') {
      return null;
    }
    try {
      return window.sessionStorage;
    } catch {
      return null;
    }
  }

  private restore(): void {
    if (!this.storage) return;
    try {
      const raw = this.storage.getItem(this.storageKey);
      if (!raw) return;
      const parsed: unknown = JSON.parse(raw);
      if (!Array.isArray(parsed)) return;
      for (const candidate of parsed.slice(-MAX_PERSISTED_ENTRIES)) {
        if (!candidate || typeof candidate !== 'object') continue;
        const value = candidate as Record<string, unknown>;
        const resolution = value.resolution;
        if (
          typeof value.breakId !== 'string' ||
          !['completed', 'blocked', 'unavailable', 'error', 'timed_out'].includes(String(resolution))
        ) {
          continue;
        }
        const userId = value.userId === null || value.userId === undefined
          ? null
          : Number(value.userId);
        if (userId !== null && !Number.isSafeInteger(userId)) continue;
        this.entries.set(value.breakId, {
          breakId: value.breakId,
          userId,
          resolution: resolution as AdBreakResolution,
          attempts: Math.max(0, Math.trunc(Number(value.attempts) || 0)),
          lastAttemptAt: value.lastAttemptAt !== null && value.lastAttemptAt !== undefined &&
            Number.isFinite(Number(value.lastAttemptAt))
            ? Number(value.lastAttemptAt)
            : null,
        });
      }
    } catch {
      // Storage is an optimization; privacy mode and malformed old data fail open.
    }
  }

  private persist(): void {
    if (!this.storage) return;
    try {
      if (this.entries.size === 0) {
        this.storage.removeItem(this.storageKey);
      } else {
        this.storage.setItem(this.storageKey, JSON.stringify([...this.entries.values()]));
      }
    } catch {
      // An in-memory retry still works when storage is unavailable.
    }
  }
}
