/**
 * Client-side sync trace ("flight recorder") ring buffer.
 *
 * Records are shaped EXACTLY like the serde JSON of
 * `common::trace::TraceRecord` (externally tagged), so a downloaded or
 * uploaded client trace can be joined with a server trace by the offline
 * RCA tooling without any conversion.
 */

import { api } from '../services/api';
import { DEFAULT_TICK_INTERVAL_MS } from '../constants';

export const TRACE_FORMAT_VERSION = 1;
const MAX_RECORDS = 20000;

export type TraceRecord =
  | {
      Meta: {
        version: number;
        side: 'Client' | 'Server';
        game_id: number;
        session: string;
        ts_ms: number;
        build: string;
        tick_duration_ms: number;
      };
    }
  | { EventIn: { ts_ms: number; committed_tick: number; msg: any } }
  | { CmdOut: { ts_ms: number; predicted_tick: number; cmd: any } }
  | { Fingerprint: { ts_ms: number; tick: number; hash: number } }
  | { Clock: { ts_ms: number; drift_ms: number; rtt_ms: number } }
  | { Note: { ts_ms: number; note: string } };

let records: TraceRecord[] = [];
let currentGameId: number | null = null;
let currentUserId: number | null = null;
let autoUploadTriggered = false;

/**
 * Reset the recorder for a new game and write the Meta record.
 * Also re-arms the once-per-game auto-upload guard.
 */
export function startTrace(
  gameId: number,
  userId: number,
  tickDurationMs: number = DEFAULT_TICK_INTERVAL_MS
): void {
  records = [];
  currentGameId = gameId;
  currentUserId = userId;
  autoUploadTriggered = false;

  records.push({
    Meta: {
      version: TRACE_FORMAT_VERSION,
      side: 'Client',
      game_id: gameId,
      session: `user_${userId}`,
      ts_ms: Date.now(),
      build: `web-${process.env.NODE_ENV ?? 'unknown'}`,
      tick_duration_ms: tickDurationMs,
    },
  });
}

/**
 * Append a record. No-op until startTrace has been called. When the buffer
 * is full the oldest non-Meta record is evicted so the Meta header (game id,
 * session, tick duration) survives for the RCA tooling.
 */
export function record(rec: TraceRecord): void {
  if (currentGameId === null) {
    return;
  }
  records.push(rec);
  if (records.length > MAX_RECORDS) {
    records.splice(1, 1);
  }
}

/** Download the current trace as game_<id>_client.jsonl. */
export function downloadTrace(): void {
  if (currentGameId === null || records.length === 0) {
    console.warn('No sync trace to download');
    return;
  }

  const jsonl = records.map((rec) => JSON.stringify(rec)).join('\n') + '\n';
  const blob = new Blob([jsonl], { type: 'application/x-ndjson' });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = `game_${currentGameId}_client.jsonl`;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  URL.revokeObjectURL(url);
}

/** Upload the current trace to the server-side debug collector. */
export async function uploadTrace(): Promise<void> {
  if (currentGameId === null || records.length === 0) {
    console.warn('No sync trace to upload');
    return;
  }

  // Snapshot before the async call so a concurrent startTrace can't mix games
  const payload = {
    game_id: currentGameId,
    user_id: currentUserId,
    records: [...records],
  };

  try {
    await api.request('/api/debug/client-trace', {
      method: 'POST',
      body: JSON.stringify(payload),
    });
    console.log(`Uploaded sync trace for game ${payload.game_id} (${payload.records.length} records)`);
  } catch (error) {
    console.warn('Failed to upload sync trace:', error);
    throw error;
  }
}

/**
 * Upload the trace at most once per game (re-armed by startTrace). Used to
 * automatically capture the first desync of a game without spamming the
 * collector on every subsequent anomaly.
 */
export function autoUploadOnce(reason: string): void {
  if (currentGameId === null || autoUploadTriggered) {
    return;
  }
  autoUploadTriggered = true;
  record({ Note: { ts_ms: Date.now(), note: `auto-uploading trace: ${reason}` } });
  uploadTrace().catch(() => {
    // Already logged; keep the guard set so we don't retry-loop on failure
  });
}

declare global {
  interface Window {
    snaketronDebug?: {
      downloadTrace: () => void;
      uploadTrace: () => Promise<void>;
    };
  }
}

if (typeof window !== 'undefined') {
  window.snaketronDebug = { downloadTrace, uploadTrace };
}
