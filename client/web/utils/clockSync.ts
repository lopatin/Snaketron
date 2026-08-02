/**
 * Clock synchronization utility for NTP-style time synchronization
 * between client and server to compensate for clock drift.
 */

interface SyncMeasurement {
  offset: number;
  rtt: number;
  timestamp: number;
}

type ClockSyncTimeout = ReturnType<typeof setTimeout>;

export interface ClockSyncOptions {
  now?: () => number;
  scheduleTimeout?: (callback: () => void, delayMs: number) => ClockSyncTimeout;
  cancelTimeout?: (timeout: ClockSyncTimeout) => void;
}

export class ClockSync {
  private measurements: SyncMeasurement[] = [];
  private maxMeasurements = 3;
  private initialSyncCount = 3;
  private syncsSent = 0;
  private syncInterval = 5000; // 5 seconds
  private onSyncRequest: ((clientTime: number) => void) | null = null;
  private timeout: ClockSyncTimeout | null = null;
  private readonly now: () => number;
  private readonly scheduleTimeout: (callback: () => void, delayMs: number) => ClockSyncTimeout;
  private readonly cancelTimeout: (timeout: ClockSyncTimeout) => void;

  constructor(options: ClockSyncOptions = {}) {
    this.now = options.now ?? (() => Date.now());
    this.scheduleTimeout = options.scheduleTimeout ?? ((callback, delayMs) => (
      setTimeout(callback, delayMs)
    ));
    this.cancelTimeout = options.cancelTimeout ?? ((timeout) => clearTimeout(timeout));
  }

  /**
   * Set the callback to send sync requests
   */
  setOnSyncRequest(callback: (clientTime: number) => void) {
    this.onSyncRequest = callback;
  }

  /**
   * Start the synchronization process
   */
  start() {
    // Reconnects and planned promotions may both start synchronization. Own a
    // single timeout chain so each new active socket replaces the old one.
    this.stop();
    this.syncsSent = 0;
    this.sendSyncRequest();
  }

  stop() {
    if (this.timeout !== null) {
      this.cancelTimeout(this.timeout);
      this.timeout = null;
    }
  }

  /**
   * Send a sync request to the server
   */
  private sendSyncRequest() {
    if (!this.onSyncRequest) return;

    const t1 = this.now();
    this.onSyncRequest(t1);

    // Schedule next sync
    const delayMs = this.syncsSent < this.initialSyncCount - 1
      ? 100
      : this.syncInterval;
    this.syncsSent++;
    this.timeout = this.scheduleTimeout(() => {
      this.timeout = null;
      this.sendSyncRequest();
    }, delayMs);
  }

  /**
   * Process a sync response from the server
   * @param t1 Client send timestamp
   * @param t2 Server receive timestamp
   * @param t3 Client receive timestamp (now)
   */
  processSyncResponse(t1: number, t2: number, t3: number = this.now()) {
    const rtt = t3 - t1;
    const offset = t2 - t1 - (rtt / 2);

    const measurement: SyncMeasurement = {
      offset,
      rtt,
      timestamp: t3
    };

    // Add measurement and keep only the most recent ones
    this.measurements.push(measurement);
    if (this.measurements.length > this.maxMeasurements) {
      this.measurements.shift();
    }

    console.log(`Clock sync - RTT: ${rtt}ms, Offset: ${offset.toFixed(1)}ms, Total measurements: ${this.measurements.length}`);
    return measurement;
  }

  /**
   * Median server-minus-client clock offset from recent measurements.
   * A positive value means the server clock is ahead, so synchronized client
   * time is `clientTime + offset`.
   */
  getServerClockOffsetMs(): number | null {
    if (this.measurements.length === 0) {
      return null;
    }

    // Sort measurements by offset to find median
    const sortedOffsets = this.measurements
      .map(m => m.offset)
      .sort((a, b) => a - b);

    const len = sortedOffsets.length;
    if (len % 2 === 0) {
      // Even number of measurements - average of middle two
      return (sortedOffsets[len / 2 - 1] + sortedOffsets[len / 2]) / 2;
    } else {
      // Odd number - take the middle one
      return sortedOffsets[Math.floor(len / 2)];
    }
  }

  /**
   * Get statistics about clock synchronization
   */
  getStats() {
    if (this.measurements.length === 0) {
      return {
        drift: 0,
        avgRtt: 0,
        measurementCount: 0,
        lastSync: null
      };
    }

    const serverClockOffsetMs = this.getServerClockOffsetMs() ?? 0;
    const avgRtt = this.measurements.reduce((sum, m) => sum + m.rtt, 0) / this.measurements.length;
    const lastSync = this.measurements[this.measurements.length - 1].timestamp;

    return {
      drift: serverClockOffsetMs,
      avgRtt,
      measurementCount: this.measurements.length,
      lastSync
    };
  }

  /**
   * Reset for a new connection, carrying the last known offset forward.
   *
   * Wiping measurements entirely would remove the server-clock estimate until
   * several new Pongs arrive — snapping the game-loop time base by the full
   * true drift on every reconnect. That spike used to mis-stamp command
   * ticks (the snake stops responding right after a reconnect). The real
   * clock offset barely changes across a reconnect, so seed the new window
   * with the previous estimate and let fresh measurements displace it.
   */
  reset() {
    const carriedOffset = this.getServerClockOffsetMs();
    this.stop();
    this.measurements = carriedOffset !== null
      ? [{ offset: carriedOffset, rtt: 0, timestamp: this.now() }]
      : [];
    this.syncsSent = 0;
  }
}

// Singleton instance
const clockSyncInstance = new ClockSync();

export const clockSync = clockSyncInstance;
export const getServerClockOffsetMs = () => clockSyncInstance.getServerClockOffsetMs();
