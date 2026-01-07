/**
 * Clock synchronization utility for NTP-style time synchronization
 * between client and server to compensate for clock drift.
 */

interface SyncMeasurement {
  offset: number;
  rtt: number;
  timestamp: number;
}

class ClockSync {
  private measurements: SyncMeasurement[] = [];
  private maxMeasurements = 3;
  private initialSyncCount = 3;
  private syncsSent = 0;
  private syncInterval = 5000; // 5 seconds
  private onSyncRequest: ((clientTime: number) => void) | null = null;

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
    this.syncsSent = 0;
    this.sendSyncRequest();
  }

  /**
   * Send a sync request to the server
   */
  private sendSyncRequest() {
    if (!this.onSyncRequest) return;

    const t1 = Date.now();
    this.onSyncRequest(t1);

    // Schedule next sync
    if (this.syncsSent < this.initialSyncCount - 1) {
      // During initial sync, send requests immediately
      setTimeout(() => this.sendSyncRequest(), 100);
    } else {
      // After initial sync, send every 5 seconds
      setTimeout(() => this.sendSyncRequest(), this.syncInterval);
    }
    
    this.syncsSent++;
  }

  /**
   * Process a sync response from the server
   * @param t1 Client send timestamp
   * @param t2 Server receive timestamp
   * @param t3 Client receive timestamp (now)
   */
  processSyncResponse(t1: number, t2: number, t3: number = Date.now()) {
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
  }

  /**
   * Get the current clock drift (median of recent measurements)
   * @returns Clock drift in milliseconds (positive means client is ahead)
   */
  getClockDrift(): number {
    if (this.measurements.length === 0) {
      return 0;
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

    const drift = this.getClockDrift();
    const avgRtt = this.measurements.reduce((sum, m) => sum + m.rtt, 0) / this.measurements.length;
    const lastSync = this.measurements[this.measurements.length - 1].timestamp;

    return {
      drift,
      avgRtt,
      measurementCount: this.measurements.length,
      lastSync
    };
  }

  /**
   * Clear all measurements
   */
  reset() {
    this.measurements = [];
    this.syncsSent = 0;
  }
}

// Singleton instance
const clockSyncInstance = new ClockSync();

export const clockSync = clockSyncInstance;
export const getClockDrift = () => clockSyncInstance.getClockDrift();