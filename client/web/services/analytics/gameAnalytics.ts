/**
 * The GameAnalytics transport.
 *
 * Two properties matter more than anything this file reports:
 *
 * 1. **It cannot break the game.** Every SDK call is wrapped, every failure is
 *    logged at info and swallowed, and a failure permanently degrades to
 *    "analytics off" rather than retrying into a hot loop.
 * 2. **It cannot report an excluded session.** The SDK is loaded by dynamic
 *    import *after* the exclusion gate resolves, so an excluded browser never
 *    fetches the vendor chunk, never opens a GameAnalytics session, and has
 *    nothing to leak if a later call is mis-wired.
 *
 * Events raised before the gate resolves are buffered, because the interesting
 * ones (a match starting) can easily beat a network round trip on a warm load.
 */

import {
  ACCOUNT_DIMENSIONS,
  ANALYTICS_BUILD_CONFIG,
  ANALYTICS_SUPPORTED_DISTRIBUTION,
  INPUT_DIMENSIONS,
  type AccountDimension,
  type AnalyticsBuildConfig,
  type InputDimension,
} from './config.ts';
import {
  addressVerdictFromConsent,
  loadCachedAddressVerdict,
  loadStoredOverride,
  readAnalyticsOverrideFromUrl,
  resolveAnalyticsDecision,
  storeAddressVerdict,
  storeOverride,
  type AddressVerdict,
  type AnalyticsDecision,
} from './exclusion.ts';
import {
  buildEventId,
  buildMatchResultEvent,
  deathCauseSlug,
  gameTypeSlug,
  matchProgression,
  type LocalMatchSummary,
} from './events.ts';
import type { AnalyticsConsent } from '../../types/generated/AnalyticsConsent';
import type { DeathCause } from '../../types/generated/DeathCause';
import type { GameState } from '../../types/generated/GameState';
import type { GameType } from '../../types/generated/GameType';
import type { QueueMode } from '../../types/generated/QueueMode';

/** Mirrors GameAnalytics' `EGAProgressionStatus`. */
const PROGRESSION_START = 1;
const PROGRESSION_COMPLETE = 2;
const PROGRESSION_FAIL = 3;

/** Mirrors GameAnalytics' `EGAErrorSeverity`. */
export const ERROR_SEVERITY = {
  warning: 3,
  error: 4,
  critical: 5,
} as const;
export type ErrorSeverity = keyof typeof ERROR_SEVERITY;

/**
 * The slice of the GameAnalytics SDK this integration depends on.
 *
 * Declared locally rather than imported from the vendor `.d.ts`: it pins our
 * dependency surface to a dozen calls, survives the SDK's legacy namespace-
 * style declarations, and lets tests substitute a recorder for the real module.
 */
export interface GameAnalyticsSdk {
  configureBuild(build: string): void;
  configureAvailableCustomDimensions01(dimensions: string[]): void;
  configureAvailableCustomDimensions02(dimensions: string[]): void;
  setEnabledInfoLog(enabled: boolean): void;
  setEnabledEventSubmission(enabled: boolean): void;
  initialize(gameKey: string, gameSecret: string): void;
  setCustomDimension01(dimension: string): void;
  setCustomDimension02(dimension: string): void;
  addProgressionEvent(
    status: number,
    progression01: string,
    progression02?: string,
    progression03?: string,
    score?: number,
  ): void;
  addDesignEvent(eventId: string, value?: number): void;
  addErrorEvent(severity: number, message: string): void;
}

export type SdkLoader = () => Promise<GameAnalyticsSdk>;

/** Asks the server whether this caller's network is on the exclusion list. */
export type ConsentLoader = () => Promise<AnalyticsConsent>;

const loadRealSdk: SdkLoader = async () => {
  const module = await import(/* webpackChunkName: "gameanalytics" */ 'gameanalytics');
  // The vendored `.d.ts` exports only the enum namespace. The `GameAnalytics`
  // class the bundle also attaches to it is declared in an ambient block that
  // does not survive the module export, so reaching it needs a cast — which is
  // exactly what `GameAnalyticsSdk` above exists to keep honest.
  const namespace = module.gameanalytics as unknown as { GameAnalytics: GameAnalyticsSdk };
  return namespace.GameAnalytics;
};

const errorMessage = (error: unknown): string => (
  error instanceof Error ? error.message : String(error)
);

/**
 * A bounded buffer. Analytics is the least important thing on the page, so a
 * gate that never resolves must cost a fixed amount of memory and nothing
 * else; the oldest events are dropped first because the newest describe what
 * the player is doing now.
 */
const MAX_QUEUED_EVENTS = 32;

type QueuedEvent = (sdk: GameAnalyticsSdk) => void;

class GameAnalyticsService {
  private sdk: GameAnalyticsSdk | null = null;
  private decision: AnalyticsDecision | null = null;
  private startup: Promise<AnalyticsDecision> | null = null;
  private queue: QueuedEvent[] = [];
  private accountDimension: AccountDimension | null = null;
  private inputDimension: InputDimension | null = null;
  /**
   * The progression currently open, as `progression01:progression02`.
   *
   * GameAnalytics rejects a Complete or Fail with no matching Start, so a
   * match joined in progress, or one whose start event was dropped by the
   * queue, must not report a result either.
   */
  private openProgression: string | null = null;

  private sdkLoader: SdkLoader = loadRealSdk;
  private consentLoader: ConsentLoader | null = null;
  private buildConfig: AnalyticsBuildConfig | null = ANALYTICS_BUILD_CONFIG;
  private supportedDistribution = ANALYTICS_SUPPORTED_DISTRIBUTION;

  /**
   * Resolve the exclusion gate and, if this session counts, load and start the
   * SDK. Safe to call repeatedly; only the first call does any work.
   */
  start = (consentLoader: ConsentLoader): Promise<AnalyticsDecision> => {
    if (this.startup) {
      return this.startup;
    }
    this.consentLoader = consentLoader;
    this.startup = this.resolveAndInitialize().catch((error) => {
      // Unreachable in practice — the body swallows its own failures — but a
      // rejected startup promise would strand every queued event.
      console.info('Analytics startup failed:', errorMessage(error));
      return this.settle({ report: false, reason: 'notConfigured' });
    });
    return this.startup;
  };

  private resolveAndInitialize = async (): Promise<AnalyticsDecision> => {
    const localOverride = this.captureLocalOverride();

    // Cheap, synchronous exclusions first: there is no reason to ask the
    // server about a bundle that could never report anything anyway.
    const withoutServer = resolveAnalyticsDecision({
      configured: this.buildConfig !== null,
      supportedDistribution: this.supportedDistribution,
      localOverride,
      addressVerdict: loadCachedAddressVerdict(),
      operatorAccount: false,
    });
    if (!withoutServer.report) {
      return this.settle(withoutServer);
    }

    const decision = resolveAnalyticsDecision({
      configured: true,
      supportedDistribution: true,
      localOverride,
      addressVerdict: await this.fetchAddressVerdict(),
      operatorAccount: false,
    });
    if (!decision.report) {
      return this.settle(decision);
    }

    const sdk = await this.loadSdk();
    return this.settle(sdk ? decision : { report: false, reason: 'notConfigured' }, sdk);
  };

  /**
   * Read `?analytics=off|on` once per page load and remember it.
   *
   * Persisting is the point: an operator types the switch once and every later
   * visit in that browser is excluded without it, including deep links and
   * pages they arrive at from elsewhere.
   */
  private captureLocalOverride = (): 'off' | 'on' | null => {
    let fromUrl: 'off' | 'on' | null = null;
    try {
      fromUrl = readAnalyticsOverrideFromUrl(window.location.search, window.location.hash);
    } catch {
      // A non-browser host (unit tests, SSR probes) simply has no URL switch.
    }
    if (fromUrl) {
      // An explicit `on` clears a previous opt-out rather than storing a flag
      // that would have to be reasoned about later.
      storeOverride(fromUrl === 'off' ? 'off' : null);
      return fromUrl;
    }
    return loadStoredOverride();
  };

  /**
   * Ask the server about this caller's address, falling back to the cached
   * verdict. A failed request must not quietly fold an excluded operator back
   * into the numbers, which is exactly what an uncached default would do.
   */
  private fetchAddressVerdict = async (): Promise<AddressVerdict> => {
    const cached = loadCachedAddressVerdict();
    if (!this.consentLoader) {
      return cached;
    }
    try {
      const verdict = addressVerdictFromConsent(await this.consentLoader());
      storeAddressVerdict(verdict);
      return verdict;
    } catch (error) {
      console.info('Analytics consent check unavailable:', errorMessage(error));
      return cached;
    }
  };

  private loadSdk = async (): Promise<GameAnalyticsSdk | null> => {
    const config = this.buildConfig;
    if (!config) {
      return null;
    }
    try {
      const sdk = await this.sdkLoader();
      sdk.setEnabledInfoLog(process.env.NODE_ENV !== 'production');
      sdk.configureBuild(config.build);
      sdk.configureAvailableCustomDimensions01([...ACCOUNT_DIMENSIONS]);
      sdk.configureAvailableCustomDimensions02([...INPUT_DIMENSIONS]);
      sdk.initialize(config.gameKey, config.secretKey);
      return sdk;
    } catch (error) {
      console.info('GameAnalytics failed to initialize:', errorMessage(error));
      return null;
    }
  };

  /** Record the outcome, then flush or discard whatever queued up meanwhile. */
  private settle = (
    decision: AnalyticsDecision,
    sdk: GameAnalyticsSdk | null = null,
  ): AnalyticsDecision => {
    this.decision = decision;
    this.sdk = decision.report ? sdk : null;

    const pending = this.queue;
    this.queue = [];
    if (this.sdk) {
      // Dimensions set before the gate resolved describe the session that is
      // about to start, so they are replayed ahead of the buffered events.
      this.applyDimensions(this.sdk);
      for (const event of pending) {
        this.invoke(event);
      }
    }
    return decision;
  };

  private applyDimensions = (sdk: GameAnalyticsSdk): void => {
    const account = this.accountDimension;
    if (account) {
      this.invoke((target) => target.setCustomDimension01(account), sdk);
    }
    const input = this.inputDimension;
    if (input) {
      this.invoke((target) => target.setCustomDimension02(input), sdk);
    }
  };

  /**
   * Run one SDK call, or buffer it while the gate is still open. A throwing
   * SDK is logged and ignored: no analytics call is worth a broken frame.
   */
  private emit = (event: QueuedEvent): void => {
    if (this.decision === null) {
      if (this.queue.length >= MAX_QUEUED_EVENTS) {
        this.queue.shift();
      }
      this.queue.push(event);
      return;
    }
    if (!this.sdk) {
      return;
    }
    this.invoke(event);
  };

  private invoke = (event: QueuedEvent, sdk: GameAnalyticsSdk | null = this.sdk): void => {
    if (!sdk) {
      return;
    }
    try {
      event(sdk);
    } catch (error) {
      console.info('GameAnalytics event dropped:', errorMessage(error));
    }
  };

  /** Whether this session is being reported. `null` until the gate resolves. */
  get status(): AnalyticsDecision | null {
    return this.decision;
  }

  /**
   * A custom dimension is session state, not an event.
   *
   * It is therefore stored and applied directly rather than queued: queueing
   * it would let a full buffer evict the dimension and silently mislabel every
   * event that follows, and would double-apply it against the replay in
   * `settle`.
   */
  setAccountType = (account: AccountDimension): void => {
    if (this.accountDimension === account) {
      return;
    }
    this.accountDimension = account;
    this.invoke((sdk) => sdk.setCustomDimension01(account));
  };

  setInputSurface = (input: InputDimension): void => {
    if (this.inputDimension === input) {
      return;
    }
    this.inputDimension = input;
    this.invoke((sdk) => sdk.setCustomDimension02(input));
  };

  /**
   * Stop reporting because this player operates the deployment.
   *
   * The account is only known after `/auth/me` resolves, which can be after
   * the session has already opened, so this both silences the live SDK and
   * remembers the choice — the next load in this browser is excluded before
   * the SDK is ever fetched.
   */
  excludeOperator = (): void => {
    storeOverride('off');
    if (this.decision?.report === false) {
      return;
    }
    const sdk = this.sdk;
    this.decision = { report: false, reason: 'operatorAccount' };
    this.sdk = null;
    this.queue = [];
    this.openProgression = null;
    if (sdk) {
      try {
        sdk.setEnabledEventSubmission(false);
      } catch (error) {
        console.info('GameAnalytics could not stop event submission:', errorMessage(error));
      }
    }
  };

  /**
   * Open a match progression. Repeated calls for the same match are ignored so
   * a re-render cannot inflate the start count.
   */
  trackMatchStart = (gameType: GameType, queueMode: QueueMode): void => {
    const progression = matchProgression(gameType, queueMode);
    if (!progression) {
      return;
    }
    const key = `${progression.progression01}:${progression.progression02}`;
    if (this.openProgression === key) {
      return;
    }
    this.openProgression = key;
    this.emit((sdk) => sdk.addProgressionEvent(
      PROGRESSION_START,
      progression.progression01,
      progression.progression02,
    ));
  };

  /**
   * Close the open match progression with the local player's result.
   *
   * Silently does nothing when no progression is open — a spectator, or a
   * player who joined after the start event — because GameAnalytics would
   * reject the unmatched Complete anyway.
   */
  trackMatchEnd = (
    gameState: Pick<GameState, 'game_type' | 'queue_mode'>,
    local: LocalMatchSummary | null,
    elapsedMs: number,
  ): void => {
    const result = buildMatchResultEvent(gameState, local, elapsedMs);
    if (!result) {
      this.openProgression = null;
      return;
    }
    const key = `${result.progression01}:${result.progression02}`;
    if (this.openProgression !== key) {
      this.openProgression = null;
      return;
    }
    this.openProgression = null;

    this.emit((sdk) => sdk.addProgressionEvent(
      result.outcome === 'complete' ? PROGRESSION_COMPLETE : PROGRESSION_FAIL,
      result.progression01,
      result.progression02,
      undefined,
      result.score,
    ));

    const durationEvent = buildEventId(['match', 'duration', result.progression01]);
    if (durationEvent) {
      this.emit((sdk) => sdk.addDesignEvent(durationEvent, result.durationSeconds));
    }
  };

  /** Report how a life ended, as bounded cause and mode slugs. */
  trackDeath = (cause: DeathCause, gameType: GameType): void => {
    const eventId = buildEventId(['match', 'death', deathCauseSlug(cause), gameTypeSlug(gameType)]);
    if (eventId) {
      this.emit((sdk) => sdk.addDesignEvent(eventId));
    }
  };

  /** Report entry into matchmaking, which is the top of the play funnel. */
  trackQueueRequest = (queue: string, mode: string): void => {
    const eventId = buildEventId(['queue', 'request', queue, mode]);
    if (eventId) {
      this.emit((sdk) => sdk.addDesignEvent(eventId));
    }
  };

  /** Report a milestone the player reached outside a match. */
  trackMilestone = (parts: readonly string[]): void => {
    const eventId = buildEventId(parts);
    if (eventId) {
      this.emit((sdk) => sdk.addDesignEvent(eventId));
    }
  };

  trackError = (severity: ErrorSeverity, message: string): void => {
    // GameAnalytics truncates a long message server-side; trimming here keeps
    // the dashboard's grouping stable instead of dependent on stack length.
    const trimmed = message.slice(0, 8_192);
    this.emit((sdk) => sdk.addErrorEvent(ERROR_SEVERITY[severity], trimmed));
  };

  /**
   * Reset every field to its constructed state and install test doubles.
   * Exists for unit tests; production code has exactly one live instance.
   */
  __resetForTests = (overrides: {
    sdkLoader?: SdkLoader;
    buildConfig?: AnalyticsBuildConfig | null;
    supportedDistribution?: boolean;
  } = {}): void => {
    this.sdk = null;
    this.decision = null;
    this.startup = null;
    this.queue = [];
    this.accountDimension = null;
    this.inputDimension = null;
    this.openProgression = null;
    this.consentLoader = null;
    this.sdkLoader = overrides.sdkLoader ?? loadRealSdk;
    this.buildConfig = overrides.buildConfig === undefined
      ? ANALYTICS_BUILD_CONFIG
      : overrides.buildConfig;
    this.supportedDistribution = overrides.supportedDistribution
      ?? ANALYTICS_SUPPORTED_DISTRIBUTION;
  };
}

export const analytics = new GameAnalyticsService();
