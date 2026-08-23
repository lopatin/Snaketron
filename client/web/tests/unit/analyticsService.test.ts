import assert from 'node:assert/strict';
import test from 'node:test';
import { analytics, type GameAnalyticsSdk } from '../../services/analytics/gameAnalytics.ts';
import {
  ADDRESS_VERDICT_STORAGE_KEY,
  OVERRIDE_STORAGE_KEY,
} from '../../services/analytics/exclusion.ts';
import type { AnalyticsConsent } from '../../types/generated/AnalyticsConsent.ts';
import type { GameType } from '../../types/generated/GameType.ts';

const duel: GameType = { TeamMatch: { per_team: 1 } };
const ffa: GameType = { FreeForAll: { max_players: 6 } };

/** Records every SDK call as a flat string so ordering is easy to assert. */
const recorder = () => {
  const calls: string[] = [];
  const sdk: GameAnalyticsSdk = {
    configureBuild: (build) => calls.push(`configureBuild(${build})`),
    configureUserId: (userId) => calls.push(`configureUserId(${userId})`),
    setExtUserId: (userId) => calls.push(`extUserId(${userId})`),
    configureAvailableCustomDimensions01: (d) => calls.push(`dims01(${d.join('|')})`),
    configureAvailableCustomDimensions02: (d) => calls.push(`dims02(${d.join('|')})`),
    setEnabledInfoLog: () => {},
    setEnabledEventSubmission: (enabled) => calls.push(`submission(${enabled})`),
    initialize: (key) => calls.push(`initialize(${key})`),
    setCustomDimension01: (value) => calls.push(`dim01(${value})`),
    setCustomDimension02: (value) => calls.push(`dim02(${value})`),
    addProgressionEvent: (status, p1, p2, _p3, score) => calls.push(
      `progression(${status},${p1},${p2}${score === undefined ? '' : `,${score}`})`,
    ),
    addDesignEvent: (id, value) => calls.push(
      `design(${id}${value === undefined ? '' : `,${value}`})`,
    ),
    addErrorEvent: (severity, message) => calls.push(`error(${severity},${message})`),
    addAdEvent: (a, t, sdkName, placement) => calls.push(`ad(${a},${t},${sdkName},${placement})`),
    addAdEventWithNoAdReason: (a, t, sdkName, placement, reason) => calls.push(
      `ad(${a},${t},${sdkName},${placement},reason=${reason})`,
    ),
  };
  return { sdk, calls, loads: 0 };
};

/**
 * Install a browser-shaped global with in-memory storage, reset the singleton,
 * and hand back the recorder plus the storage so tests can inspect what was
 * persisted.
 */
const harness = (options: {
  url?: string;
  storage?: Record<string, string>;
  configured?: boolean;
  bootUserId?: string | null | (() => string | null);
} = {}) => {
  const store = new Map<string, string>(Object.entries(options.storage ?? {}));
  const url = new URL(options.url ?? 'https://snaketron.io/');
  const rec = recorder();

  (globalThis as Record<string, unknown>).window = {
    location: { search: url.search, hash: url.hash },
    localStorage: {
      getItem: (key: string) => store.get(key) ?? null,
      setItem: (key: string, value: string) => store.set(key, value),
      removeItem: (key: string) => store.delete(key),
    },
  };

  analytics.__resetForTests({
    supportedDistribution: true,
    buildConfig: options.configured === false
      ? null
      : { gameKey: 'GAME', secretKey: 'SECRET', build: '1.2.3' },
    sdkLoader: async () => {
      rec.loads += 1;
      return rec.sdk;
    },
  });

  const bootUserId = options.bootUserId ?? null;
  const resolveUserId = typeof bootUserId === 'function'
    ? bootUserId
    : () => bootUserId;

  return { rec, store, resolveUserId };
};

const counts: AnalyticsConsent = { excluded: false, reason: null };
const excludes: AnalyticsConsent = { excluded: true, reason: 'excludedAddress' };

test.afterEach(() => {
  delete (globalThis as Record<string, unknown>).window;
  analytics.__resetForTests();
});

test('a counted session initializes once and reports', async () => {
  const { rec, store, resolveUserId } = harness();
  const decision = await analytics.start({ consent: async () => counts, resolveUserId });

  assert.deepEqual(decision, { report: true });
  assert.deepEqual(rec.calls, [
    'configureBuild(1.2.3)',
    'dims01(guest|registered)',
    'dims02(keyboard|touch)',
    'initialize(GAME)',
  ]);
  assert.equal(store.get(ADDRESS_VERDICT_STORAGE_KEY), 'counted');

  // Repeat starts are no-ops; a second GameAnalytics session would double
  // every DAU and session-length figure.
  await analytics.start({ consent: async () => counts, resolveUserId });
  assert.equal(rec.loads, 1);
});

/**
 * The strongest property in the integration: an excluded browser must never
 * fetch the vendor chunk, so there is no SDK present to mis-wire later.
 */
test('an excluded address never loads the SDK at all', async () => {
  const { rec, store, resolveUserId } = harness();
  const decision = await analytics.start({ consent: async () => excludes, resolveUserId });

  assert.deepEqual(decision, { report: false, reason: 'excludedAddress' });
  assert.equal(rec.loads, 0);
  assert.deepEqual(rec.calls, []);
  assert.equal(store.get(ADDRESS_VERDICT_STORAGE_KEY), 'excluded');
});

test('the URL switch excludes this browser and is remembered', async () => {
  const { rec, store, resolveUserId } = harness({ url: 'https://snaketron.io/?analytics=off' });
  const decision = await analytics.start({ consent: async () => counts, resolveUserId });

  assert.deepEqual(decision, { report: false, reason: 'localOverride' });
  assert.equal(rec.loads, 0, 'a locally excluded browser skips the network too');
  assert.equal(store.get(OVERRIDE_STORAGE_KEY), 'off');
});

test('an opt-in clears a stored opt-out rather than layering another flag', async () => {
  const { store, resolveUserId } = harness({
    url: 'https://snaketron.io/?analytics=on',
    storage: { [OVERRIDE_STORAGE_KEY]: 'off' },
  });

  assert.deepEqual(await analytics.start({ consent: async () => counts, resolveUserId }), { report: true });
  assert.equal(store.has(OVERRIDE_STORAGE_KEY), false);
});

/**
 * Without the cache, every API hiccup would quietly fold the operator's own
 * play back into the numbers — the exact failure this integration exists to
 * avoid.
 */
test('a failed consent check falls back to the cached verdict', async () => {
  const excluded = harness({ storage: { [ADDRESS_VERDICT_STORAGE_KEY]: 'excluded' } });
  assert.deepEqual(
    await analytics.start({
      consent: async () => { throw new Error('offline'); },
      resolveUserId: excluded.resolveUserId,
    }),
    { report: false, reason: 'excludedAddress' },
  );
  assert.equal(excluded.rec.loads, 0);

  // A player who has never been excluded is unaffected by the same outage.
  const ordinary = harness();
  assert.deepEqual(
    await analytics.start({
      consent: async () => { throw new Error('offline'); },
      resolveUserId: ordinary.resolveUserId,
    }),
    { report: true },
  );
  assert.equal(ordinary.rec.loads, 1);
});

test('a bundle with no keys never touches the network or the SDK', async () => {
  const { rec, resolveUserId } = harness({ configured: false });
  let consentCalls = 0;

  assert.deepEqual(
    await analytics.start({ consent: async () => { consentCalls += 1; return counts; }, resolveUserId }),
    { report: false, reason: 'notConfigured' },
  );
  assert.equal(consentCalls, 0);
  assert.equal(rec.loads, 0);
});

/**
 * A match can start before a warm load finishes resolving the gate, so the
 * events raised meanwhile have to survive — in order, because GameAnalytics
 * rejects a Complete that arrives before its Start.
 */
test('events raised before the gate resolves are flushed in order', async () => {
  const { rec, resolveUserId } = harness();

  analytics.setAccountType('registered');
  analytics.trackMatchStart(duel, 'Competitive');
  analytics.trackMatchEnd(
    { game_type: duel, queue_mode: 'Competitive' },
    { score: 11, isWinner: true },
    30_000,
  );

  assert.deepEqual(rec.calls, [], 'nothing reaches an SDK that does not exist yet');

  await analytics.start({ consent: async () => counts, resolveUserId });

  assert.deepEqual(rec.calls, [
    'configureBuild(1.2.3)',
    'dims01(guest|registered)',
    'dims02(keyboard|touch)',
    'initialize(GAME)',
    'dim01(registered)',
    'progression(1,duel,competitive)',
    'progression(2,duel,competitive,11)',
    'design(match:duration:duel,30)',
  ]);
});

test('events raised before an exclusion resolves are discarded', async () => {
  const { rec, resolveUserId } = harness();

  analytics.trackMatchStart(duel, 'Competitive');
  await analytics.start({ consent: async () => excludes, resolveUserId });
  analytics.trackMatchEnd(
    { game_type: duel, queue_mode: 'Competitive' },
    { score: 11, isWinner: true },
    30_000,
  );

  assert.deepEqual(rec.calls, []);
});

/**
 * GameAnalytics rejects a Complete or Fail with no matching Start, so a
 * spectator or a match joined in progress must not report a result either.
 */
test('a result is only reported for a progression this session opened', async () => {
  const { rec, resolveUserId } = harness();
  await analytics.start({ consent: async () => counts, resolveUserId });
  rec.calls.length = 0;

  // No Start at all.
  analytics.trackMatchEnd(
    { game_type: ffa, queue_mode: 'Quickmatch' },
    { score: 4, isWinner: false },
    5_000,
  );
  assert.deepEqual(rec.calls, []);

  // A Start for a different match than the one that finished.
  analytics.trackMatchStart(duel, 'Competitive');
  analytics.trackMatchEnd(
    { game_type: ffa, queue_mode: 'Quickmatch' },
    { score: 4, isWinner: false },
    5_000,
  );
  assert.deepEqual(rec.calls, ['progression(1,duel,competitive)']);
});

test('a re-rendered match start does not inflate the start count', async () => {
  const { rec, resolveUserId } = harness();
  await analytics.start({ consent: async () => counts, resolveUserId });
  rec.calls.length = 0;

  analytics.trackMatchStart(duel, 'Competitive');
  analytics.trackMatchStart(duel, 'Competitive');
  analytics.trackMatchStart(duel, 'Competitive');

  assert.deepEqual(rec.calls, ['progression(1,duel,competitive)']);
});

/**
 * The whole point of reading the id out of the session token at boot: the
 * Snaketron user id becomes GameAnalytics' *primary* identity, which is what
 * keys retention and the user explorer. That only works before `initialize`,
 * so the ordering is pinned here rather than left to call order.
 */
test('a known player id is configured before the SDK initializes', async () => {
  const { rec, resolveUserId } = harness({ bootUserId: '4711' });
  await analytics.start({ consent: async () => counts, resolveUserId });

  assert.deepEqual(rec.calls, [
    'configureBuild(1.2.3)',
    'dims01(guest|registered)',
    'dims02(keyboard|touch)',
    'configureUserId(4711)',
    'initialize(GAME)',
    // Set even though it duplicates the primary id — see below.
    'extUserId(4711)',
  ]);
  assert.ok(
    rec.calls.indexOf('configureUserId(4711)') < rec.calls.indexOf('initialize(GAME)'),
    'GameAnalytics ignores a user id configured after initialize',
  );

  // React re-reporting the same identity is a no-op, not a second call.
  analytics.setPlayerId('4711');
  assert.equal(rec.calls.filter((call) => call.startsWith('extUserId')).length, 1);
});

/**
 * A brand-new visitor has no session token yet, so their first session is
 * keyed on GameAnalytics' device id. `user_id_ext` still carries the real id
 * from the moment the guest session exists, so no events are unattributed —
 * and because it is set in both cases, a query against `user_id_ext` never has
 * to coalesce it with `user_id`.
 */
test('an id learned after init is attached as an external id', async () => {
  const { rec, resolveUserId } = harness({ bootUserId: null });
  await analytics.start({ consent: async () => counts, resolveUserId });
  rec.calls.length = 0;

  analytics.setPlayerId('9002');
  assert.deepEqual(rec.calls, ['extUserId(9002)']);

  // Idempotent, and an empty id is never sent — GameAnalytics rejects it.
  analytics.setPlayerId('9002');
  analytics.setPlayerId('');
  assert.deepEqual(rec.calls, ['extUserId(9002)']);
});

test('an id learned before the gate resolves is applied to that session', async () => {
  const { rec, resolveUserId } = harness({ bootUserId: null });

  analytics.setPlayerId('313');
  analytics.trackMatchStart(duel, 'Competitive');
  await analytics.start({ consent: async () => counts, resolveUserId });

  assert.deepEqual(rec.calls, [
    'configureBuild(1.2.3)',
    'dims01(guest|registered)',
    'dims02(keyboard|touch)',
    'initialize(GAME)',
    'extUserId(313)',
    'progression(1,duel,competitive)',
  ]);
});

/** A broken identity lookup costs the session its id, and nothing more. */
test('a throwing user-id resolver still starts the session', async () => {
  const { rec, resolveUserId } = harness({
    bootUserId: () => { throw new Error('storage blocked'); },
  });

  assert.deepEqual(await analytics.start({ consent: async () => counts, resolveUserId }), {
    report: true,
  });
  assert.deepEqual(rec.calls, [
    'configureBuild(1.2.3)',
    'dims01(guest|registered)',
    'dims02(keyboard|touch)',
    'initialize(GAME)',
  ]);
});

/**
 * The operator's account is only known once `/auth/me` resolves, which can be
 * after the session has already opened. Both halves matter: silence the live
 * SDK, and make the next load in this browser skip it entirely.
 */
test('an operator signing in stops submission and is remembered', async () => {
  const { rec, store, resolveUserId } = harness();
  await analytics.start({ consent: async () => counts, resolveUserId });
  rec.calls.length = 0;

  analytics.excludeOperator();

  assert.deepEqual(rec.calls, ['submission(false)']);
  assert.equal(store.get(OVERRIDE_STORAGE_KEY), 'off');
  assert.deepEqual(analytics.status, { report: false, reason: 'operatorAccount' });

  analytics.trackMatchStart(duel, 'Competitive');
  analytics.trackDeath('Wall', duel);
  assert.deepEqual(rec.calls, ['submission(false)'], 'nothing reports after exclusion');
});

test('ad breaks report a show or a typed failure', async () => {
  const { rec, resolveUserId } = harness();
  await analytics.start({ consent: async () => counts, resolveUserId });
  rec.calls.length = 0;

  analytics.trackAdBreak('completed', 'crazygames', 'pre_match');
  analytics.trackAdBreak('unavailable', 'crazygames', 'pre_match');
  analytics.trackAdBreak('blocked', 'crazygames', 'pre_match');

  assert.deepEqual(rec.calls, [
    'ad(2,4,crazygames,pre_match)',
    'ad(3,4,crazygames,pre_match,reason=3)',
    'ad(3,4,crazygames,pre_match,reason=1)',
  ]);
});

/**
 * A render or reconnect loop raises the same error every frame. The SDK does
 * not rate-limit error events, so an unbounded handler would spend the
 * player's bandwidth and bury the dashboard under one repeated line.
 */
test('errors are deduplicated and capped per session', async () => {
  const { rec, resolveUserId } = harness();
  await analytics.start({ consent: async () => counts, resolveUserId });
  rec.calls.length = 0;

  for (let i = 0; i < 50; i += 1) {
    analytics.trackError('error', 'the same failing frame');
  }
  assert.deepEqual(rec.calls, ['error(4,the same failing frame)'], 'a loop costs one event');

  for (let i = 0; i < 50; i += 1) {
    analytics.trackError('error', `distinct failure ${i}`);
  }
  assert.equal(rec.calls.length, 10, 'and a broken build cannot exceed the session cap');
});

test('an SDK that throws is contained rather than breaking the caller', async () => {
  const { rec, resolveUserId } = harness();
  await analytics.start({ consent: async () => counts, resolveUserId });
  rec.calls.length = 0;
  rec.sdk.addDesignEvent = () => { throw new Error('vendor exploded'); };

  assert.doesNotThrow(() => analytics.trackDeath('Wall', duel));
  assert.doesNotThrow(() => analytics.trackQueueRequest('competitive', 'duel'));
});

/** A gate that never resolves must cost a fixed amount of memory. */
test('the pre-consent queue is bounded and keeps the newest events', async () => {
  const { rec, resolveUserId } = harness();
  for (let index = 0; index < 100; index += 1) {
    analytics.trackMilestone(['bulk', `n${index}`]);
  }

  await analytics.start({ consent: async () => counts, resolveUserId });

  const designCalls = rec.calls.filter((call) => call.startsWith('design('));
  assert.equal(designCalls.length, 32);
  assert.equal(designCalls[0], 'design(bulk:n68)');
  assert.equal(designCalls.at(-1), 'design(bulk:n99)');
});
